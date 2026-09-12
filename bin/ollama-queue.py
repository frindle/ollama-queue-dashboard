#!/usr/bin/env python3
"""Simple FIFO dispatch queue for ollama-worker.py, across Studio + Unraid.

Built 2026-08-28 to close a gap identified the same night: pick_host() in
ollama-worker.py decides which host a SINGLE dispatch should use, but
nothing sequenced multiple pending dispatches -- every multi-step queue
that night was a one-off hand-written `nohup bash -c 'until ! kill -0
<pid>; do sleep 15; done; ...'` chain. That produced a real bug: an old
orphaned chain fired an OCR retry with zero coordination with a newer
chain also queuing work on the same host, because neither could see the
other's state.

This tool is the shared state those chains were missing. It is
deliberately a plain FIFO, not a priority-preemption scheduler -- Penn's
call the night this was scoped: an LLM dispatch doesn't checkpoint/resume
cleanly mid-run, so "preemption" really means "kill and redo," which stays
a judgment call, not something to hand to a scheduling algorithm.

Update (pause/resume): ollama-worker.py now saves its transcript after
every completed iteration and treats SIGTERM as a graceful pause (exit code
3, resumable via --resume), so `promote <job_id>` can preempt a running job
WITHOUT losing work: it pauses the current one and runs the dropped one
first. The FIFO ordering itself is unchanged; promote just moves one pending
job to the front of the queue.

Usage:
    ollama-queue.py enqueue --model qwen3.8:27b-q8_0 --host auto \\
        --cwd /path/to/worktree --task-file task.txt \\
        [--task-kind coding|research] [--manual-tools] [--api ollama|openai] \\
        [--verify "npm test"] [--num-ctx 65536] [--max-iters 20] \\
        [--temperature 0] [--chat-timeout 3000] [--label my-task]

    ollama-queue.py status

    ollama-queue.py promote <job_id>
        Pause whatever is running on that job's lane (graceful SIGTERM) and
        move this pending job to the front of the queue.

    ollama-queue.py resume <job_id>
        Requeue a paused job; it relaunches from its saved transcript.

    ollama-queue.py run [--poll-interval 15]
        Foreground daemon -- launch once via `nohup ... & disown`, same as
        every other background chain tonight, and leave it running. Add
        jobs from any other session with `enqueue`; the daemon picks them
        up on its next poll. An idle daemon with an empty queue is a normal
        state -- it just waits. Only ONE run daemon may be active at a time:
        a second launch detects the held daemon lock and exits (see below).

Routing: --host auto reuses pick_host()'s exact rule (Studio first if
free, Unraid only as overflow when the model fits its usable budget,
Studio unconditionally if it doesn't fit Unraid at all) -- loaded directly
from ollama-worker.py so the rule can't drift between the two tools.
--host studio / --host unraid / an explicit URL bypasses routing entirely
(needed for e.g. a Studio-only headroom test that would otherwise
legitimately auto-route to Unraid).

Concurrency: one job running per lane at a time by DEFAULT, matching
llama-server's `-np 1` single-slot constraint. Before claiming a lane the
daemon checks: its own in-memory active set (pid-keyed, so it can hold two
procs for the one shared-lane exception below), the shared state file (so a
lane occupied by any running job is never double-booked even if state was
written by an earlier daemon instance), and a live pgrep for any external
`ollama-worker.py --host <that URL>` process already running -- a safety
net against exactly the coordination bug this tool exists to fix, for as
long as legacy hand-written chains might still be running alongside it.

Dual-slot exception (added 2026-09-04, fix/dualslot-research -- owner-approved,
narrow): a SECOND job may co-run on an already-busy lane, but ONLY when every
one of these holds, else the lane stays strictly one-per-lane:
  (a) the new job's model is IDENTICAL to the model already resident on that
      lane -- no swap and no second weight load (only a second slot's KV cache
      is added), which is what keeps VRAM safe;
  (b) at least ONE of the two co-resident jobs is task_kind=research (two coding
      jobs never pair -- the extra slot is spent only when one side is IO-bound);
  (c) the backing server POSITIVELY reports >=2 parallel slots with a free one
      -- confirmed live (llama-server /props total_slots). The qwen3.8 bypass
      ships --parallel 1, so this is denied until a human opts into multi-slot
      serving; native Ollama's OLLAMA_NUM_PARALLEL is not introspectable over its
      API and is treated as 1 (never co-runs) until a real signal is added; and
  (d) the second job's num_ctx fits the already-allocated per-slot KV window
      (llama-server pre-allocates the whole ctx budget and splits it across slots,
      so no new memory is loaded), confirmed from /props.
The allow/deny rule is the PURE function slot_decision(), unit-tested by
`ollama-queue.py --self-test`; the daemon feeds it live occupancy re-read from
state on every candidate (never a snapshot), so two ticks can't both see the
second slot as free. Any missing/marginal signal denies -- fail toward serial.

Single-instance: `run` holds an exclusive non-blocking flock on its own
daemon-lock file for its whole lifetime; a second accidental launch exits
with an error instead of racing the first one over lane claims. (The state
flock alone would prevent double-claiming the same job -- claiming happens
inside the locked read-modify-write cycle -- but two daemons could still
each believe a lane was free and dispatch different jobs to it; the daemon
lock makes that impossible outright.)

Recovery: on start, any job left "running" by a dead daemon is requeued if
its worker pid is actually dead. If the pid is still alive (the daemon died
but its child worker kept running), the new daemon adopts it -- keeps the
lane blocked while it runs and requeues the job when the orphan exits, so
it can't sit stuck as "running" forever waiting for a third restart.

--api openai requires an explicit --host (ollama-worker.py's own rule --
pick_host() only knows the two native-Ollama endpoints, not an ad-hoc
llama-server port), so auto-routing is refused for it here too.
"""
import argparse
import fcntl
import importlib.util
import json
from collections import Counter
import os
import re
import signal
import subprocess
import sys
import time
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path

WORKER_PATH = Path.home() / "bin" / "ollama-worker.py"
# Alternate runners a job may name via --runner instead of ollama-worker.py (added 2026-08-30
# for the Autonomous Research session: its 6-stage orchestrator makes 30-60 dynamic model calls
# per run, so queuing each call is wrong -- instead the queue runs the WHOLE run as one job on
# the host it assigns, keeping it visible in queue state + under the VRAM-collision guards).
# Deliberately an EXACT-absolute-path allowlist, not a directory or a flag: this is a queue that
# guards a shared GPU, so "run any program" would be a hole. When --runner is used the queue passes
# ONLY the host/model assignment (--model --host --num-ctx --cwd --task-file); the runner owns
# everything else and its own output. Add an entry here (and teach that program those five flags)
# to allow a new runner.
ALLOWED_RUNNERS = {
    str(Path.home() / "bin" / "studio-research.py"),
    str(Path.home() / "bin" / "code-review-agent.py"),
    str(Path.home() / "bin" / "pet-portrait-render.py"),
    str(Path.home() / "bin" / "txt2img-render.py"),
    str(Path.home() / "bin" / "img2vid-render.py"),
    str(Path.home() / "bin" / "bakeoff-runner.py"),
}
STATE_PATH = Path.home() / "bin" / "ollama-queue-state.json"
LOCK_PATH = Path.home() / "bin" / "ollama-queue-state.lock"

# --- Dashboard-as-worklist retention (Penn 2026-09-07) -------------------------
# The dashboard is a worklist of what still needs handling, not a log. A finished
# job clears only when it is genuinely handled:
#   - failed / done_unconverged -> stay until an explicit `resolve` (I acted on it)
#   - gate / regate jobs         -> stay until we fix/integrate/merge (the handoff marker,
#                                   shown below the failures; resolve clears it on merge)
#   - plain `done` (non-gate)    -> NOT saved: cleared as soon as it finishes. The dispatch's
#                                   value lives on in its persisted gate-<id> result, not the
#                                   done row. (Penn 2026-09-07: "Done shouldn't be saved.")
#   - running / pending / paused -> never auto-pruned
# Only `prune_finished_jobs` auto-removes; everything else clears via resolve/cancel.
RETAIN_DONE_RECENT = 0
_GATE_LABEL_RE = re.compile(r"^(?:gate|regate)-")

def _is_worklist_job(job):
    """A terminal job that must persist as a dashboard worklist item until an
    explicit resolve/cancel: any failure, any unconverged-but-passed run, and
    every gate/regate job (stays until the work it reviewed is merged)."""
    if job.get("status") in ("failed", "done_unconverged", "blocked"):
        return True
    if _GATE_LABEL_RE.match(job.get("label") or ""):
        return True
    return False

def prune_finished_jobs(jobs):
    """Return `jobs` with only stale clean 'done' jobs removed: keep the most
    recent RETAIN_DONE_RECENT non-gate 'done' jobs and drop older ones. Worklist
    items (failures, unconverged, gates) and every non-terminal job are left
    untouched -- they clear only via `resolve`/`cancel`. Pure; ordered by
    enqueued_at (single lane -> finish order ~ enqueue order)."""
    prunable = [j for j in jobs if j.get("status") == "done" and not _is_worklist_job(j)]
    if len(prunable) <= RETAIN_DONE_RECENT:
        return jobs
    keep_ids = {j["id"] for j in sorted(
        prunable, key=lambda j: j.get("enqueued_at") or "", reverse=True)[:RETAIN_DONE_RECENT]}
    drop_ids = {j["id"] for j in prunable if j["id"] not in keep_ids}
    return [j for j in jobs if j.get("id") not in drop_ids]


# --- Chain dependencies + chain-final gating (Penn 2026-09-07) -----------------
# A split fix can be enqueued as an ordered chain: each job carries `after` (the
# full id of the job it must follow), an optional `chain` group tag, and
# `chain_final` on the last step. The daemon launches a job only once its `after`
# dep is `done`; if the dep can never satisfy (failed/unconverged/blocked/gone)
# the job -- and its downstream -- go to the terminal `blocked` status (a worklist
# item). The gate fires ONCE, on the chain_final job. The decisions are PURE
# functions, unit-tested by --self-test exactly like slot_decision.
def dependency_decision(job, jobs_by_id):
    """(action, reason) for a PENDING job's `after` dependency, no I/O:
      'launch'  -- no dep, or the dep is done (still subject to lane/slot logic);
      'wait'    -- dep still in flight (pending/running/paused);
      'blocked' -- dep will never satisfy (failed/done_unconverged/blocked, or gone)."""
    after = job.get("after")
    if not after:
        return ("launch", "no dependency")
    dep = jobs_by_id.get(after)
    if dep is None:
        return ("blocked", f"upstream {after} is gone (cancelled/never enqueued)")
    st = dep.get("status")
    if st == "done":
        return ("launch", f"upstream {after} done")
    if st in ("pending", "running", "paused"):
        return ("wait", f"upstream {after} is {st}")
    return ("blocked", f"upstream {after} did not converge ({st})")


def _cascade_blocked(jobs):
    """After a job is marked 'blocked', transitively block every PENDING job whose
    `after` leads to a blocked/gone dep. Mutates statuses in place; returns the
    list of newly-blocked ids. Pure over the given list (no I/O)."""
    by_id = {j["id"]: j for j in jobs}
    newly = []
    changed = True
    while changed:
        changed = False
        for j in jobs:
            if j.get("status") != "pending":
                continue
            after = j.get("after")
            if not after:
                continue
            dep = by_id.get(after)
            if dep is None or dep.get("status") == "blocked":
                j["status"] = "blocked"
                j["error"] = (f"blocked: upstream {after} did not converge "
                              f"({'gone' if dep is None else dep.get('status')})")
                newly.append(j["id"])
                changed = True
    return newly


def _should_gate_job(job):
    """Whether _fire_gate_on_complete should run for this finished job. A chain
    STEP (has `chain` but not `chain_final`) is skipped -- the chain is gated
    once, on its chain_final job. Label-based skips (image/pet/draft/gate) live
    in _fire_gate_on_complete itself."""
    if job.get("chain") and not job.get("chain_final"):
        return False
    return True
# ------------------------------------------------------------------------------
DAEMON_LOCK_PATH = Path.home() / "bin" / "ollama-queue-daemon.lock"
LOG_DIR = Path.home() / "bin" / "ollama-queue-logs"

LIVE_LOG_DIR = Path.home() / "bin" / "ollama-queue-livelogs"  # added 2026-08-29: per-job
# live-streaming status logs (separate dir from LOG_DIR's plain logs), wired through to
# ollama-worker.py's --live-log/--dispatch-tag flags so a queued job can be tailed live.

_UNSAFE_LABEL = re.compile(r"[/:\\\s]+")

def safe_label(label, maxlen=80):
    """Filesystem-safe livelog filename component. Collapses / : \\ and whitespace to
    '-'. Without this a label carrying an hf.co/... model name puts a '/' in the
    livelog filename -> parent dir doesn't exist -> the queue CRASHES at enqueue for
    any hf.co model. (Integrated from dispatch-fixes/dispatch_fixes.py, item 3a.)"""
    s = _UNSAFE_LABEL.sub("-", str(label or "job")).strip("-.")
    s = re.sub(r"-{2,}", "-", s)
    if not s or set(s) <= {"."}:
        s = "job"
    return s[:maxlen]


# Single source of truth for the iteration default (dispatch-fixes item 2a).
# Must mirror ollama-worker.py's DEFAULT_MAX_ITERS. Before this, the queue passed
# --max-iters 20 UNCONDITIONALLY, so the worker's own DEFAULT_MAX_ITERS=30 was dead
# code and every queued job silently ran on 20. Now: queue default None -> the flag
# is omitted -> the worker's constant actually governs.
WORKER_DEFAULT_MAX_ITERS = 30

def iters_flag(job_max_iters):
    """Argv fragment for --max-iters. Empty when None so the worker default wins."""
    if job_max_iters is None:
        return []
    return ["--max-iters", str(int(job_max_iters))]

# qwen3.8:27b-q8_0 crashes on Ollama's native /api/chat with a confirmed
# upstream bug ("no user query found in messages",
# github.com/ollama/ollama/issues/17778) -- ollama-worker.py's call_ollama()
# docstring and Agent-Dispatch-Log.md (2026-08-28) have the full history.
# The fix is routing it through a standalone llama-server (--jinja, so it
# renders the GGUF's own chat template instead of hitting Ollama's buggy Go
# renderer) instead of native Ollama. This queue tool treats that dedicated
# server as occupying the SAME lane as Studio's native Ollama endpoint --
# they're two processes on the one Mac sharing one 64GB unified-memory pool
# (confirmed via `sysctl hw.memsize`, smaller than assumed), so they must
# never run concurrently: a live incident tonight (2026-08-28) had a 52.5GB
# model resident in Ollama from an earlier dispatch's generous keep_alive
# leave no room for llama-server's 29GB load, OOMing it and wedging its
# Metal backend until a hard restart -- not just a slow-down, a hard crash
# that needed manual recovery.
LLAMA_SERVER_QWEN38_MODEL = "qwen3.8:27b-q8_0"
LLAMA_SERVER_QWEN38_URL = "http://127.0.0.1:8091"
LLAMA_SERVER_START_SCRIPT = Path.home() / "bin" / "start-llama-server-qwen3.8.sh"
# Written by LLAMA_SERVER_START_SCRIPT itself (see its own $SERVER_PID line).
LLAMA_SERVER_QWEN38_PID_FILE = Path("/tmp/llama-server-qwen38.pid")

# Model sizes don't change while a model is pulled; re-querying both hosts'
# /api/tags (15s timeout each) for every pending auto job on every poll would
# hold the state flock through repeated network I/O and stall enqueue/status.
MODEL_SIZE_CACHE_TTL_S = 60

_worker_mod = None
_daemon_lock_fh = None


def worker():
    """Lazy-load ollama-worker.py as a module (hyphenated filename, can't
    plain-import) so pick_host()'s routing rule is reused, not duplicated.
    Importing it has no side effects beyond reading the optional Obsidian
    token file -- main() is __main__-guarded."""
    global _worker_mod
    if _worker_mod is None:
        spec = importlib.util.spec_from_file_location("ollama_worker_lib", WORKER_PATH)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        _worker_mod = mod
    return _worker_mod


class _Locked:
    """Holds STATE_PATH locked (fcntl flock) for one read-modify-write
    cycle, so a daemon poll and an external `enqueue` can't race and lose
    an update -- the exact class of coordination bug this tool exists to
    prevent, applied to its own state file too."""

    def __enter__(self):
        LOCK_PATH.touch(exist_ok=True)
        self._fh = open(LOCK_PATH, "w")
        fcntl.flock(self._fh, fcntl.LOCK_EX)
        return self

    def __exit__(self, *exc):
        fcntl.flock(self._fh, fcntl.LOCK_UN)
        self._fh.close()

    def load(self):
        if not STATE_PATH.exists():
            return {"jobs": []}
        try:
            return json.loads(STATE_PATH.read_text())
        except (json.JSONDecodeError, OSError) as e:
            # Atomic replace below makes mid-write corruption essentially
            # impossible; getting here means the file was corrupted before
            # this version existed or hand-edited. Preserve it for inspection
            # rather than crashing every command on a bad parse.
            backup = STATE_PATH.with_name(f"ollama-queue-state.corrupt-{int(time.time())}.json")
            try:
                os.replace(STATE_PATH, backup)
            except OSError:
                pass
            print(f"[queue] WARNING: state file unreadable ({e}) -- moved to {backup}, starting empty",
                  file=sys.stderr)
            return {"jobs": []}

    def save(self, state):
        # Write temp + atomic rename so a kill mid-write can't leave a
        # truncated JSON file behind (the flock only guards concurrency,
        # not process death).
        tmp = STATE_PATH.with_name(STATE_PATH.name + ".tmp")
        tmp.write_text(json.dumps(state, indent=2))
        os.replace(tmp, STATE_PATH)


class QueueActionError(Exception):
    """User-facing error from a queue action (promote/resume). Raised rather than
    sys.exit()'d so the SAME functions can be called both from CLI subcommands and
    in-process by ollama-queue-api.py -- a sys.exit() inside the API server would kill
    the whole HTTP process, not just fail one request."""


# The single greppable line ollama-worker.py prints when it exits via its graceful-pause
# path (exit code EXIT_CODE_PAUSED) -- see that file's run_task for where/why. Parsed out
# of a job's own log file to learn which transcript to relaunch the paused job from.
RESUMABLE_TRANSCRIPT_RE = re.compile(r"^\[worker\] RESUMABLE TRANSCRIPT: (.+)$", re.MULTILINE)


def _persist_job_completion(job):
    """Freeze the terminal job facts the advisory gate needs into a per-job
    sidecar that SURVIVES pruning.

    THE RACE THIS CLOSES. RETAIN_DONE_RECENT is 0, so a 'done' job is dropped
    from ollama-queue-state.json on the very NEXT daemon tick -- typically before
    the fire-and-forget gate subprocess (_fire_gate_on_complete, below) has read
    it. gate-on-complete.py then finds no exit_code and no launch_baseline in the
    live state and abstains (not_checked = verify-exit, launch-baseline), which
    demotes an otherwise-clean pass and forces a needless 27B re-gate escalation
    (job 7699d60b92ec, 2026-09-08). This file is written from the still-complete
    job dict at fire time and is NEVER pruned, so gate-on-complete.py's readers
    (launch_baseline / job_verify_exit / job_facts) can fall back to it and
    CERTIFY those two facts instead of marking them not_checked.

    Best-effort and non-raising (rule 2): a failure here writes a warning and
    leaves the dispatch untouched -- the gate simply abstains as it did before."""
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        rec = {k: job.get(k) for k in (
            "id", "label", "model", "cwd", "verify", "status", "exit_code",
            "launch_baseline", "verify_failed_at_baseline", "scored_arm",
            "preflight", "launched_by", "launched_by_session")}
        rec["persisted_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        # Atomic write (tmp + os.replace), same discipline as _Locked.save: the
        # gate reads this sidecar concurrently and a kill mid-write must never
        # leave a truncated JSON that the gate's json.loads would choke on.
        dest = LOG_DIR / f"{job.get('id')}.done.json"
        tmp = dest.with_name(dest.name + ".tmp")
        tmp.write_text(json.dumps(rec, indent=1))
        os.replace(tmp, dest)
    except Exception as e:
        print(f"[queue] completion-record write failed for {job.get('id')}: {e}",
              file=sys.stderr)


def _fire_gate_on_complete(job):
    """Advisory auto-gate hook (2026-08-31, Penn's "wire the gate to run automatically").
    Fire-and-forget the Studio gate on a completed job. gate-on-complete.py runs the
    decidable checks (scope/completeness/verify-quality) inline and ENQUEUES the model
    review as a --runner job -- so the GPU work serialises through the queue and respects
    the VRAM guard, never a direct concurrent ollama call. Wrapped so it can NEVER raise
    into or block the daemon: the gate is commentary, the dispatch is the real work. The
    gate- label loop-guard (a gate job merges into its parent instead of re-gating) lives
    inside gate-on-complete.py."""
    # Image renders (Pet Portrait Studio et al.) produce no code diff to review --
    # skip the gate entirely so we don't queue a pointless gate-* job after each render.
    _lbl = str(job.get("label", ""))
    # Image renders and draft jobs (ollama-dispatch-draft, which only writes the verify
    # fixture) produce no code diff and have NO review to merge -- skip the hook entirely.
    if (str(job.get("model", "")).lower() == "image"
            or _lbl.startswith("pet-") or _lbl.startswith("draft-")):
        return
    # gate-/regate- jobs are the gate's OWN output. They carry no diff to persist and must
    # NOT be re-gated (no sidecar, no _should_gate_job) -- BUT they MUST still re-invoke the
    # hook: the subprocess.Popen below is the ONLY thing that routes a completed review job
    # into gate-on-complete.py's merge_review (folds the review into the parent .gate.json,
    # recomputes the verdict, escalates to the 27B, re-runs signoff). The old early-return
    # here skipped the Popen too, silently disconnecting two-tier review for EVERY dispatch
    # (post-restart gate-5c3da4998e57/gate-7699d60b92ec finished exit 0 but their parents
    # stayed review=pending). So: only persist + gate-decide for a real dispatch job.
    _is_gate_job = _lbl.startswith("gate-") or _lbl.startswith("regate-")
    if not _is_gate_job:
        # Chain STEP (not the final): the chain is gated once, on its chain_final job.
        if not _should_gate_job(job):
            return
        # Freeze the durable completion record BEFORE launching the gate: the gate
        # reads it, and the next tick will prune this job from live state.
        _persist_job_completion(job)
    try:
        subprocess.Popen(["python3", str(Path.home() / "bin" / "gate-on-complete.py"),
                          "--job-id", str(job.get("id", "")),
                          "--job-label", str(job.get("label", "")),
                          "--cwd", str(job.get("cwd", "")),
                          "--task-file", str(job.get("task_file", "")),
                          "--verify", str(job.get("verify") or "")])
    except Exception as e:
        print(f"[queue] gate-on-complete launch failed for {job.get('id')}: {e}", file=sys.stderr)


def _parse_resume_transcript(log_path):
    """Extract the resumable transcript path from a worker log file, or None if the
    marker line isn't there (log missing/unreadable, or the run never paused cleanly)."""
    if not log_path:
        return None
    try:
        text = Path(log_path).read_text(errors="replace")
    except OSError:
        return None
    m = RESUMABLE_TRANSCRIPT_RE.search(text)
    return m.group(1).strip() if m else None


def _read_pause_info(transcript_path):
    """Read the machine-readable pause classification ollama-worker.py wrote into a
    resumable transcript (see _save_transcript's pause_reason/pause_meta) -- used by the
    auto-resume watchdog below to decide WHETHER a paused job is safe to auto-bump-and-retry
    at all. Returns (reason, meta), both None/{} if the transcript is missing, unreadable, or
    predates this field (old transcripts have no pause_reason key at all -- treated the same
    as an unknown reason, which the watchdog conservatively never auto-resumes)."""
    if not transcript_path:
        return None, {}
    try:
        data = json.loads(Path(transcript_path).read_text())
    except (OSError, ValueError):
        return None, {}
    return data.get("pause_reason"), data.get("pause_meta") or {}


def _pid_alive(pid):
    if not pid:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _urls_for_lane(lane_name):
    """All concrete host URLs that resolve to this lane via _lane_name() --
    e.g. "studio" covers both the llama-server bypass port (8091) and
    Studio's native-Ollama port (11434), since they share one physical
    64GB memory pool. Kept in sync with _lane_name()'s own mapping."""
    urls = [LLAMA_SERVER_QWEN38_URL] if lane_name == "studio" else []
    w = worker()
    for name, spec in w.KNOWN_OLLAMA_HOSTS.items():
        if name == lane_name:
            urls.append(spec["url"])
    return urls


def _external_dispatch_running(lane_name):
    """True if some ollama-worker.py process -- ours or a legacy manual
    chain -- is already making requests against ANY url sharing this lane.

    Added 2026-08-29 after a real collision: this used to take a single
    host_url and pgrep for just that string, so checking whether Studio's
    native-Ollama lane (11434) was free never matched a manual dispatch
    running against the llama-server bypass (8091) on the SAME physical
    box -- _lane_name() already knew the two share one lane, but this
    function didn't consult it. A queued job landed on Studio concurrently
    with an in-flight manual --resume chain as a result. Now checks pgrep
    across every url _urls_for_lane() returns for the lane, not just one."""
    try:
        urls = _urls_for_lane(lane_name)
        if not urls:
            return False
        pattern = "ollama-worker.py.*(" + "|".join(re.escape(u) for u in urls) + ")"
        out = subprocess.run(
            ["pgrep", "-f", pattern],
            capture_output=True, text=True,
        )
        return bool(out.stdout.strip())
    except Exception:
        return False


def _lane_name(url):
    # The llama-server qwen3.8 bypass shares Studio's physical box and its
    # 64GB unified-memory pool with Studio's native Ollama endpoint -- they
    # must be mutually exclusive, so it's deliberately named into the same
    # "studio" lane rather than getting a lane of its own (see the OOM
    # incident in the LLAMA_SERVER_QWEN38_URL comment above).
    if url == LLAMA_SERVER_QWEN38_URL:
        return "studio"
    w = worker()
    for name, spec in w.KNOWN_OLLAMA_HOSTS.items():
        if spec["url"] == url:
            return name
    return url


def _studio_ollama_resident_models():
    """Models currently resident (loaded) in Studio's native Ollama, via
    /api/ps. Used to clear the way before a llama-server load that needs
    the same box's full unified-memory budget."""
    w = worker()
    url = w.KNOWN_OLLAMA_HOSTS["studio"]["url"]
    try:
        req = urllib.request.Request(f"{url}/api/ps")
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        return [m["name"] for m in data.get("models", [])]
    except Exception as e:
        print(f"[queue] WARNING: couldn't query {url}/api/ps to check for resident "
              f"models ({e}) -- proceeding without eviction", file=sys.stderr)
        return []


def _evict_studio_ollama_models(exclude_model=None):
    """keep_alive:0 every Studio-resident Ollama model except exclude_model.
    Best-effort -- a failed evict just means the next launch may OOM and
    surface its own clear error, not a silent hang."""
    w = worker()
    url = w.KNOWN_OLLAMA_HOSTS["studio"]["url"]
    for name in _studio_ollama_resident_models():
        if name == exclude_model:
            continue
        try:
            req = urllib.request.Request(
                f"{url}/api/generate",
                data=json.dumps({"model": name, "keep_alive": 0}).encode(),
                headers={"Content-Type": "application/json"}, method="POST",
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                resp.read()
            print(f"[queue] evicted {name} from Studio's native Ollama to free memory")
        except Exception as e:
            print(f"[queue] WARNING: failed to evict {name} from Studio Ollama: {e}", file=sys.stderr)


def _evict_template_bug_models():
    """Self-healing safeguard, independent of ollama-worker.py's own dispatch flow: if any
    model in TEMPLATE_BUG_MODELS (confirmed to crash Ollama's native chat template --
    github.com/ollama/ollama/issues/17778 -- qwen3.8:27b-q8_0/devstral, see that constant's
    own comment) ever shows up resident on Studio's NATIVE Ollama, evict it immediately.

    Added 2026-08-29 after a real, confirmed incident: qwen3.8:27b-q8_0 (37GB) was found
    resident on native Ollama via an active process that traced back to an `ollama-mcp` MCP
    tool call, NOT a dispatch through ollama-worker.py -- which means it completely bypassed
    the TEMPLATE_BUG_MODELS guard in ensure_model_ready(), since that guard only lives inside
    THIS harness's own dispatch path, not in Ollama itself or any other caller (an MCP tool,
    another session, a stray curl command) that can reach Ollama's API directly. That guard
    protects ollama-worker.py's own dispatches; this one protects the shared physical resource
    (Studio's 64GB pool) regardless of which caller loaded the model, since ANY caller doing
    this risks the exact double-load-memory-collision this whole class of fix exists to
    prevent -- see the llama-server-bypass-vs-native-Ollama OOM incident this same night.

    Called once per poll cycle -- cheap (one /api/ps call, usually returns nothing to do) and
    self-healing: it doesn't matter who or what loaded the model, it gets freed within one poll
    interval regardless. Does NOT replace the ensure_model_ready() guard (that one still stops
    OUR OWN dispatches before they even try) -- this is the backstop for everyone else.

    Evicts ONLY the specific forbidden model(s) found, not every model resident on the host --
    fixed 2026-08-29 after an independent review caught the first version calling
    _evict_studio_ollama_models() with no exclude_model, which per that function's own
    docstring evicts EVERYONE resident. That would have unloaded a legitimate, concurrently-
    running dispatch's own model out from under it the moment this watchdog fired -- a real
    self-inflicted collision from the fix meant to prevent collisions. Model names are compared
    normalized only (no separate raw-string fallback -- adding one back would only ever widen
    matching, not narrow it, so it can't fix a real gap; keeping the comparison singular here
    is just clearer, not a behavior change)."""
    w = worker()
    bug_models = getattr(w, "TEMPLATE_BUG_MODELS", None)
    if not bug_models:
        return
    bug_models_normalized = {w._normalize_model_name(m) for m in bug_models}
    url = w.KNOWN_OLLAMA_HOSTS["studio"]["url"]
    for name in _studio_ollama_resident_models():
        if w._normalize_model_name(name) not in bug_models_normalized:
            continue
        print(f"[queue] WATCHDOG: {name} found resident on Studio's native Ollama -- this "
              f"model is in TEMPLATE_BUG_MODELS and must never load there (crashes Ollama's "
              f"native chat template, and risks a memory collision with the llama-server "
              f"bypass). Evicting just this model, not the whole host, so any other "
              f"legitimately-running dispatch on Studio is left alone.")
        try:
            req = urllib.request.Request(
                f"{url}/api/generate",
                data=json.dumps({"model": name, "keep_alive": 0}).encode(),
                headers={"Content-Type": "application/json"}, method="POST",
            )
            with urllib.request.urlopen(req, timeout=15) as resp:
                resp.read()
            print(f"[queue] WATCHDOG: evicted {name}")
        except Exception as e:
            print(f"[queue] WATCHDOG WARNING: failed to evict {name}: {e}", file=sys.stderr)


# Auto-resume watchdog (added 2026-08-29, Penn's request: "I want you to be able to handle
# it autonomously so jobs don't just hang endlessly"). A paused job used to require a human
# or Claude to notice the pause, judge whether more room is warranted, and manually bump
# --num-ctx/--max-iters before resuming -- confirmed live tonight (wire-live-log-queue paused
# at 93% of 32768, sat there until manually caught and bumped to 65536). This automates
# exactly that judgment call, but only for the two pause reasons where "give it more room and
# try again" is actually the right response, and only within hard bounds so it can't turn a
# genuinely stuck job into a silent, unbounded resource drain.
#
# Deliberately conservative on what it will touch:
#   - "external_sigterm" pauses are NEVER auto-resumed. That pause was somebody's (or the
#     promote flow's) deliberate stop -- auto-resuming it would silently undo a real decision,
#     exactly the kind of thing the pause/resume/promote system exists to make explicit.
#   - Any unrecognized/missing pause_reason (old transcripts predate this field, or a future
#     pause path forgets to set it) is also left untouched -- unknown is treated as "don't
#     know it's safe," not "assume it's safe."
#   - Each job gets at most AUTO_RESUME_MAX_BUMPS auto-bumps total (a persistent per-job
#     counter on the job dict, surviving across daemon restarts since it's in state.json).
#     Past that it's left paused with a clear log line -- a job that needs 4 bumps almost
#     certainly has a real problem an ever-bigger budget won't fix, and this is exactly the
#     "give up and surface it" backstop for that case.
#   - context_threshold bumps are capped by a hard per-lane ceiling, not doubled forever:
#     Unraid's ceiling is UNRAID_CONFIRMED_SAFE_CTX's own per-model VRAM-safe value (the exact
#     same hard wall clamp_unraid_ctx() already enforces on every dispatch) -- since that's
#     already the max safe value, a paused Unraid job that's already there literally cannot be
#     helped by bumping and is left for a human. Studio's ceiling is a generous fixed value
#     (AUTO_RESUME_STUDIO_CTX_CEILING) reflecting its 64GB unified pool, not doubled past that.
#   - request_more_iterations bumps by what the model itself asked for (pause_meta's
#     "requested_additional", the same number a human reviewing the log would grant), floored
#     at a small default if that's missing, and capped at AUTO_RESUME_MAX_ITERS total.
AUTO_RESUME_MAX_BUMPS = 3
AUTO_RESUME_STUDIO_CTX_CEILING = 131072
AUTO_RESUME_MAX_ITERS = 60
AUTO_RESUME_MIN_ITER_BUMP = 5


# --- Auto num-ctx sizing + auto split/recombination (Penn 2026-09-08) ----------
# Replaces the two wrong defaults (worker's flat DEFAULT_NUM_CTX and the hand-passed
# 65536) with a computed START value. Under-estimating is SAFE: ollama-queue.py's
# auto-resume bump (see AUTO_RESUME_* above, ~line 750) doubles num_ctx UPWARD and
# re-queues a job that pauses on the context threshold, so sizing only picks the
# starting bucket. An explicit `--num-ctx N` always wins and disables all of this.
#
# chars->tokens: mirrors ollama-worker.py's inline `len(text) // 4` (there is no
# named constant there; kept in sync here by value, verified against that file).
CTX_CHARS_PER_TOKEN = 4
# Safe start buckets (ascending). The top bucket matches the value we used to pass
# by hand (65536). We snap the headroomed estimate UP to the smallest bucket that
# covers it, then clamp to the host ceiling.
CTX_BUCKETS = (16384, 32768, 49152, 65536)
# Headroom over the raw estimate for agentic growth: iteration 2+ carries iteration
# 1's full context forward (a strictly bigger prompt than the single-shot estimate),
# which is exactly the mechanism the UNRAID_CONFIRMED_SAFE_CTX comment documents.
CTX_HEADROOM_PCT = 40
# Historical signal: prior CONVERGED dispatches whose task_chars is within this
# multiplicative window of the new task count as "similar size". p90 of their
# peak_total_tokens is used as a floor on the estimate. Needs a quorum or it's noise.
CTX_HIST_BUCKET_RATIO = 1.5
CTX_HIST_MIN_SAMPLES = 3
# Cheap named-file detection bounds (best-effort, never fatal).
CTX_NAMED_FILES_MAX = 25
CTX_NAMED_FILES_MAX_TOTAL_CHARS = 2_000_000
_CTX_CODE_FILE_RE = re.compile(
    r'([\w./\-]+\.(?:py|js|ts|tsx|jsx|mjs|cjs|json|md|txt|sh|bash|go|rs|java|kt|'
    r'c|cc|cpp|cxx|h|hpp|rb|php|cs|swift|css|scss|html|htm|xml|yml|yaml|toml|ini|sql))\b'
)

# --- Investigation/diagnosis context FLOOR (Penn 2026-09-08) --------------------
# A diagnosis dispatch (a model reading a repo to write DIAGNOSIS.md) reads many
# files and accumulates tool output, so a small window walls it: a real one thrashed
# -- re-issuing near-identical greps against the same files -- and PAUSED at iteration
# 18/30 at 92% context on num_ctx=32768, without converging. Unlike a bounded coding
# fix (one target file, a verify to converge on), an investigation's context grows with
# how much of the tree it has to read, so it needs a floor on its starting window. This
# is applied at enqueue and, unlike the auto-ctx sizing below it, fires on BOTH the
# explicit --num-ctx path (the incident used --num-ctx 32768) and the auto-computed one.
DIAGNOSIS_CTX_FLOOR = 49152


def _dispatch_is_investigation(task_text, verify, task_kind=None):
    """Robust signal that a dispatch is an investigation/diagnosis (read a repo to
    produce findings) rather than a bounded code edit. True when ANY of:
      * the task text names DIAGNOSIS.md (the standard diagnosis deliverable), or
      * the verify command greps for / names DIAGNOSIS.md (a verify-side signal that
        survives a terse task file), or
      * the task text uses a diagnosis/diagnose/diagnostic word AND asks for a written
        finding (report/root cause/investigate/write-up) -- the two together, so a
        coding task that merely mentions a "diagnostic message" string is NOT caught.

    Deliberately does NOT key on task_kind alone: a diagnosis is normally enqueued as
    task_kind=coding with a DIAGNOSIS.md deliverable, and 'research' is a distinct kind
    with its own budget path. task_kind is accepted only to stay call-compatible.
    """
    hay = task_text or ""
    v = verify or ""
    if "DIAGNOSIS.md" in hay or "DIAGNOSIS.md" in v:
        return True
    if re.search(r"\bdiagnos(?:e|is|tic|tics|ing|ed)\b", hay, re.IGNORECASE) and \
       re.search(r"\b(?:root cause|investigat\w*|report|findings?|write[- ]?up|"
                 r"analy[sz]e|analysis)\b", hay, re.IGNORECASE):
        return True
    return False


def apply_diagnosis_ctx_floor(num_ctx, is_investigation, ceiling,
                              floor=DIAGNOSIS_CTX_FLOOR):
    """Pure decision for the investigation/diagnosis context floor.

    Returns (new_num_ctx, action) where action is one of:
      "unchanged"  -- not an investigation, or already at/above the floor (a normal
                      bounded coding fix always lands here, so it is never touched),
      "raised"     -- below the floor and the host ceiling allows raising it,
      "warn"       -- below the floor but the host ceiling won't allow raising it.
    Kept pure (no I/O) so both the detection and the flooring are unit-testable and
    prove red-on-revert."""
    if num_ctx is None or not is_investigation or num_ctx >= floor:
        return num_ctx, "unchanged"
    floored = min(floor, ceiling)
    if floored > num_ctx:
        return floored, "raised"
    return num_ctx, "warn"


def _percentile(sorted_vals, pct):
    """Nearest-rank percentile of an already-sorted list (ceil convention: the
    conservative choice for a headroom floor). None for an empty list."""
    n = len(sorted_vals)
    if n == 0:
        return None
    k = -(-pct * n // 100)  # ceil(pct/100 * n), integer-only
    k = max(1, min(k, n))
    return sorted_vals[k - 1]


def historical_p90_tokens(task_chars, metrics_path,
                          ratio=CTX_HIST_BUCKET_RATIO, min_samples=CTX_HIST_MIN_SAMPLES):
    """p90 of peak_total_tokens over CONVERGED prior dispatches whose task_chars is
    within [task_chars/ratio, task_chars*ratio]. None when the file is missing or
    fewer than min_samples similar-size samples exist (too little to trust)."""
    try:
        text = Path(metrics_path).read_text()
    except OSError:
        return None
    if task_chars <= 0:
        return None
    lo, hi = task_chars / ratio, task_chars * ratio
    vals = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except ValueError:
            continue
        if rec.get("status") != "converged":
            continue
        tc = rec.get("task_chars")
        pt = rec.get("peak_total_tokens")
        if not isinstance(tc, (int, float)) or not isinstance(pt, (int, float)):
            continue
        if lo <= tc <= hi:
            vals.append(pt)
    if len(vals) < min_samples:
        return None
    return _percentile(sorted(vals), 90)


def named_files_chars(task_text, cwd,
                      max_files=CTX_NAMED_FILES_MAX,
                      max_total=CTX_NAMED_FILES_MAX_TOTAL_CHARS):
    """Best-effort char count of the code files the task NAMES that actually exist
    under cwd. The model will likely read these, so their size feeds the estimate.
    Bounded and never fatal -- returns 0 on any trouble."""
    if not cwd:
        return 0
    try:
        cwd = Path(cwd)
    except Exception:
        return 0
    seen = set()
    total = 0
    for m in _CTX_CODE_FILE_RE.finditer(task_text):
        rel = m.group(1).lstrip("./")
        if rel in seen:
            continue
        seen.add(rel)
        if len(seen) > max_files:
            break
        try:
            p = (cwd / rel)
            if p.is_file():
                total += p.stat().st_size
        except Exception:
            continue
        if total >= max_total:
            return max_total
    return total


def estimate_task_tokens(task_text, cwd, metrics_path,
                         chars_per_token=CTX_CHARS_PER_TOKEN):
    """Estimate the peak prompt tokens a dispatch of this task will reach.
    Returns (est_tokens, info_dict). The estimate is the MAX of a char-based
    lower bound (task + named files) and the historical p90 for similar tasks."""
    task_chars = len(task_text)
    nf_chars = named_files_chars(task_text, cwd)
    char_est = (task_chars + nf_chars) // max(1, chars_per_token)
    hist_p90 = historical_p90_tokens(task_chars, metrics_path)
    est = max(char_est, int(hist_p90 or 0))
    return est, {
        "task_chars": task_chars,
        "named_file_chars": nf_chars,
        "char_est_tokens": char_est,
        "hist_p90_tokens": hist_p90,
    }


def _snap_to_bucket(value, buckets):
    for b in buckets:
        if value <= b:
            return b
    return buckets[-1]


def size_num_ctx(est_tokens, ceiling, buckets=CTX_BUCKETS, headroom_pct=CTX_HEADROOM_PCT):
    """Turn a raw token estimate into a start num_ctx.
    Returns (chosen_num_ctx, overflow, target_tokens):
      * target = est + headroom (agentic growth)
      * chosen = smallest bucket >= target that also fits the host ceiling, clamped
      * overflow = True iff target exceeds the largest usable bucket (job won't fit
        even at the top bucket -> the split path may trigger)."""
    target = (est_tokens * (100 + headroom_pct)) // 100
    usable = [b for b in buckets if b <= ceiling]
    if not usable:
        # Ceiling is below even the smallest bucket (e.g. a low Unraid per-model
        # cap): clamp to the ceiling itself. Always overflow.
        return min(buckets[0], ceiling), True, target
    top = usable[-1]
    overflow = target > top
    chosen = min(_snap_to_bucket(target, usable), ceiling)
    return chosen, overflow, target


def resolve_ctx_ceiling(host_pref, model):
    """The largest num_ctx the START host will tolerate. For an explicit unraid
    host with a confirmed-safe per-model cap, that cap; otherwise Studio's generous
    fixed ceiling. `auto` uses the Studio ceiling -- if the job later routes to
    Unraid, ollama-worker.py's clamp_unraid_ctx() hard-caps it there anyway, so we
    never exceed a real limit; this only governs bucket selection."""
    try:
        w = worker()
    except Exception:
        return AUTO_RESUME_STUDIO_CTX_CEILING
    if host_pref == "unraid":
        safe = w.UNRAID_CONFIRMED_SAFE_CTX.get(model)
        if safe is not None:
            return safe
    return AUTO_RESUME_STUDIO_CTX_CEILING


def decide_split(overflow, auto_split_flag, no_split_flag):
    """Decision path (opt-in, guarded):
      * --no-split          -> never split.
      * explicit --num-ctx  -> handled by the caller (auto logic skipped entirely).
      * --auto-split        -> split IF a clean decomposition exists.
      * otherwise           -> split ONLY when the estimate provably overflows the
                               top usable bucket (the job genuinely won't fit).
    Returns (want_split: bool, reason: str). A True here is still contingent on
    decompose_task() actually finding >=2 independent targets."""
    if no_split_flag:
        return False, "--no-split: splitting disabled"
    if auto_split_flag:
        return True, "--auto-split requested"
    if overflow:
        return True, "estimate overflows top bucket -- job will not fit even at max ctx"
    return False, "fits in a single bucket"


_CTX_SUBTASK_HEADER_RE = re.compile(
    r'^#{1,6}\s*(?:sub-?tasks?|targets?|split|slices?|independent\s+targets?)\b',
    re.IGNORECASE)
_CTX_ANY_HEADER_RE = re.compile(r'^#{1,6}\s+\S')
_CTX_LIST_ITEM_RE = re.compile(r'^\s*(?:[-*+]|\d+[.)])\s+(.*\S)')


def decompose_task(task_text):
    """Split a multi-target coding task into independent sub-specs, one per
    INDEPENDENT TARGET (per-file / per-clearly-separable-subtask named in the task).

    Strategy (deterministic, conservative):
      1. Find a '## Sub-tasks' / '## Targets' / '## Split' section and treat each
         bullet or numbered item under it as one target. The text BEFORE that
         section is the shared preamble prepended to every sub-spec.
      2. Fallback: if there is no such section, look for >=2 top-level '##' sections
         that each name a distinct code file; each becomes a sub-spec.
    Returns a list of {"title", "body"} dicts, or [] when no clean decomposition
    exists (caller must then NOT split -- run as one job and let auto-resume grow
    the ctx instead of guessing at unsafe boundaries)."""
    lines = task_text.splitlines()

    # --- Strategy 1: explicit sub-tasks/targets section ---
    hdr_idx = None
    for i, ln in enumerate(lines):
        if _CTX_SUBTASK_HEADER_RE.match(ln):
            hdr_idx = i
            break
    if hdr_idx is not None:
        preamble = "\n".join(lines[:hdr_idx]).strip()
        items = []
        for ln in lines[hdr_idx + 1:]:
            if _CTX_ANY_HEADER_RE.match(ln):
                break  # next section ends the list
            m = _CTX_LIST_ITEM_RE.match(ln)
            if m:
                items.append(m.group(1).strip())
        if len(items) >= 2:
            return [_ctx_make_subspec(preamble, idx, len(items), item)
                    for idx, item in enumerate(items)]

    # --- Strategy 2: multiple H2 sections each naming a distinct file ---
    sections = []  # (title, body_lines)
    cur = None
    for ln in lines:
        if re.match(r'^##\s+\S', ln):
            if cur:
                sections.append(cur)
            cur = [ln, []]
        elif cur:
            cur[1].append(ln)
    if cur:
        sections.append(cur)
    file_sections = []
    for title_line, body in sections:
        blob = title_line + "\n" + "\n".join(body)
        files = {m.group(1).lstrip("./") for m in _CTX_CODE_FILE_RE.finditer(blob)}
        if len(files) == 1:
            file_sections.append((title_line.lstrip("# ").strip(), blob.strip(), next(iter(files))))
    distinct_files = {f for _, _, f in file_sections}
    if len(file_sections) >= 2 and len(distinct_files) >= 2:
        preamble = ""
        first_idx = task_text.find("##")
        if first_idx > 0:
            preamble = task_text[:first_idx].strip()
        out = []
        for idx, (title, blob, _f) in enumerate(file_sections):
            body = blob if not preamble else preamble + "\n\n" + blob
            out.append({"title": title or f"target {idx + 1}", "body": body})
        return out

    return []


def _ctx_make_subspec(preamble, idx, total, item):
    title = re.sub(r'\s+', ' ', item).strip()
    # Keep the title short for labels; full item text stays in the body.
    short = title if len(title) <= 60 else title[:57] + "..."
    header = (f"## This sub-task ({idx + 1} of {total})\n\n"
              f"You are handling ONE independent target of a larger, split task. "
              f"Make ONLY the change for this target in the shared worktree; the "
              f"other targets are handled by sibling sub-tasks. Do NOT touch the "
              f"others' files.\n\n**Target:** {item}\n")
    body = (preamble + "\n\n" + header) if preamble else header
    return {"title": short, "body": body}



# Progress gate for the auto-resume bump (2026-08-31). Measured, not guessed.
#
# Mined 1396 worker transcripts. Iterations -- not context -- are the binding
# constraint: 161 runs died on the iteration cap versus THREE context-threshold
# pauses in the whole corpus. And of 146 cap-hitting non-converged runs, 137 were
# still doing VARIED work when the budget cut them off. They were starved, not
# stuck, so extending them is usually right.
#
# But a LOOPING run given more iterations just burns GPU -- that is what the
# resell #310 dispatch did three times. So extend by default and refuse only on
# unambiguous looping, since wrongly refusing starves a run that was working.
#
# Two signals, both required, measured over 184 cap-hitting runs:
#   variety  = distinct tool calls / total, over the last third of the run
#   repeats  = the most common single call's count in that window
# variety < 0.5 AND repeats >= 3 fires on 7/184 = 3.8%. Every low-variety run
# also had a repeat, so requiring both costs nothing in recall and buys
# specificity. p05 of the variety distribution is 0.50, so this is the bottom
# ~4% -- deliberately conservative.
LOOP_VARIETY_MAX = 0.5
LOOP_REPEAT_MIN = 3


def _transcript_is_looping(transcript_path):
    """(is_looping, detail). False whenever we cannot tell -- never refuse blind."""
    if not transcript_path:
        return False, "no transcript"
    try:
        data = json.loads(Path(transcript_path).read_text())
    except (OSError, ValueError):
        return False, "transcript unreadable"
    calls = []
    for m in data.get("messages") or []:
        for tc in (m.get("tool_calls") or []):
            fn = tc.get("function") or {}
            calls.append(f"{fn.get('name')}:{str(fn.get('arguments'))[:150]}")
    if len(calls) < 6:
        return False, f"only {len(calls)} tool call(s) -- too few to judge"
    tail = calls[-max(5, len(calls) // 3):]
    variety = len(set(tail)) / len(tail)
    top_call, top_n = Counter(tail).most_common(1)[0]
    if variety < LOOP_VARIETY_MAX and top_n >= LOOP_REPEAT_MIN:
        return True, (f"variety {variety:.2f} over the last {len(tail)} calls with one "
                      f"call repeated {top_n}x: {top_call[:90]}")
    return False, f"variety {variety:.2f}, max repeat {top_n}x -- still doing varied work"


def _auto_resume_paused_jobs():
    w = worker()
    with _Locked() as lock:
        state = lock.load()
        changed = False
        for job in state["jobs"]:
            if job.get("status") != "paused":
                continue
            reason = job.get("pause_reason")
            if reason not in ("context_threshold", "request_more_iterations"):
                continue  # external_sigterm, or unknown/missing -- never auto-touch
            bumps = job.get("auto_resume_count", 0)
            if bumps >= AUTO_RESUME_MAX_BUMPS:
                if not job.get("_auto_resume_gaveup_logged"):
                    print(f"[queue] AUTO-RESUME: {job['id']} ({job['label']}) has already been "
                          f"auto-bumped {bumps} times and paused again -- leaving it paused for "
                          f"manual review rather than bumping further.")
                    job["_auto_resume_gaveup_logged"] = True
                    changed = True
                continue

            meta = job.get("pause_meta") or {}
            if reason == "context_threshold":
                lane = job.get("lane") or job.get("host_pref")
                if lane == "unraid":
                    ceiling = w.UNRAID_CONFIRMED_SAFE_CTX.get(job["model"])
                else:
                    ceiling = AUTO_RESUME_STUDIO_CTX_CEILING
                current = job["num_ctx"]
                new_ctx = min(current * 2, ceiling) if ceiling else current * 2
                if not ceiling or new_ctx <= current:
                    print(f"[queue] AUTO-RESUME: {job['id']} ({job['label']}) paused on context "
                          f"threshold but is already at its host's safe ceiling ({current}) -- "
                          f"cannot help by bumping, leaving paused for manual review.")
                    continue
                job["num_ctx"] = new_ctx
                job["auto_resume_count"] = bumps + 1
                job["status"] = "pending"
                print(f"[queue] AUTO-RESUME: {job['id']} ({job['label']}) paused on context "
                      f"threshold ({meta.get('tokens_used')}/{meta.get('num_ctx')} tokens) -- "
                      f"bumping --num-ctx {current} -> {new_ctx} and re-queueing "
                      f"(bump {bumps + 1}/{AUTO_RESUME_MAX_BUMPS}).")
                changed = True
            else:  # request_more_iterations
                # Do not buy more iterations for a run that is going in circles.
                _looping, _why = _transcript_is_looping(
                    _parse_resume_transcript(job.get("log_path")))
                if _looping:
                    print(f"[queue] AUTO-RESUME REFUSED: {job['id']} ({job['label']}) asked for "
                          f"more iterations but is LOOPING -- {_why}. More budget would repeat "
                          f"the same calls; leaving paused for manual review.")
                    job["_auto_resume_gaveup_logged"] = True
                    changed = True
                    continue
                # None means "worker default governed the run" -- resolve it for the bump math.
                current = job["max_iters"] if job["max_iters"] is not None else WORKER_DEFAULT_MAX_ITERS
                requested = meta.get("requested_additional") or 0
                bump = max(requested, AUTO_RESUME_MIN_ITER_BUMP)
                new_iters = min(current + bump, AUTO_RESUME_MAX_ITERS)
                if new_iters <= current:
                    print(f"[queue] AUTO-RESUME: {job['id']} ({job['label']}) requested more "
                          f"iterations but is already at the {AUTO_RESUME_MAX_ITERS}-iteration "
                          f"auto-resume ceiling -- leaving paused for manual review.")
                    continue
                job["max_iters"] = new_iters
                job["auto_resume_count"] = bumps + 1
                job["status"] = "pending"
                print(f"[queue] AUTO-RESUME: {job['id']} ({job['label']}) model requested "
                      f"+{requested} more iterations -- bumping --max-iters {current} -> "
                      f"{new_iters} and re-queueing (bump {bumps + 1}/{AUTO_RESUME_MAX_BUMPS}).")
                changed = True
        if changed:
            lock.save(state)


def _stop_llama_server_bypass():
    """The mirror image of _evict_studio_ollama_models(): that function frees
    Ollama-resident memory before the llama-server bypass claims the lane;
    this one frees the bypass's memory before a NATIVE Ollama job claims it.
    Without this a native job launched right after a bypass job left the
    bypass server (qwen3.8, 29GB) resident and running -- confirmed live
    2026-08-28, a deepseek retest OOM'd Ollama's own internal llama-server
    exactly like the first incident, just in the opposite direction. Unlike
    Ollama, a raw llama-server process has no keep_alive/unload concept --
    the only way to free its memory is to kill the process outright. Doesn't
    delete the pid file: the start script always overwrites it fresh on next
    launch, and leaving a stale one around is a harmless, honest record of
    the last pid used, not a state this function needs to own."""
    if not LLAMA_SERVER_QWEN38_PID_FILE.is_file():
        return
    try:
        pid = int(LLAMA_SERVER_QWEN38_PID_FILE.read_text().strip())
    except (ValueError, OSError):
        return
    if not _pid_alive(pid):
        return
    try:
        os.kill(pid, signal.SIGTERM)
        print(f"[queue] stopped llama-server bypass (pid {pid}) to free memory for a native Ollama job")
    except OSError as e:
        print(f"[queue] WARNING: failed to stop llama-server bypass pid {pid}: {e}", file=sys.stderr)


def _ensure_llama_server_up(ctx_size):
    """Health-check the qwen3.8 llama-server bypass; (re)launch it via the
    vetted start script if it's not responding. The script itself blocks
    until healthy (or 90s timeout) and backgrounds the actual server
    process, so a successful return here means it's ready to take requests."""
    try:
        req = urllib.request.Request(f"{LLAMA_SERVER_QWEN38_URL}/health")
        with urllib.request.urlopen(req, timeout=5) as resp:
            if resp.status == 200:
                return True
    except Exception:
        pass
    if not LLAMA_SERVER_START_SCRIPT.is_file():
        print(f"[queue] ERROR: llama-server not responding at {LLAMA_SERVER_QWEN38_URL} and "
              f"start script missing at {LLAMA_SERVER_START_SCRIPT}", file=sys.stderr)
        return False
    print(f"[queue] llama-server not up -- launching via {LLAMA_SERVER_START_SCRIPT} (ctx={ctx_size})")
    try:
        result = subprocess.run(
            ["bash", str(LLAMA_SERVER_START_SCRIPT), str(ctx_size)],
            capture_output=True, text=True, timeout=120,
        )
        if result.returncode != 0:
            print(f"[queue] ERROR: llama-server start script failed: {result.stdout}\n{result.stderr}",
                  file=sys.stderr)
            return False
        return True
    except Exception as e:
        print(f"[queue] ERROR: llama-server start script raised: {e}", file=sys.stderr)
        return False


def _validate_host(host):
    """--host must be a routing keyword or an explicit URL; anything else is
    almost certainly a typo that would otherwise silently fall through to
    auto-routing."""
    if host in ("auto", "studio", "unraid"):
        return
    if host.startswith("http://") or host.startswith("https://"):
        return
    sys.exit(f"invalid --host {host!r}: must be auto, studio, unraid, or an explicit http(s) URL")


VERIFY_TIMEOUT_S = 300   # mirrors ollama-worker.py's verify subprocess timeout
PREFLIGHT_SLOW_S = 240   # refuse if the verify eats most of that budget (would time out in the worker)


def _preflight_verify(verify: str, cwd: Path) -> bool:
    """Returns True when the verify FAILED at baseline (before any model edit).

    That return value is the authoritative baseline reading: it is taken in the
    job's own cwd, at enqueue, before the model touches anything. The worker
    needs it to know that a still-failing verify proves nothing.
    """
    """Smoke-test + time the --verify command against cwd BEFORE enqueuing
    (feedback_dispatch_preflight_verify). A verify that can't EXECUTE (missing tool / shell
    error) or runs longer than the worker's 300s verify gate doesn't gate anything -- it just
    fails the job regardless of the model's work AND makes the model thrash trying to fix a
    phantom failure (burning MAX_COMPLETION_CLAIMS). A plain NON-ZERO exit is fine and only
    reported -- a bug-fix whose verify checks the FIX fails at baseline by design; we refuse
    only when it can't run or is too slow. Running it here also warms npx/tool caches so the
    worker's later run is fast. Skip with --no-preflight."""
    import time as _t
    print(f"[queue] pre-flight: timing --verify against {cwd} (up to {PREFLIGHT_SLOW_S}s) ...",
          file=sys.stderr)
    t0 = _t.time()
    try:
        r = subprocess.run(verify, shell=True, cwd=str(cwd), capture_output=True,
                           text=True, timeout=PREFLIGHT_SLOW_S)
    except subprocess.TimeoutExpired:
        sys.exit(f"[queue] REFUSING enqueue: --verify did not finish within {PREFLIGHT_SLOW_S}s -- "
                 f"it would TIME OUT in the worker's {VERIFY_TIMEOUT_S}s verify gate and fail the "
                 f"job regardless of the model's work. Narrow the verify's scope, or pass "
                 f"--no-preflight if you know it's only warm-up-slow.")
    except Exception as e:
        sys.exit(f"[queue] REFUSING enqueue: could not run --verify: {e} (pass --no-preflight to skip)")
    dt = _t.time() - t0
    if r.returncode in (126, 127):
        tail = "\n  ".join((r.stderr or r.stdout or "").strip().splitlines()[-3:])
        sys.exit(f"[queue] REFUSING enqueue: --verify cannot execute (exit {r.returncode} = command "
                 f"not found / not executable) -- this gate would fail EVERY run. Fix the verify "
                 f"command. Last output:\n  {tail}\n(override with --no-preflight)")
    _failed_at_baseline = r.returncode != 0
    verdict = "PASSES" if r.returncode == 0 else (f"exits {r.returncode} -- OK IF it verifies the FIX "
                                                  f"(fails at baseline by design); a red flag if it's "
                                                  f"meant to just check 'nothing broke'")
    print(f"[queue] pre-flight OK: --verify {verdict}, ran in {dt:.1f}s (worker gate = {VERIFY_TIMEOUT_S}s).",
          file=sys.stderr)
    return _failed_at_baseline

def _repo_toplevel(repo: Path):
    r = subprocess.run(["git", "-C", str(repo), "rev-parse", "--show-toplevel"],
                       capture_output=True, text=True, timeout=15)
    return Path(r.stdout.strip()) if r.returncode == 0 else None


def _is_isolated_worktree(cwd: Path) -> bool:
    """True if cwd is inside a LINKED git worktree (a .git *file* pointing at
    .git/worktrees/...), i.e. not the repo's primary checkout. A dispatch that
    edits the primary checkout is the isolation failure this enforces against."""
    r = subprocess.run(["git", "-C", str(cwd), "rev-parse", "--git-dir"],
                       capture_output=True, text=True, timeout=15)
    if r.returncode != 0:
        return False  # not a repo at all
    # In a linked worktree, --git-dir resolves under <main>/.git/worktrees/<name>.
    return "/.git/worktrees/" in (r.stdout.strip() + "/")


# Auto-created dispatch worktrees live OUTSIDE the iCloud Desktop tree
# (reference_icloud_desktop_sync_hazard: worktrees under ~/Desktop relocate
# without warning and have broken live dispatches). The linked worktree's .git
# file points back at the main repo's .git/worktrees/, which is fine cross-tree.
DISPATCH_WORKTREES = Path.home() / "dispatch-worktrees"


def _create_dispatch_worktree(repo_arg: str, base_ref, subdir, label):
    """Create a fresh isolated git worktree off base_ref and return the cwd
    (worktree root, or its <subdir>). Enforces per-dispatch isolation instead
    of trusting a hand-picked --cwd."""
    repo = Path(repo_arg).resolve()
    top = _repo_toplevel(repo)
    if top is None:
        sys.exit(f"[queue] --repo is not a git repository: {repo}")
    if base_ref:
        base = base_ref
    else:
        h = subprocess.run(["git", "-C", str(top), "rev-parse", "HEAD"],
                           capture_output=True, text=True, timeout=15)
        if h.returncode != 0:
            sys.exit(f"[queue] could not resolve HEAD of {top} for a base ref")
        base = h.stdout.strip()
    slug = safe_label(label or "dispatch")
    wid = uuid.uuid4().hex[:8]
    DISPATCH_WORKTREES.mkdir(parents=True, exist_ok=True)
    wt = DISPATCH_WORKTREES / f"{slug}-{wid}"
    branch = f"dispatch/{slug}-{wid}"
    r = subprocess.run(["git", "-C", str(top), "worktree", "add", "-b", branch, str(wt), base],
                       capture_output=True, text=True, timeout=120)
    if r.returncode != 0:
        sys.exit(f"[queue] worktree add failed: {r.stderr.strip() or r.stdout.strip()}")
    print(f"[queue] isolated worktree: {wt}\n"
          f"[queue]   branch {branch} off {base[:12]} (repo {top.name})", file=sys.stderr)
    cwd = (wt / subdir).resolve() if subdir else wt
    if not cwd.is_dir():
        sys.exit(f"[queue] --subdir {subdir!r} does not exist in the worktree: {cwd}")
    return wt, branch, str(top), cwd


def _run_setup(setup_cmd: str, cwd: Path):
    """Env-parity step: run e.g. 'npm ci' / 'python -m venv .venv && ...' in the
    fresh worktree BEFORE preflight, so the model isn't handed a depless tree
    (feedback_worktree_env_parity_before_dispatch)."""
    print(f"[queue] setup: running {setup_cmd!r} in {cwd} ...", file=sys.stderr)
    r = subprocess.run(setup_cmd, shell=True, cwd=str(cwd), capture_output=True, text=True, timeout=1800)
    if r.returncode != 0:
        sys.exit(f"[queue] setup command failed (exit {r.returncode}); refusing to enqueue a "
                 f"dep-starved job.\n--- stderr tail ---\n{r.stderr[-1500:]}")
    print(f"[queue] setup OK", file=sys.stderr)


# Untracked scaffold artifacts a dispatch worktree always carries -- excluded from
# the launch-baseline dirty count ONLY when untracked (??). A tracked modification
# to any of these basenames is real work and still counts.
_SCAFFOLD_BASENAMES = {
    "TASK.md", "task.md", "verify.sh",
    # TS/JS behavioural fixture, every stem/ext the scaffold or a hand harness
    # emits. The .mts form is what --lang ts writes; the hyphenated forms cover
    # the hand-rolled Rivian harness (verify-impl.mts) that a name-exact list
    # missed -- which is how a launch baseline read dirty (2 paths) on a job
    # whose only untracked files were the harness.
    "verify_impl.mjs", "verify_impl.js", "verify_impl.mts", "verify_impl.cts",
    "verify_impl.ts", "verify-impl.mjs", "verify-impl.js", "verify-impl.mts",
    "verify-impl.ts",
    "refimpl.py", ".preflight-state.json",
    "check_literals.py", "test_fixture.py", "task.json", "run.json",
}

# Build artifacts a toolchain regenerates during verify (e.g. `tsc` rewrites
# tsconfig.tsbuildinfo). They are never the model's work, so a dirty count that
# includes them mis-attributes the tree to the job -- and unlike the scaffold
# set these can show up TRACKED-and-modified too (tsbuildinfo is committed in
# some repos), so they are skipped regardless of the porcelain status.
_GENERATED_DIRTY = re.compile(
    r"(?:^|/)(?:[^/]*\.(?:tsbuildinfo|min\.js|min\.css|map)|"
    r"[^/]*\.(?:lock)|node_modules)$", re.I)  # node_modules: the symlink a TS
    # worktree scaffold drops in is never the model's work


def _count_real_dirty(porcelain_text: str) -> int:
    """Count real dirty paths from `git status --porcelain` output, skipping
    untracked (??) lines whose basename is a known scaffold artifact and any
    line (tracked or not) that names a regenerated build artifact. Real tracked
    edits and genuinely stray untracked files always count."""
    count = 0
    for ln in porcelain_text.splitlines():
        if not ln.strip():
            continue
        status = ln[:2]
        path = ln[3:]
        # `git status --porcelain` renders a rename as "old -> new"; score the
        # destination.
        if " -> " in path:
            path = path.split(" -> ", 1)[1]
        if status == "??" and os.path.basename(path) in _SCAFFOLD_BASENAMES:
            continue
        if _GENERATED_DIRTY.search(path):
            continue
        count += 1
    return max(0, count)


def cmd_enqueue(args):
    _validate_host(args.host)
    if args.api == "openai" and args.host == "auto":
        sys.exit("--api openai requires an explicit --host (studio/unraid/URL) -- "
                 "pick_host() only knows the two native-Ollama endpoints, not an ad-hoc "
                 "llama-server port. Pass e.g. --host http://127.0.0.1:8091.")
    task_file = Path(args.task_file).resolve()
    if not task_file.is_file():
        sys.exit(f"task file does not exist: {task_file}")

    # NO-VERIFY GATE (2026-09-10). Was a soft stderr warning (D5); hardened to a
    # gate after shipped-flip job d31d96d23b29 wandered all 55 iterations with a
    # ZERO diff -- a coding dispatch with no --verify has no goal signal / no
    # termination condition, so it thrashes to max-iters. A code-fix shape
    # (task_kind coding, or unset default) with no --verify now REFUSES unless
    # --allow-no-verify explicitly acknowledges it as scope-only/advisory. Done
    # HERE, before worktree creation, so a refusal leaves nothing behind. An
    # investigation/diagnosis shape legitimately has no code verify (it gates on
    # DIAGNOSIS.md), so it is exempt.
    if not args.verify and args.task_kind != "research":
        try:
            _tt_gate = task_file.read_text()
        except Exception:
            _tt_gate = ""
        if (not _dispatch_is_investigation(_tt_gate, args.verify, args.task_kind)
                and not getattr(args, "allow_no_verify", False)):
            sys.exit(
                "[queue] REFUSING enqueue: coding dispatch with NO --verify.\n"
                "  Without a verify the completeness and verify-RELEVANCE gates cannot run,\n"
                "  and -- as job d31d96d23b29 (shipped-flip) showed -- the model has no goal\n"
                "  signal, so it thrashes to max-iters and returns a zero diff. Attach a\n"
                "  --verify (ollama-dispatch-scaffold authors one), or pass --allow-no-verify\n"
                "  to enqueue it deliberately as a SCOPE-ONLY / ADVISORY run whose correctness\n"
                "  rides ENTIRELY on human review of the returned diff.")
    runner = None
    if getattr(args, "runner", None):
        runner = str(Path(args.runner).resolve())
        if runner not in ALLOWED_RUNNERS:
            sys.exit(f"[queue] REFUSING enqueue: --runner {runner} is not allowlisted. "
                     f"This queue guards a shared GPU; only exact-path runners in ALLOWED_RUNNERS "
                     f"may be launched. Allowed: {sorted(ALLOWED_RUNNERS)}. Add it there first.")
        if not Path(runner).is_file():
            sys.exit(f"[queue] REFUSING enqueue: --runner {runner} is allowlisted but does not exist.")
    # Scoping/worktree isolation. Either auto-create a fresh worktree (--repo,
    # enforced isolation) or accept a caller-supplied --cwd (legacy; warned if
    # it isn't an isolated worktree). Exactly one is required.
    wt_path = wt_branch = wt_repo = None
    if getattr(args, "repo", None):
        if args.cwd:
            sys.exit("[queue] pass EITHER --repo (auto-isolated worktree) OR --cwd, not both.")
        wt_path, wt_branch, wt_repo, cwd = _create_dispatch_worktree(
            args.repo, getattr(args, "base_ref", None), getattr(args, "subdir", None), args.label)
    else:
        if not args.cwd:
            sys.exit("[queue] need --repo (auto-isolated worktree) or --cwd.")
        cwd = Path(args.cwd).resolve()
        if not cwd.is_dir():
            print(f"[queue] WARNING: --cwd does not exist yet: {cwd} (job will fail to launch until it does)",
                  file=sys.stderr)
        elif not _is_isolated_worktree(cwd) and not getattr(args, "allow_unisolated", False):
            print(f"[queue] WARNING: --cwd {cwd} is NOT an isolated git worktree -- this dispatch can "
                  f"edit a primary checkout. Prefer --repo <path> [--base-ref REF] [--subdir S] so the "
                  f"queue creates a throwaway worktree per job. Pass --allow-unisolated to silence.",
                  file=sys.stderr)

    if getattr(args, "setup", None) and cwd.is_dir():
        _run_setup(args.setup, cwd)

    # Pre-flight the verify gate before we commit a job to the queue (see _preflight_verify).
    # Its return value is the authoritative baseline reading and is carried onto the job:
    # a verify that ALREADY failed here cannot later prove the work landed.
    _verify_failed_at_baseline = False
    # Honest preflight verdict carried onto the job so the handoff panel can show
    # what actually ran at enqueue (feedback: "gate still having issues" -- an
    # enqueue-path dispatch was showing the ambiguous "not-run (enqueued
    # separately)", which reads like a failure even though the preflight
    # verify-timing check DID pass). None here => genuinely nothing ran (no
    # --verify, --no-preflight, or a non-dir cwd), and the panel keeps "not-run".
    _preflight_verdict = None
    if args.verify and cwd.is_dir() and not getattr(args, "no_preflight", False):
        _verify_failed_at_baseline = _preflight_verify(args.verify, cwd)
        # A preflight is NOT a gate PASS -- it only timed the verify and read the
        # baseline. Say exactly that, and distinguish the healthy bug-fix shape
        # (verify RED at baseline, as designed) from the red flag (verify GREEN at
        # baseline, so it certifies nothing).
        _preflight_verdict = (
            "preflight-verify-ok (baseline fails as designed)"
            if _verify_failed_at_baseline
            else "preflight-verify-ran (baseline PASSES -- verify may check nothing)")

    # --- Auto num-ctx sizing (PART A) + split decision (PART B) -----------------
    # Explicit --num-ctx ALWAYS wins and turns both features off; existing dispatches
    # that pass --num-ctx are byte-for-byte unchanged. When --num-ctx is omitted we
    # compute a safe start bucket (or fall back to the top bucket if --no-auto-ctx).
    _computed_num_ctx = args.num_ctx
    _split_subspecs = None
    # Read the task text ONCE, up front: the auto-ctx sizing below needs it, and so
    # does the investigation/diagnosis floor further down -- which must run even on the
    # explicit --num-ctx path (where the sizing block is skipped entirely).
    try:
        _task_text = task_file.read_text(errors="replace")
    except OSError:
        _task_text = ""
    if args.num_ctx is None:
        _ceiling = resolve_ctx_ceiling(args.host, args.model)
        if getattr(args, "auto_ctx", True):
            _metrics_path = getattr(worker(), "DISPATCH_METRICS_PATH", None)
            _est, _info = estimate_task_tokens(_task_text, cwd if cwd.is_dir() else None,
                                               _metrics_path)
            _computed_num_ctx, _overflow, _target = size_num_ctx(_est, _ceiling)
            _hist_s = (f"{_info['hist_p90_tokens']}" if _info['hist_p90_tokens'] is not None
                       else "n/a")
            print(f"[queue] auto-ctx: task={_info['task_chars'] // 1000}kB "
                  f"(+named {_info['named_file_chars'] // 1000}kB) "
                  f"est={_est // 1000}k tok (hist_p90={_hist_s}) "
                  f"+{CTX_HEADROOM_PCT}% -> target={_target // 1000}k "
                  f"-> num_ctx={_computed_num_ctx} "
                  f"(host ceiling {_ceiling})", file=sys.stderr)
        else:
            _computed_num_ctx = min(CTX_BUCKETS[-1], _ceiling)
            _overflow = False
            print(f"[queue] auto-ctx disabled (--no-auto-ctx): num_ctx={_computed_num_ctx} "
                  f"(top bucket, host ceiling {_ceiling})", file=sys.stderr)

        _want_split, _split_reason = decide_split(
            _overflow, getattr(args, "auto_split", False), getattr(args, "no_split", False))
        if _want_split:
            _subs = decompose_task(_task_text)
            if len(_subs) >= 2:
                _split_subspecs = _subs
                print(f"[queue] auto-split: {_split_reason} -> {len(_subs)} sub-tasks, "
                      f"sequential chain in one worktree, gated once on the final slice.",
                      file=sys.stderr)
            else:
                print(f"[queue] auto-split: {_split_reason}, but no clean decomposition "
                      f"(>=2 independent targets) was found -- running as ONE job at "
                      f"num_ctx={_computed_num_ctx}; auto-resume will grow it if needed.",
                      file=sys.stderr)
    elif getattr(args, "auto_split", False):
        print("[queue] NOTE: explicit --num-ctx given -- auto-split is disabled "
              "(explicit sizing wins). Omit --num-ctx to allow splitting.", file=sys.stderr)

    # Investigation/diagnosis context FLOOR (see DIAGNOSIS_CTX_FLOOR's comment above).
    # Runs AFTER both sizing paths, on whatever num_ctx was chosen (explicit or auto),
    # so a diagnosis-shaped task never starts below the floor no matter how it was sized.
    # A normal bounded coding fix is not investigation-shaped, so this is a no-op for it.
    _is_investigation = _dispatch_is_investigation(_task_text, args.verify, args.task_kind)
    if _computed_num_ctx is not None and _is_investigation:
        _floor_ceiling = resolve_ctx_ceiling(args.host, args.model)
        _new_ctx, _floor_action = apply_diagnosis_ctx_floor(
            _computed_num_ctx, _is_investigation, _floor_ceiling)
        if _floor_action == "raised":
            print(f"[queue] diagnosis floor: investigation/diagnosis task at "
                  f"num_ctx={_computed_num_ctx} is below the {DIAGNOSIS_CTX_FLOOR} floor "
                  f"-- raising to {_new_ctx} (host ceiling {_floor_ceiling}). A small "
                  f"window walls a repo-reading diagnosis (a real one thrashed and paused "
                  f"at 92% on 32768); pass an explicit --num-ctx >= {DIAGNOSIS_CTX_FLOOR} "
                  f"to silence this.", file=sys.stderr)
            _computed_num_ctx = _new_ctx
        elif _floor_action == "warn":
            # The host ceiling itself is below the floor -- can't raise it, so warn
            # loudly rather than silently under-provisioning the investigation.
            print(f"[queue] WARNING: investigation/diagnosis task and num_ctx="
                  f"{_computed_num_ctx} is below the {DIAGNOSIS_CTX_FLOOR} floor, but the "
                  f"host ceiling ({_floor_ceiling}) will not allow raising it -- expect "
                  f"context pressure; consider a bigger-window host/model.",
                  file=sys.stderr)

    # A coding dispatch with NO --verify silently skips BOTH the completeness
    # gate and the verify-RELEVANCE gate -- they cannot run without a verify to
    # prove-fail at baseline and prove-pass on the refimpl. The run is then
    # scope-only/advisory: nothing mechanical certifies the change is correct,
    # so correctness rides entirely on human review of the diff. Say so, loudly,
    # at enqueue -- a silent skip is how a "green" dispatch reads as gated when
    # it never was. A diagnosis/investigation shape legitimately has no code
    # verify (it gates on DIAGNOSIS.md), so exclude it; only warn for a real
    # code-fix shape (task_kind coding, or unset -- the default coding path).
    if not args.verify and args.task_kind != "research" and not _is_investigation:
        _b = "[queue] " + "=" * 68
        print(_b, file=sys.stderr)
        print("[queue] WARNING: coding dispatch enqueued with NO --verify.", file=sys.stderr)
        print("[queue]   The completeness gate and the verify-RELEVANCE gate are BOTH", file=sys.stderr)
        print("[queue]   SKIPPED -- they need a verify that fails at baseline and passes", file=sys.stderr)
        print("[queue]   on the refimpl. This run is SCOPE-ONLY / ADVISORY: nothing", file=sys.stderr)
        print("[queue]   mechanical certifies the change is correct, so correctness rides", file=sys.stderr)
        print("[queue]   ENTIRELY on HUMAN REVIEW of the returned diff. Pass --verify to", file=sys.stderr)
        print("[queue]   gate it (ollama-dispatch-scaffold authors a real one).", file=sys.stderr)
        print(_b, file=sys.stderr)

    with _Locked() as lock:
        state = lock.load()
        job_id = uuid.uuid4().hex[:12]
        after_id = None
        if getattr(args, "after", None):
            _m = [j for j in state["jobs"] if j.get("id") == args.after]
            if not _m:
                _m = [j for j in state["jobs"] if str(j.get("id", "")).startswith(args.after)]
            if len(_m) == 0:
                sys.exit(f"[queue] --after {args.after!r}: no such job in the queue")
            if len(_m) > 1:
                sys.exit(f"[queue] --after {args.after!r} is ambiguous ({len(_m)} matches: "
                         f"{', '.join(j['id'] for j in _m)})")
            after_id = _m[0]["id"]
        job = {
            "id": job_id,
            "label": args.label or Path(args.cwd).name,
            "model": args.model,
            "host_pref": args.host,
            "cwd": str(cwd),
            "task_file": str(task_file),
            "runner": runner,  # None = default ollama-worker.py; else an allowlisted alternate exe
            "task_kind": args.task_kind,
            "manual_tools": args.manual_tools,
            "api": args.api,
            "verify": args.verify,
            # Authoritative baseline reading, taken in this cwd BEFORE any model edit.
            # A verify that already failed here cannot later prove the work landed, so
            # the worker must not excuse a still-failing verify as "no new failures".
            "verify_failed_at_baseline": _verify_failed_at_baseline,
            # Honest preflight reading for the handoff panel's gate cell. None =>
            # no preflight ran (kept as "not-run"); otherwise a "preflight-verify-*"
            # string that the panel surfaces INSTEAD of the review's ambiguous
            # "not-run (enqueued separately)" until the real gate/review verdict lands.
            "preflight": _preflight_verdict,
            # #8 scored bake-off arm (set by bakeoff-fire.py). Forwarded to the worker, which
            # treats it as implying verify-failed-at-baseline AND disables baseline-diagnostic
            # subtraction (for a scored arm the baseline failures ARE the task). Also lands in
            # dispatch-metrics.jsonl so scored and unscored runs are never pooled in the tracker.
            "scored_arm": args.scored_arm,
            "num_ctx": _computed_num_ctx,
            "max_iters": args.max_iters,
            "temperature": args.temperature,
            "chat_timeout": args.chat_timeout,
            "max_tokens": args.max_tokens,
            "capture_final_as": args.capture_final_as,
            "status": "pending",
            # Chain dependency (full id, resolved above) + grouping/final flags.
            "after": after_id,
            "chain": getattr(args, "chain", None),
            "chain_final": bool(getattr(args, "chain_final", False)),
            "enqueued_at": datetime.now(timezone.utc).isoformat(),
            "pid": None,
            "lane": None,
            "log_path": None,
            "exit_code": None,
            "live_log_path": None,
        }
        if wt_path is not None:
            # Auto-created isolation worktree: recorded so the gate reads the
            # right tree and cleanup can prune it after signoff.
            job["worktree"] = str(wt_path)
            job["worktree_branch"] = wt_branch
            job["repo"] = wt_repo
        if not getattr(args, "no_live_log", False):
            LIVE_LOG_DIR.mkdir(parents=True, exist_ok=True)
            job["live_log_path"] = str(LIVE_LOG_DIR / f"{job_id}-{safe_label(job['label'])}.livelog")
        # Provenance + launch-baseline, recorded at enqueue for gate-on-complete /
        # signoff to consume later (their contract: read at gate time, not enqueue).
        #  - launch_baseline {head, dirty}: dirty MUST be a real int (a string "0"
        #    reads as never-measured); clamp >=0 (negative reads as clean); OMIT the
        #    key entirely on any failure/non-repo -- never null, never 0-as-unknown.
        #  - launched_by: the enqueuing session's messaging socket (routable, but
        #    pid-keyed so it can go stale after the session ends -- the gate treats a
        #    dead socket as unknown). launched_by_session: stable, NOT routable, kept
        #    as provenance so a stale delivery is self-evident. OMIT when unset
        #    (enqueued outside a session -- cron/detached -- = unknown launcher).
        try:
            _head = subprocess.run(["git", "-C", str(cwd), "rev-parse", "HEAD"],
                                   capture_output=True, text=True, timeout=10)
            _porc = subprocess.run(["git", "-C", str(cwd), "status", "--porcelain"],
                                   capture_output=True, text=True, timeout=10)
            if _head.returncode == 0 and _porc.returncode == 0:
                _dirty = _count_real_dirty(_porc.stdout)
                job["launch_baseline"] = {"head": _head.stdout.strip(), "dirty": int(_dirty)}
        except Exception:
            pass  # not a repo / git unavailable -> key omitted = never measured
        _sock = os.environ.get("CLAUDE_CODE_MESSAGING_SOCKET")
        if _sock:
            job["launched_by"] = _sock
        _sess = os.environ.get("CLAUDE_CODE_BRIDGE_SESSION_ID")
        if _sess:
            job["launched_by_session"] = _sess
        if _split_subspecs:
            # PART B recombination wiring: expand `job` into an ordered CHAIN of
            # per-target slice jobs, all sharing THIS worktree/cwd, run sequentially
            # (each `after` the previous -- never parallel: parallel slices collide on
            # shared files like package.json's `test` line). Only the FINAL slice
            # carries the original --verify: the whole combined tree is verified ONCE,
            # after every slice has landed its edits. If any slice fails to converge,
            # dependency_decision()/_cascade_blocked() block the rest AND the final
            # gate never fires -- a half-applied split surfaces as a blocked chain, not
            # a shipped partial change.
            split_group = "split-" + job_id
            split_dir = worker().LOG_DIR / "dispatch-splits"
            split_dir.mkdir(parents=True, exist_ok=True)
            slice_ids = [uuid.uuid4().hex[:12] for _ in _split_subspecs]
            base_label = job["label"]
            for i, sub in enumerate(_split_subspecs):
                sid = slice_ids[i]
                is_final = (i == len(_split_subspecs) - 1)
                sub_path = split_dir / f"{sid}.md"
                sub_path.write_text(sub["body"])
                # Size each slice from its OWN sub-spec -- the whole point of splitting
                # is that each fits; clamp to the same host ceiling.
                s_est, _ = estimate_task_tokens(sub["body"],
                                                cwd if cwd.is_dir() else None,
                                                getattr(worker(), "DISPATCH_METRICS_PATH", None))
                s_ctx, _, _ = size_num_ctx(s_est, resolve_ctx_ceiling(args.host, args.model))
                slice_job = dict(job)
                slice_job["id"] = sid
                slice_job["task_file"] = str(sub_path)
                slice_job["num_ctx"] = s_ctx
                slice_job["label"] = f"{base_label} [slice {i + 1}/{len(_split_subspecs)}: {sub['title']}]"
                slice_job["chain"] = split_group
                slice_job["chain_final"] = is_final
                slice_job["after"] = after_id if i == 0 else slice_ids[i - 1]
                # Recombine: only the final slice runs the original verify against the
                # combined result. Intermediate slices don't gate.
                slice_job["verify"] = args.verify if is_final else None
                if not is_final:
                    slice_job["verify_failed_at_baseline"] = False
                    # The preflight was timed against the FINAL slice's (whole-tree)
                    # verify; an intermediate slice has no verify of its own, so it
                    # did not run one -- don't attribute the preflight reading to it.
                    slice_job["preflight"] = None
                if job.get("live_log_path"):
                    slice_job["live_log_path"] = str(
                        LIVE_LOG_DIR / f"{sid}-{safe_label(slice_job['label'])}.livelog")
                state["jobs"].append(slice_job)
            lock.save(state)
            print(f"enqueued SPLIT {split_group}  {base_label}  "
                  f"{len(_split_subspecs)} slices  model={args.model}  host={args.host}")
            for i, sid in enumerate(slice_ids):
                print(f"  slice {i + 1}/{len(slice_ids)}: {sid}"
                      + ("  (final: runs --verify + gate)" if i == len(slice_ids) - 1 else ""))
            return
        if getattr(args, "front", False):
            # --front: same ordering rule as `promote` without --preempt -- insert AHEAD of every
            # pending job so the launch loop picks this one up next time a lane frees. Running/
            # paused jobs keep their positions and finish on their own; nothing here ever SIGTERMs
            # a running job (no preemption). With no pending job at all there is nothing to jump
            # ahead of, so fall through to the plain append below.
            _first_pending = next((i for i, j in enumerate(state["jobs"])
                                   if j.get("status") == "pending"), None)
            if _first_pending is not None:
                state["jobs"].insert(_first_pending, job)
            else:
                state["jobs"].append(job)
        else:
            state["jobs"].append(job)
        lock.save(state)
    print(f"enqueued {job_id}  {job['label']}  model={args.model}  host={args.host}")
    if job["live_log_path"]:
        print(f"  live log: tail -f {job['live_log_path']}")


def cmd_status(_args):
    with _Locked() as lock:
        state = lock.load()
    if not state["jobs"]:
        print("(queue empty)")
        return
    for j in state["jobs"]:
        loc = j.get("lane") or j["host_pref"]
        line = (f"[{j['status']:8s}] {j['id']}  {j['label']:28s} {j['model']:32s} "
                f"host={loc:8s} pid={j.get('pid')}  exit={j.get('exit_code')}")
        if j.get("error"):
            line += f"  error={j['error']}"
        if j.get("resume_transcript"):
            line += f"  resume={j['resume_transcript']}"
        print(line)


def _build_cmd(job, host_url):
    runner = job.get("runner")
    if runner:
        # Alternate runner (allowlisted at enqueue -- re-checked here as defense in depth against a
        # hand-edited job dict). The queue supplies ONLY the host/model assignment it computed plus
        # the task + cwd; the runner owns all other behaviour, its own iteration/verify logic, and
        # its own output. Deliberately omits the worker-specific flags (--verify/--max-iters/--api/
        # --task-kind/...), which an alternate runner does not accept. A runner is expected to accept
        # exactly: --model --host --num-ctx --cwd --task-file.
        if runner not in ALLOWED_RUNNERS:
            raise ValueError(f"job {job.get('id')} names non-allowlisted runner {runner!r} "
                             f"(allowed: {sorted(ALLOWED_RUNNERS)}) -- refusing to launch it")
        return ["python3", runner,
                "--model", job["model"], "--host", host_url,
                "--num-ctx", str(job["num_ctx"]),
                "--cwd", job["cwd"], "--task-file", job["task_file"]]
    task_text = Path(job["task_file"]).read_text()
    cmd = [
        "python3", str(WORKER_PATH),
        "--model", job["model"],
        "--host", host_url,
        "--cwd", job["cwd"],
        # --task stays even for resume relaunches: the worker's argparse still REQUIRES it
        # when --resume is given (its own --resume help says so) -- only the message history
        # comes from the transcript, not the task text.
        "--task", task_text,
        "--num-ctx", str(job["num_ctx"]),
        # Resume budget: a paused job gets its ORIGINAL max_iters again as this session's
        # --max-iters (the worker's --resume flow treats it as "how many MORE iterations are
        # allowed THIS session", not a total). Why the full original budget rather than
        # tracking how many were already used and handing over only the remainder:
        #   1. A promote-paused job was cut off mid-work by an EXTERNAL decision, so it
        #      deserves at least as much remaining budget as it had left -- and the original
        #      max_iters is exactly the upper bound of that, with no bookkeeping needed.
        #   2. The job dict doesn't track per-session iteration usage; recovering it would
        #      mean parsing (potentially multi-MB) transcript JSON inside this hot path.
        #   3. Cost stays bounded: every iteration is already capped by --chat-timeout and
        #      the worker's --max-tokens, and each pause/resume cycle requires an explicit
        #      human action in the dashboard (a promote drop or a resume click), so repeated
        #      cycles can't silently multiply a runaway job's budget unnoticed.
        *iters_flag(job["max_iters"]),
        "--temperature", str(job["temperature"]),
    ]
    if job.get("task_kind"):
        cmd += ["--task-kind", job["task_kind"]]
    if job.get("manual_tools"):
        cmd += ["--manual-tools"]
    # host_url wins over job["api"]="ollama" default: a job auto-routed to
    # the llama-server bypass (see _candidate_lanes) MUST speak --api openai
    # regardless of what the job was originally enqueued with.
    api = "openai" if host_url == LLAMA_SERVER_QWEN38_URL else job.get("api")
    if api and api != "ollama":
        cmd += ["--api", api]
    if job.get("verify"):
        cmd += ["--verify", job["verify"]]
        if job.get("verify_failed_at_baseline"):
            cmd += ["--verify-failed-at-baseline"]
    if job.get("scored_arm"):
        cmd += ["--scored-arm"]
    if job.get("chat_timeout"):
        cmd += ["--chat-timeout", str(job["chat_timeout"])]
    # Per-dispatch output-token cap (Fable ruling 2026-08-29): thinking models
    # (e.g. nemotron-cascade-2) exhaust the worker's DEFAULT_MAX_TOKENS=8192 on
    # reasoning and get truncated before emitting a tool call in the agentic loop.
    # Opt-in only -- omitted => worker keeps its 8192 default, so running benches
    # see byte-identical behavior. Use 16384 for thinking-model dispatches.
    if job.get("max_tokens"):
        cmd += ["--max-tokens", str(job["max_tokens"])]
    # Auto-capture fallback (Fable 2026-08-30): for review tasks, name the deliverable so
    # the worker saves a model's final TEXT answer AS the file when it forgets to write it
    # (nemotron-cascade-2's ~29% REVIEW.md-write failures) -- scored, not lost as NO_REVIEW.
    if job.get("capture_final_as"):
        cmd += ["--capture-final-as", job["capture_final_as"]]
    # Paused-job relaunch: continue from the saved transcript instead of restarting. Set by
    # the reap loop (and daemon-start recovery) when the worker exits with EXIT_CODE_PAUSED;
    # cleared of any need to track further -- the worker keeps updating the SAME file across
    # pause/resume cycles, so a job paused twice in a row resumes from its latest state.
    if job.get("resume_transcript"):
        cmd += ["--resume", job["resume_transcript"]]
    if job.get("live_log_path"):
        cmd += ["--live-log", job["live_log_path"], "--dispatch-tag", job["id"]]
    return cmd


_size_cache = {}  # model -> (size, monotonic ts)


def _model_size_cached(w, model):
    """pick_host()'s own size lookup (first host that reports it wins), with
    a short TTL so repeated polls don't re-hit both hosts' /api/tags."""
    hit = _size_cache.get(model)
    if hit is not None and time.monotonic() - hit[1] < MODEL_SIZE_CACHE_TTL_S:
        return hit[0]
    size = None
    for spec in w.KNOWN_OLLAMA_HOSTS.values():
        s = w._get_model_size_on_host(spec["url"], model)
        if s:
            size = s
            break
    _size_cache[model] = (size, time.monotonic())
    return size


def _candidate_lanes(job, w):
    """Ordered list of lanes this job may run on. For auto jobs this mirrors
    pick_host()'s priority exactly: Studio always first; Unraid appended as
    overflow only when the model's real size fits its usable budget (or is
    unknown -- in which case pick_host() would default to Studio anyway, and
    letting a free Unraid take it is strictly more parallelism)."""
    pref = job["host_pref"]
    # qwen3.8 crashes on Studio's native Ollama (see LLAMA_SERVER_QWEN38_URL
    # comment) -- redirect auto/studio routing to the llama-server bypass
    # instead. An explicit --host unraid or explicit URL still overrides
    # this (Penn's call, e.g. a deliberate Unraid headroom test), matching
    # how explicit --host already bypasses auto-routing everywhere else.
    if job["model"] == LLAMA_SERVER_QWEN38_MODEL and pref in ("auto", "studio"):
        return [LLAMA_SERVER_QWEN38_URL]
    if pref in ("studio", "unraid"):
        return [w.KNOWN_OLLAMA_HOSTS[pref]["url"]]
    if pref.startswith("http"):
        return [pref]
    size = _model_size_cached(w, job["model"])
    fits_unraid = size is not None and size <= w.KNOWN_OLLAMA_HOSTS["unraid"]["usable_bytes"]
    if size is not None and not fits_unraid:
        return [w.KNOWN_OLLAMA_HOSTS["studio"]["url"]]
    lanes = [w.KNOWN_OLLAMA_HOSTS["studio"]["url"]]
    if fits_unraid:
        lanes.append(w.KNOWN_OLLAMA_HOSTS["unraid"]["url"])
    return lanes


# ---------------------------------------------------------------------------
# Dual-slot concurrency decision (added 2026-09-04, branch fix/dualslot-research).
#
# BACKGROUND: this queue is deliberately one-job-per-lane (see the Concurrency
# section of the module docstring) because the 2026-08-28 incident was a VRAM
# collision from two overlapping model LOADS on Studio's shared 64GB pool. The
# owner approved a NARROW exception: a second job may co-run on an already-busy
# lane ONLY when it adds no second set of weights and only spends the extra slot
# on an IO-bound research side. Everything below fails toward serial: any doubt,
# any unavailable signal, any missing guarantee -> deny and keep the strict
# one-per-lane behaviour.
#
# The decision itself is a PURE function (slot_decision) so it is unit-tested in
# isolation by --self-test; the daemon feeds it live occupancy + two positively-
# confirmed runtime facts (slot capacity, VRAM fit) computed by the helpers that
# follow it. Keeping the policy pure and the runtime probing separate is what lets
# the self-test exercise every deny path without a live GPU.
# ---------------------------------------------------------------------------
def slot_decision(new_model, new_task_kind, running_jobs, slot_capacity, vram_fits):
    """May this pending job claim a slot on this lane RIGHT NOW? Pure -- no I/O.

    Inputs:
      new_model     : the pending job's model string.
      new_task_kind : the pending job's task_kind ("coding"/"research"/None).
      running_jobs  : list of {"model": str, "task_kind": str|None} for every job
                      already RUNNING on this lane (empty == lane free).
      slot_capacity : positively-confirmed number of parallel slots the server
                      backing this lane actually serves (>=1). The caller MUST pass
                      1 whenever it cannot positively confirm >=2 (fail toward serial).
      vram_fits     : True only if the resident model plus a second job's KV cache
                      (its num_ctx) is confirmed to fit; False when unavailable or
                      marginal.

    Returns (allow: bool, reason: str). The one-per-lane default is preserved: the
    second slot is reachable ONLY when the lane already holds exactly the same model
    (no swap, no second weight load), at least one side is research, the server has a
    confirmed free slot, and VRAM fits. Any other case denies.
    """
    occupied = len(running_jobs)

    # Lane free -> normal first claim. No capacity/VRAM probing needed.
    if occupied == 0:
        return True, "lane empty -- normal first claim"

    # --- Everything from here is the guarded second-slot exception. ---

    # (server) The backing server must positively offer >=2 parallel slots.
    if slot_capacity < 2:
        return False, (f"lane serves only {slot_capacity} confirmed slot(s) -- "
                       f"no second slot to grant, serialize")

    # (capacity) All confirmed slots already in use.
    if occupied >= slot_capacity:
        return False, (f"all {slot_capacity} slot(s) already busy "
                       f"({occupied} running) -- serialize")

    # (a) Identical model already resident -- NO swap and NO second weight load.
    # A single differing model (or an unknown/blank model) means a load would be
    # required -> the exact VRAM-collision risk this whole tool exists to prevent.
    resident_models = {j.get("model") for j in running_jobs}
    if resident_models != {new_model}:
        return False, (f"resident model(s) {sorted(m or '<unknown>' for m in resident_models)} "
                       f"!= new job model {new_model!r} -- would require a load/swap/evict, deny")

    # (b) At least one of the co-resident pair must be research.
    kinds = [new_task_kind] + [j.get("task_kind") for j in running_jobs]
    if not any(k == "research" for k in kinds):
        return False, ("neither side is task_kind=research -- the second slot is only "
                       "spent when one side is IO-bound research, deny")

    # (VRAM) Resident weights + the second job's KV cache must fit.
    if not vram_fits:
        return False, ("second job's KV cache does not fit remaining VRAM headroom "
                       "(or the estimate is unavailable/marginal) -- deny")

    return True, ("same model + a research side + a confirmed free slot + VRAM fits "
                  "-- second slot granted")


def _lane_slot_capacity(lane_url):
    """Positively-confirmed parallel-slot count for the server that actually backs
    `lane_url`. Returns an int >=1, and returns 1 (serialize) whenever >=2 cannot be
    POSITIVELY confirmed from the live server -- never an optimistic guess.

    - llama-server bypass (LLAMA_SERVER_QWEN38_URL): llama.cpp's /props reports
      total_slots, which equals the process's --parallel value. The vetted start
      script (start-llama-server-qwen3.8.sh) launches it with --parallel 1 by
      default, so this is 1 unless a human has opted into multi-slot serving (see
      that script's LLAMA_SERVER_PARALLEL env) AND the running process reports it.
    - native Ollama hosts (Studio 11434, Unraid): per-model parallelism is governed
      by the SERVER's OLLAMA_NUM_PARALLEL env, which Ollama does not expose over its
      HTTP API and which is unset in this deployment. It cannot be positively
      confirmed >=2 from here, so this returns 1 -> native-Ollama lanes never
      co-run. A human enabling multi-slot Ollama must extend this helper with a
      real, confirmed signal before that path can ever open.
    """
    if lane_url == LLAMA_SERVER_QWEN38_URL:
        try:
            req = urllib.request.Request(f"{lane_url}/props")
            with urllib.request.urlopen(req, timeout=5) as resp:
                props = json.loads(resp.read())
            n = int(props.get("total_slots") or 1)
            return max(1, n)
        except Exception as e:
            print(f"[queue] slot-capacity probe of {lane_url}/props failed ({e}) -- "
                  f"assuming 1 slot (serialize)", file=sys.stderr)
            return 1
    return 1


def _second_slot_vram_fits(lane_url, new_num_ctx):
    """Confirm a SECOND job's KV cache fits alongside the already-resident model on
    `lane_url`, WITHOUT loading any new weights. Returns True only on positive
    confirmation; False on any uncertainty (fail toward serial).

    Only the llama-server bypass can ever reach a >=2 slot capacity in this
    deployment (see _lane_slot_capacity), so that is the only case with a real,
    checkable answer here:

      llama.cpp with --parallel N pre-allocates the WHOLE --ctx-size KV budget up
      front and divides it into N equal per-slot windows (per_slot = n_ctx). No
      second set of weights is loaded for slot 2, and no KV beyond what is already
      reserved is added -- so the memory is already accounted for by the running
      server. The only remaining fit question is whether the new job's num_ctx fits
      inside one per-slot window; /props' default_generation_settings.n_ctx reports
      that window exactly. If it does not fit, the request would be truncated/
      rejected, so we deny.

    Any non-bypass lane (native Ollama) returns False: those never get capacity>=2,
    and this helper deliberately has no unverified KV estimate for a co-resident
    second Ollama slot. A reviewer enabling native-Ollama concurrency must add a
    measured Ollama KV-fit check here first.
    """
    if lane_url != LLAMA_SERVER_QWEN38_URL:
        return False
    try:
        req = urllib.request.Request(f"{lane_url}/props")
        with urllib.request.urlopen(req, timeout=5) as resp:
            props = json.loads(resp.read())
    except Exception as e:
        print(f"[queue] VRAM-fit probe of {lane_url}/props failed ({e}) -- deny second slot",
              file=sys.stderr)
        return False
    gen = props.get("default_generation_settings") or {}
    per_slot = gen.get("n_ctx")
    if per_slot is None:
        # Fall back to the total context divided by the slot count.
        total = props.get("n_ctx")
        slots = props.get("total_slots") or 1
        if total is not None and slots:
            per_slot = int(total) // int(slots)
    if not per_slot:
        print(f"[queue] /props gave no per-slot n_ctx -- cannot confirm VRAM fit, deny",
              file=sys.stderr)
        return False
    if new_num_ctx > per_slot:
        print(f"[queue] second job num_ctx {new_num_ctx} exceeds per-slot window {per_slot} "
              f"-- deny second slot", file=sys.stderr)
        return False
    return True


def _safe_label(label):
    """Log filenames must survive whatever --label Penn typed."""
    return re.sub(r"[^A-Za-z0-9._-]", "_", label) or "job"


# How long promote_job waits (best-effort) for the daemon's own reap logic to pick up the
# paused job's exit after its SIGTERM. Deliberately short: the worker only honors the signal
# at the end of its CURRENT iteration (an in-flight chat call can run up to --chat-timeout),
# and the daemon reaps on ITS poll cycle (default 15s) -- so this window often expires while
# everything is still perfectly fine. Expiring changes nothing: the promoted job already sits
# at position 0, and it launches as soon as whatever poll reaps the victim frees the lane.
PROMOTE_REAP_WAIT_S = 10

# Min-progress guard for --preempt: refuse to SIGTERM a job that only just launched, so a
# promote can't thrash a job through its expensive model-load + warm-up before it has saved a
# single iteration's worth of work. Overridable with --force. Measured from job["launched_at"]
# (set at launch); a running job with no launched_at (legacy/recovered) is treated as old enough.
PREEMPT_MIN_PROGRESS_S = 90


def promote_job(job_id, preempt=False, force=False):
    """Move this PENDING job to the front of state["jobs"] so the daemon's normal launch loop
    picks it up next poll. Callable both from the `promote` CLI subcommand and in-process by
    ollama-queue-api.py; raises QueueActionError (not sys.exit) on user-facing errors -- see
    that class for why.

    preempt=False (the DEFAULT): jump the queue only. The running job on this job's lane keeps
    running to completion; the promoted job launches next time that lane frees. This is the safe
    default -- promoting no longer destroys in-flight work.
    preempt=True (explicit `--preempt`): additionally pause whatever is running on `job_id`'s
    lane (graceful SIGTERM -- the worker finishes its current iteration, saves its transcript,
    exits with EXIT_CODE_PAUSED) so the promoted job takes the lane immediately.

    Ordering note: the move-to-front happens BEFORE the SIGTERM, under one lock hold with the
    state read. If we waited for the pause first, a worker that exits quickly could free its
    lane mid-wait and let the daemon launch some OTHER pending job on it before this one ever
    reached position 0 -- silently breaking "run THIS one instead". Move-first closes that
    race; the bounded wait afterwards is best-effort confirmation only.

    A running job occupying a lane but not present in state (a legacy hand-written chain,
    caught by the launch loop's pgrep safety net) has no pid to signal -- if that's the only
    occupant of this job's lanes, it just gets moved to the front and waits out the normal
    lane-claim logic.
    """
    w = worker()
    with _Locked() as lock:
        state = lock.load()
        job = next((j for j in state["jobs"] if j.get("id") == job_id), None)
        if job is None:
            raise QueueActionError(f"job not found: {job_id}")
        if job.get("status") != "pending":
            raise QueueActionError(
                f"only pending jobs can be promoted (this one is {job.get('status')!r})")
    # Candidate lanes OUTSIDE the lock: for auto jobs this may hit both hosts' /api/tags on a
    # cache miss (15s timeout each) -- holding the state flock through that network I/O would
    # stall enqueue/status, exactly what MODEL_SIZE_CACHE_TTL_S exists to avoid.
    lane_names = [_lane_name(u) for u in _candidate_lanes(job, w)]
    with _Locked() as lock:
        state = lock.load()  # fresh read -- state may have changed since the first one
        job = next((j for j in state["jobs"] if j.get("id") == job_id), None)
        if job is None:
            raise QueueActionError(f"job not found: {job_id}")
        if job.get("status") != "pending":
            raise QueueActionError(
                f"only pending jobs can be promoted (this one is now {job.get('status')!r})")
        # The lane this job would actually claim first, in the daemon's own priority order:
        # pause whatever is running on it. (If that lane is free but a lower-priority one is
        # busy, nothing needs pausing -- the launch loop will take the free lane.)
        victim = None
        for name in lane_names:
            victim = next((j for j in state["jobs"] if j.get("status") == "running"
                           and _lane_name(j.get("lane")) == name), None)
            if victim is not None:
                break
        # Min-progress guard (#3): if we'd preempt, refuse when the victim only just launched and
        # --force wasn't given. Done BEFORE the move-to-front so a refusal leaves state untouched.
        # A running job with no launched_at (legacy/recovered) is treated as old enough to preempt.
        if victim is not None and preempt and not force:
            la = victim.get("launched_at")
            elapsed = None
            if la:
                try:
                    elapsed = (datetime.now(timezone.utc) - datetime.fromisoformat(la)).total_seconds()
                except (ValueError, TypeError):
                    elapsed = None
            if elapsed is not None and elapsed < PREEMPT_MIN_PROGRESS_S:
                raise QueueActionError(
                    f"refusing to preempt {victim['id']} ({victim['label']}): it has only run "
                    f"{elapsed:.0f}s (< {PREEMPT_MIN_PROGRESS_S}s min-progress) -- still in "
                    f"model-load/warm-up with no iteration saved yet, so preempting now wastes it. "
                    f"Pass --force to preempt anyway, or omit --preempt to just jump the queue.")
        # Move to front NOW, under this same lock hold (atomic with the read above): position 0
        # in state["jobs"], ahead of every other pending job. The launch loop iterates the list
        # in order and skips non-pending entries, so this is all that's needed for it to be
        # first in line when the lane frees -- no daemon-side change required.
        state["jobs"].remove(job)
        state["jobs"].insert(0, job)
        lock.save(state)
    victim_id = None
    if victim is not None and preempt:
        victim_id = victim["id"]
        try:
            os.kill(victim["pid"], signal.SIGTERM)
        except OSError as e:
            print(f"[queue] WARNING: SIGTERM to {victim_id} (pid {victim['pid']}) failed: {e} -- "
                  f"it will be reaped normally when it exits on its own", file=sys.stderr)
        else:
            print(f"[queue] sent SIGTERM to running job {victim_id} ({victim['label']}, pid {victim['pid']}) "
                  f"-- pausing gracefully at the end of its current iteration")
    elif victim is not None:
        # --no-preempt (the default): jumped the queue but leave the running job alone. It keeps
        # its lane until it finishes on its own; the promoted job launches next time the lane frees.
        # Nothing to wait on -- the move-to-front already happened under the lock above.
        print(f"[queue] promoted {job_id} to the front of the queue; NOT preempting running job "
              f"{victim['id']} ({victim['label']}, pid {victim['pid']}) -- it keeps its lane until "
              f"it finishes (pass --preempt to SIGTERM it and take the lane now)")
        return {"promoted": job_id, "paused_job": None, "victim_status": None,
                "running_left_alone": victim["id"]}
    # Bounded best-effort wait for the daemon's reap logic to mark the victim paused (or
    # done/failed, if it happened to finish on its own first). See PROMOTE_REAP_WAIT_S for why
    # expiring here is harmless.
    deadline = time.monotonic() + PROMOTE_REAP_WAIT_S
    final_status = "running"
    while victim_id is not None and time.monotonic() < deadline:
        with _Locked() as lock:
            state = lock.load()
            v = next((j for j in state["jobs"] if j.get("id") == victim_id), None)
            final_status = v.get("status") if v is not None else "gone"
        if final_status != "running":
            break
        time.sleep(1)
    print(f"[queue] promoted {job_id} to the front of the queue"
          + (f"; victim {victim_id} now {final_status}" if victim_id else "; no running job on its lane"))
    return {"promoted": job_id, "paused_job": victim_id,
            "victim_status": final_status if victim_id else None}


def resume_job(job_id):
    """Flip a PAUSED job back to pending so the launch loop relaunches it. Its
    resume_transcript (set when it was paused) stays on the job dict, so _build_cmd's resume
    branch continues it from the saved transcript automatically -- nothing else to do."""
    with _Locked() as lock:
        state = lock.load()
        job = next((j for j in state["jobs"] if j.get("id") == job_id), None)
        if job is None:
            raise QueueActionError(f"job not found: {job_id}")
        if job.get("status") != "paused":
            raise QueueActionError(
                f"only paused jobs can be resumed (this one is {job.get('status')!r})")
        job["status"] = "pending"
        lock.save(state)
    print(f"[queue] resumed {job_id} ({job['label']}) -- will relaunch from "
          f"{job.get('resume_transcript') or 'scratch (no transcript recorded)'} on the next free lane")
    return {"resumed": job_id, "resume_transcript": job.get("resume_transcript")}


def cancel_job(job_id):
    """Remove a job's state entry outright. Added 2026-08-29 (github-projects-bf flagged
    this as a real gap): cancelling a queued job was only reachable via the HTTP API's
    DELETE /api/jobs/<id>, which does nothing for a CLI-only session or when the API
    process is unreachable -- confirmed live the same night (a wrongly-specified pending
    job could only be cancelled from the dashboard, not the CLI). Same status restriction
    as the API's own _cancel handler (now delegates here instead of duplicating this
    logic, so the two can't drift apart): "pending" means cancel-before-it-runs,
    "done"/"failed"/"paused" means clear-a-finished-or-resumable entry -- all safe state
    edits with no live process attached. "running" is refused: removing its state entry
    without killing the pid would orphan the process and desync the daemon's own
    reap-by-pid bookkeeping -- use `kill` (SIGTERM by pid) for a running job instead."""
    with _Locked() as lock:
        state = lock.load()
        job = next((j for j in state["jobs"] if j.get("id") == job_id), None)
        if job is None:
            raise QueueActionError(f"job not found: {job_id}")
        if job.get("status") not in ("pending", "done", "done_unconverged", "failed", "paused", "blocked"):
            raise QueueActionError(
                f"only pending/done/failed/paused/blocked jobs can be cancelled (this one is "
                f"{job.get('status')!r} -- a running job must be killed by pid, not cancelled)")
        state["jobs"].remove(job)
        lock.save(state)
    print(f"[queue] cancelled {job_id} ({job['label']})")
    return {"cancelled": job_id}


def resolve_job(job_id):
    """Clear a HANDLED worklist item -- a failure I fixed/re-dispatched, an
    unconverged run I reviewed, or a gate whose work is merged. Same safe
    state-entry removal as cancel_job, distinct verb + log so the dashboard
    worklist reads as 'handled' rather than 'abandoned'. Refuses running (kill by
    pid) and pending (use cancel -- nothing has been handled yet)."""
    with _Locked() as lock:
        state = lock.load()
        job = next((j for j in state["jobs"] if j.get("id") == job_id), None)
        if job is None:
            raise QueueActionError(f"job not found: {job_id}")
        if job.get("status") not in ("done", "done_unconverged", "failed", "paused", "blocked"):
            raise QueueActionError(
                f"only finished jobs can be resolved (this one is {job.get('status')!r})")
        state["jobs"].remove(job)
        lock.save(state)
    print(f"[queue] resolved {job_id} ({job['label']})")
    return {"resolved": job_id}


def cmd_resolve(args):
    try:
        resolve_job(args.job_id)
    except QueueActionError as e:
        sys.exit(f"[queue] {e}")


def cmd_promote(args):
    try:
        promote_job(args.job_id, preempt=args.preempt, force=args.force)
    except QueueActionError as e:
        sys.exit(f"[queue] {e}")


def cmd_resume(args):
    try:
        # A job paused as "verify_uninformative" would pause again on the next
        # task_complete, because the guard is a property of the dispatch, not the
        # session. The auto-resume watchdog never touches this reason, so reaching
        # here means a HUMAN chose to resume having read why it paused. Clear the
        # guard once, loudly, rather than looping them through the same pause.
        # Resolve the (possibly prefix) id to exactly one job id ONCE, and use
        # that resolved id for BOTH the guard-clear here and resume_job below.
        # resume_job matches on the FULL id, so a prefix that cleared the guard
        # here would otherwise mutate+save state and then fail 'job not found'.
        # Prefer an exact-id hit over a prefix hit.
        resolved_id = None
        with _Locked() as _lk:
            _st = _lk.load()
            _match = None
            for _j in _st["jobs"]:
                if _j.get("id") == args.job_id:
                    _match = _j
                    break
                if _match is None and str(_j.get("id", "")).startswith(args.job_id):
                    _match = _j
            if _match is not None:
                resolved_id = _match["id"]
                if _match.get("verify_failed_at_baseline"):
                    _match["verify_failed_at_baseline"] = False
                    print(f"[queue] resume {_match['id']}: clearing the verify-baseline guard "
                          f"(its verify was failing at enqueue, so it cannot prove the work "
                          f"landed -- you are resuming with that known). Check the diff by hand.")
                    _lk.save(_st)
        resume_job(resolved_id or args.job_id)
    except QueueActionError as e:
        sys.exit(f"[queue] {e}")


def cmd_cancel(args):
    try:
        cancel_job(args.job_id)
    except QueueActionError as e:
        sys.exit(f"[queue] {e}")


def cmd_run(args):
    global _daemon_lock_fh
    w = worker()
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    # Single-instance guard: a second accidental `run` must not race the first
    # over lane claims. Non-blocking exclusive flock held for our whole life;
    # released automatically on death (including SIGKILL).
    DAEMON_LOCK_PATH.touch(exist_ok=True)
    _daemon_lock_fh = open(DAEMON_LOCK_PATH, "w")
    try:
        fcntl.flock(_daemon_lock_fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print("[queue] another 'ollama-queue.py run' daemon already holds the daemon lock -- "
              "not starting a second one. Kill it first if you meant to replace it.", file=sys.stderr)
        sys.exit(1)

    # Launched via `nohup ... & disown`, stdout is a file and Python would
    # block-buffer it -- dispatch lines (the whole point of this tool being
    # observable) would sit in the buffer for minutes. Line-buffer instead so
    # the log is readable live with plain `tail -f`.
    try:
        sys.stdout.reconfigure(line_buffering=True)
        sys.stderr.reconfigure(line_buffering=True)
    except (AttributeError, ValueError):
        pass

    # pid -> (lane_url, Popen, log file handle): jobs THIS daemon launched. Keyed by
    # pid, NOT lane_url, because the dual-slot exception (see slot_decision) can put
    # TWO procs on ONE lane_url at once -- a lane_url-keyed dict would silently drop
    # the first proc's handle when the second launched, leaking its log fd and losing
    # the ability to reap it.
    active = {}
    adopted = {}  # pid -> job_id: running jobs from a previous (dead) daemon whose worker is still alive

    with _Locked() as lock:
        state = lock.load()
        for j in state["jobs"]:
            if j.get("status") != "running":
                continue
            pid = j.get("pid")
            if not _pid_alive(pid):
                # If the worker exited via its graceful-pause path, its log still carries the
                # RESUMABLE TRANSCRIPT marker even though we never got to reap it -- preserve
                # the pause (and its transcript) instead of silently restarting from scratch.
                resume_from = _parse_resume_transcript(j.get("log_path"))
                if resume_from:
                    j["status"] = "paused"
                    j["resume_transcript"] = resume_from
                    j["pause_reason"], j["pause_meta"] = _read_pause_info(resume_from)
                    print(f"[queue] {j['id']} was marked running but pid {pid} is dead and its log shows a "
                          f"clean pause -- keeping it paused (resumable transcript at {resume_from})")
                else:
                    print(f"[queue] {j['id']} was marked running but pid {pid} is dead -- requeuing")
                    j["status"] = "pending"
                j["pid"] = None
                j["lane"] = None
            else:
                adopted[pid] = j["id"]
                print(f"[queue] adopting orphaned job {j['id']} (worker pid {pid} still alive from a previous daemon) -- "
                      f"will requeue it when that worker exits")
        lock.save(state)

    def _shutdown(signum, _frame):
        # Best effort: close our log handles so they're not left dangling.
        # Running jobs stay marked "running"; the next start recovers them.
        for _pid, (_lane_url, _proc, logf) in list(active.items()):
            try:
                logf.close()
            except Exception:
                pass
        print(f"[queue] received signal {signum}, shutting down (running jobs will be recovered on next start)")
        sys.exit(128 + signum)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    print(f"[queue] daemon starting (pid {os.getpid()}), poll interval {args.poll_interval}s")

    while True:
        with _Locked() as lock:
            state = lock.load()
            changed = False
            # Terminal jobs to hand to the advisory gate -- fired AFTER lock.save()
            # below (not inline in the reap loop) so the hook never races the daemon
            # and reads a pre-save 'running' row (finding #2). See the fire site.
            gated_jobs = []

            # Reap our own finished processes. list(...) snapshot makes the
            # in-loop `del active[pid]` safe; nothing else mutates
            # `active` mid-iteration (launches happen in a separate loop below).
            for pid, (lane_url, proc, logf) in list(active.items()):
                ret = proc.poll()
                if ret is None:
                    continue
                try:
                    logf.close()  # closed exactly once here; launch failures close it before re-raising
                except Exception:
                    pass
                del active[pid]
                job = next((j for j in state["jobs"] if j.get("pid") == proc.pid), None)
                if job is None:
                    # State was edited/reloaded without this job. Don't crash
                    # the daemon over one unmatchable process -- log and move on.
                    print(f"[queue] WARNING: process {proc.pid} (lane {_lane_name(lane_url)}) finished but "
                          f"no matching job in state -- skipping", file=sys.stderr)
                    continue
                job["exit_code"] = ret
                job["pid"] = None
                if ret == w.EXIT_CODE_PAUSED:
                    # Worker's graceful-pause exit (external SIGTERM from a promote, or the
                    # model's own review gate): resumable, NOT a failure. The transcript path
                    # comes from the worker's own greppable marker line in this job's log.
                    resume_from = _parse_resume_transcript(job.get("log_path"))
                    if resume_from:
                        job["status"] = "paused"
                        job["resume_transcript"] = resume_from
                        job["pause_reason"], job["pause_meta"] = _read_pause_info(resume_from)
                        print(f"[queue] {job['id']} ({job['label']}) PAUSED on {_lane_name(lane_url)} -- "
                              f"resumable transcript at {resume_from}")
                    else:
                        # Exit code says paused but the marker line is missing (log truncated,
                        # worker bug?) -- keep it resumable anyway; without a recorded
                        # transcript _build_cmd falls back to relaunching from scratch.
                        job["status"] = "paused"
                        job["error"] = ("worker exited paused but no RESUMABLE TRANSCRIPT line in its log "
                                        "-- resuming will restart from scratch")
                        print(f"[queue] {job['id']} ({job['label']}) PAUSED on {_lane_name(lane_url)} "
                              f"(WARNING: no resumable transcript found in its log)")
                elif ret == 0:
                    job["status"] = "done"
                    print(f"[queue] {job['id']} ({job['label']}) finished on {_lane_name(lane_url)}, exit {ret}")
                elif ret == w.EXIT_CODE_DONE_UNCONVERGED:
                    # Mirror image of the vacuous-pass guard (2026-08-29, github-projects-bf
                    # caught this live): the loop didn't exit tidily, but --verify passed on
                    # real completed work. Distinct status, NOT "failed" -- a reviewer
                    # triaging by status alone must not see this as discardable.
                    job["status"] = "done_unconverged"
                    print(f"[queue] {job['id']} ({job['label']}) DONE (unconverged, verify "
                          f"passed) on {_lane_name(lane_url)}, exit {ret} -- real completed "
                          f"work, just didn't stop cleanly. Worth reviewing, not discarding.")
                else:
                    job["status"] = "failed"
                    print(f"[queue] {job['id']} ({job['label']}) FAILED on {_lane_name(lane_url)}, exit {ret}")
                # Advisory auto-gate on any terminal (non-resumable) job. COLLECTED here,
                # FIRED after lock.save() below: firing inline would race the daemon --
                # the gate reads ollama-queue-state.json off disk, which still holds the
                # pre-save 'running' row until save() lands, so an inline fire made the
                # gate abstain on exit/baseline (finding #2). The sidecar (_persist) is the
                # durable record either way, but firing post-save also stops the hook from
                # ever seeing a misleading stale row.
                if job["status"] in ("done", "done_unconverged", "failed"):
                    gated_jobs.append(job)
                changed = True

            # Reap adopted orphans: previous daemon died, its worker kept running;
            # now that it's exited, requeue the job (exit code unknown -- we can't
            # waitpid a non-child). Same retry semantics as the dead-pid case above.
            for pid, job_id in list(adopted.items()):
                if _pid_alive(pid):
                    continue
                del adopted[pid]
                job = next((j for j in state["jobs"] if j.get("id") == job_id), None)
                if job is not None and job.get("status") == "running":
                    # Same pause-marker check as the daemon-start recovery above: a promote may
                    # have SIGTERM'd this orphan, and its log still carries the marker even
                    # though we can't waitpid it for the exit code.
                    resume_from = _parse_resume_transcript(job.get("log_path"))
                    if resume_from:
                        print(f"[queue] orphaned worker pid {pid} for {job_id} exited via a clean "
                              f"pause -- keeping it paused (resumable transcript at {resume_from})")
                        job["status"] = "paused"
                        job["resume_transcript"] = resume_from
                        job["pause_reason"], job["pause_meta"] = _read_pause_info(resume_from)
                    else:
                        print(f"[queue] orphaned worker pid {pid} for {job_id} exited -- requeuing "
                              f"(exit code unknown; the daemon that launched it had died)")
                        job["status"] = "pending"
                    job["pid"] = None
                    job["lane"] = None
                    changed = True

            # Lanes occupied by ANY running job in shared state (not just ours) --
            # belt-and-braces on top of the daemon lock, so a lane is never
            # double-booked even if state was written by an earlier instance.
            # Launch pending jobs, FIFO order. Occupancy is read FRESH from shared
            # state for each candidate lane (never a pre-loop snapshot), so a job
            # launched earlier in THIS same tick -- already flipped to
            # status="running" with its lane set below -- is counted immediately.
            # That freshness is what keeps both the strict one-per-lane default AND
            # the second-slot exception race-safe: two ticks, or two jobs in one
            # tick, can never both read the same slot as free (the belt-and-braces
            # the daemon lock already provides, re-derived from state so it also
            # holds against a lane occupied by an earlier daemon instance's job).
            _jobs_by_id = {j["id"]: j for j in state["jobs"]}
            for job in state["jobs"]:
                if job.get("status") != "pending":
                    continue
                # Chain gate: hold until the `after` dep is done; block (and cascade)
                # if it can never satisfy. Runs BEFORE lane/slot logic so a waiting
                # chain step never claims a lane.
                _dep_action, _dep_reason = dependency_decision(job, _jobs_by_id)
                if _dep_action == "wait":
                    continue
                if _dep_action == "blocked":
                    job["status"] = "blocked"
                    job["error"] = f"blocked: {_dep_reason}"
                    changed = True
                    print(f"[queue] {job['id']} ({job['label']}) BLOCKED -- {_dep_reason}")
                    for _bid in _cascade_blocked(state["jobs"]):
                        print(f"[queue] {_bid} BLOCKED (cascade: upstream chain step did not converge)")
                    continue
                chosen = None
                for lane_url in _candidate_lanes(job, w):
                    name = _lane_name(lane_url)
                    # A legacy hand-written chain on this lane is opaque -- we can't
                    # read its model/task_kind, so we can never prove the same-model
                    # + research guarantee the second slot requires. Treat it as a
                    # full, un-shareable occupant and serialize.
                    if _external_dispatch_running(name):
                        continue
                    running_here = [j for j in state["jobs"]
                                    if j.get("status") == "running" and j.get("lane")
                                    and _lane_name(j.get("lane")) == name]
                    if not running_here:
                        chosen = lane_url  # normal first claim on a free lane
                        break
                    # Occupied -> the guarded second-slot exception (see slot_decision).
                    # Positively confirm the server's slot capacity, and only then its
                    # VRAM fit, from the LIVE server; both fail toward serial when any
                    # signal is missing. vram is gated behind capacity>=2 so a
                    # single-slot lane (the common case) does no extra /props I/O.
                    capacity = _lane_slot_capacity(lane_url)
                    vram_fits = capacity >= 2 and _second_slot_vram_fits(lane_url, job["num_ctx"])
                    allow, reason = slot_decision(
                        job["model"], job.get("task_kind"),
                        [{"model": j.get("model"), "task_kind": j.get("task_kind")}
                         for j in running_here],
                        capacity, vram_fits)
                    if allow:
                        print(f"[queue] second-slot GRANTED for {job['id']} ({job['label']}) "
                              f"on {name}: {reason}")
                        chosen = lane_url
                        break
                    # Denied: leave the job queued and try its other candidate lanes,
                    # if any (an auto job may still take a free Unraid lane). Denials
                    # are the normal steady state, so they are not logged per-poll.
                if chosen is None:
                    continue

                # Studio-lane safety: whichever process is about to claim
                # this lane, clear the OTHER process's memory footprint off
                # Studio first -- see LLAMA_SERVER_QWEN38_URL comment for the
                # OOM incident this exists to prevent. Best-effort; a failed
                # evict surfaces as a clear compute-error on launch, not a
                # silent hang.
                if chosen == LLAMA_SERVER_QWEN38_URL:
                    _evict_studio_ollama_models()
                    if not _ensure_llama_server_up(job["num_ctx"]):
                        job["status"] = "failed"
                        job["error"] = "llama-server bypass failed to come up (see daemon log)"
                        changed = True
                        continue
                elif chosen == w.KNOWN_OLLAMA_HOSTS["studio"]["url"]:
                    _evict_studio_ollama_models(exclude_model=job["model"])
                    _stop_llama_server_bypass()

                log_path = LOG_DIR / f"{job['id']}-{_safe_label(job['label'])}.log"
                # Runner jobs (e.g. studio-research.py) are not given the worker's
                # --live-log flag (they own their output under the 5-flag contract),
                # so their stdout would only reach the daemon .log and the dashboard
                # livelog viewer would show nothing. Point their stdout straight at
                # the livelog file so research runs stream on the dashboard exactly
                # like coding jobs. (studio-research prints progress via log().)
                if job.get("runner") and job.get("live_log_path"):
                    log_path = Path(job["live_log_path"])
                logf = None
                try:
                    cmd = _build_cmd(job, chosen)  # raises OSError if task file vanished since enqueue
                    logf = open(log_path, "w")
                    # Stamp the launch so ollama-worker.py can tell a queued run from a
                    # direct one -- see its --direct-ok guard. Without this every queued
                    # job would be refused.
                    _env = {**os.environ, "OLLAMA_DISPATCH_VIA_QUEUE": job["id"]}
                    proc = subprocess.Popen(cmd, cwd=job["cwd"], stdout=logf,
                                            stderr=subprocess.STDOUT, env=_env)
                except Exception as e:
                    # One bad job must not kill the daemon. Close the log handle
                    # if it was opened before the failure (no leak), mark the job
                    # failed with a reason, and keep going.
                    if logf is not None:
                        try:
                            logf.close()
                        except Exception:
                            pass
                    job["status"] = "failed"
                    job["error"] = f"launch failed: {e}"
                    changed = True
                    print(f"[queue] {job['id']} ({job['label']}) launch failed on {_lane_name(chosen)}: {e}",
                          file=sys.stderr)
                    continue

                active[proc.pid] = (chosen, proc, logf)
                job["status"] = "running"
                job["pid"] = proc.pid
                job["lane"] = _lane_name(chosen)
                job["launched_at"] = datetime.now(timezone.utc).isoformat()
                job["log_path"] = str(log_path)
                changed = True
                # No separate busy-lane snapshot to maintain: the next pending job's
                # occupancy check re-reads state["jobs"], where THIS job is now
                # status="running" with its lane set, so a same-lane follow-up sees
                # it immediately. The historical bug this replaces (2026-08-28: two
                # head-to-head jobs for lane "studio" via two different URLs both
                # launching in one tick and OOMing Metal) is closed by the fresh
                # per-candidate state read plus slot_decision, which denies a second
                # job on a busy lane unless every dual-slot guarantee holds.
                print(f"[queue] launched {job['id']} ({job['label']}) on {job['lane']} pid={proc.pid} -> {log_path}")

            if changed:
                state["jobs"] = prune_finished_jobs(state["jobs"])
                lock.save(state)

        # Fire the advisory gate AFTER releasing the state lock and AFTER the save+prune
        # above (finding #2). Each _fire_gate_on_complete Popens gate-on-complete.py, which
        # reads ollama-queue-state.json off disk; firing here (not inline in the reap loop)
        # means it never observes the stale pre-save 'running' row -- it reads the durable
        # .done.json sidecar (_persist_job_completion writes it before the Popen). Outside
        # the lock so a subprocess spawn never holds the flock. Fire-and-forget: it can
        # never raise into the daemon (wrapped) and does not touch state.
        for _gj in gated_jobs:
            _fire_gate_on_complete(_gj)

        # Outside the state lock (network I/O, no state mutation) -- self-healing safeguard
        # against ANY caller (not just our own dispatches) loading a TEMPLATE_BUG_MODELS model
        # onto Studio's native Ollama. See _evict_template_bug_models's own docstring.
        _evict_template_bug_models()

        # Bounded auto-resume for context/iteration pauses -- see _auto_resume_paused_jobs's
        # own docstring for the full policy (never touches external_sigterm pauses, capped
        # bumps, host-aware ceilings).
        _auto_resume_paused_jobs()

        time.sleep(args.poll_interval)


def _self_test():
    """Unit-test the pure dual-slot decision (slot_decision) -- no GPU, no I/O.
    Same convention as dispatch-ack-reconcile.py --self-test."""
    ok = True

    def check(name, got, want):
        nonlocal ok
        if got == want:
            print(f"PASS {name}")
        else:
            ok = False
            print(f"FAIL {name}: got {got!r} want {want!r}")

    M = "qwen3.8:27b-q8_0"
    other = "qwen3:14b"

    # allow: same model + one side research + a free slot (1/2) + VRAM fits
    allow, why = slot_decision(M, "coding",
                               [{"model": M, "task_kind": "research"}],
                               slot_capacity=2, vram_fits=True)
    check("same model + research side + free slot + VRAM fits -> ALLOW", allow, True)

    # allow: the research side can be the NEW job too (symmetry)
    allow, _ = slot_decision(M, "research",
                             [{"model": M, "task_kind": "coding"}],
                             slot_capacity=2, vram_fits=True)
    check("research is the NEW job (symmetry) -> ALLOW", allow, True)

    # deny: same model but BOTH coding (no research side)
    allow, _ = slot_decision(M, "coding",
                             [{"model": M, "task_kind": "coding"}],
                             slot_capacity=2, vram_fits=True)
    check("same model + both coding -> DENY", allow, False)

    # deny: different model resident (any kinds) -- would need a load/swap
    allow, _ = slot_decision(M, "research",
                             [{"model": other, "task_kind": "research"}],
                             slot_capacity=2, vram_fits=True)
    check("different model (research kinds) -> DENY", allow, False)

    # deny: same model + research but capacity already full (2/2)
    allow, _ = slot_decision(M, "research",
                             [{"model": M, "task_kind": "research"},
                              {"model": M, "task_kind": "coding"}],
                             slot_capacity=2, vram_fits=True)
    check("same model + research but 2/2 full -> DENY", allow, False)

    # deny: same model + research but VRAM does not fit
    allow, _ = slot_decision(M, "research",
                             [{"model": M, "task_kind": "coding"}],
                             slot_capacity=2, vram_fits=False)
    check("same model + research but VRAM does not fit -> DENY", allow, False)

    # deny: server confirms only ONE slot (the llama-server --parallel 1 default)
    allow, _ = slot_decision(M, "research",
                             [{"model": M, "task_kind": "coding"}],
                             slot_capacity=1, vram_fits=True)
    check("server has only 1 confirmed slot -> DENY", allow, False)

    # allow: lane empty -> normal first claim (capacity/vram irrelevant)
    allow, _ = slot_decision(M, "coding", [], slot_capacity=1, vram_fits=False)
    check("lane empty -> ALLOW (normal first claim)", allow, True)

    # deny: same model + research + free slot + VRAM fits, but a co-resident job
    # has an UNKNOWN/blank model (can't prove no-swap) -> deny
    allow, _ = slot_decision(M, "research",
                             [{"model": None, "task_kind": "coding"}],
                             slot_capacity=2, vram_fits=True)
    check("co-resident job with unknown model -> DENY", allow, False)

    # --- dependency_decision + chain gating -----------------------------------
    def dep(after, jobs):
        return dependency_decision({"after": after}, {j["id"]: j for j in jobs})[0]

    check("no dependency -> launch",
          dependency_decision({}, {})[0], "launch")
    check("dep done -> launch",
          dep("a", [{"id": "a", "status": "done"}]), "launch")
    check("dep running -> wait",
          dep("a", [{"id": "a", "status": "running"}]), "wait")
    check("dep pending -> wait",
          dep("a", [{"id": "a", "status": "pending"}]), "wait")
    check("dep failed -> blocked",
          dep("a", [{"id": "a", "status": "failed"}]), "blocked")
    check("dep done_unconverged -> blocked",
          dep("a", [{"id": "a", "status": "done_unconverged"}]), "blocked")
    check("dep missing/gone -> blocked",
          dep("a", []), "blocked")

    # chain of 3, middle blocks -> third cascades to blocked
    chain = [
        {"id": "s1", "status": "failed", "after": None},
        {"id": "s2", "status": "pending", "after": "s1"},
        {"id": "s3", "status": "pending", "after": "s2"},
    ]
    # s2's dep (s1) failed -> s2 blocked
    check("chain middle: dep failed -> blocked",
          dependency_decision(chain[1], {j["id"]: j for j in chain})[0], "blocked")
    chain[1]["status"] = "blocked"
    newly = _cascade_blocked(chain)
    check("chain cascade blocks the third step", "s3" in newly, True)
    check("third step ends up blocked", chain[2]["status"], "blocked")

    # chain-final gating: a chain STEP is skipped, chain_final + non-chain gate
    check("chain step (not final) -> not gated",
          _should_gate_job({"chain": "c1", "chain_final": False}), False)
    check("chain final -> gated",
          _should_gate_job({"chain": "c1", "chain_final": True}), True)
    check("non-chain job -> gated",
          _should_gate_job({}), True)

    # --- _fire_gate_on_complete routing (finding #1) --------------------------
    # The subprocess.Popen in _fire_gate_on_complete is the ONLY thing that
    # re-invokes gate-on-complete.py on a completed review job, which is what
    # routes a gate-/regate- job into merge_review. The old code early-returned
    # for gate-/regate- BEFORE the Popen, so the merge NEVER ran and two-tier
    # review was disconnected. Assert the hook now REACHES Popen for a gate-
    # label (and still skips persist + re-gate for it), while image/pet/draft
    # labels -- which have no review to merge -- reach nothing.
    import subprocess as _sp
    _seen = {"popen": 0, "persist": 0}
    _orig_popen = _sp.Popen
    _orig_persist = globals()["_persist_job_completion"]

    def _fake_popen(*a, **k):
        _seen["popen"] += 1
        class _P:      # never started; just a stand-in so the caller doesn't blow up
            pid = -1
        return _P()

    def _fake_persist(job):
        _seen["persist"] += 1

    def _fire(label, model="qwen3.8:27b-q4_K_M"):
        _seen["popen"] = 0
        _seen["persist"] = 0
        _fire_gate_on_complete({"id": "jt", "label": label, "model": model,
                                "cwd": "/tmp", "verify": None})
        return _seen["popen"], _seen["persist"]

    try:
        _sp.Popen = _fake_popen
        globals()["_persist_job_completion"] = _fake_persist

        popen_n, persist_n = _fire("gate-abc123")
        check("gate- label REACHES Popen (merge_review re-invoked)", popen_n, 1)
        check("gate- label SKIPS _persist_job_completion (no diff to persist)", persist_n, 0)

        popen_n, _ = _fire("regate-abc123")
        check("regate- label REACHES Popen (authoritative merge re-invoked)", popen_n, 1)

        popen_n, _ = _fire("pet-portrait", model="image")
        check("image/pet- label does NOT reach Popen (nothing to merge)", popen_n, 0)

        popen_n, _ = _fire("draft-somejob")
        check("draft- label does NOT reach Popen", popen_n, 0)

        popen_n, persist_n = _fire("dashboard-newjobs-fix")
        check("normal dispatch REACHES Popen", popen_n, 1)
        check("normal dispatch PERSISTS its sidecar first", persist_n, 1)
    finally:
        _sp.Popen = _orig_popen
        globals()["_persist_job_completion"] = _orig_persist

    print("SELF_TEST_OK" if ok else "SELF_TEST_FAILED")
    return ok


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    e = sub.add_parser("enqueue")
    e.add_argument("--model", required=True)
    e.add_argument("--host", default="auto", help="auto|studio|unraid|<explicit URL>")
    e.add_argument("--cwd", default=None,
                    help="Working dir for the dispatch. Legacy path -- prefer --repo for enforced "
                         "isolation. Warned (unless --allow-unisolated) if it is not a linked worktree.")
    e.add_argument("--repo", default=None,
                    help="Git repo to auto-create a throwaway isolated worktree from (enforced "
                         "per-dispatch isolation). Mutually exclusive with --cwd.")
    e.add_argument("--base-ref", default=None,
                    help="Base ref for the --repo auto-worktree branch (default: repo HEAD).")
    e.add_argument("--subdir", default=None,
                    help="Subdir within the auto-worktree to use as cwd (e.g. sidecar).")
    e.add_argument("--setup", default=None,
                    help="Env-parity command run in the (worktree) cwd before preflight, e.g. "
                         "'npm ci'. A non-zero exit refuses the enqueue (no dep-starved jobs).")
    e.add_argument("--allow-unisolated", action="store_true",
                    help="Silence the warning when --cwd is not an isolated worktree.")
    e.add_argument("--task-file", required=True)
    e.add_argument("--runner", default=None,
                    help="Allowlisted alternate executable to run instead of ollama-worker.py "
                         "(currently only ~/bin/studio-research.py). The queue passes ONLY "
                         "--model/--host/--num-ctx/--cwd/--task-file; the runner owns everything "
                         "else. For a multi-call orchestrator that must run as ONE queue job so it "
                         "stays visible in queue state and under the VRAM-collision guards.")
    e.add_argument("--task-kind", default=None, choices=["coding", "research"])
    e.add_argument("--manual-tools", action="store_true")
    e.add_argument("--api", default="ollama", choices=["ollama", "openai"])
    e.add_argument("--verify", default=None)
    e.add_argument("--allow-no-verify", action="store_true",
                    help="Acknowledge a coding dispatch with NO --verify: enqueue it as a "
                         "SCOPE-ONLY/ADVISORY run (correctness rides on human review). Without "
                         "this flag a no-verify coding dispatch is REFUSED -- it has no goal "
                         "signal and thrashes to max-iters (see job d31d96d23b29).")
    e.add_argument("--num-ctx", type=int, default=None,
                   help="Explicit context window. When given it ALWAYS wins and disables "
                        "auto-ctx sizing AND auto-split. When OMITTED, a safe start bucket is "
                        "computed from task size + history (see --auto-ctx / --no-auto-ctx).")
    e.add_argument("--auto-ctx", dest="auto_ctx", action="store_true", default=True,
                   help="Compute the start num_ctx from task size + dispatch history when "
                        "--num-ctx is omitted. This is the DEFAULT; the flag is accepted for "
                        "explicitness. An explicit --num-ctx always overrides it.")
    e.add_argument("--no-auto-ctx", dest="auto_ctx", action="store_false",
                   help="Disable auto-ctx sizing; fall back to the top bucket "
                        f"({CTX_BUCKETS[-1]}) when --num-ctx is omitted.")
    e.add_argument("--auto-split", action="store_true",
                   help="Opt in to splitting a task into per-target sub-dispatches (a chain in "
                        "one worktree, gated once) when a clean decomposition exists. OFF by "
                        "default; splitting also triggers automatically only when the estimate "
                        "provably overflows the top bucket.")
    e.add_argument("--no-split", action="store_true",
                   help="Never split, even on overflow. Escape hatch that fully bypasses PART B.")
    e.add_argument("--max-iters", type=int, default=None,
                   help="Omit to let ollama-worker.py's DEFAULT_MAX_ITERS (%d) govern -- "
                        "the queue no longer forces a lower value (was silently 20)." % WORKER_DEFAULT_MAX_ITERS)
    e.add_argument("--temperature", type=float, default=0)
    e.add_argument("--chat-timeout", type=int, default=None,
                    help="Seconds per chat call; omit to use ollama-worker.py's own default "
                         "(1200s -- too short for large-model/large-ctx jobs, which have needed "
                         "3000s in every hand-written dispatch tonight)")
    e.add_argument("--max-tokens", type=int, default=None,
                    help="Per-response output-token cap (num_predict); omit to use "
                         "ollama-worker.py's default (8192). Raise for THINKING models "
                         "(e.g. nemotron-cascade-2), which otherwise spend the whole budget "
                         "reasoning and get truncated before emitting a tool call -- use 16384.")
    e.add_argument("--capture-final-as", default=None,
                    help="Deliverable filename (relative to cwd). If the run ends without that file "
                         "but with a final text answer, the worker saves the text AS the file "
                         "(capture=fallback) so it's scored, not lost. For review tasks: REVIEW.md.")
    e.add_argument("--no-preflight", action="store_true",
                    help="Skip the pre-flight smoke-test/timing of --verify against the cwd. "
                         "By default enqueue runs the verify once first and REFUSES if it can't "
                         "execute or would exceed the worker's 300s verify timeout (a non-zero "
                         "exit is allowed -- fix-verifying tasks fail at baseline).")
    e.add_argument("--label", default=None)
    e.add_argument("--scored-arm", action="store_true",
                   help="Mark this as a scored bake-off arm (set by bakeoff-fire.py). Forwarded "
                        "to the worker as --scored-arm: implies verify-failed-at-baseline and turns "
                        "off baseline-diagnostic subtraction, and tags the run in dispatch-metrics "
                        "so scored/unscored runs are never pooled.")
    e.add_argument("--after", default=None,
                    help="Chain dependency: this job stays pending until the named job (full id "
                         "or unambiguous prefix) is 'done'. If that job fails/does-not-converge, "
                         "this job (and its downstream) go to the terminal 'blocked' status.")
    e.add_argument("--chain", default=None,
                    help="Optional chain group tag. A chain STEP (in a chain but not --chain-final) "
                         "is NOT gated per-step; the chain is gated once, on its --chain-final job.")
    e.add_argument("--chain-final", action="store_true",
                    help="Mark the LAST step of a chain: gate-on-complete fires here (not on the "
                         "intermediate steps).")
    e.add_argument("--front", action="store_true",
                   help="Insert this job ahead of all currently-pending jobs so it launches next; "
                        "does not preempt a running job.")
    e.set_defaults(func=cmd_enqueue)

    s = sub.add_parser("status")
    s.set_defaults(func=cmd_status)

    p = sub.add_parser("promote", help="Move a pending job to the front of the queue so it "
                                       "launches next. By DEFAULT this does NOT kill the running "
                                       "job -- it jumps the queue and launches when the lane frees. "
                                       "Pass --preempt to also SIGTERM (graceful pause) the running "
                                       "job so the promoted one takes the lane immediately.")
    p.add_argument("job_id")
    p.add_argument("--preempt", dest="preempt", action="store_true",
                   help="Also SIGTERM the job running on the promoted job's lane (graceful pause, "
                        "exit code 3, resumable) so the promoted job takes the lane now. Destroys "
                        "no work -- the paused job resumes from its saved transcript.")
    p.add_argument("--no-preempt", dest="preempt", action="store_false",
                   help="(default) Jump the queue but let the running job finish its lane first.")
    p.add_argument("--force", action="store_true",
                   help=f"With --preempt, override the min-progress guard and SIGTERM a job even "
                        f"if it launched < {PREEMPT_MIN_PROGRESS_S}s ago.")
    p.set_defaults(func=cmd_promote, preempt=False)

    rs = sub.add_parser("resume", help="Requeue a paused job; it relaunches from its saved "
                                       "transcript (--resume) on the next free lane")
    rs.add_argument("job_id")
    rs.set_defaults(func=cmd_resume)

    cn = sub.add_parser("cancel", help="Remove a pending/done/failed/paused job's state entry "
                                        "outright (a running job must be killed by pid instead, "
                                        "not cancelled -- this refuses that case)")
    cn.add_argument("job_id")
    cn.set_defaults(func=cmd_cancel)

    rv = sub.add_parser("resolve", help="Clear a HANDLED finished job (failure fixed, "
                        "run reviewed, or gate merged). Like cancel but the worklist verb.")
    rv.add_argument("job_id")
    rv.set_defaults(func=cmd_resolve)

    r = sub.add_parser("run")
    r.add_argument("--poll-interval", type=int, default=15)
    r.set_defaults(func=cmd_run)

    args = ap.parse_args()
    args.func(args)


if __name__ == "__main__":
    # --self-test runs the pure dual-slot decision table (no GPU/state/network) and
    # exits, matching dispatch-ack-reconcile.py's convention. Checked before argparse
    # so it needs no subcommand.
    if "--self-test" in sys.argv[1:]:
        sys.exit(0 if _self_test() else 1)
    main()
