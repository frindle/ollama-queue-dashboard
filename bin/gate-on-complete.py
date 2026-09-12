#!/usr/bin/env python3
"""Auto-gate hook: run the Studio gate on a completed dispatch. ADVISORY ONLY.

THE MODEL CALL GOES THROUGH THE QUEUE, NEVER DIRECTLY. Penn's constraint, and it
restores my own original design -- the first version called ollama at
127.0.0.1:11434 via subprocess, which would have run CONCURRENTLY with real
dispatches: contending for the GPU, bypassing the VRAM guard, and forcing model
swaps. Everything this project learned about serialising GPU work, discarded in
the wiring. The queue exists precisely to schedule this.

So the gate splits by what needs a GPU:

  DECIDABLE (scope, completeness, verify-quality) -> pure Python, run HERE, now.
  MODEL REVIEW                                    -> ENQUEUED as a --runner job.

Until the review job lands, .gate.json carries the decidable findings plus
review="pending". It must never look like a clean pass while the review is still
queued -- same rule as a visible skip.

Designed so the queue's integration is ONE CALL, not a feature:

    subprocess.Popen(["python3", str(Path.home()/"bin"/"gate-on-complete.py"),
                      "--job-id", job["id"], "--cwd", job["cwd"],
                      "--task-file", job["task_file"], "--verify", job.get("verify") or ""])

Everything else -- diff acquisition, skip handling, output placement -- lives here,
so ollama-queue.py owns none of it and can be reverted by deleting one call.

TWO DESIGN RULES, both learned the hard way today:

1. A SKIPPED GATE MUST BE VISIBLY SKIPPED. If a diff cannot be obtained, this
   still writes a .gate.json with verdict="skipped" and the reason. Absence of a
   verdict must never be readable as a pass -- that is how "no findings" gets
   confused with "not checked", which is the single most dangerous thing a gate
   can do.

2. NEVER BLOCKS, NEVER FAILS THE JOB. Exit code is always 0. A gate that can
   break a dispatch is worse than no gate: the dispatch is the real work, this is
   commentary on it.
"""
import argparse, importlib.util, json, os, re, subprocess, sys, time
from pathlib import Path

# HERE is where THIS file and its sibling checkers live (gate.py, signoff.py,
# verify-relevance.py). BIN is where the QUEUE's state lives (state file, logs,
# ollama-queue.py, handoff-emit.py). In production both are ~/bin; in a worktree
# canary they differ, and the checkers under test must be the worktree's.
HERE = Path(__file__).resolve().parent
BIN = Path(os.environ.get("GATE_BIN") or (Path.home() / "bin")).expanduser()
GATE = HERE / "gate.py"
SIGNOFF = HERE / "signoff.py"
RELEVANCE = HERE / "verify-relevance.py"
# GATE_TEST_MODE=1: no review enqueue, no notify, no handoff render. Reads of
# the queue state still happen (fail-open). Lets a canary drive the FULL
# decidable path (gate.py -> relevance -> signoff --evaluate -> --auto) against
# a fixture repo without touching the live queue.
TEST_MODE = os.environ.get("GATE_TEST_MODE") == "1"

# --- Two-tier gate routing (2026-09-04, Penn's design; wired after the 14b bake-off) ---
# The cheap PRE-gate runs on Unraid (qwen3:14b, ~10.4G VRAM @ num-ctx 6144 -- fits
# the 3080 with headroom, zero CPU spillover). Bake-off vs the Studio 27B over 16
# real .gate.json diffs (2026-09-04): 88% agreement, 0 false-FAIL, 2 false-PASS;
# catches 2/4 known-defect diffs. So the pre-gate is a cheap ESCALATION TRIGGER,
# never the authority: a NON-PASS pre-gate verdict escalates an authoritative
# re-gate to the Studio 27B (qwen3.8:27b-q4_K_M), whose verdict SUPERSEDES the
# pre-gate's in .gate.json. A pre-gate PASS is NON-TERMINAL by construction --
# signoff stays shadow-mode (auto-approves nothing) and Claude reads every diff
# at merge -- so a pre-gate false-PASS never causes a bad autonomous merge.
# The re-gate carries a distinct 'regate-' label so it does NOT re-fire the
# 'gate-' loop guard. Everything is env-overridable; to roll back to the
# single-tier 27B gate set GATE_PREGATE_MODEL=qwen3.8:27b-q4_K_M
# GATE_PREGATE_HOST=studio GATE_PREGATE_NUM_CTX=32768 GATE_TWO_TIER=0.
PREGATE_HOST    = os.environ.get("GATE_PREGATE_HOST", "unraid")
PREGATE_MODEL   = os.environ.get("GATE_PREGATE_MODEL", "qwen3:14b")
PREGATE_NUM_CTX = int(os.environ.get("GATE_PREGATE_NUM_CTX", "6144"))
REGATE_HOST     = os.environ.get("GATE_REGATE_HOST", "studio")
REGATE_MODEL    = os.environ.get("GATE_REGATE_MODEL", "qwen3.8:27b-q4_K_M")
REGATE_NUM_CTX  = int(os.environ.get("GATE_REGATE_NUM_CTX", "32768"))
TWO_TIER        = os.environ.get("GATE_TWO_TIER", "1") != "0"

# --- AUTO-FIX: bounded auto-requeue of a gate-FAILing dispatch (2026-09-11) ----
# When a completed CODING dispatch reaches a TERMINAL gate verdict that a re-run
# of the model could plausibly fix, requeue it -- same sealed verify, same
# worktree baseline, the gate's concerns fed back in as guidance -- so the human
# coordinator is not dragged into routine gate-fix loops. Only DECIDABLE,
# model-actionable failures requeue; anything ambiguous/undecidable ESCALATES.
#
# MODE, deliberately mirroring signoff.py's shadow rollout (feedback:
# suspect-the-grader / high-bar-for-model-is-the-problem). "shadow" (DEFAULT)
# computes and RECORDS the decision + the exact requeue command into .gate.json
# and AUTO-FIX-QUEUE.md, and enqueues NOTHING. "live" actually requeues. It ships
# shadow so Penn can read, over real jobs, whether the classifier fires correctly
# and the requeue commands are right, at zero risk of a loop or a tampered verify.
# Flip to live ONLY after the shadow log shows it agreeing with hand judgement.
AUTOFIX_MODE       = os.environ.get("GATE_AUTOFIX_MODE", "shadow").lower()
# Loop guard: a dispatch may be auto-requeued at most this many times before the
# harness gives up and escalates to a human. round 0 = original human dispatch,
# so MAX_ROUNDS=2 means at most attempts at round 1 and round 2.
AUTOFIX_MAX_ROUNDS = int(os.environ.get("GATE_AUTOFIX_MAX_ROUNDS", "2"))
# Trust the (authoritative 27B) MODEL REVIEWER's code-high as a requeue trigger?
# Default OFF: a reviewer code-high with no red verify is exactly the false-FAIL
# class the two-tier gate exists to contain, and a false-FAIL that auto-requeues
# burns GPU chasing a defect that is not there. Off => such a verdict ESCALATES.
AUTOFIX_TRUST_REVIEWER = os.environ.get("GATE_AUTOFIX_TRUST_REVIEWER", "0") == "1"


def _signoff_record(job_id: str) -> dict:
    """The signoff.json entry for a job, read THROUGH signoff.py.

    This is the readback that crashed every automated sign-off before
    2026-09-04: emit() called `_load(...)`, a helper that exists only inside
    signoff.py, so the readback raised NameError inside its try/except, the
    error was recorded as `signoff_error: "load: NameError: name '_load' is not
    defined"`, `signoff_required` stayed None, and the `--auto` step (gated on
    signoff_required) never fired -- so no dispatch ever received a verdict
    while every canary stayed green (none of them exercised this path).

    Going through the module rather than json.loads has one more property:
    signoff.py resolves the file via SIGNOFF_DIR, so the gate reads the same
    file signoff.py just wrote, in production and under a canary alike.
    """
    return (_signoff_mod().load_signoffs() or {}).get(job_id) or {}


_SIGNOFF_MOD = None


def _signoff_mod():
    global _SIGNOFF_MOD
    if _SIGNOFF_MOD is None:
        spec = importlib.util.spec_from_file_location("signoff", SIGNOFF)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        _SIGNOFF_MOD = mod
    return _SIGNOFF_MOD


def scaffold_digests(cwd: Path) -> dict:
    """{relpath: sha256} of the scaffold files the PREFLIGHT pinned for this
    worktree (TASK.md, verify.sh, test_fixture.py, ...), from the ledger
    signoff.py already reads. Empty when there is no ledger entry."""
    try:
        entry = _signoff_mod().ledger_entry_for(str(cwd)) or {}
        d = entry.get("scaffold_sha256") or {}
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _sha256(p: Path) -> str | None:
    import hashlib
    try:
        return hashlib.sha256(p.read_bytes()).hexdigest()
    except Exception:
        return None


def out_dir_for(a) -> Path:
    return (Path(a.out_dir).expanduser() if a.out_dir
            else BIN / "ollama-queue-logs")


def merge_review(a, out_dir: Path, prefix: str = "gate-",
                 authoritative: bool = False) -> int:
    """Fold a completed review job back into the parent .gate.json.

    Runs when the QUEUE finishes a review job this hook enqueued. Recomputes the
    verdict with the code findings included -- only a CODE high can fail, input
    findings still cap at concerns.

    prefix='gate-'  (authoritative=False): the cheap Unraid PRE-gate result. On a
      non-pass verdict it ESCALATES an authoritative re-gate to the Studio 27B
      (two-tier routing); a pass stands (non-terminal -- shadow signoff + Claude).
    prefix='regate-' (authoritative=True): the Studio 27B re-gate result. Its
      verdict SUPERSEDES the pre-gate's and is terminal -- it never escalates.
    """
    parent = a.job_label[len(prefix):]
    gate_json = out_dir / f"{parent}.gate.json"
    if not gate_json.exists():
        print(f"[gate] merge: no parent record {gate_json}")
        return 0
    payload = json.loads(gate_json.read_text())
    report = Path(a.cwd).expanduser() / "report.md"
    if not report.exists():
        payload["review"] = "failed (no report produced)"
        gate_json.write_text(json.dumps(payload, indent=1))
        print(f"[gate] {parent} review FAILED -- no report")
        return 0
    txt = report.read_text()
    m = re.search(r"^## VERDICT: (.+)$", txt, re.M)
    payload["review"] = "done"
    payload["review_verdict"] = m.group(1).strip() if m else "unknown"
    # Filter by SOURCE, not category. This dropped every code-category finding
    # before re-adding the review's rows, which was fine while 'review' was the
    # only producer of them -- it is not any more: a failed-verify finding is
    # category=code and would have been silently deleted the moment the review
    # landed, quietly restoring the pass this whole change exists to prevent.
    issues = [i for i in payload.get("issues", []) if i.get("source") != "review"]
    for row in re.finditer(r"^\| \d+ \| (\w+) \| `([^`]+)` \| (.+?) \|$", txt, re.M):
        sev, where, what = row.group(1).lower(), row.group(2), row.group(3)
        f, _, ln = where.partition(":")
        issues.append({"severity": sev, "file": f,
                       "line": int(re.sub(r"\D", "", ln) or 0),
                       "what": what.strip()[:180], "source": "review",
                       "category": "code"})
    payload["issues"] = issues[:25]
    code_high = [i for i in issues if i.get("category") == "code"
                 and i["severity"] == "high"]
    payload["counts"] = {
        "code_high": len(code_high),
        "code": sum(1 for i in issues if i.get("category") == "code"),
        "input": sum(1 for i in issues if i.get("category") == "input"),
        "total": len(issues)}
    # An UNPROVEN review (the model hit its token cap and did not finish) is
    # inconclusive, not a pass: a truncated review that surfaced no HIGH row
    # must not read as a clean pass. A real code_high still fails regardless.
    _review_unproven = str(payload.get("review_verdict", "")).upper().startswith("UNPROVEN")
    payload["verdict"] = ("fail" if code_high
                          else "concerns" if (issues or _review_unproven) else "pass")
    # ESCALATION WORTHINESS: a 'concerns' verdict is only worth an authoritative
    # 27B re-gate when it carries a finding the 27B can actually adjudicate -- a
    # code-category finding, a NON-input med+ severity, or an UNPROVEN (truncated)
    # review that never finished. A concerns verdict made entirely of low
    # input/scope flags ("this edited file was not named by the task; confirm it
    # was needed") is NOT one of those: the 27B re-asks the identical scope
    # question the coordinator answers at the merge diff-glance, adding no signal
    # while burning a Studio slot. Those stand as terminal 'concerns'.
    #
    # THE MED+ CLAUSE MUST EXCLUDE input findings the 27B CANNOT ADJUDICATE
    # (2026-09-11, job fd7b1d7cd7d6). The 27B reviews the DIFF; a completeness /
    # verify-quality concern is about the TASK/VERIFY TEXT, not the diff, so the
    # 27B comes back PASS "nothing survived", having burned a Studio slot.
    # fd7b1d7cd7d6's ONLY finding was severity=medium category=input
    # source=completeness ("Must-contain lists `Bearer` which already existed in
    # baseline") and the old clause (ANY med+ regardless of category/source)
    # escalated it needlessly.
    #
    # NOT a blanket `category != "input"` exclusion: scope findings are ALSO
    # category=input in gate.py, and one of them -- the MEDIUM "the diff edits the
    # VERIFY/CHECK that gated it" selftest finding (gate.py) -- IS diff-visible and
    # exactly the kind of thing a 27B should look at. So exclude by SOURCE: a med+
    # input finding escalates UNLESS its source is a task/verify-spec source the
    # diff reviewer can't adjudicate. code findings (any severity) still escalate
    # via _code_findings; scope med+ still escalates; an unproven review still
    # escalates.
    _NON_ADJUDICABLE = {"completeness", "verify-quality", "launch-baseline"}
    _med_plus = [i for i in issues if i.get("severity") in ("high", "medium")
                 and not (i.get("category") == "input"
                          and i.get("source") in _NON_ADJUDICABLE)]
    _code_findings = [i for i in issues if i.get("category") == "code"]
    _escalation_worthy = bool(_code_findings or _med_plus or _review_unproven)
    # not_checked was written by the decidable pass and MUST survive the merge:
    # the review landing does not retroactively check what scope or completeness
    # abstained on, and this is the render most likely to be read as final.
    nc = payload.get("not_checked") or []
    nc_s = (" not_checked=" + ",".join(x.split(" (")[0] for x in nc)) if nc else ""
    # An UNTRUSTED input survives the review landing untouched: a model reading
    # the diff cannot un-contaminate the tree that diff came from. The verdict
    # recomputed above is issue-driven, and the dirty-baseline finding is an
    # input issue that stays in payload["issues"], so a pass is already
    # impossible here -- this only keeps the reason visible.
    ut = " UNTRUSTED=" + ";".join(payload["untrusted"]) if payload.get("untrusted") else ""
    gate_json.write_text(json.dumps(payload, indent=1))
    tier = "regate(27b,authoritative)" if authoritative else "pregate(14b)"
    print(f"[gate] {parent} verdict={payload['verdict']} "
          f"code_high={len(code_high)}{nc_s}{ut} (review merged, {tier})")
    if authoritative:
        # The Studio 27B verdict is the authority: mark it, and never escalate
        # again (this branch is only reached for a 'regate-' completion).
        payload["gate_authority"] = "studio-27b-regate"
        payload["regate"] = "done"
        gate_json.write_text(json.dumps(payload, indent=1))
    elif TWO_TIER and payload.get("verdict") != "pass" and not payload.get("regate"):
        # TWO-TIER ESCALATION: the Unraid pre-gate is a cheap trigger, not the
        # authority. A non-pass pre-gate verdict escalates an authoritative
        # re-gate to the Studio 27B ONLY when the concerns are worth adjudicating
        # (a code-category finding, a med+ severity, or an UNPROVEN review); its
        # verdict will supersede this one when it lands. A pre-gate PASS is left
        # to stand (non-terminal: shadow signoff holds for the human/Claude, and
        # Claude reads the full diff at merge).
        if _escalation_worthy:
            _escalate_regate(parent, gate_json, payload, out_dir)
        else:
            # Concerns are low-severity input/scope only -- no code finding, no
            # med+, review completed. Not worth a 27B slot: it would re-ask the
            # same "was this edited file needed?" question the coordinator
            # answers at the merge diff-glance. Verdict stands as terminal
            # 'concerns'; the coordinator reviews the diff. This is the
            # over-trigger that congested the Studio lane with needless regates.
            payload["regate"] = "not-warranted (low input/scope only)"
            payload["gate_authority"] = "pregate-terminal-lowscope"
            gate_json.write_text(json.dumps(payload, indent=1))
            print(f"[gate] {parent} concerns are low input/scope only "
                  f"({payload['counts']['input']} input, 0 code, 0 med+) "
                  f"-> NO re-gate (coordinator reviews the diff)")
    # RE-DECIDE THE SIGN-OFF NOW THAT THE REVIEW HAS LANDED. At emit time the
    # review was still queued, so the record said pass-pending-review and
    # review_verdict='not-run' -- two of auto_decide's conditions -- and the
    # harness held for a human on EVERY job. Without this re-run autonomous
    # sign-off could never fire in production, whatever the evidence said. In
    # shadow mode this only RECORDS a shadow_decision (verdict stays None), so a
    # pre-gate that escalated still holds for its authoritative 27B re-gate.
    # QUIET-FAIL CLOSE (finding #3). The merge is where the model review's code
    # findings actually arrive, so this -- not emit() -- is where a
    # pass-pending-review flips to 'fail' with code_high>=1. emit()'s notifier
    # never saw that verdict (the review was still queued then), so fire it here
    # on any non-pass. Advisory and best-effort (rule 2): never affects the exit
    # code. Addressed to the PARENT dispatch (its launched_by is on the parent
    # row/sidecar, not the gate job's). An authoritative regate re-notifying a
    # standing pre-gate non-pass is intended -- it is the confirmed verdict.
    try:
        if str(payload.get("verdict")) not in ("pass", "skipped", "pass-pending-review") \
                and not TEST_MODE:
            _notify_non_pass(parent, payload, gate_json)
    except Exception:
        pass
    resignoff(parent, gate_json, payload, out_dir / f"{parent}.diff")
    # AUTO-FIX decision, ONLY at a TERMINAL verdict. A pre-gate non-pass that just
    # ESCALATED (regate=="pending") is NOT terminal -- the authoritative 27B
    # verdict is still coming, and deciding now would classify off the cheap
    # pre-gate and could double-fire when the regate lands. So gate on "not still
    # waiting on a regate": an authoritative regate completion (regate=="done"),
    # a pre-gate PASS that stands (no regate), or a pre-gate concerns marked
    # terminal (regate "not-warranted..."/"enqueue-failed"/"skipped...").
    if payload.get("regate") != "pending":
        autofix_consider(parent, payload, gate_json, out_dir)
    return 0


def _escalate_regate(parent: str, gate_json: Path, payload: dict,
                     out_dir: Path) -> None:
    """Enqueue the authoritative Studio-27B re-gate for a non-pass pre-gate
    verdict. Distinct 'regate-' label so it never re-fires the 'gate-' loop
    guard. Reuses the pre-gate's diff + intent. Advisory; never raises."""
    review_task = out_dir / f"{parent}-review" / "task.json"
    diff_path = out_dir / f"{parent}.diff"
    regate_dir = out_dir / f"{parent}-regate"
    regate_dir.mkdir(parents=True, exist_ok=True)
    try:
        base = json.loads(review_task.read_text()) if review_task.exists() else {}
    except Exception:
        base = {}
    # DEFECT-2 FIX: the re-gate reviews the diff, but nothing told it WHICH files
    # the pre-gate actually raised findings on -- so a HIGH on a helper file
    # (lib/bfmrSalePrice.ts, this session) could return PASS while the report
    # only ever discussed the declared target route. Fold the pre-gate's flagged
    # files + their findings into the review context and REQUIRE the re-gate to
    # explicitly confirm or refute each one BY FILE. The reviewer already reads
    # `context` (code-review-agent honours mode/diff/intent/context), so this
    # needs no runner change. `issues` carry {severity,file,line,what}.
    _flagged: dict[str, list[str]] = {}
    for _i in payload.get("issues", []):
        _f = _i.get("file")
        if not _f:
            continue
        _sev = str(_i.get("severity", "?")).upper()
        _ln = _i.get("line") or ""
        _what = str(_i.get("what", "")).strip()
        _loc = f"{_f}:{_ln}" if _ln else _f
        _flagged.setdefault(_f, []).append(
            f"[{_sev}] {_loc} -- {_what}" if _what else f"[{_sev}] {_loc}")
    _ctx = ("AUTHORITATIVE re-gate on the Studio 27B: the Unraid pre-gate "
            "returned non-pass. Report only defects this change introduces.")
    if _flagged:
        _lines = []
        for _f in sorted(_flagged):
            _lines.append(f"  - {_f}")
            for _msg in _flagged[_f][:5]:
                _lines.append(f"      {_msg}")
        _ctx += (
            "\n\nThe pre-gate raised findings on the file(s) below. You MUST "
            "review EACH of these files' changes specifically and, for each, "
            "state in your report whether the finding is CONFIRMED or REFUTED "
            "(with the reason). Do not return PASS while any flagged file is "
            "unaddressed -- a PASS that never mentions a flagged file is not a "
            "clearance of it.\n" + "\n".join(_lines))
    (regate_dir / "task.json").write_text(json.dumps({
        "mode": "review", "diff": base.get("diff", str(diff_path)),
        "intent": base.get("intent", ""),
        "context": _ctx},
        indent=1))
    label = f"regate-{parent}"
    payload["pregate_verdict"] = payload.get("verdict")
    payload["pregate_review_verdict"] = payload.get("review_verdict")
    enq = ["python3", str(BIN / "ollama-queue.py"), "enqueue",
           "--model", REGATE_MODEL, "--host", REGATE_HOST,
           "--num-ctx", str(REGATE_NUM_CTX),
           "--cwd", str(regate_dir), "--task-file", str(regate_dir / "task.json"),
           "--runner", str(BIN / "code-review-agent.py"), "--label", label, "--front",
           "--allow-no-verify"]  # advisory scope/diff review: no --verify by design
    if TEST_MODE:
        payload["regate"] = "skipped (GATE_TEST_MODE)"
        payload["regate_label"] = label
        gate_json.write_text(json.dumps(payload, indent=1))
        return
    try:
        e = subprocess.run(enq, capture_output=True, text=True, timeout=120)
        payload["regate"] = "pending" if e.returncode == 0 else "enqueue-failed"
        payload["regate_label"] = label
        if e.returncode != 0:
            payload["regate_error"] = (e.stderr or e.stdout or "")[-200:]
    except Exception as ex:
        payload["regate"] = "enqueue-failed"
        payload["regate_error"] = f"{type(ex).__name__}: {str(ex)[:160]}"
    gate_json.write_text(json.dumps(payload, indent=1))
    print(f"[gate] {parent} pre-gate={payload.get('pregate_verdict')} -> ESCALATED "
          f"re-gate to {REGATE_MODEL}@{REGATE_HOST} (label {label}, "
          f"status={payload['regate']})")


def resignoff(job_id: str, gate_json: Path, payload: dict, diff_path: Path) -> None:
    """Run signoff.py --auto against the FINISHED gate record and mirror the
    outcome into it. Advisory; never raises."""
    if not payload.get("signoff_required") or payload.get("signoff_verdict"):
        return
    try:
        r = subprocess.run(
            ["python3", str(SIGNOFF), "--auto", job_id, "--gate", str(gate_json),
             "--diff", str(diff_path)],
            capture_output=True, text=True, timeout=60)
        if r.stdout:
            print(r.stdout, end="")
        so = _signoff_record(job_id)
        payload["signoff_verdict"] = so.get("verdict")
        payload["signoff_reviewer"] = so.get("reviewer", "")
        payload.pop("signoff_auto_blocked_by", None)
        if so.get("auto_blocked_by"):
            payload["signoff_auto_blocked_by"] = so["auto_blocked_by"]
        if so.get("shadow_decision"):
            payload["signoff_shadow_decision"] = so["shadow_decision"]
            payload["signoff_shadow_reasons"] = (so.get("shadow_reasons") or [])[:6]
        payload["signoff_redecided_after_review"] = True
        gate_json.write_text(json.dumps(payload, indent=1))
    except Exception as e:
        payload["signoff_error"] = f"re-decide: {type(e).__name__}: {e}"[:200]
        try:
            gate_json.write_text(json.dumps(payload, indent=1))
        except Exception:
            pass


QUEUE_STATE = BIN / "ollama-queue-state.json"


def _completion_record(job_id: str) -> dict | None:
    """The queue's DURABLE per-job completion sidecar, written by ollama-queue.py
    at gate-fire time (_persist_job_completion).

    WHY IT EXISTS. QUEUE_STATE prunes finished jobs immediately
    (RETAIN_DONE_RECENT = 0): by the time this fire-and-forget hook runs, the
    live row is usually gone, so exit_code and launch_baseline are unreadable
    from it and the gate abstains on both (not_checked). This file is written
    from the still-complete job dict and is never pruned, so the readers below
    fall back to it and can certify those facts. Fail-open, exactly like the live
    readers: a torn or missing file abstains rather than throwing."""
    try:
        p = BIN / "ollama-queue-logs" / f"{job_id}.done.json"
        if p.exists():
            rec = json.loads(p.read_text())
            return rec if isinstance(rec, dict) else None
    except Exception:
        return None
    return None


def _live_job(job_id: str) -> dict | None:
    """The current queue row for a job off ollama-queue-state.json, or None.

    The single place the live state file is read and scanned -- launch_baseline,
    job_verify_exit, job_facts and _job_field all route through here (and through
    _terminal_facts) instead of each re-implementing the same load+iterate loop.
    Fail-open: a torn read (the daemon writes this file concurrently) or a
    missing file abstains rather than throwing (rule 2)."""
    try:
        jobs = json.loads(QUEUE_STATE.read_text()).get("jobs") or []
        if isinstance(jobs, dict):
            jobs = list(jobs.values())
        for j in jobs:
            if j.get("id") == job_id:
                return j if isinstance(j, dict) else None
    except Exception:
        return None
    return None


def _terminal_facts(job_id: str) -> dict | None:
    """The job's TERMINAL facts, SIDECAR-FIRST. A job-record-shaped dict or None.

    THE RACE THIS RESOLVES (finding #2). This hook fires from the daemon's reap
    loop, and with RETAIN_DONE_RECENT=0 the on-disk row is either still
    'running'/exit_code=null (the daemon has not yet lock.save()'d the status
    flip) or already pruned (post-save) -- it is essentially NEVER observably
    'done' here. The old readers found the still-'running' row, hit the
    terminal-status guard, and returned None WITHOUT ever consulting the sidecar
    (the sidecar branch was only reached when the row was ABSENT), so a clean job
    kept stamping not_checked=verify-exit,launch-baseline. The .done.json sidecar
    is written from the still-complete job dict at fire time and is never pruned,
    so it is the only record that reliably certifies a terminal outcome here.
    Prefer it; consult the live row only when no sidecar exists yet, and only if
    it is itself terminal. A non-terminal live row with no sidecar returns None so
    the caller ABSTAINS (never a false PASS)."""
    rec = _completion_record(job_id)
    if isinstance(rec, dict):
        return rec
    j = _live_job(job_id)
    if isinstance(j, dict) and j.get("status") in ("done", "failed"):
        return j
    return None


def _job_field(job_id: str, key: str):
    """One field off the job record, LIVE row first then the durable sidecar.

    _notify_non_pass reads launched_by / launched_by_session through here, and
    the queue prunes the live row the instant a job finishes (finding #5), so
    without the sidecar fallback every done job's addressee would be unreadable.
    Fail-open, like launch_baseline()."""
    j = _live_job(job_id)
    if isinstance(j, dict) and j.get(key) is not None:
        return j.get(key)
    rec = _completion_record(job_id)
    if isinstance(rec, dict):
        return rec.get(key)
    return None


def _notify_non_pass(job_id: str, payload: dict, gate_json) -> None:
    """Make a non-pass verdict IMPOSSIBLE TO MISS. Never raises.

    THE GAP THIS CLOSES. This tool wrote gate.json, merged the handoff INDEX,
    printed one line, and stopped -- ZERO outbound contact of any kind. A `fail`
    with code_high=1 was byte-for-byte as quiet as a clean pass, so a real
    high-severity finding (radarr e3ccbf07fffa: _radarr_last_imported_source_title
    read only events[0].sourceTitle instead of iterating like its sibling)
    survived a human both-ways review and was caught ONLY because someone
    happened to open the JSON by hand.

    TWO CHANNELS, because the addressee is usually unknown:

      1. LOCAL, always -- stderr plus an append-only NON-PASS-GATES.md beside the
         gate records. This is the one that matters today: every job enqueued
         before ollama-queue.py began stamping launched_by has no addressee, so a
         notifier handling only the addressed case would stay silent for exactly
         the backlog that motivated it.

      2. The LAUNCHING SESSION, when one was stamped -- addressed by FILE, not by
         speaking a protocol. The obvious implementation is to connect to the
         launched_by socket and write a message, but the cc-socks wire format is
         not documented to this tool, and a wrong guess inside a try/except fails
         SILENTLY: the gate would look wired while notifying nobody. That is the
         exact "reads as working when it isn't" failure this gate exists to
         prevent, so it does not ship on a guess. A per-launcher inbox file is
         decidable instead -- the file is either there or it is not.

    STALENESS. launched_by is pid-keyed (/tmp/cc-socks/<pid>.sock), so a session
    can end and the OS can hand its pid, and its socket path, to an unrelated
    session. The inbox is named for that pid and the entry carries
    launched_by_session, so a stale delivery is self-evident to whoever reads it
    rather than looking like their own job's finding.

    Rule 2 throughout: advisory, and never able to change the gate's exit code.
    """
    v = payload.get("verdict", "?")
    counts = payload.get("counts") or {}
    top = ""
    for i in (payload.get("issues") or []):
        if (i or {}).get("severity") == "high":
            top = " | " + str(i.get("source", "?")) + ": " + str(i.get("what", ""))[:120]
            break

    line = ("[gate] NON-PASS " + str(job_id) + " verdict=" + str(v)
            + " code_high=" + str(counts.get("code_high", 0))
            + " code=" + str(counts.get("code", 0))
            + " input=" + str(counts.get("input", 0)) + top)

    try:
        print(line, file=sys.stderr, flush=True)
    except Exception:
        pass

    stamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    row = ("- `" + str(job_id) + "` **" + str(v) + "** code_high="
           + str(counts.get("code_high", 0)) + " - " + stamp
           + " - `" + str(gate_json) + "`" + top + "\n")

    try:
        with (Path(gate_json).parent / "NON-PASS-GATES.md").open("a") as fh:
            fh.write(row)
    except Exception:
        pass

    sock = _job_field(job_id, "launched_by")
    if not sock:
        return
    sess = _job_field(job_id, "launched_by_session")
    stem = Path(str(sock).replace("uds:", "", 1)).stem
    srow = row.rstrip("\n") + (" (for session " + str(sess) + ")" if sess else "") + "\n"
    try:
        with (Path(gate_json).parent / ("NON-PASS-for-" + stem + ".md")).open("a") as fh:
            fh.write(srow)
    except Exception:
        pass


def launch_baseline(job_id: str) -> dict | None:
    """The tree state the queue stamped when it LAUNCHED this job's worker.

    Shape (set by ollama-queue.py at launch, before the model touches anything):
        job["launch_baseline"] = {"head": <sha>, "dirty": <porcelain line count>}

    WHY THE GATE CANNOT DERIVE THIS ITSELF. The gate runs post-hoc, so `git diff
    HEAD` swallows the model's work and any pre-existing edits indistinguishably.
    dashboard-newjobs-fix (2026-08-31) ran on a scratch dir still holding edits
    from an earlier PAUSED run of the same task: its verify passed AT BASELINE,
    the accept-on-verify-pass net took that exit 0 at face value, and the stored
    diff was contaminated with work this job never did. Nothing downstream could
    have caught it -- only the launcher knows what the tree looked like before.

    READ, NEVER WRITTEN, and fail-open on everything: a missing key means a job
    enqueued before the stamp landed, and the state file is written concurrently
    by the daemon, so a torn read must abstain rather than throw. An exception
    here must never break a dispatch (rule 2).
    """
    # Sidecar-first (by fire time the live row is usually pre-save 'running' or
    # already pruned -- see _terminal_facts), then the live row: launch_baseline
    # is stamped at LAUNCH, so it is valid even while the job is still running.
    rec = _terminal_facts(job_id) or _live_job(job_id)
    if isinstance(rec, dict):
        lb = rec.get("launch_baseline")
        return lb if isinstance(lb, dict) else None
    return None


def job_verify_exit(job_id: str) -> int | None:
    """The exit code of the job's OWN verify, off the queue's job record.

    STATUS-GUARDED. exit_code is stamped per attempt and a job in 'running'
    can still be carrying a stale nonzero from a previous one -- one such row
    exists in the live state file right now. Trusting it unguarded would invent
    a FAIL on a job that has not finished, and a gate that cries wolf inverts
    its own purpose. Only 'done' and 'failed' are terminal enough to read.

    Fail-open on everything else, exactly like launch_baseline().
    """
    # SIDECAR-FIRST terminal facts (finding #2): the live row is pre-save
    # 'running' or already pruned by fire time, so trusting it would abstain on
    # every clean job. _terminal_facts returns the durable sidecar when present,
    # else the live row only if it is itself terminal, else None (abstain).
    rec = _terminal_facts(job_id)
    if rec is None:
        return None
    if rec.get("status") not in ("done", "failed"):
        return None
    ec = rec.get("exit_code")
    return ec if isinstance(ec, int) else None


def job_facts(job_id: str) -> dict:
    """Durable copy of the job-record fields the handoff view needs later."""
    # Sidecar-first terminal facts, then the live row for the informational
    # case (a job still genuinely running with no sidecar yet). verify_failed_at_
    # baseline is the load-bearing condition for autonomous sign-off -- a verify
    # already green at baseline proves nothing about the diff -- and the queue
    # prunes finished rows, so only the sidecar can recover it later.
    rec = _terminal_facts(job_id) or _live_job(job_id)
    if not isinstance(rec, dict):
        return {}
    return {"job_status": rec.get("status"), "job_label": rec.get("label"),
            "job_model": rec.get("model"), "job_cwd": rec.get("cwd"),
            "job_verify": rec.get("verify"),
            "job_exit_code": rec.get("exit_code"),
            "verify_failed_at_baseline": rec.get("verify_failed_at_baseline"),
            # Honest enqueue-time preflight reading. Frozen here so the handoff
            # panel can show it after the queue prunes the live row, instead of
            # the review's ambiguous "not-run (enqueued separately)".
            "job_preflight": rec.get("preflight"),
            "scored_arm": rec.get("scored_arm")}


def apply_baseline(payload: dict, lb: dict | None, override: int) -> None:
    """Flag a dispatch that STARTED from a dirty tree. Decidable, no model.

    HIGH and category=input: the diff is not attributable to this job, and the
    verify's exit 0 may predate the model entirely. It is deliberately NOT a
    'fail' -- gate.py's standing rule is that only a CODE high fails, because an
    input problem must never condemn code that may well be correct. But it must
    also never render as a clean pass, so any pass form is demoted to concerns.
    That is a different claim from the usual input finding: not 'the task was
    imperfect' but 'this result is unattributable'.
    """
    dirty = override if override >= 0 else (lb or {}).get("dirty")
    if not isinstance(dirty, int):
        # No stamp = job predates it, or a torn read. UNKNOWN, and unknown is
        # not clean -- recorded so the abstain is visible, same rule as
        # not_checked. Never invents a flag from absence.
        payload.setdefault("not_checked", []).append(
            "launch-baseline (queue recorded no launch_baseline for this job; "
            "a dirty starting tree could not be ruled out)")
        return
    payload["launch_baseline"] = dict(lb or {"dirty": dirty})
    if dirty <= 0:
        return
    payload.setdefault("issues", []).append(
        {"severity": "high", "file": "", "line": 0,
         "what": f"the dispatch STARTED from a dirty tree ({dirty} uncommitted "
                 f"path(s) at launch); the diff is not attributable to this job "
                 f"and verify's exit 0 may predate the model's work",
         "source": "launch-baseline", "category": "input"})
    payload["untrusted"] = [f"launch baseline was dirty ({dirty} path(s))"]
    c = payload.setdefault("counts", {"code_high": 0, "code": 0, "input": 0, "total": 0})
    c["input"] = c.get("input", 0) + 1
    c["total"] = c.get("total", 0) + 1
    if str(payload.get("verdict", "")).startswith("pass"):
        payload["verdict"] = "concerns"


def get_diff(cwd: Path, out: Path, scaffold: dict | None = None,
             excluded: list | None = None) -> tuple[Path | None, str]:
    """(diff_path, how). Git-backed cwd is the normal case; anything else skips.

    scaffold: {relpath: sha256} pinned by the preflight. An UNTRACKED file whose
    bytes still match its pinned digest is the scaffold, not the model's work,
    and is left out of the diff (names appended to `excluded`). Without this,
    test_fixture.py rode into every diff as a "new file", the change class was
    never bounded_single_site, and autonomous sign-off could not fire on any
    dispatch that shipped a fixture -- which is every dispatch the preflight
    accepts. A scaffold file the MODEL edited no longer matches its digest, so
    it stays in the diff and scope-check sees it."""
    # Detect the repo with `git rev-parse`, not `(cwd/".git").exists()`. The old
    # check skipped the gate on two legitimate cases it should have reviewed:
    # (1) cwd is a SUBDIRECTORY of the repo -- `.git` is at the repo root, not in
    #     cwd (e.g. a dispatch run in <worktree>/sidecar); and
    # (2) cwd is a git WORKTREE, whose `.git` is a *file* (`gitdir: ...`) pointing
    #     at the parent's worktrees/ dir -- the whole point of worktree dispatches.
    # rev-parse walks up and resolves both. The `git diff`/`ls-files` calls below
    # already pass `-C cwd` and git handles the subdir/worktree cases itself.
    try:
        chk = subprocess.run(["git", "-C", str(cwd), "rev-parse",
                              "--is-inside-work-tree"],
                             capture_output=True, text=True, timeout=30)
    except Exception as e:
        return None, f"git rev-parse failed: {type(e).__name__}"
    if chk.returncode != 0 or chk.stdout.strip() != "true":
        return None, ("cwd is not inside a git work tree, so there is no baseline "
                      "to diff against -- nothing to review")
    try:
        r = subprocess.run(["git", "-C", str(cwd), "diff", "HEAD"],
                           capture_output=True, text=True, timeout=120)
    except Exception as e:
        return None, f"git diff failed: {type(e).__name__}"
    text = r.stdout or ""
    if not text.strip():
        # Also try staged/untracked-aware form before concluding no change.
        r2 = subprocess.run(["git", "-C", str(cwd), "diff"],
                            capture_output=True, text=True, timeout=120)
        text = r2.stdout or ""
    # UNTRACKED FILES ARE PART OF THE WORK. `git diff HEAD` cannot see them, so a
    # dispatch whose deliverable is a NEW file showed the gate an incomplete
    # diff -- and every check downstream then judged work it could not see.
    # signoff-stage1 (24f5cd44262e) delivered a new signoff.py: completeness
    # reported two of its declared literals "absent from the diff" when both
    # were implemented, and the review model reviewed two support files while
    # the actual deliverable was invisible.
    #
    # Built with `git diff --no-index`, which is READ-ONLY. The obvious
    # alternative, `git add -N`, mutates the index of a tree we do not own.
    extra, added = [], 0
    try:
        u = subprocess.run(["git", "-C", str(cwd), "ls-files", "--others",
                            "--exclude-standard"],
                           capture_output=True, text=True, timeout=60)
        for rel in (u.stdout or "").splitlines():
            rel = rel.strip()
            if not rel or added >= 50:
                continue
            # DISPATCH SCAFFOLDING IS NOT MODEL OUTPUT. In a scratch dir where
            # nothing is committed, the task file and the verify are untracked
            # too -- so including them made every such job report "TASK.md
            # edited but never named by the task" and, worse, tripped the
            # self-test finding on a verify.sh the DISPATCHER wrote. Excluded
            # only on the UNTRACKED path: a model editing a TRACKED verify.sh
            # still shows up in `git diff HEAD` and must still be flagged.
            # The verify HARNESS is the one thing a review must never review:
            # the model is told not to touch it (TASK.md ## Scope) and doesn't,
            # so it rides in as an untracked "new file" and the reviewer speaks
            # to code that is not the model's work. verify_impl.mjs leaking in is
            # exactly how a review was steered onto a scaffold file. Excluded
            # only on the UNTRACKED path, symmetric with verify.sh: a model
            # editing a TRACKED harness still shows in `git diff HEAD` and counts.
            _name = Path(rel).name.lower()
            if (_name in {"task.md", "task.json", "verify.sh", "verify_impl.mjs",
                          "verify_impl.js", "task.txt", "run.json", "refimpl.py",
                          ".preflight-state.json"}
                    # verify[-_]impl.{mjs,cjs,mts,cts,js,ts,py,sh}: the hyphen
                    # and .mts/.cts forms cover the hand-rolled TS harness
                    # (verify-impl.mts) a name-exact list let ride into the diff
                    # as a "new file", which is how scope-check flagged "the
                    # diff edits the VERIFY" on a job that never touched it.
                    or re.match(r"^verify[-_]?(impl)?\.(mjs|cjs|mts|cts|js|ts|py|sh)$", _name)
                    # verify.test.{ext}: the node-test scaffold's starter test
                    # file (ollama-dispatch-scaffold --ts-runner node-test). It is
                    # authored harness, not model output; same untracked-only leak
                    # as verify_impl above if the operator did not --seal-baseline.
                    or re.match(r"^verify\.test\.(mjs|cjs|mts|cts|js|ts)$", _name)):
                if excluded is not None:
                    excluded.append(rel)
                continue
            f = cwd / rel
            if scaffold and rel in scaffold and _sha256(f) == scaffold[rel]:
                if excluded is not None:
                    excluded.append(rel)
                continue
            try:
                if not f.is_file() or f.stat().st_size > 512_000:
                    continue
                # Skip binaries by looking for a NUL byte, NOT by trying to
                # decode: a NUL is valid UTF-8, so read_text() succeeds on a
                # binary file and the "decode error means binary" test never
                # fires. Caught by asserting the property on a real binary
                # rather than trusting the idiom.
                if b"\x00" in f.read_bytes()[:8192]:
                    continue
            except Exception:
                continue
            d = subprocess.run(["git", "-C", str(cwd), "diff", "--no-index",
                                "--", "/dev/null", rel],
                               capture_output=True, text=True, timeout=60)
            if d.stdout:
                extra.append(d.stdout); added += 1
    except Exception:
        pass
    if extra:
        text = (text + "\n" + "\n".join(extra)) if text.strip() else "\n".join(extra)

    if not text.strip():
        return None, "no changes to review (git diff is empty)"
    out.write_text(text)
    return out, ("git diff HEAD" + (f" + {added} untracked file(s)" if added else ""))


RELEVANCE_MAX_MUTANTS = int(os.environ.get("GATE_RELEVANCE_MAX_MUTANTS") or 40)
RELEVANCE_BUDGET_S = int(os.environ.get("GATE_RELEVANCE_BUDGET_S") or 300)
_SURVIVOR_KEYS = ("file", "line", "class", "mutation", "snippet")


def measure_relevance(payload: dict, cwd: Path, verify_cmd: str,
                      task_file: str | None = None) -> None:
    """verify-relevance.py --applied on the job's finished tree -> payload["verify_relevance"].

    Three-valued and always present when it can be attempted: relevant / low /
    unproven, each with the reason and the named survivors. When it cannot be
    attempted the abstain goes to not_checked so the one-line print shows it.
    The tree is mutated IN PLACE one file at a time and restored after every
    mutant (verify-relevance's contract, asserted by its own test G); the job
    is finished and nothing else runs in its worktree, so this is safe.
    Advisory like everything here: never raises, never changes the exit code.
    """
    nc = payload.setdefault("not_checked", [])
    if not verify_cmd:
        nc.append("verify-relevance (the job declares no --verify, so there is "
                  "nothing to mutate against)")
        return
    if not RELEVANCE.is_file():
        nc.append(f"verify-relevance ({RELEVANCE} is missing)")
        return
    cmd = ["python3", str(RELEVANCE), str(cwd), "--applied",
           "--verify", verify_cmd, "--json",
           "--max-mutants", str(RELEVANCE_MAX_MUTANTS),
           "--budget-s", str(RELEVANCE_BUDGET_S)]
    if task_file:
        cmd += ["--task-file", str(Path(task_file).expanduser().resolve())]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True,
                           timeout=RELEVANCE_BUDGET_S + 900)
        rec = json.loads(r.stdout) if r.stdout.strip().startswith("{") else None
    except Exception as e:
        payload["verify_relevance"] = {
            "verdict": "unproven", "score": None, "source": "gate-applied",
            "reason": f"verify-relevance did not run: {type(e).__name__}: {str(e)[:160]}"}
        return
    if rec is None:
        payload["verify_relevance"] = {
            "verdict": "unproven", "score": None, "source": "gate-applied",
            "reason": ("verify-relevance exited " + str(r.returncode) + ": "
                       + (r.stderr or r.stdout or "").strip()[-200:])}
        return
    # Keep the record compact: the full mutant table stays on stdout of the
    # tool; the gate record carries the decision and what a human needs to
    # argue with it (survivors, unexercised sites, the two scores).
    block = {k: rec.get(k) for k in (
        "verdict", "score", "threshold", "mutant_score", "site_score",
        "site_coverage", "killed", "survived", "evidence_mutants", "generated",
        "literal_breaking", "crash_kills", "truncated", "untried_sites",
        "unexercised_sites", "reason", "seconds")}
    block["survivors"] = [{k: s.get(k) for k in _SURVIVOR_KEYS}
                          for s in (rec.get("survivors") or [])[:8]]
    block["source"] = "gate-applied"
    block["verify"] = verify_cmd
    payload["verify_relevance"] = block


def autofix_classify(payload: dict) -> dict:
    """PURE. Map a TERMINAL gate verdict to an auto-fix action. Never enqueues,
    never mutates a tree, never reads the queue. Returns
    {class, action, anchor, reasons}; action in {requeue, escalate, none}.

    FAIL-CLOSED: any verdict/finding shape not positively recognised as a
    model-actionable, DECIDABLE failure -> escalate. The requeue set is
    deliberately narrow -- spending GPU on a re-run is only justified when the
    failure is (a) real (a machine check said so, not just a reviewer opinion)
    and (b) something the model editing its code can actually change.

    The three classes from the design:
      (b) verify-red  : the job's OWN sealed verify exited nonzero. Machine truth,
                        the strongest and most decidable signal -> REQUEUE.
      (a) scope       : a concerns verdict whose only findings are scope ones
                        (edited a file the task never named) -> REQUEUE with a
                        tighter scope reminder. Decidable and model-actionable.
      (c) undecidable : dirty/unattributable baseline, invariant-guard
                        removed+re-added-literal FP, task/verify-quality
                        problems, a reviewer-only code-high with no red verify
                        (the false-FAIL class), verdict=error/unknown -> ESCALATE.
    """
    v = str(payload.get("verdict", ""))
    if v in ("pass", "pass-pending-review", "skipped"):
        return {"class": "clean", "action": "none", "anchor": None,
                "reasons": [f"verdict={v}: nothing to fix"]}
    if v in ("error",):
        return {"class": "gate-error", "action": "escalate", "anchor": None,
                "reasons": [f"gate verdict={v}: cannot classify, human needed"]}
    issues = payload.get("issues") or []
    # UNATTRIBUTABLE result: the tree was dirty at launch, so the diff is not this
    # job's and a re-run in the SAME dirty tree reproduces the contamination. Never
    # requeue -- this needs a human to clean/re-seal the tree. (Also: a requeue
    # here is a prime infinite-loop seed, because the dirty state persists.)
    if payload.get("untrusted"):
        return {"class": "unattributable", "action": "escalate", "anchor": None,
                "reasons": ["untrusted baseline: " + "; ".join(payload["untrusted"])[:200]]}
    code = [i for i in issues if i.get("category") == "code"]
    code_high = [i for i in code if i.get("severity") == "high"]
    inputs = [i for i in issues if i.get("category") == "input"]
    verify_red = [i for i in code_high if i.get("source") == "verify-exit"]
    review_high = [i for i in code_high if i.get("source") == "review"]
    other_high = [i for i in code_high
                  if i.get("source") not in ("verify-exit", "review")]
    scope = [i for i in inputs if i.get("source") == "scope"]
    # (b) THE SEALED VERIFY IS RED. Decidable machine truth; the model can act on
    # the named failing check. Highest-confidence requeue.
    if verify_red:
        return {"class": "verify-red", "action": "requeue", "anchor": "verify-exit",
                "reasons": [str(verify_red[0].get("what", ""))[:200]]}
    # (c) reviewer-only code-high, no red verify: the FALSE-FAIL risk class. Only
    # requeue if explicitly trusted; default escalate.
    if review_high and not other_high:
        if AUTOFIX_TRUST_REVIEWER:
            return {"class": "reviewer-code-high", "action": "requeue", "anchor": "review",
                    "reasons": [str(review_high[0].get("what", ""))[:200]]}
        return {"class": "reviewer-code-high", "action": "escalate", "anchor": None,
                "reasons": ["reviewer flagged a code high with no red verify "
                            "(false-FAIL class); GATE_AUTOFIX_TRUST_REVIEWER off"]}
    # A code-high from some OTHER decidable source (tilde-path, tw-class-check):
    # decidable and model-actionable -> requeue.
    if other_high:
        return {"class": "code-high", "action": "requeue", "anchor": "code",
                "reasons": [f"{other_high[0].get('source')}: "
                            + str(other_high[0].get("what", ""))[:180]]}
    # (a) SCOPE-ONLY concerns: the only findings are scope (out-of-scope edits).
    # Decidable, and a tighter "do not touch X" reminder is something the model
    # acts on. Requires NO invariant-guard / launch-baseline finding riding along
    # (those are undecidable and would make a requeue pointless).
    if v == "concerns" and scope and not code_high and not [
            i for i in inputs if i.get("source") in ("invariant-guard", "launch-baseline")]:
        return {"class": "scope", "action": "requeue", "anchor": "scope",
                "reasons": [str(i.get("what", ""))[:160] for i in scope[:3]]}
    # Everything else (invariant-guard removed+re-added literal, verify-quality,
    # completeness, low input/scope, unknown) -> a TASK/HARNESS or undecidable
    # problem a code re-run cannot fix. Escalate.
    srcs = sorted({str(i.get("source", "?")) for i in issues}) or ["(none)"]
    return {"class": "undecidable", "action": "escalate", "anchor": None,
            "reasons": [f"verdict={v}; findings from {','.join(srcs)} are not a "
                        f"decidable, model-actionable failure"]}


def autofix_round(job_id: str, payload: dict) -> int:
    """The auto-fix retry round of the job that just completed. DECIDABLE from two
    independent durable sources, taking the MAX so a torn read of one cannot RESET
    the counter (the infinite-loop seed): (1) the job field auto_fix_round, read
    live-then-sidecar via _job_field; (2) an `[auto-fix rN]` marker baked into the
    label. A genuine original dispatch has neither -> 0, which is correct. A
    requeue always carries both, so a torn field read still recovers N from the
    label. Never negative."""
    n = 0
    f = _job_field(job_id, "auto_fix_round")
    if isinstance(f, int) and f > n:
        n = f
    lbl = str(payload.get("job_label") or _job_field(job_id, "label") or "")
    m = re.search(r"\[auto-fix r(\d+)\]", lbl)
    if m:
        try:
            n = max(n, int(m.group(1)))
        except ValueError:
            pass
    return max(0, n)


def autofix_root(job_id: str, payload: dict) -> str:
    r = _job_field(job_id, "auto_fix_root")
    return str(r) if r else str(job_id)


def autofix_build_requeue(job_id: str, payload: dict, out_dir: Path,
                          next_round: int, root: str, dec: dict) -> tuple[list, Path]:
    """Build (enqueue_argv, feedback_file) for an auto-fix requeue. Writes the
    guidance file; does NOT enqueue. The command is also the exact shadow preview.

    CONSTRAINT #1 (the sealed contract is inviolable): the requeue re-uses the
    SAME --cwd (the original worktree) and the SAME --verify (the sealed
    verify.sh). The gate concerns are injected as a SEPARATE guidance file that
    becomes the requeue's --task-file, built as `<original TASK.md text> + a
    '## Gate feedback from previous attempt' section`, written OUTSIDE the sealed
    worktree (in ollama-queue-logs). verify.sh, the fixtures and the worktree
    TASK.md are left BYTE-IDENTICAL -- the model literally cannot reach its own
    verify through this path, and the scope audit / verify-relevance still bite
    if it edits the tracked verify anyway."""
    facts = job_facts(job_id)
    cwd = facts.get("job_cwd") or payload.get("job_cwd") or ""
    verify = facts.get("job_verify") or payload.get("job_verify") or ""
    model = _job_field(job_id, "model") or facts.get("job_model") or ""
    host = _job_field(job_id, "host_pref") or ""
    num_ctx = _job_field(job_id, "num_ctx")
    max_iters = _job_field(job_id, "max_iters")
    task_kind = _job_field(job_id, "task_kind")
    orig_label = str(facts.get("job_label") or _job_field(job_id, "label") or job_id)
    # strip any prior [auto-fix rN] so the label doesn't accrete markers
    base_label = re.sub(r"\s*\[auto-fix r\d+\]$", "", orig_label)
    orig_task = _job_field(job_id, "task_file") or payload.get("job_cwd")

    # Build the feedback-augmented task file OUTSIDE the sealed worktree.
    fb_dir = out_dir / "auto-fix"
    fb_dir.mkdir(parents=True, exist_ok=True)
    fb_file = fb_dir / f"{job_id}-r{next_round}.task.md"
    orig_text = ""
    try:
        if orig_task and Path(orig_task).is_file():
            orig_text = Path(orig_task).read_text()
    except Exception:
        orig_text = ""
    concerns = "\n".join(f"- {r}" for r in (dec.get("reasons") or []))
    top = ""
    for i in (payload.get("issues") or []):
        if i.get("severity") == "high":
            loc = f"{i.get('file','')}:{i.get('line','')}".strip(":")
            top += f"- [{str(i.get('severity')).upper()}] {loc} ({i.get('source')}): {str(i.get('what',''))[:200]}\n"
    fb_section = (
        "\n\n## Gate feedback from previous attempt (auto-fix round "
        f"{next_round})\n\n"
        "Your previous attempt did NOT clear the automated gate "
        f"(class: {dec.get('class')}). Fix the following and re-run "
        "`bash verify.sh` until it prints VERIFY_OK. Do NOT edit verify.sh, the "
        "task, or any fixture -- only the target source file(s).\n\n"
        f"{concerns}\n"
        + (("\nHigh-severity findings:\n" + top) if top else ""))
    try:
        fb_file.write_text((orig_text or "") + fb_section)
    except Exception:
        pass

    argv = ["python3", str(BIN / "ollama-queue.py"), "enqueue",
            "--model", str(model), "--cwd", str(cwd),
            "--task-file", str(fb_file),
            "--label", f"{base_label} [auto-fix r{next_round}]",
            "--auto-fix-round", str(next_round), "--auto-fix-root", str(root)]
    if host:
        argv += ["--host", str(host)]
    if verify:
        argv += ["--verify", str(verify)]
    if isinstance(num_ctx, int):
        argv += ["--num-ctx", str(num_ctx)]
    if isinstance(max_iters, int):
        argv += ["--max-iters", str(max_iters)]
    if task_kind:
        argv += ["--task-kind", str(task_kind)]
    return argv, fb_file


def _autofix_log(out_dir: Path, job_id: str, payload: dict) -> None:
    """Append the auto-fix decision to an at-a-glance worklist. Never raises."""
    try:
        stamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        act = payload.get("auto_fix_action")
        row = (f"- `{job_id}` **{act}** ({payload.get('auto_fix_class')}) "
               f"round={payload.get('auto_fix_round')} mode={payload.get('auto_fix_mode')} "
               f"verdict={payload.get('verdict')} - {stamp}\n")
        for r in (payload.get("auto_fix_reasons") or [])[:4]:
            row += f"    - {r}\n"
        if payload.get("auto_fix_cmd_preview"):
            row += "    - requeue: `" + " ".join(payload["auto_fix_cmd_preview"]) + "`\n"
        with (out_dir / "AUTO-FIX-QUEUE.md").open("a") as fh:
            fh.write(row)
    except Exception:
        pass


def autofix_consider(job_id: str, payload: dict, gate_json: Path,
                     out_dir: Path) -> None:
    """Decide + record (+ in live mode, enqueue) the auto-fix for a TERMINAL
    verdict. Advisory throughout: never raises, never changes the exit code.

    SHADOW (default): writes the decision, the round, and the exact requeue
    COMMAND into .gate.json and AUTO-FIX-QUEUE.md, and enqueues nothing.
    LIVE: additionally runs the enqueue -- ONCE (guarded by auto_fix_enqueued).
    """
    try:
        dec = autofix_classify(payload)
        rnd = autofix_round(job_id, payload)
        root = autofix_root(job_id, payload)
        payload["auto_fix_class"] = dec["class"]
        payload["auto_fix_action"] = dec["action"]
        payload["auto_fix_reasons"] = (dec.get("reasons") or [])[:6]
        payload["auto_fix_round"] = rnd
        payload["auto_fix_root"] = root
        payload["auto_fix_mode"] = AUTOFIX_MODE
        next_round = rnd + 1
        # LOOP GUARD: never requeue past the cap. At/above the cap -> escalate.
        if dec["action"] == "requeue" and next_round > AUTOFIX_MAX_ROUNDS:
            payload["auto_fix_action"] = "escalate"
            payload["auto_fix_reasons"] = (
                [f"auto-fix cap reached (would be round {next_round} > "
                 f"{AUTOFIX_MAX_ROUNDS}); escalating to human"] + payload["auto_fix_reasons"])
            dec["action"] = "escalate"
        argv = None
        if dec["action"] == "requeue":
            argv, _fb = autofix_build_requeue(job_id, payload, out_dir,
                                              next_round, root, dec)
            payload["auto_fix_next_round"] = next_round
            payload["auto_fix_cmd_preview"] = argv
        gate_json.write_text(json.dumps(payload, indent=1))
        _autofix_log(out_dir, job_id, payload)
        print(f"[gate] {job_id} auto-fix={payload['auto_fix_action']} "
              f"class={payload['auto_fix_class']} round={rnd} mode={AUTOFIX_MODE}")
        # LIVE enqueue, once. Off by default; only reached when explicitly enabled.
        if (AUTOFIX_MODE == "live" and dec["action"] == "requeue"
                and argv is not None and not TEST_MODE
                and not payload.get("auto_fix_enqueued")):
            try:
                e = subprocess.run(argv, capture_output=True, text=True, timeout=120)
                payload["auto_fix_enqueued"] = e.returncode == 0
                payload["auto_fix_enqueue_status"] = (
                    "requeued" if e.returncode == 0 else "enqueue-failed")
                if e.returncode != 0:
                    payload["auto_fix_enqueue_error"] = (e.stderr or e.stdout or "")[-200:]
            except Exception as ex:
                payload["auto_fix_enqueue_status"] = "enqueue-failed"
                payload["auto_fix_enqueue_error"] = f"{type(ex).__name__}: {str(ex)[:160]}"
            gate_json.write_text(json.dumps(payload, indent=1))
    except Exception as e:
        try:
            payload["auto_fix_error"] = f"{type(e).__name__}: {str(e)[:180]}"
            gate_json.write_text(json.dumps(payload, indent=1))
        except Exception:
            pass


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--job-id", required=True)
    ap.add_argument("--cwd", required=True)
    ap.add_argument("--task-file", default=None)
    ap.add_argument("--verify", default="")
    ap.add_argument("--model", default="qwen3.8:27b-q4_K_M")  # coding default flipped 2026-09-02: bake-off 6/6 vs qwen3-coder 1/6
    ap.add_argument("--host", default="http://127.0.0.1:11434")
    ap.add_argument("--num-ctx", type=int, default=32768)
    ap.add_argument("--out-dir", default=None)
    ap.add_argument("--verify-exit", type=int, default=None,
                    help="override the job's verify exit code (testing). "
                         "Default: read it from the queue state.")
    ap.add_argument("--launch-dirty", type=int, default=-1,
                    help="override the queue's launch_baseline.dirty count "
                         "(testing, or a caller that already knows it). -1 = "
                         "read it from the queue state.")
    ap.add_argument("--job-label", default="",
                    help="the completed job's label; a 'gate-' prefix means this "
                         "invocation merges a review result instead of gating")
    a = ap.parse_args()

    # LOOP GUARD: a gate's own review job must never be gated. When the queue
    # completes a 'gate-<id>' job this hook fires again -- that invocation MERGES
    # the review into the parent's .gate.json instead of starting a new gate.
    # 'regate-' is checked FIRST and is disjoint from 'gate-' (neither prefixes
    # the other). A regate completion folds the authoritative Studio-27B verdict
    # and is terminal -- it never escalates again.
    if a.job_label.startswith("regate-"):
        return merge_review(a, out_dir_for(a), prefix="regate-", authoritative=True)
    if a.job_label.startswith("gate-"):
        return merge_review(a, out_dir_for(a))

    cwd = Path(a.cwd).expanduser()
    out_dir = Path(a.out_dir).expanduser() if a.out_dir else (BIN / "ollama-queue-logs")
    out_dir.mkdir(parents=True, exist_ok=True)
    gate_json = out_dir / f"{a.job_id}.gate.json"
    diff_path = None        # set once get_diff() succeeds; emit() checks for None

    def emit(payload: dict) -> int:
        payload["job_id"] = a.job_id
        payload["ts"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        payload["advisory"] = True
        # Snapshot the job's own outcome INTO the gate record. The queue prunes
        # finished jobs from its state file (3 live rows against 21 completed
        # dispatches on disk), so anything reading job status later -- the
        # handoff surface especially -- finds nothing. The gate record is never
        # pruned, so it is the right place to freeze this.
        for k, v in (job_facts(a.job_id) or {}).items():
            payload.setdefault(k, v)
        # The queue prunes finished rows; if the job is already gone, the cwd
        # this hook was CALLED with is the same value, and signoff.py locates
        # the preflight ledger by it.
        payload.setdefault("job_cwd", str(cwd))
        payload.setdefault("job_verify", a.verify or None)

        # Evaluate sign-off requirements and add them to the gate record.
        # diff_path is None when the gate skipped before a diff existed (no task
        # file, not a git tree); say so instead of raising NameError into the
        # advisory catch below and recording a misleading error string.
        try:
            if diff_path is None:
                payload["signoff_error"] = ("not evaluated: the gate skipped before a "
                                            "diff existed (" + str(payload.get("reason", ""))[:120] + ")")
            else:
                signoff_cmd = ["python3", str(SIGNOFF),
                               "--evaluate", a.job_id, "--diff", str(diff_path)]
                if a.task_file:
                    signoff_cmd.extend(["--task-file", a.task_file])
                r = subprocess.run(signoff_cmd, capture_output=True, text=True, timeout=60)
                if r.returncode == 0:
                    try:
                        so = _signoff_record(a.job_id)
                        if so:
                            payload["signoff_required"] = so.get("required", False)
                            payload["signoff_reviewer"] = so.get("reviewer", "")
                            payload["signoff_verdict"] = so.get("verdict", None)
                        else:
                            payload["signoff_error"] = ("evaluate exited 0 but wrote no "
                                                        "record for this job")
                    except Exception as e:
                        payload["signoff_error"] = f"load: {type(e).__name__}: {e}"[:200]
                else:
                    payload["signoff_error"] = (
                        f"evaluate exited {r.returncode}: "
                        f"{((r.stderr or r.stdout or '').strip()[-160:])}")
        except Exception as e:
            # Sign-off evaluation must never FAIL the gate -- it is advisory. But
            # swallowing the reason made it undiagnosable: job 08c288f0d675 landed
            # with no signoff_* keys at all and nothing anywhere said why, so
            # "sign-off was not required" and "the sign-off step broke" looked
            # identical from the record. Record it, still don't raise.
            payload["signoff_error"] = f"{type(e).__name__}: {e}"[:200]
        # Always state the outcome, even when no sign-off is needed. An ABSENT
        # key cannot distinguish "not required" from "never ran".
        payload.setdefault("signoff_required", None)
        payload.setdefault("signoff_verdict", None)

        # Autonomous sign-off. Written FIRST so signoff.py reads the finished
        # record: every condition it checks (verdict, counts, verify_exit,
        # verify_failed_at_baseline, untrusted, verify_relevance ...) lives in
        # this payload, and passing a half-built one would decide on absent
        # evidence -- which, since every missing key reads as "condition not
        # met", would fail closed but for the wrong reason and look like a
        # policy result.
        gate_json.write_text(json.dumps(payload, indent=1))
        if payload.get("signoff_required") and not payload.get("signoff_verdict"):
            try:
                r2 = subprocess.run(
                    ["python3", str(SIGNOFF),
                     "--auto", a.job_id, "--gate", str(gate_json),
                     "--diff", str(diff_path)],
                    capture_output=True, text=True, timeout=60)
                if r2.stdout:
                    print(r2.stdout, end="")
                if r2.returncode != 0:
                    payload["signoff_error"] = (
                        f"auto exited {r2.returncode}: "
                        f"{((r2.stderr or r2.stdout or '').strip()[-160:])}")
                so = _signoff_record(a.job_id)
                payload["signoff_verdict"] = so.get("verdict")
                payload["signoff_reviewer"] = so.get("reviewer", "")
                if so.get("auto_blocked_by"):
                    payload["signoff_auto_blocked_by"] = so["auto_blocked_by"]
                # SHADOW mode leaves verdict None by design; surface the
                # harness's would-be decision so the record shows sign-off RAN.
                if so.get("shadow_decision"):
                    payload["signoff_shadow_decision"] = so["shadow_decision"]
                    payload["signoff_shadow_reasons"] = (so.get("shadow_reasons") or [])[:6]
            except Exception as e:
                payload["signoff_error"] = f"auto: {type(e).__name__}: {e}"[:200]

        gate_json.write_text(json.dumps(payload, indent=1))
        v = payload.get("verdict", "?")
        n = (payload.get("counts") or {}).get("code_high", 0)
        # The one-line print is what a human actually reads. A check that
        # ABSTAINED has to appear here or the abstain is invisible in practice,
        # no matter how carefully the JSON records it.
        nc = payload.get("not_checked") or []
        nc_s = (" not_checked=" + ",".join(x.split(" (")[0] for x in nc)) if nc else ""
        ut = " UNTRUSTED=" + ";".join(payload["untrusted"]) if payload.get("untrusted") else ""
        print(f"[gate] {a.job_id} verdict={v} code_high={n}{nc_s}{ut} -> {gate_json}")
        # POSITIVE CONTACT on a non-pass verdict. Advisory and
        # best-effort: wrapped whole, never affects the exit code.
        try:
            # 'pass-pending-review' is a CLEAN pass whose model review has not
            # landed yet -- it is not-yet-non-pass, not a finding (finding #4).
            # Notifying here fired for every clean job awaiting review and
            # polluted NON-PASS-GATES.md; the real non-pass surfaces later, from
            # merge_review, if the review flips it to fail (code_high>=1).
            if str(v) not in ("pass", "skipped", "pass-pending-review") and not TEST_MODE:
                _notify_non_pass(a.job_id, payload, gate_json)
        except Exception:
            pass
        # Penn's at-a-glance surface. Best-effort and last: it is a VIEW, so a
        # failure here loses nothing (rebuild with --all), and it must never be
        # able to affect the gate's own exit code -- rule 2 again.
        try:
            if not TEST_MODE:
                subprocess.run(["python3", str(BIN / "handoff-emit.py"),
                                "--job-id", a.job_id], capture_output=True, timeout=120)
        except Exception:
            pass
        # AUTO-FIX at emit ONLY when this verdict is already terminal because NO
        # review will land (review enqueue-failed, or a skip that produced no
        # review). The normal path enqueues a review (review=="pending") and the
        # decision fires from merge_review instead -- firing here too would
        # double-classify off the pre-review snapshot. A verify-red job whose
        # review enqueue FAILED would otherwise never get an auto-fix decision.
        try:
            if payload.get("review") != "pending":
                autofix_consider(a.job_id, payload, gate_json, out_dir)
        except Exception:
            pass
        return 0        # ALWAYS 0 -- see rule 2

    if not a.task_file or not Path(a.task_file).expanduser().exists():
        return emit({"verdict": "skipped",
                     "reason": "no task file, so completeness and scope cannot be judged"})

    scaffold_excluded: list = []
    diff_path, how = get_diff(cwd, out_dir / f"{a.job_id}.diff",
                              scaffold_digests(cwd), scaffold_excluded)
    if diff_path is None:
        return emit({"verdict": "skipped", "reason": how})

    # --- the DECIDABLE checks: pure Python, no GPU, run inline ---------------
    cmd = ["python3", str(GATE), "--task-file", str(Path(a.task_file).expanduser()),
           "--diff", str(diff_path), "--cwd", str(cwd), "--json", "--no-review"]
    if a.verify:
        cmd += ["--verify", a.verify]
    # DID THE JOB'S OWN VERIFY PASS? gate.py has taken --verify-exit since it was
    # written and nothing ever passed it, so the verdict never saw the single
    # most decisive fact about the job. Read it off the record, same as
    # launch_baseline, so the queue's integration stays one call.
    vexit = a.verify_exit if a.verify_exit is not None else job_verify_exit(a.job_id)
    if vexit is not None:
        cmd += ["--verify-exit", str(vexit)]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=900)
        payload = json.loads(r.stdout)
        payload["diff_source"] = how
        if scaffold_excluded:
            payload["scaffold_excluded"] = scaffold_excluded
    except Exception as e:
        return emit({"verdict": "error", "reason": f"{type(e).__name__}: {str(e)[:200]}"})

    # WAS THE STARTING TREE CLEAN? Read from the queue's stamp so the queue's
    # integration stays ONE CALL -- ollama-queue.py adds the stamp and changes
    # nothing about how it invokes this hook.
    if vexit is None:
        payload.setdefault("not_checked", []).append(
            "verify-exit (the queue record carries no terminal exit_code for this "
            "job, so whether its own verify passed is unknown)")
    apply_baseline(payload, launch_baseline(a.job_id), a.launch_dirty)

    # --- verify RELEVANCE on the model's ACTUAL diff ---------------------------
    # The preflight measured relevance against the REFERENCE impl and wrote it to
    # the ledger. This measures it against the code being signed off: mutate the
    # lines the model added, in place, and ask whether the job's own --verify
    # kills them. A green verify that lets the model's added lines be flipped,
    # negated or deleted without going red certified nothing. Runs only when
    # there is a verify to run; abstains VISIBLY otherwise (not_checked), because
    # an absent block reads as "never measured" to signoff.py and holds.
    measure_relevance(payload, cwd, a.verify, a.task_file)

    # --- the MODEL review: ENQUEUED, never called directly --------------------
    review_dir = out_dir / f"{a.job_id}-review"
    review_dir.mkdir(parents=True, exist_ok=True)
    (review_dir / "task.json").write_text(json.dumps({
        "mode": "review", "diff": str(diff_path),
        "intent": Path(a.task_file).expanduser().read_text()[:1500],
        "context": "Automated pre-gate on a completed dispatch. Report only "
                   "defects this change introduces."}, indent=1))
    label = f"gate-{a.job_id}"          # the 'gate-' prefix is the LOOP GUARD
    # TWO-TIER: the cheap PRE-gate runs on Unraid (qwen3:14b @ 6144), not the
    # inbound job's --model/--host. A non-pass result escalates to the Studio
    # 27B re-gate (see _escalate_regate); a pass stands, non-terminal.
    payload["review_tier"] = "pregate"
    payload["review_model"] = PREGATE_MODEL
    payload["review_host"] = PREGATE_HOST
    enq = ["python3", str(BIN / "ollama-queue.py"), "enqueue",
           "--model", PREGATE_MODEL, "--host", PREGATE_HOST,
           "--num-ctx", str(PREGATE_NUM_CTX),
           "--cwd", str(review_dir), "--task-file", str(review_dir / "task.json"),
           "--runner", str(BIN / "code-review-agent.py"), "--label", label, "--front",
           "--allow-no-verify"]  # advisory scope/diff review: no --verify by design
    if TEST_MODE:
        # A canary must never enqueue onto the live queue. Visible, not silent.
        payload["review"] = "skipped (GATE_TEST_MODE)"
        payload["review_label"] = label
        payload["review_dir"] = str(review_dir)
        return emit(payload)
    try:
        e = subprocess.run(enq, capture_output=True, text=True, timeout=120)
        ok = e.returncode == 0
        payload["review"] = "pending" if ok else "enqueue-failed"
        payload["review_label"] = label
        payload["review_dir"] = str(review_dir)
        if not ok:
            payload["review_error"] = (e.stderr or e.stdout or "")[-200:]
    except Exception as ex:
        payload["review"] = "enqueue-failed"
        payload["review_error"] = f"{type(ex).__name__}: {str(ex)[:160]}"

    # A pending review must NOT read as a clean pass.
    if payload.get("review") == "pending" and payload.get("verdict") == "pass":
        payload["verdict"] = "pass-pending-review"
    return emit(payload)


def _self_test() -> bool:
    """Unit-test the terminal-facts readers -- specifically the SIDECAR-FIRST
    fallback that certifies a job whose LIVE row is still non-terminal (the
    reap-vs-lock.save race, finding #2), while preserving the correct abstain
    when nothing terminal is available. No GPU, no network; writes only into a
    throwaway temp dir. Same PASS/FAIL convention as ollama-queue.py --self-test."""
    import tempfile as _tf, shutil as _sh
    global QUEUE_STATE, BIN
    ok = True

    def check(name, got, want):
        nonlocal ok
        if got == want:
            print(f"PASS {name}")
        else:
            ok = False
            print(f"FAIL {name}: got {got!r} want {want!r}")

    _orig_qs, _orig_bin = QUEUE_STATE, BIN
    _td = Path(_tf.mkdtemp(prefix="gate-selftest-"))
    try:
        BIN = _td
        QUEUE_STATE = _td / "ollama-queue-state.json"
        (_td / "ollama-queue-logs").mkdir(parents=True, exist_ok=True)

        def _state(jobs):
            QUEUE_STATE.write_text(json.dumps({"jobs": jobs}))

        def _sidecar(jid, rec):
            (_td / "ollama-queue-logs" / f"{jid}.done.json").write_text(json.dumps(rec))

        # (a) THE RACE: the live row still says running / exit_code null (the
        # daemon has not lock.save()'d the flip yet), but the terminal sidecar
        # says done exit 0. Sidecar-first must CERTIFY, not abstain.
        _state([{"id": "race1", "status": "running", "exit_code": None,
                 "launch_baseline": {"head": "abc", "dirty": 0}}])
        _sidecar("race1", {"id": "race1", "status": "done", "exit_code": 0,
                           "label": "dash-fix", "launch_baseline": {"head": "abc", "dirty": 0},
                           "launched_by": "uds:/tmp/cc-socks/123.sock",
                           "launched_by_session": "sess-xyz"})
        check("sidecar-first certifies verify-exit over non-terminal live row",
              job_verify_exit("race1"), 0)
        check("sidecar-first certifies launch-baseline over non-terminal live row",
              (launch_baseline("race1") or {}).get("dirty"), 0)
        check("job_facts reads terminal sidecar, not the running live row",
              job_facts("race1").get("job_status"), "done")
        check("_job_field falls back to sidecar for launched_by (finding #5)",
              _job_field("race1", "launched_by"), "uds:/tmp/cc-socks/123.sock")

        # (b) ABSTAIN PRESERVED: non-terminal live row, NO sidecar -> None, never
        # a false PASS/exit.
        _state([{"id": "run2", "status": "running", "exit_code": None}])
        check("no sidecar + running live row -> abstain (None)",
              job_verify_exit("run2"), None)

        # (c) ABSTAIN PRESERVED: row pruned (absent) + no sidecar -> None.
        _state([])
        check("pruned row + no sidecar -> abstain (None)",
              job_verify_exit("gone3"), None)

        # (d) terminal live row, no sidecar -> still certifies (normal fast path).
        _state([{"id": "done4", "status": "done", "exit_code": 0}])
        check("terminal live row certifies with no sidecar",
              job_verify_exit("done4"), 0)

        # (e) a FAILED sidecar certifies its nonzero exit (not swallowed to None).
        _state([])
        _sidecar("fail5", {"id": "fail5", "status": "failed", "exit_code": 2})
        check("failed sidecar certifies nonzero exit", job_verify_exit("fail5"), 2)

        # (f) launch_baseline from a still-RUNNING live row with no sidecar: it is
        # stamped at launch, so it stays readable (not gated on terminal status).
        _state([{"id": "run6", "status": "running", "exit_code": None,
                 "launch_baseline": {"head": "def", "dirty": 3}}])
        check("launch_baseline readable from running live row (no sidecar)",
              (launch_baseline("run6") or {}).get("dirty"), 3)

        # --- AUTO-FIX classifier (pure; no state needed) --------------------
        def _cls(p):
            return autofix_classify(p)["action"], autofix_classify(p)["class"]
        # (b) sealed verify RED -> requeue
        check("verify-red -> requeue",
              _cls({"verdict": "fail", "issues": [
                  {"category": "code", "severity": "high", "source": "verify-exit",
                   "what": "verify exit 1"}]}), ("requeue", "verify-red"))
        # (a) scope-only concerns -> requeue
        check("scope-only concerns -> requeue",
              _cls({"verdict": "concerns", "issues": [
                  {"category": "input", "severity": "low", "source": "scope",
                   "what": "edited file not named"}]}), ("requeue", "scope"))
        # (c) reviewer-only code-high, no red verify -> escalate (false-FAIL class)
        check("reviewer code-high, no verify -> escalate",
              _cls({"verdict": "fail", "issues": [
                  {"category": "code", "severity": "high", "source": "review",
                   "what": "reviewer says bug"}]}), ("escalate", "reviewer-code-high"))
        # (c) invariant-guard removed+re-added-literal FP -> escalate, never requeue
        check("invariant-guard FP -> escalate",
              _cls({"verdict": "concerns", "issues": [
                  {"category": "input", "severity": "high", "source": "invariant-guard",
                   "what": "removed X under invariant comment"}]}),
              ("escalate", "undecidable"))
        # dirty/unattributable baseline -> escalate, NEVER requeue (loop seed)
        check("untrusted baseline -> escalate",
              _cls({"verdict": "concerns", "untrusted": ["dirty (2)"], "issues": [
                  {"category": "input", "severity": "high", "source": "launch-baseline",
                   "what": "dirty tree"}]}), ("escalate", "unattributable"))
        # scope RIDING WITH an undecidable input finding -> NOT scope-requeue
        check("scope + invariant-guard -> escalate (not scope-requeue)",
              _cls({"verdict": "concerns", "issues": [
                  {"category": "input", "severity": "low", "source": "scope", "what": "x"},
                  {"category": "input", "severity": "high", "source": "invariant-guard",
                   "what": "y"}]}), ("escalate", "undecidable"))
        # clean verdicts -> none
        check("pass -> none", _cls({"verdict": "pass", "issues": []}),
              ("none", "clean"))
        check("pass-pending-review -> none",
              _cls({"verdict": "pass-pending-review", "issues": []}), ("none", "clean"))

        # --- LOOP GUARD round parser: label marker recovers a torn field read ---
        _state([])  # no live row, no sidecar -> field read is None
        check("round from label marker when field unreadable",
              autofix_round("norow", {"job_label": "myfix [auto-fix r2]"}), 2)
        check("round 0 for an original dispatch (no marker, no field)",
              autofix_round("norow", {"job_label": "myfix"}), 0)
        # field and label disagree -> take the MAX (torn read can't lower it)
        _state([{"id": "r7", "auto_fix_round": 1}])
        check("round takes MAX(field,label) so a stale low value can't reset it",
              autofix_round("r7", {"job_label": "myfix [auto-fix r3]"}), 3)
    finally:
        QUEUE_STATE, BIN = _orig_qs, _orig_bin
        _sh.rmtree(_td, ignore_errors=True)

    print("SELF_TEST_OK" if ok else "SELF_TEST_FAILED")
    return ok


if __name__ == "__main__":
    # --self-test unit-tests the pure readers (no GPU/state/network); checked
    # before argparse so it needs no --job-id.
    if "--self-test" in sys.argv[1:]:
        sys.exit(0 if _self_test() else 1)
    sys.exit(main())
