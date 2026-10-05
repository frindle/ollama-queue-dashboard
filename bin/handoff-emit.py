#!/usr/bin/env python3
"""Ollama job handoff: what's in flight, and what's finished but not acted on.

the user's model is WORKFLOW STATE, not gate verdict:

    pending/   jobs still in flight (queued or running)
    complete/  jobs the MODEL finished that WE have not acted on yet --
               reviewed and merged/integrated/deployed
    acted on   removed from complete/, permanently

The distinction that matters, and the thing the first version got wrong: a job
whose GATE PASSED is not "handled". resell-bfmr-oneclick gates clean and is
still unmerged -- finished by the model, untouched by us. So "acted on" is
ORTHOGONAL to the gate verdict, and only an explicit mark removes a job from
complete/. The gate columns stay, because they are how you decide what to act
on next; they just don't decide it for you.

ONE PIECE OF REAL STATE, DELIBERATELY. Everything else here is derived and the
folder can be deleted and rebuilt with --all. "Acted on" cannot be derived --
no upstream system records whether a human integrated a diff -- so it lives in
acted.json. That is not a second source of truth about job state; it is the
only source of a different fact.

A PLAIN `rm` STICKS. Deleting a page from complete/ is the obvious gesture and
it must not be undone by the next rebuild, so a rebuild treats "this job was in
the manifest and its file is now gone" as an acted-on mark and records it. The
manifest is what makes that safe: with no manifest (fresh folder, first run)
absence means "never written", not "deleted", so nothing is silently marked.

Usage:
  handoff-emit.py --all              # rebuild everything
  handoff-emit.py --job-id <id>      # refresh one job + the index (hook path)
  handoff-emit.py --acted <id> [...] # mark acted on -> drops out of complete/
  handoff-emit.py --unact <id> [...] # undo that
"""
import argparse, json, re, subprocess, sys, time
import os
from pathlib import Path

BIN = Path.home() / "bin"
LOGS = BIN / "ollama-queue-logs"
STATE = BIN / "ollama-queue-state.json"
# HANDOFF_DIR overrides this. The path was hardcoded, so consolidating the
# GitHub Projects folder on 2026-09-02 silently split the tool from its data:
# the next run would have RECREATED an empty dir at the old location and written
# there, while 100 acted entries sat in the real one. Nothing would have errored
# -- it would just have quietly kept two records. Same shape as every other
# "reads as fine while pointing at the wrong artifact" bug in this harness.
OUT = Path(os.environ.get(
    "HANDOFF_DIR",
    Path.home() / "Desktop" / "GitHub Projects" / "ollama" / "ollama-handoff")
).expanduser()
ACTED = OUT / "acted.json"
MANIFEST = OUT / ".manifest.json"

IN_FLIGHT = {"pending", "running", "queued", "paused"}


def _load(p: Path, default):
    try:
        return json.loads(p.read_text())
    except Exception:
        return default


def _save(p: Path, obj):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, indent=1, sort_keys=True))


def run_bash(command):
    import subprocess
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        return result
    except Exception as e:
        return type('obj', (object,), {'exit_code': 1, 'stdout': '', 'stderr': str(e)})()


def gate_for(job_id): return _load(LOGS / f"{job_id}.gate.json", {})


_VERDICT_RE = re.compile(r"^##\s*VERDICT:\s*([A-Za-z]+)", re.M)


def _report_verdict(job_id, kind):
    """First `## VERDICT: X` line from a reviewer's report.md (kind: review|regate)."""
    try:
        txt = (LOGS / f"{job_id}-{kind}" / "report.md").read_text()
    except Exception:
        return None
    m = _VERDICT_RE.search(txt)
    return m.group(1).upper() if m else None


def final_verdict(job_id, g, job=None):
    """Authoritative FINAL gate verdict (the user: "the final pass or the final fail").

    The regate (Studio 27B) is the final authority when it ran, so its report
    wins over the pre-gate. We surface the reviewer's narrative `## VERDICT:`
    (PASS/FAIL/CONCERNS) rather than gate.json's `verdict`, which only ever
    carries the pre-gate machine classification (e.g. "concerns").

    "not-run (enqueued separately)" is NOT a verdict -- it only means the review
    was queued as its own gate- job and hasn't merged yet. When a real reviewer
    verdict is absent, fall back to the job's own honest enqueue-time preflight
    reading (recorded by ollama-queue.py) rather than showing that ambiguous
    string, which reads like a failure. "not-run" survives only for a job that
    genuinely ran no preflight (no preflight field)."""
    gate_v = None
    if g:
        # ESCALATING (2026-09-11, job fd7b1d7cd7d6): a re-gate is IN FLIGHT, so the
        # verdict is NOT final. Do NOT fall through to the pre-gate reviewer's
        # report verdict here -- it can say PASS while the machine verdict is
        # 'concerns' and the authoritative Studio 27B is still running, which read
        # on the dashboard as a terminal PASS on a job that was actually escalating.
        # Show the escalating state and the pre-gate machine verdict that triggered
        # it, so the row reflects the OVERALL state, never a premature PASS.
        if g.get("regate") == "pending":
            _pg = g.get("pregate_verdict") or g.get("verdict") or "concerns"
            return f"escalating -> re-gate (pre-gate: {_pg})"
        if g.get("regate") == "done":
            gate_v = _report_verdict(job_id, "regate")
        if not gate_v:
            gate_v = _report_verdict(job_id, "review")
        if not gate_v:
            gate_v = (g.get("regate_verdict") or g.get("review_verdict")
                      or g.get("verdict"))
        # OVERALL-VERDICT FLOOR: never let a reviewer's narrative PASS mask a
        # machine verdict of fail/concerns. The machine `verdict` folds BOTH the
        # decidable findings and the reviewer's rows (merge_review recomputes it),
        # so a reviewer report saying PASS while `verdict` is fail/concerns means
        # a DECIDABLE finding (a red verify, a scope/completeness flag) the diff
        # reviewer never spoke to. Surface the non-pass so it can't read as clean.
        _mv = str(g.get("verdict") or "")
        if (str(gate_v).upper() == "PASS"
                and _mv in ("fail", "concerns")):
            gate_v = f"{_mv} (reviewer said PASS; decidable finding stands)"
    # A real reviewer verdict (PASS/FAIL/CONCERNS) always wins.
    if gate_v and not str(gate_v).startswith("not-run"):
        return gate_v
    # No real verdict yet: prefer the honest preflight reading if we have one.
    pf = (job or {}).get("preflight")
    if pf:
        return pf
    return gate_v or "-"
def diff_for(job_id):
    try:
        return (LOGS / f"{job_id}.diff").read_text()
    except Exception:
        return ""


def diff_files(diff):
    return sorted({m.group(1) for m in re.finditer(r"^\+\+\+ b/(.+)$", diff, re.M)
                   if m.group(1) != "/dev/null"})


def slug(s):
    return re.sub(r"[^a-zA-Z0-9._-]+", "-", (s or "job")).strip("-")[:60] or "job"


def load_jobs():
    """Live queue rows UNION every job with a gate record.

    Neither source alone is enough: the queue state holds only in-flight jobs
    (3 rows against 21 completed dispatches on disk), and gate records only
    exist once a job has finished. Pending work comes from the first, finished
    work from the second.
    """
    live = {}
    jobs = _load(STATE, {}).get("jobs") or []
    if isinstance(jobs, dict):
        jobs = list(jobs.values())
    for j in jobs:
        if j.get("id"):
            # _live marks "this row came from the LIVE queue state". Load-bearing for
            # is_pending: the queue PURGES completed jobs, so a job reconstructed from
            # its gate record carries whatever status was frozen at gate time -- and if
            # that was "running", nothing ever corrects it. Four bake-off arms sat in
            # "Pending -- in flight" indefinitely against an empty queue that way.
            live[j["id"]] = dict(j, _live=True)
    out = dict(live)
    for gf in sorted(LOGS.glob("*.gate.json")):
        jid = gf.name[: -len(".gate.json")]
        job = out.get(jid) or {"id": jid}
        g = gate_for(jid)
        for src, dst in (("job_label", "label"), ("job_model", "model"),
                         ("job_cwd", "cwd"), ("job_verify", "verify"),
                         ("job_status", "status"), ("job_exit_code", "exit_code"),
                         ("job_preflight", "preflight")):
            if not job.get(dst) and g.get(src) is not None:
                job[dst] = g[src]
        if not job.get("label"):
            logs = sorted(LOGS.glob(f"{jid}-*.log"))
            job["label"] = logs[0].name[len(jid) + 1:-4] if logs else jid
        if not job.get("status"):
            job["status"] = "unknown"
        job["_mtime"] = gf.stat().st_mtime
        out[jid] = job
    # Some jobs are not DISPATCH work -- there is nothing to act on, integrate, or
    # merge, so they must never sit on the handoff pending/complete worklist:
    #   - image generation (Pet Portrait Studio et al.), and
    #   - pipeline-internal jobs: a `draft-` case-drafting run and the `gate-`/`regate-`
    #     review jobs are steps IN a dispatch, not dispatches themselves. (A draft that
    #     picked up a spurious gate record -- fixed in the queue, but old records
    #     linger -- was showing up here as a stale "failed" the panel could never
    #     clear, which is exactly the "not keeping up" drift.)
    _INTERNAL_PREFIXES = ("pet-", "draft-", "gate-", "regate-")
    def _is_non_dispatch(j):
        label = str(j.get("label", ""))
        return (str(j.get("model", "")).lower() == "image"
                or label.startswith(_INTERNAL_PREFIXES))
    return [j for j in out.values() if not _is_non_dispatch(j)]


def is_pending(job):
    # In flight means SOMETHING IS EXECUTING IT, which requires a row in the live
    # queue state -- a status string alone only says what was true when it was last
    # written. A purged job's frozen "running" is not evidence of a running process.
    return bool(job.get("_live")) and (job.get("status") or "").lower() in IN_FLIGHT


def page_name(job):
    ts = (job.get("enqueued_at") or "")[:10] or (
        time.strftime("%Y-%m-%d", time.localtime(job["_mtime"])) if job.get("_mtime") else "undated")
    return f"{ts}-{slug(job.get('label'))}-{job.get('id')}.md"


def render(job, gate, diff):
    files = diff_files(diff)
    L = [f"# {job.get('label') or job.get('id')}", "",
         f"- **job id**: `{job.get('id')}`",
         f"- **status**: `{job.get('status')}`" +
         (f"  (exit {job['exit_code']})" if job.get("exit_code") is not None else "")]
    if gate:
        L.append(f"- **gate**: `{final_verdict(job.get('id'), gate, job)}`")
        c = gate.get("counts") or {}
        if c:
            L.append(f"- **gate counts**: {c.get('code_high',0)} code-high, "
                     f"{c.get('code',0)} code, {c.get('input',0)} input")
        for nc in gate.get("not_checked") or []:
            L.append(f"- **NOT CHECKED**: {nc}")
        for ut in gate.get("untrusted") or []:
            L.append(f"- **UNTRUSTED**: {ut}")
    for key, label in (("model", "model"), ("cwd", "cwd"), ("verify", "verify")):
        if job.get(key):
            L.append(f"- **{label}**: `{job[key]}`")
    if not is_pending(job):
        L.append(f"- **files changed** ({len(files)}): " +
                 (", ".join(f"`{f}`" for f in files) or "_none recorded_"))
    L += ["", f"_Acted on? `python3 ~/bin/handoff-emit.py --acted {job.get('id')}`_", ""]
    if gate and gate.get("issues"):
        L += ["## Gate findings", "", "| sev | where | what |", "|---|---|---|"]
        for i in gate["issues"]:
            loc = (f"{i.get('file','')}:{i.get('line')}"
                   if i.get("file") and i.get("line") else (i.get("file") or "-"))
            L.append(f"| {i.get('severity','?')} | `{loc}` | {str(i.get('what','')).replace('|', chr(92)+'|')} |")
        L.append("")
    if is_pending(job):
        L += ["_In flight -- no diff or gate record until it finishes._", ""]
    elif diff.strip():
        L += ["## Diff", "", "```diff", diff.rstrip(), "```", ""]
    else:
        L += ["## Diff", "", "_No diff was captured for this job._", ""]
    return "\n".join(L)


def reconcile_removals(jobs, acted):
    """A page that was written and is now gone was deleted on purpose."""
    manifest = _load(MANIFEST, None)
    if manifest is None:
        return acted, []          # no manifest = fresh folder; infer nothing
    newly = []
    by_id = {j["id"]: j for j in jobs}
    for jid, rel in manifest.items():
        if jid in acted or jid not in by_id:
            continue
        if rel.startswith("complete/") and not (OUT / rel).exists():
            acted[jid] = {"ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                          "how": "removed from complete/",
                          "label": by_id[jid].get("label")}
            newly.append(jid)
    return acted, newly


# Every prefix here must be specific enough that a REAL coding label can never
# start with it -- hiding a genuine deliverable is far worse than showing an
# extra measurement arm. That is why this is not simply "rt-": `rt-sync-window-fix`
# is real work, `rt-qwen38` is an arm. Extended 2026-09-02 after ~35 scored arms
# piled into the action buckets: pa2-/bo2-/automerge-ladder- were all missing.
EVAL_LABEL_PREFIXES = ("bo-", "bo2-", "pa2-", "rt-qwen", "rt2-qwen",
                       "automerge-ladder-", "bakeoff-", "canary-", "gate-",
                       # Diagnosis CALIBRATION/EVAL runs measure whether the method
                       # works; they produce a DIAGNOSIS.json we read once and score,
                       # not a change to integrate. Deliberately NOT a bare "diag-":
                       # a production diagnosis of a bug we actually need explained
                       # IS a deliverable and must stay visible, and a real coding
                       # label like diag-panel-fix must never be hidden.
                       "diag-calib-", "diag-eval-")


def _label_is_eval(label):
    """The single definition of 'this is a measurement arm, not a deliverable'.
    Both the INDEX writer and the --json read path go through it, because they
    disagreed once already: the filter was added to write_index only, so INDEX.md
    reported 0 awaiting action while the dashboard -- which renders --json -- still
    listed all 38 arms. Two renderings of one truth need one predicate."""
    return (label or "").startswith(EVAL_LABEL_PREFIXES)


def signoff_blocks_acting(jid, label):
    """Refuse to mark a job acted-on while a REQUIRED sign-off is unapproved.

    Marking acted removes a job from every bucket (rebuild() skips acted ids
    before it computes awaiting_signoff), so `--acted` silently overrode the
    sign-off gate entirely -- the one action the gate exists to hold back was
    also the action that erased it. Measured 2026-09-02: all 15
    required-and-unapproved jobs were invisible, because every one had been
    marked acted.

    An EVAL ARM is exempt: bake-off arms edit harness files inside a scratch
    worktree purely to be measured, so the basename rule flags them while there
    is nothing to integrate and nothing to sign off on.

    Returns a refusal string, or None if acting is allowed.
    """
    if _label_is_eval(label):
        return None
    so = _load(Path(__file__).parent / "signoff.json", {}).get(jid) or {}
    if so.get("required") and so.get("verdict") != "approve":
        return (f"{jid} ({label or '?'}) REQUIRES sign-off from "
                f"{so.get('reviewer') or 'a reviewer'} and none is recorded "
                f"(verdict={so.get('verdict')!r}; {so.get('reason') or 'no reason recorded'}). "
                f"Record it with:  signoff.py --verdict approve {jid}\n"
                f"    To clear it WITHOUT sign-off, pass --override-signoff "
                f"\"<why>\" -- that override is recorded.")
    return None


def _is_eval_arm(row):
    """A row is (job, gate, rel). Bake-off/eval arms are MEASUREMENT runs: they have
    no deliverable to integrate, so they belong in neither action bucket."""
    return _label_is_eval((row[0] or {}).get("label"))


# How many acted entries each INDEX section renders. acted.json stays the full
# append-only record; the INDEX is a DASHBOARD and had grown to 16KB rendering
# all 100, which is what "flooded" meant -- 0 pending and 0 awaiting action, but
# every historical entry still on the page. Truncation is stated explicitly
# rather than silent: a list that stops without saying so reads as complete,
# which is the same failure as a check that does not say it did not run.
ACTED_INDEX_LIMIT = int(os.environ.get("HANDOFF_ACTED_LIMIT", "15"))


def _capped(rows, label):
    """Render at most ACTED_INDEX_LIMIT rows, and SAY when older ones are hidden."""
    if ACTED_INDEX_LIMIT <= 0 or len(rows) <= ACTED_INDEX_LIMIT:
        return rows, []
    hidden = len(rows) - ACTED_INDEX_LIMIT
    return rows[:ACTED_INDEX_LIMIT], [
        "", f"_+{hidden} older {label} entr{'y' if hidden == 1 else 'ies'} not shown "
            f"(showing the {ACTED_INDEX_LIMIT} most recent). Full record: `acted.json`._"]


def write_index(pending, complete, acted):
    def table(rs):
        out = ["| job | status | gate | files | flags |", "|---|---|---|---:|---|"]
        for j, g, rel in rs:
            n = len(diff_files(diff_for(j["id"])))
            nc = len(g.get("not_checked") or [])
            flags = " ".join(filter(None, [
                f"{nc} unchecked" if nc else "",
                "UNTRUSTED" if g.get("untrusted") else "",
                "AWAITING SIGN-OFF" if j.get("awaiting_signoff") else ""])) or "-"
            out.append(f"| [{j.get('label') or j['id']}]({rel}) | `{j.get('status')}` | "
                       f"`{final_verdict(j['id'], g, j)}` | {n} | {flags} |")
        return out

    L = ["# Ollama job handoff", "",
         "- **pending/** — still in flight.",
         "- **complete/** — the model finished it; **we haven't acted on it yet** "
         "(reviewed + merged/integrated/deployed).",
         "",
         "A gate `pass` does **not** mean handled — plenty of clean jobs are still "
         "waiting to be merged. Only marking a job acted-on clears it:",
         "", "```", "python3 ~/bin/handoff-emit.py --acted <job-id>", "```", "",
         "Deleting a page from `complete/` by hand does the same thing and sticks. "
         "Everything else is derived — rebuild any time with `--all`.", "",
         "", ""]

    # write_index receives ROWS -- (job_dict, gate_dict, rel_path) tuples built
    # by rebuild() -- not bare job dicts. The sign-off bucket originally called
    # j.get("awaiting_signoff") straight on the tuple, which raised
    # AttributeError and broke every --all rebuild. It shipped because the
    # stage-1 verify only GREPPED for the bucket heading; a grep proves the code
    # mentions a feature, never that the feature runs. Unpack explicitly.
    # The eval-arm exclusion has to apply to BOTH action buckets. It was originally
    # applied only to the regular rows, so a scored arm that happened to carry
    # awaiting_signoff sailed past it -- which is exactly how 11 bake-off arms came
    # to sit in "Awaiting sign-off" as though a human owed them a decision.
    _awaiting_all = [r for r in complete if (r[0] or {}).get("awaiting_signoff")]
    _regular_all = [r for r in complete if not (r[0] or {}).get("awaiting_signoff")]
    awaiting_rows = [r for r in _awaiting_all if not _is_eval_arm(r)]
    regular_rows = [r for r in _regular_all if not _is_eval_arm(r)]
    # Counted separately per bucket, and always PRINTED: a filter that hides work
    # silently is the failure mode this whole index exists to prevent.
    _hidden_signoff = len(_awaiting_all) - len(awaiting_rows)
    _hidden_eval = len(_regular_all) - len(regular_rows)

    L += [f"## Awaiting sign-off ({len(awaiting_rows)})" +
          (f" · {_hidden_signoff} eval arm(s) hidden" if _hidden_signoff else ""), ""]
    L += table(awaiting_rows) if awaiting_rows else ["_Nothing waiting on sign-off._"]

    L += ["", f"## Complete — awaiting action ({len(regular_rows)})" +
          (f" · {_hidden_eval} eval arm(s) hidden" if _hidden_eval else ""), ""]
    L += table(regular_rows) if regular_rows else ["_Nothing waiting on us._"]
    
    L += ["", f"## Pending — in flight ({len(pending)})", ""]
    # `pending` is ALREADY a list of (job, gate, rel) rows, exactly like
    # awaiting_rows and regular_rows above. Re-wrapping it as if its items were
    # dicts raised "tuple indices must be integers" and broke every render that
    # had a job in flight -- the THIRD instance of this same tuple/dict
    # confusion, and the one my harness missed because its fixtures only ever
    # populated the awaiting and complete buckets. Pass the rows straight
    # through, like the two lines above it.
    L += table(pending) if pending else ["_Nothing running._"]
    
    # Add sections for merged and resolved jobs
    merged_jobs = []
    resolved_jobs = []
    unspecified_jobs = []
    
    for jid, meta in sorted(acted.items(), key=lambda kv: kv[1].get("ts", ""), reverse=True):
        if "merged" in meta:
            if meta["merged"] is True:
                merged_jobs.append((jid, meta))
            elif meta["merged"] is False:
                resolved_jobs.append((jid, meta))
        else:
            unspecified_jobs.append((jid, meta))
    
    if merged_jobs:
        L += ["", "## Merged into code", ""]
        _rows, _more = _capped(merged_jobs, "merged")
        for jid, meta in _rows:
            repo = meta.get("repo", "")
            commit = meta.get("commit", "")
            verified = meta.get("verified", False)
            status = "UNVERIFIED" if not verified else ""
            L.append(f"- `{jid}` {meta.get('label') or ''} — {meta.get('ts','')} ({status})")
            if repo and commit:
                L.append(f"  - Repo: {repo}, Commit: {commit}")
        L += _more
        L.append("")
    
    if resolved_jobs:
        L += ["", "## Resolved (no merge)", ""]
        _rows, _more = _capped(resolved_jobs, "resolved")
        for jid, meta in _rows:
            reason = meta.get("reason", "")
            L.append(f"- `{jid}` {meta.get('label') or ''} — {meta.get('ts','')} ({reason})")
        L += _more
        L.append("")
    
    if unspecified_jobs:
        L += ["", "## Acted on (unspecified)", "",
              "_Cleared. Listed so the record survives the page being removed._", ""]
        _rows, _more = _capped(unspecified_jobs, "acted")
        for jid, meta in _rows:
            L.append(f"- `{jid}` {meta.get('label') or ''} — {meta.get('ts','')} ({meta.get('how','')})")
        L += _more
    L.append("")
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "INDEX.md").write_text("\n".join(L))


def collect(acted):
    """The pending / complete split, as data. READ-ONLY.

    Split out from rebuild() so the dashboard can poll it without writing
    anything: a GET that rebuilt the folder on every poll would rewrite 20-odd
    files a few times a minute, and -- far worse -- would run the hand-delete
    reconciliation on a schedule, turning a transient missing file into a
    permanent acted-on mark. Reads never mutate.
    """
    pending, complete = [], []
    signoffs = _load(Path(__file__).parent / "signoff.json", {})
    
    for j in sorted(load_jobs(),
                    key=lambda x: (x.get("enqueued_at") or "", x.get("_mtime", 0)),
                    reverse=True):
        # Filter out internal review runners (gate- pre-gate, regate- Studio re-gate)
        # and measurement jobs (bakeoff-, fit-) -- none are deliverable work
        if j.get("label", "").startswith(("gate-", "regate-", "bakeoff-", "fit-")):
            continue
            
        jid = j["id"]
        if jid in acted:
            continue
        g = gate_for(jid)
        n = len(diff_files(diff_for(jid)))
        nc = len(g.get("not_checked") or [])
        
        # Check if job requires sign-off and hasn't been approved yet
        awaiting_signoff = False
        if (signoffs.get(jid, {}).get("required", False) and 
            signoffs.get(jid, {}).get("verdict") != "approve"):
            awaiting_signoff = True
        
        row = {"id": jid, "label": j.get("label") or jid,
               "status": j.get("status"), "gate": final_verdict(jid, g, j) or "-",
               "files": n, "not_checked": nc,
               "untrusted": bool(g.get("untrusted")),
               "code_high": (g.get("counts") or {}).get("code_high", 0),
               "model": j.get("model"), "cwd": j.get("cwd"),
               "awaiting_signoff": awaiting_signoff}
        sub = "pending" if is_pending(j) else "complete"
        row["page"] = f"{sub}/{page_name(j)}"
        (pending if sub == "pending" else complete).append(row)
    return pending, complete


def rebuild(only_id=None):
    jobs = load_jobs()
    acted = _load(ACTED, {})
    acted, newly = reconcile_removals(jobs, acted)
    if newly:
        _save(ACTED, acted)
        print(f"[handoff] {len(newly)} page(s) removed by hand -> marked acted on")

    pending, complete, manifest = [], [], {}
    for j in sorted(jobs, key=lambda x: (x.get("enqueued_at") or "", x.get("_mtime", 0)), reverse=True):
        # Filter out internal review runners (gate- pre-gate, regate- Studio re-gate)
        # and measurement jobs (bakeoff-, fit-) -- none are deliverable work
        if j.get("label", "").startswith(("gate-", "regate-", "bakeoff-", "fit-")):
            continue
            
        jid = j["id"]
        if jid in acted:
            continue
        g, d = gate_for(jid), diff_for(jid)
        sub = "pending" if is_pending(j) else "complete"
        rel = f"{sub}/{page_name(j)}"
        # Check sign-off status for the complete bucket
        signoffs = _load(Path(__file__).parent / "signoff.json", {})
        awaiting_signoff = False
        if (signoffs.get(jid, {}).get("required", False) and 
            signoffs.get(jid, {}).get("verdict") != "approve"):
            awaiting_signoff = True
        
        row = {"id": jid, "label": j.get("label") or jid,
               "status": j.get("status"), "gate": final_verdict(jid, g, j) or "-",
               "files": len(diff_files(d)), "not_checked": len(g.get("not_checked") or []),
               "untrusted": bool(g.get("untrusted")),
               "code_high": (g.get("counts") or {}).get("code_high", 0),
               "model": j.get("model"), "cwd": j.get("cwd"),
               "awaiting_signoff": awaiting_signoff}
        # Stamp the flag onto the JOB dict -- that is what gets appended below
        # and handed to write_index. The `row` built here was discarded, so
        # awaiting_signoff was computed correctly and then thrown away and the
        # bucket rendered 0 while signoff.json held a required/no-verdict entry.
        # Dead code, invisible to a grep-level check.
        j["awaiting_signoff"] = awaiting_signoff
        row["page"] = rel
        (pending if sub == "pending" else complete).append((j, g, rel))
        page = OUT / rel
        if only_id is None or jid == only_id:
            page.parent.mkdir(parents=True, exist_ok=True)
            page.write_text(render(j, g, d))
        # MANIFEST RECORDS ONLY PAGES THAT EXIST. In --job-id mode (the hook
        # path) every other job is skipped, so recording them regardless would
        # put a page in the manifest that was never written -- and the next
        # rebuild reads "in the manifest, file absent" as a deliberate hand
        # delete and marks it acted on. That would silently retire live work
        # the moment any new job appeared between two hook runs.
        if page.exists():
            manifest[jid] = rel

    # Sweep pages whose job moved folders (pending -> complete) or was acted on,
    # so a stale copy can't sit there looking like live work.
    keep = set(manifest.values())
    for sub in ("pending", "complete"):
        for f in (OUT / sub).glob("*.md") if (OUT / sub).exists() else []:
            if f"{sub}/{f.name}" not in keep:
                f.unlink()
    _save(MANIFEST, manifest)
    write_index(pending, complete, acted)
    return len(pending), len(complete), len(acted)


def _commit_in_default_branch(repo, commit):
    """True iff `commit` is an ancestor of the repo's default branch (main/master,
    local or origin/*) -- i.e. it actually LANDED, not merely that the SHA exists
    (a dispatch-worktree branch commit exists but is NOT merged; that ambiguity is
    how d31d96d23b29 got a false 'merged to main' record). Returns None when the
    repo or commit cannot be resolved here (the caller decides what to do)."""
    try:
        base = Path(repo) if repo else None
        if base is not None and not base.exists():
            base = Path.home() / "Desktop" / "GitHub Projects" / str(repo)
        if base is None or not base.exists():
            return None
        if subprocess.run(["git", "-C", str(base), "cat-file", "-e", commit + "^{commit}"],
                          capture_output=True, timeout=30).returncode != 0:
            return None
        for ref in ("main", "master", "origin/main", "origin/master"):
            if subprocess.run(["git", "-C", str(base), "rev-parse", "--verify", "--quiet", ref],
                              capture_output=True, timeout=15).returncode != 0:
                continue
            if subprocess.run(["git", "-C", str(base), "merge-base", "--is-ancestor", commit, ref],
                              capture_output=True, timeout=30).returncode == 0:
                return True
        return False
    except (OSError, subprocess.SubprocessError):
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--job-id")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--acted", nargs="+", metavar="JOB_ID")
    ap.add_argument("--unact", nargs="+", metavar="JOB_ID")
    ap.add_argument("--merged", metavar="JOB_ID")
    ap.add_argument("--commit", metavar="SHA")
    ap.add_argument("--repo", metavar="NAME")
    ap.add_argument("--reason", metavar="REASON")
    ap.add_argument("--allow-unverified-merge", action="store_true",
                    help="Record a merge/landed claim WITHOUT the ancestor-of-default-branch "
                         "check (squash/rebase gave a different SHA, or the repo is not "
                         "checked out here). Without it, a merge claim whose SHA is not an "
                         "ancestor of main/master is REFUSED (see job d31d96d23b29).")
    ap.add_argument("--override-signoff", metavar="WHY", default=None,
                    help=("clear a job whose REQUIRED sign-off is unapproved. The "
                          "reason is recorded in acted.json. Without this, --acted "
                          "refuses such a job instead of silently erasing the gate."))
    ap.add_argument("--json", action="store_true",
                    help="print the pending/complete split as JSON and write "
                         "nothing -- the dashboard's read path")
    a = ap.parse_args()

    if a.json:
        acted = _load(ACTED, {})
        pending, complete = collect(acted)
        # Same exclusion the INDEX applies, on the dashboard's read path. The
        # dashboard iterates data.complete / data.pending straight into its tables,
        # so filtering HERE makes it agree with INDEX.md without the renderer
        # changing at all. Nothing is dropped: every row keeps an eval_arm flag and
        # the excluded rows are still delivered under their own keys, so a consumer
        # that wants to show or count arms still can.
        for r in pending + complete:
            r["eval_arm"] = _label_is_eval(r.get("label"))
        ev_pending = [r for r in pending if r["eval_arm"]]
        ev_complete = [r for r in complete if r["eval_arm"]]
        print(json.dumps({"pending": [r for r in pending if not r["eval_arm"]],
                          "complete": [r for r in complete if not r["eval_arm"]],
                          "eval_arms_pending": ev_pending,
                          "eval_arms_complete": ev_complete,
                          "hidden_eval_counts": {"pending": len(ev_pending),
                                                 "complete": len(ev_complete)},
                          "acted": acted, "out_dir": str(OUT)}, indent=1))
        return 0

    if a.acted or a.unact:
        acted = _load(ACTED, {})
        labels = {j["id"]: j.get("label") for j in load_jobs()}
        # Check EVERY id before writing ANY of them: a bulk clear that marks half
        # the list and then refuses leaves the caller guessing what landed.
        _blocked = [b for b in (signoff_blocks_acting(j, labels.get(j))
                                for j in (a.acted or [])) if b]
        if _blocked and not a.override_signoff:
            for b in _blocked:
                print(f"[handoff] REFUSED: {b}")
            print(f"[handoff] {len(_blocked)} job(s) blocked by required sign-off; "
                  f"nothing was marked acted.")
            return 2
        for jid in a.acted or []:
            entry = {"ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                     "how": "marked with --acted", "label": labels.get(jid)}
            if a.override_signoff and signoff_blocks_acting(jid, labels.get(jid)):
                entry["how"] = "marked with --acted (SIGN-OFF OVERRIDDEN)"
                entry["signoff_override_reason"] = a.override_signoff
                print(f"[handoff] SIGN-OFF OVERRIDDEN for {jid}: {a.override_signoff}")
            acted[jid] = entry
            print(f"[handoff] acted on: {jid} {labels.get(jid) or ''}")
        for jid in a.unact or []:
            if acted.pop(jid, None):
                print(f"[handoff] un-acted: {jid} {labels.get(jid) or ''}")
            else:
                print(f"[handoff] {jid} was not marked acted on")
        _save(ACTED, acted)

    # Handle --merged flag
    if a.merged and a.commit:
        acted = _load(ACTED, {})
        labels = {j["id"]: j.get("label") for j in load_jobs()}
        
        # Does the commit actually exist? Recorded, never enforced.
        #
        # Two bugs to avoid here, both of which shipped in the first version:
        #   1. it called run_bash(), which does not exist in this file -- a
        #      NameError swallowed by a bare `except: pass`, so `verified` was
        #      False for EVERY commit including real ones. A check that can
        #      only ever fail is not a check.
        #   2. it interpolated the repo path into a shell string, and the real
        #      path contains a space ("GitHub Projects"), so even with a real
        #      shell it would have split the argument.
        # subprocess with a list argv fixes both, and the except now narrows to
        # the failures that are genuinely expected.
        # ENFORCED (2026-09-10): a merge claim is only recorded as verified when the
        # commit is actually an ANCESTOR OF THE DEFAULT BRANCH -- not merely that the
        # SHA exists (a dispatch-worktree branch commit exists but has not landed).
        # If it is not an ancestor, REFUSE rather than record a false merged=True,
        # unless --allow-unverified-merge acknowledges a squash/rebase/remote-only case.
        landed = _commit_in_default_branch(a.repo, a.commit)
        if landed is not True and not a.allow_unverified_merge:
            if landed is False:
                print(f"[handoff] REFUSED: {a.commit} exists in {a.repo} but is NOT an ancestor "
                      f"of its default branch (main/master) -- it has not actually landed. This "
                      f"is exactly the false 'merged to main' record this check exists to prevent "
                      f"(job d31d96d23b29). If it merged via squash/rebase (different SHA) or the "
                      f"repo is not checked out here, pass --allow-unverified-merge.")
            else:
                print(f"[handoff] REFUSED: could not verify {a.commit} landed in {a.repo} (repo "
                      f"or commit not resolvable here). Pass --allow-unverified-merge to record "
                      f"it without the check.")
            return 2
        verified = landed is True
        
        # Mark as merged (this implies acted-on)
        acted[a.merged] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "how": "merged into code",
            "label": labels.get(a.merged),
            "merged": True,
            "commit": a.commit,
            "repo": a.repo,
            "verified": verified
        }
        print(f"[handoff] merged: {a.merged} -> {a.repo}@{a.commit} (verified: {verified})")
        
        # Also mark as acted on if not already done
        if a.merged not in acted:
            acted[a.merged] = {"ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                              "how": "marked with --acted (merged)", "label": labels.get(a.merged)}
        
        _save(ACTED, acted)
    
    # Handle --acted with reason
    if a.reason and a.acted:
        # A --reason that CLAIMS a merge/landing/deploy but carries no --commit to
        # verify is exactly how a job that never landed (d31d96d23b29) got cleared as
        # "merged to main". Refuse it; steer to the verifiable --merged path.
        _MERGE_WORDS = ("merg", "landed", "land in", "in main", "to main", "deployed", "shipped to")
        if (any(w in a.reason.lower() for w in _MERGE_WORDS)
                and not a.commit and not a.allow_unverified_merge):
            print("[handoff] REFUSED: the --reason claims this job merged/landed/deployed, but no "
                  "--commit was given to verify it. A free-text merge claim is how a job that "
                  "never landed (d31d96d23b29) got cleared as 'merged to main'. Use "
                  "`--merged <id> --commit SHA --repo NAME` so the SHA is checked against the "
                  "default branch, or pass --allow-unverified-merge to record this reason as-is.")
            return 2
        acted = _load(ACTED, {})
        labels = {j["id"]: j.get("label") for j in load_jobs()}
        for jid in a.acted or []:
            acted[jid] = {"ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                          "how": f"marked with --acted (reason: {a.reason})", 
                          "label": labels.get(jid),
                          "merged": False,
                          "reason": a.reason}
            print(f"[handoff] acted on: {jid} {labels.get(jid) or ''}")
        _save(ACTED, acted)

    p, c, k = rebuild(None if (a.all or a.acted or a.unact) else a.job_id)
    print(f"[handoff] pending={p} complete(awaiting action)={c} acted={k} -> {OUT/'INDEX.md'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
