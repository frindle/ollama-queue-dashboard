"""bundle_view.py -- one line per slice, from LIVE truth (the user 2026-09-27).

The dashboard's bundle view used to be built from two sources that each lag:
  * the slicer's run file (slice-runs/<plan>.json), whose status only changes when
    the slicer DRIVER next polls. A driver blocked inside the next slice's authoring
    can sit 20+ minutes before it records that the previous slice's coding job
    finished and passed its gate -- so the bundle read "1/5" with s2 long green
    (sidecar-bfmr-login-fetch, 2026-09-27 13:24-13:50);
  * the live queue rows, which only cover slices that currently HAVE a job -- so
    s4/s5 (not started) and every reaped job vanished.

This module joins them with what is actually happening right now -- running and
queued jobs, the gate verdict sidecars, the live off-GPU progress records
(dispatch_progress: preflight / verify-relevance), the AUTO driver's chain record,
the self-heal ledger -- into one list covering EVERY slice in the plan's `order`.

Why READ-side (option a) and not have the queue write the run file (option b): the
slicer's own poll does more than flip a status -- it commits the slice's deliverable
onto the chain (3-way rebase + re-verify) and only THEN marks it done, and
save_state() deliberately preserves a DONE it finds on disk (merge_terminal_facts).
A daemon writing `done` early would make the driver skip that integration step for
the slice. The run file stays the slicer's; readers derive the live phase.

Everything here is PURE given its inputs (the loaders at the bottom do the I/O), so
the phase rules are unit-tested without a queue, a GPU or a real run file.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import time
from pathlib import Path

PHASES = ("pending", "authoring", "refining", "preflight", "queued", "coding", "gate",
          "regate", "self-heal", "escalation-review", "second-opinion", "escalated", "done",
          "failed")
ACTIVE_PHASES = {"authoring", "refining", "preflight", "queued", "coding", "gate",
                 "regate", "self-heal", "escalation-review", "second-opinion"}
ATTENTION_PHASES = {"escalated", "failed"}
AUTHOR_ATTEMPT_CAP = 5   # mirrors ollama-dispatch-slice MAX_AUTHOR_ATTEMPTS (display only)
_LIVE = {"running", "pending", "queued", "scheduled", "held", "paused"}
# Marks a job dict that load_history() rebuilt from durable files (the queue already
# pruned its row): rendered as a read-only history line, never as a live queue row.
HISTORIC = "historic"
_WAITING = _LIVE - {"running"}
_PASS = {"pass"}
# how "hot" a live phase is, for picking the ONE current slice of a bundle
_HEAT = {"coding": 9, "gate": 8, "regate": 8, "refining": 7, "authoring": 7,
         "preflight": 6, "self-heal": 5, "escalation-review": 5, "second-opinion": 3, "queued": 4}


def _ts(v):
    """ISO string / epoch -> epoch float, or None."""
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        from datetime import datetime
        s = str(v).replace("Z", "+00:00")
        return datetime.fromisoformat(s).timestamp()
    except Exception:
        return None


def strip_stage(label):
    """'auto-refine-P-s3-x-r2' -> ('P-s3-x', 'refine', 2)."""
    lbl = str(label or "")
    stage = "coding"
    m = re.match(r"^(auto-author-|auto-refine-)", lbl)
    if m:
        stage = "author" if m.group(1) == "auto-author-" else "refine"
        lbl = lbl[m.end():]
    rnd = None
    while True:
        m = re.search(r"-(r|c)(\d+)$", lbl)
        if not m:
            break
        if m.group(1) == "r" and rnd is None:
            rnd = int(m.group(2))
        lbl = lbl[:m.start()]
    return lbl, stage, rnd


_ESC_REVIEW_RE = re.compile(r"^esc-review-(?:(?:\d{0,8}T)?\d*Z-)?(.+)$")


def esc_review_ref(label):
    """'esc-review-<TS>-<bundle>-<slice>' -> '<bundle>-<slice>' (None if not one). The
    watcher keeps only the last 60 chars of the context stem, so the TS (and on long
    names even the head of the bundle) may be cut: the '<TS>-' prefix is optional."""
    m = _ESC_REVIEW_RE.match(re.sub(r"\s*\[[^\]]*\]\s*$", "", str(label or "")).strip())
    return m.group(1) if m else None


def job_sid(job, plan, sids, jobs_by_id=None, _hops=4, label_of=None):
    """The slice id (from `sids`) a queue job belongs to, or None. Gate rows resolve
    through the job they gate; everything else by label, then by worktree name."""
    lbl = str(job.get("label") or "")
    ref = esc_review_ref(lbl)
    jm = re.match(r"^job-([0-9a-f]{6,})$", ref or "")
    if jm:
        # job-level escalation: resolve through the escalated job, like a second opinion
        src = (jobs_by_id or {}).get(jm.group(1))
        if src is not None and _hops > 0 and src is not job:
            return job_sid(src, plan, sids, jobs_by_id, _hops - 1, label_of)
        pl = label_of(jm.group(1)) if label_of and _hops > 0 else None
        if pl:
            return job_sid({"label": pl}, plan, sids, jobs_by_id, _hops - 1, label_of)
        return None
    if ref:
        # longest sid first so 's1-a' never shadows 's1-a-b'; the head of `ref` may be
        # truncated (watcher keeps the last 60 chars), so also accept a tail match.
        for sid in sorted(sids, key=len, reverse=True):
            full = f"{plan}-{sid}"
            if ref == full or ref.endswith("-" + full) or \
                    (len(ref) >= 40 and full.endswith(ref)):
                return sid
        return None
    sm = re.match(r"^secondop-([0-9a-f]{6,})", lbl)
    if sm:
        # A second opinion carries only its PARENT job id: resolve through the live
        # parent, else (parent already reaped) through its durable label.
        src = (jobs_by_id or {}).get(sm.group(1))
        if src is not None and _hops > 0 and src is not job:
            return job_sid(src, plan, sids, jobs_by_id, _hops - 1, label_of)
        pl = label_of(sm.group(1)) if label_of and _hops > 0 else None
        if pl:
            return job_sid({"label": pl}, plan, sids, jobs_by_id, _hops - 1, label_of)
        return None
    m = re.match(r"^(re)?gate-([0-9a-f]{6,})", lbl)
    if m:
        src = (jobs_by_id or {}).get(m.group(2))
        if src is not None and _hops > 0 and src is not job:
            return job_sid(src, plan, sids, jobs_by_id, _hops - 1)
        return None
    base, _stage, _r = strip_stage(lbl)
    if base.startswith(f"{plan}-"):
        rest = base[len(plan) + 1:]
        if rest in sids:
            return rest
    cwd = Path(str(job.get("cwd") or "")).name
    for sid in sids:
        if cwd == f"wt-slice-{plan}-{sid}":
            return sid
    return None


def job_kind(job):
    """(kind, phase) for one queue job: authoring/refining/coding/gate/regate."""
    lbl = str(job.get("label") or "")
    if esc_review_ref(lbl):
        return "escalation review", "escalation-review"
    if lbl.startswith("secondop-"):
        return "second opinion", "second-opinion"
    if lbl.startswith("regate-"):
        return "regate", "regate"
    if lbl.startswith("gate-"):
        return "gate", "gate"
    _b, stage, rnd = strip_stage(lbl)
    if stage == "author":
        return "authoring", "authoring"
    if stage == "refine":
        return "refining", f"refining (round {rnd or 1})"
    return "coding", "coding"


def _wt_key(wt):
    try:
        return str(Path(str(wt)).resolve())
    except Exception:
        return str(wt)


def slice_phase(sid, s, plan, jobs_for_slice, verdict_of, progress, chain, heal,
                now):
    """PURE. (phase, detail, since) for ONE slice from live truth.

    `jobs_for_slice` queue rows mapped to this slice; `verdict_of(job_id)` -> the
    durable gate verdict ('pass'/'fail'/...) or None; `progress` live
    dispatch_progress records for the slice's worktree; `chain` the AUTO driver's
    auto-runs record when it is driving THIS slice (pid alive), else None; `heal`
    this slice's self-heal ledger record or None."""
    st = str((s or {}).get("status") or "pending")
    if st == "skipped":
        return "done", "skipped (already satisfied)", None
    if st == "done":
        return "done", "", _ts((s or {}).get("done_at"))
    job_id = (s or {}).get("job_id")
    # LIVE TRUTH 1: the slice's coding job already passed its gate -- done, whether
    # or not the driver has polled it yet (the stale-counter bug).
    if job_id and st == "enqueued" and (verdict_of(job_id) or "").lower() in _PASS:
        return "done", "gate pass (slicer will record it on its next poll)", None
    running = [j for j in jobs_for_slice if j.get("status") == "running"]
    # gate/regate outrank the coding row they review; a second opinion is ADDITIVE, so
    # it only ever wins when nothing else is running
    running.sort(key=lambda j: {"gate": 0, "regate": 0, "second opinion": 2}.get(
        job_kind(j)[0], 1))
    if running:
        j = running[0]
        _k, ph = job_kind(j)
        base = ph.split(" ")[0] if ph.startswith("refining") else ph
        return (base if base != "refining" else "refining"), ph if base == "refining" \
            else "", _ts(j.get("launched_at"))
    if progress:
        p = sorted(progress, key=lambda r: -float(r.get("updated_at") or 0))
        rel = next((r for r in p if r.get("tool") == "verify-relevance"), None)
        pre = next((r for r in p if r.get("tool") == "preflight"), None)
        lead = rel or pre or p[0]
        det = f"{lead.get('stage')}"
        if lead.get("total"):
            det += f" {lead.get('done') or 0}/{lead['total']} {lead.get('detail') or ''}".rstrip()
        elif lead.get("detail"):
            det += f": {lead['detail']}"
        since = min(float(r.get("started_at") or now) for r in p)
        return "preflight", det, since
    # a QUEUED second opinion never changes the slice's phase (additive child row only)
    waiting = [j for j in jobs_for_slice if j.get("status") in _WAITING
               and job_kind(j)[0] != "second opinion"]
    if waiting:
        j = waiting[0]
        k, ph = job_kind(j)
        if k == "coding":
            return "queued", "coding job waiting for the GPU", _ts(j.get("enqueued_at"))
        if k in ("gate", "regate"):
            return k, "waiting for the GPU", _ts(j.get("enqueued_at"))
        if k == "escalation review":
            return "escalation-review", "queued", _ts(j.get("enqueued_at"))
        return ("refining" if k == "refining" else "authoring"), \
            (ph + " -- queued" if k == "refining" else "queued"), _ts(j.get("enqueued_at"))
    if chain:
        step = str(chain.get("step") or "")
        since = _ts(chain.get("phase_since"))
        if chain.get("phase") == "advancing":
            if step.startswith("preflight"):
                return "preflight", step, since
            return "authoring", step or "driver advancing", since
        if chain.get("phase") == "waiting":
            _b, stage, rnd = strip_stage(step)
            if stage == "refine":
                return "refining", f"refining (round {rnd or 1})", since
            return "authoring", "", since
    if st == "escalated":
        if heal and heal.get("log"):
            last = heal["log"][-1]
            if last.get("action") in ("patch+regate", "retry-slice"):
                t = _ts(last.get("at"))
                if t and now - t < 30 * 60:
                    return "self-heal", f"{last.get('action')} (attempt {last.get('attempt')})", t
        # ESCALATED = the chain is STOPPED for a human. Say why AND how much budget it
        # burned getting here ("author 5/5 attempts, 7/8 jobs"), not just a reason prefix
        # (2026-10-06: an escalated slice read as an ordinary row with a cut-off sentence).
        _att = int(s.get("author_attempts") or 0)
        _jobs = max(0, len(s.get("author_job_ids") or []) - int(s.get("author_jobs_at_retry") or 0))
        _bud = f" [author {_att}/{AUTHOR_ATTEMPT_CAP} attempts, {_jobs} jobs]" if (_att or _jobs) else ""
        return ("escalated", "ESCALATED, needs a human: "
                + str(s.get("escalation_reason") or "(no reason recorded)")[:200] + _bud, None)
    if job_id and st == "enqueued":
        v = (verdict_of(job_id) or "").lower()
        if not v:
            return "gate", "coding finished; gate verdict pending", None
        return "failed", f"gate {v.upper()}", None
    if st == "failed":
        return "failed", "", None
    if st in ("authoring", "awaiting_review", "confirmed"):
        return ("preflight" if st == "confirmed" else "authoring"), st, None
    return "pending", ("blocked on a dependency" if st == "blocked" else ""), None


def slice_history(sid, s, jobs_for_slice, verdict_of, result_of, heal, preflight_rec,
                  now):
    """PURE. The slice's runs in time order: [{kind, id, label, status, start, end,
    duration_s, result, log}] -- queue jobs (live or not yet reaped), the coding
    job's durable result when its row is gone, preflight's last verdict, self-heal
    and escalation entries."""
    out = []
    seen = set()
    for j in jobs_for_slice:
        if j.get("id") in seen:
            continue
        k, ph = job_kind(j)
        start = _ts(j.get("launched_at"))
        dur = j.get("active_s")
        if j.get("status") == "running" and start:
            dur = float(dur or 0) + max(0.0, now - start)
        res = None
        if j.get("status") not in _LIVE:
            v = verdict_of(j.get("id")) if k == "coding" else None
            res = (f"gate {v.upper()}" if v else
                   f"{j.get('status')} exit={j.get('exit_code')}")
        historic = bool(j.get(HISTORIC))
        if historic and j.get("result"):
            res = j["result"]   # load_history already read the durable verdict/report
        out.append({"kind": ph, "id": j.get("id"), "label": j.get("label"),
                    "status": j.get("status"), "start": start or _ts(j.get("enqueued_at")),
                    "duration_s": round(float(dur), 1) if dur is not None else None,
                    "result": res, "log": j.get("id"), "live": not historic})
        seen.add(j.get("id"))
    job_id = (s or {}).get("job_id")
    if job_id and job_id not in seen:
        r = result_of(job_id) or {}
        v = r.get("verdict")
        out.append({"kind": "coding", "id": job_id, "label": r.get("label"),
                    "status": r.get("status") or "done",
                    "start": _ts(r.get("timestamp")), "duration_s": None,
                    "result": f"gate {str(v).upper()}" if v else "gate pending",
                    "log": job_id, "live": False})
    if preflight_rec:
        out.append({"kind": "preflight", "id": None, "label": "preflight",
                    "status": "done", "start": _ts(preflight_rec.get("at")),
                    "duration_s": None, "result": str(preflight_rec.get("verdict") or "?"),
                    "log": None, "live": False})
    for e in (heal or {}).get("log") or []:
        out.append({"kind": "self-heal", "id": None, "label": e.get("action"),
                    "status": "done", "start": _ts(e.get("at")), "duration_s": None,
                    "result": str(e.get("detail") or "")[:200], "log": None, "live": False})
    if str((s or {}).get("status")) == "done":
        # accept / land: the slicer committed the slice onto the chain. The run file
        # keeps no landing time, so it sorts last (start None) -- it IS the last step.
        gv = (s or {}).get("gate_verdict")
        out.append({"kind": "landed", "id": None, "label": "accepted / landed",
                    "status": "done", "start": _ts((s or {}).get("done_at")),
                    "duration_s": None,
                    "result": ("relanded" if (s or {}).get("relanded_from") else "landed")
                    + (f" (gate {str(gv).upper()})" if gv else ""),
                    "log": None, "live": False})
    if str((s or {}).get("status")) == "escalated" or (s or {}).get("escalation_reason"):
        out.append({"kind": "escalated", "id": None, "label": "escalation",
                    "status": str((s or {}).get("status")),
                    "start": _ts((s or {}).get("escalated_at")), "duration_s": None,
                    "result": str((s or {}).get("escalation_reason") or "")[:240],
                    "log": None, "live": False})
    out.sort(key=lambda e: (e["start"] is None, e["start"] or 0))
    # Group into attempts: each authoring run opens a new attempt, so a re-author
    # reads as "attempt 2" instead of a second identically-labelled row. Entries
    # without a start (pending escalation) stay last, in the attempt they belong to.
    n = 1
    seen_author = False
    for e in out:
        if e["kind"] == "authoring":
            if seen_author:
                n += 1
            seen_author = True
        e["attempt"] = n
    return out


def default_counter_of(plan, sid, s):
    """THE attempt counter (failure_ledger.slice_counter, shared with the slicer's status text
    and budget enforcement). None when the ledger module or its data is unavailable --
    the dashboard must render without it. Only slices that burned author jobs get one."""
    try:
        import sys as _sys
        for d in (Path.home() / "bin", Path(__file__).resolve().parent):
            if (d / "failure_ledger.py").is_file() and str(d) not in _sys.path:
                _sys.path.append(str(d))
        import failure_ledger as _fl
        c = _fl.slice_counter(plan, sid, s)
        return c if (c.get("jobs_lifetime") or c.get("attempts")) else None
    except Exception:
        return None


def build_view(plan, state, jobs, now=None, verdict_of=None, result_of=None,
               progress=None, chain=None, heal_ledger=None, preflight_of=None,
               sub_view=None, counter_of=None):
    """PURE given its injectables. The bundle view for slice plan `plan`:
    {key, through, total, current, slices: [{sid, title, status, phase, detail,
    since, elapsed_s, active, attention, history}]}; every slice in `order` appears.
    `sub_view(sub_label)` -> a nested view for a slice split into its own sub-plan."""
    now = time.time() if now is None else now
    verdict_of = verdict_of or (lambda _id: None)
    result_of = result_of or (lambda _id: None)
    preflight_of = preflight_of or (lambda _wt: None)
    state = state if isinstance(state, dict) else {}
    slices = state.get("slices") if isinstance(state.get("slices"), dict) else {}
    order = state.get("order") if isinstance(state.get("order"), list) else []
    ids = list(order) + [k for k in slices if k not in order]
    jobs = list(jobs or [])
    by_id = {j.get("id"): j for j in jobs if j.get("id")}
    per = {sid: [] for sid in ids}
    for j in jobs:
        sid = job_sid(j, plan, ids, by_id,
                      label_of=lambda i: (result_of(i) or {}).get("label"))
        if sid is not None:
            per[sid].append(j)
    prog_by_wt = {}
    for r in progress or []:
        prog_by_wt.setdefault(_wt_key(r.get("wt")), []).append(r)
    rows = []
    through = total = 0
    for sid in ids:
        s = slices.get(sid) or {}
        sub = sub_view(f"{plan}-{sid}") if sub_view else None
        wt = s.get("worktree")
        if not wt and state.get("repo"):
            wt = str(Path(str(state.get("worktree_root") or
                              Path.home() / ".ollama-dispatch" / "worktrees"))
                     / f"wt-slice-{plan}-{sid}")
        prog = prog_by_wt.get(_wt_key(wt), []) if wt else []
        ch = None
        if chain and str(chain.get("label") or "") == f"{plan}-{sid}":
            ch = chain
        heal = (heal_ledger or {}).get(f"{plan}/{sid}")
        if sub and sub.get("total"):
            through += sub["through"]
            total += sub["total"]
            cur = next((x for x in sub["slices"] if x["sid"] == sub.get("current")), None)
            phase = "done" if sub["through"] == sub["total"] else (
                cur["phase"] if cur else ("escalated" if any(
                    x["attention"] for x in sub["slices"]) else "pending"))
            detail = f"split into {sub['total']} sub-slices, {sub['through']} done"
            since = cur["since"] if cur else None
        else:
            total += 1
            phase, detail, since = slice_phase(sid, s, plan, per[sid], verdict_of, prog,
                                               ch, heal, now)
            if phase == "done":
                through += 1
        hist = slice_history(sid, s, per[sid], verdict_of, result_of, heal,
                             preflight_of(wt) if wt else None, now)
        rows.append({
            "sid": sid, "title": s.get("title") or "", "status": s.get("status") or "pending",
            "phase": phase, "detail": detail, "since": since,
            "elapsed_s": round(now - since, 1) if since else None,
            "active": phase in ACTIVE_PHASES, "attention": phase in ATTENTION_PHASES,
            "history": hist, "sub": sub,
            "counter": (counter_of or default_counter_of)(plan, sid, s),
        })
    act = [r for r in rows if r["active"]]
    current = max(act, key=lambda r: (_HEAT.get(r["phase"].split(" ")[0], 1),
                                      -(r["since"] or now)))["sid"] if act else None
    return {"key": plan, "through": through, "total": total, "current": current,
            "slices": rows}


# --- superseded / retired marker -------------------------------------------------------
# A failed/stalled bundle (or one failed slice) that a LATER bundle/job replaced keeps
# reading "failed" forever. `qctl supersede` records it here; bundle_outcome() then
# reports "superseded" and the dashboard excludes it from the Stalled count.
# Keys: "<bundle>" (whole bundle) or "<bundle>#<slice-id>" (one slice).
def superseded_path():
    return Path(os.environ.get("OLLAMA_SUPERSEDED_FILE")
                or Path.home() / ".ollama-dispatch" / "bundle-superseded.json")


def load_superseded(path=None):
    try:
        d = json.loads(Path(path or superseded_path()).read_text())
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _save_superseded(d, path=None):
    p = Path(path or superseded_path())
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(p.name + ".tmp")
    tmp.write_text(json.dumps(d, indent=1, sort_keys=True))
    os.replace(tmp, p)


def mark_superseded(bundle, by, reason, slice_id=None, path=None, now=None):
    if not (bundle and by and (reason or "").strip()):
        raise ValueError("bundle, superseded-by and a reason are all required")
    d = load_superseded(path)
    key = f"{bundle}#{slice_id}" if slice_id else bundle
    d[key] = {"by": by, "reason": reason.strip(),
              "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now or time.time()))}
    _save_superseded(d, path)
    return key


def unmark_superseded(bundle, slice_id=None, path=None):
    d = load_superseded(path)
    key = f"{bundle}#{slice_id}" if slice_id else bundle
    found = d.pop(key, None) is not None
    if found:
        _save_superseded(d, path)
    return found


# --- actionable vs stale backlog ---------------------------------------------------------
# The Needs-attention tile counts only ACTIONABLE stalled bundles: recent (<= ACTIONABLE_DAYS
# since last activity) or listed in the attention file (bundles the triage classified
# REAL-UNFINISHED / NEEDS-REVIEW). Everything else is the "stale backlog" -- still listed
# in the Stalled panel, never hidden, just not in the headline.
ACTIONABLE_DAYS = 7


def attention_path():
    return Path(os.environ.get("OLLAMA_ATTENTION_FILE")
                or Path.home() / ".ollama-dispatch" / "bundle-attention.json")


def load_attention(path=None):
    try:
        d = json.loads(Path(path or attention_path()).read_text())
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def is_actionable(key, last_activity, now, attention=None):
    if key in (attention or {}):
        return True
    return bool(last_activity) and (now - last_activity) <= ACTIONABLE_DAYS * 86400


def bundle_outcome(view, superseded=None, cancelled=None):
    """PURE. How a bundle with NOTHING LIVE actually ended: "finished" | "failed" |
    "incomplete" | "superseded" (+ the number of failed slices).

    ROOT CAUSE this closes (Penn 2026-10-06, "considered finished but have failed
    flags"): the finished list was "every bundle with no queue row left", i.e.
    FINISHED meant NOTHING LIVE, so a bundle whose slice FAILED (or ended / needs_opus)
    and was never retried sat in Finished, "2/3", flagged failed. FINISHED now means
    every slice done (or skipped/retired, which slice_phase already maps to "done").
      failed     any slice failed/escalated/ended (attention, recursively through a
                 split slice's sub-view) -- stalled on a failure, needs a look
      incomplete nothing failed but slices are still owed (through < total) and nothing
                 is live to run them -- also stalled, never finished
      superseded `superseded` (load_superseded()) marks the whole bundle, or a failed
                 slice ("bundle#sid"), as replaced by a later bundle/job: a marked
                 bundle that is not finished is "superseded" (not stalled); a marked
                 slice no longer counts as failed/owed
    A HUMAN-CANCELLED plan (`cancelled`: the plan_cancel.cancelled(label) record) is
    retired the same way as a superseded one: a deliberate `--cancel` is terminal, so an
    unfinished cancelled bundle reads "superseded", never "failed"/"incomplete"
    (replay-endorse sat in Needs-attention for a day after its 2026-10-05 --cancel).
    The caller must only ask this of a bundle that has no live work."""
    sup = superseded or {}
    v = view or {}
    key = v.get("key")

    def _walk(slices, acc):
        for s in slices or []:
            sub = s.get("sub")
            if isinstance(sub, dict) and sub.get("slices"):
                _walk(sub["slices"], acc)
            else:
                acc.append(s)
        return acc
    leaves = _walk(v.get("slices"), [])
    marked = [s for s in leaves if f"{key}#{s.get('sid')}" in sup]
    bad = sum(1 for s in leaves if s not in marked
              and (s.get("attention") or s.get("phase") in ATTENTION_PHASES))
    if bad:
        outcome = "failed"
    else:
        owed = int(v.get("through") or 0) < int(v.get("total") or 0)
        if owed and marked:   # a retired slice is not owed
            owed = int(v.get("through") or 0) + sum(
                1 for s in marked if s.get("phase") != "done") < int(v.get("total") or 0)
        outcome = "incomplete" if owed else "finished"
    if outcome != "finished" and (key in sup or cancelled):
        return "superseded", bad
    return outcome, bad


_CHILD_RE = re.compile(r"^(?:(?:re)?gate|secondop)-([0-9a-f]{6,})")


def child_parent_id(job):
    """The parent JOB id a gate/regate/secondop/esc-review(job form) row names, or None."""
    lbl = str(job.get("label") or "")
    m = _CHILD_RE.match(lbl)
    if m:
        return m.group(1)
    ref = esc_review_ref(lbl)
    jm = re.match(r"^job-([0-9a-f]{6,})$", ref or "")
    return jm.group(1) if jm else None


_PASSED_RUNS_MEMO = {}


def load_passed_runs(runs_dir=None):
    """I/O. [(label, bundle, ended_epoch)] for every ended auto-run (~/.ollama-dispatch/
    auto-runs/*.json) whose outcome is a clean `exit 0`. Best-effort: [] on any error."""
    out, seen = [], set()
    d = Path(runs_dir or os.environ.get("OLLAMA_DISPATCH_AUTO_RUNS_DIR")
             or Path.home() / ".ollama-dispatch" / "auto-runs")
    hit = _PASSED_RUNS_MEMO.get(str(d))
    if hit and time.monotonic() - hit[0] < 10:
        return hit[1]
    _PASSED_RUNS_MEMO[str(d)] = (time.monotonic(), out)   # filled in place below
    try:
        files = sorted(d.glob("*.json"))
    except Exception:
        return out
    for f in files:
        rec = _read_json(f)
        if not isinstance(rec, dict):
            continue
        for r in list((rec.get("runs") or {}).values()) + [rec]:
            if not isinstance(r, dict) or not r.get("label") or r.get("phase") != "ended" \
                    or str(r.get("outcome") or "").strip() != "exit 0":
                continue
            t = _ts(r.get("updated_at")) or _ts(r.get("phase_since"))
            k = (r["label"], r.get("bundle") or r.get("key"), t)
            if t and k not in seen:
                seen.add(k)
                out.append(k)
    return out


def _pid_alive(pid):
    try:
        os.kill(int(pid), 0)
        return True
    except PermissionError:
        return True
    except Exception:
        return False


def load_active_runs(runs_dir=None, alive=None):
    """I/O. [(label, bundle, step)] for auto-run chains whose driver is ALIVE and still
    working (no outcome, pid alive, not parked-after-its-last-step). A chain between queue
    rounds has no live queue row, so without this its bundle reads failed/finished."""
    alive = alive or _pid_alive
    out, seen = [], set()
    d = Path(runs_dir or os.environ.get("OLLAMA_DISPATCH_AUTO_RUNS_DIR")
             or Path.home() / ".ollama-dispatch" / "auto-runs")
    try:
        files = sorted(d.glob("*.json"))
    except Exception:
        return out
    for f in files:
        rec = _read_json(f)
        if not isinstance(rec, dict):
            continue
        for r in list((rec.get("runs") or {}).values()) + [rec]:
            if not isinstance(r, dict) or not r.get("label") or r.get("outcome") \
                    or r.get("phase") == "ended" or not r.get("pid") or not alive(r.get("pid")):
                continue
            pk = r.get("parked")
            if isinstance(pk, dict) and (_ts(pk.get("at")) or 0) >= (_ts(r.get("phase_since")) or 0):
                continue
            k = (r["label"], r.get("bundle") or r.get("key"), str(r.get("step") or r.get("phase") or ""))
            if k not in seen:
                seen.add(k)
                out.append(k)
    return out


def _attempts_summary(mine, base):
    """PURE. One compact line for a DONE group that needed several attempts:
    'passed after 3 attempts: r1 failed (prose_loop) -> r2 failed -> r3 passed'. Failed
    attempts keep their own failed label; only the final pass is called passed."""
    works = sorted([x for x in mine if child_parent_id(x) is None and not esc_review_ref(x.get("label"))
                    and x.get("status") not in _LIVE and x.get("status") != "planned"],
                   key=lambda x: (_ts(x.get("launched_at")) or _ts(x.get("enqueued_at")) or 0))
    if len(works) < 2 or not any(x.get("status") not in ("done", "done_unconverged") for x in works):
        return ""
    parts = []
    for x in works:
        _b, stage, rnd = strip_stage(x.get("label"))
        tag = (f"r{rnd}" if rnd else stage) if stage != "coding" or rnd else "code"
        st = "passed" if x.get("status") in ("done", "done_unconverged") else "failed"
        why = x.get("terminal_reason") if st == "failed" else None
        parts.append(f"{tag} {st}" + (f" ({why})" if why else ""))
    if len(parts) > 5:
        parts = ["..."] + parts[-5:]
    return f"passed after {len(works)} attempts: " + " -> ".join(parts)


def build_job_view(key, jobs, now=None, verdict_of=None, result_of=None, passed_runs=None, active_runs=None):
    """PURE. A bundle made of directly-enqueued JOBS (`--bundle <key>`, no slicer plan):
    one pseudo-slice per non-child job (header "Job . <label>"), with its gate/regate/
    secondop/esc-review rows nested as that pseudo-slice's history -- the same shape as
    build_view so the front-end renders both identically. None when `jobs` holds no
    non-child job. Jobs of one FEATURE (author/refine/coding of the same base label)
    share one pseudo-slice -- see the grouping comment below."""
    now = time.time() if now is None else now
    verdict_of = verdict_of or (lambda _i: None)
    result_of = result_of or (lambda _i: None)
    jobs = list(jobs or [])
    # an esc-review row (any form) is a CHILD of the slice/job it reviews, never a job
    mains = [j for j in jobs if child_parent_id(j) is None and not esc_review_ref(j.get("label"))]
    ids = {j.get("id") for j in mains}
    kids = {}
    for j in jobs:
        pid = child_parent_id(j)
        if pid is not None:
            kids.setdefault(pid, []).append(j)
    # a child whose parent job already left the queue: synthesize the parent from its
    # durable label (done.json), so the gate still nests under ITS job.
    for pid in kids:
        if pid not in ids:
            mains.append({"id": pid, "status": "done",
                          "label": (result_of(pid) or {}).get("label") or pid})
    _mine_runs = [a for a in active_runs or [] if a[1] is None or a[1] == key]
    if not mains and not _mine_runs:
        return None
    # ONE pseudo-slice per FEATURE, not per job (the user 2026-10-05: "the full history of
    # each slice"): auto-author-X, auto-refine-X-rN and the coding job X are the
    # author -> refine -> code stages of one unit of work, so they share a line whose
    # history carries every run (finished ones included, via load_history). Distinct
    # features stay distinct lines. Ordered by first activity; the line's sid is its
    # FIRST job's id (stable as later rounds are added) and its status/phase follow
    # the NEWEST job, with any running/waiting run winning.
    groups, order = {}, []
    for m in mains:
        base = strip_stage(re.sub(r"\s*\[[^\]]*\]\s*$", "", str(m.get("label") or "")))[0] \
            or str(m.get("id"))
        if base not in groups:
            groups[base] = []
            order.append(base)
        groups[base].append(m)

    def _t(j):
        return _ts(j.get("launched_at")) or _ts(j.get("enqueued_at"))
    rows = []
    through = 0
    for base in order:
        ms = sorted(groups[base], key=lambda j: (_t(j) is None, _t(j) or 0))
        first = ms[0]
        mine = []
        for x in ms:
            mine += [x] + kids.get(x.get("id"), [])
        live_ms = [x for x in ms if x.get("status") in _LIVE or x.get("status") == "planned"]
        m = dict(live_ms[-1] if live_ms else ms[-1])
        m["id"] = first.get("id")
        m["label"] = base if len(ms) > 1 else (first.get("label") or base)
        st = str(m.get("status") or "pending")
        running = [j for j in mine if j.get("status") == "running"]
        waiting = [j for j in mine if j.get("status") in _WAITING]
        if running:
            phase = job_kind(running[0])[1] if job_kind(running[0])[0] != "coding" else "coding"
        elif st in ("pending", "queued", "scheduled", "held", "paused", "planned"):
            phase = "queued"
        elif st in ("done", "done_unconverged"):
            phase = "done"
        else:
            phase = "failed"
        if phase == "done":
            through += 1
        since = _ts((running or [m])[0].get("launched_at"))
        hist = slice_history(m.get("id"), {}, mine, verdict_of, result_of, None, None, now)
        detail = ""
        rows.append({"sid": m.get("id"), "title": m.get("label") or "", "job": True,
                     "status": st, "phase": phase, "detail": detail, "since": since,
                     "elapsed_s": round(now - since, 1) if since and running else None,
                     "active": phase in ACTIVE_PHASES, "attention": phase == "failed",
                     "history": hist, "sub": None,
                     "summary": _attempts_summary(mine, base) if phase == "done" else "",
                     "_base": base, "_t": max([_t(x) or 0 for x in mine] or [0]),
                     "_sup": [x.get("superseded_by") for x in mine
                              if x.get("status") in ("failed", "needs_opus") and x.get("superseded_by")]})
    # SUPERSEDED FAILURES (Penn 2026-10-09, rt-bg-commitments-guard "9/9 jobs 5 failed"
    # beside chains that later exited 0): a failed pseudo-slice is OUTSTANDING only when
    # nothing later in its lineage passed or is still running. Three provable proofs, none
    # weaker; the row stays in the expanded history either way:
    #   (a) its failed job's superseded_by names a job that is now live (retrying) / done;
    #   (b) it is an `<X>-sN` sub-slice of a sibling job group X that is done / still live;
    #   (c) an ended auto-run `exit 0` for chain X (X or X-sN, same bundle) finished after
    #       the group's last activity (passed_runs=[(label, bundle, ended_epoch)]).
    by_id = {x.get("id"): x for x in jobs}
    by_base = {r["_base"]: r for r in rows}

    def _supersede(r, phase, why):
        r["phase"], r["attention"], r["active"] = phase, False, phase in ACTIVE_PHASES
        r["detail"], r["summary"] = "", why
    for r in rows:
        if r["phase"] != "failed":
            continue
        for (lab, bun, step) in active_runs or []:
            if (bun is None or bun == key) and (r["_base"] == lab or re.fullmatch(re.escape(lab) + r"-s\d+", r["_base"])):
                _supersede(r, "queued", f"chain {lab} still advancing ({step})")
                break
        if r["phase"] != "failed":
            continue
        for tid in r["_sup"]:
            tj = by_id.get(tid)
            if tj and tj.get("status") in _LIVE:
                _supersede(r, "queued", f"failed attempt superseded -- retry {tid} is {tj.get('status')}")
                break
            if tj and tj.get("status") == "done":
                _supersede(r, "done", f"failed attempt superseded by passing {tid}")
                break
        if r["phase"] != "failed":
            continue
        pm = re.match(r"^(.*)-s\d+$", r["_base"])
        par = by_base.get(pm.group(1)) if pm else None
        if par is not None and par is not r:
            if par["phase"] == "done":
                _supersede(r, "done", f"sub-slice failure superseded: {par['title']} passed")
            elif par["phase"] != "failed" and par["phase"] in ACTIVE_PHASES:
                _supersede(r, "queued", f"sub-slice failure superseded: {par['title']} is {par['status']}")
        if r["phase"] != "failed":
            continue
        for (lab, bun, end) in passed_runs or []:
            if (r["_base"] == lab or re.fullmatch(re.escape(lab) + r"-s\d+", r["_base"])) \
                    and (not bun or bun == key) and r["_t"] and r["_t"] <= end:
                _supersede(r, "done", f"chain {lab} ended exit 0 after this failure")
                break
    # a chain whose driver is alive but has no live queue row this instant (between rounds,
    # preflight, self-check) keeps its bundle RUNNING, never finished/failed.
    for (lab, bun, step) in active_runs or []:
        if bun is not None and bun != key:
            continue
        lin = [r for r in rows if r["_base"] == lab or r["_base"].startswith(lab + "-")]
        if any(r["active"] for r in lin):
            continue
        same = [r for r in lin if r["_base"] == lab]
        if same:    # the chain's own row is between rounds: it is running, not done/failed
            same[0].update(phase="authoring", active=True, attention=False,
                           summary=f"chain advancing: {step}")
            continue
        rows.append({"sid": f"chain:{lab}", "title": lab, "job": True, "status": "running",
                     "phase": "authoring", "detail": "", "summary": f"chain advancing: {step}",
                     "since": None, "elapsed_s": None, "active": True, "attention": False,
                     "history": [], "sub": None, "_base": lab, "_t": 0, "_sup": []})
    through = sum(1 for r in rows if r["phase"] == "done")
    for r in rows:
        for k in ("_base", "_t", "_sup"):
            r.pop(k, None)
    act = [r for r in rows if r["active"]]
    current = act[0]["sid"] if act else None
    return {"key": key, "through": through, "total": len(rows), "current": current,
            "unit": "jobs", "slices": rows}


# ---------------------------------------------------------------------------
# loaders (I/O) -- best-effort, never raise into a poll handler
# ---------------------------------------------------------------------------
def _read_json(p):
    try:
        return json.loads(Path(p).read_text())
    except Exception:
        return None


def load_view(plan, jobs, runs_dir, chain_dir, log_dir, heal_ledger_path,
              preflight_ledger_dir, progress_recs, alive, now=None, _seen=None):
    """The I/O wrapper: reads the run file, sub-plans, chain record, verdict
    sidecars, heal ledger and preflight ledger, then build_view()."""
    now = time.time() if now is None else now
    _seen = (_seen or set()) | {plan}
    if not re.match(r"^[A-Za-z0-9._-]+$", str(plan or "")):
        return None
    state = _read_json(Path(runs_dir) / f"{plan}.json")
    if not isinstance(state, dict) or not (state.get("order") or state.get("slices")):
        return None
    chain = None
    for key in {state.get("bundle"), plan} - {None}:
        c = _read_json(Path(chain_dir) / f"{key}.json")
        if isinstance(c, dict) and c.get("phase") != "ended" and c.get("pid") \
                and alive(c.get("pid")):
            chain = c
            break
    heal = _read_json(heal_ledger_path) or {}
    _vcache = {}

    def result_of(jid):
        if jid not in _vcache:
            _vcache[jid] = _read_json(Path(log_dir) / f"{jid}.gate.json") or {}
        g = _vcache[jid]
        d = _read_json(Path(log_dir) / f"{jid}.done.json") or {}
        return {"verdict": g.get("verdict"), "label": d.get("label") or g.get("label"),
                "status": d.get("status"), "timestamp": d.get("persisted_at")}

    def verdict_of(jid):
        if not jid:
            return None
        if jid not in _vcache:
            _vcache[jid] = _read_json(Path(log_dir) / f"{jid}.gate.json") or {}
        return _vcache[jid].get("verdict")

    def preflight_of(wt):
        k = hashlib.sha256(str(Path(str(wt)).resolve()).encode()).hexdigest()[:16]
        return _read_json(Path(preflight_ledger_dir) / f"{k}.json")

    def sub_view(label):
        if label in _seen:
            return None
        return load_view(label, jobs, runs_dir, chain_dir, log_dir, heal_ledger_path,
                         preflight_ledger_dir, progress_recs, alive, now, _seen)

    view = build_view(plan, state, jobs, now=now, verdict_of=verdict_of,
                      result_of=result_of, progress=progress_recs, chain=chain,
                      heal_ledger=heal, preflight_of=preflight_of, sub_view=sub_view)
    # A HUMAN-cancelled plan (plan_cancel marker) is terminal: its escalated/failed slices
    # are retired, not owed -- no Needs-attention flag (replay-endorse showed "2 failed"
    # a day after its --cancel).
    try:
        import plan_cancel as _pc
        canc = _pc.cancelled(plan, runs_dir=runs_dir)
    except Exception:
        canc = None
    if view and canc:
        view["cancelled"] = True
        for r in view.get("slices") or []:
            r["attention"] = False
    return view


# ---------------------------------------------------------------------------
# durable job HISTORY (the user 2026-10-05: "completed steps of a slice are not shown
# anymore -- only ACTIVE jobs are visible")
# ---------------------------------------------------------------------------
# ROOT CAUSE this closes: ollama-queue.py's prune_finished_jobs drops a clean `done`
# row from queue-state.json on the very next tick (RETAIN_DONE_RECENT=0; only rows
# whose label the slicer's plan index confirms are kept), and a clean-done gate/regate/
# secondop prunes the same way. Every bundle view was built from queue-state rows
# alone, so an `--bundle` job's author/refine rounds, every gate/regate/second opinion
# and every reaped plan-slice run vanished the moment they finished.
#
# Nothing durable was missing, only unread: every launched job leaves a never-pruned
# LIVE_LOG_DIR/<id>-<safe label>.livelog (birth time = launch, last write = finish);
# a finished main job leaves LOG_DIR[/archive]/<id>.done.json (+ <id>.gate.json, the
# final verdict); a gate/regate/secondop job's result lands in its PARENT's .gate.json
# and <parent>-{review,regate,secondop}/report.md. load_history() joins those into
# job-shaped dicts tagged HISTORIC that the view builders take alongside the live rows
# (a live row with the same id always wins). Read-only: nothing here writes anything.
_LIVELOG_NAME_RE = re.compile(r"^([0-9a-f]{6,32})-(.+)\.livelog$")
_REPORT_VERDICT_RE = re.compile(r"^##\s*VERDICT:\s*(\S+)", re.M)
_FILE_CACHE = {}   # (kind, path) -> (mtime_ns, size, value)


def _cached(kind, p, parse):
    try:
        st = os.stat(p)
    except OSError:
        return None
    k = (kind, str(p))
    hit = _FILE_CACHE.get(k)
    if hit and hit[0] == st.st_mtime_ns and hit[1] == st.st_size:
        return hit[2]
    try:
        v = parse(p)
    except Exception:
        v = None
    _FILE_CACHE[k] = (st.st_mtime_ns, st.st_size, v)
    return v


def _json_any(log_dir, name):
    """<log_dir>/<name>, else <log_dir>/archive/<name> (a run-status 'clear' moves the
    sidecars there -- cleared from the worklist, still part of the history)."""
    for d in (Path(log_dir), Path(log_dir) / "archive"):
        v = _cached("json", d / name, lambda p: json.loads(Path(p).read_text()))
        if isinstance(v, dict):
            return v
    return None


def _report_verdict(path):
    """'PASS' / 'CONCERNS' / ... from a review report.md's '## VERDICT:' line, or None."""
    def parse(p):
        with open(p, errors="replace") as fh:
            m = _REPORT_VERDICT_RE.search(fh.read(4096))
        return m.group(1).strip("*_`").upper() if m else None
    return _cached("verdict", path, parse)


def _up(v):
    return str(v).upper() if v else None


def child_result(kind, parent_id, log_dir):
    """(status, result) for a finished gate / regate / second-opinion job, read off its
    PARENT's durable gate.json + report.md (those jobs write no .done.json of their own)."""
    g = _json_any(log_dir, f"{parent_id}.gate.json") or {}
    sub = {"gate": "review", "regate": "regate", "second opinion": "secondop"}.get(kind)
    rv = _report_verdict(Path(log_dir) / f"{parent_id}-{sub}" / "report.md") if sub else None
    if kind == "gate":
        pv = _up(g.get("pregate_verdict") or g.get("verdict"))
        parts = [f"pregate {pv}" if pv else None,
                 f"review {_up(g.get('pregate_review_verdict') or rv)}"
                 if (g.get("pregate_review_verdict") or rv) else None]
    elif kind == "regate":
        auth = str(g.get("gate_authority") or "")
        parts = [f"regate {rv}" if rv else None,
                 f"final {_up(g.get('verdict'))}" if "regate" in auth and g.get("verdict")
                 else None]
    elif kind == "second opinion":
        ag = g.get("second_opinion_agreement")
        parts = [f"2nd opinion {rv}" if rv else None, ag if ag else None]
    else:
        parts = []
    parts = [p for p in parts if p]
    return ("done" if parts else "ended"), (" · ".join(parts) or "no result recorded")


def main_result(job_id, label, log_dir):
    """(status, exit_code, result, done.json) for a finished author/refine/coding/
    esc-review job from its durable sidecars."""
    d = _json_any(log_dir, f"{job_id}.done.json")
    g = _json_any(log_dir, f"{job_id}.gate.json") or {}
    v = _up(g.get("verdict"))
    if not d:
        return "ended", None, (f"gate {v}" if v else
                               "ended -- no result recorded (cancelled or killed?)"), {}
    st, ex = d.get("status") or "done", d.get("exit_code")
    if job_kind({"label": label})[0] == "coding" and v:
        res = f"gate {v}"
    else:
        res = f"{st} exit={ex}" + (f" · gate {v}" if v else "")
    return st, ex, res, d


def load_history(live_dir, log_dir, since=None):
    """Every job that ever LAUNCHED (one record per LIVE_LOG_DIR livelog), newest first,
    as job-shaped dicts tagged HISTORIC: {id, label, status, exit_code, launched_at,
    finished_at, active_s, bundle, cwd, result}. `since` (epoch) drops jobs whose last
    write is older. Best-effort: an unreadable dir is an empty history, never a raise."""
    best = {}
    try:
        it = list(os.scandir(live_dir))
    except OSError:
        return []
    for e in it:
        m = _LIVELOG_NAME_RE.match(e.name)
        if not m:
            continue
        try:
            st = e.stat()
        except OSError:
            continue
        if since is not None and st.st_mtime < since:
            continue
        jid = m.group(1)
        if jid in best and best[jid][0].st_mtime >= st.st_mtime:
            continue
        best[jid] = (st, m.group(2))
    out = []
    for jid, (st, fname_label) in best.items():
        start = getattr(st, "st_birthtime", None)
        end = st.st_mtime
        if not start or start > end:
            start = end   # no birth time (Linux) / a copied file: finish is the best anchor
        rec = {"id": jid, "label": fname_label, HISTORIC: True,
               "launched_at": start, "finished_at": end,
               "active_s": round(max(0.0, end - start), 1) if start else None}
        pm = _CHILD_RE.match(fname_label)
        kind = job_kind({"label": fname_label})[0]
        if pm and kind in ("gate", "regate", "second opinion"):
            rec["status"], rec["result"] = child_result(kind, pm.group(1), log_dir)
            rec["exit_code"] = None
        else:
            s_, ex, res, d = main_result(jid, fname_label, log_dir)
            rec.update(status=s_, exit_code=ex, result=res)
            for k in ("label", "bundle", "cwd", "task_kind", "model"):
                if d.get(k):
                    rec[k] = d[k]
        out.append(rec)
    out.sort(key=lambda r: -(r.get("finished_at") or 0))
    return out


def merge_history(live_jobs, history):
    """live rows + the HISTORIC records whose id is not live (a live row always wins)."""
    ids = {j.get("id") for j in live_jobs or []}
    return list(live_jobs or []) + [h for h in history or [] if h.get("id") not in ids]


def history_for(key, history):
    """The HISTORIC records that can belong to slice plan `key`: main jobs whose label,
    worktree or bundle tag names it, plus every gate/regate/secondop/esc-review(job form)
    child of those. A cheap PRE-filter only -- job_sid still decides the exact slice
    (so 'chat-fixes' pulling a 'chat-fixes2-...' candidate maps it to no slice)."""
    key = str(key or "")
    if not key:
        return []
    mains = [h for h in history or [] if child_parent_id(h) is None and (
        key in str(h.get("label") or "") or key in str(h.get("cwd") or "")
        or h.get("bundle") == key)]
    ids = {h.get("id") for h in mains}
    return mains + [h for h in history or [] if child_parent_id(h) in ids]
