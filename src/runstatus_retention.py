#!/usr/bin/env python3
"""Run-status RETENTION: which finished run-status rows may be archived without a
human, and the sweep / explicit-apply drivers that act on that ONE predicate.

Shared by:
  * ollama-queue-api.py  -- the periodic auto-retention sweep (live by default,
    RUNSTATUS_RETENTION=shadow|off to change) and the preview/apply endpoints
    GET /api/runs/retention, POST /api/runs/retention/apply {"ids": [...]}
  * qctl runs-clear      -- dry-run preview (default) / --apply ID... (explicit ids)

ELIGIBLE (archive is recoverable: sidecars move to LOG_DIR/archive/):
  harness         auto-author-* / auto-refine-* rows, once their parent deliverable
                  LANDED (slice done/skipped/superseded, or the bare deliverable row
                  PASSed) or the row was acted on (handoff acted.json)
  cancelled-slice a slice row whose job was cancelled, or whose slice the plan
                  skipped/superseded (and that is not itself a PASS deliverable)
  research        diag-* / research / diagnosis rows (no code gate) once acted on

NEVER: a row awaiting sign-off, a row still live in the queue, or any other row
(an unreviewed deliverable) -- FAIL-SAFE: anything not positively recognised is kept.
"""
import re

LANDED_SLICE = ("done", "skipped", "superseded")
CANCELLED_SLICE = ("skipped", "superseded", "cancelled")
LIVE_STATUSES = frozenset({"pending", "held", "queued", "running", "paused",
                           "scheduled", "planned", "launching"})
_HARNESS_RE = re.compile(r"^auto-(author|refine)-")
_RESEARCH_RE = re.compile(r"(^|[-_])(research|diagnos[ei]s|diagnose|diag)([-_]|$)", re.I)


def deliverable_label(label):
    """PURE. The deliverable (coding/slice) label a harness row serves:
    auto-author-<x>[-c1|-esc] / auto-refine-<x>-r2 -> <x>."""
    s = _HARNESS_RE.sub("", str(label or ""), count=1)
    prev = None
    while prev != s:
        prev = s
        s = re.sub(r"-(r\d+|c\d+|esc)$", "", s)
    return s


def _is_pass(row):
    v = str(row.get("raw_verdict") or row.get("verdict") or "").strip().upper()
    return v.startswith("PASS")


def decide(row, *, slice_status, acted, pass_labels):
    """PURE. {eligible, category, reason} for one annotated run-status row.
    slice_status(label) -> the slice's plan status or None (not a slice);
    acted: set of acted-on job ids; pass_labels: labels of rows whose verdict PASSed."""
    jid = row.get("id")
    lab = str(row.get("label") or "")
    status = str(row.get("status") or "")

    def out(ok, cat, why):
        return {"eligible": ok, "category": cat, "reason": why}

    if row.get("awaiting_signoff"):
        return out(False, "awaiting-signoff", "still owes a required sign-off")
    if status in LIVE_STATUSES:
        return out(False, "live", f"status {status!r} is still live")
    if _HARNESS_RE.match(lab):
        d = deliverable_label(lab)
        st = slice_status(d)
        if st in LANDED_SLICE:
            return out(True, "harness", f"parent slice {d} is {st}")
        # A slice lands only when its PLAN says so (a PASS row can still be waiting on
        # the chain commit); the bare-PASS rule is for non-sliced deliverables.
        if st is None and d in pass_labels:
            return out(True, "harness", f"parent deliverable {d} PASSed")
        if jid in acted:
            return out(True, "harness", "acted on")
        return out(False, "harness", f"parent {d} has not landed (slice {st or '-'})")
    st = slice_status(lab)
    if st is not None:
        if status == "cancelled":
            return out(True, "cancelled-slice", "slice job was cancelled")
        if st in CANCELLED_SLICE and not _is_pass(row):
            return out(True, "cancelled-slice", f"slice is {st} in its plan")
    if _RESEARCH_RE.search(lab) and not row.get("has_gate"):
        if jid in acted:
            return out(True, "research", "research/diagnosis output acted on")
        return out(False, "research", "research/diagnosis output not acted on yet")
    return out(False, "deliverable", "unreviewed deliverable / not a retention category")


def make_slice_status(reverse, read_run_state):
    """slice_status(label) from the slicer's reverse map {full slice label ->
    project} and read_run_state(project) -> run-state dict (or None)."""
    cache = {}

    def lookup(label):
        proj = reverse.get(label)
        if not proj:
            return None
        if proj not in cache:
            cache[proj] = read_run_state(proj) or {}
        sid = label[len(proj) + 1:]
        s = (cache[proj].get("slices") or {}).get(sid)
        if isinstance(s, dict):
            return s.get("status") or "unknown"
        return "unknown" if s is None else str(s)
    return lookup


def evaluate(rows, *, slice_status, acted):
    """[(row, decision)] for every row; scans ALL rows handed in."""
    pass_labels = {r.get("label") for r in rows if _is_pass(r)}
    return [(r, decide(r, slice_status=slice_status, acted=acted,
                       pass_labels=pass_labels)) for r in rows]


def sweep(rows, *, slice_status, acted, archive, apply=True):
    """Archive every eligible row via archive(job_id, reason) when apply; returns
    [{id, label, category, reason, result}] for the eligible rows only."""
    done = []
    for r, d in evaluate(rows, slice_status=slice_status, acted=acted):
        if not d["eligible"]:
            continue
        rec = {"id": r.get("id"), "label": r.get("label"),
               "category": d["category"], "reason": d["reason"]}
        rec["result"] = archive(r.get("id"), d["reason"]) if apply else "dry-run"
        done.append(rec)
    return done


def apply_ids(ids, rows, *, slice_status, acted, archive):
    """Clear ONLY the named ids, each re-checked with the SAME predicate; an id that
    is not eligible (awaiting sign-off, unreviewed deliverable, unknown) is REFUSED."""
    by_id = {r.get("id"): (r, d) for r, d in
             evaluate(rows, slice_status=slice_status, acted=acted)}
    res = []
    for jid in ids:
        if jid not in by_id:
            res.append({"id": jid, "ok": False, "result": "REFUSED: no run-status row"})
            continue
        r, d = by_id[jid]
        if not d["eligible"]:
            res.append({"id": jid, "label": r.get("label"), "ok": False,
                        "result": f"REFUSED ({d['category']}): {d['reason']}"})
            continue
        out = archive(jid, d["reason"])
        ok = bool(out.get("ok")) if isinstance(out, dict) else bool(out)
        res.append({"id": jid, "label": r.get("label"), "ok": ok,
                    "result": (f"cleared ({d['category']}): {d['reason']}" if ok
                               else f"FAILED: {out}")})
    return res


def _self_test():
    ok = True

    def check(name, got, want):
        nonlocal ok
        good = got == want
        ok &= good
        print(("PASS " if good else "FAIL ") + name + ("" if good else f": got {got!r} want {want!r}"))

    status = {"proj-s1-a": "done", "proj-s2-b": "pending", "proj-s3-c": "skipped",
              "proj-s4-d": "pending"}
    ss = lambda lab: status.get(lab)
    rows = [
        {"id": "h1", "label": "auto-author-proj-s1-a", "status": "done"},
        {"id": "h2", "label": "auto-refine-proj-s1-a-r2", "status": "done"},
        {"id": "h3", "label": "auto-author-proj-s2-b-c1", "status": "failed"},
        {"id": "h4", "label": "auto-author-solo", "status": "done"},
        {"id": "d4", "label": "solo", "status": "done", "raw_verdict": "PASS"},
        {"id": "h5", "label": "auto-author-proj-s2-b-esc", "status": "failed"},
        {"id": "sig", "label": "auto-author-proj-s1-a", "status": "done",
         "awaiting_signoff": True},
        {"id": "live", "label": "auto-author-proj-s1-a", "status": "running"},
        {"id": "can", "label": "proj-s4-d", "status": "cancelled"},
        {"id": "skp", "label": "proj-s3-c", "status": "failed", "raw_verdict": "FAIL"},
        {"id": "skpass", "label": "proj-s3-c", "status": "done", "raw_verdict": "PASS"},
        {"id": "deliv", "label": "proj-s2-b", "status": "done", "raw_verdict": "PASS"},
        {"id": "diag1", "label": "diag-thing", "status": "done", "has_gate": False},
        {"id": "diag2", "label": "rt-cc-sync-diagnosis", "status": "done", "has_gate": False},
        {"id": "gdiag", "label": "diag-gated", "status": "done", "has_gate": True},
    ]
    acted = {"h5", "diag2", "gdiag"}
    dec = {r["id"]: d for r, d in evaluate(rows, slice_status=ss, acted=acted)}
    el = {k for k, d in dec.items() if d["eligible"]}
    check("deliverable_label strips stage + continuation decorations",
          [deliverable_label(x) for x in ("auto-refine-p-s1-a-r2", "auto-author-p-s1-a-c1",
                                          "auto-author-p-esc", "auto-refine-x-c1-r3")],
          ["p-s1-a", "p-s1-a", "p", "x"])
    check("harness rows of a landed slice clear", {"h1", "h2"} <= el, True)
    check("harness row of a non-landed slice is kept", "h3" in el, False)
    check("harness row whose bare deliverable PASSed clears", "h4" in el, True)
    check("harness row acted on clears", "h5" in el, True)
    check("awaiting sign-off is NEVER eligible", "sig" in el, False)
    check("a live row is NEVER eligible", "live" in el, False)
    check("cancelled slice row clears", "can" in el, True)
    check("non-pass row of a skipped slice clears", "skp" in el, True)
    check("PASS row of a skipped slice is kept (deliverable)", "skpass" in el, False)
    check("an unreviewed deliverable is kept", "deliv" in el, False)
    check("research row not acted on is kept", "diag1" in el, False)
    check("research row acted on clears", "diag2" in el, True)
    check("a gated diag-* row is a deliverable, kept", "gdiag" in el, False)

    archived = []
    arch = lambda jid, why: (archived.append(jid) or {"ok": True})
    dry = sweep(rows, slice_status=ss, acted=acted, archive=arch, apply=False)
    check("dry-run sweep archives nothing", archived, [])
    check("dry-run sweep lists exactly the eligible set", {d["id"] for d in dry}, el)
    sweep(rows, slice_status=ss, acted=acted, archive=arch, apply=True)
    check("live sweep archives exactly the eligible set", set(archived), el)
    archived.clear()
    res = apply_ids(["h1", "sig", "deliv", "nope"], rows, slice_status=ss, acted=acted,
                    archive=arch)
    check("apply clears only the named eligible id", archived, ["h1"])
    check("apply refuses sign-off / deliverable / unknown ids",
          [r["ok"] for r in res], [True, False, False, False])
    check("apply refusal names the reason",
          res[1]["result"].startswith("REFUSED (awaiting-signoff)"), True)

    st = {"p": {"slices": {"s1-a": {"status": "done"}}}}
    lk = make_slice_status({"p-s1-a": "p"}, st.get)
    check("make_slice_status reads the plan's slice status", lk("p-s1-a"), "done")
    check("make_slice_status: a non-slice label is None", lk("other"), None)
    print("SELF-TEST " + ("PASSED" if ok else "FAILED"))
    return ok


if __name__ == "__main__":
    import sys
    sys.exit(0 if _self_test() else 1)
