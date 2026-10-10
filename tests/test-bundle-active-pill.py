#!/usr/bin/env python3
"""Regression: a bundle whose slice VIEW is active (authoring/coding/...) must not roll up to
`pending` from queue rows alone, and a failed row for that in-flight slice must not raise
needs_attention (Penn 2026-10-09, rt-costco-receipt-attach: pill `pending` beside slice
`authoring 2:45` and a yellow "1 failed").  Pure: exercises _apply_view_activity only.
Run: python3 test-bundle-active-pill.py"""
import importlib.util, os, sys
from pathlib import Path

SRC = Path(os.environ.get("DASHBOARD_SRC") or Path(__file__).resolve().parent.parent / "src")
sys.argv = ["x"]
sys.path.insert(0, str(SRC))
spec = importlib.util.spec_from_file_location("api_under_test", str(SRC / "ollama-queue-api.py"))
api = importlib.util.module_from_spec(spec)
spec.loader.exec_module(api)
FAILS = []


def check(name, got, want):
    ok = got == want
    print(("ok  " if ok else "FAIL") + f": {name}" + ("" if ok else f"  (got {got!r}, want {want!r})"))
    if not ok:
        FAILS.append(name)


K = "pl"
def rows():
    return [
        {"id": "a", "group_key": K, "status": "pending", "label": f"auto-author-{K}-s1-x-s1", "plan_status": "pending", "needs_attention": False},
        {"id": "b", "group_key": K, "status": "failed", "label": f"auto-author-{K}-s1-x-s1-r2", "plan_status": "pending", "needs_attention": True},
        {"id": "c", "group_key": K, "status": "planned", "label": f"{K}-s2-y", "plan_status": "pending", "needs_attention": False},
    ]
def view(*sl):
    return {K: {"slices": [dict(sid=s, phase=p, attention=a) for s, p, a in sl]}}

# 1. active authoring slice: pill = authoring, stale failed row no longer alarms
r = api._apply_view_activity(rows(), view(("s1-x", "authoring", False), ("s2-y", "pending", False)))
check("pill reads authoring", {x["plan_status"] for x in r}, {"authoring"})
check("failed retry row not needs_attention", [x["needs_attention"] for x in r], [False, False, False])
check("plan_active_phase stamped", {x["plan_active_phase"] for x in r}, {"authoring"})
# 2. hottest phase wins
r = api._apply_view_activity(rows(), view(("s1-x", "authoring", False), ("s2-y", "coding", False)))
check("coding outranks authoring", r[0]["plan_status"], "coding")
# 3. a running row stays running
rr = rows(); [x.update(plan_status="running") for x in rr]
check("running stays running", api._apply_view_activity(rr, view(("s1-x", "authoring", False)))[0]["plan_status"], "running")
# 4. genuinely failed slice (attention) and nothing active: untouched, alarm kept
r = api._apply_view_activity(rows(), view(("s1-x", "failed", True), ("s2-y", "pending", False)))
check("unresolved failure: pill unchanged", {x["plan_status"] for x in r}, {"pending"})
check("unresolved failure: alarm kept", r[1]["needs_attention"], True)
# 5. failure on a DIFFERENT slice than the active one keeps its alarm
rr = rows(); rr[1]["label"] = f"auto-author-{K}-s3-z-s1"
r = api._apply_view_activity(rr, view(("s1-x", "authoring", False), ("s3-z", "failed", True)))
check("other slice's failure keeps alarm", r[1]["needs_attention"], True)
# 6. queued-only (plain queue wait) and no view: untouched
r = api._apply_view_activity(rows(), view(("s1-x", "queued", False)))
check("queued view phase does not override", {x["plan_status"] for x in r}, {"pending"})
check("no view: untouched", {x["plan_status"] for x in api._apply_view_activity(rows(), {})}, {"pending"})
print("FAILED" if FAILS else "ALL OK")
sys.exit(1 if FAILS else 0)
