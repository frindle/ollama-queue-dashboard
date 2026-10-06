#!/usr/bin/env python3
"""An ESCALATED slice must read as a stopped chain that needs a human: attention phase, a detail
that says so, and how much author budget it burned (2026-10-06). Revert: bundle_view.py.bak-
20261006T182500Z-escdetail must FAIL (set BV_SRC)."""
import importlib.util, os, sys
from importlib.machinery import SourceFileLoader
from pathlib import Path
src = os.environ.get("BV_SRC") or str(Path(__file__).resolve().parent.parent / "src" / "bundle_view.py")
ld = SourceFileLoader("bv", src); bv = importlib.util.module_from_spec(importlib.util.spec_from_loader("bv", ld)); ld.exec_module(bv)
st = {"label": "p", "order": ["s0", "s1"], "slices": {
    "s0": {"status": "done", "title": "a"},
    "s1": {"status": "escalated", "title": "b", "author_attempts": 5, "author_job_ids": list("abcdefg"),
           "escalation_reason": "authoring has burned 8 author/refine queue jobs for this slice"}}}
v = bv.build_view("p", st, [])
s1 = v["slices"][1]
fails = []
def ok(n, c):
    print(("ok   - " if c else "FAIL - ") + n)
    if not c: fails.append(n)
ok("escalated slice is attention", s1["phase"] == "escalated" and s1["attention"] is True)
ok("detail says it needs a human", "needs a human" in s1["detail"])
ok("detail carries the reason", "burned 8 author/refine queue jobs" in s1["detail"])
ok("detail carries the budget use", "5/5 attempts" in s1["detail"] and "7 jobs" in s1["detail"])
ok("bundle outcome is failed (Needs attention)", bv.bundle_outcome(v)[0] == "failed")
print("ALL PASSED" if not fails else "FAILED")
sys.exit(1 if fails else 0)
