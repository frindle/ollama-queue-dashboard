#!/usr/bin/env python3
"""The dashboard slice row must carry the ONE attempt counter (failure_ledger.slice_counter)
and the page JS must render it: 'author jobs W/B (life L/C)', last cause, 'same cause as'.
(2026-10-06 rt-egift-link-s1-s4: dashboard said attempt 14, slicer said 3/8 jobs.)
--revert-check: BV_SRC / API_SRC point at mutated copies."""
import importlib.util, os, sys
from pathlib import Path
HERE = Path(__file__).resolve().parent.parent
BV = Path(os.environ.get("BV_SRC") or HERE / "src" / "bundle_view.py")
API = Path(os.environ.get("API_SRC") or HERE / "src" / "ollama-queue-api.py")
spec = importlib.util.spec_from_file_location("bv_t", BV); bv = importlib.util.module_from_spec(spec); spec.loader.exec_module(bv)
FAILS = []
def chk(n, ok):
    print(("ok  " if ok else "FAIL") + " - " + n)
    if not ok: FAILS.append(n)
ctr = {"jobs_window": 3, "job_budget": 8, "jobs_lifetime": 12, "lifetime_cap": 24, "attempts": 3, "attempt_cap": 5,
       "last_signature": "read-loop:auto-harness-check.py", "same_as": "bf98ded8188b", "at_cap": False}
state = {"order": ["s4"], "slices": {"s4": {"status": "pending", "title": "t"}}}
v = bv.build_view("plan", state, [], counter_of=lambda p, s, sl: ctr if s == "s4" else None)
chk("slice row carries the counter", v["slices"][0]["counter"] == ctr)
v2 = bv.build_view("plan", state, [], counter_of=lambda p, s, sl: None)
chk("no counter -> None, view still builds", v2["slices"][0]["counter"] is None)
js = API.read_text()
chk("page JS renders window/budget + lifetime/cap", "'author jobs ' + ctr.jobs_window + '/' + ctr.job_budget" in js and "ctr.lifetime_cap" in js)
chk("page JS renders last cause + same cause as", "ctr.last_signature" in js and "same cause as" in js)
sys.exit(1 if FAILS else 0)
