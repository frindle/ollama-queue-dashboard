#!/usr/bin/env python3
"""Dashboard B (2026-09-27): per-slice live phase, off-GPU activity, wait reasons,
livelog fallback.

Penn's complaints this answers: the bundle header read "1/5" with s2 long green
(the slicer run file lags its driver's poll); a committed bundle between GPU jobs
looked hung while preflight/verify-relevance ran on the CPU; held/pending rows gave
no reason; a reaped job's log was unreachable from the dashboard.

  A. bundle_view.build_view -- every slice in plan order, live phases, stale-counter
     truth (gate PASS => done before the slicer records it), attention, current;
  B. dispatch_progress -- write/read/clear, dead pid and stale records ignored;
  C. verify-relevance's measure_applied really writes mutant progress, then clears;
  D. the API: /api/bundle-views payload (views + activity), wait_reason, livelog
     LOG_DIR fallback, and the front-end actually consumes them.

Sandboxed: temp HOME, temp progress dir, temp log/run dirs. Nothing live is read.
Revert checks: set DASHB_REVERT=stale|wait|livelog to prove the matching asserts bite.
Run: python3 test-dashboard-b.py
"""
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

# Layout: dashboard code in <repo>/src, shared pipeline (ollama-queue.py, dispatch_progress.py,
# handoff-emit.py, .bak files) in ~/bin. Override with DASHBOARD_SRC / OLLAMA_PIPELINE_BIN.
SRC_DIR = Path(os.environ.get("DASHBOARD_SRC") or Path(__file__).resolve().parent.parent / "src")
PIPELINE_BIN = Path(os.environ.get("OLLAMA_PIPELINE_BIN") or Path.home() / "bin")
BIN = SRC_DIR
HOME = tempfile.mkdtemp(prefix="dashb-home-")
os.environ["HOME"] = HOME
os.environ["OLLAMA_DISPATCH_PROGRESS_DIR"] = os.path.join(HOME, "progress")
REVERT = os.environ.get("DASHB_REVERT", "")
failures = []


def ok(name, got, want=True):
    good = got == want
    print(f"  {'ok  ' if good else 'FAIL'} {name}" + ("" if good else f": got {got!r} want {want!r}"))
    if not good:
        failures.append(name)


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


# the API imports ~/bin/ollama-queue.py: give the temp HOME a copy of bin/
shutil.copytree(PIPELINE_BIN, Path(HOME) / "bin", symlinks=True, ignore=shutil.ignore_patterns(
    "ollama-queue-logs", "ollama-queue-livelogs", "__pycache__", "*.log"))
for _f in SRC_DIR.glob("*.py"):           # overlay the repo dashboard modules
    _d = Path(HOME) / "bin" / _f.name
    if _d.is_symlink() or _d.exists():
        _d.unlink()
    shutil.copy(_f, _d)
sys.path.insert(0, str(Path(HOME) / "bin"))

api_src = (BIN / "ollama-queue-api.py").read_text()
bv_src = (BIN / "bundle_view.py").read_text()
if REVERT == "wait":
    api_src = api_src.replace('r["wait_reason"] = f"waiting: bundle {active} in progress"',
                              'pass')
if REVERT == "livelog":
    api_src = api_src.replace("    if not re.match(r\"^[0-9a-f]{6,32}$\", str(job_id or \"\")):\n"
                              "        return None\n", "    return None\n")
if REVERT == "stale":
    bv_src = bv_src.replace('return "done", "gate pass (slicer will record it on its next poll)", None',
                            'pass')
(Path(HOME) / "bin" / "ollama-queue-api.py").write_text(api_src)
(Path(HOME) / "bin" / "bundle_view.py").write_text(bv_src)
bv = load("bundle_view", Path(HOME) / "bin" / "bundle_view.py")
sys.modules["bundle_view"] = bv
dp = load("dispatch_progress", Path(HOME) / "bin" / "dispatch_progress.py")
sys.modules["dispatch_progress"] = dp

NOW = 1_790_000_000.0
PLAN = "bfmr"
ORDER = ["s1-a", "s2-b", "s3-c", "s4-d", "s5-e"]


def state_file(slices):
    return {"label": PLAN, "repo": "/r", "order": ORDER,
            "slices": {sid: dict(status=st, **extra) for sid, (st, extra) in slices.items()}}


def main():
    print("A. bundle_view: one line per slice, from live truth")
    st = state_file({
        "s1-a": ("done", {}),
        "s2-b": ("enqueued", {"job_id": "aaa111aaa111"}),       # gate already PASS
        "s3-c": ("enqueued", {"job_id": "bbb222bbb222"}),       # coding running
        "s4-d": ("escalated", {"escalation_reason": "relevance NO-GO"}),
        "s5-e": ("pending", {"worktree": "/wt/s5"}),
    })
    jobs = [{"id": "bbb222bbb222", "label": f"{PLAN}-s3-c", "status": "running",
             "launched_at": NOW - 125}]
    verdicts = {"aaa111aaa111": "pass"}
    prog = [{"tool": "verify-relevance", "wt": "/wt/s5", "stage": "verify-relevance",
             "detail": "mutants", "done": 18, "total": 40, "started_at": NOW - 60,
             "updated_at": NOW - 1, "pid": 1}]
    v = bv.build_view(PLAN, st, jobs, now=NOW, verdict_of=verdicts.get, progress=prog)
    ph = {s["sid"]: s["phase"] for s in v["slices"]}
    ok("every slice in plan order", [s["sid"] for s in v["slices"]], ORDER)
    ok("STALE COUNTER: s2 enqueued + gate PASS reads done", ph["s2-b"], "done")
    ok("header N/M from live truth (s1 + s2 through of 5)", (v["through"], v["total"]), (2, 5))
    ok("s3 with a running coding job reads coding", ph["s3-c"], "coding")
    ok("s3 elapsed from launch", v["slices"][2]["elapsed_s"], 125.0)
    ok("s4 escalated is flagged for attention",
       (ph["s4-d"], v["slices"][3]["attention"]), ("escalated", True))
    ok("s5 off-GPU verify-relevance shows as preflight with progress",
       (ph["s5-e"], v["slices"][4]["detail"]), ("preflight", "verify-relevance 18/40 mutants"))
    ok("current = the hottest live slice (coding beats preflight)", v["current"], "s3-c")
    ok("history carries the live row by id", v["slices"][2]["history"][0]["id"], "bbb222bbb222")
    v2 = bv.build_view(PLAN, st, [], now=NOW, verdict_of=lambda _i: None)
    ok("no verdict yet: s2 reads gate (verdict pending), not done",
       {s["sid"]: s["phase"] for s in v2["slices"]}["s2-b"], "gate")

    print("B. dispatch_progress records")
    base = Path(HOME) / "progress"
    dp.write("/wt/x", "preflight", "preflight", "baseline", base=base)
    recs = dp.read_all(base=base)
    ok("a live record is read back", [r["tool"] for r in recs], ["preflight"])
    ok("a dead writer's record is ignored",
       dp.read_all(base=base, alive=lambda _p: False), [])
    ok("a stale record is ignored", dp.read_all(base=base, now=time.time() + 3600), [])
    ok("describe()", dp.describe({"stage": "verify-relevance", "done": 3, "total": 9,
                                  "detail": "mutants"}), "verify-relevance 3/9 mutants")
    dp.clear("/wt/x", "preflight", base=base)
    ok("clear() removes this process's record", dp.read_all(base=base), [])

    print("C. verify-relevance writes mutant progress while it runs")
    tvr = load("tvr", PIPELINE_BIN / "test-verify-relevance.py")
    vr = tvr.vr
    root = tvr.build(Path(HOME) / "vr-tree")
    subprocess.run(["git", "apply", "fix.patch"], cwd=root, check=True)
    diff = subprocess.run(["git", "diff"], cwd=root, capture_output=True, text=True).stdout
    seen = []
    real_write = dp.write
    dp.write = lambda wt, tool, stage, detail="", done=None, total=None, base=None: \
        seen.append((tool, done, total))
    try:
        rec = vr.measure_applied(root, "bash verify.sh", diff, max_mutants=6)
    finally:
        dp.write = real_write
    ok("progress starts at 0 and counts every mutant up to the total",
       bool(seen) and seen[0][1] == 0 and seen[-1][1] == seen[-1][2] and seen[-1][2] > 0, True)
    ok("the relevance verdict itself is unchanged by the hook",
       rec.get("verdict"), "relevant")

    print("D. API: /api/bundle-views, wait reasons, livelog fallback, front-end")
    api = load("api", Path(HOME) / "bin" / "ollama-queue-api.py")
    rd, cd, ld = (Path(HOME) / n for n in ("runs", "chain", "logs"))
    for d in (rd, cd, ld):
        d.mkdir()
    (rd / f"{PLAN}.json").write_text(json.dumps(st))
    (ld / "aaa111aaa111.gate.json").write_text(json.dumps({"verdict": "pass"}))
    qstate = {"jobs": jobs + [{"id": "ccc333ccc333", "label": "other-s1-x",
                               "status": "pending"}],
              "_bundle_commit": {"key": PLAN}}
    pay = api._bundle_views(qstate, runs_dir=rd, chain_dir=cd, log_dir=ld,
                            heal_path=Path(HOME) / "none.json",
                            preflight_dir=Path(HOME) / "none", progress=prog,
                            alive=lambda _p: True, now=NOW)
    ok("a view for the plan in the queue", sorted(pay["views"]), [PLAN])
    ok("the view's N/M is the live one", (pay["views"][PLAN]["through"],
                                           pay["views"][PLAN]["total"]), (2, 5))
    ok("activity lists the GPU job and the CPU step",
       [a["kind"] for a in pay["activity"]], ["gpu", "cpu"])
    ok("activity names the active bundle", pay["active"], PLAN)
    rows = api._annotate_wait_reason(
        [{"id": "r1", "group_key": PLAN, "status": "pending"},
         {"id": "r2", "group_key": "other", "status": "pending"},
         {"id": "r3", "group_key": "other", "status": "held", "hold_reason": "pending gate x"},
         {"id": "r4", "group_key": "other", "status": "running"}],
        {"_bundle_commit": {"key": PLAN}}, now=NOW)
    wr = {r["id"]: r.get("wait_reason") for r in rows}
    ok("a row in the active bundle is held only on the running job, not a bundle",
       wr["r1"], "held on running job r4 (None)")
    ok("a waiting row outside it says which bundle it waits on",
       wr["r2"], f"held on bundle {PLAN} (between steps)")
    ok("a held row keeps its own hold reason", wr["r3"], "held on hold: pending gate x")
    ok("a running row is never annotated", wr["r4"], None)
    ll = Path(HOME) / "ll"
    ll.mkdir()
    (ld / "ddd444ddd444-some-label.log").write_text("transcript")
    ok("livelog falls back to LOG_DIR/<id>-*.log for a reaped job",
       api._livelog_path(None, "ddd444ddd444", live_dir=ll, log_dir=ld),
       str(ld / "ddd444ddd444-some-label.log"))
    ok("a non-hex id is never globbed",
       api._livelog_path(None, "../../etc", live_dir=ll, log_dir=ld), None)
    fe = api.FRONTEND_HTML
    ok("front-end fetches /api/bundle-views", "fetch('/api/bundle-views')" in fe)
    ok("front-end renders one line per slice", "renderSliceLines(g, view)" in fe)
    ok("slice toggles are remembered", "slice: sliceExpanded" in fe)
    ok("only failed/escalated slices open by default",
       "sliceOpenByDefault(sl)" in fe and "sl.sid === view.current" not in fe)
    ok("earlier attempts collapse to one summary line, never open by default",
       "attemptSummary(n, ga.by[n])" in fe and "(ak in sliceExpanded) ? sliceExpanded[ak] : false" in fe)
    ok("earlier refine rounds fold behind one line", "foldRounds(entries)" in fe)
    ok("slice caret is the only disclosure arrow (data-open mirrors the real state)",
       "str.dataset.open = open ? '1' : '0'" in fe and "slice-dot" in fe
       and ": sl.active ? '&#9654;'" not in fe)
    ok("a failed slice badges the bundle header (slice truth, not queue rows)",
       "slices failed" in fe or "failed</span> `" in fe)
    ok("live-now tags each job with its own bundle", "a.group_key" in fe
       and "bundle in progress" not in fe)
    ok("stale bundles sort below live ones", "planRank" in fe)
    ok("wait_reason is rendered", "j.wait_reason" in fe)
    ok("live-activity row is rendered", "live now:" in fe)
    ok("do_GET routes /api/bundle-views", 'self.path == "/api/bundle-views"' in api_src)

    print(f"\n{'FAILED: ' + ', '.join(failures) if failures else 'ALL PASSED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
