#!/usr/bin/env python3
"""Regression test: the Needs-attention tile counted every stalled bundle (527 headline,
Penn 2026-10-06). Only ACTIONABLE stalled bundles (recent <= 7 days, or listed in the
attention file = triage REAL-UNFINISHED / NEEDS-REVIEW) may count; old unclassified ones
are the "stale backlog" (still listed in the Stalled panel, never hidden); superseded
ones are in neither. Runs the REAL _finished_bundle_views on temp fixtures.
Run: python3 test-bundle-actionable.py [--revert-check]
"""
import importlib.util, json, os, shutil, subprocess, sys, tempfile, time
from pathlib import Path

SRC_DIR = Path(os.environ.get("DASHBOARD_SRC") or Path(__file__).resolve().parent.parent / "src")
PIPELINE_BIN = Path(os.environ.get("OLLAMA_PIPELINE_BIN") or Path.home() / "bin")
FAILS = []


def check(name, got, want):
    ok = got == want
    print(("ok  " if ok else "FAIL") + f": {name}" + ("" if ok else f"  (got {got!r}, want {want!r})"))
    if not ok:
        FAILS.append(name)


def run(api_src):
    root = Path(tempfile.mkdtemp())
    os.environ["OLLAMA_SUPERSEDED_FILE"] = str(root / "sup.json")
    os.environ["OLLAMA_ATTENTION_FILE"] = str(root / "attn.json")
    (root / "attn.json").write_text(json.dumps({"fx-old-real": "REAL-UNFINISHED"}))
    (root / "sup.json").write_text(json.dumps({"fx-old-retired": {"by": "x", "reason": "r", "ts": "t"}}))
    sys.argv = [str(api_src)]
    spec = importlib.util.spec_from_file_location("api_under_test", str(api_src))
    api = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(api)
    now = time.time()
    live, logs, runs, chain = root / "live", root / "logs", root / "runs", root / "chain"
    for d in (live, logs, logs / "archive", runs, chain):
        d.mkdir(parents=True)

    def job(jid, label, end, bundle):
        p = live / f"{jid}-{label}.livelog"
        p.write_text("log\n"); os.utime(p, (end - 60, end))
        (logs / f"{jid}.done.json").write_text(json.dumps(
            {"id": jid, "label": label, "status": "failed", "exit_code": 1, "bundle": bundle}))
    day = 86400
    job("aaaa00000001", "a", now - 1 * day, "fx-recent")
    job("aaaa00000002", "b", now - 20 * day, "fx-old-unclassified")
    job("aaaa00000003", "c", now - 20 * day, "fx-old-real")
    job("aaaa00000004", "d", now - 20 * day, "fx-old-retired")
    api.q.LIVE_LOG_DIR = live
    api.SLICE_RUNS_DIR = runs
    api.q.slice_group_index = lambda *a, **k: {}
    fb = api._finished_bundle_views(days=3, limit=50, state={"jobs": []}, runs_dir=runs,
                                    log_dir=logs, chain_dir=chain, now=now)
    st = {v["key"]: v for v in fb["stalled"]}
    check("every stalled bundle stays listed (nothing hidden), superseded excluded",
          set(st), {"fx-recent", "fx-old-unclassified", "fx-old-real"})
    check("actionable = recent or attention-listed", 
          {k: v.get("actionable") for k, v in st.items()},
          {"fx-recent": True, "fx-old-unclassified": False, "fx-old-real": True})
    check("counts: actionable 2, stale backlog 1, superseded 1",
          (fb.get("stalled_actionable"), fb.get("stale_total"), fb.get("superseded_total")), (2, 1, 1))
    html = api.FRONTEND_HTML
    check("tile counts actionable only and links the stale backlog",
          all(t in html for t in ("stale backlog:", "filter(v => v.actionable !== false).length",
                                  "attnStats.stale")), True)
    shutil.rmtree(root, ignore_errors=True)


def main():
    if "--revert-check" in sys.argv:
        baks = sorted(PIPELINE_BIN.glob("ollama-queue-api.py.bak-*-actionable"))
        bvb = sorted(PIPELINE_BIN.glob("bundle_view.py.bak-*-actionable"))
        if not baks or not bvb:
            print("no pre-fix .bak-*-actionable files"); return 2
        d = Path(tempfile.mkdtemp())
        shutil.copy(baks[0], d / "ollama-queue-api.py"); shutil.copy(bvb[0], d / "bundle_view.py")
        shutil.copy(SRC_DIR / "runstatus_retention.py", d / "runstatus_retention.py")
        shutil.copy(PIPELINE_BIN / "dispatch_progress.py", d / "dispatch_progress.py")
        r = subprocess.run([sys.executable, __file__, "--api", str(d / "ollama-queue-api.py")],
                           capture_output=True, text=True)
        print(r.stdout[-1800:])
        print(("ok  " if r.returncode else "FAIL") + ": revert-check -- the pre-fix code FAILS this test")
        return 0 if r.returncode else 1
    src = SRC_DIR / "ollama-queue-api.py"
    if "--api" in sys.argv:
        src = Path(sys.argv[sys.argv.index("--api") + 1])
    try:
        run(src)
    except Exception as e:
        import traceback; traceback.print_exc(); FAILS.append(f"crashed: {e}")
    print(f"\n{len(FAILS)} failure(s)" if FAILS else "\nALL PASSED")
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
