#!/usr/bin/env python3
"""Regression test: a failed bundle/slice that a LATER bundle superseded must not read
"failed" / count as Stalled forever (bfmr-auth first slice vs -impl2, Penn 2026-10-06).

`qctl supersede BUNDLE --by TARGET --reason R [--slice SID]` records a marker; the REAL
_finished_bundle_views (stubbed state + temp fixtures, never the live queue) must then
report the bundle outcome "superseded", leave it out of `stalled`/`stalled_total`, and
a marked failed SLICE must stop counting as failed. Unmarked failures stay stalled; a
marker needs --by and --reason; --undo restores the stalled reading.
Run: python3 test-bundle-superseded.py [--revert-check]  (revert-check: the pre-fix
~/bin/*-superseded backups must FAIL this test)
"""
import importlib.util, json, os, shutil, subprocess, sys, tempfile, time
from pathlib import Path

SRC_DIR = Path(os.environ.get("DASHBOARD_SRC") or Path(__file__).resolve().parent.parent / "src")
PIPELINE_BIN = Path(os.environ.get("OLLAMA_PIPELINE_BIN") or Path.home() / "bin")
QCTL = PIPELINE_BIN / "qctl"
FAILS = []


def check(name, got, want):
    ok = got == want
    print(("ok  " if ok else "FAIL") + f": {name}" + ("" if ok else f"  (got {got!r}, want {want!r})"))
    if not ok:
        FAILS.append(name)


def qctl(env, *args):
    r = subprocess.run([sys.executable, str(QCTL), *args], capture_output=True, text=True, env=env)
    return r.returncode


def run(api_src):
    root = Path(tempfile.mkdtemp())
    sup_file = root / "superseded.json"
    os.environ["OLLAMA_SUPERSEDED_FILE"] = str(sup_file)
    sys.argv = [str(api_src)]
    spec = importlib.util.spec_from_file_location("api_under_test", str(api_src))
    api = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(api)
    now = time.time()
    live, logs, runs, chain = root / "live", root / "logs", root / "runs", root / "chain"
    for d in (live, logs, logs / "archive", runs, chain):
        d.mkdir(parents=True)

    def job(jid, label, end, bundle, status="done", exit_code=0, verdict="pass"):
        p = live / f"{jid}-{label}.livelog"
        p.write_text("log\n"); os.utime(p, (end - 60, end)); os.utime(p, (end - 60, end))
        (logs / f"{jid}.done.json").write_text(json.dumps(
            {"id": jid, "label": label, "status": status, "exit_code": exit_code, "bundle": bundle}))
        if verdict:
            (logs / f"{jid}.gate.json").write_text(json.dumps({"verdict": verdict}))

    job("aaaa00000001", "ok-job", now - 600, "fx-ok")
    job("aaaa00000002", "bad-job", now - 700, "fx-failed-old", "failed", 1, None)
    job("aaaa00000003", "bad-job2", now - 800, "fx-failed-real", "failed", 1, None)
    (runs / "fx-plan.json").write_text(json.dumps({
        "label": "fx-plan", "order": ["s1", "s2"],
        "slices": {"s1": {"status": "done", "title": "a"}, "s2": {"status": "failed", "title": "b"}}}))
    job("bbbb00000001", "fx-plan-s1", now - 900, "fx-plan")
    api.q.LIVE_LOG_DIR = live
    api.SLICE_RUNS_DIR = runs
    api.q.slice_group_index = lambda *a, **k: {}
    kw = dict(state={"jobs": []}, runs_dir=runs, log_dir=logs, chain_dir=chain, now=now)

    def read():
        fb = api._finished_bundle_views(days=3, limit=50, **kw)
        return fb, {x["key"]: x for x in fb.get("stalled", [])}

    fb, st = read()
    check("before any marker both failed bundles and the failed-slice plan are stalled",
          set(st), {"fx-failed-old", "fx-failed-real", "fx-plan"})
    env = dict(os.environ, QCTL_DISPATCH=str(root / "dispatch"), QCTL_BIN=str(PIPELINE_BIN))
    (root / "dispatch").mkdir()
    check("marker without --reason is refused", qctl(env, "supersede", "fx-failed-old", "--by", "fx-ok") != 0, True)
    check("marker without --by is refused", qctl(env, "supersede", "fx-failed-old", "--reason", "r") != 0, True)
    check("supersede exits 0", qctl(env, "supersede", "fx-failed-old", "--by", "fx-ok", "--reason", "replaced by fx-ok"), 0)
    fb, st = read()
    check("superseded bundle leaves Stalled; unmarked failure stays",
          (set(st), fb.get("stalled_total")), ({"fx-failed-real", "fx-plan"}, 2))
    check("superseded bundle is reported as outcome 'superseded' (not finished)",
          ([x["key"] for x in fb.get("superseded", [])], [x["key"] for x in fb["views"]]),
          (["fx-failed-old"], ["fx-ok"]))
    check("dry-run changes nothing", qctl(env, "supersede", "fx-failed-real", "--by", "x", "--reason", "r", "--dry-run"), 0)
    check("still stalled after dry-run", "fx-failed-real" in read()[1], True)
    check("slice-level marker exits 0", qctl(env, "supersede", "fx-plan", "--slice", "s2", "--by", "fx-ok", "--reason", "retired slice"), 0)
    fb, st = read()
    check("a marked failed slice stops counting: the plan is no longer stalled",
          ("fx-plan" in st, "fx-plan" in [x["key"] for x in fb["views"]]), (False, True))
    check("--undo restores the stalled reading",
          (qctl(env, "supersede", "fx-failed-old", "--undo"), "fx-failed-old" in read()[1]), (0, True))
    check("page shows the superseded chip", "superseded" in api.FRONTEND_HTML and "fb.superseded" in api.FRONTEND_HTML, True)
    shutil.rmtree(root, ignore_errors=True)


def main():
    if "--revert-check" in sys.argv:
        baks = sorted(PIPELINE_BIN.glob("ollama-queue-api.py.bak-*-superseded"))
        bvb = sorted(PIPELINE_BIN.glob("bundle_view.py.bak-*-superseded"))
        if not baks or not bvb:
            print("no pre-fix .bak-*-superseded files"); return 2
        d = Path(tempfile.mkdtemp())
        shutil.copy(baks[0], d / "ollama-queue-api.py"); shutil.copy(bvb[0], d / "bundle_view.py")
        shutil.copy(SRC_DIR / "runstatus_retention.py", d / "runstatus_retention.py")
        shutil.copy(PIPELINE_BIN / "dispatch_progress.py", d / "dispatch_progress.py")
        r = subprocess.run([sys.executable, __file__, "--api", str(d / "ollama-queue-api.py")],
                           capture_output=True, text=True)
        print(r.stdout[-2500:])
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
