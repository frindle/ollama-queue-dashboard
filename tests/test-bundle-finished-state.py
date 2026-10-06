#!/usr/bin/env python3
"""Regression test: "finished" means EVERY slice done, not "nothing live" (Penn 2026-10-06:
"considered finished but have failed flags").

Bug: /api/bundle-history listed every bundle with no queue row left as FINISHED, so
bundles whose final slice FAILED / ended (attention=true, through < total, e.g.
bfmr-auth 2/3) sat in the Finished list, and aged out of it after 3 days.

Runs the REAL _finished_bundle_views on a stubbed state + fixture livelogs/sidecars in a
temp dir (never the live queue) and asserts:
  * a clean bundle (all slices done) is finished; a failed job bundle, an ENDED job
    bundle, a slicer plan 2/3 with a failed slice and a plan that still owes slices
    with nothing live are all STALLED (separate list, outcome failed/incomplete);
  * no stalled bundle is in `views`, and `total` counts finished only;
  * a stalled bundle OLDER than the age window is still reported (never aged out);
  * the page has the Stalled / failed panel and counts stalled in the status header.
Run: python3 test-bundle-finished-state.py [--revert-check]
  --revert-check runs the same assertions against the pre-fix ~/bin/*-finishedstate
  backups and expects them to FAIL (proves the test bites).
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


def load_api(src):
    sys.argv = [str(src)]
    spec = importlib.util.spec_from_file_location("api_under_test", str(src))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def touch(path, start, end):
    path.write_text("log\n")
    os.utime(path, (start, start))
    os.utime(path, (end, end))


def run(api_src):
    api = load_api(api_src)
    root = Path(tempfile.mkdtemp())
    now = time.time()
    live, logs, runs, chain = root / "live", root / "logs", root / "runs", root / "chain"
    for d in (live, logs, logs / "archive", runs, chain):
        d.mkdir(parents=True)

    def job(jid, label, end, bundle, status="done", exit_code=0, verdict="pass"):
        touch(live / f"{jid}-{label}.livelog", end - 60, end)
        (logs / f"{jid}.done.json").write_text(json.dumps(
            {"id": jid, "label": label, "status": status, "exit_code": exit_code,
             "bundle": bundle}))
        if verdict:
            (logs / f"{jid}.gate.json").write_text(json.dumps({"verdict": verdict}))

    job("aaaa00000001", "ok-job", now - 600, "fx-ok")
    job("aaaa00000002", "bad-job", now - 700, "fx-failed-job", "failed", 1, None)
    job("aaaa00000003", "ended-job", now - 800, "fx-ended-job", "ended", 1, None)
    job("aaaa00000004", "ancient-bad-job", now - 20 * 86400, "fx-failed-ancient", "failed", 1, None)
    # slicer plan: s1 done, s2 FAILED, s3 never run (2/3 -> stalled on the failure)
    (runs / "fx-plan-failed.json").write_text(json.dumps({
        "label": "fx-plan-failed", "order": ["s1", "s2", "s3"],
        "slices": {"s1": {"status": "done", "title": "a"},
                   "s2": {"status": "failed", "title": "b"},
                   "s3": {"status": "pending", "title": "c"}}}))
    job("bbbb00000001", "fx-plan-failed-s1", now - 900, "fx-plan-failed")
    # slicer plan: s1 done, s2 pending, nothing failed, nothing live (incomplete)
    (runs / "fx-plan-owed.json").write_text(json.dumps({
        "label": "fx-plan-owed", "order": ["s1", "s2"],
        "slices": {"s1": {"status": "done", "title": "a"},
                   "s2": {"status": "pending", "title": "b"}}}))
    job("cccc00000001", "fx-plan-owed-s1", now - 1000, "fx-plan-owed")
    api.q.LIVE_LOG_DIR = live
    api.SLICE_RUNS_DIR = runs
    api.q.slice_group_index = lambda *a, **k: {}
    kw = dict(state={"jobs": []}, runs_dir=runs, log_dir=logs, chain_dir=chain, now=now)
    fb = api._finished_bundle_views(days=3, limit=50, **kw)
    fin = [x["key"] for x in fb["views"]]
    stalled = {x["key"]: x for x in fb.get("stalled", [])}
    want_stalled = {"fx-failed-job", "fx-ended-job", "fx-failed-ancient", "fx-plan-failed",
                    "fx-plan-owed"}
    check("only the clean bundle is FINISHED", fin, ["fx-ok"])
    check("no finished bundle is incomplete or carries a failed slice",
          [x["key"] for x in fb["views"]
           if x["through"] < x["total"] or any(s["attention"] for s in x["slices"])], [])
    check("failed / ended / incomplete bundles are STALLED, incl. one older than the window",
          set(stalled), want_stalled)
    check("`total` counts finished only; stalled_total counts the rest",
          (fb["total"], fb.get("stalled_total")), (1, len(want_stalled)))
    check("outcomes: failed vs incomplete",
          {k: v.get("outcome") for k, v in stalled.items()},
          {"fx-failed-job": "failed", "fx-ended-job": "failed", "fx-failed-ancient": "failed",
           "fx-plan-failed": "failed", "fx-plan-owed": "incomplete"})
    check("the failed plan reads 1/3 with one failed slice",
          (stalled.get("fx-plan-failed", {}).get("through"),
           stalled.get("fx-plan-failed", {}).get("total"),
           stalled.get("fx-plan-failed", {}).get("failed_slices")), (1, 3, 1))
    fa = api._finished_bundle_views(days=0, limit=50, **kw)
    check("show-all-ages: finished list still excludes every stalled bundle",
          [x["key"] for x in fa["views"]], ["fx-ok"])
    html = api.FRONTEND_HTML
    check("page has a Stalled / failed panel and counts stalled in the header",
          all(t in html for t in ("stalledPanel", "Stalled / failed bundles",
                                  "attnStats.stalled", "renderStalledBundles")), True)
    shutil.rmtree(root, ignore_errors=True)


def main():
    if "--revert-check" in sys.argv:
        baks = sorted(PIPELINE_BIN.glob("ollama-queue-api.py.bak-*-finishedstate"))
        bvb = sorted(PIPELINE_BIN.glob("bundle_view.py.bak-*-finishedstate"))
        if not baks or not bvb:
            print("no pre-fix .bak-*-finishedstate files to revert to")
            return 2
        d = Path(tempfile.mkdtemp())
        shutil.copy(baks[0], d / "ollama-queue-api.py")
        shutil.copy(bvb[0], d / "bundle_view.py")
        shutil.copy(SRC_DIR / "runstatus_retention.py", d / "runstatus_retention.py")
        shutil.copy(PIPELINE_BIN / "dispatch_progress.py", d / "dispatch_progress.py")
        r = subprocess.run([sys.executable, __file__, "--api", str(d / "ollama-queue-api.py")],
                           capture_output=True, text=True)
        print(r.stdout[-3000:])
        bit = r.returncode != 0
        print(("ok  " if bit else "FAIL") + ": revert-check -- the pre-fix code FAILS this test")
        return 0 if bit else 1
    src = SRC_DIR / "ollama-queue-api.py"
    if "--api" in sys.argv:
        src = Path(sys.argv[sys.argv.index("--api") + 1])
    try:
        run(src)
    except Exception as e:
        import traceback
        traceback.print_exc()
        FAILS.append(f"crashed: {e}")
    print(f"\n{len(FAILS)} failure(s)" if FAILS else "\nALL PASSED")
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
