#!/usr/bin/env python3
"""Regression test: FINISHED runs stay visible on the dashboard (Penn 2026-10-05).

Bug: completed steps of a slice/bundle vanished -- only ACTIVE jobs showed. The queue
prunes a clean `done` row from queue-state.json on the next tick (RETAIN_DONE_RECENT=0)
and every bundle view was built from those rows alone, so e.g. bundle
resell-bfmr-link-feedback showed only its running refine r2: its author run, refine r1
(1db468e4253c) and that round's gate / regate (f9750cd80a44) / second opinion were gone.

Asserts the PROPERTY through the real entry points (/api/bundle-views and
/api/bundle-history builders), from fixture livelogs + sidecars in a temp dir:
  * a live bundle's slice history carries its finished author / refine / gate / regate /
    second-opinion runs, each with its durable result, alongside the live row;
  * the author/refine/coding jobs of one feature are ONE pseudo-slice, not three;
  * a slicer-plan slice whose queue rows are all pruned still lists its author, coding
    and gate runs plus an "accepted / landed" step;
  * a bundle with NO queue row left is served by /api/bundle-history, newest activity
    first, bounded by age (days) and page size (limit / has_more), and a bundle that
    still has a queue row is NOT duplicated there;
  * the page fetches /api/bundle-history and offers show-all / show-more controls.
Run: python3 test-bundle-history.py [--revert-check]
  --revert-check runs the same assertions against the pre-fix .bak files and expects
  them to FAIL (proves the test bites).
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
HERE = SRC_DIR
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


def touch(path, text, start, end):
    path.write_text(text)
    os.utime(path, (start, start))   # macOS: an mtime before the birth time moves it
    os.utime(path, (end, end))


def fixture(root, now):
    live = root / "livelogs"
    logs = root / "logs"
    runs = root / "runs"
    for d in (live, logs, logs / "archive", runs, root / "chain"):
        d.mkdir(parents=True)

    def job(jid, label, end, done=None, gate=None, archive=False):
        touch(live / f"{jid}-{label}.livelog", "log\n", end - 60, end)
        d = logs / "archive" if archive else logs
        if done is not None:
            (d / f"{jid}.done.json").write_text(json.dumps(dict({"id": jid, "label": label}, **done)))
        if gate is not None:
            (d / f"{jid}.gate.json").write_text(json.dumps(gate))

    # -- bundle B (live): author done, refine r1 done (+gate/regate/secondop), r2 running
    B = "fx-bundle-feedback"
    job("aaaa00000001", "auto-author-fx-feature", now - 3000,
        {"status": "done", "exit_code": 0, "bundle": B})
    job("aaaa00000002", "auto-refine-fx-feature-r1", now - 2000,
        {"status": "done", "exit_code": 0, "bundle": B},
        {"verdict": "pass", "pregate_verdict": "pass", "regate": "done",
         "gate_authority": "studio-27b-regate", "second_opinion_agreement": "agree"},
        archive=True)   # a run-status "clear" moved it: still history
    for sub, v in (("review", "PASS"), ("regate", "PASS"), ("secondop", "PASS")):
        (logs / f"aaaa00000002-{sub}").mkdir()
        (logs / f"aaaa00000002-{sub}" / "report.md").write_text(f"# Review\n\n## VERDICT: {v}\n")
    job("aaaa00000003", "gate-aaaa00000002", now - 1900)
    job("aaaa00000004", "regate-aaaa00000002", now - 1800)
    job("aaaa00000005", "secondop-aaaa00000002", now - 1700)
    touch(live / "aaaa00000006-auto-refine-fx-feature-r2.livelog", "x", now - 100, now - 1)
    # -- slicer plan P (live via a pending s2): s1 fully pruned
    P = "fxplan"
    (runs / f"{P}.json").write_text(json.dumps({
        "label": P, "order": ["s1-a", "s2-b"],
        "slices": {"s1-a": {"status": "done", "title": "a", "job_id": "bbbb00000002",
                            "gate_verdict": "pass"},
                   "s2-b": {"status": "authoring", "title": "b"}}}))
    job("bbbb00000001", f"auto-author-{P}-s1-a", now - 5000,
        {"status": "done", "exit_code": 0, "bundle": P})
    job("bbbb00000002", f"{P}-s1-a", now - 4000,
        {"status": "done", "exit_code": 0, "bundle": P}, {"verdict": "pass"})
    job("bbbb00000003", "gate-bbbb00000002", now - 3900)
    # -- finished bundles (no queue row): F1 newer, F2 older, F3 9 days old
    for i, (k, age) in enumerate((("fx-done-one", 600), ("fx-done-two", 7200),
                                  ("fx-done-old", 9 * 86400))):
        job(f"cccc0000000{i}", f"{k}-job", now - age,
            {"status": "done", "exit_code": 0, "bundle": k}, {"verdict": "pass"})
    state = {"jobs": [
        {"id": "aaaa00000006", "label": "auto-refine-fx-feature-r2", "status": "running",
         "bundle": B, "launched_at": now - 100},
        {"id": "bbbb00000009", "label": f"auto-author-{P}-s2-b", "status": "pending",
         "bundle": P},
    ]}
    return live, logs, runs, root / "chain", state, B, P


def run(api_src):
    api = load_api(api_src)
    root = Path(tempfile.mkdtemp())
    now = time.time()
    live, logs, runs, chain, state, B, P = fixture(root, now)
    api.q.LIVE_LOG_DIR = live          # the durable livelog dir the fix reads
    api.SLICE_RUNS_DIR = runs
    api.q.slice_group_index = lambda *a, **k: {}
    bvs = api._bundle_views(state=state, runs_dir=runs, chain_dir=chain, log_dir=logs,
                            heal_path=root / "heal.json", preflight_dir=root / "pf",
                            progress=[], alive=lambda pid: False, now=now)
    v = bvs["views"].get(B) or {}
    sl = v.get("slices") or []
    check("the live bundle's feature is ONE pseudo-slice (author/refine share a line)",
          len(sl), 1)
    hist = [(h["kind"], h["id"], h["status"]) for s in sl for h in s["history"]]
    ids = [h[1] for h in hist]
    check("finished author + refine r1 runs are in the slice history",
          all(i in ids for i in ("aaaa00000001", "aaaa00000002")), True)
    check("finished gate / regate / second-opinion runs are in the slice history",
          [k for k, i, _ in hist if i in ("aaaa00000003", "aaaa00000004", "aaaa00000005")],
          ["gate", "regate", "second-opinion"])
    check("the running refine r2 is still the live row",
          [(k, st) for k, i, st in hist if i == "aaaa00000006"],
          [("refining (round 2)", "running")])
    res = {h["id"]: (h.get("result"), h.get("live")) for s in sl for h in s["history"]}
    check("each finished child carries its durable verdict and is read-only",
          (res.get("aaaa00000004"), res.get("aaaa00000005")),
          (("regate PASS · final PASS", False), ("2nd opinion PASS · agree", False)))
    pv = bvs["views"].get(P) or {}
    s1 = next((s for s in pv.get("slices") or [] if s["sid"] == "s1-a"), {})
    check("a pruned plan slice keeps author, coding, gate and landed steps",
          [h["kind"] for h in s1.get("history") or []],
          ["authoring", "coding", "gate", "landed"])
    fb = api._finished_bundle_views(days=3, limit=1, state=state, runs_dir=runs,
                                    log_dir=logs, chain_dir=chain, now=now)
    check("finished bundles: newest first, age-bounded, paged",
          ([x["key"] for x in fb["views"]], fb["total"], fb["has_more"]),
          (["fx-done-one"], 2, True))
    fa = api._finished_bundle_views(days=0, limit=50, state=state, runs_dir=runs,
                                    log_dir=logs, chain_dir=chain, now=now)
    keys = [x["key"] for x in fa["views"]]
    check("show all ages reaches the old bundle; live bundles are not duplicated",
          (keys, B in keys, P in keys),
          (["fx-done-one", "fx-done-two", "fx-done-old"], False, False))
    check("each finished bundle view is complete (through/total + done slice)",
          [(x["through"], x["total"], x["slices"][0]["phase"]) for x in fa["views"]][0],
          (1, 1, "done"))
    html = api.FRONTEND_HTML
    check("the page fetches /api/bundle-history with show-all / show-more controls",
          all(t in html for t in ("/api/bundle-history?days=", "show all ages", "data-fb-more")),
          True)
    check("the landed step has its own stage", "'Accepted / landed'" in html, True)
    shutil.rmtree(root, ignore_errors=True)


def main():
    if "--revert-check" in sys.argv:
        baks = sorted(PIPELINE_BIN.glob("ollama-queue-api.py.bak-*-history"))
        bvb = sorted(PIPELINE_BIN.glob("bundle_view.py.bak-*-history"))
        if not baks or not bvb:
            print("no pre-fix .bak-*-history files to revert to")
            return 2
        d = Path(tempfile.mkdtemp())
        shutil.copy(baks[-1], d / "ollama-queue-api.py")
        shutil.copy(bvb[-1], d / "bundle_view.py")
        shutil.copy(SRC_DIR / "runstatus_retention.py", d / "runstatus_retention.py")
        shutil.copy(PIPELINE_BIN / "dispatch_progress.py", d / "dispatch_progress.py")
        r = subprocess.run([sys.executable, __file__, "--api", str(d / "ollama-queue-api.py")],
                           capture_output=True, text=True)
        print(r.stdout[-3000:])
        bit = r.returncode != 0
        print(("ok  " if bit else "FAIL") + ": revert-check -- the pre-fix code FAILS this test")
        return 0 if bit else 1
    src = HERE / "ollama-queue-api.py"
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
