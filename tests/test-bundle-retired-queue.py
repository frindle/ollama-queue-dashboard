#!/usr/bin/env python3
"""Regression (Penn 2026-10-08 dashboard triage): a human-CANCELLED plan (plan_cancel
`.cancelled` marker) or a `qctl supersede`d bundle with NO live (pending/running) job is
RETIRED -- it must not sit in the live Queue panel ("pending 5/9 slices, 0 jobs", replay-
endorse, because 21 done rows remained) nor raise Needs attention. A retired bundle that
still has a live job stays visible (a marker must not hide running work); an unmarked
bundle with only done rows is untouched.

Real _bundle_views / _needs_attention_ids with stubbed state + temp fixtures (never the
live queue). Run: python3 test-bundle-retired-queue.py [--revert-check]
(--revert-check runs built-in mutants of the API source; each must FAIL)."""
import importlib.util, json, os, shutil, subprocess, sys, tempfile, time
from pathlib import Path

HERE = Path(__file__).resolve().parent
def _api_path():
    """--api PATH (the canary seam), else $API_SRC (revert-check mutants), else the repo's
    src/ (this file in the dashboard repo's tests/), else a sibling (this file in ~/bin)."""
    if "--api" in sys.argv:
        return Path(sys.argv[sys.argv.index("--api") + 1])
    if os.environ.get("API_SRC"):
        return Path(os.environ["API_SRC"])
    repo = HERE.parent / "src" / "ollama-queue-api.py"
    return repo if repo.exists() else HERE / "ollama-queue-api.py"


API = _api_path()
FAILS = []


def check(name, got, want):
    ok = got == want
    print(("ok  " if ok else "FAIL") + f": {name}" + ("" if ok else f"  (got {got!r}, want {want!r})"))
    if not ok:
        FAILS.append(name)


def run():
    root = Path(tempfile.mkdtemp())
    os.environ["OLLAMA_SUPERSEDED_FILE"] = str(root / "superseded.json")
    sys.argv = [str(API)]
    spec = importlib.util.spec_from_file_location("api_under_test", str(API))
    api = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(api)
    sys.path.insert(0, str(HERE)); sys.path.insert(0, str(Path.home() / "bin"))   # plan_cancel, bundle_view
    now = time.time()
    live, logs, runs, chain = root / "live", root / "logs", root / "runs", root / "chain"
    for d in (live, logs, runs, chain):
        d.mkdir(parents=True)
    api.q.LIVE_LOG_DIR = live
    api.SLICE_RUNS_DIR = runs
    api.q.slice_group_index = lambda *a, **k: {}
    for p in ("canc", "canclive", "sup", "plain"):
        (runs / f"{p}.json").write_text(json.dumps({
            "label": p, "order": ["s1", "s2", "s3"],
            "slices": {"s1": {"status": "done"}, "s2": {"status": "pending"},
                       "s3": {"status": "pending"}}}))
    for p in ("canc", "canclive"):
        (runs / f"{p}.cancelled").write_text(json.dumps({"label": p, "by": "t", "reason": "r"}))
    (root / "superseded.json").write_text(json.dumps(
        {"sup": {"by": "x", "reason": "replaced", "ts": "2026-10-08T00:00:00Z"}}))
    n = [0]

    def row(plan, status):
        n[0] += 1
        return {"id": f"{n[0]:012x}", "label": f"{plan}-s1", "status": status,
                "bundle": plan, "finished_at": now - 100}
    state = {"jobs": [row("canc", "done"), row("canc", "done"), row("canc", "failed"),
                      row("sup", "done"), row("sup", "failed"),
                      row("canclive", "done"), row("canclive", "pending"),
                      row("plain", "done")]}
    bvs = api._bundle_views(state=state, runs_dir=runs, chain_dir=chain, log_dir=logs,
                            heal_path=root / "heal.json", preflight_dir=root / "pf",
                            progress=[], alive=lambda pid: False, now=now, history=[])
    ks = set(bvs["views"])
    check("cancelled plan with only done/failed rows is NOT in the Queue panel", "canc" in ks, False)
    check("superseded bundle with no live job is NOT in the Queue panel", "sup" in ks, False)
    check("cancelled plan WITH a pending job stays visible", "canclive" in ks, True)
    check("unmarked bundle with only done rows is untouched", "plain" in ks, True)

    # the /api/jobs row path (the Queue panel's grouped rows): done rows of a retired plan
    # must not be rebuilt into a "pending X/Y" bundle (plan_incomplete) -- the replay-endorse bug
    def grows():
        out = []
        for p in ("canc", "sup", "canclive", "plain"):
            out += [dict(r, group_key=p, status="done") for r in [row(p, "done")]]
        out.append(dict(row("canclive", "pending"), group_key="canclive"))
        return api._annotate_plan_rollup(out, runs_dir=runs, log_dir=logs)
    gr = grows()
    inc = {r["group_key"]: r.get("plan_incomplete") for r in gr if r["status"] == "done"}
    check("rollup: cancelled/superseded no-live plan's done row is NOT plan_incomplete (not 'pending X/Y')",
          (bool(inc.get("canc")), bool(inc.get("sup"))), (False, False))
    check("rollup: unmarked plan still owes slices (plan_incomplete)", inc.get("plain"), True)
    check("rollup: cancelled plan WITH a pending job still rolls up",
          inc.get("canclive"), True)

    # pure helper
    gk = lambda j: j.get("bundle")
    jobs = [{"bundle": "a", "status": "done"}, {"bundle": "b", "status": "running"},
            {"bundle": "c", "status": "pending"}, {"bundle": "d", "status": "failed"}]
    check("helper: marked + no live => retired; live or unmarked => not",
          api._retired_bundle_keys(["a", "b", "c", "d", "e"], jobs, gk,
                                   {"a": {}, "b": {}, "d": {}}, lambda k: {"x": 1} if k == "c" else None),
          {"a", "d"})
    check("helper: no markers => nothing retired",
          api._retired_bundle_keys(["a"], jobs, gk, {}, lambda k: None), set())

    # Needs attention classifier: failed row of a superseded, non-live bundle raises no alarm
    arows = [{"id": "f1", "status": "failed", "group_key": "sup", "label": "sup-s1"},
             {"id": "f2", "status": "failed", "group_key": "plain", "label": "plain-s1"},
             {"id": "f3", "status": "failed", "group_key": "supl", "label": "supl-s1"},
             {"id": "p3", "status": "pending", "group_key": "supl", "label": "supl-s2"}]
    na = api._needs_attention_ids(arows, state_of=lambda k: None, log_dir=logs,
                                  verdict_of=lambda *a, **k: None,
                                  superseded_map={"sup": {}, "supl": {}})
    check("Needs attention: superseded no-live bundle's failed row is NOT an alarm", "f1" in na, False)
    check("Needs attention: unmarked failed row still alarms", "f2" in na, True)
    check("Needs attention: superseded bundle with a live row still alarms", "f3" in na, True)
    shutil.rmtree(root, ignore_errors=True)


MUT = {
    "marker-ignored": ("        if k in sup or canc:\n            out.add(k)", "        if False:\n            out.add(k)"),
    "live-ignored": ("        if k in live:\n            continue\n", ""),
    "not-applied": ("        for k in _retired_bundle_keys(list(views), jobs, _gk_live,\n                                      bv.load_superseded(), _canc):",
                    "        for k in ():"),
    "rollup-stranded": ("        if k in _retired:\n            continue\n        prog = _load_plan_progress",
                        "        prog = _load_plan_progress"),
    "attention-unguarded": ("        if gk and gk in _retired:\n            continue\n", ""),
    "cancel-ignored": ("            return _pc.cancelled(k, runs_dir=runs_dir)\n        for k in _retired",
                       "            return None\n        for k in _retired"),
}

if __name__ == "__main__":
    if "--revert-check" in sys.argv:
        src, bad = API.read_text(), 0
        for n_, (o, nw) in MUT.items():
            if src.count(o) != 1:
                print(f"ANCHOR MISSING: {n_}"); bad += 1; continue
            t = Path(tempfile.mkdtemp()) / "api.py"
            t.write_text(src.replace(o, nw))
            p = subprocess.run([sys.executable, __file__], env=dict(os.environ, API_SRC=str(t)),
                               capture_output=True, text=True)
            print("revert %-20s %s" % (n_, "bites" if p.returncode else "INERT"))
            bad += 0 if p.returncode else 1
        print("REVERT-CHECK OK" if not bad else f"REVERT-CHECK FAILED ({bad})")
        sys.exit(1 if bad else 0)
    try:
        run()
    except Exception:
        import traceback; traceback.print_exc(); FAILS.append("crashed")
    print(f"\n{len(FAILS)} failure(s)" if FAILS else "\nALL PASSED")
    sys.exit(1 if FAILS else 0)
