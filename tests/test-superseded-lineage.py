#!/usr/bin/env python3
"""Regression (Penn 2026-10-09, rt-bg-commitments-guard "9/9 jobs 5 failed"): failed job
pseudo-slices whose LINEAGE later passed (or is still retrying) must not turn the bundle red
or count in "N failed". Pure bundle_view.build_job_view / bundle_outcome.
Run: python3 test-superseded-lineage.py [--revert-check]  -> prints ALL PASSED."""
import importlib.util, os, subprocess, sys, tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
BV = Path(os.environ.get("BV_SRC") or HERE.parent / "src" / "bundle_view.py")
FAILS = []


def check(name, got, want):
    ok = got == want
    print(("ok  " if ok else "FAIL") + f": {name}" + ("" if ok else f"  (got {got!r}, want {want!r})"))
    if not ok:
        FAILS.append(name)


def run():
    spec = importlib.util.spec_from_file_location("bv_t", str(BV))
    bv = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(bv)
    K = "rt-bg-commitments-guard"
    T0 = 1_791_000_000.0
    iso = lambda dt: __import__("datetime").datetime.fromtimestamp(T0 + dt, __import__("datetime").timezone.utc).isoformat()

    def job(i, label, status, dt, **kw):
        return dict(id=i, label=label, status=status, bundle=K, enqueued_at=iso(dt), **kw)

    def phases(jobs, **kw):
        v = bv.build_job_view(K, jobs, now=T0 + 99999, **kw)
        return v, {s["title"]: s for s in v["slices"]}

    failed_s3 = [job("a1", "auto-author-rt-bg-sync-zero-guard-s3", "failed", 0),
                 job("a2", "auto-author-rt-bg-sync-zero-guard-s3-r1", "failed", 10)]
    # (c) ended auto-run exit 0 for the chain, after the failures
    passed = [("rt-bg-sync-zero-guard", K, T0 + 1000)]
    v, p = phases(failed_s3, passed_runs=passed)
    s = p["rt-bg-sync-zero-guard-s3"]
    check("failed rounds of a chain that later exited 0 are not attention", s["attention"], False)
    check("...and read done", s["phase"], "done")
    check("...so the bundle is not failed", bv.bundle_outcome(v)[1], 0)
    check("...and the failed history is still listed", len(s["history"]) >= 2, True)
    # not provable -> stays failed
    v, p = phases(failed_s3)
    check("no pass proof: still failed", p["rt-bg-sync-zero-guard-s3"]["attention"], True)
    check("no pass proof: counted", bv.bundle_outcome(v)[1], 1)
    v, p = phases(failed_s3, passed_runs=[("rt-bg-sync-zero-guard", K, T0 + 5)])
    check("pass BEFORE the last failure proves nothing", p["rt-bg-sync-zero-guard-s3"]["attention"], True)
    v, p = phases(failed_s3, passed_runs=[("rt-bg-sync-zero-guard", "other-bundle", T0 + 1000)])
    check("another bundle's pass proves nothing", p["rt-bg-sync-zero-guard-s3"]["attention"], True)
    v, p = phases(failed_s3, passed_runs=[("rt-bg-sync", K, T0 + 1000)])
    check("a prefix-only chain label does not match", p["rt-bg-sync-zero-guard-s3"]["attention"], True)
    # (b) sibling parent group
    parent_pending = failed_s3 + [job("b1", "rt-bg-sync-zero-guard", "pending", 50)]
    v, p = phases(parent_pending)
    s = p["rt-bg-sync-zero-guard-s3"]
    check("a RUNNING/PENDING retry in the lineage is never failed", (s["phase"], s["attention"]), ("queued", False))
    check("...bundle not red", bv.bundle_outcome(v)[1], 0)
    parent_done = failed_s3 + [job("b1", "rt-bg-sync-zero-guard", "done", 50)]
    v, p = phases(parent_done)
    check("sibling parent job done supersedes sub-slice failure", p["rt-bg-sync-zero-guard-s3"]["phase"], "done")
    parent_failed = failed_s3 + [job("b1", "rt-bg-sync-zero-guard", "failed", 50)]
    v, p = phases(parent_failed)
    check("parent also failed: both outstanding", bv.bundle_outcome(v)[1], 2)
    # (a) superseded_by a live/done job
    sup = [job("c1", "needs-opus-auto-lib", "failed", 0, superseded_by="c2"),
           job("c2", "lib", "running", 10)]
    v, p = phases(sup)
    check("failed with superseded_by running retry: not failed", bv.bundle_outcome(v)[1], 0)
    # newest-job failure with nothing after still fails
    v, p = phases([job("d1", "lonely", "failed", 0)])
    check("a lone failure still fails", bv.bundle_outcome(v)[1], 1)
    # through counts superseded-as-done consistently
    v, _ = phases(failed_s3, passed_runs=passed)
    check("through==total once superseded", (v["through"], v["total"]), (1, 1))

    # compact pass summary: failed attempts keep their failed label
    att = [job("g1", "auto-author-zed", "failed", 0, terminal_reason="prose_loop"),
           job("g2", "auto-author-zed-r1", "failed", 10),
           job("g3", "auto-author-zed-r2", "done", 20)]
    v, p = phases(att)
    sm = p["zed"]["summary"]
    check("done after failures: one-line summary", sm,
          "passed after 3 attempts: author failed (prose_loop) -> r1 failed -> r2 passed")
    check("...row is done, not attention", (p["zed"]["phase"], p["zed"]["attention"]), ("done", False))
    check("clean single pass has no summary", phases([job("h1", "solo", "done", 0)])[1]["solo"]["summary"], "")
    # live chain driver, no queue row: bundle RUNNING, never failed/finished
    v, p = phases([job("i1", "chainx", "done", 0)], active_runs=[("chainx", K, "preflight r2")])
    check("done row of a chain still advancing reads running", (p["chainx"]["phase"], p["chainx"]["active"]), ("authoring", True))
    v, p = phases([job("i2", "chainy", "failed", 0)], active_runs=[("chainy", K, "preflight r2")])
    check("failed row of a chain still advancing is not failed", bv.bundle_outcome(v)[1], 0)
    v = bv.build_job_view(K, [], now=T0, active_runs=[("chainz", K, "author r1")])
    check("bundle with only a live chain driver renders running", [(s["title"], s["active"]) for s in v["slices"]], [("chainz", True)])
    check("another bundle's driver is ignored", bv.build_job_view(K, [], now=T0, active_runs=[("c", "zz", "x")]), None)


if "--revert-check" in sys.argv:
    src = BV.read_text()
    muts = {"proof (c) removed": ('for (lab, bun, end) in passed_runs or []:', 'for (lab, bun, end) in []:'),
            "proof (b) removed": ('if par is not None and par is not r:', 'if False:'),
            "proof (a) removed": ('if tj and tj.get("status") in _LIVE:', 'if False:'),
            "summary dropped": ('"summary": _attempts_summary(mine, base) if phase == "done" else "",', '"summary": "",'),
            "driver ignored": ('active_runs or []', '[]'),
            "ordering ignored": ('and r["_t"] and r["_t"] <= end:', 'and True:')}
    bad = 0
    for name, (a, b) in muts.items():
        assert a in src, name
        with tempfile.TemporaryDirectory() as d:
            m = Path(d) / "bundle_view.py"
            m.write_text(src.replace(a, b, 0 if a == 'active_runs or []' else 1) if a != 'active_runs or []' else src.replace(a, b))
            r = subprocess.run([sys.executable, __file__], env={**os.environ, "BV_SRC": str(m)},
                               capture_output=True, text=True)
        print(("mutant caught: " if r.returncode else "MUTANT SURVIVED: ") + name)
        bad += 0 if r.returncode else 1
    sys.exit(1 if bad else 0)
run()
print("ALL PASSED" if not FAILS else "FAILED: %s" % FAILS)
sys.exit(1 if FAILS else 0)
