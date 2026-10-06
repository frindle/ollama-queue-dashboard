#!/usr/bin/env python3
"""Regression test: the queue says WHAT it is waiting on (Penn 2026-10-06: "queue looks idle
while jobs are pending and I can't see why"). Stubbed state + temp files only -- never the
live queue. Covers: the daemon-side aggregation (build_wait_state), the shared reader
(wait_view: state / stale / log-fallback / none), `status` header lines, the dashboard's
/api/jobs row field + /api/queue-wait payload + banner, and that cmd_run still threads
every skip site into the state file.
Run: python3 test-queue-wait.py [--revert-check]   (--revert-check runs it against the
pre-change ~/bin/*-waitreason backups and expects FAILURE -- proves the test bites).
"""
import glob, importlib.util, inspect, json, os, subprocess, sys, tempfile, time
from pathlib import Path

HERE = Path(__file__).resolve().parent
BIN = Path.home() / "bin"
FAILS = []


def check(name, got, want):
    ok = got == want
    print(("ok  " if ok else "FAIL") + f": {name}" + ("" if ok else f"  (got {got!r}, want {want!r})"))
    if not ok:
        FAILS.append(name)


def load(path, name):
    sys.argv = [str(path)]
    from importlib.machinery import SourceFileLoader
    spec = importlib.util.spec_from_file_location(name, str(path), loader=SourceFileLoader(name, str(path)))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def run(queue_src, api_src):
    q = load(queue_src, "oq_t")
    tmp = Path(tempfile.mkdtemp())
    sp, lp = tmp / "queue-wait.json", tmp / "daemon.log"
    now = 10_000.0
    jobs = [{"id": "p1", "status": "pending", "host_pref": "studio-db", "label": "s1-parse-link"},
            {"id": "h1", "status": "held", "host_pref": "studio-db", "hold_reason": "pending gate", "held_on": "gate-7"},
            {"id": "u1", "status": "pending", "host_pref": "unraid", "label": "u"},
            {"id": "r1", "status": "running", "lane": "unraid", "host_pref": "unraid"}]
    notes = {"p1": q.focus_wait_note("rt-egift-link-s1", "slicer advance in flight (next slice s1-parse-link pending)",
                                      True, "", None)}
    st = q.build_wait_state(jobs, notes, ["p1", "h1", "u1"], {"active": "rt-egift-link-s1", "hold": True}, None, now - 400)
    st2 = q.build_wait_state(jobs, notes, ["p1", "h1", "u1"], {"active": "rt-egift-link-s1", "hold": True}, st, now)
    q.write_wait_state(st2, sp)
    check("atomic write left no tmp file", [f.name for f in tmp.iterdir() if ".tmp" in f.name], [])
    v = q.wait_view(now=now + 5, path=sp, log_path=lp)
    check("fresh file -> source=state", v["source"], "state")
    check("studio-db idle lane flagged STUCK after >5min same reason", v["stuck"], ["studio-db"])
    hdr = q.wait_header_lines(v)
    check("status header has one IDLE line naming bundle + slicer",
          (len(hdr), "studio-db: IDLE" in hdr[0], "rt-egift-link-s1" in hdr[0], "slicer" in hdr[0],
           "STUCK" in hdr[0]), (1, True, True, True, True))
    check("busy lane (unraid) gets no header", any(l.startswith("unraid") for l in hdr), False)
    check("per-job short reasons for pending + held",
          (v["jobs"]["p1"]["code"], v["jobs"]["h1"]["short"]), ("bundle-hold", "held: pending gate -> gate-7"))
    v2 = q.wait_view(now=now - 399 + 5, path=sp, log_path=lp)  # reason only ~0s old at that clock
    check("a reason younger than 5min is not stuck", v2["stuck"], [])
    # stale file
    v3 = q.wait_view(now=now + 1000, path=sp, log_path=lp)
    check("stale file -> source=stale, no stuck claims", (v3["source"], v3["stuck"]), ("stale", []))
    # missing file + log fallback
    lp.write_text("noise\n[queue] focus: HOLDING bundle X -- launchable\n[queue] HELD abc (lbl) -- a job finished this tick\n")
    v4 = q.wait_view(now=now, path=tmp / "nope.json", log_path=lp)
    h4 = q.wait_header_lines(v4)
    check("no file -> degraded message + log lines labelled as from the log",
          (v4["source"], "reason unavailable: daemon older than state file" in h4[0],
           any("from daemon log" in l and "HOLDING bundle X" in l for l in h4),
           any("HELD abc" in l for l in h4)), ("none", True, True, True))
    # daemon wiring: every skip site records a reason and the loop persists it
    src = inspect.getsource(q.cmd_run)
    check("cmd_run threads reasons at >=7 skip sites and persists them",
          (src.count("_job_wait[job[\"id\"]]") >= 7, "write_wait_state(" in src, "load_wait_state()" in src),
          (True, True, True))
    # dashboard
    api = load(api_src, "api_t")
    os.environ["HOME"] = str(tmp)  # not used; wait_view default path is patched below
    real_wv = api.q.wait_view
    api.q.wait_view = lambda now=None: real_wv(now=now_fixed[0], path=sp, log_path=lp)
    now_fixed = [now + 5]
    rows = api._annotate_queue_wait([{"id": "p1", "status": "pending"}, {"id": "zz", "status": "done"}])
    check("/api/jobs row gets queue_wait from the daemon state",
          (rows[0].get("queue_wait") or {}).get("code"), "bundle-hold")
    check("terminal row untouched", "queue_wait" in rows[1], False)
    pl = api._queue_wait_payload()
    check("payload: lanes + stuck, no internal sig/jobs",
          (pl["source"], pl["stuck"], "sig" in json.dumps(pl), "jobs" in pl), ("state", ["studio-db"], False, False))
    api.q.wait_view = lambda now=None: real_wv(now=now, path=tmp / "nope.json", log_path=lp)
    pl2 = api._queue_wait_payload()
    check("payload degrades to log fallback", (pl2["source"], bool(pl2["log"]["focus_line"])), ("none", True))
    check("rows get no queue_wait when only the log is available",
          "queue_wait" in api._annotate_queue_wait([{"id": "p1", "status": "pending"}])[0], False)
    html = api.FRONTEND_HTML
    check("page has the Waiting on banner, stuck styling and the endpoint",
          ("waitBanner" in html, "POSSIBLE STUCK SEAM" in html, "/api/queue-wait" in html, "queue_wait" in html),
          (True, True, True, True))


def main():
    if "--revert-check" in sys.argv:
        qb = sorted(glob.glob(str(BIN / "ollama-queue.py.bak-*-waitreason")))[-1]
        ab = sorted(glob.glob(str(BIN / "ollama-queue-api.py.bak-*-waitreason")))[-1]
        r = subprocess.run([sys.executable, __file__, "--queue", qb, "--api", ab], capture_output=True, text=True)
        print(r.stdout[-1500:], r.stderr[-500:])
        bit = r.returncode != 0
        print(("ok  " if bit else "FAIL") + ": revert-check -- the pre-change code FAILS this test")
        return 0 if bit else 1
    qs = Path(sys.argv[sys.argv.index("--queue") + 1]) if "--queue" in sys.argv else BIN / "ollama-queue.py"
    as_ = Path(sys.argv[sys.argv.index("--api") + 1]) if "--api" in sys.argv else HERE.parent / "src" / "ollama-queue-api.py"
    try:
        run(qs, as_)
    except Exception as e:
        import traceback
        traceback.print_exc()
        FAILS.append(f"crashed: {e}")
    print(f"\n{len(FAILS)} failure(s)" if FAILS else "\nALL PASSED")
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
