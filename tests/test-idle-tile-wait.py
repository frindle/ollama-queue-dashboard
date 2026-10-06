#!/usr/bin/env python3
"""Idle "Running now" tile says what the queue waits on (Penn 2026-10-06): one short plain
line, never daemon-log text. Runs the page's own idleWaitText() under node against cases.
Run: python3 tests/test-idle-tile-wait.py [--revert-check]  (--revert-check runs it against
the committed HEAD source and expects FAILURE; run it BEFORE committing the change).
"""
import importlib.util, json, os, re, subprocess, sys, tempfile
from importlib.machinery import SourceFileLoader
from pathlib import Path

HERE = Path(__file__).resolve().parent
REAL_HOME = os.environ["HOME"]
FAILS = []


def check(name, got, want):
    ok = got == want
    print(("ok  " if ok else "FAIL") + f": {name}" + ("" if ok else f"  (got {got!r}, want {want!r})"))
    if not ok:
        FAILS.append(name)


def html_of(src):
    home = tempfile.mkdtemp(prefix="idle-tile-")
    os.environ["HOME"] = home
    os.symlink(os.environ.get("OLLAMA_PIPELINE_BIN", str(Path(REAL_HOME) / "bin")), os.path.join(home, "bin"))
    ld = SourceFileLoader("qapi_idle", str(src))
    m = importlib.util.module_from_spec(importlib.util.spec_from_loader("qapi_idle", ld))
    ld.exec_module(m)
    return m.FRONTEND_HTML


def run(src):
    html = html_of(src)
    m = re.search(r"function idleWaitText\(.*?\n}\n", html, re.S)
    check("idleWaitText present", bool(m), True)
    check("tile renders it", "idleWaitText(queueWait" in html, True)
    if not m:
        return
    pend = [{"status": "pending"}]
    cases = [
        ("empty", [None, [], None], "Queue empty"),
        ("done only is empty", [None, [{"status": "done"}], "x"], "Queue empty"),
        ("fallback with active bundle", [{"source": "none", "log": {"focus_line": "[queue] HELD zzz"}}, pend, "rt-egift-link-s1"],
         "Waiting on: bundle rt-egift-link-s1 to finish"),
        ("fallback no bundle", [{"source": "none"}, pend, None],
         "Waiting on: queued work (reason shows after the next daemon restart)"),
        ("daemon state reason, idle lanes only", [{"source": "state", "lanes": {"studio-db": {"state": "idle", "pending": 3,
            "reason": {"sentence": "holding bundle rt-egift-link-s1 (slicer/chain advance)"}},
            "unraid": {"state": "busy", "pending": 1, "reason": {"sentence": "no"}}}}, pend, "other"],
         "Waiting on: holding bundle rt-egift-link-s1 (slicer/chain advance)"),
    ]
    js = m.group(0) + "\nconst cases=" + json.dumps([c[1] for c in cases]) + ";\nconsole.log(JSON.stringify(cases.map(c=>idleWaitText(...c))));"
    out = subprocess.run(["node", "-e", js], capture_output=True, text=True)
    got = json.loads(out.stdout) if out.returncode == 0 else [out.stderr[-300:]] * len(cases)
    for (name, _, want), g in zip(cases, got):
        check(name, g, want)
    check("no log text in output", any("HELD" in g for g in got), False)


def main():
    if "--revert-check" in sys.argv:
        old = subprocess.run(["git", "-C", str(HERE.parent), "show", "HEAD:src/ollama-queue-api.py"],
                             capture_output=True, text=True).stdout
        tmp = Path(tempfile.mkdtemp()) / "ollama-queue-api.py"
        tmp.write_text(old)
        r = subprocess.run([sys.executable, __file__, "--src", str(tmp)], capture_output=True, text=True)
        print(r.stdout[-600:])
        bit = r.returncode != 0
        print(("ok  " if bit else "FAIL") + ": revert-check -- pre-change source FAILS this test")
        return 0 if bit else 1
    src = Path(sys.argv[sys.argv.index("--src") + 1]) if "--src" in sys.argv else HERE.parent / "src" / "ollama-queue-api.py"
    try:
        run(src)
    except Exception as e:
        import traceback; traceback.print_exc(); FAILS.append(f"crashed: {e}")
    print(f"\n{len(FAILS)} failure(s)" if FAILS else "\nALL PASSED")
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
