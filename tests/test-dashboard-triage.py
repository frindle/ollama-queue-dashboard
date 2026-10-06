#!/usr/bin/env python3
"""Dashboard surfaces the open-triage count (Needs attention tile + /api/triage).
Behavioural: _triage_summary reads the triage index and never raises. Static: the endpoint is
routed, fetched, and rendered on the tile. Run with API_SRC=<mutant> for the revert check;
`--revert-check` runs the built-in mutants (each must FAIL)."""
import json, os, re, subprocess, sys, tempfile
from pathlib import Path
HERE = Path(__file__).resolve().parent.parent
API = Path(os.environ.get("API_SRC") or HERE / "src" / "ollama-queue-api.py")
BIN = Path.home() / "bin"


def run():
    fails = []
    def chk(n, ok):
        print(("ok  " if ok else "FAIL") + " - " + n)
        if not ok: fails.append(n)
    js = API.read_text()
    m = re.search(r"def _triage_summary\(\):.*?(?=\n\ndef )", js, re.S)
    chk("_triage_summary defined", bool(m))
    tmp = Path(tempfile.mkdtemp())
    (tmp / "index.json").write_text(json.dumps({"signatures": {
        "a": {"slug": "a", "status": "open", "opened_ts": "2026-10-06T10:00:00Z", "job_ids": ["1", "2"], "live_jobs": 2},
        "b": {"slug": "b", "status": "open", "opened_ts": "2026-10-06T11:00:00Z", "job_ids": ["3"], "live_jobs": 1},
        "c": {"slug": "c", "status": "acted", "opened_ts": "2026-10-06T09:00:00Z", "job_ids": ["4"]}}}))
    os.environ["TRIAGE_DIR"] = str(tmp)
    sys.path.insert(0, str(BIN))
    ns = {"sys": sys, "QUEUE_PATH": BIN / "ollama-queue.py"}
    if m:
        exec(m.group(0), ns)
        r = ns["_triage_summary"]()
        chk("summary counts only open signatures, flags repeats", r["open"] == 2 and r["repeats"] == 1)
        import triage_packets as _tp
        _tp.INDEX = Path("/nonexistent/zzz/index.json")
        chk("missing index -> zero, no exception", ns["_triage_summary"]()["open"] == 0)
    chk("GET /api/triage routed to _triage_summary", '"/api/triage"' in js and "self._json(_triage_summary())" in js)
    chk("page fetches /api/triage", "fetch('/api/triage')" in js)
    chk("tile renders triage count with the command", "triage: ${triageOpen.open}" in js and "triage-emit.py --json" in js)
    return fails


MUT = {"route": ('self._json(_triage_summary())', 'self._json({})'),
       "fetch": ("fetch('/api/triage')", "fetch('/api/queue-wait')"),
       "tile": ("triage: ${triageOpen.open}", "triage: x"),
       "open-only": ('if status and e.get("status") != status', None)}

if __name__ == "__main__":
    if "--revert-check" in sys.argv:
        bad = []
        src = API.read_text()
        for n, (o, nw) in MUT.items():
            if nw is None or o not in src:
                continue
            t = Path(tempfile.mkdtemp()) / "api.py"
            t.write_text(src.replace(o, nw, 1))
            p = subprocess.run([sys.executable, __file__], env=dict(os.environ, API_SRC=str(t)), capture_output=True, text=True)
            print("revert %-8s %s" % (n, "killed" if p.returncode else "SURVIVED"))
            if not p.returncode: bad.append(n)
        sys.exit(1 if bad else 0)
    sys.exit(1 if run() else 0)
