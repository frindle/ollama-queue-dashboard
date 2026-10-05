#!/usr/bin/env python3
"""API glue for run-status retention (ollama-queue-api.py <-> runstatus_retention.py):
the sweep honours RETENTION_MODE, apply re-checks each id with the shared predicate,
and archives go through _archive_run with a retention 'how'. --revert-check mutates the
API glue and requires the suite to go RED. API_SRC env overrides the file under test."""
import importlib.util, os, subprocess, sys, tempfile
from importlib.machinery import SourceFileLoader
from pathlib import Path

SRC = Path(os.environ.get("API_SRC") or Path(os.environ.get("DASHBOARD_SRC") or Path(__file__).resolve().parent.parent / "src") / "ollama-queue-api.py")
FAILS = []


def check(name, got, want):
    ok = got == want
    print(("ok  " if ok else "FAIL") + f": {name}" + ("" if ok else f"  (got {got!r}, want {want!r})"))
    if not ok:
        FAILS.append(name)


def main():
    loader = SourceFileLoader("api_t", str(SRC))
    spec = importlib.util.spec_from_loader("api_t", loader)
    m = importlib.util.module_from_spec(spec)
    sys.argv = [str(SRC)]
    loader.exec_module(m)
    rows = [{"id": "h1", "label": "auto-author-p-s1-a", "status": "done"},
            {"id": "sig", "label": "auto-author-p-s1-a", "status": "done", "awaiting_signoff": True},
            {"id": "dl", "label": "p-s1-a", "status": "done", "raw_verdict": "PASS"}]
    m._run_status_jobs = lambda limit=100: [dict(r) for r in rows]
    m._load_slice_index = lambda: ({"p-s1-a": "p"}, {})
    m.SLICE_RUNS_DIR = Path(tempfile.mkdtemp())
    (m.SLICE_RUNS_DIR / "p.json").write_text('{"slices": {"s1-a": {"status": "done"}}}')
    m._RETENTION_LOG = m.SLICE_RUNS_DIR / "ret.log"
    archived = []
    m._archive_run = lambda jid, override=None, how=None: (archived.append((jid, how)) or {"ok": True})
    m.RETENTION_MODE = "shadow"
    m._retention_sweep_once()
    check("shadow sweep archives nothing", archived, [])
    m.RETENTION_MODE = "off"
    check("off sweep does nothing", m._retention_sweep_once(), [])
    m.RETENTION_MODE = "live"
    m._retention_sweep_once()
    check("live sweep archives only the eligible harness row", [a[0] for a in archived], ["h1"])
    check("archive 'how' records the retention reason",
          archived and archived[0][1].startswith("run-status retention:"), True)
    check("live sweep logs what it archived", "h1" in m._RETENTION_LOG.read_text(), True)
    archived.clear()
    res = m._retention_apply(["sig", "dl", "h1"])
    check("apply refuses sign-off + deliverable, clears the eligible id",
          [(r["id"], r["ok"]) for r in res], [("sig", False), ("dl", False), ("h1", True)])
    check("apply archived only h1", [a[0] for a in archived], ["h1"])
    pv = m._retention_preview()
    check("preview lists the eligible id", [d["id"] for d in pv["eligible"]], ["h1"])
    check("routes are wired",
          ('"/api/runs/retention/apply"' in SRC.read_text()
           and 'self.path == "/api/runs/retention"' in SRC.read_text()), True)
    print("\nALL PASS" if not FAILS else f"\n{len(FAILS)} FAILED: {FAILS}")
    return 0 if not FAILS else 1


MUTATIONS = [
    ("shadow archives", 'apply=RETENTION_MODE == "live")', 'apply=True)'),
    ("off still sweeps", 'if RETENTION_MODE not in ("live", "shadow"):\n        return []',
     'if False:\n        return []'),
    ("how dropped", 'how=f"run-status retention: {reason}"', 'how=None'),
]


def revert_check():
    bad = 0
    src = SRC.read_text()
    for name, old, new in MUTATIONS:
        assert src.count(old) == 1, f"anchor missing: {name}"
        with tempfile.NamedTemporaryFile("w", suffix="-api.py", delete=False,
                                         dir=str(SRC.parent)) as f:
            f.write(src.replace(old, new))
        r = subprocess.run([sys.executable, __file__], env={**os.environ, "API_SRC": f.name},
                           capture_output=True, text=True, timeout=300)
        os.unlink(f.name)
        red = r.returncode != 0
        print(("bites" if red else "INERT") + f": revert '{name}' -> suite {'RED' if red else 'green'}")
        bad += 0 if red else 1
    print("REVERT-CHECK OK" if not bad else f"REVERT-CHECK FAILED ({bad} inert)")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(revert_check() if "--revert-check" in sys.argv else main())
