#!/usr/bin/env python3
"""Per-lane "RUNNING NOW" (Penn 2026-10-09): one equal row per lane (studio-db, unraid, CPU lane)
with label, model, host, elapsed, bundle and a short live status line; an idle lane says so
("unraid: idle"); a gpu-exclusive job shows its phase (waiting for VRAM / loading / serving).
Runs the page's own laneNowRows()/laneNowHtml() under node.
Run: python3 tests/test-dashboard-lanes-now.py [--src PATH] [--mutants]
  --mutants applies each revert mutant to the source and expects this test to FAIL on each."""
import importlib.util, json, os, re, subprocess, sys, tempfile
from importlib.machinery import SourceFileLoader
from pathlib import Path

HERE = Path(__file__).resolve().parent
REAL_HOME = os.environ["HOME"]
FAILS = []

MUTANTS = [
    ("an idle lane is silently omitted",
     "return NOW_LANES.map(([k, name]) => ({lane: k, name, idle: !by[k].length, jobs: by[k]}));",
     "return NOW_LANES.map(([k, name]) => ({lane: k, name, idle: !by[k].length, jobs: by[k]})).filter(r => !r.idle);"),
    ("unraid jobs land in the studio lane",
     "if (l.includes('unraid')) return 'unraid';", "if (false) return 'unraid';"),
    ("the CPU lane is not shown",
     "const NOW_LANES = [['studio-db', 'studio-db'], ['unraid', 'unraid'], ['cpu', 'CPU lane']];",
     "const NOW_LANES = [['studio-db', 'studio-db'], ['unraid', 'unraid']];"),
    ("gpu-exclusive phase is not shown",
     "if (j.gpu_wait) return 'waiting for VRAM: ' + j.gpu_wait;", "if (false) return '';"),
    ("bundle missing from the row",
     " &middot; bundle ' + escapeHtml(x.bundle) : ''}", "' : ''}"),
    ("tile does not render the lane rows (idle branch)",
     "      ${other.length ? `<div class=\"now-more\">Off-GPU, live now: ${other.map(fmtAct).join(' &middot; ')}</div>` : ''}\n      ${laneNowHtml(laneNowRows(jobs, acts, window.cpuLaneRunning))}`;",
     "      ${other.length ? `<div class=\"now-more\">Off-GPU, live now: ${other.map(fmtAct).join(' &middot; ')}</div>` : ''}`;"),
    ("tile does not render the lane rows (running branch)",
     "\n      ${laneNowHtml(laneNowRows(jobs, acts, window.cpuLaneRunning))}`;\n    now.style.cursor = 'pointer';",
     "`;\n    now.style.cursor = 'pointer';"),
    ("cpu lane data never captured",
     "    window.cpuLaneRunning = d.running || [];\n", ""),
]


def check(name, got, want):
    ok = got == want
    print(("ok  " if ok else "FAIL") + f": {name}" + ("" if ok else f"  (got {got!r}, want {want!r})"))
    if not ok:
        FAILS.append(name)


def html_of(src):
    home = tempfile.mkdtemp(prefix="lanes-now-")
    os.environ["HOME"] = home
    os.symlink(os.environ.get("OLLAMA_PIPELINE_BIN", str(Path(REAL_HOME) / "bin")), os.path.join(home, "bin"))
    ld = SourceFileLoader("qapi_lanes", str(src))
    m = importlib.util.module_from_spec(importlib.util.spec_from_loader("qapi_lanes", ld))
    ld.exec_module(m)
    return m.FRONTEND_HTML


def node(js):
    out = subprocess.run(["node", "-e", js], capture_output=True, text=True)
    return json.loads(out.stdout) if out.returncode == 0 else {"err": out.stderr[-400:]}


def run(src):
    html = html_of(src)
    m = re.search(r"const NOW_LANES = .*?function laneNowHtml\(rows\) \{.*?\n\}\n", html, re.S)
    check("lane helpers present", bool(m), True)
    check("renderSummary uses them (idle + running branches)", html.count("laneNowHtml(laneNowRows(") >= 2, True)
    check("CPU lane data is captured for the tile", "window.cpuLaneRunning = d.running" in html, True)
    if not m:
        return
    pre = ("const escapeHtml=s=>String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;');"
           "const formatElapsed=s=>s==null?'?':Math.round(s)+'s';\n" + m.group(0) + "\n")
    jobs = [
        {"id": "a1", "status": "running", "lane": "studio-db", "label": "slice-x-s1", "model": "qwen3.6-35b", "elapsed_s": 75,
         "iteration": 3, "max_iters": 24, "tok_s": 41.2},
        {"id": "g1", "status": "running", "lane": "studio-db", "label": "gpu-bench", "job_kind": "gpu_exclusive",
         "gpu_wait": "VRAM busy (holder a1)", "model": "-", "elapsed_s": 5},
        {"id": "p1", "status": "pending", "lane": None, "label": "later"},
    ]
    acts = [{"kind": "gpu", "id": "a1", "label": "slice-x-s1", "display": "slice-x-s1", "model": "qwen3.6-35b", "host": "studio-db",
             "elapsed_s": 75, "group_key": "bundle-x"}]
    r = node(pre + "const rows=laneNowRows(" + json.dumps(jobs) + "," + json.dumps(acts) + ",[]);"
             "console.log(JSON.stringify({rows, html: laneNowHtml(rows)}));")
    rows = r.get("rows") or []
    check("three lanes always present, in order", [x["lane"] for x in rows], ["studio-db", "unraid", "cpu"])
    check("unraid and cpu idle, studio busy", [x["idle"] for x in rows], [False, True, True])
    h = r.get("html", "")
    check("explicit 'unraid: idle' text", "unraid</span>: idle" in h, True)
    check("explicit CPU lane idle text", "CPU lane</span>: idle" in h, True)
    sd = rows[0]["jobs"] if rows else []
    check("studio row: label/model/host/elapsed/bundle/status", [(x["label"], x["model"], x["host"], x["elapsed_s"], x["bundle"], x["line"]) for x in sd][:1],
          [("slice-x-s1", "qwen3.6-35b", "studio-db", 75, "bundle-x", "iter 3/24 - 41.2 tok/s")])
    check("gpu-exclusive job shows its waiting phase", [x["line"] for x in sd][1:], ["waiting for VRAM: VRAM busy (holder a1)"])
    check("bundle shown in the rendered row", "bundle bundle-x" in h, True)
    r2 = node(pre + "const jobs=[{id:'u1',status:'running',lane:'unraid',label:'u-job',model:'qwen3.5:9b',elapsed_s:9,phase:'warming'},"
              "{id:'g2',status:'running',lane:'studio-db',label:'g',job_kind:'gpu_exclusive',model:'-',elapsed_s:1},"
              "{id:'g3',status:'running',lane:'studio-db',label:'g3',job_kind:'gpu_exclusive',phase:'warming',model:'-',elapsed_s:1}];"
              "const rows=laneNowRows(jobs,[],[{id:'c1234567',stage:'lint',runner:'r1',running_s:12,label:'cpu-stage'}]);"
              "console.log(JSON.stringify({rows, html: laneNowHtml(rows)}));")
    rows = r2.get("rows") or []
    check("unraid job lands in the unraid lane, studio idle-free", [(x["lane"], len(x["jobs"])) for x in rows], [("studio-db", 2), ("unraid", 1), ("cpu", 1)])
    check("warming LLM job: 'loading model'", rows[1]["jobs"][0]["line"] if rows else None, "loading model")
    check("gpu-exclusive: serving vs loading phases",
          [x["line"] for x in rows[0]["jobs"]] if rows else None, ["serving", "loading"])
    check("cpu lane row", [(x["label"], x["model"], x["host"], x["elapsed_s"], x["line"]) for x in rows[2]["jobs"]] if rows else None,
          [("cpu-stage", "lint", "r1", 12, "stage lint")])
    esc = node(pre + "console.log(JSON.stringify(laneNowHtml(laneNowRows([{id:'x',status:'running',lane:'studio-db',label:'<script>',model:'m'}],[],[]))));")
    check("html escapes labels", "<script>" in str(esc), False)


def main():
    if "--mutants" in sys.argv:
        src = Path(sys.argv[sys.argv.index("--src") + 1]) if "--src" in sys.argv else HERE.parent / "src" / "ollama-queue-api.py"
        text = src.read_text()
        bad = 0
        for name, old, new in MUTANTS:
            if old not in text:
                print(f"FAIL: mutant anchor missing: {name}"); bad += 1; continue
            tmp = src.parent / ".mutant-ollama-queue-api.py"     # next to its sibling modules
            tmp.write_text(text.replace(old, new, 1))
            try:
                r = subprocess.run([sys.executable, __file__, "--src", str(tmp)], capture_output=True, text=True)
            finally:
                tmp.unlink(missing_ok=True)
            red = r.returncode != 0 and "FAIL:" in r.stdout and "crashed" not in r.stdout
            print(("ok  " if red else "FAIL") + f": mutant goes red -- {name}")
            bad += 0 if red else 1
        return 1 if bad else 0
    src = Path(sys.argv[sys.argv.index("--src") + 1]) if "--src" in sys.argv else HERE.parent / "src" / "ollama-queue-api.py"
    try:
        run(src)
    except Exception as e:
        import traceback; traceback.print_exc(); FAILS.append(f"crashed: {e}")
    print(f"\n{len(FAILS)} failure(s)" if FAILS else "\nALL PASSED")
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
