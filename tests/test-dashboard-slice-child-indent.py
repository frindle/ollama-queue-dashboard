#!/usr/bin/env python3
"""Dashboard: rows UNDER a slice line (author/gate/refine runs, stage headers,
attempt folds/dividers) render one indent level DEEPER than the slice line itself
(Penn 2026-10-03: "author and gate should be tabbed in another level from their
'host' bundle"). They used an inline padding-left:1.4rem, which overrode the CSS
child indent and put them LEFT of the slice line's 1.8rem.

Static (no DOM): inside renderSliceLines every padding-left must be >= SLICE_KID,
SLICE_KID must exceed the slice line's CSS padding (tr.queue-child col 2), and the
page's script must still parse (node --check).
API_SRC overrides the file under test (revert-check: the .bak).
"""
import importlib.util, json, os, re, shutil, subprocess, sys, tempfile
from importlib.machinery import SourceFileLoader
from pathlib import Path

# Layout: dashboard code in <repo>/src, shared pipeline (ollama-queue.py, dispatch_progress.py,
# handoff-emit.py, .bak files) in ~/bin. Override with DASHBOARD_SRC / OLLAMA_PIPELINE_BIN.
SRC_DIR = Path(os.environ.get("DASHBOARD_SRC") or Path(__file__).resolve().parent.parent / "src")
PIPELINE_BIN = Path(os.environ.get("OLLAMA_PIPELINE_BIN") or Path.home() / "bin")
BIN = PIPELINE_BIN
SRC = Path(os.environ.get("API_SRC") or SRC_DIR / "ollama-queue-api.py")
FAILS = []


def check(name, got, want=True):
    ok = got == want
    print(("ok  " if ok else "FAIL") + f": {name}" + ("" if ok else f"  (got {got!r}, want {want!r})"))
    if not ok:
        FAILS.append(name)


HARNESS = r"""
class CL { constructor(){this.s=new Set();} add(c){this.s.add(c);} contains(c){return this.s.has(c);} }
class El {
  constructor(tag){this.tag=tag;this.children=[];this.style={};this.classList=new CL();this.dataset={};this._h='';}
  set className(v){this.classList=new CL(); for (const c of String(v).split(/\s+/)) if (c) this.classList.add(c);}
  set innerHTML(v){this._h=v; if (this.tag!=='tr') return; this.children=[];
    const re=/<td([^>]*)>([\s\S]*?)<\/td>/g; let m;
    while ((m=re.exec(v))) { const td=new El('td'); td._h=m[2];
      const pl=/padding-left:\s*([\d.]+rem)/.exec(m[1]); if (pl) td.style.paddingLeft=pl[1];
      td.parentNode=this; this.children.push(td);} }
  get innerHTML(){return this._h;}
  get firstChild(){return null;}
  appendChild(c){c.parentNode=this;this.children.push(c);return c;}
  remove(){const p=this.parentNode; if(p){const i=p.children.indexOf(this); if(i>=0)p.children.splice(i,1);}}
  addEventListener(){} querySelector(){return null;}
}
const document={createElement:t=>new El(t)};
const tbody=new El('tbody');
const escapeHtml=x=>String(x); const stageLabel=(k,l)=>String(k||l||'');
const formatElapsed=s=>String(s); const sliceSuffix=()=>''; const saveExpandState=()=>{};
const refresh=()=>{}; const openLivelog=()=>{}; const buildQueueRow=()=>new El('tr');
const sliceOpenByDefault=()=>true;
const sliceHeader=sl=>({num:'Slice '+sl.sid, entry:'', tip:''});
__FUNCS__
const sliceExpanded=__EXP__;
__BODY__
renderSliceLines({key:'b', children:[]}, __VIEW__);
const strip=h=>String(h).replace(/<[^>]*>/g,'').replace(/&#9472;/g,'-').replace(/&#\d+;/g,'').replace(/\s+/g,' ').trim();
console.log(JSON.stringify(tbody.children.map(r=>{const c=r.children[1]||{style:{}};
  return {text:strip(c.innerHTML), pad:c.style.paddingLeft||'css'};})));
"""


def run_render(fe, view, expanded):
    funcs = []
    for name in ("groupAttempts", "attemptSummary", "foldRounds"):
        m = re.search(r"^function " + name + r"\(.*?^}\n", fe, re.M | re.S)
        if not m:
            return None
        funcs.append(m.group(0))
    i = fe.find("  // Rows UNDER a slice line")
    if i == -1:
        i = fe.find("const renderSliceLines")
    j = fe.find("\n  };", fe.find("const renderSliceLines"))
    js = (HARNESS.replace("__FUNCS__", "\n".join(funcs))
          .replace("__EXP__", json.dumps(expanded))
          .replace("__BODY__", fe[i:j + 4])
          .replace("__VIEW__", json.dumps(view)))
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as f:
        f.write(js)
    r = subprocess.run(["node", f.name], capture_output=True, text=True, timeout=60)
    os.unlink(f.name)
    if r.returncode:
        print(r.stderr[-600:])
        return None
    return json.loads(r.stdout.strip().splitlines()[-1])


def render_check(fe):
    H = lambda kind, st, start, att, res="": {"kind": kind, "label": kind, "status": st,
        "start": start, "duration_s": 10, "result": res, "attempt": att, "live": False}
    two = {"slices": [{"sid": "s6", "phase": "gate", "history": [
        H("authoring", "failed", 1, 1), H("gate", "running", 2, 1),
        H("authoring", "running", 3, 2)]}]}
    rows = run_render(fe, two, {"b/s6": True, "b/s6#a1": True})
    check("render harness ran (2 attempts)", rows is not None)
    if rows:
        txt = [r["text"] for r in rows]
        a1 = next((k for k, t in enumerate(txt) if t.startswith("Attempt 1")), -1)
        a2 = next((k for k, t in enumerate(txt) if "attempt 2 of 2" in t), -1)
        check("attempts oldest first: Attempt 1 row above the attempt 2 divider",
              a1 != -1 and a2 != -1 and a1 < a2, True)
        check("attempt rows one level under the slice line (3.2rem)",
              [rows[a1]["pad"], rows[a2]["pad"]] if min(a1, a2) >= 0 else None,
              ["3.2rem", "3.2rem"])
        check("attempt 1 runs (author, gate) nested under it (4.6rem)",
              [(r["text"], r["pad"]) for r in rows[a1 + 1:a2]],
              [("authoring", "4.6rem"), ("gate", "4.6rem")])
        check("attempt 2 runs nested under its divider (4.6rem)",
              [(r["text"], r["pad"]) for r in rows[a2 + 1:]], [("authoring", "4.6rem")])
    one = {"slices": [{"sid": "s1", "phase": "gate", "history": [
        H("authoring", "done", 1, 1), H("gate", "running", 2, 1)]}]}
    rows = run_render(fe, one, {"b/s1": True})
    check("render harness ran (1 attempt)", rows is not None)
    if rows:
        check("single attempt: no attempt row, runs one level under the slice (3.2rem)",
              [(r["text"], r["pad"]) for r in rows[1:]],
              [("authoring", "3.2rem"), ("gate", "3.2rem")])


def main():
    home = tempfile.mkdtemp(prefix="dash-indent-")
    os.environ["HOME"] = home
    os.symlink(BIN, os.path.join(home, "bin"))     # the API imports ~/bin/ollama-queue.py
    ld = SourceFileLoader("qapi_indent", str(SRC))
    m = importlib.util.module_from_spec(importlib.util.spec_from_loader("qapi_indent", ld))
    ld.exec_module(m)
    fe = m.FRONTEND_HTML
    css = re.search(r"tr\.queue-child td:nth-child\(2\) \{ padding-left: ([\d.]+)rem", fe)
    parent = float(css.group(1)) if css else None
    check("slice-line (tr.queue-child) CSS indent found", parent is not None)
    i = fe.find("const renderSliceLines")
    j = fe.find("\n  };", i)
    body = fe[i:j] if i != -1 else ""
    check("renderSliceLines found", bool(body))
    kid = (re.search(r"const SLICE_KID_N = ([\d.]+)", fe)
           or re.search(r"const SLICE_KID = '([\d.]+)rem'", fe))
    kidv = float(kid.group(1)) if kid else None
    check("SLICE_KID defined", kidv is not None)
    if kidv is not None and parent is not None:
        check(f"child indent {kidv}rem is one level deeper than slice line {parent}rem",
              kidv - parent >= 1.0)
    vals = [float(v) for v in re.findall(r"padding-?[lL]eft[:=]\s*'?\s*([\d.]+)rem", body)]
    low = [v for v in vals if kidv is None or v < kidv]
    check("no row under a slice line is indented less than SLICE_KID", low, [])
    check("stage header / single-run row / attempt rows use SLICE_KID",
          body.count("SLICE_KID") >= 4)
    render_check(fe)
    with tempfile.TemporaryDirectory() as td:
        ok_all = True
        for n, sc in enumerate(re.findall(r"<script>(.*?)</script>", fe, re.S)):
            f = Path(td) / f"s{n}.js"
            f.write_text(sc)
            r = subprocess.run(["node", "--check", str(f)], capture_output=True, text=True)
            if r.returncode:
                ok_all = False
                print(r.stderr[-400:])
        check("page script still parses (node --check)", ok_all)
    print(f"\n--- {len(FAILS)} failed ---")
    return 1 if FAILS else 0


if __name__ == "__main__":
    sys.exit(main())
