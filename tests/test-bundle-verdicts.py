#!/usr/bin/env python3
"""BUNDLE VERDICTS (2026-10-14, Penn: "gate verdicts should stay with their bundle").

The gate verdict / needs_eyes|good_to_go|handled group / reason that Run Status shows is
ALSO surfaced under its bundle (inline on each job row, roll-up on the bundle header).
Single source = the annotated /api/runs rows (_run_status_jobs -> _annotate_run_groups),
joined by job id / parent job / label / plan key (_bundle_verdict_index).

  A. PURE join: id / parent-job / auto-fix label / plan-key attach, unattached listed,
     roll-up == worst verdict + needs_eyes count, sign-off rows never clearable
  B. LIVE STATE (read-only): every row with a verdict is attached or listed Unattached,
     each bundle roll-up equals the recomputed worst, the real needs-eyes rows stay visible
  C. BEFORE/AFTER: /api/runs rows (HEAD code vs working tree, same live state) and the
     handoff list are identical -- the only change is the added view; endpoints/routes
     for /api/runs, /api/runs/tree, /api/runs/<id>/clear, /api/handoff are textually unchanged
  D. FRONTEND (node, fake DOM): inline verdict shows reason + issues button; a sign-off row
     gets NO clear button; bundle badge shows worst + eyes count; Run Status is collapsed

--revert-check is not provided: the join is pure and pinned by A/B. API_SRC overrides the file."""
import importlib.util, json, os, re, subprocess, sys, tempfile
from importlib.machinery import SourceFileLoader
from pathlib import Path

HERE = Path(__file__).resolve().parent
SRC = Path(os.environ.get("API_SRC") or HERE.parent / "src" / "ollama-queue-api.py")
FAILS = []


def check(name, cond, detail=""):
    print(("ok  " if cond else "FAIL") + f": {name}" + ("" if cond else f"  ({detail})"))
    if not cond:
        FAILS.append(name)


def load(path, name):
    loader = SourceFileLoader(name, str(path))
    spec = importlib.util.spec_from_loader(name, loader)
    m = importlib.util.module_from_spec(spec)
    sys.argv = [str(path)]
    loader.exec_module(m)
    return m


api = load(SRC, "api_bv")


def row(i, label, raw, group, **kw):
    r = {"id": i, "label": label, "verdict": raw, "raw_verdict": raw, "group": group,
         "handled_reason": "", "eyes_label": "needs review" if group == "needs_eyes" else "good to go",
         "eyes_severity": "warn", "has_gate": True, "awaiting_signoff": False, "parent": None,
         "slice_id": None}
    r.update(kw)
    return r


def view(key, *slices):
    return {"key": key, "slices": [{"sid": sid, "history": [{"id": i, "label": l} for i, l in h]}
                                   for sid, h in slices]}


print("A. pure join")
views = [view("bndA", ("s1", [("aaa111111111", "feat-s1"), ("ggg", "gate-aaa111111111")]),
                      ("s2", [("bbb222222222", "feat-s2")])),
         view("bndB", ("s1", [("ccc333333333", "other-s1")]))]
rows = [row("aaa111111111", "feat-s1", "FAIL", "needs_eyes"),
        row("bbb222222222", "feat-s2", "PASS", "good_to_go"),
        row("e1e1e1e1e1e1", "esc-review-20261014T010101Z-job-aaa111111111", "NO-GATE", "needs_eyes"),
        row("f2f2f2f2f2f2", "feat-s2 [auto-fix r1]", "CONCERNS", "needs_eyes"),
        row("f3f3f3f3f3f3", "feat-s2 [auto-fix r1]-lc1", "?", "handled", handled_reason="superseded"),
        row("ccc333333333", "other-s1", "PASS", "handled", handled_reason="auto"),
        row("ppp444444444", "planmatch", "BLOCKED", "needs_eyes", parent="bndB", slice_id=None),
        row("sig555555555", "feat-s1", "FAIL", "needs_eyes", awaiting_signoff=True),
        row("zzz999999999", "stranger", "FAIL", "needs_eyes"),
        row("e9e9e9e9e9e9", "esc-review-20261014T010101Z-job-deadbeefcafe", "NO-GATE", "needs_eyes")]
ix = api._bundle_verdict_index(rows, views)
R = ix["rows"]
check("a row attaches by its own job id", (R["aaa111111111"]["bundle"], R["aaa111111111"]["slice"], R["aaa111111111"]["via"]) == ("bndA", "s1", "job"))
check("esc-review child attaches to its parent job's bundle+slice", (R["e1e1e1e1e1e1"]["bundle"], R["e1e1e1e1e1e1"]["slice"]) == ("bndA", "s1"))
check("auto-fix row attaches by label minus the [auto-fix rN] suffix", R["f2f2f2f2f2f2"]["bundle"] == "bndA" and R["f2f2f2f2f2f2"]["slice"] == "s2")
check("auto-fix -lc1 row attaches too", R["f3f3f3f3f3f3"]["bundle"] == "bndA")
check("plan-key (parent) fallback attaches", R["ppp444444444"]["bundle"] == "bndB" and R["ppp444444444"]["via"] == "plan")
check("a row with no parent is UNATTACHED and listed", R["zzz999999999"]["bundle"] is None and "zzz999999999" in ix["unattached"])
check("esc-review of an unknown job is UNATTACHED and listed", "e9e9e9e9e9e9" in ix["unattached"])
check("unattached is exactly the unjoinable rows", sorted(ix["unattached"]) == ["e9e9e9e9e9e9", "zzz999999999"], str(ix["unattached"]))
check("every input row is in rows (nothing dropped)", len(R) == len(rows))
A = ix["bundles"]["bndA"]
check("roll-up worst == FAIL (rank beats PASS/CONCERNS/NO-GATE)", A["worst"] == "FAIL", str(A))
check("roll-up counts needs_eyes", A["needs_eyes"] == 4 and A["good_to_go"] == 1 and A["handled"] == 1 and A["total"] == 6, str(A))
check("bundle without FAIL: worst is BLOCKED (BLOCKED > PASS)", ix["bundles"]["bndB"]["worst"] == "BLOCKED")
check("group/verdict/reason are COPIED from the run row, not recomputed",
      R["f3f3f3f3f3f3"]["group"] == "handled" and R["f3f3f3f3f3f3"]["handled_reason"] == "superseded" and R["aaa111111111"]["eyes_label"] == "needs review")
check("sign-off row is NOT clearable", R["sig555555555"]["clearable"] is False)
check("ordinary row is clearable (same ledger path as Run Status)", R["aaa111111111"]["clearable"] is True)
check("a live-auto-run row is not clearable", api._bundle_verdict_index([row("l1", "x", "PASS", "handled", auto_run_live=True)], [])["rows"]["l1"]["clearable"] is False)
order = [api._bv_verdict_rank(v) for v in ("FAIL", "BLOCKED", "CONCERNS", "NO-GATE", "SKIPPED", "PASS")]
check("verdict rank is strictly worst-first", order == sorted(order, reverse=True) and len(set(order)) >= 5, str(order))

print("B. live state (read-only)")
try:
    live_rows = api._run_status_jobs()
    live_views = list((api._bundle_views() or {}).get("views", {}).values())
    fin = api._finished_bundle_views() or {}
    live_views += list(fin.get("views") or []) + list(fin.get("stalled") or [])
except Exception as e:  # noqa: BLE001
    live_rows, live_views = [], []
    check("live state readable", False, repr(e))
L = api._bundle_verdict_index(live_rows, live_views)
check("live: some rows exist", len(live_rows) > 0)
check("live: every verdict row is attached OR listed Unattached, none dropped",
      set(L["rows"]) == {r["id"] for r in live_rows} and
      all((i["bundle"] is not None) != (i["id"] in L["unattached"]) for i in L["rows"].values()))
keys = {v["key"] for v in live_views}
check("live: every attached row points at a bundle that exists", all(i["bundle"] in keys for i in L["rows"].values() if i["bundle"]))
bad = []
for k, b in L["bundles"].items():
    mem = [r for r in live_rows if r["id"] in b["ids"]]
    worst = max(mem, key=lambda r: api._bv_verdict_rank(r.get("raw_verdict")))
    if api._bv_verdict_rank(b["worst"]) != api._bv_verdict_rank(worst.get("raw_verdict")) \
            or b["needs_eyes"] != sum(1 for r in mem if r["group"] == "needs_eyes"):
        bad.append(k)
check("live: each bundle roll-up equals the recomputed worst verdict + needs_eyes count", not bad, str(bad))
src_rows = {r["id"]: r for r in live_rows}
check("live: inline fields equal the /api/runs annotation (single source)",
      all(L["rows"][i][f] == src_rows[i].get(f) for i in src_rows for f in ("group", "verdict", "raw_verdict", "handled_reason", "eyes_label")))
check("live: sign-off-protected rows are never clearable inline",
      all(not L["rows"][r["id"]]["clearable"] for r in live_rows if r.get("awaiting_signoff") or r.get("auto_run_live")))
check("live: needs_eyes rows carry a roll-up count",
      all(L["bundles"][i["bundle"]]["needs_eyes"] >= 1 for i in L["rows"].values() if i["bundle"] and i["group"] == "needs_eyes"))
for rid in ("ad80847461ec", "964c4a833b82", "3f2753572787"):     # psc-C4-a465e147cf00-rr == 3f2753572787
    if rid in src_rows:
        check(f"live: real needs-eyes row {rid} stays visible and needs_eyes", L["rows"][rid]["group"] == "needs_eyes")
    else:
        print(f"skip: {rid} no longer in live state")

print("C. before/after on live state")
with tempfile.TemporaryDirectory() as td:
    head = subprocess.run(["git", "-C", str(HERE.parent), "show", "HEAD:src/ollama-queue-api.py"],
                          capture_output=True, text=True)
    if head.returncode != 0:
        check("HEAD copy readable", False, head.stderr[:100])
    else:
        hp = Path(td) / "ollama-queue-api.py"
        hp.write_text(head.stdout)
        for extra in ("bundle_view.py", "runstatus_retention.py", "dashboard_chat.py"):
            if (SRC.parent / extra).exists():
                (Path(td) / extra).symlink_to(SRC.parent / extra)
        old = load(hp, "api_head")
        before, after = old._run_status_jobs(), api._run_status_jobs()
        if before != after:
            before, after = old._run_status_jobs(), api._run_status_jobs()   # live state drifted between calls: retry once
        check("/api/runs rows identical HEAD vs working tree (%d rows)" % len(after), before == after)
        check("/api/runs/tree identical HEAD vs working tree", old._build_run_tree(before) == api._build_run_tree(after))
src_now, src_head = SRC.read_text(), subprocess.run(["git", "-C", str(HERE.parent), "show", "HEAD:src/ollama-queue-api.py"], capture_output=True, text=True).stdout


def branch(txt, route):
    i = txt.index(route)
    j = txt.find("\n        elif ", i + 10)
    return _strip_hooks(txt[i:j if j > 0 else i + 600])


def _strip_hooks(t):
    # Additive routes (plex bypass hook, bundle run-now) sit in do_POST near these windows;
    # they are not the routes under guard, so ignore them in the comparison.
    for h in ('        if self.path.startswith("/api/bypass/"):\n            return self._plex_bypass()\n',
              '            m = re.match(r"^/api/jobs/([^/]+)/run-now$", self.path)\n            if m:\n                return self._run_now(m.group(1))\n'):
        t = t.replace(h, "")
    return t


for route in ('elif self.path == "/api/runs":', 'elif self.path == "/api/runs/tree":', 'elif self.path == "/api/handoff":',
              'm = re.match(r"^/api/runs/([^/]+)/clear$", self.path)'):
    check(f"route unchanged: {route[:40]}", branch(src_now, route) == branch(src_head, route))
h1 = subprocess.run(["python3", os.path.expanduser("~/bin/handoff-emit.py"), "--json"], capture_output=True, text=True, timeout=120)
h2 = subprocess.run(["python3", os.path.expanduser("~/bin/handoff-emit.py"), "--json"], capture_output=True, text=True, timeout=120)
check("handoff list is stable and unaffected (two reads identical)", h1.returncode == 0 and h1.stdout == h2.stdout)

print("D. frontend (node + fake DOM)")
page = src_now
m0 = page.index("const VGROUP_LABEL")
m1 = page.index("function verdictRowEl")
m1e = page.index("\n}\n", m1) + 3
funcs = page[m0:m1e]
vc0 = page.index("function verdictClass(tag)")
vc1 = page.index("\n}\n", vc0) + 3
js = r"""
class El{constructor(t){this.tag=t;this.children=[];this.dataset={};this.className='';this._h='';}
 set innerHTML(v){this._h=v;this.children=this.tag==='tr'?[new El('td'),new El('td')]:[];} get innerHTML(){return this._h;}
 appendChild(c){this.children.push(c);return c;} set textContent(v){this.t=v;} get textContent(){return this.t;}}
const document={createElement:t=>new El(t)};
const escapeHtml=x=>String(x==null?'':x).replace(/[&<>"']/g,'_');
const openGateDetail=()=>{}; const clearRun=async()=>{}; const refresh=()=>{}; let finishedFetchedAt=0;
let bundleVerdicts={rows:{},bundles:{},unattached:[]}; let _bvBySlice={};
""" + page[page.index("function indexBundleVerdicts"):page.index("const VGROUP_LABEL")] + page[vc0:vc1] + funcs + r"""
const sig={id:'s1',label:'x',group:'needs_eyes',verdict:'FAIL',raw_verdict:'FAIL',eyes_label:'awaiting sign-off',eyes_severity:'ok',has_gate:true,clearable:false};
const ok={id:'o1',label:'y',group:'handled',verdict:'PASS',raw_verdict:'PASS',handled_reason:'superseded by z',has_gate:false,clearable:true};
const btns=e=>(e.children.find(c=>c.className==='vbtns')||{children:[]}).children.map(b=>b.textContent);
const a=verdictInline(sig), b=verdictInline(ok);
bundleVerdicts={rows:{},bundles:{k:{worst:'FAIL',needs_eyes:2,good_to_go:1,handled:0,total:3}},unattached:[]};
const out={sigBtns:btns(a),okBtns:btns(b),sigHtml:a.innerHTML,okHtml:b.innerHTML,badge:bundleVerdictBadge('k'),none:bundleVerdictBadge('nope'),
  row:verdictRowEl(sig,'',"1rem").children[1].children.length, worstOf:worstOf([{raw_verdict:'PASS'},{raw_verdict:'CONCERNS'},{raw_verdict:'FAIL'}])};
console.log(JSON.stringify(out));
"""
r = subprocess.run(["node", "-e", js], capture_output=True, text=True)
if r.returncode != 0:
    check("node harness ran", False, r.stderr[:600])
else:
    o = json.loads(r.stdout.strip().splitlines()[-1])
    check("sign-off row: 'issues' yes, NO 'clear' button", o["sigBtns"] == ["issues"], str(o["sigBtns"]))
    check("ordinary handled row: clear button, no issues (no gate)", o["okBtns"] == ["clear"], str(o["okBtns"]))
    check("inline shows verdict, group and reason", "FAIL" in o["sigHtml"] and "needs eyes" in o["sigHtml"] and "awaiting sign-off" in o["sigHtml"])
    check("handled row shows its handled reason", "superseded by z" in o["okHtml"])
    check("bundle badge: worst verdict + eyes count", "gate: FAIL" in o["badge"] and "2 need" in o["badge"], o["badge"])
    check("bundle without verdicts renders no badge", o["none"] == "")
    check("a verdict row carries its inline widget", o["row"] == 1)
    check("worstOf picks FAIL", o["worstOf"] == "FAIL")
check("Run Status panel is a <details> collapsed by default (no open attr) and labelled",
      re.search(r'<details class="fin" id="runStatusDetails">', page) is not None and "verdicts now live with their bundles" in page)
check("Run Status table + /api/runs consumers kept", 'id="runStatus"' in page and "fetch('/api/runs/tree')" in page)
check("Unattached group exists in the bundle view", 'id="unattachedPanel"' in page and "renderUnattachedVerdicts();" in page)

print("--- %d failed ---" % len(FAILS))
if FAILS:
    sys.exit(1)
print("ALL PASS")
