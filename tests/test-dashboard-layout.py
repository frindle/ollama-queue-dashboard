#!/usr/bin/env python3
"""Dashboard layout (2026-10-05 redesign): the page RENDERED against fixture data.

The real FRONTEND_HTML is served by a throwaway HTTP server whose /api/* routes return
fixtures (a running bundle, a failed bundle, a parked bundle, pending bundles,
standalone pending/paused/failed jobs, finished bundles across two days). Headless
Chrome loads it, runs the page's own JS, and dumps the DOM. We then assert:

  * the summary strip names the running job, its model, host and tok/s;
  * Needs attention holds exactly the failed bundle, the parked bundle and the failed
    standalone job, each with its one-line reason and its primary action;
  * the Queue holds the healthy bundles and jobs (and none of the stuck ones);
  * Finished bundles is collapsed, grouped by day, and names the failed one;
  * no action was lost: every data-* control hook and every API endpoint the page had
    before the redesign is still in the page, and every hook the fixtures can reach is
    actually rendered;
  * the page never scrolls sideways at 1280px or at phone width;
  * rendering fires no POST/DELETE (looking never acts).

Needs Google Chrome (CHROME=/path overrides). --shots DIR also saves screenshots.
Run: python3 tests/test-dashboard-layout.py [--shots DIR]
"""
import html.parser
import importlib.util
import json
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib.machinery import SourceFileLoader
from pathlib import Path

SRC_DIR = Path(os.environ.get("DASHBOARD_SRC") or Path(__file__).resolve().parent.parent / "src")
PIPELINE_BIN = Path(os.environ.get("OLLAMA_PIPELINE_BIN") or Path.home() / "bin")
CHROME = os.environ.get("CHROME") or "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
FAILS = []
# load_api() points HOME at a sandbox for the API import; Chrome must keep the real one
# (with a fake HOME it never finishes starting on macOS).
REAL_ENV = dict(os.environ)

# The control hooks and endpoints the page had BEFORE the redesign (inventoried from
# the pre-redesign FRONTEND_HTML). None of them may disappear.
BEFORE_HOOKS = {
    "data-bottom", "data-down", "data-fb-age", "data-fb-less", "data-fb-more", "data-kill",
    "data-plan-cancel", "data-plan-down", "data-plan-hold", "data-plan-last", "data-plan-pause",
    "data-plan-promote", "data-plan-resume", "data-plan-up", "data-promote-front",
    "data-promote-group", "data-remove", "data-resume", "data-top", "data-up"}
BEFORE_ENDPOINTS = {
    "/api/bundle-history?days=", "/api/bundle-views", "/api/hosts", "/api/jobs", "/api/jobs/",
    "/api/jobs/move", "/api/jobs/move-group", "/api/runs/", "/api/runs/tree",
    "/api/settings/hosts", "/api/web-search-usage"}
BEFORE_IDS = {"addHostRow", "clearFinished", "closeLivelog", "hostSettings", "hostSettingsBody",
              "hosts", "hostsCfgPath", "hostsMsg", "jobs", "livelogBackdrop", "livelogContent",
              "livelogModal", "livelogTitle", "runStatus", "saveHosts", "webSearchUsage",
              "webSearchUsageBody"}


def check(name, got, want=True):
    ok = got == want
    print(("ok  " if ok else "FAIL") + f": {name}" + ("" if ok else f"  (got {got!r}, want {want!r})"))
    if not ok:
        FAILS.append(name)


def load_api():
    home = tempfile.mkdtemp(prefix="dash-layout-")
    os.environ["HOME"] = home
    os.symlink(PIPELINE_BIN, os.path.join(home, "bin"))  # the API imports ~/bin/ollama-queue.py
    ld = SourceFileLoader("qapi_layout", str(SRC_DIR / "ollama-queue-api.py"))
    m = importlib.util.module_from_spec(importlib.util.spec_from_loader("qapi_layout", ld))
    ld.exec_module(m)
    return m


def row(id_, label, status, **kw):
    base = {"id": id_, "label": label, "status": status, "model": "qwen3:14b", "host_pref": "unraid",
            "lane": None, "pid": None, "exit_code": None, "iteration": None, "max_iters": None,
            "tok_s": None, "phase": None, "elapsed_s": None, "paused_s": None, "group_key": None,
            "group_pending": 0, "plan_bundle": False, "plan_done_slices": [], "plan_incomplete": False,
            "needs_attention": False, "wait_reason": None}
    base.update(kw)
    return base


def bundle(key, rank, seq, total, done, status, **kw):
    return dict(group_key=key, bundle=key, plan_bundle=True, bundle_rank=rank, plan_seq=seq,
                plan_total=total, plan_done=done, plan_status=status, plan_incomplete=True, **kw)


def fixtures(now):
    jobs = [
        row("r1", "auto-author-fx-run-s1-parse", "running", model="qwen3.8:27b-q4_K_M", lane="studio",
            pid=4242, tok_s=41.8, iteration=3, max_iters=12, elapsed_s=425, display_seq=0,
            **bundle("fx-run", 0, 0, 3, 0, "running", group_pending=2)),
        row("r2", "fx-run-s2-mask", "pending", display_seq=1, **bundle("fx-run", 0, 0, 3, 0, "running", group_pending=2)),
        row("r3", "fx-run-s3-hash", "pending", display_seq=2, **bundle("fx-run", 0, 0, 3, 0, "running", group_pending=2)),
        row("p1", "fx-pend-s1-schema", "pending", display_seq=3,
            wait_reason="queued behind committed bundle fx-run (bundles do not interleave)",
            **bundle("fx-pend", 1, 3, 4, 1, "pending", group_pending=1)),
        row("p2", "fx-pend2-s1-route", "pending", display_seq=4, **bundle("fx-pend2", 2, 4, 2, 0, "pending", group_pending=1)),
        row("f1", "auto-author-fx-failed-s2-guard", "failed", exit_code=1, needs_attention=True,
            failure_class="verify", failure_detail="verify exited 1: test_guard AssertionError",
            display_seq=5, **bundle("fx-failed", 3, 5, 5, 3, "planned", group_pending=0)),
        row("f2", "fx-failed-s3-wire", "planned", display_seq=6, **bundle("fx-failed", 3, 5, 5, 3, "planned", group_pending=0)),
        row("h1", "fx-parked-s1-a", "held", display_seq=7, wait_reason="held by operator since 09:12",
            **bundle("fx-parked", 4, 7, 3, 1, "held", group_pending=0)),
        row("h2", "fx-parked-s2-b", "held", display_seq=8, **bundle("fx-parked", 4, 7, 3, 1, "held", group_pending=0)),
        row("s1", "bench-qwen-smoke", "pending", display_seq=9, bundle_rank=5),
        row("s2", "long-refactor-job", "paused", display_seq=10, bundle_rank=5, elapsed_s=900),
        row("s3", "plan-gen-fx-sidecar-r1", "failed", display_seq=11, bundle_rank=5, exit_code=1,
            failure_class="plan-gen", failure_detail="plan generation produced no slices",
            plan_incomplete=True, needs_attention=True),
        row("d1", "old-done-job", "done", display_seq=12, bundle_rank=6),
    ]

    def sl(sid, phase, attention=False, detail="", hist=()):
        return {"sid": sid, "title": "", "status": phase, "phase": phase, "detail": detail,
                "since": None, "elapsed_s": None, "active": False, "attention": attention,
                "history": list(hist)}

    h = lambda i, kind, st, res: {"kind": kind, "id": i, "label": f"{kind}-{i}", "status": st,
                                  "start": now - 3600, "duration_s": 61.0, "result": res,
                                  "log": i, "live": False, "attempt": 1}
    views = {"fx-failed": {"key": "fx-failed", "through": 3, "total": 5, "current": None, "slices": [
        sl("s1-base", "done"),
        sl("s2-guard", "escalated", True, "escalated: vacuous must_contain gate",
           [h("f1", "coding", "failed", "gate FAIL")]),
        sl("s3-wire", "pending")]}}
    activity = [{"kind": "gpu", "id": "r1", "label": "auto-author-fx-run-s1-parse", "group_key": "fx-run",
                 "display": None, "model": "qwen3.8:27b-q4_K_M", "host": "studio", "elapsed_s": 425.0},
                {"kind": "cpu", "what": "verify-relevance mutants 3/12", "wt": "fx-run", "elapsed_s": 12.0}]
    lt = time.localtime(now)
    yesterday_noon = time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday - 1, 12, 0, 0, 0, 0, -1))
    fin = lambda key, ts, bad=False: {
        "key": key, "through": 2, "total": 2, "current": None, "unit": "slices", "finished": True,
        "last_activity": ts, "first_activity": ts - 600, "runs": 4,
        "slices": [sl("s1-a", "done"), sl("s2-b", "escalated" if bad else "done", bad, "gate FAIL")]}
    history = {"views": [fin("fin-today-ok", now - 30), fin("fin-today-bad", now - 40, True),
                         fin("fin-yesterday", yesterday_noon)],
               "total": 7, "days": 3, "limit": 20, "offset": 0, "has_more": True}
    return {"/api/jobs": jobs,
            "/api/bundle-views": {"views": views, "activity": activity, "active": "fx-run"},
            "/api/bundle-history": history,
            "/api/hosts": {"hosts": []}, "/api/settings/hosts": {"hosts": [], "config_path": "/tmp/x"},
            "/api/runs/tree": [], "/api/web-search-usage": {}}


# Test-only probe appended to the served page: after the first render, record the page
# width vs the viewport (and the widest offenders) on the TOP document's body, so it
# also works when the page runs inside the 390px phone frame below.
PROBE = ("<script>setTimeout(() => { const t = window.parent.document.body;"
         " const iw = window.innerWidth; t.dataset.sw = document.documentElement.scrollWidth; t.dataset.iw = iw;"
         " t.dataset.wide = [...document.querySelectorAll('body *')].filter(e => e.getBoundingClientRect().right > iw + 1)"
         ".slice(0, 6).map(e => e.tagName + '#' + e.id + '.' + String(e.className).replace(/ /g, '.')).join(' ');"
         " }, 2500);</script>")
PHONE = ('<!doctype html><html><body style="margin:0">'
         '<iframe src="/" style="width:390px;height:1700px;border:0;display:block"></iframe></body></html>')


def _serve_forever(d):
    """Child-process mode (--serve DIR). The fixture server must NOT share a process
    with the test: headless Chrome never finished loading from an in-process
    ThreadingHTTPServer, while the same server in its own process answers at once."""
    d = Path(d)
    page = (d / "page.html").read_text()
    fx = json.loads((d / "fx.json").read_text())

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self, body, ctype):
            b = body.encode()
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)

        def do_GET(self):
            path = self.path.split("?")[0]
            if path == "/":
                return self._send(page.replace("</body>", PROBE + "</body>"), "text/html")
            if path == "/phone":
                return self._send(PHONE, "text/html")
            if path in fx:
                return self._send(json.dumps(fx[path]), "application/json")
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_POST(self):
            with open(d / "writes.log", "a") as f:
                f.write(self.command + " " + self.path + "\n")
            self.send_response(500)
            self.send_header("Content-Length", "0")
            self.end_headers()

        do_DELETE = do_POST

    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    (d / "port").write_text(str(srv.server_address[1]))
    srv.serve_forever()


def serve(page, fx, d):
    d = Path(d)
    (d / "page.html").write_text(page)
    (d / "fx.json").write_text(json.dumps(fx))
    proc = subprocess.Popen([sys.executable, __file__, "--serve", str(d)], env=REAL_ENV)
    for _ in range(200):
        if (d / "port").exists() and (d / "port").read_text():
            break
        time.sleep(0.05)
    return proc, f"http://127.0.0.1:{(d / 'port').read_text()}/"


def chrome(url, width, height, extra=()):
    # No --user-data-dir: with a fresh profile dir headless Chrome on macOS stalls for
    # 30s+ at startup; headless already runs on its own throwaway profile.
    r = subprocess.run([CHROME, "--headless=new", "--disable-gpu", "--hide-scrollbars",
                        "--no-first-run", "--no-default-browser-check", "--use-mock-keychain",
                        f"--window-size={width},{height}", "--virtual-time-budget=6000",
                        *extra, url], capture_output=True, text=True, timeout=120,
                       stdin=subprocess.DEVNULL, env=REAL_ENV)
    return r.stdout


class Doc(html.parser.HTMLParser):
    """Element index: id -> inner text, plus every attribute name seen."""
    def __init__(self):
        super().__init__()
        self.stack, self.text_by_id, self.attrs = [], {}, set()
        self.open_by_id = {}

    def handle_starttag(self, tag, attrs):
        d = dict(attrs)
        self.attrs.update(k for k, _ in attrs)
        if tag in ("br", "hr", "input", "col", "meta", "link", "img"):
            return
        self.stack.append(d.get("id"))
        if d.get("id"):
            self.text_by_id[d["id"]] = ""
            self.open_by_id[d["id"]] = "open" in d
            self.hidden = getattr(self, "hidden", {})
            self.hidden[d["id"]] = "hidden" in d

    def handle_endtag(self, tag):
        if tag in ("br", "hr", "input", "col", "meta", "link", "img"):
            return
        if self.stack:
            self.stack.pop()

    def handle_data(self, data):
        for i in self.stack:
            if i:
                self.text_by_id[i] += data


def norm(t):
    return re.sub(r"\s+", " ", t or "").strip()


def main():
    shots = None
    if "--shots" in sys.argv:
        shots = Path(sys.argv[sys.argv.index("--shots") + 1])
        shots.mkdir(parents=True, exist_ok=True)
    if not Path(CHROME).exists():
        print(f"FAIL: Google Chrome not found at {CHROME} (set CHROME=...)")
        return 2
    api = load_api()
    fe = api.FRONTEND_HTML

    # --- static: nothing the page could do before has gone ---------------------------
    hooks_now = set(re.findall(r"<button[^>]*?\b(data-[a-z-]+)", fe))
    bound_now = set(re.findall(r"\[(data-[a-z-]+)\]", fe))
    check("every pre-redesign control hook is still drawn as a button",
          sorted(BEFORE_HOOKS - hooks_now), [])
    check("...and every one still has a handler bound to it",
          sorted(BEFORE_HOOKS - bound_now), [])
    check("every pre-redesign API endpoint is still called",
          sorted(BEFORE_ENDPOINTS - set(re.findall(r"fetch\('(/api/[^']*)'", fe))), [])
    check("every pre-redesign element id still exists",
          sorted(BEFORE_IDS - set(re.findall(r'id="([A-Za-z]+)"', fe))), [])
    check("the Chat link and Clear finished are kept",
          ('href="/chat"' in fe and 'id="clearFinished"' in fe), True)
    check("no external stylesheet or script (dependency-free page)",
          bool(re.search(r"<(script|link)[^>]+(src|href)=\"https?://", fe)), False)
    check("light + dark via prefers-color-scheme", "@media (prefers-color-scheme: dark)" in fe, True)

    now = time.time()
    work = Path(tempfile.mkdtemp(prefix="dash-layout-srv-"))
    srv, url = serve(fe, fixtures(now), work)
    dom = chrome(url, 1280, 1000, ["--dump-dom"])
    check("chrome rendered the page", "attnTable" in dom, True)
    doc = Doc()
    doc.feed(dom)
    t = lambda i: norm(doc.text_by_id.get(i, ""))

    # --- summary strip -----------------------------------------------------------
    now_t = t("sumNow")
    check("summary: running job, model, host, tok/s", all(x in now_t for x in (
        "auto-author-fx-run-s1-parse", "qwen3.8:27b-q4_K_M", "studio", "41.8 tok/s", "7:05")), True)
    check("summary: off-GPU activity is named too", "verify-relevance mutants 3/12" in now_t, True)
    check("summary: queue depth counts pending + paused + planned", t("sumQueue").startswith("Queue 7"), True)
    check("summary: needs-attention count and breakdown",
          t("sumAttn").replace(" ", ""), "Needsattention32failed·1parked")

    # --- needs attention ------------------------------------------------------------
    attn = t("attnTable")
    check("attention: the failed bundle, with the slice that failed",
          ("fx-failed" in attn and "s2-guard: escalated: vacuous must_contain gate" in attn), True)
    check("attention: the parked bundle, with why it waits", ("fx-parked" in attn and "held by operator" in attn), True)
    check("attention: the failed standalone job, with its reason",
          ("plan-gen-fx-sidecar-r1" in attn and "plan generation produced no slices" in attn), True)
    check("attention: healthy work is not in it",
          [k for k in ("fx-run", "fx-pend", "bench-qwen-smoke", "long-refactor-job") if k in attn], [])
    attn_html = dom[dom.find('id="attnTable"'):dom.find('id="attnEmpty"')]
    check("attention: parked bundle offers Resume inline, failed rows offer View log",
          ('class="btn primary" data-plan-resume' in attn_html and
           attn_html.count(">View log</button>") >= 2), True)

    # --- queue ---------------------------------------------------------------------
    q = t("jobs")
    check("queue: running + pending bundles and standalone jobs",
          all(k in q for k in ("fx-run", "fx-pend", "fx-pend2", "bench-qwen-smoke", "long-refactor-job")), True)
    check("queue: stuck work is not duplicated here",
          [k for k in ("fx-failed", "fx-parked", "plan-gen-fx-sidecar-r1") if k in q], [])
    check("queue: the bundle shows done/total and a progress bar",
          ("0/3 slices" in q and 'class="bar"' in dom), True)
    check("queue: a waiting bundle says why", "queued behind committed bundle fx-run" in q, True)
    check("queue: the running row carries model/host/iter/tok/s on one line",
          all(x in q for x in ("qwen3.8:27b-q4_K_M", "studio", "iter 3/12", "41.8 tok/s", "pid 4242")), True)
    check("queue: finished standalone rows stay out", "old-done-job" in q, False)

    # --- finished bundles ----------------------------------------------------------
    fin = t("finishedPanel")
    check("finished: collapsed by default", doc.open_by_id.get("finishedDetails"), False)
    check("finished: grouped by day (Today, Yesterday)",
          ("Today · 2" in fin and "Yesterday · 1" in fin), True)
    check("finished: the failed one is surfaced in the collapsed summary",
          ("1 with failed slices" in t("finishedSummary") and "fin-today-bad" in t("finishedSummary")), True)

    # --- every reachable action hook is actually rendered ----------------------------
    rendered = {a for a in doc.attrs if a.startswith("data-")}
    reachable = BEFORE_HOOKS - {"data-fb-less"}   # fb-less only after "show more" was used
    check("rendered DOM carries every reachable control hook",
          sorted(reachable - rendered), [])
    check("new hooks: View log on rows and on stuck bundles",
          ({"data-log", "data-plan-log"} <= rendered), True)
    check("actions live in overflow menus", dom.count('class="menu"') >= 8, True)
    wl = work / "writes.log"
    check("rendering fired no POST/DELETE", wl.read_text().splitlines() if wl.exists() else [], [])

    # --- no sideways scroll ----------------------------------------------------------
    for w, page_url in ((1280, url), (390, url + "phone")):
        d2 = chrome(page_url, max(w, 500), 1000, ["--dump-dom"])
        msw, miw = re.search(r'data-sw="(\d+)"', d2), re.search(r'data-iw="(\d+)"', d2)
        sw, iw = (int(msw.group(1)), int(miw.group(1))) if msw and miw else (None, None)
        wide = re.search(r'data-wide="([^"]*)"', d2)
        check(f"no horizontal scroll at {w}px (scrollWidth {sw} <= innerWidth {iw})"
              + (f" wide: {wide.group(1)}" if wide and wide.group(1) else ""),
              sw is not None and iw == w and sw <= iw, True)

    if shots:
        for name, w, h, extra in (("layout-1280-light", 1280, 1100, []),
                                  ("layout-1280-dark", 1280, 1100, ["--blink-settings=preferredColorScheme=0"]),
                                  ("layout-390-light", 500, 1700, [])):
            chrome(url + ("phone" if "390" in name else ""), w, h, [f"--screenshot={shots / (name + '.png')}", *extra])
        print(f"screenshots in {shots}")
    srv.terminate()
    print(f"\n--- {len(FAILS)} failed ---")
    return 1 if FAILS else 0


if __name__ == "__main__":
    if len(sys.argv) > 2 and sys.argv[1] == "--serve":
        _serve_forever(sys.argv[2])
    sys.exit(main())
