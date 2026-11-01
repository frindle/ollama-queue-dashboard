#!/usr/bin/env python3
"""Synthetic: /docsum routing, log suppression and nav link in ollama-queue-api.py. Stub proxy only.
Run: python3 tests/test_docsum_route.py"""
import importlib.util, io, os, sys, types
from pathlib import Path

SRC = Path(os.environ.get("DASHBOARD_SRC") or Path(__file__).resolve().parent.parent / "src")
sys.argv = ["x"]
sys.path.insert(0, str(SRC))
spec = importlib.util.spec_from_file_location("api_docsum_route", str(SRC / "ollama-queue-api.py"))
api = importlib.util.module_from_spec(spec)
spec.loader.exec_module(api)

calls = []
stub = types.ModuleType("docsum_proxy")
stub.proxy = lambda h: calls.append((h.command, h.path))
sys.modules["docsum_proxy"] = stub
H = api.Handler if hasattr(api, "Handler") else None
if H is None:
    H = next(v for v in vars(api).values() if isinstance(v, type) and hasattr(v, "log_message") and hasattr(v, "do_GET"))


def mk(path, cmd):
    h = H.__new__(H)
    h.path, h.command = path, cmd
    h.client_address = ("127.0.0.1", 1)
    return h


def test_routing():
    for path in ("/docsum", "/docsum/", "/docsum/x?t=SYNTH"):
        for cmd, fn in (("GET", "do_GET"), ("POST", "do_POST")):
            calls.clear()
            getattr(mk(path, cmd), fn)()
            assert calls == [(cmd, path)], (path, cmd, calls)


def test_not_routed():
    h = mk("/docsumx", "GET")
    h._text = lambda *a, **k: calls.append("text")
    calls.clear()
    try:
        h.do_GET()
    except Exception:
        pass
    assert not any(isinstance(c, tuple) for c in calls)


def test_log_suppressed():
    old, buf = sys.stderr, io.StringIO()
    sys.stderr = buf
    try:
        mk("/docsum/?t=SYNTH", "GET").log_message("%s", "GET /docsum/?t=SYNTH")
        assert buf.getvalue() == ""
        mk("/api/jobs", "GET").log_message("%s", "GET /api/jobs")
        assert "/api/jobs" in buf.getvalue()
    finally:
        sys.stderr = old


def test_nav_link():
    html = api.FRONTEND_HTML
    assert '<a href="/docsum/">Docs &rarr;</a>' in html
    assert html.index('<a href="/chat">') < html.index('<a href="/docsum/">')


if __name__ == "__main__":
    for n, f in list(globals().items()):
        if n.startswith("test_"):
            f()
            print("ok  ", n)
