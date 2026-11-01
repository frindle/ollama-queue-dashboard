"""Frozen tests: queue API /docsum forwarder. Run: cd tests && python3 test_docsum_proxy_forward.py"""
import io
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))
import docsum_proxy as dp

SECRET = "boom-secret-text"


class FakeH:
    def __init__(self, method, path, headers, body=b""):
        self.command, self.path, self.headers = method, path, headers
        self.rfile, self.wfile = io.BytesIO(body), io.BytesIO()
        self.sent, self.hdrs = [], []

    def send_response(self, code):
        self.sent.append(code)

    def send_header(self, k, v):
        self.hdrs.append((k, v))

    def end_headers(self):
        pass


class FakeResp:
    status = 200

    def __init__(self, hdrs):
        self.h = hdrs

    def getheader(self, k):
        return self.h.get(k)

    def read(self):
        return b"synthetic-bytes"


class FakeConn:
    calls = []
    boom = False
    resp_headers = {"Content-Type": "text/plain", "Set-Cookie": "sid=abc; Path=/", "Location": "./", "X-Secret": "no"}

    def __init__(self, host, port, timeout=None):
        FakeConn.calls.append(("init", host, port))

    def request(self, method, path, body=None, headers=None):
        if FakeConn.boom:
            raise OSError(SECRET)
        data = body or b""
        FakeConn.calls.append((method, path, data, dict(headers)))

    def getresponse(self):
        return FakeResp(FakeConn.resp_headers)

    def close(self):
        pass


def last():
    assert FakeConn.calls, "upstream never contacted"
    return FakeConn.calls[-1]


def run(method, path, headers, body=b""):
    FakeConn.calls, FakeConn.boom = [], False
    h = FakeH(method, path, headers, body)
    dp.proxy(h, conn_factory=FakeConn)
    return h


def test_get_strips_prefix_keeps_query_and_returns_body():
    h = run("GET", "/docsum/api/jobs?x=1", {"Host": "q.example.test", "Cookie": "sid=abc"})
    assert last()[:2] == ("GET", "/api/jobs?x=1"), FakeConn.calls
    assert h.sent == [200] and h.wfile.getvalue() == b"synthetic-bytes"
    assert ("Content-Length", "15") in h.hdrs
    run("GET", "/docsum/", {})
    assert last()[1] == "/"
    h = run("GET", "/docsum", {})
    assert h.sent == [308] and ("Location", "/docsum/") in h.hdrs and len(FakeConn.calls) == 0


def test_post_streams_body_and_checks_origin():
    h = run("POST", "/docsum/api/upload", {"Host": "q.example.test", "Origin": "https://q.example.test", "Content-Length": "9"}, b"123456789")
    call = last()
    assert call[0] == "POST" and call[2] == b"123456789" and call[3]["Origin"] == "http://127.0.0.1:7690" and h.sent == [200]
    h = run("POST", "/docsum/api/upload", {"Host": "q.example.test", "Origin": "https://evil.example.test", "Content-Length": "1"}, b"x")
    assert h.sent == [403] and json.loads(h.wfile.getvalue()) == {"error": "E_CROSS_ORIGIN"} and len(FakeConn.calls) == 0
    assert run("POST", "/docsum/api/upload", {"Origin": "", "Content-Length": str(dp.MAX_BODY + 1)}).sent == [413]
    assert run("POST", "/docsum/api/upload", {"Content-Length": "abc"}).sent == [413]


def test_response_header_allowlist_and_no_tunnel_headers_forwarded():
    h = run("GET", "/docsum/", {"CF-Connecting-IP": "203.0.113.5", "X-Forwarded-For": "203.0.113.5", "Cookie": "sid=abc"})
    names = [k for k, _ in h.hdrs]
    assert "Set-Cookie" in names and "Location" in names and "X-Secret" not in names
    sent = last()[3]
    assert "CF-Connecting-IP" not in sent and "X-Forwarded-For" not in sent and sent["Host"] == "127.0.0.1:7690"


def test_upstream_failure_is_opaque_502():
    FakeConn.calls = []
    FakeConn.boom = True
    h = FakeH("GET", "/docsum/api/jobs", {})
    dp.proxy(h, conn_factory=FakeConn)
    FakeConn.boom = False
    assert h.sent == [502] and json.loads(h.wfile.getvalue()) == {"error": "E_DOCSUM_DOWN"}
    assert SECRET.encode() not in h.wfile.getvalue() and all(SECRET not in str(v) for _, v in h.hdrs)


TESTS = [test_get_strips_prefix_keeps_query_and_returns_body, test_post_streams_body_and_checks_origin, test_response_header_allowlist_and_no_tunnel_headers_forwarded, test_upstream_failure_is_opaque_502]

if __name__ == "__main__":
    order = TESTS[::-1] if os.environ.get("TEST_ORDER") == "reverse" else TESTS
    bad = 0
    for t in order:
        try:
            t()
            print(f"RESULT {t.__name__} PASS")
        except Exception as e:
            bad += 1
            print(f"RESULT {t.__name__} FAIL {type(e).__name__}: {e}")
    sys.exit(1 if bad else 0)
