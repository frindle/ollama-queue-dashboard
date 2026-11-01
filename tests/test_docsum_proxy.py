"""Frozen tests: queue API /docsum proxy header helpers. Run: cd tests && python3 test_docsum_proxy.py"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))
import docsum_proxy as dp


def test_origin_ok_absent_or_empty():
    assert dp.origin_ok({"Host": "q.example.test"}) is True
    assert dp.origin_ok({"Host": "q.example.test", "Origin": ""}) is True
    assert dp.origin_ok({}) is True


def test_origin_ok_same_and_different():
    assert dp.origin_ok({"Host": " Q.Example.Test ", "Origin": "https://q.example.test"}) is True
    assert dp.origin_ok({"Host": "10.9.9.9:7684", "Origin": "http://10.9.9.9:7684"}) is True
    assert dp.origin_ok({"Host": "q.example.test", "Origin": "https://evil.example.test"}) is False
    assert dp.origin_ok({"Origin": "https://q.example.test"}) is False


def test_upstream_headers_allowlist_and_host():
    got = dp.upstream_headers({"Cookie": "sid=abc", "Content-Type": "text/plain", "Content-Length": "5",
                               "Accept": "*/*", "Host": "q.example.test", "User-Agent": "ua"})
    assert got == {"Cookie": "sid=abc", "Content-Type": "text/plain", "Content-Length": "5",
                   "Accept": "*/*", "Host": "127.0.0.1:7690"}


def test_upstream_headers_rewrites_origin():
    got = dp.upstream_headers({"Origin": "https://q.example.test", "Host": "q.example.test"})
    assert got == {"Host": "127.0.0.1:7690", "Origin": "http://127.0.0.1:7690"}
    assert "Origin" not in dp.upstream_headers({"Origin": ""})


def test_upstream_headers_drops_tunnel_and_proxy_headers():
    bad = {"CF-Connecting-IP": "203.0.113.5", "Cf-Access-Jwt-Assertion": "x", "X-Forwarded-For": "203.0.113.5",
           "Forwarded": "for=203.0.113.5", "Via": "1.1 x", "X-Real-IP": "203.0.113.5", "Authorization": "Bearer x", "Referer": "r"}
    assert dp.upstream_headers(bad) == {"Host": "127.0.0.1:7690"}


TESTS = [test_origin_ok_absent_or_empty, test_origin_ok_same_and_different, test_upstream_headers_allowlist_and_host,
         test_upstream_headers_rewrites_origin, test_upstream_headers_drops_tunnel_and_proxy_headers]

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
