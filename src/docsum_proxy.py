"""Reverse proxy of /docsum/* on the queue API to the private doc-summary UI (127.0.0.1:7690). Stdlib only.
Never logs or prints a path, query, header, filename or body; documents only pass through memory in 64 KiB chunks."""
import http.client
import json
from urllib.parse import urlsplit

PREFIX = "/docsum"
UPSTREAM = ("127.0.0.1", 7690)
UPSTREAM_HOST = "%s:%d" % UPSTREAM
MAX_BODY = 110 * 1024 * 1024
CHUNK = 64 * 1024


def origin_ok(headers):
    origin = headers.get("Origin")
    if not origin:
        return True
    host = headers.get("Host")
    if not host:
        return False
    return urlsplit(origin).netloc.lower() == host.strip().lower()


def upstream_headers(headers):
    result = {"Host": UPSTREAM_HOST}
    for key in ("Cookie", "Content-Type", "Content-Length", "Accept"):
        val = headers.get(key)
        if val is not None:
            result[key] = val
    origin = headers.get("Origin")
    if origin:
        result["Origin"] = "http://" + UPSTREAM_HOST
    return result


def proxy(h, conn_factory=http.client.HTTPConnection):
    h.send_response(501)
    h.send_header("Content-Length", "0")
    h.end_headers()
