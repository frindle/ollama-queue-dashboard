"""Reverse proxy of /docsum/* on the queue API to the private doc-summary UI (127.0.0.1:7690). Stdlib only.
Never logs or prints a path, query, header, filename or body; a document body is held in memory only for the one request."""
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
    h.close_connection = True
    # Strip the /docsum prefix to get the path for upstream
    p = h.path
    qmark = p.find("?")
    if qmark == -1:
        path_no_prefix = p[len(PREFIX):]
    else:
        path_no_prefix = p[len(PREFIX):qmark] + p[qmark:]
    # Exact /docsum redirect (308) - only when path before ? is exactly /docsum
    path_before_q = p.split("?")[0]
    if path_before_q == PREFIX:
        h.send_response(308)
        h.send_header("Location", "/docsum/")
        h.send_header("Content-Length", "0")
        h.end_headers()
        return
    # POST origin check
    if h.command == "POST":
        if not origin_ok(h.headers):
            body = json.dumps({"error": "E_CROSS_ORIGIN"}).encode()
            h.send_response(403)
            h.send_header("Content-Type", "application/json")
            h.send_header("Content-Length", str(len(body)))
            h.end_headers()
            h.wfile.write(body)
            return
        cl_raw = h.headers.get("Content-Length")
        if cl_raw is None:
            body_len = 0
        elif not (cl_raw.isascii() and cl_raw.isdigit()):
            body = json.dumps({"error": "E_TOO_BIG"}).encode()
            h.send_response(413)
            h.send_header("Content-Type", "application/json")
            h.send_header("Content-Length", str(len(body)))
            h.end_headers()
            h.wfile.write(body)
            return
        else:
            body_len = int(cl_raw)
        if body_len > MAX_BODY:
            body = json.dumps({"error": "E_TOO_BIG"}).encode()
            h.send_response(413)
            h.send_header("Content-Type", "application/json")
            h.send_header("Content-Length", str(len(body)))
            h.end_headers()
            h.wfile.write(body)
            return
        if body_len > 0:
            raw_body = h.rfile.read(body_len)
        else:
            raw_body = None
    else:
        raw_body = None
    allowed = ("Content-Type", "Content-Disposition", "Cache-Control", "Set-Cookie",
               "Location", "Content-Security-Policy", "X-Content-Type-Options", "Referrer-Policy")
    conn = None
    try:
        conn = conn_factory(*UPSTREAM, timeout=60)
        conn.request(h.command, path_no_prefix, body=raw_body, headers=upstream_headers(h.headers))
        resp = conn.getresponse()
        status = resp.status
        resp_body = resp.read()
        resp_headers = [(k, resp.getheader(k)) for k in allowed]
    except Exception:
        status, resp_body = 502, json.dumps({"error": "E_DOCSUM_DOWN"}).encode()
        resp_headers = [("Content-Type", "application/json")]
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
    h.send_response(status)
    for k, v in resp_headers:
        if v is not None:
            h.send_header(k, v)
    h.send_header("Content-Length", str(len(resp_body)))
    h.end_headers()
    h.wfile.write(resp_body)
