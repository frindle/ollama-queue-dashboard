#!/usr/bin/env python3
"""Tests for the CLIENT-side `enqueue --remote` HTTP path.

This is the client half of the queue's Unraid cutover: `enqueue --remote <URL>`
does NOT touch local state / worktrees, it reads the --task-file CONTENT locally,
maps the enqueue CLI flags to the server's JSON field names, and POSTs the job to
<URL>/api/jobs (see the server contract in bin/ollama-queue-api.py _enqueue).

These tests stand up a stub HTTP server (stdlib http.server), run the real CLI in
a subprocess against it, and assert:
  - the POSTed body carries the right fields (task CONTENT, repo, verify, ...);
  - the printed id + `remote:` line come from the stub's response;
  - --remote-token sends an Authorization: Bearer header;
  - a 400 from the stub makes the CLI exit non-zero with the server's message;
  - local state is never written in remote mode.

Pure-stdlib, no GPU, loopback only. Run: python3 tests/test_remote_enqueue.py
"""
import json
import os
import subprocess
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

BIN = Path(__file__).resolve().parent.parent / "bin"
QUEUE = BIN / "ollama-queue.py"

PASS = 0
FAIL = 0


def check(name, cond):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"PASS {name}")
    else:
        FAIL += 1
        print(f"FAIL {name}")


class _StubServer:
    """A one-shot stub /api/jobs endpoint that records the last request and
    replies with a canned JSON body (or a canned status/error for the 400 case)."""

    def __init__(self, status=200, resp=None, error_text="bad request"):
        self.status = status
        self.resp = resp if resp is not None else {"id": "deadbeef1234", "label": "stub-label",
                                                    "split": False}
        self.error_text = error_text
        self.last = {}  # path, headers, body(parsed)
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_POST(self):
                length = int(self.headers.get("Content-Length", 0))
                raw = self.rfile.read(length) if length else b""
                outer.last = {
                    "path": self.path,
                    "headers": {k: v for k, v in self.headers.items()},
                    "body": json.loads(raw.decode()) if raw else None,
                }
                if outer.status >= 400:
                    body = outer.error_text.encode()
                    self.send_response(outer.status)
                    self.send_header("Content-Type", "text/plain")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                    return
                body = json.dumps(outer.resp).encode()
                self.send_response(outer.status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self._httpd = HTTPServer(("127.0.0.1", 0), Handler)
        self.port = self._httpd.server_address[1]

    def __enter__(self):
        self._t = threading.Thread(target=self._httpd.serve_forever, daemon=True)
        self._t.start()
        return self

    def __exit__(self, *a):
        self._httpd.shutdown()
        self._httpd.server_close()

    @property
    def url(self):
        return f"http://127.0.0.1:{self.port}"


def _run_cli(extra_args, env=None):
    e = dict(os.environ)
    # Never let an ambient default leak into the test.
    e.pop("OLLAMA_QUEUE_REMOTE", None)
    e.pop("QUEUE_API_TOKEN", None)
    if env:
        e.update(env)
    return subprocess.run(
        [sys.executable, str(QUEUE), "enqueue", *extra_args],
        capture_output=True, text=True, env=e,
    )


def _task_file(tmp):
    p = Path(tmp) / "task.md"
    p.write_text("Fix the flaky retry in app.py.\nBe surgical.\n")
    return p


# --------------------------------------------------------------------------
# 1) Happy path: body fields + printed id/remote line, no local state written
# --------------------------------------------------------------------------
def test_remote_happy_path():
    with tempfile.TemporaryDirectory() as tmp:
        tf = _task_file(tmp)
        with _StubServer(resp={"id": "abc123def456", "label": "remote-test",
                               "split": False}) as srv:
            r = _run_cli([
                "--remote", srv.url,
                "--model", "qwen3.8:27b-q4_K_M",
                "--task-file", str(tf),
                "--repo", "foo",
                "--verify", "true",
                "--label", "remote-test",
            ])
            check("happy path exits 0", r.returncode == 0)
            body = srv.last.get("body") or {}
            check("POSTed to /api/jobs", srv.last.get("path") == "/api/jobs")
            check("body carries model", body.get("model") == "qwen3.8:27b-q4_K_M")
            check("body carries task CONTENT (not path)",
                  body.get("task", "").startswith("Fix the flaky retry"))
            check("body has no task_file path key", "task_file" not in body)
            check("body carries repo as-is", body.get("repo") == "foo")
            check("body carries verify", body.get("verify") == "true")
            check("body carries label", body.get("label") == "remote-test")
            # Fields left at their default must NOT be forwarded (server owns defaults).
            check("default host not forwarded", "host" not in body)
            check("client-local task_file/func/remote not forwarded",
                  not ({"func", "remote", "remote_token"} & set(body)))
            check("printed the returned id", "abc123def456" in r.stdout)
            check("printed remote: line", f"remote: {srv.url}" in r.stdout)
            # Remote mode did NOT touch local worktree/git logic: --repo "foo" is not
            # a real local path, so a LOCAL enqueue would have errored resolving it.
            # A clean exit 0 + a successful POST proves the local path was skipped.
            check("no local worktree/git error (local path skipped)",
                  "worktree" not in r.stderr.lower() and "not a git" not in r.stderr.lower())


# --------------------------------------------------------------------------
# 2) Token: --remote-token sends Authorization: Bearer
# --------------------------------------------------------------------------
def test_remote_token_header():
    with tempfile.TemporaryDirectory() as tmp:
        tf = _task_file(tmp)
        with _StubServer() as srv:
            r = _run_cli([
                "--remote", srv.url, "--remote-token", "s3cr3t-token",
                "--model", "m", "--task-file", str(tf), "--repo", "foo",
                "--verify", "true",
            ])
            check("token run exits 0", r.returncode == 0)
            auth = (srv.last.get("headers") or {}).get("Authorization")
            check("Authorization: Bearer sent with --remote-token",
                  auth == "Bearer s3cr3t-token")


def test_remote_token_from_env():
    with tempfile.TemporaryDirectory() as tmp:
        tf = _task_file(tmp)
        with _StubServer() as srv:
            r = _run_cli(
                ["--remote", srv.url, "--model", "m", "--task-file", str(tf),
                 "--repo", "foo", "--verify", "true"],
                env={"QUEUE_API_TOKEN": "env-token-xyz"},
            )
            check("env-token run exits 0", r.returncode == 0)
            auth = (srv.last.get("headers") or {}).get("Authorization")
            check("Authorization: Bearer falls back to QUEUE_API_TOKEN env",
                  auth == "Bearer env-token-xyz")


def test_no_token_no_auth_header():
    with tempfile.TemporaryDirectory() as tmp:
        tf = _task_file(tmp)
        with _StubServer() as srv:
            r = _run_cli(["--remote", srv.url, "--model", "m", "--task-file", str(tf),
                          "--repo", "foo", "--verify", "true"])
            check("no-token run exits 0", r.returncode == 0)
            check("no Authorization header when no token",
                  "Authorization" not in (srv.last.get("headers") or {}))


# --------------------------------------------------------------------------
# 3) env OLLAMA_QUEUE_REMOTE defaults --remote
# --------------------------------------------------------------------------
def test_remote_from_env_default():
    with tempfile.TemporaryDirectory() as tmp:
        tf = _task_file(tmp)
        with _StubServer(resp={"id": "envjob99", "split": False}) as srv:
            r = _run_cli(
                ["--model", "m", "--task-file", str(tf), "--repo", "foo", "--verify", "true"],
                env={"OLLAMA_QUEUE_REMOTE": srv.url},
            )
            check("env-default remote exits 0", r.returncode == 0)
            check("env-default remote actually POSTed", srv.last.get("path") == "/api/jobs")
            check("env-default printed id", "envjob99" in r.stdout)


# --------------------------------------------------------------------------
# 4) A 400 from the stub -> CLI exits non-zero with the server's message
# --------------------------------------------------------------------------
def test_remote_400_propagates():
    with tempfile.TemporaryDirectory() as tmp:
        tf = _task_file(tmp)
        with _StubServer(status=400, error_text="repo (preferred) or cwd is required") as srv:
            r = _run_cli(["--remote", srv.url, "--model", "m", "--task-file", str(tf),
                          "--repo", "foo", "--verify", "true"])
            check("400 makes CLI exit non-zero", r.returncode != 0)
            check("server error text surfaced on stderr",
                  "repo (preferred) or cwd is required" in r.stderr)
            check("HTTP 400 mentioned", "400" in r.stderr)


# --------------------------------------------------------------------------
# 5) split response is reported
# --------------------------------------------------------------------------
def test_remote_split_response():
    with tempfile.TemporaryDirectory() as tmp:
        tf = _task_file(tmp)
        with _StubServer(resp={"id": "grp-primary", "label": "L", "split": True,
                               "group": "g1", "slice_ids": ["s1", "s2", "s3"]}) as srv:
            r = _run_cli(["--remote", srv.url, "--model", "m", "--task-file", str(tf),
                          "--repo", "foo", "--verify", "true"])
            check("split run exits 0", r.returncode == 0)
            check("split slices reported", "s1" in r.stdout and "3 slices" in r.stdout)


if __name__ == "__main__":
    test_remote_happy_path()
    test_remote_token_header()
    test_remote_token_from_env()
    test_no_token_no_auth_header()
    test_remote_from_env_default()
    test_remote_400_propagates()
    test_remote_split_response()
    print(f"\n{PASS} passed, {FAIL} failed")
    sys.exit(1 if FAIL else 0)
