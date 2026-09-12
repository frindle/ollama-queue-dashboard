#!/usr/bin/env python3
"""HTTP-backend dispatcher -- the queue's client for non-Ollama job types.

The universal-dispatcher queue launches THIS (instead of ollama-worker.py) for a
job whose `backend` is a remote HTTP service: comfyui / img2vid / image. It is
the exact mirror of how an Ollama job works -- a small subprocess the queue
launches on the lane it assigned, which speaks HTTP to that lane's service. The
GPU work runs on the SERVICE (e.g. the pet-portrait backend on the Studio), never
in this container.

Flow (see docs/BACKENDS.md for the authoritative request/response contract):
  1. Read --task-file (the job payload, JSON) authored by the enqueuer.
  2. POST it to  <url>/v1/dispatch  with the job's type/id/model.
  3. If the service answers synchronously (status done/error) -> finish now.
     Otherwise it returns an {"id": ...}; poll  <url>/v1/dispatch/<id>  until the
     status is terminal or --timeout is hit.
  4. On success print the result (and each result asset URL on its own
     `RESULT: <url>` line so it is greppable in the livelog) and exit 0.
     On error/timeout print the reason and exit non-zero so the queue marks the
     job failed.

Pure Python 3 stdlib -- no third-party deps, no MPS/venv re-exec, so it runs
unchanged inside the Linux queue container.

Contract flags (supplied by the queue's _build_cmd, NOT the 5-flag runner
contract): --backend --url --cwd --task-file --job-id [--model].
"""
import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

TERMINAL_OK = {"done", "succeeded", "complete", "completed"}
TERMINAL_ERR = {"error", "failed", "cancelled", "canceled"}


def _log(msg):
    print(f"[backend-dispatch] {msg}", flush=True)


def _post_json(url, body, timeout):
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url, data=data, method="POST",
        headers={"Content-Type": "application/json", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8") or "{}")


def _get_json(url, timeout):
    req = urllib.request.Request(url, method="GET", headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8") or "{}")


def _result_urls(result):
    """Best-effort extraction of asset URL(s) from a result object, tolerant of
    several shapes the contract allows: {"url": ...}, {"urls": [...]},
    {"assets": [{"url": ...}, ...]}."""
    urls = []
    if not isinstance(result, dict):
        return urls
    if isinstance(result.get("url"), str):
        urls.append(result["url"])
    for u in result.get("urls") or []:
        if isinstance(u, str):
            urls.append(u)
    for a in result.get("assets") or []:
        if isinstance(a, dict) and isinstance(a.get("url"), str):
            urls.append(a["url"])
        elif isinstance(a, str):
            urls.append(a)
    return urls


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--backend", required=True, help="comfyui|img2vid|image")
    ap.add_argument("--url", required=True, help="Base URL of the backend service")
    ap.add_argument("--cwd", default=None, help="Working dir (context; unused for the HTTP call)")
    ap.add_argument("--task-file", required=True, help="JSON job payload for the service")
    ap.add_argument("--job-id", required=True, help="Queue job id (correlation id)")
    ap.add_argument("--model", default=None, help="Model/checkpoint name to forward")
    ap.add_argument("--poll-interval", type=float, default=3.0)
    ap.add_argument("--timeout", type=float, default=1800.0,
                    help="Overall wall-clock budget (s) to reach a terminal status")
    ap.add_argument("--http-timeout", type=float, default=30.0,
                    help="Per-request socket timeout (s)")
    args = ap.parse_args()

    base = args.url.rstrip("/")

    # 1. Read the payload authored by the enqueuer.
    try:
        raw = Path(args.task_file).read_text()
    except OSError as e:
        _log(f"FATAL: cannot read task file {args.task_file}: {e}")
        return 2
    try:
        payload = json.loads(raw) if raw.strip() else {}
    except ValueError:
        # Not JSON -> treat the whole file as a text prompt (a convenience for the
        # simplest image jobs). The contract itself is JSON; this is a courtesy.
        payload = {"prompt": raw}

    body = {
        "type": args.backend,
        "job_id": args.job_id,
        "model": args.model,
        "payload": payload,
    }

    # 2. Dispatch.
    dispatch_url = f"{base}/v1/dispatch"
    _log(f"POST {dispatch_url}  (type={args.backend} job_id={args.job_id} model={args.model})")
    try:
        resp = _post_json(dispatch_url, body, args.http_timeout)
    except urllib.error.HTTPError as e:
        _log(f"FATAL: dispatch returned HTTP {e.code}: {e.read().decode('utf-8', 'replace')[:500]}")
        return 1
    except (urllib.error.URLError, OSError, ValueError) as e:
        _log(f"FATAL: dispatch failed (service unreachable / bad response): {e}")
        return 1

    status = (resp.get("status") or "").lower()

    # 3a. Synchronous service: already terminal in the POST response.
    if status in TERMINAL_OK:
        return _finish(resp)
    if status in TERMINAL_ERR:
        _log(f"FATAL: service reported {status}: {resp.get('error')}")
        return 1

    # 3b. Asynchronous service: poll on the returned id.
    backend_id = resp.get("id") or resp.get("job_id")
    if not backend_id:
        _log(f"FATAL: service did not return a job id or a terminal status: {resp}")
        return 1
    poll_url = f"{base}/v1/dispatch/{backend_id}"
    _log(f"queued as {backend_id}; polling {poll_url} every {args.poll_interval}s "
         f"(budget {args.timeout:.0f}s)")

    deadline = time.monotonic() + args.timeout
    last_status = None
    while time.monotonic() < deadline:
        time.sleep(args.poll_interval)
        try:
            st = _get_json(poll_url, args.http_timeout)
        except urllib.error.HTTPError as e:
            _log(f"poll HTTP {e.code} -- retrying")
            continue
        except (urllib.error.URLError, OSError, ValueError) as e:
            _log(f"poll error ({e}) -- retrying")
            continue
        s = (st.get("status") or "").lower()
        if s != last_status:
            _log(f"status={s or '?'}"
                 + (f" progress={st['progress']}" if "progress" in st else ""))
            last_status = s
        if s in TERMINAL_OK:
            return _finish(st)
        if s in TERMINAL_ERR:
            _log(f"FATAL: service reported {s}: {st.get('error')}")
            return 1

    _log(f"FATAL: timed out after {args.timeout:.0f}s without a terminal status "
         f"(last status={last_status})")
    return 1


def _finish(obj):
    result = obj.get("result", obj)
    urls = _result_urls(result)
    _log("DONE")
    for u in urls:
        print(f"RESULT: {u}", flush=True)
    # Emit the full result object too, so downstream consumers / the livelog have
    # the complete metadata, not just the URLs.
    print("RESULT_JSON: " + json.dumps(result), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
