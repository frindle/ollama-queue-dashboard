#!/usr/bin/env python3
"""Tests for the shared enqueue_job() and the full-fidelity HTTP POST /api/jobs.

Covers the cutover-blocker guarantees: a job enqueued over HTTP gets the SAME
fidelity as the local CLI -- worktree isolation, a recorded launch_baseline, the
pre-dispatch gates (no-verify refusal + preflight verify), and VRAM-fit auto
num-ctx sizing -- and that POST /api/jobs produces the same job shape the CLI
path (enqueue_job) does.

Pure-stdlib, no network, no GPU. Run: python3 tests/test_http_enqueue.py
"""
import importlib.util
import io
import json
import subprocess
import sys
import tempfile
import types
from pathlib import Path

BIN = Path(__file__).resolve().parent.parent / "bin"


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _stub_worker(tmp):
    """Stand in for q.worker() so tests never import the heavy ollama-worker
    module (nor touch the network). Carries only the attrs enqueue_job reads."""
    log_dir = Path(tmp) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    return types.SimpleNamespace(
        # A real (nonexistent) path, as in production -- historical_p90_tokens
        # guards a missing file (OSError -> None), which is the live behaviour.
        DISPATCH_METRICS_PATH=str(log_dir / "dispatch-metrics.jsonl"),
        LOG_DIR=log_dir,
        UNRAID_CONFIRMED_SAFE_CTX={},
    )


def _isolate_state(q, tmp):
    """Point every filesystem constant enqueue_job writes to at a temp dir, and
    stub worker(), so a test never clobbers the real queue state / worktrees."""
    q.STATE_PATH = Path(tmp) / "state.json"
    q.LOCK_PATH = Path(tmp) / "state.lock"
    q.LIVE_LOG_DIR = Path(tmp) / "livelogs"
    q.DISPATCH_WORKTREES = Path(tmp) / "dispatch-worktrees"
    q.worker = lambda: _stub_worker(tmp)


def _make_repo(tmp):
    repo = Path(tmp) / "repo"
    repo.mkdir()
    run = lambda *a: subprocess.run(["git", "-C", str(repo), *a], check=True,
                                    capture_output=True, text=True)
    run("init", "-q")
    run("config", "user.email", "t@t")
    run("config", "user.name", "t")
    (repo / "app.py").write_text("x = 1\n")
    run("add", "-A")
    run("commit", "-qm", "init")
    return repo


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


# --------------------------------------------------------------------------
# 1) enqueue_job: worktree isolation + launch_baseline + preflight + VRAM-fit
# --------------------------------------------------------------------------
def test_enqueue_job_full_fidelity():
    q = _load("oq_lib_1", BIN / "ollama-queue.py")
    with tempfile.TemporaryDirectory() as tmp:
        _isolate_state(q, tmp)
        repo = _make_repo(tmp)
        task_file = Path(tmp) / "task.md"
        task_file.write_text("Fix the thing.\n" * 20)

        spec = q.make_enqueue_spec(
            model="qwen3.8:27b-q4_K_M",
            host="auto",
            repo=str(repo),
            task_file=str(task_file),
            # A verify that RUNS fine but fails at baseline (exit 1, not 126/127):
            # exactly the healthy bug-fix shape the preflight expects.
            verify="test -f DONE_MARKER",
            label="fidelity-test",
        )
        result = q.enqueue_job(spec)
        job = result["primary"]

        check("returns non-split single job", result["split"] is False and len(result["jobs"]) == 1)
        # Worktree isolation
        wt = job.get("worktree")
        check("worktree recorded", bool(wt))
        check("worktree exists on disk", wt and Path(wt).is_dir())
        check("worktree is an ISOLATED linked worktree", wt and q._is_isolated_worktree(Path(wt)))
        check("worktree_branch recorded", bool(job.get("worktree_branch")))
        check("cwd points into the worktree", job["cwd"].startswith(str(wt)))
        # launch_baseline provenance
        lb = job.get("launch_baseline")
        check("launch_baseline recorded", isinstance(lb, dict) and "head" in lb)
        check("launch_baseline.head is a full sha", lb and len(lb["head"]) == 40)
        check("launch_baseline.dirty is an int (clean tree -> 0)", lb and lb["dirty"] == 0)
        # Preflight gate ran and read the baseline
        check("preflight ran (verify failed at baseline as designed)",
              job.get("verify_failed_at_baseline") is True)
        check("preflight verdict surfaced", "preflight-verify" in (job.get("preflight") or ""))
        # VRAM-fit auto num-ctx sizing
        nc = job.get("num_ctx")
        ceiling = q.resolve_ctx_ceiling("auto", "qwen3.8:27b-q4_K_M")
        check("num_ctx auto-computed (not None, no explicit --num-ctx)", nc is not None)
        check("num_ctx snapped to a real bucket", nc in q.CTX_BUCKETS)
        check("num_ctx within host ceiling", nc <= ceiling)
        # Job actually landed in state
        state = json.loads(q.STATE_PATH.read_text())
        check("job persisted to state", any(j["id"] == job["id"] for j in state["jobs"]))
        # Remote provenance degrades gracefully (no session env in this test proc)
        check("no launched_by socket invented", "launched_by" not in job or job["launched_by"])


# --------------------------------------------------------------------------
# 2) enqueue_job: the no-verify gate REFUSES a coding dispatch (raises, no exit)
# --------------------------------------------------------------------------
def test_no_verify_gate_refuses():
    q = _load("oq_lib_2", BIN / "ollama-queue.py")
    with tempfile.TemporaryDirectory() as tmp:
        _isolate_state(q, tmp)
        repo = _make_repo(tmp)
        task_file = Path(tmp) / "task.md"
        task_file.write_text("Make a bounded code change to app.py.\n")

        spec = q.make_enqueue_spec(
            model="qwen3.8:27b-q4_K_M", repo=str(repo),
            task_file=str(task_file), verify=None,  # no verify + coding shape
        )
        raised = False
        try:
            q.enqueue_job(spec)
        except q.EnqueueError as e:
            raised = "NO --verify" in str(e)
        check("no-verify coding dispatch RAISES EnqueueError (not sys.exit)", raised)

        # ... and --allow-no-verify lets it through (scope-only/advisory).
        spec2 = q.make_enqueue_spec(
            model="qwen3.8:27b-q4_K_M", repo=str(repo),
            task_file=str(task_file), verify=None, allow_no_verify=True,
        )
        ok = False
        try:
            r = q.enqueue_job(spec2)
            ok = r["primary"].get("id") is not None
        except q.EnqueueError:
            ok = False
        check("--allow-no-verify enqueues the advisory job", ok)


# --------------------------------------------------------------------------
# 3) make_enqueue_spec rejects unknown fields (typo caught at the call site)
# --------------------------------------------------------------------------
def test_spec_rejects_unknown_field():
    q = _load("oq_lib_3", BIN / "ollama-queue.py")
    raised = False
    try:
        q.make_enqueue_spec(model="m", task_file="/x", nonsense_field=1)
    except q.EnqueueError:
        raised = True
    check("make_enqueue_spec rejects an unknown field", raised)


# --------------------------------------------------------------------------
# 4) API-level: POST /api/jobs produces the same job shape as the CLI path
# --------------------------------------------------------------------------
def test_api_post_matches_cli_shape():
    api = _load("oq_api", BIN / "ollama-queue-api.py")
    with tempfile.TemporaryDirectory() as tmp:
        # The api module carries its OWN copy of the queue lib (api.q); isolate it.
        _isolate_state(api.q, tmp)
        api.TASKS_DIR = Path(tmp) / "web-tasks"
        api.REPOS_ROOT = tmp  # so a relative "repo" resolves under the temp root
        repo = _make_repo(tmp)  # -> <tmp>/repo, addressable as relative "repo"

        body = {
            "model": "qwen3.8:27b-q4_K_M",
            "repo": "repo",  # relative -> resolves against REPOS_ROOT (verify-locality)
            "task": "Fix the thing.\n" * 10,
            "verify": "test -f DONE_MARKER",
            "label": "api-test",
        }
        raw = json.dumps(body).encode()

        # Drive Handler._enqueue directly without a socket: build a bare instance,
        # feed it the request body, capture the JSON/text response.
        h = api.Handler.__new__(api.Handler)
        h.headers = {"Content-Length": str(len(raw))}
        h.rfile = io.BytesIO(raw)
        captured = {}
        h._json = lambda obj, status=200: captured.update({"json": obj, "status": status})
        h._text = lambda msg, status=400: captured.update({"text": msg, "status": status})

        h._enqueue()

        check("API returned JSON (not an error)", "json" in captured)
        if "text" in captured:
            print("   (API error body:", captured["text"], ")")
        resp = captured.get("json", {})
        check("API response has a job id", bool(resp.get("id")))
        check("API job got an isolated worktree", bool(resp.get("worktree")))
        check("API job got a branch", bool(resp.get("branch")))
        check("API job is not a split", resp.get("split") is False)

        # Same shape as the CLI: the persisted job carries worktree + launch_baseline
        # + preflight + auto num-ctx, exactly like enqueue_job() from the CLI.
        state = json.loads(api.q.STATE_PATH.read_text())
        job = next((j for j in state["jobs"] if j["id"] == resp.get("id")), None)
        check("API job persisted to state", job is not None)
        if job:
            check("API job has launch_baseline", isinstance(job.get("launch_baseline"), dict))
            check("API job preflight ran", job.get("verify_failed_at_baseline") is True)
            check("API job auto-sized num_ctx", job.get("num_ctx") in api.q.CTX_BUCKETS)
            check("API job carries a remote provenance marker",
                  job.get("launched_by_session") == "remote-http")
            check("API job did NOT invent a routable launched_by", "launched_by" not in job)


# --------------------------------------------------------------------------
# 5) API-level: missing required fields -> 400, not a crash
# --------------------------------------------------------------------------
def test_api_validation_errors():
    api = _load("oq_api_2", BIN / "ollama-queue-api.py")
    with tempfile.TemporaryDirectory() as tmp:
        _isolate_state(api.q, tmp)
        api.TASKS_DIR = Path(tmp) / "web-tasks"
        api.REPOS_ROOT = None  # a relative path must now be refused

        def call(body):
            raw = json.dumps(body).encode()
            h = api.Handler.__new__(api.Handler)
            h.headers = {"Content-Length": str(len(raw))}
            h.rfile = io.BytesIO(raw)
            cap = {}
            h._json = lambda obj, status=200: cap.update({"json": obj, "status": status})
            h._text = lambda msg, status=400: cap.update({"text": msg, "status": status})
            h._enqueue()
            return cap

        check("missing model -> 400", call({"repo": "/x", "task": "t"}).get("status") == 400)
        check("missing task -> 400", call({"model": "m", "repo": "/x"}).get("status") == 400)
        check("no repo/cwd -> 400", call({"model": "m", "task": "t"}).get("status") == 400)
        check("relative repo w/o REPOS_ROOT -> 400",
              call({"model": "m", "task": "t", "repo": "rel"}).get("status") == 400)
        check("repo AND cwd -> 400",
              call({"model": "m", "task": "t", "repo": "/a", "cwd": "/b"}).get("status") == 400)


if __name__ == "__main__":
    test_enqueue_job_full_fidelity()
    test_no_verify_gate_refuses()
    test_spec_rejects_unknown_field()
    test_api_post_matches_cli_shape()
    test_api_validation_errors()
    print(f"\n{PASS} passed, {FAIL} failed")
    sys.exit(1 if FAIL else 0)
