#!/usr/bin/env python3
"""dispatch-diagnostics.py -- the "get me these logs" loop for a dispatched model.

A model mid-dispatch can name what evidence it needs (a git log, a file window,
a grep, a bounded `docker logs`, the job's own --verify output) and the harness
fetches it through an ALLOWLISTED, READ-ONLY executor and hands the output back
as the tool result of the same iteration. It is NOT a shell: every kind maps to
a fixed argv (never shell=True), paths must resolve inside the job cwd, secret-
looking files are refused by name, output is capped and redacted, and the whole
loop is bounded (requests per call, calls per run, seconds per request).

Two entry points:

  * worker tool  `request_diagnostics` (ollama-worker.py) -> run_requests(...)
  * CLI          `dispatch-diagnostics.py --cwd DIR [--verify CMD] REQUEST.json`
                 (same executor, for testing a request by hand)

REQUEST SCHEMA (the model emits this as the tool's `requests` argument):

  [
    {"kind": "git",   "args": ["log", "--oneline", "-n", "20"]},
    {"kind": "git",   "args": ["diff", "HEAD", "--", "sidecar/test.js"]},
    {"kind": "file",  "path": "sidecar/test.js", "start": 40, "end": 120},
    {"kind": "ls",    "path": "sidecar", "depth": 2},
    {"kind": "grep",  "pattern": "computeSinceDate", "glob": "sidecar/*.js"},
    {"kind": "verify"},
    {"kind": "docker_logs", "container": "reselling-app-1", "tail": 200,
                            "since": "30m", "filter": "ERROR"}
  ]

  git          allowlisted READ subcommands only (GIT_READ_SUBCOMMANDS); any
               option that could write, execute, or escape the tree is refused
               (GIT_FORBIDDEN_OPTIONS); absolute paths and `..` refused.
  file         a line window [start, end] of one file inside cwd, <= FILE_MAX_LINES
               lines; symlinks resolving outside cwd, .git/ internals and
               secret-named files (SECRET_FILE_PATTERNS) are refused.
  ls           bounded directory listing inside cwd (depth <= LS_MAX_DEPTH,
               <= LS_MAX_ENTRIES entries).
  grep         `git grep -n -I --untracked` (tracked + untracked, never ignored
               files, never binaries); fixed-string by default, `"regex": true`
               for ERE; optional pathspec glob; <= GREP_MAX_MATCHES.
  verify       runs the job's own --verify command exactly as the gate would
               (the ONE non-fixed argv, and it is the harness's, not the model's);
               returns exit code + tail.
  docker_logs  container must be in the catalog (dispatch-diagnostics-catalog.json,
               next to this file, or DIAG_CATALOG); fetched read-only over the
               catalog's transport (unraid-graphql today). tail <= DOCKER_MAX_TAIL,
               since is a duration like 30m / 2h / 1d, filter is a substring
               applied AFTER fetch.

RESULT (returned to the model as JSON text):

  {"results": [{"kind": ..., "ok": true|false, "output": "...", "truncated": bool,
                "refused": "<reason>" | null}, ...],
   "budget": {"calls_used": n, "calls_left": m, "requests_dropped": k}}

BOUNDS (module constants; the worker can lower, never raise):
  MAX_REQUESTS_PER_CALL, MAX_CALLS_PER_RUN, PER_REQUEST_CHARS, PER_CALL_CHARS,
  REQUEST_TIMEOUT_S, DOCKER_TIMEOUT_S.

WHY a tool and not a sentinel in free text: the worker already parses tool
calls, records them in the transcript, loop-detects on their signature, and
returns results in the next iteration. A JSON block in prose would need a
second parser and would be invisible to the loop guard. The schema above is
what the tool's `requests` argument carries.
"""
from __future__ import annotations

import fnmatch
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent

# ---- bounds ----------------------------------------------------------------
MAX_REQUESTS_PER_CALL = 5
MAX_CALLS_PER_RUN = 6
PER_REQUEST_CHARS = 6000
PER_CALL_CHARS = 16000
REQUEST_TIMEOUT_S = 30
DOCKER_TIMEOUT_S = 45
FILE_MAX_LINES = 400
LS_MAX_DEPTH = 3
LS_MAX_ENTRIES = 400
GREP_MAX_MATCHES = 200
DOCKER_MAX_TAIL = 500
DOCKER_MAX_SINCE = timedelta(days=7)

# ---- allowlists ------------------------------------------------------------
GIT_READ_SUBCOMMANDS = {
    "status", "log", "diff", "show", "blame", "ls-files", "rev-parse",
    "grep", "branch", "shortlog", "describe", "stash",  # stash is read-only with "list"/"show" only, enforced below
}
GIT_SUB_ARG_ALLOW = {"stash": {"list", "show"}, "branch": {"-a", "-v", "-vv", "--list", "-r", "--show-current"}}
# any option that writes, runs something, or points git at another tree/config
GIT_FORBIDDEN_OPTIONS = (
    "--output", "-o", "--exec", "--ext-diff", "--textconv", "-c", "--config",
    "--git-dir", "--work-tree", "-C", "--no-index", "--relative", "--edit",
    "-e", "--interactive", "-i", "--apply", "--open-files-in-pager", "-O",
)
GIT_FORBIDDEN_PREFIXES = ("--output=", "--exec=", "--git-dir=", "--work-tree=", "-c", "-O", "-o")

SECRET_FILE_PATTERNS = (
    ".env", ".env.*", "*.pem", "*.key", "*.p12", "*.pfx", "*.jks", "id_rsa*", "id_ed25519*",
    "id_ecdsa*", ".netrc", "credentials*", "*.keychain*", "*secret*", "*token*", ".npmrc",
    ".pypirc", "*.kdbx", "known_hosts", "authorized_keys",
)
SECRET_LINE_RE = re.compile(
    r"(?i)((?:api[_-]?key|token|secret|passw(?:or)?d|authorization|x-api-key|cookie|bearer)"
    r"['\"]?\s*[=:]\s*['\"]?)([^\s'\"]{6,})"
)
BARE_TOKEN_RE = re.compile(r"\b(sk-[A-Za-z0-9]{10,}|ghp_[A-Za-z0-9]{20,}|xox[abp]-[A-Za-z0-9-]{10,}|eyJ[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,})\b")

DURATION_RE = re.compile(r"^(\d{1,4})([smhd])$")


class Refused(Exception):
    """A request the executor will not run. The REASON goes back to the model
    verbatim -- a refusal it cannot read is a refusal it will retry."""


# ---- helpers ---------------------------------------------------------------
def redact(text: str) -> str:
    text = SECRET_LINE_RE.sub(lambda m: m.group(1) + "***", text)
    return BARE_TOKEN_RE.sub("***", text)


def _clip(text: str, cap: int) -> tuple[str, bool]:
    if len(text) <= cap:
        return text, False
    head = cap * 2 // 3
    tail = cap - head
    return text[:head] + f"\n... [{len(text) - cap} chars elided] ...\n" + text[-tail:], True


def _inside(cwd: Path, rel: str) -> Path:
    """Resolve rel against cwd; refuse anything that escapes it (absolute, ..,
    symlink to outside) or reaches into .git/."""
    if not isinstance(rel, str) or not rel or rel.startswith(("/", "~")) or "\x00" in rel:
        raise Refused(f"path must be relative to the job directory, got {rel!r}")
    p = (cwd / rel).resolve()
    try:
        p.relative_to(cwd.resolve())
    except ValueError:
        raise Refused(f"path escapes the job directory: {rel}")
    parts = p.relative_to(cwd.resolve()).parts
    if parts and parts[0] == ".git":
        raise Refused("reads under .git/ are not allowed; use kind=git instead")
    return p


def _secret_named(p: Path) -> bool:
    name = p.name
    return any(fnmatch.fnmatch(name, pat) or fnmatch.fnmatch(name.lower(), pat) for pat in SECRET_FILE_PATTERNS)


def _run(argv: list[str], cwd: Path, timeout: int, env_extra: dict | None = None) -> tuple[int, str]:
    env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "HOME": os.environ.get("HOME", ""),
           "LANG": "C.UTF-8", "GIT_PAGER": "cat", "PAGER": "cat", "GIT_TERMINAL_PROMPT": "0",
           "GIT_CONFIG_NOSYSTEM": "1"}
    if env_extra:
        env.update(env_extra)
    try:
        r = subprocess.run(argv, cwd=str(cwd), capture_output=True, text=True, errors="replace",
                           timeout=timeout, env=env, stdin=subprocess.DEVNULL)
    except subprocess.TimeoutExpired:
        return 124, f"[timed out after {timeout}s]"
    except FileNotFoundError as e:
        return 127, f"[{e}]"
    out = r.stdout
    if r.stderr.strip():
        out += ("\n" if out and not out.endswith("\n") else "") + "[stderr] " + r.stderr.strip()
    return r.returncode, out


# ---- kinds -----------------------------------------------------------------
def kind_git(req: dict, cwd: Path, ctx: dict) -> str:
    args = req.get("args")
    if not isinstance(args, list) or not args or not all(isinstance(a, str) for a in args):
        raise Refused("git needs a non-empty list of string args, e.g. [\"log\",\"--oneline\",\"-n\",\"20\"]")
    sub = args[0]
    if sub not in GIT_READ_SUBCOMMANDS:
        raise Refused(f"git {sub} is not an allowlisted read subcommand; allowed: {sorted(GIT_READ_SUBCOMMANDS)}")
    rest = args[1:]
    for a in rest:
        if a in GIT_FORBIDDEN_OPTIONS or a.startswith(GIT_FORBIDDEN_PREFIXES):
            raise Refused(f"git option {a!r} is not allowed (writes, executes, or escapes the tree)")
        if a.startswith("/") or a.startswith("~") or ".." in re.split(r"[/:]", a):
            raise Refused(f"git path {a!r} must stay inside the job directory")
    if sub in GIT_SUB_ARG_ALLOW:
        bad = [a for a in rest if a.startswith("-") and a not in GIT_SUB_ARG_ALLOW[sub]]
        if sub == "stash" and (not rest or rest[0] not in GIT_SUB_ARG_ALLOW[sub]):
            raise Refused("git stash is allowed only as `stash list` or `stash show`")
        if bad:
            raise Refused(f"git {sub} options {bad} are not allowed")
    code, out = _run(["git", "--no-pager", sub, *rest], cwd, REQUEST_TIMEOUT_S)
    return f"$ git {sub} {' '.join(rest)}\n[exit {code}]\n{out}"


def kind_file(req: dict, cwd: Path, ctx: dict) -> str:
    p = _inside(cwd, req.get("path"))
    if _secret_named(p):
        raise Refused(f"{p.name} looks like a secret-bearing file and is not readable through diagnostics")
    if not p.is_file():
        raise Refused(f"not a file: {req.get('path')}")
    start = int(req.get("start") or 1)
    end = int(req.get("end") or (start + FILE_MAX_LINES - 1))
    if start < 1 or end < start:
        raise Refused("start must be >= 1 and end >= start")
    if end - start + 1 > FILE_MAX_LINES:
        end = start + FILE_MAX_LINES - 1
    lines = p.read_text(errors="replace").splitlines()
    window = lines[start - 1:end]
    body = "\n".join(f"{start + i:>5}: {ln}" for i, ln in enumerate(window))
    note = f" (file has {len(lines)} lines)" if end < len(lines) else ""
    return f"{req.get('path')} lines {start}-{min(end, len(lines))}{note}\n{body}"


def kind_ls(req: dict, cwd: Path, ctx: dict) -> str:
    p = _inside(cwd, req.get("path") or ".")
    if not p.is_dir():
        raise Refused(f"not a directory: {req.get('path')}")
    depth = max(1, min(int(req.get("depth") or 1), LS_MAX_DEPTH))
    out, n = [], 0
    base = p
    for root, dirs, files in os.walk(p):
        rel = Path(root).relative_to(base)
        level = len(rel.parts)
        dirs[:] = sorted(d for d in dirs if d not in (".git", "node_modules", ".venv", "__pycache__"))
        if level >= depth:
            dirs[:] = []
        for name in sorted(files) + [d + "/" for d in dirs]:
            out.append(str(rel / name) if rel.parts else name)
            n += 1
            if n >= LS_MAX_ENTRIES:
                out.append(f"... [listing capped at {LS_MAX_ENTRIES} entries]")
                return "\n".join(out)
    return "\n".join(out) or "(empty)"


def kind_grep(req: dict, cwd: Path, ctx: dict) -> str:
    pat = req.get("pattern")
    if not isinstance(pat, str) or not pat.strip():
        raise Refused("grep needs a non-empty pattern")
    if len(pat) > 300:
        raise Refused("grep pattern too long (300 chars max)")
    mode = ["-E"] if req.get("regex") else ["-F"]
    argv = ["git", "--no-pager", "grep", "-n", "-I", "--untracked", "--no-color", *mode, "-e", pat, "--"]
    glob = req.get("glob")
    if glob:
        if not isinstance(glob, str) or glob.startswith("/") or ".." in glob.split("/"):
            raise Refused("grep glob must be a relative pathspec")
        argv.append(glob)
    code, out = _run(argv, cwd, REQUEST_TIMEOUT_S)
    # drop hits inside secret-named files: the file kind refuses them, so
    # grep must not become the side door
    lines = [ln for ln in out.splitlines() if not _secret_named(Path(ln.split(":", 1)[0]))]
    total = len(lines)
    cap = min(int(req.get("max_matches") or GREP_MAX_MATCHES), GREP_MAX_MATCHES)
    if total > cap:
        lines = lines[:cap] + [f"... [{total - cap} more matches not shown]"]
    if not lines:
        return f"grep {pat!r}: no matches"
    return f"grep {pat!r}{' in ' + glob if glob else ''}: {total} match(es)\n" + "\n".join(lines)


def kind_verify(req: dict, cwd: Path, ctx: dict) -> str:
    verify = ctx.get("verify")
    if not verify:
        raise Refused("this job has no --verify command")
    code, out = _run(["bash", "-lc", verify], cwd, min(int(ctx.get("verify_timeout") or 240), 600))
    tail = "\n".join(out.splitlines()[-120:])
    return f"$ {verify}\n[exit {code}]\n{tail}"


def _load_catalog(ctx: dict) -> dict:
    p = Path(os.environ.get("DIAG_CATALOG") or ctx.get("catalog") or (HERE / "dispatch-diagnostics-catalog.json"))
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except (OSError, json.JSONDecodeError) as e:
        raise Refused(f"diagnostics catalog unreadable: {e}")


def _parse_since(s) -> datetime | None:
    if not s:
        return None
    m = DURATION_RE.match(str(s).strip())
    if not m:
        raise Refused("since must be a duration like 30m, 2h, 1d")
    n, unit = int(m.group(1)), m.group(2)
    delta = {"s": timedelta(seconds=n), "m": timedelta(minutes=n), "h": timedelta(hours=n), "d": timedelta(days=n)}[unit]
    if delta > DOCKER_MAX_SINCE:
        raise Refused(f"since exceeds the {DOCKER_MAX_SINCE.days}-day cap")
    return datetime.now(timezone.utc) - delta


def _unraid_graphql(query: str, variables: dict, src: dict, timeout: int) -> dict:
    """Read-only GraphQL query against Unraid. Credentials come from the
    catalog's `key_from` (a JSON file + dotted path), never from the request."""
    import urllib.request
    url = src.get("url") or os.environ.get("UNRAID_GRAPHQL_URL", "http://unraid.local/graphql")
    key_from = src.get("key_from") or {}
    creds = json.loads(Path(os.path.expanduser(key_from.get("file", "~/.claude.json"))).read_text())
    for part in (key_from.get("path") or "mcpServers.unraid.env.UNRAID_API_KEY").split("."):
        creds = creds[part]
    body = json.dumps({"query": query, "variables": variables}).encode()
    r = urllib.request.Request(url, data=body, headers={"content-type": "application/json", "x-api-key": creds})
    with urllib.request.urlopen(r, timeout=timeout) as resp:
        return json.load(resp)


def kind_docker_logs(req: dict, cwd: Path, ctx: dict) -> str:
    name = req.get("container")
    catalog = _load_catalog(ctx).get("docker") or {}
    if not isinstance(name, str) or name not in catalog:
        raise Refused(f"container {name!r} is not in the diagnostics catalog; known: {sorted(catalog)}")
    src = catalog[name]
    tail = max(1, min(int(req.get("tail") or 200), DOCKER_MAX_TAIL))
    since = _parse_since(req.get("since"))
    flt = req.get("filter")
    if flt is not None and (not isinstance(flt, str) or len(flt) > 200):
        raise Refused("filter must be a short substring")
    transport = src.get("transport")
    fetch = ctx.get("docker_fetch")  # test seam: (src, tail, since) -> list[str]
    if fetch is None:
        if transport == "unraid-graphql":
            def fetch(src, tail, since):
                q = ("query($id: PrefixedID!, $tail: Int, $since: DateTime) {"
                     " docker { logs(id: $id, tail: $tail, since: $since) { lines { timestamp message } } } }")
                cid = src.get("id")
                if not cid:
                    d = _unraid_graphql("{ docker { containers { id names } } }", {}, src, DOCKER_TIMEOUT_S)
                    for c in d["data"]["docker"]["containers"]:
                        if "/" + name in c["names"] or name in c["names"]:
                            cid = c["id"]
                            break
                    if not cid:
                        raise Refused(f"container {name} not found on {src.get('host')}")
                v = {"id": cid, "tail": tail, "since": since.isoformat() if since else None}
                d = _unraid_graphql(q, v, src, DOCKER_TIMEOUT_S)
                if d.get("errors"):
                    raise Refused("unraid graphql: " + "; ".join(e.get("message", "?") for e in d["errors"])[:300])
                return [f"{ln.get('timestamp', '')} {ln.get('message', '')}".rstrip()
                        for ln in d["data"]["docker"]["logs"]["lines"]]
        else:
            raise Refused(f"catalog transport {transport!r} for {name} is not implemented")
    t0 = time.time()
    lines = fetch(src, tail, since)
    if flt:
        lines = [ln for ln in lines if flt in ln]
    hdr = (f"docker logs {name}@{src.get('host', '?')} tail={tail}"
           f"{' since=' + req['since'] if since else ''}{' filter=' + repr(flt) if flt else ''}"
           f": {len(lines)} line(s) in {time.time() - t0:.1f}s")
    return hdr + "\n" + "\n".join(lines[-tail:])


KINDS = {
    "git": kind_git, "file": kind_file, "ls": kind_ls, "grep": kind_grep,
    "verify": kind_verify, "docker_logs": kind_docker_logs,
}


# ---- driver ----------------------------------------------------------------
class Budget:
    """Per-run call counter. The worker holds one per run_task; the CLI makes
    a fresh one per invocation."""

    def __init__(self, max_calls: int = MAX_CALLS_PER_RUN):
        self.max_calls = max_calls
        self.calls_used = 0


def run_requests(requests, cwd, budget: Budget | None = None, **ctx) -> dict:
    """Execute a list of diagnostic requests. Never raises for a bad request --
    each result carries ok/refused so the model can correct itself. Raises
    only when the CALL itself is over budget (the worker turns that into a
    refusal string)."""
    cwd = Path(cwd).resolve()
    budget = budget or Budget()
    if budget.calls_used >= budget.max_calls:
        return {"results": [], "budget": {"calls_used": budget.calls_used, "calls_left": 0, "requests_dropped": 0},
                "refused": f"diagnostics budget exhausted ({budget.max_calls} calls per run)"}
    budget.calls_used += 1
    if isinstance(requests, dict):
        requests = requests.get("requests", [requests])
    if not isinstance(requests, list):
        requests = []
    dropped = max(0, len(requests) - MAX_REQUESTS_PER_CALL)
    requests = requests[:MAX_REQUESTS_PER_CALL]
    results, total = [], 0
    for req in requests:
        entry = {"kind": None, "ok": False, "output": "", "truncated": False, "refused": None}
        try:
            if not isinstance(req, dict):
                raise Refused("each request must be an object with a `kind`")
            kind = req.get("kind")
            entry["kind"] = kind
            fn = KINDS.get(kind)
            if fn is None:
                raise Refused(f"unknown kind {kind!r}; allowed: {sorted(KINDS)}")
            out = redact(fn(req, cwd, ctx))
            remaining = max(0, PER_CALL_CHARS - total)
            out, trunc = _clip(out, min(PER_REQUEST_CHARS, remaining) or 1)
            entry.update(ok=True, output=out, truncated=trunc)
            total += len(out)
        except Refused as e:
            entry["refused"] = str(e)
        except Exception as e:  # noqa: BLE001 -- surface, never crash the worker loop
            entry["refused"] = f"{type(e).__name__}: {e}"[:300]
        results.append(entry)
    return {"results": results,
            "budget": {"calls_used": budget.calls_used, "calls_left": budget.max_calls - budget.calls_used,
                       "requests_dropped": dropped}}


TOOL_SPEC = {
    "type": "function",
    "function": {
        "name": "request_diagnostics",
        "description": (
            "Fetch read-only evidence you need to diagnose the task: git history/diff/blame, a line "
            "window of a file, a directory listing, a grep, the job's own verify output, or bounded "
            "`docker logs` from a catalogued container. Pass a LIST of requests (max 5 per call, 6 calls "
            "per run). Kinds: git {args:[...]} (read subcommands only), file {path,start,end}, "
            "ls {path,depth}, grep {pattern,glob,regex}, verify {}, docker_logs {container,tail,since,filter}. "
            "Output is capped and secrets are redacted. Nothing here can write or run arbitrary shell -- "
            "use run_bash for that."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "requests": {
                    "type": "array",
                    "description": "List of request objects, each with a `kind` plus that kind's fields.",
                    "items": {"type": "object"},
                },
                "why": {"type": "string", "description": "One line: what you expect this evidence to settle."},
            },
            "required": ["requests"],
        },
    },
}


def main(argv=None):
    import argparse
    ap = argparse.ArgumentParser(description="run a diagnostics request by hand (same executor as the worker tool)")
    ap.add_argument("request", help="JSON file with a list of requests, or '-' for stdin")
    ap.add_argument("--cwd", required=True)
    ap.add_argument("--verify", default=None)
    ap.add_argument("--catalog", default=None)
    a = ap.parse_args(argv)
    raw = sys.stdin.read() if a.request == "-" else Path(a.request).read_text()
    res = run_requests(json.loads(raw), a.cwd, verify=a.verify, catalog=a.catalog)
    print(json.dumps(res, indent=1))
    return 0 if all(r["ok"] for r in res["results"]) else 1


if __name__ == "__main__":
    sys.exit(main())
