#!/usr/bin/env python3
"""Config-driven BACKEND registry for the queue (dispatcher model).

Single source of truth for the `{name: {...}}` backend table that the queue and
ollama-worker.py consume for routing. Each entry is a backend SERVICE the queue
dispatches jobs to over HTTP; the queue itself runs none of them in-process.

An entry has a TYPE (added 2026-09-11 for the universal-dispatcher migration):

    {"name": {"type": "ollama", "url": str, "usable_bytes": int}}   # LLM host
    {"name": {"type": "comfyui|img2vid|image", "url": str}}         # HTTP backend

BACKWARD COMPATIBILITY: an entry with NO "type" key is treated as an Ollama host
(the original {"url","usable_bytes"} shape). So a pre-migration servers.json that
lists only Ollama hosts keeps working unchanged -- `ollama_hosts()` returns
exactly the `{name: {"url","usable_bytes"}}` table every existing consumer relies
on, and non-Ollama backends never leak into the Ollama routing/VRAM-fit path.

Loaded via importlib the same way ollama-queue.py loads ollama-worker.py, so
it works whether or not `bin/` happens to be on sys.path.

Resolution order for the servers file (load_servers):
  1. $OLLAMA_QUEUE_SERVERS, if set (explicit override).
  2. <repo>/config/servers.json (this file's parent's parent / "config").
  3. built-in DEFAULT_SERVERS (no file present).
"""
import json
import os
from pathlib import Path

# Recognised backend types. "ollama" is the LLM path (in-process worker POSTs to
# the host's /api or /v1). The rest are remote HTTP services the queue dispatches
# to via bin/backend-dispatch.py using the contract in docs/BACKENDS.md. Keep this
# list and that doc in sync when adding a backend.
OLLAMA_TYPE = "ollama"
BACKEND_TYPES = ("ollama", "comfyui", "img2vid", "image")

# Built-in fallback used only when no servers file exists. PLACEHOLDER hosts
# on purpose -- a shareable default, never a real LAN. Keys "studio"/"unraid"
# are preserved because existing consumers index them by name.
DEFAULT_SERVERS = {
    "studio": {
        "url": "http://host.docker.internal:11434",
        "usable_bytes": 44 * 1024**3,
    },
    "unraid": {
        "url": "http://192.0.2.10:11434",
        "usable_bytes": int(12 * 1024**3 * 0.8),
    },
}


def default_servers_path() -> Path:
    """<repo>/config/servers.json, resolved relative to this file."""
    return Path(__file__).resolve().parent.parent / "config" / "servers.json"


def _resolve_path(path=None):
    if path:
        return Path(path)
    env = os.environ.get("OLLAMA_QUEUE_SERVERS")
    if env:
        return Path(env)
    return default_servers_path()


def load_servers(path=None) -> dict:
    """Resolve the servers file (explicit path > $OLLAMA_QUEUE_SERVERS >
    <repo>/config/servers.json) and return its parsed JSON dict. Falls back to
    the built-in DEFAULT_SERVERS when no file exists or it cannot be read."""
    p = _resolve_path(path)
    try:
        if p.exists():
            return json.loads(p.read_text())
    except (OSError, ValueError):
        pass
    return dict(DEFAULT_SERVERS)


def save_servers(servers, path=None) -> None:
    """Write `servers` as JSON to the resolved path, creating parent dirs."""
    p = _resolve_path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(servers, indent=2))


# --- Backend-type helpers (dispatcher model, 2026-09-11) ---------------------

def entry_type(entry) -> str:
    """The backend type of one registry entry. A missing "type" means Ollama
    (backward compat with the original {"url","usable_bytes"} shape)."""
    return (entry or {}).get("type", OLLAMA_TYPE)


def ollama_hosts(servers=None) -> dict:
    """The Ollama-only host table, in the exact `{name: {"url","usable_bytes"}}`
    shape every legacy consumer (KNOWN_OLLAMA_HOSTS, VRAM-fit routing, the /v1
    proxy, reachability probes) relies on. Non-Ollama backends are filtered out
    so they are never probed with /api/tags or indexed for model-fit."""
    if servers is None:
        servers = load_servers()
    return {n: s for n, s in servers.items() if entry_type(s) == OLLAMA_TYPE}


def backends_of_type(job_type, servers=None) -> dict:
    """`{name: entry}` for every backend whose type matches `job_type`."""
    if servers is None:
        servers = load_servers()
    return {n: s for n, s in servers.items() if entry_type(s) == job_type}


def select_backend(job_type, name=None, servers=None):
    """Resolve a backend URL for a job of type `job_type`.

    If `name` is given it must be a registered backend of that type (a mismatch
    returns (None, None) so the caller can error clearly). With no `name`, the
    first registered backend of that type is chosen. Returns (name, url) or
    (None, None) when nothing matches."""
    if servers is None:
        servers = load_servers()
    if name:
        entry = servers.get(name)
        if entry and entry_type(entry) == job_type and entry.get("url"):
            return name, entry["url"].rstrip("/")
        return None, None
    for n, s in backends_of_type(job_type, servers).items():
        if s.get("url"):
            return n, s["url"].rstrip("/")
    return None, None
