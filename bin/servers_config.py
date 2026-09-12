#!/usr/bin/env python3
"""Config-driven Ollama server registry for the queue.

Single source of truth for the `{name: {"url": str, "usable_bytes": int}}`
host table that ollama-worker.py consumes for routing / VRAM-fit decisions.
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
