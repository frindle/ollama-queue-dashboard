#!/usr/bin/env bash
# Runs the ollama-queue daemon (the process that launches jobs) and the HTTP
# API/dashboard together in one container. Ollama itself is NOT run here -- it
# stays on its own hosts and is reached over HTTP (see config/servers.json).
set -euo pipefail

: "${HOME:=/data}"
: "${OLLAMA_QUEUE_SERVERS:=/config/servers.json}"
export HOME OLLAMA_QUEUE_SERVERS

# State + logs live under $HOME/bin (mount $HOME as a volume to persist them);
# code lives in /app/bin (baked into the image).
mkdir -p "$HOME/bin" "$(dirname "$OLLAMA_QUEUE_SERVERS")"

# Seed a servers file on first run so the dashboard has something to show/edit.
if [ ! -f "$OLLAMA_QUEUE_SERVERS" ]; then
  echo "[entrypoint] no servers file at $OLLAMA_QUEUE_SERVERS -- seeding from example"
  cp /app/config/servers.example.json "$OLLAMA_QUEUE_SERVERS"
fi

PYTHON="${PYTHON:-python3}"
daemon_pid=""
api_pid=""

shutdown() {
  echo "[entrypoint] shutting down"
  [ -n "$api_pid" ] && kill -TERM "$api_pid" 2>/dev/null || true
  [ -n "$daemon_pid" ] && kill -TERM "$daemon_pid" 2>/dev/null || true
  wait 2>/dev/null || true
  exit 0
}
trap shutdown TERM INT

echo "[entrypoint] starting queue daemon (ollama-queue.py run)"
"$PYTHON" /app/bin/ollama-queue.py run &
daemon_pid=$!

echo "[entrypoint] starting HTTP API/dashboard on :${QUEUE_API_PORT:-7684}"
"$PYTHON" /app/bin/ollama-queue-api.py &
api_pid=$!

# If either child exits, bring the container down so the orchestrator restarts it.
wait -n "$daemon_pid" "$api_pid"
echo "[entrypoint] a child process exited -- stopping the other"
shutdown
