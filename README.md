# ollama-queue-dashboard

A small, self-hostable **job queue + web dashboard for Ollama**, plus a
**universal OpenAI-compatible LLM endpoint**. It serializes work across one or
more remote Ollama hosts (so a single GPU never gets double-booked), shows what's
running, and lets anything that speaks the OpenAI API talk to your Ollama fleet
through one URL.

It is pure Python 3.12 **standard library** — no framework, no external runtime
dependencies — and ships as a Docker container designed to run on a NAS/server
such as **Unraid**.

## Architecture — the queue moves, Ollama stays put

```
        ┌───────────────────────────────┐         ┌──────────────────────┐
        │  Docker container (e.g. Unraid)│  HTTP   │  Ollama host "studio" │
  you ─▶│  ollama-queue daemon + API     │────────▶│  11434  (GPU / models)│
  /v1   │  dashboard on :7684            │         └──────────────────────┘
        │                                │  HTTP   ┌──────────────────────┐
        │  reads config/servers.json     │────────▶│  Ollama host "gpu-box"│
        └───────────────────────────────┘         │  11434  (GPU / models)│
                                                   └──────────────────────┘
```

**Only the queue** (daemon + API + dashboard) runs in the container. **Ollama and
the models stay on their existing hosts** — your Mac, a GPU box, another server —
and the container reaches them as **remote hosts over HTTP**. Nothing pulls models
into the container; it never needs a GPU itself.

## What you get

- **Job queue + dashboard** (`:7684`) — enqueue jobs, watch progress, reorder,
  pause/resume, and see which models are resident on each host right now.
- **Config-driven servers** — the host list lives in `config/servers.json`
  (`{name: {"url", "usable_bytes"}}`), editable from the **Settings page** or by
  hand. VRAM-fit routing uses `usable_bytes`.
- **OpenAI-compatible LLM endpoint** — `POST /v1/chat/completions` and
  `GET /v1/models`, proxied to an available configured Ollama host. Point any
  OpenAI client at `http://<host>:7684/v1`.
- **Optional token auth** — set `QUEUE_API_TOKEN` to require
  `Authorization: Bearer <token>` on every request.

## Quick start (Docker Compose)

```bash
git clone <this repo> && cd ollama-queue-dashboard
cp config/servers.example.json config/servers.json   # then edit to your hosts
# optional: export QUEUE_API_TOKEN=$(openssl rand -hex 16)
docker compose up -d
```

Open `http://localhost:7684`. (On first run the container seeds
`config/servers.json` from the example if you didn't create it.)

### Plain `docker run`

```bash
docker build -t ollama-queue-dashboard .
docker run -d --name ollama-queue \
  -p 7684:7684 \
  -v "$PWD/config:/config" \
  -v ollama-queue-data:/data \
  --add-host host.docker.internal:host-gateway \
  -e QUEUE_API_TOKEN=changeme \
  ollama-queue-dashboard
```

## Configuring servers

A server entry is `name → {url, usable_bytes}`:

```json
{
  "studio":  { "url": "http://host.docker.internal:11434", "usable_bytes": 47244640256 },
  "gpu-box": { "url": "http://192.0.2.10:11434",            "usable_bytes": 10307921510 }
}
```

- **`url`** — the Ollama base URL the container calls (`/api/*`, `/v1/*`).
- **`usable_bytes`** — memory budget for VRAM-fit routing (`0` = unknown).

Resolution order (first that exists wins):
`$OLLAMA_QUEUE_SERVERS` → `config/servers.json` → built-in placeholder defaults.
`config/servers.json` is gitignored so your real hosts never get committed.

**Edit from the UI:** the dashboard's **Settings — Ollama servers** panel lists
every server with add / edit / delete. Changes are saved to `servers.json`
(`POST /api/hosts`, `DELETE /api/hosts/<name>`) and picked up by new dispatches
immediately.

### Reaching your Ollama hosts from the container

| Where Ollama runs | Put this in `url` | Extra setup |
|---|---|---|
| On the **Docker host** itself | `http://host.docker.internal:11434` | `--add-host host.docker.internal:host-gateway` (already in the compose file) |
| Another **LAN machine** | `http://192.0.2.10:11434` | none — direct HTTP |
| You want host-stack networking | `http://127.0.0.1:11434` | use `network_mode: host` (see compose comments) |

Ollama must listen on a reachable interface: start it with
`OLLAMA_HOST=0.0.0.0:11434` on each host, not the default localhost-only bind.

## Using the LLM endpoint

The container is an OpenAI-compatible gateway to your fleet:

```bash
curl http://localhost:7684/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $QUEUE_API_TOKEN" \
  -d '{"model":"llama3.1:8b","messages":[{"role":"user","content":"hello"}]}'
```

- `GET /v1/models` aggregates models across all reachable configured hosts.
- Add `?host=<name|url>` to pin a specific host; otherwise the first reachable
  configured host is used.
- `"stream": true` is supported (chunks are passed straight through).

Any OpenAI SDK works — set `base_url = http://<host>:7684/v1` and, if you set a
token, `api_key = <your QUEUE_API_TOKEN>`.

## Using the dashboard

- **Loaded right now** — models resident on each host (live `/api/ps`).
- **Job table** — status, iteration/throughput, elapsed; drag to reorder pending
  jobs; pause/resume/promote/cancel.
- **Settings** — manage the server list (above).

## Deploying on Unraid

See [`docs/UNRAID.md`](docs/UNRAID.md) for a Community-Applications-style template
and step-by-step notes.

## Configuration reference

| Env var | Default | Purpose |
|---|---|---|
| `QUEUE_API_TOKEN` | *(unset = open)* | Require `Authorization: Bearer <token>` / `X-Api-Token`. |
| `OLLAMA_QUEUE_SERVERS` | `/config/servers.json` | Path to the servers file. |
| `QUEUE_API_PORT` | `7684` | API/dashboard port. |
| `HOME` | `/data` | Base for queue **state + logs** (mount as a volume). |

| Volume | Purpose |
|---|---|
| `/config` | `servers.json` (edited via Settings). |
| `/data` | Queue state, job logs, live logs. |

## Known limitations

- **Coding-dispatch verify-locality.** The full *coding dispatch* pipeline runs
  each job's **verify command where the worker runs** — i.e. inside this
  container. So dispatching a code fix that must check out and test a repo
  requires that **repo to be mounted into the container** and its toolchain
  present. Out of the box, **LLM access (`/v1`) and job routing across your Ollama
  hosts work with no extra setup**; in-container coding-dispatch is the piece that
  needs the target repos (and their build tools) mounted.
- **The container runs the queue, not Ollama.** It has no GPU and pulls no
  models; it only orchestrates and proxies to the hosts you configure.
- **`LaunchAgents/`** are the author's macOS service definitions, included for
  reference for a non-Docker install; they are not used by the container.

## Requirements

Python 3.12 standard library only — see [`requirements.txt`](requirements.txt).
Optional research/web-fetch extras are listed there but not installed by the
image.
