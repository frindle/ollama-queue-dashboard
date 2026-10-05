# ollama-queue-dashboard

A small, self-hostable **job queue + web dashboard for Ollama**, plus a
**universal OpenAI-compatible LLM endpoint**. It serializes work across one or
more remote Ollama hosts (so a single GPU never gets double-booked), shows what's
running, and lets anything that speaks the OpenAI API talk to your Ollama fleet
through one URL.

It is pure Python 3.12 **standard library** — no framework, no external runtime
dependencies — and ships as a Docker container designed to run on a NAS/server
such as **Unraid**.

## Live deployment (the Mac) — this repo IS the runtime

The dashboard that actually serves the queue today runs **straight from this
checkout** on the Mac, not from Docker:

| Path | What |
|---|---|
| `src/ollama-queue-api.py` | HTTP API + dashboard page (port 7684). **Source of truth.** |
| `src/bundle_view.py` | per-slice bundle views + finished-bundle history |
| `src/dashboard_chat.py` | `/chat` + `/api/chat/*` |
| `src/runstatus_retention.py` | run-status retention predicate (also used by `qctl runs-clear` via HTTP) |
| `chat/chat.html` | chat front end (served from `~/.ollama-dispatch/chat/chat.html`, a symlink to this file) |
| `tests/test-*.py` | dashboard tests (`test-bundle-history`, `test-dashboard-b`, `test-dashboard-chat`, `test-dashboard-layout`, `test-dashboard-slice-child-indent`, `test-runstatus-retention-api`) |

The shared **pipeline** (`ollama-queue.py`, `handoff-emit.py`, `dispatch_progress.py`,
worker, gates, preflight) stays in `~/bin` and is **not** part of this repo's runtime;
the API loads `~/bin/ollama-queue.py` and `~/bin/handoff-emit.py` by absolute path and
finds `dispatch_progress` on `~/bin` (searched after `src/`).

**How it runs:** launchd `com.penn.ollama-queue-api`
(`~/Library/LaunchAgents/com.penn.ollama-queue-api.plist`) runs
`/opt/homebrew/bin/python3 <repo>/src/ollama-queue-api.py`, KeepAlive, log at
`/tmp/ollama-queue-api.log`. Restart **only** the API (never the queue daemon):

```bash
U=gui/$(id -u)
launchctl bootout $U/com.penn.ollama-queue-api
launchctl bootstrap $U ~/Library/LaunchAgents/com.penn.ollama-queue-api.plist
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:7684/
```

**Compatibility symlinks in `~/bin`:** `ollama-queue-api.py`, `bundle_view.py`,
`dashboard_chat.py`, `runstatus_retention.py` and the five `test-*.py` above point
into this repo, so old paths keep working. Edit the repo copy (an editor that
replaces the file instead of writing through would break the symlink).

**Tests** (run from the repo; they load `src/` and the real `~/bin` pipeline;
override with `DASHBOARD_SRC` / `OLLAMA_PIPELINE_BIN`):

```bash
for t in tests/test-bundle-history.py tests/test-dashboard-b.py tests/test-dashboard-chat.py tests/test-dashboard-layout.py \
         tests/test-dashboard-slice-child-indent.py tests/test-runstatus-retention-api.py; do
  python3 "$t" || echo "FAIL $t"; done
python3 tests/test-runstatus-retention-api.py --revert-check
```

**Page layout** (top to bottom): a summary strip (running now with model/host/elapsed/tok/s,
queue depth, needs-attention count); **Needs attention** (stuck bundles and jobs: failed,
parked/held, blocked, each with a one-line reason and its most useful action); **Queue**
(active bundles with a done/total progress bar, current step and status chip; every
reorder/pause/hold/cancel control is in the row's `...` menu); **Finished bundles**
(collapsed, grouped by day, failures named in the summary); then Run Status, web search,
loaded models and host settings. A bundle that is still running or has pending work stays
in the Queue even when an earlier run failed (amber note); it moves to Needs attention
only when nothing of it is running or waiting. System fonts only, light/dark via
`prefers-color-scheme`, no horizontal scroll at phone width. `tests/test-dashboard-layout.py`
renders the page in headless Chrome against fixtures (`--shots DIR` saves screenshots).

**GPU-EXCLUSIVE rows** come from `ollama-queue.py enqueue-gpu`: a non-LLM shell job (for
example a native model-server trial on the Unraid 3080) that has one lane's GPU to itself.
Its runner (`~/bin/gpu-exclusive-runner.py`) unloads the lane's resident Ollama models
first. The row shows a **GPU-EXCLUSIVE** label, a one-line summary, and what it is waiting
for (a busy lane, or a gate for that lane, which always runs first). Its running-row action
is **Stop GPU job (not resumable)**, not Pause, because the command cannot be resumed.

See [docs/DEPLOY.md](docs/DEPLOY.md) for the deploy/sync rules.

### Docker fork (`bin/`, `Dockerfile`, compose)

`bin/` is a separate, scrubbed **productization fork** for Docker/Unraid (config-driven
servers, `/v1` proxy, token auth, HTTP enqueue). It has diverged a long way from the
live `src/` dashboard and is not what runs on the Mac; everything below this section
describes that container build.

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

### Universal dispatcher — beyond Ollama

The queue also dispatches **non-Ollama** job types (image generation, ComfyUI,
image-to-video) to **backend services over HTTP**, using the same shape it uses
for Ollama: it POSTs the job to the backend and polls for a result URL — the
backend service does the GPU work. Register each backend's `type` + `url` in the
registry (Settings page). See **[docs/BACKENDS.md](docs/BACKENDS.md)** for the
HTTP contract and how a job selects its backend.

The **one** thing that runs *in* the container is the **coding feedback loop**
(scaffolding / gates / reviews / verify): it needs `git` + `python3` + `node`
(all baked into the image) and the target repos mounted as a volume — a repo's
own deps install at verify time. See "Coding feedback loop" in
[docs/UNRAID.md](docs/UNRAID.md).

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

A registry entry is `name → {type, url, usable_bytes?}`. `type` defaults to
`ollama` if omitted, so a pre-existing Ollama-only file keeps working unchanged:

```json
{
  "studio":         { "type": "ollama",  "url": "http://host.docker.internal:11434", "usable_bytes": 47244640256 },
  "gpu-box":        { "type": "ollama",  "url": "http://192.0.2.10:11434",           "usable_bytes": 10307921510 },
  "studio-img2vid": { "type": "img2vid", "url": "http://192.0.2.20:8199" },
  "studio-image":   { "type": "image",   "url": "http://192.0.2.20:8198" }
}
```

- **`type`** — `ollama` (LLM host) or a backend type (`comfyui` | `img2vid` |
  `image`). Missing = `ollama`.
- **`url`** — the base URL the container calls. For Ollama: `/api/*`, `/v1/*`.
  For a backend: the `docs/BACKENDS.md` dispatch contract.
- **`usable_bytes`** — memory budget for Ollama VRAM-fit routing (`0`/omitted =
  unknown; not used for non-Ollama backends).

Resolution order (first that exists wins):
`$OLLAMA_QUEUE_SERVERS` → `config/servers.json` → built-in placeholder defaults.
`config/servers.json` is gitignored so your real hosts never get committed.

**Edit from the UI:** the dashboard's **Settings — backends** panel lists every
backend with a type selector and add / edit / delete. Changes are saved to
`servers.json` (`POST /api/hosts`, `DELETE /api/hosts/<name>`) and picked up by
new dispatches immediately.

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
| `OLLAMA_DEFAULT_HOST` | `http://127.0.0.1:11434` | Fallback Ollama host when a job pins none. Point at one of your configured servers (e.g. `http://host.docker.internal:11434`). |
| `OLLAMA_UNRAID_HOSTS` | *(unset)* | Comma-separated host substrings that are VRAM-limited / spillover-prone, used only to bias model-fit auto-selection. Optional. |
| `SEARXNG_HOST` | `http://127.0.0.1:8080` | SearXNG base URL for the optional web-search/research feature. Ignore if unused. |
| `OBSIDIAN_URL` | *(unset = off)* | Optional Obsidian Local-REST base URL for dispatch logging; empty disables it. Token via `OBSIDIAN_TOKEN`. |

| Volume | Purpose |
|---|---|
| `/config` | `servers.json` (edited via Settings). |
| `/data` | Queue state, job logs, live logs. |
| `/repos` *(optional)* | Target repos for the in-container coding feedback loop (rw). |

## Known limitations

- **Coding-dispatch verify-locality.** The *coding dispatch* pipeline runs each
  job's **verify command where the worker runs** — i.e. inside this container. So
  dispatching a code fix that must check out and test a repo requires that
  **repo to be mounted into the container** (e.g. `/repos`). The image ships the
  loop's own toolchain (`git`, `python3`, `nodejs`, `npm`); a repo's own deps
  install at verify time (`npm ci` / `prisma generate` / `pip install`) via the
  env-parity bootstrap. Out of the box, **LLM access (`/v1`), job routing, and
  HTTP-backend dispatch (image/video/ComfyUI) work with no extra setup**;
  in-container coding-dispatch is the piece that needs the target repos mounted.
- **Backend services are external.** `comfyui` / `img2vid` / `image` jobs are
  dispatched over HTTP to services you run elsewhere (see `docs/BACKENDS.md`).
  The queue does not run or proxy their compute; it forwards a result URL.
- **The container runs the queue, not Ollama.** It has no GPU and pulls no
  models; it only orchestrates and proxies to the hosts you configure.
- **`LaunchAgents/`** are the author's macOS service definitions, included for
  reference for a non-Docker install; they are not used by the container.

## Requirements

Python 3.12 standard library only — see [`requirements.txt`](requirements.txt).
Optional research/web-fetch extras are listed there but not installed by the
image.
