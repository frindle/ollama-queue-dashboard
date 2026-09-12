# Backend services & the dispatch HTTP contract

The queue is a **universal dispatcher**. Every job is routed to a **backend
service** over HTTP; the queue container runs none of that compute itself. There
are two families of backend:

| Type      | What runs it                          | How the queue talks to it |
|-----------|---------------------------------------|---------------------------|
| `ollama`  | An Ollama LLM host                    | `ollama-worker.py` -> `/api/*` and `/v1/*` (unchanged) |
| `comfyui` | A ComfyUI server                      | `backend-dispatch.py` -> the contract below |
| `img2vid` | An image-to-video service (Studio)    | `backend-dispatch.py` -> the contract below |
| `image`   | A text-to-image / img2img service     | `backend-dispatch.py` -> the contract below |

The one exception to "nothing runs in the container" is the **coding feedback
loop** (scaffolding / gates / reviews / verify), which runs in-container against
mounted repos. That path is unrelated to the HTTP backends described here.

## Registering a backend

Backends live in the same `config/servers.json` registry as Ollama hosts, keyed
by name, distinguished by a `type` field (add them on the Settings page):

```json
{
  "studio":         { "type": "ollama",  "url": "http://host.docker.internal:11434", "usable_bytes": 47244640256 },
  "studio-img2vid": { "type": "img2vid", "url": "http://192.0.2.20:8199" },
  "studio-image":   { "type": "image",   "url": "http://192.0.2.20:8198" },
  "studio-comfy":   { "type": "comfyui", "url": "http://192.0.2.20:8188" }
}
```

An entry with **no `type` key is treated as `ollama`**, so a pre-migration
Ollama-only file keeps working unchanged.

## How a job selects its backend

```
ollama-queue.py enqueue \
  --backend img2vid \
  --host studio-img2vid \        # a registry name of that type; or omit / --host auto
  --model svd_xt \               # forwarded to the service (checkpoint/model name)
  --cwd /some/workdir \
  --task-file /path/to/payload.json
```

- `--backend <type>` marks the job as a non-Ollama HTTP job.
- The queue resolves the service URL from the registry: the named `--host` if it
  is a backend of that type, else the **first** registered backend of that type
  (`--host auto`).
- That URL becomes the job's **lane**, so backend jobs serialise one-per-service
  through the same scheduler that guards the Ollama lanes (no VRAM-fit math is
  applied — that is Ollama-only).
- The queue launches `bin/backend-dispatch.py`, which performs the HTTP calls
  below. This **replaces** the old local `--runner` scripts
  (`img2vid-render.py`, `txt2img-render.py`, `pet-portrait-render.py`) that
  re-exec'd into a Mac MPS venv and cannot run in a Linux container.

---

## The HTTP contract (what a backend service MUST implement)

`{base}` is the registry `url`. Two endpoints, JSON in / JSON out.

### 1. Dispatch a job — `POST {base}/v1/dispatch`

Request body (sent by `backend-dispatch.py`):

```json
{
  "type": "img2vid",
  "job_id": "3f9a1c2b7d10",
  "model": "svd_xt",
  "payload": { "...": "the verbatim contents of --task-file" }
}
```

- `type` — the backend type (`comfyui` | `img2vid` | `image`).
- `job_id` — the queue's job id, for correlation/logging.
- `model` — the model/checkpoint name (may be `null`).
- `payload` — the enqueuer-authored job spec (prompt, input image URL/base64,
  dimensions, steps, seed, fps, duration, ComfyUI graph, etc.). The queue does
  **not** interpret `payload`; the service owns its schema.

Response — either **asynchronous** (preferred for long renders):

```json
{ "id": "svc-8842", "status": "queued" }      // 200; status queued|running
```

or **synchronous** (small/fast jobs may return the terminal result directly):

```json
{ "status": "done", "result": { "url": "https://.../out.mp4" } }
```

An error at dispatch: return a non-2xx **or** `{"status":"error","error":"..."}`.

### 2. Poll a job — `GET {base}/v1/dispatch/{id}`

`{id}` is the `id` returned by dispatch. Response:

```json
{
  "status": "running",          // queued | running | done | error
  "progress": 0.42,             // OPTIONAL, 0..1, surfaced in the livelog
  "result": { ... },            // present when status == done
  "error": "message"            // present when status == error
}
```

Terminal statuses:
- **success**: `done` (also accepted: `succeeded`, `complete`, `completed`)
- **failure**: `error` (also accepted: `failed`, `cancelled`, `canceled`)

### The `result` object

`backend-dispatch.py` extracts asset URLs from any of these shapes (use
whichever fits):

```json
{ "url": "https://.../out.mp4" }
{ "urls": ["https://.../a.png", "https://.../b.png"] }
{ "assets": [ { "url": "https://.../a.png", "type": "image/png" } ] }
```

Each extracted URL is printed to the job log as `RESULT: <url>` (greppable), and
the full `result` object as `RESULT_JSON: {...}`. Store the output somewhere the
URL stays reachable (the service's own static route, an S3/MinIO bucket, etc.) —
the queue only forwards the URL, it does not proxy the bytes.

### Client behaviour (already implemented in `backend-dispatch.py`)

- Overall budget `--timeout` (default 1800s); per-request socket timeout
  `--http-timeout` (default 30s); poll cadence `--poll-interval` (default 3s).
- Transient poll errors (connection blips, 5xx) are retried until the budget is
  spent; only a terminal `error` status or the timeout fails the job.
- Exit 0 on success, non-zero on error/timeout, so the queue marks the job
  `done` / `failed` exactly as it does for an LLM worker.

---

## Follow-up: building the Studio-side service

The `img2vid` and `image` services live in the **pet-portrait backend on the
Studio** (outside this repo). To make them dispatchable, wrap the existing
render entrypoints in a tiny HTTP server that implements the two endpoints
above: accept `POST /v1/dispatch`, enqueue the render, return an id; expose
`GET /v1/dispatch/{id}` reporting status and, on completion, a `result.url`
pointing at the finished asset. The queue side is done and version-agnostic —
anything that speaks this contract is dispatchable, no queue change required.
