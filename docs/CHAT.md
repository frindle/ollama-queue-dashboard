# Dashboard chat

Stdlib-only chat front end for the Ollama-queue dashboard: chat with a local vision model about a project folder, attach files and images. Read-only toward the project.

## How it works

Single module `dashboard_chat.py` (stdlib only). Sessions are JSON files at `<base>/sessions/<id>.json`; `<base>` is `$DASHBOARD_CHAT_HOME`.

- **Routing (`handle`)**: server-independent `handle(method, path, query, body_bytes) -> (status, headers, body)`. Routes: `GET /chat`, `GET /api/chat/projects`, project `tree` / `file`, `GET|POST /api/chat/sessions`, `GET|POST /api/chat/sessions/<id>[/messages]`. Bad ids, non-object bodies and bad `mode` give 4xx, never an exception.
- **Two modes per message** (`mode` = `chat` | `job`, auto-picked by `is_job_request(text)` unless given):
  - `chat` runs as a direct call on a daemon thread (`start_direct`); the assistant message is `running` then `done`/`error`.
  - `job` (coding or investigation) is queued (`enqueue_job`; `--task-kind coding`, with the answer written by the model to `ANSWER.md` via its file tool because the worker's nudge loop would otherwise record its reply to a nudge as the final answer; set `DASHBOARD_CHAT_QUEUE_CMD` to replace the queue command in tests): a per-job directory `<base>/jobs/<session_id>-<message_index>/` holds `TASK.md` and receives `ANSWER.md`; the message is `queued`. The queue job runs with `--bundle chat --cwd <job dir> --capture-final-as ANSWER.md`. `collect_job_results` (called on every `GET /api/chat/sessions/<id>`) flips a `queued` message to `done` once a non-empty `ANSWER.md` exists.
- **Requests to the model (`build_request`)**: project files go in the system message as `[FILE: path]` blocks; turns keep their real roles; images (PNG/JPEG/GIF/WebP, validated by magic bytes) ride on the last user turn. Inputs are never mutated.
- **Safety**: ids must fullmatch `^[0-9a-f]{32}$`; project paths cannot escape the project root (symlinks included); skip-dirs and non-UTF-8 files are refused. `python3 test_safety.py` runs the adversarial tests (stdlib only).

## Front end

`chat.html` (served by `GET /chat`, copied to `<base>/chat.html`) is a single static page: project picker, session list, message log that polls while a reply is running/queued, optional file list and image attach, Auto/Chat/Job mode. `ollama-queue-api.py` mounts `/chat` and `/api/chat/*` by delegating to `handle()` (lazy import; any failure is a 503 for chat only) and the queue page has a "Chat" link. `<base>/projects.json` is the project allowlist.

## Not built yet

- Job working directory is the per-job directory, not the project folder; the project folder is not yet readable by queued jobs.
- Image types beyond the four above (e.g. HEIC) are rejected.

## Where things live

- `bin/dashboard_chat.py` (module; install next to `ollama-queue-api.py` in `~/bin`), `bin/test-dashboard-chat.py` (adversarial tests), `chat/chat.html` (copy to `~/.ollama-dispatch/chat/chat.html`), allowlist `~/.ollama-dispatch/chat/projects.json`.
- Moved here from the standalone `dashboard-chat` folder (local-only history; that folder is now superseded).
