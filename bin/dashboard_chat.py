"""dashboard_chat: chat front end for the Ollama-queue dashboard (stdlib only)."""

import json
import os
import re
import sys
import tempfile
import threading
from pathlib import Path

_SESSION_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_DEFAULT_BASE_DIR = Path.home() / ".ollama-dispatch" / "chat"

_JOB_KEYWORDS = (
    "fix", "implement", "refactor", "write", "add a", "change",
    "rename", "debug", "investigate", "diagnose", "find why",
    "root cause", "review", "test", "patch", "diff",
)


def is_job_request(text: str) -> bool:
    """Return True when lowercased text contains a job-request keyword."""
    if not isinstance(text, str):
        return False
    t = text.lower()
    for kw in _JOB_KEYWORDS:
        if kw in t:
            return True
    return False


def validate_session_id(session_id: str) -> None:
    """Validate session_id against ^[0-9a-f]{32}$, raising ValueError on mismatch."""
    if not isinstance(session_id, str):
        raise ValueError("session_id must match ^[0-9a-f]{32}$")
    if not _SESSION_ID_RE.fullmatch(session_id):
        raise ValueError("session_id must match ^[0-9a-f]{32}$")


def get_base_dir() -> str:
    """Return the base directory from DASHBOARD_CHAT_HOME env or the default."""
    return os.environ.get("DASHBOARD_CHAT_HOME", str(_DEFAULT_BASE_DIR))


def write_session(base_dir: str, session_id: str, session_data: dict) -> None:
    """Write a session file atomically.

    Validates session_id against ^[0-9a-f]{32}$, raises ValueError on mismatch.
    Writes JSON to <base_dir>/sessions/<id>.json.
    """
    validate_session_id(session_id)

    sessions_dir = Path(base_dir) / "sessions"
    sessions_dir.mkdir(parents=True, exist_ok=True)

    # Build the session data structure
    messages = session_data.get("messages", [])
    session = {
        "id": session_id,
        "project": session_data.get("project", ""),
        "created": session_data.get("created", ""),
        "messages": [
            {
                "role": msg.get("role", ""),
                "content": msg.get("content", ""),
                "images": msg.get("images", []),
                "files": msg.get("files", []),
                "ts": msg.get("ts", ""),
                "status": msg.get("status", ""),
                **({"mode": msg["mode"]} if "mode" in msg else {}),
            }
            for msg in messages
        ],
    }

    target_file = sessions_dir / f"{session_id}.json"

    # Atomic write via temp file + os.replace
    fd, tmp_path = tempfile.mkstemp(dir=sessions_dir, suffix=".tmp")  # relevance: unobservable
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(session, f)
        os.replace(tmp_path, target_file)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def validate_image(data: bytes) -> str | None:
    """Check magic bytes for png, jpeg, webp, gif. Returns extension string or None."""
    if data[:4] == b'\x89\x50\x4e\x47':  # 89504e47
        return "png"
    if data[:3] == b'\xff\xd8\xff':  # ffd8ff
        return "jpeg"
    if data[:4] == b'RIFF' and data[8:12] == b'WEBP':  # 52494646...57454250
        return "webp"
    if data[:6] == b'GIF87a' or data[:6] == b'GIF89a':  # 47494638
        return "gif"
    return None  # relevance: unobservable


def read_project_file(base_dir: str, project_name: str, path: str) -> str:
    """Read a file from a project, with security checks.

    Resolves os.path.realpath, rejects '..' path components, absolute paths,
    and symlinks escaping the allowlisted root loaded from <base_dir>/projects.json
    as [{name, path}], skips directories .git, node_modules, .venv, __pycache__,
    rejects binary files containing NUL in the first 8KB, and rejects files over 256KB.
    """
    # Load projects.json
    projects_file = os.path.join(base_dir, "projects.json")
    with open(projects_file, "r") as f:
        projects = json.load(f)

    # Find the project
    project = None
    for proj in projects:
        if proj["name"] == project_name:
            project = proj
            break

    if project is None:
        raise ValueError(f"Project '{project_name}' not found")

    # Get the project root
    project_root = os.path.realpath(project["path"])

    # Reject absolute paths
    if os.path.isabs(path):
        raise ValueError("Absolute paths are not allowed")

    # Reject '..' path components
    if ".." in path.split(os.sep):
        raise ValueError("Path traversal is not allowed")

    # Build the target path
    target_path = os.path.realpath(os.path.join(project_root, path))

    # Reject symlinks escaping the root
    if not target_path.startswith(project_root + os.sep) and target_path != project_root:
        raise ValueError("Path escapes project root")

    # Reject paths containing .git, node_modules, .venv, or __pycache__ components
    p = path
    while True:
        _dir, _base = os.path.split(p)
        if _base in (".git", "node_modules", ".venv", "__pycache__"):
            raise ValueError("Path contains forbidden directory: " + _base)
        if not _dir:
            break
        p = _dir

    # Check if it's a file
    if not os.path.isfile(target_path):
        raise FileNotFoundError(f"File not found: {path}")

    # Check file size via os.stat before opening
    if os.stat(target_path).st_size > 256 * 1024:
        raise ValueError("File exceeds 256KB limit")

    # Read the file
    with open(target_path, "rb") as f:
        content = f.read()

    # Reject binary files containing NUL in the first 8KB
    if b"\x00" in content[:8192]:
        raise ValueError("Binary file rejected (contains NUL in first 8KB)")

    try:
        return content.decode("utf-8")
    except UnicodeDecodeError as e:
        raise ValueError("File is not valid UTF-8: " + str(e))


def list_project_tree(base_dir: str, project_name: str, path: str = '') -> list[str]:
    """Return the file tree listing for the project."""
    # Load projects.json
    projects_file = os.path.join(base_dir, "projects.json")
    with open(projects_file, "r") as f:
        projects = json.load(f)

    # Find the project
    project = None
    for proj in projects:
        if proj["name"] == project_name:
            project = proj
            break

    if project is None:
        raise ValueError(f"Project '{project_name}' not found")

    # Get the project root
    project_root = os.path.realpath(project["path"])

    # Build the target path
    target_path = os.path.realpath(os.path.join(project_root, path))

    # Reject symlinks escaping the root
    if not target_path.startswith(project_root + os.sep) and target_path != project_root:
        raise ValueError("Path escapes project root")

    # List the tree
    result = []
    for root, dirs, files in os.walk(target_path):
        # Skip directories .git, node_modules, .venv, __pycache__
        dirs[:] = [d for d in dirs if d not in {'.git', 'node_modules', '.venv', '__pycache__'}]

        # Get relative path
        rel_root = os.path.relpath(root, target_path)

        # Add files
        for fname in sorted(files):
            if rel_root == '.':
                result.append(fname)
            else:
                result.append(os.path.join(rel_root, fname))

    return result


def check_image_limits(images: list[bytes]) -> bool:
    """Check that all images are valid (magic bytes) and within limits (8MB, 6 per message)."""
    if len(images) > 6:
        return False
    MAX_SIZE = 8 * 1024 * 1024  # 8MB
    for img in images:
        if len(img) > MAX_SIZE:
            return False
        if validate_image(img) is None:
            return False
    return True


def stores_image(base_dir: str, session_id: str, data: bytes) -> str:
    """Store a valid image under <base_dir>/images/<session_id>/<n>.<ext>.

    Validates magic bytes, enforces 8MB max per image, and 6 per message.
    Returns the file path on success, or an error string on failure.
    """
    try:
        validate_session_id(session_id)
    except ValueError:
        return "error: invalid session_id"

    ext = validate_image(data)
    if ext is None:
        return "error: invalid image format"

    MAX_SIZE = 8 * 1024 * 1024
    if len(data) > MAX_SIZE:
        return "error: image exceeds 8MB limit"

    images_dir = Path(base_dir) / "images" / session_id
    images_dir.mkdir(parents=True, exist_ok=True)

    existing = sorted([f for f in images_dir.iterdir() if f.is_file()])
    n = len(existing)

    if n >= 6:
        return "error: maximum 6 images per message reached"

    target_file = images_dir / f"{n}.{ext}"
    target_file.write_bytes(data)

    return str(target_file)


def build_request(project_name: str, messages: list[dict], files: list[str], images: list[dict]) -> dict:
    """Construct an API request with system prompt, files, messages, and images."""
    system_prompt = (
        f"Project: {project_name}. You are a read-only assistant. "
        "Answer and discuss normally while putting any proposed code change in "
        "fenced diff blocks."
    )

    content = [{"type": "text", "text": system_prompt}]

    for f in files:
        if isinstance(f, dict):
            content.append({"type": "text", "text": "[FILE: " + f['path'] + "]\n" + f.get("content", "")})
        else:
            content.append({"type": "text", "text": "[FILE: " + f + "]"})

    turns = []
    last_user = None
    for i, msg in enumerate(messages):
        role = msg.get("role", "user")
        turns.append({"role": role, "content": msg.get("content", "")})
        if role == "user":
            last_user = i
    if images and last_user is not None:
        parts = [{"type": "text", "text": turns[last_user]["content"]}]
        for img in images:
            mime = img.get("mime", "image/png")
            encoded = img.get("encoded", "")
            parts.append({"type": "image_url", "image_url": {"url": f"data:{mime};base64,{encoded}"}})
        turns[last_user]["content"] = parts

    return {"messages": [{"role": "system", "content": content}] + turns, "files": files}


import uuid
import base64
import urllib.error
import urllib.request


def _default_http(url, headers, body, timeout):
    req = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


def call_model(request: dict, http_func=None) -> dict:
    """POST request["messages"] to the local Darkbloom chat endpoint; never raises."""
    key = None
    try:
        if not isinstance(request, dict) or not isinstance(request.get("messages"), list):
            return {"status": "error", "text": "invalid request"}
        with open(os.path.expanduser("~/.darkbloom/local.json")) as f:
            key = json.load(f)["api_key"]
        if not isinstance(key, str) or not key:
            return {"status": "error", "text": "no api key"}
        body = json.dumps({
            "model": "qwen3.6-35b-a3b-vl-mtp-mxfp8",
            "messages": request["messages"],
            "chat_template_kwargs": {"enable_thinking": False},
            "stream": False,
        }).encode()
        headers = {"Authorization": "Bearer " + key, "Content-Type": "application/json"}
        status, raw = (http_func or _default_http)(
            "http://127.0.0.1:8000/v1/chat/completions", headers, body, 60)
        if status != 200:
            return {"status": "error", "text": "http " + str(status)}
        text = json.loads(raw)["choices"][0]["message"]["content"]
        if not isinstance(text, str):
            return {"status": "error", "text": "bad response"}
        return {"status": "done", "text": text}
    except Exception as e:
        msg = (type(e).__name__ + ": " + str(e)).replace(key, "[key]") if key else type(e).__name__
        return {"status": "error", "text": msg[:200]}


def read_session(base_dir: str, session_id: str) -> dict:
    """Read a session file.

    Validates session_id against ^[0-9a-f]{32}$, raises ValueError on mismatch.
    Reads JSON from <base_dir>/sessions/<id>.json.
    Raises FileNotFoundError if the session file does not exist.
    """
    validate_session_id(session_id)

    sessions_dir = Path(base_dir) / "sessions"
    target_file = sessions_dir / f"{session_id}.json"

    if not target_file.exists():
        raise FileNotFoundError(f"Session {session_id} not found")

    with open(target_file, "r") as f:
        return json.load(f)


def enqueue_job(session_id, message_index):
    """Enqueue a job via ollama-queue.py --bundle chat."""
    import subprocess
    validate_session_id(session_id)
    base_dir = get_base_dir()
    sessions_dir = Path(base_dir) / "sessions"
    session_file = sessions_dir / f"{session_id}.json"
    session = json.loads(session_file.read_text())
    messages = session["messages"]
    user_msg = messages[message_index - 1]
    user_text = user_msg["content"]
    job_dir = Path(base_dir) / "jobs" / f"{session_id}-{message_index}"
    job_dir.mkdir(parents=True, exist_ok=True)
    task_file = job_dir / "TASK.md"
    # The worker loop nudges a model that makes no tool call and records its reply to the
    # nudge as the "final answer", so the answer must be a FILE the model writes itself.
    task_file.write_text(
        "# Task\n" + user_text + "\n\nWrite your complete answer to a file named ANSWER.md "
        "in the current working directory using your file-writing tool, then finish. "
        "ANSWER.md must contain the answer itself, nothing about this process.\n")
    queue_cmd = os.environ.get("DASHBOARD_CHAT_QUEUE_CMD")
    subprocess.run(
        (queue_cmd.split() if queue_cmd else ["python3", str(Path.home() / "bin" / "ollama-queue.py")]) + ["enqueue",
         "--model", "qwen3.6-35b-a3b-vl-mtp-mxfp8",
         "--task-file", str(task_file),
         "--task-kind", "coding",
         "--bundle", "chat",
         "--label", f"chat-{session_id}-{message_index}",
         "--cwd", str(job_dir),
         "--allow-unisolated",
         "--allow-no-verify",
         "--capture-final-as", "ANSWER.md"],
        check=False,
    )


def start_direct(session_id, message_index):
    """Start a daemon thread that calls job(session_id, message_index)."""
    threading.Thread(target=job, args=(session_id, message_index), daemon=True).start()


def collect_job_results(base_dir: str, session_id: str) -> int:
    """Check for completed job ANSWER.md files and update queued assistant messages.

    Returns the number of messages updated (0 if none or session missing/invalid).
    """
    try:
        validate_session_id(session_id)
    except ValueError:
        return 0
    try:
        session = read_session(base_dir, session_id)
    except (ValueError, FileNotFoundError):
        return 0
    messages = session.get("messages", [])
    updated = 0
    for i, msg in enumerate(messages):
        if msg.get("role") == "assistant" and msg.get("status") == "queued":
            job_dir = Path(base_dir) / "jobs" / f"{session_id}-{i}"
            answer_file = job_dir / "ANSWER.md"
            if answer_file.exists():
                text = answer_file.read_text()
                if text:
                    msg["content"] = text
                    msg["status"] = "done"
                    updated += 1
    if updated > 0:
        write_session(base_dir, session_id, session)
    return updated


def handle(method: str, path: str, query: dict, body_bytes: bytes) -> tuple[int, dict, bytes]:
    """Server-independent routing for chat endpoints.

    Returns (status_code, headers_dict, body_bytes).
    """
    base_dir = get_base_dir()

    # GET /chat serves chat.html
    if method == "GET" and path == "/chat":
        chat_html_path = Path(base_dir) / "chat.html"
        if chat_html_path.exists():
            return (200, {"Content-Type": "text/html"}, chat_html_path.read_bytes())
        return (404, {"Content-Type": "application/json"}, json.dumps({"error": "chat.html not found"}).encode())

    # /api/chat/projects
    if method == "GET" and path == "/api/chat/projects":
        projects_file = Path(base_dir) / "projects.json"
        if not projects_file.exists():
            return (404, {"Content-Type": "application/json"}, json.dumps({"error": "projects.json not found"}).encode())
        with open(projects_file, "r") as f:
            projects = json.load(f)
        return (200, {"Content-Type": "application/json"}, json.dumps(projects).encode())

    # /api/chat/projects/<name>/tree?path=
    if method == "GET" and path.startswith("/api/chat/projects/"):
        parts = path.split("/")
        if len(parts) >= 6 and parts[5] == "tree":
            name = parts[4]
            tree_path = query.get("path", "")
            try:
                tree = list_project_tree(base_dir, name, tree_path)
                return (200, {"Content-Type": "application/json"}, json.dumps(tree).encode())
            except (ValueError, FileNotFoundError) as e:
                return (404, {"Content-Type": "application/json"}, json.dumps({"error": str(e)}).encode())

        # /api/chat/projects/<name>/file?path=
        if len(parts) >= 6 and parts[5] == "file":
            name = parts[4]
            file_path = query.get("path", "")
            try:
                content = read_project_file(base_dir, name, file_path)
                return (200, {"Content-Type": "text/plain"}, content.encode())
            except (ValueError, FileNotFoundError) as e:
                return (400, {"Content-Type": "application/json"}, json.dumps({"error": str(e)}).encode())

    # /api/chat/sessions
    if path == "/api/chat/sessions":
        if method == "GET":
            sessions_dir = Path(base_dir) / "sessions"
            if not sessions_dir.exists():
                return (200, {"Content-Type": "application/json"}, json.dumps([]).encode())
            sessions = []
            for f in sorted(sessions_dir.glob("*.json")):
                with open(f, "r") as fh:
                    sessions.append(json.load(fh))
            return (200, {"Content-Type": "application/json"}, json.dumps(sessions).encode())

        if method == "POST":
            try:
                body = json.loads(body_bytes) if body_bytes else {}
            except (json.JSONDecodeError, ValueError):
                return (400, {"Content-Type": "application/json"}, json.dumps({"error": "invalid JSON"}).encode())

            if not isinstance(body, dict):
                return (400, {"Content-Type": "application/json"}, json.dumps({"error": "body must be a JSON object"}).encode())

            project = body.get("project", "")
            if not project:
                return (400, {"Content-Type": "application/json"}, json.dumps({"error": "project is required"}).encode())

            session_id = uuid.uuid4().hex
            session_data = {
                "project": project,
                "messages": [],
                "created": "",
            }
            try:
                write_session(base_dir, session_id, session_data)
            except ValueError as e:
                return (400, {"Content-Type": "application/json"}, json.dumps({"error": str(e)}).encode())

            result = {"id": session_id, "project": project, "messages": []}
            return (200, {"Content-Type": "application/json"}, json.dumps(result).encode())

    # /api/chat/sessions/<id>
    if path.startswith("/api/chat/sessions/"):
        parts = path.split("/")
        if len(parts) >= 4:
            session_id = parts[4]

            # /api/chat/sessions/<id>/messages
            if len(parts) >= 6 and parts[5] == "messages":
                if method != "POST":
                    return (404, {"Content-Type": "application/json"}, json.dumps({"error": "unknown route"}).encode())

                try:
                    body = json.loads(body_bytes) if body_bytes else {}
                except (json.JSONDecodeError, ValueError):
                    return (400, {"Content-Type": "application/json"}, json.dumps({"error": "invalid JSON"}).encode())

                if not isinstance(body, dict):
                    return (400, {"Content-Type": "application/json"}, json.dumps({"error": "body must be a JSON object"}).encode())

                text = body.get("text", "")
                if not isinstance(text, str):
                    return (400, {"Content-Type": "application/json"}, json.dumps({"error": "text must be a str"}).encode())

                files = body.get("files", [])
                if files and not isinstance(files, list):
                    return (400, {"Content-Type": "application/json"}, json.dumps({"error": "files must be a list"}).encode())

                images = body.get("images", [])
                if images and not isinstance(images, list):
                    return (400, {"Content-Type": "application/json"}, json.dumps({"error": "images must be a list"}).encode())

                try:
                    session = read_session(base_dir, session_id)
                except (ValueError, FileNotFoundError) as e:
                    return (400, {"Content-Type": "application/json"}, json.dumps({"error": str(e)}).encode())

                image_paths = []
                if images:
                    for img in images:
                        data_b64 = img.get("data_base64", "")
                        if data_b64:
                            try:
                                img_data = base64.b64decode(data_b64)
                                img_path = stores_image(base_dir, session_id, img_data)
                                if not img_path.startswith("error:"):
                                    image_paths.append(img_path)
                            except Exception:
                                pass

                # Determine mode: absent -> auto, None -> auto, present -> must be "chat"/"job"
                if "mode" not in body or body.get("mode") is None:
                    mode = "job" if is_job_request(text) else "chat"
                else:
                    mode = body["mode"]
                    if mode not in ("chat", "job"):
                        return (400, {"Content-Type": "application/json"}, json.dumps({"error": "mode must be 'chat' or 'job'"}).encode())

                messages = session.get("messages", [])
                user_msg = {
                    "role": "user",
                    "content": text,
                    "files": files,
                    "images": image_paths,
                    "ts": "",
                    "status": "completed",
                }
                messages.append(user_msg)

                assistant_msg = {
                    "role": "assistant",
                    "content": "",
                    "files": [],
                    "images": [],
                    "ts": "",
                    "status": "running" if mode == "chat" else "queued",
                }
                if mode is not None:
                    assistant_msg["mode"] = mode
                messages.append(assistant_msg)

                session["messages"] = messages
                write_session(base_dir, session_id, session)

                message_index = len(messages) - 1

                if mode == "job":
                    enqueue_job(session_id, message_index)
                else:
                    start_direct(session_id, message_index)

                return (202, {"Content-Type": "application/json"}, json.dumps({"mode": mode, "message_index": message_index, "status": assistant_msg["status"]}).encode())

            # GET /api/chat/sessions/<id>
            if method == "GET":
                try:
                    collect_job_results(base_dir, session_id)
                    session = read_session(base_dir, session_id)
                    return (200, {"Content-Type": "application/json"}, json.dumps(session).encode())
                except (ValueError, FileNotFoundError) as e:
                    return (400, {"Content-Type": "application/json"}, json.dumps({"error": str(e)}).encode())

    return (404, {"Content-Type": "application/json"}, json.dumps({"error": "unknown route"}).encode())


def job(session_id, message_index, base_dir=None, call=None):
    """Process a single chat message for a session."""
    if base_dir is None:
        base_dir = get_base_dir()
    session = read_session(base_dir, session_id)
    messages = session["messages"]
    if not isinstance(message_index, int) or isinstance(message_index, bool):
        raise ValueError("message_index must be an int")
    if message_index < 0 or message_index >= len(messages):
        raise ValueError("message_index out of range")
    if message_index < 1:
        raise ValueError("message_index must be >= 1")
    if messages[message_index]["role"] != "assistant":
        raise ValueError("messages[message_index].role must be 'assistant'")
    if messages[message_index - 1]["role"] != "user":
        raise ValueError("messages[message_index-1].role must be 'user'")
    user = messages[message_index - 1]
    images = []
    for path in user.get("images", []):
        try:
            data = Path(path).read_bytes()
            ext = validate_image(data)
            if ext is not None:
                images.append({"mime": "image/" + ext, "encoded": base64.b64encode(data).decode()})
        except (OSError, IOError):
            pass
    history = [{"role": m["role"], "content": m["content"]} for m in messages[:message_index]]
    file_list = []
    for f in user.get("files", []):
        try:
            content = read_project_file(base_dir, session["project"], f)
            file_list.append({"path": f, "content": content})
        except (ValueError, FileNotFoundError, OSError) as e:
            file_list.append({"path": f, "content": "[unreadable: " + type(e).__name__ + "]"})
    request = build_request(session["project"], history, file_list, images)
    if call is None:
        call = call_model
    try:
        result = call(request)
    except Exception as e:
        messages[message_index]["status"] = "error"
        messages[message_index]["content"] = type(e).__name__
        write_session(base_dir, session_id, session)
        return
    if isinstance(result, dict) and result.get("status") == "done" and isinstance(result.get("text"), str):
        messages[message_index]["content"] = result["text"]
        messages[message_index]["status"] = "done"
    else:
        messages[message_index]["status"] = "error"
        messages[message_index]["content"] = result["text"] if isinstance(result, dict) and isinstance(result.get("text"), str) else "error"
    write_session(base_dir, session_id, session)


def main(argv=None):
    """CLI entry point: dashboard_chat.py job <session_id> <message_index>."""
    if argv is None:
        argv = sys.argv[1:]
    if len(argv) != 3 or argv[0] != "job":
        print("Usage: dashboard_chat.py job <session_id> <message_index>", file=sys.stderr)
        return 2
    session_id = argv[1]
    try:
        message_index = int(argv[2])
    except ValueError:
        print("error: message_index must be an integer", file=sys.stderr)
        return 2
    try:
        job(session_id, message_index)
    except (ValueError, FileNotFoundError, OSError, KeyError) as e:
        print("error: " + str(e), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
