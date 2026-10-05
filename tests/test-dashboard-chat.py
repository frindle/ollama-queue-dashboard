"""Adversarial safety + behaviour tests for dashboard_chat.py (stdlib only). Exit 1 on any failure."""
import ast, base64, importlib.util, json, os, shutil, sys, tempfile
from pathlib import Path

HERE = Path(os.environ.get("DASHBOARD_SRC") or Path(__file__).resolve().parent.parent / "src")  # repo src/
spec = importlib.util.spec_from_file_location("dc", HERE / "dashboard_chat.py")
dc = importlib.util.module_from_spec(spec)
sys.modules["dc"] = dc
spec.loader.exec_module(dc)

PNG = b"\x89PNG\r\n\x1a\n" + b"0" * 16
SID = "0123456789abcdef" * 2
FAILS = []


def check(name, fn):
    try:
        ok, detail = fn()
    except Exception as e:
        ok, detail = False, "raised %s: %s" % (type(e).__name__, e)
    print(("PASS " if ok else "FAIL ") + name + ("" if ok else " -- " + str(detail)))
    if not ok:
        FAILS.append(name)


def tmp():
    return tempfile.mkdtemp(prefix="dcsafe")


def proj(files=None, symlink=None):
    base, root = tmp(), tmp()
    for rel, data in (files or {}).items():
        p = Path(root) / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
    if symlink:
        os.symlink(*symlink(root))
    json.dump([{"name": "p", "path": root}], open(os.path.join(base, "projects.json"), "w"))
    return base, root


def raises(fn, *exc):
    try:
        fn()
    except exc:
        return True, ""
    except Exception as e:
        return False, "wrong exception %s" % type(e).__name__
    return False, "no exception"


# --- stdlib only (AST) ---
def t_stdlib():
    tree = ast.parse((HERE / "dashboard_chat.py").read_text())
    bad = []
    for n in ast.walk(tree):
        names = [a.name for a in n.names] if isinstance(n, ast.Import) else [n.module or ""] if isinstance(n, ast.ImportFrom) else []
        for m in names:
            if m.split(".")[0] not in sys.stdlib_module_names:
                bad.append(m)
    return not bad, bad
check("only stdlib imports", t_stdlib)

# --- session ids ---
for bad in (SID + "\n", "../" + SID, SID[:-1], SID.upper(), "", SID + "/x"):
    check("sid rejected %r" % bad[-6:], lambda b=bad: raises(lambda: dc.validate_session_id(b), ValueError))
    check("write_session rejects %r" % bad[-6:], lambda b=bad: raises(lambda: dc.write_session(tmp(), b, {}), ValueError))

# --- images ---
check("GIF87a/GIF89a ok, bare GIF8 not", lambda: ((dc.validate_image(b"GIF89a" + b"0" * 10), dc.validate_image(b"GIF87a" + b"0" * 10), dc.validate_image(b"GIF8xx0000")) == ("gif", "gif", None), None))
check("empty bytes rejected", lambda: (dc.validate_image(b"") is None, None))
check("RIFF non-WEBP rejected", lambda: (dc.validate_image(b"RIFF0000WAVE") is None, None))
check("7 images over limit", lambda: (dc.check_image_limits([PNG] * 7) is False, None))
check("8MB+1 over limit", lambda: (dc.check_image_limits([PNG + b"0" * (8 * 1024 * 1024)]) is False, None))

def t_store_cap():
    b = tmp()
    outs = [dc.stores_image(b, SID, PNG) for _ in range(7)]
    return all(not o.startswith("error") for o in outs[:6]) and outs[6].startswith("error"), outs[-1]
check("store caps at 6", t_store_cap)

def t_store_unique():
    b = tmp()
    d = Path(b) / "images" / SID
    p0 = dc.stores_image(b, SID, PNG)
    os.remove(p0)
    p1 = dc.stores_image(b, SID, PNG)
    p2 = dc.stores_image(b, SID, PNG)
    return len({p1, p2}) == 2 and Path(p1).read_bytes() == PNG, (p1, p2)
check("stored filenames never overwrite", t_store_unique)

# --- project confinement ---
def t_read(path, files=None, **kw):
    base, root = proj(files or {"a.py": b"print(1)\n"}, **kw)
    return lambda: dc.read_project_file(base, "p", path)

check("read ok", lambda: (t_read("a.py")() == "print(1)\n", None))
check("abs path rejected", lambda: raises(t_read("/etc/passwd"), ValueError))
check("dotdot rejected", lambda: raises(t_read("../x"), ValueError))
check("dotdot mid-path rejected", lambda: raises(t_read("sub/../../x"), ValueError))
check("symlink escape rejected", lambda: raises(t_read("lnk", symlink=lambda r: ("/etc/hosts", os.path.join(r, "lnk"))), ValueError))
check("NUL binary rejected", lambda: raises(t_read("b.bin", {"b.bin": b"ab\x00cd"}), ValueError))
check("256KB+1 rejected", lambda: raises(t_read("big.txt", {"big.txt": b"a" * (256 * 1024 + 1)}), ValueError))
check("256KB exactly ok", lambda: (len(t_read("big.txt", {"big.txt": b"a" * (256 * 1024)})()) == 256 * 1024, None))
for d in (".git", "node_modules", ".venv", "__pycache__"):
    check("read skips %s" % d, lambda d=d: raises(t_read(d + "/x.py", {d + "/x.py": b"secret"}), ValueError, FileNotFoundError))
check("unknown project", lambda: raises(lambda: dc.read_project_file(proj()[0], "nope", "a.py"), ValueError))
check("non-utf8 gives ValueError not crash", lambda: raises(t_read("l.txt", {"l.txt": b"\xff\xfe\xfa"}), ValueError))

def t_tree():
    base, root = proj({"a.py": b"x", "sub/b.py": b"x", ".git/c": b"x", "node_modules/d": b"x"})
    return sorted(dc.list_project_tree(base, "p")) == ["a.py", os.path.join("sub", "b.py")], dc.list_project_tree(base, "p")
check("tree skips junk dirs", t_tree)
check("tree path escape rejected", lambda: raises(lambda: dc.list_project_tree(proj()[0], "p", "../.."), ValueError))
check("tree prefix-sibling escape rejected", lambda: raises(lambda: dc.list_project_tree(*(lambda b, r: (b, "p", "../" + os.path.basename(r) + "x"))(*proj())), ValueError, FileNotFoundError))

# --- request shape: the model must actually see turns and file CONTENT ---
def t_roles():
    r = dc.build_request("p", [{"role": "user", "content": "q1"}, {"role": "assistant", "content": "a1"}, {"role": "user", "content": "q2"}], [], [])
    roles = [m["role"] for m in r["messages"]]
    return roles[0] == "system" and roles[1:] == ["user", "assistant", "user"], roles
check("history keeps separate roles", t_roles)

def t_filecontent():
    r = dc.build_request("p", [{"role": "user", "content": "q"}], [{"path": "a.py", "content": "SECRET_BODY"}], [])
    return "SECRET_BODY" in json.dumps(r) and "a.py" in json.dumps(r), None
check("attached file content reaches the model", t_filecontent)

def t_prompt():
    r = dc.build_request("p", [], [], [])
    s = json.dumps(r["messages"][0])
    return "only with fenced" not in s.lower() and "diff" in s.lower(), "system prompt forbids discussion"
check("system prompt allows conversation, asks diffs for changes", t_prompt)


def t_job_files():
    base, root = proj({"a.py": b"UNIQUE_BODY_123\n"})
    os.environ["DASHBOARD_CHAT_HOME"] = base
    dc.write_session(base, SID, {"project": "p", "created": "c", "messages": [
        {"role": "user", "content": "look", "files": ["a.py"], "images": []},
        {"role": "assistant", "content": "", "status": "queued", "files": [], "images": []}]})
    seen = []
    dc.job(SID, 1, base_dir=base, call=lambda r: (seen.append(r), {"status": "done", "text": "ok"})[1])
    return "UNIQUE_BODY_123" in json.dumps(seen), "job sends file paths only, never contents"
check("job() sends attached file CONTENT to the model", t_job_files)

def t_job_badfile():
    base, root = proj({"a.py": b"x"})
    dc.write_session(base, SID, {"project": "p", "created": "c", "messages": [
        {"role": "user", "content": "look", "files": ["../../etc/passwd"], "images": []},
        {"role": "assistant", "content": "", "status": "queued", "files": [], "images": []}]})
    seen = []
    dc.job(SID, 1, base_dir=base, call=lambda r: (seen.append(r), {"status": "done", "text": "ok"})[1])
    return "root:" not in json.dumps(seen), "escaped path content leaked"
check("job() never leaks files outside the project", t_job_badfile)

# --- key never leaks ---
def t_leak():
    h = tempfile.mkdtemp(); os.makedirs(h + "/.darkbloom")
    json.dump({"api_key": "KEY-abc123"}, open(h + "/.darkbloom/local.json", "w"))
    old = os.environ.get("HOME"); os.environ["HOME"] = h
    try:
        outs = [dc.call_model({"messages": []}, http_func=lambda u, hd, b, t: (200, b"{bad KEY-abc123")),
                dc.call_model({"messages": []}, http_func=lambda u, hd, b, t: (_ for _ in ()).throw(RuntimeError("KEY-abc123 boom"))),
                dc.call_model({"messages": []}, http_func=lambda u, hd, b, t: (500, b"KEY-abc123"))]
    finally:
        os.environ["HOME"] = old
    return all("KEY-abc123" not in json.dumps(o) for o in outs), outs
check("key never in call_model results", t_leak)

# --- routes ---
def t_routes():
    os.environ["DASHBOARD_CHAT_HOME"] = tmp()
    out = []
    for m, p, q, b in (("GET", "/nope", {}, b""), ("POST", "/api/chat/sessions", {}, b"not json"), ("POST", "/api/chat/sessions", {}, b"[]"),
                       ("GET", "/api/chat/sessions/../../etc", {}, b""), ("GET", "/api/chat/sessions/" + "z" * 32, {}, b""),
                       ("GET", "/api/chat/projects/p/file", {"path": ["../../etc/passwd"]}, b""),
                       ("POST", "/api/chat/sessions/" + SID + "/messages", {}, b'{"text": 5}')):
        try:
            s = dc.handle(m, p, q, b)[0]
        except Exception as e:
            s = "raised " + type(e).__name__
        out.append(s)
    return all(isinstance(s, int) and 400 <= s < 500 for s in out), out
check("hostile requests give 4xx, never raise", t_routes)

def t_cli_bad():
    import subprocess
    r = subprocess.run([sys.executable, str(HERE / "dashboard_chat.py"), "job", "../x", "1"], capture_output=True, text=True)
    return r.returncode == 1 and "Traceback" not in r.stderr, r.stderr[:100]
check("CLI bad id exits 1 cleanly", t_cli_bad)

def t_enqueue_argv():
    import stat
    b = tmp(); os.environ["DASHBOARD_CHAT_HOME"] = b
    (Path(b) / "sessions").mkdir()
    dc.write_session(b, SID, {"id": SID, "project": "p", "created": "", "messages": [
        {"role": "user", "content": "investigate X", "files": [], "images": [], "ts": "", "status": "completed"},
        {"role": "assistant", "content": "", "files": [], "images": [], "ts": "", "status": "queued", "mode": "job"}]})
    rec = Path(b) / "argv.txt"
    fake = Path(b) / "fakequeue.sh"
    fake.write_text('#!/bin/sh\necho "$@" > "%s"\n' % rec)
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
    os.environ["DASHBOARD_CHAT_QUEUE_CMD"] = str(fake)
    try:
        dc.enqueue_job(SID, 1)
    finally:
        del os.environ["DASHBOARD_CHAT_QUEUE_CMD"]
    argv = rec.read_text()
    task = (Path(b) / "jobs" / (SID + "-1") / "TASK.md").read_text()
    ok = ("--task-kind coding" in argv and "--capture-final-as ANSWER.md" in argv
          and "investigate X" in task and "ANSWER.md" in task and "enqueue" in argv)
    return ok, argv + task[:80]
check("enqueue_job: coding kind, answer-file instruction, uses overridable queue cmd (never the live queue)", t_enqueue_argv)

print("%d failed" % len(FAILS))
sys.exit(1 if FAILS else 0)
