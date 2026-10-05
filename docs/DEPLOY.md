# Deploy / sync

The Mac's live dashboard runs from this checkout (`src/`), so **a commit here is a
deploy once the API restarts**.

1. Edit `src/` (or `chat/chat.html`), run the dashboard tests from the repo root
   (see README "Live deployment"); for API glue changes also run
   `python3 tests/test-runstatus-retention-api.py --revert-check`.
2. Restart only `com.penn.ollama-queue-api` (bootout + bootstrap). Never restart
   `com.penn.ollama-queue-daemon` for a dashboard change; the API reads the queue
   state under its own flock and the daemon keeps running jobs.
3. Check `curl http://127.0.0.1:7684/`, `/api/bundle-history`, `/chat` return 200 and
   the public tunnel URL loads.
4. Commit + push (no secrets, no LAN IPs / personal hostnames: the repo is public).

Rules:

- Pipeline files (`ollama-queue.py`, `ollama-worker.py`, `handoff-emit.py`,
  `dispatch_progress.py`, gates, preflight) live in `~/bin`, not here. The API loads
  them from `~/bin` at runtime.
- `~/bin/{ollama-queue-api,bundle_view,dashboard_chat,runstatus_retention}.py` and the
  dashboard `test-*.py` are symlinks into this repo. If one turns back into a regular
  file, someone edited `~/bin` by replacement: diff it against `src/`, move the change
  here, and restore the symlink.
- The `machine-config` mirror of `~/bin` should treat these as symlinks (or skip them);
  this repo is their home.
- Rollback: `~/bin/*.bak-<ts>-prerepo` are the pre-move copies and
  `~/Library/LaunchAgents/com.penn.ollama-queue-api.plist.bak-<ts>` the old plist
  (pointed at `~/bin/ollama-queue-api.py`).
- `bin/` + `Dockerfile` are the Docker fork; changes to the live dashboard do not flow
  there automatically.
