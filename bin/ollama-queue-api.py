#!/usr/bin/env python3
"""Minimal HTTP API + frontend for ollama-queue.py, meant to sit behind the
existing access policy (e.g. Cloudflare Access) on your reverse proxy --
this process trusts every request that reaches it, since Access already
authenticated it before the tunnel connector ever proxies here. Do not expose
this port directly to the LAN/internet without Access in front of it.

Reuses ollama-queue.py's own _Locked/STATE_PATH so the daemon and this API
read/write the exact same state file under the exact same flock -- no second
source of truth, no separate lock to get out of sync.

Reordering only touches PENDING jobs: running/done/failed jobs keep their
slot, and the daemon already launches strictly in state["jobs"] list order
(see its "Launch pending jobs, FIFO order" loop), so a reorder here takes
effect on the daemon's next poll with no daemon-side change needed at all.
"""
import http.server
import importlib.util
import json
import os
import re
import signal
import socketserver
import sys
import time
import urllib.request
import urllib.parse
import urllib.error
import uuid
from pathlib import Path
from datetime import datetime, timezone

QUEUE_PATH = Path(__file__).resolve().parent / "ollama-queue.py"
TASKS_DIR = Path.home() / "bin" / "ollama-queue-logs" / "web-tasks"
PORT = int(os.environ.get("QUEUE_API_PORT", "7684"))

# Optional shared-secret gate. Historically this process trusted every request
# (Cloudflare Access sat in front). For a standalone/Docker deployment without
# Access, set QUEUE_API_TOKEN and every request must carry it as
# `Authorization: Bearer <token>` or `X-Api-Token: <token>`. Unset => open, as
# before (backward compatible).
API_TOKEN = os.environ.get("QUEUE_API_TOKEN") or None

# Tailed out of each job's log file for the dashboard -- ollama-worker.py logs
# these lines itself (added 2026-08-28: "--- iteration N/M ---" always did,
# the tok/s line is new). Only the LAST match in the tail matters; a job's log
# can have many of these, we only want current progress, not history.
_ITER_RE = re.compile(r"--- iteration (\d+)/(\d+) ---")
_TOKS_RE = re.compile(r"\(([\d.]+) tok/s\)")
_LOG_TAIL_BYTES = 16_384  # comfortably more than one iteration's log output

spec = importlib.util.spec_from_file_location("ollama_queue_lib", QUEUE_PATH)
q = importlib.util.module_from_spec(spec)
spec.loader.exec_module(q)

# Config-driven server registry -- same module ollama-worker.py loads. Lives in
# bin/ next to this file. Loading it here (rather than only via q.worker()) lets
# the settings endpoints load/save servers.json without importing the whole
# heavy worker module, and reflects edits immediately (worker caches its copy at
# import time in each dispatch subprocess).
_sc_spec = importlib.util.spec_from_file_location(
    "servers_config", str(Path(__file__).resolve().parent / "servers_config.py"))
servers_config = importlib.util.module_from_spec(_sc_spec)
_sc_spec.loader.exec_module(servers_config)


def _servers_file_url(name_or_url):
    """Resolve a server identifier (a configured name, or an explicit URL) to a
    base URL, or None if a name isn't configured."""
    if not name_or_url:
        return None
    if "://" in name_or_url:
        return name_or_url.rstrip("/")
    servers = servers_config.load_servers()
    spec = servers.get(name_or_url)
    return spec["url"].rstrip("/") if spec else None


def _host_reachable(url, timeout=2):
    try:
        req = urllib.request.Request(f"{url}/api/tags")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status == 200
    except Exception:
        return False


def _pick_v1_host(preferred=None):
    """Choose an Ollama base URL for /v1 proxying. `preferred` (a configured
    name or an explicit URL) wins if reachable; otherwise the first reachable
    configured host; otherwise the first configured host (so the caller still
    gets a real upstream error rather than a silent None)."""
    if preferred:
        url = _servers_file_url(preferred)
        if url and _host_reachable(url):
            return url
    # Ollama-only: /v1 proxies to an LLM host, never to a comfyui/img2vid/image
    # backend (those don't speak /api/tags), so filter them out here.
    servers = servers_config.ollama_hosts()
    urls = [spec["url"].rstrip("/") for spec in servers.values() if spec.get("url")]
    for url in urls:
        if _host_reachable(url):
            return url
    return urls[0] if urls else None

FRONTEND_HTML = r"""<!doctype html>
<html><head><meta charset="utf-8"><title>Ollama Queue</title>
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg%20xmlns%3D%22http%3A//www.w3.org/2000/svg%22%20viewBox%3D%220%200%20128%20128%22%20width%3D%22128%22%20height%3D%22128%22%20role%3D%22img%22%20aria-label%3D%22Ollama%20Queue%22%3E%0A%20%20%3Cdefs%3E%0A%20%20%20%20%3ClinearGradient%20id%3D%22g%22%20x1%3D%220%22%20y1%3D%220%22%20x2%3D%221%22%20y2%3D%221%22%3E%0A%20%20%20%20%20%20%3Cstop%20offset%3D%220%22%20stop-color%3D%22%235b8def%22/%3E%0A%20%20%20%20%20%20%3Cstop%20offset%3D%221%22%20stop-color%3D%22%237c4dff%22/%3E%0A%20%20%20%20%3C/linearGradient%3E%0A%20%20%3C/defs%3E%0A%20%20%3Crect%20x%3D%224%22%20y%3D%224%22%20width%3D%22120%22%20height%3D%22120%22%20rx%3D%2228%22%20fill%3D%22url%28%23g%29%22/%3E%0A%20%20%3C%21--%20queue%20stack%20--%3E%0A%20%20%3Crect%20x%3D%2228%22%20y%3D%2234%22%20width%3D%2252%22%20height%3D%2212%22%20rx%3D%226%22%20fill%3D%22%23ffffff%22%20opacity%3D%220.95%22/%3E%0A%20%20%3Crect%20x%3D%2228%22%20y%3D%2258%22%20width%3D%2252%22%20height%3D%2212%22%20rx%3D%226%22%20fill%3D%22%23ffffff%22%20opacity%3D%220.80%22/%3E%0A%20%20%3Crect%20x%3D%2228%22%20y%3D%2282%22%20width%3D%2252%22%20height%3D%2212%22%20rx%3D%226%22%20fill%3D%22%23ffffff%22%20opacity%3D%220.60%22/%3E%0A%20%20%3C%21--%20dispatch%20spark%20--%3E%0A%20%20%3Cpath%20d%3D%22M92%2030%20L74%2068%20H88%20L82%20100%20L104%2058%20H90%20Z%22%20fill%3D%22%23ffd54a%22%20stroke%3D%22%23ffffff%22%20stroke-width%3D%222%22%20stroke-linejoin%3D%22round%22/%3E%0A%3C/svg%3E%0A">
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  :root { color-scheme: light dark; }
  body { font-family: -apple-system, system-ui, sans-serif; max-width: 1000px; margin: 2rem auto; padding: 0 1rem; }
  h1 { font-size: 1.3rem; }
  table { width: 100%; border-collapse: collapse; margin-bottom: 2rem; }
  th, td { text-align: left; padding: .4rem .6rem; border-bottom: 1px solid #8884; font-size: .85rem; }
  /* Actions cell (always last) wrapped to a second line whenever a row happened to
     show more buttons than another (varies by position/status -- resume, promote-
     front, send-to-top, etc. show/hide per row), changing that row's height on every
     poll re-render and visibly shifting everything below it. nowrap forces a single
     line regardless of button count, trading a wider table for a stable layout. */
  td:last-child { white-space: nowrap; }
  tr[draggable="true"] { cursor: grab; }
  tr.dragging { opacity: .4; }
  .status-pending { color: #b8860b; } .status-running { color: #2e8b57; font-weight: 600; }
  .status-warming { color: #cc7a00; font-weight: 600; }
  .status-done { color: #4682b4; } .status-failed { color: #c0392b; } .status-paused { color: #9b59b6; }
  .status-done_unconverged { color: #cc7a00; font-weight: 600; }
  button { cursor: pointer; }
  button.iconbtn { font-size: .8rem; padding: .1rem .4rem; margin: 0 .1rem; }
  form { display: grid; gap: .5rem; max-width: 600px; }
  input, select, textarea { font: inherit; padding: .4rem; }
  textarea { min-height: 6rem; }
  #err { color: #c0392b; white-space: pre-wrap; }
  .toolbar { margin-bottom: .5rem; }
  .drop-target-hover { background: #fff3cd !important; outline: 2px dashed #d4a017; }
  tr.drop-indicator-above { box-shadow: inset 0 2px 0 0 #2e8b57; }
  tr.drop-indicator-below { box-shadow: inset 0 -2px 0 0 #2e8b57; }
  /* Exit-codes legend. Default: in normal flow (mobile-safe -- a fixed legend overlapped
     content on narrow screens). Only pin it to the top-left corner once the viewport is wide
     enough that a left:1rem fixed box clears the centered 1000px column (1000 + ~2*160 margin). */
  .exit-legend { display:inline-block; margin:0 0 .6rem 0; font-size:.8rem; color:#555;
    background:rgba(128,128,128,0.12); border-radius:6px; padding:.6rem .8rem; line-height:1.6; }
  @media (min-width: 1320px) {
    .exit-legend { position:fixed; top:1rem; left:1rem; margin:0; z-index:10; }
  }
</style></head>
<body>
<h1>Ollama Queue</h1>
<div class="toolbar"><button id="clearFinished">Clear finished (done/failed)</button></div>
<div class="exit-legend">
  <strong>Exit Codes:</strong><br>
  0 - Converged<br>
  1 - Verify Failed<br>
  2 - Iteration Cap<br>
  3 - Paused, Resumable<br>
  4 - Refused to Start<br>
  5 - Done, Unconverged
</div>
<div style="overflow-x: auto;">
<table id="jobs"><thead><tr>
  <th></th><th>label</th><th>status</th><th>elapsed</th><th>model</th><th>progress</th><th>tok/s</th><th>lane</th><th>pid</th><th>exit</th><th></th>
</tr></thead><tbody></tbody></table>
</div>

<div id="livelogBackdrop" style="display:none; position:fixed; inset:0; background:rgba(0,0,0,0.4); z-index:999;"></div>
<div id="livelogModal" style="display:none; flex-direction:column; position:fixed; top:5%; left:5%; right:5%; bottom:5%; background:#fff; border:2px solid #333; z-index:1000; box-shadow:0 4px 20px rgba(0,0,0,.3);">
  <div style="display:flex; justify-content:space-between; align-items:center; padding:.75rem 1rem; border-bottom:1px solid #ccc; flex-shrink:0; background:#fff;">
    <h3 id="livelogTitle" style="margin:0;"></h3>
    <button id="closeLivelog">close</button>
  </div>
  <pre id="livelogContent" style="flex:1; overflow:auto; margin:0; padding:1rem; white-space:pre-wrap; font-size:.8rem;"></pre>
</div>

<div style="display:flex; flex-wrap:wrap; gap:2rem;">
  <div style="flex:1; min-width:300px;">   <!-- LEFT -->
    <h2>Handoff Status</h2>
    <div id="handoffPanel">
      <h3>Complete — awaiting action</h3>
      <table style="width: auto; border-collapse: collapse; margin-bottom: 1rem;">
        <thead>
          <tr>
            <th>Label</th>
            <th>Status</th>
            <th>Gate</th>
            <th>Files</th>
            <th>Action</th>
          </tr>
        </thead>
        <tbody id="handoffCompleteBody">
          <!-- Data will be populated by JavaScript -->
        </tbody>
      </table>

      <h3>Pending</h3>
      <table style="width: auto; border-collapse: collapse; margin-bottom: 1rem;">
        <thead>
          <tr>
            <th>Label</th>
            <th>Status</th>
            <th>Gate</th>
            <th>Files</th>
          </tr>
        </thead>
        <tbody id="handoffPendingBody">
          <!-- Data will be populated by JavaScript -->
        </tbody>
      </table>
    </div>
  </div>
  <div style="flex:1; min-width:300px;">   <!-- RIGHT -->
    <h2>Web Search Usage</h2>
    <div id="webSearchUsage">
      <table style="width: auto; border-collapse: collapse; margin-bottom: 1rem;">
        <thead>
          <tr>
            <th></th>
            <th>Today</th>
            <th>Last 7 Days</th>
            <th>Total</th>
          </tr>
        </thead>
        <tbody id="webSearchUsageBody">
          <!-- Data will be populated by JavaScript -->
        </tbody>
      </table>
    </div>
  </div>
</div>

<h2>Loaded right now</h2>
<div id="hosts"></div>

<h2>Settings — backends</h2>
<p style="font-size:.85rem; color:#8888;">The backend services the queue dispatches to. <b>ollama</b> hosts run LLM jobs
(routed by model-fit); <b>comfyui / img2vid / image</b> are HTTP services the
queue POSTs image/video jobs to (see <code>docs/BACKENDS.md</code>). Edits are
saved to <code>config/servers.json</code> and picked up by new dispatches
immediately. <code>usable_bytes</code> is the VRAM/memory budget used for Ollama
model-fit routing (0 = unknown; not needed for non-Ollama backends).</p>
<table id="serversTbl">
  <thead><tr><th>Name</th><th>Type</th><th>URL</th><th>usable_bytes</th><th></th></tr></thead>
  <tbody id="serversBody"></tbody>
</table>
<form id="serverForm" style="max-width:760px;">
  <div style="display:flex; gap:.5rem; flex-wrap:wrap;">
    <input id="srvName" placeholder="name (e.g. gpu-box)" style="flex:1; min-width:120px;">
    <select id="srvType" style="flex:1; min-width:110px;">
      <option value="ollama">ollama</option>
      <option value="comfyui">comfyui</option>
      <option value="img2vid">img2vid</option>
      <option value="image">image</option>
    </select>
    <input id="srvUrl" placeholder="http://192.0.2.10:11434" style="flex:2; min-width:200px;">
    <input id="srvBytes" placeholder="usable_bytes (ollama only)" style="flex:1; min-width:120px;">
    <button type="submit">Add / update</button>
  </div>
  <div id="srvErr" style="color:#c0392b;"></div>
</form>

<script>
const tbody = document.querySelector('#jobs tbody');
let dragId = null;
let dragStartedAt = null;

const transitioning = {};  // jobId -> {type: 'pausing'|'starting', since: timestamp}, client-side
                            // only visual feedback for the daemon's real ~15-30s pause/relaunch
                            // cycle latency, cleared once the real status changes or after 60s.
function markTransitioning(jobId, type) {
  transitioning[jobId] = {type, since: Date.now()};
}


function formatElapsed(s) {
  if (s === null || s === undefined) return '0:00';
  s = Math.floor(s);
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), sec = s % 60;
  const pad = n => String(n).padStart(2, '0');
  return h > 0 ? `${h}:${pad(m)}:${pad(sec)}` : `${m}:${pad(sec)}`;
}

async function moveJob(id, beforeId) {
  await fetch('/api/jobs/move', {method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({id, before_id: beforeId || null})});
  refresh();
}

let livelogInterval = null;

async function loadLivelog(id) {
  const res = await fetch('/api/jobs/' + id + '/livelog');
  const el = document.getElementById('livelogContent');
  if (!res.ok) {
    el.textContent = await res.text();
    return;
  }
  // Only auto-scroll if the user was already at (or near) the bottom -- otherwise
  // someone scrolled up to read older history would get yanked back down on every
  // refresh, which is worse than not auto-scrolling at all.
  const wasNearBottom = el.scrollHeight - el.scrollTop - el.clientHeight < 40;
  const data = await res.json();
  el.textContent = data.content + (data.truncated ? '\n\n[older lines truncated]' : '');
  if (wasNearBottom) {
    el.scrollTop = el.scrollHeight;
  }
}

function openLivelog(id, label) {
  document.getElementById('livelogTitle').textContent = label;
  document.getElementById('livelogBackdrop').style.display = 'block';
  document.getElementById('livelogModal').style.display = 'flex';
  loadLivelog(id).then(() => {
    // Jump to bottom on first open regardless of the near-bottom check above
    // (there's no "previous scroll position" yet on a fresh open).
    const el = document.getElementById('livelogContent');
    el.scrollTop = el.scrollHeight;
  });
  if (livelogInterval) clearInterval(livelogInterval);
  livelogInterval = setInterval(() => loadLivelog(id), 1500);
}

function closeLivelog() {
  document.getElementById('livelogModal').style.display = 'none';
  document.getElementById('livelogBackdrop').style.display = 'none';
  if (livelogInterval) { clearInterval(livelogInterval); livelogInterval = null; }
}

document.getElementById('closeLivelog').addEventListener('click', closeLivelog);
document.getElementById('livelogBackdrop').addEventListener('click', closeLivelog);

// paused shares pending's tier (1), not its own -- a paused job can sit interleaved
// WITHIN the true pending sequence (its position matters for what launches next once
// resumed, same as any pending job), so the display must not split them into separate
// visual groups: that mismatch between what's shown and what reorder buttons actually
// operate on (pendingIds, in true array order) is exactly what made "send to top" look
// broken -- confirmed live 2026-08-29, a paused job several jobs deep in the real order
// displayed in a totally separate section below ALL pending rows, so a reorder that
// correctly landed a row at position 0 of the true order looked like "it only moved one
// spot" relative to the wrong (grouped-by-status) visual reference point.
const STATUS_ORDER = {running: 0, pending: 1, paused: 1, failed: 3, done: 4, done_unconverged: 5};

async function refresh() {
  // A live drag is in progress -- don't let a poll-driven re-render wipe out the
  // in-progress visual reorder below. Bounded: if dragId has been stuck for more
  // than 8s, a dragend/drop somewhere failed to clear it (confirmed real 2026-08-29
  // -- a synchronous confirm() inside a drop handler can disrupt the drag session
  // so dragend never fires, permanently freezing every future refresh() otherwise)
  // -- force-clear it and proceed rather than leave the whole dashboard stuck until
  // a manual page reload.
  if (dragId) {
    // Check if there's actually an ongoing drag by looking for elements with 'dragging' class
    const isActuallyDragging = document.querySelector('tr.dragging') !== null;
    if (isActuallyDragging) {
      if (dragStartedAt && (Date.now() - dragStartedAt) < 8000) return;
    } else {
      // No actual drag in progress, clear the stale dragId
      dragId = null;
      dragStartedAt = null;
    }
  }
  const res = await fetch('/api/jobs');
  const jobs = await res.json();
  // pendingIds MUST reflect true FIFO enqueue order (the daemon's actual launch
  // order), not the display sort below -- reorder buttons compute neighbors
  // from this, and it has to match what the daemon itself iterates over.
  // Includes 'paused' jobs too (added 2026-08-29, Penn's request) -- a paused job
  // isn't launchable yet, but its position in this list still determines where it
  // lands once resumed, and pending/paused jobs share the same reorder controls.
  const pendingIds = jobs.filter(j => j.status === 'pending' || j.status === 'paused').map(j => j.id);
  // Display-only: running jobs on top, so what's actually happening right now
  // doesn't get lost below a long pending/finished list. Stable within each
  // status group (Array.sort is stable), so relative order otherwise unchanged.
  const displayJobs = [...jobs].filter(j => j.status !== 'done' && j.status !== 'done_unconverged').sort((a, b) =>
    (STATUS_ORDER[a.status] ?? 5) - (STATUS_ORDER[b.status] ?? 5));
  tbody.innerHTML = '';
  for (const j of displayJobs) {
    const tr = document.createElement('tr');
    tr.dataset.id = j.id;
    tr.style.cursor = 'pointer';
    tr.addEventListener('click', e => {
      if (e.target.tagName === 'BUTTON') return;
      openLivelog(j.id, j.label);
    });
    const isPending = j.status === 'pending' || j.status === 'paused';
    const isRunning = j.status === 'running';
    // paused is removable too: it has no live process (the worker already exited), so removing
    // its state entry is a plain safe edit -- without this, a paused job could never be cleared.
    const isRemovable = j.status === 'pending' || j.status === 'done' || j.status === 'done_unconverged' || j.status === 'failed'
      || j.status === 'paused';
    if (isPending) {
      tr.draggable = true;
      tr.addEventListener('dragstart', () => { dragId = j.id; dragStartedAt = Date.now(); tr.classList.add('dragging'); });
      tr.addEventListener('dragend', () => {
        tr.classList.remove('dragging');
        tbody.querySelectorAll('.drop-indicator-above, .drop-indicator-below').forEach(el =>
          el.classList.remove('drop-indicator-above', 'drop-indicator-below'));
        dragId = null;
        dragStartedAt = null;
        refresh();
      });
      tr.addEventListener('dragover', e => {
        e.preventDefault();
        if (!dragId || dragId === j.id) return;
        // Indicator-only: track where we're hovering, don't mutate the DOM here at all.
        // The old approach (physically moving the dragged row on every dragover) was
        // unreliable across many rows -- dragover doesn't fire for every row during a
        // fast drag, so multi-row moves got dropped or landed inconsistently. This
        // computes the final position once, at drop time, from whichever row you were
        // last actually over -- robust regardless of how many rows you crossed to get there.
        tbody.querySelectorAll('.drop-indicator-above, .drop-indicator-below').forEach(el =>
          el.classList.remove('drop-indicator-above', 'drop-indicator-below'));
        const rect = tr.getBoundingClientRect();
        const before = (e.clientY - rect.top) < rect.height / 2;
        tr.classList.add(before ? 'drop-indicator-above' : 'drop-indicator-below');
        tr.dataset.dropBefore = before ? '1' : '0';
      });
      tr.addEventListener('drop', async e => {
        e.preventDefault();
        if (!dragId || dragId === j.id) return;
        const before = tr.dataset.dropBefore === '1';
        const pos = pendingIds.indexOf(j.id);
        const beforeId = before ? j.id : (pendingIds[pos + 1] || null);
        const movedId = dragId;
        dragId = null;
        tbody.querySelectorAll('.drop-indicator-above, .drop-indicator-below').forEach(el =>
          el.classList.remove('drop-indicator-above', 'drop-indicator-below'));
        moveJob(movedId, beforeId);
      });
    } else if (isRunning) {
      // Drop target for PENDING rows: dropping one here pauses THIS running job gracefully
      // (SIGTERM -> worker finishes its iteration, saves its transcript, exits 3) and runs the
      // dropped one first -- the daemon's promote flow.
      tr.title = 'Drag a queued job onto this row to run it now (pauses this job, resumable later)';
      tr.addEventListener('dragover', e => {
        e.preventDefault();
        tr.classList.add('drop-target-hover');
      });
      tr.addEventListener('dragleave', () => tr.classList.remove('drop-target-hover'));
      tr.addEventListener('drop', async e => {
        e.preventDefault();
        if (!dragId || dragId === j.id) return;
        const dragged = displayJobs.find(x => x.id === dragId);
        // Only pending rows are draggable, so this is belt-and-braces.
        if (!dragged || dragged.status !== 'pending') return;
        if (!confirm(`Pause ${j.label} and run ${dragged.label} now?`)) return;
        tr.classList.remove('drop-target-hover');
        markTransitioning(j.id, 'pausing');
        markTransitioning(dragId, 'starting');
        await fetch('/api/jobs/' + dragId + '/promote', {method: 'POST'});
        refresh();
      });
    }
    const progress = (j.iteration != null && j.max_iters != null) ? `${j.iteration}/${j.max_iters}` : '';
    let statusLabel = j.phase === 'warming' ? 'warming' : j.status;
    const _t = transitioning[j.id];
    if (_t) {
      if (Date.now() - _t.since > 60000) {
        delete transitioning[j.id];
      } else if (_t.type === 'pausing' && j.status === 'running') {
        statusLabel = 'pausing...';
      } else if (_t.type === 'starting' && j.status === 'pending') {
        statusLabel = 'starting...';
      } else {
        delete transitioning[j.id];
      }
    }
    const toks = j.tok_s != null ? j.tok_s.toFixed(1) : '';
    // 0:00 for pending (elapsed_s null); live wall-time for running (recomputed
    // server-side from log ctime each refresh); frozen final duration once terminal.
    const elapsed = formatElapsed(j.elapsed_s);
    let actions = '';
    if (isPending) {
      const pos = pendingIds.indexOf(j.id);
      // Up: move before the pending job currently two slots earlier (i.e.
      // ahead of the one directly preceding this one). Down: move before the
      // pending job currently two slots later, or to the end if none.
      if (pos > 0) actions += `<button class="iconbtn" data-top title="Send to top of queue">&uarr;&uarr;</button>`;
      if (pos > 0) actions += `<button class="iconbtn" data-up>&uarr;</button>`;
      if (pos === 0) actions += `<button class="iconbtn" data-promote-front title="Pause whatever is running and run this one now">&uarr;</button>`;
      if (pos < pendingIds.length - 1) actions += `<button class="iconbtn" data-down>&darr;</button>`;
    }
    if (j.status === 'paused') actions += `<button class="iconbtn" data-resume>resume</button>`;
    if (isRemovable) actions += `<button class="iconbtn" data-remove>remove</button>`;
    if (j.status === 'running') actions += `<button class="iconbtn" data-kill title="Gracefully pauses the job (SIGTERM) -- it saves state and can be resumed, this does not discard work">pause</button>`;
    if (j.status === 'running') actions += `<button class="iconbtn" data-kill title="Gracefully pauses the job (SIGTERM) -- it saves state and can be resumed, this does not discard work">&darr;</button>`;
    tr.innerHTML = `
      <td${isPending ? ' title="Drag to reorder, or drop onto the running job to run this one now"' : ''}>${isPending ? '☰' : ''}</td>
      <td>${j.label}</td>
      <td class="status-${j.phase === 'warming' ? 'warming' : j.status}">${statusLabel}</td>
      <td>${elapsed}</td>
      <td>${j.model}</td>
      <td>${progress}</td>
      <td>${toks}</td>
      <td>${j.lane || j.host_pref}</td>
      <td>${j.pid ?? ''}</td>
      <td>${j.exit_code ?? ''}</td>
      <td>${actions}</td>
    `;
    const topBtn = tr.querySelector('[data-top]');
    if (topBtn) topBtn.addEventListener('click', async () => {
      // Robust send-to-top: hit the server-side promote endpoint, which inserts
      // this job at the FRONT of the pending queue atomically under the state
      // lock (preempt=false: it does NOT pause the running job, just jumps the
      // line). The old path -- moveJob(j.id, pendingIds[0]) -- computed the
      // neighbor from the render-time pendingIds snapshot, so under this queue's
      // constant churn (a gate auto-enqueues on every completion) pendingIds[0]
      // was often stale by click time and the relative insert landed mid-array,
      // not at the top. That is the "move to top doesn't stick" bug. The server
      // recomputes "front" itself, so it can't go stale.
      await fetch('/api/jobs/' + j.id + '/promote', {method: 'POST'});
      refresh();
    });
    const promoteFrontBtn = tr.querySelector('[data-promote-front]');
    if (promoteFrontBtn) promoteFrontBtn.addEventListener('click', async () => {
      const runningJob = displayJobs.find(x => x.status === 'running');
      const runningLabel = runningJob ? runningJob.label : 'the current job';
      if (!confirm(`Pause ${runningLabel} and run ${j.label} now?`)) return;
      if (runningJob) markTransitioning(runningJob.id, 'pausing');
      markTransitioning(j.id, 'starting');
      // preempt:true -- this is the "run this NOW" button whose confirm promises
      // to pause the running job. Without the flag the server ran promote_job()
      // at its preempt=false default and did NOT pause anything, so the button
      // silently lied (job jumped the line but the running one kept its lane).
      await fetch('/api/jobs/' + j.id + '/promote', {method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({preempt: true})});
      refresh();
    });
    const upBtn = tr.querySelector('[data-up]');
    if (upBtn) upBtn.addEventListener('click', () => {
      const pos = pendingIds.indexOf(j.id);
      moveJob(j.id, pendingIds[pos - 1]);
    });
    const downBtn = tr.querySelector('[data-down]');
    if (downBtn) downBtn.addEventListener('click', () => {
      const pos = pendingIds.indexOf(j.id);
      // moving down means landing after the next one, i.e. before whatever
      // currently follows it (or the end, if it's now second-to-last).
      moveJob(j.id, pendingIds[pos + 2]);
    });
    const removeBtn = tr.querySelector('[data-remove]');
    if (removeBtn) removeBtn.addEventListener('click', async () => {
      await fetch('/api/jobs/' + j.id, {method: 'DELETE'});
      refresh();
    });
    const resumeBtn = tr.querySelector('[data-resume]');
    if (resumeBtn) resumeBtn.addEventListener('click', async () => {
      await fetch('/api/jobs/' + j.id + '/resume', {method: 'POST'});
      refresh();
    });
    const killBtns = tr.querySelectorAll('[data-kill]');
    killBtns.forEach(killBtn => killBtn.addEventListener('click', async () => {
      if (!confirm(`Pause "${j.label}" (pid ${j.pid})? This gracefully stops it (SIGTERM) -- state is saved and it can be resumed later, this does not discard work.`)) return;
      markTransitioning(j.id, 'pausing');
      await fetch('/api/jobs/' + j.id + '/kill', {method: 'POST'});
      refresh();
    }));
    tbody.appendChild(tr);
  }
}

document.getElementById('clearFinished').addEventListener('click', async () => {
  const res = await fetch('/api/jobs');
  const jobs = await res.json();
  for (const j of jobs.filter(j => j.status === 'done' || j.status === 'failed' || j.status === 'done_unconverged')) {
    await fetch('/api/jobs/' + j.id, {method: 'DELETE'});
  }
  refresh();
});

async function refreshHosts() {
  const res = await fetch('/api/hosts');
  const h = await res.json();
  const fmtModels = models => models.length
    ? models.map(m => `${m.name} (${m.size_gb}GB)`).join(', ')
    : '<em>idle</em>';
  const fmtMem = (used, total) => (total && total > 0)
    ? ` — ${used}/${total} GB (${Math.round(100 * used / total)}%)`
    : '';
  // Studio's native Ollama and the llama-server bypass are TWO PROCESSES ON
  // THE SAME PHYSICAL MACHINE, sharing its one 64GB memory pool -- grouped
  // visually under one "Studio" heading so that's obvious at a glance,
  // instead of reading as three unrelated hosts.
  document.getElementById('hosts').innerHTML = `
    <table><tbody>
      <tr><td colspan="2"><strong>Studio</strong> (one machine, 64GB shared pool)${fmtMem(h.studio.used_gb, h.studio.total_gb)}</td></tr>
      <tr><td style="padding-left:1.5rem">native Ollama</td><td>${fmtModels(h.studio.models)}</td></tr>
      <tr><td style="padding-left:1.5rem">llama-server bypass (qwen3.8)</td>
          <td>${h.llama_server_bypass.up ? `${h.llama_server_bypass.model} (up)` : '<em>down</em>'}</td></tr>
      <tr><td colspan="2">&nbsp;</td></tr>
      <tr><td><strong>Unraid</strong>${fmtMem(h.unraid.used_gb, h.unraid.total_gb)}</td><td>${fmtModels(h.unraid.models)}</td></tr>
    </tbody></table>
  `;
}

refresh();
refreshHosts();
setInterval(refresh, 4000);
setInterval(refreshHosts, 4000);

// Fetch and display web search usage data
async function refreshWebSearchUsage() {
  try {
    const res = await fetch('/api/web-search-usage');
    const data = await res.json();
    
    const tbody = document.getElementById('webSearchUsageBody');
    tbody.innerHTML = '';
    
    // Define the backends in order
    const backends = ['tavily', 'brave', 'searxng', 'ollama'];
    
    // Create rows for each backend
    backends.forEach(backend => {
      const row = document.createElement('tr');
      
      // Backend name cell
      const nameCell = document.createElement('td');
      nameCell.textContent = backend.charAt(0).toUpperCase() + backend.slice(1);
      nameCell.style.fontWeight = 'bold';
      row.appendChild(nameCell);
      
      // Today cell
      const todayCell = document.createElement('td');
      todayCell.textContent = data.today[backend] || 0;
      row.appendChild(todayCell);
      
      // Last 7 days cell
      const last7dCell = document.createElement('td');
      last7dCell.textContent = data.last7d[backend] || 0;
      row.appendChild(last7dCell);
      
      // Total cell
      const totalCell = document.createElement('td');
      totalCell.textContent = data.total[backend] || 0;
      row.appendChild(totalCell);
      
      tbody.appendChild(row);
    });
  } catch (error) {
    console.error('Error fetching web search usage:', error);
  }
}

// Initial load and refresh every 5 seconds
refreshWebSearchUsage();
setInterval(refreshWebSearchUsage, 5000);

// Fetch and display handoff data
async function refreshHandoff() {
  try {
    const res = await fetch('/api/handoff');
    const data = await res.json();
    
    const completeTbody = document.getElementById('handoffCompleteBody');
    const pendingTbody = document.getElementById('handoffPendingBody');
    
    // Clear existing content
    completeTbody.innerHTML = '';
    pendingTbody.innerHTML = '';
    
    // Process complete jobs (awaiting action)
    if (data.complete && data.complete.length > 0) {
      data.complete.forEach(job => {
        const row = document.createElement('tr');
        
        // Create button for marking as acted
        const actionCell = document.createElement('td');
        const markActedBtn = document.createElement('button');
        markActedBtn.textContent = 'Mark Acted';
        markActedBtn.className = 'iconbtn';
        markActedBtn.onclick = () => markJobAsActed(job.id);
        
        actionCell.appendChild(markActedBtn);
        
        row.innerHTML = `
          <td>${job.label}</td>
          <td>${job.status}</td>
          <td>${job.gate}</td>
          <td>${job.files}</td>
        `;
        row.appendChild(actionCell);
        completeTbody.appendChild(row);
      });
    } else {
      const row = document.createElement('tr');
      row.innerHTML = '<td colspan="5">No completed jobs awaiting action</td>';
      completeTbody.appendChild(row);
    }
    
    // Process pending jobs
    if (data.pending && data.pending.length > 0) {
      data.pending.forEach(job => {
        const row = document.createElement('tr');
        row.innerHTML = `
          <td>${job.label}</td>
          <td>${job.status}</td>
          <td>${job.gate}</td>
          <td>${job.files}</td>
        `;
        pendingTbody.appendChild(row);
      });
    } else {
      const row = document.createElement('tr');
      row.innerHTML = '<td colspan="4">No pending jobs</td>';
      pendingTbody.appendChild(row);
    }
  } catch (error) {
    console.error('Error fetching handoff data:', error);
  }
}

async function markJobAsActed(jobId) {
  try {
    // First validate that the job ID exists in either complete or pending
    const res = await fetch('/api/handoff');
    const data = await res.json();
    
    let isValid = false;
    if (data.complete && data.complete.some(job => job.id === jobId)) {
      isValid = true;
    } else if (data.pending && data.pending.some(job => job.id === jobId)) {
      isValid = true;
    }
    
    if (!isValid) {
      alert('Invalid job ID: ' + jobId);
      return;
    }
    
    // Send POST request to mark as acted
    const response = await fetch('/api/handoff/acted', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ id: jobId })
    });
    
    if (response.ok) {
      // Refresh the panel to show updated status
      refreshHandoff();
    } else {
      const errorText = await response.text();
      alert('Error marking job as acted: ' + errorText);
    }
  } catch (error) {
    console.error('Error marking job as acted:', error);
    alert('Error marking job as acted');
  }
}

// Initial load and refresh every 5 seconds
refreshWebSearchUsage();
setInterval(refreshWebSearchUsage, 5000);

// Refresh handoff data periodically
refreshHandoff();
setInterval(refreshHandoff, 5000);

// ---- Settings: Ollama server registry -----------------------------------
function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, c =>
    ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}
async function refreshServers() {
  try {
    const res = await fetch('/api/servers');
    const data = await res.json();
    const body = document.getElementById('serversBody');
    body.innerHTML = (data.servers || []).map(s => `
      <tr>
        <td>${escapeHtml(s.name)}</td>
        <td>${escapeHtml(s.type || 'ollama')}</td>
        <td><code>${escapeHtml(s.url)}</code></td>
        <td>${s.usable_bytes || 0}</td>
        <td><button class="iconbtn" data-edit='${escapeHtml(JSON.stringify(s))}'>edit</button>
            <button class="iconbtn" data-del="${escapeHtml(s.name)}">delete</button></td>
      </tr>`).join('');
    body.querySelectorAll('[data-del]').forEach(b => b.addEventListener('click', async () => {
      const name = b.getAttribute('data-del');
      if (!confirm(`Remove server "${name}"?`)) return;
      await fetch('/api/hosts/' + encodeURIComponent(name), {method: 'DELETE'});
      refreshServers();
    }));
    body.querySelectorAll('[data-edit]').forEach(b => b.addEventListener('click', () => {
      const s = JSON.parse(b.getAttribute('data-edit'));
      document.getElementById('srvName').value = s.name;
      document.getElementById('srvType').value = s.type || 'ollama';
      document.getElementById('srvUrl').value = s.url;
      document.getElementById('srvBytes').value = s.usable_bytes || '';
    }));
  } catch (e) { /* settings panel is best-effort */ }
}
document.getElementById('serverForm').addEventListener('submit', async (ev) => {
  ev.preventDefault();
  const err = document.getElementById('srvErr');
  err.textContent = '';
  const name = document.getElementById('srvName').value.trim();
  const type = document.getElementById('srvType').value;
  const url = document.getElementById('srvUrl').value.trim();
  const bytesRaw = document.getElementById('srvBytes').value.trim();
  const payload = {name, type, url, usable_bytes: bytesRaw ? parseInt(bytesRaw, 10) : 0};
  const res = await fetch('/api/hosts', {method: 'POST',
    headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)});
  if (!res.ok) { err.textContent = await res.text(); return; }
  document.getElementById('srvName').value = '';
  document.getElementById('srvType').value = 'ollama';
  document.getElementById('srvUrl').value = '';
  document.getElementById('srvBytes').value = '';
  refreshServers();
});
refreshServers();
setInterval(refreshServers, 10000);

// Add a global mouseup handler to ensure drag state is cleared even if drop handlers don't fire properly
document.addEventListener('mouseup', () => {
  // Clear drag state on any mouseup event, but only if we have an active dragId
  if (dragId) {
    const isActuallyDragging = document.querySelector('tr.dragging') !== null;
    if (!isActuallyDragging) {
      dragId = null;
      dragStartedAt = null;
    }
  }
});
</script>
</body></html>
"""


def _tail_progress(log_path):
    """Read the end of a job's log file and pull out the most recent
    iteration/cap, tok/s, and warmup phase the worker logged. Best-effort:
    any failure (missing file, no matches yet, race with the writer) just
    means no progress data this poll, not an error."""
    if not log_path:
        return None, None, None, False
    try:
        with open(log_path, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - _LOG_TAIL_BYTES))
            tail = f.read().decode("utf-8", errors="replace")
    except OSError:
        return None, None, None, False
    iter_matches = _ITER_RE.findall(tail)
    toks_matches = _TOKS_RE.findall(tail)
    iteration, max_iters = (int(iter_matches[-1][0]), int(iter_matches[-1][1])) if iter_matches else (None, None)
    tok_s = float(toks_matches[-1]) if toks_matches else None
    # ollama-worker.py logs "warming up <model> (loading into memory, up to
    # 900s)..." then, once resolved, "<model> loaded and warm (Xs)." -- still
    # warming iff the former appears in the tail with no matching resolution
    # after it (checked via index, not just "did the phrase appear anywhere",
    # since the tail window can span a real warmup from an EARLIER attempt --
    # e.g. after a daemon-triggered eviction+reload -- that already resolved).
    warm_idx = tail.rfind("warming up")
    warming = warm_idx != -1 and "loaded and warm" not in tail[warm_idx:]
    return iteration, max_iters, tok_s, warming


def _ollama_resident_models(url):
    """Models currently resident on an Ollama host, via /api/ps. Empty list
    on any failure (host down, timeout) -- this is a status display, not a
    dependency anything else here relies on, so a failure is just "unknown"
    not an error worth surfacing loudly."""
    try:
        req = urllib.request.Request(f"{url}/api/ps")
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read())
        return [{"name": m["name"], "size_gb": round(m.get("size_vram", m.get("size", 0)) / 1e9, 1)}
                for m in data.get("models", [])]
    except Exception:
        return []


def _hosts_summary():
    """What's actually resident right now on each host, independent of the
    job queue -- Penn's ask: visibility into GPU/memory occupancy even when
    nothing is currently dispatched (an idle lane can still have a model
    sitting loaded from keep_alive, which is exactly the state that caused
    tonight's OOM incidents)."""
    # Read straight from the servers file so edits made via the settings UI show
    # up without restarting the API. Defensive .get(): a config that renames or
    # removes "studio"/"unraid" must not 500 this legacy occupancy panel.
    servers = servers_config.load_servers()
    studio_url = servers.get("studio", {}).get("url", "http://127.0.0.1:11434")
    unraid_url = servers.get("unraid", {}).get("url", "")
    bypass_up = False
    bypass_url = getattr(q, "LLAMA_SERVER_QWEN38_URL", None)
    if bypass_url:
        try:
            req = urllib.request.Request(f"{bypass_url}/health")
            with urllib.request.urlopen(req, timeout=3) as resp:
                bypass_up = resp.status == 200
        except Exception:
            bypass_up = False
    studio_models = _ollama_resident_models(studio_url)
    unraid_models = _ollama_resident_models(unraid_url)
    # Studio = the box we're running on (unified memory). Unraid = the 3080's
    # VRAM (12GB, hardcoded -- Penn can correct). used = sum of resident model
    # footprints reported by /api/ps.
    studio_total_gb = round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 1e9) if hasattr(os, "sysconf") else 0
    studio_used_gb = round(sum(m["size_gb"] for m in studio_models), 1)
    unraid_total_gb = 12
    unraid_used_gb = round(sum(m["size_gb"] for m in unraid_models), 1)
    return {
        "studio": {"url": studio_url, "models": studio_models, "total_gb": studio_total_gb, "used_gb": studio_used_gb},
        "unraid": {"url": unraid_url, "models": unraid_models, "total_gb": unraid_total_gb, "used_gb": unraid_used_gb},
        "llama_server_bypass": {"url": bypass_url, "up": bypass_up,
                                 "model": getattr(q, "LLAMA_SERVER_QWEN38_MODEL", None) if bypass_up else None},
    }


def _job_summary(j):
    d = {k: j.get(k) for k in
         ("id", "label", "model", "host_pref", "status", "lane", "pid", "exit_code", "error", "enqueued_at")}
    iteration, max_iters, tok_s, warming = _tail_progress(j.get("log_path"))
    d["iteration"] = iteration
    d["max_iters"] = max_iters
    d["tok_s"] = tok_s
    # Only meaningful overlaid on a "running" job -- iteration is still None
    # at that point precisely because it hasn't started iterating yet.
    d["phase"] = "warming" if (j.get("status") == "running" and warming and iteration is None) else None
    # Elapsed wall-time from the job's log file: ctime = daemon launch. Running ->
    # now - ctime (live); terminal -> mtime - ctime (frozen). None (=> 0:00) when
    # no log yet (pending). Best-effort: any stat error just means no elapsed.
    log_path = j.get("log_path")
    elapsed_s = None
    if log_path:
        try:
            st = os.stat(log_path)
            # st_ctime on macOS/Linux is last-metadata-change, not creation, so it
            # collapses to ~mtime once a job finishes -- use st_birthtime (true file
            # creation = daemon launch) where the platform has it, ctime as fallback.
            start = getattr(st, "st_birthtime", st.st_ctime)
            elapsed_s = (time.time() - start) if j.get("status") == "running" else (st.st_mtime - start)
        except OSError:
            pass
    d["elapsed_s"] = elapsed_s
    return d


def _web_search_usage():
    """Read dispatch-metrics.jsonl and compute web search usage statistics."""
    # Two producers of web-search counts, both under ollama-worker-logs/:
    #   * dispatch-metrics.jsonl  -- ollama-worker.py dispatch runs
    #   * research-metrics.jsonl  -- studio-research.py deep-research runs
    # The dashboard sums web-search usage across both.
    _LOGDIR = Path.home() / "bin" / "ollama-worker-logs"
    METRICS_PATHS = [_LOGDIR / "dispatch-metrics.jsonl",
                     _LOGDIR / "research-metrics.jsonl"]
    
    # Initialize counters
    total_counts = {"ollama": 0, "tavily": 0, "brave": 0, "searxng": 0}
    today_counts = {"ollama": 0, "tavily": 0, "brave": 0, "searxng": 0}
    last7d_counts = {"ollama": 0, "tavily": 0, "brave": 0, "searxng": 0}
    
    # Get current UTC date
    now = time.time()
    today_date = time.gmtime(now).tm_year, time.gmtime(now).tm_mon, time.gmtime(now).tm_mday
    
    try:
        lines = []
        for mp in METRICS_PATHS:
            if mp.exists():
                lines.extend(mp.read_text().splitlines())

        # Process each metrics line (across all producers)
        if True:
            for line in lines:
                try:
                    data = json.loads(line.strip())
                    
                    # Skip lines without timestamp or malformed entries
                    if "timestamp" not in data:
                        continue
                    
                    # Parse timestamp to a UTC-aware datetime. The worker writes
                    # ISO offset form ("...+00:00"); older rows may use a "Z"
                    # suffix. datetime.fromisoformat handles both (normalise Z).
                    timestamp_str = data["timestamp"]
                    try:
                        dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                    except ValueError:
                        continue  # Skip malformed timestamp
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    dt = dt.astimezone(timezone.utc)

                    line_date = (dt.year, dt.month, dt.day)
                    
                    # Get web_search data or default to empty dict
                    web_search_data = data.get("web_search", {})
                    
                    # Handle missing keys gracefully - treat as all-zero if not present
                    ollama_count = web_search_data.get("ollama", 0)
                    tavily_count = web_search_data.get("tavily", 0)
                    brave_count = web_search_data.get("brave", 0)
                    searxng_count = web_search_data.get("searxng", 0)
                    
                    # Add to totals
                    total_counts["ollama"] += ollama_count
                    total_counts["tavily"] += tavily_count
                    total_counts["brave"] += brave_count
                    total_counts["searxng"] += searxng_count
                    
                    # Check if line is from today
                    if line_date == today_date:
                        today_counts["ollama"] += ollama_count
                        today_counts["tavily"] += tavily_count
                        today_counts["brave"] += brave_count
                        today_counts["searxng"] += searxng_count
                    
                    # Check if line is within last 7 days (inclusive)
                    days_diff = (now - dt.timestamp()) / (24 * 3600)
                    if days_diff <= 7:
                        last7d_counts["ollama"] += ollama_count
                        last7d_counts["tavily"] += tavily_count
                        last7d_counts["brave"] += brave_count
                        last7d_counts["searxng"] += searxng_count
                        
                except (json.JSONDecodeError, KeyError, ValueError):
                    # Skip malformed lines
                    continue
                    
    except OSError:
        # File not accessible or other OS error - return all-zero counts
        pass
    
    return {"total": total_counts, "today": today_counts, "last7d": last7d_counts}


class Handler(http.server.BaseHTTPRequestHandler):
    def _json(self, obj, status=200):
        body = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _text(self, msg, status=400):
        body = msg.encode()
        self.send_response(status)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _html(self, html):
        body = html.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        # No Cache-Control was ever sent before this (confirmed live 2026-08-29) --
        # browsers apply their own heuristic freshness caching on a bare 200 with no
        # explicit directive, so a normal reload could silently keep serving an old,
        # possibly-inconsistent cached copy from before/between a dashboard restart.
        # This page's content is cheap to regenerate and must always be current.
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _read_json_body(self):
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length) if length else b"{}"
        return json.loads(raw or b"{}")

    def _auth_ok(self):
        """True when no token is configured (open) or the request presents it."""
        if not API_TOKEN:
            return True
        header = self.headers.get("Authorization", "")
        if header.startswith("Bearer ") and header[7:] == API_TOKEN:
            return True
        return self.headers.get("X-Api-Token") == API_TOKEN

    def _reject_unauthorized(self):
        self.send_response(401)
        self.send_header("Content-Type", "text/plain")
        self.send_header("WWW-Authenticate", "Bearer")
        self.end_headers()
        self.wfile.write(b"unauthorized")

    def do_GET(self):
        if not self._auth_ok():
            return self._reject_unauthorized()
        if self.path == "/" or self.path == "/index.html":
            self._html(FRONTEND_HTML)
        elif self.path == "/api/jobs":
            with q._Locked() as lock:
                state = lock.load()
            self._json([_job_summary(j) for j in state["jobs"]])
        elif self.path == "/api/hosts":
            self._json(_hosts_summary())
        elif self.path == "/api/servers":
            # Config-driven registry for the settings UI: [{name, type, url, usable_bytes}].
            # type defaults to "ollama" for legacy entries with no type key.
            servers = servers_config.load_servers()
            self._json({"servers": [
                {"name": n, "type": servers_config.entry_type(s),
                 "url": s.get("url", ""), "usable_bytes": s.get("usable_bytes", 0)}
                for n, s in servers.items()],
                "backend_types": list(servers_config.BACKEND_TYPES)})
        elif self.path == "/v1/models" or self.path.startswith("/v1/models?"):
            self._v1_models()
        elif self.path == "/api/web-search-usage":
            self._json(_web_search_usage())
        elif self.path == "/api/handoff":
            # Read the handoff data from handoff-emit.py --json
            import subprocess
            try:
                result = subprocess.run(['python3', os.path.expanduser('~/bin/handoff-emit.py'), '--json'], 
                                      capture_output=True, text=True, check=True)
                self._json(json.loads(result.stdout))
            except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
                self._text(f"error reading handoff data: {e}", 500)
        else:
            m = re.match(r"^/api/jobs/([^/]+)/livelog$", self.path)
            if m:
                return self._livelog(m.group(1))
            self._text("not found", 404)

    _ANSI_RE = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]")

    def _livelog(self, job_id):
        with q._Locked() as lock:
            state = lock.load()
        job = next((j for j in state["jobs"] if j["id"] == job_id), None)
        if job is None:
            return self._text("job not found", 404)
        path = job.get("live_log_path")
        if not path or not os.path.isfile(path):
            return self._text("no live log recorded for this job", 404)
        try:
            text = Path(path).read_text(errors="replace")
        except OSError as e:
            return self._text(f"error reading live log: {e}", 500)
        text = self._ANSI_RE.sub("", text)
        lines = text.splitlines()
        truncated = len(lines) > 500
        if truncated:
            lines = lines[-500:]
        self._json({"content": "\n".join(lines), "truncated": truncated})

    def do_POST(self):
        if not self._auth_ok():
            return self._reject_unauthorized()
        if self.path == "/api/jobs":
            self._enqueue()
        elif self.path == "/api/hosts":
            self._add_or_update_host()
        elif self.path == "/v1/chat/completions" or self.path.startswith("/v1/chat/completions?"):
            self._v1_chat()
        elif self.path == "/api/jobs/move":
            self._move()
        elif self.path.startswith("/api/jobs/") and self.path.endswith("/kill"):
            self._kill(self.path[len("/api/jobs/"):-len("/kill")])
        else:
            m = re.match(r"^/api/jobs/([^/]+)/promote$", self.path)
            if m:
                return self._promote(m.group(1))
            m = re.match(r"^/api/jobs/([^/]+)/resume$", self.path)
            if m:
                return self._resume(m.group(1))
            elif self.path == "/api/handoff/acted":
                return self._mark_handoff_as_acted()
            self._text("not found", 404)

    def do_DELETE(self):
        if not self._auth_ok():
            return self._reject_unauthorized()
        if self.path.startswith("/api/hosts/"):
            self._delete_host(urllib.parse.unquote(self.path[len("/api/hosts/"):]))
        elif self.path.startswith("/api/jobs/"):
            self._cancel(self.path[len("/api/jobs/"):])
        else:
            self._text("not found", 404)

    def _enqueue(self):
        try:
            data = self._read_json_body()
            model = data.get("model", "").strip()
            cwd = data.get("cwd", "").strip()
            task = data.get("task", "").strip()
            if not model or not cwd or not task:
                return self._text("model, cwd, and task are required", 400)
            cwd_path = Path(cwd).expanduser()
            if not cwd_path.is_dir():
                return self._text(f"cwd does not exist: {cwd_path}", 400)
            TASKS_DIR.mkdir(parents=True, exist_ok=True)
            task_id = uuid.uuid4().hex[:12]
            task_file = TASKS_DIR / f"{task_id}.txt"
            task_file.write_text(task)
            with q._Locked() as lock:
                state = lock.load()
                job_id = uuid.uuid4().hex[:12]
                job = {
                    "id": job_id,
                    "label": data.get("label") or cwd_path.name,
                    "model": model,
                    "host_pref": data.get("host") or "auto",
                    "cwd": str(cwd_path.resolve()),
                    "task_file": str(task_file),
                    "task_kind": data.get("task_kind") or None,
                    "manual_tools": bool(data.get("manual_tools")),
                    "api": data.get("api") or "ollama",
                    "verify": data.get("verify") or None,
                    "runner": (str(__import__("pathlib").Path(data["runner"]).expanduser().resolve()) if data.get("runner") else None),
                    "num_ctx": int(data.get("num_ctx") or 65536),
                    "max_iters": int(data.get("max_iters") or 20),
                    "temperature": float(data.get("temperature") or 0),
                    "chat_timeout": int(data["chat_timeout"]) if data.get("chat_timeout") else None,
                    "status": "pending",
                    "enqueued_at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
                    "pid": None, "lane": None, "log_path": None, "exit_code": None,
                }
                state["jobs"].append(job)
                lock.save(state)
            self._json({"id": job_id})
        except Exception as e:
            self._text(f"error: {e}", 500)

    def _move(self):
        try:
            data = self._read_json_body()
            job_id = data.get("id")
            before_id = data.get("before_id")
            if not job_id:
                return self._text("id required", 400)
            with q._Locked() as lock:
                state = lock.load()
                jobs = state["jobs"]
                moving = next((j for j in jobs if j["id"] == job_id), None)
                if moving is None:
                    return self._text("job not found", 404)
                if moving.get("status") not in ("pending", "paused"):
                    return self._text("only pending/paused jobs can be reordered", 400)
                jobs.remove(moving)
                if before_id:
                    idx = next((i for i, j in enumerate(jobs) if j["id"] == before_id), len(jobs))
                    jobs.insert(idx, moving)
                else:
                    jobs.append(moving)
                lock.save(state)
            self._json({"ok": True})
        except Exception as e:
            self._text(f"error: {e}", 500)

    def _cancel(self, job_id):
        # Delegates to ollama-queue.py's own cancel_job() (2026-08-29) instead of
        # duplicating this status-check logic -- same reasoning as _promote/_resume
        # below: a CLI `cancel` subcommand now exists too (github-projects-bf flagged
        # cancelling as reachable ONLY via this HTTP endpoint as a real gap), and
        # keeping one implementation means the two can't drift apart.
        try:
            self._json(q.cancel_job(job_id))
        except q.QueueActionError as e:
            self._text(str(e), 400)
        except Exception as e:
            self._text(f"error: {e}", 500)

    def _kill(self, job_id):
        # SIGTERM the pid and leave state alone -- the daemon's own reap
        # loop (next poll) notices the pid is gone and marks the job
        # failed with the real exit code, exactly like every manual `kill`
        # used all night. No direct state edit here, so there's no way for
        # this and the daemon's reap to race into an inconsistent state.
        with q._Locked() as lock:
            state = lock.load()
            job = next((j for j in state["jobs"] if j["id"] == job_id), None)
        if job is None:
            return self._text("job not found", 404)
        if job.get("status") != "running":
            return self._text("only a running job can be killed", 400)
        pid = job.get("pid")
        if not pid:
            return self._text("job has no pid on record", 400)
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass  # already dead -- the next daemon poll will reap it as such
        except OSError as e:
            return self._text(f"kill failed: {e}", 500)
        self._json({"ok": True})

    def _promote(self, job_id):
        # Delegates to ollama-queue.py's own promote_job() (imported as q at the top of this
        # file) -- same flock, same state file, no reimplementation here. It blocks for at most
        # a few seconds while it waits best-effort for the daemon to reap the paused job;
        # ThreadingServer means only this one request thread is tied up meanwhile.
        # preempt comes from the POST body ({"preempt": true} from the "run now"
        # button); the plain send-to-top button posts no body, so it defaults to
        # a non-preempting front-jump. _read_json_body tolerates an empty body.
        try:
            preempt = bool(self._read_json_body().get("preempt"))
            self._json(q.promote_job(job_id, preempt=preempt))
        except q.QueueActionError as e:
            self._text(str(e), 400)
        except Exception as e:
            self._text(f"error: {e}", 500)

    def _resume(self, job_id):
        try:
            self._json(q.resume_job(job_id))
        except q.QueueActionError as e:
            self._text(str(e), 400)
        except Exception as e:
            self._text(f"error: {e}", 500)

    def _mark_handoff_as_acted(self):
        """Handle marking a handoff job as acted upon."""
        try:
            data = self._read_json_body()
            job_id = data.get("id")
            
            if not job_id:
                return self._text("job id required", 400)
                
            # Validate that the job ID exists in current handoff data
            import subprocess
            try:
                result = subprocess.run(['python3', os.path.expanduser('~/bin/handoff-emit.py'), '--json'], 
                                      capture_output=True, text=True, check=True)
                handoff_data = json.loads(result.stdout)
                
                # Check if the job ID exists in any of the handoff lists,
                # including the eval-arm ones (keys may be absent or not a list)
                valid_job = False
                for key in ('complete', 'pending', 'eval_arms_pending', 'eval_arms_complete'):
                    jobs = handoff_data.get(key)
                    if not isinstance(jobs, list):
                        continue
                    for job in jobs:
                        if isinstance(job, dict) and job.get('id') == job_id:
                            valid_job = True
                            break
                    if valid_job:
                        break

                if not valid_job:
                    return self._text("invalid job id", 400)
                    
            except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
                return self._text(f"error validating job: {e}", 500)
            
            # Run the --acted command to mark this job
            try:
                result = subprocess.run(['python3', os.path.expanduser('~/bin/handoff-emit.py'), '--acted', job_id], 
                                      capture_output=True, text=True, check=True)
                self._json({"ok": True})
            except subprocess.CalledProcessError as e:
                return self._text(f"error marking as acted: {e.stderr}", 500)
                
        except Exception as e:
            self._text(f"error: {e}", 500)

    # ---- Settings: server registry CRUD -------------------------------------

    def _add_or_update_host(self):
        """POST /api/hosts -- add or update one backend. Body:
        {name, url, type?, usable_bytes?}. type defaults to "ollama" (backward
        compatible with the Ollama-only registry); a non-Ollama backend
        (comfyui/img2vid/image) registers its type + url and needs no
        usable_bytes. Persists via servers_config.save_servers and refreshes the
        worker's cached table."""
        try:
            data = self._read_json_body()
        except Exception as e:
            return self._text(f"bad JSON: {e}", 400)
        name = (data.get("name") or "").strip()
        url = (data.get("url") or "").strip()
        btype = (data.get("type") or servers_config.OLLAMA_TYPE).strip()
        if not name:
            return self._text("name is required", 400)
        if "://" not in url:
            return self._text("url must be a full http(s) URL", 400)
        if btype not in servers_config.BACKEND_TYPES:
            return self._text(f"type must be one of {list(servers_config.BACKEND_TYPES)}", 400)
        try:
            usable_bytes = int(data.get("usable_bytes") or 0)
        except (TypeError, ValueError):
            return self._text("usable_bytes must be an integer", 400)
        servers = servers_config.load_servers()
        if btype == servers_config.OLLAMA_TYPE:
            # Keep the exact legacy shape for Ollama hosts (no "type" key) so a
            # downgrade / older reader still parses the file identically.
            servers[name] = {"url": url.rstrip("/"), "usable_bytes": usable_bytes}
        else:
            entry = {"type": btype, "url": url.rstrip("/")}
            if usable_bytes:
                entry["usable_bytes"] = usable_bytes
            servers[name] = entry
        try:
            servers_config.save_servers(servers)
        except OSError as e:
            return self._text(f"could not save servers file: {e}", 500)
        self._refresh_worker_hosts()
        self._json({"ok": True, "name": name, "servers": servers})

    def _delete_host(self, name):
        """DELETE /api/hosts/<name> -- remove one server from the registry."""
        name = (name or "").strip()
        servers = servers_config.load_servers()
        if name not in servers:
            return self._text(f"no such server: {name}", 404)
        del servers[name]
        try:
            servers_config.save_servers(servers)
        except OSError as e:
            return self._text(f"could not save servers file: {e}", 500)
        self._refresh_worker_hosts()
        self._json({"ok": True, "removed": name, "servers": servers})

    @staticmethod
    def _refresh_worker_hosts():
        """Best-effort: if the worker module is already loaded in this process,
        refresh its in-memory host table so the live occupancy panel and any
        in-process routing see the edit without a restart. New dispatch
        subprocesses re-read the file at import time regardless."""
        try:
            w = q.worker()
            if hasattr(w, "reload_known_hosts"):
                w.reload_known_hosts()
        except Exception:
            pass

    # ---- OpenAI-compatible LLM proxy ----------------------------------------

    def _v1_host_from_query(self):
        parsed = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(parsed.query)
        vals = qs.get("host") or qs.get("server")
        return vals[0] if vals else None

    def _v1_models(self):
        """GET /v1/models -- aggregate models across reachable configured hosts,
        deduped by id, in OpenAI list shape. ?host=<name|url> restricts to one."""
        preferred = self._v1_host_from_query()
        # Ollama-only: /v1/models aggregates LLM hosts. Non-Ollama backends
        # (comfyui/img2vid/image) don't serve /v1/models, so exclude them.
        servers = servers_config.ollama_hosts()
        if preferred:
            url = _servers_file_url(preferred)
            urls = [url] if url else []
        else:
            urls = [s["url"].rstrip("/") for s in servers.values() if s.get("url")]
        seen, data = set(), []
        for url in urls:
            try:
                req = urllib.request.Request(f"{url}/v1/models")
                with urllib.request.urlopen(req, timeout=5) as resp:
                    payload = json.loads(resp.read())
            except Exception:
                continue
            for m in payload.get("data", []):
                mid = m.get("id")
                if mid and mid not in seen:
                    seen.add(mid)
                    data.append(m)
        self._json({"object": "list", "data": data})

    def _v1_chat(self):
        """POST /v1/chat/completions -- forward to a chosen/available Ollama host
        (Ollama is natively OpenAI-compatible). Non-streaming buffers and
        returns; streaming passes chunks straight through. ?host=<name|url>
        selects a host, else auto (first reachable configured host)."""
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw or b"{}")
        except ValueError as e:
            return self._text(f"bad JSON: {e}", 400)
        preferred = self._v1_host_from_query()
        host = _pick_v1_host(preferred)
        if not host:
            return self._text("no ollama hosts configured -- add one in Settings", 503)
        streaming = bool(body.get("stream"))
        req = urllib.request.Request(
            f"{host}/v1/chat/completions", data=raw, method="POST",
            headers={"Content-Type": "application/json"})
        # Forward an Authorization header if the caller supplied one.
        auth = self.headers.get("Authorization")
        if auth:
            req.add_header("Authorization", auth)
        try:
            resp = urllib.request.urlopen(req, timeout=600)
        except urllib.error.HTTPError as e:
            detail = e.read()
            self.send_response(e.code)
            self.send_header("Content-Type", e.headers.get("Content-Type", "application/json"))
            self.send_header("Content-Length", str(len(detail)))
            self.end_headers()
            self.wfile.write(detail)
            return
        except Exception as e:
            return self._text(f"upstream error contacting {host}: {e}", 502)
        with resp:
            if streaming:
                self.send_response(200)
                self.send_header("Content-Type", resp.headers.get("Content-Type", "text/event-stream"))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                try:
                    while True:
                        chunk = resp.read(1024)
                        if not chunk:
                            break
                        self.wfile.write(chunk)
                        self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    pass
            else:
                data = resp.read()
                self.send_response(resp.status)
                self.send_header("Content-Type", resp.headers.get("Content-Type", "application/json"))
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

    def log_message(self, fmt, *args):
        sys.stderr.write(f"[queue-api] {self.address_string()} {fmt % args}\n")


class ThreadingServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True


# Was bound to the LAN IP only (not 0.0.0.0), on the theory that this kept "no
# other route in" besides the Cloudflare tunnel connector -- that reasoning was
# wrong (github-projects-bf caught it live 2026-08-29): binding to a specific
# interface address restricts by INTERFACE, not by caller identity. Any device
# on the LAN could already reach <queue-host>:7684 directly, tunnel or not, so
# the single-IP bind provided no actual isolation -- it just also excluded
# 127.0.0.1/localhost, breaking every local CLI/tooling probe from the SAME
# machine (confirmed: bf spent real time believing this API was down based on
# a localhost check that could never have worked, while it was reachable fine
# over the LAN IP the whole time). 0.0.0.0 fixes local access with no actual
# change to the real exposure, since LAN-wide reachability already existed.
BIND_ADDR = "0.0.0.0"

if __name__ == "__main__":
    srv = ThreadingServer((BIND_ADDR, PORT), Handler)
    print(f"[queue-api] listening on {BIND_ADDR}:{PORT}")
    srv.serve_forever()
