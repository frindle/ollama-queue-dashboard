#!/usr/bin/env python3
"""Minimal HTTP API + frontend for ollama-queue.py, meant to sit behind the
existing "owner-only" Cloudflare Access policy (see the tunnel hostname) --
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
import hashlib
import inspect
import http.server
import importlib.util
import json
import os
import re
import resource
import signal
import socketserver
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from datetime import datetime, timezone

# ~/bin holds the sibling helper modules (cpu_lane, ...). This file is a symlink target in the
# dashboard repo, so Python's script-dir sys.path entry is the repo's src/, not ~/bin.
_BIN_DIR = os.path.expanduser("~/bin")
if _BIN_DIR not in sys.path:
    sys.path.append(_BIN_DIR)

QUEUE_PATH = Path.home() / "bin" / "ollama-queue.py"
TASKS_DIR = Path.home() / "bin" / "ollama-queue-logs" / "web-tasks"
PORT = 7684

# Tailed out of each job's log file for the dashboard -- ollama-worker.py logs
# these lines itself (added 2026-08-28: "--- iteration N/M ---" always did,
# the tok/s line is new). Only the LAST match in the tail matters; a job's log
# can have many of these, we only want current progress, not history.
_ITER_RE = re.compile(r"--- iteration (\d+)/(\d+) ---")
_TOKS_RE = re.compile(r"\(([\d.]+) tok/s\)")
_LOG_TAIL_BYTES = 16_384  # comfortably more than one iteration's log output

# Sort TIERS for the queue panel's display order. ONE definition, injected into the
# front-end below (see FRONTEND_HTML's `const STATUS_ORDER = __QUEUE_STATUS_ORDER__`)
# so the tiers are unit-testable here without a browser.
#
# running first (what's happening right now must never be lost below a long list),
# then EVERY "waiting in the queue, not yet run" state in ONE shared tier:
# pending/held/paused/queued/scheduled. That shared tier is the point (2026-09-18,
# the user: the queue view jumped around under him): a job flipping held -> pending as
# the gate barrier lifts has NOT changed its place in the launch order, so it must
# not change its place in the table either. Splitting those states across tiers made
# every barrier lift re-sort the list and yank rows around mid-read. Only a genuine
# reorder (drag / send-to-top / send-to-bottom) moves a row now.
#
# ONE exception to that shared tier (2026-09-18, the user): `held` is SUNK to the
# bottom. A hold is STICKY -- an operator/fit hold (the parked bonsai job) sits
# there for hours or days, and mid-list it clutters the active worklist without
# ever being actionable. `paused`/`queued`/`scheduled` are the TRANSIENT flips the
# shared tier exists to protect, so they stay at tier 1 and do not move.
#
# `planned` (a queued-up-front DAG placeholder -- see ollama-queue.PLANNED_STATUS)
# shares the waiting tier with pending, deliberately (the user: "they are essentially
# queued just waiting on the job before it to finish like other jobs in the queue").
# The tier alone does not order them -- _waiting_region_order slots each planned row
# directly after the row it waits on, so the whole waiting block reads top-to-bottom
# as the real execution sequence.
QUEUE_STATUS_ORDER = {
    "running": 0,
    "pending": 1, "paused": 1, "queued": 1, "scheduled": 1, "planned": 1,
    "failed": 3, "done": 4, "done_unconverged": 5,
    "held": 6,
}
# Unknown status -> the bottom tier, same as the front-end's `?? 5` default.
_QUEUE_STATUS_DEFAULT_TIER = 5

# Statuses that mean a queue row in a bundle did NOT succeed, and that therefore may
# never be rendered in the same faint grey as routine housekeeping counts.
#
# the user, 2026-09-19, on the `bg-escalation` bundle header: "right now the counter makes
# it seem like its already done when its failed". The header showed a prominent green
# `4/5 slices` with `running: s5-audit`, and the only trace of the failure was `1
# failed` inside the small #666 breakdown -- same tone as `3 planned`. That bundle was
# genuinely fine (the failed row was `auto-refine-bg-escalation-s3-letter-r2`, a refine
# attempt on an already-PASS slice), which is exactly the point: from the dashboard
# there was no way to tell "failed but harmless" from "failed and nobody has looked".
#
# `blocked` and `done_unconverged` are in here for the same reason, not as scope creep:
# they are equally non-success, equally actionable, and were equally invisible.
# Injected into the front-end below so there is ONE definition, same as STATUS_ORDER.
PLAN_ALERT_STATUSES = ["failed", "error", "blocked", "done_unconverged"]


def _queue_status_tier(status):
    """PURE. The display sort tier for one job status (mirror of the front-end's
    STATUS_ORDER lookup, which is injected from the same dict)."""
    return QUEUE_STATUS_ORDER.get(str(status or ""), _QUEUE_STATUS_DEFAULT_TIER)


_WAITING_TIER = 1


def _focus_bundle_key(state, now=None):
    """The bundle the DAEMON is actually holding the lanes for, read out of the shared
    queue state file the API already loads.

    FOCUS PIN (the user 2026-09-20): "if something is first and between runs it should stay
    top of the queue. the queue should reflect the actual queue and not have cosmetic
    quirks." The scheduler in ollama-queue.py is right and was never the problem: it
    keeps ONE bundle active across the gap where that bundle has no row in the queue at
    all (its next slice is still being authored/enqueued) -- that is exactly what
    sticky_active()'s grace window and sticky_incomplete ceiling exist for, and the
    daemon persists the answer as state["_focus_wait"] = {"key", "empty_since"} so it
    survives the gap. The DISPLAY knew nothing about any of it: it anchored a bundle to
    the rows that bundle happened to have on screen (see GROUP AFFINITY below), so a
    bundle with ZERO rows for a few seconds had no anchor left, and its next row landed
    wherever plain FIFO/group placement put it -- the bundle vanishing and reappearing
    out of position while the daemon's intent never changed for a moment. The persisted
    focus key is an anchor that does NOT depend on a row existing, so it survives the
    gap that the row-based anchor cannot.

    `_focus_wait["key"]` is the already-resolved active bundle from the daemon's last
    tick. A `promote --take-focus` / send-to-top written since that tick lands in
    state["_focus_override"] and has not been folded in yet, so we run the daemon's own
    resolve_focus_override() over it -- ONE authority for which bundle is active, same
    idiom as _annotate_job_groups reusing q.job_group_key -- and the dashboard reflects
    a take-focus on the very next poll instead of a tick later. Degrades to the sticky
    key (and then to None) rather than throwing: a display pin is never worth a 500."""
    try:
        fw = (state or {}).get("_focus_wait") or {}
        if not isinstance(fw, dict):
            return None         # a corrupt/legacy scalar is "no focus", never a key
        # {"key":..., "empty_since":...} since 2026-09-18; migrate the old {key: ts}.
        sticky = fw.get("key") if "key" in fw else next(iter(fw), None)
    except Exception:
        return None
    try:
        return q.resolve_focus_override(sticky, (state or {}).get("_focus_override"),
                                        now if now is not None else time.time())
    except Exception:
        return sticky


def _hoist_focus_bundle(out, focus_key):
    """PURE. Pin the daemon's focused bundle to the front of the waiting region,
    keeping its rows in the order the region ordering already gave them.

    This is the SAME concept as GROUP AFFINITY below -- keep a bundle's rows together
    and where the scheduler will actually reach them -- with the anchor moved off "a
    row this bundle currently has on screen" and onto the daemon's persisted focus, so
    it survives the bundle having no rows at all (see _focus_bundle_key). It is not a
    cosmetic preference: the daemon launches NOTHING outside the focused bundle while it
    holds, so the focused bundle genuinely is what runs next and the top of the queue is
    where it belongs.

    A REAL dep still outranks the pin, exactly as group affinity is tried only after the
    dep passes stop progressing: if any focus row waits (`after`) on a row that would be
    left behind it, the hoist is skipped wholesale rather than sorting a child above its
    parent. No focus key, or a focus bundle with no rows in the region (the gap itself),
    is a no-op -- the pin costs nothing until there is something to pin."""
    if not focus_key:
        return out
    focus = [j for j in out if j.get("group_key") == focus_key]
    if not focus or len(focus) == len(out):
        return out
    rest = [j for j in out if j.get("group_key") != focus_key]
    rest_ids = {j.get("id") for j in rest if j.get("id")}
    if any(j.get("after") in rest_ids for j in focus if j.get("after")):
        return out              # a genuine cross-bundle dep chain wins over the pin
    return focus + rest


def _waiting_region_order(waiting, known_ids, focus_key=None):
    """PURE. Order the MERGED waiting region (pending/paused/queued/scheduled AND
    the PLANNED DAG rows) as the true EXECUTION sequence.

    the user 2026-09-18: "can we put the planned runs in the queue where they'll fall
    when we run them? ... they are essentially queued just waiting on the job before
    it to finish like other jobs in the queue." So planned rows are NOT a separate
    block: each one is slotted directly after the row it waits on.

    The spine is the runnable rows in their TRUE launch order (that order is the
    daemon's own, and the reorder buttons compute neighbours from it, so it is never
    rearranged). Then each planned row is inserted immediately after its `after`
    dependency:
      * dep is a row in this region  -> straight after it;
      * dep exists but is elsewhere (it is RUNNING, or already done) -> the front of
        the region, because that slice is genuinely next;
      * no dep / an unresolvable dep, but a SIBLING of the same plan is already
        placed -> straight after that plan's last placed row (see below);
      * nothing to go on at all -> the end of the region, after everything that
        can already run.
    A planned child can therefore never sort above its planned parent. Terminating
    by construction: every pass either places a row or force-places the first one
    left.

    GROUP AFFINITY (2026-09-19). A slice's `after` points at the job for the
    previous slice, and that job is REAPED once it finishes -- so mid-plan the dep
    is neither in the region nor in known_ids and the row fell through to the tail
    of the queue, hundreds of rows away from its own bundle's live slice. Measured
    live on /api/jobs: bg-health's live `auto-author-bg-health-s2-classify` sat at
    display_seq 6 while its own s1/s3/s4 sat at 140/141/142; bg-detection 4 vs 139;
    bg-alert 5 vs 138; aw-sched-runner 9 vs 104-128. Because a bundle renders at the
    MIN display_seq of its children (plan_seq), the whole bundle was anchored by ONE
    ephemeral auto-author row: the moment that row was reaped the anchor snapped to
    the stranded planned tail and the bundle teleported ~134 rows down the page,
    then jumped back when the next auto-author was enqueued near the front. That is
    the queue "jumping around" instead of a bundle staying put until it completes.
    Keeping a plan's rows CONTIGUOUS is also just what depth-first scheduling does,
    so the display stops disagreeing with the run order. Group affinity is tried
    only once the dep passes have stopped progressing, so a real dep chain always
    wins, and it needs a group_key -- an ungrouped root planned row still lands at
    the tail exactly as before."""
    out = [j for j in waiting if j.get("status") != "planned"]
    rest = [j for j in waiting if j.get("status") == "planned"]
    region_ids = {j.get("id") for j in waiting}

    plan_rows = {}
    for j in waiting:
        if j.get("group_key"):
            plan_rows[j["group_key"]] = plan_rows.get(j["group_key"], 0) + 1

    def _place_by_group():
        """Insert the first still-unplaced planned row that has a placed sibling,
        directly after that plan's LAST placed row. Returns True if it placed one."""
        for p in list(rest):
            key = p.get("group_key")
            if not key:
                continue
            last = next((i for i in range(len(out) - 1, -1, -1)
                         if out[i].get("group_key") == key), None)
            if last is None:
                continue
            out.insert(last + 1, p)
            rest.remove(p)
            return True
        return False

    def _place_continuation():
        """A multi-slice plan with NOTHING placed yet: its previous slice's job is
        REAPED, which is the same situation as rule 3's "the dep is already done" --
        this slice is genuinely next -- but the dep id has left known_ids entirely,
        so rule 3 cannot see it. Without this, the bundle sinks to the tail for the
        whole handover window between the auto-author being reaped and the coding
        row it enqueued appearing, and then jumps back: the bundle visibly bounces
        down and up the queue once per slice. Front of the region, exactly as rule 3
        does. Guarded on the plan actually HAVING other rows, so a lone orphan
        planned row is never hoisted over runnable work."""
        for p in list(rest):
            key = p.get("group_key")
            if not key or plan_rows.get(key, 0) < 2:
                continue
            out.insert(0, p)
            rest.remove(p)
            return True
        return False

    while rest:
        progressed = False
        for p in list(rest):
            dep = p.get("after")
            if not dep:
                continue
            pos = next((i for i, j in enumerate(out) if j.get("id") == dep), None)
            if pos is None and dep in region_ids:
                continue           # its parent is still waiting to be placed
            if pos is None and dep in known_ids:
                # The dep is running/elsewhere, so this slice is genuinely next --
                # but "next" must mean next WITHIN ITS OWN BUNDLE when that bundle
                # already has rows on screen. Hoisting to the front of the whole
                # region regardless is the other half of the jumping the user sees:
                # measured live, bg-escalation's planned s4/s5 sat at display_seq
                # 1 and 2 -- the very top of the queue -- while the slice actually
                # being worked (auto-refine-...-s3-letter, pending) sat at 141. The
                # bundle rendered at the top, then dropped ~140 rows the moment
                # those placeholders were realised. Anchor to the bundle first;
                # front-of-region stays the answer when the bundle has nothing
                # placed yet (a plan whose first slice waits on the running job).
                pos = next((i for i in range(len(out) - 1, -1, -1)
                            if p.get("group_key")
                            and out[i].get("group_key") == p["group_key"]), -1)
            if pos is None:
                continue
            out.insert(pos + 1, p)
            rest.remove(p)
            progressed = True
        if not progressed and (_place_by_group() or _place_continuation()):
            continue
        if not progressed:
            # A root planned row (no dep, no placed sibling, or a dep nothing knows
            # about): it lands after everything already runnable. Placing it lets its
            # children resolve on the next pass, which is what keeps a chain in order.
            out.append(rest.pop(0))
    return _hoist_focus_bundle(out, focus_key)


import re as _re_slice


def _slice_base_label(label):
    """PURE. Strip the pipeline-stage prefixes/suffixes off a job label to get the
    underlying SLICE identity, so a planned placeholder (published under the bare
    coding label, e.g. `bg-profile-s2`) can be matched to whichever live job is
    currently realising that slice -- the authoring stage `auto-author-bg-profile-s2`,
    a refine round `auto-refine-bg-profile-s2-r2`, or the bare coding job itself. The
    slicer publishes the placeholder under the coding label expecting the CODING
    enqueue to release it, but the FIRST job for a slice is always `auto-author-...`,
    which runs for a long time -- so the placeholder and its live author twin coexist
    for the whole authoring phase and the queue reads doubled. Matching on this base
    lets the display fold the two into one."""
    s = _re_slice.sub(r"^(auto-author-|auto-refine-)", "", label or "")
    s = _re_slice.sub(r"-r\d+$", "", s)
    return s


def _drop_planned_twins(jobs):
    """PURE. Remove a PLANNED placeholder row when a live (non-planned, non-finished)
    job is already realising the same slice -- the placeholder's job now exists, so
    showing both is the doubling the user saw ("not everything is lined up ... one main
    job ... expand to show the interior"). A placeholder with no live twin (a future
    slice not yet authored) is kept: it is the only thing standing in for that work."""
    live_bases = {
        _slice_base_label(j.get("label"))
        for j in jobs
        if j.get("status") not in ("done", "done_unconverged", "planned")
    }
    return [
        j for j in jobs
        if not (j.get("status") == "planned"
                and _slice_base_label(j.get("label")) in live_bases)
    ]


def _queue_display_order(jobs, focus_key=None):
    """PURE. The queue panel's rendered row order for `jobs` (list of dicts with a
    'status'): drop finished rows AND planned twins (a placeholder whose live job
    already exists), then a STABLE sort by status tier -- running on top, then ONE
    merged waiting line in execution order, with sticky `held` at the bottom. Within
    every other tier the true FIFO order is preserved, so a mere status change cannot
    move a row."""
    jobs = _drop_planned_twins(jobs)
    live = [j for j in jobs if j.get("status") not in ("done", "done_unconverged")]
    ordered = sorted(live, key=lambda j: _queue_status_tier(j.get("status")))
    waiting = [j for j in ordered if _queue_status_tier(j.get("status")) == _WAITING_TIER]
    if not any(j.get("status") == "planned" for j in waiting) and not focus_key:
        return ordered          # nothing to slot -- exactly the old behaviour
    known = {j.get("id") for j in jobs}
    slotted = _waiting_region_order(waiting, known, focus_key=focus_key)
    head = [j for j in ordered if _queue_status_tier(j.get("status")) < _WAITING_TIER]
    tail = [j for j in ordered if _queue_status_tier(j.get("status")) > _WAITING_TIER]
    return head + slotted + tail


def _annotate_display_seq(summaries, focus_key=None):
    """Stamp each row with the server-computed display position, so the front-end
    renders the ONE ordering defined (and unit-tested) here instead of re-deriving a
    weaker one from the status tier alone. Display-only: it never changes what the
    daemon launches, nor the true FIFO order the reorder buttons use. It now READS the
    daemon's launch intent, though -- `focus_key` is the bundle the scheduler is holding
    the lanes for (see _focus_bundle_key), so the rendered order can follow the real
    scheduling decision instead of re-deriving a weaker guess that disagrees with it.

    Also DROPS planned twins (a placeholder whose live author/coding job already
    exists) from the returned list -- so `/api/jobs` never surfaces the doubled row
    at all, rather than shoving it to a high seq where it would still render."""
    ordered = _queue_display_order(summaries, focus_key=focus_key)
    order = {j.get("id"): i for i, j in enumerate(ordered)}
    kept = _drop_planned_twins(summaries)
    for j in kept:
        j["display_seq"] = order.get(j.get("id"), 10_000 + _queue_status_tier(j.get("status")))
    return kept


# Terminal statuses the queue panel never renders (the front-end filters them too).
# They are excluded from the plan rollup so a finished slice can neither inflate a
# plan's counts nor drag its parent's sort position around.
_QUEUE_TERMINAL_STATUSES = ("done", "done_unconverged")


def _row_display_seq(row):
    """PURE. The display position of one row, with the same fallback the front-end
    uses when an older payload carries no display_seq."""
    seq = row.get("display_seq")
    return seq if isinstance(seq, int) else 10_000 + _queue_status_tier(row.get("status"))


# The parent row shows its MOST ACTIVE child's status, not a generic "active"
# (2026-09-18, the user: "whatever job is actively being worked should show running" --
# the rolled-up queue read as all-pending while a slice was in fact running). Lower
# rank wins. Anything unknown sorts last, so a new status can never outrank running.
_PLAN_STATUS_PRECEDENCE = ("running", "pending", "queued", "scheduled",
                           "paused", "held", "planned")
_PLAN_STATUS_DEFAULT_RANK = len(_PLAN_STATUS_PRECEDENCE)


def _plan_status_rank(status):
    """PURE. Precedence of one status for the parent row: running > pending/queued/
    scheduled > paused/held > planned > anything else."""
    st = str(status or "")
    return (_PLAN_STATUS_PRECEDENCE.index(st) if st in _PLAN_STATUS_PRECEDENCE
            else _PLAN_STATUS_DEFAULT_RANK)


def _slice_short_name(label, group_key):
    """PURE. The slice's own name, with the pipeline-stage prefix, the refine round
    and the plan prefix stripped: ('auto-author-aw-sched-routes-s2-from-transfer-
    partners', 'aw-sched-routes') -> 's2-from-transfer-partners'. Reuses the SAME
    base-stripping rule as _slice_base_label so the name shown on the parent row is
    the same slice identity everything else groups on. Falls back to the stripped
    label when it does not start with the plan key (never returns empty)."""
    base = _slice_base_label(label)
    if group_key and base.startswith(str(group_key) + "-"):
        return base[len(str(group_key)) + 1:]
    return base


def _group_waiting_by_plan(rows):
    """PURE. Roll the queue's rows up into ONE entry per slice-PLAN, so the panel can
    render a single collapsible parent instead of 161 flat rows (2026-09-18, the user:
    "one main job ... expand to show the interior").

    The plan identity is `row['group_key']` -- stamped by _annotate_job_groups from
    ollama-queue.py's own job_group_key, the ONE authority for "same job", so this can
    never disagree with what `/promote-group` would actually move. Contract:
      * children of a plan are ordered by display_seq (the server-side execution
        order computed by _queue_display_order -- NOT recomputed here);
      * a plan sorts at the MIN display_seq of its children, so a plan whose next
        slice is imminent rises to where that slice would have been;
      * a row with a falsy group_key is STANDALONE (its own entry, rendered flat),
        and so is a plan with exactly one member -- wrapping a lone row in a parent
        would just add a click for nothing;
      * NO row is ever dropped, and ties keep first-appearance order (stable sort);
      * `lead_status` is the MOST ACTIVE child's status (see _plan_status_rank) and
        `lead_slice` names that child, so the parent row says "running" -- and which
        slice is running -- instead of a generic "active".
    Returns a list of
    {key, children, seq, standalone, counts, active, lead_status, lead_slice}."""
    entries = []
    by_key = {}
    for r in rows:
        seq = _row_display_seq(r)
        key = r.get("group_key") or None
        g = by_key.get(key) if key else None
        if g is None:
            g = {"key": key, "children": [], "seq": seq, "standalone": key is None,
                 "counts": {}, "active": False, "lead_status": None, "lead_slice": None}
            entries.append(g)
            if key:
                by_key[key] = g
        g["children"].append(r)
        if seq < g["seq"]:
            g["seq"] = seq
    for g in entries:
        g["children"].sort(key=_row_display_seq)     # stable: ties keep input order
        for r in g["children"]:
            st = str(r.get("status") or "")
            g["counts"][st] = g["counts"].get(st, 0) + 1
        # "active" = something in this plan is running or already runnable; a plan
        # that is entirely `planned` is pure future work.
        g["active"] = any(r.get("status") in ("running", "pending", "paused")
                          for r in g["children"])
        # The LEAD child: most active status wins, ties broken by display_seq (the
        # children are already in that order, and min() keeps the first best) -- so a
        # plan with a running slice reads "running", named, at a glance.
        #
        # SUPERSEDED rows are passed over (2026-09-19, see _superseded_failed_ids): a
        # `failed` attempt whose slice has since converged clean is no longer the
        # slice's last word, so it may not speak for the plan. It is NOT dropped --
        # the row still renders, still counts, still reads `failed` -- it just cannot
        # LEAD. A live row that is not superseded always wins over one that is, at any
        # rank, which is why the pool is filtered rather than the rank adjusted (a
        # superseded `failed` and a real `failed` tie on rank, and display_seq order
        # would otherwise decide which one the bundle shouted).
        pool = [r for r in g["children"] if not r.get("superseded_by_pass")]
        lead = min(pool or g["children"],
                   key=lambda r: _plan_status_rank(r.get("status")))
        g["lead_status"] = (_SUPERSEDED_LEAD_STATUS if lead.get("superseded_by_pass")
                            else lead.get("status"))
        g["lead_slice"] = _slice_short_name(lead.get("label"), g["key"])
        if len(g["children"]) == 1:
            g["standalone"] = True
    entries.sort(key=lambda g: g["seq"])              # stable: ties keep input order
    return entries


# A slice is "through" only once it has actually COMPLETED -- the slicer marks it
# 'done' after the coding job finishes AND its deliverable is committed onto the
# chain. 'enqueued' means the coding job is merely PLACED/queued/running, not
# finished, so it must NOT count toward progress (the user: "why does this show 1/2 if
# neither has run yet?" -- a regated-but-not-run coding slice was inflating X).
# Everything else (pending/blocked/failed/escalated) is also still ahead, not behind.
# 'skipped' (added to the slicer 2026-09-19) counts as THROUGH, not ahead. It is
# the deliberate retirement of a slice proven already satisfied at the chain tip,
# so no honest both-ways proof exists for it and nothing will ever land for it --
# bg-actions-s1-item/s4-email-confirm-return-key, which burned 11 authoring
# dispatches before it was retired. Left out of this tuple it would be counted as
# still-owed FOREVER: the bundle fraction would stick at 5/6 once s5 and s6 land,
# bundle_incomplete would never clear, and the chain could never reach "good to go
# -- nothing owed". A retired slice is finished business; it just finished with no
# diff.
_PLAN_SLICE_DONE_STATUSES = ("done", "skipped")
# A plan label is used as a FILENAME; refuse anything that isn't a plain label so a
# group key can never escape the slice-runs directory.
_PLAN_LABEL_RE = re.compile(r"^[A-Za-z0-9._-]+$")
# ~28 plans are re-read on every 5s dashboard poll; these are tiny json files, but
# there is no reason to stat them 28 times a poll. Same short-TTL idiom as
# ollama-queue.py's GROUP_INDEX_TTL_S -- a plan's progress shows within the TTL.
_PLAN_PROGRESS_TTL_S = 10
_PLAN_PROGRESS_CACHE = {}          # group_key -> (read_at, (x, y) | None)


def _plan_progress(state):
    """PURE. (slices_through, slices_total) for ONE slicer run-state dict
    (~/.ollama-dispatch/slice-runs/<plan-label>.json). The whole plan is counted --
    including slices whose queue rows are long gone -- which is exactly why the
    fraction comes from here and not from the live queue rows.

    A slice counts as through when its status is 'enqueued' or 'done'. The total
    prefers the larger of `order` and `slices`, so a slice that is listed in the
    plan order but has no entry in the map yet still counts toward the denominator
    rather than silently shrinking the plan. Returns (0, 0) for anything
    unrecognisable -- the caller treats that as "no progress known"."""
    if not isinstance(state, dict):
        return (0, 0)
    slices = state.get("slices")
    if isinstance(slices, dict):
        vals = list(slices.values())
    elif isinstance(slices, list):
        vals = slices
    else:
        vals = []
    order = state.get("order")
    total = max(len(vals), len(order) if isinstance(order, list) else 0)
    if not total:
        return (0, 0)
    through = sum(1 for v in vals
                  if isinstance(v, dict)
                  and str(v.get("status") or "") in _PLAN_SLICE_DONE_STATUSES)
    return (through, total)


def _load_plan_state(group_key, runs_dir=None, now=None):
    """The slicer's OWN run-state dict for a plan, or None if it can't be read.

    group_key -> filename: the slice-runs file is named for the plan LABEL, and the
    group key IS that label -- q.job_group_key resolves every slice of a plan to the
    `label` field of ~/.ollama-dispatch/slice-runs/<label>.json (verified live: every
    file's `label` equals its stem). So the mapping is just `<group_key>.json`, with
    no re-derivation that could disagree with the grouping.

    ONE read per plan per TTL, shared by the X/Y fraction and the done-slice
    synthesis. Best-effort by design: a missing/corrupt/odd file returns None and
    every caller degrades -- never an exception into the poll handler."""
    if not group_key or not _PLAN_LABEL_RE.match(str(group_key)):
        return None
    now = time.monotonic() if now is None else now
    hit = _PLAN_PROGRESS_CACHE.get(group_key)
    if hit and now - hit[0] < _PLAN_PROGRESS_TTL_S and runs_dir is None:
        return hit[1]
    try:
        d = Path(runs_dir) if runs_dir else q.SLICE_RUNS_DIR
        got = json.loads((d / f"{group_key}.json").read_text())
        if not isinstance(got, dict):
            got = None
    except Exception:
        got = None
    if runs_dir is None:
        _PLAN_PROGRESS_CACHE[group_key] = (now, got)
    return got


def _plan_progress_recursive(group_key, runs_dir=None, now=None, _seen=None):
    """(through, total) for a plan, EXPANDING any slice that was itself escalated
    into its own finer sub-plan (slice-runs/<group_key>-<sid>.json, same convention
    _load_slice_index already reads for the parent/child rollup) instead of
    counting it as one unit. Without this, a bundle split for being too big to
    dispatch in one piece (bg-eraser -> bg-eraser-s1-invoke + bg-eraser-s2-verify,
    each further split) reports its ORIGINAL top-level count forever -- "0 of 2"
    even once 8 real sub-slices are in flight underneath (the user: "I want to see
    total slices not just the initial slices"). Ids come from `order` only (never
    a slices-dict fallback beyond the no-order case) so a DROPPED slice -- already
    excluded from `order` for exactly this reason, see aw-app-wiring's s8-and --
    is never resurrected into the count. `_seen` guards a self-referential plan."""
    _seen = _seen or set()
    if not group_key or group_key in _seen:
        return (0, 0)
    _seen = _seen | {group_key}
    state = _load_plan_state(group_key, runs_dir, now)
    if not isinstance(state, dict):
        return (0, 0)
    slices = state.get("slices")
    slices = slices if isinstance(slices, dict) else {}
    order = state.get("order")
    ids = order if isinstance(order, list) and order else list(slices.keys())
    if not ids:
        return (0, 0)
    through = total = 0
    for sid in ids:
        sub_key = f"{group_key}-{sid}"
        sub_state = _load_plan_state(sub_key, runs_dir, now)
        if isinstance(sub_state, dict) and (sub_state.get("order") or sub_state.get("slices")):
            sx, sy = _plan_progress_recursive(sub_key, runs_dir, now, _seen)
            through += sx
            total += sy
            continue
        total += 1
        v = slices.get(sid)
        if isinstance(v, dict) and str(v.get("status") or "") in _PLAN_SLICE_DONE_STATUSES:
            through += 1
    return (through, total)


def _load_plan_progress(group_key, runs_dir=None, now=None):
    """(x, y) for a plan from its run state, or None when it can't be read/has no
    slices -- the caller then falls back to a live-row fraction. Recurses into any
    slice that was further split into its own sub-plan (see
    `_plan_progress_recursive`), so X/Y reflects the true total, not just the
    original top-level slice count."""
    x, y = _plan_progress_recursive(group_key, runs_dir, now)
    return (x, y) if y else None


def _move_group_args(data):
    """PURE. Validate/normalise a POST /api/jobs/move-group body into
    (job_id, before_id, error). `id` is required and is any job id in the bundle
    being moved; `before_id` is any job id in the bundle it should sit before, and
    is OPTIONAL -- absent/null/"" means "send this bundle to the end", the same
    convention the per-row move endpoint already uses for before_id. A bundle asked
    to move before ITSELF is a no-op, not an error, so a double-click at the edge of
    the list cannot produce a confusing 400."""
    if not isinstance(data, dict):
        return (None, None, "json object body required")
    job_id = data.get("id") or None
    before_id = data.get("before_id") or None
    if not job_id:
        return (None, None, "id required")
    if before_id == job_id:
        before_id = None
    return (job_id, before_id, None)


_PENDING_GATE_TAG = "pending gate"


def _live_gate_refs(rows):
    """PURE. What the still-LIVE gate/regate rows among `rows` are reviewing: the set
    of refs (`gate-<parent job id>` -> that job id, `<slice>-gate` -> that slice
    label) carried by rows that have not gone terminal. A terminal gate row is
    deliberately excluded -- its review is over, so its slice must show the RESOLVED
    verdict, not 'pending gate'."""
    refs = set()
    for r in rows or []:
        if r.get("status") in _QUEUE_TERMINAL_STATUSES:
            continue
        ref = _gate_parent_ref(r.get("label"))
        if ref:
            refs.add(ref)
    return refs


def _durable_verdict_tag(job_id, log_dir=None):
    """The gate verdict for `job_id` read from its NEVER-PRUNED sidecars
    (<log_dir>/<id>.done.json + <id>.gate.json, joined by q._load_job_result) and
    rendered through q._verdict_tag -- the SAME vocabulary the Completed table uses
    (PASS/FAIL/CONCERNS/SKIPPED/PENDING), so the queue and the results view can never
    disagree about a gate's outcome.

    This is the whole point of reading a sidecar rather than the live queue: a gate
    job's own row is dropped from /api/jobs the moment it goes terminal, but its
    verdict was already written to disk and stays there. Best-effort: any read
    failure returns None and the row simply carries no verdict."""
    if not job_id:
        return None
    try:
        res = q._load_job_result(job_id, log_dir)
    except Exception:
        return None
    if not res:
        return None
    try:
        tag = q._verdict_tag(res)
    except Exception:
        return None
    return tag if tag and tag != "?" else None


def _slice_resolved_ok(s, verdict_of=_durable_verdict_tag, log_dir=None):
    """PURE apart from the injected verdict read. Did this slice (one entry of the
    slicer's `slices` map) reach a SUCCESSFUL terminal state -- i.e. is there anything
    left to decide about it?

    `skipped` qualifies: the slice was deliberately retired as already satisfied, so
    nothing is owed. `done` qualifies only if its gate did not FAIL, because a slice
    flips to `done` the moment its CODING job converges and the deliverable commits,
    while the reviewer pass runs afterwards and can still overturn that (see the GATE
    TAG note on _plan_done_children). A slice that is done-with-a-FAIL, or in any
    non-terminal state, is unresolved. So is a slice we cannot read at all: an alarm
    that fires when the evidence is missing is the safe direction."""
    if not isinstance(s, dict):
        return False
    status = str(s.get("status") or "")
    if status == "skipped":
        return True
    if status not in _PLAN_SLICE_DONE_STATUSES:
        return False
    try:
        tag = verdict_of(s.get("job_id"), log_dir) if verdict_of else None
    except Exception:
        tag = None
    return "FAIL" not in str(tag or "").upper()


def _needs_attention_ids(rows, state_of=None, log_dir=None,
                         verdict_of=_durable_verdict_tag, superseded=(), superseded_map=None):
    """PURE apart from the injected lookups. The ids of non-success rows that a bundle
    header should actually RAISE AN ALARM about -- as opposed to merely report.

    Why (2026-09-19, the user, on the red `1 failed` this same day's badge put on
    bg-escalation): "why are we showing failed if nothing is blocking us that failed",
    then, sharpening it, "is the failure something i need to care about is a better way
    to put it." The badge was asking `status in PLAN_ALERT_STATUSES`, and non-success
    is NOT the same question. `auto-refine-bg-escalation-s3-letter-r2` failed, but
    s3-letter itself was already done+PASS: the refine ladder is a quality pass that
    runs AFTER a slice has landed and deliberately stops rather than retrying forever.
    Nothing was stuck and no decision was owed, so a red alarm was simply wrong -- and
    an alarm that cries wolf is worse than no alarm, because the NEXT one gets ignored.

    So the test is on the underlying SLICE, not the row: a non-success row alarms only
    while its slice has not reached a successful terminal state (_slice_resolved_ok).
    A row whose slice has landed still renders, and still shows up in the parent's
    faint breakdown -- it is demoted from alarm to information, never hidden.

    Slice lookup matches _superseded_failed_ids exactly (`<group_key>-<sid>` against
    the row's _slice_base_label), so the two cannot disagree about which slice a row
    belongs to; rows that function already declared superseded are skipped outright.
    Returns a set of row ids. Nothing is dropped, reordered or restyled by this."""
    out, states = set(), {}
    # a bundle `qctl supersede`d with nothing pending/running is retired: its leftover
    # failed rows raise no alarm (2026-10-08; see _retired_bundle_keys)
    try:
        _sup_map = _bundle_view_lib()[0].load_superseded() if superseded_map is None else superseded_map
        # A row's bundle is its group_key, else its explicit `bundle` tag (a tagged row the
        # grouping did not key -- qctl retire stamps the TAG): either must hit the marker.
        _bk = lambda j: j.get("group_key") or (j.get("bundle") if isinstance(j.get("bundle"), str)
                                                and j.get("bundle").strip() else None)
        _retired = _retired_bundle_keys(
            {_bk(r) for r in rows or [] if _bk(r)}, rows or [], _bk, _sup_map, None)
    except Exception:
        _retired = set()
    for r in rows or []:
        if str(r.get("status") or "") not in PLAN_ALERT_STATUSES:
            continue
        if not r.get("id") or r.get("id") in (superseded or ()):
            continue
        gk = r.get("group_key") or None
        if gk and gk in _retired:
            continue
        _tag = r.get("bundle") if isinstance(r.get("bundle"), str) else None
        if _tag and _tag.strip() in _retired:
            continue
        if gk:   # a human-cancelled plan is terminal: its rows raise no alarm
            try:
                import plan_cancel as _pc
                if _pc.cancelled(gk):
                    continue
            except Exception:
                pass
        base = _slice_base_label(r.get("label"))
        s = None
        if gk and base and state_of is not None:
            if gk not in states:
                try:
                    states[gk] = state_of(gk)
                except Exception:
                    states[gk] = None
            slices = states[gk].get("slices") if isinstance(states[gk], dict) else None
            if isinstance(slices, dict):
                for sid, sl in slices.items():
                    if f"{gk}-{sid}" == base and isinstance(sl, dict):
                        s = sl
                        break
        if not _slice_resolved_ok(s, verdict_of, log_dir):
            out.add(r["id"])
    return out


# The status a SUPERSEDED failed row reports when it is all a bundle has left to
# speak with. `done` on purpose: it goes through the SAME incomplete-clamp as any
# other terminal lead status, so a bundle that still owes slices reads `pending`
# rather than falsely claiming the whole plan finished.
_SUPERSEDED_LEAD_STATUS = "done"
# Only a CLEAN pass supersedes. Deliberately an exact match, not startswith: the
# vocabulary also contains PASS-PENDING-REVIEW (converged but the gate has not
# spoken), which must never silence a failure.
_CLEAN_PASS_TAG = "PASS"


def _superseded_failed_ids(rows, state_of=None, log_dir=None,
                           verdict_of=_durable_verdict_tag):
    """PURE apart from the injected `state_of` / `verdict_of` lookups. The ids of
    `failed` rows that are NO LONGER their slice's last word, because the slice's
    CURRENT job has since converged with a clean durable PASS.

    Why (2026-09-19, confirmed live on bg-captcha and bg-profile). A slice is
    retried: attempt 1 fails, attempt 2 fails, attempt 3 converges clean. The two
    failed rows stay in /api/jobs (`failed` is not a _QUEUE_TERMINAL_STATUS, so they
    are never pruned), the winning attempt IS terminal so it is dropped from the live
    table, and the slicer's run-state still carries a stale `escalated`/`failed`
    status because `--accept-slice` has not been run. _group_waiting_by_plan then
    picks its lead from the only rows left -- the two old failures -- and the whole
    bundle shouts `failed` about work that actually passed. The bundle was rolling up
    every historical attempt instead of the LATEST outcome per slice.

    The slice's CURRENT job, in order of authority:
      1. the slicer's own run-state `slices[<sid>]['job_id']` (the authority: it is
         what --accept-slice would accept), found by matching `<group_key>-<sid>`
         against the row's _slice_base_label;
      2. failing that (the run-state can carry job_id=None for a slice whose earlier
         round failed -- bg-profile's s3-load-profile does), the most recently
         enqueued row whose label is EXACTLY the slice base, i.e. the slice's own
         coding row rather than an `auto-author-`/`auto-refine-` stage around it.

    Its outcome is then read off the never-pruned <id>.done.json + <id>.gate.json
    sidecars through the same _durable_verdict_tag the gate-tag fix uses, so the
    queue, the Completed table and this rollup cannot disagree.

    Deliberately NARROW, so a real unresolved failure is never hidden:
      * only `failed` rows are ever superseded (not blocked/escalated/needs_opus);
      * only by an EXACT `PASS` (not PASS-PENDING-REVIEW, not CONCERNS);
      * never a row that IS the current job -- if the latest attempt is the one that
        failed, it still speaks, which is what keeps bg-brokers (current job verdict
        FAIL) and bg-eraser's s4-profile-full-name (current job verdict FAIL) reading
        `failed`;
      * a slice with no readable state and no coding row at all (cc-waitlist-r2's
        s5) supersedes nothing.
    Returns a set of row ids. No row is dropped or reordered by this -- see
    _group_waiting_by_plan, which only declines to let these rows LEAD."""
    by_base = {}
    for r in rows or []:
        base = _slice_base_label(r.get("label"))
        if not base:
            continue
        by_base.setdefault((r.get("group_key") or None, base), []).append(r)
    states, out = {}, set()
    for (gk, base), group in by_base.items():
        current = None
        if gk and state_of is not None:
            if gk not in states:
                try:
                    states[gk] = state_of(gk)
                except Exception:
                    states[gk] = None
            slices = states[gk].get("slices") if isinstance(states[gk], dict) else None
            if isinstance(slices, dict):
                for sid, s in slices.items():
                    if f"{gk}-{sid}" == base and isinstance(s, dict):
                        current = s.get("job_id") or None
                        break
        if not current:
            coding = [r for r in group if r.get("label") == base and r.get("id")]
            if coding:
                current = max(coding,
                              key=lambda r: str(r.get("enqueued_at") or ""))["id"]
        if not current:
            continue
        try:
            tag = verdict_of(current, log_dir)
        except Exception:
            tag = None
        if tag != _CLEAN_PASS_TAG:
            continue
        for r in group:
            if r.get("status") == "failed" and r.get("id") and r.get("id") != current:
                out.add(r["id"])
    return out


def _plan_done_children(state, group_key, live_rows, log_dir=None,
                        verdict_of=_durable_verdict_tag, runs_dir=None, _seen=None):
    """PURE (apart from the injected `verdict_of` sidecar read, and the sub-plan
    state reads that mirror _plan_progress_recursive). The DISPLAY-ONLY child rows
    for slices this plan has already FINISHED.

    Why they have to be synthesized (2026-09-18, the user: "1/4 but only 3 rows in the
    bundle"): Y counts the WHOLE plan, but a done slice's queue row is pruned, so an
    expanded bundle showed fewer children than its own denominator and read as
    inconsistent. These rows put the finished slices back, so the bundle is
    self-contained: s1 done -> s2 running -> s3 planned, and the visible child count
    matches Y again.

    Strictly display-only: they carry a `synthetic` flag, never enter /api/jobs'
    status counts or the queue tiers, and carry NO display_seq, so they cannot move
    a real row. Returned in plan `order`, which is the order they actually ran in.

    NON-SUCCESS ROWS DO NOT STAND IN (2026-09-19, the user: `bg-escalation-s3-letter` is
    done+PASS and
    "doesn't appear in the bundle's row list AT ALL anymore"). THE BUG: the dupe guard
    was `_slice_base_label(row.label) == base`, and _slice_base_label deliberately
    strips `auto-author-`/`auto-refine-`/`-rN` so a PLANNED placeholder can be folded
    onto whichever job is realising it. Here that same folding is wrong. The coding job
    `bg-escalation-s3-letter` had long since gone terminal and been pruned; the only
    surviving row sharing its base was `auto-refine-bg-escalation-s3-letter-r2`, an
    refine attempt that had FAILED. It matched, the tick was skipped, and the bundle
    rendered the failed refine IN PLACE OF the slice that had passed -- a green PASS
    became a bare failure with nothing left to say it had ever succeeded.

    The guard exists to stop a DUPE, i.e. a live row saying the same thing as the tick.
    A row in PLAN_ALERT_STATUSES says the OPPOSITE, so it can never be that duplicate
    and must not silence it -- the same reasoning as the GATE TAG note below, where a
    separate row's outcome is surfaced rather than allowed to swallow the slice's. A
    running/pending/done author or refine round still stands in exactly as before (it
    genuinely IS that slice, in flight, and two rows for it would double). And the
    slice's own primary row -- the bare coding label, or `slices[sid].job_id` -- always
    stands in whatever its status: that row IS the slice, so it cannot contradict it.

    GATE TAG (2026-09-18, the user: "I don't want them to continue to disappear out of
    the queue"). A slice flips to `done` in the slicer's run-state as soon as its
    CODING job converges and the deliverable commits -- its reviewer/gate pass is a
    SEPARATE queue row (`gate-<job id>`) that runs afterwards and can still change
    the outcome. That gate row is dropped from the payload the instant it goes
    terminal, so the gate's result used to vanish with it and the bundle was left
    with a bare `done` telling you nothing. Each row now carries its slice's CURRENT
    gate state instead:
      row['gate_tag']     -- 'pending gate' while a gate/regate row for this slice is
                             still live, else the resolved verdict, else None
      row['raw_verdict']  -- the resolved verdict only (None while pending), so the
                             front-end colours it with the same verdictClass() the
                             Completed table uses
    The resolved verdict comes off the durable <id>.gate.json sidecar, NOT the live
    queue -- that is what makes it survive the gate row's own pruning.

    RECURSIVE INTO SUB-PLANS (2026-09-19, the user: "all runs still aren't reflecting
    like this as they complete in the bundle"). THE BUG: the X/Y fraction is computed
    by _plan_progress_recursive, which EXPANDS any slice that was itself escalated
    into its own finer sub-plan (slice-runs/<group_key>-<sid>.json) and counts that
    sub-plan's leaves -- while this function only ever looked at the TOP-level
    `slices` map. So a bundle whose work has all happened one level down showed a
    numerator with nothing behind it: bg-actions read 5/7 with ZERO tick rows (its
    4 done + 1 skipped leaves all live in bg-actions-s1-item.json), and bg-state read
    1/12 with none. The two halves of the same bundle were derived by two different
    walks of the same tree. This now walks it the SAME way -- same `order`-only ids,
    same sub-plan detection, same `_seen` cycle guard -- so "what the denominator
    counts" and "what the bundle renders" cannot diverge again. A parent slice that
    HAS a sub-plan contributes its sub-slices and not itself, exactly as it does to Y.

    SKIPPED slices are finished business too (_PLAN_SLICE_DONE_STATUSES): a slice
    deliberately retired as already satisfied at the chain tip counts toward X, so
    leaving it out here reintroduced the very "fewer children than the denominator"
    inconsistency this function exists to kill. It is rendered with its OWN status
    (`skipped`, its own class and mark) and never a gate verdict -- nothing ran for
    it, so it must not be dressed up as a slice that passed."""
    if not isinstance(state, dict):
        return []
    _seen = (_seen or set()) | {group_key}
    slices = state.get("slices")
    slices = slices if isinstance(slices, dict) else {}
    order = state.get("order")
    order = order if isinstance(order, list) and order else list(slices.keys())
    # A live row may stand in for a finished slice only if it does not CONTRADICT it.
    # See the NON-SUCCESS note in the docstring: a failed refine round was folding onto
    # the slice by base label and erasing the very tick that said the slice had passed.
    live_bases = {_slice_base_label(r.get("label")) for r in live_rows or []
                  if str(r.get("status") or "") not in PLAN_ALERT_STATUSES}
    # ...and the slice's OWN primary row always stands in for it, whatever its status:
    # that row IS the slice, so its outcome is the slice's outcome, not a contradiction.
    live_primary = {str(r.get("label") or "") for r in live_rows or []}
    live_ids = {str(r.get("id") or "") for r in live_rows or [] if r.get("id")}
    gate_refs = _live_gate_refs(live_rows)
    out = []
    for sid in order:
        base = f"{group_key}-{sid}"
        # Same expansion rule as _plan_progress_recursive: a slice with its own
        # sub-plan file is represented by that sub-plan's leaves, not by itself.
        if base not in _seen:
            sub = _load_plan_state(base, runs_dir)
            if isinstance(sub, dict) and (sub.get("order") or sub.get("slices")):
                out.extend(_plan_done_children(sub, base, live_rows, log_dir,
                                               verdict_of, runs_dir, _seen))
                continue
        s = slices.get(sid)
        status = str(s.get("status") or "") if isinstance(s, dict) else ""
        if status not in _PLAN_SLICE_DONE_STATUSES:
            continue
        job_id = (s.get("job_id") or None) if isinstance(s, dict) else None
        # The dupe guard, narrowed: a NON-SUCCESS meta row no longer counts as this
        # slice standing in for itself (live_bases is filtered above), while the
        # slice's own primary row -- its bare coding label, or the job id the slicer
        # recorded for it -- always does.
        if (base in live_bases or base in live_primary
                or (job_id and str(job_id) in live_ids)):
            continue
        skipped = status == "skipped"
        # A live gate row names its target either by the coding job's id
        # (`gate-<id>`, the usual spelling) or by the slice label (`<slice>-gate`);
        # accept both so neither spelling silently reads as "already resolved".
        pending = (not skipped) and bool(gate_refs & {r for r in (job_id, base, sid) if r})
        verdict = None if (pending or skipped) else (verdict_of(job_id, log_dir)
                                                     if verdict_of else None)
        out.append({"id": f"slice:{base}", "label": base, "status": status,
                    "synthetic": True, "slice_order": len(out),
                    "title": (s.get("title") or "") if isinstance(s, dict) else "",
                    "job_id": job_id,
                    "gate_tag": _PENDING_GATE_TAG if pending else verdict,
                    "raw_verdict": verdict})
    return out


def _annotate_plan_rollup(rows, runs_dir=None, log_dir=None):
    """Stamp each row with where the PLAN rollup puts it, so the front-end groups on
    the ordering decided (and self-tested) here rather than re-deriving its own:
      row['plan_seq']   -- the sort position of the row's plan (min child display_seq)
      row['plan_size']  -- how many rendered rows the plan has right now
      row['plan_bundle'] -- whether the row renders INSIDE a bundle (False => flat).
                            True as soon as the PLAN has more than one slice, even
                            while only one of them currently has a queue row
      row['plan_done']  -- slices of the WHOLE plan already through (X)
      row['plan_total'] -- slices in the whole plan (Y), 0 when unknown
      row['plan_status'] -- the plan's MOST ACTIVE child status (running > pending >
                            paused/held > planned), what the parent row displays
      row['plan_lead']   -- the slice name behind that status
      row['plan_done_slices'] -- DISPLAY-ONLY rows for slices already finished, so
                            the expanded bundle shows Y children, not Y-minus-the-
                            pruned-ones (see _plan_done_children)
      row['superseded_by_pass'] -- True for a `failed` row whose slice has since
                            converged clean (see _superseded_failed_ids); such a row
                            still renders and still counts, it just cannot be the
                            plan's lead status
      row['plan_incomplete'] -- True while X < Y: the plan still owes work, so its
                            parent must NOT read as completed (the user 2026-09-18:
                            "while we work through the bundle the whole thing is
                            pending and shouldn't be moved to completed until
                            everything is completed")
    A terminal row is a plan of one -- EXCEPT in the window where a plan's every
    remaining row is terminal while later slices have not been enqueued yet: those
    rows are grouped too, so the bundle stays visible and in-progress instead of
    vanishing (or reading done) with slices still owed.
    Only ADDS fields; never drops or reorders the returned list."""
    live = [r for r in rows if r.get("status") not in _QUEUE_TERMINAL_STATUSES]
    # Fallback fraction, used only when the slicer's run-state file is unreadable:
    # the finished share of the plan's LIVE rows. Weaker (it cannot see slices whose
    # rows were reaped) but always available, and never a crash.
    fb_total, fb_done = {}, {}
    for r in rows:
        k = r.get("group_key")
        if not k:
            continue
        fb_total[k] = fb_total.get(k, 0) + 1
        if r.get("status") in _QUEUE_TERMINAL_STATUSES:
            fb_done[k] = fb_done.get(k, 0) + 1
    for r in rows:
        r["plan_seq"] = _row_display_seq(r)
        r["plan_size"] = 1
        r["plan_bundle"] = False
        r["plan_done"] = 0
        r["plan_total"] = 0
        r["plan_status"] = r.get("status")
        r["plan_lead"] = None
        r["plan_done_slices"] = []
        r["plan_incomplete"] = False
    # A `failed` row that its own slice has already moved past may not speak for the
    # bundle (2026-09-19 -- see _superseded_failed_ids). Stamped on EVERY row before
    # grouping so the flag is in the payload the front-end sees, and so the lead pick
    # below can pass these rows over without dropping them.
    _superseded = _superseded_failed_ids(
        rows, state_of=lambda k: _load_plan_state(k, runs_dir), log_dir=log_dir)
    # ...and which non-success rows a bundle header may raise an ALARM about: only
    # those whose slice is genuinely unresolved (see _needs_attention_ids). Stamped on
    # every row so the front-end never has to re-derive the judgement from a status.
    _attention = _needs_attention_ids(
        rows, state_of=lambda k: _load_plan_state(k, runs_dir), log_dir=log_dir,
        superseded=_superseded)
    for r in rows:
        r["superseded_by_pass"] = r.get("id") in _superseded
        r["needs_attention"] = r.get("id") in _attention
    # THE LEAK: when every current row of a plan is terminal but the plan is not
    # finished (the next slice has not been enqueued as a job yet), grouping only the
    # live rows forms no group at all -- those rows keep their per-row 'done' default
    # and the bundle reads completed (or disappears) with slices still owed.
    #
    # FULL RUN HISTORY (2026-09-19, the user: "batch should show the full run history
    # always, except gates -- gates can get dropped after running"). This used to
    # skip any plan that still had a live row (`k in live_keys`), which made the
    # bundle's history asymmetric in the worst possible direction: _queue_display_order
    # drops `done`/`done_unconverged` from `live` but NOT `failed`, so a stale failed
    # attempt stayed on the card with its red badge while the two later attempts that
    # PASSED and superseded it were filtered out and vanished. rt-costco s1 showed
    # exactly that -- `8e721ab56c0e` (failed) visible, `a31ff9233e4c` and
    # `227cb3bf0cae` (both PASS) gone -- so the card said "failed" about a slice whose
    # real last word was a pass.
    #
    # Every REAL attempt now stays: author and refine rounds, pass or fail. Only gate
    # and review sub-artifacts are still dropped once consumed (they are pruned from
    # the payload upstream, see the gate-row note in _plan_done_children). Terminal
    # rows carry no display_seq weight and rank below every live status, so they can
    # neither move a live row nor steal the lead.
    # The X < Y condition is DELIBERATELY kept: it decides which PLANS are rebuilt
    # from terminal rows at all, and dropping it would resurrect every finished plan
    # onto the dashboard forever -- a much larger change than the row-history bug
    # being fixed here. Only `k in live_keys` is gone, which is the asymmetry itself.
    stranded = []
    # RETIRED plans (human-cancelled / `qctl supersede`d, nothing pending or running) are
    # not "stranded": their leftover terminal rows must not rebuild a bundle that reads
    # "pending X/Y slices, 0 jobs" in the Queue panel (replay-endorse, cancelled
    # 2026-10-05, sat there with 21 done rows -- 2026-10-08). See _retired_bundle_keys.
    try:
        import plan_cancel as _pc
        _retired = _retired_bundle_keys(
            {r.get("group_key") for r in rows if r.get("group_key")}, rows,
            lambda j: j.get("group_key"), _bundle_view_lib()[0].load_superseded(),
            lambda k: _pc.cancelled(k, runs_dir=runs_dir))
    except Exception:
        _retired = set()
    for r in rows:
        k = r.get("group_key")
        if not k or r.get("status") not in _QUEUE_TERMINAL_STATUSES:
            continue
        if r in live:                     # already grouped; never double-count
            continue
        if k in _retired:
            continue
        prog = _load_plan_progress(k, runs_dir)
        if prog and prog[0] < prog[1]:
            stranded.append(r)
    for g in _group_waiting_by_plan(live + stranded):
        state = _load_plan_state(g["key"], runs_dir) if g["key"] else None
        prog = _load_plan_progress(g["key"], runs_dir) if g["key"] else None
        # Slices whose queue rows were pruned, put back as display-only children so
        # the bundle holds Y rows and reads as the whole plan.
        dones = (_plan_done_children(state, g["key"], g["children"], log_dir,
                                     runs_dir=runs_dir)
                 if state else [])
        if prog is None and g["key"]:
            prog = (fb_done.get(g["key"], 0),
                    fb_total.get(g["key"], len(g["children"])))
        done, total = prog or (0, 0)
        incomplete = bool(total and done < total)
        # The clamp: a bundle that still owes slices may never read as SUCCESS-
        # complete. `failed`/`blocked`/`escalated` are deliberately NOT clamped --
        # those need eyes and masking them would hide a broken plan.
        status = g["lead_status"]
        if incomplete and status in _QUEUE_TERMINAL_STATUSES:
            status = "pending"
        # plan_size is the VISIBLE child count (live rows + the synthesized done
        # ones), so a plan whose only surviving row is terminal still renders as a
        # bundle with its finished slices under it rather than as a lone flat row.
        size = len(g["children"]) + len(dones)
        # plan_bundle is THE render decision -- "does this row live inside a bundle" --
        # and it is deliberately a property of the PLAN, not of however many rows the
        # plan happens to have on screen this poll (2026-09-19, the user: "auto authors can
        # disappear but the job it queues needs to stay showing"). An `auto-author-<slice>`
        # row is ephemeral: it finishes, is reaped, and the real coding row it enqueued
        # takes its place. While a plan is down to ONE visible row in that handover -- or
        # because its siblings are not enqueued yet (arr-codec-floor: 1 row, 4 slices) --
        # a size-only test would collapse the whole bundle into a lone flat row floating
        # at the bottom of the queue, then re-form it a poll later. The slicer's own slice
        # count (Y) does not flicker, so a multi-slice plan stays a bundle for its entire
        # life and its slice row keeps rendering IN it through every status it passes
        # through. Only a genuine plan-of-one is ever flat.
        bundle = size > 1 or (total or 0) > 1
        for r in g["children"]:
            r["plan_seq"] = g["seq"]
            r["plan_size"] = size
            r["plan_bundle"] = bundle
            r["plan_done"] = done
            r["plan_total"] = total
            r["plan_status"] = status
            r["plan_lead"] = g["lead_slice"]
            r["plan_done_slices"] = dones
            r["plan_incomplete"] = incomplete
    return rows


# BUNDLE ORDER (the user 2026-09-27: bundle rows "keep changing position as runs start
# and finish"). The queue panel placed each bundle at plan_seq = MIN display_seq of
# its rows, and display_seq puts RUNNING on top and gives terminal rows 10_000+tier.
# So the committed bundle jumped to the top while one of its jobs ran, fell below
# every other bundle's pending rows the moment that job finished (its remaining rows
# were all done/planned between slices), and jumped back when the next one started --
# measured live 2026-09-27 21:1x: sidecar-bfmr-login-fetch (the ONLY bundle running
# since 13:19) bouncing above/below bg-eraser-cmd and bg-scan-error-reason.
#
# The bundle's place now comes from the SCHEDULER's view, never from its rows'
# momentary statuses -- nothing below reads `running`, so a run starting or
# finishing cannot move a bundle:
#   0  the committed bundle (state["_bundle_commit"].key; the daemon's focus key
#      when no commitment exists) -- it owns the lanes;
#   1  parked bundles, in the order they will RESUME (bundle_commit_step tries
#      parked bundles oldest-`since` first);
#   2  every other bundle with work left, in the order the scheduler will pick
#      them: the pinned group first, then first appearance in the queue's own list
#      (pending_launch_order's plan_rank, minus its running-plans promotion -- a
#      promotion that exists only while a job runs is exactly the jump);
#   3  bundles whose every row is a sticky operator `held` (the old tier-6 sink);
#   4  DONE bundles (every row terminal and the slice plan not owing work), by
#      completion time, oldest first, so a newly finished bundle only ever
#      appends at the very bottom.
# Standalone rows (no group_key) are ranked the same way as bundles of one.
_BUNDLE_TERMINAL = ("done", "done_unconverged", "failed")


def _row_completed_at(r):
    """PURE. Best-effort completion epoch of a finished row (enqueue + wall time)."""
    try:
        t0 = datetime.fromisoformat(str(r.get("enqueued_at"))).timestamp()
    except Exception:
        return 0.0
    try:
        return t0 + float(r.get("wall_s") or r.get("elapsed_s") or 0)
    except Exception:
        return t0


def _bundle_display_order(rows, state, focus_key=None):
    """PURE. The bundle keys of `rows` (group_key, or the row id for a standalone
    row) in stable display order -- see BUNDLE ORDER above. `rows` must be in the
    queue's own list order (state["jobs"] order), which is what /api/jobs passes."""
    state = state or {}
    commit = (state.get("_bundle_commit") or {})
    commit_key = commit.get("key") if isinstance(commit, dict) else None
    commit_key = commit_key or focus_key
    parked = state.get("_bundle_parked") or {}
    if not isinstance(parked, dict):
        parked = {}
    pinned = state.get("pinned_group")
    members, first_live, first_any = {}, {}, {}
    for i, r in enumerate(rows):
        k = r.get("group_key") or r.get("id")
        members.setdefault(k, []).append(r)
        first_any.setdefault(k, i)
        if r.get("status") not in _BUNDLE_TERMINAL:
            first_live.setdefault(k, i)

    def done(k):
        rs = members[k]
        return (all(r.get("status") in _BUNDLE_TERMINAL for r in rs)
                and not any(r.get("plan_incomplete") for r in rs))

    def key(k):
        if k == commit_key:
            return (0, 0, 0)
        if k in parked:
            try:
                since = float((parked.get(k) or {}).get("since") or 0)
            except Exception:
                since = 0.0
            return (1, since, first_any[k])
        if done(k):
            return (4, max(_row_completed_at(r) for r in members[k]), first_any[k])
        if all(r.get("status") == "held" for r in members[k]
               if r.get("status") not in _BUNDLE_TERMINAL):
            return (3, first_live.get(k, first_any[k]), 0)
        return (2, 0 if (pinned is not None and k == pinned) else 1,
                first_live.get(k, first_any[k]))

    return sorted(members, key=key)


def _annotate_bundle_rank(rows, state, focus_key=None):
    """Stamp row['bundle_rank'] (the row's bundle position from _bundle_display_order)
    on every row. The front-end sorts on it FIRST, then plan_seq/display_seq inside
    the bundle. Only ADDS a field; never drops or reorders the returned list."""
    if focus_key is None:
        focus_key = _focus_bundle_key(state)
    try:
        order = _bundle_display_order(rows, state, focus_key=focus_key)
    except Exception:
        return rows             # a display nicety is never worth a 500
    rank = {k: i for i, k in enumerate(order)}
    for r in rows:
        r["bundle_rank"] = rank.get(r.get("group_key") or r.get("id"), len(rank))
    return rows


def _active_bundle_key(state, now=None):
    """The bundle the daemon is working RIGHT NOW: a live human focus override, else
    the bundle commitment. None when nothing is committed."""
    now = time.time() if now is None else now
    try:
        fo = q.live_focus_override_key(state.get("_focus_override"), now)
    except Exception:
        fo = None
    return fo or (state.get("_bundle_commit") or {}).get("key")


def _after_ids(r):
    a = r.get("after")
    if not a:
        return []
    return [str(x) for x in (a if isinstance(a, (list, tuple)) else [a])]


def _wait_reason_for(r, active, by_id, running, db_status=None):
    """One line saying WHY a waiting row is not running, always naming what it is held
    on (the user 2026-10-01: "held on xxxx so we know why its not running"). Priority:
    hold/gate -> unfinished `after` dependency -> another bundle that is active (and
    what it is doing) -> a busy lane (and which job) -> next in line."""
    if r.get("status") == "held" and (r.get("hold_reason") or r.get("held_on")):
        on = r.get("held_on")
        why = r.get("hold_reason")
        return "held on " + (str(on) if on else "hold") + (f": {why}" if why else "")
    for dep in _after_ids(r):
        d = by_id.get(dep)
        if d is not None and d.get("status") not in ("done", "failed", "cancelled"):
            return f"held on {d.get('label') or dep} ({d.get('status')}): runs after it"
    key = r.get("group_key") or r.get("id")
    # Gate/review rows preempt bundle focus in the daemon (and often run on another
    # lane), so a bundle is never what they wait on -- do not claim it is.
    is_gate = str(r.get("label") or "").startswith(("gate-", "regate-", "secondop-", "esc-review-"))
    if active and key != active and not is_gate:
        doing = [j for j in running if (j.get("group_key") or j.get("id")) == active]
        what = (f"running {doing[0].get('label') or doing[0].get('id')}" if doing
                else "between steps")
        return f"held on bundle {active} ({what})"
    lane = r.get("lane")
    pin = r.get("host_pref")
    if is_gate and not lane and pin:
        # A gate PINNED to a host (the pre-gate pins unraid) does not wait on a running
        # job of another lane; only one on its own host holds it.
        busy = [j for j in running if j.get("id") != r.get("id")
                and pin in (j.get("lane"), j.get("host_pref"))]
        if not busy and active and key != active:
            return (f"queued behind committed bundle {active} (bundles do not interleave); "
                    f"this gate is pinned to {pin}")
        if not busy:
            return "next in line (pinned to " + str(pin) + ")"
    busy = [j for j in running if j.get("id") != r.get("id") and (not lane or j.get("lane") == lane)]
    db = _darkbloom_wait(r, db_status)
    if db and db[0]:
        return db[1]                    # every Darkbloom slot is taken (local+fleet)
    if busy:
        return (f"held on running job {busy[0].get('id')} ({busy[0].get('label')})"
                + (f"; Darkbloom {db[2]}/{db[3]} slots busy" if db else ""))
    return "next in line"


def _darkbloom_wait(r, st=None):
    """(full, reason, busy, cap) for a row on the Darkbloom lane, else None. Uses ONLY
    the queue's parsed `darkbloom status` (q.darkbloom_status, 10s-cached), never an
    HTTP load probe. the user 2026-10-01: "held on Darkbloom: N/4 slots busy"."""
    if q.DARKBLOOM_LANE not in (r.get("lane"), r.get("host_pref")):
        return None
    try:
        st = q.darkbloom_status() if st is None else st
    except Exception:
        return None
    if not st or "unfinished" not in st:
        return None
    cap = q.darkbloom_slot_cap(st, r.get("model"))
    n = int(st.get("unfinished") or 0)
    return (n >= cap, f"held on Darkbloom: {n}/{cap} slots busy (local+fleet)", n, cap)


def _annotate_wait_reason(rows, state, now=None):
    """Stamp row['wait_reason'] on every pending/held/planned row (the user 2026-09-27: a
    waiting row read as hung; 2026-10-01: name what it is held on). Only ADDS a field."""
    active = _active_bundle_key(state, now)
    by_id = {r.get("id"): r for r in rows}
    running = [r for r in rows if r.get("status") == "running"]
    try:
        db_st = q.darkbloom_status()        # 10s-cached `darkbloom status`; {} on failure
    except Exception:
        db_st = {}
    for r in rows:
        if r.get("status") not in ("pending", "held", "planned"):
            continue
        try:
            r["wait_reason"] = _wait_reason_for(r, active, by_id, running, db_st)
        except Exception:
            pass                # a display nicety is never worth a 500
    return rows

spec = importlib.util.spec_from_file_location("ollama_queue_lib", QUEUE_PATH)
q = importlib.util.module_from_spec(spec)
spec.loader.exec_module(q)


def _queue_wait_payload(now=None):
    """The daemon's own wait state (ollama-queue.py wait_view: the ONE reader `status`
    shares) for the banner. Never raises; an older queue module degrades to 'unavailable'."""
    try:
        v = q.wait_view(now=now)
    except Exception as e:      # noqa: BLE001 -- a banner is never worth a 500
        return {"source": "none", "message": f"reason unavailable: {type(e).__name__}",
                "lanes": {}, "jobs": {}, "stuck": []}
    for ln in (v.get("lanes") or {}).values():
        (ln.get("reason") or {}).pop("sig", None)
    v.pop("jobs", None)
    return v


def _annotate_queue_wait(rows, now=None):
    """Stamp row['queue_wait'] = {short, code, source} from the daemon's wait state on
    every pending/held/paused row (new field; only ADDS). No state file -> no field."""
    try:
        v = q.wait_view(now=now)
    except Exception:
        return rows
    if v.get("source") != "state":
        return rows
    for r in rows:
        w = (v.get("jobs") or {}).get(r.get("id"))
        if w and r.get("status") in ("pending", "held", "paused"):
            r["queue_wait"] = {"short": w.get("short"), "code": w.get("code"), "source": "daemon"}
    return rows

# handoff-emit.py is loaded as a LIBRARY (not shelled out to) so the unified
# run-status list reuses its ONE definition of "does this job still need
# sign-off" (signoff_blocks_acting) and "is this a measurement arm, not a
# deliverable" (_label_is_eval). Importing runs no argparse -- main() is guarded
# by __name__ == "__main__" -- so this is a pure def/const load. Same pattern as
# the q import above, needed because the filename has a hyphen.
HANDOFF_PATH = Path.home() / "bin" / "handoff-emit.py"
_ho_spec = importlib.util.spec_from_file_location("handoff_emit_lib", HANDOFF_PATH)
ho = importlib.util.module_from_spec(_ho_spec)
_ho_spec.loader.exec_module(ho)

FRONTEND_HTML = r"""<!doctype html>
<html><head><meta charset="utf-8"><title>Ollama Queue</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  /* Layout (2026-10-05 redesign): one centred column, max 1200px. Top to bottom:
     summary strip (running now / queue / needs attention), Needs attention, Queue
     (active bundles + standalone jobs), Finished bundles (collapsed, by day), then the
     reference panels (Run Status, web search, hosts, settings). Every queue table has
     the same 5 columns -- caret | what | status | time | actions -- and the per-row
     controls live in one overflow menu, so nothing is ever wider than the screen. */
  :root {
    --bg: #f5f6f8; --surface: #ffffff; --surface-2: #eef1f4; --line: #dde2e8;
    --fg: #17202b; --muted: #5b6676; --accent: #0e6a75;
    --run: #18794a; --run-bg: #e2f3e9; --warn: #8f5600; --warn-bg: #fcefd8;
    --bad: #b42318; --bad-bg: #fde7e4; --info: #38598a; --info-bg: #e6edf7;
    --bar: #d6dce4; --tint: rgba(56,89,138,0.07); --shadow: rgba(0,0,0,0.18);
    --sans: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
    --mono: ui-monospace, "SF Mono", Menlo, Consolas, monospace;
    color-scheme: light;
  }
  @media (prefers-color-scheme: dark) { :root:not([data-theme="light"]) {
    --bg: #0f1318; --surface: #161b22; --surface-2: #1c232c; --line: #2a323d;
    --fg: #e5e9ef; --muted: #97a2b0; --accent: #5bbcc8;
    --run: #56c690; --run-bg: #11301f; --warn: #f0b452; --warn-bg: #33260e;
    --bad: #ff8172; --bad-bg: #3b1714; --info: #93b2e8; --info-bg: #1a2639;
    --bar: #2c3540; --tint: rgba(147,178,232,0.07); --shadow: rgba(0,0,0,0.5);
    color-scheme: dark; } }
  :root[data-theme="dark"] {
    --bg: #0f1318; --surface: #161b22; --surface-2: #1c232c; --line: #2a323d;
    --fg: #e5e9ef; --muted: #97a2b0; --accent: #5bbcc8;
    --run: #56c690; --run-bg: #11301f; --warn: #f0b452; --warn-bg: #33260e;
    --bad: #ff8172; --bad-bg: #3b1714; --info: #93b2e8; --info-bg: #1a2639;
    --bar: #2c3540; --tint: rgba(147,178,232,0.07); --shadow: rgba(0,0,0,0.5);
    color-scheme: dark; }
  * { box-sizing: border-box; }
  body { margin: 0; background: var(--bg); color: var(--fg); font: 15px/1.45 var(--sans); }
  .wrap { max-width: 1200px; margin: 0 auto; padding-inline: 16px; padding-block: 20px 56px;
          display: grid; gap: 22px; }
  .wrap > * { min-width: 0; }
  a { color: var(--accent); }
  h1 { font-size: 1.25rem; margin: 0; letter-spacing: -.01em; }
  h2 { font-size: .98rem; margin: 0; }
  code { font-family: var(--mono); font-size: .85em; }
  header.top { display: flex; flex-wrap: wrap; align-items: center; gap: 8px 16px; }
  header.top .spacer { flex: 1; }
  .upd { color: var(--muted); font-size: .8rem; font-variant-numeric: tabular-nums; }
  button { cursor: pointer; font: inherit; color: inherit; }
  .btn, .wrap > section button:not(.mi), #hostSettings button {
    font-size: .84rem; padding: 4px 10px; border-radius: 6px; border: 1px solid var(--line);
    background: var(--surface); color: var(--fg); white-space: nowrap; }
  .btn:hover { background: var(--surface-2); }
  .btn.primary, .wrap > section button.btn.primary { border-color: var(--accent); color: var(--accent); font-weight: 600; }
  button:focus-visible, summary:focus-visible, a:focus-visible, input:focus-visible {
    outline: 2px solid var(--accent); outline-offset: 2px; }
  input, select, textarea { font: inherit; padding: .35rem; background: var(--surface);
    color: var(--fg); border: 1px solid var(--line); border-radius: 5px; }
  form { display: grid; gap: .5rem; max-width: 600px; }
  #err { color: var(--bad); white-space: pre-wrap; }

  /* ---- summary strip ---- */
  .strip { display: grid; grid-template-columns: minmax(0, 2.3fr) minmax(0, 1fr) minmax(0, 1fr); gap: 12px; }
  .tile { background: var(--surface); border: 1px solid var(--line); border-radius: 10px;
          padding: 12px 14px; min-width: 0; }
  .tile .k { font-size: .7rem; text-transform: uppercase; letter-spacing: .07em; color: var(--muted); font-weight: 650; }
  .tile .v { font-size: 1.6rem; font-weight: 650; font-variant-numeric: tabular-nums; line-height: 1.2; }
  .tile .s { color: var(--muted); font-size: .82rem; }
  .tile.attn.has { border-color: var(--bad); }
  .tile.attn.has .v { color: var(--bad); }
  .now-job { font-weight: 600; overflow-wrap: anywhere; margin-top: 3px; }
  .now-meta { display: flex; flex-wrap: wrap; gap: 3px 14px; color: var(--muted); font-size: .83rem;
              font-variant-numeric: tabular-nums; margin-top: 4px; overflow-wrap: anywhere; }
  .now-meta b { color: var(--fg); font-weight: 600; }
  .lane-now { margin-top: 8px; display: grid; gap: 4px; }
  .lane-card { display: flex; flex-wrap: wrap; gap: 2px 10px; align-items: baseline; padding: 5px 8px; border: 1px solid var(--line);
               border-radius: 6px; font-size: .83rem; min-width: 0; overflow-wrap: anywhere; }
  .lane-card.idle { color: var(--muted); }
  .lane-card.run { border-left: 3px solid var(--run); }
  .lane-name { font-weight: 650; text-transform: uppercase; font-size: .72rem; letter-spacing: .05em; color: var(--muted); }
  .lane-meta, .lane-line { color: var(--muted); font-variant-numeric: tabular-nums; }
  .now-more { margin-top: 6px; padding-top: 6px; border-top: 1px dashed var(--line); font-size: .82rem; color: var(--muted); }
  .pulse { display: inline-block; width: 8px; height: 8px; border-radius: 50%; background: var(--run);
           margin-right: 6px; vertical-align: 1px; animation: slicepulse 1.6s ease-in-out infinite; }
  .idle-dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; background: var(--bar); margin-right: 6px; vertical-align: 1px; }

  /* ---- sections + the shared 5-column queue table ---- */
  section.panel { display: grid; gap: 8px; min-width: 0; }
  .panel-head { display: flex; flex-wrap: wrap; align-items: baseline; gap: 6px 10px; }
  .panel-head .count { color: var(--muted); font-size: .85rem; }
  .panel-head .tools { margin-left: auto; display: flex; flex-wrap: wrap; gap: 6px; align-items: center; }
  .list { background: var(--surface); border: 1px solid var(--line); border-radius: 10px; min-width: 0; }
  .list.attn { border-color: var(--bad); }
  .empty { color: var(--muted); font-size: .86rem; padding: 12px 14px; }
  table.q { width: 100%; border-collapse: collapse; table-layout: fixed; }
  table.q col.c0 { width: 34px; } table.q col.c2 { width: 120px; }
  table.q col.c3 { width: 84px; } table.q col.c4 { width: 176px; }
  table.q td { padding: 8px 10px; border-top: 1px solid var(--line); vertical-align: top;
               font-size: .86rem; overflow-wrap: anywhere; text-align: left; }
  table.q tbody tr:first-child td { border-top: 0; }
  table.q td:first-child { color: var(--muted); text-align: center; padding-inline: 4px; white-space: nowrap; }
  table.q td.when { font-variant-numeric: tabular-nums; color: var(--muted); font-size: .8rem; }
  table.q td.acts { text-align: right; white-space: nowrap; overflow: visible; }
  .sub { color: var(--muted); font-size: .79rem; margin-top: 2px; display: flex; flex-wrap: wrap;
         gap: 1px 10px; align-items: center; }
  .reason { font-size: .84rem; margin-top: 3px; }
  .reason.bad { color: var(--bad); }
  .proj-name { font-weight: 650; }
  tr[draggable="true"] { cursor: grab; }
  tr.dragging { opacity: .4; }
  .drop-target-hover { background: var(--run-bg) !important; outline: 2px dashed var(--run); }
  tr.drop-indicator-above { box-shadow: inset 0 2px 0 0 var(--run); }
  tr.drop-indicator-below { box-shadow: inset 0 -2px 0 0 var(--run); }

  /* status chips: colour by meaning; only a real failure is red */
  .chip { display: inline-block; font-size: .73rem; font-weight: 650; padding: 1px 8px; border-radius: 999px;
          white-space: nowrap; background: var(--surface-2); color: var(--muted); line-height: 1.5; }
  .chip.running, .chip.ph-coding, .chip.ph-gate, .chip.ph-regate, .chip.ph-authoring, .chip.ph-refining,
  .chip.ph-preflight, .chip.ph-self-heal, .chip.ph-escalation-review, .chip.ph-second-opinion { background: var(--run-bg); color: var(--run); }
  .chip.warming { background: var(--warn-bg); color: var(--warn); }
  .chip.pending, .chip.planned, .chip.queued, .chip.ph-queued, .chip.ph-pending { background: var(--info-bg); color: var(--info); }
  .chip.held, .chip.paused, .chip.blocked, .chip.parked, .chip.warn, .chip.done_unconverged { background: var(--warn-bg); color: var(--warn); }
  .chip.failed, .chip.bad, .chip.ph-failed, .chip.ph-escalated { background: var(--bad-bg); color: var(--bad); }
  .chip.done, .chip.ph-done, .chip.skipped { background: var(--surface-2); color: var(--muted); }
  .m-chip { display: none; margin-left: 6px; vertical-align: 1px; }
  /* legacy status/verdict text colours (Run Status table, slice history lines) */
  .status-running { color: var(--run); font-weight: 600; } .status-warming { color: var(--warn); font-weight: 600; }
  .status-pending { color: var(--info); } .status-done { color: var(--muted); }
  .status-failed { color: var(--bad); } .status-paused { color: var(--warn); }
  .status-done_unconverged { color: var(--warn); font-weight: 600; }
  .status-skipped { color: var(--muted); font-style: italic; }
  .verdict-pass { color: var(--run); font-weight: 600; }
  .verdict-fail, .verdict-blocked { color: var(--bad); font-weight: 600; }
  .verdict-concerns { color: var(--warn); font-weight: 600; }
  .verdict-skipped, .verdict-pending, .verdict-unknown { color: var(--muted); }
  /* An unresolved failure INSIDE a bundle that is still moving: amber note, not an alarm.
     A bundle that is stuck goes to Needs attention instead, which is where red lives. */
  .plan-alert { font-weight: 650; font-size: .76rem; white-space: nowrap; color: var(--warn); }
  .needs-attn .plan-alert { color: var(--bad); }
  .plan-lead { color: var(--run); font-weight: 600; }
  .ph { font-weight: 600; }
  .slice-dot { color: var(--run); animation: slicepulse 1.4s ease-in-out infinite; }
  @keyframes slicepulse { 0%, 100% { opacity: 1; } 50% { opacity: .35; } }
  @media (prefers-reduced-motion: reduce) { .slice-dot, .pulse { animation: none; } }
  .waitbanner { border: 1px solid var(--warn); background: var(--warn-bg); color: var(--warn);
    border-radius: 10px; padding: .55rem .8rem; margin: 12px 0; font-size: .85rem; }
  .waitbanner.stuck { border-color: var(--bad); background: var(--bad-bg); color: var(--bad); font-weight: 650; }
  .waitbanner .lane { display: block; margin: 2px 0; }
  .waitbanner .badge { display: inline-block; font-size: .68rem; font-weight: 700; letter-spacing: .06em;
    text-transform: uppercase; padding: 1px 6px; border-radius: 6px; border: 1px solid currentColor; margin-right: 6px; }
  .waitbanner .log { opacity: .85; font-weight: 400; font-size: .78rem; }
  .wait-reason { color: var(--muted); font-size: .79rem; display: block; margin-top: 2px; }
  span.qpos { font-size: .72rem; color: var(--muted); margin-right: 6px; font-variant-numeric: tabular-nums;
              white-space: nowrap; cursor: help; }
  span.qpos.qpos-adrift { color: var(--warn); font-weight: 650; }
  .rerun { color: var(--muted); font-size: .85em; font-weight: 500; }

  /* progress bar: done/total slices */
  .prog { display: inline-flex; align-items: center; gap: 7px; font-variant-numeric: tabular-nums; white-space: nowrap; }
  .bar { width: 110px; height: 6px; border-radius: 3px; background: var(--bar); overflow: hidden; display: inline-block; }
  .bar > i { display: block; height: 100%; background: var(--accent); }
  .plan-frac { font-weight: 650; color: var(--fg); }

  /* overflow menu (one per row; every reorder/pause/hold/cancel control lives here) */
  details.menu { position: relative; display: inline-block; text-align: left; }
  details.menu > summary { list-style: none; cursor: pointer; padding: 2px 9px; border-radius: 6px;
    border: 1px solid var(--line); color: var(--muted); font-weight: 700; letter-spacing: .1em; user-select: none;
    white-space: nowrap; display: inline-block; line-height: 1.4; }
  details.menu > summary::-webkit-details-marker { display: none; }
  details.menu[open] > summary { background: var(--surface-2); color: var(--fg); }
  .menu-pop { position: absolute; right: 0; top: calc(100% + 4px); z-index: 50; width: max-content;
    min-width: 210px; max-width: min(320px, calc(100vw - 32px)); background: var(--surface); border: 1px solid var(--line);
    border-radius: 8px; box-shadow: 0 8px 24px var(--shadow); padding: 4px; display: grid; white-space: normal; }
  .menu-pop .mi { font-size: .86rem; text-align: left; background: none; border: 0; color: var(--fg);
    padding: 7px 10px; border-radius: 5px; }
  .menu-pop .mi:hover { background: var(--surface-2); }
  .menu-pop .mi.danger { color: var(--bad); }
  .menu-pop hr { border: 0; border-top: 1px solid var(--line); margin: 3px 2px; width: auto; }

  /* rows inside an expanded bundle */
  tr.queue-parent { cursor: pointer; }
  tr.queue-parent:hover > td { background: var(--tint); }
  tr.queue-child > td { background: var(--surface-2); border-top-color: transparent; font-size: .82rem; padding-block: 5px; }
  tr.queue-child td:nth-child(2) { padding-left: 1.8rem; }
  tr.slice-line td:nth-child(2) { padding-left: 1.8rem; }
  tr.slice-hist td:nth-child(2) { padding-left: 3.2rem; }
  tr.queue-child.slice-live td:nth-child(2) { padding-left: 3.2rem; }
  tr.queue-done { opacity: .62; }
  tr.live-activity td { font-size: .85em; background: var(--run-bg); }
  .stage-h { font-size: .74rem; letter-spacing: .05em; text-transform: uppercase; color: var(--muted); font-weight: 650; }

  /* finished bundles: collapsed by default, grouped by day */
  details.fin > summary { cursor: pointer; list-style: none; display: flex; flex-wrap: wrap; gap: 4px 12px;
    align-items: baseline; padding: 11px 14px; }
  details.fin > summary::-webkit-details-marker { display: none; }
  details.fin > summary::before { content: "\25B8"; color: var(--muted); }
  details.fin[open] > summary::before { content: "\25BE"; }
  details.fin[open] > summary { border-bottom: 1px solid var(--line); }
  .fin-tools { display: flex; flex-wrap: wrap; gap: 6px; padding: 8px 14px; border-bottom: 1px solid var(--line); }
  tr.day-head td { background: var(--surface-2); font-size: .7rem; text-transform: uppercase; letter-spacing: .07em;
    color: var(--muted); font-weight: 650; padding-block: 5px; text-align: left !important; }
  .fail-list { color: var(--bad); font-size: .82rem; }

  /* reference panels */
  .ref-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 16px; }
  .ref-grid > div { background: var(--surface); border: 1px solid var(--line); border-radius: 10px; padding: 12px 14px; min-width: 0; }
  .ref-grid h2 { margin-bottom: 8px; }
  .tablewrap { overflow-x: auto; -webkit-overflow-scrolling: touch; }
  .wrap table:not(.q) { width: 100%; border-collapse: collapse; }
  .wrap table:not(.q) th, .wrap table:not(.q) td { text-align: left; padding: .35rem .5rem;
    border-bottom: 1px solid var(--line); font-size: .82rem; }
  .wrap table:not(.q) th { color: var(--muted); font-weight: 600; font-size: .74rem; text-transform: uppercase; letter-spacing: .05em; }
  #runStatus td:last-child { white-space: nowrap; }
  .iconbtn { font-size: .78rem !important; padding: 1px 7px !important; margin: 0 1px; }
  details.legend { font-size: .8rem; color: var(--muted); }
  details.legend > summary { cursor: pointer; }
  details.legend .legend-body { line-height: 1.6; padding: 6px 0 0 14px; }
  /* Run Status parent/child rows (same visual language as before, on theme tokens) */
  tr.run-parent { cursor: pointer; background: var(--tint); }
  tr.run-parent:hover, tr.run-project:hover { background: var(--surface-2); }
  tr.run-parent .proj-summary, tr.run-project .proj-summary, tr.run-subplan .proj-summary { color: var(--muted); font-size: .8rem; }
  tr.run-subplan { cursor: default; }
  tr.run-subplan td { font-size: .8rem; }
  tr.run-project { cursor: pointer; background: var(--surface-2); }
  tr.run-project .proj-name { font-weight: 700; }
  .eyes-badge { font-weight: 600; }
  .eyes-badge.eyes-ok { color: var(--run); } .eyes-badge.eyes-warn { color: var(--warn); }
  .eyes-badge.eyes-bad { color: var(--bad); }
  .blocked-why { color: var(--muted); font-size: .78rem; }
  tr.run-child td:nth-child(2) { padding-left: 1.8rem; border-left: 3px solid var(--line); }
  #livelogModal { background: var(--surface) !important; color: var(--fg) !important; border-color: var(--line) !important;
    color-scheme: inherit !important; border-radius: 10px; overflow: hidden; }
  #livelogModal > div { background: var(--surface) !important; border-color: var(--line) !important; }
  #livelogContent { font-family: var(--mono); background: var(--bg); color: var(--fg); }

  /* ---- phone ---- */
  @media (max-width: 760px) {
    body { font-size: 14px; }
    .strip { grid-template-columns: 1fr 1fr; }
    .strip .tile.now { grid-column: 1 / -1; }
    table.q col.c0 { width: 26px; } table.q col.c2, table.q col.c3 { width: 0; }
    table.q col.c4 { width: 86px; }
    table.q td:nth-child(3), table.q td:nth-child(4) { display: none; }
    table.q td.acts { white-space: normal; line-height: 2; }
    table.q td.acts .btn { padding: 2px 7px; font-size: .76rem; }
    span.qpos { margin-right: 3px; }
    table.q td { padding: 8px 6px; }
    .m-chip { display: inline-block; }
    .bar { width: 72px; }
    tr.queue-child td:nth-child(2), tr.slice-line td:nth-child(2) { padding-left: .8rem; }
    #runStatus, #runStatus tbody { display: block; width: 100%; }
    #runStatus thead { display: none; }
    #runStatus tr { display: flex; flex-wrap: wrap; align-items: baseline; gap: .1rem .45rem;
      padding: .45rem .1rem; border-bottom: 1px solid var(--line); }
    #runStatus td { display: block; border: none; padding: 0; font-size: .78rem; min-width: 0;
      overflow-wrap: anywhere; word-break: break-word; }
    #runStatus td:nth-child(2) { flex: 1 1 calc(100% - 5rem); font-weight: 600; }
    #runStatus td:last-child { flex: 1 1 100%; white-space: normal; margin-top: .15rem; }
    #runStatus td:empty { display: none; }
    #runStatus td:nth-child(n+4):nth-child(-n+6) { display: none; }
    td button { white-space: nowrap; word-break: keep-all; overflow-wrap: normal; }
    tr.run-child { border-left: 3px solid var(--line); padding-left: .6rem; }
    tr.run-child td:nth-child(2) { padding-left: 0; border-left: 0; }
  }
</style></head>
<body>
<div class="wrap">
<header class="top">
  <h1>Ollama Queue</h1>
  <a href="/chat">Chat &rarr;</a>
  <span class="spacer"></span>
  <span class="upd" id="updated">loading&hellip;</span>
  <button class="btn" id="clearFinished" title="Remove every done/failed row from the queue state">Clear finished</button>
</header>

<div class="strip" id="summary">
  <div class="tile now" id="sumNow"><div class="k"><span class="idle-dot"></span>Running now</div><div class="s">loading&hellip;</div></div>
  <div class="tile" id="sumQueue"><div class="k">Queue</div><div class="v">&ndash;</div></div>
  <div class="tile attn" id="sumAttn"><div class="k">Needs attention</div><div class="v">&ndash;</div></div>
</div>

<div id="waitBanner" class="waitbanner" hidden></div>

<section class="panel" id="attnPanel">
  <div class="panel-head"><h2>Needs attention</h2><span class="count" id="attnCount"></span></div>
  <div class="list attn" id="attnList"><table class="q" id="attnTable"><colgroup><col class="c0"><col><col class="c2"><col class="c3"><col class="c4"></colgroup><tbody></tbody></table></div>
  <div class="list" id="attnEmpty" hidden><div class="empty">Nothing is stuck. Failed, parked and blocked work shows up here.</div></div>
</section>

<section class="panel" id="activePanel">
  <div class="panel-head"><h2>Queue</h2><span class="count" id="activeCount"></span>
    <span class="tools"><details class="legend"><summary>Exit codes</summary><div class="legend-body exit-legend">
      0 - Converged<br>1 - Verify Failed<br>2 - Iteration Cap<br>3 - Paused, Resumable<br>4 - Refused to Start<br>5 - Done, Unconverged</div></details></span></div>
  <div class="list"><table class="q" id="jobs"><colgroup><col class="c0"><col><col class="c2"><col class="c3"><col class="c4"></colgroup><tbody></tbody></table>
  <div class="empty" id="activeEmpty" hidden>The queue is empty.</div></div>
</section>

<section class="panel" id="stalledPanel" hidden>
  <div class="list"><details class="fin" id="stalledDetails" open>
    <summary id="stalledSummary"><b>Stalled / failed bundles</b></summary>
    <div class="empty" style="padding:.4rem .8rem;color:var(--muted)">Nothing is running for these and they did NOT finish: a slice failed or ended, or slices are still owed. Never aged out; they stay until fixed or cleared.</div>
    <table class="q" id="stalledTable"><colgroup><col class="c0"><col><col class="c2"><col class="c3"><col class="c4"></colgroup><tbody></tbody></table>
  </details></div>
</section>

<section class="panel" id="finishedPanel">
  <div class="list"><details class="fin" id="finishedDetails">
    <summary id="finishedSummary"><b>Finished bundles</b></summary>
    <div class="fin-tools" id="finishedTools"></div>
    <table class="q" id="finishedTable"><colgroup><col class="c0"><col><col class="c2"><col class="c3"><col class="c4"></colgroup><tbody></tbody></table>
  </details></div>
</section>

<!-- UNIFIED run-status list (2026-09-17, the user: "complete jobs and handoff should
     essentially be the same one list when qwen is done that shows the run status
     that we can clear once it's handled"). ONE surface replacing the old split
     between the never-pruned "Completed Jobs" durable-verdict table and the
     handoff "Complete -- awaiting action" panel. A dispatch lands here the moment
     qwen finishes (durable <id>.done.json/.gate.json sidecars), showing its final
     gate verdict + run status. "clear" ARCHIVES the row (moves its sidecars to
     LOG_DIR/archive/, recoverable) so it drops off for good once handled; a job
     still AWAITING SIGN-OFF refuses a plain clear and asks for an override reason.
     Backed by /api/runs; /api/jobs/completed and /api/handoff still exist for other
     consumers but the dashboard no longer renders their two separate tables. -->
<section class="panel" id="runPanel">
<div class="panel-head"><h2>Run Status</h2><span class="count">qwen finished: verdict + run status; clear once handled</span></div>
<div class="list tablewrap">
<table id="runStatus"><thead><tr>
  <th>verdict</th><th>label</th><th>model</th><th>host</th><th>files</th><th>when</th><th>flags</th><th></th>
</tr></thead><tbody></tbody></table>
</div>
</section>

<section class="panel" id="cpuLanePanel">
<div class="panel-head"><h2>CPU lane</h2><span class="count" id="cpuLaneSummary">Unraid CPU runner (read-only)</span></div>
<div class="list tablewrap">
<table id="cpuLaneTable"><thead><tr>
  <th>state</th><th>stage</th><th>label</th><th>bundle</th><th>runner</th><th>time</th>
</tr></thead><tbody></tbody></table>
</div>
</section>

<div id="livelogBackdrop" style="display:none; position:fixed; inset:0; background:rgba(0,0,0,0.4); z-index:999;"></div>
<div id="livelogModal" style="display:none; flex-direction:column; position:fixed; top:5%; left:5%; right:5%; bottom:5%; background:#fff; color:#111827; color-scheme:light; border:2px solid #333; z-index:1000; box-shadow:0 4px 20px rgba(0,0,0,.3);">
  <div style="display:flex; justify-content:space-between; align-items:center; padding:.75rem 1rem; border-bottom:1px solid #ccc; flex-shrink:0; background:#fff;">
    <h3 id="livelogTitle" style="margin:0;"></h3>
    <button id="closeLivelog">close</button>
  </div>
  <pre id="livelogContent" style="flex:1; overflow:auto; margin:0; padding:1rem; white-space:pre-wrap; font-size:.8rem;"></pre>
</div>

<!-- The handoff "Complete -- awaiting action" / "Pending" panel that used to sit
     here was the SECOND of the two overlapping surfaces; it is gone. In-flight work
     is the live queue table at the top of the page, and finished work (with its
     clear action) is the unified Run Status table above. -->
<div class="ref-grid">
  <div>   <!-- Web Search Usage -->
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
  <div>   <!-- Loaded right now -->
    <!-- Moved INSIDE this flex row 2026-09-26 (was stacked full-width below it):
         the two panels are both narrow, and side-by-side keeps the queue table
         above the fold. Rendered compactly (small font, tight padding, models
         wrapped under their host) to fit a ~300-400px column. -->
    <h2>Loaded right now</h2>
    <div id="hosts" class="tablewrap"></div>
  </div>
  <div>   <!-- Settings: Ollama hosts -->
    <h2>Settings &mdash; Ollama hosts</h2>
    <div style="font-size:.78rem; opacity:.65; margin-bottom:.4rem;">
      Persisted to <code id="hostsCfgPath"></code>. Live: no restart needed.
      Blank budget = <em>unmeasured</em> &mdash; the fit router will not clear
      that host, so a model is only ever sent there explicitly.
    </div>
    <table id="hostSettings" style="width:100%; border-collapse:collapse; font-size:.8rem;">
      <thead>
        <tr><th style="text-align:left">Name</th><th style="text-align:left">URL</th>
            <th style="text-align:left">Usable GB</th><th></th></tr>
      </thead>
      <tbody id="hostSettingsBody"></tbody>
    </table>
    <div style="margin-top:.5rem; display:flex; gap:.4rem; align-items:center; flex-wrap:wrap;">
      <button id="addHostRow" type="button">+ add host</button>
      <button id="saveHosts" type="button">save</button>
      <span id="hostsMsg" style="font-size:.78rem;"></span>
    </div>
  </div>
</div>
</div><!-- .wrap -->

<script>
// The queue renders into THREE tables (Needs attention / Queue / Finished bundles).
// `tbody` is the one currently being written; refresh() points it at the right one
// before each bundle or row, so every row builder below appends exactly as before.
const activeBody = document.querySelector('#jobs tbody');
const attnBody = document.querySelector('#attnTable tbody');
const finBody = document.querySelector('#finishedTable tbody');
const stalledBody = document.querySelector('#stalledTable tbody');
let tbody = activeBody;
let dragId = null;
let dragStartedAt = null;
// Bundle (parent-row) drag reordering (the user 2026-09-18). Kept SEPARATE from dragId so a
// bundle drag and a job drag can never be confused: only one is ever non-null at a time.
// Drag is initiated only from a dedicated grip handle (not the whole parent row), because
// the parent row is also the expand/collapse toggle -- a row-wide drag fights the toggle.
let dragBundleKey = null;
// Per-PLAN expand/collapse of the queue's slice rollup, keyed by group_key and
// persisted across the 5s poll (same approach as the Run Status panel's
// parentExpanded/projectExpanded) so a refresh never snaps open/shut a plan the
// user just toggled. Undefined for a plan => its default (see `active` below).
let planExpanded = {};
// Per-SLICE history toggles (Dashboard B), keyed "<plan>/<sid>"; remembered like
// planExpanded. Absent key = the default (open only for failed/escalated slices).
let sliceExpanded = {};
let bundleViews = {views: {}, activity: [], active: null};
let queueWait = null;   // last /api/queue-wait payload, read by the idle tile
// FINISHED bundles (the user 2026-10-05): bundles with no queue row left, each with its
// full per-slice history, newest activity first. Paged + age-bounded server-side
// (/api/bundle-history); "show all ages" drops the age window, "show more" pages on.
let finishedBundles = {views: [], total: 0, has_more: false, stalled: [], stalled_total: 0};
let triageOpen = {open: 0, repeats: 0};   // /api/triage: open failure-signature triage packets
let finishedDays = 3, finishedLimit = 20, finishedFetchedAt = 0;
let stalledShown = 25;   // the stalled panel is never age-bounded; page it client-side

const transitioning = {};  // jobId -> {type: 'pausing'|'starting', since: timestamp}, client-side
                            // only visual feedback for the daemon's real ~15-30s pause/relaunch
                            // cycle latency, cleared once the real status changes or after 60s.
function markTransitioning(jobId, type) {
  transitioning[jobId] = {type, since: Date.now()};
}


// BEGIN sliceSuffix (extracted verbatim and EXECUTED by --self-test)
function formatElapsed(s) {
  if (s === null || s === undefined) return '0:00';
  s = Math.floor(s);
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), sec = s % 60;
  const pad = n => String(n).padStart(2, '0');
  return h > 0 ? `${h}:${pad(m)}:${pad(sec)}` : `${m}:${pad(sec)}`;
}
function sliceSuffix(sl) {
  const hist = (sl && sl.history) || [];
  const att = hist.reduce((m, h) => Math.max(m, h.attempt || 1), 1);
  const parts = [];
  // ONE attempt counter (failure_ledger.slice_counter, same numbers as `ollama-dispatch-slice
  // --status` and the budget refusal): author jobs window/budget + lifetime/cap, last cause,
  // "same cause as <job>". Falls back to the old history-derived count if absent.
  const ctr = sl && sl.counter;
  if (ctr) {
    parts.push('author jobs ' + ctr.jobs_window + '/' + ctr.job_budget + ' (life ' + ctr.jobs_lifetime + '/' + ctr.lifetime_cap + ')' + (ctr.at_cap ? ' AT CAP' : ''));
    if (ctr.last_signature) parts.push(ctr.last_signature + (ctr.same_as ? ' = same cause as ' + String(ctr.same_as).slice(0, 8) : ''));
  } else if (att > 1) parts.push('attempt ' + att);
  let rnd = null;
  for (const h of hist) { if ((h.attempt || 1) !== att) continue; const m = /^refining \(round (\d+)\)/.exec(h.kind || ''); if (m) rnd = +m[1]; }
  if (rnd) parts.push('refine r' + rnd);
  if (sl && sl.active) { const run = hist.find(h => h.live && h.status === 'running' && h.duration_s != null); const el = run ? run.duration_s : sl.elapsed_s; if (el != null) parts.push(formatElapsed(el)); }
  return parts.join(' · ');
}
const sliceOpenByDefault = sl => !!(sl && sl.attention);
// Short stage name for a child row under a slice (the bundle + slice are already on the
// slice row above it); the full label goes in the row's title attribute.
function stageLabel(kind, label) {
  const k = String(kind || '');
  const m = /^refining \(round (\d+)\)/.exec(k);
  if (m) return 'refine r' + m[1];
  if (k === 'authoring') return 'author';
  if (k === 'coding') return 'code';
  if (k === 'escalation-review' || k === 'escalation review') return 'escalation / heal';
  if (k === 'second-opinion' || k === 'second opinion') return '2nd opinion';
  if (k) return k;
  const l = String(label || '');
  if (/^regate-/.test(l)) return 'regate';
  if (/^gate-/.test(l)) return 'gate';
  if (/^esc-review-/.test(l)) return 'escalation / heal';
  if (/^secondop-/.test(l)) return '2nd opinion';
  if (/^auto-author-/.test(l)) { const c = /-c(\d+)$/.exec(l); return c ? 'author c' + c[1] : 'author'; }
  const r = /^auto-refine-.*-r(\d+)(?: \[[^\]]*\])?$/.exec(l);
  if (r) return 'refine r' + r[1];
  return /^auto-refine-/.test(l) ? 'refine r1' : 'code';
}
// Slice header text: slice number + entry point ("s1 · mapServiceRequestStatus"); with no
// entry point (title) fall back to the slug without its sNN- prefix. `tip` keeps the
// full sid + title for the hover.
function sliceHeader(sl, key) {
  if (sl && sl.job) return {num: 'Job', entry: sl.title || '', tip: (sl.title || '') + ' ' + (sl.sid || '')};
  const sid = trimBundle(sl && sl.sid, key);
  const m = /^(s\d+[a-z]?)(?:-(.*))?$/.exec(sid);
  const num = m ? 'Slice ' + m[1].slice(1) : sid;
  const title = trimBundle((sl && sl.title) || '', key).trim();
  const entry = title || (m && m[2]) || '';
  return {num, entry, tip: sid + (title ? ' ' + title : '')};
}
// Group a slice's history by attempt: {n: [entries]} plus the sorted attempt numbers.
function groupAttempts(hist) {
  const by = {};
  for (const h of hist || []) (by[h.attempt || 1] = by[h.attempt || 1] || []).push(h);
  return {by, nums: Object.keys(by).map(Number).sort((a, b) => a - b)};
}
// One-line summary of a finished attempt: "Attempt 2 · refine r1 · done · 8:12", plus the
// last entry's outcome (gate verdict / preflight verdict / no-progress reason) when it adds.
function attemptSummary(n, entries) {
  const last = entries[entries.length - 1] || {};
  const dur = entries.reduce((t, h) => t + (h.duration_s || 0), 0);
  const st = last.status || '';
  let out = String(last.result || '').replace(/\s+/g, ' ').trim();
  if (out.length > 70) out = out.slice(0, 67) + '...';
  const parts = ['Attempt ' + n, stageLabel(last.kind, last.label)];
  if (st) parts.push(st);
  if (out && out !== st) parts.push(out);
  if (dur > 0) parts.push(formatElapsed(dur));
  return parts.join(' \u00b7 ');
}
// Within one attempt, fold every EARLIER refine round (all but the latest) into one
// collapsible item once there are two or more of them: [{h} | {fold: [h...], label}].
function foldRounds(entries) {
  const rnd = h => { const m = /^refining \(round (\d+)\)/.exec(h.kind || ''); return m ? +m[1] : null; };
  const rounds = entries.filter(h => rnd(h) !== null);
  const max = rounds.reduce((m, h) => Math.max(m, rnd(h)), 0);
  const early = rounds.filter(h => rnd(h) < max);
  if (early.length < 2) return entries.map(h => ({h}));
  const out = []; let placed = false;
  for (const h of entries) {
    if (early.includes(h)) {
      if (!placed) { placed = true; out.push({fold: early, label: 'refine r' + rnd(early[0]) + '-r' + rnd(early[early.length - 1]) + ' \u00b7 ' + early.length + ' rounds'}); }
      continue;
    }
    out.push({h});
  }
  return out;
}
// Drop a leading '<bundle>-' the bundle row above already shows.
function trimBundle(txt, key) {
  const t = String(txt || ''), p = String(key || '') + '-';
  return key && t.startsWith(p) && t.length > p.length ? t.slice(p.length) : t;
}
// END sliceSuffix

// TRUTH after an arrow (the user 2026-09-27: "that's why we have arrows"). Every reorder
// reply carries `truth` -- one honest sentence on when the job/bundle will launch
// (e.g. "queued behind committed bundle X") -- shown here for a few seconds, so an
// arrow never looks like it worked when the commitment still decides.
function showTruth(msg) {
  if (!msg) return;
  let el = document.getElementById('arrowTruth');
  if (!el) {
    el = document.createElement('div');
    el.id = 'arrowTruth';
    el.style.cssText = 'position:fixed;left:16px;right:16px;bottom:16px;max-width:720px;margin:0 auto;'
      + 'padding:10px 14px;border-radius:8px;background:#222;color:#eee;font-size:13px;'
      + 'box-shadow:0 2px 12px rgba(0,0,0,.35);z-index:9999';
    document.body.appendChild(el);
  }
  el.textContent = msg;
  el.style.display = 'block';
  clearTimeout(showTruth._t);
  showTruth._t = setTimeout(() => { el.style.display = 'none'; }, 9000);
}
async function truthOf(res) {
  try { const d = await res.clone().json(); return d && (d.truth || (d.preempt && d.preempt.truth)); }
  catch (e) { return null; }
}

async function moveJob(id, beforeId) {
  const res = await fetch('/api/jobs/move', {method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({id, before_id: beforeId || null})});
  if (res.ok) showTruth(await truthOf(res));
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

// paused/held share pending's tier (1), not their own -- a paused job can sit interleaved
// WITHIN the true pending sequence (its position matters for what launches next once
// resumed, same as any pending job), so the display must not split them into separate
// visual groups: that mismatch between what's shown and what reorder buttons actually
// operate on (pendingIds, in true array order) is exactly what made "send to top" look
// broken -- confirmed live 2026-08-29, a paused job several jobs deep in the real order
// displayed in a totally separate section below ALL pending rows, so a reorder that
// correctly landed a row at position 0 of the true order looked like "it only moved one
// spot" relative to the wrong (grouped-by-status) visual reference point.
// 'held' is the ONE state deliberately sunk to the bottom (2026-09-18, later the
// same day): a hold is STICKY -- an operator/fit hold sits for hours and is never
// actionable from the active worklist, so it clutters the middle of the table. The
// transient flips the shared tier exists to protect (paused/queued/scheduled) stay
// at tier 1 and never jump, and a held job that LIFTS to pending rejoins the
// pending block in its true FIFO position. Only a real reorder (drag / send-to-top
// / send-to-bottom) may move a row otherwise.
// Injected from Python (QUEUE_STATUS_ORDER) so the sort tiers have ONE definition
// and --self-test can prove the held -> pending index stability directly.
const STATUS_ORDER = __QUEUE_STATUS_ORDER__;

// ---- bundle-header status breakdown -----------------------------------------
// BEGIN planAlertSummary (extracted verbatim and EXECUTED by --self-test)
// The order the parent row's breakdown reads in. Top-level, not function-scoped, so
// --self-test can run the real shipped code rather than assert on its source text.
const STATUS_SUMMARY_ORDER = ['running', 'pending', 'paused', 'planned', 'held', 'blocked', 'failed'];
// Injected from Python (PLAN_ALERT_STATUSES) -- ONE definition, same as STATUS_ORDER.
const ALERT_STATUSES = __PLAN_ALERT_STATUSES__;
// PURE. Split a bundle's {status: count} breakdown into the part that needs a DECISION
// and the routine part, and render the former as its own badge.
//
// the user, 2026-09-19, on the bg-escalation header: "right now the counter makes it seem
// like its already done when its failed". `1 failed` was one comma-separated item in a
// #666 .proj-summary, immediately beside a bold `4/5 slices` chip and a green
// `running: s5-audit` -- so a bundle carrying a failure looked exactly like a bundle
// sailing through. The fraction is NOT touched (4/5 was accurate, and the bundle
// really was not blocked); the point is that a failure must survive a two-second
// glance instead of being read as done.
//
// ...and then, same day, the correction that matters more (the user: "why are we showing
// failed if nothing is blocking us that failed" / "is the failure something i need to
// care about is a better way to put it"). `alertCounts` is NOT derived from the status
// here: the server stamps needs_attention per row, having asked whether the row's
// underlying SLICE is actually unresolved (_needs_attention_ids). A failed refine
// round on a slice that already landed is reported in the faint breakdown like any
// other count and never alarms. An alarm that cries wolf trains the eye to skip it,
// which costs more than the alarm was ever worth.
function planAlertSummary(counts, alertCounts) {
  const ac = alertCounts || {};
  const alerts = [], rest = [];
  const push = s => {
    const n = ALERT_STATUSES.includes(s) ? (ac[s] || 0) : 0;
    if (n) alerts.push(`${n} ${s}`);
    if (counts[s] - n > 0) rest.push(`${counts[s] - n} ${s}`);
  };
  for (const s of STATUS_SUMMARY_ORDER) if (counts[s]) push(s);
  for (const s of Object.keys(counts)) if (counts[s] && !STATUS_SUMMARY_ORDER.includes(s)) push(s);
  const badge = alerts.length
    ? `<span class="plan-alert" title="${alerts.join(', ')} in this bundle, on a slice that has NOT reached a successful outcome -- so this one is waiting on a decision, not just a round that did not pass. Expand the bundle to see which.">&#9888; ${alerts.join(' &middot; ')}</span> `
    : '';
  return {alerts: alerts, rest: rest, badge: badge,
          rowClass: alerts.length ? ' queue-parent-alert' : ''};
}
// END planAlertSummary

// The build this document was served as. Every JSON response carries the server's
// current one in X-Frontend-Version; when they differ, THIS TAB is running an old
// front-end -- the page polls data forever but never re-fetches its own CSS/JS, so
// a restarted server silently leaves an open tab on the old build (2026-09-19: a
// mobile CSS fix looked "not deployed" on a phone for exactly this reason, over
// completely live data). Reload once, guarded so a flapping header cannot loop.
const FRONTEND_VERSION = __FRONTEND_VERSION__;
let _reloadingForVersion = false;
// The in-memory flag alone is NOT enough: location.reload() builds a fresh
// document and resets it, so if the reload does not actually deliver the new
// build the page reloads again, and again. Measured in WebKit with a pinned
// mismatching header: 419 loads in 12 seconds -- a hot loop that would hammer a
// server this page already polls every 4s. So the attempt is remembered in
// sessionStorage (per-tab, cleared when the tab closes) and each target build is
// only ever chased ONCE. If the reload did not fix it, something upstream is
// serving an old document and reloading harder will not help.
const _RELOAD_KEY = 'ollama-queue-reloaded-for';
function checkFrontendVersion(res) {
  try {
    const v = res && res.headers && res.headers.get('X-Frontend-Version');
    if (!v || _reloadingForVersion) return;
    let tried = null;
    try { tried = sessionStorage.getItem(_RELOAD_KEY); } catch (e) { /* blocked */ }
    if (v === FRONTEND_VERSION) {
      // We are current: forget any earlier attempt so a LATER build is chased too.
      if (tried) { try { sessionStorage.removeItem(_RELOAD_KEY); } catch (e) {} }
      return;
    }
    if (tried === v) return;          // already reloaded once for this build
    try { sessionStorage.setItem(_RELOAD_KEY, v); } catch (e) { /* blocked */ }
    _reloadingForVersion = true;
    location.reload();
  } catch (e) { /* header unreadable -- never break the poll over this */ }
}

// ---- layout helpers (2026-10-05 redesign) ------------------------------------
// One overflow menu per row. `items` are ready-made <button class="mi" data-...>
// strings (plus '<hr>' separators); the data-* hooks are what the handlers bind to.
function menuHtml(items, key) {
  const body = (items || []).filter(Boolean);
  if (!body.some(x => x !== '<hr>')) return '';
  return `<details class="menu" data-menu="${escapeHtml(key)}"><summary title="More actions" aria-label="More actions">&middot;&middot;&middot;</summary>`
    + `<div class="menu-pop" role="menu">${body.join('')}</div></details>`;
}
// done/total as a bar plus the numbers (the bar is decoration; the numbers are the fact).
function progHtml(through, total, unit, title) {
  const pct = total ? Math.max(0, Math.min(100, Math.round(100 * (through || 0) / total))) : 0;
  return `<span class="prog"${title ? ` title="${escapeHtml(title)}"` : ''}><span class="bar"><i style="width:${pct}%"></i></span>`
    + `<span><span class="plan-frac">${through || 0}/${total}</span> ${escapeHtml(unit || 'slices')}</span></span>`;
}
function dayLabel(ts) {
  if (!ts) return 'Earlier';
  const d = new Date(ts * 1000), now = new Date();
  const key = x => x.getFullYear() + '-' + x.getMonth() + '-' + x.getDate();
  const yest = new Date(now); yest.setDate(now.getDate() - 1);
  if (key(d) === key(now)) return 'Today';
  if (key(d) === key(yest)) return 'Yesterday';
  return d.toLocaleDateString([], {weekday: 'short', month: 'short', day: 'numeric'});
}
const EXIT_NAMES = {0: 'converged', 1: 'verify failed', 2: 'iteration cap', 3: 'paused, resumable',
                    4: 'refused to start', 5: 'done, unconverged'};
// One line on why a job failed, from the most specific field the server gave us.
function failReason(j) {
  let t = j.failure_detail || j.terminal_reason || j.error || j.failure_class || '';
  if (!t && j.exit_code != null) t = 'exit ' + j.exit_code + (EXIT_NAMES[j.exit_code] ? ' (' + EXIT_NAMES[j.exit_code] + ')' : '');
  t = String(t || 'failed').replace(/\s+/g, ' ').trim();
  if (j.failure_class && t !== j.failure_class && !t.includes(j.failure_class)) t = j.failure_class + ': ' + t;
  return t.length > 180 ? t.slice(0, 177) + '...' : t;
}
// A standalone (bundle-less) row that needs a person: failed, held, or blocked.
// paused stays in the queue: it keeps its queue position and has its own Resume button.
function jobAttentionKind(j) {
  if (j.status === 'failed') return 'failed';
  if (j.status === 'held') return 'parked';
  if (j.status === 'blocked') return 'blocked';
  return null;
}
// The one-line reason (and the run whose log explains it) for a stuck bundle.
function bundleAttention(g, view, kind) {
  const kids = g.children || [];
  if (kind === 'parked') {
    const c = kids.find(x => x.status === 'held' || x.status === 'paused') || {};
    const n = kids.filter(x => x.status === 'held' || x.status === 'paused').length;
    return {reason: c.wait_reason || `${n} slice${n === 1 ? '' : 's'} ${c.status || 'held'}; resume to put ${n === 1 ? 'it' : 'them'} back in the queue.`};
  }
  const bad = view ? (view.slices || []).filter(x => x.attention) : [];
  if (bad.length) {
    const sl = bad[0];
    const hist = (sl.history || []).filter(h => h.id);
    const h = [...hist].reverse().find(x => /fail|escalat|error/i.test(String(x.status) + ' ' + String(x.result))) || hist[hist.length - 1];
    const what = String(sl.detail || sl.phase || 'failed').replace(/\s+/g, ' ').trim();
    let reason = `${sl.sid}: ${what}`;
    if (reason.length > 170) reason = reason.slice(0, 167) + '...';
    if (bad.length > 1) reason += ` (+${bad.length - 1} more slice${bad.length > 2 ? 's' : ''})`;
    return {reason, logId: h ? h.id : null, logLabel: h ? h.label : null};
  }
  const c = kids.find(x => x.needs_attention) || kids.find(x => x.status === 'failed' || x.status === 'blocked');
  if (!c) return {reason: ''};
  if (kind === 'blocked') return {reason: c.wait_reason || (c.label + ' is blocked'), logId: null};
  return {reason: `${c.label}: ${failReason(c)}`, logId: c.id, logLabel: c.label};
}
// Summary strip: running now / queue depth / needs attention, plus section counts.
// "Waiting on" banner: one line per lane that is IDLE while work is pending, from the
// daemon's queue-wait.json. RED when the same reason has held an idle lane > stuck_after_s.
function renderWaitBanner(w, jobs) {
  const el = document.getElementById('waitBanner');
  if (!el) return;
  const dur = s => { s = Math.max(0, Math.round(s)); return s >= 3600 ? Math.floor(s / 3600) + 'h' + String(Math.floor(s % 3600 / 60)).padStart(2, '0') + 'm' : s >= 60 ? Math.floor(s / 60) + 'm' : s + 's'; };
  const now = w.now || Date.now() / 1000;
  const lines = [];
  let stuck = false;
  if (w.source === 'state') {
    for (const [lane, ln] of Object.entries(w.lanes || {})) {
      if (ln.state !== 'idle' || !ln.pending) continue;
      const r = ln.reason || {};
      const isStuck = (w.stuck || []).includes(lane);
      stuck = stuck || isStuck;
      lines.push(`<span class="lane"><span class="badge">${escapeHtml(lane)} idle ${dur(now - (ln.idle_since || now))}</span>`
        + `${isStuck ? '<b>POSSIBLE STUCK SEAM (same reason &gt; ' + Math.round((w.stuck_after_s || 300) / 60) + ' min): </b>' : ''}`
        + `Waiting on: ${escapeHtml(r.sentence || 'reason unknown')} <span class="log">(${ln.pending} pending, same reason ${dur(now - (r.since || now))})</span></span>`);
    }
  } else if ((jobs || []).some(j => j.status === 'pending' || j.status === 'held')) {
    lines.push(`<span class="lane">${escapeHtml(w.message || 'reason unavailable')}</span>`);
    const lg = w.log || {};
    if (lg.focus_line) lines.push(`<span class="lane log">from daemon log (last focus line, untimestamped): ${escapeHtml(lg.focus_line)}</span>`);
    for (const l of (lg.held_lines || [])) lines.push(`<span class="lane log">from daemon log: ${escapeHtml(l)}</span>`);
  }
  el.hidden = !lines.length;
  el.classList.toggle('stuck', stuck);
  el.innerHTML = lines.join('');
}

// "Running now" tile, idle case: ONE short plain line saying what the queue is waiting on.
// Daemon state reason when it is live; else derived from API data (the committed bundle, the
// pending count). NEVER daemon-log text (the log's HELD/focus lines can be stale).
function idleWaitText(w, jobs, activeKey) {
  const pend = (jobs || []).filter(j => j.status === 'pending' || j.status === 'held');
  if (!pend.length) return 'Queue empty';
  if (w && w.source === 'state') {
    const out = [];
    for (const ln of Object.values(w.lanes || {})) {
      const s = ln && ln.state === 'idle' && ln.pending && ln.reason && ln.reason.sentence;
      if (s && !out.includes(s)) out.push(s);
    }
    if (out.length) return 'Waiting on: ' + out.join(' | ');
  }
  if (activeKey) return 'Waiting on: bundle ' + activeKey + ' to finish';
  return 'Waiting on: queued work (reason shows after the next daemon restart)';
}


// PER-LANE "RUNNING NOW" (Penn 2026-10-09): one equal row per lane -- studio-db, unraid, CPU
// lane -- each with label, model, host, elapsed, bundle and a short live status line; an
// idle lane says so explicitly ("unraid: idle"). A gpu-exclusive job shows its PHASE.
const NOW_LANES = [['studio-db', 'studio-db'], ['unraid', 'unraid'], ['cpu', 'CPU lane']];
function laneOfJob(j) {
  const l = String(j.lane || j.host_pref || '').toLowerCase();
  if (l.includes('unraid')) return 'unraid';
  if (l === 'cpu' || l.startsWith('cpu')) return 'cpu';
  return 'studio-db';
}
function laneStatusLine(j) {
  if (j.job_kind === 'gpu_exclusive') {
    if (j.gpu_wait) return 'waiting for VRAM: ' + j.gpu_wait;
    if (j.phase === 'warming') return 'loading';
    return 'serving' + (j.gpu_summary ? ' - ' + j.gpu_summary : '');
  }
  if (j.phase === 'warming') return 'loading model';
  const bits = [];
  if (j.iteration != null && j.max_iters != null) bits.push('iter ' + j.iteration + '/' + j.max_iters);
  if (j.tok_s != null) bits.push(j.tok_s.toFixed(1) + ' tok/s');
  return bits.length ? bits.join(' - ') : 'running';
}
function laneNowRows(jobs, acts, cpuRunning) {
  const by = {};
  for (const [k] of NOW_LANES) by[k] = [];
  const actOf = id => (acts || []).find(a => a.kind === 'gpu' && a.id === id) || {};
  for (const j of (jobs || [])) {
    if (j.status !== 'running') continue;
    const a = actOf(j.id);
    by[laneOfJob(j)].push({label: a.display || j.label || j.id, model: a.model || j.model || '?',
      host: a.host || j.lane || j.host_pref || '?', elapsed_s: a.elapsed_s != null ? a.elapsed_s : j.elapsed_s,
      bundle: a.group_key || null, line: laneStatusLine(j), id: j.id});
  }
  for (const c of (cpuRunning || [])) {
    by.cpu.push({label: c.label || String(c.id || '').slice(0, 8), model: c.stage || 'cpu', host: c.runner || 'cpu',
      elapsed_s: c.running_s, bundle: c.bundle || null, line: c.stage ? 'stage ' + c.stage : 'running', id: c.id});
  }
  return NOW_LANES.map(([k, name]) => ({lane: k, name, idle: !by[k].length, jobs: by[k]}));
}
function laneNowHtml(rows) {
  return '<div class="lane-now">' + rows.map(r => {
    if (r.idle) return `<div class="lane-card idle" data-lane="${r.lane}"><span class="lane-name">${escapeHtml(r.name)}</span>: idle</div>`;
    return r.jobs.map(x => `<div class="lane-card run" data-lane="${r.lane}"><span class="lane-name">${escapeHtml(r.name)}</span>`
      + ` <b>${escapeHtml(x.label)}</b> <span class="lane-meta">model ${escapeHtml(x.model)} &middot; host ${escapeHtml(x.host)}`
      + ` &middot; ${formatElapsed(x.elapsed_s)}${x.bundle ? ' &middot; bundle ' + escapeHtml(x.bundle) : ''}</span>`
      + ` <span class="lane-line">${escapeHtml(x.line)}</span></div>`).join('');
  }).join('') + '</div>';
}

function renderSummary(jobs, acts, attnStats, q) {
  const gpu = (acts || []).filter(a => a.kind === 'gpu');
  const other = (acts || []).filter(a => a.kind !== 'gpu');
  const runningJobs = jobs.filter(j => j.status === 'running');
  const now = document.getElementById('sumNow');
  const first = gpu[0] || (runningJobs[0] ? {id: runningJobs[0].id, label: runningJobs[0].label,
    model: runningJobs[0].model, host: runningJobs[0].lane || runningJobs[0].host_pref,
    elapsed_s: runningJobs[0].elapsed_s, group_key: runningJobs[0].bundle ? runningJobs[0].group_key : null} : null);
  const fmtAct = a => a.kind === 'gpu'
    ? `&#9654; ${escapeHtml(a.display || a.label || a.id)} <span style="opacity:.75">(${escapeHtml(a.model || '')} @ ${escapeHtml(a.host || '')}, ${formatElapsed(a.elapsed_s)})</span>${a.group_key ? ' <span style="opacity:.8">[bundle: <b>' + escapeHtml(a.group_key) + '</b>]</span>' : ''}`
    : `&#9881; ${escapeHtml(a.what || a.tool)} <span style="opacity:.75">(${escapeHtml(a.wt || '')}, ${formatElapsed(a.elapsed_s)})</span>`;
  if (first) {
    const j = jobs.find(x => x.id === first.id) || {};
    const meta = [
      `<span>model <b>${escapeHtml(first.model || j.model || '?')}</b></span>`,
      `<span>host <b>${escapeHtml(first.host || j.lane || j.host_pref || '?')}</b></span>`,
      `<span>elapsed <b>${formatElapsed(first.elapsed_s != null ? first.elapsed_s : j.elapsed_s)}</b></span>`,
      j.tok_s != null ? `<span><b>${j.tok_s.toFixed(1)}</b> tok/s</span>` : '',
      (j.iteration != null && j.max_iters != null) ? `<span>iter <b>${j.iteration}/${j.max_iters}</b></span>` : '',
      first.group_key ? `<span>bundle <b>${escapeHtml(first.group_key)}</b></span>` : ''].join('');
    const rest = [...gpu.slice(1), ...other];
    now.innerHTML = `<div class="k"><span class="pulse"></span>Running now</div>
      <div class="now-job">${escapeHtml(first.display || first.label || first.id)}</div>
      <div class="now-meta">${meta}</div>
      ${rest.length ? `<div class="now-more">Also live now: ${rest.map(fmtAct).join(' &middot; ')}</div>` : ''}
      ${laneNowHtml(laneNowRows(jobs, acts, window.cpuLaneRunning))}`;
    now.style.cursor = 'pointer';
    now.onclick = () => openLivelog(first.id, first.label || first.id);
    now.title = 'Open the live log';
  } else {
    now.innerHTML = `<div class="k"><span class="idle-dot"></span>Running now</div>
      <div class="now-job" style="color:var(--muted);font-weight:500">Nothing on the GPU</div>
      <div class="now-more" id="idleWait">${escapeHtml(idleWaitText(queueWait, jobs, bundleViews.active))}</div>
      ${other.length ? `<div class="now-more">Off-GPU, live now: ${other.map(fmtAct).join(' &middot; ')}</div>` : ''}
      ${laneNowHtml(laneNowRows(jobs, acts, window.cpuLaneRunning))}`;
    now.style.cursor = ''; now.onclick = null; now.title = '';
  }
  const cnt = st => jobs.filter(j => j.status === st).length;
  const pend = cnt('pending'), paused = cnt('paused'), planned = cnt('planned');
  document.getElementById('sumQueue').innerHTML = `<div class="k">Queue</div>
    <div class="v">${pend + paused + planned}</div>
    <div class="s">${pend} pending${paused ? ' &middot; ' + paused + ' paused' : ''} &middot; ${planned} planned</div>`;
  const nA = attnStats.failed + attnStats.parked + attnStats.blocked + (attnStats.stalled || 0);
  const nList = nA - (attnStats.stalled || 0);
  const brk = ['failed', 'parked', 'blocked', 'stalled'].filter(k => attnStats[k]).map(k => attnStats[k] + ' ' + k).join(' &middot; ');
  const at = document.getElementById('sumAttn');
  at.classList.toggle('has', nA > 0);
  at.innerHTML = `<div class="k">Needs attention</div><div class="v">${nA}</div>
    <div class="s">${nA ? brk + (attnStats.stalled ? ' (bundles)' : '') : 'nothing stuck'}${(triageOpen && triageOpen.open) ? ` &middot; <span title="python3 ~/bin/triage-emit.py --json">triage: ${triageOpen.open} signature${triageOpen.open === 1 ? '' : 's'}${triageOpen.repeats ? ' (' + triageOpen.repeats + ' repeated)' : ''}</span>` : ''}${attnStats.stale ? ` &middot; <a href="#stalledPanel" class="stale-link" style="color:var(--muted)" onclick="var d=document.getElementById('stalledDetails');if(d)d.open=true">stale backlog: ${attnStats.stale}</a>` : ''}</div>`;
  document.getElementById('attnCount').textContent = nA ? String(nA) : '';
  document.getElementById('attnList').hidden = !nList;
  document.getElementById('attnEmpty').hidden = !!nList;
  document.querySelector('#attnEmpty .empty').textContent = attnStats.stalled
    ? attnStats.stalled + ' stalled/failed bundle' + (attnStats.stalled === 1 ? '' : 's') + ' (nothing live, not finished): see "Stalled / failed bundles" below.'
    : 'Nothing is stuck. Failed, parked and blocked work shows up here.';
  document.getElementById('activeCount').textContent =
    `${q.bundles} bundle${q.bundles === 1 ? '' : 's'} · ${q.jobs} job${q.jobs === 1 ? '' : 's'}`;
  document.getElementById('activeEmpty').hidden = !!(q.bundles || q.jobs);
  document.getElementById('updated').textContent = 'updated ' + new Date().toLocaleTimeString() + ' · refreshes every 4s';
}
// Menus: one open at a time; a click elsewhere, Escape, or picking an item closes it.
// While one is open the 4s re-render waits (up to 20s) so it cannot snap shut under you.
let menuOpenedAt = 0;
document.addEventListener('toggle', e => {
  const d = e.target;
  if (!d || !d.classList || !d.classList.contains('menu') || !d.open) return;
  menuOpenedAt = Date.now();
  document.querySelectorAll('details.menu[open]').forEach(o => { if (o !== d) o.open = false; });
}, true);
document.addEventListener('click', e => {
  const inMenu = e.target.closest ? e.target.closest('details.menu') : null;
  document.querySelectorAll('details.menu[open]').forEach(o => { if (o !== inMenu) o.open = false; });
  if (inMenu && e.target.closest('.menu-pop button')) inMenu.open = false;
}, true);
document.addEventListener('keydown', e => {
  if (e.key === 'Escape') document.querySelectorAll('details.menu[open]').forEach(o => { o.open = false; });
});

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
  if (document.querySelector('details.menu[open]') && Date.now() - menuOpenedAt < 20000) return;
  // finished bundles change slowly: refetch at most every 15s (or right after a control)
  const wantFinished = Date.now() - finishedFetchedAt > 15000;
  const [res, bvRes, fbRes, qwRes, trRes] = await Promise.all([fetch('/api/jobs'),
    fetch('/api/bundle-views').catch(() => null),
    wantFinished ? fetch('/api/bundle-history?days=' + finishedDays + '&limit=' + finishedLimit)
      .catch(() => null) : null,
    fetch('/api/queue-wait').catch(() => null),
    fetch('/api/triage').catch(() => null)]);
  checkFrontendVersion(res);   // an open tab notices a new build and reloads once
  const jobs = await res.json();
  try { if (qwRes && qwRes.ok) { queueWait = await qwRes.json(); renderWaitBanner(queueWait, jobs); } } catch (e) { /* keep last */ }
  try { if (trRes && trRes.ok) triageOpen = await trRes.json(); } catch (e) { /* keep last */ }
  // Dashboard B: live per-slice truth; an older API (404) just renders as before.
  try { if (bvRes && bvRes.ok) bundleViews = await bvRes.json(); } catch (e) { /* keep last */ }
  try { if (fbRes && fbRes.ok) { finishedBundles = await fbRes.json(); finishedFetchedAt = Date.now(); } }
  catch (e) { /* keep last */ }
  // pendingIds MUST reflect true FIFO enqueue order (the daemon's actual launch
  // order), not the display sort below -- reorder buttons compute neighbors
  // from this, and it has to match what the daemon itself iterates over.
  // Includes 'paused' jobs too (added 2026-08-29, the user's request) -- a paused job
  // isn't launchable yet, but its position in this list still determines where it
  // lands once resumed, and pending/paused jobs share the same reorder controls.
  const pendingIds = jobs.filter(j => j.status === 'pending' || j.status === 'paused').map(j => j.id);
  // Display-only: running jobs on top, so what's actually happening right now
  // doesn't get lost below a long pending/finished list. Stable within each
  // status group (Array.sort is stable), so relative order otherwise unchanged.
  // display_seq is computed SERVER-side (_queue_display_order, unit-tested there):
  // running on top, then ONE merged waiting line -- pending and planned together,
  // each planned slice slotted directly after the job it waits on, so the table
  // reads top-to-bottom as the real execution sequence -- then sticky `held` last.
  // The STATUS_ORDER fallback keeps an older/paused API payload rendering sanely.
  // Secondary key plan_seq (server-side _annotate_plan_rollup) comes FIRST: it is the
  // min display_seq of the row's slice-plan, so a plan's rows sit together as one
  // block at the position of its earliest slice, and display_seq then orders the
  // slices inside it. Falling back to display_seq alone keeps an older payload (no
  // plan_seq) rendering exactly as before -- flat, in execution order.
  // Finished rows are out of the queue panel, with ONE exception: a row whose PLAN
  // still owes slices (plan_incomplete, server-decided from X<Y). Without it, a plan
  // whose every current row is terminal -- the window before its next slice is
  // enqueued -- vanishes from the queue as if completed, which is exactly what the user
  // called out ("shouldn't be moved to completed until everything is completed").
  // Such a row only ever renders INSIDE its bundle; plan_incomplete is stamped on
  // grouped rows alone, so an ordinary finished job is filtered out as before.
  //
  // TERMINAL_STATUSES also includes 'failed': a plan whose slice-runs state already
  // shows X==Y (plan_incomplete false) is genuinely done, but an orphaned auto-author/
  // refine job for an earlier slice can still be sitting in the live queue as 'failed'
  // (the coding job converged+committed, then its own meta-job got SIGKILLed or
  // empty-diff-overridden afterward -- a known reap gap, not a real outstanding
  // failure). Before this, that stale failed row alone kept the whole completed bundle
  // pinned in the active queue panel with a live "cancel" button (the user 2026-09-19,
  // bg-captcha showing "3/3 slices done" yet still here). A plan that's genuinely
  // incomplete still shows its failed rows exactly as before, via plan_incomplete.
  const TERMINAL_STATUSES = ['done', 'done_unconverged', 'failed'];
  const displayJobs = [...jobs].filter(j => !TERMINAL_STATUSES.includes(j.status)
                                            || j.plan_incomplete).sort((a, b) => {
    const seq = j => j.display_seq ?? (STATUS_ORDER[j.status] ?? 5);
    // bundle_rank FIRST (server-side _bundle_display_order): the bundle's place is the
    // scheduler's -- committed, parked, pending in pick order, held, done -- and never
    // its rows' momentary statuses, so a run starting/finishing cannot move a bundle.
    const brank = j => j.bundle_rank ?? 0;
    return brank(a) - brank(b)
      || (a.plan_seq ?? seq(a)) - (b.plan_seq ?? seq(b)) || seq(a) - seq(b);
  });
  // VISUAL rank of each pending/paused row among the pending/paused rows, in the
  // plan-grouped display order just computed. This is the ONLY thing that makes the
  // two orderings comparable: pendingIds gives the true FIFO position the daemon and
  // the reorder buttons use, this gives the position the eye reads off the table.
  // They diverge whenever plan grouping hoists a row to its plan's anchor -- a
  // re-enqueued slice lands at the FIFO tail but its plan_seq stays pinned to the
  // plan's earliest row, so it renders near the top while being nearly last.
  // the user 2026-09-19: bg-crypto's re-gated slice displayed directly under the running
  // row showing only an up-arrow (it was truly 29th of 30, hence no down-arrow),
  // while bfmr-split-reservation-diagnose displayed lower showing the "I'm first"
  // single up-arrow (it truly WAS first). The arrows were correct both times; the row
  // order simply does not mean what it looks like it means.
  const visualPendingRank = {};
  displayJobs.filter(j => j.status === 'pending' || j.status === 'paused')
             .forEach((j, i) => { visualPendingRank[j.id] = i; });
  // Keep the page where the user left it: innerHTML='' collapses the table for an
  // instant, which can shorten the document and make the browser clamp scrollY.
  // Restore it only when it actually moved (no-op in the common case).
  const _scrollY = window.scrollY;
  // An open overflow menu survives the re-render (same key => reopened below).
  const _openMenu = (document.querySelector('details.menu[open]') || {dataset: {}}).dataset.menu;
  activeBody.innerHTML = ''; attnBody.innerHTML = ''; finBody.innerHTML = '';
  tbody = activeBody;
  // LIVE ACTIVITY (Dashboard B): what is happening RIGHT NOW, GPU or not -- the
  // running job(s) and every off-GPU step (preflight, verify-relevance mutants), so
  // a committed bundle between GPU jobs never reads as hung. Drawn in the summary
  // strip's "Running now" tile (renderSummary), each job naming ITS OWN bundle.
  const acts = (bundleViews.activity || []);
  // Needs-attention bookkeeping, filled while the bundles render below.
  const attnStats = {failed: 0, parked: 0, blocked: 0, stalled: ((finishedBundles || {}).stalled || []).filter(v => v.actionable !== false).length,
    stale: ((finishedBundles || {}).stalled || []).filter(v => v.actionable === false).length};
  let activeBundles = 0, activeJobs = 0;
  // ONE definition of a queue row, used for both a top-level (standalone) row and a
  // plan's child row -- the child only differs by an indent class, so every per-row
  // action (drag, promote, up/down, bottom, resume, remove, pause) keeps working
  // identically inside a plan instead of being re-wired divergently.
  const buildQueueRow = (j, isChild) => {
    const tr = document.createElement('tr');
    if (isChild) tr.classList.add('queue-child');
    tr.dataset.id = j.id;
    tr.style.cursor = 'pointer';
    tr.addEventListener('click', e => {
      if (e.target.closest('button, details, a')) return;   // a control, not the row
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
        document.querySelectorAll('.drop-indicator-above, .drop-indicator-below').forEach(el =>
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
        document.querySelectorAll('.drop-indicator-above, .drop-indicator-below').forEach(el =>
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
        document.querySelectorAll('.drop-indicator-above, .drop-indicator-below').forEach(el =>
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
    // The wait reason is a sentence: it reads under the label, never inside the status chip.
    const _qw = j.queue_wait && j.queue_wait.short;
    const waitHtml = ((_qw || j.wait_reason) && !_t) ? `<span class="wait-reason">${_qw ? '<b>Waiting on:</b> ' + escapeHtml(_qw) + (j.wait_reason && j.wait_reason !== _qw ? ' <span style="opacity:.7">&middot; ' + escapeHtml(j.wait_reason) + '</span>' : '') : escapeHtml(j.wait_reason)}</span>` : '';
    const toks = j.tok_s != null ? j.tok_s.toFixed(1) : '';
    // 0:00 for pending (elapsed_s null); live wall-time for running (recomputed
    // server-side from log ctime each refresh); frozen final duration once terminal.
    // ACTIVE runtime (paused time excluded); paused time, when material, shown beside it.
    const elapsed = formatElapsed(j.elapsed_s) + (j.paused_s != null && j.paused_s >= 60
      ? ` <span style="opacity:.55; font-size:.85em" title="time spent paused (preempted), not counted as runtime">+${formatElapsed(j.paused_s)} paused</span>` : '');
    // Controls: at most ONE inline button (the thing you would most likely do next), and
    // everything else in the row's overflow menu. Same data-* hooks and handlers as ever.
    let actions = '', primary = '';
    const items = [];
    if (isPending) {
      const pos = pendingIds.indexOf(j.id);
      // TRUE queue position, rendered before the menu. The menu's moves act on pendingIds
      // (real FIFO, what the daemon iterates) while the ROW ORDER is plan-grouped, so
      // state the real position; highlighted when the visual rank is far from it.
      const vrank = visualPendingRank[j.id];
      const adrift = vrank != null && Math.abs(vrank - pos) >= 3;
      actions += `<span class="qpos${adrift ? ' qpos-adrift' : ''}" title="True queue position ${pos + 1} of ${pendingIds.length} pending${adrift ? ` -- but it is drawn ${vrank + 1}${vrank < pos ? ' (higher than it really is)' : ' (lower than it really is)'} because the table groups each plan's rows at the position of that plan's earliest slice. The moves act on the TRUE position.` : '. Table rows are grouped by plan, so row order is not queue order.'}">#${pos + 1}</span>`;
      if (pos > 0) items.push(`<button class="mi" data-top title="Send to top of queue">&uarr;&uarr; Send to top</button>`);
      // Whole-job promote: only when this row's logical job HAS more than one pending slice.
      if (j.group_pending > 1) items.push(`<button class="mi" data-promote-group title="Promote the WHOLE job &quot;${j.group_key}&quot; (${j.group_pending} pending slices) to the top, keeping slice order">&uarr;&uarr; Send whole job to top (${j.group_pending})</button>`);
      if (pos > 0) items.push(`<button class="mi" data-up>&uarr; Move up one</button>`);
      if (pos === 0) items.push(`<button class="mi" data-promote-front title="Pause whatever is running and run this one now">&#9654; Run now (pauses current job)</button>`);
      if (pos < pendingIds.length - 1) items.push(`<button class="mi" data-down>&darr; Move down one</button>`);
      // Send to bottom: the existing move endpoint with before_id: null.
      if (pos < pendingIds.length - 1) items.push(`<button class="mi" data-bottom title="Send to bottom of queue">&darr;&darr; Send to bottom</button>`);
    }
    if (j.status === 'paused') primary = `<button class="btn primary" data-resume>Resume</button>`;
    // ONE pause control (graceful SIGTERM; state saved, resumable).
    const isGpuJob = j.job_kind === 'gpu_exclusive';
    // A GPU-EXCLUSIVE job is a plain shell command: SIGTERM STOPS it (its runner kills
    // the command and runs its on-abort cleanup). There is nothing to resume.
    if (j.status === 'running' && isGpuJob) items.push(`<button class="mi" data-kill title="Stops the exclusive GPU job (SIGTERM): the command is killed and its cleanup runs. It is not resumable.">&#9632; Stop GPU job (not resumable)</button>`);
    else if (j.status === 'running') items.push(`<button class="mi" data-kill title="Gracefully pauses the job (SIGTERM) -- it saves state and can be resumed, this does not discard work">&#10074;&#10074; Pause (keeps its work)</button>`);
    if (j.status === 'failed' && !primary) primary = `<button class="btn primary" data-log>View log</button>`;
    else items.push(`<button class="mi" data-log>View log</button>`);
    if (isRemovable) items.push(`<hr><button class="mi danger" data-remove>Remove from queue</button>`);
    actions += primary + menuHtml(items, 'job:' + j.id);
    // What used to be six mostly-empty columns: one quiet line under the label.
    const meta = [isGpuJob ? `<b title="Non-LLM job with this lane's GPU to itself: resident Ollama models were unloaded first; gates for the lane run before it">GPU-EXCLUSIVE</b> ${escapeHtml(j.gpu_summary || '')}` : '',
      isGpuJob && j.gpu_wait ? 'waiting: ' + escapeHtml(j.gpu_wait) : '',
      (j.model && !isGpuJob) ? escapeHtml(j.model) : '', escapeHtml(j.lane || j.host_pref || ''),
      progress ? 'iter ' + progress : '', toks ? toks + ' tok/s' : '',
      j.pid != null ? 'pid ' + j.pid : '', j.exit_code != null ? 'exit ' + j.exit_code : '']
      .filter(Boolean).map(x => `<span>${x}</span>`).join('');
    const why = j.status === 'failed' ? failReason(j) : '';
    const stCls = j.phase === 'warming' ? 'warming' : j.status;
    tr.innerHTML = `
      <td${isPending ? ' title="Drag to reorder, or drop onto the running job to run this one now"' : ''}>${isPending ? '☰' : ''}</td>
      <td>${escapeHtml(j.label || '')}${j.rerun ? ` <span class="rerun" title="${escapeHtml(j.rerun.cause || '')}">#${escapeHtml(String(j.rerun.n))}</span>` : ''}<span class="chip m-chip ${escapeHtml(stCls)}">${statusLabel}</span>${meta ? `<div class="sub">${meta}</div>` : ''}${why ? `<div class="reason bad">${escapeHtml(why)}</div>` : ''}${waitHtml}</td>
      <td class="st"><span class="chip status-${escapeHtml(stCls)} ${escapeHtml(stCls)}">${statusLabel}</span></td>
      <td class="when">${j.elapsed_s == null && !isRunning ? '' : elapsed}</td>
      <td class="acts">${actions}</td>
    `;
    const logBtn = tr.querySelector('[data-log]');
    if (logBtn) logBtn.addEventListener('click', () => openLivelog(j.id, j.label));
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
      const res = await fetch('/api/jobs/' + j.id + '/promote', {method: 'POST'});
      if (res.ok) showTruth(await truthOf(res));
      refresh();
    });
    const groupBtn = tr.querySelector('[data-promote-group]');
    if (groupBtn) groupBtn.addEventListener('click', async () => {
      // Server-side, atomic, and order-preserving: /promote-group resolves this row's
      // group from the slicer's own plan files and moves every PENDING member of it to
      // the front in one locked write. It does NOT pause anything that is running (a
      // running member of the same job keeps its lane), so no confirm is needed.
      const res = await fetch('/api/jobs/' + j.id + '/promote-group', {method: 'POST',
        headers: {'Content-Type': 'application/json'}, body: JSON.stringify({take_focus: true})});
      if (!res.ok) alert('promote group failed: ' + (await res.text()));
      else showTruth(await truthOf(res));
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
    const bottomBtn = tr.querySelector('[data-bottom]');
    if (bottomBtn) bottomBtn.addEventListener('click', () => {
      // before_id: null already means "append to the end" (moveJob passes
      // beforeId || null straight through) -- no separate endpoint needed.
      moveJob(j.id, null);
    });
    const removeBtn = tr.querySelector('[data-remove]');
    if (removeBtn) removeBtn.addEventListener('click', async () => {
      // The response was thrown away here, which is half of why "remove doesn't work"
      // (the user 2026-09-19) was so hard to see: the server can legitimately answer
      // "kept in place" (a paused/blocked row with resumable state), and it can answer
      // 400 QueueActionError -- and BOTH used to look identical to success, because
      // refresh() simply re-rendered the unchanged row with no message at all. Read the
      // answer and say what actually happened. The server-side half of the fix is in
      // ollama-queue.py cancel_job(): an explicit remove of a TERMINAL row now really
      // removes it.
      let r;
      try {
        r = await fetch('/api/jobs/' + j.id, {method: 'DELETE'});
      } catch (e) {
        alert('Remove failed: ' + e);
        return;
      }
      if (!r.ok) {
        alert('Could not remove "' + j.label + '":\n' + (await r.text()));
        return;
      }
      let body = {};
      try { body = await r.json(); } catch (e) { /* non-JSON success: treat as removed */ }
      if (body.cancelled_in_place) {
        alert('"' + j.label + '" was kept in the queue rather than removed: it still '
              + 'carries resumable state (was ' + body.previous_status + '). It is now '
              + 'recorded terminal in place.');
      }
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
    return tr;
  };

  // --- plan rollup ---------------------------------------------------------
  // Group the (already plan-ordered) rows into one entry per slice-PLAN. The
  // grouping key and the ordering are BOTH server-side decisions: group_key comes
  // from ollama-queue.py's job_group_key, and plan_bundle is what _annotate_plan_rollup
  // (self-tested) calls a real plan -- a genuine plan-of-one stays flat, exactly as
  // before. plan_bundle, NOT plan_size: the row count is transient (an auto-author row
  // is reaped the moment the coding row it enqueued appears), the plan's slice count is
  // not, so the bundle no longer collapses to a floating flat row mid-handover.
  const planEntries = [];
  const planByKey = {};
  for (const j of displayJobs) {
    const key = ((j.plan_bundle || j.bundle) && j.group_key) ? j.group_key : null;
    if (!key) { planEntries.push({key: null, children: [j]}); continue; }
    let g = planByKey[key];
    if (!g) { g = {key: key, children: []}; planByKey[key] = g; planEntries.push(g); }
    g.children.push(j);
  }
  // Running work always leads (the user 2026-10-01: "why isn't the running task on top?").
  // The server's plan_seq can rank a bundle whose slices are merely PLANNED ahead of the
  // bundle that is actually executing. Array.sort is stable, so every other bundle keeps
  // the server's order; the running bundle is also the real head of the run priority.
  // Stale bundles (nothing running/pending/paused/held: every child failed or finished)
  // sink BELOW the live ones so old failures never sit at the same level as live work.
  const planRank = e => !e.key ? 1 : e.children.some(c => c.status === 'running') ? 0
    : e.children.some(c => ['pending', 'paused', 'held', 'planned', 'queued'].includes(c.status)) ? 1 : 2;
  planEntries.sort((a, b) => planRank(a) - planRank(b));
  // --- bundle ORDER controls -----------------------------------------------
  // Under depth-first scheduling the order of the bundles IS the run priority, so
  // this is the control that matters (2026-09-18, the user: arrange which bundle runs
  // next). Only whole bundles move; slice order inside a bundle is sequential for a
  // reason and is never exposed here -- move_group preserves it server-side.
  // Movable = the bundle actually has a PENDING slice, which is exactly what
  // ollama-queue.py's move_group relocates (it refuses a group with none, and
  // running/terminal members never move). Same enable rule as the per-row buttons.
  const firstPendingOf = g => (g.children.find(c => c.status === 'pending') || {}).id;
  const movablePlans = planEntries.filter(e => e.key && firstPendingOf(e));
  async function moveBundle(g, beforeGroup) {
    // before_id null => send this bundle to the END of the pending region, the same
    // convention the per-row move endpoint uses.
    const res = await fetch('/api/jobs/move-group', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({id: firstPendingOf(g),
                            before_id: beforeGroup ? firstPendingOf(beforeGroup) : null})});
    if (!res.ok) alert('move bundle failed: ' + (await res.text()));
    else showTruth(await truthOf(res));
    refresh();
  }
  // DASHBOARD B: one line per slice, with its LIVE phase; click a line for its run
  // history (live queue rows keep every action; finished runs are read-only lines,
  // click one for its log). One row per slice; open only for failed/escalated
  // ones by default; a user toggle wins and is remembered.
  // Rows UNDER a slice line (author/gate/refine/... runs, stage headers, attempt
  // folds) sit one level deeper than the slice line itself (1.8rem, tr.queue-child)
  // so they read as part of that slice, not as siblings (the user 2026-10-03).
  const SLICE_KID_N = 3.2, SLICE_KID = SLICE_KID_N + 'rem';
  // ...and an attempt's runs one level deeper again, under their attempt row.
  const ATTEMPT_KID_N = 4.6;
  const renderSliceLines = (g, view) => {
    const claimed = new Set();
    for (const sl of view.slices) {
      const skey = g.key + '/' + sl.sid;
      const open = (skey in sliceExpanded) ? sliceExpanded[skey]
        : sliceOpenByDefault(sl);
      const hdr = sliceHeader(sl, g.key);
      // The left-gutter caret is the ONLY disclosure arrow (down = open, sideways =
      // closed); the status marker is never an arrow, so it cannot read as a toggle.
      const mark = sl.phase === 'done' ? '&#10003;' : sl.attention ? '&#9888;'
        : sl.active ? '<span class="slice-dot" title="running">&#9679;</span>' : '&#9675;';
      const str = document.createElement('tr');
      str.className = 'queue-child slice-line' + (sl.phase === 'done' ? ' queue-done' : '');
      str.style.cursor = 'pointer';
      str.dataset.open = open ? '1' : '0';
      const sDetail = (sl.detail || '').replace(/^refining \(round \d+\)( -- )?/, '');
      str.innerHTML = `
        <td class="slice-caret">${sl.history && sl.history.length ? (open ? '&#9662;' : '&#9656;') : ''}</td>
        <td title="${escapeHtml(hdr.tip)}">${mark} ${escapeHtml(hdr.num)}${hdr.entry ? ' <span style="opacity:.6">&middot; ' + escapeHtml(hdr.entry) + '</span>' : ''}<span class="chip m-chip ph-${sl.phase}">${sl.phase}</span>${sDetail ? '<div class="sub">' + escapeHtml(sDetail) + '</div>' : ''}</td>
        <td class="st"><span class="chip ph ph-${sl.phase}">${sl.phase}</span></td>
        <td class="when">${escapeHtml(sliceSuffix(sl))}</td>
        <td></td>`;
      str.addEventListener('click', () => {
        sliceExpanded[skey] = !open; saveExpandState(); refresh();
      });
      tbody.appendChild(str);
      if (!open) {
        for (const h of sl.history || []) if (h.live && h.id) claimed.add(h.id);
        continue;
      }
      // One history entry as a row: a live queue row (all actions kept) or a read-only line.
      const histRow = (h, indent) => {
        const live = h.live && g.children.find(c => c.id === h.id);
        if (live) {
          claimed.add(h.id);
          const r = buildQueueRow(live, true);
          r.classList.add('slice-live');
          const lc = r.children[1];
          if (lc && lc.firstChild && lc.firstChild.nodeType === 3) {
            r.title = live.label || '';
            lc.firstChild.textContent = stageLabel(h.kind, live.label);
            const rr = lc.querySelector('.rerun');
            if (rr) rr.remove();      // the "#N" rerun counter is noise under a slice
          }
          return r;
        }
        const htr = document.createElement('tr');
        htr.className = 'queue-child slice-hist';
        htr.title = h.label || '';
        if (h.id) { htr.style.cursor = 'pointer';
          htr.addEventListener('click', () => openLivelog(h.id, h.label || h.id)); }
        const when = h.start ? new Date(h.start * 1000).toLocaleTimeString() : '';
        htr.innerHTML = `
          <td></td>
          <td${indent ? ' style="padding-left:5.2rem"' : ''}>${escapeHtml(stageLabel(h.kind, h.label))}${h.id ? ' <span style="opacity:.5">' + h.id + '</span>' : ''}${h.result ? '<div class="sub">' + escapeHtml(h.result) + '</div>' : ''}</td>
          <td class="st"><span class="chip status-${escapeHtml(h.status || '')} ${escapeHtml(h.status || '')}">${escapeHtml(h.status || '')}</span></td>
          <td class="when" title="${escapeHtml(when)}">${h.duration_s != null ? formatElapsed(h.duration_s) : ''}</td>
          <td></td>`;
        return htr;
      };
      // A collapsible summary line (an earlier attempt, or earlier refine rounds).
      const foldRow = (key, text, isOpen, cls) => {
        const fr = document.createElement('tr');
        fr.className = 'queue-child ' + cls;
        fr.style.cursor = 'pointer';
        fr.dataset.open = isOpen ? '1' : '0';
        fr.innerHTML = `<td class="slice-caret">${isOpen ? '&#9662;' : '&#9656;'}</td><td colspan="3" style="color:var(--muted);padding-left:${SLICE_KID}">${escapeHtml(text)}</td><td></td>`;
        fr.addEventListener('click', () => { sliceExpanded[key] = !isOpen; saveExpandState(); refresh(); });
        return fr;
      };
      // Rows of one attempt, with earlier refine rounds folded; `prefix` keys the fold.
      // `base` (rem) is this attempt's run/stage-row indent: SLICE_KID when the slice
      // has one attempt, one level deeper (under its attempt row) when it has several.
      const renderAttemptRows = (entries, prefix, base) => {
        const b0 = base + 'rem', b1 = (base + 1) + 'rem', b2 = (base + 2) + 'rem';
        // Group by pipeline stage (author -> preflight -> code -> gates -> escalation),
        // each under a small header; order inside a stage stays chronological.
        const stageOf = h => { const k = String(h.kind || '');
          if (k === 'authoring') return [1, 'Author'];
          if (k === 'preflight') return [2, 'Preflight'];
          if (k === 'coding' || k.startsWith('refining')) return [3, 'Code + refine'];
          if (k === 'gate') return [4, 'Pregate'];
          if (k === 'regate') return [5, 'Regate'];
          if (k === 'landed') return [8, 'Accepted / landed'];
          if (k === 'second-opinion') return [6, 'Second opinion'];
          return [7, 'Escalation / heal']; };
        const items = foldRounds(entries).map((it, i) =>
          ({it, i, st: stageOf(it.fold ? it.fold[0] : it.h)}));
        items.sort((a, b) => (a.st[0] - b.st[0]) || (a.i - b.i));
        // Each stage is a collapsible sub-row of the slice (caret like slice-in-bundle):
        // open by default only when it holds live work, a failure, or is the last stage.
        const stages = [];
        for (const e of items) {
          const last = stages[stages.length - 1];
          if (last && last.st[0] === e.st[0]) last.list.push(e.it);
          else stages.push({st: e.st, list: [e.it]});
        }
        const hOf = it => it.fold ? it.fold : [it.h];
        stages.forEach((stg, si) => {
          const hs = stg.list.flatMap(hOf);
          const live = hs.some(h => h.live && ['running', 'pending', 'queued', 'held', 'paused'].includes(h.status));
          const bad = hs.some(h => /fail|error|escalat|needs_opus|NO-GO|CONCERNS|REJECT/i.test(String(h.status || '') + ' ' + String(h.result || '')));
          if (hs.length === 1 && !stg.list[0].fold) {
            // a single run needs no header: its own row already names the stage
            const r1 = histRow(stg.list[0].h, false);
            if (r1.children[1]) r1.children[1].style.paddingLeft = b0;
            tbody.appendChild(r1);
            return;
          }
          const stk = prefix + '#stage' + stg.st[0];
          const so = (stk in sliceExpanded) ? sliceExpanded[stk] : (live || bad || si === stages.length - 1);
          const lastH = hs[hs.length - 1] || {};
          const sum = (hs.length > 1 ? hs.length + ' runs' : '1 run') + (lastH.result ? ' \u00b7 ' + lastH.result : (lastH.status ? ' \u00b7 ' + lastH.status : ''));
          const sh = document.createElement('tr');
          sh.className = 'queue-child slice-stage';
          sh.style.cursor = 'pointer';
          sh.innerHTML = `<td></td><td colspan="3" style="padding-left:${b0}"><span class="slice-caret" style="display:inline-block;width:1em">${so ? '&#9662;' : '&#9656;'}</span><span class="stage-h">${stg.st[0]} &middot; ${stg.st[1]}</span> <span style="opacity:.6">&mdash; ${escapeHtml(sum)}</span></td><td></td>`;
          sh.addEventListener('click', () => { sliceExpanded[stk] = !so; saveExpandState(); refresh(); });
          tbody.appendChild(sh);
          const startLen = tbody.children.length;
          for (const it of stg.list) {
            if (!it.fold) { tbody.appendChild(histRow(it.h, false)); continue; }
            const fk = prefix + '#rounds';
            const fo = (fk in sliceExpanded) ? sliceExpanded[fk] : false;
            tbody.appendChild(foldRow(fk, it.label, fo, 'slice-fold'));
            if (fo) for (const h of it.fold) tbody.appendChild(histRow(h, true));
            else for (const h of it.fold) if (h.live && h.id) claimed.add(h.id);
          }
          const added = Array.from(tbody.children).slice(startLen - tbody.children.length);
          for (const r of added) {
            const c = r.children[1];
            if (!c) continue;
            // nested one level under the stage header: fold rows move their caret
            // out of the gutter into the indented cell so everything lines up
            if (r.classList.contains('slice-fold') && r.children[0]) {
              const g0 = r.children[0];
              c.innerHTML = '<span style="display:inline-block;width:1em">' + g0.innerHTML + '</span>' + c.innerHTML;
              g0.innerHTML = '';
            }
            c.style.paddingLeft = r.classList.contains('slice-fold') ? b1 : b2;
          }
          if (!so) for (const r of added) r.remove();
        });
      };
      // Attempts OLDEST FIRST (attempt 1, 2, ...), each a row one level under the
      // slice line, its runs one level under that. The CURRENT (last) attempt is an
      // always-open divider; every EARLIER one is a summary fold, closed by default
      // (a user toggle still wins). A single attempt keeps no attempt row.
      const ga = groupAttempts(sl.history || []);
      const cur = ga.nums[ga.nums.length - 1];
      if (cur === undefined) continue;
      if (ga.nums.length === 1) { renderAttemptRows(ga.by[cur], skey + '#a' + cur, SLICE_KID_N); continue; }
      for (const n of ga.nums) {
        const ak = skey + '#a' + n;
        if (n === cur) {
          const dv = document.createElement('tr');
          dv.className = 'queue-child slice-attempt';
          dv.innerHTML = `<td></td><td colspan="3" style="color:var(--muted);font-size:.85em;padding-top:6px;padding-left:${SLICE_KID}">&#9472;&#9472; attempt ${cur} of ${cur} &#9472;&#9472;</td><td></td>`;
          tbody.appendChild(dv);
          renderAttemptRows(ga.by[n], ak, ATTEMPT_KID_N);
          continue;
        }
        const ao = (ak in sliceExpanded) ? sliceExpanded[ak] : false;
        const fr = foldRow(ak, attemptSummary(n, ga.by[n]), ao, 'slice-attempt-fold');
        // caret into the indented cell, like a stage header, so the row reads nested
        if (fr.children[0] && fr.children[1]) {
          fr.children[1].innerHTML = '<span style="display:inline-block;width:1em">' + fr.children[0].innerHTML + '</span>' + fr.children[1].innerHTML;
          fr.children[0].innerHTML = '';
        }
        tbody.appendChild(fr);
        if (ao) renderAttemptRows(ga.by[n], ak, ATTEMPT_KID_N);
        else for (const h of ga.by[n]) if (h.live && h.id) claimed.add(h.id);
      }
    }
    // live rows no slice claimed (an unmapped gate, a hand-enqueued row) still render
    for (const c of g.children) if (!claimed.has(c.id)) tbody.appendChild(buildQueueRow(c, true));
  };
  for (const g of planEntries) {
    // Render as a BUNDLE whenever the group is keyed. A key is set only when the row's
    // plan_bundle (a genuine multi-slice plan), so a group with a SINGLE live child --
    // the normal mid-chain shape, where the earlier slices are done (pruned to the
    // synthesized plan_done_slices below) and only the current author/coding slice is
    // live -- still nests under its bundle instead of floating as a lone "one-off" row
    // at the bottom (the user 2026-09-18). A keyless standalone row still renders flat.
    if (!g.key) {
      for (const c of g.children) {
        // A standalone job that failed or is parked goes to Needs attention, with its
        // reason on the row; everything else is ordinary queue work.
        const kind = jobAttentionKind(c);
        if (kind) attnStats[kind]++; else activeJobs++;
        tbody = kind ? attnBody : activeBody;
        const r = buildQueueRow(c, false);
        if (kind) r.classList.add('needs-attn');
        tbody.appendChild(r);
      }
      tbody = activeBody;
      continue;
    }
    // --- plan fields, read across the WHOLE bundle, never off children[0] --------
    // Every child of a bundle is stamped with the same plan_* fields by
    // _annotate_plan_rollup, so children[0] USED to be as good as any. It is not a
    // property the client can rely on: displayJobs admits terminal rows on the
    // plan_incomplete escape hatch, a row can be stamped by a different pass (or by
    // a coarser group_key than the sub-plan it really belongs to), and the very first
    // child is then the one row carrying an empty plan_done_slices while its siblings
    // carry the real list -- the finished slices render NOWHERE and the bundle looks
    // like nothing ever completed. So the bundle takes the UNION of what its children
    // report, and the widest X/Y any of them knows. A bundle can under-report only if
    // EVERY child is blind, never because the first one happened to be.
    const planDoneSlices = (() => {
      const seen = new Set(); let best = [];
      for (const c of g.children) {
        const l = c.plan_done_slices || [];
        if (l.length > best.length) best = l;      // longest list keeps plan ORDER
      }
      const out = [];
      for (const d of best) { seen.add(d.id); out.push(d); }
      for (const c of g.children) for (const d of (c.plan_done_slices || [])) {
        if (seen.has(d.id)) continue;
        seen.add(d.id); out.push(d);
      }
      return out;
    })();
    const planMax = f => g.children.reduce((m, c) => Math.max(m, c[f] || 0), 0);
    const planFirst = f => (g.children.find(c => c[f] != null) || {})[f];
    const counts = {}, alertCounts = {};
    for (const c of g.children) {
      counts[c.status] = (counts[c.status] || 0) + 1;
      // needs_attention is the SERVER's judgement (is this row's slice unresolved),
      // never re-derived from the status here -- only it can see the plan run state.
      if (c.needs_attention) alertCounts[c.status] = (alertCounts[c.status] || 0) + 1;
    }
    // Anything that did not succeed leaves the faint breakdown and becomes its own
    // badge -- see planAlertSummary. `parts` is what is LEFT, i.e. routine only.
    const alertSummary = planAlertSummary(counts, alertCounts);
    // Slice-level truth: a failed/escalated SLICE badges the header even when no queue
    // row of the bundle is failed (the counts above are queue ROWS, not slices).
    const badView = (bundleViews.views || {})[g.key] || null;   // NOT `view`: declared further down (TDZ)
    const badSlices = badView ? badView.slices.filter(x => x.attention).length : 0;
    const sliceBadge = badSlices && !alertSummary.badge
      ? `<span class="plan-alert" title="slices that failed or escalated">&#9888; ${badSlices} slice${badSlices === 1 ? '' : 's'} failed</span> ` : '';
    const parts = alertSummary.rest;
    const active = !!(counts.running || counts.pending || counts.paused);
    // Default OPEN only for a plan with a RUNNING slice -- what is happening right
    // now must never be hidden behind a collapsed row. Everything else starts
    // collapsed, which is what turns ~159 rows into ~28 lines.
    //
    // Deliberately NOT "open if pending too": the slicer gives EVERY plan exactly one
    // pending head slice (measured live 2026-09-18: 25 of 25 plans), so that rule
    // would re-expand the entire queue and roll nothing up at all. Nothing is hidden
    // by this: the parent line always spells out its own pending/planned breakdown,
    // and one click opens it. A user toggle always wins and survives the 5s poll.
    const view = (bundleViews.views || {})[g.key] || null;
    const expanded = (g.key in planExpanded) ? planExpanded[g.key]
      : !!(counts.running || (view && g.key === bundleViews.active && view.current));
    const pending = g.children.filter(c => c.status === 'pending');
    // Includes the RUNNING slice: cancelling a bundle must stop it, not leave it to
    // finish and advance the plan (the user 2026-10-01). Running ones go via ?force=1.
    const cancellable = g.children;
    // A paused/held bundle has NO pending children, so it drops out of movablePlans
    // (mpos=-1) and every reorder arrow disappears -- leaving only "cancel" (the user
    // 2026-09-18: "there should be a resume button on these so i can reposition them").
    // Resume flips each paused/held child back to pending; the arrows reappear on the
    // next poll, so the bundle can then be moved like any other.
    const resumable = g.children.filter(c => c.status === 'paused' || c.status === 'held');
    // Pause the WHOLE bundle in one action: gracefully stop its running slice
    // (SIGTERM, state saved) AND hold its pending slices, so the bundle fully
    // vacates the GPU and another bundle can run (the user 2026-09-18: "need a way to
    // pause that bundle"). The header "hold" only parks pending slices, which
    // leaves the running one holding the single GPU slot -- this covers that gap.
    const running = g.children.filter(c => c.status === 'running');
    // Where this bundle sits among the MOVABLE bundles -- the up/down arrows move it
    // one place in that list. Arrows rather than drag on purpose: a parent row is
    // also the expand/collapse target, and a drag that starts on it fights the
    // toggle. The per-row arrows use exactly this index arithmetic (before the
    // previous one / before the one two later, i.e. after the next one).
    const mpos = movablePlans.indexOf(g);
    // --- where this bundle belongs: Needs attention or the Queue -----------------
    // Stuck = nothing of it is running or waiting to run. Stuck AND carrying an
    // unresolved failure => "failed"; stuck because it was held/paused => "parked";
    // only blocked rows => "blocked". A bundle that is still MOVING keeps its failure
    // as a quiet amber note in the Queue: a retry in flight is not an emergency.
    const liveNow = !!(counts.running || counts.pending || counts.queued);
    const hasAlert = alertSummary.alerts.length > 0 || badSlices > 0;
    const attnKind = (!liveNow && (counts.held || counts.paused)) ? 'parked'
      : (!liveNow && hasAlert) ? 'failed'
      : (!liveNow && counts.blocked) ? 'blocked' : null;
    const attnInfo = attnKind ? bundleAttention(g, badView, attnKind) : null;
    // Button order (the user 2026-09-18): upup, up, down, downdown, hold, cancel -- kept,
    // now as one overflow menu with words beside the arrows.
    let pprimary = '';
    const pitems = [];
    if (pending.length) pitems.push(`<button class="mi" data-plan-promote title="Run this bundle FIRST (send &quot;${g.key}&quot;'s ${pending.length} pending slices to the front of the queue, keeping slice order)">&uarr;&uarr; Run this bundle next</button>`);
    if (mpos > 0) pitems.push(`<button class="mi" data-plan-up title="Run this bundle one place EARLIER (before &quot;${movablePlans[mpos - 1].key}&quot;) -- slice order inside the bundle is untouched">&uarr; Move earlier</button>`);
    if (mpos >= 0 && mpos < movablePlans.length - 1) pitems.push(`<button class="mi" data-plan-down title="Run this bundle one place LATER (after &quot;${movablePlans[mpos + 1].key}&quot;) -- slice order inside the bundle is untouched">&darr; Move later</button>`);
    if (mpos >= 0 && mpos < movablePlans.length - 1) pitems.push(`<button class="mi" data-plan-last title="Run this bundle LAST (send it to the end of the queue)">&darr;&darr; Move to end</button>`);
    if (pitems.length) pitems.push('<hr>');
    if (running.length || pending.length) pitems.push(`<button class="mi" data-plan-pause title="Pause the WHOLE bundle: gracefully stop its running slice (state saved, resumable) and hold its ${pending.length} pending slice(s), so it vacates the GPU and another bundle can run">&#10074;&#10074; Pause bundle</button>`);
    if (pending.length) pitems.push(`<button class="mi" data-plan-hold title="Hold every pending slice of this plan (parks them out of execution; resume per row)">Hold pending slices (${pending.length})</button>`);
    const resumeBtnHtml = cls => `<button class="${cls}" data-plan-resume title="Resume every paused/held slice of this plan (${resumable.length}) -- they become pending again, and the reorder arrows come back so you can reposition the bundle">Resume${cls === 'mi' ? ' (' + resumable.length + ')' : ''}</button>`;
    if (resumable.length && attnKind === 'parked') pprimary = resumeBtnHtml('btn primary');
    else if (resumable.length) pitems.push(resumeBtnHtml('mi'));
    if (attnInfo && attnInfo.logId) {
      const lb = `data-plan-log="${escapeHtml(attnInfo.logId)}" data-log-label="${escapeHtml(attnInfo.logLabel || attnInfo.logId)}"`;
      if (!pprimary) pprimary = `<button class="btn primary" ${lb}>View log</button>`;
      else pitems.push(`<button class="mi" ${lb}>View log of the failed run</button>`);
    }
    if (cancellable.length) pitems.push(`<hr><button class="mi danger" data-plan-cancel title="Cancel every slice of this plan (a running one is stopped)">Cancel bundle&hellip;</button>`);
    while (pitems.length && pitems[pitems.length - 1] === '<hr>') pitems.pop();
    const pacts = pprimary + menuHtml(pitems, 'plan:' + g.key);
    // X/Y = how far the WHOLE plan has got: slices already enqueued/done over the
    // plan's total slice count, read server-side from the slicer's own run state
    // (~/.ollama-dispatch/slice-runs/<plan>.json), NOT from the live rows -- slices
    // whose queue rows were reaped still count. See _plan_progress.
    // It is the SAME (X, Y) the run-status panel's "N/M slices done" shows: both come
    // from _plan_progress_recursive via _load_plan_progress, so the two panels can
    // never quote different sizes for one bundle.
    // Dashboard B: the header's N/M and "what is happening" come from the LIVE view
    // (bundle_view: gate verdicts + running jobs + off-GPU progress), not only the
    // slicer's run file, which lags until its driver next polls.
    const total = view && view.total ? view.total : planMax('plan_total');
    const fracTitle = 'Plan slices through / TOTAL leaf slices, expanded through every '
      + 'sub-split to any depth (the slicer\'s own state, ~/.ollama-dispatch/slice-runs). '
      + 'The same number the run-status panel shows. The breakdown after it counts LIVE '
      + 'QUEUE ROWS, which is a different thing: a sub-slice not enqueued yet has no row, '
      + 'and a finished slice\'s row is reaped.';
    const through = view && view.total ? view.through : planMax('plan_done');
    const unitName = (view && view.unit) || 'slices';
    const frac = total ? `<span class="plan-frac" title="${fracTitle}">${through}/${total} ${unitName}</span> ` : '';
    // The parent shows its MOST ACTIVE child's status, decided server-side by
    // _group_waiting_by_plan (running > pending > paused/held > planned) and stamped
    // as plan_status -- NOT a generic "active", which made a queue with a slice
    // actually running read as all-pending (2026-09-18, the user: "whatever job is
    // actively being worked should show running"). Same status-* class as every
    // other row, so a running plan is green/bold exactly like a running job.
    const leadStatus = planFirst('plan_status') || (active ? 'pending' : 'planned');
    const leadSlice = planFirst('plan_lead');
    // Name the slice that IS running, so "which one is being worked" needs no click.
    const curSlice = view && view.current ? view.slices.find(x => x.sid === view.current) : null;
    const lead = curSlice
      ? `<span class="plan-lead">&#9654; ${escapeHtml(curSlice.job ? curSlice.title : curSlice.sid)}: <span class="ph ph-${curSlice.phase}">${curSlice.phase}</span>${curSlice.elapsed_s != null ? ' ' + formatElapsed(curSlice.elapsed_s) : ''}</span> `
      : (leadStatus === 'running' && leadSlice)
      ? `<span class="plan-lead">&#9654; running: ${leadSlice}</span> ` : '';
    // The status breakdown alone once there is an X/Y -- the fraction's Y already IS
    // the slice total, so a leading "N slices ·" just repeats it (2026-09-18, the user).
    // Without a fraction (no readable plan state) the count is the only size cue
    // there is, so it stays.
    // ...and the breakdown is explicitly labelled "queue rows" so "1/8 slices" next to
    // "5 planned" reads as two different measurements instead of a contradiction
    // (the user 2026-09-18, the Ollama Queue panel on bg-eraser).
    const summary = (total ? '' : `${g.children.length} slices &middot; `)
      + (total && parts.length ? 'queue rows: ' : '') + parts.join(' &middot; ');
    const chipTxt = attnKind || leadStatus;
    // The waiting bundle says WHY it waits (its head row's server-side wait_reason).
    const headWait = (g.children.find(c => c.wait_reason && ['pending', 'planned', 'queued'].includes(c.status)) || {}).wait_reason;
    const ptr = document.createElement('tr');
    ptr.className = 'queue-parent' + alertSummary.rowClass + (attnKind ? ' needs-attn' : '');
    ptr.innerHTML = `
      <td>${mpos >= 0 ? `<span class="bundle-grip" draggable="true" title="Drag to reorder this bundle in the queue" style="cursor:grab;margin-right:4px;opacity:0.55">&#10303;</span>` : ''}${expanded ? '&#9662;' : '&#9656;'}</td>
      <td><span class="proj-name">${escapeHtml(g.key)}</span><span class="chip m-chip ${chipTxt}">${chipTxt}</span>
          <div class="sub">${total ? progHtml(through, total, unitName, fracTitle) : ''}${lead}${alertSummary.badge}${sliceBadge}</div>
          ${summary ? `<div class="sub proj-summary">${summary}</div>` : ''}
          ${attnInfo && attnInfo.reason ? `<div class="reason${attnKind === 'failed' ? ' bad' : ''}">${escapeHtml(attnInfo.reason)}</div>`
            : (headWait && !counts.running ? `<span class="wait-reason">${escapeHtml(headWait)}</span>` : '')}</td>
      <td class="st"><span class="chip status-${chipTxt} ${chipTxt}">${chipTxt}</span></td>
      <td class="when">${curSlice && curSlice.elapsed_s != null ? formatElapsed(curSlice.elapsed_s) : ''}</td>
      <td class="acts">${pacts}</td>`;
    ptr.addEventListener('click', e => {
      // The grip is for dragging, not toggling; a click on it must not expand/collapse.
      if (e.target.closest('button, details, a') || e.target.classList.contains('bundle-grip')) return;
      planExpanded[g.key] = !expanded;
      saveExpandState();
      refresh();
    });
    // Bundle drag-reorder: drag starts ONLY from the grip (so it doesn't fight the
    // row's expand/collapse toggle); any movable parent row is a drop target. Reuses
    // the same drop-indicator CSS and the move-group endpoint the arrows already use.
    const grip = ptr.querySelector('.bundle-grip');
    if (grip) {
      grip.addEventListener('click', e => e.stopPropagation());
      grip.addEventListener('dragstart', e => {
        dragBundleKey = g.key; dragStartedAt = Date.now();
        ptr.classList.add('dragging'); e.stopPropagation();
      });
      grip.addEventListener('dragend', () => {
        ptr.classList.remove('dragging');
        document.querySelectorAll('.drop-indicator-above, .drop-indicator-below').forEach(el =>
          el.classList.remove('drop-indicator-above', 'drop-indicator-below'));
        dragBundleKey = null; dragStartedAt = null; refresh();
      });
    }
    if (mpos >= 0) {
      ptr.addEventListener('dragover', e => {
        if (!dragBundleKey || dragBundleKey === g.key) return;  // ignore job drags + self
        e.preventDefault();
        document.querySelectorAll('.drop-indicator-above, .drop-indicator-below').forEach(el =>
          el.classList.remove('drop-indicator-above', 'drop-indicator-below'));
        const rect = ptr.getBoundingClientRect();
        const before = (e.clientY - rect.top) < rect.height / 2;
        ptr.classList.add(before ? 'drop-indicator-above' : 'drop-indicator-below');
        ptr.dataset.dropBefore = before ? '1' : '0';
      });
      ptr.addEventListener('drop', e => {
        if (!dragBundleKey || dragBundleKey === g.key) return;
        e.preventDefault();
        const before = ptr.dataset.dropBefore === '1';
        // ABOVE -> land right before this bundle; BELOW -> before the NEXT movable bundle
        // (null => end of the pending region, the move-group "send to end" convention).
        const tpos = movablePlans.indexOf(g);
        const beforeGroup = before ? g : (movablePlans[tpos + 1] || null);
        const dragged = movablePlans.find(e2 => e2.key === dragBundleKey);
        dragBundleKey = null;
        document.querySelectorAll('.drop-indicator-above, .drop-indicator-below').forEach(el =>
          el.classList.remove('drop-indicator-above', 'drop-indicator-below'));
        if (dragged && dragged !== beforeGroup) moveBundle(dragged, beforeGroup);
      });
    }
    const upBundleBtn = ptr.querySelector('[data-plan-up]');
    if (upBundleBtn) upBundleBtn.addEventListener('click', () => moveBundle(g, movablePlans[mpos - 1]));
    const downBundleBtn = ptr.querySelector('[data-plan-down]');
    // Moving DOWN one place = sit before the bundle two later; if there is none,
    // this bundle is becoming the last, which is before_id null (send to the end).
    if (downBundleBtn) downBundleBtn.addEventListener('click', () => moveBundle(g, movablePlans[mpos + 2]));
    const lastBundleBtn = ptr.querySelector('[data-plan-last]');
    if (lastBundleBtn) lastBundleBtn.addEventListener('click', () => moveBundle(g, null));
    const promoteBtn = ptr.querySelector('[data-plan-promote]');
    if (promoteBtn) promoteBtn.addEventListener('click', async () => {
      // Reuses the EXISTING server-side group route: it resolves the plan from any
      // member id and moves every pending member as one ordered block, atomically.
      // ↑↑ = "this is the NEXT bundle that launches" (the user 2026-09-27), even over a
      // committed or pinned bundle: take_focus writes the human focus override. It
      // pauses NOTHING -- a running job finishes first, and a committed bundle
      // yields and resumes right after.
      const res = await fetch('/api/jobs/' + pending[0].id + '/promote-group', {method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({take_focus: true})});
      if (!res.ok) alert('promote plan failed: ' + (await res.text()));
      else showTruth(await truthOf(res));
      refresh();
    });
    const holdBtn = ptr.querySelector('[data-plan-hold]');
    if (holdBtn) holdBtn.addEventListener('click', async () => {
      // No group-level hold exists in ollama-queue.py to delegate to, so this fans
      // out over the plan's PENDING slices using the per-job hold endpoint (planned
      // placeholders have no state row to hold, and hold_job refuses a running one).
      if (!confirm(`Hold ${pending.length} pending slice(s) of "${g.key}"? They stay parked until resumed.`)) return;
      for (const c of pending) {
        const r = await fetch('/api/jobs/' + c.id + '/hold', {method: 'POST'});
        if (!r.ok) { alert('hold failed for ' + c.label + ': ' + (await r.text())); break; }
      }
      refresh();
    });
    const pauseBtn = ptr.querySelector('[data-plan-pause]');
    if (pauseBtn) pauseBtn.addEventListener('click', async () => {
      // Park the whole bundle: SIGTERM the running slice(s) via the same per-job
      // /kill the row-level pause uses (worker saves its transcript, exit 3, fully
      // resumable), then /hold the pending slices. Result: the bundle holds no GPU
      // and no queue slot, so the next incomplete bundle becomes active. Resume via
      // the header "resume" button (running slice comes back as paused->pending).
      const n = running.length + pending.length;
      if (!confirm(`Pause the whole "${g.key}" bundle? Gracefully stops ${running.length} running + holds ${pending.length} pending slice(s) (${n} total). Work is saved; resume from the header.`)) return;
      for (const c of running) {
        markTransitioning(c.id, 'pausing');
        const r = await fetch('/api/jobs/' + c.id + '/kill', {method: 'POST'});
        if (!r.ok) { alert('pause failed for ' + c.label + ': ' + (await r.text())); break; }
      }
      for (const c of pending) {
        const r = await fetch('/api/jobs/' + c.id + '/hold', {method: 'POST'});
        if (!r.ok) { alert('hold failed for ' + c.label + ': ' + (await r.text())); break; }
      }
      refresh();
    });
    const resumeBtn = ptr.querySelector('[data-plan-resume]');
    if (resumeBtn) resumeBtn.addEventListener('click', async () => {
      // Fan-out over the plan's PAUSED/HELD slices using the same per-job resume
      // endpoint the per-row resume button uses. Once they are pending again the
      // bundle re-enters movablePlans, so the reorder arrows reappear on the next
      // poll and the bundle can be repositioned in the queue (the user 2026-09-18).
      for (const c of resumable) {
        const r = await fetch('/api/jobs/' + c.id + '/resume', {method: 'POST'});
        if (!r.ok) { alert('resume failed for ' + c.label + ': ' + (await r.text())); break; }
      }
      refresh();
    });
    const cancelBtn = ptr.querySelector('[data-plan-cancel]');
    if (cancelBtn) cancelBtn.addEventListener('click', async () => {
      // Same fan-out, over the per-job DELETE. A RUNNING slice is STOPPED (?force=1 ->
      // the sanctioned graceful stop, no orphaned pid) and goes FIRST: that call marks
      // the plan cancelled before anything can advance it.
      const _running = cancellable.filter(c => c.status === 'running');
      if (!confirm(`Cancel ${cancellable.length} slice(s) of "${g.key}"?` +
          (_running.length ? ` This STOPS the ${_running.length} running slice(s) and removes the rest from the queue.`
                           : ' This removes them from the queue.'))) return;
      for (const c of [..._running, ...cancellable.filter(c => c.status !== 'running')]) {
        const r = await fetch('/api/jobs/' + c.id + (c.status === 'running' ? '?force=1' : ''), {method: 'DELETE'});
        if (!r.ok) { alert('cancel failed for ' + c.label + ': ' + (await r.text())); break; }
      }
      refresh();
    });
    const planLogBtn = ptr.querySelector('[data-plan-log]');
    if (planLogBtn) planLogBtn.addEventListener('click', () =>
      openLivelog(planLogBtn.dataset.planLog, planLogBtn.dataset.logLabel || planLogBtn.dataset.planLog));
    // Route the bundle (header + everything under it) to its section.
    if (attnKind) attnStats[attnKind]++; else activeBundles++;
    tbody = attnKind ? attnBody : activeBody;
    tbody.appendChild(ptr);
    if (expanded && view && view.slices && view.slices.length) {
      renderSliceLines(g, view);
    } else if (expanded) {
      // Finished slices FIRST: their queue rows were pruned, so the server hands
      // them back as display-only ✓ rows (plan_done_slices, already in plan order).
      // They are by definition behind everything still queued, so putting them at
      // the head of the bundle reads s1 ✓ -> s2 running -> s3 planned AND leaves the
      // real rows in exactly the display_seq order the server computed -- nothing
      // live is reordered by this. No action buttons: there is no job to act on.
      // The gate tag rides in the otherwise-empty colspan cell: 'pending gate' while
      // this slice's gate/regate row is still live, then the resolved verdict once it
      // lands. The server reads that verdict off the durable sidecar, so it KEEPS
      // showing after the gate's own queue row is pruned -- the row never goes blank
      // and never disappears (the user 2026-09-18).
      //
      // A SKIPPED slice (deliberately retired as already satisfied at the chain tip)
      // is finished business and counts toward X, so it belongs in this list -- but it
      // is drawn with its OWN mark and status text, never a tick and never a verdict.
      // Nothing ran for it, so dressing it as `done` would claim a pass that no proof
      // exists for (2026-09-19).
      for (const d of planDoneSlices.filter(d => !d.not_started)) {
        const skipped = d.status === 'skipped';
        const dtr = document.createElement('tr');
        dtr.className = 'queue-child queue-done';
        dtr.title = d.title || (skipped
          ? 'Retired: this slice was already satisfied at the chain tip, so nothing was dispatched for it. It counts as finished, not as passed.'
          : '');
        const gtag = (!skipped && d.gate_tag)
          ? `<span class="${d.raw_verdict ? verdictClass(d.raw_verdict) : 'verdict-pending'}">${d.gate_tag}</span>`
          : (skipped ? '<span class="verdict-skipped">retired &mdash; nothing dispatched</span>' : '');
        dtr.innerHTML = `
          <td></td>
          <td>${skipped ? '&#8856;' : '&#10003;'} ${d.label}${gtag ? '<div class="sub">' + gtag + '</div>' : ''}</td>
          <td class="st"><span class="chip ${skipped ? 'skipped' : 'done'}">${skipped ? 'skipped' : 'done'}</span></td>
          <td></td>
          <td></td>`;
        tbody.appendChild(dtr);
      }
      for (const c of g.children) tbody.appendChild(buildQueueRow(c, true));
      // NOT-YET-STARTED SLOTS (the user 2026-09-21: "4 slices but only three
      // showing. We should at least have a pending slot for each slice to
      // hold for anything pending"). These have no job at all yet -- still
      // `blocked` on a dependency, or `pending` before an auto-author round
      // exists -- so they render LAST (chronologically still to come), with a
      // hollow marker and no gate tag: nothing has run for them to report.
      for (const d of planDoneSlices.filter(d => d.not_started)) {
        const dtr = document.createElement('tr');
        dtr.className = 'queue-child queue-not-started';
        dtr.title = d.title || '';
        dtr.innerHTML = `
          <td></td>
          <td>&#9675; ${d.label}</td>
          <td class="st"><span class="chip ${d.status || 'planned'}">${d.status || 'planned'}</span></td>
          <td></td>
          <td></td>`;
        tbody.appendChild(dtr);
      }
    }
    tbody = activeBody;
  }
  renderStalledBundles(renderSliceLines);
  tbody = finBody;
  renderFinishedBundles(renderSliceLines);
  tbody = activeBody;
  renderSummary(jobs, acts, attnStats, {bundles: activeBundles, jobs: activeJobs});
  if (_openMenu) {
    const m = document.querySelector(`details.menu[data-menu="${CSS.escape(_openMenu)}"]`);
    if (m) m.open = true;
  }
  if (window.scrollY !== _scrollY) window.scrollTo(0, _scrollY);

  // FINISHED bundles: below the live queue, collapsed by default (one <details>),
  // grouped by the day of their last activity, newest first; any bundle with a failed
  // slice is named in the collapsed summary so it is never hidden. Open a bundle for
  // the same per-slice lines + full history the live bundles get. Read-only.
  function renderStalledBundles(renderSlices) {
    // STALLED / FAILED bundles: nothing live, but NOT finished (a slice failed/ended or
    // slices are still owed). Their own panel + count, never aged out (Penn 2026-10-06).
    const allStalled = (finishedBundles || {}).stalled || [];
    const views = allStalled.slice(0, stalledShown);
    const panel = document.getElementById('stalledPanel');
    panel.hidden = !allStalled.length;
    document.getElementById('stalledSummary').innerHTML = `<b>Stalled / failed bundles</b>
      <span class="count" style="color:var(--muted)">${views.length} of ${allStalled.length} &middot; all ages, newest first</span>
      <span class="chip bad">${allStalled.filter(v => v.actionable !== false).length} actionable</span>
      <span class="chip" style="color:var(--muted)">stale backlog: ${allStalled.filter(v => v.actionable === false).length}</span>
      ${allStalled.length > stalledShown ? '<button class="btn" data-st-more>show 25 more</button>' : ''}
      ${stalledShown > 25 ? '<button class="btn" data-st-less>show fewer</button>' : ''}`;
    const sm = document.querySelector('#stalledSummary [data-st-more]');
    if (sm) sm.addEventListener('click', ev => { ev.preventDefault(); ev.stopPropagation(); stalledShown += 25; refresh(); });
    const sf = document.querySelector('#stalledSummary [data-st-less]');
    if (sf) sf.addEventListener('click', ev => { ev.preventDefault(); ev.stopPropagation(); stalledShown = 25; refresh(); });
    const sb = stalledBody;   // renderSlices appends to the shared `tbody`, so point it here
    sb.innerHTML = '';
    tbody = sb;
    for (const view of views) {
      const fkey = 'stalled:' + view.key;
      const open = !!planExpanded[fkey];
      const bad = view.failed_slices || 0;
      const label = view.outcome === 'incomplete' ? 'incomplete' : 'failed';
      const when = view.last_activity ? new Date(view.last_activity * 1000) : null;
      const ftr = document.createElement('tr');
      ftr.className = 'queue-parent finished-bundle queue-parent-alert needs-attn';
      ftr.innerHTML = `
        <td>${open ? '&#9662;' : '&#9656;'}</td>
        <td><span class="proj-name">${escapeHtml(view.key)}</span><span class="chip m-chip failed">${label}</span>
          <div class="sub">${progHtml(view.through, view.total, view.unit || 'slices', '')}
          <span class="plan-alert">&#9888; ${bad ? bad + ' failed' : 'slices still owed, nothing running'}</span>
          <span class="proj-summary">${view.runs || 0} runs &middot; ${escapeHtml(dayLabel(view.last_activity))}</span></div></td>
        <td class="st"><span class="chip status-failed failed">${label}</span></td>
        <td class="when" title="${when ? escapeHtml(when.toLocaleString()) : ''}">${when ? when.toLocaleTimeString([], {hour: '2-digit', minute: '2-digit'}) : ''}</td><td></td>`;
      ftr.addEventListener('click', () => { planExpanded[fkey] = !open; saveExpandState(); refresh(); });
      sb.appendChild(ftr);
      if (open && view.slices && view.slices.length) renderSlices({key: view.key, children: []}, view);
    }
  }
  function renderFinishedBundles(renderSlices) {
    const fb = finishedBundles || {};
    const views = (fb.views || []).filter(x => x.finished !== false);
    const age = finishedDays ? `last ${finishedDays} days` : 'all ages';
    // The server only lists a bundle here when EVERY slice is done/landed/skipped;
    // anything failed or incomplete is in the Stalled / failed panel (never aged out).
    // Defence in depth: a view that is not `finished` is never counted as one.
    const stalledN = (fb.stalled || []).length;
    document.getElementById('finishedSummary').innerHTML = `<b>Finished bundles</b>
      <span class="count" style="color:var(--muted)">${views.length} of ${fb.total || 0} &middot; all slices done &middot; ${age}, newest first</span>
      ${stalledN ? `<span class="chip bad">${stalledN} stalled/failed (listed above, not finished)</span>` : ''}
      ${(fb.superseded || []).length ? `<span class="chip" title="replaced by a later bundle/job (qctl supersede); not counted as stalled">${fb.superseded.length} superseded</span>` : ''}`;
    const tools = document.getElementById('finishedTools');
    tools.innerHTML = `<button class="btn" data-fb-age>${finishedDays ? 'show all ages' : 'last 3 days only'}</button>
      ${fb.has_more ? '<button class="btn" data-fb-more>show more</button>' : ''}
      ${finishedLimit > 20 ? '<button class="btn" data-fb-less>show fewer</button>' : ''}`;
    const reload = () => { finishedFetchedAt = 0; refresh(); };
    tools.querySelector('[data-fb-age]').addEventListener('click', () => {
      finishedDays = finishedDays ? 0 : 3; finishedLimit = 20; reload(); });
    const more = tools.querySelector('[data-fb-more]');
    if (more) more.addEventListener('click', () => { finishedLimit += 20; reload(); });
    const less = tools.querySelector('[data-fb-less]');
    if (less) less.addEventListener('click', () => { finishedLimit = 20; reload(); });
    let lastDay = null;
    const perDay = {};
    for (const v of views) { const d = dayLabel(v.last_activity); perDay[d] = (perDay[d] || 0) + 1; }
    for (const view of views) {
      const day = dayLabel(view.last_activity);
      if (day !== lastDay) {
        lastDay = day;
        const dh = document.createElement('tr');
        dh.className = 'day-head';
        dh.innerHTML = `<td colspan="5">${escapeHtml(day)} &middot; ${perDay[day]}</td>`;
        tbody.appendChild(dh);
      }
      const fkey = 'finished:' + view.key;
      const open = !!planExpanded[fkey];
      const bad = (view.slices || []).filter(x => x.attention).length;
      const when = view.last_activity ? new Date(view.last_activity * 1000) : null;
      const ftr = document.createElement('tr');
      ftr.className = 'queue-parent finished-bundle' + (bad ? ' queue-parent-alert needs-attn' : '');
      ftr.innerHTML = `
        <td>${open ? '&#9662;' : '&#9656;'}</td>
        <td><span class="proj-name">${escapeHtml(view.key)}</span><span class="chip m-chip ${bad ? 'failed' : 'done'}">${bad ? 'failed' : 'done'}</span>
          <div class="sub">${progHtml(view.through, view.total, view.unit || 'slices', '')}
          ${bad ? `<span class="plan-alert">&#9888; ${bad} failed</span>` : ''}
          <span class="proj-summary">${view.runs || 0} runs</span></div></td>
        <td class="st"><span class="chip status-${bad ? 'failed' : 'done'} ${bad ? 'failed' : 'done'}">${bad ? 'failed' : 'done'}</span></td>
        <td class="when" title="${when ? escapeHtml(when.toLocaleString()) : ''}">${when ? when.toLocaleTimeString([], {hour: '2-digit', minute: '2-digit'}) : ''}</td><td></td>`;
      ftr.addEventListener('click', () => { planExpanded[fkey] = !open; saveExpandState(); refresh(); });
      tbody.appendChild(ftr);
      if (open && view.slices && view.slices.length) renderSlices({key: view.key, children: []}, view);
    }
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
  // COMPACT rendering (2026-09-26): this panel now shares a flex row with Web
  // Search Usage, so it has ~300-400px, not the page width. Same information as
  // the old wide table -- which models are resident on which host, and the memory
  // figure with what it measures -- just tighter: one bold host line with the
  // memory summary, models indented beneath it, wrapping instead of one-per-column.
  const fmtModels = models => models.length
    ? models.map(m => `${escapeHtml(m.name)} <span style="opacity:.6">${m.size_gb}GB</span>`).join('<br>')
    : '<em style="opacity:.6">idle</em>';
  const fmtMem = (used, total) => (total && total > 0)
    ? `${used}/${total} GB (${Math.round(100 * used / total)}%)`
    : (used || used === 0 ? `${used} GB used` : '');
  // What the mem figure actually measures, so a reader isn't misled: the local box
  // shows REAL system memory (all processes, from vm_stat) not just model
  // footprints; a remote host shows model occupancy against its configured usable
  // budget (no shell access there), and "unmeasured" when it has no budget set.
  const memKind = {system: 'real system memory', models: 'resident models (vm_stat unavailable)',
                   vram: 'resident models vs usable budget'};
  const hosts = h.hosts || [];
  const bypass = h.llama_server_bypass || {};
  let rows = '';
  if (!hosts.length) {
    rows = '<tr><td><em>no hosts configured &mdash; add one in Settings</em></td></tr>';
  }
  hosts.forEach((host, idx) => {
    if (idx) rows += '<tr><td style="border:0; height:.4rem"></td></tr>';
    const kind = memKind[host.mem_source] || '';
    const unmeasured = (host.usable_gb === null || host.usable_gb === undefined) && host.mem_source === 'vram';
    rows += `
      <tr><td style="padding:.15rem .3rem; line-height:1.3">
        <strong>${escapeHtml(host.name)}</strong>
        <span style="opacity:.6; font-size:.85em">${escapeHtml(host.url)}</span><br>
        <span style="font-size:.85em">${fmtMem(host.used_gb, host.total_gb)}${
          unmeasured ? ' <span style="opacity:.7">(no budget set &mdash; unmeasured)</span>' : ''}
          <span style="opacity:.55">${kind ? '&middot; ' + kind : ''}</span></span>
      </td></tr>
      <tr><td style="padding:.1rem .3rem .1rem 1.1rem; font-size:.85em; line-height:1.35">
        ${fmtModels(host.models)}
      </td></tr>`;
    if (host.has_bypass) {
      rows += `<tr><td style="padding:.1rem .3rem .1rem 1.1rem; font-size:.85em">
        llama-server bypass: ${bypass.up
          ? escapeHtml(bypass.model || '') + ' <span style="opacity:.6">(up)</span>'
          : '<em style="opacity:.6">down</em>'}
        </td></tr>`;
    }
  });
  document.getElementById('hosts').innerHTML =
    `<table style="width:100%; border-collapse:collapse; font-size:.8rem;"><tbody>${rows}</tbody></table>`;
}

// ---- Settings: Ollama hosts -------------------------------------------------
// Edits the SAME table ollama-worker.py's load_ollama_hosts() reads, so adding a
// host here makes it dispatchable (not just visible) and removing one takes it out
// of "Loaded right now" AND out of fit-routing. Whole-table save: one atomic write.
function escapeHtml(t) {
  return String(t == null ? '' : t).replace(/[&<>"']/g,
    c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}
function hostSettingsRow(host) {
  const tr = document.createElement('tr');
  tr.innerHTML = `
    <td style="padding:.15rem .2rem"><input class="h-name" style="width:6.5em; font-size:.8rem"
        value="${escapeHtml(host && host.name)}" placeholder="name"></td>
    <td style="padding:.15rem .2rem"><input class="h-url" style="width:100%; min-width:9em; font-size:.8rem"
        value="${escapeHtml(host && host.url)}" placeholder="http://host:11434"></td>
    <td style="padding:.15rem .2rem"><input class="h-gb" style="width:4.5em; font-size:.8rem"
        value="${host && host.usable_gb != null ? host.usable_gb : ''}" placeholder="blank"></td>
    <td style="padding:.15rem .2rem"><button type="button" class="h-del" title="remove this host">&times;</button></td>`;
  tr.querySelector('.h-del').addEventListener('click', () => tr.remove());
  return tr;
}
async function refreshHostSettings() {
  const res = await fetch('/api/settings/hosts');
  const d = await res.json();
  document.getElementById('hostsCfgPath').textContent = d.config_path || '';
  const body = document.getElementById('hostSettingsBody');
  body.innerHTML = '';
  (d.hosts || []).forEach(host => body.appendChild(hostSettingsRow(host)));
}
document.getElementById('addHostRow').addEventListener('click', () => {
  document.getElementById('hostSettingsBody').appendChild(hostSettingsRow(null));
});
document.getElementById('saveHosts').addEventListener('click', async () => {
  const msg = document.getElementById('hostsMsg');
  const rows = [...document.querySelectorAll('#hostSettingsBody tr')].map(tr => ({
    name: tr.querySelector('.h-name').value.trim(),
    url: tr.querySelector('.h-url').value.trim(),
    usable_gb: tr.querySelector('.h-gb').value.trim() || null,
  }));
  msg.style.color = '';
  msg.textContent = 'saving...';
  try {
    const res = await fetch('/api/settings/hosts', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({hosts: rows}),
    });
    if (!res.ok) {
      msg.style.color = '#c00';
      msg.textContent = await res.text();
      return;
    }
    msg.style.color = '#0a0';
    msg.textContent = 'saved';
    setTimeout(() => { msg.textContent = ''; }, 3000);
    await refreshHostSettings();
    await refreshHosts();
  } catch (e) {
    msg.style.color = '#c00';
    msg.textContent = 'save failed: ' + e;
  }
});

// Unified run-status table: backed by /api/runs (see ollama-queue-api.py's
// _run_status_jobs), which reads the NEVER-PRUNED gate sidecars minus anything
// cleared/archived. A row lands here when qwen finishes and stays until it is
// cleared; "clear" archives its sidecars so it never comes back.
const runTbody = document.querySelector('#runStatus tbody');
function verdictClass(tag) {
  const t = (tag || '').toUpperCase();
  if (t.startsWith('PASS')) return 'verdict-pass';
  if (t.startsWith('FAIL')) return 'verdict-fail';
  if (t.startsWith('CONCERNS')) return 'verdict-concerns';
  if (t.startsWith('SKIPPED')) return 'verdict-skipped';
  if (t.startsWith('PENDING')) return 'verdict-pending';
  if (t.startsWith('BLOCKED')) return 'verdict-blocked';
  return 'verdict-unknown';
}
function fmtWhen(ts) {
  if (!ts) return '';
  const d = new Date(ts);
  return isNaN(d) ? ts : d.toLocaleString();
}
async function clearRun(id, label, awaitingSignoff) {
  let override = null;
  if (awaitingSignoff) {
    // A required sign-off is still unapproved. Clearing here must be a deliberate,
    // recorded override -- never a silent erase of the one thing the gate holds back.
    override = prompt(`"${label}" still REQUIRES sign-off and none is approved.\n\n` +
      `Clearing it now OVERRIDES that gate. Type a reason to record the override, ` +
      `or Cancel to leave it in place.`);
    if (!override) return;   // cancelled or empty reason -> do nothing
  } else {
    if (!confirm(`Clear "${label}"? It is archived (recoverable) and drops off this list.`)) return;
  }
  const res = await fetch('/api/runs/' + id + '/clear', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(override ? {override} : {})});
  if (!res.ok) {
    let msg = await res.text();
    try { msg = JSON.parse(msg).error || msg; } catch (e) {}
    alert('Could not clear: ' + msg);
  }
  refreshRuns();
}

// Persist the collapsed/expanded state of the auto-handled group across polls so a
// 5s refresh does not keep snapping it back open (or shut) under the user.
let handledExpanded = false;
// The "good to go" section (passed, nothing owed) starts OPEN -- passing work should
// be plainly visible, not folded away like something that did not matter.
let goodExpanded = true;
// Per-PROJECT expand/collapse of the new top grouping level, same persistence as
// parentExpanded below (keyed by project_key, e.g. 'bg').
let projectExpanded = {};
// Per-project expand/collapse of the sliced-job rollup, persisted across the 5s
// poll so a refresh does not snap a group the user just opened/closed. Undefined
// for a project => fall back to its default (needs-eyes parents open, so a child
// that needs eyes is visible; fully-handled parents collapsed).
let parentExpanded = {};

// ...and it survives a PAGE RELOAD too, not just the poll: the dicts above are plain
// module state, so reopening the dashboard used to snap every card back to its default
// and lose whatever the user had opened. Best-effort by design -- a private window or
// blocked site data throws, and the panel just falls back to the defaults.
const EXPAND_STATE_KEY = 'ollamaQueue.expandState.v1';
function saveExpandState() {
  try {
    localStorage.setItem(EXPAND_STATE_KEY, JSON.stringify(
      {parent: parentExpanded, project: projectExpanded, plan: planExpanded,
       slice: sliceExpanded, good: goodExpanded, handled: handledExpanded}));
  } catch (e) { /* storage unavailable -- in-memory state still works this session */ }
}
function loadExpandState() {
  try {
    const raw = localStorage.getItem(EXPAND_STATE_KEY);
    if (!raw) return;
    const s = JSON.parse(raw);
    if (s && typeof s === 'object') {
      if (s.parent && typeof s.parent === 'object') parentExpanded = s.parent;
      if (s.project && typeof s.project === 'object') projectExpanded = s.project;
      if (s.plan && typeof s.plan === 'object') planExpanded = s.plan;
      if (s.slice && typeof s.slice === 'object') sliceExpanded = s.slice;
      if (typeof s.good === 'boolean') goodExpanded = s.good;
      if (typeof s.handled === 'boolean') handledExpanded = s.handled;
    }
  } catch (e) { /* unreadable/corrupt -- defaults are correct, never block the render */ }
}
loadExpandState();
// Finished bundles start collapsed; an open/close by hand is remembered.
(() => {
  const fd = document.getElementById('finishedDetails');
  fd.open = !!planExpanded['__finishedOpen'];
  fd.addEventListener('toggle', () => { planExpanded['__finishedOpen'] = fd.open; saveExpandState(); });
})();
function renderRunRow(r, handled, isChild) {
  const tr = document.createElement('tr');
  if (isChild) tr.classList.add('run-child');
  const filesTxt = (r.changed_file_count === null || r.changed_file_count === undefined) ? '-' : r.changed_file_count;
  // For a handled row the "flags" column carries WHY it's handled -- so nothing is
  // hidden: the reason it needed no eyes is right there next to it. For a live row
  // it says WHICH kind of attention it wants, at the right volume: "awaiting
  // sign-off" (please OK this -- green) reads nothing like "needs review" (amber)
  // or "blocked -- did not run" (red). Server-computed: see _annotate_eyes_labels.
  const flags = handled
    ? `<span style="color:#888">${r.handled_reason || 'auto-handled'}</span>`
    : `<span class="eyes-badge eyes-${r.eyes_severity || 'warn'}">${r.eyes_label || 'needs review'}</span>` +
      (r.blocked_reason ? `<span class="blocked-why"> ${r.blocked_reason}</span>` : '');
  if (handled) tr.style.opacity = '.6';
  tr.innerHTML = `
    <td class="${verdictClass(r.raw_verdict)}">${r.verdict}</td>
    <td${r.display_label ? ` title="${escapeHtml(r.label)}"` : ''}>${r.display_label ? escapeHtml(r.display_label) : r.label}</td>
    <td>${r.model || ''}</td>
    <td>${r.host || ''}</td>
    <td>${filesTxt}</td>
    <td>${fmtWhen(r.timestamp)}</td>
    <td>${flags}</td>
    <td></td>`;
  const actionCell = tr.lastElementChild;
  if (r.has_gate) {
    const btn = document.createElement('button');
    btn.textContent = 'issues';
    btn.className = 'iconbtn';
    btn.onclick = () => openGateDetail(r.id, r.label);
    actionCell.appendChild(btn);
  }
  const clearBtn = document.createElement('button');
  clearBtn.textContent = 'clear';
  clearBtn.className = 'iconbtn';
  clearBtn.title = 'Handled -- archive this row out of the list (recoverable)';
  clearBtn.onclick = () => clearRun(r.id, r.label, r.awaiting_signoff);
  actionCell.appendChild(clearBtn);
  return tr;
}

// --- Run Status is a THREE-level tree (2026-09-18, the user: all the bg-* runs and
// slices should collapse under ONE "broker-guard" header, each slice a sub-group
// inside it):
//     project (bg / broker-guard)
//       > feature-slice (bg-health-s1-status)
//           > individual runs (author / refine rounds)
// The nesting AND the worst-of rollup are computed server-side (_build_run_tree /
// _summarize_rows in ollama-queue-api.py) so both header levels can never disagree
// with their children and the whole thing is unit-testable without a browser. This
// file only renders what /api/runs/tree hands it. Each header carries the same
// per-column rollup a standalone row shows -- verdict (worst-of: any FAIL makes the
// whole project FAIL, then BLOCKED, then CONCERNS, all-PASS is green), model/host
// (the agreed value or "N models"), files (summed) and when (latest child).

// What a header says about the attention its children want, at the right volume.
// An all-PASS group reads "all good to go" -- never "N needs eyes", which made a
// clean project look broken.
function rollupBadges(s) {
  const b = [];
  if (s.blocked) b.push(`<span class="eyes-badge eyes-bad">${s.blocked} blocked</span>`);
  if (s.review) b.push(`<span class="eyes-badge eyes-warn">${s.review} need${s.review === 1 ? 's' : ''} review</span>`);
  if (s.signoff) b.push(`<span class="eyes-badge eyes-ok">${s.signoff} awaiting sign-off</span>`);
  if (!b.length && s.good) {
    // A passing child in a bundle that still owes slices is NOT "good to go" -- say so
    // in amber, don't paint the whole bundle green (the user 2026-09-18: cc-waitlist read
    // "all good to go (1)" while 5 of 6 slices had not started).
    if (s.incomplete) b.push(`<span class="eyes-badge eyes-warn">${s.good} passed &middot; slices still owed</span>`);
    else b.push(`<span class="eyes-badge eyes-ok">all good to go (${s.good} runs)</span>`);
  }
  // NOT a contradiction with "N/M slices done": these badges count RUN ROWS still
  // wanting something from you, and a slice that passed and was auto-handled/archived
  // leaves none (the user 2026-09-18: "its also showing 1/2 with nothing passing").
  if (!b.length) b.push('<span style="color:#888" title="Every run under this bundle is auto-handled -- nothing here is waiting on you. The slices-done fraction counts PLAN SLICES; these badges count run rows that still want a decision.">auto-handled runs</span>');
  return b.join(' &middot; ');
}

// One collapsible header row (project OR feature-slice). cls/indent are what make
// the two levels visually distinct; everything else is identical, because a header
// is a header.
function headerRow(opts) {
  const s = opts.summary;
  const hdr = document.createElement('tr');
  hdr.className = opts.cls;
  hdr.innerHTML =
    `<td style="padding-left:${opts.indent}rem">${opts.expanded ? '▾' : '▸'}</td>` +
    `<td style="padding-left:${opts.indent}rem"><span class="proj-name">${opts.name}</span> ` +
    `<span class="${verdictClass(s.rawVerdict)}">${s.verdict}</span> ` +
    `<span class="proj-summary">${opts.summary_text}</span></td>` +
    `<td>${s.model}</td>` +
    `<td>${s.host}</td>` +
    `<td>${s.files}</td>` +
    `<td>${fmtWhen(s.when)}</td>` +
    `<td>${rollupBadges(s)}</td>` +
    `<td></td>`;
  hdr.onclick = (e) => {
    if (e.target.tagName === 'BUTTON') return;
    opts.toggle();
    saveExpandState();
    rerenderRuns();
  };
  return hdr;
}

// Bulk-clear button for a header: archives every already-handled descendant in one
// click (skips anything still awaiting sign-off -- those still refuse a plain clear).
function attachBulkClear(hdr, rows, what) {
  const clearable = rows.filter(r => r.group === 'handled' && !r.awaiting_signoff);
  if (!clearable.length) return;
  const cbtn = document.createElement('button');
  cbtn.textContent = `clear ${clearable.length} handled`;
  cbtn.className = 'iconbtn';
  cbtn.title = 'Archive every auto-handled run under this group (recoverable)';
  cbtn.onclick = async (e) => {
    e.stopPropagation();
    if (!confirm(`Clear ${clearable.length} handled run(s) of ${what}?`)) return;
    for (const r of clearable) {
      await fetch('/api/runs/' + r.id + '/clear', {
        method: 'POST', headers: {'Content-Type': 'application/json'}, body: '{}'});
    }
    refreshRuns();
  };
  hdr.lastElementChild.appendChild(cbtn);
}

// One feature/slice group (the EXISTING parent rollup) + its runs when expanded.
function renderParentGroup(g, forceCollapsedDefault, indent) {
  const s = g.summary;
  const key = g.project;
  // A card carrying SIGNAL defaults OPEN -- needs_eyes (something wants a decision)
  // and good_to_go (a real PASS) alike; only 'handled' starts folded.
  const dflt = !forceCollapsedDefault && g.section !== 'handled';
  const expanded = (key in parentExpanded) ? parentExpanded[key] : dflt;
  // "N/M slices done" is the plan's OWN recursive count (server-side
  // _plan_progress_recursive, stamped as parent_done/parent_slices) -- the exact same
  // number the queue panel's bundle fraction shows. Spelled "done" so it never reads
  // as a contradiction next to the flag badges, which count RUNS, not slices.
  const summary_text = `${s.slicesDone}/${s.total} slices done` +
    (s.runs > s.slicesDone ? ` &middot; ${s.runs} runs` : '') +
    (s.breakdown ? ` &middot; ${s.breakdown}` : '');
  const hdr = headerRow({summary: s, name: g.project, cls: 'run-parent',
    indent: indent || 0, expanded, summary_text,
    toggle: () => { parentExpanded[key] = !expanded; }});
  attachBulkClear(hdr, g.allRows || g.rows, g.project);
  runTbody.appendChild(hdr);
  if (!expanded) return;
  // ONE CLICK REVEALS EVERYTHING (the user 2026-09-19): the bundle's own runs, then every
  // sub-plan's runs inline, to any depth. A sub-plan gets a LABEL row, never a second
  // collapse toggle -- a PASS two splits down must not need a second click to find.
  for (const r of g.rows) runTbody.appendChild(renderRunRow(r, r.group === 'handled', true));
  renderSubPlanRuns(g, (indent || 0) + 1);
}

// The sub-plan divider: a non-interactive sub-heading over the runs that follow it.
// Carries the sub-plan's own X/Y and verdict so the grouping still tells you which
// slice these runs belong to and how far it got -- it just cannot hide them.
function renderSubPlanDivider(g, indent) {
  const s = g.summary;
  const tr = document.createElement('tr');
  tr.className = 'run-subplan';
  tr.innerHTML =
    `<td style="padding-left:${indent}rem"></td>` +
    `<td style="padding-left:${indent}rem"><span class="proj-name">&#8627; ${g.slice_id}</span> ` +
    `<span class="${verdictClass(s.rawVerdict)}">${s.verdict}</span> ` +
    `<span class="proj-summary">${s.slicesDone}/${s.total} slices done &middot; ` +
    `${s.runs} run${s.runs === 1 ? '' : 's'}` +
    `${s.breakdown ? ' &middot; ' + s.breakdown : ''} &middot; ${g.project}</span></td>` +
    `<td colspan="6"></td>`;
  runTbody.appendChild(tr);
  return tr;
}

// Depth-first: every descendant sub-plan's runs, inline, under its own label.
function renderSubPlanRuns(g, indent) {
  for (const c of (g.children || [])) {
    renderSubPlanDivider(c, indent);
    for (const r of c.rows) runTbody.appendChild(renderRunRow(r, r.group === 'handled', true));
    renderSubPlanRuns(c, indent + 1);
  }
}

// One PROJECT group: the new top level. Its children are feature/slice groups and
// any standalone runs of the same project.
function renderProjectGroup(p, forceCollapsedDefault) {
  const s = p.summary;
  // Same rule as a bundle: only a fully auto-handled project starts folded.
  const expanded = (p.key in projectExpanded) ? projectExpanded[p.key]
    : (!forceCollapsedDefault && p.section !== 'handled');
  const nSlices = p.entries.filter(e => e.kind === 'parent').length;
  const summary_text = (nSlices ? `${nSlices} slice group${nSlices === 1 ? '' : 's'} &middot; ` : '') +
    `${s.runs} run${s.runs === 1 ? '' : 's'}` +
    (s.breakdown ? ` &middot; ${s.breakdown}` : '');
  const hdr = headerRow({summary: s, name: p.name, cls: 'run-project',
    indent: 0, expanded, summary_text,
    toggle: () => { projectExpanded[p.key] = !expanded; }});
  attachBulkClear(hdr, p.rows, p.name);
  runTbody.appendChild(hdr);
  if (!expanded) return;
  for (const e of p.entries) {
    // ...and, like a nested sub-plan, each bundle decides from its OWN section.
    if (e.kind === 'parent') renderParentGroup(e, false, 1);
    else runTbody.appendChild(renderRunRow(e.row, e.row.group === 'handled', true));
  }
}

function renderEntry(e, inFoldedSection) {
  if (e.kind === 'project') renderProjectGroup(e, inFoldedSection);
  else if (e.kind === 'parent') renderParentGroup(e, inFoldedSection, 0);
  else runTbody.appendChild(renderRunRow(e.row, e.row.group === 'handled'));
}

// A collapsible section divider -- NOT a hide. Every row under it still shows its
// verdict and WHY it is there when expanded, and 'clear' still archives it.
function sectionHeader(text, expanded, toggle, bg, color) {
  const hdr = document.createElement('tr');
  hdr.style.cursor = 'pointer';
  hdr.style.background = bg;
  hdr.innerHTML = `<td colspan="8" style="font-size:.8rem;color:${color}">` +
    `${expanded ? '▾' : '▸'} ${text}</td>`;
  hdr.onclick = () => { toggle(); saveExpandState(); rerenderRuns(); };
  runTbody.appendChild(hdr);
  return hdr;
}

// The last tree the server gave us. An expand/collapse is a pure VIEW change, so it
// re-renders from this instead of re-fetching: /api/runs/tree takes 0.5-0.9s (measured
// in-browser 2026-09-19, it stats every job sidecar), which made a click look like it
// did nothing and then have the table rearrange itself a beat later, out of sync with
// the user (the user: a nested PASS "disappears" on the next tick). Data still refreshes on
// the 5s poll; only the render is now instant.
let lastRunTree = null;

async function refreshRuns() {
  const res = await fetch('/api/runs/tree');
  if (!res.ok) return;
  lastRunTree = await res.json();
  renderRunTree(lastRunTree);
}

// Re-render the CURRENT data with the current expand state, with no network round
// trip. Falls back to a fetch only if we have never had a tree.
function rerenderRuns() {
  if (lastRunTree) renderRunTree(lastRunTree);
  else refreshRuns();
}

function renderRunTree(tree) {
  const _scrollY = window.scrollY;
  runTbody.innerHTML = '';
  if (!tree.length) {
    const tr = document.createElement('tr');
    tr.innerHTML = '<td colspan="8"><em>nothing finished and awaiting a look -- cleared jobs are archived</em></td>';
    runTbody.appendChild(tr);
    return;
  }
  const countRuns = list => list.reduce((n, e) => n + (e.kind === 'row' ? 1 : e.rows.length), 0);
  const needsEyes = tree.filter(e => e.section === 'needs_eyes');
  const goodToGo = tree.filter(e => e.section === 'good_to_go');
  const handled = tree.filter(e => e.section === 'handled');
  if (!needsEyes.length) {
    const tr = document.createElement('tr');
    tr.innerHTML = '<td colspan="8"><em>nothing needs your eyes -- everything finished either passed or is auto-handled below</em></td>';
    runTbody.appendChild(tr);
  }
  for (const e of needsEyes) renderEntry(e, false);
  // GOOD TO GO: a PASS is a PASS. Sign-off on a passing dispatch is the
  // coordinator's job, not the user's, so these are NOT in "needs your eyes" -- they get
  // their own green section that says so plainly. Expanded by default: passing work
  // should be visible, not folded away like something that did not matter.
  if (goodToGo.length) {
    sectionHeader(`Good to go -- passed, nothing owed (${countRuns(goodToGo)})`,
      goodExpanded, () => { goodExpanded = !goodExpanded; },
      'rgba(30,132,73,.12)', '#1e8449');
    // ...which is why these render UNFOLDED (was `true`, which contradicted the
    // comment above and buried the passes two clicks deep -- the user 2026-09-19).
    if (goodExpanded) for (const e of goodToGo) renderEntry(e, false);
  }
  if (handled.length) {
    sectionHeader(`Auto-handled -- no eyes needed (${countRuns(handled)}: superseded ` +
      `authoring/refine stages, eval arms, read research/diagnosis)`,
      handledExpanded, () => { handledExpanded = !handledExpanded; },
      'rgba(128,128,128,.10)', '#666');
    if (handledExpanded) for (const e of handled) renderEntry(e, true);
  }
  if (window.scrollY !== _scrollY) window.scrollTo(0, _scrollY);
}

async function openGateDetail(id, label) {
  document.getElementById('livelogTitle').textContent = `${label} -- gate detail`;
  document.getElementById('livelogBackdrop').style.display = 'block';
  document.getElementById('livelogModal').style.display = 'flex';
  const el = document.getElementById('livelogContent');
  el.textContent = 'loading...';
  try {
    const res = await fetch('/api/jobs/' + id + '/gate');
    if (!res.ok) { el.textContent = await res.text(); return; }
    const g = await res.json();
    let out = `verdict: ${g.verdict}\n`;
    if (g.review_verdict) out += `review_verdict: ${g.review_verdict}\n`;
    if (g.gate_authority) out += `gate_authority: ${g.gate_authority}\n`;
    if (g.regate_ran) out += `regate: ran\n`;
    if (g.counts) out += `counts: ${JSON.stringify(g.counts)}\n`;
    out += '\n';
    if (g.issues && g.issues.length) {
      out += `issues (${g.issues.length}):\n`;
      for (const i of g.issues) {
        const loc = (i.file && i.line) ? `${i.file}:${i.line}` : (i.file || '-');
        out += `  [${i.severity || '?'}] ${loc} -- ${i.what || ''}\n`;
      }
    } else {
      out += '(no issues recorded)\n';
    }
    if (g.not_checked && g.not_checked.length) {
      out += `\nNOT CHECKED:\n` + g.not_checked.map(x => `  - ${x}`).join('\n') + '\n';
    }
    if (g.untrusted && g.untrusted.length) {
      out += `\nUNTRUSTED:\n` + g.untrusted.map(x => `  - ${x}`).join('\n') + '\n';
    }
    el.textContent = out;
  } catch (e) {
    el.textContent = 'error loading gate detail: ' + e;
  }
}

// CPU lane (read-only): counts, running jobs with runner id, queue depth, recent durations.
async function refreshCpuLane() {
  try {
    const d = await (await fetch('/api/cpu-lane')).json();
    const panel = document.getElementById('cpuLanePanel');
    if (!d.enabled) { panel.hidden = true; window.cpuLaneRunning = []; return; }
    panel.hidden = false;
    const c = d.counts || {};
    const rn = (d.runners || []).filter(r => r.seen_s_ago < 120).length;
    document.getElementById('cpuLaneSummary').textContent =
      `${rn} runner(s) online - depth ${d.queue_depth} - running ${c.running || 0} - done ${c.done || 0}` +
      ` - failed_infra ${c.failed_infra || 0} - local stages ${d.local_stages_active || 0}`;
    const tb = document.querySelector('#cpuLaneTable tbody');
    tb.innerHTML = '';
    const add = (state, j, t) => {
      const tr = document.createElement('tr');
      [state, j.stage || '', j.label || j.id.slice(0, 8), j.bundle || '', j.runner || '', t].forEach(v => {
        const td = document.createElement('td'); td.textContent = v; tr.appendChild(td); });
      tb.appendChild(tr);
    };
    window.cpuLaneRunning = d.running || [];
    (d.running || []).forEach(j => add('running', j, Math.round(j.running_s) + 's'));
    (d.pending || []).forEach(j => add('pending', j, 'waiting ' + Math.round(j.waiting_s) + 's'));
    (d.recent || []).forEach(j => add(j.status + (j.exit_code ? ' (exit ' + j.exit_code + ')' : ''), j,
      (j.duration_s == null ? '' : j.duration_s + 's') + (j.queue_wait_s == null ? '' : ' / wait ' + j.queue_wait_s + 's')));
  } catch (e) { /* lane is optional */ }
}

refresh();
refreshHosts();
refreshHostSettings();
refreshRuns();
refreshCpuLane();
setInterval(refreshCpuLane, 5000);
setInterval(refresh, 4000);
setInterval(refreshHosts, 4000);
setInterval(refreshRuns, 5000);

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

// The old refreshHandoff()/markJobAsActed() pair drove the removed handoff panel
// (Complete -- awaiting action + Pending) against /api/handoff and /api/handoff/acted.
// The unified Run Status table (refreshRuns / clearRun, above) replaces both, so
// this JS is gone. /api/handoff itself still exists for any other consumer.

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

# ONE source of truth for the queue sort tiers: the page is a raw (non-f) string, so
# the Python dict is substituted in here rather than duplicated by hand in the JS.
FRONTEND_HTML = FRONTEND_HTML.replace("__QUEUE_STATUS_ORDER__", json.dumps(QUEUE_STATUS_ORDER))
FRONTEND_HTML = FRONTEND_HTML.replace("__PLAN_ALERT_STATUSES__", json.dumps(PLAN_ALERT_STATUSES))

# WHY THIS EXISTS (2026-09-19). The dashboard is a page people leave open for days.
# It keeps itself current by polling /api/jobs and re-rendering rows into the
# existing DOM -- but the CSS and JS are INLINE in the document, so they are frozen
# at whatever was served when the tab was first opened. Restarting the server
# changes nothing for an already-open tab: the data goes on updating, which makes
# the page look live, while the front-end stays at the old build forever.
#
# That is exactly how the mobile fix "failed" on the user's phone: he was looking at a
# tab loaded minutes before the fix landed, so it rendered the PREVIOUS commit's
# CSS (headers breaking one letter per line, `elapsed` not the short `time`) over
# completely current job data. Verified at the time: the deployed HTML contained
# the new CSS, Cache-Control has been `no-store` since 2026-09-01, and real WebKit
# at 390px rendered the new CSS correctly -- so it was neither an HTTP cache nor a
# Safari quirk, just a document that was never re-fetched.
#
# FRONTEND_VERSION is a hash of the fully-substituted page, so it changes exactly
# when the front-end changes and never on a mere restart. It rides along on the
# polls the page already makes (X-Frontend-Version on every JSON response), and the
# page reloads itself once when it sees a version that is not its own.
FRONTEND_VERSION = hashlib.sha256(FRONTEND_HTML.encode()).hexdigest()[:12]
FRONTEND_HTML = FRONTEND_HTML.replace("__FRONTEND_VERSION__", json.dumps(FRONTEND_VERSION))


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


_VM_STAT_RE = re.compile(r'^"?([A-Za-z][A-Za-z0-9 ()"/-]*?)"?:\s+(\d+)\.?\s*$')


def _parse_vm_stat(vm_stat_text, page_size, total_bytes):
    """PURE: real system-memory (used_gb, total_gb) from `vm_stat` output.

    'used' is the memory that is NOT trivially reclaimable -- total minus the
    pages the kernel can hand back on demand (free + inactive file cache +
    speculative + purgeable). That is the figure that actually predicts memory
    pressure / OOM, which is the whole point of this panel (a model sitting
    resident from keep_alive is real occupancy even when the lane is idle).
    Returns (None, None) if the text can't be parsed, so the caller can fall
    back to the model-footprint sum rather than show a wrong number."""
    pages = {}
    for line in vm_stat_text.splitlines():
        m = _VM_STAT_RE.match(line.strip())
        if m:
            pages[m.group(1).strip().lower()] = int(m.group(2))
    if not pages or page_size <= 0 or total_bytes <= 0:
        return (None, None)
    reclaimable = (pages.get("pages free", 0)
                   + pages.get("pages inactive", 0)
                   + pages.get("pages speculative", 0)
                   + pages.get("pages purgeable", 0))
    used_bytes = max(0, total_bytes - reclaimable * page_size)
    # Never let rounding/parse skew report used > total.
    used_bytes = min(used_bytes, total_bytes)
    return (round(used_bytes / 1e9, 1), round(total_bytes / 1e9, 1))


def _studio_system_mem_gb():
    """Real (used_gb, total_gb) system memory for Studio -- the box this dashboard
    runs on -- via `vm_stat` + hw.memsize. Best-effort: (None, None) on any failure
    so _hosts_summary falls back to the resident-model footprint sum."""
    try:
        import subprocess
        total_bytes = int(subprocess.run(
            ["sysctl", "-n", "hw.memsize"], capture_output=True, text=True, timeout=3
        ).stdout.strip())
        out = subprocess.run(["vm_stat"], capture_output=True, text=True, timeout=3).stdout
        page_size = 4096
        mps = re.search(r"page size of (\d+) bytes", out)
        if mps:
            page_size = int(mps.group(1))
        return _parse_vm_stat(out, page_size, total_bytes)
    except Exception:
        return (None, None)


_DBK_SUMMARY_CACHE = {"at": 0.0, "val": None}
_DBK_EST_MEM_RE = re.compile(r"^(\S+)\s+\S+\s+\S+\s+\S+\s+[\d.]+ GB\s+([\d.]+) GB", re.M)


def _darkbloom_host_summary():
    """The studio lane as Darkbloom reports it (2026-10-01): warm models + their
    estimated memory from `darkbloom status` / `models list`, system memory from
    vm_stat (Metal allocations never show in process RSS, so vm_stat -- not a sum of
    model sizes -- is the honest occupancy figure). Cached 20s: each CLI call is
    ~1.2s and the dashboard polls. None when Darkbloom isn't serving locally."""
    import subprocess as _sp
    now = time.monotonic()
    if _DBK_SUMMARY_CACHE["val"] is not None and now - _DBK_SUMMARY_CACHE["at"] < 20:
        return _DBK_SUMMARY_CACHE["val"]
    url = q._darkbloom_url()
    if not url:
        return None
    exe = os.path.expanduser("~/.darkbloom/bin/darkbloom")
    try:
        st = _sp.run([exe, "status"], capture_output=True, text=True, timeout=15).stdout
        ml = _sp.run([exe, "models", "list"], capture_output=True, text=True, timeout=15).stdout
    except Exception:
        return None
    est = {m.group(1).lower(): float(m.group(2)) for m in _DBK_EST_MEM_RE.finditer(ml)}
    warm = []
    for line in st.splitlines():
        if line.startswith("Warm models:"):
            warm = [n.strip() for n in line.split(":", 1)[1].split(",") if n.strip()]
    models = [{"name": n, "size_gb": est.get(n.lower(), 0.0)} for n in warm]
    model_gb = round(sum(m["size_gb"] for m in models), 1)
    used_gb, total_gb = _studio_system_mem_gb()
    mem_source = "system"
    if used_gb is None:
        mem_source, used_gb, total_gb = "models", model_gb, 64
    out = {"name": "studio-db", "url": url, "models": models, "total_gb": total_gb,
           "used_gb": used_gb, "model_gb": model_gb, "mem_source": mem_source,
           "usable_gb": None, "has_bypass": False, "engine": "darkbloom"}
    _DBK_SUMMARY_CACHE.update(at=now, val=out)
    return out


def _hosts_summary():
    """What's actually resident right now on each host, independent of the
    job queue -- the user's ask: visibility into GPU/memory occupancy even when
    nothing is currently dispatched (an idle lane can still have a model
    sitting loaded from keep_alive, which is exactly the state that caused
    tonight's OOM incidents).

    Iterates the CONFIGURED host table (~/.ollama-dispatch/hosts.json, edited
    from the Settings panel below) rather than the two hardcoded names it used
    to read -- so a host added in Settings shows up here, and a host removed
    there disappears from here, with no code change and no restart.
    """
    w = q.worker()
    hosts_cfg = dict(w.KNOWN_OLLAMA_HOSTS)
    big_name = getattr(w, "BIG_HOST_NAME", "studio")
    bypass_up = False
    bypass_url = getattr(q, "LLAMA_SERVER_QWEN38_URL", None)
    if bypass_url:
        try:
            req = urllib.request.Request(f"{bypass_url}/health")
            with urllib.request.urlopen(req, timeout=3) as resp:
                bypass_up = resp.status == 200
        except Exception:
            bypass_up = False

    # The box this dashboard itself runs on gets REAL system memory (total minus
    # reclaimable, via vm_stat): the sum of resident model footprints misses every
    # OTHER process holding memory, which is exactly what understated the true
    # pressure before the OOM incidents. Detected by URL (loopback) rather than by
    # the name "studio", since the name is user-chosen now.
    def _is_local(url):
        try:
            hn = (urllib.parse.urlsplit(url).hostname or "").lower()
        except Exception:
            return False
        return hn in ("127.0.0.1", "localhost", "::1", "0.0.0.0")

    out_hosts = []
    # DARKBLOOM: the big/studio host IS Darkbloom now -- report it from Darkbloom,
    # not from an Ollama /api/ps that no longer exists (see ollama-queue.py DARKBLOOM LANE).
    _dbk = _darkbloom_host_summary()
    if _dbk is not None:
        out_hosts.append(_dbk)
    for name, spec in hosts_cfg.items():
        if _dbk is not None and name == big_name:
            continue
        url = spec["url"]
        models = _ollama_resident_models(url)
        model_gb = round(sum(m["size_gb"] for m in models), 1)
        usable_bytes = spec.get("usable_bytes")
        usable_gb = round(usable_bytes / 1e9, 1) if usable_bytes else None
        if _is_local(url):
            used_gb, total_gb = _studio_system_mem_gb()
            mem_source = "system"
            if used_gb is None:
                mem_source = "models"
                total_gb = round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 1e9) \
                    if hasattr(os, "sysconf") else 0
                used_gb = model_gb
        else:
            # A remote host has no shell here (Unraid is GraphQL-only), so its
            # "real" memory is the occupancy /api/ps reports for resident models,
            # measured against the budget the fit router actually enforces. A host
            # with NO configured budget is UNMEASURED: report the occupancy and no
            # denominator rather than inventing a total.
            used_gb, mem_source = model_gb, "vram"
            total_gb = usable_gb if usable_gb else 0
        out_hosts.append({
            "name": name, "url": url, "models": models, "total_gb": total_gb,
            "used_gb": used_gb, "model_gb": model_gb, "mem_source": mem_source,
            "usable_gb": usable_gb,
            # The llama-server bypass is a SECOND process on the same physical box
            # as the big host, sharing one memory pool -- flagged so the panel can
            # group it under that host instead of showing it as another machine.
            "has_bypass": name == big_name and bool(bypass_url),
        })
    return {
        "hosts": out_hosts,
        "llama_server_bypass": {"url": bypass_url, "up": bypass_up,
                                 "model": getattr(q, "LLAMA_SERVER_QWEN38_MODEL", None) if bypass_up else None},
    }


def _hosts_settings():
    """The configured host table as the Settings panel wants it: a list of
    {name, url, usable_gb} with usable_gb None == unmeasured."""
    w = q.worker()
    out = []
    for name, spec in dict(w.KNOWN_OLLAMA_HOSTS).items():
        ub = spec.get("usable_bytes")
        out.append({"name": name, "url": spec["url"],
                    "usable_gb": round(ub / 1024 ** 3, 2) if ub else None,
                    "usable_bytes": ub})
    return {"hosts": out, "config_path": str(w.OLLAMA_HOSTS_FILE)}


def _save_hosts_settings(rows):
    """Persist a Settings-panel host list. Each row: {name, url, usable_gb?}.
    usable_gb blank/absent/0 -> usable_bytes None (UNMEASURED: the fit router
    will not clear this host, see ollama-worker.py's schema note). Raises
    ValueError with a user-facing message on bad input."""
    w = q.worker()
    if not isinstance(rows, list):
        raise ValueError("expected a list of hosts")
    table, seen = {}, set()
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("each host must be an object")
        name = str(row.get("name") or "").strip()
        url = str(row.get("url") or "").strip().rstrip("/")
        if not name:
            raise ValueError("every host needs a name")
        if not re.fullmatch(r"[A-Za-z0-9._-]+", name):
            raise ValueError(f"invalid host name {name!r}: letters, digits, . _ - only "
                             f"(the name is used as a lane key and on the --host flag)")
        if name in seen:
            raise ValueError(f"duplicate host name {name!r}")
        seen.add(name)
        if not re.match(r"^https?://[^\s/]+", url):
            raise ValueError(f"host {name!r}: url must look like http://host:11434")
        gb = row.get("usable_gb")
        if gb in (None, "", "null"):
            usable_bytes = None
        else:
            try:
                gbf = float(gb)
            except (TypeError, ValueError):
                raise ValueError(f"host {name!r}: usable GB must be a number (or blank "
                                 f"for unmeasured)")
            if gbf <= 0:
                raise ValueError(f"host {name!r}: usable GB must be > 0 (or blank for "
                                 f"unmeasured)")
            usable_bytes = int(gbf * 1024 ** 3)
        table[name] = {"url": url, "usable_bytes": usable_bytes}
    w.save_ollama_hosts(table)
    _invalidate_response_cache()
    return _hosts_settings()


def _display_model(model, host):
    """The model that actually serves a job, for display. A job on the Darkbloom
    lane carries its legacy Ollama tag (qwen3.8:27b-q4_K_M) until the lane boundary
    aliases it; showing that tag implies 3.8 is running when it is not."""
    try:
        if host in ("auto", "studio", "studio-db", "darkbloom") and q._darkbloom_url():
            return q._darkbloom_model(model)
    except Exception:
        pass
    return model


def _job_summary(j, jobs=None):
    # `after` and `plan_note` are what let the queue panel render the waiting line in
    # true EXECUTION order (each planned slice under the job it waits on).
    d = {k: j.get(k) for k in
         ("id", "label", "model", "host_pref", "status", "lane", "pid", "exit_code",
          "error", "enqueued_at", "after", "plan_note",
          # WHY it failed (worker's terminal_reason) and WHOSE fault it is
          # (ollama-queue.py's classify_failure) -- stamped at reap, read here.
          "terminal_reason", "failure_class", "failure_detail")}
    # GPU-EXCLUSIVE rows (ollama-queue.py `enqueue-gpu`): a non-LLM shell job that has a
    # lane's GPU to itself. The model field is only a placeholder, so the row carries
    # its own label: kind + one-line summary + what it is waiting for.
    if j.get("job_kind") == "gpu_exclusive":
        d["job_kind"] = "gpu_exclusive"
        d["gpu_summary"] = (j.get("gpu_job") or {}).get("summary")
        d["gpu_wait"] = j.get("gpu_wait")
    # The explicit `enqueue --bundle <tag>` stamp. q.job_group_key lets it WIN over
    # label parsing, but only if the row it is handed still carries it: dropping it
    # here made an ad-hoc bundle (mlx-smoke: no slice plan, chain=None) render as N
    # standalone rows while the daemon scheduled it as ONE bundle (2026-09-27).
    d["model"] = _display_model(d.get("model"), j.get("lane") or j.get("host_pref"))
    if j.get(q.BUNDLE_FIELD):
        d[q.BUNDLE_FIELD] = j.get(q.BUNDLE_FIELD)
    if j.get("hold_reason"):
        d["hold_reason"] = j.get("hold_reason")
    # NUMBERED RERUNS (the user 2026-10-01). {n, cause}: the dashboard draws "#N" next to
    # the label with `cause` as the tooltip, so a continuation round is never mistaken
    # for a first attempt. ollama-queue.py stamps it at enqueue; DERIVE it here for a
    # row enqueued before that existed (or one whose chain only became visible later),
    # which needs the sibling rows -- hence the optional `jobs`. Contained: a derive
    # failure just means no badge.
    _rr = j.get("rerun") if isinstance(j.get("rerun"), dict) else None
    if not _rr:
        try:
            _txt = q.rerun_header_text(j, jobs or [])
            if _txt:
                _rr = {"n": q.rerun_number(j, jobs or []), "cause": _txt}
        except Exception:
            _rr = None
    if _rr:
        d["rerun"] = _rr
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
    d["wall_s"] = elapsed_s
    # ACTIVE runtime (the user 2026-09-27: 0b130de503d8 read "8 hours" for ~38 min of
    # work -- the rest was a promote-preempt pause). The daemon accrues each finished
    # run segment into job["active_s"] (_accrue_active_s at reap); add the live
    # segment for a running job. Paused = wall - active, shown separately. A job
    # with no accrual yet (launched before this existed, never reaped since) keeps
    # the wall figure rather than showing a wrong one.
    active_s = j.get("active_s")
    try:
        if j.get("status") == "running" and j.get("launched_at") \
                and j.get("active_accrued_for") != j.get("launched_at"):
            from datetime import datetime as _dt
            seg = time.time() - _dt.fromisoformat(
                str(j["launched_at"]).replace("Z", "+00:00")).timestamp()
            active_s = (float(active_s) if active_s is not None else
                        (0.0 if elapsed_s is None or elapsed_s - seg < 60 else None))
            if active_s is not None:
                active_s += max(0.0, seg)
    except (ValueError, TypeError):
        pass
    if active_s is not None:
        d["elapsed_s"] = float(active_s)
        d["paused_s"] = (max(0.0, elapsed_s - float(active_s))
                         if elapsed_s is not None else None)
    else:
        d["elapsed_s"] = elapsed_s
        d["paused_s"] = None
    return d


def _gate_parent_ref(label):
    """PURE. What a gate/regate review row points at, or None if it is not one.

    ollama-queue.py's convention (see its _is_gate_job / the hard-hold block) is a
    PREFIX naming the job under review: `gate-<parent job id>` / `regate-<parent job
    id>` -- confirmed on the live queue (`gate-83784637d033`). A trailing
    `-gate`/`-regate` on a slice label is also accepted, because the dashboard has
    no business caring which spelling produced the row. Any '[...]' annotation and
    trailing refine rounds are stripped first, exactly like _slice_feature_base.
    Returns the bare reference: a 12-hex job id, or a slice label."""
    s = re.sub(r"\s*\[[^\]]*\]\s*$", "", str(label or "")).strip()
    m = re.match(r"^(?:re)?gate-(.+)$", s)
    if not m:
        m = re.match(r"^(.+)-(?:re)?gate$", s)
        if not m:
            return None
    return re.sub(r"(?:-r\d+)+$", "", m.group(1)) or None


def _esc_review_ref(label):
    """PURE. 'esc-review-<TS>-<bundle>-<slice>' -> '<bundle>-<slice>', else None. Same
    rule as bundle_view.esc_review_ref (the <TS>- prefix is optional: the watcher keeps
    only the last 60 chars of the stem)."""
    m = re.match(r"^esc-review-(?:(?:\d{0,8}T)?\d*Z-)?(.+)$",
                 re.sub(r"\s*\[[^\]]*\]\s*$", "", str(label or "")).strip())
    return m.group(1) if m else None


def _has_bundle_tag(row):
    """PURE. True when the row carries an explicit `--bundle` tag (q.BUNDLE_FIELD), which
    job_group_key already honours first: relinking passes must not override it. A tag
    that is itself a mangled esc-review label ('esc-review-32637Z-<plan>') is a stamping
    artefact, not a real bundle, so it never counts."""
    t = row.get(q.BUNDLE_FIELD)
    return (isinstance(t, str) and bool(t.strip())
            and not t.strip().startswith("esc-review-"))


def _esc_review_job_id(label):
    """PURE. The BFMR-style job-level escalation form: 'esc-review-<TS>-job-<12hex>'
    -> '<12hex>' (the parent JOB id), else None. The slug+slice form
    ('esc-review-<TS>-<bundle>-<slice>') returns None here."""
    ref = _esc_review_ref(label)
    m = re.match(r"^job-([0-9a-f]{6,})$", ref or "")
    return m.group(1) if m else None


def _esc_review_display(label, group_key=None):
    """PURE. Short human name for an esc-review row: 'Escalation review · <slice>' for
    the slug form (bundle prefix stripped), 'Escalation review · job <id>' for the
    job form. None when the label is not an esc-review."""
    ref = _esc_review_ref(label)
    if not ref:
        return None
    jid = _esc_review_job_id(label)
    if jid:
        return "Escalation review \u00b7 job " + jid
    if group_key and ref.startswith(str(group_key) + "-"):
        ref = ref[len(str(group_key)) + 1:]
    return "Escalation review \u00b7 " + ref


def _gate_plan_key(row, by_id, reverse=None, log_dir=None):
    """The PLAN a gate/regate row belongs to, or None when it can't be resolved.

    A gate is part of the work it gates (2026-09-18, the user: "gates should stay part
    of the bundle, not outside it"), but its own label names a job id, not a slice,
    so q.job_group_key alone makes it a standalone group of one. Resolution order:
      1. the parent row is still in this payload -> take ITS group key (the exact
         same authority, so a gate can never land in a different bundle than the
         slice it reviews);
      2. the parent was already pruned from state.json (the common case -- the gate
         outlives its job) -> its durable sidecar <log_dir>/<id>.done.json still has
         the label, so resolve that through q.job_group_key;
      3. the reference is a slice label already -> resolve it directly.
    Best-effort: any read failure just returns None and the row stays standalone."""
    ref = _gate_parent_ref(row.get("label"))
    if not ref:
        return None
    parent = (by_id or {}).get(ref)
    if parent is not None:
        return parent.get("group_key") or q.job_group_key(parent, reverse)
    if re.match(r"^[0-9a-f]{6,}$", ref):
        try:
            d = Path(log_dir) if log_dir else q.LOG_DIR
            lbl = json.loads((d / f"{ref}.done.json").read_text()).get("label")
        except Exception:
            lbl = None
        return q.job_group_key({"label": lbl}, reverse) if lbl else None
    return q.job_group_key({"label": ref}, reverse)


def _annotate_job_groups(rows, log_dir=None):
    """Tag each live-queue row with the LOGICAL job it belongs to, so the dashboard can
    offer one "promote the whole job" action instead of one click per slice:
      row['group_key']     -- the slice-plan project label (q.job_group_key), with a
                              gate/regate row folded into the plan it reviews
      row['group_pending'] -- how many PENDING rows share that key (the block that a
                              group-promote would actually move)
    The key comes from ollama-queue.py's own job_group_key -- ONE authority for what
    "the same job" means, so the button and `promote-group` can never disagree. Only
    ADDS fields. PURE apart from q's cached slice-plan index read."""
    reverse = q.slice_group_index()
    counts = {}
    for r in rows:
        r["group_key"] = q.job_group_key(r, reverse)
    # Second pass, once every row HAS a key: a gate/regate row joins the bundle of
    # the job it reviews instead of floating as its own top-level row.
    by_id = {r.get("id"): r for r in rows}
    for r in rows:
        if _gate_parent_ref(r.get("label")) and not _has_bundle_tag(r):
            plan = _gate_plan_key(r, by_id, reverse, log_dir)
            if plan:
                r["group_key"] = plan
    # Third pass: an escalation-review row (`esc-review-<TS>-<bundle>-<slice>`, from
    # dispatch-escalation-watcher) joins the bundle of the slice it reviews instead of
    # floating loose. The <TS>- prefix is stripped to recover the plan/slice label; the
    # watcher keeps only the last 60 chars of the stem, so when the exact label is not
    # in the slice index a UNIQUE index key ending with the (possibly truncated) ref is
    # taken before the label-parsing fallback.
    # A second opinion (`secondop-<parent job id>`) carries only its PARENT's id:
    # resolve it exactly like a gate row (live parent -> its group_key, else the
    # durable <id>.done.json label). An unresolvable parent stays a loose row.
    for r in rows:
        sm = re.match(r"^secondop-([0-9a-f]{6,})", str(r.get("label") or ""))
        if sm and not _has_bundle_tag(r):
            try:
                plan = _gate_plan_key({"label": "gate-" + sm.group(1)}, by_id, reverse,
                                      log_dir)
            except Exception:
                plan = None
            if plan:
                r["group_key"] = plan
    for r in rows:
        ref = _esc_review_ref(r.get("label"))
        jid = _esc_review_job_id(r.get("label"))
        if (jid or ref) and _has_bundle_tag(r):
            continue          # an explicit --bundle tag always wins over label parsing
        if jid:
            # job-level escalation: resolve through the escalated JOB (live row, else
            # its durable done.json label). Unresolvable -> stays a loose row.
            try:
                key = _gate_plan_key({"label": "gate-" + jid}, by_id, reverse, log_dir)
            except Exception:
                key = None
            if key:
                r["group_key"] = key
        elif ref:
            key = None
            if ref not in reverse and len(ref) >= 40:
                hits = [k for k in reverse if k.endswith(ref)]
                key = q.job_group_key({"label": hits[0]}, reverse) if len(hits) == 1 else None
            key = key or q.job_group_key({"label": ref}, reverse)
            if key:
                r["group_key"] = key
    for r in rows:
        if r.get("status") == "pending" and r["group_key"]:
            counts[r["group_key"]] = counts.get(r["group_key"], 0) + 1
    for r in rows:
        r["group_pending"] = counts.get(r.get("group_key"), 0)
    return rows


def _completed_summary(r):
    """Render one durable job-RESULT (from q._iter_job_results, see ollama-queue.py's
    "Durable job-RESULTS store") into the shape the dashboard's Completed table wants.
    This is the piece that survives reaping -- unlike _job_summary (state.json rows,
    gone the tick after a job completes under RETAIN_DONE_RECENT=0), these come from
    the never-pruned <id>.done.json/.gate.json/.diff sidecars, so a gate verdict here
    is durable: it keeps showing up after the live row is long gone."""
    tag = q._verdict_tag(r)
    verdict = tag
    if r.get("regate_ran"):
        verdict += " (regate)"
    elif r.get("gate_authority"):
        verdict += f" ({r['gate_authority']})"
    gx = _gate_extra(r)
    return {
        "id": r["id"],
        "label": r.get("label") or r["id"],
        "model": _display_model(r.get("model"), r.get("host")),
        "host": r.get("host"),
        "status": r.get("status"),
        "verdict": verdict,
        "raw_verdict": tag,
        "changed_file_count": r.get("changed_file_count"),
        "timestamp": r.get("timestamp"),
        "has_gate": bool(r.get("gate_json")),
        # Pipeline-stage provenance, straight off the gate payload -- what makes an
        # "author" record distinguishable from a real coding deliverable (see
        # _run_blocked_reason). q._load_job_result does not normalize these through.
        "auto_pipeline_stage": gx.get("auto_pipeline_stage"),
        "review_enqueue": gx.get("review"),
        "auto_fix_enqueue_status": gx.get("auto_fix_enqueue_status"),
        "regate": r.get("regate"),
        "terminal_reason": r.get("terminal_reason"),
        "failure_class": r.get("failure_class"),
        "failure_detail": r.get("failure_detail"),
    }


# Gate-payload fields q._load_job_result does not normalize but Run Status needs.
# Kept to a tiny fixed set and read straight from the same never-pruned sidecar.
_GATE_EXTRA_KEYS = ("auto_pipeline_stage", "auto_pipeline_action", "review",
                    "auto_fix_enqueue_status")


def _gate_extra(r):
    """Pull _GATE_EXTRA_KEYS off a job result's gate.json. Best-effort: a missing or
    unreadable sidecar just means no extras, never an error (same discipline as
    every other durable read here)."""
    gp = r.get("gate_json")
    if not gp:
        return {}
    try:
        d = json.loads(Path(gp).read_text())
    except (OSError, ValueError):
        return {}
    if not isinstance(d, dict):
        return {}
    return {k: d.get(k) for k in _GATE_EXTRA_KEYS}


# Statuses that mean "this job is still live in the queue" -- a row in this state
# belongs in the Ollama Queue panel above, never in Run Status (2026-09-18, the user:
# "pending doesn't have to show in run status, those are above in the queue").
# Applied as a status-field safety net ON TOP OF the by-id live_ids exclusion
# below: live_ids catches a job still literally present in state["jobs"]; this
# catches a durable sidecar whose OWN persisted status snapshot is non-terminal
# (e.g. a stale/partial .done.json, or a duplicate label re-enqueued under a new
# id while an old snapshot lingers) -- only a genuinely-finished run (done /
# failed / done_unconverged, or anything else with a real terminal verdict)
# belongs here. This does NOT touch awaiting_signoff: a job that's actually
# finished but still owed a sign-off has a terminal status and keeps showing.
_LIVE_QUEUE_STATUSES = {"pending", "held", "queued", "running", "paused", "scheduled"}


def _completed_jobs(limit=50):
    """Completed/reaped jobs for the dashboard's cross-check table: every durable
    result NOT currently a genuinely-live row (running/pending/paused/etc., see
    _LIVE_QUEUE_STATUSES -- that status wins per job id; see ollama-queue-api.py's
    module docstring on reusing ollama-queue.py's own state), newest-timestamp
    first, bounded so the page stays fast against thousands of historical jobs.

    Bug (2026-09-19, the user: "gate is already gone. we should leave it in the queue
    as pending gate, or gate passed for visibility until the full batch is done"):
    this used to exclude ANY job id still present in queue-state.json's `jobs`
    array, on the theory that presence there meant "still live or not yet reaped".
    But `prune_finished_jobs` deliberately keeps a confirmed slice-plan member in
    `jobs` forever once done (see its own 2026-09-19 fix, to stop a bundle from
    vanishing between slices) -- so a real slice's OWN row, freshly done with a
    final verdict, was invisible here for as long as its bundle stayed in flight.
    Fix: only exclude a job whose LIVE status is still genuinely non-terminal
    (_LIVE_QUEUE_STATUSES); a `done`/`failed`/etc. row flows through to its durable
    verdict like any reaped job, whether or not the daemon has pruned its live
    entry yet."""
    with q._Locked() as lock:
        state = lock.load()
    live_status_by_id = {j["id"]: str(j.get("status") or "") for j in state["jobs"]}
    out = []
    for r in q._iter_job_results():
        if live_status_by_id.get(r["id"]) in _LIVE_QUEUE_STATUSES:
            continue  # genuinely still running/pending -- the live row is authoritative
        summary = _completed_summary(r)
        if summary["status"] in _LIVE_QUEUE_STATUSES:
            continue  # not genuinely finished -- belongs in the queue panel, not here
        out.append(summary)
        if len(out) >= limit:
            break
    return out


# --- Unified run-status list (2026-09-17, the user: "complete jobs and handoff should
# essentially be the same one list when qwen is done that shows the run status that
# we can clear once it's handled") ---
# ONE list replaces the two overlapping surfaces that used to sit on this page:
#   * the never-pruned "Completed Jobs" durable-verdict table (/api/jobs/completed), and
#   * the handoff "Complete -- awaiting action" panel (/api/handoff, acted.json state).
# A completed dispatch shows up here the moment qwen finishes (its durable
# <id>.done.json/.gate.json sidecars land), carrying its FINAL gate verdict and run
# status. "Clear/handled" ARCHIVES the row -- it moves those sidecars into
# LOG_DIR/archive/ (the exact recoverable pattern the backlog was archived with),
# so q._iter_job_results' NON-recursive glob stops seeing it and the row drops off
# BOTH this list and handoff-emit's derivation (which globs the same dir the same
# non-recursive way) in one action, with no acted.json bookkeeping to drift.
# Archived rows STAY archived (recoverable by hand from LOG_DIR/archive/); they do
# not come back into the list.

def _run_status_jobs(limit=100):
    """The unified run-status list: every durable completed result that is NOT a
    live queue row and has NOT been cleared/archived, newest first, each annotated
    with its final verdict/run status and whether it still needs sign-off.

    Cleared jobs are absent by construction: clearing moves their sidecars under
    LOG_DIR/archive/, which q._iter_job_results (non-recursive glob) never reads.
    awaiting_signoff reuses handoff-emit's ONE predicate so this list and the
    handoff INDEX can never disagree about which jobs a human still owes a decision.
    """
    rows = _completed_jobs(limit=limit)
    for r in rows:
        r["awaiting_signoff"] = ho.signoff_blocks_acting(r["id"], r.get("label")) is not None
    # Live queue labels count as "newer stages" for supersede detection: a finished
    # auto-author CONCERNS row whose feature already has a running/pending refine
    # stage is an intermediate that a human should NOT have to eyeball.
    try:
        with q._Locked() as lock:
            live_labels = _live_queue_labels(lock.load().get("jobs", []))
    except Exception:
        live_labels = []
    _annotate_run_groups(rows, live_labels)
    _annotate_run_parents(rows)
    _annotate_run_projects(rows)
    _annotate_eyes_labels(rows)
    return rows


# --- run-status RETENTION (2026-10-02, the user via coordinator): harness rows
# (auto-author/auto-refine) once their parent deliverable lands or they are acted on,
# cancelled slice rows, and acted-on diag/research rows are archived automatically;
# awaiting-signoff rows and unreviewed deliverables never are. The ONE predicate lives
# in runstatus_retention.py and is shared by the periodic sweep below, the
# /api/runs/retention preview + /apply endpoints, and `qctl runs-clear`. It scans ALL
# rows (the /api/runs list is capped at the newest 100).
_rsr_spec = importlib.util.spec_from_file_location(
    "runstatus_retention", Path(__file__).resolve().parent / "runstatus_retention.py")
rsr = importlib.util.module_from_spec(_rsr_spec)
_rsr_spec.loader.exec_module(rsr)
RETENTION_MODE = os.environ.get("RUNSTATUS_RETENTION", "live").lower()   # live|shadow|off
RETENTION_INTERVAL_S = int(os.environ.get("RUNSTATUS_RETENTION_INTERVAL", "900"))
_RETENTION_LOG = q.LOG_DIR / "RUNSTATUS-RETENTION.log"


def _retention_inputs():
    """(rows, slice_status, acted) over EVERY durable run-status row."""
    rows = _run_status_jobs(limit=10 ** 9)
    reverse, _projects = _load_slice_index()

    def _read_state(proj):
        try:
            return json.loads((SLICE_RUNS_DIR / f"{proj}.json").read_text())
        except (OSError, ValueError):
            return None
    try:
        acted = set((ho._load(ho.ACTED, {}) or {}).keys())
    except Exception:
        acted = set()
    return rows, rsr.make_slice_status(reverse, _read_state), acted


def _retention_archive(job_id, reason):
    return _archive_run(job_id, how=f"run-status retention: {reason}")


def _retention_preview():
    rows, ss, acted = _retention_inputs()
    return {"mode": RETENTION_MODE, "scanned": len(rows),
            "eligible": rsr.sweep(rows, slice_status=ss, acted=acted,
                                  archive=_retention_archive, apply=False)}


def _retention_apply(ids):
    rows, ss, acted = _retention_inputs()
    return rsr.apply_ids([str(i) for i in ids], rows, slice_status=ss, acted=acted,
                         archive=_retention_archive)


def _retention_sweep_once():
    if RETENTION_MODE not in ("live", "shadow"):
        return []
    rows, ss, acted = _retention_inputs()
    done = rsr.sweep(rows, slice_status=ss, acted=acted, archive=_retention_archive,
                     apply=RETENTION_MODE == "live")
    if done:
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        try:
            with _RETENTION_LOG.open("a") as fh:
                for d in done:
                    fh.write(f"{stamp} mode={RETENTION_MODE} {d['id']} {d['label']} "
                             f"[{d['category']}] {d['reason']} -> {d['result']}\n")
        except OSError:
            pass
        _invalidate_response_cache()
    return done


def _retention_loop():
    time.sleep(60)
    while True:
        try:
            _retention_sweep_once()
        except Exception as e:
            print(f"[queue-api] retention sweep error: {e}", flush=True)
        time.sleep(RETENTION_INTERVAL_S)


# --- parent/child rollup of sliced jobs (2026-09-18, the user: "when jobs are auto
# sliced can we show the main job, and then expand that main job to show the jobs
# inside it ... so we can better see ... where a full project/dispatch is") ---
# A sliced dispatch enqueues each slice as "<project-label>-<sliceid>" (e.g.
# bg-escalation-s1-rules), with its authoring/refine meta-jobs as
# auto-author-<project>-<sid>-... / auto-refine-<project>-<sid>-rN. The slicer's
# OWN state -- ~/.ollama-dispatch/slice-runs/<project>.json (label + ordered slice
# ids + per-slice status) and slice-plans/<project>.slices.json (the plan) -- is
# the AUTHORITATIVE project->children map, so we read those to derive the parent
# rather than regex-guessing; label-prefix parsing is only a fallback for jobs with
# no plan file. This ADDS per-row fields (parent/slice_id/parent_total); it does
# NOT touch the needs_eyes/handled grouping above, so a child that needs eyes still
# forces its parent into the needs_eyes section on the front end (fail-safe).
SLICE_RUNS_DIR = Path.home() / ".ollama-dispatch" / "slice-runs"
SLICE_PLANS_DIR = Path.home() / ".ollama-dispatch" / "slice-plans"


def _load_slice_index(runs_dir=None, plans_dir=None):
    """Build the authoritative parent->children map from the slicer's own state.

    Returns (reverse, projects):
      reverse  : {full_slice_label -> project_label}, full = f"{project}-{sliceid}"
      projects : {project_label -> {"order": [sliceid,...], "total": int}}

    slice-run-state (has live per-slice status + ordered ids) is read first and is
    authoritative; slice-plans fill in any slice ids / projects the run-state hasn't
    recorded yet. Best-effort: an unreadable or oddly-shaped file is skipped, never
    fatal -- anything not in the map falls back to label-prefix parsing downstream."""
    runs_dir = Path(runs_dir) if runs_dir else SLICE_RUNS_DIR
    plans_dir = Path(plans_dir) if plans_dir else SLICE_PLANS_DIR
    reverse, projects = {}, {}

    def add(label, sids):
        if not label or not sids:
            return
        p = projects.setdefault(label, {"order": [], "total": 0})
        for sid in sids:
            if not sid:
                continue
            reverse.setdefault(f"{label}-{sid}", label)
            if sid not in p["order"]:
                p["order"].append(sid)
        p["total"] = len(p["order"])

    try:
        run_files = sorted(runs_dir.glob("*.json"))
    except OSError:
        run_files = []
    for fp in run_files:
        try:
            d = json.loads(fp.read_text())
        except (OSError, ValueError):
            continue
        if not isinstance(d, dict):
            continue
        sids = d.get("order")
        if not sids and isinstance(d.get("slices"), dict):
            sids = list(d["slices"].keys())
        add(d.get("label"), sids)

    try:
        plan_files = sorted(plans_dir.glob("*.json"))
    except OSError:
        plan_files = []
    for fp in plan_files:
        try:
            d = json.loads(fp.read_text())
        except (OSError, ValueError):
            continue
        if not isinstance(d, dict):
            continue
        sl = d.get("slices")
        sids = [s.get("id") for s in sl if isinstance(s, dict)] if isinstance(sl, list) else None
        add(d.get("label"), sids)

    return reverse, projects


def _project_for_base(base, reverse):
    """Map a feature base (the _feature_and_rank base of a stage label, e.g.
    'bg-escalation-s1-rules') to its parent PROJECT label. Prefers the slice-index
    reverse map (authoritative); falls back to parsing a trailing '-s<N>-...' slice
    id off the base for jobs with no plan file. None for a non-sliced job."""
    if not base:
        return None
    if base in reverse:
        return reverse[base]
    m = re.match(r"^(.+)-(s\d+-.+)$", base)
    if m:
        return m.group(1)
    return None


def _annotate_run_parents(rows, reverse=None, projects=None, progress=None):
    """Tag each run-status row with its parent project so the dashboard can roll
    sliced jobs up under one expandable parent row:
      row['parent']       -- project label, or None for a standalone job
      row['slice_id']     -- the slice id within that project (None if standalone)
      row['parent_total'] -- the project's slice count as the slice INDEX sees it
                          (len of the plan's top-level `order`). FALLBACK ONLY: it is
                          NOT expanded through sub-splits, so it is used for the tree's
                          X/Y only when the authoritative pair below is unreadable.
      row['parent_done'] / row['parent_slices'] -- THE authoritative (X, Y) for the
                          bundle, straight from _load_plan_progress ->
                          _plan_progress_recursive: every slice that was itself
                          escalated into its own finer sub-plan is expanded to its real
                          leaf slices, to any depth. None when the plan state can't be
                          read. the user 2026-09-18: "i want to see total slices not just
                          the initial slices" and "maybe we need to put the data in one
                          place and just pull from there instead of having it split so
                          many places" -- so the SAME (X, Y) that the queue panel's
                          bundle fraction uses (_annotate_plan_rollup) is stamped here
                          and consumed by _build_run_tree/_summarize_rows. There is no
                          second slice-count computation left to drift out of sync.

    row['bundle_incomplete'] -- True when the parent plan still owes slices (X < Y).
                          Used by _build_run_tree to keep a bundle OUT of "good to go
                          -- nothing owed" until the WHOLE bundle is done, even when
                          every FINISHED slice passed (the user 2026-09-18: the good-to-go
                          bar listed bundles whose slices weren't all done yet).

    PURE given reverse/projects; loads them from disk when not supplied. Only ADDS
    fields -- never changes 'group' -- so it composes with _annotate_run_groups."""
    if reverse is None or projects is None:
        reverse, projects = _load_slice_index()
    # progress source: a {parent -> (x, y)} map or a callable(parent)->(x,y)|None for
    # hermetic tests; defaults to reading the slicer's run-state from disk.
    if progress is None:
        _prog_fn = _load_plan_progress
    elif callable(progress):
        _prog_fn = progress
    else:
        _prog_fn = lambda p: progress.get(p)
    _prog_cache = {}

    def _prog(parent):
        """The ONE (X, Y) read per parent -- cached, so the incomplete flag and the
        stamped fraction can never come from two different reads of the plan."""
        if not parent:
            return None
        if parent not in _prog_cache:
            _prog_cache[parent] = _prog_fn(parent)
        return _prog_cache[parent]

    def _incomplete(parent):
        prog = _prog(parent)
        return bool(prog and prog[0] < prog[1])

    for r in rows:
        base = _feature_and_rank(r.get("label"))[0]
        # An escalation review (esc-review-<TS>-<bundle>-<slice> | ...-job-<id>) belongs
        # to the slice it reviews: strip the mangled <TS>- prefix, or resolve the
        # escalated JOB's own label for the job form.
        _ref = _esc_review_ref(r.get("label"))
        if _ref:
            _jid = _esc_review_job_id(r.get("label"))
            if _jid:
                try:
                    _pl = json.loads((q.LOG_DIR / f"{_jid}.done.json").read_text()).get("label")
                except Exception:
                    _pl = None
                base = _feature_and_rank(_pl)[0] if _pl else base
            else:
                base = _ref
        parent = _project_for_base(base, reverse)
        r["parent"] = parent
        if parent and base.startswith(parent + "-"):
            r["slice_id"] = base[len(parent) + 1:]
        else:
            r["slice_id"] = None
        if _ref and parent:
            r["display_label"] = _esc_review_display(
                r.get("label"), parent) if not _esc_review_job_id(r.get("label")) else (
                "Escalation review \u00b7 " + str(r["slice_id"] or base))
        r["parent_total"] = projects.get(parent, {}).get("total") if parent else None
        prog = _prog(parent)
        r["parent_done"] = prog[0] if prog else None
        r["parent_slices"] = prog[1] if prog else None
        r["bundle_incomplete"] = _incomplete(parent)
    return rows


# --- PROJECT layer, one level ABOVE the slice/feature rollup (2026-09-18, the user:
# every bg-* run/slice/piece should collapse under ONE "broker-guard" header, with
# each slice as a sub-group inside it). Dispatch labels are prefixed with a short
# project token -- bg-health-s1-status, aw-scan-s2-fares -- so the token before the
# first '-' IS the project. The map below only supplies a readable NAME for the
# prefixes we know; an unknown prefix falls back to the raw token, so a brand-new
# project still groups correctly the first time it runs.
PROJECT_PREFIX_NAMES = {
    "bg": "broker-guard",
    "aw": "award-hacker",
    "cc": "cc-waitlist",
    "rt": "resell-tracker",
    "arr": "arr-automation",
    "ev": "ev-dashboard",
}
# Tokens that are a JOB KIND, not a project (diag-cloudflare-en0 and diag-rivian-soc
# are unrelated one-offs; grouping them under a "diag" project would be a lie). A row
# whose prefix is one of these only gets a project when the prefix is explicitly
# known above -- otherwise it stays a top-level standalone row, exactly as before.
_NON_PROJECT_PREFIXES = {"diag", "eval", "bakeoff", "probe", "research", "test",
                         "gate", "regate", "auto", "handoff", "slice"}


def _project_prefix(label, parent=None):
    """PURE. ('bg', 'broker-guard') for a row, or (None, None) when it belongs to no
    project. Derived generically from the token before the first '-' of the row's
    project label (when the slice rollup found one) or of its feature base.

    A KNOWN prefix always wins, sliced or not. An UNKNOWN prefix only counts when the
    row is genuinely project-shaped (it has a slice parent) -- that is what keeps
    unrelated one-off diag-/eval- runs from being lumped into a fake project."""
    src = parent or _feature_and_rank(label)[0]
    tok = str(src or "").split("-", 1)[0].strip().lower()
    if not tok:
        return None, None
    if tok in PROJECT_PREFIX_NAMES:
        return tok, PROJECT_PREFIX_NAMES[tok]
    if parent and tok not in _NON_PROJECT_PREFIXES:
        return tok, tok           # unknown project -- group it under its raw prefix
    return None, None


def _annotate_run_projects(rows):
    """Tag each row with `project_key` + `project_name`, the top grouping level:
        project (bg / broker-guard) > feature-slice (bg-health-s1-status) > run.
    Only ADDS fields -- never touches 'group' or the parent rollup -- so it composes
    with _annotate_run_groups and _annotate_run_parents. Run it AFTER the parent
    annotation so a sliced row's project label is available."""
    for r in rows:
        key, name = _project_prefix(r.get("label"), r.get("parent"))
        r["project_key"], r["project_name"] = key, name
    return rows


def _feature_and_rank(label):
    """PURE (mirror of gate-on-complete.classify's _feature_and_rank). Map a stage
    label to (feature_base, stage_rank): auto-author-<base> -> rank 0, auto-refine-
    <base>-r<N> -> rank N, anything else (the terminal/coding row) -> (label, inf)."""
    s = str(label or "")
    # Strip a trailing gate annotation the pipeline appends to a label, e.g.
    # "auto-refine-bg-eraser-s1-invoke-r1 [auto-fix r1]" -- it is not part of the
    # feature identity and would otherwise defeat base/rank parsing (the -rN would
    # no longer be at the end).
    s = re.sub(r"\s*\[[^\]]*\]\s*$", "", s).strip()
    m = re.match(r"^auto-(author|refine)-(.+)$", s)
    if not m:
        return s, float("inf")
    kind, rest = m.group(1), m.group(2)
    rounds = re.findall(r"-r(\d+)", rest)
    base = re.sub(r"(?:-r\d+)+$", "", rest)
    rank = 0 if kind == "author" else (int(rounds[-1]) if rounds else 1)
    return base, rank


def _live_queue_labels(jobs):
    """PURE. The labels of queue jobs that are actually STILL TO RUN.

    `_annotate_run_groups`'s live-labels argument means "features being (re-)worked
    right now", and a row whose feature is in that set is auto-handled with "... is
    re-running in the queue -- wait for the live verdict". `_run_status_jobs` used to
    hand it EVERY job in the queue file, terminal ones included -- so a finished job
    that is still sitting in the queue as `done` made its OWN run-status row wait for
    itself, and the row was filed as auto-handled with a reason that was not true.
    With the queue holding 11 done jobs (2026-09-19), that silently swallowed real
    PASS rows out of the sign-off bar. Terminal jobs are therefore dropped here, at
    the one place that builds the list.

    An ALLOWLIST of still-to-run statuses, not a denylist of terminal ones: the
    denylist only named done/done_unconverged, so every `failed` (and cancelled,
    needs_opus, blocked...) job counted as live and filed its OWN row as "re-running
    in the queue" -- a failed author job sat hidden under auto-handled forever while
    nothing was re-running (the user 2026-09-27: bg-automate-optout-form-submission and
    auto-author-verify-relevance showing failed, never surfaced)."""
    return [j.get("label") for j in (jobs or [])
            if j.get("status") in _QUEUE_LIVE_STATUSES]


# Statuses of a queue job that has not finished yet. Anything else -- done,
# failed, cancelled, needs_opus, blocked, a status added later -- is not live.
_QUEUE_LIVE_STATUSES = frozenset({"pending", "queued", "scheduled", "planned",
                                  "running", "paused", "held"})


def _annotate_run_groups(rows, live_labels=None):
    """Tag every run-status row with a `group` ('needs_eyes' | 'handled') and, when
    handled, a `handled_reason`. NOTHING is hidden or archived here -- this only
    SORTS the one list so the surface can show a small trustworthy 'needs your eyes'
    set above a collapsed, reasoned 'auto-handled' set. FAIL-SAFE: needs_eyes wins on
    any doubt; a row still owing sign-off is ALWAYS needs_eyes.

    Two handled shapes (both no-deliverable / no-human-decision by construction):
      * superseded auto-author/auto-refine STAGE -- a strictly-newer stage of the
        same feature exists (live in the queue OR already finished), so this
        intermediate verdict is about the harness, not the app, and the real verdict
        lands on the newer/terminal row;
      * eval/bake-off/probe measurement ARM (handoff-emit's own predicate)."""
    all_labels = [r.get("label") for r in rows] + list(live_labels or [])
    # (base, rank) for every finished row AND live queue job, so an intermediate stage
    # can be recognised as superseded by a stage that is still running/pending.
    stages = [(lab, *_feature_and_rank(lab)) for lab in all_labels]
    # feature bases that have a job LIVE in the queue right now (running/pending/etc.)
    live_bases = {_feature_and_rank(lab)[0] for lab in (live_labels or []) if lab}

    def _feature_is_live(base):
        # The feature is actively being (re-)worked in the queue: its base is live, or a
        # live base is a deeper nesting of it, or it deeper-nests a live base. A verdict
        # that already finished for a feature currently re-running is not actionable NOW
        # -- wait for the live run's verdict rather than eyeballing the stale one.
        for lb in live_bases:
            if lb == base or lb.startswith(base + "-") or base.startswith(lb + "-"):
                return True
        return False

    def _superseded(base, rank):
        # A newer stage of the SAME feature exists if some other stage has either:
        #  * the exact same base and a strictly-greater rank (a later refine round), OR
        #  * a strictly-DEEPER base (base + '-...') -- the slicer's async re-authoring
        #    nests the feature into a longer label (auto-author-<feat>-s1-create-...),
        #    which is a fresh authoring pass on the same feature, not a new feature.
        for _lab, b, rk in stages:
            if b == base and rk > rank:
                return True
            if b != base and b.startswith(base + "-"):
                return True
        return False

    for r in rows:
        lab = str(r.get("label") or "")
        if r.get("awaiting_signoff"):
            r["group"], r["handled_reason"] = "needs_eyes", ""
            continue
        # A row a newer retry has already replaced is not a problem anyone needs to
        # look at -- the replacement carries the real verdict. Defensive: the field
        # may not be set by anything yet; honour it the moment it is.
        sup = r.get("superseded_by") or r.get("superseded")
        if sup:
            r["group"] = "handled"
            r["handled_reason"] = (f"superseded by {sup}" if isinstance(sup, str)
                                   else "superseded by a newer retry")
            # Display "superseded", not the raw "SKIPPED" -- the stage was intentionally
            # dismissed because a newer one carries the verdict; "SKIPPED" reads like an
            # error/dropped step (the user 2026-09-18). raw_verdict is untouched (color +
            # pass-logic unchanged).
            r["verdict"] = "superseded"
            continue
        base, rank = _feature_and_rank(lab)
        # Feature is back in the queue -> its finished rows (any stage, incl. the
        # terminal one) wait on the live run, not on a human. Fail-safe: sign-off
        # rows already returned above, so this never buries a deliverable.
        if _feature_is_live(base):
            r["group"] = "handled"
            r["handled_reason"] = f"{base} is re-running in the queue -- wait for the live verdict"
            # a finished stage a live re-run has replaced: show "superseded", not the
            # alarming raw "SKIPPED" (the user 2026-09-18). raw_verdict/color untouched.
            if str(r.get("raw_verdict") or "").strip().upper().startswith("SKIP"):
                r["verdict"] = "superseded"
            continue
        if re.match(r"^auto-(author|refine)-", lab):
            if _superseded(base, rank):
                r["group"] = "handled"
                r["handled_reason"] = f"superseded by a newer stage of {base}"
                # see the note above: show "superseded", not the alarming raw "SKIPPED"
                r["verdict"] = "superseded"
                continue
        if ho._label_is_eval(lab) or re.match(r"^toolrel\d*-", lab):
            r["group"] = "handled"
            r["handled_reason"] = "eval/bake-off/probe measurement arm (no deliverable)"
            continue
        # An AUTHOR-ONLY stage whose coding slice never ran is NOT a deliverable
        # (2026-09-18: arr-codec-floor s1-codec-rank sat in the panel as a green
        # "PASS (pending review)" although no application code was ever written and
        # the target file did not exist -- the authoring job only produced TASK/
        # refimpl/verify, and the context-budget gate then refused to enqueue the
        # coding slice). Reaching here means nothing newer exists for this feature
        # (both the live-feature and superseded folds are above), so the pipeline
        # really did stop at the harness. Rewrite the verdict so it can never read
        # as a shipped PASS, and keep it in needs_eyes: it is a stall, not a win.
        blocked = _run_blocked_reason(r, lab)
        if blocked:
            r["group"], r["handled_reason"] = "needs_eyes", ""
            r["blocked_reason"] = blocked
            r["gate_verdict"] = r.get("verdict")
            r["raw_verdict"] = "BLOCKED"
            r["verdict"] = f"BLOCKED -- {blocked}"
            continue
        # A non-gated run (research / diagnosis) carries no code-review verdict -- its
        # output is a report to READ once, not a gate verdict to sign off. A FRESH one
        # still needs eyes, so only fold it once it's had a day to be consumed; this
        # keeps a two-week-old diag-* from sitting in 'needs your eyes' forever.
        if (not r.get("has_gate")) and _older_than_hours(r.get("timestamp"), 24):
            r["group"] = "handled"
            r["handled_reason"] = "research/diagnosis output (no code gate) -- read & clear"
            continue
        # A PASS is GOOD TO GO (2026-09-18, the user: "if it passed, it passed ... i'd
        # like it to show that more clearly"). Sign-off on a passing dispatch is the
        # coordinator's job, not the user's, so a clean PASS must not sit in his "needs
        # your eyes" bucket implying something is wrong. It gets its own visible,
        # green section instead of being folded away. needs_eyes is now strictly
        # CONCERNS / FAIL / ESCALATED / BLOCKED -- rows owing a real decision.
        # Anything still owing an explicit unapproved sign-off returned above, so
        # this can never swallow a gated deliverable.
        if _verdict_is_pass(r):
            r["group"] = "good_to_go"
            r["handled_reason"] = ""
            continue
        r["group"], r["handled_reason"] = "needs_eyes", ""
    return rows


def _verdict_is_pass(row):
    """PURE. True iff this row's verdict is a clean pass (incl. 'pass-pending-review'
    and 'PASS (regate)'). Anything else -- CONCERNS/FAIL/ESCALATED/BLOCKED/PENDING/
    SKIPPED/unknown -- is NOT a pass, so it keeps needing a decision."""
    v = str(row.get("raw_verdict") or row.get("verdict") or "").strip().upper()
    return v.startswith("PASS")


# Markers on a gate payload that mean "the pipeline stopped before any application
# code was written / reviewed": the record is the AUTHORING stage (harness only), or
# a follow-on job the pipeline failed to enqueue at all.
_ENQUEUE_BLOCKED_VALUES = {"enqueue-failed", "enqueue_failed", "blocked"}


def _run_blocked_reason(row, _label=None):
    """PURE. Short human reason this row's verdict is NOT a shipped deliverable, or
    None when it is a normal run. Callers must only apply it to a row with nothing
    newer for its feature -- an author stage followed by its coding slice is simply
    superseded, and is folded before this is ever consulted."""
    stage = str(row.get("auto_pipeline_stage") or "").strip().lower()
    # ONLY the explicit gate-payload marker, never the auto-author- label prefix: a
    # plain authoring label is normal and usually just superseded, and treating every
    # one of them as blocked would flood the panel with false alarms.
    if stage == "author":
        return "authoring stage only, the coding slice never ran"
    for key, what in (("review_enqueue", "review"),
                      ("auto_fix_enqueue_status", "auto-fix"),
                      ("regate", "regate")):
        if str(row.get(key) or "").strip().lower() in _ENQUEUE_BLOCKED_VALUES:
            return f"{what} was never enqueued (enqueue-failed)"
    return None


# What the "N need eyes" badge should actually SAY for one row, and how loud it
# should be (2026-09-18, the user: a green PASS reading "needs eyes" looks like a
# fault). Severity drives the colour: 'ok' green, 'warn' amber, 'bad' red.
_EYES_LABELS = {
    "good": ("good to go", "ok"),
    "signoff": ("awaiting sign-off", "ok"),
    "review": ("needs review", "warn"),
    "blocked": ("blocked -- did not run", "bad"),
}


def _eyes_flavor(row):
    """PURE. Which flavour of attention this row wants:
      'good'    -- passed, nothing owed (shown green, out of the needs-eyes bucket)
      'signoff' -- an explicit unapproved sign-off is owed: "please OK this"
      'blocked' -- the run did not actually deliver (author-only / enqueue-failed)
      'review'  -- a real problem (CONCERNS/FAIL/ESCALATED/unknown verdict)
    """
    if row.get("group") == "good_to_go":
        return "good"
    if row.get("awaiting_signoff"):
        return "signoff"
    if row.get("blocked_reason"):
        return "blocked"
    if _verdict_is_pass(row):
        return "signoff"
    return "review"


def _annotate_eyes_labels(rows):
    """Tag each row with `eyes_flavor` + `eyes_label` + `eyes_severity` so the panel
    can say WHICH kind of attention it wants instead of one alarming "needs eyes"
    for everything. Display only: never changes `group`."""
    for r in rows:
        fl = _eyes_flavor(r)
        label, sev = _EYES_LABELS[fl]
        r["eyes_flavor"], r["eyes_label"], r["eyes_severity"] = fl, label, sev
    return rows


def _summarize_rows(rows, total=None, done=None):
    """PURE. Roll a set of child rows up into ONE header summary -- used at BOTH
    levels of the tree (a project header and a feature/slice header), so the two can
    never disagree about what their children say.

      verdict : WORST-OF children. Any FAIL makes the whole thing FAIL (one broken
                slice means the project is not shippable as-is), then BLOCKED (a run
                that never delivered), then CONCERNS, then PASS only if everything
                passed, else PENDING (nothing gated yet).
      model/host : the value every child agrees on, or "N models"/"N hosts".
      files   : summed across children (the whole diff footprint).
      when    : the most-recent child timestamp (last activity).
      counts  : how many children want which flavour of attention.

    (total, done) is the bundle's AUTHORITATIVE slice fraction, handed down from
    _build_run_tree, which reads it off the rows' parent_slices/parent_done -- stamped
    once by _annotate_run_parents from _plan_progress_recursive. When `done` is given
    it IS slicesDone: no counting of rows, no "trust total when not incomplete"
    heuristic, nothing that can disagree with the queue panel's fraction. `done=None`
    keeps the old row-derived fallback for a group with no readable plan state
    (standalone rows, projects).

    NOTE on `good` vs `slicesDone` -- they measure DIFFERENT things on purpose, and the
    front-end labels them so ("N/M slices done" vs "all good to go (N runs)"):
      slicesDone : SLICES of the plan that are through (from the plan's own state).
      good       : RUN ROWS currently sitting in the good_to_go bucket -- i.e. results
                   that already passed and need nothing from you. A bundle whose
                   passing slices were all auto-handled/archived has good == 0 with
                   slicesDone > 0, which is correct, not a contradiction (the user
                   2026-09-18: "its also showing 1/2 with nothing passing").

    Field NAMES are the ones the front-end renders directly.
    """
    rows = list(rows)
    vc = {"PASS": 0, "FAIL": 0, "CONCERNS": 0, "BLOCKED": 0, "other": 0}
    # LIVE verdict counts exclude auto-handled rows (a superseded/re-run/eval-arm row).
    # The rolled-up VERDICT is computed from these so it can never contradict the flags
    # -- the user 2026-09-18: bg-brokers read "FAIL ... all good to go (2)" because a
    # superseded FAIL was still counted in the verdict while the flags (which honour the
    # handled status) had already moved past it.
    vc_live = {"PASS": 0, "FAIL": 0, "CONCERNS": 0, "BLOCKED": 0, "other": 0}
    counts = {"needs_eyes": 0, "good_to_go": 0, "handled": 0,
              "signoff": 0, "review": 0, "blocked": 0}
    slices, models, hosts = set(), set(), set()
    files_sum, when = 0, None
    for r in rows:
        # A slice is identified by parent+slice_id so two projects' "s1-..." ids can
        # never collide; a standalone row counts as one unit of work on its own.
        if r.get("slice_id"):
            slices.add(f"{r.get('parent') or ''}/{r['slice_id']}")
        elif not r.get("parent"):
            slices.add(f"@{r.get('id')}")
        grp = r.get("group") or "needs_eyes"
        counts[grp] = counts.get(grp, 0) + 1
        if grp == "needs_eyes":
            counts[_eyes_flavor(r)] = counts.get(_eyes_flavor(r), 0) + 1
        t = str(r.get("raw_verdict") or "").upper()
        for k in ("PASS", "FAIL", "CONCERNS", "BLOCKED"):
            if t.startswith(k):
                vc[k] += 1
                if grp != "handled":
                    vc_live[k] += 1
                break
        else:
            vc["other"] += 1
            if grp != "handled":
                vc_live["other"] += 1
        if isinstance(r.get("changed_file_count"), int):
            files_sum += r["changed_file_count"]
        if r.get("model"):
            models.add(r["model"])
        if r.get("host"):
            hosts.add(r["host"])
        ts = r.get("timestamp")
        if ts and (when is None or str(ts) > str(when)):
            when = ts
    parts = []
    for key, word in (("PASS", "pass"), ("CONCERNS", "concerns"),
                      ("FAIL", "fail"), ("BLOCKED", "blocked"), ("other", "other")):
        if vc[key]:
            parts.append(f"{vc[key]} {word}")

    total_out = total if (total is not None and total > 0) else len(slices)
    # A bundle is INCOMPLETE (slices still owed) when any child carries the plan-progress
    # X<Y flag, OR a live (non-handled) run is still non-terminal. See the flags-badge
    # note below -- the SAME signal drives both the badge and the verdict.
    incomplete = (any(r.get("bundle_incomplete") for r in rows) or vc_live["other"] > 0
                  or bool(done is not None and total_out and done < total_out))

    # VERDICT rule (the user 2026-09-18): you cannot pass, partially-pass, or fail a checklist
    # you have not finished. While a bundle is incomplete its verdict is PENDING -- never
    # "PASS (partial)" (a contradiction) and never a premature FAIL. Once complete, the
    # verdict is the worst of the LIVE runs only, so it can never say FAIL while the flags
    # say "all good to go" (that mismatch came from counting a superseded FAIL).
    if incomplete:
        raw, verdict = "PENDING", "PENDING"
    elif vc_live["FAIL"]:
        raw, verdict = "FAIL", "FAIL"
    elif vc_live["BLOCKED"]:
        raw, verdict = "BLOCKED", "BLOCKED"
    elif vc_live["CONCERNS"]:
        raw, verdict = "CONCERNS", "CONCERNS"
    elif vc_live["PASS"]:
        raw, verdict = "PASS", "PASS"
    else:
        raw, verdict = "PENDING", "PENDING"

    def one_or_count(s, noun):
        return list(s)[0] if len(s) == 1 else (f"{len(s)} {noun}" if s else "")

    # slicesDone: the authoritative X when we have one -- the plan's own recursively
    # expanded done-count, the SAME number the queue panel's bundle fraction shows.
    # Only when there is no readable plan state does this fall back to the old
    # row-derived count: len(slices) counts distinct slice_ids that happen to have a
    # run-status ROW in this batch, which undercounts a slice that converged without
    # ever producing its own row (the user 2026-09-18: bg-interpret's s1-prompt), so a
    # complete bundle shows its full total rather than that undercount.
    if done is not None:
        slices_done = min(done, total_out) if total_out else done
    elif not incomplete and total_out:
        slices_done = total_out
    else:
        # Clamped: the row-derived count can OVERSHOOT a total that is itself a
        # guess (a project rollup counts a bundle with no readable plan as 1 while
        # its runs cover several slice ids -- cc read "6/3 slices"). X > Y is never
        # a thing a reader should have to interpret.
        slices_done = min(len(slices), total_out) if total_out else len(slices)

    return {
        "total": total_out,
        "slicesDone": slices_done, "runs": len(rows), "incomplete": incomplete,
        # True when total/slicesDone came from the plan's own recursive state rather
        # than from counting rows -- lets the UI (and the self-test) tell the
        # authoritative fraction apart from the degraded fallback.
        "slicesAuthoritative": done is not None,
        "eyes": counts["needs_eyes"], "good": counts["good_to_go"],
        "signoff": counts.get("signoff", 0), "review": counts.get("review", 0),
        "blocked": counts.get("blocked", 0), "handled": counts["handled"],
        "breakdown": ", ".join(parts),
        "verdict": verdict, "rawVerdict": raw,
        "model": one_or_count(models, "models"), "host": one_or_count(hosts, "hosts"),
        "files": files_sum, "when": when,
    }


# Section a header/row lands in: the WORST of its children. A single child needing
# eyes pulls its whole slice AND its whole project into "needs your eyes" -- nothing
# that wants a decision can hide behind a collapsed green header.
_SECTION_RANK = {"needs_eyes": 0, "good_to_go": 1, "handled": 2}


def _section_for(groups):
    return min(groups, key=lambda g: _SECTION_RANK.get(g, 0)) if groups else "handled"


# A sub-plan's label is its parent plan's label plus the slice id it was split out of
# -- exactly the slice-runs/<parent>-<sid>.json convention _plan_progress_recursive
# expands for the COUNT. This is the same relation, applied to the row NESTING.
_SUBPLAN_SLICE_RE = re.compile(r"^s\d+-")


def _subplan_parent_key(label, candidates):
    """PURE. The bundle `label` is a sub-plan OF, chosen from `candidates` (any
    container of bundle labels), or None.

    LONGEST match wins, so a three-level chain nests one level at a time
    (np-s1-a-s2-y -> np-s1-a -> np) instead of collapsing straight to the root. A
    bundle whose ancestor has no rows in this batch has no candidate and simply stays
    where it is -- nesting never invents a card that has nothing in it."""
    if not label:
        return None
    best = None
    for cand in candidates:
        if cand == label or not label.startswith(cand + "-"):
            continue
        if not _SUBPLAN_SLICE_RE.match(label[len(cand) + 1:]):
            continue
        if best is None or len(cand) > len(best):
            best = cand
    return best


def _build_run_tree(rows):
    """PURE. Nest annotated run-status rows into the panel's hierarchy:

        project (bg / broker-guard)
          > bundle (bg-eraser)
              > individual runs (author / refine rounds)
              > SUB-PLAN bundle (bg-eraser-s1-invoke), to any depth
                  > its runs ...

    Returns the ordered list of TOP-LEVEL entries, first-appearance order preserved
    so the newest work still floats to the top. Each entry is one of:
      {kind: 'row',     row, section}
      {kind: 'parent',  project, total, done, rows[], children[], allRows[],
                        summary, section}
      {kind: 'project', key, name, entries[], rows[], covered[], summary, section}
    A row/slice with no project stays top-level, exactly as it rendered before.

    SUB-PLANS NEST (the user 2026-09-18: "ok but we still don't have complete runs inside
    the batch"). When a slice is escalated and re-split, its sub-plan's runs used to
    render as a SEPARATE top-level bundle card. bg-eraser therefore showed "3/8 done"
    over six stale pre-split attempts and not one of the three converged PASS runs
    that back that 3 -- they were off in the sibling cards bg-eraser-s1-invoke and
    bg-eraser-s2-verify, with nothing tying them to the batch. A sub-plan bundle is
    now a CHILD of the bundle it was split out of (`children`, recursively, arbitrary
    depth -- the same slice-runs/<parent>-<sid> relation the recursive COUNT uses), so
    the parent card contains every real run under it. `rows` stays the bundle's DIRECT
    runs (what renders between the header and the nested cards); `allRows` is the full
    subtree, which is what the summary, the section rollup and bulk-clear use.

    SLICE COUNTS COME FROM EXACTLY ONE PLACE (the user 2026-09-18: "maybe we need to put
    the data in one place and just pull from there instead of having it split so many
    places"). This function does NOT compute a slice total of its own any more: it
    reads the (X, Y) that _annotate_run_parents stamped on each row as
    parent_done/parent_slices, which came from _plan_progress_recursive -- the same
    source the queue panel's bundle fraction uses. `parent_total` (the plan index's
    flat top-level count, which is what used to be shown and is what made bg-eraser
    read "1/2" while 8 real leaf slices existed under two sub-splits) survives ONLY as
    the degraded fallback for a bundle whose plan state cannot be read. Still PURE:
    everything it needs is already on the rows."""
    # Level 1 -- the per-feature/slice parent rollup.
    order, by_parent = [], {}
    for r in rows:
        p = r.get("parent")
        if p:
            e = by_parent.get(p)
            if e is None:
                e = {"kind": "parent", "project": p, "rows": [], "children": [],
                     "slice_of": None, "slice_id": None,
                     "total": r.get("parent_total"), "done": None, "authoritative": False}
                by_parent[p] = e
                order.append(e)
            e["rows"].append(r)
            if r.get("parent_slices"):
                # authoritative: recursive leaf-slice count + how many are through
                e["total"] = r["parent_slices"]
                e["done"] = r.get("parent_done") or 0
                e["authoritative"] = True
            elif not e["authoritative"] and r.get("parent_total") is not None:
                e["total"] = r["parent_total"]
        else:
            order.append({"kind": "row", "row": r})
    # Level 1b -- fold each escalated slice's own sub-plan INTO the bundle it came
    # from, deepest link first (longest-prefix match), so a chain nests one level per
    # split rather than flattening. Done before the project layer, so a nested bundle
    # is no longer a top-level entry anywhere.
    nested = set()
    for e in order:
        if e["kind"] != "parent":
            continue
        anc = _subplan_parent_key(e["project"], by_parent)
        if anc is None:
            continue
        e["slice_of"] = anc
        e["slice_id"] = e["project"][len(anc) + 1:]
        by_parent[anc]["children"].append(e)
        nested.add(e["project"])
    order = [e for e in order
             if not (e["kind"] == "parent" and e["project"] in nested)]
    # Level 0 -- the project layer above it.
    top, by_project = [], {}
    for e in order:
        kids = e["rows"] if e["kind"] == "parent" else [e["row"]]
        key = kids[0].get("project_key")
        if not key:
            top.append(e)
            continue
        pe = by_project.get(key)
        if pe is None:
            pe = {"kind": "project", "key": key,
                  "name": kids[0].get("project_name") or key, "entries": []}
            by_project[key] = pe
            top.append(pe)
        pe["entries"].append(e)

    def finish(e):
        if e["kind"] == "row":
            e["section"] = e["row"].get("group") or "needs_eyes"
            return [e["row"]]
        if e["kind"] == "parent":
            # The bundle's OWN runs, then everything its nested sub-plans contain.
            # `rows` stays the direct runs (the UI renders those, then the child
            # cards); `allRows` is the subtree the summary/section/bulk-clear see, so
            # a needs-eyes run three splits down still pulls the whole batch open.
            kids = list(e["rows"])
            for c in e["children"]:
                kids += finish(c)
            e["allRows"] = kids
        else:
            kids = []
            for sub in e["entries"]:
                kids += finish(sub)
            e["rows"] = kids
            # Project total = sum of its bundles. No dedup arithmetic is needed any
            # more: a sub-plan bundle is now NESTED inside the bundle it was split out
            # of, so it is not in `entries` at all and cannot be added twice (that
            # double count is what made the bg header read 40). `covered` names the
            # bundles that were folded in, purely so this stays inspectable.
            cov = []

            def _walk(x):
                for c in x["children"]:
                    cov.append(c["project"])
                    _walk(c)

            for sub in e["entries"]:
                if sub["kind"] == "parent":
                    _walk(sub)
            e["covered"] = sorted(cov)
            # Sum the bundles' RESOLVED totals (summary.total), not their raw ones: a
            # bundle with no readable plan state has total None, and counting it as 1
            # made the cc project header read "1/1" over a child card showing 6/6.
            # summary.total is that same fallback already resolved, so the two levels
            # cannot disagree.
            e["total"] = sum(sub["summary"]["total"] if sub["kind"] == "parent" else 1
                             for sub in e["entries"])
            # Project-level X is authoritative only when every counted bundle is; a
            # standalone run counts as one unit of work, through iff it is finished.
            if e["entries"] and all(sub.get("authoritative") for sub in e["entries"]
                                    if sub["kind"] == "parent"):
                e["done"] = sum((sub.get("done") or 0) if sub["kind"] == "parent"
                                else (1 if sub["row"].get("group") in
                                      ("handled", "good_to_go") else 0)
                                for sub in e["entries"])
                e["authoritative"] = any(sub.get("authoritative") for sub in e["entries"]
                                         if sub["kind"] == "parent")
            else:
                e["done"], e["authoritative"] = None, False
        e["summary"] = _summarize_rows(kids, e.get("total"),
                                       e.get("done") if e.get("authoritative") else None)
        e["section"] = _section_for([r.get("group") or "needs_eyes" for r in kids])
        # A bundle that still owes slices (X < Y) may NOT read "good to go -- nothing
        # owed", even when every FINISHED slice passed: its not-yet-run slices simply
        # have no rows here, so the pass-only children would otherwise float it green.
        # Keep it in needs_eyes until the WHOLE bundle is done (the user 2026-09-18), the
        # same "don't call it complete while slices are owed" clamp the queue rollup uses.
        if e["section"] == "good_to_go" and any(r.get("bundle_incomplete") for r in kids):
            e["section"] = "needs_eyes"
        return kids

    for e in top:
        finish(e)
    return top


def _older_than_hours(ts, hours):
    """True iff timestamp ts (ISO8601 Z) is more than `hours` old. Unparseable/missing
    -> False (fail toward needs_eyes: never fold something we can't date)."""
    if not ts:
        return False
    try:
        t = datetime.strptime(str(ts), "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return False
    return (datetime.now(timezone.utc) - t).total_seconds() > hours * 3600


def _clear_manifest_append(archive_dir, entry):
    """Append one clear event to LOG_DIR/archive/clear-manifest.json (a JSON list),
    so every archived-by-clear job is recoverable by hand with a record of when and
    why. Kept separate from the bulk _archive_manifest_*.json the backlog sweep
    wrote, so the two never collide. Best-effort: a manifest write failure does not
    undo the archive (the files are already moved and still recoverable from disk)."""
    mpath = archive_dir / "clear-manifest.json"
    try:
        existing = json.loads(mpath.read_text()) if mpath.exists() else []
        if not isinstance(existing, list):
            existing = []
    except (OSError, ValueError):
        existing = []
    existing.append(entry)
    try:
        mpath.write_text(json.dumps(existing, indent=1))
    except OSError:
        pass


def _archive_run(job_id, override=None, how=None):
    """Clear/handled: move a completed job's durable sidecars out of the active
    LOG_DIR into LOG_DIR/archive/, dropping it off the run-status list.

    Archives exactly what the backlog sweep did -- the <id>.done.json / .gate.json /
    .diff sidecars -- and NOT the run transcript log, so a livelog stays inspectable.

    EXCEPTION (2026-09-22, the user: 3 research jobs -- rt-cc-sync-diagnosis,
    rt-churning-research, rt-price-apis-research -- kept reappearing after
    "{ok: true, moved: []}"): a job with NEITHER a .done.json NOR a .gate.json is a
    log-only research/diagnosis job predating the durable sidecars (see
    q._log_only_result). For that shape, q._iter_job_results' *.log fallback glob
    (matched against LABEL_RESEARCH_RE) is the ONLY thing making the id enumerable
    at all -- so the run log (and any cached <id>.answer.md) IS this job's sidecar
    of record, not an inspectable extra, and it MUST move too or "clear" is a no-op
    that silently reports success while leaving the row's only durable trace right
    where _iter_job_results will find it again. This is added ONLY when done_json/
    gate_json are both absent, so a normal coding job's livelog is still left in
    place exactly as before.

    SIGN-OFF IS PRESERVED: if the job still needs a REQUIRED sign-off that is not
    approved, this REFUSES (returns awaiting_signoff) unless `override` (a reason
    string) is given -- clearing must never silently erase the one thing the
    sign-off gate exists to hold back. The refusal reuses handoff-emit's own
    signoff_blocks_acting so it matches the CLI's behaviour exactly.

    Returns a dict: {ok: True, id, moved:[...]} on success, or
    {ok: False, code, error, ...} which the handler maps to an HTTP status."""
    ld = q.LOG_DIR
    res = q._load_job_result(job_id, ld)
    if res is None:
        return {"ok": False, "code": 404, "error": "no durable result for this job id"}
    label = res.get("label")
    block = ho.signoff_blocks_acting(job_id, label)
    if block and not override:
        return {"ok": False, "code": 409, "awaiting_signoff": True, "error": block}
    archive = ld / "archive"
    archive.mkdir(parents=True, exist_ok=True)
    srcs = [ld / f"{job_id}.done.json", ld / f"{job_id}.gate.json", ld / f"{job_id}.diff"]
    if not res.get("done_json") and not res.get("gate_json"):
        # Log-only job: the run log is what makes it enumerable, so it has to move
        # for the clear to be durable. See the EXCEPTION note above.
        if res.get("run_log"):
            srcs.append(Path(res["run_log"]))
        srcs.append(ld / f"{job_id}.answer.md")
    moved = []
    for src in srcs:
        if src.exists():
            # replace() is atomic within one filesystem; a name clash in archive/
            # (re-clear of a restored job) is overwritten, which is fine -- it is
            # the same job's newer copy.
            src.replace(archive / src.name)
            moved.append(src.name)
    entry = {"archived_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
             "id": job_id, "label": label, "files": moved, "how": how or "cleared from dashboard"}
    if block and override:
        entry["how"] = "cleared from dashboard (SIGN-OFF OVERRIDDEN)"
        entry["signoff_override_reason"] = override
    _clear_manifest_append(archive, entry)
    return {"ok": True, "id": job_id, "moved": moved,
            "signoff_overridden": bool(block and override)}


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

        # Process each metrics line (across all producers). One bad line must never
        # take the whole endpoint down: a non-object line, a `"web_search": null`
        # field or a non-numeric count used to escape as TypeError/AttributeError
        # and 500 the request, blanking the usage table for every poll after it.
        for line in lines:
            try:
                data = json.loads(line.strip())
                if not isinstance(data, dict) or "timestamp" not in data:
                    continue

                # Parse timestamp to a UTC-aware datetime. The worker writes
                # ISO offset form ("...+00:00"); older rows may use a "Z"
                # suffix. datetime.fromisoformat handles both (normalise Z).
                timestamp_str = str(data["timestamp"])
                try:
                    dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                except ValueError:
                    continue  # Skip malformed timestamp
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                dt = dt.astimezone(timezone.utc)

                line_date = (dt.year, dt.month, dt.day)

                web_search_data = data.get("web_search") or {}
                if not isinstance(web_search_data, dict):
                    continue
                counts = {}
                for backend in total_counts:
                    v = web_search_data.get(backend, 0)
                    counts[backend] = int(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else 0

                for backend, n in counts.items():
                    total_counts[backend] += n
                if line_date == today_date:
                    for backend, n in counts.items():
                        today_counts[backend] += n
                # within the last 7 days (inclusive)
                if (now - dt.timestamp()) / (24 * 3600) <= 7:
                    for backend, n in counts.items():
                        last7d_counts[backend] += n

            except (json.JSONDecodeError, KeyError, ValueError, TypeError, AttributeError):
                # Skip malformed lines
                continue

    except OSError:
        # File not accessible or other OS error - return all-zero counts
        pass
    
    return {"total": total_counts, "today": today_counts, "last7d": last7d_counts}


def _reorder_job(jobs, job_id, before_id):
    """Move job_id to just before before_id within jobs (in place, pure list
    mutation -- no I/O, no lock), or to the END of the list when before_id is
    falsy. Returns True if job_id was found and moved, False otherwise.

    Factored out of Handler._move (2026-09-18, the dashboard's new "send to
    bottom" button) so --self-test can prove the before_id=None -> append-to-end
    semantics directly, without going through the full HTTP+flock plumbing --
    the frontend's send-to-bottom button reuses the existing POST /api/jobs/move
    with before_id: null rather than adding a new endpoint, precisely because
    this was already true."""
    moving = next((j for j in jobs if j["id"] == job_id), None)
    if moving is None:
        return False
    jobs.remove(moving)
    if before_id:
        idx = next((i for i, j in enumerate(jobs) if j["id"] == before_id), len(jobs))
        jobs.insert(idx, moving)
    else:
        jobs.append(moving)
    return True


# --- Response cache for the heavy read endpoints (2026-09-22) -------------------
# /api/runs and /api/runs/tree re-derive the whole run-status list from the durable
# sidecars on EVERY poll (~0.45s each, measured), and every open dashboard tab polls
# them every 5s. With several tabs open the requests overlap, each one a thread
# holding a socket and opening sidecar files behind the state flock -- which is the
# pile-up that pushed the process past launchd's 256-fd soft limit on 2026-09-21
# (see _raise_fd_limit and ollama-queue.py's _Locked.load). A short TTL plus
# single-flight (one thread computes, the others wait for its result) bounds the
# concurrent work to one computation per endpoint per TTL, whatever the tab count.
# Every state-changing request (POST/DELETE) drops the cache so an action is
# visible on the very next poll; the TTL is shorter than the poll interval anyway.
_RESP_CACHE_TTL = 2.0
_RESP_CACHE = {}          # key -> (monotonic_at, value)
_RESP_CACHE_LOCKS = {}    # key -> threading.Lock (single-flight per key)
_RESP_CACHE_GUARD = threading.Lock()


def _livelog_path(job, job_id, live_dir=None, log_dir=None):
    """The transcript to show for `job_id`: its recorded live log, else (the row was
    reaped, or never recorded one) the durable files by id -- LIVE_LOG_DIR's
    <id>-*.livelog, then LOG_DIR's <id>-*.log run transcript (and LOG_DIR/archive).
    Only a plain hex id is globbed, so a crafted id can never escape the log dirs."""
    p = (job or {}).get("live_log_path")
    if p and os.path.isfile(p):
        return p
    if not re.match(r"^[0-9a-f]{6,32}$", str(job_id or "")):
        return None
    live_dir = Path(live_dir) if live_dir else q.LIVE_LOG_DIR
    log_dir = Path(log_dir) if log_dir else q.LOG_DIR
    for d, pat in ((live_dir, f"{job_id}-*.livelog"), (log_dir, f"{job_id}-*.log"),
                   (log_dir / "archive", f"{job_id}-*.log")):
        try:
            hits = sorted(d.glob(pat), key=lambda x: x.stat().st_mtime, reverse=True)
        except OSError:
            hits = []
        if hits:
            return str(hits[0])
    return None


def _triage_summary():
    """{'open': N, 'repeats': M, 'oldest': ts} from the triage handoff index. Never raises."""
    try:
        pb = str(QUEUE_PATH.parent)
        if pb not in sys.path:
            sys.path.append(pb)
        import triage_packets as _tp
        return _tp.open_summary()
    except Exception:
        return {"open": 0, "repeats": 0, "oldest": None}


def _bundle_view_lib():
    """bundle_view lives next to this file (the repo's src/); dispatch_progress is a
    shared PIPELINE lib (written by preflight / verify-relevance) that stays in the
    pipeline bin dir (QUEUE_PATH.parent, i.e. ~/bin) -- searched after this dir."""
    here = str(Path(__file__).resolve().parent)
    if here not in sys.path:
        sys.path.insert(0, here)
    pipeline_bin = str(QUEUE_PATH.parent)
    if pipeline_bin not in sys.path:
        sys.path.append(pipeline_bin)
    import bundle_view as _bv
    import dispatch_progress as _dp
    return _bv, _dp


LIVE_BUNDLE_STATUSES = ("pending", "running")


def _retired_bundle_keys(keys, jobs, gkey, superseded=None, cancelled=None):
    """PURE. Which of `keys` are RETIRED: marked superseded (`qctl supersede`:
    bundle-superseded.json, whole-bundle key) or human-cancelled (`cancelled(key)`:
    plan_cancel's `.cancelled` marker, own or an ancestor plan's) AND with no LIVE job
    (pending/running) left. Such a bundle is finished business: it must not show in the
    live Queue panel or raise Needs attention -- it belongs to the finished/stalled
    history. ROOT CAUSE (2026-10-08): replay-endorse, cancelled 2026-10-05, sat in the
    Queue as "pending 5/9 slices, 0 jobs" because 21 DONE rows were still in
    queue-state; only the history path consulted the markers. A bundle with a live
    job is never retired here (a marker must not hide running work). `gkey(job)` is the
    queue's grouping authority; a job's `bundle` tag also counts as its bundle."""
    sup = superseded or {}
    live = set()
    for j in jobs or []:
        if str(j.get("status") or "") not in LIVE_BUNDLE_STATUSES:
            continue
        try:
            k = gkey(j)
        except Exception:
            k = None
        if k:
            live.add(k)
        t = j.get("bundle")
        if isinstance(t, str) and t.strip():
            live.add(t.strip())
    out = set()
    for k in keys:
        if k in live:
            continue
        try:
            canc = cancelled(k) if cancelled else None
        except Exception:
            canc = None
        if k in sup or canc:
            out.add(k)
    return out


def _bundle_views(state=None, runs_dir=None, chain_dir=None, log_dir=None,
                  heal_path=None, preflight_dir=None, progress=None, alive=None,
                  now=None, history=None, live_dir=None):
    """Dashboard B payload: {views: {plan: bundle_view}, activity: [...], active}.

    A view is built for every plan key that has a queue row or is committed/parked,
    from LIVE truth (bundle_view.load_view: run file + queue rows + gate sidecars +
    off-GPU progress + the AUTO driver's chain record + the self-heal ledger), so the
    header N/M and each slice's phase do not wait for the slicer driver's next poll.
    Each slice's HISTORY also carries its FINISHED runs (author, refine rounds, coding,
    gate, regate, second opinion, escalation review, landed) rebuilt from the durable
    livelogs + sidecars by bundle_view.load_history -- the queue prunes a finished row
    on the next tick, so views built from queue-state alone showed only ACTIVE jobs
    (the user 2026-10-05). `history` injects those records (tests); None reads them.
    `activity` is what is happening right now, GPU or not: running jobs plus every
    live dispatch_progress record (preflight / verify-relevance). Best-effort: a
    missing lib or unreadable file yields an empty payload, never a 500."""
    now = time.time() if now is None else now
    if state is None:
        with q._Locked() as lock:
            state = lock.load()
    try:
        bv, dp = _bundle_view_lib()
    except Exception:
        return {"views": {}, "activity": [], "active": None}
    jobs = list(state.get("jobs") or [])
    runs_dir = Path(runs_dir) if runs_dir else SLICE_RUNS_DIR
    chain_dir = Path(chain_dir) if chain_dir else Path(os.environ.get(
        "OLLAMA_DISPATCH_AUTO_RUNS_DIR", Path.home() / ".ollama-dispatch" / "auto-runs"))
    log_dir = Path(log_dir) if log_dir else q.LOG_DIR
    heal_path = heal_path or (Path.home() / ".ollama-dispatch" / "escalations"
                              / "self-heal.json")
    preflight_dir = preflight_dir or Path(os.environ.get(
        "OLLAMA_PREFLIGHT_LEDGER", q.LOG_DIR / "preflight-ledger"))
    alive = alive or dp._alive
    progress = dp.read_all(now=now, alive=alive) if progress is None else progress
    try:
        reverse = q.slice_group_index()
    except Exception:
        reverse = {}
    if history is None:
        try:
            history = bv.load_history(Path(live_dir) if live_dir else q.LIVE_LOG_DIR,
                                      log_dir)
        except Exception:
            history = []
    live_ids = {j.get("id") for j in jobs}
    history = [h for h in history if h.get("id") not in live_ids]
    keys = set()
    for j in jobs:
        try:
            k = q.job_group_key(j, reverse)
        except Exception:
            k = None
        if k:
            keys.add(k)
    active = _active_bundle_key(state, now)
    keys |= {k for k in [active, *(state.get("_bundle_parked") or {}).keys()] if k}
    views = {}
    for k in sorted(keys):
        try:
            v = bv.load_view(k, jobs + bv.history_for(k, history), runs_dir,
                             chain_dir, log_dir, heal_path, preflight_dir, progress,
                             alive, now=now)
        except Exception:
            v = None
        if v:
            views[k] = v
    # `--bundle <name>` jobs with NO slicer plan: still one bundle, one pseudo-slice per
    # job, with its gate/regate/secondop/esc-review children nested under it.
    try:
        _tagged = {}
        for j in jobs:
            t = j.get(q.BUNDLE_FIELD)
            if isinstance(t, str) and t.strip() and t.strip() not in views \
                    and not t.strip().startswith("esc-review-") \
                    and not (bv.esc_review_ref(j.get("label"))
                             and bv.child_parent_id(j) is None) \
                    and q.job_group_key({"label": j.get("label")}, reverse) not in views:
                _tagged.setdefault(t.strip(), []).append(j)
        for t, tj in _tagged.items():
            # + this bundle's FINISHED jobs (durable bundle tag) and their children
            tj = tj + [h for h in history if h.get(q.BUNDLE_FIELD) == t
                       and bv.child_parent_id(h) is None
                       and not bv.esc_review_ref(h.get("label"))]
            ids_t = {j.get("id") for j in tj}
            kids = [j for j in jobs + history
                    if j not in tj and bv.child_parent_id(j) in ids_t]
            def _lab(i):
                try:
                    return {"label": json.loads((log_dir / f"{i}.done.json").read_text()).get("label")}
                except Exception:
                    return {}
            v = bv.build_job_view(t, tj + kids, now=now, result_of=_lab)
            if v:
                views[t] = v
    except Exception:
        pass
    # RETIRED bundles (superseded / human-cancelled, nothing pending or running) leave
    # the live panel -- see _retired_bundle_keys. Best-effort: never raise into a poll.
    try:
        def _gk_live(j):
            return q.job_group_key(j, reverse)

        def _canc(k):
            import plan_cancel as _pc
            return _pc.cancelled(k, runs_dir=runs_dir)
        for k in _retired_bundle_keys(list(views), jobs, _gk_live,
                                      bv.load_superseded(), _canc):
            views.pop(k, None)
    except Exception:
        pass
    activity = []
    # each running job's OWN bundle (never the focused one): annotate copies of the
    # rows through the same grouping authority, and keep it only if a real bundle view
    # exists for that key.
    try:
        _arows = _annotate_job_groups([dict(j) for j in jobs], log_dir)
        _gk = {r.get("id"): r.get("group_key") for r in _arows}
    except Exception:
        _gk = {}
    for j in jobs:
        if j.get("status") != "running":
            continue
        t = bv._ts(j.get("launched_at"))
        _k = _gk.get(j.get("id"))
        activity.append({"kind": "gpu", "id": j.get("id"), "label": j.get("label"),
                         "group_key": _k if _k in views else None,
                         "display": _esc_review_display(j.get("label"), _k if _k in views else None),
                         "model": _display_model(j.get("model"), j.get("lane") or j.get("host_pref")), "host": j.get("lane") or j.get("host_pref"),
                         "elapsed_s": round(now - t, 1) if t else None})
    for r in progress:
        t = float(r.get("started_at") or now)
        activity.append({"kind": "cpu", "tool": r.get("tool"), "what": dp.describe(r),
                         "wt": Path(str(r.get("wt") or "")).name,
                         "elapsed_s": round(now - t, 1)})
    return {"views": views, "activity": activity, "active": active}


# --- FINISHED bundles (the user 2026-10-05: "I want to see the full history of each
# slice ... done ones included, grouped under their slice/bundle") ---
# _bundle_views covers bundles that still have a queue row. Once a bundle's last row
# is pruned it used to vanish from the queue panel entirely; this rebuilds it from the
# same durable history (bundle_view.load_history), one view per bundle key, NEWEST
# ACTIVITY FIRST. Bounded so the page never grows without limit: only bundles active
# in the last `days` (0 = any age), `limit` per page from `offset`, with `total` and
# `has_more` so the UI can offer "show more" / "show all".
FINISHED_BUNDLE_DAYS = 3
FINISHED_BUNDLE_PAGE = 20
_FIN_VIEW_MEMO = {}          # (key, last_activity, runs) -> (monotonic ts, view); live path only
_FIN_VIEW_MEMO_TTL = 600.0   # a full rebuild of every bundle takes ~10s cold; keyed on last_activity+runs, so new work busts it


def _finished_bundle_views(days=FINISHED_BUNDLE_DAYS, limit=FINISHED_BUNDLE_PAGE,
                           offset=0, state=None, history=None, runs_dir=None,
                           log_dir=None, chain_dir=None, now=None):
    now = time.time() if now is None else now
    state_arg, history_arg = state, history
    try:
        days = max(0.0, float(days))
    except (TypeError, ValueError):
        days = float(FINISHED_BUNDLE_DAYS)
    try:
        limit = max(1, min(500, int(limit)))
        offset = max(0, int(offset))
    except (TypeError, ValueError):
        limit, offset = FINISHED_BUNDLE_PAGE, 0
    out = {"views": [], "total": 0, "days": days, "limit": limit, "offset": offset,
           "has_more": False, "stalled": [], "stalled_total": 0,
           "stalled_actionable": 0, "stale_total": 0,
           "superseded": [], "superseded_total": 0}
    if state is None:
        with q._Locked() as lock:
            state = lock.load()
    try:
        bv, dp = _bundle_view_lib()
    except Exception:
        return out
    log_dir = Path(log_dir) if log_dir else q.LOG_DIR
    runs_dir = Path(runs_dir) if runs_dir else SLICE_RUNS_DIR
    chain_dir = Path(chain_dir) if chain_dir else Path(os.environ.get(
        "OLLAMA_DISPATCH_AUTO_RUNS_DIR", Path.home() / ".ollama-dispatch" / "auto-runs"))
    if history is None:
        try:
            history = bv.load_history(q.LIVE_LOG_DIR, log_dir)
        except Exception:
            history = []
    jobs = list(state.get("jobs") or [])
    try:
        reverse = q.slice_group_index()
    except Exception:
        reverse = {}

    def gkey(j):
        try:
            return q.job_group_key(j, reverse)
        except Exception:
            return None
    # a bundle that still has ANY queue row is the queue panel's (_bundle_views), not ours
    live_keys = {k for k in (gkey(j) for j in jobs) if k}
    live_keys |= {k for k in [_active_bundle_key(state, now),
                              *(state.get("_bundle_parked") or {}).keys()] if k}
    live_ids = {j.get("id") for j in jobs}
    history = [h for h in history if h.get("id") not in live_ids]
    by_id = {h.get("id"): h for h in history}
    members, last = {}, {}
    for h in history:
        pid = bv.child_parent_id(h)
        if pid is not None:
            parent = by_id.get(pid)
            k = gkey(parent) if parent else None
        elif bv.esc_review_ref(h.get("label")):
            k = h.get(q.BUNDLE_FIELD)   # its stamped bundle, else it rides history_for
        else:
            k = gkey(h)
        if not k or k in live_keys:
            continue
        members.setdefault(k, []).append(h)
        last[k] = max(last.get(k, 0), h.get("finished_at") or 0)
    cutoff = now - days * 86400 if days else None
    heal_path = Path.home() / ".ollama-dispatch" / "escalations" / "self-heal.json"
    preflight_dir = Path(os.environ.get("OLLAMA_PREFLIGHT_LEDGER",
                                        q.LOG_DIR / "preflight-ledger"))

    def _lab(i):
        h = by_id.get(i)
        if h and h.get("label"):
            return {"label": h["label"]}
        try:
            return {"label": json.loads((log_dir / f"{i}.done.json").read_text()).get("label")}
        except Exception:
            return {}

    attn = bv.load_attention()
    sup = bv.load_superseded()   # `qctl supersede` markers (read each call: tiny file)

    def _plan_cancel_rec(k):
        # a human `ollama-dispatch-slice --cancel` marker is terminal (plan_cancel)
        try:
            import plan_cancel as _pc
            return _pc.cancelled(k, runs_dir=runs_dir)
        except Exception:
            return None

    def build(k):
        recs = members[k]
        v = None
        try:
            v = bv.load_view(k, bv.merge_history(recs, bv.history_for(k, history)),
                             runs_dir, chain_dir, log_dir, heal_path, preflight_dir, [],
                             dp._alive, now=now)
        except Exception:
            v = None
        if not v:
            try:
                v = bv.build_job_view(k, recs, now=now, result_of=_lab)
            except Exception:
                v = None
        if not v:
            return None
        starts = [h.get("launched_at") for h in recs if h.get("launched_at")]
        outcome, nbad = bv.bundle_outcome(v, sup, _plan_cancel_rec(k))
        # FINISHED = every slice done/landed/skipped. A bundle with nothing live that
        # failed (or still owes slices) is STALLED, not finished (Penn 2026-10-06).
        v.update(finished=(outcome == "finished"), outcome=outcome, failed_slices=nbad,
                 last_activity=last[k],
                 first_activity=min(starts) if starts else None, runs=len(recs))
        return v

    live_path = state_arg is None and history_arg is None
    memo = _FIN_VIEW_MEMO if live_path else {}
    for mk in [mk for mk, (t, _v) in memo.items() if time.monotonic() - t > _FIN_VIEW_MEMO_TTL]:
        memo.pop(mk, None)

    def view_of(k):
        mk = (k, last[k], len(members[k]))
        hit = memo.get(mk)
        if hit and time.monotonic() - hit[0] <= _FIN_VIEW_MEMO_TTL:
            return hit[1]
        v = build(k)
        memo[mk] = (time.monotonic(), v)
        return v
    # Stalled bundles are NEVER bounded by the age window or the page (a failure must
    # not age out of sight), so every bundle is classified; views are memoised briefly
    # because that means building them all.
    ordered = sorted(members, key=lambda k: -last[k])
    stalled, fin_keys, superseded = [], [], []
    in_window = lambda k: cutoff is None or last[k] >= cutoff
    for k in ordered:
        v = view_of(k)
        if not v:
            continue
        # markers can change inside the memo TTL: classify from the live marker set
        v["outcome"], v["failed_slices"] = bv.bundle_outcome(v, sup, _plan_cancel_rec(k))
        v["finished"] = v["outcome"] == "finished"
        if v["outcome"] == "superseded":
            superseded.append({"key": k, **(sup.get(k) or _plan_cancel_rec(k) or {})})
        elif v["outcome"] != "finished":
            v["actionable"] = bv.is_actionable(k, last[k], now, attn)
            stalled.append(v)
        elif in_window(k):
            fin_keys.append(k)
    out["total"] = len(fin_keys)
    out["has_more"] = offset + limit < len(fin_keys)
    for k in fin_keys[offset:offset + limit]:
        out["views"].append(view_of(k))
    out["stalled"] = stalled
    out["stalled_total"] = len(stalled)
    out["stalled_actionable"] = sum(1 for v in stalled if v.get("actionable"))
    out["stale_total"] = len(stalled) - out["stalled_actionable"]
    out["superseded"] = superseded
    out["superseded_total"] = len(superseded)
    return out


def _cached_response(key, fn, ttl=_RESP_CACHE_TTL):
    """fn() at most once per `ttl` seconds per key; concurrent callers share the
    one in-flight computation instead of each running their own."""
    now = time.monotonic()
    hit = _RESP_CACHE.get(key)
    if hit and now - hit[0] < ttl:
        return hit[1]
    with _RESP_CACHE_GUARD:
        lock = _RESP_CACHE_LOCKS.setdefault(key, threading.Lock())
    with lock:
        hit = _RESP_CACHE.get(key)
        if hit and time.monotonic() - hit[0] < ttl:
            return hit[1]
        val = fn()
        _RESP_CACHE[key] = (time.monotonic(), val)
        return val


def _invalidate_response_cache():
    _RESP_CACHE.clear()


def _raise_fd_limit(target=10240):
    """Lift the soft RLIMIT_NOFILE to the hard limit (capped at `target`).

    launchd starts this server with the default soft limit of 256 descriptors
    (`launchctl limit maxfiles` -> 256 / unlimited). One thread per request, each
    holding a socket and reading sidecar files, is enough to exhaust that under
    ordinary dashboard polling -- 97 `[Errno 24] Too many open files` lines in
    /tmp/ollama-queue-api.log on 2026-09-21, one of which landed inside
    ollama-queue.py's state read and quarantined a valid state file. Returns the
    (soft, hard) pair now in force; never raises."""
    try:
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        want = target if hard == resource.RLIM_INFINITY else min(hard, target)
        if soft < want:
            try:
                resource.setrlimit(resource.RLIMIT_NOFILE, (want, hard))
            except (ValueError, OSError):
                # macOS refuses anything above OPEN_MAX even when hard is unlimited;
                # fall back a step rather than staying at 256.
                resource.setrlimit(resource.RLIMIT_NOFILE, (min(want, 4096), hard))
        return resource.getrlimit(resource.RLIMIT_NOFILE)
    except Exception as e:  # pragma: no cover -- never let this stop the server
        print(f"[queue-api] WARNING: could not raise RLIMIT_NOFILE: {e}", file=sys.stderr)
        return None


_CHAT_DIR = os.path.dirname(os.path.abspath(__file__))  # dashboard_chat.py lives beside this file


def _chat_module():
    """Import dashboard_chat lazily so a missing/broken module only disables chat."""
    import importlib
    if _CHAT_DIR not in sys.path:
        sys.path.insert(0, _CHAT_DIR)
    return importlib.import_module("dashboard_chat")


class Handler(http.server.BaseHTTPRequestHandler):
    # A client that connects and then stalls (a backgrounded phone tab, a proxy
    # holding the socket) otherwise pins a thread and a descriptor forever --
    # part of the same fd pile-up. The server speaks HTTP/1.0 (no keep-alive), so
    # this only ever bounds the wait for the request line/body.
    timeout = 30

    def _json(self, obj, status=200):
        body = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        # Rides along on the polls an open tab already makes, so a stale front-end
        # can notice itself without any extra request. See FRONTEND_VERSION.
        self.send_header("X-Frontend-Version", FRONTEND_VERSION)
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

    def _chat(self, method):
        """Delegate /chat and /api/chat/* to dashboard_chat.handle (server-independent
        router in ~/Desktop/GitHub Projects/dashboard-chat). Any failure is a 5xx
        here, never an exception that could take the queue API's handler down."""
        try:
            import urllib.parse
            u = urllib.parse.urlsplit(self.path)
            query = {k: v[0] for k, v in urllib.parse.parse_qs(u.query).items()}
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length) if length else b""
            mod = _chat_module()
            status, headers, out = mod.handle(method, u.path, query, body)
        except Exception as e:  # noqa: BLE001
            return self._text("chat unavailable: %s" % type(e).__name__, 503)
        self.send_response(status)
        for k, v in headers.items():
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(out)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(out)

    def _cpu_lane(self, method):
        """/api/cpu/* = the Unraid CPU runner's API. It enforces its OWN bearer token and
        refuses Cloudflare-fronted requests (cpu_lane.handle); everything else on this
        server trusts the caller. A broken/missing module only disables the lane."""
        try:
            import cpu_lane
        except Exception as e:  # noqa: BLE001
            return self._text("cpu lane unavailable: %s" % type(e).__name__, 503)
        return cpu_lane.handle(self, method)

    def do_PUT(self):
        _invalidate_response_cache()
        if self.path.startswith("/api/cpu/"):
            return self._cpu_lane("PUT")
        self._text("not found", 404)

    def do_GET(self):
        if self.path.startswith("/api/cpu/"):
            return self._cpu_lane("GET")
        if self.path == "/api/cpu-lane":
            # read-only dashboard feed (no token: same trust as every other dashboard
            # route, behind Cloudflare Access). Counts and ids only: never the spec/cmd/env.
            try:
                import cpu_lane
                return self._json(cpu_lane.get_store().summary())
            except Exception as e:  # noqa: BLE001
                return self._json({"enabled": False, "error": type(e).__name__})
        if self.path == "/chat" or self.path.startswith("/api/chat/"):
            return self._chat("GET")
        if self.path == "/" or self.path == "/index.html":
            self._html(FRONTEND_HTML)
        elif self.path == "/api/jobs":
            with q._Locked() as lock:
                state = lock.load()
            # The rendered order follows the daemon's OWN focused bundle (read from the
            # same state we just loaded), so a bundle that is first stays first across
            # the gap where its next slice has not been enqueued yet.
            self._json(_annotate_queue_wait(_annotate_wait_reason(_annotate_bundle_rank(_annotate_plan_rollup(
                _annotate_display_seq(
                    _annotate_job_groups([_job_summary(j, state["jobs"])
                                          for j in state["jobs"]]),
                    focus_key=_focus_bundle_key(state))), state), state)))
        elif self.path == "/api/triage":
            # Open failure-signature triage packets (triage_packets.open_summary; read-only,
            # fail-open: an unreadable index is {"open": 0}).
            self._json(_triage_summary())
        elif self.path == "/api/queue-wait":
            # Lane-level "what is the queue waiting on" (daemon queue-wait.json, or the
            # clearly-labelled daemon-log fallback until the daemon is restarted).
            self._json(_queue_wait_payload())
        elif self.path == "/api/bundle-views":
            # Dashboard B: one line per slice with its LIVE phase + history, per plan
            # in the queue, and the live off-GPU activity row (see _bundle_views).
            self._json(_cached_response("bundle-views", _bundle_views, ttl=3))
        elif self.path == "/api/bundle-history" or self.path.startswith("/api/bundle-history?"):
            # FINISHED bundles with their full per-slice history, newest first, paged
            # (see _finished_bundle_views). ?days=N (0 = all ages) &limit= &offset=
            qs = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query)
            try:
                a = {"days": float(qs.get("days", [FINISHED_BUNDLE_DAYS])[0]),
                     "limit": int(qs.get("limit", [FINISHED_BUNDLE_PAGE])[0]),
                     "offset": int(qs.get("offset", [0])[0])}
            except ValueError:
                return self._text("days/limit/offset must be numbers", 400)
            self._json(_cached_response(
                "bundle-history:" + json.dumps(a, sort_keys=True),
                lambda: _finished_bundle_views(**a), ttl=15))
        elif self.path == "/api/jobs/completed":
            self._json(_cached_response("completed", _completed_jobs))
        elif self.path == "/api/runs":
            # Unified run-status list (see _run_status_jobs). Supersedes the split
            # between /api/jobs/completed and /api/handoff; those endpoints remain
            # for any other consumer, but the dashboard front-end now reads this one.
            self._json(_cached_response("runs", _run_status_jobs))
        elif self.path == "/api/runs/retention":
            # Retention preview: every row the auto-retention predicate would archive,
            # with the reason (never clears). See runstatus_retention.py.
            self._json(_retention_preview())
        elif self.path == "/api/runs/tree":
            # The same rows as /api/runs, pre-nested project > slice > run with the
            # worst-of rollup already computed (see _build_run_tree). The panel reads
            # THIS; /api/runs stays exactly as it was for any other consumer.
            self._json(_cached_response("runs/tree",
                                        lambda: _build_run_tree(_run_status_jobs())))
        elif self.path == "/api/hosts":
            self._json(_hosts_summary())
        elif self.path == "/api/settings/hosts":
            # The editable host table behind the Settings panel. Reads through
            # ollama-worker.py's live config loader, so it never disagrees with
            # what the dispatcher/fit-router is actually using.
            self._json(_hosts_settings())
        elif self.path == "/api/web-search-usage":
            self._json(_web_search_usage())
        elif self.path == "/api/handoff":
            # Read the handoff data from handoff-emit.py --json. Bounded: an
            # untimed child here held a request thread (and its socket) for as long
            # as handoff-emit took, which is one more way to pile up descriptors.
            try:
                result = subprocess.run(['python3', os.path.expanduser('~/bin/handoff-emit.py'), '--json'],
                                      capture_output=True, text=True, check=True, timeout=60)
                self._json(json.loads(result.stdout))
            except subprocess.TimeoutExpired:
                self._text("handoff-emit.py --json did not finish within 60s", 504)
            except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
                self._text(f"error reading handoff data: {e}", 500)
        else:
            m = re.match(r"^/api/jobs/([^/]+)/livelog$", self.path)
            if m:
                return self._livelog(m.group(1))
            m = re.match(r"^/api/jobs/([^/]+)/gate$", self.path)
            if m:
                return self._gate_detail(m.group(1))
            self._text("not found", 404)

    _ANSI_RE = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]")

    def _livelog(self, job_id):
        with q._Locked() as lock:
            state = lock.load()
        job = next((j for j in state["jobs"] if j["id"] == job_id), None)
        path = _livelog_path(job, job_id)
        if path is None:
            return self._text("job not found" if job is None
                              else "no live log recorded for this job", 404)
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

    def _gate_detail(self, job_id):
        """Serve the durable gate.json (verdict + issues[]) for one job, by id, from
        LOG_DIR -- unlike _livelog this does NOT require a live state.json row, so it
        keeps working after the job is reaped. This is what backs the Completed
        table's "view issues" action: the gate verdict is a cross-check exactly
        because it's still inspectable once the job itself is long gone."""
        res = q._load_job_result(job_id)
        if res is None or not res.get("gate_json"):
            return self._text("no gate record for this job", 404)
        try:
            gate = json.loads(Path(res["gate_json"]).read_text())
        except (OSError, ValueError) as e:
            return self._text(f"error reading gate record: {e}", 500)
        self._json({
            "id": job_id,
            "label": res.get("label"),
            "verdict": res.get("verdict"),
            "review_verdict": res.get("review_verdict"),
            "gate_authority": res.get("gate_authority"),
            "regate_ran": res.get("regate_ran"),
            "counts": res.get("counts"),
            "issues": gate.get("issues") or [],
            "not_checked": gate.get("not_checked") or [],
            "untrusted": gate.get("untrusted") or [],
        })

    def do_POST(self):
        # Any state change must be visible on the very next poll.
        _invalidate_response_cache()
        if self.path.startswith("/api/cpu/"):
            return self._cpu_lane("POST")
        if self.path.startswith("/api/chat/"):
            return self._chat("POST")
        if self.path == "/api/jobs":
            self._enqueue()
        elif self.path == "/api/settings/hosts":
            return self._save_hosts()
        elif self.path == "/api/jobs/move":
            self._move()
        elif self.path == "/api/jobs/move-group":
            self._move_group()
        elif self.path.startswith("/api/jobs/") and self.path.endswith("/kill"):
            self._kill(self.path[len("/api/jobs/"):-len("/kill")])
        else:
            m = re.match(r"^/api/jobs/([^/]+)/promote$", self.path)
            if m:
                return self._promote(m.group(1))
            m = re.match(r"^/api/jobs/([^/]+)/promote-group$", self.path)
            if m:
                return self._promote_group(m.group(1))
            m = re.match(r"^/api/jobs/([^/]+)/hold$", self.path)
            if m:
                return self._hold(m.group(1))
            m = re.match(r"^/api/jobs/([^/]+)/resume$", self.path)
            if m:
                return self._resume(m.group(1))
            if self.path == "/api/runs/retention/apply":
                return self._retention_apply()
            m = re.match(r"^/api/runs/([^/]+)/clear$", self.path)
            if m:
                return self._clear_run(m.group(1))
            elif self.path == "/api/handoff/acted":
                return self._mark_handoff_as_acted()
            self._text("not found", 404)

    def do_DELETE(self):
        _invalidate_response_cache()
        if self.path.startswith("/api/cpu/"):
            return self._cpu_lane("DELETE")
        if self.path.startswith("/api/jobs/"):
            _p, _, _qs = self.path[len("/api/jobs/"):].partition("?")
            self._cancel(_p, force="force=1" in _qs.split("&"))
        else:
            self._text("not found", 404)

    def _save_hosts(self):
        """PUT-ish POST of the WHOLE host table (add/edit/remove are all just a
        different list). Deliberately whole-table rather than per-host verbs: the
        table is small, and one atomic write can't leave the dispatcher looking at
        a half-applied edit. Persists immediately -- every reader re-reads the file
        at use time, so no restart is needed."""
        try:
            data = self._read_json_body()
            rows = data.get("hosts") if isinstance(data, dict) else data
            return self._json(_save_hosts_settings(rows))
        except ValueError as e:
            return self._text(str(e), 400)
        except Exception as e:
            return self._text(f"could not save hosts: {e}", 500)

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
                # Same atomic duplicate-label guard as ollama-queue.py's
                # cmd_enqueue (Bug, 2026-09-19). This path writes to state["jobs"]
                # directly rather than going through cmd_enqueue, so without this
                # it is a hole straight past the guard.
                _label = data.get("label") or cwd_path.name
                _dupe = next((j for j in state["jobs"]
                              if str(j.get("label")) == str(_label)
                              and j.get("status") in q._LIVE_LABEL_STATES), None)
                if _dupe and not data.get("allow_duplicate_label"):
                    return self._text(
                        f"duplicate label: {_label!r} already has a LIVE job "
                        f"{_dupe['id']} ({_dupe.get('status')}). Adopt that job, or "
                        f"pass allow_duplicate_label to enqueue anyway.", 409)
                job_id = uuid.uuid4().hex[:12]
                job = {
                    "id": job_id,
                    "label": _label,
                    "model": model,
                    "host_pref": data.get("host") or "auto",
                    "cwd": str(cwd_path.resolve()),
                    "task_file": str(task_file),
                    "task_kind": data.get("task_kind") or None,
                    "manual_tools": bool(data.get("manual_tools")),
                    "api": data.get("api") or "ollama",
                    "verify": data.get("verify") or None,
                    "runner": (str(Path(data["runner"]).expanduser().resolve()) if data.get("runner") else None),
                    "num_ctx": int(data.get("num_ctx") or 65536),
                    "max_iters": int(data.get("max_iters") or 20),
                    "temperature": float(data.get("temperature") or 0),
                    "chat_timeout": int(data["chat_timeout"]) if data.get("chat_timeout") else None,
                    "status": "pending",
                    "enqueued_at": datetime.now(timezone.utc).isoformat(),
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
                if not _reorder_job(jobs, job_id, before_id):
                    return self._text("job not found", 404)
                lock.save(state)
                try:
                    truth = q.launch_truth(state, moving)
                except Exception:
                    truth = None
            self._json({"ok": True, "truth": truth})
        except Exception as e:
            self._text(f"error: {e}", 500)

    def _move_group(self):
        # Reorder whole BUNDLES relative to each other (2026-09-18, the user: arrange
        # which bundle runs next -- under depth-first that IS the run priority).
        # Body: {id: <a job id in the bundle to move>,
        #        before_id: <a job id in the bundle to sit before>, null = to the end}
        # Both endpoints are ROW ids, resolved to their group keys by
        # ollama-queue.py's move_group_for_job -- the same delegation rule as
        # _promote_group, so the API never reimplements what "the same bundle" means
        # and can never disagree with the CLI's `move-group`. Slice order INSIDE a
        # bundle is preserved by move_group itself; nothing here can reorder slices.
        try:
            job_id, before_id, err = _move_group_args(self._read_json_body())
            if err:
                return self._text(err, 400)
            res = q.move_group_for_job(job_id, before_id)
            try:
                with q._Locked() as lock:
                    _st = lock.load()
                _j = next((j for j in _st["jobs"] if j.get("id") == job_id), None)
                if isinstance(res, dict) and _j is not None:
                    res["truth"] = q.launch_truth(_st, _j)
            except Exception:
                pass
            self._json(res)
        except q.QueueActionError as e:
            self._text(str(e), 400)
        except Exception as e:
            self._text(f"error: {e}", 500)

    def _cancel(self, job_id, force=False):
        # Delegates to ollama-queue.py's own cancel_job() (2026-08-29) instead of
        # duplicating this status-check logic -- same reasoning as _promote/_resume
        # below: a CLI `cancel` subcommand now exists too (github-projects-bf flagged
        # cancelling as reachable ONLY via this HTTP endpoint as a real gap), and
        # keeping one implementation means the two can't drift apart.
        try:
            if force:
                # BUNDLE CANCEL must stop the RUNNING slice too (the user 2026-10-01): the
                # button used to skip it, so cancelling a bundle left its live slice
                # running -- and when it finished, the slicer advanced the plan. Same
                # sequence as `ollama-queue.py cancel --force`: mark the plan cancelled
                # FIRST (so nothing re-arms it), then the sanctioned graceful stop
                # (SIGTERM + bounded SIGKILL; never a raw kill, never an orphaned pid).
                q._mark_plan_cancelled_by_id(job_id, "dashboard bundle cancel")
                _res = q.stop_job(job_id)
                return self._json(_res if isinstance(_res, dict) else {"stopped": job_id})
            # explicit=True: DELETE /api/jobs/<id> is the dashboard's "remove" button,
            # an operator action -- see cancel_job() for why a terminal row may go.
            self._json(q.cancel_job(job_id, explicit=True))
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

    def _promote_group(self, job_id):
        # Whole-job promote: delegates to ollama-queue.py's promote_group_for_job(), which
        # resolves this row's group key (its slice-plan project) and moves EVERY pending
        # member to the front as one ordered block under the same state flock -- same
        # delegation rule as _promote, no reimplementation here. {"preempt": true}
        # (the bundle "run now" confirm) also pauses the running lane holder.
        try:
            body = self._read_json_body()
            self._json(q.promote_group_for_job(job_id, preempt=bool(body.get("preempt")),
                                               take_focus=bool(body.get("take_focus"))))
        except q.QueueActionError as e:
            self._text(str(e), 400)
        except Exception as e:
            self._text(f"error: {e}", 500)

    def _hold(self, job_id):
        # Sticky operator hold on a not-yet-running job. Delegates to
        # ollama-queue.py's hold_job() (same delegation rule as _promote/_resume --
        # no reimplementation here), which was previously reachable ONLY from the
        # CLI; the queue panel's "hold whole plan" button needs it per slice, since
        # there is no group-level hold in ollama-queue.py to delegate to. hold_job
        # itself refuses a running/terminal job, so this adds no new policy.
        try:
            self._json(q.hold_job(job_id))
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

    def _retention_apply(self):
        """POST {"ids": [...]}: clear ONLY those ids, each re-checked against the
        retention predicate; awaiting-signoff rows and unreviewed deliverables are
        refused per id. Never chooses its own scope."""
        try:
            ids = self._read_json_body().get("ids") or []
        except Exception:
            ids = []
        if not isinstance(ids, list) or not ids:
            return self._text("ids (non-empty list) required", 400)
        try:
            self._json({"results": _retention_apply(ids)})
        except Exception as e:
            self._text(f"error: {e}", 500)

    def _clear_run(self, job_id):
        """Clear/handled action for the unified run-status list: archive the job's
        durable sidecars out of the active dir. Body may carry {"override": "<why>"}
        to clear a job whose REQUIRED sign-off is still unapproved; without it, such
        a job is REFUSED with 409 so 'clear' can never silently erase a pending
        sign-off."""
        try:
            override = self._read_json_body().get("override")
        except Exception:
            override = None
        try:
            result = _archive_run(job_id, override=override)
        except Exception as e:
            return self._text(f"error: {e}", 500)
        if not result.get("ok"):
            return self._json(result, status=result.get("code", 400))
        self._json(result)

    def _mark_handoff_as_acted(self):
        """Handle marking a handoff job as acted upon."""
        try:
            data = self._read_json_body()
            job_id = data.get("id")
            
            if not job_id:
                return self._text("job id required", 400)
                
            # Validate that the job ID exists in current handoff data
            try:
                result = subprocess.run(['python3', os.path.expanduser('~/bin/handoff-emit.py'), '--json'],
                                      capture_output=True, text=True, check=True, timeout=60)
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
                    
            except (subprocess.CalledProcessError, json.JSONDecodeError,
                    subprocess.TimeoutExpired) as e:
                return self._text(f"error validating job: {e}", 500)

            # Run the --acted command to mark this job
            try:
                result = subprocess.run(['python3', os.path.expanduser('~/bin/handoff-emit.py'), '--acted', job_id],
                                      capture_output=True, text=True, check=True, timeout=60)
                self._json({"ok": True})
            except subprocess.TimeoutExpired:
                return self._text("handoff-emit.py --acted did not finish within 60s", 504)
            except subprocess.CalledProcessError as e:
                detail = "; ".join(p for p in [(e.stderr or "").strip(), (e.stdout or "").strip()] if p) or f"exit {e.returncode}"
                # exit 2 = handoff-emit refused (e.g. required sign-off pending); reason is on stdout, not stderr
                return self._text(f"error marking as acted: {detail}", 500)
                
        except Exception as e:
            self._text(f"error: {e}", 500)

    def log_message(self, fmt, *args):
        sys.stderr.write(f"[queue-api] {self.address_string()} {fmt % args}\n")


class ThreadingServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True


def _self_test():
    """Prove the Completed-jobs cross-check (2026-09-14, "i need it in dashboard as
    my cross check") actually survives reaping: a job with sidecars but NO live
    state.json row (i.e. already reaped) still shows up with its verdict, a job
    still present in live state is NOT duplicated from its durable copy, and the
    gate-/regate- internal helper rows stay excluded, matching ollama-queue.py's
    own `results` behavior."""
    import tempfile
    import shutil
    import subprocess
    ok = True

    def check(name, got, want):
        nonlocal ok
        if got != want:
            ok = False
            print(f"FAIL: {name}: got {got!r} want {want!r}")
        else:
            print(f"ok: {name}")

    # ACTIVE runtime, not wall-clock since first launch (0b130de503d8: ~38 min of
    # work shown as "8 hours" after a 7h20m preempt pause).
    with tempfile.TemporaryDirectory() as _rtd:
        _lp = Path(_rtd) / "j.log"
        _lp.write_text("x\n")
        _now = time.time()
        os.utime(_lp, (_now, _now))
        from datetime import datetime as _dt, timezone as _tz
        _la = _dt.fromtimestamp(_now - 300, _tz.utc).isoformat()
        _run = _job_summary({"id": "a", "label": "l", "status": "running", "log_path": str(_lp),
                             "launched_at": _la, "active_s": 1800.0})
        check("runtime: running job shows accrued active + live segment, not wall",
              round(_run["elapsed_s"] / 60), 35)
        _done = _job_summary({"id": "b", "label": "l", "status": "failed", "log_path": str(_lp),
                              "launched_at": _la, "active_accrued_for": _la, "active_s": 2280.0})
        check("runtime: terminal job shows its accrued active time", _done["elapsed_s"], 2280.0)
        _leg = _job_summary({"id": "c", "label": "l", "status": "failed", "log_path": str(_lp)})
        check("runtime: a job with no accrual keeps the wall figure, paused unknown",
              (_leg["elapsed_s"] == _leg["wall_s"], _leg["paused_s"]), (True, None))
        check("runtime: paused = wall - active, never negative",
              _done["paused_s"] is not None and _done["paused_s"] >= 0, True)

    with tempfile.TemporaryDirectory() as td:
        ld = Path(td)
        orig_log_dir = q.LOG_DIR
        q.LOG_DIR = ld
        try:
            def mk(jid, label, verdict, regate="done", diff_files=0):
                (ld / f"{jid}.done.json").write_text(json.dumps({
                    "id": jid, "label": label, "model": "qwen3.8:27b-q4_K_M",
                    "host_pref": "studio", "status": "done", "exit_code": 0,
                    "persisted_at": f"2026-09-14T0{jid[0]}:00:00Z"}))
                (ld / f"{jid}.gate.json").write_text(json.dumps(
                    {"verdict": verdict, "regate": regate}))
                if diff_files:
                    (ld / f"{jid}.diff").write_text(
                        "".join(f"diff --git a/f{i}.py b/f{i}.py\n+x\n" for i in range(diff_files)))

            mk("1reapedjob01", "reaped-concerns", "concerns", diff_files=2)
            mk("2livejob0002", "still-live", "pass", diff_files=1)
            (ld / "3gate-parent0.done.json").write_text(json.dumps(
                {"id": "3gate-parent0", "label": "gate-someparent", "status": "done"}))
            (ld / "3gate-parent0.gate.json").write_text(json.dumps({"verdict": "pass"}))
            # --- Run Status must not leak a still-live-status row (2026-09-18, the user:
            # "pending doesn't have to show in run status, those are above in the
            # queue") -- a sidecar whose OWN persisted status is a live-queue state
            # (e.g. a stale/partial snapshot, or a duplicate label re-enqueued under a
            # new id) must be excluded even though it is NOT in live_ids (FakeLock
            # below only lists job 2 as live), and even though it carries a real gate
            # verdict -- status, not verdict, decides "genuinely finished" here.
            (ld / "6staleleak00.done.json").write_text(json.dumps({
                "id": "6staleleak00", "label": "leaked-pending-row", "model": "qwen3.8:27b-q4_K_M",
                "host_pref": "studio", "status": "pending", "exit_code": None,
                "persisted_at": "2026-09-18T00:00:00Z"}))
            (ld / "6staleleak00.gate.json").write_text(json.dumps({"verdict": "pass"}))

            class FakeLock:
                def __enter__(self):
                    return self

                def __exit__(self, *a):
                    return False

                def load(self):
                    # Only job 2 is still live -- job 1 has been reaped (its sidecars
                    # are the ONLY trace left, same as the real RETAIN_DONE_RECENT=0 tick).
                    return {"jobs": [{"id": "2livejob0002", "status": "running"}]}

            orig_locked = q._Locked
            q._Locked = lambda *a, **k: FakeLock()
            try:
                rows = _completed_jobs()
            finally:
                q._Locked = orig_locked

            ids = {r["id"] for r in rows}
            check("reaped job appears in Completed table", "1reapedjob01" in ids, True)
            check("still-live job NOT duplicated from its durable copy", "2livejob0002" in ids, False)
            check("gate-/regate- helper row excluded", "3gate-parent0" in ids, False)
            check("a sidecar with a live-queue status (pending) is excluded even with a real verdict",
                  "6staleleak00" in ids, False)
            r1 = next((r for r in rows if r["id"] == "1reapedjob01"), None)
            check("reaped job row present", r1 is not None, True)
            if r1:
                check("reaped job verdict text includes regate authority",
                      r1["verdict"], "CONCERNS (regate)")
                check("reaped job changed-file count from .diff", r1["changed_file_count"], 2)
                # The parent-row rollup (change 3, front-end only -- see
                # summarizeGroup/renderParentGroup in FRONTEND_HTML) reads model/
                # host/changed_file_count/timestamp/raw_verdict straight off each
                # child row. Nothing server-side computes the rollup itself, but a
                # row missing any of these fields would silently render a blank
                # parent cell, so pin the data contract here.
                check("completed-job row carries every field the parent-row rollup needs",
                      all(k in r1 for k in ("model", "host", "changed_file_count", "timestamp", "raw_verdict")),
                      True)

            # --- Unified run-status list + clear/archive (2026-09-17, the user: "one list
            # ... that we can clear once it's handled") ---
            # A reaped (not live) job that still REQUIRES sign-off, so it appears in
            # the list and exercises the clear-refusal path.
            mk("4signoffjob0", "needs-signoff", "pass", diff_files=1)
            # Drive the sign-off predicate directly: handoff-emit's real signoff.json
            # is not what's under test here -- THIS list/clear logic is. Only job 4 is
            # treated as requiring an unapproved sign-off.
            orig_signoff = ho.signoff_blocks_acting
            ho.signoff_blocks_acting = (lambda jid, label:
                "REQUIRES sign-off" if jid == "4signoffjob0" else None)
            orig_locked2 = q._Locked
            q._Locked = lambda *a, **k: FakeLock()
            try:
                runs = _run_status_jobs()
                run_ids = {r["id"] for r in runs}
                check("run-status list shows a reaped completed job",
                      "1reapedjob01" in run_ids, True)
                check("run-status list excludes the still-live job",
                      "2livejob0002" in run_ids, False)
                check("run-status list excludes gate-/regate- helper rows",
                      "3gate-parent0" in run_ids, False)
                check("run-status list excludes a live-queue-status sidecar",
                      "6staleleak00" in run_ids, False)
                r_so = next((r for r in runs if r["id"] == "4signoffjob0"), None)
                check("awaiting_signoff flag set on the job that needs sign-off",
                      bool(r_so and r_so["awaiting_signoff"]), True)
                r_ok = next((r for r in runs if r["id"] == "1reapedjob01"), None)
                check("awaiting_signoff flag clear on a job that doesn't need it",
                      bool(r_ok and r_ok["awaiting_signoff"]), False)

                # Clear a normal job: its sidecars move to archive/ and it drops off.
                res_clear = _archive_run("1reapedjob01")
                check("clear of a normal job succeeds", res_clear.get("ok"), True)
                check("clear moved .done.json + .gate.json + .diff",
                      sorted(res_clear.get("moved", [])),
                      ["1reapedjob01.diff", "1reapedjob01.done.json", "1reapedjob01.gate.json"])
                check("cleared sidecars are gone from the active dir",
                      (ld / "1reapedjob01.gate.json").exists(), False)
                check("cleared sidecars are recoverable under archive/",
                      (ld / "archive" / "1reapedjob01.gate.json").exists(), True)
                check("a cleared job does NOT reappear in the list",
                      "1reapedjob01" in {r["id"] for r in _run_status_jobs()}, False)

                # Clear WITHOUT override on a job awaiting sign-off: REFUSED, nothing
                # moved -- the one case the sign-off gate exists to hold back.
                res_refused = _archive_run("4signoffjob0")
                check("clear refuses a job awaiting sign-off", res_refused.get("ok"), False)
                check("refusal is flagged awaiting_signoff",
                      res_refused.get("awaiting_signoff"), True)
                check("refused clear left the sidecars in place (not hidden)",
                      (ld / "4signoffjob0.gate.json").exists(), True)
                check("job awaiting sign-off still appears after a refused clear",
                      "4signoffjob0" in {r["id"] for r in _run_status_jobs()}, True)

                # Clear WITH an override reason: allowed, and the reason is recorded.
                res_override = _archive_run("4signoffjob0", override="operator said ship it")
                check("clear with override succeeds", res_override.get("ok"), True)
                check("override is recorded on the result",
                      res_override.get("signoff_overridden"), True)
                check("overridden job is archived out of the list",
                      "4signoffjob0" in {r["id"] for r in _run_status_jobs()}, False)
                man = json.loads((ld / "archive" / "clear-manifest.json").read_text())
                check("clear-manifest records the override reason",
                      any(e.get("signoff_override_reason") == "operator said ship it" for e in man), True)

                # --- Log-only job (no .done.json/.gate.json) must ALSO stay cleared
                # (2026-09-22, the user: rt-cc-sync-diagnosis/rt-churning-research/
                # rt-price-apis-research kept reporting {ok: true, moved: []} and
                # reappearing after "clear" -- see _archive_run's EXCEPTION note).
                # Fixture: a bare research job with ONLY a run log + cached answer,
                # no durable done/gate sidecar at all, same as a pre-Fix-2 job.
                (ld / "7c0d0f1a02-rt-price-apis-research.log").write_text(
                    "[worker] model: qwen3.8:27b-q4_K_M\nsome transcript noise\n")
                (ld / "7c0d0f1a02.answer.md").write_text(
                    "FINDING: no cheaper API found.")
                res_lo = _run_status_jobs()
                check("log-only research job appears in run-status before clearing",
                      "7c0d0f1a02" in {r["id"] for r in res_lo}, True)
                res_clear_lo = _archive_run("7c0d0f1a02")
                check("clear of a log-only job reports ok", res_clear_lo.get("ok"), True)
                check("clear of a log-only job actually MOVES something (not moved: [])",
                      len(res_clear_lo.get("moved", [])) > 0, True)
                check("log-only job's run log is gone from the active dir",
                      (ld / "7c0d0f1a02-rt-price-apis-research.log").exists(), False)
                check("log-only job's run log is recoverable under archive/",
                      (ld / "archive" / "7c0d0f1a02-rt-price-apis-research.log").exists(), True)
                check("log-only job's cached answer is recoverable under archive/",
                      (ld / "archive" / "7c0d0f1a02.answer.md").exists(), True)
                check("a cleared log-only job does NOT reappear in run-status",
                      "7c0d0f1a02" in {r["id"] for r in _run_status_jobs()}, False)
                check("a cleared log-only job does NOT reappear in the Completed table",
                      "7c0d0f1a02" in {r["id"] for r in _completed_jobs()}, False)
                # Re-clearing an already-cleared log-only job is a clean 404, not a
                # silent {ok: true, moved: []} -- there is genuinely nothing left.
                res_reclear_lo = _archive_run("7c0d0f1a02")
                check("re-clearing an already-cleared log-only job is a 404, not a fake ok",
                      res_reclear_lo.get("ok"), False)
            finally:
                q._Locked = orig_locked2
                ho.signoff_blocks_acting = orig_signoff
        finally:
            q.LOG_DIR = orig_log_dir

    # --- run-status grouping (2026-09-18, the user: "if they're cluttered there, how do
    # i know what's been handled?") -- prove the needs_eyes/handled split so the
    # panel can fold intermediate/no-eyes rows WITHOUT hiding anything. Pure logic,
    # no filesystem: drive _annotate_run_groups directly.
    orig_eval = ho._label_is_eval
    ho._label_is_eval = lambda lab: str(lab or "").startswith("eval-")
    try:
        def grp(rows, live):
            _annotate_run_groups(rows, live)
            return {r["id"]: r["group"] for r in rows}

        # superseded by a later refine round already finished
        g = grp([{"id": "a", "label": "auto-author-feat-x", "awaiting_signoff": False,
                  "has_gate": True, "verdict": "CONCERNS", "timestamp": "2026-09-18T00:00:00Z"},
                 {"id": "b", "label": "auto-refine-feat-x-r2", "awaiting_signoff": False,
                  "has_gate": True, "verdict": "PASS", "timestamp": "2026-09-18T01:00:00Z"}], [])
        check("earlier authoring stage folds under a later refine round", g["a"], "handled")
        # The latest stage is NOT folded as superseded -- it passed, so it lands in
        # good_to_go (see the "a PASS is good to go" block below), not needs_eyes.
        check("the latest stage is not folded away as superseded", g["b"], "good_to_go")

        # superseded by a DEEPER (nested) re-author that is LIVE in the queue
        g = grp([{"id": "c", "label": "auto-author-feat-y", "awaiting_signoff": False,
                  "has_gate": True, "verdict": "FAIL", "timestamp": "2026-09-18T00:00:00Z"}],
                ["auto-author-feat-y-s1-create-thing"])
        check("finished stage folds when feature is re-running (deeper live base)", g["c"], "handled")

        # a label carrying a [auto-fix rN] annotation still parses to its base+rank
        check("[auto-fix] suffix stripped for base parsing",
              _feature_and_rank("auto-refine-feat-z-r1 [auto-fix r1]"), ("feat-z", 1))

        # sign-off ALWAYS wins over any fold
        g = grp([{"id": "d", "label": "auto-author-feat-w", "awaiting_signoff": True,
                  "has_gate": True, "verdict": "PASS", "timestamp": "2026-09-18T00:00:00Z"},
                 {"id": "e", "label": "auto-refine-feat-w-r1", "awaiting_signoff": False,
                  "has_gate": True, "verdict": "PASS", "timestamp": "2026-09-18T01:00:00Z"}], [])
        check("a sign-off-owed row is NEVER folded", g["d"], "needs_eyes")

        # eval arm folds; a fresh non-gated diag stays; an old one folds
        g = grp([{"id": "f", "label": "eval-bakeoff-arm3", "awaiting_signoff": False,
                  "has_gate": False, "verdict": "PENDING", "timestamp": "2026-09-18T00:00:00Z"},
                 {"id": "h", "label": "diag-fresh", "awaiting_signoff": False,
                  "has_gate": False, "verdict": "PENDING",
                  "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")},
                 {"id": "i", "label": "diag-stale", "awaiting_signoff": False,
                  "has_gate": False, "verdict": "PENDING", "timestamp": "2026-09-01T00:00:00Z"}], [])
        check("eval measurement arm folds", g["f"], "handled")
        check("a FRESH (<24h) research/diag stays needs_eyes", g["h"], "needs_eyes")
        check("a STALE (>24h) research/diag folds", g["i"], "handled")

        # --- a PASS is GOOD TO GO, not "needs eyes" (2026-09-18, the user: "if it
        # passed, it passed ... i'd like it to show that more clearly"). Sign-off on
        # a passing dispatch is the coordinator's job, so a clean PASS must leave
        # the user's needs_eyes bucket entirely; needs_eyes is now strictly CONCERNS /
        # FAIL / ESCALATED / BLOCKED. Fails without the fix (PASS was needs_eyes).
        g = grp([{"id": "j", "label": "real-app-fix", "awaiting_signoff": False,
                  "has_gate": True, "raw_verdict": "PASS", "verdict": "PASS",
                  "timestamp": "2026-09-18T00:00:00Z"},
                 {"id": "k", "label": "real-app-fix-2", "awaiting_signoff": False,
                  "has_gate": True, "raw_verdict": "CONCERNS", "verdict": "CONCERNS",
                  "timestamp": "2026-09-18T00:00:00Z"},
                 {"id": "l", "label": "real-app-fix-3", "awaiting_signoff": False,
                  "has_gate": True, "raw_verdict": "FAIL", "verdict": "FAIL",
                  "timestamp": "2026-09-18T00:00:00Z"}], [])
        check("a passing deliverable is GOOD TO GO, not needs_eyes", g["j"], "good_to_go")
        check("a CONCERNS deliverable still needs eyes", g["k"], "needs_eyes")
        check("a FAIL deliverable still needs eyes", g["l"], "needs_eyes")
        check("a pass-pending-review verdict counts as a pass",
              _verdict_is_pass({"raw_verdict": "pass-pending-review"}), True)
        check("CONCERNS is not a pass", _verdict_is_pass({"raw_verdict": "CONCERNS"}), False)

        # --- an AUTHOR-ONLY stage whose coding slice never ran is BLOCKED, never a
        # green PASS deliverable (2026-09-18: arr-codec-floor s1-codec-rank showed
        # as "PASS (pending review)" although no code was ever written -- the
        # authoring job wrote only TASK/refimpl/verify and the context-budget gate
        # then refused to enqueue the coding slice). Fails without the fix: the row
        # classified good_to_go/needs_eyes carrying a PASS verdict.
        rows_b = [{"id": "m", "label": "auto-author-arr-codec-floor-s1-codec-rank",
                   "awaiting_signoff": False, "has_gate": True,
                   "raw_verdict": "PASS", "verdict": "PASS (pending review)",
                   "auto_pipeline_stage": "author", "review_enqueue": "enqueue-failed",
                   "timestamp": "2026-09-18T05:32:54Z"}]
        gb = grp(rows_b, [])
        check("an author-only stage with no coding slice is NOT good_to_go",
              gb["m"] != "good_to_go", True)
        check("an author-only stage with no coding slice needs eyes", gb["m"], "needs_eyes")
        check("its verdict is rewritten so it cannot read as a shipped PASS",
              rows_b[0]["raw_verdict"], "BLOCKED")
        check("the original gate verdict is preserved alongside it",
              rows_b[0]["gate_verdict"], "PASS (pending review)")
        check("the blocked reason names the authoring stage",
              "coding slice never ran" in rows_b[0]["blocked_reason"], True)
        # ...but an author stage FOLLOWED by its coding slice is just superseded.
        gb2 = grp([{"id": "n", "label": "auto-author-proj-s1-thing", "awaiting_signoff": False,
                    "has_gate": True, "raw_verdict": "PASS", "verdict": "PASS",
                    "auto_pipeline_stage": "author", "timestamp": "2026-09-18T00:00:00Z"},
                   {"id": "o", "label": "proj-s1-thing", "awaiting_signoff": False,
                    "has_gate": True, "raw_verdict": "PASS", "verdict": "PASS",
                    "timestamp": "2026-09-18T01:00:00Z"}], [])
        check("an author stage whose coding slice DID run just folds as superseded",
              gb2["n"], "handled")
        check("and that coding slice itself is good to go", gb2["o"], "good_to_go")
        check("_run_blocked_reason is None for an ordinary coding row",
              _run_blocked_reason({"label": "proj-s1-thing"}), None)

        # --- a row a newer retry replaced folds OUT of needs_eyes (defensive: the
        # field may not be set by anything yet, but it must be honoured if it is).
        g = grp([{"id": "s1", "label": "some-fix", "awaiting_signoff": False,
                  "has_gate": True, "raw_verdict": "FAIL", "verdict": "FAIL",
                  "superseded_by": "some-fix-retry", "timestamp": "2026-09-18T00:00:00Z"},
                 {"id": "s2", "label": "other-fix", "awaiting_signoff": False,
                  "has_gate": True, "raw_verdict": "FAIL", "verdict": "FAIL",
                  "superseded": True, "timestamp": "2026-09-18T00:00:00Z"}], [])
        check("a row with superseded_by folds into handled", g["s1"], "handled")
        check("a row with a bare superseded flag folds into handled", g["s2"], "handled")

        # --- Bug 13: WHAT the badge says, and how loud (label + severity only --
        # membership is decided above). A green PASS must never read like a fault.
        check("a passing row reads 'good to go' in green",
              _EYES_LABELS[_eyes_flavor({"group": "good_to_go"})], ("good to go", "ok"))
        check("a sign-off-owed row reads 'awaiting sign-off', neutral/green",
              _EYES_LABELS[_eyes_flavor({"group": "needs_eyes", "awaiting_signoff": True,
                                         "raw_verdict": "PASS"})],
              ("awaiting sign-off", "ok"))
        check("a CONCERNS row reads 'needs review', amber",
              _EYES_LABELS[_eyes_flavor({"group": "needs_eyes", "raw_verdict": "CONCERNS"})],
              ("needs review", "warn"))
        check("a FAIL row reads 'needs review', amber",
              _EYES_LABELS[_eyes_flavor({"group": "needs_eyes", "raw_verdict": "FAIL"})],
              ("needs review", "warn"))
        check("a blocked row reads 'blocked -- did not run', red",
              _EYES_LABELS[_eyes_flavor({"group": "needs_eyes", "raw_verdict": "BLOCKED",
                                         "blocked_reason": "authoring stage only"})],
              ("blocked -- did not run", "bad"))
        lab_rows = [{"group": "good_to_go", "raw_verdict": "PASS"}]
        _annotate_eyes_labels(lab_rows)
        check("_annotate_eyes_labels tags label + severity on the row",
              (lab_rows[0]["eyes_label"], lab_rows[0]["eyes_severity"]), ("good to go", "ok"))
    finally:
        ho._label_is_eval = orig_eval

    # --- parent/child rollup of sliced jobs (2026-09-18, the user: "show the main job,
    # and then expand that main job to show the jobs inside it") -- prove the
    # slice-index-driven parent resolution + per-row annotation. Pure logic: inject a
    # reverse/projects index rather than touching ~/.ollama-dispatch.
    reverse = {  # full_slice_label -> project (as _load_slice_index would build)
        "bg-escalation-s1-rules": "bg-escalation",
        "bg-escalation-s2-sla": "bg-escalation",
        "bg-actions-s1-item": "bg-actions",
    }
    projects = {"bg-escalation": {"order": ["s1-rules", "s2-sla", "s3-letter"], "total": 3},
                "bg-actions": {"order": ["s1-item", "s2-merge"], "total": 2}}

    # An auto-author stage of a sliced project rolls up to its project, carries the
    # slice id, and learns the project's total slice count from the plan.
    rows = [{"id": "p1", "label": "auto-author-bg-escalation-s1-rules"},
            {"id": "p2", "label": "auto-refine-bg-escalation-s2-sla-r2 [auto-fix r1]"},
            {"id": "p3", "label": "auto-refine-bg-actions-s1-item-r3"},
            {"id": "p4", "label": "diag-cloudflare-en0"},
            {"id": "p5", "label": "rt-costco-always-sites-s7-newslice"}]
    _annotate_run_parents(rows, reverse, projects)
    by = {r["id"]: r for r in rows}
    check("author stage rolls up to its project", by["p1"]["parent"], "bg-escalation")
    check("author stage carries its slice id", by["p1"]["slice_id"], "s1-rules")
    check("parent_total comes from the plan", by["p1"]["parent_total"], 3)
    check("refine stage (with [auto-fix] + -rN) rolls up to its project",
          by["p2"]["parent"], "bg-escalation")
    check("refine stage slice id is the -rN-stripped base tail", by["p2"]["slice_id"], "s2-sla")
    check("a different project's slice rolls up separately", by["p3"]["parent"], "bg-actions")
    check("a standalone (non-sliced) job has no parent", by["p4"]["parent"], None)
    check("standalone job carries no slice id", by["p4"]["slice_id"], None)
    # Fallback path: a slice with NO plan-file entry still groups via the trailing
    # '-s<N>-...' label parse, so a freshly-sliced project isn't left ungrouped.
    check("unknown slice falls back to label-prefix parent",
          by["p5"]["parent"], "rt-costco-always-sites")
    check("_project_for_base fallback parses trailing slice id",
          _project_for_base("some-proj-s2-thing", {}), "some-proj")
    check("_project_for_base returns None for a non-slice base",
          _project_for_base("rt-cc-sync-diagnosis", {}), None)
    check("_project_for_base prefers the reverse map over the regex",
          _project_for_base("bg-actions-s1-item", reverse), "bg-actions")

    # --- PROJECT layer above the slice rollup (2026-09-18, the user: all the bg-* runs
    # and slices under ONE "broker-guard" header, each slice a sub-group inside it).
    # Prove project > slice > run nesting AND that the worst-of rollup propagates all
    # the way up. Pure logic: inject the slice index rather than touching disk.
    preverse = {"bg-health-s1-status": "bg-health", "bg-health-s2-retry": "bg-health",
                "bg-eraser-s1-invoke": "bg-eraser", "aw-scan-s1-fares": "aw-scan"}
    pprojects = {"bg-health": {"order": ["s1-status", "s2-retry"], "total": 2},
                 "bg-eraser": {"order": ["s1-invoke"], "total": 1},
                 "aw-scan": {"order": ["s1-fares"], "total": 1}}
    trows = [
        {"id": "t1", "label": "auto-author-bg-health-s1-status", "group": "handled",
         "raw_verdict": "PASS", "model": "qwen3.8:27b-q4_K_M", "host": "studio",
         "changed_file_count": 2, "timestamp": "2026-09-18T01:00:00Z"},
        {"id": "t2", "label": "auto-refine-bg-health-s1-status-r1", "group": "good_to_go",
         "raw_verdict": "PASS", "model": "qwen3.8:27b-q4_K_M", "host": "studio",
         "changed_file_count": 1, "timestamp": "2026-09-18T02:00:00Z"},
        {"id": "t3", "label": "bg-health-s2-retry", "group": "needs_eyes",
         "raw_verdict": "FAIL", "model": "qwen3.8:27b-q4_K_M", "host": "unraid",
         "changed_file_count": 3, "timestamp": "2026-09-18T03:00:00Z"},
        {"id": "t4", "label": "bg-eraser-s1-invoke", "group": "good_to_go",
         "raw_verdict": "PASS", "model": "qwen3.8:27b-q4_K_M", "host": "studio",
         "changed_file_count": 4, "timestamp": "2026-09-18T04:00:00Z"},
        {"id": "t5", "label": "aw-scan-s1-fares", "group": "good_to_go",
         "raw_verdict": "PASS", "model": "qwen3.8:27b-q4_K_M", "host": "studio",
         "changed_file_count": 5, "timestamp": "2026-09-18T05:00:00Z"},
        {"id": "t6", "label": "diag-cloudflare-en0", "group": "handled",
         "raw_verdict": "PENDING", "model": "qwen3.8:27b-q4_K_M", "host": "studio",
         "changed_file_count": 0, "timestamp": "2026-09-18T06:00:00Z"},
    ]
    _annotate_run_parents(trows, preverse, pprojects, progress={})
    _annotate_run_projects(trows)
    check("a bg-* slice maps to the broker-guard project",
          (trows[0]["project_key"], trows[0]["project_name"]), ("bg", "broker-guard"))
    check("an aw-* slice maps to the award-hacker project",
          (trows[4]["project_key"], trows[4]["project_name"]), ("aw", "award-hacker"))
    check("a one-off diag row belongs to no project (not a fake 'diag' project)",
          trows[5]["project_key"], None)
    check("an UNKNOWN prefix on a sliced job falls back to the raw prefix",
          _project_prefix("zz-thing-s1-bit", "zz-thing"), ("zz", "zz"))
    check("a known prefix maps even without a slice parent",
          _project_prefix("cc-waitlist-hotfix", None), ("cc", "cc-waitlist"))

    tree = _build_run_tree(trows)
    kinds = [(e["kind"], e.get("key") or e.get("project") or e["row"]["id"]) for e in tree]
    check("top level is PROJECTS (plus the unprojected one-off), in first-appearance order",
          kinds, [("project", "bg"), ("project", "aw"), ("row", "t6")])
    bg = tree[0]
    check("every bg-* slice group nests under the one broker-guard header",
          [e["project"] for e in bg["entries"]], ["bg-health", "bg-eraser"])
    check("the project header is named from the prefix map", bg["name"], "broker-guard")
    check("the runs of a slice nest under THAT slice, not the project",
          [r["id"] for r in bg["entries"][0]["rows"]], ["t1", "t2", "t3"])
    check("project > slice > run: the project sees every descendant run",
          [r["id"] for r in bg["rows"]], ["t1", "t2", "t3", "t4"])
    # worst-of propagation, at BOTH levels
    check("a FAIL child makes its SLICE header FAIL",
          bg["entries"][0]["summary"]["rawVerdict"], "FAIL")
    check("...and propagates all the way up to the PROJECT header",
          bg["summary"]["rawVerdict"], "FAIL")
    check("a FAIL child pulls the whole project into needs_eyes",
          bg["section"], "needs_eyes")
    check("an all-PASS slice header is green, not dragged down by a sibling",
          bg["entries"][1]["summary"]["rawVerdict"], "PASS")
    check("an all-PASS project header is PASS/good-to-go",
          (tree[1]["summary"]["rawVerdict"], tree[1]["section"]), ("PASS", "good_to_go"))
    check("an all-PASS project header reports 0 needing eyes",
          tree[1]["summary"]["eyes"], 0)
    # the other rollups keep working at both levels
    check("files sum across the whole project", bg["summary"]["files"], 10)
    check("when is the latest descendant timestamp",
          bg["summary"]["when"], "2026-09-18T04:00:00Z")
    check("one agreed model rolls up as itself",
          bg["summary"]["model"], "qwen3.8:27b-q4_K_M")
    check("differing hosts roll up as a count", bg["summary"]["host"], "2 hosts")
    check("slice-level rollup still reports its own files sum",
          bg["entries"][0]["summary"]["files"], 6)
    check("project total is the sum of its slice groups' totals", bg["summary"]["total"], 3)
    check("the needs_eyes/handled split is untouched by the project layer",
          [r["group"] for r in bg["rows"]],
          ["handled", "good_to_go", "needs_eyes", "good_to_go"])

    # A bundle whose finished slices ALL passed but which still OWES slices (X < Y)
    # must NOT read "good to go -- nothing owed" (the user 2026-09-18: the green bar listed
    # bundles that weren't fully done). Same all-PASS aw-scan rows, but now the plan
    # progress says 1 of 3 slices are through -> the project drops to needs_eyes.
    inc_rows = [dict(t4) for t4 in [trows[4]]]  # aw-scan-s1-fares, PASS/good_to_go
    _annotate_run_parents(inc_rows, preverse,
                          {"aw-scan": {"order": ["s1-fares"], "total": 3}},
                          progress={"aw-scan": (1, 3)})
    _annotate_run_projects(inc_rows)
    inc_tree = _build_run_tree(inc_rows)
    check("an all-PASS but INCOMPLETE bundle is NOT good_to_go (slices still owed)",
          inc_tree[0]["section"], "needs_eyes")
    # control: the SAME rows with the plan complete (3/3) stay good_to_go
    com_rows = [dict(trows[4])]
    _annotate_run_parents(com_rows, preverse,
                          {"aw-scan": {"order": ["s1-fares"], "total": 3}},
                          progress={"aw-scan": (3, 3)})
    _annotate_run_projects(com_rows)
    check("a COMPLETE all-PASS bundle stays good_to_go",
          _build_run_tree(com_rows)[0]["section"], "good_to_go")

    # The rollup BADGE must match the section clamp: an incomplete bundle's summary
    # carries incomplete=True so rollupBadges renders "N passed - slices still owed"
    # (amber), never "all good to go (N)" (green). the user 2026-09-18: cc-waitlist's flags
    # read "all good to go (1)" with total=6, slicesDone=1 (5 slices not even started).
    check("incomplete bundle summary flags incomplete=True",
          _build_run_tree(inc_rows)[0]["summary"]["incomplete"], True)
    check("complete bundle summary flags incomplete=False",
          _build_run_tree(com_rows)[0]["summary"]["incomplete"], False)
    # A PASS child carrying bundle_incomplete keeps its good count (not zeroed) but the
    # summary flags incomplete so the badge re-labels it amber, never green. This mirrors
    # the real cc-waitlist row (grp=good_to_go, verdict=PASS, bundle_incomplete=True).
    inc_sum = _summarize_rows([{"slice_id": "s4-on", "parent": "cc-waitlist",
                                "group": "good_to_go", "raw_verdict": "PASS",
                                "bundle_incomplete": True}], total=6)
    check("a PASS child with bundle_incomplete makes the summary incomplete",
          inc_sum["incomplete"], True)
    check("...and its good count is not zeroed, just re-labelled by the badge",
          inc_sum["good"], 1)
    # A COMPLETE plan whose done-slices are not all present as run-rows (total>slicesDone)
    # must NOT be mislabelled incomplete -- bundle_incomplete is the only signal.
    com_sum = _summarize_rows([{"slice_id": "s3", "parent": "done-plan",
                                "group": "good_to_go", "raw_verdict": "PASS",
                                "bundle_incomplete": False}], total=6)
    check("total>slicesDone alone does NOT mark a complete bundle incomplete",
          com_sum["incomplete"], False)

    # VERDICT MODEL (the user 2026-09-18): you cannot pass/partially-pass/fail an unfinished
    # checklist, and a superseded FAIL must not contradict "all good to go".
    # (1) incomplete bundle with a passing slice -> PENDING, never "PASS (partial)".
    part = _summarize_rows([
        {"slice_id": "s1", "parent": "p", "group": "good_to_go", "raw_verdict": "PASS",
         "bundle_incomplete": True}], total=3)
    check("an incomplete bundle's verdict is PENDING, not 'PASS (partial)'",
          (part["verdict"], part["rawVerdict"]), ("PENDING", "PENDING"))
    # (2) a COMPLETE bundle whose only FAIL was superseded (auto-handled) reads PASS,
    #     matching the flags -- the FAIL must not resurrect in the verdict.
    sup = _summarize_rows([
        {"slice_id": "s1", "parent": "p", "group": "handled", "raw_verdict": "FAIL",
         "bundle_incomplete": False},
        {"slice_id": "s1", "parent": "p", "group": "good_to_go", "raw_verdict": "PASS",
         "bundle_incomplete": False},
        {"slice_id": "s2", "parent": "p", "group": "good_to_go", "raw_verdict": "PASS",
         "bundle_incomplete": False}], total=2)
    check("a superseded (handled) FAIL does not override live PASSes in the verdict",
          sup["verdict"], "PASS")
    check("...and such a bundle is not flagged incomplete", sup["incomplete"], False)
    # (3) a LIVE (needs-eyes) FAIL in a complete bundle still reads FAIL.
    liv = _summarize_rows([
        {"slice_id": "s1", "parent": "p", "group": "needs_eyes", "raw_verdict": "FAIL",
         "bundle_incomplete": False},
        {"slice_id": "s2", "parent": "p", "group": "good_to_go", "raw_verdict": "PASS",
         "bundle_incomplete": False}], total=2)
    check("a live FAIL in a complete bundle still reads FAIL", liv["verdict"], "FAIL")
    # (4) a running re-run (live 'other') keeps a bundle PENDING even at X==Y.
    runx = _summarize_rows([
        {"slice_id": "s1", "parent": "p", "group": "needs_eyes", "raw_verdict": None,
         "bundle_incomplete": False}], total=1)
    check("a live non-terminal run keeps the bundle PENDING", runx["verdict"], "PENDING")

    # --- "send to bottom" queue control (2026-09-18, the user's dashboard queue panel)
    # -- prove before_id=None already means "append to the end" in _reorder_job, the
    # pure function the dashboard's new send-to-bottom button relies on (it reuses
    # the existing POST /api/jobs/move with before_id: null rather than a new
    # endpoint, precisely because this was already true).
    jobs_a = [{"id": "x"}, {"id": "y"}, {"id": "z"}]
    check("_reorder_job(before_id=None) sends the job to the end (send-to-bottom)",
          (_reorder_job(jobs_a, "x", None), [j["id"] for j in jobs_a]),
          (True, ["y", "z", "x"]))
    jobs_b = [{"id": "x"}, {"id": "y"}, {"id": "z"}]
    check("_reorder_job with a real before_id still inserts before it (unchanged path)",
          (_reorder_job(jobs_b, "z", "y"), [j["id"] for j in jobs_b]),
          (True, ["x", "z", "y"]))
    jobs_c = [{"id": "x"}]
    check("_reorder_job(before_id=None) on an already-last job is a same-position no-op",
          (_reorder_job(jobs_c, "x", None), [j["id"] for j in jobs_c]),
          (True, ["x"]))
    check("_reorder_job returns False for an unknown job id (nothing to move)",
          _reorder_job([{"id": "x"}], "nope", None), False)

    # --- queue display order must not jump on a held -> pending transition
    # (2026-09-18, the user: "the view jumps when a job's status changes"). A gate
    # barrier lifting is NOT a reorder: the job's place in the launch order is
    # unchanged, so its ROW must keep its index. This fails without the shared
    # pending/held/paused tier (held used to fall through to the bottom tier).
    qjobs = [{"id": "r1", "status": "running"},
             {"id": "p1", "status": "pending"},
             {"id": "h1", "status": "held"},
             {"id": "p2", "status": "pending"},
             {"id": "z1", "status": "paused"},
             {"id": "f1", "status": "failed"}]
    before = [j["id"] for j in _queue_display_order(qjobs)]
    check("queue display keeps true FIFO order within the waiting tier",
          before, ["r1", "p1", "p2", "z1", "f1", "h1"])
    # HELD SINKS TO THE BOTTOM (2026-09-18, the user: the parked bonsai job cluttered
    # the middle of the active worklist). A hold is sticky, not a transient flip.
    check("a held job sorts BELOW every pending job",
          before.index("h1") > max(before.index("p1"), before.index("p2")), True)
    check("a held job sorts below a paused one too",
          before.index("h1") > before.index("z1"), True)
    check("held is its own bottom tier, strictly below pending",
          _queue_status_tier("held") > _queue_status_tier("pending"), True)
    check("held is at or below the unknown-status default tier",
          _queue_status_tier("held") >= _QUEUE_STATUS_DEFAULT_TIER, True)
    # ... but the TRANSIENT states the shared tier was built for do NOT sink, and a
    # held -> pending lift still lands the row in the pending block, not a new group.
    for j in qjobs:
        if j["id"] == "h1":
            j["status"] = "pending"   # the gate barrier lifts -- nothing else changes
    after = [j["id"] for j in _queue_display_order(qjobs)]
    check("a lifted held job rejoins the pending tier in true FIFO position",
          after, ["r1", "p1", "h1", "p2", "z1", "f1"])
    # --- planned rows sit IN the queue, where they will actually run -----------
    # the user 2026-09-18: "can we put the planned runs in the queue where they'll fall
    # when we run them? ... they are essentially queued just waiting on the job
    # before it to finish like other jobs in the queue."
    check("planned shares the waiting tier with pending (not its own block)",
          _queue_status_tier("planned"), _queue_status_tier("pending"))
    dag = [{"id": "run1", "status": "running"},
           {"id": "p1", "status": "pending", "label": "other-work"},
           {"id": "p2", "status": "pending", "label": "more-work"},
           {"id": "h1", "status": "held"},
           # chain A: its first slice waits on the RUNNING job -> genuinely next
           {"id": "a1", "status": "planned", "label": "alpha-s1", "after": "run1"},
           {"id": "a2", "status": "planned", "label": "alpha-s2", "after": "a1"},
           {"id": "a3", "status": "planned", "label": "alpha-s3", "after": "a2"},
           # chain B: nothing to wait on yet -> after everything already runnable
           {"id": "b1", "status": "planned", "label": "beta-s1", "after": None},
           {"id": "b2", "status": "planned", "label": "beta-s2", "after": "b1"}]
    seq = [j["id"] for j in _queue_display_order(dag)]
    check("the queue reads as the real execution sequence",
          seq, ["run1", "a1", "a2", "a3", "p1", "p2", "b1", "b2", "h1"])
    check("a planned slice waiting on the RUNNING job slots right under it",
          seq.index("a1"), seq.index("run1") + 1)
    for parent, child in (("a1", "a2"), ("a2", "a3"), ("b1", "b2")):
        check(f"a planned child ({child}) never sorts above its planned parent ({parent})",
              seq.index(child) > seq.index(parent), True)
    check("the true FIFO order of the runnable rows is never rearranged",
          seq.index("p1") < seq.index("p2"), True)
    check("held is still the only thing at the bottom", seq[-1], "h1")
    check("a queue with no planned rows is ordered exactly as before",
          [j["id"] for j in _queue_display_order(dag[:4])],
          [j["id"] for j in sorted(dag[:4],
                                   key=lambda j: _queue_status_tier(j["status"]))])
    _ann = _annotate_display_seq([dict(j) for j in dag])
    check("display_seq matches that order (the front-end sorts by it)",
          [j["id"] for j in sorted(_ann, key=lambda j: j["display_seq"])], seq)
    # --- a bundle stays PUT until it completes (2026-09-19) --------------------
    # THE BUG the user reported live: "queue still seems to be jumping around and not
    # staying in a single bundle until completion." A slice's `after` points at the
    # job for the PREVIOUS slice, and that job is reaped when it finishes, so mid-plan
    # the dep resolves to nothing and the row fell to the tail of the region -- while
    # the plan's one live `auto-author-...` row sat near the front. The bundle then
    # renders at plan_seq = min(child display_seq), i.e. at the ephemeral row, and
    # teleports the length of the queue the instant that row is reaped. Shape taken
    # from the live /api/jobs: bg-health (auto-author s2 at 6; s1/s3/s4 at 140-142).
    # `ha` is near the FRONT with unrelated runnable work BEHIND it -- the live shape,
    # and the only shape that distinguishes "with its bundle" from "at the tail".
    _jump = [{"id": "run1", "status": "running", "label": "someone-else"},
             {"id": "ha", "status": "pending", "group_key": "bg-health",
              "label": "auto-author-bg-health-s2-classify", "after": None},
             {"id": "o1", "status": "pending", "label": "other-1"},
             {"id": "o2", "status": "pending", "label": "other-2"},
             # s1's dep is gone entirely (reaped), s3 chains off a reaped job too
             {"id": "h1", "status": "planned", "group_key": "bg-health",
              "label": "bg-health-s1-status", "after": None},
             {"id": "h3", "status": "planned", "group_key": "bg-health",
              "label": "bg-health-s3-heartbeat", "after": "reaped-564bb33a"},
             {"id": "h4", "status": "planned", "group_key": "bg-health",
              "label": "bg-health-s4-report", "after": "h3"}]
    _jseq = [j["id"] for j in _queue_display_order(_jump)]
    check("THE BUG: a plan's stranded planned slices sit WITH their bundle, not at "
          "the tail of the queue",
          _jseq, ["run1", "ha", "h1", "h3", "h4", "o1", "o2"])
    _jann = _annotate_display_seq([dict(j) for j in _jump])
    _jby = {j["id"]: j for j in _jann}
    _anchor = min(_jby[i]["display_seq"] for i in ("ha", "h1", "h3", "h4"))
    # The handover: the ephemeral auto-author row is reaped, its coding row has not
    # appeared yet. The bundle's anchor must barely move -- NOT jump to the tail.
    _after_reap = _annotate_display_seq([dict(j) for j in _jump if j["id"] != "ha"])
    _aby = {j["id"]: j for j in _after_reap}
    _anchor2 = min(_aby[i]["display_seq"] for i in ("h1", "h3", "h4"))
    check("a bundle's anchor does not move when its ephemeral auto-author row is "
          "reaped mid-plan",
          _anchor2 - _anchor, 0)
    check("every row of the plan is contiguous in the display order",
          [i for i, j in enumerate(_jseq) if _jby[j].get("group_key") == "bg-health"],
          [1, 2, 3, 4])
    check("group affinity keeps the plan's slices in slice order",
          [_jseq.index(i) for i in ("h1", "h3", "h4")],
          sorted(_jseq.index(i) for i in ("h1", "h3", "h4")))
    check("group affinity never outranks a REAL dep chain (h4 still follows h3)",
          _jseq.index("h4") > _jseq.index("h3"), True)
    check("the runnable rows keep their true FIFO order through the fix",
          _jseq.index("ha") < _jseq.index("o1") < _jseq.index("o2"), True)
    # The other half, measured live: bg-escalation's planned s4/s5 sat at display_seq
    # 1-2 (top of the queue) because their dep resolved to a job "elsewhere", while
    # the slice actually being worked sat at 141. Rule 3's front-of-region hoist must
    # yield to the bundle whenever the bundle already has a row placed.
    _hoist = [{"id": "run1", "status": "running", "label": "unrelated-running"},
              {"id": "q1", "status": "pending", "label": "unrelated-1"},
              {"id": "q2", "status": "pending", "label": "unrelated-2"},
              {"id": "e3", "status": "pending", "group_key": "bg-escalation",
               "label": "auto-refine-bg-escalation-s3-letter-r1", "after": None},
              {"id": "e4", "status": "planned", "group_key": "bg-escalation",
               "label": "bg-escalation-s4-router", "after": "run1"},
              {"id": "e5", "status": "planned", "group_key": "bg-escalation",
               "label": "bg-escalation-s5-audit", "after": "e4"}]
    _hseq = [j["id"] for j in _queue_display_order(_hoist)]
    check("THE BUG: a planned slice does not hoist to the top of the queue past its "
          "OWN bundle's live row",
          _hseq, ["run1", "q1", "q2", "e3", "e4", "e5"])
    check("...and front-of-region still applies when the bundle has nothing placed",
          [j["id"] for j in _queue_display_order(
              [{"id": "run1", "status": "running", "label": "r"},
               {"id": "q1", "status": "pending", "label": "q"},
               {"id": "n1", "status": "planned", "group_key": "newplan",
                "label": "newplan-s1", "after": "run1"},
               {"id": "n2", "status": "planned", "group_key": "newplan",
                "label": "newplan-s2", "after": "n1"}])],
          ["run1", "n1", "n2", "q1"])
    check("an UNGROUPED root planned row still lands at the tail, exactly as before",
          [j["id"] for j in _queue_display_order(
              [{"id": "run1", "status": "running", "label": "solo-run"},
               {"id": "p1", "status": "pending", "label": "solo-pending"},
               {"id": "b1", "status": "planned", "label": "solo-planned",
                "after": None}])],
          ["run1", "p1", "b1"])
    # A cyclic/self-referential dep must not hang the panel.
    _cyc = [{"id": "c1", "status": "planned", "after": "c2"},
            {"id": "c2", "status": "planned", "after": "c1"}]
    check("a dependency cycle still terminates and renders both rows",
          sorted(j["id"] for j in _queue_display_order(_cyc)), ["c1", "c2"])
    check("paused still shares pending's tier (transient, must not jump)",
          _queue_status_tier("paused"), _queue_status_tier("pending"))
    check("running still sorts above the waiting tier",
          _queue_status_tier("running") < _queue_status_tier("pending"), True)
    check("an unknown status still falls to the bottom tier",
          _queue_status_tier("no-such-status"), _QUEUE_STATUS_DEFAULT_TIER)
    check("a real reorder DOES move the row (only reorders move rows)",
          [j["id"] for j in _queue_display_order(
              [qjobs[0], qjobs[2], qjobs[1], qjobs[3], qjobs[4], qjobs[5]])],
          ["r1", "h1", "p1", "p2", "z1", "f1"])
    check("the front-end gets the same tier map (no hand-copied duplicate)",
          f"const STATUS_ORDER = {json.dumps(QUEUE_STATUS_ORDER)};" in FRONTEND_HTML, True)

    # --- bundle header: a failure must not hide next to the counter (2026-09-19) ----
    # the user, on bg-escalation: "right now the counter makes it seem like its already
    # done when its failed". These are BEHAVIOURAL, not containment: the real shipped
    # planAlertSummary is cut out of the served page between its markers and EXECUTED
    # under node, so what is asserted is the markup a bundle header actually renders.
    check("the front-end gets the alert statuses from Python too (no hand-copy)",
          f"const ALERT_STATUSES = {json.dumps(PLAN_ALERT_STATUSES)};" in FRONTEND_HTML,
          True)
    # WHICH rows may alarm: the server's judgement, on the underlying SLICE, not the
    # row's status (the user: "is the failure something i need to care about is a better
    # way to put it"). The fixture IS the bg-escalation shape that prompted it.
    _esc = {"label": "bg-escalation",
            "order": ["s3-letter", "s5-audit"],
            "slices": {"s3-letter": {"status": "done", "job_id": "jpass"},
                       "s5-audit": {"status": "pending", "job_id": "jrun"}}}
    _escrows = [
        {"id": "r-refine", "group_key": "bg-escalation", "status": "failed",
         "label": "auto-refine-bg-escalation-s3-letter-r2"},
        {"id": "r-run", "group_key": "bg-escalation", "status": "running",
         "label": "auto-author-bg-escalation-s5-audit"},
    ]
    _att = lambda rows, state, verdicts: _needs_attention_ids(
        rows, state_of=lambda k: state, verdict_of=lambda j, ld=None: verdicts.get(j))
    check("THE BUG: a failed refine on a slice that already PASSED does not alarm",
          _att(_escrows, _esc, {"jpass": "PASS"}), set())
    check("...nor does one on a slice the plan deliberately SKIPPED",
          _att([_escrows[0]],
               {"slices": {"s3-letter": {"status": "skipped", "job_id": None}}},
               {}), set())
    check("...nor on a done slice whose gate never ran (no verdict recorded)",
          _att([_escrows[0]], _esc, {}), set())
    check("but the SAME row DOES alarm when its slice's gate said FAIL",
          _att(_escrows, _esc, {"jpass": "FAIL"}), {"r-refine"})
    check("...and when the slice is still unresolved (not a terminal status)",
          _att([{"id": "r-f", "group_key": "p", "status": "failed", "label": "p-s1"}],
               {"slices": {"s1": {"status": "escalated", "job_id": "j"}}}, {}),
          {"r-f"})
    check("...and when the plan state cannot be read at all (alarm on no evidence)",
          _att([{"id": "r-f", "group_key": "p", "status": "failed", "label": "p-s1"}],
               None, {}),
          {"r-f"})
    check("a blocked row on an unresolved slice alarms, on a landed one does not",
          (_att([{"id": "b1", "group_key": "p", "status": "blocked", "label": "p-s1"}],
                {"slices": {"s1": {"status": "pending"}}}, {}),
           _att([{"id": "b1", "group_key": "p", "status": "blocked", "label": "p-s1"}],
                {"slices": {"s1": {"status": "done", "job_id": "j"}}}, {"j": "PASS"})),
          ({"b1"}, set()))
    check("a SUCCESS row is never an alarm however unresolved its slice",
          _att([{"id": "ok", "group_key": "p", "status": "running", "label": "p-s1"}],
               {"slices": {"s1": {"status": "pending"}}}, {}), set())
    check("an already-superseded failed row is not double-counted as an alarm",
          _needs_attention_ids(_escrows, state_of=lambda k: _esc,
                               verdict_of=lambda j, ld=None: "FAIL",
                               superseded={"r-refine"}), set())
    check("the rollup stamps the judgement on every row for the front-end",
          all("needs_attention" in r for r in _annotate_plan_rollup(
              [{"id": "x", "group_key": "p", "status": "failed", "label": "p-s1",
                "display_seq": 1}])), True)

    _ajs = re.search(r"// BEGIN planAlertSummary[^\n]*\n(.*?)// END planAlertSummary",
                     FRONTEND_HTML, re.S)
    check("the executable planAlertSummary block is still delimited in the page",
          bool(_ajs), True)
    _node = shutil.which("node")
    if _ajs and _node:
        _probe = _ajs.group(1) + """
// A failed row whose SLICE is unresolved -- the server flagged it, so it alarms.
const withFail = planAlertSummary({running: 1, planned: 3, failed: 1}, {failed: 1});
// The exact bg-escalation shape: same failed row, but its slice already landed, so
// the server did NOT flag it. Same counts, and it must not alarm.
const moot     = planAlertSummary({running: 1, planned: 3, failed: 1}, {});
const clean    = planAlertSummary({running: 1, planned: 3}, {});
console.log(JSON.stringify({
  failBadge: withFail.badge, failRest: withFail.rest, failRow: withFail.rowClass,
  mootBadge: moot.badge, mootRest: moot.rest, mootRow: moot.rowClass,
  cleanBadge: clean.badge, cleanRest: clean.rest, cleanRow: clean.rowClass,
  // Two failed rows, one unresolved: alarm on the one, report the other.
  mixed: planAlertSummary({failed: 2}, {failed: 1}),
  blocked: planAlertSummary({blocked: 2}, {blocked: 2}).alerts,
  unconv: planAlertSummary({done_unconverged: 1}, {done_unconverged: 1}).alerts,
  unknown: planAlertSummary({weird: 1}, {weird: 1}).rest,
}));
"""
        with tempfile.NamedTemporaryFile("w", suffix=".mjs", delete=False) as _af:
            _af.write(_probe)
            _apath = _af.name
        try:
            _ar = subprocess.run([_node, _apath], capture_output=True, text=True,
                                 timeout=30)
        finally:
            os.unlink(_apath)
        check("the extracted front-end block actually runs (node exit 0)",
              (_ar.returncode, _ar.stderr.strip()[:200]), (0, ""))
        _a = json.loads(_ar.stdout) if _ar.returncode == 0 else {}
        check("THE BUG: a bundle with a failed row renders a distinct alert badge",
              ('class="plan-alert"' in _a.get("failBadge", "")
               and "1 failed" in _a.get("failBadge", "")), True)
        check("...and an otherwise identical clean bundle renders NO badge at all",
              _a.get("cleanBadge"), "")
        check("...the failure LEAVES the faint routine breakdown (badge, not a dupe)",
              (_a.get("failRest"), _a.get("cleanRest")),
              (["1 running", "3 planned"], ["1 running", "3 planned"]))
        check("...and the whole ROW is flagged, so it is findable while scanning",
              (_a.get("failRow"), _a.get("cleanRow")), (" queue-parent-alert", ""))
        # the user: "is the failure something i need to care about is a better way to put
        # it." Identical COUNTS to the alarming case -- only the server's per-row
        # judgement differs -- so this can only pass by honouring needs_attention.
        check("THE BUG: a failed row on an already-landed slice raises NO alarm",
              (_a.get("mootBadge"), _a.get("mootRow")), ("", ""))
        check("...and it is still REPORTED, demoted to the faint breakdown, not hidden",
              _a.get("mootRest"), ["1 running", "3 planned", "1 failed"])
        check("...and a bundle with one moot and one live failure alarms about ONE",
              (_a.get("mixed", {}).get("alerts"), _a.get("mixed", {}).get("rest")),
              (["1 failed"], ["1 failed"]))
        check("blocked and done_unconverged are alerts too, not routine grey text",
              (_a.get("blocked"), _a.get("unconv")),
              (["2 blocked"], ["1 done_unconverged"]))
        check("an unknown status still lands in the routine breakdown, never dropped",
              _a.get("unknown"), ["1 weird"])
    elif not _node:
        print("  SKIP planAlertSummary behavioural checks: no `node` on PATH")

    # --- second-opinion rows resolve through their PARENT job id (the user 2026-10-01) ---
    _bvs = _bundle_view_lib()[0]
    _sp = {"id": "b0c6d90fdf38", "label": "p1-s2-thing", "status": "running",
           "group_key": "p1"}
    _sop = {"id": "so1", "label": "secondop-b0c6d90fdf38", "status": "pending"}
    check("a secondop row joins the bundle of its live parent",
          _annotate_job_groups([dict(_sp, group_key=None), dict(_sop)])[1]["group_key"], "p1")
    check("an unresolvable secondop parent stays loose (no crash)",
          _annotate_job_groups([{"id": "so2", "label": "secondop-ffffffffffff",
                                 "status": "pending"}])[0]["group_key"],
          "secondop-ffffffffffff")
    check("secondop maps to its parent's slice via the live parent, or the durable label",
          (_bvs.job_sid(_sop, "p1", ["s1", "s2-thing"], {"b0c6d90fdf38": _sp}),
           _bvs.job_sid(_sop, "p1", ["s1", "s2-thing"], {},
                        label_of=lambda i: "p1-s2-thing"),
           _bvs.job_sid(_sop, "p1", ["s1", "s2-thing"], {})), ("s2-thing", "s2-thing", None))
    check("secondop is a distinct 'second opinion' kind/phase",
          _bvs.job_kind(_sop), ("second opinion", "second-opinion"))
    _ss = {"status": "enqueued", "job_id": "b0c6d90fdf38"}
    _pass = lambda i: "pass"
    check("a PENDING secondop never changes the slice phase; a passed parent stays done",
          (_bvs.slice_phase("s2-thing", _ss, "p1", [_sop], lambda i: None, [], None, None,
                            1000.0)[0],
           _bvs.slice_phase("s2-thing", _ss, "p1", [dict(_sop, status="running")], _pass,
                            [], None, None, 1000.0)[0]), ("gate", "done"))
    check("a RUNNING secondop makes the slice active, but a running coding job outranks it",
          (_bvs.slice_phase("s2-thing", {"status": "enqueued"}, "p1",
                            [dict(_sop, status="running")], lambda i: None, [], None, None,
                            1000.0)[0],
           _bvs.slice_phase("s2-thing", {"status": "enqueued"}, "p1",
                            [dict(_sop, status="running"), dict(_sp, status="running")],
                            lambda i: None, [], None, None, 1000.0)[0]),
          ("second-opinion", "coding"))

    # --- `--bundle` jobs with no slicer plan: pseudo-slices + tagged children ------------
    _tj = [{"id": "849f2521c0d7", "label": "rt-amazon-detail-orderid", "status": "done",
            "bundle": "rt-941-fixes"},
           {"id": "722d979c6a59", "label": "rt-missing-tracking-pending", "status": "done",
            "bundle": "rt-941-fixes"},
           {"id": "g1", "label": "gate-722d979c6a59", "status": "pending",
            "bundle": "rt-941-fixes", "host_pref": "unraid"},
           {"id": "eea8e2a7134c", "label": "rt-bg-cc-exclude", "status": "running",
            "lane": "studio-db", "bundle": "rt-941-fixes"}]
    # the gate's parent is already REAPED: only its done.json label (untagged) remains
    _ld2 = Path(tempfile.mkdtemp())
    (_ld2 / "722d979c6a59.done.json").write_text(
        json.dumps({"label": "rt-missing-tracking-pending"}))
    _gk2 = _annotate_job_groups([dict(j) for j in _tj if j["id"] != "722d979c6a59"],
                                log_dir=_ld2)
    check("an explicit --bundle tag beats the gate's parent-label relink (gate stays in the bundle)",
          {r["label"]: r["group_key"] for r in _gk2}["gate-722d979c6a59"], "rt-941-fixes")
    _jv = _bundle_view_lib()[0].build_job_view("rt-941-fixes", _tj)
    check("a plan-less bundle renders one pseudo-slice per JOB (children are not slices)",
          ([r["title"] for r in _jv["slices"]], _jv["total"], _jv["through"], _jv["unit"]),
          (["rt-amazon-detail-orderid", "rt-missing-tracking-pending", "rt-bg-cc-exclude"],
           3, 2, "jobs"))
    _p722 = next(r for r in _jv["slices"] if r["sid"] == "722d979c6a59")
    check("...and the gate row nests under ITS parent job, queued, not as a loose row",
          ([(h["kind"], h["id"]) for h in _p722["history"]], _p722["job"]),
          ([("coding", "722d979c6a59"), ("gate", "g1")], True))
    check("the running job's pseudo-slice is the active one",
          (_jv["current"], next(r for r in _jv["slices"] if r["sid"] == "eea8e2a7134c")["phase"]),
          ("eea8e2a7134c", "coding"))
    _ov = _bundle_view_lib()[0].build_job_view(
        "x", [_tj[2]], result_of=lambda i: {"label": "rt-missing-tracking-pending"})
    check("a gate whose parent job was REAPED nests under a pseudo-slice from its durable label",
          [(r["title"], [h["id"] for h in r["history"]]) for r in _ov["slices"]],
          [("rt-missing-tracking-pending", ["722d979c6a59", "g1"])])
    check("...and with no label to recover it still yields a pseudo-slice, never a crash",
          _bundle_view_lib()[0].build_job_view("x", [_tj[2]])["slices"][0]["title"],
          "722d979c6a59")
    _gw = _wait_reason_for(_tj[2], "ev-service", {}, [_tj[3]])
    check("a gate pinned to unraid never says 'held on running job' for a studio job",
          ("held on running job" not in _gw and "committed bundle ev-service" in _gw), True)
    check("...but a plain row still names the busy lane's job",
          _wait_reason_for({"id": "z", "label": "x", "status": "pending", "lane": "studio-db"},
                           None, {}, [_tj[3]], {}).startswith("held on running job eea8e2a7134c"), True)
    # --- Darkbloom slot saturation names itself (only from parsed `darkbloom status`) ---
    _dbr = {"id": "z", "label": "x", "status": "pending", "lane": "studio-db", "model": "m"}
    _dbfull = {"unfinished": 4, "concurrency": {}, "default_cap": 4}
    check("all Darkbloom slots busy (local+fleet) -> 'held on Darkbloom: 4/4 slots busy'",
          _wait_reason_for(_dbr, None, {}, [], _dbfull).startswith("held on Darkbloom: 4/4 slots busy"), True)
    check("...and it outranks a same-lane busy job (the slots are the real wall)",
          _wait_reason_for(_dbr, None, {}, [_tj[3]], _dbfull).startswith("held on Darkbloom"), True)
    check("free slots -> the busy-lane reason carries the Darkbloom load",
          _wait_reason_for(_dbr, None, {}, [_tj[3]], {"unfinished": 2, "default_cap": 4}).endswith(
              "; Darkbloom 2/4 slots busy"), True)
    check("a non-Darkbloom row never mentions Darkbloom",
          "Darkbloom" in _wait_reason_for({**_dbr, "lane": "unraid"}, None, {}, [], _dbfull), False)
    check("unparseable status -> no Darkbloom claim (degrade, never guess)",
          _wait_reason_for(_dbr, None, {}, [], {}), "next in line")
    check("the front-end groups --bundle rows into a bundle", "(j.plan_bundle || j.bundle) && j.group_key" in FRONTEND_HTML, True)

    # --- the timestamped esc-review form must nest in its plan's bundle AND slice ----
    _EL3 = "esc-review-32637Z-ev-service-screen-1-rivian-service-s3-pick-work-order"
    _EP3 = "ev-service-screen-1-rivian-service"
    _s3 = ["s1-request-status-map", "s3-pick-work-order"]
    _bv3 = _bundle_view_lib()[0]
    check("esc-review-<stamp>Z-<plan>-<slice> resolves to its slice, kind escalation-review",
          (_bv3.esc_review_ref(_EL3), _bv3.job_sid({"label": _EL3}, _EP3, _s3),
           _bv3.job_kind({"label": _EL3})),
          ("ev-service-screen-1-rivian-service-s3-pick-work-order", "s3-pick-work-order",
           ("escalation review", "escalation-review")))
    check("...and the full-stamp and bare forms still resolve",
          [_bv3.job_sid({"label": l}, _EP3, _s3) for l in (
              "esc-review-20261002T012432Z-" + _EP3 + "-s3-pick-work-order",
              "esc-review-" + _EP3 + "-s3-pick-work-order")], ["s3-pick-work-order"] * 2)
    _g3 = _annotate_job_groups([
        {"id": "e", "label": _EL3, "status": "running"},
        {"id": "e2", "label": _EL3, "status": "running",
         "bundle": "esc-review-32637Z-" + _EP3}])      # old mangled stamp
    check("it joins the plan's bundle, even when stamped with a mangled esc-review bundle",
          [r["group_key"] for r in _g3], [_EP3, _EP3])
    _tv = _bv3.build_job_view("esc-review-32637Z-" + _EP3,
                              [{"id": "e", "label": _EL3, "status": "running"}])
    check("an esc-review row is never a pseudo-slice job of a per-job bundle", _tv, None)
    check("the stage row is named 'escalation / heal'",
          "return 'escalation / heal'" in FRONTEND_HTML, True)

    # --- esc-review: job-<id> form, Run Status grouping, live-now tag, eval arms ------
    check("esc-review job form parses to the parent job id; slug form does not",
          (_esc_review_job_id("esc-review-20261002T012432Z-job-10293117a1d0"),
           _esc_review_job_id("esc-review-41Z-ev-service-s1-request-status-map"),
           _esc_review_job_id("esc-review-41Z-p-s1-x")), ("10293117a1d0", None, None))
    _ej = {"id": "10293117a1d0", "label": "sidecar-bfmr-sink-s1-thing", "status": "done",
           "group_key": "sidecar-bfmr-sink"}
    _er2 = {"id": "er", "label": "esc-review-20261002T012432Z-job-10293117a1d0",
            "status": "running"}
    check("a job-form esc-review nests under its escalated JOB's bundle, not the focused one",
          _annotate_job_groups([dict(_ej), dict(_er2)])[1]["group_key"], "sidecar-bfmr-sink")
    check("...and an unresolvable job parent stays loose (no crash)",
          _annotate_job_groups([dict(_er2, id="er2", label="esc-review-1Z-job-ffffffffffff")]
                               )[0]["group_key"].startswith("esc-review"), True)
    check("live-now display names: slug form drops the bundle, job form names the job",
          (_esc_review_display("esc-review-41Z-p-s1-x", "p"),
           _esc_review_display("esc-review-1Z-job-abc123def456"),
           _esc_review_display("auto-author-p-s1-x")),
          ("Escalation review \u00b7 s1-x", "Escalation review \u00b7 job abc123def456", None))
    _bvj = _bundle_view_lib()[0]
    check("bundle_view maps a job-form esc-review to the slice of its job",
          _bvj.job_sid(_er2, "sidecar-bfmr-sink", ["s1-thing"],
                       {"10293117a1d0": _ej}), "s1-thing")
    check("the live-now strip tags each job with ITS OWN bundle, not the focused one",
          ("bundle in progress" not in FRONTEND_HTML and "a.group_key" in FRONTEND_HTML), True)
    _rp = _annotate_run_parents([
        {"label": "esc-review-02T001032Z-ev-service-screen-1-rivian-service-s2-appt-number"},
        {"label": "esc-review-180229Z-sidecar-bfmr-login-fetch-s2-installinterceptor-conte"},
        {"label": "esc-review-1Z-job-ffffffffffff"}], reverse={}, projects={}, progress={})
    check("Run Status: esc-review rows join their slice's bundle, with a clean label",
          ([(r["parent"], r["slice_id"]) for r in _rp[:2]], _rp[0].get("display_label")),
          ([("ev-service-screen-1-rivian-service", "s2-appt-number"),
            ("sidecar-bfmr-login-fetch", "s2-installinterceptor-conte")],
           "Escalation review \u00b7 s2-appt-number"))
    check("...and an unresolved job-form review is standalone, not an 'esc' project",
          (_rp[2]["parent"], _project_prefix(_rp[2]["label"], _rp[2]["parent"])),
          (None, (None, None)))
    check("toolrel eval arms fold into the collapsed auto-handled section",
          [r["group"] for r in _annotate_run_groups([
              {"label": "toolrel2-9b-st03", "verdict": "PENDING"},
              {"label": "toolrel3-9b-ns04", "verdict": "PENDING"}])], ["handled", "handled"])
    check("the slice caret is the only disclosure arrow; the running marker is a dot",
          ("slice-dot" in FRONTEND_HTML and "str.dataset.open = open ? '1' : '0'" in FRONTEND_HTML
           and ": sl.active ? '&#9654;'" not in FRONTEND_HTML), True)

    # --- escalation-review rows belong to their bundle/slice (the user 2026-10-01) -------
    _EL = "esc-review-41Z-ev-service-screen-1-rivian-service-s1-request-status-map"
    _EP = "ev-service-screen-1-rivian-service"
    _bvm = _bundle_view_lib()[0]
    _egr = _annotate_job_groups([{"id": "e1", "label": _EL, "status": "pending"}])
    check("an esc-review row groups under its bundle, not as a loose row",
          _egr[0]["group_key"], _EP)
    check("...and so does a TS-less / truncated label",
          _annotate_job_groups([{"id": "e2", "status": "pending",
              "label": "esc-review-" + _EL.split("-", 3)[3]}])[0]["group_key"], _EP)
    check("esc-review maps to the slice and a distinct escalation-review kind/phase",
          (_bvm.job_sid({"label": _EL}, _EP, ["s1-request-status-map", "s2-x"]),
           _bvm.job_kind({"label": _EL})), ("s1-request-status-map",
                                            ("escalation review", "escalation-review")))
    check("a non-esc label is untouched by the esc-review parser",
          (_bvm.esc_review_ref("auto-author-p-s1-x"), _esc_review_ref("esc-reviewer-x")),
          (None, None))
    _ew = {"id": "e3", "label": _EL, "status": "pending", "group_key": _EP}
    _er = {"id": "r1", "label": "other-bundle-s1-a", "status": "running", "group_key": "other"}
    check("an esc-review wait reason never says 'held on bundle' (like gate-)",
          _wait_reason_for(_ew, "other", {}, [_er]).startswith("held on bundle"), False)
    check("...while a plain row of another bundle still does",
          _wait_reason_for({"id": "z", "label": "x-s1-a", "status": "pending",
                            "group_key": "x"}, "other", {}, [_er]
                           ).startswith("held on bundle other"), True)

    # --- one row per slice: sliceSuffix / sliceOpenByDefault (EXECUTED under node) ----
    _sjs = re.search(r"// BEGIN sliceSuffix[^\n]*\n(.*?)// END sliceSuffix",
                     FRONTEND_HTML, re.S)
    check("the executable sliceSuffix block is still delimited in the page",
          bool(_sjs), True)
    check("a slice no longer auto-opens just for being the bundle's current one",
          "sl.sid === view.current" not in FRONTEND_HTML, True)
    if _sjs and _node:
        _sprobe = _sjs.group(1) + """
const H = (k, a, x = {}) => Object.assign({kind: k, attempt: a}, x);
const full = sliceSuffix({active: true, elapsed_s: 75, history: [
  H('authoring', 1), H('refining (round 1)', 1), H('authoring', 2),
  H('refining (round 2)', 2, {live: true, status: 'running', duration_s: 60})]});
const none = sliceSuffix({phase: 'pending', history: []});
const escOnly = sliceSuffix({phase: 'escalated', attention: true, history: [H('escalated', 1)]});
console.log(JSON.stringify({
  full, none, escOnly,
  stages: [['authoring'], ['refining (round 2)'], ['coding'], ['gate'], ['regate'],
           ['escalation-review'], ['second-opinion'], ['', 'secondop-b0c6d90fdf38'], ['preflight'],
           ['', 'auto-author-p-s1-x'], ['', 'auto-author-p-s1-x-c2'], ['', 'auto-refine-p-s1-x-r3 [auto-fix r1]'],
           ['', 'gate-abcdef123456'], ['', 'esc-review-41Z-p-s1-x'], ['', 'p-s1-x']].map(a => stageLabel(a[0], a[1])),
  hdr: [sliceHeader({sid: 's1-request-status-map', title: 'mapServiceRequestStatus'}, 'b'),
        sliceHeader({sid: 's2-appt-number', title: ''}, 'b'),
        sliceHeader({sid: 'b-s3b-x-y'}, 'b'), sliceHeader({sid: 'weird'}, null),
        sliceHeader({sid: 'abc123', job: true, title: 'rt-amazon-detail-orderid'}, 'b')],
  att: (() => { const hh = [H('authoring', 1, {status: 'done', duration_s: 60}), H('refining (round 1)', 1, {status: 'done', duration_s: 120, result: 'gate PASS'}),
        H('authoring', 2, {status: 'done', duration_s: 30}), H('preflight', 2, {status: 'done', result: 'NO-GO relevance'})];
        const ga = groupAttempts(hh);
        return {nums: ga.nums, n1: ga.by[1].length, sum1: attemptSummary(1, ga.by[1]), sum2: attemptSummary(2, ga.by[2])}; })(),
  fold: [foldRounds([H('authoring', 1), H('refining (round 1)', 1), H('refining (round 2)', 1), H('refining (round 3)', 1)]).map(i => i.fold ? i.label : i.h.kind),
         foldRounds([H('authoring', 1), H('refining (round 1)', 1), H('refining (round 2)', 1)]).map(i => i.fold ? 'F' : i.h.kind)],
  trim: [trimBundle('b-s1-x', 'b'), trimBundle('s1-x', 'b'), trimBundle('b-', 'b'), trimBundle('s1', null)],
  openEsc: sliceOpenByDefault({attention: true}),
  openRun: sliceOpenByDefault({phase: 'coding', active: true, attention: false}),
}));
"""
        with tempfile.NamedTemporaryFile("w", suffix=".mjs", delete=False) as _sf:
            _sf.write(_sprobe)
            _spath = _sf.name
        try:
            _sr = subprocess.run([_node, _spath], capture_output=True, text=True,
                                 timeout=30)
        finally:
            os.unlink(_spath)
        check("the extracted sliceSuffix block actually runs (node exit 0)",
              (_sr.returncode, _sr.stderr.strip()[:200]), (0, ""))
        _s = json.loads(_sr.stdout) if _sr.returncode == 0 else {}
        check("a retried, refining, live slice collapses to ONE suffix",
              _s.get("full"), "attempt 2 · refine r2 · 1:00")
        check("child rows show a short stage label, not the full label",
              _s.get("stages"),
              ["author", "refine r2", "code", "gate", "regate", "escalation / heal",
               "2nd opinion", "2nd opinion", "preflight", "author", "author c2", "refine r3", "gate",
               "escalation / heal", "code"])
        check("slice header = number + entry point, falling back to the slug sans sNN-",
              _s.get("hdr"),
              [{"num": "Slice 1", "entry": "mapServiceRequestStatus",
                "tip": "s1-request-status-map mapServiceRequestStatus"},
               {"num": "Slice 2", "entry": "appt-number", "tip": "s2-appt-number"},
               {"num": "Slice 3b", "entry": "x-y", "tip": "s3b-x-y"},
               {"num": "weird", "entry": "", "tip": "weird"},
               {"num": "Job", "entry": "rt-amazon-detail-orderid",
                "tip": "rt-amazon-detail-orderid abc123"}])
        check("attempts group and each earlier attempt collapses to ONE summary line",
              (_s.get("att", {}).get("nums"), _s.get("att", {}).get("n1"),
               _s.get("att", {}).get("sum1"), _s.get("att", {}).get("sum2")),
              ([1, 2], 2, "Attempt 1 \u00b7 refine r1 \u00b7 done \u00b7 gate PASS \u00b7 3:00",
               "Attempt 2 \u00b7 preflight \u00b7 done \u00b7 NO-GO relevance \u00b7 0:30"))
        check("earlier refine rounds fold into one line only when there are two or more",
              _s.get("fold"),
              [["authoring", "refine r1-r2 \u00b7 2 rounds", "refining (round 3)"],
               ["authoring", "refining (round 1)", "refining (round 2)"]])
        check("the bundle prefix is trimmed only when it is really a prefix",
              _s.get("trim"), ["s1-x", "s1-x", "b-", "s1"])
        check("an idle slice and a lone escalated slice carry no suffix",
              (_s.get("none"), _s.get("escOnly")), ("", ""))
        check("only a failed/escalated slice opens by default, a running one stays one row",
              (_s.get("openEsc"), _s.get("openRun")), (True, False))

    # --- run-status panel: a signal-carrying card must not hide (the user 2026-09-19) ----
    # A nested sub-plan is where the real PASS rows live once a slice was re-split, so
    # the batch auto-opening is pointless if the child does not. Three properties, all
    # checked as SOURCE invariants (there is no JS runtime here; the behaviour itself
    # was verified in-browser via Playwright against the live daemon):
    check("only a fully auto-handled bundle starts folded (good_to_go opens too)",
          "const dflt = !forceCollapsedDefault && g.section !== 'handled';"
          in FRONTEND_HTML, True)
    # the user's design call, 2026-09-19: ONE click on the bundle reveals every run under
    # it, sub-plans included. A sub-plan is a label, never a second collapse toggle.
    check("a sub-plan's runs are rendered inline, not behind their own card",
          ("function renderSubPlanRuns(g, indent)" in FRONTEND_HTML
           and "renderSubPlanRuns(g, (indent || 0) + 1);" in FRONTEND_HTML), True)
    check("...recursively, so a split of a split is still one click away",
          "renderSubPlanRuns(c, indent + 1);" in FRONTEND_HTML, True)
    check("a sub-plan NEVER renders as another collapsible parent card",
          "renderParentGroup(c," in FRONTEND_HTML, False)
    check("the sub-plan divider has no click handler and no pointer cursor",
          ("onclick" not in FRONTEND_HTML.split("function renderSubPlanDivider")[1]
                                         .split("function renderSubPlanRuns")[0]
           and "tr.run-subplan { cursor: default; }" in FRONTEND_HTML), True)
    check("the divider still names the slice and its own X/Y (grouping, not hiding)",
          ("&#8627; ${g.slice_id}" in FRONTEND_HTML
           and "${s.slicesDone}/${s.total} slices done" in FRONTEND_HTML), True)
    check("the 'Good to go' section renders its entries UNFOLDED",
          "if (goodExpanded) for (const e of goodToGo) renderEntry(e, false);"
          in FRONTEND_HTML, True)
    check("no render path still forces a bundle collapsed by its section",
          "renderParentGroup(e, forceCollapsedDefault" in FRONTEND_HTML, False)
    # Expand/collapse is a VIEW change: instant re-render off the cached tree (the
    # fetch measured 0.5-0.9s in-browser), and the state survives a page reload.
    check("a toggle re-renders from the cached tree instead of re-fetching",
          ("function rerenderRuns()" in FRONTEND_HTML
           and "opts.toggle();\n    saveExpandState();\n    rerenderRuns();" in FRONTEND_HTML),
          True)
    check("every expand/collapse dict is persisted, none forgotten",
        all(k in FRONTEND_HTML.split("function saveExpandState()")[1].split("}")[0]
            for k in ("parentExpanded", "projectExpanded", "planExpanded",
                      "goodExpanded", "handledExpanded")), True)
    check("...and reading it back is wrapped so blocked storage cannot break the panel",
          FRONTEND_HTML.count("catch (e) { /*") >= 2, True)

    # --- "Loaded right now": REAL system memory from vm_stat (2026-09-18, the user:
    # show real system memory, not just the model-footprint sum). 16KB pages, a
    # 64GiB box; used = total - reclaimable(free+inactive+speculative+purgeable).
    PS = 16384
    TOTAL = 68719476736  # 64 GiB, exactly what hw.memsize reports on Studio
    sample = (
        "Mach Virtual Memory Statistics: (page size of 16384 bytes)\n"
        "Pages free:                                   100000.\n"
        "Pages active:                                 646955.\n"
        "Pages inactive:                                50000.\n"
        "Pages speculative:                             10000.\n"
        "Pages wired down:                            1553249.\n"
        "Pages purgeable:                                5000.\n"
        "Pages occupied by compressor:                 324176.\n")
    used, total = _parse_vm_stat(sample, PS, TOTAL)
    reclaimable_pages = 100000 + 50000 + 10000 + 5000
    want_used = round((TOTAL - reclaimable_pages * PS) / 1e9, 1)
    check("vm_stat total is the real hw.memsize (decimal GB)", total, round(TOTAL / 1e9, 1))
    check("vm_stat used = total - reclaimable (real pressure, not model sum)", used, want_used)
    check("vm_stat used is well under total here (not a model-footprint sum)", used < total, True)
    # Revert-test anchor: if the parser stopped subtracting reclaimable pages and
    # just returned total (or 0), this inequality would flip / the number move.
    check("vm_stat used strictly less than total when memory is free", used < total, True)
    # A fully-committed box: no reclaimable pages -> used == total, never over.
    full = ("Mach Virtual Memory Statistics: (page size of 16384 bytes)\n"
            "Pages free:                                        0.\n"
            "Pages wired down:                            4194304.\n")
    fused, ftotal = _parse_vm_stat(full, PS, TOTAL)
    check("vm_stat used never exceeds total", fused <= ftotal, True)
    # Unparseable / empty -> (None, None) so the caller falls back to the model sum.
    check("garbage vm_stat yields (None, None) for model-sum fallback",
          _parse_vm_stat("not vm_stat output", PS, TOTAL), (None, None))
    check("empty vm_stat yields (None, None)", _parse_vm_stat("", PS, TOTAL), (None, None))
    check("zero total yields (None, None) (no divide-by-zero downstream)",
          _parse_vm_stat(sample, PS, 0), (None, None))

    # --- group-promote: row annotation + route wiring (no server stood up) ----
    _rev_g = {"aw-transfer-partners-s1-a": "aw-transfer-partners",
              "aw-transfer-partners-s2-b": "aw-transfer-partners",
              "aw-transfer-partners-s3-c": "aw-transfer-partners"}
    _saved_idx = q._GROUP_INDEX_CACHE.copy()
    try:
        q._GROUP_INDEX_CACHE.update({"at": time.monotonic(), "reverse": _rev_g})
        _grows = _annotate_job_groups([
            {"id": "g1", "label": "auto-author-aw-transfer-partners-s1-a", "status": "running"},
            {"id": "g2", "label": "auto-author-aw-transfer-partners-s2-b", "status": "pending"},
            {"id": "g3", "label": "auto-refine-aw-transfer-partners-s3-c-r1", "status": "pending"},
            {"id": "u1", "label": "auto-author-esim-global-s1-parse", "status": "pending"},
        ])
        _by = {r["id"]: r for r in _grows}
        check("every slice of one sliced job shares a group key",
              {_by[i]["group_key"] for i in ("g1", "g2", "g3")}, {"aw-transfer-partners"})
        check("group_pending counts only PENDING members (the running one excluded)",
              _by["g2"]["group_pending"], 2)
        check("an unrelated job is its own group of one",
              (_by["u1"]["group_key"], _by["u1"]["group_pending"]), ("esim-global", 1))
        check("the group button's render condition is false for a lone job",
              _by["u1"]["group_pending"] > 1, False)
        check("the group button's render condition is true for a multi-slice job",
              _by["g3"]["group_pending"] > 1, True)
    finally:
        q._GROUP_INDEX_CACHE.clear()
        q._GROUP_INDEX_CACHE.update(_saved_idx)
    # Route wiring: /promote-group must NOT be swallowed by the per-slice /promote
    # regex (which would silently promote one slice instead of the job).
    check("POST /api/jobs/<id>/promote-group does not match the per-slice promote route",
          bool(re.match(r"^/api/jobs/([^/]+)/promote$", "/api/jobs/abc123/promote-group")), False)
    check("POST /api/jobs/<id>/promote-group matches the group route",
          re.match(r"^/api/jobs/([^/]+)/promote-group$",
                   "/api/jobs/abc123/promote-group").group(1), "abc123")
    check("the per-slice promote route still matches (not regressed)",
          re.match(r"^/api/jobs/([^/]+)/promote$", "/api/jobs/abc123/promote").group(1), "abc123")
    check("the handler exposes _promote_group", hasattr(Handler, "_promote_group"), True)
    check("it delegates to ollama-queue.py's promote_group_for_job",
          callable(getattr(q, "promote_group_for_job", None)), True)

    # --- QUEUE panel plan rollup: _group_waiting_by_plan / _annotate_plan_rollup ---
    # One collapsible parent per slice-PLAN. Grouping only -- it must never reorder
    # within a plan, never move a standalone row, and never drop anything.
    _pr = [
        {"id": "a2", "group_key": "alpha", "status": "planned", "display_seq": 5},
        {"id": "solo", "group_key": None, "status": "pending", "display_seq": 1},
        {"id": "b1", "group_key": "beta", "status": "planned", "display_seq": 3},
        {"id": "a1", "group_key": "alpha", "status": "running", "display_seq": 0},
        {"id": "b2", "group_key": "beta", "status": "planned", "display_seq": 4},
        {"id": "hold1", "group_key": None, "status": "held", "display_seq": 99},
        {"id": "lone", "group_key": "gamma", "status": "pending", "display_seq": 2},
    ]
    _groups = _group_waiting_by_plan(_pr)
    check("no row is ever dropped by the rollup",
          sorted(r["id"] for g in _groups for r in g["children"]),
          sorted(r["id"] for r in _pr))
    check("parents sort by the MIN display_seq of their children",
          [g["key"] or g["children"][0]["id"] for g in _groups],
          ["alpha", "solo", "gamma", "beta", "hold1"])
    check("children render in display_seq order inside a plan",
          [r["id"] for g in _groups if g["key"] == "alpha" for r in g["children"]],
          ["a1", "a2"])
    check("a running slice stays under its plan parent",
          [g["key"] for g in _groups if any(r["id"] == "a1" for r in g["children"])],
          ["alpha"])
    check("a falsy group_key is STANDALONE (never wrapped in a parent)",
          [g["standalone"] for g in _groups if g["key"] is None], [True, True])
    check("two rows with no group_key are NOT merged into one group",
          len([g for g in _groups if g["key"] is None]), 2)
    check("a plan of one renders flat too (standalone)",
          [g["standalone"] for g in _groups if g["key"] == "gamma"], [True])
    check("a real multi-slice plan is NOT standalone",
          [g["standalone"] for g in _groups if g["key"] == "beta"], [False])
    check("a plan with a running/pending child is ACTIVE (default-expanded)",
          [g["active"] for g in _groups if g["key"] == "alpha"], [True])
    check("an all-planned plan is not active (default-collapsed)",
          [g["active"] for g in _groups if g["key"] == "beta"], [False])
    check("the parent's status breakdown counts every child",
          [g["counts"] for g in _groups if g["key"] == "alpha"],
          [{"running": 1, "planned": 1}])
    # The annotation is the ONLY thing the front-end groups on: plan_seq puts a
    # plan's rows together as one block, plan_size>1 is what makes a parent appear.
    _ann_pr = _annotate_plan_rollup([dict(r) for r in _pr])
    _byp = {r["id"]: r for r in _ann_pr}
    check("plan_seq is the plan's min display_seq for every member",
          (_byp["a1"]["plan_seq"], _byp["a2"]["plan_seq"]), (0, 0))
    check("plan_size marks a real plan (parent) vs a lone row (flat)",
          (_byp["a1"]["plan_size"], _byp["lone"]["plan_size"], _byp["solo"]["plan_size"]),
          (2, 1, 1))
    check("the front-end sort key (plan_seq, display_seq) keeps each plan contiguous",
          [r["id"] for r in sorted(_ann_pr, key=lambda r: (r["plan_seq"], r["display_seq"]))],
          ["a1", "a2", "solo", "lone", "b1", "b2", "hold1"])
    check("the held row has no group_key -> standalone, and stays last",
          (_byp["hold1"]["plan_size"], _byp["hold1"]["plan_seq"]), (1, 99))
    # A finished slice must not inflate a plan's counts or drag its parent upward.
    _ann_done = _annotate_plan_rollup([
        {"id": "d1", "group_key": "delta", "status": "done", "display_seq": 0},
        {"id": "d2", "group_key": "delta", "status": "pending", "display_seq": 7},
        {"id": "d3", "group_key": "delta", "status": "planned", "display_seq": 8},
    ])
    check("a done slice is excluded from the plan rollup",
          [(r["id"], r["plan_size"], r["plan_seq"]) for r in _ann_done],
          [("d1", 1, 0), ("d2", 2, 7), ("d3", 2, 7)])
    check("a missing display_seq falls back to the status tier, never crashes",
          _group_waiting_by_plan([{"id": "x", "group_key": None, "status": "pending"}])[0]["seq"],
          10_000 + _queue_status_tier("pending"))
    # --- parent status = the MOST ACTIVE child (the user: "whatever job is actively
    # being worked should show running") ----------------------------------------
    check("running outranks every other status on the parent row",
          min(["planned", "pending", "running", "paused"], key=_plan_status_rank), "running")
    check("pending/queued/scheduled outrank paused/held and planned",
          [min(["planned", "held", s], key=_plan_status_rank)
           for s in ("pending", "queued", "scheduled")], ["pending", "queued", "scheduled"])
    check("paused/held outrank planned",
          [min(["planned", s], key=_plan_status_rank) for s in ("paused", "held")],
          ["paused", "held"])
    check("an unknown status never outranks running",
          min(["running", "wat"], key=_plan_status_rank), "running")
    _lead = _group_waiting_by_plan([
        {"id": "l1", "group_key": "aw-sched-routes", "status": "planned", "display_seq": 1,
         "label": "aw-sched-routes-s3-later"},
        {"id": "l2", "group_key": "aw-sched-routes", "status": "running", "display_seq": 2,
         "label": "auto-author-aw-sched-routes-s2-from-transfer-partners-r1"},
        {"id": "l3", "group_key": "allplanned", "status": "planned", "display_seq": 3,
         "label": "allplanned-s1-a"},
        {"id": "l4", "group_key": "allplanned", "status": "planned", "display_seq": 4,
         "label": "allplanned-s2-b"},
        {"id": "l5", "group_key": "pend", "status": "planned", "display_seq": 5,
         "label": "pend-s2-b"},
        {"id": "l6", "group_key": "pend", "status": "pending", "display_seq": 6,
         "label": "pend-s1-a"},
    ])
    _bl = {g["key"]: g for g in _lead}
    check("a plan with ANY running child shows running, not a generic 'active'",
          _bl["aw-sched-routes"]["lead_status"], "running")
    check("the running slice is NAMED (stage prefix, -rN and plan prefix stripped)",
          _bl["aw-sched-routes"]["lead_slice"], "s2-from-transfer-partners")
    check("a plan whose best child is pending shows pending",
          _bl["pend"]["lead_status"], "pending")
    check("an all-planned plan shows planned",
          _bl["allplanned"]["lead_status"], "planned")
    check("the lead is the most-active child even when it is not first by seq",
          _bl["pend"]["lead_slice"], "s1-a")
    check("_slice_short_name strips stage prefix, refine round and plan prefix",
          _slice_short_name("auto-refine-bg-eraser-s1-invoke-r2", "bg-eraser"),
          "s1-invoke")
    check("_slice_short_name falls back to the base when the plan prefix is absent",
          _slice_short_name("auto-author-unrelated-s1-x", "bg-eraser"), "unrelated-s1-x")
    _lead_rows = _annotate_plan_rollup([dict(r) for r in _lead[0]["children"]])
    check("plan_status/plan_lead are stamped on every child (JS re-derives nothing)",
          [(r["plan_status"], r["plan_lead"]) for r in _lead_rows],
          [("running", "s2-from-transfer-partners")] * 2)

    # --- plan progress fraction X/Y (_plan_progress / _load_plan_progress) -------
    _pstate = {"label": "bg-state-s1-init-db",
               "order": ["s1", "s2", "s3", "s4", "s5"],
               "slices": {"s1": {"status": "done"}, "s2": {"status": "enqueued"},
                          "s3": {"status": "failed"}, "s4": {"status": "blocked"},
                          "s5": {"status": "pending"}}}
    check("X counts ONLY slices that have actually completed (done); enqueued does NOT count",
          _plan_progress(_pstate), (1, 5))
    check("an enqueued-but-not-run slice is NOT counted as through",
          _plan_progress({"order": ["a", "b"],
                          "slices": {"a": {"status": "enqueued"}, "b": {"status": "pending"}}}),
          (0, 2))
    check("failed/blocked/escalated/pending slices are NOT counted as through",
          _plan_progress({"slices": {"a": {"status": "failed"}, "b": {"status": "blocked"},
                                     "c": {"status": "escalated"}, "d": {"status": "pending"}}}),
          (0, 4))
    check("a SKIPPED slice counts as through -- it is retired, not still owed",
          _plan_progress({"order": ["a", "b", "c"],
                          "slices": {"a": {"status": "done"}, "b": {"status": "skipped"},
                                     "c": {"status": "pending"}}}),
          (2, 3))
    check("an all-done-or-skipped plan is COMPLETE, so bundle_incomplete can clear",
          _plan_progress({"order": ["a", "b"],
                          "slices": {"a": {"status": "done"}, "b": {"status": "skipped"}}}),
          (2, 2))
    check("Y is the WHOLE plan, not just the live rows",
          _plan_progress(_pstate)[1], len(_pstate["order"]))
    check("a slice in `order` with no entry yet still counts toward Y",
          _plan_progress({"order": ["s1", "s2", "s3"], "slices": {"s1": {"status": "done"}}}),
          (1, 3))
    check("a slices LIST (not map) is read too", _plan_progress(
        {"slices": [{"status": "done"}, {"status": "pending"}]}), (1, 2))
    check("garbage state yields (0,0), never an exception",
          (_plan_progress(None), _plan_progress({}), _plan_progress({"slices": 7})),
          ((0, 0), (0, 0), (0, 0)))
    with tempfile.TemporaryDirectory() as _rd:
        _rdp = Path(_rd)
        (_rdp / "bg-state-s1-init-db.json").write_text(json.dumps(_pstate))
        (_rdp / "corrupt-plan.json").write_text("{not json")
        check("group_key -> <group_key>.json in the slice-runs dir",
              _load_plan_progress("bg-state-s1-init-db", _rdp), (1, 5))
        check("a MISSING run-state file returns None (caller falls back)",
              _load_plan_progress("no-such-plan", _rdp), None)
        check("a CORRUPT run-state file returns None, never raises",
              _load_plan_progress("corrupt-plan", _rdp), None)
        check("a path-traversing group key is refused outright",
              _load_plan_progress("../../etc/passwd", _rdp), None)
        # End-to-end: rows of a plan WITH a state file carry the true X/Y; rows of a
        # plan with NO state file fall back to their live-row fraction (never crash).
        _prog_rows = _annotate_plan_rollup([
            {"id": "p1", "group_key": "bg-state-s1-init-db", "status": "running", "display_seq": 0},
            {"id": "p2", "group_key": "bg-state-s1-init-db", "status": "planned", "display_seq": 1},
            {"id": "n1", "group_key": "no-such-plan", "status": "done", "display_seq": 2},
            {"id": "n2", "group_key": "no-such-plan", "status": "pending", "display_seq": 3},
            {"id": "n3", "group_key": "no-such-plan", "status": "planned", "display_seq": 4},
        ], runs_dir=_rdp)
        _bp = {r["id"]: r for r in _prog_rows}
        check("plan rows carry the run-state fraction (whole plan, not live rows)",
              (_bp["p1"]["plan_done"], _bp["p1"]["plan_total"]), (1, 5))
        check("every child of a plan shows the SAME fraction",
              (_bp["p2"]["plan_done"], _bp["p2"]["plan_total"]), (1, 5))
        check("no run-state file -> live-row fallback fraction, not a crash",
              (_bp["n2"]["plan_done"], _bp["n2"]["plan_total"]), (1, 3))
        check("a standalone row reports no fraction (parent line renders none)",
              _annotate_plan_rollup([{"id": "s", "group_key": None, "status": "held",
                                      "display_seq": 9}], runs_dir=_rdp)[0]["plan_total"], 0)

        # --- ONE authoritative slice count, all the way to the tree -------------
        # the user 2026-09-18: "i want to see total slices not just the initial slices"
        # and "maybe we need to put the data in one place and just pull from there
        # instead of having it split so many places". bg-eraser read "1/2" in the
        # run-status tree (its plan-index top-level count) while the queue panel read
        # "1/8" (the recursive count) for the same bundle at the same moment.
        # Fixture: np -> s1-alpha (split) -> s2-y (split AGAIN). THREE levels, so a
        # recursion hardcoded to one level cannot pass this.
        (_rdp / "np.json").write_text(json.dumps(
            {"label": "np", "order": ["s1-alpha", "s2-beta"],
             "slices": {"s1-alpha": {"status": "escalated"},
                        "s2-beta": {"status": "pending"}}}))
        (_rdp / "np-s1-alpha.json").write_text(json.dumps(
            {"label": "np-s1-alpha", "order": ["s1-x", "s2-y"],
             "slices": {"s1-x": {"status": "done"}, "s2-y": {"status": "escalated"}}}))
        (_rdp / "np-s1-alpha-s2-y.json").write_text(json.dumps(
            {"label": "np-s1-alpha-s2-y", "order": ["s1-p", "s2-q", "s3-r"],
             "slices": {"s1-p": {"status": "done"}, "s2-q": {"status": "pending"},
                        "s3-r": {"status": "pending"}}}))
        check("the FLAT count still sees only the two original top-level slices",
              _plan_progress(_load_plan_state("np", _rdp)), (0, 2))
        check("recursion expands sub-plans of sub-plans (3 levels), not just one",
              _plan_progress_recursive("np", _rdp), (2, 5))
        check("a sub-plan asked directly reports its own recursive total",
              _plan_progress_recursive("np-s1-alpha", _rdp), (2, 4))
        check("_load_plan_progress hands back that same recursive pair",
              _load_plan_progress("np", _rdp), (2, 5))

        _nrev = {"np-s1-alpha-s1-x": "np-s1-alpha", "np-s2-beta": "np"}
        _nproj = {"np": {"order": ["s1-alpha", "s2-beta"], "total": 2},
                  "np-s1-alpha": {"order": ["s1-x", "s2-y"], "total": 2}}
        _nrows = [{"id": "n1", "label": "np-s1-alpha-s1-x", "group": "handled",
                   "raw_verdict": "PASS"},
                  {"id": "n2", "label": "np-s2-beta", "group": "handled",
                   "raw_verdict": "PASS"}]
        _annotate_run_parents(_nrows, _nrev, _nproj,
                              progress=lambda p: _load_plan_progress(p, _rdp))
        _nby = {r["id"]: r for r in _nrows}
        check("the row carries the AUTHORITATIVE recursive pair",
              (_nby["n2"]["parent_done"], _nby["n2"]["parent_slices"]), (2, 5))
        check("...while parent_total keeps the stale flat count (fallback only)",
              _nby["n2"]["parent_total"], 2)
        _annotate_run_projects(_nrows)
        _ntree = _build_run_tree(_nrows)
        _nbundles = {}

        def _collect(entries):
            for _e in entries:
                if _e["kind"] != "parent":
                    continue
                _nbundles[_e["project"]] = _e
                _collect(_e["children"])
        _collect(_ntree[0]["entries"])
        check("THE BUG: the tree's bundle total is the recursive one, not parent_total",
              (_nbundles["np"]["total"], _nbundles["np"]["done"]), (5, 2))
        check("...and the rendered summary quotes exactly that X/Y",
              (_nbundles["np"]["summary"]["slicesDone"],
               _nbundles["np"]["summary"]["total"]), (2, 5))
        check("the tree total now MATCHES _plan_progress_recursive (one source)",
              (_nbundles["np"]["summary"]["slicesDone"],
               _nbundles["np"]["summary"]["total"]),
              _plan_progress_recursive("np", _rdp))
        check("the summary says its fraction is authoritative, not row-derived",
              _nbundles["np"]["summary"]["slicesAuthoritative"], True)
        check("a sub-plan bundle reports ITS own recursive total too",
              (_nbundles["np-s1-alpha"]["summary"]["slicesDone"],
               _nbundles["np-s1-alpha"]["summary"]["total"]), (2, 4))
        # A sub-plan's slices are ALREADY inside its parent's recursive total, so the
        # project rollup must not add both (that is what inflated the bg header).
        check("the project rollup does not double-count a sub-plan sibling",
              (_ntree[0]["summary"]["slicesDone"], _ntree[0]["summary"]["total"]), (2, 5))
        check("...and it names the bundle it folded in",
              _ntree[0]["covered"], ["np-s1-alpha"])

        # --- the sub-plan's runs live INSIDE the batch card, not beside it ---------
        # the user 2026-09-18: "ok but we still don't have complete runs inside the batch".
        # bg-eraser showed 3/8 done over six stale pre-split attempts while the three
        # converged PASS runs sat in the separate top-level cards bg-eraser-s1-invoke
        # and bg-eraser-s2-verify. Same fixture plus a THIRD level, so a nesting that
        # only handles one extra level cannot pass.
        check("_subplan_parent_key picks the LONGEST ancestor (nests one level a time)",
              _subplan_parent_key("np-s1-alpha-s2-y", ["np", "np-s1-alpha"]),
              "np-s1-alpha")
        check("a bundle that is nobody's sub-plan has no ancestor",
              _subplan_parent_key("np", ["np-s1-alpha"]), None)
        check("a shared prefix that is not a slice id is NOT an ancestor",
              _subplan_parent_key("bg-eraser-helper", ["bg-eraser"]), None)
        _drows = [{"id": "d1", "label": "np-s1-alpha-s1-x", "group": "handled",
                   "raw_verdict": "PASS"},
                  {"id": "d2", "label": "np-s2-beta", "group": "handled",
                   "raw_verdict": "PASS"},
                  {"id": "d3", "label": "np-s1-alpha-s2-y-s1-p", "group": "good_to_go",
                   "raw_verdict": "PASS"}]
        _drev = dict(_nrev, **{"np-s1-alpha-s2-y-s1-p": "np-s1-alpha-s2-y"})
        _dproj = dict(_nproj, **{"np-s1-alpha-s2-y":
                                 {"order": ["s1-p", "s2-q", "s3-r"], "total": 3}})
        _annotate_run_parents(_drows, _drev, _dproj,
                              progress=lambda p: _load_plan_progress(p, _rdp))
        _annotate_run_projects(_drows)
        _dtree = _build_run_tree(_drows)
        _dproj_e = _dtree[0]
        check("a sub-plan is NO LONGER a top-level bundle beside its parent",
              [e["project"] for e in _dproj_e["entries"] if e["kind"] == "parent"],
              ["np"])
        _dnp = _dproj_e["entries"][0]
        # Indexed defensively so a regression reports FAIL rather than an IndexError.
        _dkid = (_dnp["children"] or [{}])[0]
        check("the sub-plan bundle is a CHILD of the batch it was split out of",
              [c["project"] for c in _dnp["children"]], ["np-s1-alpha"])
        check("...and nesting keeps going, level 3 under level 2 (arbitrary depth)",
              [c["project"] for c in _dkid.get("children") or []],
              ["np-s1-alpha-s2-y"])
        check("a nested bundle records WHICH slice of its parent it replaced",
              (_dkid.get("slice_of"), _dkid.get("slice_id")), ("np", "s1-alpha"))
        check("the batch card's DIRECT rows are still only its own runs",
              [r["id"] for r in _dnp["rows"]], ["d2"])
        check("THE FIX: the batch card now CONTAINS every run under it, all depths",
              sorted(r["id"] for r in _dnp["allRows"]), ["d1", "d2", "d3"])
        check("a run 3 levels down still reaches the batch summary",
              _dnp["summary"]["runs"], 3)
        check("the batch's X/Y is unchanged by nesting (still the recursive pair)",
              (_dnp["summary"]["slicesDone"], _dnp["summary"]["total"]), (2, 5))
        check("the project counts the batch ONCE, not once per nested sub-plan",
              _dproj_e["summary"]["total"], 5)
        check("...and lists every bundle it folded in, at every depth",
              _dproj_e["covered"], ["np-s1-alpha", "np-s1-alpha-s2-y"])
        check("the project still sees every descendant run exactly once",
              sorted(r["id"] for r in _dproj_e["rows"]), ["d1", "d2", "d3"])


        # good vs slicesDone: DIFFERENT measurements, and the fix is that neither is
        # derived from the other. Every run here is auto-handled, so good == 0 while
        # 2 slices are genuinely through -- the user's "1/2 with nothing passing". The UI
        # says "2/5 slices done" + "auto-handled runs", which do not contradict.
        # (runs == 2, not 1: the batch card now contains its nested sub-plan's run too
        # -- see the nesting block below.)
        check("slicesDone counts PLAN SLICES, good counts live run rows",
              (_nbundles["np"]["summary"]["slicesDone"],
               _nbundles["np"]["summary"]["good"],
               _nbundles["np"]["summary"]["runs"]), (2, 0, 2))
        _gsum = _summarize_rows(
            [{"slice_id": "s1", "parent": "np", "group": "good_to_go",
              "raw_verdict": "PASS", "bundle_incomplete": False}], total=5, done=2)
        check("an authoritative done is NEVER rounded up to total by the old heuristic",
              (_gsum["slicesDone"], _gsum["total"]), (2, 5))
        check("...and X<Y alone marks the bundle incomplete", _gsum["incomplete"], True)
        check("a good_to_go run still counts toward good independently of slicesDone",
              _gsum["good"], 1)
        # X > Y is never shown: the row-derived fallback is clamped to the total
        # (the cc project header read "6/3 slices" -- 6 distinct slice ids across
        # runs, over a total of 3 because one bundle has no readable plan state).
        _over = _summarize_rows(
            [{"slice_id": f"s{i}", "parent": "cc", "group": "handled",
              "raw_verdict": "PASS", "bundle_incomplete": True} for i in range(6)],
            total=3)
        check("a row-derived slicesDone can never exceed the total (no '6/3 slices')",
              (_over["slicesDone"], _over["total"]), (3, 3))

        # --- a finished job still sitting in the queue must not supersede ITSELF -------
        # the user 2026-09-19: "nothing is showing" -- every run-status row read auto-handled
        # with "<feature> is re-running in the queue -- wait for the live verdict" while
        # nothing was re-running. _run_status_jobs handed _annotate_run_groups EVERY job
        # in the queue file, so each of the 11 `done` jobs made its own PASS row wait for
        # itself and the whole sign-off bar emptied out.
        _qjobs = [{"label": "alpha-s1", "status": "done"},
                  {"label": "beta-s1", "status": "done_unconverged"},
                  {"label": "gamma-s1", "status": "planned"},
                  {"label": "delta-s1", "status": "running"},
                  {"label": "eps-s1", "status": "pending"}]
        check("terminal queue jobs are NOT 'live' (a done job is nothing to wait for)",
              _live_queue_labels(_qjobs), ["gamma-s1", "delta-s1", "eps-s1"])
        check("failed/cancelled/escalated/blocked jobs are NOT live either",
              _live_queue_labels([{"label": "f-s1", "status": "failed"},
                                  {"label": "c-s1", "status": "cancelled"},
                                  {"label": "n-s1", "status": "needs_opus"},
                                  {"label": "b-s1", "status": "blocked"},
                                  {"label": "p-s1", "status": "paused"},
                                  {"label": "h-s1", "status": "held"}]),
              ["p-s1", "h-s1"])
        check("_live_queue_labels tolerates an empty/None queue",
              (_live_queue_labels([]), _live_queue_labels(None)), ([], []))
        _pass_row = [{"id": "z1", "label": "alpha-s1", "raw_verdict": "PASS",
                      "verdict": "PASS", "awaiting_signoff": False}]
        _z = _annotate_run_groups([dict(_pass_row[0])], _live_queue_labels(_qjobs))[0]
        check("THE EMPTY PANEL: a PASS row is no longer buried by its own done job",
              (_z["group"], _z.get("handled_reason")), ("good_to_go", ""))
        _z2 = _annotate_run_groups([dict(_pass_row[0])],
                                   [j["label"] for j in _qjobs])[0]
        check("...which is exactly what the unfiltered list did (the bug)",
              _z2["group"], "handled")
        _z3 = _annotate_run_groups([{"id": "z3", "label": "delta-s1", "raw_verdict": "PASS",
                                     "verdict": "PASS", "awaiting_signoff": False}],
                                   _live_queue_labels(_qjobs))[0]
        check("a genuinely RUNNING re-run still parks its stale row as handled",
              _z3["group"], "handled")
        # ...and the bundle rollup surfaces that PASS instead of reading auto-handled.
        _srows = [{"id": "z1", "label": "bg-x-s1-a", "group": "good_to_go",
                   "raw_verdict": "PASS", "bundle_incomplete": False},
                  {"id": "z2", "label": "bg-x-s2-b", "group": "handled",
                   "raw_verdict": "SKIPPED", "bundle_incomplete": False}]
        _annotate_run_parents(_srows, {"bg-x-s1-a": "bg-x", "bg-x-s2-b": "bg-x"},
                              {"bg-x": {"order": ["s1-a", "s2-b"], "total": 2}},
                              progress={"bg-x": (2, 2)})
        _annotate_run_projects(_srows)
        _stree = _build_run_tree(_srows)
        check("one good_to_go row lifts its bundle out of 'auto-handled'",
              _stree[0]["entries"][0]["section"], "good_to_go")
        check("...and the project header follows it",
              _stree[0]["section"], "good_to_go")

    # --- FIX 1: done slices are synthesized back into the bundle ----------------
    # the user: "1/4 but only 3 rows in the bundle" -- a done slice's queue row is pruned
    # while Y still counts it, so the expanded plan must render it from run state.
    _dstate = {"label": "aw-airport-groups",
               "order": ["s1-is-group", "s2-alias", "s3-map", "s4-emit"],
               "slices": {"s1-is-group": {"status": "done", "title": "is_group()"},
                          "s2-alias": {"status": "pending"},
                          "s3-map": {"status": "pending"},
                          "s4-emit": {"status": "pending"}}}
    _dlive = [{"id": "a", "label": "auto-author-aw-airport-groups-s2-alias", "status": "pending"},
              {"id": "b", "label": "aw-airport-groups-s3-map", "status": "planned"},
              {"id": "c", "label": "aw-airport-groups-s4-emit", "status": "planned"}]
    _dsyn = _plan_done_children(_dstate, "aw-airport-groups", _dlive)
    check("a done slice with no live row is synthesized back into the bundle",
          [d["label"] for d in _dsyn], ["aw-airport-groups-s1-is-group"])
    check("the bundle's visible child count now equals Y",
          len(_dsyn) + len(_dlive), _plan_progress(_dstate)[1])
    check("synthesized rows are display-only (flagged, no display_seq)",
          (_dsyn[0]["synthetic"], "display_seq" in _dsyn[0]), (True, False))
    check("a synthesized row is marked done and keeps its plan order",
          (_dsyn[0]["status"], _dsyn[0]["slice_order"]), ("done", 0))
    # THE BUG (the user, 2026-09-19): this case used to assert `[]` -- i.e. that ANY row
    # whose _slice_base_label folded onto the slice suppressed its tick. On
    # bg-escalation the only such row was a FAILED auto-refine round, so the slice that
    # had actually PASSED vanished from the bundle and the failure rendered in its
    # place. A meta row is not a dupe: it renders under its own distinct label.
    _drefine = _plan_done_children(
        _dstate, "aw-airport-groups",
        _dlive + [{"id": "z", "status": "failed",
                   "label": "auto-refine-aw-airport-groups-s1-is-group-r2"}])
    check("THE BUG: a FAILED refine round no longer erases the slice that PASSED",
          [d["label"] for d in _drefine], ["aw-airport-groups-s1-is-group"])
    check("...and it is still rendered as done, not repainted by the refine's status",
          [d["status"] for d in _drefine], ["done"])
    check("a still-RUNNING refine round does stand in (in flight, not a contradiction)",
          _plan_done_children(_dstate, "aw-airport-groups",
                              _dlive + [{"id": "z", "status": "running",
                                         "label": "auto-refine-aw-airport-groups-s1-is-group-r2"}]),
          [])
    check("...and so does a still-running AUTHOR row for that slice",
          _plan_done_children(_dstate, "aw-airport-groups",
                              _dlive + [{"id": "z", "status": "running",
                                         "label": "auto-author-aw-airport-groups-s1-is-group"}]),
          [])
    check("a BLOCKED meta row cannot erase a passed slice either",
          [d["label"] for d in _plan_done_children(
              _dstate, "aw-airport-groups",
              _dlive + [{"id": "z", "status": "blocked",
                         "label": "auto-refine-aw-airport-groups-s1-is-group-r2"}])],
          ["aw-airport-groups-s1-is-group"])
    # ...but the guard still does its real job: the slice's OWN primary coding row.
    check("no dupe when the slice's own primary coding row is still live (by label)",
          _plan_done_children(_dstate, "aw-airport-groups",
                              _dlive + [{"id": "z", "status": "running",
                                         "label": "aw-airport-groups-s1-is-group"}]),
          [])
    check("...or when that row is matched by the slicer's recorded job_id",
          _plan_done_children(
              {"order": ["s1"], "slices": {"s1": {"status": "done", "job_id": "abc123"}}},
              "p", [{"id": "abc123", "status": "running", "label": "something-else"}]),
          [])
    _multi = _plan_done_children(
        {"order": ["s1", "s2", "s3"],
         "slices": {"s1": {"status": "done"}, "s2": {"status": "pending"},
                    "s3": {"status": "done"}}}, "p", [])
    check("several done slices come back in plan `order`, not file order",
          [d["label"] for d in _multi], ["p-s1", "p-s3"])
    check("only DONE slices are synthesized (enqueued/failed/pending are not)",
          _plan_done_children({"order": ["s1", "s2", "s3"],
                               "slices": {"s1": {"status": "enqueued"},
                                          "s2": {"status": "failed"},
                                          "s3": {"status": "pending"}}}, "p", []), [])
    check("an unreadable plan state synthesizes nothing, never raises",
          (_plan_done_children(None, "p", []), _plan_done_children({}, "p", [])), ([], []))
    with tempfile.TemporaryDirectory() as _rd2:
        _rd2p = Path(_rd2)
        (_rd2p / "aw-airport-groups.json").write_text(json.dumps(_dstate))
        _drows = _annotate_plan_rollup([dict(r, group_key="aw-airport-groups",
                                             display_seq=i) for i, r in enumerate(_dlive)],
                                       runs_dir=_rd2p)
        check("plan_done_slices rides on the plan's rows for the front-end",
              [d["label"] for d in _drows[0]["plan_done_slices"]],
              ["aw-airport-groups-s1-is-group"])
        check("rendered children (synthesized + live) == Y",
              len(_drows[0]["plan_done_slices"]) + len(_drows), _drows[0]["plan_total"])
        check("the synthesized rows never entered the returned row list",
              [r["id"] for r in _drows], ["a", "b", "c"])

    # --- FIX 1b: the bundle's done rows follow SUB-PLANS, like the fraction does --
    # the user 2026-09-19: "all runs still aren't reflecting like this as they complete in
    # the bundle". THE BUG: X/Y came from _plan_progress_recursive (which expands a
    # slice that was escalated into its own sub-plan) while the tick rows came from a
    # FLAT read of the top-level slices map. A plan whose work all happened one level
    # down therefore showed a numerator with nothing behind it -- live, bg-actions read
    # 5/7 with ZERO tick rows and bg-eraser read 7/8 as a single FLOATING row. Both
    # halves now walk the tree the same way.
    with tempfile.TemporaryDirectory() as _rd3:
        _rd3p = Path(_rd3)
        # s1 was escalated into its own sub-plan; NOTHING at the top level is done.
        (_rd3p / "bg-x.json").write_text(json.dumps(
            {"order": ["s1-item", "s2-merge"],
             "slices": {"s1-item": {"status": "escalated"},
                        "s2-merge": {"status": "pending"}}}))
        (_rd3p / "bg-x-s1-item.json").write_text(json.dumps(
            {"order": ["a", "b", "c"],
             "slices": {"a": {"status": "done", "job_id": "j-a"},
                        "b": {"status": "skipped"},
                        "c": {"status": "pending"}}}))
        _sub = _plan_done_children(None, "bg-x", [], runs_dir=_rd3p)
        check("an unreadable state still synthesizes nothing even with a runs_dir",
              _sub, [])
        _sub = _plan_done_children(_load_plan_state("bg-x", _rd3p), "bg-x", [],
                                   verdict_of=lambda j, ld=None: None, runs_dir=_rd3p)
        check("THE BUG: finished slices of an escalated SUB-plan reach the bundle",
              [d["label"] for d in _sub], ["bg-x-s1-item-a", "bg-x-s1-item-b"])
        check("...and a SKIPPED slice is shown, with its own status, not as 'done'",
              [d["status"] for d in _sub], ["done", "skipped"])
        check("a skipped slice never carries a gate verdict (nothing ran for it)",
              (_sub[1]["gate_tag"], _sub[1]["raw_verdict"], _sub[1]["job_id"]),
              (None, None, None))
        check("the done rows are numbered in the FLATTENED plan order",
              [d["slice_order"] for d in _sub], [0, 1])
        check("what the bundle RENDERS and what Y counts agree after the expansion",
              len(_sub), _plan_progress_recursive("bg-x", _rd3p)[0])
        # THE FLOATING ROW: one live row, and every finished slice a level down. Before
        # the fix dones was empty, so plan_size was 1 and this row rendered flat at the
        # bottom of the queue instead of nesting under its plan.
        _frow = [{"id": "f", "label": "auto-author-bg-x-s1-item-c",
                  "status": "pending", "group_key": "bg-x", "display_seq": 0}]
        _fann = _annotate_plan_rollup([dict(r) for r in _frow], runs_dir=_rd3p)[0]
        check("a plan whose ONLY live row is one slice still renders as a bundle",
              (_fann["plan_bundle"], _fann["plan_size"]), (True, 3))
        check("...with its sub-plan's finished slices under it, not floating alone",
              [d["label"] for d in _fann["plan_done_slices"]],
              ["bg-x-s1-item-a", "bg-x-s1-item-b"])
        # the user 2026-09-19: "auto authors can disappear but the job it queues needs to
        # stay showing". The auto-author row is reaped the instant the coding row it
        # enqueued appears; a size-only bundle test collapsed the bundle to a flat row
        # in that handover window. plan_bundle is a property of the PLAN, so it doesn't.
        (_rd3p / "bg-y.json").write_text(json.dumps(
            {"order": ["s1", "s2", "s3"],
             "slices": {"s1": {"status": "pending"}, "s2": {"status": "pending"},
                        "s3": {"status": "pending"}}}))
        for _st in ("pending", "running", "needs_opus", "blocked", "failed"):
            _h = _annotate_plan_rollup(
                [{"id": "h", "label": "bg-y-s1", "status": _st,
                  "group_key": "bg-y", "display_seq": 0}], runs_dir=_rd3p)[0]
            check("the lone surviving slice row stays IN its bundle while %s" % _st,
                  (_h["plan_bundle"], _h["plan_total"]), (True, 3))
        check("a genuine plan-of-one is still rendered FLAT, not wrapped",
              _annotate_plan_rollup([{"id": "s", "label": "one-off", "status": "pending",
                                      "group_key": "one-off", "display_seq": 0}],
                                    runs_dir=_rd3p)[0]["plan_bundle"], False)

    # --- FIX 1c: the bundle must not read its done-slices off children[0] ---------
    # Measured live: for bg-brokers and bg-profile the FIRST child of the rendered
    # group carried an EMPTY plan_done_slices while a sibling carried two, so every
    # finished slice rendered nowhere. The client now takes the union over the whole
    # bundle; this proves the server never hands it a bundle where that matters, in
    # the one place the server can be held to it.
    with tempfile.TemporaryDirectory() as _rd4:
        _rd4p = Path(_rd4)
        (_rd4p / "bg-z.json").write_text(json.dumps(
            {"order": ["s1", "s2", "s3"],
             "slices": {"s1": {"status": "done"}, "s2": {"status": "pending"},
                        "s3": {"status": "pending"}}}))
        _zr = _annotate_plan_rollup(
            [{"id": "z1", "label": "auto-author-bg-z-s2", "status": "pending",
              "group_key": "bg-z", "display_seq": 0},
             {"id": "z2", "label": "bg-z-s3", "status": "planned",
              "group_key": "bg-z", "display_seq": 1}], runs_dir=_rd4p)
        check("EVERY child of a bundle carries the same done-slice list",
              len({tuple(d["label"] for d in r["plan_done_slices"]) for r in _zr}), 1)
        check("...and it is not empty, so children[0] can never be the blind one",
              [d["label"] for d in _zr[0]["plan_done_slices"]], ["bg-z-s1"])
        check("every child also agrees on X/Y and on being a bundle",
              {(r["plan_done"], r["plan_total"], r["plan_bundle"]) for r in _zr},
              {(1, 3, True)})

    # --- FIX 2: a done slice's row carries its GATE state, and never goes blank ---
    # the user 2026-09-18: "I need them to stop disappearing when they're done and being
    # replaced by gates that then disappear ... change the tag on it to pending gate,
    # and then once the gate resolves, put the updated tag on it".
    # A slice flips to `done` when its CODING job converges; its gate is a separate
    # row that runs after and is pruned the moment it goes terminal.
    _gstate = {"order": ["s1", "s2"],
               "slices": {"s1": {"status": "done", "job_id": "abc123abc123"},
                          "s2": {"status": "pending"}}}
    _fake_verdicts = {"abc123abc123": "CONCERNS"}
    _vof = lambda jid, ld=None: _fake_verdicts.get(jid)
    _gate_live = [{"id": "g", "label": "gate-abc123abc123", "status": "running"}]
    _gp = _plan_done_children(_gstate, "p", _gate_live, verdict_of=_vof)
    check("while its gate row is LIVE the slice row is tagged 'pending gate'",
          (len(_gp), _gp[0]["gate_tag"], _gp[0]["raw_verdict"]),
          (1, "pending gate", None))
    # THE BUG, revert-tested: the gate row goes terminal and is dropped from the
    # payload entirely -- the slice row must now show the RESOLVED verdict, not vanish
    # and not go blank.
    _gp2 = _plan_done_children(_gstate, "p",
                               [dict(_gate_live[0], status="done")], verdict_of=_vof)
    check("once the gate row goes terminal the slice row shows the resolved verdict",
          (len(_gp2), _gp2[0]["gate_tag"], _gp2[0]["raw_verdict"]),
          (1, "CONCERNS", "CONCERNS"))
    _gp3 = _plan_done_children(_gstate, "p", [], verdict_of=_vof)
    check("...and it still shows once the gate row is gone from the payload for good",
          (len(_gp3), _gp3[0]["gate_tag"]), (1, "CONCERNS"))
    check("THE ROW NEVER DISAPPEARS: one row per done slice in all three phases",
          [len(_gp), len(_gp2), len(_gp3)], [1, 1, 1])
    check("a `<slice>-gate` spelling is recognised as pending too",
          _plan_done_children(_gstate, "p",
                              [{"id": "g", "label": "p-s1-gate", "status": "running"}],
                              verdict_of=_vof)[0]["gate_tag"], "pending gate")
    check("a regate row pends the slice exactly like a gate row",
          _plan_done_children(_gstate, "p",
                              [{"id": "g", "label": "regate-abc123abc123",
                                "status": "pending"}],
                              verdict_of=_vof)[0]["gate_tag"], "pending gate")
    check("no sidecar verdict yet + no live gate => no tag, still exactly one row",
          [(r["gate_tag"], r["raw_verdict"])
           for r in _plan_done_children(_gstate, "p", [], verdict_of=lambda j, l=None: None)],
          [(None, None)])
    check("_live_gate_refs ignores terminal gate rows and non-gate rows",
          _live_gate_refs([{"label": "gate-aaa", "status": "running"},
                           {"label": "gate-bbb", "status": "done"},
                           {"label": "regate-ccc", "status": "done_unconverged"},
                           {"label": "auto-author-p-s1", "status": "running"}]),
          {"aaa"})
    # The durable read is the whole point: the verdict comes off <id>.gate.json, which
    # outlives the gate's queue row. Proven end-to-end against real sidecars.
    with tempfile.TemporaryDirectory() as _ld2:
        _ld2p = Path(_ld2)
        (_ld2p / "abc123abc123.done.json").write_text(json.dumps(
            {"id": "abc123abc123", "label": "p-s1", "status": "done"}))
        (_ld2p / "abc123abc123.gate.json").write_text(json.dumps(
            {"verdict": "pass", "job_id": "abc123abc123"}))
        check("the verdict is read from the DURABLE .gate.json sidecar, uppercased",
              _durable_verdict_tag("abc123abc123", _ld2p), "PASS")
        (_ld2p / "ddd456ddd456.done.json").write_text(json.dumps(
            {"id": "ddd456ddd456", "label": "p-s9", "status": "done"}))
        check("a converged job whose gate has not landed yet reads PENDING",
              _durable_verdict_tag("ddd456ddd456", _ld2p), "PENDING")
        check("an unknown job id yields no tag at all, never a crash",
              (_durable_verdict_tag("nosuchjob0000", _ld2p),
               _durable_verdict_tag(None, _ld2p)), (None, None))
        _real = _plan_done_children(_gstate, "p", [], log_dir=_ld2p)
        check("the default resolver wires the sidecar through to the row",
              (_real[0]["gate_tag"], _real[0]["raw_verdict"]), ("PASS", "PASS"))
        # ...and _annotate_plan_rollup passes log_dir down, so /api/jobs gets it.
        with tempfile.TemporaryDirectory() as _rd4:
            _rd4p = Path(_rd4)
            (_rd4p / "p.json").write_text(json.dumps(_gstate))
            _grr = _annotate_plan_rollup(
                [{"id": "b", "label": "p-s2", "status": "planned", "group_key": "p",
                  "display_seq": 0}], runs_dir=_rd4p, log_dir=_ld2p)
            check("plan_done_slices carries the gate tag all the way to the payload",
                  [(d["label"], d["gate_tag"])
                   for d in _grr[0]["plan_done_slices"]], [("p-s1", "PASS")])

    # --- FIX 3: a bundle rolls up the LATEST outcome per slice, not every attempt -
    # Confirmed live 2026-09-19 on bg-captcha and bg-profile: a slice was retried,
    # the 3rd attempt converged clean, and the bundle still shouted `failed` because
    # the two OLD failed rows were the only ones left in the live table (`failed` is
    # not a terminal status so it is never pruned; the winning attempt IS terminal so
    # it is) and the slicer's run-state still said `escalated` (--accept-slice had not
    # been run). See _superseded_failed_ids.
    #
    # Everything below drives the REAL wiring -- _annotate_plan_rollup reading real
    # <id>.done.json/<id>.gate.json sidecars off disk -- so it cannot pass against a
    # stubbed verdict the live dashboard would not get.
    with tempfile.TemporaryDirectory() as _sl, tempfile.TemporaryDirectory() as _sr:
        _slp, _srp = Path(_sl), Path(_sr)

        def _sidecar(jid, label, status, verdict):
            (_slp / f"{jid}.done.json").write_text(json.dumps(
                {"id": jid, "label": label, "status": status}))
            if verdict:
                (_slp / f"{jid}.gate.json").write_text(json.dumps(
                    {"verdict": verdict, "job_id": jid}))

        def _roll(rows):
            return _annotate_plan_rollup([dict(r) for r in rows],
                                         runs_dir=_srp, log_dir=_slp)

        def _bundle_status(annotated, key):
            """What the bundle BADGE shows: the plan_status the plan's LIVE rows
            carry (a terminal row keeps its own per-row status and is not the
            bundle)."""
            return {r["plan_status"] for r in annotated
                    if r.get("group_key") == key
                    and r.get("status") not in _QUEUE_TERMINAL_STATUSES}

        # THE POSITIVE CASE (bg-captcha's shape): 3-slice plan, 2 through. Slice s3's
        # attempts 1 and 2 failed; attempt 3 (the run-state's CURRENT job_id) is done
        # with a clean PASS. The run-state status is the STALE `escalated` left by an
        # earlier preflight NO-GO -- exactly the live value -- so the fix may not lean
        # on it.
        (_srp / "cap.json").write_text(json.dumps(
            {"label": "cap", "order": ["s1", "s2", "s3"],
             "slices": {"s1": {"status": "done", "job_id": "cap1"},
                        "s2": {"status": "done", "job_id": "cap2"},
                        "s3": {"status": "escalated", "job_id": "cap3win"}}}))
        _sidecar("cap3win", "cap-s3", "done", "pass")
        _sidecar("capf1", "auto-refine-cap-s3-r1", "failed", "fail")
        _sidecar("capf2", "auto-refine-cap-s3-r1", "failed", "fail")
        _cap_rows = [
            {"id": "capf1", "label": "auto-refine-cap-s3-r1", "status": "failed",
             "group_key": "cap", "display_seq": 10, "enqueued_at": "2026-09-19T00:18Z"},
            {"id": "capf2", "label": "auto-refine-cap-s3-r1", "status": "failed",
             "group_key": "cap", "display_seq": 11, "enqueued_at": "2026-09-19T02:50Z"},
            {"id": "cap3win", "label": "cap-s3", "status": "done", "group_key": "cap",
             "display_seq": 10004, "enqueued_at": "2026-09-19T04:01Z"},
        ]
        _cap = _roll(_cap_rows)
        check("THE BUG: a bundle whose slice has since converged clean is NOT failed",
              _bundle_status(_cap, "cap"), {"pending"})
        check("...because both stale attempts are flagged superseded",
              sorted(r["id"] for r in _cap if r.get("superseded_by_pass")),
              ["capf1", "capf2"])
        check("NOTHING DISAPPEARS: every row survives, still reading `failed`",
              [(r["id"], r["status"]) for r in _cap],
              [("capf1", "failed"), ("capf2", "failed"), ("cap3win", "done")])
        check("the winning attempt is never called superseded by itself",
              [r["id"] for r in _cap if r.get("superseded_by_pass")].count("cap3win"), 0)

        # bg-profile's shape: the run-state carries job_id=None for the slice (its
        # earlier round failed and --accept-slice was never run), so the CURRENT job
        # has to be found from the slice's own coding row instead.
        (_srp / "prof.json").write_text(json.dumps(
            {"label": "prof", "order": ["s1", "s2", "s3"],
             "slices": {"s1": {"status": "done", "job_id": "pr1"},
                        "s2": {"status": "done", "job_id": "pr2"},
                        "s3": {"status": "failed", "job_id": None}}}))
        _sidecar("prwin", "prof-s3", "done", "pass")
        _sidecar("prf1", "auto-refine-prof-s3-r1", "failed", "fail")
        _prof = _roll([
            {"id": "prwin", "label": "prof-s3", "status": "done", "group_key": "prof",
             "display_seq": 10004, "enqueued_at": "2026-09-19T06:16:03Z"},
            {"id": "prf1", "label": "auto-refine-prof-s3-r1", "status": "failed",
             "group_key": "prof", "display_seq": 20,
             "enqueued_at": "2026-09-19T06:16:13Z"},
        ])
        check("a run-state with job_id=None still finds the slice's coding job",
              (_bundle_status(_prof, "prof"),
               {r["id"] for r in _prof if r.get("superseded_by_pass")}),
              ({"pending"}, {"prf1"}))

        # --- NEGATIVE CONTROLS: bundles that are failing for real stay failed ------
        # bg-eraser's s4-profile-full-name: the slice's CURRENT job is itself a FAIL.
        (_srp / "era.json").write_text(json.dumps(
            {"label": "era", "order": ["s1", "s2"],
             "slices": {"s1": {"status": "done", "job_id": "era1"},
                        "s2": {"status": "escalated", "job_id": "erabad"}}}))
        _sidecar("erabad", "era-s2", "done", "fail")
        _sidecar("eraf1", "auto-refine-era-s2-r1", "failed", "fail")
        _era = _roll([
            {"id": "erabad", "label": "era-s2", "status": "done", "group_key": "era",
             "display_seq": 10004, "enqueued_at": "2026-09-19T05:15Z"},
            {"id": "eraf1", "label": "auto-refine-era-s2-r1", "status": "failed",
             "group_key": "era", "display_seq": 30, "enqueued_at": "2026-09-19T05:12Z"},
        ])
        check("NEGATIVE CONTROL (bg-eraser): a slice whose current job FAILED stays failed",
              (_bundle_status(_era, "era"),
               any(r.get("superseded_by_pass") for r in _era)), ({"failed"}, False))

        # bg-brokers: the slice's current job is FAIL and the failing row is an
        # auto-author stage around it -- nothing clean anywhere, so nothing is hidden.
        (_srp / "brk.json").write_text(json.dumps(
            {"label": "brk", "order": ["s1"],
             "slices": {"s1": {"status": "enqueued", "job_id": "brkcur"}}}))
        _sidecar("brkcur", "brk-s1", "done", "fail")
        _sidecar("brkf1", "auto-author-brk-s1", "failed", "skipped")
        _brk = _roll([
            {"id": "brkf1", "label": "auto-author-brk-s1", "status": "failed",
             "group_key": "brk", "display_seq": 40, "enqueued_at": "2026-09-19T03:58Z"},
            {"id": "brkcur", "label": "brk-s1", "status": "needs_opus",
             "group_key": "brk", "display_seq": 41, "enqueued_at": "2026-09-19T05:49Z"},
        ])
        check("NEGATIVE CONTROL (bg-brokers): a SKIPPED/FAIL slice keeps its failed badge",
              (_bundle_status(_brk, "brk"),
               any(r.get("superseded_by_pass") for r in _brk)), ({"failed"}, False))

        # cc-waitlist-r2: no run-state file and no coding row for the slice at all.
        _sidecar("ccf1", "auto-author-cc-s5", "failed", "fail")
        _cc = _roll([
            {"id": "ccf1", "label": "auto-author-cc-s5", "status": "failed",
             "group_key": "cc", "display_seq": 50, "enqueued_at": "2026-09-19T04:00Z"},
            {"id": "ccp1", "label": "cc-s6", "status": "failed",
             "group_key": "cc", "display_seq": 51, "enqueued_at": "2026-09-19T04:10Z"},
        ])
        check("NEGATIVE CONTROL (cc-waitlist): no readable current job supersedes nothing",
              (_bundle_status(_cc, "cc"),
               any(r.get("superseded_by_pass") for r in _cc)), ({"failed"}, False))

        # Only a CLEAN pass counts. PASS-PENDING-REVIEW means the gate has not spoken.
        (_srp / "ppr.json").write_text(json.dumps(
            {"label": "ppr", "order": ["s1"],
             "slices": {"s1": {"status": "done", "job_id": "pprwin"}}}))
        _sidecar("pprwin", "ppr-s1", "done", "pass-pending-review")
        _sidecar("pprf1", "auto-refine-ppr-s1-r1", "failed", "fail")
        _ppr = _roll([
            {"id": "pprwin", "label": "ppr-s1", "status": "done", "group_key": "ppr",
             "display_seq": 10004, "enqueued_at": "2026-09-19T01:00Z"},
            {"id": "pprf1", "label": "auto-refine-ppr-s1-r1", "status": "failed",
             "group_key": "ppr", "display_seq": 60, "enqueued_at": "2026-09-19T01:10Z"},
        ])
        check("PASS-PENDING-REVIEW does NOT supersede a failure (the gate has not spoken)",
              (_bundle_status(_ppr, "ppr"),
               any(r.get("superseded_by_pass") for r in _ppr)), ({"failed"}, False))
        check("...and _superseded_failed_ids agrees directly on the same rows",
              _superseded_failed_ids(
                  [{"id": "pprf1", "label": "auto-refine-ppr-s1-r1", "status": "failed",
                    "group_key": "ppr"}],
                  state_of=lambda k: json.loads((_srp / f"{k}.json").read_text()),
                  log_dir=_slp), set())

        # A REAL failure in the same bundle as a superseded one still leads. This is
        # why the lead pool is FILTERED rather than the rank nudged: `failed` and the
        # superseded stand-in tie on rank, so display_seq alone would have decided.
        (_srp / "mixp.json").write_text(json.dumps(
            {"label": "mixp", "order": ["s1", "s2"],
             "slices": {"s1": {"status": "escalated", "job_id": "mixwin"},
                        "s2": {"status": "failed", "job_id": "mixbad"}}}))
        _sidecar("mixwin", "mixp-s1", "done", "pass")
        _sidecar("mixbad", "mixp-s2", "done", "fail")
        _sidecar("mixf1", "auto-refine-mixp-s1-r1", "failed", "fail")
        _sidecar("mixf2", "auto-refine-mixp-s2-r1", "failed", "fail")
        _mixb = _roll([
            {"id": "mixf1", "label": "auto-refine-mixp-s1-r1", "status": "failed",
             "group_key": "mixp", "display_seq": 1, "enqueued_at": "2026-09-19T01:00Z"},
            {"id": "mixf2", "label": "auto-refine-mixp-s2-r1", "status": "failed",
             "group_key": "mixp", "display_seq": 99, "enqueued_at": "2026-09-19T02:00Z"},
            {"id": "mixwin", "label": "mixp-s1", "status": "done", "group_key": "mixp",
             "display_seq": 10004, "enqueued_at": "2026-09-19T03:00Z"},
            {"id": "mixbad", "label": "mixp-s2", "status": "done", "group_key": "mixp",
             "display_seq": 10005, "enqueued_at": "2026-09-19T03:10Z"},
        ])
        check("a REAL failure still leads even when it sorts BELOW a superseded one",
              (_bundle_status(_mixb, "mixp"),
               {r["id"] for r in _mixb if r.get("superseded_by_pass")}),
              ({"failed"}, {"mixf1"}))

        # Nothing else about the bundle moves: the fraction and the incomplete flag
        # are untouched by the supersede. (Size is NOT a supersede effect -- see the
        # count below.)
        check("the supersede changes ONLY the lead status -- not X/Y, size or the flag",
              [(r["plan_done"], r["plan_total"], r["plan_size"], r["plan_incomplete"])
               for r in _cap if r["id"] == "capf1"],
              # 5 rendered children: the 2 live failed rows, the REAL done+PASS row
              # cap3win, and the 2 synthesized done-slice ticks for s1 and s2.
              #
              # This asserted 4 until 2026-09-19 (the user: "batch should show the full
              # run history always, except gates -- gates can get dropped after
              # running"). cap3win is an actual job attempt that PASSED, and it used
              # to be dropped on the floor: the stranded-row recovery only ran for
              # plans with NO live rows, so a plan still holding a failed refine
              # rendered the failure and silently swallowed the pass. That is the
              # rt-costco shape exactly -- 8e721ab56c0e (failed) stayed on the card
              # while a31ff9233e4c and 227cb3bf0cae (both PASS) vanished from it.
              # The pass is history and history is permanent; only gate/review
              # sub-artifacts may be dropped once consumed.
              [(2, 3, 5, True)])
        # A bundle with NOTHING owed still reads done, not pending, after a supersede.
        (_srp / "fin.json").write_text(json.dumps(
            {"label": "fin", "order": ["s1"],
             "slices": {"s1": {"status": "done", "job_id": "finwin"}}}))
        _sidecar("finwin", "fin-s1", "done", "pass")
        _sidecar("finf1", "auto-refine-fin-s1-r1", "failed", "fail")
        _fin = _roll([
            {"id": "finf1", "label": "auto-refine-fin-s1-r1", "status": "failed",
             "group_key": "fin", "display_seq": 70, "enqueued_at": "2026-09-19T01:00Z"},
            {"id": "finwin", "label": "fin-s1", "status": "done", "group_key": "fin",
             "display_seq": 10004, "enqueued_at": "2026-09-19T02:00Z"},
        ])
        check("a COMPLETE plan reads done (not pending) once its failure is superseded",
              _bundle_status(_fin, "fin"), {"done"})

    # --- an unfinished bundle NEVER reads as completed --------------------------
    # the user: "while we work through the bundle the whole thing is pending and
    # shouldn't be moved to completed until everything is completed."
    with tempfile.TemporaryDirectory() as _rd3:
        _rd3p = Path(_rd3)
        # 4-slice plan, 2 through -- but its only surviving rows are DONE, which is
        # the window where the bundle used to read completed (or vanish entirely).
        (_rd3p / "midplan.json").write_text(json.dumps(
            {"label": "midplan", "order": ["s1-a", "s2-b", "s3-c", "s4-d"],
             "slices": {"s1-a": {"status": "done"}, "s2-b": {"status": "done"},
                        "s3-c": {"status": "pending"}, "s4-d": {"status": "pending"}}}))
        _mid = _annotate_plan_rollup([
            {"id": "m1", "group_key": "midplan", "status": "done", "display_seq": 10001,
             "label": "auto-author-midplan-s1-a"},
            {"id": "m2", "group_key": "midplan", "status": "done", "display_seq": 10002,
             "label": "auto-author-midplan-s2-b"},
        ], runs_dir=_rd3p)
        check("a plan whose every row is terminal but X<Y does NOT read completed",
              {r["plan_status"] for r in _mid}, {"pending"})
        check("...and it is flagged incomplete so the front-end keeps showing it",
              [r["plan_incomplete"] for r in _mid], [True, True])
        check("...and it still forms a bundle (not two stray flat done rows)",
              {r["plan_size"] for r in _mid}, {2})
        check("the fraction is untouched by the clamp",
              (_mid[0]["plan_done"], _mid[0]["plan_total"]), (2, 4))
        # X == Y: the plan really is finished, so done may show.
        (_rd3p / "fullplan.json").write_text(json.dumps(
            {"label": "fullplan", "order": ["s1-a", "s2-b"],
             "slices": {"s1-a": {"status": "done"}, "s2-b": {"status": "done"}}}))
        _full = _annotate_plan_rollup([
            {"id": "f1", "group_key": "fullplan", "status": "done", "display_seq": 10001,
             "label": "auto-author-fullplan-s1-a"},
            {"id": "f2", "group_key": "fullplan", "status": "done", "display_seq": 10002,
             "label": "auto-author-fullplan-s2-b"},
        ], runs_dir=_rd3p)
        check("a plan with X == Y DOES report done (nothing left to owe)",
              {r["plan_status"] for r in _full}, {"done"})
        check("a finished plan is not flagged incomplete (front-end drops it again)",
              [r["plan_incomplete"] for r in _full], [False, False])
        # A failed slice must still surface as failed -- the clamp is only about not
        # showing SUCCESS-complete early.
        _fail = _annotate_plan_rollup([
            {"id": "x1", "group_key": "midplan", "status": "failed", "display_seq": 1,
             "label": "auto-author-midplan-s3-c"},
            {"id": "x2", "group_key": "midplan", "status": "done", "display_seq": 10002,
             "label": "auto-author-midplan-s1-a"},
        ], runs_dir=_rd3p)
        check("a failed slice is NEVER masked by the clamp",
              {r["plan_status"] for r in _fail if r["id"] == "x1"}, {"failed"})
        # A terminal row of a plan that STILL HAS live rows joins the bundle too.
        #
        # This asserted the opposite (a terminal row "stays ungrouped") until
        # 2026-09-19, and its own comment gave the reason away: "no normal bundle
        # changes because of this" -- it was conservatism about blast radius, not a
        # defence against any bug. the user's criterion retires it: "batch should show
        # the full run history always". y2 is a real author attempt that finished;
        # leaving it outside the bundle is exactly how a completed attempt fell off
        # its own plan card. It is grouped, so it carries the PLAN's rollup
        # (pending / incomplete) rather than its own bare row status.
        _mix = _annotate_plan_rollup([
            {"id": "y1", "group_key": "midplan", "status": "pending", "display_seq": 1,
             "label": "auto-author-midplan-s3-c"},
            {"id": "y2", "group_key": "midplan", "status": "done", "display_seq": 10002,
             "label": "auto-author-midplan-s1-a"},
        ], runs_dir=_rd3p)
        _bm = {r["id"]: r for r in _mix}
        check("a terminal row of a plan that still has live rows JOINS the bundle",
              (_bm["y2"]["plan_incomplete"], _bm["y2"]["plan_status"]), (True, "pending"))
        check("...while the live side of that plan reads pending as usual",
              _bm["y1"]["plan_status"], "pending")

        # THE rt-costco SHAPE, end to end (2026-09-19). One slice, three real
        # attempts: a failed refine and two that passed. Before the fix the card
        # showed ONLY the failure -- both passes were filtered out of `live` as
        # terminal and then denied re-entry by the stranded-row guard, because the
        # plan still had that one failed row keeping it "live". All three attempts
        # are job history and all three must render.
        (_rd3p / "rtc.json").write_text(json.dumps(
            {"label": "rtc", "order": ["s1", "s2"],
             "slices": {"s1": {"status": "done", "job_id": "rtcp2"},
                        "s2": {"status": "pending"}}}))
        _rtc = _annotate_plan_rollup([
            {"id": "rtcfail", "group_key": "rtc", "status": "failed", "display_seq": 1,
             "label": "auto-refine-rtc-s1-r1"},
            {"id": "rtcp1", "group_key": "rtc", "status": "done", "display_seq": 10002,
             "label": "auto-refine-rtc-s1-r2"},
            {"id": "rtcp2", "group_key": "rtc", "status": "done", "display_seq": 10003,
             "label": "rtc-s1"},
        ], runs_dir=_rd3p)
        # NOT just "the rows exist" -- _annotate_plan_rollup returns every row it is
        # given, so an id check alone passes even with the bug. What the bug did was
        # leave the passes OUTSIDE the bundle as bare strays. So assert each one is
        # bundled: carrying the plan's rollup, not its own per-row default.
        check("rt-costco: every attempt is IN the bundle, passes included",
              sorted((r["id"], r["plan_incomplete"], r["plan_status"]) for r in _rtc),
              [("rtcfail", True, "failed"), ("rtcp1", True, "failed"),
               ("rtcp2", True, "failed")])
        check("...all three are in ONE bundle, not a failure plus two strays",
              {r.get("group_key") for r in _rtc} | {r["plan_size"] for r in _rtc},
              {"rtc", 3})
        check("...and no attempt is rendered twice (the dupe guard still bites)",
              len(_rtc) - len({r["id"] for r in _rtc}), 0)

    # --- FIX 2: a gate/regate row belongs INSIDE its plan's bundle --------------
    # ollama-queue.py's convention is the PREFIX form gate-<parent job id> (live:
    # gate-83784637d033), whose parent is usually already pruned from state.json.
    check("the prefix form is recognised and yields the parent reference",
          (_gate_parent_ref("gate-83784637d033"), _gate_parent_ref("regate-abc123def456")),
          ("83784637d033", "abc123def456"))
    check("a trailing -gate/-regate spelling is accepted too",
          (_gate_parent_ref("alpha-s1-a-gate"), _gate_parent_ref("alpha-s1-a-regate")),
          ("alpha-s1-a", "alpha-s1-a"))
    check("refine rounds and [annotations] are stripped off the reference",
          _gate_parent_ref("regate-alpha-s1-a-r2 [auto-fix r2]"), "alpha-s1-a")
    check("an ordinary slice row is NOT a gate row",
          _gate_parent_ref("auto-author-alpha-s1-a"), None)
    _saved_idx2 = q._GROUP_INDEX_CACHE.copy()
    try:
        q._GROUP_INDEX_CACHE.update({"at": time.monotonic(),
                                     "reverse": {"alpha-s1-a": "alpha", "alpha-s2-b": "alpha"}})
        with tempfile.TemporaryDirectory() as _ld2:
            _ld2p = Path(_ld2)
            (_ld2p / "deadbeef1234.done.json").write_text(
                json.dumps({"id": "deadbeef1234", "label": "auto-author-alpha-s2-b"}))
            _grows2 = _annotate_job_groups([
                {"id": "live1", "label": "auto-author-alpha-s1-a", "status": "running"},
                {"id": "gA", "label": "gate-live1", "status": "pending"},
                {"id": "gB", "label": "gate-deadbeef1234", "status": "pending"},
                {"id": "gC", "label": "alpha-s2-b-gate", "status": "pending"},
                {"id": "gD", "label": "gate-ffffffffffff", "status": "pending"},
            ], log_dir=_ld2p)
            _bg2 = {r["id"]: r["group_key"] for r in _grows2}
            check("a gate whose parent row is still live joins THAT row's plan",
                  _bg2["gA"], "alpha")
            check("a gate whose parent was pruned resolves via its durable sidecar",
                  _bg2["gB"], "alpha")
            check("a <plan>-<sid>-gate label groups under <plan>", _bg2["gC"], "alpha")
            check("an unresolvable gate stays standalone (its own key, not a bundle)",
                  _bg2["gD"] in (None, "gate-ffffffffffff"), True)
            check("the gated slice row itself is untouched", _bg2["live1"], "alpha")
            check("gate rows count toward the plan's pending block",
                  {r["group_pending"] for r in _grows2 if r["id"] == "gA"}, {3})
    finally:
        q._GROUP_INDEX_CACHE.clear()
        q._GROUP_INDEX_CACHE.update(_saved_idx2)

    # --- bundle reordering: POST /api/jobs/move-group ---------------------------
    # Whole bundles move relative to each other; slice order inside one never does
    # (move_group preserves it). Route/handler wiring + the pure body parse.
    check("move-group body: id is required",
          _move_group_args({})[2], "id required")
    check("a non-object body is refused, not crashed on",
          _move_group_args(None)[2], "json object body required")
    check("id + before_id are passed straight through as ROW ids",
          _move_group_args({"id": "a1", "before_id": "b1"}), ("a1", "b1", None))
    check("a missing/null/empty before_id means 'send to the end'",
          [_move_group_args(d)[1] for d in ({"id": "a1"}, {"id": "a1", "before_id": None},
                                            {"id": "a1", "before_id": ""})],
          [None, None, None])
    check("moving a bundle before ITSELF is a no-op to the end, not a 400",
          _move_group_args({"id": "a1", "before_id": "a1"}), ("a1", None, None))
    check("POST /api/jobs/move-group is its own route, not the per-row /move",
          ("/api/jobs/move-group" == "/api/jobs/move"), False)
    check("the per-row move route still matches exactly (not regressed)",
          ("/api/jobs/move" == "/api/jobs/move"), True)
    check("move-group is not swallowed by the /api/jobs/<id>/... routes",
          bool(re.match(r"^/api/jobs/([^/]+)/(promote|promote-group|hold|resume)$",
                        "/api/jobs/move-group")), False)
    check("the handler exposes _move_group", hasattr(Handler, "_move_group"), True)
    check("it delegates to ollama-queue.py's move_group_for_job",
          callable(getattr(q, "move_group_for_job", None)), True)
    check("...which is the wrapper over move_group (bundle-level, not per-slice)",
          callable(getattr(q, "move_group", None)), True)

    # Batch hold is a fan-out over the per-job route added for it; prove it is wired.
    check("POST /api/jobs/<id>/hold matches the hold route",
          re.match(r"^/api/jobs/([^/]+)/hold$", "/api/jobs/abc123/hold").group(1), "abc123")
    check("the hold route is not swallowed by the promote/resume routes",
          bool(re.match(r"^/api/jobs/([^/]+)/(promote|resume)$", "/api/jobs/abc123/hold")), False)
    check("the handler exposes _hold", hasattr(Handler, "_hold"), True)
    check("it delegates to ollama-queue.py's hold_job",
          callable(getattr(q, "hold_job", None)), True)

    # --- an open tab cannot sit on an old front-end forever (2026-09-19) --------
    # The dashboard polls data into an existing DOM but never re-fetches its own
    # inline CSS/JS, so a restarted server left every open tab on the old build --
    # which is how a shipped, correctly-deployed mobile fix rendered as the
    # PREVIOUS commit on the user's phone while the job data on it was fully current.
    check("the page carries the build it was served as",
          f"const FRONTEND_VERSION = {json.dumps(FRONTEND_VERSION)};" in FRONTEND_HTML,
          True)
    check("no unsubstituted version placeholder is shipped",
          "__FRONTEND_VERSION__" in FRONTEND_HTML, False)
    check("the version is derived from the PAGE, so it changes only when the "
          "front-end does -- never on a mere restart",
          FRONTEND_VERSION,
          hashlib.sha256(
              FRONTEND_HTML.replace(
                  f"const FRONTEND_VERSION = {json.dumps(FRONTEND_VERSION)};",
                  "const FRONTEND_VERSION = __FRONTEND_VERSION__;").encode()
          ).hexdigest()[:12])
    check("every JSON response advertises it, so the check needs no extra request",
          'self.send_header("X-Frontend-Version", FRONTEND_VERSION)'
          in inspect.getsource(Handler._json), True)
    check("the poll the page already makes is what checks it",
          "checkFrontendVersion(res);" in FRONTEND_HTML, True)
    check("a mismatch reloads", "location.reload();" in FRONTEND_HTML, True)
    # The in-memory flag resets on reload, so it CANNOT stop a loop on its own --
    # measured in WebKit against a pinned mismatching header: 419 loads in 12s.
    # The attempt has to outlive the document, hence sessionStorage.
    check("the reload attempt outlives the reload, so a build is chased only ONCE",
          ("sessionStorage.setItem(_RELOAD_KEY, v)" in FRONTEND_HTML
           and "if (tried === v) return;" in FRONTEND_HTML), True)
    check("...and being current clears it, so a LATER build is still chased",
          "sessionStorage.removeItem(_RELOAD_KEY)" in FRONTEND_HTML, True)
    check("blocked storage (private mode) never breaks the poll",
          FRONTEND_HTML.count("catch (e) { /* blocked */ }") >= 2, True)
    check("an unreadable header never breaks the poll",
          "catch (e) { /* header unreadable" in FRONTEND_HTML, True)

    # BUNDLE ORDER is stable across run activity (the user 2026-09-27: bundle rows kept
    # changing position as runs started/finished). The committed bundle C is walked
    # through every activity phase -- author running, author done + coding pending,
    # coding running, everything done between slices -- and the whole /api/jobs
    # pipeline (display_seq -> plan rollup -> bundle rank) plus the front-end's sort
    # key must render the SAME bundle sequence every time.
    with tempfile.TemporaryDirectory() as _bo_rd:
        _bo_state = {"_bundle_commit": {"key": "C"},
                     "_bundle_parked": {"P2": {"since": 200.0}, "P1": {"since": 100.0}},
                     "pinned_group": None}

        def _bo_rows(c_rows):
            return ([{"id": "x1", "group_key": "B", "status": "pending",
                      "label": "B-s1", "enqueued_at": "2026-09-27T10:00:00+00:00"},
                     {"id": "d1", "group_key": "D", "status": "done", "label": "D-s1",
                      "enqueued_at": "2026-09-27T08:00:00+00:00", "wall_s": 60}]
                    + c_rows +
                    [{"id": "p2", "group_key": "P2", "status": "pending", "label": "P2-s1",
                      "enqueued_at": "2026-09-27T10:05:00+00:00"},
                     {"id": "y1", "group_key": "E", "status": "pending", "label": "E-s1",
                      "enqueued_at": "2026-09-27T10:10:00+00:00"},
                     {"id": "p1", "group_key": "P1", "status": "planned", "label": "P1-s1",
                      "enqueued_at": "2026-09-27T10:15:00+00:00"},
                     {"id": "h1", "group_key": "H", "status": "held", "label": "H-s1",
                      "enqueued_at": "2026-09-27T07:00:00+00:00"},
                     {"id": "d2", "group_key": "D2", "status": "done", "label": "D2-s1",
                      "enqueued_at": "2026-09-27T09:00:00+00:00", "wall_s": 60}])

        def _bo_render(c_rows):
            rows = _annotate_bundle_rank(_annotate_plan_rollup(_annotate_display_seq(
                [dict(r) for r in _bo_rows(c_rows)], focus_key="C"),
                runs_dir=Path(_bo_rd), log_dir=Path(_bo_rd)), _bo_state, focus_key="C")
            seq = lambda r: r.get("display_seq", 10_000)
            rows.sort(key=lambda r: (r.get("bundle_rank", 0),
                                     r.get("plan_seq", seq(r)), seq(r)))
            out = []
            for r in rows:
                if r["group_key"] not in out:
                    out.append(r["group_key"])
            return out

        _c = lambda i, st, lab: {"id": i, "group_key": "C", "status": st, "label": lab,
                                 "enqueued_at": "2026-09-27T11:00:00+00:00"}
        _phases = [
            [_c("c1", "running", "auto-author-C-s2"), _c("c9", "planned", "C-s3")],
            [_c("c1", "done", "auto-author-C-s2"), _c("c2", "pending", "C-s2"),
             _c("c9", "planned", "C-s3")],
            [_c("c1", "done", "auto-author-C-s2"), _c("c2", "running", "C-s2"),
             _c("c9", "planned", "C-s3")],
            [_c("c1", "done", "auto-author-C-s2"), _c("c2", "done", "C-s2")],
            [],                                     # the gap: C has no row at all
        ]
        _want = ["C", "P1", "P2", "B", "E", "H", "D", "D2"]
        for _i, _ph in enumerate(_phases):
            check(f"bundle order is stable across activity (phase {_i})",
                  _bo_render(_ph), _want if _ph else _want[1:])
        # the OLD sort key (plan_seq first) really did move C -- the guard bites
        _old = lambda c_rows: [r["group_key"] for r in sorted(
            _annotate_plan_rollup(_annotate_display_seq(
                [dict(r) for r in _bo_rows(c_rows)], focus_key="C"),
                runs_dir=Path(_bo_rd), log_dir=Path(_bo_rd)),
            key=lambda r: (r.get("plan_seq", 0), r.get("display_seq", 10_000)))]
        check("without bundle_rank the committed bundle moves when its run finishes",
              _old(_phases[2]).index("C") != _old(_phases[3]).index("C"), True)
        check("no commitment: the daemon focus key still ranks first",
              _bundle_display_order([{"id": "a", "group_key": "A", "status": "pending"},
                                     {"id": "b", "group_key": "B", "status": "running"}],
                                    {}, focus_key="B"), ["B", "A"])
        check("pinned group ranks ahead of earlier-queued pending bundles",
              _bundle_display_order([{"id": "a", "group_key": "A", "status": "pending"},
                                     {"id": "b", "group_key": "B", "status": "pending"}],
                                    {"pinned_group": "B"}), ["B", "A"])
        check("a running job does not promote a non-committed bundle",
              _bundle_display_order([{"id": "a", "group_key": "A", "status": "pending"},
                                     {"id": "b", "group_key": "B", "status": "running"}],
                                    {"_bundle_commit": {"key": "Z"}}), ["A", "B"])
        check("front-end sorts on bundle_rank first",
              "brank(a) - brank(b)" in FRONTEND_HTML, True)

    # THE ARROWS are the interface (the user 2026-09-27): drive the real handler methods
    # with a fake request and a recording queue module.
    _calls = []

    class _FakeH:
        def __init__(self, body):
            self._body = body
            self.out = None
        def _read_json_body(self):
            return dict(self._body)
        def _json(self, obj):
            self.out = obj
        def _text(self, t, code=200):
            self.out = (code, t)
    _real_pgj = q.promote_group_for_job
    try:
        q.promote_group_for_job = lambda jid, preempt=False, take_focus=False: (
            _calls.append((jid, preempt, take_focus)) or {"truth": "launches next"})
        _h = _FakeH({"take_focus": True})
        Handler._promote_group(_h, "m1")
        check("arrows: bundle ↑↑ handler forwards take_focus, no preempt",
              _calls[-1], ("m1", False, True))
    finally:
        q.promote_group_for_job = _real_pgj
    check("arrows: the bundle ↑↑ button posts take_focus and no pause confirm",
          "body: JSON.stringify({take_focus: true})});\n      if (!res.ok) alert('promote plan failed"
          in FRONTEND_HTML and "and run bundle" not in FRONTEND_HTML, True)
    check("arrows: every arrow reply is surfaced (showTruth)",
          FRONTEND_HTML.count("showTruth(await truthOf(res))") >= 4, True)

    # AD-HOC BUNDLE (2026-09-27, mlx-smoke). Three jobs enqueued with --bundle and no
    # slice plan must render as ONE bundle, ranked as one, through the real /api/jobs
    # chain starting from raw state rows (so _job_summary is exercised). Red on
    # revert: without the bundle field in _job_summary each row is its own group.
    with tempfile.TemporaryDirectory() as _ah_rd:
        _ah_raw = [{"id": f"m{i}", "label": lab, "status": st, "bundle": "mlx-smoke",
                    "enqueued_at": f"2026-09-27T12:0{i}:00+00:00"}
                   for i, (lab, st) in enumerate([("mlx-toolcall-smoke", "running"),
                                                  ("mlx-go-slice-a", "pending"),
                                                  ("mlx-go-slice-b", "pending")])]
        _ah_raw += [{"id": "o1", "label": "other-job", "status": "pending",
                     "enqueued_at": "2026-09-27T11:00:00+00:00"}]
        _ah_state = {"jobs": _ah_raw, "_bundle_commit": {"key": "mlx-smoke"}}
        _ah = _annotate_bundle_rank(_annotate_plan_rollup(_annotate_display_seq(
            _annotate_job_groups([_job_summary(j) for j in _ah_raw]),
            focus_key="mlx-smoke"), runs_dir=Path(_ah_rd), log_dir=Path(_ah_rd)),
            _ah_state)
        _m = [r for r in _ah if r["id"].startswith("m")]
        check("ad-hoc bundle: summary keeps the --bundle tag",
              {r.get("bundle") for r in _m}, {"mlx-smoke"})
        check("ad-hoc bundle: all 3 share group_key = the tag",
              {r["group_key"] for r in _m}, {"mlx-smoke"})
        check("ad-hoc bundle: renders as ONE bundle of 3",
              [(r["plan_bundle"], r["plan_size"]) for r in _m], [(True, 3)] * 3)
        check("ad-hoc bundle: one stable bundle_rank, ahead of the untagged job",
              (len({r["bundle_rank"] for r in _m}),
               _m[0]["bundle_rank"] < next(r for r in _ah if r["id"] == "o1")["bundle_rank"]),
              (1, True))

    # NUMBERED RERUNS (the user 2026-10-01). The real chain off ollama-queue-state.json:
    # dbcf30f84454 -> d96a71500b9b -> f059db0bce62. _job_summary must expose
    # rerun={n, cause} so the row can draw "#3" with the cause on hover, and must
    # expose NOTHING on a first attempt.
    _rr_raw = [
        {"id": "dbcf30f84454", "label": "auto-author-sidecar-bfmr-login-nudge",
         "status": "failed", "bundle": "sidecar-bfmr-login-nudge",
         "auto_fix_root": "dbcf30f84454", "failure_class": "model",
         "failure_detail": "stopped at iteration 11/24; VERIFY FAILED (exit 1)"},
        {"id": "d96a71500b9b", "label": "auto-author-sidecar-bfmr-login-nudge-c1",
         "status": "failed", "continues": "dbcf30f84454", "failure_class": "model",
         "bundle": "sidecar-bfmr-login-nudge", "auto_fix_root": "d96a71500b9b",
         "failure_detail": "stopped at iteration 12/24; VERIFY FAILED (exit 1)"},
        {"id": "f059db0bce62", "label": "auto-author-sidecar-bfmr-login-nudge-c2",
         "status": "failed", "continues": "d96a71500b9b", "failure_class": "model",
         "bundle": "sidecar-bfmr-login-nudge", "auto_fix_root": "f059db0bce62"},
    ]
    _rr = [_job_summary(j, _rr_raw) for j in _rr_raw]
    check("rerun: the first attempt carries no badge",
          "rerun" in _rr[0], False)
    check("rerun: the continuation rounds are numbered #2 and #3",
          [r["rerun"]["n"] for r in _rr[1:]], [2, 3])
    check("rerun: the cause names the ancestor it continues and the failure class",
          ("continues d96a71500b9b" in _rr[2]["rerun"]["cause"],
           _rr[2]["rerun"]["cause"].endswith("; caused by: model")), (True, True))
    check("rerun: a stamped job['rerun'] is passed through verbatim",
          _job_summary({"id": "z", "label": "l", "rerun": {"n": 7, "cause": "c"}}
                       )["rerun"], {"n": 7, "cause": "c"})
    check("rerun badge: the label cell draws #N with the cause as an escaped title",
          ('title="${escapeHtml(j.rerun.cause || \'\')}">#${escapeHtml(String(j.rerun.n))}'
           in FRONTEND_HTML
           and "<td>${escapeHtml(j.label || '')}${j.rerun ?" in FRONTEND_HTML), True)

    print("SELF-TEST " + ("PASSED" if ok else "FAILED"))
    return ok


# Was bound to the LAN IP only (not 0.0.0.0), on the theory that this kept "no
# other route in" besides the Cloudflare tunnel connector -- that reasoning was
# wrong (github-projects-bf caught it live 2026-08-29): binding to a specific
# interface address restricts by INTERFACE, not by caller identity. Any device
# on the LAN could already reach <LAN-IP>:7684 directly, tunnel or not, so
# the single-IP bind provided no actual isolation -- it just also excluded
# 127.0.0.1/localhost, breaking every local CLI/tooling probe from the SAME
# machine (confirmed: bf spent real time believing this API was down based on
# a localhost check that could never have worked, while it was reachable fine
# over the LAN IP the whole time). 0.0.0.0 fixes local access with no actual
# change to the real exposure, since LAN-wide reachability already existed.
BIND_ADDR = "0.0.0.0"

if __name__ == "__main__":
    if "--self-test" in sys.argv:
        sys.exit(0 if _self_test() else 1)
    _lim = _raise_fd_limit()
    threading.Thread(target=_retention_loop, name="runstatus-retention", daemon=True).start()
    try:
        import cpu_lane
        cpu_lane.read_token(create=True)      # 0600 random token if absent (never printed)
        cpu_lane.start_reaper()
    except Exception as _e:  # noqa: BLE001
        print(f"[queue-api] cpu lane disabled: {_e!r}", flush=True)
    srv = ThreadingServer((BIND_ADDR, PORT), Handler)
    # flush=True: under launchd stdout is a file, so it is block-buffered and
    # this one-line banner would otherwise sit in the buffer forever (the
    # per-request lines go via stderr, which is unbuffered).
    print(f"[queue-api] listening on {BIND_ADDR}:{PORT} (RLIMIT_NOFILE soft/hard = {_lim})", flush=True)
    srv.serve_forever()
