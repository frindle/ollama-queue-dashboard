#!/usr/bin/env python3
"""Standalone agentic coding dispatch against Ollama's native /api/chat,
bypassing opencode's CLI entirely.

Built 2026-08-21 because opencode's CLI has two confirmed harness bugs
that make it unreliable for real dispatch:
  1. It corrupts the user-turn prompt before sending it to the provider
     (partial/broken quote-escaping) -- causally proven, 0/7 vs 6/6
     tool-call success with/without the corruption. Filed upstream:
     https://github.com/anomalyco/opencode/issues/43923
  2. A separate infinite self-nudge loop in its `build` agent
     ("Continue if you have next steps..."), confirmed model-independent.
Both bugs live inside opencode's compiled binary and don't exist if
opencode isn't in the dispatch path -- hence this script talks to Ollama
directly and implements its own minimal tool-execution loop.

Termination is based purely on "the model's response has no more
tool_calls" -- never on injecting a self-generated "continue" prompt --
which is the structural fix for bug #2 above.
"""
import argparse
import html as html_module
import io
import importlib.util
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

try:
    import trafilatura
except ImportError:
    trafilatura = None

try:
    import pypdf
except ImportError:
    pypdf = None

try:
    import pdfplumber
except ImportError:
    pdfplumber = None

DEFAULT_MODEL = "qwen3-14b-agentic"
DEFAULT_HOST = "http://10.0.7.143:11434"
# --api openai's default when --host is omitted (start-llama-server-qwen3.8.sh's
# fixed port; it must already be running, this script does not start it) --
# pick_host() only knows the two native-Ollama hosts, so this is separate.
DEFAULT_OPENAI_HOST = "http://127.0.0.1:8091"
# Models confirmed to crash on Ollama's native /api/chat (the "no user query
# found in messages" template bug, github.com/ollama/ollama/issues/17778) --
# these MUST go through the DEFAULT_OPENAI_HOST llama-server bypass. Added
# 2026-08-29 after a real double-load: a dispatch accidentally routed
# qwen3.8:27b-q8_0 through native Ollama on Studio (127.0.0.1:11434) while
# the dedicated bypass server for the exact same model was already resident
# on DEFAULT_OPENAI_HOST -- two ~35GB copies of the same weights in Studio's
# 64GB unified memory at once, which pushed swap to 13GB+ and left the
# bypass server wedged (every request 500'd with "Compute error") until both
# were killed and restarted. See ensure_model_ready()'s native-Ollama branch.
TEMPLATE_BUG_MODELS = {"qwen3.8:27b-q8_0", "devstral:24b"}
DEFAULT_TEMPERATURE = 0.15
DEFAULT_NUM_CTX = 16384
DEFAULT_MAX_ITERS = 30  # raised 2026-08-28 from 20 -- now that --chat-timeout/--max-tokens/
                        # --repeat-penalty all bound the worst case PER iteration, a higher
                        # iteration ceiling no longer multiplies runaway risk the way it did
                        # before those existed; a real coding dispatch tonight (the live-log
                        # feature) used most of a 16-iteration budget for genuinely productive
                        # work, and a research task hit DID-NOT-CONVERGE at 15 while still
                        # making real (if unproductive) progress -- both suggest the old default
                        # was cutting off legitimate work, not just runaway loops.
# 60s was tight: a warm `npm run build` measures ~10-30s, but a large edit that
# invalidates the Turbopack cache can exceed it, and the model then receives a
# spurious "command timed out" that looks like its own code hanging. Cheap
# insurance -- a real hang still terminates, just later.
BASH_TIMEOUT_S = 240
WEB_TIMEOUT_S = 20
# Per-page cap for read_file. In CHARS, mirroring WEB_FETCH_MAX_CHARS, because a
# char budget is token-agnostic and needs no tokenizer. 16000 chars is ~4K
# tokens. Additionally clamped at call time to ~1/8 of the context window
# (num_ctx * 4 // 8 chars) so a small-ctx dispatch cannot spend an eighth of its
# window on a single read -- the cap that matters is whichever is SMALLER.
READ_FILE_MAX_CHARS = 16000

# Pre-send ceiling, as a fraction of num_ctx. Deliberately ABOVE
# CONTEXT_REVIEW_THRESHOLD: this is the last-resort "this request will not fit"
# guard, and the review threshold should normally pause first, on real reported
# usage rather than on a bytes/4 estimate.
PRESEND_CONTEXT_LIMIT = 0.95

WEB_FETCH_MAX_CHARS = 5000  # lowered from 8000 2026-08-29 -- confirmed live that a real
                            # multi-document research task (2-3 full tariff PDFs) on
                            # Unraid's 12GB card compounds across iterations past even a
                            # carefully calibrated --num-ctx ceiling (started at ~12.5K
                            # tokens with real headroom under 14336, grew past it within
                            # 3 more iterations of fetching). 5000 chars (~1250 tokens) is
                            # still a substantial excerpt; the model can always fetch again
                            # for a specific detail it's missing.
ITERATION_LOW_BUDGET_THRESHOLD = 3  # added 2026-08-29, paired with the per-turn budget note in
# the main loop below: once <= this many iterations remain, the harness appends an explicit
# user-turn reminder that request_more_iterations exists, instead of only a passive count. Kept
# separate from CONTEXT_REVIEW_THRESHOLD (a fraction) because this is a small absolute count --
# the two review gates are independent and can trip at different times.

CONTEXT_REVIEW_THRESHOLD = 0.90  # added 2026-08-28 (Penn's request, "can we do the same for
# context?" -- paired with request_more_iterations): fraction of --num-ctx at which a dispatch
# pauses for review (same clean-stop-and-resume shape as the iteration request) rather than
# risk an actual context overflow or a silently degraded response near the ceiling.
# Proactive mid-run budget nudges (Penn 2026-09-08): the 0.90 pause above is post-hoc -- by
# the time it fires the run is already walled. These earlier thresholds warn the model to
# CONVERGE while it still has room, each firing at most once per run. Kept strictly below
# CONTEXT_REVIEW_THRESHOLD so the pause always supersedes the top nudge.
CONTEXT_NUDGE_THRESHOLDS = (0.60, 0.78)
DEFAULT_SEARXNG_HOST = "http://10.0.12.41:8080"
LOG_DIR = Path.home() / "bin" / "ollama-worker-logs"
UNRAID_OLLAMA_HOSTS = ("10.0.7.143",)  # substrings matched against --host to detect "this dispatch targets Unraid"

# Host auto-selection -- added 2026-08-28 after dispatching qwen3.8:27b-q8_0
# (~30.7GB) to Unraid by relying on DEFAULT_HOST above without checking fit:
# confirmed via /api/ps that only ~9.3GB landed in the 3080's VRAM (12GB
# card) and ~21.4GB (70% of the model) spilled into system RAM, running
# mostly CPU-bound -- a 386s cold load. Two real hosts, two very different
# memory shapes: Unraid has a small dedicated GPU (fast, but VRAM-limited),
# the Mac Studio has 64GB of unified memory (no VRAM/RAM split, Metal treats
# it as one pool). Picking the host should be driven by whether the model
# actually fits the GPU, not by a single hardcoded default.
KNOWN_OLLAMA_HOSTS = {
    "unraid": {
        "url": "http://10.0.7.143:11434",
        # 3080, 12GB VRAM (confirmed 2026-08-27). Reserve ~20% for KV
        # cache/context overhead so a model that just barely fits the raw
        # VRAM figure doesn't still spill once a real context is loaded.
        "usable_bytes": int(12 * 1024**3 * 0.8),
    },
    "studio": {
        "url": "http://127.0.0.1:11434",
        # 64GB unified memory (confirmed live via `sysctl hw.memsize` on
        # pennsmacstudio). Reserve ~20GB for the OS and whatever else is
        # running rather than the full 64GB.
        "usable_bytes": 44 * 1024**3,
    },
}

# Confirmed live 2026-08-28 (measured via /api/ps: size == size_vram, zero
# spillover) -- see Agent-Dispatch-Log.md "Unraid VRAM math now fully
# mapped". A hard cap, not a suggestion: Unraid's spillover check in
# ensure_model_ready() already aborts on ANY spillover, but only AFTER a
# real warmup load -- up to several minutes wasted finding out the hard way.
# This catches it before dispatch even starts. Penn's call 2026-08-28
# ("can we code that in as a hard cap that will catch prior to dispatch?"):
# clamp down and log loudly, don't silently proceed and don't just warn.
# A model with no entry here is simply unmeasured, not assumed safe --
# ensure_model_ready()'s live post-warmup check remains the real backstop
# for anything not in this table yet.
UNRAID_SPILLOVER_EXCEPTIONS = {
    # The blanket "ANY spillover aborts" rule below exists for DENSE-model
    # spillover, which is genuinely bad (every layer touches every token, so
    # CPU-resident layers tax every forward pass). MoE models are a different
    # case: active-param sparsity means CPU-offloaded experts are only
    # touched when routed to, not every token -- confirmed live 2026-08-29,
    # gpt-oss:20b loaded at 10.96GB VRAM / 3.46GB CPU (24% spilled) and still
    # ran ~41 tok/s on a short response, competitive with several fully-
    # on-GPU dense models tested the same night. Penn's call: document real
    # per-model exceptions here rather than loosen the rule generally.
    "gpt-oss:20b": {"max_spill_frac": 0.30},
}

UNRAID_CONFIRMED_SAFE_CTX = {
    # qwen3.5:9b/qwen3:8b/llama3.1:8b lowered 14336 -> 12288 on 2026-08-29 after a
    # real, reproducible CUDA OOM (Fable's diagnosis): qwen3.5:9b at the OLD 14336
    # value succeeded on iteration 1 of a multi-turn agentic dispatch, then failed
    # identically on iteration 2's generation call, 3/3 attempts, same settings, same
    # already-resident model. 14336 was validated against a single-shot prompt size;
    # llama.cpp's CUDA compute buffers scale with actual prompt/batch size, not just
    # num_ctx, and iteration 2+ of a real agentic session carries iteration 1's full
    # output forward -- a strictly bigger prompt than whatever validated this number
    # originally, landing right at the 3080's 12GB margin. Lowered uniformly for the
    # other two same-size dense models sharing this value, not just the one that
    # actually failed -- same GPU, same failure mechanism, same risk. qwen3:14b's
    # 6144 is untouched (different size class, no observed failure at that value yet).
    "qwen3.5:9b": 12288,
    "qwen3:8b": 12288,
    "llama3.1:8b": 12288,
    "qwen3:14b": 6144,
    # gpt-oss:20b: base weights alone (13.79GB) exceed Unraid's 10.3GB
    # static usable-VRAM gate -- no --num-ctx value fixes this, it needs
    # Studio instead. Deliberately has no entry: there is no safe number to
    # clamp to, the model just doesn't belong on this host at all.
}


def clamp_unraid_ctx(host: str, model: str, num_ctx: int) -> int:
    """Hard cap: if `host` is Unraid and `model` has a confirmed-safe ceiling
    on record, clamp num_ctx down to it and log loudly. Returns the
    (possibly unchanged) num_ctx to actually use."""
    if host != KNOWN_OLLAMA_HOSTS["unraid"]["url"]:
        return num_ctx
    safe = UNRAID_CONFIRMED_SAFE_CTX.get(model)
    if safe is not None and num_ctx > safe:
        log(f"[worker] HARD CAP: {model} on Unraid requested --num-ctx {num_ctx}, but "
            f"confirmed-safe ceiling is {safe} (measured live, zero spillover) -- clamping "
            f"down instead of wasting a warmup-then-abort cycle finding this out the hard way.")
        return safe
    return num_ctx
LAN_MOUNT_ROOT = "/Volumes/data"  # Unraid's SMB share, mounted here when available
COPY_HELPER = str(Path.home() / "bin" / "copy-ollama-model-from-unraid.py")

# Local hot cache: OLLAMA_MODELS normally points here (fast NVMe). The
# shared SMB store (mounted for both this Mac and Unraid) is the source of
# truth / cold storage -- confirmed live 2026-08-21 that Ollama's own
# model-load path over SMB hangs indefinitely regardless of mmap setting,
# while a plain file copy from the same share does not, so copying once
# and loading locally is the actual fix, not a network/client tuning one.
# Ollama's REAL model store on this Mac. Was `~/ollama-models-local` until
# 2026-08-22, which nothing ever read: OLLAMA_MODELS is unset in the running
# `ollama serve` process, so Ollama loads from its default `~/.ollama/models`.
#
# The consequence was a double SMB transfer for every model not already local:
# ensure_model_cached() copied blobs+manifest from the share into
# ~/ollama-models-local (invisible to Ollama), ensure_model_ready() then found
# the model still missing from /api/tags and ran copy-ollama-model-from-unraid.py,
# which copies from the SAME share again into ~/.ollama/models (its LOCAL_ROOT).
# That second copy is the one that ever worked; the first just accumulated --
# 83GB of it, across 19 blobs, none of them unique.
#
# Pointing this at the real store makes the first copy the only copy: blobs and
# manifest land where Ollama reads, so the model is visible immediately and the
# helper's per-blob "SKIP (already at destination)" makes the second pass free.
LOCAL_MODEL_CACHE = Path.home() / ".ollama" / "models"
SMB_MODEL_SOURCE = Path("/Volumes/data/ollama-models")
MODEL_PULL_LOG = Path.home() / "bin" / "ollama-model-pulls.log"
OBSIDIAN_URL = "http://10.0.6.230:27123"
OBSIDIAN_TOKEN_FILE = Path.home() / ".config" / "ollama-worker" / "obsidian-token"
# Env var first (if a caller's shell happens to have it), else the local
# file -- launchctl setenv only affects processes launched AFTER the
# setenv call, which proved unreliable for background-dispatched runs
# from an already-running shell, so the file is the primary path.
OBSIDIAN_TOKEN = os.environ.get("OBSIDIAN_TOKEN") or (
    OBSIDIAN_TOKEN_FILE.read_text().strip() if OBSIDIAN_TOKEN_FILE.exists() else ""
)
OBSIDIAN_DISPATCH_LOG_PATH = "Claude/Ollama/Ollama-Dispatch-Log.md"

SYSTEM_PROMPT = """You are a focused coding agent. You have ten tools: \
list_files, read_file, write_file, edit_file, run_bash, web_search, \
web_fetch, request_diagnostics, request_more_iterations, and task_complete. \
When you need evidence (recent commits, a diff, a grep, a file window, the \
verify output, container logs), call request_diagnostics with a list of \
requests before reaching for run_bash -- it is bounded and read-only, so it \
cannot damage the tree. Use them to \
accomplish the task directly -- don't describe what you would do, actually \
call the tools.

You are running under a finite iteration budget, and the harness keeps \
you informed of it: after each iteration's tool results it appends a \
short note of the form "[Iteration i/N: R remaining in your budget.]", \
and when few iterations remain it says so explicitly. Track your own \
progress against that budget as you go -- if you can see concrete \
remaining work that will not fit in the iterations left, call \
request_more_iterations now with a specific reason and how many more \
you need. Use exactly the standard its description sets: only for real \
remaining work you can point to -- never as a routine check-in, and \
never to recover from being stuck (fix the actual problem instead). It \
pauses the session for review rather than granting anything immediately; \
if the task is actually done, call task_complete.

ALWAYS start by calling list_files on '.' to see the project's real \
structure, then read the files that matter, BEFORE writing anything. Never \
guess a file path, and never assume a framework or directory layout -- \
discover it. The project's existing stack and conventions are whatever \
list_files and read_file actually show you, not what a project like this \
usually looks like. Prefer edit_file over write_file when changing an \
existing file.

If AGENTS.md, CLAUDE.md, README.md or CONTRIBUTING.md exist at the top \
level, read them before writing any code. They are written for you and \
state this project's conventions, which may deliberately differ from what \
you learned in training. Follow them over your own habits.

Before you CREATE a new file, read an existing file of the same kind in \
this project and match its idiom exactly -- its imports, its export style, \
its function signatures, its naming. A framework often has several valid \
styles from different versions; the only one that works here is the one \
already in use. If you are adding an API route, read an existing API route \
first. If you are adding a module or target, read how existing ones are \
declared. Your training data is likely to be older than this project.

web_search/web_fetch are for external documentation only (an unfamiliar \
third-party API). Never use them to learn about the project in front of \
you -- that is what list_files and read_file are for.

If you import or require a package, make sure it is actually a dependency \
of this project first -- check package.json (or the equivalent manifest) \
with read_file, and if it is missing either install it with run_bash or \
use something already available. Code that imports a package the project \
does not have will fail to build.

If a path you tried does not exist, do not try it again. Call list_files \
to find where the file actually is, and only use paths you have seen in a \
list_files result.

When the task is fully complete, call task_complete with a short summary. \
(A plain-text response with no further tool calls also ends the session, \
but task_complete is preferred -- it lets the harness confirm your work \
against the task's verification command before accepting it, instead of \
finding out only afterward.) Keep file paths relative to the working \
directory. Do not ask clarifying questions; make reasonable assumptions \
and proceed."""

RESEARCH_SYSTEM_PROMPT = """You are a focused research agent. You have two \
tools that matter for this task: web_search (self-hosted SearXNG) and \
web_fetch (reads the full content of a specific URL, including PDFs). You \
also have list_files/read_file/write_file/edit_file/run_bash available, but \
this is a research task, not a coding task -- there is no project to \
discover and nothing to build. Do not call list_files or read_file out of \
habit; there is nothing there to find unless the task itself gives you a \
reason to look.

A web_search result is a short snippet only -- a title, URL, and one or two \
truncated sentences. It is NEVER enough on its own to state a specific \
number, date, name, or any other concrete fact. For every specific claim in \
your final answer, you must have actually called web_fetch on a real source \
and read its content. If you catch yourself about to state a specific \
detail you only saw in a snippet, not in fetched page content, stop and \
fetch the source first, or say plainly in your final answer that you \
couldn't confirm it. A wrong-but-confident answer is worse than an honest \
"I couldn't verify this."

Use web_search multiple times with different, more specific queries if your \
first search doesn't turn up enough -- don't stop after one search just \
because it returned something.

When you have enough to answer the question, respond with your final answer \
as plain text and no further tool calls -- do not write it to a file unless \
the task explicitly asked for a file. Do not ask clarifying questions; make \
reasonable judgment calls about scope and proceed."""

# Added 2026-08-28 after confirming live (qwen3.5:9b) that reusing the
# coding SYSTEM_PROMPT for a research dispatch actively causes harm, not
# just irrelevance: "You are a focused coding agent... ALWAYS start by
# calling list_files on '.'" primed the model, on an EMPTY research scratch
# directory, to hallucinate an entire unrelated coding task (it wrote a
# Flask web server nobody asked for) rather than doing the research the
# task actually described. The whole rest of that prompt (matching existing
# code idiom, reading AGENTS.md/package.json, "Prefer edit_file over
# write_file") is equally irrelevant noise for a task with no project to
# read. This is a separate, more foundational fix than the corrective-nudge
# task_kind fix above -- that one only affects behavior once a no-tool-call
# response happens; this one shapes the model's understanding of what kind
# of task it's even doing from the very first token.

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "list_files",
            "description": "List the files and subdirectories at a path. Use this FIRST to discover the project's structure before reading or writing anything. Pass '.' for the working directory.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Directory path relative to the working directory. Use '.' for the working directory itself."},
                },
                "required": ["path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "read_file",
            # The SCHEMA is the fix, not just the implementation. This tool
            # previously declared `path` only and described itself as "Read a
            # file's full contents", while a model -- reasoning from other
            # harnesses -- called it with offset/length anyway. Those arguments
            # were silently dropped and the whole file came back: a 275KB read
            # against a 131K window, which paused a real dispatch at
            # 129,692/131,072 tokens on iteration 5. Describing the paging here
            # is what stops the prompt teaching the old behaviour.
            "description": (
                "Read a file. Large files come back in PAGES, not in full. "
                "Call with just `path` to get the first page; if the file is "
                "bigger than one page the result ends with a notice giving the "
                "total line count and the exact next call to make. Pass "
                "`offset` (1-based line number to start at) and `length` "
                "(how many lines) to read a specific range -- that is how you "
                "reach the rest of a large file, and how you re-read around a "
                "specific region."),
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Path relative to the working directory."},
                    "offset": {"type": "integer",
                               "description": "1-based line number to start reading at. Omit to start at line 1."},
                    "length": {"type": "integer",
                               "description": "How many lines to read. Omit to read to the end of the file (still capped to one page)."},
                },
                "required": ["path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "write_file",
            "description": "Create a NEW file, or fully overwrite an existing one, with the given content. Creates parent directories as needed. For an existing file where you're only changing part of it, use edit_file instead -- write_file forces you to regenerate the entire file from scratch in one response, which is slow and error-prone for anything but a small or brand-new file.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Path relative to the working directory."},
                    "content": {"type": "string", "description": "Full file content to write."},
                },
                "required": ["path", "content"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "edit_file",
            "description": "Replace one exact occurrence of old_string with new_string in an existing file. Use this instead of write_file for any change to a file you didn't just create -- it only requires you to output the small changed region, not the whole file. old_string must match the file's current content exactly (including whitespace/indentation) and must be unique in the file; include enough surrounding context (a few lines before/after) to make it unique if the change itself is a short/common line.",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Path relative to the working directory."},
                    "old_string": {"type": "string", "description": "Exact existing text to replace, unique within the file."},
                    "new_string": {"type": "string", "description": "Text to replace it with."},
                },
                "required": ["path", "old_string", "new_string"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_bash",
            "description": "Run a shell command in the working directory and return stdout, stderr, and exit code.",
            "parameters": {
                "type": "object",
                "properties": {
                    "command": {"type": "string", "description": "Shell command to run."},
                },
                "required": ["command"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "web_search",
            "description": "Search the web (self-hosted SearXNG). Returns the top results as title/url/snippet.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query."},
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "web_fetch",
            "description": "Fetch a URL and return its main readable text content (HTML stripped of nav/ads/scripts). Truncated if very long.",
            "parameters": {
                "type": "object",
                "properties": {
                    "url": {"type": "string", "description": "Full URL to fetch."},
                },
                "required": ["url"],
            },
        },
    },
    {
        # "get me these logs" loop (2026-09-03). Read-only, allowlisted,
        # capped, redacted -- executor lives in dispatch-diagnostics.py so it
        # can be tested and reasoned about without the worker. NOT a shell:
        # run_bash stays the (audited) escape hatch; this is the bounded path
        # a model should reach for first when it needs evidence.
        "type": "function",
        "function": {
            "name": "request_diagnostics",
            "description": (
                "Fetch read-only evidence to diagnose the task: git history/diff/blame, a line window "
                "of a file, a directory listing, a grep, this job's own verify output, or bounded "
                "`docker logs` from a catalogued container. Pass a LIST of requests (max 5 per call, "
                "6 calls per run). Kinds: git {args:[...]} (read subcommands only), file {path,start,end}, "
                "ls {path,depth}, grep {pattern,glob,regex}, verify {}, docker_logs {container,tail,since,filter}. "
                "Output is capped and secrets are redacted. Refusals come back with the reason -- read it, "
                "do not retry the same request."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "requests": {
                        "type": "array",
                        "description": "Request objects, each {kind: ..., ...that kind's fields}.",
                        "items": {"type": "object"},
                    },
                    "why": {"type": "string", "description": "One line: what this evidence will settle."},
                },
                "required": ["requests"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "request_more_iterations",
            "description": "Ask for more iterations if you're making real progress but won't "
                            "finish within your current budget. Only call this when you can "
                            "point to concrete remaining work -- not as a routine check-in, and "
                            "not to recover from being stuck (fix the actual problem instead). "
                            "This pauses the session for review rather than granting anything "
                            "immediately -- you will not get a response to this call.",
            "parameters": {
                "type": "object",
                "properties": {
                    "additional": {"type": "integer",
                                   "description": "How many more iterations you're asking for."},
                    "reason": {"type": "string",
                               "description": "What concrete remaining work justifies this -- "
                                              "be specific."},
                },
                "required": ["additional", "reason"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "task_complete",
            "description": "Call this ONCE, when the task is fully complete, instead of just "
                            "stopping. Pass a short summary of what you did. If the task has a "
                            "verification command, the harness runs it before accepting: if it "
                            "fails, you get its output back as this tool's result and must fix "
                            "the reported problem(s) before calling task_complete again. Do not "
                            "call this if you know work remains -- and do not run the "
                            "verification command yourself, the harness already does.",
            "parameters": {
                "type": "object",
                "properties": {
                    "summary": {"type": "string",
                                "description": "What was done, which files changed, and how "
                                               "you know it works."},
                },
                "required": ["summary"],
            },
        },
    },
]


def render_manual_tools_block(tools: list) -> str:
    """Textual tool-schema injection for models whose Ollama chat template
    doesn't render the native `tools` API field into the prompt at all --
    confirmed 2026-08-21 for deepseek-r1 distills (incl. the community
    'MFDoom/deepseek-r1-tool-calling' build, which ships the exact same
    template gap): rendering the real Jinja template locally with `tools`
    populated proved it never appears in the output. Root cause per an
    Ollama maintainer (github.com/ollama/ollama/issues/8517): these distills
    don't emit the exact special-token sequence Ollama's native tool_calls
    parser requires. This sidesteps that parser entirely by describing the
    tools as plain text (Qwen/DeepSeek's own convention) and having
    extract_manual_tool_call() below parse the model's response ourselves."""
    lines = [json.dumps({"type": "function", "function": t.get("function", t)}) for t in tools]
    tools_xml = "\n".join(lines)
    valid_names = ", ".join(t.get("function", t)["name"] for t in tools)
    return f"""# Tools

You may call one function per response to assist with the user query. You are provided with function signatures within <tools></tools> XML tags:
<tools>
{tools_xml}
</tools>

For each function call, return ONLY a JSON object with function name and arguments within <tool_call></tool_call> XML tags -- nothing else, no other text:
<tool_call>
{{"name": <function-name>, "arguments": <args-json-object>}}
</tool_call>

CRITICAL: the "name" field must be EXACTLY one of these literal tool names:
  {valid_names}
Never put a file path, a filename, or anything else in "name" -- file paths always go inside "arguments".
Correct:   {{"name": "read_file", "arguments": {{"path": "app/page.tsx"}}}}
INCORRECT: {{"name": "app/page.tsx", "arguments": {{"path": "app"}}}}

When the task is fully done and no more function calls are needed, respond with a normal plain-text message and NOT a <tool_call> block."""


_VALID_JSON_ESCAPE_CHARS = set('"\\/bfnrtu')


def _repair_invalid_json_escapes(s: str) -> str:
    """Char-by-char (not regex-substitution) repair of a JSON-string
    candidate containing backslashes that aren't valid JSON escapes --
    e.g. a model copying a shell `find ... \\( -o ... \\)` snippet into a
    tool-call argument without doubling those backslashes for JSON.
    Confirmed live 2026-08-21 (qwen2.5-coder:14b) that a naive single-char
    lookahead regex (`re.sub(r'\\\\(?!...)', ...)`) double-counts an
    ALREADY-valid two-character escape like `\\.` (backslash-backslash
    then a literal dot) -- it inspects the second backslash of that valid
    pair in isolation, sees the dot after it, and wrongly "repairs" a
    correct escape. Walking left-to-right and consuming valid pairs whole
    avoids that: only a backslash NOT already paired with a valid escape
    char gets doubled, and the walk advances 2 chars over anything it
    correctly recognized as already-valid."""
    out = []
    i = 0
    n = len(s)
    while i < n:
        c = s[i]
        if c == "\\" and i + 1 < n and s[i + 1] in _VALID_JSON_ESCAPE_CHARS:
            out.append(c)
            out.append(s[i + 1])
            i += 2
            continue
        if c == "\\":
            out.append("\\\\")
            i += 1
            continue
        out.append(c)
        i += 1
    return "".join(out)


_NUMERIC_CLAIM_RE = re.compile(
    r'\$\s?\d+(?:\.\d+)?\s*(?:/\s?k?Wh|per\s+k?Wh|/\s?kW\b)?'
    r'|\b\d{1,2}(?::\d{2})?\s?(?:AM|PM|am|pm)\b'
)


def find_ungrounded_numeric_claims(answer_text: str, source_text: str) -> list:
    """Extract dollar-amount and clock-time claims from a research answer and return
    any that don't appear (whitespace-normalized) in the source facts text.

    Confirmed live 2026-08-28 (qwen3:8b, NV-Energy facts-provided pass-2 run): a model
    can invent precise, confident-sounding rate figures ($0.25/kWh, specific hour
    ranges) with zero basis in the facts it was actually given, directly against an
    explicit "do not fabricate a number" instruction in the task text -- and nothing
    caught it, because the old fabrication nudge only checked whether a web_fetch had
    succeeded THIS session, and that check is disabled entirely in facts-provided mode
    (see run_task's facts_provided param) since zero fetches is the whole point there.
    This isn't a general fact-checker -- it only catches the two claim shapes that
    have shown up as confident fabrications in practice, and it will also flag a
    correctly-cited number if the source phrases it differently enough to fail a
    normalized substring match. Both are fine: it exists to force a second look before
    a model's answer is accepted, not to silently pass or fail it."""
    def norm(s):
        return re.sub(r'\s+', ' ', s).strip()
    source_norm = norm(source_text)
    claims = sorted(set(norm(m.group(0)) for m in _NUMERIC_CLAIM_RE.finditer(answer_text)))
    return [c for c in claims if c not in source_norm]


# Real tool names, used to validate the loosest tool-call dialect (the flat
# {"tool": NAME, ...} form) so a prose JSON object that merely carries a "tool"
# key is never mistaken for a call.
_VALID_TOOL_NAMES = {t["function"]["name"] for t in TOOLS}


def _coerce_tool_call(obj):
    """Normalize one candidate JSON object into {"name":..., "arguments":{...}}
    if it is recognizably a tool call, else return None.

    Priority order:
      1. explicit {"name": NAME, "arguments"|"parameters": {...}} -- accepted for
         ANY name (unchanged behaviour: an unknown name is surfaced by the loop).
      2. flat {"tool": NAME, <arg-key>: <val>, ...} -- the devstral/Mistral fenced
         ```json dialect where the tool name is under "tool" and the arguments are
         sibling keys (NOT nested). Confirmed live 2026-09-09 (devstral:24b,
         transcript 20260909T021925Z.json). VALIDATED against real tool names to
         avoid prose false positives.
      3. neither -> None (a {"function": {...}} / {"tool_call": {...}} wrapper,
         which the caller rescans INTO)."""
    if not isinstance(obj, dict):
        return None

    def _as_dict(a):
        if isinstance(a, str):
            try:
                a = json.loads(a)
            except Exception:
                return {}
        return a if isinstance(a, dict) else {}

    if isinstance(obj.get("name"), str) and ("arguments" in obj or "parameters" in obj):
        args = obj["arguments"] if "arguments" in obj else obj["parameters"]
        return {"name": obj["name"], "arguments": _as_dict(args)}

    tname = obj.get("tool")
    if not isinstance(tname, str):
        tname = obj.get("tool_name")
    if isinstance(tname, str) and tname.strip() in _VALID_TOOL_NAMES:
        if "arguments" in obj:
            args = _as_dict(obj["arguments"])
        elif "parameters" in obj:
            args = _as_dict(obj["parameters"])
        else:
            args = {k: v for k, v in obj.items() if k not in ("tool", "tool_name")}
        return {"name": tname.strip(), "arguments": args}

    return None


def _extract_tool_calls_token(content: str, calls: list) -> str:
    """Consume Mistral/devstral `[TOOL_CALLS]` payloads: the literal token
    followed by a JSON array (or single object) of calls. Appends recognized
    calls to `calls` and returns `content` with each consumed span blanked out
    (spaces of equal length) so the brace scanner below can't double-count."""
    if "[TOOL_CALLS]" not in content:
        return content
    out = content
    search_from = 0
    while True:
        idx = out.find("[TOOL_CALLS]", search_from)
        if idx < 0:
            break
        rest_start = idx + len("[TOOL_CALLS]")
        rest = out[rest_start:]
        stripped = rest.lstrip()
        lead = len(rest) - len(stripped)
        try:
            payload, consumed = json.JSONDecoder().raw_decode(stripped)
        except Exception:
            search_from = rest_start
            continue
        items = payload if isinstance(payload, list) else [payload]
        for it in items:
            c = _coerce_tool_call(it)
            if c is not None:
                calls.append(c)
        payload_end = rest_start + lead + consumed
        out = out[:idx] + (" " * (payload_end - idx)) + out[payload_end:]
        search_from = payload_end
    return out


def extract_manual_tool_calls(content: str) -> tuple:
    """Find every {"name": ..., "arguments": ...} object in plain-text model
    output. Tag-agnostic on purpose -- confirmed live 2026-08-21 that
    deepseek-r1:14b is inconsistent about wrapping this in <tool_call>,
    <tools>, or no tag at all, but the JSON object itself came back
    correctly-schema'd (right field names) in every real test, unlike
    native tool_calls mode which hallucinated wrong field names.

    Returns a LIST, not just the first match -- confirmed live 2026-08-21
    that despite the system prompt saying "one function per response", the
    model sometimes emits several call objects back-to-back in a single
    response (e.g. write two files then run a command). Silently taking
    only the first one drops real, correctly-formed work the model already
    did -- confirmed as a real bug this way (a second file's write_file
    call was dropped, breaking the task) before this was made to scan the
    rest of the string instead of stopping at the first match.

    Returns (calls, cleaned_content) as of 2026-08-29 (root-caused by Fable,
    relayed via github-projects-bf): qwen3-coder intermittently emits a
    well-formed <function=NAME><parameter=KEY>VALUE</parameter></function>
    block with a DANGLING </tool_call> and no matching opener -- confirmed
    live in .../ollama-worker-logs/20260829T165416Z.json. Ollama's native
    parser keys on the opening tag to enter tool-parsing mode, so the whole
    call passes through as plain content and tool_calls comes back empty.
    Worse, the malformed assistant message goes back into context verbatim,
    and the model imitates its own prior formatting on the next turn --
    one slip becomes a dead job. cleaned_content has any successfully-
    salvaged XML call (and its wrapper tags, present or not) stripped, so
    the CALLER can overwrite the stored message and break that lock-in --
    see the call site's own comment for why mutating msg["content"] in
    place is sufficient (messages.append(msg) already holds a reference to
    the same dict, not a copy)."""
    # Pre-pass: normalize Python-style triple-quoted values into real JSON
    # strings. Confirmed live 2026-08-22 (deepseek-r1:32b, clamshell task):
    # the model emitted
    #     {"name": "write_file", "arguments": {"path": "Sources/Clamshell/
    #      ConfirmationBridge.swift", "content": """// swiftlint:disable all
    # -- a correct call, at a correct path, with `"""` where JSON needs `"`.
    # json.loads rejects it, the call was discarded, and the run scored
    # files=0 as though the model had done nothing. This must run BEFORE the
    # brace scan below: `"""` opens-then-closes-then-reopens a string as far
    # as the scanner is concerned, so the span would be found wrong too.
    #
    # Anchored on the closing `"""` being followed by the object's closing
    # braces, so a legitimate `"""` INSIDE the content (a Python docstring)
    # doesn't terminate the match early and truncate the file being written.
    content = re.sub(
        r'(:\s*)"""(.*?)"""(?=\s*\}\s*\})',
        lambda m: m.group(1) + json.dumps(m.group(2)),
        content,
        flags=re.DOTALL,
    )

    calls = []
    # Mistral/devstral [TOOL_CALLS] payloads first -- consumed and blanked out so
    # the brace scan below can't re-find the same objects (see helper).
    content = _extract_tool_calls_token(content, calls)
    pos = 0
    while True:
        # Match either key order. Confirmed empirically that a model emitting
        # {"arguments": {...}, "name": "..."} was dropped entirely by the
        # name-then-arguments-only pattern -- it scores 0 while calling tools
        # correctly, which is the exact failure class this session kept
        # mistaking for model incompetence.
        # Confirmed live 2026-08-28 (llama3.1:8b, NV-Energy model bake-off): a model
        # can emit a correctly-shaped call using "parameters" instead of "arguments"
        # as the payload key (mirroring the tool DEFINITION schema's key name rather
        # than the call schema's) -- the old arguments-only regex never matched at
        # all, so this call was invisible to the fallback and read as "no tool call
        # yet", nudging the model away from what was actually a real, usable call.
        # Third alt added 2026-09-09: the flat {"tool": NAME, ...} dialect (args
        # as sibling keys, not nested) -- devstral emitted this in ```json fences
        # and every call was invisible to the old two-alt pattern.
        m = re.search(
            r'\{.*?"name"\s*:.*?"(?:arguments|parameters)"\s*:'
            r'|\{.*?"(?:arguments|parameters)"\s*:.*?"name"\s*:'
            r'|\{[^{}]*?"(?:tool|tool_name)"\s*:',
            content[pos:], re.DOTALL)
        if not m:
            break
        start = pos + m.start()
        depth = 0
        end = None
        # String-aware brace matching. A naive depth counter also counts
        # braces that appear INSIDE a JSON string literal -- i.e. inside
        # the file content a model is writing -- so any unbalanced brace
        # in generated code (a `}` in a comment, a truncated snippet)
        # ends the candidate at the wrong offset and the whole tool call
        # is silently dropped. Skip over string literals entirely.
        in_str = False
        esc = False
        for i in range(start, len(content)):
            c = content[i]
            if in_str:
                if esc:
                    esc = False
                elif c == '\\':
                    esc = True
                elif c == '"':
                    in_str = False
                continue
            if c == '"':
                in_str = True
            elif c == '{':
                depth += 1
            elif c == '}':
                depth -= 1
                if depth == 0:
                    end = i
                    break
        if end is None:
            break
        candidate = content[start:end + 1]
        try:
            parsed = json.loads(candidate)
        except Exception:
            # Confirmed live 2026-08-21 (qwen2.5-coder:14b, model-bakeoff run):
            # models embedding a shell command inside the "arguments" string
            # sometimes copy shell-escape backslashes (e.g. `\(`, `\)` from a
            # `find ... \( -o ... \)` snippet) verbatim without doubling them,
            # producing a backslash followed by a character JSON doesn't
            # recognize as an escape (strict json.loads raises "Invalid
            # \escape" on this, unlike some lenient parsers). Repair by
            # doubling any backslash not already followed by a valid JSON
            # escape char, then retry once before giving up on this
            # candidate -- same spirit as _fix_literal_escapes above, but
            # for the opposite direction (under- rather than over-escaped).
            #
            # Second repair, confirmed live 2026-08-22 (deepseek-r1:32b,
            # clamshell ConfirmationBridge task): a model can escape its
            # quotes correctly (\"Clamshell\") while emitting REAL newline
            # characters inside the "content" string rather than \n. JSON
            # forbids raw control characters in string literals, so
            # json.loads raises and the tool call is thrown away -- the
            # worker then treats a write_file attempt as a final text
            # answer, stops, and `swift build` passes on an untouched tree
            # (exit=0, files_changed=0), which reads as a clean pass. Try
            # each repair alone and then combined before giving up.
            parsed = None
            for repair in (
                _repair_invalid_json_escapes,
                _escape_raw_control_chars,
                lambda s: _escape_raw_control_chars(_repair_invalid_json_escapes(s)),
            ):
                try:
                    parsed = json.loads(repair(candidate))
                    break
                except Exception:
                    continue
            if parsed is None:
                pos = end + 1
                continue
        coerced = _coerce_tool_call(parsed)
        if coerced is not None:
            calls.append(coerced)
            pos = end + 1
        else:
            # The balanced object parsed but isn't a tool call itself -- it's a
            # WRAPPER around one: {"tool_call": {...}}, {"function": {...}}.
            # Skipping to end+1 would step over the real call nested inside and
            # return nothing. Rescan from just inside this object instead, so
            # the inner call is found on the next pass.
            pos = start + 1

    # qwen3-coder XML salvage (2026-08-29, see this function's own docstring).
    # Matches the function block with an OPTIONAL <tool_call> before and an
    # OPTIONAL </tool_call> after, in one span -- covers properly-wrapped,
    # dangling-closer-only, and bare-with-no-wrapper-at-all uniformly, so the
    # whole thing (wrapper artifacts included) comes out in one removable match
    # rather than needing a second pass to clean up stray tags afterward.
    _valid_tool_names = {t["function"]["name"] for t in TOOLS}
    _xml_call_re = re.compile(
        r'(?:<tool_call>\s*)?<function=([\w_]+)>(.*?)</function>(?:\s*</tool_call>)?',
        re.DOTALL)
    _param_re = re.compile(r'<parameter=([\w_]+)>(.*?)</parameter>', re.DOTALL)
    cleaned_content = content
    for m in _xml_call_re.finditer(content):
        name = m.group(1)
        if name not in _valid_tool_names:
            # Doesn't validate -- almost certainly the model legitimately
            # quoting/discussing the XML syntax itself (e.g. explaining the
            # tool format), not an actual malformed call. Leave it alone
            # entirely: don't add a call, don't touch cleaned_content for
            # this span, so real prose that happens to look like this isn't
            # silently mangled.
            continue
        args = {}
        for pm in _param_re.finditer(m.group(2)):
            key, value = pm.group(1), pm.group(2)
            # Trim exactly ONE newline immediately adjacent to the tags, not
            # all whitespace -- a value can legitimately start or end with
            # meaningful blank lines (e.g. file content), only the formatting
            # newline the model puts right after <parameter=KEY>\n and right
            # before \n</parameter> is an artifact of the tag layout itself.
            if value.startswith("\n"):
                value = value[1:]
            if value.endswith("\n"):
                value = value[:-1]
            if key == "additional":
                # The one integer-typed param across every tool schema
                # (request_more_iterations) -- everything else is a string.
                try:
                    value = int(value.strip())
                except ValueError:
                    pass  # leave as string; the tool impl's own validation catches it
            args[key] = value
        calls.append({"name": name, "arguments": args})
        cleaned_content = cleaned_content.replace(m.group(0), "", 1)
    return calls, cleaned_content.strip()


def _escape_raw_control_chars(s: str) -> str:
    """Escape literal newline/CR/tab characters appearing INSIDE a JSON
    string literal, leaving structural whitespace between tokens alone.
    JSON forbids raw control characters inside strings; some models emit
    them anyway when writing multi-line file content. Tracks string state
    so it never touches the JSON's own formatting."""
    out = []
    in_str = False
    esc = False
    for ch in s:
        if not in_str:
            out.append(ch)
            if ch == '"':
                in_str = True
            continue
        if esc:
            out.append(ch)
            esc = False
            continue
        if ch == '\\':
            out.append(ch)
            esc = True
            continue
        if ch == '"':
            out.append(ch)
            in_str = False
            continue
        out.append({'\n': '\\n', '\r': '\\r', '\t': '\\t'}.get(ch, ch))
    return ''.join(out)


def log(msg):
    print(msg, flush=True)


def resolve_path(cwd: Path, path: str) -> Path:
    p = (cwd / path).resolve()
    if cwd.resolve() not in p.parents and p != cwd.resolve():
        raise ValueError(f"path escapes working directory: {path}")
    return p


def tool_list_files(cwd: Path, args: dict) -> str:
    """List a directory. Added 2026-08-22 after probing showed BOTH 7B
    models reach for a listing tool that did not exist: qwen2.5-coder:7b
    called read_file({"path": "."}) (reading a directory as a file), and
    deepseek-r1:7b invented a `list_files` call outright and then
    hallucinated the directory contents. Without this, a small model's
    only route to discovery was guessing filenames -- which is exactly
    what produced the fabricated Flask/MERN stacks in the bake-off."""
    p = resolve_path(cwd, args.get("path") or ".")
    if not p.exists():
        return f"ERROR: path not found: {args.get('path')}"
    if not p.is_dir():
        return f"ERROR: not a directory: {args.get('path')} (use read_file for files)"
    skip = {".git", "node_modules", ".next", ".build", "__pycache__", ".venv"}
    entries = []
    try:
        for child in sorted(p.iterdir(), key=lambda c: (not c.is_dir(), c.name)):
            if child.name in skip:
                entries.append(f"{child.name}/  (skipped: large/generated)")
            elif child.is_dir():
                entries.append(f"{child.name}/")
            else:
                entries.append(f"{child.name}  ({child.stat().st_size} bytes)")
    except Exception as e:
        return f"ERROR listing directory: {e}"
    if not entries:
        return "(empty directory)"
    return "\n".join(entries)


def tool_read_file(cwd: Path, args: dict, max_chars: int = None,
                   num_ctx: int = None) -> str:
    # Confirmed live 2026-08-28 (llama3.1:8b, request_more_iterations test dispatch): a
    # missing required argument on any of read_file/write_file/edit_file raised a bare
    # KeyError via direct dict indexing, stringified as just the key name (e.g. "'content'")
    # with zero explanation -- same failure class already fixed for web_fetch's "url". Applying
    # the same explicit-check-with-clear-message pattern to all three file tools here.
    if "path" not in args:
        return 'ERROR: read_file requires a "path" argument.'
    p = resolve_path(cwd, args["path"])
    if not p.exists():
        return f"ERROR: file not found: {args['path']} -- use list_files to see what exists."
    if p.is_dir():
        # Do not dead-end here: a bare "Is a directory" errno was what the
        # models hit before list_files existed, with no hint what to do next.
        return (f"ERROR: {args['path']} is a directory, not a file. "
                f"Listing it instead:\n" + tool_list_files(cwd, {"path": args["path"]}))
    try:
        text = p.read_text()
    except Exception as e:
        return f"ERROR reading file: {e}"
    return _paginate_read(text, args, args["path"], max_chars=max_chars, num_ctx=num_ctx)


def _read_file_cap(max_chars: int | None, num_ctx: int | None) -> int:
    """Whichever budget is SMALLER: the flag/default, or ~1/8 of the window.

    A fixed 16000-char page is ~4K tokens, which is fine at 64K but is a
    quarter of a 16K window. The clamp keeps one read from dominating a small
    dispatch's context, which is the failure this whole change exists to stop.
    """
    cap = max_chars if max_chars is not None else READ_FILE_MAX_CHARS
    if num_ctx:
        cap = min(cap, max(2000, num_ctx * 4 // 8))
    return cap


def _paginate_read(text: str, args: dict, shown_path: str,
                   max_chars: int | None = None, num_ctx: int | None = None) -> str:
    """Return one page of `text`, with a notice saying how to get the next.

    LINES for the range, CHARS for the cap -- deliberately mixed. The model that
    triggered this natively emitted line-based offset/length, so the parameters
    speak lines; the cap protects the context window, which is a size question,
    so it counts chars.

    NEVER REFUSES. A hard "file too large, use offset" dead-ends weaker models
    and burns an iteration to learn something the first page could have told
    them -- and the first page is genuinely useful. Cap and explain instead.

    NO LINE-NUMBER PREFIXES ON BODY LINES. Models paste read_file output
    straight into edit_file's old_string, and a "  312| " prefix would poison
    the exact match -- turning a read improvement into an edit regression. All
    range information lives in the header and footer only.
    """
    def _int(v):
        try:
            return int(str(v).strip())
        except Exception:
            return None

    lines = text.splitlines(keepends=True)
    total_lines, total_bytes = len(lines), len(text)
    cap = _read_file_cap(max_chars, num_ctx)

    off = _int(args.get("offset"))
    ln = _int(args.get("length"))
    start = max(1, off) if off else 1
    if total_lines and start > total_lines:
        return (f"ERROR: offset {start} is past the end of {shown_path} -- the file has "
                f"{total_lines} lines. Call read_file with "
                f'{{"path":"{shown_path}","offset":1}} to start over.')
    end_excl = (start - 1 + ln) if (ln and ln > 0) else total_lines
    window = lines[start - 1:end_excl]

    # Trim to WHOLE lines within the char cap: a page cut mid-line is a page a
    # model cannot safely copy into edit_file.
    kept, used, cut_by_cap = [], 0, False
    for line in window:
        if used + len(line) > cap and kept:
            cut_by_cap = True
            break
        kept.append(line); used += len(line)
    body = "".join(kept)
    last = start - 1 + len(kept)
    complete = (start == 1 and last >= total_lines)
    if complete:
        return body          # small file, unchanged behaviour -- no notice noise

    nxt = last + 1
    header = (f"[read_file {shown_path}: lines {start}-{last} of {total_lines} "
              f"({total_bytes} bytes total){', page truncated at ' + str(cap) + ' chars' if cut_by_cap else ''}]\n")
    if nxt > total_lines:
        footer = (f"\n[end of file -- lines {start}-{last} of {total_lines} shown, "
                  f"this is the last page]")
    else:
        # The LITERAL next call, with the real numbers already filled in. A
        # notice that only says "use offset" makes the model do arithmetic it
        # gets wrong; this one can be copied verbatim.
        suggest = max(1, len(kept)) if kept else 300
        footer = (f"\n[{total_lines - last} more line(s) not shown. To continue, call "
                  f'read_file with {{"path":"{shown_path}","offset":{nxt},"length":{suggest}}}]')
    return header + body + footer


def _fix_literal_escapes(content: str) -> str:
    """Some models (confirmed: devstral:24b) emit write_file content with
    literal two-character `\\n`/`\\t` sequences instead of real newline/tab
    bytes -- a generation quirk, not a JSON-parsing bug (Ollama's own
    tool_calls arguments field already decodes cleanly; the model just
    wrote backslash-n as text). Heuristic: only fix this when the content
    has ZERO real newlines but at least one literal `\\n` -- a file that
    already has real newlines mixed with a few genuinely-intended literal
    backslash sequences (e.g. a regex) is left alone, to avoid mangling
    correct output from models that don't have this quirk."""
    if "\n" not in content and "\\n" in content:
        content = (content.replace("\\r\\n", "\n").replace("\\n", "\n")
                          .replace("\\t", "\t").replace('\\"', '"'))
    return content


def tool_write_file(cwd: Path, args: dict) -> str:
    if "path" not in args:
        return 'ERROR: write_file requires a "path" argument.'
    if "content" not in args:
        return 'ERROR: write_file requires a "content" argument.'
    p = resolve_path(cwd, args["path"])
    p.parent.mkdir(parents=True, exist_ok=True)
    content = _fix_literal_escapes(args["content"])
    p.write_text(content)
    return f"OK: wrote {len(content)} bytes to {args['path']}"


def tool_edit_file(cwd: Path, args: dict) -> str:
    """Targeted find/replace, added 2026-08-22 after a real incident: a
    model (qwen3-coder-next) forced to re-emit a whole 762-line file via
    write_file to make one small change generated 35,000+ tokens without
    stopping (confirmed live via Ollama's own generation log -- steady
    ~43 tok/s the entire time, two separate context-window shifts along
    the way) before being killed. Long-verbatim-file reproduction is a
    known LLM failure mode; giving models a small-diff tool instead of
    forcing a full rewrite is the actual fix, not a longer timeout."""
    for required in ("path", "old_string", "new_string"):
        if required not in args:
            return f'ERROR: edit_file requires a "{required}" argument.'
    p = resolve_path(cwd, args["path"])
    if not p.exists():
        return f"ERROR: file not found: {args['path']}"
    try:
        content = p.read_text()
    except Exception as e:
        return f"ERROR reading file: {e}"
    old_string = _fix_literal_escapes(args["old_string"])
    new_string = _fix_literal_escapes(args["new_string"])
    count = content.count(old_string)
    if count == 0:
        # Whitespace-tolerant fallback (added 2026-08-29): local models -- thinking
        # models especially (nemotron-cascade-2 emitted `let ws  = null` with a
        # double space that isn't in the file) -- routinely mangle INTERIOR
        # whitespace, which fails the exact match. Retry matching the same
        # non-whitespace tokens with each whitespace run treated as flexible
        # (\s+), and apply ONLY if it resolves to EXACTLY ONE span. If it's
        # ambiguous (0 or >1), fall through to the exact-match error -- we never
        # silently edit the wrong location. new_string is spliced literally (no
        # regex substitution), so its contents are never reinterpreted.
        # Split on whitespace runs, escape each non-whitespace token, rejoin with
        # \s+ so any run of whitespace (spaces/tabs/newlines) matches flexibly.
        # (Can't re.escape-then-sub: re.escape also escapes the whitespace chars.)
        _toks = [re.escape(t) for t in re.split(r"\s+", old_string) if t]
        ws_pattern = r"\s+".join(_toks)
        ws_matches = list(re.finditer(ws_pattern, content)) if ws_pattern else []
        if len(ws_matches) == 1:
            m = ws_matches[0]
            new_content = content[:m.start()] + new_string + content[m.end():]
            p.write_text(new_content)
            return (f"OK: replaced 1 occurrence in {args['path']} ({len(new_content)} bytes total) "
                    f"[whitespace-tolerant match: your old_string matched except for interior whitespace]")
        return (f"ERROR: old_string not found in {args['path']} -- it must match the file's "
                f"current exact content, including whitespace/indentation. To check, re-read "
                f"AROUND the region you are editing: call read_file with an `offset` near it "
                f"(and a small `length`), not a bare re-read -- on a large file a bare read "
                f"returns only the first page, which may not contain your region at all.")
    if count > 1:
        return (f"ERROR: old_string matches {count} locations in {args['path']} -- it must be "
                f"unique. Include more surrounding context (a line or two before/after) to disambiguate.")
    new_content = content.replace(old_string, new_string, 1)
    p.write_text(new_content)
    return f"OK: replaced 1 occurrence in {args['path']} ({len(new_content)} bytes total)"


# Harness-level guard against a dispatch's own tool calls loading a second
# model into the same VRAM pool it's already resident in. Added 2026-08-29
# after a real self-inflicted CUDA OOM: a dispatch model on Unraid (12GB)
# curl'd Ollama's own /api/embed to test the endpoint it was building code
# against, loading nomic-embed-text alongside itself and crashing the next
# request. First fix attempt was prompt-level ("don't call the endpoint
# yourself") -- Fable (peer review) correctly called that out as the same
# mistake the night's own research had just diagnosed: models don't reliably
# obey an instruction not to do something, this needs enforcement at a layer
# that can't be talked out of it, same as the existing repeated-failure hard
# block below. This is deliberately broad (blocks ANY apparent inference call
# via shell, not just ones naming a different model) -- the dispatch harness
# is the only thing that should be making inference calls; a model's own
# tool use has no legitimate reason to hit an LLM serving endpoint directly.
_INFERENCE_ENDPOINT_RE = re.compile(
    r"/api/(generate|chat|embed|pull|create)\b|/v1/(chat/completions|completions|embeddings)\b"
    r"|\bollama\s+(run|pull|create)\b",
    re.IGNORECASE,
)


def _command_risks_self_collision(command: str) -> bool:
    return bool(_INFERENCE_ENDPOINT_RE.search(command or ""))


# Read-only local-inspection commands. A research-kind dispatch whose real sources
# are LOCAL repo files (trace a call path, find why X is gated on Y, locate a perf
# hotspot) verifies through read_file/list_files AND run_bash greps/cats -- so a
# run_bash that reads or searches the tree is genuine local-source verification, the
# same as a read_file. Used only to decide whether a converged research answer with
# zero web_fetch calls actually showed local verification (in which case the
# unverified-provenance warning is a FALSE signal) vs. showed no verification of any
# kind (in which case it is the real signal the warning was built for). Deliberately
# a leading-verb match on the FIRST simple command: a mutation like `rm`/`git commit`
# must not count as a "read" just because a `grep` appears later in the pipeline.
_LOCAL_READ_CMD_RE = re.compile(
    r"^\s*(?:sudo\s+)?(?:grep|egrep|fgrep|rg|ag|ack|cat|bat|head|tail|less|more|"
    r"sed|awk|find|fd|ls|tree|wc|nl|cut|sort|uniq|diff|stat|file|"
    r"git\s+(?:grep|log|show|diff|blame|status|ls-files|cat-file))\b",
    re.IGNORECASE,
)


def _command_is_local_read(command: str) -> bool:
    """True when a run_bash command is a read-only inspection of the local tree
    (grep/cat/find/git log/etc.). See _LOCAL_READ_CMD_RE for the rationale."""
    return bool(_LOCAL_READ_CMD_RE.match(command or ""))


def should_stamp_unverified(task_kind: str, web_fetch_succeeded: bool,
                            local_read_count: int, facts_provided: bool) -> bool:
    """Decide whether a converged research answer gets the "claims can't be
    verified" harness stamp. Extracted from run_task's convergence path so the
    property is testable directly rather than through the whole tool loop.

    The stamp is reserved for a research answer that shows NO verification of ANY
    kind: zero successful web_fetch calls AND zero local reads. A research task
    that read/grepped the local repo (local_read_count > 0) performed genuine
    local-source verification and is exempt even with zero web_fetch -- stamping
    it is a false signal (job 8d690b764cec). facts_provided (pass-2 synthesis) is
    always exempt: zero fetches is expected by design there and it has its own
    grounding check. coding-kind tasks are never stamped by this path.
    """
    if task_kind != "research":
        return False
    if facts_provided:
        return False
    return not web_fetch_succeeded and local_read_count == 0


def tool_run_bash(cwd: Path, args: dict) -> str:
    try:
        result = subprocess.run(
            args["command"], shell=True, cwd=cwd,
            capture_output=True, text=True, timeout=BASH_TIMEOUT_S,
        )
        return json.dumps({
            "exit_code": result.returncode,
            "stdout": result.stdout[-4000:],
            "stderr": result.stderr[-4000:],
        })
    except subprocess.TimeoutExpired:
        return f"ERROR: command timed out after {BASH_TIMEOUT_S}s"
    except Exception as e:
        return f"ERROR running command: {e}"


WEB_SEARCH_RETRIES = 2
WEB_SEARCH_RETRY_DELAY_S = 20


_KEYCHAIN_CACHE = {}
# Tavily's free tier is a capped 1000 credits/MONTH (shared across both harnesses), so cap Tavily
# calls per dispatch -- each worker process is exactly one run, so a module-level counter = per-run
# (e2's finding 2026-08-30). Ollama web-search is unmetered + returns full page content, so it goes
# first and does the bulk; Tavily is the quality reserve behind it.
_TAVILY_CALLS = 0
_TAVILY_MAX_PER_RUN = 8
# Per-run tally of which web-search backend actually SERVED a usable result (a fresh worker
# process runs per dispatch, so this starts at 0 each run -- no reset needed). Surfaced in
# dispatch-metrics.jsonl["web_search"] and the queue dashboard's web-search panel. Mutated in
# place (dict item assignment needs no `global`).
_WEB_SEARCH_CALLS = {"ollama": 0, "tavily": 0, "brave": 0, "searxng": 0}


def _keychain_secret(service: str):
    """Fetch a secret from the macOS login Keychain by service name, at call time, cached for the
    process (added 2026-08-30 for Brave/Tavily search keys). Returns None if the item is missing OR
    the keychain is locked -- which is exactly the non-GUI-session case ("User interaction is not
    allowed"): a headless worker after a card-only cold boot won't have the login keychain unlocked,
    so callers must degrade gracefully (fall back to SearXNG) rather than fail. Never logs the value."""
    if service in _KEYCHAIN_CACHE:
        return _KEYCHAIN_CACHE[service]
    val = None
    try:
        acct = os.environ.get("USER") or "penn"
        r = subprocess.run(["security", "find-generic-password", "-a", acct, "-s", service, "-w"],
                           capture_output=True, text=True, timeout=10)
        if r.returncode == 0:
            val = r.stdout.strip() or None
        elif "interaction is not allowed" in (r.stderr or "").lower():
            log(f"[worker] web_search: Keychain locked for this session -- can't read {service} "
                f"(non-GUI session); falling back to SearXNG. (Unlock with `security unlock-keychain`.)")
    except Exception:
        val = None
    _KEYCHAIN_CACHE[service] = val
    return val


def _format_search_results(items) -> str:
    """items: iterable of (title, url, snippet). Returns the standard '- title / url / snippet'
    block (top 5) or None if empty, so every backend renders identically to the model."""
    lines = []
    for title, url, snippet in list(items)[:5]:
        if not (url or title):
            continue
        lines.append(f"- {title}\n  {url}\n  {(snippet or '')[:300]}")
    return "\n".join(lines) if lines else None


def _search_tavily(query: str):
    """Tavily -- purpose-built LLM-research search (ranked, answer-shaped). Key: Keychain tavily-api.
    Returns a formatted block, or None to fall through (no key / locked keychain / API error)."""
    key = _keychain_secret("tavily-api")
    if not key:
        return None
    global _TAVILY_CALLS
    if _TAVILY_CALLS >= _TAVILY_MAX_PER_RUN:
        log(f"[worker] web_search: Tavily per-run cap ({_TAVILY_MAX_PER_RUN} credits) reached -- "
            f"skipping Tavily for the rest of this run (falls through to SearXNG).")
        return None
    _TAVILY_CALLS += 1
    try:
        body = json.dumps({"api_key": key, "query": query, "max_results": 5,
                           "search_depth": "basic"}).encode()
        req = urllib.request.Request("https://api.tavily.com/search", data=body,
                                     headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=WEB_TIMEOUT_S) as resp:
            data = json.loads(resp.read())
        return _format_search_results(
            (r.get("title", ""), r.get("url", ""), r.get("content", ""))
            for r in (data.get("results") or []))
    except Exception as e:
        log(f"[worker] web_search: Tavily failed ({e}) -- falling back")
        return None


def _search_brave(query: str):
    """Brave Search API -- reliable general web. Key: Keychain brave-search-api (X-Subscription-Token).
    Returns a formatted block, or None to fall through."""
    key = _keychain_secret("brave-search-api")
    if not key:
        return None
    try:
        url = "https://api.search.brave.com/res/v1/web/search?" + urllib.parse.urlencode(
            {"q": query, "count": 5})
        req = urllib.request.Request(url, headers={"Accept": "application/json",
                                                   "X-Subscription-Token": key})
        with urllib.request.urlopen(req, timeout=WEB_TIMEOUT_S) as resp:
            data = json.loads(resp.read())
        return _format_search_results(
            (r.get("title", ""), r.get("url", ""), r.get("description", ""))
            for r in ((data.get("web") or {}).get("results") or []))
    except Exception as e:
        log(f"[worker] web_search: Brave failed ({e}) -- falling back")
        return None


def _search_ollama(query: str):
    """Ollama's HOSTED web-search API (ollama.com/api/web_search) -- free base tier, a real API (not
    scraped consumer SERPs), so it doesn't rate-limit itself into the ground under its own use like
    the self-hosted SearXNG does. NOTE this is a CLOUD search endpoint at ollama.com; it loads no
    model and never touches the Studio/Unraid GPUs, so it is OUTSIDE the no-raw-local-ollama dispatch
    rule (which exists to prevent local VRAM collisions). Called via direct HTTPS, not the ollama MCP.
    Key: Keychain ollama-web-search (Bearer). Returns a formatted block or None to fall through."""
    key = _keychain_secret("ollama-web-search")
    if not key:
        return None
    try:
        body = json.dumps({"query": query, "max_results": 5}).encode()
        req = urllib.request.Request("https://ollama.com/api/web_search", data=body,
                                     headers={"Content-Type": "application/json",
                                              "Authorization": f"Bearer {key}"}, method="POST")
        with urllib.request.urlopen(req, timeout=WEB_TIMEOUT_S) as resp:
            data = json.loads(resp.read())
        return _format_search_results(
            (r.get("title", ""), r.get("url", ""), r.get("content", ""))
            for r in (data.get("results") or []))
    except Exception as e:
        log(f"[worker] web_search: Ollama web-search failed ({e}) -- falling back")
        return None


def tool_web_search(cwd: Path, args: dict, searxng_host: str) -> str:
    """Tries the free real-API backends first (Tavily, then Ollama web-search), then the free but
    self-degrading SearXNG, then Brave as a PAID last resort (2026-08-30). The two real APIs don't
    rate-limit themselves under their own use the way scraped SearXNG engines do, so a busy research
    run no longer collapses from 3 engines to 1. All keys pulled from Keychain at call time, never
    stored; a locked keychain (non-GUI session) makes that key lookup return None, so the backend is
    simply skipped and the chain degrades gracefully to whatever is reachable. See _search_searxng
    for the original multi-engine + degradation-surfacing behavior."""
    query = args["query"]
    # 1. Free real-API backends first, ORDER = COST not quality (e2's finding 2026-08-30): Ollama
    #    web-search FIRST (unmetered + returns full page content, so it does the bulk work), then
    #    Tavily (richer raw_content but a capped 1000 credits/MONTH shared pool, so it's the quality
    #    reserve behind Ollama, and capped per-run inside _search_tavily).
    for _name, _fn in (("Ollama", _search_ollama), ("Tavily", _search_tavily)):
        out = _fn(query)
        if out:
            _WEB_SEARCH_CALLS[_name.lower()] += 1
            log(f"[worker] web_search: served by {_name}")
            return out
    # 2. SearXNG -- free, self-degrading fallback. Returns a "- title/url/snippet" block on real hits,
    #    else a "No results / engines blocked" explanatory message. Distinguish by the result prefix.
    searxng_out = _search_searxng(query, searxng_host)
    if searxng_out and searxng_out.startswith("- "):
        _WEB_SEARCH_CALLS["searxng"] += 1
        log("[worker] web_search: served by SearXNG")
        return searxng_out
    # Brave (PAID -- $5/1k, card bills) is DELIBERATELY NOT in the auto-chain (e2 + Penn's free-
    # services preference, 2026-08-30): a paid fallback that fires unnoticed accumulates a bill
    # quietly, and "last in chain" is not the same as "never fires without a decision". _search_brave
    # stays defined and the key stays in Keychain for a DELIBERATE future choice, but web_search never
    # auto-invokes it. Nothing free had results -> return SearXNG's explanatory message (names the
    # blocked engines, so the model treats empty as an infra gap, not proof the thing doesn't exist).
    return searxng_out or "No results found."


def _capture_decision(capture_target, file_exists, paused_reason, final_text):
    """(should_write, text_to_write, log_tag). dispatch-fixes item 3b.
    Replaces the `capture_final_as and not paused_for_review` guard: a paused run
    still captures its final answer, with a PARTIAL banner so a reader/scorer sees
    it's incomplete rather than the answer being silently lost (was observed live)."""
    if not capture_target:
        return (False, None, "no-target")
    if file_exists:
        return (False, None, "model-wrote-it")   # never clobber the model's own file
    text = (final_text or "").strip()
    if not text:
        return (False, None, "no-final-text")
    if paused_reason:
        banner = (f"<!-- PARTIAL: run paused before completion ({paused_reason}). "
                  f"Captured from the final text answer. -->\n\n")
        return (True, banner + text, "capture=fallback-partial")
    return (True, text, "capture=fallback")


def _annotate_search_coverage(rendered, results, data, engines_queried):
    """Append a coverage warning to a SUCCESSFUL search when engines are down.
    (dispatch-fixes item 3c.) Before this, unresponsive_engines was only read on
    the EMPTY path -- so 4-of-6 engines suspended with 2 still answering gave the
    model a thin result set with no signal that coverage was degraded, the same
    failure class that burned an entire iteration budget with a non-empty set."""
    unresponsive = (data or {}).get("unresponsive_engines") or []
    if not unresponsive:
        return rendered
    names = sorted({(u[0] if isinstance(u, (list, tuple)) and u else str(u)) for u in unresponsive})
    live = max(0, engines_queried - len(names))
    return (rendered + f"\n\n[COVERAGE WARNING: {len(names)} of {engines_queried} search "
            f"engines are currently blocked or unresponsive ({', '.join(names)}). "
            f"These {len(results)} result(s) come from only {live} engine(s). "
            f"Absence of a result here is NOT evidence the thing does not exist -- "
            f"note the gap rather than concluding a negative.]")


def _search_searxng(query: str, searxng_host: str) -> str:
    """Added 2026-08-28 (retry + engine-status surfacing) after a real dispatch
    (the Unraid model-survey task) burned its entire iteration budget hitting
    empty results and never converging -- confirmed live, independent of any
    model, that SearXNG's every enabled general-web engine (brave, duckduckgo,
    google cse, startpage) was simultaneously suspended ("too many requests" /
    CAPTCHA), a known SearXNG operational issue (self-hosted instances get
    bot-detected), not a query problem. Retested ~15-20 min later and it had
    self-cleared with no config change -- so a bounded retry-with-backoff here
    covers the common case (a suspension already near expiry).

    2026-08-28: the retry-with-backoff above was originally paired with a
    claim that the real fix needed a settings.yml change + container restart
    on Unraid (unreachable, no SSH/Docker access) -- that claim was wrong, not
    verified. Checked live via /config: this instance actually has far more
    general-web engines ENABLED than were ever being queried (bing, mojeek,
    qwant, "duckduckgo web" as a distinct engine from plain duckduckgo, yahoo,
    etc, on top of the 4 default ones) -- they just weren't part of the
    *default* engine set a plain /search call (no `engines=` param) hits.
    Confirmed live which of them actually return real results right now
    (`bing` and `duckduckgo web` both did; `qwant` CAPTCHA'd, `yahoo` errored
    -- same class of failure as the original 4, so not worth adding). Now
    passing an explicit `engines=` param naming both the original 4 AND the
    2 newly-confirmed ones, so a simultaneous suspension needs 6 independent
    engines down at once instead of 4 -- a client-side fix, no Unraid access
    needed at all. `mojeek` was tried too and returned zero results for the
    test query without being flagged unresponsive -- inconclusive rather than
    confirmed-working, left out until it's verified on a query with known
    real results.
    When it's STILL empty after retrying, tell the model WHY (which engines are
    down and why) instead of a flat "No results found" -- so it can correctly
    treat that as an infra gap to note, not as proof the thing it searched for
    doesn't exist (the failure mode that burned the whole iteration budget).

    Renamed 2026-08-30 from tool_web_search to _search_searxng: now the SearXNG fallback behind the
    Tavily (free) primary, with Brave (PAID -- $5/1k, card bills; not the free tier it once was) as a
    last resort only when both free paths are empty. Takes `query` directly (no longer the raw args)."""
    url = f"{searxng_host}/search?" + urllib.parse.urlencode({
        "q": query, "format": "json",
        "engines": "brave,duckduckgo,google cse,startpage,bing,duckduckgo web",
    })
    req = urllib.request.Request(url, headers={"Accept": "application/json"})

    last_data = None
    for attempt in range(1, WEB_SEARCH_RETRIES + 2):
        try:
            with urllib.request.urlopen(req, timeout=WEB_TIMEOUT_S) as resp:
                data = json.loads(resp.read())
        except Exception as e:
            return (f"ERROR: web_search failed ({e}). Is SearXNG deployed and reachable "
                    f"at {searxng_host}?")
        results = (data.get("results") or [])[:5]
        if results:
            lines = []
            for r in results:
                title = r.get("title", "")
                result_url = r.get("url", "")
                snippet = (r.get("content") or "")[:300]
                lines.append(f"- {title}\n  {result_url}\n  {snippet}")
            # 6 = the engines queried on line ~1205; annotate when some were down.
            return _annotate_search_coverage("\n".join(lines), results, data, 6)
        last_data = data
        unresponsive = data.get("unresponsive_engines") or []
        if not unresponsive:
            # Genuinely no results for this query, not an infra issue -- retrying
            # won't help, so stop immediately rather than waste time.
            break
        if attempt <= WEB_SEARCH_RETRIES:
            log(f"[worker] web_search: empty result, {len(unresponsive)} engine(s) "
                f"suspended -- retry {attempt}/{WEB_SEARCH_RETRIES} in {WEB_SEARCH_RETRY_DELAY_S}s")
            time.sleep(WEB_SEARCH_RETRY_DELAY_S)

    unresponsive = (last_data or {}).get("unresponsive_engines") or []
    if unresponsive:
        reasons = ", ".join(f"{name}: {reason}" for name, reason in unresponsive)
        return (f"No results found, and every engine that tried is currently blocked "
                f"({reasons}) -- this looks like a temporary search-infrastructure issue, "
                f"not necessarily an absence of real results for this query. Consider a "
                f"different, more specific query, or note this limitation explicitly in "
                f"your final answer rather than treating the empty result as proof nothing "
                f"exists.")
    return "No results found."


_BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
               "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")


def _extract_aem_json_text(node, out: list, _depth: int = 0) -> None:
    """Walk an AEM content-fragment JSON blob (no fixed schema -- structure
    varies by site config) and collect string values that look like real
    prose rather than IDs/paths/technical keys, so a fetch that lands on a
    JS-rendered page's data endpoint can still return something useful. A
    plain length+space heuristic (>=15 chars, contains a space) is crude but
    effective for the common case: real sentences/labels pass, short
    slugs/UUIDs/booleans-as-strings don't."""
    if _depth > 12:  # guard against a pathological/cyclic structure
        return
    if isinstance(node, str):
        if len(node) >= 15 and " " in node:
            out.append(node)
    elif isinstance(node, dict):
        for v in node.values():
            _extract_aem_json_text(v, out, _depth + 1)
    elif isinstance(node, list):
        for v in node:
            _extract_aem_json_text(v, out, _depth + 1)


def _try_aem_model_json(url: str, timeout: int) -> str | None:
    """Best-effort fallback for JS-rendered (typically Adobe AEM) pages:
    the same content is often exposed as fetchable JSON alongside the
    rendered page. Convention varies by site -- try the two most common
    ones. Returns extracted text, or None if neither variant yields
    anything useful (never raises -- this must never turn a real fetch
    failure into a crash, only into the existing plain error message)."""
    base = url.rstrip("/")
    for candidate in (f"{base}.model.json", f"{base}/_jcr_content.model.json"):
        try:
            req = urllib.request.Request(candidate, headers={"User-Agent": _BROWSER_UA})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
            data = json.loads(raw)
        except Exception:
            continue  # not this variant -- try the next, or give up quietly
        strings: list = []
        _extract_aem_json_text(data, strings)
        text = "\n".join(dict.fromkeys(strings))  # de-dupe, preserve order
        if len(text) >= 200:
            return f"[NOTE: page was JS-rendered; content extracted from its AEM {candidate.rsplit('/', 1)[-1]} endpoint instead]\n{text}"
    return None


def tool_web_fetch(cwd: Path, args: dict, max_chars: int = None) -> str:
    """Added 2026-08-28: PDF handling. Confirmed live during a real research
    dispatch (NV Energy tariff research, qwen3.5:9b) that fetching a PDF
    (a real NV Energy rate-schedule PDF the model found via web_search --
    exactly the kind of document real research legitimately needs to read)
    fell through to the raw-bytes-as-UTF8 fallback below, since trafilatura
    is an HTML extractor and returns nothing useful for PDF bytes. That
    garbage (raw PDF binary decoded as if it were UTF-8 text -- compressed
    streams, control bytes, the works) got returned as the tool result and
    fed back into the next Ollama /api/chat call, which then failed with a
    hard 500 Internal Server Error, crashing the whole dispatch. Root cause
    confirmed by reading the actual crashed transcript, not guessed. Fix:
    detect PDF content (both by Content-Type header AND by magic bytes,
    since servers sometimes mislabel) before ever reaching the HTML
    fallback, and extract real text via pypdf -- never let raw binary reach
    the raw-decode path, which is what caused the crash."""
    # Confirmed live 2026-08-28 (llama3.1:8b, EV-charging discovery dispatch): a model
    # can call web_fetch with a "query" argument (web_search's schema) instead of "url"
    # -- direct dict indexing raised a bare KeyError, which stringifies to just "'url'"
    # with no explanation of what went wrong or how to fix it, burning a tool-call turn
    # on confusion rather than a correction the model could actually act on.
    url = args.get("url")
    if not url:
        if args.get("query"):
            return ('ERROR: web_fetch requires a "url" argument (a specific page to read), '
                     'not a "query" -- that\'s web_search\'s argument. Use web_search first '
                     'to find a URL, then call web_fetch with that exact URL.')
        return 'ERROR: web_fetch requires a "url" argument (the full URL of the page to fetch).'
    # A real browser UA, not a self-identifying "(ollama-worker)" string --
    # confirmed 2026-08-28 this specific string doesn't explain any actual
    # failure seen so far (a live 404 tested identically with this UA, a
    # real Chrome UA, and no UA at all -- genuinely a dead link, not a
    # block), but plenty of other sites' WAFs do filter on a self-declared
    # bot UA even when this one didn't -- a real browser string removes
    # that whole failure class for future fetches at zero cost/risk.
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    })
    try:
        with urllib.request.urlopen(req, timeout=WEB_TIMEOUT_S) as resp:
            content_type = resp.headers.get("Content-Type", "")
            html = resp.read()
    except Exception as e:
        return f"ERROR: web_fetch failed: {e}"

    is_pdf = "pdf" in content_type.lower() or html[:5] == b"%PDF-"
    if is_pdf:
        if pdfplumber is None and pypdf is None:
            return ("ERROR: web_fetch got a PDF at this URL, but neither pdfplumber nor pypdf "
                     "is installed -- can't extract its text. Try a different source, or search "
                     "for a non-PDF page covering the same information.")
        # Confirmed live 2026-08-28 (NV-Energy rate PDF, llama3.1:8b verify-fetch dispatch):
        # pypdf's plain extract_text() reads a real two-column page in an order that scrambles
        # a table's labels away from their values -- a "HOW TO CALCULATE YOUR BILL" side-box's
        # text landed BETWEEN "Winter" and its "$0.08658" figure in the linear output, so a
        # model correctly declined to report a value it could no longer associate with its
        # label. pdfplumber's extract_text(layout=True) preserves the page's visual column
        # structure instead, keeping "Winter... $0.08658" on one line -- verified directly
        # against this exact document. Prefer it; fall back to pypdf only if pdfplumber isn't
        # installed or itself fails, so a missing/broken pdfplumber degrades instead of breaking
        # PDF fetches entirely.
        text = ""
        if pdfplumber is not None:
            try:
                with pdfplumber.open(io.BytesIO(html)) as pdf:
                    pages = [p.extract_text(layout=True) or "" for p in pdf.pages]
                text = "\n\n".join(pages).strip()
            except Exception:
                text = ""
        if not text and pypdf is not None:
            try:
                reader = pypdf.PdfReader(io.BytesIO(html))
                pages = [p.extract_text() or "" for p in reader.pages]
                text = "\n\n".join(pages).strip()
            except Exception as e:
                return f"ERROR: web_fetch found a PDF at this URL but failed to parse it: {e}"
        if not text:
            return ("ERROR: web_fetch found a PDF at this URL but could not extract any "
                     "text from it (it may be a scanned image with no text layer). Try a "
                     "different source.")
    else:
        text = None
        if trafilatura is not None:
            text = trafilatura.extract(html, url=url, include_comments=False, include_tables=True)
        if not text:
            # Added 2026-08-28: this fallback used to dump the FULL raw HTML
            # (up to WEB_FETCH_MAX_CHARS) whenever trafilatura found no
            # extractable main content -- confirmed live during a real
            # research dispatch (NV Energy, qwen3.5:9b) that this is nearly
            # always useless AND expensive: two JS-rendered nvenergy.com
            # pages each dumped a full 8027-char wall of <head>/meta-tag/
            # script-loader boilerplate with zero real content, burning 55%
            # of that run's entire context budget on garbage and directly
            # contributing to it running out of room before writing its
            # actual answer. Fix: strip tags/scripts/styles crudely first
            # (no new dependency -- stdlib re + html.unescape) and check
            # whether there's real substance left. If there is, that's
            # usually a page trafilatura was just too conservative about
            # (still return it, plain-stripped rather than raw markup --
            # much more compact either way). If not, this is almost always
            # a JS-rendered shell with no real static content -- return a
            # short, clear error instead of thousands of wasted chars, so
            # the model can try a different source instead of ingesting soup.
            stripped = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html.decode("utf-8", errors="replace"),
                               flags=re.DOTALL | re.IGNORECASE)
            stripped = re.sub(r"<[^>]+>", " ", stripped)
            stripped = html_module.unescape(stripped)
            stripped = re.sub(r"[ \t]+", " ", stripped)
            stripped = re.sub(r"\n\s*\n+", "\n\n", stripped).strip()
            if len(stripped) >= 200:
                text = f"[NOTE: main-content extraction unavailable, showing tag-stripped page text instead]\n{stripped}"
            else:
                # Added 2026-08-28: confirmed live during a real research
                # dispatch (NV Energy TOU EVRR, llama3.1:8b) that this exact
                # error path fired on three different nvenergy.com URLs, all
                # JS-rendered (Adobe AEM) shells with zero static content --
                # the model then fabricated specific numbers rather than
                # admit it never got real data. AEM sites commonly expose
                # the same content as fetchable JSON alongside the rendered
                # page (the convention varies by site: some serve it at
                # `<path>.model.json`, others need `_jcr_content` inserted
                # before the last path segment) -- try both before giving up.
                aem_text = _try_aem_model_json(url, WEB_TIMEOUT_S)
                if aem_text:
                    text = aem_text
                else:
                    return (f"ERROR: web_fetch could not extract any real content from this page "
                             f"(likely JavaScript-rendered -- the raw HTML has no meaningful static "
                             f"text, only {len(stripped)} chars after stripping markup; also tried "
                             f"this site's AEM .model.json content-fragment endpoints, no luck). "
                             f"Try a different URL for this information -- a PDF version, a cached "
                             f"copy, or a different source entirely.")

    # Confirmed live 2026-08-28 (llama3.1:8b, NV-Energy verify-fetch dispatch): the
    # global 5000-char default was tuned for open-ended multi-fetch research (avoids
    # accumulated-context OOM across 2-3 fetches in one pass), but that same limit
    # silently truncates a single real multi-page document before the actually-needed
    # content -- a real NV Energy rate PDF's page-1 boilerplate alone consumed the
    # whole budget, so the page-2 table with the actual answer never reached the
    # model. It correctly said "not specified" rather than fabricate -- the real bug
    # was truncation, not the model. `max_chars` lets a caller who knows a dispatch is
    # doing few, targeted fetches (not open-ended multi-fetch collection) raise this
    # per-dispatch via --web-fetch-max-chars without weakening the safe default for
    # everyone else.
    limit = max_chars if max_chars is not None else WEB_FETCH_MAX_CHARS
    truncated = len(text) > limit
    text = text[:limit]
    if truncated:
        text += f"\n\n[TRUNCATED at {limit} chars]"
    return text


_TOOL_ARG_NAMES = {
    t["function"]["name"]: set((t["function"].get("parameters") or {}).get("properties") or {})
    for t in TOOLS
}


def _tool_arg_names(name: str) -> set:
    """Argument names a tool actually declares. Derived from TOOLS, never a
    second hand-maintained list -- the whole read_file incident was the schema
    and the behaviour disagreeing, and a duplicated arg list would be the same
    bug in a new place."""
    return _TOOL_ARG_NAMES.get(name, set())


_DIAG_MOD = None


def _diag_mod():
    """Lazy-load dispatch-diagnostics.py (hyphenated filename) from next to this
    file, falling back to ~/bin, so the worker keeps running when the module is
    absent -- the tool then refuses with a visible reason instead of crashing."""
    global _DIAG_MOD
    if _DIAG_MOD is None:
        for cand in (Path(__file__).resolve().parent / "dispatch-diagnostics.py",
                     Path.home() / "bin" / "dispatch-diagnostics.py"):
            if cand.exists():
                spec = importlib.util.spec_from_file_location("dispatch_diagnostics", cand)
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                _DIAG_MOD = mod
                break
    return _DIAG_MOD


def make_tool_request_diagnostics(verify: str = None):
    """One Budget per run_task: the per-run call cap lives in the closure, so
    a resumed transcript starts a fresh budget (deliberate -- a resume is a
    new review-granted allowance, like extra iterations)."""
    mod = _diag_mod()
    budget = mod.Budget() if mod else None

    def tool(cwd: Path, args: dict) -> str:
        if mod is None:
            return "ERROR: request_diagnostics is unavailable on this host (dispatch-diagnostics.py not found); use run_bash."
        reqs = args.get("requests") if isinstance(args, dict) else None
        if isinstance(reqs, str):
            try:
                reqs = json.loads(reqs)
            except json.JSONDecodeError:
                return "ERROR: `requests` must be a JSON list of request objects, e.g. [{\"kind\":\"git\",\"args\":[\"log\",\"--oneline\",\"-n\",\"10\"]}]"
        if not isinstance(reqs, list) or not reqs:
            return "ERROR: `requests` must be a non-empty list of request objects (kinds: git, file, ls, grep, verify, docker_logs)."
        res = mod.run_requests(reqs, cwd, budget=budget, verify=verify)
        return json.dumps(res, ensure_ascii=False)

    return tool


def build_tool_impls(searxng_host: str, web_fetch_max_chars: int = None,
                     read_file_max_chars: int = None, num_ctx: int = None,
                     verify: str = None) -> dict:
    return {
        "request_diagnostics": make_tool_request_diagnostics(verify),
        "list_files": tool_list_files,
        # A lambda, not a bare ref, so the page cap and the window size reach
        # it -- read_file was the last bare function here and that is exactly
        # why it had no way to know how big the context was.
        "read_file": lambda cwd, args: tool_read_file(
            cwd, args, max_chars=read_file_max_chars, num_ctx=num_ctx),
        "write_file": tool_write_file,
        "edit_file": tool_edit_file,
        "run_bash": tool_run_bash,
        "web_search": lambda cwd, args: tool_web_search(cwd, args, searxng_host),
        "web_fetch": lambda cwd, args: tool_web_fetch(cwd, args, max_chars=web_fetch_max_chars),
    }


def _manifest_path(models_root: Path, model: str) -> Path:
    # "qwen3-coder-next:q4_K_M" -> manifests/registry.ollama.ai/library/qwen3-coder-next/q4_K_M
    # "qwen3.6" -> .../qwen3.6/latest
    # "MFDoom/deepseek-r1-tool-calling:14b" -> .../registry.ollama.ai/MFDoom/deepseek-r1-tool-calling/14b
    #
    # Only OFFICIAL models live under "library/"; a namespaced (org/user)
    # model sits directly under the registry root. Hardcoding "library"
    # here silently cost MFDoom/deepseek-r1-tool-calling:14b both of its
    # build-off tasks on 2026-08-22 -- ensure_model_cached raised
    # "not found in SMB source" in 0s and the driver recorded exit=1,
    # files=0, which reads as a model failure rather than a harness bug.
    name, _, tag = model.partition(":")
    tag = tag or "latest"
    root = models_root / "manifests" / "registry.ollama.ai"
    if "/" in name:
        return root / Path(name) / tag
    return root / "library" / name / tag


def _manifest_digests(manifest_path: Path) -> list[str]:
    manifest = json.loads(manifest_path.read_text())
    digests = [manifest["config"]["digest"]] + [l["digest"] for l in manifest["layers"]]
    return [d.replace(":", "-") for d in digests]


def log_model_pull(model: str, size_bytes: int, duration_s: float) -> None:
    MODEL_PULL_LOG.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "ts": datetime.now(timezone.utc).isoformat(), "model": model,
        "size_gb": round(size_bytes / 1024**3, 2), "duration_s": round(duration_s, 1),
    }
    with MODEL_PULL_LOG.open("a") as f:
        f.write(json.dumps(entry) + "\n")
    log(f"[worker] logged model pull: {entry}")


def _normalize_model_name(m: str) -> str:
    n, _, t = m.partition(":")
    return f"{n}:{t or 'latest'}"


def _get_model_size_on_host(host_url: str, model: str) -> int | None:
    """Return this model's total size in bytes as reported by a host's own
    /api/tags, or None if that host is unreachable or doesn't have it."""
    try:
        req = urllib.request.Request(f"{host_url}/api/tags")
        with urllib.request.urlopen(req, timeout=15) as resp:
            tags = json.loads(resp.read())
    except Exception:
        return None
    target = _normalize_model_name(model)
    for m in tags.get("models", []):
        if _normalize_model_name(m.get("name", "")) == target:
            return m.get("size")
    return None


def _host_is_free(host_url: str) -> bool:
    """True if this host's Ollama currently has no model loaded -- the same
    proxy for "busy" used manually via ollama_ps/api_ps throughout
    2026-08-28's dispatches. Fails toward "busy" (False) on any error, so a
    host we can't confirm is free is never treated as free by default."""
    try:
        req = urllib.request.Request(f"{host_url}/api/ps")
        with urllib.request.urlopen(req, timeout=10) as resp:
            ps = json.loads(resp.read())
        return len(ps.get("models") or []) == 0
    except Exception:
        return False


def pick_host(model: str) -> str:
    """Choose which of the two known Ollama hosts to dispatch `model` to.

    Priority, Penn's call 2026-08-28: **Studio first if it's currently
    free, Unraid as parallel overflow capacity, Unraid only ever used when
    the model actually fits it.** (Previously preferred Unraid whenever a
    model fit, to keep Studio free for other things -- reversed because
    Studio is the generally stronger/safer host and Unraid's small 12GB
    VRAM makes it spillover-prone even for models nominally "under" the
    fit threshold; see the ensure_model_ready() spillover check for a case
    that already bit this.) The fit constraint is absolute and NOT
    overridden by busy-ness in either direction: a model that doesn't fit
    Unraid goes to Studio regardless of whether Studio is currently busy
    (queues there -- there's no alternative), and a model that fits Unraid
    only gets routed there when Studio is actually busy (dispatching to
    Studio must never be what blocks a task that could otherwise run on
    Unraid in parallel).

    Queries each host's own /api/tags for the model's real size rather than
    assuming; if neither host currently has it pulled, defaults to Studio
    (unified memory is the safer bet for an unknown-size model -- a bad
    guess there degrades to slow, not broken, whereas guessing Unraid for
    an oversized model repeats the exact spillover this function exists to
    avoid)."""
    known_size = None
    for spec in KNOWN_OLLAMA_HOSTS.values():
        size = _get_model_size_on_host(spec["url"], model)
        if size:
            known_size = size
            break

    fits_unraid = known_size is not None and known_size <= KNOWN_OLLAMA_HOSTS["unraid"]["usable_bytes"]

    if known_size is not None and not fits_unraid:
        log(f"[worker] pick_host: {model} ({known_size/1e9:.1f}GB) doesn't fit Unraid's usable VRAM -- "
            f"Studio only, regardless of busy-ness.")
        return KNOWN_OLLAMA_HOSTS["studio"]["url"]

    if _host_is_free(KNOWN_OLLAMA_HOSTS["studio"]["url"]):
        log(f"[worker] pick_host: Studio is free -- using it.")
        return KNOWN_OLLAMA_HOSTS["studio"]["url"]
    if fits_unraid:
        log(f"[worker] pick_host: Studio busy, {model} ({known_size/1e9:.1f}GB) fits Unraid -- "
            f"using Unraid as parallel capacity.")
        return KNOWN_OLLAMA_HOSTS["unraid"]["url"]
    log(f"[worker] pick_host: Studio busy, {model} size unknown (not pulled anywhere yet) -- "
        f"defaulting to Studio anyway (queues there; safer than an unverified Unraid load).")
    return KNOWN_OLLAMA_HOSTS["studio"]["url"]


def _model_visible_to_local_ollama(model: str) -> bool:
    """Ask the local Ollama server directly whether it already has this
    model, regardless of which directory it's actually stored in. Added
    2026-08-21 after ensure_model_cached blindly copied a model's full
    blobs into LOCAL_MODEL_CACHE even though it was already sitting in
    Ollama's real (default, OLLAMA_MODELS-unset) directory the whole
    time -- wasted ~24GB of a redundant SMB copy before being caught."""
    try:
        req = urllib.request.Request("http://localhost:11434/api/tags")
        with urllib.request.urlopen(req, timeout=15) as resp:
            tags = json.loads(resp.read())
    except Exception:
        return False
    names = {_normalize_model_name(m.get("name", "")) for m in tags.get("models", []) if m.get("name")}
    return _normalize_model_name(model) in names


def ensure_model_cached(model: str) -> None:
    """Copy `model`'s blobs from the SMB source into LOCAL_MODEL_CACHE
    (OLLAMA_MODELS normally points here) if not already present. Confirmed
    live 2026-08-21: Ollama's own model-load path over SMB hangs
    indefinitely; a plain file copy from the same share does not -- so
    this, not client-side tuning, is the actual fix. No-op if the model is
    already cached locally (checked two ways: already visible to the
    running local Ollama server via /api/tags -- covers the case where
    OLLAMA_MODELS isn't actually pointed at LOCAL_MODEL_CACHE, e.g. a
    model placed directly in Ollama's default directory -- or already
    present in LOCAL_MODEL_CACHE's own manifest)."""
    if _model_visible_to_local_ollama(model):
        log(f"[worker] {model} already visible to local Ollama (/api/tags) -- skipping SMB copy entirely.")
        return
    local_manifest = _manifest_path(LOCAL_MODEL_CACHE, model)
    if local_manifest.exists():
        log(f"[worker] {model} already cached locally, skipping copy.")
        return
    source_manifest = _manifest_path(SMB_MODEL_SOURCE, model)
    if not source_manifest.exists():
        raise RuntimeError(f"model {model} not found in SMB source at {source_manifest}")

    digests = _manifest_digests(source_manifest)
    log(f"[worker] caching {model} locally ({len(digests)} blob(s)) -- this reads the full "
        f"model over SMB once, expect real time for a large model...")
    start = datetime.now(timezone.utc)
    total_size = 0
    (LOCAL_MODEL_CACHE / "blobs").mkdir(parents=True, exist_ok=True)
    for digest in digests:
        src = SMB_MODEL_SOURCE / "blobs" / digest
        dst = LOCAL_MODEL_CACHE / "blobs" / digest
        if not dst.exists():
            shutil.copyfile(src, dst)
        total_size += dst.stat().st_size
    local_manifest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source_manifest, local_manifest)
    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    log(f"[worker] cached {model}: {total_size/1024**3:.1f}GB in {elapsed:.0f}s")
    log_model_pull(model, total_size, elapsed)


def evict_model(model: str) -> None:
    """Remove `model`'s blobs from LOCAL_MODEL_CACHE, but only blobs not
    referenced by any OTHER cached model's manifest -- safe even if models
    share layers. Used by --cleanup-after to avoid permanently consuming
    local disk for models only used occasionally."""
    local_manifest = _manifest_path(LOCAL_MODEL_CACHE, model)
    if not local_manifest.exists():
        log(f"[worker] {model} not in local cache, nothing to evict.")
        return
    this_digests = set(_manifest_digests(local_manifest))

    other_digests = set()
    manifests_root = LOCAL_MODEL_CACHE / "manifests"
    for other_manifest in manifests_root.rglob("*"):
        if not other_manifest.is_file() or other_manifest == local_manifest:
            continue
        try:
            other_digests.update(_manifest_digests(other_manifest))
        except Exception:
            continue

    freed = 0
    for digest in this_digests - other_digests:
        blob = LOCAL_MODEL_CACHE / "blobs" / digest
        if blob.exists():
            freed += blob.stat().st_size
            blob.unlink()
    local_manifest.unlink()
    log(f"[worker] evicted {model} from local cache, freed {freed/1024**3:.1f}GB")


def log_dispatch_to_obsidian(model: str, task: str, converged: bool, verify_passed, log_path: Path,
                              baseline_no_regression: bool = False) -> None:
    """Best-effort append to the vault's dispatch log -- never raises,
    a logging failure must not fail the actual dispatch. Requires
    OBSIDIAN_TOKEN in the environment (set via launchctl setenv, never
    hardcoded in this file)."""
    if not OBSIDIAN_TOKEN:
        log("[worker] OBSIDIAN_TOKEN not set -- skipping vault dispatch log (task itself is unaffected).")
        return
    status = "converged" if converged else "DID NOT CONVERGE"
    # baseline_no_regression: the verify FAILED but every failure pre-existed the dispatch
    # (0 new from the model's diff). Reported distinctly so this never reads as a clean PASS.
    if baseline_no_regression:
        verify_str = "BASELINE-BROKEN (0 new failures; verify was already failing at start)"
    else:
        verify_str = "PASSED" if verify_passed is True else "FAILED" if verify_passed is False else "not run"
    entry = (
        f"\n- **{datetime.now(timezone.utc).isoformat(timespec='seconds')}** "
        f"`{model}` -- {status}, verify {verify_str}\n"
        f"  task: {task[:200]}{'...' if len(task) > 200 else ''}\n"
        f"  transcript: `{log_path}`\n"
    )
    try:
        req = urllib.request.Request(
            f"{OBSIDIAN_URL}/vault/{urllib.parse.quote(OBSIDIAN_DISPATCH_LOG_PATH)}",
            data=entry.encode(), method="POST",
            headers={"Authorization": f"Bearer {OBSIDIAN_TOKEN}", "Content-Type": "text/markdown"},
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            resp.read()
        log("[worker] dispatch logged to Obsidian vault.")
    except Exception as e:
        log(f"[worker] vault logging failed (non-fatal): {e}")


CHAT_TIMEOUT_S = 1200
WARMUP_TIMEOUT_S = 900  # cold model load (weights off disk into RAM/VRAM) can take minutes, not seconds
CHAT_RETRIES = 2


def _is_cuda_oom(error_detail: str) -> bool:
    """Matches Ollama's own CUDA-out-of-memory error body, distinct from a generic
    HTTP/network failure -- see call_ollama/call_ollama_streaming's own recovery
    comment for why this specific signature gets a force-unload-and-retry instead of
    the normal same-state retry (which is provably useless against it, confirmed
    live 2026-08-29: 3/3 identical failures at unchanged settings)."""
    return "CUDA error" in error_detail and "out of memory" in error_detail
# Confirmed live 2026-08-28: an Unraid llama3.1:8b dispatch generated past
# n_gen=123,000 tokens with no stop token, triggered a mid-generation
# context-shift (discarding 12,285 tokens just to keep going), and was still
# running when killed -- a genuine runaway-generation loop, not a slow-but-
# real response (confirmed via the container's own live print_timing log,
# not guessed). Nothing anywhere in this file capped response length before
# this, so a model that fails to emit a stop token can burn the entire
# --chat-timeout budget generating nothing useful. 8192 is generous for a
# real long single-turn output (a full file write, a long tool-call
# payload) while nowhere near what a genuine runaway would need to be
# caught early.
DEFAULT_MAX_TOKENS = 8192
# Same incident, second contributing factor -- confirmed via real research
# (github.com/ollama/ollama/issues/3759; ggml-org/llama.cpp discussion
# #3005), not guessed: this file never set repeat_penalty anywhere, so it
# always ran at llama.cpp's neutral default (1.0 = disabled). Combined with
# --temperature 0 (fully greedy decoding, the common case for every
# dispatch tonight for reproducibility), that's a documented, reproducible
# recipe for a self-reinforcing repetition loop once generation drifts --
# nothing pushes it back out. A small, standard penalty (1.1, the commonly
# recommended value) breaks that without materially changing normal output.
DEFAULT_REPEAT_PENALTY = 1.1

# Structured, one-line-per-dispatch token-usage log -- added 2026-08-28 after
# a real dispatch tonight hit a hard context-exhaustion failure (llama-server:
# "request (68352 tokens) exceeds the available context size (32768 tokens)")
# with no record anywhere of how close to the ceiling past dispatches had
# come. Deliberately JSONL, not prose in the Obsidian vault log -- the point
# is to eventually query "what's the actual right --num-ctx for a task this
# size" across many dispatches, which means every entry needs the same fixed
# fields, not a paragraph a human has to re-parse each time.
DISPATCH_METRICS_PATH = LOG_DIR / "dispatch-metrics.jsonl"

# Mutated in place by run_task as the dispatch progresses (not returned,
# since main()'s crash handler needs to see whatever was accumulated even
# when run_task never reaches a normal return -- see write_dispatch_metrics).
_dispatch_metrics: dict = {}


def write_dispatch_metrics(metrics: dict) -> None:
    """Append one JSONL line for this dispatch's token usage, whether it
    converged, failed verify, or crashed outright. Never raises -- a
    metrics-logging failure must not fail (or mask the real error of) the
    actual dispatch. Called both from run_task's normal completion path and
    from main()'s crash handler, so a context-exhaustion crash -- the exact
    failure mode this log exists to eventually let Penn threshold against --
    still gets a real entry instead of silently vanishing."""
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        with open(DISPATCH_METRICS_PATH, "a") as f:
            f.write(json.dumps(metrics) + "\n")
    except Exception as e:
        log(f"[worker] dispatch metrics logging failed (non-fatal): {e}")


def call_ollama(host: str, model: str, messages: list, temperature: float, num_ctx: int,
                 timeout: int = CHAT_TIMEOUT_S, tools: bool = True,
                 top_p: float = None, top_k: int = None, api_style: str = "ollama",
                 max_tokens: int = DEFAULT_MAX_TOKENS,
                 repeat_penalty: float = DEFAULT_REPEAT_PENALTY, think=None) -> dict:
    """api_style="openai" targets llama-server (or any OpenAI-compatible
    /v1/chat/completions endpoint) instead of Ollama's native /api/chat.
    Added 2026-08-22: confirmed live that Ollama's own chat-template
    validation has a real upstream bug ("no user query found in messages",
    github.com/ollama/ollama/issues/17778) that crashes qwen3.8/devstral
    even on trivial requests -- llama-server renders the GGUF's own embedded
    chat template directly and doesn't run Ollama's custom Go renderer code
    at all, so it doesn't hit this bug. Returns a response already
    normalized to Ollama's shape ({"message": {...}}) so callers don't need
    to know which backend actually served the request."""
    options = {"temperature": temperature, "num_ctx": num_ctx}
    if top_p is not None:
        options["top_p"] = top_p
    if top_k is not None:
        options["top_k"] = top_k
    if max_tokens is not None:
        options["num_predict"] = max_tokens
    if repeat_penalty is not None:
        options["repeat_penalty"] = repeat_penalty

    if api_style == "openai":
        payload = {"model": model, "messages": messages, "stream": False,
                   "temperature": temperature}
        if top_p is not None:
            payload["top_p"] = top_p
        if max_tokens is not None:
            payload["max_tokens"] = max_tokens
        if repeat_penalty is not None:
            payload["repeat_penalty"] = repeat_penalty
        if tools:
            payload["tools"] = TOOLS
        url = f"{host}/v1/chat/completions"
    else:
        payload = {"model": model, "messages": messages, "stream": False, "options": options}
        # think (added 2026-08-30, e2's finding): a top-level Ollama key, native /api/chat only.
        # None => omit (model's default / no toggle). False => disable reasoning so a hybrid model
        # (qwen3.5:9b etc.) doesn't spend its whole token budget on the `thinking` field and return
        # empty content / no tool call. True => force it on. Always-thinking models (nemotron-a3b)
        # reject an explicit value with an HTTP error; there is NO auto-retry that drops think --
        # use --think auto (the default) for those models.
        if think is not None:
            payload["think"] = think
        if tools:
            payload["tools"] = TOOLS
        url = f"{host}/api/chat"

    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    last_err = None
    last_err_detail = None
    for attempt in range(1, CHAT_RETRIES + 2):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = json.loads(resp.read())
                if api_style == "openai":
                    # Normalize {"choices": [{"message": {...}}]} to Ollama's
                    # {"message": {...}} shape so the rest of run_task's loop
                    # doesn't need to know which backend answered. llama-server's
                    # /v1/chat/completions already reports usage in the standard
                    # OpenAI {prompt_tokens, completion_tokens, total_tokens}
                    # shape -- pass it straight through.
                    return {"message": raw["choices"][0]["message"], "usage": raw.get("usage") or {}}
                # Ollama's native /api/chat reports token counts under
                # different keys (prompt_eval_count/eval_count, no
                # total_tokens at all) -- normalize to the same
                # {prompt_tokens, completion_tokens, total_tokens} shape as
                # the openai branch above so run_task's usage tracking
                # doesn't need to know which backend answered either.
                pt = raw.get("prompt_eval_count") or 0
                ct = raw.get("eval_count") or 0
                raw["usage"] = {"prompt_tokens": pt, "completion_tokens": ct, "total_tokens": pt + ct}
                return raw
        except urllib.error.HTTPError as e:
            # The response BODY (the server's actual error message) was never
            # being read here -- every crash tonight (2026-08-28) had to be
            # reconstructed from raw ollama.log/llama-server stdout after the
            # fact instead of just being visible in the raised error, because
            # str(e) on an HTTPError is just the status line ("HTTP Error
            # 500: Internal Server Error"), not the JSON body a server like
            # llama-server actually sends (e.g. {"error":{"message":"tools
            # param requires --jinja flag", ...}}). Read it once, defensively
            # (the body can itself be unreadable/already consumed).
            try:
                body = e.read().decode("utf-8", errors="replace")[:2000]
            except Exception:
                body = "<could not read response body>"
            # Bug fixed 2026-08-29: this used to overwrite last_err with a
            # plain f-string (including the body) instead of the exception
            # object -- `raise ... from last_err` then crashed with
            # "exception causes must derive from BaseException" once retries
            # were exhausted, MASKING the real underlying error (a genuine
            # CUDA OOM, in the case that surfaced this) behind an unrelated
            # TypeError. Keep last_err as the real exception for `from`;
            # carry the body separately for display only.
            last_err = e
            last_err_detail = f"{e} -- body: {body}"
            if attempt <= CHAT_RETRIES:
                log(f"[worker] {'llama-server' if api_style == 'openai' else 'Ollama'} request failed ({last_err_detail}), retry {attempt}/{CHAT_RETRIES}...")
        except (urllib.error.URLError, TimeoutError) as e:
            last_err = e
            last_err_detail = str(e)
            if attempt <= CHAT_RETRIES:
                log(f"[worker] {'llama-server' if api_style == 'openai' else 'Ollama'} request failed ({e}), retry {attempt}/{CHAT_RETRIES}...")
    if api_style == "ollama" and last_err_detail and _is_cuda_oom(last_err_detail):
        # Same recovery as call_ollama_streaming's own version of this block -- see
        # its comment for the full reasoning. Gated to native Ollama only: llama-server
        # (api_style="openai") has no /api/generate keep_alive concept to force-unload
        # through, and is a single-model-per-process server anyway, so this specific
        # recovery doesn't apply there.
        log(f"[worker] CUDA OOM detected after normal retries -- force-unloading "
            f"{model} and retrying once more (a fresh load can defragment the CUDA "
            f"pool; a same-state retry cannot, which is why the retries above failed "
            f"identically).")
        set_keep_alive(host, model, "0")
        time.sleep(3)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = json.loads(resp.read())
                pt = raw.get("prompt_eval_count") or 0
                ct = raw.get("eval_count") or 0
                raw["usage"] = {"prompt_tokens": pt, "completion_tokens": ct, "total_tokens": pt + ct}
                return raw
        except Exception as e:
            log(f"[worker] CUDA OOM recovery retry also failed: {e}")
    raise RuntimeError(f"Chat request failed after retries: {last_err_detail}") from last_err

# ---------------------------------------------------------------------------
# Opt-in live streaming log (--live-log). Ported from qwen-dispatch.sh's
# embedded Python: the checkpoint-extraction regexes, the margin=20 trick,
# the sentence-boundary-aware snippet(), the None -> 'thinking' -> 'writing'
# phase state machine, and the DIM/YELLOW/CYAN/GREEN/RED ANSI scheme.
# Differences from qwen-dispatch.sh (which is one-shot and tool-less):
#   * every line is prefixed with [dispatch-tag] so several concurrent
#     dispatches sharing one log file stay separable under `tail -f`
#   * tool-call / tool-result lines (magenta) -- qwen-dispatch.sh has no
#     tool loop at all
#   * per-tag rate file (~/qwen-rate-<tag>.txt) instead of ~/qwen-rate.txt
# When --live-log is not passed, none of this runs: call_ollama() is
# untouched and the non-streaming path is exactly what it was before.
# ---------------------------------------------------------------------------

LIVE_DIM = '\033[2m'
LIVE_YELLOW = '\033[1;33m'
LIVE_CYAN = '\033[1;36m'
LIVE_GREEN = '\033[1;32m'
LIVE_RED = '\033[1;31m'
LIVE_MAGENTA = '\033[1;35m'
LIVE_RESET = '\033[0m'

_LIVE_BOLD_RE = re.compile(r'\*\*([^*]{4,80})\*\*')
_LIVE_NUMBERED_RE = re.compile(r'(?:^|\n)\s*(?:\d+[.):]|Step \d+)\s*([A-Z][^\n.]{4,80})', re.MULTILINE)
_LIVE_CHECKLIST_RE = re.compile(r'(?:^|\n)\s*-\s*([A-Z][a-zA-Z ]{2,30}):\s*[^\n]{0,60}?(✓|✗|\bMet\b|\bmatch(?:es)?\b)', re.MULTILINE)


def _live_snippet(text, n=100):
    """Ported from qwen-dispatch.sh: collapse whitespace, then cut at the
    start of the last complete sentence within the tail window so the
    heartbeat reads as a real thought instead of a mid-word fragment."""
    text = ' '.join(text.split())
    if len(text) <= n:
        return text
    tail = text[-n:]
    for sep in ('. ', '? ', '! '):
        idx = tail.rfind(sep)
        if idx != -1 and idx < len(tail) - 15:  # don't cut right at the end
            return tail[idx + len(sep):]
    return tail


def _live_find_new_checkpoints(text, seen, margin=20):
    """Ported from qwen-dispatch.sh: pull out bold headers / numbered-step /
    checklist markers as they appear, so the live view shows real structure
    instead of an arbitrary rolling text window. margin: only accept a match
    that ends at least this many chars before the end of text -- otherwise,
    on a streaming buffer, a still-growing partial line matches a
    slightly-longer version of itself on every token and spams one line per
    token."""
    found = []
    limit = len(text) - margin
    for pattern, build in (
        (_LIVE_BOLD_RE, lambda m: m.group(1).strip()),
        (_LIVE_NUMBERED_RE, lambda m: m.group(1).strip()),
        (_LIVE_CHECKLIST_RE, lambda m: f'{m.group(1).strip()}: {m.group(2)}'),
    ):
        for m in pattern.finditer(text):
            if m.end() > limit:
                continue  # too close to the live edge, may still be growing
            c = build(m)
            if c not in seen:
                seen.add(c)
                found.append(c)
    return found


class LiveLog:
    """Appends tagged, colored status lines to a file a human can `tail -f`
    (the qwen.penndalton.com ttyd terminal already does exactly this for
    qwen-dispatch.sh's log). Every line is prefixed with [tag] because
    multiple concurrent dispatches commonly share one --live-log file. The
    log path is whatever the caller passed -- this file stays host-agnostic
    about where the dashboard lives."""

    def __init__(self, path, tag, model, host):
        self.path = Path(path)
        self.tag = tag
        self.model = model
        self.host = host
        # Per-tag rate file (qwen-dispatch.sh writes ~/qwen-rate.txt for its
        # tmux status bar; with concurrent tagged dispatches sharing a log,
        # one rate file per tag is the direct generalization).
        self.rate_path = Path.home() / f"qwen-rate-{tag}.txt"
        # Line-buffered so `tail -f` sees every line the moment it's written.
        self._fh = open(self.path, "a", buffering=1)
        self.write_rate(f"{model}: dispatch starting")
        self.dispatch_header()

    def _ts(self):
        return time.strftime("%H:%M:%S")

    def emit(self, line, color):
        self._fh.write(f"{LIVE_DIM}[{self._ts()}]{LIVE_RESET} [{self.tag}] {color}{line}{LIVE_RESET}\n")
        self._fh.flush()

    def write_rate(self, text):
        try:
            self.rate_path.write_text(text)
        except Exception:
            pass  # rate file is cosmetic; never fail a dispatch over it

    def dispatch_header(self):
        self.emit("═" * 60, LIVE_CYAN)
        self.emit(f"▶ NEW DISPATCH {self._ts()} — {self.model} @ {self.host}", LIVE_CYAN)
        self.emit("═" * 60, LIVE_CYAN)

    def iteration(self, n, total):
        self.emit(f"── iteration {n}/{total} ──", LIVE_DIM)

    def tool_call(self, name, args):
        parts = []
        for k, v in (args or {}).items():
            s = str(v).replace("\n", " ")
            if len(s) > 60:
                s = s[:57] + "..."
            parts.append(f"{k}='{s}'" if isinstance(v, str) else f"{k}={v!r}")
        self.emit(f"-> calling {name}({', '.join(parts)})", LIVE_MAGENTA)

    def tool_result(self, name, result):
        s = str(result)
        n_lines = s.count("\n") + 1
        # Byte/line count only -- tool results can be huge file reads, and
        # dumping them here would defeat the point of a glanceable live view.
        self.emit(f"<- {name} returned {len(s)} bytes ({n_lines} lines)", LIVE_MAGENTA)

    def result_box(self, text, converged, iterations, status=None):
        # status overrides the converged/not binary for the third real outcome: PAUSED.
        # A paused run is not a failure and must not be painted as one -- after worker
        # batch #7 every legitimately-paused arm (the verify cannot answer the question,
        # a human is needed) reaches here, and reading "NO CONVERGENCE" on a run that
        # stopped deliberately at 5/20 misrepresents both the outcome and the budget.
        if status == "paused":
            color, title = LIVE_YELLOW, "PAUSED FOR REVIEW"
        else:
            color = LIVE_GREEN if converged else LIVE_RED
            title = "RESULT" if converged else "NO CONVERGENCE"
        if len(text) > 4000:
            text = text[:4000] + "\n... [truncated]"
        self.emit(f"┌─ {title} " + "─" * (53 + (6 - len(title))), color)
        for l in text.split("\n"):
            self.emit(f"│ {l}", color)
        self.emit("└" + "─" * 63, color)

    def close(self):
        try:
            self._fh.close()
        except Exception:
            pass


def call_ollama_streaming(host: str, model: str, messages: list, temperature: float, num_ctx: int,
                          timeout: int = CHAT_TIMEOUT_S, tools: bool = True,
                          top_p: float = None, top_k: int = None, live: "LiveLog" = None,
                          max_tokens: int = DEFAULT_MAX_TOKENS,
                          repeat_penalty: float = DEFAULT_REPEAT_PENALTY, think=None) -> dict:
    """Streaming counterpart of call_ollama() for the native Ollama
    /api/chat path ONLY (api_style="ollama", native tools). Used only when
    --live-log is active: sends the same request with stream: true, parses
    the newline-delimited streamed JSON exactly like qwen-dispatch.sh does
    (msg.get('thinking', ''), msg.get('content', ''), d.get('done')), and
    emits live status lines to the LiveLog as it goes.

    Returns the same normalized shape call_ollama() returns --
    {"message": {...}, "usage": {prompt_tokens, completion_tokens,
    total_tokens}} -- so run_task()'s loop logic downstream doesn't change.
    The --manual-tools / api_style="openai" paths are deliberately NOT
    covered here (out of scope for this pass); run_task falls back to the
    blocking call_ollama for those."""
    options = {"temperature": temperature, "num_ctx": num_ctx}
    if top_p is not None:
        options["top_p"] = top_p
    if top_k is not None:
        options["top_k"] = top_k
    if max_tokens is not None:
        options["num_predict"] = max_tokens
    if repeat_penalty is not None:
        options["repeat_penalty"] = repeat_penalty
    payload = {"model": model, "messages": messages, "stream": True, "options": options}
    if think is not None:  # native /api/chat top-level key (see call_ollama)
        payload["think"] = think
    if tools:
        payload["tools"] = TOOLS
    url = f"{host}/api/chat"

    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    # Retry only the connection attempt (same policy as call_ollama). Once
    # the stream is open and tokens are flowing, a mid-stream failure is
    # raised as-is -- retrying would duplicate a partially-consumed
    # generation, which is worse than surfacing the error.
    resp = None
    last_err = None
    last_err_detail = None
    for attempt in range(1, CHAT_RETRIES + 2):
        try:
            resp = urllib.request.urlopen(req, timeout=timeout)
            break
        except urllib.error.HTTPError as e:
            # Same body-reading fix call_ollama() got on 2026-08-29 (see its
            # HTTPError handler for the full story): str(e) is just the status
            # line, not the JSON body a server actually sends.
            try:
                body = e.read().decode("utf-8", errors="replace")[:2000]
            except Exception:
                body = "<could not read response body>"
            last_err = e
            last_err_detail = f"{e} -- body: {body}"
            if attempt <= CHAT_RETRIES:
                log(f"[worker] Ollama streaming request failed ({last_err_detail}), retry {attempt}/{CHAT_RETRIES}...")
        except (urllib.error.URLError, TimeoutError) as e:
            last_err = e
            last_err_detail = str(e)
            if attempt <= CHAT_RETRIES:
                log(f"[worker] Ollama streaming request failed ({e}), retry {attempt}/{CHAT_RETRIES}...")
    if resp is None and last_err_detail and _is_cuda_oom(last_err_detail):
        # Fable diagnosis, 2026-08-29 (confirmed live on Unraid: qwen3.5:9b succeeded
        # on iteration 1, failed identically on iteration 2's generation call, 3/3
        # attempts, same settings, same already-resident model): a normal retry can't
        # fix this because it's deterministic -- llama.cpp's CUDA compute buffers scale
        # with actual prompt size, not just num_ctx, and a later agentic iteration
        # carries the whole prior turn forward, landing right at the GPU's VRAM margin.
        # A fresh load defragments the CUDA pool, so force-unload and retry ONCE more
        # before giving up -- distinct from the normal CHAT_RETRIES loop above (which
        # already ran and failed identically every time for exactly this reason).
        log(f"[worker] CUDA OOM detected after normal retries -- force-unloading "
            f"{model} and retrying once more (a fresh load can defragment the CUDA "
            f"pool; a same-state retry cannot, which is why the retries above failed "
            f"identically).")
        set_keep_alive(host, model, "0")
        time.sleep(3)
        try:
            resp = urllib.request.urlopen(req, timeout=timeout)
        except Exception as e:
            log(f"[worker] CUDA OOM recovery retry also failed: {e}")
    if resp is None:
        raise RuntimeError(f"Chat request failed after retries: {last_err_detail}") from last_err

    start = time.time()
    last_rate_write = 0.0
    last_status_write = 0.0
    token_count = 0
    think_chars = 0
    think_buf = ''
    content_parts = []
    tool_calls_acc = []
    phase = None  # None -> 'thinking' -> 'writing'
    seen_checkpoints = set()
    prompt_tokens = 0
    completion_tokens = 0

    def emit(line, color):
        if live is not None:
            live.emit(line, color)

    with resp:
        for line in resp:
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except Exception:
                continue
            msg = d.get('message', {})
            think = msg.get('thinking', '')
            content = msg.get('content', '')
            tcs = msg.get('tool_calls') or []
            if tcs:
                # Ollama delivers tool_calls in the final message chunk(s)
                # before the done chunk; accumulate so a split delivery
                # still yields the complete list.
                tool_calls_acc.extend(tcs)

            if think:
                if phase != 'thinking':
                    emit(f'[{model}] thinking...', LIVE_YELLOW)
                    phase = 'thinking'
                think_chars += len(think)
                think_buf += think
                token_count += max(1, len(think) // 4)
                for cp in _live_find_new_checkpoints(think_buf, seen_checkpoints):
                    topic = cp if len(cp) <= 45 else cp[:42].rsplit(' ', 1)[0] + '...'
                    emit(f'[{model}] thinking about: {topic}', LIVE_YELLOW)
                    last_status_write = time.time()

            if content:
                if phase != 'writing':
                    emit(f'[{model}] thought for ~{think_chars} chars, now writing...' if think_chars
                         else f'[{model}] writing...', LIVE_YELLOW)
                    phase = 'writing'
                content_parts.append(content)
                token_count += max(1, len(content) // 4)

            now = time.time()
            if phase == 'thinking' and now - last_status_write > 8:
                # Fallback for stretches with no bold/numbered structure to latch onto.
                emit(f'[{model}]   ...still thinking: "{_live_snippet(think_buf)}"', LIVE_DIM)
                last_status_write = now
            elif phase == 'writing' and now - last_status_write > 2:
                cur_lines = ''.join(content_parts).count(chr(10)) + 1
                emit(f'[{model}]   ...writing: line {cur_lines}', LIVE_DIM)
                last_status_write = now

            if now - last_rate_write > 0.5:
                elapsed = now - start
                rate = token_count / elapsed if elapsed > 0 else 0
                if live is not None:
                    live.write_rate(f'{model}: {rate:.1f} tok/s (est)')
                last_rate_write = now

            if d.get('done'):
                prompt_tokens = d.get('prompt_eval_count') or 0
                completion_tokens = d.get('eval_count') or 0
                if live is not None and completion_tokens and d.get('eval_duration'):
                    real_rate = completion_tokens / (d['eval_duration'] / 1e9)
                    live.write_rate(f'{model}: {real_rate:.1f} tok/s (last run, done)')

    full_content = ''.join(content_parts)
    full_thinking = think_buf
    elapsed = time.time() - start
    if live is not None:
        n_lines = full_content.count(chr(10)) + 1 if full_content else 0
        emit(f'[{model}] done — {n_lines} lines ({len(full_content)} chars)'
             + (f', {len(tool_calls_acc)} tool call(s)' if tool_calls_acc else '')
             + f' in {elapsed:.1f}s', LIVE_GREEN)

    # Same normalized shape call_ollama() returns for the native path:
    # message dict (role/content, plus thinking and tool_calls when the
    # model produced them -- matching what a non-streaming response
    # contains) and usage normalized to the OpenAI-style token keys.
    message = {"role": "assistant", "content": full_content}
    if full_thinking:
        message["thinking"] = full_thinking
    if tool_calls_acc:
        message["tool_calls"] = tool_calls_acc
    return {
        "message": message,
        "usage": {"prompt_tokens": prompt_tokens,
                  "completion_tokens": completion_tokens,
                  "total_tokens": prompt_tokens + completion_tokens},
    }



def set_keep_alive(host: str, model: str, keep_alive: str) -> None:
    """Adjust how long a model stays resident after this dispatch ends,
    without generating anything -- a bare /api/generate with no `prompt`
    just applies the new `keep_alive` TTL to the already-loaded model.
    Ollama's own default keep_alive is 5m, which would otherwise unload the
    model almost immediately after the last real request in the loop below.
    Added 2026-08-28 alongside pick_host(): now that dispatch fits a model
    to whichever host actually has room for it, it's worth leaving it
    resident there for reuse instead of paying a full cold-load again on
    the very next dispatch."""
    try:
        req = urllib.request.Request(
            f"{host}/api/generate",
            data=json.dumps({"model": model, "keep_alive": keep_alive}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            resp.read()
        log(f"[worker] set keep_alive={keep_alive} for {model} on {host}.")
    except Exception as e:
        log(f"[worker] WARNING: failed to set keep_alive for {model} on {host}: {e}")


def _try_lan_copy(host: str, model: str) -> bool:
    """Before falling back to a fresh registry pull, try copying the model
    over the LAN instead -- from Unraid's shared store if this dispatch
    targets Mac Studio's local Ollama, or from Mac Studio's local cache if
    this dispatch targets Unraid. Mirrors bakeoff-driver-buildoff.sh's
    ensure_model_available(), generalized here so it applies to any
    dispatch (ad-hoc or driver-orchestrated), not only driver-orchestrated
    ones -- confirmed live 2026-08-22 that a direct ollama-worker.py
    invocation skipped this safeguard entirely before this fix, since it
    previously only lived in the bash driver wrapping this function.
    Returns True if the copy succeeded (model is now present at `host`),
    False if the LAN path isn't available or the model isn't there either
    -- caller falls back to a fresh pull in that case."""
    if not os.path.ismount(LAN_MOUNT_ROOT):
        log(f"[worker] {LAN_MOUNT_ROOT} not mounted -- skipping LAN-copy path.")
        return False

    targets_unraid = any(h in host for h in UNRAID_OLLAMA_HOSTS)
    direction = "push" if targets_unraid else "pull"
    log(f"[worker] {model} not present at {host} -- trying LAN copy ({direction}) before a fresh pull...")

    try:
        result = subprocess.run(
            [sys.executable, COPY_HELPER, direction, model],
            capture_output=True, text=True, timeout=600,
        )
    except Exception as e:
        log(f"[worker] LAN copy failed to run: {e}")
        return False

    if result.returncode != 0:
        log(f"[worker] LAN copy unsuccessful ({direction} {model}): {(result.stdout or '').strip()} {(result.stderr or '').strip()}")
        return False

    log(f"[worker] LAN copy succeeded ({direction} {model}).")
    return True


# ZERO CPU spillover tolerated on Unraid -- Penn, 2026-08-28, in these exact
# words, after a dispatch was reported as "should clear, ~10% spillover,
# under threshold": "no spillover, at all, period." A percentage-threshold
# framing (this constant used to be SPILLOVER_ABORT_FRACTION = 0.15) reads
# as "up to 15% is fine," which is not the rule and got restated back to
# Penn as if it were -- the standing rule has always been zero, going back
# to "no. no cpu spillover on unraid." earlier this same session. The
# post-warmup check below now aborts on ANY measured spillover at all, no
# threshold, no epsilon -- size/size_vram from /api/ps are exact integer
# byte counts, not noisy floats, so there is nothing to buffer against.


def _host_usable_bytes(host: str) -> int | None:
    """usable_bytes for `host` from KNOWN_OLLAMA_HOSTS, matched by URL, or
    None if this isn't one of the two known hosts (an unrecognized/future
    host has no data to check against, so the spillover checks below just
    skip rather than block)."""
    for spec in KNOWN_OLLAMA_HOSTS.values():
        if spec["url"] == host:
            return spec["usable_bytes"]
    return None


def ensure_model_ready(host: str, model: str, temperature: float, num_ctx: int,
                        top_p: float = None, top_k: int = None, api_style: str = "ollama",
                        manual_tools: bool = False) -> None:
    """Confirm the model is pulled, then explicitly load it into memory with
    a generous timeout, fully separate from the main dispatch loop's
    request timeout. A cold model load (reading multi-GB weights off disk)
    can easily exceed a normal chat-request timeout on its own, before any
    real work even starts -- this was the actual cause of an earlier crash
    tonight (a plain 180s timeout on the very first request to a model
    that hadn't been loaded yet).

    Also enforces host/model fit -- added 2026-08-28 after dispatching
    gpt-oss:20b to Unraid with an explicit --host, which bypasses
    pick_host()'s fit check entirely (that check only runs when --host is
    omitted). Confirmed live: gpt-oss:20b's base weights (13.79GB) already
    exceed Unraid's 12GB card before any context is even added, and at
    --num-ctx 65536 it loaded at ~10GB VRAM / ~22.8GB total -- over half
    spilled to CPU. Penn: "we need hard rules programmed for model
    dispatching to prevent this. it keeps happening." Confirmed unconditional,
    no override -- Penn: "no. no cpu spillover on unraid." This is that hard
    rule, made non-bypassable by living here (every dispatch calls this,
    regardless of how --host was chosen) rather than only in pick_host()
    (which an explicit --host skips past), and by having no escape hatch at
    all. Two checks, both real data:
    pre-flight (the model's own advertised size vs. the host's usable
    budget, before even attempting a load) and post-warmup (the ACTUAL
    measured VRAM/total split from the host's own /api/ps, not an
    estimate) -- the second one is what would have caught this specific
    case, since the pre-flight size alone doesn't account for --num-ctx's
    contribution to the loaded footprint."""
    if api_style == "openai":
        # llama-server loads exactly one model, given via -m at process
        # startup -- there's no /api/tags-style discovery or /api/pull, and
        # nothing to warm up (the model is either already resident because
        # the server started successfully, or the server isn't up at all).
        log(f"[worker] checking llama-server is up at {host}...")
        req = urllib.request.Request(f"{host}/health")
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                resp.read()
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            # Connection refused/timeout = nothing is listening at all --
            # a different failure from a server that IS up but broken (that
            # one answers /health fine and gets caught by the tool-support
            # preflight below).
            if host == DEFAULT_OPENAI_HOST:
                raise RuntimeError(
                    f"Nothing is listening on {host} -- start it first: "
                    f"~/bin/start-llama-server-qwen3.8.sh"
                ) from e
            raise RuntimeError(
                f"could not connect to {host}/health ({e}) -- is a server "
                f"running there?"
            ) from e
        except Exception as e:
            raise RuntimeError(f"could not reach {host}/health: {e}") from e
        log(f"[worker] llama-server is up.")
        if not manual_tools:
            # Tool-support preflight (added 2026-08-28): a llama-server
            # started WITHOUT --jinja -- e.g. Ollama's own internal backend,
            # which runs with --no-jinja -- rejects ANY request carrying
            # `tools` with a deterministic 500:
            # {"error":{"message":"tools param requires --jinja flag",...}}.
            # /health alone can't catch that: the server is perfectly
            # healthy, it just can't do tool-calling. Confirmed live
            # 2026-08-28: a dispatch that grepped `ps aux` for "any"
            # llama-server grabbed Ollama's internal backend and only hit
            # this error mid-run, after real context had already been
            # spent. Send one minimal real request with the actual TOOLS
            # schema so this fails in seconds HERE, before run_task's main
            # loop ever starts. Skipped under --manual-tools: that path
            # never sends `tools` at all, so the check would be pointless
            # overhead.
            log(f"[worker] preflight: verifying {host} accepts tool-calling requests...")
            preflight_req = urllib.request.Request(
                f"{host}/v1/chat/completions",
                data=json.dumps({
                    "model": model,
                    "messages": [{"role": "user", "content": "ready"}],
                    "tools": TOOLS,
                    "stream": False,
                }).encode(),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                with urllib.request.urlopen(preflight_req, timeout=30) as resp:
                    resp.read()
            except urllib.error.HTTPError as e:
                try:
                    body = e.read().decode("utf-8", errors="replace")[:2000]
                except Exception:
                    body = "<could not read response body>"
                if e.code == 500:
                    # ANY 500 from this specific preflight call is treated
                    # as "no tool-calling support" -- the confirmed wording
                    # mentions --jinja, but we don't want a slightly
                    # different error string to slip past and surface
                    # mid-dispatch.
                    raise RuntimeError(
                        f"llama-server at {host} does not support tool-calling "
                        f"(preflight check failed: {body}). If this is Ollama's own "
                        f"internal backend, it was started without --jinja. Start the "
                        f"dedicated server instead: run "
                        f"~/bin/start-llama-server-qwen3.8.sh, then retry with "
                        f"--host {DEFAULT_OPENAI_HOST}."
                    ) from e
                raise RuntimeError(
                    f"llama-server at {host} rejected the tool-calling preflight "
                    f"request (HTTP {e.code}: {body})."
                ) from e
            except Exception as e:
                raise RuntimeError(
                    f"tool-calling preflight request to {host}/v1/chat/completions "
                    f"failed: {e}"
                ) from e
            log(f"[worker] preflight OK: {host} accepts tool-calling requests.")
        return

    # Hard rule, no override (same pattern as the Unraid-spillover rule
    # above): a TEMPLATE_BUG_MODELS model on Studio's native Ollama crashes
    # AND, if the dedicated bypass server for it is already resident, silently
    # double-loads the same multi-GB weights into unified memory a second
    # time -- confirmed live 2026-08-29, see TEMPLATE_BUG_MODELS' comment.
    # DEFAULT_OPENAI_HOST is always loopback, so it only ever collides with a
    # loopback native-Ollama host (Studio) -- Unraid's native Ollama is a
    # different physical machine and has no bypass server to collide with.
    if model in TEMPLATE_BUG_MODELS and host in ("http://127.0.0.1:11434", "http://localhost:11434"):
        raise RuntimeError(
            f"{model} is in TEMPLATE_BUG_MODELS -- Ollama's native /api/chat crashes it "
            f"(github.com/ollama/ollama/issues/17778) and routing it here risks double-"
            f"loading the same weights alongside the dedicated bypass server. Use "
            f"--api openai --host {DEFAULT_OPENAI_HOST} instead (start it first with "
            f"~/bin/start-llama-server-qwen3.8.sh if it isn't already up). No override."
        )

    log(f"[worker] checking {model} is pulled on {host}...")
    req = urllib.request.Request(f"{host}/api/tags")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            tags = json.loads(resp.read())
    except Exception as e:
        raise RuntimeError(f"could not reach {host}/api/tags: {e}") from e
    names = {m.get("name") for m in tags.get("models", [])}

    usable = _host_usable_bytes(host)
    if usable is not None:
        target = _normalize_model_name(model)
        entry = next((m for m in tags.get("models", []) if _normalize_model_name(m.get("name", "")) == target), None)
        base_size = entry.get("size") if entry else None
        if (base_size and base_size > usable
                and target not in UNRAID_SPILLOVER_EXCEPTIONS
                and model not in UNRAID_SPILLOVER_EXCEPTIONS):
            raise RuntimeError(
                f"{model} ({base_size/1e9:.1f}GB) already exceeds {host}'s usable budget "
                f"({usable/1e9:.1f}GB) from base weights alone, before any --num-ctx overhead. "
                f"This host cannot fit this model -- no override. Use pick_host()'s auto-selection "
                f"(omit --host) or pick a different host."
            )
        # A UNRAID_SPILLOVER_EXCEPTIONS model skips this conservative base-size
        # gate on purpose -- it's allowed to exceed usable_bytes here because
        # the real enforcement is the measured post-warmup spillover check
        # below, which is accurate where this pre-estimate is just a guess.

    # Ollama's /api/tags always qualifies a tag-less model with ":latest"
    # (e.g. "qwen3-14b-agentic" is listed as "qwen3-14b-agentic:latest"),
    # but a caller passing the bare name (no ":" at all) never matches that
    # literally -- confirmed live 2026-08-21: this false "not present"
    # triggered a real /api/pull against the public registry for a
    # locally-built custom model with no upstream equivalent, which 500'd
    # and crashed the whole dispatch. Normalize both sides to name:tag
    # (defaulting a missing tag to "latest") before comparing.
    normalized_names = {_normalize_model_name(n) for n in names if n}
    if _normalize_model_name(model) not in normalized_names:
        # Before ever pulling fresh from the public registry, try a LAN copy
        # first. This safeguard already existed in bakeoff-driver-buildoff.sh
        # (ensure_model_available()) but only for driver-orchestrated runs --
        # confirmed live 2026-08-22 that any ad-hoc direct dispatch (calling
        # this script by hand, not through a driver) skipped it entirely,
        # since the check lived in bash wrapping this function rather than in
        # the function itself. Moving it here makes it apply universally.
        if not _try_lan_copy(host, model):
            log(f"[worker] {model} not present locally or on the LAN -- pulling fresh from the registry (this can take a while for a large model)...")
            pull_req = urllib.request.Request(
                f"{host}/api/pull",
                data=json.dumps({"model": model, "stream": False}).encode(),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(pull_req, timeout=1800) as resp:
                resp.read()
            log(f"[worker] pull complete.")

    log(f"[worker] warming up {model} (loading into memory, up to {WARMUP_TIMEOUT_S}s)...")
    start = datetime.now(timezone.utc)
    call_ollama(host, model, [{"role": "user", "content": "ready"}], temperature, num_ctx,
                timeout=WARMUP_TIMEOUT_S, tools=False, top_p=top_p, top_k=top_k)
    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    log(f"[worker] {model} loaded and warm ({elapsed:.0f}s).")
    if any(h in host for h in UNRAID_OLLAMA_HOSTS):
        # Fable's suggestion, 2026-08-29 (Unraid CUDA-OOM investigation): the one real
        # VRAM signal available without SSH/nvidia-smi access to that box -- /api/ps
        # reports size_vram per resident model. Logging it here on every Unraid
        # dispatch means a future OOM investigation has real numbers on file instead
        # of having to guess at "what else might have been using VRAM" after the fact
        # (which is exactly the position this session was in tonight).
        try:
            req = urllib.request.Request(f"{host}/api/ps")
            with urllib.request.urlopen(req, timeout=15) as resp:
                ps = json.loads(resp.read())
            for m in ps.get("models", []):
                gb = (m.get("size_vram") or 0) / 1e9
                log(f"[worker] Unraid VRAM check: {m.get('name')} using {gb:.2f}GB")
        except Exception as e:
            log(f"[worker] Unraid VRAM check failed (non-fatal): {e}")

    try:
        req = urllib.request.Request(f"{host}/api/ps")
        with urllib.request.urlopen(req, timeout=15) as resp:
            ps = json.loads(resp.read())
        target = _normalize_model_name(model)
        entry = next((m for m in ps.get("models", []) if _normalize_model_name(m.get("name", "")) == target), None)
        if entry:
            total = entry.get("size") or 0
            vram = entry.get("size_vram") or 0
            if total > 0 and vram < total:
                spilled_frac = (total - vram) / total
                exception = UNRAID_SPILLOVER_EXCEPTIONS.get(_normalize_model_name(model)) or \
                    UNRAID_SPILLOVER_EXCEPTIONS.get(model)
                if exception is not None and spilled_frac <= exception["max_spill_frac"]:
                    log(f"[worker] {model} on {host} loaded with {(total - vram)/1e9:.2f}GB "
                        f"({spilled_frac*100:.1f}%) off-GPU -- within its documented "
                        f"UNRAID_SPILLOVER_EXCEPTIONS allowance ({exception['max_spill_frac']*100:.0f}%), "
                        f"proceeding (MoE expert-offload, not dense spillover).")
                else:
                    raise RuntimeError(
                        f"{model} on {host} loaded with {(total - vram)/1e9:.2f}GB "
                        f"({spilled_frac*100:.1f}%) off-GPU at --num-ctx {num_ctx} "
                        f"(total {total/1e9:.2f}GB, VRAM {vram/1e9:.2f}GB) -- ANY spillover aborts "
                        f"unless the model has a documented UNRAID_SPILLOVER_EXCEPTIONS entry it fits "
                        f"under (none does here). Aborting before spending real dispatch time on a "
                        f"degraded host. Lower --num-ctx, or use pick_host()'s auto-selection (omit --host)."
                    )
    except RuntimeError:
        raise
    except Exception as e:
            log(f"[worker] WARNING: spillover check against {host}/api/ps failed (non-fatal, proceeding): {e}")


# Process exit codes (main() does sys.exit(run_task(...))): 0 = converged,
# 1 = verify failed, 2 = did not converge (genuinely ran out of budget, and
# verify either wasn't given or didn't pass -- nothing here is confirmed good).
# 3 = paused for review -- the run stopped cleanly with a resumable transcript
# on disk and is NOT a failure. Covers BOTH pause sources: the model's own
# request_more_iterations / context-threshold review gates AND an external
# SIGTERM from the queue daemon's promote flow (see _sigterm_pause_handler).
# ollama-queue.py reads this constant off the imported worker module so the
# two can't drift apart. 4 = refused to start (dispatched outside the queue,
# see the OLLAMA_DISPATCH_VIA_QUEUE guard in main()). 5 = EXIT_CODE_DONE_UNCONVERGED
# (below) -- the mirror image of the vacuous-pass guard: did not converge, but
# verify PASSED, so real completed work exists despite the loop not exiting
# tidily. Distinct from plain 2 so a reviewer triaging by status doesn't
# discard a genuinely finished, verified change just because the model kept
# talking after finishing (github-projects-bf caught this live 2026-08-29:
# job nfc-check-section hit DID-NOT-CONVERGE after 30 iterations, but had
# already made 3 real edits and passed its own --verify; reported FAILED,
# nearly got discarded unread).
EXIT_CODE_PAUSED = 3
EXIT_CODE_DONE_UNCONVERGED = 5

# Set by the SIGTERM handler installed at the top of run_task: the queue
# daemon's promote flow (drop a pending job onto a running one in the
# dashboard) sends SIGTERM to pause a running dispatch gracefully instead of
# killing it -- finish the current iteration, save the transcript, exit with
# EXIT_CODE_PAUSED. Checked once per iteration, right after that iteration's
# incremental transcript save (see run_task's loop), so the transcript on
# disk is always complete through some whole iteration and resuming loses
# nothing.
_sigterm_pause_requested = False


def _sigterm_pause_handler(signum, frame):
    global _sigterm_pause_requested
    _sigterm_pause_requested = True
    log("[worker] SIGTERM received -- finishing the current iteration, then pausing "
        "gracefully (transcript saved, exit code 3; resume with --resume <transcript>).")


def _changed_file_count(cwd) -> int | None:
    """How many paths did this dispatch leave changed? None if unknowable.

    Recorded on every metrics row so a later question like "do multi-deliverable
    jobs cap out" has a size measure on the OUTPUT side, not just task_chars on
    the input side -- task_chars turned out to be a near-useless predictor
    (r=0.031 against iterations over 309 rows), and the number of files a job
    actually had to touch is the more plausible candidate.

    Union of tracked changes and untracked files, because a job whose whole
    output is new files would otherwise count as zero. Never raises: this runs
    on the metrics path, and metrics must never be able to fail a dispatch.
    """
    try:
        names = subprocess.run(["git", "diff", "--name-only", "HEAD"], cwd=cwd,
                                capture_output=True, text=True, timeout=15)
        if names.returncode != 0:
            return None
        paths = {l for l in names.stdout.splitlines() if l.strip()}
        unt = subprocess.run(["git", "ls-files", "--others", "--exclude-standard"],
                              cwd=cwd, capture_output=True, text=True, timeout=15)
        if unt.returncode == 0:
            paths |= {l for l in unt.stdout.splitlines() if l.strip()}
        return len(paths)
    except Exception:
        return None


def _git_worktree_snapshot(cwd) -> str:
    """Ground-truth "did anything in this working tree actually change" check for the
    vacuous-verify-pass guard (Fable ruling 2026-08-29, following a real miss
    github-projects-bf caught: a session that only explored via run_bash -- no edits,
    no diff -- still only WARNED under the first version of this guard, which inferred
    intent from tool-call counts instead of checking the tree itself). Returns None if
    cwd is not inside a git work tree at all (the guard falls back to the tool-call
    heuristic in that case); otherwise a string combining HEAD's rev, `git status
    --porcelain`, and `git diff HEAD`, taken together specifically because any one
    alone has a real gap the others cover:
      - porcelain alone: a file already modified BEFORE the dispatch started shows the
        identical status line even if the model edits it further -- false vacuous-fail
        on real work if compared start-vs-end with porcelain only.
      - status+diff alone (no rev): a model that COMMITS via run_bash leaves porcelain
        and diff-from-HEAD both clean even though the tree genuinely changed -- a moved
        HEAD is what proves that happened.
    Known residual gap, accepted rather than solved: editing an already-untracked file
    that was present before the dispatch started (its `??` line in porcelain doesn't
    change, and `git diff HEAD` doesn't see untracked content at all) is invisible to
    this check. Not worth a second, heavier mechanism (content hashing an unbounded
    tree) for what's a false-negative on an already-narrow guard, not a false-positive
    that would fail real work."""
    try:
        is_repo = subprocess.run(["git", "rev-parse", "--is-inside-work-tree"], cwd=cwd,
                                  capture_output=True, text=True, timeout=10)
        if is_repo.returncode != 0 or is_repo.stdout.strip() != "true":
            return None
        rev = subprocess.run(["git", "rev-parse", "HEAD"], cwd=cwd,
                              capture_output=True, text=True, timeout=10)
        rev_str = rev.stdout.strip() if rev.returncode == 0 else "NO_COMMITS_YET"
        status = subprocess.run(["git", "status", "--porcelain"], cwd=cwd,
                                 capture_output=True, text=True, timeout=15)
        diff_str = ""
        if rev.returncode == 0:
            diff = subprocess.run(["git", "diff", "HEAD"], cwd=cwd,
                                   capture_output=True, text=True, timeout=30)
            diff_str = diff.stdout if diff.returncode == 0 else ""
        return f"{rev_str}\n---STATUS---\n{status.stdout}\n---DIFF---\n{diff_str}"
    except Exception:
        return None


def _save_transcript(log_path: Path, model: str, host: str, cwd: Path, task: str,
                      converged: bool, iteration: int, messages: list,
                      pause_reason: str = None, pause_meta: dict = None,
                      worktree_start_snapshot: str = None,
                      baseline_verify_sig=None) -> None:
    """Write the current transcript state to log_path -- shared by the
    incremental per-iteration save and the final end-of-run save (see
    run_task's --resume support for why incremental saving exists). Same
    JSON shape either way, so a partially-written (paused) transcript and a
    finished one are both valid --resume input.

    pause_reason/pause_meta (added 2026-08-29, paired with ollama-queue.py's
    auto-resume watchdog): a machine-readable classification of WHY a paused
    run stopped, distinct from the free-text log line meant for a human. One
    of "context_threshold", "request_more_iterations", "external_sigterm",
    or None (not paused / converged / failed normally). Reading this back on
    resume lets the queue daemon decide whether it's even safe to auto-bump
    and retry (context/iteration shortfalls are), vs. an external_sigterm
    pause, which was someone's deliberate stop and must never be
    auto-resumed.

    worktree_start_snapshot (added 2026-08-29, Fable ruling on the vacuous-
    pass guard's git-diff discriminator): the working tree's state (see
    _git_worktree_snapshot) as of the ORIGINAL dispatch's start, persisted so
    a --resume'd session compares against the true start, not a fresh
    snapshot taken after the pause -- a fresh snapshot at resume time would
    wrongly read real pre-pause edits + post-resume inaction as "nothing
    changed"."""
    log_path.write_text(json.dumps({
        "model": model, "host": host, "cwd": str(cwd), "task": task,
        "converged": converged, "iterations": iteration, "messages": messages,
        "worktree_start_snapshot": worktree_start_snapshot,
        # Sets aren't JSON-serializable; store sorted for a stable, diff-friendly transcript.
        # Restored to a set on --resume (see run_task). None stays None = "no baseline".
        "baseline_verify_sig": (sorted(baseline_verify_sig)
                                 if baseline_verify_sig is not None else None),
        "pause_reason": pause_reason, "pause_meta": pause_meta or {},
    }, indent=2))


def _quick_verify(verify: str, cwd) -> tuple:
    """Lightweight in-loop verify check, used only to gate an early completion claim
    (the task_complete tool, or a silent no-tool-call final answer) before accepting
    it. Fable design 2026-08-29, built against real transcripts where a model
    declared itself done on objectively incomplete work and the harness had no way
    to notice until the run had already ended.

    Deliberately NOT the authoritative check -- the existing end-of-run verify
    block (with its vacuous-pass worktree-diff guard) still runs exactly once after
    the loop actually ends, regardless of how it ended, and its result is what
    decides the real exit code. This just answers "does verify pass right now,"
    cheaply enough to call a bounded few times per dispatch. Bounded by each call
    site's own claim/nudge cap, so worst case this doubles the verify command's
    cost for a dispatch that needed the retries -- acceptable next to burning the
    rest of the iteration budget on a task the model already believes is done.
    """
    try:
        result = subprocess.run(verify, shell=True, cwd=cwd, capture_output=True,
                                 text=True, timeout=300)
    except subprocess.TimeoutExpired:
        return False, "(verify command timed out after 300s)"
    if result.returncode in (126, 127):
        return False, (f"(verify command itself could not run, exit {result.returncode} -- "
                        f"check the --verify string)\n{result.stderr}")
    output = (result.stdout or "") + (("\n" + result.stderr) if result.stderr else "")
    return result.returncode == 0, output


# Diagnostic-line shapes for _verify_failure_signature. Kept module-level (compiled
# once) and deliberately SHAPE-ANCHORED: each requires a source location or an explicit
# error/failure token, so aggregate/summary/timing lines ("Found 168 errors", "117
# tests / 3 failed", "duration_ms 101") never match. See the helper's docstring for why
# that asymmetry is the safe one.
_VERIFY_DIAG_RES = (
    re.compile(r'\(\d+,\s*\d+\):\s*(error|warning)\b', re.I),   # tsc:   path(line,col): error TSxxxx
    re.compile(r':\d+:(\d+:)?\s*(error|warning)\b', re.I),      # gcc/eslint: path:line[:col]: error ...
    re.compile(r'\b(error|warning)\b.*\bTS\d+\b', re.I),        # TS diagnostic code anywhere on the line
    re.compile(r'^(FAIL|✕|✗|×|not ok|AssertionError|Traceback)\b'),  # common test-runner failure markers
    # Additions per Fable review 2026-08-30 -- the set above missed several real
    # failure shapes, which (combined with the empty-signature fix below) would have
    # let an unrecognized NEW failure read as "no new failures" and false-accept:
    re.compile(r'^FAILED\b'),                        # pytest: FAILED tests/x.py::y  (\bFAIL rejects FAILED)
    re.compile(r'^--- FAIL\b'),                      # go test: --- FAIL: TestX
    re.compile(r'error\[E\d+\]'),                    # rust: error[E0308]: mismatched types
    re.compile(r'^\S+\.\w{1,4}:\d+:\d+:'),           # go/generic compile: path.go:12:5: undefined: foo
)


def _verify_failure_signature(output: str) -> set:
    """Normalize a --verify command's OUTPUT into a set of per-defect 'signature' lines,
    so two runs can be diffed to isolate the failures a model's diff actually INTRODUCED
    from failures already present at dispatch start (the baseline).

    Added 2026-08-30 after a real convergence failure (resell-tracker): a --verify of
    `npx tsc --noEmit && npm test` returned 168 errors on the UNEDITED worktree -- a
    missing `prisma generate` left every DB result typed `any`. The dispatched model made
    its two CORRECT edits, which left all 168 pre-existing errors in place, and the harness
    fed the whole 168-line dump back as 'fix the reported problems': undirectable noise in
    files the model never opened. It correctly refused to touch them and looped
    task_complete to the cap, then reported FAILED. Diffing current-vs-baseline turns that
    into 'you introduced 0 new failures', which is directable (accept) instead of noise.

    Normalization keeps only DIAGNOSTIC-looking lines (individual compiler errors and
    test-failure markers, per _VERIFY_DIAG_RES) and drops aggregate/summary/timing lines.
    Rationale for that specific asymmetry: a single diagnostic is deterministic and
    location-anchored, so set-difference isolates exactly the diagnostics attributable to
    the diff; aggregate lines are per-RUN not per-defect and drift with counts (168 vs
    165), so including them would flag a pure REDUCTION in errors as a spurious 'new' line.
    A line matching no known diagnostic shape is DROPPED rather than guessed at -- the
    failure modes are not symmetric: a false 'no new failures' is still caught downstream
    by the end-of-run authoritative verify (which continues to fail on real regressions
    whose lines we happened not to recognize), whereas a false 'new failure' would
    re-introduce exactly the undirectable-noise problem this helper exists to remove."""
    sig = set()
    for raw in (output or "").splitlines():
        line = raw.strip()
        if line and any(r.search(line) for r in _VERIFY_DIAG_RES):
            sig.add(line)
    return sig


def _verify_delta_feedback(verify: str, cwd, baseline_sig):
    """Run the verify once and classify the result against the persisted baseline
    signature. Returns (passed, new_failures, preexisting_present, current_recognized, raw_output):
      passed              -- verify exited 0 (no failures at all)
      new_failures        -- sorted list of diagnostic lines present now but NOT at
                             baseline (i.e. attributable to the model's diff)
      preexisting_present -- True if any current failure was already in the baseline
      current_recognized  -- True if the CURRENT failing output produced at least one
                             recognized diagnostic line. Critical for the no-regression
                             decision (Fable review 2026-08-30): a failing verify whose
                             lines match no regex yields an EMPTY current signature, which
                             must NOT be read as 'no new failures' -- callers require this
                             True before accepting a BASELINE-BROKEN no-regression run, and
                             otherwise fall back to raw-output feedback.
      raw_output          -- the verify command's combined stdout+stderr

    When baseline_sig is None (no baseline captured -- a verify that couldn't run at start,
    a legacy resumed transcript, or a non-coding task) every current failure counts as 'new'
    and current_recognized reflects only what we could parse; callers treat None-baseline as
    'cannot classify' and fall back to the old raw-output behavior."""
    ok, out = _quick_verify(verify, cwd)
    if ok:
        return True, [], False, False, out
    current = _verify_failure_signature(out)
    if baseline_sig is None:
        return False, sorted(current), False, bool(current), out
    new = current - baseline_sig
    preexisting = bool(current & baseline_sig)
    return False, sorted(new), preexisting, bool(current), out


# --- Anti-thrash guard for re-issued identical READ-ONLY tool calls -------------
# Motivating failure (Penn 2026-09-08): a diagnosis dispatch re-issued near-identical
# run_bash greps against the SAME files ("Let me grep app/bfmr/page.tsx..." fired
# repeatedly), burning context going in circles. The existing loop-detect (below, in
# the tool loop) only SOFT-nudges at 3/6/9 and still RE-RUNS the command every time,
# paying full tool-output cost per repeat. This guard is complementary: on a repeat of
# an identical read-only call whose result we already have, hand back the CACHED result
# annotated ("you already ran this; act on it") WITHOUT re-running the tool, and after
# ANTI_THRASH_STRONG_AFTER repeats also emit a stronger nudge to change approach.
ANTI_THRASH_STRONG_AFTER = 3  # identical read-only repeats before escalating the nudge


def _anti_thrash_intercept(sig, cacheable, result_cache, repeat_counts):
    """Intercept a re-issued identical read-only tool call.

    `cacheable` is the caller's decision that this (name,args) is a read-only inspection
    safe to serve from cache (read_file/list_files, or a run_bash local-read -- never a
    mutation, and never the job's own verify). Returns (cached_result_or_None, nudge_or_None):
    a non-None cached_result means "short-circuit -- do NOT run the tool, use this instead".
    Mutates repeat_counts so the escalation fires once it crosses the threshold.
    """
    if not cacheable or sig not in result_cache:
        return None, None
    n = repeat_counts.get(sig, 0) + 1
    repeat_counts[sig] = n
    annotated = (
        str(result_cache[sig])
        + f"\n\n[NOTE: you already ran this exact command earlier (repeat #{n}); the "
          f"result is unchanged -- do not repeat it, act on it.]"
    )
    nudge = None
    if n >= ANTI_THRASH_STRONG_AFTER:
        nudge = (
            f"You have now re-issued the identical read-only call {n} times and its result "
            f"has not changed. STOP repeating it -- either use what you already have to "
            f"produce your final output/file NOW, or take a genuinely different action (a "
            f"different command, a different file, or a different approach)."
        )
    return annotated, nudge


# --- Resume-time transcript compaction (Penn 2026-09-08) ------------------------
# A job that paused at ~92% context leaves a resume= transcript, but resuming INTO a
# ~92%-full window can't make progress -- the pre-send projection re-pauses almost
# immediately, so a plain resume is inert. (The queue's auto-resume bumps num_ctx
# UPWARD, but that has a host ceiling.) Compaction makes the resumed run start with
# real headroom: keep the task spec + verify identity + the most-recent tool results,
# and replace the earlier exploration with a compact summary of what was already done.
RESUME_COMPACT_TARGET = 0.70    # after compaction, aim below this fraction of num_ctx
RESUME_COMPACT_KEEP_TAIL = 8    # most-recent messages kept verbatim (the model needs these)
RESUME_COMPACT_SUMMARY_CAP = 4000  # max chars of the exploration summary


def _estimate_transcript_tokens(messages):
    """Same bytes/4 heuristic the pre-send accumulation guard uses (kept in sync by
    value with that inline `len(text) // 4`)."""
    return sum(len(str(m.get("content") or "")) for m in messages) // 4


def _summarize_exploration(dropped, cap=RESUME_COMPACT_SUMMARY_CAP):
    """Collapse the dropped middle of a transcript into ONE compact user message that
    lists what was already explored (tool calls + truncated results), so the resumed
    model does not repeat those reads. Returns a message dict, or None when there is
    nothing to summarize."""
    if not dropped:
        return None
    lines = []
    for msg in dropped:
        role = msg.get("role")
        content = str(msg.get("content") or "").strip().replace("\n", " ")
        for tc in (msg.get("tool_calls") or []):
            fn = tc.get("function", {}) if isinstance(tc, dict) else {}
            lines.append(f"- called {fn.get('name')}({str(fn.get('arguments'))[:120]})")
        if not content:
            continue
        if role == "assistant":
            lines.append(f"- reasoned: {content[:160]}")
        else:  # tool / user (tool results in either message shape)
            lines.append(f"- result: {content[:160]}")
    body = "\n".join(lines)
    if len(body) > cap:
        body = body[:cap] + "\n- ...(earlier exploration truncated)"
    return {"role": "user", "content":
            "[CONTEXT COMPACTED ON RESUME] Earlier exploration in this run was summarized to "
            "free up context. You have ALREADY done the following -- do NOT repeat these "
            "reads/searches, act on what you found:\n" + body +
            "\n\nProceed to produce your final output/file now."}


def _compact_resumed_transcript(messages, num_ctx,
                                review_threshold=CONTEXT_REVIEW_THRESHOLD,
                                target=RESUME_COMPACT_TARGET,
                                keep_tail=RESUME_COMPACT_KEEP_TAIL):
    """Compact a paused transcript so a resume starts with headroom.

    Returns (new_messages, compacted, before_tokens, after_tokens). Leaves the transcript
    byte-for-byte UNCHANGED (compacted=False) when num_ctx is unknown or the transcript
    already fits comfortably below review_threshold -- so a resume that has room is never
    disturbed. When it is near/over the ceiling, preserve the system prompt (msg 0) and the
    original task (first user message) VERBATIM -- the task spec, target-file identity and
    verify live there and must never be dropped -- keep the most-recent `keep_tail` messages
    verbatim, and replace everything between with one summary. If a single kept message is
    itself huge, trim the tail (never below the last result) until under `target`.
    """
    if not num_ctx:
        return messages, False, 0, 0
    before = _estimate_transcript_tokens(messages)
    if before < num_ctx * review_threshold:
        return messages, False, before, before
    rest = list(messages)
    head = []
    if rest and rest[0].get("role") == "system":
        head.append(rest.pop(0))
    if rest and rest[0].get("role") == "user":
        head.append(rest.pop(0))  # the original task spec (verify + target identity)
    tail = rest[-keep_tail:] if keep_tail > 0 else []
    middle = rest[:-keep_tail] if keep_tail > 0 else rest
    summary = _summarize_exploration(middle)
    new_messages = head + ([summary] if summary else []) + tail
    after = _estimate_transcript_tokens(new_messages)
    while after >= num_ctx * target and len(tail) > 1:
        tail = tail[1:]
        new_messages = head + ([summary] if summary else []) + tail
        after = _estimate_transcript_tokens(new_messages)
    return new_messages, True, before, after


def _context_budget_nudges(total_tokens_used, num_ctx, already_fired,
                           thresholds=CONTEXT_NUDGE_THRESHOLDS,
                           review_threshold=CONTEXT_REVIEW_THRESHOLD):
    """Proactive mid-run context-budget nudges (see CONTEXT_NUDGE_THRESHOLDS).

    Returns a list of (threshold, message) for each budget threshold NEWLY crossed
    this iteration, and mutates `already_fired` (a set) so each threshold fires at most
    once per run. Only thresholds strictly below review_threshold are considered -- the
    0.90 pause supersedes the top nudge. Pure (no I/O) so it is unit-testable and proves
    red-on-revert.
    """
    out = []
    if not num_ctx:
        return out
    for thr in thresholds:
        if thr >= review_threshold:
            continue
        if total_tokens_used >= num_ctx * thr and thr not in already_fired:
            already_fired.add(thr)
            pct = total_tokens_used / num_ctx
            out.append((thr,
                f"[Context budget: you are at {pct:.0%} of your context window "
                f"({total_tokens_used}/{num_ctx} tokens). You have limited room left -- "
                f"converge and produce your final output/file NOW rather than exploring "
                f"further. Do NOT re-read files you have already seen; act on what you have.]"))
    return out


def run_task(model, host, cwd, task, verify, max_iters, temperature, num_ctx, searxng_host,
             system_prompt_file=None, cleanup_after=False, manual_tools=False,
             top_p=None, top_k=None, api_style="ollama", claude_prep_tokens=None,
             resume_from=None, task_kind="coding", chat_timeout=CHAT_TIMEOUT_S,
             max_tokens=DEFAULT_MAX_TOKENS, repeat_penalty=DEFAULT_REPEAT_PENALTY,
             facts_provided=False, web_fetch_max_chars=None, read_file_max_chars=None,
             verify_failed_at_baseline=False, scored_arm=False, num_ctx_bumps=0,
             min_web_fetches=0,
             live_log=None, dispatch_tag=None, think=None):
    cwd = Path(cwd).resolve()
    cwd.mkdir(parents=True, exist_ok=True)
    num_ctx = clamp_unraid_ctx(host, model, num_ctx)
    tool_impls = build_tool_impls(searxng_host, web_fetch_max_chars=web_fetch_max_chars,
                                  read_file_max_chars=read_file_max_chars, num_ctx=num_ctx,
                                  verify=verify)

    # External graceful pause support -- see _sigterm_pause_handler above. Installed here,
    # BEFORE model warmup (which can take minutes on a cold load), so a SIGTERM arriving any
    # time after this point pauses the run instead of killing it with the default handler.
    # The flag is honored at the end of each completed iteration in the loop below.
    signal.signal(signal.SIGTERM, _sigterm_pause_handler)

    # Advisory only (Fable review, 2026-08-29) -- a negative-grep verify guard
    # ('! grep ... pattern') can't distinguish a forbidden command being EXECUTED
    # from that same text merely being printed/echoed/commented (confirmed real
    # incident the same day: a guard against editing /etc/pam.d failed correct
    # work that only PRINTED the command for a human to run manually). Not
    # something the harness can safely auto-correct -- just flag it so whoever's
    # reading the log notices before trusting a FAILED verdict from one of these.
    if verify and re.match(r"^\s*!\s*grep", verify):
        log(f"[worker] NOTE: --verify looks like a negative-grep guard ({verify!r}) -- these "
            f"can't distinguish a forbidden pattern being EXECUTED from it merely being "
            f"echoed/printed/commented. If this fails, check whether the match is real before "
            f"trusting VERIFY FAILED.")

    # --resume: added 2026-08-28 after killing an in-progress dispatch
    # (the research test) to free the host for an urgent fix and losing all
    # its progress -- Ollama's inference is stateless per-request (the full
    # message history gets resent every call), so the dispatch's real state
    # is just this `messages` list, not something living inside the model.
    # Saving it incrementally (below) and reloading it here is the whole
    # mechanism: unload model A mid-task, load model B for something urgent,
    # unload B, reload A, resume A's messages and let it finish -- no
    # progress lost. Model/host/task are still taken fresh from the CLI args
    # (not read back out of the saved file), so resuming with a DIFFERENT
    # model than started the task is a deliberate, supported case, not an
    # error -- that's the actual pause-work-resume workflow this exists for.
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    if resume_from:
        resume_path = Path(resume_from).resolve()
        saved = json.loads(resume_path.read_text())
        messages = saved["messages"]
        resumed_at_iteration = saved.get("iterations", 0)
        log_path = resume_path  # keep updating the same file across pause/resume cycles
        # Preserve the FULL restored transcript for the vacuous-pass counter reconstruction
        # below (it scans messages for OK:wrote / exit_code markers): compaction may drop the
        # middle where those markers live, so the counters must be rebuilt from the original.
        _resume_full_messages = list(messages)
        # Resume-time compaction (see _compact_resumed_transcript): a resume into a near-full
        # window is otherwise inert. No-op when the transcript already has room.
        messages, _compacted, _c_before, _c_after = _compact_resumed_transcript(messages, num_ctx)
        if _compacted:
            log(f"[worker] resume compaction: transcript ~{_c_before} tok "
                f"({_c_before / num_ctx:.0%}) -> ~{_c_after} tok ({_c_after / num_ctx:.0%}) of "
                f"{num_ctx} -- kept the task/verify + recent results, summarized earlier "
                f"exploration so the resumed run starts with headroom.")
        log_path = resume_path
        log(f"[worker] resuming from {resume_path} (was at iteration {resumed_at_iteration}, "
            f"{len(messages)} messages) -- now dispatching to {model} on {host}")
        if manual_tools:
            log("[worker] --manual-tools: native tool_calls bypassed, using textual tool-schema "
                "injection + our own response parsing instead (see render_manual_tools_block).")
    else:
        system_prompt = RESEARCH_SYSTEM_PROMPT if task_kind == "research" else SYSTEM_PROMPT
        if system_prompt_file:
            system_prompt = Path(system_prompt_file).read_text()
            log(f"[worker] using custom system prompt from {system_prompt_file}")
        elif task_kind == "research":
            log("[worker] --task-kind research: using RESEARCH_SYSTEM_PROMPT (not the coding one).")
        if manual_tools:
            system_prompt = system_prompt + "\n\n" + render_manual_tools_block(TOOLS)
            log("[worker] --manual-tools: native tool_calls bypassed, using textual tool-schema "
                "injection + our own response parsing instead (see render_manual_tools_block).")
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": task},
        ]
        resumed_at_iteration = 0
        _resume_full_messages = None  # no pre-pause history on a fresh run
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        log_path = LOG_DIR / f"{ts}.json"

    log(f"[worker] model={model} host={host} cwd={cwd}")
    log(f"[worker] task: {task}")

    # Opt-in live log (--live-log): when set, the native-Ollama-tools path
    # below switches to call_ollama_streaming and appends tagged, colored
    # status lines to this file IN ADDITION to the normal log() output
    # above. When not set, live stays None and every code path below is
    # exactly what it was before this feature existed.
    live = None
    if live_log:
        live = LiveLog(live_log, dispatch_tag or Path(cwd).name, model, host)

    # Populated as the loop runs, written out on success only (see the
    # `if converged:` gate below) via write_dispatch_metrics -- see that
    # function's docstring and DISPATCH_METRICS_PATH above for why this
    # exists. `claude_prep_tokens` is a separate, deliberately distinct
    # figure from everything else here: Claude's own output-token cost
    # (investigation, writing the task spec) for GETTING to this dispatch,
    # captured by the caller via claude-token-cursor.py before/after and
    # passed straight through -- kept apart from sum_completion_tokens
    # (the local model's generation cost) so the two are directly
    # comparable: which side actually burned more tokens on this task.
    # LOUD, because a silent budget change makes two runs look comparable when
    # they are not: the queue watchdog can raise a job's context between attempts,
    # so the number a run actually had must appear in its own log, not only in the
    # metrics file someone may never open.
    if num_ctx_bumps:
        log(f"[worker] CONTEXT BUDGET: running at num_ctx={num_ctx} after "
            f"{num_ctx_bumps} watchdog bump(s). Cost is reported, not penalised -- "
            f"but do NOT compare this run's iteration/token counts against a run "
            f"at a different budget without saying so.")
    else:
        log(f"[worker] CONTEXT BUDGET: num_ctx={num_ctx}, no bumps.")
    # Batch #8 (2026-09-02). A SCORED BAKE-OFF ARM is staged from a verify PROVEN to
    # fail at baseline, so for it the baseline failures ARE the task. Two consequences,
    # one flag: (1) it implies verify_failed_at_baseline -- "proven failing at stage" is
    # a stronger statement than the queue pre-flight's observation, and it must hold even
    # when the arm was fired without that flag; (2) baseline-diagnostic SUBTRACTION is
    # switched off, because subtracting the baseline here hides the only diagnostics that
    # matter and lets an untouched bug read as "no new failures". LOUD, because a scored
    # arm silently graded under the lenient rule produces a number nobody can trust.
    _baseline_is_the_task = bool(verify_failed_at_baseline or scored_arm)
    if scored_arm:
        log("[worker] SCORED ARM: baseline-diagnostic subtraction is OFF and a still-failing "
            "verify can NEVER be accepted -- the baseline failures ARE the task. Do not "
            "compare this run against an unscored dispatch.")
    elif verify_failed_at_baseline:
        log("[worker] BASELINE PROVEN FAILING at enqueue: a still-failing verify cannot show "
            "the fix landed, so it is fed back as work to do, not accepted.")
    _dispatch_metrics.clear()
    _dispatch_metrics.update({
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "model": model, "host": host, "api_style": api_style,
        "configured_num_ctx": num_ctx,
        # How many times the queue's auto-resume watchdog raised this job's
        # context before this run. Penn's framing 2026-09-02: resource cost is
        # REPORTED DATA, never a penalty -- "solved but needed 131072 and 3 bumps"
        # is a deployment fact worth having, and equalising budgets would measure
        # a model we had crippled rather than the one we would deploy. Kept as a
        # first-class metric so a scorer can report cost alongside capability.
        "num_ctx_bumps": num_ctx_bumps,
        # Whether this run was a scored bake-off arm (batch #8). A scored arm is graded
        # under the strict rule above; an unscored dispatch is not. Recorded so the two
        # are never pooled by a scorer reading this file.
        "scored_arm": bool(scored_arm),
        "task_preview": task[:200] + ("..." if len(task) > 200 else ""),
        "task_chars": len(task),
        "claude_prep_tokens": claude_prep_tokens,
        "iterations": 0, "calls": 0,
        "peak_prompt_tokens": 0, "peak_total_tokens": 0, "sum_completion_tokens": 0,
        "verify_passed": None, "status": "running",
    })
    # (path, offset) -> times read. Same-page re-reads are SUCCESSES, so the
    # existing failure-keyed loop detector cannot see them.
    _repeat_reads: dict = {}
    _dispatch_start = datetime.now(timezone.utc)

    # Only manage the local cache when dispatching against this Mac's own
    # Ollama (Unraid's has its own independent GPU + storage, nothing to
    # copy there). Detected by host, not hardcoded to localhost only, so
    # a Mac Studio reached by LAN IP still gets cache management.
    is_local_ollama = api_style == "ollama" and host in ("http://localhost:11434", "http://127.0.0.1:11434")
    if is_local_ollama:
        ensure_model_cached(model)
    ensure_model_ready(host, model, temperature, num_ctx, top_p=top_p, top_k=top_k,
                       api_style=api_style, manual_tools=manual_tools)

    converged = False
    final_summary = None  # set by task_complete; falls back to `content` if never set
    completion_claims = 0
    MAX_COMPLETION_CLAIMS = 3  # after this many failed verify-gated claims, accept the
    # model's word rather than looping the claim forever -- same bounded-not-unlimited
    # philosophy as MAX_NUDGES below.
    completion_verify_nudges = 0
    MAX_COMPLETION_VERIFY_NUDGES = 2  # same idea for the silent (no-tool-call) path below
    repeated_failures = {}   # "tool:args" -> consecutive identical failures
    repeated_calls = {}      # "tool:args" -> consecutive identical calls, success or not
    _tool_result_cache = {}  # "tool:args" -> last result for a cacheable read-only call
    _thrash_repeats = {}     # "tool:args" -> times we served this from cache (anti-thrash)
    _ctx_nudge_fired = set()  # context-budget thresholds already nudged (fire once each)
    loop_break_notes = []    # corrective guidance to deliver next turn
    nudge_count = 0
    MAX_NUDGES = 3  # hard ceiling regardless of progress -- see below for the
    # condition that governs whether a nudge under that ceiling is actually sent.
    # Added 2026-08-28 (Penn's request): let a model ask for more iterations itself, via the
    # request_more_iterations tool, instead of the only recovery being a human noticing
    # DID-NOT-CONVERGE after the fact. Penn's call: this is a review gate, not an auto-grant --
    # setting this to a non-None reason string cleanly pauses the whole dispatch (both loops),
    # same as running out of iterations, so it's resumable via the exact same `--resume` flow
    # already proven live tonight. See its two setters below (request_more_iterations, and the
    # context-usage threshold check) for what can trigger it.
    paused_for_review = None
    pause_reason_code = None
    pause_meta = None
    # Vacuous-verify-pass guard state (added 2026-08-29, Fable GO-with-conditions):
    # a --verify command can pass trivially on a file the model never touched (e.g.
    # `bash -n script.sh` on an untouched script) -- confirmed real, 2026-08-29,
    # a model whose tool calls came through as unparsed text made zero edits and
    # still got VERIFY PASSED. Tracked separately from run_bash success because a
    # model that makes its edits via run_bash (heredoc/sed/git apply) instead of
    # write_file/edit_file is legitimate and must not be penalized the same way as
    # one that made no tool calls that did anything at all -- see the tiered check
    # at verify-evaluation time below.
    _files_modified_count = 0
    _run_bash_success_count = 0
    # Reconstruct both counters from any RESUMED history -- --resume only restores
    # `messages`/`iterations` (see above), every other session flag starts fresh, so
    # without this a dispatch that did real work, paused, and resumed would run
    # verify against a counter that forgot everything before the pause and could
    # wrongly override a legitimate PASS to FAILED. Two message shapes to match,
    # not one: role:"tool" stores the raw result string directly (native/openai
    # paths), but the manual-tools/textual-fallback path stores it as
    # role:"user", content=f"[tool result for {name}]: {result}" (see the
    # tool-result-append `if manual_call_this_turn: ... elif api_style ==
    # "openai": ... else:` branch below) -- searching for the marker anchored to
    # either start-of-string OR right after "]: " covers both without needing to
    # know which path produced any given message.
    _FILE_MOD_RE = re.compile(r'(?:^|\]: )(?:OK: wrote|OK: replaced)')
    _RUN_BASH_OK_RE = re.compile(r'(?:^|\]: )\{"exit_code"')
    # Scan the FULL restored transcript, not the possibly-compacted `messages`: resume
    # compaction (above) may have summarized away the middle where OK:wrote / exit_code
    # markers live, and this reconstruction must still see every pre-pause edit or the
    # vacuous-pass guard would forget real work and false-fail a legitimate completion.
    for _m in (_resume_full_messages if _resume_full_messages is not None else messages):
        _c = str(_m.get("content", ""))
        if _FILE_MOD_RE.search(_c):
            _files_modified_count += 1
        elif _RUN_BASH_OK_RE.search(_c):
            _run_bash_success_count += 1

    # Working-tree snapshot at the ORIGINAL dispatch's start (Fable ruling 2026-08-29,
    # replacing run_bash-success-count as the vacuous-pass guard's primary signal when
    # cwd is a git repo -- see _git_worktree_snapshot's own docstring for why). On
    # --resume this MUST be the true start, not a fresh snapshot taken now: real edits
    # made before a pause plus inaction after resuming would otherwise read as "nothing
    # changed" and false-fail legitimate completed work, the exact counter-amnesia
    # problem already solved above for _files_modified_count/_run_bash_success_count.
    if resume_from:
        _worktree_start_snapshot = saved.get("worktree_start_snapshot")
        if _worktree_start_snapshot is None:
            log("[worker] resumed transcript predates the vacuous-pass worktree-diff "
                "guard -- taking a fresh snapshot now instead of the true dispatch "
                "start (edits made before this resume won't count toward the diff "
                "check this session; the tool-call counters above still cover them).")
            _worktree_start_snapshot = _git_worktree_snapshot(cwd)
    else:
        _worktree_start_snapshot = _git_worktree_snapshot(cwd)

    # Baseline verify-failure signature at the ORIGINAL dispatch's start (added 2026-08-30,
    # same convergence incident as _verify_failure_signature). Captured ONCE, before the
    # model touches anything, so every later verify run can be diffed against it to feed the
    # model only the failures ITS diff caused (see the two completion gates and the end-of-run
    # verify below). Persisted across --resume exactly like _worktree_start_snapshot and for
    # the identical counter-amnesia reason: re-running the verify at resume time would capture
    # a baseline that already contains the model's pre-pause edits, so pre-pause work would
    # wrongly count as pre-existing (masking a real regression) -- the baseline must be the
    # TRUE start. Stored as a sorted list in JSON (sets aren't JSON-serializable); restored to
    # a set. None means "no baseline available" -> _verify_delta_feedback treats every failure
    # as new, i.e. exactly the old pre-baseline behavior. Only meaningful for coding tasks with
    # a --verify; a passing baseline yields the empty set (no pre-existing failures to subtract).
    _baseline_verify_sig = None
    if verify and task_kind == "coding":
        if resume_from:
            _saved_bl = saved.get("baseline_verify_sig")
            if _saved_bl is not None:
                _baseline_verify_sig = set(_saved_bl)
            else:
                log("[worker] resumed transcript predates the baseline-verify-delta guard -- "
                    "no start-of-dispatch baseline available, so this session's verify feedback "
                    "falls back to the full raw output (pre-existing failures can't be subtracted "
                    "from a baseline that was never recorded).")
        else:
            _bl_ok, _bl_out = _quick_verify(verify, cwd)
            if _bl_ok:
                _baseline_verify_sig = set()
                log("[worker] baseline verify PASSED at dispatch start -- any later failure is "
                    "attributable to the model's work.")
            else:
                _baseline_verify_sig = _verify_failure_signature(_bl_out)
                log(f"[worker] baseline verify FAILED at dispatch start with "
                    f"{len(_baseline_verify_sig)} pre-existing diagnostic(s) -- these will be "
                    f"subtracted from later verify runs so the model is only asked to fix what its "
                    f"own diff introduces. (A large count here usually means the worktree wasn't "
                    f"fully provisioned, e.g. a missing codegen/`prisma generate` step.)")

    empty_answer_nudge_sent = False
    tool_called_since_last_nudge = True  # starts True so the first nudge is
    # always allowed regardless of history, matching the original "exactly one
    # nudge" design's unconditional first attempt.
    # For coding tasks: specifically write_file/edit_file, not any tool --
    # confirmed live 2026-08-22 (qwen3.8:27b-q8_0, resell-tracker-photo-upload): a
    # model can do 18 iterations of pure read_file/run_bash exploration, narrate a
    # full implementation as prose on iteration 19, get cut off mid-sentence with no
    # tool call, and stop -- because the old `any_tool_called` flag was set True by
    # the very first read_file/run_bash call in iteration 1, permanently disarming
    # the corrective-nudge safeguard below for the rest of the run. Tracking mutation
    # calls specifically (not any tool call) is what the nudge actually needs to mean
    # "the model has made real progress," not "the model has done anything at all."
    #
    # task_kind gates what "real progress" means, added 2026-08-28: confirmed live
    # (qwen3:8b, twice, identically) that this write_file/edit_file-only definition
    # is a coding-task assumption that doesn't fit research tasks at all -- a research
    # dispatch's deliverable IS the final text response, there's no file to save, so
    # this condition was structurally impossible to satisfy and every research
    # dispatch got at least one guaranteed false-positive nudge, even after doing
    # real web_fetch-verified work and correctly concluding. In both traced cases the
    # model complied with the nudge's suggested action literally -- calling write_file
    # to save its (already-written, not yet source-verified) answer to a file it was
    # never asked to produce -- actively steering it toward the wrong action rather
    # than just failing to help. For task_kind="research", ANY tool call at all counts
    # as real progress (there's no equivalent "narrated but never saved" failure mode
    # to guard against when the deliverable is text, not a file).
    any_mutation_called = False
    if resume_from and (_files_modified_count > 0 or _run_bash_success_count > 0):
        # Fable finding 2026-08-29: --resume only restores messages/iterations (see
        # _save_transcript's own docstring), so any_mutation_called reset to False on
        # every resume regardless of real pre-pause progress -- confirmed live, this
        # produced a false "you have not made real progress on the task yet" nudge
        # after a resume that already had 2 successful edits, which then drove the
        # model into a no-op re-edit loop. _files_modified_count/_run_bash_success_count
        # are already correctly reconstructed from message history above; reuse that
        # instead of adding a third, separately-fragile reconstruction path.
        any_mutation_called = True
    # Confirmed live 2026-08-28 (llama3.1:8b, and qwen3:8b earlier tonight):
    # a research dispatch can converge on a confident final answer with
    # specific fabricated numbers after every single web_fetch call failed,
    # directly against an explicit "don't state facts from a snippet alone"
    # instruction. Track whether ANY web_fetch this dispatch has ever
    # actually succeeded so the convergence check below can catch this
    # before accepting the answer -- see its use near "no tool calls in
    # response" below. fabrication_nudge_sent bounds it to exactly one nudge
    # (same bounded-not-looping philosophy as nudge_count/MAX_NUDGES above).
    web_fetch_succeeded = False
    web_fetch_success_count = 0
    # Count genuine LOCAL-source verification (read_file/list_files/read-only run_bash
    # like grep/cat/find) so the end-of-run unverified-provenance warning can tell two
    # cases apart, added 2026-09-06 (false-signal fix, job 8d690b764cec): a research
    # dispatch whose real sources are local repo files correctly makes ZERO web_fetch
    # calls, and stamping "claims of verification can't be trusted" on it is a FALSE
    # signal -- it DID verify, just not over the web. The warning is reserved for a
    # research answer that shows NO verification of ANY kind (zero web_fetch AND zero
    # local reads). Like web_fetch_succeeded above, this is NOT reconstructed on
    # --resume (matching that flag's existing semantics), so both share the same
    # resume blind spot rather than introducing a new asymmetry.
    local_read_count = 0
    # Confirmed live 2026-08-28 (Electrify America research): the grounding checks below used
    # to scrape ALL tool-role messages for their grounding source, which silently included
    # web_search snippets alongside real web_fetch content -- exactly the "snippet, not a
    # confirmed source" distinction the system prompt itself warns about. A model cited a real
    # number ("328 stations") that only ever appeared in an unrelated site's SEARCH SNIPPET,
    # attributed to a URL that actually 404'd, and the grounding check passed it because the
    # snippet text was in the same pool as real fetches. Track only genuine successful
    # web_fetch results here, at the one place that already knows which tool call this is, so
    # grounding can never be satisfied by a snippet again.
    real_fetched_texts = []
    min_fetches_nudge_count = 0
    MAX_MIN_FETCH_NUDGES = 3  # bounded, not unlimited -- see the nudge site below for why
    fabrication_nudge_sent = False
    # 2026-08-22 (devstral:24b, resell-tracker-photo-upload): the original single
    # nudge worked -- it produced a real tool call on the very next turn -- but the
    # model then relapsed into narration two iterations later with no nudges left to
    # correct it. That's meaningfully different from opencode's infinite self-nudge
    # bug (which re-injected the same generic message forever regardless of whether
    # the model ever responded to it): here each nudge was demonstrably producing
    # real forward progress, just not durably. The `tool_called_since_last_nudge`
    # gate is what preserves the original safety property -- a model that ignores a
    # nudge outright (zero tool calls afterward) does NOT get another one, so a
    # truly stuck model still stops after one unproductive nudge, same as before.
    # Only a model demonstrating it's actually listening gets the extra budget, and
    # even that is hard-capped at MAX_NUDGES so this can never become unbounded.
    # --resume: iteration numbering continues from where the saved transcript
    # left off rather than restarting at 1 -- the file already had
    # resumed_at_iteration iterations before this session, so max_iters here
    # means "how many MORE iterations are allowed this session", not a reset
    # of the total count. On a fresh run resumed_at_iteration is 0 and this
    # is exactly the original range(1, max_iters + 1).
    total_iters = resumed_at_iteration + max_iters
    for i in range(resumed_at_iteration + 1, total_iters + 1):
        log(f"[worker] --- iteration {i}/{total_iters} ---")
        if live is not None:
            live.iteration(i, total_iters)
        _dispatch_metrics["iterations"] = i

        # EARLY NON-CONVERGENCE ABORT (2026-09-10). A goalless run -- typically a
        # coding dispatch enqueued with no --verify (now gated at enqueue in
        # ollama-queue.py; kept here as defence in depth) -- thrashes: it re-issues
        # the same reads, the anti-thrash cache serves them, and it grinds to
        # max_iters with a ZERO diff. Job d31d96d23b29 (shipped-flip) did exactly
        # that: 29 cache-serves, 55/55 iterations, empty diff. Once the thrash is
        # unambiguous (past an iteration floor + many accumulated cache-serves) AND
        # the working tree is STILL byte-for-byte unchanged since dispatch start,
        # bail instead of burning the rest of the budget. The tree-unchanged check
        # is LAST so its git snapshot only runs once the cheap thrash signal has
        # already tripped; the conjunction with "unchanged tree" means this can
        # never fire on a run that has actually edited anything.
        if (i > 12 and _worktree_start_snapshot is not None
                and (max(_thrash_repeats.values(), default=0) >= 10
                     or sum(_thrash_repeats.values()) >= 15)
                and _git_worktree_snapshot(cwd) == _worktree_start_snapshot):
            log(f"[worker] EARLY ABORT at iteration {i}/{total_iters}: "
                f"{sum(_thrash_repeats.values())} anti-thrash cache-serves "
                f"(top signature x{max(_thrash_repeats.values(), default=0)}) and the "
                f"working tree is STILL unchanged since dispatch start -- non-productive "
                f"thrash, not converging. Stopping to save GPU rather than grinding to "
                f"max_iters. (A no-verify dispatch has no goal signal to converge to; "
                f"attach a --verify.)")
            _dispatch_metrics["early_abort"] = "thrash_zero_diff"
            break

        # PRE-SEND ACCUMULATION GUARD. With read_file capped, run_bash at
        # 4000+4000 and web_fetch at 5000, no SINGLE tool result can blow the
        # window any more -- the residual risk is the transcript growing past
        # it across many iterations. The existing context check runs on the
        # usage the API reports AFTER a call, which cannot help when the
        # request itself is already over: that request fails or silently
        # truncates, and the pause lands too late.
        #
        # So project the prompt size BEFORE sending (the same bytes/4 heuristic
        # used at dispatch time) and, if it is about to exceed the window, take
        # the EXISTING context_threshold pause path -- resumable with a bigger
        # --num-ctx -- rather than sending a doomed request. Warn-and-pause,
        # never a hard refusal: the run is recoverable, and pausing keeps the
        # transcript intact for the resume.
        if num_ctx and not paused_for_review:
            _projected = sum(len(str(_m.get("content") or "")) for _m in messages) // 4
            if _projected >= num_ctx * PRESEND_CONTEXT_LIMIT:
                paused_for_review = (
                    f"projected prompt ~{_projected} tokens vs {num_ctx} context "
                    f"({_projected / num_ctx:.0%}) -- pausing BEFORE sending a request "
                    f"that would not fit")
                pause_reason_code = "context_threshold"
                pause_meta = {"tokens_used": _projected, "num_ctx": num_ctx,
                              "detected": "pre-send projection"}
                log(f"[worker] PAUSED FOR REVIEW: {paused_for_review}. Resume with "
                    f"--resume {log_path} --num-ctx <bigger>.")
                break

        _call_started = time.monotonic()
        try:
            if live is not None and api_style == "ollama" and not manual_tools:
                # Streaming path, native Ollama tools only (see
                # call_ollama_streaming's docstring for scope). Returns the same
                # normalized {"message": {...}, "usage": {...}} shape as
                # call_ollama, so nothing downstream changes.
                resp = call_ollama_streaming(host, model, messages, temperature, num_ctx,
                                             timeout=chat_timeout, tools=not manual_tools,
                                             top_p=top_p, top_k=top_k, live=live,
                                             max_tokens=max_tokens, repeat_penalty=repeat_penalty,
                                             think=think)
            else:
                resp = call_ollama(host, model, messages, temperature, num_ctx, timeout=chat_timeout,
                                    tools=not manual_tools, top_p=top_p, top_k=top_k, api_style=api_style,
                                    max_tokens=max_tokens, repeat_penalty=repeat_penalty, think=think)
        except RuntimeError as chat_err:
            # A chat request that exhausted all its retries (a transient Ollama HTTP
            # 500 / template-parse "XML syntax error" / network drop mid-run) used to
            # propagate out of run_task and crash main() with exit 1 -- discarding a
            # fully checkpointed, resumable transcript AND any partial worktree edits.
            # Observed live 2026-08-30: an Ollama 500 at iteration 7 threw away an
            # ~80%-done fix. Treat it like a graceful pause instead: fall through to
            # paused_for_review's existing save/exit path so the transcript is saved
            # and the process exits RESUMABLE (code 3), letting `--resume` (or the
            # queue's auto-resume for transient errors) pick up from here rather than
            # losing the work. NOT a verify failure -- the model's work so far stands.
            log(f"[worker] CHAT REQUEST FAILED at iteration {i}/{total_iters}: {chat_err}")
            log("[worker] pausing with a resumable transcript (exit code 3) instead of "
                "hard-failing -- resume with --resume <transcript> to retry from here.")
            paused_for_review = f"transient chat failure: {chat_err}"
            pause_reason_code = "chat_request_failed"
            pause_meta = {}
            break
        _call_elapsed = time.monotonic() - _call_started
        usage = resp.get("usage") or {}
        _dispatch_metrics["calls"] += 1
        _dispatch_metrics["peak_prompt_tokens"] = max(_dispatch_metrics["peak_prompt_tokens"], usage.get("prompt_tokens") or 0)
        _dispatch_metrics["peak_total_tokens"] = max(_dispatch_metrics["peak_total_tokens"], usage.get("total_tokens") or 0)
        _dispatch_metrics["sum_completion_tokens"] += usage.get("completion_tokens") or 0
        # Added 2026-08-28 (Penn: "can we put tok/s ... on the dashboard?") -- the
        # queue-tool API parses this line out of the job's log file to show live
        # throughput. completion_tokens is generation only (excludes prompt
        # processing), matching how tok/s is normally reported for LLM inference.
        _completion_tok = usage.get("completion_tokens") or 0
        if _call_elapsed > 0 and _completion_tok:
            log(f"[worker] iteration {i}/{total_iters} generated {_completion_tok} tokens "
                f"in {_call_elapsed:.1f}s ({_completion_tok / _call_elapsed:.1f} tok/s)")
        msg = resp.get("message", {})
        messages.append(msg)

        # Context-usage counterpart to request_more_iterations, added same day at Penn's
        # request ("can we do the same for context?"): a model can't self-report running low
        # on context the way it can ask for more iterations, since it doesn't see its own
        # token accounting -- so this is harness-driven instead, checked every turn against
        # the objective usage the API already returns. Same review-gate shape: pause cleanly
        # (both loops) the moment usage crosses the threshold, resumable via `--resume
        # <transcript> --num-ctx <bigger>` once reviewed, rather than silently continuing
        # toward an actual overflow/degraded-quality response or a hard API failure.
        tool_calls = msg.get("tool_calls") or []
        content = (msg.get("content") or "").strip()
        if content:
            log(f"[worker] model: {content[:500]}")

        manual_call_this_turn = False
        if not tool_calls and content:
            # Universal fallback, not gated behind --manual-tools: confirmed
            # live 2026-08-21 that qwen2.5-coder:14b has a CORRECT Ollama
            # template (proper <tools> schema injection, explicit
            # instruction to respond with <tool_call>...</tool_call> and no
            # backticks) but the model itself still sometimes ignores that
            # format and wraps the same call in a ```json fence instead --
            # Ollama's native parser only recognizes the <tool_call> tag
            # form, so tool_calls came back empty even though the model's
            # intent was clearly a real tool call. extract_manual_tool_calls
            # is tag-agnostic (finds the JSON object regardless of
            # wrapper), so it catches this for ANY model as a safety net,
            # not just the templateless models --manual-tools exists for.
            parsed_calls, _cleaned_content = extract_manual_tool_calls(content)
            if parsed_calls:
                manual_call_this_turn = True
                tool_calls = [{"function": {"name": p.get("name"), "arguments": p.get("arguments", {})}}
                              for p in parsed_calls]
                log(f"[worker] fallback-parse: recovered {len(tool_calls)} tool call(s) that "
                    f"native tool_calls missed -- {[tc['function']['name'] for tc in tool_calls]}")
                if _cleaned_content != content:
                    # Break the lock-in (2026-08-29, Fable root-cause via
                    # github-projects-bf): rewrite the ALREADY-STORED assistant
                    # message in place -- messages.append(msg) above holds a
                    # reference to this same dict, not a copy, so mutating it
                    # here updates what the model sees on every future turn.
                    # Without this, a malformed XML call goes back into context
                    # verbatim and the model imitates its own prior formatting
                    # on the next response -- confirmed live: every one of
                    # these failures was malformed from iteration 1 and never
                    # recovered on its own.
                    msg["content"] = _cleaned_content
                    content = _cleaned_content
                    log("[worker] fallback-parse: rewrote the stored assistant message to drop "
                        "the salvaged XML call, so the model doesn't imitate its own malformed "
                        "formatting on the next turn.")

        if not tool_calls:
            # Confirmed live 2026-08-21 (devstral:24b): a model can narrate
            # code in a fenced block instead of calling write_file, even
            # with an explicit system-prompt instruction not to. Give
            # exactly ONE corrective nudge if the response looks like a
            # narrated/summarized non-answer instead of real tool use --
            # bounded, not a loop, structurally different from opencode's
            # confirmed infinite self-nudge bug (that one re-injected a
            # generic "continue" with no new information forever; this
            # injects a specific correction once).
            #
            # Originally only fired on a code fence with no tool call, but
            # confirmed live 2026-08-21 (qwen3-coder-next) that a model can
            # also just write a plain-English summary of the files it read
            # (no fence at all) and stop having done zero edits -- same
            # underlying failure (treating description as the deliverable),
            # so the trigger is now "no mutation has happened yet at all" (see
            # any_mutation_called above for why read-only tool calls don't count).
            # Fable gap-fix 2026-08-29 (Penn: rv6-control-checklist-r1 "said done but is on
            # iteration 40/41"): the mutation-count nudge below fires when any_mutation_called
            # is False -- but that flag only counts write_file/edit_file, so a task whose
            # deliverable is written via a run_bash heredoc (the review-bench pattern: the model
            # `cat > REVIEW.md <<EOF`'d a complete 17KB review, zero write_file calls) reads as
            # "no progress" and gets nudged away from a CORRECT completion, three times, burning
            # the budget. When a coding task has a --verify command, that command is a strictly
            # better "is it actually done" signal than the mutation counter -- so defer to the
            # verify-based silence gate below (which runs verify and either converges on a pass
            # or gives a SPECIFIC verify-failure nudge) instead of firing this generic one. The
            # generic nudge stays the only safeguard when there's no verify to consult.
            defer_to_verify_gate = task_kind == "coding" and bool(verify)
            if (nudge_count < MAX_NUDGES and tool_called_since_last_nudge
                    and not any_mutation_called and not defer_to_verify_gate):
                nudge_count += 1
                tool_called_since_last_nudge = False
                log(f"[worker] no tool call yet and none made this response -- "
                    f"sending corrective nudge {nudge_count}/{MAX_NUDGES} instead of "
                    f"accepting it as final.")
                # Confirmed live 2026-08-28 (qwen3:8b, twice, identically): this
                # nudge originally hardcoded "e.g. write_file" as the example
                # action, which is a CODING-task assumption baked into a
                # mechanism meant to apply to every dispatch. On a research
                # task, the model responded to the nudge by literally calling
                # write_file to save its already-written (and unverified/
                # fabricated) answer to a file -- technically satisfying the
                # nudge's narrow check ("was a tool called") while doing
                # nothing to fix the actual problem (it still hadn't fetched
                # a source to back its claims). The nudge was steering toward
                # the wrong action, not just failing to help. Reworded to be
                # task-type-agnostic: point back at what the task itself
                # asked for, and name a read tool (web_fetch) as an equally
                # valid example alongside a write tool, so a research
                # dispatch isn't nudged toward writing a file it was never
                # asked to write.
                if "</tool_call>" in content or "<function=" in content:
                    # Fable's question #3 (2026-08-29, via github-projects-bf): when the
                    # content shows signs of a malformed tool-call attempt that salvage
                    # couldn't fully recover (a dangling </tool_call>, or a <function=...>
                    # whose name didn't validate), the generic nudge above is unactionable
                    # -- the model believes it DID make a call, so "call a tool" doesn't
                    # tell it anything new. Quote the exact expected format, INCLUDING the
                    # opening tag it's apparently dropping, instead.
                    messages.append({
                        "role": "user",
                        "content": "Your last response looks like an attempted tool call that "
                                   "wasn't recognized -- check that you opened it correctly. "
                                   "The exact format is:\n<tool_call>\n<function=NAME>\n"
                                   "<parameter=KEY>\nVALUE\n</parameter>\n</function>\n"
                                   "</tool_call>\nMake sure <tool_call> opens the block -- a "
                                   "response with only the closing </tool_call> and no opener "
                                   "is not recognized as a real call.",
                    })
                else:
                    messages.append({
                        "role": "user",
                        "content": "You have not made real progress on the task yet -- "
                                   "describing, printing, or summarizing does not count as "
                                   "doing the work. Call whichever tool actually advances what "
                                   "the task asked for -- that might be write_file/edit_file if "
                                   "the task wants a code change, or web_fetch to verify a claim "
                                   "before stating it, or another tool entirely, depending on "
                                   "what THIS task needs. Do not call a tool just to satisfy this "
                                   "message if it doesn't genuinely move the task forward.",
                    })
                continue
            # Confirmed live 2026-08-28 (llama3.1:8b, EV-charging-network research, several
            # consecutive attempts): a model can find exactly the right pages via web_search
            # repeatedly and just never call web_fetch on them, converging on snippet-only
            # claims every time even with iteration budget to spare. A ONE-TIME nudge here
            # proved genuinely insufficient in practice -- confirmed live: a Tesla research
            # dispatch got nudged once at 1/3 required fetches, made exactly one more real
            # fetch attempt, then gave up and was accepted at 1/3 anyway. Penn's standing
            # instruction: when Unraid research comes back incomplete, the mechanism should be
            # strengthened and retried, not just reported thin. Bounded to MAX_MIN_FETCH_NUDGES
            # (matching the file's other bounded-nudge conventions) instead of exactly one, so
            # the requirement actually has teeth without being literally unbounded.
            if (task_kind == "research" and min_web_fetches > 0
                    and web_fetch_success_count < min_web_fetches
                    and min_fetches_nudge_count < MAX_MIN_FETCH_NUDGES):
                min_fetches_nudge_count += 1
                log(f"[worker] research task converging with only {web_fetch_success_count}/"
                    f"{min_web_fetches} required successful web_fetch calls -- sending "
                    f"corrective nudge {min_fetches_nudge_count}/{MAX_MIN_FETCH_NUDGES} before "
                    f"accepting the answer.")
                messages.append({
                    "role": "user",
                    "content": f"You have successfully fetched {web_fetch_success_count} real "
                               f"page(s) so far, but this task requires at least "
                               f"{min_web_fetches}. Search results alone are not enough -- "
                               f"call web_fetch on real URLs (ones you haven't already "
                               f"successfully fetched) until you reach that minimum, then give "
                               f"your final answer. If a specific site keeps failing, try a "
                               f"genuinely different site, not a reworded search for the same one.",
                })
                continue
            if (task_kind == "research" and not web_fetch_succeeded
                    and not fabrication_nudge_sent and not facts_provided):
                fabrication_nudge_sent = True
                log("[worker] research task converging with zero successful web_fetch calls -- "
                    "sending one corrective nudge before accepting the answer.")
                messages.append({
                    "role": "user",
                    "content": "Every web_fetch call in this session has failed -- you have not "
                               "actually read a real source. Any specific number, date, name, or "
                               "other concrete detail in your answer so far is UNVERIFIED, not a "
                               "confirmed fact. Rewrite your final answer: for each item you cannot "
                               "trace back to real fetched content, say plainly you could not "
                               "confirm it (name what you would check next) instead of presenting "
                               "it as a settled fact with a confidence label. Do not fabricate a "
                               "number just to fill in the answer.",
                })
                continue
            # Confirmed live 2026-08-28 (qwen3:8b-tuned llama3.1:8b via a review fork,
            # EV-charging-network discovery): the branch above only guards against ZERO
            # successful fetches -- a model that makes real, successful fetches can still state
            # specific numbers that don't appear anywhere in what it actually fetched (two real
            # Wikipedia fetches; the specific station-count figures it then stated were
            # confirmed absent from both). Genuine collect-mode research had no equivalent of
            # facts-provided mode's grounding check. Reuses the same find_ungrounded_numeric_
            # claims helper, but against only the fetched content this session (not task text,
            # which for real research is the question, not a source of facts).
            if (task_kind == "research" and web_fetch_succeeded and not facts_provided
                    and not fabrication_nudge_sent):
                ungrounded_collect = find_ungrounded_numeric_claims(
                    content, "\n".join(real_fetched_texts))
                if ungrounded_collect:
                    fabrication_nudge_sent = True
                    log(f"[worker] research answer (real fetches happened) contains "
                        f"{len(ungrounded_collect)} numeric/time claim(s) not found in "
                        f"anything actually fetched this session -- sending one corrective "
                        f"nudge before accepting the answer: {ungrounded_collect}")
                    messages.append({
                        "role": "user",
                        "content": "Your answer states the following specific figures that do "
                                   "NOT appear anywhere in the content you actually fetched: "
                                   + "; ".join(ungrounded_collect) + ". These look fabricated -- "
                                   "having made a real fetch elsewhere doesn't make an unrelated "
                                   "invented number acceptable. Rewrite your final answer: for "
                                   "each one, either point to exactly which fetched source it "
                                   "comes from, or replace it with an explicit 'could not "
                                   "confirm this figure from what I fetched' statement. Do not "
                                   "invent a number to fill the gap.",
                    })
                    continue
            # facts-provided mode has no web_fetch signal to check (zero fetches is
            # expected by design), so it needs its own grounding check instead of the
            # branch above -- see find_ungrounded_numeric_claims for why this exists.
            if (task_kind == "research" and facts_provided and not fabrication_nudge_sent):
                # Confirmed live 2026-08-28 (llama3.1:8b, NV-Energy retry): the task text
                # explicitly permits "at most one targeted fetch" if a supplied fact proves
                # insufficient -- a model that does exactly that and gets real new content
                # was still flagged, because the grounding source was only the ORIGINAL
                # facts, not anything legitimately fetched this session. Include every tool
                # result's content too, so a real verified fetch counts as grounding.
                ungrounded = find_ungrounded_numeric_claims(
                    content, task + "\n" + "\n".join(real_fetched_texts))
                if ungrounded:
                    fabrication_nudge_sent = True
                    log(f"[worker] facts-provided research answer contains "
                        f"{len(ungrounded)} numeric/time claim(s) not found in the "
                        f"supplied facts -- sending one corrective nudge before "
                        f"accepting the answer: {ungrounded}")
                    messages.append({
                        "role": "user",
                        "content": "Your answer states the following specific figures that do "
                                   "NOT appear anywhere in the facts you were given: "
                                   + "; ".join(ungrounded) + ". These look fabricated. Rewrite "
                                   "your final answer: for each one, either point to exactly "
                                   "where in the provided facts it comes from, or replace it "
                                   "with an explicit 'could not confirm this figure from the "
                                   "provided facts' statement. Do not invent a number to fill "
                                   "the gap.",
                    })
                    continue
            # Confirmed live 2026-08-28 (qwen3.5:9b, NV-Energy round 2): a model can stop with
            # BOTH zero tool calls AND empty content -- the checks above all key off `content`
            # having something in it (nudge text, fabrication claims), so a genuinely blank
            # response sailed through everything and got accepted as "the final answer" with
            # nothing in it at all. Catch this explicitly before accepting anything.
            if not content.strip() and not empty_answer_nudge_sent:
                empty_answer_nudge_sent = True
                log("[worker] response has no tool calls AND no content -- sending one "
                    "corrective nudge instead of accepting a blank final answer.")
                messages.append({
                    "role": "user",
                    "content": "Your last response was empty -- no tool call, no text. Either "
                               "call a tool to keep working, or write out your actual final "
                               "answer now. An empty response is not acceptable as the final "
                               "answer.",
                })
                continue
            # Confirmed live 2026-08-28 (llama3.1:8b, EV-charging-network discovery dispatch):
            # the fabrication nudge above is a ONE-TIME correction, and a model can respond to
            # it by making more unsuccessful/absent web_fetch attempts and then simply CLAIM
            # verification it never had -- this run's final answer stated a specific figure was
            # "verified through web_fetch" when zero web_fetch calls succeeded anywhere in the
            # entire session (confirmed directly against the raw transcript: every tool result
            # was either a web_search snippet or a web_fetch ERROR). Prompting alone clearly
            # isn't reliable here, so this is an unconditional, harness-level warning appended
            # after the fact -- it doesn't depend on the model being honest about its own
            # process, only on the objective, harness-tracked fact of whether a fetch ever
            # actually succeeded.
            # facts_provided excluded: confirmed live 2026-08-28 (EV-charging pass-2 dispatch)
            # that this fired on a genuinely honest, correctly-sourced answer synthesized from
            # facts real-fetched in a PRIOR pass -- zero fetches THIS session is expected and
            # correct there by design, not a red flag, and facts-provided mode already has its
            # own real grounding check (find_ungrounded_numeric_claims above) for this exact
            # concern. This banner is for genuine collect-mode research with no verification at
            # all, not for pass-2 synthesis correctly skipping a fetch it was told not to do.
            # local_read_count gate added 2026-09-06 (false-signal fix, job 8d690b764cec):
            # the old condition was `not web_fetch_succeeded` ALONE, which stamped this
            # unverified warning on EVERY research dispatch that made zero web_fetch calls
            # -- including the large, legitimate class whose real sources are LOCAL repo
            # files (trace a call path, find why X is gated on Y, locate a perf hotspot).
            # Those tasks correctly use read_file/grep and correctly make zero web_fetch
            # calls, and stamping "claims can't be verified" on them is a FALSE signal: the
            # answer WAS verified, just against the repo instead of the web. The warning is
            # for the case it was actually built for -- a research answer that shows NO
            # verification of ANY kind. So require BOTH zero web_fetch AND zero local reads:
            # a task that read/grepped the tree has shown genuine local verification and is
            # exempt; a task that fetched nothing AND read nothing local (the real "confident
            # answer, no sources touched" failure) is still flagged, web-research included.
            if should_stamp_unverified(task_kind, web_fetch_succeeded,
                                       local_read_count, facts_provided):
                warning = ("\n\n---\nHARNESS WARNING: no web_fetch call succeeded anywhere in "
                           "this session. Any claim above of having 'verified' or 'confirmed' a "
                           "detail is NOT reliable, regardless of what the text above says -- "
                           "treat every specific fact in this answer as unconfirmed until it is "
                           "checked against a source that was actually, successfully fetched.")
                msg["content"] = (msg.get("content") or "") + warning
                log("[worker] research task converged with zero successful web_fetch calls AND "
                    "zero local reads all session -- appending unconditional harness warning "
                    "(no verification of any kind; the model's claims can't be trusted).")
            elif (task_kind == "research" and not web_fetch_succeeded
                    and local_read_count > 0 and not facts_provided):
                log(f"[worker] research task converged with zero web_fetch but "
                    f"{local_read_count} local read(s) -- NOT stamping the unverified warning "
                    f"(local-source verification is genuine; web-provenance expectation "
                    f"does not apply to a local-repo investigation).")
            # Same task_complete verify-gate as above, applied to the silent path -- a model
            # that just stops (no tool call at all) gets the same chance to see WHY it isn't
            # done instead of the harness accepting it blind and finding out only at the
            # authoritative end-of-run verify. Bounded (MAX_COMPLETION_VERIFY_NUDGES) so a
            # task whose verify can never be satisfied still converges eventually, same as
            # the model's own word being accepted after MAX_COMPLETION_CLAIMS above.
            if (task_kind == "coding" and verify
                    and completion_verify_nudges < MAX_COMPLETION_VERIFY_NUDGES):
                # Same baseline-delta classification (and same soundness gates) as the
                # task_complete gate above -- see there for the conditions' full rationale.
                v_ok, new_failures, preexisting, current_recognized, v_out = (
                    _verify_delta_feedback(verify, cwd, _baseline_verify_sig))
                _did_work = (_files_modified_count > 0 or _run_bash_success_count > 0)
                _no_regression = (bool(_baseline_verify_sig) and current_recognized
                                   and not new_failures)
                if not v_ok and (_baseline_is_the_task or not (_no_regression and _did_work)):
                    # Something to fix: either new failures from this diff, or an unclassifiable
                    # failure (unrecognized lines / no baseline / no evidence of work). Nudge with
                    # the most directive content available -- new failures if we isolated them,
                    # else the raw output rather than a false "you're done".
                    #
                    # Batch #7/#8: _baseline_is_the_task forces the nudge even on a clean
                    # no-regression reading. This silent-stop path had the SAME false-accept as
                    # the task_complete gate -- a model that simply stopped talking, on a verify
                    # that was failing by design before it started, was accepted on "0 new
                    # failures", which is precisely what an untouched bug produces. It also
                    # forces the RAW output (no baseline subtraction): the pre-existing failures
                    # ARE the task, so omitting them would leave the nudge with nothing to say.
                    completion_verify_nudges += 1
                    _shown = ("\n".join(new_failures)[-3000:]
                              if new_failures and not _baseline_is_the_task else v_out[-3000:])
                    _pre_note = ("" if (_baseline_is_the_task
                                        or not (new_failures and preexisting)) else
                                 "\n(Other failures in the output PRE-EXIST your changes and are "
                                 "NOT yours to fix -- they are omitted here.)")
                    if _baseline_is_the_task:
                        _pre_note = ("\n(This verify was ALREADY FAILING before you started -- by "
                                     "design. Those pre-existing failures ARE the bug you were "
                                     "asked to fix; you are not done until this command PASSES.)")
                    log(f"[worker] silent final answer given but verify still fails "
                        f"({len(new_failures)} new / classifiable={bool(new_failures)}) -- nudge "
                        f"{completion_verify_nudges}/{MAX_COMPLETION_VERIFY_NUDGES}, feeding back "
                        f"instead of accepting.")
                    messages.append({
                        "role": "user",
                        "content": f"You stopped without calling a tool, but the verify still "
                                   f"fails:\n{_shown}{_pre_note}\nFix the reported problem(s), then "
                                   f"call task_complete (or stop again) once the verify passes.",
                    })
                    continue
                elif not v_ok:
                    log("[worker] silent final answer given, verify fails but ONLY on pre-existing "
                        "baseline failures (0 new from this diff) and the model did real work -- "
                        "accepting, not nudging on noise it can't fix (BASELINE-BROKEN).")
            converged = True
            log("[worker] no tool calls in response -- treating as final answer, stopping.")
            break

        tool_called_since_last_nudge = True
        for tc in tool_calls:
            fn = tc.get("function", {})
            name = fn.get("name")
            if task_kind == "research" or name in ("write_file", "edit_file"):
                any_mutation_called = True
            raw_args = fn.get("arguments", {})
            args = raw_args if isinstance(raw_args, dict) else json.loads(raw_args or "{}")
            if live is not None:
                live.tool_call(name, args)
            tool_call_id = tc.get("id")

            if name == "request_more_iterations":
                # Special-cased here rather than in tool_impls/build_tool_impls: it needs to
                # stop the whole dispatch, not just return a tool result. Penn's call
                # 2026-08-28: this is a REVIEW GATE, not an auto-grant or a wait-with-timeout --
                # no in-process polling at all. The request pauses the dispatch cleanly (same
                # incremental-save mechanism that already makes every DID-NOT-CONVERGE stop
                # resumable), and a human/Claude reviews it on their own time, then resumes with
                # `--resume <transcript> --max-iters N` -- reusing the exact resume flow already
                # proven live tonight (the queue-tool-build resume), rather than inventing a new
                # side-channel. No response is recorded for this call since the dispatch stops
                # before there's anywhere to deliver one -- resuming re-adds the same tool result
                # naturally via the next model turn once it's given more budget.
                requested = args.get("additional")
                reason = args.get("reason", "")
                requested = requested if isinstance(requested, int) and requested > 0 else 0
                paused_for_review = (
                    f"model requested +{requested} more iterations (currently at {i}/"
                    f"{total_iters}), reason: {reason!r}"
                )
                pause_reason_code = "request_more_iterations"
                pause_meta = {"requested_additional": requested, "reason": reason}
                log(f"[worker] PAUSED FOR REVIEW: {paused_for_review}. To grant, resume with: "
                    f"--resume {log_path} --max-iters <N> (in addition to the same other flags "
                    f"this dispatch used). To deny, just don't -- the transcript stays as-is.")
                break

            sig = f"{name}:{json.dumps(args, sort_keys=True)}"
            if name == "run_bash" and re.match(r"^\s*echo\b", str(args.get("command", ""))):
                # Fable finding 2026-08-29: a model trying to signal "I'm done" through the
                # only channel it reliably uses (a tool call, not silence) tends to reach for
                # a ritual `run_bash echo "..."` -- confirmed live, 18 consecutive iterations,
                # varying the echoed text just enough (checkmarks, phrasing) that the exact-
                # args signature below never repeated identically and loop-detect never fired.
                # Collapse every echo-only run_bash call to one signature regardless of what it
                # echoes, so this pattern actually trips the existing 3x loop-detect guidance
                # instead of evading it for the entire run.
                sig = "run_bash:<echo>"
            # A model re-reading the SAME page of the same file is the specific
            # loop this paging change can create: page 1 answers the question
            # "what's in this file" badly, so it asks again identically instead
            # of advancing the offset. The existing loop-detect only counts
            # FAILED calls, and a read that returns page 1 is a success every
            # time -- so it would never fire. Count successful same-page reads
            # separately and hand back the arithmetic rather than a scolding.
            if name == "read_file" and isinstance(args, dict):
                _rk = (str(args.get("path")), str(args.get("offset") or 1))
                _repeat_reads[_rk] = _repeat_reads.get(_rk, 0) + 1
            # Is this the job's OWN verify command? (Re-running it after every edit is
            # exactly what a converge-on-verify task instructs -- never treat that as a
            # loop, and never serve it from the anti-thrash cache.) Computed here so the
            # anti-thrash intercept just below and the loop-detect further down share it.
            _is_own_verify = False
            if verify and name == "run_bash" and isinstance(args, dict):
                _c = " ".join(str(args.get("command", "")).split())
                _v = " ".join(str(verify).split())
                _is_own_verify = _c == _v or _c.startswith(_v)
            # A read-only inspection whose repeat is safe to serve from cache: reads and
            # listings always, a run_bash only when it is a local read (grep/cat/find/...),
            # never a mutation and never the verify.
            _thrash_cacheable = (not _is_own_verify) and (
                name in ("read_file", "list_files")
                or (name == "run_bash" and isinstance(args, dict)
                    and _command_is_local_read(str(args.get("command", "")))))
            _thrash_cached, _thrash_nudge = _anti_thrash_intercept(
                sig, _thrash_cacheable, _tool_result_cache, _thrash_repeats)
            impl = tool_impls.get(name)
            # SILENT ARG-DROPPING IS WHAT MADE THE read_file BUG INVISIBLE. The
            # model asked for offset/length, the schema declared neither, and
            # the harness executed the call anyway as though the arguments had
            # never been sent -- so the model saw a successful read and had no
            # way to learn its request was ignored. It repeated the call. Any
            # argument the schema does not declare now comes back as a visible
            # note appended to the result, never as a refusal: the call still
            # runs, because rejecting it would break working dispatches over a
            # stray key, but the model is told what was ignored.
            _unknown = sorted(set(args) - _tool_arg_names(name)) if isinstance(args, dict) else []
            if repeated_failures.get(sig, 0) >= 3:
                # HARD BLOCK. Warning alone was not enough: confirmed live
                # 2026-08-22 (qwen2.5-coder:7b, v4) that after the advisory
                # loop-break message the model retried the same dead path
                # anyway, reaching 11 identical failures and burning the run.
                # Refusing to execute is the only thing that reliably forces
                # a different action.
                result = (f"REFUSED: you have already called {name} with these exact arguments "
                          f"3+ times and it failed every time. This path does not exist. Stop "
                          f"retrying it. Call list_files on '.' to see the real structure, and "
                          f"use only paths that appeared in a list_files result.")
            elif name == "run_bash" and _command_risks_self_collision(args.get("command", "")):
                # Harness-level block, not a prompt-level ask -- see
                # _command_risks_self_collision's docstring/comment above.
                result = (f"REFUSED: this command appears to call an LLM inference endpoint "
                          f"(Ollama's /api/generate|chat|embed|pull|create, an OpenAI-style "
                          f"/v1/chat/completions|completions|embeddings path, or `ollama run|pull|"
                          f"create`) directly. You are already running as a resident model on this "
                          f"host -- a second inference call risks loading another model into the "
                          f"same VRAM/memory pool and crashing this dispatch (confirmed: this exact "
                          f"pattern OOM'd a prior run). Do not test, warm up, or call any inference "
                          f"endpoint yourself. If you need to verify an API's request/response "
                          f"shape, reason about it from documentation/what you already know instead "
                          f"of making a live call.")
            elif name == "task_complete":
                # Fable design 2026-08-29 (Penn: "how do we fix so we get it to converge?"),
                # built against real transcripts where coding-tuned models structurally
                # avoided the old silence-only convergence signal (some never emitted a
                # single content-only turn across 13-30 iterations) while also, separately,
                # declaring victory on objectively incomplete work that a weak --verify
                # blessed. Gate the claim on the task's own verify command so a false
                # completion claim comes back as concrete feedback instead of being either
                # accepted blind or silently discarded at DID-NOT-CONVERGE. Bounded by
                # MAX_COMPLETION_CLAIMS so a task whose verify can never be satisfied still
                # stops eventually instead of looping the claim forever.
                completion_claims += 1
                if verify and completion_claims <= MAX_COMPLETION_CLAIMS:
                    # Baseline-delta feedback (added 2026-08-30): classify the verify result
                    # against the start-of-dispatch baseline so the model is told only about
                    # failures ITS diff introduced, not pre-existing noise it can't and
                    # shouldn't fix (see _verify_failure_signature's docstring for the incident).
                    v_ok, new_failures, preexisting, current_recognized, v_out = (
                        _verify_delta_feedback(verify, cwd, _baseline_verify_sig))
                    # Evidence the model actually did work this session -- required before a
                    # BASELINE-BROKEN accept so a zero-edit claim on a broken baseline can't
                    # exit clean (Fable review 2026-08-30, condition 2: mirrors the end-of-run
                    # vacuous-pass guard, which the BASELINE-BROKEN path would otherwise skip).
                    _did_work = (_files_modified_count > 0 or _run_bash_success_count > 0)
                    # A no-regression accept is only SOUND when: the baseline genuinely had
                    # pre-existing failures (non-empty set, not a passing baseline's empty set
                    # and not a None/legacy baseline), we RECOGNIZED the current failing output
                    # (else an unrecognized new failure yields an empty delta and false-accepts
                    # -- Fable condition 1), and every recognized current failure is pre-existing.
                    _no_regression = (bool(_baseline_verify_sig) and current_recognized
                                       and not new_failures)
                    if v_ok and _baseline_is_the_task and not _did_work:
                        # Batch #7: the TRUE anomaly, and the only pause left on this path.
                        # This verify was PROVEN failing before the model touched anything,
                        # and it now passes over an UNCHANGED tree (no file writes, no
                        # successful run_bash). Nothing this run did can explain the flip, so
                        # the verify is nondeterministic or depends on state outside the
                        # worktree -- under either reading its pass is not evidence of a fix,
                        # and accepting would score an untouched bug as solved. Unlike a
                        # still-failing verify (which the model can act on, see below), there
                        # is nothing to feed back here: the defect is in the verify itself.
                        log(f"[worker] task_complete claim {completion_claims}: verify now PASSES "
                            f"but it was PROVEN FAILING at baseline and this run changed nothing "
                            f"(0 file edits, 0 successful run_bash) -- the verify flipped on its "
                            f"own. Pausing rather than scoring an untouched tree as a pass.")
                        pause_reason_code = "verify_flipped_without_work"
                        paused_for_review = (
                            "verify failed at baseline and now passes over an unchanged tree: it "
                            "is nondeterministic or depends on state outside the worktree, so its "
                            "pass proves nothing. Check the verify command by hand.")
                        result = ("NOT ACCEPTED: the verify passes now, but it was failing before "
                                  "your work began and you have not changed any file. This run is "
                                  "paused for a human to look at.")
                        converged = False
                    elif v_ok:
                        result = "ACCEPTED: verify passed."
                        converged = True
                        final_summary = args.get("summary", "")
                    # Reads the run_task PARAMETER, not `args`. In this scope `args`
                    # is the TOOL-CALL dict (see args.get("summary") directly above),
                    # so getattr(args, "verify_failed_at_baseline", False) returned the
                    # default False unconditionally and this branch was unreachable --
                    # the guard was deployed and inert, which is why three jobs were
                    # false-accepted on 2026-09-01 with the fix supposedly in place.
                    elif _baseline_is_the_task:
                        # Batch #7, replacing an immediate pause (2026-09-02). This verify was
                        # already failing before the model started -- for a bug-fix verify that
                        # is BY DESIGN, so "no NEW failures" is exactly what a NON-fix produces
                        # and must never be accepted (measured: three consecutive resell #310
                        # dispatches were accepted this way while the bug survived untouched).
                        #
                        # But it is not a reason to STOP. The old code paused here on the first
                        # claim, which is what parked scored arms at 21/30 with iterations
                        # unspent and a human on the critical path -- a run that still had every
                        # resource it needed to finish. The honest move is neither accept nor
                        # pause: tell the model the truth (the bar is a PASSING verify, not an
                        # unchanged one), hand it the FULL output -- NO baseline subtraction,
                        # because here the pre-existing failures ARE the task (batch #8) -- and
                        # let it keep working. The pause moves to the claim cap below, where the
                        # iterations really are spent and a human is genuinely needed.
                        log(f"[worker] task_complete claim {completion_claims}/"
                            f"{MAX_COMPLETION_CLAIMS}: verify still FAILS and its baseline was "
                            f"already failing at stage/enqueue -- NOT accepting (a still-failing "
                            f"verify cannot show the fix landed) and NOT pausing: feeding the "
                            f"full output back and continuing, claims remain.")
                        result = (f"NOT ACCEPTED: the task's verification command still fails.\n"
                                  f"--- verify output ---\n{v_out[-3000:]}\n--- end ---\n"
                                  f"IMPORTANT: this verify was ALREADY FAILING before you started "
                                  f"-- that is by design for this task. So 'I introduced no new "
                                  f"failures' is NOT success here: those pre-existing failures ARE "
                                  f"the bug you were asked to fix. You are not done until this "
                                  f"command PASSES (exits 0). Keep working. Do not run the verify "
                                  f"command yourself -- the harness already did.")
                        converged = False
                    elif _no_regression and _did_work:
                        # Verify still fails, but every recognized current failure was ALSO present
                        # at dispatch start -- the model's diff introduced no new failures. Do NOT
                        # loop the claim on pre-existing noise (the convergence failure this fixes):
                        # accept, with a loud BASELINE-BROKEN marker. The end-of-run authoritative
                        # verify applies the identical rule (+ the same evidence-of-work guard).
                        log(f"[worker] task_complete claim {completion_claims}: verify still fails "
                            f"BUT 0 new recognized failures vs the dispatch-start baseline, and the "
                            f"model did real work -- accepting (BASELINE-BROKEN: verify was already "
                            f"failing before this run, likely an unprovisioned worktree).")
                        result = ("ACCEPTED: your changes introduced no new verify failures. The "
                                  "verify command still reports failures, but they were ALL "
                                  "present before your work began (a broken verify baseline, not "
                                  "your responsibility) -- so your task is accepted as complete.")
                        converged = True
                        final_summary = args.get("summary", "")
                    elif new_failures:
                        log(f"[worker] task_complete claim {completion_claims}/"
                            f"{MAX_COMPLETION_CLAIMS}: verify FAILED with "
                            f"{len(new_failures)} NEW failure(s) from this diff -- feeding only "
                            f"those back instead of the full output.")
                        _shown = "\n".join(new_failures)[-3000:]
                        _pre_note = ("" if not preexisting else
                                     "\n(Other failures in the verify output PRE-EXIST your "
                                     "changes and are NOT yours to fix -- they are omitted here.)")
                        result = (f"NOT ACCEPTED: your changes introduced these "
                                  f"{len(new_failures)} new verification failure(s):\n"
                                  f"--- new failures ---\n{_shown}\n--- end ---{_pre_note}\n"
                                  f"Fix ONLY these, then call task_complete again. Do not run the "
                                  f"verify command yourself -- the harness already did.")
                    else:
                        # Couldn't classify as no-regression: either no baseline (None/legacy or a
                        # passing baseline where ANY failure is new), we recognized none of the
                        # current failing output, or there was no evidence of work. Fall back to the
                        # original raw-output feedback rather than false-accept (Fable condition 1).
                        log(f"[worker] task_complete claim {completion_claims}/"
                            f"{MAX_COMPLETION_CLAIMS}: verify FAILED and could not be attributed to "
                            f"the baseline (unrecognized failure lines, no baseline, or no evidence "
                            f"of work) -- feeding the raw output back.")
                        result = (f"NOT ACCEPTED: the task's verification command still fails.\n"
                                  f"--- verify output ---\n{v_out[-3000:]}\n--- end ---\nFix the "
                                  f"reported problem(s), then call task_complete again. Do not run "
                                  f"the verify command yourself -- the harness already did.")
                elif verify and _baseline_is_the_task:
                    # Batch #7: this is where the pause now lives. The claim cap is reached,
                    # so the model has had MAX_COMPLETION_CLAIMS attempts with the full verify
                    # output in hand and still cannot make it pass. Accepting the model's word
                    # (what the generic cap branch below does) is unsound here for the same
                    # reason the in-loop accept was: the verify was failing before this run, so
                    # its continued failure is exactly what an untouched bug looks like. Two
                    # readings -- the fix did not land, or the verify's environment is broken --
                    # and neither is evidence of completion. Now the iterations really are
                    # spent, so a human is the right next step.
                    log(f"[worker] task_complete claim {completion_claims}: claim cap reached and "
                        f"the verify -- already failing at stage/enqueue -- still fails. It cannot "
                        f"distinguish a completed fix from an untouched bug (BASELINE-BROKEN). "
                        f"Pausing for review instead of accepting the model's word.")
                    # DISTINCT reason code on purpose. ollama-queue.py's auto-resume watchdog
                    # only ever auto-bumps "context_threshold" and "request_more_iterations";
                    # anything else it refuses to touch. A reason that fell into either bucket
                    # would be silently re-queued with more iterations and pause again -- an
                    # infinite loop burning GPU on a verify that cannot answer the question.
                    # This code makes the pause terminal until a human acts.
                    pause_reason_code = "verify_uninformative"
                    paused_for_review = (
                        "verify was already failing at stage/enqueue and still fails after "
                        f"{MAX_COMPLETION_CLAIMS} completion claims: it cannot show whether the "
                        "work landed. Either the fix did not take, or the verify's baseline is "
                        "broken. Fix the baseline, or check the change by hand.")
                    result = ("NOT ACCEPTED: the verify still fails, and it was already failing "
                              "before your work began -- so it cannot show whether your change "
                              "worked. This run is paused for a human to look at.")
                    converged = False
                else:
                    log(f"[worker] task_complete called (claim {completion_claims}) -- "
                        f"{'no --verify given' if not verify else 'claim cap reached'}, "
                        f"accepting the model's word.")
                    result = "ACCEPTED."
                    converged = True
                    final_summary = args.get("summary", "")
            elif _thrash_cached is not None:
                # Anti-thrash: identical read-only call we already ran this run. Serve the
                # cached result annotated instead of re-running the tool (see
                # _anti_thrash_intercept). The stronger nudge, once past the threshold, is
                # delivered via loop_break_notes with the rest of this turn's guidance.
                result = _thrash_cached
                if _thrash_nudge:
                    loop_break_notes.append(_thrash_nudge)
                log(f"[worker] anti-thrash: served {name} from cache "
                    f"(repeat #{_thrash_repeats.get(sig)}) instead of re-running.")
            elif impl is None:
                result = f"ERROR: unknown tool {name}"
            else:
                try:
                    result = impl(cwd, args)
                except Exception as e:
                    result = f"ERROR: {e}"
            # Same-page re-read nudge: appended to a SUCCESSFUL read, since the
            # loop this catches never produces an error to hang a warning on.
            if (name == "read_file" and isinstance(args, dict) and isinstance(result, str)
                    and _repeat_reads.get((str(args.get("path")), str(args.get("offset") or 1)), 0) >= 3):
                _m = re.search(r"lines (\d+)-(\d+) of (\d+)", result[:400])
                if _m and int(_m.group(2)) < int(_m.group(3)):
                    _nxt = int(_m.group(2)) + 1
                    result += (f"\n\n[NOTE: you have now read this exact page of "
                               f"{args.get('path')} 3+ times and it will not change. The rest of "
                               f"the file is further down -- call read_file with "
                               f'{{"path":"{args.get("path")}","offset":{_nxt},"length":300}} '
                               f"to advance, or use run_bash with grep to find a specific line "
                               f"number first and read around that offset.]")
            # Tell the model what it sent that the tool does not accept. Appended
            # AFTER the call so the result is unchanged when nothing is unknown,
            # and so a stray key never costs the call itself.
            if _unknown and isinstance(result, str) and not result.startswith("REFUSED"):
                _decl = sorted(_tool_arg_names(name))
                result += (f"\n\n[NOTE: {name} does not take "
                           f"{', '.join(repr(u) for u in _unknown)} -- "
                           f"{'that argument was' if len(_unknown) == 1 else 'those arguments were'} "
                           f"IGNORED, not applied. {name} accepts: {', '.join(_decl) or '(none)'}.]")
            # Vacuous-verify-pass tracking (see the counters' own init comment above).
            if isinstance(result, str):
                if name in ("write_file", "edit_file") and result.startswith("OK: "):
                    _files_modified_count += 1
                elif name == "run_bash" and result.startswith('{"exit_code"'):
                    _run_bash_success_count += 1
            args_preview = json.dumps(args)[:200]
            result_preview = str(result)[:300]
            log(f"[worker] tool {name}({args_preview}) -> {result_preview}")
            if live is not None:
                live.tool_result(name, result)

            # Loop detection. Confirmed live 2026-08-22 (qwen2.5-coder:7b,
            # photo-upload): the model burned ALL 30 iterations repeating
            #   read_file app/components/ProfitCard.tsx -> not found
            #   list_files app/components/            -> not found
            # over and over. `components/` is at the repo root, not under
            # `app/` -- it had that fact from its own earlier listing and
            # never re-oriented. Nothing intervened, so a single wrong guess
            # became a total loss (0 files written). A prior run of the SAME
            # model on the SAME task wrote 5 correct files; the difference in
            # outcome was one bad turn with no recovery, which is why the
            # run-to-run variance looked so implausibly large.
            failed = str(result).startswith("ERROR") or str(result).startswith("REFUSED")
            # Anti-thrash cache store: remember a FRESH, successful read-only result so a
            # later identical call is served from here (see _anti_thrash_intercept). Only
            # fresh runs (_thrash_cached is None) so a cache hit never re-stores its own
            # annotated copy, and never a failure (a transient error should be retryable).
            if _thrash_cacheable and _thrash_cached is None and not failed:
                _tool_result_cache[sig] = result
            if name == "web_fetch" and not failed:
                web_fetch_succeeded = True
                web_fetch_success_count += 1
                real_fetched_texts.append(str(result))
            # Local-source verification ledger (see local_read_count's init comment):
            # a successful read_file/list_files, or a run_bash that reads/searches the
            # tree (grep/cat/find/git log/...), is genuine local verification. Pulled
            # from the SAME tool-call ledger web_fetch uses, one place that already
            # knows the tool name, success/failure, and args for this exact call.
            if not failed:
                if name in ("read_file", "list_files"):
                    local_read_count += 1
                elif name == "run_bash" and _command_is_local_read(
                        str(args.get("command", "")) if isinstance(args, dict) else ""):
                    local_read_count += 1
            if failed:
                repeated_failures[sig] = repeated_failures.get(sig, 0) + 1
                if repeated_failures[sig] in (3, 6, 9):
                    log(f"[worker] loop-break: {name} has failed 3x with identical args -- injecting corrective guidance.")
                    # Added 2026-08-28: this used to hardcode file-path advice
                    # ("call list_files... find where the file actually
                    # lives") unconditionally -- wrong, confusing guidance if
                    # the tool that's actually looping is web_search/web_fetch
                    # (e.g. retrying the same dead URL 3 times), where the
                    # right advice is "try a different query/URL", not
                    # "list_files". Same underlying pattern as the
                    # coding-biased corrective nudge and system prompt fixed
                    # earlier tonight -- adapt the guidance to which tool is
                    # actually stuck instead of assuming it's always a file path.
                    if name in ("read_file", "list_files", "write_file", "edit_file"):
                        specific = ("That path does not exist. Do NOT call it again. Call list_files "
                                    "on '.' and on the parent directory to find where the file actually "
                                    "lives, and use only paths you have seen in a list_files result.")
                    elif name in ("web_search", "web_fetch"):
                        specific = ("That query/URL is not working. Do NOT call it again with the same "
                                    "arguments. Try a different, more specific search query, or a "
                                    "different source URL entirely.")
                    else:
                        specific = "Do NOT call it again with the same arguments. Try a different approach."
                    loop_break_notes.append(
                        f"You have now called {name} with exactly these arguments 3 times and it has "
                        f"failed every time: {args_preview}. {specific}"
                    )
            else:
                repeated_failures.pop(sig, None)
            # Confirmed live 2026-08-28 (llama3.1:8b, EV-charging discovery v5): the loop-break
            # guard above only counts FAILING calls, so a tool call that "succeeds" every time
            # but returns the same unhelpful result (a web_search whose top hit is irrelevant,
            # e.g.) never trips it -- this run repeated the identical web_search query for all
            # 22/22 iterations, burned the entire budget, and never converged. Track identical
            # calls regardless of success/failure and nudge (soft, not a hard REFUSE -- a
            # search that keeps "succeeding" isn't a dead path the way a 404 is) once repetition
            # itself is the problem.
            # EXEMPT THE JOB'S OWN VERIFY (2026-09-02). Re-running `bash verify.sh`
            # after every edit is exactly what a converge-on-verify task INSTRUCTS,
            # and it is the progress signal, not a loop. Confirmed live during the
            # coding bake-off: loop-detect fired "run_bash called 3x with identical
            # arguments, none of them advancing the task" where the identical
            # command WAS the verify -- on a run that was converging and went on to
            # pass 8/0. Counting it as repetition pathologises correct behaviour for
            # every model on every dispatch, and it briefly made a converging model
            # look like it was stuck in a loop. (_is_own_verify was computed once, up where
            # the anti-thrash intercept needs it -- reused here for the loop-detect exemption.)
            if _is_own_verify:
                repeated_calls.pop(sig, None)
            else:
                repeated_calls[sig] = repeated_calls.get(sig, 0) + 1
            if not _is_own_verify and not failed and repeated_calls[sig] in (3, 6, 9):
                log(f"[worker] loop-detect: {name} called {repeated_calls[sig]}x with identical "
                    f"arguments, none of them advancing the task -- injecting corrective guidance.")
                loop_break_notes.append(
                    f"You've now called {name} with the exact same arguments "
                    f"{repeated_calls[sig]} times, and it keeps returning the same result without "
                    f"moving the task forward. Repeating it again will not help -- try a "
                    f"genuinely different query, URL, or approach instead. If the task is "
                    f"actually finished, call task_complete instead of repeating this."
                )
            if failed:
                repeated_calls.pop(sig, None)
            if manual_call_this_turn:
                # role:"tool" combined with omitting the native `tools` API
                # field is an untested combination for this model -- a
                # plain user-role result message is what was actually
                # proven to work end-to-end live 2026-08-21, so stick with
                # that instead of assuming role:"tool" also works here.
                messages.append({"role": "user", "content": f"[tool result for {name}]: {result}"})
            elif api_style == "openai":
                # OpenAI-compatible tool-result messages are keyed back to
                # their call via tool_call_id -- confirmed required (tested
                # live against llama-server 2026-08-22) for the model to
                # correctly associate the result with its own call.
                tool_msg = {"role": "tool", "content": str(result)}
                if tool_call_id:
                    tool_msg["tool_call_id"] = tool_call_id
                messages.append(tool_msg)
            else:
                messages.append({"role": "tool", "content": str(result)})

        if converged:
            # task_complete was accepted above -- stop immediately rather than sending loop-
            # break guidance or a budget note for an iteration that will never happen. The
            # final _save_transcript below (after the outer loop) records this correctly.
            break

        # Deliver any loop-break guidance accumulated this turn, as a plain
        # user message so it reaches models with no tool-role template.
        if loop_break_notes:
            messages.append({"role": "user", "content": "\n\n".join(loop_break_notes)})
            loop_break_notes = []

        if not paused_for_review:
            remaining = total_iters - i
            if remaining > 0 and remaining <= ITERATION_LOW_BUDGET_THRESHOLD:
                messages.append({
                    "role": "user",
                    "content": f"[Iteration budget warning: you are on iteration {i} of "
                               f"{total_iters} -- only {remaining} iteration(s) remain before "
                               f"this dispatch stops with DID-NOT-CONVERGE if the task isn't "
                               f"done. If you have concrete remaining work that will not fit "
                               f"in those, call request_more_iterations now (it pauses for "
                               f"review rather than granting anything immediately). Only do "
                               f"that for real remaining work -- not as a routine check-in, "
                               f"and not to recover from being stuck. If the task is actually "
                               f"complete, call task_complete now instead of calling another "
                               f"tool just to keep going.]",
                })
            elif remaining > 0 and i % 3 == 0:
                # Fable finding 2026-08-29: this note used to fire every single iteration,
                # addressing the model directly right after every turn including ones where it
                # had just given a would-be-final answer -- a real transcript showed a model
                # settle into repeating a summary + a no-op "echo done" tool call 18 times in a
                # row, plausibly because being re-addressed after each "final" turn reads as a
                # prompt to keep acting. Every 3rd iteration is enough to keep the model aware
                # of its budget without constant "act now" pressure; the low-budget variant
                # above still fires every iteration since urgency is warranted there.
                messages.append({
                    "role": "user",
                    "content": f"[Iteration {i}/{total_iters}: {remaining} remaining in your budget.]",
                })

        # Incremental save after this iteration's tool execution completes:
        # a kill at any point loses at most this one iteration's work, not
        # the whole run -- see _save_transcript's docstring and the --resume
        # support above for why this exists. converged=False here because the
        # real value isn't known until the loop ends; the final save below
        # overwrites it with the real one.
        _save_transcript(log_path, model, host, cwd, task, False, i, messages,
                          worktree_start_snapshot=_worktree_start_snapshot,
                          baseline_verify_sig=_baseline_verify_sig)

        # Context-usage counterpart to request_more_iterations (Penn's request, "can we do
        # the same for context?"): a model can't self-report running low on context the way
        # it can ask for more iterations, since it doesn't see its own token accounting --
        # so this is harness-driven instead, checked against the objective usage the API
        # already returned for this iteration. Moved HERE (was checked immediately on
        # receiving the response, before tool_calls were even read) after Fable found this
        # was silently dropping a pending tool call: the model's response for this iteration
        # could legitimately include a tool call, but breaking before line ~2802 ever looked
        # at tool_calls meant it was appended to `messages` and then simply never executed or
        # answered -- on resume, the model faced its own dangling, unanswered call and often
        # produced a blank response. Checking here instead, after this iteration's tool calls
        # have actually run and been saved, means a pause always lands on a fully completed
        # iteration, same as the SIGTERM check just below and the file's own documented rule
        # for `_save_transcript` (see its docstring).
        if not paused_for_review:
            total_tokens_used = usage.get("total_tokens") or 0
            if num_ctx and total_tokens_used >= num_ctx * CONTEXT_REVIEW_THRESHOLD:
                paused_for_review = (
                    f"context usage {total_tokens_used}/{num_ctx} tokens "
                    f"({total_tokens_used / num_ctx:.0%}) at or above the "
                    f"{CONTEXT_REVIEW_THRESHOLD:.0%} review threshold"
                )
                pause_reason_code = "context_threshold"
                pause_meta = {"tokens_used": total_tokens_used, "num_ctx": num_ctx}
                log(f"[worker] PAUSED FOR REVIEW: {paused_for_review}. To grant more room, "
                    f"resume with: --resume {log_path} --num-ctx <bigger> (in addition to the "
                    f"same other flags this dispatch used). To deny, just don't -- the "
                    f"transcript stays as-is.")
            elif num_ctx:
                # Proactive mid-run budget nudges BELOW the 0.90 pause (see
                # _context_budget_nudges): warn the model to converge while it still has
                # room, each threshold at most once per run. Only reached when the pause
                # above did not fire this iteration.
                for _thr, _msg in _context_budget_nudges(
                        total_tokens_used, num_ctx, _ctx_nudge_fired):
                    messages.append({"role": "user", "content": _msg})
                    log(f"[worker] context-budget nudge at {total_tokens_used / num_ctx:.0%} "
                        f"(threshold {_thr:.0%}) -- steering toward convergence.")

        # External pause (SIGTERM from the queue daemon's promote flow): honor it HERE -- at
        # the end of a fully completed and saved iteration -- so the transcript on disk is
        # always complete through some whole iteration. Reuses paused_for_review's existing
        # break/log/save path below exactly as request_more_iterations does; no separate stop
        # logic, and the exit code at the bottom reports EXIT_CODE_PAUSED for both pause
        # sources alike (resumable, not a failure).
        if _sigterm_pause_requested:
            paused_for_review = "external pause (SIGTERM) from queue daemon"
            pause_reason_code = "external_sigterm"
            pause_meta = {}

        # request_more_iterations sets this from inside the tool-call loop above (a `break`
        # there only exits that inner loop) -- check it here, after this iteration's transcript
        # save, so the pause is captured before the outer loop actually stops.
        if paused_for_review:
            break

    if not converged:
        if paused_for_review:
            log(f"[worker] PAUSED FOR REVIEW at iteration {i}/{total_iters}: {paused_for_review}")
        else:
            log(f"[worker] DID NOT CONVERGE after {total_iters} iterations -- stopping, output may be incomplete.")
            # Confirmed live 2026-08-28 (EVgo research): the warning banner above only fires on
            # the clean "no tool calls, accepted" exit -- a model that gets nudged for zero real
            # fetches and then simply runs out of iteration budget mid-nudge-cycle (a real
            # DID-NOT-CONVERGE, not a paused-for-review) skips that check entirely, so its last
            # message can carry confidently-stated fabricated numbers with NO warning attached
            # at all. Apply the same unconditional banner here, to whichever message is actually
            # the last assistant turn (that's what a reader sees as "the answer"), covering this
            # exit path too.
            if task_kind == "research" and not web_fetch_succeeded and not facts_provided:
                for m in reversed(messages):
                    if m.get("role") == "assistant":
                        m["content"] = (m.get("content") or "") + (
                            "\n\n---\nHARNESS WARNING: this dispatch ran out of iterations "
                            "without ever completing a verified answer, and no web_fetch call "
                            "succeeded anywhere in this session. Any claim above of having "
                            "'verified' or 'confirmed' a detail is NOT reliable, regardless of "
                            "what the text above says -- treat every specific fact in this "
                            "answer as unconfirmed until it is checked against a source that "
                            "was actually, successfully fetched."
                        )
                        log("[worker] research task hit DID-NOT-CONVERGE with zero successful "
                            "web_fetch calls -- appending the same unconditional harness "
                            "warning to the last assistant message.")
                        break

    # Final save via the same helper as the incremental per-iteration saves
    # above, with the real converged value. log_path is NOT reassigned here:
    # on a fresh run it's the new timestamped file created up top, and on a
    # resumed run it's the resume file itself, which keeps getting updated
    # across pause/resume cycles.
    _save_transcript(log_path, model, host, cwd, task, converged, i, messages,
                      pause_reason=pause_reason_code, pause_meta=pause_meta,
                      worktree_start_snapshot=_worktree_start_snapshot,
                      baseline_verify_sig=_baseline_verify_sig)
    log(f"[worker] full transcript written to {log_path}")
    if paused_for_review:
        # Single greppable marker for the queue daemon (ollama-queue.py parses this out of
        # the job's log file to learn which transcript to --resume from when it relaunches a
        # paused job). Printed for BOTH pause sources -- external SIGTERM and the model's own
        # review gates -- since both leave an identically resumable transcript.
        log(f"[worker] RESUMABLE TRANSCRIPT: {log_path}")

    if live is not None:
        if converged:
            live.result_box(final_summary or content or "(no final text)", True, i)
        elif paused_for_review:
            # The iteration the run ACTUALLY stopped at (i), not the budget (total_iters):
            # a pause at 5/20 left 15 iterations unspent, and that distinction is the whole
            # point of the pause. The plain log() above already reports it this way -- this
            # is the live/dashboard view catching up.
            live.result_box(f"PAUSED at iteration {i}/{total_iters} -- {paused_for_review}",
                            False, i, status="paused")
        else:
            live.result_box(f"DID NOT CONVERGE after {total_iters} iterations", False, total_iters)

    # Auto-capture fallback (Fable ruling 2026-08-30): some models -- nemotron-cascade-2
    # especially -- put a review-style deliverable in their final TEXT answer instead of
    # writing the file, despite the task instruction and the bounded verify-nudges. When
    # --capture-final-as names a file the run never produced, write the final text answer
    # to it and log `capture=fallback`, so the content is SCORED rather than lost as a
    # NO_REVIEW cell. Runs BEFORE verify (so a `[ -f REVIEW.md ]` verify then passes) and
    # only for a completed (non-paused) run -- a paused run's output is incomplete by
    # definition. Report the fallback rate separately (grep logs for "capture=fallback").
    capture_fallback = False
    if getattr(args, "capture_final_as", None):
        _cap = resolve_path(cwd, args.capture_final_as)
        _should, _text, _tag = _capture_decision(
            args.capture_final_as, _cap.exists(), paused_for_review, (final_summary or content or ""))
        if _should:
            try:
                _cap.write_text(_text)
                capture_fallback = True
                log(f"[worker] {_tag}: saved final answer to {args.capture_final_as} ({len(_text)} chars).")
            except Exception as _e:
                log(f"[worker] capture-final-as write failed for {args.capture_final_as}: {_e}")

    verify_passed = None  # None = not run, distinct from False = ran and failed
    # True only when the end-of-run verify FAILED yet every failure pre-existed the dispatch
    # (0 new failures from the model's diff). Kept separate from verify_passed so the vault
    # log can say "no regression" rather than a bare PASSED, while the exit code still treats
    # it as not-a-failure. Added 2026-08-30 (baseline-delta).
    _baseline_no_regression = False
    if verify and paused_for_review:
        # A paused run is by definition incomplete -- running its verify command against
        # half-finished output would just produce a misleading failure (and could take up to
        # 300s), so it's skipped; the exit code below reports "paused", not "verify failed".
        log("[worker] skipping verify command: run is paused, output is incomplete by definition.")
    elif verify:
        log(f"[worker] running verify command: {verify}")
        # Fable code review, 2026-08-29: this subprocess.run had NO timeout handling
        # anywhere up the call stack (run_task is invoked bare via sys.exit(run_task(...))
        # at the bottom of this file) -- a verify command hanging past 300s raised
        # TimeoutExpired and killed the whole process with a traceback AFTER all the
        # model's real work: no VERIFY line logged, the vault log below never ran despite
        # being documented as unconditional, no keep_alive/evict, live.close() skipped.
        # A real crash bug, not a hypothetical one -- fixed in the same change as the
        # vacuous-pass guard below since Fable flagged it four lines from this edit.
        # Snapshot taken BEFORE running verify, not after (Fable ruling) -- a verify
        # command that builds artifacts (compiles, generates a lockfile, etc.) would
        # dirty the tree itself and fake a non-empty diff if taken afterward. None
        # when cwd isn't a git repo at all, same as _worktree_start_snapshot.
        _worktree_end_snapshot = (_git_worktree_snapshot(cwd)
                                   if _worktree_start_snapshot is not None else None)
        try:
            result = subprocess.run(verify, shell=True, cwd=cwd, capture_output=True, text=True, timeout=300)
        except subprocess.TimeoutExpired:
            log("[worker] VERIFY TIMED OUT (300s) -- treating as failed. The task's own work "
                "may well be fine; it's the verify COMMAND itself that didn't finish in time "
                "(a hanging build/test, a network call with no timeout of its own, etc.).")
            verify_passed = False
        else:
            log(f"[worker] verify stdout:\n{result.stdout}")
            if result.stderr:
                log(f"[worker] verify stderr:\n{result.stderr}")
            if result.returncode in (126, 127):
                # Distinct from a real assertion failure: 126/127 means the shell couldn't
                # even RUN the verify command (not found / not executable), not that it ran
                # and found something wrong. Conflating the two sends debugging down the
                # wrong path -- the fix here is to the --verify string, not the model's work.
                log(f"[worker] VERIFY FAILED (exit {result.returncode}) -- this looks like the "
                    f"verify COMMAND ITSELF is broken (not found / not executable), not a real "
                    f"assertion failure against the task's work. Check the --verify string.")
                verify_passed = False
            else:
                verify_passed = result.returncode == 0
                if verify_passed:
                    log("[worker] VERIFY PASSED (exit 0).")
                    # Vacuous-pass guard (Fable GO-with-conditions 2026-08-29, REVISED same
                    # day after github-projects-bf caught a real gap in the first version: a
                    # session that only explored via run_bash -- 9 successful calls, zero
                    # edits, zero diff -- landed in a WARN-only branch under a run_bash-
                    # success-count discriminator, because "run_bash succeeded" only proves a
                    # command RAN, not that it changed anything. Ground truth (did the tree
                    # actually change) supersedes inferring intent from tool-call counts.
                    # git-repo case: the diff-snapshot comparison is authoritative --
                    # write_file/edit_file AND heredoc/sed/git-apply-via-run_bash AND direct
                    # commits are all covered (rev-parse HEAD is part of the snapshot
                    # specifically so a commit-without-a-dirty-tree still counts as changed).
                    # Coding-only: task_kind=="research" has no file-based deliverable by
                    # design (see any_mutation_called's own comment above), so this guard
                    # doesn't apply there regardless of which branch below would otherwise fire.
                    if task_kind == "coding" and _worktree_start_snapshot is not None:
                        if _worktree_end_snapshot != _worktree_start_snapshot:
                            pass  # tree genuinely changed -- verify_passed stands as True
                        elif _files_modified_count == 0:
                            verify_passed = False
                            log("[worker] OVERRIDING TO FAILED: verify passed but the working "
                                "tree is byte-for-byte unchanged since dispatch start (same "
                                "HEAD, same git status, same diff) AND zero write_file/edit_file "
                                "calls succeeded -- almost certainly a vacuous pass (e.g. a "
                                "verify command that only checks pre-existing file validity), "
                                "not real completed work. If this task genuinely required no "
                                "changes at all, that's a real limitation of this guard -- use a "
                                "--verify command that actually asserts something about the "
                                "task's output instead.")
                        else:
                            log("[worker] VACUOUS-WARN: verify passed and the git working tree "
                                "shows no change, but write_file/edit_file DID succeed this "
                                "session -- likely a write outside the repo or to a "
                                ".gitignore'd path (real work, just invisible to git), possibly "
                                "still a vacuous pass otherwise. Not auto-failing this one, but "
                                "worth a look before trusting it. (Greppable prefix VACUOUS-WARN "
                                "for downstream tooling -- WARN + exit 0 is otherwise invisible.)")
                    elif task_kind == "coding" and _files_modified_count == 0:
                        # cwd isn't a git repo (or the snapshot itself failed) -- fall back to
                        # the original tool-call-count heuristic, degraded but not silent about it.
                        if _run_bash_success_count == 0:
                            verify_passed = False
                            log("[worker] OVERRIDING TO FAILED (degraded check -- cwd is not a "
                                "git repo, no working-tree diff available): verify passed but "
                                "ZERO files were modified and ZERO run_bash calls succeeded this "
                                "session -- almost certainly a vacuous pass, not real completed "
                                "work.")
                        else:
                            log("[worker] VACUOUS-WARN: (degraded check -- cwd is not a git "
                                "repo) verify passed with zero write_file/edit_file calls, but "
                                "run_bash ran successfully this session -- legitimate if the "
                                "model made its edits via run_bash, still possibly a vacuous "
                                "pass otherwise. Not auto-failing this one, but worth a look.")
                else:
                    # Baseline-delta on the AUTHORITATIVE end-of-run verify (added 2026-08-30):
                    # if every current failure was already present at dispatch start, the model's
                    # diff introduced no regressions the verify can see. Reporting exit 1 (FAILED)
                    # here is what falsely failed correct work in the resell-tracker incident (a
                    # verify already broken by a missing `prisma generate`). Distinguish the two:
                    # a genuine NEW failure still FAILS; a no-new-failure run is marked
                    # BASELINE-BROKEN and allowed to pass (exit 0) so real work isn't discarded.
                    # Excludes the 126/127 branch above by construction (that's a broken verify
                    # COMMAND, handled separately). num_ctx/timeouts unaffected.
                    #
                    # A no-regression PASS here needs the SAME three soundness gates as the in-loop
                    # accept (Fable review 2026-08-30): (1) the baseline was genuinely non-empty
                    # (real pre-existing failures, not a passing baseline's empty set nor a
                    # None/legacy baseline); (2) we RECOGNIZED the current failing output (an empty
                    # current signature can't prove "no new failures" -- an unrecognized regression
                    # would otherwise pass); (3) evidence the model actually changed the tree, since
                    # setting verify_passed=True on a FAILING verify bypasses the exit-0-only
                    # vacuous-pass guard below -- without this a zero-edit run on a broken baseline
                    # would flip exit 1 -> 0. All three must hold, else this stays a real FAILURE.
                    _end_out = (result.stdout or "") + (
                        ("\n" + result.stderr) if result.stderr else "")
                    _end_cur = _verify_failure_signature(_end_out)
                    _end_new = (_end_cur - _baseline_verify_sig
                                 if _baseline_verify_sig is not None else None)
                    if _worktree_start_snapshot is not None:
                        _end_did_work = (_worktree_end_snapshot != _worktree_start_snapshot)
                    else:
                        _end_did_work = (_files_modified_count > 0)
                    # Same rule at the authoritative end-of-run verify: if the queue's
                    # pre-flight already saw this verify FAIL at enqueue, a still-failing
                    # verify proves nothing and must not be flipped to a pass.
                    if (_end_new is not None and not _end_new
                            and bool(_baseline_verify_sig) and _end_cur and _end_did_work
                            and not _baseline_is_the_task):
                        _baseline_no_regression = True
                        verify_passed = True  # authoritative: no recognized regression + real work
                        log(f"[worker] VERIFY FAILED (exit {result.returncode}) BUT every recognized "
                            f"failure pre-existed at dispatch start (0 new from this diff) and the "
                            f"model changed the tree -- marking BASELINE-BROKEN and NOT failing the "
                            f"run (verify baseline was already broken, e.g. an unprovisioned "
                            f"worktree / missing codegen). Fix the baseline before trusting this as "
                            f"a clean pass.")
                    else:
                        if _end_new:
                            _n = f" ({len(_end_new)} new failure(s) attributable to this diff)"
                        elif _end_new is not None and not _end_cur:
                            _n = " (no recognized diagnostics to attribute -- not treated as no-regression)"
                        elif _end_new is not None and not _end_did_work:
                            _n = " (0 new failures but no tree change -- not treated as no-regression)"
                        else:
                            _n = ""
                        log(f"[worker] VERIFY FAILED (exit {result.returncode}){_n}. "
                            f"Do not trust this output as-is.")
    else:
        log("[worker] No --verify command given. Output has NOT been verified -- "
            "build/test it before trusting it.")

    # Accept-on-verify-pass despite no task_complete (2026-08-30, Penn: "we're still
    # choking these processes"). Root cause of a class of false NON-CONVERGENCE:
    # qwen3-coder reliably WRITES a correct deliverable and verifies it by running it
    # via run_bash, but often never emits the task_complete tool call to SIGNAL done --
    # so it edits/re-runs to the iteration cap and the run is scored NO CONVERGENCE
    # even though the objective --verify passes against real, changed work. That's a
    # model-signalling gap, not incomplete work. So: if the run did NOT converge (no
    # task_complete) and was NOT paused, but a --verify was given and PASSED (the
    # vacuous-pass / baseline-delta guards above already applied, so verify_passed here
    # means a genuine pass against a genuinely-changed tree), treat it as converged.
    # Narrowly scoped: only fires when there's a real verify that really passed -- a run
    # with no --verify, or a failing verify, still reports non-convergence as before.
    if verify and verify_passed and not converged and not paused_for_review:
        log("[worker] ACCEPTING as converged despite no task_complete: hit the iteration "
            "cap without the model signalling done, but the --verify PASSED against real "
            "changed work. Known qwen3-coder gap (verifies by running, doesn't emit "
            "task_complete). Scored as success, not a false non-convergence.")
        converged = True

    # Always logged, per standing instruction -- not conditional on
    # success, since a failed/non-converged dispatch is exactly the data
    # worth having a record of too.
    log_dispatch_to_obsidian(model, task, converged, verify_passed, log_path,
                              baseline_no_regression=_baseline_no_regression)

    # This log USED to be success-only by design (Penn's call): a crashed or
    # non-converged run's token counts aren't a clean signal for "how much
    # context does a task like this actually need", and would pollute the
    # threshold analysis the log exists for.
    #
    # That rationale still holds and is PRESERVED -- by the `status` field, not
    # by omission. Any context-threshold analysis filters status == "converged"
    # and sees exactly the same population it saw before; not one converged row
    # changes meaning. What omission also did, though, was make failures
    # unanswerable: asked on 2026-09-01 whether over-scoped jobs predictably
    # die at the iteration cap, the file could not answer, because all 309 rows
    # were status=converged and the outcome variable had a single value. That
    # is total survivorship bias -- the two cap-deaths that prompted the
    # question were simply absent. A log that records only successes cannot be
    # used to study failure, and studying failure is now the point.
    #
    # NOTE FOR CONSUMERS: dispatch-tally.py counts rows without filtering
    # status, so its dispatch count will now include failures. That is more
    # correct -- a failed dispatch still consumed a GPU slot -- but it is a
    # step change in a tracked number, so the tally prints the breakdown.
    _dispatch_metrics["verify_passed"] = verify_passed
    # Distinguish a clean pass from an accepted no-regression-on-broken-baseline run so
    # the metrics JSONL doesn't over-count clean verify passes (baseline-delta, 2026-08-30).
    _dispatch_metrics["baseline_no_regression"] = _baseline_no_regression
    # `converged` is checked FIRST and unqualified, which deliberately does NOT
    # mirror the return-code precedence below. A converged run that also paused,
    # or that failed verify, has always written status="converged" with the
    # detail in verify_passed -- reordering to match the exit code would
    # silently redefine all 309 existing rows. So status and exit code diverge
    # in exactly those cases (verified exhaustively: 4 of 12 reachable
    # combinations, all of them converged=True), and that divergence is the
    # existing contract, not a bug. Read `status` for what the run achieved and
    # the exit code for how it terminated; they answer different questions.
    # The non-converged branches below DO mirror the return-code precedence.
    _dispatch_metrics["status"] = (
        "converged" if converged
        else "paused" if paused_for_review
        else "done_unconverged" if (verify and verify_passed)
        else "verify_failed" if verify
        else "unconverged")
    _dispatch_metrics["wall_time_s"] = round((datetime.now(timezone.utc) - _dispatch_start).total_seconds(), 1)
    _dispatch_metrics["transcript_path"] = str(log_path)
    _dispatch_metrics["max_iters"] = max_iters
    # Did it die ON the cap? The single most useful field for the question that
    # motivated this change -- p95 of converged runs sat exactly on the cap,
    # which is a censoring signature, and this makes it directly countable
    # instead of inferred from a histogram.
    _dispatch_metrics["hit_iter_cap"] = bool(
        isinstance(max_iters, int) and _dispatch_metrics.get("iterations", 0) >= max_iters)
    _dispatch_metrics["files_changed"] = _changed_file_count(cwd)
    _dispatch_metrics["web_search"] = dict(_WEB_SEARCH_CALLS)
    write_dispatch_metrics(_dispatch_metrics)

    if is_local_ollama and cleanup_after:
        evict_model(model)
    elif api_style == "ollama":
        # Leave it warm for 24h so the next dispatch of this model (very
        # likely, given pick_host() now routes consistently) skips the cold
        # load -- see set_keep_alive()'s docstring. Skipped when
        # cleanup_after already evicted the model outright.
        set_keep_alive(host, model, "24h")

    if live is not None:
        live.close()

    if paused_for_review:
        # Distinct from both 1 (verify failed) and 2 (genuinely ran out of budget): the queue
        # daemon uses this to mark the job "paused" instead of "failed" and relaunch it with
        # --resume <transcript>. Covers BOTH pause sources -- the model's own review gates AND
        # an external SIGTERM from the daemon's promote flow -- which are the same situation
        # for a caller: gracefully stopped, resumable, not a failure.
        return EXIT_CODE_PAUSED
    if verify and not verify_passed:
        return 1
    if not converged and verify and verify_passed:
        # Mirror image of the vacuous-pass guard above: the loop didn't exit tidily
        # (model kept iterating/rambling after finishing, or otherwise ran out of
        # budget), but --verify genuinely passed -- and if task_kind=="coding" the
        # vacuous-pass guard already required real tree changes or file writes to
        # leave verify_passed True at all (a vacuous pass gets overridden to False
        # before this point), so reaching here with verify_passed=True means real,
        # confirmed work exists. Report that distinctly instead of plain FAILED.
        log(f"[worker] DONE-BUT-UNCONVERGED: verify passed with real completed work, "
            f"but the loop didn't exit cleanly ({total_iters} iterations used) -- "
            f"reporting exit {EXIT_CODE_DONE_UNCONVERGED}, not plain DID-NOT-CONVERGE, "
            f"so this isn't discarded unread by anything triaging on status alone.")
        return EXIT_CODE_DONE_UNCONVERGED
    return 0 if converged else 2


def main():
    ap = argparse.ArgumentParser(description="Dispatch a coding task to a local Ollama model, bypassing opencode.")
    ap.add_argument("--model", default=DEFAULT_MODEL)
    ap.add_argument("--direct-ok", action="store_true",
                     help="Run without going through ollama-queue.py. Off by default: a direct "
                          "invocation is invisible to the queue dashboard AND to the queue's "
                          "cross-host coordination, so two dispatches can silently collide on "
                          "one host. Use only for a deliberate one-off you are watching.")
    ap.add_argument("--host", default=None,
                     help="Ollama host to dispatch to. Omit to auto-pick based on which of the "
                          "two known hosts (Unraid's 3080, the Studio's unified memory) the "
                          "model actually fits -- see pick_host()/KNOWN_OLLAMA_HOSTS. Pass "
                          "explicitly to override (e.g. a third/ad-hoc host, or an OpenAI-style "
                          "--api openai endpoint, which pick_host doesn't know about).")
    ap.add_argument("--cwd", required=True)
    ap.add_argument("--task", required=True,
                     help="Task description. Still required when --resume is given (keeps the "
                          "CLI consistent), but when resuming the actual message history comes "
                          "from the resumed transcript file, not reconstructed from --task.")
    ap.add_argument("--resume", default=None,
                     help="Path to a previously saved transcript JSON to resume from: its "
                          "messages are reloaded and the loop continues from where that run "
                          "left off (iteration numbering continues, and --max-iters means how "
                          "many MORE iterations are allowed this session). Model/host come "
                          "from the CLI args, so resuming with a DIFFERENT model than started "
                          "the task is a supported case, not an error. The transcript keeps "
                          "updating the same file across pause/resume cycles.")
    ap.add_argument("--verify-failed-at-baseline", action="store_true",
                    help=("the queue's pre-flight ran --verify in this cwd BEFORE the model "
                          "touched anything and it FAILED. That makes a still-failing verify "
                          "UNINFORMATIVE, not excusable: it means either the fix did not land "
                          "or the environment is broken, and neither is evidence of completion. "
                          "Suppresses the BASELINE-BROKEN auto-accept: the full verify output "
                          "is fed back as work to do while completion claims remain, and the "
                          "run pauses for review only once the claim cap is reached."))
    ap.add_argument("--scored-arm", action="store_true",
                    help=("this run is a SCORED BAKE-OFF ARM, staged from a verify PROVEN to "
                          "fail at baseline. Implies --verify-failed-at-baseline (a stronger "
                          "statement than the queue pre-flight's observation) AND disables "
                          "baseline-diagnostic subtraction, because for a scored arm the "
                          "baseline failures ARE the task -- subtracting them would hide the "
                          "only diagnostics that matter and let an untouched bug read as 'no "
                          "new failures'. Set by bakeoff-fire.py; recorded in dispatch-metrics "
                          "as scored_arm so a scorer never pools scored and unscored runs."))
    ap.add_argument("--verify", default=None, help=(
        "Shell command to run after the loop completes, e.g. 'npm run build'. Two authoring "
        "rules, confirmed real-incident 2026-08-29 (Fable review): (1) a negative-grep guard "
        "('! grep pattern') cannot distinguish a forbidden command being EXECUTED from that "
        "same text merely being echoed/printed/commented -- exclude echo/printf/comment lines "
        "or it will fail correct work that only prints the pattern. (2) verify should assert "
        "something that was FALSE before the dispatch ran, not just something trivially true of "
        "any file (a bare syntax check like 'bash -n script.sh' passes on an UNMODIFIED file too "
        "-- the harness now catches the zero-files-touched case, see the vacuous-pass guard in "
        "run_task, but a check that's simply too weak to fail on wrong-but-syntactically-valid "
        "work is not caught by anything)."
    ))
    ap.add_argument("--max-iters", type=int, default=DEFAULT_MAX_ITERS)
    ap.add_argument("--temperature", type=float, default=DEFAULT_TEMPERATURE)
    ap.add_argument("--num-ctx", type=int, default=DEFAULT_NUM_CTX)
    ap.add_argument("--num-ctx-bumps", type=int, default=0,
                    help="how many times the queue watchdog raised this job's context "
                         "before this run; recorded as a metric, never a penalty")
    ap.add_argument("--top-p", type=float, default=None,
                     help="Nucleus sampling cutoff. Added 2026-08-22: both Qwen's and DeepSeek-R1's "
                          "own model cards specifically warn against temperature=0 (greedy decoding) "
                          "-- documented to cause endless-repetition failures, confirmed live as the "
                          "root cause of a real runaway-generation crash tonight. Their recommended "
                          "settings pair temperature=0.6 with top_p=0.95. Omit to leave Ollama's "
                          "default (unset here).")
    ap.add_argument("--top-k", type=int, default=None,
                     help="Top-k sampling cutoff. Qwen's recommended setting is 20, paired with the "
                          "temperature/top_p above -- see --top-p's help for why this exists.")
    ap.add_argument("--searxng-host", default=DEFAULT_SEARXNG_HOST,
                     help="Self-hosted SearXNG instance for web_search (see deploy notes in the vault -- "
                          "not yet deployed as of 2026-08-21, web_search will error until it is).")
    ap.add_argument("--system-prompt-file", default=None,
                     help="Path to a file with a custom system prompt, replacing the default. "
                          "Needed for devstral, which produces zero tool calls on this Ollama build "
                          "without its own OpenHands-scaffold system prompt.")
    ap.add_argument("--task-kind", choices=["coding", "research"], default="coding",
                     help="What counts as 'real progress' for the corrective-nudge safeguard. "
                          "\"coding\" (default) requires an actual write_file/edit_file call before "
                          "a no-tool-call response is accepted without a nudge -- catches a model "
                          "narrating a change instead of saving it. \"research\" accepts ANY tool "
                          "call (e.g. web_fetch) as real progress, since a research task's "
                          "deliverable is the final text answer, not a file -- added 2026-08-28 "
                          "after confirming live that the coding-only definition guaranteed a "
                          "false-positive nudge on every research dispatch, which pushed at least "
                          "one model into writing its unverified answer to a file it was never "
                          "asked to produce.")
    ap.add_argument("--facts-provided", action="store_true",
                     help="Suppresses the research task-kind's anti-fabrication nudge entirely. "
                          "Use for a two-pass 'write the final answer' dispatch where the facts "
                          "were already gathered and verified in a prior pass and are handed in "
                          "directly in the task text -- without this flag the nudge fires because "
                          "it can only see zero web_fetch calls THIS session, and cannot tell that "
                          "apart from a model that never did any real research at all. Added "
                          "2026-08-28 after the flag's absence forced a model with genuinely real, "
                          "pre-verified facts to rewrite a correct answer into a false 'could not "
                          "confirm anything, all fetches failed' disclaimer.")
    ap.add_argument("--read-file-max-chars", type=int, default=None,
                     help="Override the per-read page cap (default: READ_FILE_MAX_CHARS, "
                          "additionally clamped to ~1/8 of --num-ctx).")
    ap.add_argument("--web-fetch-max-chars", type=int, default=None,
                     help="Override the per-fetch truncation limit (default: WEB_FETCH_MAX_CHARS, "
                          "currently 5000 chars). The default is tuned for open-ended multi-fetch "
                          "collection (avoids accumulated-context OOM across several fetches in one "
                          "pass) but truncates a single real multi-page document before content that "
                          "matters -- confirmed live 2026-08-28: a real NV Energy rate PDF's page-1 "
                          "boilerplate alone consumed the whole 5000-char budget, so the page-2 table "
                          "with the actual answer never reached the model, which correctly said 'not "
                          "specified' rather than fabricate. Raise this for a dispatch doing few, "
                          "targeted fetches of known-large documents; leave it alone for open-ended "
                          "multi-fetch research where the original OOM risk still applies.")
    ap.add_argument("--min-web-fetches", type=int, default=0,
                     help="Research task_kind only: require at least this many SUCCESSFUL "
                          "web_fetch calls before the task is allowed to converge -- a hard "
                          "requirement, not just a nudge. Added 2026-08-28 after confirming live "
                          "(llama3.1:8b, EV-charging-network discovery, 4 consecutive attempts) "
                          "that a model can repeatedly find the right pages via web_search and "
                          "just never fetch them, converging on unverified snippet claims with "
                          "iteration budget to spare -- the existing one-time fabrication nudge "
                          "only asks the model to hedge, it doesn't force real verification, and "
                          "in that same session the model responded to the nudge by falsely "
                          "claiming a fact was 'verified through web_fetch' when zero fetches had "
                          "ever succeeded. Default 0 (off) preserves prior behavior for tasks "
                          "where search-only answers are acceptable.")
    ap.add_argument("--cleanup-after", action="store_true",
                     help="Evict this model from the local disk cache after the task completes "
                          "(only blobs not shared by another cached model are removed). Trades "
                          "disk space for a repeated copy-from-SMB cost on the next dispatch of "
                          "this model -- omit to keep it cached (default, recommended when disk "
                          "space isn't tight).")
    ap.add_argument("--manual-tools", action="store_true",
                     help="Bypass Ollama's native tool_calls parsing entirely: inject the tool "
                          "schemas as plain text in the system prompt and parse the model's "
                          "response for a {\"name\":...,\"arguments\":...} object ourselves. "
                          "Needed for deepseek-r1 distills (confirmed 2026-08-21: their Ollama "
                          "template never renders the native `tools` field into the prompt at "
                          "all, and native tool_calls stays null even on the community "
                          "'MFDoom/deepseek-r1-tool-calling' build -- see Ollama-Dispatch-Log.md "
                          "and github.com/ollama/ollama/issues/8517). Confirmed working live "
                          "against deepseek-r1:14b across write_file/read_file tasks including "
                          "multi-turn continuation and correct termination.")
    ap.add_argument("--api", choices=["ollama", "openai"], default="ollama",
                     help="Which API shape --host speaks. 'ollama' (default) targets Ollama's "
                          "native /api/chat and /api/tags|pull for model management. 'openai' "
                          "targets an OpenAI-compatible /v1/chat/completions endpoint (llama-server, "
                          "etc.) instead -- no model pull/discovery is attempted (the server already "
                          "has exactly one model loaded via its own -m flag), just a /health check. "
                          "Added 2026-08-22 to route around a confirmed upstream Ollama bug "
                          "('no user query found in messages', github.com/ollama/ollama/issues/17778) "
                          "that crashes qwen3.8/some other models even on trivial requests -- "
                          "llama-server renders the GGUF's own embedded chat template directly and "
                          "doesn't run Ollama's custom Go renderer code, so it doesn't hit this bug.")
    ap.add_argument("--chat-timeout", type=int, default=CHAT_TIMEOUT_S,
                     help=f"Per-request timeout (seconds) for the main dispatch-loop chat call "
                          f"(default {CHAT_TIMEOUT_S}s). Does NOT affect model warmup ({WARMUP_TIMEOUT_S}s, "
                          f"already generous and separate) -- only the per-iteration call. Raise this "
                          f"for a --resume retry after a run crashed on repeated timeouts at exactly "
                          f"the default value with a large accumulated context (confirmed real "
                          f"2026-08-28: a 27B model given ~35-40K tokens of context genuinely needed "
                          f"longer than 1200s to respond, not stuck/hung -- the backend was still "
                          f"actively computing the whole time).")
    ap.add_argument("--max-tokens", type=int, default=DEFAULT_MAX_TOKENS,
                     help=f"Hard cap on generated tokens per chat call (default {DEFAULT_MAX_TOKENS}). "
                          f"Added 2026-08-28 after a real runaway-generation incident (an Unraid "
                          f"dispatch generated past 123,000 tokens with no stop token, confirmed via "
                          f"the backend's own print_timing log, not guessed). Ollama native: "
                          f"options.num_predict. --api openai: top-level max_tokens.")
    ap.add_argument("--capture-final-as", default=None,
                     help="Filename (relative to cwd) of the run's expected deliverable. If the run "
                          "ends with that file NOT written but a final text answer present, the worker "
                          "writes the final text to it and logs capture=fallback -- for models that put "
                          "a review/answer in their text reply instead of writing the file (Fable "
                          "2026-08-30, nemotron-cascade-2). Runs before verify; skipped on a paused run.")
    ap.add_argument("--repeat-penalty", type=float, default=DEFAULT_REPEAT_PENALTY,
                     help=f"Sampler repeat penalty (default {DEFAULT_REPEAT_PENALTY}; 1.0 = disabled). "
                          f"Same incident as --max-tokens: temperature=0 (fully greedy, the common "
                          f"case here) with repeat_penalty=1.0 is a documented llama.cpp infinite-"
                          f"repetition-loop combination (github.com/ollama/ollama/issues/3759, "
                          f"ggml-org/llama.cpp discussion #3005) -- this file never set it before, so "
                          f"every past dispatch ran at the disabled default.")
    ap.add_argument("--think", choices=["auto", "on", "off"], default="auto",
                     help="Native Ollama reasoning-mode toggle (top-level `think` key). auto (default) "
                          "= omit it, model decides / no behaviour change. off = disable reasoning -- "
                          "use for tool-driven dispatches on HYBRID models (qwen3.5:9b etc.), where "
                          "thinking-on burns the whole token budget on the reasoning field and returns "
                          "empty content / no tool call (e2 measured this). on = force it. Always-"
                          "thinking models (nemotron-a3b) reject an explicit value with an HTTP error; "
                          "there is no auto-retry -- use auto for those models. Ignored on --api "
                          "openai (llama-server).")
    ap.add_argument("--live-log", default=None,
                     help="Opt-in live streaming log: when set, the default native-Ollama-tools "
                          "path streams each model response and appends tagged, colored status "
                          "lines (thinking checkpoints, tool calls/results, iteration "
                          "boundaries, a final result box) to this file for `tail -f` viewing. "
                          "This is IN ADDITION to the normal log() output, not a replacement. "
                          "When omitted, behavior is exactly as before (blocking call_ollama, "
                          "no streaming). Only the default native-tools Ollama path streams; "
                          "--manual-tools / --api openai runs still use the blocking call even "
                          "with this flag set.")
    ap.add_argument("--dispatch-tag", default=None,
                     help="Short tag prefixed to every --live-log line ([tag] ...) so one "
                          "dispatch can be followed among several concurrent dispatches "
                          "sharing the same log file. Defaults to the --cwd basename.")
    ap.add_argument("--claude-prep-tokens", type=int, default=None,
                     help="Claude's own output-token cost of getting to this dispatch (investigation, "
                          "writing the task spec) -- a separate figure from anything measured here, "
                          "logged alongside it in dispatch-metrics.jsonl so the two are directly "
                          "comparable. Compute via claude-token-cursor.py: run it once before starting "
                          "prep, once again right before this dispatch, pass the delta here. Omit if "
                          "not tracking this for a given dispatch.")
    args = ap.parse_args()

    # Dispatch must go through ollama-queue.py. Enforced here rather than left
    # as a rule because the rule has failed repeatedly: a direct `nohup
    # ollama-worker.py ...` run does real work but never appears on the queue
    # dashboard and is invisible to the queue's own host-coordination, which is
    # exactly the collision the queue exists to prevent (2026-08-29: a direct
    # run occupied Studio while the queue believed Studio was free). The queue
    # stamps OLLAMA_DISPATCH_VIA_QUEUE on every worker it launches.
    if not os.environ.get("OLLAMA_DISPATCH_VIA_QUEUE") and not args.direct_ok:
        sys.stderr.write(
            "refusing to run: this worker was not launched by ollama-queue.py.\n"
            "Direct runs are invisible to the queue dashboard and to its cross-host\n"
            "coordination, so they can collide with queued work on the same host.\n\n"
            "Enqueue it instead:\n"
            "  python3 ~/bin/ollama-queue.py enqueue --model MODEL --host auto \\\n"
            "      --cwd DIR --task-file FILE --label NAME\n\n"
            "Pass --direct-ok only for a deliberate one-off you are actively watching.\n"
        )
        # Exit 4, not 2: the worker already uses 2 for DID-NOT-CONVERGE, and
        # conflating "refused to start" with "ran but gave up" sent one real
        # diagnosis down the wrong path entirely.
        sys.exit(4)

    host = args.host
    if host is None:
        if args.api == "openai":
            # pick_host only knows about the two Ollama-native hosts; an
            # OpenAI-style endpoint (llama-server, etc.) has no sensible
            # auto-pick. Default to the dedicated llama-server
            # (start-llama-server-qwen3.8.sh, fixed PORT=8091, started with
            # --jinja so tool-calling works) instead of erroring out. We do
            # NOT launch that script here -- if nothing is listening,
            # ensure_model_ready() says so plainly. An explicit --host
            # always overrides this default, unchanged from before.
            host = DEFAULT_OPENAI_HOST
            log(f"[worker] --api openai without --host: defaulting to {DEFAULT_OPENAI_HOST} "
                f"(start-llama-server-qwen3.8.sh's fixed port; it must already be running -- "
                f"this script does not start it)")
        else:
            host = pick_host(args.model)

    # CRASH PATH. write_dispatch_metrics' docstring has always claimed it is
    # "called both from run_task's normal completion path and from main()'s
    # crash handler, so a context-exhaustion crash -- the exact failure mode
    # this log exists to eventually let Penn threshold against -- still gets a
    # real entry instead of silently vanishing." There was no crash handler.
    # The claim was aspirational, and the crashes it names were exactly the
    # rows missing from the file. Now it is true.
    #
    # Catches Exception only: SystemExit and KeyboardInterrupt propagate
    # untouched, and so does the daemon's SIGTERM promote flow, which is a
    # graceful pause and already has its own path. Re-raises after logging --
    # this observes the failure, it must never swallow it.
    try:
        rc = run_task(
            args.model, host, args.cwd, args.task, args.verify,
            args.max_iters, args.temperature, args.num_ctx, args.searxng_host,
            args.system_prompt_file, args.cleanup_after, args.manual_tools,
            top_p=args.top_p, top_k=args.top_k, api_style=args.api,
            claude_prep_tokens=args.claude_prep_tokens,
            resume_from=args.resume,
            task_kind=args.task_kind,
            chat_timeout=args.chat_timeout,
            max_tokens=args.max_tokens,
            repeat_penalty=args.repeat_penalty,
            facts_provided=args.facts_provided,
            web_fetch_max_chars=args.web_fetch_max_chars,
            read_file_max_chars=args.read_file_max_chars,
            verify_failed_at_baseline=args.verify_failed_at_baseline,
            scored_arm=args.scored_arm,
            num_ctx_bumps=args.num_ctx_bumps,
            min_web_fetches=args.min_web_fetches,
            live_log=args.live_log,
            dispatch_tag=args.dispatch_tag or (Path(args.cwd).name or "dispatch"),
            think={"on": True, "off": False, "auto": None}[args.think],
        )
    except Exception as e:
        if _dispatch_metrics.get("status") == "running":
            _dispatch_metrics["status"] = "crashed"
            _dispatch_metrics["error"] = f"{type(e).__name__}: {str(e)[:200]}"
            _dispatch_metrics["web_search"] = dict(_WEB_SEARCH_CALLS)
            write_dispatch_metrics(_dispatch_metrics)
        raise
    sys.exit(rc)


if __name__ == "__main__":
    main()
