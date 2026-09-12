#!/usr/bin/env python3
"""verify-relevance.py -- does a verify test the PROPERTY, or a proxy for it?

WHY THIS EXISTS (the ceiling on autonomous sign-off)
-----------------------------------------------------
The dispatch gate proves a verify DISCRIMINATES: it fails on the untouched
baseline and passes with a reference implementation applied. That is necessary
and it is where the gate stopped. It is NOT sufficient, because a verify can be
discriminating while testing something other than the property the task asked
for:

    grep -q "THRESHOLD * 2" target.py            # a LITERAL proxy
    ast.walk(...) finds a Compare with GtE        # an AST proxy
    is_safe({"count": 99}, True) == True         # a BENIGN behavioural case

Each of these fails at baseline and passes with the fix, so each passes the
both-ways proof -- and each also passes with `>=` flipped to `>`, with the flag
gate deleted, with the threshold off by one. That is how broken work has passed
a green verify in this project. "Assert the property, not a proxy" and "verify
relevance, not just discrimination" are the two memories this file mechanises.

THE SIGNAL: targeted mutation of the reference implementation
-------------------------------------------------------------
We already hold, at gate time, a reference implementation that turns the verify
green. Perturb it in ways that BREAK THE PROPERTY IT INTRODUCED and ask whether
the verify notices. The perturbations ("mutants") are generated on the lines the
reference implementation ADDED -- that region is, by construction, the fix -- so
each mutant is "the fix, but wrong in one specific way":

    comparison flipped at its boundary   (>= -> >, == -> !=)
    boolean glue swapped                 (and <-> or), `not` dropped
    a constant nudged                    (n -> n+1, n-1, True -> False)
    an arithmetic operator swapped       (* -> +)
    a branch condition negated / forced  (if c -> if not c / if True)
    a return value blanked or negated    (return x -> return None / not x)
    an added statement deleted           (the partial revert)
    an added hunk reverted whole         (language-agnostic partial revert)

A verify that tests the property KILLS these (goes red). A verify that tests a
proxy lets them SURVIVE, because the proxy is still satisfied: the literal is
still in the file, the AST still has a Compare, the benign case still returns
True.

WHAT IS AND IS NOT COUNTED AS EVIDENCE -- the three filters
-----------------------------------------------------------
Mutation testing's classical weakness is the EQUIVALENT MUTANT: a change that
alters the text but not the behaviour, which no test can kill and which then
reads as a gap in the test. Three filters keep that from turning this check
into noise, and every one of them errs in the SAFE direction (fewer mutants,
never a mutant that flatters the verify):

  1. ONLY THE ADDED LINES ARE MUTATED. Mutating untouched code would measure
     the verify's coverage of pre-existing behaviour, which is not the question
     and would penalise a perfectly relevant verify for not being a regression
     suite.

  2. LITERAL-BREAKING MUTANTS ARE NOT EVIDENCE. If a mutant removes one of the
     task's `## Must contain` literals from the file, the scaffold's own
     check_literals step kills it regardless of what the behavioural checks do.
     Such a kill says nothing about relevance, so those mutants are generated,
     run, reported -- and EXCLUDED from the score. Without this filter a pure
     grep verify would score well on every mutant that happened to touch its
     literal. This is the single most important design decision in the file.

  3. INNOCUOUS STATEMENTS ARE NOT MUTATED. Logging, printing, docstrings,
     comments, imports, assertions: deleting or perturbing these is expected to
     be unobservable, and a relevance check that penalises a verify for not
     catching a no-op is wrong. The skip list is a heuristic and it errs toward
     skipping -- a skipped mutant costs at most one piece of evidence, an
     equivalent mutant that is counted costs a false "low relevance" flag.

Kills by CRASH (a traceback in the verify output) are counted as kills but
reported separately: a crash on a mutant means the verify EXECUTED the mutated
path with an input that reached it, which is behavioural evidence, but a verify
that only compiles the file kills nothing this way because every mutant here is
built to parse. `.pyc` caches are removed before every run: a mutant of the same
byte-length written in the same second would otherwise load stale bytecode and
read as a survivor.

THE DECISION, and the asymmetry it is built around
--------------------------------------------------
    score = killed / evidence_mutants        (evidence = literal-preserving)

    relevant   score >= threshold  AND  evidence_mutants >= min_mutants
    low        score <  threshold  AND  evidence_mutants >= min_mutants
    unproven   fewer than min_mutants evidence mutants, refimpl not green,
               nothing mutable (non-Python target with a single hunk), or the
               time budget ran out before min_mutants were tried

The two error directions are NOT symmetric. A false "relevant" lets broken
work auto-approve; a false "low" costs a human a look they were already giving.
So the defaults are strict (threshold 0.8, min 3 evidence mutants) and every
"could not tell" is UNPROVEN, never a pass. Survivors are NAMED in the output,
each with its class and the source it produced, so a human can settle in
seconds whether a survivor is an equivalent mutant or a real hole -- the check
is designed to be argued with, not obeyed blindly.

MEASURED (test-verify-relevance.py, and one real dispatch), 2026-09-03
----------------------------------------------------------------------
Toy fixture (is_safe threshold), every verify below clears both-ways:
    behavioural verify with adversarial cases      score 1.00   relevant
    same verify, refimpl padded with log/print     score 1.00   relevant
    grep-two-literals proxy                        score 0.00   low
    AST-shape proxy (finds a GtE Compare + an If)  score 0.20   low
    benign behavioural (count=99 True, count=1 F)  score 0.40   low
    HALF the property (flag gate only)             score 0.64   low
Real dispatch (claude-vs-ollama-tokens, 56-line refimpl, human-reviewed
fixture, 40 mutants in 6s):
    original fixture                 mutant 0.725  site 0.89   LOW
      survivors named: the zero-data text branch (L295, 8 mutants), the
      `claude_tokens` alias key (L247), the bool guard (L142)
    fixture + the 3 cases those survivors point at
                                     mutant 0.925  site 0.975  RELEVANT
      remaining 3 survivors are domain-equivalent (`+`->`-` on a both-zero
      check; a bool-valued usage field that never occurs)
So: every proxy shape we have been burned by scores <= 0.64; a relevant
verify scores >= 0.9; the threshold 0.8 sits in the gap with 0.16 on the
proxy side and 0.1 on the relevant side. The real-dispatch LOW is the
honest answer -- the holes were real requirements from the task's own
"must NOT change" list -- and the survivor list turned it RELEVANT in three
added cases. That actionability is the point: the check argues, it does
not just refuse.

THREE SCORER HOLES, measured and closed 2026-09-03 (test section I)
------------------------------------------------------------------
  1. TRUNCATION WAS CLASS-BALANCED, NOT SITE-BALANCED. The toy fixture at
     --max-mutants 3 tried L7 and L9 and never L8, and said RELEVANT with
     truncated=true; wt-tokens' cap of 40 put seven mutants on one line and
     left three hunks untried. Now: every site is tried once before any
     site twice, `untried_sites` is reported, and an untried site makes the
     run UNPROVEN (never relevant).
  2. AN UNEXERCISED SITE WAS A FOOTNOTE. wt-tokens with only the alias case
     removed: L247's single mutant survived and the verdict stayed RELEVANT
     0.9 (mutant) / 0.925 (site mean) -- a task "must NOT change" item
     untested, and every average clearing the threshold because at twenty
     sites one hole is a 5% dent. Now: any site where every tried mutant
     survived (>= 1, was >= 2) makes the run LOW, and `site_coverage`
     (sites with at least one kill) is the third score in the min().
     Re-measured: original fixture LOW 0.8 naming L247/L295/L298 (the three
     task items); improved RELEVANT 0.925 coverage 1.0; alias-removed LOW
     naming L247. The wide toy (six gates, five tested) has mutant 0.85 and
     site 0.833 -- both over the threshold -- and is LOW naming gate_6.
  3. CRASH-KILL INFLATION was the suspected third hole: a smoke-run proxy
     that executes the code and asserts nothing. Measured 0.0 on the toy
     and 0 crash kills on wt-tokens -- NOT a hole today; the smoke proxy is
     now a permanent arm so it stays that way.
  The cost of 1 and 2 is a false LOW on a line whose only mutant is
  equivalent (a dead store) or a run whose cap is under the site count;
  both name the line, both are a human look, neither is an approval.

FALSE-POSITIVE / FALSE-NEGATIVE ANALYSIS
---------------------------------------
  false RELEVANT (expensive: broken work auto-approves)
    - a verify that diffs the target against a golden copy kills every
      mutant and scores 1.0 while testing nothing behavioural. Not seen in
      this project; would show as 100% kill including crash-free constant
      nudges of message strings. Mitigation available: an "equivalence
      canary" mutant (rename a local variable) that a golden-diff kills and
      a behavioural verify does not. NOT implemented; the golden-diff shape
      also fails verify-quality's literal check in practice.
    - a fixture that asserts exactly the refimpl's outputs on exactly one
      input still kills most mutants of a small fix. It is relevant to that
      input only. Layer 3 (change class) bounds the blast radius; the human
      read is the backstop in shadow mode.
  false LOW (cheap: a human looks)
    - equivalent mutants: fallback constants (`or 0`), rounding precision,
      docstring hunks, logger setup were all seen on the first real run and
      each got a filter (const-default / FORMATTING_CALL_NAMES /
      python_code_lines / INNOCUOUS_CALL_NAMES). More will appear; each one
      costs a look, not an approval, and is named in the survivor list.
    - per-mutant scoring overweights a complex untested line; per-site
      scoring is reported alongside and the verdict takes the lower.

KNOWN LIMITS (honest, and written down so they survive the authors)
-------------------------------------------------------------------
  * A verify that is relevant to the ADDED code but the task's real property
    lives in code the refimpl did not change (a wrong-site refimpl) is not
    caught; that is a refimpl defect and refimpl-satisfies owns it.
  * Mutants are generated for Python via AST spans; other languages get only
    hunk-level partial reverts, which need >= 2 hunks to say anything. A Swift
    dispatch with one hunk is UNPROVEN, which is the correct answer.
  * A verify that is a near-complete oracle for the fixed function will kill
    equivalent mutants too rarely to matter; a verify that kills EVERYTHING,
    including obvious equivalents, is worth a second look -- it may be
    diffing the file against a golden copy, which is a proxy of a different
    kind. The `kill_all_including_equivalent` heuristic is NOT implemented;
    the survivor list is the place a human sees it.
  * It costs one verify run per mutant. Capped by --max-mutants and
    --budget-s; a capped run reports `truncated` and still needs min_mutants.

USAGE
-----
  verify-relevance.py <worktree> --refimpl fix.patch [--verify 'bash verify.sh']
  verify-relevance.py <worktree> --refimpl-cmd 'python3 refimpl.py' --json
  verify-relevance.py <worktree> --applied      # tree ALREADY carries the fix;
                                                # measured in place, fix restored

Exit codes: 0 relevant, 1 low, 3 unproven, 2 usage/error.
The preflight gate imports `measure_applied()` directly.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

DEFAULT_THRESHOLD = 0.8
DEFAULT_MIN_MUTANTS = 3
DEFAULT_MAX_MUTANTS = 40
DEFAULT_BUDGET_S = 600
VERIFY_TIMEOUT_S = 900

# Calls whose deletion or perturbation is expected to be unobservable. Erring
# toward skipping is the safe direction (see filter 3 in the module docstring).
INNOCUOUS_CALL_NAMES = {
    "print", "log", "debug", "info", "warning", "warn", "error", "exception",
    "critical", "trace", "logger", "logging", "pprint", "getLogger",
    # project audit/activity logger: records a human-readable activity string,
    # never carries the behavioural property. Nudging a constant inside its
    # message (e.g. a `/86400` day figure) is an equivalent mutant, same class
    # as `log.info` above.
    "record_activity",
}
COMMENT_PREFIXES = ("#", "//", "/*", "*", "*/", "--")
# Constants that are PRESENTATION, not property: `round(x, 2)` -> `round(x, 3)`
# changes nothing a tolerance-based fixture can see, and a task never asks for
# "2 decimal places" as its property. Measured on the first real dispatch
# (claude-vs-ollama-tokens): 3 of 16 survivors were the precision argument of
# round(). Skipping them is the safe direction (fewer mutants, never a kinder
# score for a proxy).
FORMATTING_CALL_NAMES = {"round", "format", "ljust", "rjust", "center", "zfill",
                         "quantize", "truncate"}

# BENIGN-MUTATION ALLOWLIST (pain point #2). Three classes of added line are
# UNOBSERVABLE by construction, so a verify that does not notice their deletion
# is not thereby a proxy verify -- flagging them wasted whole GO iterations on
# the first greenfield FastAPI dispatch. Each class is skipped in the SAFE
# direction (fewer evidence mutants, never a kinder score for a real proxy):
#
#   1. Resource cleanup as a BARE statement -- `conn.close()`, `await
#      session.aclose()`, `f.flush()`. Deleting it leaks a handle; no
#      behavioural fixture can see that within one process, so its stmt-delete
#      is an equivalent mutant. Only the bare-statement form is exempt: a
#      cleanup call whose RETURN is used (`x = q.close()`) is untouched.
#   2. A human MESSAGE string passed as a keyword argument a framework treats
#      as prose -- `HTTPException(status_code=404, detail="not found")`,
#      `raise ValueError(message="...")`. The `status_code` beside it is still
#      mutated (it IS the property); only the prose kwarg's const-str is skipped.
#   3. Anything the author marks `# relevance: unobservable` (also `ignore` /
#      `benign`) on the SAME physical line -- the escape hatch for the one case
#      a static rule cannot decide, e.g. a redundant `session.commit()` that a
#      later commit masks. Author-explicit, one line at a time, never a blanket.
#
# None of these can exempt a genuinely behavioural line: cleanup deletion is
# unobservable, a prose message carries no property, and the annotation is a
# deliberate per-line act. The behavioural mutants (compare-flip, return-flip,
# const-int on real logic, cond-*) are untouched, so the revert-test in
# test-ollama-dispatch-preflight.py still NO-GOes a weakened fixture.
CLEANUP_CALL_NAMES = {"close", "aclose", "flush", "dispose", "disconnect",
                      "shutdown", "release", "cleanup", "teardown"}
MESSAGE_KWARGS = {"detail", "message", "msg", "description", "reason", "hint",
                  "help", "title", "error_message", "err_msg"}
RELEVANCE_OPT_OUT = re.compile(r"#\s*relevance:\s*(?:unobservable|ignore|benign)\b")

CMP_FLIPS = {
    ast.Lt: ["<=", ">="], ast.LtE: ["<", ">"],
    ast.Gt: [">=", "<="], ast.GtE: [">", "<"],
    ast.Eq: ["!="], ast.NotEq: ["=="],
    ast.Is: ["is not"], ast.IsNot: ["is"],
    ast.In: ["not in"], ast.NotIn: ["in"],
}
CMP_TEXT = {ast.Lt: "<", ast.LtE: "<=", ast.Gt: ">", ast.GtE: ">=",
            ast.Eq: "==", ast.NotEq: "!=", ast.Is: "is", ast.IsNot: "is not",
            ast.In: "in", ast.NotIn: "not in"}
BIN_FLIPS = {
    ast.Add: ["-"], ast.Sub: ["+"], ast.Mult: ["+", "//"], ast.Div: ["*"],
    ast.FloorDiv: ["*"], ast.Mod: ["//"], ast.Pow: ["*"],
}
BIN_TEXT = {ast.Add: "+", ast.Sub: "-", ast.Mult: "*", ast.Div: "/",
            ast.FloorDiv: "//", ast.Mod: "%", ast.Pow: "**"}


# --------------------------------------------------------------------------
# diff parsing
# --------------------------------------------------------------------------
def parse_unified_diff(diff_text: str) -> dict[str, dict]:
    """Per file: added line numbers (in the NEW file) and the hunks.

    Each hunk is {new_start, new_len, old_lines, new_lines} so a hunk can be
    reverted on its own (the language-agnostic partial revert).
    """
    files: dict[str, dict] = {}
    cur = None
    hunk = None
    for ln in diff_text.splitlines():
        if ln.startswith("+++ "):
            name = ln[4:].strip()
            if name.startswith("b/"):
                name = name[2:]
            if name == "/dev/null":
                cur = None
                continue
            cur = files.setdefault(name, {"added": set(), "hunks": []})
            hunk = None
            continue
        if cur is None:
            continue
        m = re.match(r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@", ln)
        if m:
            hunk = {"old_start": int(m.group(1)),
                    "new_start": int(m.group(3)),
                    "old_lines": [], "new_lines": []}
            cur["hunks"].append(hunk)
            new_no = int(m.group(3))
            continue
        if hunk is None or ln.startswith("\\"):
            continue
        if ln.startswith("+"):
            hunk["new_lines"].append(ln[1:])
            cur["added"].add(new_no)
            new_no += 1
        elif ln.startswith("-"):
            hunk["old_lines"].append(ln[1:])
        else:
            # context line (only present when the diff was not -U0)
            hunk["old_lines"].append(ln[1:] if ln.startswith(" ") else ln)
            hunk["new_lines"].append(ln[1:] if ln.startswith(" ") else ln)
            new_no += 1
    return files


def _is_comment_only(lines) -> bool:
    for l in lines:
        s = l.strip()
        if s and not s.startswith(COMMENT_PREFIXES):
            return False
    return True


# --------------------------------------------------------------------------
# span-based source editing (preserves every byte the mutation does not touch)
# --------------------------------------------------------------------------
class Src:
    def __init__(self, text: str):
        self.text = text
        self.lines = text.split("\n")
        # ast col offsets are UTF-8 BYTE offsets.
        self.blines = [l.encode("utf-8") for l in self.lines]

    def offset(self, lineno: int, col: int) -> int:
        """Character offset into self.text for (1-based line, byte col)."""
        off = 0
        for i in range(lineno - 1):
            off += len(self.lines[i]) + 1
        return off + len(self.blines[lineno - 1][:col].decode("utf-8", "replace"))

    def span(self, node) -> tuple[int, int]:
        return (self.offset(node.lineno, node.col_offset),
                self.offset(node.end_lineno, node.end_col_offset))

    def get(self, a: int, b: int) -> str:
        return self.text[a:b]

    def replace(self, a: int, b: int, new: str) -> str:
        return self.text[:a] + new + self.text[b:]


def _call_name(node) -> str:
    f = node.func
    if isinstance(f, ast.Name):
        return f.id
    if isinstance(f, ast.Attribute):
        return f.attr
    return ""


def _is_innocuous_stmt(stmt) -> bool:
    if isinstance(stmt, ast.Expr):
        v = stmt.value
        if isinstance(v, ast.Constant):          # docstring / bare literal
            return True
        if isinstance(v, ast.Call):
            name = _call_name(v)
            root = v.func
            while isinstance(root, ast.Attribute):
                root = root.value
            rootname = root.id if isinstance(root, ast.Name) else ""
            if (name in INNOCUOUS_CALL_NAMES or rootname in INNOCUOUS_CALL_NAMES
                    or name.startswith("log")
                    or name in CLEANUP_CALL_NAMES):
                # CLEANUP as a bare statement only (this branch is ast.Expr):
                # deleting `conn.close()` leaks a handle no in-process fixture
                # can observe. A cleanup call whose return is USED goes through
                # the Assign branch below, which does NOT exempt it.
                return True
    if isinstance(stmt, ast.Assign) and isinstance(stmt.value, ast.Call):
        if _call_name(stmt.value) in INNOCUOUS_CALL_NAMES:
            return True
    if isinstance(stmt, (ast.Import, ast.ImportFrom, ast.Assert, ast.Pass,
                         ast.Global, ast.Nonlocal, ast.FunctionDef,
                         ast.AsyncFunctionDef, ast.ClassDef)):
        return True
    return False


def _inside_innocuous_call(node, parents) -> bool:
    for p in parents:
        if isinstance(p, ast.Call):
            name = _call_name(p)
            if name in INNOCUOUS_CALL_NAMES or name.startswith("log"):
                return True
    return False


def _inside_formatting_call(node, parents) -> bool:
    p = parents[-1] if parents else None
    return isinstance(p, ast.Call) and _call_name(p) in FORMATTING_CALL_NAMES


def python_code_lines(text: str) -> set[int] | None:
    """Line numbers carrying CODE: every statement's span minus docstrings.
    None if the file does not parse. A refimpl hunk whose added lines carry
    no code (a docstring edit, a comment block) is an equivalent mutant by
    construction when reverted -- the first real dispatch produced two such
    survivors from its own docstring changes."""
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return None
    lines: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.stmt):
            if (isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant)
                    and isinstance(node.value.value, str)):
                continue
            # a compound statement's span covers its body; take only the header
            end = node.end_lineno
            if isinstance(node, (ast.If, ast.For, ast.While, ast.With, ast.Try,
                                 ast.FunctionDef, ast.AsyncFunctionDef,
                                 ast.ClassDef)):
                first = node.body[0].lineno if getattr(node, "body", None) else node.lineno
                end = max(node.lineno, first - 1)
            for ln in range(node.lineno, end + 1):
                lines.add(ln)
    return lines


class Mutant:
    def __init__(self, path: str, klass: str, desc: str, source: str,
                 lineno: int):
        self.path, self.klass, self.desc, self.source, self.lineno = (
            path, klass, desc, source, lineno)
        self.id = hashlib.sha256(
            f"{path}:{klass}:{lineno}:{desc}:{source}".encode()).hexdigest()[:10]
        self.literal_preserving = True
        self.killed = None
        self.crash = False
        self.seconds = None
        self.snippet = ""

    def as_dict(self):
        return {"id": self.id, "file": self.path, "class": self.klass,
                "line": self.lineno, "mutation": self.desc,
                "literal_preserving": self.literal_preserving,
                "killed": self.killed, "crash": self.crash,
                "seconds": self.seconds, "snippet": self.snippet}


# --------------------------------------------------------------------------
# Python AST mutants, restricted to the ADDED lines
# --------------------------------------------------------------------------
def python_mutants(rel: str, fixed_text: str, added: set[int]) -> list[Mutant]:
    try:
        tree = ast.parse(fixed_text)
    except SyntaxError:
        return []
    src = Src(fixed_text)
    out: list[Mutant] = []
    # Lines the author opted out of relevance mutation (allowlist class 3).
    annotated = {i + 1 for i, line in enumerate(src.lines)
                 if RELEVANCE_OPT_OUT.search(line)}

    def emit(klass, desc, a, b, new, lineno):
        if lineno in annotated:
            # `# relevance: unobservable` on this line -- author-declared benign.
            return
        text = src.replace(a, b, new)
        if text == fixed_text:
            return
        try:
            compile(text, rel, "exec")
        except SyntaxError:
            return
        m = Mutant(rel, klass, desc, text, lineno)
        lines = text.split("\n")
        m.snippet = lines[lineno - 1].strip()[:120] if lineno - 1 < len(lines) else ""
        out.append(m)

    def in_added(node) -> bool:
        return getattr(node, "lineno", None) in added

    # parent chain for the innocuous-call filter
    parents_of: dict[int, list] = {}

    def walk(node, parents):
        for child in ast.iter_child_nodes(node):
            parents_of[id(child)] = parents + [node]
            walk(child, parents + [node])
    walk(tree, [])

    # ---- statements -------------------------------------------------------
    for node in ast.walk(tree):
        body_lists = []
        for field in ("body", "orelse", "finalbody"):
            lst = getattr(node, field, None)
            if isinstance(lst, list) and lst and isinstance(lst[0], ast.stmt):
                body_lists.append(lst)
        for lst in body_lists:
            for stmt in lst:
                if not in_added(stmt) or _is_innocuous_stmt(stmt):
                    continue
                a, b = src.span(stmt)
                # partial revert: delete the added statement
                if len(lst) > 1 or not isinstance(stmt, (ast.Return,)):
                    emit("stmt-delete", f"delete `{src.get(a, b).splitlines()[0][:60]}`",
                         a, b, "pass", stmt.lineno)
                if isinstance(stmt, (ast.If, ast.While)):
                    ta, tb = src.span(stmt.test)
                    t = src.get(ta, tb)
                    emit("cond-negate", f"negate `{t[:60]}`", ta, tb,
                         f"not ({t})", stmt.lineno)
                    emit("cond-force-true", f"force `{t[:60]}` True", ta, tb,
                         "True", stmt.lineno)
                    emit("cond-force-false", f"force `{t[:60]}` False", ta, tb,
                         "False", stmt.lineno)
                if isinstance(stmt, ast.Return) and stmt.value is not None:
                    va, vb = src.span(stmt.value)
                    v = src.get(va, vb)
                    if isinstance(stmt.value, ast.Constant) and isinstance(
                            stmt.value.value, bool):
                        emit("return-flip", f"return {not stmt.value.value}",
                             va, vb, str(not stmt.value.value), stmt.lineno)
                    else:
                        emit("return-none", f"return None instead of `{v[:60]}`",
                             va, vb, "None", stmt.lineno)
                        if isinstance(stmt.value, (ast.Compare, ast.BoolOp,
                                                   ast.UnaryOp, ast.Call)):
                            emit("return-negate", f"return not ({v[:60]})",
                                 va, vb, f"not ({v})", stmt.lineno)
                if isinstance(stmt, ast.AugAssign):
                    op = type(stmt.op)
                    if op in BIN_FLIPS:
                        ta, tb = src.span(stmt.target)
                        va, vb = src.span(stmt.value)
                        between = src.get(tb, va)
                        cur = BIN_TEXT[op] + "="
                        if cur in between:
                            new = between.replace(cur, BIN_FLIPS[op][0] + "=", 1)
                            emit("augassign-op", f"{cur} -> {BIN_FLIPS[op][0]}=",
                                 tb, va, new, stmt.lineno)

    # ---- expressions ------------------------------------------------------
    for node in ast.walk(tree):
        if not in_added(node):
            continue
        parents = parents_of.get(id(node), [])
        if _inside_innocuous_call(node, parents):
            continue
        if isinstance(node, ast.Compare):
            prev = node.left
            for op, comp in zip(node.ops, node.comparators):
                pa, pb = src.span(prev)
                ca, cb = src.span(comp)
                between = src.get(pb, ca)
                cur = CMP_TEXT[type(op)]
                if cur in between:
                    for new_op in CMP_FLIPS[type(op)]:
                        new = between.replace(cur, new_op, 1)
                        emit("compare-flip", f"{cur} -> {new_op}", pb, ca, new,
                             node.lineno)
                prev = comp
        elif isinstance(node, ast.BoolOp):
            cur = "and" if isinstance(node.op, ast.And) else "or"
            new_op = "or" if cur == "and" else "and"
            a0, b0 = src.span(node.values[0])
            a1, b1 = src.span(node.values[1])
            between = src.get(b0, a1)
            if re.search(rf"\b{cur}\b", between):
                emit("boolop-swap", f"{cur} -> {new_op}", b0, a1,
                     re.sub(rf"\b{cur}\b", new_op, between, count=1), node.lineno)
        elif isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
            a, b = src.span(node)
            t = src.get(a, b)
            if t.startswith("not "):
                emit("not-drop", f"drop `not` in `{t[:60]}`", a, b, t[4:],
                     node.lineno)
        elif isinstance(node, ast.BinOp) and type(node.op) in BIN_FLIPS:
            la, lb = src.span(node.left)
            ra, rb = src.span(node.right)
            between = src.get(lb, ra)
            cur = BIN_TEXT[type(node.op)]
            if cur in between:
                for new_op in BIN_FLIPS[type(node.op)]:
                    emit("binop-swap", f"{cur} -> {new_op}", lb, ra,
                         between.replace(cur, new_op, 1), node.lineno)
        elif isinstance(node, ast.Constant):
            if _inside_formatting_call(node, parents):
                continue
            v = node.value
            a, b = src.span(node)
            parent = parents[-1] if parents else None
            if isinstance(v, bool):
                emit("const-bool", f"{v} -> {not v}", a, b, str(not v), node.lineno)
            elif isinstance(v, int) and not isinstance(parent, ast.JoinedStr):
                if isinstance(parent, ast.BoolOp) and parent.values[-1] is node:
                    # A FALLBACK constant (`item.get(k) or 0`). Nudging it by
                    # one is almost always an equivalent mutant: 0 -> 1 is
                    # still under any threshold. The property-breaking form
                    # is "a wrong default", so push it far enough to cross
                    # whatever boundary the code compares against.
                    nv = v + 1000003
                    emit("const-default", f"fallback {v} -> {nv}", a, b, str(nv),
                         node.lineno)
                elif (isinstance(parent, ast.Call)
                        and isinstance(parent.func, ast.Attribute)
                        and parent.func.attr == "get"
                        and len(parent.args) == 2 and parent.args[-1] is node):
                    # A dict `.get(key, DEFAULT)` fallback -- same class as the
                    # `or 0` case above: nudging the default by one is an
                    # equivalent mutant (a missing key defaulting to 0 vs 1 is
                    # still under any real threshold). The property-breaking
                    # form is a wrong default, so push it across the boundary.
                    nv = v + 1000003
                    emit("const-default", f"get-default {v} -> {nv}", a, b,
                         str(nv), node.lineno)
                else:
                    for nv in (v + 1, v - 1):
                        emit("const-int", f"{v} -> {nv}", a, b, str(nv), node.lineno)
            elif isinstance(v, float):
                for nv in (v + 1.0, v * 2 if v else 1.0):
                    emit("const-float", f"{v} -> {nv}", a, b, repr(nv), node.lineno)
            elif isinstance(v, str) and isinstance(
                    parent, (ast.Compare, ast.Subscript, ast.Return, ast.Assign,
                             ast.Dict, ast.keyword)):
                # Only where a string plausibly carries behaviour (a key, a
                # compared value, a returned value) -- never inside messages.
                # Allowlist class 2: a prose kwarg (`detail=`, `message=`) is a
                # human message, not the property -- skip its const-str even
                # though the enclosing call (e.g. HTTPException) is behavioural.
                if isinstance(parent, ast.keyword) and parent.arg in MESSAGE_KWARGS:
                    continue
                q = src.get(a, b)[0]
                if q in ("'", '"') and "\n" not in v:
                    emit("const-str", f"{v!r} -> {v + '_X'!r}", a, b,
                         q + v + "_X" + q, node.lineno)
    return out


# --------------------------------------------------------------------------
# TypeScript / JavaScript AST-span mutants (via the ts-mutator.mjs sidecar)
# --------------------------------------------------------------------------
# The Node sidecar walks the TS compiler-API AST and returns, for the ADDED
# lines only, the same class of property-breaking mutants python_mutants makes
# (compare-flip / boolop-swap / const-* / cond-* / stmt-delete / call-delete),
# each already validated to re-parse. It returns the FULL mutated file text per
# mutant, so this side just wraps it in a Mutant exactly as the Python path does
# -- no cross-language offset math. A single-hunk TS/JS refimpl now yields real
# span mutants (the old "non-Python single hunk -> UNPROVEN" limit is gone for
# these extensions). If Node or the sidecar is unavailable the function returns
# [] and the run falls back to hunk reverts (i.e. UNPROVEN on one hunk) -- the
# safe direction, never a spurious pass.
TS_EXTS = (".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs")


def _ts_mutator_path() -> Path | None:
    env = os.environ.get("TS_MUTATOR")
    if env:
        p = Path(env).expanduser()
        return p if p.is_file() else None
    here = Path(__file__).resolve().parent
    for cand in (here / "ts-mutator.mjs", here / "ts-mutator" / "ts-mutator.mjs"):
        if cand.is_file():
            return cand
    return None


def ts_mutants(worktree: Path, rel: str, fixed_text: str,
               added: set[int]) -> list[Mutant]:
    """Mirror of python_mutants for .ts/.tsx/.js/.jsx/.mjs/.cjs via the sidecar.

    The mutated file text is produced by the sidecar (it holds the AST); this
    function only re-homes each mutant into a Mutant with the Python snippet
    convention so scoring, literal-preserving filtering and reporting are byte
    identical to the Python path.
    """
    script = _ts_mutator_path()
    if script is None:
        return []
    target = worktree / rel
    payload = json.dumps({"path": str(target), "addedLines": sorted(added)})
    try:
        p = subprocess.run(["node", str(script)], input=payload,
                           capture_output=True, text=True, timeout=120)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return []
    if p.returncode != 0 or not p.stdout.strip():
        return []
    try:
        data = json.loads(p.stdout)
    except json.JSONDecodeError:
        return []
    out: list[Mutant] = []
    for md in data.get("mutants", []):
        source = md.get("source")
        lineno = md.get("line")
        if source is None or source == fixed_text or lineno is None:
            continue
        m = Mutant(rel, md.get("class", "ts-mutant"), md.get("desc", ""),
                   source, lineno)
        lines = source.split("\n")
        m.snippet = lines[lineno - 1].strip()[:120] if lineno - 1 < len(lines) else ""
        out.append(m)
    return out


# --------------------------------------------------------------------------
# language-agnostic hunk reverts
# --------------------------------------------------------------------------
def hunk_revert_mutants(rel: str, fixed_text: str, hunks: list[dict],
                        code_lines: set[int] | None = None) -> list[Mutant]:
    """Revert ONE hunk of the refimpl while keeping the others: the partial
    revert. Needs >= 2 material hunks, otherwise it is the full revert (already
    proven by baseline-fails) and says nothing new."""
    def material_hunk(h):
        if code_lines is not None:
            new_nos = range(h["new_start"], h["new_start"] + len(h["new_lines"]))
            return any(n in code_lines for n in new_nos)
        return (not _is_comment_only(h["new_lines"])
                or not _is_comment_only(h["old_lines"]))
    material = [h for h in hunks if material_hunk(h)]
    if len(material) < 2:
        return []
    out = []
    lines = fixed_text.split("\n")
    for h in material:
        start = h["new_start"] - 1
        n_new = len(h["new_lines"])
        # sanity: the fixed text must carry the hunk where the diff says
        if lines[start:start + n_new] != h["new_lines"]:
            continue
        new_lines = lines[:start] + h["old_lines"] + lines[start + n_new:]
        text = "\n".join(new_lines)
        m = Mutant(rel, "hunk-revert",
                   f"revert hunk @{h['new_start']} ({n_new} added line(s))",
                   text, h["new_start"])
        m.snippet = (h["new_lines"][0].strip()[:120] if h["new_lines"] else "")
        out.append(m)
    return out


# --------------------------------------------------------------------------
# running
# --------------------------------------------------------------------------
def _run_verify(cmd: str, cwd: Path, timeout: int) -> tuple[int, str, float]:
    env = dict(os.environ)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    t0 = time.time()
    try:
        p = subprocess.run(cmd, shell=True, cwd=str(cwd), env=env,
                           capture_output=True, text=True, timeout=timeout)
        out = p.stdout + p.stderr
        rc = p.returncode
    except subprocess.TimeoutExpired as e:
        out = ((e.stdout or b"").decode("utf-8", "replace")
               if isinstance(e.stdout, bytes) else (e.stdout or "")) + "\n[timeout]"
        rc = 124
    return rc, out, time.time() - t0


def _clear_pycache(root: Path):
    for d in root.rglob("__pycache__"):
        if ".venv" in d.parts or "venv" in d.parts:
            continue
        shutil.rmtree(d, ignore_errors=True)


def _green(rc: int, out: str) -> bool:
    return rc == 0 and "VERIFY_OK" in out


def must_contain_literals(task_text: str) -> list[str]:
    """Same extraction as ollama-dispatch-preflight.must_contain_literals."""
    m = re.search(r"##+\s*Must contain[^\n]*\n(.*?)(?=\n##\s|\Z)",
                  task_text, re.S | re.I)
    if not m:
        return []
    return [b.group(1) for b in re.finditer(r"`([^`\n]{4,200})`", m.group(1))]


_GREP_RE = re.compile(r"""\bgrep\b(?:\s+-[A-Za-z]+)*\s+(?:-e\s+)?(?:"((?:[^"\\]|\\.)*)"|'([^']*)')""")
_RUNS_RE = re.compile(r"""(?:^|[|;&(]\s*)(?:bash|sh|python3?|"?\$\{?\w+\}?"?)\s+(?:-\S+\s+)*([\w./-]+\.(?:sh|py))""", re.M)


def verify_grep_literals(worktree: Path, verify_cmd: str) -> list[str]:
    """Literals the verify itself GREPS for, in verify.sh and scripts it runs.

    Why these join the literal-preserving filter: a mutant that deletes the
    exact line a proxy verify greps for is "killed" -- by textual coincidence,
    not by any behaviour. Measured before this extractor existed, a pure
    grep-two-lines verify scored 0.45 because half the mutants happened to
    rewrite one of its two grepped lines. Excluding grep-asserted text from the
    evidence set is what makes a grep proxy score what it deserves. It cannot
    see every proxy shape (a Python `"x" in open(f).read()` is not extracted);
    those still score low because most mutants leave their literal intact.
    Unescapes the common grep escapes (\\* \\. \\[) so the text matches source.
    """
    texts = []
    frontier = [verify_cmd]
    seen = set()
    for _ in range(3):
        nxt = []
        for chunk in frontier:
            for m in _RUNS_RE.finditer(chunk):
                rel = m.group(1)
                if rel in seen:
                    continue
                seen.add(rel)
                f = worktree / rel
                if f.is_file():
                    try:
                        t = f.read_text(errors="replace")
                    except Exception:
                        continue
                    texts.append(t)
                    nxt.append(t)
        frontier = nxt
    out = []
    for t in texts:
        for m in _GREP_RE.finditer(t):
            lit = m.group(1) if m.group(1) is not None else m.group(2)
            lit = re.sub(r"\\([*.\[\]()+?^$|])", r"\1", lit)
            if lit and len(lit) >= 3 and not lit.startswith("-"):
                out.append(lit)
    return sorted(set(out))


def generate(worktree: Path, diff_text: str, literals) -> tuple[list[Mutant], dict]:
    """All candidate mutants for the applied refimpl, plus generation notes."""
    files = parse_unified_diff(diff_text)
    mutants: list[Mutant] = []
    notes = {"files": {}, "skipped_files": []}
    for rel, info in files.items():
        p = worktree / rel
        if not p.is_file():
            notes["skipped_files"].append(f"{rel} (not a file)")
            continue
        try:
            fixed = p.read_text()
        except Exception as e:
            notes["skipped_files"].append(f"{rel} ({type(e).__name__})")
            continue
        ms = []
        code_lines = None
        if rel.endswith(".py"):
            ms += python_mutants(rel, fixed, info["added"])
            code_lines = python_code_lines(fixed)
        elif rel.endswith(TS_EXTS):
            ms += ts_mutants(worktree, rel, fixed, info["added"])
        ms += hunk_revert_mutants(rel, fixed, info["hunks"], code_lines)
        # filter 2: a mutant that drops a Must-contain literal is not evidence.
        # Only literals the FIXED file actually contains can be BROKEN by a
        # mutant of it. A literal absent from the target -- e.g. a target PATH or
        # a log-pattern the verify greps out of tsc/stderr OUTPUT rather than out
        # of source (the TS/JS verifies do exactly this: `grep -F <target>` and
        # an ERR_MODULE regex against a build log) -- is otherwise "absent from
        # every mutant" and marks the WHOLE set literal-breaking, starving the
        # evidence set to zero and forcing a spurious UNPROVEN. Restricting to
        # file-present literals is strictly safe: it can only ADD evidence
        # mutants (raising the kill bar), never license a spurious "relevant".
        file_lits = [l for l in literals if l in fixed]
        for m in ms:
            m.literal_preserving = all(l in m.source for l in file_lits)
        # dedupe identical sources
        seen = set()
        uniq = []
        for m in ms:
            if m.source in seen:
                continue
            seen.add(m.source)
            uniq.append(m)
        notes["files"][rel] = {"added_lines": len(info["added"]),
                               "hunks": len(info["hunks"]),
                               "mutants": len(uniq)}
        mutants += uniq
    return mutants, notes


def _site_of(m: Mutant) -> tuple:
    return (m.path, m.lineno)


def _balanced_sample(mutants: list[Mutant], cap: int) -> list[Mutant]:
    """Order mutants so that EVERY SITE is tried before any site is tried
    twice, rotating classes within a site; then truncate to cap.

    The first version balanced across CLASSES only. Measured (2026-09-03,
    test-verify-relevance's own relevant fixture, 11 mutants over 3 sites):
    with --max-mutants 3 or 5 it tried sites L7 and L9 seven times between
    them and never touched L8 (`return False`), and still said RELEVANT with
    truncated=true. On the real wt-tokens dispatch the cap of 40 spent seven
    mutants on one line and left three whole hunks untried. A verdict over a
    sample that skipped a site is a verdict about the verify's coverage of
    SOME of the change; measure_applied() now reports the untried sites and
    refuses to call such a run relevant.

    Ordering is deterministic (sorted sites, sorted classes) so a re-run is
    the same run. Always returns the FULL ordering when nothing is dropped.
    """
    by_site: dict[tuple, dict[str, list[Mutant]]] = {}
    for m in mutants:
        by_site.setdefault(_site_of(m), {}).setdefault(m.klass, []).append(m)
    # per-site round-robin over classes
    per_site: dict[tuple, list[Mutant]] = {}
    for s, by_k in by_site.items():
        order, seq, i = sorted(by_k), [], 0
        while True:
            progressed = False
            for k in order:
                if i < len(by_k[k]):
                    seq.append(by_k[k][i]); progressed = True
            if not progressed:
                break
            i += 1
        per_site[s] = seq
    sites = sorted(per_site)
    out: list[Mutant] = []
    i = 0
    while len(out) < len(mutants):
        progressed = False
        for s in sites:
            if i < len(per_site[s]):
                out.append(per_site[s][i]); progressed = True
        if not progressed:
            break
        i += 1
    return out[:cap]


def measure_applied(worktree: Path, verify_cmd: str, diff_text: str, *,
                    literals=(), threshold=DEFAULT_THRESHOLD,
                    min_mutants=DEFAULT_MIN_MUTANTS, max_mutants=DEFAULT_MAX_MUTANTS,
                    budget_s=DEFAULT_BUDGET_S, verify_timeout=VERIFY_TIMEOUT_S,
                    skip_green_check=False, progress=None) -> dict:
    """Measure relevance on a tree that ALREADY carries the reference impl.

    Every mutated file is restored to its fixed content after each mutant, so
    the tree leaves exactly as it came (still carrying the fix). The caller
    owns applying and reverting the refimpl -- the preflight gate already does
    both around this call.
    """
    worktree = Path(worktree)
    t_start = time.time()
    rec = {
        "verdict": "unproven", "score": None, "threshold": threshold,
        "min_mutants": min_mutants, "evidence_mutants": 0, "killed": 0,
        "survived": 0, "crash_kills": 0, "literal_breaking": 0,
        "generated": 0, "tried": 0, "truncated": False, "survivors": [],
        "mutants": [], "reason": "", "seconds": 0.0,
    }
    if not skip_green_check:
        rc, out, secs = _run_verify(verify_cmd, worktree, verify_timeout)
        if not _green(rc, out):
            rec["reason"] = (f"the reference impl does not turn the verify green "
                             f"(exit {rc}); nothing to mutate against")
            rec["seconds"] = time.time() - t_start
            return rec
    grep_lits = verify_grep_literals(worktree, verify_cmd)
    all_lits = list(dict.fromkeys(list(literals) + grep_lits))
    mutants, notes = generate(worktree, diff_text, all_lits)
    notes["literals_from_task"] = list(literals)
    notes["literals_from_verify_grep"] = grep_lits
    rec["generation"] = notes
    rec["generated"] = len(mutants)
    evidence = [m for m in mutants if m.literal_preserving]
    breaking = [m for m in mutants if not m.literal_preserving]
    rec["literal_breaking"] = len(breaking)
    if not evidence:
        _has_ast = any(f.endswith(".py") or f.endswith(TS_EXTS)
                       for f in notes["files"])
        rec["reason"] = ("no literal-preserving mutant could be generated for the "
                         "added lines" + ("" if _has_ast else
                                          " (target with no AST mutator and < 2 "
                                          "hunks; a TS/JS target here means the "
                                          "ts-mutator sidecar or Node was "
                                          "unavailable)"))
        rec["seconds"] = time.time() - t_start
        return rec
    sample = _balanced_sample(evidence, max_mutants)
    rec["truncated"] = len(sample) < len(evidence)
    originals: dict[str, str] = {}
    try:
        for i, m in enumerate(sample):
            if time.time() - t_start > budget_s:
                rec["truncated"] = True
                rec["reason"] = f"time budget {budget_s}s exhausted after {i} mutant(s)"
                break
            p = worktree / m.path
            if m.path not in originals:
                originals[m.path] = p.read_text()
            p.write_text(m.source)
            _clear_pycache(p.parent)
            rc, out, secs = _run_verify(verify_cmd, worktree, verify_timeout)
            p.write_text(originals[m.path])
            _clear_pycache(p.parent)
            m.killed = not _green(rc, out)
            m.crash = m.killed and ("Traceback (most recent call last)" in out)
            m.seconds = round(secs, 2)
            rec["tried"] += 1
            if progress:
                progress(m)
    finally:
        for rel, text in originals.items():
            try:
                (worktree / rel).write_text(text)
            except Exception:
                pass
        _clear_pycache(worktree)
    tried = [m for m in sample if m.killed is not None]
    rec["mutants"] = [m.as_dict() for m in mutants]
    rec["evidence_mutants"] = len(tried)
    rec["killed"] = sum(1 for m in tried if m.killed)
    rec["crash_kills"] = sum(1 for m in tried if m.crash)
    rec["survived"] = len(tried) - rec["killed"]
    rec["survivors"] = [m.as_dict() for m in tried if not m.killed]
    rec["seconds"] = round(time.time() - t_start, 2)
    # THREE SCORES, and the verdict takes the LOWEST.
    #   mutant_score   killed / tried. Overweights a complex line: one untested
    #                  `if a + b == 0:` yields eight mutants and sinks the score
    #                  eight times for one hole.
    #   site_score     mean over changed lines of (killed / tried on that line).
    #                  One hole counts once; one equivalent mutant on a line of
    #                  three only costs a third of a site.
    #   site_coverage  fraction of tried sites with AT LEAST ONE kill. This is
    #                  the one that sees a single-mutant hole: on wt-tokens
    #                  with only the `claude_tokens` alias case removed -- a
    #                  "must NOT change" item in the task -- L247's one mutant
    #                  (const-str) survived and the other two scores still
    #                  said RELEVANT 0.9 / 0.925 (measured 2026-09-03).
    # Neither of the first two is "the" truth, so the check reports all three
    # and gates on min() -- the direction that flags.
    #
    # An UNEXERCISED SITE -- every mutant tried on a line survived -- is the
    # most actionable finding this tool makes (the verify never runs that
    # path), and it is now a verdict, not just a report: any unexercised site
    # makes the run LOW whatever the averages say, because at twenty sites a
    # whole untested property is a 5% dent in every mean. The cost is a false
    # LOW on a line whose only mutant is equivalent (a dead store, say); the
    # site is named, so that is a ten-second human look. Threshold: >= 1
    # tried mutant (was 2, which is how L247 above slipped).
    #
    # UNTRIED SITES -- generated evidence but the cap or the time budget ran
    # out before the site was reached -- make the run UNPROVEN, never
    # relevant: the sample says nothing about those lines. Site-first ordering
    # (_balanced_sample) means this only happens when cap < number of sites
    # or the budget is exhausted inside the first pass.
    sites: dict[tuple, list] = {}
    for m in tried:
        sites.setdefault(_site_of(m), []).append(m)
    all_sites = {_site_of(m) for m in evidence}
    untried = sorted(f"{k[0]}:{k[1]}" for k in all_sites - set(sites))
    site_scores = {f"{k[0]}:{k[1]}": round(sum(1 for m in v if m.killed) / len(v), 3)
                   for k, v in sites.items()}
    rec["site_scores"] = site_scores
    rec["untried_sites"] = untried
    rec["unexercised_sites"] = sorted(
        f"{k[0]}:{k[1]}" for k, v in sites.items()
        if not any(m.killed for m in v))
    if len(tried) < min_mutants:
        rec["reason"] = rec["reason"] or (
            f"only {len(tried)} evidence mutant(s) were tried; {min_mutants} "
            f"needed before a verdict means anything")
        rec["verdict"] = "unproven"
        rec["score"] = (rec["killed"] / len(tried)) if tried else None
        return rec
    rec["mutant_score"] = round(rec["killed"] / len(tried), 3)
    rec["site_score"] = round(sum(site_scores.values()) / len(site_scores), 3)
    rec["site_coverage"] = round(
        sum(1 for v in sites.values() if any(m.killed for m in v)) / len(sites), 3)
    rec["score"] = min(rec["mutant_score"], rec["site_score"], rec["site_coverage"])
    if untried:
        rec["verdict"] = "unproven"
        rec["reason"] = (f"{len(untried)} changed site(s) were never tried "
                         f"({', '.join(untried[:4])}{'...' if len(untried) > 4 else ''}) "
                         f"-- the cap or the time budget ran out before them, so "
                         f"the sample says nothing about those lines. Raise "
                         f"--max-mutants/--budget-s.")
        return rec
    if rec["unexercised_sites"]:
        rec["verdict"] = "low"
        u = rec["unexercised_sites"]
        rec["reason"] = (f"{len(u)} changed site(s) are UNEXERCISED -- every mutant "
                         f"on {', '.join(u[:4])}{'...' if len(u) > 4 else ''} survived, "
                         f"so the verify never runs that path (killed "
                         f"{rec['killed']}/{len(tried)} elsewhere)")
        return rec
    if rec["score"] >= threshold:
        rec["verdict"] = "relevant"
        rec["reason"] = (f"verify killed {rec['killed']}/{len(tried)} property-"
                         f"breaking mutants of the reference impl")
    else:
        rec["verdict"] = "low"
        rec["reason"] = (f"verify let {rec['survived']}/{len(tried)} property-"
                         f"breaking mutants of the reference impl pass -- it is "
                         f"testing a proxy, or a benign slice, of the property")
    return rec


# --------------------------------------------------------------------------
# CLI: apply / revert around measure_applied
# --------------------------------------------------------------------------
def _git(wt: Path, *a) -> tuple[int, str]:
    p = subprocess.run(["git", "-C", str(wt), *a], capture_output=True, text=True)
    return p.returncode, p.stdout + p.stderr


def _apply(wt: Path, patch: str | None, cmd: str | None) -> tuple[bool, str]:
    if patch:
        pp = Path(patch).expanduser().resolve()
        rc, out = _git(wt, "apply", str(pp))
        if rc != 0:
            p2 = subprocess.run(["patch", "-p1", "-i", str(pp)], cwd=str(wt),
                                capture_output=True, text=True)
            if p2.returncode != 0:
                return False, out[-300:]
        return True, ""
    rc, out, _ = _run_verify(cmd, wt, 600)
    return rc == 0, out[-300:]


def _untracked(wt: Path) -> set[str]:
    rc, out = _git(wt, "ls-files", "--others", "--exclude-standard")
    return {l.strip() for l in out.splitlines() if l.strip()}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("worktree")
    ap.add_argument("--verify", default="bash verify.sh")
    ap.add_argument("--task-file", default="TASK.md")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--refimpl", help="reference-impl patch")
    g.add_argument("--refimpl-cmd", help="command that writes the reference impl")
    g.add_argument("--applied", action="store_true",
                   help="the tree already carries the fix as uncommitted "
                        "changes; measure in place and leave it so")
    ap.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD)
    ap.add_argument("--min-mutants", type=int, default=DEFAULT_MIN_MUTANTS)
    ap.add_argument("--max-mutants", type=int, default=DEFAULT_MAX_MUTANTS)
    ap.add_argument("--budget-s", type=int, default=DEFAULT_BUDGET_S)
    ap.add_argument("--json", action="store_true")
    ap.add_argument("-v", action="store_true", help="print each mutant as it runs")
    a = ap.parse_args()
    if not (a.refimpl or a.refimpl_cmd or a.applied):
        ap.error("need --refimpl, --refimpl-cmd or --applied")

    wt = Path(a.worktree).expanduser().resolve()
    task = wt / a.task_file
    literals = must_contain_literals(task.read_text()) if task.is_file() else []

    before = _untracked(wt)
    if not a.applied:
        rc, out = _git(wt, "status", "--porcelain")
        if any(l[:2] != "??" for l in out.splitlines() if l.strip()):
            print("tree is not at baseline (tracked modifications); refusing to "
                  "apply a refimpl on top", file=sys.stderr)
            return 2
        ok, why = _apply(wt, a.refimpl, a.refimpl_cmd)
        if not ok:
            print(f"could not apply the reference impl: {why}", file=sys.stderr)
            return 2

    # New/untracked files the refimpl CREATED do not appear in `git diff` at
    # all, so their lines would never be counted as "added" and never mutated
    # (the new-file blind spot). Stage them intent-to-add (`git add -N`) so the
    # diff reports their full contents as added lines -- scoped to EXACTLY the
    # files the refimpl added, i.e. those that appeared since `before`. This is
    # only decidable in the --refimpl / --refimpl-cmd paths, where `before` is a
    # clean pre-apply snapshot; in --applied mode the fix is already present, so
    # there is no baseline to tell a fix-created file from unrelated untracked
    # scaffolding -- staging there would mutate files the fix never touched, so
    # we do not (the safe direction). Pre-existing tracked code the diff did not
    # touch is never pulled in either way.
    new_untracked = [] if a.applied else sorted(_untracked(wt) - before)
    staged_ita: list[str] = []
    if new_untracked:
        rc, _ = _git(wt, "add", "-N", "--", *new_untracked)
        if rc == 0:
            staged_ita = new_untracked

    def _unstage_ita():
        # undo the intent-to-add so the files return to untracked (leaving the
        # working-tree bytes intact); the finally block then removes the ones
        # the refimpl created, or, in --applied mode, leaves the fix in place.
        if staged_ita:
            _git(wt, "reset", "-q", "--", *staged_ita)

    rc, diff_text = _git(wt, "diff", "-U0")
    if not diff_text.strip():
        # nothing tracked AND no new files staged above: truly nothing to mutate
        print("the reference impl produced no tracked diff; nothing to mutate",
              file=sys.stderr)
        _unstage_ita()
        if not a.applied:
            _git(wt, "checkout", "--", ".")
        return 3

    def prog(m):
        if a.v:
            print(f"  {'KILLED ' if m.killed else 'SURVIVE'} {m.klass:<16} "
                  f"{m.path}:{m.lineno} {m.desc}"
                  f"{' [crash]' if m.crash else ''}")

    try:
        rec = measure_applied(wt, a.verify, diff_text, literals=literals,
                              threshold=a.threshold, min_mutants=a.min_mutants,
                              max_mutants=a.max_mutants, budget_s=a.budget_s,
                              progress=prog)
    finally:
        _unstage_ita()
        if not a.applied:
            _git(wt, "checkout", "--", ".")
            for rel in sorted(_untracked(wt) - before):
                p = wt / rel
                try:
                    shutil.rmtree(p) if p.is_dir() else p.unlink()
                except Exception:
                    pass
    if a.json:
        print(json.dumps(rec, indent=2))
    else:
        print(f"verify-relevance: {rec['verdict'].upper()}  score="
              f"{rec['score'] if rec['score'] is not None else '-'} "
              f"(killed {rec['killed']}/{rec['evidence_mutants']} evidence mutants; "
              f"{rec['literal_breaking']} literal-breaking excluded; "
              f"{rec['crash_kills']} crash kills; {rec['seconds']}s)")
        print(f"  {rec['reason']}")
        if rec.get("site_score") is not None:
            print(f"  mutant_score={rec['mutant_score']} site_score={rec['site_score']} "
                  f"site_coverage={rec.get('site_coverage')} (verdict uses the lowest)")
        for u in rec.get("untried_sites", []):
            print(f"  UNTRIED {u}: the cap/budget ran out before this line was "
                  f"mutated -- nothing is known about it")
        for u in rec.get("unexercised_sites", []):
            print(f"  UNEXERCISED {u}: every mutant on this line survived -- the "
                  f"verify never runs this path")
        for s in rec["survivors"]:
            print(f"  SURVIVOR {s['class']:<16} {s['file']}:{s['line']} "
                  f"{s['mutation']}  ->  {s['snippet']}")
    return {"relevant": 0, "low": 1, "unproven": 3}[rec["verdict"]]


if __name__ == "__main__":
    sys.exit(main())
