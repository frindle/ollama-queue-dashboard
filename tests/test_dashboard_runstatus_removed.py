"""The Run Status panel is gone from the dashboard UI; the backend run routes and the
shared clear/verdict path (Stalled / failed panel, bundle inline buttons) are kept.
Reads the page source as text. DASHBOARD_SRC=<dir holding ollama-queue-api.py>."""
import os
from pathlib import Path

import pytest

SRC = Path(os.environ.get("DASHBOARD_SRC") or Path(__file__).resolve().parent.parent / "src") / "ollama-queue-api.py"
TEXT = SRC.read_text()

GONE = ['id="runPanel"', 'id="runStatusDetails"', 'id="runStatusSummary"', 'id="runStatus"',
        "#runStatus", "const runTbody", "function renderRunRow", "function renderRunTree",
        "function refreshRuns", "function rerenderRuns", "function renderParentGroup",
        "function acceptRun", "function fmtWhen", "__runStatusOpen", "setInterval(refreshRuns",
        "tr.run-parent", "tr.run-child", "refreshRuns"]
KEPT = ["function verdictClass(", "async function clearRun(", "async function openGateDetail(",
        "function verdictInline(", "function bundleVerdictBadge(", "'/api/runs/' + id + '/clear'",
        'id="stalledPanel"', 'id="stalledDetails"', 'self.path == "/api/runs/tree"']


@pytest.mark.parametrize("s", GONE)
def test_removed(s):
    assert s not in TEXT


@pytest.mark.parametrize("s", KEPT)
def test_kept(s):
    assert s in TEXT
