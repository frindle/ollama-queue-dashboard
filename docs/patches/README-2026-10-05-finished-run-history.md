# Finished-run history on the dashboard (2026-10-05)

Symptom: completed steps of a slice/bundle (author, refine rounds, coding, gate,
regate, second opinion, escalation review, landed) disappeared from the queue
dashboard; only active jobs were shown.

Root cause: `ollama-queue.py` prunes a clean `done` row from the queue state on the
next tick (`RETAIN_DONE_RECENT = 0`; only rows the slicer's plan index confirms are
kept), and clean-done gate/regate/secondop rows prune the same way. Every bundle view
(`/api/bundle-views`) was built from queue-state rows alone, so finished runs vanished,
and a bundle with no row left vanished from the queue panel entirely.

Fix (read-side only, the queue/CLI is untouched -- `status` output is unchanged):
* `bundle_view.load_history` rebuilds every launched job from the never-pruned
  livelogs + `.done.json`/`.gate.json` sidecars (incl. `archive/`) and the parent's
  `-review`/`-regate`/`-secondop` reports; views merge these as read-only history.
* A plan-less bundle groups author/refine/coding of one feature into one line.
* Done plan slices get an "accepted / landed" step.
* `GET /api/bundle-history?days=3&limit=20&offset=0`: bundles with no queue row,
  newest activity first, age-bounded and paged; the page shows them under
  "Finished bundles" with show-all-ages / show-more controls.

The live deployment runs from `~/bin` (a superset of this repo's `bin/`), so the change
is carried here as `2026-10-05-finished-run-history.patch` against those files, plus
`tests/test_bundle_history.py` (run with `--revert-check` to prove it fails pre-fix).
