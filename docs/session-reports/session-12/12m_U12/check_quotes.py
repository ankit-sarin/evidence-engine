"""12m-U12-READ step 1 — the brief's READ quotes against the plan on disk. Read-only.
usage, from the repository root: check_quotes.py   (stdout: one line per quote, count of occurrences)"""
s = open("docs/plan/ENGINE_REFACTOR_PLAN.md", encoding="utf-8").read()
Q = {
 "A-1 statement": "`foreign_lock_held()` passes when nobody holds the lock, and only `run_extraction` and the eval harnesses hold it — screeners, auditor, vision parse, the judge passes and the nightly's model tier can be restarted beneath. `pass2_full` restarts on a cadence and takes no lock.",
 "A-1 fix": "Have every model-calling entry point hold the lock.",
 "A-1 note W1-F5-C2": "The restart gate is a probe released immediately, not a hold across the restart",
 "A-1N statement": "The nightly neither takes nor checks the experiment lock and runs the model tier; the suite fence stops it restarting Ollama but not loading models.",
 "A-1N R612(q)": "U12's handoff reads the citation-mcp nightly harness read-only and reports whether it loads Ollama models (N20).",
 "B-F17 part 1": "10 of 15 requirements unpinned (incl. `pydantic`); `requests`, `httpx`, PyMuPDF, `fontTools`, `pandas` imported but undeclared; `pytest` declared nowhere",
 "B-F17 part 2": "creating a **fresh** review database imports both provider SDKs through migration 014 and fails without them.",
 "B-F17 D13 note": "`scripts/nightly_tests.sh` has `set -euo pipefail`, so a failing run already exits non-zero; what holds is that the nightly runs every tier with no marker expression and nothing reads its status.",
 "INT-g12-7 (with the brief's final period)": "An outer `flock(1)` on the lock file deadlocks `run_extraction`. A constraint on how A-1's fix (R528) and any lock-holding cron job are built.",
 "INT-g12-7 (without it)": "An outer `flock(1)` on the lock file deadlocks `run_extraction`. A constraint on how A-1's fix (R528) and any lock-holding cron job are built",
 "INT-l12-14 R614(a)": "the manifest's end_reason names the exception class and message.",
 "INT-l12-14 site a": "`scripts/run_pipeline.py::run_pipeline`",
 "INT-l12-14 site b": "`except Exception as exc:`",
 "INT-l12-14 site c": '`_finish_review_run(db, run_id, "failed")`',
}
for k, q in Q.items():
    print(f"{s.count(q):2d}  {k}")
