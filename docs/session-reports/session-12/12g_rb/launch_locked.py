"""12g front-half rehearsal launcher (R564): the whole run_pipeline run under the experiment lock.

Run from the repository root with PYTHONPATH=. :

    .venv/bin/python ~/scratch/12g-ra/launch_locked.py --check   # P3/P6: lock check only, no model call
    .venv/bin/python ~/scratch/12g-ra/launch_locked.py           # the run

No outer flock(1): run_extraction's own acquire is a blocking flock on a new file
description and would deadlock against it (RA-3). The lock is re-entrant in-process.
"""
import runpy
import sys
import time

from engine.utils.ollama_lock import (
    check_experiment_lock, foreign_lock_held, hold_experiment_lock, self_holds_lock,
)

ARGV = ["scripts/run_pipeline.py", "--review", "rb_12g", "--spec", "data/rb_12g/spec.yaml",
        "--skip-to", "screen"]

if "--check" in sys.argv:
    print("P6 lock held by anyone before acquire:", check_experiment_lock())
    t = time.monotonic()
    with hold_experiment_lock(blocking=False):
        print("outer acquired; self_holds_lock:", self_holds_lock(), "foreign_lock_held:", foreign_lock_held())
        with hold_experiment_lock():            # the blocking form run_extraction uses
            print("nested acquired; self_holds_lock:", self_holds_lock())
        print("after nested exit; self_holds_lock:", self_holds_lock())
    print("released; held by anyone:", check_experiment_lock(), "| elapsed %.3f s" % (time.monotonic() - t))
    sys.exit(0)

sys.argv = ARGV
with hold_experiment_lock(blocking=False):      # refuse at once if another experiment holds it
    runpy.run_path("scripts/run_pipeline.py", run_name="__main__")
