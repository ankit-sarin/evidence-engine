"""C1: an exception raised AFTER the paper's events are committed, still inside
_extract_selected's per-paper try, is recorded as the paper's extraction failure.
The transient fault here is one failed stderr write in ProgressReporter.report."""
from unittest.mock import patch
from _w1f1_setup import *
from engine.agents import extractor as ex
from engine.core.effective import effective_state, live_claim_events
from engine.core.selection import select_for_extraction
from engine.utils import progress

tmp, db, cb, spec, run_id = make_review()
real_report, fired = progress.ProgressReporter.report, []

def flaky_report(self, paper_id, status, elapsed):
    if status == "EXTRACTED" and not fired:
        fired.append(1)
        raise BlockingIOError(11, "write could not complete without blocking")   # one stderr write
    return real_report(self, paper_id, status, elapsed)

import io, sys
with client([resp("draft"), pass2(cb)]), \
     patch("engine.utils.ollama_preflight.require_preflight"), \
     patch.object(progress.ProgressReporter, "report", flaky_report), \
     patch.object(sys, "stderr", io.StringIO()):
    stats = ex.run_extraction(db, spec, "w1f1", restart_every=0, experiment_lock=False, run_id=run_id)

arm = spec.extraction_models.arm
st = effective_state(db._conn, 1)
print("model calls made:", db._conn.execute("SELECT COUNT(*) FROM run_calls").fetchone()[0])
print("stats:", {k: stats[k] for k in ("extracted", "failed", "total_spans")})
print("extractor paper events, in order:", paper_events(db))
print("live claims of the arm on the paper:", len(live_claim_events(db._conn, 1, arm)))
print("effective_state: processing=%s reason=%s analysis_ready=%s" % (
    st.processing, st.processing_reason, st.analysis_ready))
sel = select_for_extraction(db._conn, arm=arm)
print("next run's selection: to_extract=%s skipped_asserted=%s" % (
    [p for p, _ in sel.to_extract], list(sel.skipped_asserted)))
cleanup(tmp, db)
