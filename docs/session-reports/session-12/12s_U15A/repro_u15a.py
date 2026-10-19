"""12s-U15A — INT-i12-12 reproducers at HEAD, on disposable scratch databases. Evidence, not a test.
usage, from the repository root: .venv/bin/python -I repro_u15a.py <scratch dir>   (stdout: JSON)
Each case builds a fresh ReviewDatabase under <scratch dir>, writes claims and events through the
engine's own writers (the suite's fixture helpers, imported, not run as tests), and calls the
production audit_run with semantic_verify mocked, as tests/test_audit_events.py does. No model,
no network: socket.connect is replaced before any engine import. Nothing under data/ is opened.

Cases:
  row18_<state>  — 2(f): a paper whose latest processing event is <state>, above live unlocated
                   claims of the spec's arm (the 12i L03 reproducer's construction, widened to
                   every non-admitted state).
  d24_reparse    — 2(d): claims on text v1; v2 recorded; the extraction of v2 fails in the loop.
  d21_refusal    — 2(d): claims on text v1; v2 recorded and its file removed; selection refuses
                   the paper and record_selection_refusals writes its extraction_failed.
  same_key       — 2(d): claims on the current text; selection skips the paper as held; a failure
                   on the SAME key written directly (no production path does this, by selection).
  located_then_failure — claims already located by a first audit, then a failure event; a second
                   audit."""
import json, shutil, socket, sys
from pathlib import Path
from unittest.mock import patch

def _no_network(*a, **k): raise RuntimeError("network attempt in a reproducer")
socket.socket.connect = _no_network
REPO = Path.cwd(); sys.path.insert(0, str(REPO)); sys.path.insert(0, str(REPO / "tests"))

from engine.agents import audit_events as AE                       # noqa: E402
from engine.agents.auditor import AuditVerdict                     # noqa: E402
from engine.agents.extractor import record_selection_refusals      # noqa: E402
from engine.core import events, paper_state as PS                  # noqa: E402
from engine.core.database import ReviewDatabase                    # noqa: E402
from engine.core.effective import effective_state                  # noqa: E402
from engine.core.extraction_events import PaperFailure, write_extraction_events  # noqa: E402
from engine.core.parsed_text import resolve_parsed_text            # noqa: E402
from engine.core.review_spec import load_review_spec               # noqa: E402
from engine.core.selection import select_for_extraction            # noqa: E402
from engine.exporters.prisma import generate_prisma_flow           # noqa: E402
from _event_store_fixture import (FIXTURE_CONTEXT_SHA, claim_identity, open_extraction_run,  # noqa: E402
                                  seed_eligibility, seed_processing)
from _parsed_text_fixture import write_parsed                      # noqa: E402

ROOT = Path(sys.argv[1]); ROOT.mkdir(parents=True, exist_ok=True)
SPEC = load_review_spec(REPO / "review_specs" / "surgical_autonomy.yaml")
ARM = SPEC.extraction_models.arm
PID = 7
T1 = "The trial enrolled forty patients at two centres. The robot sutured porcine tissue.\n"
T2 = "The trial enrolled forty-one patients at three centres. The robot sutured porcine tissue.\n"
SNIP = "The trial enrolled forty patients at two centres."   # in T1, not in T2


def fresh(name):
    d = ROOT / name
    if d.exists():
        shutil.rmtree(d)
    db = ReviewDatabase("u15", data_root=d)
    shutil.copy2(REPO / "data/surgical_autonomy/extraction_codebook.yaml",
                 Path(db.db_path).parent / "extraction_codebook.yaml")
    db._conn.execute("INSERT INTO papers (id, title, source, status, created_at, updated_at) "
                     "VALUES (?, 't', 's', 'FT_ELIGIBLE', 'n', 'n')", (PID,))
    seed_eligibility(db._conn, PID); write_parsed(db, PID, T1); db._conn.commit()
    return db, open_extraction_run(db, SPEC)


def claim(db, run_id, field="study_type", value="RCT", snippet=SNIP):
    ref = resolve_parsed_text(db._conn, PID)
    events.write_field_event(
        db._conn, event_type="asserted", paper_id=PID, field_name=field, arm=ARM, value=value,
        source_snippet=snippet, extraction_uid=events.mint_extraction_uid(), actor_kind="model",
        actor_role="extractor", actor_name="deepseek-r1:32b",
        payload=claim_identity(ARM, PID, sha=ref.sha256, uid=ref.parsed_text_uid), run_id=run_id,
        presented_context_sha256=FIXTURE_CONTEXT_SHA)


def extracted_event(db, run_id):
    seed_processing(db._conn, PID, "extracted", run_id=run_id); db._conn.commit()


def snap(db):
    s = effective_state(db._conn, PID)
    from engine.core.effective import live_claim_events
    return {"processing": s.processing, "reason": s.processing_reason,
            "analysis_ready": s.analysis_ready,
            "live_asserted_claims": len(live_claim_events(db._conn, PID, ARM, event_types=("asserted",))),
            "located_events": db._conn.execute(
                "SELECT COUNT(*) FROM field_events WHERE paper_id = ? AND event_type = 'citation_located'",
                (PID,)).fetchone()[0],
            "studies_included": generate_prisma_flow(db)["studies_included"]}


def audit(db, run_id):
    with patch.object(AE, "semantic_verify", return_value=AuditVerdict(
            status="flagged", grep_found=False, reasoning="not supported")):
        rep = AE.audit_run(db._conn, SPEC, run_id=run_id, arm=ARM, review_dir=Path(db.db_path).parent)
    last = db._conn.execute("SELECT from_state, to_state, reason_code FROM paper_events "
                            "WHERE paper_id = ? ORDER BY event_id DESC LIMIT 1", (PID,)).fetchone()
    return {"papers_audited": rep.papers_audited, "skipped_refused": list(rep.skipped_refused),
            "last_paper_event": list(last)}


OUT = {}
REASON = {"parse_failed": "parse_file_unreadable", "extraction_failed": "model_call_failed",
          "input_exceeds_context": "input_overflow_estimated",
          "full_text_not_obtainable": "acquire_cascade_exhausted"}

# row 18, widened: every processing state the axis can hold, above live unlocated claims
for state in ("parse_failed", "extraction_failed", "input_exceeds_context", "full_text_not_obtainable",
              "parsed", "no_recorded_state", "extracted"):
    db, run_id = fresh(f"row18_{state}")
    claim(db, run_id)
    if state != "no_recorded_state":
        seed_processing(db._conn, PID, state, reason_code=REASON.get(state), run_id=run_id)
        db._conn.commit()
    before = snap(db); a = audit(db, run_id); after = snap(db)
    OUT[f"row18_{state}"] = {"before": before, "audit": a, "after": after}
    db.close()

# D24: the text changes after extraction; the re-extraction fails in the loop
db, run_id = fresh("d24_reparse")
claim(db, run_id); extracted_event(db, run_id)
write_parsed(db, PID, T2)                          # v2 recorded (reparse_papers writes no paper event)
sel = select_for_extraction(db._conn, arm=ARM)
before = snap(db)
write_extraction_events(db._conn, PaperFailure(paper_id=PID, arm=ARM, run_id=run_id,
                                               reason_code="model_call_failed", stage_name="extract"))
mid = snap(db); a = audit(db, run_id); after = snap(db)
OUT["d24_reparse"] = {"selection": {"to_extract": [p for p, _ in sel.to_extract],
                                    "skipped_asserted": list(sel.skipped_asserted)},
                      "before": before, "after_failure": mid, "audit": a, "after": after}
db.close()

# D21: the current text's file is missing at selection; the refusal is recorded by the production writer
db, run_id = fresh("d21_refusal")
claim(db, run_id); extracted_event(db, run_id)
p2 = write_parsed(db, PID, T2); p2.unlink()
sel = select_for_extraction(db._conn, arm=ARM)
before = snap(db)
n = record_selection_refusals(db, sel, run_id=run_id)
mid = snap(db); a = audit(db, run_id); after = snap(db)
OUT["d21_refusal"] = {"selection": {"to_extract": [p for p, _ in sel.to_extract],
                                    "skipped_refused": [list(x) for x in sel.skipped_refused]},
                      "refusals_written": n, "before": before, "after_refusal": mid, "audit": a, "after": after}
db.close()

# the same key: no production path writes it (selection skips a held paper); written directly
db, run_id = fresh("same_key")
claim(db, run_id); extracted_event(db, run_id)
sel = select_for_extraction(db._conn, arm=ARM)
before = snap(db)
write_extraction_events(db._conn, PaperFailure(paper_id=PID, arm=ARM, run_id=run_id,
                                               reason_code="model_call_failed", stage_name="extract"))
mid = snap(db); a = audit(db, run_id); after = snap(db)
OUT["same_key"] = {"selection": {"to_extract": [p for p, _ in sel.to_extract],
                                 "skipped_asserted": list(sel.skipped_asserted)},
                   "before": before, "after_direct_failure": mid, "audit": a, "after": after}
db.close()

# claims already located, then a failure; a second audit
db, run_id = fresh("located_then_failure")
claim(db, run_id); extracted_event(db, run_id)
a1 = audit(db, run_id)
seed_processing(db._conn, PID, "parse_failed", reason_code="parse_file_unreadable", run_id=run_id)
db._conn.commit()
before = snap(db); a2 = audit(db, run_id); after = snap(db)
OUT["located_then_failure"] = {"first_audit": a1, "before_second": before, "second_audit": a2, "after": after}
db.close()

print(json.dumps(OUT, indent=1))
