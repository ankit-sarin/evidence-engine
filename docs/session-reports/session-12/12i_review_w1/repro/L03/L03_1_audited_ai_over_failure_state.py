"""L03-C1 reproducer: audit_run writes `audited_ai` whatever the paper's processing
state is. Synthetic scratch ReviewDatabase; semantic_verify stubbed; no model, no network."""
import shutil, sys, tempfile
from pathlib import Path
from unittest.mock import patch
REPO = Path("/home/ankitsarin/projects/evidence-engine")
sys.path.insert(0, str(REPO / "tests"))          # fixture helpers only (imported, not run as tests)
from engine.agents import audit_events as AE
from engine.agents.auditor import AuditVerdict
from engine.core import events
from engine.core.database import ReviewDatabase
from engine.core.effective import effective_state
from engine.core.parsed_text import resolve_parsed_text
from engine.core.review_spec import load_review_spec
from _event_store_fixture import (FIXTURE_CONTEXT_SHA, claim_identity, open_extraction_run,
                                  seed_eligibility, seed_processing)
from _parsed_text_fixture import write_parsed

tmp = Path(tempfile.mkdtemp(dir=Path.cwd()))
spec = load_review_spec(REPO / "review_specs" / "surgical_autonomy.yaml")
db = ReviewDatabase("aud", data_root=tmp)
rd = Path(db.db_path).parent
shutil.copy2(REPO / "data/surgical_autonomy/extraction_codebook.yaml", rd / "extraction_codebook.yaml")
TEXT = "The trial enrolled forty patients at two centres.\n"
arm = spec.extraction_models.arm
for pid in (7,):
    db._conn.execute("INSERT INTO papers (id,title,source,status,created_at,updated_at) "
                     "VALUES (?, 't','s','FT_ELIGIBLE','n','n')", (pid,))
    seed_eligibility(db._conn, pid); write_parsed(db, pid, TEXT)
db._conn.commit()
run_id = open_extraction_run(db, spec)
ref = resolve_parsed_text(db._conn, 7)
events.write_field_event(db._conn, event_type="asserted", paper_id=7, field_name="study_type",
    arm=arm, value="RCT", source_snippet="The trial enrolled forty patients at two centres.",
    extraction_uid=events.mint_extraction_uid(), actor_kind="model", actor_role="extractor",
    actor_name="m", payload=claim_identity(arm, 7, sha=ref.sha256, uid=ref.parsed_text_uid),
    run_id=run_id, presented_context_sha256=FIXTURE_CONTEXT_SHA)
for state, reason in (("parse_failed", None), ("input_exceeds_context", None),
                      ("extraction_failed", None)):
    pass
from engine.core import paper_state as PS
# pick any reason code mapped to parse_failed (a later re-parse that failed)
reason = next(c for c, s in PS.PROCESSING_REASONS.items() if s == "parse_failed")
seed_processing(db._conn, 7, "parse_failed", reason_code=reason, run_id=run_id)
db._conn.commit()
s = effective_state(db._conn, 7)
print("before audit :", s.processing, s.processing_reason, "analysis_ready =", s.analysis_ready)
with patch.object(AE, "semantic_verify", return_value=AuditVerdict(status="flagged", grep_found=False, reasoning="x")):
    rep = AE.audit_run(db._conn, spec, run_id=run_id, arm=arm, review_dir=rd)
s = effective_state(db._conn, 7)
print("report       :", rep)
print("after audit  :", s.processing, s.processing_reason, "analysis_ready =", s.analysis_ready)
print("audited event from_state:", db._conn.execute(
    "SELECT from_state, to_state FROM paper_events WHERE paper_id=7 ORDER BY event_id DESC LIMIT 1").fetchone()[:])
db.close(); shutil.rmtree(tmp)
