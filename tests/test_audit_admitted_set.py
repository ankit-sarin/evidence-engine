"""U15 (INT-i12-12, R677(c), R681): the audit stage's admitted set — T1–T6, T8.

`audit_run` audits a paper only when its processing state is in
`paper_state.AUDIT_ADMITTED_STATES`. A paper outside it is skipped with one
WARNING line naming its id, state and reason, listed on
`AuditReport.skipped_by_state`, and nothing is written for it.

The read-out is docs/session-reports/session-12/12s_U15A/12s_U15A.md (§6, §11);
the constructions are its reproducer's (`repro_u15a.py`). T7 is a test of
option (B)'s write guard and is INT-s12-2's (R681(b)). Every database is a
scratch `ReviewDatabase` with a real manifest; `semantic_verify` is mocked — no
model is called.
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path
from unittest.mock import patch

import pytest

from engine.agents import audit_events as AE
from engine.agents.auditor import AuditVerdict
from engine.agents.extractor import record_selection_refusals
from engine.core import events
from engine.core.database import ReviewDatabase
from engine.core.effective import effective_state, live_claim_events
from engine.core.extraction_events import PaperFailure, write_extraction_events
from engine.core.parsed_text import resolve_parsed_text
from engine.core.review_spec import load_review_spec
from engine.core.selection import select_for_extraction
from engine.exporters.prisma import generate_prisma_flow
from _event_store_fixture import (
    FIXTURE_CONTEXT_SHA, claim_identity, open_extraction_run, seed_eligibility, seed_processing,
)
from _parsed_text_fixture import write_parsed

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"
T1 = "The trial enrolled forty patients at two centres. The robot sutured porcine tissue.\n"
T2 = "The trial enrolled forty-one patients at three centres. The robot sutured porcine tissue.\n"
SNIP = "The trial enrolled forty patients at two centres."   # in T1, not in T2
P7, P8 = 7, 8

#: One reason code per failure token, as `repro_u15a.py` seeds them.
REASON = {"parse_failed": "parse_file_unreadable", "extraction_failed": "model_call_failed",
          "input_exceeds_context": "input_overflow_estimated",
          "full_text_not_obtainable": "acquire_cascade_exhausted"}


@pytest.fixture(scope="module")
def spec():
    return load_review_spec(REPO / "review_specs" / "surgical_autonomy.yaml")


@pytest.fixture
def db(tmp_path):
    rdb = ReviewDatabase("u15", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(rdb.db_path).parent / "extraction_codebook.yaml")
    for pid in (P7, P8):
        rdb._conn.execute("INSERT INTO papers (id, title, source, status, created_at, "
                          "updated_at) VALUES (?, 't', 's', 'FT_ELIGIBLE', 'n', 'n')", (pid,))
        seed_eligibility(rdb._conn, pid)
        write_parsed(rdb, pid, T1)
    rdb._conn.commit()
    yield rdb
    rdb.close()


@pytest.fixture
def run_id(db, spec):
    return open_extraction_run(db, spec)


def claim(db, spec, run_id, pid, field="study_type", value="RCT", snippet=SNIP):
    ref = resolve_parsed_text(db._conn, pid)
    arm = spec.extraction_models.arm
    events.write_field_event(
        db._conn, event_type="asserted", paper_id=pid, field_name=field, arm=arm, value=value,
        source_snippet=snippet, extraction_uid=events.mint_extraction_uid(), actor_kind="model",
        actor_role="extractor", actor_name="deepseek-r1:32b",
        payload=claim_identity(arm, pid, sha=ref.sha256, uid=ref.parsed_text_uid), run_id=run_id,
        presented_context_sha256=FIXTURE_CONTEXT_SHA)


def at_state(db, run_id, pid, state):
    """The paper's latest processing event is `state` (nothing for `no_recorded_state`)."""
    if state != "no_recorded_state":
        seed_processing(db._conn, pid, state, reason_code=REASON.get(state), run_id=run_id)
    db._conn.commit()


def written(db, pid):
    """Every event the store holds for the paper: (paper events, field events by type)."""
    pe = db._conn.execute("SELECT COUNT(*) FROM paper_events WHERE paper_id = ?",
                          (pid,)).fetchone()[0]
    fe = dict(db._conn.execute("SELECT event_type, COUNT(*) FROM field_events WHERE paper_id = ? "
                               "GROUP BY event_type", (pid,)).fetchall())
    return pe, fe


def audit(db, spec, run_id):
    with patch.object(AE, "semantic_verify", return_value=AuditVerdict(
            status="flagged", grep_found=False, reasoning="not supported")):
        return AE.audit_run(db._conn, spec, run_id=run_id, arm=spec.extraction_models.arm,
                            review_dir=Path(db.db_path).parent)


def assert_not_audited(db, pid, rep, state, reason):
    s = effective_state(db._conn, pid)
    assert (s.processing, s.processing_reason, s.analysis_ready) == (state, reason, False)
    assert rep.papers_audited == 0
    assert db._conn.execute(
        "SELECT COUNT(*) FROM field_events WHERE paper_id = ? AND event_type = 'citation_located'",
        (pid,)).fetchone()[0] == 0
    assert db._conn.execute(
        "SELECT COUNT(*) FROM paper_events WHERE paper_id = ? AND event_type = 'audited'",
        (pid,)).fetchone()[0] == 0


# ── T1 (§6, each failure token) ───────────────────────────────────────
@pytest.mark.parametrize("state", ["parse_failed", "extraction_failed",
                                   "input_exceeds_context", "full_text_not_obtainable"])
def test_t1_a_paper_at_a_failure_state_with_unlocated_claims_is_not_audited(db, spec, run_id,
                                                                            state):
    claim(db, spec, run_id, P7)
    at_state(db, run_id, P7, state)
    before = written(db, P7)
    rep = audit(db, spec, run_id)
    assert_not_audited(db, P7, rep, state, REASON[state])
    assert written(db, P7) == before


# ── T2 (§6, parsed and no_recorded_state) ─────────────────────────────
@pytest.mark.parametrize("state", ["parsed", "no_recorded_state"])
def test_t2_a_paper_at_parsed_or_no_recorded_state_is_not_audited(db, spec, run_id, state):
    claim(db, spec, run_id, P7)
    at_state(db, run_id, P7, state)
    before = written(db, P7)
    rep = audit(db, spec, run_id)
    assert_not_audited(db, P7, rep, state, None)
    assert written(db, P7) == before


# ── T3 (§6 control) ───────────────────────────────────────────────────
def test_t3_a_paper_at_extracted_is_audited_as_today(db, spec, run_id):
    claim(db, spec, run_id, P7)
    at_state(db, run_id, P7, "extracted")
    rep = audit(db, spec, run_id)
    s = effective_state(db._conn, P7)
    assert (s.processing, s.processing_reason, s.analysis_ready) == ("audited_ai", None, True)
    assert rep.papers_audited == 1 and rep.located == 1
    assert db._conn.execute(
        "SELECT from_state FROM paper_events WHERE paper_id = ? AND event_type = 'audited'",
        (P7,)).fetchone()[0] == "extracted"


# ── T4 (PRISMA) ───────────────────────────────────────────────────────
def test_t4_prisma_counts_one_included_and_one_parse_failed_after_a_mixed_audit(db, spec, run_id):
    for pid, state in ((P7, "extracted"), (P8, "parse_failed")):
        claim(db, spec, run_id, pid)
        at_state(db, run_id, pid, state)
    rep = audit(db, spec, run_id)
    flow = generate_prisma_flow(db)
    assert rep.papers_audited == 1
    assert flow["studies_included"] == 1 and flow["parse_failed"] == 1


# ── T5 (the log and the report) ───────────────────────────────────────
def test_t5_the_skipped_paper_gets_one_warning_line_naming_its_id_and_state(db, spec, run_id,
                                                                           caplog):
    """§11 item 5 as R681(c) rules it: the line names id, state and reason (a
    failure's reason code, or none), and the report lists the paper."""
    claim(db, spec, run_id, P7)
    at_state(db, run_id, P7, "parse_failed")
    claim(db, spec, run_id, P8)                      # no processing event at all
    caplog.set_level(logging.WARNING, logger=AE.__name__)
    rep = audit(db, spec, run_id)
    assert rep.skipped_by_state == ((P7, "parse_failed", "parse_file_unreadable"),
                                    (P8, "no_recorded_state", None))
    lines = [r.getMessage() for r in caplog.records
             if r.name == AE.__name__ and r.levelno == logging.WARNING]
    p7 = [m for m in lines if m.startswith(f"Paper {P7}:")]
    p8 = [m for m in lines if m.startswith(f"Paper {P8}:")]
    assert len(p7) == 1 and "parse_failed" in p7[0] and "parse_file_unreadable" in p7[0]
    assert len(p8) == 1 and "no_recorded_state" in p8[0] and "reason none" in p8[0]
    assert rep.papers_audited == 0 and rep.skipped_refused == ()


# ── T6 (§4 same_key) ──────────────────────────────────────────────────
def test_t6_a_same_key_failure_over_live_claims_stays_extraction_failed_after_the_audit(
        db, spec, run_id):
    arm = spec.extraction_models.arm
    claim(db, spec, run_id, P7)
    at_state(db, run_id, P7, "extracted")
    write_extraction_events(db._conn, PaperFailure(paper_id=P7, arm=arm, run_id=run_id,
                                                   reason_code="model_call_failed",
                                                   stage_name="extract"))
    assert len(live_claim_events(db._conn, P7, arm, event_types=("asserted",))) == 1
    rep = audit(db, spec, run_id)
    assert_not_audited(db, P7, rep, "extraction_failed", "model_call_failed")


# ── T8 (§4 d24_reparse, d21_refusal: unchanged) ───────────────────────
def test_t8_d24_reparse_the_failure_supersedes_and_the_audit_writes_nothing(db, spec, run_id):
    arm = spec.extraction_models.arm
    claim(db, spec, run_id, P7)
    at_state(db, run_id, P7, "extracted")
    write_parsed(db, P7, T2)                         # v2 recorded; no paper event
    sel = select_for_extraction(db._conn, arm=arm)
    assert P7 in [p for p, _ in sel.to_extract]
    write_extraction_events(db._conn, PaperFailure(paper_id=P7, arm=arm, run_id=run_id,
                                                   reason_code="model_call_failed",
                                                   stage_name="extract"))
    assert live_claim_events(db._conn, P7, arm, event_types=("asserted",)) == ()
    before = written(db, P7)
    rep = audit(db, spec, run_id)
    assert_not_audited(db, P7, rep, "extraction_failed", "model_call_failed")
    assert written(db, P7) == before


def test_t8_d21_refusal_the_refusal_supersedes_and_the_audit_writes_nothing(db, spec, run_id):
    arm = spec.extraction_models.arm
    claim(db, spec, run_id, P7)
    at_state(db, run_id, P7, "extracted")
    write_parsed(db, P7, T2).unlink()                # v2 recorded, its file removed
    sel = select_for_extraction(db._conn, arm=arm)
    assert [p for p, _ in sel.skipped_refused] == [P7]
    assert record_selection_refusals(db, sel, run_id=run_id) == 1
    assert live_claim_events(db._conn, P7, arm, event_types=("asserted",)) == ()
    before = written(db, P7)
    rep = audit(db, spec, run_id)
    assert_not_audited(db, P7, rep, "extraction_failed", "parsed_text_missing")
    assert written(db, P7) == before
