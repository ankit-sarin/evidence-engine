"""Reader 8 on the reader (WRITE-PATH-01 9b-FLIP 1/2, R112; T6).

The post-audit stage completions decide from the processing axis of the event
store. `papers.status` is set to values that would give the opposite answer, so
a decision that still read it would fail these tests.
"""

from __future__ import annotations

import pytest

import scripts.run_pipeline as rp
from engine.adjudication.workflow import is_stage_done
from engine.core import events
from engine.core.database import ReviewDatabase
from _event_store_fixture import fixture_run, seed_eligibility


@pytest.fixture
def db(tmp_path):
    rdb = ReviewDatabase("stages", data_root=tmp_path)
    for pid in (1, 2):
        rdb._conn.execute("INSERT INTO papers (id, title, source, status, created_at, "
                          "updated_at) VALUES (?, 't', 's', 'INGESTED', 'n', 'n')", (pid,))
        seed_eligibility(rdb._conn, pid)
    rdb._conn.commit()
    yield rdb
    rdb.close()


def _processing(db, pid, to_state):
    events.write_paper_event(
        db._conn, event_type="extracted" if to_state == "extracted" else "audited",
        paper_id=pid, to_state=to_state, actor_kind="engine", actor_role="system",
        actor_name="extractor", run_id=fixture_run(db._conn))


def test_t6_extraction_complete_follows_the_processing_axis_not_papers_status(db):
    db._conn.execute("UPDATE papers SET status = 'INGESTED'")      # garbage for this question
    _processing(db, 1, "extracted")
    counts = rp._advance_extraction_workflow(db._conn)
    assert counts == {"extracted": 1, "audited": 0}
    assert is_stage_done(db._conn, "EXTRACTION_COMPLETE")
    assert not is_stage_done(db._conn, "AI_AUDIT_COMPLETE_STAGE")


def test_t6_audit_complete_follows_audited_ai(db):
    _processing(db, 1, "extracted")
    _processing(db, 1, "audited_ai")
    assert rp._advance_extraction_workflow(db._conn) == {"extracted": 1, "audited": 1}
    assert is_stage_done(db._conn, "AI_AUDIT_COMPLETE_STAGE")


def test_t6_a_legacy_status_alone_completes_nothing(db):
    db._conn.execute("UPDATE papers SET status = 'AI_AUDIT_COMPLETE'")
    db._conn.commit()
    assert rp._advance_extraction_workflow(db._conn) == {"extracted": 0, "audited": 0}
    assert not is_stage_done(db._conn, "EXTRACTION_COMPLETE")
