"""C40: every `run_pipeline` close goes through `_finish_review_run`, and a gate
stop closes 'interrupted' with its own gate's reason, never with none.

Each test drives `run_pipeline` itself on a scratch `ReviewDatabase` with a real
manifest (`open_extraction_run`: injected digest and git, no HTTP, no
`git status`). The stages are stubbed; no model is called and nothing reaches
`data/`. The 'aborted' close is pinned by
`test_cut_over.py::test_t3_an_aborted_run_closes_as_aborted_with_reason`.
"""

from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path

import pytest

import scripts.run_pipeline as rp
from engine.core import run_manifest as rm
from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec
from _event_store_fixture import open_extraction_run

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"


@pytest.fixture(scope="module")
def spec():
    return load_review_spec(REPO / "review_specs" / "surgical_autonomy.yaml")


@pytest.fixture
def db(tmp_path):
    rdb = ReviewDatabase("closes", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(rdb.db_path).parent / "extraction_codebook.yaml")
    yield rdb
    rdb.close()


@pytest.fixture
def pipeline(db, spec, monkeypatch):
    """`run_pipeline` wired to the scratch database and a real manifest, every
    stage stubbed. Returns the run_id; tests then set the gates."""
    run_id = open_extraction_run(db, spec)
    monkeypatch.setattr(rp, "load_spec_for", lambda name, path=None: spec)
    monkeypatch.setattr(rp, "ReviewDatabase", lambda name: db)
    monkeypatch.setattr(rp, "_open_run_manifest", lambda d, s, i, **k: run_id)
    for stage in ("_stage_parse", "_stage_extract", "_stage_audit", "_stage_export"):
        monkeypatch.setattr(rp, stage, lambda *a, **k: {})
    monkeypatch.setattr(rp, "_advance_extraction_workflow", lambda conn: {})
    monkeypatch.setattr(rp, "is_adjudication_complete", lambda conn: True)
    monkeypatch.setattr(rp, "is_audit_review_complete", lambda conn: True)
    return run_id


def _blocker(stage_name):
    return lambda conn: {"stage_name": stage_name, "next_step": "do the review"}


def _end(db, run_id):
    conn = sqlite3.connect(db.db_path)
    try:
        return conn.execute("SELECT end_status, end_reason, ended_at FROM run_manifests "
                            "WHERE run_id = ?", (run_id,)).fetchone()
    finally:
        conn.close()


def test_the_adjudication_gate_closes_interrupted_with_its_reason(db, pipeline, monkeypatch):
    monkeypatch.setattr(rp, "is_adjudication_complete", lambda conn: False)
    monkeypatch.setattr(rp, "get_current_blocker", _blocker("FULL_TEXT_ADJUDICATION_COMPLETE"))
    rp.run_pipeline("closes", skip_to="extract")
    status, reason, ended_at = _end(db, pipeline)
    assert (status, reason) == ("interrupted", rm.REASON_BLOCKED_ADJUDICATION)
    assert ended_at is not None


def test_the_audit_review_gate_closes_interrupted_with_its_reason(db, pipeline, monkeypatch):
    """Run 7's launch path (R-SCOPE-9): extract, audit, then this gate."""
    monkeypatch.setattr(rp, "is_audit_review_complete", lambda conn: False)
    monkeypatch.setattr(rp, "get_current_blocker", _blocker("AUDIT_QUEUE_EXPORTED"))
    rp.run_pipeline("closes", skip_to="extract")
    status, reason, ended_at = _end(db, pipeline)
    assert (status, reason) == ("interrupted", rm.REASON_BLOCKED_AUDIT_REVIEW)
    assert ended_at is not None


def test_the_gate_reasons_are_distinct_blocked_tokens_in_the_closed_set():
    reasons = (rm.REASON_BLOCKED_ADJUDICATION, rm.REASON_BLOCKED_AUDIT_REVIEW)
    assert len(set(reasons)) == 2
    assert all(r.startswith("blocked:") for r in reasons)
    assert set(reasons) <= set(rm.INTERRUPTED_REASONS)


def test_a_completed_run_closes_completed_with_no_reason(db, pipeline):
    rp.run_pipeline("closes", skip_to="extract")
    status, reason, ended_at = _end(db, pipeline)
    assert (status, reason) == ("completed", None)
    assert ended_at is not None


def test_a_failed_run_closes_failed_with_no_reason(db, pipeline, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("stage failed")
    monkeypatch.setattr(rp, "_stage_extract", boom)
    with pytest.raises(RuntimeError, match="stage failed"):
        rp.run_pipeline("closes", skip_to="extract")
    status, reason, ended_at = _end(db, pipeline)
    assert (status, reason) == ("failed", None)
    assert ended_at is not None
