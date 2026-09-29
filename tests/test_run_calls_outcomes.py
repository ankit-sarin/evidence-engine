"""R216/C24 — the recorder on every ollama_chat outcome; R215/C26 — RunAborted
closes 'aborted' with a reason (10a-C3).

A stub `_client` throughout: no model call, no request to any server. The
input-fit guard's two functions (`_check_input_fits`, `_check_input_was_read`)
are monkeypatched per test to a pass-through or a chosen refusal — this suite
is about the RECORDER, not the guard's own thresholds (tests/test_ollama_input_fit.py).
"""

from __future__ import annotations

import sqlite3
import uuid
from types import SimpleNamespace

import pytest

from engine.core.database import ReviewDatabase
from engine.core import run_manifest as rm
from engine.utils import ollama_client as oc


@pytest.fixture(autouse=True)
def _no_restart(monkeypatch):
    monkeypatch.setenv(oc.RESTART_OPT_OUT_ENV, "1")


@pytest.fixture(autouse=True)
def _passthrough_input_fit(monkeypatch):
    """Default: the guard passes everything through unchanged. Individual
    tests re-monkeypatch one of these two to raise their chosen refusal."""
    monkeypatch.setattr(
        oc, "_check_input_fits",
        lambda model, messages, options, label: {
            "model": model, "ceiling": 999_999, "chars": 1, "estimate_low": 0})
    monkeypatch.setattr(
        oc, "_check_input_was_read", lambda response, fit, label: response)


@pytest.fixture
def db(tmp_path):
    database = ReviewDatabase("rco", data_root=tmp_path)
    yield database
    database.close()


STAGE = "extract_pass1"


def _linked_run(conn) -> int:
    """A run_manifests row + one run_stage_configs row for STAGE — the FK
    run_calls needs, minted with raw SQL (no spec/codebook required)."""
    conn.execute(
        "INSERT INTO run_manifests (run_uid, review_id, run_kind, git_commit, "
        "git_dirty, spec_hash, codebook_hash, codebook_sha256, "
        "library_versions_json, host, started_at, manifest_json, manifest_sha256) "
        "VALUES (?, 'r', 'extraction', ?, 0, 'h', 'h', 'h', '{}', 'h', 't', '{}', 'h')",
        (str(uuid.uuid4()), "0" * 40),
    )
    run_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO run_stage_configs (run_id, stage, stage_kind, provider, "
        "model_name, model_digest, options_json, options_hash, sent_keys_json, "
        "sources_json, keep_alive, format_schema_hash, prompt_hash) VALUES "
        "(?, ?, ?, 'ollama', 'm', ?, '{}', 'h', '[]', '{}', '-1', 'h', 'h')",
        (run_id, STAGE, STAGE, "a" * 64),
    )
    conn.commit()
    return run_id


def _rows(conn) -> list[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    return conn.execute(
        "SELECT call_id, outcome, outcome_detail, request_hash, response_digest "
        "FROM run_calls ORDER BY call_id"
    ).fetchall()


class StubResponse:
    def __init__(self, content="ok", thinking="", prompt_eval_count=100):
        self.message = SimpleNamespace(content=content, thinking=thinking)
        self.prompt_eval_count = prompt_eval_count
        self.done_reason = "stop"


def _chat(model="m", stage=STAGE, paper_id=None):
    return oc.ollama_chat(
        model=model, messages=[{"role": "user", "content": "x"}],
        paper_id=paper_id, max_retries=0, retry_delay=0, stage=stage)


# ── T1 ──────────────────────────────────────────────────────────────


def test_T1_success_records_one_completed_row(db, monkeypatch):
    monkeypatch.setattr(oc, "_client", SimpleNamespace(chat=lambda **kw: StubResponse()))
    conn = db._conn
    run_id = _linked_run(conn)
    with rm.active_run(conn, run_id):
        response = _chat()
    assert response.message.content == "ok"
    rows = _rows(conn)
    assert len(rows) == 1
    assert rows[0]["outcome"] == "completed"
    assert rows[0]["outcome_detail"] is None
    assert rows[0]["response_digest"] is not None
    assert rows[0]["request_hash"] is not None


# ── T2 ──────────────────────────────────────────────────────────────


def test_T2_input_overflow_records_one_refused_row_and_raises(db, monkeypatch):
    def _raise_overflow(model, messages, options, label):
        raise oc.InputOverflow(model=model, chars=100, estimate_low=100.0, ceiling=10)

    monkeypatch.setattr(oc, "_check_input_fits", _raise_overflow)
    conn = db._conn
    run_id = _linked_run(conn)
    with rm.active_run(conn, run_id):
        with pytest.raises(oc.InputOverflow) as exc:
            _chat()
    rows = _rows(conn)
    assert len(rows) == 1
    assert rows[0]["outcome"] == "refused_input_overflow"
    assert rows[0]["request_hash"] is not None
    assert rows[0]["response_digest"] is None
    assert "InputOverflow" in rows[0]["outcome_detail"]
    assert "cannot fit" in str(exc.value)


# ── T3 ──────────────────────────────────────────────────────────────


def test_T3_ceiling_unavailable_records_one_refused_row_and_raises(db, monkeypatch):
    def _raise_ceiling(model, messages, options, label):
        raise oc.CeilingUnavailable(model=model, reason="show failed: boom")

    monkeypatch.setattr(oc, "_check_input_fits", _raise_ceiling)
    conn = db._conn
    run_id = _linked_run(conn)
    with rm.active_run(conn, run_id):
        with pytest.raises(oc.CeilingUnavailable) as exc:
            _chat()
    rows = _rows(conn)
    assert len(rows) == 1
    assert rows[0]["outcome"] == "refused_ceiling_unavailable"
    assert rows[0]["request_hash"] is not None
    assert rows[0]["response_digest"] is None
    assert "CeilingUnavailable" in rows[0]["outcome_detail"]
    assert "show failed" in str(exc.value)


# ── T4 ──────────────────────────────────────────────────────────────


def test_T4_input_truncated_records_one_refused_row_with_digest_and_raises(db, monkeypatch):
    monkeypatch.setattr(oc, "_client", SimpleNamespace(chat=lambda **kw: StubResponse()))

    def _raise_truncated(response, fit, label):
        raise oc.InputTruncated(model="m", count=1000, ceiling=900, chars=5000)

    monkeypatch.setattr(oc, "_check_input_was_read", _raise_truncated)
    conn = db._conn
    run_id = _linked_run(conn)
    with rm.active_run(conn, run_id):
        with pytest.raises(oc.InputTruncated) as exc:
            _chat()
    rows = _rows(conn)
    assert len(rows) == 1
    assert rows[0]["outcome"] == "refused_input_truncated"
    assert rows[0]["response_digest"] is not None, (
        "a response DID come back — the digest must be recorded")
    assert "InputTruncated" in rows[0]["outcome_detail"]
    assert "truncated" in str(exc.value)


# ── T5 ──────────────────────────────────────────────────────────────


def test_T5_input_dropped_records_one_refused_row_with_digest_and_raises(db, monkeypatch):
    monkeypatch.setattr(oc, "_client", SimpleNamespace(chat=lambda **kw: StubResponse()))

    def _raise_dropped(response, fit, label):
        raise oc.InputDropped(model="m", count=10, chars=5000, floor=500.0, ceiling=900)

    monkeypatch.setattr(oc, "_check_input_was_read", _raise_dropped)
    conn = db._conn
    run_id = _linked_run(conn)
    with rm.active_run(conn, run_id):
        with pytest.raises(oc.InputDropped) as exc:
            _chat()
    rows = _rows(conn)
    assert len(rows) == 1
    assert rows[0]["outcome"] == "refused_input_dropped"
    assert rows[0]["response_digest"] is not None
    assert "InputDropped" in rows[0]["outcome_detail"]
    assert "dropped" in str(exc.value)


# ── T6 ──────────────────────────────────────────────────────────────


def test_T6_client_exception_after_retries_records_one_error_row_and_raises(db, monkeypatch):
    def _boom(**kwargs):
        raise RuntimeError("connection refused")

    monkeypatch.setattr(oc, "_client", SimpleNamespace(chat=_boom))
    conn = db._conn
    run_id = _linked_run(conn)
    with rm.active_run(conn, run_id):
        with pytest.raises(RuntimeError, match="connection refused"):
            _chat()
    rows = _rows(conn)
    assert len(rows) == 1
    assert rows[0]["outcome"] == "error"
    assert rows[0]["response_digest"] is None
    assert "RuntimeError" in rows[0]["outcome_detail"]
    assert "connection refused" in rows[0]["outcome_detail"]


# ── T7 ──────────────────────────────────────────────────────────────


def test_T7_no_active_manifest_refuses_and_records_nothing(db, monkeypatch):
    def _raise_overflow(model, messages, options, label):
        raise oc.InputOverflow(model=model, chars=100, estimate_low=100.0, ceiling=10)

    monkeypatch.setattr(oc, "_check_input_fits", _raise_overflow)
    conn = db._conn
    _linked_run(conn)  # a run exists, but is never activated
    with pytest.raises(oc.InputOverflow):
        _chat()
    assert _rows(conn) == []


# ── T8 ──────────────────────────────────────────────────────────────


def test_T8_success_refusal_success_in_order(db, monkeypatch):
    conn = db._conn
    run_id = _linked_run(conn)

    monkeypatch.setattr(oc, "_client", SimpleNamespace(chat=lambda **kw: StubResponse()))
    with rm.active_run(conn, run_id):
        _chat()

        def _raise_overflow(model, messages, options, label):
            raise oc.InputOverflow(model=model, chars=100, estimate_low=100.0, ceiling=10)
        monkeypatch.setattr(oc, "_check_input_fits", _raise_overflow)
        with pytest.raises(oc.InputOverflow):
            _chat()

        monkeypatch.setattr(
            oc, "_check_input_fits",
            lambda model, messages, options, label: {
                "model": model, "ceiling": 999_999, "chars": 1, "estimate_low": 0})
        _chat()

    rows = _rows(conn)
    assert len(rows) == 3
    assert [r["outcome"] for r in rows] == [
        "completed", "refused_input_overflow", "completed"]


# ── T9 ──────────────────────────────────────────────────────────────


def test_T9_run_aborted_closes_aborted_with_reason(db):
    """The exact wiring scripts/run_pipeline.py's RunAborted handler
    performs — 10a-C3 B4 — exercised directly against a fixture run rather
    than a full pipeline (out of scope: selection/extraction machinery)."""
    from scripts.run_pipeline import _finish_review_run
    from engine.core.extraction_events import RunAborted

    conn = db._conn
    run_id = _linked_run(conn)
    exc = RunAborted(
        f"run {run_id} aborted: 3 consecutive papers produced nothing usable "
        f"(last: paper 42, no_fields_returned). Every paper's event is "
        "written; the run closes as 'failed'.")

    _finish_review_run(db, run_id, "aborted", reason=str(exc))

    row = conn.execute(
        "SELECT end_status, end_reason, ended_at FROM run_manifests WHERE run_id = ?",
        (run_id,),
    ).fetchone()
    assert row[0] == "aborted"
    assert row[1] is not None
    assert "3 consecutive papers" in row[1]
    assert row[2] is not None


# ── T10 ──────────────────────────────────────────────────────────────


def test_T10_finish_review_run_failed_leaves_end_reason_null(db):
    """Reference only — the DDL-level CHECKs (completed refuses a reason,
    aborted requires one) are 022's own T5; this tests _finish_review_run's
    wiring specifically: a 'failed' close with no reason passed stays NULL."""
    from scripts.run_pipeline import _finish_review_run

    conn = db._conn
    run_id = _linked_run(conn)
    _finish_review_run(db, run_id, "failed")
    row = conn.execute(
        "SELECT end_status, end_reason FROM run_manifests WHERE run_id = ?", (run_id,)
    ).fetchone()
    assert row[0] == "failed"
    assert row[1] is None
