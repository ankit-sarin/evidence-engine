"""Tests for the SQLite review database."""

import json
import sqlite3

import pytest

from engine.core.database import ReviewDatabase, STATUSES, ALLOWED_TRANSITIONS
from engine.search.models import Citation


@pytest.fixture()
def db(tmp_path):
    """Create a fresh ReviewDatabase in a temp directory."""
    rdb = ReviewDatabase("test_review", data_root=tmp_path)
    yield rdb
    rdb.close()


def _cit(**kw):
    defaults = dict(title="Study A", source="pubmed", pmid="111", doi="10.1/a")
    defaults.update(kw)
    return Citation(**defaults)


# ── Table Creation ───────────────────────────────────────────────────


def test_tables_exist(db):
    tables = {
        r[0]
        for r in db._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    expected = {
        "papers",
        "abstract_screening_decisions",
        "full_text_assets",
        "extractions",
        "evidence_spans",
        "review_runs",
    }
    assert expected.issubset(tables)


def test_directories_created(db, tmp_path):
    base = tmp_path / "test_review"
    assert (base / "pdfs").is_dir()
    assert (base / "parsed_text").is_dir()
    assert (base / "vector_store").is_dir()


def test_wal_mode(db):
    mode = db._conn.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode == "wal"


# ── Add Papers & Dedup ───────────────────────────────────────────────


def test_add_papers(db):
    cits = [_cit(pmid=str(i), title=f"Study {i}") for i in range(5)]
    added = db.add_papers(cits)
    assert added == 5


def test_no_duplicates_on_readd(db):
    cits = [_cit(pmid="99", title="Dup Study")]
    db.add_papers(cits)
    added_again = db.add_papers(cits)
    assert added_again == 0

    rows = db.get_papers_by_status("INGESTED")
    pmid_99 = [r for r in rows if r["pmid"] == "99"]
    assert len(pmid_99) == 1


# ── Full Lifecycle Walk ──────────────────────────────────────────────


def test_full_lifecycle(db):
    db.add_papers([_cit(pmid="LC1", title="Lifecycle Paper")])
    paper = db.get_papers_by_status("INGESTED")[0]
    pid = paper["id"]

    # INGESTED → ABSTRACT_SCREENED_IN
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    assert db.get_papers_by_status("ABSTRACT_SCREENED_IN")[0]["id"] == pid

    # ABSTRACT_SCREENED_IN → PDF_ACQUIRED
    db.update_status(pid, "PDF_ACQUIRED")

    # PDF_ACQUIRED → PARSED
    db.update_status(pid, "PARSED")

    # PARSED → EXTRACTED
    db.update_status(pid, "EXTRACTED")

    # EXTRACTED → AI_AUDIT_COMPLETE
    db.update_status(pid, "AI_AUDIT_COMPLETE")
    assert db.get_papers_by_status("AI_AUDIT_COMPLETE")[0]["id"] == pid


def test_ai_to_human_audit_transition(db):
    db.add_papers([_cit(pmid="AH1", title="AI to Human")])
    pid = db.get_papers_by_status("INGESTED")[0]["id"]

    for status in ("ABSTRACT_SCREENED_IN", "PDF_ACQUIRED", "PARSED", "EXTRACTED", "AI_AUDIT_COMPLETE"):
        db.update_status(pid, status)

    db.update_status(pid, "HUMAN_AUDIT_COMPLETE")
    assert db.get_papers_by_status("HUMAN_AUDIT_COMPLETE")[0]["id"] == pid


def test_screened_out_lifecycle(db):
    db.add_papers([_cit(pmid="SO1", title="Screened Out")])
    paper = db.get_papers_by_status("INGESTED")[0]
    pid = paper["id"]

    db.update_status(pid, "ABSTRACT_SCREENED_OUT")
    assert db.get_papers_by_status("ABSTRACT_SCREENED_OUT")[0]["id"] == pid


def test_flagged_then_resolved(db):
    db.add_papers([_cit(pmid="FL1", title="Flagged")])
    paper = db.get_papers_by_status("INGESTED")[0]
    pid = paper["id"]

    db.update_status(pid, "ABSTRACT_SCREEN_FLAGGED")
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    assert db.get_papers_by_status("ABSTRACT_SCREENED_IN")[0]["id"] == pid


# ── Invalid Transitions ─────────────────────────────────────────────


def test_invalid_transition_raises(db):
    db.add_papers([_cit(pmid="IT1", title="Bad Transition")])
    paper = db.get_papers_by_status("INGESTED")[0]
    pid = paper["id"]

    with pytest.raises(ValueError, match="Invalid transition"):
        db.update_status(pid, "EXTRACTED")


def test_invalid_status_raises(db):
    db.add_papers([_cit(pmid="IS1", title="Invalid Status")])
    paper = db.get_papers_by_status("INGESTED")[0]
    pid = paper["id"]

    with pytest.raises(ValueError, match="Invalid status"):
        db.update_status(pid, "NONEXISTENT")


def test_screened_out_is_terminal(db):
    db.add_papers([_cit(pmid="T1", title="Terminal")])
    paper = db.get_papers_by_status("INGESTED")[0]
    pid = paper["id"]

    db.update_status(pid, "ABSTRACT_SCREENED_OUT")
    with pytest.raises(ValueError, match="Invalid transition"):
        db.update_status(pid, "ABSTRACT_SCREENED_IN")


# ── Screening Decisions ─────────────────────────────────────────────


def test_abstract_screening_decisions(db):
    db.add_papers([_cit(pmid="SD1", title="Screen Me")])
    paper = db.get_papers_by_status("INGESTED")[0]
    pid = paper["id"]

    d1 = db.add_screening_decision(pid, 1, "include", "Relevant study", "qwen3:8b")
    d2 = db.add_screening_decision(pid, 2, "include", "Confirmed relevant", "qwen3:8b")
    assert d1 > 0
    assert d2 > d1

    rows = db._conn.execute(
        "SELECT * FROM abstract_screening_decisions WHERE paper_id = ?", (pid,)
    ).fetchall()
    assert len(rows) == 2
    assert rows[0]["pass_number"] == 1
    assert rows[1]["pass_number"] == 2


# ── Staleness Detection ─────────────────────────────────────────────


def test_staleness_detection(db):
    db.add_papers([_cit(pmid="ST1", title="Stale")])
    paper = db.get_papers_by_status("INGESTED")[0]
    pid = paper["id"]

    # Walk to EXTRACTED
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")
    db.update_status(pid, "PARSED")
    db.update_status(pid, "EXTRACTED")

    old_hash = "abc123"
    db.add_extraction(pid, old_hash, {"study_design": "RCT"}, "thinking...", "deepseek-r1:32b")

    # Same hash → not stale
    assert len(db.get_stale_extractions(old_hash)) == 0

    # Different hash → stale
    stale = db.get_stale_extractions("new_hash_456")
    assert len(stale) == 1
    assert stale[0]["id"] == pid


# ── Evidence Spans & Audit ───────────────────────────────────────────


def test_evidence_spans_and_audit(db):
    db.add_papers([_cit(pmid="ES1", title="Spans")])
    paper = db.get_papers_by_status("INGESTED")[0]
    pid = paper["id"]

    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")
    db.update_status(pid, "PARSED")
    db.update_status(pid, "EXTRACTED")

    ext_id = db.add_extraction(pid, "hash1", {"design": "RCT"}, "trace", "deepseek-r1:32b")
    span_id = db.add_evidence_span(ext_id, "study_design", "RCT", "This was an RCT...", 0.95)

    # Verify pending
    span = db._conn.execute(
        "SELECT * FROM evidence_spans WHERE id = ?", (span_id,)
    ).fetchone()
    assert span["audit_status"] == "pending"

    # Audit it
    db.update_audit(span_id, "verified", "qwen3:32b", "Confirmed RCT design")
    span = db._conn.execute(
        "SELECT * FROM evidence_spans WHERE id = ?", (span_id,)
    ).fetchone()
    assert span["audit_status"] == "verified"
    assert span["auditor_model"] == "qwen3:32b"


def test_evidence_spans_contested_status(db):
    """New 'contested' audit status is accepted by the schema."""
    db.add_papers([_cit(pmid="CS1", title="Contested")])
    pid = db.get_papers_by_status("INGESTED")[0]["id"]
    for s in ("ABSTRACT_SCREENED_IN", "PDF_ACQUIRED", "PARSED", "EXTRACTED"):
        db.update_status(pid, s)

    ext_id = db.add_extraction(pid, "h1", {}, "t", "m")
    span_id = db.add_evidence_span(ext_id, "f", "v", "s", 0.9)
    db.update_audit(span_id, "contested", "qwen3:32b", "Grep fail, semantic pass")

    span = db._conn.execute("SELECT audit_status FROM evidence_spans WHERE id = ?", (span_id,)).fetchone()
    assert span["audit_status"] == "contested"


def test_evidence_spans_invalid_snippet_status(db):
    """New 'invalid_snippet' audit status is accepted by the schema."""
    db.add_papers([_cit(pmid="IS2", title="Invalid Snippet")])
    pid = db.get_papers_by_status("INGESTED")[0]["id"]
    for s in ("ABSTRACT_SCREENED_IN", "PDF_ACQUIRED", "PARSED", "EXTRACTED"):
        db.update_status(pid, s)

    ext_id = db.add_extraction(pid, "h1", {}, "t", "m")
    span_id = db.add_evidence_span(ext_id, "f", "v", "s", 0.9)
    db.update_audit(span_id, "invalid_snippet", "qwen3:32b", "Ellipsis bridging")

    span = db._conn.execute("SELECT audit_status FROM evidence_spans WHERE id = ?", (span_id,)).fetchone()
    assert span["audit_status"] == "invalid_snippet"


# ── Atomic Extraction ─────────────────────────────────────────────────


def test_atomic_extraction_commits_all(db):
    """All spans and the extraction record land in one transaction."""
    db.add_papers([_cit(pmid="AT1", title="Atomic OK")])
    pid = db.get_papers_by_status("INGESTED")[0]["id"]
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")
    db.update_status(pid, "PARSED")
    db.update_status(pid, "EXTRACTED")

    spans = [
        {"field_name": f"field_{i}", "value": f"val_{i}",
         "source_snippet": f"snippet {i}", "confidence": 0.9}
        for i in range(15)
    ]
    ext_id = db.add_extraction_atomic(
        pid, "hash_ok", {"f": 1}, "trace", "model", spans,
    )

    # Extraction exists
    row = db._conn.execute(
        "SELECT * FROM extractions WHERE id = ?", (ext_id,)
    ).fetchone()
    assert row is not None
    assert row["paper_id"] == pid

    # All 15 spans exist
    span_rows = db._conn.execute(
        "SELECT * FROM evidence_spans WHERE extraction_id = ?", (ext_id,)
    ).fetchall()
    assert len(span_rows) == 15


def test_atomic_extraction_rolls_back_on_failure(db):
    """If a span insert fails, no extraction or spans are committed."""
    db.add_papers([_cit(pmid="AT2", title="Atomic Fail")])
    pid = db.get_papers_by_status("INGESTED")[0]["id"]
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")
    db.update_status(pid, "PARSED")
    db.update_status(pid, "EXTRACTED")

    # Span #2 has a bad confidence (string instead of float) → will fail SQL
    spans = [
        {"field_name": "f1", "value": "v1", "source_snippet": "s1", "confidence": 0.9},
        {"field_name": "f2", "value": "v2", "source_snippet": "s2", "confidence": 0.8},
        {"field_name": "f3", "value": None, "source_snippet": "s3", "confidence": 0.7},  # NULL value → NOT NULL constraint
    ]

    with pytest.raises(Exception):
        db.add_extraction_atomic(
            pid, "hash_fail", {"f": 1}, "trace", "model", spans,
        )

    # Nothing committed
    ext_count = db._conn.execute("SELECT COUNT(*) FROM extractions").fetchone()[0]
    span_count = db._conn.execute("SELECT COUNT(*) FROM evidence_spans").fetchone()[0]
    assert ext_count == 0
    assert span_count == 0


# ── Reset for Re-Audit ──────────────────────────────────────────────


def _walk_to_ai_audit(db, pmid):
    """Helper: add a paper and walk it to AI_AUDIT_COMPLETE with spans."""
    db.add_papers([_cit(pmid=pmid, title=f"Paper {pmid}")])
    pid = db.get_papers_by_status("INGESTED")[-1]["id"]
    for s in ("ABSTRACT_SCREENED_IN", "PDF_ACQUIRED", "PARSED", "EXTRACTED"):
        db.update_status(pid, s)
    ext_id = db.add_extraction(pid, "h", {}, "t", "m")
    s1 = db.add_evidence_span(ext_id, "f1", "v1", "snip1", 0.9)
    s2 = db.add_evidence_span(ext_id, "f2", "v2", "snip2", 0.8)
    db.update_audit(s1, "verified", "qwen3:32b", "ok")
    db.update_audit(s2, "flagged", "qwen3:32b", "bad")
    db.update_status(pid, "AI_AUDIT_COMPLETE")
    return pid


def test_reset_for_reaudit_atomicity(db):
    """reset_for_reaudit resets both papers and spans in one transaction."""
    pid = _walk_to_ai_audit(db, "RA1")

    result = db.reset_for_reaudit()
    assert result["papers_reset"] == 1
    assert result["spans_reset"] == 2

    # Paper back to EXTRACTED
    assert len(db.get_papers_by_status("EXTRACTED")) == 1
    assert len(db.get_papers_by_status("AI_AUDIT_COMPLETE")) == 0

    # All spans back to pending
    pending = db._conn.execute(
        "SELECT COUNT(*) FROM evidence_spans WHERE audit_status = 'pending'"
    ).fetchone()[0]
    assert pending == 2

    # Audit columns cleared
    span = db._conn.execute("SELECT * FROM evidence_spans LIMIT 1").fetchone()
    assert span["auditor_model"] is None
    assert span["audit_rationale"] is None
    assert span["audited_at"] is None


def test_reset_for_reaudit_preserves_extraction_data(db):
    """Extracted values and snippets are untouched by reset."""
    pid = _walk_to_ai_audit(db, "RA2")
    db.reset_for_reaudit()

    spans = db._conn.execute("SELECT * FROM evidence_spans ORDER BY id").fetchall()
    assert spans[0]["value"] == "v1"
    assert spans[0]["source_snippet"] == "snip1"
    assert spans[1]["value"] == "v2"


# ── Reject Paper ────────────────────────────────────────────────────


def test_reject_paper(db):
    pid = _walk_to_ai_audit(db, "RJ1")
    db.reject_paper(pid, "Extended abstract only")

    paper = db._conn.execute("SELECT * FROM papers WHERE id = ?", (pid,)).fetchone()
    assert paper["status"] == "REJECTED"
    assert paper["rejected_reason"] == "Extended abstract only"


def test_reject_paper_invalid_status(db):
    db.add_papers([_cit(pmid="RJ2", title="Cannot Reject")])
    pid = db.get_papers_by_status("INGESTED")[0]["id"]

    with pytest.raises(ValueError, match="not allowed"):
        db.reject_paper(pid, "some reason")


# ── Min Status Gate ─────────────────────────────────────────────────


def test_min_status_gate(db):
    pid = _walk_to_ai_audit(db, "MG1")

    assert db.min_status_gate(pid, "EXTRACTED") is True
    assert db.min_status_gate(pid, "AI_AUDIT_COMPLETE") is True
    assert db.min_status_gate(pid, "HUMAN_AUDIT_COMPLETE") is False


def test_min_status_gate_missing_paper(db):
    assert db.min_status_gate(9999, "EXTRACTED") is False


def test_min_status_gate_missing_paper_logs_warning(db, caplog):
    """M10: min_status_gate logs WARNING for nonexistent paper_id."""
    import logging
    with caplog.at_level(logging.WARNING, logger="engine.core.database"):
        result = db.min_status_gate(9999, "EXTRACTED")

    assert result is False
    assert "paper_id 9999 not found" in caplog.text


# ── Pipeline Stats ───────────────────────────────────────────────────


def test_pipeline_stats(db):
    cits = [_cit(pmid=str(i), title=f"Stat {i}") for i in range(10)]
    db.add_papers(cits)

    # Screen 3 in, 2 out
    papers = db.get_papers_by_status("INGESTED")
    for p in papers[:3]:
        db.update_status(p["id"], "ABSTRACT_SCREENED_IN")
    for p in papers[3:5]:
        db.update_status(p["id"], "ABSTRACT_SCREENED_OUT")

    stats = db.get_pipeline_stats()
    assert stats["total_papers"] == 10
    assert stats["ABSTRACT_SCREENED_IN"] == 3
    assert stats["ABSTRACT_SCREENED_OUT"] == 2
    assert stats["INGESTED"] == 5
    assert stats["total_extractions"] == 0
    assert stats["total_evidence_spans"] == 0


# ── Reset for Re-Extraction ──────────────────────────────────────────


def test_reset_for_reextraction(db):
    """reset_for_reextraction deletes extractions/spans and moves papers to PARSED."""
    # Create two audited papers and one screened-out paper with its own extraction
    pid1 = _walk_to_ai_audit(db, "RE1")
    pid2 = _walk_to_ai_audit(db, "RE2")

    # Screened-out paper should never be touched
    db.add_papers([_cit(pmid="RE_SO", title="Screened Out")])
    so_paper = [p for p in db.get_papers_by_status("INGESTED") if p["pmid"] == "RE_SO"][0]
    db.update_status(so_paper["id"], "ABSTRACT_SCREENED_OUT")

    # Record pre-reset counts
    pre_extractions = db._conn.execute("SELECT COUNT(*) FROM extractions").fetchone()[0]
    pre_spans = db._conn.execute("SELECT COUNT(*) FROM evidence_spans").fetchone()[0]
    assert pre_extractions == 2  # one per audited paper
    assert pre_spans == 4  # two spans per paper

    result = db.reset_for_reextraction()
    assert result["papers_reset"] == 2
    assert result["spans_deleted"] == 4
    assert result["extractions_deleted"] == 2

    # Both papers are now PARSED
    parsed = db.get_papers_by_status("PARSED")
    parsed_ids = {p["id"] for p in parsed}
    assert pid1 in parsed_ids
    assert pid2 in parsed_ids

    # No papers left at EXTRACTED or AI_AUDIT_COMPLETE
    assert len(db.get_papers_by_status("EXTRACTED")) == 0
    assert len(db.get_papers_by_status("AI_AUDIT_COMPLETE")) == 0

    # Extraction records and spans are gone
    assert db._conn.execute("SELECT COUNT(*) FROM extractions").fetchone()[0] == 0
    assert db._conn.execute("SELECT COUNT(*) FROM evidence_spans").fetchone()[0] == 0

    # ABSTRACT_SCREENED_OUT paper is untouched
    so = db.get_papers_by_status("ABSTRACT_SCREENED_OUT")
    assert len(so) == 1
    assert so[0]["pmid"] == "RE_SO"


# ── Cleanup Orphaned Spans ────────────────────────────────────────────


def test_cleanup_orphaned_spans(db):
    """cleanup_orphaned_spans deletes spans from older extractions only."""
    db.add_papers([_cit(pmid="CO1", title="Cleanup")])
    pid = db.get_papers_by_status("INGESTED")[0]["id"]
    for s in ("ABSTRACT_SCREENED_IN", "PDF_ACQUIRED", "PARSED", "EXTRACTED"):
        db.update_status(pid, s)

    # First extraction (will become orphaned)
    old_ext_id = db.add_extraction(pid, "h_old", {}, "t1", "m")
    db.add_evidence_span(old_ext_id, "f1", "old_v1", "old_snip1", 0.9)
    db.add_evidence_span(old_ext_id, "f2", "old_v2", "old_snip2", 0.8)

    # Second extraction (current — should survive)
    new_ext_id = db.add_extraction(pid, "h_new", {}, "t2", "m")
    db.add_evidence_span(new_ext_id, "f1", "new_v1", "new_snip1", 0.95)
    db.add_evidence_span(new_ext_id, "f2", "new_v2", "new_snip2", 0.85)

    # Before cleanup: 4 spans total
    total = db._conn.execute("SELECT COUNT(*) FROM evidence_spans").fetchone()[0]
    assert total == 4

    deleted = db.cleanup_orphaned_spans()
    assert deleted == 2  # old extraction's spans

    # After cleanup: only 2 spans from the new extraction
    remaining = db._conn.execute("SELECT * FROM evidence_spans").fetchall()
    assert len(remaining) == 2
    for s in remaining:
        assert s["extraction_id"] == new_ext_id
        assert s["value"].startswith("new_")


# ── Context Manager ──────────────────────────────────────────────────


def test_context_manager_opens_and_closes(tmp_path):
    """ReviewDatabase works as a context manager; connection open inside, closed after."""
    with ReviewDatabase("ctx_test", data_root=tmp_path) as db:
        # Connection should be live inside the block
        row = db._conn.execute("SELECT 1").fetchone()
        assert row[0] == 1
        # Basic operation works
        db.add_papers([_cit(pmid="CM1", title="Context Mgr")])
        assert len(db.get_papers_by_status("INGESTED")) == 1

    # After exiting, _conn should be None (closed)
    assert db._conn is None


def test_context_manager_closes_on_exception(tmp_path):
    """Connection is closed even when an exception is raised inside the with block."""
    with pytest.raises(RuntimeError, match="boom"):
        with ReviewDatabase("ctx_exc", data_root=tmp_path) as db:
            db.add_papers([_cit(pmid="CE1", title="Exception")])
            raise RuntimeError("boom")

    assert db._conn is None


def test_close_is_idempotent(tmp_path):
    """Calling .close() twice does not raise."""
    db = ReviewDatabase("ctx_idem", data_root=tmp_path)
    db.add_papers([_cit(pmid="CI1", title="Idempotent")])
    db.close()
    db.close()  # Second call should be a no-op
    assert db._conn is None


def test_manual_usage_still_works(tmp_path):
    """Existing non-context-manager usage (db = ...; db.close()) works identically."""
    db = ReviewDatabase("ctx_manual", data_root=tmp_path)
    db.add_papers([_cit(pmid="MU1", title="Manual")])
    papers = db.get_papers_by_status("INGESTED")
    assert len(papers) == 1
    assert papers[0]["title"] == "Manual"
    db.close()
    assert db._conn is None


# ── Migration Error Filtering ───────────────────────────────────────


def test_migration_existing_column_succeeds_silently(tmp_path):
    """Adding an already-existing column is silently ignored (idempotent)."""
    import sqlite3

    # First creation adds all columns via migrations
    db1 = ReviewDatabase("mig_test", data_root=tmp_path)
    db1.close()

    # Second creation re-runs migrations — should not raise
    db2 = ReviewDatabase("mig_test", data_root=tmp_path)
    # Verify the DB is functional
    db2.add_papers([_cit(pmid="M1", title="Migration OK")])
    assert len(db2.get_papers_by_status("INGESTED")) == 1
    db2.close()


def test_migration_syntax_error_raises(tmp_path):
    """A migration with a syntax error raises OperationalError instead of being swallowed."""
    import sqlite3
    from engine.core import database as db_mod

    # Create a valid DB first
    db = ReviewDatabase("mig_err", data_root=tmp_path)
    db.close()

    # Patch _SIMPLE_MIGRATIONS to include a bad SQL statement
    bad_migrations = ["CREAT TABLE bad_syntax (id INTEGER PRIMARY KEY)"]
    original = db_mod._SIMPLE_MIGRATIONS

    try:
        db_mod._SIMPLE_MIGRATIONS = bad_migrations
        with pytest.raises(sqlite3.OperationalError):
            ReviewDatabase("mig_err", data_root=tmp_path)
    finally:
        db_mod._SIMPLE_MIGRATIONS = original


# ── Admin Reset ──────────────────────────────────────────────────────


def test_admin_reset_status_succeeds_and_logs(tmp_path):
    """admin_reset_status bypasses state machine and records audit trail."""
    db = ReviewDatabase("admin_test", data_root=tmp_path)
    db.add_papers([_cit(pmid="AR1", title="Admin Reset")])
    pid = db.get_papers_by_status("INGESTED")[0]["id"]
    for s in ("ABSTRACT_SCREENED_IN", "PDF_ACQUIRED", "PARSED",
              "EXTRACTED", "AI_AUDIT_COMPLETE"):
        db.update_status(pid, s)

    # AI_AUDIT_COMPLETE → PARSED is NOT in ALLOWED_TRANSITIONS
    prev = db.admin_reset_status(pid, "PARSED", reason="schema cleanup")
    assert prev == "AI_AUDIT_COMPLETE"

    # Paper is now PARSED
    assert db.get_papers_by_status("PARSED")[0]["id"] == pid

    # Audit trail recorded
    row = db._conn.execute(
        "SELECT * FROM admin_resets WHERE paper_id = ?", (pid,)
    ).fetchone()
    assert row is not None
    assert row["from_status"] == "AI_AUDIT_COMPLETE"
    assert row["to_status"] == "PARSED"
    assert row["reason"] == "schema cleanup"
    db.close()


def test_admin_reset_invalid_target_raises(tmp_path):
    """admin_reset_status rejects an invalid target status."""
    db = ReviewDatabase("admin_bad", data_root=tmp_path)
    db.add_papers([_cit(pmid="AB1", title="Bad Target")])
    pid = db.get_papers_by_status("INGESTED")[0]["id"]

    with pytest.raises(ValueError, match="Invalid target status"):
        db.admin_reset_status(pid, "NONEXISTENT", reason="test")
    db.close()


def test_normal_pipeline_cannot_use_admin_transition(tmp_path):
    """update_status still rejects AI_AUDIT_COMPLETE → PARSED."""
    db = ReviewDatabase("admin_guard", data_root=tmp_path)
    db.add_papers([_cit(pmid="AG1", title="Guard")])
    pid = db.get_papers_by_status("INGESTED")[0]["id"]
    for s in ("ABSTRACT_SCREENED_IN", "PDF_ACQUIRED", "PARSED",
              "EXTRACTED", "AI_AUDIT_COMPLETE"):
        db.update_status(pid, s)

    with pytest.raises(ValueError, match="Invalid transition"):
        db.update_status(pid, "PARSED")
    db.close()


# ── L3: NOT NULL constraints ──────────────────────────────────────────


def test_null_confidence_raises_integrity_error(db):
    """L3: Inserting a span with NULL confidence raises IntegrityError."""
    db.add_papers([_cit(pmid="L3_1", title="Null Conf")])
    pid = db.get_papers_by_status("INGESTED")[0]["id"]
    for s in ("ABSTRACT_SCREENED_IN", "PDF_ACQUIRED", "PARSED", "EXTRACTED"):
        db.update_status(pid, s)

    ext_id = db.add_extraction(pid, "h", {}, "t", "m")

    with pytest.raises(sqlite3.IntegrityError):
        db._conn.execute(
            """INSERT INTO evidence_spans
               (extraction_id, field_name, value, source_snippet, confidence)
               VALUES (?, ?, ?, ?, ?)""",
            (ext_id, "f", "v", "s", None),
        )


def test_null_tier_raises_integrity_error(db):
    """L3: Inserting a span with NULL tier raises IntegrityError."""
    db.add_papers([_cit(pmid="L3_tier", title="Null Tier")])
    pid = db.get_papers_by_status("INGESTED")[0]["id"]
    for s in ("ABSTRACT_SCREENED_IN", "PDF_ACQUIRED", "PARSED", "EXTRACTED"):
        db.update_status(pid, s)

    ext_id = db.add_extraction(pid, "h", {}, "t", "m")

    with pytest.raises(sqlite3.IntegrityError):
        db._conn.execute(
            """INSERT INTO evidence_spans
               (extraction_id, field_name, value, source_snippet, confidence, tier)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (ext_id, "f", "v", "s", 0.9, None),
        )


def test_cloud_null_confidence_raises_integrity_error(tmp_path):
    """L3: cloud_evidence_spans rejects NULL confidence."""
    from engine.cloud.schema import init_cloud_tables

    db = ReviewDatabase("test_cloud_l3", data_root=tmp_path)
    init_cloud_tables(str(db.db_path))

    # Need a paper for the FK
    db.add_papers([_cit(pmid="CL3_1", title="Cloud L3")])
    pid = db.get_papers_by_status("INGESTED")[0]["id"]

    ce_id = db._conn.execute(
        """INSERT INTO cloud_extractions
           (paper_id, arm, model_string, extracted_at)
           VALUES (?, 'test_arm', 'model', '2026-01-01')""",
        (pid,),
    ).lastrowid
    db._conn.commit()

    with pytest.raises(sqlite3.IntegrityError):
        db._conn.execute(
            """INSERT INTO cloud_evidence_spans
               (cloud_extraction_id, field_name, value, confidence, tier)
               VALUES (?, 'f', 'v', NULL, 1)""",
            (ce_id,),
        )
    db.close()


def test_cloud_null_tier_raises_integrity_error(tmp_path):
    """L3: cloud_evidence_spans rejects NULL tier."""
    from engine.cloud.schema import init_cloud_tables

    db = ReviewDatabase("test_cloud_l3b", data_root=tmp_path)
    init_cloud_tables(str(db.db_path))

    db.add_papers([_cit(pmid="CL3_2", title="Cloud L3b")])
    pid = db.get_papers_by_status("INGESTED")[0]["id"]

    ce_id = db._conn.execute(
        """INSERT INTO cloud_extractions
           (paper_id, arm, model_string, extracted_at)
           VALUES (?, 'test_arm', 'model', '2026-01-01')""",
        (pid,),
    ).lastrowid
    db._conn.commit()

    with pytest.raises(sqlite3.IntegrityError):
        db._conn.execute(
            """INSERT INTO cloud_evidence_spans
               (cloud_extraction_id, field_name, value, confidence, tier)
               VALUES (?, 'f', 'v', 0.9, NULL)""",
            (ce_id,),
        )
    db.close()


# ── Atomic update_status ──────────────────────────────────────────────


def test_update_status_atomic_valid_transition(tmp_path):
    """update_status commits atomically on a valid transition."""
    db = ReviewDatabase("atomic_ok", data_root=tmp_path)
    db.add_papers([_cit(pmid="AO1", title="Atomic")])
    pid = db.get_papers_by_status("INGESTED")[0]["id"]
    db.update_status(pid, "ABSTRACT_SCREENED_IN")

    # Verify committed — reopen DB and check
    db.close()
    db2 = ReviewDatabase("atomic_ok", data_root=tmp_path)
    assert db2.get_papers_by_status("ABSTRACT_SCREENED_IN")[0]["id"] == pid
    db2.close()


def test_update_status_invalid_transition_still_raises(tmp_path):
    """update_status still raises ValueError on invalid transitions (existing behavior)."""
    db = ReviewDatabase("atomic_err", data_root=tmp_path)
    db.add_papers([_cit(pmid="AE1", title="Invalid")])
    pid = db.get_papers_by_status("INGESTED")[0]["id"]

    with pytest.raises(ValueError, match="Invalid transition"):
        db.update_status(pid, "EXTRACTED")

    # Paper should still be INGESTED
    assert db.get_papers_by_status("INGESTED")[0]["id"] == pid
    db.close()
