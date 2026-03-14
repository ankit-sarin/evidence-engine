"""Tests for extraction cleanup utility."""

import logging

import pytest

from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec
from engine.search.models import Citation
from engine.utils.extraction_cleanup import (
    check_stale_extractions,
    cleanup_stale_extractions,
    get_current_schema_hash,
)


SPEC_PATH = "review_specs/surgical_autonomy_v1.yaml"


@pytest.fixture
def db(tmp_path):
    rdb = ReviewDatabase("test_cleanup", data_root=tmp_path)
    yield rdb
    rdb.close()


def _add_paper(db, pmid="1"):
    db.add_papers([Citation(title=f"Paper {pmid}", source="pubmed", pmid=pmid)])
    return db._conn.execute("SELECT id FROM papers WHERE pmid = ?", (pmid,)).fetchone()["id"]


def _add_extraction(db, paper_id, schema_hash="hash_v1", n_spans=3):
    """Insert an extraction with n evidence spans. Returns extraction id."""
    db._conn.execute(
        "INSERT INTO extractions (paper_id, extraction_schema_hash, extracted_data, "
        "model, extracted_at) VALUES (?, ?, '{}', 'test', '2026-01-01')",
        (paper_id, schema_hash),
    )
    ext_id = db._conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    for i in range(n_spans):
        db._conn.execute(
            "INSERT INTO evidence_spans (extraction_id, field_name, value, "
            "source_snippet, confidence) VALUES (?, ?, ?, ?, ?)",
            (ext_id, f"field_{i}", f"val_{i}", "snip", 0.9),
        )
    db._conn.commit()
    return ext_id


def _advance_to(db, pid, target):
    """Walk paper through lifecycle to target status."""
    path = {
        "EXTRACTED": ["ABSTRACT_SCREENED_IN", "PDF_ACQUIRED", "PARSED", "EXTRACTED"],
        "AI_AUDIT_COMPLETE": ["ABSTRACT_SCREENED_IN", "PDF_ACQUIRED", "PARSED",
                              "EXTRACTED", "AI_AUDIT_COMPLETE"],
        "HUMAN_AUDIT_COMPLETE": ["ABSTRACT_SCREENED_IN", "PDF_ACQUIRED", "PARSED",
                                 "EXTRACTED", "AI_AUDIT_COMPLETE", "HUMAN_AUDIT_COMPLETE"],
    }
    for status in path[target]:
        db.update_status(pid, status)


class TestDryRun:

    def test_dry_run_reports_without_deleting(self, db):
        pid = _add_paper(db)
        _advance_to(db, pid, "EXTRACTED")
        _add_extraction(db, pid, "old_hash", n_spans=5)

        result = cleanup_stale_extractions(db, schema_hash="new_hash", dry_run=True)

        assert result["dry_run"] is True
        assert result["extractions_deleted"] == 1
        assert result["spans_deleted"] == 5

        # Nothing actually deleted
        ext_count = db._conn.execute("SELECT COUNT(*) FROM extractions").fetchone()[0]
        span_count = db._conn.execute("SELECT COUNT(*) FROM evidence_spans").fetchone()[0]
        assert ext_count == 1
        assert span_count == 5


class TestSchemaCleanup:

    def test_removes_non_matching_schema_only(self, db):
        pid = _add_paper(db)
        _advance_to(db, pid, "EXTRACTED")
        _add_extraction(db, pid, "old_hash", n_spans=5)
        _add_extraction(db, pid, "current_hash", n_spans=3)

        result = cleanup_stale_extractions(db, schema_hash="current_hash", dry_run=False)

        assert result["extractions_deleted"] == 1
        assert result["spans_deleted"] == 5

        # Only current-hash extraction remains
        rows = db._conn.execute("SELECT extraction_schema_hash FROM extractions").fetchall()
        assert len(rows) == 1
        assert rows[0]["extraction_schema_hash"] == "current_hash"

    def test_spans_cascade_deleted(self, db):
        pid = _add_paper(db, pmid="2")
        _advance_to(db, pid, "EXTRACTED")
        _add_extraction(db, pid, "stale", n_spans=10)

        result = cleanup_stale_extractions(db, schema_hash="fresh", dry_run=False)

        assert result["spans_deleted"] == 10
        span_count = db._conn.execute("SELECT COUNT(*) FROM evidence_spans").fetchone()[0]
        assert span_count == 0


class TestStatusReset:

    def test_extracted_papers_reset_to_parsed(self, db):
        pid = _add_paper(db)
        _advance_to(db, pid, "EXTRACTED")
        _add_extraction(db, pid, "old")

        cleanup_stale_extractions(db, schema_hash="new", dry_run=False)

        status = db._conn.execute(
            "SELECT status FROM papers WHERE id = ?", (pid,)
        ).fetchone()["status"]
        assert status == "PARSED"

    def test_ai_audit_complete_papers_reset_to_parsed(self, db):
        pid = _add_paper(db, pmid="3")
        _advance_to(db, pid, "AI_AUDIT_COMPLETE")
        _add_extraction(db, pid, "old")

        cleanup_stale_extractions(db, schema_hash="new", dry_run=False)

        status = db._conn.execute(
            "SELECT status FROM papers WHERE id = ?", (pid,)
        ).fetchone()["status"]
        assert status == "PARSED"

    def test_human_audit_complete_papers_untouched(self, db):
        pid = _add_paper(db, pmid="4")
        _advance_to(db, pid, "HUMAN_AUDIT_COMPLETE")
        _add_extraction(db, pid, "old", n_spans=5)

        result = cleanup_stale_extractions(db, schema_hash="new", dry_run=False)

        # Extraction and spans are deleted
        assert result["extractions_deleted"] == 1

        # But status is NOT reset
        status = db._conn.execute(
            "SELECT status FROM papers WHERE id = ?", (pid,)
        ).fetchone()["status"]
        assert status == "HUMAN_AUDIT_COMPLETE"
        assert result["papers_reset"] == 0


class TestDedup:

    def test_dedup_keeps_latest_extraction(self, db):
        pid = _add_paper(db, pmid="5")
        _advance_to(db, pid, "EXTRACTED")
        ext1 = _add_extraction(db, pid, "v1", n_spans=3)
        ext2 = _add_extraction(db, pid, "v2", n_spans=5)

        result = cleanup_stale_extractions(db, schema_hash=None, dry_run=False)

        assert result["extractions_deleted"] == 1
        # Only the latest (ext2) remains
        remaining = db._conn.execute("SELECT id FROM extractions").fetchone()
        assert remaining["id"] == ext2


class TestSchemaHashResolution:

    def test_get_current_schema_hash_matches_extractor(self):
        """get_current_schema_hash returns the same hash the extractor uses."""
        spec = load_review_spec(SPEC_PATH)
        expected = spec.extraction_hash()
        actual = get_current_schema_hash("surgical_autonomy", spec_path=SPEC_PATH)
        assert actual == expected

    def test_get_current_schema_hash_auto_discovers_spec(self):
        """Auto-discovery finds the spec from review_specs/{name}*.yaml."""
        # surgical_autonomy_v1.yaml exists in review_specs/
        h = get_current_schema_hash("surgical_autonomy")
        assert len(h) == 64  # SHA-256 hex

    def test_get_current_schema_hash_missing_review_raises(self):
        with pytest.raises(FileNotFoundError, match="No review spec found"):
            get_current_schema_hash("nonexistent_review_xyz")


class TestStaleExtractionCheck:

    def test_check_stale_returns_count(self, db):
        pid1 = _add_paper(db, pmid="10")
        pid2 = _add_paper(db, pmid="11")
        _advance_to(db, pid1, "EXTRACTED")
        _advance_to(db, pid2, "EXTRACTED")
        _add_extraction(db, pid1, "old_hash")
        _add_extraction(db, pid2, "old_hash")

        count = check_stale_extractions(db, "new_hash")
        assert count == 2

    def test_check_stale_zero_when_all_current(self, db):
        pid = _add_paper(db, pmid="12")
        _advance_to(db, pid, "EXTRACTED")
        _add_extraction(db, pid, "current")

        count = check_stale_extractions(db, "current")
        assert count == 0


class TestExtractionRunnerWarning:

    def test_warns_when_stale_exist(self, db, caplog):
        """run_extraction logs warning when stale extractions exist."""
        from unittest.mock import patch, MagicMock
        from engine.core.review_spec import load_review_spec

        spec = load_review_spec(SPEC_PATH)
        pid = _add_paper(db, pmid="20")
        _advance_to(db, pid, "EXTRACTED")
        _add_extraction(db, pid, "stale_hash_abc")

        # Mock to avoid actually running extraction (no parsed text, etc.)
        with caplog.at_level(logging.WARNING):
            from engine.agents.extractor import run_extraction
            run_extraction(db, spec, review_name="test_cleanup")

        assert any("stale schema extractions" in m for m in caplog.messages)

    def test_silent_when_no_stale(self, db, caplog):
        """run_extraction does not warn when all extractions match current schema."""
        from engine.core.review_spec import load_review_spec
        from engine.agents.extractor import run_extraction

        spec = load_review_spec(SPEC_PATH)
        current_hash = spec.extraction_hash()

        pid = _add_paper(db, pmid="21")
        _advance_to(db, pid, "EXTRACTED")
        _add_extraction(db, pid, current_hash)

        with caplog.at_level(logging.WARNING):
            run_extraction(db, spec, review_name="test_cleanup")

        assert not any("stale schema extractions" in m for m in caplog.messages)
