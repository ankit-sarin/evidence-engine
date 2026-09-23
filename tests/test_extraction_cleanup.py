"""Tests for the extraction staleness report.

The delete branch is retired (R25, R94; row D10). Every test that pinned a
deletion, a status reset or the delete transaction is REWRITTEN to pin the
refusal (B5): each keeps its subject and says in its docstring which retired
behaviour it used to pin.
"""

import logging

import pytest

from engine.core.database import ReviewDatabase
from engine.core.review_paths import ReviewIdMismatchError
from engine.core.review_spec import ReviewSpecError, load_review_spec
from engine.search.models import Citation
from engine.utils.extraction_cleanup import (
    DeletionRetired,
    check_stale_extractions,
    cleanup_stale_extractions,
    get_current_schema_hash,
)


SPEC_PATH = "review_specs/surgical_autonomy.yaml"
CODEBOOK_PATH = "data/surgical_autonomy/extraction_codebook.yaml"


@pytest.fixture
def db(tmp_path):
    rdb = ReviewDatabase("test_cleanup", data_root=tmp_path)
    yield rdb
    rdb.close()


def _add_paper(db, pmid="1"):
    db.add_papers([Citation(title=f"Paper {pmid}", source="pubmed", pmid=pmid)])
    return db._conn.execute("SELECT id FROM papers WHERE pmid = ?", (pmid,)).fetchone()["id"]


def _add_extraction(db, paper_id, codebook_hash="hash_v1", n_spans=3):
    """Insert an extraction with n evidence spans. Returns extraction id.

    Writes `codebook_hash`, which is what staleness compares now — the old
    `extraction_schema_hash` hashed a spec section that no longer exists
    (SCHEMA-DERIVE-01).
    """
    db._conn.execute(
        "INSERT INTO extractions (paper_id, codebook_hash, extracted_data, "
        "model, extracted_at) VALUES (?, ?, '{}', 'test', '2026-01-01')",
        (paper_id, codebook_hash),
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


def _counts(db):
    ext = db._conn.execute("SELECT COUNT(*) FROM extractions").fetchone()[0]
    spans = db._conn.execute("SELECT COUNT(*) FROM evidence_spans").fetchone()[0]
    return ext, spans


def _status(db, pid):
    return db._conn.execute(
        "SELECT status FROM papers WHERE id = ?", (pid,)).fetchone()["status"]


class TestSchemaCleanup:

    def test_confirm_refuses_and_keeps_every_extraction(self, db):
        """Was test_removes_non_matching_schema_only, which pinned the delete."""
        pid = _add_paper(db)
        _advance_to(db, pid, "EXTRACTED")
        _add_extraction(db, pid, "old_hash", n_spans=5)
        _add_extraction(db, pid, "current_hash", n_spans=3)

        with pytest.raises(DeletionRetired, match="R94"):
            cleanup_stale_extractions(db, schema_hash="current_hash", dry_run=False)

        assert _counts(db) == (2, 8)

    def test_confirm_refuses_and_keeps_every_span(self, db):
        """Was test_spans_cascade_deleted, which pinned the span delete."""
        pid = _add_paper(db, pmid="2")
        _advance_to(db, pid, "EXTRACTED")
        _add_extraction(db, pid, "stale", n_spans=10)

        with pytest.raises(DeletionRetired):
            cleanup_stale_extractions(db, schema_hash="fresh", dry_run=False)

        assert _counts(db) == (1, 10)

    def test_the_refusal_names_the_event_route(self, db):
        with pytest.raises(DeletionRetired) as exc:
            cleanup_stale_extractions(db, schema_hash="x", dry_run=False)
        msg = str(exc.value)
        assert "R25" in msg and "R94" in msg
        assert "superseded by event, never deleted" in msg


class TestStatusReset:

    def test_extracted_paper_is_not_reset(self, db):
        """Was test_extracted_papers_reset_to_parsed, which pinned the reset."""
        pid = _add_paper(db)
        _advance_to(db, pid, "EXTRACTED")
        _add_extraction(db, pid, "old")

        with pytest.raises(DeletionRetired):
            cleanup_stale_extractions(db, schema_hash="new", dry_run=False)

        assert _status(db, pid) == "EXTRACTED"

    def test_ai_audit_complete_paper_is_not_reset(self, db):
        """Was test_ai_audit_complete_papers_reset_to_parsed — the path that
        put every live corpus paper back in the extractor's pickup set."""
        pid = _add_paper(db, pmid="3")
        _advance_to(db, pid, "AI_AUDIT_COMPLETE")
        _add_extraction(db, pid, "old")

        with pytest.raises(DeletionRetired):
            cleanup_stale_extractions(db, schema_hash="new", dry_run=False)

        assert _status(db, pid) == "AI_AUDIT_COMPLETE"

    def test_report_does_not_count_human_audit_complete_as_resettable(self, db):
        """Was test_human_audit_complete_papers_untouched (which ran the delete);
        the protection survives as a property of the read-only report."""
        pid = _add_paper(db, pmid="4")
        _advance_to(db, pid, "HUMAN_AUDIT_COMPLETE")
        _add_extraction(db, pid, "old", n_spans=5)

        result = cleanup_stale_extractions(db, schema_hash="new", dry_run=True)

        assert result["extractions_deleted"] == 1  # "would delete"
        assert result["papers_reset"] == 0
        assert _status(db, pid) == "HUMAN_AUDIT_COMPLETE"
        assert _counts(db) == (1, 5)


class TestDedup:

    def test_dedup_is_reported_not_performed(self, db):
        """Was test_dedup_keeps_latest_extraction, which pinned the delete."""
        pid = _add_paper(db, pmid="5")
        _advance_to(db, pid, "EXTRACTED")
        ext1 = _add_extraction(db, pid, "v1", n_spans=3)
        _add_extraction(db, pid, "v2", n_spans=5)

        report = cleanup_stale_extractions(db, schema_hash=None, dry_run=True)
        assert [d["extraction_id"] for d in report["details"]] == [ext1]

        with pytest.raises(DeletionRetired):
            cleanup_stale_extractions(db, schema_hash=None, dry_run=False)
        assert _counts(db) == (2, 8)


class TestSchemaHashResolution:

    def test_get_current_schema_hash_is_the_codebook_hash(self):
        """It returns what an extraction is stale AGAINST.

        Was `spec.extraction_hash()`; the spec section it hashed is gone and
        the prompt is built from the codebook (SCHEMA-DERIVE-01).
        """
        from engine.core.codebook import load_codebook_for

        expected = load_codebook_for("surgical_autonomy").semantic_hash
        assert get_current_schema_hash("surgical_autonomy") == expected

    def test_get_current_schema_hash_derives_the_spec_path(self):
        """The spec path is derived from the review id, not searched for.

        It replaced a glob ({name}*.yaml, first lexicographic match) that
        after the SPEC-AUTH-01 rename matched two files.
        """
        h = get_current_schema_hash("surgical_autonomy")
        assert len(h) == 64  # SHA-256 hex

    def test_get_current_schema_hash_missing_review_raises(self):
        from engine.core.codebook import CodebookError

        with pytest.raises(CodebookError, match="Codebook not found"):
            get_current_schema_hash("nonexistent_review_xyz")

    def test_get_current_schema_hash_refuses_a_codebook_for_another_review(self):
        """An override naming a different review is refused, not used."""
        from engine.core.codebook import CodebookIdentityError

        with pytest.raises(CodebookIdentityError):
            get_current_schema_hash("some_other_review", codebook_path=CODEBOOK_PATH)


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

    def test_informs_when_stale_exist_and_names_no_deletion(self, db, caplog):
        """Was test_warns_when_stale_exist, which pinned a WARNING telling the
        operator to run extraction_cleanup (R94: its delete is retired)."""
        from unittest.mock import patch
        from engine.core.review_spec import load_review_spec

        spec = load_review_spec(SPEC_PATH)
        pid = _add_paper(db, pmid="20")
        _advance_to(db, pid, "EXTRACTED")
        _add_extraction(db, pid, "stale_hash_abc")

        with caplog.at_level(logging.INFO):
            from engine.agents.extractor import run_extraction
            with patch("engine.utils.ollama_preflight.require_preflight"):
                run_extraction(db, spec, review_name="test_cleanup")

        msgs = [r for r in caplog.records if "without the current codebook hash" in r.getMessage()]
        assert len(msgs) == 1
        assert msgs[0].levelno == logging.INFO
        text = msgs[0].getMessage()
        assert "extraction_cleanup" not in text
        assert "delet" not in text.lower() and "clean up" not in text.lower()
        assert "session 9" in text

    def test_silent_when_no_stale(self, db, caplog):
        """run_extraction says nothing when all extractions match current schema."""
        from unittest.mock import patch
        from engine.core.review_spec import load_review_spec
        from engine.agents.extractor import run_extraction

        from engine.core.codebook import load_codebook_beside

        spec = load_review_spec(SPEC_PATH)
        current_hash = load_codebook_beside(db.db_path).semantic_hash

        pid = _add_paper(db, pmid="21")
        _advance_to(db, pid, "EXTRACTED")
        _add_extraction(db, pid, current_hash)

        with caplog.at_level(logging.INFO):
            with patch("engine.utils.ollama_preflight.require_preflight"):
                run_extraction(db, spec, review_name="test_cleanup")

        assert not any("without the current codebook hash" in m for m in caplog.messages)


class TestAtomicDelete:

    def test_refusal_precedes_any_query(self):
        """Was test_both_deletes_in_single_transaction. The refusal comes before
        the database is touched at all: an object with no connection is enough."""
        class NoDb:
            @property
            def _conn(self):
                raise AssertionError("the refusal must not reach the database")

        with pytest.raises(DeletionRetired):
            cleanup_stale_extractions(NoDb(), schema_hash="new_hash", dry_run=False)

    def test_refusal_leaves_everything_under_a_blocking_trigger(self, db):
        """Was test_delete_failure_preserves_all (rollback of a failed delete).
        No delete is attempted, so the blocking trigger never fires."""
        pid = _add_paper(db, pmid="AD2")
        _advance_to(db, pid, "EXTRACTED")
        _add_extraction(db, pid, "old_hash", n_spans=5)
        db._conn.execute(
            """CREATE TRIGGER block_ext_delete BEFORE DELETE ON extractions
               BEGIN SELECT RAISE(ABORT, 'simulated delete failure'); END""")
        db._conn.commit()

        with pytest.raises(DeletionRetired):
            cleanup_stale_extractions(db, schema_hash="new_hash", dry_run=False)
        assert _counts(db) == (1, 5)


class TestAdminResetAuditTrail:

    def test_refusal_writes_no_admin_reset(self, db):
        """Was test_cleanup_uses_admin_reset, which pinned the audited reset."""
        pid = _add_paper(db, pmid="ART1")
        _advance_to(db, pid, "AI_AUDIT_COMPLETE")
        _add_extraction(db, pid, "old_hash")

        with pytest.raises(DeletionRetired):
            cleanup_stale_extractions(db, schema_hash="new_hash", dry_run=False)

        assert _status(db, pid) == "AI_AUDIT_COMPLETE"
        # admin_reset_status creates its log table on first use, so no table
        # at all is the strongest form of "no reset was recorded".
        has_table = db._conn.execute(
            "SELECT 1 FROM sqlite_master WHERE name = 'admin_resets'").fetchone()
        assert not has_table or db._conn.execute(
            "SELECT COUNT(*) FROM admin_resets WHERE paper_id = ?", (pid,)
        ).fetchone()[0] == 0


class TestCli:

    def test_confirm_refuses_before_any_database_is_constructed(self, monkeypatch, capsys):
        """G4: `--confirm` exits 2 without ever constructing ReviewDatabase,
        whose __init__ runs the migration runner."""
        import sys
        import engine.utils.extraction_cleanup as ec

        def boom(*a, **k):
            raise AssertionError("ReviewDatabase must not be constructed")

        monkeypatch.setattr(ec, "ReviewDatabase", boom)
        monkeypatch.setattr(sys, "argv", ["extraction_cleanup", "--review",
                                          "surgical_autonomy", "--confirm"])
        with pytest.raises(SystemExit) as exc:
            ec.main()
        assert exc.value.code == 2
        assert "R94" in capsys.readouterr().err
