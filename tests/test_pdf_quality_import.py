"""Tests for PDF quality disposition import."""

import json
from pathlib import Path

import pytest

from engine.acquisition.pdf_quality_import import (
    VALID_DISPOSITIONS,
    VALID_EXCLUDE_REASONS,
    import_dispositions,
    validate_disposition_json,
)
from engine.core.database import ReviewDatabase
from engine.search.models import Citation


# ── Fixtures ──────────────────────────────────────────────────────────


def _make_citation(idx, **kwargs):
    return Citation(
        title=kwargs.get("title", f"Paper {idx}"),
        abstract="Abstract",
        pmid=str(10000 + idx),
        doi=kwargs.get("doi"),
        source="pubmed",
        authors=kwargs.get("authors", ["Smith J"]),
        journal="J Test",
        year=kwargs.get("year", 2024),
    )


def _setup_db(tmp_path, n_papers=3):
    """Create DB with n papers at ABSTRACT_SCREENED_IN."""
    db = ReviewDatabase("test_import", data_root=tmp_path)
    pids = []
    for i in range(n_papers):
        cit = _make_citation(i)
        db.add_papers([cit])
        pid = db._conn.execute(
            "SELECT id FROM papers ORDER BY id DESC LIMIT 1"
        ).fetchone()["id"]
        db.update_status(pid, "ABSTRACT_SCREENED_IN")
        pids.append(pid)
    return db, pids


def _write_json(tmp_path, papers, complete=False):
    """Write a disposition JSON file and return its path."""
    data = {
        "review": "test_import",
        "exported_at": "2026-03-14T00:00:00Z",
        "mode": "quality_check",
        "complete": complete,
        "papers": papers,
    }
    path = tmp_path / "dispositions.json"
    path.write_text(json.dumps(data))
    return path


# ── Validation tests ─────────────────────────────────────────────────


class TestValidation:
    def test_valid_proceed(self, tmp_path):
        db, pids = _setup_db(tmp_path)
        data = {"papers": [
            {"paper_id": pids[0], "disposition": "PROCEED"},
        ]}
        errors = validate_disposition_json(data, db._conn)
        assert errors == []

    def test_valid_exclude(self, tmp_path):
        db, pids = _setup_db(tmp_path)
        data = {"papers": [
            {"paper_id": pids[0], "disposition": "EXCLUDE_NON_ENGLISH",
             "exclude_reason": "NON_ENGLISH"},
        ]}
        errors = validate_disposition_json(data, db._conn)
        assert errors == []

    def test_valid_all_dispositions(self, tmp_path):
        db, pids = _setup_db(tmp_path, n_papers=7)
        papers = [
            {"paper_id": pids[0], "disposition": "PROCEED"},
            {"paper_id": pids[1], "disposition": "PDF_WILL_ATTEMPT"},
            {"paper_id": pids[2], "disposition": "EXCLUDE_NON_ENGLISH", "exclude_reason": "NON_ENGLISH"},
            {"paper_id": pids[3], "disposition": "EXCLUDE_NOT_MANUSCRIPT", "exclude_reason": "NOT_MANUSCRIPT"},
            {"paper_id": pids[4], "disposition": "EXCLUDE_INACCESSIBLE", "exclude_reason": "INACCESSIBLE"},
            {"paper_id": pids[5], "disposition": "EXCLUDE_OTHER", "exclude_reason": "OTHER", "exclude_detail": "thesis"},
            {"paper_id": pids[6], "disposition": "UNSET"},
        ]
        errors = validate_disposition_json({"papers": papers}, db._conn)
        assert errors == []

    def test_invalid_disposition(self, tmp_path):
        db, pids = _setup_db(tmp_path)
        data = {"papers": [
            {"paper_id": pids[0], "disposition": "INVALID_VALUE"},
        ]}
        errors = validate_disposition_json(data, db._conn)
        assert len(errors) == 1
        assert "invalid disposition" in errors[0]

    def test_missing_paper_id(self, tmp_path):
        db, _ = _setup_db(tmp_path)
        data = {"papers": [{"disposition": "PROCEED"}]}
        errors = validate_disposition_json(data, db._conn)
        assert len(errors) == 1
        assert "missing paper_id" in errors[0]

    def test_nonexistent_paper(self, tmp_path):
        db, _ = _setup_db(tmp_path)
        data = {"papers": [
            {"paper_id": 99999, "disposition": "PROCEED"},
        ]}
        errors = validate_disposition_json(data, db._conn)
        assert len(errors) == 1
        assert "not found in database" in errors[0]

    def test_duplicate_paper_id(self, tmp_path):
        db, pids = _setup_db(tmp_path)
        data = {"papers": [
            {"paper_id": pids[0], "disposition": "PROCEED"},
            {"paper_id": pids[0], "disposition": "PROCEED"},
        ]}
        errors = validate_disposition_json(data, db._conn)
        assert len(errors) == 1
        assert "duplicate" in errors[0]

    def test_invalid_exclude_reason(self, tmp_path):
        db, pids = _setup_db(tmp_path)
        data = {"papers": [
            {"paper_id": pids[0], "disposition": "EXCLUDE_NON_ENGLISH",
             "exclude_reason": "BAD_REASON"},
        ]}
        errors = validate_disposition_json(data, db._conn)
        assert len(errors) == 1
        assert "invalid exclude_reason" in errors[0]

    def test_missing_papers_key(self, tmp_path):
        db, _ = _setup_db(tmp_path)
        errors = validate_disposition_json({}, db._conn)
        assert len(errors) == 1
        assert "Missing 'papers'" in errors[0]

    def test_non_integer_paper_id(self, tmp_path):
        db, _ = _setup_db(tmp_path)
        data = {"papers": [
            {"paper_id": "abc", "disposition": "PROCEED"},
        ]}
        errors = validate_disposition_json(data, db._conn)
        assert len(errors) == 1
        assert "must be an integer" in errors[0]


# ── Import tests ─────────────────────────────────────────────────────


class TestImport:
    def test_proceed_sets_human_confirmed(self, tmp_path):
        db, pids = _setup_db(tmp_path)
        json_path = _write_json(tmp_path, [
            {"paper_id": pids[0], "disposition": "PROCEED"},
        ])
        stats = import_dispositions("test_import", str(json_path), data_root=tmp_path,
                                    dry_run=False)
        assert stats["proceeded"] == 1

        row = db._conn.execute(
            "SELECT pdf_quality_check_status FROM papers WHERE id = ?",
            (pids[0],),
        ).fetchone()
        assert row["pdf_quality_check_status"] == "HUMAN_CONFIRMED"

    def test_exclude_sets_pdf_excluded(self, tmp_path):
        db, pids = _setup_db(tmp_path)
        json_path = _write_json(tmp_path, [
            {"paper_id": pids[0], "disposition": "EXCLUDE_NON_ENGLISH",
             "exclude_reason": "NON_ENGLISH", "exclude_detail": None},
        ])
        stats = import_dispositions("test_import", str(json_path), data_root=tmp_path)
        assert stats["excluded"] == 1
        assert stats["exclude_breakdown"]["NON_ENGLISH"] == 1

        row = db._conn.execute(
            "SELECT status, pdf_exclusion_reason FROM papers WHERE id = ?",
            (pids[0],),
        ).fetchone()
        assert row["status"] == "PDF_EXCLUDED"
        assert row["pdf_exclusion_reason"] == "NON_ENGLISH"

    def test_exclude_other_with_detail(self, tmp_path):
        db, pids = _setup_db(tmp_path)
        json_path = _write_json(tmp_path, [
            {"paper_id": pids[0], "disposition": "EXCLUDE_OTHER",
             "exclude_reason": "OTHER", "exclude_detail": "thesis cover page"},
        ])
        stats = import_dispositions("test_import", str(json_path), data_root=tmp_path)
        assert stats["excluded"] == 1

        row = db._conn.execute(
            "SELECT pdf_exclusion_reason, pdf_exclusion_detail FROM papers WHERE id = ?",
            (pids[0],),
        ).fetchone()
        assert row["pdf_exclusion_reason"] == "OTHER"
        assert row["pdf_exclusion_detail"] == "thesis cover page"

    def test_will_attempt_no_change(self, tmp_path):
        db, pids = _setup_db(tmp_path)
        json_path = _write_json(tmp_path, [
            {"paper_id": pids[0], "disposition": "PDF_WILL_ATTEMPT"},
        ])
        stats = import_dispositions("test_import", str(json_path), data_root=tmp_path)
        assert stats["will_attempt"] == 1

        row = db._conn.execute(
            "SELECT status, pdf_quality_check_status FROM papers WHERE id = ?",
            (pids[0],),
        ).fetchone()
        assert row["status"] == "ABSTRACT_SCREENED_IN"
        assert row["pdf_quality_check_status"] is None

    def test_unset_skipped(self, tmp_path):
        db, pids = _setup_db(tmp_path)
        json_path = _write_json(tmp_path, [
            {"paper_id": pids[0], "disposition": "UNSET"},
        ])
        stats = import_dispositions("test_import", str(json_path), data_root=tmp_path)
        assert stats["skipped_unset"] == 1

        row = db._conn.execute(
            "SELECT status FROM papers WHERE id = ?", (pids[0],),
        ).fetchone()
        assert row["status"] == "ABSTRACT_SCREENED_IN"

    def test_skip_already_excluded(self, tmp_path):
        db, pids = _setup_db(tmp_path)
        # Pre-exclude
        db._conn.execute(
            "UPDATE papers SET status = 'PDF_EXCLUDED' WHERE id = ?",
            (pids[0],),
        )
        db._conn.commit()

        json_path = _write_json(tmp_path, [
            {"paper_id": pids[0], "disposition": "PROCEED"},
        ])
        stats = import_dispositions("test_import", str(json_path), data_root=tmp_path)
        assert stats["skipped_already"] == 1
        assert stats["proceeded"] == 0

    def test_skip_already_confirmed(self, tmp_path):
        db, pids = _setup_db(tmp_path)
        db._conn.execute(
            "UPDATE papers SET pdf_quality_check_status = 'HUMAN_CONFIRMED' WHERE id = ?",
            (pids[0],),
        )
        db._conn.commit()

        json_path = _write_json(tmp_path, [
            {"paper_id": pids[0], "disposition": "EXCLUDE_NON_ENGLISH",
             "exclude_reason": "NON_ENGLISH"},
        ])
        stats = import_dispositions("test_import", str(json_path), data_root=tmp_path)
        assert stats["skipped_already"] == 1
        assert stats["excluded"] == 0

    def test_dry_run_no_changes(self, tmp_path):
        db, pids = _setup_db(tmp_path)
        json_path = _write_json(tmp_path, [
            {"paper_id": pids[0], "disposition": "PROCEED"},
            {"paper_id": pids[1], "disposition": "EXCLUDE_NOT_MANUSCRIPT",
             "exclude_reason": "NOT_MANUSCRIPT"},
        ])
        stats = import_dispositions("test_import", str(json_path), data_root=tmp_path, dry_run=True)
        assert stats["proceeded"] == 1
        assert stats["excluded"] == 1

        # DB unchanged
        for pid in pids[:2]:
            row = db._conn.execute(
                "SELECT status, pdf_quality_check_status FROM papers WHERE id = ?",
                (pid,),
            ).fetchone()
            assert row["status"] == "ABSTRACT_SCREENED_IN"
            assert row["pdf_quality_check_status"] is None

    def test_atomic_rejects_all_on_error(self, tmp_path):
        db, pids = _setup_db(tmp_path)
        json_path = _write_json(tmp_path, [
            {"paper_id": pids[0], "disposition": "PROCEED"},
            {"paper_id": 99999, "disposition": "PROCEED"},  # bad ID
        ])
        with pytest.raises(ValueError, match="Validation failed"):
            import_dispositions("test_import", str(json_path), data_root=tmp_path)

        # First paper should NOT have been changed
        row = db._conn.execute(
            "SELECT pdf_quality_check_status FROM papers WHERE id = ?",
            (pids[0],),
        ).fetchone()
        assert row["pdf_quality_check_status"] is None

    def test_complete_flag(self, tmp_path):
        db, pids = _setup_db(tmp_path)
        json_path = _write_json(tmp_path, [
            {"paper_id": pids[0], "disposition": "PROCEED"},
        ], complete=True)
        stats = import_dispositions("test_import", str(json_path), data_root=tmp_path)
        assert stats["is_complete"] is True

    def test_mixed_dispositions(self, tmp_path):
        db, pids = _setup_db(tmp_path, n_papers=5)
        json_path = _write_json(tmp_path, [
            {"paper_id": pids[0], "disposition": "PROCEED"},
            {"paper_id": pids[1], "disposition": "EXCLUDE_INACCESSIBLE",
             "exclude_reason": "INACCESSIBLE"},
            {"paper_id": pids[2], "disposition": "PDF_WILL_ATTEMPT"},
            {"paper_id": pids[3], "disposition": "UNSET"},
            {"paper_id": pids[4], "disposition": "EXCLUDE_NON_ENGLISH",
             "exclude_reason": "NON_ENGLISH"},
        ])
        stats = import_dispositions("test_import", str(json_path), data_root=tmp_path)
        assert stats["proceeded"] == 1
        assert stats["excluded"] == 2
        assert stats["exclude_breakdown"]["INACCESSIBLE"] == 1
        assert stats["exclude_breakdown"]["NON_ENGLISH"] == 1
        assert stats["will_attempt"] == 1
        assert stats["skipped_unset"] == 1

    def test_file_not_found(self, tmp_path):
        db, _ = _setup_db(tmp_path)
        with pytest.raises(FileNotFoundError):
            import_dispositions("test_import", str(tmp_path / "missing.json"))

    def test_db_not_found(self, tmp_path):
        json_path = _write_json(tmp_path, [])
        with pytest.raises(FileNotFoundError):
            import_dispositions("nonexistent_review", str(json_path))

    def test_all_exclude_reasons(self, tmp_path):
        """Each EXCLUDE_* disposition maps to the correct DB reason."""
        db, pids = _setup_db(tmp_path, n_papers=4)
        papers = [
            {"paper_id": pids[0], "disposition": "EXCLUDE_NON_ENGLISH", "exclude_reason": "NON_ENGLISH"},
            {"paper_id": pids[1], "disposition": "EXCLUDE_NOT_MANUSCRIPT", "exclude_reason": "NOT_MANUSCRIPT"},
            {"paper_id": pids[2], "disposition": "EXCLUDE_INACCESSIBLE", "exclude_reason": "INACCESSIBLE"},
            {"paper_id": pids[3], "disposition": "EXCLUDE_OTHER", "exclude_reason": "OTHER", "exclude_detail": "retracted"},
        ]
        json_path = _write_json(tmp_path, papers)
        import_dispositions("test_import", str(json_path), data_root=tmp_path)

        expected = {
            pids[0]: "NON_ENGLISH",
            pids[1]: "NOT_MANUSCRIPT",
            pids[2]: "INACCESSIBLE",
            pids[3]: "OTHER",
        }
        for pid, reason in expected.items():
            row = db._conn.execute(
                "SELECT status, pdf_exclusion_reason FROM papers WHERE id = ?",
                (pid,),
            ).fetchone()
            assert row["status"] == "PDF_EXCLUDED"
            assert row["pdf_exclusion_reason"] == reason
