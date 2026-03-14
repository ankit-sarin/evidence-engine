"""Tests for post-extraction field validation."""

from pathlib import Path

import pytest

from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec
from engine.search.models import Citation
from engine.validators.extraction_validator import validate_extraction, _closest_match


@pytest.fixture
def spec():
    return load_review_spec("review_specs/surgical_autonomy_v1.yaml")


@pytest.fixture
def db(tmp_path):
    rdb = ReviewDatabase("test_val", data_root=tmp_path)
    yield rdb
    rdb.close()


def _add_paper_and_extraction(db, spans, pmid="1"):
    """Helper: add paper, extraction, and evidence spans."""
    db.add_papers([Citation(title="Test", source="pubmed", pmid=pmid)])
    pid = db._conn.execute("SELECT id FROM papers WHERE pmid = ?", (pmid,)).fetchone()["id"]
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")
    db.update_status(pid, "PARSED")
    db.update_status(pid, "EXTRACTED")

    db._conn.execute(
        "INSERT INTO extractions (paper_id, model, extraction_schema_hash, extracted_at, extracted_data) "
        "VALUES (?, 'test', 'abc', '2026-01-01', '{}')",
        (pid,),
    )
    ext_id = db._conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    for s in spans:
        db._conn.execute(
            "INSERT INTO evidence_spans (extraction_id, field_name, value, "
            "source_snippet, confidence) VALUES (?, ?, ?, ?, ?)",
            (ext_id, s["field_name"], s["value"], "snippet", 0.9),
        )
    db._conn.commit()
    return pid


def test_valid_spans_no_issues(db, spec):
    pid = _add_paper_and_extraction(db, [
        {"field_name": "study_type", "value": "Original Research"},
        {"field_name": "autonomy_level", "value": "3 (Conditional autonomy)"},
        {"field_name": "sample_size", "value": "42"},
        {"field_name": "robot_platform", "value": "da Vinci"},
    ])
    issues = validate_extraction(spec, pid, db)
    assert issues == []


def test_unknown_field_name_flagged(db, spec):
    pid = _add_paper_and_extraction(db, [
        {"field_name": "studytype", "value": "Original Research"},
    ], pmid="2")
    issues = validate_extraction(spec, pid, db)
    assert len(issues) == 1
    assert "unknown field name" in issues[0]["issue"]
    assert "study_type" in issues[0]["issue"]  # closest match suggestion


def test_invalid_categorical_value_flagged(db, spec):
    pid = _add_paper_and_extraction(db, [
        {"field_name": "study_type", "value": "Orginal Research"},
    ], pmid="3")
    issues = validate_extraction(spec, pid, db)
    assert len(issues) == 1
    assert "invalid categorical value" in issues[0]["issue"]
    assert "closest:" in issues[0]["issue"]
    assert "Original Research" in issues[0]["issue"]


def test_numeric_field_non_numeric_flagged(db, spec):
    pid = _add_paper_and_extraction(db, [
        {"field_name": "sample_size", "value": "twelve patients"},
    ], pmid="4")
    issues = validate_extraction(spec, pid, db)
    assert len(issues) == 1
    assert "non-numeric sample_size" in issues[0]["issue"]


def test_not_found_value_accepted(db, spec):
    pid = _add_paper_and_extraction(db, [
        {"field_name": "study_type", "value": "NOT_FOUND"},
        {"field_name": "sample_size", "value": "NR"},
    ], pmid="5")
    issues = validate_extraction(spec, pid, db)
    assert issues == []


def test_closest_match_similarity():
    assert _closest_match("Feasability study", ["Feasibility study", "Case Report"]) == "Feasibility study"
    assert _closest_match("xyz_garbage", ["a", "b"]) is None
