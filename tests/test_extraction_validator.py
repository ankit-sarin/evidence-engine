"""Tests for post-extraction field validation."""

from pathlib import Path

import pytest

from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec
from engine.search.models import Citation
from engine.validators.extraction_validator import (
    detect_cross_field_bleed,
    normalize_categorical_values,
    normalize_prefix,
    validate_extraction,
    verify_schema_parity,
    _closest_match,
)


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


# ── normalize_prefix unit tests ──────────────────────────────────────


SAMPLE_VALID = [
    "Original Research",
    "Case Report/Series",
    "Review",
    "Systematic Review",
    "Other",
]


def test_normalize_prefix_exact_match():
    """Exact match returns the value unchanged (including case normalization)."""
    assert normalize_prefix("Original Research", SAMPLE_VALID) == "Original Research"
    assert normalize_prefix("original research", SAMPLE_VALID) == "Original Research"


def test_normalize_prefix_unambiguous():
    """Unambiguous prefix resolves to the full canonical value."""
    assert normalize_prefix("Case", SAMPLE_VALID) == "Case Report/Series"
    assert normalize_prefix("Syst", SAMPLE_VALID) == "Systematic Review"
    # Case-insensitive
    assert normalize_prefix("case", SAMPLE_VALID) == "Case Report/Series"


def test_normalize_prefix_ambiguous():
    """Ambiguous prefix (matches multiple) returns value unchanged."""
    # "Re" matches both "Review" and "Systematic Review" would not, but
    # actually "Re" only prefix-matches "Review" — use "Other" vs "Original"
    # "Or" matches "Original Research" only. Let's use a clear ambiguous case.
    vals = ["Level 3 - Conditional", "Level 3 - High"]
    assert normalize_prefix("Level 3", vals) == "Level 3"


def test_normalize_prefix_no_match():
    """No prefix match returns value unchanged."""
    assert normalize_prefix("Randomized Trial", SAMPLE_VALID) == "Randomized Trial"


# ── normalize_categorical_values integration tests ───────────────────


def test_normalize_categorical_db_unambiguous(db, spec):
    """Unambiguous prefix in DB span is updated to canonical value."""
    pid = _add_paper_and_extraction(db, [
        {"field_name": "study_type", "value": "Case Rep"},
    ], pmid="norm1")

    changes = normalize_categorical_values(spec, pid, db)
    assert len(changes) == 1
    assert changes[0]["original"] == "Case Rep"
    assert changes[0]["canonical"] == "Case Report/Series"

    # Verify the DB was actually updated
    val = db._conn.execute(
        """SELECT es.value FROM evidence_spans es
           JOIN extractions e ON es.extraction_id = e.id
           WHERE e.paper_id = ?""",
        (pid,),
    ).fetchone()["value"]
    assert val == "Case Report/Series"


def test_normalize_categorical_db_exact(db, spec):
    """Exact match produces no changes."""
    pid = _add_paper_and_extraction(db, [
        {"field_name": "study_type", "value": "Original Research"},
    ], pmid="norm2")

    changes = normalize_categorical_values(spec, pid, db)
    assert changes == []


def test_normalize_categorical_db_ambiguous(db, spec):
    """Ambiguous prefix produces no changes and leaves DB untouched."""
    # autonomy_level has "0 (No autonomy)" and could be ambiguous with short prefixes
    # but a truly ambiguous case: validation_setting has "In vivo (human)" and "In vivo (animal)"
    pid = _add_paper_and_extraction(db, [
        {"field_name": "validation_setting", "value": "In vivo"},
    ], pmid="norm3")

    changes = normalize_categorical_values(spec, pid, db)
    assert changes == []

    val = db._conn.execute(
        """SELECT es.value FROM evidence_spans es
           JOIN extractions e ON es.extraction_id = e.id
           WHERE e.paper_id = ?""",
        (pid,),
    ).fetchone()["value"]
    assert val == "In vivo"


def test_normalize_categorical_db_no_match(db, spec):
    """Non-matching value produces no changes."""
    pid = _add_paper_and_extraction(db, [
        {"field_name": "study_type", "value": "Randomized Controlled Trial"},
    ], pmid="norm4")

    changes = normalize_categorical_values(spec, pid, db)
    assert changes == []


# ── element-wise semicolon validation tests ──────────────────────────


def test_semicolon_all_valid(db, spec):
    """All semicolon-separated elements valid → no issues."""
    pid = _add_paper_and_extraction(db, [
        {"field_name": "task_monitor", "value": "H; R; Shared"},
    ], pmid="semi1")
    issues = validate_extraction(spec, pid, db)
    assert issues == []


def test_semicolon_one_invalid(db, spec):
    """One invalid element among valid ones — only the bad element reported."""
    pid = _add_paper_and_extraction(db, [
        {"field_name": "task_monitor", "value": "H; Robotic; Shared"},
    ], pmid="semi2")
    issues = validate_extraction(spec, pid, db)
    assert len(issues) == 1
    assert issues[0]["value"] == "Robotic"
    assert "invalid categorical value" in issues[0]["issue"]


def test_single_value_no_semicolons(db, spec):
    """Single value without semicolons — unchanged validation behavior."""
    pid = _add_paper_and_extraction(db, [
        {"field_name": "study_type", "value": "Orginal Research"},
    ], pmid="semi3")
    issues = validate_extraction(spec, pid, db)
    assert len(issues) == 1
    assert issues[0]["value"] == "Orginal Research"
    assert "closest:" in issues[0]["issue"]
    assert "Original Research" in issues[0]["issue"]


def test_semicolon_all_invalid(db, spec):
    """All elements invalid — each one reported separately."""
    pid = _add_paper_and_extraction(db, [
        {"field_name": "task_monitor", "value": "Robotic; Autonomous"},
    ], pmid="semi4")
    issues = validate_extraction(spec, pid, db)
    assert len(issues) == 2
    bad_values = {i["value"] for i in issues}
    assert bad_values == {"Robotic", "Autonomous"}


# ── cross-field bleed detection tests ────────────────────────────────


def test_bleed_no_bleed(spec):
    """All values in their correct fields — no bleed detected."""
    data = [
        {"field_name": "study_type", "value": "Original Research"},
        {"field_name": "autonomy_level", "value": "3 (Conditional autonomy)"},
        {"field_name": "validation_setting", "value": "In vivo (human)"},
    ]
    bleeds = detect_cross_field_bleed(spec, data)
    assert bleeds == []


def test_bleed_detected(spec):
    """Value from validation_setting placed in study_type → bleed flagged."""
    # "Cadaver" is valid for validation_setting but not study_type
    data = [
        {"field_name": "study_type", "value": "Cadaver"},
    ]
    bleeds = detect_cross_field_bleed(spec, data)
    assert len(bleeds) == 1
    assert bleeds[0]["field_name"] == "study_type"
    assert bleeds[0]["extracted_value"] == "Cadaver"
    assert bleeds[0]["belongs_to_field"] == "validation_setting"


def test_bleed_invalid_for_all_fields(spec):
    """Value that doesn't match ANY field's vocabulary — not bleed, just wrong."""
    data = [
        {"field_name": "study_type", "value": "Quantum Teleportation"},
    ]
    bleeds = detect_cross_field_bleed(spec, data)
    assert bleeds == []


def test_bleed_semicolon_multi_value(spec):
    """One element of a semicolon-separated value bleeds."""
    # "Feasibility study" is valid for study_design but not study_type
    data = [
        {"field_name": "study_type", "value": "Original Research; Feasibility study"},
    ]
    bleeds = detect_cross_field_bleed(spec, data)
    assert len(bleeds) == 1
    assert bleeds[0]["extracted_value"] == "Feasibility study"
    assert bleeds[0]["belongs_to_field"] == "study_design"


# ── schema hash parity tests ────────────────────────────────────────


def test_same_spec_same_hash(spec):
    """Same spec produces the same prompt hash deterministically."""
    h1 = verify_schema_parity(spec)
    h2 = verify_schema_parity(spec)
    assert h1 == h2
    assert len(h1) == 64  # SHA-256 hex digest


def test_modified_spec_different_hash():
    """Modifying the spec changes the prompt hash."""
    from copy import deepcopy
    spec_a = load_review_spec("review_specs/surgical_autonomy_v1.yaml")
    spec_b = deepcopy(spec_a)

    # Add a new field to change the extraction schema
    from engine.core.review_spec import ExtractionField
    spec_b.extraction_schema.fields.append(
        ExtractionField(
            name="fake_new_field",
            description="A fake field for testing",
            type="str",
            tier=1,
        )
    )

    h_a = verify_schema_parity(spec_a)
    h_b = verify_schema_parity(spec_b)
    assert h_a != h_b
