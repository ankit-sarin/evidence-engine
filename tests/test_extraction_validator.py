"""Tests for post-extraction field validation."""

import shutil
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from engine.core.codebook import load_codebook
from engine.core.review_spec import load_review_spec
from engine.validators import extraction_validator as V
from engine.validators.extraction_validator import (
    detect_cross_field_bleed,
    normalize_prefix,
    validate_all,
    validate_extraction,
    verify_schema_parity,
    _closest_match,
)
from tests._event_store_fixture import add_values

LIVE_CODEBOOK = (Path(__file__).resolve().parent.parent
                 / "data" / "surgical_autonomy" / "extraction_codebook.yaml")

#: The arm every rewritten fixture declares its values under (9d-C1, R175).
ARM = "local_fixture_arm"


@pytest.fixture
def spec():
    return load_review_spec("review_specs/surgical_autonomy.yaml")


@pytest.fixture
def codebook():
    from engine.core.codebook import load_codebook_for

    return load_codebook_for("surgical_autonomy")


@pytest.fixture
def review(tmp_path):
    """A review directory: the real 20-field codebook beside an event-store
    database. The validator reads the grid (9d-C1, R175), so the fixture declares
    its values as field events — one claim per (paper, field) on `ARM` — rather
    than as `evidence_spans` rows no reader looks at."""
    shutil.copy2(LIVE_CODEBOOK, tmp_path / "extraction_codebook.yaml")
    return tmp_path


def _declare(review, spans, pid=1, arm=ARM):
    """Declare `spans` as `arm`'s claims on paper `pid` (eligible by the fixture)."""
    for s in spans:
        add_values(review / "review.db", arm, s["field_name"], [s["value"]], start_paper=pid)
    return pid


def _issues(review, arm=ARM):
    conn = sqlite3.connect(review / "review.db")
    try:
        return validate_all(conn, load_codebook(review / "extraction_codebook.yaml"), arm=arm)[0]
    finally:
        conn.close()


def test_valid_spans_no_issues(review):
    _declare(review, [
        {"field_name": "study_type", "value": "Original Research"},
        {"field_name": "autonomy_level", "value": "3 (Conditional autonomy)"},
        {"field_name": "sample_size", "value": "42"},
        {"field_name": "robot_platform", "value": "da Vinci"},
    ])
    assert _issues(review) == []


# test_unknown_field_name_flagged retired 2026-09-27 (9d-C1, R175): the grid's
# fields are the codebook's, so the check cannot fire; the writer's refusal of an
# unknown field name is Step 2 row B15.


def test_invalid_categorical_value_flagged(review):
    _declare(review, [
        {"field_name": "study_type", "value": "Orginal Research"},
    ])
    issues = _issues(review)
    assert len(issues) == 1
    assert "invalid categorical value" in issues[0]["issue"]
    assert "closest:" in issues[0]["issue"]
    assert "Original Research" in issues[0]["issue"]


def test_numeric_field_non_numeric_flagged(review):
    _declare(review, [
        {"field_name": "sample_size", "value": "twelve patients"},
    ])
    issues = _issues(review)
    assert len(issues) == 1
    assert "non-numeric sample_size" in issues[0]["issue"]


def test_not_found_value_accepted(review):
    _declare(review, [
        {"field_name": "study_type", "value": "NOT_FOUND"},
        {"field_name": "sample_size", "value": "NR"},
    ])
    assert _issues(review) == []


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


# The four in-place categorical normalisation tests retired 2026-09-25 with their
# subject (9c-C5, R160a; R47).


# ── element-wise semicolon validation tests ──────────────────────────


def test_semicolon_all_valid(review):
    """All semicolon-separated elements valid → no issues."""
    _declare(review, [
        {"field_name": "task_monitor", "value": "H; R; Shared"},
    ])
    assert _issues(review) == []


def test_semicolon_one_invalid(review):
    """One invalid element among valid ones — only the bad element reported."""
    _declare(review, [
        {"field_name": "task_monitor", "value": "H; Robotic; Shared"},
    ])
    issues = _issues(review)
    assert len(issues) == 1
    assert issues[0]["value"] == "Robotic"
    assert "invalid categorical value" in issues[0]["issue"]


def test_single_value_no_semicolons(review):
    """Single value without semicolons — unchanged validation behavior."""
    _declare(review, [
        {"field_name": "study_type", "value": "Orginal Research"},
    ])
    issues = _issues(review)
    assert len(issues) == 1
    assert issues[0]["value"] == "Orginal Research"
    assert "closest:" in issues[0]["issue"]
    assert "Original Research" in issues[0]["issue"]


def test_semicolon_all_invalid(review):
    """All elements invalid — each one reported separately."""
    _declare(review, [
        {"field_name": "task_monitor", "value": "Robotic; Autonomous"},
    ])
    issues = _issues(review)
    assert len(issues) == 2
    bad_values = {i["value"] for i in issues}
    assert bad_values == {"Robotic", "Autonomous"}


# ── cross-field bleed detection tests ────────────────────────────────


def test_bleed_no_bleed(codebook):
    """All values in their correct fields — no bleed detected."""
    data = [
        {"field_name": "study_type", "value": "Original Research"},
        {"field_name": "autonomy_level", "value": "3 (Conditional autonomy)"},
        {"field_name": "validation_setting", "value": "In vivo (human)"},
    ]
    bleeds = detect_cross_field_bleed(codebook, data)
    assert bleeds == []


def test_bleed_detected(codebook):
    """Value from validation_setting placed in study_type → bleed flagged."""
    # "Cadaver" is valid for validation_setting but not study_type
    data = [
        {"field_name": "study_type", "value": "Cadaver"},
    ]
    bleeds = detect_cross_field_bleed(codebook, data)
    assert len(bleeds) == 1
    assert bleeds[0]["field_name"] == "study_type"
    assert bleeds[0]["extracted_value"] == "Cadaver"
    assert bleeds[0]["belongs_to_field"] == "validation_setting"


def test_bleed_invalid_for_all_fields(codebook):
    """Value that doesn't match ANY field's vocabulary — not bleed, just wrong."""
    data = [
        {"field_name": "study_type", "value": "Quantum Teleportation"},
    ]
    bleeds = detect_cross_field_bleed(codebook, data)
    assert bleeds == []


def test_bleed_semicolon_multi_value(codebook):
    """One element of a semicolon-separated value bleeds."""
    # "Feasibility study" is valid for study_design but not study_type
    data = [
        {"field_name": "study_type", "value": "Original Research; Feasibility study"},
    ]
    bleeds = detect_cross_field_bleed(codebook, data)
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


def test_modified_codebook_different_hash(tmp_path, spec):
    """Adding a field changes the prompt hash — from the CODEBOOK now.

    This used to append an ExtractionField to spec.extraction_schema. The
    prompt never read the spec's field list for its CONTENT and, since
    SCHEMA-DERIVE-01, does not read it for the field SET either — so
    mutating the spec changed nothing and the test compared a hash with
    itself. The field set is the codebook's.
    """
    import yaml

    from engine.core.codebook import clear_cache

    live = (Path(__file__).resolve().parent.parent
            / "data" / "surgical_autonomy" / "extraction_codebook.yaml")
    h_a = verify_schema_parity(spec)

    # data_root_for resolves against the cwd, so mirror data/<review_id>
    review_dir = tmp_path / "data" / "surgical_autonomy"
    review_dir.mkdir(parents=True)
    doc = yaml.safe_load(live.read_text())
    doc["fields"].append({
        "name": "fake_new_field", "type": "free_text", "tier": 1,
        "definition": "A fake field for testing.", "instruction": "Extract it.",
        "field_class": "stated", "judge_rubric_family": "free_text",
    })
    (review_dir / "extraction_codebook.yaml").write_text(yaml.safe_dump(doc))

    clear_cache()
    try:
        import os
        cwd = os.getcwd()
        os.chdir(tmp_path)
        try:
            h_b = verify_schema_parity(spec)
        finally:
            os.chdir(cwd)
    finally:
        clear_cache()

    assert h_a != h_b


# ── the grid, the arm and the CLI (9d-C1, R174 as amended, R175) ─────


def test_empty_cell_is_skipped(review, codebook):
    """A cell with no claim for the arm (value None, state `missing`) is skipped,
    not flagged — and not dereferenced (`None.split` would raise)."""
    _declare(review, [{"field_name": "study_type", "value": "Original Research"}])
    assert validate_extraction(codebook, 1, [
        {"field_name": "study_type", "value": None},
        {"field_name": "sample_size", "value": None},
    ]) == []
    # Through the grid: 19 of the paper's 20 cells are empty and none is reported.
    assert _issues(review) == []


def test_only_the_named_arm_is_validated(review):
    """G4: two registered arms with claims under both; each arm's report carries
    only its own cells."""
    _declare(review, [{"field_name": "study_type", "value": "Orginal Research"}], arm="arm_a")
    _declare(review, [{"field_name": "task_monitor", "value": "Robotic"}], arm="arm_b")
    a, b = _issues(review, arm="arm_a"), _issues(review, arm="arm_b")
    assert [(i["field_name"], i["value"]) for i in a] == [("study_type", "Orginal Research")]
    assert [(i["field_name"], i["value"]) for i in b] == [("task_monitor", "Robotic")]


def _point_cli_at(monkeypatch, review, spec_arm):
    monkeypatch.setattr(V, "data_root_for", lambda review_id: review)
    monkeypatch.setattr(V, "load_spec_for", lambda review_id, override=None:
                        SimpleNamespace(extraction_models=SimpleNamespace(arm=spec_arm)))


def test_default_arm_unregistered_reports_zero_cells(review, monkeypatch, capsys):
    """The spec's extraction arm is used as named even when it is not registered
    yet (R174 as amended by 9d-C1): zero cells, the arm named, exit 0 — never a
    refusal. `iter_grid` itself raises `UnknownArm` for such an arm."""
    _declare(review, [{"field_name": "study_type", "value": "Orginal Research"}])
    conn = sqlite3.connect(review / "review.db")
    try:
        cb = load_codebook(review / "extraction_codebook.yaml")
        assert V.arm_cells(conn, cb, "not_yet_pinned") == {}
        assert validate_all(conn, cb, arm="not_yet_pinned") == ([], [])
    finally:
        conn.close()

    _point_cli_at(monkeypatch, review, "not_yet_pinned")
    assert V.main(["--review", "surgical_autonomy"]) == 0
    out = capsys.readouterr().out
    assert "not_yet_pinned" in out and "not a registered arm" in out
    assert "Populated cells: 0" in out


def test_arm_override_unregistered_is_refused(review, monkeypatch, capsys):
    """`--arm` must name a registered arm; the refusal lists the registered ones."""
    _declare(review, [{"field_name": "study_type", "value": "Original Research"}])
    _point_cli_at(monkeypatch, review, ARM)
    assert V.main(["--review", "surgical_autonomy", "--arm", "no_such_arm"]) == 2
    err = capsys.readouterr().err
    assert "no_such_arm" in err and ARM in err

    # A registered override is accepted and validated.
    assert V.main(["--review", "surgical_autonomy", "--arm", ARM]) == 0
    assert "Populated cells: 1" in capsys.readouterr().out


def test_cli_opens_read_only(review, monkeypatch):
    """The CLI's one connection is `open_read_only`'s `mode=ro` connection: a
    write through it is refused, and a CLI run leaves the database byte-identical."""
    import hashlib

    _declare(review, [{"field_name": "study_type", "value": "Orginal Research"}])
    db = review / "review.db"
    before = hashlib.sha256(db.read_bytes()).hexdigest()

    opened = []
    real = V.open_read_only

    def spy(path):
        conn = real(path)
        opened.append(conn)
        return conn

    monkeypatch.setattr(V, "open_read_only", spy)
    _point_cli_at(monkeypatch, review, ARM)
    assert V.main(["--review", "surgical_autonomy"]) == 0
    assert len(opened) == 1

    conn = real(db)
    try:
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            conn.execute("INSERT INTO papers (id) VALUES (999)")
    finally:
        conn.close()
    assert hashlib.sha256(db.read_bytes()).hexdigest() == before
