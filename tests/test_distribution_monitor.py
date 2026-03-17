"""Tests for engine.validators.distribution_monitor — distribution collapse detector."""

import math
import sqlite3
from pathlib import Path

import pytest

from engine.validators.distribution_monitor import (
    DistributionCollapseError,
    assert_no_collapse,
    check_distribution,
    print_distribution_report,
    shannon_entropy,
    _is_null,
    _load_categorical_fields,
)


# ── Helpers ──────────────────────────────────────────────────────────


CODEBOOK_PATH = Path("data/surgical_autonomy/extraction_codebook.yaml")


def _make_db(tmp_path: Path) -> Path:
    """Create DB with extraction tables for local, cloud, and human arms."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.executescript("""
        CREATE TABLE extractions (
            id INTEGER PRIMARY KEY,
            paper_id INTEGER NOT NULL,
            extraction_schema_hash TEXT NOT NULL DEFAULT 'test',
            extracted_data TEXT NOT NULL DEFAULT '{}',
            extracted_at TEXT NOT NULL DEFAULT '2026-01-01'
        );
        CREATE TABLE evidence_spans (
            id INTEGER PRIMARY KEY,
            extraction_id INTEGER NOT NULL REFERENCES extractions(id),
            field_name TEXT NOT NULL,
            value TEXT NOT NULL,
            source_snippet TEXT,
            confidence REAL,
            audit_status TEXT NOT NULL DEFAULT 'pending'
        );
        CREATE TABLE cloud_extractions (
            id INTEGER PRIMARY KEY,
            paper_id INTEGER NOT NULL,
            arm TEXT NOT NULL,
            model_string TEXT NOT NULL DEFAULT 'test',
            extracted_at TEXT NOT NULL DEFAULT '2026-01-01'
        );
        CREATE TABLE cloud_evidence_spans (
            id INTEGER PRIMARY KEY,
            cloud_extraction_id INTEGER NOT NULL REFERENCES cloud_extractions(id),
            field_name TEXT NOT NULL,
            value TEXT
        );
        CREATE TABLE human_extractions (
            id INTEGER PRIMARY KEY,
            paper_id TEXT NOT NULL,
            extractor_id TEXT NOT NULL,
            field_name TEXT NOT NULL,
            value TEXT,
            source_quote TEXT,
            notes TEXT,
            imported_at TEXT NOT NULL DEFAULT '2026-01-01',
            UNIQUE(paper_id, extractor_id, field_name)
        );
    """)
    conn.commit()
    conn.close()
    return db_path


def _insert_local_spans(db_path: Path, field_name: str, values: list[str | None]) -> None:
    """Insert local extraction spans for a field with given values."""
    conn = sqlite3.connect(str(db_path))
    for i, val in enumerate(values):
        # Create a unique extraction per paper
        ext_id = conn.execute(
            "INSERT INTO extractions (paper_id, extracted_data, extracted_at) "
            "VALUES (?, '{}', '2026-01-01')",
            (i + 1,),
        ).lastrowid
        if val is not None:
            conn.execute(
                "INSERT INTO evidence_spans (extraction_id, field_name, value, confidence) "
                "VALUES (?, ?, ?, 0.9)",
                (ext_id, field_name, val),
            )
    conn.commit()
    conn.close()


def _insert_cloud_spans(db_path: Path, arm: str, field_name: str, values: list[str | None]) -> None:
    """Insert cloud extraction spans for a field with given values."""
    conn = sqlite3.connect(str(db_path))
    for i, val in enumerate(values):
        ce_id = conn.execute(
            "INSERT INTO cloud_extractions (paper_id, arm, model_string, extracted_at) "
            "VALUES (?, ?, 'test-model', '2026-01-01')",
            (i + 1, arm),
        ).lastrowid
        if val is not None:
            conn.execute(
                "INSERT INTO cloud_evidence_spans (cloud_extraction_id, field_name, value) "
                "VALUES (?, ?, ?)",
                (ce_id, field_name, val),
            )
    conn.commit()
    conn.close()


def _insert_human_spans(db_path: Path, extractor_id: str, field_name: str,
                         values: list[tuple[str, str | None]]) -> None:
    """Insert human extraction rows: values is [(paper_id, value), ...]."""
    conn = sqlite3.connect(str(db_path))
    for paper_id, val in values:
        conn.execute(
            "INSERT INTO human_extractions "
            "(paper_id, extractor_id, field_name, value, imported_at) "
            "VALUES (?, ?, ?, ?, '2026-01-01')",
            (paper_id, extractor_id, field_name, val),
        )
    conn.commit()
    conn.close()


# ── Tests: _is_null ──────────────────────────────────────────────────


class TestIsNull:

    def test_none_is_null(self):
        assert _is_null(None) is True

    def test_empty_is_null(self):
        assert _is_null("") is True

    def test_nr_is_null(self):
        assert _is_null("NR") is True
        assert _is_null("nr") is True
        assert _is_null(" NR ") is True

    def test_not_reported_is_null(self):
        assert _is_null("Not Reported") is True

    def test_real_value_not_null(self):
        assert _is_null("Original Research") is False

    def test_n_a_is_null(self):
        assert _is_null("N/A") is True


# ── Tests: _load_categorical_fields ──────────────────────────────────


class TestLoadCategoricalFields:

    def test_loads_from_real_codebook(self):
        fields = _load_categorical_fields(CODEBOOK_PATH)
        assert "study_type" in fields
        assert "autonomy_level" in fields
        assert "clinical_readiness_assessment" in fields
        # Free-text fields excluded
        assert "robot_platform" not in fields
        assert "task_performed" not in fields


# ── Tests: shannon_entropy ───────────────────────────────────────────


class TestShannonEntropy:

    def test_empty_list(self):
        assert shannon_entropy([]) == 0.0

    def test_single_value(self):
        assert shannon_entropy(["A"] * 10) == 0.0

    def test_two_equal_values(self):
        h = shannon_entropy(["A"] * 5 + ["B"] * 5)
        assert abs(h - 1.0) < 1e-9  # log2(2) = 1.0

    def test_four_equal_values(self):
        h = shannon_entropy(["A"] * 5 + ["B"] * 5 + ["C"] * 5 + ["D"] * 5)
        assert abs(h - 2.0) < 1e-9  # log2(4) = 2.0

    def test_skewed_distribution(self):
        # 90% A, 10% B — entropy should be < 1.0
        h = shannon_entropy(["A"] * 9 + ["B"] * 1)
        assert 0 < h < 1.0


# ── Tests: check_distribution — COLLAPSED ────────────────────────────


class TestCheckCollapsed:

    def test_all_same_value_collapsed(self, tmp_path):
        """15 papers all with same value → COLLAPSED."""
        db_path = _make_db(tmp_path)
        _insert_local_spans(db_path, "study_type", ["Original Research"] * 15)
        results = check_distribution(db_path, "test", "local", CODEBOOK_PATH)
        st = [r for r in results if r["field_name"] == "study_type"]
        assert len(st) == 1
        assert st[0]["status"] == "COLLAPSED"
        assert st[0]["distinct_count"] == 1
        assert st[0]["top_value_pct"] == 1.0
        assert st[0]["entropy"] == 0.0

    def test_all_same_below_threshold_not_collapsed(self, tmp_path):
        """Only 5 papers all same value — below threshold, not COLLAPSED."""
        db_path = _make_db(tmp_path)
        _insert_local_spans(db_path, "study_type", ["Original Research"] * 5)
        results = check_distribution(db_path, "test", "local", CODEBOOK_PATH)
        st = [r for r in results if r["field_name"] == "study_type"]
        assert st[0]["status"] == "OK"

    def test_collapsed_entropy_zero(self, tmp_path):
        db_path = _make_db(tmp_path)
        _insert_local_spans(db_path, "study_type", ["Original Research"] * 20)
        results = check_distribution(db_path, "test", "local", CODEBOOK_PATH)
        st = [r for r in results if r["field_name"] == "study_type"]
        assert st[0]["entropy"] == 0.0


# ── Tests: check_distribution — LOW_VARIANCE ─────────────────────────


class TestCheckLowVariance:

    def test_85pct_dominant_low_variance(self, tmp_path):
        """18/20 same value (90%) → LOW_VARIANCE."""
        db_path = _make_db(tmp_path)
        values = ["Original Research"] * 18 + ["Case Report/Series"] * 2
        _insert_local_spans(db_path, "study_type", values)
        results = check_distribution(db_path, "test", "local", CODEBOOK_PATH)
        st = [r for r in results if r["field_name"] == "study_type"]
        assert st[0]["status"] == "LOW_VARIANCE"
        assert st[0]["distinct_count"] == 2

    def test_84pct_not_low_variance(self, tmp_path):
        """16/20 same value (80%) → OK (below 85% threshold)."""
        db_path = _make_db(tmp_path)
        values = ["Original Research"] * 16 + ["Case Report/Series"] * 4
        _insert_local_spans(db_path, "study_type", values)
        results = check_distribution(db_path, "test", "local", CODEBOOK_PATH)
        st = [r for r in results if r["field_name"] == "study_type"]
        assert st[0]["status"] == "OK"

    def test_low_variance_below_count_threshold(self, tmp_path):
        """90% dominant but only 10 papers → OK (below 20 threshold)."""
        db_path = _make_db(tmp_path)
        values = ["Original Research"] * 9 + ["Case Report/Series"] * 1
        _insert_local_spans(db_path, "study_type", values)
        results = check_distribution(db_path, "test", "local", CODEBOOK_PATH)
        st = [r for r in results if r["field_name"] == "study_type"]
        assert st[0]["status"] == "OK"


# ── Tests: check_distribution — OK ───────────────────────────────────


class TestCheckOK:

    def test_healthy_spread(self, tmp_path):
        """Even distribution across 4 values → OK."""
        db_path = _make_db(tmp_path)
        values = (
            ["Original Research"] * 8
            + ["Case Report/Series"] * 7
            + ["Review"] * 5
            + ["Systematic Review"] * 5
        )
        _insert_local_spans(db_path, "study_type", values)
        results = check_distribution(db_path, "test", "local", CODEBOOK_PATH)
        st = [r for r in results if r["field_name"] == "study_type"]
        assert st[0]["status"] == "OK"
        assert st[0]["distinct_count"] == 4
        assert st[0]["entropy"] > 1.0  # healthy entropy


# ── Tests: check_distribution — edge cases ───────────────────────────


class TestCheckEdgeCases:

    def test_all_nr_excluded(self, tmp_path):
        """All NR values → total_non_null = 0, OK."""
        db_path = _make_db(tmp_path)
        _insert_local_spans(db_path, "study_type", ["NR"] * 15)
        results = check_distribution(db_path, "test", "local", CODEBOOK_PATH)
        st = [r for r in results if r["field_name"] == "study_type"]
        assert st[0]["total_non_null"] == 0
        assert st[0]["status"] == "OK"

    def test_mixed_null_and_values(self, tmp_path):
        """Nulls excluded, remaining values analyzed."""
        db_path = _make_db(tmp_path)
        values = ["Original Research"] * 12 + ["NR"] * 5 + [None] * 3
        _insert_local_spans(db_path, "study_type", values)
        results = check_distribution(db_path, "test", "local", CODEBOOK_PATH)
        st = [r for r in results if r["field_name"] == "study_type"]
        assert st[0]["total_non_null"] == 12
        assert st[0]["status"] == "COLLAPSED"  # 12 same, >= 10

    def test_no_extractions_empty(self, tmp_path):
        """Empty DB → all fields get 0 non-null, OK."""
        db_path = _make_db(tmp_path)
        results = check_distribution(db_path, "test", "local", CODEBOOK_PATH)
        assert len(results) > 0
        for r in results:
            assert r["total_non_null"] == 0
            assert r["status"] == "OK"

    def test_only_categorical_fields_checked(self, tmp_path):
        """Free-text fields are not included in results."""
        db_path = _make_db(tmp_path)
        # Insert spans for a free-text field
        _insert_local_spans(db_path, "robot_platform", ["da Vinci Xi"] * 15)
        results = check_distribution(db_path, "test", "local", CODEBOOK_PATH)
        field_names = [r["field_name"] for r in results]
        assert "robot_platform" not in field_names


# ── Tests: check_distribution — arm routing ──────────────────────────


class TestArmRouting:

    def test_cloud_arm(self, tmp_path):
        """Cloud arm queries cloud_evidence_spans."""
        db_path = _make_db(tmp_path)
        _insert_cloud_spans(db_path, "openai_o4_mini_high", "study_type",
                            ["Original Research"] * 15)
        results = check_distribution(db_path, "test", "openai_o4_mini_high", CODEBOOK_PATH)
        st = [r for r in results if r["field_name"] == "study_type"]
        assert st[0]["total_non_null"] == 15
        assert st[0]["status"] == "COLLAPSED"

    def test_human_arm(self, tmp_path):
        """Human arm queries human_extractions for specific extractor."""
        db_path = _make_db(tmp_path)
        papers = [(f"EE-{i:03d}", "Original Research") for i in range(1, 16)]
        _insert_human_spans(db_path, "A", "study_type", papers)
        results = check_distribution(db_path, "test", "human_A", CODEBOOK_PATH)
        st = [r for r in results if r["field_name"] == "study_type"]
        assert st[0]["total_non_null"] == 15

    def test_cloud_arm_filters_by_arm_name(self, tmp_path):
        """Only values from the specified cloud arm are included."""
        db_path = _make_db(tmp_path)
        _insert_cloud_spans(db_path, "openai_o4_mini_high", "study_type",
                            ["Original Research"] * 10)
        _insert_cloud_spans(db_path, "anthropic_sonnet_4_6", "study_type",
                            ["Case Report/Series"] * 10)
        results = check_distribution(db_path, "test", "openai_o4_mini_high", CODEBOOK_PATH)
        st = [r for r in results if r["field_name"] == "study_type"]
        assert st[0]["total_non_null"] == 10
        assert st[0]["top_value"] == "Original Research"


# ── Tests: assert_no_collapse ─────────────────────────────────────────


class TestAssertNoCollapse:

    def test_no_issues_passes(self):
        results = [
            {"field_name": "study_type", "status": "OK", "top_value": "X",
             "top_value_pct": 0.5, "total_non_null": 20},
        ]
        assert_no_collapse(results)  # should not raise

    def test_collapsed_raises(self):
        results = [
            {"field_name": "study_type", "status": "COLLAPSED", "top_value": "X",
             "top_value_pct": 1.0, "total_non_null": 20},
        ]
        with pytest.raises(DistributionCollapseError, match="study_type"):
            assert_no_collapse(results)

    def test_low_variance_warns_but_passes(self):
        results = [
            {"field_name": "study_type", "status": "LOW_VARIANCE", "top_value": "X",
             "top_value_pct": 0.9, "total_non_null": 20},
        ]
        assert_no_collapse(results)  # should not raise

    def test_strict_mode_fails_on_low_variance(self):
        results = [
            {"field_name": "study_type", "status": "LOW_VARIANCE", "top_value": "X",
             "top_value_pct": 0.9, "total_non_null": 20},
        ]
        with pytest.raises(DistributionCollapseError):
            assert_no_collapse(results, strict=True)

    def test_error_contains_collapsed_fields(self):
        results = [
            {"field_name": "study_type", "status": "COLLAPSED", "top_value": "X",
             "top_value_pct": 1.0, "total_non_null": 20},
            {"field_name": "autonomy_level", "status": "COLLAPSED", "top_value": "Y",
             "top_value_pct": 1.0, "total_non_null": 20},
        ]
        with pytest.raises(DistributionCollapseError) as exc_info:
            assert_no_collapse(results)
        assert len(exc_info.value.collapsed_fields) == 2


# ── Tests: print_distribution_report ─────────────────────────────────


class TestPrintReport:

    def test_prints_without_error(self, capsys):
        results = [
            {"field_name": "study_type", "arm": "local", "total_non_null": 20,
             "distinct_count": 4, "top_value": "Original Research",
             "top_value_pct": 0.4, "entropy": 1.8, "status": "OK",
             "distribution": {"Original Research": 8, "Review": 5,
                              "Case Report/Series": 4, "Other": 3}},
            {"field_name": "autonomy_level", "arm": "local", "total_non_null": 20,
             "distinct_count": 1, "top_value": "2 (Task autonomy)",
             "top_value_pct": 1.0, "entropy": 0.0, "status": "COLLAPSED",
             "distribution": {"2 (Task autonomy)": 20}},
        ]
        print_distribution_report(results)
        captured = capsys.readouterr()
        assert "study_type" in captured.out
        assert "autonomy_level" in captured.out
        assert "COLLAPSED" in captured.out

    def test_empty_results(self, capsys):
        print_distribution_report([])
        captured = capsys.readouterr()
        assert "0 OK" in captured.out
