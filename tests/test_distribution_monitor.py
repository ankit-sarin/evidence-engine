"""Tests for engine.validators.distribution_monitor — distribution collapse detector."""

import math
import sqlite3
from pathlib import Path

import pytest

from tests._event_store_fixture import add_values, ensure_event_store
from engine.validators.distribution_monitor import (
    DEFAULT_COLLAPSED_MIN_PAPERS,
    DEFAULT_LOW_VARIANCE_MIN_PAPERS,
    DEFAULT_LOW_VARIANCE_THRESHOLD,
    DistributionCollapseError,
    assert_no_collapse,
    check_distribution,
    print_distribution_report,
    run_post_extraction_check,
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
    # The reader takes its field set from the codebook beside the database
    # (R26: the codebook is the sole source of field names), so the fixture
    # carries one — the review's own, not a hand-typed copy.
    import shutil
    shutil.copy(CODEBOOK_PATH, tmp_path / "extraction_codebook.yaml")
    ensure_event_store(db_path)
    return db_path


# B5 rewrite (READERS-01 Phase 2a, R30). These helpers declared values by
# INSERTing into `evidence_spans` / `cloud_evidence_spans` / `human_extractions`.
# `_query_values` no longer reads any of those — it reads `effective_value`
# through the arm registry, which is what closes A12 — so the declarations move
# to the event store. The tests below are unchanged: they pin distributions,
# and a distribution is still what they pin.

def _insert_local_spans(db_path: Path, field_name: str, values: list[str | None]) -> None:
    add_values(db_path, "local", field_name, values)


def _insert_cloud_spans(db_path: Path, arm: str, field_name: str,
                        values: list[str | None]) -> None:
    add_values(db_path, arm, field_name, values)


def _insert_human_spans(db_path: Path, extractor_id: str, field_name: str,
                        values: list[tuple[str, str | None]]) -> None:
    """R13: a `human_extractor` arm is assigned nothing until session 12, so its
    cells read OUT OF SCOPE. The rows are declared anyway, because the point of
    `TestArmRouting::test_human_arm` is that the arm RESOLVES — before Phase 2a
    it fell into the cloud branch and returned `{}` with no error (A12)."""
    def _pid(raw: str) -> int:
        # `human_extractions.paper_id` is TEXT "EE-NNN"; R14 reconciles it to
        # `papers.id` in session 12. Until then the fixture does the obvious
        # thing and records why it is obvious.
        return int(str(raw).split("-")[-1])

    arm = f"human_{extractor_id}"
    ordered = sorted(values, key=lambda pv: _pid(pv[0]))
    add_values(db_path, arm, field_name, [v for _pid_, v in ordered],
               arm_kind="human_extractor", start_paper=_pid(ordered[0][0]))


# ── Tests: _is_null ──────────────────────────────────────────────────


class TestIsNull:
    """R133: absence is the codebook's `absence_sentinels`; "", "n/r" and "none"
    are the monitor's named malformed-output forms. Rewritten under B5 when
    `_NULL_SYNONYMS` was split (every call now passes the codebook's set)."""

    @pytest.fixture(autouse=True)
    def _sentinels(self):
        from engine.core.codebook import load_codebook
        self.absence = load_codebook(CODEBOOK_PATH).absence_sentinel_set

    def null(self, value):
        return _is_null(value, absence_sentinels=self.absence)

    def test_none_is_null(self):
        assert self.null(None) is True

    @pytest.mark.parametrize("form", ["", "  ", "n/r", "N/R", "none", "None"])
    def test_malformed_output_forms_are_null(self, form):
        assert self.null(form) is True

    @pytest.mark.parametrize("sentinel", ["NR", "N/A", "NA", "NOT_FOUND",
                                          "NOT FOUND", "NOT REPORTED"])
    def test_every_codebook_sentinel_is_null_in_any_case(self, sentinel):
        assert self.null(sentinel) is True
        assert self.null(f" {sentinel.lower()} ") is True

    def test_real_value_not_null(self):
        assert self.null("Original Research") is False

    def test_absence_comes_from_the_argument_not_a_module_list(self):
        # A form no codebook declares and the monitor does not normalise is a value.
        assert _is_null("NR", absence_sentinels=frozenset()) is False
        assert _is_null("Not discussed", absence_sentinels=self.absence) is False


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

    def test_an_arm_with_no_values_is_all_zero_and_OK(self, tmp_path):
        """B5 rewrite (A12). This was `test_no_extractions_empty` and it called
        `check_distribution` for `"local"` on a database where no arm had been
        registered, expecting zeros. That is exactly the conflation A12 names: a
        name nobody declared and an arm that happens to hold nothing gave the
        same answer, so a typo in an arm name read as a clean result.

        The zeros are still the contract — for a REGISTERED arm that holds
        nothing. The other half is the test below."""
        db_path = _make_db(tmp_path)
        add_values(db_path, "local", "study_type", [])   # registered, no claims
        results = check_distribution(db_path, "test", "local", CODEBOOK_PATH)
        assert len(results) > 0
        for r in results:
            assert r["total_non_null"] == 0
            assert r["status"] == "OK"

    def test_an_unregistered_arm_raises_rather_than_reading_as_empty(self, tmp_path):
        """The other half of A12, and the reason the rewrite above was needed."""
        from engine.core.effective import UnknownArm

        db_path = _make_db(tmp_path)
        add_values(db_path, "local", "study_type", ["RCT"])
        with pytest.raises(UnknownArm):
            check_distribution(db_path, "test", "locl", CODEBOOK_PATH)

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

    def test_human_arm_resolves_and_reads_out_of_scope_under_R13(self, tmp_path):
        """B5 rewrite (A12, R13).

        This pinned "a human arm queries `human_extractions`", asserting 15
        values. Two things were wrong with the world it pinned, and the ruling
        changed both:

        * `concordance.load_arm` had no such branch, so the two files disagreed
          about where a `human_*` arm's values live — and `human_extractions`
          does not exist on this review's database at all, so the branch was
          unreachable, not merely divergent (A12).
        * R13 says a `human_extractor` arm is assigned nothing until the
          assignment table arrives with human arm loading in session 12, so its
          cells are OUT OF SCOPE — excluded from every denominator, which is why
          the count is 0 and not 15.

        What is asserted now is what A12 was opened for: the arm RESOLVES, the
        answer is a defined one, and no error and no silent `{}` is produced."""
        from engine.core.effective import effective_value

        db_path = _make_db(tmp_path)
        papers = [(f"EE-{i:03d}", "Original Research") for i in range(1, 16)]
        _insert_human_spans(db_path, "A", "study_type", papers)

        results = check_distribution(db_path, "test", "human_A", CODEBOOK_PATH)
        st = [r for r in results if r["field_name"] == "study_type"]
        assert st[0]["total_non_null"] == 0

        conn = sqlite3.connect(str(db_path))
        try:
            ev = effective_value(conn, 1, "study_type", "human_A",
                                 sentinels=frozenset())
            assert (ev.rule_row, ev.state) == (0, "out of scope")
        finally:
            conn.close()

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


# ── Tests: run_post_extraction_check ──────────────────────────────────


class TestRunPostExtractionCheck:

    def test_called_on_completion(self, tmp_path):
        """Monitor runs and returns results when extraction completes."""
        db_path = _make_db(tmp_path)
        values = (
            ["Original Research"] * 8
            + ["Case Report/Series"] * 7
            + ["Review"] * 5
        )
        _insert_local_spans(db_path, "study_type", values)
        summary = run_post_extraction_check(
            db_path=db_path,
            review_name="test",
            arm="local",
            codebook_path=CODEBOOK_PATH,
            extracted_count=20,
            failed_count=0,
        )
        assert summary["skipped"] is False
        assert summary["ok"] >= 1
        assert isinstance(summary["collapsed"], int)
        assert isinstance(summary["low_variance"], int)

    def test_skipped_on_partial_run_too_few(self, tmp_path):
        """Monitor skipped when fewer than 10 papers extracted."""
        db_path = _make_db(tmp_path)
        _insert_local_spans(db_path, "study_type", ["Original Research"] * 5)
        summary = run_post_extraction_check(
            db_path=db_path,
            review_name="test",
            arm="local",
            codebook_path=CODEBOOK_PATH,
            extracted_count=5,
            failed_count=0,
        )
        assert summary["skipped"] is True

    def test_skipped_on_partial_run_failures(self, tmp_path):
        """Monitor skipped when there are failed papers."""
        db_path = _make_db(tmp_path)
        _insert_local_spans(db_path, "study_type", ["Original Research"] * 20)
        summary = run_post_extraction_check(
            db_path=db_path,
            review_name="test",
            arm="local",
            codebook_path=CODEBOOK_PATH,
            extracted_count=20,
            failed_count=3,
        )
        assert summary["skipped"] is True

    def test_collapsed_raises_distribution_collapse_error(self, tmp_path, caplog):
        """COLLAPSED field raises DistributionCollapseError after logging ERROR."""
        db_path = _make_db(tmp_path)
        _insert_local_spans(db_path, "study_type", ["Original Research"] * 15)
        import logging
        with caplog.at_level(logging.ERROR):
            with pytest.raises(DistributionCollapseError, match="study_type"):
                run_post_extraction_check(
                    db_path=db_path,
                    review_name="test",
                    arm="local",
                    codebook_path=CODEBOOK_PATH,
                    extracted_count=15,
                    failed_count=0,
                )
        # ERROR log was emitted before the raise
        error_messages = [r.message for r in caplog.records if r.levelno >= logging.ERROR]
        assert any("COLLAPSED" in m for m in error_messages)

    def test_low_variance_does_not_raise_by_default(self, tmp_path):
        """LOW_VARIANCE field does NOT raise without strict mode."""
        db_path = _make_db(tmp_path)
        values = ["Original Research"] * 18 + ["Case Report/Series"] * 2
        _insert_local_spans(db_path, "study_type", values)
        summary = run_post_extraction_check(
            db_path=db_path,
            review_name="test",
            arm="local",
            codebook_path=CODEBOOK_PATH,
            extracted_count=20,
            failed_count=0,
        )
        assert summary["low_variance"] >= 1
        assert summary["skipped"] is False

    def test_strict_mode_raises_on_low_variance(self, tmp_path):
        """strict=True causes LOW_VARIANCE to raise DistributionCollapseError."""
        db_path = _make_db(tmp_path)
        values = ["Original Research"] * 18 + ["Case Report/Series"] * 2
        _insert_local_spans(db_path, "study_type", values)
        with pytest.raises(DistributionCollapseError):
            run_post_extraction_check(
                db_path=db_path,
                review_name="test",
                arm="local",
                codebook_path=CODEBOOK_PATH,
                extracted_count=20,
                failed_count=0,
                strict=True,
            )

    def test_results_in_summary(self, tmp_path):
        """Summary includes counts for OK, LOW_VARIANCE — healthy data returns normally."""
        db_path = _make_db(tmp_path)
        # Healthy distribution — no COLLAPSED fields
        values = (
            ["Original Research"] * 8
            + ["Case Report/Series"] * 7
            + ["Review"] * 5
            + ["Systematic Review"] * 5
        )
        _insert_local_spans(db_path, "study_type", values)
        summary = run_post_extraction_check(
            db_path=db_path,
            review_name="test",
            arm="local",
            codebook_path=CODEBOOK_PATH,
            extracted_count=25,
            failed_count=0,
        )
        total = summary["ok"] + summary["low_variance"] + summary["collapsed"]
        assert total > 0
        assert summary["skipped"] is False
        assert isinstance(summary["collapsed_fields"], list)
        assert isinstance(summary["low_variance_fields"], list)


class TestRunPostExtractionCheckLocalFlags:
    """B9 (R167, 9c-C7): the local path's flags. The cloud defaults
    (raise_on_collapse=True, skip_on_failures=True, min_population="run") are the
    seven tests above, unchanged."""

    def test_t3_raise_on_collapse_true_still_raises_the_cloud_contract(self, tmp_path):
        db_path = _make_db(tmp_path)
        _insert_local_spans(db_path, "study_type", ["Original Research"] * 12)
        with pytest.raises(DistributionCollapseError):
            run_post_extraction_check(
                db_path=db_path, review_name="test", arm="local",
                codebook_path=CODEBOOK_PATH, extracted_count=12, failed_count=0,
                raise_on_collapse=True)

    def test_t3_raise_on_collapse_false_returns_the_collapse(self, tmp_path):
        db_path = _make_db(tmp_path)
        _insert_local_spans(db_path, "study_type", ["Original Research"] * 12)
        summary = run_post_extraction_check(
            db_path=db_path, review_name="test", arm="local",
            codebook_path=CODEBOOK_PATH, extracted_count=12, failed_count=0,
            raise_on_collapse=False)
        assert summary["skipped"] is False and "study_type" in summary["collapsed_fields"]
        assert any(r["field_name"] == "study_type" and r["status"] == "COLLAPSED"
                   for r in summary["results"])

    def test_t4_arm_population_runs_despite_failures_and_a_small_run(self, tmp_path):
        db_path = _make_db(tmp_path)
        values = ["Original Research"] * 8 + ["Case Report/Series"] * 7 + ["Review"] * 5
        _insert_local_spans(db_path, "study_type", values)
        summary = run_post_extraction_check(
            db_path=db_path, review_name="test", arm="local",
            codebook_path=CODEBOOK_PATH, extracted_count=3, failed_count=2,
            raise_on_collapse=False, skip_on_failures=False, min_population="arm")
        assert summary["skipped"] is False
        assert summary["arm_population"] == 20
        assert (summary["extracted_count"], summary["failed_count"]) == (3, 2)
        assert summary["ok"] >= 1

    def test_t4_the_run_population_rule_skips_the_same_inputs(self, tmp_path):
        db_path = _make_db(tmp_path)
        values = ["Original Research"] * 8 + ["Case Report/Series"] * 7 + ["Review"] * 5
        _insert_local_spans(db_path, "study_type", values)
        summary = run_post_extraction_check(
            db_path=db_path, review_name="test", arm="local",
            codebook_path=CODEBOOK_PATH, extracted_count=3, failed_count=2,
            raise_on_collapse=False, skip_on_failures=False, min_population="run")
        assert summary["skipped"] is True
        assert "only 3 papers extracted" in summary["skip_reason"]

    def test_t4_skip_on_failures_still_vetoes_under_the_arm_rule(self, tmp_path):
        db_path = _make_db(tmp_path)
        values = ["Original Research"] * 8 + ["Case Report/Series"] * 7 + ["Review"] * 5
        _insert_local_spans(db_path, "study_type", values)
        summary = run_post_extraction_check(
            db_path=db_path, review_name="test", arm="local",
            codebook_path=CODEBOOK_PATH, extracted_count=3, failed_count=2,
            raise_on_collapse=False, skip_on_failures=True, min_population="arm")
        assert summary["skipped"] is True
        assert "failed extraction" in summary["skip_reason"]


# ── Tests: L1 — configurable thresholds ─────────────────────────────


class TestConfigurableThresholds:

    def test_defaults_match_named_constants(self):
        """Named constants match the original magic numbers."""
        assert DEFAULT_COLLAPSED_MIN_PAPERS == 10
        assert DEFAULT_LOW_VARIANCE_THRESHOLD == 0.85
        assert DEFAULT_LOW_VARIANCE_MIN_PAPERS == 20

    def test_custom_low_variance_threshold_triggers(self, tmp_path):
        """A stricter threshold flags a distribution that would be OK under defaults."""
        db_path = _make_db(tmp_path)
        # 15/20 same value = 75% — OK under default 85%, LOW_VARIANCE under 70%
        values = ["Original Research"] * 15 + ["Case Report/Series"] * 5
        _insert_local_spans(db_path, "study_type", values)

        # Default: OK
        results_default = check_distribution(db_path, "test", "local", CODEBOOK_PATH)
        st_default = [r for r in results_default if r["field_name"] == "study_type"]
        assert st_default[0]["status"] == "OK"

        # Stricter threshold: LOW_VARIANCE
        results_strict = check_distribution(
            db_path, "test", "local", CODEBOOK_PATH,
            low_variance_threshold=0.70,
        )
        st_strict = [r for r in results_strict if r["field_name"] == "study_type"]
        assert st_strict[0]["status"] == "LOW_VARIANCE"

    def test_custom_collapsed_min_papers(self, tmp_path):
        """Lower collapsed_min_papers catches smaller collapses."""
        db_path = _make_db(tmp_path)
        # 5 papers all same — OK under default min 10, COLLAPSED under min 3
        _insert_local_spans(db_path, "study_type", ["Original Research"] * 5)

        results_default = check_distribution(db_path, "test", "local", CODEBOOK_PATH)
        st = [r for r in results_default if r["field_name"] == "study_type"]
        assert st[0]["status"] == "OK"

        results_custom = check_distribution(
            db_path, "test", "local", CODEBOOK_PATH,
            collapsed_min_papers=3,
        )
        st = [r for r in results_custom if r["field_name"] == "study_type"]
        assert st[0]["status"] == "COLLAPSED"

    def test_custom_low_variance_min_papers(self, tmp_path):
        """Lower low_variance_min_papers catches smaller datasets."""
        db_path = _make_db(tmp_path)
        # 9/10 same value = 90% — OK under default min 20 papers, LOW_VARIANCE under min 5
        values = ["Original Research"] * 9 + ["Case Report/Series"] * 1
        _insert_local_spans(db_path, "study_type", values)

        results_default = check_distribution(db_path, "test", "local", CODEBOOK_PATH)
        st = [r for r in results_default if r["field_name"] == "study_type"]
        assert st[0]["status"] == "OK"

        results_custom = check_distribution(
            db_path, "test", "local", CODEBOOK_PATH,
            low_variance_min_papers=5,
        )
        st = [r for r in results_custom if r["field_name"] == "study_type"]
        assert st[0]["status"] == "LOW_VARIANCE"
