"""Tests for concordance normalization and scoring.

Test cases derived from the Run 5 format audit (Item 1 findings).
"""

import pytest

from engine.analysis.normalize import normalize_for_concordance
from engine.analysis.scoring import FieldScore, score_pair


# ── Normalization tests ──────────────────────────────────────────────


class TestNullHandling:
    """NR / null synonyms all normalize to None."""

    @pytest.mark.parametrize("raw", [None, "", "NR", "N/R", "Not reported", "NOT_FOUND", "none", "N/A"])
    def test_null_synonyms(self, raw):
        assert normalize_for_concordance("autonomy_level", raw) is None

    def test_whitespace_only_is_null(self):
        assert normalize_for_concordance("autonomy_level", "   ") is None


class TestAutonomyLevelPrefix:
    """Bare integers normalize to full parenthetical form."""

    def test_bare_2(self):
        assert normalize_for_concordance("autonomy_level", "2") == "2 (Task autonomy)"

    def test_bare_3(self):
        assert normalize_for_concordance("autonomy_level", "3") == "3 (Conditional autonomy)"

    def test_bare_4(self):
        assert normalize_for_concordance("autonomy_level", "4") == "4 (High autonomy)"

    def test_full_value_unchanged(self):
        assert normalize_for_concordance("autonomy_level", "2 (Task autonomy)") == "2 (Task autonomy)"

    def test_mixed_multiple_unchanged(self):
        assert normalize_for_concordance("autonomy_level", "Mixed/Multiple") == "Mixed/Multiple"


class TestSystemMaturityPrefix:
    """Truncated prefix normalizes to full value."""

    def test_research_prototype(self):
        assert normalize_for_concordance("system_maturity", "Research prototype") == "Research prototype (hardware)"

    def test_full_value_unchanged(self):
        result = normalize_for_concordance("system_maturity", "Algorithm on existing platform")
        assert result == "Algorithm on existing platform"


class TestValidationSettingMultiValue:
    """Semicolon multi-values split into normalized sets."""

    def test_single_value_returns_string(self):
        result = normalize_for_concordance("validation_setting", "Phantom/Simulation")
        assert result == "Phantom/Simulation"

    def test_multi_value_returns_set(self):
        result = normalize_for_concordance("validation_setting", "In vivo (animal); Phantom/Simulation")
        assert result == {"In vivo (animal)", "Phantom/Simulation"}

    def test_cross_field_bleed_preserved(self):
        """Values from other fields' vocabularies are returned as-is."""
        result = normalize_for_concordance("validation_setting", "Simulation / computational only")
        # Not in validation_setting enum — preserved as-is, no normalization
        assert result == "Simulation / computational only"

    def test_multi_value_with_bleed_in_set(self):
        result = normalize_for_concordance(
            "validation_setting", "Simulation / computational only; Phantom/Simulation"
        )
        assert isinstance(result, set)
        assert "Phantom/Simulation" in result


class TestSurgicalDomainMultiValue:
    """surgical_domain also allows semicolons."""

    def test_multi_value(self):
        result = normalize_for_concordance(
            "surgical_domain", "Non-clinical Bench / Phantom; Computational / Simulation Only"
        )
        assert isinstance(result, set)
        assert "Non-clinical Bench / Phantom" in result
        assert "Computational / Simulation Only" in result


class TestNumericFields:
    def test_sample_size_strips_nonnumeric(self):
        assert normalize_for_concordance("sample_size", "n=42") == "42"

    def test_sample_size_pure_integer(self):
        assert normalize_for_concordance("sample_size", "10") == "10"

    def test_sample_size_with_spaces(self):
        assert normalize_for_concordance("sample_size", " 100 patients ") == "100"

    def test_primary_outcome_value_passthrough(self):
        result = normalize_for_concordance("primary_outcome_value", "  0.95 mm  ")
        assert result == "0.95 mm"


class TestFreeText:
    def test_lowercase_and_collapse(self):
        result = normalize_for_concordance("robot_platform", "  Da Vinci   Xi  ")
        assert result == "da vinci xi"

    def test_country(self):
        result = normalize_for_concordance("country", "  USA  ")
        assert result == "usa"


class TestCategoricalExactMatch:
    """Categorical values already matching enum are unchanged."""

    def test_study_type(self):
        assert normalize_for_concordance("study_type", "Original Research") == "Original Research"

    def test_task_monitor(self):
        assert normalize_for_concordance("task_monitor", "H") == "H"

    def test_clinical_readiness(self):
        result = normalize_for_concordance("clinical_readiness_assessment", "Proof of concept only")
        assert result == "Proof of concept only"


# ── Scoring tests ────────────────────────────────────────────────────


class TestScorePairNulls:
    def test_both_none(self):
        s = score_pair("autonomy_level", None, None)
        assert s.result == "MATCH"

    def test_nr_vs_none(self):
        """NR vs None → MATCH (both represent absence)."""
        s = score_pair("autonomy_level", "NR", None)
        assert s.result == "MATCH"

    def test_nr_vs_not_found(self):
        s = score_pair("country", "NR", "NOT_FOUND")
        assert s.result == "MATCH"

    def test_one_absent(self):
        s = score_pair("autonomy_level", "2 (Task autonomy)", None)
        assert s.result == "MISMATCH"


class TestScorePairAutonomyLevel:
    def test_bare_vs_full_match(self):
        """'2' vs '2 (Task autonomy)' → MATCH after normalization."""
        s = score_pair("autonomy_level", "2", "2 (Task autonomy)")
        assert s.result == "MATCH"

    def test_bare_4_vs_full(self):
        s = score_pair("autonomy_level", "4", "4 (High autonomy)")
        assert s.result == "MATCH"

    def test_different_levels_mismatch(self):
        s = score_pair("autonomy_level", "2 (Task autonomy)", "3 (Conditional autonomy)")
        assert s.result == "MISMATCH"


class TestScorePairMultiValue:
    def test_identical_sets(self):
        s = score_pair(
            "validation_setting",
            "In vivo (animal); Phantom/Simulation",
            "Phantom/Simulation; In vivo (animal)",
        )
        assert s.result == "MATCH"

    def test_subset_ambiguous(self):
        """'In vivo (animal); Phantom/Simulation' vs 'Phantom/Simulation' → AMBIGUOUS."""
        s = score_pair(
            "validation_setting",
            "In vivo (animal); Phantom/Simulation",
            "Phantom/Simulation",
        )
        assert s.result == "AMBIGUOUS"
        assert "Jaccard" in s.detail

    def test_disjoint_sets_mismatch(self):
        s = score_pair("validation_setting", "Ex vivo", "In vivo (human)")
        assert s.result == "MISMATCH"


class TestScorePairFreeText:
    def test_exact_match(self):
        s = score_pair("robot_platform", "da Vinci Xi", "da Vinci Xi")
        assert s.result == "MATCH"

    def test_substring_containment(self):
        """'da Vinci Xi' vs 'da Vinci Xi (Intuitive Surgical)' → MATCH."""
        s = score_pair("robot_platform", "da Vinci Xi", "da Vinci Xi (Intuitive Surgical)")
        assert s.result == "MATCH"
        assert "substring" in s.detail

    def test_no_overlap_mismatch(self):
        s = score_pair("robot_platform", "KUKA iiwa", "Raven II")
        assert s.result == "MISMATCH"

    def test_partial_overlap_ambiguous(self):
        s = score_pair(
            "task_performed",
            "autonomous suturing of tissue",
            "suturing of tissue with needle driving",
        )
        assert s.result in ("MATCH", "AMBIGUOUS")


class TestScorePairNumeric:
    def test_same_integer(self):
        s = score_pair("sample_size", "10", "n=10")
        assert s.result == "MATCH"

    def test_different_integer(self):
        s = score_pair("sample_size", "10", "20")
        assert s.result == "MISMATCH"


class TestScorePairSystemMaturity:
    def test_prefix_truncation(self):
        """'Research prototype' vs 'Research prototype (hardware)' → MATCH."""
        s = score_pair("system_maturity", "Research prototype", "Research prototype (hardware)")
        assert s.result == "MATCH"
