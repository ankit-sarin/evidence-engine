"""Tests for Pass 2 prompt + windowing pieces in judge_prompts.py."""

from __future__ import annotations

import pytest

from analysis.paper1.judge_prompts import (
    ABSENCE_SENTINELS,
    PASS2_FULL_TEXT_BUDGET_TOKENS,
    PASS2_GAP_MARKER,
    arm_short_circuit_eligible,
    build_pass2_prompt,
    compute_seed,
    compute_seed_pass2,
    count_tokens,
    is_absence_claim,
    window_source_text,
)
from analysis.paper1.judge_schema import ArmOutput, JudgeInput
from analysis.paper1.precheck import PreCheckFlags


def _flags(clean=True):
    return PreCheckFlags(
        span_present=True,
        span_in_source=clean, value_in_span=clean,
        span_length=40,
        span_match_method="exact_substring" if clean else "none",
        value_match_method="categorical_exact" if clean else "none",
    )


def _arm(name, value="RCT", span="randomized controlled trial", clean=True):
    return ArmOutput(arm_name=name, value=value, span=span,
                     precheck_flags=_flags(clean))


def _input(arms=None, field_type="categorical"):
    if arms is None:
        arms = [_arm("a"), _arm("b", clean=False), _arm("c")]
    return JudgeInput(
        paper_id="p1",
        field_name="study_design",
        field_type=field_type,
        field_definition="Design of the study.",
        field_valid_values=["RCT", "Cohort"] if field_type == "categorical" else None,
        arms=arms,
    )


# ── seed & short-circuit helpers ───────────────────────────────────


class TestSeeds:
    def test_pass1_and_pass2_seeds_differ(self):
        s1 = compute_seed("p1", "f", "r1")
        s2 = compute_seed_pass2("p1", "f", "r1")
        assert s1 != s2

    def test_pass2_seed_deterministic(self):
        assert compute_seed_pass2("p1", "f", "r1") == \
               compute_seed_pass2("p1", "f", "r1")

    def test_pass2_seed_fits_in_32_bits(self):
        s = compute_seed_pass2("p1", "f", "r1")
        assert 0 <= s < 2**32


class TestArmShortCircuit:
    def test_eligible_when_both_flags_true(self):
        assert arm_short_circuit_eligible(_arm("x", clean=True)) is True

    def test_not_eligible_when_flags_false(self):
        assert arm_short_circuit_eligible(_arm("x", clean=False)) is False

    def test_not_eligible_when_only_one_flag(self):
        f = PreCheckFlags(
            span_present=True, span_in_source=True, value_in_span=False,
            span_length=40, span_match_method="exact_substring",
            value_match_method="none",
        )
        arm = ArmOutput(arm_name="x", value="v", span="s", precheck_flags=f)
        assert arm_short_circuit_eligible(arm) is False


# ── windowing ──────────────────────────────────────────────────────


class TestWindowSourceText:
    def test_short_text_passes_through_unwindowed(self):
        text = "This was a randomized controlled trial."
        out, windowed, toks = window_source_text(text, [text])
        assert out == text
        assert windowed is False
        assert toks == count_tokens(text)

    def test_long_text_gets_windowed(self):
        big = "paragraph.\n\n" * 4000 + "this needle appears here. " \
              + "\n\ntail paragraph.\n\n" * 4000
        assert count_tokens(big) > PASS2_FULL_TEXT_BUDGET_TOKENS
        out, windowed, toks = window_source_text(big, ["this needle appears here"])
        assert windowed is True
        assert "this needle appears here" in out
        assert toks <= PASS2_FULL_TEXT_BUDGET_TOKENS

    def test_budget_is_respected_when_span_found(self):
        big = ("content " * 10_000)  # huge
        big = big + "TARGET_NEEDLE appears in the middle of the doc. " \
              + "content " * 10_000
        out, windowed, toks = window_source_text(big, ["TARGET_NEEDLE"])
        assert windowed is True
        assert toks <= PASS2_FULL_TEXT_BUDGET_TOKENS

    def test_no_span_found_falls_back_to_prefix(self):
        big = "unique_sentinel content " * 10_000
        out, windowed, toks = window_source_text(big, ["never_present"])
        assert windowed is True
        assert toks <= PASS2_FULL_TEXT_BUDGET_TOKENS
        # Should start near the beginning of the source.
        assert out.startswith("unique_sentinel content")

    def test_multiple_spans_merge_windows(self):
        chunks = []
        chunks.append("x " * 3000)  # ~3000 tokens of filler
        chunks.append("SPAN_ALPHA appears here.")
        chunks.append("y " * 3000)
        chunks.append("SPAN_BETA appears here.")
        chunks.append("z " * 3000)
        text = "\n\n".join(chunks)
        out, windowed, toks = window_source_text(
            text, ["SPAN_ALPHA", "SPAN_BETA"]
        )
        assert "SPAN_ALPHA appears here" in out
        assert "SPAN_BETA appears here" in out

    def test_empty_spans_list_handled(self):
        text = "short text that fits"
        out, windowed, toks = window_source_text(text, [])
        assert windowed is False
        assert out == text

    def test_none_spans_handled(self):
        text = "short text"
        out, _, _ = window_source_text(text, [None, None])
        assert out == text

    def test_empty_source_returns_zero_tokens(self):
        out, windowed, toks = window_source_text("", ["x"])
        assert out == ""
        assert toks == 0
        assert windowed is False


# ── build_pass2_prompt ─────────────────────────────────────────────


class TestBuildPass2Prompt:
    def test_contains_field_definition_and_paper_id(self):
        inp = _input()
        prompt = build_pass2_prompt(inp, inp.arms, "source text", False)
        assert "Paper id: p1" in prompt
        assert "Field name: study_design" in prompt
        assert "Design of the study." in prompt

    def test_labels_clean_arms_as_clean_pre_check(self):
        inp = _input()
        prompt = build_pass2_prompt(inp, inp.arms, "source text", False)
        # Two clean arms + one dirty — count the slot tag specifically
        # (the literal "CLEAN PRE-CHECK" phrase also appears once in the
        # task block describing the short-circuit rule).
        assert prompt.count("(CLEAN PRE-CHECK)") == 2
        assert prompt.count("(NEEDS FULL VERIFICATION)") == 1

    def test_includes_three_precheck_flags_per_slot(self):
        inp = _input()
        prompt = build_pass2_prompt(inp, inp.arms, "source", False)
        assert prompt.count("span_present:") == 3
        assert prompt.count("span_in_source:") == 3
        assert prompt.count("value_in_span:") == 3

    def test_source_header_distinguishes_full_vs_window(self):
        inp = _input()
        full = build_pass2_prompt(inp, inp.arms, "source", False)
        win = build_pass2_prompt(inp, inp.arms, "source", True)
        assert "SOURCE TEXT (full)" in full
        assert "SOURCE EXCERPT (windowed)" in win

    def test_includes_task_and_output_and_short_circuit_language(self):
        inp = _input()
        prompt = build_pass2_prompt(inp, inp.arms, "source", False)
        assert "=== YOUR TASK ===" in prompt
        assert "=== OUTPUT FORMAT ===" in prompt
        assert "Short-circuit rule" in prompt
        assert "Default these to SUPPORTED UNLESS" in prompt

    def test_categorical_injects_valid_values(self):
        inp = _input()
        prompt = build_pass2_prompt(inp, inp.arms, "source", False)
        assert "Valid values: RCT, Cohort" in prompt

    def test_non_categorical_omits_valid_values(self):
        inp = _input(field_type="free_text")
        prompt = build_pass2_prompt(inp, inp.arms, "source", False)
        assert "Valid values:" not in prompt

    def test_example_is_compact_not_inlined_schema(self):
        inp = _input()
        prompt = build_pass2_prompt(inp, inp.arms, "source", False)
        assert '"verdict": "SUPPORTED"' in prompt
        assert "$defs" not in prompt
        assert "$ref" not in prompt

    def test_slot_numbering_is_one_indexed(self):
        inp = _input()
        prompt = build_pass2_prompt(inp, inp.arms, "source", False)
        assert "--- Slot 1 " in prompt
        assert "--- Slot 2 " in prompt
        assert "--- Slot 3 " in prompt
        assert "--- Slot 0 " not in prompt

    def test_contains_source_text_verbatim(self):
        inp = _input()
        src = "Methods: we enrolled 120 patients in a randomized trial."
        prompt = build_pass2_prompt(inp, inp.arms, src, False)
        assert src in prompt

    def test_prompt_deterministic_given_inputs(self):
        inp = _input()
        p1 = build_pass2_prompt(inp, inp.arms, "source", False)
        p2 = build_pass2_prompt(inp, inp.arms, "source", False)
        assert p1 == p2


# ── absence sentinel detection ─────────────────────────────────────


class TestIsAbsenceClaim:
    @pytest.mark.parametrize("value", [
        "NR", "N/A", "NA", "NOT_FOUND", "NOT FOUND", "NOT REPORTED",
    ])
    def test_canonical_sentinels_flagged(self, value):
        assert is_absence_claim(value) is True

    @pytest.mark.parametrize("value", [
        "nr", "n/a", "Not_Found", "not reported", "  NR  ", "\tN/A\n",
    ])
    def test_sentinels_case_and_whitespace_insensitive(self, value):
        assert is_absence_claim(value) is True

    def test_empty_string_is_absence(self):
        assert is_absence_claim("") is True

    def test_whitespace_only_is_absence(self):
        assert is_absence_claim("   ") is True

    def test_none_is_absence(self):
        assert is_absence_claim(None) is True

    @pytest.mark.parametrize("value", [
        "RCT", "0", "19", "Feasibility study", "N/Applicable",
        "not really", "N/A compliant", "NRS-5",
    ])
    def test_positive_values_not_flagged(self, value):
        assert is_absence_claim(value) is False

    def test_sentinel_set_contains_canonical_forms(self):
        assert "NR" in ABSENCE_SENTINELS
        assert "NOT_FOUND" in ABSENCE_SENTINELS
        assert "N/A" in ABSENCE_SENTINELS


# ── absence-aware prompt branching ─────────────────────────────────


def _absence_arm(name, value="NR"):
    """Arm with a sentinel value — precheck reflects no span to match."""
    flags = PreCheckFlags(
        span_present=False, span_in_source=False, value_in_span=False,
        span_length=0,
        span_match_method="none", value_match_method="none",
    )
    return ArmOutput(arm_name=name, value=value, span=None, precheck_flags=flags)


class TestAbsenceRubricBranching:
    def test_absence_slot_tagged_absence_claim(self):
        arms = [_arm("a"), _arm("b"), _absence_arm("c", "NR")]
        inp = _input(arms=arms)
        prompt = build_pass2_prompt(inp, arms, "source text", False)
        assert "(ABSENCE CLAIM)" in prompt
        assert prompt.count("(ABSENCE CLAIM)") == 1

    def test_absence_rubric_block_appears_when_any_arm_is_absence(self):
        arms = [_arm("a"), _arm("b"), _absence_arm("c", "NOT_FOUND")]
        inp = _input(arms=arms)
        prompt = build_pass2_prompt(inp, arms, "source", False)
        assert "=== ABSENCE-CLAIM VERIFICATION ===" in prompt
        # field_name interpolated into the rubric
        assert "value for study_design" in prompt
        assert "statement of study_design" in prompt

    def test_no_absence_rubric_when_all_arms_positive(self):
        arms = [_arm("a"), _arm("b"), _arm("c")]
        inp = _input(arms=arms)
        prompt = build_pass2_prompt(inp, arms, "source", False)
        assert "ABSENCE-CLAIM VERIFICATION" not in prompt
        assert "(ABSENCE CLAIM)" not in prompt

    def test_standard_rubric_preserved_alongside_absence_rubric(self):
        arms = [_arm("a"), _arm("b"), _absence_arm("c", "N/A")]
        inp = _input(arms=arms)
        prompt = build_pass2_prompt(inp, arms, "source", False)
        # Standard task block still present
        assert "=== YOUR TASK ===" in prompt
        assert "Short-circuit rule" in prompt
        # Plus absence-aware block
        assert "=== ABSENCE-CLAIM VERIFICATION ===" in prompt


class TestMixedTripleIntegration:
    def test_mixed_triple_slot_tags_match_arm_values(self):
        # 1 absence, 1 clean positive, 1 dirty positive
        arms = [
            _arm("a", value="RCT", clean=True),
            _absence_arm("b", "NR"),
            _arm("c", value="Cohort", clean=False),
        ]
        inp = _input(arms=arms)
        prompt = build_pass2_prompt(inp, arms, "source", False)
        assert prompt.count("(CLEAN PRE-CHECK)") == 1
        assert prompt.count("(ABSENCE CLAIM)") == 1
        assert prompt.count("(NEEDS FULL VERIFICATION)") == 1
        # Exactly one absence rubric block, regardless of how many absence arms
        assert prompt.count("=== ABSENCE-CLAIM VERIFICATION ===") == 1

    def test_all_absence_triple_still_emits_single_rubric_block(self):
        arms = [
            _absence_arm("a", "NR"),
            _absence_arm("b", "NOT_FOUND"),
            _absence_arm("c", ""),
        ]
        inp = _input(arms=arms)
        prompt = build_pass2_prompt(inp, arms, "source", False)
        assert prompt.count("(ABSENCE CLAIM)") == 3
        assert prompt.count("=== ABSENCE-CLAIM VERIFICATION ===") == 1
        # Absence-arm tag takes precedence over CLEAN/NEEDS tags
        assert "(CLEAN PRE-CHECK)" not in prompt
        assert "(NEEDS FULL VERIFICATION)" not in prompt
