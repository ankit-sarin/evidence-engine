"""Tests for analysis/paper1/judge_prompts.py."""

import pytest

from analysis.paper1.judge_prompts import (
    SPAN_TRUNCATE_CHARS,
    build_pass1_prompt,
    compute_seed,
    randomize_arm_assignment,
)
from analysis.paper1.judge_schema import ArmOutput, JudgeInput
from analysis.paper1.precheck import PreCheckFlags

SEED_MOD = 2**32


def _flags(present=True, in_source=True, in_span=True):
    return PreCheckFlags(
        span_present=present,
        span_in_source=in_source,
        value_in_span=in_span,
        span_length=50 if present else 0,
        span_match_method="exact_substring" if in_source else "none",
        value_match_method="categorical_exact" if in_span else "none",
    )


def _arm(name, value="VAL", span="A supporting span for value VAL here."):
    return ArmOutput(
        arm_name=name, value=value, span=span, precheck_flags=_flags()
    )


def _categorical_input(arms=None):
    if arms is None:
        arms = [_arm("a"), _arm("b"), _arm("c")]
    return JudgeInput(
        paper_id="EE-001",
        field_name="study_design",
        field_type="categorical",
        field_definition="The design used in the study.",
        field_valid_values=["RCT", "Cohort", "Case Series"],
        arms=arms,
    )


def _free_text_input():
    return JudgeInput(
        paper_id="EE-001",
        field_name="primary_outcome",
        field_type="free_text",
        field_definition="The primary outcome reported.",
        field_valid_values=None,
        arms=[_arm("a"), _arm("b")],
    )


# ---------------------------------------------------------------------------
# randomize_arm_assignment
# ---------------------------------------------------------------------------


class TestRandomize:
    def test_deterministic_same_seed(self):
        arms = [_arm(n) for n in ("alpha", "beta", "gamma", "delta")]
        a_shuf, a_perm = randomize_arm_assignment(arms, seed=12345)
        b_shuf, b_perm = randomize_arm_assignment(arms, seed=12345)
        assert a_perm == b_perm
        assert [x.arm_name for x in a_shuf] == [x.arm_name for x in b_shuf]

    def test_different_seeds_can_differ(self):
        arms = [_arm(n) for n in ("alpha", "beta", "gamma", "delta", "epsilon")]
        perms = {
            tuple(randomize_arm_assignment(arms, seed=s)[1])
            for s in (1, 2, 3, 4, 5, 6)
        }
        assert len(perms) > 1

    def test_length_matches(self):
        arms = [_arm(n) for n in ("a", "b", "c")]
        shuffled, perm = randomize_arm_assignment(arms, seed=7)
        assert len(shuffled) == len(arms)
        assert len(perm) == len(arms)

    def test_preserves_all_arm_names(self):
        names = ["a", "b", "c", "d", "e"]
        arms = [_arm(n) for n in names]
        _, perm = randomize_arm_assignment(arms, seed=99)
        assert set(perm) == set(names)

    def test_perm_aligns_with_shuffled(self):
        arms = [_arm("x"), _arm("y"), _arm("z")]
        shuffled, perm = randomize_arm_assignment(arms, seed=42)
        assert [a.arm_name for a in shuffled] == perm


# ---------------------------------------------------------------------------
# build_pass1_prompt
# ---------------------------------------------------------------------------


class TestBuildPrompt:
    def test_contains_field_definition(self):
        inp = _categorical_input()
        prompt = build_pass1_prompt(inp, inp.arms)
        assert "The design used in the study." in prompt
        assert "Field name: study_design" in prompt
        assert "Field type: categorical" in prompt

    def test_includes_all_arm_values(self):
        arms = [
            _arm("a", value="RCT"),
            _arm("b", value="Cohort"),
            _arm("c", value="Case Series"),
        ]
        inp = _categorical_input(arms)
        prompt = build_pass1_prompt(inp, arms)
        assert "RCT" in prompt
        assert "Cohort" in prompt
        assert "Case Series" in prompt

    def test_includes_all_three_precheck_flags_per_slot(self):
        inp = _categorical_input()
        prompt = build_pass1_prompt(inp, inp.arms)
        assert prompt.count("span_present:") == 3
        assert prompt.count("span_in_source:") == 3
        assert prompt.count("value_in_span:") == 3

    def test_absent_value_marker(self):
        arms = [_arm("a", value=None, span="span text"), _arm("b")]
        inp = JudgeInput(
            paper_id="p", field_name="f", field_type="free_text",
            field_definition="x", field_valid_values=None, arms=arms,
        )
        prompt = build_pass1_prompt(inp, arms)
        assert "(absent / NR)" in prompt

    def test_absent_span_marker(self):
        arms = [_arm("a", value="VAL", span=None), _arm("b")]
        inp = JudgeInput(
            paper_id="p", field_name="f", field_type="free_text",
            field_definition="x", field_valid_values=None, arms=arms,
        )
        prompt = build_pass1_prompt(inp, arms)
        assert "(no span provided)" in prompt

    def test_span_truncation(self):
        long_span = "x" * (SPAN_TRUNCATE_CHARS + 500)
        arms = [_arm("a", span=long_span), _arm("b")]
        inp = JudgeInput(
            paper_id="p", field_name="f", field_type="free_text",
            field_definition="x", field_valid_values=None, arms=arms,
        )
        prompt = build_pass1_prompt(inp, arms)
        assert "[span truncated for prompt length]" in prompt
        # Full 900-char span not present verbatim
        assert long_span not in prompt

    def test_short_span_not_truncated(self):
        span = "x" * SPAN_TRUNCATE_CHARS  # exactly at boundary → no truncation
        arms = [_arm("a", span=span), _arm("b")]
        inp = JudgeInput(
            paper_id="p", field_name="f", field_type="free_text",
            field_definition="x", field_valid_values=None, arms=arms,
        )
        prompt = build_pass1_prompt(inp, arms)
        assert "[span truncated for prompt length]" not in prompt

    def test_omits_valid_values_for_non_categorical(self):
        inp = _free_text_input()
        prompt = build_pass1_prompt(inp, inp.arms)
        assert "Valid values:" not in prompt

    def test_includes_valid_values_for_categorical(self):
        inp = _categorical_input()
        prompt = build_pass1_prompt(inp, inp.arms)
        assert "Valid values: RCT, Cohort, Case Series" in prompt

    def test_omits_valid_values_when_categorical_but_none(self):
        inp = JudgeInput(
            paper_id="p", field_name="f", field_type="categorical",
            field_definition="x", field_valid_values=None,
            arms=[_arm("a"), _arm("b")],
        )
        prompt = build_pass1_prompt(inp, inp.arms)
        assert "Valid values:" not in prompt

    def test_includes_bias_mitigation_section(self):
        inp = _categorical_input()
        prompt = build_pass1_prompt(inp, inp.arms)
        assert "=== BIAS-MITIGATION INSTRUCTIONS ===" in prompt
        assert "Slot ordering is randomized." in prompt

    def test_includes_task_and_output_sections(self):
        inp = _categorical_input()
        prompt = build_pass1_prompt(inp, inp.arms)
        assert "=== YOUR TASK ===" in prompt
        assert "=== OUTPUT FORMAT ===" in prompt
        assert "=== EXTRACTED OUTPUTS ===" in prompt
        assert "=== SYSTEM ROLE ===" in prompt

    def test_output_format_uses_compact_example_not_json_schema(self):
        inp = _categorical_input()
        prompt = build_pass1_prompt(inp, inp.arms)
        # Compact example present.
        assert "\"pairwise_ratings\": [" in prompt
        assert "\"disagreement_type\": \"GRANULARITY\"" in prompt
        assert "<one or two sentences>" in prompt
        # No inlined JSON Schema structural keys.
        assert "$defs" not in prompt
        assert "$ref" not in prompt
        assert "propertyName" not in prompt

    def test_prompt_under_5000_chars_for_typical_input(self):
        inp = _categorical_input()
        prompt = build_pass1_prompt(inp, inp.arms)
        assert len(prompt) < 5000, f"len={len(prompt)}"

    def test_slot_labels_are_one_indexed(self):
        inp = _categorical_input()
        prompt = build_pass1_prompt(inp, inp.arms)
        assert "--- Slot 1 ---" in prompt
        assert "--- Slot 2 ---" in prompt
        assert "--- Slot 3 ---" in prompt
        assert "--- Slot 0 ---" not in prompt


# ---------------------------------------------------------------------------
# compute_seed
# ---------------------------------------------------------------------------


class TestComputeSeed:
    def test_deterministic(self):
        a = compute_seed("EE-001", "study_design", "run-2026-04-20")
        b = compute_seed("EE-001", "study_design", "run-2026-04-20")
        assert a == b

    def test_fits_in_32_bits(self):
        s = compute_seed("EE-001", "study_design", "run-x")
        assert 0 <= s < SEED_MOD

    def test_differs_across_inputs(self):
        seeds = {
            compute_seed("EE-001", "a", "r1"),
            compute_seed("EE-002", "a", "r1"),
            compute_seed("EE-001", "b", "r1"),
            compute_seed("EE-001", "a", "r2"),
        }
        assert len(seeds) == 4
