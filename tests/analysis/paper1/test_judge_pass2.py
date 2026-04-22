"""Tests for Pass 2 orchestrator (run_pass2) in analysis/paper1/judge.py."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from analysis.paper1 import judge as judge_module
from analysis.paper1.judge import (
    DEFAULT_MODEL,
    JudgeCallError,
    JudgeInvariantError,
    JudgeParseError,
    de_randomize_verdicts,
    run_pass2,
)
from analysis.paper1.judge_schema import (
    ArmOutput,
    JudgeInput,
    Pass2Output,
    Pass2Result,
    SupportedVerdict,
    UnsupportedVerdict,
)
from analysis.paper1.precheck import PreCheckFlags


# ── fixtures ───────────────────────────────────────────────────────


def _flags(clean=True):
    return PreCheckFlags(
        span_present=True, span_in_source=clean, value_in_span=clean,
        span_length=40,
        span_match_method="exact_substring" if clean else "none",
        value_match_method="categorical_exact" if clean else "none",
    )


def _arm(name, value="RCT", span="randomized controlled trial", clean=True):
    return ArmOutput(arm_name=name, value=value, span=span,
                     precheck_flags=_flags(clean))


def _input(arms=None):
    if arms is None:
        arms = [_arm("local"), _arm("openai_o4_mini_high", clean=False),
                _arm("anthropic_sonnet_4_6")]
    return JudgeInput(
        paper_id="p1", field_name="study_design",
        field_type="categorical", field_definition="Design.",
        field_valid_values=["RCT", "Cohort"],
        arms=arms,
    )


def _mock_resp(content):
    return SimpleNamespace(message=SimpleNamespace(content=content))


def _happy_payload(n=3):
    verdicts = [
        {"arm_slot": i + 1, "verdict": "SUPPORTED", "verification_span": f"q{i+1}"}
        for i in range(n)
    ]
    return json.dumps({
        "paper_id": "p1",
        "field_name": "study_design",
        "arm_verdicts": verdicts,
        "overall_fabrication_detected": False,
    })


# ── happy path ─────────────────────────────────────────────────────


class TestRunPass2Happy:
    def test_returns_pass2_result(self):
        inp = _input()
        with patch.object(judge_module, "ollama_chat",
                          lambda **kw: _mock_resp(_happy_payload(3))), \
             patch.object(judge_module, "fetch_model_digest",
                          lambda m: "sha256:abc"):
            res = run_pass2(inp, run_id="r1", source_text="some source.")
        assert isinstance(res, Pass2Result)
        assert res.paper_id == "p1"
        assert res.field_name == "study_design"
        assert res.judge_model_name == DEFAULT_MODEL
        assert res.judge_model_digest == "sha256:abc"
        assert len(res.pass2.arm_verdicts) == 3

    def test_short_circuit_dict_records_clean_arms(self):
        inp = _input()
        with patch.object(judge_module, "ollama_chat",
                          lambda **kw: _mock_resp(_happy_payload(3))), \
             patch.object(judge_module, "fetch_model_digest", lambda m: "d"):
            res = run_pass2(inp, run_id="r1", source_text="src")
        # local + anthropic clean, openai dirty.
        assert res.pre_check_short_circuit_by_arm["local"] is True
        assert res.pre_check_short_circuit_by_arm["anthropic_sonnet_4_6"] is True
        assert res.pre_check_short_circuit_by_arm["openai_o4_mini_high"] is False

    def test_arm_permutation_covers_all_arms(self):
        inp = _input()
        with patch.object(judge_module, "ollama_chat",
                          lambda **kw: _mock_resp(_happy_payload(3))), \
             patch.object(judge_module, "fetch_model_digest", lambda m: "d"):
            res = run_pass2(inp, run_id="r1", source_text="src")
        assert set(res.arm_permutation) == {
            "local", "openai_o4_mini_high", "anthropic_sonnet_4_6"
        }

    def test_ollama_options_include_seed_and_temperature_zero(self):
        inp = _input()
        calls = {}

        def capture(**kwargs):
            calls.update(kwargs)
            return _mock_resp(_happy_payload(3))

        with patch.object(judge_module, "ollama_chat", capture), \
             patch.object(judge_module, "fetch_model_digest", lambda m: "d"):
            res = run_pass2(inp, run_id="r1", source_text="src")
        assert calls["options"]["temperature"] == 0.0
        assert calls["options"]["seed"] == res.seed

    def test_de_randomize_maps_slot_to_arm_name(self):
        inp = _input()
        with patch.object(judge_module, "ollama_chat",
                          lambda **kw: _mock_resp(_happy_payload(3))), \
             patch.object(judge_module, "fetch_model_digest", lambda m: "d"):
            res = run_pass2(inp, run_id="r1", source_text="src")
        mapping = de_randomize_verdicts(res)
        assert set(mapping.keys()) == {
            "local", "openai_o4_mini_high", "anthropic_sonnet_4_6"
        }
        assert all(isinstance(v, SupportedVerdict) for v in mapping.values())

    def test_source_windowed_flag_records_false_for_short_text(self):
        inp = _input()
        with patch.object(judge_module, "ollama_chat",
                          lambda **kw: _mock_resp(_happy_payload(3))), \
             patch.object(judge_module, "fetch_model_digest", lambda m: "d"):
            res = run_pass2(inp, run_id="r1", source_text="short source.")
        assert res.source_text_windowed is False
        assert res.source_text_tokens > 0

    def test_prompt_hash_is_sha256_hex(self):
        inp = _input()
        with patch.object(judge_module, "ollama_chat",
                          lambda **kw: _mock_resp(_happy_payload(3))), \
             patch.object(judge_module, "fetch_model_digest", lambda m: "d"):
            res = run_pass2(inp, run_id="r1", source_text="src")
        assert len(res.prompt_hash) == 64
        assert all(c in "0123456789abcdef" for c in res.prompt_hash)

    def test_determinism_same_inputs_same_seed_and_prompt_hash(self):
        inp = _input()
        with patch.object(judge_module, "ollama_chat",
                          lambda **kw: _mock_resp(_happy_payload(3))), \
             patch.object(judge_module, "fetch_model_digest", lambda m: "d"):
            r1 = run_pass2(inp, run_id="r1", source_text="src")
            r2 = run_pass2(inp, run_id="r1", source_text="src")
        assert r1.seed == r2.seed
        assert r1.prompt_hash == r2.prompt_hash


# ── error paths ────────────────────────────────────────────────────


class TestRunPass2Errors:
    def test_empty_source_text_raises(self):
        with pytest.raises(JudgeInvariantError):
            run_pass2(_input(), run_id="r1", source_text="")

    def test_fewer_than_two_arms_raises(self):
        inp = _input(arms=[_arm("only")])
        with pytest.raises(JudgeInvariantError):
            run_pass2(inp, run_id="r1", source_text="src")

    def test_ollama_error_wrapped(self):
        def boom(**kwargs):
            raise TimeoutError("upstream timeout")
        with patch.object(judge_module, "ollama_chat", boom), \
             patch.object(judge_module, "fetch_model_digest", lambda m: "d"):
            with pytest.raises(JudgeCallError):
                run_pass2(_input(), run_id="r1", source_text="src")

    def test_malformed_json_raises_parse_error(self):
        with patch.object(judge_module, "ollama_chat",
                          lambda **kw: _mock_resp("not json {")), \
             patch.object(judge_module, "fetch_model_digest", lambda m: "d"):
            with pytest.raises(JudgeParseError) as exc_info:
                run_pass2(_input(), run_id="r1", source_text="src")
        assert "not json {" == exc_info.value.raw_response

    def test_schema_violation_raises_parse_error(self):
        bad = json.dumps({
            "paper_id": "p1", "field_name": "study_design",
            "arm_verdicts": [
                {"arm_slot": 1, "verdict": "UNSUPPORTED", "reasoning": "r"},
            ],
            "overall_fabrication_detected": True,
        })
        with patch.object(judge_module, "ollama_chat",
                          lambda **kw: _mock_resp(bad)), \
             patch.object(judge_module, "fetch_model_digest", lambda m: "d"):
            with pytest.raises(JudgeParseError):
                run_pass2(_input(), run_id="r1", source_text="src")

    def test_duplicate_slot_raises_parse_error(self):
        bad = json.dumps({
            "paper_id": "p1", "field_name": "study_design",
            "arm_verdicts": [
                {"arm_slot": 1, "verdict": "SUPPORTED"},
                {"arm_slot": 1, "verdict": "SUPPORTED"},
                {"arm_slot": 2, "verdict": "SUPPORTED"},
            ],
            "overall_fabrication_detected": False,
        })
        with patch.object(judge_module, "ollama_chat",
                          lambda **kw: _mock_resp(bad)), \
             patch.object(judge_module, "fetch_model_digest", lambda m: "d"):
            with pytest.raises(JudgeParseError):
                run_pass2(_input(), run_id="r1", source_text="src")

    def test_missing_slot_raises_parse_error(self):
        bad = json.dumps({
            "paper_id": "p1", "field_name": "study_design",
            "arm_verdicts": [
                {"arm_slot": 1, "verdict": "SUPPORTED"},
                {"arm_slot": 2, "verdict": "SUPPORTED"},
            ],
            "overall_fabrication_detected": False,
        })
        with patch.object(judge_module, "ollama_chat",
                          lambda **kw: _mock_resp(bad)), \
             patch.object(judge_module, "fetch_model_digest", lambda m: "d"):
            with pytest.raises(JudgeParseError):
                run_pass2(_input(), run_id="r1", source_text="src")

    def test_out_of_range_slot_raises_parse_error(self):
        bad = json.dumps({
            "paper_id": "p1", "field_name": "study_design",
            "arm_verdicts": [
                {"arm_slot": 1, "verdict": "SUPPORTED"},
                {"arm_slot": 2, "verdict": "SUPPORTED"},
                {"arm_slot": 99, "verdict": "SUPPORTED"},
            ],
            "overall_fabrication_detected": False,
        })
        with patch.object(judge_module, "ollama_chat",
                          lambda **kw: _mock_resp(bad)), \
             patch.object(judge_module, "fetch_model_digest", lambda m: "d"):
            with pytest.raises(JudgeParseError):
                run_pass2(_input(), run_id="r1", source_text="src")
