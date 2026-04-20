"""Tests for analysis/paper1/judge.py (Pass 1 orchestrator)."""

from __future__ import annotations

import json
from datetime import datetime
from itertools import combinations
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from analysis.paper1 import judge as judge_module
from analysis.paper1.judge import (
    DEFAULT_MODEL,
    JudgeCallError,
    JudgeInvariantError,
    JudgeParseError,
    de_randomize_pairs,
    run_pass1,
)
from analysis.paper1.judge_schema import (
    ArmOutput,
    DisagreementPair,
    EquivalentPair,
    JudgeInput,
    Pass1Output,
)
from analysis.paper1.precheck import PreCheckFlags


def _flags():
    return PreCheckFlags(
        span_present=True,
        span_in_source=True,
        value_in_span=True,
        span_length=40,
        span_match_method="exact_substring",
        value_match_method="categorical_exact",
    )


def _arm(name, value="RCT", span="This is a randomized controlled trial."):
    return ArmOutput(
        arm_name=name, value=value, span=span, precheck_flags=_flags()
    )


def _input(arms=None, field_type="categorical"):
    if arms is None:
        arms = [_arm("local"), _arm("o4_mini"), _arm("sonnet")]
    return JudgeInput(
        paper_id="EE-001",
        field_name="study_design",
        field_type=field_type,
        field_definition="Design of the study.",
        field_valid_values=["RCT", "Cohort", "Case Series"] if field_type == "categorical" else None,
        arms=arms,
    )


def _equiv(a, b):
    return {
        "slot_a": a, "slot_b": b, "rating": "EQUIVALENT",
        "disagreement_type": None, "rationale": "match",
    }


def _pass1_response_json(n_slots: int, consensus: str | None = "RCT") -> str:
    pairs = [_equiv(a, b) for a, b in combinations(range(1, n_slots + 1), 2)]
    return json.dumps(
        {
            "pairwise_ratings": pairs,
            "fabrication_risk": "low",
            "proposed_consensus": consensus,
            "overall_rationale": "all slots align",
        }
    )


def _mock_chat_response(content: str):
    return SimpleNamespace(message=SimpleNamespace(content=content))


class _MockChatFactory:
    def __init__(self, response_text: str):
        self.response_text = response_text
        self.calls: list[dict] = []

    def __call__(self, **kwargs):
        self.calls.append(kwargs)
        return _mock_chat_response(self.response_text)


# ---------------------------------------------------------------------------
# run_pass1 happy path + invariants
# ---------------------------------------------------------------------------


class TestRunPass1Happy:
    def test_happy_path_3_arm(self):
        inp = _input()
        mock = _MockChatFactory(_pass1_response_json(3))
        with patch.object(judge_module, "ollama_chat", mock), \
             patch.object(judge_module, "get_model_digest",
                          lambda m: "sha256:abc"):
            result = run_pass1(inp, run_id="r1")

        assert result.paper_id == "EE-001"
        assert result.field_name == "study_design"
        assert isinstance(result.pass1, Pass1Output)
        assert len(result.pass1.pairwise_ratings) == 3
        assert result.judge_model_name == DEFAULT_MODEL
        assert result.judge_model_digest == "sha256:abc"
        assert set(result.arm_permutation) == {"local", "o4_mini", "sonnet"}
        assert len(mock.calls) == 1

    def test_prompt_hash_is_sha256_hex(self):
        inp = _input()
        with patch.object(judge_module, "ollama_chat",
                          _MockChatFactory(_pass1_response_json(3))), \
             patch.object(judge_module, "get_model_digest", lambda m: "d"):
            result = run_pass1(inp, run_id="r1")
        assert len(result.prompt_hash) == 64
        assert all(c in "0123456789abcdef" for c in result.prompt_hash)

    def test_prompt_hash_deterministic(self):
        inp = _input()
        with patch.object(judge_module, "ollama_chat",
                          _MockChatFactory(_pass1_response_json(3))), \
             patch.object(judge_module, "get_model_digest", lambda m: "d"):
            r1 = run_pass1(inp, run_id="r1")
            r2 = run_pass1(inp, run_id="r1")
        assert r1.prompt_hash == r2.prompt_hash
        assert r1.seed == r2.seed

    def test_prompt_hash_differs_with_different_inputs(self):
        inp_a = _input()
        inp_b = _input(
            arms=[_arm("local", value="Cohort"),
                  _arm("o4_mini"), _arm("sonnet")]
        )
        with patch.object(judge_module, "ollama_chat",
                          _MockChatFactory(_pass1_response_json(3))), \
             patch.object(judge_module, "get_model_digest", lambda m: "d"):
            r1 = run_pass1(inp_a, run_id="r1")
            r2 = run_pass1(inp_b, run_id="r1")
        assert r1.prompt_hash != r2.prompt_hash

    def test_timestamp_iso_parses(self):
        inp = _input()
        with patch.object(judge_module, "ollama_chat",
                          _MockChatFactory(_pass1_response_json(3))), \
             patch.object(judge_module, "get_model_digest", lambda m: "d"):
            result = run_pass1(inp, run_id="r1")
        # Python 3.11+ parses trailing +00:00 natively.
        parsed = datetime.fromisoformat(result.timestamp_iso)
        assert parsed.tzinfo is not None

    def test_arm_permutation_matches_prompt_order(self):
        inp = _input()
        captured: dict[str, str] = {}

        def capture(**kwargs):
            captured["prompt"] = kwargs["messages"][0]["content"]
            return _mock_chat_response(_pass1_response_json(3))

        with patch.object(judge_module, "ollama_chat", capture), \
             patch.object(judge_module, "get_model_digest", lambda m: "d"):
            result = run_pass1(inp, run_id="r1")

        prompt = captured["prompt"]
        positions = [
            prompt.index(f"--- Slot {i + 1} ---")
            for i in range(len(result.arm_permutation))
        ]
        assert positions == sorted(positions)

    def test_seed_passed_to_ollama_options(self):
        inp = _input()
        mock = _MockChatFactory(_pass1_response_json(3))
        with patch.object(judge_module, "ollama_chat", mock), \
             patch.object(judge_module, "get_model_digest", lambda m: "d"):
            result = run_pass1(inp, run_id="r1")
        opts = mock.calls[0]["options"]
        assert opts["seed"] == result.seed
        assert opts["temperature"] == 0.0

    def test_5_arm_produces_10_pairwise(self):
        arms = [_arm(n) for n in ("a1", "a2", "a3", "a4", "a5")]
        inp = _input(arms=arms)
        with patch.object(judge_module, "ollama_chat",
                          _MockChatFactory(_pass1_response_json(5))), \
             patch.object(judge_module, "get_model_digest", lambda m: "d"):
            result = run_pass1(inp, run_id="r1")
        assert len(result.pass1.pairwise_ratings) == 10


# ---------------------------------------------------------------------------
# invariants
# ---------------------------------------------------------------------------


class TestRunPass1Invariants:
    def test_fewer_than_two_arms(self):
        inp = _input(arms=[_arm("only")])
        with pytest.raises(JudgeInvariantError):
            run_pass1(inp, run_id="r1")

    def test_duplicate_arm_names(self):
        inp = _input(arms=[_arm("dup"), _arm("dup"), _arm("other")])
        with pytest.raises(JudgeInvariantError):
            run_pass1(inp, run_id="r1")

    def test_zero_arms(self):
        inp = _input(arms=[])
        with pytest.raises(JudgeInvariantError):
            run_pass1(inp, run_id="r1")


# ---------------------------------------------------------------------------
# error paths
# ---------------------------------------------------------------------------


class TestRunPass1Errors:
    def test_call_error_wraps_client_exception(self):
        inp = _input()

        def boom(**kwargs):
            raise TimeoutError("upstream timeout")

        with patch.object(judge_module, "ollama_chat", boom), \
             patch.object(judge_module, "get_model_digest", lambda m: "d"):
            with pytest.raises(JudgeCallError):
                run_pass1(inp, run_id="r1")

    def test_parse_error_on_malformed_json(self):
        inp = _input()
        mock = _MockChatFactory("not json at all { ")
        with patch.object(judge_module, "ollama_chat", mock), \
             patch.object(judge_module, "get_model_digest", lambda m: "d"):
            with pytest.raises(JudgeParseError) as exc_info:
                run_pass1(inp, run_id="r1")
        assert exc_info.value.raw_response == "not json at all { "

    def test_parse_error_on_schema_violation(self):
        inp = _input()
        bad = json.dumps(
            {
                "pairwise_ratings": [
                    {
                        "slot_a": 1, "slot_b": 2, "rating": "PARTIAL",
                        "rationale": "missing disagreement_type",
                    }
                ],
                "fabrication_risk": "low",
                "proposed_consensus": None,
                "overall_rationale": "x",
            }
        )
        mock = _MockChatFactory(bad)
        with patch.object(judge_module, "ollama_chat", mock), \
             patch.object(judge_module, "get_model_digest", lambda m: "d"):
            with pytest.raises(JudgeParseError) as exc_info:
                run_pass1(inp, run_id="r1")
        assert exc_info.value.raw_response == bad


# ---------------------------------------------------------------------------
# de_randomize_pairs
# ---------------------------------------------------------------------------


def _pairwise(a, b):
    return EquivalentPair(
        slot_a=a, slot_b=b, rating="EQUIVALENT", rationale="x"
    )


class TestDeRandomize:
    def test_stable_ordering(self):
        perm = ["local", "sonnet", "o4_mini"]  # slot1=local, slot2=sonnet, slot3=o4_mini
        out = Pass1Output(
            pairwise_ratings=[
                _pairwise(1, 2),
                _pairwise(1, 3),
                _pairwise(2, 3),
            ],
            fabrication_risk="low",
            proposed_consensus="x",
            overall_rationale="x",
        )
        mapping = de_randomize_pairs(out, perm)
        assert ("local", "sonnet") in mapping
        assert ("local", "o4_mini") in mapping
        assert ("o4_mini", "sonnet") in mapping
        for (a, b) in mapping.keys():
            assert a < b

    def test_three_arm_full_coverage(self):
        perm = ["a", "b", "c"]
        out = Pass1Output(
            pairwise_ratings=[_pairwise(1, 2), _pairwise(1, 3), _pairwise(2, 3)],
            fabrication_risk="low",
            proposed_consensus="x",
            overall_rationale="x",
        )
        mapping = de_randomize_pairs(out, perm)
        assert len(mapping) == 3

    def test_four_arm_full_coverage(self):
        perm = ["a", "b", "c", "d"]
        pairs = [_pairwise(a, b) for a, b in combinations(range(1, 5), 2)]
        out = Pass1Output(
            pairwise_ratings=pairs,
            fabrication_risk="low",
            proposed_consensus="x",
            overall_rationale="x",
        )
        mapping = de_randomize_pairs(out, perm)
        assert len(mapping) == 6

    def test_invalid_slot_pair_rejected(self):
        perm = ["a", "b"]
        out = Pass1Output(
            pairwise_ratings=[_pairwise(1, 3)],  # slot 3 doesn't exist
            fabrication_risk="low",
            proposed_consensus=None,
            overall_rationale="x",
        )
        with pytest.raises(ValueError):
            de_randomize_pairs(out, perm)
