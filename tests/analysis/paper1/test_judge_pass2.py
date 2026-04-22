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
from analysis.paper1.judge import _validate_pass2_coverage
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


# ── Grammar-tightening regressions (arm_verdicts cardinality / slot enum) ──


class TestArmVerdictsGrammarRegression:
    """Post-fix guard: cardinality=3 + arm_slot Literal[1,2,3] in the Pydantic
    schema are the grammar-visible invariants. Slot uniqueness remains a
    post-validator responsibility.

    Failure mode captured on paper 366 / primary_outcome_value (Pass 2 run
    surgical_autonomy_pass2_full_20260421T174729Z, seed=1770411156):
    Gemma emitted 4 arm_verdicts entries with slots [1, 2, 3, 3]. The
    duplicate-slot post-validator caught it; the permissive Pydantic
    schema did not prevent it. These tests lock in both layers.
    """

    def test_four_element_payload_rejected_by_pydantic_maxitems(self):
        """With maxItems=3 in the generated JSON Schema, a 4-element
        response is rejected at Pydantic parse time — the same raw
        Gemma output that failed on paper 366 is now caught before the
        post-validator even runs."""
        captured = json.dumps({
            "paper_id": "366",
            "field_name": "primary_outcome_value",
            "arm_verdicts": [
                {"arm_slot": 1, "verdict": "SUPPORTED",
                 "verification_span": "accuracy 77.9%"},
                {"arm_slot": 2, "verdict": "PARTIALLY_SUPPORTED",
                 "reasoning": "r",
                 "verification_span": "sensitivity 72.3%"},
                {"arm_slot": 3, "verdict": "SUPPORTED",
                 "verification_span": "NN Accuracy 77.9% ± 5.9%"},
                {"arm_slot": 3, "verdict": "SUPPORTED",
                 "verification_span": "NN Accuracy 77.9% ± 5.9%"},
            ],
            "overall_fabrication_detected": False,
        })
        with patch.object(judge_module, "ollama_chat",
                          lambda **kw: _mock_resp(captured)), \
             patch.object(judge_module, "fetch_model_digest", lambda m: "d"):
            with pytest.raises(JudgeParseError):
                run_pass2(_input(), run_id="r1", source_text="src")

    def test_post_validator_still_rejects_duplicate_slot_on_four_elements(self):
        """Belt-and-suspenders: if someone later bypasses Pydantic
        (e.g., via model_construct or a downgraded schema), the
        post-validator must still raise on [1,2,3,3].

        Guards against future code that removes the duplicate-slot check
        on the false assumption that grammar alone is sufficient."""
        v1 = SupportedVerdict.model_construct(
            arm_slot=1, verdict="SUPPORTED", verification_span="q1"
        )
        v2 = SupportedVerdict.model_construct(
            arm_slot=2, verdict="SUPPORTED", verification_span="q2"
        )
        v3a = SupportedVerdict.model_construct(
            arm_slot=3, verdict="SUPPORTED", verification_span="q3a"
        )
        v3b = SupportedVerdict.model_construct(
            arm_slot=3, verdict="SUPPORTED", verification_span="q3b"
        )
        # model_construct skips Pydantic validation → 4-element list
        # reaches the post-validator.
        pass2 = Pass2Output.model_construct(
            paper_id="366",
            field_name="primary_outcome_value",
            arm_verdicts=[v1, v2, v3a, v3b],
            overall_fabrication_detected=False,
        )
        with pytest.raises(JudgeParseError, match="duplicate arm_slot=3"):
            _validate_pass2_coverage(
                pass2,
                ["local", "openai_o4_mini_high", "anthropic_sonnet_4_6"],
            )

    def test_json_schema_emits_cardinality_and_slot_enum(self):
        """Static check: what Ollama receives via `format=` carries both
        grammar-enforceable invariants. If Pydantic ever stops emitting
        these, llama.cpp's grammar backend will silently go back to
        permissive decoding."""
        s = Pass2Output.model_json_schema()
        av = s["properties"]["arm_verdicts"]
        assert av["type"] == "array"
        assert av["minItems"] == 3
        assert av["maxItems"] == 3
        # arm_slot enum on each discriminator branch.
        items = av["items"]
        assert "oneOf" in items and "discriminator" in items
        defs = s["$defs"]
        for branch in items["oneOf"]:
            name = branch["$ref"].rsplit("/", 1)[-1]
            slot = defs[name]["properties"]["arm_slot"]
            assert slot["enum"] == [1, 2, 3], (
                f"variant {name} arm_slot enum={slot.get('enum')}"
            )
            assert slot["type"] == "integer"

    def test_discriminator_still_routes_unsupported_verdict(self):
        """Confirm the discriminated union (verdict=SUPPORTED /
        PARTIALLY_SUPPORTED / UNSUPPORTED) still routes correctly after
        the arm_verdicts container edit. UNSUPPORTED must still require
        reasoning + fabrication_hypothesis."""
        good = json.dumps({
            "paper_id": "p1", "field_name": "study_design",
            "arm_verdicts": [
                {"arm_slot": 1, "verdict": "SUPPORTED",
                 "verification_span": "q1"},
                {"arm_slot": 2, "verdict": "PARTIALLY_SUPPORTED",
                 "reasoning": "partial match",
                 "verification_span": "q2"},
                {"arm_slot": 3, "verdict": "UNSUPPORTED",
                 "reasoning": "no evidence",
                 "fabrication_hypothesis": "plausible-sounding default",
                 "verification_span": "q3"},
            ],
            "overall_fabrication_detected": True,
        })
        with patch.object(judge_module, "ollama_chat",
                          lambda **kw: _mock_resp(good)), \
             patch.object(judge_module, "fetch_model_digest", lambda m: "d"):
            res = run_pass2(_input(), run_id="r1", source_text="src")
        kinds = {type(v).__name__ for v in res.pass2.arm_verdicts}
        assert kinds == {
            "SupportedVerdict",
            "PartiallySupportedVerdict",
            "UnsupportedVerdict",
        }

    def test_unsupported_without_hypothesis_still_rejected(self):
        """UNSUPPORTED without fabrication_hypothesis must still fail
        — the container edit must not loosen per-variant required
        fields."""
        bad = json.dumps({
            "paper_id": "p1", "field_name": "study_design",
            "arm_verdicts": [
                {"arm_slot": 1, "verdict": "SUPPORTED",
                 "verification_span": "q1"},
                {"arm_slot": 2, "verdict": "SUPPORTED",
                 "verification_span": "q2"},
                {"arm_slot": 3, "verdict": "UNSUPPORTED",
                 "reasoning": "no evidence",
                 "verification_span": "q3"},  # missing fabrication_hypothesis
            ],
            "overall_fabrication_detected": True,
        })
        with patch.object(judge_module, "ollama_chat",
                          lambda **kw: _mock_resp(bad)), \
             patch.object(judge_module, "fetch_model_digest", lambda m: "d"):
            with pytest.raises(JudgeParseError):
                run_pass2(_input(), run_id="r1", source_text="src")


# ── Live-Gemma regression for the paper-366 pathology ──────────────


@pytest.mark.ollama
@pytest.mark.integration
def test_paper_366_grammar_prevents_four_element_emission():
    """Replay the exact prompt that previously induced [1,2,3,3] on
    paper 366 / primary_outcome_value and confirm that the tightened
    schema produces exactly 3 elements with slots as a permutation of
    {1, 2, 3}.

    Requires a running Ollama with gemma3:27b. Skipped in the default
    offline suite (markers: ollama, integration). Produces no DB
    writes.
    """
    from pathlib import Path

    from analysis.paper1.judge_loader import load_ai_triples_csv, load_codebook
    from analysis.paper1.judge_prompts import compute_seed_pass2
    from engine.core.database import ReviewDatabase

    review_dir = Path("data/surgical_autonomy")
    pairs_csv = review_dir / "exports/disagreement_pairs_3arm.csv"
    codebook_path = review_dir / "extraction_codebook.yaml"
    if not (pairs_csv.exists() and codebook_path.exists()):
        pytest.skip("surgical_autonomy artifacts not available in this env")

    db = ReviewDatabase("surgical_autonomy")
    try:
        codebook = load_codebook(codebook_path)
        inputs = load_ai_triples_csv(pairs_csv, db, codebook, limit=None)
        match = [
            i for i in inputs
            if i.paper_id == "366" and i.field_name == "primary_outcome_value"
        ]
        if not match:
            pytest.skip("pairs CSV missing paper 366 / primary_outcome_value")
        inp = match[0]
        parsed = sorted(
            (review_dir / "parsed_text").glob("366_v*.md"), reverse=True
        )
        if not parsed:
            pytest.skip("parsed text for paper 366 not on disk")
        source_text = parsed[0].read_text()
    finally:
        db.close()

    run_id = "surgical_autonomy_pass2_full_20260421T174729Z"
    # Deterministic check: the seed under which the failure occurred.
    assert compute_seed_pass2(
        "366", "primary_outcome_value", run_id
    ) == 1770411156, "seed drift — paper 366 no longer hashes to 1770411156"

    result = run_pass2(inp, run_id=run_id, source_text=source_text)

    assert len(result.pass2.arm_verdicts) == 3, (
        f"expected 3 arm_verdicts, got {len(result.pass2.arm_verdicts)} — "
        f"grammar-enforced maxItems=3 was not respected by llama.cpp on "
        f"this model; raw={result.raw_response!r}"
    )
    slots = sorted(v.arm_slot for v in result.pass2.arm_verdicts)
    assert slots == [1, 2, 3], (
        f"expected slot permutation of [1,2,3], got {slots} — "
        f"raw={result.raw_response!r}"
    )
