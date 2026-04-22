"""Tests for Pass 2 schema pieces in analysis/paper1/judge_schema.py."""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from analysis.paper1.judge_schema import (
    PartiallySupportedVerdict,
    Pass2Output,
    SupportedVerdict,
    UnsupportedVerdict,
    verdict_requires_hypothesis,
    verdict_requires_reasoning,
)


# ── variants ────────────────────────────────────────────────────────


class TestSupportedVerdict:
    def test_minimal_payload(self):
        v = SupportedVerdict(arm_slot=1, verdict="SUPPORTED")
        assert v.verdict == "SUPPORTED"
        assert v.reasoning is None
        assert v.verification_span is None

    def test_accepts_optional_reasoning(self):
        v = SupportedVerdict(arm_slot=1, verdict="SUPPORTED",
                             reasoning="x", verification_span="y")
        assert v.reasoning == "x"

    def test_rejects_arm_slot_zero(self):
        with pytest.raises(ValidationError):
            SupportedVerdict(arm_slot=0, verdict="SUPPORTED")


class TestPartiallySupportedVerdict:
    def test_happy_path(self):
        v = PartiallySupportedVerdict(
            arm_slot=2, verdict="PARTIALLY_SUPPORTED",
            reasoning="partial match"
        )
        assert v.reasoning == "partial match"

    def test_requires_reasoning(self):
        with pytest.raises(ValidationError):
            PartiallySupportedVerdict(arm_slot=1, verdict="PARTIALLY_SUPPORTED")

    def test_rejects_empty_reasoning(self):
        with pytest.raises(ValidationError):
            PartiallySupportedVerdict(
                arm_slot=1, verdict="PARTIALLY_SUPPORTED", reasoning=""
            )


class TestUnsupportedVerdict:
    def test_happy_path(self):
        v = UnsupportedVerdict(
            arm_slot=3, verdict="UNSUPPORTED",
            reasoning="no source evidence",
            fabrication_hypothesis="default guess",
        )
        assert v.reasoning == "no source evidence"
        assert v.fabrication_hypothesis == "default guess"

    def test_requires_reasoning(self):
        with pytest.raises(ValidationError):
            UnsupportedVerdict(arm_slot=1, verdict="UNSUPPORTED",
                               fabrication_hypothesis="h")

    def test_requires_fabrication_hypothesis(self):
        with pytest.raises(ValidationError):
            UnsupportedVerdict(arm_slot=1, verdict="UNSUPPORTED",
                               reasoning="r")


# ── discriminated union ────────────────────────────────────────────


class TestPass2OutputDiscriminator:
    def _payload(self, verdicts):
        return {
            "paper_id": "p1",
            "field_name": "study_design",
            "arm_verdicts": verdicts,
            "overall_fabrication_detected": False,
        }

    def test_parses_all_three_variants(self):
        out = Pass2Output.model_validate(self._payload([
            {"arm_slot": 1, "verdict": "SUPPORTED"},
            {"arm_slot": 2, "verdict": "PARTIALLY_SUPPORTED", "reasoning": "r"},
            {"arm_slot": 3, "verdict": "UNSUPPORTED",
             "reasoning": "r", "fabrication_hypothesis": "h"},
        ]))
        assert [type(v).__name__ for v in out.arm_verdicts] == [
            "SupportedVerdict",
            "PartiallySupportedVerdict",
            "UnsupportedVerdict",
        ]

    def test_rejects_unknown_verdict_label(self):
        with pytest.raises(ValidationError):
            Pass2Output.model_validate(self._payload([
                {"arm_slot": 1, "verdict": "MAYBE"},
            ]))

    def test_unsupported_missing_hypothesis_rejected_via_union(self):
        with pytest.raises(ValidationError):
            Pass2Output.model_validate(self._payload([
                {"arm_slot": 1, "verdict": "UNSUPPORTED", "reasoning": "r"},
            ]))

    def test_partially_without_reasoning_rejected_via_union(self):
        with pytest.raises(ValidationError):
            Pass2Output.model_validate(self._payload([
                {"arm_slot": 1, "verdict": "PARTIALLY_SUPPORTED"},
            ]))

    def test_requires_at_least_two_verdicts(self):
        with pytest.raises(ValidationError):
            Pass2Output.model_validate(self._payload([
                {"arm_slot": 1, "verdict": "SUPPORTED"},
            ]))

    def test_model_json_schema_is_ollama_friendly(self):
        schema = Pass2Output.model_json_schema()
        # Must contain the discriminator and defs for each variant.
        assert "$defs" in schema
        assert "SupportedVerdict" in schema["$defs"]
        assert "PartiallySupportedVerdict" in schema["$defs"]
        assert "UnsupportedVerdict" in schema["$defs"]


# ── helpers ────────────────────────────────────────────────────────


def test_verdict_requires_reasoning_matches_spec():
    assert verdict_requires_reasoning("SUPPORTED") is False
    assert verdict_requires_reasoning("PARTIALLY_SUPPORTED") is True
    assert verdict_requires_reasoning("UNSUPPORTED") is True


def test_verdict_requires_hypothesis_matches_spec():
    assert verdict_requires_hypothesis("SUPPORTED") is False
    assert verdict_requires_hypothesis("PARTIALLY_SUPPORTED") is False
    assert verdict_requires_hypothesis("UNSUPPORTED") is True


# ── round-trip through JSON (model_validate_json, as used in run_pass2) ─


def test_round_trip_json_validate():
    data = {
        "paper_id": "p2",
        "field_name": "primary_outcome",
        "arm_verdicts": [
            {"arm_slot": 1, "verdict": "SUPPORTED"},
            {"arm_slot": 2, "verdict": "SUPPORTED"},
            {"arm_slot": 3, "verdict": "SUPPORTED"},
        ],
        "overall_fabrication_detected": False,
    }
    raw = json.dumps(data)
    out = Pass2Output.model_validate_json(raw)
    assert out.field_name == "primary_outcome"
    assert len(out.arm_verdicts) == 3
