"""Pydantic + dataclass schemas for the Paper 1 LLM-as-judge pipeline.

Pass 1 (pairwise rating) and Pass 2 (per-arm fabrication verification).

PairwiseRating (Pass 1) is a Pydantic v2 discriminated union on `rating`:
  - EquivalentPair has no disagreement_type field at all.
  - DisagreementPair requires disagreement_type.

Pass2ArmVerdict (Pass 2) is a discriminated union on `verdict`:
  - SupportedVerdict     — reasoning optional.
  - PartiallySupported   — reasoning required.
  - UnsupportedVerdict   — reasoning AND fabrication_hypothesis required.

The discriminator lets Ollama's grammar-constrained generation force
the correct shape per branch (fixes the smoke-1-style failure where
optional fields default to null under a permissive schema).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Literal, Optional, Union

from pydantic import BaseModel, Field, model_validator

from analysis.paper1.precheck import FieldType, PreCheckFlags

Level1Rating = Literal["EQUIVALENT", "PARTIAL", "DIVERGENT"]
Level2Type = Literal[
    "GRANULARITY",
    "SELECTION",
    "OMISSION",
    "CONTRADICTION",
    "FABRICATION",
]
FabricationRisk = Literal["low", "medium", "high"]


@dataclass(frozen=True)
class ArmOutput:
    """Single arm's extraction for one (paper_id, field) cell."""

    arm_name: str
    value: Optional[str]
    span: Optional[str]
    precheck_flags: PreCheckFlags


@dataclass(frozen=True)
class JudgeInput:
    """Everything the judge needs for one triple (or N-tuple)."""

    paper_id: str
    field_name: str
    field_type: FieldType
    field_definition: str
    field_valid_values: Optional[list[str]]
    arms: list[ArmOutput]


class _BasePair(BaseModel):
    """Shared fields for both pair variants."""

    slot_a: int = Field(ge=1)
    slot_b: int = Field(ge=1)
    rationale: str = Field(min_length=1, max_length=500)

    @model_validator(mode="after")
    def _slot_order(self) -> "_BasePair":
        if self.slot_a >= self.slot_b:
            raise ValueError("Pairs must be ordered (slot_a < slot_b)")
        return self


class EquivalentPair(_BasePair):
    """Two slots the judge rated as semantically equivalent.

    No disagreement_type field by design — attempting to set one
    raises ValidationError.
    """

    rating: Literal["EQUIVALENT"]


class DisagreementPair(_BasePair):
    """Two slots the judge rated as partial overlap or divergent.

    disagreement_type is required (grammar-enforced).
    """

    rating: Literal["PARTIAL", "DIVERGENT"]
    disagreement_type: Level2Type


# Discriminated-union alias. Use this for type annotations and
# for list[...] parameterization on Pass1Output.
PairwiseRating = Annotated[
    Union[EquivalentPair, DisagreementPair],
    Field(discriminator="rating"),
]


class Pass1Output(BaseModel):
    """Full Pass 1 judge output for one JudgeInput."""

    pairwise_ratings: list[PairwiseRating] = Field(min_length=1)
    fabrication_risk: FabricationRisk
    proposed_consensus: Optional[str] = None
    overall_rationale: str = Field(min_length=1, max_length=1000)


@dataclass(frozen=True)
class JudgeResult:
    """Wrapper combining de-randomized parsed output + audit trail."""

    paper_id: str
    field_name: str
    arm_permutation: list[str]
    pass1: Pass1Output
    prompt_hash: str
    judge_model_digest: str
    judge_model_name: str
    raw_response: str
    seed: int
    timestamp_iso: str


def pair_disagreement_type(pair) -> Optional[Level2Type]:
    """Narrowed accessor: return disagreement_type for DisagreementPair,
    None for EquivalentPair. Use everywhere downstream code needs the
    value without branching on isinstance inline.
    """
    if isinstance(pair, DisagreementPair):
        return pair.disagreement_type
    return None


# ═════════════════════════════════════════════════════════════════════
# Pass 2 — per-arm fabrication verification
# ═════════════════════════════════════════════════════════════════════


Verdict = Literal["SUPPORTED", "PARTIALLY_SUPPORTED", "UNSUPPORTED"]


class _BaseVerdict(BaseModel):
    """Shared fields across all three verdict shapes.

    arm_slot is a Literal over {1, 2, 3} so the JSON Schema emitted to
    Ollama carries ``"enum": [1, 2, 3]`` on each variant. llama.cpp's
    grammar backend respects this and prevents the model from producing
    slot indices outside the expected domain. Cross-element uniqueness
    (each slot appears at most once across the three array elements)
    cannot be reliably enforced by grammar — that remains the
    post-validator's job; see ``_validate_pass2_coverage``.
    """

    arm_slot: Literal[1, 2, 3]
    verification_span: Optional[str] = Field(default=None, max_length=2000)


class SupportedVerdict(_BaseVerdict):
    """Source grounds the arm's value — reasoning optional."""

    verdict: Literal["SUPPORTED"]
    reasoning: Optional[str] = Field(default=None, max_length=1000)


class PartiallySupportedVerdict(_BaseVerdict):
    """Source partially grounds the value — reasoning required."""

    verdict: Literal["PARTIALLY_SUPPORTED"]
    reasoning: str = Field(min_length=1, max_length=1000)


class UnsupportedVerdict(_BaseVerdict):
    """Source does not ground the value — reasoning AND hypothesis required."""

    verdict: Literal["UNSUPPORTED"]
    reasoning: str = Field(min_length=1, max_length=1000)
    fabrication_hypothesis: str = Field(min_length=1, max_length=1000)


Pass2ArmVerdict = Annotated[
    Union[SupportedVerdict, PartiallySupportedVerdict, UnsupportedVerdict],
    Field(discriminator="verdict"),
]


class Pass2Output(BaseModel):
    """Full Pass 2 judge output for one triple.

    arm_verdicts is pinned to exactly 3 elements — one per arm (local,
    openai_o4_mini_high, anthropic_sonnet_4_6). The emitted JSON Schema
    carries ``minItems: 3`` and ``maxItems: 3`` so llama.cpp's grammar
    backend prevents the model from producing 2-element or 4-element
    arrays (the specific pathology observed on paper 366 /
    primary_outcome_value, which emitted ``[slot=1, slot=2, slot=3,
    slot=3]`` twice with the same deterministic seed).
    """

    paper_id: str = Field(min_length=1)
    field_name: str = Field(min_length=1)
    arm_verdicts: list[Pass2ArmVerdict] = Field(min_length=3, max_length=3)
    overall_fabrication_detected: bool


@dataclass(frozen=True)
class Pass2Result:
    """Wrapper combining de-randomized Pass 2 output + audit trail."""

    paper_id: str
    field_name: str
    arm_permutation: list[str]
    pass2: Pass2Output
    pre_check_short_circuit_by_arm: dict[str, bool]
    prompt_hash: str
    judge_model_digest: str
    judge_model_name: str
    raw_response: str
    seed: int
    timestamp_iso: str
    source_text_windowed: bool
    source_text_tokens: int


def verdict_requires_reasoning(verdict: Verdict) -> bool:
    """PARTIALLY_SUPPORTED and UNSUPPORTED must carry reasoning."""
    return verdict != "SUPPORTED"


def verdict_requires_hypothesis(verdict: Verdict) -> bool:
    """Only UNSUPPORTED requires fabrication_hypothesis."""
    return verdict == "UNSUPPORTED"


__all__ = [
    "ArmOutput",
    "DisagreementPair",
    "EquivalentPair",
    "FabricationRisk",
    "JudgeInput",
    "JudgeResult",
    "Level1Rating",
    "Level2Type",
    "PairwiseRating",
    "Pass1Output",
    "PartiallySupportedVerdict",
    "Pass2ArmVerdict",
    "Pass2Output",
    "Pass2Result",
    "SupportedVerdict",
    "UnsupportedVerdict",
    "Verdict",
    "pair_disagreement_type",
    "verdict_requires_hypothesis",
    "verdict_requires_reasoning",
]
