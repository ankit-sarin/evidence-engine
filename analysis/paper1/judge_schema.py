"""Pydantic + dataclass schemas for the Paper 1 LLM-as-judge pipeline.

Pass 1 only. Pass 2 (fabrication verification) will ship separately.

PairwiseRating is a Pydantic v2 discriminated union on `rating`:
  - EquivalentPair has no disagreement_type field at all.
  - DisagreementPair requires disagreement_type.
The discriminator lets Ollama's grammar-constrained generation
force the correct shape per branch (fixing the smoke-1 failure
where disagreement_type defaulted to null under the optional
schema).
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
    "pair_disagreement_type",
]
