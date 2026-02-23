"""Review Spec: YAML parser, Pydantic models, and protocol hashing."""

import hashlib
import json
from datetime import date
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, Field, field_validator


# ── Extraction Schema ────────────────────────────────────────────────


class ExtractionField(BaseModel):
    """Single field to extract from a study's full text."""

    name: str
    description: str
    type: str = Field(
        description="Data type: str, int, float, bool, list[str], enum, etc."
    )
    tier: int = Field(ge=1, le=3, description="1=required, 2=important, 3=optional")
    enum_values: Optional[list[str]] = Field(
        default=None, description="Allowed values when type is 'enum'"
    )


class ExtractionSchema(BaseModel):
    """Full extraction schema organized by tier."""

    fields: list[ExtractionField]

    @field_validator("fields")
    @classmethod
    def at_least_one_tier1(cls, v: list[ExtractionField]) -> list[ExtractionField]:
        if not any(f.tier == 1 for f in v):
            raise ValueError("Extraction schema must have at least one tier-1 field")
        return v

    def fields_by_tier(self, tier: int) -> list[ExtractionField]:
        return [f for f in self.fields if f.tier == tier]


# ── PICO ─────────────────────────────────────────────────────────────


class PICO(BaseModel):
    """Population, Intervention, Comparator, Outcomes."""

    population: str
    intervention: str
    comparator: str
    outcomes: list[str]


# ── Search Strategy ──────────────────────────────────────────────────


class SearchStrategy(BaseModel):
    """Databases and query parameters for literature search."""

    databases: list[str]
    query_terms: list[str]
    date_range: list[int] = Field(
        min_length=2, max_length=2, description="[start_year, end_year]"
    )

    @field_validator("date_range")
    @classmethod
    def valid_date_range(cls, v: list[int]) -> list[int]:
        if v[0] > v[1]:
            raise ValueError(
                f"Start year ({v[0]}) must be <= end year ({v[1]})"
            )
        return v


# ── Screening Criteria ───────────────────────────────────────────────


class ScreeningCriteria(BaseModel):
    """Inclusion/exclusion rules for title-abstract screening."""

    inclusion: list[str]
    exclusion: list[str]


# ── Review Spec (top-level) ──────────────────────────────────────────


class ReviewSpec(BaseModel):
    """Top-level model for a systematic review specification."""

    title: str
    version: str
    authors: list[str]
    date: date
    prospero_id: Optional[str] = None
    pico: PICO
    search_strategy: SearchStrategy
    screening_criteria: ScreeningCriteria
    extraction_schema: ExtractionSchema

    # ── Protocol hashing ─────────────────────────────────────────

    def screening_hash(self) -> str:
        """SHA-256 of the screening criteria section (canonical JSON)."""
        return _canonical_hash(self.screening_criteria.model_dump())

    def extraction_hash(self) -> str:
        """SHA-256 of the extraction schema section (canonical JSON)."""
        return _canonical_hash(self.extraction_schema.model_dump())


# ── Helpers ──────────────────────────────────────────────────────────


def _canonical_hash(data: dict) -> str:
    """Deterministic SHA-256 hash of a dict via sorted-key JSON."""
    blob = json.dumps(data, sort_keys=True, default=str).encode()
    return hashlib.sha256(blob).hexdigest()


def load_review_spec(path: str | Path) -> ReviewSpec:
    """Load a YAML Review Spec from disk and return a validated model."""
    path = Path(path)
    with open(path) as f:
        raw = yaml.safe_load(f)
    return ReviewSpec.model_validate(raw)
