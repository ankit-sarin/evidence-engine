"""Review Spec: YAML parser, Pydantic models, and protocol hashing."""

import hashlib
import json
from datetime import date
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator


# ── Extraction Schema ────────────────────────────────────────────────


class ExtractionField(BaseModel):
    """Single field to extract from a study's full text."""

    name: str
    description: str
    type: str = Field(
        description="Data type: str, int, float, bool, list[str], enum, etc."
    )
    tier: int = Field(ge=1, le=4, description="1=explicit, 2=interpretive, 3=numeric, 4=judgment")
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


class ScreeningModels(BaseModel):
    """Model configuration for dual-model screening."""

    primary: str = Field(default="qwen3:8b", description="Fast high-recall primary screener")
    verification: str = Field(default="qwen3:32b", description="Larger model for verification of includes")


class FTScreeningModels(BaseModel):
    """Model configuration for full-text screening."""

    primary: str = Field(default="qwen3.5:27b", description="Full-text primary screener")
    verifier: str = Field(default="gemma3:27b", description="Full-text verification model")
    think: bool = Field(default=False, description="Enable thinking mode (slow, not recommended)")
    temperature: float = Field(default=0.0, ge=0.0, le=1.0)


class ScreeningCriteria(BaseModel):
    """Inclusion/exclusion rules for title-abstract screening."""

    inclusion: list[str]
    exclusion: list[str]


# ── Specialty Scope ──────────────────────────────────────────────────


class SpecialtyScope(BaseModel):
    """Surgical specialty inclusion/exclusion scope for screening."""

    included: list[str] = Field(min_length=1)
    excluded: list[str] = Field(min_length=1)
    notes: Optional[str] = None

    def format_for_prompt(self) -> str:
        """Format specialty scope as a string for inclusion in screening prompts."""
        lines = ["SPECIALTY SCOPE:"]
        lines.append("  Included specialties:")
        for s in self.included:
            lines.append(f"    - {s}")
        lines.append("  Excluded specialties:")
        for s in self.excluded:
            lines.append(f"    - {s}")
        if self.notes:
            lines.append(f"  Notes: {self.notes.strip()}")
        return "\n".join(lines)


# ── PDF Quality Check ───────────────────────────────────────────────


class PDFQualityCheck(BaseModel):
    """Configuration for AI-based PDF quality classification."""

    enabled: bool = Field(default=True, description="Enable PDF quality check")
    ai_model: str = Field(
        default="qwen2.5vl:7b",
        description="Ollama vision model for first-page classification",
    )
    dpi: int = Field(
        default=150, ge=72, le=600,
        description="Render DPI for first-page image",
    )
    timeout: int = Field(
        default=120, ge=10, le=600,
        description="Ollama request timeout in seconds",
    )
    exclude_reasons: list[str] = Field(
        default=["NON_ENGLISH", "NOT_MANUSCRIPT", "INACCESSIBLE", "OTHER"],
        description="Valid exclusion reason codes for disposition",
    )


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
    screening_models: ScreeningModels = Field(default_factory=ScreeningModels)
    ft_screening_models: FTScreeningModels = Field(default_factory=FTScreeningModels)
    screening_criteria: ScreeningCriteria
    extraction_schema: ExtractionSchema
    specialty_scope: Optional[SpecialtyScope] = Field(
        default=None,
        description="Surgical specialty inclusion/exclusion scope. If absent, no specialty filtering.",
    )
    low_yield_threshold: int = Field(
        default=4,
        ge=1,
        description=(
            "Minimum number of non-null extracted fields required. Papers below "
            "this threshold are flagged as LOW_YIELD for PI review."
        ),
    )
    auditor_model: Optional[str] = Field(
        default=None,
        description="Ollama model for extraction audit. Defaults to gemma3:27b if not set.",
    )
    unpaywall_email: Optional[str] = Field(
        default=None,
        description="Email for Unpaywall API queries (required for OA checking).",
    )
    institutional_proxy_pattern: Optional[str] = Field(
        default=None,
        description=(
            "Institutional proxy URL pattern with {doi} placeholder for manual downloads. "
            "Proxy URL patterns vary by institution (e.g., libproxy, EZproxy) and typically "
            "require browser-level VPN or SSO authentication to work. The manual download "
            "list uses this as one of several link options alongside Google Scholar, Direct "
            "DOI, and PubMed."
        ),
    )
    pdf_quality_check: PDFQualityCheck = Field(
        default_factory=PDFQualityCheck,
        description="Configuration for AI-based PDF quality classification.",
    )

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
