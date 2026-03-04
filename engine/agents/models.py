"""Shared data models for extraction and audit agents."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class EvidenceSpan(BaseModel):
    """A single extracted field with its supporting evidence."""

    field_name: str
    value: str
    source_snippet: str = Field(
        description="Verbatim quote from paper supporting this value (1-3 sentences)"
    )
    confidence: float = Field(ge=0.0, le=1.0)
    tier: int = Field(ge=1, le=4)

    @field_validator("confidence", mode="before")
    @classmethod
    def clamp_confidence(cls, v):
        """Clamp out-of-range confidence values (e.g. -1 from DeepSeek-R1 for NOT_FOUND)."""
        if isinstance(v, (int, float)):
            return max(0.0, min(1.0, float(v)))
        return v


class ExtractionResult(BaseModel):
    """Full structured extraction from a single paper."""

    paper_id: int
    fields: list[EvidenceSpan]
    reasoning_trace: str
    model: str
    extraction_schema_hash: str
    extracted_at: datetime


class ExtractionOutput(BaseModel):
    """Schema used for Ollama structured output (Pass 2 only)."""

    fields: list[EvidenceSpan]
