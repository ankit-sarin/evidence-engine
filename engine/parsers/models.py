"""Shared data models for parsers."""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class ParseAttempt(BaseModel):
    """One parser run within a single parse of one PDF, judged or skipped.

    A skipped attempt (`skipped_reason` set) carries no metrics: it records that
    the cascade declined to run a parser and why, which is a different fact from
    a parser that ran and failed. Both are kept, because "vision was never tried"
    and "vision was tried and did not help" call for different next actions.
    """

    attempt_index: int = Field(ge=1)
    parser_used: Literal["docling", "docling_sanitized", "docling_ocr",
                         "docling_ocr_sanitized", "pymupdf", "qwen2.5vl"]
    passed: bool = False
    failures: list[tuple[str, Any, Any]] = Field(default_factory=list)
    metrics: dict[str, Any] = Field(default_factory=dict)
    elapsed_s: float = 0.0
    accepted: bool = False
    skipped_reason: str | None = None


class ParsedDocument(BaseModel):
    """Result of parsing a PDF into Markdown."""

    paper_id: int
    source_pdf_path: str
    pdf_hash: str
    parsed_markdown: str
    parser_used: Literal["docling", "docling_sanitized", "docling_ocr",
                         "docling_ocr_sanitized", "pymupdf", "qwen2.5vl"]
    parsed_at: datetime
    version: int = Field(ge=1, default=1)

    # ── parse-quality gate (PARSE-GATE-02) ──
    # Optional and defaulted: every pre-gate construction site stays valid, and a
    # same-hash short-circuit legitimately has no attempts to report.
    verdict: dict[str, Any] | None = None
    attempts: list[ParseAttempt] = Field(default_factory=list)
    accepted_parser: str | None = None

    @property
    def quality_passed(self) -> bool | None:
        """True/False once the gate has run; None when it did not (short-circuit)."""
        return None if self.verdict is None else bool(self.verdict.get("passed"))
