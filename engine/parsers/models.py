"""Shared data models for parsers."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class ParsedDocument(BaseModel):
    """Result of parsing a PDF into Markdown."""

    paper_id: int
    source_pdf_path: str
    pdf_hash: str
    parsed_markdown: str
    parser_used: Literal["docling", "qwen2.5vl"]
    parsed_at: datetime
    version: int = Field(ge=1, default=1)
