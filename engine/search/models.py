"""Shared data models for search modules."""

from typing import Literal, Optional

from pydantic import BaseModel, Field


class Citation(BaseModel):
    """A single literature citation returned by a search source."""

    pmid: Optional[str] = None
    doi: Optional[str] = None
    title: str
    abstract: Optional[str] = None
    authors: list[str] = Field(default_factory=list)
    journal: Optional[str] = None
    year: Optional[int] = None
    source: Literal["pubmed", "openalex"]
    raw_data: dict = Field(default_factory=dict)
