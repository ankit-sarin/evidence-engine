"""Deduplicate citations from PubMed and OpenAlex."""

import logging
import re
from difflib import SequenceMatcher

from pydantic import BaseModel

from engine.search.models import Citation

logger = logging.getLogger(__name__)


# ── Result Model ─────────────────────────────────────────────────────


class DedupResult(BaseModel):
    """Result of deduplication across search sources."""

    unique_citations: list[Citation]
    duplicate_pairs: list[tuple[str, str]]  # (kept title, removed title)
    stats: dict


# ── Public API ───────────────────────────────────────────────────────


def deduplicate(
    pubmed_citations: list[Citation],
    openalex_citations: list[Citation],
) -> DedupResult:
    """Merge PubMed and OpenAlex citations, removing duplicates.

    PubMed records are preferred as primary; OpenAlex fills missing fields.
    """
    # Index PubMed records by DOI, PMID, and normalized title
    doi_index: dict[str, int] = {}
    pmid_index: dict[str, int] = {}
    title_index: list[tuple[str, int]] = []  # (normalized_title, index)

    unique: list[Citation] = []
    duplicate_pairs: list[tuple[str, str]] = []

    for cit in pubmed_citations:
        idx = len(unique)
        unique.append(cit)
        if cit.doi:
            doi_index[cit.doi.strip().lower()] = idx
        if cit.pmid:
            pmid_index[cit.pmid.strip()] = idx
        title_index.append((normalize_title(cit.title), idx))

    # Try to match each OpenAlex record against the PubMed set
    for oa_cit in openalex_citations:
        match_idx = _find_match(oa_cit, doi_index, pmid_index, title_index)

        if match_idx is not None:
            # Merge: fill gaps in PubMed record from OpenAlex
            unique[match_idx] = _merge(unique[match_idx], oa_cit)
            duplicate_pairs.append((unique[match_idx].title, oa_cit.title))
        else:
            # New unique citation — also index it for intra-OpenAlex dedup
            idx = len(unique)
            unique.append(oa_cit)
            if oa_cit.doi:
                doi_index[oa_cit.doi.strip().lower()] = idx
            if oa_cit.pmid:
                pmid_index[oa_cit.pmid.strip()] = idx
            title_index.append((normalize_title(oa_cit.title), idx))

    stats = {
        "pubmed_total": len(pubmed_citations),
        "openalex_total": len(openalex_citations),
        "duplicates_found": len(duplicate_pairs),
        "unique_total": len(unique),
    }

    logger.info(
        "Deduplication: %d PubMed + %d OpenAlex → %d unique (%d duplicates removed)",
        stats["pubmed_total"],
        stats["openalex_total"],
        stats["unique_total"],
        stats["duplicates_found"],
    )

    return DedupResult(
        unique_citations=unique,
        duplicate_pairs=duplicate_pairs,
        stats=stats,
    )


# ── Matching ─────────────────────────────────────────────────────────


def _find_match(
    cit: Citation,
    doi_index: dict[str, int],
    pmid_index: dict[str, int],
    title_index: list[tuple[str, int]],
) -> int | None:
    """Find a matching citation index, or None."""
    # Priority 1: DOI exact match
    if cit.doi:
        key = cit.doi.strip().lower()
        if key in doi_index:
            return doi_index[key]

    # Priority 2: PMID exact match
    if cit.pmid:
        key = cit.pmid.strip()
        if key in pmid_index:
            return pmid_index[key]

    # Priority 3: Fuzzy title match
    norm = normalize_title(cit.title)
    for existing_title, idx in title_index:
        if title_similarity(norm, existing_title) > 0.9:
            return idx

    return None


# ── Merging ──────────────────────────────────────────────────────────


def _merge(primary: Citation, secondary: Citation) -> Citation:
    """Fill missing fields in primary from secondary."""
    data = primary.model_dump()
    for field in ("doi", "pmid", "abstract", "journal", "year"):
        if data.get(field) is None and getattr(secondary, field) is not None:
            data[field] = getattr(secondary, field)
    if not data.get("authors") and secondary.authors:
        data["authors"] = secondary.authors
    return Citation.model_validate(data)


# ── Helpers ──────────────────────────────────────────────────────────


_PUNCT_RE = re.compile(r"[^\w\s]", re.UNICODE)
_SPACE_RE = re.compile(r"\s+")


def normalize_title(title: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace."""
    t = title.lower()
    t = _PUNCT_RE.sub("", t)
    t = _SPACE_RE.sub(" ", t).strip()
    return t


def title_similarity(t1: str, t2: str) -> float:
    """Fuzzy similarity between two normalized titles (0.0–1.0)."""
    return SequenceMatcher(None, t1, t2).ratio()
