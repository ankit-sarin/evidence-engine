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

    Two-phase strategy:
      Phase 1 — Exact match on DOI, then PMID (O(n), handles ~95% of dupes).
      Phase 2 — Fuzzy title match on unresolved records only (small set).

    PubMed records are preferred as primary; OpenAlex fills missing fields.
    """
    doi_index: dict[str, int] = {}
    pmid_index: dict[str, int] = {}
    title_norm_index: dict[str, int] = {}  # exact normalized title → index

    unique: list[Citation] = []
    duplicate_pairs: list[tuple[str, str]] = []

    # Seed with all PubMed records
    for cit in pubmed_citations:
        idx = len(unique)
        unique.append(cit)
        if cit.doi:
            doi_index[cit.doi.strip().lower()] = idx
        if cit.pmid:
            pmid_index[cit.pmid.strip()] = idx
        title_norm_index[normalize_title(cit.title)] = idx

    # Phase 1: exact DOI/PMID/title match for each OpenAlex record
    unresolved: list[Citation] = []

    for oa_cit in openalex_citations:
        match_idx = _exact_match(oa_cit, doi_index, pmid_index, title_norm_index)

        if match_idx is not None:
            unique[match_idx] = _merge(unique[match_idx], oa_cit)
            duplicate_pairs.append((unique[match_idx].title, oa_cit.title))
        else:
            unresolved.append(oa_cit)

    # Phase 2: fuzzy title match on unresolved records only
    # Build a list of normalized titles for fuzzy comparison
    fuzzy_titles: list[tuple[str, int]] = [
        (norm, idx) for norm, idx in title_norm_index.items()
    ]

    still_unresolved: list[Citation] = []
    for oa_cit in unresolved:
        norm = normalize_title(oa_cit.title)
        match_idx = _fuzzy_title_match(norm, fuzzy_titles)

        if match_idx is not None:
            unique[match_idx] = _merge(unique[match_idx], oa_cit)
            duplicate_pairs.append((unique[match_idx].title, oa_cit.title))
        else:
            still_unresolved.append(oa_cit)

    # Add genuinely new records and index them for intra-OpenAlex dedup
    for oa_cit in still_unresolved:
        norm = normalize_title(oa_cit.title)

        # Check against other newly-added OpenAlex records (exact + fuzzy)
        match_idx = _exact_match(oa_cit, doi_index, pmid_index, title_norm_index)
        if match_idx is None:
            # Build fresh fuzzy list from all titles added so far
            new_fuzzy = [(n, i) for n, i in title_norm_index.items()]
            match_idx = _fuzzy_title_match(norm, new_fuzzy)

        if match_idx is not None:
            unique[match_idx] = _merge(unique[match_idx], oa_cit)
            duplicate_pairs.append((unique[match_idx].title, oa_cit.title))
        else:
            idx = len(unique)
            unique.append(oa_cit)
            if oa_cit.doi:
                doi_index[oa_cit.doi.strip().lower()] = idx
            if oa_cit.pmid:
                pmid_index[oa_cit.pmid.strip()] = idx
            title_norm_index[norm] = idx

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


def _exact_match(
    cit: Citation,
    doi_index: dict[str, int],
    pmid_index: dict[str, int],
    title_norm_index: dict[str, int],
) -> int | None:
    """O(1) match on DOI, PMID, or exact normalized title."""
    if cit.doi:
        key = cit.doi.strip().lower()
        if key in doi_index:
            return doi_index[key]

    if cit.pmid:
        key = cit.pmid.strip()
        if key in pmid_index:
            return pmid_index[key]

    norm = normalize_title(cit.title)
    if norm in title_norm_index:
        return title_norm_index[norm]

    return None


def _fuzzy_title_match(
    norm_title: str,
    title_list: list[tuple[str, int]],
) -> int | None:
    """Fuzzy title match against a list of (normalized_title, index) pairs."""
    for existing_title, idx in title_list:
        if title_similarity(norm_title, existing_title) > 0.9:
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
