"""PubMed search client using Biopython's Entrez module."""

import logging
import time
from xml.etree.ElementTree import Element

from Bio import Entrez, Medline

from engine.core.review_spec import ReviewSpec
from engine.search.models import Citation

logger = logging.getLogger(__name__)

Entrez.email = "ankit.sarin@ucdavis.edu"

_BATCH_SIZE = 500
_RATE_LIMIT_DELAY = 0.34  # seconds between requests (NCBI < 3 req/s)
_MAX_RETRIES = 3


# ── Public API ───────────────────────────────────────────────────────


def search_pubmed(spec: ReviewSpec) -> list[Citation]:
    """Search PubMed using the review spec's search strategy.

    Returns a deduplicated list of Citation objects.
    """
    query = _build_query(spec)
    logger.info("PubMed query: %s", query)

    # Phase 1: esearch to get PMIDs
    pmids = _esearch(query)
    total = len(pmids)
    if total == 0:
        logger.info("PubMed returned 0 results")
        return []

    logger.info("PubMed found %d PMIDs", total)

    # Phase 2: efetch in batches to get full records
    citations: list[Citation] = []
    for start in range(0, total, _BATCH_SIZE):
        batch_ids = pmids[start : start + _BATCH_SIZE]
        records = _efetch(batch_ids)
        for rec in records:
            citation = _parse_record(rec)
            if citation:
                citations.append(citation)
        logger.info("Fetched %d/%d citations from PubMed", len(citations), total)
        if start + _BATCH_SIZE < total:
            time.sleep(_RATE_LIMIT_DELAY)

    return citations


# ── Query Builder ────────────────────────────────────────────────────


def _build_query(spec: ReviewSpec) -> str:
    """Combine query terms with AND and apply date range filter."""
    terms = " AND ".join(spec.search_strategy.query_terms)
    start, end = spec.search_strategy.date_range
    date_filter = f"{start}:{end}[dp]"
    return f"({terms}) AND {date_filter}"


# ── Entrez Wrappers with Retry ───────────────────────────────────────


def _entrez_call(func, **kwargs):
    """Call an Entrez function with retries and rate limiting."""
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            time.sleep(_RATE_LIMIT_DELAY)
            handle = func(**kwargs)
            return handle
        except Exception as exc:
            if attempt == _MAX_RETRIES:
                raise
            wait = 2**attempt
            logger.warning(
                "Entrez call failed (attempt %d/%d): %s — retrying in %ds",
                attempt,
                _MAX_RETRIES,
                exc,
                wait,
            )
            time.sleep(wait)


def _esearch(query: str) -> list[str]:
    """Run ESearch and return all matching PMIDs."""
    handle = _entrez_call(
        Entrez.esearch,
        db="pubmed",
        term=query,
        retmax=0,
        usehistory="y",
    )
    result = Entrez.read(handle)
    handle.close()

    total = int(result["Count"])
    if total == 0:
        return []

    web_env = result["WebEnv"]
    query_key = result["QueryKey"]

    pmids: list[str] = []
    for start in range(0, total, _BATCH_SIZE):
        handle = _entrez_call(
            Entrez.esearch,
            db="pubmed",
            term=query,
            retmax=_BATCH_SIZE,
            retstart=start,
            WebEnv=web_env,
            query_key=query_key,
        )
        batch = Entrez.read(handle)
        handle.close()
        pmids.extend(batch["IdList"])
        if start + _BATCH_SIZE < total:
            time.sleep(_RATE_LIMIT_DELAY)

    return pmids


def _efetch(pmids: list[str]) -> list[dict]:
    """Fetch MEDLINE records for a batch of PMIDs."""
    handle = _entrez_call(
        Entrez.efetch,
        db="pubmed",
        id=",".join(pmids),
        rettype="medline",
        retmode="text",
    )
    records = list(Medline.parse(handle))
    handle.close()
    return records


# ── Record Parser ────────────────────────────────────────────────────


def _parse_record(rec: dict) -> Citation | None:
    """Convert a MEDLINE record dict into a Citation."""
    title = rec.get("TI")
    if not title:
        return None

    # Extract year from Date of Publication (DP) field, e.g. "2023 Jan"
    year = None
    dp = rec.get("DP", "")
    if dp:
        try:
            year = int(dp[:4])
        except (ValueError, IndexError):
            pass

    # DOI is in Article Identifier (AID) field, tagged with [doi]
    doi = None
    for aid in rec.get("AID", []):
        if aid.endswith("[doi]"):
            doi = aid.replace(" [doi]", "")
            break

    return Citation(
        pmid=rec.get("PMID"),
        doi=doi,
        title=title,
        abstract=rec.get("AB"),
        authors=rec.get("AU", []),
        journal=rec.get("JT"),
        year=year,
        source="pubmed",
        raw_data=rec,
    )
