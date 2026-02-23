"""OpenAlex search client using the pyalex library."""

import logging
import time

import pyalex
from pyalex import Works, invert_abstract

from engine.core.review_spec import ReviewSpec
from engine.search.models import Citation

logger = logging.getLogger(__name__)

pyalex.config.email = "ankit.sarin@ucdavis.edu"

_PER_PAGE = 200
_MAX_RETRIES = 3


# ── Public API ───────────────────────────────────────────────────────


def search_openalex(spec: ReviewSpec) -> list[Citation]:
    """Search OpenAlex using the review spec's search strategy.

    Returns a list of Citation objects.
    """
    query_text = " ".join(spec.search_strategy.query_terms)
    start_year, end_year = spec.search_strategy.date_range

    logger.info("OpenAlex query: %s (%d–%d)", query_text, start_year, end_year)

    works_query = (
        Works()
        .search(query_text)
        .filter(
            publication_year=f"{start_year}-{end_year}",
            type="article",
        )
    )

    citations: list[Citation] = []

    for page in _paginate_with_retry(works_query):
        for work in page:
            citation = _parse_work(work)
            if citation:
                citations.append(citation)
        logger.info("Fetched %d citations from OpenAlex so far...", len(citations))

    logger.info("OpenAlex total: %d citations", len(citations))
    return citations


# ── Pagination with Retry ────────────────────────────────────────────


def _paginate_with_retry(works_query):
    """Yield pages from cursor pagination with retry on HTTP errors."""
    paginator = works_query.paginate(per_page=_PER_PAGE)

    while True:
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                page = next(paginator)
                break
            except StopIteration:
                return
            except Exception as exc:
                if attempt == _MAX_RETRIES:
                    raise
                wait = 2**attempt
                logger.warning(
                    "OpenAlex request failed (attempt %d/%d): %s — retrying in %ds",
                    attempt,
                    _MAX_RETRIES,
                    exc,
                    wait,
                )
                time.sleep(wait)
        else:
            return  # pragma: no cover
        yield page


# ── Abstract Reconstruction ──────────────────────────────────────────


def reconstruct_abstract(inverted_index: dict | None) -> str | None:
    """Reassemble full abstract text from an OpenAlex inverted index.

    OpenAlex stores abstracts as {word: [position, ...]} dicts.
    Returns None if the inverted index is empty or None.
    """
    if not inverted_index:
        return None
    return invert_abstract(inverted_index)


# ── Work → Citation ──────────────────────────────────────────────────


def _parse_work(work: dict) -> Citation | None:
    """Convert an OpenAlex Work dict into a Citation."""
    title = work.get("title")
    if not title:
        return None

    # Extract PMID from ids dict (e.g. "https://pubmed.ncbi.nlm.nih.gov/12345678")
    pmid = None
    ids = work.get("ids") or {}
    pmid_url = ids.get("pmid") or ""
    if pmid_url:
        pmid = pmid_url.replace("https://pubmed.ncbi.nlm.nih.gov/", "")

    # DOI — strip prefix
    doi = work.get("doi")
    if doi and doi.startswith("https://doi.org/"):
        doi = doi[len("https://doi.org/"):]

    # Authors
    authors = []
    for authorship in work.get("authorships") or []:
        author = authorship.get("author") or {}
        name = author.get("display_name")
        if name:
            authors.append(name)

    # Journal
    journal = None
    primary = work.get("primary_location") or {}
    source = primary.get("source") or {}
    journal = source.get("display_name")

    # Abstract
    abstract = reconstruct_abstract(work.get("abstract_inverted_index"))

    return Citation(
        pmid=pmid,
        doi=doi,
        title=title,
        abstract=abstract,
        authors=authors,
        journal=journal,
        year=work.get("publication_year"),
        source="openalex",
        raw_data=work,
    )
