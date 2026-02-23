"""Tests for PubMed search client (live queries against NCBI)."""

from pathlib import Path

import pytest

from engine.core.review_spec import load_review_spec
from engine.search.models import Citation
from engine.search.pubmed import search_pubmed, _build_query

SPEC_PATH = Path(__file__).resolve().parent.parent / "review_specs" / "surgical_autonomy_v1.yaml"


@pytest.fixture(scope="module")
def spec():
    return load_review_spec(SPEC_PATH)


@pytest.fixture(scope="module")
def pubmed_results(spec):
    """Run a single live PubMed query, shared across tests in this module."""
    return search_pubmed(spec)


# ── Query Construction ───────────────────────────────────────────────


def test_build_query(spec):
    query = _build_query(spec)
    assert "AND" in query
    assert "2010:2025[dp]" in query


# ── Live Search ──────────────────────────────────────────────────────


@pytest.mark.network
def test_search_returns_citations(pubmed_results):
    assert len(pubmed_results) > 0
    assert all(isinstance(c, Citation) for c in pubmed_results)


@pytest.mark.network
def test_citations_have_required_fields(pubmed_results):
    for c in pubmed_results[:10]:  # spot-check first 10
        assert c.title
        assert c.source == "pubmed"
        assert c.pmid is not None


@pytest.mark.network
def test_citations_have_abstracts(pubmed_results):
    """Most PubMed results should have abstracts."""
    with_abstract = [c for c in pubmed_results if c.abstract]
    ratio = len(with_abstract) / len(pubmed_results)
    assert ratio > 0.5, f"Only {ratio:.0%} of citations have abstracts"


@pytest.mark.network
def test_rate_limiting_no_errors(pubmed_results):
    """If we got here without exceptions, rate limiting worked."""
    assert len(pubmed_results) > 0
