"""Tests for OpenAlex search client (live queries)."""

from pathlib import Path

import pytest

from engine.core.review_spec import load_review_spec
from engine.search.models import Citation
from engine.search.openalex import search_openalex, reconstruct_abstract

SPEC_PATH = Path(__file__).resolve().parent.parent / "review_specs" / "surgical_autonomy_v1.yaml"


@pytest.fixture(scope="module")
def spec():
    return load_review_spec(SPEC_PATH)


@pytest.fixture(scope="module")
def openalex_results(spec):
    """Run a single live OpenAlex query, shared across tests in this module."""
    return search_openalex(spec)


# ── Abstract Reconstruction ──────────────────────────────────────────


def test_reconstruct_abstract_from_inverted_index():
    inv_index = {
        "This": [0],
        "is": [1],
        "a": [2, 5],
        "test": [3],
        "of": [4],
        "function": [6],
    }
    result = reconstruct_abstract(inv_index)
    assert result == "This is a test of a function"


def test_reconstruct_abstract_none():
    assert reconstruct_abstract(None) is None


def test_reconstruct_abstract_empty():
    assert reconstruct_abstract({}) is None


# ── Live Search ──────────────────────────────────────────────────────


@pytest.mark.network
def test_search_returns_citations(openalex_results):
    assert len(openalex_results) > 0
    assert all(isinstance(c, Citation) for c in openalex_results)


@pytest.mark.network
def test_citations_have_required_fields(openalex_results):
    for c in openalex_results[:10]:
        assert c.title
        assert c.source == "openalex"


@pytest.mark.network
def test_citations_have_years_in_range(openalex_results):
    for c in openalex_results[:20]:
        if c.year:
            assert 2010 <= c.year <= 2025, f"Year {c.year} out of range"


@pytest.mark.network
def test_citations_have_authors(openalex_results):
    with_authors = [c for c in openalex_results if c.authors]
    ratio = len(with_authors) / len(openalex_results)
    assert ratio > 0.8, f"Only {ratio:.0%} of citations have authors"
