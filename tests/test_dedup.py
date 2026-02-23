"""Tests for citation deduplication."""

import pytest

from engine.search.dedup import (
    DedupResult,
    deduplicate,
    normalize_title,
    title_similarity,
)
from engine.search.models import Citation


# ── Factories ────────────────────────────────────────────────────────


def _pm(title="Study A", pmid="111", doi="10.1000/a", abstract="PubMed abstract", **kw):
    return Citation(title=title, pmid=pmid, doi=doi, abstract=abstract, source="pubmed", **kw)


def _oa(title="Study A", pmid="111", doi="10.1000/a", abstract="OpenAlex abstract", **kw):
    return Citation(title=title, pmid=pmid, doi=doi, abstract=abstract, source="openalex", **kw)


# ── DOI Match ────────────────────────────────────────────────────────


def test_doi_exact_match():
    pm = [_pm(doi="10.1000/abc")]
    oa = [_oa(doi="10.1000/abc", pmid=None)]
    result = deduplicate(pm, oa)
    assert result.stats["duplicates_found"] == 1
    assert result.stats["unique_total"] == 1


def test_doi_case_insensitive():
    pm = [_pm(doi="10.1000/ABC")]
    oa = [_oa(doi="10.1000/abc", pmid=None)]
    result = deduplicate(pm, oa)
    assert result.stats["duplicates_found"] == 1


# ── PMID Match ───────────────────────────────────────────────────────


def test_pmid_exact_match():
    pm = [_pm(pmid="12345", doi=None)]
    oa = [_oa(pmid="12345", doi=None)]
    result = deduplicate(pm, oa)
    assert result.stats["duplicates_found"] == 1
    assert result.stats["unique_total"] == 1


# ── Fuzzy Title Match ────────────────────────────────────────────────


def test_fuzzy_title_trailing_period():
    pm = [_pm(title="Autonomous suturing with the STAR robot", doi=None, pmid=None)]
    oa = [_oa(title="Autonomous suturing with the STAR robot.", doi=None, pmid=None)]
    result = deduplicate(pm, oa)
    assert result.stats["duplicates_found"] == 1


def test_fuzzy_title_different_case():
    pm = [_pm(title="A Study on Robotic Surgery", doi=None, pmid=None)]
    oa = [_oa(title="a study on robotic surgery", doi=None, pmid=None)]
    result = deduplicate(pm, oa)
    assert result.stats["duplicates_found"] == 1


# ── Non-Duplicates Preserved ─────────────────────────────────────────


def test_non_duplicates_preserved():
    pm = [_pm(title="Study Alpha", doi="10.1000/a", pmid="1")]
    oa = [_oa(title="Completely Different Study", doi="10.1000/b", pmid="2")]
    result = deduplicate(pm, oa)
    assert result.stats["duplicates_found"] == 0
    assert result.stats["unique_total"] == 2


# ── Merge: PubMed Preferred, OpenAlex Fills Gaps ─────────────────────


def test_merge_pubmed_preferred():
    pm = [_pm(abstract="PubMed version", journal="PM Journal")]
    oa = [_oa(abstract="OA version", journal="OA Journal")]
    result = deduplicate(pm, oa)
    merged = result.unique_citations[0]
    assert merged.abstract == "PubMed version"
    assert merged.journal == "PM Journal"
    assert merged.source == "pubmed"


def test_merge_fills_missing_fields():
    pm = [_pm(abstract=None, journal=None, year=None)]
    oa = [_oa(abstract="OA abstract", journal="OA Journal", year=2023)]
    result = deduplicate(pm, oa)
    merged = result.unique_citations[0]
    assert merged.abstract == "OA abstract"
    assert merged.journal == "OA Journal"
    assert merged.year == 2023


# ── Stats ────────────────────────────────────────────────────────────


def test_stats_correct():
    pm = [_pm(title=f"PM {i}", doi=f"10.1/{i}", pmid=str(i)) for i in range(3)]
    oa = [
        _oa(title="PM 0", doi="10.1/0", pmid="0"),   # dup
        _oa(title="PM 2", doi="10.1/2", pmid="2"),   # dup
        _oa(title="New OA", doi="10.2/x", pmid="99"),  # unique
    ]
    result = deduplicate(pm, oa)
    assert result.stats == {
        "pubmed_total": 3,
        "openalex_total": 3,
        "duplicates_found": 2,
        "unique_total": 4,
    }


# ── Empty Inputs ─────────────────────────────────────────────────────


def test_empty_pubmed():
    oa = [_oa(title="Only OA")]
    result = deduplicate([], oa)
    assert result.stats["unique_total"] == 1


def test_empty_openalex():
    pm = [_pm(title="Only PM")]
    result = deduplicate(pm, [])
    assert result.stats["unique_total"] == 1


def test_both_empty():
    result = deduplicate([], [])
    assert result.stats["unique_total"] == 0
    assert result.stats["duplicates_found"] == 0


# ── Helpers ──────────────────────────────────────────────────────────


def test_normalize_title():
    assert normalize_title("Hello, World!  ") == "hello world"
    assert normalize_title("A.B.C") == "abc"


def test_title_similarity_identical():
    assert title_similarity("hello world", "hello world") == 1.0


def test_title_similarity_different():
    assert title_similarity("hello world", "goodbye moon") < 0.5
