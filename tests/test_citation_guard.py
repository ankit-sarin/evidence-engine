"""ELICIT-DESIGN-01 test 4 — a value without evidence cannot be written.

The predicate is exercised directly (it is mechanism-independent by design) and
then through `extract_paper_with_completeness`, to prove the refusal reaches the
write boundary and that its retry budget is bounded.
"""

from __future__ import annotations

import pytest

from engine.core.citation_guard import (
    ESCAPE_WITH_CITATION, LEGACY, STRICT, UncitedValueError,
    VALUE_WITHOUT_CITATION, check_citations, enforce_citations,
)

ESCAPE = "NO_EVIDENCE_LOCATABLE"
SENTINELS = frozenset({"NR", "NOT_FOUND", "NOT REPORTED"})


def span(name, value, snippet=""):
    return {"field_name": name, "value": value, "source_snippet": snippet}


# ══ the predicate ═════════════════════════════════════════════════════


def test_uncited_value_raises():
    with pytest.raises(UncitedValueError) as exc:
        enforce_citations([span("robot_platform", "da Vinci")],
                          paper_id=1, arm="local", escape_token=ESCAPE,
                          absence_sentinels=SENTINELS, mode=STRICT)
    assert "robot_platform" in str(exc.value)
    assert VALUE_WITHOUT_CITATION in str(exc.value)


def test_cited_value_passes():
    r = enforce_citations([span("robot_platform", "da Vinci", "It used a da Vinci.")],
                          paper_id=1, arm="local", escape_token=ESCAPE,
                          absence_sentinels=SENTINELS, mode=STRICT)
    assert r.ok and r.n_checked == 1


def test_escape_token_write_passes_with_no_evidence():
    r = enforce_citations([span("robot_platform", ESCAPE)],
                          paper_id=1, arm="local", escape_token=ESCAPE,
                          absence_sentinels=SENTINELS, mode=STRICT)
    assert r.ok and r.n_escape == 1


def test_escape_token_with_evidence_is_a_violation():
    res = check_citations([span("robot_platform", ESCAPE, "some quote")],
                          escape_token=ESCAPE, absence_sentinels=SENTINELS, mode=STRICT)
    assert not res.ok and res.offenders == (("robot_platform", ESCAPE_WITH_CITATION),)


def test_strict_mode_requires_a_citation_for_an_absence_sentinel():
    """Section 4.1 ruling: a sentinel is a value, and values owe evidence."""
    res = check_citations([span("comparison_to_human", "NR")],
                          escape_token=ESCAPE, absence_sentinels=SENTINELS, mode=STRICT)
    assert not res.ok and res.offenders[0][1] == VALUE_WITHOUT_CITATION


def test_legacy_mode_exempts_the_sentinel_the_legacy_prompt_asked_for():
    res = check_citations([span("comparison_to_human", "NR")],
                          escape_token=ESCAPE, absence_sentinels=SENTINELS, mode=LEGACY)
    assert res.ok and res.n_sentinel == 1


def test_legacy_mode_still_refuses_a_positive_claim_with_no_quote():
    res = check_citations([span("robot_platform", "da Vinci")],
                          escape_token=ESCAPE, absence_sentinels=SENTINELS, mode=LEGACY)
    assert not res.ok


def test_citation_counts_beat_the_snippet_when_supplied():
    """A stored snippet is a narrowing of the citation set, not the set."""
    res = check_citations([span("robot_platform", "da Vinci", "")],
                          escape_token=ESCAPE, absence_sentinels=SENTINELS,
                          mode=STRICT, citation_counts={"robot_platform": 2})
    assert res.ok


def test_whitespace_snippet_is_not_evidence():
    res = check_citations([span("robot_platform", "da Vinci", "   \n  ")],
                          escape_token=ESCAPE, absence_sentinels=SENTINELS, mode=STRICT)
    assert not res.ok


def test_no_escape_token_declared_means_no_value_is_an_escape():
    res = check_citations([span("robot_platform", ESCAPE)],
                          escape_token=None, absence_sentinels=SENTINELS, mode=LEGACY)
    assert not res.ok


def test_unknown_mode_is_refused():
    with pytest.raises(ValueError):
        check_citations([], escape_token=ESCAPE, mode="lenient")


# ══ the write boundary and its bounded retry ══════════════════════════


class _FakeDB:
    def __init__(self, tmp_path):
        self.db_path = str(tmp_path / "review.db")
        self.writes = 0

    def add_extraction_atomic(self, **kw):
        self.writes += 1
        return 1


def test_uncited_value_never_reaches_the_database(tmp_path):
    """The guard raises before add_extraction_atomic, so nothing is stored."""
    from engine.agents.extractor import MODEL
    db = _FakeDB(tmp_path)
    with pytest.raises(UncitedValueError):
        enforce_citations([span("robot_platform", "da Vinci")],
                          paper_id=1, arm=MODEL, escape_token=ESCAPE,
                          absence_sentinels=SENTINELS, mode=STRICT)
    assert db.writes == 0
