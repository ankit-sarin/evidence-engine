"""ELICIT-DESIGN-01 test 4 — a value without evidence cannot be written.

The predicate is exercised directly (it is mechanism-independent by design), and
the write boundary through the real legacy `extract_paper` with its model passes
stubbed: an uncited value raises before `write_extraction_events` is reached. The
bounded retry is tested in `test_elicitation_pipeline.py`.
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


def test_uncited_value_never_reaches_the_database(tmp_path):
    """The guard refuses before the write boundary: the real legacy
    `extract_paper`, its two model passes stubbed (the `test_extraction_events`
    `_legacy` pattern), returns one real value with no quote. It must raise
    `UncitedValueError` and `write_extraction_events` must never be called, so no
    claim or paper event reaches the store. 9c-C1 (R158, K2): this replaced a
    fake legacy writer that no engine code was ever handed."""
    import shutil
    from pathlib import Path
    from unittest.mock import patch

    from engine.agents import extractor as E
    from engine.agents.models import EvidenceSpan, ExtractionResult
    from engine.core.codebook import load_codebook
    from engine.core.database import ReviewDatabase
    from engine.core.parsed_text import resolve_parsed_text
    from engine.core.review_spec import load_review_spec
    from _event_store_fixture import open_extraction_run, seed_eligibility
    from _parsed_text_fixture import write_parsed

    repo = Path(__file__).resolve().parent.parent
    pid = 7
    spec = load_review_spec(repo / "review_specs" / "surgical_autonomy.yaml")
    db = ReviewDatabase("cg", data_root=tmp_path)
    cb_path = Path(db.db_path).parent / "extraction_codebook.yaml"
    shutil.copy2(repo / "data" / "surgical_autonomy" / "extraction_codebook.yaml", cb_path)
    db._conn.execute("INSERT INTO papers (id, title, source, status, created_at, "
                     "updated_at) VALUES (?, 't', 's', 'FT_ELIGIBLE', 'n', 'n')", (pid,))
    seed_eligibility(db._conn, pid)
    write_parsed(db, pid, "The paper reports a trial. A clean sentence.\n")
    db._conn.commit()
    run_id = open_extraction_run(db, spec)

    names = [f["name"] for f in load_codebook(cb_path).fields]
    fields = [EvidenceSpan(field_name=n, value="NR", source_snippet=f"Snippet for {n}.",
                           confidence=0.9, tier=1) for n in names]
    fields[0] = EvidenceSpan(field_name=names[0], value="A real value", source_snippet="",
                             confidence=0.9, tier=1)            # the uncited value
    pass2 = ExtractionResult(paper_id=pid, fields=fields, reasoning_trace="t",
                             model="deepseek-r1:32b", codebook_hash="h",
                             extracted_at="2026-09-25T00:00:00Z")

    written = []
    try:
        with patch.object(E, "extract_pass1_reasoning", return_value="trace"), \
             patch.object(E, "extract_pass2_structured", return_value=pass2), \
             patch.object(E, "write_extraction_events",
                          side_effect=lambda *a, **k: written.append((a, k))):
            with pytest.raises(UncitedValueError) as exc:
                E.extract_paper(pid, "The paper reports a trial.", spec, db,
                                parsed_text_ref=resolve_parsed_text(db._conn, pid),
                                run_id=run_id)
        assert names[0] in str(exc.value)
        assert written == []
        assert db._conn.execute("SELECT COUNT(*) FROM field_events").fetchone()[0] == 0
    finally:
        db.close()
