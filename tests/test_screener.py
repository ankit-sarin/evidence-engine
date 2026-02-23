"""Tests for dual-pass screening agent."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from engine.agents.screener import (
    ScreeningDecision,
    screen_paper,
    run_screening,
)
from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec
from engine.search.models import Citation

SPEC_PATH = Path(__file__).resolve().parent.parent / "review_specs" / "surgical_autonomy_v1.yaml"


@pytest.fixture(scope="module")
def spec():
    return load_review_spec(SPEC_PATH)


# ── Structured Output Parsing ────────────────────────────────────────


def test_screening_decision_parses():
    raw = '{"decision": "include", "rationale": "Relevant study.", "confidence": 0.85}'
    d = ScreeningDecision.model_validate_json(raw)
    assert d.decision == "include"
    assert d.confidence == 0.85


def test_screening_decision_rejects_invalid():
    raw = '{"decision": "maybe", "rationale": "Unsure.", "confidence": 0.5}'
    with pytest.raises(Exception):
        ScreeningDecision.model_validate_json(raw)


# ── Live Ollama Tests ────────────────────────────────────────────────


@pytest.mark.ollama
def test_screen_relevant_paper(spec):
    """A clearly relevant paper should be included."""
    paper = {
        "title": "Autonomous Robotic Suturing Using the STAR System: A Porcine Model",
        "abstract": (
            "We demonstrate Level 3 autonomous suturing using the Smart Tissue "
            "Autonomous Robot (STAR) on ex-vivo porcine tissue. The system completed "
            "10 running sutures with 95% accuracy compared to expert surgeons, with "
            "no tissue damage events."
        ),
    }
    result = screen_paper(paper, spec, pass_number=1)
    assert result.decision == "include"
    assert result.confidence > 0.5


@pytest.mark.ollama
def test_screen_irrelevant_paper(spec):
    """A clearly irrelevant paper should be excluded."""
    paper = {
        "title": "Optimization of Sous Vide Cooking Times for Wagyu Beef",
        "abstract": (
            "This study examines the effect of temperature and duration on the "
            "tenderness and flavor profile of Wagyu beef prepared using sous vide "
            "cooking methods. 50 samples were tested across 5 temperature settings."
        ),
    }
    result = screen_paper(paper, spec, pass_number=1)
    assert result.decision == "exclude"


# ── Dual-Pass Agreement Logic (Mocked) ──────────────────────────────


def _mock_decision(decision: str, confidence: float = 0.8):
    return ScreeningDecision(
        decision=decision,
        rationale=f"Mock {decision}.",
        confidence=confidence,
    )


def test_dual_pass_both_include(tmp_path, spec):
    db = ReviewDatabase("test", data_root=tmp_path)
    db.add_papers([Citation(title="Paper A", source="pubmed", pmid="1")])

    with patch("engine.agents.screener.screen_paper", return_value=_mock_decision("include")):
        stats = run_screening(db, spec)

    assert stats["screened_in"] == 1
    assert stats["screened_out"] == 0
    assert stats["flagged"] == 0
    assert db.get_papers_by_status("SCREENED_IN")
    db.close()


def test_dual_pass_both_exclude(tmp_path, spec):
    db = ReviewDatabase("test", data_root=tmp_path)
    db.add_papers([Citation(title="Paper B", source="pubmed", pmid="2")])

    with patch("engine.agents.screener.screen_paper", return_value=_mock_decision("exclude")):
        stats = run_screening(db, spec)

    assert stats["screened_out"] == 1
    assert stats["screened_in"] == 0
    assert db.get_papers_by_status("SCREENED_OUT")
    db.close()


def test_dual_pass_disagreement(tmp_path, spec):
    db = ReviewDatabase("test", data_root=tmp_path)
    db.add_papers([Citation(title="Paper C", source="pubmed", pmid="3")])

    # Pass 1 includes, Pass 2 excludes
    side_effects = [_mock_decision("include"), _mock_decision("exclude")]
    with patch("engine.agents.screener.screen_paper", side_effect=side_effects):
        stats = run_screening(db, spec)

    assert stats["flagged"] == 1
    assert db.get_papers_by_status("SCREEN_FLAGGED")
    db.close()


def test_dual_pass_records_both_decisions(tmp_path, spec):
    db = ReviewDatabase("test", data_root=tmp_path)
    db.add_papers([Citation(title="Paper D", source="pubmed", pmid="4")])

    with patch("engine.agents.screener.screen_paper", return_value=_mock_decision("include")):
        run_screening(db, spec)

    rows = db._conn.execute(
        "SELECT * FROM screening_decisions ORDER BY pass_number"
    ).fetchall()
    assert len(rows) == 2
    assert rows[0]["pass_number"] == 1
    assert rows[1]["pass_number"] == 2
    db.close()
