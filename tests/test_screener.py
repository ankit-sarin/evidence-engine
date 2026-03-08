"""Tests for dual-model screening agent."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from engine.agents.screener import (
    ScreeningDecision,
    screen_paper,
    run_screening,
    run_verification,
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


# ── Verification Logic (Mocked) ─────────────────────────────────────


def _setup_screened_in(tmp_path, spec, n_papers=3):
    """Helper: create n papers and screen them all in."""
    db = ReviewDatabase("test", data_root=tmp_path)
    for i in range(n_papers):
        db.add_papers([Citation(title=f"Paper {i}", source="pubmed", pmid=str(100 + i))])

    with patch("engine.agents.screener.screen_paper", return_value=_mock_decision("include")):
        run_screening(db, spec)

    assert len(db.get_papers_by_status("SCREENED_IN")) == n_papers
    return db


def test_verification_all_confirmed(tmp_path, spec):
    """Verifier includes all → all stay SCREENED_IN."""
    db = _setup_screened_in(tmp_path, spec, n_papers=3)

    with patch("engine.agents.screener.screen_paper", return_value=_mock_decision("include")):
        stats = run_verification(db, spec)

    assert stats["confirmed"] == 3
    assert stats["flagged"] == 0
    assert len(db.get_papers_by_status("SCREENED_IN")) == 3
    assert len(db.get_papers_by_status("SCREEN_FLAGGED")) == 0
    db.close()


def test_verification_some_flagged(tmp_path, spec):
    """Verifier excludes some → those move to SCREEN_FLAGGED."""
    db = _setup_screened_in(tmp_path, spec, n_papers=3)

    # First paper confirmed, second and third excluded by verifier
    side_effects = [
        _mock_decision("include"),
        _mock_decision("exclude"),
        _mock_decision("exclude"),
    ]
    with patch("engine.agents.screener.screen_paper", side_effect=side_effects):
        stats = run_verification(db, spec)

    assert stats["confirmed"] == 1
    assert stats["flagged"] == 2
    assert len(db.get_papers_by_status("SCREENED_IN")) == 1
    assert len(db.get_papers_by_status("SCREEN_FLAGGED")) == 2
    db.close()


def test_verification_all_flagged(tmp_path, spec):
    """Verifier excludes all → all move to SCREEN_FLAGGED."""
    db = _setup_screened_in(tmp_path, spec, n_papers=2)

    with patch("engine.agents.screener.screen_paper", return_value=_mock_decision("exclude")):
        stats = run_verification(db, spec)

    assert stats["confirmed"] == 0
    assert stats["flagged"] == 2
    assert len(db.get_papers_by_status("SCREENED_IN")) == 0
    assert len(db.get_papers_by_status("SCREEN_FLAGGED")) == 2
    db.close()


def test_verification_stores_decisions(tmp_path, spec):
    """Verification decisions are stored in verification_decisions table."""
    db = _setup_screened_in(tmp_path, spec, n_papers=2)

    side_effects = [_mock_decision("include"), _mock_decision("exclude")]
    with patch("engine.agents.screener.screen_paper", side_effect=side_effects):
        run_verification(db, spec)

    rows = db._conn.execute(
        "SELECT * FROM verification_decisions ORDER BY paper_id"
    ).fetchall()
    assert len(rows) == 2
    assert rows[0]["decision"] == "include"
    assert rows[1]["decision"] == "exclude"
    assert rows[0]["model"] == spec.screening_models.verification
    db.close()


def test_verification_does_not_touch_screening_decisions(tmp_path, spec):
    """Verification should not add rows to screening_decisions table."""
    db = _setup_screened_in(tmp_path, spec, n_papers=1)

    # Count screening_decisions before verification
    pre_count = db._conn.execute("SELECT COUNT(*) FROM screening_decisions").fetchone()[0]

    with patch("engine.agents.screener.screen_paper", return_value=_mock_decision("exclude")):
        run_verification(db, spec)

    post_count = db._conn.execute("SELECT COUNT(*) FROM screening_decisions").fetchone()[0]
    assert post_count == pre_count  # No new rows in screening_decisions
    db.close()


def test_screening_uses_primary_model(tmp_path, spec):
    """Primary screening should use spec.screening_models.primary."""
    db = ReviewDatabase("test", data_root=tmp_path)
    db.add_papers([Citation(title="Paper X", source="pubmed", pmid="500")])

    with patch("engine.agents.screener.screen_paper", return_value=_mock_decision("include")) as mock:
        run_screening(db, spec)

    # All calls should pass the primary model
    for call in mock.call_args_list:
        assert call.kwargs.get("model") == spec.screening_models.primary
    db.close()
