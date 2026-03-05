"""Tests for cross-model audit agent."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from engine.agents.auditor import (
    AuditVerdict,
    audit_span,
    grep_verify,
    run_audit,
    semantic_verify,
    _normalize,
    _INVALID_SNIPPET_RE,
)
from engine.core.database import ReviewDatabase
from engine.search.models import Citation


PAPER_TEXT = (
    "This study evaluates the Smart Tissue Autonomous Robot (STAR) system "
    "for autonomous suturing. A randomized controlled trial was conducted "
    "with 20 trials on porcine tissue. The system achieved Level 3 autonomy "
    "per the Yang et al. taxonomy. Suture placement accuracy was 95.2% "
    "compared to 91.0% for expert surgeons. No tissue damage events were "
    "reported during autonomous operation."
)


# ── grep_verify: Exact Match ─────────────────────────────────────────


def test_grep_exact_match():
    assert grep_verify("randomized controlled trial was conducted", PAPER_TEXT)


def test_grep_exact_match_case_insensitive():
    assert grep_verify("RANDOMIZED CONTROLLED TRIAL WAS CONDUCTED", PAPER_TEXT)


# ── grep_verify: Whitespace Difference ───────────────────────────────


def test_grep_whitespace_normalized():
    snippet = "randomized   controlled  trial   was   conducted"
    assert grep_verify(snippet, PAPER_TEXT)


# ── grep_verify: Fabricated Snippet ──────────────────────────────────


def test_grep_fabricated_fails():
    assert not grep_verify("The experiment was performed on cadaveric specimens", PAPER_TEXT)


def test_grep_completely_unrelated():
    assert not grep_verify("Cooking temperature affects protein denaturation", PAPER_TEXT)


# ── grep_verify: Fuzzy Match (OCR-style errors) ─────────────────────


def test_grep_fuzzy_matches_minor_error():
    # Simulate OCR error: "porcine" → "porcme"
    snippet = "20 trials on porcme tissue"
    assert grep_verify(snippet, PAPER_TEXT)


def test_grep_fuzzy_rejects_major_difference():
    snippet = "500 experiments on synthetic phantoms with laser guidance"
    assert not grep_verify(snippet, PAPER_TEXT)


# ── grep_verify: Edge Cases ──────────────────────────────────────────


def test_grep_empty_snippet():
    assert not grep_verify("", PAPER_TEXT)


def test_grep_empty_text():
    assert not grep_verify("some snippet", "")


# ── Normalization: Glued Punctuation ─────────────────────────────────


def test_normalize_glued_punctuation():
    assert _normalize("Table.I shows") == "table i shows"


def test_normalize_smart_quotes():
    assert _normalize("\u201cquoted\u201d") == '"quoted"'


def test_grep_glued_punctuation_match():
    """Table.I in paper text matches Table I in snippet."""
    paper = "Results are in Table.I below."
    assert grep_verify("Table I below", paper)


# ── Invalid Snippet Detection ────────────────────────────────────────


def test_invalid_snippet_ellipsis_bracket():
    assert _INVALID_SNIPPET_RE.search("some text [...] more text")


def test_invalid_snippet_unicode_ellipsis():
    assert _INVALID_SNIPPET_RE.search("some text… more text")


def test_invalid_snippet_triple_dots():
    assert _INVALID_SNIPPET_RE.search("some text... more text")


def test_valid_snippet_no_ellipsis():
    assert not _INVALID_SNIPPET_RE.search("some text, more text")


# ── semantic_verify (mocked) ─────────────────────────────────────────


def test_semantic_verify_mocked():
    from engine.agents.models import EvidenceSpan

    span = EvidenceSpan(
        field_name="study_design",
        value="RCT",
        source_snippet="A randomized controlled trial was conducted.",
        confidence=0.95,
        tier=1,
    )

    mock_resp = MagicMock()
    mock_resp.message.content = AuditVerdict(
        status="verified", grep_found=True, reasoning="Value matches snippet."
    ).model_dump_json()

    with patch("engine.agents.auditor.ollama.chat", return_value=mock_resp):
        verdict = semantic_verify(span, PAPER_TEXT)

    assert verdict.status == "verified"


# ── audit_span: 4-state outcomes ─────────────────────────────────────


def test_audit_span_invalid_snippet():
    """Snippet with ellipsis bridging → invalid_snippet."""
    span_data = {
        "field_name": "sample_size",
        "value": "20 patients",
        "source_snippet": "We enrolled [...] twenty patients in total.",
        "confidence": 0.9,
    }
    status, reasoning = audit_span(span_data, PAPER_TEXT)
    assert status == "invalid_snippet"


def test_audit_span_verified():
    """Grep pass + semantic pass → verified."""
    span_data = {
        "field_name": "study_design",
        "value": "RCT",
        "source_snippet": "A randomized controlled trial was conducted",
        "confidence": 0.95,
    }
    mock_resp = MagicMock()
    mock_resp.message.content = AuditVerdict(
        status="verified", grep_found=True, reasoning="Confirmed."
    ).model_dump_json()

    with patch("engine.agents.auditor.ollama.chat", return_value=mock_resp):
        status, reasoning = audit_span(span_data, PAPER_TEXT)

    assert status == "verified"


def test_audit_span_contested():
    """Grep fail + semantic pass → contested."""
    span_data = {
        "field_name": "study_design",
        "value": "RCT",
        "source_snippet": "This snippet does not exist in the paper text at all.",
        "confidence": 0.95,
    }
    mock_resp = MagicMock()
    mock_resp.message.content = AuditVerdict(
        status="verified", grep_found=False, reasoning="Value is reasonable."
    ).model_dump_json()

    with patch("engine.agents.auditor.ollama.chat", return_value=mock_resp):
        status, reasoning = audit_span(span_data, PAPER_TEXT)

    assert status == "contested"
    assert "Grep failed" in reasoning


def test_audit_span_flagged():
    """Grep fail + semantic fail → flagged."""
    span_data = {
        "field_name": "study_design",
        "value": "Cohort study",
        "source_snippet": "This snippet does not exist in the paper text at all.",
        "confidence": 0.7,
    }
    mock_resp = MagicMock()
    mock_resp.message.content = AuditVerdict(
        status="flagged", grep_found=False, reasoning="Value not supported."
    ).model_dump_json()

    with patch("engine.agents.auditor.ollama.chat", return_value=mock_resp):
        status, reasoning = audit_span(span_data, PAPER_TEXT)

    assert status == "flagged"


def test_audit_span_not_found_value():
    """Absence values → auto-verified without LLM."""
    span_data = {
        "field_name": "fda_status",
        "value": "NOT_FOUND",
        "source_snippet": "",
        "confidence": 0.0,
    }
    status, reasoning = audit_span(span_data, PAPER_TEXT)
    assert status == "verified"


# ── Tier 4 Semantic-Only Routing ─────────────────────────────────────


def test_audit_span_tier4_skips_grep():
    """Tier 4 fields skip grep, go straight to semantic verification."""
    span_data = {
        "field_name": "key_limitation",
        "value": "Small sample size limits generalizability",
        "source_snippet": "This snippet is fabricated but tier 4 skips grep.",
        "confidence": 0.8,
    }
    mock_resp = MagicMock()
    mock_resp.message.content = AuditVerdict(
        status="verified", grep_found=False, reasoning="Assessment is reasonable."
    ).model_dump_json()

    with patch("engine.agents.auditor.ollama.chat", return_value=mock_resp) as mock_chat:
        status, reasoning = audit_span(span_data, PAPER_TEXT, field_tier=4)

    # LLM was called (grep was skipped)
    mock_chat.assert_called_once()
    assert status == "verified"


def test_audit_span_tier4_semantic_fail():
    """Tier 4 field with semantic failure → flagged."""
    span_data = {
        "field_name": "clinical_readiness_assessment",
        "value": "Ready for clinical use",
        "source_snippet": "Early-stage bench testing only.",
        "confidence": 0.6,
    }
    mock_resp = MagicMock()
    mock_resp.message.content = AuditVerdict(
        status="flagged", grep_found=False, reasoning="Contradicts early-stage evidence."
    ).model_dump_json()

    with patch("engine.agents.auditor.ollama.chat", return_value=mock_resp):
        status, reasoning = audit_span(span_data, PAPER_TEXT, field_tier=4)

    assert status == "flagged"


# ── Full Audit Flow (mocked) ────────────────────────────────────────


def _make_verified_response():
    resp = MagicMock()
    resp.message.content = AuditVerdict(
        status="verified", grep_found=True, reasoning="Confirmed."
    ).model_dump_json()
    return resp


def _make_flagged_response():
    resp = MagicMock()
    resp.message.content = AuditVerdict(
        status="flagged", grep_found=True, reasoning="Value misinterprets snippet."
    ).model_dump_json()
    return resp


def test_full_audit_flow_mocked(tmp_path):
    db = ReviewDatabase("test_audit", data_root=tmp_path)

    # Add a paper and walk to EXTRACTED
    db.add_papers([Citation(title="STAR Study", source="pubmed", pmid="A1")])
    paper = db.get_papers_by_status("INGESTED")[0]
    pid = paper["id"]
    db.update_status(pid, "SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")
    db.update_status(pid, "PARSED")
    db.update_status(pid, "EXTRACTED")

    # Write parsed text
    parsed_dir = Path(db.db_path).parent / "parsed_text"
    (parsed_dir / f"{pid}_v1.md").write_text(PAPER_TEXT)

    # Add an extraction with spans
    ext_id = db.add_extraction(pid, "hash1", {}, "trace", "deepseek-r1:32b")

    # Span 1: good snippet (will be found by grep, verified by LLM)
    db.add_evidence_span(
        ext_id, "study_design", "RCT",
        "A randomized controlled trial was conducted", 0.95,
    )
    # Span 2: good snippet (will be found by grep, but LLM flags)
    db.add_evidence_span(
        ext_id, "sample_size", "200",  # wrong value (paper says 20)
        "20 trials on porcine tissue", 0.8,
    )
    # Span 3: fabricated snippet (grep will fail → goes to semantic → flagged)
    db.add_evidence_span(
        ext_id, "robot_platform", "da Vinci",
        "The da Vinci Xi system was the primary platform", 0.7,
    )

    # Mock Ollama: call 1 (study_design) → verified, call 2 (sample_size) → flagged,
    # call 3 (robot_platform) → flagged (grep fails, semantic called, returns flagged)
    with patch("engine.agents.auditor.ollama.chat") as mock_chat:
        mock_chat.side_effect = [
            _make_verified_response(),
            _make_flagged_response(),
            _make_flagged_response(),
        ]
        stats = run_audit(db, "test_audit")

    assert stats["papers_audited"] == 1
    assert stats["spans_verified"] == 1
    assert stats["spans_flagged"] == 2

    # Verify paper reached AI_AUDIT_COMPLETE
    assert len(db.get_papers_by_status("AI_AUDIT_COMPLETE")) == 1

    # Verify span statuses in DB
    spans = db._conn.execute(
        "SELECT field_name, audit_status FROM evidence_spans ORDER BY id"
    ).fetchall()
    assert dict(spans[0])["audit_status"] == "verified"
    assert dict(spans[1])["audit_status"] == "flagged"
    assert dict(spans[2])["audit_status"] == "flagged"

    db.close()
