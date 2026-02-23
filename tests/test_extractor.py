"""Tests for two-pass extraction agent."""

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from engine.agents.extractor import (
    build_extraction_prompt,
    extract_paper,
    extract_pass1_reasoning,
    extract_pass2_structured,
    parse_thinking_trace,
    run_extraction,
)
from engine.agents.models import EvidenceSpan, ExtractionOutput, ExtractionResult
from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec
from engine.search.models import Citation

SPEC_PATH = Path(__file__).resolve().parent.parent / "review_specs" / "surgical_autonomy_v1.yaml"


@pytest.fixture(scope="module")
def spec():
    return load_review_spec(SPEC_PATH)


# ── Prompt Builder ───────────────────────────────────────────────────


def test_build_prompt_includes_all_fields(spec):
    prompt = build_extraction_prompt("Some paper text here.", spec)
    for field in spec.extraction_schema.fields:
        assert field.name in prompt
        assert field.description in prompt


def test_build_prompt_includes_tiers(spec):
    prompt = build_extraction_prompt("Paper text.", spec)
    assert "Tier 1" in prompt
    assert "Tier 2" in prompt
    assert "Tier 3" in prompt


def test_build_prompt_includes_paper_text(spec):
    prompt = build_extraction_prompt("CUSTOM_PAPER_CONTENT_HERE", spec)
    assert "CUSTOM_PAPER_CONTENT_HERE" in prompt


# ── Thinking Trace Parsing ───────────────────────────────────────────


def test_parse_thinking_trace_with_tags():
    content = "prefix <think>This is the reasoning about the paper.</think> suffix"
    trace = parse_thinking_trace(content)
    assert trace == "This is the reasoning about the paper."


def test_parse_thinking_trace_multiline():
    content = "<think>\nLine 1\nLine 2\nLine 3\n</think>"
    trace = parse_thinking_trace(content)
    assert "Line 1" in trace
    assert "Line 3" in trace


def test_parse_thinking_trace_no_tags():
    content = "Just some reasoning without tags."
    trace = parse_thinking_trace(content)
    assert trace == "Just some reasoning without tags."


# ── Pydantic Validation ─────────────────────────────────────────────


def test_evidence_span_validation():
    span = EvidenceSpan(
        field_name="study_design",
        value="RCT",
        source_snippet="This was a randomized controlled trial.",
        confidence=0.95,
        tier=1,
    )
    assert span.field_name == "study_design"
    assert span.confidence == 0.95


def test_evidence_span_rejects_invalid_confidence():
    with pytest.raises(Exception):
        EvidenceSpan(
            field_name="x", value="y", source_snippet="z",
            confidence=1.5, tier=1,
        )


def test_extraction_result_validation():
    result = ExtractionResult(
        paper_id=1,
        fields=[
            EvidenceSpan(
                field_name="study_design", value="RCT",
                source_snippet="An RCT was performed.", confidence=0.9, tier=1,
            )
        ],
        reasoning_trace="The paper describes an RCT...",
        model="deepseek-r1:32b",
        extraction_schema_hash="abc123",
        extracted_at=datetime.now(timezone.utc),
    )
    assert result.paper_id == 1
    assert len(result.fields) == 1


# ── Mocked Two-Pass Flow ────────────────────────────────────────────


def _mock_pass1_response():
    resp = MagicMock()
    resp.message.content = (
        "<think>The paper describes an RCT using the STAR robot for "
        "autonomous suturing on porcine tissue. Sample size was 20 trials. "
        "Autonomy level is 3. Accuracy was 95%.</think>\n"
        "Based on my analysis..."
    )
    return resp


def _mock_pass2_response():
    output = ExtractionOutput(
        fields=[
            EvidenceSpan(
                field_name="study_design", value="RCT",
                source_snippet="A randomized controlled trial was conducted.",
                confidence=0.95, tier=1,
            ),
            EvidenceSpan(
                field_name="sample_size", value="20",
                source_snippet="Twenty trials were performed.",
                confidence=0.9, tier=1,
            ),
            EvidenceSpan(
                field_name="robot_platform", value="STAR",
                source_snippet="The Smart Tissue Autonomous Robot (STAR) was used.",
                confidence=0.98, tier=1,
            ),
            EvidenceSpan(
                field_name="autonomy_level", value="3",
                source_snippet="Level 3 autonomy was achieved.",
                confidence=0.95, tier=1,
            ),
            EvidenceSpan(
                field_name="task_performed", value="suturing",
                source_snippet="Autonomous suturing was the primary task.",
                confidence=0.97, tier=1,
            ),
            EvidenceSpan(
                field_name="accuracy_metric", value="suture placement accuracy",
                source_snippet="Accuracy was measured as suture placement precision.",
                confidence=0.85, tier=1,
            ),
            EvidenceSpan(
                field_name="accuracy_value", value="95%",
                source_snippet="The system achieved 95% accuracy.",
                confidence=0.9, tier=1,
            ),
        ]
    )
    resp = MagicMock()
    resp.message.content = output.model_dump_json()
    return resp


def test_full_two_pass_mocked(tmp_path, spec):
    db = ReviewDatabase("test_ext", data_root=tmp_path)
    db.add_papers([Citation(title="STAR Suturing", source="pubmed", pmid="E1")])
    paper = db.get_papers_by_status("INGESTED")[0]
    pid = paper["id"]

    # Walk to PARSED
    db.update_status(pid, "SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")
    db.update_status(pid, "PARSED")

    paper_text = "This RCT used the STAR robot for autonomous suturing..."

    with patch("engine.agents.extractor.ollama.chat") as mock_chat:
        mock_chat.side_effect = [_mock_pass1_response(), _mock_pass2_response()]
        result = extract_paper(pid, paper_text, spec, db)

    assert result.paper_id == pid
    assert len(result.fields) == 7
    assert "STAR robot" in result.reasoning_trace

    # Check database records
    extractions = db._conn.execute(
        "SELECT * FROM extractions WHERE paper_id = ?", (pid,)
    ).fetchall()
    assert len(extractions) == 1

    spans = db._conn.execute(
        "SELECT * FROM evidence_spans WHERE extraction_id = ?",
        (extractions[0]["id"],),
    ).fetchall()
    assert len(spans) == 7
    assert all(s["audit_status"] == "pending" for s in spans)

    db.close()


# ── Staleness Skip ───────────────────────────────────────────────────


def test_staleness_skip(tmp_path, spec):
    db = ReviewDatabase("test_stale", data_root=tmp_path)
    db.add_papers([Citation(title="Already Done", source="pubmed", pmid="S1")])
    paper = db.get_papers_by_status("INGESTED")[0]
    pid = paper["id"]

    db.update_status(pid, "SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")
    db.update_status(pid, "PARSED")

    # Write a parsed text file
    parsed_dir = Path(db.db_path).parent / "parsed_text"
    (parsed_dir / f"{pid}_v1.md").write_text("Paper content here.")

    # Pre-insert an extraction with the current schema hash
    schema_hash = spec.extraction_hash()
    db.add_extraction(pid, schema_hash, {"fields": []}, "trace", "deepseek-r1:32b")

    # run_extraction should skip this paper
    stats = run_extraction(db, spec, "test_stale")
    assert stats["skipped"] == 1
    assert stats["extracted"] == 0

    db.close()


def test_run_extraction_no_parsed_text(tmp_path, spec):
    db = ReviewDatabase("test_notext", data_root=tmp_path)
    db.add_papers([Citation(title="No Text", source="pubmed", pmid="N1")])
    paper = db.get_papers_by_status("INGESTED")[0]
    pid = paper["id"]

    db.update_status(pid, "SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")
    db.update_status(pid, "PARSED")

    # Don't write any parsed text file — should fail gracefully
    stats = run_extraction(db, spec, "test_notext")
    assert stats["failed"] == 1
    assert stats["extracted"] == 0

    db.close()
