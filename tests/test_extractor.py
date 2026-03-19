"""Tests for two-pass extraction agent."""

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from engine.agents.extractor import (
    _has_invalid_snippet,
    _validate_and_retry_snippets,
    build_extraction_prompt,
    extract_paper,
    extract_pass1_reasoning,
    extract_pass2_structured,
    parse_thinking_trace,
    restart_ollama,
    run_extraction,
)
from engine.agents.models import EvidenceSpan, ExtractionOutput, ExtractionResult
from engine.core.constants import INVALID_SNIPPET_RE
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
        # Field name must appear as bold header in the prompt
        assert f"**{field.name}**" in prompt


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


def test_evidence_span_clamps_confidence():
    # Above 1.0 clamps to 1.0
    span_high = EvidenceSpan(
        field_name="x", value="y", source_snippet="z",
        confidence=1.5, tier=1,
    )
    assert span_high.confidence == 1.0

    # Below 0.0 clamps to 0.0 (DeepSeek-R1 -1 for NOT_FOUND)
    span_low = EvidenceSpan(
        field_name="x", value="y", source_snippet="z",
        confidence=-1, tier=1,
    )
    assert span_low.confidence == 0.0


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
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")
    db.update_status(pid, "PARSED")

    paper_text = "This RCT used the STAR robot for autonomous suturing..."

    with patch("engine.agents.extractor.ollama_chat") as mock_chat:
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

    db.update_status(pid, "ABSTRACT_SCREENED_IN")
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

    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")
    db.update_status(pid, "PARSED")

    # Don't write any parsed text file — should fail gracefully
    stats = run_extraction(db, spec, "test_notext")
    assert stats["failed"] == 1
    assert stats["extracted"] == 0

    db.close()


# ── Snippet Validation Tests ──────────────────────────────────────────


def test_invalid_snippet_re_imported_from_constants():
    """INVALID_SNIPPET_RE is defined in engine.core.constants, not locally."""
    import engine.core.constants as c
    assert hasattr(c, "INVALID_SNIPPET_RE")
    assert INVALID_SNIPPET_RE is c.INVALID_SNIPPET_RE


def test_ellipsis_snippet_triggers_retry():
    """Snippet with '...' ellipsis bridging is detected as invalid and retried."""
    span = EvidenceSpan(
        field_name="sample_size", value="20",
        source_snippet="Twenty participants... completed the study.",
        confidence=0.9, tier=1,
    )
    assert _has_invalid_snippet(span.source_snippet)

    with patch("engine.agents.extractor._retry_snippet") as mock_retry:
        mock_retry.return_value = "Twenty participants completed the study."
        validated = _validate_and_retry_snippets([span], "paper text", paper_id=1)

    assert len(validated) == 1
    assert validated[0].source_snippet == "Twenty participants completed the study."
    assert validated[0].value == "20"
    mock_retry.assert_called_once()


def test_bracket_ellipsis_triggers_retry():
    """Snippet with '[...]' is detected as invalid and retried."""
    span = EvidenceSpan(
        field_name="robot_platform", value="STAR",
        source_snippet="The STAR robot [...] was used for suturing.",
        confidence=0.95, tier=1,
    )
    assert _has_invalid_snippet(span.source_snippet)

    with patch("engine.agents.extractor._retry_snippet") as mock_retry:
        mock_retry.return_value = "The STAR robot was used for suturing."
        validated = _validate_and_retry_snippets([span], "paper text", paper_id=1)

    assert validated[0].source_snippet == "The STAR robot was used for suturing."
    mock_retry.assert_called_once()


def test_retry_exhausted_nulls_snippet_preserves_value():
    """After 2 failed retries, source_snippet is empty and value is preserved."""
    span = EvidenceSpan(
        field_name="study_design", value="RCT",
        source_snippet="An RCT... was performed... on tissue.",
        confidence=0.85, tier=1,
    )

    with patch("engine.agents.extractor._retry_snippet") as mock_retry:
        mock_retry.return_value = None  # all retries fail
        validated = _validate_and_retry_snippets([span], "paper text", paper_id=1)

    assert len(validated) == 1
    assert validated[0].source_snippet == ""
    assert validated[0].value == "RCT"
    assert mock_retry.call_count == 2  # SNIPPET_MAX_RETRIES


def test_valid_snippet_no_retry():
    """A clean verbatim snippet passes through without any retry calls."""
    span = EvidenceSpan(
        field_name="study_design", value="RCT",
        source_snippet="A randomized controlled trial was conducted.",
        confidence=0.95, tier=1,
    )
    assert not _has_invalid_snippet(span.source_snippet)

    with patch("engine.agents.extractor._retry_snippet") as mock_retry:
        validated = _validate_and_retry_snippets([span], "paper text", paper_id=1)

    assert validated[0].source_snippet == "A randomized controlled trial was conducted."
    mock_retry.assert_not_called()


# ── Proactive Ollama Restart Tests ───────────────────────────────────


class TestProactiveRestart:
    """Verify proactive Ollama restart behaviour in run_extraction."""

    @patch("engine.agents.extractor.restart_ollama")
    @patch("engine.agents.extractor.extract_paper")
    @patch("engine.utils.ollama_preflight.require_preflight")
    @patch("engine.utils.ollama_client.get_model_digest", return_value="abc123")
    @patch("engine.utils.extraction_cleanup.check_stale_extractions", return_value=0)
    def test_restart_triggers_after_n_papers(
        self, _stale, _digest, _preflight, mock_extract, mock_restart, tmp_path,
    ):
        """Proactive restart fires after restart_every papers are processed."""
        db, spec = self._setup_db(tmp_path, n_papers=5)

        mock_extract.side_effect = self._make_fake_extract(spec)

        run_extraction(db, spec, "test_review", restart_every=3)

        # 5 papers processed, restart_every=3 → should fire once (after paper 3)
        mock_restart.assert_called_once()
        assert "3 papers" in mock_restart.call_args.kwargs.get("reason", "")
        db.close()

    @patch("engine.agents.extractor.restart_ollama")
    @patch("engine.agents.extractor.extract_paper")
    @patch("engine.utils.ollama_preflight.require_preflight")
    @patch("engine.utils.ollama_client.get_model_digest", return_value="abc123")
    @patch("engine.utils.extraction_cleanup.check_stale_extractions", return_value=0)
    def test_restart_disabled_when_zero(
        self, _stale, _digest, _preflight, mock_extract, mock_restart, tmp_path,
    ):
        """restart_every=0 disables proactive restarts entirely."""
        db, spec = self._setup_db(tmp_path, n_papers=5)
        mock_extract.side_effect = self._make_fake_extract(spec)

        run_extraction(db, spec, "test_review", restart_every=0)

        mock_restart.assert_not_called()
        db.close()

    @patch("engine.agents.extractor.restart_ollama", side_effect=RuntimeError("Failed to restart Ollama: systemctl failed"))
    @patch("engine.agents.extractor.extract_paper")
    @patch("engine.utils.ollama_preflight.require_preflight")
    @patch("engine.utils.ollama_client.get_model_digest", return_value="abc123")
    @patch("engine.utils.extraction_cleanup.check_stale_extractions", return_value=0)
    def test_restart_failure_continues_gracefully(
        self, _stale, _digest, _preflight, mock_extract, _mock_restart, tmp_path,
    ):
        """If Ollama restart fails, extraction continues (H4: graceful degradation)."""
        db, spec = self._setup_db(tmp_path, n_papers=3)
        mock_extract.side_effect = self._make_fake_extract(spec)

        stats = run_extraction(db, spec, "test_review", restart_every=3)
        assert stats["extracted"] == 3
        assert stats["failed"] == 0
        db.close()

    @patch("engine.agents.extractor.restart_ollama")
    @patch("engine.agents.extractor.ollama_chat")
    @patch("engine.utils.ollama_preflight.require_preflight")
    @patch("engine.utils.ollama_client.get_model_digest", return_value="abc123")
    @patch("engine.utils.extraction_cleanup.check_stale_extractions", return_value=0)
    def test_zero_span_extraction_marks_extract_failed(
        self, _stale, _digest, _preflight, mock_chat, _restart, tmp_path,
    ):
        """LLM returns valid JSON with zero fields → EXTRACT_FAILED, no spans in DB."""
        db, spec = self._setup_db(tmp_path, n_papers=1)

        # Pass 1 response (reasoning)
        pass1_resp = MagicMock()
        pass1_resp.message.content = "<think>Reasoning about the paper.</think>"

        # Pass 2 response (structured JSON with empty fields list)
        zero_span_output = ExtractionOutput(fields=[])
        pass2_resp = MagicMock()
        pass2_resp.message.content = zero_span_output.model_dump_json()

        mock_chat.side_effect = [pass1_resp, pass2_resp]

        stats = run_extraction(db, spec, "test_review", restart_every=0)

        assert stats["failed"] == 1
        assert stats["extracted"] == 0

        # Verify no extraction or span rows were inserted
        ext_count = db._conn.execute("SELECT COUNT(*) FROM extractions").fetchone()[0]
        span_count = db._conn.execute("SELECT COUNT(*) FROM evidence_spans").fetchone()[0]
        assert ext_count == 0
        assert span_count == 0

        # Verify paper status is EXTRACT_FAILED (direct SQL — simplified schema)
        paper = db._conn.execute("SELECT status FROM papers WHERE id = 1").fetchone()
        assert paper["status"] == "EXTRACT_FAILED"
        db.close()

    # ── helpers ──

    def _setup_db(self, tmp_path, n_papers=5):
        """Create a minimal in-memory-style DB with N FT_ELIGIBLE papers."""
        db_path = str(tmp_path / "test.db")
        db = ReviewDatabase.__new__(ReviewDatabase)
        import sqlite3
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("""CREATE TABLE papers (
            id INTEGER PRIMARY KEY, title TEXT, authors TEXT, year INTEGER,
            status TEXT, abstract TEXT, doi TEXT, pmid TEXT, source TEXT,
            pdf_local_path TEXT, added_at TEXT, updated_at TEXT)""")
        conn.execute("""CREATE TABLE extractions (
            id INTEGER PRIMARY KEY, paper_id INTEGER, extraction_schema_hash TEXT,
            extracted_data TEXT, reasoning_trace TEXT, model TEXT,
            model_digest TEXT, auditor_model_digest TEXT, extracted_at TEXT)""")
        conn.execute("""CREATE TABLE evidence_spans (
            id INTEGER PRIMARY KEY, extraction_id INTEGER, field_name TEXT,
            value TEXT, source_snippet TEXT, confidence REAL)""")
        for i in range(1, n_papers + 1):
            conn.execute(
                "INSERT INTO papers (id, title, status, added_at) VALUES (?, ?, 'FT_ELIGIBLE', '2026-01-01')",
                (i, f"Test Paper {i}"),
            )
            # Create parsed text file
            parsed_dir = tmp_path / "parsed_text"
            parsed_dir.mkdir(exist_ok=True)
            (parsed_dir / f"{i}_v1.md").write_text(f"Paper {i} text content.")
        conn.commit()
        db._conn = conn
        db.db_path = db_path
        db.review_name = "test_review"
        spec = load_review_spec(str(SPEC_PATH))
        return db, spec

    def _make_fake_extract(self, spec):
        """Return a fake extract_paper function that stores results in DB."""
        schema_hash = spec.extraction_hash()

        def _fake(paper_id, paper_text, spec_arg, db, **kwargs):
            from engine.agents.models import ExtractionResult, EvidenceSpan
            result = ExtractionResult(
                paper_id=paper_id,
                fields=[EvidenceSpan(
                    field_name="study_type", value="RCT",
                    source_snippet="A randomized trial.", confidence=0.9, tier=1,
                )],
                reasoning_trace="test",
                model="test-model",
                extraction_schema_hash=schema_hash,
                extracted_at=datetime.now(timezone.utc),
            )
            db._conn.execute(
                "INSERT INTO extractions (paper_id, extraction_schema_hash, extracted_data, model, extracted_at) VALUES (?, ?, '[]', 'test', '2026-01-01')",
                (paper_id, schema_hash),
            )
            db._conn.commit()
            return result
        return _fake


# ── H4: Graceful handling of restart_ollama failure ──────────────────


class TestRestartOllamaGraceful:
    """restart_ollama() failure in extraction loop must not crash the run."""

    def test_restart_failure_continues_extraction(self, tmp_path, caplog):
        """If restart_ollama raises RuntimeError, extraction continues."""
        db, spec = self._setup_db(tmp_path, n_papers=3)
        fake_extract = self._make_fake_extract(spec)

        with (
            patch("engine.utils.ollama_preflight.require_preflight"),
            patch("engine.utils.ollama_client.get_model_digest", return_value="sha256:test"),
            patch("engine.agents.extractor.extract_paper", side_effect=fake_extract),
            patch("engine.agents.extractor.restart_ollama",
                  side_effect=RuntimeError("Ollama did not respond")),
        ):
            with caplog.at_level("ERROR", logger="engine.agents.extractor"):
                stats = run_extraction(db, spec, "test_review", restart_every=1)

        # All 3 papers should have been extracted despite restart failures
        assert stats["extracted"] == 3
        assert stats["failed"] == 0

        # The error should be logged
        assert "Proactive Ollama restart failed" in caplog.text
        db._conn.close()

    # Re-use helpers from TestRunExtraction
    def _setup_db(self, tmp_path, n_papers=3):
        db_path = str(tmp_path / "test.db")
        db = ReviewDatabase.__new__(ReviewDatabase)
        import sqlite3
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("""CREATE TABLE papers (
            id INTEGER PRIMARY KEY, title TEXT, authors TEXT, year INTEGER,
            status TEXT, abstract TEXT, doi TEXT, pmid TEXT, source TEXT,
            pdf_local_path TEXT, added_at TEXT, updated_at TEXT)""")
        conn.execute("""CREATE TABLE extractions (
            id INTEGER PRIMARY KEY, paper_id INTEGER, extraction_schema_hash TEXT,
            extracted_data TEXT, reasoning_trace TEXT, model TEXT,
            model_digest TEXT, auditor_model_digest TEXT, extracted_at TEXT)""")
        conn.execute("""CREATE TABLE evidence_spans (
            id INTEGER PRIMARY KEY, extraction_id INTEGER, field_name TEXT,
            value TEXT, source_snippet TEXT, confidence REAL)""")
        for i in range(1, n_papers + 1):
            conn.execute(
                "INSERT INTO papers (id, title, status, added_at) VALUES (?, ?, 'FT_ELIGIBLE', '2026-01-01')",
                (i, f"Test Paper {i}"),
            )
            parsed_dir = tmp_path / "parsed_text"
            parsed_dir.mkdir(exist_ok=True)
            (parsed_dir / f"{i}_v1.md").write_text(f"Paper {i} text content.")
        conn.commit()
        db._conn = conn
        db.db_path = db_path
        db.review_name = "test_review"
        spec = load_review_spec(str(SPEC_PATH))
        return db, spec

    def _make_fake_extract(self, spec):
        schema_hash = spec.extraction_hash()

        def _fake(paper_id, paper_text, spec_arg, db, **kwargs):
            from engine.agents.models import ExtractionResult, EvidenceSpan
            result = ExtractionResult(
                paper_id=paper_id,
                fields=[EvidenceSpan(
                    field_name="study_type", value="RCT",
                    source_snippet="A randomized trial.", confidence=0.9, tier=1,
                )],
                reasoning_trace="test",
                model="test-model",
                extraction_schema_hash=schema_hash,
                extracted_at=datetime.now(timezone.utc),
            )
            db._conn.execute(
                "INSERT INTO extractions (paper_id, extraction_schema_hash, extracted_data, model, extracted_at) VALUES (?, ?, '[]', 'test', '2026-01-01')",
                (paper_id, schema_hash),
            )
            db._conn.commit()
            return result
        return _fake
