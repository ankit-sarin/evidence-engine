"""Tests for two-pass extraction agent."""

import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from types import SimpleNamespace

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
from engine.core.codebook import load_codebook_beside
from engine.core.codebook import load_codebook_for
from _parsed_text_fixture import write_parsed
from _event_store_fixture import (claim_identity, open_extraction_run, seed_eligibility,
                                  upgrade_event_store)
import importlib as _importlib
_M021 = _importlib.import_module("engine.migrations.021_parsed_text_sha256")

CBK = load_codebook_for("surgical_autonomy")

SPEC_PATH = Path(__file__).resolve().parent.parent / "review_specs" / "surgical_autonomy.yaml"


def _write_codebook(review_dir):
    """A review directory carries a codebook (CODEBOOK-AUTH-01).

    These fixtures build a ReviewDatabase with __new__, so conftest's
    review-directory invariant does not fire for them.
    """
    import shutil

    shutil.copy2(
        Path(__file__).resolve().parent.parent
        / "data" / "surgical_autonomy" / "extraction_codebook.yaml",
        Path(review_dir) / "extraction_codebook.yaml",
    )



@pytest.fixture(scope="module")
def spec():
    return load_review_spec(SPEC_PATH)


# ── Prompt Builder ───────────────────────────────────────────────────


def test_build_prompt_includes_all_fields(spec):
    prompt = build_extraction_prompt("Some paper text here.", spec)
    for field in CBK.fields:
        # Field name must appear as bold header in the prompt
        assert f"**{field['name']}**" in prompt


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
    """Legacy inline-tag shape (Ollama < 0.12) still parses."""
    content = "prefix <think>This is the reasoning about the paper.</think> suffix"
    trace, branch = parse_thinking_trace(content)
    assert trace == "This is the reasoning about the paper."
    assert branch == "legacy-tags"


def test_parse_thinking_trace_multiline():
    content = "<think>\nLine 1\nLine 2\nLine 3\n</think>"
    trace, branch = parse_thinking_trace(content)
    assert "Line 1" in trace
    assert "Line 3" in trace
    assert branch == "legacy-tags"


def test_parse_thinking_trace_no_tags_raises():
    """REGRESSION-01: this test previously asserted the silent fallback —
    that the whole response content be returned as the reasoning trace. That
    fallback is what broke Pass 1 on Ollama 0.21.0, so it is gone and absence is
    now an error. Full coverage lives in tests/test_thinking_channel.py."""
    from engine.agents.extractor import MissingThinkingChannelError

    with pytest.raises(MissingThinkingChannelError):
        parse_thinking_trace("Just some reasoning without tags.")


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
        codebook_hash="abc123",
        extracted_at=datetime.now(timezone.utc),
    )
    assert result.paper_id == 1
    assert len(result.fields) == 1


# ── Mocked Two-Pass Flow ────────────────────────────────────────────


def _mock_pass1_response():
    """Ollama >= 0.12 shape: reasoning in message.thinking, answer in content."""
    return SimpleNamespace(
        message=SimpleNamespace(
            content="Based on my analysis...",
            thinking=(
                "The paper describes an RCT using the STAR robot for "
                "autonomous suturing on porcine tissue. Sample size was 20 trials. "
                "Autonomy level is 3. Accuracy was 95%."
            ),
        ),
        done_reason="stop",
    )


def _complete_pass2_fields(spec):
    """One span per prompted field, so the INSTRUMENT-01 completeness guard passes.

    Built from the spec so it tracks the codebook instead of pinning a count.
    """
    fields = []
    for tier in (1, 2, 3, 4):
        for f in CBK.fields_by_tier(tier):
            fields.append(
                EvidenceSpan(
                    field_name=f["name"], value="NR",
                    source_snippet=f"Snippet for {f['name']}.",
                    confidence=0.9, tier=f["tier"],
                )
            )
    return fields


def _mock_pass2_complete(spec):
    output = ExtractionOutput(fields=_complete_pass2_fields(spec))
    return SimpleNamespace(
        message=SimpleNamespace(content=output.model_dump_json()),
    )


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
    # 9b-2c (R118): the mocked Pass 2 answers the live codebook's fields, so the
    # review carries that codebook — against conftest's one-field test codebook
    # the other fields are now dropped as unexpected rather than stored.
    _write_codebook(Path(db.db_path).parent)
    db.add_papers([Citation(title="STAR Suturing", source="pubmed", pmid="E1")])
    paper = db.get_papers_by_status("INGESTED")[0]
    pid = paper["id"]

    # Walk to PARSED
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")
    db.update_status(pid, "PARSED")

    paper_text = "This RCT used the STAR robot for autonomous suturing..."
    # 9b-FLIP: the write is events now — it needs a run and the text's identity.
    write_parsed(db, pid, paper_text)
    run_id = open_extraction_run(db, spec)
    from engine.core.parsed_text import resolve_parsed_text

    n_expected = len(CBK.fields)
    with patch("engine.agents.extractor.ollama_chat") as mock_chat:
        mock_chat.side_effect = [_mock_pass1_response(), _mock_pass2_complete(spec)]
        result = extract_paper(pid, paper_text, spec, db, run_id=run_id,
                               parsed_text_ref=resolve_parsed_text(db._conn, pid))

    assert result.paper_id == pid
    assert len(result.fields) == n_expected
    assert "STAR robot" in result.reasoning_trace

    # 9b-FLIP (R111): one asserted claim per field under the run, one
    # `extracted` paper event. (9c-C1: the legacy-table counts that stood here
    # could not fail — nothing writes those tables any more.)
    claims = db._conn.execute(
        "SELECT run_id FROM field_events WHERE paper_id = ? AND event_type = 'asserted'",
        (pid,)).fetchall()
    assert len(claims) == n_expected and {r[0] for r in claims} == {run_id}
    from engine.core.effective import effective_state
    assert effective_state(db._conn, pid).processing == "extracted"

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

    # Write and record a parsed text (S3e: the resolver reads references)
    write_parsed(db, pid, "Paper content here.")
    seed_eligibility(db._conn, pid)

    # 9b-2a: the extractions/codebook_hash skip is gone. A paper is skipped when
    # the spec's arm holds a live claim carrying the reuse key of this paper's
    # current text (R96) — the replacement for "already extracted".
    from engine.core import events
    from engine.core.events import PAYLOAD_REUSE_KEY
    from engine.core.parsed_text import resolve_parsed_text
    from engine.core.reuse_key import reuse_key

    arm = spec.extraction_models.arm
    run_id = open_extraction_run(db, spec)   # 9b-2b: before the claim (R21 freeze)
    key = reuse_key(arm, pid, resolve_parsed_text(db._conn, pid).sha256)
    events.write_field_event(
        db._conn, event_type="asserted", paper_id=pid, field_name="study_design",
        arm=arm, value="RCT", source_snippet="RCT", actor_kind="model",
        actor_role="extractor", actor_name="m",
        payload={**claim_identity(arm, pid, sha=resolve_parsed_text(db._conn, pid).sha256),
                 PAYLOAD_REUSE_KEY: key},
        run_id=run_id)

    # run_extraction should skip this paper. Preflight is patched out: it shells
    # out to `systemctl show ollama` and loads deepseek-r1:32b against the live
    # server, neither of which this test is about (OPSFIX-01).
    with patch("engine.utils.ollama_preflight.require_preflight"):
        stats = run_extraction(db, spec, "test_stale", run_id=run_id)
    assert stats["skipped_asserted"] == 1
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
    seed_eligibility(db._conn, pid)   # 9b-2a: selection reads the eligibility axis

    # Don't write any parsed text file — should fail gracefully. Preflight is
    # patched out for the same reason as test_staleness_skip (OPSFIX-01).
    # 9b-2a (R122): a refused text is reported at selection, not as a failure.
    with patch("engine.utils.ollama_preflight.require_preflight"):
        stats = run_extraction(db, spec, "test_notext",
                               run_id=open_extraction_run(db, spec))  # 9b-2b
    assert stats["skipped_refused"] == 1
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
    @patch("engine.utils.ollama_client.fetch_model_digest", return_value="a" * 64)
    @patch("engine.utils.extraction_cleanup.check_stale_extractions", return_value=0)
    def test_restart_triggers_after_n_papers(
        self, _stale, _digest, _preflight, mock_extract, mock_restart, tmp_path,
    ):
        """Proactive restart fires after restart_every papers are processed."""
        db, spec = self._setup_db(tmp_path, n_papers=5)

        mock_extract.side_effect = self._make_fake_extract(spec)

        run_extraction(db, spec, "test_review", restart_every=3, run_id=self.run_id)

        # 5 papers processed, restart_every=3 → should fire once (after paper 3)
        mock_restart.assert_called_once()
        assert "3 papers" in mock_restart.call_args.kwargs.get("reason", "")
        db.close()

    @patch("engine.agents.extractor.restart_ollama")
    @patch("engine.agents.extractor.extract_paper")
    @patch("engine.utils.ollama_preflight.require_preflight")
    @patch("engine.utils.ollama_client.fetch_model_digest", return_value="a" * 64)
    @patch("engine.utils.extraction_cleanup.check_stale_extractions", return_value=0)
    def test_restart_disabled_when_zero(
        self, _stale, _digest, _preflight, mock_extract, mock_restart, tmp_path,
    ):
        """restart_every=0 disables proactive restarts entirely."""
        db, spec = self._setup_db(tmp_path, n_papers=5)
        mock_extract.side_effect = self._make_fake_extract(spec)

        run_extraction(db, spec, "test_review", restart_every=0, run_id=self.run_id)

        mock_restart.assert_not_called()
        db.close()

    @patch("engine.agents.extractor.restart_ollama", side_effect=RuntimeError("Failed to restart Ollama: systemctl failed"))
    @patch("engine.agents.extractor.extract_paper")
    @patch("engine.utils.ollama_preflight.require_preflight")
    @patch("engine.utils.ollama_client.fetch_model_digest", return_value="a" * 64)
    @patch("engine.utils.extraction_cleanup.check_stale_extractions", return_value=0)
    def test_restart_failure_continues_gracefully(
        self, _stale, _digest, _preflight, mock_extract, _mock_restart, tmp_path,
    ):
        """If Ollama restart fails, extraction continues (H4: graceful degradation)."""
        db, spec = self._setup_db(tmp_path, n_papers=3)
        mock_extract.side_effect = self._make_fake_extract(spec)

        stats = run_extraction(db, spec, "test_review", restart_every=3, run_id=self.run_id)
        assert stats["extracted"] == 3
        assert stats["failed"] == 0
        db.close()

    @patch("engine.agents.extractor.restart_ollama")
    @patch("engine.agents.extractor.ollama_chat")
    @patch("engine.utils.ollama_preflight.require_preflight")
    @patch("engine.utils.ollama_client.fetch_model_digest", return_value="a" * 64)
    @patch("engine.utils.extraction_cleanup.check_stale_extractions", return_value=0)
    def test_zero_span_extraction_marks_extract_failed(
        self, _stale, _digest, _preflight, mock_chat, _restart, tmp_path,
    ):
        """LLM returns valid JSON with zero fields → retried under the budget, then
        a paper event extraction_failed / no_fields_returned (9b-FLIP T5); no rows."""
        db, spec = self._setup_db(tmp_path, n_papers=1)

        # Pass 1 response (reasoning)
        pass1_resp = MagicMock()
        pass1_resp.message.content = "<think>Reasoning about the paper.</think>"
        # 9b-FLIP: a real thinking channel. With a MagicMock here the attempt died
        # on ExtractionResult's reasoning_trace (a ValidationError) and never
        # reached the zero-span path this test is named for.
        pass1_resp.message.thinking = "Reasoning about the paper."

        # Pass 2 response (structured JSON with empty fields list)
        zero_span_output = ExtractionOutput(fields=[])
        pass2_resp = MagicMock()
        pass2_resp.message.content = zero_span_output.model_dump_json()

        mock_chat.side_effect = [pass1_resp, pass2_resp] * 3   # the budget of 3

        stats = run_extraction(db, spec, "test_review", restart_every=0, run_id=self.run_id)

        assert stats["failed"] == 1
        assert stats["extracted"] == 0

        # 9b-FLIP: the outcome is a paper event, never a status write.
        paper = db._conn.execute("SELECT status FROM papers WHERE id = 1").fetchone()
        assert paper["status"] == "FT_ELIGIBLE"
        ev = db._conn.execute(
            "SELECT to_state, reason_code FROM paper_events WHERE paper_id = 1 "
            "AND event_type = 'extraction_failed'").fetchall()
        assert [tuple(r) for r in ev] == [("extraction_failed", "no_fields_returned")]
        assert mock_chat.call_count == 6
        db.close()

    @pytest.mark.parametrize("kind", ["overflow", "truncated", "dropped"])
    @patch("engine.agents.extractor.restart_ollama")
    @patch("engine.agents.extractor.extract_paper")
    @patch("engine.utils.ollama_preflight.require_preflight")
    @patch("engine.utils.ollama_client.fetch_model_digest", return_value="a" * 64)
    @patch("engine.utils.extraction_cleanup.check_stale_extractions", return_value=0)
    def test_input_fit_failure_fails_the_paper_and_the_run_continues(
        self, _stale, _digest, _preflight, mock_extract, _restart, kind, tmp_path, caplog,
    ):
        """T6 (INPUT-FIT-01): each input-fit exception lands in EXTRACT_FAILED with its
        fields in the failure log entry, is not retried, and the next paper runs."""
        from engine.utils.ollama_client import InputDropped, InputOverflow, InputTruncated

        exc = {
            "overflow": InputOverflow(model="deepseek-r1:32b", chars=800_000,
                                      estimate_low=152_000.0, ceiling=131_072),
            "truncated": InputTruncated(model="deepseek-r1:32b", count=131_072,
                                        ceiling=131_072, chars=305_628),
            "dropped": InputDropped(model="deepseek-r1:32b", count=900, chars=400_000,
                                    floor=40_000.0, ceiling=131_072),
        }[kind]
        db, spec = self._setup_db(tmp_path, n_papers=2)
        fake = self._make_fake_extract(spec)

        def extract(paper_id, *args, **kwargs):
            if paper_id == 1:
                raise exc
            return fake(paper_id, *args, **kwargs)

        mock_extract.side_effect = extract
        with caplog.at_level("ERROR", logger="engine.agents.extractor"):
            stats = run_extraction(db, spec, "test_review", restart_every=0, run_id=self.run_id)

        assert stats["failed"] == 1 and stats["extracted"] == 1
        assert mock_extract.call_count == 2  # paper 1 not retried; paper 2 ran
        # 9b-FLIP: an input-fit refusal is a paper event (R120), not a status write.
        status = db._conn.execute("SELECT status FROM papers WHERE id = 1").fetchone()["status"]
        assert status == "FT_ELIGIBLE"
        ev = db._conn.execute(
            "SELECT to_state, reason_code FROM paper_events WHERE paper_id = 1 "
            "AND event_type = 'extraction_failed'").fetchone()
        assert tuple(ev) == ("input_exceeds_context", {
            "overflow": "input_overflow_estimated", "truncated": "input_truncated_at_ceiling",
            "dropped": "input_dropped_below_floor"}[kind])
        entry = next(r.getMessage() for r in caplog.records
                     if r.getMessage().startswith("Paper 1 extraction failed"))
        assert "input_fit=" in entry
        for name, value in exc.fields.items():
            assert f"'{name}': {value!r}" in entry
        db.close()

    # ── helpers ──

    def _setup_db(self, tmp_path, n_papers=5):
        """Create a minimal in-memory-style DB with N FT_ELIGIBLE papers."""
        db_path = str(tmp_path / "test.db")
        _write_codebook(tmp_path)
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
            model_digest TEXT, auditor_model_digest TEXT, extracted_at TEXT,
            codebook_hash TEXT, codebook_sha256 TEXT)""")
        conn.execute("""CREATE TABLE evidence_spans (
            id INTEGER PRIMARY KEY, extraction_id INTEGER, field_name TEXT,
            value TEXT, source_snippet TEXT, confidence REAL)""")
        conn.execute(_M021.table_sql())
        conn.commit()
        upgrade_event_store(db_path)  # 9b-2a: selection reads the eligibility axis
        for i in range(1, n_papers + 1):
            conn.execute(
                "INSERT INTO papers (id, title, status, added_at) VALUES (?, ?, 'FT_ELIGIBLE', '2026-01-01')",
                (i, f"Test Paper {i}"),
            )
            seed_eligibility(conn, i)
            # Create and record parsed text (S3e: the resolver reads references)
            write_parsed(conn, i, f"Paper {i} text content.", version=1)
        conn.commit()
        db._conn = conn
        db.db_path = db_path
        db.review_name = "test_review"
        spec = load_review_spec(str(SPEC_PATH))
        self.run_id = open_extraction_run(db, spec)   # 9b-2b: run_id is required
        return db, spec

    def _make_fake_extract(self, spec):
        """Return a fake extract_paper that returns a result and writes nothing.

        The real extract_paper writes events; `run_extraction` counts a paper by
        this return. 9c-C1 (R158): the legacy INSERT it used to make was read by
        nothing after the cut-over."""

        def _fake(paper_id, paper_text, spec_arg, db, **kwargs):
            schema_hash = load_codebook_beside(db.db_path).semantic_hash
            from engine.agents.models import ExtractionResult, EvidenceSpan
            result = ExtractionResult(
                paper_id=paper_id,
                fields=[EvidenceSpan(
                    field_name="study_type", value="RCT",
                    source_snippet="A randomized trial.", confidence=0.9, tier=1,
                )],
                reasoning_trace="test",
                model="test-model",
                codebook_hash=schema_hash,
                extracted_at=datetime.now(timezone.utc),
            )
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
            patch("engine.utils.ollama_client.fetch_model_digest", return_value="a" * 64),
            patch("engine.agents.extractor.extract_paper", side_effect=fake_extract),
            patch("engine.agents.extractor.restart_ollama",
                  side_effect=RuntimeError("Ollama did not respond")),
        ):
            with caplog.at_level("ERROR", logger="engine.agents.extractor"):
                stats = run_extraction(db, spec, "test_review", restart_every=1, run_id=self.run_id)

        # All 3 papers should have been extracted despite restart failures
        assert stats["extracted"] == 3
        assert stats["failed"] == 0

        # The error should be logged
        assert "Proactive Ollama restart failed" in caplog.text
        db._conn.close()

    # Re-use helpers from TestRunExtraction
    def _setup_db(self, tmp_path, n_papers=3):
        db_path = str(tmp_path / "test.db")
        _write_codebook(tmp_path)
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
            model_digest TEXT, auditor_model_digest TEXT, extracted_at TEXT,
            codebook_hash TEXT, codebook_sha256 TEXT)""")
        conn.execute("""CREATE TABLE evidence_spans (
            id INTEGER PRIMARY KEY, extraction_id INTEGER, field_name TEXT,
            value TEXT, source_snippet TEXT, confidence REAL)""")
        conn.execute(_M021.table_sql())
        conn.commit()
        upgrade_event_store(db_path)  # 9b-2a: selection reads the eligibility axis
        for i in range(1, n_papers + 1):
            conn.execute(
                "INSERT INTO papers (id, title, status, added_at) VALUES (?, ?, 'FT_ELIGIBLE', '2026-01-01')",
                (i, f"Test Paper {i}"),
            )
            seed_eligibility(conn, i)
            write_parsed(conn, i, f"Paper {i} text content.", version=1)
        conn.commit()
        db._conn = conn
        db.db_path = db_path
        db.review_name = "test_review"
        spec = load_review_spec(str(SPEC_PATH))
        self.run_id = open_extraction_run(db, spec)   # 9b-2b: run_id is required
        return db, spec

    def _make_fake_extract(self, spec):
        def _fake(paper_id, paper_text, spec_arg, db, **kwargs):
            schema_hash = load_codebook_beside(db.db_path).semantic_hash
            from engine.agents.models import ExtractionResult, EvidenceSpan
            result = ExtractionResult(
                paper_id=paper_id,
                fields=[EvidenceSpan(
                    field_name="study_type", value="RCT",
                    source_snippet="A randomized trial.", confidence=0.9, tier=1,
                )],
                reasoning_trace="test",
                model="test-model",
                codebook_hash=schema_hash,
                extracted_at=datetime.now(timezone.utc),
            )
            return result
        return _fake
