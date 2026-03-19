"""Tests for cloud extraction module — all API calls mocked."""

import csv
import json
import shutil
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import anthropic
import openai
import pytest

from engine.cloud.schema import init_cloud_tables
from engine.cloud.base import CloudExtractorBase
from engine.cloud.openai_extractor import OpenAIExtractor, COST_INPUT_PER_M, COST_OUTPUT_PER_M
from engine.cloud.anthropic_extractor import AnthropicExtractor

BACKUP_DB = Path(__file__).resolve().parent.parent / "data" / "surgical_autonomy" / "review_backup_v1_schema.db"
SPEC_PATH = Path(__file__).resolve().parent.parent / "review_specs" / "surgical_autonomy_v1.yaml"

pytestmark = pytest.mark.skipif(
    not BACKUP_DB.exists() or not SPEC_PATH.exists(),
    reason="Backup database or spec not available",
)

# ── Shared fixtures ──────────────────────────────────────────────────


@pytest.fixture()
def test_db(tmp_path):
    """Copy backup DB to temp dir so tests don't modify the original."""
    db_copy = tmp_path / "review.db"
    shutil.copy2(BACKUP_DB, db_copy)
    # Also copy parsed_text dir structure with a minimal test file
    parsed_dir = tmp_path / "parsed_text"
    parsed_dir.mkdir()
    # Create a fake parsed text for the first paper that has an extraction
    conn = sqlite3.connect(str(db_copy))
    conn.row_factory = sqlite3.Row
    # Migrate legacy AUDITED status to AI_AUDIT_COMPLETE
    conn.execute("UPDATE papers SET status = 'AI_AUDIT_COMPLETE' WHERE status = 'AUDITED'")
    conn.commit()
    row = conn.execute(
        "SELECT paper_id FROM extractions ORDER BY paper_id LIMIT 1"
    ).fetchone()
    first_pid = row[0] if row else 1
    conn.close()
    (parsed_dir / f"{first_pid}_v1.md").write_text(
        "# Test Paper\n\nThis is a test paper about surgical robotics."
    )
    return str(db_copy)


@pytest.fixture()
def spec_path():
    return str(SPEC_PATH)


# Sample valid extraction JSON matching ExtractionOutput schema
SAMPLE_FIELDS = [
    {
        "field_name": "study_type",
        "value": "Original Research",
        "source_snippet": "This is an original research study.",
        "confidence": 0.9,
        "tier": 1,
    },
    {
        "field_name": "robot_platform",
        "value": "da Vinci",
        "source_snippet": "Using the da Vinci surgical system.",
        "confidence": 0.85,
        "tier": 1,
    },
]

SAMPLE_RESPONSE = {"fields": SAMPLE_FIELDS}


# ── init_cloud_tables ────────────────────────────────────────────────


class TestInitCloudTables:
    def test_creates_tables(self, test_db):
        init_cloud_tables(test_db)

        conn = sqlite3.connect(test_db)
        tables = [
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        ]
        conn.close()

        assert "cloud_extractions" in tables
        assert "cloud_evidence_spans" in tables

    def test_tables_have_expected_columns(self, test_db):
        init_cloud_tables(test_db)

        conn = sqlite3.connect(test_db)
        ext_cols = [
            r[1] for r in conn.execute("PRAGMA table_info(cloud_extractions)").fetchall()
        ]
        span_cols = [
            r[1] for r in conn.execute("PRAGMA table_info(cloud_evidence_spans)").fetchall()
        ]
        conn.close()

        assert "paper_id" in ext_cols
        assert "arm" in ext_cols
        assert "model_string" in ext_cols
        assert "cost_usd" in ext_cols
        assert "reasoning_tokens" in ext_cols

        assert "cloud_extraction_id" in span_cols
        assert "field_name" in span_cols
        assert "tier" in span_cols

    def test_idempotent(self, test_db):
        init_cloud_tables(test_db)
        init_cloud_tables(test_db)  # Should not raise

        conn = sqlite3.connect(test_db)
        count = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE name='cloud_extractions'"
        ).fetchone()[0]
        conn.close()
        assert count == 1


# ── CloudExtractorBase ───────────────────────────────────────────────


class TestCloudExtractorBase:
    def test_get_pending_papers(self, test_db, spec_path):
        base = CloudExtractorBase(test_db, spec_path)
        pending = base.get_pending_papers("openai_o3_mini_high")

        # Should have papers (all EXTRACTED/AI_AUDIT_COMPLETE, none with cloud extractions yet)
        assert len(pending) > 0
        assert all("paper_id" in p for p in pending)
        base.close()

    def test_get_pending_excludes_completed(self, test_db, spec_path):
        base = CloudExtractorBase(test_db, spec_path)
        pending_before = base.get_pending_papers("openai_o3_mini_high")

        # Store a result for the first paper
        if pending_before:
            pid = pending_before[0]["paper_id"]
            base.store_result(
                paper_id=pid,
                arm="openai_o3_mini_high",
                model_string="o3-mini",
                extracted_data=SAMPLE_RESPONSE,
                reasoning_trace="test trace",
                prompt_text="test prompt",
                input_tokens=100,
                output_tokens=200,
                reasoning_tokens=50,
                cost_usd=0.001,
                spans=SAMPLE_FIELDS,
            )
            pending_after = base.get_pending_papers("openai_o3_mini_high")
            assert len(pending_after) == len(pending_before) - 1

        base.close()

    def test_parse_response_valid_json(self, test_db, spec_path):
        base = CloudExtractorBase(test_db, spec_path)
        spans = base.parse_response_to_spans(SAMPLE_RESPONSE)

        assert len(spans) == 2
        assert spans[0]["field_name"] == "study_type"
        assert spans[0]["value"] == "Original Research"
        assert spans[0]["tier"] == 1
        base.close()

    def test_parse_response_malformed_json(self, test_db, spec_path):
        base = CloudExtractorBase(test_db, spec_path)

        # String that isn't JSON
        spans = base.parse_response_to_spans("not json at all")
        assert spans == []

        # Dict with no 'fields' key
        spans = base.parse_response_to_spans({"study_type": "RCT"})
        assert spans == []

        base.close()

    def test_parse_response_bare_list(self, test_db, spec_path):
        base = CloudExtractorBase(test_db, spec_path)
        spans = base.parse_response_to_spans(SAMPLE_FIELDS)
        assert len(spans) == 2
        base.close()

    def test_parse_response_data_extraction_key(self, test_db, spec_path):
        base = CloudExtractorBase(test_db, spec_path)
        spans = base.parse_response_to_spans({"data_extraction": SAMPLE_FIELDS})
        assert len(spans) == 2
        assert spans[0]["field_name"] == "study_type"
        base.close()

    def test_parse_response_recovers_from_raw(self, test_db, spec_path):
        import json
        base = CloudExtractorBase(test_db, spec_path)
        raw_json = "```json\n" + json.dumps(SAMPLE_FIELDS) + "\n```"
        spans = base.parse_response_to_spans({"fields": [], "raw": raw_json})
        assert len(spans) == 2
        base.close()

    def test_store_result_rejects_empty_spans(self, test_db, spec_path):
        base = CloudExtractorBase(test_db, spec_path)
        pending = base.get_pending_papers("test_arm")
        if not pending:
            base.close()
            pytest.skip("No pending papers")

        pid = pending[0]["paper_id"]
        with pytest.raises(ValueError, match="0 spans"):
            base.store_result(
                paper_id=pid,
                arm="test_arm",
                model_string="test-model",
                extracted_data={"fields": []},
                reasoning_trace="",
                prompt_text="",
                input_tokens=0,
                output_tokens=0,
                reasoning_tokens=0,
                cost_usd=0.0,
                spans=[],
            )

        # Verify nothing was written
        row = base._conn.execute(
            "SELECT COUNT(*) FROM cloud_extractions WHERE paper_id = ? AND arm = 'test_arm'",
            (pid,),
        ).fetchone()
        assert row[0] == 0
        base.close()

    def test_store_result_atomic(self, test_db, spec_path):
        base = CloudExtractorBase(test_db, spec_path)
        pending = base.get_pending_papers("test_arm")
        if not pending:
            base.close()
            pytest.skip("No pending papers")

        pid = pending[0]["paper_id"]
        ext_id = base.store_result(
            paper_id=pid,
            arm="test_arm",
            model_string="test-model",
            extracted_data=SAMPLE_RESPONSE,
            reasoning_trace="test trace",
            prompt_text="test prompt",
            input_tokens=100,
            output_tokens=200,
            reasoning_tokens=50,
            cost_usd=0.001,
            spans=SAMPLE_FIELDS,
        )

        # Verify extraction row
        row = base._conn.execute(
            "SELECT * FROM cloud_extractions WHERE id = ?", (ext_id,)
        ).fetchone()
        assert row is not None
        assert row["arm"] == "test_arm"
        assert row["cost_usd"] == 0.001

        # Verify span rows
        span_rows = base._conn.execute(
            "SELECT * FROM cloud_evidence_spans WHERE cloud_extraction_id = ?",
            (ext_id,),
        ).fetchall()
        assert len(span_rows) == 2
        base.close()

    def test_get_progress(self, test_db, spec_path):
        base = CloudExtractorBase(test_db, spec_path)
        progress = base.get_progress("openai_o3_mini_high")

        assert "total_papers" in progress
        assert "completed" in progress
        assert "remaining" in progress
        assert "total_cost_usd" in progress
        assert progress["completed"] == 0
        assert progress["remaining"] == progress["total_papers"]
        base.close()


# ── OpenAIExtractor (mocked) ────────────────────────────────────────


class TestOpenAIExtractor:
    def _make_mock_response(self, content_json, input_toks=1000, output_toks=500, reasoning_toks=200):
        """Build a mock OpenAI API response."""
        details = SimpleNamespace(reasoning_tokens=reasoning_toks)
        usage = SimpleNamespace(
            prompt_tokens=input_toks,
            completion_tokens=output_toks,
            completion_tokens_details=details,
        )
        message = SimpleNamespace(
            content=json.dumps(content_json),
            reasoning_content="This is the reasoning trace about the paper.",
        )
        choice = SimpleNamespace(message=message)
        return SimpleNamespace(choices=[choice], usage=usage)

    @patch("engine.cloud.openai_extractor.openai.OpenAI")
    def test_extract_paper(self, mock_openai_cls, test_db, spec_path):
        mock_client = MagicMock()
        mock_openai_cls.return_value = mock_client
        mock_client.chat.completions.create.return_value = self._make_mock_response(
            SAMPLE_RESPONSE
        )

        extractor = OpenAIExtractor(test_db, spec_path, api_key="test-key")

        # Get a real paper ID
        pending = extractor.get_pending_papers(extractor.ARM)
        if not pending:
            extractor.close()
            pytest.skip("No pending papers")
        pid = pending[0]["paper_id"]

        result = extractor.extract_paper(pid, "Test paper text about surgery.")

        assert result["paper_id"] == pid
        assert len(result["spans"]) == 2
        assert result["input_tokens"] == 1000
        assert result["output_tokens"] == 500
        assert result["reasoning_tokens"] == 200
        assert result["reasoning_trace"] == "This is the reasoning trace about the paper."
        extractor.close()

    @patch("engine.cloud.openai_extractor.openai.OpenAI")
    def test_cost_calculation(self, mock_openai_cls, test_db, spec_path):
        mock_client = MagicMock()
        mock_openai_cls.return_value = mock_client
        mock_client.chat.completions.create.return_value = self._make_mock_response(
            SAMPLE_RESPONSE, input_toks=1_000_000, output_toks=1_000_000,
        )

        extractor = OpenAIExtractor(test_db, spec_path, api_key="test-key")
        pending = extractor.get_pending_papers(extractor.ARM)
        if not pending:
            extractor.close()
            pytest.skip("No pending papers")

        result = extractor.extract_paper(
            pending[0]["paper_id"], "Test paper text."
        )

        expected_cost = COST_INPUT_PER_M + COST_OUTPUT_PER_M  # 1M each
        assert abs(result["cost_usd"] - expected_cost) < 0.01
        extractor.close()


# ── AnthropicExtractor (mocked) ─────────────────────────────────────


class TestAnthropicExtractor:
    def _make_mock_response(self, content_json, input_toks=2000, output_toks=1000):
        """Build a mock Anthropic API response."""
        thinking_block = SimpleNamespace(
            type="thinking",
            thinking="Let me analyze this paper step by step...",
        )
        text_block = SimpleNamespace(
            type="text",
            text=json.dumps(content_json),
        )
        usage = SimpleNamespace(
            input_tokens=input_toks,
            output_tokens=output_toks,
        )
        return SimpleNamespace(content=[thinking_block, text_block], usage=usage)

    @patch("engine.cloud.anthropic_extractor.anthropic.Anthropic")
    def test_extract_paper(self, mock_anthropic_cls, test_db, spec_path):
        mock_client = MagicMock()
        mock_anthropic_cls.return_value = mock_client
        mock_client.messages.create.return_value = self._make_mock_response(
            SAMPLE_RESPONSE
        )

        extractor = AnthropicExtractor(test_db, spec_path, api_key="test-key")
        pending = extractor.get_pending_papers(extractor.ARM)
        if not pending:
            extractor.close()
            pytest.skip("No pending papers")
        pid = pending[0]["paper_id"]

        result = extractor.extract_paper(pid, "Test paper text about surgery.")

        assert result["paper_id"] == pid
        assert len(result["spans"]) == 2
        assert result["input_tokens"] == 2000
        assert result["output_tokens"] == 1000
        extractor.close()

    @patch("engine.cloud.anthropic_extractor.anthropic.Anthropic")
    def test_reasoning_trace_from_thinking_blocks(self, mock_anthropic_cls, test_db, spec_path):
        mock_client = MagicMock()
        mock_anthropic_cls.return_value = mock_client
        mock_client.messages.create.return_value = self._make_mock_response(
            SAMPLE_RESPONSE
        )

        extractor = AnthropicExtractor(test_db, spec_path, api_key="test-key")
        pending = extractor.get_pending_papers(extractor.ARM)
        if not pending:
            extractor.close()
            pytest.skip("No pending papers")

        result = extractor.extract_paper(
            pending[0]["paper_id"], "Test paper text."
        )

        assert "analyze this paper" in result["reasoning_trace"]
        extractor.close()

    @patch("engine.cloud.anthropic_extractor.anthropic.Anthropic")
    def test_json_extracted_from_text_blocks(self, mock_anthropic_cls, test_db, spec_path):
        mock_client = MagicMock()
        mock_anthropic_cls.return_value = mock_client
        mock_client.messages.create.return_value = self._make_mock_response(
            SAMPLE_RESPONSE
        )

        extractor = AnthropicExtractor(test_db, spec_path, api_key="test-key")
        pending = extractor.get_pending_papers(extractor.ARM)
        if not pending:
            extractor.close()
            pytest.skip("No pending papers")

        result = extractor.extract_paper(
            pending[0]["paper_id"], "Test paper text."
        )

        assert "fields" in result["extracted_data"]
        assert len(result["extracted_data"]["fields"]) == 2
        extractor.close()

    @patch("engine.cloud.anthropic_extractor.anthropic.Anthropic")
    def test_cost_calculation(self, mock_anthropic_cls, test_db, spec_path):
        mock_client = MagicMock()
        mock_anthropic_cls.return_value = mock_client

        from engine.cloud.anthropic_extractor import COST_INPUT_PER_M as A_IN, COST_OUTPUT_PER_M as A_OUT

        mock_client.messages.create.return_value = self._make_mock_response(
            SAMPLE_RESPONSE, input_toks=1_000_000, output_toks=1_000_000,
        )

        extractor = AnthropicExtractor(test_db, spec_path, api_key="test-key")
        pending = extractor.get_pending_papers(extractor.ARM)
        if not pending:
            extractor.close()
            pytest.skip("No pending papers")

        result = extractor.extract_paper(
            pending[0]["paper_id"], "Test paper text."
        )

        expected_cost = A_IN + A_OUT  # 1M each
        assert abs(result["cost_usd"] - expected_cost) < 0.01
        extractor.close()


# ── CLI tests ────────────────────────────────────────────────────────


class TestCLI:
    def test_progress_no_api_calls(self, test_db, spec_path):
        """--progress should not instantiate any API client."""
        from scripts.run_cloud_extraction import show_progress
        # This should work without API keys since it only reads the DB
        show_progress(test_db, spec_path)

    def test_dry_run_no_api_calls(self, test_db, spec_path):
        """--dry-run should not instantiate any API client."""
        from scripts.run_cloud_extraction import dry_run
        # This should work without API keys
        dry_run(test_db, spec_path, ["openai", "anthropic"])


# ── Empty-string normalization (Anthropic) ────────────────────────


class TestSonnetEmptyStringNormalization:
    """Verify Anthropic extractor normalizes empty strings to null with annotation."""

    def _make_mock_response(self, content_json, input_toks=2000, output_toks=1000):
        thinking_block = SimpleNamespace(
            type="thinking",
            thinking="Analyzing the paper...",
        )
        text_block = SimpleNamespace(
            type="text",
            text=json.dumps(content_json),
        )
        usage = SimpleNamespace(
            input_tokens=input_toks,
            output_tokens=output_toks,
        )
        return SimpleNamespace(content=[thinking_block, text_block], usage=usage)

    @patch("engine.cloud.anthropic_extractor.anthropic.Anthropic")
    def test_empty_string_normalized_to_null(self, mock_anthropic_cls, test_db, spec_path):
        """Empty string '' gets normalized to null with 'empty_string_to_null' annotation."""
        mock_client = MagicMock()
        mock_anthropic_cls.return_value = mock_client

        fields_with_empty = {"fields": [
            {"field_name": "study_type", "value": "", "source_snippet": "snippet", "confidence": 0.9, "tier": 1},
        ]}
        mock_client.messages.create.return_value = self._make_mock_response(fields_with_empty)

        extractor = AnthropicExtractor(test_db, spec_path, api_key="test-key")
        pending = extractor.get_pending_papers(extractor.ARM)
        if not pending:
            extractor.close()
            pytest.skip("No pending papers")

        result = extractor.extract_paper(pending[0]["paper_id"], "Test text.")
        span = result["spans"][0]
        assert span["value"] is None
        assert span["notes"] == "empty_string_to_null"

        # Verify it round-trips through store_result
        ext_id = extractor.store_result(
            paper_id=pending[0]["paper_id"], arm=extractor.ARM,
            model_string=extractor.model_string,
            extracted_data=result["extracted_data"],
            reasoning_trace=result["reasoning_trace"],
            prompt_text=result["prompt_text"],
            input_tokens=result["input_tokens"],
            output_tokens=result["output_tokens"],
            reasoning_tokens=result["reasoning_tokens"],
            cost_usd=result["cost_usd"], spans=result["spans"],
        )
        row = extractor._conn.execute(
            "SELECT value, notes FROM cloud_evidence_spans WHERE cloud_extraction_id = ?",
            (ext_id,),
        ).fetchone()
        assert row["value"] is None
        assert row["notes"] == "empty_string_to_null"
        extractor.close()

    @patch("engine.cloud.anthropic_extractor.anthropic.Anthropic")
    def test_null_converted_to_nr(self, mock_anthropic_cls, test_db, spec_path):
        """Null value from Sonnet is converted to 'NR' before Pydantic validation.

        After Fix 2, null values are converted to 'NR' in parse_response_to_spans,
        so they survive Pydantic validation. The empty-string normalization in
        extract_paper does NOT fire (value is 'NR', not '').
        """
        mock_client = MagicMock()
        mock_anthropic_cls.return_value = mock_client

        fields_with_null = {"fields": [
            {"field_name": "study_type", "value": None, "source_snippet": "snippet", "confidence": 0.9, "tier": 1},
            {"field_name": "sample_size", "value": "42", "source_snippet": "n=42", "confidence": 0.8, "tier": 1},
        ]}
        mock_client.messages.create.return_value = self._make_mock_response(fields_with_null)

        extractor = AnthropicExtractor(test_db, spec_path, api_key="test-key")
        pending = extractor.get_pending_papers(extractor.ARM)
        if not pending:
            extractor.close()
            pytest.skip("No pending papers")

        result = extractor.extract_paper(pending[0]["paper_id"], "Test text.")
        # Both spans survive now — null was converted to "NR"
        assert len(result["spans"]) == 2
        nr_span = [s for s in result["spans"] if s["field_name"] == "study_type"][0]
        assert nr_span["value"] == "NR"
        assert nr_span["source_snippet"] == ""
        # No empty_string_to_null annotation (value is "NR", not "")
        assert nr_span.get("notes") is None
        # Non-null span unchanged
        num_span = [s for s in result["spans"] if s["field_name"] == "sample_size"][0]
        assert num_span["value"] == "42"
        extractor.close()

    @patch("engine.cloud.anthropic_extractor.anthropic.Anthropic")
    def test_nonempty_string_unchanged(self, mock_anthropic_cls, test_db, spec_path):
        """Non-empty string value is left as-is, no annotation."""
        mock_client = MagicMock()
        mock_anthropic_cls.return_value = mock_client

        fields_normal = {"fields": [
            {"field_name": "study_type", "value": "RCT", "source_snippet": "snippet", "confidence": 0.9, "tier": 1},
        ]}
        mock_client.messages.create.return_value = self._make_mock_response(fields_normal)

        extractor = AnthropicExtractor(test_db, spec_path, api_key="test-key")
        pending = extractor.get_pending_papers(extractor.ARM)
        if not pending:
            extractor.close()
            pytest.skip("No pending papers")

        result = extractor.extract_paper(pending[0]["paper_id"], "Test text.")
        span = result["spans"][0]
        assert span["value"] == "RCT"
        assert span.get("notes") is None
        extractor.close()


# ── Rate limit backoff (Anthropic) ────────────────────────────────


class TestSonnetRateLimitBackoff:
    """Verify 429 responses trigger longer backoff than generic errors."""

    @patch("engine.cloud.anthropic_extractor.time.sleep")
    @patch("engine.cloud.anthropic_extractor.anthropic.Anthropic")
    def test_429_triggers_30s_plus_backoff(self, mock_anthropic_cls, mock_sleep, test_db, spec_path):
        """Rate limit error uses 30s+ exponential backoff, not the 2s/4s default."""
        import anthropic as anthropic_mod

        mock_client = MagicMock()
        mock_anthropic_cls.return_value = mock_client

        # Build a mock 429 response
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.headers = {}

        rate_err = anthropic_mod.RateLimitError(
            message="rate limit exceeded",
            response=mock_response,
            body={"error": {"type": "rate_limit_error", "message": "rate limit"}},
        )

        # First two calls raise 429, third succeeds
        thinking_block = SimpleNamespace(type="thinking", thinking="ok")
        text_block = SimpleNamespace(type="text", text=json.dumps(SAMPLE_RESPONSE))
        usage = SimpleNamespace(input_tokens=100, output_tokens=50)
        success_resp = SimpleNamespace(content=[thinking_block, text_block], usage=usage)

        mock_client.messages.create.side_effect = [rate_err, rate_err, success_resp]

        extractor = AnthropicExtractor(test_db, spec_path, api_key="test-key")
        pending = extractor.get_pending_papers(extractor.ARM)
        if not pending:
            extractor.close()
            pytest.skip("No pending papers")

        # Create parsed text file
        parsed_dir = Path(test_db).parent / "parsed_text"
        pid = pending[0]["paper_id"]
        (parsed_dir / f"{pid}_v1.md").write_text("Test paper text.")

        extractor.run(max_papers=1)

        # Verify sleep was called with 30s+ values (not 2/4)
        sleep_calls = [c.args[0] for c in mock_sleep.call_args_list]
        assert len(sleep_calls) == 2
        assert sleep_calls[0] >= 30  # first retry: 30s
        assert sleep_calls[1] >= 60  # second retry: 60s
        extractor.close()

    @patch("engine.cloud.anthropic_extractor.time.sleep")
    @patch("engine.cloud.anthropic_extractor.anthropic.Anthropic")
    def test_429_uses_retry_after_header(self, mock_anthropic_cls, mock_sleep, test_db, spec_path):
        """If retry-after header is present, use that value."""
        import anthropic as anthropic_mod

        mock_client = MagicMock()
        mock_anthropic_cls.return_value = mock_client

        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.headers = {"retry-after": "45"}

        rate_err = anthropic_mod.RateLimitError(
            message="rate limit exceeded",
            response=mock_response,
            body={"error": {"type": "rate_limit_error", "message": "rate limit"}},
        )

        thinking_block = SimpleNamespace(type="thinking", thinking="ok")
        text_block = SimpleNamespace(type="text", text=json.dumps(SAMPLE_RESPONSE))
        usage = SimpleNamespace(input_tokens=100, output_tokens=50)
        success_resp = SimpleNamespace(content=[thinking_block, text_block], usage=usage)

        mock_client.messages.create.side_effect = [rate_err, success_resp]

        extractor = AnthropicExtractor(test_db, spec_path, api_key="test-key")
        pending = extractor.get_pending_papers(extractor.ARM)
        if not pending:
            extractor.close()
            pytest.skip("No pending papers")

        parsed_dir = Path(test_db).parent / "parsed_text"
        pid = pending[0]["paper_id"]
        (parsed_dir / f"{pid}_v1.md").write_text("Test paper text.")

        extractor.run(max_papers=1)

        sleep_calls = [c.args[0] for c in mock_sleep.call_args_list]
        assert len(sleep_calls) == 1
        assert sleep_calls[0] == 45  # from retry-after header
        extractor.close()


# ── C2: store_result crash protection ──────────────────────────────


class TestStoreResultCrashProtection:
    """store_result failures are caught per-paper; run continues."""

    @patch("engine.cloud.openai_extractor.openai.OpenAI")
    def test_openai_store_failure_continues_run(self, mock_openai_cls, test_db, spec_path):
        """If store_result raises on paper 1, paper 2 is still processed."""
        mock_client = MagicMock()
        mock_openai_cls.return_value = mock_client

        # Both papers return valid extraction
        details = SimpleNamespace(reasoning_tokens=10)
        usage = SimpleNamespace(
            prompt_tokens=100, completion_tokens=50,
            completion_tokens_details=details,
        )
        msg = SimpleNamespace(
            content=json.dumps(SAMPLE_RESPONSE),
            reasoning_content="trace",
        )
        choice = SimpleNamespace(message=msg)
        resp = SimpleNamespace(choices=[choice], usage=usage)
        mock_client.chat.completions.create.return_value = resp

        extractor = OpenAIExtractor(test_db, spec_path, api_key="test-key")
        pending = extractor.get_pending_papers(extractor.ARM)
        if len(pending) < 2:
            extractor.close()
            pytest.skip("Need at least 2 pending papers")

        # Create parsed text for first 2 papers
        parsed_dir = Path(test_db).parent / "parsed_text"
        for p in pending[:2]:
            (parsed_dir / f"{p['paper_id']}_v1.md").write_text("Paper text.")

        # Make store_result fail on first call, succeed on second
        original_store = extractor.store_result
        call_count = [0]

        def _failing_store(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise sqlite3.IntegrityError("simulated constraint violation")
            return original_store(*args, **kwargs)

        extractor.store_result = _failing_store

        stats = extractor.run(max_papers=2)

        assert stats["failed"] == 1
        assert stats["extracted"] == 1
        extractor.close()

    @patch("engine.cloud.openai_extractor.openai.OpenAI")
    def test_distribution_collapse_not_caught(self, mock_openai_cls, test_db, spec_path):
        """DistributionCollapseError propagates — not caught by store_result handler."""
        from engine.validators.distribution_monitor import DistributionCollapseError

        mock_client = MagicMock()
        mock_openai_cls.return_value = mock_client

        extractor = OpenAIExtractor(test_db, spec_path, api_key="test-key")

        # Mock run_distribution_check to raise
        def _raise_collapse(stats):
            raise DistributionCollapseError([{"field_name": "study_type"}])

        extractor.run_distribution_check = _raise_collapse

        # Need at least one paper to get past the empty-pending check
        # but the error should fire after the loop, not per-paper
        with pytest.raises(DistributionCollapseError):
            extractor.run(max_papers=0)

        extractor.close()


# ── H2: Auth error immediate abort ─────────────────────────────────


class TestAuthErrorAbort:

    @patch("engine.cloud.openai_extractor.openai.OpenAI")
    def test_openai_401_aborts_immediately(self, mock_openai_cls, test_db, spec_path):
        """OpenAI AuthenticationError aborts run — no retries, no subsequent papers."""
        mock_client = MagicMock()
        mock_openai_cls.return_value = mock_client

        mock_client.chat.completions.create.side_effect = openai.AuthenticationError(
            message="Incorrect API key provided",
            response=MagicMock(status_code=401, headers={}),
            body={"error": {"message": "Incorrect API key"}},
        )

        extractor = OpenAIExtractor(test_db, spec_path, api_key="bad-key")
        pending = extractor.get_pending_papers(extractor.ARM)
        if not pending:
            extractor.close()
            pytest.skip("No pending papers")

        parsed_dir = Path(test_db).parent / "parsed_text"
        for p in pending[:2]:
            (parsed_dir / f"{p['paper_id']}_v1.md").write_text("Paper text.")

        with pytest.raises(openai.AuthenticationError):
            extractor.run(max_papers=2)

        # Should have called API only once (first paper, no retry)
        assert mock_client.chat.completions.create.call_count == 1
        extractor.close()

    @patch("engine.cloud.anthropic_extractor.anthropic.Anthropic")
    def test_anthropic_401_aborts_immediately(self, mock_anthropic_cls, test_db, spec_path):
        """Anthropic AuthenticationError aborts run immediately."""
        mock_client = MagicMock()
        mock_anthropic_cls.return_value = mock_client

        mock_client.messages.create.side_effect = anthropic.AuthenticationError(
            message="Invalid API key",
            response=MagicMock(status_code=401, headers={}),
            body={"error": {"type": "authentication_error", "message": "Invalid API key"}},
        )

        extractor = AnthropicExtractor(test_db, spec_path, api_key="bad-key")
        pending = extractor.get_pending_papers(extractor.ARM)
        if not pending:
            extractor.close()
            pytest.skip("No pending papers")

        parsed_dir = Path(test_db).parent / "parsed_text"
        for p in pending[:2]:
            (parsed_dir / f"{p['paper_id']}_v1.md").write_text("Paper text.")

        with pytest.raises(anthropic.AuthenticationError):
            extractor.run(max_papers=2)

        assert mock_client.messages.create.call_count == 1
        extractor.close()


# ── H3: OpenAI 429 rate limit backoff ──────────────────────────────


class TestOpenAIRateLimitBackoff:

    @patch("engine.cloud.openai_extractor.time.sleep")
    @patch("engine.cloud.openai_extractor.openai.OpenAI")
    def test_429_triggers_30s_plus_backoff(self, mock_openai_cls, mock_sleep, test_db, spec_path):
        """OpenAI rate limit uses 30s+ exponential backoff, not the 2/4/8s default."""
        mock_client = MagicMock()
        mock_openai_cls.return_value = mock_client

        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.headers = {}

        rate_err = openai.RateLimitError(
            message="rate limit exceeded",
            response=mock_response,
            body={"error": {"message": "rate limit exceeded"}},
        )

        # Two 429s then success
        details = SimpleNamespace(reasoning_tokens=10)
        usage = SimpleNamespace(
            prompt_tokens=100, completion_tokens=50,
            completion_tokens_details=details,
        )
        msg = SimpleNamespace(
            content=json.dumps(SAMPLE_RESPONSE),
            reasoning_content="trace",
        )
        choice = SimpleNamespace(message=msg)
        success_resp = SimpleNamespace(choices=[choice], usage=usage)

        mock_client.chat.completions.create.side_effect = [rate_err, rate_err, success_resp]

        extractor = OpenAIExtractor(test_db, spec_path, api_key="test-key")
        pending = extractor.get_pending_papers(extractor.ARM)
        if not pending:
            extractor.close()
            pytest.skip("No pending papers")

        parsed_dir = Path(test_db).parent / "parsed_text"
        pid = pending[0]["paper_id"]
        (parsed_dir / f"{pid}_v1.md").write_text("Test paper text.")

        extractor.run(max_papers=1)

        sleep_calls = [c.args[0] for c in mock_sleep.call_args_list]
        assert len(sleep_calls) == 2
        assert sleep_calls[0] >= 30  # first retry: 30s
        assert sleep_calls[1] >= 60  # second retry: 60s
        extractor.close()

    @patch("engine.cloud.openai_extractor.time.sleep")
    @patch("engine.cloud.openai_extractor.openai.OpenAI")
    def test_429_uses_retry_after_header(self, mock_openai_cls, mock_sleep, test_db, spec_path):
        """OpenAI rate limit respects retry-after header."""
        mock_client = MagicMock()
        mock_openai_cls.return_value = mock_client

        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.headers = {"retry-after": "45"}

        rate_err = openai.RateLimitError(
            message="rate limit exceeded",
            response=mock_response,
            body={"error": {"message": "rate limit exceeded"}},
        )

        details = SimpleNamespace(reasoning_tokens=10)
        usage = SimpleNamespace(
            prompt_tokens=100, completion_tokens=50,
            completion_tokens_details=details,
        )
        msg = SimpleNamespace(
            content=json.dumps(SAMPLE_RESPONSE),
            reasoning_content="trace",
        )
        choice = SimpleNamespace(message=msg)
        success_resp = SimpleNamespace(choices=[choice], usage=usage)

        mock_client.chat.completions.create.side_effect = [rate_err, success_resp]

        extractor = OpenAIExtractor(test_db, spec_path, api_key="test-key")
        pending = extractor.get_pending_papers(extractor.ARM)
        if not pending:
            extractor.close()
            pytest.skip("No pending papers")

        parsed_dir = Path(test_db).parent / "parsed_text"
        pid = pending[0]["paper_id"]
        (parsed_dir / f"{pid}_v1.md").write_text("Test paper text.")

        extractor.run(max_papers=1)

        sleep_calls = [c.args[0] for c in mock_sleep.call_args_list]
        assert len(sleep_calls) == 1
        assert sleep_calls[0] == 45
        extractor.close()


# ── M16: Spec-driven model names and cost rates ───────────────────


class TestSpecDrivenConfig:

    @patch("engine.cloud.openai_extractor.openai.OpenAI")
    def test_openai_reads_from_spec(self, mock_openai_cls, test_db, tmp_path):
        """OpenAIExtractor uses model/cost from spec when cloud_models is set."""
        import yaml
        from engine.core.review_spec import load_review_spec

        # Load existing spec, add cloud_models section, write to temp
        spec_path = str(SPEC_PATH)
        with open(spec_path) as f:
            spec_data = yaml.safe_load(f)

        spec_data["cloud_models"] = {
            "openai": {
                "model": "o4-mini-custom",
                "cost_input_per_m": 2.00,
                "cost_output_per_m": 8.00,
            },
        }
        custom_spec = tmp_path / "custom_spec.yaml"
        custom_spec.write_text(yaml.dump(spec_data))

        mock_openai_cls.return_value = MagicMock()

        extractor = OpenAIExtractor(test_db, str(custom_spec), api_key="test-key")
        assert extractor.model_string == "o4-mini-custom"
        assert extractor.cost_input_per_m == 2.00
        assert extractor.cost_output_per_m == 8.00
        extractor.close()

    @patch("engine.cloud.anthropic_extractor.anthropic.Anthropic")
    def test_anthropic_reads_from_spec(self, mock_anthropic_cls, test_db, tmp_path):
        """AnthropicExtractor uses model/cost from spec when cloud_models is set."""
        import yaml

        spec_path = str(SPEC_PATH)
        with open(spec_path) as f:
            spec_data = yaml.safe_load(f)

        spec_data["cloud_models"] = {
            "anthropic": {
                "model": "claude-custom-model",
                "cost_input_per_m": 5.00,
                "cost_output_per_m": 25.00,
            },
        }
        custom_spec = tmp_path / "custom_spec.yaml"
        custom_spec.write_text(yaml.dump(spec_data))

        mock_anthropic_cls.return_value = MagicMock()

        extractor = AnthropicExtractor(test_db, str(custom_spec), api_key="test-key")
        assert extractor.model_string == "claude-custom-model"
        assert extractor.cost_input_per_m == 5.00
        assert extractor.cost_output_per_m == 25.00
        extractor.close()

    @patch("engine.cloud.openai_extractor.openai.OpenAI")
    def test_openai_falls_back_to_defaults(self, mock_openai_cls, test_db, spec_path):
        """Without cloud_models in spec, OpenAI uses hardcoded defaults."""
        mock_openai_cls.return_value = MagicMock()

        from engine.cloud.openai_extractor import _DEFAULT_MODEL, _DEFAULT_COST_INPUT_PER_M, _DEFAULT_COST_OUTPUT_PER_M
        extractor = OpenAIExtractor(test_db, spec_path, api_key="test-key")
        assert extractor.model_string == _DEFAULT_MODEL
        assert extractor.cost_input_per_m == _DEFAULT_COST_INPUT_PER_M
        assert extractor.cost_output_per_m == _DEFAULT_COST_OUTPUT_PER_M
        extractor.close()
