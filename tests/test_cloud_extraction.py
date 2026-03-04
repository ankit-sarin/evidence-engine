"""Tests for cloud extraction module — all API calls mocked."""

import csv
import json
import shutil
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

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

        # Should have papers (all EXTRACTED/AUDITED, none with cloud extractions yet)
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
