"""Tests for Ollama pre-flight health check."""

import logging
from unittest.mock import patch, MagicMock

import pytest

from engine.utils.ollama_preflight import (
    check_model,
    preflight_check,
    require_preflight,
    ModelResult,
)


# ── Unit Tests ───────────────────────────────────────────────────────


class TestCheckModel:

    def test_success_returns_ok(self):
        mock_response = MagicMock()
        mock_response.message.content = "OK"
        with patch("engine.utils.ollama_preflight.ollama_chat", return_value=mock_response):
            with patch("engine.utils.ollama_preflight._get_model_vram_gb", return_value=12.5):
                result = check_model("test-model:7b")

        assert result.status == "ok"
        assert result.model == "test-model:7b"
        assert result.load_time_seconds >= 0
        assert result.vram_used_gb == 12.5
        assert result.error_message == ""

    def test_error_returns_error_with_message(self):
        with patch("engine.utils.ollama_preflight.ollama_chat",
                   side_effect=ConnectionError("model not found")):
            result = check_model("nonexistent:99b")

        assert result.status == "error"
        assert result.model == "nonexistent:99b"
        assert "model not found" in result.error_message


class TestPreflightCheck:

    def test_all_ok_returns_success(self):
        mock_response = MagicMock()
        mock_response.message.content = "OK"
        with patch("engine.utils.ollama_preflight.ollama_chat", return_value=mock_response):
            with patch("engine.utils.ollama_preflight.ollama.ps",
                       return_value={"models": [{"name": "a", "size": 10 * 1024**3}]}):
                result = preflight_check(["model-a:7b", "model-b:13b"])

        assert result.success is True
        assert len(result.models) == 2
        assert all(m.status == "ok" for m in result.models)

    def test_one_failure_returns_failure(self):
        call_count = [0]
        mock_response = MagicMock()
        mock_response.message.content = "OK"

        def mock_chat(**kwargs):
            call_count[0] += 1
            if call_count[0] == 2:
                raise ConnectionError("OOM")
            return mock_response

        with patch("engine.utils.ollama_preflight.ollama_chat", side_effect=mock_chat):
            with patch("engine.utils.ollama_preflight.ollama.ps",
                       return_value={"models": []}):
                result = preflight_check(["good:7b", "bad:99b"])

        assert result.success is False
        assert result.models[0].status == "ok"
        assert result.models[1].status == "error"
        assert "OOM" in result.error_summary


class TestRequirePreflight:

    def test_raises_on_failure(self):
        with patch("engine.utils.ollama_preflight.ollama_chat",
                   side_effect=ConnectionError("dead")):
            with pytest.raises(RuntimeError, match="pre-flight check failed"):
                require_preflight(["broken:7b"], runner_name="Test")

    def test_passes_silently_on_success(self):
        mock_response = MagicMock()
        mock_response.message.content = "OK"
        with patch("engine.utils.ollama_preflight.ollama_chat", return_value=mock_response):
            with patch("engine.utils.ollama_preflight.ollama.ps",
                       return_value={"models": []}):
                # Should not raise
                require_preflight(["good:7b"], runner_name="Test")


# ── Integration with batch runners ───────────────────────────────────


class TestRunnerIntegration:

    def test_ft_screening_calls_preflight(self, tmp_path, caplog):
        """run_ft_screening calls preflight before processing."""
        from engine.core.database import ReviewDatabase
        from engine.core.review_spec import load_review_spec
        from engine.agents.ft_screener import run_ft_screening

        db = ReviewDatabase("test_pf", data_root=tmp_path)
        spec = load_review_spec("review_specs/surgical_autonomy_v1.yaml")

        # Mock preflight to track it was called, then fail to avoid actual screening
        with patch("engine.utils.ollama_preflight.require_preflight",
                   side_effect=RuntimeError("preflight failed")) as mock_pf:
            with pytest.raises(RuntimeError, match="preflight failed"):
                run_ft_screening(db, spec, review_name="test_pf")

            mock_pf.assert_called_once_with(
                [spec.ft_screening_models.primary, spec.ft_screening_models.verifier],
                runner_name="FT screening",
            )
        db.close()

    def test_extraction_calls_preflight(self, tmp_path):
        """run_extraction calls preflight before processing."""
        from engine.core.database import ReviewDatabase
        from engine.core.review_spec import load_review_spec
        from engine.agents.extractor import run_extraction, MODEL

        db = ReviewDatabase("test_pf2", data_root=tmp_path)
        spec = load_review_spec("review_specs/surgical_autonomy_v1.yaml")

        with patch("engine.utils.ollama_preflight.require_preflight",
                   side_effect=RuntimeError("preflight failed")) as mock_pf:
            with pytest.raises(RuntimeError, match="preflight failed"):
                run_extraction(db, spec, review_name="test_pf2")

            mock_pf.assert_called_once_with([MODEL], runner_name="Extraction")
        db.close()

    def test_audit_calls_preflight(self, tmp_path):
        """run_audit calls preflight before processing."""
        from engine.core.database import ReviewDatabase
        from engine.agents.auditor import run_audit, DEFAULT_AUDITOR_MODEL

        db = ReviewDatabase("test_pf3", data_root=tmp_path)

        with patch("engine.utils.ollama_preflight.require_preflight",
                   side_effect=RuntimeError("preflight failed")) as mock_pf:
            with pytest.raises(RuntimeError, match="preflight failed"):
                run_audit(db, review_name="test_pf3")

            mock_pf.assert_called_once_with(
                [DEFAULT_AUDITOR_MODEL], runner_name="Audit",
            )
        db.close()

    def test_runner_proceeds_on_preflight_success(self, tmp_path):
        """When preflight passes, runner continues to main loop (no papers = clean exit)."""
        from engine.core.database import ReviewDatabase
        from engine.core.review_spec import load_review_spec
        from engine.agents.extractor import run_extraction

        db = ReviewDatabase("test_pf4", data_root=tmp_path)
        spec = load_review_spec("review_specs/surgical_autonomy_v1.yaml")

        mock_response = MagicMock()
        mock_response.message.content = "OK"
        with patch("engine.utils.ollama_preflight.ollama_chat", return_value=mock_response):
            with patch("engine.utils.ollama_preflight.ollama.ps",
                       return_value={"models": []}):
                stats = run_extraction(db, spec, review_name="test_pf4")

        # No papers to extract, but runner completed without error
        assert stats["extracted"] == 0
        assert stats["failed"] == 0
        db.close()
