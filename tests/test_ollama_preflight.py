"""Tests for Ollama pre-flight health check."""

import logging
from unittest.mock import patch, MagicMock

import pytest

from engine.utils.ollama_preflight import (
    check_model,
    check_ollama_env,
    preflight_check,
    require_preflight,
    ModelResult,
    _get_ollama_env,
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

    def test_vram_filters_to_relevant_models(self):
        """M4: VRAM calculation only counts models relevant to the current operation."""
        mock_response = MagicMock()
        mock_response.message.content = "OK"

        ps_data = {"models": [
            {"name": "qwen3:8b", "size": 8 * 1024**3},       # relevant
            {"name": "deepseek-r1:32b", "size": 32 * 1024**3},  # unrelated
        ]}

        with patch("engine.utils.ollama_preflight.ollama_chat", return_value=mock_response):
            with patch("engine.utils.ollama_preflight.ollama.ps", return_value=ps_data):
                result = preflight_check(["qwen3:8b"])

        # Only qwen3:8b should be counted (8 GB), not deepseek-r1:32b
        assert result.total_vram_gb == 8.0
        assert result.success is True

    def test_vram_unrelated_model_does_not_cause_rejection(self, caplog):
        """M4: Unrelated loaded model does not cause false VRAM rejection."""
        mock_response = MagicMock()
        mock_response.message.content = "OK"

        # Budget is 100 GB. Relevant = 8 GB (under budget).
        # Unrelated = 95 GB (if counted, total would exceed budget).
        ps_data = {"models": [
            {"name": "qwen3:8b", "size": 8 * 1024**3},
            {"name": "llama3:70b", "size": 95 * 1024**3},
        ]}

        with patch("engine.utils.ollama_preflight.ollama_chat", return_value=mock_response):
            with patch("engine.utils.ollama_preflight.ollama.ps", return_value=ps_data):
                with caplog.at_level("INFO", logger="engine.utils.ollama_preflight"):
                    result = preflight_check(["qwen3:8b"])

        assert result.success is True
        assert result.total_vram_gb == 8.0
        assert "llama3:70b" in caplog.text
        assert "not included in VRAM calculation" in caplog.text

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


class TestCheckOllamaEnv:

    def test_passes_when_env_correct(self):
        env = {
            "OLLAMA_FLASH_ATTENTION": "true",
            "OLLAMA_MAX_LOADED_MODELS": "1",
            "OLLAMA_KV_CACHE_TYPE": "f16",
        }
        with patch("engine.utils.ollama_preflight._get_ollama_env", return_value=env):
            check_ollama_env()  # should not raise

    def test_fails_when_flash_attention_missing(self):
        env = {"OLLAMA_MAX_LOADED_MODELS": "1"}
        with patch("engine.utils.ollama_preflight._get_ollama_env", return_value=env):
            with pytest.raises(RuntimeError, match="OLLAMA_FLASH_ATTENTION.*not set"):
                check_ollama_env()

    def test_fails_when_max_loaded_models_wrong(self):
        env = {"OLLAMA_FLASH_ATTENTION": "true", "OLLAMA_MAX_LOADED_MODELS": "4"}
        with patch("engine.utils.ollama_preflight._get_ollama_env", return_value=env):
            with pytest.raises(RuntimeError, match="OLLAMA_MAX_LOADED_MODELS.*expected='1'.*actual='4'"):
                check_ollama_env()

    def test_fails_lists_all_errors(self):
        with patch("engine.utils.ollama_preflight._get_ollama_env", return_value={}):
            with pytest.raises(RuntimeError, match="OLLAMA_FLASH_ATTENTION") as exc_info:
                check_ollama_env()
            assert "OLLAMA_MAX_LOADED_MODELS" in str(exc_info.value)

    def test_does_not_assert_kv_cache_type(self):
        """KV cache type is deliberately NOT checked — we may change it."""
        env = {
            "OLLAMA_FLASH_ATTENTION": "true",
            "OLLAMA_MAX_LOADED_MODELS": "1",
            # No OLLAMA_KV_CACHE_TYPE — should still pass
        }
        with patch("engine.utils.ollama_preflight._get_ollama_env", return_value=env):
            check_ollama_env()  # should not raise

    def test_get_ollama_env_parses_systemctl_output(self):
        mock_cp = MagicMock()
        mock_cp.stdout = "Environment=OLLAMA_FLASH_ATTENTION=true OLLAMA_MAX_LOADED_MODELS=1 PATH=/usr/bin\n"
        with patch("engine.utils.ollama_preflight.subprocess.run", return_value=mock_cp):
            env = _get_ollama_env()
        assert env["OLLAMA_FLASH_ATTENTION"] == "true"
        assert env["OLLAMA_MAX_LOADED_MODELS"] == "1"
        assert env["PATH"] == "/usr/bin"

    def test_get_ollama_env_returns_empty_on_error(self):
        with patch("engine.utils.ollama_preflight.subprocess.run",
                   side_effect=FileNotFoundError("no systemctl")):
            env = _get_ollama_env()
        assert env == {}


class TestRequirePreflight:

    def test_raises_on_failure(self):
        with patch("engine.utils.ollama_preflight.check_ollama_env"):
            with patch("engine.utils.ollama_preflight.ollama_chat",
                       side_effect=ConnectionError("dead")):
                with pytest.raises(RuntimeError, match="pre-flight check failed"):
                    require_preflight(["broken:7b"], runner_name="Test")

    def test_passes_silently_on_success(self):
        mock_response = MagicMock()
        mock_response.message.content = "OK"
        with patch("engine.utils.ollama_preflight.check_ollama_env"):
            with patch("engine.utils.ollama_preflight.ollama_chat", return_value=mock_response):
                with patch("engine.utils.ollama_preflight.ollama.ps",
                           return_value={"models": []}):
                    # Should not raise
                    require_preflight(["good:7b"], runner_name="Test")

    def test_env_check_runs_before_model_check(self):
        """If env check fails, model check should not run."""
        with patch("engine.utils.ollama_preflight.check_ollama_env",
                   side_effect=RuntimeError("env bad")):
            with patch("engine.utils.ollama_preflight.preflight_check") as mock_pf:
                with pytest.raises(RuntimeError, match="env bad"):
                    require_preflight(["model:7b"], runner_name="Test")
                mock_pf.assert_not_called()


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
