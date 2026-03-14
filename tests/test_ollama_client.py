"""Tests for the shared Ollama client wrapper with watchdog timeouts."""

import time
from unittest.mock import MagicMock, patch

import pytest

from engine.utils.ollama_client import (
    DEFAULT_MAX_RETRIES,
    MODEL_TIMEOUTS,
    _wall_timeout_for_model,
    ollama_chat,
)


# ── Timeout tier resolution ─────────────────────────────────────────


class TestWallTimeoutForModel:
    def test_8b_model(self):
        assert _wall_timeout_for_model("qwen3:8b") == 300.0

    def test_27b_model(self):
        assert _wall_timeout_for_model("gemma3:27b") == 600.0

    def test_32b_model(self):
        assert _wall_timeout_for_model("deepseek-r1:32b") == 900.0

    def test_70b_model(self):
        assert _wall_timeout_for_model("llama3:70b") == 1200.0

    def test_unknown_model_gets_default(self):
        assert _wall_timeout_for_model("mystery-model") == 600.0


# ── Watchdog fires on hanging call ───────────────────────────────────


class TestWatchdogTimeout:
    @patch("engine.utils.ollama_client._client")
    def test_watchdog_fires_on_hang(self, mock_client):
        """A hanging Ollama call should raise TimeoutError within wall_timeout + margin."""
        def hang_forever(**kwargs):
            time.sleep(60)  # simulate indefinite hang

        mock_client.chat.side_effect = hang_forever

        t0 = time.monotonic()
        with pytest.raises(TimeoutError, match="timed out"):
            ollama_chat(
                model="qwen3:8b",
                messages=[{"role": "user", "content": "hello"}],
                paper_id=42,
                max_retries=0,
                wall_timeout=2.0,  # 2 second watchdog for fast test
            )
        elapsed = time.monotonic() - t0

        # Should fire within 2s + small margin, not wait the full 60s
        assert elapsed < 5.0, f"Watchdog took {elapsed:.1f}s, expected < 5s"

    @patch("engine.utils.ollama_client._client")
    def test_watchdog_retries_then_raises(self, mock_client):
        """Watchdog should retry the configured number of times before raising."""
        call_count = 0

        def hang(**kwargs):
            nonlocal call_count
            call_count += 1
            time.sleep(60)

        mock_client.chat.side_effect = hang

        with pytest.raises(TimeoutError):
            ollama_chat(
                model="qwen3:8b",
                messages=[{"role": "user", "content": "hello"}],
                max_retries=2,
                wall_timeout=1.0,
                retry_delay=0.1,
            )

        assert call_count == 3  # 1 initial + 2 retries

    @patch("engine.utils.ollama_client._client")
    def test_successful_call_returns_response(self, mock_client):
        """Normal call should return the response object."""
        mock_response = MagicMock()
        mock_response.message.content = '{"decision": "include"}'
        mock_client.chat.return_value = mock_response

        result = ollama_chat(
            model="qwen3:8b",
            messages=[{"role": "user", "content": "hello"}],
        )

        assert result.message.content == '{"decision": "include"}'
        mock_client.chat.assert_called_once()

    @patch("engine.utils.ollama_client._client")
    def test_transient_failure_then_success(self, mock_client):
        """Should retry on transient error and succeed."""
        mock_response = MagicMock()
        mock_response.message.content = "ok"
        mock_client.chat.side_effect = [
            ConnectionError("connection reset"),
            mock_response,
        ]

        result = ollama_chat(
            model="qwen3:8b",
            messages=[{"role": "user", "content": "hello"}],
            retry_delay=0.1,
        )

        assert result.message.content == "ok"
        assert mock_client.chat.call_count == 2


# ── Logging ──────────────────────────────────────────────────────────


class TestTimeoutLogging:
    @patch("engine.utils.ollama_client._client")
    def test_timeout_logs_warning(self, mock_client, caplog):
        """Timeout events should log at WARNING with model, paper_id, elapsed."""
        def hang(**kwargs):
            time.sleep(60)

        mock_client.chat.side_effect = hang

        with pytest.raises(TimeoutError):
            with caplog.at_level("WARNING", logger="engine.utils.ollama_client"):
                ollama_chat(
                    model="gemma3:27b",
                    messages=[{"role": "user", "content": "test"}],
                    paper_id=99,
                    max_retries=0,
                    wall_timeout=1.0,
                )

        assert "gemma3:27b" in caplog.text
        assert "paper_id=99" in caplog.text
        assert "wall-clock timeout" in caplog.text.lower()


# ── kwargs pass-through ──────────────────────────────────────────────


class TestKwargsPassThrough:
    @patch("engine.utils.ollama_client._client")
    def test_format_and_options_forwarded(self, mock_client):
        """Extra kwargs like format, options, think should pass through."""
        mock_response = MagicMock()
        mock_client.chat.return_value = mock_response

        schema = {"type": "object"}
        ollama_chat(
            model="qwen3:8b",
            messages=[{"role": "user", "content": "hello"}],
            format=schema,
            options={"temperature": 0},
            think=False,
        )

        _, kwargs = mock_client.chat.call_args
        assert kwargs["format"] == schema
        assert kwargs["options"] == {"temperature": 0}
        assert kwargs["think"] is False
