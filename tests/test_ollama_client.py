"""Tests for the shared Ollama client wrapper with watchdog timeouts."""

import time
from unittest.mock import MagicMock, patch, call

import pytest

from engine.utils.ollama_client import (
    DEFAULT_MAX_RETRIES,
    MODEL_TIMEOUTS,
    ModelDigestError,
    _restart_ollama_and_retry,
    _wall_timeout_for_model,
    fetch_model_digest,
    get_model_digest,
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
    @patch("engine.utils.ollama_client.subprocess.run")
    @patch("engine.utils.ollama_client._client")
    def test_watchdog_fires_on_hang(self, mock_client, mock_subprocess):
        """A hanging Ollama call should raise TimeoutError within wall_timeout + margin."""
        def hang_forever(**kwargs):
            time.sleep(60)  # simulate indefinite hang

        mock_client.chat.side_effect = hang_forever
        mock_subprocess.return_value = MagicMock(returncode=0)

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

        # 2s watchdog + 2s post-restart attempt + 10s stabilization wait + margin
        assert elapsed < 20.0, f"Watchdog took {elapsed:.1f}s, expected < 20s"

    @patch("engine.utils.ollama_client.subprocess.run")
    @patch("engine.utils.ollama_client._client")
    def test_watchdog_retries_then_raises(self, mock_client, mock_subprocess):
        """Watchdog should retry the configured number of times before raising."""
        call_count = 0

        def hang(**kwargs):
            nonlocal call_count
            call_count += 1
            time.sleep(60)

        mock_client.chat.side_effect = hang
        mock_subprocess.return_value = MagicMock(returncode=0)

        with pytest.raises(TimeoutError):
            ollama_chat(
                model="qwen3:8b",
                messages=[{"role": "user", "content": "hello"}],
                max_retries=2,
                wall_timeout=1.0,
                retry_delay=0.1,
            )

        assert call_count == 4  # 1 initial + 2 retries + 1 post-restart

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


# ── Ollama restart recovery ──────────────────────────────────────────


class TestOllamaRestartRecovery:
    @patch("engine.utils.ollama_client.subprocess.run")
    @patch("engine.utils.ollama_client._client")
    def test_restart_attempted_after_retries_exhausted(self, mock_client, mock_subprocess):
        """After all retries timeout, should attempt sudo systemctl restart ollama."""
        call_count = 0

        def hang(**kwargs):
            nonlocal call_count
            call_count += 1
            time.sleep(60)

        mock_client.chat.side_effect = hang
        mock_subprocess.return_value = MagicMock(returncode=0)

        with pytest.raises(TimeoutError):
            ollama_chat(
                model="qwen3:8b",
                messages=[{"role": "user", "content": "hello"}],
                max_retries=0,
                wall_timeout=1.0,
                retry_delay=0.1,
            )

        # Verify restart was called
        mock_subprocess.assert_called_once()
        restart_args = mock_subprocess.call_args[0][0]
        assert restart_args == ["sudo", "systemctl", "restart", "ollama"]

    @patch("engine.utils.ollama_client.subprocess.run")
    @patch("engine.utils.ollama_client._client")
    def test_restart_success_then_call_succeeds(self, mock_client, mock_subprocess):
        """If restart succeeds and post-restart call works, should return response."""
        mock_response = MagicMock()
        mock_response.message.content = '{"result": "ok"}'

        # Test _restart_ollama_and_retry directly to avoid timing complexity
        mock_client.chat.return_value = mock_response
        mock_subprocess.return_value = MagicMock(returncode=0)

        result = _restart_ollama_and_retry(
            model="qwen3:8b",
            messages=[{"role": "user", "content": "hello"}],
            paper_label="paper_id=1",
            effective_timeout=30.0,
            max_retries=1,
        )

        assert result.message.content == '{"result": "ok"}'
        mock_subprocess.assert_called_once()

    @patch("engine.utils.ollama_client.subprocess.run")
    @patch("engine.utils.ollama_client._client")
    def test_restart_failure_raises_original_timeout(self, mock_client, mock_subprocess):
        """If restart itself fails, should raise TimeoutError (not subprocess error)."""
        def hang(**kwargs):
            time.sleep(60)

        mock_client.chat.side_effect = hang
        mock_subprocess.side_effect = OSError("sudo not available")

        with pytest.raises(TimeoutError, match="timed out"):
            ollama_chat(
                model="qwen3:8b",
                messages=[{"role": "user", "content": "hello"}],
                max_retries=0,
                wall_timeout=1.0,
                retry_delay=0.1,
            )

        # Restart was attempted even though it failed
        mock_subprocess.assert_called_once()

    @patch("engine.utils.ollama_client.subprocess.run")
    @patch("engine.utils.ollama_client._client")
    def test_restart_logs_warning(self, mock_client, mock_subprocess, caplog):
        """Restart attempt should log at WARNING level."""
        def hang(**kwargs):
            time.sleep(60)

        mock_client.chat.side_effect = hang
        mock_subprocess.return_value = MagicMock(returncode=0)

        with pytest.raises(TimeoutError):
            with caplog.at_level("WARNING", logger="engine.utils.ollama_client"):
                ollama_chat(
                    model="gemma3:27b",
                    messages=[{"role": "user", "content": "test"}],
                    max_retries=0,
                    wall_timeout=1.0,
                    retry_delay=0.1,
                )

        assert "restarting ollama service" in caplog.text.lower()

    @patch("engine.utils.ollama_client.subprocess.run",
           side_effect=OSError("systemctl not found"))
    def test_restart_ollama_and_retry_raises_on_restart_failure(self, mock_subprocess):
        """M3: _restart_ollama_and_retry raises RuntimeError (not None) on restart failure."""
        with pytest.raises(RuntimeError, match="Ollama restart failed"):
            _restart_ollama_and_retry(
                model="qwen3:8b",
                messages=[{"role": "user", "content": "hello"}],
                paper_label="paper_id=1",
                effective_timeout=30.0,
                max_retries=1,
            )

    @patch("engine.utils.ollama_client.subprocess.run")
    @patch("engine.utils.ollama_client._client")
    def test_restart_ollama_and_retry_raises_on_post_restart_failure(
        self, mock_client, mock_subprocess,
    ):
        """M3: _restart_ollama_and_retry raises RuntimeError when post-restart call fails."""
        mock_subprocess.return_value = MagicMock(returncode=0)

        def hang(**kwargs):
            time.sleep(60)

        mock_client.chat.side_effect = hang

        with pytest.raises(RuntimeError, match="Post-restart Ollama call failed"):
            _restart_ollama_and_retry(
                model="qwen3:8b",
                messages=[{"role": "user", "content": "hello"}],
                paper_label="paper_id=1",
                effective_timeout=1.0,
                max_retries=1,
            )


# ── Model digest ───────────────────────────────────────────────────


class TestGetModelDigest:
    @patch("engine.utils.ollama_client._client")
    def test_digest_returned_from_api(self, mock_client):
        """Digest from mock API is returned correctly."""
        mock_info = MagicMock()
        mock_info.digest = "sha256:abc123def456"
        mock_client.show.return_value = mock_info

        result = get_model_digest("deepseek-r1:32b")
        assert result == "sha256:abc123def456"
        mock_client.show.assert_called_once_with("deepseek-r1:32b")

    @patch("engine.utils.ollama_client._client")
    def test_api_failure_returns_none(self, mock_client, caplog):
        """API failure returns None without raising, logs WARNING."""
        mock_client.show.side_effect = ConnectionError("ollama down")

        with caplog.at_level("WARNING", logger="engine.utils.ollama_client"):
            result = get_model_digest("deepseek-r1:32b")

        assert result is None
        assert "Failed to get digest" in caplog.text
        assert "deepseek-r1:32b" in caplog.text


# ── Strict digest fetch (judge orchestrator) ─────────────────────────


VALID_DIGEST = "a" * 64  # 64 lowercase hex chars


def _tags_response(models, status=200):
    """Build a minimal httpx.Response stand-in for /api/tags."""
    resp = MagicMock()
    resp.status_code = status
    resp.text = "(body)"
    if isinstance(models, Exception):
        resp.json.side_effect = models
    else:
        resp.json.return_value = {"models": models}
    return resp


class TestFetchModelDigest:
    def test_well_formed_returns_digest(self):
        models = [
            {"name": "gemma3:27b", "digest": VALID_DIGEST,
             "modified_at": "2026-03-09T01:01:47Z"},
            {"name": "deepseek-r1:32b", "digest": "b" * 64,
             "modified_at": "2026-01-01T00:00:00Z"},
        ]
        with patch("engine.utils.ollama_client.httpx.get",
                   return_value=_tags_response(models)) as mock_get:
            digest = fetch_model_digest("gemma3:27b")

        assert digest == VALID_DIGEST
        assert len(digest) == 64 and all(c in "0123456789abcdef" for c in digest)
        # Asserts the canonical endpoint was hit
        called_url = mock_get.call_args.args[0]
        assert called_url.endswith("/api/tags")

    def test_malformed_digest_raises(self):
        # Truncated to 32 chars — fails ^[0-9a-f]{64}$
        bad = "a" * 32
        models = [{"name": "gemma3:27b", "digest": bad,
                   "modified_at": "2026-03-09T01:01:47Z"}]
        with patch("engine.utils.ollama_client.httpx.get",
                   return_value=_tags_response(models)):
            with pytest.raises(ModelDigestError, match="malformed digest"):
                fetch_model_digest("gemma3:27b")

    def test_non_hex_digest_raises(self):
        # Uppercase hex is not accepted (regex requires lowercase)
        bad = "A" * 64
        models = [{"name": "gemma3:27b", "digest": bad,
                   "modified_at": "2026-03-09T01:01:47Z"}]
        with patch("engine.utils.ollama_client.httpx.get",
                   return_value=_tags_response(models)):
            with pytest.raises(ModelDigestError, match="malformed digest"):
                fetch_model_digest("gemma3:27b")

    def test_missing_model_raises(self):
        models = [{"name": "deepseek-r1:32b", "digest": VALID_DIGEST,
                   "modified_at": "2026-01-01T00:00:00Z"}]
        with patch("engine.utils.ollama_client.httpx.get",
                   return_value=_tags_response(models)):
            with pytest.raises(ModelDigestError, match="no entry for model_name"):
                fetch_model_digest("gemma3:27b")

    def test_ambiguous_multiple_matches_raises(self):
        models = [
            {"name": "gemma3:27b", "digest": "a" * 64,
             "modified_at": "2026-03-09T01:01:47Z"},
            {"name": "gemma3:27b", "digest": "b" * 64,
             "modified_at": "2026-04-01T00:00:00Z"},
        ]
        with patch("engine.utils.ollama_client.httpx.get",
                   return_value=_tags_response(models)):
            with pytest.raises(ModelDigestError, match="ambiguous"):
                fetch_model_digest("gemma3:27b")

    def test_non_200_status_raises(self):
        resp = _tags_response([], status=503)
        with patch("engine.utils.ollama_client.httpx.get", return_value=resp):
            with pytest.raises(ModelDigestError, match="non-200"):
                fetch_model_digest("gemma3:27b")


class TestDigestInExtraction:
    def test_digest_columns_in_extraction_record(self, tmp_path):
        """model_digest and auditor_model_digest columns are stored in extraction record."""
        from engine.core.database import ReviewDatabase
        from engine.search.models import Citation

        db = ReviewDatabase("test_digest", data_root=tmp_path)
        try:
            db.add_papers([Citation(title="Test", source="pubmed", pmid="999")])
            pid = db._conn.execute("SELECT id FROM papers WHERE pmid = '999'").fetchone()["id"]
            db.update_status(pid, "ABSTRACT_SCREENED_IN")
            db.update_status(pid, "PDF_ACQUIRED")
            db.update_status(pid, "PARSED")

            ext_id = db.add_extraction_atomic(
                paper_id=pid,
                schema_hash="test_hash",
                extracted_data=[{"field_name": "study_type", "value": "RCT"}],
                reasoning_trace="trace",
                model="deepseek-r1:32b",
                spans=[{"field_name": "study_type", "value": "RCT",
                        "source_snippet": "...", "confidence": 0.9}],
                model_digest="sha256:extractor_digest_abc",
                auditor_model_digest="sha256:auditor_digest_xyz",
            )

            row = db._conn.execute(
                "SELECT model_digest, auditor_model_digest FROM extractions WHERE id = ?",
                (ext_id,),
            ).fetchone()
            assert row["model_digest"] == "sha256:extractor_digest_abc"
            assert row["auditor_model_digest"] == "sha256:auditor_digest_xyz"
        finally:
            db.close()
