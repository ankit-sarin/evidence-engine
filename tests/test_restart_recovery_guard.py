"""`_restart_ollama_and_retry` must respect the experiment lock and the opt-out.

OPSFIX-01. `extractor.restart_ollama` has been flock-gated since OPS-GUARD-01,
but `ollama_client`'s last-resort recovery — a second, independent path to the
same `sudo systemctl restart ollama` — was not. That is the class of bug that
destroyed the inference-determinism Arm P rerun, and QUALGAP-01 had to
monkeypatch the private function to run safely.

The gate is deliberately `foreign_lock_held()`, not `check_experiment_lock()`:
a process holding the lock for its own long run must still be able to recover.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
import time
from unittest.mock import MagicMock, patch

import pytest

from engine.utils import ollama_client as oc
from engine.utils import ollama_lock as L


@pytest.fixture(autouse=True)
def isolated_lock(tmp_path, monkeypatch):
    monkeypatch.setenv("OLLAMA_EXPERIMENT_LOCK", str(tmp_path / "experiment.lock"))
    monkeypatch.delenv(oc.RESTART_OPT_OUT_ENV, raising=False)
    monkeypatch.setattr(L, "_SELF_DEPTH", 0, raising=False)
    monkeypatch.setattr(L, "_SELF_FD", None, raising=False)
    yield
    L._SELF_DEPTH = 0
    L._SELF_FD = None


def _spawn_holder(tmp_path):
    """A foreign process holding the flock, as a real second process."""
    lock = os.environ["OLLAMA_EXPERIMENT_LOCK"]
    ready = str(tmp_path / "ready")
    proc = subprocess.Popen([sys.executable, "-c", textwrap.dedent(f"""
        import fcntl, os, time
        fd = os.open({lock!r}, os.O_RDWR | os.O_CREAT, 0o644)
        fcntl.flock(fd, fcntl.LOCK_EX)
        open({ready!r}, "w").write("held")
        time.sleep(30)
    """)])
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if os.path.exists(ready):
            return proc
        time.sleep(0.02)
    proc.kill()
    raise AssertionError("foreign holder did not acquire the lock in time")


def _call(**overrides):
    kwargs = dict(model="deepseek-r1:32b", messages=[], paper_label="paper_id=1",
                  effective_timeout=1.0, max_retries=0)
    kwargs.update(overrides)
    return oc._restart_ollama_and_retry(**kwargs)


# ── the flock gate ───────────────────────────────────────────────────────


def test_restart_refused_under_foreign_lock(tmp_path):
    proc = _spawn_holder(tmp_path)
    try:
        with patch("engine.utils.ollama_client.subprocess.run") as mock_run:
            with pytest.raises(RuntimeError, match="experiment lock"):
                _call()
        mock_run.assert_not_called()
    finally:
        proc.kill()
        proc.wait()


def test_restart_proceeds_under_self_lock():
    """Holding the lock ourselves must not disable our own recovery."""
    with patch("engine.utils.ollama_client.subprocess.run") as mock_run, \
         patch("engine.utils.ollama_client._client") as mock_client:
        mock_run.return_value = MagicMock(returncode=0)
        mock_client.chat.return_value = MagicMock()
        with L.hold_experiment_lock():
            assert L.check_experiment_lock() is True   # held...
            assert L.foreign_lock_held() is False      # ...but by us
            _call()
        mock_run.assert_called_once()
        assert mock_run.call_args[0][0] == ["sudo", "systemctl", "restart", "ollama"]


def test_restart_proceeds_when_lock_is_free():
    with patch("engine.utils.ollama_client.subprocess.run") as mock_run, \
         patch("engine.utils.ollama_client._client") as mock_client:
        mock_run.return_value = MagicMock(returncode=0)
        mock_client.chat.return_value = MagicMock()
        _call()
        mock_run.assert_called_once()


def test_gate_matches_the_extractor_path():
    """Both restart paths must key off the same predicate, or they will drift."""
    from engine.agents import extractor
    from engine.utils.ollama_lock import foreign_lock_held

    assert extractor.foreign_lock_held is foreign_lock_held
    assert oc.foreign_lock_held is foreign_lock_held


# ── the opt-out switch ───────────────────────────────────────────────────


def test_opt_out_refuses_before_touching_subprocess(monkeypatch):
    monkeypatch.setenv(oc.RESTART_OPT_OUT_ENV, "1")
    with patch("engine.utils.ollama_client.subprocess.run") as mock_run:
        with pytest.raises(RuntimeError, match=oc.RESTART_OPT_OUT_ENV):
            _call()
    mock_run.assert_not_called()


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "on", "anything"])
def test_truthy_values_disable_restart(monkeypatch, value):
    monkeypatch.setenv(oc.RESTART_OPT_OUT_ENV, value)
    assert oc.restart_disabled() is True


@pytest.mark.parametrize("value", ["", "0", "false", "FALSE", "no"])
def test_falsey_values_leave_restart_armed(monkeypatch, value):
    monkeypatch.setenv(oc.RESTART_OPT_OUT_ENV, value)
    assert oc.restart_disabled() is False


def test_unset_leaves_restart_armed(monkeypatch):
    monkeypatch.delenv(oc.RESTART_OPT_OUT_ENV, raising=False)
    assert oc.restart_disabled() is False


# ── refusal is surfaced, not swallowed ───────────────────────────────────


def test_refusal_surfaces_as_timeout_from_ollama_chat(monkeypatch):
    """A refused restart must still end as TimeoutError, not a silent success."""
    monkeypatch.setenv(oc.RESTART_OPT_OUT_ENV, "1")

    def hang(**kwargs):
        time.sleep(60)

    with patch("engine.utils.ollama_client._client") as mock_client:
        mock_client.chat.side_effect = hang
        with pytest.raises(TimeoutError):
            oc.ollama_chat(model="gemma3:27b", messages=[{"role": "user", "content": "x"}],
                           paper_id=1, max_retries=0, wall_timeout=1.0)
