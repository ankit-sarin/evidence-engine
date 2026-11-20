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

import contextlib
import json
import os
import sqlite3
import subprocess
import sys
import textwrap
import time
from unittest.mock import MagicMock, patch

import httpx
import pytest

from engine.core import run_manifest as rm
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


@pytest.fixture(autouse=True)
def fixed_input_fit_ceiling(monkeypatch):
    """The restart gate is under test here, not the input-fit guard. The guard's
    ceiling read goes through `_client.show`, which a MagicMock client cannot
    answer, so the ceiling is fixed (tests/test_ollama_input_fit.py covers it)."""
    monkeypatch.setattr(oc, "effective_ceiling", lambda model, options=None: 131_072)


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


@contextlib.contextmanager
def _asked_run():
    """A run that asked for the recovery restart (U41, R819(d)): its manifest records
    `ollama_restarts.on_read_timeout` true. The calls name no stage, so nothing but the
    restart's gate reads the run."""
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE run_manifests (run_id INTEGER PRIMARY KEY, manifest_json TEXT)")
    conn.execute("INSERT INTO run_manifests VALUES (1, ?)", (json.dumps(
        {"ollama_restarts": {"proactive_every": 0, "on_read_timeout": True}}),))
    try:
        with rm.active_run(conn, 1):
            yield
    finally:
        conn.close()


def _call(**overrides):
    """The recovery path, armed explicitly by a run that asked (U41): the gates under test
    here are the opt-out and the flock, which come before and after the run's request."""
    kwargs = dict(model="deepseek-r1:32b", messages=[], paper_label="paper_id=1",
                  effective_timeout=1.0, max_retries=0)
    kwargs.update(overrides)
    with _asked_run():
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
        assert mock_run.call_args[0][0] == ["sudo", "-n", "systemctl", "restart", "ollama"]  # R819(c)


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
        time.sleep(1.2)  # past the 1 s watchdog, then the fake read timeout ends it (R761(c))
        raise httpx.ReadTimeout("read timed out")

    with patch("engine.utils.ollama_client._client") as mock_client:
        mock_client.chat.side_effect = hang
        with pytest.raises(TimeoutError):
            oc.ollama_chat(model="gemma3:27b", messages=[{"role": "user", "content": "x"}],
                           paper_id=1, max_retries=0, wall_timeout=1.0)
