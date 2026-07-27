"""restart_ollama() must stand down for a FOREIGN experiment lock only (OPS-GUARD-01 C).

The distinction matters: run_extraction() holds the lock for its own duration,
and the periodic restart is how a long extraction clears CUDA context
fragmentation. If restart_ollama() skipped whenever the lock was held at all, it
would disable itself on every real run.
"""

import os
import subprocess
import sys
import textwrap
import time
from unittest.mock import patch

import pytest

from engine.agents import extractor
from engine.utils import ollama_lock as L


@pytest.fixture(autouse=True)
def isolated_lock(tmp_path, monkeypatch):
    monkeypatch.setenv("OLLAMA_EXPERIMENT_LOCK", str(tmp_path / "experiment.lock"))
    monkeypatch.setattr(L, "_SELF_DEPTH", 0, raising=False)
    monkeypatch.setattr(L, "_SELF_FD", None, raising=False)
    yield
    L._SELF_DEPTH = 0
    L._SELF_FD = None


def _spawn_holder(tmp_path):
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


def test_restart_skipped_under_foreign_lock(tmp_path, caplog):
    proc = _spawn_holder(tmp_path)
    try:
        with patch("engine.agents.extractor.subprocess.run") as mock_run, \
             patch("engine.agents.extractor.httpx.get") as mock_get:
            with caplog.at_level("WARNING"):
                extractor.restart_ollama(reason="proactive after 25 papers", papers_done=25)
        # the whole point: no systemctl, no HTTP poll
        mock_run.assert_not_called()
        mock_get.assert_not_called()
        assert "RESTART SKIPPED — experiment lock held" in caplog.text
    finally:
        proc.kill()
        proc.wait()


def test_restart_proceeds_under_self_lock(tmp_path):
    """Holding the lock ourselves must NOT disable our own restart."""
    with patch("engine.agents.extractor.subprocess.run") as mock_run, \
         patch("engine.agents.extractor.httpx.get") as mock_get:
        mock_get.return_value.status_code = 200
        with L.hold_experiment_lock():
            assert L.check_experiment_lock() is True   # held...
            assert L.foreign_lock_held() is False      # ...but by us
            extractor.restart_ollama(reason="proactive", papers_done=25)
        mock_run.assert_called_once()
        assert mock_run.call_args[0][0] == ["sudo", "systemctl", "restart", "ollama"]


def test_restart_proceeds_when_lock_is_free():
    with patch("engine.agents.extractor.subprocess.run") as mock_run, \
         patch("engine.agents.extractor.httpx.get") as mock_get:
        mock_get.return_value.status_code = 200
        extractor.restart_ollama(reason="proactive", papers_done=25)
        mock_run.assert_called_once()


def test_skip_returns_none_and_does_not_raise(tmp_path):
    """A skip is a no-op, not an error: extraction must continue regardless."""
    proc = _spawn_holder(tmp_path)
    try:
        with patch("engine.agents.extractor.subprocess.run"), \
             patch("engine.agents.extractor.httpx.get"):
            assert extractor.restart_ollama(reason="proactive", papers_done=25) is None
    finally:
        proc.kill()
        proc.wait()


def test_run_extraction_holds_the_lock():
    """run_extraction() must own the lock while it runs (OPS-GUARD-01 part 5)."""
    observed = {}

    def fake_inner(db, spec, review_name, restart_every):
        observed["held"] = L.check_experiment_lock()
        observed["self"] = L.self_holds_lock()
        observed["foreign"] = L.foreign_lock_held()
        return {"extracted": 0}

    with patch("engine.agents.extractor._run_extraction_unlocked", side_effect=fake_inner):
        extractor.run_extraction(db=None, spec=None, review_name="r")

    assert observed == {"held": True, "self": True, "foreign": False}
    assert L.check_experiment_lock() is False  # released afterwards


def test_run_extraction_can_opt_out():
    def fake_inner(db, spec, review_name, restart_every):
        assert L.self_holds_lock() is False
        return {"extracted": 0}

    with patch("engine.agents.extractor._run_extraction_unlocked", side_effect=fake_inner):
        extractor.run_extraction(db=None, spec=None, review_name="r", experiment_lock=False)
