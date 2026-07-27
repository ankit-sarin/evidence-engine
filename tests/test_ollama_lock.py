"""Unit tests for the Ollama experiment lock (OPS-GUARD-01).

Covers the three things that make or break this guard:
  * acquire / release / auto-release-on-death (flock, not existence);
  * the non-blocking probe leaves state untouched;
  * self-vs-foreign, which exists because flock is per open file description —
    a same-process probe is indistinguishable from a foreign holder without
    explicit bookkeeping.
"""

import os
import subprocess
import sys
import textwrap
import time

import pytest

from engine.utils import ollama_lock as L


@pytest.fixture(autouse=True)
def isolated_lock(tmp_path, monkeypatch):
    """Point the module at a temp lock file and reset self-ownership state."""
    monkeypatch.setenv("OLLAMA_EXPERIMENT_LOCK", str(tmp_path / "experiment.lock"))
    monkeypatch.setattr(L, "_SELF_DEPTH", 0, raising=False)
    monkeypatch.setattr(L, "_SELF_FD", None, raising=False)
    yield
    L._SELF_DEPTH = 0
    L._SELF_FD = None


def _holder_script(lock_path: str, ready: str, hold_seconds: float = 30) -> str:
    """A child process that takes the lock, signals readiness, then waits."""
    return textwrap.dedent(f"""
        import fcntl, os, sys, time
        fd = os.open({lock_path!r}, os.O_RDWR | os.O_CREAT, 0o644)
        fcntl.flock(fd, fcntl.LOCK_EX)
        open({ready!r}, "w").write("held")
        time.sleep({hold_seconds})
    """)


def _spawn_holder(tmp_path, hold_seconds=30):
    lock = os.environ["OLLAMA_EXPERIMENT_LOCK"]
    ready = str(tmp_path / "ready")
    proc = subprocess.Popen(
        [sys.executable, "-c", _holder_script(lock, ready, hold_seconds)]
    )
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if os.path.exists(ready):
            return proc
        time.sleep(0.02)
    proc.kill()
    raise AssertionError("foreign holder did not acquire the lock in time")


# ── basic acquire / release ──────────────────────────────────────────────


def test_lock_is_free_initially():
    assert L.check_experiment_lock() is False
    assert L.self_holds_lock() is False
    assert L.foreign_lock_held() is False


def test_hold_and_release():
    with L.hold_experiment_lock():
        assert L.check_experiment_lock() is True
        assert L.self_holds_lock() is True
    assert L.check_experiment_lock() is False
    assert L.self_holds_lock() is False


def test_lock_file_is_created_but_existence_means_nothing():
    with L.hold_experiment_lock():
        pass
    assert L.lock_path().exists()          # file persists ...
    assert L.check_experiment_lock() is False  # ... and the lock is free


def test_probe_does_not_leave_the_lock_held():
    for _ in range(3):
        assert L.check_experiment_lock() is False
    with L.hold_experiment_lock():
        assert L.check_experiment_lock() is True


def test_release_happens_even_on_exception():
    with pytest.raises(ValueError):
        with L.hold_experiment_lock():
            raise ValueError("boom")
    assert L.check_experiment_lock() is False
    assert L.self_holds_lock() is False


# ── re-entrancy (a second blocking flock in-process would deadlock) ──────


def test_nested_hold_is_reentrant():
    with L.hold_experiment_lock():
        with L.hold_experiment_lock():
            assert L.self_holds_lock() is True
        # inner exit must NOT release
        assert L.self_holds_lock() is True
        assert L.check_experiment_lock() is True
    assert L.self_holds_lock() is False


# ── self vs foreign ──────────────────────────────────────────────────────


def test_foreign_holder_is_seen_as_held_and_foreign(tmp_path):
    proc = _spawn_holder(tmp_path)
    try:
        assert L.check_experiment_lock() is True
        assert L.self_holds_lock() is False
        assert L.foreign_lock_held() is True
    finally:
        proc.kill()
        proc.wait()


def test_self_holder_is_held_but_not_foreign():
    with L.hold_experiment_lock():
        assert L.check_experiment_lock() is True
        assert L.foreign_lock_held() is False


def test_lock_auto_releases_when_holder_dies(tmp_path):
    """No stale locks: the kernel drops the flock when the fd closes."""
    proc = _spawn_holder(tmp_path)
    assert L.check_experiment_lock() is True
    proc.kill()
    proc.wait()
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if L.check_experiment_lock() is False:
            break
        time.sleep(0.05)
    assert L.check_experiment_lock() is False


def test_non_blocking_acquire_raises_under_foreign_hold(tmp_path):
    proc = _spawn_holder(tmp_path)
    try:
        with pytest.raises(BlockingIOError):
            with L.hold_experiment_lock(blocking=False):
                pass
        # a failed acquire must not corrupt self-ownership state
        assert L.self_holds_lock() is False
    finally:
        proc.kill()
        proc.wait()


def test_blocking_acquire_waits_then_succeeds(tmp_path):
    proc = _spawn_holder(tmp_path, hold_seconds=1.0)
    try:
        t0 = time.monotonic()
        with L.hold_experiment_lock():
            waited = time.monotonic() - t0
            assert L.self_holds_lock() is True
        assert waited >= 0.3, "blocking acquire should have waited for the holder"
    finally:
        proc.kill()
        proc.wait()


def test_probe_failure_fails_safe(monkeypatch):
    """An unopenable lock file must read as HELD, never as free."""
    def boom():
        raise OSError("permission denied")
    monkeypatch.setattr(L, "_open_lock_file", boom)
    assert L.check_experiment_lock() is True


def test_lock_path_honours_env_override(tmp_path, monkeypatch):
    target = tmp_path / "custom" / "my.lock"
    monkeypatch.setenv("OLLAMA_EXPERIMENT_LOCK", str(target))
    assert L.lock_path() == target
    with L.hold_experiment_lock():
        assert target.exists()
