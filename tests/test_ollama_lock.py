"""Unit tests for the Ollama experiment lock (OPS-GUARD-01).

Covers the things that make or break this guard:
  * acquire / release / auto-release-on-death (flock, not existence);
  * the non-blocking probe leaves state untouched;
  * self-vs-foreign, which exists because flock is per open file description —
    a same-process probe is indistinguishable from a foreign holder without
    explicit bookkeeping;
  * no acquisition waits (R629(b)): a held lock is refused at once with an
    `ExperimentLockHeld` naming the file, under an unrelated holder and under
    each outer-lock form that used to deadlock (INT-g12-7);
  * the descriptor is closed on any exit, an interrupt included (12n F-1).
"""

import fcntl
import logging
import os
import subprocess
import sys
import textwrap
import time
from pathlib import Path

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


# ── re-entrancy (a second flock in-process would be refused like anyone's) ──


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


# ── no acquisition waits (R629(b)) ───────────────────────────────────────


def test_a_held_lock_is_refused_at_once_naming_the_file_and_the_holder(tmp_path, caplog):
    """T4. Another process holds the lock: the acquire raises within 2 s, never waits."""
    proc = _spawn_holder(tmp_path)
    lock = os.environ["OLLAMA_EXPERIMENT_LOCK"]
    try:
        t0 = time.monotonic()
        with caplog.at_level(logging.ERROR, logger="engine.utils.ollama_lock"):
            with pytest.raises(L.ExperimentLockHeld) as refused:
                with L.hold_experiment_lock():
                    pytest.fail("the block ran under someone else's lock")
        assert time.monotonic() - t0 < 2.0
        exc = refused.value
        assert isinstance(exc, BlockingIOError)
        assert lock in str(exc) and exc.lock_path == Path(lock)
        assert exc.holder_pid == proc.pid
        assert f"pid recorded by the kernel: {proc.pid}" in str(exc)
        # logged once, at the refusal, with the message's own text
        assert [r.getMessage() for r in caplog.records] == [str(exc)]
        # a failed acquire must not corrupt self-ownership state
        assert L.self_holds_lock() is False
    finally:
        proc.kill()
        proc.wait()


def test_the_lock_is_taken_as_soon_as_the_holder_is_gone(tmp_path):
    """There is no wait to inherit: the refusal holds nothing, so the next attempt wins."""
    proc = _spawn_holder(tmp_path)
    try:
        with pytest.raises(L.ExperimentLockHeld):
            with L.hold_experiment_lock():
                pass
    finally:
        proc.kill()
        proc.wait()
    with L.hold_experiment_lock():
        assert L.self_holds_lock() is True


def test_a_refusal_with_no_pid_in_proc_locks_still_names_the_file(tmp_path, monkeypatch):
    proc = _spawn_holder(tmp_path)
    monkeypatch.setattr(L, "_holder_pid", lambda fd: None)
    try:
        with pytest.raises(L.ExperimentLockHeld) as refused:
            with L.hold_experiment_lock():
                pass
        assert refused.value.holder_pid is None
        assert os.environ["OLLAMA_EXPERIMENT_LOCK"] in str(refused.value)
        assert "no holder pid" in str(refused.value)
    finally:
        proc.kill()
        proc.wait()


# ── T5 — the outer-lock forms that used to deadlock (INT-g12-7) ──────────

_CHILD = """
import sys
from engine.utils import ollama_lock as L
try:
    with L.hold_experiment_lock():
        print("ACQUIRED")
except L.ExperimentLockHeld as exc:
    print("REFUSED", exc)
    sys.exit(3)
"""
# The four forms measured in 12n_U12_C2A A2: each blocked a child until its timeout.
_OUTER_FORMS = {
    "E1_flock_wrapper": 'flock "$LOCK" "$PY" -c "$CHILD"',
    "E2_flock_wrapper_fd_closed": 'flock -o "$LOCK" "$PY" -c "$CHILD"',
    "E3_inherited_fd_9": 'exec 9>>"$LOCK"; flock -n 9 || exit 99; "$PY" -c "$CHILD"',
    "E4_fd_9_closed_for_child": 'exec 9>>"$LOCK"; flock -n 9 || exit 99; "$PY" -c "$CHILD" 9>&-',
}


@pytest.mark.parametrize("form", sorted(_OUTER_FORMS))
def test_a_child_under_an_outer_lock_is_refused_not_blocked(form, tmp_path):
    lock = os.environ["OLLAMA_EXPERIMENT_LOCK"]
    repo = str(Path(__file__).resolve().parents[1])
    env = {"PATH": os.environ["PATH"], "PYTHONPATH": repo, "OLLAMA_EXPERIMENT_LOCK": lock,
           "LOCK": lock, "PY": sys.executable, "CHILD": _CHILD}
    try:
        r = subprocess.run(["bash", "-c", _OUTER_FORMS[form]], env=env, cwd=repo,
                           capture_output=True, text=True, timeout=30)
    except subprocess.TimeoutExpired:
        pytest.fail(f"{form}: the child blocked on the outer lock instead of refusing")
    assert r.returncode == 3, (r.returncode, r.stdout, r.stderr)
    assert r.stdout.startswith("REFUSED Experiment lock held: " + lock), r.stdout
    assert "ACQUIRED" not in r.stdout


# ── T6 — a nested acquire makes no second flock ──────────────────────────


def test_a_nested_acquire_makes_no_second_flock_and_the_outer_hold_survives(monkeypatch):
    real, taken = fcntl.flock, []

    def counting(fd, op):
        if op & fcntl.LOCK_EX:
            taken.append(fd)
        return real(fd, op)

    monkeypatch.setattr(L.fcntl, "flock", counting)
    with L.hold_experiment_lock():
        outer_fd = L._SELF_FD
        assert taken == [outer_fd]
        with L.hold_experiment_lock():
            with L.hold_experiment_lock():
                assert L._SELF_FD == outer_fd
        assert taken == [outer_fd]                 # no second flock by this process
        assert L.self_holds_lock() is True         # still held after the inner blocks
        monkeypatch.setattr(L.fcntl, "flock", real)
        assert L.check_experiment_lock() is True
    assert L.self_holds_lock() is False


# ── T7 — an interrupt leaves the lock free and the descriptor closed (F-1) ──


def _is_closed(fd: int) -> bool:
    try:
        os.fstat(fd)
    except OSError:
        return True
    return False


def test_an_interrupt_inside_the_hold_frees_the_lock_and_closes_the_descriptor():
    held = {}
    with pytest.raises(KeyboardInterrupt):
        with L.hold_experiment_lock():
            held["fd"] = L._SELF_FD
            raise KeyboardInterrupt
    assert _is_closed(held["fd"])
    assert L.self_holds_lock() is False and L.check_experiment_lock() is False


def test_an_interrupt_during_the_acquire_itself_closes_the_descriptor(monkeypatch):
    """The site F-1 named: before this commit `except Exception` left it open."""
    opened, real_open = [], L._open_lock_file

    def tracking_open():
        opened.append(real_open())
        return opened[-1]

    def interrupted(fd, op):
        raise KeyboardInterrupt

    monkeypatch.setattr(L, "_open_lock_file", tracking_open)
    monkeypatch.setattr(L.fcntl, "flock", interrupted)
    with pytest.raises(KeyboardInterrupt):
        with L.hold_experiment_lock():
            pytest.fail("the block ran")
    assert len(opened) == 1 and _is_closed(opened[0])
    assert L.self_holds_lock() is False


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
