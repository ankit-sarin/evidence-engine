"""restart_ollama() must stand down for a FOREIGN experiment lock only (OPS-GUARD-01 C).

The distinction matters: run_extraction() holds the lock for its own duration,
and the periodic restart is how a long extraction clears CUDA context
fragmentation. If restart_ollama() skipped whenever the lock was held at all, it
would disable itself on every real run.

The explicit opt-out (`EVIDENCE_ENGINE_NO_OLLAMA_RESTART`, N17) is checked before
the lock: with it set no restart is attempted at all, and the cadence in the
extraction loop logs the skip once per cadence.
"""

import logging
import os
import subprocess
import sys
import textwrap
import time
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from engine.agents import extractor
from engine.utils import ollama_client as oc
from engine.utils import ollama_lock as L


@pytest.fixture(autouse=True)
def isolated_lock(tmp_path, monkeypatch):
    monkeypatch.setenv("OLLAMA_EXPERIMENT_LOCK", str(tmp_path / "experiment.lock"))
    monkeypatch.setattr(L, "_SELF_DEPTH", 0, raising=False)
    monkeypatch.setattr(L, "_SELF_FD", None, raising=False)
    monkeypatch.delenv(oc.RESTART_OPT_OUT_ENV, raising=False)
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

    def fake_inner(db, spec, review_name, restart_every, *, run_id, selection=None):
        observed["held"] = L.check_experiment_lock()
        observed["self"] = L.self_holds_lock()
        observed["foreign"] = L.foreign_lock_held()
        return {"extracted": 0}

    with patch("engine.agents.extractor._run_extraction_unlocked", side_effect=fake_inner):
        extractor.run_extraction(db=None, spec=None, review_name="r", run_id=1)

    assert observed == {"held": True, "self": True, "foreign": False}
    assert L.check_experiment_lock() is False  # released afterwards


def test_run_extraction_can_opt_out():
    def fake_inner(db, spec, review_name, restart_every, *, run_id, selection=None):
        assert L.self_holds_lock() is False
        return {"extracted": 0}

    with patch("engine.agents.extractor._run_extraction_unlocked", side_effect=fake_inner):
        extractor.run_extraction(db=None, spec=None, review_name="r", experiment_lock=False,
                                 run_id=1)


# ── the explicit opt-out (N17) ───────────────────────────────────────────


def _skip_lines(caplog):
    return [r.getMessage() for r in caplog.records if "RESTART SKIPPED" in r.getMessage()]


def test_the_opt_out_skips_the_restart_before_the_lock_is_consulted(monkeypatch, caplog):
    """T1. Set, with the lock free: no systemctl, no poll, one skip line, None returned."""
    monkeypatch.setenv(oc.RESTART_OPT_OUT_ENV, "1")
    with patch("engine.agents.extractor.subprocess.run") as mock_run, \
         patch("engine.agents.extractor.httpx.get") as mock_get, \
         patch("engine.agents.extractor.foreign_lock_held") as mock_lock:
        with caplog.at_level(logging.WARNING, logger="engine.agents.extractor"):
            assert extractor.restart_ollama(reason="proactive after 25 papers", papers_done=25) is None
    mock_run.assert_not_called()
    mock_get.assert_not_called()
    mock_lock.assert_not_called()
    lines = _skip_lines(caplog)
    assert len(lines) == 1, lines
    assert oc.RESTART_OPT_OUT_ENV in lines[0] and "proactive after 25 papers" in lines[0]
    assert "experiment lock held" not in lines[0]


@pytest.mark.parametrize("value", ["", "0", "false", "no"])
def test_a_falsy_opt_out_value_does_not_disable_the_restart(monkeypatch, value):
    """T4. The spellings `restart_disabled()` reads as unset leave the restart armed."""
    monkeypatch.setenv(oc.RESTART_OPT_OUT_ENV, value)
    with patch("engine.agents.extractor.subprocess.run") as mock_run, \
         patch("engine.agents.extractor.httpx.get") as mock_get:
        mock_get.return_value.status_code = 200
        extractor.restart_ollama(reason="proactive", papers_done=25)
    mock_run.assert_called_once()
    assert mock_run.call_args[0][0] == ["sudo", "systemctl", "restart", "ollama"]


def test_the_extraction_loop_logs_one_skip_per_cadence_under_the_opt_out(
        monkeypatch, tmp_path, caplog):
    """T3. Four papers at a cadence of two: two skips, no restart, the counter reset each time.

    `_extract_selected` zeroes `papers_since_restart` whenever `restart_ollama` returns
    without raising, so a skip resets it as a performed restart does. Were it not reset,
    the third paper would skip again, as "proactive after 3 papers".
    """
    monkeypatch.setenv(oc.RESTART_OPT_OUT_ENV, "1")
    monkeypatch.setattr(extractor, "load_codebook_beside", lambda p: SimpleNamespace(semantic_hash="h" * 64))
    monkeypatch.setattr(extractor, "stage_config", lambda stage, spec: SimpleNamespace(model="fake-model"))
    monkeypatch.setattr(extractor, "read_parsed_text", lambda ref: "paper text")
    monkeypatch.setattr(extractor, "extract_paper_with_completeness",
                        lambda *a, **k: SimpleNamespace(fields=["f"]))
    monkeypatch.setattr(extractor, "run_extraction_summary", lambda conn, run_id, selection: {
        "stored_incomplete": 0, "all_declined": 0, "below_floor": 0, "incomplete_fields": {},
        "value_divergence": {"substantive": {"count": 0, "fields": {}},
                             "case_or_whitespace": {"count": 0, "fields": {}}}})
    conn = SimpleNamespace(execute=lambda *a: SimpleNamespace(fetchone=lambda: ("A title",)))
    db = SimpleNamespace(_conn=conn, db_path=str(tmp_path / "review.db"))
    spec = SimpleNamespace(extraction_models=SimpleNamespace(arm="an_arm"))
    selection = SimpleNamespace(to_extract=[(pid, object()) for pid in (1, 2, 3, 4)],
                                skipped_asserted=[], skipped_refused=[])
    with patch("engine.utils.ollama_preflight.require_preflight"), \
         patch("engine.agents.extractor.subprocess.run") as mock_run, \
         patch("engine.agents.extractor.httpx.get") as mock_get, \
         caplog.at_level(logging.WARNING, logger="engine.agents.extractor"):
        stats = extractor._extract_selected(db, spec, selection, 2, run_id=1,
                                            extractor_digest="d" * 64, auditor_digest=None)
    assert stats["extracted"] == 4 and stats["failed"] == 0
    mock_run.assert_not_called()
    mock_get.assert_not_called()
    lines = _skip_lines(caplog)
    assert len(lines) == 2, lines
    assert "(proactive after 2 papers, 2 papers done)" in lines[0]
    assert "(proactive after 2 papers, 4 papers done)" in lines[1]
