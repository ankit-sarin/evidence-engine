"""An entry point takes the experiment lock before it opens anything, and refuses at once.

U12 commit 2 (R629(b), (c), (e); N19, L11-C4). `test_lock_census.py` proves statically that
every entry point with a path to a local model call sits under the lock; these tests run
two of them: `run_pipeline`, which before this commit held the lock for its extract stage
only, and the preflight CLI, whose probe is one timeout away from a service restart.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import scripts.run_pipeline as rp
from engine.utils import ollama_lock as L
from engine.utils import ollama_preflight as preflight

_HOLDER = """
import fcntl, os, sys
fd = os.open(sys.argv[1], os.O_RDWR | os.O_CREAT, 0o644)
fcntl.flock(fd, fcntl.LOCK_EX)
print("held", flush=True)
sys.stdin.read()
"""


@pytest.fixture
def held_elsewhere():
    """Another process holds this test's lock for the test's duration. Yields the path."""
    lock = os.environ["OLLAMA_EXPERIMENT_LOCK"]
    holder = subprocess.Popen([sys.executable, "-c", _HOLDER, lock],
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    try:
        assert holder.stdout.readline().strip() == "held"
        yield lock
    finally:
        holder.stdin.close()
        holder.wait(timeout=10)


def _never(name):
    def called(*a, **k):
        pytest.fail(f"{name} ran although the experiment lock was held elsewhere")
    return called


# ── T8 — run_pipeline refuses before it opens a database ─────────────


def test_run_pipeline_refuses_under_a_held_lock_before_opening_the_database(
        held_elsewhere, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["run_pipeline.py", "--review", "locked_out"])
    monkeypatch.setattr(rp, "ReviewDatabase", _never("ReviewDatabase"))
    monkeypatch.setattr(rp, "load_spec_for", _never("load_spec_for"))
    monkeypatch.setattr(rp, "_open_run_manifest", _never("_open_run_manifest"))
    with pytest.raises(SystemExit) as stopped:
        rp.main()
    assert stopped.value.code not in (0, None)              # a non-zero exit
    assert held_elsewhere in str(stopped.value.code)        # with the refusal's message
    assert str(stopped.value.code).startswith("Experiment lock held: ")
    assert L.self_holds_lock() is False


# ── T9 — every stage of run_pipeline runs under the lock ─────────────


def test_run_pipeline_holds_the_lock_across_every_stage(monkeypatch, tmp_path):
    seen: dict[str, bool] = {}

    def stage(name):
        def run(*a, **k):
            seen[name] = L.self_holds_lock()
            return {}
        return run

    db = SimpleNamespace(_conn=object(), db_path=str(tmp_path / "review.db"),
                         get_pipeline_stats=lambda: {}, close=lambda: seen.setdefault("closed", L.self_holds_lock()))
    spec = SimpleNamespace(review_id="held", title="t", version="1")
    monkeypatch.setattr(sys, "argv", ["run_pipeline.py", "--review", "held"])
    monkeypatch.setattr(rp, "load_spec_for", lambda name, path=None: spec)
    monkeypatch.setattr(rp, "ReviewDatabase", lambda name: seen.setdefault("database", L.self_holds_lock()) and db)
    monkeypatch.setattr(rp, "format_workflow_status", lambda conn, review_name=None: "")
    monkeypatch.setattr(rp, "_open_run_manifest",
                        lambda db, spec, start_idx, max_papers=None: seen.setdefault("manifest", L.self_holds_lock()) and 1)
    monkeypatch.setattr(rp.rm, "activate", lambda conn, run_id: None)
    monkeypatch.setattr(rp.rm, "deactivate", lambda token: None)
    monkeypatch.setattr(rp, "is_adjudication_complete", lambda conn: True)
    monkeypatch.setattr(rp, "is_audit_review_complete", lambda conn: True)
    monkeypatch.setattr(rp, "_advance_extraction_workflow", lambda conn: {})
    monkeypatch.setattr(rp, "_finish_review_run", lambda db, run_id, status, reason=None: seen.setdefault("close_run", status))
    for name in ("search", "screen", "parse", "extract", "audit", "export"):
        monkeypatch.setattr(rp, f"_stage_{name}", stage(name))

    assert L.self_holds_lock() is False
    assert rp.main() == 0
    assert seen == {"database": True, "manifest": True, "search": True, "screen": True,
                    "parse": True, "extract": True, "audit": True, "export": True,
                    "close_run": "completed", "closed": True}
    assert L.self_holds_lock() is False and L.check_experiment_lock() is False


def test_run_extractions_own_acquire_nests_under_the_entry_points(monkeypatch):
    """C3: `run_extraction` keeps its acquisition; under a holder it re-enters, no refusal."""
    from engine.agents import extractor as E
    seen = {}
    monkeypatch.setattr(E, "_run_extraction_unlocked",
                        lambda *a, **k: seen.setdefault("held", L.self_holds_lock()) and {"extracted": 0})
    with L.hold_experiment_lock():
        fd = L._SELF_FD
        E.run_extraction(db=None, spec=None, review_name="r", run_id=1)
        assert L._SELF_FD == fd and L.self_holds_lock() is True     # the outer hold survives
    assert seen == {"held": True}


# ── T10 — the preflight CLI refuses under a held lock ────────────────


def test_the_preflight_cli_refuses_under_a_held_lock_before_probing(held_elsewhere, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["ollama_preflight.py", "--models", "some-model:1b"])
    monkeypatch.setattr(preflight, "preflight_check", _never("preflight_check"))
    monkeypatch.setattr(preflight, "check_model", _never("check_model"))
    with pytest.raises(SystemExit) as stopped:
        preflight.main()
    assert stopped.value.code not in (0, None)
    assert held_elsewhere in str(stopped.value.code)


def test_the_preflight_cli_probes_under_the_lock_when_it_is_free(monkeypatch, capsys):
    seen = {}

    def probe(models, timeout=30, *, spec=None):
        seen["held"] = L.self_holds_lock()
        return preflight.PreflightResult(success=True, models=[], total_vram_gb=0.0)

    monkeypatch.setattr(sys, "argv", ["ollama_preflight.py", "--models", "some-model:1b"])
    monkeypatch.setattr(preflight, "preflight_check", probe)
    preflight.main()
    assert seen == {"held": True} and L.self_holds_lock() is False
    assert "Status: PASS" in capsys.readouterr().out
