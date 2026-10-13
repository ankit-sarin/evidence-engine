"""A failed manifest close says what failed: "<ExceptionClass>: <message>".

U12 commit 4 (R625(a)(8), INT-l12-14). `run_pipeline`, the FT invocation and the cloud
runner closed a failed run with no reason, and the three import paths stored the message
without its class. One helper, `run_manifest.failure_reason`, now writes the reason at
all six sites. This file holds the helper's own tests and the cloud runner's; the other
five sites are asserted where each is already driven:
`test_run_pipeline_closes.py`, `test_ft_bridge.py`, `test_ft_adjudication_import.py`,
`test_import_extraction_entry.py`, `test_import_screening_entry.py`.
"""

from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path

import pytest

from engine.core import run_manifest as rm
from engine.core.database import ReviewDatabase

REPO = Path(__file__).resolve().parent.parent
SPEC_PATH = REPO / "review_specs" / "surgical_autonomy.yaml"
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"


class Refused(RuntimeError):
    pass


class Stop(BaseException):
    pass


# ── T1 — the helper ──────────────────────────────────────────────────


@pytest.mark.parametrize("exc,context,want", [
    (ValueError("x"), None, "ValueError: x"),
    (Refused("no"), None, "Refused: no"),                                   # unqualified class name
    (RuntimeError(""), None, "RuntimeError"),                               # an empty message
    (RuntimeError(), None, "RuntimeError"),
    (RuntimeError("first line\nsecond line\nthird"), None, "RuntimeError: first line"),
    (RuntimeError("\n\n  spaced   out\ttext  \nmore"), None, "RuntimeError: spaced out text"),
    (KeyboardInterrupt(), None, "KeyboardInterrupt"),                       # a BaseException
    (Stop("now"), None, "Stop: now"),
    (RuntimeError("injected"), "entry 2 (EE2)", "RuntimeError: entry 2 (EE2): injected"),
    (RuntimeError(""), "entry 2 (EE2)", "RuntimeError: entry 2 (EE2)"),     # context, no message
    (RuntimeError("m"), "", "RuntimeError: m"),                             # an empty context is none
    (KeyError("missing"), None, "KeyError: 'missing'"),                     # str(exc), as Python gives it
], ids=["class_and_message", "unqualified_class", "empty_message", "no_arguments", "first_line_only",
        "whitespace_collapsed", "keyboard_interrupt", "base_exception", "context", "context_no_message",
        "empty_context", "str_of_the_exception"])
def test_failure_reason_is_the_class_then_the_context_then_the_first_line(exc, context, want):
    assert rm.failure_reason(exc, context=context) == want


def test_failure_reason_is_cut_to_500_characters_with_a_marker():
    reason = rm.failure_reason(RuntimeError("m" * 2000))
    assert len(reason) == rm.FAILURE_REASON_MAX == 500
    assert reason.startswith("RuntimeError: mmm") and reason.endswith("m…")
    with_context = rm.failure_reason(RuntimeError("m" * 2000), context="entry 9 (X)")
    assert len(with_context) == 500 and with_context.startswith("RuntimeError: entry 9 (X): mm")
    # exactly at the limit is not cut
    exact = "m" * (500 - len("RuntimeError: "))
    assert rm.failure_reason(RuntimeError(exact)) == "RuntimeError: " + exact


def test_a_stored_failure_reason_satisfies_the_manifest_check(tmp_path):
    """The reason is free text for a 'failed' close (022's CHECK); the longest one is stored whole."""
    conn = sqlite3.connect(tmp_path / "x.db")
    conn.execute("CREATE TABLE run_manifests (run_id INTEGER PRIMARY KEY, ended_at TEXT, "
                 "end_status TEXT, end_reason TEXT)")
    conn.execute("INSERT INTO run_manifests (run_id) VALUES (1)")
    reason = rm.failure_reason(RuntimeError("m" * 2000))
    rm.close_run(conn, 1, "failed", reason=reason)
    assert conn.execute("SELECT end_status, end_reason FROM run_manifests").fetchone() == ("failed", reason)


# ── T3 — the cloud runner (site 3) ───────────────────────────────────


@pytest.fixture
def cloud(tmp_path, monkeypatch):
    """`run_arm` on a scratch review with one enabled cloud arm and a transport that sends
    nothing. Returns (run, database path): `run(behaviour)` calls `run_arm` with a
    transport whose `run()` is `behaviour`."""
    if not LIVE_CODEBOOK.exists():
        pytest.skip("codebook not available")
    from engine.core.review_spec import load_review_spec
    from scripts import run_cloud_extraction as cli

    db = ReviewDatabase("failedclose", data_root=tmp_path)
    db_path = Path(db.db_path)
    shutil.copy2(LIVE_CODEBOOK, db_path.parent / "extraction_codebook.yaml")
    db.close()
    text = SPEC_PATH.read_text()
    assert text.count("enabled_arms: []") == 1
    arm = next(a.name for a in load_review_spec(SPEC_PATH).arms if a.provider == "openai")
    spec_path = tmp_path / "spec.yaml"
    spec_path.write_text(text.replace("enabled_arms: []", f"enabled_arms: [{arm}]"))
    monkeypatch.setattr(rm, "git_state",
                        lambda *a, **k: rm.GitState(commit="b" * 40, dirty=False, tag=None))

    def run(behaviour):
        class Transport:
            def __init__(self, db_path, spec_path, arm_name):
                self._conn = sqlite3.connect(db_path)
                self.run_id = None

            def run(self, **kw):
                return behaviour()

            def close(self):
                self._conn.close()

        monkeypatch.setitem(cli.EXTRACTORS, "openai", Transport)
        cli.run_arm(arm, str(db_path), str(spec_path), None, None)

    return run, db_path


def _end(db_path):
    conn = sqlite3.connect(db_path)
    try:
        return conn.execute("SELECT end_status, end_reason, ended_at IS NOT NULL "
                            "FROM run_manifests ORDER BY run_id").fetchall()
    finally:
        conn.close()


def _raise(exc):
    def behaviour():
        raise exc
    return behaviour


def test_a_cloud_arm_that_raises_closes_failed_with_the_class_and_message(cloud):
    run, db_path = cloud
    with pytest.raises(RuntimeError, match="provider down"):
        run(_raise(RuntimeError("provider down\nretry later")))
    assert _end(db_path) == [("failed", "RuntimeError: provider down", 1)]


def test_a_cloud_arm_interrupted_closes_failed_naming_the_interrupt(cloud):
    """This runner has no interrupt handling of its own (C47 covers run_pipeline and the FT
    CLI): an interrupt is a failed close here, as before, and now it says so."""
    run, db_path = cloud
    with pytest.raises(KeyboardInterrupt):
        run(_raise(KeyboardInterrupt()))
    assert _end(db_path) == [("failed", "KeyboardInterrupt", 1)]


def test_a_cloud_arm_that_returns_closes_completed_with_no_reason(cloud):
    run, db_path = cloud
    run(lambda: None)
    assert _end(db_path) == [("completed", None, 1)]
