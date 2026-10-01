"""C47: an interrupt — KeyboardInterrupt, or SIGTERM/SIGHUP as `RunInterrupted` —
closes the run 'interrupted' with a reason naming it, in `run_pipeline` and in
the FT CLI's `run_ft_invocation`, then propagates to the command-line main,
which exits 128 + signum.

Every database is a scratch `ReviewDatabase` with a real manifest (injected
digest and git: no HTTP, no `git status`). No signal is ever sent: the handler
`interrupt_signals` installs is fetched with `signal.getsignal` and called.
"""

from __future__ import annotations

import shutil
import signal
import sqlite3
import sys
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import scripts.run_pipeline as rp
from engine.agents import extractor as E
from engine.agents import ft_screener as ft
from engine.agents.models import EvidenceSpan, ExtractionOutput
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec
from engine.utils import ollama_client as oc
from _event_store_fixture import open_extraction_run, seed_eligibility
from _parsed_text_fixture import write_parsed

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"
FT_DIGESTS = {"qwen3:32b": "1" * 64, "gemma3:27b": "2" * 64}
CLEAN = rm.GitState(commit="0" * 40, dirty=False, tag=None)
SIGNALS = [(signal.SIGTERM, rm.REASON_INTERRUPT_SIGTERM),
           (signal.SIGHUP, rm.REASON_INTERRUPT_SIGHUP)]


@pytest.fixture(scope="module")
def spec():
    return load_review_spec(REPO / "review_specs" / "surgical_autonomy.yaml")


@pytest.fixture
def fields():
    return tuple(f["name"] for f in load_codebook(LIVE_CODEBOOK).fields)


@pytest.fixture
def db(tmp_path):
    rdb = ReviewDatabase("intr", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(rdb.db_path).parent / "extraction_codebook.yaml")
    yield rdb
    rdb.close()


def _end(db, run_id):
    conn = sqlite3.connect(db.db_path)
    try:
        return conn.execute("SELECT end_status, end_reason, ended_at FROM run_manifests "
                            "WHERE run_id = ?", (run_id,)).fetchone()
    finally:
        conn.close()


def _raise(exc):
    def stage(*a, **k):
        raise exc
    return stage


@pytest.fixture
def pipeline(db, spec, monkeypatch):
    """`run_pipeline` on the scratch database with a real manifest; every stage
    stubbed, both gates open. Returns the run_id."""
    run_id = open_extraction_run(db, spec)
    monkeypatch.setattr(rp, "load_spec_for", lambda name, path=None: spec)
    monkeypatch.setattr(rp, "ReviewDatabase", lambda name: db)
    monkeypatch.setattr(rp, "_open_run_manifest", lambda d, s, i, **k: run_id)
    for stage in ("_stage_parse", "_stage_extract", "_stage_audit", "_stage_export"):
        monkeypatch.setattr(rp, stage, lambda *a, **k: {})
    monkeypatch.setattr(rp, "_advance_extraction_workflow", lambda conn: {})
    monkeypatch.setattr(rp, "is_adjudication_complete", lambda conn: True)
    monkeypatch.setattr(rp, "is_audit_review_complete", lambda conn: True)
    return run_id


# ── T1 / T2: run_pipeline ─────────────────────────────────────────────
def test_t1_run_pipeline_closes_a_keyboard_interrupt_as_sigint(db, pipeline, monkeypatch):
    monkeypatch.setattr(rp, "_stage_extract", _raise(KeyboardInterrupt()))
    with pytest.raises(KeyboardInterrupt):
        rp.run_pipeline("intr", skip_to="extract")
    status, reason, ended_at = _end(db, pipeline)
    assert (status, reason) == ("interrupted", rm.REASON_INTERRUPT_SIGINT)
    assert ended_at is not None


@pytest.mark.parametrize("signum,reason", SIGNALS)
def test_t2_run_pipeline_closes_a_signal_with_its_reason(db, pipeline, monkeypatch,
                                                         signum, reason):
    monkeypatch.setattr(rp, "_stage_extract", _raise(rm.RunInterrupted(signum)))
    with pytest.raises(rm.RunInterrupted) as caught:
        rp.run_pipeline("intr", skip_to="extract")
    assert caught.value.signum == signum and caught.value.run_id == pipeline
    assert _end(db, pipeline)[:2] == ("interrupted", reason)


# ── T3: run_ft_invocation ─────────────────────────────────────────────
@pytest.mark.parametrize("exc,reason", [
    (KeyboardInterrupt(), rm.REASON_INTERRUPT_SIGINT),
    (rm.RunInterrupted(signal.SIGTERM), rm.REASON_INTERRUPT_SIGTERM),
])
def test_t3_run_ft_invocation_closes_an_interrupt_with_its_reason(db, spec, monkeypatch,
                                                                  exc, reason):
    monkeypatch.setattr(ft, "run_ft_screening", _raise(exc))
    with pytest.raises(type(exc)) as caught:
        ft.run_ft_invocation(db, spec, review_name="intr", screen_only=True,
                             git=CLEAN, digest_fn=FT_DIGESTS.__getitem__)
    status, end_reason, ended_at = _end(db, caught.value.run_id)
    assert (status, end_reason) == ("interrupted", reason)
    assert ended_at is not None


# ── T4: handlers ──────────────────────────────────────────────────────
@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGHUP])
def test_t4_the_installed_handler_raises_run_interrupted_with_the_signum(signum):
    prior = signal.getsignal(signum)
    with rm.interrupt_signals():
        handler = signal.getsignal(signum)
        assert handler is not prior
        with pytest.raises(rm.RunInterrupted) as caught:
            handler(signum, None)
        assert caught.value.signum == signum
    assert signal.getsignal(signum) is prior


def _handlers():
    return {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGHUP)}


def _pipeline_main(monkeypatch, body):
    monkeypatch.setattr(sys, "argv", ["run_pipeline.py", "--review", "intr"])
    monkeypatch.setattr(rp, "run_pipeline", body)
    return rp.main()


def _ft_main(monkeypatch, db, body):
    import engine.core.review_paths as review_paths
    monkeypatch.setattr(review_paths, "load_spec_for", lambda name, path=None: None)
    monkeypatch.setattr(ft, "ReviewDatabase", lambda name: db)
    monkeypatch.setattr(ft, "run_ft_invocation", body)
    return ft.main(["--review", "intr"])


@pytest.mark.parametrize("raises", [None, KeyboardInterrupt(), RuntimeError("boom")])
@pytest.mark.parametrize("which", ["pipeline", "ft"])
def test_t4_each_main_installs_then_restores_the_handlers(monkeypatch, db, which, raises):
    before, during = _handlers(), {}

    def body(*a, **k):
        during.update(_handlers())
        if raises is not None:
            raise raises

    call = (lambda: _pipeline_main(monkeypatch, body)) if which == "pipeline" else \
           (lambda: _ft_main(monkeypatch, db, body))
    with (pytest.raises(RuntimeError) if isinstance(raises, RuntimeError) else nullcontext()):
        call()
    assert all(during[s] is not before[s] for s in before), "handlers were not installed"
    assert _handlers() == before, "handlers were not restored"


# ── T5: a gate close stands ───────────────────────────────────────────
def test_t5_an_interrupt_after_a_gate_close_leaves_the_gate_close(db, pipeline, monkeypatch):
    real = rp._finish_review_run
    calls = []

    def finish(*a, **k):
        calls.append(k.get("reason"))
        real(*a, **k)
        if len(calls) == 1:
            raise KeyboardInterrupt()   # arrives just after the gate's close

    monkeypatch.setattr(rp, "_finish_review_run", finish)
    monkeypatch.setattr(rp, "is_audit_review_complete", lambda conn: False)
    monkeypatch.setattr(rp, "get_current_blocker",
                        lambda conn: {"stage_name": "AUDIT_QUEUE_EXPORTED", "next_step": "x"})
    with pytest.raises(KeyboardInterrupt):          # the interrupt, not an IntegrityError
        rp.run_pipeline("intr", skip_to="extract")
    assert calls == [rm.REASON_BLOCKED_AUDIT_REVIEW]
    assert _end(db, pipeline)[:2] == ("interrupted", rm.REASON_BLOCKED_AUDIT_REVIEW)


# ── T6: the in-flight paper ───────────────────────────────────────────
class _InterruptingClient:
    """Pass 1 then a complete pass 2 per paper; the 4th call — paper 2's pass 2
    — raises KeyboardInterrupt, after paper 2's pass 1 was recorded."""

    def __init__(self, fields):
        self._pass2 = ExtractionOutput(fields=[
            EvidenceSpan(field_name=f, value="RCT" if f == "study_type" else "NR",
                         source_snippet=f"Snippet for {f}.", confidence=0.9, tier=1)
            for f in fields]).model_dump_json()
        self.calls = []
        self._client = SimpleNamespace(base_url="http://intr.invalid")

    def show(self, model):
        return SimpleNamespace(modelinfo={"general.context_length": 131_072})

    def chat(self, **kwargs):
        self.calls.append(kwargs)
        if len(self.calls) == 4:
            raise KeyboardInterrupt()
        first = len(self.calls) % 2 == 1
        content, thinking = ("draft", "Reasoning.") if first else (self._pass2, None)
        return SimpleNamespace(
            message=SimpleNamespace(content=content, thinking=thinking),
            prompt_eval_count=max(1, int(oc.message_chars(kwargs.get("messages")) * 0.3)),
            done_reason="stop", eval_count=10)


def test_t6_an_interrupt_mid_paper_keeps_finished_papers_and_writes_none_for_it(
        db, spec, fields, monkeypatch):
    import httpx
    for pid in (1, 2, 3):
        db._conn.execute("INSERT INTO papers (id, title, source, status, created_at, "
                         "updated_at) VALUES (?, 't', 's', 'FT_ELIGIBLE', 'n', 'n')", (pid,))
        seed_eligibility(db._conn, pid)
        write_parsed(db, pid, "\n".join(f"Snippet for {f}." for f in fields) + "\n")
    db._conn.commit()
    run_id = open_extraction_run(db, spec)
    monkeypatch.setattr(httpx.Client, "send",
                        lambda *a, **k: pytest.fail("an interrupt test made a real HTTP request"))
    monkeypatch.setattr(oc, "_client", _InterruptingClient(fields))
    monkeypatch.setattr(E, "hold_experiment_lock", nullcontext)
    monkeypatch.setattr(E, "restart_ollama", lambda **kw: None)
    monkeypatch.setattr(rp, "load_spec_for", lambda name, path=None: spec)
    monkeypatch.setattr(rp, "ReviewDatabase", lambda name: db)
    monkeypatch.setattr(rp, "_open_run_manifest", lambda d, s, i, **k: run_id)
    monkeypatch.setattr(rp, "is_adjudication_complete", lambda conn: True)
    oc.clear_ceiling_cache()
    try:
        with patch("engine.utils.ollama_preflight.require_preflight"), \
             pytest.raises(KeyboardInterrupt):
            rp.run_pipeline("intr", skip_to="extract")
    finally:
        oc.clear_ceiling_cache()

    conn = sqlite3.connect(db.db_path)
    try:
        def n(sql, *args):
            return conn.execute(sql, args).fetchone()[0]
        claims = "SELECT COUNT(*) FROM field_events WHERE paper_id = ?"
        processing = ("SELECT COUNT(*) FROM paper_events WHERE paper_id = ? AND to_state IN "
                      "('extracted', 'extraction_failed', 'audited_ai')")
        assert n(claims, 1) == len(fields) and n(processing, 1) == 1
        assert n(claims, 2) == 0 and n(processing, 2) == 0
        assert n(claims, 3) == 0 and n(processing, 3) == 0
        # paper 2's pass 1 returned, so its run_calls row stands; its pass 2 has none
        assert n("SELECT COUNT(*) FROM run_calls WHERE paper_id = 2") == 1
    finally:
        conn.close()
    assert _end(db, run_id)[:2] == ("interrupted", rm.REASON_INTERRUPT_SIGINT)


# ── T7: exit codes ────────────────────────────────────────────────────
@pytest.mark.parametrize("exc,code", [
    (KeyboardInterrupt(), 130),
    (rm.RunInterrupted(signal.SIGTERM), 143),
    (rm.RunInterrupted(signal.SIGHUP), 129),
])
@pytest.mark.parametrize("which", ["pipeline", "ft"])
def test_t7_each_main_exits_128_plus_the_signal(monkeypatch, db, which, exc, code):
    body = _raise(exc)
    got = _pipeline_main(monkeypatch, body) if which == "pipeline" else \
        _ft_main(monkeypatch, db, body)
    assert got == code


def test_t7_a_clean_main_exits_zero(monkeypatch, db):
    assert _pipeline_main(monkeypatch, lambda *a, **k: None) == 0
    assert _ft_main(monkeypatch, db, lambda *a, **k: 1) == 0


# ── T8: an uncommitted write is not committed by the close ────────────
def test_t8_an_interrupt_leaves_an_uncommitted_write_absent(db, pipeline, monkeypatch):
    def half_written(d, *a, **k):
        d._conn.execute("INSERT INTO papers (id, title, source, status, created_at, "
                        "updated_at) VALUES (99, 't', 's', 'INGESTED', 'n', 'n')")
        raise KeyboardInterrupt()

    monkeypatch.setattr(rp, "_stage_extract", half_written)
    with pytest.raises(KeyboardInterrupt):
        rp.run_pipeline("intr", skip_to="extract")
    conn = sqlite3.connect(db.db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM papers WHERE id = 99").fetchone()[0] == 0
    finally:
        conn.close()
    assert _end(db, pipeline)[:2] == ("interrupted", rm.REASON_INTERRUPT_SIGINT)
