"""P1 unit U02 (R612): the run-fault predicate and extract-path handlers 1 and 2.

C56 / A-7 under R531–R533 and R542. A run fault is not a paper's outcome: it
leaves `_extract_selected` and `_retry_snippet` as itself, the paper keeps no
`extraction_failed` event, and the consecutive-failure count does not move.
Everything else is still a paper outcome (R532).

Riding here: Wave 1 row 8 / N41 (post-commit bookkeeping is outside the try; the
failure event is written before any progress output), Wave 2 row 24 (the
progress reporter survives a dead stderr), Wave 1 row 81 (an answered snippet
retry keeps its request hash) and Wave 1 row 84 (a failed BEGIN IMMEDIATE
surfaces as itself).

Every database is a scratch `ReviewDatabase` under `tmp_path`; no model, no
service, no live review.
"""

from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import ollama
import pytest

from engine.agents import extractor as E
from engine.agents.models import EvidenceSpan, ExtractionOutput
from engine.core import extraction_events as X
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.effective_config import UndeclaredOverride, stage_config
from engine.core.events import EventRefused
from engine.core.review_spec import load_review_spec
from engine.elicitation.classes import CodebookContractError
from engine.elicitation.materialize import SnippetNotExact
from engine.elicitation.pipeline import ElicitationSentinelUnsatisfiable
from engine.utils import ollama_client as oc
from engine.utils.progress import ProgressReporter
from _event_store_fixture import open_extraction_run, seed_eligibility
from _parsed_text_fixture import write_parsed

REPO = Path(__file__).resolve().parent.parent
SPEC_PATH = REPO / "review_specs" / "surgical_autonomy.yaml"
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"
PIDS = (7, 8)

# THE TUPLE, as U02's handoff fixes it (R612(g)), one instance per class. The
# connection entries are how a refused or timed-out connection to the Ollama
# server reaches the extract loop with ollama 0.6.1: the client turns
# httpx.ConnectError into the builtin ConnectionError; a connect timeout stays
# httpx.ConnectTimeout.
RUN_FAULTS = {
    sqlite3.Error: lambda: sqlite3.OperationalError("database is locked"),
    oc.CeilingUnavailable: lambda: oc.CeilingUnavailable(model="m", reason="show failed"),
    rm.UndeclaredCall: lambda: rm.UndeclaredCall("extract_pass2", None, "m"),
    UndeclaredOverride: lambda: UndeclaredOverride("vision_parse", {"num_ctx": {"recorded": 1, "requested": 2}}),
    rm.RunRefused: lambda: rm.RunRefused("a run that may not open"),
    X.RunAborted: lambda: X.RunAborted("aborted"),
    EventRefused: lambda: EventRefused("a write the store forbids"),
    CodebookContractError: lambda: CodebookContractError("the codebook cannot support it"),
    X.ExhaustedWithoutRecord: lambda: X.ExhaustedWithoutRecord("no record"),
    SnippetNotExact: lambda: SnippetNotExact("not the text at its offsets"),
    ConnectionError: lambda: ConnectionError("Failed to connect to Ollama."),
    httpx.ConnectError: lambda: httpx.ConnectError("connection refused"),
    httpx.ConnectTimeout: lambda: httpx.ConnectTimeout("connect timed out"),
}
# every RunRefused member, by name, so a new one without a case fails here
RUN_REFUSED_MEMBERS = {
    rm.DirtyTree, rm.AmbiguousEngineState, rm.PreManifestArm, rm.RetiredArm,
    rm.ArmPinMismatch, rm.CloudArmNotEnabled, rm.StageNotInRun,
    rm.CodebookReviewMismatch,
    ElicitationSentinelUnsatisfiable,
}
FAULTS = [pytest.param(make, id=cls.__name__) for cls, make in RUN_FAULTS.items()] + [
    pytest.param(lambda c=c: c("refused"), id=c.__name__)
    for c in sorted(RUN_REFUSED_MEMBERS, key=lambda c: c.__name__)]

# R532's complement: still paper outcomes, each with the reason it records.
OUTCOMES = [
    pytest.param(lambda: TimeoutError("timed out after 3 attempts + restart"),
                 "model_call_failed", id="TimeoutError"),
    pytest.param(lambda: oc.InputOverflow(model="m", chars=9, estimate_low=1.0, ceiling=1),
                 "input_overflow_estimated", id="InputOverflow"),
    pytest.param(lambda: oc.InputTruncated(model="m", count=9, ceiling=9, chars=9),
                 "input_truncated_at_ceiling", id="InputTruncated"),
    pytest.param(lambda: oc.InputDropped(model="m", count=1, chars=9, floor=2.0, ceiling=9),
                 "input_dropped_below_floor", id="InputDropped"),
    pytest.param(lambda: ollama.ResponseError("model runner has crashed", 500),
                 "model_call_failed", id="ResponseError"),
    pytest.param(lambda: httpx.ReadTimeout("read timed out"),
                 "model_call_failed", id="ReadTimeout"),
]


@pytest.fixture(scope="module")
def spec():
    return load_review_spec(SPEC_PATH)


@pytest.fixture
def db(tmp_path):
    rdb = ReviewDatabase("runfault", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(rdb.db_path).parent / "extraction_codebook.yaml")
    for pid in PIDS:
        rdb._conn.execute("INSERT INTO papers (id, title, source, status, created_at, "
                          "updated_at) VALUES (?, 't', 's', 'FT_ELIGIBLE', 'n', 'n')", (pid,))
        seed_eligibility(rdb._conn, pid)
        write_parsed(rdb, pid, "The paper reports a trial. A clean sentence.\n")
    rdb._conn.commit()
    yield rdb
    rdb.close()


def _run(db, spec, run_id):
    with patch("engine.utils.ollama_preflight.require_preflight"):
        return E.run_extraction(db, spec, "runfault", experiment_lock=False,
                                restart_every=0, run_id=run_id)


def _failed_events(db):
    return [tuple(r) for r in db._conn.execute(
        "SELECT paper_id, reason_code FROM paper_events WHERE to_state IN "
        "('extraction_failed', 'input_exceeds_context') ORDER BY event_id")]


# ── the declared tuple ────────────────────────────────────────────────
def test_the_declared_run_fault_tuple_is_the_handoffs():
    assert set(X.run_fault_classes()) == set(RUN_FAULTS)
    for cls in RUN_REFUSED_MEMBERS:
        assert issubclass(cls, rm.RunRefused)
    # no RunRefused member exists that this file does not exercise
    def subclasses(c):
        return {s for d in c.__subclasses__() for s in {d} | subclasses(d)}
    assert subclasses(rm.RunRefused) == RUN_REFUSED_MEMBERS
    assert all(X.is_run_fault(make()) for make in RUN_FAULTS.values())
    assert not any(X.is_run_fault(p.values[0]()) for p in OUTCOMES)
    assert not X.is_run_fault(ValueError("anything else"))


# ── handler 2: _extract_selected ──────────────────────────────────────
@pytest.mark.parametrize("make", FAULTS)
def test_run_faults_leave_the_extract_path_as_themselves(db, spec, make, monkeypatch):
    run_id = open_extraction_run(db, spec)
    exc = make()
    # one failure would reach the abort: a run fault must not be counted as one
    monkeypatch.setattr(E, "CONSECUTIVE_FAILURE_ABORT", 1)
    with patch.object(E, "extract_paper_with_completeness", side_effect=exc) as call:
        with pytest.raises(type(exc)) as err:
            _run(db, spec, run_id)
    assert err.value is exc                      # itself, not RunAborted, not a wrapper
    assert call.call_count == 1                  # the run stopped at this paper
    assert _failed_events(db) == []              # the paper is untouched


@pytest.mark.parametrize("make, reason", OUTCOMES)
def test_the_complement_is_still_a_paper_outcome(db, spec, make, reason):
    run_id = open_extraction_run(db, spec)

    def raiser(*a, **kw):
        raise make()
    with patch.object(E, "extract_paper_with_completeness", side_effect=raiser) as call:
        stats = _run(db, spec, run_id)
    assert call.call_count == len(PIDS)          # the run went on to the next paper
    assert stats["failed"] == len(PIDS) and stats["extracted"] == 0
    assert _failed_events(db) == [(pid, reason) for pid in PIDS]


# ── handler 1: _retry_snippet ─────────────────────────────────────────
@pytest.mark.parametrize("make", FAULTS)
def test_run_faults_leave_the_snippet_retry_as_themselves(spec, make, monkeypatch):
    exc = make()

    def boom(**kw):
        raise exc
    monkeypatch.setattr(E, "ollama_chat", boom)
    with pytest.raises(type(exc)) as err:
        E._retry_snippet("f", "v", "text", 7, cfg=stage_config("extract_retry_snippet", spec),
                         return_request_hash=True)
    assert err.value is exc


@pytest.mark.parametrize("make, reason", OUTCOMES)
def test_the_complement_is_still_a_failed_snippet_retry(spec, make, reason, monkeypatch):
    def boom(**kw):
        raise make()
    monkeypatch.setattr(E, "ollama_chat", boom)
    cfg = stage_config("extract_retry_snippet", spec)
    assert E._retry_snippet("f", "v", "text", 7, cfg=cfg, return_request_hash=True) == (None, None)
    assert E._retry_snippet("f", "v", "text", 7, cfg=cfg) is None


# ── Wave 1 row 81: an answered call keeps its request hash ────────────
def test_an_answered_snippet_retry_that_does_not_parse_keeps_its_request_hash(spec, monkeypatch):
    answered = SimpleNamespace(message=SimpleNamespace(content="this is not a JSON object"))
    monkeypatch.setattr(E, "ollama_chat", lambda **kw: (answered, "a" * 64))
    cfg = stage_config("extract_retry_snippet", spec)
    assert E._retry_snippet("f", "v", "text", 7, cfg=cfg,
                            return_request_hash=True) == (None, "a" * 64)
    # a JSON value that is not an object is answered-and-unparseable too
    monkeypatch.setattr(E, "ollama_chat", lambda **kw: (
        SimpleNamespace(message=SimpleNamespace(content='["a list"]')), "b" * 64))
    assert E._retry_snippet("f", "v", "text", 7, cfg=cfg,
                            return_request_hash=True) == (None, "b" * 64)
    # and the hash reaches the chain the claims' context is built from
    span = EvidenceSpan(field_name="f", value="v", source_snippet="a ... b",
                        confidence=0.9, tier=1)
    _, contexts, chain = E._validate_and_retry_snippets(
        [span], "text", 7, cfg=cfg, return_snippet_contexts=True)
    assert chain == ["b" * 64] * E.SNIPPET_MAX_RETRIES and contexts == {}


# ── Wave 1 row 8 / N41: bookkeeping is outside the try ────────────────
class _Client:
    """`ollama.Client` stand-in (as in test_extraction_run_link): pass 1, then a
    complete pass 2, no snippet retry."""

    def __init__(self, codebook_path):
        cb = load_codebook(codebook_path)
        self._pass2 = ExtractionOutput(fields=[
            EvidenceSpan(field_name=f["name"], value="NR",
                         source_snippet=f"Snippet for {f['name']}.",
                         confidence=0.9, tier=f["tier"]) for f in cb.fields
        ]).model_dump_json()
        self.calls = []
        self._client = SimpleNamespace(base_url="http://runfault.invalid")

    def show(self, model):
        return SimpleNamespace(modelinfo={"general.context_length": 131_072})

    def chat(self, **kwargs):
        self.calls.append(kwargs)
        first = len(self.calls) % 2 == 1
        content, thinking = (("draft", "The paper reports a trial.") if first
                             else (self._pass2, None))
        chars = oc.message_chars(kwargs.get("messages"))
        return SimpleNamespace(
            message=SimpleNamespace(content=content, thinking=thinking),
            prompt_eval_count=max(1, int(chars * 0.3)), done_reason="stop", eval_count=10)


@pytest.fixture
def fake_ollama(monkeypatch, db):
    def _refuse(*a, **kw):
        raise AssertionError("a run-fault test attempted a real HTTP request")
    monkeypatch.setattr(httpx.Client, "send", _refuse)
    client = _Client(Path(db.db_path).parent / "extraction_codebook.yaml")
    monkeypatch.setattr(oc, "_client", client)
    oc.clear_ceiling_cache()
    yield client
    oc.clear_ceiling_cache()


def test_a_post_commit_bookkeeping_failure_is_not_an_extraction_failure(
        db, spec, fake_ollama, monkeypatch):
    run_id = open_extraction_run(db, spec)

    calls = []

    def report(self, paper_id, status, elapsed):
        # transient: only the first write fails (a persistent failure raised out
        # of the old handler too, before it could write the wrong event)
        calls.append(status)
        if len(calls) == 1:
            raise RuntimeError("bookkeeping failed after the commit")
    monkeypatch.setattr(ProgressReporter, "report", report)
    with pytest.raises(RuntimeError, match="bookkeeping failed"):
        _run(db, spec, run_id)
    assert calls == ["EXTRACTED"]
    # the first paper's claims committed and it is not recorded as failed
    n = db._conn.execute("SELECT COUNT(*) FROM field_events WHERE paper_id = ? AND "
                         "event_type = 'asserted'", (PIDS[0],)).fetchone()[0]
    assert n == 20
    assert _failed_events(db) == []


def test_a_progress_failure_in_the_handler_does_not_cost_the_failure_event(
        db, spec, monkeypatch):
    run_id = open_extraction_run(db, spec)

    def report(self, paper_id, status, elapsed):
        raise RuntimeError("progress write failed")
    monkeypatch.setattr(ProgressReporter, "report", report)
    with patch.object(E, "extract_paper_with_completeness",
                      side_effect=TimeoutError("timed out")):
        with pytest.raises(RuntimeError, match="progress write failed"):
            _run(db, spec, run_id)
    assert _failed_events(db) == [(PIDS[0], "model_call_failed")]


# ── Wave 2 row 24: the reporter survives a dead stderr ────────────────
def test_the_progress_reporter_survives_a_broken_stream(caplog):
    class _Dead:
        writes = 0

        def write(self, text):
            _Dead.writes += 1
            raise BrokenPipeError(32, "Broken pipe")

        def flush(self):
            raise BrokenPipeError(32, "Broken pipe")

    reporter = ProgressReporter(3, "run", file=_Dead())
    with caplog.at_level("WARNING"):
        reporter.report(1, "EXTRACTED", 1.0)
        first = _Dead.writes
        reporter.report(2, "FAILED", 1.0)
        block = reporter.summary()
    assert (reporter.completed, reporter.failed) == (2, 1)      # the counts are kept
    assert _Dead.writes == first                                # and it stops writing
    assert "Total papers : 2" in block
    assert sum("progress output" in r.message for r in caplog.records) == 1


# ── Wave 1 row 84: a failed BEGIN IMMEDIATE surfaces as itself ────────
def test_a_lock_on_begin_immediate_surfaces_as_the_lock_error(db):
    db._conn.execute("PRAGMA busy_timeout=0")
    holder = sqlite3.connect(db.db_path, timeout=0)
    try:
        holder.execute("BEGIN IMMEDIATE")
        with pytest.raises(sqlite3.OperationalError, match="database is locked"):
            db.update_status(PIDS[0], "FT_SCREENED_OUT")
    finally:
        holder.rollback()
        holder.close()
    assert not db._conn.in_transaction
