"""The cut-over (WRITE-PATH-01 9b-FLIP 2/2, R111): T1–T5 and T7.

Every database is a scratch `ReviewDatabase` with a real manifest (the 2(b)
helper, which declares the extraction stages and `audit`). Ollama is the fake
client at `ollama_client._client`; the semantic auditor and the preflights are
mocked. No model is called and nothing reaches `data/`.
"""

from __future__ import annotations

import ast
import inspect
import json
import shutil
import sqlite3
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from engine.agents import audit_events as AE
from engine.agents import extractor as E
from engine.agents.auditor import AuditVerdict
from engine.agents.models import EvidenceSpan, ExtractionOutput
from engine.core import extraction_events as X
from engine.core import paper_state as PS
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.effective import effective_state
from engine.core.events import PAYLOAD_PARSED_TEXT_SHA256, PAYLOAD_PARSED_TEXT_UID, PAYLOAD_REUSE_KEY
from engine.core.parsed_text import resolve_parsed_text
from engine.core.review_spec import load_review_spec
from engine.utils import ollama_client as oc
from _event_store_fixture import open_extraction_run, seed_eligibility
from _parsed_text_fixture import write_parsed

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"
STATUSES = {1: "FT_ELIGIBLE", 2: "AI_AUDIT_COMPLETE", 3: "PARSED"}


@pytest.fixture(scope="module")
def spec():
    return load_review_spec(REPO / "review_specs" / "surgical_autonomy.yaml")


@pytest.fixture
def fields():
    return tuple(f["name"] for f in load_codebook(LIVE_CODEBOOK).fields)


def _paper_text(fields):
    return "\n".join(f"Snippet for {f}." for f in fields) + "\n"


@pytest.fixture
def db(tmp_path, fields):
    rdb = ReviewDatabase("flip", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(rdb.db_path).parent / "extraction_codebook.yaml")
    for pid, status in STATUSES.items():
        rdb._conn.execute("INSERT INTO papers (id, title, source, status, created_at, "
                          "updated_at) VALUES (?, 't', 's', ?, 'n', 'n')", (pid, status))
        seed_eligibility(rdb._conn, pid)
        write_parsed(rdb, pid, _paper_text(fields))
    rdb._conn.commit()
    yield rdb
    rdb.close()


class _Client:
    """`ollama.Client` stand-in: pass 1, then a complete pass 2 whose every
    snippet is a verbatim line of the paper."""

    def __init__(self, fields):
        self._pass2 = ExtractionOutput(fields=[
            EvidenceSpan(field_name=f, value="RCT" if f == "study_type" else "NR",
                         source_snippet=f"Snippet for {f}.", confidence=0.9, tier=1)
            for f in fields]).model_dump_json()
        self.calls = []
        self._client = SimpleNamespace(base_url="http://flip.invalid")

    def show(self, model):
        return SimpleNamespace(modelinfo={"general.context_length": 131_072})

    def chat(self, **kwargs):
        self.calls.append(kwargs)
        first = len(self.calls) % 2 == 1
        content, thinking = ("draft", "Reasoning.") if first else (self._pass2, None)
        return SimpleNamespace(
            message=SimpleNamespace(content=content, thinking=thinking),
            prompt_eval_count=max(1, int(oc.message_chars(kwargs.get("messages")) * 0.3)),
            done_reason="stop", eval_count=10)


@pytest.fixture
def fake_models(monkeypatch, fields):
    import httpx
    monkeypatch.setattr(httpx.Client, "send",
                        lambda *a, **k: pytest.fail("a cut-over test made a real HTTP request"))
    monkeypatch.setattr(oc, "_client", _Client(fields))
    monkeypatch.setattr(E, "hold_experiment_lock", nullcontext)
    monkeypatch.setattr(E, "restart_ollama", lambda **kw: None)
    oc.clear_ceiling_cache()
    with patch("engine.utils.ollama_preflight.require_preflight"), \
         patch.object(AE, "semantic_verify", return_value=AuditVerdict(
             status="verified", grep_found=True, reasoning="ok")):
        yield
    oc.clear_ceiling_cache()


def _count(db, table, where="", args=()):
    return db._conn.execute(f"SELECT COUNT(*) FROM {table} {where}", args).fetchone()[0]


def _paper_events(db, pid):
    return [r[0] for r in db._conn.execute(
        "SELECT to_state FROM paper_events WHERE paper_id = ? AND event_type <> 'screened' "
        "ORDER BY event_id", (pid,))]


# ── T1 (G3) ───────────────────────────────────────────────────────────
def test_t1_extract_then_audit_under_one_manifest_writes_events_only(db, spec, fields,
                                                                    fake_models):
    import scripts.run_pipeline as rp
    run_id = open_extraction_run(db, spec)
    legacy = (_count(db, "extractions"), _count(db, "evidence_spans"))
    statuses = dict(db._conn.execute("SELECT id, status FROM papers").fetchall())

    ext = rp._stage_extract(db, spec, "flip", run_id=run_id)
    aud = rp._stage_audit(db, "flip", spec, run_id=run_id)

    assert ext["extracted"] == 3 and aud["papers_audited"] == 3
    for pid in STATUSES:
        claims = db._conn.execute(
            "SELECT claim_id, payload_json, run_id FROM field_events WHERE paper_id = ? "
            "AND event_type = 'asserted'", (pid,)).fetchall()
        assert len(claims) == len(fields)
        for _, payload, rid in claims:
            p = json.loads(payload)
            assert p[PAYLOAD_REUSE_KEY] and p[PAYLOAD_PARSED_TEXT_SHA256] and \
                p[PAYLOAD_PARSED_TEXT_UID] and rid == run_id
        located = {r[0] for r in db._conn.execute(
            "SELECT claim_id FROM field_events WHERE paper_id = ? AND event_type = "
            "'citation_located'", (pid,))}
        assert located == {c[0] for c in claims}
        assert _paper_events(db, pid) == ["extracted", "audited_ai"]
        assert effective_state(db._conn, pid).processing == "audited_ai"
    assert (_count(db, "extractions"), _count(db, "evidence_spans")) == legacy
    assert dict(db._conn.execute("SELECT id, status FROM papers").fetchall()) == statuses


# ── T2 (finding 1, D17) ───────────────────────────────────────────────
def test_t2_a_live_shaped_paper_extracts_and_keeps_its_status(db, spec, fake_models):
    run_id = open_extraction_run(db, spec)
    stats = E.run_extraction(db, spec, "flip", restart_every=0, run_id=run_id)
    assert stats["extracted"] == 3 and stats["failed"] == 0
    assert db._conn.execute("SELECT status FROM papers WHERE id = 2").fetchone()[0] \
        == "AI_AUDIT_COMPLETE"
    assert effective_state(db._conn, 2).processing == "extracted"


# ── T3 ────────────────────────────────────────────────────────────────
def _add_paper(db, pid, fields):
    db._conn.execute("INSERT INTO papers (id, title, source, status, created_at, updated_at) "
                     "VALUES (?, 't', 's', 'FT_ELIGIBLE', 'n', 'n')", (pid,))
    seed_eligibility(db._conn, pid)
    write_parsed(db, pid, _paper_text(fields))
    db._conn.commit()


def test_t3_three_consecutive_failures_abort_after_the_third_event(db, spec, fields,
                                                                   fake_models):
    _add_paper(db, 4, fields)
    run_id = open_extraction_run(db, spec)
    with patch.object(E, "extract_paper_with_completeness",
                      side_effect=TimeoutError("gave up")):
        with pytest.raises(X.RunAborted, match="3 consecutive papers"):
            E.run_extraction(db, spec, "flip", restart_every=0, run_id=run_id)
    reasons = db._conn.execute("SELECT paper_id, reason_code FROM paper_events WHERE "
                               "event_type = 'extraction_failed' ORDER BY paper_id").fetchall()
    assert [tuple(r) for r in reasons] == [(p, PS.REASON_MODEL_CALL_FAILED) for p in (1, 2, 3)]
    assert _paper_events(db, 4) == []


def test_t3_a_success_between_failures_resets_the_count(db, spec, fields, fake_models):
    _add_paper(db, 4, fields)
    run_id = open_extraction_run(db, spec)
    outcomes = iter([TimeoutError("a"), None, TimeoutError("b"), TimeoutError("c")])

    def extract(*a, **kw):
        exc = next(outcomes)
        if exc:
            raise exc
        return SimpleNamespace(fields=[])
    with patch.object(E, "extract_paper_with_completeness", side_effect=extract):
        stats = E.run_extraction(db, spec, "flip", restart_every=0, run_id=run_id)
    assert stats["failed"] == 3 and stats["extracted"] == 1


def test_t3_an_aborted_run_closes_as_failed(db, spec, monkeypatch):
    import scripts.run_pipeline as rp
    run_id = open_extraction_run(db, spec)
    monkeypatch.setattr(rp, "load_spec_for", lambda name, path=None: spec)
    monkeypatch.setattr(rp, "ReviewDatabase", lambda name: db)
    monkeypatch.setattr(rp, "_open_run_manifest", lambda d, s, i: run_id)
    monkeypatch.setattr(rp, "is_adjudication_complete", lambda conn: True)
    monkeypatch.setattr(rp, "_stage_parse", lambda *a, **k: {})
    monkeypatch.setattr(rp, "_stage_extract",
                        lambda *a, **k: (_ for _ in ()).throw(X.RunAborted("run aborted: 3")))
    with pytest.raises(X.RunAborted):
        rp.run_pipeline("flip", skip_to="extract")
    conn = sqlite3.connect(db.db_path)
    try:
        assert conn.execute("SELECT end_status FROM run_manifests WHERE run_id = ?",
                            (run_id,)).fetchone()[0] == "failed"
    finally:
        conn.close()


# ── T4 ────────────────────────────────────────────────────────────────
def test_t4_a14_and_input_fit_neither_count_nor_reset(db, spec, fields, fake_models):
    _add_paper(db, 4, fields)
    run_id = open_extraction_run(db, spec)
    sel = E.select_for_extraction(db._conn, arm=spec.extraction_models.arm)
    resolve_parsed_text(db._conn, 2).path.write_text("edited after selection\n")   # A14
    outcomes = iter([TimeoutError("a"),
                     oc.InputTruncated(model="m", count=10, ceiling=10, chars=1),
                     TimeoutError("b")])

    def extract(*a, **kw):
        raise next(outcomes)
    with patch.object(E, "extract_paper_with_completeness", side_effect=extract):
        stats = E.run_extraction(db, spec, "flip", restart_every=0, run_id=run_id,
                                 selection=sel)
    rows = dict(db._conn.execute("SELECT paper_id, reason_code FROM paper_events WHERE "
                                 "event_type = 'extraction_failed'").fetchall())
    assert rows == {1: PS.REASON_MODEL_CALL_FAILED, 2: PS.REASON_PARSED_TEXT_MODIFIED,
                    3: PS.REASON_INPUT_TRUNCATED_AT_CEILING, 4: PS.REASON_MODEL_CALL_FAILED}
    assert stats["failed"] == 3 and stats["skipped_refused"] == 1


def test_t4_the_counting_rule():
    fail = lambda code: X.PaperFailure(1, "a", 1, code, "s")   # noqa: E731
    assert X.counts_toward_abort(fail(PS.REASON_MODEL_CALL_FAILED))
    for code in X.A14_REASONS | {PS.REASON_INPUT_TRUNCATED_AT_CEILING}:
        assert not X.counts_toward_abort(fail(code))
    empty = X.ExtractionRecord(1, "a", 1, "u", None, "m", None, (), ("f",), 3, "s")
    assert X.counts_toward_abort(empty)


# ── T5 ────────────────────────────────────────────────────────────────
def test_t5_zero_spans_become_no_fields_returned(db, spec, fields, fake_models):
    run_id = open_extraction_run(db, spec)
    empty = ExtractionOutput(fields=[]).model_dump_json()
    oc._client._pass2 = empty
    # Three papers in a row with nothing usable is also the abort (R6).
    with pytest.raises(X.RunAborted, match="no_fields_returned"):
        E.run_extraction(db, spec, "flip", restart_every=0, run_id=run_id)
    reasons = {r[0] for r in db._conn.execute(
        "SELECT reason_code FROM paper_events WHERE event_type = 'extraction_failed'")}
    assert reasons == {PS.REASON_NO_FIELDS_RETURNED}


# ── T7 ────────────────────────────────────────────────────────────────
def test_t7_the_thinking_channel_error_lives_in_core():
    from engine.core.extraction_events import MissingThinkingChannelError
    assert E.MissingThinkingChannelError is MissingThinkingChannelError
    src = inspect.getsource(X.outcome_for_exception)
    mods = {n.module for n in ast.walk(ast.parse(src.replace("\n    ", "\n")
                                                     if src.startswith(" ") else src))
            if isinstance(n, ast.ImportFrom)}
    assert not any(m and m.startswith("engine.agents") for m in mods), mods
