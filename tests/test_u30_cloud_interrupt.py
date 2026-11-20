"""U30 with N46 (INT-i12-71, INT-j12-51; R805(d)(1)–(4), R807(b)): a stopped cloud run leaves nothing
half-written and says it was stopped, and the cloud store holds the field names its claims hold.

R805(d): (1) an interrupted cloud run closes `interrupted` with its signal's reason, the local runners'
form; (2) `store_result` rolls back on any exit that did not commit, and `run_arm` rolls back an open
transaction before its close and closes only a run not already closed; (3) `send` records a provider call
an interrupt ends, as an error naming the interrupt, and re-raises; (4) the cloud entry point handles
SIGTERM and SIGHUP as the other mains do. R807(b): `store_result` drops a span whose field the codebook did
not ask for, by `drop_unexpected`, after the completeness check and before the citation check.

Every case drives `scripts/run_cloud_extraction.run_arm` itself on a fresh scratch review (two papers, each
with an eligibility event and one recorded parsed text; the tracked codebook beside it; a spec copy whose
`enabled_arms` names the openai arm), as `12af_U30A/fake_probe_u30a.py` and `12ag_N46A/fake_probe_n46a.py`
drove it. The transport is the real `OpenAIExtractor` with a placeholder key; its module's `openai.OpenAI`
is a fake whose `create` returns a response in the SDK's shape (`ChatCompletion`). An interrupt is raised
in-process at a seam: the extractor's connection is wrapped so that a named statement raises. No signal is
sent to any process, no SDK client is constructed, and `httpx.Client.send` and `socket.connect` are
refusing recorders, asserted untouched.
"""
from __future__ import annotations

import json
import logging
import shutil
import signal
import socket
import sqlite3
import sys
from pathlib import Path

import httpx
import pytest
from openai.types.chat import ChatCompletion

from engine.cloud import openai_extractor as oe
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec
from engine.search.models import Citation
from scripts import run_cloud_extraction as cli
from _event_store_fixture import seed_eligibility
from _parsed_text_fixture import write_parsed

REPO = Path(__file__).resolve().parent.parent
SPEC_PATH = REPO / "review_specs" / "surgical_autonomy.yaml"
CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"
pytestmark = pytest.mark.skipif(not CODEBOOK.exists(), reason="codebook not available")

#: The seam reads each case records (I3): the connection's in_transaction when the interrupt was raised.
SEAMS: dict[str, bool] = {}


def _content(extra=None):
    cbk = load_codebook(CODEBOOK)
    fields = [{"field_name": f["name"], "value": "Original Research" if f["name"] == "study_type" else "NR",
               "source_snippet": "Snippet for study_type." if f["name"] == "study_type" else "",
               "confidence": 0.9, "tier": f["tier"]} for tier in (1, 2, 3, 4) for f in cbk.fields_by_tier(tier)]
    if extra is not None:
        fields.append(extra)
    return json.dumps({"fields": fields})


class SeamConn:
    """Delegates everything to the real connection. On the first statement starting with `trip` it raises
    `exc` — before the statement, or after it when `after` (an UPDATE executed, its commit not reached)."""

    def __init__(self, real, trip, exc, after, seam):
        self.real, self.trip, self.exc, self.after, self.seam, self.tripped = real, trip, exc, after, seam, False

    def execute(self, sql, *a, **k):
        if self.trip and not self.tripped and sql.lstrip().upper().startswith(self.trip):
            self.tripped = True
            if self.after:
                cur = self.real.execute(sql, *a, **k)
            SEAMS[self.seam] = self.real.in_transaction
            raise self.exc
        return self.real.execute(sql, *a, **k)

    def __getattr__(self, name):
        return getattr(self.real, name)


@pytest.fixture
def no_network(monkeypatch):
    rec = {"httpx": 0, "socket": 0}

    def send(self, *a, **k):
        rec["httpx"] += 1
        raise RuntimeError("no network")

    def connect(self, *a, **k):
        rec["socket"] += 1
        raise RuntimeError("no network")
    monkeypatch.setattr(httpx.Client, "send", send)
    monkeypatch.setattr(socket.socket, "connect", connect)
    monkeypatch.setattr(rm, "git_state", lambda *a, **k: rm.GitState(commit="b" * 40, dirty=False, tag=None))
    monkeypatch.setattr(oe.time, "sleep", lambda s: None)
    yield rec
    assert rec == {"httpx": 0, "socket": 0}


@pytest.fixture
def arm_case(tmp_path, monkeypatch, no_network):
    """Returns `make(trip=None, exc=None, after=False, provider=None, content=None)`, which builds the scratch
    review and runs `run_arm` once; it returns (what left run_arm, db_path, arm, paper ids, constructions)."""
    def make(trip=None, exc=None, after=False, provider=None, content=None, seam=None, run=None):
        db = ReviewDatabase("u30b", data_root=tmp_path)
        db.add_papers([Citation(title=f"P{i}", source="pubmed", pmid=str(i)) for i in (1, 2)])
        pids = [r["id"] for r in db.get_papers_by_status("INGESTED")]
        for pid in pids:
            seed_eligibility(db._conn, pid)
            write_parsed(db, pid, f"Paper {pid}.\n\nSnippet for study_type.\n", version=1)
        db._conn.commit(); db_path = Path(db.db_path); db.close()
        shutil.copy2(CODEBOOK, db_path.parent / "extraction_codebook.yaml")
        arm = next(a.name for a in load_review_spec(SPEC_PATH).arms if a.provider == "openai")
        text = SPEC_PATH.read_text()
        assert text.count("enabled_arms: []") == 1
        spec_path = tmp_path / "spec.yaml"
        spec_path.write_text(text.replace("enabled_arms: []", f"enabled_arms: [{arm}]"))
        body = content or _content()
        made = []

        def create(**payload):
            if provider is not None:
                SEAMS[seam or "provider"] = None
                raise provider
            return ChatCompletion(**{"id": "c", "object": "chat.completion", "created": 0, "model": payload["model"],
                                     "choices": [{"index": 0, "finish_reason": "stop",
                                                  "message": {"role": "assistant", "content": body}}],
                                     "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}})

        class FakeOpenAI:
            def __init__(self, **kw):
                made.append(kw)
                self.chat = type("C", (), {"completions": type("CC", (), {"create": staticmethod(create)})()})()

        class Probe(oe.OpenAIExtractor):
            def __init__(self, *a, **k):
                k["api_key"] = "placeholder-not-a-credential"
                super().__init__(*a, **k)
                if trip:
                    self._real_conn = self._conn
                    self._conn = SeamConn(self._conn, trip, exc, after, seam or trip)
                if run is not None:
                    self.run = lambda **kw: run(self)

            def close(self):
                getattr(self, "_real_conn", self._conn).close()

        monkeypatch.setattr(oe.openai, "OpenAI", FakeOpenAI)
        monkeypatch.setitem(cli.EXTRACTORS, "openai", Probe)
        try:
            cli.run_arm(arm, str(db_path), str(spec_path), None, None)
            left = None
        except BaseException as e:  # noqa: BLE001 — the case reports what left run_arm
            left = e
        assert all(kw.get("api_key") == "placeholder-not-a-credential" for kw in made)
        return left, db_path, arm, pids, spec_path
    return make


def _paper(db_path, arm, pid):
    c = sqlite3.connect(db_path)
    try:
        q = lambda s, *a: c.execute(s, a).fetchone()[0]
        return {"cloud_extractions": q("SELECT COUNT(*) FROM cloud_extractions WHERE arm = ? AND paper_id = ?", arm, pid),
                "spans": q("SELECT COUNT(*) FROM cloud_evidence_spans s JOIN cloud_extractions e ON e.id = s.cloud_extraction_id "
                           "WHERE e.arm = ? AND e.paper_id = ?", arm, pid),
                "field_events": q("SELECT COUNT(*) FROM field_events WHERE arm = ? AND paper_id = ?", arm, pid),
                "claim_inputs": q("SELECT COUNT(*) FROM claim_inputs WHERE arm = ? AND paper_id = ?", arm, pid)}
    finally:
        c.close()


def _run_calls(db_path, pid):
    c = sqlite3.connect(db_path)
    try:
        return [tuple(r) for r in c.execute("SELECT outcome, outcome_detail FROM run_calls WHERE paper_id = ? "
                                            "ORDER BY call_id", (pid,))]
    finally:
        c.close()


def _cloud_end(db_path):
    """The cloud run's end (the last extraction manifest; the fixture's eligibility runs come first)."""
    c = sqlite3.connect(db_path)
    try:
        return tuple(c.execute("SELECT end_status, end_reason, ended_at IS NOT NULL FROM run_manifests "
                               "WHERE cloud_arms_json != '[]' ORDER BY run_id DESC LIMIT 1").fetchone())
    finally:
        c.close()


def _pending(db_path, spec_path, arm, monkeypatch):
    monkeypatch.setattr(oe.openai, "OpenAI", lambda **k: None)
    base = cli.CloudExtractorBase(str(db_path), str(spec_path))
    try:
        return [p["paper_id"] for p in base.get_pending_papers(arm)]
    finally:
        base.close()


NOTHING = {"cloud_extractions": 0, "spans": 0, "field_events": 0, "claim_inputs": 0}


# ── 1–4: an interrupt inside the store or the provider call ────────────────


def test_1_an_interrupt_between_the_extraction_and_span_inserts_leaves_nothing(arm_case, monkeypatch):
    """Case 1. Seam: the first `INSERT INTO cloud_evidence_spans` raises KeyboardInterrupt (after the
    extraction INSERT, Q2 row 7). No row of the paper in the four stores; the paper still pending; the run
    closed interrupted, reason interrupt:SIGINT; the paper's committed run_calls row still there (E6)."""
    left, db_path, arm, pids, spec_path = arm_case(trip="INSERT INTO CLOUD_EVIDENCE_SPANS", exc=KeyboardInterrupt(),
                                                  seam="row7_span_insert")
    assert isinstance(left, KeyboardInterrupt)
    assert _paper(db_path, arm, pids[0]) == NOTHING
    assert pids[0] in _pending(db_path, spec_path, arm, monkeypatch)
    assert _cloud_end(db_path) == ("interrupted", "interrupt:SIGINT", 1)
    assert _run_calls(db_path, pids[0]) == [("completed", None)]


def test_2_an_interrupt_inside_the_event_writers_savepoint_leaves_nothing(arm_case, monkeypatch):
    """Case 2. Seam: the first `INSERT INTO field_events` raises KeyboardInterrupt (inside the event writer's
    savepoint, Q2 row 9). The same four things as case 1."""
    left, db_path, arm, pids, spec_path = arm_case(trip="INSERT INTO FIELD_EVENTS", exc=KeyboardInterrupt(),
                                                  seam="row9_field_events")
    assert isinstance(left, KeyboardInterrupt)
    assert _paper(db_path, arm, pids[0]) == NOTHING
    assert pids[0] in _pending(db_path, spec_path, arm, monkeypatch)
    assert _cloud_end(db_path) == ("interrupted", "interrupt:SIGINT", 1)
    assert _run_calls(db_path, pids[0]) == [("completed", None)]


def test_3_an_interrupt_inside_the_provider_call_is_recorded(arm_case, monkeypatch):
    """Case 3. Seam: the fake SDK's `create` raises KeyboardInterrupt (Q2 row 2). One run_calls row for the
    call, error, its detail naming the interrupt in send's `Class: message` form (R800(b), amended by
    R805(d)(3)); the paper pending; the run closed interrupted."""
    left, db_path, arm, pids, spec_path = arm_case(provider=KeyboardInterrupt(), seam="row2_provider")
    assert isinstance(left, KeyboardInterrupt)
    rows = _run_calls(db_path, pids[0])
    assert len(rows) == 1, rows
    assert rows[0][0] == "error" and rows[0][1].startswith("KeyboardInterrupt")
    assert _paper(db_path, arm, pids[0]) == NOTHING
    assert pids[0] in _pending(db_path, spec_path, arm, monkeypatch)
    assert _cloud_end(db_path) == ("interrupted", "interrupt:SIGINT", 1)


@pytest.mark.parametrize("signum, reason", [(signal.SIGTERM, "interrupt:SIGTERM"), (signal.SIGHUP, "interrupt:SIGHUP")],
                         ids=["SIGTERM", "SIGHUP"])
def test_4_a_signal_interrupt_closes_interrupted_with_its_reason(arm_case, monkeypatch, signum, reason):
    """Case 4. Seam as case 1, the exception `rm.RunInterrupted(signum)` (what `interrupt_signals`' handler
    raises; no signal is sent). Closed interrupted with the signal's reason; nothing of the paper left."""
    left, db_path, arm, pids, spec_path = arm_case(trip="INSERT INTO CLOUD_EVIDENCE_SPANS", exc=rm.RunInterrupted(signum),
                                                  seam=f"row7_{signal.Signals(signum).name}")
    assert isinstance(left, rm.RunInterrupted)
    assert _cloud_end(db_path) == ("interrupted", reason, 1)
    assert _paper(db_path, arm, pids[0]) == NOTHING


# ── 5: the entry point's signals ───────────────────────────────────────────


@pytest.mark.parametrize("signum", [signal.SIGTERM, signal.SIGHUP], ids=["SIGTERM", "SIGHUP"])
def test_5_the_cloud_main_handles_sigterm_and_sighup_as_the_local_main_does(monkeypatch, signum):
    """Case 5. Seam: `cli.run_arm` replaced by a function that reads `signal.getsignal` for both signals and
    calls each handler directly (no signal is sent), then raises `rm.RunInterrupted(signum)`; the spec read,
    the arm selection and `maybe_background` replaced. Inside main's call of run_arm both handlers raise
    `RunInterrupted` for their signal, as `interrupt_signals` installs them; main ends returning
    128 + signum, as `scripts/run_pipeline.py`'s main does; the prior handlers are restored after."""
    import engine.utils.background as bg
    seen = {}
    before = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGHUP)}

    def fake_run_arm(*a, **k):
        for s in (signal.SIGTERM, signal.SIGHUP):
            handler = signal.getsignal(s)
            seen[s] = handler
            assert callable(handler), f"{signal.Signals(s).name}: {handler!r} is not a handler"
            with pytest.raises(rm.RunInterrupted) as caught:
                handler(s, None)
            assert caught.value.signum == s
        raise rm.RunInterrupted(signum)
    monkeypatch.setattr(cli, "run_arm", fake_run_arm)
    monkeypatch.setattr(cli, "select_cloud_arms", lambda spec, arms: ["some_arm"])
    monkeypatch.setattr(cli, "load_spec_for", lambda review, path=None: object())
    monkeypatch.setattr(bg, "maybe_background", lambda *a, **k: None)
    monkeypatch.setattr(sys, "argv", ["run_cloud_extraction.py", "--review", "x", "--arm", "some_arm",
                                      "--db", "/nonexistent/review.db", "--spec", "/nonexistent/spec.yaml"])
    assert cli.main() == 128 + signum
    assert set(seen) == {signal.SIGTERM, signal.SIGHUP}
    assert {s: signal.getsignal(s) for s in before} == before


# ── 6–8: the close ─────────────────────────────────────────────────────────


def test_6_an_interrupt_between_the_closes_update_and_its_commit_does_not_leave_the_run_open(arm_case):
    """Case 6. Seam: the run returns at once; the first `UPDATE run_manifests` (the 'completed' close)
    executes and then raises KeyboardInterrupt before its commit (Q2 row 12). The run is not left open: it
    reads interrupted with interrupt:SIGINT."""
    left, db_path, arm, pids, spec_path = arm_case(trip="UPDATE RUN_MANIFESTS", exc=KeyboardInterrupt(), after=True,
                                                  seam="row12_close_update", run=lambda ex: None)
    assert isinstance(left, KeyboardInterrupt)
    assert _cloud_end(db_path) == ("interrupted", "interrupt:SIGINT", 1)


def _store_first_paper(ex):
    """The run's body for case 7: `store_result` called for the first pending paper with the codebook's spans.
    Called directly, so an Exception from the store reaches run_arm (the transport's loop catches one and
    marks the paper failed, so through `run` it never would)."""
    pid = ex.get_pending_papers(ex.arm_name)[0]["paper_id"]
    spans = ex.parse_response_to_spans(_content())
    ex.store_result(paper_id=pid, arm=ex.arm_name, model_string="m", extracted_data={}, reasoning_trace="",
                    prompt_text="", input_tokens=0, output_tokens=0, reasoning_tokens=0, cost_usd=0.0, spans=spans)


def test_7_an_exception_from_the_store_closes_failed_and_commits_nothing_of_the_paper(arm_case):
    """Case 7. Seam: the run stores its first paper by `store_result` directly, and the first
    `INSERT INTO cloud_evidence_spans` raises RuntimeError (after the extraction INSERT). The run closes failed
    with "<Class>: <message>", as now, and no row of the paper is committed by the close."""
    left, db_path, arm, pids, spec_path = arm_case(trip="INSERT INTO CLOUD_EVIDENCE_SPANS", exc=RuntimeError("disk said no"),
                                                  seam="store_exception", run=_store_first_paper)
    assert isinstance(left, RuntimeError)
    assert _cloud_end(db_path) == ("failed", "RuntimeError: disk said no", 1)
    assert _paper(db_path, arm, pids[0]) == NOTHING


def test_8_a_run_that_returns_closes_completed_with_no_reason(arm_case):
    """Case 8. Seam: none; both papers stored. Completed, no reason, as now."""
    left, db_path, arm, pids, spec_path = arm_case()
    assert left is None
    assert _cloud_end(db_path) == ("completed", None, 1)
    assert all(_paper(db_path, arm, p)["cloud_extractions"] == 1 for p in pids)


# ── 9–10: a field the codebook did not ask for (N46, R807(b)) ──────────────


def _span_names(db_path, arm, pid):
    c = sqlite3.connect(db_path)
    try:
        return sorted(r[0] for r in c.execute(
            "SELECT s.field_name FROM cloud_evidence_spans s JOIN cloud_extractions e ON e.id = s.cloud_extraction_id "
            "WHERE e.arm = ? AND e.paper_id = ?", (arm, pid)))
    finally:
        c.close()


def test_9_a_cited_field_outside_the_codebook_is_dropped_before_any_write(arm_case, caplog):
    """Case 9. Seam: the response carries the codebook's fields and one `invented_field`, cited. The spans
    table holds exactly the codebook's names; the claims are the codebook's and no other; `extracted_data`
    still carries the name (R807(b): it stays the response as received); the "dropped (R118)" line is logged."""
    extra = {"field_name": "invented_field", "value": "something", "source_snippet": "Snippet for study_type.",
             "confidence": 0.9, "tier": 1}
    with caplog.at_level(logging.WARNING):
        left, db_path, arm, pids, spec_path = arm_case(content=_content(extra))
    assert left is None
    names = sorted(f["name"] for f in load_codebook(CODEBOOK).fields)
    assert _span_names(db_path, arm, pids[0]) == names
    c = sqlite3.connect(db_path)
    try:
        claimed = sorted({r[0] for r in c.execute("SELECT field_name FROM field_events WHERE arm = ? AND paper_id = ?",
                                                  (arm, pids[0]))})
        data = c.execute("SELECT extracted_data FROM cloud_extractions WHERE arm = ? AND paper_id = ?",
                         (arm, pids[0])).fetchone()[0]
    finally:
        c.close()
    assert claimed == names
    assert "invented_field" in data
    assert "dropped (R118)" in caplog.text


def test_10_an_uncited_field_outside_the_codebook_no_longer_costs_the_paper(arm_case):
    """Case 10. Seam as case 9, the invented field uncited (an empty snippet): the paper stores."""
    extra = {"field_name": "invented_field", "value": "something", "source_snippet": "", "confidence": 0.9, "tier": 1}
    left, db_path, arm, pids, spec_path = arm_case(content=_content(extra))
    assert left is None
    assert _paper(db_path, arm, pids[0])["cloud_extractions"] == 1
    assert "invented_field" not in _span_names(db_path, arm, pids[0])
