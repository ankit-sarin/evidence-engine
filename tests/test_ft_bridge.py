"""STAGED-ENTRY-01 11b — the eligibility bridge (R258–R262, 11b-BRIDGE-R1).

The FT screener runs inside one `screening` manifest per invocation and writes
model-actor eligibility events: the primary's exclude is `screened` →
`full_text_out`, the verifier's confirm is `verified` → `eligible`. Every decided
paper is one transaction. Include, flag, no-text and malformed write no event.

No test reaches a model server or git: the Ollama client is replaced by a fake
that answers `show` and `chat` (so both input-fit guards run for real, as in
`test_request_capture`), the manifest is opened with a fixed git state and
digest function, and the preflight's environment and `ps` probes are patched.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from engine.adjudication.workflow import ensure_workflow_table
from engine.agents import ft_screener as ft
from engine.core import run_manifest as rm
from engine.core.database import ReviewDatabase
from engine.core.review_paths import load_spec_for
from engine.exporters.prisma import validate_prisma_counts
from engine.search.models import Citation
from engine.utils import ollama_client as oc
from _parsed_text_fixture import write_parsed

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"

pytestmark = pytest.mark.skipif(not LIVE_CODEBOOK.exists(), reason="codebook not available")

CLEAN = rm.GitState(commit="b" * 40, dirty=False, tag=None)
DIRTY = rm.GitState(commit="b" * 40, dirty=True, tag=None)
DIGESTS = {"qwen3:32b": "1" * 64, "gemma3:27b": "2" * 64}
PRIMARY, VERIFIER = "ft_screen_primary", "ft_screen_verifier"


# ── Fixtures ────────────────────────────────────────────────────────


class FakeClient:
    """`ollama.Client` stand-in. `answers[model]` is a list of FT responses
    consumed in order; a call without `format` is the preflight probe."""

    def __init__(self):
        self.calls: list[dict] = []
        self.answers: dict[str, list] = {"qwen3:32b": [], "gemma3:27b": []}
        self._client = SimpleNamespace(base_url="http://bridge.invalid")

    def show(self, model):
        return SimpleNamespace(modelinfo={"general.context_length": 131_072})

    def chat(self, **kwargs):
        self.calls.append(kwargs)
        if "format" not in kwargs:
            content = "ok"
        else:
            answer = self.answers[kwargs["model"]].pop(0)
            content = answer if isinstance(answer, str) else json.dumps(answer)
        chars = oc.message_chars(kwargs.get("messages"))
        return SimpleNamespace(
            message=SimpleNamespace(content=content, thinking=None),
            prompt_eval_count=max(1, int(chars * 0.3)), done_reason="stop", eval_count=10)

    def ft_calls(self):
        return [c for c in self.calls if "format" in c]


@pytest.fixture(autouse=True)
def _no_real_http(monkeypatch):
    import httpx

    def _refuse(*a, **kw):
        raise AssertionError("a bridge test attempted a real HTTP request")
    monkeypatch.setattr(httpx.Client, "send", _refuse)
    monkeypatch.setattr(httpx.AsyncClient, "send", _refuse)
    oc.clear_ceiling_cache()
    yield
    oc.clear_ceiling_cache()


@pytest.fixture
def fake(monkeypatch):
    from engine.utils import ollama_preflight as pf
    client = FakeClient()
    monkeypatch.setattr(oc, "_client", client)
    monkeypatch.setattr(pf, "check_ollama_env", lambda: None)
    monkeypatch.setattr(pf.ollama, "ps", lambda: {"models": []})
    return client


@pytest.fixture(scope="module")
def spec():
    return load_spec_for("surgical_autonomy")


@pytest.fixture
def db(tmp_path):
    d = ReviewDatabase("ft_bridge", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(d.db_path).parent / "extraction_codebook.yaml")
    yield d
    d.close()


def _paper(db, n, *, text=True):
    """A paper at PARSED, with recorded parsed text unless `text` is False."""
    db.add_papers([Citation(
        title=f"Paper {n}", abstract="Autonomous suturing abstract.", pmid=f"BR{n}",
        source="pubmed", authors=["A"], journal="J", year=2024)])
    pid = db._conn.execute("SELECT id FROM papers WHERE pmid = ?", (f"BR{n}",)).fetchone()[0]
    for s in ("ABSTRACT_SCREENED_IN", "PDF_ACQUIRED", "PARSED"):
        db.update_status(pid, s)
    if text:
        write_parsed(db, pid, "# Paper\n\nAutonomous robotic suturing in surgery. " * 40)
    return pid


def _at_ft_eligible(db, n):
    """A primary include without a verification decision (verification pending)."""
    pid = _paper(db, n)
    db.update_status(pid, "FT_ELIGIBLE")
    return pid


def _include():
    return {"decision": "FT_ELIGIBLE", "reason_code": "eligible", "rationale": "r",
            "confidence": 0.9}


def _exclude():
    return {"decision": "FT_EXCLUDE", "reason_code": "wrong_specialty", "rationale": "r",
            "confidence": 0.9}


def _confirm():
    return {"decision": "FT_ELIGIBLE", "rationale": "r", "confidence": 0.9}


def _flag():
    return {"decision": "FT_FLAGGED", "rationale": "r", "confidence": 0.9}


def _invoke(db, spec, *, git=CLEAN, **kw):
    return ft.run_ft_invocation(db, spec, git=git, digest_fn=DIGESTS.__getitem__, **kw)


def _one(db, sql, *args):
    return db._conn.execute(sql, args).fetchone()


def _events(db, pid=None):
    sql = "SELECT * FROM paper_events" + (" WHERE paper_id = ?" if pid else "")
    return db._conn.execute(sql, (pid,) if pid else ()).fetchall()


def _status(db, pid):
    return _one(db, "SELECT status FROM papers WHERE id = ?", pid)[0]


def _count(db, table, pid):
    return _one(db, f"SELECT COUNT(*) FROM {table} WHERE paper_id = ?", pid)[0]


def _stage_rows(db, run_id):
    return sorted(r[0] for r in db._conn.execute(
        "SELECT stage FROM run_stage_configs WHERE run_id = ?", (run_id,)))


# ── 1, 2 — the manifest ─────────────────────────────────────────────


def test_1_default_run_manifest_is_screening_with_four_stage_rows(db, spec, fake):
    run_id = _invoke(db, spec)
    m = _one(db, "SELECT run_kind AS kind, end_status, end_reason, ended_at FROM run_manifests "
                 "WHERE run_id = ?", run_id)
    assert m["kind"] == "screening"
    assert m["end_status"] == "completed" and m["end_reason"] is None
    assert m["ended_at"] is not None
    assert _stage_rows(db, run_id) == sorted(
        [PRIMARY, VERIFIER, "preflight:gemma3:27b", "preflight:qwen3:32b"])


def test_1_verify_only_manifest_declares_the_two_ft_stages(db, spec, fake):
    run_id = _invoke(db, spec, verify_only=True)
    assert _stage_rows(db, run_id) == [PRIMARY, VERIFIER]
    assert _one(db, "SELECT end_status FROM run_manifests WHERE run_id = ?",
                run_id)[0] == "completed"
    assert fake.calls == []          # no preflight on the verifier path


def test_2_preflight_calls_carry_paper_id_null_and_satisfy_the_fk(db, spec, fake):
    run_id = _invoke(db, spec)
    rows = db._conn.execute(
        "SELECT stage, paper_id, outcome FROM run_calls WHERE run_id = ? ORDER BY stage",
        (run_id,)).fetchall()
    assert [tuple(r) for r in rows] == [
        ("preflight:gemma3:27b", None, "completed"),
        ("preflight:qwen3:32b", None, "completed")]
    assert db._conn.execute("PRAGMA foreign_key_check(run_calls)").fetchall() == []


# ── 3, 4, 13 — the two events ───────────────────────────────────────


def _assert_event(db, ev, *, run_id, stage, model, event_type, to_state):
    assert ev["event_type"] == event_type and ev["to_state"] == to_state
    assert ev["actor_kind"] == "model" and ev["actor_role"] == "reviewer"
    assert ev["actor_name"] == model
    assert ev["actor_digest"] == DIGESTS[model] == rm.stage_digest(db._conn, run_id, stage)
    assert ev["run_id"] == run_id and ev["run_marker"] is None
    assert ev["stage_name"] == stage
    assert ev["reason"] is None and ev["reason_code"] is None
    assert ev["payload_json"] == "{}"                      # R-P1
    assert ev["from_state"] is None and ev["prior_event_id"] is None   # R-E1
    call = db._conn.execute(
        "SELECT request_hash FROM run_calls WHERE run_id = ? AND stage = ?",
        (run_id, stage)).fetchall()
    assert len(call) == 1                                  # 13: one call per decided paper
    assert ev["presented_context_sha256"] == call[0][0]    # 13: linked by hash (R261, R-C31)


def test_3_primary_exclude_writes_one_screened_full_text_out_event(db, spec, fake):
    pid = _paper(db, 1)
    fake.answers["qwen3:32b"] = [_exclude()]
    run_id = _invoke(db, spec, screen_only=True)
    evs = _events(db, pid)
    assert len(evs) == 1
    _assert_event(db, evs[0], run_id=run_id, stage=PRIMARY, model="qwen3:32b",
                  event_type="screened", to_state="full_text_out")
    assert _status(db, pid) == "FT_SCREENED_OUT"
    assert _count(db, "ft_screening_decisions", pid) == 1


def test_4_verifier_confirm_writes_one_verified_eligible_event(db, spec, fake):
    pid = _at_ft_eligible(db, 1)
    fake.answers["gemma3:27b"] = [_confirm()]
    run_id = _invoke(db, spec, verify_only=True)
    evs = _events(db, pid)
    assert len(evs) == 1
    _assert_event(db, evs[0], run_id=run_id, stage=VERIFIER, model="gemma3:27b",
                  event_type="verified", to_state="eligible")
    assert _status(db, pid) == "FT_ELIGIBLE"
    assert _count(db, "ft_verification_decisions", pid) == 1


def test_4_include_then_confirm_in_one_default_invocation(db, spec, fake):
    pid = _paper(db, 1)
    fake.answers["qwen3:32b"] = [_include()]
    fake.answers["gemma3:27b"] = [_confirm()]
    run_id = _invoke(db, spec)
    evs = _events(db, pid)
    assert [(e["event_type"], e["to_state"], e["run_id"]) for e in evs] == [
        ("verified", "eligible", run_id)]


# ── 5 — the paths that write no event ───────────────────────────────


def test_5_primary_include_writes_no_event(db, spec, fake):
    pid = _paper(db, 1)
    fake.answers["qwen3:32b"] = [_include()]
    _invoke(db, spec, screen_only=True)
    assert _status(db, pid) == "FT_ELIGIBLE" and _events(db) == []


def test_5_primary_no_text_and_malformed_write_no_event(db, spec, fake):
    no_text = _paper(db, 1, text=False)
    bad = _paper(db, 2)
    fake.answers["qwen3:32b"] = ["{not json"]
    _invoke(db, spec, screen_only=True)
    assert _status(db, no_text) == _status(db, bad) == "FT_FLAGGED"
    assert _events(db) == []
    assert _count(db, "ft_screening_decisions", no_text) == 0
    assert _count(db, "ft_screening_decisions", bad) == 0


def test_5_verifier_flag_no_text_and_malformed_write_no_event(db, spec, fake):
    flagged = _at_ft_eligible(db, 1)
    bad = _at_ft_eligible(db, 2)
    no_text = _paper(db, 3, text=False)
    db.update_status(no_text, "FT_ELIGIBLE")
    fake.answers["gemma3:27b"] = [_flag(), "{not json"]
    _invoke(db, spec, verify_only=True)
    for pid in (flagged, bad, no_text):
        assert _status(db, pid) == "FT_FLAGGED"
    assert _events(db) == []
    assert _count(db, "ft_verification_decisions", flagged) == 1
    assert _count(db, "ft_verification_decisions", bad) == 0
    assert _count(db, "ft_verification_decisions", no_text) == 0


# ── 6 — one transaction per decided paper ───────────────────────────


def _snapshot(db, pid):
    return (_count(db, "ft_screening_decisions", pid),
            _count(db, "ft_verification_decisions", pid),
            _status(db, pid), len(_events(db, pid)))


def _last_run_status(db):
    return _one(db, "SELECT end_status FROM run_manifests ORDER BY run_id DESC LIMIT 1")[0]


def test_6_a_refused_status_write_leaves_nothing_for_the_paper(db, spec, fake, monkeypatch):
    pid = _paper(db, 1)
    before = _snapshot(db, pid)
    fake.answers["qwen3:32b"] = [_exclude()]

    def refuse(paper_id, new_status):
        raise ValueError(f"Invalid transition: PARSED → {new_status}")
    monkeypatch.setattr(db, "update_status", refuse)
    with pytest.raises(ValueError, match="Invalid transition"):
        _invoke(db, spec, screen_only=True)
    assert _snapshot(db, pid) == before
    assert _last_run_status(db) == "failed"
    assert not db._conn.in_transaction


def test_6_an_exception_at_the_primary_event_write_leaves_nothing(db, spec, fake, monkeypatch):
    pid = _paper(db, 1)
    before = _snapshot(db, pid)
    fake.answers["qwen3:32b"] = [_exclude()]

    def boom(*a, **kw):
        raise RuntimeError("event write failed")
    monkeypatch.setattr(ft, "write_paper_event", boom)
    with pytest.raises(RuntimeError, match="event write failed"):
        _invoke(db, spec, screen_only=True)
    assert _snapshot(db, pid) == before == (0, 0, "PARSED", 0)
    assert _last_run_status(db) == "failed"


def test_6_an_exception_at_the_verifier_event_write_leaves_nothing(db, spec, fake, monkeypatch):
    pid = _at_ft_eligible(db, 1)
    before = _snapshot(db, pid)
    fake.answers["gemma3:27b"] = [_confirm()]

    def boom(*a, **kw):
        raise RuntimeError("event write failed")
    monkeypatch.setattr(ft, "write_paper_event", boom)
    with pytest.raises(RuntimeError):
        _invoke(db, spec, verify_only=True)
    assert _snapshot(db, pid) == before == (0, 0, "FT_ELIGIBLE", 0)
    assert _last_run_status(db) == "failed"


def test_6_a_refused_decision_row_leaves_nothing(db, spec, fake):
    pid = _paper(db, 1)
    before = _snapshot(db, pid)
    fake.answers["qwen3:32b"] = [{**_exclude(), "reason_code": "not_a_code"}]
    with pytest.raises(ValueError, match="reason-code"):
        _invoke(db, spec, screen_only=True)
    assert _snapshot(db, pid) == before
    assert _last_run_status(db) == "failed"


# ── 7 — a dirty tree refuses before any call ────────────────────────


def test_7_dirty_tree_refuses_before_any_model_call_and_writes_nothing(db, spec, fake):
    pid = _paper(db, 1)
    fake.answers["qwen3:32b"] = [_exclude()]
    with pytest.raises(rm.DirtyTree):
        _invoke(db, spec, git=DIRTY)
    assert fake.calls == []
    for table in ("run_manifests", "run_stage_configs", "run_calls",
                  "ft_screening_decisions", "paper_events"):
        assert _one(db, f"SELECT COUNT(*) FROM {table}")[0] == 0
    assert _status(db, pid) == "PARSED"


# ── 8, 9 — resume and verifier selection (R-V1) ─────────────────────


def test_8_a_resumed_run_is_a_new_manifest_and_no_paper_gets_a_second_event(
        db, spec, fake, monkeypatch):
    a, b = _at_ft_eligible(db, 1), _at_ft_eligible(db, 2)
    fake.answers["gemma3:27b"] = [_confirm(), _confirm()]
    real = ft._write_eligibility_event
    seen = []

    def crash_on_second(*args, **kw):
        seen.append(kw["paper_id"])
        if len(seen) == 2:
            raise RuntimeError("crash mid-verification")
        return real(*args, **kw)
    monkeypatch.setattr(ft, "_write_eligibility_event", crash_on_second)
    with pytest.raises(RuntimeError):
        _invoke(db, spec, verify_only=True)
    first = _one(db, "SELECT MAX(run_id) FROM run_manifests")[0]
    monkeypatch.setattr(ft, "_write_eligibility_event", real)

    fake.answers["gemma3:27b"] = [_confirm()]
    second = _invoke(db, spec, verify_only=True)
    assert second != first
    assert len(fake.ft_calls()) == 3          # a once, b twice (the crashed attempt rolled back)
    for pid in (a, b):
        assert len(_events(db, pid)) == 1
        assert _count(db, "ft_verification_decisions", pid) == 1


def test_9_i_after_a_completed_confirm_a_fresh_verify_only_selects_nothing(db, spec, fake):
    _at_ft_eligible(db, 1)
    fake.answers["gemma3:27b"] = [_confirm()]
    _invoke(db, spec, verify_only=True)
    n = len(fake.calls)
    run_id = _invoke(db, spec, verify_only=True)
    assert len(fake.calls) == n
    assert _one(db, "SELECT COUNT(*) FROM run_calls WHERE run_id = ?", run_id)[0] == 0


def test_9_ii_a_decided_paper_without_an_event_is_not_selected(db, spec, fake):
    """The adjudicated-after-verifier-flag shape: FT_ELIGIBLE, a verification
    decision row, no eligible event."""
    pid = _at_ft_eligible(db, 1)
    db.add_ft_verification_decision(pid, "gemma3:27b", "FT_FLAGGED", "r", 0.9)
    _invoke(db, spec, verify_only=True)
    assert fake.calls == [] and _events(db) == []


def test_9_iii_a_primary_include_with_neither_is_selected(db, spec, fake):
    pid = _at_ft_eligible(db, 1)
    fake.answers["gemma3:27b"] = [_confirm()]
    _invoke(db, spec, verify_only=True)
    assert len(fake.ft_calls()) == 1 and len(_events(db, pid)) == 1


# ── 10 — the seam after the bridge ──────────────────────────────────


def test_10_seam_include_is_pending_confirm_is_eligible_exclude_is_valid(db, spec, fake):
    inc, exc = _paper(db, 1), _paper(db, 2)
    fake.answers["qwen3:32b"] = [_include(), _exclude()]
    _invoke(db, spec, screen_only=True)
    v = validate_prisma_counts(db)
    assert v["valid"] and v["verification_pending"] == 1, v

    fake.answers["gemma3:27b"] = [_confirm()]
    _invoke(db, spec, verify_only=True)
    v = validate_prisma_counts(db)
    assert v["valid"] and v["verification_pending"] == 0, v
    from engine.core.effective import eligible_paper_ids
    assert eligible_paper_ids(db._conn) == (inc,)
    assert _status(db, exc) == "FT_SCREENED_OUT"


# ── 11, 12 — the writers' and run functions' signatures ─────────────


def test_11_decision_writers_commit_by_default(db, spec):
    pid = _paper(db, 1)
    db.add_ft_screening_decision(pid, "m", "FT_ELIGIBLE", "eligible", "r", 0.9,
                                 reason_codes={"eligible"})
    assert not db._conn.in_transaction
    db.add_ft_verification_decision(pid, "m", "FT_ELIGIBLE", "r", 0.9)
    assert not db._conn.in_transaction
    other = sqlite3.connect(db.db_path)
    try:
        for table in ("ft_screening_decisions", "ft_verification_decisions"):
            assert other.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 1
    finally:
        other.close()


def test_11_commit_false_leaves_the_row_in_the_callers_transaction(db, spec):
    pid = _paper(db, 1)
    db.add_ft_screening_decision(pid, "m", "FT_ELIGIBLE", "eligible", "r", 0.9,
                                 reason_codes={"eligible"}, commit=False)
    db.add_ft_verification_decision(pid, "m", "FT_ELIGIBLE", "r", 0.9, commit=False)
    assert db._conn.in_transaction
    db._conn.rollback()
    assert _count(db, "ft_screening_decisions", pid) == 0
    assert _count(db, "ft_verification_decisions", pid) == 0


@pytest.mark.parametrize("fn", [ft.run_ft_screening, ft.run_ft_verification])
def test_12_run_id_is_a_required_keyword(db, spec, fn):
    with pytest.raises(TypeError, match="run_id"):
        fn(db, spec)


# ── 14 — workflow_state (R-W1) ──────────────────────────────────────


def _workflow(db):
    return db._conn.execute(
        "SELECT * FROM workflow_state ORDER BY stage_name").fetchall()


def test_14_a_zero_decision_run_leaves_workflow_state_byte_identical(db, spec, fake):
    ensure_workflow_table(db._conn)
    db._conn.execute("UPDATE workflow_state SET status = 'complete', completed_at = "
                     "'2026-03-14T23:55:25+00:00', metadata = '143 confirmed, 36 flagged' "
                     "WHERE stage_name = 'FULL_TEXT_SCREENING_COMPLETE'")
    db._conn.commit()
    no_text = _paper(db, 1, text=False)
    db.update_status(no_text, "FT_ELIGIBLE")   # flagged with no decision row
    before = [tuple(r) for r in _workflow(db)]
    _invoke(db, spec)
    _invoke(db, spec, verify_only=True)
    assert [tuple(r) for r in _workflow(db)] == before
    assert _status(db, no_text) == "FT_FLAGGED"


def test_14_a_deciding_run_completes_the_stage(db, spec, fake):
    ensure_workflow_table(db._conn)
    _at_ft_eligible(db, 1)
    fake.answers["gemma3:27b"] = [_confirm()]
    _invoke(db, spec, verify_only=True)
    row = _one(db, "SELECT status, metadata FROM workflow_state "
                   "WHERE stage_name = 'FULL_TEXT_SCREENING_COMPLETE'")
    assert tuple(row) == ("complete", "1 confirmed, 0 flagged")
