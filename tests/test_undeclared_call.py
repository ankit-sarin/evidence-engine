"""C54 — `ollama_chat` refuses, before sending, a call the active run's manifest
did not declare (12e-C54-R2 / R3).

The manifest is the contract: under an active run a call whose stage key has no
`run_stage_configs` row, or whose row names a different model, raises
`UndeclaredCall` before the input-fit read and the send — the fake client
receives nothing and no `run_calls` row is written. Outside a run there is no
check. Inside the extraction loop the refusal is a run fault: it propagates,
writes no paper event and is not a count toward the consecutive-failure abort.

Every database is a scratch `ReviewDatabase` with a real manifest; Ollama is a
fake client at `ollama_client._client`. No model is called.
"""

from __future__ import annotations

import shutil
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from engine.agents import extractor as E
from engine.core import effective_config as ec
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.review_paths import load_spec_for
from engine.utils import ollama_client as oc
from _event_store_fixture import open_extraction_run, seed_eligibility
from _parsed_text_fixture import write_parsed

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"

pytestmark = pytest.mark.skipif(not LIVE_CODEBOOK.exists(), reason="codebook not available")

CLEAN = rm.GitState(commit="b" * 40, dirty=False, tag=None)
DIGEST = "a" * 64
OTHER_MODEL = "undeclared-model:1b"
MESSAGES = [{"role": "user", "content": "hello"}]


class FakeClient:
    """Stands in for `ollama.Client`: records every request it is handed, the
    `show` read included — a refused call must reach neither."""

    def __init__(self):
        self.calls: list[dict] = []
        self.shows: list[str] = []
        self._client = SimpleNamespace(base_url="http://undeclared.invalid")

    def show(self, model):
        self.shows.append(model)
        return SimpleNamespace(modelinfo={"general.context_length": 131_072})

    def chat(self, **kwargs):
        self.calls.append(kwargs)
        chars = oc.message_chars(kwargs.get("messages"))
        return SimpleNamespace(
            message=SimpleNamespace(content="OK", thinking="Reasoning."),
            prompt_eval_count=max(1, int(chars * 0.3)), done_reason="stop", eval_count=1)

    @property
    def requests(self) -> int:
        return len(self.calls) + len(self.shows)


@pytest.fixture
def fake(monkeypatch):
    import httpx
    monkeypatch.setattr(httpx.Client, "send",
                        lambda *a, **k: pytest.fail("a C54 test made a real HTTP request"))
    client = FakeClient()
    monkeypatch.setattr(oc, "_client", client)
    oc.clear_ceiling_cache()
    yield client
    oc.clear_ceiling_cache()


@pytest.fixture(scope="module")
def spec():
    return load_spec_for("surgical_autonomy")


@pytest.fixture
def review(tmp_path):
    db = ReviewDatabase("undeclared_call", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(db.db_path).parent / "extraction_codebook.yaml")
    cb = load_codebook(Path(db.db_path).parent / "extraction_codebook.yaml")
    yield db, cb
    db.close()


def _open(db, spec, cb, stages, preflight_models=()):
    return rm.open_run(db._conn, spec, kind="screening", stages=stages, codebook=cb,
                       preflight_models=preflight_models, git=CLEAN,
                       digest_fn=lambda m: DIGEST).run_id


def _run_calls(db):
    return [tuple(r) for r in db._conn.execute(
        "SELECT stage, outcome, request_hash FROM run_calls ORDER BY call_id")]


# ── (1) a declared stage, a differing model ───────────────────────────
def test_1_a_declared_stage_with_a_differing_model_is_refused_before_the_send(
        review, spec, fake):
    db, cb = review
    run_id = _open(db, spec, cb, ("audit",))
    cfg = ec.stage_config("audit", spec)
    kwargs = {**cfg.kwargs(), "model": OTHER_MODEL}
    with rm.active_run(db._conn, run_id):
        with pytest.raises(rm.UndeclaredCall) as exc:
            oc.ollama_chat(messages=MESSAGES, **kwargs)
    assert (exc.value.stage_key, exc.value.declared_model, exc.value.requested_model) == \
        ("audit", cfg.model, OTHER_MODEL)
    assert fake.requests == 0
    assert _run_calls(db) == []


# ── (2) an undeclared stage key ───────────────────────────────────────
def test_2_an_undeclared_stage_is_refused_before_the_send(review, spec, fake):
    db, cb = review
    run_id = _open(db, spec, cb, ("audit",))
    cfg = ec.stage_config("vision_parse", spec)
    with rm.active_run(db._conn, run_id):
        with pytest.raises(rm.UndeclaredCall) as exc:
            oc.ollama_chat(messages=MESSAGES, **cfg.kwargs())
    assert (exc.value.stage_key, exc.value.declared_model, exc.value.requested_model) == \
        ("vision_parse", None, cfg.model)
    assert fake.requests == 0
    assert _run_calls(db) == []


# ── (3) preflight: declared per model ─────────────────────────────────
def test_3_a_declared_preflight_model_is_sent(review, spec, fake):
    db, cb = review
    declared = ec.stage_config("audit", spec).model
    run_id = _open(db, spec, cb, ("audit", "preflight"), preflight_models=[declared])
    with rm.active_run(db._conn, run_id):
        oc.ollama_chat(messages=MESSAGES,
                       **ec.stage_config("preflight", spec, model=declared).kwargs())
    assert [c["model"] for c in fake.calls] == [declared]
    assert [(s, o) for s, o, _ in _run_calls(db)] == [(f"preflight:{declared}", "completed")]


def test_3_an_undeclared_preflight_model_is_refused_before_the_send(review, spec, fake):
    db, cb = review
    declared = ec.stage_config("audit", spec).model
    run_id = _open(db, spec, cb, ("audit", "preflight"), preflight_models=[declared])
    with rm.active_run(db._conn, run_id):
        with pytest.raises(rm.UndeclaredCall) as exc:
            oc.ollama_chat(messages=MESSAGES,
                           **ec.stage_config("preflight", spec, model=OTHER_MODEL).kwargs())
    assert (exc.value.stage_key, exc.value.declared_model, exc.value.requested_model) == \
        (f"preflight:{OTHER_MODEL}", None, OTHER_MODEL)
    assert fake.requests == 0
    assert _run_calls(db) == []


# ── (4) a declared call is sent and recorded, bytes unchanged ─────────
def test_4_a_declared_call_is_sent_and_recorded_with_its_request_unchanged(
        review, spec, fake):
    db, cb = review
    run_id = _open(db, spec, cb, ("audit",))
    cfg = ec.stage_config("audit", spec)
    with rm.active_run(db._conn, run_id):
        oc.ollama_chat(messages=MESSAGES, **cfg.kwargs())
    expected = {k: v for k, v in cfg.kwargs().items() if k != "stage"}
    expected["messages"] = MESSAGES
    assert fake.calls == [expected]
    assert _run_calls(db) == [("audit", "completed", rm.request_hash(expected))]


# ── (5) no active run, no check ───────────────────────────────────────
def test_5_no_active_run_means_no_check(review, spec, fake):
    db, cb = review
    _open(db, spec, cb, ("audit",))            # a manifest exists; it is not active
    kwargs = {**ec.stage_config("vision_parse", spec).kwargs(), "model": OTHER_MODEL}
    oc.ollama_chat(messages=MESSAGES, **kwargs)
    assert [c["model"] for c in fake.calls] == [OTHER_MODEL]
    assert _run_calls(db) == []


# ── (6) inside the extraction loop it is a run fault ──────────────────
def test_6_an_undeclared_call_leaves_the_extraction_loop_as_a_run_fault(
        tmp_path, spec, fake, monkeypatch):
    """The run declares the spec's extractor; the loop is then driven with a
    spec naming another one, so Pass 1's own call is the undeclared one. Four
    papers: were the refusal a paper outcome, three would fail and the abort
    would fire."""
    fields = tuple(f["name"] for f in load_codebook(LIVE_CODEBOOK).fields)
    db = ReviewDatabase("undeclared_loop", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(db.db_path).parent / "extraction_codebook.yaml")
    for pid in (1, 2, 3, 4):
        db._conn.execute("INSERT INTO papers (id, title, source, status, created_at, "
                         "updated_at) VALUES (?, 't', 's', 'FT_ELIGIBLE', 'n', 'n')", (pid,))
        seed_eligibility(db._conn, pid)
        write_parsed(db, pid, "\n".join(f"Snippet for {f}." for f in fields) + "\n")
    db._conn.commit()
    run_id = open_extraction_run(db, spec)
    other = spec.model_copy(update={"extraction_models": spec.extraction_models.model_copy(
        update={"extractor": OTHER_MODEL})})
    monkeypatch.setattr(E, "hold_experiment_lock", nullcontext)
    monkeypatch.setattr(E, "restart_ollama", lambda **kw: None)
    try:
        with patch("engine.utils.ollama_preflight.require_preflight"):
            with pytest.raises(rm.UndeclaredCall) as exc:
                E.run_extraction(db, other, "undeclared_loop", restart_every=0, run_id=run_id)
        assert (exc.value.stage_key, exc.value.requested_model) == ("extract_pass1", OTHER_MODEL)
        assert fake.requests == 0
        assert db._conn.execute(
            "SELECT COUNT(*) FROM paper_events WHERE event_type = 'extraction_failed'"
        ).fetchone()[0] == 0
        assert db._conn.execute("SELECT COUNT(*) FROM run_calls").fetchone()[0] == 0
        assert db._conn.execute("SELECT COUNT(*) FROM field_events").fetchone()[0] == 0
    finally:
        db.close()
