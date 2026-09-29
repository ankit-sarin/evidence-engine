"""The extraction bound (10b-C2): `run_pipeline --max-papers N`.

Declared at `open_run` and recorded in the manifest under `selection_bound`,
applied after selection's reuse-key skip, deterministic (ascending paper_id),
and a window on the unskipped remainder: a second bounded run takes the NEXT N.

Every database is a scratch `ReviewDatabase` with twelve eligible papers and a
parsed text each. The manifest is opened through `run_pipeline`'s own
`_open_run_manifest`, with `open_run`'s digest and git lookups injected (the
`open_extraction_run` values). Ollama is the fake client at
`ollama_client._client`; no model is called and nothing reaches `data/`.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import argparse
import pytest

import scripts.run_pipeline as rp
from engine.agents import extractor as E
from engine.agents.models import EvidenceSpan, ExtractionOutput
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec
from engine.core.run_telemetry import read_run_events
from engine.core.selection import bound_selection, select_for_extraction
from engine.utils import ollama_client as oc
from _event_store_fixture import FIXTURE_DIGEST, seed_eligibility
from _parsed_text_fixture import write_parsed

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"
PAPERS = tuple(range(1, 13))  # twelve eligible papers


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
    rdb = ReviewDatabase("bnd", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(rdb.db_path).parent / "extraction_codebook.yaml")
    for pid in PAPERS:
        rdb._conn.execute("INSERT INTO papers (id, title, source, status, created_at, "
                          "updated_at) VALUES (?, 't', 's', 'FT_ELIGIBLE', 'n', 'n')", (pid,))
        seed_eligibility(rdb._conn, pid)
        write_parsed(rdb, pid, _paper_text(fields))
    rdb._conn.commit()
    yield rdb
    rdb.close()


@pytest.fixture
def injected_open_run(monkeypatch):
    """`_open_run_manifest` calls `rm.open_run` with no digest or git override;
    inject the fixture values so no HTTP request or `git status` decides it."""
    real = rm.open_run
    monkeypatch.setattr(rm, "open_run", lambda *a, **k: real(
        *a, digest_fn=lambda m: FIXTURE_DIGEST,
        git=rm.GitState(commit="0" * 40, dirty=False, tag=None), host="fixture", **k))


class _Client:
    """`ollama.Client` stand-in: pass 1, then a complete pass 2 whose every
    snippet is a verbatim line of the paper (the cut-over tests' client)."""

    def __init__(self, fields):
        self._pass2 = ExtractionOutput(fields=[
            EvidenceSpan(field_name=f, value="RCT" if f == "study_type" else "NR",
                         source_snippet=f"Snippet for {f}.", confidence=0.9, tier=1)
            for f in fields]).model_dump_json()
        self.calls = []
        self._client = SimpleNamespace(base_url="http://bound.invalid")

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
                        lambda *a, **k: pytest.fail("a bound test made a real HTTP request"))
    monkeypatch.setattr(oc, "_client", _Client(fields))
    monkeypatch.setattr(E, "hold_experiment_lock", nullcontext)
    monkeypatch.setattr(E, "restart_ollama", lambda **kw: None)
    oc.clear_ceiling_cache()
    with patch("engine.utils.ollama_preflight.require_preflight"):
        yield
    oc.clear_ceiling_cache()


EXTRACT = rp.STAGES.index("extract")


def _manifest(db, run_id) -> dict:
    return json.loads(db._conn.execute(
        "SELECT manifest_json FROM run_manifests WHERE run_id = ?", (run_id,)).fetchone()[0])


def _bounded_events(db, run_id):
    return [e for e in read_run_events(Path(db.db_path).parent)
            if e["kind"] == "selection_bounded" and e["run_id"] == run_id]


def _capture_selection(monkeypatch):
    """Stub run_extraction and keep the selection the stage handed it."""
    seen = {}

    def fake(db, spec, review_name, *, selection, run_id):
        seen["ids"] = tuple(pid for pid, _ in selection.to_extract)
        return {"extracted": 0, "failed": 0}
    monkeypatch.setattr(rp, "run_extraction", fake)
    return seen


def _claimed_papers(db, arm):
    return {r[0] for r in db._conn.execute(
        "SELECT DISTINCT paper_id FROM field_events WHERE arm = ?", (arm,))}


# ── T1 ────────────────────────────────────────────────────────────────
def test_T1_absent_bound_selects_as_today_and_records_nothing(db, spec, monkeypatch,
                                                              injected_open_run):
    run_id = rp._open_run_manifest(db, spec, EXTRACT)
    assert "selection_bound" not in _manifest(db, run_id)
    seen = _capture_selection(monkeypatch)
    rp._stage_extract(db, spec, "bnd", run_id=run_id)
    today = select_for_extraction(db._conn, arm=spec.extraction_models.arm)
    assert seen["ids"] == tuple(pid for pid, _ in today.to_extract) == PAPERS
    assert _bounded_events(db, run_id) == []


# ── T2 ────────────────────────────────────────────────────────────────
def test_T2_a_bound_of_five_selects_the_first_five_and_is_recorded(db, spec, fields,
                                                                   fake_models,
                                                                   injected_open_run):
    run_id = rp._open_run_manifest(db, spec, EXTRACT, max_papers=5)
    assert _manifest(db, run_id)["selection_bound"] == {"stage": "extract", "max_papers": 5}
    stats = rp._stage_extract(db, spec, "bnd", run_id=run_id, max_papers=5)
    assert stats["extracted"] == 5
    assert _claimed_papers(db, spec.extraction_models.arm) == {1, 2, 3, 4, 5}
    (event,) = _bounded_events(db, run_id)
    assert (event["payload"]["max_papers"], event["payload"]["selected_before"],
            event["payload"]["selected_after"]) == (5, 12, 5)


# ── T3 ────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("bad", [0, -1, "5", 2.5, True])
def test_T3_an_invalid_bound_refuses_before_any_manifest(db, spec, monkeypatch, bad):
    monkeypatch.setattr(rp, "load_spec_for", lambda name, path=None: spec)
    monkeypatch.setattr(rp, "ReviewDatabase", lambda name: db)
    before = db._conn.execute("SELECT COUNT(*) FROM run_manifests").fetchone()[0]
    with pytest.raises(ValueError, match="max-papers"):
        rp.run_pipeline("bnd", skip_to="extract", max_papers=bad)
    assert db._conn.execute("SELECT COUNT(*) FROM run_manifests").fetchone()[0] == before


@pytest.mark.parametrize("text", ["0", "-1", "x", "2.5"])
def test_T3_the_cli_type_refuses_non_positive_and_non_integer(text):
    with pytest.raises(argparse.ArgumentTypeError):
        rp._positive_int(text)
    assert rp._positive_int("5") == 5


def test_T3_a_bound_on_a_run_without_the_extract_stage_refuses(db, spec, monkeypatch):
    monkeypatch.setattr(rp, "load_spec_for", lambda name, path=None: spec)
    monkeypatch.setattr(rp, "ReviewDatabase", lambda name: db)
    before = db._conn.execute("SELECT COUNT(*) FROM run_manifests").fetchone()[0]
    with pytest.raises(ValueError, match="extract stage"):
        rp.run_pipeline("bnd", skip_to="audit", max_papers=5)
    assert db._conn.execute("SELECT COUNT(*) FROM run_manifests").fetchone()[0] == before


def test_T3_run_pipeline_threads_the_bound_to_the_manifest_and_the_stage(db, spec,
                                                                         monkeypatch):
    got = {}
    monkeypatch.setattr(rp, "load_spec_for", lambda name, path=None: spec)
    monkeypatch.setattr(rp, "ReviewDatabase", lambda name: db)
    monkeypatch.setattr(rp, "is_adjudication_complete", lambda conn: True)
    monkeypatch.setattr(rp, "_stage_parse", lambda *a, **k: {})
    def fake_open(d, s, i, **k):
        got["manifest"] = k
        return 1
    monkeypatch.setattr(rp, "_open_run_manifest", fake_open)
    monkeypatch.setattr(rp.rm, "activate", lambda conn, run_id: None)
    monkeypatch.setattr(rp.rm, "deactivate", lambda token: None)

    def stop(*a, **k):
        got["stage"] = k
        raise RuntimeError("stop after extract")
    monkeypatch.setattr(rp, "_stage_extract", stop)
    monkeypatch.setattr(rp, "_finish_review_run", lambda *a, **k: None)
    monkeypatch.setattr(db, "close", lambda: None)
    with pytest.raises(RuntimeError, match="stop after extract"):
        rp.run_pipeline("bnd", skip_to="extract", max_papers=7)
    assert got["manifest"] == {"max_papers": 7}
    assert got["stage"]["max_papers"] == 7


# ── T4 ────────────────────────────────────────────────────────────────
def test_T4_a_second_bounded_run_takes_the_next_window(db, spec, fields, fake_models,
                                                       injected_open_run):
    arm = spec.extraction_models.arm
    first = rp._open_run_manifest(db, spec, EXTRACT, max_papers=5)
    rp._stage_extract(db, spec, "bnd", run_id=first, max_papers=5)
    assert _claimed_papers(db, arm) == {1, 2, 3, 4, 5}

    second = rp._open_run_manifest(db, spec, EXTRACT, max_papers=5)
    stats = rp._stage_extract(db, spec, "bnd", run_id=second, max_papers=5)
    assert stats["extracted"] == 5
    assert stats["skipped_asserted"] == 5
    second_ids = {r[0] for r in db._conn.execute(
        "SELECT DISTINCT fe.paper_id FROM field_events fe JOIN claim_inputs ci "
        "ON fe.extraction_uid = ci.extraction_uid WHERE ci.run_id = ?", (second,))}
    assert second_ids == {6, 7, 8, 9, 10}
    (event,) = _bounded_events(db, second)
    assert (event["payload"]["selected_before"], event["payload"]["selected_after"]) == (7, 5)


# ── T5 ────────────────────────────────────────────────────────────────
def test_T5_a_bound_above_the_eligible_count_selects_all(db, spec, monkeypatch,
                                                        injected_open_run):
    run_id = rp._open_run_manifest(db, spec, EXTRACT, max_papers=20)
    assert _manifest(db, run_id)["selection_bound"] == {"stage": "extract", "max_papers": 20}
    seen = _capture_selection(monkeypatch)
    rp._stage_extract(db, spec, "bnd", run_id=run_id, max_papers=20)
    assert seen["ids"] == PAPERS
    (event,) = _bounded_events(db, run_id)
    assert (event["payload"]["max_papers"], event["payload"]["selected_before"],
            event["payload"]["selected_after"]) == (20, 12, 12)


def test_bound_selection_keeps_order_and_skip_lists():
    from engine.core.selection import SelectionResult
    sel = SelectionResult("a", ((3, None), (1, None), (2, None)), (9,), ((8, "x"),))
    out = bound_selection(sel, 2)
    assert out.to_extract == ((3, None), (1, None))
    assert out.skipped_asserted == (9,) and out.skipped_refused == ((8, "x"),)
