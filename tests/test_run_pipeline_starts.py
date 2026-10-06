"""C57 — a `run_pipeline` start at or before SCREEN stops at the adjudication
gate, whether or not `--skip-to` named the start (12e-C55-R1).

Each test drives `run_pipeline` itself, from its real manifest
(`_open_run_manifest`, injected git state and digests) through the real SEARCH
and SCREEN stages, on a scratch `ReviewDatabase`. The two searches return
nothing; Ollama is a fake client at `ollama_client._client`. The review holds
two INGESTED papers for SCREEN and two eligible papers with parsed text, so a
run that passed the gate would have PARSE-and-later work to do and would send
extraction calls. No model is called and nothing reaches `data/`.
"""

from __future__ import annotations

import shutil
import sqlite3
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import scripts.run_pipeline as rp
from engine.agents import extractor as E
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec
from engine.utils import ollama_client as oc
from _event_store_fixture import seed_eligibility
from _parsed_text_fixture import write_parsed

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"

pytestmark = pytest.mark.skipif(not LIVE_CODEBOOK.exists(), reason="codebook not available")

SCREEN_IN = '{"decision": "include", "rationale": "r", "confidence": 0.9}'


class FakeClient:
    """Stands in for `ollama.Client`. A screening request (it carries the
    screening format) is answered with an include; anything else with `OK`."""

    def __init__(self):
        self.calls: list[dict] = []
        self._client = SimpleNamespace(base_url="http://starts.invalid")

    def show(self, model):
        return SimpleNamespace(modelinfo={"general.context_length": 131_072})

    def chat(self, **kwargs):
        self.calls.append(kwargs)
        screening = "decision" in str(kwargs.get("format") or "")
        return SimpleNamespace(
            message=SimpleNamespace(content=SCREEN_IN if screening else "OK", thinking=None),
            prompt_eval_count=max(1, int(oc.message_chars(kwargs.get("messages")) * 0.3)),
            done_reason="stop", eval_count=5)

    @property
    def models(self) -> list[str]:
        return [c["model"] for c in self.calls]


@pytest.fixture(scope="module")
def spec():
    return load_review_spec(REPO / "review_specs" / "surgical_autonomy.yaml")


@pytest.fixture
def review(tmp_path, spec, monkeypatch):
    """A scratch review and `run_pipeline` wired to it. Yields the database
    path and the fake client; `run_pipeline` closes the database itself."""
    import httpx
    monkeypatch.setattr(httpx.Client, "send",
                        lambda *a, **k: pytest.fail("a start test made a real HTTP request"))
    fields = tuple(f["name"] for f in load_codebook(LIVE_CODEBOOK).fields)
    db = ReviewDatabase("starts", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(db.db_path).parent / "extraction_codebook.yaml")
    for pid in (1, 2):
        db._conn.execute(
            "INSERT INTO papers (id, title, abstract, source, status, created_at, updated_at) "
            "VALUES (?, 'A title', 'An abstract.', 's', 'INGESTED', 'n', 'n')", (pid,))
    for pid in (11, 12):
        db._conn.execute(
            "INSERT INTO papers (id, title, source, status, created_at, updated_at) "
            "VALUES (?, 't', 's', 'FT_ELIGIBLE', 'n', 'n')", (pid,))
        seed_eligibility(db._conn, pid)
        write_parsed(db, pid, "\n".join(f"Snippet for {f}." for f in fields) + "\n")
    db._conn.commit()

    fake = FakeClient()
    monkeypatch.setattr(oc, "_client", fake)
    monkeypatch.setattr(oc, "fetch_model_digest", lambda model, *a, **k: "a" * 64)
    monkeypatch.setattr(rm, "git_state",
                        lambda *a, **k: rm.GitState(commit="b" * 40, dirty=False, tag=None))
    monkeypatch.setattr(rp, "load_spec_for", lambda name, path=None: spec)
    monkeypatch.setattr(rp, "ReviewDatabase", lambda name: db)
    monkeypatch.setattr(rp, "search_pubmed", lambda s: [])
    monkeypatch.setattr(rp, "search_openalex", lambda s: [])
    monkeypatch.setattr(E, "hold_experiment_lock", nullcontext)
    monkeypatch.setattr(E, "restart_ollama", lambda **kw: None)
    oc.clear_ceiling_cache()
    yield Path(db.db_path), fake
    oc.clear_ceiling_cache()
    db.close()


def _state(db_path):
    """What the run left: its end, its recorded calls by stage, and whether any
    stage past the gate wrote."""
    conn = sqlite3.connect(db_path)
    try:
        run_id, status, reason = conn.execute(
            "SELECT run_id, end_status, end_reason FROM run_manifests "
            "WHERE run_kind <> 'import' ORDER BY run_id DESC LIMIT 1").fetchone()
        stages = sorted(r[0] for r in conn.execute(
            "SELECT DISTINCT stage FROM run_calls WHERE run_id = ?", (run_id,)))
        field_events = conn.execute(
            "SELECT COUNT(*) FROM field_events WHERE run_id = ?", (run_id,)).fetchone()[0]
        screened = sorted(r[0] for r in conn.execute(
            "SELECT status FROM papers WHERE id IN (1, 2)"))
        return (status, reason), stages, field_events, screened
    finally:
        conn.close()


# ── (i) / (ii): the gate, with the screening preflight out of the way ─
@pytest.mark.parametrize("skip_to", ["search", "screen", None])
def test_a_start_at_or_before_screen_stops_at_the_adjudication_gate(review, spec, skip_to):
    """`None` is the default start, which has always stopped here (ii); the two
    named starts must end exactly as it does (i). The preflight is patched out,
    as in the 12e_C55-A probe, so the test is about the gate alone."""
    db_path, fake = review
    with patch("engine.utils.ollama_preflight.require_preflight"):
        rp.run_pipeline("starts", skip_to=skip_to)
    end, stages, field_events, screened = _state(db_path)
    assert end == ("interrupted", rm.REASON_BLOCKED_ADJUDICATION)
    # SCREEN ran, and nothing after it: no PARSE, EXTRACT or AUDIT call.
    assert screened == ["ABSTRACT_SCREENED_IN", "ABSTRACT_SCREENED_IN"]
    assert stages == ["abstract_screen_primary"]
    assert set(fake.models) == {spec.screening_models.primary}
    assert len(fake.calls) == 4
    assert field_events == 0
