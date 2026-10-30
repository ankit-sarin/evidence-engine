"""P1 unit U01 (R612): units as source slices, R534's EXACT check, E-TEST.

INT-g6-1 — a unit is an exact (start, end) slice of the raw parsed text, and a
cited run is one contiguous slice (R572), never units joined by a separator.
A-10 — comments are excluded from units by span arithmetic, so the stored
snippet is located against the same text it was cut from. E-TEST (R543) — one
elicited run, opened, pinned, verified and written through the real event
writer, with the model call faked at `ollama_client._client`.

Every database is a scratch `ReviewDatabase` under `tmp_path`; nothing here
reaches a model, a service or the live review.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from engine.agents import extractor as E
from engine.agents.models import EvidenceSpan, ExtractionOutput
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook, load_codebook_beside
from engine.core.database import ReviewDatabase
from engine.core.locator import locate
from engine.core.review_spec import load_review_spec
from engine.elicitation import classes as C
from engine.elicitation import materialize as M
from engine.elicitation.contracts import FieldRecord
from engine.elicitation.units import build_unit_map
from engine.utils import ollama_client as oc
from _loaded_context_fixture import loaded_ps  # U20 commit 2: the fake's /api/ps
from _event_store_fixture import seed_eligibility
from _parsed_text_fixture import write_parsed

REPO = Path(__file__).resolve().parent.parent
SPEC_PATH = REPO / "review_specs" / "surgical_autonomy.yaml"
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"
PID = 7


def _record(*indices: int) -> FieldRecord:
    return FieldRecord(field_name="f", field_class=C.STATED, value="v", is_escape=False,
                       indices=tuple(indices), inference="", steps=(), violations=(),
                       bad_indices=(), duplicate_indices=())


def _presented(text: str) -> str:
    return " ".join(text.split())


# A line-wrapped sentence, a double space, an ellipsis, a blank line and a
# Docling comment — each a place where joining unit strings with one space
# produced text that is not in the paper.
RAW = ("Alpha beta gamma delta\nepsilon zeta.  Eta theta iota kappa... "
       "Lambda mu nu xi.\n\n<!-- image -->\n\nOmicron pi rho sigma. Tau upsilon phi chi.\n")


# ── INT-g6-1 / R534 ───────────────────────────────────────────────────
def test_a_materialised_run_is_one_exact_slice_of_the_raw_text():
    um = build_unit_map(1, RAW)
    assert um.n == 5, um.units
    assert len(um.spans) == um.n
    # every unit is an exact, trimmed slice; units are ordered and disjoint
    prev_end = 0
    for (start, end), shown in zip(um.spans, um.units):
        piece = RAW[start:end]
        assert piece and piece == piece.strip()
        assert shown == _presented(piece)
        assert start >= prev_end
        prev_end = end
    # pysbd ends a sentence at the line break, so "epsilon zeta." is a short unit
    # and merges forward: the merged unit is one slice, double space included,
    # and the ellipsis belongs to the unit before it. Nothing is dropped.
    assert [RAW[s:e] for s, e in um.spans] == [
        "Alpha beta gamma delta", "epsilon zeta.  Eta theta iota kappa...",
        "Lambda mu nu xi.", "Omicron pi rho sigma.", "Tau upsilon phi chi."]
    assert um.units[1] == "epsilon zeta. Eta theta iota kappa..."
    # no comment text in any unit
    assert all("<!--" not in RAW[s:e] and "image" not in RAW[s:e] for s, e in um.spans)

    # a multi-unit run is one contiguous slice, first start to last end
    for first in range(1, um.n):
        snippet = M.source_snippet(_record(first, first + 1), um)
        assert snippet == RAW[um.spans[first - 1][0]:um.spans[first][1]]
        assert snippet in RAW
    # the line wrap and the double space survive in the stored text
    assert M.source_snippet(_record(1, 2), um) == (
        "Alpha beta gamma delta\nepsilon zeta.  Eta theta iota kappa...")

    # R534: the EXACT check refuses a snippet that is not the text at its offsets
    other = RAW.replace("Alpha", "Alpxa")
    with pytest.raises(M.SnippetNotExact):
        M.source_snippet(_record(1, 2), um, source_text=other)


# ── A-10 / R572 ───────────────────────────────────────────────────────
def test_a_run_straddling_a_docling_comment_and_a_blank_line_is_located_exact():
    raw = "A first sentence here.\n\n<!-- image -->\n\nB second sentence here."
    um = build_unit_map(1, raw)
    assert um.units == ("A first sentence here.", "B second sentence here.")
    snippet = M.source_snippet(_record(1, 2), um)
    assert snippet == raw and snippet in raw
    assert locate(raw, snippet).kind == "exact"

    # Wave 1 row 7: an unterminated `<!--` before a later comment must not
    # swallow the body between them.
    broken = ("Intro words go here. <!-- broken Body sentence one here. "
              "Body sentence two here. <!-- image --> Tail words go here.")
    um2 = build_unit_map(1, broken)
    joined = " ".join(um2.units)
    assert "Body sentence one here." in joined and "Body sentence two here." in joined
    assert "image" not in joined


def test_the_presented_form_is_the_whitespace_collapsed_slice():
    raw = "Alpha beta\ngamma  delta epsilon. Zeta eta theta iota."
    um = build_unit_map(1, raw)
    start, end = um.spans[0]
    assert raw[start:end] == "Alpha beta\ngamma  delta epsilon."
    assert um.units[0] == "Alpha beta gamma delta epsilon."
    assert um.resolve(1) == um.units[0]
    assert um.render().splitlines()[0] == "[S1] Alpha beta gamma delta epsilon."
    assert um.to_json()["spans"][0] == [start, end]


# ── E-TEST (R543) ─────────────────────────────────────────────────────
PAPER = ("Title of the paper here.\n\n"
         "The system used a da Vinci\nResearch Kit.  "
         "All experiments ran at Vancouver General Hospital.\n\n"
         "<!-- image -->\n\n"
         "Five porcine subjects were enrolled in the trial. "
         "The planner computed paths without operator input.\n")


@pytest.fixture(scope="module")
def elicited_spec():
    spec = load_review_spec(SPEC_PATH)
    return spec.model_copy(update={
        "extraction_models": spec.extraction_models.model_copy(update={"elicitation": True})})


@pytest.fixture
def db(tmp_path):
    rdb = ReviewDatabase("etest", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(rdb.db_path).parent / "extraction_codebook.yaml")
    rdb._conn.execute("INSERT INTO papers (id, title, source, status, created_at, "
                      "updated_at) VALUES (?, 't', 's', 'FT_ELIGIBLE', 'n', 'n')", (PID,))
    seed_eligibility(rdb._conn, PID)
    write_parsed(rdb, PID, PAPER)
    rdb._conn.commit()
    yield rdb
    rdb.close()


def _value(field: dict) -> str:
    vv = field.get("valid_values") or []
    if vv:
        return vv[0]["value"] if isinstance(vv[0], dict) else str(vv[0])
    return {"numeric": "5"}.get(field["type"], "a stated value")


class _ElicitedClient:
    """`ollama.Client` stand-in: a Pass-1 citation response that meets every
    class contract, then a complete Pass 2. STATED fields cite units 2–3 (a line
    wrap between them and a double space inside the second); the other classes
    cite units 3–4, which straddle a blank line and a Docling comment. Unit 3 is
    "Research Kit.  All experiments … Hospital.": pysbd ends a sentence at the
    line break and the two-token "Research Kit." merges forward."""

    def __init__(self, codebook_path):
        cb = load_codebook(codebook_path)
        entries, spans = [], []
        for f in cb.fields:
            cls, v = C.field_class(f["name"], cb.raw), _value(f)
            if cls == C.STATED:
                e = {"field_name": f["name"], "unit_indices": [2, 3], "value": v}
            elif cls == C.INFERABLE:
                e = {"field_name": f["name"], "unit_indices": [3, 4],
                     "inference": "The hospital named fixes the setting.", "value": v}
            else:
                e = {"field_name": f["name"],
                     "reasoning_steps": [{"step": "The trial and its setting are described.",
                                          "unit_indices": [3, 4]}],
                     "value": v}
            entries.append(e)
            spans.append(EvidenceSpan(field_name=f["name"], value=v,
                                      source_snippet="what the model typed (ignored)",
                                      confidence=0.9, tier=f["tier"]))
        self._pass1 = json.dumps({"fields": entries})
        self._pass2 = ExtractionOutput(fields=spans).model_dump_json()
        self.calls = []
        self._client = SimpleNamespace(base_url="http://etest.invalid")

    def ps(self):
        return loaded_ps()

    def show(self, model):
        return SimpleNamespace(modelinfo={"general.context_length": 131_072})

    def chat(self, **kwargs):
        self.calls.append(kwargs)
        content = self._pass1 if len(self.calls) == 1 else self._pass2
        chars = oc.message_chars(kwargs.get("messages"))
        return SimpleNamespace(
            message=SimpleNamespace(content=content, thinking="reasoning"),
            prompt_eval_count=max(1, int(chars * 0.3)), done_reason="stop", eval_count=10)


@pytest.fixture
def fake_ollama(monkeypatch, db):
    import httpx

    def _refuse(*a, **kw):
        raise AssertionError("E-TEST attempted a real HTTP request")
    monkeypatch.setattr(httpx.Client, "send", _refuse)
    client = _ElicitedClient(Path(db.db_path).parent / "extraction_codebook.yaml")
    monkeypatch.setattr(oc, "_client", client)
    monkeypatch.setattr(oc, "fetch_model_digest",
                        lambda m: pytest.fail(f"fetch_model_digest({m!r}) during a run"))
    oc.clear_ceiling_cache()
    yield client
    oc.clear_ceiling_cache()


def test_an_elicited_run_opens_pins_verifies_and_writes_real_events(db, elicited_spec,
                                                                    fake_ollama):
    spec, conn = elicited_spec, db._conn
    stages = E.extraction_stages(spec)
    assert stages[0] == "elicitation_pass1"
    handle = rm.open_run(
        conn, spec, kind="extraction", stages=[*stages, "audit"],
        codebook=load_codebook_beside(db.db_path), digest_fn=lambda m: "e" * 64,
        git=rm.GitState(commit="0" * 40, dirty=False, tag=None), host="fixture")

    # the manifest registered the arm; the extract-time check pins it (R569(b),
    # P1 U05) and agrees with the pin
    arm = spec.extraction_models.arm
    pin = ("SELECT configuration_marker, pinned_run_id, pinned_sha256 FROM arms "
           "WHERE arm_name = ?")
    assert tuple(conn.execute(pin, (arm,)).fetchone()) == (None, None, None)
    assert E.verify_extraction_run(conn, spec, handle.run_id) == "e" * 64
    marker, pinned_run, pinned_sha = conn.execute(pin, (arm,)).fetchone()
    assert (marker, pinned_run) == ("pinned", handle.run_id) and len(pinned_sha) == 64

    with patch("engine.utils.ollama_preflight.require_preflight"):
        stats = E.run_extraction(db, spec, "etest", experiment_lock=False,
                                 restart_every=0, run_id=handle.run_id)
    assert stats["extracted"] == 1
    assert [c[0] for c in conn.execute(
        "SELECT stage FROM run_calls ORDER BY call_id")] == ["elicitation_pass1",
                                                             "extract_pass2"]

    # the claims went through the real event writer, on the pinned arm and run
    rows = conn.execute(
        "SELECT field_name, value, source_snippet, arm, run_id, actor_digest "
        "FROM field_events WHERE paper_id = ? AND event_type = 'asserted'", (PID,)).fetchall()
    n_fields = len(load_codebook_beside(db.db_path).fields)
    assert len(rows) == n_fields == 20
    assert {(r[3], r[4], r[5]) for r in rows} == {(arm, handle.run_id, "e" * 64)}

    # and every stored snippet is an exact substring of the parsed text (INT-g6-1)
    stated = PAPER[PAPER.index("The system used"):PAPER.index("Hospital.") + len("Hospital.")]
    straddling = PAPER[PAPER.index("Research Kit."):PAPER.index("the trial.") + len("the trial.")]
    assert "<!-- image -->" in straddling and "\n" in stated and "  " in stated
    snippets = {r[2] for r in rows}
    assert snippets == {stated, straddling}
    assert all(s in PAPER and locate(PAPER, s).kind == "exact" for s in snippets)

    # the unit map on disk records the offsets the snippets were cut at
    maps = list(Path(db.db_path).parent.glob("elicitation/*/unit_maps/7.json"))
    assert len(maps) == 1
    data = json.loads(maps[0].read_text())
    assert [PAPER[s:e] for s, e in data["spans"]][0] == "Title of the paper here."
    assert len(data["spans"]) == data["n_units"] == len(data["units"])
