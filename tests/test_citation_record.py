"""P1 unit U03 (R612): the full citation record on the claim; the unit map keyed by run.

INT-g12-1 — a claim that carries evidence stores every unit it cites, not only
the first contiguous run: the cited indices in the model's order, the runs the
engine derives from them with their offsets into the parsed text, and — for a
JUDGMENT field — each step's own citations. It resolves from the parsed text
the claim names by sha256, with no unit-map file.
E-UNITMAP — the unit-map directory is named by the manifest run, and the
telemetry row says which.

Every database is a scratch `ReviewDatabase` under `tmp_path`; the model call is
faked at `ollama_client._client`. Nothing here reaches a model, a service or
the live review.
"""

from __future__ import annotations

import hashlib
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
from engine.core.extraction_telemetry import telemetry_path
from engine.core.parsed_text import resolve_parsed_text
from engine.core.review_spec import load_review_spec
from engine.elicitation import classes as C
from engine.elicitation.units import UNITS_VERSION, build_unit_map
from engine.utils import ollama_client as oc
from _loaded_context_fixture import loaded_ps  # U20 commit 2: the fake's /api/ps
from _event_store_fixture import seed_eligibility
from _parsed_text_fixture import write_parsed

REPO = Path(__file__).resolve().parent.parent
SPEC_PATH = REPO / "review_specs" / "surgical_autonomy.yaml"
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"
PID = 7
PAYLOAD_CITATION_RECORD = "citation_record"      # events.PAYLOAD_CITATION_RECORD, pinned below

# Fifty one-sentence units, a line break after every fifth so a multi-unit run
# can span one.
_WORDS = ("alpha beta gamma delta epsilon zeta eta theta iota kappa lambda mu nu xi "
          "omicron pi rho sigma tau upsilon").split()
PAPER = "".join(
    f"Finding {_WORDS[i % 20]} {_WORDS[(i * 7 + 3) % 20]} was measured in cohort {_WORDS[(i * 3 + 1) % 20]}."
    + ("\n" if i % 5 == 4 else " ") for i in range(50))

STATED_CITES = [47, 48, 12]
INFERABLE_CITES = [3, 4]
JUDGMENT_FIELD_CITES = [5]
JUDGMENT_STEPS = [
    {"step": "The cohort and its measure are described.", "unit_indices": [20, 21, 9]},
    {"step": "A later finding repeats the measure.", "unit_indices": [30]},
    {"step": "The codebook's threshold applies.", "criteria_application": True},
]


def test_the_fixture_paper_has_fifty_units():
    um = build_unit_map(PID, PAPER)
    assert um.n == 50 and um.version == UNITS_VERSION == "units-2"
    assert "\n" in PAPER[um.spans[19][0]:um.spans[20][1]]      # units 20–21 span a line break


@pytest.fixture(scope="module")
def elicited_spec():
    spec = load_review_spec(SPEC_PATH)
    return spec.model_copy(update={
        "extraction_models": spec.extraction_models.model_copy(update={"elicitation": True})})


def _add_paper(rdb, pid: int) -> None:
    rdb._conn.execute("INSERT INTO papers (id, title, source, status, created_at, "
                      "updated_at) VALUES (?, 't', 's', 'FT_ELIGIBLE', 'n', 'n')", (pid,))
    seed_eligibility(rdb._conn, pid)
    write_parsed(rdb, pid, PAPER)
    rdb._conn.commit()


@pytest.fixture
def db(tmp_path):
    rdb = ReviewDatabase("u03", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(rdb.db_path).parent / "extraction_codebook.yaml")
    _add_paper(rdb, PID)
    yield rdb
    rdb.close()


def _value(field: dict) -> str:
    vv = field.get("valid_values") or []
    if vv:
        return vv[0]["value"] if isinstance(vv[0], dict) else str(vv[0])
    return {"numeric": "5"}.get(field["type"], "a stated value")


class _Client:
    """`ollama.Client` stand-in. Pass 1 meets every class contract: STATED fields
    cite [47, 48, 12] (two runs, the lower-numbered one cited last), INFERABLE
    fields cite [3, 4], JUDGMENT fields cite unit 5 at field level and units
    [20, 21, 9] and [30] in two steps, with a third criteria-application step.
    Pass 2 is complete. Calls alternate Pass 1, Pass 2."""

    def __init__(self, codebook_path):
        cb = load_codebook(codebook_path)
        entries, spans = [], []
        for f in cb.fields:
            cls, v = C.field_class(f["name"], cb.raw), _value(f)
            if cls == C.STATED:
                e = {"field_name": f["name"], "unit_indices": STATED_CITES, "value": v}
            elif cls == C.INFERABLE:
                e = {"field_name": f["name"], "unit_indices": INFERABLE_CITES,
                     "inference": "The cohort named fixes the setting.", "value": v}
            else:
                e = {"field_name": f["name"], "unit_indices": JUDGMENT_FIELD_CITES,
                     "reasoning_steps": JUDGMENT_STEPS, "value": v}
            entries.append(e)
            spans.append(EvidenceSpan(field_name=f["name"], value=v,
                                      source_snippet="what the model typed (ignored)",
                                      confidence=0.9, tier=f["tier"]))
        self._pass1 = json.dumps({"fields": entries})
        self._pass2 = ExtractionOutput(fields=spans).model_dump_json()
        self.calls = []
        self._client = SimpleNamespace(base_url="http://u03.invalid")

    def ps(self):
        return loaded_ps()

    def show(self, model):
        return SimpleNamespace(modelinfo={"general.context_length": 131_072})

    def chat(self, **kwargs):
        self.calls.append(kwargs)
        content = self._pass1 if len(self.calls) % 2 == 1 else self._pass2
        chars = oc.message_chars(kwargs.get("messages"))
        return SimpleNamespace(
            message=SimpleNamespace(content=content, thinking="reasoning"),
            prompt_eval_count=max(1, int(chars * 0.3)), done_reason="stop", eval_count=10)


@pytest.fixture
def fake_ollama(monkeypatch, db):
    import httpx

    def _refuse(*a, **kw):
        raise AssertionError("U03 attempted a real HTTP request")
    monkeypatch.setattr(httpx.Client, "send", _refuse)
    client = _Client(Path(db.db_path).parent / "extraction_codebook.yaml")
    monkeypatch.setattr(oc, "_client", client)
    monkeypatch.setattr(oc, "fetch_model_digest",
                        lambda m: pytest.fail(f"fetch_model_digest({m!r}) during a run"))
    oc.clear_ceiling_cache()
    yield client
    oc.clear_ceiling_cache()


def _run(db, spec) -> int:
    """One elicited run through open_run, run_extraction and the real event writer."""
    handle = rm.open_run(
        db._conn, spec, kind="extraction", stages=[*E.extraction_stages(spec), "audit"],
        codebook=load_codebook_beside(db.db_path), digest_fn=lambda m: "e" * 64,
        git=rm.GitState(commit="0" * 40, dirty=False, tag=None), host="fixture")
    with patch("engine.utils.ollama_preflight.require_preflight"):
        stats = E.run_extraction(db, spec, "u03", experiment_lock=False,
                                 restart_every=0, run_id=handle.run_id)
    assert stats["extracted"] == 1, stats
    return handle.run_id


def _claims(db, pid: int = PID) -> dict[str, tuple[str, dict]]:
    """{field_name: (source_snippet, payload)} for the paper's asserted claims."""
    rows = db._conn.execute(
        "SELECT field_name, source_snippet, payload_json FROM field_events "
        "WHERE paper_id = ? AND event_type = 'asserted'", (pid,)).fetchall()
    assert len(rows) == 20
    return {r[0]: (r[1], json.loads(r[2])) for r in rows}


def _parsed_text_named_by(db, payload: dict, pid: int = PID) -> str:
    """The parsed text the claim names by sha256, read from disk and verified."""
    from engine.core.events import PAYLOAD_PARSED_TEXT_SHA256
    ref = resolve_parsed_text(db._conn, pid)
    data = ref.path.read_bytes()
    assert hashlib.sha256(data).hexdigest() == ref.sha256 == payload[PAYLOAD_PARSED_TEXT_SHA256]
    return data.decode("utf-8")


def _run_texts(text: str, runs: list[dict]) -> list[str]:
    return [text[r["start"]:r["end"]] for r in runs]


def _by_class(db) -> dict[str, list[str]]:
    cb = load_codebook_beside(db.db_path)
    out: dict[str, list[str]] = {cls: [] for cls in C.CLASSES}
    for f in cb.fields:
        out[C.field_class(f["name"], cb.raw)].append(f["name"])
    return out


# ── INT-g12-1, gate 1 ─────────────────────────────────────────────────
def test_an_asserted_claim_carries_every_cited_run_resolvable_from_the_parsed_text(
        db, elicited_spec, fake_ollama):
    _run(db, elicited_spec)
    review_dir = Path(db.db_path).parent
    # the unit-map directory is gone before anything is resolved
    assert list(review_dir.glob("elicitation/*/unit_maps/7.json"))
    shutil.rmtree(review_dir / "elicitation")

    claims, names = _claims(db), _by_class(db)
    um = build_unit_map(PID, PAPER)                 # the expectation, not the resolver
    for name in names[C.STATED]:
        snippet, payload = claims[name]
        text = _parsed_text_named_by(db, payload)
        rec = payload[PAYLOAD_CITATION_RECORD]
        assert rec["units_version"] == "units-2"
        assert rec["cited"] == [47, 48, 12]         # the model's order
        assert [(r["first"], r["last"]) for r in rec["runs"]] == [(12, 12), (47, 48)]
        low, high = _run_texts(text, rec["runs"])
        assert low == PAPER[um.spans[11][0]:um.spans[11][1]]
        assert high == PAPER[um.spans[46][0]:um.spans[47][1]]
        assert " ".join(high.split()) == f"{um.units[46]} {um.units[47]}"
        # the stored snippet stays the first derived (lowest-numbered) run
        assert snippet == low
    assert names[C.STATED]

    # every asserted claim carries a record, and its first run is its snippet
    for name, (snippet, payload) in claims.items():
        rec = payload[PAYLOAD_CITATION_RECORD]
        text = _parsed_text_named_by(db, payload)
        assert _run_texts(text, rec["runs"])[0] == snippet
        assert all(t and t == t.strip() for t in _run_texts(text, rec["runs"]))
    for name in names[C.INFERABLE]:
        rec = claims[name][1][PAYLOAD_CITATION_RECORD]
        assert rec["cited"] == [3, 4] and "steps" not in rec
        assert [(r["first"], r["last"]) for r in rec["runs"]] == [(3, 4)]


# ── INT-g12-1, gate 2 ─────────────────────────────────────────────────
def test_a_judgment_claim_records_each_steps_citations_resolvably(db, elicited_spec,
                                                                  fake_ollama):
    _run(db, elicited_spec)
    shutil.rmtree(Path(db.db_path).parent / "elicitation")
    claims, names = _claims(db), _by_class(db)
    um = build_unit_map(PID, PAPER)

    def slice_of(first: int, last: int) -> str:
        return PAPER[um.spans[first - 1][0]:um.spans[last - 1][1]]

    assert names[C.JUDGMENT]
    for name in names[C.JUDGMENT]:
        snippet, payload = claims[name]
        text = _parsed_text_named_by(db, payload)
        rec = payload[PAYLOAD_CITATION_RECORD]
        # every index the citation check counts: field level first, then the steps'
        assert rec["cited"] == [5, 20, 21, 9, 30]
        assert [(r["first"], r["last"]) for r in rec["runs"]] == [
            (5, 5), (9, 9), (20, 21), (30, 30)]
        assert _run_texts(text, rec["runs"]) == [
            slice_of(5, 5), slice_of(9, 9), slice_of(20, 21), slice_of(30, 30)]
        assert snippet == slice_of(5, 5)

        steps = rec["steps"]
        assert [s["cited"] for s in steps] == [[20, 21, 9], [30], []]
        assert [s["criteria_application"] for s in steps] == [False, False, True]
        assert [[(r["first"], r["last"]) for r in s["runs"]] for s in steps] == [
            [(9, 9), (20, 21)], [(30, 30)], []]
        assert _run_texts(text, steps[0]["runs"]) == [slice_of(9, 9), slice_of(20, 21)]
        assert "\n" in slice_of(20, 21)
        assert _run_texts(text, steps[1]["runs"]) == [slice_of(30, 30)]


# ── E-UNITMAP, gate 3 ─────────────────────────────────────────────────
def _stored_rows(review_dir: Path) -> dict[int, dict]:
    rows = [json.loads(line) for line in telemetry_path(review_dir).read_text().splitlines()]
    return {r["paper_id"]: r for r in rows if r["outcome"] == "stored"}


def test_the_unit_map_directory_is_named_by_the_manifest_run_id(db, elicited_spec,
                                                               fake_ollama):
    review_dir = Path(db.db_path).parent
    first = _run(db, elicited_spec)
    _add_paper(db, 8)
    second = _run(db, elicited_spec)                # same process, a second manifest run
    assert first != second

    dirs = {p.parent.parent.name: p.name
            for p in review_dir.glob("elicitation/*/unit_maps/*.json")}
    assert dirs == {f"run_{first}": "7.json", f"run_{second}": "8.json"}
    assert (E.unit_map_dir_for(first), E.unit_map_dir_for(second)) == (
        f"run_{first}", f"run_{second}")

    rows = _stored_rows(review_dir)
    assert rows[7]["extra"]["unit_map_dir"] == f"run_{first}"
    assert rows[8]["extra"]["unit_map_dir"] == f"run_{second}"


def test_without_a_manifest_run_the_directory_name_cannot_be_a_manifest_name():
    name = E.unit_map_dir_for(None)
    assert name == E.unit_map_dir_for(None)        # one name per process
    manifest_prefix = E.unit_map_dir_for(1)[:-1]
    assert E.unit_map_dir_for(12) == f"{manifest_prefix}12"
    assert not name.startswith(manifest_prefix)


# ── The record itself ─────────────────────────────────────────────────
def test_the_citation_record_is_checked_exact_against_the_text_the_claim_names():
    from engine.core import events
    from engine.elicitation import materialize as M
    from engine.elicitation.contracts import FieldRecord, Step
    assert events.PAYLOAD_CITATION_RECORD == PAYLOAD_CITATION_RECORD
    um = build_unit_map(PID, PAPER)
    rec = FieldRecord("f", C.JUDGMENT, "v", False, indices=(5, 20, 21, 9),
                      steps=(Step("s", (20, 21, 9)), Step("c", (), True)))
    out = M.citation_record(rec, um, PAPER)
    assert out["cited"] == [5, 20, 21, 9]
    assert out["runs"][0] == {"first": 5, "last": 5,
                              "start": um.spans[4][0], "end": um.spans[4][1]}
    assert PAPER[out["runs"][0]["start"]:out["runs"][0]["end"]] == M.source_snippet(rec, um, PAPER)
    json.dumps(out)                                  # plain JSON, as stored
    # a text other than the one the unit map was built from is refused (R534)
    with pytest.raises(M.SnippetNotExact):
        M.citation_record(rec, um, PAPER.replace("Finding", "Findxng"))
    # nothing cited, nothing recorded
    assert M.citation_record(FieldRecord("f", C.STATED, "v", False), um, PAPER) is None
