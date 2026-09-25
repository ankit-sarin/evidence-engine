"""The extractor's run link (WRITE-PATH-01 9b-2b: R116, R117, run_calls.paper_id).

T1–T6 of the 9b-2b brief, plus R5's narrowed idempotence. Every database is a
scratch `ReviewDatabase` under `tmp_path`; Ollama is the fake client from
`test_request_capture` at `ollama_client._client`, so `ollama_chat` itself runs
and records `run_calls` exactly as in production.
"""

from __future__ import annotations

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
from engine.core.effective_config import stage_config
from engine.core.review_spec import load_review_spec
from engine.utils import ollama_client as oc
from _event_store_fixture import FIXTURE_DIGEST, open_extraction_run, seed_eligibility
from _parsed_text_fixture import write_parsed

REPO = Path(__file__).resolve().parent.parent
SPEC_PATH = REPO / "review_specs" / "surgical_autonomy.yaml"
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"
PID = 7


@pytest.fixture(scope="module")
def spec():
    return load_review_spec(SPEC_PATH)


@pytest.fixture
def db(tmp_path):
    rdb = ReviewDatabase("runlink", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(rdb.db_path).parent / "extraction_codebook.yaml")
    rdb._conn.execute("INSERT INTO papers (id, title, source, status, created_at, "
                      "updated_at) VALUES (?, 't', 's', 'FT_ELIGIBLE', 'n', 'n')",
                      (PID,))
    # FT_ELIGIBLE, not live's AI_AUDIT_COMPLETE: the legacy status write that
    # follows a stored extraction refuses AI_AUDIT_COMPLETE -> EXTRACTED, and
    # that write retires with the event writer (2(c)). Reported in 9b-2b.
    seed_eligibility(rdb._conn, PID)
    write_parsed(rdb, PID, "The paper reports a trial. A clean sentence.\n")
    rdb._conn.commit()
    yield rdb
    rdb.close()


class _Client:
    """`ollama.Client` stand-in: pass 1 then a complete pass 2, no snippet retry."""

    def __init__(self, codebook_path):
        cb = load_codebook(codebook_path)
        self._pass2 = ExtractionOutput(fields=[
            EvidenceSpan(field_name=f["name"], value="NR",
                         source_snippet=f"Snippet for {f['name']}.",
                         confidence=0.9, tier=f["tier"]) for f in cb.fields
        ]).model_dump_json()
        self.calls = []
        self._client = SimpleNamespace(base_url="http://runlink.invalid")

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
            prompt_eval_count=max(1, int(chars * 0.3)), done_reason="stop",
            eval_count=10)


@pytest.fixture
def fake_ollama(monkeypatch, db):
    import httpx

    def _refuse(*a, **kw):
        raise AssertionError("a run-link test attempted a real HTTP request")
    monkeypatch.setattr(httpx.Client, "send", _refuse)
    client = _Client(Path(db.db_path).parent / "extraction_codebook.yaml")
    monkeypatch.setattr(oc, "_client", client)
    # T5: the run never fetches a digest of its own (R117).
    monkeypatch.setattr(oc, "fetch_model_digest",
                        lambda m: pytest.fail(f"fetch_model_digest({m!r}) during a run"))
    oc.clear_ceiling_cache()
    yield client
    oc.clear_ceiling_cache()


def _run(db, spec, run_id, **kw):
    with patch("engine.utils.ollama_preflight.require_preflight"):
        return E.run_extraction(db, spec, "runlink", experiment_lock=False,
                                restart_every=0, run_id=run_id, **kw)


def _count(db, table):
    return db._conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


# ── T1 ────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("call", [
    lambda db, spec: E.run_extraction(db, spec, "r"),
    lambda db, spec: E.extract_paper(PID, "text", spec, db),
    lambda db, spec: E.extract_paper_with_completeness(PID, "text", spec, db),
])
def test_t1_run_id_is_required(db, spec, call):
    with pytest.raises(TypeError, match="run_id"):
        call(db, spec)


# ── T2 ────────────────────────────────────────────────────────────────
def test_t2_an_unknown_run_refuses_before_selection(db, spec):
    with patch.object(E, "select_for_extraction",
                      side_effect=AssertionError("selected")) as sel, \
         patch("engine.utils.ollama_preflight.require_preflight",
               side_effect=AssertionError("preflight reached")):
        with pytest.raises(rm.StageNotInRun, match="run 999"):
            E.run_extraction(db, spec, "r", experiment_lock=False, run_id=999)
    sel.assert_not_called()


# ── T3 ────────────────────────────────────────────────────────────────
def test_t3_a_digest_disagreeing_with_the_pin_refuses_before_selection(db, spec,
                                                                        fake_ollama):
    first = open_extraction_run(db, spec)            # pins the arm to FIXTURE_DIGEST
    other = "b" * 64
    # Below the writer: a second run whose stage rows disagree with the pin. A
    # real open_run cannot produce it — ArmPinMismatch refuses at open (R10).
    c = db._conn
    c.execute("INSERT INTO run_manifests (run_uid, review_id, run_kind, git_commit, "
              "git_dirty, spec_hash, codebook_hash, codebook_sha256, library_versions_json, "
              "host, started_at, manifest_json, manifest_sha256) SELECT 'mismatch', "
              "review_id, run_kind, git_commit, git_dirty, spec_hash, codebook_hash, "
              "codebook_sha256, library_versions_json, host, started_at, manifest_json, "
              "manifest_sha256 FROM run_manifests WHERE run_id = ?", (first,))
    second = c.execute("SELECT run_id FROM run_manifests WHERE run_uid = 'mismatch'"
                       ).fetchone()[0]
    c.execute("INSERT INTO run_stage_configs (run_id, stage, stage_kind, arm_name, "
              "provider, model_name, model_digest, options_json, options_hash, "
              "sent_keys_json, sources_json, keep_alive, format_schema_hash, prompt_hash) "
              "SELECT ?, stage, stage_kind, arm_name, provider, model_name, ?, options_json, "
              "options_hash, sent_keys_json, sources_json, keep_alive, format_schema_hash, "
              "prompt_hash FROM run_stage_configs WHERE run_id = ?", (second, other, first))
    c.commit()
    with patch.object(E, "select_for_extraction",
                      side_effect=AssertionError("selected")) as sel:
        with pytest.raises(rm.ArmPinMismatch) as err:
            _run(db, spec, second)
    sel.assert_not_called()
    assert other in str(err.value) and FIXTURE_DIGEST in str(err.value)
    assert "'extract_pass1'" in str(err.value)
    assert _count(db, "run_calls") == 0 and _count(db, "extractions") == 0
    assert fake_ollama.calls == []


# ── T4 ────────────────────────────────────────────────────────────────
def test_t4_every_extractor_call_records_its_paper(db, spec, fake_ollama):
    run_id = open_extraction_run(db, spec)
    stats = _run(db, spec, run_id)
    assert stats["extracted"] == 1
    rows = db._conn.execute("SELECT run_id, stage, paper_id FROM run_calls "
                            "ORDER BY call_id").fetchall()
    assert [tuple(r) for r in rows] == [(run_id, "extract_pass1", PID),
                                        (run_id, "extract_pass2", PID)]


def test_t4_the_snippet_retry_records_its_paper(db, spec, monkeypatch):
    """The third extractor site (:400) passes paper_id too."""
    run_id = open_extraction_run(db, spec)
    seen = {}
    monkeypatch.setattr(E, "ollama_chat",
                        lambda **kw: seen.update(kw) or SimpleNamespace(
                            message=SimpleNamespace(content="{}")))
    E._retry_snippet("f", "v", "text", PID,
                     cfg=stage_config("extract_retry_snippet", spec))
    assert seen["paper_id"] == PID


# ── T5 ────────────────────────────────────────────────────────────────
def test_t5_the_stored_digests_are_the_manifests(db, spec, fake_ollama):
    extractor_model = stage_config("extract_pass1", spec).model
    handle = rm.open_run(
        db._conn, spec, kind="extraction",
        stages=[*E.extraction_stages(spec), "audit"],
        codebook=load_codebook_beside(db.db_path),
        digest_fn=lambda m: "e" * 64 if m == extractor_model else "d" * 64,
        git=rm.GitState(commit="0" * 40, dirty=False, tag=None), host="fixture")
    _run(db, spec, handle.run_id)
    # 9b-FLIP: the digest rides on the claims (actor_digest), not on a legacy row.
    digests = {r[0] for r in db._conn.execute(
        "SELECT actor_digest FROM field_events WHERE paper_id = ? AND event_type = "
        "'asserted'", (PID,))}
    assert digests == {rm.stage_digest(db._conn, handle.run_id, "extract_pass1")} \
        == {"e" * 64}
    assert _count(db, "extractions") == 0


# test_t5_no_audit_stage_means_no_auditor_digest retired 2026-09-25 (9b-FLIP, R47):
# the auditor digest was stored only on the legacy extractions row, which the
# cut-over no longer writes; an audit's digest is its run's audit stage.


def test_t5_the_extractor_names_no_digest_fetch():
    assert "fetch_model_digest" not in Path(E.__file__).read_text()


# ── T6 ────────────────────────────────────────────────────────────────
def test_t6_extract_paper_hands_the_unit_map_dir_name_to_the_elicited_path(
        db, spec, monkeypatch):
    seen = {}
    monkeypatch.setattr("engine.elicitation.pipeline.extract_paper_elicited",
                        lambda *a, **kw: seen.update(kw))
    elicited = SimpleNamespace(extraction_models=SimpleNamespace(elicitation=True))
    E.extract_paper(PID, "text", elicited, db, unit_map_dir_name="run_X", run_id=1)
    assert seen["unit_map_dir_name"] == "run_X"
    # 9b-2c R3: the manifest run_id also goes down, for the record a refusal carries.
    assert seen["run_id"] == 1


# test_a_second_run_with_the_same_run_id_selects_the_same_set retired 2026-09-25
# (9b-FLIP R8, R47): 2(b)'s narrowed idempotence, superseded by T13's full property.


# ── T13 (9b-2c): the full idempotence property, owed by the flip ─────
def test_t13_a_second_run_under_the_same_input_selects_nothing(db, spec, fake_ollama):
    """9b-2c T13, un-xfailed by the cut-over (G4): the first run stamped the reuse
    key on its claims, so a second run under the same arm selects nothing and
    writes no field event."""
    run_id = open_extraction_run(db, spec)
    _run(db, spec, run_id)
    n = _count(db, "field_events")
    again = E.select_for_extraction(db._conn, arm=spec.extraction_models.arm)
    assert again.to_extract == ()
    assert again.skipped_asserted == (PID,)
    assert _run(db, spec, run_id)["extracted"] == 0
    assert _count(db, "field_events") == n
