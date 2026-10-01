"""R224a — presented context: exposure, threading, and the writer refusal
(10a-C8-B).

`ollama_chat`'s `return_request_hash` opt-in (T1/T2), the extractor's threading
of that hash into `ExtractionRecord.presented_context_sha256` / `context_chain`
/ `snippet_contexts` (T3/T4), the elicited path's two-attempt chain (T5), the
writer's `ClaimWithoutPresentedContext` refusal (T6), the reader (T7, by
reference — no duplicate), and the cloud path's untouched hash (T8).
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from engine.agents.models import EvidenceSpan, ExtractionOutput
from engine.core import events
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook, load_codebook_for
from engine.core.completeness import expected_field_names
from engine.core.database import ReviewDatabase
from engine.core.events import ClaimWithoutPresentedContext, PAYLOAD_CONTEXT_CHAIN
from engine.core.review_paths import load_spec_for
from engine.utils import ollama_client as oc
from engine.utils.ollama_client import InputOverflow, ollama_chat
from _event_store_fixture import FIXTURE_CONTEXT_SHA, claim_identity, fixture_run

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"

pytestmark = pytest.mark.skipif(not LIVE_CODEBOOK.exists(), reason="codebook not available")

CLEAN = rm.GitState(commit="b" * 40, dirty=False, tag=None)
DIGEST = "a" * 64
EXTRACTION = ("extract_pass1", "extract_pass2", "extract_retry_snippet")
CBK = load_codebook_for("surgical_autonomy")


def _digest(model):
    return DIGEST


@pytest.fixture(autouse=True)
def _fixed_ceiling(monkeypatch):
    """The input-fit guard reads the model's trained context via `_client.show`,
    which a MagicMock client cannot answer meaningfully — fixed here, as
    tests/test_ollama_client.py does, since these tests are not about it."""
    monkeypatch.setattr(oc, "effective_ceiling", lambda model, options=None: 131_072)


@pytest.fixture
def review(tmp_path):
    db = ReviewDatabase("presented_context", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(db.db_path).parent / "extraction_codebook.yaml")
    cb = load_codebook(Path(db.db_path).parent / "extraction_codebook.yaml")
    yield db, cb
    db.close()


@pytest.fixture(scope="module")
def spec():
    return load_spec_for("surgical_autonomy")


def _open(db, spec, cb, **kw):
    kw.setdefault("stages", EXTRACTION)
    kw.setdefault("git", CLEAN)
    kw.setdefault("digest_fn", _digest)
    return rm.open_run(db._conn, spec, kind=kw.pop("kind", "extraction"), codebook=cb, **kw)


def _insert_paper(db, pid=1):
    db._conn.execute(
        "INSERT INTO papers (id, title, source, created_at, updated_at) "
        "VALUES (?, 't', 's', 'n', 'n')", (pid,))
    db._conn.commit()


def _fake_response(content, thinking="t", prompt_eval_count=8000):
    """`prompt_eval_count` well above the input-fit guard's drop floor for a
    live-length prompt (T1's tiny message doesn't need this; T3-T5 do)."""
    return SimpleNamespace(
        message=SimpleNamespace(content=content, thinking=thinking),
        done_reason="stop", prompt_eval_count=prompt_eval_count, eval_count=5,
    )


# ── T1 — ollama_chat's returned hash equals the recorded row ──────────
def test_T1_success_hash_equals_the_recorded_row(review, spec):
    db, cb = review
    h = _open(db, spec, cb)
    fake_client = MagicMock()
    fake_client.chat.return_value = _fake_response("{}")
    with patch.object(oc, "_client", fake_client):
        with rm.active_run(db._conn, h.run_id):
            response, req_hash = ollama_chat(
                model="deepseek-r1:32b", messages=[{"role": "user", "content": "hi"}],
                stage="extract_pass1", return_request_hash=True)
    row = db._conn.execute("SELECT request_hash FROM run_calls").fetchone()
    assert row["request_hash"] == req_hash
    assert response.message.content == "{}"


def test_T1_pre_call_refusal_hash_equals_the_recorded_row(review, spec, monkeypatch):
    """No return occurs (InputOverflow raises before the call) — the hash is
    verified against the row alone, computed independently of any return."""
    db, cb = review
    h = _open(db, spec, cb)
    monkeypatch.setattr(
        oc, "_check_input_fits",
        lambda model, messages, options, paper_label: (_ for _ in ()).throw(
            InputOverflow(model=model, chars=1, estimate_low=999_999, ceiling=1)))
    messages = [{"role": "user", "content": "hi"}]
    with rm.active_run(db._conn, h.run_id):
        with pytest.raises(InputOverflow):
            ollama_chat(model="deepseek-r1:32b", messages=messages,
                       stage="extract_pass1", return_request_hash=True)
    row = db._conn.execute("SELECT request_hash, outcome FROM run_calls").fetchone()
    assert row["outcome"] == "refused_input_overflow"
    assert row["request_hash"] == rm.request_hash({"model": "deepseek-r1:32b", "messages": messages})


def test_T1_exhaustion_hash_equals_the_recorded_row(review, spec):
    """Every retry fails; no response ever exists to return, so 'error' is
    recorded from the request hash computed once at the top of ollama_chat."""
    db, cb = review
    h = _open(db, spec, cb)
    fake_client = MagicMock()
    fake_client.chat.side_effect = RuntimeError("boom")
    messages = [{"role": "user", "content": "hi"}]
    with patch.object(oc, "_client", fake_client):
        with rm.active_run(db._conn, h.run_id):
            with pytest.raises(RuntimeError):
                ollama_chat(model="deepseek-r1:32b", messages=messages, stage="extract_pass1",
                           max_retries=0, return_request_hash=True)
    row = db._conn.execute("SELECT request_hash, outcome FROM run_calls").fetchone()
    assert row["outcome"] == "error"
    assert row["request_hash"] == rm.request_hash({"model": "deepseek-r1:32b", "messages": messages})


# ── T2 — every other caller is unchanged ───────────────────────────────
def test_T2_default_returns_the_bare_response(review, spec):
    db, cb = review
    h = _open(db, spec, cb)
    fake_client = MagicMock()
    fake_client.chat.return_value = _fake_response("{}")
    with patch.object(oc, "_client", fake_client):
        with rm.active_run(db._conn, h.run_id):
            result = ollama_chat(model="deepseek-r1:32b",
                                 messages=[{"role": "user", "content": "hi"}],
                                 stage="extract_pass1")
    assert not isinstance(result, tuple)
    assert result.message.content == "{}"


# ── T3/T4 — extract_paper threads the chain and the snippet map ───────
def _complete_pass2_fields():
    fields = []
    for tier in (1, 2, 3, 4):
        for f in CBK.fields_by_tier(tier):
            fields.append(EvidenceSpan(field_name=f["name"], value="NR",
                                       source_snippet=f"Snippet for {f['name']}.",
                                       confidence=0.9, tier=f["tier"]))
    return fields


def _paper(db, spec):
    from engine.core.parsed_text import resolve_parsed_text
    from _event_store_fixture import seed_eligibility
    from _parsed_text_fixture import write_parsed
    pid = 1
    db._conn.execute(
        "INSERT INTO papers (id, title, source, status, created_at, updated_at) "
        "VALUES (?, 't', 's', 'PARSED', 'n', 'n')", (pid,))
    seed_eligibility(db._conn, pid)
    write_parsed(db, pid, "This RCT used the STAR robot for autonomous suturing.")
    db._conn.commit()
    return pid, resolve_parsed_text(db._conn, pid)


def test_T3_every_claim_carries_pass2s_hash_and_the_full_chain(review, spec):
    db, cb = review
    h = _open(db, spec, cb)
    pid, ref = _paper(db, spec)

    fake_client = MagicMock()
    pass1_resp = _fake_response("draft", thinking="The paper reports an RCT.")
    pass2_resp = _fake_response(ExtractionOutput(fields=_complete_pass2_fields()).model_dump_json())
    fake_client.chat.side_effect = [pass1_resp, pass2_resp]

    from engine.agents.extractor import extract_paper
    with patch.object(oc, "_client", fake_client):
        with rm.active_run(db._conn, h.run_id):
            extract_paper(pid, "This RCT used the STAR robot for autonomous suturing.",
                          spec, db, parsed_text_ref=ref, run_id=h.run_id)

    calls = db._conn.execute(
        "SELECT stage, request_hash FROM run_calls ORDER BY call_id").fetchall()
    assert [c["stage"] for c in calls] == ["extract_pass1", "extract_pass2"]
    pass1_hash, pass2_hash = calls[0]["request_hash"], calls[1]["request_hash"]

    rows = db._conn.execute(
        "SELECT presented_context_sha256, payload_json FROM field_events "
        "WHERE event_type = 'asserted'").fetchall()
    assert rows, "at least one asserted claim was written"
    for r in rows:
        assert r["presented_context_sha256"] == pass2_hash
        chain = json.loads(r["payload_json"])[PAYLOAD_CONTEXT_CHAIN]
        assert chain == [pass1_hash, pass2_hash]


def test_T4_a_retried_snippet_carries_its_own_hash_a_clean_one_does_not(review, spec):
    db, cb = review
    h = _open(db, spec, cb)
    pid, ref = _paper(db, spec)

    fields = _complete_pass2_fields()
    # Force exactly one field's snippet to need a retry (ellipsis bridging).
    bad_name = fields[0].field_name
    fields[0] = EvidenceSpan(field_name=bad_name, value="NR",
                             source_snippet="Twenty participants... completed.",
                             confidence=0.9, tier=fields[0].tier)

    fake_client = MagicMock()
    pass1_resp = _fake_response("draft", thinking="The paper reports an RCT.")
    pass2_resp = _fake_response(ExtractionOutput(fields=fields).model_dump_json())
    retry_resp = _fake_response(json.dumps({"source_snippet": "Twenty participants completed it."}))
    fake_client.chat.side_effect = [pass1_resp, pass2_resp, retry_resp]

    from engine.agents.extractor import extract_paper
    with patch.object(oc, "_client", fake_client):
        with rm.active_run(db._conn, h.run_id):
            extract_paper(pid, "This RCT used the STAR robot for autonomous suturing.",
                          spec, db, parsed_text_ref=ref, run_id=h.run_id)

    calls = db._conn.execute(
        "SELECT stage, request_hash FROM run_calls ORDER BY call_id").fetchall()
    assert [c["stage"] for c in calls] == \
        ["extract_pass1", "extract_pass2", "extract_retry_snippet"]
    retry_hash = calls[2]["request_hash"]

    def _payload(field_name):
        row = db._conn.execute(
            "SELECT payload_json FROM field_events WHERE field_name = ? "
            "AND event_type = 'asserted'", (field_name,)).fetchone()
        return json.loads(row["payload_json"])

    assert _payload(bad_name)[events.PAYLOAD_SNIPPET_CONTEXT] == retry_hash
    other_name = fields[1].field_name
    assert events.PAYLOAD_SNIPPET_CONTEXT not in _payload(other_name)


# ── T5 — the elicited path's chain carries both pass-1 attempts ───────
# A COMPLETE codebook, matching tests/test_elicitation_pipeline.py's proven
# shape (CODEBOOK-AUTH-01 validates eagerly, so a partial fixture cannot load).
_ELICIT_CODEBOOK = {
    "version": "1.0", "review": "test_review", "date": "2026-01-01",
    "escape_token": "NO_EVIDENCE_LOCATABLE", "contract_unmet_token": "CONTRACT_UNMET",
    "absence_sentinels": ["NR", "NOT_FOUND"], "canonical_absence_sentinel": "NR",
    "fields": [
        {"name": "robot_platform", "type": "free_text", "field_class": "stated",
         "definition": "The robot.", "instruction": "Name it.",
         "judge_rubric_family": "free_text", "tier": 1},
    ],
}
_ELICIT_PAPER = "The system used a da Vinci Research Kit for the procedure."
# Empty unit_indices fails the citation contract (CONTRACT_UNMET); a real
# index succeeds (EVIDENCED_VALUE) — the same shape test_elicitation_pipeline.py
# pins for PASS1_BAD / PASS1_GOOD.
_PASS1_BAD = json.dumps({"fields": [
    {"field_name": "robot_platform", "unit_indices": [], "value": "da Vinci Research Kit"}]})
_PASS1_GOOD = json.dumps({"fields": [
    {"field_name": "robot_platform", "unit_indices": [1], "value": "da Vinci Research Kit"}]})
_PASS2_OUT = ExtractionOutput(fields=[
    EvidenceSpan(field_name="robot_platform", value="da Vinci Research Kit",
                 source_snippet="the model's own snippet, overwritten by the materializer",
                 confidence=0.9, tier=1),
]).model_dump_json()


class _ElicitDB:
    def __init__(self, path):
        self.db_path = str(path / "review.db")
        self.stored = None
        self._conn = self


class _ElicitSpec:
    review_id = "t"

    class extraction_models:
        elicitation = True


def test_T5_the_elicited_chain_carries_both_attempts_presented_is_pass2(tmp_path, monkeypatch):
    import engine.agents.extractor as E
    import engine.elicitation.pipeline as PL

    (tmp_path / "extraction_codebook.yaml").write_text(json.dumps(_ELICIT_CODEBOOK))
    monkeypatch.setattr(PL, "write_extraction_events",
                        lambda conn, record, **kw: setattr(conn, "stored", record))

    hashes = iter(["h-p1a", "h-p1b", "h-p2"])
    pass1_contents = iter([_PASS1_BAD, _PASS1_GOOD])  # attempt 1 fails, attempt 2 succeeds

    monkeypatch.setattr(PL, "ollama_chat",
                        lambda **kw: (_fake_response(next(pass1_contents), thinking=None),
                                     next(hashes)))
    monkeypatch.setattr(E, "ollama_chat",
                        lambda **kw: (_fake_response(_PASS2_OUT, thinking=None), next(hashes)))

    db = _ElicitDB(tmp_path)
    PL.extract_paper_elicited(7, _ELICIT_PAPER, _ElicitSpec(), db, unit_map_dir_name="run_T5")

    assert db.stored.context_chain == ("h-p1a", "h-p1b", "h-p2")
    assert db.stored.presented_context_sha256 == "h-p2"


def test_T5_pass2_skipped_presented_is_the_accepted_pass1_attempt(tmp_path, monkeypatch):
    """Both attempts fail the same field's contract (a tie keeps attempt 1,
    Ruling 4's rule) — pass 2 never runs, so the accepted attempt's own hash,
    not pass 2's, is what every terminal state carries."""
    import engine.elicitation.pipeline as PL

    (tmp_path / "extraction_codebook.yaml").write_text(json.dumps(_ELICIT_CODEBOOK))
    monkeypatch.setattr(PL, "write_extraction_events",
                        lambda conn, record, **kw: setattr(conn, "stored", record))

    hashes = iter(["h-p1a", "h-p1b"])
    monkeypatch.setattr(PL, "ollama_chat",
                        lambda **kw: (_fake_response(_PASS1_BAD, thinking=None), next(hashes)))

    db = _ElicitDB(tmp_path)
    result = PL.extract_paper_elicited(7, _ELICIT_PAPER, _ElicitSpec(), db,
                                       unit_map_dir_name="run_T5b")

    assert db.stored.context_chain == ("h-p1a", "h-p1b")
    assert db.stored.presented_context_sha256 == "h-p1a"
    # 12c-E-PIN-B-R2: pass 2 skipped, the stored trace is the (empty) priming
    # block, unchanged by the pass2_priming factoring. Recorded on the tree
    # before it.
    from engine.core.effective_config import sha256_canonical
    assert sha256_canonical(result.reasoning_trace) == SKIPPED_PASS2_TRACE_SHA256


SKIPPED_PASS2_TRACE_SHA256 = "12ae32cb1ec02d01eda3581b127c1fee3b0dc53572ed6baf239721a03d82e126"


# ── T6 — the writer refusal ─────────────────────────────────────────
def test_T6_no_presented_context_is_refused_nothing_written(review, spec):
    db, cb = review
    h = _open(db, spec, cb)
    arm = spec.extraction_models.arm
    with pytest.raises(ClaimWithoutPresentedContext):
        events.write_field_event(
            db._conn, event_type="asserted", paper_id=1, field_name="f", arm=arm,
            value="v", source_snippet="v", extraction_uid=events.mint_extraction_uid(),
            actor_kind="model", actor_role="extractor", actor_name="m",
            payload=claim_identity(arm, 1), run_id=h.run_id)
    assert db._conn.execute("SELECT COUNT(*) FROM field_events").fetchone()[0] == 0


def test_T6_an_empty_chain_is_refused(review, spec):
    db, cb = review
    h = _open(db, spec, cb)
    arm = spec.extraction_models.arm
    payload = {**claim_identity(arm, 1), PAYLOAD_CONTEXT_CHAIN: []}
    with pytest.raises(ClaimWithoutPresentedContext):
        events.write_field_event(
            db._conn, event_type="asserted", paper_id=1, field_name="f", arm=arm,
            value="v", source_snippet="v", extraction_uid=events.mint_extraction_uid(),
            actor_kind="model", actor_role="extractor", actor_name="m",
            payload=payload, run_id=h.run_id, presented_context_sha256="x" * 64)
    assert db._conn.execute("SELECT COUNT(*) FROM field_events").fetchone()[0] == 0


def test_T6_a_reviewer_event_is_exempt(review, spec):
    db, cb = review
    h = _open(db, spec, cb)
    _insert_paper(db)
    arm = spec.extraction_models.arm
    uid = events.mint_extraction_uid()
    events.write_field_event(
        db._conn, event_type="asserted", paper_id=1, field_name="f", arm=arm,
        value="v", source_snippet="v", extraction_uid=uid,
        actor_kind="model", actor_role="extractor", actor_name="m",
        payload=claim_identity(arm, 1), run_id=h.run_id,
        presented_context_sha256=FIXTURE_CONTEXT_SHA)
    cid = events.make_claim_id(arm, uid, "f")
    # No presented_context_sha256, no context_chain — a reviewer event is not
    # a claim (actor_role != 'extractor'), so it is not refused.
    events.write_field_event(
        db._conn, event_type="human_corrected", paper_id=1, field_name="f", arm=arm,
        value="w", claim_id=cid, actor_kind="human", actor_role="reviewer",
        actor_name="PI", against_claims={cid}, run_id=h.run_id)
    assert [r[0] for r in db._conn.execute(
        "SELECT event_type FROM field_events ORDER BY event_id")] == \
        ["asserted", "human_corrected"]


def test_T6_a_migration_marked_event_is_exempt(review, spec):
    """A migration seed writes claims below the writer's extractor checks
    entirely — `seed_claim` builds the row the way a migration would."""
    from _event_store_fixture import seed_claim
    db, cb = review
    _open(db, spec, cb)
    _insert_paper(db)
    events.register_arm(db._conn, "premanifest_seed", "model",
                        configuration_marker=events.PRE_MANIFEST)
    claim_id = seed_claim(db._conn, arm="premanifest_seed", paper_id=1, field_name="f",
                          value="5")
    row = db._conn.execute(
        "SELECT run_id, run_marker FROM field_events WHERE claim_id = ?",
        (claim_id,)).fetchone()
    assert row["run_id"] is None and row["run_marker"] == "pre-manifest"


# T7 — the reader is unchanged (R224a(7)); already tested against a non-null
# value at tests/test_effective_reader.py::test_row5_corrected_by_human_returns_the_reviewers_value
# (row 5, asserts provenance["presented_context_sha256"] == "ctx-human_corrected" —
# effective.py's other surfacing site, row 8's endorsement, is the same read),
# not duplicated here.


# ── T8 — the cloud path's row is unaffected ────────────────────────────
def test_T8_cloud_send_still_computes_its_own_hash_unaffected(review, spec):
    """A4: record_call falls back to computing request_hash(request) exactly
    as before this parameter existed, for any caller that omits the keyword —
    the cloud path's send() is untouched and always omits it."""
    db, cb = review
    h = _open(db, spec, cb, stages=("extract_pass1",))
    payload = {"model": "gpt-x", "input": "hello"}
    rm.record_call(db._conn, h.run_id, "extract_pass1", None, payload, None,
                   "t0", "t1", outcome="completed")
    row = db._conn.execute("SELECT request_hash FROM run_calls").fetchone()
    assert row["request_hash"] == rm.request_hash(payload)
