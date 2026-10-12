"""P1 unit U05 (R612(h), Design A; R569(b)): the extraction arm is pinned by the
run that reaches the extract stage, not by the first manifest that declares it.

INT-g12-4 — a run that opens with an extraction stage and stops before extract
(a screen start halted at the adjudication gate) used to pin the arm, so a
configuration nobody had extracted under bound the arm for good. Now:

* at open, an unregistered local extraction arm is registered unpinned; a pinned
  one is compared over its whole tuple;
* at the extract stage, before anything is selected or sent, an unpinned arm is
  pinned with the tuple the run resolved at open, and a pinned one is compared
  over the whole tuple;
* cloud arms and `arms=` arms still pin at open.

Scratch databases under `tmp_path`; every stage but extract is stubbed and the
corpus is empty, so nothing reaches a model, a service or the live review.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
from pathlib import Path

import pytest

import scripts.run_pipeline as rp
from engine.agents import extractor as E
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.review_spec import CloudConfig, load_review_spec
from engine.utils import ollama_client as oc

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"

pytestmark = pytest.mark.skipif(not LIVE_CODEBOOK.exists(), reason="codebook not available")

CLEAN = rm.GitState(commit="b" * 40, dirty=False, tag=None)
DIGEST = "a" * 64
ARM = "local_deepseek_r1_32b"
REVIEW = "armpin"


@pytest.fixture(scope="module")
def spec():
    return load_review_spec(REPO / "review_specs" / "surgical_autonomy.yaml")


def _other(spec):
    """The same spec with one extraction option changed: a non-digest key."""
    return spec.model_copy(update={"extraction_models":
                                   spec.extraction_models.model_copy(update={"temperature": 0.2})})


def _arm(db_path) -> dict | None:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute("SELECT * FROM arms WHERE arm_name = ?", (ARM,)).fetchone()
        return None if row is None else dict(row)
    finally:
        conn.close()


def _runs(db_path) -> list[tuple]:
    conn = sqlite3.connect(str(db_path))
    try:
        return conn.execute("SELECT run_id, end_status, end_reason FROM run_manifests "
                            "ORDER BY run_id").fetchall()
    finally:
        conn.close()


# ── through run_pipeline ──────────────────────────────────────────────
@pytest.fixture
def pipeline(tmp_path, spec, monkeypatch):
    """`run_pipeline` on a scratch review with its real manifest opener and its
    real extract stage; search, screen, parse, audit and export are stubbed.
    Returns `(db_path, state)`: `state["spec"]` is the spec the next run loads,
    `state["gate_open"]` the adjudication gate, `state["at_parse"]` the arm row
    as the parse stage saw it."""
    first = ReviewDatabase(REVIEW, data_root=tmp_path)
    db_path = Path(first.db_path)
    shutil.copy2(LIVE_CODEBOOK, db_path.parent / "extraction_codebook.yaml")
    first.close()
    state = {"spec": spec, "gate_open": True, "at_parse": "not reached"}

    monkeypatch.setattr(rp, "load_spec_for", lambda name, path=None: state["spec"])
    monkeypatch.setattr(rp, "ReviewDatabase",
                        lambda name: ReviewDatabase(REVIEW, data_root=tmp_path))
    monkeypatch.setattr(rm, "git_state", lambda: CLEAN)
    monkeypatch.setattr(oc, "fetch_model_digest", lambda m: DIGEST)
    for stage in ("_stage_search", "_stage_screen", "_stage_audit", "_stage_export"):
        monkeypatch.setattr(rp, stage, lambda *a, **k: {})

    def parse(db, *a, **k):
        state["at_parse"] = _arm(db.db_path)
        return {}
    monkeypatch.setattr(rp, "_stage_parse", parse)
    monkeypatch.setattr(rp, "_advance_extraction_workflow", lambda conn: {})
    monkeypatch.setattr(rp, "is_adjudication_complete", lambda conn: state["gate_open"])
    monkeypatch.setattr(rp, "is_audit_review_complete", lambda conn: True)
    monkeypatch.setattr(rp, "get_current_blocker", lambda conn: {
        "stage_name": "FULL_TEXT_ADJUDICATION_COMPLETE", "next_step": "do the review"})
    return db_path, state


def test_a_screen_start_that_stops_at_the_gate_leaves_the_extraction_arm_unpinned(
        pipeline, spec):
    db_path, state = pipeline
    state["gate_open"] = False
    rp.run_pipeline(REVIEW, skip_to="screen")
    (run_1, status, reason), = _runs(db_path)
    assert (status, reason) == ("interrupted", rm.REASON_BLOCKED_ADJUDICATION)
    assert state["at_parse"] == "not reached"

    # the run declared the extraction stages, and the arm is registered but unpinned
    arm = _arm(db_path)
    assert arm is not None
    assert (arm["configuration_marker"], arm["pinned_run_id"], arm["pinned_sha256"]) == (
        None, None, None)

    # a later start at extract with a different extraction configuration opens and pins
    state["gate_open"], state["spec"] = True, _other(spec)
    rp.run_pipeline(REVIEW, skip_to="extract")
    runs = _runs(db_path)
    assert [r[1] for r in runs] == ["interrupted", "completed"]
    arm = _arm(db_path)
    assert (arm["configuration_marker"], arm["pinned_run_id"]) == ("pinned", runs[1][0])
    assert arm["pinned_run_id"] != run_1
    assert arm["pinned_sha256"] == rm.sha256_canonical(json.loads(arm["configuration_json"]))


def test_a_screen_start_that_reaches_extract_pins_there_and_a_differing_run_is_refused(
        pipeline, spec):
    db_path, state = pipeline
    rp.run_pipeline(REVIEW, skip_to="screen")
    (run_1, status, _), = _runs(db_path)
    assert status == "completed"
    # at the parse stage — after open, before extract — the arm was still unpinned
    assert state["at_parse"]["configuration_marker"] is None
    assert state["at_parse"]["pinned_run_id"] is None
    arm = _arm(db_path)
    assert (arm["configuration_marker"], arm["pinned_run_id"]) == ("pinned", run_1)

    # a second run resolving differently is refused, and nothing is written for it
    state["spec"] = _other(spec)
    with pytest.raises(rm.ArmPinMismatch, match="options_hash"):
        rp.run_pipeline(REVIEW, skip_to="extract")
    assert len(_runs(db_path)) == 1
    assert _arm(db_path) == arm


# ── at the manifest ───────────────────────────────────────────────────
@pytest.fixture
def review(tmp_path):
    db = ReviewDatabase(REVIEW, data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(db.db_path).parent / "extraction_codebook.yaml")
    cb = load_codebook(Path(db.db_path).parent / "extraction_codebook.yaml")
    yield db, cb
    db.close()


def _open(db, spec, cb, **kw):
    kw.setdefault("stages", [*E.extraction_stages(spec), "audit"])
    return rm.open_run(db._conn, spec, kind="extraction", codebook=cb, git=CLEAN,
                       digest_fn=lambda m: DIGEST, **kw)


def test_two_runs_opened_unpinned_the_first_to_reach_extract_pins_and_the_other_is_refused(
        review, spec):
    db, cb = review
    conn = db._conn
    other = _other(spec)
    h1 = _open(db, spec, cb)
    h2 = _open(db, other, cb)                        # opens: nothing is pinned yet
    assert _arm(db.db_path)["configuration_marker"] is None
    # the two differ in no digest
    digests = [conn.execute("SELECT stage, model_digest FROM run_stage_configs WHERE run_id = ? "
                            "AND arm_name = ? ORDER BY stage", (h.run_id, ARM)).fetchall()
               for h in (h1, h2)]
    assert digests[0] == digests[1]

    # the second-opened run reaches extract first, and pins
    assert E.verify_extraction_run(conn, other, h2.run_id) == DIGEST
    arm = _arm(db.db_path)
    assert (arm["configuration_marker"], arm["pinned_run_id"]) == ("pinned", h2.run_id)

    # the other is refused at extract, naming the key, and changes nothing
    with pytest.raises(rm.ArmPinMismatch, match="options_hash") as exc:
        E.verify_extraction_run(conn, spec, h1.run_id)
    assert "model_digest" not in str(exc.value)
    assert _arm(db.db_path) == arm
    # and a later run with the first configuration is now refused at open
    with pytest.raises(rm.ArmPinMismatch, match="options_hash"):
        _open(db, spec, cb)


def test_verifying_twice_in_one_run_writes_the_pin_once(review, spec):
    db, cb = review
    conn = db._conn
    h1 = _open(db, spec, cb)
    def rows_changed_by_verify(run_id) -> int:
        before = conn.total_changes
        assert E.verify_extraction_run(conn, spec, run_id) == DIGEST
        return conn.total_changes - before

    assert rows_changed_by_verify(h1.run_id) == 1     # the pin: one row of `arms`
    assert rows_changed_by_verify(h1.run_id) == 0     # the loop's second check
    # a later run on the same configuration compares equal and keeps the first pin
    h2 = _open(db, spec, cb)
    assert rows_changed_by_verify(h2.run_id) == 0
    arm = _arm(db.db_path)
    assert (arm["configuration_marker"], arm["pinned_run_id"]) == ("pinned", h1.run_id)
    tup = json.loads(arm["configuration_json"])
    assert set(tup["stages"]) == set(E.extraction_stages(spec))
    assert arm["pinned_sha256"] == rm.sha256_canonical(tup)


def test_a_run_that_declares_no_extraction_stage_registers_no_extraction_arm(review, spec):
    db, cb = review
    _open(db, spec, cb, stages=["audit"])
    assert _arm(db.db_path) is None


def test_a_cloud_arm_and_a_named_arm_still_pin_at_open(review, spec):
    from engine.cloud.base import PAYLOAD_DESCRIPTION
    db, cb = review
    conn = db._conn
    cloud = next(a.name for a in spec.arms if a.provider == "anthropic")
    enabled = spec.model_copy(update={"cloud": CloudConfig(enabled_arms=[cloud],
                                                           prices=spec.cloud.prices)})
    h = _open(db, enabled, cb, stages=(), cloud_arms=[cloud],
              payload_description=PAYLOAD_DESCRIPTION)
    assert tuple(conn.execute("SELECT configuration_marker, pinned_run_id FROM arms "
                              "WHERE arm_name = ?", (cloud,)).fetchone()) == ("pinned", h.run_id)
    # an arm named through `arms=` pins at open too, the local extraction arm included
    h2 = _open(db, spec, cb, arms=[ARM])
    arm = _arm(db.db_path)
    assert (arm["configuration_marker"], arm["pinned_run_id"]) == ("pinned", h2.run_id)


def test_an_unpinned_arm_that_already_holds_a_claim_is_refused_by_name(review, spec):
    """The freeze trigger refuses to pin an arm that holds a claim (R21). The
    event writer refuses such a claim since INT-l12-18, so the claim here is
    written with that refusal switched off — a row seeded below the writer. If
    one exists, the run refuses with a named `ArmPinMismatch`, not a bare
    IntegrityError."""
    from engine.core import events
    from _event_store_fixture import FIXTURE_CONTEXT_SHA, claim_identity
    db, cb = review
    conn = db._conn
    h = _open(db, spec, cb)
    conn.execute("INSERT INTO papers (id, title, source, status, created_at, updated_at) "
                 "VALUES (7, 't', 's', 'FT_ELIGIBLE', 'n', 'n')")
    from unittest.mock import patch
    with patch.object(events, "_refuse_claim_on_arm"):
        events.write_field_event(
            conn, event_type="asserted", paper_id=7, field_name="study_design", arm=ARM,
            value="RCT", source_snippet="RCT", actor_kind="model", actor_role="extractor",
            actor_name="m", payload=claim_identity(ARM, 7), run_id=h.run_id,
            presented_context_sha256=FIXTURE_CONTEXT_SHA)
    with pytest.raises(rm.ArmPinMismatch, match="cannot be pinned") as exc:
        E.verify_extraction_run(conn, spec, h.run_id)
    assert "new arm name" in str(exc.value)
    assert _arm(db.db_path)["configuration_marker"] is None
