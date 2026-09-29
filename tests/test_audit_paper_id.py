"""R225/C31 — the audit stage's ollama_chat call carries paper_id.

`semantic_verify` previously called `ollama_chat` with no `paper_id`, so every
audit-stage `run_calls` row was unattributable to the paper it audited
(row C31; 9d-PA read-out §0 I2). This drives the real event-side auditor
(`audit_events.audit_run`) through a mocked `_client.chat`, so `ollama_chat`
runs for real and writes real `run_calls` rows.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from engine.agents import audit_events as AE
from engine.core import events
from engine.core import run_manifest as rm
from engine.core.database import ReviewDatabase
from engine.core.parsed_text import resolve_parsed_text
from engine.core.review_spec import load_review_spec
from engine.utils import ollama_client as oc
from _event_store_fixture import FIXTURE_CONTEXT_SHA, claim_identity, open_extraction_run, \
    seed_eligibility
from _parsed_text_fixture import write_parsed

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"
TEXT = ("The trial enrolled forty patients at two centres. The robot performed "
        "autonomous suturing on porcine tissue in ten trials. Outcomes were measured "
        "at six months.\n")
P7, P8 = 7, 8

pytestmark = pytest.mark.skipif(not LIVE_CODEBOOK.exists(), reason="codebook not available")


@pytest.fixture(scope="module")
def spec():
    return load_review_spec(REPO / "review_specs" / "surgical_autonomy.yaml")


@pytest.fixture
def db(tmp_path):
    rdb = ReviewDatabase("aud_pid", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(rdb.db_path).parent / "extraction_codebook.yaml")
    for pid in (P7, P8):
        rdb._conn.execute("INSERT INTO papers (id, title, source, status, created_at, "
                          "updated_at) VALUES (?, 't', 's', 'FT_ELIGIBLE', 'n', 'n')", (pid,))
        seed_eligibility(rdb._conn, pid)
        write_parsed(rdb, pid, TEXT)
    rdb._conn.commit()
    yield rdb
    rdb.close()


@pytest.fixture
def run_id(db, spec):
    return open_extraction_run(db, spec)


@pytest.fixture
def review_dir(db):
    return Path(db.db_path).parent


@pytest.fixture(autouse=True)
def _fixed_ceiling(monkeypatch):
    monkeypatch.setattr(oc, "effective_ceiling", lambda model, options=None: 131_072)


def _claim_unlocatable(db, spec, run_id, pid, field):
    """A snippet nothing in TEXT is close to — unlocated, so semantic_verify
    (and therefore an ollama_chat call) is the only way this claim resolves."""
    uid = events.mint_extraction_uid()
    ref = resolve_parsed_text(db._conn, pid)
    events.write_field_event(
        db._conn, event_type="asserted", paper_id=pid, field_name=field,
        arm=spec.extraction_models.arm, value="Norway", source_snippet="Conducted in Norway.",
        extraction_uid=uid, actor_kind="model", actor_role="extractor",
        actor_name="deepseek-r1:32b",
        payload=claim_identity(spec.extraction_models.arm, pid, sha=ref.sha256,
                               uid=ref.parsed_text_uid), run_id=run_id,
        presented_context_sha256=FIXTURE_CONTEXT_SHA)
    return events.make_claim_id(spec.extraction_models.arm, uid, field)


def _fake_response():
    return SimpleNamespace(
        message=SimpleNamespace(
            content='{"status": "flagged", "grep_found": false, "reasoning": "not supported"}',
            thinking=None),
        done_reason="stop", prompt_eval_count=200, eval_count=20,
    )


# ── T6 — every audit-stage run_calls row carries its paper's id ────────
def test_T6_every_audit_call_row_carries_the_paper_it_verified(db, spec, run_id, review_dir):
    arm = spec.extraction_models.arm
    c7 = _claim_unlocatable(db, spec, run_id, P7, "country")
    c8 = _claim_unlocatable(db, spec, run_id, P8, "country")

    fake_client = MagicMock()
    fake_client.chat.return_value = _fake_response()
    with patch.object(oc, "_client", fake_client):
        with rm.active_run(db._conn, run_id):
            AE.audit_run(db._conn, spec, run_id=run_id, arm=arm, review_dir=review_dir)

    rows = db._conn.execute(
        "SELECT paper_id FROM run_calls WHERE stage = 'audit' ORDER BY call_id").fetchall()
    assert {r[0] for r in rows} == {P7, P8}

    verdict_papers = {r[0] for r in db._conn.execute(
        "SELECT paper_id FROM audit_verdicts WHERE claim_id IN (?, ?)", (c7, c8))}
    assert verdict_papers == {P7, P8}
    assert {r[0] for r in rows} == verdict_papers


# ── T7 — no other stage's rows are touched ─────────────────────────────
def test_T7_no_other_stage_is_affected(db, spec, run_id, review_dir):
    """T6 exercises only the audit stage; confirm the pre-existing extract
    stages' rows (there are none written here) and the stage set on
    run_stage_configs are unaffected — the change is local to semantic_verify."""
    stages_before = {r[0] for r in db._conn.execute(
        "SELECT DISTINCT stage FROM run_stage_configs WHERE run_id = ?", (run_id,))}
    arm = spec.extraction_models.arm
    _claim_unlocatable(db, spec, run_id, P7, "country")

    fake_client = MagicMock()
    fake_client.chat.return_value = _fake_response()
    with patch.object(oc, "_client", fake_client):
        with rm.active_run(db._conn, run_id):
            AE.audit_run(db._conn, spec, run_id=run_id, arm=arm, review_dir=review_dir)

    stages_after = {r[0] for r in db._conn.execute(
        "SELECT DISTINCT stage FROM run_calls WHERE run_id = ?", (run_id,))}
    assert stages_after == {"audit"}
    assert stages_before == {r[0] for r in db._conn.execute(
        "SELECT DISTINCT stage FROM run_stage_configs WHERE run_id = ?", (run_id,))}
