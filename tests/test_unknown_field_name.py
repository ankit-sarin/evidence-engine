"""R225/B15 — an unknown field name is refused under an active run.

`write_field_event` holds no codebook reference itself; the active run's
field-name set is stashed by `activate()`/`active_run()` (read once, at
activation, from the codebook beside the review db — never per event) and
read back through `run_manifest.active_field_names()`.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from engine.core import events
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
from engine.core.events import UnknownFieldName
from engine.core.review_paths import load_spec_for
from _event_store_fixture import FIXTURE_CONTEXT_SHA, claim_identity

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"

pytestmark = pytest.mark.skipif(not LIVE_CODEBOOK.exists(), reason="codebook not available")

CLEAN = rm.GitState(commit="b" * 40, dirty=False, tag=None)
DIGEST = "a" * 64


def _digest(model):
    return DIGEST


@pytest.fixture
def review(tmp_path):
    db = ReviewDatabase("unknown_field_name", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(db.db_path).parent / "extraction_codebook.yaml")
    cb = load_codebook(Path(db.db_path).parent / "extraction_codebook.yaml")
    db._conn.execute(
        "INSERT INTO papers (id, title, source, created_at, updated_at) "
        "VALUES (1, 't', 's', 'n', 'n')")
    db._conn.commit()
    yield db, cb
    db.close()


@pytest.fixture(scope="module")
def spec():
    return load_spec_for("surgical_autonomy")


def _open(db, spec, cb):
    return rm.open_run(db._conn, spec, kind="extraction", stages=("extract_pass1",),
                       codebook=cb, git=CLEAN, digest_fn=_digest)


def _write_claim(db, run_id, arm, field_name, paper_id=1):
    return events.write_field_event(
        db._conn, event_type="asserted", paper_id=paper_id, field_name=field_name,
        arm=arm, value="v", source_snippet="v", extraction_uid=events.mint_extraction_uid(),
        actor_kind="model", actor_role="extractor", actor_name="m",
        payload=claim_identity(arm, paper_id), run_id=run_id,
        presented_context_sha256=FIXTURE_CONTEXT_SHA)


# ── T1 — an unknown name under a run is refused, nothing written ──────
def test_T1_an_unknown_field_name_under_a_run_is_refused(review, spec):
    db, cb = review
    h = _open(db, spec, cb)
    arm = spec.extraction_models.arm
    # R225a (10a-C10): field_names is no longer an activate()/active_run()
    # keyword — open_run already registered cb's field set under h.run_id.
    with rm.active_run(db._conn, h.run_id):
        with pytest.raises(UnknownFieldName, match="study_desiggn"):
            _write_claim(db, h.run_id, arm, "study_desiggn")
    assert db._conn.execute("SELECT COUNT(*) FROM field_events").fetchone()[0] == 0


# ── T2 — every codebook name is accepted ───────────────────────────────
def test_T2_every_codebook_field_name_is_accepted(review, spec):
    db, cb = review
    h = _open(db, spec, cb)
    arm = spec.extraction_models.arm
    # R225a (10a-C10): field_names is no longer an activate()/active_run()
    # keyword — open_run already registered cb's field set under h.run_id.
    with rm.active_run(db._conn, h.run_id):
        for name in cb.field_names:
            _write_claim(db, h.run_id, arm, name)
    rows = db._conn.execute(
        "SELECT COUNT(DISTINCT field_name) FROM field_events").fetchone()[0]
    assert rows == len(cb.field_names)


# ── T3 — no active run is today's behaviour ────────────────────────────
def test_T3_no_active_run_is_todays_behaviour(review, spec):
    db, cb = review
    h = _open(db, spec, cb)
    # No rm.active_run(...) here — active_field_names() is None regardless
    # of what field_name names. A system event isolates this from C8's
    # extractor-only presented-context/input-identity checks.
    events.write_field_event(
        db._conn, event_type="citation_located", paper_id=1, field_name="not_a_real_field",
        arm=spec.extraction_models.arm, extraction_uid=events.mint_extraction_uid(),
        actor_kind="engine", actor_role="system", actor_name="locator",
        payload={"located": True}, run_id=h.run_id)
    assert db._conn.execute("SELECT COUNT(*) FROM field_events").fetchone()[0] == 1


# ── T4 — the refusal reaches reviewer events too ───────────────────────
def test_T4_a_reviewer_event_with_an_unknown_field_name_is_refused(review, spec):
    db, cb = review
    h = _open(db, spec, cb)
    arm = spec.extraction_models.arm
    uid = events.mint_extraction_uid()
    cid = events.make_claim_id(arm, uid, cb.field_names[0])
    # R225a (10a-C10): field_names is no longer an activate()/active_run()
    # keyword — open_run already registered cb's field set under h.run_id.
    with rm.active_run(db._conn, h.run_id):
        _write_claim(db, h.run_id, arm, cb.field_names[0])
        with pytest.raises(UnknownFieldName):
            events.write_field_event(
                db._conn, event_type="human_corrected", paper_id=1,
                field_name="typo_field", arm=arm, claim_id=cid, value="w",
                actor_kind="human", actor_role="reviewer", actor_name="PI",
                against_claims={cid}, run_id=h.run_id)
    assert [r[0] for r in db._conn.execute(
        "SELECT event_type FROM field_events ORDER BY event_id")] == ["asserted"]


# R225a (10a-C10) superseded this file's original design note: field_names
# was an activate()/active_run() keyword (opt-in per activation, R225/B15),
# and 10a-CLOSE's stop report found that engine/agents/extractor.py::
# run_extraction nests a second, unguarded `active_run(db._conn, run_id)`
# inside scripts/run_pipeline.py's already-activated run — silently
# resetting the field set for the whole extraction stage, since the old
# design stashed it in a ContextVar every activate() call overwrote. R225a
# replaces the per-activation keyword with a process-local registry keyed by
# run_id, written once by open_run — T1-T4 above are edited for the removed
# keyword only; tests/test_run_scoped_field_names.py (10a-C10) tests the
# registry directly, including the nested-activation production shape this
# file never covered.
