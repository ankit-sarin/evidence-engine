"""R225a (10a-C10, amending R225/B15) — the codebook field set is a property
of the run, registered once by `open_run` and keyed by `run_id`, not carried
per-activation.

10a-CLOSE's stop report found that `engine/agents/extractor.py::run_extraction`
nests `with rm.active_run(db._conn, run_id):` (no `field_names=`) inside
`scripts/run_pipeline.py`'s own already-activated run — and because the old
design stashed `field_names` in a ContextVar set fresh by every `activate()`
call, the inner, unguarded activation silently reset it to `None` for the
whole extraction stage. The registry design makes that structurally
impossible: there is nothing per-activation to reset.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from engine.core import events
from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.database import ReviewDatabase
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
    db = ReviewDatabase("run_scoped_field_names", data_root=tmp_path)
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


# ── T1 — open_run registers the fixture codebook's field set ──────────
def test_T1_open_run_registers_the_codebooks_field_set(review, spec):
    db, cb = review
    h = _open(db, spec, cb)
    expected = frozenset(cb.field_names)
    assert rm._RUN_FIELD_NAMES[h.run_id] == expected
    with rm.active_run(db._conn, h.run_id):
        assert rm.active_field_names() == expected


# ── T2 — nested active_run of the same run keeps the field set ────────
def test_T2_nested_active_run_of_the_same_run_keeps_the_field_set(review, spec):
    """run_pipeline's outer activation, then extractor.py's inner, unguarded
    `with rm.active_run(db._conn, run_id):` — the exact production shape."""
    db, cb = review
    h = _open(db, spec, cb)
    expected = frozenset(cb.field_names)
    with rm.active_run(db._conn, h.run_id):                  # run_pipeline's shape
        assert rm.active_field_names() == expected
        with rm.active_run(db._conn, h.run_id):                # extractor.py:932's shape
            assert rm.active_field_names() == expected
        assert rm.active_field_names() == expected             # restored, not cleared
    assert rm.active_field_names() is None


# ── T3 — an unknown name inside the nested activation is refused ──────
def test_T3_an_unknown_name_inside_the_nested_activation_is_refused(review, spec):
    """The production shape 10a-C9 never tested: a claim written from inside
    extractor.py's inner active_run block, nested inside run_pipeline's outer
    one — this is where every real extraction claim is written."""
    db, cb = review
    h = _open(db, spec, cb)
    arm = spec.extraction_models.arm
    with rm.active_run(db._conn, h.run_id):
        with rm.active_run(db._conn, h.run_id):
            with pytest.raises(events.UnknownFieldName):
                events.write_field_event(
                    db._conn, event_type="asserted", paper_id=1, field_name="bogus_field",
                    arm=arm, value="v", source_snippet="v",
                    extraction_uid=events.mint_extraction_uid(),
                    actor_kind="model", actor_role="extractor", actor_name="m",
                    payload=claim_identity(arm, 1), run_id=h.run_id,
                    presented_context_sha256=FIXTURE_CONTEXT_SHA)
    assert db._conn.execute("SELECT COUNT(*) FROM field_events").fetchone()[0] == 0


# ── T4 — a run activated without open_run has no entry, no check ──────
def test_T4_a_run_activated_without_open_run_has_no_entry_no_check(review, spec):
    """Matches tests/test_cut_over.py's test_t3_an_aborted_run_closes_as_
    aborted_with_reason, which monkeypatches _open_run_manifest to return a
    plain int run_id (M3) — a run_id open_run never registered."""
    db, _ = review
    never_registered_run_id = 999_999
    assert never_registered_run_id not in rm._RUN_FIELD_NAMES
    with rm.active_run(db._conn, never_registered_run_id):
        assert rm.active_field_names() is None
