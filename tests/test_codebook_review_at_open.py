"""P1 unit U10 (R612): a codebook that names another review is refused at run
open (INT-i12-9).

The codebook's `review` key was compared with a review id only inside
`load_codebook_for`. Every loader on the run path takes the codebook by path —
beside the database — and checked no identity, so a mis-copied codebook beside
a database (R235's hand edit: "`review_id` / `review:` changed and nothing
else") was used unrefused, and the manifest recorded its hashes under the
spec's review id. `open_run` now refuses a run whose codebook's `review` is not
`spec.review_id`, or is empty, before it writes anything.

Scratch databases under `tmp_path`. Nothing here reaches a model, a service or
the live review.
"""

from __future__ import annotations

import dataclasses
import shutil
import sqlite3
from pathlib import Path

import pytest

import scripts.run_pipeline as rp
from engine.agents import extractor as E
from engine.core import run_manifest as rm
from engine.core.codebook import clear_cache, load_codebook, load_codebook_beside
from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec
from engine.utils import ollama_client as oc

REPO = Path(__file__).resolve().parent.parent
SPEC_PATH = REPO / "review_specs" / "surgical_autonomy.yaml"
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"
CLEAN = rm.GitState(commit="b" * 40, dirty=False, tag=None)
DIGEST = "a" * 64
OURS, OTHER = "surgical_autonomy", "another_review"

pytestmark = pytest.mark.skipif(not LIVE_CODEBOOK.exists(), reason="codebook not available")


@pytest.fixture(scope="module")
def spec():
    s = load_review_spec(SPEC_PATH)
    assert s.review_id == OURS
    return s


def _write_codebook(review_dir: Path, review: str) -> Path:
    text = LIVE_CODEBOOK.read_text()
    assert text.count(f'review: "{OURS}"') == 1
    path = review_dir / "extraction_codebook.yaml"
    path.write_text(text.replace(f'review: "{OURS}"', f'review: "{review}"'))
    clear_cache()
    return path


@pytest.fixture
def review(tmp_path):
    """A scratch review whose codebook declares ANOTHER review — the file R235's
    copy step leaves when the `review:` edit is wrong or was never made."""
    db = ReviewDatabase("rev", data_root=tmp_path)
    _write_codebook(Path(db.db_path).parent, OTHER)
    yield db
    db.close()
    clear_cache()


def _rows(db_path) -> tuple[int, int, int]:
    conn = sqlite3.connect(str(db_path))
    try:
        return tuple(conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                     for t in ("run_manifests", "arms", "run_stage_configs"))
    finally:
        conn.close()


def _open(db, spec, cb, **kw):
    return rm.open_run(db._conn, spec, kind="extraction",
                       stages=[*E.extraction_stages(spec), "audit"], codebook=cb,
                       git=CLEAN, digest_fn=lambda m: DIGEST, **kw)


# ── gate 1 ────────────────────────────────────────────────────────────
def test_open_run_refuses_a_codebook_beside_the_database_that_declares_another_review(
        review, spec):
    cb = load_codebook_beside(review.db_path)
    assert cb.review == OTHER                      # it loads: no loader on this path checks
    with pytest.raises(rm.CodebookReviewMismatch) as exc:
        _open(review, spec, cb)
    assert isinstance(exc.value, rm.RunRefused)
    assert OURS in str(exc.value) and OTHER in str(exc.value)
    assert str(cb.path) in str(exc.value)
    assert _rows(review.db_path) == (0, 0, 0)

    # a codebook that declares no review is refused too
    with pytest.raises(rm.CodebookReviewMismatch, match="declares no review"):
        _open(review, spec, dataclasses.replace(cb, review=""))
    assert _rows(review.db_path) == (0, 0, 0)

    # the matching codebook opens
    _write_codebook(Path(review.db_path).parent, OURS)
    handle = _open(review, spec, load_codebook_beside(review.db_path))
    assert _rows(review.db_path)[0] == 1 and handle.run_id == 1


def test_a_review_session_is_refused_the_same_way(review, spec):
    with pytest.raises(rm.CodebookReviewMismatch):
        rm.open_review_session(review._conn, spec, codebook=load_codebook_beside(review.db_path),
                               git=CLEAN)
    assert _rows(review.db_path) == (0, 0, 0)


# ── gate 2 ────────────────────────────────────────────────────────────
def test_run_pipelines_own_opener_refuses_before_any_row_or_fetch(review, spec, monkeypatch):
    monkeypatch.setattr(rm, "git_state", lambda: CLEAN)
    fetched: list[str] = []
    monkeypatch.setattr(oc, "fetch_model_digest", lambda m: fetched.append(m) or DIGEST)
    with pytest.raises(rm.CodebookReviewMismatch) as exc:
        rp._open_run_manifest(review, spec, rp.STAGES.index("extract"))
    assert OURS in str(exc.value) and OTHER in str(exc.value)
    assert _rows(review.db_path) == (0, 0, 0)
    assert fetched == []                           # refused before any digest is fetched


def test_run_cloud_extractions_run_arm_refuses_before_any_row_or_send(review, spec,
                                                                      monkeypatch, tmp_path):
    from scripts import run_cloud_extraction as cli
    arm = next(a.name for a in spec.arms if a.provider == "openai")
    text = SPEC_PATH.read_text()
    assert text.count("enabled_arms: []") == 1
    spec_path = tmp_path / "spec.yaml"
    spec_path.write_text(text.replace("enabled_arms: []", f"enabled_arms: [{arm}]"))
    monkeypatch.setattr(rm, "git_state", lambda: CLEAN)
    events: list[str] = []

    class Extractor:
        """Stands in for the provider transport: a connection and nothing sent."""

        def __init__(self, db_path, spec_path, arm_name):
            self._conn = sqlite3.connect(db_path)
            self.run_id = None

        def run(self, **kw):
            events.append("run")

        def close(self):
            events.append("close")
            self._conn.close()

    monkeypatch.setitem(cli.EXTRACTORS, "openai", Extractor)
    with pytest.raises(rm.CodebookReviewMismatch) as exc:
        cli.run_arm(arm, review.db_path, str(spec_path), None, None)
    assert OURS in str(exc.value) and OTHER in str(exc.value)
    assert events == ["close"]                     # never ran: nothing was sent
    assert _rows(review.db_path) == (0, 0, 0)


# ── the predicate (gate 3 lives in test_run_fault_predicate.py) ───────
def test_the_refusal_is_a_run_fault():
    from engine.core import extraction_events as X
    assert X.is_run_fault(rm.CodebookReviewMismatch("refused"))
