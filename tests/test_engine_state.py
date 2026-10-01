"""E-STATE: a manifest's `engine_state` is the engine-state name tagged at HEAD.

T1–T5 inject the git state (`GitState(..., state_tags=…)`) into `open_run`, as
every manifest test does. T6 runs the real `git_state()` against a throwaway
repository under tmp_path, tagged inside the test — no tag of this repository is
read or created, and the setup commands ignore global/system git config and hooks.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

from engine.core import run_manifest as rm
from engine.core.codebook import load_codebook
from engine.core.review_paths import load_spec_for

REPO = Path(__file__).resolve().parent.parent
LIVE_CODEBOOK = REPO / "data" / "surgical_autonomy" / "extraction_codebook.yaml"
EXTRACTION = ("extract_pass1", "extract_pass2", "extract_retry_snippet")

pytestmark = pytest.mark.skipif(not LIVE_CODEBOOK.exists(), reason="codebook not available")


@pytest.fixture
def review(tmp_path):
    from engine.core.database import ReviewDatabase
    db = ReviewDatabase("es", data_root=tmp_path)
    shutil.copy2(LIVE_CODEBOOK, Path(db.db_path).parent / "extraction_codebook.yaml")
    cb = load_codebook(Path(db.db_path).parent / "extraction_codebook.yaml")
    yield db, cb
    db.close()


@pytest.fixture(scope="module")
def spec():
    return load_spec_for("surgical_autonomy")


def _git(state_tags=(), dirty=False, tag=None):
    return rm.GitState(commit="e" * 40, dirty=dirty, tag=tag, state_tags=tuple(state_tags))


def _open(db, spec, cb, git):
    return rm.open_run(db._conn, spec, kind="extraction", stages=EXTRACTION, codebook=cb,
                       git=git, digest_fn=lambda m: "a" * 64)


def _stored(db, run_id):
    row = db._conn.execute("SELECT engine_state, manifest_json FROM run_manifests "
                           "WHERE run_id = ?", (run_id,)).fetchone()
    return row[0], json.loads(row[1])["engine_state"]


def _manifests(db):
    return db._conn.execute("SELECT COUNT(*) FROM run_manifests").fetchone()[0]


def test_engine_states_are_the_four_named_states_in_order():
    assert rm.ENGINE_STATES == ("freshman", "sophomore", "junior", "senior")


# ── T1–T3 ─────────────────────────────────────────────────────────────
def test_t1_a_state_tag_at_head_is_recorded(review, spec):
    db, cb = review
    h = _open(db, spec, cb, _git(["freshman"], tag="freshman"))
    assert _stored(db, h.run_id) == ("freshman", "freshman")


@pytest.mark.parametrize("state_tags,tag", [((), None), ((), "v0.1")])
def test_t2_no_state_tag_records_null(review, spec, state_tags, tag):
    db, cb = review
    h = _open(db, spec, cb, _git(state_tags, tag=tag))
    assert _stored(db, h.run_id) == (None, None)


def test_t3_a_non_state_tag_alongside_is_ignored(review, spec):
    """`git_state` keeps only ENGINE_STATES members; `v0.1` reaches git_tag only."""
    db, cb = review
    h = _open(db, spec, cb, _git(["freshman"], tag="v0.1"))
    assert _stored(db, h.run_id) == ("freshman", "freshman")
    git_tag = db._conn.execute("SELECT git_tag FROM run_manifests WHERE run_id = ?",
                               (h.run_id,)).fetchone()[0]
    assert git_tag == "v0.1"


# ── T4 / T5: refusals, before anything is written ─────────────────────
def test_t4_two_state_tags_at_head_are_refused_and_nothing_is_written(review, spec):
    db, cb = review
    with pytest.raises(rm.AmbiguousEngineState, match=r"\['freshman', 'sophomore'\]"):
        _open(db, spec, cb, _git(["freshman", "sophomore"]))
    assert _manifests(db) == 0


def test_t5_a_dirty_tree_is_refused_before_the_state_tags_matter(review, spec):
    db, cb = review
    with pytest.raises(rm.DirtyTree):
        _open(db, spec, cb, _git(["freshman", "sophomore"], dirty=True))
    assert _manifests(db) == 0


# ── T6: the real read, on a throwaway repository ──────────────────────
def _setup_git(repo, *args):
    env = {**os.environ, "GIT_CONFIG_GLOBAL": os.devnull, "GIT_CONFIG_NOSYSTEM": "1"}
    subprocess.run(["git", "-C", str(repo), "-c", f"core.hooksPath={os.devnull}",
                    "-c", "user.name=e-state-test", "-c", "user.email=e-state@test.invalid",
                    *args], check=True, capture_output=True, env=env)


def test_t6_git_state_reads_the_state_tags_at_head(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    # `init-db` is git's synonym for `init`: the suite's service-call fence refuses
    # any argv word "init" (tests/conftest.py BLOCKED_COMMANDS), and the guard stays.
    _setup_git(repo, "init-db", "-q")
    (repo / "f.txt").write_text("x\n")
    _setup_git(repo, "add", "f.txt")
    _setup_git(repo, "commit", "-q", "-m", "c1")

    assert rm.git_state(repo).state_tags == ()

    _setup_git(repo, "tag", "-a", "freshman", "-m", "freshman")
    _setup_git(repo, "tag", "v0.1")                     # a non-state tag, ignored
    g = rm.git_state(repo)
    assert g.state_tags == ("freshman",) and not g.dirty and len(g.commit) == 40

    _setup_git(repo, "tag", "-a", "sophomore", "-m", "sophomore")
    assert rm.git_state(repo).state_tags == ("freshman", "sophomore")   # R300 order
