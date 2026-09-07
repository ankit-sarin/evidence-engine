"""CORPUS-PRED-01 — the single corpus-eligibility authority and its three adopters.

The three sites agreed before this task only because `FT_ELIGIBLE`, `EXTRACTED`
and `HUMAN_AUDIT_COMPLETE` are empty in the live review, so no count could reveal
a divergence. Every test here therefore uses a fixture in which all four corpus
statuses are populated **and** the non-corpus ones are too — the world state the
live database cannot currently provide.
"""

from __future__ import annotations

import re
import sqlite3
import subprocess
from pathlib import Path

import pytest

from engine.core.corpus import CORPUS_STATUSES, corpus_status_sql, is_corpus_member
from engine.core.database import ALLOWED_TRANSITIONS, STATUSES
from tests._corpus_fixture import (
    FIXTURE_CARRIED,
    FIXTURE_CARRIED_NON_CORPUS,
    FIXTURE_STATUSES,
    STATUS_PROBE_IDS,
    build_fixture,
)

REPO = Path(__file__).resolve().parents[1]

# ── Pre-patch baselines, captured at HEAD 377b4ec by running the code below
#    against tests/_corpus_fixture.build_fixture() BEFORE any source edit.
#    Not hand-written: see CORPUS-PRED-01 report §2.
PREPATCH_SELECT_SAMPLE = [(39, 1000, 'short', 'rct', 'carried'), (386, 4500, 'medium', 'unknown', 'carried'), (466, 6000, 'long', 'bench', 'carried'), (498, 4750, 'medium', 'unknown', 'carried'), (547, 7750, 'long', 'rct', 'carried'), (629, 500, 'short', 'unknown', 'carried'), (691, 6750, 'long', 'bench', 'carried'), (694, 7500, 'long', 'unknown', 'carried'), (708, 1750, 'short', 'rct', 'carried'), (799, 6000, 'long', 'unknown', 'carried'), (1002, 1250, 'short', 'rct', 'new'), (1003, 1500, 'short', 'unknown', 'new'), (1006, 2250, 'short', 'rct', 'new'), (1007, 2500, 'short', 'unknown', 'new'), (1008, 2750, 'short', 'bench', 'new'), (1014, 4250, 'medium', 'rct', 'new'), (1015, 4500, 'medium', 'unknown', 'new'), (1018, 5250, 'medium', 'rct', 'new'), (1019, 5500, 'medium', 'unknown', 'new'), (1020, 5750, 'medium', 'bench', 'new'), (1021, 6000, 'long', 'unknown', 'new'), (1024, 6750, 'long', 'bench', 'new'), (1025, 7000, 'long', 'unknown', 'new'), (1026, 7250, 'long', 'rct', 'new'), (1027, 7500, 'long', 'unknown', 'new'), (1030, 8250, 'long', 'rct', 'new'), (1031, 8500, 'long', 'unknown', 'new'), (1032, 8750, 'long', 'bench', 'new'), (1036, 500, 'short', 'bench', 'new'), (1038, 1000, 'short', 'rct', 'new'), (1039, 1250, 'short', 'unknown', 'new'), (1042, 2000, 'short', 'rct', 'new'), (1043, 2250, 'short', 'unknown', 'new'), (1044, 2500, 'short', 'bench', 'new'), (1048, 3500, 'medium', 'bench', 'new'), (1050, 4000, 'medium', 'rct', 'new'), (1054, 5000, 'medium', 'rct', 'new'), (1055, 5250, 'medium', 'unknown', 'new'), (1056, 5500, 'medium', 'bench', 'new'), (1057, 5750, 'medium', 'unknown', 'new')]
PREPATCH_ELIGIBLE_IDS = [8, 12, 13, 14, 39, 386, 466, 498, 691, 694, 708, 1000, 1001, 1002, 1003, 1006, 1007, 1008, 1009, 1012, 1013, 1014, 1015, 1018, 1019, 1020, 1021, 1024, 1025, 1026, 1027, 1030, 1031, 1032, 1033, 1036, 1037, 1038, 1039, 1042, 1043, 1044, 1045, 1048, 1049, 1050, 1051, 1054, 1055, 1056, 1057]
PREPATCH_CLOUD_PENDING = [8, 12, 13, 14, 39, 386, 466, 498, 691, 694, 708, 1002, 1003, 1006, 1007, 1008, 1009, 1012, 1013, 1014, 1015, 1018, 1019, 1020, 1021, 1024, 1025, 1026, 1027, 1030, 1031, 1032, 1033, 1036, 1037, 1038, 1039, 1042, 1043, 1044, 1045, 1048, 1049, 1050, 1051, 1054, 1055, 1056, 1057]
PREPATCH_CLOUD_PROGRESS = {'completed': 2, 'remaining': 49, 'total_cost_usd': 3.0, 'total_papers': 51}


@pytest.fixture
def review_dir(tmp_path):
    return build_fixture(tmp_path / "review")


# ── T1 — the authority names exactly the four statuses ────────────────────

def test_authority_yields_exactly_the_four_corpus_statuses():
    assert CORPUS_STATUSES == (
        "FT_ELIGIBLE", "EXTRACTED", "AI_AUDIT_COMPLETE", "HUMAN_AUDIT_COMPLETE",
    )
    assert set(CORPUS_STATUSES) <= set(STATUSES)


def test_predicate_admits_only_those_four_across_the_whole_lifecycle():
    admitted = {s for s in STATUSES if is_corpus_member(s)}
    assert admitted == set(CORPUS_STATUSES)


def test_predicate_fails_closed_on_a_missing_paper():
    assert is_corpus_member(None) is False
    assert is_corpus_member("NOT_A_STATUS") is False


# ── T2 (1b path) — the guard that fires when the lifecycle grows ──────────

def test_the_lifecycle_status_set_is_pinned_whole():
    """A new status must be ruled on for corpus membership, not absorbed silently.

    CORPUS_STATUSES is a declared constant because ALLOWED_TRANSITIONS carries no
    success/failure marking (its forward closure from FT_ELIGIBLE is nine statuses,
    not four). That declaration is only safe while someone is forced to revisit it
    when the lifecycle changes, which is what this test does: adding a member to
    STATUSES turns it red until the new status is classified here.
    """
    assert STATUSES == FIXTURE_STATUSES
    assert len(STATUSES) == 15


def test_the_transition_graph_still_does_not_yield_the_four_by_closure():
    """Pins the reason the fallback path (Contract 1b) was taken.

    If someone later adds a success/failure marking to the state machine, this
    test fails and the authority should be re-derived from it (Contract 1a).
    """
    seen, stack = {"FT_ELIGIBLE"}, ["FT_ELIGIBLE"]
    while stack:
        for nxt in ALLOWED_TRANSITIONS.get(stack.pop(), ()):
            if nxt not in seen:
                seen.add(nxt)
                stack.append(nxt)
    assert seen != set(CORPUS_STATUSES)
    assert seen - set(CORPUS_STATUSES) == {
        "EXTRACT_FAILED", "FT_FLAGGED", "FT_SCREENED_OUT", "PARSED", "REJECTED",
    }


# ── T3 — predicate and SQL view agree on every status ─────────────────────

def test_predicate_and_sql_view_agree_on_one_paper_per_status(review_dir):
    conn = sqlite3.connect(review_dir / "review.db")
    sql, params = corpus_status_sql()
    by_sql = {r[0] for r in conn.execute(f"SELECT id FROM papers WHERE {sql}", params)}
    conn.close()

    by_predicate = {pid for status, pid in STATUS_PROBE_IDS.items()
                    if is_corpus_member(status)}
    # Restrict to the one-per-status probe ids; the pool shares those statuses.
    assert by_sql & set(STATUS_PROBE_IDS.values()) == by_predicate
    assert len(by_predicate) == 4


def test_sql_view_accepts_a_qualified_column(review_dir):
    conn = sqlite3.connect(review_dir / "review.db")
    sql, params = corpus_status_sql("p.status")
    n = conn.execute(
        f"SELECT COUNT(*) FROM papers p WHERE {sql}", params).fetchone()[0]
    conn.close()
    assert n == len(PREPATCH_ELIGIBLE_IDS)


# ── T4 — each adopting site selects the same ids pre/post patch ───────────

def test_site_schema_eval2_pool_query_unchanged(review_dir):
    from analysis.eval.schema_eval2 import select_sample  # noqa: F401  (import path)

    conn = sqlite3.connect(f"file:{review_dir / 'review.db'}?mode=ro", uri=True)
    sql, params = corpus_status_sql()
    now = sorted(r[0] for r in conn.execute(
        f"SELECT id FROM papers WHERE {sql}", params))
    conn.close()
    assert now == PREPATCH_ELIGIBLE_IDS


def _detached_extractor(review_dir):
    """A CloudExtractorBase with only the connection wired.

    __init__ loads a review spec and a codebook, neither of which this fixture
    has and neither of which the two SQL sites read. Bypassing it exercises the
    real method bodies — the point of the test — without that machinery.
    """
    from engine.cloud.base import CloudExtractorBase

    ext = object.__new__(CloudExtractorBase)
    ext._conn = sqlite3.connect(review_dir / "review.db")
    ext._conn.row_factory = sqlite3.Row
    return ext


def test_site_cloud_get_pending_papers_unchanged(review_dir):
    ext = _detached_extractor(review_dir)
    try:
        assert [r["paper_id"] for r in ext.get_pending_papers("openai")] == \
            PREPATCH_CLOUD_PENDING
    finally:
        ext._conn.close()


def test_site_cloud_get_progress_unchanged(review_dir):
    ext = _detached_extractor(review_dir)
    try:
        assert ext.get_progress("openai") == PREPATCH_CLOUD_PROGRESS
    finally:
        ext._conn.close()


def test_cloud_pending_excludes_papers_already_extracted_for_the_arm(review_dir):
    """Guards the parameter ORDER in the rewritten f-string query.

    The status binds now precede the arm bind; getting that order wrong would
    still return rows, just the wrong ones.
    """
    ext = _detached_extractor(review_dir)
    try:
        pending = {r["paper_id"] for r in ext.get_pending_papers("openai")}
        assert 1000 not in pending and 1001 not in pending
        assert {r["paper_id"] for r in ext.get_pending_papers("anthropic")} >= {1000, 1001}
    finally:
        ext._conn.close()


# ── T5 — the CARRIED_NON_CORPUS declaration ───────────────────────────────

def test_carried_declaration_passes_on_the_documented_three(review_dir):
    from analysis.eval.schema_eval2 import CARRIED_NON_CORPUS, select_sample

    assert CARRIED_NON_CORPUS == frozenset(FIXTURE_CARRIED_NON_CORPUS)
    sample = select_sample(review_dir)          # must not raise
    assert {p.paper_id for p in sample} >= set(FIXTURE_CARRIED)


def _set_status(review_dir, pid, status):
    conn = sqlite3.connect(review_dir / "review.db")
    conn.execute("UPDATE papers SET status = ? WHERE id = ?", (status, pid))
    conn.commit()
    conn.close()


def test_carried_declaration_raises_naming_a_fourth_offender(review_dir):
    from analysis.eval.schema_eval2 import (
        CarriedCorpusDeclarationError, select_sample,
    )

    _set_status(review_dir, 691, "FT_SCREENED_OUT")
    with pytest.raises(CarriedCorpusDeclarationError) as exc:
        select_sample(review_dir)
    msg = str(exc.value)
    assert "691" in msg and "FT_SCREENED_OUT" in msg
    assert "not declared" in msg


def test_carried_declaration_raises_when_a_declared_one_becomes_eligible(review_dir):
    from analysis.eval.schema_eval2 import (
        CarriedCorpusDeclarationError, select_sample,
    )

    _set_status(review_dir, 629, "AI_AUDIT_COMPLETE")
    with pytest.raises(CarriedCorpusDeclarationError) as exc:
        select_sample(review_dir)
    msg = str(exc.value)
    assert "629" in msg and "now pass it" in msg


def test_carried_declaration_names_both_directions_at_once(review_dir):
    from analysis.eval.schema_eval2 import (
        CarriedCorpusDeclarationError, select_sample,
    )

    _set_status(review_dir, 694, "FT_SCREENED_OUT")
    _set_status(review_dir, 799, "EXTRACTED")
    with pytest.raises(CarriedCorpusDeclarationError) as exc:
        select_sample(review_dir)
    msg = str(exc.value)
    assert "694" in msg and "799" in msg


def test_the_three_are_not_filtered_out_of_the_sample(review_dir):
    """Contract 4: a declaration, never a filter. They stay in."""
    from analysis.eval.schema_eval2 import select_sample

    assert set(FIXTURE_CARRIED_NON_CORPUS) <= {p.paper_id
                                               for p in select_sample(review_dir)}


# ── T6 — fresh draw for a fixed seed is unchanged ─────────────────────────

def test_fresh_draw_is_byte_identical_to_the_pre_patch_sample(review_dir):
    from analysis.eval.schema_eval2 import select_sample

    got = [(p.paper_id, p.chars, p.length_stratum, p.study_type, p.source)
           for p in select_sample(review_dir)]
    assert got == PREPATCH_SELECT_SAMPLE


def test_fresh_draw_is_stable_under_an_explicit_seed(review_dir):
    from analysis.eval.schema_eval2 import SEED, select_sample

    a = [p.paper_id for p in select_sample(review_dir, seed=SEED)]
    b = [p.paper_id for p in select_sample(review_dir, seed=SEED)]
    assert a == b == [r[0] for r in PREPATCH_SELECT_SAMPLE]


# ── T7 — no inline copy of the literal survives ───────────────────────────

def test_no_inline_four_status_literal_outside_the_authority():
    """The four-status set must appear as a literal only in engine/core/corpus.py.

    Out of scope by brief: extraction_validator.py (three statuses — a different
    predicate, "has an extraction to validate") and pdf_quality_html.py (adds
    PARSED, drops AI_AUDIT_COMPLETE). Neither is the corpus question. Tests are
    exempt: this file and the fixture pin the literal on purpose.
    """
    # All four names on one line, in any quoting or order-preserving spacing —
    # the shape an `IN (...)` list takes. Deliberately does NOT match the
    # multi-line STATUSES tuple in database.py (a definition, not a copy), nor
    # pdf_quality_html.py, whose list omits AI_AUDIT_COMPLETE because it answers
    # a different question. Scoped to Python: prose in docs/ is a record, not code.
    pattern = (r"FT_ELIGIBLE.*EXTRACTED.*AI_AUDIT_COMPLETE.*HUMAN_AUDIT_COMPLETE")
    out = subprocess.run(
        ["git", "grep", "-l", "-E", pattern, "--", "*.py"],
        cwd=REPO, capture_output=True, text=True,
    )
    hits = {line for line in out.stdout.split() if line}
    allowed = {
        "engine/core/corpus.py",
        "tests/test_corpus_authority.py",
        "tests/_corpus_fixture.py",
    }
    assert hits <= allowed, f"inline corpus-status literal survives in: {sorted(hits - allowed)}"


def test_the_three_adopting_sites_carry_no_status_literal():
    for rel in ("engine/cloud/base.py", "analysis/eval/schema_eval2.py"):
        text = (REPO / rel).read_text()
        assert "AI_AUDIT_COMPLETE" not in text, f"{rel} still names a status inline"
        assert "corpus_status_sql" in text, f"{rel} does not use the authority"
