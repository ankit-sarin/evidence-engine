"""Migrations 016 (schema) and 017 (seed) — EFFECTIVE-RESULT-02.

016 is ordinary: a fresh database gets the structure and a receipt. 017 is a
**data** migration and must never run on a fresh database, because it seeds from
*this* database's corpus, parsed texts, spec and codebook — a fresh one has
none of those and no review directory beside it.
"""

from __future__ import annotations

import importlib
import json
import sqlite3

import pytest

from engine.core import events
from engine.core.database import ReviewDatabase
from engine.core.effective import PRE_MANIFEST, effective_state
from engine.migrations import runner
from engine.tools.db_fingerprint import fingerprint

_016 = importlib.import_module("engine.migrations.016_event_store")
_017 = importlib.import_module("engine.migrations.017_seed_event_store")

SEED_TABLES = ("arms", "paper_events", "parsed_text_refs", "review_identities",
               "field_events")


@pytest.fixture
def fresh(tmp_path):
    db = ReviewDatabase("scratch_review", data_root=tmp_path)
    path = db.db_path
    db.close()
    return path


def _receipt_ids(path):
    conn = sqlite3.connect(str(path))
    try:
        return [r[0] for r in conn.execute(
            "SELECT migration_id FROM schema_migrations ORDER BY migration_id")]
    finally:
        conn.close()


# ── 016 ───────────────────────────────────────────────────────────────
def test_016_runs_on_a_fresh_database_and_leaves_a_receipt(fresh):
    assert "016_event_store" in _receipt_ids(fresh)


def test_016_creates_exactly_the_seven_event_store_tables(fresh):
    conn = sqlite3.connect(str(fresh))
    names = {r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    conn.close()
    assert {"arms", "field_events", "paper_events", "field_event_against",
            "field_event_against_decisions", "parsed_text_refs",
            "review_identities"} <= names


def test_016_is_idempotent_and_changes_no_fingerprint(fresh):
    before = fingerprint(fresh)["overall_sha256"]
    _016.run_migration(str(fresh))
    assert fingerprint(fresh)["overall_sha256"] == before


# ── 017 — never on a fresh database (G3) ──────────────────────────────
def test_017_is_skipped_on_a_fresh_database(fresh):
    result = runner.run(fresh)
    assert "017_seed_event_store" in result["skipped"]
    assert "017_seed_event_store" not in _receipt_ids(fresh)


def test_017_targets_are_all_empty_on_a_fresh_database(fresh):
    conn = sqlite3.connect(str(fresh))
    counts = {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
              for t in SEED_TABLES}
    conn.close()
    assert counts == dict.fromkeys(SEED_TABLES, 0), counts


def test_017_is_declared_a_data_migration_with_its_reason(fresh):
    assert runner.kind_of("017_seed_event_store") == "data"
    assert runner.kind_of("016_event_store") == "schema"


# ── 017 — the seed itself, on a synthetic database ────────────────────
@pytest.fixture
def seeded(fresh):
    """A fresh database with three corpus papers and two parsed texts."""
    conn = sqlite3.connect(str(fresh))
    conn.executemany(
        "INSERT INTO papers (id, title, source, status, created_at, updated_at) "
        "VALUES (?, ?, 't', ?, 'now', 'now')",
        [(1, "a", "FT_ELIGIBLE"), (2, "b", "EXTRACTED"),
         (3, "c", "AI_AUDIT_COMPLETE"), (4, "d", "ABSTRACT_SCREENED_OUT")])
    conn.commit()
    ids = _017.corpus_paper_ids(conn)
    rows = [(1, "p/1_v1.md", 1, 11), (1, "p/1_v2.md", 2, 12), (2, "p/2_v1.md", 1, 13)]
    result = _017.build_seed(
        conn, corpus_ids=ids, parsed_rows=rows,
        spec_identity={"path": "s.yaml", "file_sha256": "a" * 64,
                       "screening_hash": "b" * 64, "review_id": "scratch_review"},
        codebook_identity={"path": "c.yaml", "sha256": "c" * 64, "field_count": 20})
    return conn, result, ids


def test_017_seeds_eligibility_only_one_state_at_migration_event_per_corpus_paper(seeded):
    conn, result, ids = seeded
    assert ids == [1, 2, 3]               # the excluded paper is not a corpus member
    assert result["paper_events"] == 3
    rows = conn.execute("SELECT paper_id, to_state, from_state, event_type, "
                        "actor_kind, actor_role, payload_json FROM paper_events "
                        "ORDER BY paper_id").fetchall()
    for pid, to_state, from_state, etype, kind, role, payload in rows:
        assert to_state == "eligible"          # eligibility, never a processing fact
        assert from_state is None              # Q7
        assert etype == "state_at_migration"
        assert (kind, role) == ("engine", "system")
        assert json.loads(payload)["note"] == _017.SEED_NOTE


def test_017_seeds_no_paper_rows_papers_are_addressable_by_reference(seeded):
    conn, result, _ = seeded
    assert result["papers"] == 0
    assert conn.execute("SELECT COUNT(*) FROM papers").fetchone()[0] == 4


def test_017_seeds_every_parsed_text_version_making_no_choice(seeded):
    conn, result, _ = seeded
    assert result["parsed_text_refs"] == 3
    versions = conn.execute(
        "SELECT paper_id, parsed_text_version FROM parsed_text_refs "
        "ORDER BY paper_id, parsed_text_version").fetchall()
    assert versions == [(1, 1), (1, 2), (2, 1)]


def test_017_mints_its_own_parsed_text_uid_never_the_reusable_source_id(seeded):
    conn, _, _ = seeded
    uids, srcs = zip(*conn.execute(
        "SELECT parsed_text_uid, source_full_text_assets_id FROM parsed_text_refs"))
    assert len(set(uids)) == 3
    assert not (set(uids) & {str(s) for s in srcs})
    assert sorted(srcs) == [11, 12, 13]      # kept as provenance only


def test_017_registers_the_three_arms_pre_manifest_making_row_7_reachable(seeded):
    conn, result, _ = seeded
    assert result["arms"] == 3
    rows = conn.execute("SELECT arm_name, arm_kind, configuration_marker, retired_at "
                        "FROM arms ORDER BY arm_name").fetchall()
    assert [r[0] for r in rows] == sorted(_017.EXISTING_ARMS)
    for _, kind, marker, retired in rows:
        assert (kind, marker, retired) == ("model", PRE_MANIFEST, None)


def test_017_writes_three_identity_rows_spec_codebook_and_the_seed_marker(seeded):
    conn, result, _ = seeded
    assert result["review_identities"] == 3
    kinds = dict(conn.execute("SELECT kind, key FROM review_identities"))
    assert kinds == {"spec": "review_spec", "codebook": "extraction_codebook",
                     "seed": "017"}
    cb = json.loads(conn.execute(
        "SELECT value_json FROM review_identities WHERE kind='codebook'").fetchone()[0])
    assert cb["field_count"] == 20


def test_017_is_idempotent_a_second_direct_run_inserts_nothing(seeded):
    conn, first, ids = seeded
    before = {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
              for t in SEED_TABLES}
    again = _017.build_seed(
        conn, corpus_ids=ids, parsed_rows=[(1, "p/1_v1.md", 1, 11)],
        spec_identity={"x": 1}, codebook_identity={"y": 2})
    assert again["already_seeded"] is True
    assert all(again[t] == 0 for t in
               ("arms", "paper_events", "parsed_text_refs", "review_identities"))
    after = {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
             for t in SEED_TABLES}
    assert after == before


def test_017_writes_the_seed_marker_last_so_a_failure_leaves_nothing(fresh):
    """The append-only triggers make a partial seed uncleanable, so it is atomic."""
    conn = sqlite3.connect(str(fresh))
    conn.execute("INSERT INTO papers (id, title, source, status, created_at, "
                 "updated_at) VALUES (1, 'a', 't', 'FT_ELIGIBLE', 'now', 'now')")
    conn.commit()
    with pytest.raises(TypeError):        # fails at the LAST identity insert
        _017.build_seed(conn, corpus_ids=[1], parsed_rows=[(1, "p.md", 1, 1)],
                        spec_identity={"a": 1}, codebook_identity=object())
    assert not _017.already_seeded(conn)
    for t in ("arms", "paper_events", "parsed_text_refs", "review_identities"):
        assert conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] == 0
    conn.close()


def test_017_seeded_papers_read_back_through_effective_state(seeded):
    conn, _, ids = seeded
    for pid in ids:
        s = effective_state(conn, pid)
        # B5 rewrite (R29/R39): the seed writes ELIGIBILITY only, and after the
        # split that is now visible in the return value rather than implied.
        assert s.eligibility == "eligible"
        assert s.processing == "no_recorded_state"
        assert s.in_corpus and not s.analysis_ready
        assert s.eligibility_provenance["rule_row"] == 17
    unseeded = effective_state(conn, 4)
    assert unseeded.eligibility == "no_recorded_state"
    assert unseeded.processing == "no_recorded_state"


def test_017_derives_its_paths_from_the_db_path_with_no_review_literal(fresh):
    """Migration 003's defect: a source directory and default target both naming
    one review. 017 derives both from the db_path it is handed."""
    src = _017.run_migration.__doc__ or ""
    import inspect
    body = inspect.getsource(_017.run_migration)
    assert "surgical_autonomy" not in body
    assert "db_path.parent" in body and "review_root.name" in body
