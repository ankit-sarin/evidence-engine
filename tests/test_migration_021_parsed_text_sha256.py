"""Migration 021 — parsed_text_refs gains parsed_text_sha256 (INPUT-IDENTITY-01, gate G3).

Fresh databases through the runner; databases WITH rows built at the pre-021
(016) shape in a temp directory, with their parsed-text files under a temp
"repo root" and a baseline JSON written beside them. Nothing here opens a real
review database (the conftest fence would refuse it).
"""

from __future__ import annotations

import hashlib
import importlib
import json
import shutil
import sqlite3
from pathlib import Path

import pytest

from engine.core.database import ReviewDatabase
from engine.migrations import runner

m016 = importlib.import_module("engine.migrations.016_event_store")
m021 = importlib.import_module("engine.migrations.021_parsed_text_sha256")

MID = "021_parsed_text_sha256"


def _sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


# ── fresh databases ───────────────────────────────────────────────────
@pytest.fixture
def fresh(tmp_path):
    db = ReviewDatabase("fresh_021", data_root=tmp_path)
    path = db.db_path
    db.close()
    return path


def _table_sql(path) -> str:
    c = sqlite3.connect(str(path))
    try:
        return c.execute("SELECT sql FROM sqlite_master WHERE type='table' "
                         "AND name='parsed_text_refs'").fetchone()[0]
    finally:
        c.close()


def test_021_executes_on_a_fresh_database_with_an_executed_receipt(fresh):
    c = sqlite3.connect(str(fresh))
    mode = c.execute("SELECT mode FROM schema_migrations WHERE migration_id = ?",
                     (MID,)).fetchone()
    c.close()
    assert mode == ("executed",)
    assert "parsed_text_sha256" in _table_sql(fresh)


def test_021_second_run_is_already(fresh):
    assert MID in runner.run(fresh)["already"]
    assert m021.run_migration(str(fresh))["status"] == "already_applied"


def test_021_is_declared_schema_kind():
    assert runner.kind_of(MID) == "schema"


def test_021_the_unique_makes_a_version_tie_impossible(fresh):
    c = sqlite3.connect(str(fresh))
    c.execute("INSERT INTO papers (id, title, source, status, created_at, updated_at) "
              "VALUES (1, 't', 's', 'PARSED', 'n', 'n')")
    ins = ("INSERT INTO parsed_text_refs (parsed_text_uid, paper_id, parsed_text_path, "
           "parsed_text_version, recorded_at, parsed_text_sha256) VALUES (?, 1, ?, 1, 'n', ?)")
    c.execute(ins, ("u1", "a.md", "0" * 64))
    with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
        c.execute(ins, ("u2", "b.md", "1" * 64))
    c.close()


@pytest.mark.parametrize("value, ok", [
    (None, False),                 # the NULL reproducer (rule 11)
    ("a" * 64, True),
    ("0123456789abcdef" * 4, True),
    ("A" * 64, False),             # upper case is not the canonical form
    ("a" * 63, False),
    ("a" * 65, False),
    ("a" + "z" * 63, False),       # passes GLOB '[0-9a-f]*'; the CHECK refuses it
    ("", False),
])
def test_021_hash_check_permits_only_64_lowercase_hex(fresh, value, ok):
    c = sqlite3.connect(str(fresh))
    c.execute("INSERT INTO papers (id, title, source, status, created_at, updated_at) "
              "VALUES (1, 't', 's', 'PARSED', 'n', 'n')")
    stmt = ("INSERT INTO parsed_text_refs (parsed_text_uid, paper_id, parsed_text_path, "
            "parsed_text_version, recorded_at, parsed_text_sha256) VALUES ('u', 1, 'p', 1, 'n', ?)")
    if ok:
        c.execute(stmt, (value,))
    else:
        with pytest.raises(sqlite3.IntegrityError):
            c.execute(stmt, (value,))
    c.close()


# ── R87: the append-only guard, restored verbatim ─────────────────────
def _016_trigger_text() -> dict[str, str]:
    """What 016 leaves in sqlite_master: its own text minus IF NOT EXISTS."""
    raw = m016._append_only("parsed_text_refs").replace("IF NOT EXISTS ", "")
    stmts = [s.strip() for s in raw.split("END;") if s.strip()]
    return {s.split()[2]: s + " END" for s in stmts}


def test_021_triggers_equal_016s_text_character_for_character(fresh):
    c = sqlite3.connect(str(fresh))
    got = dict(c.execute("SELECT name, sql FROM sqlite_master WHERE type='trigger' "
                         "AND tbl_name='parsed_text_refs'").fetchall())
    c.close()
    assert got == _016_trigger_text()


def test_021_one_row_refusal_on_a_throwaway_copy(fresh, tmp_path):
    copy = tmp_path / "throwaway.db"
    shutil.copy(fresh, copy)
    c = sqlite3.connect(str(copy))
    c.execute("INSERT INTO papers (id, title, source, status, created_at, updated_at) "
              "VALUES (1, 't', 's', 'PARSED', 'n', 'n')")
    c.execute("INSERT INTO parsed_text_refs (parsed_text_uid, paper_id, parsed_text_path, "
              "parsed_text_version, recorded_at, parsed_text_sha256) "
              "VALUES ('u', 1, 'p', 1, 'n', ?)", ("a" * 64,))
    c.commit()
    for stmt in ("UPDATE parsed_text_refs SET parsed_text_path = 'q'",
                 "DELETE FROM parsed_text_refs"):
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            c.execute(stmt)
    c.close()


# ── databases with rows, at the pre-021 shape ─────────────────────────
@pytest.fixture
def world(tmp_path):
    """A 016-shape database with three references, their files under a temp
    repo root, and a baseline JSON matching them."""
    root = tmp_path / "repo"
    pdir = root / "data" / "rv" / "parsed_text"
    pdir.mkdir(parents=True)
    files = {("u1", 1, 1): b"paper one, v1\n", ("u2", 1, 2): b"paper one, v2\n",
             ("u3", 2, 1): b"paper two\n"}
    db = tmp_path / "rows.db"
    c = sqlite3.connect(str(db))
    c.execute("CREATE TABLE papers (id INTEGER PRIMARY KEY, title TEXT NOT NULL DEFAULT 't')")
    c.executemany("INSERT INTO papers (id) VALUES (?)", [(1,), (2,)])
    m016.create_schema(c)
    entries = []
    for (uid, pid, v), body in files.items():
        rel = f"data/rv/parsed_text/{pid}_v{v}.md"
        (root / rel).write_bytes(body)
        c.execute("INSERT INTO parsed_text_refs (parsed_text_uid, paper_id, parsed_text_path, "
                  "parsed_text_version, source_full_text_assets_id, recorded_at) "
                  "VALUES (?, ?, ?, ?, ?, 'then')", (uid, pid, rel, v, 10 + v))
        entries.append({"parsed_text_uid": uid, "paper_id": pid, "parsed_text_path": rel,
                        "parsed_text_version": v, "size_bytes": len(body), "sha256": _sha(body)})
    c.commit()
    c.close()

    def write_baseline(ents):
        b = tmp_path / "baseline.json"
        b.write_text(json.dumps({"count": len(ents), "entries": ents}))
        return b, _sha(b.read_bytes())

    return {"db": db, "root": root, "entries": entries, "baseline": write_baseline}


def _run(world, entries=None, **kw):
    b, digest = world["baseline"](world["entries"] if entries is None else entries)
    return m021.run_migration(str(world["db"]), baseline_path=b, baseline_sha256=digest,
                              repo_root=world["root"], **kw)


def _state(db):
    c = sqlite3.connect(str(db))
    try:
        sql = c.execute("SELECT sql FROM sqlite_master WHERE name='parsed_text_refs'").fetchone()[0]
        n = c.execute("SELECT COUNT(*) FROM parsed_text_refs").fetchone()[0]
        trig = c.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' "
                         "AND tbl_name='parsed_text_refs'").fetchone()[0]
        return {"hashed": "parsed_text_sha256" in sql, "rows": n, "triggers": trig}
    finally:
        c.close()


def test_021_backfills_every_row_with_its_file_hash(world):
    out = _run(world)
    assert out == {"status": "executed", "rows": 3, "paths_normalized": 0}
    c = sqlite3.connect(str(world["db"]))
    got = dict(c.execute("SELECT parsed_text_uid, parsed_text_sha256 FROM parsed_text_refs"))
    c.close()
    assert got == {e["parsed_text_uid"]: e["sha256"] for e in world["entries"]}
    assert _state(world["db"]) == {"hashed": True, "rows": 3, "triggers": 2}
    assert _run(world)["status"] == "already_applied"


def test_021_seeded_fixture_reports_zero_paths_changed(world):
    """R100 / N2: every stored path already repo-relative → 0 rows changed."""
    assert _run(world)["paths_normalized"] == 0


def test_021_normalizes_one_absolute_path_under_the_repo(world):
    c = sqlite3.connect(str(world["db"]))
    c.execute("DROP TRIGGER parsed_text_refs_no_update")  # fixture set-up only
    absolute = str(world["root"] / "data/rv/parsed_text/2_v1.md")
    c.execute("UPDATE parsed_text_refs SET parsed_text_path = ? WHERE parsed_text_uid = 'u3'",
              (absolute,))
    c.commit()
    c.close()
    assert _run(world)["paths_normalized"] == 1
    c = sqlite3.connect(str(world["db"]))
    p = c.execute("SELECT parsed_text_path FROM parsed_text_refs "
                  "WHERE parsed_text_uid = 'u3'").fetchone()[0]
    c.close()
    assert p == "data/rv/parsed_text/2_v1.md"


def test_021_refuses_a_wrong_baseline_entry_naming_the_row_and_writes_nothing(world):
    ents = [dict(e) for e in world["entries"]]
    ents[1]["sha256"] = "f" * 64
    before = _state(world["db"])
    with pytest.raises(m021.Migration021Refused, match="parsed_text_uid=u2") as e:
        _run(world, ents)
    assert "f" * 64 in str(e.value)
    assert _state(world["db"]) == before == {"hashed": False, "rows": 3, "triggers": 2}


def test_021_refuses_a_missing_file_naming_the_row_and_writes_nothing(world):
    (world["root"] / "data/rv/parsed_text/2_v1.md").unlink()
    with pytest.raises(m021.Migration021Refused, match="parsed_text_uid=u3.*file missing"):
        _run(world)
    assert _state(world["db"]) == {"hashed": False, "rows": 3, "triggers": 2}


def test_021_refuses_a_row_absent_from_the_baseline(world):
    with pytest.raises(m021.Migration021Refused, match="u3 .*not in the baseline"):
        _run(world, world["entries"][:2])
    assert _state(world["db"])["hashed"] is False


def test_021_refuses_a_baseline_that_is_not_the_committed_one(world):
    b, _ = world["baseline"](world["entries"])
    with pytest.raises(m021.Migration021Refused, match="not the committed"):
        m021.run_migration(str(world["db"]), baseline_path=b, baseline_sha256="0" * 64,
                           repo_root=world["root"])
    assert _state(world["db"])["hashed"] is False


def test_021_refuses_two_rows_at_one_version(world):
    c = sqlite3.connect(str(world["db"]))
    body = b"another text at v2\n"
    rel = "data/rv/parsed_text/1_v2b.md"
    (world["root"] / rel).write_bytes(body)
    c.execute("INSERT INTO parsed_text_refs (parsed_text_uid, paper_id, parsed_text_path, "
              "parsed_text_version, recorded_at) VALUES ('u4', 1, ?, 2, 'then')", (rel,))
    c.commit()
    c.close()
    ents = world["entries"] + [{"parsed_text_uid": "u4", "paper_id": 1, "parsed_text_version": 2,
                                "parsed_text_path": rel, "sha256": _sha(body)}]
    with pytest.raises(m021.Migration021Refused, match="paper_id=1 version=2"):
        _run(world, ents)


def test_021_a_forced_mid_rebuild_failure_leaves_the_table_and_guard(world):
    with pytest.raises(RuntimeError, match="forced"):
        _run(world, _fail_after_copy=True)
    assert _state(world["db"]) == {"hashed": False, "rows": 3, "triggers": 2}


def test_the_committed_baseline_matches_the_declared_digest():
    """R101: the default the runner uses is the JSON committed in Phase 1."""
    data = m021.BASELINE_PATH.read_bytes()
    assert _sha(data) == m021.BASELINE_SHA256
    assert json.loads(data)["count"] == 194


def test_canonical_path_keeps_outside_paths_absolute(tmp_path):
    root = tmp_path / "repo"
    assert m021.canonical_path("/elsewhere/x.md", root) == "/elsewhere/x.md"
    assert m021.canonical_path("data/a/../b.md", root) == "data/b.md"
    assert m021.canonical_path(str(root / "data" / "b.md"), root) == "data/b.md"
