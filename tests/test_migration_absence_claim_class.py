"""Migration 011 must widen the taxonomy enum without losing a single census row."""

import importlib.util
import sqlite3
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(
        name, ROOT / "engine" / "migrations" / filename
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


m010 = _load("m010", "010_add_provenance_classifications.py")
m011 = _load("m011", "011_add_absence_claim_class.py")

_ROW = (
    "run-1", "local_deepseek_r1_32b", 7, "sample_size", "evidence_spans", 42,
    "NR", 31, "UNTRACEABLE_NO_BASIS", "extractive", 1, 1, 0, "[0.4]", 0.4,
    None, 0, "docling", "2026-07-27T00:00:00Z",
)
_INSERT = """INSERT INTO provenance_classifications
    (census_run_id, arm, paper_id, field_name, span_table, span_row_id, value,
     snippet_chars, taxonomy_class, field_class, n_sentences, n_evaluated,
     n_exact, sentence_ratios, min_ratio, strict_variant_class, has_ellipsis,
     parser_tier, classified_at)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""


@pytest.fixture
def db(tmp_path):
    path = tmp_path / "review.db"
    m010.run_migration(str(path))
    conn = sqlite3.connect(path)
    conn.execute(
        """INSERT INTO provenance_census_runs
             (census_run_id, review_name, created_at, definitions_sha256,
              normalization_version, tokenizer, tokenizer_version,
              threshold_primary, threshold_band, min_sentence_tokens,
              ratio_ceiling, spans_total)
           VALUES ('run-1','r','2026-07-27T00:00:00Z','abc','prov-norm-1',
                   'pysbd','0.3.4',0.9,'[0.85,0.9,0.95]',3,0.95,3)"""
    )
    for i in range(3):
        conn.execute(_INSERT, (*_ROW[:3], f"field_{i}", *_ROW[4:]))
    conn.commit()
    conn.close()
    return path


def test_absence_claim_rejected_before_migration(db):
    conn = sqlite3.connect(db)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(_INSERT, (*_ROW[:3], "f_new", *_ROW[4:8], "ABSENCE_CLAIM", *_ROW[9:]))
    conn.close()


def test_migration_widens_enum_and_preserves_rows(db):
    summary = m011.run_migration(str(db))
    assert summary["rebuilt"] is True
    assert summary["rows_preserved"] == 3

    conn = sqlite3.connect(db)
    assert conn.execute("SELECT COUNT(*) FROM provenance_classifications").fetchone()[0] == 3
    # the previously-rejected value now inserts
    conn.execute(_INSERT.replace(", classified_at)", ", classified_at)"),
                 (*_ROW[:3], "f_new", *_ROW[4:8], "ABSENCE_CLAIM", *_ROW[9:]))
    conn.commit()
    cols = {r[1] for r in conn.execute("PRAGMA table_info(provenance_classifications)")}
    assert "absence_pattern" in cols
    runcols = {r[1] for r in conn.execute("PRAGMA table_info(provenance_census_runs)")}
    assert "absence_pattern_version" in runcols
    assert conn.execute("PRAGMA foreign_key_check").fetchall() == []
    conn.close()


def test_migration_is_idempotent(db):
    m011.run_migration(str(db))
    second = m011.run_migration(str(db))
    assert second["rebuilt"] is False
    conn = sqlite3.connect(db)
    assert conn.execute("SELECT COUNT(*) FROM provenance_classifications").fetchone()[0] == 3
    conn.close()


def test_indexes_survive_the_rebuild(db):
    m011.run_migration(str(db))
    conn = sqlite3.connect(db)
    idx = {
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' "
            "AND tbl_name='provenance_classifications'"
        )
    }
    assert {
        "idx_prov_class_run",
        "idx_prov_class_run_arm_class",
        "idx_prov_class_run_field",
        "idx_prov_class_absence_pattern",
    } <= idx
    conn.close()
