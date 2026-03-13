#!/usr/bin/env python3
"""Migration 002: Rename screening labels to distinguish abstract vs full-text screening.

Renames:
  - Paper statuses: SCREENED_IN → ABSTRACT_SCREENED_IN, SCREENED_OUT → ABSTRACT_SCREENED_OUT,
    SCREEN_FLAGGED → ABSTRACT_SCREEN_FLAGGED
  - Tables: screening_decisions → abstract_screening_decisions,
    verification_decisions → abstract_verification_decisions,
    screening_adjudication → abstract_screening_adjudication
  - Workflow stages: SCREENING_COMPLETE → ABSTRACT_SCREENING_COMPLETE,
    DIAGNOSTIC_SAMPLE_COMPLETE → ABSTRACT_DIAGNOSTIC_COMPLETE,
    CATEGORIES_CONFIGURED → ABSTRACT_CATEGORIES_CONFIGURED,
    QUEUE_EXPORTED → ABSTRACT_QUEUE_EXPORTED,
    ADJUDICATION_COMPLETE → ABSTRACT_ADJUDICATION_COMPLETE
  - New workflow stages inserted: FULL_TEXT_SCREENING_COMPLETE, FULL_TEXT_ADJUDICATION_COMPLETE

Future full-text screening values (not yet implemented):
  - Statuses: FT_ELIGIBLE, FT_SCREENED_OUT, FT_FLAGGED
  - Tables: ft_screening_decisions, ft_verification_decisions, ft_screening_adjudication
  - Workflow stages: FULL_TEXT_SCREENING_COMPLETE, FULL_TEXT_ADJUDICATION_COMPLETE

Idempotent — safe to run multiple times. Backs up the DB before any changes.
"""

import shutil
import sqlite3
import sys
from pathlib import Path


def _has_value(conn: sqlite3.Connection, table: str, column: str, value: str) -> bool:
    """Check if a specific value exists in a table column."""
    row = conn.execute(
        f"SELECT COUNT(*) FROM {table} WHERE {column} = ?", (value,)
    ).fetchone()
    return row[0] > 0


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return row[0] > 0


def run_migration(db_path: str | Path) -> dict:
    """Run the screening rename migration. Returns summary of changes."""
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    # Step 1: Backup
    backup_path = db_path.with_suffix(".db.pre_rename_backup")
    if not backup_path.exists():
        shutil.copy2(db_path, backup_path)
        print(f"[backup] Created {backup_path}")
    else:
        print(f"[backup] Backup already exists at {backup_path}, skipping")

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=OFF")  # needed for table renames
    changes = []

    # Step 2: Rename paper status values
    status_renames = {
        "SCREENED_IN": "ABSTRACT_SCREENED_IN",
        "SCREENED_OUT": "ABSTRACT_SCREENED_OUT",
        "SCREEN_FLAGGED": "ABSTRACT_SCREEN_FLAGGED",
    }
    for old, new in status_renames.items():
        count = conn.execute(
            "SELECT COUNT(*) FROM papers WHERE status = ?", (old,)
        ).fetchone()[0]
        if count > 0:
            conn.execute(
                "UPDATE papers SET status = ? WHERE status = ?", (new, old)
            )
            msg = f"[status] papers.status: {old} → {new} ({count} rows)"
            print(msg)
            changes.append(msg)
        else:
            already = conn.execute(
                "SELECT COUNT(*) FROM papers WHERE status = ?", (new,)
            ).fetchone()[0]
            if already > 0:
                print(f"[status] papers.status: {old} → {new} — already migrated ({already} rows)")
            else:
                print(f"[status] papers.status: {old} — no rows found, skipping")
    conn.commit()

    # Step 3: Rename tables
    table_renames = {
        "screening_decisions": "abstract_screening_decisions",
        "verification_decisions": "abstract_verification_decisions",
        "screening_adjudication": "abstract_screening_adjudication",
    }
    for old_name, new_name in table_renames.items():
        if _table_exists(conn, old_name) and not _table_exists(conn, new_name):
            conn.execute(f"ALTER TABLE {old_name} RENAME TO {new_name}")
            msg = f"[table] {old_name} → {new_name}"
            print(msg)
            changes.append(msg)
        elif _table_exists(conn, new_name):
            print(f"[table] {new_name} already exists — skipping rename")
        else:
            print(f"[table] {old_name} does not exist — skipping")

    # Rename indexes (drop old, create new — SQLite renames indexes with table)
    # SQLite automatically renames indexes when table is renamed, but the
    # index names stay the same. We'll recreate them with proper names.
    index_renames = [
        ("idx_screening_paper", "abstract_screening_decisions", "paper_id",
         "idx_abstract_screening_paper"),
        ("idx_verification_paper", "abstract_verification_decisions", "paper_id",
         "idx_abstract_verification_paper"),
        ("idx_adjudication_paper", "abstract_screening_adjudication", "paper_id",
         "idx_abstract_adjudication_paper"),
        ("idx_adjudication_ext_key", "abstract_screening_adjudication", "external_key",
         "idx_abstract_adjudication_ext_key"),
        ("idx_adjudication_decision", "abstract_screening_adjudication", "adjudication_decision",
         "idx_abstract_adjudication_decision"),
    ]
    for old_idx, table, column, new_idx in index_renames:
        # Check if old index exists
        exists = conn.execute(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?",
            (old_idx,),
        ).fetchone()[0]
        if exists:
            conn.execute(f"DROP INDEX IF EXISTS {old_idx}")
        # Create new index if not exists
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS {new_idx} ON {table}({column})"
        )
    conn.commit()

    # Step 4: Rename workflow stage values
    stage_renames = {
        "SCREENING_COMPLETE": "ABSTRACT_SCREENING_COMPLETE",
        "DIAGNOSTIC_SAMPLE_COMPLETE": "ABSTRACT_DIAGNOSTIC_COMPLETE",
        "CATEGORIES_CONFIGURED": "ABSTRACT_CATEGORIES_CONFIGURED",
        "QUEUE_EXPORTED": "ABSTRACT_QUEUE_EXPORTED",
        "ADJUDICATION_COMPLETE": "ABSTRACT_ADJUDICATION_COMPLETE",
    }

    if _table_exists(conn, "workflow_state"):
        for old, new in stage_renames.items():
            count = conn.execute(
                "SELECT COUNT(*) FROM workflow_state WHERE stage_name = ?", (old,)
            ).fetchone()[0]
            if count > 0:
                conn.execute(
                    "UPDATE workflow_state SET stage_name = ? WHERE stage_name = ?",
                    (new, old),
                )
                msg = f"[stage] workflow_state: {old} → {new}"
                print(msg)
                changes.append(msg)
            else:
                already = conn.execute(
                    "SELECT COUNT(*) FROM workflow_state WHERE stage_name = ?", (new,)
                ).fetchone()[0]
                if already:
                    print(f"[stage] workflow_state: {old} → {new} — already migrated")
                else:
                    print(f"[stage] workflow_state: {old} — not found, skipping")

        # Step 5: Insert new full-text screening stages
        new_stages = [
            "FULL_TEXT_SCREENING_COMPLETE",
            "FULL_TEXT_ADJUDICATION_COMPLETE",
        ]
        for stage in new_stages:
            exists = conn.execute(
                "SELECT COUNT(*) FROM workflow_state WHERE stage_name = ?", (stage,)
            ).fetchone()[0]
            if not exists:
                conn.execute(
                    "INSERT INTO workflow_state (stage_name, status) VALUES (?, 'pending')",
                    (stage,),
                )
                msg = f"[stage] Inserted new stage: {stage}"
                print(msg)
                changes.append(msg)
            else:
                print(f"[stage] {stage} already exists — skipping insert")

        conn.commit()
    else:
        print("[stage] workflow_state table not found — skipping stage renames")

    conn.execute("PRAGMA foreign_keys=ON")
    conn.close()

    print(f"\n[done] Migration complete. {len(changes)} changes applied.")
    return {"changes": changes, "backup_path": str(backup_path)}


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m engine.migrations.002_screening_rename <path/to/review.db>")
        sys.exit(1)

    db_path = sys.argv[1]
    result = run_migration(db_path)
    print(f"\nSummary: {len(result['changes'])} changes, backup at {result['backup_path']}")
