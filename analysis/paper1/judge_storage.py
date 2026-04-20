"""Storage API for the Paper 1 LLM-as-judge tables.

Persistence only. No loading, prompting, or CLI. Mirrors the
`try: BEGIN ... COMMIT / except: ROLLBACK` transaction idiom
used by engine.core.database.ReviewDatabase.add_extraction.
"""

from __future__ import annotations

import json
import math
import sqlite3
from datetime import datetime, timezone
from typing import Optional

from analysis.paper1.judge_schema import (
    FieldType,
    JudgeResult,
    pair_disagreement_type,
)
from engine.core.database import ReviewDatabase


class JudgeStorageError(Exception):
    """Persistence-layer failure. Underlying sqlite3 error in __cause__."""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _expected_pair_count(n_arms: int) -> int:
    return math.comb(n_arms, 2)


def create_judge_run(
    db: ReviewDatabase,
    run_id: str,
    judge_model_name: str,
    judge_model_digest: str,
    codebook_sha256: str,
    pass_number: int,
    input_scope: str,
    run_config: dict,
    notes: Optional[str] = None,
) -> None:
    """Insert a new judge_runs row. completed_at = NULL, counters = 0."""
    try:
        config_json = json.dumps(run_config)
    except (TypeError, ValueError) as exc:
        raise JudgeStorageError(
            f"run_config not JSON-serializable: {exc}"
        ) from exc

    try:
        db._conn.execute("BEGIN")
        db._conn.execute(
            """INSERT INTO judge_runs
               (run_id, judge_model_name, judge_model_digest,
                codebook_sha256, pass_number, input_scope,
                started_at, completed_at,
                n_triples_attempted, n_triples_succeeded,
                n_triples_failed, run_config_json, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?, NULL, 0, 0, 0, ?, ?)""",
            (
                run_id,
                judge_model_name,
                judge_model_digest,
                codebook_sha256,
                pass_number,
                input_scope,
                _now_iso(),
                config_json,
                notes,
            ),
        )
        db._conn.execute("COMMIT")
    except sqlite3.Error as exc:
        db._conn.execute("ROLLBACK")
        raise JudgeStorageError(f"create_judge_run failed: {exc}") from exc


def complete_judge_run(
    db: ReviewDatabase,
    run_id: str,
    n_triples_attempted: int,
    n_triples_succeeded: int,
    n_triples_failed: int,
) -> None:
    """Mark a judge_runs row complete. Fails on unknown or already-completed run."""
    if n_triples_succeeded + n_triples_failed != n_triples_attempted:
        raise JudgeStorageError(
            f"counter mismatch: succeeded({n_triples_succeeded}) + "
            f"failed({n_triples_failed}) != attempted({n_triples_attempted})"
        )

    row = db._conn.execute(
        "SELECT started_at, completed_at FROM judge_runs WHERE run_id = ?",
        (run_id,),
    ).fetchone()
    if row is None:
        raise JudgeStorageError(f"run_id not found: {run_id}")
    # sqlite3.Row index and key access both work.
    if row["completed_at"] is not None:
        raise JudgeStorageError(
            f"run_id {run_id} already marked complete at {row['completed_at']}"
        )

    try:
        db._conn.execute("BEGIN")
        db._conn.execute(
            """UPDATE judge_runs
               SET completed_at = ?,
                   n_triples_attempted = ?,
                   n_triples_succeeded = ?,
                   n_triples_failed = ?
               WHERE run_id = ?""",
            (
                _now_iso(),
                n_triples_attempted,
                n_triples_succeeded,
                n_triples_failed,
                run_id,
            ),
        )
        db._conn.execute("COMMIT")
    except sqlite3.Error as exc:
        db._conn.execute("ROLLBACK")
        raise JudgeStorageError(f"complete_judge_run failed: {exc}") from exc


def _validate_insert_invariants(result: JudgeResult) -> None:
    n_arms = len(result.arm_permutation)
    if n_arms < 2:
        raise JudgeStorageError(
            f"arm_permutation has {n_arms} entries; need >= 2"
        )

    expected = _expected_pair_count(n_arms)
    actual = len(result.pass1.pairwise_ratings)
    if actual != expected:
        raise JudgeStorageError(
            f"pairwise_ratings has {actual} entries; "
            f"expected C({n_arms},2) = {expected}"
        )

    for r in result.pass1.pairwise_ratings:
        if not (1 <= r.slot_a <= n_arms):
            raise JudgeStorageError(
                f"slot_a={r.slot_a} out of range [1, {n_arms}]"
            )
        if not (1 <= r.slot_b <= n_arms):
            raise JudgeStorageError(
                f"slot_b={r.slot_b} out of range [1, {n_arms}]"
            )


def insert_judge_result(
    db: ReviewDatabase,
    run_id: str,
    result: JudgeResult,
    field_type: FieldType,
) -> int:
    """Atomic: insert one judge_ratings row + C(N,2) judge_pair_ratings rows."""
    _validate_insert_invariants(result)

    arm_permutation_json = json.dumps(result.arm_permutation)
    pass1 = result.pass1

    pair_rows: list[tuple] = []
    for r in pass1.pairwise_ratings:
        name_a = result.arm_permutation[r.slot_a - 1]
        name_b = result.arm_permutation[r.slot_b - 1]
        arm_a, arm_b = (name_a, name_b) if name_a < name_b else (name_b, name_a)
        if arm_a == arm_b:
            raise JudgeStorageError(
                f"slot_a and slot_b de-randomize to the same arm '{arm_a}' "
                f"(duplicate arm_name in permutation?)"
            )
        pair_rows.append(
            (arm_a, arm_b, r.rating, pair_disagreement_type(r), r.rationale)
        )

    try:
        db._conn.execute("BEGIN")
        cur = db._conn.execute(
            """INSERT INTO judge_ratings
               (run_id, paper_id, field_name, field_type, seed,
                arm_permutation_json, prompt_hash, raw_response,
                pass1_fabrication_risk, pass1_proposed_consensus,
                pass1_overall_rationale, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                run_id,
                result.paper_id,
                result.field_name,
                field_type,
                result.seed,
                arm_permutation_json,
                result.prompt_hash,
                result.raw_response,
                pass1.fabrication_risk,
                pass1.proposed_consensus,
                pass1.overall_rationale,
                _now_iso(),
            ),
        )
        rating_id = cur.lastrowid

        for arm_a, arm_b, level1, level2, rationale in pair_rows:
            db._conn.execute(
                """INSERT INTO judge_pair_ratings
                   (rating_id, arm_a, arm_b, level1_rating,
                    level2_type, rationale)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (rating_id, arm_a, arm_b, level1, level2, rationale),
            )

        db._conn.execute("COMMIT")
        return rating_id
    except sqlite3.Error as exc:
        db._conn.execute("ROLLBACK")
        raise JudgeStorageError(f"insert_judge_result failed: {exc}") from exc


__all__ = [
    "JudgeStorageError",
    "complete_judge_run",
    "create_judge_run",
    "insert_judge_result",
]
