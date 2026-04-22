"""Re-run 5 triples with the post-fix run_id to test Gemma determinism.

The 6 flipped (non-absence) arm verdicts from the delta live inside 5
triples. We re-invoke run_pass2() with the SAME run_id as the post-fix
smoke, which reproduces the exact seed, permutation, and prompt.
If Ollama is deterministic at temp=0, the verdict for each named arm
should match the stored post-fix verdict exactly.

No DB writes — read-only stability check.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from analysis.paper1.judge import run_pass2
from analysis.paper1.judge_loader import (
    compute_codebook_sha256,
    load_ai_triples_csv,
    load_codebook,
)
from engine.core.database import ReviewDatabase

DB_PATH = Path("data/surgical_autonomy/review.db")
POST_RUN = "surgical_autonomy_pass2_smoke_fixed_20260421T165202Z"
PAIRS_CSV = Path("data/surgical_autonomy/exports/disagreement_pairs_3arm.csv")
CODEBOOK = Path("data/surgical_autonomy/extraction_codebook.yaml")

# (paper_id, field_name, arm_name, expected_verdict_from_post_fix_run)
TARGETS: list[tuple[str, str, str]] = [
    ("9", "robot_platform", "local"),
    ("9", "study_design", "local"),
    ("9", "system_maturity", "local"),
    ("17", "task_performed", "local"),
    ("411", "sample_size", "anthropic_sonnet_4_6"),
    ("411", "sample_size", "openai_o4_mini_high"),
]


def load_expected(conn: sqlite3.Connection) -> dict[tuple[str, str, str], str]:
    rows = conn.execute(
        """
        SELECT paper_id, field_name, arm_name, verdict
        FROM fabrication_verifications
        WHERE judge_run_id=?
        """,
        (POST_RUN,),
    ).fetchall()
    return {(r[0], r[1], r[2]): r[3] for r in rows}


def paper_text(review_dir: Path, paper_id: str) -> str:
    md = sorted(
        (review_dir / "parsed_text").glob(f"{paper_id}_v*.md"), reverse=True
    )
    return md[0].read_text() if md else ""


def main() -> None:
    db = ReviewDatabase("surgical_autonomy")
    expected = load_expected(db._conn)

    codebook = load_codebook(CODEBOOK)
    inputs = load_ai_triples_csv(PAIRS_CSV, db, codebook, limit=None)
    input_lookup = {(i.paper_id, i.field_name): i for i in inputs}

    # Group targets by (paper, field) so each triple is called once.
    triples: dict[tuple[str, str], list[str]] = {}
    for pid, field, arm in TARGETS:
        triples.setdefault((pid, field), []).append(arm)

    review_dir = Path(DB_PATH).parent
    reproduce: list[tuple[str, str, str, str, str, str]] = []

    for (pid, field), arms_to_check in triples.items():
        inp = input_lookup[(pid, field)]
        src = paper_text(review_dir, pid)
        result = run_pass2(inp, run_id=POST_RUN, source_text=src)
        for v in result.pass2.arm_verdicts:
            arm_name = result.arm_permutation[v.arm_slot - 1]
            if arm_name in arms_to_check:
                prev = expected.get((pid, field, arm_name), "MISSING")
                status = "MATCH" if v.verdict == prev else "DIFFER"
                reproduce.append(
                    (pid, field, arm_name, prev, v.verdict, status)
                )

    # Sort in the order the task listed them
    order = {(p, f, a): i for i, (p, f, a) in enumerate(TARGETS)}
    reproduce.sort(key=lambda r: order[(r[0], r[1], r[2])])

    n_match = sum(1 for r in reproduce if r[5] == "MATCH")
    print(f"\n=== Stability — {n_match}/{len(reproduce)} reproduce post-fix verdict ===")
    for pid, field, arm, prev, new, status in reproduce:
        print(f"  [{status}] {pid}/{field}/{arm}: post-fix={prev}  rerun={new}")
    print()


if __name__ == "__main__":
    main()
