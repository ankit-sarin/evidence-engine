"""Single-triple Pass 2 retry.

Retries one (paper_id, field_name) triple into an existing judge_run, using
the same deterministic Pass 2 seed scheme the original run used. Raw Gemma
output is written to a log file *before* post-validation, so a repeat
failure yields a diagnostic artifact.

On success: inserts 3 arm-rows into fabrication_verifications, updates
judge_runs counters (n_triples_succeeded +=1, n_triples_failed -=1).

Usage:
  python -m analysis.paper1.pass2_retry_single \\
      --review surgical_autonomy \\
      --run-id surgical_autonomy_pass2_full_20260421T174729Z \\
      --paper-id 366 \\
      --field-name primary_outcome_value \\
      --pairs-csv data/surgical_autonomy/exports/disagreement_pairs_3arm.csv \\
      --codebook data/surgical_autonomy/extraction_codebook.yaml
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

from pydantic import ValidationError

from analysis.paper1.judge import (
    _extract_response_text,
    _hash_prompt,
    _validate_pass2_coverage,
)
from analysis.paper1.judge_loader import (
    compute_codebook_sha256,
    load_ai_triples_csv,
    load_codebook,
)
from analysis.paper1.judge_prompts import (
    arm_short_circuit_eligible,
    build_pass2_prompt,
    compute_seed_pass2,
    randomize_arm_assignment,
    window_source_text,
)
from analysis.paper1.judge_schema import JudgeInput, Pass2Output, Pass2Result
from analysis.paper1.judge_storage import (
    JudgeStorageError,
    insert_pass2_verifications,
)
from engine.core.database import ReviewDatabase
from engine.utils.ollama_client import get_model_digest, ollama_chat

DEFAULT_MODEL = "gemma3:27b"
DEFAULT_NUM_CTX = 24576


def _load_judge_input(
    pairs_csv: Path,
    codebook_path: Path,
    db: ReviewDatabase,
    paper_id: str,
    field_name: str,
) -> JudgeInput:
    codebook = load_codebook(codebook_path)
    inputs = load_ai_triples_csv(pairs_csv, db, codebook, limit=None)
    for inp in inputs:
        if inp.paper_id == paper_id and inp.field_name == field_name:
            return inp
    raise SystemExit(
        f"JudgeInput not found: paper_id={paper_id} field={field_name}"
    )


def _paper_text(review_dir: Path, paper_id: str) -> str:
    parsed = sorted(
        (review_dir / "parsed_text").glob(f"{paper_id}_v*.md"), reverse=True
    )
    if not parsed:
        raise SystemExit(f"No parsed text for paper_id={paper_id}")
    return parsed[0].read_text()


def _confirm_risk(db: ReviewDatabase, pass1_run_id: str,
                  paper_id: str, field_name: str) -> str:
    row = db._conn.execute(
        """SELECT pass1_fabrication_risk FROM judge_ratings
           WHERE run_id = ? AND paper_id = ? AND field_name = ?""",
        (pass1_run_id, paper_id, field_name),
    ).fetchone()
    if row is None:
        raise SystemExit(
            f"No Pass 1 rating for {paper_id}/{field_name} in {pass1_run_id}"
        )
    risk = row["pass1_fabrication_risk"]
    if risk not in {"medium", "high"}:
        raise SystemExit(
            f"fabrication_risk={risk!r} for {paper_id}/{field_name}; "
            "expected medium|high"
        )
    return risk


def _already_present(db: ReviewDatabase, run_id: str,
                     paper_id: str, field_name: str) -> int:
    row = db._conn.execute(
        """SELECT COUNT(*) AS n FROM fabrication_verifications
           WHERE judge_run_id = ? AND paper_id = ? AND field_name = ?""",
        (run_id, paper_id, field_name),
    ).fetchone()
    return row["n"]


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="analysis.paper1.pass2_retry_single")
    p.add_argument("--review", required=True)
    p.add_argument("--run-id", required=True,
                   help="Existing judge_runs.run_id to retry into")
    p.add_argument("--paper-id", required=True)
    p.add_argument("--field-name", required=True)
    p.add_argument("--pairs-csv", required=True, type=Path)
    p.add_argument("--codebook", required=True, type=Path)
    p.add_argument("--model", default=DEFAULT_MODEL)
    p.add_argument("--log-dir", type=Path,
                   default=Path("analysis/paper1/logs"))
    p.add_argument("--data-root", type=Path, default=None)
    return p


def main() -> int:
    args = _build_arg_parser().parse_args()

    if args.data_root is not None:
        db = ReviewDatabase(args.review, data_root=args.data_root)
    else:
        db = ReviewDatabase(args.review)

    # Confirm run exists, read its pass1_run_id from run_config_json.
    row = db._conn.execute(
        """SELECT run_config_json, completed_at,
                  n_triples_attempted, n_triples_succeeded, n_triples_failed
           FROM judge_runs WHERE run_id = ?""",
        (args.run_id,),
    ).fetchone()
    if row is None:
        print(f"run_id not found: {args.run_id}", file=sys.stderr)
        return 2
    cfg = json.loads(row["run_config_json"])
    pass1_run_id = cfg["pass1_run_id"]
    print(f"run_id: {args.run_id}")
    print(f"pass1_run_id (from run_config_json): {pass1_run_id}")
    print(f"completed_at: {row['completed_at']}")
    print(f"counters before: attempted={row['n_triples_attempted']} "
          f"succeeded={row['n_triples_succeeded']} "
          f"failed={row['n_triples_failed']}")

    # Guard against double-write.
    existing = _already_present(db, args.run_id, args.paper_id, args.field_name)
    if existing != 0:
        print(
            f"Refusing retry: {existing} arm-rows already exist for "
            f"{args.paper_id}/{args.field_name} in {args.run_id}",
            file=sys.stderr,
        )
        return 2

    risk = _confirm_risk(db, pass1_run_id, args.paper_id, args.field_name)
    print(f"Pass 1 fabrication_risk: {risk}")

    inp = _load_judge_input(
        args.pairs_csv, args.codebook, db, args.paper_id, args.field_name
    )
    source_text = _paper_text(db.db_path.parent, args.paper_id)
    print(f"source_text chars: {len(source_text)}")

    # ── Mirror run_pass2() exactly, with pre-validation raw capture ──
    seed = compute_seed_pass2(args.paper_id, args.field_name, args.run_id)
    shuffled, permutation = randomize_arm_assignment(inp.arms, seed)
    windowed_text, was_windowed, src_tokens = window_source_text(
        source_text, [a.span for a in shuffled]
    )
    prompt = build_pass2_prompt(inp, shuffled, windowed_text, was_windowed)
    prompt_hash = _hash_prompt(prompt)
    short_circuit_by_arm = {
        a.arm_name: arm_short_circuit_eligible(a) for a in inp.arms
    }

    print(f"seed: {seed}  arm_permutation: {permutation}")
    print(f"prompt_hash: {prompt_hash}")
    print(f"source windowed={was_windowed} tokens={src_tokens}")

    args.log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    raw_path = args.log_dir / (
        f"pass2_retry_{args.paper_id}_{args.field_name}_{ts}.log"
    )

    call_ok = False
    raw_response = ""
    try:
        response = ollama_chat(
            model=args.model,
            messages=[{"role": "user", "content": prompt}],
            format=Pass2Output.model_json_schema(),
            options={"temperature": 0.0, "seed": seed, "num_ctx": DEFAULT_NUM_CTX},
            think=False,
        )
        call_ok = True
        raw_response = _extract_response_text(response)
    except Exception as exc:
        raw_path.write_text(
            f"OLLAMA_CALL_ERROR: {type(exc).__name__}: {exc}\n"
        )
        print(f"OLLAMA_CALL_ERROR: {exc}", file=sys.stderr)
        print(f"raw log (empty): {raw_path}", file=sys.stderr)
        return 3

    # Persist raw output BEFORE any validation can raise.
    header = (
        f"# Pass 2 retry raw output\n"
        f"# run_id         = {args.run_id}\n"
        f"# paper_id       = {args.paper_id}\n"
        f"# field_name     = {args.field_name}\n"
        f"# model          = {args.model}\n"
        f"# seed           = {seed}\n"
        f"# arm_permutation= {permutation}\n"
        f"# prompt_hash    = {prompt_hash}\n"
        f"# captured_at    = {ts}\n"
        f"# call_ok        = {call_ok}\n"
        f"# raw_len_chars  = {len(raw_response)}\n"
        f"# ── BEGIN RAW RESPONSE ──\n"
    )
    raw_path.write_text(header + raw_response + "\n# ── END RAW RESPONSE ──\n")
    print(f"raw log: {raw_path}")

    # ── Post-validation ──
    try:
        pass2 = Pass2Output.model_validate_json(raw_response)
    except (ValidationError, ValueError) as exc:
        print(f"VALIDATION_FAILED (schema): {exc}", file=sys.stderr)
        print("── raw response ──", file=sys.stderr)
        sys.stderr.write(raw_response + "\n")
        return 4

    try:
        _validate_pass2_coverage(pass2, permutation)
    except Exception as exc:
        print(f"VALIDATION_FAILED (coverage): {exc}", file=sys.stderr)
        print("── raw response ──", file=sys.stderr)
        sys.stderr.write(raw_response + "\n")
        return 4

    result = Pass2Result(
        paper_id=args.paper_id,
        field_name=args.field_name,
        arm_permutation=permutation,
        pass2=pass2,
        pre_check_short_circuit_by_arm=short_circuit_by_arm,
        prompt_hash=prompt_hash,
        judge_model_digest=get_model_digest(args.model) or args.model,
        judge_model_name=args.model,
        raw_response=raw_response,
        seed=seed,
        timestamp_iso=datetime.now(timezone.utc).isoformat(),
        source_text_windowed=was_windowed,
        source_text_tokens=src_tokens,
    )

    try:
        n_rows = insert_pass2_verifications(db, args.run_id, result)
    except JudgeStorageError as exc:
        print(f"INSERT_FAILED: {exc}", file=sys.stderr)
        return 5
    print(f"Inserted {n_rows} arm-rows.")

    # Update counters. The run was already completed_at-stamped; we bump
    # succeeded and drop failed for the retried triple. Leave completed_at
    # untouched (keeps the original wall-clock closure stamp).
    try:
        db._conn.execute("BEGIN")
        db._conn.execute(
            """UPDATE judge_runs
               SET n_triples_succeeded = n_triples_succeeded + 1,
                   n_triples_failed    = n_triples_failed    - 1
               WHERE run_id = ?""",
            (args.run_id,),
        )
        db._conn.execute("COMMIT")
    except sqlite3.Error as exc:
        db._conn.execute("ROLLBACK")
        print(f"COUNTER_UPDATE_FAILED: {exc}", file=sys.stderr)
        return 6

    row = db._conn.execute(
        """SELECT n_triples_attempted, n_triples_succeeded, n_triples_failed
           FROM judge_runs WHERE run_id = ?""",
        (args.run_id,),
    ).fetchone()
    total = db._conn.execute(
        "SELECT COUNT(*) FROM fabrication_verifications WHERE judge_run_id = ?",
        (args.run_id,),
    ).fetchone()[0]
    print(f"counters after: attempted={row['n_triples_attempted']} "
          f"succeeded={row['n_triples_succeeded']} "
          f"failed={row['n_triples_failed']}")
    print(f"fabrication_verifications total: {total}")
    print("Per-arm verdicts:")
    for v in result.pass2.arm_verdicts:
        arm = permutation[v.arm_slot - 1]
        sc = short_circuit_by_arm.get(arm, False)
        print(f"  {arm}: verdict={v.verdict} short_circuit={sc}")

    db.close()
    return 0 if total == 3636 else 7


if __name__ == "__main__":
    raise SystemExit(main())
