"""Judge pipeline CLI: loader → run_pass1 → storage.

Usage:
    python -m analysis.paper1.judge_cli \\
        --review surgical_autonomy \\
        --input AI_TRIPLES \\
        --pairs-csv data/surgical_autonomy/exports/disagreement_pairs_3arm.csv \\
        --codebook data/surgical_autonomy/extraction_codebook.yaml \\
        --pass 1 \\
        --limit 10 \\
        --run-note "smoke test pass1"
"""

from __future__ import annotations

import argparse
import logging
import sys
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from analysis.paper1.judge import (
    DEFAULT_MODEL,
    JudgeError,
    run_pass1,
)
from analysis.paper1.judge_loader import (
    CodebookEntry,
    LoaderError,
    compute_codebook_sha256,
    load_ai_triples_csv,
    load_codebook,
)
from analysis.paper1.judge_schema import (
    JudgeInput,
    JudgeResult,
    pair_disagreement_type,
)
from analysis.paper1.judge_storage import (
    JudgeStorageError,
    complete_judge_run,
    create_judge_run,
    insert_judge_result,
)
from engine.core.database import ReviewDatabase
from engine.utils.ollama_client import get_model_digest

logger = logging.getLogger(__name__)

INPUT_CHOICES = ["AI_TRIPLES", "HUMAN_PAIRS", "ALL", "SMOKE_TEST"]
SMOKE_PASS_THRESHOLD = 0.9  # exit 0 iff succeeded/attempted >= 0.9


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="analysis.paper1.judge_cli",
        description="Run the LLM-as-judge pipeline (Pass 1) for Paper 1.",
    )
    p.add_argument("--review", required=True,
                   help="Review name, e.g. surgical_autonomy")
    p.add_argument("--input", required=True, choices=INPUT_CHOICES)
    p.add_argument("--pairs-csv", type=Path,
                   help="Disagreement-pairs CSV (required for AI_TRIPLES)")
    p.add_argument("--codebook", required=True, type=Path,
                   help="Path to extraction_codebook.yaml")
    p.add_argument("--pass", dest="pass_number", type=int, default=1,
                   choices=[1, 2], help="Pass number (only 1 implemented)")
    p.add_argument("--limit", type=int, default=0,
                   help="Limit number of triples (0 = all)")
    p.add_argument("--dry-run", action="store_true",
                   help="Load + call judge, skip DB writes")
    p.add_argument("--model", default=DEFAULT_MODEL,
                   help=f"Judge model name (default: {DEFAULT_MODEL})")
    p.add_argument("--run-note", default=None,
                   help="Free-text note stored on judge_runs.notes")
    p.add_argument("--data-root", type=Path, default=None,
                   help="Override ReviewDatabase data_root (for tests)")
    return p


def _new_run_id(review: str, pass_number: int) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    short = uuid.uuid4().hex[:8]
    return f"{review}_pass{pass_number}_{ts}_{short}"


def _configure_logging(review_dir: Path) -> None:
    log_dir = review_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "judge_pipeline.log"
    root = logging.getLogger()
    if root.handlers:
        return
    root.setLevel(logging.INFO)
    fmt = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    stream = logging.StreamHandler(sys.stderr)
    stream.setFormatter(fmt)
    file_h = logging.FileHandler(log_file)
    file_h.setFormatter(fmt)
    root.addHandler(stream)
    root.addHandler(file_h)


def _print_summary(
    *,
    run_id: str,
    input_scope: str,
    pass_number: int,
    model: str,
    model_digest: str,
    codebook_sha256: str,
    dry_run: bool,
    inputs: list[JudgeInput],
    results: list[JudgeResult],
    failures: list[tuple[str, str, str]],
    attempted: int,
    succeeded: int,
    failed: int,
) -> None:
    pct_success = (succeeded / attempted * 100) if attempted else 0.0

    level1_counter: Counter[str] = Counter()
    level2_counter: Counter[str] = Counter()
    fab_risk_counter: Counter[str] = Counter()
    for r in results:
        fab_risk_counter[r.pass1.fabrication_risk] += 1
        for pr in r.pass1.pairwise_ratings:
            level1_counter[pr.rating] += 1
            dtype = pair_disagreement_type(pr)
            if dtype is not None:
                level2_counter[dtype] += 1

    field_type_counter = Counter(inp.field_type for inp in inputs)
    total_pairs = sum(level1_counter.values())

    def _pct(n: int, d: int) -> str:
        return f"{(n / d * 100):5.1f}%" if d else "  0.0%"

    bar = "=" * 60
    rule = "-" * 60
    lines = [
        bar,
        "JUDGE RUN SUMMARY",
        bar,
        f"run_id:                {run_id}",
        f"input_scope:           {input_scope}",
        f"pass:                  {pass_number}",
        f"model:                 {model} (digest {model_digest})",
        f"codebook_sha256:       {codebook_sha256}",
        f"dry_run:               {dry_run}",
        rule,
        f"Triples attempted:     {attempted}",
        f"Triples succeeded:     {succeeded} ({pct_success:.1f}%)",
        f"Triples failed:        {failed}",
        rule,
        f"Level 1 rating distribution (across {total_pairs} pair-ratings):",
    ]
    for level in ("EQUIVALENT", "PARTIAL", "DIVERGENT"):
        n = level1_counter.get(level, 0)
        lines.append(f"  {level:<18} {n:>5} ({_pct(n, total_pairs)})")
    lines.append(rule)
    lines.append("Level 2 type distribution (PARTIAL + DIVERGENT only):")
    for typ in ("GRANULARITY", "SELECTION", "OMISSION", "CONTRADICTION",
                "FABRICATION"):
        lines.append(f"  {typ:<18} {level2_counter.get(typ, 0):>5}")
    lines.append(rule)
    lines.append("Fabrication risk (per triple):")
    for risk in ("low", "medium", "high"):
        lines.append(f"  {risk:<18} {fab_risk_counter.get(risk, 0):>5}")
    lines.append(rule)
    lines.append("Field-type breakdown:")
    for ft in ("free_text", "categorical", "numeric"):
        lines.append(f"  {ft:<18} {field_type_counter.get(ft, 0):>5} triples")
    if failures:
        lines.append(rule)
        lines.append("First 5 failures:")
        for paper_id, field_name, msg in failures[:5]:
            lines.append(f"  {paper_id} {field_name}: {msg}")
    lines.append(bar)
    print("\n".join(lines))


def _resolve_db(args) -> ReviewDatabase:
    if args.data_root is not None:
        return ReviewDatabase(args.review, data_root=args.data_root)
    return ReviewDatabase(args.review)


def _load_inputs(
    args, db: ReviewDatabase, codebook: dict[str, CodebookEntry]
) -> list[JudgeInput]:
    if args.input == "AI_TRIPLES":
        if not args.pairs_csv:
            raise LoaderError("--pairs-csv is required for --input=AI_TRIPLES")
        limit = args.limit if args.limit and args.limit > 0 else None
        return load_ai_triples_csv(args.pairs_csv, db, codebook, limit=limit)
    raise NotImplementedError(
        f"--input={args.input} is not implemented in this task; AI_TRIPLES only"
    )


def run(argv: Optional[list[str]] = None) -> int:
    parser = _build_arg_parser()
    args = parser.parse_args(argv)

    if args.pass_number == 2:
        raise NotImplementedError(
            "Pass 2 (fabrication verification) will land in task #7"
        )

    db = _resolve_db(args)
    _configure_logging(db.db_path.parent)

    try:
        codebook = load_codebook(args.codebook)
        codebook_sha = compute_codebook_sha256(args.codebook)
        inputs = _load_inputs(args, db, codebook)
    except LoaderError as exc:
        logger.error("Load failed: %s", exc)
        print(f"Load failed: {exc}", file=sys.stderr)
        db.close()
        return 2

    if not inputs:
        print("No triples to process after load — nothing to do.", file=sys.stderr)
        db.close()
        return 1

    run_id = _new_run_id(args.review, args.pass_number)
    model_digest = get_model_digest(args.model) or args.model

    if not args.dry_run:
        try:
            create_judge_run(
                db, run_id=run_id,
                judge_model_name=args.model,
                judge_model_digest=model_digest,
                codebook_sha256=codebook_sha,
                pass_number=args.pass_number,
                input_scope=args.input,
                run_config={
                    "limit": args.limit,
                    "pairs_csv": str(args.pairs_csv) if args.pairs_csv else None,
                    "codebook_path": str(args.codebook),
                    "model": args.model,
                },
                notes=args.run_note,
            )
        except JudgeStorageError as exc:
            logger.error("create_judge_run failed: %s", exc)
            print(f"create_judge_run failed: {exc}", file=sys.stderr)
            db.close()
            return 2

    results: list[JudgeResult] = []
    failures: list[tuple[str, str, str]] = []
    attempted = succeeded = failed = 0

    for inp in inputs:
        attempted += 1
        try:
            result = run_pass1(inp, run_id=run_id, model=args.model)
            if not args.dry_run:
                insert_judge_result(db, run_id, result, inp.field_type)
            results.append(result)
            succeeded += 1
            logger.info(
                "OK %s %s fab_risk=%s pairs=%d",
                inp.paper_id, inp.field_name,
                result.pass1.fabrication_risk,
                len(result.pass1.pairwise_ratings),
            )
        except (JudgeError, JudgeStorageError) as exc:
            failed += 1
            msg = f"{type(exc).__name__}: {exc}"
            failures.append((inp.paper_id, inp.field_name, msg))
            logger.error("FAIL %s %s: %s", inp.paper_id, inp.field_name, msg)

    if not args.dry_run:
        try:
            complete_judge_run(db, run_id,
                               n_triples_attempted=attempted,
                               n_triples_succeeded=succeeded,
                               n_triples_failed=failed)
        except JudgeStorageError as exc:
            logger.error("complete_judge_run failed: %s", exc)

    _print_summary(
        run_id=run_id,
        input_scope=args.input,
        pass_number=args.pass_number,
        model=args.model,
        model_digest=model_digest,
        codebook_sha256=codebook_sha,
        dry_run=args.dry_run,
        inputs=inputs,
        results=results,
        failures=failures,
        attempted=attempted,
        succeeded=succeeded,
        failed=failed,
    )

    db.close()

    if attempted == 0:
        return 1
    if succeeded / attempted >= SMOKE_PASS_THRESHOLD:
        return 0
    return 1


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
