"""Pass 2 full 1,212-triple fabrication verification run.

Iterates all (medium + high) fabrication-risk triples from the parent
Pass 1 run_id, invokes run_pass2() per triple, writes verdicts to
fabrication_verifications, and emits a preliminary distribution report.

Usage:
    python -m analysis.paper1.pass2_full \\
        --review surgical_autonomy \\
        --pass1-run-id surgical_autonomy_pass1_20260420T093840Z_1d93b6e5 \\
        --pairs-csv data/surgical_autonomy/exports/disagreement_pairs_3arm.csv \\
        --codebook data/surgical_autonomy/extraction_codebook.yaml

Abort conditions (checked every CHECKPOINT_EVERY triples):
  - Failure rate > 5%
  - Projected wall-clock > 30h
  - Single-triple wall > STALL_THRESHOLD_SEC is logged (not auto-abort
    on its own; Ollama stalls tend to be transient).
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from analysis.paper1.judge import JudgeError, run_pass2
from analysis.paper1.judge_loader import (
    compute_codebook_sha256,
    load_ai_triples_csv,
    load_codebook,
)
from analysis.paper1.judge_schema import (
    JudgeInput,
    PartiallySupportedVerdict,
    Pass2Result,
    SupportedVerdict,
    UnsupportedVerdict,
)
from analysis.paper1.judge_storage import (
    JudgeStorageError,
    complete_judge_run,
    create_judge_run,
    insert_pass2_verifications,
)
from engine.core.database import ReviewDatabase
from engine.utils.ollama_client import fetch_model_digest

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "gemma3:27b"
CHECKPOINT_EVERY = 100
STALL_THRESHOLD_SEC = 5 * 60
FAIL_RATE_THRESHOLD = 0.05       # 5%
ABORT_PROJECTION_HOURS = 30.0


class AbortError(RuntimeError):
    """Raised when an abort condition fires at a checkpoint."""


# ── Triple universe ───────────────────────────────────────────────


@dataclass(frozen=True)
class Candidate:
    paper_id: str
    field_name: str
    field_type: str
    risk: str

    def sort_key(self):
        try:
            pid = (0, int(self.paper_id))
        except ValueError:
            pid = (1, self.paper_id)
        return pid + (self.field_name,)


def fetch_triples(db: ReviewDatabase, pass1_run_id: str) -> list[Candidate]:
    """Return all medium+high risk triples, ordered by (paper_id, field_name)."""
    rows = db._conn.execute(
        """SELECT paper_id, field_name, field_type, pass1_fabrication_risk
           FROM judge_ratings
           WHERE run_id = ?
             AND pass1_fabrication_risk IN ('medium', 'high')""",
        (pass1_run_id,),
    ).fetchall()
    cands = [
        Candidate(r["paper_id"], r["field_name"], r["field_type"],
                  r["pass1_fabrication_risk"])
        for r in rows
    ]
    cands.sort(key=Candidate.sort_key)
    return cands


# ── Execution ─────────────────────────────────────────────────────


@dataclass
class TripleResult:
    candidate: Candidate
    wall_sec: float
    windowed: bool
    source_tokens: int
    verdicts_by_arm: dict[str, str]
    short_circuit_by_arm: dict[str, bool]
    error: Optional[str] = None


def _paper_text(review_dir: Path, paper_id: str) -> Optional[str]:
    parsed_dir = review_dir / "parsed_text"
    md_files = sorted(parsed_dir.glob(f"{paper_id}_v*.md"), reverse=True)
    if not md_files:
        return None
    try:
        return md_files[0].read_text()
    except OSError:
        return None


def _new_run_id(review: str, tag: str = "full") -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{review}_pass2_{tag}_{ts}"


def _check_abort_conditions(
    i: int,
    total: int,
    failures: int,
    t_start: float,
) -> None:
    """Raise AbortError if a terminal condition is hit at this checkpoint."""
    wall = time.time() - t_start
    fail_pct = 100.0 * failures / i
    rate = i / wall if wall > 0 else 0.0
    remaining_sec = (total - i) / rate if rate > 0 else float("inf")
    proj_hours = (wall + remaining_sec) / 3600.0

    if fail_pct > 100.0 * FAIL_RATE_THRESHOLD:
        raise AbortError(
            f"failure rate {fail_pct:.1f}% > {FAIL_RATE_THRESHOLD*100:.0f}% "
            f"at checkpoint {i}/{total}"
        )
    if proj_hours > ABORT_PROJECTION_HOURS:
        raise AbortError(
            f"projected wall-clock {proj_hours:.1f}h > {ABORT_PROJECTION_HOURS:.0f}h "
            f"at checkpoint {i}/{total}"
        )


def _derandomize(result: Pass2Result) -> tuple[dict[str, str], dict[str, bool]]:
    vba: dict[str, str] = {}
    for v in result.pass2.arm_verdicts:
        arm = result.arm_permutation[v.arm_slot - 1]
        vba[arm] = v.verdict
    return vba, dict(result.pre_check_short_circuit_by_arm)


def run_full(
    db: ReviewDatabase,
    pass1_run_id: str,
    candidates: list[Candidate],
    judge_inputs: dict[tuple[str, str], JudgeInput],
    *,
    review: str,
    model: str,
    codebook_path: Path,
    codebook_sha: str,
    run_tag: str,
    dry_run: bool,
    t_start: float,
) -> tuple[str, list[TripleResult]]:
    """Execute Pass 2 verification for every candidate. Incremental DB writes."""
    run_id = _new_run_id(review, tag=run_tag)
    model_digest = fetch_model_digest(model)

    if not dry_run:
        create_judge_run(
            db,
            run_id=run_id,
            judge_model_name=model,
            judge_model_digest=model_digest,
            codebook_sha256=codebook_sha,
            pass_number=2,
            input_scope="AI_TRIPLES",
            run_config={
                "pass1_run_id": pass1_run_id,
                "codebook_path": str(codebook_path),
                "model": model,
                "n_selected": len(candidates),
                "checkpoint_every": CHECKPOINT_EVERY,
                "stall_threshold_sec": STALL_THRESHOLD_SEC,
                "fail_rate_threshold": FAIL_RATE_THRESHOLD,
                "abort_projection_hours": ABORT_PROJECTION_HOURS,
            },
            notes=f"pass2 full run linked to {pass1_run_id}",
        )
        logger.info("Created judge_run %s", run_id)

    review_dir = db.db_path.parent
    results: list[TripleResult] = []
    total = len(candidates)
    failures = 0
    running = {"SUPPORTED": 0, "PARTIALLY_SUPPORTED": 0, "UNSUPPORTED": 0}

    for i, cand in enumerate(candidates, 1):
        t_triple = time.time()
        key = (cand.paper_id, cand.field_name)
        inp = judge_inputs.get(key)

        if inp is None:
            failures += 1
            results.append(TripleResult(
                candidate=cand, wall_sec=0.0, windowed=False, source_tokens=0,
                verdicts_by_arm={}, short_circuit_by_arm={},
                error="JudgeInput lookup miss (not in pairs CSV)",
            ))
            logger.error("MISS [%d/%d] %s/%s: not in pairs CSV",
                         i, total, cand.paper_id, cand.field_name)
            if i % CHECKPOINT_EVERY == 0 and not dry_run:
                _log_checkpoint(i, total, failures, running, t_start)
                _check_abort_conditions(i, total, failures, t_start)
            continue

        source = _paper_text(review_dir, cand.paper_id)
        if source is None:
            failures += 1
            results.append(TripleResult(
                candidate=cand, wall_sec=0.0, windowed=False, source_tokens=0,
                verdicts_by_arm={}, short_circuit_by_arm={},
                error="no parsed text",
            ))
            logger.error("NO_TEXT [%d/%d] %s/%s",
                         i, total, cand.paper_id, cand.field_name)
            if i % CHECKPOINT_EVERY == 0 and not dry_run:
                _log_checkpoint(i, total, failures, running, t_start)
                _check_abort_conditions(i, total, failures, t_start)
            continue

        try:
            result: Pass2Result = run_pass2(
                inp, run_id=run_id, source_text=source, model=model
            )
        except JudgeError as exc:
            elapsed = time.time() - t_triple
            failures += 1
            results.append(TripleResult(
                candidate=cand, wall_sec=elapsed, windowed=False, source_tokens=0,
                verdicts_by_arm={}, short_circuit_by_arm={},
                error=f"{type(exc).__name__}: {exc}",
            ))
            logger.error("FAIL [%d/%d] %s/%s (%.1fs): %s",
                         i, total, cand.paper_id, cand.field_name, elapsed, exc)
            if i % CHECKPOINT_EVERY == 0 and not dry_run:
                _log_checkpoint(i, total, failures, running, t_start)
                _check_abort_conditions(i, total, failures, t_start)
            continue

        elapsed = time.time() - t_triple
        if elapsed > STALL_THRESHOLD_SEC:
            logger.error(
                "STALL [%d/%d] %s/%s took %.0fs (>%ds) — continuing",
                i, total, cand.paper_id, cand.field_name, elapsed, STALL_THRESHOLD_SEC,
            )

        if not dry_run:
            try:
                insert_pass2_verifications(db, run_id, result)
            except JudgeStorageError as exc:
                failures += 1
                vba, scba = _derandomize(result)
                results.append(TripleResult(
                    candidate=cand, wall_sec=elapsed,
                    windowed=result.source_text_windowed,
                    source_tokens=result.source_text_tokens,
                    verdicts_by_arm=vba, short_circuit_by_arm=scba,
                    error=f"insert_pass2_verifications failed: {exc}",
                ))
                logger.error("DB_FAIL [%d/%d] %s/%s: %s",
                             i, total, cand.paper_id, cand.field_name, exc)
                if i % CHECKPOINT_EVERY == 0:
                    _log_checkpoint(i, total, failures, running, t_start)
                    _check_abort_conditions(i, total, failures, t_start)
                continue

        vba, scba = _derandomize(result)
        for v in vba.values():
            running[v] = running.get(v, 0) + 1

        results.append(TripleResult(
            candidate=cand, wall_sec=elapsed,
            windowed=result.source_text_windowed,
            source_tokens=result.source_text_tokens,
            verdicts_by_arm=vba, short_circuit_by_arm=scba,
        ))
        logger.info(
            "OK [%d/%d] %s/%s [%s] %.1fs windowed=%s",
            i, total, cand.paper_id, cand.field_name, cand.risk,
            elapsed, result.source_text_windowed,
        )

        if i % CHECKPOINT_EVERY == 0 and not dry_run:
            _log_checkpoint(i, total, failures, running, t_start)
            _check_abort_conditions(i, total, failures, t_start)

    if not dry_run:
        successes = len(results) - failures
        try:
            complete_judge_run(
                db, run_id,
                n_triples_attempted=len(results),
                n_triples_succeeded=successes,
                n_triples_failed=failures,
            )
        except JudgeStorageError as exc:
            logger.error("complete_judge_run failed: %s", exc)

    return run_id, results


def _log_checkpoint(
    i: int, total: int, failures: int,
    running: dict[str, int], t_start: float,
) -> None:
    wall = time.time() - t_start
    rate = i / wall if wall > 0 else 0.0
    remaining = (total - i) / rate if rate > 0 else 0.0
    proj_hours = (wall + remaining) / 3600.0
    fail_pct = 100.0 * failures / i
    logger.info(
        "CHECKPOINT %d/%d elapsed=%.2fh proj_total=%.2fh "
        "failures=%d (%.1f%%) S=%d PS=%d U=%d",
        i, total, wall / 3600.0, proj_hours, failures, fail_pct,
        running["SUPPORTED"], running["PARTIALLY_SUPPORTED"], running["UNSUPPORTED"],
    )


# ── Preliminary report ────────────────────────────────────────────


def _percentile(sorted_vals: list[float], pct: float) -> float:
    if not sorted_vals:
        return 0.0
    k = int(round((pct / 100.0) * (len(sorted_vals) - 1)))
    return sorted_vals[k]


def _distribution_row(counts: dict[str, int]) -> tuple[int, int, int, int]:
    s = counts.get("SUPPORTED", 0)
    p = counts.get("PARTIALLY_SUPPORTED", 0)
    u = counts.get("UNSUPPORTED", 0)
    return s, p, u, s + p + u


def _pct(n: int, total: int) -> str:
    return f"{100.0*n/total:.1f}%" if total else "—"


def build_preliminary_report(
    db: ReviewDatabase,
    run_id: str,
    pass1_run_id: str,
    model: str,
    codebook_sha: str,
    results: list[TripleResult],
    t_start: float,
    t_end: float,
) -> str:
    successes = [r for r in results if r.error is None]
    failures = [r for r in results if r.error is not None]
    latencies = sorted(r.wall_sec for r in successes if r.wall_sec > 0)
    p50 = _percentile(latencies, 50)
    p95 = _percentile(latencies, 95)
    wall = t_end - t_start

    # Overall + per-arm from DB.
    cur = db._conn.execute(
        """SELECT arm_name, verdict, COUNT(*) AS n
           FROM fabrication_verifications
           WHERE judge_run_id = ?
           GROUP BY arm_name, verdict""",
        (run_id,),
    ).fetchall()
    per_arm: dict[str, dict[str, int]] = {}
    overall: dict[str, int] = {}
    for r in cur:
        per_arm.setdefault(r["arm_name"], {})[r["verdict"]] = r["n"]
        overall[r["verdict"]] = overall.get(r["verdict"], 0) + r["n"]
    total_rows = sum(overall.values())

    # Per field_type (join via judge_ratings for field_type).
    cur = db._conn.execute(
        """SELECT jr.field_type AS ft, fv.verdict, COUNT(*) AS n
           FROM fabrication_verifications fv
           JOIN judge_ratings jr
             ON jr.run_id = ?
            AND jr.paper_id = fv.paper_id
            AND jr.field_name = fv.field_name
           WHERE fv.judge_run_id = ?
           GROUP BY jr.field_type, fv.verdict""",
        (pass1_run_id, run_id),
    ).fetchall()
    per_ftype: dict[str, dict[str, int]] = {}
    for r in cur:
        per_ftype.setdefault(r["ft"], {})[r["verdict"]] = r["n"]

    # Per arm × field (60 cells).
    cur = db._conn.execute(
        """SELECT fv.field_name, fv.arm_name, fv.verdict, COUNT(*) AS n
           FROM fabrication_verifications fv
           WHERE fv.judge_run_id = ?
           GROUP BY fv.field_name, fv.arm_name, fv.verdict""",
        (run_id,),
    ).fetchall()
    per_armfield: dict[tuple[str, str], dict[str, int]] = {}
    for r in cur:
        per_armfield.setdefault(
            (r["field_name"], r["arm_name"]), {}
        )[r["verdict"]] = r["n"]

    # UNSUPPORTED top 10 papers.
    top_unsupp = db._conn.execute(
        """SELECT paper_id, COUNT(*) AS n
           FROM fabrication_verifications
           WHERE judge_run_id = ? AND verdict = 'UNSUPPORTED'
           GROUP BY paper_id
           ORDER BY n DESC, CAST(paper_id AS INTEGER) ASC
           LIMIT 10""",
        (run_id,),
    ).fetchall()

    # Windowed path stats (from in-memory results).
    windowed = [r for r in successes if r.windowed]
    windowed_verdicts: dict[str, int] = {}
    for r in windowed:
        for v in r.verdicts_by_arm.values():
            windowed_verdicts[v] = windowed_verdicts.get(v, 0) + 1

    # Short-circuit firing (from DB).
    sc_total = db._conn.execute(
        """SELECT COUNT(*) FROM fabrication_verifications
           WHERE judge_run_id = ? AND pre_check_short_circuit = 1""",
        (run_id,),
    ).fetchone()[0]

    # ── Render
    L: list[str] = []
    L.append(f"# Pass 2 full — preliminary report · `{run_id}`")
    L.append("")
    L.append(f"**Parent Pass 1 run:** `{pass1_run_id}`")
    L.append(f"**Model:** `{model}` · codebook_sha256 `{codebook_sha}`")
    L.append(f"**Start:** {datetime.fromtimestamp(t_start, tz=timezone.utc).isoformat()}")
    L.append(f"**End:**   {datetime.fromtimestamp(t_end,   tz=timezone.utc).isoformat()}")
    L.append(
        f"**Wall-clock:** {wall/3600:.2f}h · "
        f"latency p50={p50:.1f}s · p95={p95:.1f}s"
    )
    L.append(
        f"**Triples:** {len(results)} attempted · {len(successes)} succeeded · "
        f"{len(failures)} failed"
    )
    L.append(
        f"**Verification rows written:** {total_rows} "
        f"(target {len(successes)*3} = successes × 3)"
    )
    L.append("")

    # Overall distribution
    L.append("## Overall verdict distribution")
    L.append("")
    L.append("| verdict | n | % |")
    L.append("|---|---:|---:|")
    for v in ("SUPPORTED", "PARTIALLY_SUPPORTED", "UNSUPPORTED"):
        n = overall.get(v, 0)
        L.append(f"| {v} | {n} | {_pct(n, total_rows)} |")
    L.append(f"| **total** | **{total_rows}** | 100.0% |")
    L.append("")

    # Per-arm
    L.append("## Verdict distribution by arm")
    L.append("")
    L.append("| arm | SUPPORTED | PARTIALLY_SUPPORTED | UNSUPPORTED | total | U% |")
    L.append("|---|---:|---:|---:|---:|---:|")
    for arm in ("local", "openai_o4_mini_high", "anthropic_sonnet_4_6"):
        s, p, u, t = _distribution_row(per_arm.get(arm, {}))
        L.append(f"| {arm} | {s} | {p} | {u} | {t} | {_pct(u, t)} |")
    L.append("")

    # Per field_type
    L.append("## Verdict distribution by field_type")
    L.append("")
    L.append("| field_type | SUPPORTED | PARTIALLY_SUPPORTED | UNSUPPORTED | total | U% |")
    L.append("|---|---:|---:|---:|---:|---:|")
    for ft in sorted(per_ftype.keys()):
        s, p, u, t = _distribution_row(per_ftype[ft])
        L.append(f"| {ft} | {s} | {p} | {u} | {t} | {_pct(u, t)} |")
    L.append("")

    # Per arm × field
    L.append("## Verdict distribution by arm × field (60 cells)")
    L.append("")
    L.append(
        "| field | arm | SUPPORTED | PARTIALLY_SUPPORTED | UNSUPPORTED | total |"
    )
    L.append("|---|---|---:|---:|---:|---:|")
    field_names = sorted({k[0] for k in per_armfield.keys()})
    for fn in field_names:
        for arm in ("local", "openai_o4_mini_high", "anthropic_sonnet_4_6"):
            s, p, u, t = _distribution_row(per_armfield.get((fn, arm), {}))
            if t == 0:
                continue
            L.append(f"| {fn} | {arm} | {s} | {p} | {u} | {t} |")
    L.append("")

    # Top 10 UNSUPPORTED papers
    L.append("## Top 10 papers by UNSUPPORTED count")
    L.append("")
    L.append("| paper_id | UNSUPPORTED |")
    L.append("|---|---:|")
    for r in top_unsupp:
        L.append(f"| {r['paper_id']} | {r['n']} |")
    if not top_unsupp:
        L.append("| _(none)_ | 0 |")
    L.append("")

    # Windowed path
    L.append("## Windowed-path triples")
    L.append("")
    L.append(
        f"- **Count:** {len(windowed)}/{len(successes)} triples "
        f"({_pct(len(windowed), len(successes))})"
    )
    if windowed:
        L.append("- **Verdict distribution (arm-rows in windowed triples):**")
        L.append("")
        L.append("| verdict | n |")
        L.append("|---|---:|")
        for v in ("SUPPORTED", "PARTIALLY_SUPPORTED", "UNSUPPORTED"):
            L.append(f"| {v} | {windowed_verdicts.get(v, 0)} |")
    L.append("")

    # Short-circuit
    L.append("## Short-circuit firing")
    L.append("")
    L.append(
        f"- **Arm-rows with short_circuit=True:** {sc_total}/{total_rows} "
        f"({_pct(sc_total, total_rows)})"
    )
    L.append("")

    # Failures
    if failures:
        L.append("## Failures")
        L.append("")
        L.append("| paper_id | field_name | error |")
        L.append("|---|---|---|")
        for r in failures:
            err = (r.error or "").replace("|", "\\|")[:160]
            L.append(f"| {r.candidate.paper_id} | {r.candidate.field_name} | {err} |")
        L.append("")

    L.append("---")
    L.append(
        "_PI review gate: do NOT proceed to Pass 2 interpretation or audit "
        "sampling until this report is reviewed._"
    )
    L.append("")
    return "\n".join(L)


# ── CLI ────────────────────────────────────────────────────────────


def _resolve_db(args) -> ReviewDatabase:
    if args.data_root is not None:
        return ReviewDatabase(args.review, data_root=args.data_root)
    return ReviewDatabase(args.review)


def _setup_logging(log_dir: Path, tag: str) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_file = log_dir / f"pass2_{tag}_{ts}.log"
    fmt = logging.Formatter(
        "%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    root = logging.getLogger()
    if not root.handlers:
        root.setLevel(logging.INFO)
        sh = logging.StreamHandler(sys.stderr)
        sh.setFormatter(fmt)
        root.addHandler(sh)
        fh = logging.FileHandler(log_file)
        fh.setFormatter(fmt)
        root.addHandler(fh)
    return log_file


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="analysis.paper1.pass2_full",
        description="Run Pass 2 fabrication verification over all "
                    "medium+high risk triples for a parent Pass 1 run.",
    )
    p.add_argument("--review", required=True)
    p.add_argument("--pass1-run-id", required=True)
    p.add_argument("--pairs-csv", required=True, type=Path)
    p.add_argument("--codebook", required=True, type=Path)
    p.add_argument("--out-dir", type=Path,
                   default=Path("analysis/paper1/reports"))
    p.add_argument("--log-dir", type=Path,
                   default=Path("analysis/paper1/logs"))
    p.add_argument("--model", default=DEFAULT_MODEL)
    p.add_argument("--run-tag", default="full",
                   help="Inserted into run_id: <review>_pass2_<tag>_<ts>")
    p.add_argument("--limit", type=int, default=None,
                   help="Cap triple count (sanity/smoke use only)")
    p.add_argument("--dry-run", action="store_true",
                   help="No DB writes / no Ollama call (plan-only)")
    p.add_argument("--data-root", type=Path, default=None)
    return p


def run(argv: Optional[list[str]] = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    log_file = _setup_logging(args.log_dir, args.run_tag)
    logger.info("pass2 FULL run starting; log=%s", log_file)

    db = _resolve_db(args)
    codebook = load_codebook(args.codebook)
    codebook_sha = compute_codebook_sha256(args.codebook)

    logger.info("Fetching triple universe from judge_ratings")
    candidates = fetch_triples(db, args.pass1_run_id)
    if args.limit is not None:
        candidates = candidates[: args.limit]
        logger.info("Limited to first %d triples (flag: --limit)", args.limit)
    logger.info(
        "Universe: %d triples (%d high, %d medium)",
        len(candidates),
        sum(1 for c in candidates if c.risk == "high"),
        sum(1 for c in candidates if c.risk == "medium"),
    )

    logger.info("Loading pairs CSV to build per-triple JudgeInput")
    inputs_list = load_ai_triples_csv(
        args.pairs_csv, db, codebook, limit=None
    )
    judge_inputs = {(i.paper_id, i.field_name): i for i in inputs_list}

    if args.dry_run:
        missing = [
            (c.paper_id, c.field_name) for c in candidates
            if (c.paper_id, c.field_name) not in judge_inputs
        ]
        print(f"DRY RUN: {len(candidates)} triples planned, "
              f"{len(missing)} missing from pairs CSV")
        for pid, fn in missing[:10]:
            print(f"  missing: {pid}/{fn}")
        db.close()
        return 0

    t_start = time.time()
    try:
        run_id, results = run_full(
            db, args.pass1_run_id, candidates, judge_inputs,
            review=args.review, model=args.model,
            codebook_path=args.codebook, codebook_sha=codebook_sha,
            run_tag=args.run_tag, dry_run=False, t_start=t_start,
        )
    except AbortError as exc:
        logger.error("ABORT: %s", exc)
        db.close()
        return 2
    t_end = time.time()

    logger.info("Building preliminary report")
    md = build_preliminary_report(
        db, run_id, args.pass1_run_id, args.model, codebook_sha,
        results, t_start, t_end,
    )
    args.out_dir.mkdir(parents=True, exist_ok=True)
    report_path = args.out_dir / f"pass2_full_{run_id}_preliminary.md"
    report_path.write_text(md)
    logger.info("Wrote preliminary report: %s", report_path)

    db.close()
    successes = sum(1 for r in results if r.error is None)
    failures = len(results) - successes
    print(
        f"\npass2 FULL complete — {successes}/{len(results)} succeeded, "
        f"{failures} failed\nreport: {report_path}"
    )
    return 0 if failures == 0 else 1


def main() -> None:
    raise SystemExit(run())


if __name__ == "__main__":
    main()
