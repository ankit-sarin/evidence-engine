"""Concordance analysis orchestrator.

Loads extractions from multiple arms, aligns by paper, scores field pairs,
and computes agreement metrics.

CLI:
    python -m engine.analysis.concordance --review surgical_autonomy \\
        --arms local,openai_o4_mini_high,anthropic_sonnet_4_6
"""

import argparse
import csv
import json
import logging
import sqlite3
import sys
from dataclasses import asdict, dataclass, field
from itertools import combinations
from pathlib import Path

from engine.analysis.metrics import FieldSummary, field_summary
from engine.analysis.scoring import FieldScore, score_pair

logger = logging.getLogger(__name__)


@dataclass
class Disagreement:
    """A single MISMATCH or AMBIGUOUS pair."""

    paper_id: int
    field_name: str
    value_a: str | None
    value_b: str | None
    result: str
    detail: str


@dataclass
class ConcordanceReport:
    """Full concordance comparison between two arms."""

    arm_a: str
    arm_b: str
    n_papers: int
    n_papers_a_only: int
    n_papers_b_only: int
    field_summaries: dict[str, FieldSummary]
    disagreements: list[Disagreement]


def load_arm(db_path: str, arm: str) -> dict[int, dict[str, str]]:
    """Load extracted values for an arm.

    Args:
        db_path: Path to review.db
        arm: "local" for the local extraction arm, or a cloud arm name
             (e.g. "openai_o4_mini_high", "anthropic_sonnet_4_6").

    Returns:
        {paper_id: {field_name: value}} — empty dict when no data exists
        for the arm (valid result).

    Raises:
        sqlite3.OperationalError: If the database is missing or corrupted.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    try:
        result: dict[int, dict[str, str]] = {}

        if arm == "local":
            rows = conn.execute(
                """SELECT e.paper_id, es.field_name, es.value
                   FROM evidence_spans es
                   JOIN extractions e ON e.id = es.extraction_id
                   ORDER BY e.paper_id, es.field_name"""
            ).fetchall()
            for row in rows:
                pid = row["paper_id"]
                if pid not in result:
                    result[pid] = {}
                result[pid][row["field_name"]] = row["value"]
        else:
            rows = conn.execute(
                """SELECT ce.paper_id, cs.field_name, cs.value
                   FROM cloud_evidence_spans cs
                   JOIN cloud_extractions ce ON ce.id = cs.cloud_extraction_id
                   WHERE ce.arm = ?
                   ORDER BY ce.paper_id, cs.field_name""",
                (arm,),
            ).fetchall()
            for row in rows:
                pid = row["paper_id"]
                if pid not in result:
                    result[pid] = {}
                result[pid][row["field_name"]] = row["value"]

        return result
    finally:
        conn.close()


def align_arms(
    arm_a: dict[int, dict[str, str]],
    arm_b: dict[int, dict[str, str]],
) -> tuple[list[tuple[int, str, str | None, str | None]], set[int], set[int]]:
    """Align two arms by paper_id and field_name.

    Returns:
        - List of (paper_id, field_name, value_a, value_b) for overlapping papers
        - Set of paper_ids only in arm_a
        - Set of paper_ids only in arm_b
    """
    keys_a = set(arm_a.keys())
    keys_b = set(arm_b.keys())
    shared = sorted(keys_a & keys_b)
    a_only = keys_a - keys_b
    b_only = keys_b - keys_a

    if a_only:
        logger.info("Papers only in arm A (%d): %s", len(a_only), sorted(a_only)[:10])
    if b_only:
        logger.info("Papers only in arm B (%d): %s", len(b_only), sorted(b_only)[:10])

    aligned: list[tuple[int, str, str | None, str | None]] = []
    for pid in shared:
        fields_a = arm_a[pid]
        fields_b = arm_b[pid]
        all_fields = sorted(set(fields_a.keys()) | set(fields_b.keys()))
        for fname in all_fields:
            val_a = fields_a.get(fname)
            val_b = fields_b.get(fname)
            aligned.append((pid, fname, val_a, val_b))

    return aligned, a_only, b_only


def check_schema_parity(db_path: str, arms: list[str]) -> dict[str, set[str]]:
    """Verify all arms used the same extraction schema hash.

    Queries distinct extraction_schema_hash values for each arm from the
    local ``extractions`` and ``cloud_extractions`` tables.

    Returns ``{arm: {hash, ...}}`` mapping.  Logs a WARNING if hashes differ
    across arms, but does not block execution.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    arm_hashes: dict[str, set[str]] = {}

    for arm in arms:
        if arm == "local":
            rows = conn.execute(
                "SELECT DISTINCT extraction_schema_hash FROM extractions"
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT DISTINCT extraction_schema_hash FROM cloud_extractions WHERE arm = ?",
                (arm,),
            ).fetchall()
        hashes = {r[0] for r in rows if r[0]}
        arm_hashes[arm] = hashes

    conn.close()

    # Check parity across all arms
    all_hashes = set()
    for h in arm_hashes.values():
        all_hashes |= h

    if len(all_hashes) > 1:
        details = ", ".join(
            f"{arm}={sorted(h)}" for arm, h in arm_hashes.items() if h
        )
        logger.warning(
            "Schema hash mismatch across arms — results may not be comparable. %s",
            details,
        )

    return arm_hashes


def run_concordance(
    db_path: str,
    arm_a: str,
    arm_b: str,
    spec_path: str | None = None,
) -> ConcordanceReport:
    """Full concordance pipeline: load → align → score → metrics."""
    from engine.core.review_spec import ReviewSpec, load_review_spec

    spec = load_review_spec(spec_path) if spec_path else None

    # Check schema parity before comparing
    check_schema_parity(db_path, [arm_a, arm_b])

    data_a = load_arm(db_path, arm_a)
    data_b = load_arm(db_path, arm_b)

    aligned, a_only, b_only = align_arms(data_a, data_b)

    # Score each aligned pair
    scored_by_field: dict[str, list[FieldScore]] = {}
    disagreements: list[Disagreement] = []

    for pid, fname, val_a, val_b in aligned:
        fs = score_pair(fname, val_a, val_b, spec)

        if fname not in scored_by_field:
            scored_by_field[fname] = []
        scored_by_field[fname].append(fs)

        if fs.result != "MATCH":
            disagreements.append(Disagreement(
                paper_id=pid,
                field_name=fname,
                value_a=val_a,
                value_b=val_b,
                result=fs.result,
                detail=fs.detail,
            ))

    # Compute per-field summaries
    summaries = {
        fname: field_summary(fname, scores)
        for fname, scores in sorted(scored_by_field.items())
    }

    shared_papers = set(data_a.keys()) & set(data_b.keys())

    return ConcordanceReport(
        arm_a=arm_a,
        arm_b=arm_b,
        n_papers=len(shared_papers),
        n_papers_a_only=len(a_only),
        n_papers_b_only=len(b_only),
        field_summaries=summaries,
        disagreements=disagreements,
    )


def run_all_pairs(
    db_path: str,
    arms: list[str],
    spec_path: str | None = None,
) -> list[ConcordanceReport]:
    """Run concordance for every unique pair of arms."""
    reports = []
    for arm_a, arm_b in combinations(arms, 2):
        logger.info("Running concordance: %s vs %s", arm_a, arm_b)
        report = run_concordance(db_path, arm_a, arm_b, spec_path)
        reports.append(report)
    return reports


# ── CLI ─────────────────────────────────────────────────────────────


def _print_report(report: ConcordanceReport) -> None:
    """Print a concordance report to stdout."""
    print(f"\n{'=' * 72}")
    print(f"  {report.arm_a}  vs  {report.arm_b}")
    print(f"  Papers: {report.n_papers} shared, "
          f"{report.n_papers_a_only} only-A, {report.n_papers_b_only} only-B")
    print(f"{'=' * 72}")

    print(f"\n{'Field':<40} {'κ':>6} {'95% CI':>16} {'%Agr':>6} "
          f"{'n':>4} {'Match':>5} {'Mis':>5} {'Amb':>5}")
    print("-" * 92)

    for fname, fs in report.field_summaries.items():
        ci = f"[{fs.ci_lower:.2f}, {fs.ci_upper:.2f}]" if not _is_nan(fs.ci_lower) else "N/A"
        kappa_str = f"{fs.kappa:.3f}" if not _is_nan(fs.kappa) else "N/A"
        pct_str = f"{fs.percent_agreement:.1%}" if not _is_nan(fs.percent_agreement) else "N/A"
        print(f"{fname:<40} {kappa_str:>6} {ci:>16} {pct_str:>6} "
              f"{fs.n:>4} {fs.n_match:>5} {fs.n_mismatch:>5} {fs.n_ambiguous:>5}")

    if report.disagreements:
        print(f"\nDisagreements ({len(report.disagreements)}):")
        for d in report.disagreements[:20]:
            tag = "MISMATCH" if d.result == "MISMATCH" else "AMBIG"
            print(f"  [{tag}] paper {d.paper_id} / {d.field_name}: "
                  f"{_trunc(d.value_a)} vs {_trunc(d.value_b)}")
        if len(report.disagreements) > 20:
            print(f"  ... and {len(report.disagreements) - 20} more")


def _is_nan(v: float) -> bool:
    import math
    return math.isnan(v)


def _trunc(v: str | None, maxlen: int = 40) -> str:
    if v is None:
        return "None"
    return v[:maxlen] + "..." if len(v) > maxlen else v


def _save_report(report: ConcordanceReport, output_dir: Path) -> None:
    """Save detailed report as JSON."""
    output_dir.mkdir(parents=True, exist_ok=True)
    fname = f"concordance_{report.arm_a}_vs_{report.arm_b}.json"
    path = output_dir / fname

    data = {
        "arm_a": report.arm_a,
        "arm_b": report.arm_b,
        "n_papers": report.n_papers,
        "n_papers_a_only": report.n_papers_a_only,
        "n_papers_b_only": report.n_papers_b_only,
        "field_summaries": {
            fname: asdict(fs) for fname, fs in report.field_summaries.items()
        },
        "disagreements": [asdict(d) for d in report.disagreements],
    }

    path.write_text(json.dumps(data, indent=2, default=str))
    logger.info("Report saved to %s", path)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Run concordance analysis across extraction arms")
    parser.add_argument("--review", required=True, help="Review name (e.g. surgical_autonomy)")
    parser.add_argument(
        "--arms",
        required=True,
        help="Comma-separated arm names (e.g. local,openai_o4_mini_high,anthropic_sonnet_4_6)",
    )
    parser.add_argument("--spec", default=None, help="Path to review spec YAML (optional)")

    args = parser.parse_args()
    arms = [a.strip() for a in args.arms.split(",")]

    from engine.core.database import DATA_ROOT
    db_path = str(DATA_ROOT / args.review / "review.db")
    output_dir = DATA_ROOT / args.review / "analysis"

    spec_path = args.spec
    if not spec_path:
        default = Path(f"review_specs/{args.review}_v1.yaml")
        if default.exists():
            spec_path = str(default)

    reports = run_all_pairs(db_path, arms, spec_path)

    # Per-pair JSON reports (detailed)
    for report in reports:
        _save_report(report, output_dir)

    # Summary + CSV + HTML via report module
    from engine.analysis.report import print_summary, write_report
    print_summary(reports)
    write_report(reports, output_dir)

    print(f"Reports saved to {output_dir}/")


if __name__ == "__main__":
    main()
