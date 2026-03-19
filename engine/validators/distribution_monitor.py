"""Post-extraction distribution collapse detector.

Flags categorical fields where a single value dominates the corpus beyond
a reasonable threshold. Catches prompt deficiencies, model failure modes,
and codebook misconfigurations before data reaches concordance or human review.

CLI:
    python -m engine.validators.distribution_monitor \\
        --review surgical_autonomy --arm local [--strict]
"""

import argparse
import json
import logging
import math
import sqlite3
import sys
from collections import Counter
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)

# Values that represent absence — excluded from distribution analysis.
_NULL_SYNONYMS = {"", "nr", "n/r", "not reported", "not_found", "none", "n/a"}


class DistributionCollapseError(Exception):
    """Raised when a categorical field has zero variance (COLLAPSED)."""

    def __init__(self, collapsed_fields: list[dict]):
        self.collapsed_fields = collapsed_fields
        names = [f["field_name"] for f in collapsed_fields]
        super().__init__(
            f"Distribution collapse detected in {len(collapsed_fields)} field(s): "
            + ", ".join(names)
        )


# ── Codebook loading ────────────────────────────────────────────────


def _load_categorical_fields(codebook_path: Path) -> list[str]:
    """Return names of categorical fields from codebook YAML."""
    with open(codebook_path) as f:
        cb = yaml.safe_load(f)
    return [
        fd["name"]
        for fd in cb.get("fields", [])
        if fd.get("type") == "categorical"
    ]


# ── Value queries by arm type ────────────────────────────────────────


def _is_null(value: str | None) -> bool:
    """Check if a value represents absence."""
    if value is None:
        return True
    return value.strip().lower() in _NULL_SYNONYMS


def _query_local_values(conn: sqlite3.Connection, field_name: str) -> list[str]:
    """Get all values for a field from local evidence_spans."""
    rows = conn.execute(
        """SELECT es.value
           FROM evidence_spans es
           JOIN extractions e ON e.id = es.extraction_id
           WHERE es.field_name = ?""",
        (field_name,),
    ).fetchall()
    return [r[0] for r in rows if not _is_null(r[0])]


def _query_cloud_values(conn: sqlite3.Connection, field_name: str, arm: str) -> list[str]:
    """Get all values for a field from cloud_evidence_spans for a specific arm."""
    rows = conn.execute(
        """SELECT cs.value
           FROM cloud_evidence_spans cs
           JOIN cloud_extractions ce ON ce.id = cs.cloud_extraction_id
           WHERE cs.field_name = ? AND ce.arm = ?""",
        (field_name, arm),
    ).fetchall()
    return [r[0] for r in rows if not _is_null(r[0])]


def _query_human_values(conn: sqlite3.Connection, field_name: str, extractor_id: str) -> list[str]:
    """Get all values for a field from human_extractions for a specific extractor."""
    rows = conn.execute(
        """SELECT value FROM human_extractions
           WHERE field_name = ? AND extractor_id = ?""",
        (field_name, extractor_id),
    ).fetchall()
    return [r[0] for r in rows if not _is_null(r[0])]


def _query_values(conn: sqlite3.Connection, field_name: str, arm: str) -> list[str]:
    """Route value query to the right table based on arm name."""
    if arm == "local":
        return _query_local_values(conn, field_name)
    if arm.startswith("human_"):
        extractor_id = arm.split("_", 1)[1]
        return _query_human_values(conn, field_name, extractor_id)
    return _query_cloud_values(conn, field_name, arm)


# ── Shannon entropy ─────────────────────────────────────────────────


def shannon_entropy(values: list[str]) -> float:
    """Compute Shannon entropy H = -Σ(p * log2(p)).

    Returns 0.0 for empty input or single-value distributions.
    """
    if not values:
        return 0.0
    counts = Counter(values)
    total = len(values)
    entropy = 0.0
    for count in counts.values():
        p = count / total
        if p > 0:
            entropy -= p * math.log2(p)
    return entropy


# ── Main check ───────────────────────────────────────────────────────


def check_distribution(
    db_path: Path,
    review_name: str,
    arm: str,
    codebook_path: Path,
) -> list[dict]:
    """Check distribution of each categorical field for an extraction arm.

    Returns list of dicts with: field_name, arm, total_non_null, distinct_count,
    top_value, top_value_pct, entropy, status.
    """
    categorical_fields = _load_categorical_fields(codebook_path)

    conn = sqlite3.connect(str(db_path))
    try:
        results: list[dict] = []
        for field_name in categorical_fields:
            values = _query_values(conn, field_name, arm)
            total_non_null = len(values)

            if total_non_null == 0:
                results.append({
                    "field_name": field_name,
                    "arm": arm,
                    "total_non_null": 0,
                    "distinct_count": 0,
                    "top_value": None,
                    "top_value_pct": 0.0,
                    "entropy": 0.0,
                    "status": "OK",
                    "distribution": {},
                })
                continue

            counts = Counter(values)
            distinct_count = len(counts)
            top_value, top_count = counts.most_common(1)[0]
            top_value_pct = top_count / total_non_null

            # Determine status
            if distinct_count <= 1 and total_non_null >= 10:
                status = "COLLAPSED"
            elif top_value_pct >= 0.85 and total_non_null >= 20:
                status = "LOW_VARIANCE"
            else:
                status = "OK"

            results.append({
                "field_name": field_name,
                "arm": arm,
                "total_non_null": total_non_null,
                "distinct_count": distinct_count,
                "top_value": top_value,
                "top_value_pct": top_value_pct,
                "entropy": shannon_entropy(values),
                "status": status,
                "distribution": dict(counts),
            })
    finally:
        conn.close()

    return results


# ── Report ───────────────────────────────────────────────────────────


def print_distribution_report(results: list[dict]) -> None:
    """Terminal-formatted distribution report, grouped by status."""
    # Detect color support
    use_color = hasattr(sys.stdout, "isatty") and sys.stdout.isatty()

    def _red(s: str) -> str:
        return f"\033[91m{s}\033[0m" if use_color else s

    def _yellow(s: str) -> str:
        return f"\033[93m{s}\033[0m" if use_color else s

    def _green(s: str) -> str:
        return f"\033[92m{s}\033[0m" if use_color else s

    def _status_fmt(status: str) -> str:
        if status == "COLLAPSED":
            return _red(status)
        if status == "LOW_VARIANCE":
            return _yellow(status)
        return _green(status)

    # Group by status priority
    status_order = {"COLLAPSED": 0, "LOW_VARIANCE": 1, "OK": 2}
    sorted_results = sorted(results, key=lambda r: (status_order.get(r["status"], 3), r["field_name"]))

    header = f"{'Field':<35s} {'N':>5s} {'Dist':>5s} {'Top Value':<30s} {'Top%':>6s} {'H':>6s} {'Status':<14s}"
    print(header)
    print("-" * len(header.replace("\033[91m", "").replace("\033[93m", "").replace("\033[92m", "").replace("\033[0m", "")))

    for r in sorted_results:
        top_val = r["top_value"] or "—"
        if len(top_val) > 28:
            top_val = top_val[:25] + "..."

        print(
            f"{r['field_name']:<35s} "
            f"{r['total_non_null']:>5d} "
            f"{r['distinct_count']:>5d} "
            f"{top_val:<30s} "
            f"{r['top_value_pct']:>5.1%} "
            f"{r['entropy']:>6.2f} "
            f"{_status_fmt(r['status'])}"
        )

        # Show full distribution for flagged fields
        if r["status"] in ("COLLAPSED", "LOW_VARIANCE") and r.get("distribution"):
            dist = r["distribution"]
            for val, count in sorted(dist.items(), key=lambda x: -x[1]):
                pct = count / r["total_non_null"]
                bar = "█" * int(pct * 20)
                print(f"  {'':>35s} {count:>5d}  {bar:<20s} {pct:>5.1%}  {val}")

    # Summary
    collapsed = sum(1 for r in results if r["status"] == "COLLAPSED")
    low_var = sum(1 for r in results if r["status"] == "LOW_VARIANCE")
    ok = sum(1 for r in results if r["status"] == "OK")
    print(f"\nSummary: {_green(f'{ok} OK')}, {_yellow(f'{low_var} LOW_VARIANCE')}, {_red(f'{collapsed} COLLAPSED')}")


# ── Assertion gate ───────────────────────────────────────────────────


def assert_no_collapse(results: list[dict], strict: bool = False) -> None:
    """Raise DistributionCollapseError if any field is COLLAPSED.

    With strict=True, also raises on LOW_VARIANCE fields.
    """
    collapsed = [r for r in results if r["status"] == "COLLAPSED"]
    low_var = [r for r in results if r["status"] == "LOW_VARIANCE"]

    if low_var:
        for r in low_var:
            logger.warning(
                "LOW_VARIANCE: %s — %s at %.0f%% (%d/%d)",
                r["field_name"], r["top_value"], r["top_value_pct"] * 100,
                int(r["top_value_pct"] * r["total_non_null"]), r["total_non_null"],
            )

    failures = collapsed
    if strict:
        failures = collapsed + low_var

    if failures:
        raise DistributionCollapseError(failures)


# ── Automatic post-extraction gate ────────────────────────────────────


def run_post_extraction_check(
    db_path: Path,
    review_name: str,
    arm: str,
    codebook_path: Path,
    *,
    extracted_count: int = 0,
    failed_count: int = 0,
    strict: bool = False,
) -> dict:
    """Run distribution monitor as an automatic post-extraction quality gate.

    Called at the end of extraction runs. Logs per-field results, then calls
    assert_no_collapse() — raises DistributionCollapseError on COLLAPSED fields
    (or LOW_VARIANCE too when strict=True).

    Returns a summary dict with keys: ok, low_variance, collapsed, skipped,
    collapsed_fields.

    If extraction was partial (failed > 0 or extracted < 10), skips the check
    and logs a reason.
    """
    summary = {
        "ok": 0,
        "low_variance": 0,
        "collapsed": 0,
        "skipped": True,
        "collapsed_fields": [],
        "low_variance_fields": [],
    }

    if extracted_count < 10:
        logger.info(
            "Distribution monitor skipped: only %d papers extracted "
            "(minimum 10 required for meaningful analysis)",
            extracted_count,
        )
        return summary

    if failed_count > 0:
        logger.info(
            "Distribution monitor skipped: %d papers failed extraction "
            "(partial run — re-run monitor manually after retry)",
            failed_count,
        )
        return summary

    if not codebook_path.exists():
        logger.warning(
            "Distribution monitor skipped: codebook not found at %s",
            codebook_path,
        )
        return summary

    summary["skipped"] = False
    logger.info("Running distribution monitor for arm '%s'...", arm)

    results = check_distribution(db_path, review_name, arm, codebook_path)

    for r in results:
        if r["status"] == "COLLAPSED":
            summary["collapsed"] += 1
            summary["collapsed_fields"].append(r["field_name"])
            logger.error(
                "COLLAPSED: %s — only value '%s' across %d papers (entropy=%.2f). "
                "This field has zero variance and needs investigation.",
                r["field_name"], r["top_value"],
                r["total_non_null"], r["entropy"],
            )
        elif r["status"] == "LOW_VARIANCE":
            summary["low_variance"] += 1
            summary["low_variance_fields"].append(r["field_name"])
            logger.warning(
                "LOW_VARIANCE: %s — '%s' at %.0f%% (%d/%d, entropy=%.2f)",
                r["field_name"], r["top_value"],
                r["top_value_pct"] * 100,
                int(r["top_value_pct"] * r["total_non_null"]),
                r["total_non_null"], r["entropy"],
            )
        else:
            summary["ok"] += 1

    logger.info(
        "Distribution monitor complete: %d OK, %d LOW_VARIANCE, %d COLLAPSED",
        summary["ok"], summary["low_variance"], summary["collapsed"],
    )

    # Fail-fast: raise on COLLAPSED (or LOW_VARIANCE in strict mode)
    assert_no_collapse(results, strict=strict)

    return summary


# ── CLI ──────────────────────────────────────────────────────────────


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="Post-extraction distribution collapse detector"
    )
    parser.add_argument("--review", required=True, help="Review name")
    parser.add_argument("--arm", required=True, help="Extraction arm (local, cloud arm name, human_X)")
    parser.add_argument("--codebook", type=Path, default=None, help="Codebook YAML path (auto-detected)")
    parser.add_argument("--strict", action="store_true", help="Treat LOW_VARIANCE as failure too")

    args = parser.parse_args()

    # Resolve DB path
    from engine.core.database import ReviewDatabase
    db = ReviewDatabase(args.review)
    db_path = Path(db.db_path)
    data_dir = db_path.parent
    db.close()

    codebook_path = args.codebook or (data_dir / "extraction_codebook.yaml")
    if not codebook_path.exists():
        logger.error("Codebook not found: %s", codebook_path)
        raise SystemExit(1)

    results = check_distribution(db_path, args.review, args.arm, codebook_path)
    print_distribution_report(results)

    try:
        assert_no_collapse(results, strict=args.strict)
    except DistributionCollapseError as exc:
        logger.error(str(exc))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
