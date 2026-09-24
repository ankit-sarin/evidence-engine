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

# ── Default thresholds (overridden by review spec when available) ───
DEFAULT_COLLAPSED_MIN_PAPERS = 10
DEFAULT_LOW_VARIANCE_THRESHOLD = 0.85
DEFAULT_LOW_VARIANCE_MIN_PAPERS = 20

# Malformed-output forms, NOT absence sentinels (R133): an empty string, "n/r"
# and "none" are shapes a model's output takes when it emits nothing usable. They
# are excluded from the observation set for the same reason an absence is, but
# they are this monitor's normalisation, not the codebook's vocabulary. The
# sentinel half of the old `_NULL_SYNONYMS` comes from the codebook
# (`Codebook.absence_sentinel_set`) and is never listed here.
_MALFORMED_NULL_FORMS = frozenset({"", "n/r", "none"})


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


from engine.elicitation.classes import non_value_tokens_for
from engine.core.codebook import load_codebook


def _load_categorical_fields(codebook_path: Path) -> list[str]:
    """Return names of categorical fields from codebook YAML."""
    cb = load_codebook(codebook_path).raw
    return [fd["name"] for fd in cb["fields"] if fd["type"] == "categorical"]


# ── Value queries by arm type ────────────────────────────────────────


def _is_null(value: str | None, non_value: frozenset[str] = frozenset(), *,
             absence_sentinels: frozenset[str]) -> bool:
    """Should this value be excluded from the observation set?

    Three reasons, different in kind. `absence_sentinels` (the codebook's,
    upper-cased; R133) is ABSENCE: the paper does not report the item, which is
    a reading of the paper and simply not a categorical observation.
    `_MALFORMED_NULL_FORMS` is output that carries no reading. `non_value`
    (ELICIT-DESIGN-02 D1, site 4) is a TERMINAL STATE: no reading was recorded
    at all.

    Counting either as a level would put mass on a category the codebook does
    not define, and for a collapse check that is the dangerous direction —
    manufactured variance is exactly what stops COLLAPSED from firing on a field
    that really did collapse. The more fields the engine correctly refused, the
    healthier the distribution would look.
    """
    if value is None:
        return True
    v = str(value).strip()
    if v.upper() in non_value or v.upper() in absence_sentinels:
        return True
    return v.lower() in _MALFORMED_NULL_FORMS


def _query_values(conn: sqlite3.Connection, field_name: str, arm: str,
                  non_value: frozenset[str] = frozenset(), *,
                  codebook=None) -> list[str]:
    """Every value this arm holds for `field_name`, THROUGH THE READER.

    READERS-01 Phase 2a (A12, R12, R30). What was here were three branches —
    `if arm == "local"`, `if arm.startswith("human_")`, else cloud — each hitting
    a different table, and each a private copy of a routing rule. Two things were
    wrong with that beyond the duplication:

    * `concordance.load_arm` had only TWO of the three, so the two files
      disagreed about where a `human_*` arm's values live (A12);
    * the `human_` branch read `human_extractions`, a table that does not exist
      on this review's database at all, so the branch was not merely divergent,
      it was unreachable.

    Routing is now the `arms` registry (R12) and there is no name test left. An
    arm not in the registry raises `UnknownArm` rather than returning `{}`.
    """
    from engine.core.codebook import load_codebook_beside
    from engine.core.effective import UnknownArm, iter_grid, registered_arms

    if arm not in registered_arms(conn, include_retired=True):
        raise UnknownArm(
            f"arm {arm!r} is not in this review's registry — registered arms are "
            f"{registered_arms(conn, include_retired=True)}."
        )

    if codebook is None:
        codebook = load_codebook_beside(_db_path_of(conn))

    return _query_all_fields(conn, arm, non_value, codebook=codebook).get(
        field_name, [])


def _query_all_fields(conn: sqlite3.Connection, arm: str,
                      non_value: frozenset[str] = frozenset(), *,
                      codebook=None) -> dict[str, list[str]]:
    """`{field_name: [value, ...]}` for one arm, in ONE pass over the grid.

    `_query_values` is per-field, and `check_distribution` loops over every
    categorical field — so a per-field implementation that enumerated the whole
    grid each time did |fields| times the work it needed. The caller uses this;
    `_query_values` keeps its signature for the tests and the two external
    callers, and delegates.
    """
    from engine.core.codebook import load_codebook_beside
    from engine.core.effective import UnknownArm, iter_grid, registered_arms

    if arm not in registered_arms(conn, include_retired=True):
        raise UnknownArm(
            f"arm {arm!r} is not in this review's registry — registered arms are "
            f"{registered_arms(conn, include_retired=True)}."
        )
    if codebook is None:
        codebook = load_codebook_beside(_db_path_of(conn))

    out: dict[str, list[str]] = {}
    for _pid, fname, _arm, ev in iter_grid(conn, codebook=codebook, arms=(arm,)):
        if not _is_null(ev.value, non_value,
                        absence_sentinels=codebook.absence_sentinel_set):
            out.setdefault(fname, []).append(ev.value)
    return out


def _db_path_of(conn: sqlite3.Connection) -> str:
    row = conn.execute("PRAGMA database_list").fetchone()
    return row[2]


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
    *,
    collapsed_min_papers: int = DEFAULT_COLLAPSED_MIN_PAPERS,
    low_variance_threshold: float = DEFAULT_LOW_VARIANCE_THRESHOLD,
    low_variance_min_papers: int = DEFAULT_LOW_VARIANCE_MIN_PAPERS,
) -> list[dict]:
    """Check distribution of each categorical field for an extraction arm.

    Returns list of dicts with: field_name, arm, total_non_null, distinct_count,
    top_value, top_value_pct, entropy, status.
    """
    categorical_fields = _load_categorical_fields(codebook_path)
    non_value = non_value_tokens_for(codebook_path)

    from engine.core.codebook import load_codebook_beside

    # mode=ro (I5's family): this validator only ever reads.
    conn = sqlite3.connect(f"file:{Path(db_path).resolve()}?mode=ro", uri=True)
    try:
        codebook = load_codebook_beside(db_path)
        by_field = _query_all_fields(conn, arm, non_value, codebook=codebook)
        results: list[dict] = []
        for field_name in categorical_fields:
            values = by_field.get(field_name, [])
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
            if distinct_count <= 1 and total_non_null >= collapsed_min_papers:
                status = "COLLAPSED"
            elif top_value_pct >= low_variance_threshold and total_non_null >= low_variance_min_papers:
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
    collapsed_min_papers: int = DEFAULT_COLLAPSED_MIN_PAPERS,
    low_variance_threshold: float = DEFAULT_LOW_VARIANCE_THRESHOLD,
    low_variance_min_papers: int = DEFAULT_LOW_VARIANCE_MIN_PAPERS,
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

    results = check_distribution(
        db_path, review_name, arm, codebook_path,
        collapsed_min_papers=collapsed_min_papers,
        low_variance_threshold=low_variance_threshold,
        low_variance_min_papers=low_variance_min_papers,
    )

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
