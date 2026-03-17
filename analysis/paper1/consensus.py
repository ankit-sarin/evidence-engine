"""Majority-vote consensus derivation for shared human extraction papers.

Paper 1 analysis — for the ~30 papers extracted by all 4 human extractors,
derives a consensus "gold standard" value per field via majority vote.
This consensus is the reference standard for comparing AI extraction arms.
"""

import argparse
import json
import logging
import re
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import yaml

from engine.analysis.normalize import normalize_for_concordance

logger = logging.getLogger(__name__)


# ── Shared paper identification ──────────────────────────────────────


def identify_shared_papers(db_path: Path, min_extractors: int = 3) -> list[str]:
    """Return paper_ids with extractions from >= min_extractors distinct extractors.

    Queries the human_extractions table. The ~30-paper overlap is a design
    parameter derived from data, not hardcoded.
    """
    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(
            """SELECT paper_id
               FROM human_extractions
               GROUP BY paper_id
               HAVING COUNT(DISTINCT extractor_id) >= ?
               ORDER BY paper_id""",
            (min_extractors,),
        ).fetchall()
    finally:
        conn.close()
    return [r[0] for r in rows]


# ── Field type lookup ────────────────────────────────────────────────


def _load_field_types(codebook_path: Path) -> dict[str, str]:
    """Return {field_name: type} from codebook YAML."""
    with open(codebook_path) as f:
        cb = yaml.safe_load(f)
    return {fd["name"]: fd["type"] for fd in cb.get("fields", [])}


# ── Normalization helpers ────────────────────────────────────────────


def _normalize_whitespace(s: str) -> str:
    """Lowercase, strip, collapse whitespace for free-text exact clustering."""
    return re.sub(r"\s+", " ", s.strip().lower())


def _values_equal(a, b) -> bool:
    """Compare two normalized values (str or set)."""
    if isinstance(a, set) and isinstance(b, set):
        return a == b
    return a == b


# ── Consensus logic ─────────────────────────────────────────────────


def _majority_vote(values: list[str | set | None], field_type: str) -> dict:
    """Compute majority vote from a list of extractor values.

    Returns dict with: consensus_value, consensus_status, vote_counts.
    """
    # Filter out None/null abstentions
    non_null = [v for v in values if v is not None]

    if len(non_null) < 2:
        return {
            "consensus_value": non_null[0] if non_null else None,
            "consensus_status": "INSUFFICIENT",
            "vote_counts": _count_votes(values),
        }

    if field_type == "free_text":
        return _free_text_majority(non_null, values)

    # Categorical and numeric: exact-match majority on normalized values
    return _exact_majority(non_null, values)


def _exact_majority(non_null: list, all_values: list) -> dict:
    """Exact-match majority for categorical/numeric fields."""
    # Convert sets to frozen sets for hashing
    hashable = []
    for v in non_null:
        if isinstance(v, set):
            hashable.append(frozenset(v))
        else:
            hashable.append(v)

    counts = Counter(hashable)
    most_common_val, most_common_count = counts.most_common(1)[0]
    threshold = len(non_null) / 2  # >50% means strictly more than half

    if most_common_count > threshold:
        # Convert frozenset back to semicolon-joined string for storage
        if isinstance(most_common_val, frozenset):
            consensus_val = "; ".join(sorted(most_common_val))
        else:
            consensus_val = most_common_val

        status = "UNANIMOUS" if most_common_count == len(non_null) else "MAJORITY"
        return {
            "consensus_value": consensus_val,
            "consensus_status": status,
            "vote_counts": _count_votes(all_values),
        }

    return {
        "consensus_value": None,
        "consensus_status": "NO_CONSENSUS",
        "vote_counts": _count_votes(all_values),
    }


def _free_text_majority(non_null: list, all_values: list) -> dict:
    """Free-text majority: cluster by exact whitespace-normalized match."""
    # Normalize for clustering
    normalized = [_normalize_whitespace(str(v)) for v in non_null]

    counts = Counter(normalized)
    most_common_norm, most_common_count = counts.most_common(1)[0]
    threshold = len(non_null) / 2

    if most_common_count > threshold:
        # Return the first original (un-normalized) value from the winning cluster
        for orig, norm in zip(non_null, normalized):
            if norm == most_common_norm:
                consensus_val = orig
                break
        status = "UNANIMOUS" if most_common_count == len(non_null) else "MAJORITY"
        return {
            "consensus_value": consensus_val,
            "consensus_status": status,
            "vote_counts": _count_votes(all_values),
        }

    return {
        "consensus_value": None,
        "consensus_status": "NO_CONSENSUS",
        "vote_counts": _count_votes(all_values),
    }


def _count_votes(values: list) -> dict:
    """Build a vote distribution dict {value_str: count}."""
    dist: dict[str, int] = {}
    for v in values:
        if v is None:
            key = "<null>"
        elif isinstance(v, set):
            key = "; ".join(sorted(v))
        else:
            key = str(v)
        dist[key] = dist.get(key, 0) + 1
    return dist


# ── Main derivation ─────────────────────────────────────────────────


def derive_consensus(db_path: Path, codebook_path: Path) -> list[dict]:
    """Derive majority-vote consensus for each shared paper × field.

    Returns list of dicts with: paper_id, field_name, consensus_value,
    consensus_status, vote_counts, extractor_values.
    """
    field_types = _load_field_types(codebook_path)
    shared_papers = identify_shared_papers(db_path)

    if not shared_papers:
        logger.warning("No shared papers found (need >= 3 extractors per paper)")
        return []

    conn = sqlite3.connect(str(db_path))
    try:
        # Get all field names from the data
        field_rows = conn.execute(
            "SELECT DISTINCT field_name FROM human_extractions ORDER BY field_name"
        ).fetchall()
        field_names = [r[0] for r in field_rows]

        results: list[dict] = []

        for paper_id in shared_papers:
            for field_name in field_names:
                rows = conn.execute(
                    """SELECT extractor_id, value
                       FROM human_extractions
                       WHERE paper_id = ? AND field_name = ?
                       ORDER BY extractor_id""",
                    (paper_id, field_name),
                ).fetchall()

                if not rows:
                    continue

                # Build extractor_values map and normalized values list
                extractor_values: dict[str, str | None] = {}
                normalized_values: list = []

                ft = field_types.get(field_name, "free_text")

                for extractor_id, raw_value in rows:
                    extractor_values[extractor_id] = raw_value
                    # Normalize using the same logic as concordance
                    normed = normalize_for_concordance(field_name, raw_value)
                    normalized_values.append(normed)

                vote = _majority_vote(normalized_values, ft)

                results.append({
                    "paper_id": paper_id,
                    "field_name": field_name,
                    "consensus_value": vote["consensus_value"],
                    "consensus_status": vote["consensus_status"],
                    "vote_counts": vote["vote_counts"],
                    "extractor_values": extractor_values,
                })
    finally:
        conn.close()

    return results


# ── Storage ──────────────────────────────────────────────────────────


_CREATE_TABLE = """\
CREATE TABLE IF NOT EXISTS consensus_values (
    id INTEGER PRIMARY KEY,
    paper_id TEXT NOT NULL,
    field_name TEXT NOT NULL,
    consensus_value TEXT,
    consensus_status TEXT NOT NULL,
    vote_distribution TEXT NOT NULL,
    derived_at TEXT NOT NULL,
    UNIQUE(paper_id, field_name)
)"""


def store_consensus(results: list[dict], db_path: Path) -> int:
    """Store consensus results into consensus_values table.

    Returns the number of rows inserted.
    """
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(_CREATE_TABLE)

    now = datetime.now(timezone.utc).isoformat()
    inserted = 0

    try:
        for r in results:
            conn.execute(
                "INSERT INTO consensus_values "
                "(paper_id, field_name, consensus_value, consensus_status, "
                "vote_distribution, derived_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    r["paper_id"],
                    r["field_name"],
                    str(r["consensus_value"]) if r["consensus_value"] is not None else None,
                    r["consensus_status"],
                    json.dumps(r["vote_counts"]),
                    now,
                ),
            )
            inserted += 1
        conn.commit()
    except sqlite3.IntegrityError as exc:
        conn.rollback()
        raise RuntimeError(
            f"Duplicate consensus entry detected. Clear existing rows before re-deriving. "
            f"Detail: {exc}"
        ) from exc
    finally:
        conn.close()

    return inserted


# ── Export NO_CONSENSUS for adjudication ─────────────────────────────


def export_no_consensus_for_adjudication(db_path: Path) -> list[dict]:
    """Collect all NO_CONSENSUS fields with each extractor's value.

    Returns list of dicts suitable for rendering in a multi-extractor
    adjudication interface (paper_id, field_name, extractor_values, vote_counts).
    """
    conn = sqlite3.connect(str(db_path))
    try:
        # Get NO_CONSENSUS entries
        nc_rows = conn.execute(
            """SELECT paper_id, field_name, vote_distribution
               FROM consensus_values
               WHERE consensus_status = 'NO_CONSENSUS'
               ORDER BY paper_id, field_name"""
        ).fetchall()

        results: list[dict] = []
        for paper_id, field_name, vote_dist_json in nc_rows:
            # Get each extractor's raw value
            ext_rows = conn.execute(
                """SELECT extractor_id, value
                   FROM human_extractions
                   WHERE paper_id = ? AND field_name = ?
                   ORDER BY extractor_id""",
                (paper_id, field_name),
            ).fetchall()

            extractor_values = {eid: val for eid, val in ext_rows}

            results.append({
                "paper_id": paper_id,
                "field_name": field_name,
                "extractor_values": extractor_values,
                "vote_counts": json.loads(vote_dist_json),
            })
    finally:
        conn.close()

    return results


# ── Summary ──────────────────────────────────────────────────────────


def print_summary(db_path: Path) -> None:
    """Print per-field consensus breakdown."""
    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(
            """SELECT field_name, consensus_status, COUNT(*)
               FROM consensus_values
               GROUP BY field_name, consensus_status
               ORDER BY field_name, consensus_status"""
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        print("No consensus data found.")
        return

    # Pivot: field_name -> {status: count}
    fields: dict[str, dict[str, int]] = {}
    for field_name, status, count in rows:
        fields.setdefault(field_name, {})[status] = count

    statuses = ["UNANIMOUS", "MAJORITY", "NO_CONSENSUS", "INSUFFICIENT"]
    header = f"{'Field':<35s}" + "".join(f"{s:>15s}" for s in statuses) + f"{'Total':>10s}"
    print(header)
    print("-" * len(header))

    total_by_status = {s: 0 for s in statuses}
    grand_total = 0

    for field_name in sorted(fields.keys()):
        counts = fields[field_name]
        row_total = sum(counts.values())
        line = f"{field_name:<35s}"
        for s in statuses:
            c = counts.get(s, 0)
            total_by_status[s] += c
            line += f"{c:>15d}"
        line += f"{row_total:>10d}"
        grand_total += row_total
        print(line)

    print("-" * len(header))
    totals_line = f"{'TOTAL':<35s}"
    for s in statuses:
        totals_line += f"{total_by_status[s]:>15d}"
    totals_line += f"{grand_total:>10d}"
    print(totals_line)


# ── CLI ──────────────────────────────────────────────────────────────


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="Consensus derivation for shared human extraction papers"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # derive subcommand
    derive_cmd = sub.add_parser("derive", help="Derive majority-vote consensus")
    derive_cmd.add_argument("--review", required=True, help="Review name")
    derive_cmd.add_argument("--codebook", type=Path, default=None, help="Codebook YAML path")
    derive_cmd.add_argument("--dry-run", action="store_true", help="Derive but do not store")

    # summary subcommand
    summary_cmd = sub.add_parser("summary", help="Print per-field consensus breakdown")
    summary_cmd.add_argument("--review", required=True, help="Review name")

    args = parser.parse_args()

    # Resolve DB path
    from engine.core.database import ReviewDatabase
    db = ReviewDatabase(args.review)
    db_path = Path(db.db_path)
    data_dir = db_path.parent
    db.close()

    if args.command == "derive":
        codebook_path = args.codebook or (data_dir / "extraction_codebook.yaml")
        if not codebook_path.exists():
            logger.error("Codebook not found: %s", codebook_path)
            raise SystemExit(1)

        # Identify shared papers
        shared = identify_shared_papers(db_path)
        logger.info("Found %d shared papers (>= 3 extractors)", len(shared))

        if not shared:
            logger.warning("No shared papers found — nothing to derive")
            return

        # Derive consensus
        results = derive_consensus(db_path, codebook_path)
        logger.info("Derived consensus for %d paper×field combinations", len(results))

        # Summary counts
        status_counts = Counter(r["consensus_status"] for r in results)
        for status in ["UNANIMOUS", "MAJORITY", "NO_CONSENSUS", "INSUFFICIENT"]:
            logger.info("  %s: %d", status, status_counts.get(status, 0))

        if args.dry_run:
            # Report NO_CONSENSUS fields
            nc = [r for r in results if r["consensus_status"] == "NO_CONSENSUS"]
            if nc:
                print(f"\n{len(nc)} NO_CONSENSUS fields requiring PI adjudication:")
                for r in nc:
                    print(f"  {r['paper_id']} / {r['field_name']}: {r['extractor_values']}")
            print(f"\nDry run complete — {len(results)} results, no data stored.")
            return

        # Store
        inserted = store_consensus(results, db_path)
        logger.info("Stored %d consensus rows in %s", inserted, db_path)

        # Report NO_CONSENSUS count
        nc_count = status_counts.get("NO_CONSENSUS", 0)
        if nc_count:
            logger.info(
                "%d NO_CONSENSUS fields need PI adjudication — "
                "run `export_no_consensus_for_adjudication()` to generate queue",
                nc_count,
            )

    elif args.command == "summary":
        print_summary(db_path)


if __name__ == "__main__":
    main()
