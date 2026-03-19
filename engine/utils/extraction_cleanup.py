"""Extraction cleanup utility for schema version transitions.

Removes stale extractions and associated evidence spans, then resets
affected papers to PARSED so they're eligible for re-extraction.

Destructive operation — dry-run is the default. Requires --confirm to execute.
"""

import argparse
import logging
import sys
from pathlib import Path

from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec
from engine.utils.db_backup import auto_backup

logger = logging.getLogger(__name__)

# Papers at these statuses will be reset to PARSED after cleanup.
# HUMAN_AUDIT_COMPLETE is excluded — those have human-verified data.
_RESETTABLE_STATUSES = {"EXTRACTED", "AI_AUDIT_COMPLETE"}

# Convention: review_specs/{review_name}*.yaml
_SPECS_DIR = Path(__file__).resolve().parent.parent.parent / "review_specs"


def find_review_spec(review_name: str) -> Path | None:
    """Auto-discover the review spec YAML for a given review name.

    Searches review_specs/ for files matching {review_name}*.yaml,
    returning the first match (lexicographic). Returns None if not found.
    """
    candidates = sorted(_SPECS_DIR.glob(f"{review_name}*.yaml"))
    return candidates[0] if candidates else None


def get_current_schema_hash(review_name: str, spec_path: str | Path | None = None) -> str:
    """Compute the current extraction schema hash for a review.

    If spec_path is provided, loads that file. Otherwise, auto-discovers
    the spec from review_specs/{review_name}*.yaml.
    """
    if spec_path is None:
        found = find_review_spec(review_name)
        if found is None:
            raise FileNotFoundError(
                f"No review spec found for '{review_name}' in {_SPECS_DIR}"
            )
        spec_path = found

    spec = load_review_spec(spec_path)
    return spec.extraction_hash()


def check_stale_extractions(db: ReviewDatabase, current_hash: str) -> int:
    """Count papers with extractions from a different schema version.

    Returns the count of stale extractions. Used as a pre-flight check
    by the extraction runner.
    """
    row = db._conn.execute(
        """SELECT COUNT(DISTINCT paper_id) FROM extractions
           WHERE extraction_schema_hash != ?""",
        (current_hash,),
    ).fetchone()
    return row[0]


def cleanup_stale_extractions(
    db: ReviewDatabase,
    schema_hash: str | None = None,
    dry_run: bool = True,
) -> dict:
    """Remove stale extractions and their evidence spans.

    If schema_hash is provided: remove extractions where
    extraction_schema_hash != schema_hash (keeps only current schema).

    If no schema_hash: for papers with multiple extraction rows, keep
    only the most recent (highest id) and delete the rest.

    Returns summary: {papers_affected, extractions_deleted, spans_deleted,
                      papers_reset, details: [...]}.
    """
    conn = db._conn

    if schema_hash:
        # Find all extractions NOT matching the target hash
        stale = conn.execute(
            """SELECT e.id AS ext_id, e.paper_id, e.extraction_schema_hash,
                      (SELECT COUNT(*) FROM evidence_spans WHERE extraction_id = e.id) AS span_count
               FROM extractions e
               WHERE e.extraction_schema_hash != ?
               ORDER BY e.paper_id""",
            (schema_hash,),
        ).fetchall()
    else:
        # Dedup: for papers with multiple extractions, mark all but the latest
        stale = conn.execute(
            """SELECT e.id AS ext_id, e.paper_id, e.extraction_schema_hash,
                      (SELECT COUNT(*) FROM evidence_spans WHERE extraction_id = e.id) AS span_count
               FROM extractions e
               WHERE e.id NOT IN (
                   SELECT MAX(id) FROM extractions GROUP BY paper_id
               )
               ORDER BY e.paper_id""",
        ).fetchall()

    details = []
    paper_ids_affected = set()

    for row in stale:
        paper_ids_affected.add(row["paper_id"])
        details.append({
            "paper_id": row["paper_id"],
            "extraction_id": row["ext_id"],
            "schema_hash": row["extraction_schema_hash"],
            "span_count": row["span_count"],
        })
        logger.info(
            "Paper %d: extraction %d (hash=%s, %d spans) — %s",
            row["paper_id"], row["ext_id"],
            row["extraction_schema_hash"][:12],
            row["span_count"],
            "would delete" if dry_run else "deleting",
        )

    total_extractions = len(stale)
    total_spans = sum(d["span_count"] for d in details)

    # Determine which papers to reset (only EXTRACTED / AI_AUDIT_COMPLETE)
    papers_to_reset = []
    if paper_ids_affected:
        placeholders = ",".join("?" * len(paper_ids_affected))
        reset_rows = conn.execute(
            f"SELECT id, status FROM papers WHERE id IN ({placeholders})",
            list(paper_ids_affected),
        ).fetchall()
        papers_to_reset = [
            r["id"] for r in reset_rows if r["status"] in _RESETTABLE_STATUSES
        ]

        # Log papers that won't be reset
        for r in reset_rows:
            if r["status"] not in _RESETTABLE_STATUSES:
                logger.info(
                    "Paper %d at %s — will NOT reset status (protected)",
                    r["id"], r["status"],
                )

    summary = {
        "papers_affected": len(paper_ids_affected),
        "extractions_deleted": total_extractions,
        "spans_deleted": total_spans,
        "papers_reset": len(papers_to_reset),
        "details": details,
        "dry_run": dry_run,
    }

    if dry_run:
        logger.info(
            "DRY RUN — would delete %d extractions (%d spans) across %d papers, "
            "reset %d papers to PARSED",
            total_extractions, total_spans,
            len(paper_ids_affected), len(papers_to_reset),
        )
        return summary

    # Back up before destructive operations
    auto_backup(db.db_path, "pre-cleanup")

    # Execute deletions + status resets in a single atomic transaction
    try:
        conn.execute("BEGIN")

        ext_ids = [d["extraction_id"] for d in details]
        if ext_ids:
            placeholders = ",".join("?" * len(ext_ids))

            spans_deleted = conn.execute(
                f"DELETE FROM evidence_spans WHERE extraction_id IN ({placeholders})",
                ext_ids,
            ).rowcount

            extractions_deleted = conn.execute(
                f"DELETE FROM extractions WHERE id IN ({placeholders})",
                ext_ids,
            ).rowcount

            summary["spans_deleted"] = spans_deleted
            summary["extractions_deleted"] = extractions_deleted

        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    # Reset affected papers via admin_reset_status (audited, outside the
    # delete transaction — papers are only reset after deletions succeed)
    for pid in papers_to_reset:
        db.admin_reset_status(
            pid, "PARSED", reason="extraction_cleanup: stale schema removal",
        )

    logger.info(
        "CLEANUP COMPLETE — deleted %d extractions (%d spans) across %d papers, "
        "reset %d papers to PARSED",
        summary["extractions_deleted"], summary["spans_deleted"],
        summary["papers_affected"], summary["papers_reset"],
    )
    return summary


# ── CLI ──────────────────────────────────────────────────────────────


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(
        description="Remove stale extractions from a previous schema version (destructive)"
    )
    parser.add_argument("--review", required=True, help="Review name")
    parser.add_argument(
        "--keep-schema", metavar="HASH",
        help="Keep only extractions matching this schema hash (delete all others)",
    )
    parser.add_argument(
        "--spec", help="Path to review spec YAML (overrides auto-discovery)",
    )
    parser.add_argument(
        "--confirm", action="store_true",
        help="Actually execute deletions (default is dry-run)",
    )
    args = parser.parse_args()

    # Resolve schema hash: explicit > --spec > auto-discover
    schema_hash = args.keep_schema
    if not schema_hash:
        try:
            schema_hash = get_current_schema_hash(args.review, spec_path=args.spec)
            logger.info("Current extraction schema hash: %s", schema_hash[:12])
        except FileNotFoundError as e:
            logger.error(str(e))
            logger.error("Provide --spec or --keep-schema explicitly.")
            sys.exit(1)

    db = ReviewDatabase(args.review)
    try:
        dry_run = not args.confirm
        if dry_run:
            print("\n*** DRY RUN — no changes will be made. Add --confirm to execute. ***\n")

        summary = cleanup_stale_extractions(db, schema_hash=schema_hash, dry_run=dry_run)

        print(f"\n{'PLAN' if dry_run else 'RESULT'}:")
        print(f"  Papers affected:       {summary['papers_affected']}")
        print(f"  Extractions to delete: {summary['extractions_deleted']}")
        print(f"  Spans to delete:       {summary['spans_deleted']}")
        print(f"  Papers to reset:       {summary['papers_reset']}")

        if dry_run and summary["extractions_deleted"] > 0:
            print(f"\nRe-run with --confirm to execute.")
    finally:
        db.close()


if __name__ == "__main__":
    main()
