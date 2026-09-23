"""Extraction staleness report for codebook transitions — read-only.

It used to remove stale extractions and their evidence spans, then reset the
affected papers to PARSED. **That delete branch is retired (R25, R94; row
D10).** Extractions are superseded by event, never deleted: on the live
database the NULL-inclusive staleness predicate matched every one of the 190
extractions, so `--confirm` would have deleted the whole local arm and put
every corpus paper back in the extractor's pickup set. `--confirm` now refuses
unconditionally, before any database is opened; the dry-run report remains.
"""

import argparse
import logging
import sys
from pathlib import Path

from engine.core.database import ReviewDatabase
from engine.core.codebook import CodebookError, load_codebook_for

logger = logging.getLogger(__name__)

# Papers at these statuses would have been reset to PARSED by the retired
# delete branch; the dry-run report still counts them.
# HUMAN_AUDIT_COMPLETE is excluded — those have human-verified data.
_RESETTABLE_STATUSES = {"EXTRACTED", "AI_AUDIT_COMPLETE"}

# The spec path is derived, never searched. The glob this replaced
# ("{review_name}*.yaml", first lexicographic match) was a second path
# authority and an ambiguous one: after the SPEC-AUTH-01 rename it matched
# both surgical_autonomy.yaml and surgical_autonomy_v1_original.yaml, and
# would have picked whichever sorted first.


REFUSAL = (
    "extraction_cleanup's delete branch is retired (R25, R94; row D10): "
    "extractions are superseded by event, never deleted. The staleness report "
    "(no --confirm) is read-only and remains. Re-extraction under a changed "
    "input is decided at selection by the reuse key from session 9."
)


class DeletionRetired(RuntimeError):
    """The delete branch was asked for. It no longer exists (R94)."""


def get_current_schema_hash(
    review_name: str, codebook_path: str | Path | None = None
) -> str:
    """The current CODEBOOK hash for a review — what staleness compares against.

    It used to return `spec.extraction_hash()`, a hash of a spec section that
    no longer exists. The prompt is built from the codebook, so the codebook's
    semantic hash is what an extraction is stale against (SCHEMA-DERIVE-01).

    An override path is refused if it names a different review.
    """
    return load_codebook_for(review_name, codebook_path).semantic_hash


def check_stale_extractions(db: ReviewDatabase, current_hash: str) -> int:
    """Count papers with extractions from a different schema version.

    Returns the count of stale extractions. Used as a pre-flight check
    by the extraction runner.
    """
    row = db._conn.execute(
        """SELECT COUNT(DISTINCT paper_id) FROM extractions
           WHERE (codebook_hash IS NULL OR codebook_hash != ?)""",
        (current_hash,),
    ).fetchone()
    return row[0]


def cleanup_stale_extractions(
    db: ReviewDatabase,
    schema_hash: str | None = None,
    dry_run: bool = True,
) -> dict:
    """Report what the retired delete branch WOULD have removed. Read-only.

    If schema_hash is provided: extractions whose codebook_hash is NULL or
    differs from it. If no schema_hash: every extraction but the most recent
    (highest id) per paper.

    `dry_run=False` asks for the deletion, which is retired (R25, R94): it
    raises `DeletionRetired` before any query runs. The keys keep their old
    names (`extractions_deleted`, ...) and mean "would delete".

    Returns summary: {papers_affected, extractions_deleted, spans_deleted,
                      papers_reset, details: [...]}.
    """
    if not dry_run:
        raise DeletionRetired(REFUSAL)

    conn = db._conn

    if schema_hash:
        # Find all extractions NOT matching the target hash
        stale = conn.execute(
            """SELECT e.id AS ext_id, e.paper_id, e.codebook_hash,
                      (SELECT COUNT(*) FROM evidence_spans WHERE extraction_id = e.id) AS span_count
               FROM extractions e
               WHERE (e.codebook_hash IS NULL OR e.codebook_hash != ?)
               ORDER BY e.paper_id""",
            (schema_hash,),
        ).fetchall()
    else:
        # Dedup: for papers with multiple extractions, mark all but the latest
        stale = conn.execute(
            """SELECT e.id AS ext_id, e.paper_id, e.codebook_hash,
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
            "codebook_hash": row["codebook_hash"],
            "span_count": row["span_count"],
        })
        logger.info(
            "Paper %d: extraction %d (hash=%s, %d spans) — %s",
            row["paper_id"], row["ext_id"],
            (row["codebook_hash"] or "none-recorded")[:12],
            row["span_count"],
            "would delete",
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

    logger.info(
        "DRY RUN — would delete %d extractions (%d spans) across %d papers, "
        "reset %d papers to PARSED",
        total_extractions, total_spans,
        len(paper_ids_affected), len(papers_to_reset),
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
        description="Report extractions stale against the current codebook (read-only; the delete branch is retired, R94)"
    )
    parser.add_argument("--review", required=True, help="Review name")
    parser.add_argument(
        "--keep-schema", metavar="HASH",
        help="Report extractions not matching this codebook hash",
    )
    parser.add_argument(
        "--codebook", default=None,
        help=("Override the codebook path. Defaults to "
              "data/<review>/extraction_codebook.yaml; an override must "
              "declare the same review."),
    )
    parser.add_argument(
        "--confirm", action="store_true",
        help="RETIRED (R94): refuses. Extractions are superseded by event, never deleted",
    )
    args = parser.parse_args()

    # R94: refuse before anything is opened. Constructing ReviewDatabase runs
    # the migration runner, so a refusal that came after it would not be a
    # refusal that touched nothing.
    if args.confirm:
        print(f"REFUSED: {REFUSAL}", file=sys.stderr)
        sys.exit(2)

    # Resolve the current hash: explicit > --codebook > derived from the review
    # id. It is the CODEBOOK's hash now: the spec section it used to come from
    # no longer exists (SCHEMA-DERIVE-01), so this entry point takes no --spec.
    schema_hash = args.keep_schema
    if not schema_hash:
        try:
            schema_hash = get_current_schema_hash(args.review, args.codebook)
            logger.info("Current codebook hash: %s", schema_hash[:12])
        except CodebookError as e:
            logger.error(str(e))
            logger.error("Provide --codebook or --keep-schema explicitly.")
            sys.exit(1)

    db = ReviewDatabase(args.review)
    try:
        print("\n*** STALENESS REPORT — read-only. Nothing is deleted (R94). ***\n")

        summary = cleanup_stale_extractions(db, schema_hash=schema_hash, dry_run=True)

        print("\nREPORT:")
        print(f"  Papers affected:            {summary['papers_affected']}")
        print(f"  Extractions stale by hash:  {summary['extractions_deleted']}")
        print(f"  Their spans:                {summary['spans_deleted']}")
        print(f"  Papers at a resettable status: {summary['papers_reset']}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
