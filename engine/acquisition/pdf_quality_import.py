"""Import PDF quality disposition JSON into the review database.

Reads the disposition JSON exported by pdf_quality_html.py and applies
each paper's disposition to the database:

  PROCEED           → pdf_quality_check_status = HUMAN_CONFIRMED
  EXCLUDE_*         → status = PDF_EXCLUDED, with reason/detail
  PDF_WILL_ATTEMPT  → no change (left as pending)
  UNSET             → no change (skipped)

All changes are atomic: if any validation error is found, zero rows
are modified.

CLI:
    python -m engine.acquisition.pdf_quality_import \\
        --review surgical_autonomy \\
        --input data/surgical_autonomy/pdf_acquisition/surgical_autonomy_pdf_quality_final.json

    python -m engine.acquisition.pdf_quality_import \\
        --review surgical_autonomy \\
        --input path/to/draft.json --dry-run
"""

import argparse
import json
import logging
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from engine.core.database import DATA_ROOT

logger = logging.getLogger(__name__)

# ── Valid enum values ────────────────────────────────────────────────

VALID_DISPOSITIONS = {
    "PROCEED",
    "PDF_WILL_ATTEMPT",
    "EXCLUDE_NON_ENGLISH",
    "EXCLUDE_NOT_MANUSCRIPT",
    "EXCLUDE_INACCESSIBLE",
    "EXCLUDE_OTHER",
    "UNSET",
}

VALID_EXCLUDE_REASONS = {
    "NON_ENGLISH",
    "NOT_MANUSCRIPT",
    "INACCESSIBLE",
    "OTHER",
}

EXCLUDE_DISPOSITIONS = {
    "EXCLUDE_NON_ENGLISH",
    "EXCLUDE_NOT_MANUSCRIPT",
    "EXCLUDE_INACCESSIBLE",
    "EXCLUDE_OTHER",
}

# Map disposition → exclusion reason stored in DB
_DISPOSITION_TO_REASON = {
    "EXCLUDE_NON_ENGLISH": "NON_ENGLISH",
    "EXCLUDE_NOT_MANUSCRIPT": "NOT_MANUSCRIPT",
    "EXCLUDE_INACCESSIBLE": "INACCESSIBLE",
    "EXCLUDE_OTHER": "OTHER",
}


# ── Validation ───────────────────────────────────────────────────────


def validate_disposition_json(data: dict, conn: sqlite3.Connection) -> list[str]:
    """Validate the disposition JSON. Returns list of error strings (empty = valid)."""
    errors = []

    # Top-level structure
    if not isinstance(data, dict):
        return ["JSON root must be an object"]
    if "papers" not in data:
        return ["Missing 'papers' key"]
    if not isinstance(data["papers"], list):
        return ["'papers' must be a list"]

    paper_ids_seen = set()

    for i, entry in enumerate(data["papers"]):
        prefix = f"papers[{i}]"

        if not isinstance(entry, dict):
            errors.append(f"{prefix}: must be an object")
            continue

        # Required fields
        pid = entry.get("paper_id")
        if pid is None:
            errors.append(f"{prefix}: missing paper_id")
            continue
        if not isinstance(pid, int):
            errors.append(f"{prefix}: paper_id must be an integer, got {type(pid).__name__}")
            continue

        # Duplicate check
        if pid in paper_ids_seen:
            errors.append(f"{prefix}: duplicate paper_id {pid}")
        paper_ids_seen.add(pid)

        # Paper exists in DB
        row = conn.execute("SELECT id FROM papers WHERE id = ?", (pid,)).fetchone()
        if not row:
            errors.append(f"{prefix}: paper_id {pid} not found in database")

        # Disposition
        disp = entry.get("disposition")
        if disp is None:
            errors.append(f"{prefix}: missing disposition")
            continue
        if disp not in VALID_DISPOSITIONS:
            errors.append(
                f"{prefix}: invalid disposition '{disp}' "
                f"(valid: {', '.join(sorted(VALID_DISPOSITIONS))})"
            )

        # Exclude reason validation
        if disp in EXCLUDE_DISPOSITIONS:
            reason = entry.get("exclude_reason")
            if reason is not None and reason not in VALID_EXCLUDE_REASONS:
                errors.append(
                    f"{prefix}: invalid exclude_reason '{reason}' "
                    f"(valid: {', '.join(sorted(VALID_EXCLUDE_REASONS))})"
                )

    return errors


# ── Import logic ─────────────────────────────────────────────────────


def import_dispositions(
    review_name: str,
    input_path: str,
    dry_run: bool = False,
    data_root: Path | None = None,
) -> dict:
    """Import disposition JSON into the review database.

    Returns a stats dict with counts by disposition type.
    Raises ValueError on validation failure.
    """
    root = data_root or DATA_ROOT
    db_path = root / review_name / "review.db"
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    input_file = Path(input_path)
    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")

    with open(input_file) as f:
        data = json.load(f)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Validate first — atomic: reject everything on any error
    errors = validate_disposition_json(data, conn)
    if errors:
        conn.close()
        raise ValueError(
            f"Validation failed with {len(errors)} error(s):\n"
            + "\n".join(f"  - {e}" for e in errors)
        )

    is_complete = data.get("complete", False)
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    stats = {
        "proceeded": 0,
        "excluded": 0,
        "exclude_breakdown": {},
        "will_attempt": 0,
        "skipped_unset": 0,
        "skipped_already": 0,
        "is_complete": is_complete,
    }

    # Get already-finalized sets
    already_excluded = set(
        r[0] for r in conn.execute(
            "SELECT id FROM papers WHERE status = 'PDF_EXCLUDED'"
        ).fetchall()
    )
    already_confirmed = set(
        r[0] for r in conn.execute(
            "SELECT id FROM papers WHERE pdf_quality_check_status = 'HUMAN_CONFIRMED'"
        ).fetchall()
    )

    for entry in data["papers"]:
        pid = entry["paper_id"]
        disp = entry["disposition"]
        detail = entry.get("exclude_detail")

        if disp == "UNSET":
            stats["skipped_unset"] += 1
            continue

        if disp == "PDF_WILL_ATTEMPT":
            stats["will_attempt"] += 1
            logger.info("  PDF_WILL_ATTEMPT pid=%d", pid)
            continue

        if pid in already_excluded or pid in already_confirmed:
            stats["skipped_already"] += 1
            continue

        if disp == "PROCEED":
            if not dry_run:
                conn.execute(
                    "UPDATE papers SET pdf_quality_check_status = 'HUMAN_CONFIRMED' WHERE id = ?",
                    (pid,),
                )
            stats["proceeded"] += 1
            logger.info("  PROCEED pid=%d → HUMAN_CONFIRMED", pid)

        elif disp in EXCLUDE_DISPOSITIONS:
            reason = _DISPOSITION_TO_REASON[disp]
            # Use detail from JSON, or from exclude_reason field as fallback
            db_detail = detail or entry.get("exclude_detail")
            if not dry_run:
                conn.execute(
                    """UPDATE papers
                       SET status = 'PDF_EXCLUDED',
                           pdf_exclusion_reason = ?,
                           pdf_exclusion_detail = ?,
                           updated_at = ?
                       WHERE id = ?""",
                    (reason, db_detail, now, pid),
                )
            stats["excluded"] += 1
            stats["exclude_breakdown"][reason] = (
                stats["exclude_breakdown"].get(reason, 0) + 1
            )
            logger.info("  EXCLUDE pid=%d reason=%s", pid, reason)

    if not dry_run:
        conn.commit()

    if is_complete:
        logger.info("PDF acquisition finalized (complete=true)")

    conn.close()
    return stats


# ── CLI ──────────────────────────────────────────────────────────────


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s:%(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Import PDF quality disposition JSON into the review database"
    )
    parser.add_argument("--review", required=True, help="Review name")
    parser.add_argument("--input", required=True, help="Path to disposition JSON")
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print what would change without modifying the database",
    )
    args = parser.parse_args()

    try:
        stats = import_dispositions(
            review_name=args.review,
            input_path=args.input,
            dry_run=args.dry_run,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)

    # Report
    print(f"\n{'=' * 50}")
    print("PDF QUALITY IMPORT REPORT")
    if args.dry_run:
        print("(DRY RUN — no changes written)")
    print(f"{'=' * 50}")
    print(f"  Proceeded (HUMAN_CONFIRMED): {stats['proceeded']}")
    print(f"  Excluded (PDF_EXCLUDED):     {stats['excluded']}")
    for reason, count in sorted(stats["exclude_breakdown"].items()):
        print(f"    {reason:<20} {count}")
    print(f"  Will attempt (pending):      {stats['will_attempt']}")
    print(f"  Skipped (UNSET):             {stats['skipped_unset']}")
    print(f"  Skipped (already finalized): {stats['skipped_already']}")
    if stats["is_complete"]:
        print(f"\n  PDF acquisition finalized.")


if __name__ == "__main__":
    main()
