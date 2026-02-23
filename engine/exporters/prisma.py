"""PRISMA flow diagram data and CSV export."""

import csv
import logging

from engine.core.database import ReviewDatabase

logger = logging.getLogger(__name__)


def generate_prisma_flow(db: ReviewDatabase) -> dict:
    """Generate PRISMA flow counts from the database."""
    conn = db._conn

    # Records identified by source
    source_counts = {}
    for row in conn.execute(
        "SELECT source, COUNT(*) as cnt FROM papers GROUP BY source"
    ).fetchall():
        source_counts[row["source"]] = row["cnt"]

    total_identified = sum(source_counts.values())

    # Status counts
    status_counts = {}
    for row in conn.execute(
        "SELECT status, COUNT(*) as cnt FROM papers GROUP BY status"
    ).fetchall():
        status_counts[row["status"]] = row["cnt"]

    # Screening outcomes
    screened_out = status_counts.get("SCREENED_OUT", 0)
    screen_flagged = status_counts.get("SCREEN_FLAGGED", 0)

    # Records that passed screening (anything beyond SCREENED_IN)
    post_screening_statuses = {
        "SCREENED_IN", "PDF_ACQUIRED", "PARSED", "EXTRACTED", "AUDITED"
    }
    screened_in = sum(status_counts.get(s, 0) for s in post_screening_statuses)

    # Screening exclusion reasons from screening_decisions
    exclusion_reasons = {}
    for row in conn.execute(
        """SELECT sd.rationale, COUNT(*) as cnt
           FROM screening_decisions sd
           JOIN papers p ON p.id = sd.paper_id
           WHERE p.status = 'SCREENED_OUT' AND sd.decision = 'exclude'
           GROUP BY sd.rationale"""
    ).fetchall():
        exclusion_reasons[row["rationale"] or "No reason given"] = row["cnt"]

    # Full text stages
    full_text_assessed = sum(
        status_counts.get(s, 0)
        for s in ("PDF_ACQUIRED", "PARSED", "EXTRACTED", "AUDITED")
    )

    studies_included = sum(
        status_counts.get(s, 0) for s in ("EXTRACTED", "AUDITED")
    )

    # Audit stats
    spans_verified = conn.execute(
        "SELECT COUNT(*) FROM evidence_spans WHERE audit_status = 'verified'"
    ).fetchone()[0]
    spans_flagged = conn.execute(
        "SELECT COUNT(*) FROM evidence_spans WHERE audit_status = 'flagged'"
    ).fetchone()[0]

    return {
        "records_identified": total_identified,
        "records_by_source": source_counts,
        "duplicates_removed": 0,  # tracked externally by dedup module
        "records_screened": screened_in + screened_out + screen_flagged,
        "records_excluded": screened_out,
        "exclusion_reasons": exclusion_reasons,
        "screen_flagged": screen_flagged,
        "full_text_assessed": full_text_assessed,
        "studies_included": studies_included,
        "spans_verified": spans_verified,
        "spans_flagged": spans_flagged,
    }


def export_prisma_csv(db: ReviewDatabase, output_path: str) -> None:
    """Write PRISMA flow data as a CSV file."""
    flow = generate_prisma_flow(db)

    rows = [
        ("Stage", "Count", "Detail"),
        ("Records identified", flow["records_identified"], ""),
    ]
    for source, count in flow["records_by_source"].items():
        rows.append(("", count, f"From {source}"))

    rows.extend([
        ("Duplicates removed", flow["duplicates_removed"], ""),
        ("Records screened", flow["records_screened"], ""),
        ("Records excluded", flow["records_excluded"], ""),
    ])
    for reason, count in flow["exclusion_reasons"].items():
        rows.append(("", count, reason[:80]))

    rows.extend([
        ("Screen flagged", flow["screen_flagged"], "For human review"),
        ("Full text assessed", flow["full_text_assessed"], ""),
        ("Studies included", flow["studies_included"], ""),
        ("Evidence spans verified", flow["spans_verified"], ""),
        ("Evidence spans flagged", flow["spans_flagged"], ""),
    ])

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(rows)

    logger.info("PRISMA CSV exported to %s", output_path)
