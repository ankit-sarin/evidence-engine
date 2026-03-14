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
    screened_out = status_counts.get("ABSTRACT_SCREENED_OUT", 0)
    screen_flagged = status_counts.get("ABSTRACT_SCREEN_FLAGGED", 0)

    # Records that passed screening (anything beyond ABSTRACT_SCREENED_IN)
    post_screening_statuses = {
        "ABSTRACT_SCREENED_IN", "PDF_ACQUIRED", "PARSED", "EXTRACTED",
        "AI_AUDIT_COMPLETE", "HUMAN_AUDIT_COMPLETE",
    }
    screened_in = sum(status_counts.get(s, 0) for s in post_screening_statuses)

    # Screening exclusion reasons from abstract_screening_decisions
    exclusion_reasons = {}
    for row in conn.execute(
        """SELECT sd.rationale, COUNT(*) as cnt
           FROM abstract_screening_decisions sd
           JOIN papers p ON p.id = sd.paper_id
           WHERE p.status = 'ABSTRACT_SCREENED_OUT' AND sd.decision = 'exclude'
           GROUP BY sd.rationale"""
    ).fetchall():
        exclusion_reasons[row["rationale"] or "No reason given"] = row["cnt"]

    # PDF exclusions (between abstract screening and full-text screening)
    pdf_excluded = status_counts.get("PDF_EXCLUDED", 0)
    pdf_exclusion_reasons = {}
    for row in conn.execute(
        "SELECT pdf_exclusion_reason, COUNT(*) as cnt FROM papers "
        "WHERE status = 'PDF_EXCLUDED' GROUP BY pdf_exclusion_reason"
    ).fetchall():
        pdf_exclusion_reasons[row["pdf_exclusion_reason"] or "No reason given"] = row["cnt"]

    # Full text stages
    full_text_assessed = sum(
        status_counts.get(s, 0)
        for s in ("PDF_ACQUIRED", "PARSED", "EXTRACTED", "AI_AUDIT_COMPLETE", "HUMAN_AUDIT_COMPLETE")
    )

    studies_included = sum(
        status_counts.get(s, 0) for s in ("AI_AUDIT_COMPLETE", "HUMAN_AUDIT_COMPLETE")
    )

    # Full-text screening exclusions
    ft_screened_out = status_counts.get("FT_SCREENED_OUT", 0)
    ft_flagged = status_counts.get("FT_FLAGGED", 0)

    # Rejected papers
    rejected = status_counts.get("REJECTED", 0)
    rejection_reasons = {}
    for row in conn.execute(
        "SELECT rejected_reason, COUNT(*) as cnt FROM papers "
        "WHERE status = 'REJECTED' GROUP BY rejected_reason"
    ).fetchall():
        rejection_reasons[row["rejected_reason"] or "No reason given"] = row["cnt"]

    # Low-yield rejections (subset of rejected — reason starts with "low_yield")
    low_yield_rejected = 0
    for reason, cnt in rejection_reasons.items():
        if "low_yield" in reason.lower():
            low_yield_rejected += cnt

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
        "pdf_excluded": pdf_excluded,
        "pdf_exclusion_reasons": pdf_exclusion_reasons,
        "ft_screened_out": ft_screened_out,
        "ft_flagged": ft_flagged,
        "full_text_assessed": full_text_assessed,
        "studies_included": studies_included,
        "papers_rejected": rejected,
        "rejection_reasons": rejection_reasons,
        "low_yield_rejected": low_yield_rejected,
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

    rows.append(("Screen flagged", flow["screen_flagged"], "For human review"))

    # PDF exclusions — between abstract screening and full-text screening
    if flow.get("pdf_excluded", 0) > 0:
        rows.append(("PDFs excluded", flow["pdf_excluded"], "Excluded at PDF quality check"))
        for reason, count in flow.get("pdf_exclusion_reasons", {}).items():
            rows.append(("", count, reason))

    rows.extend([
        ("Full text screened out", flow.get("ft_screened_out", 0), "Excluded at full-text screening"),
        ("Full text flagged", flow.get("ft_flagged", 0), "For human review (FT)"),
        ("Full text assessed", flow["full_text_assessed"], ""),
        ("Papers rejected", flow["papers_rejected"], "Post-extraction exclusion"),
    ])
    for reason, count in flow.get("rejection_reasons", {}).items():
        rows.append(("", count, reason[:80]))
    if flow.get("low_yield_rejected", 0) > 0:
        rows.append((
            "  — Excluded after extraction (insufficient data)",
            flow["low_yield_rejected"],
            "LOW_YIELD: too few populated fields",
        ))
    rows.extend([
        ("Studies included", flow["studies_included"], ""),
        ("Evidence spans verified", flow["spans_verified"], ""),
        ("Evidence spans flagged", flow["spans_flagged"], ""),
    ])

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(rows)

    logger.info("PRISMA CSV exported to %s", output_path)
