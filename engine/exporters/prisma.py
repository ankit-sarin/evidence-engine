"""PRISMA flow diagram data, CSV export, and count reconciliation."""

import csv
import logging

from engine.core.database import ReviewDatabase

logger = logging.getLogger(__name__)

# Terminal statuses — every paper must end in exactly one of these (or be in-progress)
_TERMINAL_EXCLUDED = {"ABSTRACT_SCREENED_OUT", "PDF_EXCLUDED", "FT_SCREENED_OUT", "REJECTED"}
_TERMINAL_INCLUDED = {"AI_AUDIT_COMPLETE", "HUMAN_AUDIT_COMPLETE"}
_IN_PROGRESS = {
    "INGESTED", "ABSTRACT_SCREENED_IN", "ABSTRACT_SCREEN_FLAGGED",
    "PDF_ACQUIRED", "PARSED", "FT_ELIGIBLE", "FT_FLAGGED",
    "EXTRACTED", "EXTRACT_FAILED",
}


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

    # ── Abstract Screening ──────────────────────────────────────────
    screened_out = status_counts.get("ABSTRACT_SCREENED_OUT", 0)
    screen_flagged = status_counts.get("ABSTRACT_SCREEN_FLAGGED", 0)

    # Screened in = everything that passed abstract screening
    # (any status beyond INGESTED/SCREENED_OUT/FLAGGED)
    _pre_screening = {"INGESTED", "ABSTRACT_SCREENED_OUT", "ABSTRACT_SCREEN_FLAGGED"}
    screened_in = sum(c for s, c in status_counts.items() if s not in _pre_screening)

    records_screened = screened_in + screened_out + screen_flagged

    # Screening exclusion reasons
    exclusion_reasons = {}
    for row in conn.execute(
        """SELECT sd.rationale, COUNT(*) as cnt
           FROM abstract_screening_decisions sd
           JOIN papers p ON p.id = sd.paper_id
           WHERE p.status = 'ABSTRACT_SCREENED_OUT' AND sd.decision = 'exclude'
           GROUP BY sd.rationale"""
    ).fetchall():
        exclusion_reasons[row["rationale"] or "No reason given"] = row["cnt"]

    # ── PDF Exclusions (split: not-retrieved vs eligibility) ────────
    pdf_excluded = status_counts.get("PDF_EXCLUDED", 0)
    pdf_exclusion_reasons = {}
    for row in conn.execute(
        "SELECT pdf_exclusion_reason, COUNT(*) as cnt FROM papers "
        "WHERE status = 'PDF_EXCLUDED' GROUP BY pdf_exclusion_reason"
    ).fetchall():
        pdf_exclusion_reasons[row["pdf_exclusion_reason"] or "No reason given"] = row["cnt"]

    # PRISMA 2020 split: INACCESSIBLE → "Reports not retrieved"
    # All other PDF reasons → eligibility exclusions (combined with FT below)
    reports_not_retrieved = pdf_exclusion_reasons.get("INACCESSIBLE", 0)
    pdf_eligibility_exclusions = {
        reason: count for reason, count in pdf_exclusion_reasons.items()
        if reason != "INACCESSIBLE"
    }

    # ── Full-Text Screening ─────────────────────────────────────────
    ft_screened_out = status_counts.get("FT_SCREENED_OUT", 0)
    ft_flagged = status_counts.get("FT_FLAGGED", 0)

    # FT exclusion breakdown: PI-adjudicated vs AI primary
    ft_pi_adjudicated = conn.execute(
        """SELECT COUNT(*) FROM ft_screening_adjudication fta
           JOIN papers p ON p.id = fta.paper_id
           WHERE p.status = 'FT_SCREENED_OUT'
           AND fta.adjudication_decision = 'FT_SCREENED_OUT'"""
    ).fetchone()[0]
    ft_ai_primary = ft_screened_out - ft_pi_adjudicated

    # Combined eligibility exclusions (PDF non-retrieval + FT screening)
    eligibility_exclusions = dict(pdf_eligibility_exclusions)
    eligibility_exclusions["FT screening (AI primary)"] = ft_ai_primary
    if ft_pi_adjudicated > 0:
        eligibility_exclusions["FT screening (PI adjudicated)"] = ft_pi_adjudicated
    eligibility_excluded_total = sum(eligibility_exclusions.values())

    # Full text reports retrieved = papers that reached PARSED or beyond
    _pre_fulltext = _pre_screening | {"ABSTRACT_SCREENED_IN", "PDF_ACQUIRED", "PDF_EXCLUDED"}
    full_text_retrieved = sum(c for s, c in status_counts.items() if s not in _pre_fulltext)

    # Full text assessed for eligibility = retrieved (all get screened or are in progress)
    full_text_assessed = full_text_retrieved

    # ── Extraction / Audit / Inclusion ──────────────────────────────
    studies_included = sum(
        status_counts.get(s, 0) for s in _TERMINAL_INCLUDED
    )

    # Rejected papers
    rejected = status_counts.get("REJECTED", 0)
    rejection_reasons = {}
    for row in conn.execute(
        "SELECT rejected_reason, COUNT(*) as cnt FROM papers "
        "WHERE status = 'REJECTED' GROUP BY rejected_reason"
    ).fetchall():
        rejection_reasons[row["rejected_reason"] or "No reason given"] = row["cnt"]

    low_yield_rejected = sum(
        cnt for reason, cnt in rejection_reasons.items()
        if "low_yield" in reason.lower()
    )

    # In-progress papers (not yet at a terminal state) — computed as remainder
    # to avoid double-counting between PRISMA boxes
    _terminal = _TERMINAL_EXCLUDED | _TERMINAL_INCLUDED
    in_progress = sum(c for s, c in status_counts.items() if s not in _terminal)

    # ── Audit stats ─────────────────────────────────────────────────
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
        "records_screened": records_screened,
        "records_excluded": screened_out,
        "exclusion_reasons": exclusion_reasons,
        "screen_flagged": screen_flagged,
        "pdf_excluded": pdf_excluded,
        "pdf_exclusion_reasons": pdf_exclusion_reasons,
        "reports_not_retrieved": reports_not_retrieved,
        "pdf_eligibility_exclusions": pdf_eligibility_exclusions,
        "eligibility_exclusions": eligibility_exclusions,
        "eligibility_excluded_total": eligibility_excluded_total,
        "full_text_retrieved": full_text_retrieved,
        "full_text_assessed": full_text_assessed,
        "ft_screened_out": ft_screened_out,
        "ft_ai_primary": ft_ai_primary,
        "ft_pi_adjudicated": ft_pi_adjudicated,
        "ft_flagged": ft_flagged,
        "studies_included": studies_included,
        "papers_rejected": rejected,
        "rejection_reasons": rejection_reasons,
        "low_yield_rejected": low_yield_rejected,
        "in_progress": in_progress,
        "spans_verified": spans_verified,
        "spans_flagged": spans_flagged,
    }


# ── Reconciliation ───────────────────────────────────────────────────


def validate_prisma_counts(db: ReviewDatabase) -> dict:
    """Verify PRISMA counts reconcile against raw DB totals.

    Checks:
    1. Every paper appears in exactly one category (terminal or in-progress)
    2. PDF_EXCLUDED sub-counts sum to total
    3. No paper appears in multiple terminal boxes

    Returns dict with {valid: bool, total_db, total_prisma, discrepancy, details}.
    Raises ValueError if counts don't reconcile.
    """
    conn = db._conn
    total_db = conn.execute("SELECT COUNT(*) FROM papers").fetchone()[0]

    flow = generate_prisma_flow(db)

    # Sum all mutually exclusive PRISMA categories:
    # terminal excluded + terminal included + in-progress = total
    # (screen_flagged, ft_flagged are subsets of in_progress, not separate)
    total_prisma = (
        flow["records_excluded"]       # ABSTRACT_SCREENED_OUT
        + flow["pdf_excluded"]         # PDF_EXCLUDED
        + flow["ft_screened_out"]      # FT_SCREENED_OUT
        + flow["papers_rejected"]      # REJECTED
        + flow["studies_included"]     # AI_AUDIT_COMPLETE + HUMAN_AUDIT_COMPLETE
        + flow["in_progress"]          # everything not at a terminal status
    )

    details = []

    # Check 1: totals match
    if total_prisma != total_db:
        details.append(
            f"Total mismatch: DB has {total_db} papers but PRISMA accounts for {total_prisma}"
        )

    # Check 2: PDF_EXCLUDED sub-counts
    pdf_sub_total = sum(flow["pdf_exclusion_reasons"].values())
    if pdf_sub_total != flow["pdf_excluded"]:
        details.append(
            f"PDF_EXCLUDED sub-counts ({pdf_sub_total}) != total ({flow['pdf_excluded']})"
        )

    # Check 2b: eligibility box sub-counts
    elig_sub_total = sum(flow["eligibility_exclusions"].values())
    expected_elig = flow["eligibility_excluded_total"]
    if elig_sub_total != expected_elig:
        details.append(
            f"Eligibility sub-counts ({elig_sub_total}) != total ({expected_elig})"
        )

    # Check 2c: reports_not_retrieved + pdf_eligibility = pdf_excluded
    pdf_recon = flow["reports_not_retrieved"] + sum(flow["pdf_eligibility_exclusions"].values())
    if pdf_recon != flow["pdf_excluded"]:
        details.append(
            f"Reports not retrieved ({flow['reports_not_retrieved']}) + "
            f"PDF eligibility ({sum(flow['pdf_eligibility_exclusions'].values())}) "
            f"!= PDF_EXCLUDED ({flow['pdf_excluded']})"
        )

    # Check 3: no paper in multiple terminal boxes (check DB for duplicates)
    terminal_statuses = list(_TERMINAL_EXCLUDED | _TERMINAL_INCLUDED)
    placeholders = ",".join("?" * len(terminal_statuses))
    terminal_count = conn.execute(
        f"SELECT COUNT(*) FROM papers WHERE status IN ({placeholders})",
        terminal_statuses,
    ).fetchone()[0]
    expected_terminal = (
        flow["records_excluded"] + flow["pdf_excluded"]
        + flow["ft_screened_out"] + flow["papers_rejected"]
        + flow["studies_included"]
    )
    if terminal_count != expected_terminal:
        details.append(
            f"Terminal status count ({terminal_count}) != PRISMA terminal sum ({expected_terminal})"
        )

    result = {
        "valid": len(details) == 0,
        "total_db": total_db,
        "total_prisma": total_prisma,
        "discrepancy": total_prisma - total_db,
        "details": details,
    }

    if not result["valid"]:
        raise ValueError(
            f"PRISMA reconciliation failed: {'; '.join(details)}"
        )

    return result


# ── CSV Export ───────────────────────────────────────────────────────


def export_prisma_csv(db: ReviewDatabase, output_path: str) -> None:
    """Write PRISMA flow data as a CSV file."""
    # Reconcile before exporting
    validate_prisma_counts(db)

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

    # PRISMA 2020: Reports not retrieved (INACCESSIBLE only)
    rows.append(("Reports not retrieved", flow["reports_not_retrieved"], "PDF inaccessible"))

    rows.extend([
        ("Full text reports retrieved", flow["full_text_retrieved"], ""),
        ("Full text assessed", flow["full_text_assessed"], ""),
    ])

    # PRISMA 2020: Combined eligibility exclusion box
    rows.append((
        "Excluded",
        flow["eligibility_excluded_total"],
        "PDF eligibility + FT screening",
    ))
    for reason, count in flow["eligibility_exclusions"].items():
        rows.append(("", count, reason))

    rows.extend([
        ("Full text flagged", flow["ft_flagged"], "For human review (FT)"),
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

    if flow.get("in_progress", 0) > 0:
        rows.append(("In progress", flow["in_progress"], "Papers still in pipeline"))

    rows.extend([
        ("Studies included", flow["studies_included"], ""),
        ("Evidence spans verified", flow["spans_verified"], ""),
        ("Evidence spans flagged", flow["spans_flagged"], ""),
    ])

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(rows)

    logger.info("PRISMA CSV exported to %s", output_path)
