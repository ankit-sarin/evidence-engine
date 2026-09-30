"""PRISMA flow diagram data, CSV export, and count reconciliation.

The freshman count model (R183–R196): two stores, one seam.

* **Screening side** — `papers.status`, read ONLY for the nine tokens in
  `SCREENING_TOKENS` and `VERIFICATION_PENDING_TOKENS` for the seam's
  in-progress clause (R-S1), plus the screening tables. The screeners have not been cut
  over to the event store (R183); until they are (junior), these tokens are the
  screening record.
* **Extraction side** — the event store ONLY, through `engine.core.effective`:
  the eligibility axis says which papers reached extraction, the processing axis
  says how far each one got.
* **The seam** — the papers whose status is outside `SCREENING_TOKENS` must be
  exactly the papers `eligible` on the eligibility axis. `validate_prisma_counts`
  checks it as SETS.

Every count is an explicit sum of named screening tokens, a screening-table read,
an event-axis read, or a sum of named PRISMA counts. No count is a complement
over the status column (R184 e; pinned by source inspection in
`tests/test_prisma_reconciliation.py`).
"""

import csv
import logging
import os

from engine.core.database import (
    SCREENING_TOKENS, VERIFICATION_PENDING_TOKENS, ReviewDatabase,
)
from engine.core.effective import effective_state, eligible_paper_ids
from engine.core.paper_state import FAILURE_STATES, NO_RECORDED_STATE

logger = logging.getLogger(__name__)

#: Processing-axis tokens counted as "still in extraction" (R184 d): no
#: processing record yet, parsed, or extracted but not yet audited.
_EXTRACTION_IN_PROGRESS = (NO_RECORDED_STATE, "parsed", "extracted")

#: CSV label per processing failure token (R184 d), one line per non-zero reason.
_FAILURE_LABELS = {
    "extraction_failed": "Extraction failed",
    "input_exceeds_context": "Input exceeds context",
    "parse_failed": "Parse failed",
    "full_text_not_obtainable": "Full text not obtainable",
}


def _status_counts(conn) -> dict:
    return {row[0]: row[1] for row in conn.execute(
        "SELECT status, COUNT(*) FROM papers GROUP BY status").fetchall()}


def _pending_ids(conn) -> set[int]:
    """Papers at a `VERIFICATION_PENDING_TOKENS` status (R-S1's P set)."""
    placeholders = ", ".join("?" * len(VERIFICATION_PENDING_TOKENS))
    return {r[0] for r in conn.execute(
        f"SELECT id FROM papers WHERE status IN ({placeholders})",
        tuple(sorted(VERIFICATION_PENDING_TOKENS)))}


def generate_prisma_flow(db: ReviewDatabase) -> dict:
    """Generate PRISMA flow counts from the database."""
    conn = db._conn

    # ── Identity ────────────────────────────────────────────────────
    source_counts = {}
    for row in conn.execute(
        "SELECT source, COUNT(*) as cnt FROM papers GROUP BY source"
    ).fetchall():
        source_counts[row["source"]] = row["cnt"]
    total_identified = sum(source_counts.values())
    duplicates_removed = 0  # tracked externally by dedup module

    status_counts = _status_counts(conn)

    def n(token: str) -> int:
        assert token in SCREENING_TOKENS, token
        return status_counts.get(token, 0)

    # ── Abstract Screening ──────────────────────────────────────────
    screened_out = n("ABSTRACT_SCREENED_OUT")
    screen_flagged = n("ABSTRACT_SCREEN_FLAGGED")
    records_screened = total_identified - duplicates_removed - n("INGESTED")

    exclusion_reasons = {}
    for row in conn.execute(
        """SELECT sd.rationale, COUNT(*) as cnt
           FROM abstract_screening_decisions sd
           JOIN papers p ON p.id = sd.paper_id
           WHERE p.status = 'ABSTRACT_SCREENED_OUT' AND sd.decision = 'exclude'
           GROUP BY sd.rationale"""
    ).fetchall():
        exclusion_reasons[row["rationale"] or "No reason given"] = row["cnt"]

    # Every paper past abstract screening (PRISMA 2020 "reports sought").
    reports_sought = records_screened - screened_out - screen_flagged

    # ── PDF Exclusions (split: not-retrieved vs eligibility) ────────
    pdf_excluded = n("PDF_EXCLUDED")
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
    ft_screened_out = n("FT_SCREENED_OUT")
    ft_flagged = n("FT_FLAGGED")

    # FT exclusion breakdown: PI-adjudicated vs AI primary
    ft_pi_adjudicated = conn.execute(
        """SELECT COUNT(*) FROM ft_screening_adjudication fta
           JOIN papers p ON p.id = fta.paper_id
           WHERE p.status = 'FT_SCREENED_OUT'
           AND fta.adjudication_decision = 'FT_SCREENED_OUT'"""
    ).fetchone()[0]
    ft_ai_primary = ft_screened_out - ft_pi_adjudicated

    # Combined eligibility exclusions (PDF eligibility + FT screening)
    eligibility_exclusions = dict(pdf_eligibility_exclusions)
    eligibility_exclusions["FT screening (AI primary)"] = ft_ai_primary
    if ft_pi_adjudicated > 0:
        eligibility_exclusions["FT screening (PI adjudicated)"] = ft_pi_adjudicated
    eligibility_excluded_total = sum(eligibility_exclusions.values())

    screening_in_progress = (
        n("ABSTRACT_SCREENED_IN") + n("ABSTRACT_SCREEN_FLAGGED")
        + n("PDF_ACQUIRED") + n("PARSED") + n("FT_FLAGGED")
    )

    # ── The seam: papers reaching extraction, from the eligibility axis ──
    eligible_ids = eligible_paper_ids(conn)
    n_eligible = len(eligible_ids)

    # R-S1 (c): the FT primary included it, the verifier has not confirmed it —
    # verification pending, so screening is still in progress for it.
    verification_pending = len(_pending_ids(conn) - set(eligible_ids))
    screening_in_progress += verification_pending

    # ABSTRACT_SCREENED_IN = acquisition pending, so not yet retrieved.
    full_text_retrieved = (
        reports_sought - reports_not_retrieved - n("ABSTRACT_SCREENED_IN")
    )
    full_text_assessed = (
        sum(pdf_eligibility_exclusions.values()) + ft_flagged + ft_screened_out
        + n_eligible
    )

    # ── Extraction side: the processing axis, over the eligible papers ──
    studies_included = 0
    extraction_in_progress = 0
    failures = {token: 0 for token in FAILURE_STATES}
    failure_reasons = {token: {} for token in FAILURE_STATES}
    for pid in eligible_ids:
        state = effective_state(conn, pid)
        if state.processing == "audited_ai":
            studies_included += 1
        elif state.processing in failures:
            failures[state.processing] += 1
            reasons = failure_reasons[state.processing]
            reasons[state.processing_reason] = reasons.get(state.processing_reason, 0) + 1
        elif state.processing in _EXTRACTION_IN_PROGRESS:
            extraction_in_progress += 1
        else:  # pragma: no cover - PROCESSING_STATES is closed and covered above
            raise ValueError(
                f"paper {pid}: processing token {state.processing!r} has no PRISMA box")

    return {
        "records_identified": total_identified,
        "records_by_source": source_counts,
        "duplicates_removed": duplicates_removed,
        "records_screened": records_screened,
        "records_excluded": screened_out,
        "exclusion_reasons": exclusion_reasons,
        "screen_flagged": screen_flagged,
        "reports_sought": reports_sought,
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
        "screening_in_progress": screening_in_progress,
        "verification_pending": verification_pending,
        "n_eligible": n_eligible,
        "studies_included": studies_included,
        "extraction_failed": failures["extraction_failed"],
        "input_exceeds_context": failures["input_exceeds_context"],
        "parse_failed": failures["parse_failed"],
        "full_text_not_obtainable": failures["full_text_not_obtainable"],
        "failure_reasons": failure_reasons,
        "extraction_in_progress": extraction_in_progress,
    }


# ── Reconciliation ───────────────────────────────────────────────────


def validate_prisma_counts(db: ReviewDatabase, flow: dict | None = None) -> dict:
    r"""Verify PRISMA counts reconcile (R186 as amended by 9e-R1a, identity 1
    refined by R-S1, session 11).

    1. Screening partition + seam. Every paper is counted once under a
       screening token or is in the remainder (status outside
       `SCREENING_TOKENS`). With E = papers with a live `eligible` event,
       S = papers at a screening token and P = papers at a
       `VERIFICATION_PENDING_TOKENS` status (FT_ELIGIBLE):
         (a) E ∩ S is empty — "eligible … but at a screening token";
         (b) every paper outside S and outside P is in E — "past screening …
             but not eligible";
         (c) a paper in P \ E is verification pending: the FT primary
             included it and the verifier has not confirmed it. It is counted
             in `screening_in_progress`, reported as `verification_pending`,
             and is never a failure; it is not in `n_eligible`;
         (d) a paper in P whose live eligibility-axis state is `full_text_out`
             is a failure — reversed on the eligibility axis without a status
             write. (Such a paper is also in P \ E; the failure governs.)
       Each paper named in a failure carries its status token verbatim. The
       total check is unchanged: the remainder is still defined by status, so
       a P \ E paper is in the remainder and the partition still sums to the
       database's paper count.
    2. Extraction partition (events only): the eligible papers are exactly
       studies included + every failure box + extraction in progress.
    Plus the PDF sub-count check and reports-not-retrieved + PDF eligibility
    == PDF_EXCLUDED.

    `flow` is the result of `generate_prisma_flow(db)`; computed when omitted.
    Returns dict with {valid, total_db, total_prisma, discrepancy,
    verification_pending, details}.
    Raises ValueError if counts don't reconcile.
    """
    conn = db._conn
    if flow is None:
        flow = generate_prisma_flow(db)

    status_of = dict(conn.execute("SELECT id, status FROM papers").fetchall())
    total_db = len(status_of)
    status_counts = _status_counts(conn)
    screening_total = sum(status_counts.get(t, 0) for t in SCREENING_TOKENS)
    remainder_ids = {pid for pid, s in status_of.items() if s not in SCREENING_TOKENS}
    eligible_ids = set(eligible_paper_ids(conn))
    pending_ids = _pending_ids(conn)
    total_prisma = screening_total + len(remainder_ids)

    details = []

    # Identity 1: screening partition + seam
    if total_prisma != total_db:
        details.append(
            f"Total mismatch: DB has {total_db} papers but screening tokens + "
            f"remainder account for {total_prisma}"
        )
    not_eligible = sorted(remainder_ids - pending_ids - eligible_ids)          # (b)
    if not_eligible:
        details.append(
            "Seam: papers past screening on papers.status but not eligible on the "
            "eligibility axis: "
            + ", ".join(f"{pid} ({status_of[pid]})" for pid in not_eligible)
        )
    not_in_remainder = sorted(eligible_ids - remainder_ids)                   # (a)
    if not_in_remainder:
        details.append(
            "Seam: papers eligible on the eligibility axis but at a screening "
            "token on papers.status: "
            + ", ".join(f"{pid} ({status_of.get(pid)})" for pid in not_in_remainder)
        )

    reversed_ids = sorted(pid for pid in pending_ids                        # (d)
                          if effective_state(conn, pid).eligibility == "full_text_out")
    if reversed_ids:
        details.append(
            "Seam: papers at FT_ELIGIBLE reversed to full_text_out on the "
            "eligibility axis without a status write: "
            + ", ".join(f"{pid} ({status_of[pid]})" for pid in reversed_ids)
        )
    verification_pending = len(pending_ids - eligible_ids)                   # (c)

    # Identity 2: extraction partition
    extraction_total = (
        flow["studies_included"]
        + sum(flow[token] for token in FAILURE_STATES)
        + flow["extraction_in_progress"]
    )
    if extraction_total != flow["n_eligible"]:
        details.append(
            f"Extraction partition: {flow['n_eligible']} eligible papers but "
            f"included + failures + in progress = {extraction_total}"
        )

    # PDF_EXCLUDED sub-counts
    pdf_sub_total = sum(flow["pdf_exclusion_reasons"].values())
    if pdf_sub_total != flow["pdf_excluded"]:
        details.append(
            f"PDF_EXCLUDED sub-counts ({pdf_sub_total}) != total ({flow['pdf_excluded']})"
        )

    # reports_not_retrieved + pdf_eligibility = pdf_excluded
    pdf_recon = flow["reports_not_retrieved"] + sum(flow["pdf_eligibility_exclusions"].values())
    if pdf_recon != flow["pdf_excluded"]:
        details.append(
            f"Reports not retrieved ({flow['reports_not_retrieved']}) + "
            f"PDF eligibility ({sum(flow['pdf_eligibility_exclusions'].values())}) "
            f"!= PDF_EXCLUDED ({flow['pdf_excluded']})"
        )

    result = {
        "valid": len(details) == 0,
        "total_db": total_db,
        "total_prisma": total_prisma,
        "discrepancy": total_prisma - total_db,
        "verification_pending": verification_pending,
        "details": details,
    }

    if not result["valid"]:
        raise ValueError(
            f"PRISMA reconciliation failed: {'; '.join(details)}"
        )

    return result


# ── CSV Export ───────────────────────────────────────────────────────


def export_prisma_csv(db: ReviewDatabase, output_path: str) -> None:
    """Write PRISMA flow data as a CSV file, after reconciling it."""
    flow = generate_prisma_flow(db)
    validate_prisma_counts(db, flow)

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
    rows.append(("Reports sought for retrieval", flow["reports_sought"], ""))

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

    rows.append(("Full text flagged", flow["ft_flagged"], "For human review (FT)"))

    if flow["screening_in_progress"] > 0:
        rows.append(("Screening in progress", flow["screening_in_progress"],
                     "Papers still in screening"))

    rows.append(("Eligible for extraction", flow["n_eligible"], ""))

    for token in FAILURE_STATES:
        for reason, count in sorted(flow["failure_reasons"][token].items()):
            if count > 0:
                rows.append((_FAILURE_LABELS[token], count, reason))

    if flow["extraction_in_progress"] > 0:
        rows.append(("Extraction in progress", flow["extraction_in_progress"],
                     "Eligible papers not yet audited"))

    rows.append(("Studies included", flow["studies_included"], ""))

    tmp_path = output_path + ".tmp"
    try:
        with open(tmp_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(rows)
        os.replace(tmp_path, output_path)
    except BaseException:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise

    logger.info("PRISMA CSV exported to %s", output_path)
