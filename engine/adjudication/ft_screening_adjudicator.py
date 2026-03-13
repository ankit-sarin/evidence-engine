"""Full-text screening adjudication pipeline — export FT_FLAGGED papers for
human review, import decisions back into the database.

Mirrors the screening_adjudicator pattern but for full-text screening results.
"""

import logging
from datetime import datetime, timezone
from pathlib import Path

from engine.adjudication.schema import ensure_adjudication_table
from engine.adjudication.workflow import complete_stage
from engine.core.database import ReviewDatabase

logger = logging.getLogger(__name__)


# ── Data Collection ─────────────────────────────────────────────────


def _collect_ft_flagged(db: ReviewDatabase) -> list[dict]:
    """Collect FT_FLAGGED papers from review.db with screening rationale."""
    papers = db.get_papers_by_status("FT_FLAGGED")
    results = []

    for p in papers:
        pid = p["id"]

        # Get FT screening decision (primary)
        ft_row = db._conn.execute(
            "SELECT model, decision, reason_code, rationale, confidence "
            "FROM ft_screening_decisions WHERE paper_id = ? ORDER BY id DESC LIMIT 1",
            (pid,),
        ).fetchone()

        primary_decision = ft_row["decision"] if ft_row else ""
        primary_rationale = ft_row["rationale"] if ft_row else ""
        reason_code = ft_row["reason_code"] if ft_row else ""

        # Get FT verification decision
        vrow = db._conn.execute(
            "SELECT model, decision, rationale, confidence "
            "FROM ft_verification_decisions WHERE paper_id = ? "
            "ORDER BY id DESC LIMIT 1",
            (pid,),
        ).fetchone()

        verifier_decision = vrow["decision"] if vrow else ""
        verifier_rationale = vrow["rationale"] if vrow else ""

        results.append({
            "paper_id": pid,
            "title": p["title"],
            "abstract": p.get("abstract") or "",
            "doi": p.get("doi") or "",
            "pmid": p.get("pmid") or "",
            "year": p.get("year") or "",
            "journal": p.get("journal") or "",
            "reason_code": reason_code,
            "primary_decision": primary_decision,
            "primary_rationale": primary_rationale,
            "verifier_decision": verifier_decision,
            "verifier_rationale": verifier_rationale,
        })

    return results


# ── Export ──────────────────────────────────────────────────────────


def export_ft_adjudication_queue(
    review_db: ReviewDatabase,
    output_path: str | Path,
    *,
    format: str = "xlsx",
) -> dict:
    """Export all FT_FLAGGED papers as a human-review Excel queue.

    Returns summary dict with counts.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    all_flagged = _collect_ft_flagged(review_db)

    if not all_flagged:
        logger.warning("No FT_FLAGGED papers found — nothing to export")
        return {"total": 0, "by_reason": {}}

    # Count by reason code
    reason_counts: dict[str, int] = {}
    for p in all_flagged:
        rc = p["reason_code"] or "unknown"
        reason_counts[rc] = reason_counts.get(rc, 0) + 1

    # Sort by reason code then title
    all_flagged.sort(key=lambda p: (p["reason_code"] or "zzz", p["title"]))

    if format == "xlsx":
        _write_ft_xlsx(all_flagged, output_path, reason_counts)
    else:
        raise ValueError(f"Unsupported format: {format}")

    logger.info(
        "Exported FT adjudication queue: %d papers to %s",
        len(all_flagged), output_path,
    )
    for rc, count in sorted(reason_counts.items()):
        logger.info("  %s: %d", rc, count)

    return {
        "total": len(all_flagged),
        "by_reason": reason_counts,
        "output_path": str(output_path),
    }


def _write_ft_xlsx(papers: list[dict], output_path: Path,
                   reason_counts: dict) -> None:
    """Write the FT adjudication queue as an Excel workbook."""
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill

    wb = Workbook()

    # ── Sheet 1: Review Queue ──
    ws = wb.active
    ws.title = "FT Review Queue"

    headers = [
        "Row #",
        "Paper ID",
        "Reason Code",
        "Title",
        "Abstract",
        "DOI",
        "PMID",
        "Year",
        "Journal",
        "Primary Decision",
        "Primary Rationale",
        "Verifier Decision",
        "Verifier Rationale",
        "DECISION (FT_ELIGIBLE/FT_SCREENED_OUT)",
        "Notes (optional)",
    ]

    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill(start_color="0A5E56", end_color="0A5E56", fill_type="solid")
    decision_fill = PatternFill(start_color="FFF2CC", end_color="FFF2CC", fill_type="solid")
    wrap_align = Alignment(wrap_text=True, vertical="top")

    for col, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=header)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center", wrap_text=True)

    for i, paper in enumerate(papers, 1):
        row_num = i + 1

        ws.cell(row=row_num, column=1, value=i)
        ws.cell(row=row_num, column=2, value=paper["paper_id"])
        ws.cell(row=row_num, column=3, value=paper["reason_code"])
        ws.cell(row=row_num, column=4, value=paper["title"])
        ws.cell(row=row_num, column=5, value=paper["abstract"][:2000] if paper["abstract"] else "")
        ws.cell(row=row_num, column=6, value=paper["doi"])
        ws.cell(row=row_num, column=7, value=paper["pmid"])
        ws.cell(row=row_num, column=8, value=paper.get("year", ""))
        ws.cell(row=row_num, column=9, value=paper["journal"])
        ws.cell(row=row_num, column=10, value=paper["primary_decision"])
        ws.cell(row=row_num, column=11, value=paper["primary_rationale"][:1000] if paper["primary_rationale"] else "")
        ws.cell(row=row_num, column=12, value=paper["verifier_decision"])
        ws.cell(row=row_num, column=13, value=paper["verifier_rationale"][:1000] if paper["verifier_rationale"] else "")

        decision_cell = ws.cell(row=row_num, column=14, value="")
        decision_cell.fill = decision_fill

        ws.cell(row=row_num, column=15, value="")

        for col in [4, 5, 11, 13]:
            ws.cell(row=row_num, column=col).alignment = wrap_align

    ws.freeze_panes = "A2"

    # ── Sheet 2: Reason Summary ──
    ws2 = wb.create_sheet("Reason Summary")
    ws2.cell(row=1, column=1, value="Reason Code").font = Font(bold=True)
    ws2.cell(row=1, column=2, value="Count").font = Font(bold=True)

    for i, (rc, count) in enumerate(sorted(reason_counts.items()), 2):
        ws2.cell(row=i, column=1, value=rc)
        ws2.cell(row=i, column=2, value=count)

    ws2.column_dimensions["A"].width = 30
    ws2.column_dimensions["B"].width = 10

    # ── Sheet 3: Instructions ──
    ws3 = wb.create_sheet("Instructions")
    instructions = [
        "FULL-TEXT SCREENING ADJUDICATION INSTRUCTIONS",
        "",
        "1. Review each paper in the 'FT Review Queue' sheet.",
        "2. For each paper, enter FT_ELIGIBLE or FT_SCREENED_OUT in the yellow DECISION column (N).",
        "3. Optionally add notes in the Notes column (O).",
        "",
        "CONTEXT:",
        "- These papers were flagged during full-text screening verification.",
        "- The primary screener marked them eligible but the verifier disagreed.",
        "- Your decision resolves the disagreement.",
        "",
        "DECISION CRITERIA:",
        "- FT_ELIGIBLE: The paper meets all inclusion criteria based on full text.",
        "- FT_SCREENED_OUT: The paper should be excluded (see reason code for likely cause).",
        "",
        "After completing all decisions, save the file and run:",
        "  from engine.adjudication.ft_screening_adjudicator import import_ft_adjudication_decisions",
        "  import_ft_adjudication_decisions(review_db, 'path/to/this/file.xlsx')",
    ]
    for i, line in enumerate(instructions, 1):
        ws3.cell(row=i, column=1, value=line)
    ws3.column_dimensions["A"].width = 90

    wb.save(output_path)


# ── Import ─────────────────────────────────────────────────────────


def import_ft_adjudication_decisions(
    review_db: ReviewDatabase,
    input_path: str | Path,
) -> dict:
    """Read completed FT adjudication Excel, write decisions to database.

    For each paper:
      FT_ELIGIBLE → transition to FT_ELIGIBLE
      FT_SCREENED_OUT → transition to FT_SCREENED_OUT

    Records decisions in ft_screening_adjudication table.
    Auto-advances FULL_TEXT_ADJUDICATION_COMPLETE if zero unresolved.

    Returns summary dict.
    """
    from openpyxl import load_workbook

    input_path = Path(input_path)
    wb = load_workbook(input_path)
    ws = wb["FT Review Queue"]

    ensure_adjudication_table(review_db._conn)

    now = datetime.now(timezone.utc).isoformat()
    stats = {"ft_eligible": 0, "ft_screened_out": 0, "missing": 0, "invalid": 0, "total": 0}
    warnings = []

    for row in ws.iter_rows(min_row=2, values_only=False):
        title_val = row[3].value  # column D = title
        if not title_val:
            continue

        stats["total"] += 1

        row_num = row[0].value        # column A = Row #
        paper_id = row[1].value       # column B = Paper ID
        reason_code = row[2].value    # column C = Reason Code
        title = row[3].value          # column D
        decision_raw = row[13].value  # column N = DECISION
        notes = row[14].value or ""   # column O = Notes

        if not decision_raw:
            stats["missing"] += 1
            warnings.append(f"Row {row_num}: '{title[:60]}...' — no decision")
            continue

        decision = decision_raw.strip().upper()
        if decision not in ("FT_ELIGIBLE", "FT_SCREENED_OUT"):
            stats["invalid"] += 1
            warnings.append(
                f"Row {row_num}: invalid decision '{decision_raw}' "
                f"(must be FT_ELIGIBLE or FT_SCREENED_OUT)"
            )
            continue

        # Record in ft_screening_adjudication table
        review_db._conn.execute(
            """INSERT INTO ft_screening_adjudication
               (paper_id, title, reason_code, adjudication_decision,
                adjudication_reason, adjudication_timestamp, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (paper_id, title, reason_code, decision, notes, now, now),
        )

        # Update paper status (FT_FLAGGED → FT_ELIGIBLE or FT_SCREENED_OUT)
        if paper_id:
            try:
                review_db.update_status(int(paper_id), decision)
            except ValueError as e:
                warnings.append(f"Row {row_num}: status update failed — {e}")

        if decision == "FT_ELIGIBLE":
            stats["ft_eligible"] += 1
        else:
            stats["ft_screened_out"] += 1

    review_db._conn.commit()

    if warnings:
        for w in warnings[:10]:
            logger.warning(w)
        if len(warnings) > 10:
            logger.warning("... and %d more warnings", len(warnings) - 10)

    logger.info(
        "FT adjudication import: %d eligible, %d screened out, "
        "%d missing, %d invalid (of %d total)",
        stats["ft_eligible"], stats["ft_screened_out"],
        stats["missing"], stats["invalid"], stats["total"],
    )

    # Auto-advance workflow: FULL_TEXT_ADJUDICATION_COMPLETE if zero unresolved
    if stats["missing"] == 0 and stats["invalid"] == 0:
        complete_stage(
            review_db._conn, "FULL_TEXT_ADJUDICATION_COMPLETE",
            metadata=(
                f"{stats['ft_eligible']} eligible, {stats['ft_screened_out']} screened out "
                f"(of {stats['total']} total)"
            ),
        )
    else:
        logger.warning(
            "FT adjudication not complete: %d missing + %d invalid decisions remain",
            stats["missing"], stats["invalid"],
        )

    return {"stats": stats, "warnings": warnings}


# ── Pipeline Gate ──────────────────────────────────────────────────


def check_ft_adjudication_gate(review_db: ReviewDatabase) -> int:
    """Check for unresolved FT_FLAGGED papers. Returns count."""
    flagged = review_db.get_papers_by_status("FT_FLAGGED")
    return len(flagged)
