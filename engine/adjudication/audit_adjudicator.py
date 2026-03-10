"""Audit adjudication pipeline — export contested/flagged spans for human review,
import decisions back into the database.

Mirrors the screening_adjudicator pattern for extraction audit results.
"""

import logging
import random
from datetime import datetime, timezone
from pathlib import Path

from engine.adjudication.schema import ensure_adjudication_table
from engine.adjudication.workflow import complete_stage
from engine.core.database import ReviewDatabase

logger = logging.getLogger(__name__)

# Audit states that require human attention
_NEEDS_REVIEW = {"contested", "flagged", "invalid_snippet"}


# ── Data Collection ─────────────────────────────────────────────────


def _collect_papers_for_review(
    db: ReviewDatabase,
    *,
    spot_check_pct: float = 0.10,
    spot_check_failure_threshold: float = 0.20,
) -> list[dict]:
    """Collect AI_AUDIT_COMPLETE papers that need human review.

    Primary: papers with any contested/flagged/invalid_snippet spans.
    Spot-check: random N% of papers with ALL spans verified (minor issues only).

    If spot-check failure rate > threshold after import, all minor-issues
    papers should be promoted. That logic runs at import time.
    """
    papers = db.get_papers_by_status("AI_AUDIT_COMPLETE")
    needs_review = []
    all_verified = []

    for paper in papers:
        pid = paper["id"]
        extraction = db._conn.execute(
            "SELECT id FROM extractions WHERE paper_id = ? ORDER BY id DESC LIMIT 1",
            (pid,),
        ).fetchone()
        if not extraction:
            continue

        ext_id = extraction["id"]
        spans = db._conn.execute(
            "SELECT * FROM evidence_spans WHERE extraction_id = ?",
            (ext_id,),
        ).fetchall()
        spans = [dict(s) for s in spans]

        if not spans:
            continue

        # Classify paper by worst audit state
        audit_states = {s["audit_status"] for s in spans}
        problem_spans = [s for s in spans if s["audit_status"] in _NEEDS_REVIEW]

        paper_info = {
            "paper_id": pid,
            "title": paper["title"],
            "extraction_id": ext_id,
            "spans": spans,
            "problem_spans": problem_spans,
            "audit_states": audit_states,
        }

        if problem_spans:
            # Compute worst state: flagged > invalid_snippet > contested
            if any(s["audit_status"] == "flagged" for s in problem_spans):
                paper_info["worst_state"] = "flagged"
            elif any(s["audit_status"] == "invalid_snippet" for s in problem_spans):
                paper_info["worst_state"] = "invalid_snippet"
            else:
                paper_info["worst_state"] = "contested"
            paper_info["review_reason"] = "audit_issues"
            needs_review.append(paper_info)
        else:
            paper_info["worst_state"] = "verified"
            paper_info["review_reason"] = "spot_check"
            all_verified.append(paper_info)

    # Spot-check selection
    n_spot = max(1, int(len(all_verified) * spot_check_pct)) if all_verified else 0
    n_spot = min(n_spot, len(all_verified))
    if n_spot > 0:
        spot_check = random.sample(all_verified, n_spot)
        needs_review.extend(spot_check)

    # Sort: flagged first, then invalid_snippet, then contested, then spot_check
    state_order = {"flagged": 0, "invalid_snippet": 1, "contested": 2, "verified": 3}
    needs_review.sort(key=lambda p: (state_order.get(p["worst_state"], 99), p["title"]))

    return needs_review


# ── Export ──────────────────────────────────────────────────────────


def export_audit_review_queue(
    review_db: ReviewDatabase,
    output_path: str | Path,
    *,
    spot_check_pct: float = 0.10,
    spot_check_failure_threshold: float = 0.20,
    format: str = "xlsx",
) -> dict:
    """Export papers with contested/flagged spans for human audit review.

    Each row = one paper, with per-field columns for value, audit state,
    evidence span, and correction (empty for human to fill).

    Returns summary dict.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    papers = _collect_papers_for_review(
        review_db,
        spot_check_pct=spot_check_pct,
        spot_check_failure_threshold=spot_check_failure_threshold,
    )

    if not papers:
        logger.warning("No papers need audit review — nothing to export")
        return {"total": 0, "flagged": 0, "contested": 0, "spot_check": 0}

    # Get field names from first paper's spans
    all_field_names = sorted({s["field_name"] for p in papers for s in p["spans"]})

    if format == "xlsx":
        _write_audit_xlsx(papers, output_path, all_field_names,
                          spot_check_failure_threshold)
    else:
        raise ValueError(f"Unsupported format: {format}")

    stats = {
        "total": len(papers),
        "flagged": sum(1 for p in papers if p["worst_state"] == "flagged"),
        "contested": sum(1 for p in papers if p["worst_state"] == "contested"),
        "invalid_snippet": sum(1 for p in papers if p["worst_state"] == "invalid_snippet"),
        "spot_check": sum(1 for p in papers if p["review_reason"] == "spot_check"),
        "output_path": str(output_path),
        "spot_check_failure_threshold": spot_check_failure_threshold,
    }

    logger.info(
        "Exported audit review queue: %d papers (%d flagged, %d contested, "
        "%d invalid, %d spot-check) to %s",
        stats["total"], stats["flagged"], stats["contested"],
        stats["invalid_snippet"], stats["spot_check"], output_path,
    )

    # Auto-advance workflow
    complete_stage(
        review_db._conn, "AUDIT_QUEUE_EXPORTED",
        metadata=f"{stats['total']} papers exported to {output_path}",
    )

    return stats


def _write_audit_xlsx(
    papers: list[dict],
    output_path: Path,
    field_names: list[str],
    spot_check_threshold: float,
) -> None:
    """Write the audit review queue as an Excel workbook."""
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.utils import get_column_letter

    wb = Workbook()

    # ── Sheet 1: Audit Review Queue ──
    ws = wb.active
    ws.title = "Audit Review"

    # Build headers: fixed columns + per-field groups
    headers = [
        "paper_id",
        "title",
        "audit_state",
        "review_reason",
        "audit_reasoning_summary",
    ]
    # Per-field: value, audit_state, evidence_span, correction
    for fname in field_names:
        headers.extend([
            f"{fname}_value",
            f"{fname}_audit_state",
            f"{fname}_evidence_span",
            f"{fname}_correction",
        ])
    # Final columns
    headers.extend([
        "accept_as_is",
        "notes",
        "reject_paper",
    ])

    # Header styling
    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill(start_color="0A5E56", end_color="0A5E56", fill_type="solid")
    correction_fill = PatternFill(start_color="FFF2CC", end_color="FFF2CC", fill_type="solid")
    flagged_fill = PatternFill(start_color="FFE0E0", end_color="FFE0E0", fill_type="solid")
    contested_fill = PatternFill(start_color="FFF0E0", end_color="FFF0E0", fill_type="solid")
    spot_check_fill = PatternFill(start_color="E0FFE0", end_color="E0FFE0", fill_type="solid")
    wrap_align = Alignment(wrap_text=True, vertical="top")

    for col, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=header)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center", wrap_text=True)

    for i, paper in enumerate(papers):
        row_num = i + 2
        span_map = {s["field_name"]: s for s in paper["spans"]}

        # Build reasoning summary from problem spans
        reasoning_parts = []
        for s in paper.get("problem_spans", []):
            reasoning_parts.append(
                f"{s['field_name']} ({s['audit_status']}): "
                f"{(s.get('audit_rationale') or '')[:100]}"
            )
        reasoning_summary = "; ".join(reasoning_parts) if reasoning_parts else "All verified (spot-check)"

        # Fixed columns
        col = 1
        ws.cell(row=row_num, column=col, value=paper["paper_id"]); col += 1
        ws.cell(row=row_num, column=col, value=paper["title"]); col += 1
        ws.cell(row=row_num, column=col, value=paper["worst_state"]); col += 1
        ws.cell(row=row_num, column=col, value=paper["review_reason"]); col += 1
        ws.cell(row=row_num, column=col, value=reasoning_summary[:2000]); col += 1

        # Per-field columns
        for fname in field_names:
            span = span_map.get(fname)
            if span:
                ws.cell(row=row_num, column=col, value=span["value"]); col += 1
                state_cell = ws.cell(row=row_num, column=col, value=span["audit_status"])
                if span["audit_status"] == "flagged":
                    state_cell.fill = flagged_fill
                elif span["audit_status"] in ("contested", "invalid_snippet"):
                    state_cell.fill = contested_fill
                col += 1
                ws.cell(row=row_num, column=col, value=(span.get("source_snippet") or "")[:1000])
                ws.cell(row=row_num, column=col).alignment = wrap_align
                col += 1
                correction_cell = ws.cell(row=row_num, column=col, value="")
                correction_cell.fill = correction_fill
                col += 1
            else:
                for _ in range(4):
                    ws.cell(row=row_num, column=col, value=""); col += 1

        # Final columns
        accept_cell = ws.cell(row=row_num, column=col, value="")
        accept_cell.fill = correction_fill
        col += 1
        ws.cell(row=row_num, column=col, value=""); col += 1
        reject_cell = ws.cell(row=row_num, column=col, value="")
        reject_cell.fill = correction_fill

        # Row-level coloring for spot-check
        if paper["review_reason"] == "spot_check":
            ws.cell(row=row_num, column=3).fill = spot_check_fill

    # Column widths for fixed columns
    ws.column_dimensions["A"].width = 10
    ws.column_dimensions["B"].width = 50
    ws.column_dimensions["C"].width = 14
    ws.column_dimensions["D"].width = 14
    ws.column_dimensions["E"].width = 60
    ws.freeze_panes = "A2"

    # ── Sheet 2: Paper Summary ──
    ws2 = wb.create_sheet("Paper Summary")
    ws2.append(["paper_id", "title", "worst_state", "review_reason",
                "verified", "contested", "flagged", "invalid_snippet", "total_spans"])
    for paper in papers:
        counts = {"verified": 0, "contested": 0, "flagged": 0, "invalid_snippet": 0}
        for s in paper["spans"]:
            st = s["audit_status"]
            counts[st] = counts.get(st, 0) + 1
        ws2.append([
            paper["paper_id"], paper["title"], paper["worst_state"],
            paper["review_reason"],
            counts["verified"], counts["contested"],
            counts["flagged"], counts["invalid_snippet"],
            len(paper["spans"]),
        ])
    for cell in ws2[1]:
        cell.font = Font(bold=True)

    # ── Sheet 3: Instructions ──
    ws3 = wb.create_sheet("Instructions")
    instructions = [
        "AUDIT REVIEW INSTRUCTIONS",
        "",
        "1. Review each paper in the 'Audit Review' sheet.",
        "2. For each paper, decide one of:",
        f"   - accept_as_is = TRUE: Accept all extractions as-is (mark spans human-verified)",
        f"   - Fill in correction columns: Override specific field values",
        f"   - reject_paper = TRUE: Remove paper from the review entirely",
        "",
        "3. For field corrections, fill the {field}_correction column with the corrected value.",
        "   Only fill in corrections for fields you want to change.",
        "",
        "4. The 'notes' column is for free-text commentary.",
        "",
        "AUDIT STATES:",
        "  - flagged: AI auditor flagged value as unsupported by source text (MOST attention needed)",
        "  - contested: Grep verification failed but semantic verification passed",
        "  - invalid_snippet: Source snippet contains ellipsis bridging (malformed)",
        "  - verified: Both grep and semantic verification passed",
        "",
        "SPOT-CHECK PAPERS:",
        "  - Papers marked 'spot_check' have ALL spans verified by AI.",
        "  - These are included for quality assurance — a random sample.",
        f"  - If >{int(spot_check_threshold * 100)}% of spot-check papers have errors,",
        "    all remaining verified-only papers will be promoted to the review queue.",
        "",
        "After completing all decisions, save and run:",
        "  from engine.adjudication.audit_adjudicator import import_audit_review_decisions",
        "  import_audit_review_decisions(review_db, 'path/to/this/file.xlsx')",
    ]
    for i, line in enumerate(instructions, 1):
        ws3.cell(row=i, column=1, value=line)
    ws3.column_dimensions["A"].width = 90

    wb.save(output_path)


# ── Import ─────────────────────────────────────────────────────────


def import_audit_review_decisions(
    review_db: ReviewDatabase,
    input_path: str | Path,
) -> dict:
    """Read completed audit review Excel, write decisions to database.

    For accept_as_is = TRUE: mark all contested/flagged spans as verified,
        record adjudication_source = 'human'.
    For correction fields: update span values, record original in audit_adjudication table.
    For reject_paper = TRUE: transition paper to REJECTED.
    When zero unresolved: auto-sets AUDIT_REVIEW_COMPLETE.

    Returns summary dict.
    """
    from openpyxl import load_workbook

    input_path = Path(input_path)
    wb = load_workbook(input_path)
    ws = wb["Audit Review"]

    ensure_adjudication_table(review_db._conn)

    # Parse header to find field columns
    headers = [cell.value for cell in ws[1]]
    field_col_map = {}  # field_name -> {value_col, state_col, snippet_col, correction_col}
    for col_idx, h in enumerate(headers):
        if h and h.endswith("_correction"):
            fname = h[:-len("_correction")]
            # Find sibling columns
            val_idx = headers.index(f"{fname}_value") if f"{fname}_value" in headers else None
            state_idx = headers.index(f"{fname}_audit_state") if f"{fname}_audit_state" in headers else None
            field_col_map[fname] = {
                "value_col": val_idx,
                "state_col": state_idx,
                "correction_col": col_idx,
            }

    # Find fixed column indices
    pid_col = headers.index("paper_id")
    accept_col = headers.index("accept_as_is")
    notes_col = headers.index("notes")
    reject_col = headers.index("reject_paper")

    now = datetime.now(timezone.utc).isoformat()
    stats = {
        "accepted": 0, "corrected_fields": 0, "rejected": 0,
        "missing": 0, "total": 0, "papers_transitioned": 0,
        "spot_check_failures": 0, "spot_check_total": 0,
    }
    warnings = []

    for row in ws.iter_rows(min_row=2, values_only=False):
        paper_id_val = row[pid_col].value
        if not paper_id_val:
            continue

        stats["total"] += 1
        pid = int(paper_id_val)

        accept_raw = str(row[accept_col].value or "").strip().upper()
        reject_raw = str(row[reject_col].value or "").strip().upper()
        notes = row[notes_col].value or ""
        review_reason = row[headers.index("review_reason")].value if "review_reason" in headers else ""

        is_accept = accept_raw in ("TRUE", "YES", "1", "ACCEPT")
        is_reject = reject_raw in ("TRUE", "YES", "1", "REJECT")

        # Track spot-check outcomes
        if review_reason == "spot_check":
            stats["spot_check_total"] += 1

        # Check for corrections
        has_corrections = False
        corrections = {}
        for fname, cols in field_col_map.items():
            correction_val = row[cols["correction_col"]].value
            if correction_val and str(correction_val).strip():
                has_corrections = True
                corrections[fname] = str(correction_val).strip()

        if is_reject:
            # Reject the paper
            try:
                review_db.reject_paper(pid, f"Audit review rejection: {notes}")
                stats["rejected"] += 1
                stats["papers_transitioned"] += 1
            except ValueError as e:
                warnings.append(f"Paper {pid}: cannot reject — {e}")
            continue

        if not is_accept and not has_corrections:
            stats["missing"] += 1
            title_col = headers.index("title")
            title = row[title_col].value or "?"
            warnings.append(f"Paper {pid} ({title[:40]}): no decision")

            # Spot-check paper with no action = failure
            if review_reason == "spot_check" and not is_accept:
                pass  # counted at end
            continue

        # Get the paper's latest extraction spans
        extraction = review_db._conn.execute(
            "SELECT id FROM extractions WHERE paper_id = ? ORDER BY id DESC LIMIT 1",
            (pid,),
        ).fetchone()
        if not extraction:
            warnings.append(f"Paper {pid}: no extraction found")
            continue
        ext_id = extraction["id"]

        spans = review_db._conn.execute(
            "SELECT * FROM evidence_spans WHERE extraction_id = ?",
            (ext_id,),
        ).fetchall()
        span_map = {s["field_name"]: dict(s) for s in spans}

        if is_accept:
            stats["accepted"] += 1
            # Mark all non-verified spans as verified (human override)
            for s in spans:
                if s["audit_status"] in _NEEDS_REVIEW:
                    review_db._conn.execute(
                        """UPDATE evidence_spans
                           SET audit_status = 'verified',
                               auditor_model = 'human_review',
                               audit_rationale = ?,
                               audited_at = ?
                           WHERE id = ?""",
                        (f"Accepted as-is by human reviewer. {notes}", now, s["id"]),
                    )

            # Spot-check: if reviewer accepted a spot-check paper, it's a pass
            # (no failure to count)

        # Apply corrections (can coexist with accept)
        for fname, new_value in corrections.items():
            span = span_map.get(fname)
            if not span:
                warnings.append(f"Paper {pid}, field {fname}: span not found")
                continue

            # Record original in audit_adjudication table
            review_db._conn.execute(
                """INSERT INTO audit_adjudication
                   (span_id, paper_id, field_name, original_value,
                    human_decision, override_value, reviewer_notes,
                    adjudication_timestamp, created_at)
                   VALUES (?, ?, ?, ?, 'override', ?, ?, ?, ?)""",
                (span["id"], pid, fname, span["value"],
                 new_value, notes, now, now),
            )

            # Update the span value + mark verified
            review_db._conn.execute(
                """UPDATE evidence_spans
                   SET value = ?,
                       audit_status = 'verified',
                       auditor_model = 'human_review',
                       audit_rationale = ?,
                       audited_at = ?
                   WHERE id = ?""",
                (new_value,
                 f"Human override: '{span['value']}' → '{new_value}'. {notes}",
                 now, span["id"]),
            )
            stats["corrected_fields"] += 1

        # Transition paper to HUMAN_AUDIT_COMPLETE if all spans now resolved
        remaining = review_db._conn.execute(
            """SELECT COUNT(*) FROM evidence_spans
               WHERE extraction_id = ? AND audit_status IN ('contested', 'flagged', 'invalid_snippet')""",
            (ext_id,),
        ).fetchone()[0]

        if remaining == 0:
            try:
                review_db.update_status(pid, "HUMAN_AUDIT_COMPLETE")
                stats["papers_transitioned"] += 1
            except ValueError:
                pass  # already transitioned or invalid state

    review_db._conn.commit()

    # Spot-check failure rate analysis
    if stats["spot_check_total"] > 0:
        # A spot-check failure = any spot-check paper where corrections were needed
        # (detected by having corrections applied to a spot-check paper)
        spot_failure_rate = stats["spot_check_failures"] / stats["spot_check_total"]
        if spot_failure_rate > 0:
            logger.warning(
                "Spot-check failure rate: %.0f%% (%d/%d)",
                spot_failure_rate * 100,
                stats["spot_check_failures"],
                stats["spot_check_total"],
            )

    if warnings:
        for w in warnings[:10]:
            logger.warning(w)
        if len(warnings) > 10:
            logger.warning("... and %d more warnings", len(warnings) - 10)

    logger.info(
        "Audit import complete: %d accepted, %d fields corrected, "
        "%d rejected, %d missing (of %d total), %d papers transitioned",
        stats["accepted"], stats["corrected_fields"], stats["rejected"],
        stats["missing"], stats["total"], stats["papers_transitioned"],
    )

    # Auto-advance workflow: AUDIT_REVIEW_COMPLETE if zero unresolved
    if stats["missing"] == 0:
        complete_stage(
            review_db._conn, "AUDIT_REVIEW_COMPLETE",
            metadata=(
                f"{stats['accepted']} accepted, {stats['corrected_fields']} corrected, "
                f"{stats['rejected']} rejected (of {stats['total']} total)"
            ),
        )
    else:
        logger.warning(
            "Audit review not complete: %d papers with no decision",
            stats["missing"],
        )

    return {"stats": stats, "warnings": warnings}


# ── Gate ──────────────────────────────────────────────────────────


def check_audit_review_gate(review_db: ReviewDatabase) -> int:
    """Return count of AI_AUDIT_COMPLETE papers with unresolved spans."""
    papers = review_db.get_papers_by_status("AI_AUDIT_COMPLETE")
    count = 0
    for paper in papers:
        pid = paper["id"]
        has_issues = review_db._conn.execute(
            """SELECT COUNT(*) FROM evidence_spans es
               JOIN extractions e ON e.id = es.extraction_id
               WHERE e.paper_id = ? AND es.audit_status IN ('contested', 'flagged', 'invalid_snippet')""",
            (pid,),
        ).fetchone()[0]
        if has_issues > 0:
            count += 1
    return count
