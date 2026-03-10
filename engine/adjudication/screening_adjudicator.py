"""Screening adjudication pipeline — export flagged papers for human review,
import decisions back into the database.

Works with two data sources:
  1. review.db SCREEN_FLAGGED papers (standard pipeline)
  2. Expanded search CSV files (pre-ingestion screening)
"""

import csv
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from engine.adjudication.categorizer import (
    CategoryConfig,
    categorize_paper,
    get_category_descriptions,
    load_config,
)
from engine.adjudication.schema import ensure_adjudication_table
from engine.adjudication.workflow import complete_stage, is_adjudication_complete
from engine.core.database import ReviewDatabase

logger = logging.getLogger(__name__)

EXPANDED_SEARCH_DIR = Path("data/surgical_autonomy/expanded_search")


# ── Data Collection ─────────────────────────────────────────────────


def _collect_db_flagged(db: ReviewDatabase) -> list[dict]:
    """Collect SCREEN_FLAGGED papers from review.db with screening rationale."""
    papers = db.get_papers_by_status("SCREEN_FLAGGED")
    results = []

    for p in papers:
        pid = p["id"]

        # Get screening decisions (pass 1 and 2)
        rows = db._conn.execute(
            "SELECT pass_number, decision, rationale, model "
            "FROM screening_decisions WHERE paper_id = ? ORDER BY pass_number",
            (pid,),
        ).fetchall()

        primary_decision = ""
        primary_rationale = ""
        for r in rows:
            if r["pass_number"] == 1:
                primary_decision = r["decision"]
                primary_rationale = r["rationale"] or ""

        # Get verification decision if any
        vrow = db._conn.execute(
            "SELECT decision, rationale, model "
            "FROM verification_decisions WHERE paper_id = ? "
            "ORDER BY id DESC LIMIT 1",
            (pid,),
        ).fetchone()

        verifier_decision = vrow["decision"] if vrow else ""
        verifier_rationale = vrow["rationale"] if vrow else ""

        results.append({
            "source_type": "db",
            "paper_id": pid,
            "external_key": "",
            "title": p["title"],
            "abstract": p.get("abstract") or "",
            "doi": p.get("doi") or "",
            "pmid": p.get("pmid") or "",
            "year": p.get("year") or "",
            "journal": p.get("journal") or "",
            "data_source": p.get("source") or "",
            "flagged_by": "primary_disagreement" if not vrow else "verifier_exclude",
            "primary_decision": primary_decision,
            "primary_rationale": primary_rationale,
            "verifier_decision": verifier_decision,
            "verifier_rationale": verifier_rationale,
        })

    return results


def _collect_expanded_flagged(expanded_dir: Path) -> list[dict]:
    """Collect flagged papers from expanded search CSV files.

    Pulls from both screening_results.csv (primary disagreements)
    and verification_results.csv (verifier excludes).
    """
    results = []
    abstracts_map: dict[str, str] = {}

    # Load abstracts for lookup
    abstracts_jsonl = expanded_dir / "abstracts.jsonl"
    if abstracts_jsonl.exists():
        with open(abstracts_jsonl) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    key = rec.get("key", "")
                    abstracts_map[key] = rec.get("abstract") or ""
                    # Also index by doi and pmid for lookup
                    if rec.get("doi"):
                        abstracts_map[rec["doi"]] = rec.get("abstract") or ""
                    if rec.get("pmid"):
                        abstracts_map[rec["pmid"]] = rec.get("abstract") or ""
                except (json.JSONDecodeError, KeyError):
                    continue

    # Phase 1: flagged from primary screening (pass1 != pass2)
    screening_csv = expanded_dir / "screening_results.csv"
    if screening_csv.exists():
        with open(screening_csv, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("screening_decision") != "flagged":
                    continue

                ext_key = row.get("doi") or row.get("pmid") or row["title"]
                abstract = abstracts_map.get(ext_key, "")
                if not abstract:
                    abstract = abstracts_map.get(row.get("doi", ""), "")
                if not abstract:
                    abstract = abstracts_map.get(row.get("pmid", ""), "")

                results.append({
                    "source_type": "expanded_csv",
                    "paper_id": None,
                    "external_key": ext_key,
                    "title": row["title"],
                    "abstract": abstract,
                    "doi": row.get("doi", ""),
                    "pmid": row.get("pmid", ""),
                    "year": row.get("year", ""),
                    "journal": row.get("journal", ""),
                    "data_source": row.get("source", ""),
                    "flagged_by": "primary_disagreement",
                    "primary_decision": f"pass1={row.get('pass1_decision', '')}, pass2={row.get('pass2_decision', '')}",
                    "primary_rationale": f"Pass1: {row.get('pass1_rationale', '')} | Pass2: {row.get('pass2_rationale', '')}",
                    "verifier_decision": "",
                    "verifier_rationale": "",
                })

    # Phase 2: flagged from verification (primary include, verifier exclude)
    verification_csv = expanded_dir / "verification_results.csv"
    if verification_csv.exists():
        with open(verification_csv, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("final_decision") != "flagged":
                    continue

                ext_key = row.get("doi") or row.get("pmid") or row["title"]
                abstract = abstracts_map.get(ext_key, "")
                if not abstract:
                    abstract = abstracts_map.get(row.get("doi", ""), "")
                if not abstract:
                    abstract = abstracts_map.get(row.get("pmid", ""), "")

                results.append({
                    "source_type": "expanded_csv",
                    "paper_id": None,
                    "external_key": ext_key,
                    "title": row["title"],
                    "abstract": abstract,
                    "doi": row.get("doi", ""),
                    "pmid": row.get("pmid", ""),
                    "year": row.get("year", ""),
                    "journal": row.get("journal", ""),
                    "data_source": row.get("source", ""),
                    "flagged_by": "verifier_exclude",
                    "primary_decision": row.get("primary_decision", "include"),
                    "primary_rationale": "",
                    "verifier_decision": row.get("verification_decision", "exclude"),
                    "verifier_rationale": row.get("verification_rationale", ""),
                })

    return results


# ── Export ──────────────────────────────────────────────────────────


def export_adjudication_queue(
    review_db: ReviewDatabase,
    output_path: str | Path,
    *,
    expanded_search_dir: Path | None = None,
    review_name: str | None = None,
    category_config: CategoryConfig | None = None,
    format: str = "xlsx",
) -> dict:
    """Export all flagged papers as a human-review Excel queue.

    Pulls SCREEN_FLAGGED from review.db plus flagged papers from
    expanded search CSVs (if expanded_search_dir is provided).

    Category config is resolved in priority order:
      1. Explicit category_config parameter
      2. YAML config at data/{review_name}/adjudication_categories.yaml
      3. No config → all papers are 'ambiguous'

    Returns summary dict with counts.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Resolve category config
    if category_config is None:
        category_config = load_config(review_name=review_name)

    # Collect flagged papers from all sources
    all_flagged = _collect_db_flagged(review_db)

    if expanded_search_dir and expanded_search_dir.exists():
        all_flagged.extend(_collect_expanded_flagged(expanded_search_dir))

    if not all_flagged:
        logger.warning("No flagged papers found — nothing to export")
        return {"total": 0, "categories": {}}

    # Auto-advance workflow: CATEGORIES_CONFIGURED if config has categories
    if category_config and category_config.categories:
        complete_stage(
            review_db._conn, "CATEGORIES_CONFIGURED",
            metadata=f"{len(category_config.categories)} categories loaded",
        )

    # Auto-categorize
    for paper in all_flagged:
        paper["auto_category"] = categorize_paper(
            paper["title"], paper["abstract"], config=category_config
        )

    # Sort: ambiguous first, then by category name
    category_order = {"ambiguous": 0}
    all_flagged.sort(key=lambda p: (
        category_order.get(p["auto_category"], 1),
        p["auto_category"],
        p["title"],
    ))

    # Count by category
    cat_counts: dict[str, int] = {}
    for p in all_flagged:
        cat = p["auto_category"]
        cat_counts[cat] = cat_counts.get(cat, 0) + 1

    if format == "xlsx":
        _write_xlsx(all_flagged, output_path, cat_counts, category_config)
    else:
        raise ValueError(f"Unsupported format: {format}")

    logger.info(
        "Exported adjudication queue: %d papers to %s",
        len(all_flagged), output_path,
    )
    for cat, count in sorted(cat_counts.items()):
        logger.info("  %s: %d", cat, count)

    # Auto-advance workflow: QUEUE_EXPORTED
    complete_stage(
        review_db._conn, "QUEUE_EXPORTED",
        metadata=f"{len(all_flagged)} papers exported to {output_path}",
    )

    return {
        "total": len(all_flagged),
        "categories": cat_counts,
        "output_path": str(output_path),
    }


def _write_xlsx(papers: list[dict], output_path: Path, cat_counts: dict,
                category_config: CategoryConfig | None = None) -> None:
    """Write the adjudication queue as an Excel workbook."""
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.utils import get_column_letter

    wb = Workbook()

    # ── Sheet 1: Review Queue ──
    ws = wb.active
    ws.title = "Review Queue"

    headers = [
        "Row #",
        "Auto Category",
        "Title",
        "Abstract",
        "DOI",
        "PMID",
        "Year",
        "Journal",
        "Source",
        "Flagged By",
        "Primary Decision",
        "Primary Rationale",
        "Verifier Decision",
        "Verifier Rationale",
        "DECISION (INCLUDE/EXCLUDE)",
        "Notes (optional)",
    ]

    # Header styling
    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill(start_color="0A5E56", end_color="0A5E56", fill_type="solid")
    decision_fill = PatternFill(start_color="FFF2CC", end_color="FFF2CC", fill_type="solid")

    for col, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=header)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center", wrap_text=True)

    # Category color map
    cat_fills = {
        "ambiguous": PatternFill(start_color="FFE0E0", end_color="FFE0E0", fill_type="solid"),
        "cv_perception": PatternFill(start_color="E0F0FF", end_color="E0F0FF", fill_type="solid"),
        "review_editorial": PatternFill(start_color="E8E8E8", end_color="E8E8E8", fill_type="solid"),
        "hardware_sensing": PatternFill(start_color="FFF0E0", end_color="FFF0E0", fill_type="solid"),
        "planning_only": PatternFill(start_color="E0FFE0", end_color="E0FFE0", fill_type="solid"),
        "teleoperation_only": PatternFill(start_color="F0E0FF", end_color="F0E0FF", fill_type="solid"),
        "rehabilitation_prosthetics": PatternFill(start_color="FFE0F0", end_color="FFE0F0", fill_type="solid"),
        "industrial_nonmedical": PatternFill(start_color="E0FFFF", end_color="E0FFFF", fill_type="solid"),
    }

    wrap_align = Alignment(wrap_text=True, vertical="top")

    for i, paper in enumerate(papers, 1):
        row_num = i + 1
        cat = paper["auto_category"]

        ws.cell(row=row_num, column=1, value=i)
        ws.cell(row=row_num, column=2, value=cat)
        ws.cell(row=row_num, column=3, value=paper["title"])
        ws.cell(row=row_num, column=4, value=paper["abstract"][:2000] if paper["abstract"] else "")
        ws.cell(row=row_num, column=5, value=paper["doi"])
        ws.cell(row=row_num, column=6, value=paper["pmid"])
        ws.cell(row=row_num, column=7, value=paper.get("year", ""))
        ws.cell(row=row_num, column=8, value=paper["journal"])
        ws.cell(row=row_num, column=9, value=paper["data_source"])
        ws.cell(row=row_num, column=10, value=paper["flagged_by"])
        ws.cell(row=row_num, column=11, value=paper["primary_decision"])
        ws.cell(row=row_num, column=12, value=paper["primary_rationale"][:1000] if paper["primary_rationale"] else "")
        ws.cell(row=row_num, column=13, value=paper["verifier_decision"])
        ws.cell(row=row_num, column=14, value=paper["verifier_rationale"][:1000] if paper["verifier_rationale"] else "")

        # Decision column — highlighted for human input
        decision_cell = ws.cell(row=row_num, column=15, value="")
        decision_cell.fill = decision_fill

        ws.cell(row=row_num, column=16, value="")

        # Apply category coloring to the category cell
        cat_cell = ws.cell(row=row_num, column=2)
        if cat in cat_fills:
            cat_cell.fill = cat_fills[cat]

        # Wrap text for long fields
        for col in [3, 4, 12, 14]:
            ws.cell(row=row_num, column=col).alignment = wrap_align

    # Column widths
    col_widths = {
        1: 6, 2: 20, 3: 50, 4: 80, 5: 25, 6: 12,
        7: 6, 8: 30, 9: 10, 10: 20, 11: 15, 12: 50,
        13: 15, 14: 50, 15: 25, 16: 30,
    }
    for col, width in col_widths.items():
        ws.column_dimensions[get_column_letter(col)].width = width

    # Freeze header row
    ws.freeze_panes = "A2"

    # ── Sheet 2: Category Summary ──
    ws2 = wb.create_sheet("Category Summary")
    ws2.cell(row=1, column=1, value="Category").font = Font(bold=True)
    ws2.cell(row=1, column=2, value="Count").font = Font(bold=True)
    ws2.cell(row=1, column=3, value="Description").font = Font(bold=True)

    descs = get_category_descriptions(config=category_config)
    for i, (cat, count) in enumerate(sorted(cat_counts.items()), 2):
        ws2.cell(row=i, column=1, value=cat)
        ws2.cell(row=i, column=2, value=count)
        ws2.cell(row=i, column=3, value=descs.get(cat, ""))

    ws2.column_dimensions["A"].width = 25
    ws2.column_dimensions["B"].width = 10
    ws2.column_dimensions["C"].width = 60

    # ── Sheet 3: Instructions ──
    ws3 = wb.create_sheet("Instructions")
    instructions = [
        "SCREENING ADJUDICATION INSTRUCTIONS",
        "",
        "1. Review each paper in the 'Review Queue' sheet.",
        "2. For each paper, enter INCLUDE or EXCLUDE in the yellow 'DECISION' column (O).",
        "3. Optionally add notes in the 'Notes' column (P).",
        "",
        "SORTING STRATEGY:",
        "- 'ambiguous' papers are listed first — these need the most careful review.",
        "- Other categories are grouped together for batch decisions.",
        "- Papers in 'cv_perception', 'review_editorial', 'hardware_sensing' are likely excludes.",
        "- Papers in 'ambiguous' may go either way — read the abstract carefully.",
        "",
        "DECISION CRITERIA:",
        "- INCLUDE: The paper describes autonomous or semi-autonomous surgical robot execution.",
        "- EXCLUDE: The paper is about perception-only, planning-only, teleoperation-only,",
        "  reviews/editorials, hardware/sensors, rehabilitation, or non-medical robotics.",
        "",
        "After completing all decisions, save the file and run:",
        "  import_adjudication_decisions(review_db, 'path/to/this/file.xlsx')",
    ]
    for i, line in enumerate(instructions, 1):
        ws3.cell(row=i, column=1, value=line)
    ws3.column_dimensions["A"].width = 90

    wb.save(output_path)


# ── Import ─────────────────────────────────────────────────────────


def import_adjudication_decisions(
    review_db: ReviewDatabase,
    input_path: str | Path,
) -> dict:
    """Read completed adjudication Excel, write decisions to database.

    For papers in review.db (with paper_id), updates their status:
      INCLUDE → SCREENED_IN
      EXCLUDE → SCREENED_OUT

    For expanded search papers (no paper_id), records the decision in
    the screening_adjudication table for later pipeline use.

    Returns summary dict.
    """
    from openpyxl import load_workbook

    input_path = Path(input_path)
    wb = load_workbook(input_path)
    ws = wb["Review Queue"]

    ensure_adjudication_table(review_db._conn)

    now = datetime.now(timezone.utc).isoformat()
    stats = {"include": 0, "exclude": 0, "missing": 0, "invalid": 0, "total": 0}
    warnings = []

    for row in ws.iter_rows(min_row=2, values_only=False):
        # Skip empty rows
        title_val = row[2].value  # column C = title
        if not title_val:
            continue

        stats["total"] += 1

        row_num = row[0].value       # column A = Row #
        category = row[1].value      # column B = Auto Category
        title = row[2].value         # column C
        doi = row[4].value or ""     # column E
        pmid = row[5].value or ""    # column F
        decision_raw = row[14].value  # column O = DECISION
        notes = row[15].value or ""   # column P = Notes

        if not decision_raw:
            stats["missing"] += 1
            warnings.append(f"Row {row_num}: '{title[:60]}...' — no decision")
            continue

        decision = decision_raw.strip().upper()
        if decision not in ("INCLUDE", "EXCLUDE"):
            stats["invalid"] += 1
            warnings.append(
                f"Row {row_num}: invalid decision '{decision_raw}' "
                f"(must be INCLUDE or EXCLUDE)"
            )
            continue

        # Determine external key
        ext_key = doi or pmid or title

        # Write to adjudication table
        review_db._conn.execute(
            """INSERT INTO screening_adjudication
               (paper_id, external_key, title, adjudication_decision,
                adjudication_source, adjudication_reason,
                adjudication_category, adjudication_timestamp, created_at)
               VALUES (?, ?, ?, ?, 'human', ?, ?, ?, ?)""",
            (
                None,  # paper_id filled below if in DB
                ext_key,
                title,
                decision,
                notes,
                category,
                now,
                now,
            ),
        )

        # Try to find and update paper in review.db
        paper_row = None
        if pmid:
            paper_row = review_db._conn.execute(
                "SELECT id, status FROM papers WHERE pmid = ?", (str(pmid),)
            ).fetchone()
        if not paper_row and doi:
            paper_row = review_db._conn.execute(
                "SELECT id, status FROM papers WHERE doi = ?", (doi,)
            ).fetchone()

        if paper_row and paper_row["status"] == "SCREEN_FLAGGED":
            pid = paper_row["id"]
            new_status = "SCREENED_IN" if decision == "INCLUDE" else "SCREENED_OUT"
            review_db.update_status(pid, new_status)

            # Update adjudication record with paper_id
            review_db._conn.execute(
                """UPDATE screening_adjudication
                   SET paper_id = ?
                   WHERE external_key = ? AND adjudication_timestamp = ?""",
                (pid, ext_key, now),
            )

        if decision == "INCLUDE":
            stats["include"] += 1
        else:
            stats["exclude"] += 1

    review_db._conn.commit()

    if warnings:
        for w in warnings[:10]:
            logger.warning(w)
        if len(warnings) > 10:
            logger.warning("... and %d more warnings", len(warnings) - 10)

    logger.info(
        "Import complete: %d include, %d exclude, %d missing, %d invalid (of %d total)",
        stats["include"], stats["exclude"], stats["missing"],
        stats["invalid"], stats["total"],
    )

    # Auto-advance workflow: ADJUDICATION_COMPLETE if zero unresolved
    if stats["missing"] == 0 and stats["invalid"] == 0:
        complete_stage(
            review_db._conn, "ADJUDICATION_COMPLETE",
            metadata=(
                f"{stats['include']} included, {stats['exclude']} excluded "
                f"(of {stats['total']} total)"
            ),
        )
    else:
        logger.warning(
            "Adjudication not complete: %d missing + %d invalid decisions remain",
            stats["missing"], stats["invalid"],
        )

    return {"stats": stats, "warnings": warnings}


# ── Pipeline Gate ──────────────────────────────────────────────────


def check_adjudication_gate(review_db: ReviewDatabase) -> int:
    """Check for unresolved SCREEN_FLAGGED papers. Returns count."""
    flagged = review_db.get_papers_by_status("SCREEN_FLAGGED")
    return len(flagged)
