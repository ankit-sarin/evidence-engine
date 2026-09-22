"""Evidence table exports: CSV and Excel."""

import csv
import json
import logging
import os
from pathlib import Path

import openpyxl

from engine.core.database import ReviewDatabase
from engine.core.review_spec import ReviewSpec
from engine.core.codebook import load_codebook_beside

logger = logging.getLogger(__name__)

NO_EXTRACTION_MARKER = "[NO EXTRACTION DATA]"


# ── Helpers ──────────────────────────────────────────────────────────


def _build_evidence_rows(
    db: ReviewDatabase, spec: ReviewSpec,
    min_status: str = "AI_AUDIT_COMPLETE",
    exclude_empty: bool = False,
    arm: str = "local",
) -> tuple[list[str], list[list]]:
    """Build header and data rows for the evidence table, THROUGH THE READER.

    READERS-01 Phase 2a (R30, R33, R36, R18/Q4, A1).

    What was here was `"SELECT id FROM extractions WHERE paper_id = ? ORDER BY id
    DESC LIMIT 1"` and that extraction's spans — one of three copies of a
    resolution rule in this repository, and the "exporter (latest extraction)"
    half of A1's first disagreeing reader pair. It is gone, not kept beside the
    reader (R30). Resolution is rule v2.1, and the numbered row that produced
    each value is exported beside it.

    **`arm` is now a parameter (R18/Q4)** and defaults to `"local"`, which is
    what every caller silently meant.

    **Columns changed (R36).** Per field the export emitted
    `value, snippet, confidence, audit_status`. `confidence` was a per-span
    scalar the event store does not model, and carrying it forward as NULL would
    publish a column that means nothing — the shape of C7. It is dropped, and
    two columns that carry more replace it: `{field}_state`, the reader's field
    state, and `{field}_rule_row`, the v2.1 row that produced it. A disagreement
    about a value is then a disagreement about a numbered row.

    Args:
        min_status: kept for call compatibility and **no longer selects papers**.
            The paper set is the corpus — the ELIGIBILITY axis of
            `effective_state` (S3h) — and `HUMAN_AUDIT_COMPLETE` as a
            paper-level gate is a per-FIELD question under the event model, which
            the `{field}_state` column now answers per cell.
        exclude_empty: omit papers for which no field carries a value.
        arm: which arm to export. Required knowledge, defaulted for compatibility.
    """
    from engine.core.effective import (
        effective_state, eligible_paper_ids, iter_grid, registered_arms,
    )

    codebook = load_codebook_beside(db.db_path)
    field_names = list(codebook.field_names)

    headers = ["paper_id", "pmid", "doi", "title", "authors", "year", "journal",
               "eligibility", "processing", "processing_reason", "analysis_ready"]
    for fname in field_names:
        headers.extend([fname, f"{fname}_snippet", f"{fname}_state",
                        f"{fname}_rule_row"])

    conn = db._conn
    if arm not in registered_arms(conn, include_retired=True):
        from engine.core.effective import UnknownArm
        raise UnknownArm(
            f"arm {arm!r} is not in this review's registry — registered arms are "
            f"{registered_arms(conn, include_retired=True)}."
        )

    paper_ids = eligible_paper_ids(conn)
    if not paper_ids:
        return headers, []

    cells: dict[int, dict[str, object]] = {}
    for paper_id, field_name, _arm, ev in iter_grid(
            conn, codebook=codebook, papers=paper_ids, arms=(arm,)):
        cells.setdefault(paper_id, {})[field_name] = ev

    marks = ", ".join("?" * len(paper_ids))
    papers = conn.execute(
        f"SELECT * FROM papers WHERE id IN ({marks}) ORDER BY id", paper_ids
    ).fetchall()

    rows = []
    for paper in papers:
        pid = paper["id"]
        state = effective_state(conn, pid)
        by_field = cells.get(pid, {})
        # "Has data" is a cell the store has a RECORD for — not a cell with a
        # non-null value. A paper whose every field was withdrawn or declined has
        # data, and saying [NO EXTRACTION DATA] about it would throw away exactly
        # the states R1 and the field-state work exist to make visible. The
        # marker now means what it says: nothing was ever recorded for this arm.
        has_data = any(ev.state != "missing" for ev in by_field.values())

        if not has_data and exclude_empty:
            logger.warning("Paper %d has no value on any field for arm %s", pid, arm)
            continue

        row = [pid, paper["pmid"], paper["doi"], paper["title"], paper["authors"],
               paper["year"], paper["journal"],
               state.eligibility, state.processing, state.processing_reason,
               state.analysis_ready]

        if not has_data:
            logger.warning("Paper %d has no value on any field for arm %s", pid, arm)
            row.extend([NO_EXTRACTION_MARKER, "", "", ""])
            for _ in field_names[1:]:
                row.extend(["", "", "", ""])
        else:
            for fname in field_names:
                ev = by_field.get(fname)
                if ev is None:
                    row.extend(["", "", "", ""])
                else:
                    row.extend([
                        ev.value if ev.value is not None else "",
                        (ev.provenance.get("located") or {}).get("snippet", "")
                        if isinstance(ev.provenance.get("located"), dict) else "",
                        ev.state,
                        ev.rule_row,
                    ])

        rows.append(row)

    return headers, rows


# ── CSV Export ───────────────────────────────────────────────────────


def export_evidence_csv(
    db: ReviewDatabase, spec: ReviewSpec, output_path: str,
    min_status: str = "AI_AUDIT_COMPLETE",
    exclude_empty: bool = False,
    arm: str = "local",
) -> None:
    """Export evidence table as CSV. `arm` per R18/Q4."""
    headers, rows = _build_evidence_rows(db, spec, min_status=min_status,
                                          exclude_empty=exclude_empty, arm=arm)

    tmp_path = output_path + ".tmp"
    try:
        with open(tmp_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)
        os.replace(tmp_path, output_path)
    except BaseException:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise

    logger.info("Evidence CSV exported to %s (%d rows)", output_path, len(rows))


# ── Excel Export ─────────────────────────────────────────────────────


def export_evidence_excel(
    db: ReviewDatabase, spec: ReviewSpec, output_path: str,
    min_status: str = "AI_AUDIT_COMPLETE",
    exclude_empty: bool = False,
    arm: str = "local",
) -> None:
    """Export evidence table as Excel with 3 sheets. `arm` per R18/Q4."""
    from engine.core.effective import eligible_paper_ids

    codebook = load_codebook_beside(db.db_path)
    paper_ids = eligible_paper_ids(db._conn)
    wb = openpyxl.Workbook()

    # Sheet 1: Evidence Table
    ws1 = wb.active
    ws1.title = "Evidence Table"
    headers, rows = _build_evidence_rows(db, spec, min_status=min_status,
                                          exclude_empty=exclude_empty, arm=arm)
    ws1.append(headers)
    for row in rows:
        ws1.append(row)
    _style_header(ws1)

    # Sheet 2: Screening Log
    ws2 = wb.create_sheet("Screening Log")
    ws2.append(["paper_id", "title", "pass_number", "decision", "rationale", "model", "decided_at"])
    screen_rows = db._conn.execute(
        """SELECT p.id, p.title, sd.pass_number, sd.decision,
                  sd.rationale, sd.model, sd.decided_at
           FROM abstract_screening_decisions sd
           JOIN papers p ON p.id = sd.paper_id
           ORDER BY p.id, sd.pass_number"""
    ).fetchall()
    for r in screen_rows:
        ws2.append(list(dict(r).values()))
    _style_header(ws2)

    # Sheet 3: Field states.
    #
    # This sheet used to be an "Audit Log" read straight off `evidence_spans`
    # joined to every extraction with NO latest-extraction filter — so ONE FILE
    # DISAGREED WITH ITSELF: sheet 1 showed the newest extraction's value and
    # sheet 3 showed every value the paper ever had, side by side in one
    # workbook. That is A1 inside a single exporter, and R30 removes it rather
    # than keeping it beside the reader.
    #
    # What replaces it answers the question the audit log was read for — "which
    # values are backed by evidence, and which are not" — from the reader, per
    # cell, with the numbered v2.1 row that decided it.
    ws3 = wb.create_sheet("Field States")
    ws3.append(["paper_id", "title", "arm", "field_name", "value",
                "state", "rule_row", "located"])
    from engine.core.effective import iter_grid
    titles = {r[0]: r[1] for r in db._conn.execute("SELECT id, title FROM papers")}
    for paper_id, field_name, arm_name, ev in iter_grid(
            db._conn, codebook=codebook, papers=paper_ids, arms=(arm,)):
        located = ev.provenance.get("located")
        ws3.append([
            paper_id, titles.get(paper_id), arm_name, field_name,
            ev.value, ev.state, ev.rule_row,
            bool(located.get("located")) if isinstance(located, dict) else False,
        ])
    _style_header(ws3)

    tmp_path = output_path + ".tmp"
    try:
        wb.save(tmp_path)
        os.replace(tmp_path, output_path)
    except BaseException:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise

    logger.info("Evidence Excel exported to %s", output_path)


def _style_header(ws) -> None:
    """Bold the header row."""
    from openpyxl.styles import Font
    for cell in ws[1]:
        cell.font = Font(bold=True)
