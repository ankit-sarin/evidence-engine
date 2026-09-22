"""DOCX formatted evidence table for journal submission."""

import json
import logging
import os

from docx import Document
from docx.enum.section import WD_ORIENT
from docx.shared import Inches, Pt

from engine.core.database import ReviewDatabase
from engine.core.review_spec import ReviewSpec
from engine.core.codebook import load_codebook_beside

logger = logging.getLogger(__name__)

#: Processing tokens that mean the machinery did not complete (R39). Imported,
#: never re-spelled: the vocabulary has one home.
from engine.core.paper_state import FAILURE_STATES as _FAILURE_TOKENS  # noqa: E402

#: Reader states that are a CLAIM about the extraction rather than an absence of
#: one. They render as a marker so a submission table does not print an empty
#: cell where the record says the model abstained or the engine refused.
#: `withdrawn` is deliberately NOT here: R1 makes a withdrawal "no value in the
#: current result", and an empty cell is what no value looks like.
_MARKED_STATES = {
    "declined": "[declined]",
    "contract unmet": "[contract unmet]",
    "unresolved (needs re-review)": "[unresolved]",
    "unresolved (duplicate values)": "[unresolved: duplicate]",
}


def _cell_text(ev) -> str:
    """One table cell from one `EffectiveValue` (or None for a cell not enumerated)."""
    if ev is None:
        return ""
    if ev.value is not None:
        return ev.value
    return _MARKED_STATES.get(ev.state, "")


def export_evidence_docx(
    db: ReviewDatabase, spec: ReviewSpec, output_path: str,
    min_status: str = "AI_AUDIT_COMPLETE",
    arm: str = "local",
) -> None:
    """Export a professional evidence table as DOCX, THROUGH THE READER.

    READERS-01 Phase 2b (R33 as amended, R30, R18/Q4, A1). What was here was
    `"SELECT id FROM extractions WHERE paper_id = ? ORDER BY id DESC LIMIT 1"`
    and that extraction's spans — the second of three copies of one resolution
    rule in this repository. It is removed, not kept beside the reader.

    **`arm` is a parameter now** (R18/Q4), defaulted to `"local"`, which is what
    every caller silently meant.

    **A non-value state is shown, not blanked.** R1 makes REJECT a withdrawal —
    "no value in the current result" — so a withdrawn field renders EMPTY, the
    same as a field nobody extracted. A *declined* field is a different fact: the
    model was asked and abstained, and printing an empty cell for it would make
    a submission table say "not reported" where the record says "declined". The
    non-value states that are claims about the extraction therefore render as a
    bracketed marker; `missing` and `out of scope` render empty.

    **`min_status` no longer selects papers.** The set is the corpus — the
    eligibility axis of `effective_state` (S3h) — so a paper whose extraction
    failed is not silently absent. It cannot be shown as evidence either, so
    S3h's other half is honoured by a note under the table reporting the failed
    count by reason rather than by dropping the rows without saying so.
    """
    from engine.core.effective import (
        UnknownArm, effective_state, eligible_paper_ids, iter_grid,
        registered_arms,
    )

    doc = Document()

    # Page setup: landscape, narrow margins
    section = doc.sections[0]
    section.orientation = WD_ORIENT.LANDSCAPE
    section.page_width, section.page_height = section.page_height, section.page_width
    section.left_margin = Inches(0.5)
    section.right_margin = Inches(0.5)
    section.top_margin = Inches(0.5)
    section.bottom_margin = Inches(0.5)

    # Title
    title_para = doc.add_paragraph()
    run = title_para.add_run(spec.title)
    run.bold = True
    run.font.size = Pt(14)
    title_para.add_run(f"\nVersion {spec.version} — {spec.date}")

    doc.add_paragraph("")  # spacer

    codebook = load_codebook_beside(db.db_path)
    field_names = list(codebook.field_names)
    base_cols = ["Study", "Year", "Journal"]
    all_cols = base_cols + field_names

    conn = db._conn
    if arm not in registered_arms(conn, include_retired=True):
        raise UnknownArm(
            f"arm {arm!r} is not in this review's registry — registered arms are "
            f"{registered_arms(conn, include_retired=True)}."
        )

    paper_ids = eligible_paper_ids(conn)

    cells: dict[int, dict[str, object]] = {}
    for paper_id, field_name, _arm, ev in iter_grid(
            conn, codebook=codebook, papers=paper_ids, arms=(arm,)):
        cells.setdefault(paper_id, {})[field_name] = ev

    papers = []
    if paper_ids:
        marks = ", ".join("?" * len(paper_ids))
        papers = conn.execute(
            f"SELECT * FROM papers WHERE id IN ({marks}) ORDER BY id", paper_ids
        ).fetchall()

    table = doc.add_table(rows=1 + len(papers), cols=len(all_cols))
    table.style = "Table Grid"

    # Header row
    for i, col_name in enumerate(all_cols):
        cell = table.rows[0].cells[i]
        cell.text = col_name.replace("_", " ").title()
        for paragraph in cell.paragraphs:
            for run in paragraph.runs:
                run.bold = True
                run.font.size = Pt(9)

    failures: dict[str, int] = {}

    # Data rows
    for row_idx, paper in enumerate(papers, 1):
        pid = paper["id"]
        by_field = cells.get(pid, {})

        state = effective_state(conn, pid)
        if state.processing in _FAILURE_TOKENS:
            reason = state.processing_reason or state.processing
            failures[reason] = failures.get(reason, 0) + 1

        # Authors: first author et al.
        authors_raw = paper["authors"] or "[]"
        try:
            authors = json.loads(authors_raw)
        except (json.JSONDecodeError, TypeError):
            authors = []
        if authors:
            study_label = f"{authors[0].split()[-1]} et al." if len(authors) > 1 else authors[0]
        else:
            study_label = paper["title"][:40]

        base_values = [study_label, str(paper["year"] or ""), paper["journal"] or ""]
        field_values = [_cell_text(by_field.get(f)) for f in field_names]
        all_values = base_values + field_values

        for col_idx, val in enumerate(all_values):
            cell = table.rows[row_idx].cells[col_idx]
            cell.text = str(val) if val else ""
            for paragraph in cell.paragraphs:
                for run in paragraph.runs:
                    run.font.size = Pt(9)

    if failures:
        doc.add_paragraph("")
        note = doc.add_paragraph()
        note_run = note.add_run(
            "Processing outcomes: "
            + "; ".join(f"{n} paper(s) — {reason}"
                        for reason, n in sorted(failures.items()))
            + ". These papers are eligible for the review and are listed above; "
              "the machinery did not complete for them."
        )
        note_run.font.size = Pt(8)
        note_run.italic = True

    tmp_path = output_path + ".tmp"
    try:
        doc.save(tmp_path)
        os.replace(tmp_path, output_path)
    except BaseException:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise

    logger.info("Evidence DOCX exported to %s (%d studies, arm %s)",
                output_path, len(papers), arm)
