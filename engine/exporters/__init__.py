"""Export convenience function."""

import logging
from pathlib import Path

from engine.core.database import ReviewDatabase
from engine.core.review_spec import ReviewSpec
from engine.exporters.docx_export import export_evidence_docx
from engine.exporters.evidence_table import export_evidence_csv, export_evidence_excel
from engine.exporters.methods_section import export_methods_md
from engine.exporters.prisma import export_prisma_csv

logger = logging.getLogger(__name__)


def export_all(
    db: ReviewDatabase,
    spec: ReviewSpec,
    review_name: str,
    output_dir: str | None = None,
    min_status: str = "AI_AUDIT_COMPLETE",
) -> dict:
    """Run all exports and return dict of file paths created."""
    if output_dir is None:
        output_dir = str(Path(db.db_path).parent / "exports")

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    paths = {}

    prisma_path = str(out / "prisma_flow.csv")
    export_prisma_csv(db, prisma_path)
    paths["prisma_csv"] = prisma_path

    evidence_csv_path = str(out / "evidence_table.csv")
    export_evidence_csv(db, spec, evidence_csv_path, min_status=min_status)
    paths["evidence_csv"] = evidence_csv_path

    evidence_xlsx_path = str(out / "evidence_table.xlsx")
    export_evidence_excel(db, spec, evidence_xlsx_path, min_status=min_status)
    paths["evidence_xlsx"] = evidence_xlsx_path

    docx_path = str(out / "evidence_table.docx")
    export_evidence_docx(db, spec, docx_path, min_status=min_status)
    paths["evidence_docx"] = docx_path

    methods_path = str(out / "methods_section.md")
    export_methods_md(db, spec, methods_path)
    paths["methods_md"] = methods_path

    # The trace exports were RETIRED, not migrated (R46, READERS-01 Phase 2b).
    #
    # `trace_exporter.py` reported on `extractions.reasoning_trace` and on
    # `evidence_spans.audit_status` / `.confidence` / `.audit_rationale`, and not
    # one of those has a counterpart in the event store — so "migrating" it would
    # have left the file standing while every number it produced went empty. It
    # served the reading of Run 6, not the engine going forward, which is R31's
    # test. Trace quality (truncation detection, length distribution, flagged
    # papers) is rebuilt over TRACE EVENTS at session 9 (S5d); the prior design
    # is recoverable at 443e3d8968bf5bcee9679102dcb798bf4f40bdcf:engine/exporters/trace_exporter.py, recorded
    # in the plan's retention ledger.

    logger.info("All exports written to %s", output_dir)
    return paths
