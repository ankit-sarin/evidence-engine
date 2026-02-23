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
    export_evidence_csv(db, spec, evidence_csv_path)
    paths["evidence_csv"] = evidence_csv_path

    evidence_xlsx_path = str(out / "evidence_table.xlsx")
    export_evidence_excel(db, spec, evidence_xlsx_path)
    paths["evidence_xlsx"] = evidence_xlsx_path

    docx_path = str(out / "evidence_table.docx")
    export_evidence_docx(db, spec, docx_path)
    paths["evidence_docx"] = docx_path

    methods_path = str(out / "methods_section.md")
    export_methods_md(db, spec, methods_path)
    paths["methods_md"] = methods_path

    logger.info("All exports written to %s", output_dir)
    return paths
