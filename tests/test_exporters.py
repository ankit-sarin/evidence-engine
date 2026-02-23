"""Tests for export modules: PRISMA, CSV, Excel, DOCX, methods section."""

import csv
import json
from pathlib import Path

import openpyxl
import pytest

from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec
from engine.exporters import export_all
from engine.exporters.docx_export import export_evidence_docx
from engine.exporters.evidence_table import export_evidence_csv, export_evidence_excel
from engine.exporters.methods_section import generate_methods_section, export_methods_md
from engine.exporters.prisma import generate_prisma_flow, export_prisma_csv
from engine.search.models import Citation

SPEC_PATH = Path(__file__).resolve().parent.parent / "review_specs" / "surgical_autonomy_v1.yaml"


@pytest.fixture(scope="module")
def spec():
    return load_review_spec(SPEC_PATH)


@pytest.fixture()
def populated_db(tmp_path, spec):
    """Create a DB with papers at various pipeline stages and extraction data."""
    db = ReviewDatabase("test_export", data_root=tmp_path)

    # 10 PubMed + 5 OpenAlex papers
    pm_cits = [
        Citation(title=f"PubMed Study {i}", source="pubmed", pmid=str(i),
                 doi=f"10.1/{i}", authors=["Smith A", "Jones B"],
                 journal="J Surg Robot", year=2023)
        for i in range(1, 11)
    ]
    oa_cits = [
        Citation(title=f"OpenAlex Study {i}", source="openalex", pmid=str(100 + i),
                 doi=f"10.2/{i}", authors=["Lee C"], journal="Robot Rev", year=2024)
        for i in range(1, 6)
    ]
    db.add_papers(pm_cits + oa_cits)

    papers = db.get_papers_by_status("INGESTED")

    # Screen 8 in, 4 out, 3 flagged
    for p in papers[:8]:
        db.add_screening_decision(p["id"], 1, "include", "Relevant", "qwen3:8b")
        db.add_screening_decision(p["id"], 2, "include", "Confirmed", "qwen3:8b")
        db.update_status(p["id"], "SCREENED_IN")

    for p in papers[8:12]:
        db.add_screening_decision(p["id"], 1, "exclude", "Not surgical", "qwen3:8b")
        db.add_screening_decision(p["id"], 2, "exclude", "Confirmed exclude", "qwen3:8b")
        db.update_status(p["id"], "SCREENED_OUT")

    for p in papers[12:15]:
        db.add_screening_decision(p["id"], 1, "include", "Maybe relevant", "qwen3:8b")
        db.add_screening_decision(p["id"], 2, "exclude", "Borderline", "qwen3:8b")
        db.update_status(p["id"], "SCREEN_FLAGGED")

    # Walk 5 screened-in papers to EXTRACTED/AUDITED
    screened_in = db.get_papers_by_status("SCREENED_IN")
    schema_hash = spec.extraction_hash()

    for j, p in enumerate(screened_in[:5]):
        pid = p["id"]
        db.update_status(pid, "PDF_ACQUIRED")
        db.update_status(pid, "PARSED")
        db.update_status(pid, "EXTRACTED")

        ext_id = db.add_extraction(
            pid, schema_hash,
            {"study_design": "RCT", "sample_size": "20"},
            "reasoning trace here", "deepseek-r1:32b",
        )

        # Add evidence spans for a few fields
        db.add_evidence_span(ext_id, "study_design", "RCT", "An RCT was performed.", 0.95)
        db.add_evidence_span(ext_id, "sample_size", "20", "Twenty trials.", 0.9)
        db.add_evidence_span(ext_id, "robot_platform", "STAR", "The STAR robot.", 0.85)

        # Audit 3 papers
        if j < 3:
            spans = db._conn.execute(
                "SELECT id FROM evidence_spans WHERE extraction_id = ?", (ext_id,)
            ).fetchall()
            for s in spans:
                db.update_audit(s["id"], "verified", "qwen3:32b", "Confirmed.")
            db.update_status(pid, "AUDITED")

    yield db
    db.close()


# ── PRISMA Flow ──────────────────────────────────────────────────────


def test_prisma_flow_counts(populated_db):
    flow = generate_prisma_flow(populated_db)
    assert flow["records_identified"] == 15
    assert flow["records_by_source"]["pubmed"] == 10
    assert flow["records_by_source"]["openalex"] == 5
    assert flow["records_excluded"] == 4
    assert flow["screen_flagged"] == 3
    assert flow["studies_included"] >= 3  # EXTRACTED + AUDITED


def test_prisma_csv(populated_db, tmp_path):
    out = str(tmp_path / "prisma.csv")
    export_prisma_csv(populated_db, out)
    assert Path(out).exists()

    with open(out) as f:
        reader = csv.reader(f)
        rows = list(reader)
    # Header + data rows
    assert len(rows) > 5
    assert rows[0] == ["Stage", "Count", "Detail"]


# ── Evidence CSV ─────────────────────────────────────────────────────


def test_evidence_csv_columns(populated_db, spec, tmp_path):
    out = str(tmp_path / "evidence.csv")
    export_evidence_csv(populated_db, spec, out)
    assert Path(out).exists()

    with open(out) as f:
        reader = csv.reader(f)
        rows = list(reader)

    headers = rows[0]
    # Base columns present
    assert "paper_id" in headers
    assert "pmid" in headers
    assert "title" in headers

    # Extraction field columns present
    assert "study_design" in headers
    assert "study_design_snippet" in headers
    assert "study_design_confidence" in headers
    assert "study_design_audit" in headers

    # 5 included papers (EXTRACTED + AUDITED)
    assert len(rows) - 1 == 5


# ── Evidence Excel ───────────────────────────────────────────────────


def test_evidence_excel_sheets(populated_db, spec, tmp_path):
    out = str(tmp_path / "evidence.xlsx")
    export_evidence_excel(populated_db, spec, out)
    assert Path(out).exists()

    wb = openpyxl.load_workbook(out)
    assert wb.sheetnames == ["Evidence Table", "Screening Log", "Audit Log"]

    # Evidence Table has header + data rows
    ws1 = wb["Evidence Table"]
    assert ws1.max_row >= 6  # 1 header + 5 papers

    # Screening Log has entries
    ws2 = wb["Screening Log"]
    assert ws2.max_row > 1

    # Audit Log has entries
    ws3 = wb["Audit Log"]
    assert ws3.max_row > 1
    wb.close()


# ── DOCX ─────────────────────────────────────────────────────────────


def test_docx_created(populated_db, spec, tmp_path):
    out = str(tmp_path / "evidence.docx")
    export_evidence_docx(populated_db, spec, out)
    assert Path(out).exists()

    # Verify it's a valid docx by loading it
    from docx import Document
    doc = Document(out)
    # Should have at least one table
    assert len(doc.tables) >= 1
    # Table should have header + data rows
    table = doc.tables[0]
    assert len(table.rows) >= 6  # 1 header + 5 papers


# ── Methods Section ──────────────────────────────────────────────────


def test_methods_section_content(populated_db, spec):
    methods = generate_methods_section(populated_db, spec)

    # Key pipeline details present
    assert "PubMed" in methods
    assert "OpenAlex" in methods
    assert "qwen3:8b" in methods
    assert "deepseek-r1:32b" in methods
    assert "qwen3:32b" in methods
    assert "dual-pass" in methods
    assert "two-pass" in methods
    assert "15" in methods  # total records


def test_methods_md_export(populated_db, spec, tmp_path):
    out = str(tmp_path / "methods.md")
    export_methods_md(populated_db, spec, out)
    assert Path(out).exists()

    content = Path(out).read_text()
    assert content.startswith("# Methods")
    assert "systematic search" in content


# ── export_all ───────────────────────────────────────────────────────


def test_export_all(populated_db, spec, tmp_path):
    out_dir = str(tmp_path / "all_exports")
    paths = export_all(populated_db, spec, "test_export", output_dir=out_dir)

    expected_keys = {"prisma_csv", "evidence_csv", "evidence_xlsx", "evidence_docx", "methods_md"}
    assert set(paths.keys()) == expected_keys

    for key, path in paths.items():
        assert Path(path).exists(), f"{key} not found at {path}"
