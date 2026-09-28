"""Tests for export modules: PRISMA, CSV, Excel, DOCX, methods section."""

import csv
import json
import os
from pathlib import Path
from unittest.mock import patch

import openpyxl
import pytest

from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec
from engine.exporters import export_all
from engine.exporters.docx_export import export_evidence_docx
from engine.exporters.evidence_table import (
    NO_EXTRACTION_MARKER,
    export_evidence_csv,
    export_evidence_excel,
)
from engine.exporters.methods_section import generate_methods_section, export_methods_md
from engine.exporters.prisma import generate_prisma_flow, export_prisma_csv
from engine.search.models import Citation
from engine.core.codebook import load_codebook_beside
from tests._event_store_fixture import add_values, open_extraction_run, seed_prisma_world

SPEC_PATH = Path(__file__).resolve().parent.parent / "review_specs" / "surgical_autonomy.yaml"


def _real_codebook(db):
    """Exporters read the codebook beside the database (SCHEMA-DERIVE-01).

    These tests assert against the real 20-field schema, so the real codebook
    belongs in their review directory — conftest's one-field placeholder
    describes a different review.
    """
    import shutil

    shutil.copy2(
        Path(__file__).resolve().parent.parent
        / "data" / "surgical_autonomy" / "extraction_codebook.yaml",
        Path(db.db_path).parent / "extraction_codebook.yaml",
    )


@pytest.fixture(scope="module")
def spec():
    return load_review_spec(SPEC_PATH)


def _toolkit_world(tmp_path, spec, name, *, citations):
    """A review on the event toolkit (9e-C-P2, R190): papers, a real extraction
    run pinning the spec's extraction arm, and both PRISMA sides declared."""
    db = ReviewDatabase(name, data_root=tmp_path)
    _real_codebook(db)
    db.add_papers(citations)
    open_extraction_run(db, spec)
    return db


@pytest.fixture()
def exporter_db(tmp_path, spec):
    """exporter_db's world on the toolkit (9e-C-P2 B1, R190).

    15 papers (10 PubMed, 5 OpenAlex); 4 screened out, 3 flagged, 3 screened in
    and not yet acquired; 5 eligible. The 3 papers the old fixture audited are
    `audited_ai` with located claims; the 2 it only extracted are `extracted`
    with claims and no `citation_located`. Claims are on the spec's extraction
    arm, which `open_extraction_run` pins.
    """
    db = _toolkit_world(
        tmp_path, spec, "test_export",
        citations=[Citation(title=f"PubMed Study {i}", source="pubmed", pmid=str(i),
                            doi=f"10.1/{i}", authors=["Smith A", "Jones B"],
                            journal="J Surg Robot", year=2023) for i in range(1, 11)]
        + [Citation(title=f"OpenAlex Study {i}", source="openalex", pmid=str(100 + i),
                    doi=f"10.2/{i}", authors=["Lee C"], journal="Robot Rev", year=2024)
           for i in range(1, 6)])
    world = seed_prisma_world(
        db,
        screening=[("ABSTRACT_SCREENED_OUT", None)] * 4
        + [("ABSTRACT_SCREEN_FLAGGED", None)] * 3
        + [("ABSTRACT_SCREENED_IN", None)] * 3,
        eligible=[("audited_ai", None)] * 3 + [("extracted", None)] * 2,
    )
    # Screening decisions for the Screening Log sheet, through the screening
    # writer; retires at the screeners' cut-over (R163 precedent).
    for pid in world["screening"]:
        db.add_screening_decision(pid, 1, "include", "Relevant", "qwen3:8b")
        db.add_screening_decision(pid, 2, "include", "Confirmed", "qwen3:8b")
    arm = spec.extraction_models.arm
    audited, extracted = world["eligible"][:3], world["eligible"][3:]
    for pids, located in ((audited, True), (extracted, False)):
        for field, value in (("study_design", "RCT"), ("sample_size", "20"),
                             ("robot_platform", "STAR")):
            add_values(db.db_path, arm, field, [value] * len(pids),
                       start_paper=pids[0], located=located)
    yield db
    db.close()


# ── PRISMA Flow ──────────────────────────────────────────────────────


@pytest.fixture()
def prisma_db(tmp_path):
    """9e-C-P1 (R190, R193): both PRISMA sides declared — screening tokens on
    `papers.status`, the corpus on the event store. 10 PubMed + 5 OpenAlex."""
    db = ReviewDatabase("test_prisma_export", data_root=tmp_path)
    db.add_papers(
        [Citation(title=f"PubMed Study {i}", source="pubmed", pmid=str(i))
         for i in range(1, 11)]
        + [Citation(title=f"OpenAlex Study {i}", source="openalex", pmid=str(100 + i))
           for i in range(1, 6)])
    seed_prisma_world(
        db,
        screening=[("ABSTRACT_SCREENED_OUT", None)] * 4
        + [("ABSTRACT_SCREEN_FLAGGED", None)] * 3
        + [("ABSTRACT_SCREENED_IN", None)] * 3,
        eligible=[("audited_ai", None)] * 3 + [("extracted", None)] * 2,
    )
    yield db
    db.close()


def test_prisma_flow_counts(prisma_db):
    flow = generate_prisma_flow(prisma_db)
    assert flow["records_identified"] == 15
    assert flow["records_by_source"]["pubmed"] == 10
    assert flow["records_by_source"]["openalex"] == 5
    assert flow["records_excluded"] == 4
    assert flow["screen_flagged"] == 3
    assert flow["studies_included"] == 3  # eligible and audited_ai on events
    assert flow["extraction_in_progress"] == 2


def test_prisma_csv(prisma_db, tmp_path):
    out = str(tmp_path / "prisma.csv")
    export_prisma_csv(prisma_db, out)
    assert Path(out).exists()

    with open(out) as f:
        reader = csv.reader(f)
        rows = list(reader)
    # Header + data rows
    assert len(rows) > 5
    assert rows[0] == ["Stage", "Count", "Detail"]
    labels = {r[0]: r[1] for r in rows if r[0]}
    assert labels["Screening in progress"] == "6"  # 3 flagged + 3 screened in
    assert labels["Extraction in progress"] == "2"
    assert labels["Studies included"] == "3"


# ── Evidence CSV ─────────────────────────────────────────────────────


def test_evidence_csv_columns(exporter_db, spec, tmp_path):
    out = str(tmp_path / "evidence.csv")
    export_evidence_csv(exporter_db, spec, out, arm=spec.extraction_models.arm)
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
    # B5 (R36): `{field}_confidence` is gone. It was a per-span scalar the event
    # store does not model, and carrying it forward as NULL would publish a
    # column that means nothing — the shape of C7. Two columns that carry more
    # replace it.
    assert "study_design_confidence" not in headers
    assert "study_design_state" in headers
    assert "study_design_rule_row" in headers
    # R29/R39: the paper's two axes are columns of their own.
    for col in ("eligibility", "processing", "processing_reason", "analysis_ready"):
        assert col in headers
    # `{field}_audit` carried `evidence_spans.audit_status`. Auditor verdicts are
    # PROVENANCE, not a field state (R18/Q7), and the field's state is what the
    # export now carries — asserted with/without evidence, declined, withdrawn,
    # corrected by human, or one of the two unresolved rows.
    assert "study_design_audit" not in headers

    # B5 (S3h/A9). The count was "3 papers at AI_AUDIT_COMPLETE", a
    # `papers.status` gate. The paper set is now the corpus — the ELIGIBILITY
    # axis — so it is asserted as a DERIVATION against the reader rather than as
    # a literal that decays the first time the fixture gains a paper.
    from engine.core.effective import eligible_paper_ids
    assert len(rows) - 1 == len(eligible_paper_ids(exporter_db._conn))


# ── Evidence Excel ───────────────────────────────────────────────────


def test_evidence_excel_sheets(exporter_db, spec, tmp_path):
    out = str(tmp_path / "evidence.xlsx")
    export_evidence_excel(exporter_db, spec, out, arm=spec.extraction_models.arm)
    assert Path(out).exists()

    wb = openpyxl.load_workbook(out)
    # B5 (R30). Sheet 3 was an "Audit Log" read straight off `evidence_spans`
    # joined to EVERY extraction with no latest-extraction filter, while sheet 1
    # showed only the newest — so one workbook disagreed with itself. That is A1
    # inside a single file. "Field States" answers the question the audit log was
    # read for, per cell, from the reader.
    assert wb.sheetnames == ["Evidence Table", "Screening Log", "Field States"]

    # Evidence Table has header + data rows
    ws1 = wb["Evidence Table"]
    assert ws1.max_row >= 4  # 1 header + the fixture's 5 eligible papers

    # Screening Log has entries
    ws2 = wb["Screening Log"]
    assert ws2.max_row > 1

    # Field States has one row per (paper, field) in the corpus grid for the arm
    ws3 = wb["Field States"]
    from engine.core.codebook import load_codebook_beside
    from engine.core.effective import eligible_paper_ids
    n_papers = len(eligible_paper_ids(exporter_db._conn))
    n_fields = len(load_codebook_beside(exporter_db.db_path).field_names)
    assert ws3.max_row == 1 + n_papers * n_fields     # a derivation, not a literal
    wb.close()


# ── DOCX ─────────────────────────────────────────────────────────────


def test_docx_created(exporter_db, spec, tmp_path):
    out = str(tmp_path / "evidence.docx")
    export_evidence_docx(exporter_db, spec, out, arm=spec.extraction_models.arm)
    assert Path(out).exists()

    # Verify it's a valid docx by loading it
    from docx import Document
    doc = Document(out)
    # Should have at least one table
    assert len(doc.tables) >= 1
    # Table should have header + data rows
    table = doc.tables[0]
    assert len(table.rows) >= 4  # 1 header + the fixture's 5 eligible papers


# ── Methods Section ──────────────────────────────────────────────────


def test_methods_section_content(exporter_db, spec):
    methods = generate_methods_section(exporter_db, spec, run_id=None)

    # Key pipeline details present
    assert "PubMed" in methods
    assert "OpenAlex" in methods
    # Model names should come from spec, not be hardcoded
    assert spec.screening_models.primary in methods
    # 9d-C2-R1 (2): the extraction and audit model strings are pinned by
    # test_methods_uses_db_extraction_model and test_methods_uses_db_audit_model.
    assert "dual-pass" in methods
    assert "two-pass" in methods
    assert "15" in methods  # total records


def test_methods_md_export(exporter_db, spec, tmp_path):
    out = str(tmp_path / "methods.md")
    export_methods_md(exporter_db, spec, out, run_id=None)  # 9d-C2-R1 (1)
    assert Path(out).exists()

    content = Path(out).read_text()
    assert content.startswith("# Methods")
    assert "systematic search" in content


# ── export_all ───────────────────────────────────────────────────────


def test_export_all(exporter_db, spec, tmp_path):
    out_dir = str(tmp_path / "all_exports")
    # 9d-C3: export_all exports the spec's extraction arm, which holds the
    # fixture's claims (9e-C-P2).
    paths = export_all(exporter_db, spec, "test_export", output_dir=out_dir,
                       run_id=None)  # 9d-C2-R1 (1): run_id is required; None = no run

    # The three trace keys went with `trace_exporter.py` (R46): it reported on
    # reasoning traces and auditor verdicts, and the event store carries neither,
    # so it was retired rather than migrated. Their ABSENCE is asserted, because
    # a caller that still reads `paths["traces_dir"]` should fail here and not in
    # production.
    expected_keys = {
        "prisma_csv", "evidence_csv", "evidence_xlsx", "evidence_docx", "methods_md",
    }
    assert expected_keys.issubset(set(paths.keys()))
    assert not {"trace_quality_report", "trace_quality_report_md",
                "traces_dir"} & set(paths)

    for key, path in paths.items():
        assert Path(path).exists(), f"{key} not found at {path}"


# ── H6: Atomic write — no partial files on error ────────────────────


def test_atomic_csv_no_partial_on_error(exporter_db, spec, tmp_path):
    """If CSV export fails mid-write, no final file or temp file remains."""
    out = str(tmp_path / "evidence.csv")

    with patch("engine.exporters.evidence_table.csv.writer") as mock_writer:
        # Let writerow succeed for header, fail on writerows
        instance = mock_writer.return_value
        instance.writerows.side_effect = IOError("disk full")

        with pytest.raises(IOError, match="disk full"):
            export_evidence_csv(exporter_db, spec, out, arm=spec.extraction_models.arm)

    assert not Path(out).exists(), "Final file should not exist after error"
    assert not Path(out + ".tmp").exists(), "Temp file should be cleaned up"


def test_atomic_docx_no_partial_on_error(exporter_db, spec, tmp_path):
    """If DOCX export fails during save, no final file or temp file remains."""
    out = str(tmp_path / "evidence.docx")

    with patch("engine.exporters.docx_export.Document") as mock_doc_cls:
        mock_doc = mock_doc_cls.return_value
        mock_doc.sections = [type("Sec", (), {
            "orientation": None, "page_width": 1, "page_height": 2,
            "left_margin": 1, "right_margin": 1, "top_margin": 1, "bottom_margin": 1,
        })()]
        mock_doc.add_paragraph.return_value = type("Para", (), {
            "add_run": lambda self, *a, **kw: type("Run", (), {
                "bold": False, "font": type("F", (), {"size": None})()
            })()
        })()
        mock_doc.add_table.return_value = type("T", (), {
            "style": None,
            "rows": [type("R", (), {"cells": [type("C", (), {
                "text": "", "paragraphs": []
            })() for _ in range(50)]})() for _ in range(20)]
        })()
        mock_doc.save.side_effect = IOError("disk full")

        with pytest.raises(IOError, match="disk full"):
            export_evidence_docx(exporter_db, spec, out, arm=spec.extraction_models.arm)

    assert not Path(out).exists(), "Final file should not exist after error"
    assert not Path(out + ".tmp").exists(), "Temp file should be cleaned up"


def test_atomic_prisma_csv_no_partial_on_error(prisma_db, tmp_path):
    """If PRISMA CSV export fails, no final or temp file remains."""
    out = str(tmp_path / "prisma.csv")

    with patch("engine.exporters.prisma.csv.writer") as mock_writer:
        instance = mock_writer.return_value
        instance.writerows.side_effect = IOError("disk full")

        with pytest.raises(IOError, match="disk full"):
            export_prisma_csv(prisma_db, out)

    assert not Path(out).exists()
    assert not Path(out + ".tmp").exists()


def test_atomic_methods_md_no_partial_on_error(exporter_db, spec, tmp_path):
    """If methods MD export fails during write, no file remains."""
    out = str(tmp_path / "methods.md")

    with patch("builtins.open", side_effect=IOError("disk full")):
        with pytest.raises(IOError, match="disk full"):
            export_methods_md(exporter_db, spec, out, run_id=None)  # 9d-C2-R1 (1)

    assert not Path(out).exists()


# ── H13: Empty extraction rows marked ───────────────────────────────


@pytest.fixture()
def empty_extraction_db(tmp_path, spec):
    """empty_extraction_db' world on the toolkit (9e-C-P2 B2, R190): two
    eligible papers; paper 1 `audited_ai` with a located study_design claim,
    paper 2 `extracted` with no field events."""
    db = _toolkit_world(
        tmp_path, spec, "test_empty",
        citations=[Citation(title=f"Study {i}", source="pubmed", pmid=str(i),
                            doi=f"10.1/{i}", authors=["Auth A"], journal="J Test",
                            year=2023) for i in range(1, 3)])
    world = seed_prisma_world(db, eligible=[("audited_ai", None), ("extracted", None)])
    add_values(db.db_path, spec.extraction_models.arm, "study_design", ["RCT"],
               start_paper=world["eligible"][0])
    yield db
    db.close()


def test_empty_extraction_has_marker(empty_extraction_db, spec, tmp_path):
    """Papers with no extraction spans get [NO EXTRACTION DATA] marker."""
    out = str(tmp_path / "evidence.csv")
    export_evidence_csv(empty_extraction_db, spec, out, arm=spec.extraction_models.arm)

    with open(out) as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    assert len(rows) == 2

    # Find the empty paper (paper 2, no spans)
    markers = [r for r in rows if NO_EXTRACTION_MARKER in r.values()]
    assert len(markers) == 1, "Exactly one paper should have the marker"

    # The populated paper should NOT have the marker
    non_markers = [r for r in rows if NO_EXTRACTION_MARKER not in r.values()]
    assert len(non_markers) == 1
    assert non_markers[0]["study_design"] == "RCT"


def test_exclude_empty_omits_empty_papers(empty_extraction_db, spec, tmp_path):
    """With exclude_empty=True, papers with no extraction data are omitted."""
    out = str(tmp_path / "evidence.csv")
    export_evidence_csv(empty_extraction_db, spec, out, exclude_empty=True,
                        arm=spec.extraction_models.arm)

    with open(out) as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    assert len(rows) == 1
    assert rows[0]["study_design"] == "RCT"


def test_exclude_empty_excel(empty_extraction_db, spec, tmp_path):
    """Excel export also supports exclude_empty."""
    out = str(tmp_path / "evidence.xlsx")
    export_evidence_excel(empty_extraction_db, spec, out, exclude_empty=True,
                          arm=spec.extraction_models.arm)

    wb = openpyxl.load_workbook(out)
    ws = wb["Evidence Table"]
    # 1 header + 1 data row (empty paper excluded)
    assert ws.max_row == 2
    wb.close()


# ── H15: Dynamic model names in methods section ─────────────────────


def test_methods_uses_spec_screening_model(exporter_db, spec):
    """Methods section uses screening model from spec, not hardcoded."""
    methods = generate_methods_section(exporter_db, spec, run_id=None)  # 9d-C2-R1 (1)
    assert spec.screening_models.primary in methods


#: Model names no spec attribute carries: the run is opened on a copy of the
#: spec declaring them, and the module is handed the UNMODIFIED spec, so a
#: rendered name can only have come from the run's stage rows (9d-C2-R1 (3)).
#: `run_stage_configs` refuses UPDATE (migration 020), hence the spec copy.
RUN_EXTRACTOR = "fixture-extractor:x"
RUN_AUDITOR = "fixture-auditor:y"


@pytest.fixture()
def run_db(tmp_path, spec):
    """A review with one extraction run (R177): its extract stages name
    RUN_EXTRACTOR, its audit stage RUN_AUDITOR. Three papers; paper 1 is called
    twice on extract_pass1 and once on extract_pass2, paper 2 once on
    extract_pass1; paper 1 holds the run's one `audited_ai` event.
    Returns (db, run_id)."""
    from engine.core import events, run_manifest as rm
    from tests._event_store_fixture import open_extraction_run

    db = ReviewDatabase("test_methods_run", data_root=tmp_path)
    _real_codebook(db)
    db.add_papers([Citation(title=f"Study {i}", source="pubmed", pmid=str(i))
                   for i in range(1, 4)])
    p1, p2, _p3 = [r[0] for r in db._conn.execute("SELECT id FROM papers ORDER BY id")]

    arm = spec.extraction_models.arm
    run_spec = spec.model_copy(update={
        "extraction_models": spec.extraction_models.model_copy(
            update={"extractor": RUN_EXTRACTOR}),
        "arms": [a.model_copy(update={"model": RUN_EXTRACTOR}) if a.name == arm else a
                 for a in spec.arms],
        "auditor_model": RUN_AUDITOR,
    })
    run_id = open_extraction_run(db, run_spec)

    t = "2026-01-01T00:00:00+00:00"
    for stage, pid in (("extract_pass1", p1), ("extract_pass1", p1),
                       ("extract_pass2", p1), ("extract_pass1", p2)):
        rm.record_call(db._conn, run_id, stage, pid, {"stage": stage, "paper": pid},
                       "d" * 64, t, t)
    events.write_paper_event(
        db._conn, event_type="audited", paper_id=p1, to_state="audited_ai",
        actor_kind="engine", actor_role="system", actor_name="auditor",
        run_id=run_id, stage_name="audit")
    yield db, run_id
    db.close()


def test_methods_uses_db_extraction_model(run_db, spec):
    """Methods section names the extraction model from the run's stage rows."""
    db, run_id = run_db
    methods = generate_methods_section(db, spec, run_id=run_id)
    assert RUN_EXTRACTOR in methods
    assert RUN_EXTRACTOR not in spec.model_dump_json()   # the row, not the spec
    # Papers, over the run's stages in `_LOCAL_EXTRACTION_STAGES`: papers 1 and 2.
    from engine.exporters.methods_section import _run_extraction_models
    assert _run_extraction_models(db._conn, run_id) == {RUN_EXTRACTOR: 2}
    # G4: without the run, the placeholder.
    assert RUN_EXTRACTOR not in generate_methods_section(db, spec, run_id=None)


def test_methods_uses_db_audit_model(run_db, spec):
    """Methods section names the auditor from the run's audit stage row, and
    counts the papers holding the run's `audited_ai` event."""
    db, run_id = run_db
    methods = generate_methods_section(db, spec, run_id=run_id)
    assert f"Cross-model verification was performed by {RUN_AUDITOR}." in methods
    assert RUN_AUDITOR not in spec.model_dump_json()     # the row, not the spec
    from engine.exporters.methods_section import _run_audit_models
    assert _run_audit_models(db._conn, run_id) == {RUN_AUDITOR: 1}
    # G4: without the run, the placeholder.
    assert ("Cross-model verification was performed by [MODEL NOT SPECIFIED]."
            in generate_methods_section(db, spec, run_id=None))


def test_methods_counts_papers_not_calls(run_db):
    """Paper 1 has three extraction calls and paper 2 one: the count is 2
    papers, not 4 calls (R177: counts are papers throughout)."""
    from engine.exporters.methods_section import _run_extraction_models
    db, run_id = run_db
    calls = db._conn.execute(
        "SELECT COUNT(*) FROM run_calls WHERE run_id = ?", (run_id,)).fetchone()[0]
    assert calls == 4
    assert _run_extraction_models(db._conn, run_id) == {RUN_EXTRACTOR: 2}


def test_methods_multi_model_ft_screening(tmp_path, spec):
    """Methods section reports multiple FT screening models with counts."""
    db = ReviewDatabase("test_ft_models", data_root=tmp_path)
    _real_codebook(db)

    cits = [
        Citation(title=f"Study {i}", source="pubmed", pmid=str(i),
                 doi=f"10.1/{i}", authors=["Auth A"], journal="J Test", year=2023)
        for i in range(1, 6)
    ]
    db.add_papers(cits)
    papers = db.get_papers_by_status("INGESTED")

    for p in papers:
        pid = p["id"]
        db.add_screening_decision(pid, 1, "include", "Relevant", "qwen3:8b")
        db.add_screening_decision(pid, 2, "include", "Confirmed", "qwen3:8b")
        db.update_status(pid, "ABSTRACT_SCREENED_IN")
        db.update_status(pid, "PDF_ACQUIRED")
        db.update_status(pid, "PARSED")

    # Add FT screening decisions with two different models
    from engine.core.database import _now
    for i, p in enumerate(papers):
        model = "qwen3.5:27b" if i < 3 else "qwen3:32b"
        db._conn.execute(
            """INSERT INTO ft_screening_decisions
               (paper_id, model, decision, reason_code, rationale, confidence, decided_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (p["id"], model, "FT_ELIGIBLE", "IN_SCOPE", "Relevant", 0.9, _now()),
        )
        db.update_status(p["id"], "FT_ELIGIBLE")

    # B5 (R200 row 17): the extraction block was incidental — the FT model
    # counts come from ft_screening_decisions alone — so papers stay at FT_ELIGIBLE.
    db._conn.commit()

    methods = generate_methods_section(db, spec, run_id=None)  # 9d-C2-R1 (1)

    # Should contain both FT models with counts
    assert "qwen3.5:27b (n=3)" in methods
    assert "qwen3:32b (n=2)" in methods

    db.close()


def test_methods_placeholder_when_no_data(tmp_path, spec):
    """Methods section uses [MODEL NOT SPECIFIED] when DB has no extraction data."""
    db = ReviewDatabase("test_placeholder", data_root=tmp_path)
    _real_codebook(db)

    cits = [
        Citation(title="Study 1", source="pubmed", pmid="1",
                 doi="10.1/1", authors=["Auth A"], journal="J Test", year=2023)
    ]
    db.add_papers(cits)
    papers = db.get_papers_by_status("INGESTED")
    for p in papers:
        db.add_screening_decision(p["id"], 1, "include", "Relevant", "qwen3:8b")
        db.add_screening_decision(p["id"], 2, "include", "OK", "qwen3:8b")
        db.update_status(p["id"], "ABSTRACT_SCREENED_IN")
        db.update_status(p["id"], "PDF_ACQUIRED")
        db.update_status(p["id"], "PARSED")  # B5 (R200 row 27): the walk stops here

    # C30 (9d-C2-R1 (2)): the spec's `auditor_model` is never a fallback.
    sentinel = "sentinel-auditor-must-not-render"
    methods = generate_methods_section(
        db, spec.model_copy(update={"auditor_model": sentinel}), run_id=None)

    # No extractions or audits in DB → placeholders
    assert "[MODEL NOT SPECIFIED]" in methods
    assert sentinel not in methods

    db.close()


# ── Row C29: the export arm (9d-C3, R174, R176) ─────────────────────


def test_export_all_exports_the_spec_arm(tmp_path, spec):
    """`export_all` exports the spec's `extraction_models.arm` — here
    `fixture-arm` — and nothing from the pre-manifest `local` arm, which holds
    claims of its own on the same papers. Before 9d-C3 the exporters defaulted to
    `"local"` and `export_all` passed no arm (row C29)."""
    from docx import Document

    from engine.core import events
    from engine.core.effective import PRE_MANIFEST
    from tests._event_store_fixture import add_values, seed_claim

    db = ReviewDatabase("test_export_arm", data_root=tmp_path)
    _real_codebook(db)
    db.add_papers([Citation(title=f"Arm Study {i}", source="pubmed", pmid=str(i))
                   for i in range(1, 3)])
    pids = [r[0] for r in db._conn.execute("SELECT id FROM papers ORDER BY id")]
    # Raw SQL on papers.status: retires at the screeners' cut-over (R163 precedent).
    # The corpus status matches the eligible events below, so PRISMA's seam holds.
    db._conn.execute("UPDATE papers SET status = 'FT_ELIGIBLE'")
    db._conn.commit()

    # fixture-arm: registered and pinned by the fixture run; papers made eligible.
    add_values(db.db_path, "fixture-arm", "study_type",
               ["Original Research"] * len(pids), start_paper=pids[0])
    # local: the Run-6 shape — registered pre-manifest, seeded claims.
    events.register_arm(db._conn, "local", "model", configuration_marker=PRE_MANIFEST)
    for pid in pids:
        seed_claim(db._conn, arm="local", paper_id=pid, field_name="study_type",
                   value="Review", source_snippet="local-only snippet")
    db._conn.commit()

    arm_spec = spec.model_copy(update={"extraction_models": spec.extraction_models.model_copy(
        update={"arm": "fixture-arm"})})
    paths = export_all(db, arm_spec, "test_export_arm",
                       output_dir=str(tmp_path / "out"), run_id=None)

    with open(paths["evidence_csv"]) as f:
        rows = list(csv.DictReader(f))
    assert [r["study_type"] for r in rows] == ["Original Research"] * len(pids)
    assert "local-only snippet" not in Path(paths["evidence_csv"]).read_text()

    docx_text = "\n".join(c.text for t in Document(paths["evidence_docx"]).tables
                          for row in t.rows for c in row.cells)
    assert "Original Research" in docx_text
    assert "Review" not in docx_text
    db.close()
