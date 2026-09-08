"""PARSE-GATE-06b — the deterministic OCR tier, reroute rewrite, glue telemetry,
and the ledger that survives a wholly failed parse.

The tier exists because p719's text layer is present and WRONG: 5,472 GLYPH
tokens wrapped around Caesar-shifted gibberish. PyMuPDF reads the same layer, so
it was never a remedy; re-OCR-ing the page image is. PARSE-GATE-05 measured the
tier at ~6 s/page, byte-reproducible, recovering every heading p719 had lost.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest
from fpdf import FPDF

from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec
from engine.parsers.parse_quality import (
    ADDED_METRIC_NAMES,
    METRIC_NAMES,
    compute_metrics,
)
from engine.parsers.pdf_parser import (
    _MAX_ATTEMPTS,
    _REROUTE,
    parse_all_pdfs,
    parse_pdf,
    parse_with_docling_ocr,
    reparse_papers,
)
from engine.search.models import Citation

SPEC = "review_specs/surgical_autonomy_v1.yaml"
REPO = Path(__file__).resolve().parents[1]
P719_PDF = REPO / "data" / "surgical_autonomy" / "pdfs" / "EE-567_Bauzano_2010.pdf"
FIXTURES = REPO / "tests" / "fixtures" / "parse_quality"

CLEAN = (
    "The robotic assistant was evaluated in a porcine model over twelve "
    "procedures. Task completion time fell by nineteen percent against the "
    "manual baseline. No adverse events were recorded during the study. "
) * 12
GLYPHY = CLEAN + " " + " ".join(["GLYPH<c=1,font=/AOIIJB+MinionPro>"] * 90)
SHATTERED_TEXT = "\n".join(list("computerassistedsurgicalnavigation" * 40))


@pytest.fixture(autouse=True)
def _no_unstubbed_ocr(request):
    """Keep real docling+rapidocr out of the standard gate.

    Integration-marked tests opt out: they exist to run the real tier.
    """
    if request.node.get_closest_marker("integration"):
        yield
        return
    with patch("engine.parsers.pdf_parser.parse_with_docling_ocr",
               side_effect=RuntimeError("docling_ocr not stubbed in this test")):
        yield


@pytest.fixture()
def digital_pdf(tmp_path):
    pdf = FPDF(); pdf.add_page(); pdf.set_font("Helvetica", size=12)
    pdf.multi_cell(w=0, text="Autonomous robotic suturing evaluation. " * 40)
    p = tmp_path / "digital.pdf"; pdf.output(str(p)); return p


@pytest.fixture()
def scanned_pdf(tmp_path):
    pdf = FPDF(); pdf.add_page()          # no text layer
    p = tmp_path / "scanned.pdf"; pdf.output(str(p)); return p


@pytest.fixture()
def big_scanned_pdf(tmp_path):
    pdf = FPDF()
    for _ in range(101):
        pdf.add_page()
    p = tmp_path / "big_scanned.pdf"; pdf.output(str(p)); return p


@pytest.fixture()
def db(tmp_path):
    rdb = ReviewDatabase("test_ocr", data_root=tmp_path)
    yield rdb
    rdb.close()


def _paper(db, hint="1") -> int:
    db.add_papers([Citation(title=f"Paper {hint}", source="pubmed", pmid=hint)])
    pid = db.get_papers_by_status("INGESTED")[-1]["id"]
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")
    return pid


def _rows(db, pid):
    return db._conn.execute(
        "SELECT * FROM parse_attempts WHERE paper_id = ? ORDER BY attempt_index",
        (pid,)).fetchall()


# ── T1/T2 — the scanned route now leads with the deterministic tier ────

def test_scanned_pdf_goes_to_the_ocr_tier_first(scanned_pdf, db):
    pid = _paper(db)
    with patch("engine.parsers.pdf_parser.parse_with_docling_ocr", return_value=CLEAN), \
         patch("engine.parsers.pdf_parser.parse_with_vision") as vision:
        result = parse_pdf(str(scanned_pdf), pid, "test_ocr", db)

    vision.assert_not_called()
    assert result.accepted_parser == "docling_ocr"
    rows = _rows(db, pid)
    assert [r["parser_used"] for r in rows] == ["docling_ocr"]
    assert rows[0]["passed"] == 1 and rows[0]["accepted"] == 1


def test_scanned_ocr_failure_falls_through_to_vision(scanned_pdf, db):
    pid = _paper(db)
    with patch("engine.parsers.pdf_parser.parse_with_docling_ocr",
               side_effect=RuntimeError("ocr exploded")), \
         patch("engine.parsers.pdf_parser.parse_with_vision", return_value=CLEAN):
        result = parse_pdf(str(scanned_pdf), pid, "test_ocr", db)

    rows = _rows(db, pid)
    assert [r["parser_used"] for r in rows] == ["docling_ocr", "qwen2.5vl"]
    assert rows[0]["skipped_reason"].startswith("error: RuntimeError: ocr exploded")
    assert result.accepted_parser == "qwen2.5vl"


def test_scanned_ocr_sparse_output_also_falls_through_to_vision(scanned_pdf, db):
    """OCR that returns nothing on a scanned page is a tier failure, not a verdict."""
    pid = _paper(db)
    with patch("engine.parsers.pdf_parser.parse_with_docling_ocr", return_value="  "), \
         patch("engine.parsers.pdf_parser.parse_with_vision", return_value=CLEAN):
        result = parse_pdf(str(scanned_pdf), pid, "test_ocr", db)

    rows = _rows(db, pid)
    assert rows[0]["parser_used"] == "docling_ocr"
    assert "ocr output sparse" in rows[0]["skipped_reason"]
    assert result.accepted_parser == "qwen2.5vl"


# ── T3 — the digital route is untouched ───────────────────────────────

def test_digital_clean_docling_never_reaches_the_ocr_tier(digital_pdf, db):
    pid = _paper(db)
    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value=CLEAN), \
         patch("engine.parsers.pdf_parser.parse_with_docling_ocr") as ocr, \
         patch("engine.parsers.pdf_parser.parse_with_vision") as vision:
        result = parse_pdf(str(digital_pdf), pid, "test_ocr", db)

    ocr.assert_not_called()
    vision.assert_not_called()
    assert result.accepted_parser == "docling"
    assert len(_rows(db, pid)) == 1


# ── T4 — reroute order ────────────────────────────────────────────────

def test_every_criterion_reroutes_to_the_ocr_tier_before_vision():
    from engine.parsers.parse_quality import (
        EMPTY_TEXT, GLYPH_DENSITY, REPLACEMENT_DENSITY, SHATTERED,
    )
    assert _REROUTE[GLYPH_DENSITY] == ("docling_ocr", "qwen2.5vl")
    assert _REROUTE[REPLACEMENT_DENSITY] == ("docling_ocr", "qwen2.5vl")
    assert _REROUTE[SHATTERED] == ("docling_ocr", "qwen2.5vl")
    # EMPTY_TEXT keeps PyMuPDF in front: an empty docling result is usually a
    # structural failure over a good text layer, which PyMuPDF reads instantly.
    assert _REROUTE[EMPTY_TEXT] == ("pymupdf", "docling_ocr", "qwen2.5vl")
    for order in _REROUTE.values():
        assert order.index("qwen2.5vl") == len(order) - 1, "vision is the last resort"


def test_glyph_failure_reaches_ocr_without_touching_pymupdf(digital_pdf, db):
    pid = _paper(db)
    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value=GLYPHY), \
         patch("engine.parsers.pdf_parser.parse_with_docling_ocr", return_value=CLEAN), \
         patch("engine.parsers.pdf_parser.parse_with_pymupdf") as pymupdf, \
         patch("engine.parsers.pdf_parser.parse_with_vision") as vision:
        result = parse_pdf(str(digital_pdf), pid, "test_ocr", db)

    pymupdf.assert_not_called()
    vision.assert_not_called()
    assert result.accepted_parser == "docling_ocr"


# ── T5 — the sanitized copy is reused by the OCR tier ─────────────────

def test_ocr_tier_reuses_the_sanitized_copy_and_it_is_cleaned_up(tmp_path, db):
    import fitz
    pdf = FPDF(); pdf.add_page(); pdf.set_font("Helvetica", size=12)
    pdf.multi_cell(w=0, text="Autonomous robotic suturing evaluation. " * 40)
    base = tmp_path / "b.pdf"; pdf.output(str(base))
    d = fitz.open(base)
    d[0].insert_link({"kind": fitz.LINK_URI, "from": fitz.Rect(50, 50, 200, 70),
                      "uri": "dx.doi.org/10.1016/j.cmpb.2013.01.017"})
    linked = tmp_path / "linked.pdf"; d.save(str(linked), garbage=3, deflate=True); d.close()

    seen = {}

    def docling(path):
        if "parse_sanitized_" in str(path):
            return GLYPHY                      # sanitized retry succeeds but FAILS the gate
        raise RuntimeError("PdfHyperlink")

    def ocr(path, ocr_engine="rapidocr"):
        seen["path"] = str(path)
        seen["existed"] = Path(path).exists()
        return CLEAN

    pid = _paper(db)
    with patch("engine.parsers.pdf_parser.parse_with_docling", side_effect=docling), \
         patch("engine.parsers.pdf_parser.parse_with_docling_ocr", side_effect=ocr):
        result = parse_pdf(str(linked), pid, "test_ocr", db)

    assert "parse_sanitized_" in seen["path"], "OCR must read the sanitized copy"
    assert seen["existed"] is True, "copy must still exist during the OCR attempt"
    assert not Path(seen["path"]).exists(), "copy must be gone at the end of parse_pdf"

    rows = _rows(db, pid)
    assert [r["parser_used"] for r in rows] == [
        "docling", "docling_sanitized", "docling_ocr_sanitized"]
    assert result.accepted_parser == "docling_ocr_sanitized"


# ── T6 — the OCR page cap ─────────────────────────────────────────────

def test_ocr_cap_skips_the_tier_and_the_next_parser_is_tried(big_scanned_pdf, db):
    """101 pages: over ocr_max_pages=100. The cap is not a dead end."""
    pid = _paper(db)
    with patch("engine.parsers.pdf_parser.parse_with_vision", return_value=CLEAN):
        result = parse_pdf(str(big_scanned_pdf), pid, "test_ocr", db)

    rows = _rows(db, pid)
    assert rows[0]["parser_used"] == "docling_ocr"
    assert "ocr skipped: 101 pages exceeds ocr_max_pages=100" in rows[0]["skipped_reason"]
    assert result.accepted_parser == "qwen2.5vl"


def test_ocr_cap_on_a_reroute_falls_through_to_vision(digital_pdf, db):
    """A cap is not a dead end: the next parser in the criterion order is tried."""
    spec = load_review_spec(SPEC)
    spec.pdf_parsing.ocr_max_pages = 0 if False else 1
    pid = _paper(db)
    # The fixture is 1 page, so cap it below that to force the skip.
    spec.pdf_parsing.ocr_max_pages = 1
    with patch("engine.parsers.pdf_parser.page_count", return_value=500), \
         patch("engine.parsers.pdf_parser.parse_with_docling", return_value=GLYPHY), \
         patch("engine.parsers.pdf_parser.parse_with_vision", return_value=CLEAN):
        result = parse_pdf(str(digital_pdf), pid, "test_ocr", db, spec=spec)

    rows = _rows(db, pid)
    assert rows[1]["parser_used"] == "docling_ocr"
    assert "ocr skipped: 500 pages exceeds ocr_max_pages=1" in rows[1]["skipped_reason"]
    # vision is capped too at 500 pages, so it is skipped as well and the
    # least-bad judged attempt (docling) is accepted -- the point is that the
    # cascade CONTINUED past the OCR cap rather than stopping at it.
    assert rows[2]["parser_used"] == "qwen2.5vl"
    assert result.accepted_parser == "docling"


# ── T7 — attempt cap ──────────────────────────────────────────────────

def test_attempt_cap_is_five():
    assert _MAX_ATTEMPTS == 5


def test_sparse_length_fallback_still_precedes_the_gate(digital_pdf, db):
    """PARSE-GATE-02 T7 ordering pin, unchanged by the OCR tier."""
    pid = _paper(db)
    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value="short"), \
         patch("engine.parsers.pdf_parser.parse_with_pymupdf", return_value=CLEAN), \
         patch("engine.parsers.pdf_parser.parse_with_docling_ocr") as ocr, \
         patch("engine.parsers.pdf_parser.parse_with_vision") as vision:
        result = parse_pdf(str(digital_pdf), pid, "test_ocr", db)

    ocr.assert_not_called()
    vision.assert_not_called()
    assert [r["parser_used"] for r in _rows(db, pid)] == ["pymupdf"]
    assert result.accepted_parser == "pymupdf"


# ── T8 — the ledger survives a total failure ──────────────────────────

def test_total_failure_still_writes_the_attempt_rows(digital_pdf, db):
    pid = _paper(db)
    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value="  "), \
         patch("engine.parsers.pdf_parser.parse_with_pymupdf", return_value="  "), \
         patch("engine.parsers.pdf_parser.parse_with_vision", return_value="  "):
        with pytest.raises(ValueError, match="all parsers returned empty text"):
            parse_pdf(str(digital_pdf), pid, "test_ocr", db)

    rows = _rows(db, pid)
    assert len(rows) >= 1, "a wholly failed parse used to leave no trace at all"
    assert all(r["accepted"] == 0 for r in rows)
    assert db._conn.execute(
        "SELECT COUNT(*) FROM full_text_assets WHERE paper_id=?", (pid,)).fetchone()[0] == 0
    parsed_dir = Path(db.db_path).parent / "parsed_text"
    assert not list(parsed_dir.glob(f"{pid}_v*.md"))


def test_reparse_reports_a_total_failure_with_rows_written(digital_pdf, db):
    pid = _paper(db)
    pdfs = Path(db.db_path).parent / "pdfs"; pdfs.mkdir(parents=True, exist_ok=True)
    (pdfs / f"{pid}.pdf").write_bytes(digital_pdf.read_bytes())
    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value=CLEAN):
        parse_all_pdfs(db, "test_ocr")

    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value="  "), \
         patch("engine.parsers.pdf_parser.parse_with_pymupdf", return_value="  "), \
         patch("engine.parsers.pdf_parser.parse_with_vision", return_value="  "):
        out = reparse_papers(db, [pid])

    assert "error" in out[pid]
    assert "all parsers returned empty text" in out[pid]["error"]
    versions = {r["parsed_text_version"] for r in _rows(db, pid)}
    assert versions == {1, 2}, "the failed run's rows carry the version it would have taken"


# ── T9 — glue telemetry ───────────────────────────────────────────────

def test_long_token_share_counts_glued_runs():
    glued = "x" * 30
    clean = ["alpha", "beta", "gamma", "delta"] * 4 + ["epsilon"]   # 17 tokens
    text = " ".join(clean + [glued] * 3)                            # + 3 glued = 20
    m = compute_metrics(text)
    assert m["long_token_share_pct"] == 15.0        # 3 glued of 20 alpha tokens


def test_long_token_share_is_zero_without_glue_and_on_empty():
    assert compute_metrics("alpha beta gamma.")["long_token_share_pct"] == 0.0
    assert compute_metrics("")["long_token_share_pct"] == 0.0
    assert compute_metrics("123 456 !!!")["long_token_share_pct"] == 0.0


def test_long_token_share_on_the_committed_fixtures():
    """Documentation, not a threshold: neither fixture is OCR output.

    The measured separation lives on the OCR route -- PARSE-GATE-05 recorded
    2.525% for p455 full-page OCR and 0.0% for the same paper's docling text
    layer. Recorded here so the numbers have a home in the suite.
    """
    a = compute_metrics((FIXTURES / "p455_shattered.md").read_text())
    b = compute_metrics((FIXTURES / "p561_clean.md").read_text())
    assert a["long_token_share_pct"] == 0.0
    assert b["long_token_share_pct"] == 0.0


def test_metric_name_set_gained_exactly_one_name():
    assert "long_token_share_pct" in METRIC_NAMES
    assert ADDED_METRIC_NAMES == {
        "glyph_density_per_kchar", "replacement_density_per_kchar",
        "uni_escape_count", "long_token_share_pct", "is_empty",
    }
    from engine.parsers.parse_quality import CRITERIA
    assert "long_token_share_pct" not in CRITERIA      # telemetry, never judged


# ── T10 — spec ────────────────────────────────────────────────────────

def test_spec_defaults_for_the_ocr_tier():
    s = load_review_spec(SPEC).pdf_parsing
    assert s.ocr_engine == "rapidocr"
    assert s.ocr_max_pages == 100


def test_unknown_ocr_engine_raises_clearly():
    with pytest.raises(ValueError, match="unsupported ocr_engine"):
        parse_with_docling_ocr("/nonexistent.pdf", ocr_engine="tesseract")


def test_spec_ocr_engine_override_reaches_the_parser(digital_pdf, db):
    spec = load_review_spec(SPEC)
    spec.pdf_parsing.ocr_engine = "not-a-real-engine"
    pid = _paper(db)
    seen = {}

    def ocr(path, ocr_engine="rapidocr"):
        seen["engine"] = ocr_engine
        return CLEAN

    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value=GLYPHY), \
         patch("engine.parsers.pdf_parser.parse_with_docling_ocr", side_effect=ocr):
        parse_pdf(str(digital_pdf), pid, "test_ocr", db, spec=spec)
    assert seen["engine"] == "not-a-real-engine"


# ── T11 — the real tier on the real document (integration) ────────────

@pytest.mark.integration
@pytest.mark.skipif(not P719_PDF.exists(), reason="corpus PDF not present")
def test_real_ocr_tier_recovers_p719():
    """p719's stored text layer is Caesar-shifted gibberish behind 5,472 GLYPH
    tokens. PARSE-GATE-05 measured the OCR tier at 28,836 chars, PASS, glyph 0."""
    from engine.parsers.parse_quality import assess
    import engine.parsers.pdf_parser as P

    md = P.parse_with_docling_ocr(str(P719_PDF))
    v = assess(md)
    assert v.passed is True
    assert v.metrics["glyph_density_per_kchar"] == 0.0
    assert abs(len(md) - 28_836) / 28_836 < 0.10
    assert "PASSIVE WRIST EMULATION".lower() in md.lower()
