"""PARSE-GATE-06c — the vision route, bounded.

Unbounded, one page of p455 produced a 211-character cycle at 42.6 tok/s and
would have run ~50 minutes to the 128k context, then restarted the Ollama
service on the third retry (PARSE-GATE-03/04). Three bounds close that: an
output cap, a context window, and a per-page wall clock -- plus an abort on the
first page that hits the cap, because a truncated page is a loop and its text is
a correct-looking prefix followed by garbage.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fpdf import FPDF

from engine.core.database import ReviewDatabase
from engine.core.review_spec import load_review_spec
from engine.parsers.pdf_parser import (
    VISION_PROMPT,
    VisionTruncatedError,
    parse_pdf,
    parse_with_vision,
)
from engine.search.models import Citation

SPEC = "review_specs/surgical_autonomy.yaml"
CLEAN = (
    "The robotic assistant was evaluated in a porcine model over twelve "
    "procedures. Task completion time fell by nineteen percent against the "
    "manual baseline. No adverse events were recorded during the study. "
) * 12


def _resp(text="page text", done_reason="stop", eval_count=724):
    m = MagicMock()
    m.message.content = text
    m.done_reason = done_reason
    m.eval_count = eval_count
    m.prompt_eval_count = 2630
    return m


@pytest.fixture()
def three_page_pdf(tmp_path):
    pdf = FPDF()
    for i in range(3):
        pdf.add_page()
        pdf.set_font("Helvetica", size=12)
        pdf.multi_cell(w=0, text=f"Page {i + 1} body text. " * 30)
    p = tmp_path / "three.pdf"
    pdf.output(str(p))
    return p


@pytest.fixture()
def scanned_pdf(tmp_path):
    pdf = FPDF(); pdf.add_page()
    p = tmp_path / "scanned.pdf"; pdf.output(str(p)); return p


@pytest.fixture()
def db(tmp_path):
    rdb = ReviewDatabase("test_vbounds", data_root=tmp_path)
    yield rdb
    rdb.close()


def _paper(db, hint="1") -> int:
    db.add_papers([Citation(title=f"Paper {hint}", source="pubmed", pmid=hint)])
    pid = db.get_papers_by_status("INGESTED")[-1]["id"]
    db.update_status(pid, "ABSTRACT_SCREENED_IN")
    db.update_status(pid, "PDF_ACQUIRED")
    return pid


# ── T1 — options carry the caps ───────────────────────────────────────

def test_defaults_send_num_predict_and_num_ctx(three_page_pdf):
    with patch("engine.parsers.pdf_parser.ollama_chat", return_value=_resp()) as chat:
        parse_with_vision(str(three_page_pdf))
    opts = chat.call_args.kwargs["options"]
    assert opts["num_predict"] == 2048
    assert opts["num_ctx"] == 8192
    assert opts["temperature"] == 0


def test_explicit_overrides_reach_the_call(three_page_pdf):
    with patch("engine.parsers.pdf_parser.ollama_chat", return_value=_resp()) as chat:
        parse_with_vision(str(three_page_pdf), num_predict=99, num_ctx=1024)
    opts = chat.call_args.kwargs["options"]
    assert opts["num_predict"] == 99 and opts["num_ctx"] == 1024


def test_spec_overrides_reach_the_call_through_parse_pdf(scanned_pdf, db):
    spec = load_review_spec(SPEC)
    spec.pdf_parsing.vision_num_predict = 512
    spec.pdf_parsing.vision_num_ctx = 4096
    spec.pdf_parsing.vision_page_timeout_s = 77
    pid = _paper(db)

    with patch("engine.parsers.pdf_parser.parse_with_docling_ocr",
               side_effect=RuntimeError("ocr off")), \
         patch("engine.parsers.pdf_parser.ollama_chat",
               return_value=_resp(CLEAN)) as chat:
        parse_pdf(str(scanned_pdf), pid, "test_vbounds", db, spec=spec)

    opts = chat.call_args.kwargs["options"]
    assert opts["num_predict"] == 512 and opts["num_ctx"] == 4096
    assert chat.call_args.kwargs["wall_timeout"] == 77
    assert chat.call_args.kwargs["paper_id"] == pid


# ── T2 — the prompt ───────────────────────────────────────────────────

def test_prompt_is_pinned_exactly():
    assert VISION_PROMPT == (
        "Transcribe all text on this page verbatim, in reading order. "
        "Output plain text only — no LaTeX, no Markdown markup. "
        "Keep each heading on its own line. "
        "Keep table rows as lines with cells separated by ' | '."
    )


def test_the_looping_prompt_is_not_reintroduced():
    """PARSE-GATE-04 probe D: changing ONLY this string stopped the loop.

    The old prompt asked for reformatting ("Output as Markdown") on pages with
    nothing to format. Guard the exact instruction that caused it.
    """
    assert "Output as Markdown" not in VISION_PROMPT
    assert "Preserve tables, headings, and formatting" not in VISION_PROMPT
    assert "Transcribe" in VISION_PROMPT


def test_the_prompt_is_what_is_sent(three_page_pdf):
    with patch("engine.parsers.pdf_parser.ollama_chat", return_value=_resp()) as chat:
        parse_with_vision(str(three_page_pdf))
    assert chat.call_args.kwargs["messages"][0]["content"] == VISION_PROMPT


# ── T3 — truncation aborts the attempt ────────────────────────────────

def test_truncation_on_page_2_aborts_before_page_3(three_page_pdf):
    calls = {"n": 0}

    def responses(*a, **k):
        calls["n"] += 1
        return _resp(done_reason="length" if calls["n"] == 2 else "stop")

    with patch("engine.parsers.pdf_parser.ollama_chat", side_effect=responses):
        with pytest.raises(VisionTruncatedError, match="page 2 hit num_predict=2048"):
            parse_with_vision(str(three_page_pdf))

    assert calls["n"] == 2, "page 3 must never be requested"


def test_truncation_becomes_an_unselectable_error_row(scanned_pdf, db):
    pid = _paper(db)
    with patch("engine.parsers.pdf_parser.parse_with_docling_ocr",
               side_effect=RuntimeError("ocr off")), \
         patch("engine.parsers.pdf_parser.ollama_chat",
               return_value=_resp(done_reason="length")):
        # The real cause propagates -- it is more useful than a generic message --
        # and the ledger is committed anyway (PARSE-GATE-06b Contract 7).
        with pytest.raises(VisionTruncatedError, match="page 1 hit num_predict"):
            parse_pdf(str(scanned_pdf), pid, "test_vbounds", db)

    rows = db._conn.execute(
        "SELECT * FROM parse_attempts WHERE paper_id = ? ORDER BY attempt_index",
        (pid,)).fetchall()
    vision = [r for r in rows if r["parser_used"] == "qwen2.5vl"]
    assert len(vision) == 1
    assert vision[0]["skipped_reason"].startswith("error: VisionTruncatedError")
    assert "page 1 hit num_predict=2048" in vision[0]["skipped_reason"]
    assert vision[0]["accepted"] == 0 and vision[0]["passed"] == 0


def test_no_partial_document_survives_a_truncated_page(three_page_pdf):
    """Pages already transcribed are discarded, not returned as a short document."""
    calls = {"n": 0}

    def responses(*a, **k):
        calls["n"] += 1
        return _resp(text="good page " * 40,
                     done_reason="length" if calls["n"] == 3 else "stop")

    with patch("engine.parsers.pdf_parser.ollama_chat", side_effect=responses):
        with pytest.raises(VisionTruncatedError):
            parse_with_vision(str(three_page_pdf))
    # nothing to assert on a return value: there is none, which is the point.


# ── T4 — the healthy path is unchanged ────────────────────────────────

def test_all_pages_stop_assembles_the_document_as_before(three_page_pdf):
    with patch("engine.parsers.pdf_parser.ollama_chat",
               return_value=_resp(text="body")):
        out = parse_with_vision(str(three_page_pdf))
    assert out.count("<!-- Page ") == 3
    assert out.count("\n\n---\n\n") == 2          # joining unchanged
    assert "<!-- Page 1 -->\nbody" in out


def test_per_page_log_lines_are_emitted(three_page_pdf, caplog):
    import logging
    with caplog.at_level(logging.INFO, logger="engine.parsers.pdf_parser"):
        with patch("engine.parsers.pdf_parser.ollama_chat", return_value=_resp()):
            parse_with_vision(str(three_page_pdf), paper_id=455)
    lines = [r.getMessage() for r in caplog.records if "Vision page" in r.getMessage()]
    assert len(lines) == 3
    assert "done_reason=stop" in lines[0]
    assert "eval_count=724" in lines[0]
    assert "prompt_eval_count=2630" in lines[0]
    assert "paper_id=455" in lines[0]


# ── T5 — per-call timeout (I2 = yes) ──────────────────────────────────

def test_per_call_wall_timeout_is_passed(three_page_pdf):
    """ollama_chat accepts wall_timeout, so Contract 4 applies."""
    with patch("engine.parsers.pdf_parser.ollama_chat", return_value=_resp()) as chat:
        parse_with_vision(str(three_page_pdf), page_timeout_s=123)
    assert chat.call_args.kwargs["wall_timeout"] == 123


def test_default_page_timeout_is_the_spec_default(three_page_pdf):
    with patch("engine.parsers.pdf_parser.ollama_chat", return_value=_resp()) as chat:
        parse_with_vision(str(three_page_pdf))
    assert chat.call_args.kwargs["wall_timeout"] == 240
    assert load_review_spec(SPEC).pdf_parsing.vision_page_timeout_s == 240


def test_spec_defaults_for_the_three_new_fields():
    s = load_review_spec(SPEC).pdf_parsing
    assert (s.vision_num_predict, s.vision_num_ctx, s.vision_page_timeout_s) == (
        2048, 8192, 240)
