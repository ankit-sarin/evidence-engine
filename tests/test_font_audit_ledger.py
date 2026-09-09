"""The `parse_attempts.font_audit` column, the migration that adds it, and the
cascade wiring that fills it (FONT-AUDIT-02).

These tests are about persistence and coupling, not about the audit's arithmetic
— that lives in `tests/test_font_audit.py`. Every database here is built in
`tmp_path` via `ReviewDatabase(name, data_root=tmp_path)`; the JUDGE-DBGUARD-01
fence refuses any open of the real `data/` tree and `tests/test_live_db_guard.py`
is what keeps that true.
"""

from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest
from fpdf import FPDF

from engine.core.database import ReviewDatabase
from engine.core.review_spec import ParseQuality
from engine.parsers.font_audit import (
    ESTIMATOR,
    SIGNATURE,
    as_ledger_json,
    audit,
    font_structure,
    structure_ledger_json,
)
from engine.parsers.models import ParseAttempt
from engine.parsers.parse_quality import (
    FONT_EXPOSURE,
    GLYPH_DENSITY,
    Thresholds,
    assess,
    compute_metrics,
)
from engine.parsers.pdf_parser import _insert_attempts
from engine.search.models import Citation
from tests._font_fixtures import _make_pdf

_LEDGER_KEYS = {
    "estimator", "block_min", "reason", "font_exposure_per_kchar",
    "signature_fonts", "unresolving_fonts", "total_fonts", "sig_chars_pdf",
    "marked_in_text", "marked_unattributed", "silent_in_text_lo",
    "silent_in_text_hi", "silent_on_dropped_pages", "space_recoverable",
    "exported_chars", "exposure_lo", "exposure_hi", "fonts",
}


@pytest.fixture()
def db(tmp_path):
    rdb = ReviewDatabase("test_font_ledger", data_root=tmp_path)
    yield rdb
    rdb.close()


@pytest.fixture()
def small_pdf(tmp_path):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Helvetica", size=12)
    pdf.multi_cell(w=0, text="Autonomous robotic suturing evaluation. " * 20)
    path = tmp_path / "small.pdf"
    pdf.output(str(path))
    return path


def _paper(db) -> int:
    db.add_papers([Citation(title="Paper 1", source="pubmed", pmid="1")])
    return db.get_papers_by_status("INGESTED")[-1]["id"]


@pytest.fixture()
def signature_pdf_6p(tmp_path):
    """Six pages of signature-font text — enough for a per-page deadline to bite."""
    path, _ = _make_pdf(tmp_path, "an ordinary sentence of prose about probes",
                        broken=True, pages=6, name="sig6.pdf")
    return path


def _columns(conn, table) -> set[str]:
    return {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}


# ── the migration ────────────────────────────────────────────────────


def test_the_column_exists_on_a_fresh_database(db):
    assert "font_audit" in _columns(db._conn, "parse_attempts")


def test_the_migration_is_idempotent_across_reopens(tmp_path):
    """Open, close, open again. The ALTER must not raise the second time.

    `_SIMPLE_MIGRATIONS` re-executes every statement on every open and swallows
    "duplicate column"; this asserts the swallow actually covers this statement
    rather than trusting that it does.
    """
    first = ReviewDatabase("reopen_probe", data_root=tmp_path)
    assert "font_audit" in _columns(first._conn, "parse_attempts")
    first.close()

    second = ReviewDatabase("reopen_probe", data_root=tmp_path)
    assert "font_audit" in _columns(second._conn, "parse_attempts")
    second.close()

    third = ReviewDatabase("reopen_probe", data_root=tmp_path)
    assert "font_audit" in _columns(third._conn, "parse_attempts")
    third.close()


def test_the_migration_adds_the_column_to_a_database_that_predates_it(tmp_path):
    """The upgrade path, not the create path.

    A database built before FONT-AUDIT-02 has `parse_attempts` without the
    column. Dropping and recreating the table without it reproduces that state,
    and reopening must add it back without touching the rows.
    """
    rdb = ReviewDatabase("legacy_probe", data_root=tmp_path)
    path = rdb.db_path
    rdb.close()

    conn = sqlite3.connect(str(path))
    conn.executescript(
        """
        DROP TABLE parse_attempts;
        CREATE TABLE parse_attempts (
            id INTEGER PRIMARY KEY, paper_id INTEGER NOT NULL,
            pdf_hash TEXT, parsed_text_version INTEGER NOT NULL,
            attempt_index INTEGER NOT NULL, parser_used TEXT NOT NULL,
            passed INTEGER NOT NULL DEFAULT 0, failures TEXT, metrics TEXT,
            elapsed_s REAL, accepted INTEGER NOT NULL DEFAULT 0,
            skipped_reason TEXT, created_at TEXT NOT NULL
        );
        INSERT INTO parse_attempts
            (paper_id, parsed_text_version, attempt_index, parser_used, created_at)
            VALUES (1, 2, 1, 'docling', '2026-01-01T00:00:00Z');
        """
    )
    conn.commit()
    assert "font_audit" not in _columns(conn, "parse_attempts")
    conn.close()

    reopened = ReviewDatabase("legacy_probe", data_root=tmp_path)
    try:
        assert "font_audit" in _columns(reopened._conn, "parse_attempts")
        row = reopened._conn.execute(
            "SELECT parser_used, font_audit FROM parse_attempts"
        ).fetchone()
        assert row[0] == "docling"
        assert row[1] is None, "a pre-existing row is NULL, never backfilled"
    finally:
        reopened.close()


# ── the round trip ───────────────────────────────────────────────────


def test_the_ledger_row_round_trips_the_json(db, small_pdf):
    pid = _paper(db)
    result = audit(small_pdf, "some exported text about suturing evaluation")
    payload = as_ledger_json(result)

    attempt = ParseAttempt(attempt_index=1, parser_used="docling", passed=True,
                           font_audit=payload)
    _insert_attempts(db, pid, "deadbeef", 2, [attempt], "2026-09-09T00:00:00Z")
    db._conn.commit()

    stored = db._conn.execute(
        "SELECT font_audit FROM parse_attempts WHERE paper_id = ?", (pid,)
    ).fetchone()[0]
    assert json.loads(stored) == payload
    assert set(payload) == _LEDGER_KEYS
    assert payload["estimator"] == ESTIMATOR


def test_an_unaudited_attempt_stores_null_not_an_empty_object(db):
    """NULL means nobody looked. `{}` would read as "looked, found nothing"."""
    pid = _paper(db)
    attempt = ParseAttempt(attempt_index=1, parser_used="docling",
                           skipped_reason="vision skipped: too many pages")
    _insert_attempts(db, pid, "deadbeef", 2, [attempt], "2026-09-09T00:00:00Z")
    db._conn.commit()
    stored = db._conn.execute(
        "SELECT font_audit FROM parse_attempts WHERE paper_id = ?", (pid,)
    ).fetchone()[0]
    assert stored is None


def test_the_structure_payload_has_the_same_keys_and_says_why(small_pdf):
    """A total-failure record must be readable by the same reader as a full one."""
    payload = structure_ledger_json(font_structure(small_pdf), "no_text_produced")
    assert set(payload) == _LEDGER_KEYS
    assert payload["reason"] == "no_text_produced"
    assert payload["font_exposure_per_kchar"] is None, "absent, never 0.0"
    assert payload["exposure_lo"] is None and payload["exposure_hi"] is None
    assert payload["total_fonts"] >= 1


def test_the_font_list_is_filtered_to_classified_rows(small_pdf):
    """415 declares 1,647 font objects to describe seven damaged characters."""
    rows = font_structure(small_pdf)
    payload = structure_ledger_json(rows, "no_text_produced")
    assert payload["total_fonts"] == len(rows)
    assert all(f["klass"] for f in payload["fonts"])
    assert len(payload["fonts"]) == sum(1 for r in rows if r.klass)


# ── the timeout path ─────────────────────────────────────────────────


def test_a_timed_out_alignment_records_the_reason_not_a_zero(signature_pdf_6p):
    """Deadline zero: nothing is aligned, and the row says so rather than 0.0."""
    result = audit(signature_pdf_6p, "exported text", deadline_s=0.0)
    assert result.sig_chars_pdf > 0, "fixture must carry signature characters"
    assert result.reason is not None
    assert result.reason.startswith("alignment_timeout")
    assert result.silent_in_text_lo == 0 and result.silent_in_text_hi == 0


def test_the_timeout_reason_names_how_many_pages_were_reached(signature_pdf_6p):
    """Ruling 6: overshoot is bounded by one page, so the row says which page.

    The count is READ OUT OF THE PRODUCED STRING and checked against the
    document, rather than compared with a reason string typed into the test —
    a fixture carrying the text another component is supposed to produce tests
    nothing but itself.
    """
    result = audit(signature_pdf_6p, "exported text", deadline_s=0.0)
    match = re.search(r"alignment_timeout: (\d+) of (\d+) pages", result.reason)
    assert match, f"reason does not name the page counts: {result.reason!r}"
    reached, total = int(match.group(1)), int(match.group(2))
    assert reached == 0, "a zero deadline aligns nothing"
    assert total == 6, "all six pages carry silent characters"
    assert "neither placed nor dropped" in result.reason


def test_a_generous_deadline_measures_everything_and_sets_no_reason(signature_pdf_6p):
    """The other branch. Without it, a permanently-timing-out audit would pass."""
    result = audit(signature_pdf_6p, "exported text", deadline_s=600.0)
    assert result.reason is None
    assert result.silent_in_text_hi + result.silent_on_dropped_pages == sum(
        f.codes_ge32 for f in result.fonts
    )


def test_a_timed_out_audit_leaves_the_criterion_unevaluated(signature_pdf_6p):
    """The verdict falls back to the four text criteria; None is not 0.0."""
    result = audit(signature_pdf_6p, "exported text", deadline_s=0.0)
    exposure = None if result.reason else result.font_exposure_per_kchar
    assert exposure is None
    verdict = assess("word " * 400, font_exposure_per_kchar=exposure)
    assert FONT_EXPOSURE not in verdict.criteria
    assert "font_exposure_per_kchar" not in verdict.metrics


def test_the_ledger_payload_carries_the_timeout_reason(signature_pdf_6p):
    payload = as_ledger_json(audit(signature_pdf_6p, "exported text", deadline_s=0.0))
    assert payload["reason"].startswith("alignment_timeout")
    assert payload["block_min"] == 12


# ── the coupling, pinned ─────────────────────────────────────────────


def test_font_exposure_dominates_glyph_density():
    """The fifth criterion counts the third's markers, by G1's formula.

    `FONT_EXPOSURE = (marked_in_text + silent_in_text_lo) / kchar` and
    `marked_in_text` is the same `RE_GLYPH` count `GLYPH_DENSITY` divides by the
    same denominator. So FONT_EXPOSURE >= GLYPH_DENSITY always, and a spec that
    relaxes one limit without the other does not relax the gate. That surprised
    an existing cascade test; it is recorded here so it surprises nobody twice.
    """
    text = ("Ordinary prose about robotic suturing. "
            "GLYPH&lt;c=3,font=/AB+Times&gt; " * 30)
    metrics = compute_metrics(text)
    glyph = metrics["glyph_density_per_kchar"]
    assert glyph > 0

    marked = metrics["glyph_artifacts"]
    exposure = round(1000.0 * (marked + 0) / len(text), 3)
    assert exposure >= glyph - 0.001

    v = assess(text, font_exposure_per_kchar=exposure)
    assert GLYPH_DENSITY in v.criteria
    assert FONT_EXPOSURE in v.criteria


def test_the_spec_carries_the_new_limit_and_matches_the_engine_default():
    """A spec default that disagreed with the engine would make the gate depend
    on whether a review happened to declare the section."""
    assert ParseQuality().font_exposure_per_kchar_max == \
        Thresholds().font_exposure_per_kchar_max
    assert Thresholds.from_mapping(ParseQuality().model_dump()) == Thresholds()


# ── the cascade ──────────────────────────────────────────────────────


def test_a_judged_attempt_records_its_audit(db, small_pdf):
    """The wiring, end to end: parse a PDF and read the column back."""
    from engine.parsers.pdf_parser import parse_pdf

    pid = _paper(db)
    with patch("engine.parsers.pdf_parser.parse_with_docling",
               return_value="Autonomous robotic suturing evaluation. " * 40):
        parse_pdf(str(small_pdf), pid, "test_font_ledger", db)

    row = db._conn.execute(
        "SELECT font_audit FROM parse_attempts WHERE paper_id = ? "
        "ORDER BY attempt_index", (pid,)
    ).fetchone()
    assert row[0] is not None, "a judged attempt must carry an audit"
    payload = json.loads(row[0])
    assert set(payload) == _LEDGER_KEYS
    assert payload["reason"] is None
    assert payload["font_exposure_per_kchar"] == 0.0, "clean text, measured"


def test_a_total_failure_records_the_structure_half(db, small_pdf):
    """A parse that produced nothing still records which fonts the PDF declares.

    Contract 7 kept the attempt rows for a wholly failed parse; this keeps the
    half of the audit that needs no text. `font_exposure_per_kchar` is None on
    these rows — the criterion was never evaluated, and the reason says why.
    """
    from engine.parsers.pdf_parser import parse_pdf

    pid = _paper(db)
    with patch("engine.parsers.pdf_parser.parse_with_docling", return_value="  "), \
         patch("engine.parsers.pdf_parser.parse_with_pymupdf", return_value="  "), \
         patch("engine.parsers.pdf_parser.parse_with_docling_ocr", return_value="  "), \
         patch("engine.parsers.pdf_parser.parse_with_vision", return_value="  "):
        with pytest.raises(ValueError, match="all parsers returned empty text"):
            parse_pdf(str(small_pdf), pid, "test_font_ledger", db)

    rows = db._conn.execute(
        "SELECT font_audit FROM parse_attempts WHERE paper_id = ?", (pid,)
    ).fetchall()
    assert rows, "the attempt rows themselves must survive (Contract 7)"
    payloads = [json.loads(r[0]) for r in rows if r[0] is not None]
    assert payloads, "a total failure must still record the font structure"
    for payload in payloads:
        assert payload["reason"] == "no_text_produced"
        assert payload["font_exposure_per_kchar"] is None
        assert payload["total_fonts"] >= 1
        assert payload["sig_chars_pdf"] == 0


def test_an_audit_that_raises_does_not_fail_the_parse(db, small_pdf):
    """A measurement of the parse must not become part of it.

    A malformed font table is a fact about the PDF, not a reason to throw away a
    parse that worked. The criterion is left UNEVALUATED and the row says so.
    """
    from engine.parsers.pdf_parser import parse_pdf

    pid = _paper(db)
    with patch("engine.parsers.pdf_parser.parse_with_docling",
               return_value="Autonomous robotic suturing evaluation. " * 40), \
         patch("engine.parsers.font_audit.audit",
               side_effect=RuntimeError("font table exploded")):
        result = parse_pdf(str(small_pdf), pid, "test_font_ledger", db)

    assert result.accepted_parser == "docling"
    row = db._conn.execute(
        "SELECT font_audit, failures FROM parse_attempts WHERE paper_id = ?", (pid,)
    ).fetchone()
    payload = json.loads(row[0])
    assert payload["reason"].startswith("audit_error")
    assert payload["font_exposure_per_kchar"] is None
    assert FONT_EXPOSURE not in [f[0] for f in json.loads(row[1])]
