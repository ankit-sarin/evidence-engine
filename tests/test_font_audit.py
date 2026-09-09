"""Tests for `engine.parsers.font_audit`.

**Fixtures are synthesised, not committed.** Every PDF these tests read is built
in `tmp_path` from a TrueType font this file constructs with
`fontTools.fontBuilder` -- no system font, no network, no binary in the repo, and
nothing whose provenance a later reader has to take on trust. The construction is
three steps, and each one reproduces a property measured on the real corpus:

1. PyMuPDF's `insert_text(..., fontfile=...)` embeds a font as **Type0 /
   Identity-H** with a `/ToUnicode`, which is the clean case;
2. `xref_set_key(..., "ToUnicode", "null")` removes the CMap -- PDF 32000-1
   §7.3.9 makes a null value equivalent to an absent key, and the audit must
   agree;
3. the embedded program is reopened with `fontTools`, its `cmap` and `post`
   tables deleted, and written back with `update_stream`.

After step 3 MuPDF resolves every character in that font to U+FFFD, which is
exactly what `get_texttrace` reports for the real signature fonts in 586, 699 and
719. Steps 2 and 3 are both required: without 2 the ToUnicode still resolves the
text, and without 3 MuPDF falls back to the program's own `cmap`.

A fixture that only carried the things the audit is supposed to find would let
`total_fonts` and the non-signature paths go untested, so `signature_pdf` also
embeds a **second, clean** font and the assertions name both.
"""

from __future__ import annotations

import subprocess
import sys

import fitz
import pytest

from tests._font_fixtures import _LETTERS, _PAD, _build_ttf, _make_pdf

from engine.parsers.font_audit import (
    ESTIMATOR,
    SIGNATURE,
    MUPDF_BASEFONT_MAX,
    UNRESOLVING,
    audit,
    font_structure,
    normalise_exported_text,
)

# The glyphs the synthetic font carries. `space` is deliberately outline-less:
# it is what the space-recovery test looks for, and it mirrors the real finding
# that every RECOVERABLE-BY-POSITION row in 586/699 was an empty outline with a
# non-zero advance (PARSE-GATE-09 §5).
@pytest.fixture
def clean_pdf(tmp_path):
    path, _ = _make_pdf(tmp_path, "an ordinary sentence of prose", broken=False)
    return path


@pytest.fixture
def signature_pdf(tmp_path):
    """A signature font AND a clean one on the same page."""
    return _make_pdf(
        tmp_path,
        "an ordinary sentence of prose about probes and cameras",
        broken=True,
        clean_too=True,
    )


# ── the audit itself ─────────────────────────────────────────────────


def test_a_pdf_without_signature_fonts_has_zero_exposure(clean_pdf):
    result = audit(clean_pdf, "an ordinary sentence of prose\n")
    assert result.signature_fonts == 0
    assert result.sig_chars_pdf == 0
    assert result.silent_in_text_lo == 0
    assert result.silent_in_text_hi == 0
    assert result.silent_on_dropped_pages == 0
    assert result.exposure_lo == 0.0
    assert result.exposure_hi == 0.0
    assert result.estimator == ESTIMATOR


def test_the_signature_is_detected_and_the_clean_font_is_not(signature_pdf):
    path, _ = signature_pdf
    result = audit(path, "irrelevant")
    assert result.signature_fonts == 1
    assert result.total_fonts >= 2, "the clean font must also be inventoried"
    klasses = {row.klass for row in result.fonts}
    assert SIGNATURE in klasses
    assert "" in klasses, "the clean font must not be classed as damaged"
    signature_row = next(r for r in result.fonts if r.klass == SIGNATURE)
    assert signature_row.has_tounicode is False, "/ToUnicode null must read as absent"
    assert signature_row.embedded is True
    assert signature_row.has_cmap is False
    assert signature_row.has_post is False
    assert signature_row.chars_pdf > 0
    # The fixture must carry BOTH damage classes, or half the module is untested
    # and nothing says so. `_PAD` is what puts the letters above glyph id 32;
    # this is the assertion that turns red if anyone removes it.
    assert signature_row.codes_lt32 > 0, "no marked characters in the fixture"
    assert signature_row.codes_ge32 > 0, "no silent characters in the fixture"
    assert signature_row.codes_lt32 + signature_row.codes_ge32 == signature_row.chars_pdf


def test_markers_in_the_exported_text_are_counted(signature_pdf):
    path, _ = signature_pdf
    exported = (
        "a caption GLYPH&lt;c=3,font=/ABCDEF+AuditProbe&gt; and "
        "another GLYPH<c=4,font=/ABCDEF+AuditProbe> here\n"
    )
    result = audit(path, exported)
    assert result.marked_in_text == 2
    assert result.marked_unattributed == 0


def test_the_bare_marker_form_is_counted_but_unattributed(signature_pdf):
    path, _ = signature_pdf
    exported = "a channel of GLYPH<31> 3.6 mm and GLYPH<c=9,font=/ABCDEF+AuditProbe>\n"
    result = audit(path, exported)
    assert result.marked_in_text == 2
    assert result.marked_unattributed == 1, "GLYPH<31> carries no font to charge it to"


def test_silent_characters_are_found_when_the_page_aligns(signature_pdf):
    """The exported text IS the emitted reconstruction, so alignment must place it."""
    path, emitted = signature_pdf
    exported = emitted.replace("\x00", " ")
    result = audit(path, exported)
    assert result.sig_chars_pdf > 0
    assert result.silent_in_text_lo > 0
    assert result.silent_on_dropped_pages == 0


def test_silent_characters_on_a_page_that_did_not_align_are_dropped(signature_pdf):
    """Docling exported something else entirely -- a figure region it discarded."""
    path, _ = signature_pdf
    result = audit(path, "wholly unrelated exported prose about other matters\n" * 4)
    assert result.silent_in_text_lo == 0
    assert result.silent_in_text_hi == 0
    assert result.silent_on_dropped_pages > 0


def test_the_exposure_interval_is_ordered_and_accounts_for_every_silent_char(
    signature_pdf,
):
    path, emitted = signature_pdf
    for exported in (emitted.replace("\x00", " "), "unrelated text entirely\n"):
        result = audit(path, exported)
        silent = sum(row.codes_ge32 for row in result.fonts)
        assert result.silent_in_text_lo <= result.silent_in_text_hi
        assert result.silent_in_text_hi + result.silent_on_dropped_pages == silent
        assert result.exposure_lo <= result.exposure_hi


def test_a_blank_glyph_that_advances_is_space_recoverable_and_one_that_does_not_is_not(
    tmp_path,
):
    """Blank outline AND a non-zero advance. Both halves, or the test is vacuous.

    The fixture text carries three spaces (blank, advance 500) and three `z`
    characters mapped to `zerowidth` (blank, advance 0). Only the spaces may be
    counted; `.notdef` and combining marks are blank too.
    """
    path, _ = _make_pdf(tmp_path, "a b c zzz", broken=True)
    result = audit(path, "irrelevant")
    row = next(r for r in result.fonts if r.klass == SIGNATURE)
    assert row.space_recoverable == result.space_recoverable
    assert result.space_recoverable == 3, (
        "expected the three spaces only; the three zero-advance blanks must not count"
    )


def test_an_identity_font_that_keeps_its_tounicode_is_not_the_signature(tmp_path):
    """Structure alone: with the CMap present the font is clean, whatever it renders."""
    path, _ = _make_pdf(tmp_path, "prose that resolves normally", broken=False)
    result = audit(path, "prose that resolves normally\n")
    assert result.signature_fonts == 0
    assert result.unresolving_fonts == 0
    assert all(row.klass != SIGNATURE for row in result.fonts)


def test_block_min_is_a_knob_and_a_larger_one_never_places_more(signature_pdf):
    path, emitted = signature_pdf
    exported = emitted.replace("\x00", " ")
    loose = audit(path, exported, block_min=6)
    tight = audit(path, exported, block_min=40)
    assert tight.silent_in_text_lo <= loose.silent_in_text_lo


# ── normalisation ────────────────────────────────────────────────────


def test_markers_are_replaced_before_html_is_unescaped():
    """Order matters: unescaping first would resurrect a marker already removed."""
    out = normalise_exported_text("x GLYPH&lt;c=3,font=/A+B&gt; y &amp; z")
    assert "GLYPH" not in out
    assert " & " in out, "entities outside markers must still be unescaped"


def test_whitespace_runs_are_collapsed():
    assert normalise_exported_text("a   b\n\n\tc") == "a b c"


# ── the dependency boundary (FONT-AUDIT-01 ruling 3) ─────────────────


def test_font_audit_does_not_import_the_analysis_lane():
    """`analysis/` is the study lane; an engine module must not drag it in.

    Checked in a subprocess because this test session has already imported half
    the tree, so `sys.modules` in-process proves nothing.
    """
    code = (
        "import sys; import engine.parsers.font_audit; "
        "leaked = sorted(m for m in sys.modules if m.split('.')[0] == 'analysis'); "
        "db = [m for m in sys.modules if m.startswith('engine.core.database')]; "
        "print(repr(leaked)); print(repr(db))"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    leaked, db = proc.stdout.strip().splitlines()
    assert leaked == "[]", f"font_audit pulled in the analysis lane: {leaked}"
    assert db == "[]", f"font_audit pulled in the database layer: {db}"


def test_markers_module_imports_nothing_of_ours():
    code = (
        "import sys; import engine.parsers.markers; "
        "print(sorted(m for m in sys.modules "
        "if m.split('.')[0] in {'analysis', 'fitz', 'fontTools'}))"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True
    )
    assert proc.stdout.strip() == "[]"


def test_the_unresolving_class_is_named_and_distinct():
    assert UNRESOLVING != SIGNATURE
    assert "tounicode" in UNRESOLVING and "tounicode" in SIGNATURE


def test_a_basefont_past_mupdfs_buffer_still_joins(tmp_path):
    """MuPDF clips `/BaseFont` before reporting it; the join must survive that.

    Measured on paper 670, whose `CFDIMI+RpsvssMicrosoftYaHei-Bold-GBK-EUC-H` is
    a SIGNATURE font reported as `RpsvssMicrosoftYaHei-Bol`. An exact-equality
    join drops its characters **silently** -- the inventory still lists the font,
    so nothing looks wrong, and PARSE-GATE-10 §8.1 charged them to its
    unattributed residual for exactly this reason.

    The first two assertions establish that the renderer really did clip the
    name. Without them this test would pass against a matcher that handles no
    truncation at all.
    """
    long_base = "QWERTY+ProbeFontWithADeliberatelyExcessiveBaseFontName"
    path, _ = _make_pdf(tmp_path, "an ordinary sentence of prose",
                        broken=True, basefont=long_base)

    doc = fitz.open(str(path))
    reported = {span["font"] for span in doc[0].get_texttrace()}
    doc.close()
    stripped_full = long_base.split("+", 1)[1]
    assert stripped_full not in reported, (
        "the renderer did not clip; this test is not exercising the path"
    )
    assert long_base[:MUPDF_BASEFONT_MAX].split("+", 1)[1] in reported

    result = audit(path, "irrelevant")
    assert result.signature_fonts == 1
    row = next(r for r in result.fonts if r.klass == SIGNATURE)
    assert row.name == stripped_full, "the row must carry the dictionary name"
    assert row.chars_pdf > 0, "a clipped span name lost the font's characters"
    assert result.sig_chars_pdf == row.chars_pdf
    assert result.space_recoverable > 0


# ── font_structure: the inventory without the scan (FONT-AUDIT-02) ───


def test_font_structure_classifies_the_signature_without_touching_the_text(
    tmp_path, monkeypatch
):
    """No character scan at all -- asserted by making one fatal.

    `get_texttrace` is the module's only route to a character, so a version of
    it that raises turns "does not scan" from a claim about the code into a
    property the test enforces. Reading the source instead would pass against a
    scan added tomorrow.
    """
    path, _ = _make_pdf(tmp_path, "an ordinary sentence of prose", broken=True,
                        clean_too=True)

    def explode(self, *a, **kw):
        raise AssertionError("font_structure must not read characters")

    monkeypatch.setattr(fitz.Page, "get_texttrace", explode)

    rows = font_structure(path)
    assert len(rows) >= 2, "the clean font must be inventoried too"
    signature = [r for r in rows if r.klass == SIGNATURE]
    assert len(signature) == 1
    row = signature[0]
    assert row.has_tounicode is False
    assert row.embedded is True
    assert row.has_cmap is False and row.has_post is False
    assert row.pages == (1,)


def test_font_structure_counts_nothing_because_it_counted_nothing(signature_pdf):
    """Every character field is 0 -- not "unknown", not carried over from audit."""
    path, _ = signature_pdf
    rows = font_structure(path)
    assert all(
        (r.chars_pdf, r.codes_lt32, r.codes_ge32, r.space_recoverable) == (0, 0, 0, 0)
        for r in rows
    )
    full = audit(path, "irrelevant")
    assert {r.basefont for r in rows} == {r.basefont for r in full.fonts}
    assert sum(r.chars_pdf for r in full.fonts) > 0, (
        "the audited run must count something, or this test compares two zeroes"
    )


def test_font_structure_never_reports_the_unresolving_class(tmp_path):
    """UNRESOLVING is a fact about rendering; structure alone cannot see it."""
    path, _ = _make_pdf(tmp_path, "prose that resolves normally", broken=False)
    assert all(r.klass != UNRESOLVING for r in font_structure(path))
