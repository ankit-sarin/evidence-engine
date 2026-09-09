"""Synthetic signature-font PDFs, shared by the FONT-AUDIT test modules.

A helper module rather than a conftest fixture because two test files need the
BUILDER, not a fixture instance, and because `tests/_live_db_guard.py` already
establishes that shared test names live in a module of their own (pytest loads
conftest as top-level `conftest`, so importing names from it gets a second copy).

Construction, and why each step is required — every one reproduces a property
measured on the real corpus:

1. PyMuPDF's `insert_text(..., fontfile=...)` embeds a font as **Type0 /
   Identity-H** with a `/ToUnicode`, which is the clean case;
2. `xref_set_key(..., "ToUnicode", "null")` removes the CMap — PDF 32000-1
   §7.3.9 makes a null value equivalent to an absent key;
3. the embedded program is reopened with `fontTools`, its `cmap` and `post`
   tables deleted, and written back with `update_stream`.

After step 3 MuPDF resolves every character in that font to U+FFFD, exactly as
`get_texttrace` does for the real signature fonts in 586, 699 and 719. Steps 2
and 3 are both required: without 2 the ToUnicode still resolves the text, and
without 3 MuPDF falls back to the program\'s own `cmap`.
"""

from __future__ import annotations

import io
import re

import fitz
from fontTools.fontBuilder import FontBuilder
from fontTools.pens.ttGlyphPen import TTGlyphPen


_LETTERS = "abcdefghilmnoprstuvwy"


#: Filler glyphs pushing the letters above glyph id 32. Without them a font this
#: small puts every letter below 32, Docling would mark every character, and the
#: silent class -- the whole reason the module exists -- would be untestable. The
#: real subsets carry both classes at once (586: 589 marked, 492 silent), and a
#: fixture that carries only one of them tests half the code.
_PAD = 40


def _build_ttf(ps_name: str = "AuditProbe-Regular") -> bytes:
    """A minimal but valid TrueType font covering space + `_LETTERS`.

    `space` lands at glyph id 1 -- below 32, so Docling marks it -- and the
    letters land at 42 and above, so Docling writes them silently. One font,
    both classes.
    """
    order = (
        [".notdef", "space"]
        + [f"pad{i:02d}" for i in range(_PAD)]
        + ["zerowidth"]
        + [f"uni{ord(c):04X}" for c in _LETTERS]
    )
    fb = FontBuilder(unitsPerEm=1000, isTTF=True)
    fb.setupGlyphOrder(order)
    fb.setupCharacterMap(
        {0x20: "space", 0x7A: "zerowidth",
         **{ord(c): f"uni{ord(c):04X}" for c in _LETTERS}}
    )

    def box() -> object:
        pen = TTGlyphPen(None)
        pen.moveTo((50, 0))
        pen.lineTo((50, 600))
        pen.lineTo((450, 600))
        pen.lineTo((450, 0))
        pen.closePath()
        return pen.glyph()

    def blank() -> object:
        return TTGlyphPen(None).glyph()

    glyphs = {".notdef": blank(), "space": blank(), "zerowidth": blank()}
    glyphs.update({f"pad{i:02d}": box() for i in range(_PAD)})
    glyphs.update({f"uni{ord(c):04X}": box() for c in _LETTERS})
    fb.setupGlyf(glyphs)
    # `zerowidth` is blank AND advances nothing: the negative case for the space
    # test. Without it "empty outline" alone would pass the test, and an empty
    # outline also describes `.notdef` and a combining mark.
    metrics = {g: (500, 50) for g in order}
    metrics["zerowidth"] = (0, 0)
    fb.setupHorizontalMetrics(metrics)
    fb.setupHorizontalHeader(ascent=800, descent=-200)
    fb.setupNameTable({"familyName": ps_name.split("-")[0], "styleName": "Regular",
                       "psName": ps_name})
    fb.setupOS2()
    fb.setupPost()
    out = io.BytesIO()
    fb.save(out)
    return out.getvalue()


def _break_font(doc: fitz.Document, xref: int) -> None:
    """Turn one embedded Type0 font into the signature: no ToUnicode, no cmap/post."""
    doc.xref_set_key(xref, "ToUnicode", "null")
    _name, _ext, _ftype, program = doc.extract_font(xref)
    tt = fitz.TOOLS  # noqa: F841  (keeps the fitz import honest for linters)
    from fontTools.ttLib import TTFont

    font = TTFont(io.BytesIO(program), lazy=False)
    for table in ("cmap", "post"):
        if table in font:
            del font[table]
    stripped = io.BytesIO()
    font.save(stripped)

    flat = " ".join(doc.xref_object(xref, compressed=False).split())
    desc_xref = int(re.search(r"/DescendantFonts \[ (\d+) 0 R", flat).group(1))
    desc = " ".join(doc.xref_object(desc_xref, compressed=False).split())
    fd_xref = int(re.search(r"/FontDescriptor (\d+) 0 R", desc).group(1))
    fd = " ".join(doc.xref_object(fd_xref, compressed=False).split())
    ff_xref = int(re.search(r"/FontFile2 (\d+) 0 R", fd).group(1))
    doc.update_stream(ff_xref, stripped.getvalue())


def _rename_basefont(doc: fitz.Document, xref: int, new: str) -> None:
    """Give an embedded Type0 font a `/BaseFont` past MuPDF's report buffer.

    PyMuPDF clips the name it writes at embed time, so a long name has to be put
    back afterwards -- on the parent AND the descendant, since the audit reads
    the descendant's.
    """
    doc.xref_set_key(xref, "BaseFont", f"/{new}")
    flat = " ".join(doc.xref_object(xref, compressed=False).split())
    desc = int(re.search(r"/DescendantFonts \[ (\d+) 0 R", flat).group(1))
    doc.xref_set_key(desc, "BaseFont", f"/{new}")


def _make_pdf(tmp_path, text: str, *, broken: bool, clean_too: bool = False,
              ps_name: str = "AuditProbe-Regular", basefont: str | None = None,
              pages: int = 1, name: str | None = None):
    """Write a PDF and return (path, the text as Docling would emit it).

    `pages` repeats the same line on N pages, which is what a per-page deadline
    test needs: the audit aligns page by page, so a document with one page can
    never show a partial result.
    """
    ttf = tmp_path / "probe.ttf"
    ttf.write_bytes(_build_ttf(ps_name))

    doc = fitz.open()
    for _ in range(pages):
        page = doc.new_page()
        page.insert_text((60, 100), text, fontname="PR", fontfile=str(ttf), fontsize=11)
        if clean_too:
            page.insert_text((60, 140), "clean helvetica line",
                             fontname="helv", fontsize=11)
    raw = doc.tobytes()
    doc.close()

    doc = fitz.open("pdf", raw)
    for xref, _ext, subtype, _base, _ref, _enc in doc.get_page_fonts(0):
        if not str(subtype).startswith("Type0"):
            continue
        if basefont:
            _rename_basefont(doc, xref, basefont)
        if broken:
            _break_font(doc, xref)
    path = tmp_path / (name or ("broken.pdf" if broken else "clean.pdf"))
    doc.save(str(path))
    doc.close()

    doc = fitz.open(str(path))
    emitted = []
    for span in doc[0].get_texttrace():
        for ucs, gid, _o, _b in span["chars"]:
            if ucs != 0xFFFD:
                emitted.append(chr(ucs))
            elif gid < 32:
                emitted.append("\x00")
            else:
                emitted.append(chr(gid))
    doc.close()
    return path, "".join(emitted)
