"""Text-level exposure of the Identity-H / no-ToUnicode font signature.

**The question this answers, and why the three text metrics cannot.**
`parse_quality`'s `GLYPH_DENSITY`, `REPLACEMENT_DENSITY` and `SHATTERED` all read
the exported markdown, and none of them can see a character that is *wrong but
valid*. PARSE-GATE-09 measured why: a Type0 font with `/Encoding /Identity-H`,
no `/ToUnicode`, and an embedded program stripped of both `cmap` and `post`
carries nothing that maps a code to Unicode, and Docling responds by writing
``GLYPH<c=N,font=/...>`` for a CID **below 32** and the raw ``chr(CID)`` --
silently, unmarked, indistinguishable from prose -- for a CID at or above 32.
586 carries 542 marked and 492 silent; 719 carries 5,472 and 22,057.

**Why font structure alone is not the answer either.** PARSE-GATE-10 swept all
190 corpus PDFs and found 11 carrying the signature, but coverage measured at the
PDF is not exposure measured in the text: paper 670 renders 907 signature
characters (3.05% of its text layer) of which **three** reach `670_v2.md`,
because its SimSun subsets set figure labels that Docling drops. Font structure
predicts where damage *can* come from; only the exported text says whether it
arrived.

**The estimator: alignment, not n-grams (FONT-AUDIT-01 ruling 1).** The obvious
approach -- take a silent character's neighbours and look them up in the
markdown -- was built and measured before being rejected. At the one-character
window it scores **54.2% precision at 13.1% recall** on 586: short keys collide
across the document (`' A '` matches 18 sites), and worse, corrupted text repeats
its own token shapes, so a key lifted from ``WCGNTCGLX`` lands confidently on an
*earlier* ``CGNX``. Widening the window buys precision only by giving up recall
(95.8% at 4.7% by w=6). So the page is aligned to the markdown as a whole and a
silent character is IN_TEXT if it falls inside a matched block. On 586 that
places 27.6% of silent characters while placing 92.6% of all emitted characters
-- the gap is the finding, not an error: silent characters really are
concentrated in the regions Docling drops.

**The estimate is an interval, never a point.** Alignment misses short table
cells and captions, so the aligned count is a floor, not a truth. `..._lo` is
what alignment placed; `..._hi` charges every silent character on any page that
aligned at all; `silent_on_dropped_pages` is the remainder, on pages the exporter
did not reach. `lo <= hi`, and `hi + dropped == ` the signature silent total, by
construction.

Pure: no database, no writes, no network, no model calls. Reads one PDF through
PyMuPDF and one string. Imports nothing from `analysis/` and nothing from
`engine.core.database`.
"""

from __future__ import annotations

import difflib
import html
import io
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Sequence

import fitz  # PyMuPDF
from fontTools.cffLib import CFFFontSet
from fontTools.pens.boundsPen import BoundsPen
from fontTools.ttLib import TTFont

from engine.parsers.markers import RE_GLYPH, RE_GLYPH_ATTRIBUTED

__all__ = [
    "SIGNATURE", "UNRESOLVING", "ESTIMATOR",
    "ALIGN_BLOCK_MIN", "MUPDF_BASEFONT_MAX", "FontRow", "PaperAudit",
    "audit", "font_structure", "as_ledger_json", "structure_ledger_json",
]

#: Type0 + Identity-* encoding + no `/ToUnicode`. The structural signature
#: PARSE-GATE-09 found behind every corrupted font in 586, 699 and 719, and
#: PARSE-GATE-10 confirmed across the corpus: 88 of 88 matching objects are
#: embedded, subset, and stripped of both `cmap` and `post`, without exception.
SIGNATURE = "type0_identity_no_tounicode"

#: Type0 + Identity-* **with** a `/ToUnicode` that nonetheless fails to resolve
#: -- the U+FFFF/U+FFFE "no glyph" convention. A structural test alone reports
#: these clean, which is why the class is named rather than folded in: it is the
#: signature's false negative, and it is not hypothetical. `MicrosoftYaHei-Bold`
#: in paper 670 is the measured instance, 125 characters (PARSE-GATE-10 §8.1).
#: Unlike `SIGNATURE`, it cannot be decided from the font dictionary -- a font is
#: only UNRESOLVING once at least one of its characters has actually failed.
UNRESOLVING = "type0_identity_with_tounicode"

#: Stamped into every `PaperAudit` so a stored row says which estimator produced
#: it. A number whose derivation is not recorded beside it is a number the next
#: reader has to trust rather than check.
ESTIMATOR = "alignment_v1"

#: Shortest run of agreeing characters counted as an alignment. Chosen at 12 over
#: 8 and 16 by measurement on 586, not by taste -- see the FONT-AUDIT-01 read-out
#: §R1. Below ~8 the matcher starts joining unrelated whitespace-and-punctuation
#: runs; above ~16 it drops real table cells.
ALIGN_BLOCK_MIN = 12

#: Stands in for a marker on both sides of the comparison, so that a
#: `GLYPH<...>` in the markdown aligns against the character that produced it
#: rather than against 20-odd characters of literal marker text. Chosen outside
#: every plane a PDF can render.
_MARK = "\x00"

_WS = re.compile(r"\s+")
_SUBSET_TAG = re.compile(r"^[A-Z]{6}\+")
_FONTFILE = re.compile(r"/FontFile([23]?)[\s/\[<]")
_DESCENDANT = re.compile(r"/DescendantFonts\s*\[?\s*(\d+)\s+0\s+R")
_BASEFONT = re.compile(r"/BaseFont\s*/([^\s/\]>]+)")
_FONTDESC = re.compile(r"/FontDescriptor\s+(\d+)\s+0\s+R")
#: `/ToUnicode` whose value is the null object. PDF 32000-1 §7.3.9: "A null
#: object ... shall be equivalent to the absence of the entry", so a font
#: carrying `/ToUnicode null` has no ToUnicode CMap and IS the signature. A
#: substring test for the key alone would call it clean.
_TOUNICODE_NULL = re.compile(r"/ToUnicode\s+null\b")

#: MuPDF truncates a `/BaseFont` to this many characters **before** the subset
#: tag is stripped, and `get_texttrace` reports what is left. Measured against
#: paper 670, where the rule reproduces all 59 reported names and 30 and 32 do
#: not (53/59 and 56/59): `CFDIMI+RpsvssMicrosoftYaHei-Bold-GBK-EUC-H` (42
#: characters) is reported as `RpsvssMicrosoftYaHei-Bol` (24) because the
#: seven-character tag is inside the budget. The visible length therefore depends
#: on the tag, which is why this constant is the *basefont* limit and not a
#: name limit.
#:
#: It matters because that font is a SIGNATURE font. A join on exact name
#: equality drops its characters **silently** -- the audit still lists the font
#: in its inventory and reports none of its damage, which is the worst shape a
#: miscount can take. PARSE-GATE-10 §8.1 used an exact join and charged those
#: characters to its unattributed residual instead.
MUPDF_BASEFONT_MAX = 31


def _has_tounicode(parent: str) -> bool:
    return "/ToUnicode" in parent and not _TOUNICODE_NULL.search(parent)


def _trace_keys(basefont: str) -> tuple[str, ...]:
    """Every form of `basefont` a `get_texttrace` span might report.

    The tag-stripped full name, plus the tag-stripped truncation MuPDF applies.
    Registering both means the join succeeds whichever the renderer hands back,
    and it collapses to one key whenever the name is short enough -- which is
    every font in 586, 699 and 719, and is why the defect stayed invisible until
    a paper with long CJK names was audited.
    """
    full = _strip_tag(basefont)
    short = _strip_tag(basefont[:MUPDF_BASEFONT_MAX])
    return (full,) if short == full else (full, short)


@dataclass(frozen=True)
class FontRow:
    """One font object, its dictionary facts, and what it rendered.

    `name` is the basefont with its six-letter subset tag stripped, because that
    is what `get_texttrace` reports and therefore the only key the character
    scan can join on. `basefont` keeps the tag. The two are separate fields
    because a document can carry two objects that strip to the same `name` --
    586 has both `GPONNK+TimesNewRomanPSMT` (Type0, signature) and
    `GPONCD+TimesNewRomanPSMT` (TrueType/WinAnsi, clean) -- and only `basefont`
    tells them apart.
    """

    name: str
    basefont: str
    subtype: str
    encoding: str
    has_tounicode: bool
    embedded: bool
    program: str            # "sfnt" | "cff" | "none"
    has_cmap: bool | None
    has_post: bool | None
    pages: tuple[int, ...]
    klass: str              # SIGNATURE | UNRESOLVING | ""
    chars_pdf: int          # characters rendered in this font that failed to resolve
    codes_lt32: int         # ...of which Docling would mark
    codes_ge32: int         # ...of which Docling writes silently as chr(code)
    space_recoverable: int


@dataclass(frozen=True)
class PaperAudit:
    """Per-paper summary. Every count is a measurement; the two exposures are an
    interval and must be read as one."""

    signature_fonts: int
    unresolving_fonts: int
    total_fonts: int
    sig_chars_pdf: int
    marked_in_text: int
    marked_unattributed: int
    silent_in_text_lo: int
    silent_in_text_hi: int
    silent_on_dropped_pages: int
    space_recoverable: int
    exported_chars: int
    exposure_lo: float
    exposure_hi: float
    estimator: str
    #: The alignment parameter this result was produced with, carried so a stored
    #: row can be compared with a later one rather than assumed comparable.
    block_min: int = ALIGN_BLOCK_MIN
    #: `None` when every page was measured. Otherwise why the measurement is
    #: incomplete -- currently only `alignment_timeout`, whose text names how
    #: many pages were reached. **When it is set, `silent_in_text_lo` and
    #: `_hi` are floors over the pages that WERE aligned and the identity
    #: `hi + dropped == silent total` does not hold**, because the remaining
    #: pages were neither placed nor dropped: nobody looked.
    reason: str | None = None
    fonts: tuple[FontRow, ...] = field(default_factory=tuple)

    @property
    def font_exposure_per_kchar(self) -> float:
        """Marked plus placed-silent characters per thousand exported characters.

        The FLOOR, deliberately: `_lo` and not `_hi`, because a gate criterion
        that can fail a document on an upper bound fails it on characters nobody
        located. `_hi` is telemetry and stays telemetry.
        """
        total = self.marked_in_text + self.silent_in_text_lo
        return round(1000.0 * total / self.exported_chars, 3) if self.exported_chars else 0.0


# ── normalisation ────────────────────────────────────────────────────


def _normalise(s: str) -> tuple[str, list[int]]:
    """Collapse whitespace runs to one space; return the text and an index map.

    Docling inserts spaces around dropped and marked runs -- the PDF emits
    ``WCEN CFN`` where `586_v2.md` holds ``WCEN  CFN`` -- so a comparison that
    respects run length compares the exporter's layout choices rather than its
    characters. The index map carries each surviving character back to its
    source offset, which is how a silent character's position survives the
    collapse.
    """
    out: list[str] = []
    idx: list[int] = []
    prev_ws = False
    for i, ch in enumerate(s):
        if ch.isspace():
            if prev_ws:
                continue
            out.append(" ")
            idx.append(i)
            prev_ws = True
        else:
            out.append(ch)
            idx.append(i)
            prev_ws = False
    return "".join(out), idx


def normalise_exported_text(text: str) -> str:
    """Markdown, prepared for comparison against a page.

    Order matters and is not interchangeable: markers are replaced **before**
    unescaping, because `html.unescape` would turn a stored
    ``GLYPH&lt;c=3,font=/X&gt;`` into ``GLYPH<c=3,font=/X>`` -- still a marker,
    but now one that has already been counted and would be matched twice.
    Unescaping second is what stops `&amp;` and `&gt;` from displacing a silent
    character's neighbours, which cost the rejected n-gram matcher eight
    percentage points of precision.
    """
    return _normalise(html.unescape(RE_GLYPH.sub(_MARK, text)))[0]


# ── font dictionaries ────────────────────────────────────────────────


def _strip_tag(name: str) -> str:
    return name[7:] if _SUBSET_TAG.match(name or "") else (name or "")


def _font_objects(doc: "fitz.Document") -> dict[int, dict[str, Any]]:
    """xref -> dictionary facts, for every font on every page.

    A composite font is resolved through `/DescendantFonts` to the descendant's
    `/BaseFont`, because that -- not the Type0 parent's -- is the name Docling
    reports and `get_texttrace` returns. 699's parent is
    `MinionPro-Regular-Identity-H` while its marker says
    `AOIIJB+MinionPro-Regular`; a reader matching on the parent finds nothing.
    """
    objs: dict[int, dict[str, Any]] = {}
    pages: dict[int, set[int]] = {}
    for pno in range(doc.page_count):
        for xref, _ext, subtype, basefont, _refname, enc in doc.get_page_fonts(pno):
            pages.setdefault(xref, set()).add(pno + 1)
            if xref in objs:
                continue
            parent = doc.xref_object(xref, compressed=False)
            m = _DESCENDANT.search(parent)
            desc = doc.xref_object(int(m.group(1)), compressed=False) if m else ""
            db = _BASEFONT.search(desc) if desc else None
            eff_basefont = db.group(1) if db else basefont
            fd = _FONTDESC.search(parent + "\n" + desc)
            fdobj = doc.xref_object(int(fd.group(1)), compressed=False) if fd else ""
            ff = _FONTFILE.search(fdobj)
            objs[xref] = {
                "xref": xref,
                "basefont": eff_basefont,
                "name": _strip_tag(eff_basefont),
                "subtype": subtype,
                "encoding": str(enc or ""),
                "has_tounicode": _has_tounicode(parent),
                "embedded": ff is not None,
            }
    for xref, ps in pages.items():
        objs[xref]["pages"] = tuple(sorted(ps))
    return objs


def _is_signature(o: dict[str, Any]) -> bool:
    """Type0 + Identity-* + no `/ToUnicode`.

    `/Differences` is deliberately not tested: it is a simple-font
    Encoding-dictionary key and **cannot** exist on a composite font, so a test
    for it would always pass and would read like evidence.
    """
    return (
        str(o["subtype"]).startswith("Type0")
        and o["encoding"].startswith("Identity")
        and not o["has_tounicode"]
    )


def _is_identity_with_tounicode(o: dict[str, Any]) -> bool:
    return (
        str(o["subtype"]).startswith("Type0")
        and o["encoding"].startswith("Identity")
        and o["has_tounicode"]
    )


def _program_facts(doc: "fitz.Document", xref: int) -> dict[str, Any]:
    """`cmap`/`post` presence and per-glyph outline emptiness.

    Two container shapes, both real in this corpus: an sfnt-wrapped TrueType
    (`/FontFile2`) that `TTFont` reads, and a **bare CID-keyed CFF**
    (`/FontFile3`) that `TTFont` refuses with `bad sfntVersion` and `CFFFontSet`
    reads -- 699's MinionPro is the second kind, and a reader that only tries
    `TTFont` records it as unparseable.
    """
    facts: dict[str, Any] = {
        "program": "none", "has_cmap": None, "has_post": None, "empty": {},
    }
    try:
        _name, _ext, _ftype, buf = doc.extract_font(xref)
    except Exception:
        return facts
    if not buf:
        return facts
    try:
        tt = TTFont(io.BytesIO(buf), lazy=False)
    except Exception:
        pass
    else:
        empty: dict[int, bool] = {}
        if "glyf" in tt:
            glyf = tt["glyf"]
            for gid, gname in enumerate(tt.getGlyphOrder()):
                try:
                    empty[gid] = glyf[gname].numberOfContours == 0
                except Exception:
                    pass
        facts.update(program="sfnt", has_cmap="cmap" in tt,
                     has_post="post" in tt, empty=empty)
        return facts
    try:
        cff = CFFFontSet()
        cff.decompile(io.BytesIO(buf), None)
        top = cff[cff.fontNames[0]]
        charstrings = top.CharStrings
        empty = {}
        for gid, gname in enumerate(top.charset):
            try:
                pen = BoundsPen(None)
                charstrings[gname].draw(pen)
                empty[gid] = pen.bounds is None
            except Exception:
                pass
        facts.update(program="cff", has_cmap=False, has_post=False, empty=empty)
    except Exception:
        pass
    return facts


# ── the character scan ───────────────────────────────────────────────


def _emitted(ucs: int, gid: int) -> str:
    """What Docling writes for one character.

    Verified against `586_v2.md` at five passages PARSE-GATE-09 §7 named: the
    reconstruction reproduces ``commonly only ████66``, ``in the order of
    ████6$)``, ``lies between █T██C6,`` and ``approximately ███C6 and the
    resolution ███C`` exactly, so ``chr(gid)`` is the emitted form and not an
    approximation of it.
    """
    if ucs != 0xFFFD:
        return chr(ucs)
    if gid < 32 or gid >= 0x110000:
        return _MARK
    return chr(gid)


def _page_chars(page: "fitz.Page") -> tuple[str, list[tuple[int, int, str, float]]]:
    """The page as Docling would emit it, plus (ucs, gid, font, advance) per character.

    The advance is carried because the space test needs it: an empty outline
    alone also describes `.notdef` and a zero-width mark, and neither is a word
    space. PARSE-GATE-09 §5 established the pair -- empty outline AND an advance
    equal to the font's space width -- and dropped the `spacewidth` half of the
    comparison only because MuPDF reports a fallback value for exactly the fonts
    under test (CambriaMath: 4.863 against a true 2.193). What survives is
    "blank, but it moves the pen".
    """
    out: list[str] = []
    meta: list[tuple[int, int, str, float]] = []
    for span in page.get_texttrace():
        font = span["font"]
        for ucs, gid, _origin, bbox in span["chars"]:
            out.append(_emitted(ucs, gid))
            meta.append((ucs, gid, font, bbox[2] - bbox[0]))
    return "".join(out), meta


def _aligned_spans(page_norm: str, md_norm: str, block_min: int) -> list[tuple[int, int]]:
    matcher = difflib.SequenceMatcher(None, page_norm, md_norm, autojunk=False)
    return [
        (b.a, b.a + b.size)
        for b in matcher.get_matching_blocks()
        if b.size >= block_min
    ]


# ── public entry point ───────────────────────────────────────────────


def audit(
    pdf_path: str | Path,
    exported_text: str,
    *,
    block_min: int = ALIGN_BLOCK_MIN,
    deadline_s: float | None = None,
) -> PaperAudit:
    """Audit one PDF against the markdown that was exported from it.

    Only pages carrying at least one signature character are aligned. That is not
    an optimisation with a correctness cost -- a page with no signature character
    contributes nothing to `_lo`, `_hi` or `_dropped` by definition -- and it is
    what keeps paper 415 (728 pages, 1.44M characters, seven signature
    characters) from costing a full-document alignment per page.

    `deadline_s` bounds the alignment phase. **It is checked BEFORE each page,
    never during one**, because `difflib.SequenceMatcher.get_matching_blocks` is
    a single uninterruptible call -- so the real bound is the deadline plus one
    page's alignment, which on the worst document in this corpus (415) is about
    70 s. On expiry the remaining pages are left unmeasured, `reason` is set, and
    the caller is expected to treat the exposure as unknown rather than as low.
    The pages already aligned keep their counts; they are a floor, and the reason
    string says how many pages produced it.
    """
    md_norm = normalise_exported_text(exported_text)
    doc = fitz.open(str(pdf_path))
    try:
        objs = _font_objects(doc)
        # Joined on every form the renderer might report, never on the
        # dictionary name alone; and accounting is keyed back to the DICTIONARY
        # name, so a truncated span and a full one accumulate into one row
        # rather than two half-rows.
        canonical = {
            k: o["name"]
            for o in objs.values() if _is_signature(o)
            for k in _trace_keys(o["basefont"])
        }
        canonical_cand = {
            k: o["name"]
            for o in objs.values() if _is_identity_with_tounicode(o)
            for k in _trace_keys(o["basefont"])
        }
        sig_names = set(canonical)
        cand_names = set(canonical_cand)
        programs: dict[str, dict[str, Any]] = {}
        for o in objs.values():
            if o["name"] in sig_names and o["name"] not in programs:
                programs[o["name"]] = _program_facts(doc, o["xref"])

        # Pass 1 — characters, per page, without alignment.
        per_font: dict[str, dict[str, int]] = {}
        unresolving_seen: set[str] = set()
        page_rows: list[dict[str, Any]] = []
        for pno in range(doc.page_count):
            raw, meta = _page_chars(doc[pno])
            sig_positions: list[tuple[int, int]] = []   # (src index, gid)
            for src, (ucs, gid, font, advance) in enumerate(meta):
                if ucs != 0xFFFD:
                    continue
                if font in cand_names:
                    unresolving_seen.add(canonical_cand.get(font, font))
                if font not in sig_names:
                    continue
                key = canonical.get(font, font)
                acc = per_font.setdefault(
                    key, {"chars": 0, "lt32": 0, "ge32": 0, "space": 0}
                )
                acc["chars"] += 1
                if gid < 32:
                    acc["lt32"] += 1
                else:
                    acc["ge32"] += 1
                    sig_positions.append((src, gid))
                if programs.get(key, {}).get("empty", {}).get(gid) and advance > 0:
                    acc["space"] += 1
            page_rows.append({"page": pno + 1, "raw": raw, "silent": sig_positions})

        # Pass 2 — align only the pages that carry silent signature characters.
        lo = hi = dropped = 0
        reason: str | None = None
        started = time.monotonic()
        aligned_pages = 0
        to_align = [r for r in page_rows if r["silent"]]
        for row in to_align:
            if deadline_s is not None and time.monotonic() - started > deadline_s:
                reason = (
                    f"alignment_timeout: {aligned_pages} of {len(to_align)} "
                    f"pages with silent characters aligned in {deadline_s:g}s; "
                    "the remainder were neither placed nor dropped"
                )
                break
            aligned_pages += 1
            page_norm, idx = _normalise(row["raw"])
            inverse: dict[int, int] = {}
            for norm_i, src_i in enumerate(idx):
                inverse.setdefault(src_i, norm_i)
            spans = _aligned_spans(page_norm, md_norm, block_min)
            n_silent = len(row["silent"])
            if not spans:
                dropped += n_silent
                continue
            hi += n_silent
            for src, _gid in row["silent"]:
                pos = inverse.get(src)
                if pos is not None and any(a <= pos < b for a, b in spans):
                    lo += 1

        fonts = _font_rows(objs, sig_names, unresolving_seen, programs, per_font)
        marked = RE_GLYPH.findall(exported_text)
        attributed = RE_GLYPH_ATTRIBUTED.findall(exported_text)
        sig_chars = sum(v["chars"] for v in per_font.values())
        exported_chars = len(exported_text)
        return PaperAudit(
            signature_fonts=sum(1 for o in objs.values() if _is_signature(o)),
            unresolving_fonts=len(unresolving_seen),
            total_fonts=len(objs),
            sig_chars_pdf=sig_chars,
            marked_in_text=len(marked),
            marked_unattributed=len(marked) - len(attributed),
            silent_in_text_lo=lo,
            silent_in_text_hi=hi,
            silent_on_dropped_pages=dropped,
            space_recoverable=sum(v["space"] for v in per_font.values()),
            exported_chars=exported_chars,
            exposure_lo=_ratio(len(marked) + lo, exported_chars),
            exposure_hi=_ratio(len(marked) + hi, exported_chars),
            estimator=ESTIMATOR,
            block_min=block_min,
            reason=reason,
            fonts=fonts,
        )
    finally:
        doc.close()


def font_structure(pdf_path: str | Path) -> tuple[FontRow, ...]:
    """The font inventory alone -- no character scan, no alignment, no text.

    This is what can be known about a PDF whose parse produced nothing to
    measure: which fonts it declares, whether each is embedded and subset,
    whether its program still carries a `cmap` or a `post`, and whether it
    matches the signature. Every character count on the returned rows is 0,
    because none was counted.

    **`UNRESOLVING` can never appear here, and that is a property of the class,
    not a gap in this function.** A font is UNRESOLVING only once one of its
    characters has actually failed to resolve despite a `/ToUnicode` being
    present, which is a fact about rendering and not about the dictionary. Rows
    that would qualify carry `klass == ""` here; `audit()` is what can promote
    them.

    Cost is sub-second even on the worst document in the corpus: 0.17 s for
    paper 415's 1,647 font objects across 728 pages, against 212 s for a full
    `audit()` of the same file, essentially all of which is alignment.
    """
    doc = fitz.open(str(pdf_path))
    try:
        objs = _font_objects(doc)
        programs: dict[str, dict[str, Any]] = {}
        for o in objs.values():
            if _is_signature(o) and o["name"] not in programs:
                programs[o["name"]] = _program_facts(doc, o["xref"])
        return _font_rows(objs, set(), set(), programs, {})
    finally:
        doc.close()


def as_ledger_json(result: PaperAudit) -> dict[str, Any]:
    """The `parse_attempts.font_audit` payload for an audited attempt.

    `fonts` is filtered to classified rows. Unfiltered it would store **1,647
    font objects for paper 415**, almost all of them irrelevant Type1 subsets, to
    describe seven damaged characters; `total_fonts` carries the full count so
    nothing is lost but the noise.
    """
    return {
        "estimator": result.estimator,
        "block_min": result.block_min,
        "reason": result.reason,
        "font_exposure_per_kchar": result.font_exposure_per_kchar,
        "signature_fonts": result.signature_fonts,
        "unresolving_fonts": result.unresolving_fonts,
        "total_fonts": result.total_fonts,
        "sig_chars_pdf": result.sig_chars_pdf,
        "marked_in_text": result.marked_in_text,
        "marked_unattributed": result.marked_unattributed,
        "silent_in_text_lo": result.silent_in_text_lo,
        "silent_in_text_hi": result.silent_in_text_hi,
        "silent_on_dropped_pages": result.silent_on_dropped_pages,
        "space_recoverable": result.space_recoverable,
        "exported_chars": result.exported_chars,
        "exposure_lo": result.exposure_lo,
        "exposure_hi": result.exposure_hi,
        "fonts": [_font_json(f) for f in result.fonts if f.klass],
    }


def structure_ledger_json(rows: Sequence[FontRow], reason: str) -> dict[str, Any]:
    """The payload for an attempt that produced no text to measure.

    Every count is 0 and `font_exposure_per_kchar` is **absent, not zero**: this
    row records which fonts the PDF declares, and says in `reason` why that is
    all it records.
    """
    return {
        "estimator": ESTIMATOR,
        "block_min": None,
        "reason": reason,
        "font_exposure_per_kchar": None,
        "signature_fonts": sum(1 for r in rows if r.klass == SIGNATURE),
        "unresolving_fonts": 0,
        "total_fonts": len(rows),
        "sig_chars_pdf": 0,
        "marked_in_text": 0,
        "marked_unattributed": 0,
        "silent_in_text_lo": 0,
        "silent_in_text_hi": 0,
        "silent_on_dropped_pages": 0,
        "space_recoverable": 0,
        "exported_chars": 0,
        "exposure_lo": None,
        "exposure_hi": None,
        "fonts": [_font_json(f) for f in rows if f.klass],
    }


def _font_json(f: FontRow) -> dict[str, Any]:
    return {
        "name": f.name, "basefont": f.basefont, "klass": f.klass,
        "subtype": f.subtype, "encoding": f.encoding,
        "embedded": f.embedded, "program": f.program,
        "has_cmap": f.has_cmap, "has_post": f.has_post,
        "pages": list(f.pages), "chars_pdf": f.chars_pdf,
        "codes_lt32": f.codes_lt32, "codes_ge32": f.codes_ge32,
        "space_recoverable": f.space_recoverable,
    }


def _ratio(n: int, d: int) -> float:
    return round(100.0 * n / d, 4) if d else 0.0


def _font_rows(
    objs: dict[int, dict[str, Any]],
    sig_names: set[str],
    unresolving: set[str],
    programs: dict[str, dict[str, Any]],
    per_font: dict[str, dict[str, int]],
) -> tuple[FontRow, ...]:
    rows: list[FontRow] = []
    counted: set[str] = set()
    for o in sorted(objs.values(), key=lambda d: (d["name"], d["xref"])):
        name = o["name"]
        is_sig = _is_signature(o)
        klass = SIGNATURE if is_sig else (UNRESOLVING if name in unresolving else "")
        prog = programs.get(name, {})
        # Character counts belong to the NAME, and several objects can share one
        # (719 re-embeds `JGFKKL+TimesNewRoman` once per page). Charging the
        # total to every object would multiply it, so the first row for a name
        # carries the count and the rest carry zero; the paper-level totals are
        # summed per name, never per row.
        acc = per_font.get(name, {}) if (is_sig and name not in counted) else {}
        if acc:
            counted.add(name)
        rows.append(FontRow(
            name=name,
            basefont=o["basefont"],
            subtype=str(o["subtype"]),
            encoding=o["encoding"],
            has_tounicode=o["has_tounicode"],
            embedded=o["embedded"],
            program=prog.get("program", "none"),
            has_cmap=prog.get("has_cmap"),
            has_post=prog.get("has_post"),
            pages=o.get("pages", ()),
            klass=klass,
            chars_pdf=acc.get("chars", 0),
            codes_lt32=acc.get("lt32", 0),
            codes_ge32=acc.get("ge32", 0),
            space_recoverable=acc.get("space", 0),
        ))
    return tuple(rows)
