# PARSE-GATE-09 — read-out: font forensics on 586, 699, 719

**Date:** 2026-09-08 · **Machine:** DGX Spark · **Type:** diagnostic read-out, no engine code, no DB writes
**HEAD at start:** `f96d647`, tree clean apart from the held PARSE-GATE-08 read-out.

One question was measured: **can the unresolved glyphs in 586, 699 and 719 be mapped to Unicode
without OCR, from what the PDFs themselves contain?** The answer is a property of four font
dictionaries and their embedded font programs, and it had not been read. **No recommendation and
no ruling is offered here; that is the architect's.**

**Tools.** Project `.venv` only — PyMuPDF 1.27.1 (MuPDF 1.27.1) and fontTools 4.61.1, both already
installed. **No scratch venv, no install, `pip freeze` byte-identical before and after.** All PDF
access read-only; `review.db` opened `mode=ro` once, for the three `pdf_local_path` values.

| paper | id | PDF |
|---|---|---|
| 586 | EE-434 | `data/surgical_autonomy/pdfs/EE-434_Varghese_2017.pdf` |
| 699 | EE-547 | `data/surgical_autonomy/pdfs/EE-547_Han_2024.pdf` |
| 719 | EE-567 | `data/surgical_autonomy/pdfs/EE-567_Bauzano_2010.pdf` |

---

## 0. The one-line answer

**Every offending font in all three papers is a composite (Type0) font with `/Encoding
/Identity-H`, NO `/ToUnicode`, and an embedded font program stripped of BOTH its `cmap` and its
`post` table.** There is therefore no ToUnicode entry to read, no `/Differences` array to resolve
through the AGL (Identity-H CID fonts have no `/Differences` at all), and no glyph name or reverse
cmap in the font program. **Nothing in any of the three files maps a single one of these codes to
Unicode.** What the files *do* retain is the per-CID advance width (`/W`) and the glyph outlines.

---

## 1. Inferred-assumption resolutions (I1–I4)

| # | claim | verdict | source |
|---|---|---|---|
| **I1** | PyMuPDF exposes per-page font lists, and per-span font name + **raw character codes via `page.get_text("rawdict")`** | **CONCLUSION TRUE, NAMED MECHANISM FALSE** | `get_page_fonts(n)` gives the per-page font list (xref, ext, subtype, basefont, refname, encoding) — used throughout §2. But `rawdict`'s per-char dict carries only `{'origin','bbox','c','synthetic'}`, and `c` is the **already-resolved** character (here `'�'`) — **no raw code**. The raw code is exposed by **`page.get_texttrace()`**, whose `chars` entries are `(ucs, gid, origin, bbox)`; with `/CIDToGIDMap /Identity` (measured, all fonts) the `gid` **is** the CID **is** docling's `c=`. Font dictionaries come from `doc.xref_object(xref)`, font programs from `doc.extract_font(xref)`. **No other tool was needed** — fontTools 4.61.1 is already in `.venv`. |
| **I2** | the offending fonts are embedded (subset) in all three PDFs, and `/Encoding`, `/Differences`, `/ToUnicode`, `/FontFile` can be read | **TRUE for embedding/subsetting and readability; TWO OF THE FOUR KEYS DO NOT EXIST** | All offending fonts are embedded (`/FontFile2` for CIDFontType2, `/FontFile3` for 699's CIDFontType0) and subset (six-letter tag: `GPONNK+`, `AOIIJB+`, `JGFKKL+`, …). `/Encoding` is `/Identity-H` in every case. **`/ToUnicode` is absent from every offending font.** **`/Differences` does not exist and cannot** — it is a simple-font Encoding-dictionary key; a Type0/Identity-H font has no such dictionary. Full dictionaries quoted in §3. |
| **I3** | 699's `c=1` in MinionPro-Regular is a code the font maps to a space or a no-op glyph; measure advance and outline | **TRUE — and it is a space, on four independent measurements** | The embedded program is a **bare CID-keyed CFF** (`ROS = ('Adobe','Identity',0)`, 13 glyphs, charset `['.notdef','cid00001','cid00066',…]`). `cid00001`: **charstring width 227**, **`BoundsPen` bounds `None` — the outline is empty**. `/W [ 1 [ 227 ] ]` agrees. Measured span advance **2.262 pt at size 9.96 = 0.227 em**, and mupdf's own computed `spacewidth` for that span is **2.262 — equal to three decimal places**. 227/1000 is Minion Pro's space advance. Position: PARSE-GATE-08 measured all 419 as isolated singles in inter-word slots. |
| **I4** | 586's unresolved codes come from a small number of fonts (**one or two**), not scattered across all fonts on the page | **SUBSTANTIVE CLAIM TRUE; THE PARENTHETICAL COUNT IS FALSE — it is FOUR** | 542 markers over **4** fonts: `GPONNK+TimesNewRomanPSMT` 244, `GPONFG+TimesNewRomanPS-BoldMT` 149, `GPONOK+CambriaMath` 139, `GPONCE+SymbolMT` 10. The document carries **13** distinct fonts; 4 are affected and 9 are not. The four are **exactly** the document's four Type0/Identity-H/no-ToUnicode fonts — the set is bounded by a structural property, not by luck. Flagged under CONTRADICTION HANDLING: reported, not adapted to; nothing downstream was changed on the strength of it. |

---

## 2. F1 — font inventory

`emb` = embedded · `sub` = subset (six-letter tag) · `tU` = has `/ToUnicode` · `diff` = has
`/Differences` · **bold rows are the fonts that produce unresolved glyphs.**

### 586 — 8 pages, 13 distinct fonts

| xref | BaseFont | subtype | program | emb | sub | tU | diff | encoding | pages |
|---:|---|---|---|:--:|:--:|:--:|:--:|---|---|
| **22** | **GPONFG+TimesNewRomanPS-BoldMT** | **Type0 / CIDFontType2** | **`/FontFile2`** | **y** | **y** | **n** | **n** | **Identity-H** | **1** |
| **23** | **GPONNK+TimesNewRomanPSMT** | **Type0 / CIDFontType2** | **`/FontFile2`** | **y** | **y** | **n** | **n** | **Identity-H** | **1,3,5,6,8** |
| **24** | **GPONOK+CambriaMath** | **Type0 / CIDFontType2** | **`/FontFile2`** | **y** | **y** | **n** | **n** | **Identity-H** | **1–6,8** |
| **28** | **GPONCE+SymbolMT** | **Type0 / CIDFontType2** | **`/FontFile2`** | **y** | **y** | **n** | **n** | **Identity-H** | **4,5,6** |
| 25 | GPONHH+TimesNewRomanPS-BoldMT | TrueType | `/FontFile2` | y | y | n | n | WinAnsi | 1,2,4,5 |
| 26 | GPONEF+TimesNewRomanPS-BoldItalicMT | TrueType | `/FontFile2` | y | y | n | n | WinAnsi | 1 |
| 27 | GPONCD+TimesNewRomanPSMT | TrueType | `/FontFile2` | y | y | n | n | WinAnsi | 1–8 |
| 29 | GPOOHJ+Arial-BoldMT | TrueType | `/FontFile2` | y | y | n | n | WinAnsi | 1–8 |
| 30 | GPONMJ+TimesNewRomanPS-ItalicMT | TrueType | `/FontFile2` | y | y | n | n | WinAnsi | 1,2,4–8 |
| 31 | GPONKI+ArialMT | TrueType | `/FontFile2` | y | y | n | n | WinAnsi | 1–6 |
| 43 | GPPADP+CambriaMath | TrueType | `/FontFile2` | y | y | n | n | WinAnsi | 3,4 |
| 52 | GPPEAI+Arial-ItalicMT | TrueType | `/FontFile2` | y | y | n | n | WinAnsi | 5 |
| 32 | Helvetica | Type1 (standard 14) | — | **n** | n | n | n | WinAnsi | 1–8 |

**The split is exact: every Identity-H font in 586 produces unresolved glyphs; no WinAnsi font
does.** The WinAnsi TrueType fonts carry no ToUnicode either and do not need one — a base encoding
resolves them.

### 699 — 6 pages, 19 distinct fonts

| xref | BaseFont | subtype | program | emb | sub | tU | diff | encoding | pages |
|---:|---|---|---|:--:|:--:|:--:|:--:|---|---|
| **28** | **AOIIJB+MinionPro-Regular** (parent BaseFont `MinionPro-Regular-Identity-H`) | **Type0 / CIDFontType0** | **`/FontFile3` (bare CFF)** | **y** | **y** | **n** | **n** | **Identity-H** | **1** |
| 27 | VPMSVL+Calibri | Type0 / CIDFontType2 | `/FontFile2` | y | y | **y** | n | Identity-H | 1 |
| 82 | AOINCN+SegoeUI | Type0 / CIDFontType2 | `/FontFile2` | y | y | n | n | Identity-H | 4 |
| 83 | AOINEN+SimHei-GBK-EUC-H | Type0 / CIDFontType2 | `/FontFile2` | y | y | n | n | Identity-H | 4 |
| 29–32, 71, 72, 77–79, 88 | NimbusRomNo9L-{Medi,Regu,ReguItal,MediItal}, CMMI10, CMMI7, CMR10, CMR7, CMSY10, CMSY6 | Type1 / CFF | `/FontFile3` | y | y | **y** | **y** | (custom + `/Differences`) | 1–6 |
| 25, 26, 86, 87 | AOIIKC+Arial, AOIIKB+TimesNewRoman, AOIPAI+TimesNewRomanPSMT, AOIPAJ+TimesNewRomanPS-BoldMT | TrueType | `/FontFile2` | y | y | n | n | WinAnsi | 1,5,6 |
| 33 | Helvetica | Type1 (standard 14) | — | **n** | n | n | n | WinAnsi | 1–6 |

**699 is the instructive contrast.** The ten Type1/CFF fonts that carry the body text have BOTH
`/ToUnicode` AND `/Differences` and resolve perfectly. Two Identity-H fonts without ToUnicode
(SegoeUI, SimHei) sit inside figures on p4 and are unresolved but produce no docling marker (§4).
**One** font is responsible for the whole gate failure.

### 719 — 6 pages, 38 distinct font objects (14 distinct subset names)

Every one of the 36 Type0 objects is `CIDFontType2 / Identity-H / CIDToGIDMap Identity / no
ToUnicode / no Differences / `/FontFile2``; the same subset tag is re-embedded once per page
(e.g. `JGFKKL+TimesNewRoman` at xrefs 34, 47, 61, 70, 73, 83, 95). The only exceptions are the two
`MyriadPro` Type1/CFF fonts (xrefs 32, 35 — `/ToUnicode` **y**, `/Differences` **y**), the
`JGFLJO+ArialNarrow` TrueType/WinAnsi (xref 33), and the non-embedded `Helvetica` (xref 36).

| subset name (Type0, all identical structure) | pages |
|---|---|
| `JGFKKL+TimesNewRoman` | 1–6 |
| `JGFLCM+TimesNewRoman,Italic` | 1–6 |
| `JGFKPL+TimesNewRoman,Bold` | 1, 3 |
| `JGFLBL+Arial` | 1–6 |
| `JGFLGL+Arial,Italic` | 1–5 |
| `JGFMCP+MTExtra` | 2–5 |
| `JGFNFM+Symbol` | 3, 4, 5 |
| `JGFKNK+TimesNewRoman,BoldItalic` | 1 |

**Whole-document tally.** 586: **4 of 13** fonts affected. 699: **1 of 19**. 719: **all 36 Type0
objects**, i.e. the entire text body.

---

## 3. What the font dictionaries actually say

Quoted verbatim from `doc.xref_object` (whitespace normalised). These four dictionaries are the
whole of the evidence for §5's verdicts.

**586, `GPONNK+TimesNewRomanPSMT`** — parent xref 23, descendant xref 67:

```
<< /DescendantFonts [ 67 0 R ] /BaseFont /GPONNK+TimesNewRomanPSMT /Type /Font
   /Subtype /Type0 /Encoding /Identity-H >>
<< /BaseFont /GPONNK+TimesNewRomanPSMT
   /CIDSystemInfo << /Ordering (Identity) /Registry (Adobe) /Supplement 0 >>
   /W [ 1 [ 250 333 250 ] 4 [ 250 722 667 722 ] 8 9 722 10 [ 889 556 611 722 444 500 444
        500 444 333 500 ] 21 [ 500 278 500 278 778 500 ] 27 29 500 30 [ 333 389 278 500 ]
        34 [ 500 722 500 444 760 980 500 444 ] 43 [ 444 333 ] ]
   /Type /Font /Subtype /CIDFontType2 /FontDescriptor 80 0 R /DW 1000
   /CIDToGIDMap /Identity >>
```

No `/ToUnicode`. Embedded program tables: `['GlyphOrder','OS/2','cvt ','fpgm','glyf','head',
'hhea','hmtx','loca','maxp','name','prep']` — **no `cmap`, no `post`.**

**699, `AOIIJB+MinionPro-Regular`** — parent xref 28, descendant xref 58:

```
<< /Subtype /Type0 /BaseFont /MinionPro-Regular-Identity-H /Type /Font
   /Encoding /Identity-H /DescendantFonts [ 58 0 R ] >>
<< /DW 1000 /CIDSystemInfo << /Supplement 0 /Registry (Adobe) /Ordering (Identity) >>
   /Subtype /CIDFontType0 /BaseFont /AOIIJB+MinionPro-Regular /FontDescriptor 92 0 R
   /Type /Font /W [ 1 [ 227 ] 66 [ 439 ] 69 [ 528 425 296 ] 73 [ 534 ] 78 [ 819 ]
                    80 [ 510 524 ] 83 [ 371 367 305 ] ] >>
```

No `/ToUnicode`. Program is a bare CID-keyed CFF (not sfnt-wrapped — `TTFont` refuses it with
`bad sfntVersion`; `CFFFontSet.decompile` reads it). 13 glyphs, charset `.notdef` + `cidNNNNN`
synthetic names, **no `cmap`, no `post`, no glyph names.**

**719, `JGFKKL+TimesNewRoman`** — parent xref 34, descendant xref 106:

```
<< /DescendantFonts [ 106 0 R ] /BaseFont /JGFKKL+TimesNewRoman /Type /Font
   /Subtype /Type0 /Encoding /Identity-H >>
<< /BaseFont /JGFKKL+TimesNewRoman
   /CIDSystemInfo << /Ordering (Identity) /Registry (Adobe) /Supplement 0 >>
   /W [ 3 [ 250 ] 11 [ 333 333 ] 15 [ 250 333 250 ] 19 [ 500 ×10, 278 278 ] 32 [ 564 ]
        35 [ 921 722 667 667 722 611 556 722 722 333 389 722 611 889 722 722 556 ]
        53 [ 667 556 611 722 722 944 722 722 611 333 ] 64 [ 333 ]
        68 [ 444 500 444 500 444 333 500 500 278 278 500 278 778 500 500 500 500 333
             389 278 500 500 722 500 500 444 ] 120 [ 500 ] 124 [ 500 ]
        179 [ 444 444 ] 182 [ 333 ] 214 [ 333 ] ]
   /Type /Font /Subtype /CIDFontType2 /FontDescriptor 120 0 R /DW 1000
   /CIDToGIDMap /Identity >>
```

No `/ToUnicode`. Same table set as 586 — **no `cmap`, no `post`.**

> 🔴 **The `/W` array of 719 is decisive and 586's is decisive the other way.** Read 719's widths
> against the **standard Macintosh TrueType glyph ordering** (`fontTools.ttLib.standardGlyphOrder`,
> 258 entries, index == GID): 3 `space` 250 ✓ · 11/12 `parenleft`/`parenright` 333 ✓ · 15 `comma`
> 250 ✓ · 16 `hyphen` 333 ✓ · 17 `period` 250 ✓ · 19–28 `zero`…`nine` 500 ✓ · 29/30 `colon`/
> `semicolon` 278 ✓ · 32 `equal` 564 ✓ · 35 `at` 921 ✓ · 36–51 `A`…`P` 722 667 667 722 611 556 722
> 722 333 389 722 611 889 722 722 556 ✓. **Every value is Times New Roman's own advance for the
> glyph the standard order places at that index. 719's subsetter preserved standard order.**
> Now 586: standard order requires GID 4 = `exclam` (Times 333) and GID 5 = `quotedbl` (408); 586
> declares **250** and **722**. **586's subsetter did not.**

---

## 4. F2 — every unresolved token accounted for

Marker regex is the shipped gate's own `RE_GLYPH` (`engine/parsers/parse_quality.py:47`), which
matches both the raw and the HTML-escaped form. **The stored `.md` files carry the escaped form
(`GLYPH&lt;c=…&gt;`) — a plain `grep 'GLYPH<'` returns 0 on 586 and 699 and undercounts 719 by
5,346.**

| paper | (font, code) rows | Σ counts | expected (PARSE-GATE-08) |
|---|---:|---:|---|
| 586 | **88** | **542** | 542 ✓ |
| 699 | **1** | **419** | 419 ✓ |
| 719 | **39** | **5,472** | not previously stated; **reported here as 5,472** |

**586 by font:** `TimesNewRomanPSMT` 244 tokens / 31 rows · `TimesNewRomanPS-BoldMT` 149 / 31 · `CambriaMath` 139 / 24 · `SymbolMT` 10 / 2. The per-font token tally matches PARSE-GATE-08 exactly.
`CambriaMath` 139 (19) · `SymbolMT` 10 (2). Matches 08's per-font tally exactly.
**719 by font:** `TimesNewRoman` 5,071 / 18 · `TimesNewRoman,Italic` 246 / 10 · `TimesNewRoman,Bold` 112 / 6 · `Arial` 33 / 1 · `Arial,Italic` 7 / 1 · `Symbol` 3 / 3.
`TimesNewRoman,Bold` 112 (6) · `Arial` 33 (1) · `Arial,Italic` 7 (1) · `Symbol` 3 (3).

### 🔴 The marker count is NOT the corruption count

`get_texttrace` resolves **every** character in these fonts to U+FFFD. Counting those and
splitting on the CID value shows what docling does with each:

| paper | unresolved chars in the offending fonts (PyMuPDF) | of which CID < 32 → emitted as `GLYPH<…>` | of which CID ≥ 32 → **emitted as a raw, wrong ASCII character, with no marker** |
|---|---:|---:|---:|
| 586 | **1,081** | 589 (542 survive into the markdown) | **492** |
| 699 | **440** | 419 (419 survive) | **21** |
| 719 | **27,565** | 5,508 (5,472 survive) | **22,057** |

The docling backend emits the raw CID as a character when it is ≥ 32 and a `GLYPH<>` marker when
it is < 32 — measured: **not one marker in any of the three papers has `c ≥ 32`.** So the visible
markers are the *low-CID tail* of the damage. The characters that carry the most information —
letters — are all high-CID and come through **silently wrong**. This is the same mechanism
PARSE-GATE-08 §5.1 found on 719's PyMuPDF layer; it is now measured to be present in **586 too**,
at 492 characters, and in the *docling* output, not only PyMuPDF's. It is what produces the junk
that reads as literal text in `586_v2.md`: `)`, `$+`, `66`, `C6`, `\`, `]`, `>`, `T`.

### Per-page distribution (unresolved chars in offending fonts, PyMuPDF)

| paper | p1 | p2 | p3 | p4 | p5 | p6 | p7 | p8 | total |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 586 | 189 | 146 | 252 | 119 | 136 | 192 | **0** | 47 | 1,081 |
| 699 | **440** | 0 | 0 | 0 | 0 | 0 | — | — | 440 |
| 719 | 5,297 | 4,458 | 3,824 | 4,268 | 5,205 | 4,513 | — | — | 27,565 |

(699's 440 are the MinionPro spaces plus 21 high-CID characters, all on p1; the 42+ unresolved
SegoeUI/SimHei characters on p4 are in figure artwork docling does not export, which is why they
appear in no marker count.)

---

## 5. F3 — verdict per (font, code) row

**The evidence is identical for all 128 rows across the three papers**, because it is a property
of the four dictionaries in §3, not of the individual code:

| evidence question | 586 (4 fonts) | 699 (1 font) | 719 (6 fonts) |
|---|---|---|---|
| ToUnicode entry present / absent / bogus | **absent** (no `/ToUnicode` key) | **absent** | **absent** |
| `/Differences` glyph name present, AGL-resolvable | **cannot exist** — Identity-H composite font | **cannot exist** | **cannot exist** |
| embedded program has a `cmap` naming the glyph | **no `cmap` table** | **no `cmap`** (bare CFF) | **no `cmap` table** |
| embedded program has a `post` table naming the glyph | **no `post` table** | **no glyph names** (`cidNNNNN`) | **no `post` table** |
| what the program *does* retain | `glyf` outlines + `hmtx`; `/W` per CID | CFF charstrings + widths; `/W` | `glyf` outlines + `hmtx`; `/W` |

So **no row in any of the three papers earns RECOVERABLE-DETERMINISTIC.** The rows split into
BY-POSITION and UNRECOVERABLE on the outline-and-width evidence:

### 699 — 1 row

| font | code | n | `/W` | outline | glyph name | **verdict** |
|---|---:|---:|---:|---|---|---|
| `MinionPro-Regular` | 1 | **419** | 227 | **empty** (`BoundsPen` → `None`) | `cid00001` (synthetic) | **RECOVERABLE-BY-POSITION → U+0020 SPACE** |

Evidence: empty outline; `/W` 227 = Minion Pro's space advance; measured span advance 0.227 em
**equal to mupdf's own `spacewidth` for that span to three decimals** (2.262 = 2.262); all 419
isolated singles in inter-word slots (PARSE-GATE-08). **One row, one character, 419 tokens, and
100% of the paper's gate failure.**

### 586 — 88 rows

Four rows are `c=1` and are the space of their font; **84 rows are UNRECOVERABLE.**

| font | code | n | `/W` | outline | base-font space advance | **verdict** |
|---|---:|---:|---:|---|---:|---|
| `TimesNewRomanPSMT` | 1 | 39 | 250 | **empty** | Times 250 ✓ | **BY-POSITION → U+0020** |
| `TimesNewRomanPS-BoldMT` | 1 | 16 | 250 | **empty** | Times Bold 250 ✓ | **BY-POSITION → U+0020** |
| `CambriaMath` | 1 | 8 | 220 | **empty** | Cambria 220 ✓ | **BY-POSITION → U+0020** |
| `SymbolMT` | 1 | 1 | 250 | **empty** | Symbol 250 ✓ | **BY-POSITION → U+0020** |

That is **64 of 542 tokens (11.8%)**. The remaining **478 tokens over 84 rows are UNRECOVERABLE**;
every one has a non-blank outline, a synthetic `glyph000NN` name, no cmap and no post. The full
84-row list with the prose each sits in is §7 (F4c).

### 719 — 39 rows

| font | codes | n | outline | **verdict** |
|---|---|---:|---|---|
| `TimesNewRoman`, `TimesNewRoman,Italic`, `TimesNewRoman,Bold`, `Arial`, `Arial,Italic` | **3** | **4,541** | **empty** | **BY-POSITION → U+0020** (`/W` 250 Times / 278 Arial = each font's space) |
| the other 34 rows (codes 10–30 across 6 fonts, plus `Symbol` 11/12/16) | — | **931** | non-blank | **UNRECOVERABLE** *by the F3 test as written* — see the qualification in §8 |

Full 719 row table (`std` = the glyph the **standard Macintosh order** places at that index):

| font (subset tags dropped) | code | n | `/W` | outline | `std` name at that index |
|---|---:|---:|---:|---|---|
| `TimesNewRoman` | 3 | 4186 | 250 | empty | space |
| `TimesNewRoman` | 17 | 297 | 250 | non-blank | period |
| `TimesNewRoman` | 15 | 234 | 250 | non-blank | comma |
| `TimesNewRoman,Italic` | 3 | 215 | 250 | empty | space |
| `TimesNewRoman,Bold` | 3 | 100 | 250 | empty | space |
| `TimesNewRoman` | 30 | 49 | 278 | non-blank | semicolon |
| `TimesNewRoman` | 20 | 48 | 500 | non-blank | one |
| `TimesNewRoman` | 12 | 36 | 333 | non-blank | parenright |
| `TimesNewRoman` | 19 | 35 | 500 | non-blank | zero |
| `TimesNewRoman` | 11 | 34 | 333 | non-blank | parenleft |
| `Arial` | 3 | 33 | 278 | empty | space |
| `TimesNewRoman` | 16 | 30 | 333 | non-blank | hyphen |
| `TimesNewRoman` | 21 | 30 | 500 | non-blank | two |
| `TimesNewRoman` | 22 | 17 | 500 | non-blank | three |
| `TimesNewRoman` | 24 | 16 | 500 | non-blank | five |
| `TimesNewRoman` | 23 | 13 | 500 | non-blank | four |
| `TimesNewRoman` | 29 | 11 | 278 | non-blank | colon |
| `TimesNewRoman,Italic` | 16 | 11 | 333 | non-blank | hyphen |
| `TimesNewRoman` | 28 | 10 | 500 | non-blank | nine |
| `TimesNewRoman` | 25 | 9 | 500 | non-blank | six |
| `TimesNewRoman` | 26 | 8 | 500 | non-blank | seven |
| `TimesNewRoman` | 27 | 8 | 500 | non-blank | eight |
| `TimesNewRoman,Italic` | 17 | 8 | 250 | non-blank | period |
| `TimesNewRoman,Bold` | 17 | 7 | 250 | non-blank | period |
| `Arial,Italic` | 3 | 7 | 278 | empty | space |
| `TimesNewRoman,Italic` | 12 | 4 | 333 | non-blank | parenright |
| `TimesNewRoman,Bold` | 15 | 2 | 250 | non-blank | comma |
| `TimesNewRoman,Italic` | 19 | 2 | 500 | non-blank | zero |
| `TimesNewRoman,Italic` | 22 | 2 | 500 | non-blank | three |
| `TimesNewRoman,Bold` | 11 | 1 | 333 | non-blank | parenleft |
| `TimesNewRoman,Bold` | 12 | 1 | 333 | non-blank | parenright |
| `TimesNewRoman,Bold` | 16 | 1 | 333 | non-blank | hyphen |
| `TimesNewRoman,Italic` | 10 | 1 | 214 | non-blank | quotesingle |
| `TimesNewRoman,Italic` | 20 | 1 | 500 | non-blank | one |
| `TimesNewRoman,Italic` | 21 | 1 | 500 | non-blank | two |
| `TimesNewRoman,Italic` | 23 | 1 | 500 | non-blank | four |
| `Symbol` | 11 | 1 | 333 | non-blank | parenleft |
| `Symbol` | 12 | 1 | 333 | non-blank | parenright |
| `Symbol` | 16 | 1 | 549 | non-blank | hyphen |


---

## 6. F4(a) — cross-check against the v3 OCR token at the same position

**As specified the check is not performable, and the reason is the finding: ZERO (font, code) rows
earn a RECOVERABLE-DETERMINISTIC verdict**, in any of the three papers, because no ToUnicode entry,
no `/Differences` name, no `cmap` and no `post` exists to resolve *any* code (§3, §5). There is
therefore no font-derived Unicode to cross-check.

**Substitute check, run in its place, on ten occurrences.** Ten marker occurrences were selected
for unambiguous alignment to `586_v3.md`, spanning **eight** distinct `(font, code)` rows. The
comparison is: what character does the v3 OCR carry at that position, and is the code's declared
`/W` advance consistent with that character in the base font? This is a genuine cross-family check
— the OCR tier (RapidOCR pixels) never sees the font dictionary.

| # | (font, code) | v2 context (markers as █) | v3 OCR at the same position | implied character | `/W` | consistent? |
|---:|---|---|---|:--:|---:|:--:|
| 1 | CambriaMath 15 | `commonly only █ ███66 as a trade-off` | `commonlyonly0.24mmasatrade-off` | `0` | 554 | ✓ digit width |
| 2 | CambriaMath 10 | (same run) | (same run) | `.` | 205 | ✓ narrow |
| 3 | CambriaMath 17 | (same run) | (same run) | `2` | 554 | ✓ digit width |
| 4 | CambriaMath 19 | (same run) | (same run) | `4` | 554 | ✓ digit width |
| 5 | CambriaMath 18 | `images at ██ fps` | `images at 30fps` | `3` | 554 | ✓ digit width |
| 6 | CambriaMath 15 | (same run — **replicate**) | (same run) | `0` | 554 | ✓ **agrees with #1** |
| 7 | CambriaMath 16 | `(in the order of ████6$ )` | `(in the order of ~100mN)` | `1` | 554 | ✓ digit width |
| 8 | CambriaMath 8 | `approximately ██████ fibre cores` | `approximately30,000fibre cores` | `,` | 205 | ✓ = the `.` width |
| 9 | CambriaMath 23 | `lies between █ T██C6 ,` | `between0-80um,` | `8` | 554 | ✓ digit width |
| 10 | TimesNewRomanPSMT 1 | `Endomicroscopy,  a █ █ )…` | `Endomicroscopy, an "optical-biopsy"` | **space** | 250 | ✓ **confirms the BY-POSITION verdict** |

**Agree 10 / 10, disagree 0 / 10.** `CambriaMath 15` was resolved independently at **four**
separate passages (`0.24mm`, `30fps`, `~100mN`, `0-80um`) and read `0` at every one, so the code
is stable, not position-dependent. All nine Cambria codes with `/W` 554 land on digits and both
codes with `/W` 205 land on `.`/`,` — the width class is consistent with the OCR character in
every case.

> 🔴 **But consistency is not recovery, and the distinction is the point of the whole probe.**
> Width narrows a code to a *class* (`554` = "one of Cambria's tabular figures", 10 candidates;
> `205` = "period or comma", 2 candidates) and never to a character. The character in the table
> above came from **the OCR, not from the font**. A recovery step built on `/W` alone would have to
> guess which of ten digits, on a page whose destroyed values include a frame rate (`30fps`), a
> field of view (`0.24mm`), a force (`~100mN`), a fibre count (`30,000`) and a working range
> (`0-80um`) — five numeric facts an extractor would read as data.

Two further alignments recovered while doing this, not counted above because they are literal
characters rather than markers: **`6` = `m` and `C` = `µ`** in `CambriaMath` (CIDs 54 and 67,
emitted raw because they are ≥ 32) — the mechanism of §4's silent layer, caught in the act.

`SymbolMT c=2` (n=9, `/W` 460 = Adobe Symbol's `bullet` advance) sits at `- █ A surgical robot
system`; v3 renders the same lines as `- An endomicroscopyimagingsystem`, absorbing the bullet into
the markdown list marker. Not counted as an agreement — v3 has no character there to compare.

## 7. F4(b) and F4(c)

### (b) paragraphs carrying at least one unresolved glyph, in `586_v2.md`

| denominator | paragraphs | with ≥ 1 marker | share |
|---|---:|---:|---:|
| all non-empty paragraphs (`split("\n\n")`) | 106 | **26** | 24.5% |
| body paragraphs, PARSE-GATE-07 rule (`len(strip) > 200`) | 54 | **24** | **44.4%** |

The 07-rule figure is the comparable one: 08 published `30/54` glyph-**free** body paragraphs for
586 v2, i.e. 24 affected — reproduced exactly. For context, the same measure on the other two:
699 **3 / 40** body paragraphs (7.5%); 719 **91 / 91** (100%).

### (c) the UNRECOVERABLE rows and the prose they sit in

**84 of 586's 88 rows, 478 of its 542 tokens.** Full list; `prose` is the first occurrence with
±48 characters, other markers rendered as █. The four `code = 1` rows are the BY-POSITION spaces
and are included for completeness, marked as such.

| font | code | n | `/W` | outline | verdict | prose (first occurrence) |
|---|---:|---:|---:|---|---|---|
| `TimesNewRomanPSMT` | 1 | 39 | 250 | empty | BY-POSITION (space) | `acterization capabilities. Endomicroscopy,  a █ █ )██  ███ █ -███ █ █ $+ based  approach  for vis` |
| `CambriaMath` | 15 | 31 | 554 | non-blank | UNRECOVERABLE | `e  limited  Field-of-View  (FOV), commonly only █ ███66 as a trade-off of the high-resolution cel` |
| `TimesNewRomanPSMT` | 18 | 29 | 444 | non-blank | UNRECOVERABLE | `into  is  based  on  balancing  a  trade-off █ █ # ███ █ █ █ █ ██ █ █  , ██ capabilities and the` |
| `CambriaMath` | 8 | 27 | 205 | non-blank | UNRECOVERABLE | `and all data for the three metrics are: CN█ WCEN█ CFN█ CGNX██ CL█ \CEL █ CFL█ CGL ]█ and CK█ \CEK` |
| `TimesNewRomanPSMT` | 22 | 20 | 278 | non-blank | UNRECOVERABLE | `ation capabilities. Endomicroscopy,  a █ █ )██  ███ █ -███ █ █ $+ based  approach  for visualizat` |
| `TimesNewRomanPSMT` | 31 | 20 | 389 | non-blank | UNRECOVERABLE | `ities. Endomicroscopy,  a █ █ )██  ███ █ -███ █ █ $+ based  approach  for visualization  and  dia` |
| `TimesNewRomanPSMT` | 27 | 19 | 500 | non-blank | UNRECOVERABLE | `erization capabilities. Endomicroscopy,  a █ █ )██  ███ █ -███ █ █ $+ based  approach  for visual` |
| `TimesNewRomanPS-BoldMT` | 25 | 17 | 500 | non-blank | UNRECOVERABLE | `urgical robot systems █ ███ █ █ ██ █ █ ██ ██ ████ ██ █   "██ ████ ██ █ ██ █ ██ █ ███ █ . This pap` |
| `TimesNewRomanPS-BoldMT` | 1 | 16 | 250 | empty | BY-POSITION (space) | `ck in current surgical robot systems █ ███ █ █ ██ █ █ ██ ██ ████ ██ █   "██ ████ ██ █ ██ █ ██ █ █` |
| `TimesNewRomanPSMT` | 14 | 16 | 444 | non-blank | UNRECOVERABLE | `ion capabilities. Endomicroscopy,  a █ █ )██  ███ █ -███ █ █ $+ based  approach  for visualizatio` |
| `TimesNewRomanPS-BoldMT` | 24 | 15 | 556 | non-blank | UNRECOVERABLE | `c feedback in current surgical robot systems █ ███ █ █ ██ █ █ ██ ██ ████ ██ █   "██ ████ ██ █ ██` |
| `CambriaMath` | 10 | 13 | 205 | non-blank | UNRECOVERABLE | `limited  Field-of-View  (FOV), commonly only █ ███66 as a trade-off of the high-resolution cellu` |
| `CambriaMath` | 16 | 13 | 554 | non-blank | UNRECOVERABLE | `ed also needs to be regulated (in the order of ████6$ ) to minimize tissue deformation which impa` |
| `TimesNewRomanPSMT` | 26 | 12 | 500 | non-blank | UNRECOVERABLE | `aracterization capabilities. Endomicroscopy,  a █ █ )██  ███ █ -███ █ █ $+ based  approach  for v` |
| `TimesNewRomanPSMT` | 30 | 12 | 333 | non-blank | UNRECOVERABLE | `on  balancing  a  trade-off █ █ # ███ █ █ █ █ ██ █ █  , ██ capabilities and the computational t` |
| `TimesNewRomanPSMT` | 24 | 11 | 278 | non-blank | UNRECOVERABLE | `n capabilities. Endomicroscopy,  a █ █ )██  ███ █ -███ █ █ $+ based  approach  for visualization` |
| `TimesNewRomanPS-BoldMT` | 16 | 10 | 444 | non-blank | UNRECOVERABLE | `eedback in current surgical robot systems █ ███ █ █ ██ █ █ ██ ██ ████ ██ █   "██ ████ ██ █ ██ █ █` |
| `TimesNewRomanPS-BoldMT` | 20 | 10 | 278 | non-blank | UNRECOVERABLE | `ic feedback in current surgical robot systems █ ███ █ █ ██ █ █ ██ ██ ████ ██ █   "██ ████ ██ █ ██` |
| `SymbolMT` | 2 | 9 | 460 | non-blank | UNRECOVERABLE | `owing hardware (shown in Fig. 4(a) and (b)):  - █ A surgical robot system - █ An endomicroscopy i` |
| `TimesNewRomanPS-BoldMT` | 14 | 9 | 444 | non-blank | UNRECOVERABLE | `t surgical robot systems █ ███ █ █ ██ █ █ ██ ██ ████ ██ █   "██ ████ ██ █ ██ █ ██ █ ███ █ . This` |
| `TimesNewRomanPS-BoldMT` | 28 | 9 | 389 | non-blank | UNRECOVERABLE | `ack in current surgical robot systems █ ███ █ █ ██ █ █ ██ ██ ████ ██ █   "██ ████ ██ █ ██ █ ██ █` |
| `TimesNewRomanPS-BoldMT` | 12 | 8 | 500 | non-blank | UNRECOVERABLE | `█ ███ █ █ ██ █ █ ██ ██ ████ ██ █   "██ ████ ██ █ ██ █ ██ █ ███ █ . This paper proposes a sensorl` |
| `TimesNewRomanPS-BoldMT` | 29 | 8 | 333 | non-blank | UNRECOVERABLE | `in current surgical robot systems █ ███ █ █ ██ █ █ ██ ██ ████ ██ █   "██ ████ ██ █ ██ █ ██ █ ███` |
| `TimesNewRomanPSMT` | 17 | 8 | 500 | non-blank | UNRECOVERABLE | `images as acceptable and the rest seven ██ █ !█ ████ █ █ █ █ █ █ █ !█ ██ $ ██ █ █ █ █ █ █ █ █  ██` |
| `TimesNewRomanPSMT` | 19 | 8 | 333 | non-blank | UNRECOVERABLE | `three images as acceptable and the rest seven ██ █ !█ ████ █ █ █ █ █ █ █ !█ ██ $ ██ █ █ █ █ █ █` |
| `TimesNewRomanPSMT` | 21 | 8 | 500 | non-blank | UNRECOVERABLE | `based  on  balancing  a  trade-off █ █ # ███ █ █ █ █ ██ █ █  , ██ capabilities and the computati` |
| `TimesNewRomanPSMT` | 25 | 8 | 778 | non-blank | UNRECOVERABLE | `n ██ █ !█ ████ █ █ █ █ █ █ █ !█ ██ $ ██ █ █ █ █ █ █ █ █  ██ █ ██ █ █ █ █ █ █ █ █ ████ ██ █ ██, █` |
| `CambriaMath` | 1 | 8 | 220 | empty | BY-POSITION (space) | `for the three metrics are: CN█ WCEN█ CFN█ CGNX██ CL█ \CEL █ CFL█ CGL ]█ and CK█ \CEK █ CFK█ CGK` |
| `TimesNewRomanPS-BoldMT` | 27 | 7 | 444 | non-blank | UNRECOVERABLE | `dback in current surgical robot systems █ ███ █ █ ██ █ █ ██ ██ ████ ██ █   "██ ████ ██ █ ██ █ ██` |
| `TimesNewRomanPSMT` | 15 | 7 | 500 | non-blank | UNRECOVERABLE | `apabilities. Endomicroscopy,  a █ █ )██  ███ █ -███ █ █ $+ based  approach  for visualization  an` |
| `CambriaMath` | 9 | 6 | 264 | non-blank | UNRECOVERABLE | `, bad and all data for the three metrics are: CN█ WCEN█ CFN█ CGNX██ CL█ \CEL █ CFL█ CGL ]█ and CK` |
| `CambriaMath` | 17 | 6 | 554 | non-blank | UNRECOVERABLE | `limited  Field-of-View  (FOV), commonly only █ ███66 as a trade-off of the high-resolution cellul` |
| `CambriaMath` | 26 | 6 | 457 | non-blank | UNRECOVERABLE | `█ █ `███a , which describes for each state &gt; █ + and action u █ U , the conditional probabilit` |
| `TimesNewRomanPS-BoldMT` | 15 | 4 | 556 | non-blank | UNRECOVERABLE | `feedback in current surgical robot systems █ ███ █ █ ██ █ █ ██ ██ ████ ██ █   "██ ████ ██ █ ██ █` |
| `TimesNewRomanPS-BoldMT` | 22 | 4 | 278 | non-blank | UNRECOVERABLE | `gical robot systems █ ███ █ █ ██ █ █ ██ ██ ████ ██ █   "██ ████ ██ █ ██ █ ██ █ ███ █ . This paper` |
| `TimesNewRomanPS-BoldMT` | 23 | 4 | 833 | non-blank | UNRECOVERABLE | `(email: r.varghese15@imperial.ac.uk)  ## █ ████ ███ ███ ████ ████████████████ █ █████ █ ████ ██ █` |
| `TimesNewRomanPSMT` | 28 | 4 | 500 | non-blank | UNRECOVERABLE | `rization capabilities. Endomicroscopy,  a █ █ )██  ███ █ -███ █ █ $+ based  approach  for visuali` |
| `TimesNewRomanPS-BoldMT` | 18 | 3 | 500 | non-blank | UNRECOVERABLE | `al robot systems █ ███ █ █ ██ █ █ ██ ██ ████ ██ █   "██ ████ ██ █ ██ █ ██ █ ███ █ . This paper pr` |
| `TimesNewRomanPS-BoldMT` | 19 | 3 | 556 | non-blank | UNRECOVERABLE | `ptic feedback in current surgical robot systems █ ███ █ █ ██ █ █ ██ ██ ████ ██ █   "██ ████ ██ █` |
| `TimesNewRomanPS-BoldMT` | 30 | 3 | 556 | non-blank | UNRECOVERABLE | `.uk)  ## █ ████ ███ ███ ████ ████████████████ █ █████ █ ████ ██ ███ █ █ ██████████████ ███████ █` |
| `TimesNewRomanPSMT` | 3 | 3 | 250 | non-blank | UNRECOVERABLE | `fibre bundle █ █ ███"█ %█ █ '█ ███ █ █ █ █ █ █ █ █ ██!███ ██ █ █ ████████ █ █ ██&amp;█ █ ███ █ █` |
| `TimesNewRomanPSMT` | 16 | 3 | 444 | non-blank | UNRECOVERABLE | `tion capabilities. Endomicroscopy,  a █ █ )██  ███ █ -███ █ █ $+ based  approach  for visualizati` |
| `TimesNewRomanPSMT` | 20 | 3 | 500 | non-blank | UNRECOVERABLE | `█ '█ ███ █ █ █ █ █ █ █ █ ██!███ ██ █ █ ████████ █ █ ██&amp;█ █ ███ █ ██ █ France) is used with ap` |
| `CambriaMath` | 18 | 3 | 554 | non-blank | UNRECOVERABLE | `provides  non-optically-sectioned  images  at ██ fps. With both systems, a commercial fibre bun` |
| `CambriaMath` | 19 | 3 | 554 | non-blank | UNRECOVERABLE | `imited  Field-of-View  (FOV), commonly only █ ███66 as a trade-off of the high-resolution cellula` |
| `CambriaMath` | 20 | 3 | 554 | non-blank | UNRECOVERABLE | `over. The load cell  used  had  a  range  of █ T███2 .  A  linear  fitting  was derived to conver` |
| `CambriaMath` | 29 | 3 | 838 | non-blank | UNRECOVERABLE | `e set of transition probabilities %█ + U ) U +█ █ `███a , which describes for each state &gt; █ +` |
| `TimesNewRomanPS-BoldMT` | 3 | 2 | 722 | non-blank | UNRECOVERABLE | `on, UK (email: r.varghese15@imperial.ac.uk)  ## █ ████ ███ ███ ████ ████████████████ █ █████ █ ██` |
| `TimesNewRomanPS-BoldMT` | 10 | 2 | 556 | non-blank | UNRECOVERABLE | `hese15@imperial.ac.uk)  ## █ ████ ███ ███ ████ ████████████████ █ █████ █ ████ ██ ███ █ █ ███████` |
| `TimesNewRomanPS-BoldMT` | 13 | 2 | 556 | non-blank | UNRECOVERABLE | `██ ███ ████ ████████████████ █ █████ █ ████ ██ ███ █ █ ██████████████ ███████ █ ████ ████████████` |
| `TimesNewRomanPS-BoldMT` | 26 | 2 | 556 | non-blank | UNRECOVERABLE | `██ █ █ ██ ██ ████ ██ █   "██ ████ ██ █ ██ █ ██ █ ███ █ . This paper proposes a sensorless altern` |
| `TimesNewRomanPSMT` | 23 | 2 | 500 | non-blank | UNRECOVERABLE | `ained from pCLE  to demonstrate the █ █ ██ █ #███ , ██ █████ $ █ █ █ █ ███ ███ ███ █ ██ ██ █ ██ █` |
| `CambriaMath` | 2 | 2 | 568 | non-blank | UNRECOVERABLE | `u █ U , the conditional probability % R█R [ Q V ███ ^&gt; Z █&gt;█ =\_ ,  to  transition  to  sta` |
| `CambriaMath` | 6 | 2 | 414 | non-blank | UNRECOVERABLE | `█ U , the conditional probability % R█R [ Q V ███ ^&gt; Z █&gt;█ =\_ ,  to  transition  to  stat` |
| `CambriaMath` | 12 | 2 | 490 | non-blank | UNRECOVERABLE | `ap (at recommended scanning velocities of ███66 █;0. [26]) between  consecutive  images  for  goo` |
| `CambriaMath` | 13 | 2 | 316 | non-blank | UNRECOVERABLE | `conditional probability % R█R [ Q V ███ ^&gt; Z █&gt;█ =\_ ,  to  transition  to  state &gt; Z █` |
| `CambriaMath` | 14 | 2 | 712 | non-blank | UNRECOVERABLE | `ied also needs to be regulated (in the order of ████6$ ) to minimize tissue deformation which imp` |
| `SymbolMT` | 1 | 1 | 250 | empty | BY-POSITION (space) | `█  Abstract ! Advances  in  optical  imaging,  an` |
| `TimesNewRomanPS-BoldMT` | 2 | 1 | 333 | non-blank | UNRECOVERABLE | `███ ████ ████████████████ █ █████ █ ████ ██ ███ █ █ ██████████████ ███████ █ ████ ██████████████` |
| `TimesNewRomanPS-BoldMT` | 4 | 1 | 722 | non-blank | UNRECOVERABLE | `███████████████ █ █████ █ ████ ██ ███ █ █ ██████████████ ███████ █ ████ ██████████████ ████ █████` |
| `TimesNewRomanPS-BoldMT` | 5 | 1 | 667 | non-blank | UNRECOVERABLE | `███ █ █ ██████████████ ███████ █ ████ ██████████████ ████ ████████████████  Rejin John Varghese,` |
| `TimesNewRomanPS-BoldMT` | 6 | 1 | 611 | non-blank | UNRECOVERABLE | `UK (email: r.varghese15@imperial.ac.uk)  ## █ ████ ███ ███ ████ ████████████████ █ █████ █ ████` |
| `TimesNewRomanPS-BoldMT` | 7 | 1 | 944 | non-blank | UNRECOVERABLE | `██████ █ █████ █ ████ ██ ███ █ █ ██████████████ ███████ █ ████ ██████████████ ████ ██████████████` |
| `TimesNewRomanPS-BoldMT` | 8 | 1 | 611 | non-blank | UNRECOVERABLE | `██ ███ ███ ████ ████████████████ █ █████ █ ████ ██ ███ █ █ ██████████████ ███████ █ ████ ████████` |
| `TimesNewRomanPS-BoldMT` | 9 | 1 | 722 | non-blank | UNRECOVERABLE | `████ ██ ███ █ █ ██████████████ ███████ █ ████ ██████████████ ████ ████████████████  Rejin John Va` |
| `TimesNewRomanPS-BoldMT` | 11 | 1 | 667 | non-blank | UNRECOVERABLE | `█ ████ ████████████████ █ █████ █ ████ ██ ███ █ █ ██████████████ ███████ █ ████ ██████████████ ██` |
| `TimesNewRomanPS-BoldMT` | 17 | 1 | 333 | non-blank | UNRECOVERABLE | `.varghese15@imperial.ac.uk)  ## █ ████ ███ ███ ████ ████████████████ █ █████ █ ████ ██ ███ █ █ ██` |
| `TimesNewRomanPS-BoldMT` | 21 | 1 | 556 | non-blank | UNRECOVERABLE | `: r.varghese15@imperial.ac.uk)  ## █ ████ ███ ███ ████ ████████████████ █ █████ █ ████ ██ ███ █ █` |
| `TimesNewRomanPS-BoldMT` | 31 | 1 | 722 | non-blank | UNRECOVERABLE | `mail: r.varghese15@imperial.ac.uk)  ## █ ████ ███ ███ ████ ████████████████ █ █████ █ ████ ██ ███` |
| `TimesNewRomanPSMT` | 2 | 1 | 333 | non-blank | UNRECOVERABLE | `s. With both systems, a commercial fibre bundle █ █ ███"█ %█ █ '█ ███ █ █ █ █ █ █ █ █ ██!███ ██ █` |
| `TimesNewRomanPSMT` | 4 | 1 | 250 | non-blank | UNRECOVERABLE | `e rest seven ██ █ !█ ████ █ █ █ █ █ █ █ !█ ██ $ ██ █ █ █ █ █ █ █ █  ██ █ ██ █ █ █ █ █ █ █ █ ████` |
| `TimesNewRomanPSMT` | 5 | 1 | 722 | non-blank | UNRECOVERABLE | `est seven ██ █ !█ ████ █ █ █ █ █ █ █ !█ ██ $ ██ █ █ █ █ █ █ █ █  ██ █ ██ █ █ █ █ █ █ █ █ ████ ██` |
| `TimesNewRomanPSMT` | 6 | 1 | 667 | non-blank | UNRECOVERABLE | `With both systems, a commercial fibre bundle █ █ ███"█ %█ █ '█ ███ █ █ █ █ █ █ █ █ ██!███ ██ █ █` |
| `TimesNewRomanPSMT` | 7 | 1 | 722 | non-blank | UNRECOVERABLE | `, a commercial fibre bundle █ █ ███"█ %█ █ '█ ███ █ █ █ █ █ █ █ █ ██!███ ██ █ █ ████████ █ █ ██&a` |
| `TimesNewRomanPSMT` | 8 | 1 | 722 | non-blank | UNRECOVERABLE | `s, a commercial fibre bundle █ █ ███"█ %█ █ '█ ███ █ █ █ █ █ █ █ █ ██!███ ██ █ █ ████████ █ █ ██&` |
| `TimesNewRomanPSMT` | 9 | 1 | 722 | non-blank | UNRECOVERABLE | `le █ █ ███"█ %█ █ '█ ███ █ █ █ █ █ █ █ █ ██!███ ██ █ █ ████████ █ █ ██&amp;█ █ ███ █ ██ █ France)` |
| `TimesNewRomanPSMT` | 10 | 1 | 889 | non-blank | UNRECOVERABLE | `re bundle █ █ ███"█ %█ █ '█ ███ █ █ █ █ █ █ █ █ ██!███ ██ █ █ ████████ █ █ ██&amp;█ █ ███ █ ██ █` |
| `TimesNewRomanPSMT` | 11 | 1 | 556 | non-blank | UNRECOVERABLE | `█ █ █ █ █ ██!███ ██ █ █ ████████ █ █ ██&amp;█ █ ███ █ ██ █ France) is used with approximately ███` |
| `TimesNewRomanPSMT` | 12 | 1 | 611 | non-blank | UNRECOVERABLE | `███"█ %█ █ '█ ███ █ █ █ █ █ █ █ █ ██!███ ██ █ █ ████████ █ █ ██&amp;█ █ ███ █ ██ █ France) is use` |
| `TimesNewRomanPSMT` | 13 | 1 | 722 | non-blank | UNRECOVERABLE | `ms, a commercial fibre bundle █ █ ███"█ %█ █ '█ ███ █ █ █ █ █ █ █ █ ██!███ ██ █ █ ████████ █ █ ██` |
| `TimesNewRomanPSMT` | 29 | 1 | 500 | non-blank | UNRECOVERABLE | `ble and the rest seven ██ █ !█ ████ █ █ █ █ █ █ █ !█ ██ $ ██ █ █ █ █ █ █ █ █  ██ █ ██ █ █ █ █ █ █` |
| `CambriaMath` | 11 | 1 | 332 | non-blank | UNRECOVERABLE | `C.  Pierce,  D.  J.  Javier,  and  R.  Richards █ Kortum,  "Optical contrast agents and imaging s` |
| `CambriaMath` | 21 | 1 | 554 | non-blank | UNRECOVERABLE | `-objective  and  outer  maximum  diameter  of ███66 at the distal tip. The  resulting field-of-vi` |
| `CambriaMath` | 22 | 1 | 554 | non-blank | UNRECOVERABLE | `p;\&gt;█ =] █ \█ T@]&amp;\&gt;█ =] S @\;,6950] \█] The optimal values and policy in terms of the` |
| `CambriaMath` | 23 | 1 | 554 | non-blank | UNRECOVERABLE | `g distance, the range for which lies between █ T██C6 , based on the lens assembly at the tip of t` |
| `CambriaMath` | 27 | 1 | 258 | non-blank | UNRECOVERABLE | `The likelihood of occurrence of the state &gt;█ is  a  reflection of the unknown transition pro` |
| `CambriaMath` | 28 | 1 | 838 | non-blank | UNRECOVERABLE | `ource state &gt; and action = :  &amp;\&gt;█ =] █ \█ T@]&amp;\&gt;█ =] S @\;,6950] \█] The optima` |
| `CambriaMath` | 30 | 1 | 623 | non-blank | UNRECOVERABLE | `discount rate for discounting future values. @ █ `███a is  the learning rate which blends in the` |


**Three groups are visible in that prose, and they are not equivalent:**

1. **List bullets** — `SymbolMT` 2 and 1, 10 tokens, `- █ A surgical robot system`. Cosmetic.
2. **Inline mathematics** — `CambriaMath` and the Times Type0 fonts inside displayed and inline
   equations (`CN█ WCEN█ CFN█ CGNX██ CL█ \CEL █ CFL█ CGL ]█`, `%█ + U ) U +█ █ ...███a`). The
   surrounding text in these runs is *already* junk from the §4 silent layer, so removing the
   markers would not make them readable.
3. **Destroyed numeric values in running prose** — `commonly only █ ███66`, `images at ██ fps`,
   `(in the order of ████6$ )`, `approximately ██████ fibre cores`, `lies between █ T██C6`,
   `approximately ███C6 and the resolution ███C 6`. **Six measurements, in six sentences an
   extractor reads for data.** These are the tokens §6 resolved from OCR and could not resolve
   from the font.

## 8. F5 — 719: does the embedded font's cmap explain the PyMuPDF Caesar shift?

**No — there is no cmap to explain it, and that is the answer.** The shift is not a mapping the
font carries; it is the **standard Macintosh TrueType glyph ordering**, which 719's subsetter
preserved and which places `space` at GID 3, so a printable ASCII character sits at
`GID = codepoint − 29` throughout `0x20`–`0x7E`. The parser is not applying a cipher: with no
`cmap`, no `post` and no `/ToUnicode`, it emits the raw CID as a codepoint, and because the order
is standard that raw CID is uniformly 29 below the truth. **The offset is constant per font and
constant across fonts** — 29 for every Times New Roman and Arial subset in the document. It is
**verified independently of the offset itself** by the `/W` array, whose 40-odd declared advances
are Times New Roman's own advances for exactly the glyphs the standard order places at those
indices (§3). Decoding at the *span* level (where the font is known) with `chr(gid + 29)` for
every U+FFFD reconstructs the paper in correct English:

```
A Multi-Behavior Algorithm for Auto-Guided Movements in Surgeon Assistance
E. Bauzano, V.F. Mu?oz and I. Garcia-Morales   University of Malaga, Spain
Abstract ? This paper focuses on autonomous movements to aid the surgeon to perform
certain tasks. Robotic assistants have solved the drawbacks of Minimally Invasive
Surgery (MIS) and provide additional skills to the surgeons. …
```

Coverage of that decode, over all 27,565 unresolved characters:

| font | unresolved chars | map into printable ASCII under `gid+29` | share |
|---|---:|---:|---:|
| TimesNewRoman | 24,727 | 24,696 | **99.87%** |
| TimesNewRoman,Italic | 2,010 | 2,001 | 99.55% |
| TimesNewRoman,Bold | 656 | 655 | 99.85% |
| MTExtra | 53 | 53 | 100% |
| Arial | 33 | 33 | 100% |
| Arial,Italic | 7 | 7 | 100% |
| TimesNewRoman,BoldItalic | 8 | 8 | 100% |
| **Symbol** | **71** | **39** | **54.93%** |
| **total** | **27,565** | **27,492** | **99.74%** |

The residue is where the assumption stops holding: the `ñ` of *Muñoz* and the em-dash of
*Abstract —* sit above GID 96 in the accented/punctuation tail, where standard order and Unicode
diverge; and `Symbol` has its own glyph order, not the Macintosh one, so only its low shared range
survives. **Two constraints on reading this as a recovery route.** First, the assumption is
*external* — "this subsetter preserved standard order" is not recorded anywhere in the file, and
586 in the same corpus proves it is not general (§3). Second, **at the markdown level the shift is
not invertible**: `719_v2.md` interleaves shifted characters with genuine ones from the two
ToUnicode-bearing MyriadPro fonts and with docling's own inserted whitespace, and the shifted
alphabet (`0x03`–`0x5E`) overlaps the unshifted one, so a blanket text-level shift corrupts every
real space into `=`. It inverts only where the font is known — i.e. at the PDF span, not in
anything the cascade currently stores.

Applying the same test to 586 fails at the first step: standard order requires `/W` 333 at GID 4
and 408 at GID 5; 586 declares 250 and 722. Its widths instead form a monotone run consistent with
**ascending-original-GID order over an unrecorded subset** (CID 1 = 250 space, 2 = 333, 3–4 = 250,
5 = 722, 6 = 667, … 10 = 889, … 14 = 444, 22 = 278, 25 = 778), which fixes the *order* of the kept
glyphs but not *which* glyphs were kept. **That information is nowhere in the file.**

---

## 9. Acceptance gates

| # | gate | result |
|---|---|---|
| 1 | F2 accounts for every unresolved token; Σ(font,code) = 419 (699), 542 (586); 719 total reported | **PASS** — 419 / 1 row, 542 / 88 rows, **5,472 / 39 rows**, using the gate's own `RE_GLYPH` |
| 2 | every (font, code) row carries one of the three F3 verdicts and its evidence | **PASS** — 128 rows: **0 RECOVERABLE-DETERMINISTIC**, **10 rows / 5,024 tokens RECOVERABLE-BY-POSITION** (699 ×1, 586 ×4, 719 ×5 — 419 + 64 + 4,541), **118 rows / 1,409 tokens UNRECOVERABLE**. Evidence in §3 and §5 |
| 3 | F4(a) reports agree/disagree on ten codes, or states why fewer were checkable | **PASS with the reason stated** — the specified check is vacuous (0 rows earn DETERMINISTIC, §6); the substitute check ran on **ten occurrences over eight rows, 10 agree / 0 disagree** |
| 4 | `review.db` size/mtime unchanged; six v2/v3 sha256 unchanged; `.venv` pip freeze diff empty | **PASS** — see §10 |
| 5 | one commit, two files, `ls-remote` == local HEAD, tree clean | **PASS** — see §10 |

## 10. Close-state ledger

*(values verified after all measurement, before the commit — the commit itself touches only
`docs/session-reports/`)*

| item | at session start | at session end |
|---|---|---|
| `review.db` size | 99,770,368 B | **99,770,368 B** |
| `review.db` mtime | 2026-09-08 03:48:48.732577380 +0000 | **identical to the nanosecond** |
| `586_v2.md` sha256 | `dd8935f5f746e1fe…` | unchanged |
| `586_v3.md` sha256 | `29eb250acb1db6c2…` | unchanged |
| `699_v2.md` sha256 | `a83f443fb1e6b9ee…` | unchanged |
| `699_v3.md` sha256 | `5313b9e201163cf5…` | unchanged |
| `719_v2.md` sha256 | `4cbc3557ff5ef4b0…` | unchanged |
| `719_v3.md` sha256 | `37bfcdf8094609ff…` | unchanged |
| `.venv` `pip freeze` | 168 packages | **168, diff empty** |
| Ollama | not touched | no model load, no restart |

All scratch output is under the session scratchpad (`f1b.json`, `f2.json`, `f3b.json`,
`f4_ctx.json`, the collapsed-text files, and five probe scripts); nothing was written under
`data/`.

---

## 11. Observed, not asked about

1. **🔴 The marker count understates the corruption by roughly a factor of two on 586, and by four
   on 719 (§4).** Docling emits `GLYPH<c=N>` only when `N < 32`; at `N ≥ 32` it emits the raw CID
   as a printable character with **no marker of any kind**. Measured: not one marker in the three
   papers has `c ≥ 32`. So `glyph_density_per_kchar` — the criterion that failed all three papers —
   is measuring the **low-CID tail** of the defect. 586 carries **492** additional silently-wrong
   characters that the gate cannot see; 719 carries **22,057**. This generalises PARSE-GATE-08 §5.1
   (which found the silent class on PyMuPDF's layer, on 719) to **docling's own output, and to 586**.
2. **The whole defect class has one structural signature, and it is cheap to detect.** In all three
   papers the affected fonts are exactly `Type0 + /Identity-H + no /ToUnicode`, and the unaffected
   ones are exactly those with either a base encoding or a ToUnicode CMap. That is a **property of
   the PDF, readable in milliseconds before any parse**, and it is what actually predicts the
   failure: 586 4/13, 699 1/19, 719 36/38. It does not need the parsed text to exist.
3. **699 and 586 are not the same case in any respect** — one font versus four, one code versus 88,
   a space versus five destroyed measurements, 3/40 paragraphs versus 24/54. PARSE-GATE-08 said as
   much from the text; the fonts say it more sharply.
4. **719's `Symbol` is the counter-example that bounds the standard-order assumption** (54.93%
   mappable against ≥99.5% for the Times and Arial subsets). Any rule keyed on "standard order"
   must fail closed on symbol-ordered fonts rather than emit plausible ASCII for them.
5. **PyMuPDF's `rawdict` cannot see raw codes; `get_texttrace` can** (I1). Worth recording because
   `rawdict` is the natural first reach and it silently hands back the *resolved* character.
6. **699's parent `/BaseFont` is `MinionPro-Regular-Identity-H`, not `AOIIJB+MinionPro-Regular`** —
   the name docling reports is the **descendant** CIDFont's. Anything matching a docling marker to
   a font object must look through `/DescendantFonts`, or it will miss.
