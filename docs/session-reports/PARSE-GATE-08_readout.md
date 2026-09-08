# PARSE-GATE-08 — read-out: defect granularity on 586/699, and a region-level hybrid probe

**Date:** 2026-09-08 · **Machine:** DGX Spark · **Type:** diagnostic read-out, no engine code, no DB writes
**HEAD at start and end:** `f96d647`, tree clean apart from this file.

Two questions were measured: (A) do per-page gate metrics isolate the defects on 586 and 699 —
is a page-level remedy feasible; and (B) does pymupdf4llm's hybrid OCR repair the corrupted
regions without degrading clean body text — is a region-level remedy available. **No ruling on
which parse version Run 7 reads is offered here; that is the architect's.**

---

## 1. Inferred-assumption resolutions (I1–I5)

| # | claim | verdict | source |
|---|---|---|---|
| **I1** | the gate's metric functions can be applied to an arbitrary text unit | **TRUE** | `engine/parsers/parse_quality.py:31-32` states "Pure: no I/O, no database, no model calls. `compute_metrics` and `assess` are functions of their text argument alone"; confirmed at the definitions, `compute_metrics(text)` :184 and `assess(text, thresholds)` :270. No document-level state, no corpus. Applied per page unmodified. |
| **I2** | page boundaries are recoverable for v2 and v3 | **TRUE, by re-run — not from the stored file** | v2/v3 are `result.document.export_to_markdown()` with **no page markers** (`pdf_parser.py:157`, `:186`), so the stored `.md` cannot be split. But `DoclingDocument.export_to_markdown` accepts `page_no=` (docling-core 2.65.1), and re-running the pinned tiers in scratch reproduces all six stored files **byte-identically** (sha256 match, table below) — so per-page slices are the same text as the stored versions, not an approximation. |
| **I3** | pymupdf4llm installs here and its hybrid OCR can be driven by an installed engine | **TRUE; RapidOCR is a supported engine and is the one selected** | Installed into `/tmp/pg08/b_venv` from pure aarch64 manylinux wheels, no compilation. `pymupdf4llm/ocr/detect_rapidocr.py:1-16` probes `rapidocr` then `rapidocr_onnxruntime`; measured return `"rapidocr"`. `helpers/document_layout.py:1183-1241 select_ocr_function()` prefers the new RapidOCR backend outright and returned `rapidocr_api.exec_ocr`. Tesseract is absent — `pymupdf.get_tessdata()` raises `RuntimeError: No tessdata specified and Tesseract is not installed`, caught by that function's bare `except`. Five OCR API modules ship (`rapidocr`, `paddleocr`, `tesseract`, `rapidtess`, `paddletess`); the three tesseract-bearing ones are unusable on this box. |
| **I4** | per-page / per-region OCR attribution is exposed | **PAGE: yes. REGION: no.** | **Page** — `helpers/document_layout.py:40` `INFO_MESSAGES = io.StringIO()`, written at `:1441` `print(f"OCR on {page.number=}/{page.number+1}.", file=INFO_MESSAGES)`; readable after the run. The decision itself is callable directly: `make_ocr_decision(page, use_ocr)` :1258 and `utils.analyze_page(page)` :1269. **Region** — the culled-pixmap rectangle list is built inside the callback (`ocr/exec_ocr_interface.py:215-232`, passed to `get_culled_pixmap.get_pixmap(..., rects=spans)`) and is **never returned or logged**; recognised text is inserted into the page with a normal font, so OCR-origin spans are indistinguishable downstream. Region attribution would need a patched callback. |
| **I5** | the 07 fidelity counts are reproducible from the 07 method | **TRUE, reused verbatim** | Recovered from the archived transcript `~/claude-session-archive/00d781e5-7b67-47b3-93b3-1ce18e5d3bfc.jsonl`, the `K4f hybrid-question counts` tool call at **line 1944**. Method: headings = lines whose strip starts `#`; glyph-corrupted = `RE_GLYPH.search`; glued tokens = `[w for w in text.split() if len(w) > 22 and w.isalpha()]`; body paragraphs = `text.split("\n\n")` kept at `len(strip) > 200`; degraded = paragraph containing any glued token. Re-running it on the stored v2/v3 files reproduces 07's published counts exactly (586 `1/16` headings, `30/54` and `25/51` paragraphs, 38 glued; 699 `0/15`, `37/40`, `19/39`, 33 glued; 719 `16/16`, `0/91`, `26/50`, 51 glued). |

**I2 reproduction check** — pinned docling 2.74.0 / docling-core 2.65.1, re-run in scratch:

| paper | tier | stored sha256 | re-run sha256 | chars | identical | wall |
|---|---|---|---|---:|:--:|---:|
| 586 | docling (v2) | `dd8935f5f746e1fe…` | `dd8935f5f746e1fe…` | 69,214 | **yes** | 12.9 s |
| 586 | docling_ocr (v3) | `29eb250acb1db6c2…` | `29eb250acb1db6c2…` | 40,486 | **yes** | 52.7 s |
| 699 | docling (v2) | `a83f443fb1e6b9ee…` | `a83f443fb1e6b9ee…` | 48,546 | **yes** | 7.5 s |
| 699 | docling_ocr (v3) | `5313b9e201163cf5…` | `5313b9e201163cf5…` | 27,045 | **yes** | 34.9 s |
| 719 | docling (v2) | `4cbc3557ff5ef4b0…` | `4cbc3557ff5ef4b0…` | 279,389 | **yes** | 12.9 s |
| 719 | docling_ocr (v3) | `37bfcdf8094609ff…` | `37bfcdf8094609ff…` | 28,836 | **yes** | 37.8 s |

Both tiers, including the RapidOCR one, are byte-deterministic across runs and across a 19-hour gap.

---

## 2. Probe A — per-page gate metrics

Thresholds are the shipped absolute defaults, which are also the values the Review Spec resolves to
(07 pre-flight I1): `short_unit_share_pct_max=50.0`, `chars_per_unit_min=20.0`,
`glyph_density_per_kchar_max=5.0`, `replacement_density_per_kchar_max=1.0`. `ltsp%` is
`long_token_share_pct` (telemetry, never judged). `U+FFFD` is the raw `replacement_chars` count.
719 is carried as the control.

#### p586 — 8 pages

| page | source | chars | units | chars/unit | short% | glyph/kchar | repl/kchar | U+FFFD | ltsp% | would-FAIL | criteria |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|:--:|---|
| 1 | pymupdf (text layer) | 6,506 | 161 | 40.4 | 27.3 | 0.0 | 0.0 | 0 | 0.0 | n | — |
| 1 | docling v2 (text layer) | 15,185 | 42 | 361.5 | 7.1 | 11.459 | 0.0 | 0 | 0.0 | **y** | GLYPH_DENSITY |
| 1 | docling_ocr v3 (OCR tier) | 5,835 | 28 | 208.4 | 3.6 | 0.0 | 0.0 | 0 | 3.376 | n | — |
| 2 | pymupdf (text layer) | 5,561 | 166 | 33.5 | 36.7 | 0.0 | 0.0 | 0 | 0.0 | n | — |
| 2 | docling v2 (text layer) | 6,384 | 63 | 101.3 | 14.3 | 1.88 | 0.0 | 0 | 0.0 | n | — |
| 2 | docling_ocr v3 (OCR tier) | 5,555 | 50 | 111.1 | 20.0 | 0.0 | 0.0 | 0 | 0.984 | n | — |
| 3 | pymupdf (text layer) | 4,963 | 139 | 35.7 | 27.3 | 0.0 | 0.0 | 0 | 0.0 | n | — |
| 3 | docling v2 (text layer) | 7,303 | 48 | 152.1 | 16.7 | 7.805 | 0.0 | 0 | 0.0 | **y** | GLYPH_DENSITY |
| 3 | docling_ocr v3 (OCR tier) | 4,475 | 37 | 120.9 | 18.9 | 0.0 | 0.0 | 0 | 0.799 | n | — |
| 4 | pymupdf (text layer) | 5,049 | 146 | 34.6 | 37.0 | 0.0 | 0.0 | 0 | 0.0 | n | — |
| 4 | docling v2 (text layer) | 5,833 | 52 | 112.2 | 13.5 | 3.6 | 0.0 | 0 | 0.0 | n | — |
| 4 | docling_ocr v3 (OCR tier) | 4,554 | 38 | 119.8 | 26.3 | 0.0 | 0.0 | 0 | 2.622 | n | — |
| 5 | pymupdf (text layer) | 4,922 | 129 | 38.2 | 20.2 | 0.0 | 0.0 | 0 | 0.0 | n | — |
| 5 | docling v2 (text layer) | 8,860 | 51 | 173.7 | 5.9 | 9.932 | 0.0 | 0 | 0.0 | **y** | GLYPH_DENSITY |
| 5 | docling_ocr v3 (OCR tier) | 4,575 | 43 | 106.4 | 14.0 | 0.0 | 0.0 | 0 | 2.045 | n | — |
| 6 | pymupdf (text layer) | 5,462 | 134 | 40.8 | 20.1 | 0.0 | 0.0 | 0 | 0.0 | n | — |
| 6 | docling v2 (text layer) | 12,456 | 49 | 254.2 | 10.2 | 12.042 | 0.0 | 0 | 0.118 | **y** | GLYPH_DENSITY |
| 6 | docling_ocr v3 (OCR tier) | 5,054 | 29 | 174.3 | 20.7 | 0.0 | 0.0 | 0 | 2.03 | n | — |
| 7 | pymupdf (text layer) | 3,730 | 80 | 46.6 | 16.2 | 0.0 | 0.0 | 0 | 0.0 | n | — |
| 7 | docling v2 (text layer) | 3,737 | 30 | 124.6 | 10.0 | 0.0 | 0.0 | 0 | 0.0 | n | — |
| 7 | docling_ocr v3 (OCR tier) | 3,505 | 18 | 194.7 | 22.2 | 0.0 | 0.0 | 0 | 0.217 | n | — |
| 8 | pymupdf (text layer) | 7,527 | 216 | 34.8 | 36.6 | 0.0 | 0.0 | 0 | 0.0 | n | — |
| 8 | docling v2 (text layer) | 9,442 | 83 | 113.8 | 22.9 | 4.236 | 0.0 | 0 | 0.0 | n | — |
| 8 | docling_ocr v3 (OCR tier) | 6,919 | 61 | 113.4 | 26.2 | 0.0 | 0.0 | 0 | 2.994 | n | — |

#### p699 — 6 pages

| page | source | chars | units | chars/unit | short% | glyph/kchar | repl/kchar | U+FFFD | ltsp% | would-FAIL | criteria |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|:--:|---|
| 1 | pymupdf (text layer) | 6,826 | 148 | 46.1 | 22.3 | 0.0 | 0.0 | 0 | 0.0 | n | — |
| 1 | docling v2 (text layer) | 26,184 | 61 | 429.2 | 31.1 | 16.002 | 0.0 | 0 | 0.0 | **y** | GLYPH_DENSITY |
| 1 | docling_ocr v3 (OCR tier) | 5,699 | 29 | 196.5 | 3.4 | 0.0 | 0.0 | 0 | 4.174 | n | — |
| 2 | pymupdf (text layer) | 2,920 | 68 | 42.9 | 16.2 | 0.0 | 0.0 | 0 | 0.0 | n | — |
| 2 | docling v2 (text layer) | 2,829 | 25 | 113.2 | 16.0 | 0.0 | 0.0 | 0 | 0.0 | n | — |
| 2 | docling_ocr v3 (OCR tier) | 2,739 | 24 | 114.1 | 25.0 | 0.0 | 0.0 | 0 | 1.69 | n | — |
| 3 | pymupdf (text layer) | 4,067 | 100 | 40.7 | 18.0 | 0.0 | 0.0 | 0 | 0.0 | n | — |
| 3 | docling v2 (text layer) | 3,982 | 47 | 84.7 | 17.0 | 0.0 | 0.0 | 0 | 0.0 | n | — |
| 3 | docling_ocr v3 (OCR tier) | 3,833 | 41 | 93.5 | 29.3 | 0.0 | 0.0 | 0 | 1.606 | n | — |
| 4 | pymupdf (text layer) | 4,720 | 126 | 37.5 | 36.5 | 0.0 | 0.0 | 0 | 0.0 | n | — |
| 4 | docling v2 (text layer) | 4,328 | 36 | 120.2 | 25.0 | 0.0 | 0.0 | 0 | 0.0 | n | — |
| 4 | docling_ocr v3 (OCR tier) | 4,174 | 29 | 143.9 | 27.6 | 0.0 | 0.0 | 0 | 2.065 | n | — |
| 5 | pymupdf (text layer) | 4,996 | 119 | 42.0 | 23.5 | 0.0 | 0.0 | 0 | 0.0 | n | — |
| 5 | docling v2 (text layer) | 4,625 | 34 | 136.0 | 20.6 | 0.0 | 0.0 | 0 | 0.0 | n | — |
| 5 | docling_ocr v3 (OCR tier) | 4,430 | 27 | 164.1 | 25.9 | 0.0 | 0.0 | 0 | 1.887 | n | — |
| 6 | pymupdf (text layer) | 6,497 | 183 | 35.5 | 35.5 | 0.0 | 0.0 | 0 | 0.0 | n | — |
| 6 | docling v2 (text layer) | 6,588 | 87 | 75.7 | 9.2 | 0.0 | 0.0 | 0 | 0.0 | n | — |
| 6 | docling_ocr v3 (OCR tier) | 6,160 | 55 | 112.0 | 21.8 | 0.0 | 0.0 | 0 | 4.743 | n | — |

#### p719 — 6 pages

| page | source | chars | units | chars/unit | short% | glyph/kchar | repl/kchar | U+FFFD | ltsp% | would-FAIL | criteria |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|:--:|---|
| 1 | pymupdf (text layer) | 6,075 | 120 | 50.6 | 53.3 | 0.0 | 0.0 | 0 | 0.0 | n | — |
| 1 | docling v2 (text layer) | 45,987 | 19 | 2420.4 | 21.1 | 20.267 | 0.0 | 0 | 0.0 | **y** | GLYPH_DENSITY |
| 1 | docling_ocr v3 (OCR tier) | 5,123 | 30 | 170.8 | 10.0 | 0.0 | 0.0 | 0 | 2.791 | n | — |
| 2 | pymupdf (text layer) | 5,050 | 113 | 44.7 | 65.5 | 0.0 | 0.0 | 0 | 0.0 | n | — |
| 2 | docling v2 (text layer) | 39,857 | 23 | 1732.9 | 21.7 | 20.574 | 0.0 | 0 | 0.0 | **y** | GLYPH_DENSITY |
| 2 | docling_ocr v3 (OCR tier) | 4,307 | 33 | 130.5 | 15.2 | 0.0 | 0.0 | 0 | 3.288 | n | — |
| 3 | pymupdf (text layer) | 4,427 | 170 | 26.0 | 83.5 | 0.0 | 0.0 | 0 | 0.0 | n | — |
| 3 | docling v2 (text layer) | 37,693 | 38 | 991.9 | 28.9 | 20.163 | 0.0 | 0 | 0.0 | **y** | GLYPH_DENSITY |
| 3 | docling_ocr v3 (OCR tier) | 3,860 | 43 | 89.8 | 16.3 | 0.0 | 0.0 | 0 | 0.874 | n | — |
| 4 | pymupdf (text layer) | 5,041 | 279 | 18.1 | 82.8 | 0.0 | 0.0 | 0 | 0.0 | **y** | SHATTERED |
| 4 | docling v2 (text layer) | 56,474 | 47 | 1201.6 | 36.2 | 16.025 | 0.0 | 0 | 0.0 | **y** | GLYPH_DENSITY |
| 4 | docling_ocr v3 (OCR tier) | 6,057 | 46 | 131.7 | 19.6 | 0.0 | 0.0 | 0 | 2.735 | n | — |
| 5 | pymupdf (text layer) | 5,931 | 157 | 37.8 | 67.5 | 0.0 | 0.0 | 0 | 0.0 | n | — |
| 5 | docling v2 (text layer) | 48,988 | 27 | 1814.4 | 25.9 | 20.74 | 0.0 | 0 | 0.0 | **y** | GLYPH_DENSITY |
| 5 | docling_ocr v3 (OCR tier) | 5,085 | 41 | 124.0 | 9.8 | 0.0 | 0.0 | 0 | 1.868 | n | — |
| 6 | pymupdf (text layer) | 5,076 | 101 | 50.3 | 49.5 | 0.0 | 0.0 | 0 | 0.0 | n | — |
| 6 | docling v2 (text layer) | 50,380 | 39 | 1291.8 | 20.5 | 20.623 | 0.0 | 0 | 0.0 | **y** | GLYPH_DENSITY |
| 6 | docling_ocr v3 (OCR tier) | 4,394 | 67 | 65.6 | 22.4 | 0.0 | 0.0 | 0 | 3.563 | n | — |

### A3 — answers

**Which pages FAIL on the text layer.** The cascade's actual attempt-1 text layer for all three
papers is **docling**, so that is the row that governs; the pymupdf row is reported because A1 asked
for it and because it turns out to carry a separate finding (below).

| paper | docling v2 text layer FAILs | share | pymupdf text layer FAILs |
|---|---|---:|---|
| **586** | pages **1, 3, 5, 6** | **4 / 8** | none |
| **699** | page **1** | **1 / 6** | none |
| 719 (control) | pages 1–6 | 6 / 6 | page 4 only (`SHATTERED`) |

**Does the known defect land on those pages.** Partly for 586, exactly for 699 — and the
characterisation differs from the one on record.

* **586 — the defect is NOT one heading, and it is not localized.** 542 GLYPH tokens are spread
  over **7 of 8 pages** (only page 7 is clean): 174 / 12 / 57 / 21 / 88 / 150 / 0 / 40. Of those,
  **109 sit in heading lines and 433 do not**, and every one of the 109 is on page 1. By font:
  `TimesNewRomanPSMT` 244, `TimesNewRomanPS-BoldMT` 149, `CambriaMath` 139, `SymbolMT` 10. Read in
  context, the 433 non-heading tokens are a **mixture of three different things**: list bullets
  (page 6, `- █ To use the generated measurement signal…` — harmless), inline math in CambriaMath
  (page 3, `The MDP is defined as: #!%\+█ )█ %█'] , where + is a set of states…` — note the
  surrounding text is already mojibake independently of the glyph markers), and **lost numeric
  values inside running prose** (page 5, `provides optically-sectioned microscopic images at ███
  fps` and `provides non-optically-sectioned images at ██ fps` — a measurement destroyed in a
  sentence an extractor would read).
* **699 — the defect IS localized, and it is a word-separator defect, not content corruption.**
  All **419 of 419** glyph tokens are on page 1; pages 2–6 carry zero. All 419 are the **same
  glyph in the same font** — `GLYPH<c=1,font=/AOIIJB+MinionPro-Regular>` — and all 419 stand
  **isolated** (419 runs of length 1). In context they occupy exactly the inter-word space
  positions: `using █ the █ plenoptic █ camera █ and █ pre-marked █ positions`. The independent
  PyMuPDF read of the same page confirms it: 397 U+FFFD, of which **370 are isolated singles**
  in the same positions (`using�the�plenoptic�camera`). **No word on p699 is corrupted.** The
  8.631/kchar that routed the whole paper to full-page OCR is an undecodable space glyph in one
  region of one page.

**How many pages would be routed to OCR under page-level gating, and what that would cost.**
Degradation counted on the routed pages alone, by the 07 method with per-page denominators:

| paper | pages routed | v3 degradation on the routed pages | v3 degradation whole-document (page-summed) | degradation avoided |
|---|---:|---|---|---|
| **586** | **4 / 8** | **14 / 24** paras carry a glued run (19 glued tokens) | 25 / 51 | **11 / 27 paragraphs spared** |
| **699** | **1 / 6** | **4 / 7** paras carry a glued run (5 glued tokens) | 19 / 39 | **15 / 32 paragraphs spared** |
| 719 | 6 / 6 | 26 / 50 | 26 / 50 | 0 — nothing to spare |

(The whole-document page-summed figures reproduce 07's published 25/51, 19/39 and 26/50 exactly,
which is the cross-check that the per-page decomposition is sound.)

### A3 — expected vs observed

| | expected in the brief | observed |
|---|---|---|
| **586** | "one or zero pages FAIL; if zero, page-level gating cannot see a single-heading defect" | **4 of 8 FAIL.** The brief's stated reading for this branch applies: *many pages FAIL → the defect is document-wide.* But it is a **partial** mismatch, not a full one — 4 of 8 is document-wide relative to "one heading" and still spares half the document. |
| **699** | "a small set FAILs" | **1 of 6 — held**, and it is the sharpest localization of the three: 419/419 glyphs on one page. |
| 719 | (control) | 6 of 6 FAIL; page-level gating degenerates to document-level, as it should for a whole-document defect. |

> 🔴 **A premise in the record does not survive contact with disk.** `primer.md` §"The open ruling"
> states: *"586's document-wide 7.831/kchar came from ONE corrupted heading, and routed the whole
> paper to OCR for a localized defect."* **That causal claim is false.** 07's own count — `1/16`
> headings glyph-corrupted — counted *heading lines*, not glyph tokens; 542 tokens on 7 pages, 433
> of them outside any heading, produce the 7.831. The task's ASSUMPTION LEDGER phrases the same
> fact correctly and narrowly ("586 has a single corrupted heading on v2 with document-wide glyph
> density 7.8/k" — both halves true, no causal link asserted), so the ledger is not contradicted;
> the primer's inference from it is. **586 is not the clean single-heading hybrid case it has been
> carried as. 699 is a cleaner localized case than either document claims, and of a different
> kind — a space glyph, not a content defect.** Treating 586 and 699 as one case is the thing this
> probe most clearly disturbs.

---

## 3. Probe B — pymupdf4llm hybrid OCR

**Versions** (scratch venv `/tmp/pg08/b_venv`, Python 3.12, aarch64; the project `.venv` was not
touched and its `pip freeze` is byte-identical before and after):

`pymupdf4llm 1.28.2` · `pymupdf 1.28.2` · `pymupdf_layout 1.28.2` · `onnxruntime 1.29.0` ·
`numpy 2.5.3` · `rapidocr 3.6.0` (installed to match the project's version) · `opencv_python
5.0.0.93` · `Shapely 2.1.2` · `pyclipper 1.4.0`. Layout path active (`_use_layout=True`). Run with
library defaults: `use_ocr=True`, `force_ocr=False`, `ocr_dpi=150`, `page_chunks=True`.

**Engine finding (I3).** RapidOCR is supported and is what the library selects here, unprompted.
No system package was needed and nothing had to compile.

**Attribution finding (I4).** Page-level attribution is available and was captured; region-level is
not exposed. Per-page decisions, read from `make_ocr_decision` / `analyze_page` and confirmed
against the `OCR on page.number=…` lines in `INFO_MESSAGES`:

| paper | pages OCR'd | pages left to the text layer | why the untouched pages were skipped |
|---|---|---|---|
| **586** | 2, 3, 4, 5, 6, 7 | **1, 8** | both have `img_area=0.0`, `only_text=True`, `needs_ocr=False` |
| **699** | 2, 3, 4 | **1**, 5, 6 | page 1 `img_area=0.0`, `only_text=True`, `needs_ocr=False` |
| 719 | 1, 2, 3, 4, 5, 6 | none | page 1 has `img_area=0.0` yet `needs_ocr=True` — the decision model fired on the text itself |

> 🔴 **The pages the hybrid OCR'd are not the pages that are broken.** On both 586 and 699 the
> single most corrupted page is exactly the page the hybrid **declined** to OCR. The decision is
> dominated by `img_area` — pages carrying figures get OCR'd, text-only pages do not — so the
> repair that landed on 586 pages 2–7 and 699 pages 2–4 is **figure-region OCR that repaired the
> glyph damage incidentally**, not garble detection. On 719 the ONNX decision model
> (`ocr/ocr_decision_model.onnx`) did fire on a text-only page, so the model *can* detect
> illegibility — it did not judge 586 p1 or 699 p1 illegible.

**B3 — scores against v2 and v3** (07 method; gate metrics on the whole document):

| paper | version | chars | c/unit | short% | glyph/k | repl/k | ltsp% | GATE | headings | hdg glyphed | glued tokens | paras | paras glyph-free | paras glued | 07 anchor recall |
|---|---|---:|---:|---:|---:|---:|---:|:--:|---:|---:|---:|---:|---:|---:|:--:|
| **586** | v2 docling | 69,214 | 168.4 | 12.9 | 7.831 | 0.0 | 0.016 | **FAIL** | 16 | 1 | 0 | 54 | 30 | 0 | 9/9 |
| | v3 docling_ocr | 40,486 | 134.5 | 19.6 | 0.0 | 0.0 | 1.935 | pass | 16 | 0 | 38 | 51 | 51 | **25** | 9/9 |
| | **v4h pymupdf4llm** | 46,750 | 107.0 | 16.0 | **0.0** | **5.027** | **0.144** | **FAIL** | 15 | 0 | **1** | 70 | 70 | **1** | **9/9** |
| **699** | v2 docling | 48,546 | 166.8 | 19.2 | 8.631 | 0.0 | 0.0 | **FAIL** | 15 | 0 | 0 | 40 | 37 | 0 | 12/12 |
| | v3 docling_ocr | 27,045 | 130.7 | 22.7 | 0.0 | 0.0 | 2.744 | pass | 15 | 0 | 33 | 39 | 39 | **19** | 12/12 |
| | **v4h pymupdf4llm** | 30,988 | 103.3 | 17.7 | **0.0** | **12.811** | **0.023** | **FAIL** | 14 | 0 | **0** | 53 | 53 | **0** | **12/12** |
| **719** | v2 docling | 279,389 | 1447.6 | 26.9 | 19.586 | 0.0 | 0.0 | **FAIL** | 16 | 16 | 0 | 91 | 0 | 0 | **0/8** |
| | v3 docling_ocr | 28,836 | 110.9 | 16.5 | 0.0 | 0.0 | 2.441 | pass | 15 | 0 | 51 | 50 | 50 | 26 | 8/8 |
| | **v4h pymupdf4llm** | 29,988 | 99.0 | 18.8 | **0.0** | **0.0** | **0.842** | **pass** | 14 | 0 | **25** | 62 | 62 | **14** | **8/8** |

**Denominator note.** 07's `x/51` and `x/39` are v3's own paragraph counts; paragraph segmentation
is parser-dependent, so the hybrid's denominators are its own (70, 53, 62). The 07 method computes
the denominator from the text under test, and it was applied unmodified. The comparable quantities
across parsers are the *rates* — glued tokens, `ltsp%`, and anchor recall.

**Where the hybrid's U+FFFD sit.** Entirely on the pages it declined to OCR — 586 pages 1 (188)
and 8 (47), 699 page 1 (397), 719 none. Their shape differs by paper and matches the v2 glyph
shape exactly: on **699 p1**, 370 of 397 are **isolated singles in space positions**; on **586 p1**,
186 of 188 fall in **8 contiguous runs of ≥5** — the corrupted title block — with 2 isolated
singles inline; 586 p8's 47 are one 46-char run plus one inline single (`Richards�Kortum`).

### B3 — expected vs observed

| | expected in the brief | observed |
|---|---|---|
| headings repaired at or near v3 quality | — | **Met where the hybrid ran, not where it mattered.** No glyph-corrupted heading survives in any hybrid output (`hdg glyphed = 0` everywhere), and anchor recall is **9/9, 12/12, 8/8** — full, matching v3, and on 719 recovering a document v2 lost entirely (0/8). But on 586 the corrupted title block was **not repaired**; it is re-expressed as 186 U+FFFD rather than as GLYPH markers. |
| body degradation at or near v2, i.e. near zero | — | **Met, decisively.** 586: **1 glued token and 1 degraded paragraph** against v3's 38 and 25. 699: **0 and 0** against v3's 33 and 19. 719: 25 and 14/62 against v3's 51 and 26/50 — roughly **half** the gluing of the shipped OCR tier, at the same anchor recall. `ltsp%` falls 1.935→0.144 (586), 2.744→0.023 (699), 2.441→0.842 (719). |
| *mismatch reading — "if body degradation approaches v3's, the 'original text never replaced' claim does not hold"* | — | **The claim holds.** Degradation did not approach v3's on any of the three; the culling mechanism (`exec_ocr_interface.py:215-232`) demonstrably keeps legible text out of the OCR pixmap. |
| *mismatch reading — "if headings are not repaired, the garble detector did not trigger on our glyph pattern"* | — | **This is the branch that fired, and the mechanism is now known.** The detector keys on two things only: a span is sent to OCR if it is previously-OCR'd or **contains U+FFFD** (`exec_ocr_interface.py:218-224`); everything else is culled as "good text". The page-level gate in front of it is dominated by `img_area`. Our corruption on 586 p1 and 699 p1 is on text-only pages, so it never reached the span-level test. |

**Net.** The hybrid is the only one of the three parsers that is **non-destructive** on 586 and 699:
it preserves the body text almost exactly (1 and 0 degraded paragraphs) *and* achieves full anchor
recall. It is also the only one that **fails the shipped gate on those two papers** — and it fails
on `REPLACEMENT_DENSITY`, a criterion the gate already ships, firing on the genuinely damaged
regions and nothing else, page-localized to 586 {1, 8} and 699 {1}. On 719 it passes the gate
outright with half of v3's word-gluing.

---

## 4. Acceptance gates

| # | gate | result |
|---|---|---|
| 1 | I1–I5 each resolved with a stated source | **PASS** — §1, each with file/line, package path, or transcript line |
| 2 | Probe A table complete for both papers and both versions; A3 answered with counts | **PASS** — §2; three sources per page rather than two, 719 carried as control |
| 3 | Probe B complete with B3 scores for all three papers | **PASS** — §3; not stopped at B1, RapidOCR was usable |
| 4 | `review.db` 99,770,368 B / mtime 2026-09-08 03:48:48.73 UTC unchanged; six parsed_text files unchanged | **PASS** — size and mtime exact to the nanosecond (`…48.732577380 +0000`); all six sha256 identical before and after; `full_text_assets`=794 and `parse_attempts`=8 as at session start |
| 5 | `git status --porcelain -uall` shows only the read-out file; `.venv` unchanged | **PASS** — porcelain lists only this file; `pip freeze` diff empty (168 packages before and after) |
| 6 | no Ollama model load | **PASS** — `ollama ps` still `gemma3:27b` only, `NRestarts=0`, `ExecMainStartTimestamp` still 2026-08-31 00:41:59; journal for the window shows only `HEAD /` and `GET /api/ps` polls, no runner start. Both probes are classical OCR on CPU. |

**Wall time:** ~35 minutes, well inside the 60-minute budget. All probe outputs are under
`/tmp/pg08/` (`pages/`, `probeB/`, `probeA_rows.json`, `probeB_summary.json`, the five probe
scripts); nothing was written under `data/`.

---

## 5. Observed, not asked about

1. **The shipped gate has a blind spot the control paper walks straight through.** PyMuPDF's text
   layer on **719** — a document 07 correctly called "Caesar-shifted gibberish" — **passes the gate
   on 5 of its 6 pages** (page 4 fails, on `SHATTERED`, incidentally). The extracted text is
   `$\x030XOWL\x10%HKDYLRU\x03$OJRULWKP` for "A Multi-Behavior Algorithm": a broken cmap maps every
   glyph to a **valid but wrong codepoint**, so there are no `GLYPH<>` markers, no U+FFFD, and
   nothing for `GLYPH_DENSITY` or `REPLACEMENT_DENSITY` to see. Only docling's marker convention
   made 719 visible to the gate at all. This is a **third** un-instrumented defect class alongside
   word-gluing, and unlike gluing it is undetectable by any current criterion; had 719 been routed
   to PyMuPDF rather than docling it would have entered the corpus silently.
2. **U+FFFD is a live, already-shipped signal for exactly this corruption — from the other
   parser.** The same regions docling marks `GLYPH<…>` come through PyMuPDF's layout path as
   U+FFFD, at page-level densities of 29.1/k (586 p1), 6.2/k (586 p8) and 59.3/k (699 p1), against
   a shipped limit of 1.0. `REPLACEMENT_DENSITY` was added for p262's six stray characters; it
   turns out to localize this defect class cleanly and by page. Note the naive `page.get_text("text")`
   does **not** produce these — it yields `\x01` and other control characters (0 U+FFFD on every
   page of all three papers). The U+FFFD appear only on pymupdf4llm's layout path.
3. **699's whole gate failure is an undecodable space.** Recorded here because the cheapest
   conceivable remedy for it involves no OCR of any kind, and because a paper whose only defect is
   its word separators has been carried in the same category as 719, whose every word was
   destroyed.
4. **`review.db-shm` was touched by this session's read-only opens** (mtime 22:20:41; 32,768 B).
   A read-only connection to a WAL-mode database maps the shared-memory index — no data changed,
   and `review.db` itself is byte- and mtime-identical. Separately, `review.db-wal` exists at
   **0 bytes** with mtime **2026-09-08 10:30:35**, i.e. created ~12 h before this session by
   something else — the timestamp coincides with the 10:30 UTC user-tier snapshot run. `primer.md`
   currently says "The WAL is **gone — checkpointed**"; a zero-byte WAL is equivalent in content
   but the file is present again, which matters to anyone using its absence as a check.
5. **Both parse tiers are byte-deterministic**, including the RapidOCR one, reproduced 19 hours
   after the originals (§1). Worth knowing before any future A/B is designed around re-parsing.
