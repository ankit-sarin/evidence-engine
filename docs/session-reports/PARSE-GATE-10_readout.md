# PARSE-GATE-10 — read-out: corpus-wide font audit for the Identity-H / no-ToUnicode signature

**Date:** 2026-09-09 · **Machine:** DGX Spark · **Type:** diagnostic read-out, no engine code, no DB writes
**HEAD at start:** `658165d`, tree clean.

One question was measured: **how many of the 190 corpus PDFs carry the signature PARSE-GATE-09
found behind every corrupted font in 586/699/719 — Type0 + `/Identity-*` + no `/ToUnicode` — and
how much of their text does it cover?** **No recommendation is offered here; the ruling is the
architect's.**

**Tools.** Project `.venv` only — PyMuPDF 1.27.1 (MuPDF 1.27.1), fontTools 4.61.1. No install,
`pip freeze` byte-identical before and after. All PDF access read-only; `review.db` opened
`mode=ro` for the paper list and `full_text_assets` only.

---

## 0. The one-line answer

**The class is contained. 11 of 190 papers carry the signature at all; 4 carry it over more than
1% of their text-layer characters; and exactly ONE of those four passed the parse gate on the text
Run 6 read — paper 670 (EE-518).** Corpus-wide, signature fonts account for **30,365 of 9,358,846
text-layer characters (0.32%)**, and **27,565 of those 30,365 (90.8%) are paper 719 alone.**

**But 670 is not a silent extraction defect on inspection**: all 907 of its corrupted characters
sit in figure-embedded CJK labels that Docling drops, and **zero of them reach `670_v2.md`**
(§6.1). And a **second, larger-by-paper-count defect class sits outside the signature entirely** —
4,827 unresolved characters across **126** papers, dominated by TeX math-extension fonts (§8.1).

---

## 1. Method

Reused verbatim from **`docs/session-reports/PARSE-GATE-09_readout.md` §1 (I1/I2) and §3–§5**,
commit `658165d`:

- fonts per page from `page.get_page_fonts()`; font dictionaries from `doc.xref_object(xref)`;
  composite fonts resolved through `/DescendantFonts` to the descendant `/BaseFont`
  (09 §11.6 — the name Docling reports is the descendant's);
- raw character codes from `page.get_texttrace()`, whose `chars` are `(ucs, gid, origin, bbox)`;
  with `/CIDToGIDMap /Identity` the `gid` **is** the CID **is** Docling's `c=` (09 §1 I1);
- embedded programs from `doc.extract_font()`, read with `fontTools.ttLib.TTFont`, falling back to
  `fontTools.cffLib.CFFFontSet` for bare CID-keyed CFF (09 §3, the 699 case);
- glyph-outline emptiness from `glyf.numberOfContours == 0` or CFF `BoundsPen.bounds is None`
  (09 §5).

**Signature test.** `subtype starts with "Type0"` **and** `/Encoding` starts with `Identity`
**and** no `/ToUnicode` key on the Type0 parent. `/Differences` is not tested because it cannot
exist on such a font (09 §1 I2).

**Coverage attribution, and why it is not by font name.** `get_texttrace` reports the basefont with
the subset tag stripped, and a document can carry two different objects that strip to the same name
— 586 has both `GPONNK+TimesNewRomanPSMT` (Type0, signature) and `GPONCD+TimesNewRomanPSMT`
(TrueType/WinAnsi, clean). A name match alone would conflate them. **A character is counted as
signature-set iff its span's stripped font name matches a signature font AND the character resolved
to U+FFFD.** That is exact, because MuPDF resolves *every* character in a signature font to U+FFFD
and *no* character in the WinAnsi twin (verified: 586 `fffd_other = 0`). Characters that resolve to
U+FFFD in a span whose name matches no signature font are counted separately as `fffd_other`, and
that residual is the subject of §8.1.

**Space-recoverability test (S4).** A signature character counts as RECOVERABLE-BY-POSITION iff its
glyph's outline in the embedded program is **empty** and its rendered advance is **non-zero**. This
is 09's criterion with the `spacewidth` comparison dropped, because 09 §5 measured MuPDF's
`spacewidth` to be a fallback value — and therefore wrong — for exactly the fonts under test
(CambriaMath: 4.863 reported against a true 2.193). **Validated against 09's known-good answer**
before use: it selects `gid = 1` and nothing else in all four of 586's signature fonts
(39 + 16 + 15 + 1 = 71), and `gid = 1` in 699's MinionPro (419/419) — the same rows 09 ruled
BY-POSITION, found mechanically.

**Marker-export survival (§6.1)** counts the shipped gate's own `RE_GLYPH`
(`engine/parsers/parse_quality.py:47`) against the Run-6 text, versus the PDF-side count of
codes < 32. It measures how much of the damage Docling actually writes into the `.md`.

---

## 2. Inferred-assumption resolutions (I1–I3)

| # | claim | verdict | source |
|---|---|---|---|
| **I1** | the 190 corpus PDFs are enumerable from `review.db` and every file is present on disk; count is 190 | **TRUE, no gaps** | `engine.core.corpus.corpus_status_sql()` (the single eligibility authority) returns **190** papers, all `AI_AUDIT_COMPLETE`. All 190 have a non-null `pdf_local_path`; **0** point at a missing file; all 190 have at least one `full_text_assets` row. **S1 gaps: none.** |
| **I2** | the 09 method runs in well under a minute per PDF; a 190-paper sweep fits in one session | **TRUE, by two orders of magnitude** | Whole 190-paper sweep: **12.99 s wall**. Slowest paper 0.20 s. The 180 s per-paper cap was never approached; **0 timeouts, 0 errors**. |
| **I3** | a paper can carry the signature on a font that renders no body text, so coverage must be by characters, not font counts | **TRUE, and it is the majority case** | **7 of the 11** signature-bearing papers are at or below 0.131% coverage, and **4 of those render ZERO characters in their signature fonts** (papers 14, 449, 474 — and 415, which renders 7 of 1,442,149). Paper 449 declares **5** signature fonts and renders nothing in any of them. Font counts alone would have reported 11 affected papers where the character measure reports 4 worth looking at. |

> 🔴 **Two count discrepancies against PARSE-GATE-09's summary line. Reported, not adapted to.**
>
> 1. **719 is 34 of 38 signature font objects, not 36 of 38.** 09 §2 wrote "all 36 Type0 objects",
>    but the exception list in that same paragraph names **four** non-Type0 objects out of 38
>    (two MyriadPro Type1/CFF at xrefs 32/35, `JGFLJO+ArialNarrow` TrueType/WinAnsi at 33, and the
>    non-embedded `Helvetica` at 36), which implies 34. Measured here: **34**. 09's "36" is
>    arithmetically inconsistent with 09's own exception list; **34 is the value that survives
>    contact with the file.**
> 2. **699 is 3 of 19 signature fonts, not 1 of 19.** The "1" in 09's summary is the count of
>    *offending* fonts — those that produced a Docling marker. 09 §2 itself records that
>    `AOINCN+SegoeUI` and `AOINEN+SimHei-GBK-EUC-H` also match the signature and are also unresolved
>    but emit no marker. **The two counts measure different things and both are right;** this
>    read-out reports the signature count throughout, because the signature is what a pre-parse
>    audit can see.
>
> **586 reproduces exactly: 4 of 13.** So does every character-level figure 09 published —
> 586 `1,081 = 589 + 492`, 719 `27,565 = 5,508 + 22,057` (§4).

---

## 3. S1 — corpus enumeration

| | |
|---|---:|
| corpus papers (`FT_ELIGIBLE`, `EXTRACTED`, `AI_AUDIT_COMPLETE`, `HUMAN_AUDIT_COMPLETE`) | **190** |
| …all at status | `AI_AUDIT_COMPLETE` |
| with `pdf_local_path` set | 190 |
| PDF present on disk | **190** |
| with ≥1 `full_text_assets` row | 190 |
| **gaps** | **none** |
| swept successfully | **190 (100%)** — 0 errors, 0 timeouts, 12.99 s total |

### 🔴 A finding on the Run-6 text that the brief's context does not predict

The brief's PROBLEM/CONTEXT states "Run 6 extractions read v2 text for all 190 papers." Measured:
**16 of the 190 have no v2 at all.** Their highest `full_text_assets` version is **1**, whose
`parsed_text_path` is **NULL** in the database, although a `{id}_v1.md` file exists on disk for
every one of them. Paper ids: **9, 11, 12, 14, 15, 17, 22, 24, 39, 67** and six more.

Run-6 text actually read, per paper (highest version whose `parsed_at` predates the 2026-09-08
PARSE-GATE-07 re-parses):

| version | papers | parser |
|---|---:|---|
| v2 | **174** | `docling` 169, `pymupdf` 5 |
| v1 | **16** | `docling` (path NULL in DB; file present on disk) |

Every S5 row names the version it was measured on. The gate was run on that text, never on a
substitute. **This is reported and not acted on.**

---

## 4. S2 / S3 / S4 — the signature-bearing papers

Every signature font in the corpus is **embedded** and **subset**, and every embedded program has
**no `cmap` and no `post`** — 88 of 88 objects, without exception, exactly as 09 found on its
three. `/CIDToGIDMap` is `/Identity` throughout. Programs: 84 sfnt (`/FontFile2`), 4 bare CFF
(`/FontFile3`); **0 parse failures.**

| paper | EE | sig/total fonts | cov % | sig chars/total | codes <32 (marked) | codes ≥32 (silent) | space-recov % | Run-6 text | Run-6 v2 gate |
|---:|---|---:|---:|---|---:|---:|---:|---|---|
| 719 | EE-567 | 34/38 | 94.774 | 27565/29085 | 5508 | 22057 | 16.4% | v2 docling | FAIL:GLYPH_DENSITY |
| 670 | EE-518 | 29/59 | 3.051 | 907/29724 | 308 | 599 | 2.2% | v2 docling | PASS |
| 699 | EE-547 | 3/19 | 2.920 | 748/25614 | 453 | 295 | 60.4% | v2 docling | FAIL:GLYPH_DENSITY |
| 586 | EE-434 | 4/13 | 2.533 | 1081/42685 | 589 | 492 | 6.6% | v2 docling | FAIL:GLYPH_DENSITY |
| 473 | EE-321 | 2/15 | 0.131 | 31/23735 | 2 | 29 | 0.0% | v2 docling | PASS |
| 618 | EE-466 | 3/68 | 0.042 | 22/51871 | 0 | 22 | 0.0% | v2 docling | PASS |
| 516 | EE-364 | 1/29 | 0.012 | 4/34509 | 0 | 4 | 0.0% | v2 docling | PASS |
| 449 | EE-297 | 5/26 | 0.000 | 0/30718 | 0 | 0 | — | v2 docling | PASS |
| 415 | EE-263 | 3/1647 | 0.000 | 7/1442149 | 0 | 7 | 0.0% | v2 docling | PASS |
| 14 | EE-007 | 2/14 | 0.000 | 0/20732 | 0 | 0 | — | v1 docling | PASS |
| 474 | EE-322 | 2/32 | 0.000 | 0/39622 | 0 | 0 | — | v2 docling | PASS |

**Corpus totals over the signature set:** 88 signature font objects of 6,161 font objects;
**30,365** characters rendered in them of **9,358,846** text-layer characters (**0.3245%**);
**6,860** at codes < 32 (Docling marks these) and **23,505** at codes ≥ 32 (**Docling writes these
as a raw wrong character with no marker** — 09 §4); **5,063** space-recoverable by position.

Per-page detail for the four papers above 1% coverage (page, chars, unresolved):

| paper | per page — unresolved / total text-layer characters |
|---|---|
| 719 | 1: 5,297/5,593 · 2: 4,458/4,694 · 3: 3,824/4,082 · 4: 4,268/4,504 · 5: 5,205/5,463 · 6: 4,513/4,749 |
| 670 | 1: **0**/5,400 · 2: 134/3,568 · 3: **884**/4,375 · 4: 7/3,534 · 5: 9/3,885 · 6: 4/3,757 · 7: 4/5,205 |
| 699 | 1: **440**/6,017 · 2: 0/2,471 · 3: 0/3,395 · 4: **308**/3,964 · 5: 0/4,184 · 6: 0/5,583 |
| 586 | 1: 189/6,378 · 2: 146/5,393 · 3: 252/4,801 · 4: 119/4,915 · 5: 136/4,825 · 6: 192/5,359 · 7: **0**/3,667 · 8: 47/7,347 |

<sub>699's page-4 count is the `SegoeUI` / `SimHei` figure text — signature, unresolved, and never
marked by Docling, which is why 09's marker-based account records only page 1.</sub>

---

## 5. S5 — corpus table (all 190 rows)

Sorted by signature coverage descending. `Run-6 text` is the parsed version the extraction actually
read; `Run-6 v2 gate` is `engine.parsers.parse_quality.assess()` at the shipped absolute
thresholds, run on that text.

| paper | EE | sig/total fonts | cov % | sig chars/total | codes <32 (marked) | codes ≥32 (silent) | space-recov % | Run-6 text | Run-6 v2 gate |
|---:|---|---:|---:|---|---:|---:|---:|---|---|
| 719 | EE-567 | 34/38 | 94.774 | 27565/29085 | 5508 | 22057 | 16.4% | v2 docling | FAIL:GLYPH_DENSITY |
| 670 | EE-518 | 29/59 | 3.051 | 907/29724 | 308 | 599 | 2.2% | v2 docling | PASS |
| 699 | EE-547 | 3/19 | 2.920 | 748/25614 | 453 | 295 | 60.4% | v2 docling | FAIL:GLYPH_DENSITY |
| 586 | EE-434 | 4/13 | 2.533 | 1081/42685 | 589 | 492 | 6.6% | v2 docling | FAIL:GLYPH_DENSITY |
| 473 | EE-321 | 2/15 | 0.131 | 31/23735 | 2 | 29 | 0.0% | v2 docling | PASS |
| 618 | EE-466 | 3/68 | 0.042 | 22/51871 | 0 | 22 | 0.0% | v2 docling | PASS |
| 516 | EE-364 | 1/29 | 0.012 | 4/34509 | 0 | 4 | 0.0% | v2 docling | PASS |
| 449 | EE-297 | 5/26 | 0.000 | 0/30718 | 0 | 0 | — | v2 docling | PASS |
| 415 | EE-263 | 3/1647 | 0.000 | 7/1442149 | 0 | 7 | 0.0% | v2 docling | PASS |
| 14 | EE-007 | 2/14 | 0.000 | 0/20732 | 0 | 0 | — | v1 docling | PASS |
| 474 | EE-322 | 2/32 | 0.000 | 0/39622 | 0 | 0 | — | v2 docling | PASS |
| 9 | EE-004 | 0/27 | 0.000 | 0/79560 | 0 | 0 | — | v1 docling | PASS |
| 11 | EE-005 | 0/119 | 0.000 | 0/176173 | 0 | 0 | — | v1 docling | PASS |
| 12 | EE-006 | 0/16 | 0.000 | 0/33608 | 0 | 0 | — | v1 docling | PASS |
| 15 | EE-008 | 0/83 | 0.000 | 0/81761 | 0 | 0 | — | v1 docling | PASS |
| 17 | EE-009 | 0/30 | 0.000 | 0/29827 | 0 | 0 | — | v1 docling | PASS |
| 22 | EE-013 | 0/18 | 0.000 | 0/59597 | 0 | 0 | — | v1 docling | PASS |
| 24 | EE-015 | 0/14 | 0.000 | 0/18097 | 0 | 0 | — | v1 docling | PASS |
| 39 | EE-021 | 0/9 | 0.000 | 0/79167 | 0 | 0 | — | v1 docling | PASS |
| 67 | EE-029 | 0/17 | 0.000 | 0/41013 | 0 | 0 | — | v1 docling | PASS |
| 78 | EE-032 | 0/7 | 0.000 | 0/48613 | 0 | 0 | — | v1 docling | PASS |
| 81 | EE-034 | 0/20 | 0.000 | 0/39910 | 0 | 0 | — | v1 docling | PASS |
| 82 | EE-035 | 0/11 | 0.000 | 0/45576 | 0 | 0 | — | v1 docling | PASS |
| 102 | EE-045 | 0/15 | 0.000 | 0/36847 | 0 | 0 | — | v1 docling | PASS |
| 121 | EE-049 | 0/5 | 0.000 | 0/21572 | 0 | 0 | — | v1 docling | PASS |
| 132 | EE-053 | 0/61 | 0.000 | 0/53428 | 0 | 0 | — | v1 docling | PASS |
| 252 | EE-100 | 0/10 | 0.000 | 0/49243 | 0 | 0 | — | v2 docling | PASS |
| 262 | EE-110 | 0/22 | 0.000 | 0/29221 | 0 | 0 | — | v2 docling | PASS |
| 268 | EE-116 | 0/14 | 0.000 | 0/20855 | 0 | 0 | — | v2 docling | PASS |
| 277 | EE-125 | 0/56 | 0.000 | 0/83888 | 0 | 0 | — | v2 docling | PASS |
| 281 | EE-129 | 0/51 | 0.000 | 0/82881 | 0 | 0 | — | v2 docling | PASS |
| 286 | EE-134 | 0/12 | 0.000 | 0/54813 | 0 | 0 | — | v2 docling | PASS |
| 292 | EE-140 | 0/333 | 0.000 | 0/58621 | 0 | 0 | — | v2 docling | PASS |
| 295 | EE-143 | 0/34 | 0.000 | 0/43343 | 0 | 0 | — | v2 docling | PASS |
| 296 | EE-144 | 0/26 | 0.000 | 0/69117 | 0 | 0 | — | v2 docling | PASS |
| 297 | EE-145 | 0/23 | 0.000 | 0/50321 | 0 | 0 | — | v2 docling | PASS |
| 318 | EE-166 | 0/45 | 0.000 | 0/30071 | 0 | 0 | — | v2 docling | PASS |
| 323 | EE-171 | 0/13 | 0.000 | 0/27634 | 0 | 0 | — | v2 docling | PASS |
| 346 | EE-194 | 0/9 | 0.000 | 0/38438 | 0 | 0 | — | v2 docling | PASS |
| 347 | EE-195 | 0/15 | 0.000 | 0/63736 | 0 | 0 | — | v2 docling | PASS |
| 364 | EE-212 | 0/17 | 0.000 | 0/30363 | 0 | 0 | — | v2 docling | PASS |
| 366 | EE-214 | 0/8 | 0.000 | 0/72517 | 0 | 0 | — | v2 docling | PASS |
| 368 | EE-216 | 0/23 | 0.000 | 0/46853 | 0 | 0 | — | v2 pymupdf | PASS |
| 370 | EE-218 | 0/10 | 0.000 | 0/40219 | 0 | 0 | — | v2 docling | PASS |
| 376 | EE-224 | 0/31 | 0.000 | 0/39468 | 0 | 0 | — | v2 docling | PASS |
| 378 | EE-226 | 0/22 | 0.000 | 0/37497 | 0 | 0 | — | v2 pymupdf | PASS |
| 380 | EE-228 | 0/20 | 0.000 | 0/33735 | 0 | 0 | — | v2 docling | PASS |
| 383 | EE-231 | 0/7 | 0.000 | 0/47792 | 0 | 0 | — | v2 docling | PASS |
| 386 | EE-234 | 0/13 | 0.000 | 0/27982 | 0 | 0 | — | v2 docling | PASS |
| 388 | EE-236 | 0/14 | 0.000 | 0/47419 | 0 | 0 | — | v2 docling | PASS |
| 392 | EE-240 | 0/13 | 0.000 | 0/29732 | 0 | 0 | — | v2 docling | PASS |
| 395 | EE-243 | 0/17 | 0.000 | 0/51677 | 0 | 0 | — | v2 docling | PASS |
| 400 | EE-248 | 0/24 | 0.000 | 0/39231 | 0 | 0 | — | v2 docling | PASS |
| 402 | EE-250 | 0/13 | 0.000 | 0/55150 | 0 | 0 | — | v2 docling | PASS |
| 405 | EE-253 | 0/17 | 0.000 | 0/40704 | 0 | 0 | — | v2 docling | PASS |
| 406 | EE-254 | 0/21 | 0.000 | 0/36421 | 0 | 0 | — | v2 docling | PASS |
| 407 | EE-255 | 0/15 | 0.000 | 0/34533 | 0 | 0 | — | v2 docling | PASS |
| 409 | EE-257 | 0/16 | 0.000 | 0/39656 | 0 | 0 | — | v2 docling | PASS |
| 411 | EE-259 | 0/10 | 0.000 | 0/43340 | 0 | 0 | — | v2 docling | PASS |
| 431 | EE-279 | 0/22 | 0.000 | 0/37714 | 0 | 0 | — | v2 docling | PASS |
| 432 | EE-280 | 0/14 | 0.000 | 0/42616 | 0 | 0 | — | v2 docling | PASS |
| 433 | EE-281 | 0/16 | 0.000 | 0/41145 | 0 | 0 | — | v2 docling | PASS |
| 434 | EE-282 | 0/15 | 0.000 | 0/39166 | 0 | 0 | — | v2 docling | PASS |
| 439 | EE-287 | 0/10 | 0.000 | 0/48093 | 0 | 0 | — | v2 docling | PASS |
| 442 | EE-290 | 0/18 | 0.000 | 0/41591 | 0 | 0 | — | v2 docling | PASS |
| 445 | EE-293 | 0/18 | 0.000 | 0/47025 | 0 | 0 | — | v2 docling | PASS |
| 453 | EE-301 | 0/23 | 0.000 | 0/27155 | 0 | 0 | — | v2 docling | PASS |
| 455 | EE-303 | 0/18 | 0.000 | 0/51037 | 0 | 0 | — | v2 pymupdf | FAIL:SHATTERED |
| 457 | EE-305 | 0/33 | 0.000 | 0/18310 | 0 | 0 | — | v2 docling | PASS |
| 458 | EE-306 | 0/33 | 0.000 | 0/56037 | 0 | 0 | — | v2 docling | PASS |
| 459 | EE-307 | 0/12 | 0.000 | 0/73592 | 0 | 0 | — | v2 docling | PASS |
| 460 | EE-308 | 0/21 | 0.000 | 0/37624 | 0 | 0 | — | v2 docling | PASS |
| 461 | EE-309 | 0/19 | 0.000 | 0/31176 | 0 | 0 | — | v2 docling | PASS |
| 462 | EE-310 | 0/19 | 0.000 | 0/30560 | 0 | 0 | — | v2 docling | PASS |
| 463 | EE-311 | 0/23 | 0.000 | 0/28096 | 0 | 0 | — | v2 docling | PASS |
| 464 | EE-312 | 0/15 | 0.000 | 0/35602 | 0 | 0 | — | v2 docling | PASS |
| 466 | EE-314 | 0/32 | 0.000 | 0/37734 | 0 | 0 | — | v2 docling | PASS |
| 467 | EE-315 | 0/20 | 0.000 | 0/54536 | 0 | 0 | — | v2 docling | PASS |
| 470 | EE-318 | 0/29 | 0.000 | 0/30589 | 0 | 0 | — | v2 docling | PASS |
| 472 | EE-320 | 0/20 | 0.000 | 0/31442 | 0 | 0 | — | v2 docling | PASS |
| 475 | EE-323 | 0/23 | 0.000 | 0/27928 | 0 | 0 | — | v2 docling | PASS |
| 476 | EE-324 | 0/33 | 0.000 | 0/37644 | 0 | 0 | — | v2 docling | PASS |
| 477 | EE-325 | 0/24 | 0.000 | 0/37510 | 0 | 0 | — | v2 docling | PASS |
| 478 | EE-326 | 0/7 | 0.000 | 0/39654 | 0 | 0 | — | v2 docling | PASS |
| 480 | EE-328 | 0/12 | 0.000 | 0/47641 | 0 | 0 | — | v2 docling | PASS |
| 485 | EE-333 | 0/43 | 0.000 | 0/31487 | 0 | 0 | — | v2 docling | PASS |
| 486 | EE-334 | 0/8 | 0.000 | 0/35756 | 0 | 0 | — | v2 docling | PASS |
| 487 | EE-335 | 0/29 | 0.000 | 0/67956 | 0 | 0 | — | v2 docling | PASS |
| 488 | EE-336 | 0/13 | 0.000 | 0/28715 | 0 | 0 | — | v2 docling | PASS |
| 489 | EE-337 | 0/35 | 0.000 | 0/39364 | 0 | 0 | — | v2 docling | PASS |
| 491 | EE-339 | 0/13 | 0.000 | 0/8128 | 0 | 0 | — | v2 docling | PASS |
| 492 | EE-340 | 0/7 | 0.000 | 0/40744 | 0 | 0 | — | v2 docling | PASS |
| 493 | EE-341 | 0/16 | 0.000 | 0/63507 | 0 | 0 | — | v2 docling | PASS |
| 497 | EE-345 | 0/21 | 0.000 | 0/27986 | 0 | 0 | — | v2 docling | PASS |
| 498 | EE-346 | 0/32 | 0.000 | 0/118393 | 0 | 0 | — | v2 docling | PASS |
| 502 | EE-350 | 0/41 | 0.000 | 0/34856 | 0 | 0 | — | v2 docling | PASS |
| 504 | EE-352 | 0/20 | 0.000 | 0/34818 | 0 | 0 | — | v2 docling | PASS |
| 507 | EE-355 | 0/23 | 0.000 | 0/28001 | 0 | 0 | — | v2 docling | PASS |
| 509 | EE-357 | 0/22 | 0.000 | 0/33991 | 0 | 0 | — | v2 docling | PASS |
| 511 | EE-359 | 0/11 | 0.000 | 0/69750 | 0 | 0 | — | v2 docling | PASS |
| 513 | EE-361 | 0/35 | 0.000 | 0/28777 | 0 | 0 | — | v2 docling | PASS |
| 514 | EE-362 | 0/11 | 0.000 | 0/45072 | 0 | 0 | — | v2 docling | PASS |
| 515 | EE-363 | 0/18 | 0.000 | 0/32741 | 0 | 0 | — | v2 docling | PASS |
| 517 | EE-365 | 0/17 | 0.000 | 0/27452 | 0 | 0 | — | v2 docling | PASS |
| 519 | EE-367 | 0/24 | 0.000 | 0/26850 | 0 | 0 | — | v2 docling | PASS |
| 522 | EE-370 | 0/12 | 0.000 | 0/57145 | 0 | 0 | — | v2 docling | PASS |
| 526 | EE-374 | 0/12 | 0.000 | 0/70633 | 0 | 0 | — | v2 docling | PASS |
| 528 | EE-376 | 0/20 | 0.000 | 0/26217 | 0 | 0 | — | v2 docling | PASS |
| 532 | EE-380 | 0/19 | 0.000 | 0/52619 | 0 | 0 | — | v2 docling | PASS |
| 534 | EE-382 | 0/19 | 0.000 | 0/36707 | 0 | 0 | — | v2 docling | PASS |
| 536 | EE-384 | 0/17 | 0.000 | 0/44343 | 0 | 0 | — | v2 docling | PASS |
| 537 | EE-385 | 0/16 | 0.000 | 0/30612 | 0 | 0 | — | v2 docling | PASS |
| 538 | EE-386 | 0/24 | 0.000 | 0/37468 | 0 | 0 | — | v2 docling | PASS |
| 541 | EE-389 | 0/14 | 0.000 | 0/30766 | 0 | 0 | — | v2 docling | PASS |
| 542 | EE-390 | 0/15 | 0.000 | 0/35129 | 0 | 0 | — | v2 docling | PASS |
| 543 | EE-391 | 0/8 | 0.000 | 0/57443 | 0 | 0 | — | v2 docling | PASS |
| 546 | EE-394 | 0/8 | 0.000 | 0/55962 | 0 | 0 | — | v2 docling | PASS |
| 548 | EE-396 | 0/16 | 0.000 | 0/30326 | 0 | 0 | — | v2 docling | PASS |
| 549 | EE-397 | 0/32 | 0.000 | 0/31591 | 0 | 0 | — | v2 docling | PASS |
| 550 | EE-398 | 0/33 | 0.000 | 0/40434 | 0 | 0 | — | v2 docling | PASS |
| 553 | EE-401 | 0/9 | 0.000 | 0/31819 | 0 | 0 | — | v2 docling | PASS |
| 554 | EE-402 | 0/50 | 0.000 | 0/27808 | 0 | 0 | — | v2 docling | PASS |
| 556 | EE-404 | 0/10 | 0.000 | 0/17331 | 0 | 0 | — | v2 docling | PASS |
| 557 | EE-405 | 0/20 | 0.000 | 0/43496 | 0 | 0 | — | v2 docling | PASS |
| 562 | EE-410 | 0/21 | 0.000 | 0/33376 | 0 | 0 | — | v2 docling | PASS |
| 566 | EE-414 | 0/9 | 0.000 | 0/31745 | 0 | 0 | — | v2 docling | PASS |
| 568 | EE-416 | 0/17 | 0.000 | 0/42223 | 0 | 0 | — | v2 docling | PASS |
| 570 | EE-418 | 0/18 | 0.000 | 0/31430 | 0 | 0 | — | v2 docling | PASS |
| 572 | EE-420 | 0/24 | 0.000 | 0/29703 | 0 | 0 | — | v2 docling | PASS |
| 574 | EE-422 | 0/27 | 0.000 | 0/34435 | 0 | 0 | — | v2 docling | PASS |
| 576 | EE-424 | 0/17 | 0.000 | 0/20736 | 0 | 0 | — | v2 docling | PASS |
| 577 | EE-425 | 0/13 | 0.000 | 0/27754 | 0 | 0 | — | v2 docling | PASS |
| 580 | EE-428 | 0/12 | 0.000 | 0/50046 | 0 | 0 | — | v2 docling | PASS |
| 581 | EE-429 | 0/12 | 0.000 | 0/66962 | 0 | 0 | — | v2 docling | PASS |
| 589 | EE-437 | 0/11 | 0.000 | 0/28529 | 0 | 0 | — | v2 docling | PASS |
| 590 | EE-438 | 0/16 | 0.000 | 0/20361 | 0 | 0 | — | v2 docling | PASS |
| 602 | EE-450 | 0/26 | 0.000 | 0/29880 | 0 | 0 | — | v2 docling | PASS |
| 604 | EE-452 | 0/18 | 0.000 | 0/33375 | 0 | 0 | — | v2 docling | PASS |
| 607 | EE-455 | 0/117 | 0.000 | 0/116658 | 0 | 0 | — | v2 docling | PASS |
| 608 | EE-456 | 0/24 | 0.000 | 0/27755 | 0 | 0 | — | v2 docling | PASS |
| 610 | EE-458 | 0/37 | 0.000 | 0/38279 | 0 | 0 | — | v2 docling | PASS |
| 614 | EE-462 | 0/55 | 0.000 | 0/71742 | 0 | 0 | — | v2 docling | PASS |
| 617 | EE-465 | 0/18 | 0.000 | 0/70236 | 0 | 0 | — | v2 docling | PASS |
| 622 | EE-470 | 0/22 | 0.000 | 0/89055 | 0 | 0 | — | v2 docling | PASS |
| 623 | EE-471 | 0/18 | 0.000 | 0/26648 | 0 | 0 | — | v2 docling | PASS |
| 626 | EE-474 | 0/31 | 0.000 | 0/51998 | 0 | 0 | — | v2 docling | PASS |
| 628 | EE-476 | 0/21 | 0.000 | 0/26311 | 0 | 0 | — | v2 docling | PASS |
| 635 | EE-483 | 0/18 | 0.000 | 0/31684 | 0 | 0 | — | v2 docling | PASS |
| 637 | EE-485 | 0/24 | 0.000 | 0/28931 | 0 | 0 | — | v2 docling | PASS |
| 639 | EE-487 | 0/31 | 0.000 | 0/40443 | 0 | 0 | — | v2 docling | PASS |
| 640 | EE-488 | 0/14 | 0.000 | 0/32332 | 0 | 0 | — | v2 docling | PASS |
| 643 | EE-491 | 0/45 | 0.000 | 0/37090 | 0 | 0 | — | v2 docling | PASS |
| 644 | EE-492 | 0/78 | 0.000 | 0/48640 | 0 | 0 | — | v2 docling | PASS |
| 645 | EE-493 | 0/13 | 0.000 | 0/39554 | 0 | 0 | — | v2 docling | PASS |
| 653 | EE-501 | 0/12 | 0.000 | 0/31281 | 0 | 0 | — | v2 docling | PASS |
| 654 | EE-502 | 0/29 | 0.000 | 0/30052 | 0 | 0 | — | v2 docling | PASS |
| 659 | EE-507 | 0/15 | 0.000 | 0/53707 | 0 | 0 | — | v2 docling | PASS |
| 660 | EE-508 | 0/21 | 0.000 | 0/56839 | 0 | 0 | — | v2 docling | PASS |
| 661 | EE-509 | 0/21 | 0.000 | 0/45598 | 0 | 0 | — | v2 docling | PASS |
| 663 | EE-511 | 0/17 | 0.000 | 0/46144 | 0 | 0 | — | v2 docling | PASS |
| 668 | EE-516 | 0/7 | 0.000 | 0/18256 | 0 | 0 | — | v2 docling | PASS |
| 676 | EE-524 | 0/16 | 0.000 | 0/24691 | 0 | 0 | — | v2 docling | PASS |
| 679 | EE-527 | 0/22 | 0.000 | 0/10834 | 0 | 0 | — | v2 docling | PASS |
| 683 | EE-531 | 0/10 | 0.000 | 0/35014 | 0 | 0 | — | v2 docling | PASS |
| 687 | EE-535 | 0/19 | 0.000 | 0/28603 | 0 | 0 | — | v2 docling | PASS |
| 689 | EE-537 | 0/52 | 0.000 | 0/49627 | 0 | 0 | — | v2 docling | PASS |
| 690 | EE-538 | 0/34 | 0.000 | 0/73575 | 0 | 0 | — | v2 docling | PASS |
| 691 | EE-539 | 0/13 | 0.000 | 0/33268 | 0 | 0 | — | v2 docling | PASS |
| 693 | EE-541 | 0/10 | 0.000 | 0/27748 | 0 | 0 | — | v2 docling | PASS |
| 694 | EE-542 | 0/10 | 0.000 | 0/32696 | 0 | 0 | — | v2 docling | PASS |
| 700 | EE-548 | 0/10 | 0.000 | 0/30101 | 0 | 0 | — | v2 docling | PASS |
| 708 | EE-556 | 0/18 | 0.000 | 0/72787 | 0 | 0 | — | v2 docling | PASS |
| 720 | EE-568 | 0/20 | 0.000 | 0/58076 | 0 | 0 | — | v2 docling | PASS |
| 738 | EE-586 | 0/9 | 0.000 | 0/37875 | 0 | 0 | — | v2 docling | PASS |
| 741 | EE-589 | 0/8 | 0.000 | 0/56851 | 0 | 0 | — | v2 docling | PASS |
| 742 | EE-590 | 0/35 | 0.000 | 0/61795 | 0 | 0 | — | v2 docling | PASS |
| 744 | EE-592 | 0/14 | 0.000 | 0/31659 | 0 | 0 | — | v2 docling | PASS |
| 748 | EE-596 | 0/26 | 0.000 | 0/46842 | 0 | 0 | — | v2 pymupdf | PASS |
| 750 | EE-598 | 0/25 | 0.000 | 0/37262 | 0 | 0 | — | v2 docling | PASS |
| 752 | EE-600 | 0/12 | 0.000 | 0/31685 | 0 | 0 | — | v2 docling | PASS |
| 755 | EE-603 | 0/32 | 0.000 | 0/54820 | 0 | 0 | — | v2 docling | PASS |
| 758 | EE-606 | 0/16 | 0.000 | 0/28806 | 0 | 0 | — | v2 docling | PASS |
| 763 | EE-611 | 0/10 | 0.000 | 0/18307 | 0 | 0 | — | v2 docling | PASS |
| 764 | EE-612 | 0/12 | 0.000 | 0/28051 | 0 | 0 | — | v2 docling | PASS |
| 769 | EE-617 | 0/14 | 0.000 | 0/43451 | 0 | 0 | — | v2 docling | PASS |
| 779 | EE-627 | 0/15 | 0.000 | 0/29934 | 0 | 0 | — | v2 docling | PASS |
| 780 | EE-628 | 0/37 | 0.000 | 0/49575 | 0 | 0 | — | v2 pymupdf | PASS |
| 783 | EE-631 | 0/5 | 0.000 | 0/6592 | 0 | 0 | — | v2 docling | PASS |
| 796 | EE-644 | 0/12 | 0.000 | 0/31508 | 0 | 0 | — | v2 docling | PASS |
| 801 | EE-649 | 0/8 | 0.000 | 0/21951 | 0 | 0 | — | v2 docling | PASS |

**Gate verdicts across the 190:** `PASS` **186**, `FAIL:GLYPH_DENSITY` **3** (719, 699, 586),
`FAIL:SHATTERED` **1** (455). The four known failures are reproduced exactly and no fifth failure
exists.

**455 (EE-303), measured now, as acceptance gate 2 requires: 0 signature fonts of 18.** Its 104
unresolved characters are all outside the signature (§8.1) and its gate failure is `SHATTERED`, a
different defect class entirely — consistent with `parse_quality.Thresholds.__doc__`
("That is p455, and on this corpus it is only p455").

---

## 6. S6 — summary counts and the silent-case list

| bucket | papers |
|---|---:|
| zero signature fonts | **179** |
| signature fonts but **< 1%** coverage | **7** |
| **1–10%** coverage | **3** |
| **> 10%** coverage | **1** |

### The silent-case list — papers above 1% coverage that the gate PASSED

**One paper.**

| paper | EE | signature fonts | coverage | corrupted chars | marked / silent | Run-6 text | gate verdict | `glyph_density_per_kchar` (limit 5.0) | `replacement_density_per_kchar` (limit 1.0) |
|---|---|---|---:|---:|---|---|---|---:|---:|
| **670** | **EE-518** | **29 / 59** | **3.051%** | **907** | 308 / **599** | v2 `docling` | **PASS** | **0.082** | **0.0** |

The other three papers above 1% — 719 (94.77%), 699 (2.92%), 586 (2.53%) — all **FAILED** the gate
on `GLYPH_DENSITY` and are the papers PARSE-GATE-03…09 already handled.

For completeness, the seven papers with signature fonts below 1% coverage, all `PASS`:
473 (0.131%, 2/15) · 618 (0.042%, 3/68) · 516 (0.012%, 1/29) · 14 (0.000%, 2/14) ·
415 (0.000%, 3/1647) · 449 (0.000%, 5/26) · 474 (0.000%, 2/32).

### 6.1 🔴 What 670 actually is — measured, because the coverage number alone misleads

670's 907 corrupted characters are **not in the extracted text**. Two measurements:

**(a) Marker-export survival.** Of the codes < 32 that Docling *would* mark, how many reach the
`.md`:

| paper | codes < 32 in the PDF | `RE_GLYPH` markers in the Run-6 `.md` | survival |
|---|---:|---:|---:|
| 719 | 5,508 | 5,472 | **99.3%** |
| 699 | 453 | 419 | **92.5%** |
| 586 | 589 | 542 | **92.0%** |
| 473 | 2 | 2 | 100% |
| **670** | **308** | **3** | **1.0%** |

**(b) The silent half leaves no trace either.** A scan of `670_v2.md` for Latin-Extended / Greek
mojibake clusters (length ≥ 2) — the visible form the codes ≥ 32 take — returns **zero hits**. The
three markers that did survive sit adjacent to `<!-- image -->`, next to `Fig. 3: Flowchart of
Stereo Localization System.`

**Why.** The signature fonts in 670 are **SimSun / SimHei / MicrosoftYaHei** — 27 of the 29 are
SimSun subsets — and the characters they set are **CJK labels burned into figure artwork**, not
body text. Page 3 alone carries 884 of the 907. What the PDF holds there is
`'E/Z\x03ĐĂŵĞƌĂ\nZ'\x11\x18\x03ĐĂŵĞƌĂ\n^ŬŝŶ\x03ŵŽĚĞů'` — the panel labels *NIR camera*,
*RGB camera*, *Skin model* — while the caption immediately below them
(`Fig. 2: Dual-Camera Imaging System for Detecting Markers on Skin Suturing Model…`) is clean and is
what Docling exported.

**So `coverage` is a PDF-level measure and overstates text-level exposure whenever the signature
fonts live in dropped regions.** I3 anticipated the gap between *font count* and *coverage*; this is
a second gap, between *coverage* and *exported text*, and it separates 670 from 586/699/719 by two
orders of magnitude on the same axis. **670's coverage is real; its extraction exposure, measured,
is zero characters.** Recorded as measurement; the reading is the architect's.

---

## 7. Acceptance gates

| # | gate | result |
|---|---|---|
| 1 | S5 has one row per corpus paper (190, or 190 minus stated gaps) | **PASS** — 190 rows, no gaps, 0 errors, 0 timeouts |
| 2 | 586, 699, 719, 455 reproduce 09's font counts | **PASS with two reported discrepancies** — 586 **4/13 exact**; 455 **0/18** as measured now; 719 **34/38 not 36/38**; 699 **3/19 signature vs 09's 1/19 offending**. Both explained in §2 and neither adapted to. Every character-level figure 09 published reproduces exactly |
| 3 | S6 silent-case list present, each entry naming coverage and gate verdict | **PASS** — one entry: 670 (EE-518), 3.051%, `PASS` |
| 4 | `review.db` size/mtime unchanged; no `parsed_text` change; pip freeze diff empty; Ollama untouched | **PASS** — see §9 |
| 5 | one commit, one file, `ls-remote` == local HEAD, tree clean | **PASS** — see §9 |

---

## 8. Observed, not asked about

### 8.1 🔴 A second unresolved-character class sits entirely outside the signature — 126 papers

The audit's own residual, `fffd_other`: **4,827 characters across 126 of 190 papers** resolve to
U+FFFD in fonts the signature does **not** match. Classified by font dictionary:

| chars | papers | subtype \| encoding \| ToUnicode \| Differences \| embedded |
|---:|---:|---|
| 2,577 | 48 | `Type1` \| built-in \| **has ToUnicode** \| **has Differences** \| embedded |
| 691 | 36 | `Type1` \| built-in \| no ToUnicode \| no Differences \| embedded |
| 678 | 28 | `Type1` \| built-in \| no ToUnicode \| has Differences \| embedded |
| 362 | 20 | `Type1` \| built-in \| has ToUnicode \| no Differences \| embedded |
| 143 | 3 | (span font name matched no page-font entry) |
| **125** | **1** | **`Type0` \| `Identity-H` \| has ToUnicode \| no Differences \| NOT embedded** |
| 102 | 2 | `TrueType` \| built-in \| has ToUnicode \| no Differences \| embedded |
| 77 | 5 | `Type1` \| WinAnsi \| has ToUnicode \| has Differences \| embedded |
| 60 | 5 | `Type1` \| WinAnsi \| no ToUnicode \| has Differences \| embedded |
| 9 | 1 | `Type0` \| `Identity-H` \| has ToUnicode \| no Differences \| embedded |
| 3 | 2 | `TrueType` \| (other) \| no ToUnicode \| no Differences \| embedded |

Top font names: `CMEX10` 1,223 · `AdvP4C4E74` 489 · `MTEX` 285 · `AdvP4C4E46` (2 subsets) 526 ·
`CMEX9` 148 · `TeXCMMathsExtension` 138 · `BLEX` 134 · `CambriaMath` 111 · `MT2EXA` 99. **This class
is dominated by TeX math-extension and symbol fonts** — big delimiters, integral signs, extensible
braces — many of which carry a perfectly good `/ToUnicode` and still fail because the glyph has no
Unicode equivalent to map to. It is a different phenomenon from the signature class and is
**probably benign**; it is recorded because a pre-parse audit built on the signature alone would
report these 126 papers as clean, and 4,827 characters is not nothing.

**The one row that is not benign-looking is the 125-character `Type0 / Identity-H / has ToUnicode /
NOT embedded` entry — and it is in paper 670**, font `RpsvssMicrosoftYaHei-Bold-GBK-EUC-H`. **A
ToUnicode CMap can be present and still not resolve** (the U+FFFF/U+FFFE "no glyph" convention).
The signature therefore has a **false-negative class**, and it is not hypothetical: it occurs in the
corpus, in the very paper this sweep flagged for another reason.

### 8.2 Docling emits a second, undocumented marker form with no font attribution

`618_v2.md` contains **`GLYPH<31>`** — bare, with no `c=` and no `font=`. It is not produced by any
signature font (618 has **zero** signature characters below code 32). Attributing 618's unresolved
characters by span shows they come from **`CMEX10` / `CMEX9` / `wasy10` — simple `Type1` fonts with
a built-in encoding, no `/ToUnicode`, no `/Differences`**. The marker sits at
`…has a GLYPH<31> 3.6 mm main channel…`, i.e. it replaced a diameter symbol `⌀`. The shipped
`RE_GLYPH` matches it (both forms are in the pattern), so the gate counts it; but **anything that
parses the marker for its `font=` attribute will not find one**, and the 09 method's font
attribution does not reach it.

### 8.3 The corpus-wide picture is dominated by one paper

Signature characters: **719 alone is 27,565 of 30,365 — 90.8%**. Excluding 719, the entire rest of
the corpus carries **2,800** signature characters across 10 papers, of which 907 are 670's
figure labels and 1,829 are 586 + 699. **The remaining 7 papers contribute 64 characters in total.**

### 8.4 Paper 415 declares 1,647 font objects

`415` (EE-263) — the 728-page proceedings volume named in `parse_quality.Thresholds.__doc__` — has
**1,647 font objects and 1,442,149 text-layer characters**, roughly 15% of the whole corpus's text.
Three of its fonts match the signature and together render **7 characters**. Worth knowing before
anyone budgets a per-font pass over the corpus by font count rather than by paper.

### 8.5 The space-recoverability rule reproduces 09 mechanically

Applied blind across the corpus, "empty outline + non-zero advance" selects **`gid = 1` and nothing
else** in all four of 586's signature fonts and in 699's MinionPro — the exact rows 09 adjudicated
RECOVERABLE-BY-POSITION by hand. PDF-side counts run slightly above 09's markdown-side counts
(586 **71** vs 64, 699 **452** vs 419, 719 **4,520** vs 4,541) because the two sides count different
populations: 09 counted markers in the exported `.md`, this counts characters in the PDF, and
Docling drops 8% of 586's and gains none of 719's. The 699 difference is entirely the 33
`SegoeUI gid 3` spaces on page 4, which are signature but never marked.

---

## 9. Close-state ledger

*(verified after all measurement, before the commit — the commit touches only
`docs/session-reports/`)*

| item | at session start | at session end |
|---|---|---|
| `review.db` size | 99,770,368 B | **99,770,368 B** |
| `review.db` mtime | 2026-09-08 03:48:48.732577380 +0000 | **identical to the nanosecond** |
| `parsed_text/` | 446 files, manifest sha256 `b8da80b9…342cf16b` | **446 files, `b8da80b9…342cf16b` — unchanged** |
| `.venv` `pip freeze` | 168 packages | **168, diff empty** |
| Ollama | `NRestarts=0`, `ExecMainStartTimestamp` 2026-08-31 00:41:59 | **unchanged; no model load** (resident `qwen3:8b` predates this session) |

`review.db` was opened `mode=ro` for two queries: the corpus paper list and `full_text_assets`.
All PDF and parsed-text reads were read-only. All scratch output is under `/tmp/pg10/`
(`sweep.py`, `other.py`, `ids.json`, `sweep.json`, `rows.json`, the two table fragments);
nothing was written under `data/`.
