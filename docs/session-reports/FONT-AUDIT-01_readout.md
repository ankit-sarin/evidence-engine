# FONT-AUDIT-01 — text-level exposure audit for the Identity-H / no-ToUnicode signature

**Date:** 2026-09-09 · **Machine:** DGX Spark · **Type:** feature (module + tests) plus a read-out
**HEAD at start:** `581568c`, tree clean.

PARSE-GATE-10 measured the signature's coverage **in the PDF** across 190 papers and showed that
the number overstates what reaches the exported text — paper 670 renders 3.05% of its text layer in
signature fonts and puts three characters into `670_v2.md`. This task builds the text-level
measurement as an engine module with tests and runs it on the eleven signature papers plus 455 as a
control. **No gate wiring, no ledger change, no re-parse, and no ruling on 586 or 699.**

---

## 1. Phase 1 outcomes (R1–R4), as ruled

### R1 — primitives and authorities

| item | resolution |
|---|---|
| **I1 — `parse_quality` primitives reusable** | **TRUE for the regex; the tokenizer is not needed.** `RE_GLYPH` matched both marker forms already. ⚠️ Importing it from `parse_quality` would have dragged `analysis.provenance.segment` into a new engine module, so **ruling 3** moved the pattern to `engine/parsers/markers.py`, which imports nothing. `parse_quality` re-exports it **by identity** — `parse_quality.RE_GLYPH is markers.RE_GLYPH` — so every existing import site is unchanged and the pattern still has one definition. |
| **I4 — a parsed-text resolution authority exists** | **HALF FALSE, and the half that is false is now queued as PARSED-PATH-01.** `engine.core.corpus.corpus_status_sql` is a genuine single authority. A parsed-text resolver is not: **seven** implementations exist (`engine/agents/auditor.py:413`, `engine/review/human_review.py:90` and `:231`, `engine/agents/ft_screener.py:302`, `engine/adjudication/ft_screening_adjudicator.py:67`, `analysis/provenance/census.py:104`, `analysis/paper1/judge_loader.py:169`). Only `census.py` sorts by integer version; the DB-driven one returns NULL for the 16 v1-only papers. Per **ruling 5** the E3 runner uses `census.parsed_text_path` and **no new resolver was written anywhere**. |

### R2 — I2 is TRUE: the silent character is exactly `chr(gid)`

Reconstructing each unresolved character as `chr(gid)` reproduces `586_v2.md` at every passage
PARSE-GATE-09 §7 named — `commonly only ████66`, `in the order of ████6$)`,
`lies between █T██C6,`, `approximately ███C6 and the resolution ███C`. Five codes matched to their
visible wrong characters, all `CambriaMath`: **54→`6`** (true `m`), **36→`$`** (`N`),
**84→`T`** (`-`), **67→`C`** (`µ`), **43→`+`**. The only systematic difference is whitespace:
Docling inserts spaces around dropped and marked runs, which is why both sides are
whitespace-normalised before comparison.

### R3 — I3 is FALSE, and estimator (b) shipped

The n-gram matcher was built and measured before being rejected (ruling 1). On 586, against a
ground truth of ±25-character context agreement:

| window *w* | matched | verified | **precision** | recall |
|---:|---:|---:|---:|---:|
| **1 (as specified)** | 118 | 64 | **54.2%** | 13.1% |
| 2 | 57 | 45 | 78.9% | 9.2% |
| 3 | 38 | 31 | 81.6% | 6.3% |
| 4 | 30 | 26 | 86.7% | 5.3% |
| 6 | 24 | 23 | 95.8% | 4.7% |

Twenty hand-checked cases at w=1: **12 OK, 8 BAD**. Failure modes, worst first: common keys collide
(`' A '` → 18 sites, matching *"…highest quality. A drop in image sharpness…"*); **corrupted text
collides with itself** — a key lifted from `WCGNTCGLX` landed confidently on an earlier `CGNX`;
HTML entities displace neighbours (`&amp;` for `&`), worth 8 percentage points; Docling's inserted
whitespace; and equation regions reflowed wholesale so position carries no information.

**No n-gram matcher ships.** `alignment_v1` is what is in the module.

### R4 — the interface as built

`engine/parsers/font_audit.py`, pure, no DB, no writes:

```python
SIGNATURE   = "type0_identity_no_tounicode"
UNRESOLVING = "type0_identity_with_tounicode"
ESTIMATOR   = "alignment_v1"
ALIGN_BLOCK_MIN    = 12
MUPDF_BASEFONT_MAX = 31

def audit(pdf_path, exported_text, *, block_min=ALIGN_BLOCK_MIN) -> PaperAudit
def normalise_exported_text(text) -> str
```

`PaperAudit` carries exactly the fields ruling 2 named — `signature_fonts`, `unresolving_fonts`,
`total_fonts`, `sig_chars_pdf`, `marked_in_text`, `marked_unattributed`, `silent_in_text_lo`,
`silent_in_text_hi`, `silent_on_dropped_pages`, `space_recoverable`, `exported_chars`,
`exposure_lo`, `exposure_hi`, `estimator`, `fonts` — plus a `FontRow` per font object.

---

## 2. R1 (ruling) — alignment block size

Measured on 586 v2, 492 silent signature characters, everything else held fixed:

| `block_min` | `silent_in_text_lo` | `silent_in_text_hi` | dropped | lo / silent | `exposure_lo` |
|---:|---:|---:|---:|---:|---:|
| 8 | **163** | 492 | 0 | 33.1% | 1.019% |
| **12** | **135** | 492 | 0 | 27.4% | **0.978%** |
| 16 | **132** | 492 | 0 | 26.8% | 0.974% |

**12 is kept.** Two reasons, and the ruling's own test is the first. (a) *Does 8 change the answer
materially?* **No.** It moves `exposure_lo` by 0.041 percentage points and moves no paper across
any band a reader would act on — 586 is above 0.5% either way, and no other paper's `lo` comes near
the boundary. (b) The curve is **flat above 12** (135 → 132, a 3-character difference for a 33%
wider block) and **steep below it** (135 → 163, +21%). A parameter sitting on the flat part of its
own curve is one whose exact value the answer does not depend on; a parameter on the steep part is
one where the next reader's choice changes the number. The 28 extra placements at block 8 are by
construction the shortest runs, which is where a coincidental agreement of whitespace and
punctuation is most likely, and this task did not adjudicate them individually.

---

## 3. E3 — the eleven signature papers, plus 455 as control

`‡` marks the extra **v2** row for the four papers PARSE-GATE-07 re-parsed on 2026-09-08. The
unmarked row is what `census.parsed_text_path` returns — the highest version, i.e. the text Run 7
would read today. Both are reported rather than one substituted for the other: PARSE-GATE-10's
counts and acceptance gate 4 are stated on v2, the text Run 6 read.

| paper | EE | text | sig/unres/total fonts | sig_chars_pdf | marked (unattr) | silent lo | silent hi | dropped | space_rec | exported | exposure_lo % | exposure_hi % |
|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 719 | EE-567 | v3 | 34/0/38 | 27565 | 0 (0) | 0 | 22057 | 0 | 4520 | 28836 | 0.0 | 76.4912 |
| 719 ‡ | EE-567 | v2 | 34/0/38 | 27565 | 5472 (0) | 4944 | 22057 | 0 | 4520 | 279389 | 3.7281 | 9.8533 |
| 670 | EE-518 | v2 | 29/0/59 | 1032 | 3 (0) | 0 | 716 | 0 | 28 | 36728 | 0.0082 | 1.9576 |
| 699 | EE-547 | v3 | 3/0/19 | 748 | 0 (0) | 0 | 295 | 0 | 452 | 27045 | 0.0 | 1.0908 |
| 699 ‡ | EE-547 | v2 | 3/0/19 | 748 | 419 (0) | 0 | 295 | 0 | 452 | 48546 | 0.8631 | 1.4708 |
| 586 | EE-434 | v3 | 4/0/13 | 1081 | 0 (0) | 0 | 492 | 0 | 71 | 40486 | 0.0 | 1.2152 |
| 586 ‡ | EE-434 | v2 | 4/0/13 | 1081 | 542 (0) | 135 | 492 | 0 | 71 | 69214 | 0.9781 | 1.4939 |
| 473 | EE-321 | v2 | 2/0/15 | 31 | 2 (0) | 6 | 29 | 0 | 0 | 25025 | 0.032 | 0.1239 |
| 618 | EE-466 | v2 | 3/0/68 | 22 | 2 (2) | 0 | 22 | 0 | 0 | 58878 | 0.0034 | 0.0408 |
| 516 | EE-364 | v2 | 1/0/29 | 4 | 0 (0) | 0 | 4 | 0 | 0 | 40806 | 0.0 | 0.0098 |
| 415 | EE-263 | v2 | 3/0/1647 | 7 | 0 (0) | 0 | 7 | 0 | 0 | 1771635 | 0.0 | 0.0004 |
| 14 | EE-007 | v1 | 2/0/14 | 0 | 0 (0) | 0 | 0 | 0 | 0 | 24640 | 0.0 | 0.0 |
| 449 | EE-297 | v2 | 5/0/26 | 0 | 0 (0) | 0 | 0 | 0 | 0 | 34161 | 0.0 | 0.0 |
| 474 | EE-322 | v2 | 2/0/32 | 0 | 0 (0) | 0 | 0 | 0 | 0 | 46069 | 0.0 | 0.0 |
| 455 | EE-303 | v3 | 0/0/18 | 0 | 0 (0) | 0 | 0 | 0 | 0 | 52403 | 0.0 | 0.0 |
| 455 ‡ | EE-303 | v2 | 0/0/18 | 0 | 0 (0) | 0 | 0 | 0 | 0 | 59964 | 0.0 | 0.0 |

**Papers whose exposure reaches 0.5% of exported characters** (E4's call-out threshold; with an
interval the two bounds must be read separately):

| paper | `exposure_lo` ≥ 0.5% | `exposure_hi` ≥ 0.5% |
|---|---|---|
| **719** (v2) | **yes — 3.728%** | yes — 9.853% |
| **586** (v2) | **yes — 0.978%** | yes — 1.494% |
| **699** (v2) | **yes — 0.863%** | yes — 1.471% |
| **670** (v2) | no — 0.008% | **yes — 1.958%** |
| 719 / 586 / 699 (v3, the re-parses) | no — 0.000% | yes — 76.491% / 1.215% / 1.091% |
| every other paper | no | no |

The v3 rows are the shape the interval is meant to show and not a defect in it: the OCR re-parse
shares almost no character sequence with the broken text layer, so **nothing aligns** (`lo = 0`)
while the pages still align *somewhere* (`hi` = the whole silent population). 719 v3's
`exposure_hi` of 76% is `hi` charged against a document one tenth the length — the honest reading is
"the upper bound carries no information here", which is what an upper bound built to be generous
looks like when the two texts are unrelated. **No ruling on which version any paper should read is
offered.**

**Cost.** 16 audits over 12 papers, **270.4 s** total — inside the ~10 minute budget. **Paper 415
alone is 212.1 s of it (78%)**: 728 pages and a 1.77 MB markdown, so each page carrying a silent
character is aligned against 1.77 M characters by `difflib`. Every other audit is between 0.0 s and
13.9 s. Reported as the cost, not optimised: the module already restricts alignment to pages that
carry at least one silent signature character, which is lossless by definition, and 415's seven
characters are what the remaining time buys.

### Gate 5 — 699, explicitly

699 carries **295 silent characters** (codes ≥ 32 in signature fonts). Of those:

| | count |
|---|---:|
| `silent_in_text_lo` — placed inside an aligned block | **0** |
| `silent_in_text_hi` — on a page that aligned somewhere | **295** |
| `silent_on_dropped_pages` | **0** |

**Not one of the 295 was placed in the exported text**, and none is on a page the exporter skipped
entirely, so the interval is the widest it can be: 0 to 295. Their fonts and pages:

| font | page | chars | codes < 32 | codes ≥ 32 | space-recoverable | program | cmap | post |
|---|---:|---:|---:|---:|---:|---|:--:|:--:|
| `AOIIJB+MinionPro-Regular` | **1** | 440 | 419 | **21** | 419 | bare CFF | no | no |
| `AOINCN+SegoeUI` | **4** | 300 | 34 | **266** | 33 | sfnt | no | no |
| `AOINEN+SimHei-GBK-EUC-H` | **4** | 8 | 0 | **8** | 0 | sfnt | no | no |

So **274 of the 295 are SegoeUI and SimHei on page 4** — the figure region PARSE-GATE-09 §2
identified and whose characters Docling marks not at all — and the remaining **21 are MinionPro on
page 1**, alongside the 419 marked spaces that are the whole of 699's gate failure. The paper's
`marked_in_text` is 419 on v2, every one of them the same space glyph.

---

## 4. Acceptance gates

| # | gate | result |
|---|---|---|
| 1 | Phase 1 reported and acknowledged before any edit | **PASS** — Phase 1 reported with no edits; the tree was clean at the ruling |
| 2 | standard gate passes; count = 1,853 + tests added, no other delta | **PASS** — **1,869 passed, 17 deselected**; 1,853 + **16** new tests, no other change |
| 3 | module imports nothing from `engine.core.database`, nothing from `analysis/`, and no file writes | **PASS** — asserted in a subprocess by `test_font_audit_does_not_import_the_analysis_lane`, which checks `sys.modules` after importing the module in a fresh interpreter; `markers.py` is checked the same way |
| 4 | E3 reproduces 10's marked counts for 586/699/719 under the stated definition | **PASS with the difference stated** — see below |
| 5 | 699's row states whether any of its 295 silent characters are in the exported text | **PASS** — §3, "Gate 5 explicitly": **0** placed, 295 upper bound, 0 dropped, fonts and pages named |
| 6 | `review.db`, `parsed_text/` unchanged; pip freeze diff empty; Ollama untouched | **PASS** — §6 |
| 7 | commits scoped as ruled; `ls-remote` == local HEAD; tree clean | **PASS** — §6 |

### Gate 4 — the counting definition, and the difference

Under ruling 6, `marked_in_text` counts `RE_GLYPH` matches **in the exported text**, both forms.
Measured on v2: **586 = 542, 699 = 419, 719 = 5,472** — reproducing PARSE-GATE-10 §6.1 exactly.

PARSE-GATE-10's *table* column headed "codes <32 (marked)" reads **589 / 453 / 5,508**, and those
are a different quantity: **PDF-side** counts of characters whose CID is below 32, i.e. what Docling
*would* mark. The gap — 47, 34 and 36 characters — is PARSE-GATE-10 §6.1's export-survival
measurement, the markers Docling produced and then dropped from the markdown. Both numbers are in
this module: `sum(row.codes_lt32)` is the PDF-side one and `marked_in_text` the text-side one, and
they are deliberately separate fields.

---

## 5. 🔴 A finding that contradicts the assumption ledger

**MuPDF clips `/BaseFont` to 31 characters before `get_texttrace` reports it, and PARSE-GATE-10's
join did not account for that.** Consequences, measured:

1. **Paper 670 renders 1,032 signature characters, not 907.** The missing 125 are
   `CFDIMI+RpsvssMicrosoftYaHei-Bold-GBK-EUC-H` (42 characters), reported by the renderer as
   `RpsvssMicrosoftYaHei-Bol` (24, because the seven-character subset tag is inside the budget).
   An exact-name join finds no match and drops the font's characters **silently** — the inventory
   still lists the font and reports none of its damage. 670's PDF-level coverage is therefore
   **3.47%**, not the 3.051% PARSE-GATE-10 §6 published.
2. **The ledger's UNRESOLVING instance is not one.** The ledger records "false-negative class
   exists: has-ToUnicode but unresolving (U+FFFF-style), 125 chars, paper 670", from PARSE-GATE-10
   §8.1's `Type0 | Identity-H | has ToUnicode | not embedded` row. Measured directly, that font's
   dictionary reads **`/ToUnicode` absent** and `/FontFile2` **present** — it is a **SIGNATURE**
   font, and the 125 characters were classified as they were because the join failed, not because
   the font has a ToUnicode. **`unresolving_fonts` is 0 for every one of the twelve papers audited
   here.** The class remains named and flagged in the module, as E1 requires; what changes is that
   **it has no known instance in this corpus.**
3. The rule is `strip_tag(basefont[:31])`, established by measurement: on 670 it reproduces
   **59 of 59** reported names, where 30 reproduces 53 and 32 reproduces 56.

**Reported, not adjudicated.** PARSE-GATE-10 is not amended here, its §8.1 residual is not
re-derived, and nothing downstream was changed on the strength of this. The module carries the fix
because a module that silently loses a font's characters is not measuring what it claims to.

`tests/test_font_audit.py::test_a_basefont_past_mupdfs_buffer_still_joins` reproduces the clipping
end to end — it asserts the renderer really did shorten the name **before** asserting the join
survived, and reverting the join to exact equality turns it red.

---

## 6. Close-state ledger

| item | at start | at end |
|---|---|---|
| `review.db` size / mtime | 99,770,368 B / 2026-09-08 03:48:48.732577380 +0000 | **identical to the nanosecond** |
| `parsed_text/` | 446 files, manifest sha256 `b8da80b9…342cf16b` | **446, `b8da80b9…342cf16b` — unchanged** |
| `.venv` `pip freeze` | 168 packages | **168, diff empty** |
| Ollama | `NRestarts=0`, start 2026-08-31 00:41:59 | **unchanged; no model load** |

Files changed, and nothing else:

| commit | files |
|---|---|
| `refactor(parse): move RE_GLYPH to markers.py` | `engine/parsers/markers.py` (new), `engine/parsers/parse_quality.py` |
| `feat(parse): font-signature exposure audit + tests` | `engine/parsers/font_audit.py` (new), `tests/test_font_audit.py` (new) |
| `docs(session-reports): FONT-AUDIT-01 read-out` | this file |

The E3 runner is scratch and uncommitted (`/tmp/fa01/e3_run.py`), per ruling 5. It resolves parsed
text through `analysis.provenance.census.parsed_text_path` and defines no resolver of its own.

---

## 7. Observed, not asked about

1. **The bare `GLYPH<31>` form is confirmed end to end on real data.** Paper 618 reports
   `marked_in_text = 2, marked_unattributed = 2` — both of its markers carry no `font=` and cannot
   be charged to any font. Its three signature fonts contribute **zero** characters below code 32,
   so the two markers do not come from the signature at all; PARSE-GATE-10 §8.2 attributed them to
   simple Type1 `CMEX`/`wasy` fonts. **A marker count and a signature count measure different
   populations, and 618 is the paper where they visibly disagree.**
2. **`space_recoverable` needs both halves of its test, and one paper proves it.** The first
   implementation checked only "empty outline" and reported 22 space-recoverable characters for
   670; adding the non-zero-advance half — which PARSE-GATE-09 §5 always had — brings it to 20 on
   the same input. An empty outline also describes `.notdef` and a zero-width combining mark. The
   synthetic fixture carries a deliberate `zerowidth` glyph (blank, advance 0) alongside `space`
   (blank, advance 500) so the test fails if either half is dropped.
3. **Four of the eleven signature papers render zero signature characters** — 14, 449, 474, and
   415 renders seven. Paper 449 declares **five** signature fonts and renders nothing in any of
   them. This is I3 from PARSE-GATE-10 restated at the text level: a font inventory is not an
   exposure measurement, and for 4 of 11 papers the two answers differ by everything.
4. **670's damage is spread over 29 fonts and concentrated on one page.** 577 of its 1,032
   characters are a single SimSun-Bold subset on page 3; 25 fonts of the 29 are bare CFF programs.
   Its `marked_in_text` is 3.
5. **`difflib` is the whole cost.** 415 is 78% of the sweep's runtime for 7 characters of signal.
   If this ever runs corpus-wide rather than on eleven papers, that is the number to design
   against; nothing was optimised here because nothing was asked to be.
