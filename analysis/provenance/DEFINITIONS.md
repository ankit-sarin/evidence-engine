# Evidence-Provenance Taxonomy — Pinned Definitions

**Status:** pre-registration artifact for Paper 1/1b. Wording is manuscript-grade.
**Version:** `prov-def-1` · **Normalization version:** `prov-norm-1`
**Implementation:** `analysis/provenance/` (`normalize.py`, `segment.py`, `classifier.py`)

This document is the authority. The code implements it; where they disagree, the
code is wrong. Every constant used anywhere in the classification path appears in
§5 with a one-line rationale — there are no unstated constants.

---

## 1. Scope and object of classification

The unit of classification is one **evidence span**: the triple
(arm, paper, field) together with the `value` the arm reported and the
`source_snippet` it offered as evidence. Spans are read from
`evidence_spans` (local arm) and `cloud_evidence_spans` (both cloud arms).

The classification asks one question and only one: **can the snippet be located
in the parsed text of the paper the arm was given?** It does not ask whether the
value is correct, whether the snippet supports the value, or whether the arm
reasoned well. Those are separate instruments (concordance, LLM-as-judge, PI
audit) and must not be conflated with this one.

Reference text is the parsed markdown the extractors actually consumed:
`data/<review>/parsed_text/{paper_id}_v{N}.md` at the highest `N`. All three
arms consumed the identical file per paper (verified in DIAG-VISION-01), so the
reference text is a property of the paper, not of the arm.

---

## 2. Normalization (T1–T9)

Every string — paper text and snippet alike — passes through exactly these nine
transforms, in this order, before any comparison. Implemented in
`analysis/provenance/normalize.py`.

| # | Transform | Rationale |
|---|---|---|
| T1 | `html.unescape()` | Docling emits HTML entities (`&lt;` for `<`). A snippet quoting "(<1mm)" must match paper text rendered as "(&lt;1mm)". |
| T2 | Unicode NFKC | Folds PDF ligatures (`ﬁ`→`fi`, `ﬂ`→`fl`) and full-width forms. PyMuPDF preserves ligatures, Docling does not; without T2 the same sentence fails to match itself across parser tiers. |
| T3 | Delete U+00AD, U+200B, U+200C, U+200D, U+FEFF | Soft hyphens and zero-width characters are invisible layout artifacts with no textual content. |
| T4 | Quote folding (`‘ ’ ‚ ‛ « » ‹ › \` ´` → `'`; `“ ” „ ‟` → `"`) | Typographic vs. straight quotes is a rendering choice made by the parser, not a difference in what the paper says. |
| T5 | Dash folding (`‐ ‑ ‒ – — ― −` → `-`) | Same rationale as T4; en/em/minus dashes are interchangeable across PDF encodings. |
| T6 | Ellipsis folding (`…` → `...`) | Normalizes the two spellings of one marker. Deliberately **not** deleted — see §4.1. |
| T7 | De-hyphenation across line breaks: `([A-Za-z])-\s*\n\s*([A-Za-z])` → `\1\2` | PDF line-break hyphenation splits words ("ori-\nfice"). Restricted to letter–letter so numeric ranges ("1990-\n1995") and compound identifiers are never merged. Paper 455 carries 106 such breaks. |
| T8 | Lowercase | Parsers differ in heading case (`## INTRODUCTION` vs `Introduction`); case is not evidence. |
| T9 | Whitespace collapse (`\s+` → single space), then strip | Line breaks are a layout artifact of PDF column width. **T7 must precede T9**, because T7 needs the newlines T9 destroys. |

**Not applied, deliberately:** markdown structure is *not* stripped. Docling's
`## heading` markers, pipe tables and `<!-- image -->` placeholders stay in the
reference text. Removing them would splice together passages that are not
adjacent in the paper and manufacture false ANCHORED verdicts. The cost is that
a snippet quoting across an interpolated figure caption reads as STITCHED rather
than ANCHORED; that is the conservative direction and it is a true statement
about the source text as parsed.

---

## 3. Sentence segmentation

**Tokenizer: `pysbd` (Pragmatic Sentence Boundary Disambiguation) 0.3.4,
language `en`, `clean=False`.** Rule-based, deterministic, no model files, no
training data, no randomness — identical output on every machine and every run.
Pinned in `requirements.txt` as `pysbd==0.3.4`.

Chosen over regex splitting on `(?<=[.!?])\s+` because scientific prose is dense
with non-terminal periods — `Fig. 3`, `et al.`, `e.g.`, `1.04 mm`, `vs.`, `[82].`
— that a regex shreds into fragments. The shredding is not cosmetic: a fragment
like `"3 for safety consideration."` matches almost any paper by chance and
inflates the traceable-sentence count while its parent sentence is lost. pysbd
handles all five of those constructions correctly on the Run 6 corpus.

### 3.1 T-seg-0 — ellipsis pre-split

Before pysbd runs, the snippet is split at ellipsis markers (`\.{3,}` or `…`)
and each side is segmented independently. The marker itself is discarded.

Rationale, and it is load-bearing: an ellipsis is the writer's **explicit
statement that text was omitted between the two sides**. Treating "A ... B" as
one unit guarantees it can never match, so a snippet whose halves are both
verbatim paper text would land in `UNTRACEABLE_NO_BASIS` — the exact opposite of
the truth. 81% of one arm's non-anchored snippets carry an ellipsis, so this
single decision moves more spans than any other in the taxonomy. A secondary
reason: pysbd rewrites a trailing `...` to `.` even with `clean=False`, mutating
the text; pre-splitting means pysbd never sees an ellipsis.

### 3.2 Minimum sentence length

`MIN_SENTENCE_TOKENS = 3`. Sentences with fewer than three whitespace-delimited
tokens are excluded from the traceability test. A one- or two-token fragment
(`"2 (c)."`, `"H."`) carries no discriminative content and would match nearly any
paper by chance, inflating every traceable class. Excluded fragments are
**counted and reported** (`n_sentences` vs `n_evaluated`), never silently
dropped. A span in which *every* sentence is excluded is classed
`UNCLASSIFIABLE_SHORT` rather than being forced into the taxonomy.

---

## 4. The taxonomy

Decision order is strict; the first matching rule wins.

| Order | Class | Definition |
|---|---|---|
| 0a | `ABSENCE_DECLARED` | Snippet is empty **and** the value is a codebook absence sentinel. The arm claimed the field is not reported and correctly offered no evidence. |
| 0b | `MISSING_SNIPPET` | Snippet is empty **and** the value asserts something. An evidence obligation was not met. |
| 0c | `UNCLASSIFIABLE_SHORT` | Every segmented sentence falls below `MIN_SENTENCE_TOKENS`. |
| 1 | **`ANCHORED`** | The normalized snippet occurs as a **contiguous substring** of the normalized paper text. One passage, verbatim, in order. |
| 2 | **`STITCHED`** | Not anchored, but **every** evaluated sentence occurs as a contiguous substring of the normalized paper text. The text is real; the arm joined passages that are not adjacent (or reordered them, or bridged them with an ellipsis). |
| 3 | **`DRIFTED`** | Not anchored or stitched, but **every** evaluated sentence reaches similarity ≥ `THRESHOLD_PRIMARY` against its best-matching window. Near-verbatim with minor edits: punctuation, a dropped clause, parser noise. |
| 4 | **`UNTRACEABLE_PARTIAL`** | At least one, but not all, evaluated sentences are traceable (exact or ≥ threshold). Sub-class of untraceable: *near-verbatim drift failure* — the snippet has real textual basis, but at least one sentence cannot be located. |
| 5 | **`UNTRACEABLE_NO_BASIS`** | **No** evaluated sentence is traceable. Sub-class of untraceable: *no textual basis* — nothing in the snippet corresponds to anything in the paper. |

`UNTRACEABLE_PARTIAL` and `UNTRACEABLE_NO_BASIS` together constitute
**untraceable**. Report them separately: they are different defects. A partial is
usually a quotation that drifted too far; a no-basis is usually prose the arm
wrote itself.

### 4.1 What ANCHORED deliberately excludes

A snippet containing an ellipsis cannot be ANCHORED, because T6 preserves the
`...` and paper text effectively never contains it. This is intended: an
ellipsis-bridged quote is not one contiguous passage, and the taxonomy's job is
to say so. It will normally classify as STITCHED, which is the accurate
description — real text, non-contiguous.

### 4.2 Similarity function

`difflib.SequenceMatcher(None, a, b, autojunk=False).ratio()` over normalized
character strings, from the Python 3.12.3 standard library.

**`autojunk=False` is mandatory and is a substantive choice.** The default
heuristic treats any character appearing in more than 1% of a sequence of length
≥ 200 as "junk" and excludes it from matching. Snippets in this corpus routinely
exceed 200 characters, so the default silently deflates ratios for exactly the
long quotations the taxonomy most needs to score. The legacy `grep_verify`
(`engine/agents/auditor.py:65`) ran with autojunk on; this is one of the two
reasons the two censuses do not agree exactly (§6).

### 4.3 Window search

For a sentence of *n* words, every *n*-word window of the paper is scored and the
best ratio is kept. `real_quick_ratio()`/`quick_ratio()` are exact upper bounds on
`ratio()`, so skipping windows that cannot beat the incumbent is an exact
optimization, not an approximation. The scan stops early once a window reaches
`RATIO_CEILING`; see §5.

---

## 5. Constants

| Constant | Value | Where | Rationale |
|---|---|---|---|
| `THRESHOLD_PRIMARY` | **0.90** | `classifier.py` | The primary drift threshold. Set stricter than the legacy 0.85 because DRIFTED is meant to mean *near-verbatim* — a minor edit, not a paraphrase; 0.85 admits sentence pairs that share topic and phrasing but differ in content words. |
| `THRESHOLD_BAND` | **(0.85, 0.90, 0.95)** | `classifier.py` | Sensitivity analysis at ±0.05 around the primary, reported in the supplement. **0.85 is the legacy `grep_verify` value, so the legacy family sits inside the declared band by construction.** |
| `RATIO_CEILING` | **0.95** | `classifier.py` | Window scan stops here. Every threshold in the band is ≤ 0.95, so a window at or above the ceiling classifies identically at all three thresholds and further search cannot change any verdict. Recorded ratios are exact below 0.95 and right-censored at ≥ 0.95. |
| `MIN_SENTENCE_TOKENS` | **3** | `segment.py` | See §3.2 — below three tokens a fragment matches by chance. |
| `ABSENCE_SENTINELS` | `"" , nr, n/a, na, not_found, not found, not reported, none` | `classifier.py` | Mirrors `extraction_codebook.yaml: absence_sentinels` plus the empty string and `none`. Held as a literal so the classifier is importable and testable without a review directory; the census asserts the two agree. |
| `NORMALIZATION_VERSION` | `prov-norm-1` | `normalize.py` | Bumped if any transform is added, removed or reordered. Census rows carry it. |
| Tokenizer | `pysbd==0.3.4`, `en`, `clean=False` | `segment.py` | See §3. |

---

## 6. Relationship to the legacy "unanchored" figure

The legacy figure (DIAG-UNANCHOR-01, 22.1% of snippet-bearing spans) came from
`grep_verify()` in `engine/agents/auditor.py`: normalized exact substring, else a
sliding word-window `SequenceMatcher(...).ratio() > 0.85` **on the whole snippet
at once**. Under this taxonomy that maps to *not* ANCHORED and *not* DRIFTED-as-a-
single-unit — it has no concept of a per-sentence verdict, so it cannot separate
STITCHED from UNTRACEABLE, which is the distinction the taxonomy exists to make.

Three known sources of disagreement, all expected and all directional:

1. **Per-sentence vs. whole-snippet matching.** A stitched snippet fails as a
   whole and passes sentence-by-sentence. This moves spans *out* of untraceable.
2. **`autojunk`.** Legacy ran with the default heuristic on; this taxonomy turns
   it off (§4.2). Affects snippets > 200 characters.
3. **Normalization.** Legacy applied NFKC, smart-quote folding, glued-punctuation
   splitting and whitespace collapse. It did **not** unescape HTML entities and
   did **not** de-hyphenate line breaks (T1, T7). Both move spans *out* of
   untraceable.

The legacy figure is restated in the census output under these definitions for
the manuscript's revision-history disclosure. It is not silently replaced.

---

## 7. What this taxonomy does not license

- It does not measure fabrication. A `UNTRACEABLE_NO_BASIS` snippet may state
  something entirely true about the paper — the defect is that it is not a
  quotation, not that it is false.
- It does not compare arms to humans. The human workbooks require a snippet on
  2 of 20 fields and explicitly permit non-contiguous "passage(s)"
  (DIAG-UNANCHOR-01 §2), so STITCHED is compliant behaviour for a human and
  non-compliant for a model. Any cross-population use of these classes must
  state that asymmetry.
- It does not adjudicate the extractive/interpretive field split, which is
  **PROPOSED** in `field_class.py` and requires architect ratification before any
  downstream use.
