# TAXONOMY-CENSUS-01 — Evidence-provenance taxonomy pinned; full Run 6 census

Project: evidence-engine · Machine: DGX (spark-59e4) · Date: 2026-07-27
Commit: `b158a25` — analysis code + DEFINITIONS.md only. Census outputs live in
`data/surgical_autonomy/analysis/provenance/` (gitignored, per repo convention).

**Arm P rerun state:** RUNNING throughout (PID 3291661, `probes.cache_arms --arm P`,
45:36 elapsed at task start). **Ollama call count: 0** — the classifier is pure-Python
string matching; no inference, no `/api/*`, no CLI. No Arm P results were read.

**Census run id:** `provcensus_surgical_autonomy_20260727T183832Z`
`definitions_sha256=93f6466d…` · tokenizer `pysbd 0.3.4` · τ=0.90 · **11,017 / 11,017 spans
classified (100%)**.

---

## 0. What was pinned

`analysis/provenance/DEFINITIONS.md` (`prov-def-1`) is the pre-registration artifact.
Headlines:

- **Tokenizer: pysbd 0.3.4**, language `en`, `clean=False` — rule-based, deterministic,
  no model files. Chosen because regex splitting on `(?<=[.!?])\s+` shreds `Fig. 3`,
  `et al.`, `1.04 mm`, `[82].` into fragments that then match by chance.
- **Normalization T1–T9**, enumerated with rationale: HTML unescape → NFKC → zero-width
  removal → quote fold → dash fold → ellipsis fold → line-break de-hyphenation →
  lowercase → whitespace collapse. Order is load-bearing (T7 before T9). Markdown
  structure is deliberately *not* stripped — removing `<!-- image -->` or `##` would
  splice non-adjacent passages and manufacture false ANCHORED verdicts.
- **T-seg-0 ellipsis pre-split** (§3.1). Snippets are split at `...`/`…` before
  segmentation. This is the single highest-impact decision in the taxonomy; see §6.1.
- **Similarity:** `difflib.SequenceMatcher(autojunk=False).ratio()`. autojunk off is
  mandatory — the default treats characters appearing in >1% of a ≥200-element sequence
  as junk, silently deflating ratios for exactly the long snippets at issue.
- **τ = 0.90** primary, band **(0.85, 0.90, 0.95)**. The legacy 0.85 therefore sits inside
  the declared band by construction rather than being displaced by it.
- **MIN_SENTENCE_TOKENS = 3**; shorter fragments are excluded from the traceability test
  but counted (`n_sentences` vs `n_evaluated`), never silently dropped.

Persistence: migration 010 → `provenance_census_runs` + `provenance_classifications`.
Per-sentence ratios are stored, so the sensitivity analysis is re-derivable without
re-running the matcher. No existing audit table was read-modified or written.

Tests: 28 new (one fixture per class, incl. ellipsis-stitched and rationale/no-basis,
plus normalization/segmentation/threshold/legacy-equivalence). **Full offline suite:
1318 passed, 15 deselected.**

---

## 1. Taxonomy distribution per arm

Denominator = **all** spans (the census covers 100%, not just snippet-bearing spans).

| arm | n | ANCHORED | STITCHED | DRIFTED | UNTR_PARTIAL | UNTR_NO_BASIS | ABSENCE_DECL | MISSING_SNIP | UNCLASS_SHORT |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| local_deepseek_r1_32b | 3760 | 2034 (54.1%) | 11 (0.3%) | 676 (18.0%) | 34 (0.9%) | **863 (23.0%)** | 82 (2.2%) | 59 (1.6%) | 1 |
| openai_o4_mini_high | 3457 | 2540 (73.5%) | 100 (2.9%) | 454 (13.1%) | 37 (1.1%) | 78 (2.3%) | 127 (3.7%) | 121 (3.5%) | 0 |
| anthropic_sonnet_4_6 | 3800 | 1692 (44.5%) | **618 (16.3%)** | 1133 (29.8%) | 238 (6.3%) | 96 (2.5%) | 2 (0.1%) | 21 (0.6%) | 0 |
| **ALL** | **11017** | 6266 (56.9%) | 729 (6.6%) | 2263 (20.5%) | 309 (2.8%) | 1037 (9.4%) | 211 (1.9%) | 201 (1.8%) | 1 |

Restricted to snippet-bearing spans (comparable to the legacy denominator, n=10,605):
ANCHORED 59.1%, STITCHED 6.9%, DRIFTED 21.3%, UNTRACEABLE_PARTIAL 2.9%,
UNTRACEABLE_NO_BASIS 9.8% → **untraceable total 12.7%**.

The taxonomy separates what the old label conflated, and the three arms now have visibly
different signatures:

- **local** — high ANCHORED (54.1%) *and* high NO_BASIS (23.0%), almost no STITCHED
  (0.3%). It either copies a passage cleanly or writes its own sentence. It essentially
  never joins passages.
- **openai** — the compliance leader: 73.5% ANCHORED, 2.3% NO_BASIS.
- **anthropic** — lowest ANCHORED (44.5%) but its non-anchored mass is almost all real
  text: STITCHED 16.3% + DRIFTED 29.8% = 46.1%, against only 2.5% NO_BASIS. Its failure
  mode is contiguity, not invention.

---

## 2. Taxonomy × field class, per arm

Denominator = all spans of that arm and field class.

| arm / class | n | ANCHORED | STITCHED | DRIFTED | UNTR_PARTIAL | UNTR_NO_BASIS |
|---|---:|---:|---:|---:|---:|---:|
| local / extractive | 1879 | 54.3% | 0.3% | 16.4% | 1.1% | 21.4% |
| local / interpretive | 1879 | 53.9% | 0.3% | 19.6% | 0.7% | 24.4% |
| openai / extractive | 1737 | 69.1% | 3.6% | 12.2% | 0.9% | 1.6% |
| openai / interpretive | 1720 | 77.8% | 2.2% | 14.1% | 1.3% | 3.0% |
| anthropic / extractive | 1900 | 51.8% | 11.5% | 26.5% | 5.6% | 3.3% |
| anthropic / interpretive | 1900 | 37.2% | **21.1%** | 33.1% | 6.9% | 1.7% |
| **POOLED / extractive** | 5516 | 58.1% | 5.2% | 18.6% | 2.6% | 8.9% |
| **POOLED / interpretive** | 5499 | 55.6% | 8.1% | 22.5% | 3.0% | 9.9% |

No-basis rate by field class (denominator = snippet-bearing spans):

| arm | extractive | interpretive |
|---|---:|---:|
| local | 403/1758 (22.9%) | 458/1859 (24.6%) |
| openai | 27/1517 (1.8%) | 51/1692 (3.0%) |
| anthropic | 63/1877 (3.4%) | **33/1900 (1.7%)** — reversed |
| **POOLED** | **493/5152 (9.6%)** | **542/5451 (9.9%)** |

**This is the census's most important negative result and §6.2 treats it as such.**

---

## 3. PROPOSED field classification — ARCHITECT RATIFICATION REQUIRED

Implemented in `analysis/provenance/field_class.py`, marked
`STATUS = "PROPOSED — ARCHITECT RATIFICATION REQUIRED"`, and consumed by nothing
downstream. Derived from codebook `type`, `judge_rubric_family`,
`source_quote_required`, per-value definitions, and instruction wording; an inference
verb in the instruction ("infer", "your judgment", "synthesize", "decision tree") was
treated as decisive for INTERPRETIVE.

| field | tier | class | justification (one sentence) |
|---|---:|---|---|
| study_type | 1 | extractive | Instruction leads with "Look for explicit statements like 'prospective study,' 'case series'" — normally the paper's own word, inference only as documented fallback. |
| robot_platform | 1 | extractive | A proper noun naming hardware; if the paper used a robot it names it. |
| task_performed | 1 | extractive | The task is described in the methods in the paper's own words. |
| sample_size | 1 | extractive | Counts are stated; summing groups is arithmetic over quoted numbers. |
| surgical_domain | 1 | **interpretive** | Bench/phantom/simulation studies have no stated specialty, so 'Non-clinical Bench / Phantom' is an assignment, not a term the paper uses. |
| autonomy_level | 2 | **interpretive** | The codebook supplies a five-step decision tree for the common case where the paper never references the Yang levels. |
| validation_setting | 2 | extractive | In vivo / ex vivo / phantom / simulation is stated in the methods; "select most advanced" ranks stated facts. |
| task_monitor | 2 | **interpretive** | No paper writes 'R' or 'Shared'; agency is read off a system description. |
| task_generate | 2 | **interpretive** | Who authors the plan is inferred from an architecture description, never labelled. |
| task_select | 2 | **interpretive** | Plan selection is frequently not described at all, so the value is often a judgment of silence. |
| task_execute | 2 | **interpretive** | 'Shared' summarizes a cooperative-control arrangement described across several sentences. |
| system_maturity | 2 | **interpretive** | Readiness categories are the codebook's frame for the work, not the authors' claim. |
| study_design | 2 | **interpretive** | The codebook's design vocabulary is finer than authors' self-description. |
| country | 2 | extractive | Affiliation text is on page 1; the first-author fallback still resolves to text. |
| primary_outcome_metric | 3 | extractive | Defined positionally as the first quantitative outcome — a rule for locating text, explicitly "no judgment". |
| primary_outcome_value | 3 | extractive | The codebook says to copy the exact numeric reporting. |
| comparison_to_human | 3 | extractive | Either the paper reports the comparison in text or it does not. |
| secondary_outcomes | 3 | extractive | Quoted metric/value pairs from the results section. |
| key_limitation | 4 | **interpretive** | Instruction forbids copying the authors' limitations and asks for the extractor's judgment. |
| clinical_readiness_assessment | 4 | **interpretive** | The codebook states outright that "there is no right answer in the text". |

Split: **10 extractive / 10 interpretive.** The empirical verdict on this split is in §6.2
and it is not favourable — ratify with that in hand.

---

## 4. Legacy figure restated

`analysis/provenance/legacy.py` reproduces `auditor.grep_verify` verdict-for-verdict
(equivalence asserted by test fixtures covering exact/fuzzy-accept/fuzzy-reject/degenerate
inputs, including one inherited quirk deliberately preserved: a whitespace-only snippet
normalizes to `""` and is reported anchored).

| arm | legacy unanchored / snippet-bearing | rate |
|---|---:|---:|
| anthropic_sonnet_4_6 | 1317 / 3777 | 34.9% |
| local_deepseek_r1_32b | 828 / 3619 | 22.9% |
| openai_o4_mini_high | 194 / 3209 | 6.0% |
| **pooled** | **2339 / 10605** | **22.1%** |

**Identical to DIAG-UNANCHOR-01 to the last span**, which validates both the earlier
diagnostic and the fast reimplementation. For the revision-history disclosure the
statement is: *the previously reported 22.1% "unanchored" rate is reproduced exactly; under
the pinned taxonomy the comparable quantity (UNTRACEABLE_PARTIAL + UNTRACEABLE_NO_BASIS)
is 12.7%, and the difference is fully accounted for in §5 — the legacy instrument had no
per-sentence verdict and so could not distinguish a stitched quotation from an invented
one.*

---

## 5. Reconciliation with the prior census

Delta: legacy 2339 untraceable → taxonomy 1346. **−993 spans, −9.4pp.** Far above the
0.5pp tolerance, and fully decomposed:

| direction | n | driver |
|---|---:|---|
| legacy-unanchored → **STITCHED** | 682 | Per-sentence matching. 538 of these carry a literal ellipsis: the whole snippet could never match, every sentence does. |
| legacy-unanchored → **DRIFTED** | 533 | Per-sentence matching plus T1 (HTML entities) and T7 (de-hyphenation), neither of which the legacy normalization had. |
| **rescued subtotal** | **1215** | |
| legacy-anchored → UNTRACEABLE_PARTIAL | 91 | Whole-snippet fuzzy passed while an individual sentence failed. |
| legacy-anchored → UNTRACEABLE_NO_BASIS | 131 | Same, at the extreme. |
| **added subtotal** | **222** | |
| **net** | **−993** | 1215 − 222 = 993 ✓ exact |

Of the 222 spans moving *into* untraceable, **166 (75%) have `min_ratio` in [0.85, 0.90)**
— i.e. they are resolved by the threshold change alone and return to DRIFTED at τ=0.85.
There is no unexplained residual.

Threshold sensitivity (re-derived from stored per-sentence ratios; ANCHORED and STITCHED
are threshold-independent by construction):

| τ | DRIFTED | UNTR_PARTIAL | UNTR_NO_BASIS | untraceable (% of snippet-bearing) |
|---|---:|---:|---:|---:|
| 0.85 | 2484 | 219 | 906 | 1125 (**10.6%**) |
| **0.90** | 2263 | 309 | 1037 | 1346 (**12.7%**) |
| 0.95 | 1767 | 525 | 1317 | 1842 (**17.4%**) |

Strict-variant sensitivity (a sentence counts as verbatim only if it falls inside a single
paper sentence, never across a boundary): **44 of 4338 spans change class (1.0%), all
STITCHED → DRIFTED.** The primary rule is robust; the choice of containment target is not
a material degree of freedom.

Parser-tier check: no-basis 9.7% on Docling-parsed papers vs 11.0% on PyMuPDF (5 papers,
291 spans) — consistent with DIAG-VISION-01's conclusion that tier is not a live confound.

---

## 6. Five things in the data that argue against the design — surfaced, not smoothed

### 6.1 One pinned constant moves more spans than the threshold does

T-seg-0 (ellipsis pre-split) is not a detail. 1102 anthropic spans carry a literal
ellipsis; under the pinned definitions 544 classify STITCHED and 426 DRIFTED. Without the
pre-split, pysbd would hand back a single unmatchable unit and essentially all of them
would land in UNTRACEABLE_NO_BASIS — anthropic's no-basis rate would read ~25% instead of
2.5%. **A reviewer who disagrees with T-seg-0 disagrees with the headline result.** It is
defensible (an ellipsis is the writer's own statement that text was omitted, so scoring
the two sides separately is what the writer asked for), but it should be argued for in the
manuscript rather than buried in a definitions file. It also deserves noting that pysbd
*mutates* a trailing `...` to `.` even with `clean=False`; the pre-split exists partly to
keep the tokenizer away from text it would silently rewrite.

### 6.2 The proposed extractive/interpretive split does not predict no-basis

Pooled: extractive 9.6% vs interpretive 9.9%. That is not a difference. Per arm it is
inconsistent in sign — local +1.7pp and openai +1.2pp toward interpretive, **anthropic
−1.7pp the other way**. The DIAG-OPTSET-01 hypothesis (models invent prose where no
sentence can state the coded value) survives as a *description of the local arm's
behaviour* but **fails as a property of the field taxonomy**: local's no-basis rate is
22.9% on extractive fields, barely below its 24.6% on interpretive ones. Whatever drives
local to write its own sentences, it is not confined to fields whose answers are
unquotable.

What the split *does* predict is **stitching**: anthropic stitches 21.1% of interpretive
spans against 11.5% of extractive ones, and pooled STITCHED is 8.1% interpretive vs 5.2%
extractive. That is the coherent signal — a judgment field needs evidence from several
places, so the arm joins passages. If the split is ratified, it should be ratified as a
predictor of *contiguity pressure*, not of fabrication risk.

### 6.3 The worst field by no-basis is an extractive one, for a reason the taxonomy misses

`comparison_to_human` tops the table: 102 no-basis spans of 550. **80 of those 102 carry
the value "No comparison reported"** — the arm correctly determined the paper contains no
robot-vs-human comparison and then wrote a sentence *saying so* into the snippet field
instead of leaving it empty. Corpus-wide, **162 of 1037 no-basis spans (15.6%) have a value
that is itself an absence claim** (`NR` ×82, `No comparison reported` ×80), 157 of them
local.

These are not evidence failures in the same sense as an invented quotation; they are
absence declarations placed in the wrong column. The taxonomy already has
`ABSENCE_DECLARED` for exactly this, but it only fires when the snippet is *empty*. **If
absence-claim values were exempted from the evidence obligation, no-basis would fall from
1037 to 875 — 12.7% → 11.2% untraceable, and 9.8% → 8.3% no-basis.** Recommend the
architect decide explicitly whether an absence claim carries an evidence obligation at all;
until then the 9.8% figure bundles two different defects.

### 6.4 Worst-five fields do not separate by class either

| field | class | no-basis | spans | rate | per-arm (local / openai / anthropic) |
|---|---|---:|---:|---:|---|
| comparison_to_human | extractive | 102 | 550 | 18.5% | 89 / 3 / 10 |
| task_select | interpretive | 85 | 549 | 15.5% | 66 / 11 / 8 |
| task_execute | interpretive | 80 | 550 | 14.5% | 66 / 9 / 5 |
| primary_outcome_value | extractive | 80 | 550 | 14.5% | 51 / 6 / 23 |
| task_generate | interpretive | 71 | 550 | 12.9% | 60 / 8 / 3 |

Three interpretive, two extractive, and the top entry extractive. The per-arm columns show
the real structure: **local accounts for 332 of these 418 no-basis spans (79%)**. The
dominant axis is arm, not field class.

### 6.5 A definitional soft spot worth stating in the manuscript

ANCHORED is contiguity in the *parsed markdown*, and markdown structure is deliberately not
stripped (DEFINITIONS §2). A snippet that quotes one true paragraph interrupted by a
Docling `<!-- image -->` placeholder or a `## heading` is therefore STITCHED, not ANCHORED
— a parser artifact reported as an arm behaviour. The conservative alternative (strip the
markers) is worse, because it would splice genuinely non-adjacent passages and create false
ANCHORED verdicts. The strict-variant result (1.0% class change) bounds a related degree of
freedom but not this one. Recommend disclosing it rather than attempting to fix it: the
current rule is a true statement about the text as parsed, which is the text the arms
actually saw.

---

## Acceptance gates

| gate | status |
|---|---|
| 1. DEFINITIONS.md complete, no unstated constants, tokenizer named | ✅ `prov-def-1`, pysbd 0.3.4 named and pinned in requirements.txt; §5 table lists every constant with rationale |
| 2. Census covers 100% of spans; per-span table persisted; classifier tests pass; suite green | ✅ 11,017 / 11,017; `provenance_classifications` populated; 28 new tests; 1318 passed / 15 deselected |
| 3. Field classification present and marked PROPOSED | ✅ §3, `STATUS = "PROPOSED — ARCHITECT RATIFICATION REQUIRED"` |
| 4. Reconciliation explained | ✅ §5, exact decomposition, zero residual |
| 5. Zero Ollama calls; no prompt/codebook/judge/audit-table edits | ✅ `git diff` touched only requirements.txt (+1 line); migration 010 creates new tables only |

**Out of scope and not done:** per-field evidence-policy fix design, judge restatement of
the 33.7% figure, prompt/codebook changes, Arm P analysis, primer.md edits.
