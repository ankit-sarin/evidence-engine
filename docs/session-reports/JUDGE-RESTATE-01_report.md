# JUDGE-RESTATE-01 — Pass 2 judge verdicts under the frozen provenance taxonomy

Project: evidence-engine · Machine: DGX (spark-59e4) · Date: 2026-07-28
Commit **`97179b4`** · repo clean at start (`9b0da41`, FIELDCLASS-01 handed back).

**Ollama call count: 0.** Read-only against the judge, census and Run 6 tables; no judge
rework, no re-scoring, no prompt/codebook edits. Full offline suite: **1402 passed, 15
deselected**.

| | |
|---|---|
| **Judge run** | `surgical_autonomy_pass2_codebook_v2_20260604T042317Z` (the codebook-aware v2 run; 1,212 triples, 3,636 arm-rows, 0 failed) |
| Census run | `provcensus_surgical_autonomy_20260727T194748Z` (taxonomy v1.1, ABSENCE_CLAIM separated) |
| Field classes | `prov-fieldclass-1` (STATED / INFERABLE / JUDGMENT) |

### Framing, applied throughout

The judge is an **NLI-style supportedness** instrument: does this snippet support this value?
The census is a **lexical-provenance** instrument: can this snippet be located in the paper?
They measure different dimensions. `SUPPORTED` on `STITCHED` or `DRIFTED` is **expected
inter-instrument divergence, not judge failure** — the text is real, the arm joined or lightly
edited it, and it does support the value. The one cell that is a genuine judge failure is
**`SUPPORTED` on `UNTRACEABLE_NO_BASIS`**: the judge endorsing evidence the arm authored.

---

## 0. Join (Gate 1)

**Key: `(arm, paper_id, field_name)`**, with exactly two transformations, both documented in
code: the judge's arm label `local` maps to the census arm id `local_deepseek_r1_32b`, and the
judge's `paper_id` is stored as TEXT and is cast to INTEGER.

| | n |
|---|---:|
| judged arm-rows | 3,636 |
| joined | **3,472** |
| **unjoined** | **164** |

**The 164 unjoined rows are a finding, not a footnote.** They are returned by `join()` rather
than dropped, and they are not random:

- **Every one** belongs to an extraction that stored **exactly one span** for that paper
  (`span_count_of_affected_extraction: {1: 164}`).
- 152 are openai across 17 papers; 12 are local on paper 719.
- **160 of the 164 were scored `SUPPORTED`**; 4 `UNSUPPORTED`.

The underlying data defect is larger than previously recorded. Span counts per paper:

| arm | 20 spans | 19 spans | **1 span** |
|---|---:|---:|---:|
| anthropic | 190 | 0 | 0 |
| local | 186 | 2 | **2** |
| **openai** | 172 | 0 | **17** |

TAXONOMY-CENSUS-02 §6.6 flagged the two local collapsed extractions (`Title`, `field_1`).
**Seventeen openai papers collapsed the same way and had not been counted.** Because Pass 2's
input was the disagreement-pairs CSV rather than the span tables, the judge scored those
arm-rows anyway — rendering a supportedness verdict, 160 times SUPPORTED, on arm-rows for
which no evidence span exists at all. That is an input-integrity problem upstream of the
judge, but it inflates the judge's SUPPORTED count and belongs in the disclosure.

All rates below use the 3,472 joined rows.

---

## 1. Restated headline vs. legacy (Gate 2)

Of **2,210 `SUPPORTED`** verdicts:

| measure | POOLED | local | openai | anthropic |
|---|---:|---:|---:|---:|
| SUPPORTED verdicts (joined) | 2,210 | 744 | 840 | 626 |
| **legacy instrument, recomputed** — "absent from the paper" | **752 (34.03%)** | 281 (37.77%) | 148 (17.62%) | 323 (51.60%) |
| **restated: on no-basis (true failure)** | **268 (12.13%)** | **202 (27.15%)** | 28 (3.33%) | 38 (6.07%) |
| on stitched/drifted (real-but-nonconforming) | 712 (32.22%) | 149 (20.03%) | 187 (22.26%) | **376 (60.06%)** |
| on any non-anchored | 1,230 (55.66%) | 451 (60.62%) | 297 (35.36%) | 482 (77.00%) |
| on untraceable (partial + no-basis) | 353 (15.97%) | — | — | — |
| on anchored | 980 (44.34%) | 293 (39.38%) | 543 (64.64%) | 144 (23.00%) |
| on ABSENCE_CLAIM | 53 (2.40%) | 53 (7.12%) | 0 | 0 |

**The headline moves 34.0% → 12.1%.** The 22-point drop is almost exactly the
stitched/drifted population (32.2%): the legacy instrument counted real-but-nonconforming
quotation as absence, because it had no per-sentence verdict and so could not distinguish a
stitched quotation from an invented one.

**On the standing 33.7% figure — I cannot reproduce it exactly, and that should be resolved
before it is cited or retired.** Faithfully recomputing the legacy instrument gives **34.03%**
on this judge run (0.33 pp above the standing figure) and **30.90%** on the superseded
`…20260421T174729Z` run. Alternative denominators I tested: 32.45% excluding ABSENCE_CLAIM,
30.51% restricting to non-empty snippets, 28.75% both, 37.20% including PARTIALLY_SUPPORTED.
None lands on 33.7%. The standing number is *approximately* the v2-run figure, but its exact
provenance is not recoverable from what is on disk.

Full verdict × provenance table (pooled, 3,472 rows):

| verdict | ANCHORED | STITCHED | DRIFTED | UNTR_PARTIAL | UNTR_NO_BASIS | ABSENCE_CLAIM | ABSENCE_DECL | MISSING_SNIP |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| SUPPORTED | 980 | 172 | 540 | 85 | **268** | 53 | 88 | **24** |
| PARTIALLY_SUPPORTED | 272 | 155 | 288 | 66 | 90 | 0 | 0 | 2 |
| UNSUPPORTED | 84 | 62 | 123 | 25 | 67 | 5 | 2 | 20 |

Two cells beyond the brief are worth flagging: **88 SUPPORTED on `ABSENCE_DECLARED`** (empty
snippet, absence value — arguably correct, the arm claimed nothing and offered nothing) and
**24 SUPPORTED on `MISSING_SNIPPET`** (empty snippet, but the value asserts something — the
judge endorsed a claim with no evidence whatsoever).

---

## 2. True failures (Gate 3)

**268 `SUPPORTED`-on-`UNTRACEABLE_NO_BASIS` verdicts** — 12.13% of SUPPORTED, 7.72% of all
joined arm-rows. By arm and field class (cell = failures / SUPPORTED-in-class):

| arm | STATED | INFERABLE | JUDGMENT | total |
|---|---|---|---|---:|
| local | 66/339 (**19.47%**) | 71/192 (**36.98%**) | 65/213 (30.52%) | **202** |
| openai | 15/424 (3.54%) | 5/187 (2.67%) | 8/229 (3.49%) | 28 |
| anthropic | 25/262 (9.54%) | 7/137 (5.11%) | 6/227 (2.64%) | 38 |
| **POOLED** | 106/1025 (10.34%) | **83/516 (16.09%)** | 79/669 (11.81%) | **268** |

### Does the true-failure set concentrate on the INFERABLE fields the census flagged?

**YES in rate, pooled and on the local arm; NO in absolute counts; and NO on the anthropic
arm.** Precisely:

- **Rates**: pooled INFERABLE 16.09% > JUDGMENT 11.81% > STATED 10.34%. Local: 36.98% >
  30.52% > 19.47%. This is the same ordering FIELDCLASS-01 found in the census itself, now
  reproduced in an independent instrument — the judge is most likely to endorse authored
  evidence exactly where the census says the answer exists but is unquotable.
- **Counts**: the top failure fields are `primary_outcome_value` 35 (STATED),
  `system_maturity` 28 (JUDGMENT), `task_generate` 27 (INFERABLE), `study_design` 26
  (JUDGMENT), `secondary_outcomes` 24 (STATED). All three classes are well represented; the
  concentration is in rates, not in where the failures live. A fix targeting only INFERABLE
  fields would leave roughly two-thirds of the 268 untouched.
- **Anthropic reverses** (STATED 9.54% > INFERABLE 5.11% > JUDGMENT 2.64%), consistent with
  its arm-level behaviour in every previous census: it stitches rather than invents, so its
  small no-basis population does not track unquotability.

---

## 3. Symmetry check (Gate 4)

`UNSUPPORTED` on `ANCHORED` — the judge rejecting evidence that is verbatim present:

| arm | UNSUPPORTED / ANCHORED | rate | also PARTIALLY_SUPPORTED on anchored |
|---|---|---:|---:|
| local | 45 / 463 | 9.72% | 125 |
| openai | 22 / 654 | 3.36% | 89 |
| anthropic | 17 / 219 | 7.76% | 58 |
| **POOLED** | **84 / 1,336** | **6.29%** | 272 |

**Reading 10 sampled examples (seed 20260728): this cell is a judge *strength*, not a
defect — 10 of 10 are correct.** `ANCHORED` means "the quote is real", not "the quote supports
the value", so an NLI instrument *should* reject a genuine quotation that does not entail the
claim. Representative:

| # | paper / field / arm | value | anchored snippet | judge |
|---|---|---|---|---|
| 1 | 407 `secondary_outcomes` local | NASA-TLX Mental 6.5, Physical 3.0 | "Questionnaires reveal that the performance of the RFE is the best among the three endoscope systems." | source gives no NASA-TLX scores — values appear fabricated |
| 5 | 411 `sample_size` anthropic | 30 | "…we conducted 10 tests on fresh porcine tongue samples" (×3 strategies) | correct arithmetic, but the arm's snippet does not state 30 |
| 8 | 694 `comparison_to_human` anthropic | "No comparison reported" | a real sentence about soft-robot compliance | the span does not establish the absence it is offered for |
| 10 | 517 `key_limitation` local | "demonstrated on a bench task without clinical translation" | "The modular architecture is expected to generalize…" | source says the opposite of the limitation claimed |

**5 of the 10 are `key_limitation`** — a JUDGMENT field where the arm quotes a real passage but
the limitation is its own inference. That is the JUDGMENT-field evidence problem showing up
from the judge's side, and it is the judge behaving correctly under a contract that gives it no
way to say "supporting passage, not sufficient evidence".

---

## 4. ABSENCE_CLAIM interaction (Gate 5)

Of the 150 `ABSENCE_CLAIM` spans in the census, **58 reached Pass 2** (Pass 2 scored only
medium/high fabrication-risk triples). All 58 are local — the only arm that produces absence
claims.

| verdict | n | share |
|---|---:|---:|
| **SUPPORTED** | **53** | **91.4%** |
| UNSUPPORTED | 5 | 8.6% |
| PARTIALLY_SUPPORTED | 0 | 0% |

By detector pattern: `P1_referent_negation` 36, `P2_bare_no_np` 13, `P6_only_x_reported` 5,
`P3_not_explicitly` 3, `P4_bare_sentinel` 1.
By field: `sample_size` 20, `task_select` 12, `comparison_to_human` 12, `secondary_outcomes` 9,
`primary_outcome_value` 3, `country` 2.

**The judge endorses a model-authored absence assertion as supporting evidence 91% of the
time.** This is not obviously wrong on the judge's own terms — "The paper does not report the
sample size" genuinely does support the value `NR` — but it means the judge cannot currently
distinguish *evidence that the paper is silent* from *an assertion that the paper is silent*.
Combined with the 88 SUPPORTED-on-`ABSENCE_DECLARED` rows, this is the direct input the
`NOT_FOUND` schema fix needs: the schema has no vocabulary for absence, so absence gets written
into the evidence field, and the judge then validates it.

---

## 5. Judge health under the two-instrument framing

**Conditional — the v2 judge does not need rework before Paper 1, provided three things are
disclosed rather than fixed.** On the evidence here the judge is behaving as a supportedness
instrument should: it endorses anchored evidence 44% of the time and non-anchored-but-real
evidence at rates that track how much real text the arm actually quoted (anthropic 60%
stitched/drifted, matching its known stitching style); it correctly rejects genuine quotations
that do not entail the claim in all 10 sampled symmetry cases; and its true-failure rate —
endorsing evidence with no textual basis at all — is 12.1% of SUPPORTED verdicts, concentrated
in the arm and the field classes where the census independently predicts authored prose. Two
instruments built on different principles agreeing on where the problem is, is the strongest
validation signal in this analysis. What must be disclosed, not repaired: (i) the standing
33.7% figure is superseded and should be restated as 12.1% with the legacy recomputation
(34.03%) alongside and its irreproducible 0.33 pp discrepancy noted; (ii) the judge scored 164
arm-rows that have no stored evidence span, 160 of them SUPPORTED, because 19 extractions
collapsed to a single span and Pass 2 read values from the pairs CSV rather than the span
tables — an input-integrity defect upstream of the judge that nonetheless inflates its
SUPPORTED count by up to 7%; and (iii) the judge validates model-authored absence assertions
91% of the time, which is correct on its own terms and wrong for the paper's purposes, and is
the strongest argument for the `NOT_FOUND` schema fix rather than for judge rework. Rework
would be warranted if the symmetry cell had shown the judge rejecting sound evidence, or if
true failures had been spread evenly across arms and field classes; neither is the case.

---

## Acceptance gates

| gate | status |
|---|---|
| 1. Join documented; unjoined rows enumerated and explained | ✅ §0 — key + two transformations in code; 164 unjoined, all from single-span extractions, 160 SUPPORTED |
| 2. Restated headline with legacy alongside | ✅ §1 — 12.13% vs 34.03% recomputed legacy, per arm and pooled; 33.7% irreproducibility stated |
| 3. True-failure counts, field-class × arm cross-tab, concentration answered | ✅ §2 — 268 failures; YES in rate (pooled + local), NO in counts, NO on anthropic |
| 4. Symmetry check with examples | ✅ §3 — 84/1,336 (6.29%), 10 sampled, 10/10 judge-correct |
| 5. Zero Ollama; no judge/prompt/codebook edits; suite green | ✅ commit touches `analysis/paper1/judge_provenance.py` + one test file; 1402 passed |

**Out of scope and not done:** judge rework or re-scoring, evidence-contract implementation,
prompt/codebook changes, Arm P, literature-citation verification.
