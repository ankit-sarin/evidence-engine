# TAXONOMY-CENSUS-02 — Absence-claim separation and field-class recount

Project: evidence-engine · Machine: DGX (spark-59e4) · Date: 2026-07-27
Commit: `fdbf076` — analysis code + DEFINITIONS.md v1.1 amendment. Census outputs in
gitignored `data/surgical_autonomy/analysis/provenance/`.

**Ollama call count: 0.** Pure-Python regex and string matching throughout; no inference,
no `/api/*`, no CLI. Source tables (`evidence_spans`, `cloud_evidence_spans`, `extractions`,
`cloud_extractions`, `full_text_assets`) opened read-only; the only writes were INSERTs into
the two census tables plus migration 011's rebuild of `provenance_classifications`.

| | |
|---|---|
| New census run | `provcensus_surgical_autonomy_20260727T194748Z` |
| Prior run (retained, queryable) | `provcensus_surgical_autonomy_20260727T183832Z` |
| Definitions | `prov-def-1.1` · absence patterns `prov-absence-1` · normalization `prov-norm-1` |
| Coverage | **11,017 / 11,017 spans (100%)**, key set identical to the prior run |
| Tests | 60 provenance + 4 migration; full offline suite **1350 passed, 15 deselected** |

---

## 1. Movement into ABSENCE_CLAIM

| prior class | → ABSENCE_CLAIM |
|---|---:|
| `UNTRACEABLE_NO_BASIS` | **149** |
| `ANCHORED` | **1** |
| every other class | 0 |
| **total** | **150** |

**No span changed class in any other way** — the run is otherwise bit-identical to CENSUS-01,
which is the intended behaviour: the amendment carves out a population, it does not perturb
the ladder.

By pattern:

| pattern | n |
|---|---:|
| `P1_referent_negation` | 91 |
| `P2_bare_no_np` | 45 |
| `P3_not_explicitly` | 6 |
| `P6_only_x_reported` | 5 |
| `P5_referent_without_report` | 2 |
| `P4_bare_sentinel` | 1 |

**By arm: local 150, openai 0, anthropic 0.** The behaviour is entirely local's.

The single `ANCHORED → ABSENCE_CLAIM` span is the P4 precedence case predicted in §A3.2: a
bare `NR` snippet that "anchored" in v1.0 only because the two-character string `nr` occurs
inside some word of that paper. Note that CENSUS-01's headline 22.1%-era figure counted it
as a *verbatim quotation*.

**Against expectation:** CENSUS-01 anticipated ~162 movers, based on the count of no-basis
spans whose *value* was an absence sentinel. The realized figure is 149 from no-basis. The
13-span gap is not a detector miss but a definitional difference, and it is the right one:
the amendment is **snippet-based**, so a span whose value is `NR` but whose snippet is an
ordinary invented sentence stays in no-basis. §4 and §5 treat this directly.

---

## 2. Recounted distribution

### 2a. Per arm (denominator = all spans)

| arm | n | ANCHORED | STITCHED | DRIFTED | UNTR_PARTIAL | UNTR_NO_BASIS | **ABSENCE_CLAIM** | ABSENCE_DECL | MISSING_SNIP |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| local | 3760 | 54.1% | 0.3% | 18.0% | 0.9% | **19.0%** | **4.0%** | 2.2% | 1.6% |
| openai | 3457 | 73.5% | 2.9% | 13.1% | 1.1% | 2.3% | 0.0% | 3.7% | 3.5% |
| anthropic | 3800 | 44.5% | 16.3% | 29.8% | 6.3% | 2.5% | 0.0% | 0.1% | 0.6% |
| **all** | 11017 | 56.9% | 6.6% | 20.5% | 2.8% | **8.1%** | **1.4%** | 1.9% | 1.8% |

Pooled untraceable (PARTIAL + NO_BASIS) moves **1346 → 1197 of 10,605 snippet-bearing spans,
12.7% → 11.3%**. Local's no-basis falls 23.0% → 19.0%.

### 2b. Taxonomy × field class × arm (denominator = all spans of that cell)

| arm / class | n | ANCHORED | STITCHED | DRIFTED | UNTR_PARTIAL | UNTR_NO_BASIS | ABSENCE_CLAIM |
|---|---:|---:|---:|---:|---:|---:|---:|
| local / extractive | 1879 | 54.3% | 0.3% | 16.4% | 1.1% | **14.3%** | **7.2%** |
| local / interpretive | 1879 | 53.9% | 0.3% | 19.6% | 0.7% | **23.6%** | 0.8% |
| openai / extractive | 1737 | 69.1% | 3.6% | 12.2% | 0.9% | 1.6% | 0.0% |
| openai / interpretive | 1720 | 77.8% | 2.2% | 14.1% | 1.3% | 3.0% | 0.0% |
| anthropic / extractive | 1900 | 51.8% | 11.5% | 26.5% | 5.6% | 3.3% | 0.0% |
| anthropic / interpretive | 1900 | 37.2% | 21.1% | 33.1% | 6.9% | 1.7% | 0.0% |
| POOLED / extractive | 5516 | 58.1% | 5.2% | 18.6% | 2.6% | **6.5%** | **2.4%** |
| POOLED / interpretive | 5499 | 55.6% | 8.1% | 22.5% | 3.0% | **9.6%** | 0.3% |

The asymmetry that drives everything is visible in one row-pair: **local's absence claims are
7.2% of extractive spans and 0.8% of interpretive ones — a 9:1 concentration.** That is
mechanically obvious in hindsight (only an extractive field can be *absent* from a paper; an
interpretive field always has an answer, because the answer is a judgment) and it is exactly
why leaving them in the no-basis bucket suppressed the field-class signal.

---

## 3. Headline verdict

> **With absence claims separated, is no-basis elevated on interpretive vs extractive fields?**
>
> **YES pooled, and YES on both local models — but NO on anthropic.** The hypothesis survives
> on 2 of 3 arms and on the pooled corpus, having failed on all counts in CENSUS-01.

Denominator = spans with a snippet, **excluding ABSENCE_CLAIM**:

| arm | extractive | interpretive | diff | ratio |
|---|---:|---:|---:|---:|
| local | 269/1623 (**16.6%**) | 443/1844 (**24.0%**) | **+7.4 pp** | **1.45×** |
| openai | 27/1517 (1.8%) | 51/1692 (3.0%) | +1.2 pp | 1.69× |
| anthropic | 63/1877 (3.4%) | 33/1900 (1.7%) | **−1.6 pp** | 0.52× |
| **POOLED** | 359/5017 (**7.2%**) | 527/5436 (**9.7%**) | **+2.5 pp** | **1.35×** |

Side by side with CENSUS-01, where absence claims were still inside no-basis:

| arm | CENSUS-01 ext → int | CENSUS-02 ext → int |
|---|---|---|
| local | 22.9% → 24.6% (+1.7, **1.07×**) | 16.6% → 24.0% (+7.4, **1.45×**) |
| openai | 1.8% → 3.0% (+1.2, 1.69×) | 1.8% → 3.0% (+1.2, 1.69×) — unchanged |
| anthropic | 3.4% → 1.7% (−1.7) | 3.4% → 1.7% (−1.6) — unchanged |
| **POOLED** | 9.6% → 9.9% (+0.3, **1.03×**) | 7.2% → 9.7% (+2.5, **1.35×**) |

The entire change is local's, because local is the only arm that produced absence claims at
all. CENSUS-01's negative result was a measurement artifact of exactly the kind the amendment
was written to remove: absence claims are concentrated on extractive fields, so leaving them
in no-basis inflated the extractive rate by 6.3 pp and flattened the contrast to nothing.

**What has not changed: anthropic still runs the wrong way**, and it does so with zero absence
claims, so no further separation can rescue it. Any ratification of the field split must be
stated as a property that holds for the two arms that write their own prose and does not hold
for the arm whose characteristic failure is stitching.

---

## 4. Per-field no-basis, all 20 fields, all arms

Denominator excludes ABSENCE_CLAIM from both numerator and denominator. Final column is the
absence-claim count carved out of that field.

| field | class | no-basis | denom | rate | local | openai | anthro | absence |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| task_execute | interpretive | 80 | 546 | 14.7% | 35.1% | 5.4% | 2.6% | 0 |
| primary_outcome_value | extractive | 73 | 516 | 14.1% | 25.7% | 3.8% | 12.2% | 7 |
| task_select | interpretive | 71 | 522 | 13.6% | 31.5% | 6.6% | 4.2% | 14 |
| task_generate | interpretive | 71 | 545 | 13.0% | 31.9% | 4.8% | 1.6% | 0 |
| task_monitor | interpretive | 64 | 539 | 11.9% | 28.1% | 4.9% | 2.1% | 0 |
| secondary_outcomes | extractive | 51 | 485 | 10.5% | 25.2% | 4.1% | 2.8% | 12 |
| primary_outcome_metric | extractive | 56 | 541 | 10.4% | 20.5% | 3.6% | 6.3% | 1 |
| country | extractive | 54 | 538 | 10.0% | 28.1% | 1.2% | 1.1% | 3 |
| system_maturity | interpretive | 53 | 548 | 9.7% | 25.1% | 2.3% | 1.1% | 0 |
| study_design | interpretive | 50 | 548 | 9.1% | 20.9% | 2.9% | 3.2% | 0 |
| comparison_to_human | extractive | 27 | 304 | 8.9% | 19.2% | 6.0% | 5.5% | **75** |
| clinical_readiness_assessment | interpretive | 46 | 547 | 8.4% | 23.7% | 0.6% | 0.5% | 1 |
| autonomy_level | interpretive | 39 | 548 | 7.1% | 18.2% | 1.8% | 1.1% | 0 |
| validation_setting | extractive | 35 | 545 | 6.4% | 17.4% | 0.0% | 1.6% | 0 |
| key_limitation | interpretive | 28 | 549 | 5.1% | 13.3% | 1.2% | 0.5% | 0 |
| surgical_domain | interpretive | 25 | 544 | 4.6% | 13.1% | 0.0% | 0.5% | 0 |
| sample_size | extractive | 19 | 429 | 4.4% | 14.4% | 0.8% | 1.1% | **37** |
| task_performed | extractive | 19 | 549 | 3.5% | 8.5% | 0.6% | 1.1% | 0 |
| study_type | extractive | 14 | 566 | 2.5% | 5.9% | 1.1% | 0.5% | 0 |
| robot_platform | extractive | 11 | 544 | 2.0% | 4.3% | 0.0% | 1.6% | 0 |

Reading the gradient directly, rather than through the class aggregate:

- **`comparison_to_human` is transformed.** It topped CENSUS-01's table at 18.5% no-basis;
  75 of its 102 no-basis spans were absence claims, and it now sits mid-table at 8.9% on a
  denominator shrunk to 304. The field that most embarrassed the extractive hypothesis was
  the field most contaminated by absence claims.
- **The top of the table is now four interpretive fields out of five** (`task_execute`,
  `task_select`, `task_generate`, `task_monitor`), with `primary_outcome_value` the extractive
  intruder at rank 2 — and its local rate (25.7%) is well below the `task_*` local rates
  (28–35%). The bottom five are four extractive plus `surgical_domain`.
- **`sample_size` drops out of contention** (4.4%, 37 absence claims removed).
- Three extractive fields remain stubbornly high on local: `country` (28.1%),
  `primary_outcome_value` (25.7%), `secondary_outcomes` (25.2%). These are the residual
  counter-evidence and they are discussed in §6.

---

## 5. Detector validation — 50 spans inspected

**5a. 25 randomly sampled ABSENCE_CLAIM spans (seed 20260727): 25/25 correct.
**False positives: 0.** Every one is an unambiguous assertion about what the paper does not
report. Representative:

- `P1` — "The paper does not explicitly state the number of cases, experiments, or subjects."
- `P1` — "The paper does not compare the autonomous system's performance to human performance."
- `P2` — "No explicit comparison to human performance."
- `P3` — "Not explicitly stated."
- `P6` — "Only one primary outcome was reported."

Pattern mix in the sample: P1 ×17, P2 ×4, P3 ×2, P6 ×1, P5 ×1 — consistent with the
population.

**5b. 25 randomly sampled residual `UNTRACEABLE_NO_BASIS` spans: 0 missed absence claims.
**False negatives in-sample: 0.** All 25 are genuine no-basis spans — model-authored
rationales, paraphrased results, or invented specificity. Representative:

- "The system generates a plan based on detected mental workload." (rationale)
- "The robot autonomously adjusts the FOV without real-time human control." (rationale;
  note it contains "without" and correctly did *not* trigger P5, whose subject must be a
  paper referent)
- "The mean distance was 3.2538 mm with a standard deviation of 0.3149 mm." (paraphrased
  result, no textual match)
- "The authors acknowledge the need for further clinical validation." (a claim about what the
  authors *did* say — the inverse of an absence claim)

**Known out-of-sample false negatives.** DEFINITIONS §A5 enumerates ~22 spans corpus-wide
that carry an absence *value* while their snippet is a rationale, e.g. "The study focuses on
the system's performance without direct comparison to human operators." (subject is not a
paper referent) and "The exact numeric results are not provided in the snippet." (subject is
the result, not the paper). A random 25-span draw from 888 would be expected to contain ~0.6
of these, so seeing zero is consistent, not evidence of their absence. **Estimated detector
recall against the absence-value proxy: 140/162 = 86%; precision on inspection: 100%.**

---

## 6. Arguments against the ABSENCE_CLAIM design

### 6.1 The precedence rule that motivated the amendment never fires on real data

`ANCHORED beats P1/P2/P3/P5/P6` (§A3.1) — the rule written around the "paper says it itself"
near-miss — applies to **zero** corpus spans. No absence-pattern snippet in Run 6 was anchored.
It is exercised only by the unit test. That is not a defect (the rule is the correct semantics
and costs nothing) but it should not be described in the manuscript as though it were doing
work. Conversely the *inverse* exception, P4 beating ANCHORED, fires on exactly one span — so
both halves of the precedence rule are, empirically, near-decorative.

### 6.2 Snippet-based vs value-based is a live fork, and the report can only defend one

The amendment asks what the *snippet asserts*. An alternative asks what the *value claims*
— and it would catch the 22 §A5 residuals, raising ABSENCE_CLAIM to ~172. The two disagree on
about 13% of the population. The snippet-based rule was chosen because a value-based rule
would relabel spans whose snippet is an ordinary invented sentence purely because the value
happened to be `NR`, which would move fabricated evidence out of the fabrication bucket. But
this is a judgment, not a derivation, and a reviewer could reasonably prefer the other. The
disagreement set is small and enumerable; recommend disclosing it rather than defending the
choice as forced.

### 6.3 The class is defined by regex over model prose, which is a moving target

Six patterns were derived from **one** arm's output on **one** corpus. Local produced 150
absence claims; the two cloud arms produced none — not because they never assert absence, but
because they handle it differently (openai leaves the snippet empty 121 times,
`MISSING_SNIPPET`; anthropic essentially never declines). A different model, or a re-run of
the same model, could phrase absence in a form none of the six patterns matches, and the
census would silently under-count without any signal that it had. There is no held-out
validation set. Mitigations in place: `absence_pattern` is persisted per span so drift is
auditable, and the rejected candidates are recorded. Mitigation *not* in place: nothing
detects a novel phrasing family. **If ABSENCE_CLAIM is used on any future run, the pattern
set must be re-derived and the delta reported, not assumed.**

### 6.4 The separation improves the headline, which is a reason for suspicion

CENSUS-01 reported no field-class signal; CENSUS-02 introduces a new class and the signal
appears. That sequence is exactly what motivated-reasoning looks like from the outside, and
the honest framing matters: the amendment was ratified on a *mechanism* (absence claims are
structurally concentrated on extractive fields, because only extractive fields can be absent),
and that mechanism predicts the direction of the change before the recount is run. The 9:1
extractive-to-interpretive concentration of absence claims in §2b is the check — it is a
property of the carved-out population, not of the residual, so it cannot have been produced
by the recount. Recommend reporting §2b alongside §3 in the manuscript, never §3 alone.

### 6.5 Three extractive fields still behave interpretively, and the class does not explain them

On local, `country` (28.1%), `primary_outcome_value` (25.7%) and `secondary_outcomes` (25.2%)
sit at `task_*`-level no-basis rates despite being extractive with the absence claims already
removed. Inspection of the §5b sample suggests a different mechanism for these — the arm
paraphrases a real number rather than quoting it ("The mean distance was 3.2538 mm…", "The
average of the RMS errors was 27.9 mm.") — i.e. a *numeric-restatement* failure, not an
absence and not an invention. If that is a third population, the taxonomy currently scores it
as no-textual-basis alongside genuine fabrication, which is the same conflation this amendment
just corrected one instance of. Flagging, not proposing: the fix design is out of scope.

### 6.6 Two spans in the corpus are not codebook fields at all

`provenance_classifications` contains one span with `field_name = 'Title'` (paper 415) and one
with `field_name = 'field_1'` (paper 719), both from the local arm, both `UNTRACEABLE_NO_BASIS`.
These are schema violations that leaked out of the extractor into `evidence_spans` and were
never caught by the extraction validator. They carry `field_class = ''` so they are excluded
from every extractive/interpretive aggregate in this report and cannot have affected the
verdict — but they are live data corruption in the Run 6 source table and should be raised
separately.

---

## Acceptance gates

| gate | status |
|---|---|
| 1. DEFINITIONS v1.1 complete, every pattern enumerated | ✅ §A2 lists all six with regex semantics, examples and corpus counts; §A4 records the two rejected candidates; §A5 the deliberate false negatives |
| 2. 100% coverage; migration additive; suite green | ✅ 11,017/11,017; migration 011 preserves all 11,017 prior rows with a row-count abort guard (rebuild required only because SQLite cannot alter a CHECK in place — stated in the migration docstring); 1350 passed / 15 deselected |
| 3. Headline answered yes/no with cross-tab + per-field table | ✅ §3 (yes pooled + 2 of 3 arms, no on anthropic), §2b cross-tab, §4 all 20 fields |
| 4. Detector validation with error counts | ✅ §5 — 25 ABSENCE_CLAIM: 0 FP; 25 residual no-basis: 0 FN in-sample; ~22 known out-of-sample FN, recall ≈86% |
| 5. Zero Ollama; no prompt/codebook/judge edits | ✅ `git show --stat fdbf076` touches only `analysis/provenance/`, `engine/migrations/011_*`, and two test files |

**Out of scope and not done:** schema/`NOT_FOUND` fix design, judge restatement, field
classification ratification (architect's call, now with §3 and §6 in hand), Arm P,
primer.md / CLAUDE.md edits.
