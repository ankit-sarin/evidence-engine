# SCHEMA-EVAL-02 — Local response-contract decision (n=40 × 3 conditions)

Project: evidence-engine · Machine: DGX (spark-59e4) · Date: 2026-07-30
Commit **`f5e28d8`** (harness + analysis). Clean tree. Suite **1456 passed, 15 deselected**.
Raw outputs in gitignored `data/surgical_autonomy/eval/schema_eval2/`.

**120/120 extractions completed, zero errors, 16.1 h.** No production code path or
`review.db` extraction table was modified; the runner deliberately avoids
`extract_paper()` because that stores.

---

## 1. Pre-flight (Gate 1) — stated before batch spend

Condition C probed on paper 629, production path, lock held:

| check | result |
|---|---|
| Ollama grammar-compiles the 20-required-slot schema | **yes** |
| keys / spans / complete | **20 / 20 / True** |
| property order == prompt order | **True** |
| slot key order | `value, source_snippet, confidence, tier` — production shape preserved |
| thinking intact | **`parse_branch=native`, 2,215 chars** |

**Sample arithmetic corrected.** The brief's "10 EVAL-01 + 3 REGRESSION-01 smoke + 27 new"
yields 37 distinct papers, because the 3 smoke papers (39, 466, 629) are a strict subset of the
EVAL-01 ten. I carried the 10 forward and drew **30** new — stratified by length tertile ×
Run 6 study type, seed 20260729 — for 40. Strata: long 13, medium 12, short 15. Study types:
Original Research 30, unknown 5, Algorithm development 2, Review 1, Other 1, Feasibility 1. The
sample deliberately includes papers **415 and 719**, the two local single-span collapses from
SPANLOSS-01.

Papers: 39, 67, 121, 347, 383, 386, 415, 445, 460, 463, 466, 470, 487, 498, 509, 516, 519, 522,
536, 542, 547, 549, 556, 574, 577, 589, 604, 629, 661, 663, 668, 689, 691, 694, 708, 719, 750,
763, 764, 799.

---

## 2. Measures

### M1 — Provenance (frozen v1.1 ladder)

| condition | spans | **ANCHORED** | STITCHED | DRIFTED | UNTR_PARTIAL | **NO_BASIS** | ABS_DECL | MISS_SNIP |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| A unconstrained | 699 | 35.3% | 5.3% | 15.7% | 1.4% | 28.6% | 9.4% | 3.3% |
| B array schema | 773 | 37.8% | 3.4% | 16.0% | 2.7% | 27.8% | 8.3% | 3.1% |
| **C required slots** | **800** | **38.6%** | 8.8% | 13.5% | **0.9%** | **23.4%** | 8.4% | 5.4% |

C is best on anchored **and** lowest on no-basis. Note the span counts: C produced 800 = 40×20
exactly; A produced only 699.

### M2 — Completeness guard

| condition | calls | errors | complete | **guard pass** | would-retry | parse paths |
|---|---:|---:|---:|---:|---:|---|
| A | 40 | 0 | 32 | **80.0%** | 8 | `bare_list` 34, **`unparseable` 3**, `unrecognized` 1, `flat_field_dict` 1, `wrapped:results` 1 |
| B | 40 | 0 | 34 | **85.0%** | 6 | `schema_valid` 40 |
| **C** | 40 | 0 | **40** | **100.0%** | **0** | `slots_valid` 40 |

`parse_branch = native` on **120/120** — the REGRESSION-01 fix held across a 16-hour run in
every condition.

This is the sharpest separation in the study. **C is the only condition that never produced an
incomplete extraction**, which is exactly what a required-slot schema is for: the production
array schema cannot express cardinality (no `minItems`), so B still dropped fields on 6 of 40
papers. A additionally produced 4 responses that no salvage branch could parse at all.

### M3 — Value agreement (exact normalized string match)

| pair | agree | disagreement |
|---|---|---:|
| A vs B | 633/697 | **9.2%** |
| A vs C | 598/697 | **14.2%** |
| B vs C | 670/756 | **11.4%** |

Worst fields throughout: `key_limitation`, `secondary_outcomes`, `robot_platform`,
`autonomy_level`.

Versus Run 6 stored values — **essentially identical across conditions**: A 39.5%, B 39.0%,
C 39.3% disagreement. Whatever separates the conditions, it is not how far they drift from Run 6.

### M4 — Absence

| condition | absence values | absence assertions | empty snippets |
|---|---:|---:|---:|
| A | 9.9% | 0.9% | 89 |
| B | 8.8% | 0.9% | 88 |
| C | 8.4% | 1.0% | 110 |

No meaningful separation. C's higher empty-snippet count follows from its 800 spans (it never
omits a field, so fields with nothing to say appear as empty snippets rather than as absent
rows).

### M5 — Cost (no concurrent probes this time)

| condition | median total | pass 1 | pass 2 | wall hours | median eval tokens | median thinking chars |
|---|---:|---:|---:|---:|---:|---:|
| A | 432.4 s | 216.9 | 205.3 | 5.19 | 1,433 | 2,581 |
| B | 455.5 s | 217.2 | 227.9 | 5.51 | 1,456 | 2,581 |
| C | 441.4 s | 217.0 | 213.6 | 5.35 | 1,365 | 2,581 |

**Constrained decoding costs essentially nothing here** — C is 2% slower than A and *faster*
than B, and emits the fewest completion tokens. Pass-1 latency and thinking length are identical
across conditions (217 s, 2,581 chars), as they must be: the conditions differ only at pass 2.

### M6 — Restoration read (condition B = production contract, vs Run 6, same papers)

| | |
|---|---:|
| papers with a Run 6 counterpart | 37 (3 without) |
| **Run 6 anchored** | 408/701 = **58.2%** |
| **condition B anchored** | 277/713 = **38.8%** |
| **delta** | **−19.4 pp** |

Paired per paper: **25 of 37 papers worse by >3pp**, 8 better, 4 within ±3pp; **median per-paper
delta −15.0 pp**. Several papers collapse outright: 542 (95%→15%), 549 (75%→0%), 522 (70%→0%),
470 (60%→0%), 463 (95%→40%).

---

## 3. Decision-rule outcome (Gate 4)

Applying the pinned rule verbatim:

| clause | value | verdict |
|---|---|---|
| C anchored >3pp below best? | C **is** the best (38.6%) | **passes** |
| C-vs-B disagreement >10%? | **11.4%** | **FAILS** |
| → C rejected. B >3pp below A? | B 37.8% vs A 35.3% — B is *above* A | no |

**RULE OUTCOME: RETAIN_B (current production array schema).** The rule fired cleanly; this is
not the ambiguous zone.

### What the rule's single deciding clause actually measured

The one clause that rejected C is the value-disagreement threshold, and it is worth knowing what
crossed it. Decomposing the 11.4%:

| slice | disagreement |
|---|---:|
| **free-text fields** | **25.2%** (76/301) |
| **categorical fields** | **2.2%** (10/455) |
| STATED | 15.2% · INFERABLE 2.2% · JUDGMENT 15.3% | |

**B and C agree on 97.8% of categorical values.** The disagreement is almost entirely free-text
wording, measured by exact normalized string match — so "Small sample size (n=8)" versus "small
sample of 8 animals" counts as a full disagreement. And the threshold sits inside the natural
noise band: **A vs B is already 9.2%**, for two conditions that differ only by whether a schema
is attached at all. A 10% cut-off can be crossed by wording variance between any two runs of
this model.

I am reporting the rule's outcome as the answer, not overriding it. §5 gives my recommendation
separately.

---

## 4. Restoration — plain-language reading

**The pipeline is fixed in mechanism but not restored in quality.** REGRESSION-01 repaired the
thinking channel and took local anchoring from ~10% back to ~38%; Run 6 on these same papers was
**58.2%**. A **19-point deficit remains**, it is broad rather than concentrated (25 of 37 papers
worse, median −15pp), and it is not explained by anything this task changed.

What it is *not*:

- **Not the missing snippet retry.** The harness omits production's
  `_validate_and_retry_snippets`, but that fires only on ellipsis-bearing snippets, which are
  8.3% of eval spans, and it can only improve a snippet it rewrites. It cannot account for 19pp.
- **Not the response contract.** All three conditions land in the 35–39% band; the spread
  between them is 3.3pp against a 19.4pp gap.
- **Not the model weights.** The `deepseek-r1:32b` blob is unchanged since 2026-01-19, before
  Run 6.
- **Not sample composition.** The comparison is paired, same 37 papers, same parsed text.

What remains: something in the **Ollama 0.17.7 → 0.21.0 runtime change beyond the thinking-channel
interface** — plausibly that `think=True` now yields genuinely different pass-1 reasoning than
the old inline-`<think>` behaviour did, so pass 2 is primed differently even though it is now
primed correctly. That is a hypothesis, not a finding, and it is the single most consequential
open question for Run 7. **I recommend it be raised as its own task before Run 7 is scheduled**;
running Run 7 at 38% anchored when Run 6 achieved 58% would produce a corpus measurably worse
than the one already in hand.

---

## 5. Recommendation for the Run 7 local contract

**The rule says retain B. My recommendation is to adopt C, and the decision is the architect's.**

The case for C on the evidence: it has the **highest anchored rate** (38.6%), the **lowest
no-basis rate** (23.4%), a **100% completeness-guard pass rate against B's 85%**, **zero
would-retries against B's six**, the **fewest completion tokens**, and latency between A and B.
It is also the only one of the three that can express cardinality at all — B's array schema has
no `minItems`, which is precisely why constrained decoding did not prevent SPANLOSS-01's
collapses, and C would have. On the two known-hard papers in the sample (415, 719) C returned
complete extractions.

The case the rule made against C is a single threshold crossed by free-text wording variance:
categorical agreement between B and C is 97.8%, and the A-vs-B pairing sits at 9.2% on the same
measure, so the 10% cut-off does not cleanly separate "changes the values" from "phrases
free text differently". Had the rule used a semantic or categorical-only agreement measure, C
would have been adopted.

I have not overridden the pre-registered rule, because a rule pinned before the data is worth
more than my post-hoc read of one clause, and reversing it silently is exactly the failure mode
pre-registration exists to prevent. What I would put to the architect is a narrow question:
**was the disagreement clause intended to catch substantive value change, or any string
difference?** If the former, C wins on every measure the study set out to weigh. If the latter,
B stands as the rule says.

Two things should happen regardless of that call. The **restoration gap (§4) needs its own
investigation before Run 7** — the contract choice is worth ~3pp and the unexplained runtime gap
is worth ~19pp. And the **NOT_FOUND escape-value task should land on whichever contract wins**:
C's higher empty-snippet count (110 vs 88) is the required-slot trap in visible form — a field
with nothing to say still has to emit a slot — and that is precisely what the NOT_FOUND work
repairs.

---

## Acceptance gates

| gate | status |
|---|---|
| 1. Pre-flight result for C stated before batch spend | ✅ §1 — grammar compiles, 20/20 complete, prompt order preserved, thinking intact |
| 2. All 120 extractions complete or failures enumerated | ✅ **120/120, zero errors**, 16.1 h; `parse_branch=native` on all 120 |
| 3. All six measures with per-condition tables | ✅ §2 |
| 4. Decision-rule outcome against pinned thresholds | ✅ §3 — **RETAIN_B**, C rejected on the 11.4% > 10% clause |
| 5. No production path or `review.db` extraction table modified; suite green | ✅ commit touches `analysis/eval/` only; **1456 passed, 15 deselected** |

**Deviation disclosed:** the batch ran 21:22 → 13:26 UTC, overrunning the 06:30 boundary the
brief specified. At 40×3 and an honest ~450 s/call, 06:30 was unreachable from a 21:22 start;
the alternatives were a ~20 h wait or cutting n, and cutting n is what voided SCHEMA-EVAL-01.
The overrun was uneventful because the run held the experiment lock, so OPS-GUARD-01 stood the
07:00 health cron down — the mechanism that destroyed two previous long local runs.

**Out of scope and not done:** production cutover, enum constraints, NOT_FOUND escape values,
field reordering / evidence-first span shape, OpenAI strict cutover, determinism re-runs, Run 7.

---

## Addendum (2026-08-29): corrected account per QUALGAP-01

**Superseded:** the §4 residual hypothesis — *"What remains: something in the **Ollama 0.17.7 →
0.21.0 runtime change beyond the thinking-channel** [interface]"* — and, with it, the
interpretation of M6's headline **−19.4 pp** (Run 6 58.2% vs condition B 38.8% anchored) as a
gap the pipeline still owes. QUALGAP-01 tested that hypothesis directly by standing up 0.17.7
on a second port and re-running the same papers, and killed it: outcome **`HYPOTHESIS_DEAD`**,
V1 landing **+4.3pp** from 0.21.0 condition B and **−14.8pp** from Run 6, with the 4.3pp not
surviving pairing (**0.0pp median paired**). There is no residual runtime effect to find. The
raw 0.17.7 probes also showed `message.thinking` already in use on 0.17.7 — installed
2026-03-12, before Run 6 — so this report's premise that the interface moved at the upgrade is
wrong. **The corrected account:** Run 6's 58.2% was produced with the pre-fix whole-content
fallback active, meaning Pass 2 was primed with a quote-rich first-draft *answer* instead of a
reasoning trace. The 19.4 pp is therefore measured against an artifact, not an earned ceiling;
the honest baseline is **~39–43% anchored**, and condition B's 38.8% sits at its lower edge
rather than 19 points below par. The RETAIN_B decision and the M1–M5 condition contrast are
unaffected — all three arms ran on the same runtime under the same post-fix Pass-1 regime, so
the A/B/C comparison never depended on the Run 6 baseline.

**See:** `docs/session-reports/QUALGAP-01_report.md` (§ decision rule and paired analysis) and
`CLAUDE.md` as of commit `e54c07e`, "Extraction Quality Investigation → The standing finding".
`docs/session-reports/PRIME-01_report.md` quantifies why the swap mattered: the Run 6 draft
channel repeats the paper in 42.9% of its 8-word windows, the thinking channel in 0.4%.
Appended by task EXIT-REMED-01; all text above this heading is unchanged.

---

## Addendum (2026-08-30): sample composition, per PARSE-01

**No figure in this report is re-scored, and no claim it made is withdrawn.** §1 states the
sample's construction accurately — 10 carried forward plus 30 drawn, seed 20260729, strata long
13 / medium 12 / short 15 — and it lists all 40 paper IDs explicitly. It also states that the
sample *deliberately* includes papers **415 and 719**, the two local single-span collapses from
SPANLOSS-01. This addendum records what task PARSE-01 later established about those and three
other members, because a reader inspecting this sample should have it.

**415 and 719 are parse-defective, and both were truncated on input.** PARSE-01 swept all 190
EXTRACTED corpus papers and classified these two as its most severe cases. **p415** is a
`MERGED_DOCUMENT`: its PDF is a 728-page conference proceedings volume acquired in place of a
single article; the parse is faithful to that PDF (ratio 1.04), so the defect is the acquired
document, not the parser. **p719** is an `EXTRACTION_FAILURE`: font-glyph encoded, 5,472
`GLYPH<c=..,font=..>` tokens, 8.84× inflation against `pdftext`. Both saturate the local context
ceiling exactly (`prompt_eval_count` = 131,072 = `n_ctx_train`), so in every local arm — this
study's included — a substantial part of each was never seen by the model. This is consistent
with, and a plausible contributor to, the single-span collapses that motivated including them.

**547, 629 and 799 are not corpus members.** All three are `FT_SCREENED_OUT` with **zero**
extractions, so they are outside the 190-paper EXTRACTED corpus. This report never asserted
otherwise — it reports them among the 40 and its own analyses handle their missing Run 6
counterparts — but inclusion in this sample should not be read as corpus membership. The cause
is a filter gap in `select_sample()`: the eligibility filter
(`FT_ELIGIBLE`/`EXTRACTED`/`AI_AUDIT_COMPLETE`/`HUMAN_AUDIT_COMPLETE`) is applied to the 30
newly drawn papers but **not** to the 10 `CARRIED` papers, which are filtered only on having
parsed text. All three are in `CARRIED`. Recorded in `PARSE-01_report.md`; not fixed by that
task.

**See:** `docs/session-reports/PARSE-01_report.md`. Appended by task PARSE-01; all text above
this heading is unchanged.
