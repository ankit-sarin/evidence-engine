# ADJUD-01 — Adjudicating the schema-decision disagreement clause

Project: evidence-engine · Machine: DGX (spark-59e4) · Date: 2026-07-31
Repo at start: clean at **`92c70a5`** (the session-records preservation commit; parent `f5e28d8`).
Suite: **1456 passed, 15 deselected** (`-m "not network and not ollama and not integration"`).

**Read-only. Zero model API calls — local, cloud, or otherwise.** Every pair below was
adjudicated by reading the two value strings, and for the nine hard cases the two stored
source snippets as well. No prompt, codebook, schema, or production path was touched; nothing
was written to `review.db`.

---

## 0. The question

SCHEMA-EVAL-02's pre-registered rule returned **RETAIN_B**. Condition C — the required-slot
response contract — was the best condition on anchoring (38.6%), no-basis (23.4%), guard pass
(100.0% vs B's 85.0%), would-retries (0 vs 6), and completion tokens, and was rejected on a
**single clause**: C-vs-B value disagreement **11.4% > 10%**.

That clause was written to catch **substantive value change** — the premise being that a schema
*shape* should not change what the model *extracts*. The metric that implements it is exact match
on the normalized value string (`analyze_schema_eval2.py:108`), which cannot tell a changed fact
from a changed phrasing. This task determines which one the 11.4% is.

---

## 1. Enumeration — reproducing the clause exactly

`analysis/eval/adjud01_pairs.disagreement_pairs()` mirrors `analyze_schema_eval2.agreement()`
term for term: same paper intersection, same field intersection, same `_norm` comparison. It
reproduces the published figure exactly.

| | compared | disagree | rate |
|---|---:|---:|---:|
| **all fields** | **756** | **86** | **11.4%** ✓ matches SCHEMA-EVAL-02 §2 M3 |
| free_text (7 fields) | 301 | 76 | **25.2%** ✓ matches §3 |
| categorical (12 fields) | 417 | 8 | 1.9% |
| numeric (1 field: `sample_size`) | 38 | 2 | 5.3% |

Field types are read from `data/surgical_autonomy/extraction_codebook.yaml`, not hand-assigned.
The report's "categorical 2.2% (10/455)" is categorical **and** numeric pooled — `(8+2)/(417+38)`
— so this enumeration is consistent with it and slightly finer-grained.

Free-text disagreements by field (n=76): `key_limitation` 26, `secondary_outcomes` 13,
`robot_platform` 11, `task_performed` 8, `primary_outcome_value` 7, `primary_outcome_metric` 5,
`comparison_to_human` 4, `country` 2.

All 86 pairs are dumped to the gitignored
`data/surgical_autonomy/eval/schema_eval2/adjud01_all_pairs.json`.

### Sample

**Seed 20260731, n=60 of the 76 free-text pairs**, drawn with `random.Random(seed).sample()`
over a `pair_id`-sorted list so the draw does not depend on dict iteration order. Seed and n
were fixed in the brief before any pair was read. The 16 unsampled pairs are enumerated in the
same JSON and were **not** adjudicated.

---

## 2. Result

| label | n | share |
|---|---:|---:|
| **SAME_FACT** | **51** | **85.0%** |
| DIFFERENT_FACT | 6 | 10.0% |
| UNCLEAR | 3 | 5.0% |

### Pre-registered rule outcome

> if ≥80% of adjudicated pairs are SAME_FACT, the recommendation is ADOPT C with the override
> documented; if <80%, RETAIN B stands.

**85.0% ≥ 80% → the recommendation is ADOPT C, with the override documented.**

The margin is 3 pairs. The rule is insensitive to how UNCLEAR is treated — UNCLEAR is already
excluded from the numerator, so even scoring all three as DIFFERENT_FACT leaves 85.0%. To fall
below the threshold, **four** of the 51 SAME_FACT calls would have to be wrong in the same
direction.

### Where the real disagreement lives

| field | SAME | DIFF | UNCLEAR |
|---|---:|---:|---:|
| key_limitation | 18 | 1 | 1 |
| robot_platform | 8 | 0 | 1 |
| task_performed | 8 | 0 | 0 |
| primary_outcome_value | 5 | 0 | 0 |
| primary_outcome_metric | 4 | 0 | 0 |
| **secondary_outcomes** | **4** | **4** | **1** |
| comparison_to_human | 2 | 1 | 0 |
| country | 2 | 0 | 0 |

**Five of the nine non-SAME_FACT calls fall on `secondary_outcomes`**, and a sixth on
`comparison_to_human`. Splitting the sample on whether the field's answer is a **list of
metric/value items**:

| field shape | fields | n | SAME_FACT |
|---|---|---:|---:|
| list-shaped | `secondary_outcomes`, `comparison_to_human` | 12 | **50.0%** (6/12) |
| single-answer | the other six | 48 | **93.8%** (45/48) |

The signal is not "C changes what the model extracts"; it is "**multi-value list fields are
where the two contracts diverge**" — the arms differ on which items to list and on whether to
declare the unlisted ones absent.

`key_limitation` — 26 of the 76 disagreements, the single largest contributor to the 11.4% —
is **18/20 SAME_FACT**. It dominates the raw count and almost none of it is substantive.

---

## 3. The six DIFFERENT_FACT calls, quoted

**1. `445::secondary_outcomes`** — numeric conflict, not framing.
> **B:** `Leak pressure: STAR performed significantly better than all other techniques; Number of mistakes: Less than one mistake per sample; Completion time: Comparable to LAP and RAS.`
> **C:** `Leak pressure: 16.76 ± 4.08 mmHg; Mistakes: <2 per sample; Completion time: 50.0 ± 14.7 min`

"Less than one mistake per sample" and "<2 per sample" are different numbers. B also claims the
leak pressure was "significantly better than all other techniques" while C's own snippet reads
*"The burst pressure after STAR end-to-end ex vivo anastomosis was 16.76 ± 4.08 mmHg, which is
comparable to these other techniques."*

**2. `460::secondary_outcomes`** — different metric set from the same table.
> **B:** `Fragment false negative: 0.0%; Fragment false positive: 0.0%; Grasp failure: 3.6%; Multiple grasp: 5.2%`
> **C:** `Average number of replanning steps: 10.58; Fragment false negative (%): 0.0`

C introduces a metric B does not carry and drops three that B does. Overlap is one item of four.

**3. `549::comparison_to_human`** — different metric selected from a snippet containing both.
> **B:** `Autonomous: Path length reduced by about 50% vs Manual control`
> **C:** `Autonomous: 39.3s vs Manual: 45.7s (p=0.003)`

The shared snippet reads *"the path length of the remote controller and the total clutching
number can be reduced significantly by about 50%. The average task completion time is reduced
from 45.7 s to 39.3 s."* B took path length, C took completion time. C additionally attaches
`p=0.003`, which appears in neither snippet.

**4. `663::key_limitation`** — two unrelated limitations, with a crossover.
> **B:** `The system does not provide needle steering, which limits accuracy in tissue with variations in density causing needle deflections.`
> **C:** `Reliance on US imaging with lower resolution compared to other modalities`

Worth noting: C's snippet is *"Due to variations in tissue density, the needle deflects and bends
during perforation. The described system does not provide needle steering"* — which supports
**B's** value, not C's. Neither snippet supports C's value.

**5. `694::secondary_outcomes`** — positive claim against absence claim.
> **B:** `Success rate: Not explicitly stated; Confidence level: Improved with stiffness variation`
> **C:** `Success rate: NR; Confidence level: NR`

B's snippet (*"They mention success rates and confidence levels in nodule depth estimation"*)
does not support the improvement claim; C has no snippet at all. Whichever is correct, the two
cells assert opposite things about the same field.

**6. `764::secondary_outcomes`** — conflicting pose-error value.
> **B:** `Pose error: 6.44 ± 8.59 mm; Contact force: 0.99 ± 0.68 N`
> **C:** `pose error: within 9.00 mm; contact force: 0 . 99 ± 0 . 68 N`

B's own snippet says *"The average tracking errors along the X and Y axis are within 9.00 mm"* —
C's figure. `6.44 ± 8.59` appears in neither snippet. The contact force is identical modulo the
PDF's digit spacing.

---

## 4. The three UNCLEAR calls, quoted

**1. `386::secondary_outcomes`** — allocation across fields, not wording, and not a conflict.
> **B:** `Accuracy: 1.30 ± 0.44 mm; Accuracy: 1.54 ± 0.34 mm`
> **C:** `Accuracy: 1.54 ± 0.34 mm; Force: Not reported; Success rate: Not reported`

The shared snippet carries both numbers (*"1.30 ± 0.44 mm in the first test and 1.54 ± 0.34 mm in
the second test"*). On the companion `primary_outcome_value` field, B emitted both and C emitted
`1.30 ± 0.44 mm` alone — so across the pair of fields the two arms extracted the **same numbers**
and split them differently, and C added two explicit absence declarations. No numeric conflict;
also not pure wording variance.

**2. `466::key_limitation`** — shared head clause, diverging tails.
> **B:** `The system currently operates in controlled environments without real-time feedback.`
> **C:** `The approach works in controlled environments without considering unpredictable factors.`

The identical snippet reads *"Although our current results were obtained in controlled and
predictable environments, we are continuing our research with more generally setup experiments."*
C's tail restates the snippet; B's "without real-time feedback" is a separate deficit the snippet
does not mention. The limitation identified is the same; the elaborations are two different claims.

**3. `516::robot_platform`** — identical answer, incompatible qualifiers.
> **B:** `No physical platform — computational simulation of a 6-DOF gripper`
> **C:** `No physical platform — Algorithm on existing platform`

Both answer the field the same way. B's snippet supports simulation of a *"generic 6-DOF
gripper"*; C's qualifier implies an existing platform was used, which its snippet does not
support.

---

## 5. Five SAME_FACT pairs, so the wording-variance claim is inspectable

**1. `763::country`** — the whole disagreement.
> **B:** `United States`  **C:** `USA`

**2. `463::robot_platform`**
> **B:** `Raven II surgical system`  **C:** `Raven II`

**3. `750::key_limitation`**
> **B:** `Only one cadaver was tested, which is a significant limitation.`
> **C:** `Only one cadaver was tested.`

**4. `668::secondary_outcomes`** — three metrics, seven numbers, all identical; one plural.
> **B:** `motor torques: no significant difference; travel path distances: left arm 2.02 m, right arm 1.73 m (no latency); endoscope camera motion: 108.2 mm (no latency), 44.9 mm (simulated latency), 37.6 mm (ISS latency)`
> **C:** `motor torques: no significant difference; travel path distance: left arm 2.02 m, right arm 1.73 m (no latency); endoscope camera motion: 108.2 mm (no latency), 44.9 mm (simulated latency), 37.6 mm (ISS latency)`

**5. `463::key_limitation`** — tense.
> **B:** `The main limitation was tissue approximation and robot flexibility affecting results.`
> **C:** `The main limitation is the tissue approximation and robot flexibility affecting results.`

Two more of the same kind, for scale: `574::task_performed` differs only in the capital B of
"Bi-manual" and a dropped comma before "including"; `764::key_limitation` differs only in
"affecting **the** results" vs "affecting results".

Distribution of the variance tags across the 51 SAME_FACT pairs: **granularity 22, wording 16,
formatting 8, abbreviation 5** (descriptive only; the rule does not use them).

---

## 6. Answer to the question SCHEMA-EVAL-02 put to the architect

> *Was the disagreement clause intended to catch substantive value change, or any string
> difference?*

The 11.4% **is not substantive value change**. On the adjudicated sample, 85% of the free-text
disagreements — which are 76 of the 86 total — are the same fact in different words, and the
categorical fields the clause was really protecting agree at **97.8%** (`(417-8+38-2)/455`).
The clause as implemented measures string difference; the 10% threshold sits inside the model's
own wording-variance band, which SCHEMA-EVAL-02 already showed by measuring **A vs B at 9.2%**
for two conditions differing only in whether a schema is attached.

**Recommendation: ADOPT C, override documented, per the rule pinned in the ADJUD-01 brief.**
The clause fired on wording variance, not value change; C wins on every measure the study set
out to weigh, and it is the only one of the three contracts that can express cardinality —
which is the defect that produced SPANLOSS-01.

**One caveat that should travel with the adoption.** The substantive disagreement that *does*
exist is concentrated in the list-shaped fields — `secondary_outcomes` (5 of 9 sampled pairs
non-SAME_FACT) and `comparison_to_human`. Two mechanisms are visible in the quoted pairs: the
arms disagree on **which items** to list (`460`, `445`, `549`), and they disagree on whether an
unlisted item should be **declared absent** (`694`, `386`). The second is exactly the
`NOT_FOUND` escape-value problem SCHEMA-EVAL-02 §5 said should land on whichever contract wins.
This adjudication is further evidence that it should, and that it should be scoped to the
list-shaped fields specifically rather than uniformly.

---

## 7. Scope and standing

**This adjudication is engineering telemetry for a contract decision.** It is one adjudicator,
one seeded sample of 60, labels applied by reading strings — it is not a concordance
instrument. Paper-grade concordance continues to use the pre-registered pipeline instruments
(the provenance ladder, the Pass 1/Pass 2 judge, the PI audit), not this read. Nothing here
should be cited as an agreement statistic.

Three further limits, stated rather than smoothed:

- **The adjudicator wrote the labels and benefits from the outcome.** The mitigations are that
  the 80% rule and the seed were pinned in the brief before any pair was read, that all nine
  non-SAME_FACT calls are quoted in full above, and that the sample and all 86 pairs are
  reproducible from the committed script. There is no second rater.
- **`granularity` counts as SAME_FACT by the brief's definition**, and it is the largest tag
  (22 of 51). A stricter reading that treated "one arm dropped a detail the other kept" as a
  different value would change the outcome. That reading was not the one pinned.
- **The sample is 60 of 76 free-text pairs**; the 16 unsampled ones are enumerated but
  unjudged. Their surface appearance is consistent with the sample (e.g. `764::robot_platform`
  is `Virtuose 6d (Haption)` vs `Virtuose 6d`), but that is an impression, not a measurement.

---

## Acceptance gates

| gate | status |
|---|---|
| 1. All C-vs-B pairs enumerated; sample seed stated | ✅ §1 — 86 pairs, 11.4% reproduced exactly; seed **20260731**, n=60 of 76 free-text |
| 2. Every pair classified SAME_FACT / DIFFERENT_FACT / UNCLEAR | ✅ `analysis/eval/adjud01_labels.json`, 60/60 labelled |
| 3. Every DIFFERENT_FACT and UNCLEAR call quoted | ✅ §3 (6) and §4 (3), both strings quoted, snippets where they decide the call |
| 4. Rule outcome stated against the threshold | ✅ §2 — **85.0% ≥ 80% → ADOPT C with documented override** |
| 5. Read-only, zero model API calls | ✅ no network, no Ollama, no cloud; `ollama ps` unchanged; experiment lock never taken |
| 6. Script + summary committed, raw outputs gitignored | ✅ script + labels + this report committed; `adjud01_{all_pairs,sample,summary}.json` under gitignored `data/` |

**Out of scope and not done:** implementing the contract switch (no production path touched, B
remains the production contract until the architect rules), the runtime-version A/B for the
19.4 pp restoration gap, `NOT_FOUND` escape values, re-extraction, Run 7.
