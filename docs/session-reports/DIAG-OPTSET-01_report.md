# DIAG-OPTSET-01 — Production option-set audit (categorical fields)

Project: evidence-engine · Machine: DGX (spark-59e4) · Date: 2026-07-27
Scope: diagnosis only. No prompt / codebook / code changes, no DB writes, no commits.

## Arm P rerun state — ACCEPTANCE GATE 3

`tmux d0s2_armP_rerun` was checked once at task start: session alive, pane not dead,
child process `PID 3291661 .venv/bin/python -m probes.cache_arms --arm P --n 15 --label armPrerun`
in state `S+` with 21:40 elapsed, log tail mid-run (`filler 2: paper 14`). **The rerun was
still in flight**, so the hard zero-Ollama constraint applied.

> **Ollama call count for this task: 0.** No inference, no `/api/ps`, no `/api/tags`, no
> `ollama` CLI. All work was file reads, offline Python string assembly (`build_extraction_prompt`
> with a placeholder paper body), openpyxl reads, read-only SQLite, and reuse of the
> DIAG-UNANCHOR-01 census JSONs. No Arm P results were read or analysed.

---

## 1. Categorical/ordinal field inventory

`data/surgical_autonomy/extraction_codebook.yaml` defines **20 fields, of which 11 are
`type: categorical`**. Five carry `judge_rubric_family: ordinal` and a redundant
`ordered_values:` list; six are `nominal_categorical`. No field is typed `ordinal` directly.

| # | field | codebook line | tier | rubric family | options | `ordered_values` present | `source_quote_required` |
|---|---|---:|---:|---|---:|---|---|
| 1 | study_type | 30 (values 40) | 1 | nominal_categorical | 8 | — | no |
| 2 | surgical_domain | 104 (values 115) | 1 | nominal_categorical | 10 | — | no |
| 3 | autonomy_level | 146 (values 169) | 2 | ordinal | 8 | yes (149) | no |
| 4 | validation_setting | 203 (values 222) | 2 | ordinal | 8 | yes (206) | no |
| 5 | task_monitor | 249 (values 257) | 2 | nominal_categorical | 4 | — | no |
| 6 | task_generate | 274 (values 283) | 2 | nominal_categorical | 4 | — | no |
| 7 | task_select | 300 (values 309) | 2 | nominal_categorical | 4 | — | no |
| 8 | task_execute | 326 (values 334) | 2 | nominal_categorical | 4 | — | no |
| 9 | system_maturity | 351 (values 369) | 2 | ordinal | 6 | yes (354) | no |
| 10 | study_design | 391 (values 403) | 2 | nominal_categorical | 10 | — | no |
| 11 | clinical_readiness_assessment | 512 (values 530) | 4 | ordinal | 5 | yes (515) | **yes (571)** |

Every option in every field carries a non-empty `definition:` string in the codebook, and
`ordered_values` is byte-identical to the `valid_values` value list for all five ordinals
(checked programmatically — zero missing, zero extra, same order). `clinical_readiness_assessment`
is the only categorical field with `source_quote_required: true`; the only other such field in
the codebook is the free-text `key_limitation` (line 510).

---

## 2. Rendered prompt vs. codebook — ACCEPTANCE GATE 1 (part 1)

The prompt was rendered offline via `build_extraction_prompt(paper_text, spec, codebook_path=…)`
with a placeholder body (`extractor.py:105-180`). Option sets reach the model through
`_build_field_block()` (`extractor.py:65-72`) as `(allowed values: A, B, C)` on the field header
line, plus a `**Value definitions:**` block (`extractor.py:79-82`).

**Result: zero disagreement.** For all 11 fields the rendered option list is identical to
`valid_values` in membership, wording, and order; all 73 options carry their codebook definition
in the prompt; the per-field `*Source quote required for this field.*` marker fires on
`clinical_readiness_assessment` and on no other categorical field. Missing options: 0. Extra
options: 0. Reworded labels: 0. Missing definitions: 0. Ordering differences: 0.

A fourth source was checked for completeness: `review_specs/surgical_autonomy_v1.yaml`
`enum_values` (used by `normalize.py` and the extraction validator, **not** by the prompt).
It is also identical to the codebook for all 11 fields. So codebook ≡ spec ≡ rendered prompt.

**The option sets are not the defect.** What follows are the disagreements that *do* exist —
all of them between the model-facing artifacts and the human workbook, or internal to the
prompt's own instruction footer.

---

## 3. Three-way comparison: codebook vs. rendered prompt vs. human workbook

Workbook: `data/surgical_autonomy/Extraction_Workbook_v2_A.xlsx`, sheets `Extraction Form`
(dropdown `dataValidation` list formulas, 11 of them, one per categorical field) and `Codebook`
(col E `Valid Values`, col G `Source Quote Required`).

| field | codebook | prompt | workbook dropdown (cell range) | verdict |
|---|---|---|---|---|
| study_type | 8 | 8 | 8 · `E3:E72` | identical |
| surgical_domain | 10 | 10 | 10 · `I3:I72` | identical membership; **single-select vs. multi-value** (§3.2) |
| autonomy_level | 8 | 8 | 8 · `J3:J72` | **LABELS DIFFER** (§3.1) |
| validation_setting | 8 | 8 | 8 · `K3:K72` | identical membership; **single-select vs. multi-value** (§3.2) |
| task_monitor | 4 | 4 | 4 · `L3:L72` | identical |
| task_generate | 4 | 4 | 4 · `M3:M72` | identical |
| task_select | 4 | 4 | 4 · `N3:N72` | identical |
| task_execute | 4 | 4 | 4 · `O3:O72` | identical |
| system_maturity | 6 | 6 | 6 · `P3:P72` | identical |
| study_design | 10 | 10 | 10 · `Q3:Q72` | identical |
| clinical_readiness_assessment | 5 | 5 | 5 · `X3:X72` | identical |

The workbook `Codebook` sheet col E agrees with the dropdown formula for all 11 fields (no
internal workbook drift), and col G (`No` ×18, `MANDATORY` ×2) agrees with
`source_quote_required` in the codebook.

### 3.1 `autonomy_level` — the one label disagreement (verbatim side-by-side)

| source | file / cell | verbatim option list |
|---|---|---|
| codebook | `extraction_codebook.yaml:169-176` (`valid_values`) and `:149-157` (`ordered_values`) | `0 (No autonomy)`, `1 (Robot assistance)`, `2 (Task autonomy)`, `3 (Conditional autonomy)`, `4 (High autonomy)`, `5 (Full autonomy)`, `Mixed/Multiple`, `NR` |
| rendered prompt | `extractor.py:68` header, rendered as `(allowed values: …)` | `0 (No autonomy), 1 (Robot assistance), 2 (Task autonomy), 3 (Conditional autonomy), 4 (High autonomy), 5 (Full autonomy), Mixed/Multiple, NR` |
| human workbook | `Extraction Form!J3:J72` dataValidation `formula1`; same string in `Codebook!E7` | `0,1,2,3,4,5,Mixed/Multiple,NR` |

Six of eight labels differ. A human types `2`; a model emits `2 (Task autonomy)`. This is
**mitigated downstream but not eliminated**: `_build_prefix_map()` in
`engine/analysis/normalize.py:20-38` maps the bare prefix to the parenthetical form for
concordance scoring, so κ is unaffected. It is *not* applied to raw stored values, so any
consumer reading `human_extractions.value` against `evidence_spans.value` without normalization
— including anything that shows raw arm values to a judge — sees a spurious mismatch. Note also
that 8 Run 6 model spans stored the bare `'2'`, 2 stored `'3'`, 2 stored `'4'` (§4.4): the
human-style label leaks into the model arm too.

### 3.2 Multi-value: the prompt permits what the workbook cannot express

`extractor.py:170` (verbatim, global instruction block):

> - For **validation_setting** and **surgical_domain**: if multiple categories apply, list all
> separated by semicolons (e.g., "In vivo (animal); Phantom/Simulation"). Each value must
> exactly match one allowed value.

`normalize.py:10` mirrors this (`_MULTI_VALUE_FIELDS = {"validation_setting", "surgical_domain",
"secondary_outcomes"}`). The workbook offers `K3:K72` and `I3:I72` as **Excel list validations —
single-select**; there is no multi-select control, no "select all that apply" instruction
anywhere in the `Instructions` sheet, and the `Codebook` sheet says only "Select most advanced if
multiple (human > animal > cadaver > ex vivo > phantom > simulation)" for validation_setting
(`Codebook!D8`) — i.e. it instructs humans to **collapse** exactly where the prompt instructs
models to **enumerate**.

This is not hypothetical. Semicolon multi-values in Run 6:

| field | anthropic | local | openai |
|---|---|---|---|
| validation_setting | 57/190 (30.0%) | 48/188 (25.5%) | 44/172 (25.6%) |
| surgical_domain | 6/190 | 1/188 | 0/172 |

**On roughly a quarter of papers, the model arm emits a validation_setting value the human arm
is structurally incapable of producing.**

### 3.3 Absence: the prompt contradicts its own option sets

Two clauses of the same instruction block:

- `extractor.py:168`: "If the field is not found in the paper, set to **"NOT_FOUND"**."
- `extractor.py:172`: "For all categorical fields: use **ONLY the exact allowed values** listed.
  Do not paraphrase, abbreviate, or combine them."

`NOT_FOUND` is not an allowed value of any of the 11 fields. Six fields (`autonomy_level`,
`validation_setting`, `task_monitor`, `task_generate`, `task_select`, `task_execute`) carry `NR`
as an explicit option, so the model has a legal escape there. **Five do not**: `study_type`,
`surgical_domain`, `system_maturity`, `study_design`, `clinical_readiness_assessment`. On those
five, a model that cannot find the information is instructed to do two mutually exclusive things.
Observed effect is small but non-zero — one stored `NOT_FOUND` each on study_type, system_maturity,
study_design, clinical_readiness_assessment (4 spans total).

The human side has the mirror-image gap. `Instructions!A15` (verbatim):

> • If a field cannot be determined from the paper, enter **NR (Not Reported)**.

`NR` appears in only 6 of the 11 dropdowns — the same six. On the other five a human following
the instruction literally cannot enter `NR`, because Excel list validation rejects it.
`study_type` / `surgical_domain` / `study_design` offer `Other`; `clinical_readiness_assessment`
offers `Not assessable`; **`system_maturity` offers neither** and is the one field where neither
population has any way to express "not determinable".

### 3.4 Coverage of the evidence obligation (carried over from DIAG-UNANCHOR-01)

Restated here because it is an option-set-adjacent asymmetry: the prompt requires a snippet for
all 20 fields; the workbook has quote columns (`Y`, `Z`) for 2 fields only, and
`Instructions!A27-A29` tells humans "**No source quotes needed**" for all Tier 1–3 fields — i.e.
for 10 of the 11 categorical fields. `clinical_readiness_assessment` is the sole categorical
field where both populations owe evidence, and there both say the same thing.

---

## 4. Untraceable spans by field type and arm — ACCEPTANCE GATE 2

Source: the DIAG-UNANCHOR-01 census (`grep_verify()` semantics — normalized exact substring, else
sliding word-window `SequenceMatcher > 0.85`, against `parsed_text/{paper_id}_v*.md`). Reused
read-only; nothing recomputed against the DB. Denominator throughout is **spans with a non-empty
snippet**; spans with an empty snippet are excluded (412 of 11,017 total).

### 4.1 Primary cross-tab

| arm | field type | spans w/ snippet | untraceable | rate | share of that arm's untraceable |
|---|---|---:|---:|---:|---:|
| local_deepseek_r1_32b | **categorical** | 2043 | 452 | 22.1% | **54.6%** |
| local_deepseek_r1_32b | free-text/numeric | 1576 | 376 | 23.9% | 45.4% |
| openai_o4_mini_high | **categorical** | 1880 | 101 | 5.4% | **52.1%** |
| openai_o4_mini_high | free-text/numeric | 1329 | 93 | 7.0% | 47.9% |
| anthropic_sonnet_4_6 | **categorical** | 2090 | 842 | 40.3% | **63.9%** |
| anthropic_sonnet_4_6 | free-text/numeric | 1687 | 475 | 28.2% | 36.1% |
| **pooled** | categorical | 6013 | 1395 | **23.2%** | — |
| **pooled** | free-text/numeric | 4592 | 944 | **20.6%** | — |

**Direct answer to the contract question: 54.6% of local-arm untraceable spans sit on
categorical fields** (452 of 828). But the honest reading is that this is close to the base rate
— categorical fields are 56.4% of local's snippet-bearing spans (2043/3619), so the *share* is
essentially proportional and the *rate* is actually marginally lower on categorical (22.1%) than
on free-text (23.9%). **There is no categorical concentration in the raw untraceable count for
the local arm.** The only arm showing a genuine categorical excess is anthropic (40.3% vs 28.2%).

### 4.2 Where the categorical concentration actually is

The concentration appears once you split by *kind* of untraceability (census buckets from
DIAG-UNANCHOR-01: A = every sentence traceable, i.e. stitched/elided real text; C/C1 = no
sentence traceable at all):

| arm | untraceable | of which bucket C/C1 ("no real text at all") | of those, categorical |
|---|---:|---:|---:|
| local_deepseek_r1_32b | 828 | 628 (75.8%) | 349 (55.6%) |
| openai_o4_mini_high | 194 | 14 (7.2%) | 5 (35.7%) |
| anthropic_sonnet_4_6 | 1317 | 54 (4.1%) | 22 (40.7%) |

So: the local arm owns 628 of the 696 spans (90.2%) where the snippet has no textual basis at
all, and 349 of those (55.6%) are categorical. Anthropic's 842 untraceable categorical spans are
almost entirely bucket A — real sentences, stitched, 81% of them with a visible `...`.

### 4.3 Per-field untraceable counts (categorical only, all arms)

| field | untraceable | denom | rate | local share |
|---|---:|---:|---:|---:|
| clinical_readiness_assessment | 167 | 548 | 30.5% | 40 |
| task_generate | 160 | 545 | 29.4% | 55 |
| task_monitor | 156 | 539 | 28.9% | 50 |
| task_select | 143 | 536 | 26.7% | 62 |
| system_maturity | 138 | 548 | 25.2% | 41 |
| study_design | 134 | 548 | 24.5% | 41 |
| validation_setting | 128 | 545 | 23.5% | 34 |
| task_execute | 126 | 546 | 23.1% | 61 |
| autonomy_level | 124 | 548 | 22.6% | 31 |
| surgical_domain | 79 | 544 | 14.5% | 25 |
| study_type | 40 | 566 | **7.1%** | 12 |

The gradient is interpretable: `study_type` (7.1%) is the one categorical field whose coded value
is routinely stated in so many words in a paper's methods section — the codebook instruction
itself says "Look for explicit statements like 'prospective study,' 'case series'"
(`extraction_codebook.yaml:36-39`). The high end is occupied by fields whose value is a
*judgment about* the paper (`clinical_readiness_assessment`) or a *decomposition of agency*
(`task_*`) that no sentence in a paper ever phrases as `R` or `Shared`.

### 4.4 Option-set conformance of stored values (supplementary)

| arm | out-of-set categorical values | rate |
|---|---:|---:|
| local_deepseek_r1_32b | 48 / 2067 | 2.3% |
| openai_o4_mini_high | 14 / 1909 | 0.7% |
| anthropic_sonnet_4_6 | 0 / 2090 | **0.0%** |

Two systematic patterns, both worth recording:

- **Cross-field option bleed on `validation_setting`** (25 of local's 48): values borrowed from
  the *neighbouring* fields' option sets — `'Computational / Simulation Only'` (×5, a
  `surgical_domain` option), `'Simulation / computational only; Phantom/Simulation'` (×5, a
  `system_maturity` option), `'Non-clinical Bench / Phantom'` (×2, `surgical_domain`).
- **Human-style bare labels on `autonomy_level`**: `'2'` ×8, `'3'` ×2, `'4'` ×2 — the workbook
  form (§3.1) appearing in the model arm even though the model was never shown it.

---

## 5. Example spans — top-5 categorical fields by untraceable count

Two spans per field, deterministic sample (`random.Random(20260727)`), distinct arms where the
field's untraceable pool contains more than one arm. "Rationale?" = does the snippet read as
model-authored prose justifying the coded option rather than a quotation.

| field | paper | arm | value | snippet (verbatim, truncated) | census bucket | **rationale restating the option?** |
|---|---|---|---|---|---|---|
| clinical_readiness_assessment | 553 | local | `Proof of concept only` | "The AFE has been tested in a bench setup and simulation environment but not in clinical trials." | C (no sentence traceable) | **Yes** — restates the option definition ("phantom, bench model… no clinical pathway", `codebook:552`) |
| clinical_readiness_assessment | 474 | anthropic | `Proof of concept only` | "For the first three PSM tasks, we set up the physical experiment following the setting of [37]… **…** We believe SurRoL will embrace the advances in learning-based methods…" | A (all sentences traceable) | No — two real, distant passages joined by a literal `...` |
| task_generate | 366 | local | `R` | "The system generates a plan based on detected mental workload." | C | **Yes** — restates "R = system computes the plan" |
| task_generate | 15 | anthropic | `Shared` | "The Task Manager**...**generates a sequential plan using pre-operative information… **…** The domain for the suturing task is represented using parameters such as ?psm…" | A | No — real text, two ellipsis bridges |
| task_monitor | 764 | local | `R` | "The robot uses sensors and cameras to monitor the fetoscope tip." | C | **Yes** — restates "R = autonomous sensing (cameras, force sensors)" |
| task_monitor | 498 | anthropic | `Shared` | "A pair of industrial CMOS cameras is used for online visual feedback… **…** a surgeon's decision and supervision remains critical…" | A | No — real text, one bridge; the bridge is what makes `Shared` visible |
| task_select | 277 | local | `NR` | "The paper does not explicitly describe task selection." | C1 (meta-statement) | **Yes** — absence assertion, not a quote |
| task_select | 617 | anthropic | `R` | "Proximal policy optimization (PPO) is used to train agents. **...** two vision-based agents for irrigation and suction are trained in the simulator…" | A | No — both sentences verbatim @1.00 |
| system_maturity | 534 | anthropic | `Commercial system + research autonomy` | "Additionally, we employed a Universal Robots UR3 robot and an Intel RealSense D435i (RS435i) camera**...** a control system for a Universal Robots UR3 robot, which marks the needle entry points…" | A | No — real text, one bridge |
| system_maturity | 281 | openai | `Commercial system + research autonomy` | "Figure 1A shows the hardware configuration of our system, which consists of a da Vinci Research Kit (dVRK) Si with wrist cameras mounted…" | A | No — single verbatim sentence, coverage 0.97; fails `grep_verify` only on parser noise |

**5/10 are model-authored rationales; all 5 are local-arm.** The split is perfectly clean along
the arm axis in this sample, consistent with §4.2.

Scaled up over all **412** local untraceable categorical spans with no traceable sentence
(buckets C/C1/too-short), a lexical proxy — snippet contains the field's action stem
(monitor/observ/sensor…, generat/plan/comput…, select/choos/decid…, etc.) or the option wording —
classifies:

| pattern | n | % |
|---|---:|---:|
| rationale restating the coded option (lexical proxy) | 274 | 66.5% |
| other untraceable prose | 88 | 21.4% |
| absence / meta-statement ("The paper does not…") | 50 | 12.1% |

Per field: task_generate 96%, validation_setting 85%, task_monitor 82%, autonomy_level 70%,
clinical_readiness_assessment 67%, system_maturity 62%, task_execute 60%, task_select 59%,
surgical_domain 58%, study_type 40%, study_design 30%. *This is a lexical heuristic, not
adjudication* — it establishes the shape of the population, and the 10 hand-read examples above
are the evidentiary basis.

---

## 6. Implication for the per-field evidence-policy question (descriptive only)

The option sets themselves are clean: codebook, review spec, and rendered prompt agree exactly on
all 11 categorical fields — membership, wording, order, and per-option definitions — so nothing
in the untraceable-span population is attributable to a malformed, truncated, or drifted option
list, and no prompt-assembly bug is implicated. What the audit surfaces instead is that the
evidence obligation is uniform across fields while the *evidenceability* of those fields is not:
`study_type`, whose coded value papers routinely state in words, fails the anchoring test 7.1% of
the time, while `clinical_readiness_assessment` and the four `task_*` agency-decomposition fields
— where no sentence in any paper says `R`, `Shared`, or `Proof of concept only` — fail at 23–31%,
and two-thirds of the local arm's no-textual-basis snippets on those fields are the model writing
its own one-line justification in a slot the prompt reserves for copied text. The three-way diff
adds that the human corpus was never asked the same question at all on 10 of the 11 categorical
fields ("No source quotes needed", `Instructions!A27-A29`), that where both populations *are*
asked (`clinical_readiness_assessment`) the instructions genuinely agree, and that two structural
mismatches sit underneath any per-field policy: the prompt licenses semicolon multi-values on
`validation_setting`/`surgical_domain` that the single-select workbook cannot represent (~26–30%
of validation_setting spans per arm), and the `NOT_FOUND` / "use ONLY the allowed values"
instruction pair is self-contradictory on the five fields with no absence option — the same five
where a human told to "enter NR" also has no `NR` in the dropdown, with `system_maturity` having
no escape value of any kind for either population. Whatever per-field policy is chosen, these
findings say the decision boundary is not tier and not model arm: it is whether a field's value
is *quotable from* the paper or *inferred about* it, and the current single-string, one-snippet-
per-field contract offers no way to express the second case, which is what the models are
visibly working around.

---

## Provenance

Read-only inputs: `data/surgical_autonomy/extraction_codebook.yaml`,
`review_specs/surgical_autonomy_v1.yaml`, `data/surgical_autonomy/Extraction_Workbook_v2_A.xlsx`,
`engine/agents/extractor.py`, `engine/analysis/normalize.py`,
`data/surgical_autonomy/review.db` (opened read-only), and the DIAG-UNANCHOR-01 artifacts
`anchor_results.json` / `census2.json`.

Scratchpad scripts (not committed): `optset_diff.py` → `optset_diff.json`,
`rendered_prompt_schema_only.txt`; `xtab.py` → `xtab.json`; ad-hoc conformance and example pulls
→ `top5_examples.json`.

Out of scope and not done: vision-tier use rate, any fix design or implementation, Arm P result
analysis, metric-definition changes, primer.md edits.
