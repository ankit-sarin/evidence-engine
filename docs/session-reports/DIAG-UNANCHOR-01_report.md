# DIAG-UNANCHOR-01 — Snippet-instruction location audit
**Production extraction prompt vs. human extractor workbooks**

Project: evidence-engine · Machine: DGX (spark-59e4) · Date: 2026-07-27
Scope: diagnosis only. No prompt changes, no fix implemented, no DB writes, no commits.

> **Ollama attestation — ACCEPTANCE GATE 4.** Zero Ollama API calls of any kind were made
> during this task: no inference, no `/api/ps`, no `/api/tags`, no `ollama` CLI invocation.
> All work was file reads, offline Python string assembly, read-only SQLite (`mode=ro`), and
> pure-Python text matching. `engine.agents.extractor` and `engine.agents.auditor` were
> imported for `build_extraction_prompt()` / `_normalize()` / `grep_verify()` semantics only;
> no function that reaches Ollama was called. The `d0s2_armP_rerun` tmux session was not
> touched, inspected, or signalled.

---

## 1. Instruction comparison table

### 1a. Model population — exact rendered text

The production prompt is assembled by `build_extraction_prompt()` in
`engine/agents/extractor.py:105-180`, from `data/surgical_autonomy/extraction_codebook.yaml`.
Snippet governance appears in **two** places and nowhere else.

**(i) Global footer instruction — `engine/agents/extractor.py:173`** (verbatim, one line in
the rendered prompt; renders at line 226 of the 232-line prompt header, i.e. the last block
before `## Paper Text`):

> - **source_snippet**: A verbatim quote (1-3 sentences) copied character-for-character from
> the paper that supports your extraction. Do NOT paraphrase, summarize, or rephrase in any
> way. Do NOT bridge distant passages with "..." or ellipses — quote one continuous passage
> only. If value is "NOT_FOUND", set source_snippet to "". Never fabricate a snippet — every
> non-empty snippet must be a real quote from the paper. For Tier 4 judgment fields, quote the
> passage that most informed your judgment.

**(ii) Per-field marker — `engine/agents/extractor.py:99-100`**, emitted only when the
codebook sets `source_quote_required: true` (verbatim):

> `  *Source quote required for this field.*`

`source_quote_required: true` is set on exactly **two of twenty** fields:
`key_limitation` (`extraction_codebook.yaml:510`) and `clinical_readiness_assessment`
(`extraction_codebook.yaml:571`). Their codebook `instruction:` strings add (verbatim):

> `Quote the passage(s) that informed your assessment.` (`extraction_codebook.yaml:509`)
> `Synthesize the results, limitations, and validation setting to make this judgment. Quote the key evidence behind your assessment.` (`extraction_codebook.yaml:528-529`)

**(iii) Grammar-level description — `engine/agents/models.py:14-16`** (verbatim). This string
is inside the JSON schema handed to Ollama's `format=` parameter, so it is visible to the
model at Pass 2 generation time:

> `description="Verbatim quote from paper supporting this value (1-3 sentences). Empty string if value is NOT_FOUND."`

**(iv) Arm-specific system prompts.** The user-turn prompt is byte-identical across all three
arms (`engine/cloud/base.py:69-71` → `build_extraction_prompt(parsed_text, self.spec)`).
The system turns differ and are *weaker* than the footer:

| Arm | System-prompt evidence clause | File:line |
|---|---|---|
| local (deepseek-r1:32b) | Pass 1: "reason through each extraction field step by step" — **no snippet clause**. Pass 2: "Respond ONLY with the requested JSON" — **no snippet clause**. | `extractor.py:196-206`, `extractor.py:241-255` |
| openai (o4-mini) | "Be thorough and **cite source text** for every extracted value." | `cloud/openai_extractor.py:70-74` |
| anthropic (sonnet-4-6) | "Be thorough and **cite source text** for every extracted value." | `cloud/anthropic_extractor.py:73-77` |

"Cite source text" is a support requirement, not a verbatim requirement. It does not
contradict the footer, but it is the only snippet language the cloud arms see twice, and the
weaker of the two formulations.

**(v) Post-generation enforcement (local arm only).**
`_validate_and_retry_snippets()` (`extractor.py:326-360`) re-prompts up to
`SNIPPET_MAX_RETRIES = 2` times, but the trigger is `INVALID_SNIPPET_RE`
(`engine/core/constants.py:7`, `r"\[\.{3}\]|\[…\]|…|\.{3,}"`) — **ellipsis characters only**.
The retry prompt (`extractor.py:295-305`) says "a single contiguous verbatim sentence copied
exactly from the text… If no single sentence supports this value, return null". Nothing in
the local pipeline checks at generation time whether the snippet actually occurs in the paper;
that test (`grep_verify`, `auditor.py:65-100`) runs only in the downstream audit stage, and
only for the local arm. The cloud arms have **no** snippet validation at all.

**Timing check (material):** the strengthened footer landed in commit `cc1343a`
(2026-03-04 21:40 UTC, "Extractor: emit Tier 4 fields, enforce verbatim snippets"). Run 6
extractions are dated 2026-03-15 → 2026-03-18 (local), 2026-03-16 → 2026-03-18 (cloud). **All
three arms in Run 6 ran under the current, strong verbatim instruction.** The 25.1% finding is
not an artifact of a weaker legacy prompt.

### 1b. Human population — exact workbook text

Source: `data/surgical_autonomy/Extraction_Workbook_v2_A.xlsx` (sheets: `Instructions`,
`Extraction Form`, `Codebook`). **No generator for this workbook exists in the repository** —
`grep -rl "Extraction Form|Extraction_Workbook" --include=*.py` returns only the *importer*
(`analysis/paper1/human_import.py`) and its test. `engine/exporters/review_workbook.py` is the
shared adjudication-workbook builder and does not produce this file. The workbook is a
hand-authored artifact; the .xlsx is the authoritative text.

`Instructions` sheet, row 23 (verbatim):

> 6. Source quotes (columns Y-Z): MANDATORY for key_limitation and
> clinical_readiness_assessment only. Copy the exact passage(s) that informed your answer.

`Instructions` sheet, rows 27-30 (verbatim, field-summary block):

> Tier 1 — Explicit (5 fields): … Usually directly stated in text. **No source quotes needed.**
> Tier 2 — Interpretive (9 fields): … **No source quotes needed** but helpful for ambiguous cases.
> Tier 3 — Numeric (4 fields): … **No source quotes needed.**
> Tier 4 — Judgment (2 fields): key_limitation, clinical_readiness_assessment. YOUR
> interpretation. **Source quotes MANDATORY.**

`Extraction Form` sheet, row 1 col Y: `SOURCE QUOTES (MANDATORY)`; row 2 cols Y/Z:
`SQ: key_limitation [REQ]`, `SQ: clinical_readiness [REQ]`. There are **no** source-quote
columns for the other 18 fields — a human physically cannot record evidence for Tier 1-3.

`Codebook` sheet, col G (`Source Quote Required`): `No` for rows 2-19 (all Tier 1-3 fields),
`MANDATORY` for rows 20-21 (`key_limitation`, `clinical_readiness_assessment`).

### 1c. Side-by-side

| Property | **Model population** | **Human population** |
|---|---|---|
| Verbatim required? | **Yes, explicitly and maximally**: "copied character-for-character", "Do NOT paraphrase, summarize, or rephrase in any way", "Never fabricate a snippet" | **Yes, but softer**: "Copy the exact passage(s) that informed your answer" |
| Contiguity required? | **Yes, explicitly**: "Do NOT bridge distant passages with '...' or ellipses — quote one continuous passage only" | **No.** "passage(**s**)" — plural is explicitly permitted; no contiguity or ellipsis rule stated anywhere |
| Which fields? | **All 20 fields.** Snippet is a mandatory member of every emitted record ("You MUST emit exactly one entry per field") | **2 of 20 fields only** (`key_limitation`, `clinical_readiness_assessment`). The other 18 have no quote column at all |
| Location in prompt/doc | Global footer, in the `## Instructions` block, immediately before `## Paper Text` (rendered line 226 of 232) — i.e. maximally distant from the field definitions it governs, and immediately adjacent to ~46k chars of paper text | Instructions sheet step 6 of 7, reinforced in the tier summary, reinforced in the column headers (`[REQ]`), reinforced in the Codebook sheet col G |
| Global vs per-field | **Both**: one global footer clause + a per-field marker for the 2 `source_quote_required` fields. The other 18 fields carry no per-field snippet language | **Per-field only**, and only for the same 2 fields. There is no global "always quote" rule |
| Absence handling | `value = "NOT_FOUND"` → `source_snippet = ""` (explicit) | `NR (Not Reported)` for the value; no snippet column exists for Tier 1-3, so no instruction |
| Enforcement | Local: ellipsis-regex retry ×2 at generation, `grep_verify` at audit. Cloud: **none** | Importer validation (`human_import.py`); no textual anchoring check |

---

## 2. Divergence verdict — ACCEPTANCE GATE 2

**Yes — the two populations diverge, and the divergence is structural, not cosmetic.**

Precise characterization, in order of decreasing importance:

1. **Coverage divergence (the dominant one).** Models are required to produce a verbatim
   snippet for **all 20 fields**. Humans are required to produce one for **2 fields**, and the
   workbook has no column in which they *could* produce one for the other 18. So for 18 of 20
   fields the human corpus contains **no evidence claim at all**, while the model corpus
   contains 20 evidence claims per paper. The two populations are not operating under a
   different definition of evidence for those fields — the humans are not operating under
   *any* evidence obligation, so the comparison is undefined there rather than unequal.
2. **Contiguity divergence.** Where both populations *do* produce quotes (the 2 Tier 4 fields),
   models are forbidden to bridge passages ("quote one continuous passage only", no ellipses);
   humans are explicitly invited to bridge ("passage(**s**)"). A human doing exactly what the
   workbook asks would generate a snippet that the model-side anchoring test scores as
   **unanchored**. The metric is therefore not symmetric across populations.
3. **Verbatim-strictness divergence (mild, same direction).** Both say copy exactly; the model
   instruction is far more emphatic and adds an anti-fabrication clause. Humans get "Copy the
   exact passage(s)" once, with no anti-paraphrase or anti-fabrication language.
4. **Non-divergence worth recording.** For the 2 Tier 4 fields, both populations are told to
   quote *the passage that informed the judgment* — the codebook (`:509`, `:529`) and the
   workbook (row 23) use near-identical wording. The Tier 4 evidence contract is genuinely
   shared. Everything else is not.

The one-line answer for the fix design: **models are told "verbatim, contiguous, for every
field"; humans are told "exact passage(s), non-contiguous OK, for two fields."** The 25.1%
unanchored rate is measured against a rule only the models were ever given.

---

## 3. Span classification — ACCEPTANCE GATE 3

### 3a. Method

`UNANCHORED` is not a stored column anywhere in the schema (`grep -rn "UNANCHORED"` over the
repo returns nothing; `evidence_spans.audit_status ∈ {pending, verified, contested, flagged,
invalid_snippet}` and `cloud_evidence_spans` has no audit column at all). I therefore
reconstructed it using the codebase's own anchoring test — `grep_verify()`
(`auditor.py:65-100`): normalized exact substring, else sliding word-window
`SequenceMatcher > 0.85` — applied to every Run 6 span in all three arms against
`data/surgical_autonomy/parsed_text/{paper_id}_v*.md`. Read-only, pure Python.

Reconstructed rates (spans with a non-empty snippet as denominator):

| Arm | spans | with snippet | anchored | **UNANCHORED** | rate |
|---|---:|---:|---:|---:|---:|
| anthropic_sonnet_4_6 | 3800 | 3777 | 2460 | 1317 | **34.9%** |
| local_deepseek_r1_32b | 3760 | 3619 | 2791 | 828 | **22.9%** |
| openai_o4_mini_high | 3457 | 3209 | 3015 | 194 | **6.0%** |
| **all arms** | 11017 | 10605 | 8266 | **2339** | **22.1%** |

I could not reproduce 25.1% exactly under any denominator I tried (22.1% of snippet-bearing
spans; 21.2% of all spans; 23.1% counting value-bearing spans with an empty snippet as
unanchored; 41.2% if only exact substring counts and fuzzy is disallowed). **The definition
behind the 25.1% headline should be pinned down before it goes in a manuscript** — the plain
`grep_verify` reading of this DB gives 22.1%, and the gap between 22.1% and 41.2% is entirely
the fuzzy-match tolerance, which is a defensible-but-arbitrary 0.85.

### 3b. The 10 spans (stratified: 3 local / 3 openai / 4 anthropic, one field each)

Sampling: deterministic `random.Random(20260727)` shuffle of the unanchored pool per arm,
first N distinct `field_name`s. Each snippet was then split into sentences and each sentence
re-tested against the paper independently, to separate "quote doesn't exist" from "quote
exists but isn't contiguous".

| # | paper_id | arm | field (tier) | whole-snippet best ratio | per-sentence result | **classification** |
|---|---|---|---|---:|---|---|
| 1 | 556 | local | comparison_to_human | 0.49 | 0/1 sentences in paper | **Apparent fabrication (non-quote)** — sub-type: model-authored absence meta-statement |
| 2 | 392 | local | country | 0.58 | 0/1 in paper | **Apparent fabrication (non-quote)** — sub-type: model-authored inference statement |
| 3 | 553 | local | sample_size | 0.49 | 0/1 in paper | **Apparent fabrication (non-quote)** — sub-type: absence meta-statement |
| 4 | 663 | openai | task_generate (2) | 0.41 | 2/2 in paper @1.00, non-adjacent | **Near-verbatim with drift** — silent stitch of two distant passages |
| 5 | 549 | openai | validation_setting (2) | 0.68 | 2/2 in paper @1.00 | **Near-verbatim with drift** — silent elision of one intervening sentence |
| 6 | 476 | openai | secondary_outcomes (3) | 0.77 | 1/2 @1.00, 1 partial @0.69 | **Near-verbatim with drift** — elision + parser noise (a figure caption is interpolated mid-sentence in the parsed markdown) |
| 7 | 748 | anthropic | clinical_readiness_assessment (4) | 0.63 | 2/2 @0.99/0.95 | **Near-verbatim with drift** — explicit `...` ellipsis bridging two distant passages |
| 8 | 610 | anthropic | surgical_domain (1) | 0.55 | 3/3 @1.00 | **Near-verbatim with drift** — silent stitch (3 verbatim sentences, non-adjacent) |
| 9 | 11 | anthropic | secondary_outcomes (3) | 0.78 | 2/3 @1.00, 1 partial @0.62 | **Near-verbatim with drift** — parser-garbled source region (`1 04` for `1.04`, dropped words) + elision |
| 10 | 738 | anthropic | task_select (2) | 0.58 | 5/5 @1.00 | **Near-verbatim with drift** — silent stitch across a figure caption |

**Counts (n=10):**

| Category | count |
|---|---:|
| Near-verbatim with drift (minor edits, ellipses, stitching, parser noise) | **7** |
| Clear paraphrase | **0** |
| Apparent fabrication | **3** (all 3 are non-quotes — see note) |

*Note on the third bucket.* None of the three local cases invents a **fact**; all three state
something true or defensible about the paper. What they fabricate is the **quotation** — the
model emitted its own prose into a field that the prompt reserves for copied text. Scoring
them as "fabrication" is correct under the prompt's own rule ("every non-empty snippet must be
a real quote from the paper") but would be misleading if reported as hallucinated content. I
have kept the contract's three buckets and flagged the sub-type rather than silently
reclassifying.

### 3c. Census over all 2,339 unanchored spans (supplementary, not required by the contract)

The 10-span pattern is not a sampling accident. Sentence-level 5-gram containment
(fragment counted traceable at ≥60% shingle coverage — robust to parser noise) over the full
unanchored set:

| Bucket | all arms | anthropic | local | openai |
|---|---:|---:|---:|---:|
| A — every sentence traceable to the paper (pure stitch/elision) | 1365 (58.4%) | 1122 (**85.2%**) | 74 (8.9%) | 169 (**87.1%**) |
| B — some sentences traceable | 163 (7.0%) | 140 (10.6%) | 12 (1.4%) | 11 (5.7%) |
| C — no sentence traceable | 513 (21.9%) | 54 (4.1%) | 445 (**53.7%**) | 14 (7.2%) |
| C1 — no sentence traceable, opens with meta-language ("The paper…", "Not reported…") | 183 (7.8%) | 4 (0.3%) | 183 (**22.1%**) | 1 (0.5%) |
| too short to classify (<7 words) | 115 (4.9%) | 1 | 114 (13.8%) | 0 |
| **literal ellipsis present in snippet** | 1083 (46.3%) | **1065 (80.9%)** | **0 (0.0%)** | 18 (9.3%) |

Read across the rows: **"unanchored" is one label covering two unrelated failure modes.**

- **Cloud arms (96% of their unanchored spans are bucket A or B): a contiguity failure.** The
  text is real; the model bridged. Anthropic does it with a visible `...` 81% of the time —
  it is openly signalling the bridge, complying with the *spirit* of "quote what supports this"
  while violating the explicit contiguity clause. Ellipsis rate 0.0% in the local arm vs 80.9%
  in Anthropic is a direct measurement of the local-only `INVALID_SNIPPET_RE` retry
  (`extractor.py:326-360`): that guard works, and nothing equivalent guards the cloud arms.
- **Local arm (76% bucket C/C1/too-short): a quotation failure.** The model wrote its own
  sentence instead of copying one. Representative local C-bucket snippets, verbatim from the
  DB: `'The robot performs the physical movements autonomously.'` (p455, task_execute),
  `'The robot uses YOLO11n for visual tracking.'` (p607, task_monitor),
  `"The mean Young's modulus during autonomous palpation was found to be 0.24 kPa."` (p660,
  primary_outcome_value), `'No explicit selection process described.'` (p14, task_select).
  These are *justifications*, not quotes. Note also that they cluster on the categorical
  Tier 1-2 fields (`task_*`, `system_maturity`, `autonomy_level`) — precisely the fields for
  which no contiguous sentence in the paper ever says "R" or "Level 4", so the model has
  nothing to copy and writes a rationale instead.

---

## 4. Implication for the fix design (descriptive only — nothing implemented)

The audit says the 25.1%/22.1% headline is not one defect and will not respond to one prompt
edit. Roughly three-fifths of it (the cloud arms, and Anthropic in particular) is a
**contiguity** failure against a rule the models are given exactly once, in a footer sitting
immediately before ~46k characters of paper text — the weakest possible position for a
constraint that must survive 20 sequential field decisions — and which the arm-level system
prompts then dilute to "cite source text"; the offending snippets are made of real,
individually verifiable sentences, and Anthropic marks its bridges with a visible `...` 81% of
the time, so this population is largely *recoverable* by machine rather than lost. The
remaining two-fifths (essentially all local) is a **quotation** failure concentrated on
categorical fields where no single sentence in the paper can literally state the coded value,
so the model substitutes its own rationale — a structural mismatch between "one verbatim
contiguous quote per field" and what a categorical judgment field can actually be evidenced by,
not a compliance problem the model could have solved as instructed. Any fix design must
therefore decide three things before touching prompt text: (a) whether the evidence contract
stays uniform across all 20 fields or splits by field type, given that the human arm already
answers this question the other way — quotes for 2 judgment fields only, non-contiguity
explicitly permitted; (b) whether contiguity remains a hard requirement or becomes a
structured multi-span field, since the current single-string schema (`models.py:14-16`) offers
a model with two supporting sentences no legal way to report both; and (c) whether the
concordance comparison against human extractors is even well-posed for the 18 fields where the
human workbook has no evidence column at all — the 22.1% is currently measured against a rule
that only one of the two populations was ever issued, and a symmetric definition (or an
explicitly asymmetric, documented one) is a prerequisite for reporting it in the manuscript.
The two enforcement asymmetries found along the way are separately actionable and independent
of any prompt change: the ellipsis retry guard exists only on the local arm, and no anchoring
check of any kind runs on cloud spans.

---

## Provenance

Read-only inputs:
`engine/agents/extractor.py`, `engine/agents/models.py`, `engine/agents/auditor.py`,
`engine/cloud/base.py`, `engine/cloud/openai_extractor.py`, `engine/cloud/anthropic_extractor.py`,
`engine/core/constants.py`, `engine/exporters/review_workbook.py`,
`data/surgical_autonomy/extraction_codebook.yaml`,
`data/surgical_autonomy/Extraction_Workbook_v2_A.xlsx`,
`data/surgical_autonomy/parsed_text/*.md`,
`data/surgical_autonomy/review.db` (opened `mode=ro`).

Scratchpad scripts (not committed):
`render_prompt.py` (offline prompt render → `rendered_prompt_paper100.txt`, 72,172 chars),
`anchor_audit.py` (→ `anchor_results.json`), `sample_spans.py` (→ `sample_spans.json`),
`frag_check.py`, `frag_census.py`, `frag_census2.py` (→ `census2.json`).

Not done, per scope: vision-tier use rate, production option-set audit, any prompt
modification, any re-run, any judge re-scoring, any primer.md edit.
