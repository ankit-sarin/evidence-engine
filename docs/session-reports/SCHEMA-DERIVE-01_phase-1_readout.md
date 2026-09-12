# SCHEMA-DERIVE-01 phase-1 — recovered read-out

**Provenance header.** This file is a verbatim recovery of one Claude Code
assistant message. Nothing below the header has been edited, summarised or
reformatted.

- **Source transcript:** `/home/ankitsarin/claude-session-archive/4500483d-106b-4e9c-989a-a3314c775cb2.jsonl`
- **Session id:** `4500483d-106b-4e9c-989a-a3314c775cb2`
  - Note: the unified plan v46 attributes this session's work to 2885f251; the work was done in 4500483d, its continuation.
- **Source message line:** L1870 (NDJSON record, `type: assistant`)
- **Brief at transcript line:** L1814, 2026-09-10T22:39:02.687Z
- **Message timestamp:** 2026-09-10T22:41:09.887Z
- **HEAD stated in read-out:** **HEAD `4106a8f`**, tree clean.
- **Architect acknowledgement:** L1874, 2026-09-10T23:04:23.340Z — "RULINGS on SCHEMA-DERIVE-01 Phase 1 — acknowledged at HEAD 4106a8f."
  *(user-authored; architect turn, not CC output)*
- **Trust:** Transcript-derived. Values in this report that trace to a tool call in the source transcript at the stated HEAD may be cited as MEASURED. Classifications, counts arrived at by reading, and judgements are INFERRED until re-measured.
- **Recovered by:** READOUT-ARCHIVE-01, 2026-09-11, recovering session `410e9ecc-67a0-44c6-9523-ffad8f528ca3`.
  Compiled from transcript `/home/ankitsarin/claude-session-archive/4500483d-106b-4e9c-989a-a3314c775cb2.jsonl` by session 410e9ecc… (READOUT-ARCHIVE-01) on 2026-09-11.
  *(files written 2026-09-12 00:49 UTC; the session opened 2026-09-11 23:29 UTC and crossed midnight)*

---

Read-only phase complete. No writes, no commits.

---

# SCHEMA-DERIVE-01 PHASE 1 — READ-OUT

**HEAD `4106a8f`**, tree clean. `review.db` at the R2 baseline: `99,770,368 B @ 2026-09-10 22:19:01.118726508 UTC` — **unchanged** (G2).

## Assumption verdicts

| | verdict |
|---|---|
| **I1** `description == definition` on all 20 | **🔴 FALSE — 19 of 20 differ.** Only `system_maturity` is a substring match. See S2 |
| **I2** every read maps to a codebook attribute | **TRUE with two additions and one exception.** `Codebook` exposes only `field`, `field_names`, `raw`, `lint_findings` — it needs `fields_by_tier` and an `enum_values` helper. The exception is `description`, which has **no clean replacement** |
| **I3** no direct sub-model construction | **FALSE, but trivially** — one site: `tests/test_extraction_validator.py:331` constructs `ExtractionField(...)`, which I already touched in CODEBOOK-AUTH-01 when the type validator rejected its `type="str"` |

## S1 — Consumer table

| file:line | reads | codebook replacement |
|---|---|---|
| `engine/agents/extractor.py:138,155` | `fields_by_tier(t)` — the prompt's field set and tier grouping | **needs `Codebook.fields_by_tier`** |
| `engine/agents/extractor.py:149` | `f.name, f.type, f.enum_values, f.description` — **bare-spec fallback** | dead path: fires only when a field is absent from the codebook, and logs a WARNING when it does |
| `engine/agents/extractor.py:290,779` · `cloud/base.py:36` · `elicitation/pipeline.py:284` | `extraction_hash()` | `codebook_hash` per R3 |
| `engine/core/completeness.py:111` | `fields_by_tier(t)` → names | `fields_by_tier` |
| `engine/validators/extraction_validator.py:103,192,197,271` | `f.name`, `f.type`, `f.enum_values`; annotates `dict[str, ExtractionField]` | `type`, `valid_values[].value`; **annotation must change type** |
| `engine/analysis/normalize.py:58` | `f.name`, then the field object for `enum_values` | `field()`, `valid_values` |
| `engine/analysis/report.py:29` | `{f.name: f.tier}` | `tier` |
| `engine/agents/auditor.py:390` | `{name: type}`, `{name: tier}` | direct |
| `engine/exporters/evidence_table.py:39` · `docx_export.py:43` | `[f.name …]` | `field_names` |
| `engine/exporters/methods_section.py:57` | `len(fields)` | `len(cb.fields)` |
| `engine/adjudication/audit_adjudicator.py:205-211` | `name, type, tier, **description**, enum_values` | **🔴 `description` has no replacement** |
| `scripts/eval_auditor_models.py:91-92` · `smoke_test_fixes.py:51-52` | `{name: type}`, `{name: tier}`, count | direct |
| `analysis/paper1/export_disagreement_pairs.py:48,59` | `f.type`, `f.tier` | direct |
| `analysis/eval/run_cloud_strict.py:177` | `strict_extraction_schema(expected)` — **names only**, not the spec object | unaffected |

**One site with no clean replacement: `audit_adjudicator.py:209`**, which writes `field.description` into the human audit-review queue's instruction block. That is a live, human-facing consumer — not a dead fallback — and S2 shows the text it prints is not in the codebook.

## S2 — `description` vs `definition`, all 20 fields

**19 of 20 differ.** The spec's `description` is not a longer or shorter version of one codebook attribute; it is *differently factored* prose.

| relation to `definition + " " + instruction` | count | fields |
|---|---:|---|
| exact | **0** | — |
| one contains the other | 1 | `system_maturity` |
| neither | **19** | all the rest |

Totals: spec `description` **5,712 chars** vs codebook `definition + instruction` **5,869**. Close in aggregate, and that closeness is misleading — **6 fields have MORE text in the spec than in the codebook's two fields combined**:

| field | spec | def+instr |
|---|---:|---:|
| `autonomy_level` | **799** | 429 |
| `task_generate` | **332** | 221 |
| `task_select` | **327** | 189 |
| `task_monitor` | **325** | 136 |
| `task_execute` | **291** | 105 |
| `validation_setting` | **213** | 186 |

Inspection shows why: the spec's `description` inlines the **per-value rubric** — `autonomy_level` carries the full Yang et al. Level 0–5 enumeration; the four `task_*` fields carry the `H = … R = … Shared = … NR = …` decoding. The codebook holds that content in `valid_values[].definition` instead. So the material is *probably* recoverable from `definition + instruction + valid_values[].definition`, but **not by string equality anywhere**, and I did not verify a three-way reconstruction because Phase 1 is read-only and the answer would be a judgement about prose, not a measurement.

Where the codebook is longer it carries content the spec lacks — e.g. `study_type`'s *"If the paper does not explicitly state the study type, infer from the methods section structure"*, and `clinical_readiness_assessment` at 547 vs 289.

**Neither file is a superset of the other.**

## S3 — Hash readers

**Written** at `extractor.py:290,335,779`, `cloud/base.py:36,279`, `elicitation/pipeline.py:284,292,372`, `run_pipeline.py:345-351` (`review_runs`), and stored `NOT NULL` at `database.py:150`.

**Read for a decision** at four sites, all comparing stored-vs-current:

| file:line | what it does |
|---|---|
| `engine/core/database.py:864-872` `get_stale_extractions` | `WHERE e.extraction_schema_hash != ?` on each paper's latest extraction → the staleness list |
| `engine/utils/extraction_cleanup.py:47-51` `check_stale_extractions` | `COUNT(DISTINCT paper_id) WHERE extraction_schema_hash != ?` → the extractor's pre-flight warning |
| `engine/utils/extraction_cleanup.py:76-89` | selects rows to **delete** — the destructive path |
| `engine/analysis/concordance.py:175-179` `check_schema_parity` | `SELECT DISTINCT` per arm, warns on divergence, does not block |

Plus two non-deciding readers: `extractor.py:821` (idempotence lookup on `(paper_id, hash)`) and `trace_exporter.py:334,380` (prints it into the trace).

`review_spec_hash` (`database.py:207`) is written once at `run_pipeline.py:349` as `screening_hash + extraction_hash` and **read by nothing**.

## S4 — Direct constructions

**One**, and it is a test: `tests/test_extraction_validator.py:331`. Nothing in `engine/`, `scripts/` or `analysis/` constructs `ExtractionField` or `ExtractionSchema` outside their definitions at `review_spec.py:33,67`.

## S5 — What removal costs the spec

`extraction_schema:` occupies **lines 110–242 — 133 of 242 lines, 54% of the file.** After removal: **109 lines, 16 top-level keys** (the brief expected 21 declared *model* fields; the live file declares **17** and would declare 16).

## S6 — What makes Phase 2 more than mechanical

Eleven of the twelve consumers are a rename — `f.type`, `f.tier`, `f.name`, `enum_values → valid_values[].value` — needing only two additions to the `Codebook` object (`fields_by_tier`, an `enum_values` accessor) and one changed type annotation. **The whole of the difficulty is `description`.** It is read in exactly one live place, `audit_adjudicator.py:209`, where it prints the field rubric into the instruction block of the human audit-review queue; it differs from `definition` on 19 of 20 fields; and on six fields it carries per-value decoding text — the Yang levels, the `H/R/Shared/NR` legend — that lives in `valid_values[].definition` in the codebook, in a different shape. So removing the section silently changes what a human adjudicator reads, and no test compares the two strings, which is why the divergence has gone unnoticed. That is a **content decision, not a code decision**: either the adjudicator is rewritten to render `definition + instruction + valid_values` (a new rubric format, which should be reviewed by eye once against the current output), or the spec's descriptions are folded into the codebook's `instruction` field first as a separate content task, or the loss is accepted deliberately. Everything else in Phase 2 — including R3's hash retirement, where `extraction_schema_hash` stops being written and the four deciding readers move to `codebook_hash` — is mechanical, with the single caveat that those four readers compare against **existing rows whose `codebook_hash` is NULL** (all 190/379 of them, by MIGRATE-012's design), so `!= current_hash` will match every historical row and the staleness and cleanup paths need an explicit NULL policy before they are switched over.

**One incidental finding**, recorded not acted on: `analysis/paper1/export_disagreement_pairs.py:52` hardcodes `if field_name in ("sample_size",): return "numeric"` — a per-field literal that exists only because the spec said `text` where the codebook said `numeric`. CODEBOOK-AUTH-01 made both files say `numeric`, so this branch is now redundant and can be deleted with the rest.

---

Stopping here. Phase 2 not drafted. Awaiting the next brief or ruling.
