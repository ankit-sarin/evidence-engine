# SCHEMA-DERIVE-01 phase-2b — recovered read-out

**Provenance header.** This file is a verbatim recovery of one Claude Code
assistant message. Nothing below the header has been edited, summarised or
reformatted.

- **Source transcript:** `/home/ankitsarin/claude-session-archive/4500483d-106b-4e9c-989a-a3314c775cb2.jsonl`
- **Session id:** `4500483d-106b-4e9c-989a-a3314c775cb2`
  - Note: the unified plan v46 attributes this session's work to 2885f251; the work was done in 4500483d, its continuation.
- **Source message line:** L2714 (NDJSON record, `type: assistant`)
- **Brief at transcript line:** L1912, 2026-09-10T23:19:26.249Z
- **Message timestamp:** 2026-09-11T00:47:35.265Z
- **HEAD stated in read-out:** **HEAD `e99204f`**, tree clean.
- **Architect acknowledgement:** L2724, 2026-09-11T01:57:24.951Z — "RULINGS on SCHEMA-DERIVE-01 Phase 2b report — accepted at HEAD e99204f."
  *(user-authored; architect turn, not CC output)*
- **Trust:** Transcript-derived. Values in this report that trace to a tool call in the source transcript at the stated HEAD may be cited as MEASURED. Classifications, counts arrived at by reading, and judgements are INFERRED until re-measured.
- **Recovered by:** READOUT-ARCHIVE-01, 2026-09-11, recovering session `410e9ecc-67a0-44c6-9523-ffad8f528ca3`.
  Compiled from transcript `/home/ankitsarin/claude-session-archive/4500483d-106b-4e9c-989a-a3314c775cb2.jsonl` by session 410e9ecc… (READOUT-ARCHIVE-01) on 2026-09-11.
  *(files written 2026-09-12 00:49 UTC; the session opened 2026-09-11 23:29 UTC and crossed midnight)*

---

Pushed. **HEAD `e99204f`**, tree clean. R6 is the one item I have not executed — reason below.

---

# SCHEMA-DERIVE-01 PHASE 2b — FINAL REPORT

## 🔴 R6 held, not executed — P0 contradicts a stated MEASURED assumption

The addendum states, as MEASURED, that *"ordered_values does not reach prompt text"*, citing CODEBOOK-AUTH-01 P2. **That measurement was scoped to the extraction and elicitation prompts. `ordered_values` does reach a prompt — the judge's.**

`analysis/paper1/judge_prompts.py:710-712`, inside `_ordinal_rubric`, renders it directly into Pass-2 judge prompt text:

```
Field '<name>' is an ORDERED SCALE; its values represent increasing levels of
<dimension>, in order: A → B → C.
...
UNSUPPORTED — if the assigned level is HIGHER than the evidence supports.
```

So **I1 of the addendum is TRUE** (it feeds ordinal judge scoring and touches nothing in extraction or export), but the MEASURED line it sits under is too broad. Two consequences the PI should weigh before R6 lands:

1. **Reordering `validation_setting.ordered_values` changes the judge's rubric text** for every future judge run — it is the direction "higher than the evidence supports" is measured along. That is presumably *why* the PI wants fidelity order, so this likely confirms the ruling rather than undermining it.
2. **It will move `codebook_hash`.** `SEMANTIC_KEYS` covers `fields[*]` wholesale, so `ordered_values` is in the projection. Answering R8's explicit question: **yes, the semantic hash includes `ordered_values`.** After R6 every new extraction gets a different `codebook_hash` from today's, and all existing rows read as stale against it — even though the extraction prompt would not change (R9's check would pass).

Per contradiction handling I did not adapt. Everything else is done; R6 is one commit away on your word.

## Pre-flight (G1)

**P0** — `ordered_values` is consumed in exactly two places, both Paper-1: `judge_prompts.py:710-712` (the ordinal rubric, above) and `pi_audit_sampler_v2.py:241-242` (`"Ordered levels (low→high): …"` in the blinded audit workbook). Nowhere in extraction or export.

**P1 — clean, both guards present.** `extraction_cleanup.py:146-156`:

```python
    if dry_run:
        logger.info("DRY RUN — would delete %d extractions ...")
        return summary

    # Back up before destructive operations
    auto_backup(db.db_path, "pre-cleanup")
```

plus the module docstring — *"Destructive operation — dry-run is the default. Requires --confirm to execute"* — and `dry_run = not args.confirm` at :240. **I1 of the main brief TRUE**, so the cleanup predicate was switched.

**P2** — the section was exactly lines **110–242** of a 242-line file, running to EOF. In-memory removal leaves 16 top-level keys.

## Commits

| commit | what |
|---|---|
| `9b3f170` | `fields_by_tier`, `enum_values`, `FieldView` (C1, T1) |
| `911cd28` | the twelve consumers onto the codebook; fallback and `sample_size` literal deleted (C3) |
| `9de8249` | audit rubric rendered by the prompt's own builder (C4/R5, T7) |
| `dfde733` | staleness/cleanup/parity on `codebook_hash`; migration 013 (C5/C6, T4–T6) |
| `e99204f` | the spec section, its models and `extraction_hash()` removed (C2/C7, T2) |

## Gates

| gate | result |
|---|---|
| **G2** suite | **2150 passed, 17 deselected** (was 2,134/17), run as four chunks — see note |
| **G3** review.db | `99770368 B @ 2026-09-10 22:19:01.118726508 UTC` — unchanged; **013 not applied live** |
| **G4** prompt hash | **`2f12adc7daa3721935e2494850d7508d3726f100e1c422553836155a86c26762` — unchanged**, verified again after the section removal |
| **G5** commits | five, separated as specified, pushed |

`screening_hash` unmoved at `0d97b9d6…f98b90`. Live spec: 242 → **108 lines**. Codebook untouched — still `9581a4c`, `sha256 3807335b…4e6b5f`, `codebook_hash b551ec43…44fcbad`.

**Note on the gate:** the full-suite run was killed twice by the harness for "low memory" while 113 GB was free, so I ran it as four foreground chunks instead. `tests/test_ollama_client.py` needs ~87 s alone (retry/watchdog backoffs) and was the reason a combined chunk kept exceeding the foreground limit; it is slow, not hanging, and I verified on a stashed tree that its slowness predates this task.

## The rubric, before and after (C4)

**Before** — `spec.extraction_schema.fields[].description`, 9,244 chars:

```
EXTRACTION SCHEMA FIELDS

  study_type (categorical, Tier 1):
    The type of study as described in the methods section. Look for explicit statements like 'prospective study,' 'case series,' etc.
    Valid values: Original Research, Case Report/Series, Review, Systematic Review, ...
```

**After** — the prompt's own field block, 26,332 chars:

```
EXTRACTION FIELD CONTRACTS

Verbatim, as the extractor was given them. Judge the extracted
value against THIS text — it is the instruction the model followed.

  --- Tier 1 ---

  - **study_type** (categorical (allowed values: Original Research, ...)): The type of study as described in the methods section.
    *Instruction:* Look for explicit statements like "prospective study," "case series," "systematic review," etc. If the paper does not explicitly state the study type, infer from the methods section structure.
    **Value definitions:**
    - **Original Research** — Primary research reporting new data — experiments, trials, technical evaluations, algorithm development with results.
    ...
    **Decision criteria:**
    1. Does the paper report original experimental data or results? → "Original Research"
    ...
```

The human now sees the value definitions, decision criteria and examples the spec's prose never carried.

## Migration 013 (temp copy)

```
run 1  rebuilt: extractions.extraction_schema_hash, review_runs.extraction_hash
       already nullable: cloud_extractions.extraction_schema_hash
run 2  rebuilt: []   (no-op)

extractions        190 rows   content IDENTICAL
cloud_extractions  379 rows   content IDENTICAL
review_runs          6 rows   content IDENTICAL
```

**`cloud_extractions.extraction_schema_hash` was already nullable**, so it is not rebuilt — the brief assumed both were constrained. Rebuilding a table to remove a constraint it does not have is risk with no benefit. The new schema is derived from `sqlite_master`, so columns added by 004/005/012 survive.

## 🔴 Operational gate before any extraction

**The live database still has `extractions.extraction_schema_hash NOT NULL`, and the writers no longer supply it.** An extraction run against `review.db` will fail on the insert — loudly, naming the column — until **MIGRATE-013** is applied. Nothing is silent, and no run is scheduled, but this is a hard precondition for Run 7.

Related, and worth a decision: the idempotence lookup keys on `codebook_hash`, and all 190 existing extractions have NULL there. Under R3 they are stale, so a re-run would re-extract every paper rather than skip it. That is the intended semantics, not a defect — but it is a 190-paper consequence.

## R2 note, recorded

`validation_setting`'s spec description said *"Select the most advanced setting if multiple were used"* with an explicit hierarchy; the codebook's contract — the text the model actually received — says to list all, semicolon-separated. Human adjudicators read the spec text until commit `9de8249`. They now read the codebook's, so the discrepancy no longer reaches a human, but the **97 rows already adjudicated were judged against the wrong instruction.**

## Three findings fixed in passing

- `run_extraction` resolved the codebook from `spec.review_id`, ignoring a `data_root` override — now reads the one beside its own database.
- The trace exporter is pointed at legacy backups; it selects `codebook_hash` only when the database has the column, rather than refusing to run on a pre-012 file.
- `extraction_cleanup` lost `--spec` and gained `--codebook`: the hash comes from the codebook, so the flag determined nothing.

---

Stopping here. **R6 not executed**, awaiting your call on the P0 contradiction. Not applying 013 live; not starting SCREEN-AUTH-01.
