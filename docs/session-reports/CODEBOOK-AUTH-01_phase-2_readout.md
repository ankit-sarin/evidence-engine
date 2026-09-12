# CODEBOOK-AUTH-01 phase-2 — recovered read-out

**Provenance header.** This file is a verbatim recovery of one Claude Code
assistant message. Nothing below the header has been edited, summarised or
reformatted.

- **Source transcript:** `/home/ankitsarin/claude-session-archive/4500483d-106b-4e9c-989a-a3314c775cb2.jsonl`
- **Session id:** `4500483d-106b-4e9c-989a-a3314c775cb2`
  - Note: the unified plan v46 attributes this session's work to 2885f251; the work was done in 4500483d, its continuation.
  - Note: the unified plan v46 cites this read-out as 'Phase 2b'. The transcript label is 'Phase 2'. The plan is corrected at wrap.
- **Source message line:** L1686 (NDJSON record, `type: assistant`)
- **Brief at transcript line:** L954, 2026-09-10T20:37:40.483Z
- **Message timestamp:** 2026-09-10T22:06:33.860Z
- **HEAD stated in read-out:** **HEAD `13b16a4`**, tree clean, local == origin.
- **Architect acknowledgement:** L1696, 2026-09-10T22:18:09.511Z — "RULINGS on CODEBOOK-AUTH-01 Phase 2 report — accepted at HEAD 13b16a4."
  *(user-authored; architect turn, not CC output)*
- **Trust:** Transcript-derived. Values in this report that trace to a tool call in the source transcript at the stated HEAD may be cited as MEASURED. Classifications, counts arrived at by reading, and judgements are INFERRED until re-measured.
- **Recovered by:** READOUT-ARCHIVE-01, 2026-09-11, recovering session `410e9ecc-67a0-44c6-9523-ffad8f528ca3`.
  Compiled from transcript `/home/ankitsarin/claude-session-archive/4500483d-106b-4e9c-989a-a3314c775cb2.jsonl` by session 410e9ecc… (READOUT-ARCHIVE-01) on 2026-09-11.
  *(files written 2026-09-12 00:49 UTC; the session opened 2026-09-11 23:29 UTC and crossed midnight)*

---

Done and pushed. **HEAD `13b16a4`**, tree clean, local == origin.

---

# CODEBOOK-AUTH-01 PHASE 2 — FINAL REPORT

## Pre-flight (G1)

**P1 — the model is shown the codebook's type, on all 20 fields.** `build_extraction_prompt` (`extractor.py:115-165`) takes the field **set** and its tier grouping from `spec.extraction_schema.fields_by_tier`, and every field **block** from the codebook entry via `_build_field_block`, which renders `cb_field["type"]`. The `sample_size` block, verbatim:

```
- **sample_size** (numeric): Total number of cases, experiments, procedures, or subjects.
```

`numeric` — the codebook's value, not the spec's `text`. **The arms render identically**: `cloud/base.py:90` and `extractor.py:492` call the same function with the same auto-discovery, and the two prompts compare equal. No stop.

**P2 — I2 holds.** Codebook content reaching prompt text: `fields[*]` via `_build_field_block` (`name, type, definition, instruction, valid_values[].value, valid_values[].definition, decision_criteria, examples[].scenario, examples[].value, source_quote_required`), `field_class` via `classes_by_field`, and `escape_token` + `absence_sentinels` rendered into Pass-1 (`prompts.py:182-183, 352`). `contract_unmet_token` reaches no prompt — it is a stored span **value** (`pipeline.py:305`). `version`, `date` and `review` reach **no code at all**. The semantic-hash projection is exactly the specified one.

**P3 — the comment is TRUE.** `completeness.py:98-123` says the guard follows the spec "because that is what the prompt asks for", and P1 confirms the prompt's field *set* is spec-driven while the codebook supplies each block's *content*. Not false today; it converges with the codebook once SCHEMA-DERIVE-01 lands.

## Commits

| commit | what |
|---|---|
| `962e2a1` | **C1–C4, C7** — `engine/core/codebook.py`; `_find_codebook_path` and its `data/` glob deleted; eager validation; identity check; semantic + byte hash; lint at load. `VALID_FIELD_TYPES` and `compute_codebook_sha256` relocated out of `analysis/`. |
| `87093a3` | **C6** — `non_value_tokens_for`'s bare `except`, `absence_sentinels`' default and `completeness`'s swallow removed. Fixes a bug this task introduced one commit earlier (see below). |
| `cc7a65f` | **C5** — migration `012_codebook_provenance`, wired; write sites in extractor, cloud base and the pipeline's run record; lint findings into `review_runs.log`. |
| `02452ee` | **C9, C10** — spec adopts the codebook's type vocabulary on 9 fields; `ExtractionField.type` validated; parity test. |
| `13b16a4` | **C8** — the remaining nine raw readers onto the loader; `_pass2_stability`'s path derived. |

## Gates

| gate | result |
|---|---|
| **G2** suite | **2133 passed, 17 deselected** (was 2,070 / 17) — **+63** |
| **G3** review.db | `99770368 B @ 2026-09-09 19:48:46.784974707 UTC` — **unchanged**; migration applied only to a temp copy |
| **G4** prompt | **byte-identical**, `2f12adc7daa3721935e2494850d7508d3726f100e1c422553836155a86c26762` before and after. **No diff at all** — the brief allowed for the `sample_size` token moving; it does not, because the prompt never read the spec's type |
| **G5** commits | five, separated as specified, pushed |

## Hashes

```
extraction_hash  d311eb20…9a6790  →  fc40fe1340fdc49256efac9bebf23b21aa98692495cd9d599c8dd799e40b8307
screening_hash   0d97b9d61161eeca6c81dd82f895bfb8c6f933b8e8ea23f79056a69f0cf98b90   (unmoved)

live codebook, after C9 (it was not edited — C9 changed the spec):
  codebook_hash    b551ec4393d8c9ef2a4106c704d41b119edd016b97c63b625111a804444fcbad
  codebook_sha256  3807335b31852c03e2141acf217553cfa96ce44ed68e440132d98d921f4e6b5f
```

`extraction_hash` moved because the hashed section genuinely changed; existing extractions now read as stale against it, which is staleness detection working. T6 is re-pinned to the new value with the old one and the reason in the comment.

## Migration

`engine/migrations/012_codebook_provenance.py`, wired into `_run_migrations` (not hand-run like 010/011). On a temp copy of the live `review.db`: six columns added across `extractions`, `cloud_extractions`, `review_runs`; second run a no-op; **190 / 379 / 6 existing rows NULL**, nothing backfilled. `cloud_extractions` is created outside that path, so `engine/cloud/schema.py` carries the columns in its `CREATE` and adds them to a pre-existing table.

## Three things worth your attention

**1. I introduced a bug and caught it one commit later.** Commit 1 resolved the codebook as `data/<review_id>/…`. That ignores `data_root`: a run under an override would read another review's codebook — or none — while writing to the right database. Fixed in `87093a3`; the codebook is now located from the **database's own directory**, which is the review root. The identity check applies where a review id is genuinely authoritative (`load_codebook_for`), not where a path was handed in (`load_codebook`).

**2. C2's "relocate `_VALID_FIELD_TYPES` into the loader" collided with the import graph.** `review_spec` must validate against it, `codebook` imports `review_paths`, and `review_paths` imports `review_spec`. Rather than duplicate the tuple I deferred `codebook`'s `review_paths` import into `codebook_path_for` — the module now has no engine imports at module scope. The vocabulary has exactly one definition, pinned by test (`judge_loader._VALID_FIELD_TYPES is VALID_FIELD_TYPES`).

**3. Eager validation cost 65 test fixtures, and one contract decision went against the judge lane.** Temp reviews had no codebook — a state production cannot reach — so `conftest.py` now writes one beside any temp `ReviewDatabase`, modelling the invariant once instead of in fifteen fixtures; six fixtures that build their tree without `ReviewDatabase` were fixed individually. Two judge-lane details I settled rather than worked around: **`tolerance` is allowed** (a real optional key `judge_loader` reads with a default — refusing it would reject a codebook that lane can already read), and **bare-string `valid_values` stays refused**, because `_build_field_block` renders both `value` and `definition` per entry, so such a codebook raises while building the prompt rather than degrading. `_parse_valid_values` still accepts the shape and is still pinned directly.

## Debt and follow-ons

- **SCHEMA-DERIVE-01** is what the parity test is holding the line for. Until then the two files are parallel, not derived.
- The codebook's `review` key equals `spec.review_id` and, before this task, was read by **nothing**; `load_codebook_for` is now its only consumer.
- `check_schema_parity` still compares only spec-derived hashes arm-to-arm. It does not look at `codebook_hash`. Wiring it is not in this brief.
- **The live migration is not applied.** Until it is, `review.db` has no codebook-provenance columns and any real extraction run against it would fail on the INSERT. That needs your sign-off plus a backup.

---

Stopping here. Not starting SCHEMA-DERIVE-01 and not applying the migration live. Awaiting the next brief or ruling.
