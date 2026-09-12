# INVENTORY-01 — recovered read-out

**Provenance header.** This file is a verbatim recovery of one Claude Code
assistant message. Nothing below the header has been edited, summarised or
reformatted.

- **Source transcript:** `/home/ankitsarin/claude-session-archive/4500483d-106b-4e9c-989a-a3314c775cb2.jsonl`
- **Session id:** `4500483d-106b-4e9c-989a-a3314c775cb2`
  - Note: the unified plan v46 attributes this session's work to 2885f251; the work was done in 4500483d, its continuation.
- **Source message line:** L768 (NDJSON record, `type: assistant`)
- **Brief at transcript line:** L601, 2026-09-10T00:09:34.512Z
- **Message timestamp:** 2026-09-10T00:24:12.041Z
- **HEAD stated in read-out:** **HEAD `3ab8cff`**, pushed, **local == origin/main**, tree clean.
- **Architect acknowledgement:** L773, 2026-09-10T00:37:58.545Z — "RULINGS on INVENTORY-01 report — accepted at HEAD 3ab8cff."
  *(user-authored; architect turn, not CC output)*
- **Trust:** Transcript-derived. Values in this report that trace to a tool call in the source transcript at the stated HEAD may be cited as MEASURED. Classifications, counts arrived at by reading, and judgements are INFERRED until re-measured.
- **Recovered by:** READOUT-ARCHIVE-01, 2026-09-11, recovering session `410e9ecc-67a0-44c6-9523-ffad8f528ca3`.
  Compiled from transcript `/home/ankitsarin/claude-session-archive/4500483d-106b-4e9c-989a-a3314c775cb2.jsonl` by session 410e9ecc… (READOUT-ARCHIVE-01) on 2026-09-11.
  *(files written 2026-09-12 00:49 UTC; the session opened 2026-09-11 23:29 UTC and crossed midnight)*

---

INVENTORY-01 complete and pushed. Report below.

---

# INVENTORY-01 — FINAL REPORT

**HEAD `3ab8cff`**, pushed, **local == origin/main**, tree clean.

## 🔴 One ruling clause was unperformable, and one premise did not survive contact with disk

**R2's second clause has no referent.** `CLAUDE.md` carries **no table count** — the "24 tables / 84,400 rows" figure lives in `primer.md`, and it is **already correct there**: lines 10–11 already state that it holds only when `sqlite_sequence` is counted and that user tables alone are 23 / 84,395. There was nothing stale to correct and nothing in `CLAUDE.md` to correct it in. I applied R2's first clause in full and did not invent an edit; the commit subject is `docs(claude-md): --review invocation after SPEC-AUTH-01`, dropping `; table count`.

**INVENTORY-01's C1 premise that `data/*/` names review ids is false.** `data/` holds **eight** subdirectories and exactly **one** is a review:

| directory | what it actually is |
|---|---|
| `surgical_autonomy` | the review — `review.db`, 99,770,368 B |
| `test_verify` | **a leaked test database** — `review.db`, 131,072 B, 2026-03-13 |
| `data` | nested `data/data/surgical_autonomy` — a data-root composition slip |
| `my_review`, `myreview`, `review`, `test_review` | `logs/` only — default and test review names that reached the live tree |
| `backups` | DB backups, and now the retired `_original` spec (R3) |

The cause is structural: **`ReviewDatabase.__init__` calls `root.mkdir(parents=True, exist_ok=True)`**, so any run or test that constructed one without a temp `data_root` left a directory behind. Taking C1 literally would have made "literal review ids" flag every occurrence of the strings `data`, `review`, `my_review` and `backups` across the codebase — a useless inventory.

I did not silently substitute my own definition. The tool computes **both** sets, uses `spec stems ∪ {data/<d> with a review.db}` for classification, and **emits every exclusion with its reason** into the JSON and the markdown, so what was excluded and why is visible in the artifact rather than buried in this report. The criterion is stated in the module docstring and pinned by a test. **Your call whether the filter is right; the seven debris directories are a separate finding and I have not touched them.**

`I1` and `I2` both **hold**: argparse only across all three trees, zero non-literal flag names, zero syntax errors; scan is **0.81 s**.

## Summary block (as generated)

| count | value |
|---|---:|
| files scanned | 195 |
| entry points | 97 |
| entry points with review name flag | 74 |
| entry points with spec flag | 29 |
| entry points name only | 45 |
| entry points constructing reviewdatabase | 37 |
| name only constructing reviewdatabase | 20 |
| **db before spec scopes** | **0** |
| files calling resolver | 33 |
| files calling load review spec directly | 8 |
| raw yaml load sites | 13 |
| files with raw yaml loads | 12 |
| path construction sites in code | 67 |
| **fstring spec path sites** | **0** |
| literal review id sites in code | 32 |
| review id constants | 8 |
| default review named constants | 7 |
| unparsed sites | 0 |

## Reconciliation (G1)

| figure | hand-built | now | |
|---|---:|---:|---|
| argparse entry points naming a review | 74 | 74 | matches |
| of those, spec-bearing | 29 | 29 | matches |
| of those, name-only | 45 | 45 | matches |
| name-only, constructing ReviewDatabase | 20 | 20 | matches |
| raw yaml load sites | 13 | 13 | matches |
| DEFAULT_REVIEW constants | 7 | 7 | matches |
| **entry points constructing ReviewDatabase** | 35 | **37** | the hand scan enumerated by argparse **flag**, so it could not see a file that opens a database with no `--review` at all |
| **f-string spec-path builders** | 19 | **0** | SPEC-AUTH-01 moved every one onto the resolver |

**The two the hand scan could not see, and one of them matters.** `scripts/_pass2_stability.py:60` constructs **`ReviewDatabase("surgical_autonomy")`** — the live review database, hardcoded, no flag, no `data_root`. `scripts/test_e2e_search_screen.py:63` constructs `ReviewDatabase("e2e_test")`, which would create `data/e2e_test/` in the live tree on any run. Recorded, **not acted on** (out of scope).

Two figures deliberately have no baseline row: **entry points (97)** counts anything with argparse flags *or* a `__main__` guard, a wider net than the hand scan's review-naming CLIs; and the hand-built "12 of the 13 raw YAML loads are codebook readers" is a semantic judgement about what a file *means*, which the tool does not make — it reports the call site and the target expression.

**`review_id_constants` is 8 against 7 `DEFAULT_REVIEW`:** the eighth is `_FALLBACK_REVIEW_ID` in `engine/analysis/normalize.py:48`, which I introduced in SPEC-AUTH-01. The tool counts both, and the reconciliation row keys on the `DEFAULT_REVIEW` name specifically.

## UNPARSED

**None.** Every scanned file parsed, and every argparse flag name was a string literal.

## Three classification rules worth knowing, all structural

Each came from a test failing against my first implementation, not from reading the output and tidying it:

1. **An f-string's literal fragments are fragments, not strings.** Without this one built path counted three times; `path_construction_sites_in_code` was inflated 86 → 71.
2. **A string passed to an exception constructor or a logger is a `message`.** `fstring_spec_path_sites` read **1**, and the one hit was `review_spec.py:479` — the error text telling an operator the naming convention. A "hand-built spec paths" metric whose floor is above its target teaches the reader to ignore it. After the rule: 0, and 71 → 67.
3. **Review ids match by equality, not containment.** A docstring mentioning a review is prose about it.

**Order is computed per enclosing function**, not per file — a helper defined above `main()` would otherwise make `main()` read as database-before-spec, which is exactly what I had to disambiguate by eye in Phase 1.

**The drift check compares only the JSON's `data` block.** `meta.generated_at_commit` moves with every commit and would make the check fail on its own success.

**One thing to expect:** the drift test reads `data/*/`, so if a future test leaks a directory into the live tree the gate goes red with `data/ subdirectories changed`. That is the alarm that would have caught `test_verify` and the other five, so I left it armed rather than freezing the set.

## Gates

| gate | result |
|---|---|
| **G1** reconciliation | 6 of 8 match; both differences explained in the generated table |
| **G2** suite | **2070 passed, 17 deselected in 257.37s** (was 2,052 / 17) — **+18** |
| **G3** review.db | `99770368 B @ 2026-09-09 19:48:46.784974707 UTC` — unchanged; the gate created no new `data/` directories |
| **G4** commits | three, separated, pushed |

**T3 scan time: 0.81 s** over three runs (asserted < 5 s in the suite).

## Commits

| commit | what |
|---|---|
| `967d135` | `docs(claude-md)` — R2 (10 command lines, 7 blocks) + R3 (move + `.gitignore:8`) |
| `5f4de81` | `feat(tools)` — `engine/tools/inventory.py`, `engine/tools/__init__.py`, `tests/test_inventory.py` (18 tests) |
| `3ab8cff` | `docs(inventory)` — `docs/inventory/entry_points.{md,json}` |

**One note on the split you asked for:** a drift test cannot be green before the artifact it guards exists, so `5f4de81` is transiently red on its two drift tests until `3ab8cff` adds the generated files. Both were pushed together and `--check` reports `inventory in sync` at HEAD. I followed the separation you specified rather than collapsing the two.

## R3 result

```
$ ls review_specs/
.gitkeep
surgical_autonomy.yaml
```

`surgical_autonomy_v1_original.yaml` moved to `data/backups/`, content untouched, still ignored (now by the blanket `data/` rule rather than a line naming it). **I1 verified before editing:** `.gitignore:8` named only that file, and it was untracked. `review_specs/` now holds exactly what the resolver can resolve, which is what makes `review_specs/<review_id>.yaml` readable as a rule rather than a convention.

## R4 recorded, not acted on

Missing-spec clean exit → PATH-AUTH-01. Effective-configuration record → EFFECTIVE-SPEC-01, after MONITOR-WIRE-01.

---

Stopping here. Not starting CODEBOOK-AUTH-01; awaiting the next brief or ruling.
