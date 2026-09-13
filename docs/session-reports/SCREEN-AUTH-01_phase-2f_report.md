# SCREEN-AUTH-01 Phase 2f — three-arm abstract-screening smoke

**Date:** 2026-09-13. **Run:** 04:24:43 → 05:20:34 UTC, status COMPLETE, no pause.
**Harness commit:** `e2adff3`. **Outputs commit:** `8a7dc4f`. **Set:** the 86-paper PI-labelled
specialty-rescreen workbook (PI_decision 76 exclude / 10 include).

Every value below is **MEASURED** from the committed outputs in
`docs/session-reports/screen-auth-2f-smoke/` unless tagged **INFERRED**. Reason
categories are not scored (out of scope). This is a smoke: it scores decisions and
lists disagreements; the ruling on 2c follows it.

## What ran

| arm | engine tree | spec | screening_hash |
|---|---|---|---|
| A | `83defc5` worktree (pre-fold) | its own `review_specs/surgical_autonomy.yaml` | `d804ced7bd4c45e872524ebfd4d55f3d093ff9d18db28cfee8e2f67f4c571ae9` |
| B | HEAD (engine/ and review_specs/ byte-identical to `61326fa`) | live spec | `d563134ed6a92a8d0ee3e877574fcf844f7094889ec411f88e3285bb58a06a29` |
| C | HEAD | live spec + `abstract_primary` on `exc-abstract-only`, `exc-teleoperation-only`, `exc-analysis-only`, `exc-no-autonomy` (four added lines, nothing else; `full/armC_spec.yaml`, sha256 `50b178a6…efc7`) | `e23b74de9275df8655d3efeacc2ca769542ac41c325d9691c1b2fac11b0f0120` |

* **Call pattern, each arm, each paper** — the runner's own (`screener.run_screening` /
  `run_verification`), through that arm's own `screen_paper`: primary pass 1 and 2 on
  `qwen3:8b` (include+include → IN, exclude+exclude → OUT, otherwise FLAGGED; a malformed
  pass 1 skips pass 2); verifier on `gemma3:27b` for primary IN only (include keeps IN,
  anything else → FLAGGED). Final outcome ∈ {IN, OUT, FLAGGED}.
* **Options, identical across arms:** `options={"temperature": 0}`, `think=False`, `format` =
  `ScreeningDecision` schema, model per role from the spec. No seed, no `num_ctx`.
* **Phase-batched:** all three arms' primaries, then all three arms' verifiers. Each
  arm-phase is its own process run with `python -P` and `PYTHONPATH` at the arm's tree root;
  the worker refuses if `engine` resolves outside it.
* **Input text:** `papers_86.jsonl` ({id, title, abstract}), exported read-only from the
  review database by `analysis/eval/screen2f_export.py`, sha256
  `8217b887e1c49aa7036a350694e25665d498f0c15a40b797e472125a5db09115` — the same value in
  `papers_86_export_manifest.json`, `preflight/run_summary.json` and `full/run_summary.json`.
  The workbook's abstracts are 500-char prefixes; the database text was used. Cross-checks:
  86/86 workbook abstracts are prefixes of the export; 73/73 matched `abstracts.jsonl`
  entries are byte-equal.

## Gates

| gate | result |
|---|---|
| 1 placeholder identity | **pass**, 11/11 checks, before any model call, in the pre-flight and again in the full run. A `33bdeea9`/`2c89ef61`; B `e02ce2c9` (= frozen R1) / `bc36e291` (= R2); C primary `0af23cef` differs from B only by four added exclusion lines, C verifier = R2 |
| 2 call pattern, each paper once | **pass** — `call_pattern_problems` empty for A, B, C; 86 primary results per arm with claims = results; verifier results only on primary IN (A 81, B 39, C 35); no forced calls in the full run; 0 reprocessed-after-interrupt. Pre-flight forced verifier call on paper 579: not needed in any arm (579 was primary IN in all three), so no extra call was made |
| parse failures ≤ 2% per arm | **pass** — 0 of 253 (A), 0 of 211 (B), 0 of 207 (C) calls; 0 client errors, 0 client retries |
| 3 database untouched | **pass** — review database 101,978,112 B, mtime 2026-09-11 02:00:52.636956943 before and after the export, the pre-flight, the full run and every gate; harness and scorer sources carry no reference to it (AST tests); the export script is the named exception |
| 4 scoring, three lists | **pass** — below |
| 5 quiet window | **pass** — no call inside 06:30–09:30 UTC; the run never paused (`pauses: []`; `full/pause_ollama_ps.txt` was never created) |
| 6 tree clean, pushed, worktree removed | worktree removed after scoring; commits below |

## Timing and resident models

| phase | wall | arm A | arm B | arm C |
|---|---|---|---|---|
| primary (qwen3:8b, 516 calls) | 29 m 56 s | 8 m 28 s | 10 m 18 s | 11 m 10 s |
| verifier (gemma3:27b, 155 calls) | 25 m 54 s | 13 m 48 s (81) | 6 m 29 s (39) | 5 m 38 s (35) |

Per call: qwen3:8b mean 3.47 s, median 3.39 s, max 6.62 s; gemma3:27b mean 10.00 s,
median 9.85 s, max 16.77 s. **Two swaps, each visible as the one call per model with a load
over 1 s:** gemma→qwen 2.34 s at the first primary call, qwen→gemma 4.57 s at the first
verifier call. Pre-flight projection was 72 min worst case; actual 55 m 51 s (the verifier
ran on 155 of a possible 258).

`ollama ps` at every phase boundary (name, bytes, context):

| at (UTC) | boundary | resident |
|---|---|---|
| 04:24:44 | primary start | gemma3:27b, 30,459,802,624, 131072 (left by the pre-flight) |
| 04:54:40 | primary end | qwen3:8b, 11,454,255,232, 40960 |
| 04:54:40 | verifier start | qwen3:8b, 11,454,255,232, 40960 |
| 05:20:34 | verifier end | gemma3:27b, 30,459,802,624, 131072 |

Resident at close: **gemma3:27b**, 30 GB, context 131072 (the server's default; the pipeline
sets no `num_ctx` either). Ollama 0.21.0, never restarted.

## Scoring (2b)

### Final outcome × PI decision

| final | A inc | A exc | B inc | B exc | C inc | C exc |
|---|---:|---:|---:|---:|---:|---:|
| IN | 3 | 1 | 1 | 0 | 0 | 0 |
| OUT | 0 | 5 | 7 | 40 | 9 | 40 |
| FLAGGED | 7 | 70 | 2 | 36 | 1 | 36 |
| ERROR | 0 | 0 | 0 | 0 | 0 | 0 |

### Sensitivity (n=10) and specificity (n=76), Wilson 95% CI

| arm | FLAGGED treated as | TP/FN/TN/FP | sensitivity | specificity | balanced |
|---|---|---|---|---|---|
| A | include | 10/0/5/71 | 1.000 [0.723, 1.000] | 0.066 [0.028, 0.145] | 0.533 |
| A | exclude | 3/7/75/1 | 0.300 [0.108, 0.603] | 0.987 [0.929, 0.998] | 0.643 |
| A | dropped (9 decided) | 3/0/5/1 | 1.000 [0.439, 1.000] | 0.833 [0.437, 0.970] | 0.917 |
| B | include | 3/7/40/36 | 0.300 [0.108, 0.603] | 0.526 [0.416, 0.635] | 0.413 |
| B | exclude | 1/9/76/0 | 0.100 [0.018, 0.404] | 1.000 [0.952, 1.000] | 0.550 |
| B | dropped (48 decided) | 1/7/40/0 | 0.125 [0.022, 0.471] | 1.000 [0.912, 1.000] | 0.563 |
| C | include | 1/9/40/36 | 0.100 [0.018, 0.404] | 0.526 [0.416, 0.635] | 0.313 |
| C | exclude | 0/10/76/0 | 0.000 [0.000, 0.278] | 1.000 [0.952, 1.000] | 0.500 |
| C | dropped (49 decided) | 0/9/40/0 | 0.000 [0.000, 0.299] | 1.000 [0.912, 1.000] | 0.500 |

### Rates

| arm | primary IN / OUT / FLAGGED | final IN / OUT / FLAGGED | flag rate | d1≠d2 | verifier overturn |
|---|---|---|---|---|---|
| A | 81 / 5 / 0 | 4 / 5 / 77 | 89.5% | 0 / 86 | 77 / 81 (95.1%) |
| B | 39 / 47 / 0 | 1 / 47 / 38 | 44.2% | 0 / 86 | 38 / 39 (97.4%) |
| C | 35 / 49 / 2 | 0 / 49 / 37 | 43.0% | 2 / 86 | 35 / 35 (100%) |

### The primary stage by itself

The verifier overturns 95–100% of primary IN in every arm, so the final outcome is mostly the
verifier's. The primary stage is where 2c's abstract-primary change acts:

| arm | PI includes kept (primary IN) | PI excludes removed (primary OUT) | PI excludes primary FLAGGED |
|---|---:|---:|---:|
| A | 10 / 10 | 5 / 76 | 0 |
| B | 3 / 10 (579, 190, 469) | 40 / 76 | 0 |
| C | 1 / 10 (579) | 40 / 76 | 2 (261, 264) |

### Agreement between arms

| pair | final: agree / 86, % , κ | primary: agree / 86, %, κ |
|---|---|---|
| A–B | 43, 50.0%, 0.126 | 44, 51.2%, 0.098 |
| B–C | 80, 93.0%, 0.860 | 79, 91.9%, 0.839 |
| A–C | 41, 47.7%, 0.100 | 40, 46.5%, 0.083 |

Final-outcome transitions — A→B: FLAGGED→FLAGGED 37, **FLAGGED→OUT 40** (35 PI exclude, 5 PI
include), IN→OUT 2 (both PI include: 702 EE-550, 693 EE-541), IN→FLAGGED 1 (264, PI exclude),
IN→IN 1 (469, PI include), OUT→OUT 5. At primary, A→B is IN→OUT 42 and OUT→IN 0.
B→C: OUT→OUT 45, FLAGGED→FLAGGED 35, FLAGGED→OUT 3, IN→OUT 1, OUT→FLAGGED 2.

### The ten PI includes

| paper | A primary → verifier → final | B | C |
|---|---|---|---|
| 579 EE-427 | IN → exclude → FLAGGED | IN → exclude → FLAGGED | IN → exclude → FLAGGED |
| 583 EE-431 | IN → exclude → FLAGGED | OUT | OUT |
| 222 EE-092 | IN → exclude → FLAGGED | OUT | OUT |
| 569 EE-417 | IN → exclude → FLAGGED | OUT | OUT |
| 190 EE-080 | IN → exclude → FLAGGED | IN → exclude → FLAGGED | OUT |
| 786 EE-634 | IN → exclude → FLAGGED | OUT | OUT |
| 170 EE-070 | IN → exclude → FLAGGED | OUT | OUT |
| 469 EE-317 | IN → include → **IN** | IN → include → **IN** | OUT |
| 702 EE-550 | IN → include → **IN** | OUT | OUT |
| 693 EE-541 | IN → include → **IN** | OUT | OUT |

### Does eight exclusions at abstract primary, instead of four, change decisions? (B vs C)

Six of 86 final outcomes differ (93.0% agreement, κ 0.860):

| paper | PI | B | C | what moved |
|---|---|---|---|---|
| 190 EE-080 | include | FLAGGED (verifier) | OUT | primary IN → OUT |
| 469 EE-317 | include | IN | OUT | primary IN → OUT; C: "does not present any original research" |
| 736 EE-584 | exclude | FLAGGED (verifier) | OUT | primary IN → OUT |
| 728 EE-576 | exclude | FLAGGED (verifier) | OUT | primary IN → OUT |
| 261 EE-109 | exclude | OUT | FLAGGED | C pass 1 include, pass 2 exclude |
| 341 EE-189 | exclude | OUT | FLAGGED (verifier) | primary OUT → IN, verifier excluded |

Both papers C loses are PI includes; under FLAGGED→include, sensitivity falls from 3/10 to
1/10 with specificity unchanged at 40/76.

## Disagreement lists (2c)

On final outcome, with PI_notes and each arm's final outcome and the rationale of the call
that decided it. FLAGGED is never PI-consistent.

| list | papers | files |
|---|---:|---|
| any arm ≠ PI | 81 | `disagreements_any_arm_vs_PI.{md,csv}` |
| A ≠ B | 43 | `disagreements_A_vs_B.{md,csv}` |
| B ≠ C | 6 | `disagreements_B_vs_C.{md,csv}` (reproduced above) |

Per-arm per-paper detail (d1, d2, both rationales, verifier decision and rationale, request
hashes, wall time): `arm_{A,B,C}_papers.csv`. Raw calls, including full model output and
metrics: `full/arm_*/primary.jsonl` and `verifier.jsonl`.

## The workbook's own primary/verifier columns vs arm A (2d, descriptive)

**What produced the workbook (MEASURED from git):** `scripts/rescreen_with_specialty.py`, added
in `d5fa5bc` (2026-03-13 01:02 UTC); the workbook's mtime is 2026-03-13 04:16 UTC. That script
runs the same dual-primary + verifier pattern through `screen_paper`, tagging decisions
`specialty_rescreen`. The spec tracked at that commit is `review_specs/surgical_autonomy_v1.yaml`,
with `screening_models: primary "qwen3:8b", verification "gemma3:27b"` and top-level
`specialty_scope` and `screening_criteria` sections. Eight commits changed the screener or the
spec between `d5fa5bc` and `83defc5`.

**Comparison (MEASURED):**

* Primary: workbook include with arm A include/include 78; workbook include with arm A
  exclude/exclude 5; workbook exclude with arm A include/include 3.
* Verifier: workbook exclude with arm A exclude 76; include/include 3; workbook exclude with
  arm A include 1; workbook include with arm A exclude 1; arm A never called the verifier on
  5 (primary OUT).
* Rationale text. The workbook's reasoning columns are cut at 300 characters (75 at 300,
  11 at 299). Arm A's verifier rationale is identical to the workbook's on 17 papers and
  starts with the workbook's text on 57 more (74 of 81 comparable). Arm A's primary rationale
  starts with the workbook's text on 36 (pass 1) / 38 (pass 2), shares only the first 40
  characters on 42 / 41, and differs on 8 / 7.

**INFERRED:** the workbook's verifier column came from gemma3:27b on a request essentially the
same as arm A's verifier request — near-verbatim text on 74 of 81 papers is hard to produce
otherwise. Its primary column came from qwen3:8b on a request close to, but not identical
with, arm A's primary request; 2b's relocation was designed to preserve rendered wording, and
the text divergence after the first sentence on about half the papers suggests at least one
of the eight intervening commits changed what the primary pass reads. Which one is not
established here.

## Found, not asked

1. **INVENTORY-02 — the "spec-bearing" reconciliation row counts the wrong population.**
   `engine/tools/inventory.py` computes `spec_bearing` over `entry_points` (every entry
   point), but `docs/inventory/entry_points.md` labels it "of those" under "argparse entry
   points naming a review". Adding the worker (`--spec` required, no `--review`) changed the
   generated row from

   `| of those, spec-bearing | 29 | 28 | differs — unexplained, investigate |`

   to

   `| of those, spec-bearing | 29 | 29 | matches |`

   The "matches" is an accident of an unrelated file. Not fixed; the flag was not renamed to
   hide it.
2. **The f-string spec-path sentinel was tripped by the first harness draft** (0 → 1) — an
   error message containing `review_specs/`. The harness was changed, not the sentinel:
   spec paths now come from `spec_path_for`, and the count is 0 at `e2adff3` and after.
3. **Temperature 0 did not make the two primary passes agree in arm C.** Pass 1 and pass 2 sent
   byte-identical requests for every paper in every arm (checked on the stored request
   hashes). Arm C still disagreed with itself on 2 of 86 papers — 261 EE-109 (include, then
   exclude) and 264 EE-112 (exclude, then include). Arms A and B: 0 of 86. Where the
   decisions agreed, the generations often did not: the two passes produced different
   `eval_count`s on 36 of 86 papers in A, 44 in B and 53 in C (e.g. A paper 261: 90 and 100
   tokens). Temperature 0 fixed the decision most of the time, not the output. No call in the
   pre-flight or the full run logged a client warning or retry.
4. **Paper 737 EE-585's database abstract is the 19-character string `Original scientific`.**
   J1 passed because it is not empty. Arm A included it on both primary passes (the verifier
   flagged it); B and C excluded it at primary. PI decision: exclude.
5. **The committed inventory has to be regenerated whenever an entry point is added**; its
   drift test is at the standard gate. Done for `e2adff3` (four modules) and for the outputs
   commit (the scorer).
6. **The scorer and its tests are in the outputs commit, not the harness commit.** They were
   written while the run went and produce the files committed beside them.
7. **The session's scratch directory disappeared mid-run** (session context continued); the
   run itself was unaffected. Gate chunk lists for the outputs commit were written to the
   gitignored `logs/gate_2f_b/` instead. The first gate launched for this report's commit
   carried two flags outside the convention (`-p no:cacheprovider` on four chunks,
   `--color=no`); it was stopped before completing and rerun exactly per convention
   (`logs/gate_2f_c2/`). The figure in the commits table is the rerun's.

## Commits

| commit | what | standard gate before it |
|---|---|---|
| `e2adff3` (a) | export script, harness, orchestrator, T1 (31 tests), inventory | 2,252 passed / 17 deselected (475/498/428/435/416; 0/0/10/6/1) |
| `8a7dc4f` (b) | pre-flight and full-run outputs, export and manifest, scoring outputs, scorer + 7 tests, inventory | 2,259 passed / 17 deselected (475/565/361/442/416; 0/0/10/6/1) |
| this file (c) | this report | 2,259 passed / 17 deselected (475/565/361/442/416; 0/0/10/6/1) |

## Not done

2d provenance, 2e categorizer, INVENTORY-02, SPEC-GUARD-01, FT-STATUS-01, any prompt or spec
change, reason-category scoring. No write to the review database.
