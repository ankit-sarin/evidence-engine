# SCREEN-AUTH-01 Phase 2g P3 — the arm-D re-smoke

**Date:** 2026-09-19/20. **Run:** 23:34:07 → 00:30:52 UTC, status COMPLETE, no pause.
**Harness commits:** `9a69fbe` (arm sets, corrections, rule, stability) and `19241b9` (arm D's
tree rule). **Pre-registration:** `c600e8d`, extended in `9a69fbe` — both before any arm-D model
call. **Set:** the same 86 papers, `papers_86.jsonl` sha256 `8217b887…9115`.

| arm | tree | what it is | screening_hash | R1 |
|---|---|---|---|---|
| A | worktree `83defc5` | pre-fold engine and spec | `d804ced7…1ae9` | `33bdeea9…abee3` |
| B | worktree `61326fa` | the exact 2f arm-B tree | `d563134e…6a29` | `e02ce2c9…4f649` |
| D | repo, inputs = `56c5c57` | the exclusion-basis fold | `e8fa9719…3596` | `a84e1a72…8642` |

Identity gate **8/8 before any model call**; call options identical across arms
(`{"temperature": 0}`, `think=false`, the `format` schema, no seed, no `num_ctx`).

Every value below is MEASURED from the committed outputs in
`docs/session-reports/screen-auth-2g/`. This report does not rule on D.

## 1. The pre-registered rule: NO-GO

Scored under O1–O5 as committed before the run (`arm_D_preregistration.md`).

| clause | measured | threshold | result |
|---|---|---|---|
| **O2** — SILENT papers back at IN or FLAGGED | **1 of 12** | ≥ 10 | **FAIL** |
| **O3** — EVIDENCED papers must not reach IN | **0 of 5** at IN; 0 FLAGGED | == 0 | **PASS** |
| **O4** — flag rate materially below A | D **0.4419** vs A **0.8953**, margin 0.4534 | D ≤ A − 0.15 | **PASS** |

**Overall: NO-GO**, on clause 1.

The twelve SILENT papers under D: 786 FLAGGED; 693, 325, 774, 429, 583, 222, 655, 139, 344, 112
and 765 all **OUT**. The five EVIDENCED papers (569, 170, 702, 737, 598) are all OUT.

## 2. Stability control — the reruns are exact

| arm | this run vs committed 2f | disagreements |
|---|---|---|
| A (`83defc5`) | **86/86 = 1.0000** | none |
| B (`61326fa`) | **86/86 = 1.0000** | none |

Both arms reproduce their 2f outcomes paper for paper, so nondeterminism contributes **zero**
paper-level movement here, and the D-vs-B difference below is a property of the fold, not noise.
Arm B ran from a worktree at `61326fa`, the exact 2f tree, so this is a pure rerun.

## 3. Scoring (2f format)

PI reference: the April workbook **corrected** by `pi_verdicts_2f_part1.csv` — 569, 170 and 702
move include → exclude, so the reference is **79 exclude / 7 include** (uncorrected: 76/10).
Every metric in this section uses the corrected reference; the uncorrected disagreement list is
committed beside it.

### Final outcome × PI decision

| arm | IN inc/exc | OUT inc/exc | FLAGGED inc/exc |
|---|---|---|---|
| A | 2 / 2 | 0 / 5 | 5 / 72 |
| B | 1 / 0 | 4 / 43 | 2 / 36 |
| D | **0 / 0** | 4 / 44 | 3 / 35 |

### Sensitivity (n=7) and specificity (n=79), Wilson 95% CI

| arm | FLAGGED as | sens | spec | balanced |
|---|---|---|---|---|
| A | include | 1.0 [0.646, 1.0] | 0.063 [0.027, 0.140] | 0.532 |
| A | exclude | 0.286 [0.082, 0.641] | 0.975 [0.912, 0.993] | 0.630 |
| A | dropped (n=9) | 1.0 [0.342, 1.0] | 0.714 [0.359, 0.918] | 0.857 |
| B | include | 0.429 [0.158, 0.750] | 0.544 [0.435, 0.650] | 0.486 |
| B | exclude | 0.143 [0.026, 0.513] | 1.0 [0.954, 1.0] | 0.571 |
| B | dropped (n=48) | 0.200 [0.036, 0.625] | 1.0 [0.918, 1.0] | 0.600 |
| D | include | 0.429 [0.158, 0.750] | 0.557 [0.447, 0.661] | 0.493 |
| D | exclude | **0.0** [0.0, 0.354] | 1.0 [0.954, 1.0] | 0.500 |
| D | dropped (n=48) | **0.0** [0.0, 0.490] | 1.0 [0.920, 1.0] | 0.500 |

### Rates

| arm | finals | primary | flag rate | d1≠d2 | verifier overturn | calls |
|---|---|---|---|---|---|---|
| A | FLAGGED 77, IN 4, OUT 5 | IN 81, OUT 5 | 0.8953 | 0/86 | 77/81 = 0.9506 | 253 |
| B | OUT 47, FLAGGED 38, IN 1 | OUT 47, IN 39 | 0.4419 | 0/86 | 38/39 = 0.9744 | 211 |
| D | OUT 48, FLAGGED 38, **IN 0** | OUT 48, IN 37, FLAGGED 1 | 0.4419 | 1/86 = 0.0116 | **37/37 = 1.0000** | 209 |

0 parse failures and 0 errors in all three arms; 86/86 papers per arm; no forced calls;
0 reprocessed-after-interrupt; `call_pattern_problems` empty for A, B and D.

### Agreement

| pair | final | κ | primary | κ |
|---|---|---|---|---|
| A–B | 43/86 = 0.500 | 0.126 | 44/86 = 0.512 | 0.098 |
| **B–D** | **73/86 = 0.849** | 0.698 | 73/86 = 0.849 | 0.698 |
| A–D | 43/86 = 0.500 | 0.126 | 42/86 = 0.488 | 0.090 |

D sits with B, not with A.

## 4. Disagreement lists

Committed as CSV and Markdown: `disagreements_A_vs_B` (43), `disagreements_B_vs_D` (13),
`disagreements_A_vs_D` (43), `disagreements_any_arm_vs_PI` (81, corrected) and
`disagreements_any_arm_vs_PI_uncorrected` (81).

**B → D, all 13:** 6 × OUT → FLAGGED (336, 450, 613, 363, 161, 786), 6 × FLAGGED → OUT
(354, 264, 413, 280, 333, 736), 1 × IN → OUT (469).

**The ten April includes** (PI label now in the last column):

| paper | ee | A | B | D | PI now |
|---|---|---|---|---|---|
| 579 | EE-427 | FLAGGED | FLAGGED | FLAGGED | include |
| 583 | EE-431 | FLAGGED | OUT | OUT | include |
| 222 | EE-092 | FLAGGED | OUT | OUT | include |
| 569 | EE-417 | FLAGGED | OUT | OUT | exclude |
| 190 | EE-080 | FLAGGED | FLAGGED | FLAGGED | include |
| 786 | EE-634 | FLAGGED | OUT | FLAGGED | include |
| 170 | EE-070 | FLAGGED | OUT | OUT | exclude |
| 469 | EE-317 | IN | IN | **OUT** | include |
| 702 | EE-550 | IN | OUT | OUT | exclude |
| 693 | EE-541 | IN | OUT | OUT | include |

## 5. The 18 priority papers

| # | paper | ee | verdict / detail | A | B | D | D d1/d2 |
|---:|---|---|---|---|---|---|---|
| 1 | 693 | EE-541 | SILENT / S-AUTONOMY | IN | OUT | OUT | exclude/exclude |
| 2 | 261 | EE-109 | MISREAD / M-SPECIALTY | FLAGGED | OUT | OUT | exclude/exclude |
| 3 | 325 | EE-173 | SILENT / S-AUTONOMY | FLAGGED | OUT | OUT | exclude/exclude |
| 4 | 774 | EE-622 | SILENT / S-AUTONOMY | FLAGGED | OUT | OUT | exclude/exclude |
| 5 | 429 | EE-277 | SILENT / S-AUTONOMY | FLAGGED | OUT | OUT | exclude/exclude |
| 6 | 583 | EE-431 | SILENT / S-AUTONOMY | FLAGGED | OUT | OUT | exclude/exclude |
| 7 | 222 | EE-092 | SILENT / S-AUTONOMY | FLAGGED | OUT | OUT | exclude/exclude |
| 8 | 569 | EE-417 | EVIDENCED / E-SPECIALTY | FLAGGED | OUT | OUT | exclude/exclude |
| 9 | 786 | EE-634 | SILENT / S-PHYSICAL-TASK | FLAGGED | OUT | **FLAGGED** | include/include |
| 10 | 170 | EE-070 | EVIDENCED / E-TELEOPERATED | FLAGGED | OUT | OUT | exclude/exclude |
| 11 | 702 | EE-550 | EVIDENCED / E-NO-PHYSICAL-TASK | IN | OUT | OUT | exclude/exclude |
| 12 | 655 | EE-503 | SILENT / S-AUTONOMY | FLAGGED | OUT | OUT | exclude/exclude |
| 13 | 139 | EE-057 | SILENT / S-AUTONOMY | FLAGGED | OUT | OUT | exclude/exclude |
| 14 | 344 | EE-192 | SILENT / S-INSUFFICIENT | FLAGGED | OUT | OUT | exclude/exclude |
| 15 | 112 | EE-048 | SILENT / S-SPECIALTY | FLAGGED | OUT | OUT | exclude/exclude |
| 16 | 765 | EE-613 | SILENT / S-AUTONOMY | FLAGGED | OUT | OUT | exclude/exclude |
| 17 | 737 | EE-585 | EVIDENCED / E-SPECIALTY | FLAGGED | OUT | OUT | exclude/exclude |
| 18 | 598 | EE-446 | EVIDENCED / E-SPECIALTY | FLAGGED | OUT | OUT | exclude/exclude |

D's full pass-1 rationales for all 18 are in `arm_D_papers.csv` (`d1_rationale`, untruncated).
The eleven SILENT papers D excluded give, in D's own words, grounds of the form the bar
forbids — quoted verbatim, uninterpreted:

- **693** — "does not mention any level of surgical autonomy (Levels 1–5 per the Yang et al.
  taxonomy) or any autonomous/semi-autonomous robotic component…"
- **222** — "does not mention any level of autonomy (Levels 1–5) or any autonomous/semi-autonomous
  robotic component… Additionally, the paper does not involve a physical surgical task…"
- **112** — "The title and abstract do not mention any level of surgical autonomy… The abstract
  does not provide sufficient information to determine eligibility for inclusion."
- **344** — "it does not mention any autonomous or semi-autonomous robotic component… The title
  and abstract do not meet the inclusion criteria…"
- **765** — "does not mention any autonomous or semi-autonomous surgical robotic system…"
- **774** — "does not mention any autonomous or semi-autonomous robotic component…"
- **737** (EVIDENCED) — "mention neurosurgery, which is an excluded specialty. The paper does not
  provide sufficient information to determine if it involves autonomous or semi-autonomous
  robotic systems…"

**786 is the one SILENT paper D kept:** its primary passed it (include/include, citing an
autonomous laparoscope positioner), and the **verifier** flagged it — "It is a positioning
system for the laparoscope, not an autonomous surgical tool. This fails test #1."

## 6. Timing and resident models

Run 23:34:07 → 00:30:52 UTC, **56 min 45 s**, no pause, outside 06:30–09:30 UTC throughout.

| model | calls | mean | median | max | total | max load |
|---|---|---|---|---|---|---|
| qwen3:8b | 516 | 3.453 s | 3.335 s | 7.117 s | 1781.9 s | 2.395 s |
| gemma3:27b | 157 | 10.278 s | 10.197 s | 17.033 s | 1613.7 s | 4.728 s |

Per-arm elapsed: A primary 8m52s, B primary 10m40s, D primary 10m20s; A verifier 14m01s,
B verifier 6m40s, D verifier 6m20s. `ollama ps` at every phase boundary held only qwen3:8b
(11.5 GB, context 40960) or gemma3:27b (30.5 GB, context 131072). Two swaps, as designed.

`review.db`: **101,978,112 B, mtime 2026-09-11 02:00:52.636956943**, before and after the run
and at every phase boundary (the orchestrator's `--watch-file`).

## 7. Also found

1. **D's exclusions still rest on silence, in D's own rationales.** The bar renders in D's prompt
   (surface P1, frozen `5f96f61c…`), and the model still writes "does not mention any level of
   autonomy" and "does not provide sufficient information" as its ground. The fold changed what
   the prompt says; these rationales are what the model did with it. Reported as measurement, not
   as a cause.
2. **D never reaches IN.** 0 of 86, against B's 1 and A's 4, and the verifier overturned
   **37 of 37** primary INs. Under FLAGGED-dropped scoring, D's sensitivity is 0 by construction.
3. **D's flag rate equals B's exactly** (0.4419, 38 papers each), but not on the same papers:
   6 OUT → FLAGGED and 6 FLAGGED → OUT.
4. **One d1≠d2 in D** (paper 469, the 15,770-char abstract), against 0 in both A and B.
5. **The corrections changed no disagreement count** (81 either way), because all three corrected
   papers were already counted as inconsistent under both references.
6. **A scorer defect, found and fixed after the run, before scoring:** `timing()` iterated the
   hard-coded 2f arm names and raised `KeyError: 'C'` on the 2g set. Fixed to follow the arm set;
   Gate 1 (the committed 2f outputs, no flags) re-checked byte for byte afterwards.

## 8. Commits and gates

| commit | what | standard gate before it |
|---|---|---|
| `9a69fbe` | pre-flight: arm sets, `--corrections`, `--verdicts`, `--stability-baseline`, O1–O5, 21 tests | 2,328 / 17 (504/594/377/437/416) |
| `19241b9` | arm D's rule: `repo_inputs_equal_commit` at `56c5c57` | 2,329 / 17 (504/594/377/438/416) |
| this one | run outputs, scoring, the `timing()` fix, this report | recorded in the commit message |

Gate 1 holds throughout: the scorer with no flags reproduces all 11 committed 2f outputs byte for
byte, as a check and as a test.

## 9. Not done

No ruling on D. The arm-D re-smoke is measured and reported; what follows from it is the
architect's. Untouched: the verifier and FT exclusion-basis rendering, SCREEN-INPUT-01, 2d
provenance, 2e categorizer, and Part 2 of the reading sheet.
