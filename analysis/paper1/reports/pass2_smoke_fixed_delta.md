# Pass 2 smoke — before/after delta (absence-aware fix)

**Pre-fix run:**  `surgical_autonomy_pass2_smoke_20260421T122916Z`
**Post-fix run:** `surgical_autonomy_pass2_smoke_fixed_20260421T165202Z`

## Aggregate verdict counts

| verdict | before | after | Δ |
|---|---:|---:|---:|
| SUPPORTED | 31 | 43 | +12 |
| PARTIALLY_SUPPORTED | 13 | 11 | -2 |
| UNSUPPORTED | 28 | 18 | -10 |

## Original 28 UNSUPPORTED cells — new verdicts

`absence?` = the arm value is a codebook absence sentinel (NR / N/A / NOT_FOUND / empty). Rows where `before→after` flips indicate the rubric change took effect.

| # | paper | field | arm | value | absence? | before | after | status |
|---|---|---|---|---|:---:|---|---|---|
| 1 | 9 | `robot_platform` | local | Smart Tissue Autonomous Robot (STAR); KUKA LBR Med | N | UNSUPPORTED | SUPPORTED | **flipped** |
| 2 | 9 | `sample_size` | anthropic_sonnet_4_6 | 18 | N | UNSUPPORTED | UNSUPPORTED | preserved |
| 3 | 9 | `sample_size` | local | N/A | Y | UNSUPPORTED | SUPPORTED | **flipped** |
| 4 | 9 | `sample_size` | openai_o4_mini_high | 9 | N | UNSUPPORTED | UNSUPPORTED | preserved |
| 5 | 9 | `study_design` | local | Feasibility study | N | UNSUPPORTED | PARTIALLY_SUPPORTED | **flipped** |
| 6 | 9 | `system_maturity` | local | Research prototype (hardware) | N | UNSUPPORTED | SUPPORTED | **flipped** |
| 7 | 12 | `robot_platform` | local | da Vinci Surgical System; da Vinci Research Kit... | N | UNSUPPORTED | UNSUPPORTED | preserved |
| 8 | 17 | `task_performed` | local | exchanging a red ring between robotic tools | N | UNSUPPORTED | SUPPORTED | **flipped** |
| 9 | 39 | `autonomy_level` | local | 3 | N | UNSUPPORTED | UNSUPPORTED | preserved |
| 10 | 67 | `sample_size` | anthropic_sonnet_4_6 | 1800 | N | UNSUPPORTED | UNSUPPORTED | preserved |
| 11 | 67 | `sample_size` | local | NR | Y | UNSUPPORTED | SUPPORTED | **flipped** |
| 12 | 295 | `sample_size` | openai_o4_mini_high | NOT_FOUND | Y | UNSUPPORTED | SUPPORTED | **flipped** |
| 13 | 383 | `sample_size` | anthropic_sonnet_4_6 | 19 | N | UNSUPPORTED | UNSUPPORTED | preserved |
| 14 | 383 | `sample_size` | local | 19 | N | UNSUPPORTED | UNSUPPORTED | preserved |
| 15 | 407 | `comparison_to_human` | local | Autonomous: 34.8s vs Manual: 28.3s; Autonomous:... | N | UNSUPPORTED | UNSUPPORTED | preserved |
| 16 | 411 | `sample_size` | anthropic_sonnet_4_6 | 30 | N | UNSUPPORTED | SUPPORTED | **flipped** |
| 17 | 411 | `sample_size` | openai_o4_mini_high | 30 | N | UNSUPPORTED | SUPPORTED | **flipped** |
| 18 | 458 | `validation_setting` | local | Ex vivo | N | UNSUPPORTED | UNSUPPORTED | preserved |
| 19 | 478 | `sample_size` | anthropic_sonnet_4_6 | 140 | N | UNSUPPORTED | UNSUPPORTED | preserved |
| 20 | 557 | `primary_outcome_value` | local | NR | Y | UNSUPPORTED | SUPPORTED | **flipped** |
| 21 | 570 | `secondary_outcomes` | local | Tissue tracking error: Translation Error: 0.711... | N | UNSUPPORTED | UNSUPPORTED | preserved |
| 22 | 693 | `sample_size` | anthropic_sonnet_4_6 | 31 | N | UNSUPPORTED | UNSUPPORTED | preserved |
| 23 | 719 | `autonomy_level` | local | _(empty)_ | Y | UNSUPPORTED | SUPPORTED | **flipped** |
| 24 | 719 | `autonomy_level` | openai_o4_mini_high | NOT_FOUND | Y | UNSUPPORTED | SUPPORTED | **flipped** |
| 25 | 719 | `key_limitation` | local | _(empty)_ | Y | UNSUPPORTED | SUPPORTED | **flipped** |
| 26 | 719 | `key_limitation` | openai_o4_mini_high | NOT_FOUND | Y | UNSUPPORTED | SUPPORTED | **flipped** |
| 27 | 738 | `validation_setting` | anthropic_sonnet_4_6 | Computational/Virtual | N | UNSUPPORTED | UNSUPPORTED | preserved |
| 28 | 755 | `sample_size` | anthropic_sonnet_4_6 | 200 | N | UNSUPPORTED | UNSUPPORTED | preserved |

## Summary of the 28 original UNSUPPORTED cells

- **Absence cells:** 8
  - flipped to SUPPORTED / PARTIALLY_SUPPORTED: **8**
  - still UNSUPPORTED (legit — Gemma found the field IS reported): **0**
- **Positive-claim cells:** 20
  - still UNSUPPORTED (regression-safe): **14**
  - flipped (non-absence flip): **6**

## Newly UNSUPPORTED cells in post-fix run

Cells that were SUPPORTED/PARTIALLY_SUPPORTED before, now UNSUPPORTED.

| paper | field | arm | value | absence? | pre | reasoning |
|---|---|---|---|:---:|---|---|
| 39 | `autonomy_level` | openai_o4_mini_high | Mixed/Multiple | N | PARTIALLY_SUPPORTED | The text does not mention a 'Mixed/Multiple' autonomy level. It describes robotic assistance and supervised autonomy, but doesn't combine these into a mixed category. |
| 292 | `comparison_to_human` | local | Autonomous system outperforms human i... | N | PARTIALLY_SUPPORTED | The source states the agents outperformed humans, but does not specify *how* they outperformed them (e.g., task completion time and total distance). This is an addition beyond w... |
| 295 | `sample_size` | anthropic_sonnet_4_6 | 4 | N | PARTIALLY_SUPPORTED | The text mentions two successful ex vivo incisions, but does not state a total sample size. The value '4' appears to be a misinterpretation or fabrication. |
| 295 | `sample_size` | local | 4 | N | PARTIALLY_SUPPORTED | The text mentions a study, but does not state the sample size. The value '4' appears to be a misinterpretation or fabrication. |

## Example: flipped absence cells (now SUPPORTED)

### paper 9 / `sample_size` / local — value `N/A`
- **before:** UNSUPPORTED — The source text does not mention any sample size, and the arm provides no supporting span. The claim of 'N/A' is not grounded in the provided text.
- **after:** SUPPORTED — (no reasoning — absence rubric allows this on SUPPORTED)

### paper 67 / `sample_size` / local — value `NR`
- **before:** UNSUPPORTED — The source mentions the CPU and GPU used for training and testing, but does not provide a sample size. The value 'NR' is not found in the text.
- **after:** SUPPORTED — (no reasoning — absence rubric allows this on SUPPORTED)

### paper 295 / `sample_size` / openai_o4_mini_high — value `NOT_FOUND`
- **before:** UNSUPPORTED — The source does not mention a sample size. The arm reports 'NOT_FOUND' but provides no supporting span, indicating a lack of evidence in the text.
- **after:** SUPPORTED — (no reasoning — absence rubric allows this on SUPPORTED)

### paper 557 / `primary_outcome_value` / local — value `NR`
- **before:** UNSUPPORTED — The source states that the exact numeric results are not provided, directly contradicting the 'NR' value. The arm correctly identifies the lack of numeric results but reports it...
- **after:** SUPPORTED — (no reasoning — absence rubric allows this on SUPPORTED)

## Verification checkpoint (per task spec)

- **Gate 8 (all 8 gates pass):** see post-fix report — ALL GATES PASS.
- **Absence cells flipped away from UNSUPPORTED:** 8/8 — every absence-sentinel cell now returns SUPPORTED (none remain UNSUPPORTED).
- **Positive-claim UNSUPPORTED preserved:** 14/20 of the 20 non-absence originals carried over. Net post-fix UNSUPPORTED = 18 (14 preserved + 4 newly-flagged positive-claim cells).
- **Note on task's predicted counts:** the task description predicted "10 absence cases" and "18 non-absence UNSUPPORTED". Actual absence count (by sentinel detection) is 8 — the task's list included two positive-value cells (#14 `383/sample_size/local=19` and #18 `458/validation_setting/local=Ex vivo`) which are NOT codebook absence sentinels. Those remain UNSUPPORTED in the fixed run, as expected for positive fabrications.
- **New UNSUPPORTED (not in original 28):** 4 cells that were PARTIALLY_SUPPORTED pre-fix moved to UNSUPPORTED post-fix (e.g., paper 295/sample_size: '4' flagged as fabricated). Not caused by the rubric change (none are absence claims); these are Gemma drawing a sharper line on second run.
