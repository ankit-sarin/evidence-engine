# Pass 2 smoke — `surgical_autonomy_pass2_smoke_fixed_20260421T165202Z`

**Parent Pass 1 run:** `surgical_autonomy_pass1_20260420T093840Z_1d93b6e5`
**Model:** `gemma3:27b` · codebook_sha256 `ae66c12926951649e6da5747a06d6f81f584c3af53cb3f1979136986230da87d`

## Gate pass/fail

- **PASS** · 1. parse_cleanliness — 24/24 triples parsed cleanly; 0 failures
- **PASS** · 2. invariant_compliance — 0 UNSUPPORTED rows violating reasoning/hypothesis NOT NULL
- **PASS** · 3. row_count — 72 rows in fabrication_verifications (expected 72)
- **PASS** · 4. unsupported_present — 18 UNSUPPORTED verdicts across 24 triples
- **PASS** · 5. short_circuit_firing — 14 arm-rows with short_circuit=True
- **PASS** · 6. windowing_exercised — 3/24 triples used windowed source
- **PASS** · 7. latency — p50=46.7s (limit 75), p95=57.0s (limit 120)
- **PASS** · 8. arm_verdict_balance — no arm is 100% SUPPORTED (per-arm counts: {'openai_o4_mini_high': {'UNSUPPORTED': 2, 'SUPPORTED': 21, 'PARTIALLY_SUPPORTED': 1}, 'local': {'UNSUPPORTED': 8, 'SUPPORTED': 14, 'PARTIALLY_SUPPORTED': 2}, 'anthropic_sonnet_4_6': {'SUPPORTED': 8, 'UNSUPPORTED': 8, 'PARTIALLY_SUPPORTED': 8}})

**Overall: ALL GATES PASS — cleared for Task #10 full run**

## Selection

Selection SQL (deterministic; stratified in Python):

```sql
-- Smoke triple universe (run against judge_ratings
-- with pass1_run_id = :pass1_run_id):
--   high:   SELECT paper_id, field_name, field_type FROM judge_ratings
--           WHERE run_id=? AND pass1_fabrication_risk='high'
--           ORDER BY paper_id, field_name;
--   medium: SELECT paper_id, field_name, field_type FROM judge_ratings
--           WHERE run_id=? AND pass1_fabrication_risk='medium'
--           ORDER BY paper_id, field_name;
-- Stratification is applied in-Python (see pass2_smoke.select_triples).
-- Within each stratum, candidates are sorted by (paper_id ASC as int,
-- field_name ASC) and the first N unseen are taken.
```

All strata populated to target count.

### 24 selected triples

| # | paper_id | field_name | risk | field_type | stratum | windowed | sc_arms |
|---|---|---|---|---|---|---|---|
| 1 | 39 | autonomy_level | high | categorical | high | Y | - |
| 2 | 67 | sample_size | high | numeric | high | N | openai_o4_mini_high |
| 3 | 292 | comparison_to_human | high | free_text | high | N | openai_o4_mini_high,anthropic_sonnet_4_6 |
| 4 | 295 | sample_size | high | numeric | high | N | - |
| 5 | 383 | sample_size | high | numeric | high | N | - |
| 6 | 407 | comparison_to_human | high | free_text | high | N | anthropic_sonnet_4_6 |
| 7 | 411 | sample_size | high | numeric | high | N | local |
| 8 | 458 | validation_setting | high | categorical | high | N | openai_o4_mini_high |
| 9 | 478 | sample_size | high | numeric | high | N | local,openai_o4_mini_high |
| 10 | 557 | primary_outcome_value | high | free_text | high | N | - |
| 11 | 570 | secondary_outcomes | high | free_text | high | N | - |
| 12 | 693 | sample_size | high | numeric | high | N | local,openai_o4_mini_high |
| 13 | 738 | validation_setting | high | categorical | high | N | - |
| 14 | 755 | sample_size | high | numeric | high | N | local,openai_o4_mini_high |
| 15 | 14 | task_performed | medium | free_text | task_performed_saturated | N | - |
| 16 | 17 | task_performed | medium | free_text | task_performed_saturated | N | openai_o4_mini_high |
| 17 | 9 | robot_platform | medium | free_text | robot_platform_saturated | N | - |
| 18 | 12 | robot_platform | medium | free_text | robot_platform_saturated | N | - |
| 19 | 9 | study_design | medium | categorical | categorical_any | N | - |
| 20 | 9 | system_maturity | medium | categorical | categorical_any | N | - |
| 21 | 9 | sample_size | medium | numeric | numeric | N | - |
| 22 | 719 | autonomy_level | medium | categorical | paper719_windowing | Y | - |
| 23 | 719 | key_limitation | medium | free_text | paper719_windowing | Y | - |
| 24 | 9 | task_generate | medium | categorical | short_circuit_eligible | N | openai_o4_mini_high |

## Per-triple results

| # | paper/field | latency | src_toks | local | o4mini | sonnet | overall_fab |
|---|---|---:|---:|---|---|---|:---:|
| 1 | 39/autonomy_level | 33.6s | 1255 | UNSUPPORTED | UNSUPPORTED | SUPPORTED | Y |
| 2 | 67/sample_size | 45.2s | 11804 | SUPPORTED | SUPPORTED | UNSUPPORTED | Y |
| 3 | 292/comparison_to_human | 43.9s | 12650 | UNSUPPORTED | SUPPORTED | SUPPORTED | Y |
| 4 | 295/sample_size | 41.9s | 12246 | UNSUPPORTED | SUPPORTED | UNSUPPORTED | Y |
| 5 | 383/sample_size | 48.7s | 13050 | UNSUPPORTED | SUPPORTED | UNSUPPORTED | Y |
| 6 | 407/comparison_to_human | 47.8s | 9085 | UNSUPPORTED | SUPPORTED | SUPPORTED | Y |
| 7 | 411/sample_size | 37.9s | 12065 | SUPPORTED | SUPPORTED | SUPPORTED | N |
| 8 | 458/validation_setting | 54.2s | 13275 | UNSUPPORTED | SUPPORTED | PARTIALLY_SUPPORTED | Y |
| 9 | 478/sample_size | 43.1s | 10243 | SUPPORTED | SUPPORTED | UNSUPPORTED | Y |
| 10 | 557/primary_outcome_value | 45.3s | 11202 | SUPPORTED | SUPPORTED | PARTIALLY_SUPPORTED | N |
| 11 | 570/secondary_outcomes | 62.7s | 9346 | UNSUPPORTED | SUPPORTED | PARTIALLY_SUPPORTED | Y |
| 12 | 693/sample_size | 37.3s | 7239 | SUPPORTED | SUPPORTED | UNSUPPORTED | Y |
| 13 | 738/validation_setting | 46.7s | 9427 | PARTIALLY_SUPPORTED | SUPPORTED | UNSUPPORTED | Y |
| 14 | 755/sample_size | 45.7s | 14750 | SUPPORTED | SUPPORTED | UNSUPPORTED | Y |
| 15 | 14/task_performed | 40.6s | 6067 | SUPPORTED | SUPPORTED | PARTIALLY_SUPPORTED | N |
| 16 | 17/task_performed | 40.7s | 7464 | SUPPORTED | SUPPORTED | PARTIALLY_SUPPORTED | N |
| 17 | 9/robot_platform | 57.0s | 18175 | SUPPORTED | SUPPORTED | SUPPORTED | N |
| 18 | 12/robot_platform | 41.0s | 8741 | UNSUPPORTED | SUPPORTED | SUPPORTED | Y |
| 19 | 9/study_design | 53.7s | 18175 | PARTIALLY_SUPPORTED | SUPPORTED | SUPPORTED | N |
| 20 | 9/system_maturity | 53.5s | 18175 | SUPPORTED | PARTIALLY_SUPPORTED | SUPPORTED | N |
| 21 | 9/sample_size | 52.3s | 18175 | SUPPORTED | UNSUPPORTED | UNSUPPORTED | Y |
| 22 | 719/autonomy_level | 54.9s | 20000 | SUPPORTED | SUPPORTED | PARTIALLY_SUPPORTED | N |
| 23 | 719/key_limitation | 55.9s | 20000 | SUPPORTED | SUPPORTED | PARTIALLY_SUPPORTED | N |
| 24 | 9/task_generate | 50.5s | 18175 | SUPPORTED | SUPPORTED | PARTIALLY_SUPPORTED | N |

## Verdict × arm cross-tab

| arm | SUPPORTED | PARTIALLY_SUPPORTED | UNSUPPORTED | total |
|---|---:|---:|---:|---:|
| local | 14 | 2 | 8 | 24 |
| openai_o4_mini_high | 21 | 1 | 2 | 24 |
| anthropic_sonnet_4_6 | 8 | 8 | 8 | 24 |

## Short-circuit firing

- 14/72 arm-rows eligible (19.4%)

## Example reasoning traces

### supported — 39/autonomy_level (arm=anthropic_sonnet_4_6)
- verification_span: 'Level-3 autonomy is demonstrated on phantom using fluoroscopy in a real-world end-use environment.'
- reasoning: 'The text explicitly states that level-3 autonomy is demonstrated, aligning with the claimed value and supporting span.'
- fabrication_hypothesis: None

### unsupported — 39/autonomy_level (arm=openai_o4_mini_high)
- verification_span: None
- reasoning: "The text does not mention a 'Mixed/Multiple' autonomy level. It describes robotic assistance and supervised autonomy, but doesn't combine these into a mixed category."
- fabrication_hypothesis: 'plausible-sounding default based on combining observed autonomy features'

### short-circuit fired — 67/sample_size (arm=openai_o4_mini_high)
- verdict: SUPPORTED
- verification_span: 'Our training set in total contained 1360 images and the testing set contained 440 images as shown in Table 2.'
- reasoning: 'The text explicitly states the testing set contained 440 images.'

## Anomalies

- none

