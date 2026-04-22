# Pass 2 smoke — `surgical_autonomy_pass2_smoke_20260421T122916Z`

**Parent Pass 1 run:** `surgical_autonomy_pass1_20260420T093840Z_1d93b6e5`
**Model:** `gemma3:27b` · codebook_sha256 `82e901509f8b3d73057983f39e85bcd7120f389cbd344b52cabf457f5031e9cb`

## Gate pass/fail

- **PASS** · 1. parse_cleanliness — 24/24 triples parsed cleanly; 0 failures
- **PASS** · 2. invariant_compliance — 0 UNSUPPORTED rows violating reasoning/hypothesis NOT NULL
- **PASS** · 3. row_count — 72 rows in fabrication_verifications (expected 72)
- **PASS** · 4. unsupported_present — 28 UNSUPPORTED verdicts across 24 triples
- **PASS** · 5. short_circuit_firing — 14 arm-rows with short_circuit=True
- **PASS** · 6. windowing_exercised — 3/24 triples used windowed source
- **PASS** · 7. latency — p50=48.7s (limit 75), p95=62.4s (limit 120)
- **PASS** · 8. arm_verdict_balance — no arm is 100% SUPPORTED (per-arm counts: {'local': {'UNSUPPORTED': 15, 'PARTIALLY_SUPPORTED': 3, 'SUPPORTED': 6}, 'openai_o4_mini_high': {'PARTIALLY_SUPPORTED': 1, 'SUPPORTED': 18, 'UNSUPPORTED': 5}, 'anthropic_sonnet_4_6': {'SUPPORTED': 7, 'UNSUPPORTED': 8, 'PARTIALLY_SUPPORTED': 9}})

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
| 1 | 39/autonomy_level | 40.8s | 1255 | UNSUPPORTED | PARTIALLY_SUPPORTED | SUPPORTED | Y |
| 2 | 67/sample_size | 46.0s | 11804 | UNSUPPORTED | SUPPORTED | UNSUPPORTED | Y |
| 3 | 292/comparison_to_human | 45.0s | 12650 | PARTIALLY_SUPPORTED | SUPPORTED | SUPPORTED | N |
| 4 | 295/sample_size | 50.7s | 12246 | PARTIALLY_SUPPORTED | UNSUPPORTED | PARTIALLY_SUPPORTED | Y |
| 5 | 383/sample_size | 48.7s | 13050 | UNSUPPORTED | SUPPORTED | UNSUPPORTED | Y |
| 6 | 407/comparison_to_human | 49.8s | 9085 | UNSUPPORTED | SUPPORTED | SUPPORTED | Y |
| 7 | 411/sample_size | 41.3s | 12065 | SUPPORTED | UNSUPPORTED | UNSUPPORTED | Y |
| 8 | 458/validation_setting | 54.2s | 13275 | UNSUPPORTED | SUPPORTED | PARTIALLY_SUPPORTED | Y |
| 9 | 478/sample_size | 37.1s | 10243 | SUPPORTED | SUPPORTED | UNSUPPORTED | Y |
| 10 | 557/primary_outcome_value | 42.7s | 11202 | UNSUPPORTED | SUPPORTED | PARTIALLY_SUPPORTED | Y |
| 11 | 570/secondary_outcomes | 55.7s | 9346 | UNSUPPORTED | SUPPORTED | PARTIALLY_SUPPORTED | Y |
| 12 | 693/sample_size | 31.8s | 7239 | SUPPORTED | SUPPORTED | UNSUPPORTED | Y |
| 13 | 738/validation_setting | 46.6s | 9427 | PARTIALLY_SUPPORTED | SUPPORTED | UNSUPPORTED | Y |
| 14 | 755/sample_size | 45.7s | 14750 | SUPPORTED | SUPPORTED | UNSUPPORTED | Y |
| 15 | 14/task_performed | 40.6s | 6067 | SUPPORTED | SUPPORTED | PARTIALLY_SUPPORTED | N |
| 16 | 17/task_performed | 45.9s | 7464 | UNSUPPORTED | SUPPORTED | PARTIALLY_SUPPORTED | Y |
| 17 | 9/robot_platform | 56.4s | 18175 | UNSUPPORTED | SUPPORTED | SUPPORTED | Y |
| 18 | 12/robot_platform | 41.0s | 8741 | UNSUPPORTED | SUPPORTED | SUPPORTED | Y |
| 19 | 9/study_design | 54.5s | 18175 | UNSUPPORTED | SUPPORTED | SUPPORTED | Y |
| 20 | 9/system_maturity | 51.2s | 18175 | UNSUPPORTED | SUPPORTED | SUPPORTED | Y |
| 21 | 9/sample_size | 59.9s | 18175 | UNSUPPORTED | UNSUPPORTED | UNSUPPORTED | Y |
| 22 | 719/autonomy_level | 65.5s | 20000 | UNSUPPORTED | UNSUPPORTED | PARTIALLY_SUPPORTED | Y |
| 23 | 719/key_limitation | 62.4s | 20000 | UNSUPPORTED | UNSUPPORTED | PARTIALLY_SUPPORTED | Y |
| 24 | 9/task_generate | 51.2s | 18175 | SUPPORTED | SUPPORTED | PARTIALLY_SUPPORTED | N |

## Verdict × arm cross-tab

| arm | SUPPORTED | PARTIALLY_SUPPORTED | UNSUPPORTED | total |
|---|---:|---:|---:|---:|
| local | 6 | 3 | 15 | 24 |
| openai_o4_mini_high | 18 | 1 | 5 | 24 |
| anthropic_sonnet_4_6 | 7 | 9 | 8 | 24 |

## Short-circuit firing

- 14/72 arm-rows eligible (19.4%)

## Example reasoning traces

### supported — 39/autonomy_level (arm=anthropic_sonnet_4_6)
- verification_span: 'It is concluded that robot-assisted TAVI is technically feasible in-vivo, and presents a strong case for a clinically meaningful application of level-3 autonomy.'
- reasoning: 'The source explicitly states that the demonstrated system represents level-3 autonomy.'
- fabrication_hypothesis: None

### unsupported — 39/autonomy_level (arm=local)
- verification_span: None
- reasoning: 'The source does not mention any specific autonomy level. The supporting span only discusses the need for innovation in valve deployment.'
- fabrication_hypothesis: 'Plausible-sounding default value assigned without source support.'

### short-circuit fired — 67/sample_size (arm=openai_o4_mini_high)
- verdict: SUPPORTED
- verification_span: 'Our training set in total contained 1360 images and the testing set contained 440 images as shown in Table 2.'
- reasoning: None

## Anomalies

- none

