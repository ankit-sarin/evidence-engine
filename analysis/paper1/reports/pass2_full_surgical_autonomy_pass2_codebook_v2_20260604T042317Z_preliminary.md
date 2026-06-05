# Pass 2 full — preliminary report · `surgical_autonomy_pass2_codebook_v2_20260604T042317Z`

**Parent Pass 1 run:** `surgical_autonomy_pass1_20260420T093840Z_1d93b6e5`
**Model:** `gemma3:27b` · codebook_sha256 `9cce10339b17f0d39eb837e381a2a4f154c883c1352aa1af57ee6f80ec3640d7`
**Start:** 2026-06-04T04:23:17.603263+00:00
**End:**   2026-06-04T19:49:45.863251+00:00
**Wall-clock:** 15.44h · latency p50=45.1s · p95=63.0s
**Triples:** 1212 attempted · 1212 succeeded · 0 failed
**Verification rows written:** 3636 (target 3636 = successes × 3)

## Overall verdict distribution

| verdict | n | % |
|---|---:|---:|
| SUPPORTED | 2370 | 65.2% |
| PARTIALLY_SUPPORTED | 873 | 24.0% |
| UNSUPPORTED | 393 | 10.8% |
| **total** | **3636** | 100.0% |

## Verdict distribution by arm

| arm | SUPPORTED | PARTIALLY_SUPPORTED | UNSUPPORTED | total | U% |
|---|---:|---:|---:|---:|---:|
| local | 756 | 289 | 167 | 1212 | 13.8% |
| openai_o4_mini_high | 988 | 155 | 69 | 1212 | 5.7% |
| anthropic_sonnet_4_6 | 626 | 429 | 157 | 1212 | 13.0% |

## Verdict distribution by field_type

| field_type | SUPPORTED | PARTIALLY_SUPPORTED | UNSUPPORTED | total | U% |
|---|---:|---:|---:|---:|---:|
| categorical | 1220 | 393 | 133 | 1746 | 7.6% |
| free_text | 1028 | 470 | 173 | 1671 | 10.4% |
| numeric | 122 | 10 | 87 | 219 | 39.7% |

## Verdict distribution by arm × field (60 cells)

| field | arm | SUPPORTED | PARTIALLY_SUPPORTED | UNSUPPORTED | total |
|---|---|---:|---:|---:|---:|
| autonomy_level | local | 35 | 28 | 12 | 75 |
| autonomy_level | openai_o4_mini_high | 45 | 25 | 5 | 75 |
| autonomy_level | anthropic_sonnet_4_6 | 19 | 37 | 19 | 75 |
| clinical_readiness_assessment | local | 14 | 11 | 3 | 28 |
| clinical_readiness_assessment | openai_o4_mini_high | 25 | 1 | 2 | 28 |
| clinical_readiness_assessment | anthropic_sonnet_4_6 | 18 | 7 | 3 | 28 |
| comparison_to_human | local | 29 | 14 | 14 | 57 |
| comparison_to_human | openai_o4_mini_high | 50 | 0 | 7 | 57 |
| comparison_to_human | anthropic_sonnet_4_6 | 35 | 16 | 6 | 57 |
| country | local | 8 | 2 | 2 | 12 |
| country | openai_o4_mini_high | 11 | 1 | 0 | 12 |
| country | anthropic_sonnet_4_6 | 6 | 5 | 1 | 12 |
| key_limitation | local | 44 | 53 | 20 | 117 |
| key_limitation | openai_o4_mini_high | 67 | 49 | 1 | 117 |
| key_limitation | anthropic_sonnet_4_6 | 52 | 50 | 15 | 117 |
| primary_outcome_metric | local | 29 | 22 | 14 | 65 |
| primary_outcome_metric | openai_o4_mini_high | 55 | 4 | 6 | 65 |
| primary_outcome_metric | anthropic_sonnet_4_6 | 38 | 23 | 4 | 65 |
| primary_outcome_value | local | 52 | 15 | 11 | 78 |
| primary_outcome_value | openai_o4_mini_high | 65 | 6 | 7 | 78 |
| primary_outcome_value | anthropic_sonnet_4_6 | 55 | 14 | 9 | 78 |
| robot_platform | local | 16 | 6 | 6 | 28 |
| robot_platform | openai_o4_mini_high | 28 | 0 | 0 | 28 |
| robot_platform | anthropic_sonnet_4_6 | 19 | 8 | 1 | 28 |
| sample_size | local | 62 | 1 | 10 | 73 |
| sample_size | openai_o4_mini_high | 50 | 2 | 21 | 73 |
| sample_size | anthropic_sonnet_4_6 | 10 | 7 | 56 | 73 |
| secondary_outcomes | local | 80 | 43 | 22 | 145 |
| secondary_outcomes | openai_o4_mini_high | 134 | 10 | 1 | 145 |
| secondary_outcomes | anthropic_sonnet_4_6 | 49 | 80 | 16 | 145 |
| study_design | local | 64 | 7 | 3 | 74 |
| study_design | openai_o4_mini_high | 65 | 6 | 3 | 74 |
| study_design | anthropic_sonnet_4_6 | 67 | 7 | 0 | 74 |
| study_type | local | 0 | 2 | 0 | 2 |
| study_type | openai_o4_mini_high | 2 | 0 | 0 | 2 |
| study_type | anthropic_sonnet_4_6 | 1 | 1 | 0 | 2 |
| surgical_domain | local | 33 | 7 | 7 | 47 |
| surgical_domain | openai_o4_mini_high | 45 | 1 | 1 | 47 |
| surgical_domain | anthropic_sonnet_4_6 | 39 | 5 | 3 | 47 |
| system_maturity | local | 60 | 31 | 5 | 96 |
| system_maturity | openai_o4_mini_high | 73 | 19 | 4 | 96 |
| system_maturity | anthropic_sonnet_4_6 | 71 | 23 | 2 | 96 |
| task_execute | local | 25 | 6 | 1 | 32 |
| task_execute | openai_o4_mini_high | 32 | 0 | 0 | 32 |
| task_execute | anthropic_sonnet_4_6 | 21 | 11 | 0 | 32 |
| task_generate | local | 57 | 4 | 8 | 69 |
| task_generate | openai_o4_mini_high | 61 | 6 | 2 | 69 |
| task_generate | anthropic_sonnet_4_6 | 21 | 46 | 2 | 69 |
| task_monitor | local | 43 | 6 | 11 | 60 |
| task_monitor | openai_o4_mini_high | 49 | 7 | 4 | 60 |
| task_monitor | anthropic_sonnet_4_6 | 22 | 33 | 5 | 60 |
| task_performed | local | 41 | 11 | 3 | 55 |
| task_performed | openai_o4_mini_high | 47 | 7 | 1 | 55 |
| task_performed | anthropic_sonnet_4_6 | 18 | 31 | 6 | 55 |
| task_select | local | 31 | 4 | 5 | 40 |
| task_select | openai_o4_mini_high | 38 | 1 | 1 | 40 |
| task_select | anthropic_sonnet_4_6 | 28 | 7 | 5 | 40 |
| validation_setting | local | 33 | 16 | 10 | 59 |
| validation_setting | openai_o4_mini_high | 46 | 10 | 3 | 59 |
| validation_setting | anthropic_sonnet_4_6 | 37 | 18 | 4 | 59 |

## Top 10 papers by UNSUPPORTED count

| paper_id | UNSUPPORTED |
|---|---:|
| 455 | 10 |
| 11 | 9 |
| 39 | 8 |
| 9 | 6 |
| 81 | 6 |
| 493 | 6 |
| 507 | 6 |
| 14 | 5 |
| 296 | 5 |
| 380 | 5 |

## Windowed-path triples

- **Count:** 65/1212 triples (5.4%)
- **Verdict distribution (arm-rows in windowed triples):**

| verdict | n |
|---|---:|
| SUPPORTED | 123 |
| PARTIALLY_SUPPORTED | 37 |
| UNSUPPORTED | 35 |

## Short-circuit firing

- **Arm-rows with short_circuit=True:** 350/3636 (9.6%)

---
_PI review gate: do NOT proceed to Pass 2 interpretation or audit sampling until this report is reviewed._
