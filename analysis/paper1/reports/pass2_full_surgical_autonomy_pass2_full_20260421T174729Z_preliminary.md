# Pass 2 full — preliminary report · `surgical_autonomy_pass2_full_20260421T174729Z`

**Parent Pass 1 run:** `surgical_autonomy_pass1_20260420T093840Z_1d93b6e5`
**Model:** `gemma3:27b` · codebook_sha256 `ae66c12926951649e6da5747a06d6f81f584c3af53cb3f1979136986230da87d`
**Start:** 2026-04-21T17:47:29.579430+00:00
**End:**   2026-04-22T09:10:54.990008+00:00
**Wall-clock:** 15.39h · latency p50=45.1s · p95=62.2s
**Triples:** 1212 attempted · 1211 succeeded · 1 failed
**Verification rows written:** 3633 (target 3633 = successes × 3)

## Overall verdict distribution

| verdict | n | % |
|---|---:|---:|
| SUPPORTED | 2082 | 57.3% |
| PARTIALLY_SUPPORTED | 1038 | 28.6% |
| UNSUPPORTED | 513 | 14.1% |
| **total** | **3633** | 100.0% |

## Verdict distribution by arm

| arm | SUPPORTED | PARTIALLY_SUPPORTED | UNSUPPORTED | total | U% |
|---|---:|---:|---:|---:|---:|
| local | 666 | 342 | 203 | 1211 | 16.8% |
| openai_o4_mini_high | 930 | 193 | 88 | 1211 | 7.3% |
| anthropic_sonnet_4_6 | 486 | 503 | 222 | 1211 | 18.3% |

## Verdict distribution by field_type

| field_type | SUPPORTED | PARTIALLY_SUPPORTED | UNSUPPORTED | total | U% |
|---|---:|---:|---:|---:|---:|
| categorical | 986 | 538 | 222 | 1746 | 12.7% |
| free_text | 974 | 483 | 211 | 1668 | 12.6% |
| numeric | 122 | 17 | 80 | 219 | 36.5% |

## Verdict distribution by arm × field (60 cells)

| field | arm | SUPPORTED | PARTIALLY_SUPPORTED | UNSUPPORTED | total |
|---|---|---:|---:|---:|---:|
| autonomy_level | local | 21 | 44 | 10 | 75 |
| autonomy_level | openai_o4_mini_high | 27 | 43 | 5 | 75 |
| autonomy_level | anthropic_sonnet_4_6 | 11 | 48 | 16 | 75 |
| clinical_readiness_assessment | local | 5 | 17 | 6 | 28 |
| clinical_readiness_assessment | openai_o4_mini_high | 19 | 4 | 5 | 28 |
| clinical_readiness_assessment | anthropic_sonnet_4_6 | 3 | 17 | 8 | 28 |
| comparison_to_human | local | 24 | 14 | 19 | 57 |
| comparison_to_human | openai_o4_mini_high | 42 | 2 | 13 | 57 |
| comparison_to_human | anthropic_sonnet_4_6 | 29 | 22 | 6 | 57 |
| country | local | 8 | 2 | 2 | 12 |
| country | openai_o4_mini_high | 11 | 0 | 1 | 12 |
| country | anthropic_sonnet_4_6 | 7 | 2 | 3 | 12 |
| key_limitation | local | 46 | 51 | 20 | 117 |
| key_limitation | openai_o4_mini_high | 81 | 36 | 0 | 117 |
| key_limitation | anthropic_sonnet_4_6 | 39 | 58 | 20 | 117 |
| primary_outcome_metric | local | 30 | 22 | 13 | 65 |
| primary_outcome_metric | openai_o4_mini_high | 56 | 5 | 4 | 65 |
| primary_outcome_metric | anthropic_sonnet_4_6 | 36 | 26 | 3 | 65 |
| primary_outcome_value | local | 48 | 17 | 12 | 77 |
| primary_outcome_value | openai_o4_mini_high | 64 | 4 | 9 | 77 |
| primary_outcome_value | anthropic_sonnet_4_6 | 52 | 17 | 8 | 77 |
| robot_platform | local | 14 | 8 | 6 | 28 |
| robot_platform | openai_o4_mini_high | 28 | 0 | 0 | 28 |
| robot_platform | anthropic_sonnet_4_6 | 15 | 9 | 4 | 28 |
| sample_size | local | 62 | 2 | 9 | 73 |
| sample_size | openai_o4_mini_high | 50 | 4 | 19 | 73 |
| sample_size | anthropic_sonnet_4_6 | 10 | 11 | 52 | 73 |
| secondary_outcomes | local | 75 | 41 | 29 | 145 |
| secondary_outcomes | openai_o4_mini_high | 126 | 17 | 2 | 145 |
| secondary_outcomes | anthropic_sonnet_4_6 | 41 | 79 | 25 | 145 |
| study_design | local | 44 | 24 | 6 | 74 |
| study_design | openai_o4_mini_high | 61 | 12 | 1 | 74 |
| study_design | anthropic_sonnet_4_6 | 48 | 25 | 1 | 74 |
| study_type | local | 0 | 2 | 0 | 2 |
| study_type | openai_o4_mini_high | 2 | 0 | 0 | 2 |
| study_type | anthropic_sonnet_4_6 | 1 | 1 | 0 | 2 |
| surgical_domain | local | 22 | 17 | 8 | 47 |
| surgical_domain | openai_o4_mini_high | 42 | 3 | 2 | 47 |
| surgical_domain | anthropic_sonnet_4_6 | 24 | 13 | 10 | 47 |
| system_maturity | local | 56 | 32 | 8 | 96 |
| system_maturity | openai_o4_mini_high | 63 | 30 | 3 | 96 |
| system_maturity | anthropic_sonnet_4_6 | 62 | 30 | 4 | 96 |
| task_execute | local | 23 | 5 | 4 | 32 |
| task_execute | openai_o4_mini_high | 31 | 0 | 1 | 32 |
| task_execute | anthropic_sonnet_4_6 | 20 | 12 | 0 | 32 |
| task_generate | local | 54 | 3 | 12 | 69 |
| task_generate | openai_o4_mini_high | 60 | 4 | 5 | 69 |
| task_generate | anthropic_sonnet_4_6 | 18 | 42 | 9 | 69 |
| task_monitor | local | 37 | 5 | 18 | 60 |
| task_monitor | openai_o4_mini_high | 43 | 7 | 10 | 60 |
| task_monitor | anthropic_sonnet_4_6 | 10 | 20 | 30 | 60 |
| task_performed | local | 40 | 11 | 4 | 55 |
| task_performed | openai_o4_mini_high | 47 | 7 | 1 | 55 |
| task_performed | anthropic_sonnet_4_6 | 15 | 33 | 7 | 55 |
| task_select | local | 30 | 3 | 7 | 40 |
| task_select | openai_o4_mini_high | 35 | 0 | 5 | 40 |
| task_select | anthropic_sonnet_4_6 | 18 | 10 | 12 | 40 |
| validation_setting | local | 27 | 22 | 10 | 59 |
| validation_setting | openai_o4_mini_high | 42 | 15 | 2 | 59 |
| validation_setting | anthropic_sonnet_4_6 | 27 | 28 | 4 | 59 |

## Top 10 papers by UNSUPPORTED count

| paper_id | UNSUPPORTED |
|---|---:|
| 455 | 10 |
| 14 | 9 |
| 780 | 9 |
| 11 | 8 |
| 742 | 8 |
| 9 | 7 |
| 39 | 7 |
| 432 | 7 |
| 442 | 7 |
| 487 | 7 |

## Windowed-path triples

- **Count:** 65/1211 triples (5.4%)
- **Verdict distribution (arm-rows in windowed triples):**

| verdict | n |
|---|---:|
| SUPPORTED | 115 |
| PARTIALLY_SUPPORTED | 49 |
| UNSUPPORTED | 31 |

## Short-circuit firing

- **Arm-rows with short_circuit=True:** 350/3633 (9.6%)

## Failures

| paper_id | field_name | error |
|---|---|---|
| 366 | primary_outcome_value | JudgeParseError: duplicate arm_slot=3 in arm_verdicts |

---
_PI review gate: do NOT proceed to Pass 2 interpretation or audit sampling until this report is reviewed._
