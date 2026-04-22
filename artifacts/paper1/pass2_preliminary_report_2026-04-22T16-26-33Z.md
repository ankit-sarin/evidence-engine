# Pass 2 full preliminary report — Branch B (n=3,633 arm-rows)

_Generated 2026-04-22T16:26:33.443050+00:00 · Descriptive only — no interpretation, no CI, no recommendations._

_PI review gate: do NOT proceed to Pass 2 interpretation or audit sampling until this report is reviewed._

## §1. Run metadata

| field | value |
|---|---|
| run_id | `surgical_autonomy_pass2_full_20260421T174729Z` |
| pass1_run_id | `surgical_autonomy_pass1_20260420T093840Z_1d93b6e5` |
| judge_model_name | `gemma3:27b` |
| judge_model_digest | `gemma3:27b` ⚠ stored as model-name string, not a content digest |
| codebook_sha256 | `ae66c12926951649e6da5747a06d6f81f584c3af53cb3f1979136986230da87d` |
| input_scope | AI_TRIPLES |
| pass_number | 2 |
| started_at | 2026-04-21T17:47:29.757542+00:00 |
| completed_at | 2026-04-22T09:10:54.987528+00:00 |
| wall-clock duration | 15.39 h (55405 s) |
| triples attempted | 1212 |
| triples succeeded | 1211 (arm-rows written: 3633) |
| triples failed | 1 |
| mean seconds per succeeded triple | 45.75 s |
| Ollama disconnect warnings in log | 4 (transient; handled by internal retries) |
| Ollama service restarts | 0 (no systemctl restart detected in run log) |
| Pass 2 seed scheme | SHA-256(`paper_id \x1f field_name \x1f run_id \x1f p2`) → int, first 4 bytes, `% 2**31` (confirmed in `compute_seed_pass2`) |

**Failure commentary (per retry outcome):**

- 1/1,212 triples (0.08%) failed Pass 2 verification due to a cross-field uniqueness violation in the judge's structured output on retry; excluded from the verdict denominator.
- Failed triple: `paper_id=366` / `field_name=primary_outcome_value`. Deterministic Pass 2 seed = `1770411156`. Gemma emitted four `arm_verdicts` entries with slots `[1, 2, 3, 3]` on both the original run and the single retry (same seed, same prompt hash), tripping the post-validation duplicate-slot check. Raw Gemma output captured at `analysis/paper1/logs/pass2_retry_366_primary_outcome_value_20260422T162155Z.log`.

## §2. Overall verdict distribution

| verdict | n | % |
|---|---:|---:|
| SUPPORTED | 2082 | 57.3% |
| PARTIALLY_SUPPORTED | 1038 | 28.6% |
| UNSUPPORTED | 513 | 14.1% |
| **total** | **3633** | 100.0% |

## §3. Verdict distribution — per arm

| arm | SUPPORTED | PARTIALLY_SUPPORTED | UNSUPPORTED | total | UNSUPPORTED % |
|---|---:|---:|---:|---:|---:|
| local | 666 | 342 | 203 | 1211 | 16.8% |
| openai_o4_mini_high | 930 | 193 | 88 | 1211 | 7.3% |
| anthropic_sonnet_4_6 | 486 | 503 | 222 | 1211 | 18.3% |

## §4. Absence-sentinel breakdown

Arm-rows whose extraction value is an absence sentinel (`NR`, `N/A`, `NA`, `NOT_FOUND`, `NOT FOUND`, `NOT REPORTED`, empty, or null). Absence rows are verified under the absence-aware Pass 2 rubric (`build_pass2_prompt`).

| arm | absence arm-rows | SUPPORTED | PARTIALLY_SUPPORTED | UNSUPPORTED |
|---|---:|---:|---:|---:|
| local | 104 | 103 | 0 | 1 |
| openai_o4_mini_high | 211 | 210 | 0 | 1 |
| anthropic_sonnet_4_6 | 4 | 4 | 0 | 0 |
| **total** | **319** | | | |

## §5. Short-circuit distribution

Arm-rows with `pre_check_short_circuit = 1` (clean pre-check: span-in-source AND value-in-span, soft-nudges judge toward SUPPORTED).

- **Overall short-circuit rate:** 350/3633 (9.6%)

| arm | short-circuit arm-rows | % of arm | SUPPORTED | PARTIALLY_SUPPORTED | UNSUPPORTED |
|---|---:|---:|---:|---:|---:|
| local | 86 | 7.1% | 86 | 0 | 0 |
| openai_o4_mini_high | 211 | 17.4% | 210 | 0 | 1 |
| anthropic_sonnet_4_6 | 53 | 4.4% | 52 | 1 | 0 |

## §6. Field-type concentration

### §6a. UNSUPPORTED count per field — top 10

| field | UNSUPPORTED | arm-rows total | UNSUPPORTED % |
|---|---:|---:|---:|
| sample_size | 80 | 219 | 36.5% |
| task_monitor | 58 | 180 | 32.2% |
| secondary_outcomes | 56 | 435 | 12.9% |
| key_limitation | 40 | 351 | 11.4% |
| comparison_to_human | 38 | 171 | 22.2% |
| autonomy_level | 31 | 225 | 13.8% |
| primary_outcome_value | 29 | 231 | 12.6% |
| task_generate | 26 | 207 | 12.6% |
| task_select | 24 | 120 | 20.0% |
| primary_outcome_metric | 20 | 195 | 10.3% |

### §6b. field_type × verdict cross-tab

| field_type | SUPPORTED | PARTIALLY_SUPPORTED | UNSUPPORTED | total | UNSUPPORTED % |
|---|---:|---:|---:|---:|---:|
| categorical | 986 | 538 | 222 | 1746 | 12.7% |
| free_text | 974 | 483 | 211 | 1668 | 12.6% |
| numeric | 122 | 17 | 80 | 219 | 36.5% |

## §7. Windowed-path vs full-text-path verdict distribution

- **Windowed-path coverage:** 8 papers, 65 triples, 195 arm-rows (paper source text exceeded the 20K-token Pass 2 budget → windowed around arm spans via `window_source_text`).

| path | SUPPORTED | PARTIALLY_SUPPORTED | UNSUPPORTED | total | UNSUPPORTED % |
|---|---:|---:|---:|---:|---:|
| windowed | 115 | 49 | 31 | 195 | 15.9% |
| full-text | 1967 | 989 | 482 | 3438 | 14.0% |

## §8. Fabrication hypotheses — raw sample (n=10)

Uniform round-robin across arms over UNSUPPORTED arm-rows (seed=42, no curation). Spans truncated to 200 chars.

| # | paper_id | field | arm | arm value | source span (truncated) | fabrication_hypothesis |
|---|---|---|---|---|---|---|
| 1 | 742 | task_execute | local | R | (none) | The system likely inferred full autonomy from the use of a robot, without considering the 'shared control' context. |
| 2 | 323 | autonomy_level | openai_o4_mini_high | 2 (Task autonomy) | (none) | plausible-sounding default |
| 3 | 693 | autonomy_level | anthropic_sonnet_4_6 | 4 (High autonomy) | (none) | The arm likely over-generalized from the description of the system's advanced features, assuming a higher level of autonomy than is actually stated. |
| 4 | 366 | key_limitation | local | Small sample size affecting model fidelity | (none) | plausible-sounding default; small sample size is often linked to lower model fidelity. |
| 5 | 493 | sample_size | openai_o4_mini_high | 50 | (none) | Plausible-sounding default; the arm likely inferred a total number of experiments from the number of backgrounds and experiments per background. |
| 6 | 460 | task_monitor | anthropic_sonnet_4_6 | R | (none) | Conflation with description of vision system; the arm incorrectly associated the vision system with task monitoring. |
| 7 | 432 | secondary_outcomes | local | Mean insertion depth: 125.23 mmrms; Localization error: 2.65 mmrms | (none) | Plausible-sounding default values were inserted, likely conflated with values from another slot. |
| 8 | 485 | task_performed | openai_o4_mini_high | Move pin1 to North; Move tool back to East; Move pin2 to West; Move tool back to East; Move pin3 to South; Move tool bac… | (none) | The arm likely reconstructed the task sequence from the table and descriptions, creating a more detailed list than what is explicitly stated in the source. |
| 9 | 380 | sample_size | anthropic_sonnet_4_6 | 12 | (none) | plausible-sounding default |
| 10 | 780 | task_execute | local | H | (none) | The arm likely conflated the robot's ability to allow free orientation with it actively performing the orientation. |

## §9. PI audit sampling feasibility check

| stratum (verdict) | target n | available arm-rows | local | openai | anthropic | status |
|---|---:|---:|---:|---:|---:|---|
| UNSUPPORTED | 40 | 513 | 203 | 88 | 222 | OK ✓ |
| PARTIALLY_SUPPORTED | 40 | 1038 | 342 | 193 | 503 | OK ✓ |
| SUPPORTED | 20 | 2082 | 666 | 930 | 486 | OK ✓ |

## §10. Data integrity confirmations

| check | result |
|---|---|
| fabrication_verifications row count = 3,636 (strict) | ✗ (3,633 with 1 documented exclusion) |
| UNIQUE (judge_run_id, paper_id, field_name, arm_name) holds | ✓ |
| CASCADE FK to judge_runs intact (parent row present) | ✓ |
| CHECK constraints (verdict enum, short-circuit bool, UNSUPPORTED requires reasoning+hypothesis) | ✓ |
