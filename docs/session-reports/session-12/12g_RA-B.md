# 12g — Rehearsal A: the run and its analysis

**Status: COMPLETE, clean end.** The elicited dress rehearsal designed in `12g_RA-A.md` ran on
the throwaway review `ra_12g`, on the engine at `931e385` (before the P1 fixes; the pin is not
binding). No engine, test or script change; no live write. Ends at a STOP.

**Every rate below is a stress sample (N=12, selected for length and text pathologies); not a Run 7 estimate.**

| | |
| --- | --- |
| Run | manifest 2 of `ra_12g`, 2026-10-08T18:25:01Z → 2026-10-08T20:38:43Z, 133 m 41 s (2.23 h; estimate was 2.0–3.1 h) |
| End | `('interrupted', "blocked:audit_review")`; no export written |
| Pin | `77ae99d8671534aaa1350013f1cb6a4409a16eba7a1e097dad408188b81d1bf2` — as expected (R553) |
| Papers | 12 eligible: 11 `extracted` then `audited_ai`; 1 `input_exceeds_context` (live 415) |
| Live fingerprint | IDENTICAL to the migration-02 record at pre-flight, after the import, after the move, and after the push |
| Retained | `~/scratch/retained/ra_12g/`, 32 files, every sha256 equal before and after the move; `review.db` 36 tables, overall `0401f7492785cc11a29b90e168073073256c3256161e784e05a0b560f8635b82` |

## 1. Pre-flight and the inferred items

| | Result | Evidence |
| --- | --- | --- |
| P1 | pass | launched 18:24:58 UTC; the 4.6 h abort line fell at about 23:00 |
| P2 | pass | live `--compare` exit 0, overall `0d3eedea…cb25`; HEAD `931e385`, clean; no other `data/<dir>/review.db` |
| P3 / I1 | confirmed | `launch_locked.py --check`: outer `hold_experiment_lock(blocking=False)`, then a nested blocking acquire, returned in 0.000 s; the lock was free after |
| P4 / I2 | confirmed, in two places | every call's `input_fit` log line carries `done_reason` (`ollama_client._check_input_was_read`); telemetry carries `pass1_done_reason` and `finish_reason` (Pass 2) per stored attempt. `run_calls` does not carry it. R552's reclassification of RA-1 does not apply |
| P5 / I3 | confirmed | `OLLAMA_MAX_LOADED_MODELS=1`, `OLLAMA_NUM_PARALLEL=1`, `OLLAMA_FLASH_ATTENTION=true`, `OLLAMA_KEEP_ALIVE=-1`, `OLLAMA_KV_CACHE_TYPE=f16`; `NRestarts=0`; `ActiveEnterTimestamp` and `ExecMainStartTimestamp` Fri 2026-09-11 21:09:13 UTC; resident `qwen3:8b` |
| P6 | pass | the lock was held by no one |
| I4 | confirmed | live 415: "input cannot fit deepseek-r1:32b: at least 365,765 tokens (1,925,081 characters x 0.19) against a ceiling of 131,072. Nothing was sent." One `run_calls` row `refused_input_overflow`; paper event `input_exceeds_context`, reason `input_overflow_estimated`; the run went on to paper 3 and the abort counter never fired |

**One departure from the brief, accepted by R556:** the log was written to both
`~/scratch/12g-ra/run.log` and `data/ra_12g/logs/ra_12g_run.log` (the two files are identical).

## 2. Construction, launch, end

- **Input** (`build_input.py`): twelve texts read from live through the engine's resolver and written to `~/scratch/12g-ra/input/texts/`; each sha256 equal to the read-out's (`id_map.json`). `entry.json` sha256 `14512cd1fc20b0a48c61a2f7899066cf544e4debc731ae0f56e6cb0b0710a5bf`.
- **Review:** `data/ra_12g/` with a byte copy of the codebook (`89dbfa91…ad82`) and `spec.yaml`; `diff` against the live spec showed exactly the two lines (`review_id`, `elicitation`).
- **Import:** 12 papers at FT_ELIGIBLE under `import` manifest 1; 12 refs, each sha256 equal to its input; 0 arms; eight workflow stages complete. Live `--compare` exit 0 after it.
- **Launch:** tmux session `ra_12g`, `launch_locked.py`: `run_pipeline --review ra_12g --spec data/ra_12g/spec.yaml --skip-to extract --max-papers 12` inside `hold_experiment_lock(blocking=False)`.
- **Checkpoints:** nine, about 15 minutes apart, each a bounded wait on the manifest's `end_status`. No abort condition fired at any of them: no restart, no truncated or dropped input, no undeclared call, no timeout, one expected refusal.
- **End:** manifest closed `interrupted` / `blocked:audit_review` at 20:38:43 UTC; the lock was released; no `exports/` directory exists. `EXTRACTION_COMPLETE` and `AI_AUDIT_COMPLETE_STAGE` complete; `AUDIT_QUEUE_EXPORTED` and `AUDIT_REVIEW_COMPLETE` pending.
- **Move:** `lsof` empty; fingerprint and per-file sha256 taken; `data/ra_12g/` moved to `~/scratch/retained/ra_12g/`; all 32 hashes equal; the fingerprint IDENTICAL. Re-checked after the analysis reads: still equal.

**Residency (R554).** Before: `qwen3:8b`, 11 GB, Forever. During extraction: `deepseek-r1:32b`, 64 GB at 131,072 context. After: `gemma3:27b`, 30 GB at 131,072 context, Forever. `NRestarts=0` and both timestamps unchanged. Nothing was unloaded or restarted; `qwen3:8b` is no longer resident and will load on its next caller's request.

## 3. Per paper

| # | Live id | Result | Wall time | Pass-1 calls | Largest prompt (tokens) | Asserted / declined / contract unmet | Unlocated | Snippets not exact in the text | Swap status |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 121 | extracted, audited | 9 m 25 s | 2 (accepted 1) | 12,797 | 14 / 4 / 2 | 0 | 0 | clean |
| 2 | 415 | refused before send (`input_exceeds_context`) | 0 m 28 s | 0 | — | — | — | — | — |
| 3 | 11 | extracted, audited | 15 m 54 s | 2 (accepted 2) | 59,027 | 13 / 3 / 4 | 0 | 5 | clean |
| 4 | 498 | extracted, audited | 15 m 54 s | 2 (accepted 2) | 48,941 | 11 / 4 / 5 | 0 | 0 | clean |
| 5 | 607 | extracted, audited | 16 m 31 s | 2 (accepted 2) | 46,934 | 18 / 1 / 1 | 0 | 1 | clean |
| 6 | 748 | extracted, audited | 12 m 40 s | 2 (accepted 1) | 27,788 | 19 / 0 / 1 | 0 | 8 | clean |
| 7 | 368 | extracted, audited | 14 m 32 s | 2 (accepted 1) | 27,560 | 13 / 1 / 6 | 0 | 4 | clean |
| 8 | 455 | extracted, audited | 9 m 09 s | 2 (accepted 1) | 20,856 | 16 / 0 / 4 | 0 | 1 | clean |
| 9 | 699 | extracted, audited | 11 m 39 s | 2 (accepted 1) | 16,011 | 19 / 0 / 1 | 1 | 6 | clean |
| 10 | 431 | extracted, audited | 9 m 16 s | 2 (accepted 1) | 18,574 | 17 / 0 / 3 | 0 | 0 | clean |
| 11 | 783 | extracted, audited | 6 m 57 s | 1 (accepted 1) | 9,497 | 19 / 1 / 0 | 0 | 1 | clean |
| 12 | 604 | extracted, audited | 10 m 40 s | 2 (accepted 2) | 18,190 | 18 / 1 / 1 | 0 | 0 | clean |

Extracted papers: 6 m 57 s to 16 m 31 s, median 11 m 39 s, mean 12 m 03 s. The audit stage took 16 s in all and made 1 model call.

## 4. Tokens, headroom, input fit (`ra_tokens.py` → `ra_tokens.json`, `ra_tokens_calls.csv`)

36 calls, one `input_fit` log line each, paired to `run_calls` in order (paper and end time checked on every pair).

| Stage | Calls | Tokens per request character (min / median / max) | Actual ÷ low estimate | Largest prompt (share of ceiling) | Least room left after the prompt (tokens) | `done_reason` | Seconds per call |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `elicitation_pass1` | 21 | 0.234 / 0.258 / 0.297 | 1.23 / 1.36 / 1.56 | 59,027 (45%) | 72,045 | {'stop': 21} | 88 / 229 / 378 |
| `extract_pass2` | 11 | 0.224 / 0.238 / 0.269 | 1.18 / 1.25 / 1.41 | 50,569 (39%) | 80,503 | {'stop': 11} | 194 / 273 / 370 |
| `audit` | 1 | 0.251 / 0.251 / 0.251 | 1.32 / 1.32 / 1.32 | 271 (0%) | 130,801 | {'stop': 1} | 8 / 8 / 8 |

- **A-8.** On `[Sn]`-marked text the model counted a median 0.258 tokens per character, up to 0.297. The guard's low estimate uses 0.19, so the actual count was 1.36× the estimate at the median and 1.56× at most. The estimate is a refusal floor, not a prediction, and behaved as one: nothing that was sent came near the ceiling.
- **Headroom, by proxy** (`eval_count` is not recorded, RA-1). The largest prompt used 45% of the 131,072-token ceiling, leaving 72,045 tokens. Every extraction call ended `stop`; none ended `length`. Output per paper was small beside that room: Pass-1 answer 3,358–4,725 characters with 1,745–3,903 characters of thinking, Pass-2 answer 4,350–10,095 characters. The one `length` in the log is the extractor preflight probe, which sets `num_predict` 4.
- **Input fit.** One pre-call refusal (live 415). No `refused_input_truncated`, no `refused_input_dropped`, no `refused_ceiling_unavailable`, no `error`.
- **Time is all model time.** The calls' own durations sum to 100% of the extracted papers' wall time.

## 5. The Pass 2 format schema (`ra_schema.py` → `ra_schema.json`)

Rebuilt from `stage_config("extract_pass2", spec)` with the retained spec; the engine and scripts at analysis are the run's commit (`931e385`; changed since: False). Stored hash `2c9ec255d3b2a90d941cd4a81f8f9fd685c17d6077b4acb25b41c110d87a10b3`, rebuilt `2c9ec255d3b2a90d941cd4a81f8f9fd685c17d6077b4acb25b41c110d87a10b3`: equal. The body is in `ra_schema.json`. It is the array-wrapped schema: `fields` is an array of `EvidenceSpan` with `minItems` None and `maxItems` None — no lower bound on how many fields come back (A-11's premise, confirmed on the elicited path). `elicitation_pass1` sends no format; the `audit` stage's hash also matches.

## 6. Field outcomes (`ra_fields.py` → `ra_fields.json`)

11 extracted papers × 20 fields = 220 cells.

| Outcome | Cells | Share |
| --- | --- | --- |
| Asserted (a value with evidence) | 177 | 80.5% |
| Declined (`NO_EVIDENCE_LOCATABLE`) | 15 | 6.8% |
| Contract unmet (no value stored) | 28 | 12.7% |
| Incomplete (missing after the budget) | 0 | 0.0% |

- **`incomplete_fields`: empty on all 11 papers.** No completeness retry ran (0); every paper stored on its first attempt with 20 terminal states. **R535's floor would have changed nothing here**: 0 papers under it. No paper has fewer than ten asserted values either.
- **Contract unmet:** 10 of 11 papers have at least one. Violation codes: `FIELD_MISSING` 2, `INFERENCE_MISSING` 3, `STEPS_MISSING` 5, `VALUE_WITHOUT_CITATION` 19.
- **The Pass-1 second attempt.** 10 of 11 papers ran it (every paper whose first attempt had a failing field). It was accepted on 4 and discarded on 6. Against the first attempt it had strictly fewer failing fields on 4 papers, the same number on 1, and **more on 5**. Summed over those ten papers the first attempts failed 46 fields and the second attempts 62. The acceptance rule (strictly fewer) did its job: no regression was stored. The cost is one more Pass-1 call on ten papers.

| Field | Asserted | Declined | Contract unmet | Violation codes |
| --- | --- | --- | --- | --- |
| `autonomy_level` | 10 | 0 | 1 | STEPS_MISSING 1 |
| `clinical_readiness_assessment` | 8 | 0 | 3 | STEPS_MISSING 1, VALUE_WITHOUT_CITATION 2 |
| `comparison_to_human` | 4 | 2 | 5 | VALUE_WITHOUT_CITATION 5 |
| `country` | 8 | 0 | 3 | FIELD_MISSING 1, INFERENCE_MISSING 2 |
| `key_limitation` | 9 | 0 | 2 | STEPS_MISSING 1, VALUE_WITHOUT_CITATION 1 |
| `primary_outcome_metric` | 9 | 2 | 0 | — |
| `primary_outcome_value` | 7 | 3 | 1 | VALUE_WITHOUT_CITATION 1 |
| `robot_platform` | 11 | 0 | 0 | — |
| `sample_size` | 3 | 6 | 2 | VALUE_WITHOUT_CITATION 2 |
| `secondary_outcomes` | 8 | 2 | 1 | VALUE_WITHOUT_CITATION 1 |
| `study_design` | 9 | 0 | 2 | STEPS_MISSING 1, VALUE_WITHOUT_CITATION 1 |
| `study_type` | 10 | 0 | 1 | VALUE_WITHOUT_CITATION 1 |
| `surgical_domain` | 10 | 0 | 1 | INFERENCE_MISSING 1, VALUE_WITHOUT_CITATION 1 |
| `system_maturity` | 10 | 0 | 1 | STEPS_MISSING 1 |
| `task_execute` | 10 | 0 | 1 | VALUE_WITHOUT_CITATION 1 |
| `task_generate` | 10 | 0 | 1 | VALUE_WITHOUT_CITATION 1 |
| `task_monitor` | 10 | 0 | 1 | VALUE_WITHOUT_CITATION 1 |
| `task_performed` | 11 | 0 | 0 | — |
| `task_select` | 9 | 0 | 2 | FIELD_MISSING 1, VALUE_WITHOUT_CITATION 1 |
| `validation_setting` | 11 | 0 | 0 | — |

## 7. Unlocated claims (`ra_unlocated.py` → `ra_unlocated.json`, `ra_unlocated_claims.csv`)

**1 of 177 asserted claims was unlocated; 176 were located, all by the locator's normalised exact match (none fuzzy).**

The one unlocated claim, classified by the read-out's procedure as **INT-g6-1 (a unit that differs from its source)**:
- live 699, `secondary_outcomes`, value "Task completion rate: 83.3%; Average completion time: 70.81s", cited units 95 96; best window score 0.724 against the 0.85 threshold.
- stored snippet: "(4),Eq. (5). According to the experimental data in Table 1, the calculations yield CR=83.3% and CT=70.81s. In this paper, we proposed a cooperative suturing scheme."
- unit 95 is not a substring of the comment-stripped text; no A-9 repair locates it. The auditor's one verdict of the run was on this claim: `verified`.

Counts by class over the unlocated set: {'INT-g6-1_unit_rewritten': 1}. A-10 (comment adjacency): 0. A-9 (hyphenation, ligatures, soft hyphens): 0. Unexplained: 0.

**The same tests over all 177 claims**, because the locator compares normalised text (lower case, collapsed whitespace, NFKC) and can pass a snippet that is not in the paper verbatim:

| | Claims |
| --- | --- |
| Stored snippet is an exact substring of the raw text | 151 |
| Not exact — a cited unit differs from its source (INT-g6-1) | 17 |
| Not exact — each unit is in the text but their joined run is not (INT-g6-1) | 9 |
| Not exact — in the comment-stripped text only (A-10) | 0 |
| **Would fail a materialisation-time EXACT check against the raw text** | **26** (14.7%) |
| …against the comment-stripped text | 26 |

- Of the 26 inexact snippets, 25 become exact once runs of whitespace are collapsed (the segmenter's and the join's spacing), and 1 does not — the unlocated one.
- **A-10 produced nothing on this sample.** All twelve texts carry Docling comments (2 to 671), yet no stored snippet was exact in the stripped text and inexact in the raw text. Stripping a comment and checking against either text gave the same 26.
- **A-9 produced nothing.** Live 748 and 368 carry 15 and 8 line-end hyphens and 64 and 92 ligature characters; their inexact snippets (8 and 4) are all whitespace differences, and the locator's NFKC step already folds ligatures.
- The stored snippet equals the first contiguous run rebuilt from the unit map on all 177 claims. 89 of 177 claims cite more than one run; only the first is on the claim, the rest only in telemetry and the unit map.
- Located is not the same as supported. One example from the claims file: live 11, `autonomy_level`, whose stored snippet is the table fragment "facilities. jurisdictio autonomo". The run measures location; nothing in it measures support.

## 8. Foreign loads and the timing figure (R556; `ra_swaps.py` → `ra_swaps.json`)

The Ollama journal is readable without root and logs each model load (a `starting runner … --model <blob>` line); it has no unload line, and under `MAX_LOADED_MODELS=1` a load of another model is the eviction. In the window:

| Time (UTC) | Model | Run call in flight | Foreign |
| --- | --- | --- | --- |
| 18:25:02 | `deepseek-r1:32b` | `preflight:deepseek-r1:32b` | no |
| 20:38:29 | `gemma3:27b` | `preflight:gemma3:27b` | no |

**No foreign load.** The journal also shows 35 `POST /api/chat` requests in the window, equal to the run's 35 completed calls, so no other caller used the loaded model either. Every paper is marked `clean`.

RA-5, computed twice as ruled; the two are the same because no paper was affected:

| | Papers | Mean wall time | Hours at the sample mean × 189 fitting papers | Hours by a length model over the corpus |
| --- | --- | --- | --- | --- |
| All extracted papers | 11 | 12 m 03 s | 38.0 | 34.6 |
| Clean papers only | 11 | 12 m 03 s | 38.0 | 34.6 |

The length model is a least-squares line through the eleven papers (508 s plus 3.23 s per 1,000 text characters) applied to each of the 189 corpus papers that fit. The sample's median text is 52,403 characters against the corpus's 42,738, and ten of eleven papers ran a second Pass-1 attempt, so both figures lean long. They are stress-sample figures, recorded against the Run 7 estimate (R552), not a replacement for it.

## 9. Findings (R509)

| # | Finding | Class | Package |
| --- | --- | --- | --- |
| RB-1 | INT-g6-1, sized: 26 of 177 stored snippets are not exact substrings of the paper; 25 differ only in whitespace and pass the locator, 1 differs further and is unlocated. R534's materialisation-time EXACT check would refuse all 26 as the engine stands | 1 (INT-g6-1's; no new row) | P1 |
| RB-2 | A-10 and A-9 produced no unlocated claim and no inexact snippet on a sample chosen to provoke them. Input to the locator-2 decision (R534): on this evidence exact-slice units remove the whole problem and comment-aware or hyphen-aware normalisation removes none of it | — (measurement for R534) | P1 |
| RB-3 | The Pass-1 second attempt ran on 10 of 11 papers, was worse than the first on 5 and was accepted on 4. It costs about one extra Pass-1 call per paper (median 229 s). Whether the typed-feedback retry earns that is a design question; any change to it changes what the elicited run sends (R539) | 2 (proposed) | P1, pin-affecting |
| RB-4 | A-11's floor is untested by this run: no paper had an incomplete field, so R535's reason code and reselection path were not exercised | — (measurement) | P1 |
| RB-5 | A-8, measured: 0.258 tokens per character at the median on marked text, 0.297 at most, against the 0.19 floor. No paper that fits the floor came near the ceiling | — (informational, A-8) | — |
| RB-6 | On the elicited path the cross-family auditor is almost idle: 1 call for 177 claims, because a materialised snippet is located by construction. Run 7's audit verdicts will cover only what the locator misses | — (informational; CLAUDE.md already warns anchored rates are not comparable) | — |
| RB-7 | Every one of the 177 `asserted` events carries `state_at_write` "asserted without locatable evidence" in its payload, including claims located a moment later. Not read further | 3 (proposed; a wording to check) | P7 |
| RB-8 | RA-5 confirmed on the current engine: mean 12 m 03 s per paper; 34.6–38.0 h for the corpus by the two stress-sample figures | — (planning) | — |

Carried from Phase A and unchanged by the run: RA-1 (`eval_count` unrecorded; stays Class 3 since `done_reason` is recorded), RA-2 (the unit-map directory `elicitation/run_20261008T182516Z/` is linked to run 2 by nothing on disk; joined here by file name), RA-3 (no outer `flock`).

**Not anticipated by the read-out, none an abort condition:** the unit map for the refused paper (live 415, 17,058 units) was built and written before the refusal, which is where that paper's 28 seconds went.

## 10. Retention row (R31; drafted, not transcribed)

| Artifact | Ruling | Disposition |
| --- | --- | --- |
| The throwaway review `ra_12g` — `~/scratch/retained/ra_12g/` (`review.db` and sidecars, `spec.yaml`, `extraction_codebook.yaml`, `parsed_text/`, `elicitation/run_20261008T182516Z/unit_maps/`, `telemetry/`, `logs/`) and `~/scratch/12g-ra/input/` (the import's JSON and twelve texts) | R550, R31, R235, R241(iv), R309 | **RETAINED to the freshman freeze.** Rehearsal A (12g): a fresh review by the extraction-entry import, 12 papers, elicited, at `931e385` before the P1 fixes; arm pinned `77ae99d8…1bf2`, not binding (R539). Manifest 2 `('interrupted', "blocked:audit_review")`; 11 extracted and audited, 1 `input_exceeds_context`. `review.db` 36 tables, overall `0401f7492785cc11a29b90e168073073256c3256161e784e05a0b560f8635b82`, file sha256 `31bb0ef52fa4c1833ec79e278b1f07484fa30232c8cee89e7129dbf044cd9647`. Moved from `data/ra_12g/` after the manifest closed; every file's sha256 equal before and after (`12g_ra/ra_12g_sha256_at_move.txt`). The manifests' stored paths name the pre-move directories; the sha256 is the identity (R319). |

## 11. Files

`12g_ra/`: the five analysis scripts with `ra_common.py`, their outputs (`ra_tokens.json`, `ra_tokens_calls.csv`, `ra_schema.json`, `ra_fields.json`, `ra_unlocated.json`, `ra_unlocated_claims.csv`, `ra_swaps.json`), the launcher, the input builder and its `id_map.json`, the checkpoint script, the journal's load lines, and the retained copy's hashes and fingerprint at the move. Each script runs from the repository root as `PYTHONPATH=docs/session-reports/session-12/12g_ra .venv/bin/python <script> ~/scratch/retained/ra_12g`.
