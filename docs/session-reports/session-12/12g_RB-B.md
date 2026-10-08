# 12g — Front-half rehearsal: the run and its analysis

**Status: COMPLETE, clean end.** The rehearsal designed in `12g_RB-A.md` ran on the throwaway
review `rb_12g`, on the engine at `015b2e4`. No engine, test or script change; no live write.
Ends at a STOP.

| | |
| --- | --- |
| Run | manifest 2 of `rb_12g`, 2026-10-08T21:44:56Z → 2026-10-08T21:49:35Z, 279 s (estimate 4–9 minutes; abort line 13) |
| End | `('interrupted', "blocked:adjudication")` — as expected |
| Pin at run open | `defd293bfe62e427b906e072dcf56d0ca6e321e7d1ca4ad1fde3758fb84f3e6e` — as expected |
| Calls | 85: 1 `preflight:qwen3:8b`, 84 `abstract_screen_primary`, all `completed` |
| Records | 42: 15 screened in, 27 screened out, 0 flagged |
| Live fingerprint | IDENTICAL to the migration-02 record at pre-flight, after the import, after the move, and after the push |
| Retained | `~/scratch/retained/rb_12g/`, 6 files, every sha256 equal before and after the move; `review.db` 36 tables, overall `70e07f3e9ecee9d86b515e591260d97132fe9cb3b70bd8a138cec80ab072276d`, file sha256 `64f0497c73b5a6fea8b985c760f19c8cd777589ffa352182533acac613c78f8d` |

## 1. Pre-flight and the inferred items

| | Result | Evidence |
| --- | --- | --- |
| P1 | pass | live `--compare` exit 0, overall `0d3eedea…cb25`; HEAD `015b2e4`, clean; no other `data/<dir>/review.db`; the lock held by no one |
| P2 | recorded | `OLLAMA_MAX_LOADED_MODELS=1`, `OLLAMA_NUM_PARALLEL=1`, `OLLAMA_FLASH_ATTENTION=true`, `OLLAMA_KEEP_ALIVE=-1`, `OLLAMA_KV_CACHE_TYPE=f16`; `NRestarts=0`; `ActiveEnterTimestamp` and `ExecMainStartTimestamp` Fri 2026-09-11 21:09:13 UTC; resident `gemma3:27b` (left by Rehearsal A) |
| I1 | confirmed | crontab's model-calling jobs are 07:00 (Ollama health check) and 09:00 (nightly suite); the run was 21:44–21:49 UTC. No system timer calls Ollama. The two five-minute user timers (`surgical-cv-health`, `surgical-cv-worker`) run modules with no Ollama call. The journal showed no request to Ollama between Rehearsal A's end and this launch |
| I2 | confirmed | the guard let both through and the model read them whole — table in section 4: the larger is 15.8% of `qwen3:8b`'s 40,960-token context |

The two report decisions the architect did not see (lock; A-8) were both settled by R564, so nothing was restated before construction.

## 2. Construction, launch, end against expected

- **Input** (`build_input.py`): 42 records read from live `mode=ro` in `rb_sample.csv`'s order, each abstract's length checked against the sample file; 2 abstracts are `null`. `entry.json` sha256 `08e4902bad74ad44f2637bc838e467171b304b6c7fa55593bd7e00cd531fbdc5`.
- **Review:** `data/rb_12g/` with a byte copy of the codebook (`89dbfa91…ad82`) and `spec.yaml`; `diff` against the live spec showed exactly one changed line (`review_id`).
- **Import:** 42 papers at `INGESTED` under `import` manifest 1; no events, no arms. Live `--compare` exit 0 after it.
- **Launch:** tmux session `rb_12g`, the Rehearsal A launcher with its argv changed: `run_pipeline --review rb_12g --spec data/rb_12g/spec.yaml --skip-to screen` inside `hold_experiment_lock(blocking=False)`. Logged to `data/rb_12g/logs/rb_12g_run.log` and `~/scratch/12g-rb/run.log` (identical).
- **No abort condition fired:** no restart, no undeclared call or override, no `refused_input_*`, no PARSE stage line, 279 s against the 13-minute line.

| Expected (12g_RB-A.md) | Actual |
| --- | --- |
| Manifest `('interrupted', "blocked:adjudication")` | the same |
| 1 preflight call, 84 `abstract_screen_primary`, all completed | the same; `paper_id` NULL on all 85 |
| 84 decision rows | 84 |
| Every paper screened in, out or flagged | {'ABSTRACT_SCREENED_IN': 15, 'ABSTRACT_SCREENED_OUT': 27} — none flagged |
| Zero paper events, zero field events | 0, 0 |
| Twelve `workflow_state` rows, all pending | 12 rows, 12 pending |
| One arm, pinned; no parsed text; no export | 1 arm pinned by run 2; 0 refs; no `exports/` directory |
| The gate names `ABSTRACT_SCREENING_COMPLETE` | log: "BLOCKED: Adjudication workflow incomplete." / "Current stage: ABSTRACT_SCREENING_COMPLETE" |
| No stage after SCREEN | the log's only stage line is "STAGE: SCREEN" |

**No difference from the expected end state.**

**Residency.** Before: `gemma3:27b`, 30 GB, Forever. After: `qwen3:8b`, 11 GB at 40,960 context, Forever — the model that was resident before today's rehearsals. `NRestarts=0` and both timestamps unchanged. Nothing was unloaded or restarted.

## 3. Calls against declarations (`rb_calls.py` → `rb_calls.json`, `rb_calls_records.csv`)

| Declared stage | Model | Calls | Outcomes | |
| --- | --- | --- | --- | --- |
| `abstract_screen_primary` | `qwen3:8b` | 84 | completed 84 | called |
| `abstract_screen_verifier` | `gemma3:27b` | 0 | — | declared, not reached: the run stops at the adjudication gate (RB-A2) |
| `audit` | `gemma3:27b` | 0 | — | declared, not reached: the run stops at the adjudication gate (RB-A2) |
| `extract_pass1` | `deepseek-r1:32b` | 0 | — | declared, not reached: the run stops at the adjudication gate (RB-A2) |
| `extract_pass2` | `deepseek-r1:32b` | 0 | — | declared, not reached: the run stops at the adjudication gate (RB-A2) |
| `extract_retry_snippet` | `deepseek-r1:32b` | 0 | — | declared, not reached: the run stops at the adjudication gate (RB-A2) |
| `preflight:deepseek-r1:32b` | `deepseek-r1:32b` | 0 | — | declared, not reached: the run stops at the adjudication gate (RB-A2) |
| `preflight:gemma3:27b` | `gemma3:27b` | 0 | — | declared, not reached: the run stops at the adjudication gate (RB-A2) |
| `preflight:qwen3:8b` | `qwen3:8b` | 1 | completed 1 | called |
| `vision_parse` | `qwen2.5vl:7b` | 0 | — | declared, not reached: the run stops at the adjudication gate (RB-A2) |

- 10 stage rows declared, 2 with calls; **0 calls under an undeclared stage**; the log has 0 `UndeclaredCall` / `UndeclaredOverride` lines and 0 tracebacks. The eight zero-call rows are the stages past the gate (RB-A2, held for the 12h read RD-5).
- C55 on a real model: the screening preflight is declared and recorded (`preflight:qwen3:8b`, completed, 2.42 s; its `done_reason` `length` is the probe's `num_predict` 4).
- **Foreign loads:** one model load in the window, `qwen3:8b` at 21:44:57 UTC during the run's own preflight; none foreign. The journal's 85 `POST /api/chat` requests equal the run's 85 completed calls.

**Timing.** Each `input_fit` log line was paired to its `run_calls` row in order (end times within 2 s on all 85), and each screening call to its decision row (written at most 0.016 s after the call ended).

| | Minimum | Median | Maximum |
| --- | --- | --- | --- |
| Seconds per screening call (n = 84) | 1.56 | 3.10 | 6.16 |
| Seconds per record (two calls) | 3.32 | 6.25 | 9.47 |

The calls sum to 274.7 s of the run's 279 s. March's median was 2.85 s per decision; this run's is 3.10 s.

## 4. Input fit and the longest abstracts

Tokens per request character on `qwen3:8b`: 0.2014 / 0.2111 / 0.2236 (minimum / median / maximum). All 84 screening calls ended `stop`; no refusal, truncation or drop line.

| Live id | Abstract chars | Request chars | Prompt tokens | Share of the 40,960 ceiling | `done_reason` (pass 1 / 2) | Seconds for the record |
| --- | --- | --- | --- | --- | --- | --- |
| 2976 | 27,606 | 31,176 | 6,473 | 15.8% | stop / stop | 9.47 |
| 2000 | 26,352 | 29,906 | 6,022 | 14.7% | stop / stop | 7.59 |

## 5. Decisions (`rb_decisions.py` → `rb_decisions.json`, `rb_decisions_records.csv`)

| Stratum | Records | In / out / flagged (this run) | March outcome | Same outcome as March |
| --- | --- | --- | --- | --- |
| abstract under 200 characters (E5) | 2 | 0 / 2 / 0 | out 2 | 2 |
| both passes exclude | 12 | 0 / 12 / 0 | out 12 | 12 |
| both passes include, excluded later at abstract (verifier or PI) | 4 | 0 / 4 / 0 | in 4 | 0 |
| included at abstract, eligible today | 10 | 10 / 0 / 0 | in 10 | 10 |
| included at abstract, excluded at full text | 6 | 5 / 1 / 0 | in 6 | 5 |
| longest abstracts | 2 | 0 / 2 / 0 | out 2 | 2 |
| no abstract (E5) | 2 | 0 / 2 / 0 | out 2 | 2 |
| passes disagreed | 4 | 0 / 4 / 0 | flagged 4 | 0 |

**The two passes never disagreed** (0 of 42), so nothing was flagged. See finding RB-B1: the two passes send the same request.

### The E5 records

| Live id | Abstract chars | Request chars | Pass 1 / pass 2 | Pass-1 rationale |
| --- | --- | --- | --- | --- |
| 21 | 0 (null) | 3,764 | exclude / exclude | The title indicates the paper focuses on learning surgical skills under the RCM constraint from demonstrations in robot-assisted minimally invasive surgery, which suggests a teleoperated or manually guided system rather than an autonomous or semi-autonomous ro… |
| 95 | 147 | 3,710 | exclude / exclude | The paper describes an MR-safe endovascular robotic platform but does not mention any level of surgical autonomy (Levels 1–5) or any autonomous/semi-autonomous robotic component that controls or directs a physical robot to execute a surgical action. It focuses… |
| 8754 | 1 | 3,591 | exclude / exclude | The title indicates that the paper is a collection of abstracts from a conference meeting, and the abstract section is empty. There is no information provided to determine if the paper meets the inclusion criteria. The paper does not describe any autonomous or… |
| 10039 | 0 (null) | 3,763 | exclude / exclude | The title indicates the paper is focused on engineering elastohydrodynamic friction on soft substrates through surface patterning and porous microstructures, which is unrelated to surgical robotics or autonomous/semi-autonomous surgical systems. There is no me… |

- A record with no abstract is screened like any other. The prompt carries the fallback line in place of the abstract: "Abstract: [Not available. Exclusion criterion: Papers with no abstract or insufficient information to determine eligibility — do not default to inclusion when evidence is absent]". The full message for live 21 is in `rb_decisions.json`.
- Both null-abstract records were excluded on what the **title** suggested, not on the absence of an abstract. For live 21 the rationale reasons from the title to "a teleoperated or manually guided system"; live 21's March decision was also exclude.
- There is no input-adequacy check (E5): nothing marks these four decisions as made on a title alone. `papers.status` reads `ABSTRACT_SCREENED_OUT` for all four, the same as for a record excluded on a full abstract.

### Agreement with March — prompts changed since March; not a performance measure

33 of 42 records ended with the same outcome as live's latest March pair. Cross-tabulation (March → now): flagged->out: 4, in->in: 15, in->out: 5, out->out: 18.

| Live id | Stratum | March pass 1 / 2 | This run pass 1 / 2 |
| --- | --- | --- | --- |
| 18 | both passes include, excluded later at abstract (verifier or PI) | include / include | exclude / exclude |
| 382 | passes disagreed | include / exclude | exclude / exclude |
| 803 | included at abstract, excluded at full text | include / include | exclude / exclude |
| 998 | passes disagreed | exclude / include | exclude / exclude |
| 1654 | both passes include, excluded later at abstract (verifier or PI) | include / include | exclude / exclude |
| 2102 | passes disagreed | include / exclude | exclude / exclude |
| 3273 | both passes include, excluded later at abstract (verifier or PI) | include / include | exclude / exclude |
| 9682 | passes disagreed | exclude / include | exclude / exclude |
| 9688 | both passes include, excluded later at abstract (verifier or PI) | include / include | exclude / exclude |

Every change is toward exclusion. Eight of the nine are records that March's primary passed or split on and that were later excluded at abstract anyway (by the verifier or the PI); the ninth (live 803) was excluded at full text. This is a description of nine records under a changed prompt, not a measure of the screener.

## 6. Findings (R509)

| # | Finding | Class | Package |
| --- | --- | --- | --- |
| RB-B1 | **The two abstract passes send byte-identical requests.** All 42 pass-1 / pass-2 pairs have the same `run_calls.request_hash`, at temperature 0 with no seed. The decisions agreed on 42 of 42; the response text was identical on 16 of 42 pairs. A second pass of the same request is not an independent reading, and a disagreement between them (the route to `ABSTRACT_SCREEN_FLAGGED`) is sampling noise. Live's record holds 17 such splits among 10,039 records (the latest pair per record, read `mode=ro` for this report) | 2 (proposed) for the mechanism, whose redesign is S4's (junior); 3 for any text that calls it a dual-pass check | S4 (junior) for the mechanism; P7 for the wording, if ruled pre-tag |
| RB-B2 | RB-A1 confirmed on a real run: all 85 `run_calls` rows carry a NULL `paper_id`, so a screening call is tied to its record only by order and time | 3 (ruled, R564) | P7, with C48 |
| RB-B3 | E5 on real input: four records with no usable abstract were decided on the title, and nothing in the stored record distinguishes those decisions | — (existing row E5; sized here) | E5's |
| RB-B4 | C55, C54 and C57 hold on real models: the screening preflight is declared and recorded, every call is under a declared stage, and a screen start stops at the adjudication gate with the manifest closed `interrupted` / `blocked:adjudication` | — (confirmation) | — |
| RB-B5 | Abstract screening timing on the current engine: median 3.10 s per call, 6.25 s per record | — (informational) | — |

Nothing occurred that the design read did not anticipate, apart from RB-B1.

## 7. Retention row (R31; drafted, not transcribed)

| Artifact | Ruling | Disposition |
| --- | --- | --- |
| The throwaway review `rb_12g` — `~/scratch/retained/rb_12g/` (`review.db` and sidecars, `spec.yaml`, `extraction_codebook.yaml`, `logs/`) and `~/scratch/12g-rb/input/entry.json` | R564, R31, R235, R241(iv), R309 | **RETAINED to the freshman freeze.** The front-half rehearsal (12g): a fresh review by the screening-entry import, 42 records, `run_pipeline --skip-to screen` at `015b2e4`. Manifest 2 `('interrupted', "blocked:adjudication")`; 85 `run_calls`, all completed; 84 decision rows; 15 screened in, 27 out; no events; arm `local_deepseek_r1_32b` pinned `defd293b…3e6e` by the screen-start manifest (RB-A2). `review.db` 36 tables, overall `70e07f3e9ecee9d86b515e591260d97132fe9cb3b70bd8a138cec80ab072276d`, file sha256 `64f0497c73b5a6fea8b985c760f19c8cd777589ffa352182533acac613c78f8d`. Moved from `data/rb_12g/` after the manifest closed; every file's sha256 equal before and after (`12g_rb/rb_12g_sha256_at_move.txt`). |

## 8. Files

`12g_rb/`: `rb_calls.py`, `rb_decisions.py` and their outputs (`rb_calls.json`, `rb_calls_records.csv`, `rb_decisions.json`, `rb_decisions_records.csv`); the launcher, the input builder and its `id_map.json`; the Ollama journal's load lines and request counts for the window; the retained copy's hashes and fingerprint at the move. Each script runs from the repository root as `.venv/bin/python docs/session-reports/session-12/12g_rb/<script> ~/scratch/retained/rb_12g`.
