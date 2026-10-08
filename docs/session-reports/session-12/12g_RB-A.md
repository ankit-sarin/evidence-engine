# 12g — Front-half rehearsal, Phase A (the design read)

**Status: COMPLETE.** Read-only. No review created, no model call, no Ollama call, no directory
under `data/`. Live `review.db` opened `mode=ro` only. Ends at a STOP.

| | |
| --- | --- |
| HEAD at the read | `6d9a56a5adace29aad11f124d2bc0ef73ed88794` |
| Live fingerprint | IDENTICAL to the migration-02 record at start and after the push |
| Script | `12g_rb/rb_sample.py` → `12g_rb/rb_summary.json`, `12g_rb/rb_sample.csv` |

Every count below is a value of those two files.

**One premise of the brief does not hold.** The abstract stage writes **no events**. "Eligibility
events from a screened review" cannot be measured by this rehearsal, because abstract screening
has not been moved onto the event store (S4, junior). It writes decision rows and `papers.status`.
Also: a screen-start manifest declares every stage the run could reach, not only the ones it
calls, so "declares exactly the calls" is measured as "every call is declared".

## 1. The inferred items

**I1 — confirmed.** `engine/adjudication/import_screening_entry.py::import_screening_entry(review_db, input_path, *, spec, git=None, digest_fn=None)`. Input: `{"source": "<non-empty>", "papers": [{"title": str, "pmid"?: str | null, "doi"?: str | null, "abstract"?: str | null, "authors"?: [str] | null, "journal"?: str | null, "year"?: int | null}]}`. "The import writes what the search stage writes and nothing more: one `papers` row per entry at INGESTED, under an `import` manifest whose `inputs` pin the file (R288). No paper event …, no parsed text, no workflow stage …, no model call." The review must hold no papers row; within-file duplicates are refused. No CLI.

**I2 — partly false.**
- Both primary passes, same model: `run_screening` calls `screen_paper(paper, spec, pass_number=1, model=primary_model)` then `pass_number=2`, with `primary_model = spec.screening_models.primary` (`qwen3:8b`); each is one `ollama_chat` under stage `abstract_screen_primary`.
- **What it writes:** two `abstract_screening_decisions` rows per record (`add_screening_decision`, one commit each), then `papers.status` by `update_status` — `ABSTRACT_SCREENED_IN` on two includes, `ABSTRACT_SCREENED_OUT` on two excludes, `ABSTRACT_SCREEN_FLAGGED` on a disagreement or a malformed response. A checkpoint file `screening_checkpoint.json` every ten records, removed at the end. **No `paper_events` row and no `field_events` row**: `update_status` writes none and the screener calls no event writer.
- **"Declares exactly the stages it calls" is false.** `_open_run_manifest` declares the configured stages of every pipeline stage from the start to the end: for `--skip-to screen`, `abstract_screen_primary`, `abstract_screen_verifier`, `audit`, `extract_pass1`, `extract_pass2`, `extract_retry_snippet`, `preflight:deepseek-r1:32b`, `preflight:gemma3:27b`, `preflight:qwen3:8b`, `vision_parse` — ten rows, run kind `extraction`. The run calls two of them (`preflight:qwen3:8b`, `abstract_screen_primary`) and stops at the gate. Calls are a subset of declarations by design ("the resolved configuration of every stage this run can call").
- A consequence to expect, not a finding: because extract stages are declared, `open_run` registers and pins the arm `local_deepseek_r1_32b` in the fresh review. Computed with no fetch: `defd293bfe62e427b906e072dcf56d0ca6e321e7d1ca4ad1fde3758fb84f3e6e`, the non-elicited pin, equal with `review_id` changed (True).

**I3 — confirmed: no lock.** `engine/agents/screener.py` imports nothing from `ollama_lock`; `run_screening` opens with `require_preflight([primary_model], runner_name="Abstract screening")` (no `spec=`, row C49) and loops. The whole-run launcher supplies the lock.

**I4 — confirmed.** `run_pipeline`: `if start_idx <= STAGES.index("screen"): results["screen"] = _stage_screen(db, spec, limit)` runs to completion, then `target_stage = STAGES[start_idx] if start_idx > STAGES.index("screen") else "parse"`; `if not is_adjudication_complete(db._conn): … _finish_review_run(db, run_id, "interrupted", reason=rm.REASON_BLOCKED_ADJUDICATION); return`. `is_adjudication_complete` is `is_stage_done(conn, "ABSTRACT_ADJUDICATION_COMPLETE")`. In a fresh review nothing is complete: `run_screening` never completes `ABSTRACT_SCREENING_COMPLETE` (only `run_verification` does, E9), so the blocker named will be `ABSTRACT_SCREENING_COMPLETE`.

**I5 — confirmed.** Live's rows written during screening (not the backfilled ones): 1,798 gaps between consecutive decisions, median 2.85 s, 90th percentile 3.56 s, maximum 6.36 s. One decision is one call; a record is two.

## 2. What the rehearsal exercises, and what it cannot

| Exercises | How it shows |
| --- | --- |
| C55 on real models: the abstract preflight is declared and recorded | a `run_calls` row `preflight:qwen3:8b`, `completed`, under a declared stage row |
| C54: every call is declared | `run_calls` stages ⊆ `run_stage_configs` stages; no `UndeclaredCall` / `UndeclaredOverride` in the log |
| C57: a screen start stops at the adjudication gate | manifest `('interrupted', "blocked:adjudication")`; no parse, extract, audit or export call |
| The gate on real workflow state | the log's "Current stage: ABSTRACT_SCREENING_COMPLETE"; twelve `workflow_state` rows, all pending |
| The screening-entry import feeding `run_screening` | every imported record leaves `INGESTED` with two decision rows |
| E5 on real input: a record with no abstract, or a one-character one, is screened like any other | those records' decisions, and the prompt's absent-abstract fallback (`render.absent_abstract_fallback`) |
| Input fit on the longest abstracts (27,606 and 26,352 characters) | their `input_fit` log lines |
| The arm pin at a screen-start run | `arms.pinned_sha256` = `defd293b…3e6e` |

| Cannot exercise | Why (existing row) |
| --- | --- |
| Eligibility or any other events from abstract screening | the abstract stage writes none (S4, junior) |
| Abstract verification (`gemma3:27b`) | `run_verification` has no production caller (E9); its stage is declared and gets zero calls |
| `ABSTRACT_SCREENING_COMPLETE` completing | only `run_verification` completes it (E9) |
| Full-text screening | no `run_pipeline` stage |
| A screening bound | `--limit` is a no-op on screen (B-F12, R525); it is not passed |
| Search, deduplication | the import replaces them |
| Restart and retry behaviour (B-F13's orphan rows) | needs an interruption; none is planned |

## 3. The sample

**N = 42**, from live's 10,039 records, by this rule (in `rb_sample.py`): within each stratum, the ascending-id list sampled at even spacing; strata with ordinary abstracts are limited to 400–3,000 characters so that length is tested only where it is meant to be.

| Stratum (live's March record) | Population | Taken |
| --- | --- | --- |
| abstract under 200 characters (E5) | 309 | 2 |
| both passes exclude | 6593 | 12 |
| both passes include, excluded later at abstract (verifier or PI) | 406 | 4 |
| included at abstract, eligible today | 175 | 10 |
| included at abstract, excluded at full text | 155 | 6 |
| longest abstracts | 2 longest of 10,039 | 2 |
| no abstract (E5) | 1595 | 2 |
| passes disagreed | 16 | 4 |

| # | Live id | Abstract chars | March pass 1 / pass 2 | March verifier | Live status today | Stratum |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 1 | 1,762 | exclude / exclude | — | ABSTRACT_SCREENED_OUT | both passes exclude |
| 2 | 5 | 1,006 | include / include | include | FT_SCREENED_OUT | included at abstract, excluded at full text |
| 3 | 9 | 1,788 | include / include | include | AI_AUDIT_COMPLETE | included at abstract, eligible today |
| 4 | 18 | 1,251 | include / include | exclude | ABSTRACT_SCREENED_OUT | both passes include, excluded later at abstract (verifier or PI) |
| 5 | 21 | 0 | exclude / exclude | — | ABSTRACT_SCREENED_OUT | no abstract (E5) |
| 6 | 95 | 147 | exclude / exclude | — | ABSTRACT_SCREENED_OUT | abstract under 200 characters (E5) |
| 7 | 281 | 1,497 | include / include | include | AI_AUDIT_COMPLETE | included at abstract, eligible today |
| 8 | 361 | 1,951 | include / include | include | FT_SCREENED_OUT | included at abstract, excluded at full text |
| 9 | 382 | 1,412 | include / exclude | include | ABSTRACT_SCREENED_OUT | passes disagreed |
| 10 | 400 | 1,227 | include / include | include | AI_AUDIT_COMPLETE | included at abstract, eligible today |
| 11 | 463 | 1,113 | include / include | include | AI_AUDIT_COMPLETE | included at abstract, eligible today |
| 12 | 494 | 1,147 | include / include | include | FT_SCREENED_OUT | included at abstract, excluded at full text |
| 13 | 497 | 886 | include / include | include | AI_AUDIT_COMPLETE | included at abstract, eligible today |
| 14 | 538 | 1,759 | include / include | include | AI_AUDIT_COMPLETE | included at abstract, eligible today |
| 15 | 563 | 1,429 | include / include | include | FT_SCREENED_OUT | included at abstract, excluded at full text |
| 16 | 577 | 810 | include / include | include | AI_AUDIT_COMPLETE | included at abstract, eligible today |
| 17 | 639 | 1,364 | include / include | include | AI_AUDIT_COMPLETE | included at abstract, eligible today |
| 18 | 656 | 1,498 | include / include | include | FT_SCREENED_OUT | included at abstract, excluded at full text |
| 19 | 699 | 1,399 | include / include | include | AI_AUDIT_COMPLETE | included at abstract, eligible today |
| 20 | 801 | 1,774 | include / include | include | AI_AUDIT_COMPLETE | included at abstract, eligible today |
| 21 | 803 | 1,334 | include / include | include | FT_SCREENED_OUT | included at abstract, excluded at full text |
| 22 | 998 | 1,568 | exclude / include | — | ABSTRACT_SCREENED_OUT | passes disagreed |
| 23 | 1321 | 1,503 | exclude / exclude | — | ABSTRACT_SCREENED_OUT | both passes exclude |
| 24 | 1654 | 1,044 | include / include | exclude | ABSTRACT_SCREENED_OUT | both passes include, excluded later at abstract (verifier or PI) |
| 25 | 2000 | 26,352 | exclude / exclude | — | ABSTRACT_SCREENED_OUT | longest abstracts |
| 26 | 2102 | 2,127 | include / exclude | — | ABSTRACT_SCREENED_OUT | passes disagreed |
| 27 | 2198 | 2,785 | exclude / exclude | — | ABSTRACT_SCREENED_OUT | both passes exclude |
| 28 | 2969 | 1,325 | exclude / exclude | — | ABSTRACT_SCREENED_OUT | both passes exclude |
| 29 | 2976 | 27,606 | exclude / exclude | — | ABSTRACT_SCREENED_OUT | longest abstracts |
| 30 | 3273 | 1,867 | include / include | exclude | ABSTRACT_SCREENED_OUT | both passes include, excluded later at abstract (verifier or PI) |
| 31 | 3721 | 925 | exclude / exclude | — | ABSTRACT_SCREENED_OUT | both passes exclude |
| 32 | 4455 | 1,290 | exclude / exclude | — | ABSTRACT_SCREENED_OUT | both passes exclude |
| 33 | 5188 | 1,381 | exclude / exclude | — | ABSTRACT_SCREENED_OUT | both passes exclude |
| 34 | 5916 | 1,000 | exclude / exclude | — | ABSTRACT_SCREENED_OUT | both passes exclude |
| 35 | 6668 | 1,647 | exclude / exclude | — | ABSTRACT_SCREENED_OUT | both passes exclude |
| 36 | 7380 | 1,226 | exclude / exclude | — | ABSTRACT_SCREENED_OUT | both passes exclude |
| 37 | 8121 | 2,157 | exclude / exclude | — | ABSTRACT_SCREENED_OUT | both passes exclude |
| 38 | 8754 | 1 | exclude / exclude | — | ABSTRACT_SCREENED_OUT | abstract under 200 characters (E5) |
| 39 | 9682 | 780 | exclude / include | — | ABSTRACT_SCREENED_OUT | passes disagreed |
| 40 | 9688 | 2,244 | include / include | exclude | ABSTRACT_SCREENED_OUT | both passes include, excluded later at abstract (verifier or PI) |
| 41 | 9719 | 1,040 | exclude / exclude | — | ABSTRACT_SCREENED_OUT | both passes exclude |
| 42 | 10039 | 0 | exclude / exclude | — | ABSTRACT_SCREENED_OUT | no abstract (E5) |

The March decisions are for an **informational** agreement table only. The screening prompts have changed since many of those rows were written (role-aware prompts, commit `234125c`, 2026-03-09), and the decisions for the 9,787 net-new records were produced by `scripts/screen_expanded.py` and backfilled by migration 003, so disagreement is not a defect and agreement is not a gate.

**Time.** 42 records × 2 calls at the March median 2.85 s is 4 minutes; at the March maximum 6.36 s, 9 minutes; plus one model load. **Estimate 4–9 minutes; abort line 13 minutes (1.5×).**

**Window.** Any start from 09:30 UTC to 08:30 UTC next day: clear of 08:55–09:25, nothing spans 09:00, and the directory is out of `data/` before the nightly.

## 4. Construction and launch (nothing below was executed)

1. **Input, outside the repo:** `~/scratch/12g-rb/input/entry.json`, built from live `mode=ro`: `source`, and per record `title`, `pmid`, `doi`, `abstract`, `authors`, `journal`, `year`, in the table's order. Empty abstracts go in as `null`, as live holds them.
2. **Review id `rb_12g`:** `data/rb_12g/` with a byte copy of live's codebook (`open_run` loads the codebook beside the database) and `spec.yaml`.
3. **Spec diff against live, one line:**
   ```
   -review_id: surgical_autonomy
   +review_id: rb_12g
   ```
   Nothing else is needed: the screen stage reads `screening_models`, the PICO and the eligibility block, all unchanged. `elicitation` stays `false`.
4. **Import:** `import_screening_entry(ReviewDatabase('rb_12g'), '<input>/entry.json', spec=load_spec_for('rb_12g', 'data/rb_12g/spec.yaml'))`. Expected: 42 papers at `INGESTED`, one `import` manifest, no events, no workflow rows.
5. **Launch:** the 12g-RA-B launcher with its argv changed, in tmux, whole run under `hold_experiment_lock(blocking=False)`:
   `run_pipeline --review rb_12g --spec data/rb_12g/spec.yaml --skip-to screen` (no `--limit`, no `--max-papers`: the latter is refused only for a start after extract, and bounds nothing here).
   Log to `data/rb_12g/logs/rb_12g_run.log` and `~/scratch/12g-rb/run.log` (R556's form).
6. **At run open:** read `arms.pinned_sha256`; expected `defd293bfe62e427b906e072dcf56d0ca6e321e7d1ca4ad1fde3758fb84f3e6e`. `open_run` fetches digests for all four declared models (`qwen3:8b`, `gemma3:27b`, `deepseek-r1:32b`, `qwen2.5vl:7b`; all four have manifests on disk).
7. **After the manifest closes:** `lsof` empty, hash and fingerprint, move `data/rb_12g/` to `~/scratch/retained/rb_12g/`, hash and fingerprint again; live `--compare`.

**Expected end state.** Manifest 2 `('interrupted', "blocked:adjudication")`. `run_calls`: 1 `preflight:qwen3:8b` and 84 `abstract_screen_primary`, all `completed`, `paper_id` NULL on each (the screener passes none). 84 `abstract_screening_decisions` rows. Every paper at `ABSTRACT_SCREENED_IN`, `ABSTRACT_SCREENED_OUT` or `ABSTRACT_SCREEN_FLAGGED`. Zero `paper_events`, zero `field_events`. Twelve `workflow_state` rows, all pending, written by the gate's own check (C37). One arm, pinned. No parsed text, no export, no checkpoint file left.

**Residency.** `MAX_LOADED_MODELS=1`: the run loads `qwen3:8b` and evicts `gemma3:27b`, which Rehearsal A left resident. It ends with `qwen3:8b` resident, as the box was before today.

## 5. Measurements

| Measurement | Source | Analysis script (to be written, `12g_rb/`) |
| --- | --- | --- |
| Calls per stage against declarations | `run_calls` grouped by `stage`, `outcome`; `run_stage_configs` | `rb_calls.py` |
| Declared stages with zero calls | the same two tables | `rb_calls.py` |
| Any undeclared call or override | the log (`UndeclaredCall`, `UndeclaredOverride`); `run_calls.outcome = 'error'` | `rb_calls.py` |
| Decisions per record | `abstract_screening_decisions` (pass 1, pass 2), `papers.status` | `rb_decisions.py` |
| Events written | `paper_events`, `field_events` counts by `run_id` (expected 0) | `rb_decisions.py` |
| `workflow_state` rows touched | the table after the run; the log's blocker line | `rb_decisions.py` |
| The gate's close | `run_manifests.end_status`, `end_reason`; the log's `BLOCKED: Adjudication workflow incomplete.` | `rb_calls.py` |
| Per-record timing | `run_calls.started_at` / `ended_at`, paired to records in order (two consecutive calls per record, ascending `papers.id`), checked against `abstract_screening_decisions.decided_at` | `rb_calls.py` |
| Prompt size and input fit | the log's `input_fit` lines (`chars`, `count`, `ratio`, `done_reason`) | `rb_calls.py` |
| Agreement with March (informational) | this run's pair against `rb_sample.csv`'s March pair, by stratum | `rb_decisions.py` |
| Foreign model loads (R556's form) | `journalctl -u ollama` for the window, against `run_calls` | `rb_calls.py` |

## 6. Abort conditions and retention

**Stop the run (SIGINT to the tmux pane) if:** wall time passes 13 minutes; Ollama's `NRestarts`, `ActiveEnterTimestamp` or `ExecMainStartTimestamp` differs from the launch values; any `UndeclaredCall` or `UndeclaredOverride`; any `refused_input_*` outcome; the pin at open is not `defd293b…3e6e`; or the run passes the gate into PARSE (a `vision_parse` call or a "STAGE: PARSE" line). A stopped run is moved out of `data/` and retained; no analysis without a ruling.

**Retention row (R31; drafted):**

| Artifact | Ruling | Disposition |
| --- | --- | --- |
| The throwaway review `rb_12g` — `~/scratch/retained/rb_12g/` (`review.db` and sidecars, `spec.yaml`, `extraction_codebook.yaml`, `logs/`) and `~/scratch/12g-rb/input/entry.json` | R31, R235, R241(iv), R309 | **RETAINED to the freshman freeze.** The front-half rehearsal (12g): a fresh review by the screening-entry import, 42 records, `run_pipeline --skip-to screen`, on the engine before the P-package fixes. Manifest end, call counts, `review.db` file sha256 and fingerprint overall recorded at the move. |

## 7. A-8's class and package (R560)

Assessment A's claim: the `[Sn]` markers inflate the prompt's token count beyond what the size estimate assumes. Measured in Rehearsal A (`12g_ra/ra_tokens.json`): 0.258 tokens per request character at the median on marked Pass-1 text and 0.297 at most, against 0.224–0.269 on Pass 2 (no markers) and the guard's 0.19. So the inflation is real and about a tenth.

It is not a silent failure. The pre-call check is a refusal floor ("an input refused at it cannot fit however favourably it tokenizes"), and the post-call check compares the model's own count with the ceiling, so an input that overran would raise `InputTruncated`. The only cost is a wasted call for a request between about 441,000 characters (ceiling ÷ 0.297) and 690,000 (ceiling ÷ 0.19); the corpus has no paper in that band (the largest that fits is 195,040; the next is 1,925,081).

**Proposed: Class 3, P7** — record the measured marked-text ratio beside the provisional constants (CONST-PROV-01) so the band is documented. A's remedies (a different marker, a tokenizer in `units.py`) change what the elicited run sends and are capability, not a pre-tag defect; if wanted, a sophomore row.

## 8. Findings (R509) and decisions

| # | Finding | Class | Package |
| --- | --- | --- | --- |
| RB-A1 | Abstract screening calls pass no `paper_id`, so their `run_calls` rows carry NULL — the abstract-stage form of C48 (FT) | 3 | P7, with C48 |
| RB-A2 | A screen-start manifest declares ten stage rows and pins the extraction arm for a run that makes screening calls only. By design (R73); recorded so the rehearsal's zero-call stages are not read as a defect | — (informational) | — |
| RB-A3 | The brief's "eligibility events from a screened review" has no source: the abstract stage writes no events until S4 | — (existing: S4, junior) | — |

**Decisions for the PI:**
1. **Sample:** 42 as listed (recommended); or a smaller set (one record per stratum, 8) since the gate and call checks do not depend on N.
2. **Lock:** the whole-run launcher, as in Rehearsal A (recommended).
3. **A-8:** Class 3, P7 as proposed; or close as not a defect.
4. **Window:** any start from 09:30 to 08:30 UTC.
