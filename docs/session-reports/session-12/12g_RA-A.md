# 12g — Rehearsal A, Phase A (the design read)

**Status: COMPLETE.** Read-only. No review created, no spec written, no model call, no Ollama
call, no directory under `data/`. Live `review.db` opened `mode=ro` only. Scratch under
`~/scratch/12g-ra/`. Ends at a STOP.

| | |
| --- | --- |
| HEAD at the read | `c85a49f59f1e8e5541ffba41fe7cc61ce3fa938f` |
| Live fingerprint | IDENTICAL to the migration-02 record at start and after the push |
| Script | `12g_ra/ra_sample.py` → `12g_ra/ra_summary.json`, `12g_ra/ra_candidates.csv` (one row per eligible paper) |

Every count and length below is a value of those two files.

**Two inferred items do not hold as written.** I5: `eval_count` (output tokens) is recorded
nowhere, so "output headroom" has no direct source. I9: the timing on disk is 10–16 minutes per
elicited paper, not 6–11. Neither blocks the rehearsal; both change what it can report and how
long it takes.

## 1. The inferred items

**I1 — confirmed.** `engine/adjudication/import_extraction_entry.py::import_extraction_entry(review_db, input_path, *, spec, git=None, digest_fn=None)`. Input: one JSON file, `{"source": …, "papers": [{"title", "pmid", "doi", …, "text_path": "<relative to the JSON file's directory>"}]}`. It "writes what the skipped stages would have written: each paper at FT_ELIGIBLE, its parsed text under the review's `parsed_text/` with a `parsed_text_refs` row, an `adjudicated` → `eligible` event under an `import` manifest (R202), and the eight screening stages of `workflow_state` complete. No model is called". It takes file bytes only: the texts are copied out of live by a read, and live is never opened by it. No acquisition tool is involved, so I22's hazard does not arise. Validation requires an empty review, an empty `parsed_text/`, and a pmid or doi per entry (all 190 eligible papers have one). No CLI; the 11c invocation is on disk at `~/scratch/retained/ee_extract_smoke_11c/ee_smoke_11c/logs/invocation.txt`.

**I2 — confirmed.** `engine/agents/extractor.py::extract_paper`: `if getattr(getattr(spec, "extraction_models", None), "elicitation", False): … return extract_paper_elicited(…)`. `extraction_stages`: `first = ("elicitation_pass1" if spec.extraction_models.elicitation else "extract_pass1")`. `scripts/run_pipeline.py::_open_run_manifest`: `if spec.extraction_models.elicitation and "extract_pass1" in stages: stages[stages.index("extract_pass1")] = "elicitation_pass1"`. `load_spec_for` requires the spec's `review_id` to equal `--review`.

**I3 — confirmed, computed.** `run_manifest.pin_tuple` hashes `arm_kind`, `provider`, `model`, per-stage `model`, `model_digest`, `options_hash`, `format_schema_hash`, `prompt_hash`, the codebook hash and the client library version. Review identity is not in it. Recomputed with no fetch and no database: the live spec with the flag flipped gives `77ae99d8671534aaa1350013f1cb6a4409a16eba7a1e097dad408188b81d1bf2`; the same with `review_id` changed gives `77ae99d8671534aaa1350013f1cb6a4409a16eba7a1e097dad408188b81d1bf2`; equal: True; stage rows equal: True. One condition: the prompt hash renders from the review's codebook, so the throwaway's `extraction_codebook.yaml` must be a byte copy of live's (the computation used live's file to stand in for it).

**I4 — answered: partly.** `run_pipeline` itself takes no lock. `run_extraction` does: `with hold_experiment_lock(): return _run_extraction_unlocked(…)`. The audit stage (`_stage_audit` → `require_preflight`, `audit_run`) runs after that block, unlocked. So a plain launch holds the lock for extraction and not for audit. The lock is re-entrant only inside one process ("A nested acquire in the same process must NOT re-flock … Nested calls bump a depth counter"). **An outer `flock(1)` on the lock file would deadlock the run**: `run_extraction`'s acquire is blocking on a new file description. The whole-run wrap must therefore be in-process (section 3).

**I5 — partly false.**
- `prompt_eval_count`: stored. `telemetry/extraction_calls.jsonl` (`pass1_prompt_eval_count`, `pass2_prompt_eval_count`; on the elicited path `extra.attempts[].prompt_eval_count` and `extra.pass1_prompt_chars` per Pass-1 attempt), and the pipeline log's `input_fit paper_id=N {"ceiling", "chars", "count", "done_reason", "estimate_low", "model", "ratio"}` line for every call.
- Input-fit outcome: stored. `run_calls.outcome` (`completed`, `refused_input_overflow`, `refused_ceiling_unavailable`, `refused_input_truncated`, `refused_input_dropped`, `error`) with `outcome_detail`.
- **`eval_count`: not stored anywhere.** No file under `engine/` reads it. `run_calls` has no token columns at all.
- Pass 2 format schema as sent: the body is not stored. `run_stage_configs.format_schema_hash` is (elicited `extract_pass2`: `2c9ec255d3b2a90d941cd4a81f8f9fd685c17d6077b4acb25b41c110d87a10b3`), and the schema is rebuildable from `stage_config("extract_pass2", spec).format` at the run's commit; equivalence is checked by hashing the rebuilt schema to the stored hash.

**I6 — confirmed.** `engine/core/extraction_events.py`: the `extracted` paper event's payload carries `"incomplete_fields": list(rec.incomplete_fields)` beside `asserted`, `contract_unmet`, `declined`; a record with no fields becomes `extraction_failed` with `detail={"incomplete_fields": …}`. So: `paper_events.payload_json`. Telemetry also records `missing_fields` per attempt.

**I7 — confirmed, and the link is weaker than stated.** `persist_unit_map`: `review_dir / "elicitation" / unit_map_dir_name / "unit_maps" / f"{paper_id}.json"`, with `unit_map_dir_name = _default_run_id()` = `run_%Y%m%dT%H%M%SZ`, one per process. `_LAST_PASS2_TELEMETRY["elicitation_run_id"] = unit_map_dir_name` is set, but `record_call` is never passed that key, so **the link is written to no file**. The rehearsal's join does not need it: a fresh review with one run in one process has exactly one `elicitation/run_*/` directory, joined to papers by file name.

**I8 — confirmed.** `engine/agents/audit_events.py::audit_run`: every live `asserted` claim without a `citation_located` event is located (`res = locate(text, c.source_snippet)`) and a `citation_located` field event is written for each, located or not (`payload=locate_payload(res, threshold=FUZZY_THRESHOLD, parsed_text_sha256=…, parsed_text_uid=…)`; the table's CHECK requires `$.located`). `LocateResult` carries `located`, `kind` (EXACT / FUZZY / NONE), `score`, `snippet_supplied`, `bridged`. Unlocated, populated claims go to `semantic_verify` and an `audit_verdicts` row.

**I9 — false as a number.** On disk, elicited (`data/surgical_autonomy/eval/elicit_design01/`, September smokes, `latency_s`): 600 s (paper 121, 21,348 characters), 643 s (604, 38,794), 963 s and 869 s (498, 148,805). That is 10–16 minutes per paper, four successful extractions of three papers, on the ELICIT-DESIGN-02 code. Non-elicited, 11c: extract 600.2 s, audit 41.9 s for one paper. At 600–963 s, 190 papers is 32–51 hours of extraction, against v76's 20–35.

## 2. The sample

**What the corpus offers** (190 eligible papers, current parsed text):
- The Pass-1 request is much larger than the text: median 1.79× (range 1.09–4.98×), because of the `[Sn]` markers and the fixed codebook prompt. Request size: median 76,010 characters, maximum 1,925,081.
- Ceiling: 131,072 tokens for `deepseek-r1:32b`, read from the 11c log's `input_fit` lines (no `/api/show` call was made here). The pre-call guard refuses at `chars × 0.19 ≥ ceiling`.
- **One paper is over the ceiling: [415].** No paper is expected to be truncated after the call, and **none sits between 36% and 100% of the ceiling** at the highest ratio observed (0.2419). There is no "near the ceiling" paper to pick.
- Docling comments: all 190 papers. Line-end hyphens: 4 papers. Ligature characters: 4 papers (the same four). Soft hyphens: 0.
- Multi-version on live: [455, 586, 699, 719]. A fresh review receives one text per paper, so the property does not carry over; the current version (v3) is what is imported.

**Proposed: N = 12, in this import order** (the run extracts in ascending throwaway `paper_id`, which is the import order).

| # | Live id | Version | Text chars | Units | Pass-1 request chars | Share of ceiling | Comments / hyphens / ligatures | Parsed-text sha256 | Why |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 121 | v1 | 21,348 | 152 | 53,602 | 10% | 7 / 0 / 0 | `2eff45aa80e1eee6eff579d8fdfb42cf317d386aff871c72801734314391ca13` | prior elicited smoke paper (600 s); first, for an early comparable result |
| 2 | 415 | v2 | 1,771,635 | 17,058 | 1,925,081 | 355% | 671 / 0 / 0 | `09933a869b91a816776452152ea70d244a334b72f626ce0a7a9b3e42f176af8a` | the one paper over the ceiling: a pre-call refusal is expected (`input_exceeds_context`) |
| 3 | 11 | v1 | 156,196 | 1,491 | 195,040 | 36% | 27 / 0 / 0 | `8e01895288b44a947ffb646509e713104de8df277108b9cc0efd9aa186d6f8b5` | largest request that fits |
| 4 | 498 | v2 | 148,805 | 1,064 | 185,509 | 34% | 79 / 0 / 0 | `3ff539a382b63237b0e3f5facc9fbfbc64a15572b78835c7c724b12101d51dcd` | second largest; prior smoke paper (869–963 s) |
| 5 | 607 | v2 | 130,677 | 1,249 | 169,547 | 31% | 47 / 0 / 0 | `86261a1fa24d72ce163fbb1465524a7c52dd0934b4a34202de258a39b8cd4863` | third largest |
| 6 | 748 | v2 | 56,017 | 1,146 | 95,271 | 18% | 16 / 15 / 64 | `79e2ac16a33519a74af81fc6f130d03342e4d5e62a79a880fabcba1b069091da` | most line-end hyphens (15), with ligatures |
| 7 | 368 | v2 | 55,731 | 1,082 | 94,572 | 17% | 10 / 8 / 92 | `dc407b8314caab8617642bf5948f8add6073c00a52e3b93e575b714c01328742` | second most line-end hyphens (8), with ligatures |
| 8 | 455 | v3 | 52,403 | 369 | 85,435 | 16% | 34 / 0 / 0 | `71d3448949a0952d4c08dd0b5ada3b6889115b534b5993d16636ce94916c6e51` | multi-version on live (v2, v3); current v3 is imported |
| 9 | 699 | v3 | 27,045 | 147 | 59,083 | 11% | 15 / 0 / 0 | `5313b9e201163cf5a79345a7a6a3325ab14c265c2f568501983c39be7ef54de7` | multi-version on live (v2, v3); short |
| 10 | 431 | v2 | 38,170 | 369 | 70,633 | 13% | 60 / 0 / 0 | `3c9924328f81971c44086e3aa42ea6f3db819ecf5962b79b381c27ceb33597f4` | densest Docling comments |
| 11 | 783 | v2 | 7,987 | 58 | 39,752 | 7% | 2 / 0 / 0 | `709549ee800d9e5924cd1b30fc9238c1bb0560a95d3f97daff5155c1c7b2ff67` | shortest text; the request is 4.98× the text |
| 12 | 604 | v2 | 38,794 | 304 | 71,825 | 13% | 18 / 0 / 0 | `50b6346a59007286b58b69f04655c19f0af73d8f3055056297c0b90050a35e5d` | prior smoke paper (643 s) |

Why 12: 11 papers are expected to extract, which clears the 10-paper floor so the distribution check runs instead of skipping; it stays under `RESTART_EVERY_N = 25`, so the run issues no proactive `sudo systemctl restart ollama`; and it covers each inspection target with at least two papers.

**Time.** 11 papers reach the model (733,173 characters of text in all). At the measured 600–963 s each: 1.8–2.9 h of extraction, plus about 8 minutes of audit at 11c's 42 s per paper. **Estimate 2.0–3.1 h.** A Pass-1 second attempt or a completeness retry lengthens a paper. The 1.5× abort line is 4.6 h.

**Window.** Cron on this box: 07:00 UTC Ollama health check (stands down under the lock), 09:00 the nightly suite, 09:30 another project's nightly. Start no earlier than 09:30 UTC and no later than 04:15 UTC, so that even the abort line ends before 08:55. The throwaway must also be out of `data/` before 09:00, because the nightly's inventory test counts every `data/<dir>/review.db` (R241).

## 3. Construction and launch (nothing below was executed)

1. **Input, outside the repo.** `~/scratch/12g-ra/input/`: the twelve parsed texts copied from live's `parsed_text/` by a read, each verified against its `parsed_text_refs.parsed_text_sha256`, and `entry.json` with `source`, and per paper `title`, `pmid`, `doi`, `abstract`, `authors`, `journal`, `year` read from live `mode=ro`, in the order above.
2. **Review id `ra_12g`.** `data/ra_12g/` holds `extraction_codebook.yaml` (a byte copy of live's, sha256 `89dbfa91…ad82`) and `spec.yaml` (below). The tree must be clean (`open_run` refuses `DirtyTree`); `data/` is gitignored.
3. **Spec diff against `review_specs/surgical_autonomy.yaml`**, two lines and nothing else:
   ```
   -review_id: surgical_autonomy
   +review_id: ra_12g
   -  elicitation: false
   +  elicitation: true
   ```
4. **Import** (11c's form): `PYTHONPATH=. .venv/bin/python -c "…import_extraction_entry(ReviewDatabase('ra_12g'), '<input>/entry.json', spec=load_spec_for('ra_12g', 'data/ra_12g/spec.yaml'))"`. Expected: 12 papers at FT_ELIGIBLE, one `import` manifest, 12 refs whose sha256 equal the table's, 0 arms.
5. **Live** is `--compare`d before step 1 and after the run. Nothing in steps 1–4 opens live for write.

**Lock and launch.** `run_pipeline --background` would hold the lock for extraction only (I4). To hold it for the whole run, a launcher outside `engine/` and `scripts/` runs the CLI inside the lock, in tmux:

```
# ~/scratch/12g-ra/launch_locked.py  (to be written at the run brief; not written here)
import runpy, sys
from engine.utils.ollama_lock import hold_experiment_lock
sys.argv = ["scripts/run_pipeline.py", "--review", "ra_12g", "--spec", "data/ra_12g/spec.yaml",
            "--skip-to", "extract", "--max-papers", "12"]
with hold_experiment_lock(blocking=False):      # refuse at once if another experiment holds it
    runpy.run_path("scripts/run_pipeline.py", run_name="__main__")
```

`tmux new-session -d -s ra_12g "cd ~/projects/evidence-engine && PYTHONPATH=. .venv/bin/python ~/scratch/12g-ra/launch_locked.py 2>&1 | tee data/ra_12g/logs/ra_12g_run.log"`. The pipeline command inside is the 11c one with the new id and bound. `--max-papers 12` is declared in the manifest (R236).

**Expected end:** the manifest closes `('interrupted', "blocked:audit_review")`; no export. 12 `eligible` papers; one `extraction_failed` / `input_exceeds_context` (throwaway paper 2, live 415) with a `run_calls` row `refused_input_overflow`; eleven `extracted` then `audited_ai`; the arm registered and pinned at `77ae99d8…1bf2` (not binding: the P1 fixes change what the elicited run sends, R539).

**After the run:** wait on the manifest's `end_status` (a bounded wait on the artifact, never a process-name search); move `data/ra_12g/` to `~/scratch/retained/ra_12g/` before any gate and before 09:00 UTC.

## 4. Measurements

| §8 measurement | Source | Exists today | Analysis script (to be written; under `docs/session-reports/session-12/12g_ra/`) |
| --- | --- | --- | --- |
| Estimated prompt tokens, per call | pipeline log, `input_fit … "estimate_low"` and `"chars"`; elicited Pass 1 also `extra.attempts[].prompt_chars` in `telemetry/extraction_calls.jsonl` | yes | `ra_tokens.py` |
| Actual prompt tokens (`prompt_eval_count`) | the same log line's `"count"` and `"ratio"`; telemetry `pass1_prompt_eval_count`, `pass2_prompt_eval_count`, `extra.attempts[].prompt_eval_count` | yes | `ra_tokens.py` — also the A-8 figure: actual tokens per request character on `[Sn]`-marked text against `RATIO_MIN` 0.19 |
| Output headroom | **no direct source: `eval_count` is not recorded.** Proxies: `ceiling − count` (room left after the prompt); `extra.pass1_content_chars`, `extra.pass1_thinking_chars`, `raw_content_chars` (output size in characters); `pass1_done_reason` / `finish_reason` = `length` (output hit a limit). The stage rows set no `num_predict` and no `num_ctx` | proxies only | `ra_tokens.py`, labelled as proxies |
| Input-fit events | `run_calls.outcome`, `outcome_detail`; log lines `input_fit REFUSED / TRUNCATED / DROPPED / UNVERIFIED`; `paper_events` `input_exceeds_context` | yes | `ra_tokens.py` |
| Pass 2 format schema sent | `run_stage_configs.format_schema_hash` for `extract_pass2`; body rebuilt from `stage_config("extract_pass2", spec).format` at the run's commit and hashed to it | hash yes; body by rebuild | `ra_schema.py` |
| `incomplete_fields` rates | `paper_events.payload_json` of each `extracted` event (and `extraction_failed` detail); telemetry `missing_fields`, `outcome` per attempt | yes | `ra_fields.py` — also what R535's floor (fewer than half of 20 fields) would have done to each paper |
| Contract outcomes | `field_events.event_type` (`asserted`, `declined`, `contract_unmet`); telemetry `extra.n_contract_unmet`, `extra.accepted_attempt`, `extra.attempts[].failed_fields` | yes | `ra_fields.py` |
| Unlocated claims | `field_events` `citation_located` with `$.located` false, `$.kind`, `$.score`; `audit_verdicts` | yes | `ra_unlocated.py` |
| Timing per paper and per call | `run_calls.started_at` / `ended_at` by stage and paper; `run_manifests` start and end | yes | `ra_tokens.py` |

**Unlocated-claim inspection** (`ra_unlocated.py`, read-only on the retained copy). For every `citation_located` event with `located` false: take the claim's `source_snippet`, the paper's parsed text (by the event's `parsed_text_sha256`), and the unit map (`elicitation/run_*/unit_maps/<paper_id>.json`) with the claim's cited indices (telemetry `extra.attempts[].fields`). Then classify, first match:
1. **A-10, comment adjacency** — the snippet is an exact substring of the comment-stripped text but not of the raw text.
2. **INT-g6-1, segmenter rewrite** — the snippet is not an exact substring of the stripped text either; record which of the cited units differ from their source slice and how (character rewrite, dropped ellipsis in a joined run).
3. **A-9** — the snippet matches the raw text after removing line-end hyphenation, or after NFKC folding of ligatures, or after removing soft hyphens (each tested separately and counted separately).
4. **Unexplained** — listed in full for reading.
Each class is reported per paper and in total, with the locator's `kind` and `score`. The same script reports located-by-FUZZY claims, since a fuzzy hit can hide classes 1–3.

## 5. Abort conditions and retention

**Stop the run (SIGINT to the tmux pane, which closes the manifest `interrupted`) if:**
- (the engine stops by itself on 3 consecutive failed papers, `CONSECUTIVE_FAILURE_ABORT = 3`, closing the manifest `aborted`; nothing to do but record it)
- `systemctl show ollama` shows `NRestarts` or `ExecMainStartTimestamp` changed from the values recorded at launch (a restart under the run), or the log shows the watchdog's last-resort restart;
- more than one paper ends `input_exceeds_context`, or any call records `refused_input_truncated` or `refused_input_dropped` (one refusal, paper 415, is expected: 1 of 12);
- wall time passes 4.6 h;
- any `UndeclaredCall` or `UndeclaredOverride` appears in the log (the engine raises; the run fails).
A stopped run is still moved out of `data/` and retained.

**Retention (R31 ledger row, proposed text):** "The throwaway review `ra_12g` — `~/scratch/retained/ra_12g/` (`review.db` and sidecars, `spec.yaml`, `extraction_codebook.yaml`, `parsed_text/`, `elicitation/run_*/unit_maps/`, `telemetry/`, `logs/`) and `~/scratch/12g-ra/input/` (the import's JSON and twelve texts) | R31, R235, R241(iv), R309 | RETAINED to the freshman freeze. Rehearsal A: a fresh review by the extraction-entry import, 12 papers, elicited, on the engine before the P1 fixes; its arm pin is not binding (R539). `review.db` file sha256 and fingerprint overall recorded at the move."

## 6. Known defects the run will show

| Defect | What the run will show | What the rehearsal records |
| --- | --- | --- |
| INT-g6-1 | some materialised snippets are not exact substrings of the text | class 2 counts from `ra_unlocated.py`; the units that differ |
| A-10 | a citation touching a Docling comment is unlocated (all 190 papers carry comments) | class 1 counts |
| A-9 | hyphenation and ligature misses, most likely in live 748 and 368 | class 3 counts, per kind |
| E-UNITMAP | the unit maps sit under `elicitation/run_<UTC>/`, linked to the run by nothing on disk | the directory name, and that the join was by file name |
| A-11 | a paper may store as `extracted` with fields missing after the budget | `incomplete_fields` per paper; papers under R535's floor |
| A-8 | actual tokens per character above the 0.19 estimate on marked text | the measured ratio per call |
| A-1 | none, if launched under the wrapper | the launch form used |
| B-F14 | after a watchdog timeout the earlier request keeps running | any `TimeoutError` in the log, with `run_calls` rows for that paper |

## 7. Findings (R509) and decisions

| # | Finding | Class | Package |
| --- | --- | --- | --- |
| RA-1 | `eval_count` is recorded nowhere, so output size in tokens and output headroom cannot be measured on any run, Run 7 included | 3 (telemetry) | P1, with the A-8 work |
| RA-2 | `elicitation_run_id` is set on `_LAST_PASS2_TELEMETRY` and never written, so the unit-map directory's link to its run is stored nowhere. Sizes E-UNITMAP; no new row | 1 (E-UNITMAP's) | P1 |
| RA-3 | An outer `flock(1)` on the lock file deadlocks `run_extraction`. A constraint on how A-1's fix (R528) and any lock-holding cron job are built | 3 (a note on A-1) | P1 |
| RA-4 | The Pass-1 request is a median 1.79× the text; one paper (415) is over the ceiling and none is near it. Informational for A-8 | — | — |
| RA-5 | Elicited extraction measured 10–16 minutes per paper on disk, so Run 7 is nearer 32–51 h than 20–35 h | — (planning) | — |

**Decisions for the PI:**
1. **Sample.** 12 as listed (recommended); or 6 (121, 415, 498, 748, 455, 431) for about half the time, which skips the distribution check; or another list.
2. **Lock.** The in-process wrapper for the whole run (recommended), or `--background` as 11c did, leaving the audit stage unlocked.
3. **Output headroom.** Accept the proxies for this rehearsal (recommended; recording `eval_count` is an engine change and belongs to P1), or hold the rehearsal until it is recorded.
4. **Paper 415.** Keep it, to see the refusal path end to end (recommended), or drop it.
5. **Window.** Any start between 09:30 and 04:15 UTC.
6. **Residency.** Calls send `keep_alive=-1`, so `deepseek-r1:32b` and `gemma3:27b` stay loaded after the run beside the resident `qwen3:8b`. Leave them, or have the operator unload them.
