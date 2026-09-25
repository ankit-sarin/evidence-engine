# WRITE-PATH-01 Phase 1 — slice-3 census (9c-P1)

**Tree:** HEAD `40ea017`, clean, level with origin. **Date:** 2026-09-25. **Mode:** read-only census.
No file under `engine/`, `scripts/` or `tests/` was edited. `review.db` was not opened by the census
(only `--compare`, mode=ro). No model call was made.

**Scope:** the five slice-3 items (R129): the legacy-fixture rewrites, readers 10–14, `ReviewDatabase`'s
legacy write methods, `count_populated_fields`' dict shape, and B9. The census re-measures the P2 and
P8 counts from the Phase 1a read-out at `80d8df1`, which were current as of that commit and not as of
this one.

**How to read the labels.** Grep locates, and reading classifies. Every classification here is
**READ**: the site or test and the code it calls were read at HEAD. Test-id counts come from
`pytest --collect-only` (Q6), never from grep. Q1's per-test classification was made by three
read-only census agents working from the Q6 id list. CC re-read the load-bearing claims against the
code before quoting them, and corrections are marked. Anchors are quoted content, never line numbers.

## Inferred-assumption results

| id | assumption | result |
|---|---|---|
| I1 | Of P8's 18 files, `test_retry_failed.py` is gone and `TestCheckLowYield`'s cases are gone; 17 present | **holds**: 17 of 18 present (`test_retry_failed.py` absent). All 22 P8 names: 21 present. The three retired `check_low_yield` cases are gone, but the class `TestCheckLowYield` survives, holding one test that reads the spec only (`test_threshold_from_review_spec`). |
| I2 | Reader 10's PARSED / FT_ELIGIBLE selection is out of scope; only its extraction-token reads are in | **holds** (READ; Q2) |
| I3 | Readers 11–13 depend on `evidence_spans.audit_status`, and no event-side table stores a per-span audit verdict | **holds** (READ). `audit_events.audit_run` writes `citation_located` field events and one `audited_ai` paper event. Verdicts go to `record_verdict(review_dir, ...)`, which appends to the file `audit_calls.jsonl` (`engine/core/audit_telemetry.py`, `TELEMETRY_FILENAME = "audit_calls.jsonl"`), not to a table. |
| I4 | Two distribution monitors exist: one migrated at 6a (`9370aee`), and `run_post_extraction_check`, which reads legacy tables | **FALSE** (READ; Q5, N1). There is one monitor. `run_post_extraction_check` is a gating wrapper around the same `check_distribution` that `9370aee` migrated, and it reads through `iter_grid`. No legacy table is read. |
| I5 | `add_extraction_atomic`, `update_audit` and `update_status` (extraction-stage token) have no production caller | **holds** (census; Q3). The only `update_status` calls with a post-extraction token (`"HUMAN_AUDIT_COMPLETE"`) are unreachable, behind `raise AuditAdjudicationDeprecated`. |
| I6 | `--collect-only` with the gate's arguments collects 2,876 ids | **holds as pytest counts it** (`2859/2876 tests collected (17 deselected)`). The committed list holds the **2,859** selected ids (Q6). |

## Q1 — Fixture census

**Method.** Grep located legacy constructs per file with the pattern `INSERT INTO extractions|INSERT INTO
evidence_spans|add_extraction_atomic|add_extraction\(|add_evidence_span|update_audit|update_status|workflow_state|UPDATE
papers SET status|UPDATE extractions|reset_for_reaudit|cleanup_orphaned_spans`. Every collected id in
each file was then read, including the fixtures and helpers it uses, and classified. A "legacy fixture"
is one that constructs or mutates `extractions`, `evidence_spans`, an extraction-stage `papers.status`
(EXTRACTED, EXTRACT_FAILED, AI_AUDIT_COMPLETE, HUMAN_AUDIT_COMPLETE, REJECTED) or `workflow_state`, or
that calls or fakes a `ReviewDatabase` legacy writer. Screening-token status writes alone are not
counted. The `ReviewDatabase.__init__` seed of `workflow_state` is not counted, since every test would
count.

**Classes.** (a) surviving engine code pinned through a legacy fixture → B5 rewrite to an event
fixture · **a†** a subset of (a): the only legacy construct is `workflow_state`, which is still the live
store and has **no** event-side equivalent · (b) retired code, or a fixture that is now **dead weight**
(the code under test never reads the rows it builds). No test of retired code survives; every (b) is
dead weight · (c) moves with a reader 10–14 or a reporting reader · (d) a `ReviewDatabase` legacy
method is itself the subject (slice-3 item 3; added by this census, since the brief's three classes do
not separate it from (a)) · (m) a test of an applied, checksummed migration whose subject is the legacy
table. It is neither rewritable nor dead.

### P8's 22 names at HEAD

| file | present | collected | still legacy? | dependent | a | a† | b | c | d | m |
|---|---|---|---|---|---|---|---|---|---|---|
| tests/test_extractor.py | yes | 25 | yes: fake `extract_paper` INSERTs `extractions` | 7 | 0 | 0 | 7 | 0 | 0 | 0 |
| tests/test_auditor.py | yes | 24 | **no** | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| tests/test_low_yield.py | yes | 13 | yes | 4 | 0 | 0 | 0 | 3 | 1 | 0 |
| tests/test_codebook_staleness.py | yes | 13 | yes | 7 | 0 | 0 | 0 | 2 | 4 | 1 |
| tests/test_extraction_cleanup.py | yes | 20 | yes | 13 | 7 | 0 | 6 | 0 | 0 | 0 |
| tests/test_atomic_terminal_write.py | yes | 6 | yes (`add_extraction_atomic`) | 4 | 0 | 0 | 0 | 0 | 4 | 0 |
| tests/test_codebook_provenance.py | yes | 13 | yes (`add_extraction_atomic`) | 2 | 0 | 0 | 0 | 0 | 2 | 0 |
| tests/test_ollama_client.py | yes | 27 | yes (`add_extraction_atomic`) | 1 | 0 | 0 | 0 | 0 | 1 | 0 |
| tests/test_database.py | yes | 43 | yes | 24 | 0 | 0 | 0 | 0 | 24 | 0 |
| tests/test_human_review.py | yes | 7 | yes | 7 | 0 | 0 | 0 | 7 | 0 | 0 |
| tests/test_audit_adjudication.py | yes | 15 | yes | 12 | 0 | 0 | 1 | 10 | 1 | 0 |
| tests/test_workflow.py | yes | 30 | `workflow_state` only | 20 | 0 | 16 | 0 | 4 | 0 | 0 |
| tests/test_ft_screening.py | yes | 67 | yes | 4 | 0 | 1 | 0 | 1 | 2 | 0 |
| tests/test_retry_failed.py | **no** (retired at `6e09166`) | — | — | — | — | — | — | — | — | — |
| tests/test_request_capture.py | yes | 12 | **no** (the extractor writes events) | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| tests/test_run_manifest.py | yes | 23 | one inert `'EXTRACTED'` token | 1 | 0 | 0 | 1 | 0 | 0 | 0 |
| tests/test_cloud_extraction.py | yes | 36 | yes: a copy of a legacy-schema DB | 36 | 24 | 0 | 12 | 0 | 0 | 0 |
| tests/test_extraction_validator.py | yes | 24 | yes | 13 | 0 | 0 | 0 | 13 | 0 | 0 |
| tests/test_citation_guard.py (fake) | yes | 12 | fake `_FakeDB.add_extraction_atomic`, never handed to engine code | 1 | 0 | 0 | 1 | 0 | 0 | 0 |
| tests/test_elicitation_pipeline.py (fake) | yes | 15 | **no**: `6e09166` replaced the fake with a capture of `write_extraction_events` | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| tests/_corpus_fixture.py (helper) | yes | — | yes: builds all 15 statuses plus `extractions` / `evidence_spans` rows, then derives events from statuses | — | | | | | | |
| tests/_event_store_fixture.py (helper) | yes | — | events only, except `status ... DEFAULT 'EXTRACTED'` in `_PAPERS_DDL` and `mirror_legacy_into_events`, which **reads** legacy rows | — | | | | | | |
| **total (20 test modules, 19 present)** | | **425** | | **156** | **31** | **17** | **28** | **40** | **39** | **1** |

`test_ft_screening.py`'s a† is `TestWorkflowFTStages::test_complete_ft_screening_stage`. The (c) count
for `test_workflow.py` is the four `format_workflow_status` tests, which read only `workflow_state`
(reader 14). `test_database.py`'s 24 (d) include 7 "subject only" ids that build no legacy row but
whose subject is a (d) method.

**Class totals: (a) 31 · a† 17 · (b) 28 · (c) 40 · (d) 39 · (m) 1 = 156 dependent of 425 collected.**
Every one of the 156 ids is in the Q6 list (checked with `grep -vxFf` against it: 0 missing).

**The two shared helpers.**
- `tests/_corpus_fixture.py` provides `FIXTURE_STATUSES` (all 15, literal), `STATUS_PROBE_IDS`,
  `FIXTURE_CARRIED` / `FIXTURE_CARRIED_NON_CORPUS`, `_POOL_STATUS_CYCLE`, a private minimal `_SCHEMA`,
  `_add_paper` (raw status INSERT), `build_fixture(root)` (papers, parsed text, `INSERT INTO extractions
  (id, paper_id)`, `INSERT INTO evidence_spans (extraction_id, field_name, value) VALUES (?,
  'study_type', ?)`, two `cloud_extractions` rows) and `_seed_event_store` (016 + 019, then
  `state_at_migration → eligible` for `_ELIGIBLE_FROM_STATUS`, which includes EXTRACT_FAILED, plus an
  `extraction_failed` processing event). **Imported by:** `tests/test_corpus_authority.py` only.
- `tests/_event_store_fixture.py` provides `ensure_event_store`, `fixture_run`,
  `seed_pre_manifest_paper_event`, `add_values`, `mirror_legacy_into_events`, `upgrade_event_store`,
  `seed_claim`, `run_for`, `seed_eligibility`, `open_extraction_run` (a real `open_run`) and
  `claim_identity`. Everything is event-only except the two items in the table above.
  **Imported by 23 files:** `tests/analysis/paper1/test_judge_loader.py`,
  `test_adjudication_pairs.py`, `test_audit_events.py`, `test_cloud_extraction.py`,
  `test_concordance.py`, `test_cut_over.py`, `test_distribution_monitor.py`,
  `test_effective_reader.py`, `test_event_writer_refusals.py`, `test_exporters.py`,
  `test_extraction_cleanup.py`, `test_extraction_events.py`, `test_extraction_run_link.py`,
  `test_extractor.py`, `test_non_value_tokens_downstream.py`, `test_ollama_preflight.py`,
  `test_parsed_text_resolver.py`, `test_readers_on_the_reader.py`, `test_request_capture.py`,
  `test_run_manifest.py`, `test_selection.py`, `test_stage_completion.py`,
  `test_two_axis_state_and_grid.py`. **`mirror_legacy_into_events` has one consumer,
  `tests/test_exporters.py` (18 of its 20 ids).** It declares its world with `add_extraction` /
  `add_evidence_span` / `update_audit` / `update_status` and then translates those rows into events. So
  retiring those writers breaks the translator as well as the files P8 named.

**Corrections to the census agents' notes, made by CC on re-read.** Group B reported that
`_collect_papers_for_review`'s LOW_YIELD branch is untested. That holds for
`test_audit_adjudication.py` only. At suite level the branch is driven by
`tests/test_low_yield.py::TestLowYieldInAuditQueue::{test_low_yield_papers_in_audit_export,test_export_includes_low_yield_spans}`.

The per-test tables follow, as the census agents wrote them.

### Q1 detail — group A

Read-only census. Counts are from `collected_ids_sorted.txt` (standard gate). `tests/conftest.py`
has no legacy construct (its autouse fixtures are `block_service_calls` and
`block_live_database` only), so every dependency below is module-local.

Class key: **(a)** surviving code pinned through a legacy fixture · **(b)** code retired, or the
legacy fixture is dead weight · **(c)** slice-3 legacy reader · **(d)** a ReviewDatabase legacy
method is itself the subject · **(m)** added by this census: an applied, checksummed migration
whose subject IS the legacy table. No event rewrite is possible, and the test is not dead. Kept
separate so it does not inflate (a).

Production-caller check (grep over `engine/ scripts/ analysis/`): `add_extraction`,
`add_extraction_atomic`, `add_evidence_span`, `update_audit` and `get_stale_extractions` have
**zero** callers outside `engine/core/database.py`. `check_stale_extractions` has one: the
extractor's pre-flight in `_extract_selected` (anchor: `stale_count = check_stale_extractions(db, schema_hash)`).

---

#### tests/test_extractor.py

Collected: 25

Legacy constructs present:
- `TestProactiveRestart._setup_db` and `TestRestartOllamaGraceful._setup_db` hand-roll the legacy schema (`CREATE TABLE extractions (` … `CREATE TABLE evidence_spans (`). Paper rows are inserted at `'FT_ELIGIBLE'`, a screening token. The two helpers are duplicates.
- `_make_fake_extract` (in both classes), a fake `extract_paper` that writes a legacy row: `"INSERT INTO extractions (paper_id, extraction_schema_hash, extracted_data, model, extracted_at) VALUES (?, ?, '[]', 'test', '2026-01-01')"`.
- Screening-only constructs (not legacy): `db.update_status(pid, "PARSED")` chains in `test_full_two_pass_mocked`, `test_staleness_skip` and `test_run_extraction_no_parsed_text`.

| test id | legacy construct it depends on | class | code it pins (module::function) |
|---|---|---|---|
| tests/test_extractor.py::TestProactiveRestart::test_restart_triggers_after_n_papers | `_make_fake_extract` INSERT INTO extractions; hand-rolled legacy schema | (b) dead weight | engine.agents.extractor::run_extraction (proactive restart) |
| tests/test_extractor.py::TestProactiveRestart::test_restart_disabled_when_zero | same | (b) dead weight | engine.agents.extractor::run_extraction |
| tests/test_extractor.py::TestProactiveRestart::test_restart_failure_continues_gracefully | same | (b) dead weight | engine.agents.extractor::run_extraction |
| tests/test_extractor.py::TestProactiveRestart::test_input_fit_failure_fails_the_paper_and_the_run_continues[dropped] | same (paper 2 goes through `_make_fake_extract`) | (b) dead weight | engine.agents.extractor::run_extraction / record_failure |
| tests/test_extractor.py::TestProactiveRestart::test_input_fit_failure_fails_the_paper_and_the_run_continues[overflow] | same | (b) dead weight | engine.agents.extractor::run_extraction / record_failure |
| tests/test_extractor.py::TestProactiveRestart::test_input_fit_failure_fails_the_paper_and_the_run_continues[truncated] | same | (b) dead weight | engine.agents.extractor::run_extraction / record_failure |
| tests/test_extractor.py::TestRestartOllamaGraceful::test_restart_failure_continues_extraction | same (`TestRestartOllamaGraceful._make_fake_extract`) | (b) dead weight | engine.agents.extractor::run_extraction |

Why (b): at HEAD `run_extraction` → `_extract_selected` counts "extracted" when
`extract_paper_with_completeness` returns, and reads nothing from `extractions`. The fake's
INSERT is never read. `check_stale_extractions` is the only reader of that table, and
`TestProactiveRestart` patches it (`return_value=0`). In `TestRestartOllamaGraceful` it is not
patched, but it runs once in pre-flight, before the loop, so the fake's rows are written after
the only read. The fix is to drop the INSERT, not to rewrite it to events. The hand-rolled
`CREATE TABLE extractions` is still needed in `TestRestartOllamaGraceful`, because the unpatched
`check_stale_extractions` queries it. `evidence_spans` is needed by `upgrade_event_store`, which
creates it if absent.

Not dependent: 18 ids

---

#### tests/test_auditor.py

Collected: 24

Legacy constructs present: none. Every test drives `grep_verify`, `_normalize`,
`semantic_verify`, `audit_span` or `INVALID_SNIPPET_RE` on in-memory dicts or strings. No
database is built.

(no dependent tests)

Not dependent: 24 ids

---

#### tests/test_low_yield.py

Collected: 13

Legacy constructs present:
- `_advance_to_ai_audit`: `db.update_status(pid, "EXTRACTED")` … `db.update_status(pid, "AI_AUDIT_COMPLETE")`, `db.add_extraction(`, `db.add_evidence_span(ext_id, …)`, `db.update_audit(s["id"], "verified", "gemma3:27b", "OK")`.
- `_seed_low_yield`: `"UPDATE extractions SET low_yield = 1 WHERE id = (SELECT MAX(id) "`.
- `tmp_db.reject_paper(pid, "low_yield_excluded: …")`, which moves papers.status to REJECTED.
- `test_low_yield_defaults_to_zero`: an inline `update_status(pid, "EXTRACTED")` plus `tmp_db.add_extraction(`.

| test id | legacy construct it depends on | class | code it pins (module::function) |
|---|---|---|---|
| tests/test_low_yield.py::TestLowYieldInAuditQueue::test_low_yield_papers_in_audit_export | `_advance_to_ai_audit` + `_seed_low_yield` | (c) reader 11 | engine.adjudication.audit_adjudicator::_collect_papers_for_review |
| tests/test_low_yield.py::TestLowYieldInAuditQueue::test_export_includes_low_yield_spans | `_advance_to_ai_audit` + `_seed_low_yield` | (c) reader 11 | engine.adjudication.audit_adjudicator::export_audit_review_queue |
| tests/test_low_yield.py::TestPrismaLowYield::test_prisma_includes_low_yield_rejected | `_advance_to_ai_audit` (status chain to AI_AUDIT_COMPLETE) + `reject_paper` → REJECTED; `_seed_low_yield` | (c) reporting reader | engine.exporters.prisma::generate_prisma_flow |
| tests/test_low_yield.py::TestLowYieldSchema::test_low_yield_defaults_to_zero | `update_status(... "EXTRACTED")` + `add_extraction(` | (d) | engine.core.database::ReviewDatabase.add_extraction (the column default) |

Note on the prisma test: `generate_prisma_flow` reads only `papers.status` and
`rejected_reason` (it counts `"low_yield" in reason.lower()`). It never reads `extractions`. So
the `add_extraction`, `add_evidence_span` and `update_audit` rows and `_seed_low_yield` are dead
weight in this test. Only the status chain and `reject_paper` carry the assertion.

Not dependent: 9 ids. One of these, `test_extractions_has_low_yield_column`, pins the legacy
**schema** (`PRAGMA table_info(extractions)`) but builds no rows. See Findings.

---

#### tests/test_codebook_staleness.py

Collected: 13

Legacy constructs present:
- Inline, in the three T4 tests and the count-predicate test: `db.update_status(pid, "EXTRACTED")` (via the `("ABSTRACT_SCREENED_IN", "PDF_ACQUIRED", "PARSED", "EXTRACTED")` loop) plus `db.add_extraction(pid, …, codebook_hash=…)`.
- The T5 parity tests build a raw sqlite file: `INSERT INTO extractions (paper_id, codebook_hash) VALUES (1, 'aaa')` / `VALUES (1, NULL)`.
- `test_013_rebuilds_a_database_that_still_has_the_constraint`: `INSERT INTO extractions (extraction_schema_hash, extracted_data)`.
- The `db_copy` fixture is `shutil.copy2(LIVE_DB, dest)`. It builds no rows; it copies the live database's legacy rows. See Findings.

| test id | legacy construct it depends on | class | code it pins (module::function) |
|---|---|---|---|
| tests/test_codebook_staleness.py::test_null_codebook_hash_is_stale | update_status → EXTRACTED + `add_extraction(` (no codebook_hash) | (d) | engine.core.database::ReviewDatabase.get_stale_extractions |
| tests/test_codebook_staleness.py::test_matching_codebook_hash_is_not_stale | same | (d) | engine.core.database::ReviewDatabase.get_stale_extractions |
| tests/test_codebook_staleness.py::test_differing_codebook_hash_is_stale | same | (d) | engine.core.database::ReviewDatabase.get_stale_extractions |
| tests/test_codebook_staleness.py::test_the_count_predicate_agrees_with_the_row_predicate | same, ×3 papers | (d) | engine.core.database::ReviewDatabase.get_stale_extractions **and** engine.utils.extraction_cleanup::check_stale_extractions (surviving; (a) on that half) |
| tests/test_codebook_staleness.py::test_parity_reads_codebook_hash_and_warns_without_blocking | raw `INSERT INTO extractions (paper_id, codebook_hash) VALUES (1, 'aaa')` | (c) reporting reader | engine.analysis.concordance::check_schema_parity |
| tests/test_codebook_staleness.py::test_parity_reports_null_as_none_recorded | raw `INSERT INTO extractions (paper_id, codebook_hash) VALUES (1, NULL)` | (c) reporting reader | engine.analysis.concordance::check_schema_parity |
| tests/test_codebook_staleness.py::test_013_rebuilds_a_database_that_still_has_the_constraint | raw `INSERT INTO extractions (extraction_schema_hash, extracted_data)` | (m) | engine.migrations.013_drop_schema_hash_not_null::run_migration |

Not dependent: 6 ids. `test_the_predicate_is_spelled_so_null_matches` is a source-text grep.
`test_the_current_hash_is_the_codebooks` builds no rows. The four `db_copy` / wiring tests
(`test_013_lifts_not_null_preserving_every_row`, `test_013_leaves_the_columns_in_place`,
`test_013_is_idempotent`, `test_013_is_wired`) construct nothing, but three of them read a copy
of the live legacy rows. See Findings.

---

#### tests/test_extraction_cleanup.py

Collected: 20

Legacy constructs present:
- `_add_extraction` (raw SQL): `"INSERT INTO extractions (paper_id, codebook_hash, extracted_data, "` and `"INSERT INTO evidence_spans (extraction_id, field_name, value, "`.
- `_advance_to`: `db.update_status` along paths ending at `"EXTRACTED"`, `"AI_AUDIT_COMPLETE"` or `"HUMAN_AUDIT_COMPLETE"`.
- `test_refusal_leaves_everything_under_a_blocking_trigger`: `CREATE TRIGGER block_ext_delete BEFORE DELETE ON extractions`.

| test id | legacy construct it depends on | class | code it pins (module::function) |
|---|---|---|---|
| tests/test_extraction_cleanup.py::TestDryRun::test_dry_run_reports_without_deleting | `_advance_to` EXTRACTED + `_add_extraction` (5 spans) | (a) | engine.utils.extraction_cleanup::cleanup_stale_extractions (dry run) |
| tests/test_extraction_cleanup.py::TestSchemaCleanup::test_confirm_refuses_and_keeps_every_extraction | `_advance_to` EXTRACTED + `_add_extraction` ×2 | (b) dead weight | engine.utils.extraction_cleanup::cleanup_stale_extractions (refusal) |
| tests/test_extraction_cleanup.py::TestSchemaCleanup::test_confirm_refuses_and_keeps_every_span | `_advance_to` EXTRACTED + `_add_extraction` (10 spans) | (b) dead weight | engine.utils.extraction_cleanup::cleanup_stale_extractions (refusal) |
| tests/test_extraction_cleanup.py::TestStatusReset::test_extracted_paper_is_not_reset | `_advance_to` EXTRACTED + `_add_extraction` | (b) dead weight | engine.utils.extraction_cleanup::cleanup_stale_extractions (refusal) |
| tests/test_extraction_cleanup.py::TestStatusReset::test_ai_audit_complete_paper_is_not_reset | `_advance_to` AI_AUDIT_COMPLETE + `_add_extraction` | (b) dead weight | engine.utils.extraction_cleanup::cleanup_stale_extractions (refusal) |
| tests/test_extraction_cleanup.py::TestStatusReset::test_report_does_not_count_human_audit_complete_as_resettable | `_advance_to` HUMAN_AUDIT_COMPLETE + `_add_extraction` | (a) | engine.utils.extraction_cleanup::cleanup_stale_extractions (`_RESETTABLE_STATUSES` read of papers.status) |
| tests/test_extraction_cleanup.py::TestDedup::test_dedup_is_reported_not_performed | `_advance_to` EXTRACTED + `_add_extraction` ×2 | (a) | engine.utils.extraction_cleanup::cleanup_stale_extractions (dedup dry run; refusal half is dead weight) |
| tests/test_extraction_cleanup.py::TestStaleExtractionCheck::test_check_stale_returns_count | `_advance_to` EXTRACTED + `_add_extraction` ×2 | (a) | engine.utils.extraction_cleanup::check_stale_extractions |
| tests/test_extraction_cleanup.py::TestStaleExtractionCheck::test_check_stale_zero_when_all_current | `_advance_to` EXTRACTED + `_add_extraction` | (a) | engine.utils.extraction_cleanup::check_stale_extractions |
| tests/test_extraction_cleanup.py::TestExtractionRunnerWarning::test_informs_when_stale_exist_and_names_no_deletion | `_advance_to` EXTRACTED + `_add_extraction` | (a) | engine.agents.extractor::_extract_selected pre-flight → extraction_cleanup::check_stale_extractions |
| tests/test_extraction_cleanup.py::TestExtractionRunnerWarning::test_silent_when_no_stale | `_advance_to` EXTRACTED + `_add_extraction` (current hash) | (a) | engine.agents.extractor::_extract_selected pre-flight → extraction_cleanup::check_stale_extractions |
| tests/test_extraction_cleanup.py::TestAtomicDelete::test_refusal_leaves_everything_under_a_blocking_trigger | `_advance_to` EXTRACTED + `_add_extraction` + delete trigger on extractions | (b) dead weight | engine.utils.extraction_cleanup::cleanup_stale_extractions (refusal) |
| tests/test_extraction_cleanup.py::TestAdminResetAuditTrail::test_refusal_writes_no_admin_reset | `_advance_to` AI_AUDIT_COMPLETE + `_add_extraction` | (b) dead weight | engine.utils.extraction_cleanup::cleanup_stale_extractions (refusal) |

Why the (b) rows are dead weight: `cleanup_stale_extractions(dry_run=False)` raises
`DeletionRetired(REFUSAL)` as its first statement, before `conn = db._conn`. The sibling test
`test_refusal_precedes_any_query` already proves this with a connectionless `NoDb`. The legacy
rows and statuses in these tests are read only by the test's own after-assertions, never by the
code under test.

Why the (a) rows cannot follow the stated remedy: `extraction_cleanup` exists to report on
`extractions`. Its tests cannot be rewritten to an event fixture, because the module has no
event-side input. See Findings.

Not dependent: 7 ids

---

#### tests/test_atomic_terminal_write.py

Collected: 6

Legacy constructs present:
- The `db` fixture: `"INSERT INTO papers (id, title, source, status, created_at, updated_at) "` … `'PARSED'`. A screening token only, so not legacy by itself.
- `db.add_extraction_atomic(` in four tests.
- Legacy-table count assertions: `"SELECT COUNT(*) FROM extractions"` / `"SELECT COUNT(*) FROM evidence_spans"`.

| test id | legacy construct it depends on | class | code it pins (module::function) |
|---|---|---|---|
| tests/test_atomic_terminal_write.py::test_all_twenty_states_land_in_one_transaction | `add_extraction_atomic(` | (d) | engine.core.database::ReviewDatabase.add_extraction_atomic |
| tests/test_atomic_terminal_write.py::test_the_terminal_state_rides_on_every_entry_not_only_the_unmet_ones | `add_extraction_atomic(` | (d) | engine.core.database::ReviewDatabase.add_extraction_atomic |
| tests/test_atomic_terminal_write.py::test_extracted_data_keeps_the_list_shape_downstream_readers_expect | `add_extraction_atomic(` | (d) | engine.core.database::ReviewDatabase.add_extraction_atomic (+ engine.agents.auditor::count_populated_fields on the stored JSON) |
| tests/test_atomic_terminal_write.py::test_a_failing_span_rolls_the_whole_paper_back | `add_extraction_atomic(` | (d) | engine.core.database::ReviewDatabase.add_extraction_atomic |

Not dependent: 2 ids. `test_a_pre_write_refusal_stores_nothing` builds no legacy rows (PARSED
paper only), but its witness is a legacy-table count. See Findings.
`test_no_uncited_non_escape_value_can_be_stored_as_a_value` is pure `check_citations`.

---

#### tests/test_codebook_provenance.py

Collected: 13

Legacy constructs present:
- `db.add_extraction_atomic(` in two tests (the papers are `add_papers` only, at INGESTED).
- The migration-012 synthetic databases do `CREATE TABLE extractions (...)` with **no rows**, so they are not row construction.
- The `db_copy` fixture is `shutil.copy2(LIVE_DB, dest)` and reads live rows. See Findings.

| test id | legacy construct it depends on | class | code it pins (module::function) |
|---|---|---|---|
| tests/test_codebook_provenance.py::test_add_extraction_atomic_stores_both | `add_extraction_atomic(` with `codebook_hash="cb-semantic", codebook_sha256="cb-bytes"` | (d) | engine.core.database::ReviewDatabase.add_extraction_atomic |
| tests/test_codebook_provenance.py::test_the_columns_default_to_null_when_not_supplied | `add_extraction_atomic(` without codebook kwargs | (d) | engine.core.database::ReviewDatabase.add_extraction_atomic |

Not dependent: 11 ids

---

#### Totals

| file | collected | (a) | (b) | (c) | (d) | (m) | dependent | not dependent |
|---|---|---|---|---|---|---|---|---|
| tests/test_extractor.py | 25 | 0 | 7 | 0 | 0 | 0 | 7 | 18 |
| tests/test_auditor.py | 24 | 0 | 0 | 0 | 0 | 0 | 0 | 24 |
| tests/test_low_yield.py | 13 | 0 | 0 | 3 | 1 | 0 | 4 | 9 |
| tests/test_codebook_staleness.py | 13 | 0 | 0 | 2 | 4 | 1 | 7 | 6 |
| tests/test_extraction_cleanup.py | 20 | 7 | 6 | 0 | 0 | 0 | 13 | 7 |
| tests/test_atomic_terminal_write.py | 6 | 0 | 0 | 0 | 4 | 0 | 4 | 2 |
| tests/test_codebook_provenance.py | 13 | 0 | 0 | 0 | 2 | 0 | 2 | 11 |
| **group A** | **114** | **7** | **13** | **5** | **11** | **1** | **37** | **77** |

No collected test in this group exercises code that was retired outright (`run_audit`,
`check_low_yield`, `reset_failed_papers`, `reextract_failed`, `reset_for_reextraction`, the
ELICIT smoke). The retired tests are already gone, and tombstone comments are left in their
place (anchors: `# test_full_audit_flow_mocked retired 2026-09-25 with run_audit` in
test_auditor.py; `# test_paper_below_threshold_flagged, test_paper_above_threshold_not_flagged and`
in test_low_yield.py). Every (b) here is dead weight, not retired code.

#### Findings

1. **Fake `extract_paper` still writes the legacy table (test_extractor.py, 7 ids).** Both copies
   of `_make_fake_extract` (docstring: "Return a fake extract_paper function that stores results
   in DB") INSERT into `extractions`. Since 9b-FLIP, nothing on the run path reads that row. The
   real extractor writes events, and `run_extraction` counts success by return value. Delete the
   INSERT. `TestRestartOllamaGraceful._setup_db` and `_make_fake_extract` duplicate
   `TestProactiveRestart`'s (comment anchor: "Re-use helpers from TestRunExtraction", a class that
   no longer exists).
2. **`check_stale_extractions` is a surviving legacy reader on the RUN PATH, and it is not on the
   slice-3 list.** `_extract_selected` calls it in pre-flight to log an "Informational" count of
   `extractions` rows without the current codebook hash. After the cut-over no run writes
   `extractions`, so on any database created after the cut-over the count is structurally 0. On
   live it reports the frozen Run-6 population forever. Its pinning tests
   (`TestExtractionRunnerWarning` ×2, `TestStaleExtractionCheck` ×2, and half of
   `test_the_count_predicate_agrees_with_the_row_predicate`) are (a) by definition, but they
   cannot be rewritten to an event fixture. The whole `engine/utils/extraction_cleanup.py` module
   (read-only report, `--confirm` retired) is the same case: surviving, legacy-only input,
   absent from the reader list. This needs a ruling: add it to slice 3 as a reader, or retire it
   under R31.
3. **Six refusal tests in test_extraction_cleanup.py build legacy rows the code never reaches.**
   `DeletionRetired` is raised before `db._conn` is touched, and `test_refusal_precedes_any_query`
   proves it. The "keeps every extraction/span/status" assertions cannot fail through the code
   under test, so they measure the fixture. This is the "check that passes before the change"
   pattern.
4. **Five ReviewDatabase legacy writers are pinned by tests and have zero production callers:**
   `add_extraction` (test_low_yield `_advance_to_ai_audit`, `test_low_yield_defaults_to_zero`,
   four staleness tests), `add_extraction_atomic` (four in test_atomic_terminal_write, two in
   test_codebook_provenance), `add_evidence_span` and `update_audit` (test_low_yield
   `_advance_to_ai_audit`), and `get_stale_extractions` (four staleness tests). All 11 (d) ids
   pin methods that nothing in `engine/`, `scripts/` or `analysis/` calls at HEAD. In particular,
   test_atomic_terminal_write.py's module docstring ("ELICIT-DESIGN-02 gate 3 — a paper is
   written whole or not at all") describes a guarantee of the retired legacy write path. The
   event-side atomicity equivalent would need its own test.
5. **Check that cannot fail: `test_a_pre_write_refusal_stores_nothing`.** It proves "stores
   nothing" by `SELECT COUNT(*) FROM extractions` / `evidence_spans` after calling only
   `enforce_citations`, which never writes. Nothing writes those tables any more, so the witness
   is 0 whether or not the guard raises. The meaningful witness would be `field_events` /
   `paper_events`, or the removal of the count lines. The same pattern, in a milder form, is the
   legacy-count asserts in `test_full_two_pass_mocked` and
   `TestProactiveRestart::test_zero_span_extraction_marks_extract_failed` (`SELECT COUNT(*) FROM
   extractions` == 0). These are true by construction at HEAD, but they sit beside event
   assertions that carry the real signal. Not counted as dependent, because they build no rows.
6. **Dead weight inside a (c) test:** in `TestPrismaLowYield::test_prisma_includes_low_yield_rejected`,
   `generate_prisma_flow` reads only `papers.status` and `rejected_reason`. The
   extraction, span and audit rows and `_seed_low_yield` do nothing there. When slice 3 moves
   prisma, only the status chain and `reject_paper` need an event equivalent.
7. **`_seed_low_yield` docstring is imprecise.** It says "readers 10–14 still read" the
   `low_yield` flag. In this file its only live consumer is reader 11
   (`audit_adjudicator._collect_papers_for_review`, anchor
   `"SELECT id, low_yield FROM extractions WHERE paper_id = ? ORDER BY id DESC LIMIT 1"`). Prisma
   (the third caller) does not read it.
8. **Tests over a copy of live data that pin legacy content** (not row construction, so not
   counted). `test_codebook_staleness.py::test_013_lifts_not_null_preserving_every_row` asserts
   `before["extractions"][0] == 190`. `test_codebook_provenance.py::test_existing_rows_are_null_not_backfilled`
   asserts that every live `extractions` / `cloud_extractions` / `review_runs` row has
   `codebook_hash IS NULL`. Both hold only while the live database is fixed. They are migration
   tests (012/013, applied and checksummed) and are not affected by slice 3, but the 190 is a
   captured count, not a derivation.
9. **Schema-only legacy pin:** `TestLowYieldSchema::test_extractions_has_low_yield_column` asserts
   that the legacy column exists. It is harmless until someone drops or freezes `extractions`,
   which would make it a retirement candidate alongside `low_yield`'s last reader (reader 11).
10. **Unused imports and helpers in test_auditor.py:** `ReviewDatabase`, `Citation`,
    `write_parsed`, and the helpers `_make_verified_response` / `_make_flagged_response` are
    leftovers of the retired `test_full_audit_flow_mocked`. This is cosmetic.

### Q1 detail — group B

Read-only. Collected counts come from `collected_ids_sorted.txt` (standard gate). Every id below was checked with `grep -F` against that file.

**Conventions used in this file**
- **"subject only"** in the construct column means the test builds no legacy row. It is listed because a class-(d) ReviewDatabase legacy method, or a class-(c) reader, is its subject. Those rows are counted separately in the totals so the caller can drop them.
- **a†** means class (a) whose "legacy fixture" is `workflow_state` only. The code under test is `engine/adjudication/workflow.py`, which is still present at HEAD. Its only store is `workflow_state`, and it reads no extraction table. No event-fixture equivalent exists, so these tests need rewriting only if slice 3 moves `workflow_state` itself.
- Seeding of `workflow_state` by `ReviewDatabase.__init__` (via `ensure_workflow_table`) happens in every test in the suite. It is **not** counted as a construct. A test counts only if the test, a helper it uses, or the code under test writes `workflow_state` (`complete_stage` / `bypass_stage` / `reset_stage` / `advance_stage` / `ensure_workflow_table`).

---

#### tests/test_ollama_client.py

Collected: **27**

Legacy constructs present:
- `class TestDigestInExtraction` calls `db.add_extraction_atomic(` with `model_digest="sha256:extractor_digest_abc"` / `auditor_model_digest=...`, then reads with `SELECT model_digest, auditor_model_digest FROM extractions`. It sets papers.status only through PARSED.

| test id | legacy construct it depends on | class | code it pins (module::function) |
|---|---|---|---|
| tests/test_ollama_client.py::TestDigestInExtraction::test_digest_columns_in_extraction_record | `add_extraction_atomic` writes `extractions` + `evidence_spans` | d | engine/core/database.py::ReviewDatabase.add_extraction_atomic (the `extractions.model_digest` / `auditor_model_digest` columns) |

Not dependent: 26 ids

---

#### tests/test_database.py

Collected: **43**

Legacy constructs present:
- `update_status(pid, "EXTRACTED")` / `"AI_AUDIT_COMPLETE"` / `"HUMAN_AUDIT_COMPLETE"` inline walks
- `def _walk_to_ai_audit(db, pmid)`: `add_extraction` + 2× `add_evidence_span` + `update_audit` (verified/flagged) + `update_status(pid, "AI_AUDIT_COMPLETE")`
- `db.add_extraction(`, `db.add_evidence_span(`, `db.update_audit(`, `db.add_extraction_atomic(`
- raw `INSERT INTO evidence_spans` (the L3 NOT NULL tests)
- `db.reset_for_reaudit()`, `db.reject_paper(`, `db.min_status_gate(`, `db.get_pipeline_stats()`, `db.cleanup_orphaned_spans()`, `db.admin_reset_status(`, `db.get_stale_extractions(`

| test id | legacy construct it depends on | class | code it pins (module::function) |
|---|---|---|---|
| tests/test_database.py::test_full_lifecycle | `update_status` → EXTRACTED → AI_AUDIT_COMPLETE | d | database.py::ReviewDatabase.update_status (+ ALLOWED_TRANSITIONS extraction edges) |
| tests/test_database.py::test_ai_to_human_audit_transition | `update_status` through AI_AUDIT_COMPLETE → HUMAN_AUDIT_COMPLETE | d | database.py::ReviewDatabase.update_status |
| tests/test_database.py::test_invalid_transition_raises | subject only: `update_status(pid, "EXTRACTED")` from INGESTED, refused, no row written | d | database.py::ReviewDatabase.update_status (extraction token as target) |
| tests/test_database.py::test_staleness_detection | `update_status` → EXTRACTED; `add_extraction(... codebook_hash=old_hash)` | d | database.py::ReviewDatabase.get_stale_extractions (+ add_extraction) |
| tests/test_database.py::test_evidence_spans_and_audit | `add_extraction` + `add_evidence_span` + `update_audit(span_id, "verified", ...)` | d | database.py::ReviewDatabase.add_extraction / add_evidence_span / update_audit |
| tests/test_database.py::test_evidence_spans_contested_status | same trio, `update_audit(..., "contested", ...)` | d | database.py::ReviewDatabase.update_audit (+ evidence_spans.audit_status CHECK) |
| tests/test_database.py::test_evidence_spans_invalid_snippet_status | same trio, `update_audit(..., "invalid_snippet", ...)` | d | database.py::ReviewDatabase.update_audit (+ evidence_spans.audit_status CHECK) |
| tests/test_database.py::test_atomic_extraction_commits_all | `update_status` → EXTRACTED; `add_extraction_atomic` with 15 spans | d | database.py::ReviewDatabase.add_extraction_atomic |
| tests/test_database.py::test_atomic_extraction_rolls_back_on_failure | `update_status` → EXTRACTED; `add_extraction_atomic` with a NULL-value span | d | database.py::ReviewDatabase.add_extraction_atomic (rollback) |
| tests/test_database.py::test_reset_for_reaudit_atomicity | `_walk_to_ai_audit` | d | database.py::ReviewDatabase.reset_for_reaudit |
| tests/test_database.py::test_reset_for_reaudit_preserves_extraction_data | `_walk_to_ai_audit` | d | database.py::ReviewDatabase.reset_for_reaudit |
| tests/test_database.py::test_reject_paper | `_walk_to_ai_audit` | d | database.py::ReviewDatabase.reject_paper |
| tests/test_database.py::test_reject_paper_invalid_status | subject only: paper at INGESTED | d | database.py::ReviewDatabase.reject_paper (refusal) |
| tests/test_database.py::test_min_status_gate | `_walk_to_ai_audit` | d | database.py::ReviewDatabase.min_status_gate |
| tests/test_database.py::test_min_status_gate_missing_paper | subject only: no paper | d | database.py::ReviewDatabase.min_status_gate |
| tests/test_database.py::test_min_status_gate_missing_paper_logs_warning | subject only: no paper | d | database.py::ReviewDatabase.min_status_gate |
| tests/test_database.py::test_pipeline_stats | subject only: only screening tokens are set (ABSTRACT_SCREENED_IN/OUT); asserts `total_extractions == 0`, `total_evidence_spans == 0` | d (also reader 14) | database.py::ReviewDatabase.get_pipeline_stats |
| tests/test_database.py::test_cleanup_orphaned_spans | `update_status` → EXTRACTED; 2× `add_extraction` + 4× `add_evidence_span` | d | database.py::ReviewDatabase.cleanup_orphaned_spans |
| tests/test_database.py::test_admin_reset_status_succeeds_and_logs | `update_status` → EXTRACTED → AI_AUDIT_COMPLETE | d | database.py::ReviewDatabase.admin_reset_status |
| tests/test_database.py::test_admin_reset_invalid_target_raises | subject only: paper at INGESTED | d | database.py::ReviewDatabase.admin_reset_status |
| tests/test_database.py::test_normal_pipeline_cannot_use_admin_transition | `update_status` → EXTRACTED → AI_AUDIT_COMPLETE | d | database.py::ReviewDatabase.update_status (refuses AI_AUDIT_COMPLETE → PARSED) |
| tests/test_database.py::test_null_confidence_raises_integrity_error | `update_status` → EXTRACTED; `add_extraction`; raw `INSERT INTO evidence_spans` with NULL confidence | d | database.py evidence_spans DDL (NOT NULL confidence), via add_extraction |
| tests/test_database.py::test_null_tier_raises_integrity_error | same, NULL `tier` | d | database.py evidence_spans DDL (NOT NULL tier), via add_extraction |
| tests/test_database.py::test_update_status_invalid_transition_still_raises | subject only: `update_status(pid, "EXTRACTED")` from INGESTED, refused | d | database.py::ReviewDatabase.update_status |

Not dependent: 19 ids. Of the 24 listed, 17 build legacy rows and 7 are subject only.

---

#### tests/test_human_review.py

Collected: **7**

Legacy constructs present:
- `def _make_audited_paper(db, tmp_path, pmid, span_statuses)`: `update_status` through `"EXTRACTED"`, then `db.add_extraction(pid, "hash", {}, "trace", "model")` + `db.add_evidence_span(` + `db.update_audit(` + `db.update_status(pid, "AI_AUDIT_COMPLETE")`. `write_parsed` (parsed_text_refs) is not legacy.
- test bodies read `SELECT audit_status FROM evidence_spans` and assert `get_papers_by_status("HUMAN_AUDIT_COMPLETE")`
- `test_span_and_status_atomic_on_failure` also creates `TRIGGER block_human_audit ... WHEN NEW.status = 'HUMAN_AUDIT_COMPLETE'` on `papers`

| test id | legacy construct it depends on | class | code it pins (module::function) |
|---|---|---|---|
| tests/test_human_review.py::test_export_review_queue_columns | `_make_audited_paper` | c (reader 12) | engine/review/human_review.py::export_review_queue |
| tests/test_human_review.py::test_import_accept | `_make_audited_paper` | c (reader 12) | human_review.py::import_review_decisions → _import_review_csv → _apply_audit_decisions (UPDATE evidence_spans; papers → HUMAN_AUDIT_COMPLETE) |
| tests/test_human_review.py::test_import_rejects_bad_corrected_snippet | `_make_audited_paper` | c (reader 12) | human_review.py::import_review_decisions (_import_review_csv validation) |
| tests/test_human_review.py::test_import_reject_value | `_make_audited_paper` | c (reader 12) | human_review.py::import_review_decisions (REJECT_VALUE → span value "NR") |
| tests/test_human_review.py::test_bulk_accept_does_not_transition_to_human | `_make_audited_paper` | c (reader 12) | human_review.py::bulk_accept |
| tests/test_human_review.py::test_span_and_status_atomic_on_failure | `_make_audited_paper` + trigger on papers.status HUMAN_AUDIT_COMPLETE | c (reader 12) | human_review.py::import_review_decisions → _import_review_json → _apply_audit_decisions (atomicity) |
| tests/test_human_review.py::test_paper_transitions_only_when_all_resolved | `_make_audited_paper` | c (reader 12) | human_review.py::_apply_audit_decisions (transition gate) |

Not dependent: 0 ids

---

#### tests/test_audit_adjudication.py

Collected: **15**

Legacy constructs present:
- `def _add_paper_with_extraction(db, pmid, *, spans, status="AI_AUDIT_COMPLETE")`: `_transition_to` (`update_status` through EXTRACTED / AI_AUDIT_COMPLETE / HUMAN_AUDIT_COMPLETE), raw `INSERT INTO extractions (paper_id, extraction_schema_hash, extracted_data, model, extracted_at)`, raw `INSERT INTO evidence_spans ... audit_status, auditor_model, audit_rationale, audited_at`. `extractions.low_yield` is not seeded, so it defaults.
- `def _complete_prereq_stages(db)`: `complete_stage` on 9 stages, including `"EXTRACTION_COMPLETE"` and `"AI_AUDIT_COMPLETE_STAGE"`, which writes `workflow_state`
- `from engine.core.database import ReviewDatabase, _STATUS_ORDER`

| test id | legacy construct it depends on | class | code it pins (module::function) |
|---|---|---|---|
| tests/test_audit_adjudication.py::test_collect_flagged_paper | `_add_paper_with_extraction` | c (reader 11) | engine/adjudication/audit_adjudicator.py::_collect_papers_for_review |
| tests/test_audit_adjudication.py::test_collect_contested_paper | `_add_paper_with_extraction` | c (reader 11) | audit_adjudicator.py::_collect_papers_for_review |
| tests/test_audit_adjudication.py::test_collect_spot_check | `_add_paper_with_extraction` ×10 | c (reader 11) | audit_adjudicator.py::_collect_papers_for_review (spot-check sampling) |
| tests/test_audit_adjudication.py::test_collect_minimum_one_spot_check | `_add_paper_with_extraction` ×5 | c (reader 11) | audit_adjudicator.py::_collect_papers_for_review |
| tests/test_audit_adjudication.py::test_flatten_exports_problem_spans_only | `_add_paper_with_extraction` | c (reader 11) | audit_adjudicator.py::_flatten_to_span_rows (fed by _collect_papers_for_review) |
| tests/test_audit_adjudication.py::test_flatten_spot_check_exports_all_spans | `_add_paper_with_extraction` | c (reader 11) | audit_adjudicator.py::_flatten_to_span_rows |
| tests/test_audit_adjudication.py::test_export_creates_xlsx | `_add_paper_with_extraction` + `_complete_prereq_stages` (workflow_state) | c (reader 11) | audit_adjudicator.py::export_audit_review_queue → _write_audit_xlsx |
| tests/test_audit_adjudication.py::test_export_sets_workflow_stage | `_add_paper_with_extraction` + `_complete_prereq_stages` (workflow_state) | c (reader 11) | audit_adjudicator.py::export_audit_review_queue (complete_stage "AUDIT_QUEUE_EXPORTED") |
| tests/test_audit_adjudication.py::test_min_status_filtering | `_add_paper_with_extraction(status="AI_AUDIT_COMPLETE")` and `(status="HUMAN_AUDIT_COMPLETE")` | d | database.py::_STATUS_ORDER (the min_status_gate ordering). The test re-implements a status filter with raw SQL and calls no engine function (see Findings). |
| tests/test_audit_adjudication.py::test_check_audit_review_gate | `_add_paper_with_extraction` ×2 | c (reader 11) | audit_adjudicator.py::check_audit_review_gate |
| tests/test_audit_adjudication.py::test_check_audit_review_gate_zero_when_clean | `_add_paper_with_extraction` | c (reader 11) | audit_adjudicator.py::check_audit_review_gate |
| tests/test_audit_adjudication.py::test_missing_span_not_counted_as_success | `_add_paper_with_extraction` (contested span), then asserts `SELECT audit_status FROM evidence_spans` unchanged | b (dead-weight fixture) | audit_adjudicator.py::import_audit_review_decisions. Its first statement is `raise AuditAdjudicationDeprecated(DEPRECATION_MESSAGE)`, so it never reads the extractions/evidence_spans rows the fixture builds. The span-unchanged assertion holds by construction. |

Not dependent: 3 ids (`test_the_audit_import_path_refuses_and_names_its_successor`, `test_the_audit_import_path_refuses_before_it_reads_anything`, `test_audit_adjudication_is_gone_and_does_not_come_back`)

---

#### tests/test_workflow.py

Collected: **30**

Legacy constructs present:
- `complete_stage(db._conn, ...)`, `bypass_stage(`, `reset_stage(`, `advance_stage(`, `ensure_workflow_table(`, all writing `workflow_state`
- `for stage in WORKFLOW_STAGES: complete_stage(db._conn, stage)`, which completes all 12, including the extraction stages `EXTRACTION_COMPLETE`, `AI_AUDIT_COMPLETE_STAGE`, `AUDIT_QUEUE_EXPORTED` and `AUDIT_REVIEW_COMPLETE`
- the three integration tests set papers.status to `"ABSTRACT_SCREEN_FLAGGED"` only (a screening token, so not legacy) and drive `screening_adjudicator`, which calls `complete_stage`
- No test in this file touches `extractions`, `evidence_spans` or an extraction-stage papers.status.

**Gates.** `engine/adjudication/workflow.py` reads **only** `workflow_state`. `can_advance_to`, `get_current_blocker`, `is_adjudication_complete`, `advance_stage` and `format_workflow_status` all go through `get_workflow_status`, and there is no `extractions` / `evidence_spans` / `papers` SQL in the module. The extraction-stage stages are **set** by callers outside workflow.py: `scripts/run_pipeline.py` sets `EXTRACTION_COMPLETE` / `AI_AUDIT_COMPLETE_STAGE` from the event store ("Reader 8 on the reader (9b-FLIP 1/2)"), and audit_adjudicator sets `AUDIT_QUEUE_EXPORTED` / `AUDIT_REVIEW_COMPLETE`.

| test id | legacy construct it depends on | class | code it pins (module::function) — stages exercised |
|---|---|---|---|
| tests/test_workflow.py::test_ensure_idempotent | `ensure_workflow_table` ×2 (INSERT OR IGNORE) | a† | workflow.py::ensure_workflow_table — all 12 (count) |
| tests/test_workflow.py::test_complete_stage | `complete_stage` | a† | workflow.py::complete_stage / is_stage_done — ABSTRACT_SCREENING_COMPLETE |
| tests/test_workflow.py::test_bypass_stage | `bypass_stage` | a† | workflow.py::bypass_stage — ABSTRACT_DIAGNOSTIC_COMPLETE |
| tests/test_workflow.py::test_reset_stage | `complete_stage` + `reset_stage` | a† | workflow.py::reset_stage — ABSTRACT_SCREENING_COMPLETE |
| tests/test_workflow.py::test_can_advance_after_prereqs | `complete_stage` | a† | workflow.py::can_advance_to — ABSTRACT_SCREENING → ABSTRACT_DIAGNOSTIC |
| tests/test_workflow.py::test_cannot_skip_stages | `complete_stage` | a† | workflow.py::can_advance_to — ABSTRACT_CATEGORIES_CONFIGURED blocked |
| tests/test_workflow.py::test_get_current_blocker_after_first | `complete_stage` | a† | workflow.py::get_current_blocker — ABSTRACT_DIAGNOSTIC_COMPLETE |
| tests/test_workflow.py::test_get_current_blocker_none_when_all_complete | `complete_stage` ×12 | a† | workflow.py::get_current_blocker — all 12 incl. extraction stages 9–12 |
| tests/test_workflow.py::test_adjudication_complete_after_all_stages | `complete_stage` ×12 | a† | workflow.py::is_adjudication_complete — all 12 |
| tests/test_workflow.py::test_advance_stage_success | `advance_stage` (writes complete) | a† | workflow.py::advance_stage — ABSTRACT_SCREENING_COMPLETE |
| tests/test_workflow.py::test_advance_stage_force | `advance_stage(force=True)` (writes bypassed) | a† | workflow.py::advance_stage — ABSTRACT_DIAGNOSTIC_COMPLETE |
| tests/test_workflow.py::test_advance_stage_already_complete | `complete_stage` + `advance_stage` | a† | workflow.py::advance_stage — ABSTRACT_SCREENING_COMPLETE |
| tests/test_workflow.py::test_format_workflow_status_all_pending | subject only: seeded rows, no write | c (reader 14) | workflow.py::format_workflow_status — stages 1, 5 |
| tests/test_workflow.py::test_format_workflow_status_partial | `complete_stage` | c (reader 14) | workflow.py::format_workflow_status — stages 1–2 |
| tests/test_workflow.py::test_format_workflow_status_bypassed | `bypass_stage` | c (reader 14) | workflow.py::format_workflow_status — stage 1 |
| tests/test_workflow.py::test_format_workflow_status_all_complete | `complete_stage` ×12 | c (reader 14) | workflow.py::format_workflow_status — all 12 |
| tests/test_workflow.py::test_get_workflow_status_tracks_metadata | `complete_stage` | a† | workflow.py::get_workflow_status — stage 1 |
| tests/test_workflow.py::test_export_sets_queue_exported | code under test writes `workflow_state` (papers.status ABSTRACT_SCREEN_FLAGGED only) | a† | engine/adjudication/screening_adjudicator.py::export_adjudication_queue — ABSTRACT_QUEUE_EXPORTED |
| tests/test_workflow.py::test_import_sets_adjudication_complete | same | a† | screening_adjudicator.py::import_adjudication_decisions — ABSTRACT_ADJUDICATION_COMPLETE |
| tests/test_workflow.py::test_import_does_not_set_complete_with_missing | same (export writes ABSTRACT_QUEUE_EXPORTED) | a† | screening_adjudicator.py::import_adjudication_decisions — ABSTRACT_ADJUDICATION_COMPLETE stays pending |

Not dependent: 10 ids. These read only the seeded pending rows: `test_workflow_table_created`, `test_workflow_table_seeded`, `test_complete_unknown_stage_raises` (raises before any write), `test_is_stage_done_false_initially`, `test_can_advance_first_stage`, `test_cannot_advance_without_prereqs`, `test_get_current_blocker_initial`, `test_adjudication_complete_false_initially`, `test_advance_stage_blocked` (blocked, no write), `test_get_workflow_status_returns_all_stages`. All 30 tests in the file pin workflow.py, whose only store is `workflow_state`.

---

#### Totals

| file | collected | listed | a | b | c | d | not dependent |
|---|---|---|---|---|---|---|---|
| tests/test_ollama_client.py | 27 | 1 | 0 | 0 | 0 | 1 | 26 |
| tests/test_database.py | 43 | 24 | 0 | 0 | 0 | 24 (17 construct + 7 subject only) | 19 |
| tests/test_human_review.py | 7 | 7 | 0 | 0 | 7 | 0 | 0 |
| tests/test_audit_adjudication.py | 15 | 12 | 0 | 1 | 10 | 1 | 3 |
| tests/test_workflow.py | 30 | 20 | 16 (all a†) | 0 | 4 (1 subject only) | 0 | 10 |
| **group B** | **122** | **64** | **16** (all a†) | **1** | **21** | **26** | **58** |

- Tests that build extraction-table or extraction-status legacy rows (excluding workflow_state-only and subject-only rows): 1 + 17 + 7 + 12 = **37**.
- There is **no true class-(a) test** in this group, meaning no surviving non-reader engine code pinned through an extractions/evidence_spans fixture. All 16 class-a tests are a† (workflow_state).

#### Findings

1. **Most of the class-(d) ReviewDatabase methods have no production caller at HEAD.** A grep of engine/, scripts/ and analysis/ finds `add_extraction_atomic`, `add_extraction`, `add_evidence_span`, `update_audit`, `get_stale_extractions`, `reset_for_reaudit`, `cleanup_orphaned_spans`, `admin_reset_status` and `min_status_gate` referenced only by their own `def` in engine/core/database.py (plus comments/docstrings). 20 of the 24 test_database.py rows, and the one test_ollama_client.py row, pin code that nothing runs. The remaining 4 pin `update_status` (still live for screening and acquisition tokens), `reject_paper` or `get_pipeline_stats`, which still have callers. `reject_paper` is reached only from human_review.py::_apply_audit_decisions and audit_adjudicator.py::_import_legacy_format, and the latter sits behind the refusing importer. `get_pipeline_stats` is called by scripts/run_pipeline.py.
2. **`TestDigestInExtraction` pins a column path the run no longer writes.** `extract_paper` / `extract_paper_with_completeness` (engine/agents/extractor.py) and engine/elicitation/pipeline.py still accept and thread `auditor_model_digest=`, but after 6e09166 nothing consumes it: engine/core/extraction_events.py takes only `model_digest`. It looks like a dead parameter left over from the cut-over (a candidate for the slice-3 item-3 sweep). I did not trace every call chain end to end.
3. **`test_missing_span_not_counted_as_success` is class (b).** Its fixture builds extractions and evidence_spans rows that `import_audit_review_decisions` can never read, because the function body starts with `raise AuditAdjudicationDeprecated(...)`. The "original span unchanged" assertion holds by construction. `test_the_audit_import_path_refuses_before_it_reads_anything` already pins the same property without the fixture.
4. **`test_min_status_filtering` calls no engine function.** It re-implements an "exporter" min_status filter in raw SQL over `database._STATUS_ORDER`. At HEAD nothing outside database.py uses `_STATUS_ORDER`; engine/core/effective.py describes "`_STATUS_ORDER` in the exporter" as a former rule. So the test pins a constant used only by `min_status_gate`, which itself has no caller (Finding 1). It is arguably class (b): the exporter behaviour it modelled is gone. I classed it (d) because the constant is still present.
5. **Reader 11 functions under test have no caller outside their module.** `check_audit_review_gate` is referenced only by its own def. `_collect_papers_for_review` is reached only through `export_audit_review_queue`, which engine/adjudication/__init__.py re-exports and workflow.py names only inside instruction text. Neither module has a CLI. The same holds for reader 12: `export_review_queue` and `bulk_accept` in engine/review/human_review.py have no external caller, and `import_review_decisions` is called only from audit_adjudicator's refusing importer. All 17 class-(c) tests in these two files therefore pin reader code with no live entry point.
6. **The workflow.py module docstring is stale.** It still reads "9. EXTRACTION_COMPLETE — auto: all included papers reach EXTRACTED status" and "10. AI_AUDIT_COMPLETE_STAGE — auto: audit run finishes (all papers audited)". Since 4756bd2 (R112), scripts/run_pipeline.py sets both from the event store. The docstring describes a legacy-status gate that no longer exists.
7. **Schema pins that are not in the table, because they build no rows:**
   - `test_database.py::test_tables_exist` asserts that `extractions`, `evidence_spans` **and `review_runs`** exist. `review_runs` is no longer written (R73, per the scripts/run_pipeline.py comment "`review_runs` is no longer written"). Any slice-3 drop of these tables turns this test red.
   - `test_null_confidence_raises_integrity_error` / `test_null_tier_raises_integrity_error` pin the evidence_spans DDL. They are listed above because they call `add_extraction`.
8. **`test_pipeline_stats` covers both reader 14 and a (d) method.** Its only construct is screening tokens, but it asserts `total_extractions == 0` / `total_evidence_spans == 0` from `get_pipeline_stats`, so it also pins the legacy counts.
9. **test_audit_adjudication.py's `_add_paper_with_extraction` never sets `extractions.low_yield`.** `_collect_papers_for_review` reads `low_yield` to route papers, so the LOW_YIELD branch (`review_reason == "low_yield"`) is untested here. That matters only if reader 11 is moved rather than retired.

### Q1 detail — group C

Read-only. Collected counts are taken from `collected_ids_sorted.txt` (standard gate). Every id in the tables below was checked with `grep -F` against that file. Anchors are quoted content, not line numbers.

Class key: **a** = surviving code pinned through a legacy fixture, so it needs an event fixture. **b** = retired subject, or a legacy fixture that is now dead weight. **c** = a slice-3 legacy reader. **d** = a ReviewDatabase legacy method is itself the subject. A dagger (†) marks a classification that carries a caveat, explained under Findings.

---

#### tests/test_ft_screening.py

- Collected: **67**
- Legacy constructs present:
  - `update_status` with extraction tokens in test bodies: `tmp_db.update_status(pid, "EXTRACTED")`, in `test_ft_eligible_to_extracted` and `test_parsed_can_skip_ft_to_extracted`.
  - A class helper, `_advance_to_ai_audit`: `db.update_status(paper_id, "EXTRACTED")` then `db.update_status(paper_id, "AI_AUDIT_COMPLETE")`.
  - A `workflow_state` mutation in a test body: `complete_stage(tmp_db._conn, "FULL_TEXT_SCREENING_COMPLETE", metadata="test")`. `complete_stage` runs `UPDATE workflow_state SET status = 'complete'`.
  - Screening and acquisition tokens only (NOT legacy under this census). `_advance_to_parsed` sets `"ABSTRACT_SCREENED_IN"`, `"PDF_ACQUIRED"` and `"PARSED"`. `_advance_to_ft_flagged` sets `"FT_ELIGIBLE"` then `"FT_FLAGGED"`. There is also a raw `UPDATE papers SET status = 'INGESTED'`.

| test id | legacy construct it depends on | class | code it pins (module::function) |
|---|---|---|---|
| tests/test_ft_screening.py::TestFTTransitions::test_ft_eligible_to_extracted | `update_status(pid, "EXTRACTED")` from FT_ELIGIBLE, then asserts `status == "EXTRACTED"` | d | engine/core/database.py::ReviewDatabase.update_status (FT_ELIGIBLE→EXTRACTED transition) |
| tests/test_ft_screening.py::TestFTTransitions::test_parsed_can_skip_ft_to_extracted | `update_status(pid, "EXTRACTED")` from PARSED | d | engine/core/database.py::ReviewDatabase.update_status (PARSED→EXTRACTED transition) |
| tests/test_ft_screening.py::TestFTScreeningSkipsAdvancedStatus::test_ft_screen_ai_audit_complete_records_decision | `_advance_to_ai_audit` (`update_status` → EXTRACTED → AI_AUDIT_COMPLETE) | c | engine/agents/ft_screener.py::run_ft_screening. Pins `db.get_papers_by_status("AI_AUDIT_COMPLETE")` plus the `_PAST_FT` skip (reader 10). It also exercises update_status incidentally. |
| tests/test_ft_screening.py::TestWorkflowFTStages::test_complete_ft_screening_stage | `complete_stage(...)` ×7 writes `workflow_state` | a† | engine/adjudication/workflow.py::complete_stage / is_stage_done |

Not dependent: **63** ids.

Screening-only constructs (not legacy, recorded as asked). **19** of the 63 set `papers.status` only to screening or acquisition tokens:
- The TestFTTransitions tests `test_parsed_to_ft_eligible`, `test_parsed_to_ft_screened_out`, `test_parsed_to_ft_flagged`, `test_ft_flagged_to_ft_eligible`, `test_ft_flagged_to_ft_screened_out` and `test_ft_screened_out_is_terminal` (6).
- The TestFTAdjudication tests that go through `_advance_to_ft_flagged`: `test_collect_ft_flagged`, `test_export_ft_queue`, `test_export_creates_xlsx_sheets`, `test_import_ft_decisions`, `test_import_rejects_blank_decisions`, `test_import_rejects_invalid_decision`, `test_import_advances_workflow`, `test_ft_adjudication_gate`, `test_ft_adjudication_records_in_table` and `test_status_update_failure_tracked` (10).
- `TestFTAdjudicationTextFailure::test_read_failure_logs_warning_and_sets_marker`.
- `TestMissingParsedText::test_no_parsed_text_marks_ft_flagged` and `TestFTParseError::test_malformed_output_flags_paper`. Both drive `run_ft_screening`, and the `get_papers_by_status("AI_AUDIT_COMPLETE")` line executes there, but it returns nothing: their papers are PARSED only, so they exercise screening selection, not reader 10.

---

#### tests/test_request_capture.py

- Collected: **12**
- Legacy constructs present: **none**.
  - `test_extraction_pass1_pass2_and_snippet_retry` builds a `ReviewDatabase` with `db.get_papers_by_status("INGESTED")` (a screening token), then calls `extract_paper(... run_id=open_extraction_run(db, spec) ...)`. At HEAD the extractor writes events (`write_extraction_events`), and no `add_extraction_atomic` call remains anywhere in engine/, scripts/ or analysis/.
  - `test_audit` calls `auditor.audit_span` on an in-memory dict and touches no database.
  - The cloud tests use `_cloud_db` (a fresh ReviewDatabase) and write no legacy rows.

(no dependent tests)

Not dependent: **12** ids.

---

#### tests/test_run_manifest.py

- Collected: **23**
- Legacy constructs present: a raw INSERT with an extraction token, `"INSERT INTO papers (id, title, source, status, created_at, updated_at) " "VALUES (1, 't', 's', 'EXTRACTED', 'x', 'x')"`. Only `claim_identity` is imported from `_event_store_fixture` (a payload dict, not legacy).

| test id | legacy construct it depends on | class | code it pins (module::function) |
|---|---|---|---|
| tests/test_run_manifest.py::test_run_id_is_on_every_event_row_a_run_writes | `INSERT INTO papers ... 'EXTRACTED'` | b (dead weight) | engine/core/events.py::write_paper_event / write_field_event. The papers row is needed only for the FK. `events.py` contains no `status` read at all, so the `'EXTRACTED'` token is inert and any NOT NULL value would do. |

Not dependent: **22** ids.

---

#### tests/test_cloud_extraction.py

- Collected: **36** (the module has a `skipif(not BACKUP_DB.exists() ...)`, so the ids are collected either way)
- Legacy constructs present. All of them sit in the module fixture `test_db`, which every test in the file requests:
  - It copies a whole legacy-schema database: `shutil.copy2(BACKUP_DB, db_copy)`, where `BACKUP_DB = ... "data" / "surgical_autonomy" / "review_backup_v1_schema.db"`. That file carries legacy `papers.status`, `extractions` and `evidence_spans` rows. (Not opened for this census.)
  - It mutates papers.status to an extraction token: `conn.execute("UPDATE papers SET status = 'AI_AUDIT_COMPLETE' WHERE status = 'AUDITED'")`.
  - It reads `extractions` to choose the paper that gets parsed text: `"SELECT paper_id FROM extractions ORDER BY paper_id LIMIT 1"`.
  - `_seed_corpus_events` turns legacy statuses into eligibility: `corpus = ("FT_ELIGIBLE", "EXTRACTED", "AI_AUDIT_COMPLETE", "HUMAN_AUDIT_COMPLETE")`, then `seed_pre_manifest_paper_event(conn, pid)`. The corpus the cloud code sees is therefore derived from legacy `papers.status`.
- The code under test reads the eligibility axis, not legacy rows. `CloudExtractorBase.get_pending_papers` and `.get_progress` both use `corpus_id_sql(self._conn, ...)`. The legacy rows are load-bearing only through the status→event translation. That makes the dependent tests class a (surviving code, needs a native event fixture) or b (dead weight, where the code under test never reaches paper or corpus rows).

| test id | legacy construct it depends on | class | code it pins (module::function) |
|---|---|---|---|
| tests/test_cloud_extraction.py::TestCloudExtractorBase::test_get_pending_papers | `test_db` corpus (legacy statuses → eligible events); `assert len(pending) > 0` | a | engine/cloud/base.py::CloudExtractorBase.get_pending_papers |
| tests/test_cloud_extraction.py::TestCloudExtractorBase::test_get_pending_excludes_completed | `test_db` corpus | a | engine/cloud/base.py::CloudExtractorBase.get_pending_papers / store_result |
| tests/test_cloud_extraction.py::TestCloudExtractorBase::test_store_result_rejects_empty_spans | `test_db` corpus (pid from pending; `pytest.skip("No pending papers")` if empty) | a | engine/cloud/base.py::CloudExtractorBase.store_result |
| tests/test_cloud_extraction.py::TestCloudExtractorBase::test_store_result_atomic | `test_db` corpus (pid from pending; skip if empty) | a | engine/cloud/base.py::CloudExtractorBase.store_result |
| tests/test_cloud_extraction.py::TestCloudExtractorBase::test_get_progress | `test_db` corpus (`total_papers`) | a | engine/cloud/base.py::CloudExtractorBase.get_progress |
| tests/test_cloud_extraction.py::TestOpenAIExtractor::test_extract_paper | `test_db` corpus (pid from pending; skip if empty) | a | engine/cloud/openai_extractor.py::OpenAIExtractor.extract_paper |
| tests/test_cloud_extraction.py::TestOpenAIExtractor::test_cost_calculation | `test_db` corpus (skip if empty) | a | engine/cloud/openai_extractor.py::OpenAIExtractor.extract_paper (cost) |
| tests/test_cloud_extraction.py::TestAnthropicExtractor::test_extract_paper | `test_db` corpus (skip if empty) | a | engine/cloud/anthropic_extractor.py::AnthropicExtractor.extract_paper |
| tests/test_cloud_extraction.py::TestAnthropicExtractor::test_reasoning_trace_from_thinking_blocks | `test_db` corpus (skip if empty) | a | engine/cloud/anthropic_extractor.py::AnthropicExtractor.extract_paper |
| tests/test_cloud_extraction.py::TestAnthropicExtractor::test_json_extracted_from_text_blocks | `test_db` corpus (skip if empty) | a | engine/cloud/anthropic_extractor.py::AnthropicExtractor.extract_paper |
| tests/test_cloud_extraction.py::TestAnthropicExtractor::test_cost_calculation | `test_db` corpus (skip if empty) | a | engine/cloud/anthropic_extractor.py::AnthropicExtractor.extract_paper (cost) |
| tests/test_cloud_extraction.py::TestCLI::test_progress_no_api_calls | `test_db` corpus | a | scripts/run_cloud_extraction.py::show_progress → CloudExtractorBase.get_progress |
| tests/test_cloud_extraction.py::TestCLI::test_dry_run_no_api_calls | `test_db` corpus | a | scripts/run_cloud_extraction.py::dry_run → CloudExtractorBase.get_pending_papers |
| tests/test_cloud_extraction.py::TestSonnetEmptyStringNormalization::test_empty_string_normalized_to_null | `test_db` corpus (skip if empty) | a | engine/cloud/anthropic_extractor.py::AnthropicExtractor.extract_paper + base.store_result |
| tests/test_cloud_extraction.py::TestSonnetEmptyStringNormalization::test_null_converted_to_nr | `test_db` corpus (skip if empty) | a | engine/cloud/anthropic_extractor.py::AnthropicExtractor.extract_paper |
| tests/test_cloud_extraction.py::TestSonnetEmptyStringNormalization::test_nonempty_string_unchanged | `test_db` corpus (skip if empty) | a | engine/cloud/anthropic_extractor.py::AnthropicExtractor.extract_paper |
| tests/test_cloud_extraction.py::TestSonnetRateLimitBackoff::test_429_triggers_30s_plus_backoff | `test_db` corpus (pid from pending, `run(max_papers=1)`) | a | engine/cloud/anthropic_extractor.py::AnthropicExtractor.run |
| tests/test_cloud_extraction.py::TestSonnetRateLimitBackoff::test_429_uses_retry_after_header | `test_db` corpus | a | engine/cloud/anthropic_extractor.py::AnthropicExtractor.run |
| tests/test_cloud_extraction.py::TestOpenAIRateLimitBackoff::test_429_triggers_30s_plus_backoff | `test_db` corpus | a | engine/cloud/openai_extractor.py::OpenAIExtractor.run |
| tests/test_cloud_extraction.py::TestOpenAIRateLimitBackoff::test_429_uses_retry_after_header | `test_db` corpus | a | engine/cloud/openai_extractor.py::OpenAIExtractor.run |
| tests/test_cloud_extraction.py::TestStoreResultCrashProtection::test_openai_store_failure_continues_run | `test_db` corpus (first 2 pending papers) | a | engine/cloud/openai_extractor.py::OpenAIExtractor.run |
| tests/test_cloud_extraction.py::TestStoreResultCrashProtection::test_distribution_collapse_not_caught | `test_db` corpus (`run(max_papers=0)` iterates ALL pending: `if max_papers:` treats 0 as "no limit") | a† | engine/cloud/openai_extractor.py::OpenAIExtractor.run |
| tests/test_cloud_extraction.py::TestAuthErrorAbort::test_openai_401_aborts_immediately | `test_db` corpus | a | engine/cloud/openai_extractor.py::OpenAIExtractor.run |
| tests/test_cloud_extraction.py::TestAuthErrorAbort::test_anthropic_401_aborts_immediately | `test_db` corpus | a | engine/cloud/anthropic_extractor.py::AnthropicExtractor.run |
| tests/test_cloud_extraction.py::TestInitCloudTables::test_creates_tables | `test_db` legacy DB copy | b (dead weight) | engine/cloud/schema.py::init_cloud_tables. It reads no paper rows. |
| tests/test_cloud_extraction.py::TestInitCloudTables::test_tables_have_expected_columns | `test_db` legacy DB copy | b (dead weight) | engine/cloud/schema.py::init_cloud_tables |
| tests/test_cloud_extraction.py::TestInitCloudTables::test_idempotent | `test_db` legacy DB copy | b (dead weight) | engine/cloud/schema.py::init_cloud_tables |
| tests/test_cloud_extraction.py::TestCloudExtractorBase::test_parse_response_valid_json | `test_db` legacy DB copy | b (dead weight) | engine/cloud/base.py::CloudExtractorBase.parse_response_to_spans. This is pure parsing. |
| tests/test_cloud_extraction.py::TestCloudExtractorBase::test_parse_response_malformed_json | `test_db` legacy DB copy | b (dead weight) | engine/cloud/base.py::CloudExtractorBase.parse_response_to_spans |
| tests/test_cloud_extraction.py::TestCloudExtractorBase::test_parse_response_bare_list | `test_db` legacy DB copy | b (dead weight) | engine/cloud/base.py::CloudExtractorBase.parse_response_to_spans |
| tests/test_cloud_extraction.py::TestCloudExtractorBase::test_parse_response_data_extraction_key | `test_db` legacy DB copy | b (dead weight) | engine/cloud/base.py::CloudExtractorBase.parse_response_to_spans |
| tests/test_cloud_extraction.py::TestCloudExtractorBase::test_parse_response_recovers_from_raw | `test_db` legacy DB copy | b (dead weight) | engine/cloud/base.py::CloudExtractorBase.parse_response_to_spans |
| tests/test_cloud_extraction.py::TestSpecDrivenConfig::test_openai_reads_from_spec | `test_db` legacy DB copy | b (dead weight) | engine/cloud/openai_extractor.py::OpenAIExtractor.__init__. It only resolves the spec. |
| tests/test_cloud_extraction.py::TestSpecDrivenConfig::test_anthropic_reads_from_spec | `test_db` legacy DB copy | b (dead weight) | engine/cloud/anthropic_extractor.py::AnthropicExtractor.__init__ |
| tests/test_cloud_extraction.py::TestSpecDrivenConfig::test_no_default_price | `test_db` legacy DB copy | b (dead weight) | engine/cloud/openai_extractor.py::OpenAIExtractor.__init__ |
| tests/test_cloud_extraction.py::TestSpecDrivenConfig::test_no_default_arm | `test_db` legacy DB copy | b (dead weight) | engine/cloud/openai_extractor.py::OpenAIExtractor.__init__ |

Not dependent: **0** ids. All 36 request `test_db`.

---

#### tests/test_extraction_validator.py

- Collected: **24**
- Legacy constructs present. All are in the helper `_add_paper_and_extraction`:
  - `db.update_status(pid, "EXTRACTED")`.
  - `"INSERT INTO extractions (paper_id, model, extraction_schema_hash, extracted_at, extracted_data) "`.
  - `"INSERT INTO evidence_spans (extraction_id, field_name, value, "`.
  - The code under test reads and writes the legacy tables directly. `validate_extraction` and `normalize_categorical_values` both query `FROM evidence_spans es JOIN extractions e ON es.extraction_id = e.id`, and `normalize_categorical_values` runs `"UPDATE evidence_spans SET value = ? WHERE id = ?"`. The `EXTRACTED` status is not read by either function (they take `pid`), but the extractions and span rows are load-bearing.

| test id | legacy construct it depends on | class | code it pins (module::function) |
|---|---|---|---|
| tests/test_extraction_validator.py::test_valid_spans_no_issues | `_add_paper_and_extraction` (extractions + evidence_spans rows, status EXTRACTED) | c | engine/validators/extraction_validator.py::validate_extraction (sole caller: validate_all) |
| tests/test_extraction_validator.py::test_unknown_field_name_flagged | `_add_paper_and_extraction` | c | engine/validators/extraction_validator.py::validate_extraction |
| tests/test_extraction_validator.py::test_invalid_categorical_value_flagged | `_add_paper_and_extraction` | c | engine/validators/extraction_validator.py::validate_extraction |
| tests/test_extraction_validator.py::test_numeric_field_non_numeric_flagged | `_add_paper_and_extraction` | c | engine/validators/extraction_validator.py::validate_extraction |
| tests/test_extraction_validator.py::test_not_found_value_accepted | `_add_paper_and_extraction` | c | engine/validators/extraction_validator.py::validate_extraction |
| tests/test_extraction_validator.py::test_semicolon_all_valid | `_add_paper_and_extraction` | c | engine/validators/extraction_validator.py::validate_extraction |
| tests/test_extraction_validator.py::test_semicolon_one_invalid | `_add_paper_and_extraction` | c | engine/validators/extraction_validator.py::validate_extraction |
| tests/test_extraction_validator.py::test_single_value_no_semicolons | `_add_paper_and_extraction` | c | engine/validators/extraction_validator.py::validate_extraction |
| tests/test_extraction_validator.py::test_semicolon_all_invalid | `_add_paper_and_extraction` | c | engine/validators/extraction_validator.py::validate_extraction |
| tests/test_extraction_validator.py::test_normalize_categorical_db_unambiguous | `_add_paper_and_extraction`; asserts the UPDATE by re-reading `evidence_spans` | c† | engine/validators/extraction_validator.py::normalize_categorical_values (no production caller, see Findings) |
| tests/test_extraction_validator.py::test_normalize_categorical_db_exact | `_add_paper_and_extraction` | c† | engine/validators/extraction_validator.py::normalize_categorical_values |
| tests/test_extraction_validator.py::test_normalize_categorical_db_ambiguous | `_add_paper_and_extraction`; re-reads `evidence_spans` | c† | engine/validators/extraction_validator.py::normalize_categorical_values |
| tests/test_extraction_validator.py::test_normalize_categorical_db_no_match | `_add_paper_and_extraction` | c† | engine/validators/extraction_validator.py::normalize_categorical_values |

Not dependent: **11** ids: `test_closest_match_similarity`, the four `test_normalize_prefix_*`, the four `test_bleed_*` (codebook plus a dict), `test_same_spec_same_hash` and `test_modified_codebook_different_hash`.

---

#### tests/test_citation_guard.py

- Collected: **12**
- Legacy constructs present: a fake writer, `class _FakeDB:` with `def add_extraction_atomic(self, **kw): self.writes += 1`.
- Is P8's claim still true? **Yes for this file, but the coupling is vacuous.** The fake is built but never handed to any engine code. The test calls `enforce_citations(...)` directly and then asserts `db.writes == 0`, which holds trivially. No engine code path calls `add_extraction_atomic` at HEAD: its only occurrence under engine/, scripts/ and analysis/ is the definition, `engine/core/database.py: def add_extraction_atomic(`.

| test id | legacy construct it depends on | class | code it pins (module::function) |
|---|---|---|---|
| tests/test_citation_guard.py::test_uncited_value_never_reaches_the_database | fake `_FakeDB.add_extraction_atomic` | b (dead weight) | engine/core/citation_guard.py::enforce_citations. The "never reaches the database" half pins nothing. |

Not dependent: **11** ids (the pure predicate tests).

---

#### tests/test_elicitation_pipeline.py

- Collected: **15**
- Legacy constructs present: **none**.
- Is P8's claim still true? **No, it is stale.** Commit 6e09166 removed `def add_extraction_atomic(self, **kw): self.stored = kw` from `_DB` and replaced it with an autouse capture of the event writer: `monkeypatch.setattr(PL, "write_extraction_events", lambda conn, record, **kw: setattr(conn, "stored", record))`. `_DB` is now "A review with no database" with `self._conn = self`. `test_retry_is_bounded_and_the_paper_is_failed` asserts `db.stored is None` against that event-writer capture.

(no dependent tests)

Not dependent: **15** ids.

---

#### Shared helpers (not test modules)

#### tests/_corpus_fixture.py: **builds legacy rows (yes)**

- **Consumer:** only `tests/test_corpus_authority.py`. None of the group-C files import it.
- `FIXTURE_STATUSES`, a literal tuple of all 15 statuses, including `"EXTRACT_FAILED"`, `"EXTRACTED"`, `"AI_AUDIT_COMPLETE"`, `"HUMAN_AUDIT_COMPLETE"` and `"REJECTED"`. `STATUS_PROBE_IDS` maps one paper to each status.
- `FIXTURE_CARRIED` / `FIXTURE_CARRIED_NON_CORPUS`: the ten carried ids. Seven get `"AI_AUDIT_COMPLETE"` and three get `"FT_SCREENED_OUT"`.
- `_POOL_STATUS_CYCLE = ("AI_AUDIT_COMPLETE", "EXTRACTED", "FT_ELIGIBLE", "HUMAN_AUDIT_COMPLETE", "FT_SCREENED_OUT", "EXTRACT_FAILED")`, the status cycle for the 60-paper pool.
- `_SCHEMA` holds a private minimal DDL. It creates `papers`, `extractions (id INTEGER PRIMARY KEY, paper_id INTEGER NOT NULL)`, `evidence_spans (... field_name TEXT NOT NULL, value TEXT)` and `cloud_extractions`.
- `_add_paper(conn, pid, status)` does a raw `INSERT INTO papers (id, title, status, ...)`, so it **writes extraction-stage status tokens** directly.
- `build_fixture(root)` builds the whole review directory:
  - papers per status, carried and pool (legacy statuses);
  - parsed_text files;
  - **legacy rows**: `INSERT INTO extractions (id, paper_id)` plus `INSERT INTO evidence_spans (extraction_id, field_name, value) VALUES (?, 'study_type', ?)` for every other carried or pool paper;
  - two `cloud_extractions` rows;
  - finally a call to `_seed_event_store`.
- `_ELIGIBLE_FROM_STATUS` holds the 4 corpus statuses plus `"EXTRACT_FAILED"`.
- `_seed_event_store(db_path)` runs 016 `create_schema` and 019 `run_migration`. For each papers row whose status is in `_ELIGIBLE_FROM_STATUS`, it inserts a raw `'state_at_migration'` → `'eligible'` paper_event. EXTRACT_FAILED papers also get an `'extraction_failed'` processing event. So the event side is *derived from* the legacy statuses.

#### tests/_event_store_fixture.py: **event-store builder. Its only legacy constructs are a status default and an empty `evidence_spans` stub, and it has one legacy-row reader.**

- `_PAPERS_DDL` is used when `papers` is absent: `status TEXT NOT NULL DEFAULT 'EXTRACTED'`. Any papers row this helper inserts on a database whose `papers` table it created **therefore carries the extraction token `EXTRACTED` implicitly**. `add_values` does exactly that with `"INSERT OR IGNORE INTO papers (id) VALUES (?)"`. This is a legacy construct by default value, and nothing in the event path reads it.
- `ensure_event_store(db_path)` creates `papers` if absent. If `evidence_spans` is absent it creates an **empty stub** (`CREATE TABLE evidence_spans (id INTEGER PRIMARY KEY)`), with no rows. It then runs 016 `create_schema`, 019, 020 and 021. It is memoised per path in `_DONE`.
- `fixture_run(conn, arm=None, *, arm_kind="model")` inserts or reuses the `run_manifests` row `FIXTURE_RUN_UID = "fixture-run"`. With an `arm`, it registers the arm (`events.register_arm`) and inserts a `run_stage_configs` row that pins it. Event-only.
- `seed_pre_manifest_paper_event(conn, paper_id, ...)` inserts a raw `'state_at_migration'` paper_event with `run_marker 'pre-manifest'`, standing in for 017's seed. Event-only.
- `add_values(db_path, arm, field_name, values, ...)` ensures the store, then for each paper: `INSERT OR IGNORE INTO papers (id)` (see the default above), a pre-manifest eligibility seed, then `events.write_field_event` `asserted` (+ `citation_located`). Event rows only, except for the implicit status default.
- `mirror_legacy_into_events(db, *, arm="local")` **reads** legacy rows and **builds none**. It reads the papers ids whose `status IN (FT_ELIGIBLE, EXTRACTED, AI_AUDIT_COMPLETE, HUMAN_AUDIT_COMPLETE, EXTRACT_FAILED)` and the latest extraction's `evidence_spans` (`JOIN extractions e ON e.id = es.extraction_id`), and translates them into events. Its docstring says the calling fixtures build their world with `ReviewDatabase.add_extraction` / `add_evidence_span` / `update_status`, so every consumer of this function has a legacy fixture by construction. Its only consumer is `tests/test_exporters.py`, which is outside group C.
- `upgrade_event_store(db_path)` adds the same empty `evidence_spans` stub if absent, then runs 016, 019 and 020. It does not run 021.
- `seed_claim(conn, ...)` inserts a raw pre-manifest `field_events` claim. Event-only.
- `run_for(conn)` returns the fixture run and pins every claimable model arm. Event-only.
- `seed_eligibility(conn, paper_id, *, to_state="eligible")` calls `events.write_paper_event` `screened` under the fixture run. Event-only.
- `FIXTURE_DIGEST = "a"*64` and `open_extraction_run(db, spec, *, digest=...)` open a real manifest via `run_manifest.open_run` with the extraction stages plus `audit`. Event and manifest only.
- `FIXTURE_TEXT_SHA = "f"*64` and `claim_identity(arm, paper_id, ...)` return the three input-identity payload keys (reuse key, parsed-text sha, parsed-text uid). This is a dict with no DB access.
- **Group-C consumers:**
  - test_request_capture uses `open_extraction_run`.
  - test_run_manifest uses `claim_identity`.
  - test_cloud_extraction uses `ensure_event_store` and `seed_pre_manifest_paper_event`, via `_seed_corpus_events`, driven by legacy statuses. Its papers table comes from the backup DB, so the `'EXTRACTED'` default does not apply there.

---

#### Totals

| file | collected | dependent | a | b | c | d |
|---|---|---|---|---|---|---|
| tests/test_ft_screening.py | 67 | 4 | 1† | 0 | 1 | 2 |
| tests/test_request_capture.py | 12 | 0 | 0 | 0 | 0 | 0 |
| tests/test_run_manifest.py | 23 | 1 | 0 | 1 | 0 | 0 |
| tests/test_cloud_extraction.py | 36 | 36 | 24 (1†) | 12 | 0 | 0 |
| tests/test_extraction_validator.py | 24 | 13 | 0 | 0 | 13 (4†) | 0 |
| tests/test_citation_guard.py | 12 | 1 | 0 | 1 | 0 | 0 |
| tests/test_elicitation_pipeline.py | 15 | 0 | 0 | 0 | 0 | 0 |
| **group C** | **189** | **55** | **25** | **14** | **14** | **2** |

All 14 class-b entries are **dead-weight** fixtures. None of the group-C tests pins code retired at 6e09166 or 0fade3d, so no test here "should already be gone".

#### Findings

1. **P8 is half stale.** `test_elicitation_pipeline.py` no longer fakes `add_extraction_atomic`: 6e09166 replaced it with an autouse capture of `write_extraction_events`. `test_citation_guard.py` still has `_FakeDB.add_extraction_atomic`, but the fake is never passed to any engine code. `test_uncited_value_never_reaches_the_database` calls `enforce_citations` directly and asserts `db.writes == 0` trivially, so the "never reaches the database" claim is untested in that file. The module docstring still says the refusal is exercised "then through `extract_paper_with_completeness`", and no test in the file does so. That coverage actually lives in `test_elicitation_pipeline.py::test_retry_is_bounded_and_the_paper_is_failed`.
2. **`ReviewDatabase.add_extraction_atomic` has no caller at HEAD** in engine/, scripts/ or analysis/; only its definition remains. It is a live legacy writer with zero production callers, a candidate for the class-d retirement list.
3. **`extraction_validator.normalize_categorical_values` has never had a production caller.** `git log -S` shows only its introducing commit 3aa7e11, and a grep at HEAD finds no caller outside its own tests. It still performs `UPDATE evidence_spans SET value = ?`, a legacy-store **write**. Its 4 tests are marked c† because the function is not named in the slice-3 reader list. It lives beside `validate_all` but is neither a reader nor reachable. Slice 3 should rule on it: retire it, or move it with validate_all. Relatedly, `validate_extraction` (9 tests) is reachable only via `validate_all` → `main`, so its tests follow reader `validate_all` (c).
4. **test_cloud_extraction's whole corpus is a copy of a real legacy-schema review database** at `data/surgical_autonomy/review_backup_v1_schema.db`, which is gitignored. The module `skipif` means all 36 ids silently skip on any machine without that file. Its corpus is derived from legacy `papers.status` via `_seed_corpus_events`, and 18 of the 24 class-a tests `pytest.skip("No pending papers")` when the corpus is empty. A rewrite to an event fixture that loses eligibility would therefore turn failures into **skips, not reds**. The rewrite should replace `skip` with an assertion that the corpus is non-empty.
5. **`_seed_corpus_events` in test_cloud_extraction omits `EXTRACT_FAILED`**: `corpus = ("FT_ELIGIBLE", "EXTRACTED", "AI_AUDIT_COMPLETE", "HUMAN_AUDIT_COMPLETE")`. `_corpus_fixture._ELIGIBLE_FROM_STATUS` and `_event_store_fixture.mirror_legacy_into_events` both include it, on A9 grounds. That makes three copies of the status→eligibility predicate that disagree, which conflicts with "one predicate, shared, never copied".
6. **`test_distribution_collapse_not_caught` (a†) depends on the corpus only incidentally.** `OpenAIExtractor.run` has `if max_papers:`, so `max_papers=0` means *no limit*, and the test iterates every pending paper in the backup corpus against a `MagicMock` client. The test comment says "Need at least one paper to get past the empty-pending check", but there is no such check: `run_distribution_check` is reached with an empty corpus too. The assertion is insensitive to the fixture. Separately, the `max_papers=0` → unlimited semantics look like a latent defect.
7. **`test_complete_ft_screening_stage` (a†).** `workflow_state` is listed as legacy for this census, but at HEAD it is still the **live** store: `workflow.complete_stage` does `UPDATE workflow_state`, and `ft_screener.run_ft_verification` and `ft_screening_adjudicator` call it in production. No event-store equivalent exists, so "rewrite to an event fixture" has no target yet. It needs a ruling on whether workflow_state is in slice 3 at all. The FT import tests (`test_import_ft_decisions`, `test_import_advances_workflow`, `test_ft_adjudication_records_in_table`, `test_status_update_failure_tracked`) also mutate `workflow_state`, but through the code under test (`complete_stage(review_db._conn, "FULL_TEXT_ADJUDICATION_COMPLETE", ...)`) rather than through a fixture. So they are not listed, although `test_import_advances_workflow` asserts on it (`is_stage_done`).
8. **`test_run_manifest.py::test_run_id_is_on_every_event_row_a_run_writes`** inserts a papers row with `'EXTRACTED'` that nothing reads (events.py has no `status` read). Changing the token to any screening token is a one-token fix.
9. **The `_event_store_fixture._PAPERS_DDL` default `status ... DEFAULT 'EXTRACTED'`** silently stamps an extraction token on every papers row `add_values` creates (`INSERT OR IGNORE INTO papers (id)`). It is inert for event readers, but any test that also exercises a status-reading legacy reader (reader 14, prisma, and so on) on an `add_values` database will see phantom EXTRACTED papers. This is worth checking in the groups that consume `add_values` (test_concordance, test_distribution_monitor, test_non_value_tokens_downstream).
10. **Cloud tables are outside this census but are legacy per migration 016's docstring.** Its list reads "`extractions`, `evidence_spans`, `cloud_extractions`, `cloud_evidence_spans` ... stay exactly as they are, read-only". Yet `CloudExtractorBase.store_result` still INSERTs into `cloud_extractions` / `cloud_evidence_spans` at HEAD, and 8+ cloud tests assert on those rows. Under this census's definition cloud tables are not legacy, so these tests are counted only via `test_db`.
11. **`test_ft_screening.py::TestFTScreeningSkipsAdvancedStatus::test_ft_screen_ai_audit_complete_records_decision` is the only group-C test that pins reader 10's extraction-token path**, both `get_papers_by_status("AI_AUDIT_COMPLETE")` and `_PAST_FT`. `run_ft_verification`'s `_PAST_FT`-free path and the `_PAST_FT_2` branch in the parse-error path have no extraction-token test here: `test_malformed_output_flags_paper` uses PARSED papers only.

## Q2 — Readers 10–14 and the reporting readers at HEAD

**Method.** Located by grep over `engine/ scripts/ analysis/` for `get_papers_by_status`, `SET status`,
the extraction-stage tokens (`EXTRACTED`, `EXTRACT_FAILED`, `AI_AUDIT_COMPLETE`),
`FROM extractions|JOIN extractions|FROM evidence_spans|JOIN evidence_spans`, `low_yield`, and each
reader's function names. Every site below was then **READ**. Tests that drive a site were found by an
AST pass over each collected id: the test function, plus fixtures and helpers named in it one level
deep, that mention the symbol. That is a locator. The ids themselves come from the Q6 list.

"Event-side source" means `effective_value` / `effective_state` / `eligible_paper_ids` /
`live_claim_events` / `iter_grid` (`engine/core/effective.py`), `audit_events.low_yield`, the run
manifest tables, or the audit telemetry file. **P** = makes a progression decision; **D** = display or
export only.

### Reader 10 — `engine/agents/ft_screener.py`

| read (quoted) | function | axis | P/D | event-side source |
|---|---|---|---|---|
| `db.get_papers_by_status("PARSED")` | `run_ft_screening` | screening selection | P | out of slice 3 (I2) |
| `db.get_papers_by_status("AI_AUDIT_COMPLETE")` (same statement: `papers = db.get_papers_by_status("PARSED") + db.get_papers_by_status("AI_AUDIT_COMPLETE")`) | `run_ft_screening` | **extraction token** | P (selects papers to screen) | y: `effective_state(...).processing == "audited_ai"` |
| `_PAST_FT = {"FT_ELIGIBLE", "FT_FLAGGED", "EXTRACTED", "EXTRACT_FAILED", "AI_AUDIT_COMPLETE", "HUMAN_AUDIT_COMPLETE", "REJECTED"}`, tested against `paper.get("status", "")` in three places (`_PAST_FT` in the no-text branch, `_PAST_FT_2` in the parse-error branch, `_PAST_FT` before the status write) | `run_ft_screening` | **mixed**: FT tokens plus extraction tokens, one set | P (suppresses `update_status`) | partial: the extraction half maps to the processing axis; the FT half is screening |
| `db.get_papers_by_status("FT_ELIGIBLE")` | `run_ft_verification` | screening selection | P | out of slice 3 (I2) |

Writes, all screening tokens: `db.update_status(pid, "FT_FLAGGED")`, `"FT_ELIGIBLE"`,
`"FT_SCREENED_OUT"`, plus `complete_stage(...)`. **Entry points:** `python -m engine.agents.ft_screener`
(its `__main__`) and `scripts/ft_screening_smoke_test.py`. `run_pipeline` does not call it.
**Tests:** `tests/test_ft_screening.py::TestFTParseError::test_malformed_output_flags_paper`,
`::TestFTScreeningSkipsAdvancedStatus::test_ft_screen_ai_audit_complete_records_decision`,
`::TestMissingParsedText::test_no_parsed_text_marks_ft_flagged`,
`tests/test_ollama_preflight.py::TestRunnerIntegration::test_ft_screening_calls_preflight`. There is no
direct test of `run_ft_verification`.
**I2: holds (READ).** Selection by `PARSED` and `FT_ELIGIBLE` is screening-stage. The in-scope reads are
the `AI_AUDIT_COMPLETE` selection and the extraction-token half of the three `_PAST_FT` sets.

### Reader 11 — `engine/adjudication/audit_adjudicator.py`

| read (quoted) | function | P/D | event-side source |
|---|---|---|---|
| `db.get_papers_by_status("AI_AUDIT_COMPLETE")` | `_collect_papers_for_review` | P (queue membership) | y (`effective_state`) |
| `"SELECT id, low_yield FROM extractions WHERE paper_id = ? ORDER BY id DESC LIMIT 1"` | `_collect_papers_for_review` | P (routes a paper to review, `review_reason = "low_yield"`) | y: `audit_events.low_yield(conn, pid, arm, codebook=, threshold=)` (R148) |
| `"SELECT * FROM evidence_spans WHERE extraction_id = ?"`, then `s["audit_status"] in _NEEDS_REVIEW` | `_collect_papers_for_review` | P (review queue) | **n**: no per-span audit verdict is stored on the event side (see I3) |
| `review_db.get_papers_by_status("AI_AUDIT_COMPLETE")` + `"""SELECT COUNT(*) FROM evidence_spans es JOIN extractions e ON e.id = es.extraction_id WHERE e.paper_id = ? AND es.audit_status IN ('contested', 'flagged', 'invalid_snippet')"""` | `check_audit_review_gate` | P (a gate) | **n** (verdict) |
| `"SELECT id FROM extractions WHERE paper_id = ? ORDER BY id DESC LIMIT 1"`, `"SELECT * FROM evidence_spans WHERE extraction_id = ? AND field_name = ?"`, `"""SELECT COUNT(*) FROM evidence_spans WHERE extraction_id = ? AND audit_status IN ('contested', 'flagged', 'invalid_snippet')"""` | `import_audit_review_decisions` (body after the raise) | P | **unreachable**: the first statement is `raise AuditAdjudicationDeprecated(DEPRECATION_MESSAGE)` |
| `"SELECT id FROM extractions WHERE paper_id = ? ORDER BY id DESC LIMIT 1"`, `"SELECT * FROM evidence_spans WHERE extraction_id = ?"` | `_import_legacy_format` | P | **unreachable**: its only call, `return _import_legacy_format(review_db, ws, col_index)`, is inside the refusing function's dead body |

**Legacy writes in this file (READ):** `UPDATE evidence_spans SET audit_status = 'verified' …`,
`INSERT INTO audit_adjudication` (the table 018 dropped), `review_db.update_status(pid,
"HUMAN_AUDIT_COMPLETE")`, `review_db.reject_paper(...)`. **All are behind the raise.**
`complete_stage(review_db._conn, "AUDIT_QUEUE_EXPORTED", …)` in `export_audit_review_queue` is live.
**Callers / entry points:** no CLI, and no caller in `engine/`, `scripts/` or `analysis/`.
`engine/adjudication/__init__.py` re-exports `export_audit_review_queue` and
`import_audit_review_decisions`, and `workflow.py` names them only in guidance text.
`check_audit_review_gate` has no caller outside its own `def`. `export_audit_review_queue` calls
`generate_extraction_audit_html` (reader 13) when `format="html"`.
**Tests:** `tests/test_audit_adjudication.py` (12 dependent ids; Q1) and
`tests/test_low_yield.py::TestLowYieldInAuditQueue` (2 ids).

### Reader 12 — `engine/review/human_review.py`

| read (quoted) | function | P/D | event-side source |
|---|---|---|---|
| `"SELECT * FROM papers WHERE status = 'AI_AUDIT_COMPLETE' ORDER BY id"` | `export_review_queue` | D (export) | y |
| `"SELECT id FROM papers WHERE status NOT IN ('ABSTRACT_SCREENED_OUT', 'REJECTED') ORDER BY id"` (the EE-NNN map) | `export_review_queue` | D | partial (eligibility axis; EE numbering is its own concern) |
| `"SELECT id FROM extractions WHERE paper_id = ? ORDER BY id DESC LIMIT 1"` + `"""SELECT * FROM evidence_spans WHERE extraction_id = ? AND audit_status IN ('contested', 'flagged', 'invalid_snippet') ORDER BY field_name"""` | `export_review_queue` | D | **n** (verdict) |
| `"""SELECT es.id FROM evidence_spans es JOIN extractions e ON es.extraction_id = e.id WHERE e.paper_id = ? AND es.field_name = ? ORDER BY es.id DESC LIMIT 1"""` | `_import_review_csv` | P | n: the span id is the legacy key; the event key is `claim_id` |
| `"SELECT id FROM evidence_spans WHERE id = ?"` | `_import_review_json` | P | n (same) |
| `"SELECT status FROM papers WHERE id = ?"` + `"""SELECT COUNT(*) FROM evidence_spans es JOIN extractions e ON es.extraction_id = e.id WHERE e.paper_id = ? AND es.audit_status IN ('contested', 'flagged', 'invalid_snippet')"""` | `_apply_audit_decisions` | **P**: sets HUMAN_AUDIT_COMPLETE | **n** (verdict) |
| `"SELECT id FROM papers WHERE status = 'AI_AUDIT_COMPLETE'"` / `f"SELECT id FROM papers WHERE id IN ({placeholders}) AND status = 'AI_AUDIT_COMPLETE'"` + the latest-extraction read | `bulk_accept` | P | partial |

**Legacy writes in this file (READ), all live code:** `UPDATE evidence_spans SET audit_status =
'verified'…` (three forms in `_apply_audit_decisions`, one in `bulk_accept`), `"UPDATE papers SET
status = 'HUMAN_AUDIT_COMPLETE', updated_at = ? WHERE id = ?"`, and `db.reject_paper(pid, "Human review
rejection")`. **Callers / entry points:** none reachable. There is no `__main__`.
`import_review_decisions` is called only from `audit_adjudicator.import_audit_review_decisions`, after
its raise. `export_review_queue` and `bulk_accept` have no caller.
**Tests:** `tests/test_human_review.py`, all 7 ids.

### Reader 13 — `engine/review/extraction_audit_html.py::_query_review_spans`

Read (quoted): `SELECT es.id AS span_id, e.paper_id, p.title, p.authors, p.year, p.doi, p.abstract,
es.field_name, es.value AS extracted_value, es.source_snippet, es.confidence, es.audit_status,
es.audit_rationale FROM evidence_spans es JOIN extractions e ON es.extraction_id = e.id JOIN papers p ON
e.paper_id = p.id WHERE p.status = 'AI_AUDIT_COMPLETE' AND es.audit_status IN ('contested', 'flagged',
'invalid_snippet', 'low_yield') ORDER BY e.paper_id, es.id`. **D** (HTML review page). Event-side:
paper selection y; values and snippets y (`live_claim_events`/`effective_value`); **the
`audit_status` filter n**. Its `'low_yield'` is a *span* audit status, which is neither
`extractions.low_yield` nor R148's computed flag. **Entry points:** `python -m
engine.review.extraction_audit_html --review <name>` (`main`), and
`audit_adjudicator.export_audit_review_queue(format="html")`. It opens `sqlite3.connect(str(db_path))`,
a writable connection with no `mode=ro`, for a read-only page. **Tests:** zero ids reference
`_query_review_spans` or `generate_extraction_audit_html` (pattern: those two names; AST locator
returned 0).

### Reader 14 — `advance_stage --status`, `get_pipeline_stats`, `scripts/monitor_extraction.py`

- `engine/adjudication/advance_stage.py --status` → `format_workflow_status(db._conn, review_name=...)`
  → `get_workflow_status` → `"SELECT status, completed_at, metadata FROM workflow_state WHERE
  stage_name = ?"`. **Reads `workflow_state` only. No extraction table and no `papers.status`.** D.
  **Tests:** `tests/test_workflow.py::test_format_workflow_status_{all_pending,partial,bypassed,all_complete}`.
- `ReviewDatabase.get_pipeline_stats` → `get_screening_summary` (`"SELECT status, COUNT(*) as cnt
  FROM papers GROUP BY status"`) plus `"SELECT COUNT(*) FROM extractions"`, `"SELECT COUNT(*) FROM
  evidence_spans"`, and `"SELECT COUNT(*) FROM evidence_spans WHERE audit_status = '<s>'"` for
  `verified`, `flagged`, `contested` and `invalid_snippet`. **D** (logged). **Caller:**
  `scripts/run_pipeline.py`, in the `finally:` of every run: `stats = db.get_pipeline_stats()` → `"Pipeline
  stats: %s"`. After the cut-over it logs extraction and span totals that no run writes. Event-side: y
  for the counts (`effective_state`, field events); n for the verdict buckets. **Tests:**
  `tests/test_database.py::test_pipeline_stats`.
- `scripts/monitor_extraction.py::get_extracted_count` → `"SELECT COUNT(*) FROM papers WHERE status =
  'EXTRACTED'"`. **D**. It opens `sqlite3.connect(str(DB_PATH))` (writable). Its other input,
  `data/<review>/extract_log.txt`, has no writer in `engine/` or `scripts/` at HEAD, and had none in
  `scripts/run5_extract_and_audit.py` before its retirement (pattern `extract_log`: 0 hits outside
  this script). Event-side y (`processing == "extracted"`). **Tests:** zero (pattern
  `monitor_extraction` in tests/: no hit).

### Reporting readers

- **PRISMA** — `engine/exporters/prisma.py::generate_prisma_flow` / `validate_prisma_counts`. Reads
  `"SELECT status, COUNT(*) as cnt FROM papers GROUP BY status"`; the terminal sets
  `_TERMINAL_INCLUDED = {"AI_AUDIT_COMPLETE", "HUMAN_AUDIT_COMPLETE"}` and `_IN_PROGRESS` (contains
  `"EXTRACTED", "EXTRACT_FAILED"`); `"SELECT rejected_reason, COUNT(*) as cnt FROM papers WHERE status =
  'REJECTED' GROUP BY rejected_reason"`; `"SELECT COUNT(*) FROM evidence_spans WHERE audit_status =
  'verified'"` / `'flagged'`; plus screening-table joins. **The `low_yield` read is not a read of
  `extractions.low_yield`.** Quoted in full:
  `low_yield_rejected = sum(cnt for reason, cnt in rejection_reasons.items() if "low_yield" in
  reason.lower())`, which is a substring match on `papers.rejected_reason`. D (export). Called from
  `export_all` → `run_pipeline._stage_export`. Event-side: status counts y (both axes); `rejected_reason`
  n (the eligibility axis is `eligible · abstract_out · full_text_out`; there is no REJECTED state or reason on the event side); span verdict counts n. **The three tests the
  retention ledger names** ("seed the legacy column directly"):
  `tests/test_low_yield.py::TestLowYieldInAuditQueue::test_low_yield_papers_in_audit_export`,
  `::TestLowYieldInAuditQueue::test_export_includes_low_yield_spans`,
  `::TestPrismaLowYield::test_prisma_includes_low_yield_rejected`, all through
  `_seed_low_yield` (`"UPDATE extractions SET low_yield = 1 WHERE id = (SELECT MAX(id) "`). The first two
  drive reader 11, which does read the column. **In the PRISMA test the seed is inert**: its assertion
  is carried by `reject_paper(pid, "low_yield_excluded: …")`. Other PRISMA tests (by id):
  `tests/test_exporters.py::test_prisma_flow_counts`,
  `tests/test_low_yield.py::TestPrismaLowYield::test_prisma_no_low_yield_when_none_rejected`, and 8 in
  `tests/test_prisma_reconciliation.py` (`TestExtractFailed` ×2, `TestNoDoubleCount` ×2,
  `TestPDFExcludedSubcounts::test_pdf_excluded_subcounts_sum`, `TestReconciliation` ×3; see the Q6 list).
- **methods_section** — `engine/exporters/methods_section.py`: `_query_extraction_models`: `"SELECT model,
  COUNT(DISTINCT paper_id) as cnt FROM extractions WHERE model IS NOT NULL GROUP BY model"`;
  `_query_audit_models`: `"SELECT auditor_model, COUNT(DISTINCT es.extraction_id) as cnt FROM
  evidence_spans es WHERE es.auditor_model IS NOT NULL GROUP BY es.auditor_model"`. D (export, via
  `export_all`). Event-side: **y**. `run_stage_configs.model` per stage, with `run_calls` for the papers
  actually called. **Tests:** `tests/test_exporters.py::test_methods_{multi_model_ft_screening,
  placeholder_when_no_data,section_content,uses_db_audit_model,uses_db_extraction_model,
  uses_spec_screening_model}`.
- **extraction_validator** — `engine/validators/extraction_validator.py::validate_all`: `"SELECT id FROM
  papers WHERE status = ?"` over `statuses = ("EXTRACTED", "AI_AUDIT_COMPLETE",
  "HUMAN_AUDIT_COMPLETE")`, and `"""SELECT es.field_name, es.value FROM evidence_spans es JOIN
  extractions e ON es.extraction_id = e.id WHERE e.paper_id = ?"""`. It calls `validate_extraction`,
  which runs the same span SQL. `main` also runs `"""SELECT COUNT(*) FROM evidence_spans es JOIN
  extractions e ON es.extraction_id = e.id JOIN papers p ON e.paper_id = p.id WHERE p.status IN
  ('EXTRACTED', 'AI_AUDIT_COMPLETE', 'HUMAN_AUDIT_COMPLETE')"""`. D (a read-only diagnostic CLI). Event-side
  y (`iter_grid` / `effective_value`). **Callers:** its own CLI only. **Tests:** 9 ids in
  `tests/test_extraction_validator.py` plus
  `tests/test_non_value_tokens_downstream.py::test_site3_the_skip_is_wired_into_all_three_check_points`;
  `validate_all` directly: 0.
- **concordance.check_schema_parity** — `"SELECT DISTINCT codebook_hash FROM extractions"` (arm
  `local`) / `"SELECT DISTINCT codebook_hash FROM cloud_extractions WHERE arm = ?"`, over a `mode=ro`
  connection. It is kept on purpose, per its own comment: "R31: this KEEPS its legacy read,
  deliberately". It warns and never blocks (D). Caller: `run_concordance`. **Note (READ):** the branch
  `if arm == "local"` is a per-arm name test. The live spec's local arm is `local_deepseek_r1_32b`, so for
  that arm the `else` branch reads `cloud_extractions`. **Tests:**
  `tests/test_codebook_staleness.py::test_parity_{reads_codebook_hash_and_warns_without_blocking,reports_null_as_none_recorded}`,
  `tests/test_concordance_pipeline.py::TestCheckSchemaParity::test_{matching_hashes_no_warning,mismatched_hashes_warns}`.

### Summary table

| site | reads | P / D | event-side source exists |
|---|---|---|---|
| 10 ft_screener `run_ft_screening` | `get_papers_by_status("AI_AUDIT_COMPLETE")`; `_PAST_FT` ×3 | P | y |
| 10 ft_screener (screening) | `"PARSED"`, `"FT_ELIGIBLE"` selection | P | out of scope (I2) |
| 11 `_collect_papers_for_review` / `export_audit_review_queue` | AI_AUDIT_COMPLETE; `extractions.low_yield`; `evidence_spans.audit_status` | P | status y · low_yield y (R148) · **verdict n** |
| 11 `check_audit_review_gate` | AI_AUDIT_COMPLETE ⋈ `audit_status` | P (no caller) | **n** |
| 11 `import_audit_review_decisions` / `_import_legacy_format` | spans + status | unreachable (raises) | n/a |
| 12 human_review (export, import, bulk) | AI_AUDIT_COMPLETE; spans by `audit_status`; span-id keys | P + writes | status y · **verdict n** · **key n** |
| 13 `_query_review_spans` | AI_AUDIT_COMPLETE ⋈ `audit_status` | D | values y · **verdict n** |
| 14 `format_workflow_status` | `workflow_state` only | D | not a legacy extraction read |
| 14 `get_pipeline_stats` | status counts; `extractions` / `evidence_spans` counts; verdict buckets | D (every run's `finally`) | counts y · verdict n |
| 14 `monitor_extraction.py` | `status = 'EXTRACTED'` | D | y |
| PRISMA | status counts; `rejected_reason` substring; span verdict counts | D | status y · reason n · verdict n |
| methods_section | `extractions.model`; `evidence_spans.auditor_model` | D | y (manifest) |
| extraction_validator `validate_all` | status set; spans | D | y |
| concordance `check_schema_parity` | `codebook_hash` | D (warn) | kept by R31 |

## Q3 — ReviewDatabase legacy write methods

**Method.** `engine/core/database.py` read in full for methods whose SQL writes `extractions`,
`evidence_spans` or `papers.status`. Callers found by `grep -rnE "\.<method>"` over `engine/ scripts/
analysis/` (definition lines excluded) and over `tests/`. Test ids came from the AST locator above,
applied to the Q6 list. `workflow_state` is not written by any `ReviewDatabase` method: the
constructor seeds it through `ensure_workflow_table`, and `engine/adjudication/workflow.py` writes it.

**`update_status` vocabulary as written.** `STATUSES` = `INGESTED, ABSTRACT_SCREENED_IN,
ABSTRACT_SCREENED_OUT, ABSTRACT_SCREEN_FLAGGED, PDF_ACQUIRED, PDF_EXCLUDED, PARSED, FT_ELIGIBLE,
FT_SCREENED_OUT, FT_FLAGGED, EXTRACT_FAILED, EXTRACTED, AI_AUDIT_COMPLETE, HUMAN_AUDIT_COMPLETE,
REJECTED`. Extraction-stage edges in `ALLOWED_TRANSITIONS`: `"PARSED": {..., "EXTRACTED",
"EXTRACT_FAILED"}`, `"FT_ELIGIBLE": {"EXTRACTED", "EXTRACT_FAILED", "FT_FLAGGED"}`, `"EXTRACT_FAILED":
{"PARSED", "FT_ELIGIBLE", "EXTRACTED"}`, `"EXTRACTED": {"AI_AUDIT_COMPLETE"}`, `"AI_AUDIT_COMPLETE":
{"HUMAN_AUDIT_COMPLETE", "REJECTED"}`, `"HUMAN_AUDIT_COMPLETE": {"REJECTED"}`. `_STATUS_ORDER` (for
`min_status_gate`): `PARSED 0, ABSTRACT_SCREENED_OUT 1, EXTRACTED 2, AI_AUDIT_COMPLETE 3,
HUMAN_AUDIT_COMPLETE 4`.

| method | writes | production callers (pattern `\.<method>`, engine/ scripts/ analysis/) | test ids (AST locator; by file) | serves-only |
|---|---|---|---|---|
| `add_extraction` | `INSERT INTO extractions` | **0** | 45: test_exporters 19, test_database 11, test_human_review 7, test_low_yield 4, test_codebook_staleness 4 | — |
| `add_extraction_atomic` | `INSERT INTO extractions` + `INSERT INTO evidence_spans` | **0** | 9: test_atomic_terminal_write 4, test_codebook_provenance 2, test_database 2, test_ollama_client 1 | — |
| `add_evidence_span` | `INSERT INTO evidence_spans` | **0** | 37: test_exporters 19, test_database 8, test_human_review 7, test_low_yield 3 | — |
| `update_audit` | `UPDATE evidence_spans SET audit_status …` | **0** | 36: test_exporters 19, test_database 7, test_human_review 7, test_low_yield 3 | — |
| `update_status` with EXTRACTED / EXTRACT_FAILED / AI_AUDIT_COMPLETE | `UPDATE papers SET status` | **0** | (see Q1: test_database, test_extractor, test_low_yield, test_codebook_staleness, test_extraction_cleanup, test_human_review, test_audit_adjudication, test_extraction_validator, test_exporters, test_prisma_reconciliation …) | — |
| `update_status` with HUMAN_AUDIT_COMPLETE | same | **2**, both `engine/adjudication/audit_adjudicator.py` `review_db.update_status(pid, "HUMAN_AUDIT_COMPLETE")`, **both behind `raise AuditAdjudicationDeprecated`** | as above | — |
| `update_status`, screening/acquisition tokens | same | live: screener, ft_screener, the three adjudicators, pdf_parser, `scripts/rescreen_with_specialty.py`, `scripts/parse_expanded_corpus.py` | — | out of slice 3 |
| `reject_paper` | `UPDATE papers SET status = 'REJECTED', rejected_reason` | 2: `engine/review/human_review.py` (reader 12, no live entry) and `audit_adjudicator._import_legacy_format` (unreachable) | 3: test_database 2, test_low_yield 1 | — |
| `reset_for_reaudit` | `UPDATE evidence_spans SET audit_status = 'pending'…`, `UPDATE papers SET status = 'EXTRACTED' … WHERE status IN ('AI_AUDIT_COMPLETE', 'HUMAN_AUDIT_COMPLETE', 'AUDITED')` | **0** | 2: `test_database.py::test_reset_for_reaudit_{atomicity,preserves_extraction_data}` | — |
| `cleanup_orphaned_spans` | `DELETE FROM evidence_spans WHERE extraction_id NOT IN (...)` (after `auto_backup`) | **0** | 1: `test_database.py::test_cleanup_orphaned_spans` | — |
| `admin_reset_status` | `CREATE TABLE IF NOT EXISTS admin_resets`, `INSERT INTO admin_resets`, `UPDATE papers SET status` (any token) | **0** | 2: `test_database.py::test_admin_reset_{status_succeeds_and_logs,invalid_target_raises}` | — |
| (read) `get_stale_extractions` | — | **0** | 5: test_codebook_staleness 4, test_database 1 | serves no writer |
| (read) `min_status_gate` + `_STATUS_ORDER` | — | **0** | 3 (`test_database.py::test_min_status_gate*`) + `test_audit_adjudication.py::test_min_status_filtering` reads `_STATUS_ORDER` | — |
| (read) `get_pipeline_stats` | — | 1: `scripts/run_pipeline.py` (`finally:`) | 1 | reader 14 |

**I5: holds (READ + census)** for `add_extraction_atomic`, `update_audit`, and `update_status` with
`EXTRACTED` / `EXTRACT_FAILED` / `AI_AUDIT_COMPLETE`. The only `update_status` calls with a
post-extraction token (`HUMAN_AUDIT_COMPLETE`) are unreachable. The census also finds `add_extraction`,
`add_evidence_span`, `reset_for_reaudit`, `cleanup_orphaned_spans` and `admin_reset_status` at 0
production callers. **Writers outside `ReviewDatabase`** that write the same legacy stores are listed
under Findings (human_review, audit_adjudicator, `scripts/rescreen_with_specialty.py`).

## Q4 — `count_populated_fields`

**Method.** READ `engine/agents/auditor.py`; the census used pattern `count_populated_fields` over
`engine/ scripts/ tests/ analysis/`.

Signature: `def count_populated_fields(extraction_data: dict | list, non_value_tokens: frozenset[str] =
frozenset(), *, absence_sentinels: frozenset[str]) -> int:`. Docstring, quoted: "Count non-null,
non-absence extracted fields in an extraction. Handles both v1 format (dict of field_name→value) and v2
format (list of span dicts with 'field_name' and 'value' keys)." It continues with the
`non_value_tokens` paragraph (ELICIT-DESIGN-02 D1, site 2) and the `absence_sentinels` paragraph
(R124/R136). Body: `if isinstance(extraction_data, list):` → `span.get("value") if isinstance(span,
dict) else None`; `elif isinstance(extraction_data, dict):` → `for _key, value in
extraction_data.items()`. Both branches decide through `is_populated` (R136, the one predicate).

**Shapes accepted today:** the v2 list `[{field_name, value, ...}]` and the v1 dict `{field_name:
value}`. **Callers:** one in production, `engine/agents/audit_events.py::low_yield`, which passes **the
list shape**: `values = [{"field_name": f["name"], "value": effective_value(conn, paper_id, f["name"],
arm, sentinels=declared).value} for f in codebook.fields]`, then `count_populated_fields(values,
_non_value_tokens(codebook), absence_sentinels=codebook.absence_sentinel_set)`. **Tests** (9 ids):
`tests/test_low_yield.py::TestCountPopulatedFields::*` (6, **all v1 dict**),
`tests/test_non_value_tokens_downstream.py::test_site2_terminal_states_do_not_count_as_populated`
(list), `::test_site2_handles_the_v1_dict_shape_too` (dict),
`tests/test_atomic_terminal_write.py::test_extracted_data_keeps_the_list_shape_downstream_readers_expect`
(the list read back from `extractions.extracted_data`, written by `add_extraction_atomic`). The
event-side path is also covered by `tests/test_audit_events.py::test_t10_low_yield_reads_the_codebook`
(through `AE.low_yield`).

**What "dict shape" would change (description only).** The event path already builds a reader row per
codebook field, in the list shape, one entry per field whether it has a value or not. So the
denominator is the codebook, not what the extractor emitted. The v1 dict branch exists for a legacy
`extracted_data` JSON blob, and no production caller passes one any more. Retiring it would narrow
the signature to the list, or to an iterable of reader rows or `EffectiveValue`s. It would also move
the seven dict-shape tests to the list shape, or retire them. The list-from-`extracted_data` test
pins `add_extraction_atomic`, not the event path.

## Q5 — B9

**Method.** READ `engine/validators/distribution_monitor.py` in full at the named functions;
`engine/cloud/base.py::run_distribution_check`; CLAUDE.md. The census used pattern
`run_post_extraction_check` over `engine/ scripts/ tests/ analysis/`.

- `run_post_extraction_check(db_path, review_name, arm, codebook_path, *, extracted_count=0,
  failed_count=0, strict=False, ...)`. It reads **nothing directly**. It returns early when
  `extracted_count < 10`, when `failed_count > 0`, or when the codebook is missing. Otherwise it calls
  `check_distribution(db_path, review_name, arm, codebook_path, ...)` → `conn =
  sqlite3.connect(f"file:{Path(db_path).resolve()}?mode=ro", uri=True)` → `by_field =
  _query_all_fields(conn, arm, non_value, codebook=codebook)` → `for _pid, fname, _arm, ev in
  iter_grid(conn, codebook=codebook, arms=(arm,))`. **Every value it examines comes through the reader
  (`engine/core/effective.py::iter_grid`); no legacy table is read.** Then `assert_no_collapse(results,
  strict=strict)` raises `DistributionCollapseError` on COLLAPSED.
- **Callers at HEAD:** one, `engine/cloud/base.py::CloudExtractorBase.run_distribution_check` (`from
  engine.validators.distribution_monitor import run_post_extraction_check`). There is none on the
  local path: `run_pipeline`, `engine/agents/extractor.py` and `engine/elicitation/pipeline.py` do not
  reference it (pattern returned 0 in those files). The CLI `python -m
  engine.validators.distribution_monitor --arm <arm>` calls `check_distribution` directly.
- **Tests:** `tests/test_distribution_monitor.py::TestRunPostExtractionCheck::*`: `test_called_on_completion`,
  `test_collapsed_raises_distribution_collapse_error`, `test_low_variance_does_not_raise_by_default`,
  `test_results_in_summary`, `test_skipped_on_partial_run_failures`, `test_skipped_on_partial_run_too_few`,
  `test_strict_mode_raises_on_low_variance` (7).
- **CLAUDE.md, verbatim**, in section "## Concordance Analysis Architecture", bullet anchored
  "Distribution collapse detection (engine/validators/distribution_monitor.py)": "- Distribution collapse
  detection (engine/validators/distribution_monitor.py): post-extraction quality gate, flags
  COLLAPSED/LOW_VARIANCE categorical fields, minimum 10 papers, runs automatically at end of all
  extraction pipelines". Pipeline-stages item 8 ("**DISTRIBUTION CHECK** — Post-extraction quality gate:
  detect categorical field collapse across any arm") makes the same implicit claim.
- **The 6a monitor (I4):** module `engine/validators/distribution_monitor.py`, migrated at `9370aee`
  ("refactor(analysis): concordance and the distribution monitor route through the registry").
  `_query_values` / `_query_all_fields` read through `registered_arms` + `iter_grid`.
  Callers: `check_distribution` (and so `run_post_extraction_check`), the CLI, and the tests.
- **One function or two: ONE monitor.** `run_post_extraction_check` is a gating wrapper
  (thresholds, skip rules, raise) around the same `check_distribution` that 6a migrated. There is no
  second monitor that reads legacy tables. **I4 is false (READ)**; see Findings N1.

## Q6 — Collected-id baseline

`python -m pytest tests/ --collect-only -q -m "not network and not ollama and not integration"` at
40ea017. pytest's own line reads **`2859/2876 tests collected (17 deselected)`**. The id file lists the
**2,859** selected ids, one per line, sorted, with `2859` on the first line:
`docs/session-reports/write-path-01/collected_ids_40ea017.txt`. **I6:** the 2,876 figure is pytest's
collected total, equal to 2,858 + 1 + 17. The file holds 2,876 − 17 = 2,859, which is 2,858 passed + 1
xfailed at P0. All ids are unique (`sort -u` gives 2,859).

## Findings

Findings that contradict the brief's ledger are marked **[ledger]**. Items no slice-3 ruling names
are marked **[scope]**. They are listed, not folded in.

- **N1 [ledger] — I4 is false: there is one distribution monitor, and it reads through the reader.**
  `run_post_extraction_check` → `check_distribution` → `_query_all_fields` → `iter_grid(conn,
  codebook=codebook, arms=(arm,))`, over a `mode=ro` connection. It reads no legacy table. Row B9's
  measured half holds: no local-path caller since `6e09166`; `engine/cloud/base.py` is the only caller;
  CLAUDE.md's "runs automatically at end of all extraction pipelines" is false for the local path. B9
  is therefore a wiring and wording question only. There is no reader to migrate.
- **N2 [scope] — The extractor's run path still reads `extractions`.** `_extract_selected`'s pre-flight
  calls `check_stale_extractions(db, schema_hash)` (`"""SELECT COUNT(DISTINCT paper_id) FROM
  extractions WHERE (codebook_hash IS NULL OR codebook_hash != ?)"""`) and logs an informational count.
  After the cut-over no run writes `extractions`, so on live the count describes the frozen Run-6
  population indefinitely. The whole `engine/utils/extraction_cleanup.py` module (the report,
  `_RESETTABLE_STATUSES = {"EXTRACTED", "AI_AUDIT_COMPLETE"}`, and the retired `--confirm`) is in the
  same position. It is surviving, its only input is legacy, and it is on no reader list. Its 7 (a)
  tests cannot be rewritten to an event fixture.
- **N3 [scope] — Legacy WRITERS outside `ReviewDatabase`.** Item 3 names only `ReviewDatabase`'s
  methods, but four more writers exist:
  - `engine/review/human_review.py`: `UPDATE evidence_spans` ×4, raw `"UPDATE papers SET status =
    'HUMAN_AUDIT_COMPLETE', updated_at = ? WHERE id = ?"`, and `reject_paper`. Live code with no
    reachable entry point.
  - `engine/adjudication/audit_adjudicator.py`: `UPDATE evidence_spans` ×5, `INSERT INTO
    audit_adjudication` (the table 018 dropped), and `update_status(..., "HUMAN_AUDIT_COMPLETE")` ×2.
    All unreachable behind the raise.
  - `engine/validators/extraction_validator.py::normalize_categorical_values`: `"UPDATE evidence_spans
    SET value = ? WHERE id = ?"`. No production caller at HEAD; `git log -S` shows only its introducing
    commit `3aa7e11`.
  - `scripts/rescreen_with_specialty.py`: raw `"UPDATE papers SET status = ?, updated_at = ? WHERE id =
    ?"`, which reads `AI_AUDIT_COMPLETE` papers.
- **N4 — Reader 11 is mostly unreachable.** `import_audit_review_decisions` raises first. Its dead body
  is the only call site of `_import_legacy_format`. `check_audit_review_gate` has no caller.
  `export_audit_review_queue` / `_collect_papers_for_review` have no CLI and no caller in `engine/`,
  `scripts/` or `analysis/`; they are only re-exported. Reader 12 has no reachable entry at all. Its
  only in-code caller is the dead body above.
- **N5 [ledger] — PRISMA does not read `extractions.low_yield`.** Its `low_yield_rejected` is a
  substring count over `papers.rejected_reason`. The only reader of the column at HEAD is reader 11's
  `"SELECT id, low_yield FROM extractions WHERE paper_id = ? ORDER BY id DESC LIMIT 1"`. Of the three
  tests the retention ledger says "seed the legacy column", two drive reader 11. In the third
  (`test_prisma_includes_low_yield_rejected`) the seed is inert, and `reject_paper`'s reason carries
  the assertion. R148's "readers of it re-point in slice 3" therefore concerns reader 11 alone.
- **N6 [scope] — P8 missed test files.** These files outside P8's 22 carry legacy constructs by the
  Q1 pattern and were not classified here: `tests/test_exporters.py` (19 ids via `add_extraction` /
  `add_evidence_span` / `update_audit` + `mirror_legacy_into_events`), `tests/test_prisma_reconciliation.py`,
  `tests/test_adjudication_pairs.py`, `tests/test_concordance_pipeline.py`, `tests/test_db_backup.py`,
  `tests/analysis/paper1/test_judge_cli.py`, `tests/analysis/paper1/test_judge_loader.py`,
  `tests/analysis/paper1/test_pi_audit_sampler.py`, `tests/test_corpus_authority.py`,
  `tests/test_parse_gate.py`, `tests/test_pdf_quality_import.py`, `tests/test_stage_completion.py`,
  `tests/test_screener.py` and `tests/test_migration_018_cloud_shape.py`. Some of these may be
  screening-token or migration-only; that is unmeasured.
- **N7 — P8 is partly stale.** `test_retry_failed.py` is gone. `test_elicitation_pipeline.py` no longer
  fakes the writer. `test_citation_guard.py`'s `_FakeDB` is never handed to engine code, so
  `test_uncited_value_never_reaches_the_database`'s `db.writes == 0` cannot fail. `test_auditor.py` and
  `test_request_capture.py` no longer build legacy rows. `TestCheckLowYield` survives as a
  one-test class.
- **N8 — Checks that cannot fail (28 class-(b) ids).** Examples:
  - 7 `test_extractor.py` ids whose fake `extract_paper` INSERTs a row nothing reads;
  - 6 `test_extraction_cleanup.py` refusal tests. `DeletionRetired` is raised before `db._conn` is
    touched, so "keeps every extraction/span" measures the fixture;
  - `test_audit_adjudication.py::test_missing_span_not_counted_as_success` (raise-first);
  - 12 `test_cloud_extraction.py` ids that need no paper rows at all.

  Also not counted as dependent: `test_atomic_terminal_write.py::test_a_pre_write_refusal_stores_nothing`,
  whose witness is a legacy-table count that nothing writes any more.
- **N9 [scope] — `workflow_state` is a live store with no event-side equivalent.** `complete_stage`
  writes it in production: from the screeners, the adjudicators, `run_pipeline`'s reader-8 advance and
  `export_audit_review_queue`. Reader 14's `format_workflow_status` reads only `workflow_state`. There
  are 17 a† ids. If the rulings keep `workflow_state` in slice 3, these have no rewrite target. If
  they don't, reader 14 reduces to `get_pipeline_stats` and `monitor_extraction.py`.
- **N10 [scope] — Other extraction-token and legacy-table readers not in P2 / R129.** These are
  `engine/utils/extraction_cleanup.py` (N2), `engine/acquisition/pdf_quality_html.py` (status lists
  including `'AI_AUDIT_COMPLETE'`, `'EXTRACTED'`, `'HUMAN_AUDIT_COMPLETE'`),
  `scripts/rescreen_with_specialty.py`, `scripts/backfill_authors.py`, `scripts/rescreen_original_251.py`,
  and `ReviewDatabase.get_screening_summary` (via `get_pipeline_stats`). A grep of `engine/ scripts/
  analysis/` for `FROM extractions|JOIN extractions|FROM evidence_spans|JOIN evidence_spans|evidence_spans es`
  lists 26 files, including 12 under `analysis/` and 3 under `scripts/` (`_pass2_eyeball.py`,
  `q8_validation.py`, `q8_validation_fast.py`). Those are unclassified here.
- **N11 — `get_pipeline_stats` runs in every `run_pipeline` `finally:`**, logging `total_extractions`,
  `total_evidence_spans` and four `audit_status` buckets that no post-cut-over run writes.
- **N12 — Three test copies of the status→eligibility map disagree.**
  `test_cloud_extraction.py::_seed_corpus_events` omits `EXTRACT_FAILED`.
  `_corpus_fixture._ELIGIBLE_FROM_STATUS` and `mirror_legacy_into_events` include it.
- **N13 — `_event_store_fixture._PAPERS_DDL` defaults `status` to `'EXTRACTED'`.** So every papers row
  `add_values` creates carries an extraction token that no event reader sees, but that any status
  reader would.
- **N14 — `test_cloud_extraction.py` depends on a gitignored database,**
  `data/surgical_autonomy/review_backup_v1_schema.db`. A module `skipif` hides all 36 ids where it is
  absent, and 18 (a) ids `pytest.skip("No pending papers")` on an empty corpus. At P0 the gate showed
  0 skipped, so the file is present on this box.
- **N15 — Stale docstrings or comments.**
  - `engine/agents/audit_events.py`: "Until then nothing in the run path calls this module" (it has
    been wired since `6e09166`).
  - `engine/adjudication/workflow.py`: stages 9–10 are still described as set from EXTRACTED status
    (they have been set from the event store since `4756bd2`).
  - `tests/test_low_yield.py::_seed_low_yield`: "readers 10–14 still read". Only reader 11 reads the
    column.
- **N16 — `auditor_model_digest` is a dead parameter.** `extract_paper`,
  `extract_paper_with_completeness` and `engine/elicitation/pipeline.py` accept and pass it along
  (`auditor_model_digest=auditor_digest`), but no event writer consumes it. Its only sink is
  `add_extraction_atomic`, which has no caller.
- **N17 — `check_schema_parity`'s `if arm == "local":` is a name test.** The live local arm is
  `local_deepseek_r1_32b`, which takes the `cloud_extractions` branch. R31 keeps the read, and this
  finding is about the branch only.
- **N18 — Writable connections for read-only work.** `extraction_audit_html.generate_extraction_audit_html`
  (`sqlite3.connect(str(db_path))`) and `scripts/monitor_extraction.py` (`sqlite3.connect(str(DB_PATH))`)
  open `review.db` without `mode=ro`.
- **N19 — Incidental, out of slice 3.**
  - `OpenAIExtractor.run`'s `if max_papers:` treats `0` as unlimited.
  - `CloudExtractorBase.store_result` still INSERTs `cloud_extractions` / `cloud_evidence_spans`,
    which 016's docstring lists as read-only legacy (fork F15 open).
- **N20 — I6 wording.** 2,876 is pytest's collected total. The committed baseline has 2,859 lines of
  ids.

## What Phase 2 must decide

Options only; no recommendation.

1. **F16 — fixture rewrites: wholesale vs per reader.**
   (i) Wholesale: one commit rewrites every (a) id to event fixtures, and (b) ids drop their dead
   fixture. Readers move afterwards, with their (c) tests.
   (ii) Per reader: each reader's commit carries its own (c) tests. The (a), (b) and (d) sets go in
   separate commits alongside the item they pin.
   (iii) Per class: (b) dead-weight removal first (no behaviour change), then (d) with item 3, then (c)
   per reader, then (a).
   Both (i) and (ii) must also rule on N6's unclassified files, `mirror_legacy_into_events` (its
   consumer `test_exporters.py` breaks when the (d) writers go), the a† set (N9), the (m) migration
   test, and N14's skip behaviour.
2. **Reader 10 (`ft_screener`, extraction-token reads only).** Re-point the `AI_AUDIT_COMPLETE`
   selection and the extraction half of `_PAST_FT` to `effective_state`. Or hold it to the screeners'
   cut-over, since the reads sit inside screening code.
3. **Reader 11 (`audit_adjudicator`).** Retire it under R47: its import raises, its gate has no
   caller, and its export has no entry point. Or re-point the export only (status and `low_yield` have
   event sources; the span verdict does not) and hold the rest to session 12's importer. Or hold the
   whole module to session 10 (B8's verdict table) or session 12.
4. **Reader 12 (`human_review`).** Retire it under R47 (no reachable entry; it contains legacy writes,
   N3). Or hold it to session 12, whose importer is specified against this path. Its keys are legacy
   span ids, and there is no event equivalent.
5. **Reader 13 (`extraction_audit_html`).** Re-point everything but the `audit_status` filter. Or hold
   it to session 10 (B8's verdict table). Or retire it with reader 11. It has a live CLI and no tests.
6. **Reader 14.** `format_workflow_status` is not a legacy extraction read, so the options are to
   declare it out of scope or to rule `workflow_state` into scope (N9). For `get_pipeline_stats`:
   re-point its counts to the reader, drop its extraction and span totals, or retire the `finally:`
   log line. Its verdict buckets have no event source. For `monitor_extraction.py`: re-point or retire
   under R31/R47. Its log input has no writer (Q2).
7. **Reporting readers.** For PRISMA: re-point the status counts to both axes; decide how
   `rejected_reason` and the span verdict counts are represented (neither has an event source). The
   low_yield read is not the column (N5). For `methods_section`: re-point to `run_stage_configs`.
   For `validate_all` / `validate_extraction`: re-point to `iter_grid`, and rule separately on
   `normalize_categorical_values` (N3). For `check_schema_parity`: keep it under R31, with N17 as a
   separate question.
8. **Item 3 scope.** Retire the `ReviewDatabase` writers with 0 production callers (`add_extraction`,
   `add_extraction_atomic`, `add_evidence_span`, `update_audit`, `reset_for_reaudit`,
   `cleanup_orphaned_spans`, `admin_reset_status`, and the extraction edges of `ALLOWED_TRANSITIONS` /
   `update_status`), plus the reads `get_stale_extractions` / `min_status_gate` / `_STATUS_ORDER`. Then
   decide whether the non-`ReviewDatabase` writers in N3 join item 3 or go with their readers. Decide
   also what happens to N16's `auditor_model_digest`.
9. **Item 4 (`count_populated_fields`).** Narrow it to the list/reader-row shape and retire the v1 dict
   branch and its seven dict-shape tests (rewrite or retire). Or keep both shapes: the event path
   already passes the list, so the dict branch serves tests only.
10. **B9, given N1.**
    (i) Wire `run_post_extraction_check` onto the local path, meaning `run_pipeline`'s extract stage or
    `run_extraction`, with `extracted_count` / `failed_count` from the run. Correct CLAUDE.md to name
    the paths.
    (ii) Rule the automatic check retired on the local path, leave the CLI as the local route, and
    correct CLAUDE.md's sentence and pipeline item 8.
    (iii) Hold wiring to session 10's smoke run, correct CLAUDE.md now, and keep B9 ARMED until then.
11. **Sixth-item scope alarms.** Decide whether each of these joins slice 3, is held, or is left out:
    N2 (`check_stale_extractions` on the run path, and `extraction_cleanup`), N3 (writers outside
    `ReviewDatabase`), N6 (unclassified test files), N9 (`workflow_state`), and N10 (other token
    readers in `engine/`, `scripts/` and `analysis/`).
