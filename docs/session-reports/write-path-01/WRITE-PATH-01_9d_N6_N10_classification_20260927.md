# WRITE-PATH-01 9d-C4 — N6 / N10 classification (R179 as amended by 9d-C4)

Read-only census at HEAD `17009edb554e1c30937e5dcd8140f5161af646f9`. Every label is READ: the file
and the code it drives were read at HEAD; grep only located. Id counts come from
`pytest --collect-only -q` (all tiers) at HEAD. The census being updated is
`WRITE-PATH-01_phase1_slice3_census_20260925.md` at `9456c40`; where it is stale, that is said in
the row. No engine, script, test or analysis file was edited by this commit.

Labels: **EXTRACTION-TOKEN** (reads or seeds an extraction-stage status or a legacy
extraction/span row for a surviving reader) · **SCREENING-TOKEN** (screening statuses/tables only;
live, out of slice 3) · **MIGRATION-ONLY** (pins a migration or schema shape) · **ANALYSIS** (Paper 1
telemetry on legacy tables by design, R18/Q10) · **NOT-DEPENDENT** (the construct is inert or gone)
· **RETIRE-CANDIDATE** (R47: its subject has no surviving reader). Owners: 9e (3b) · 9e (PRISMA) ·
session 10 · junior · session 12 · none.

---

## §1 N6 — the 14 test files

| file | ids | legacy construct (quoted anchor) | drives | label | owner |
|---|---|---|---|---|---|
| `tests/test_exporters.py` | 22 | `populated_db` and `db_with_empty_extractions`: `db.add_extraction(`, `db.add_evidence_span(`, `db.update_audit(`, `db.update_status(pid, "EXTRACTED")` / `"AI_AUDIT_COMPLETE"`, then `mirror_legacy_into_events(db)`; two inline fixtures `update_status(... "EXTRACTED" / "AI_AUDIT_COMPLETE")` | PRISMA (`generate_prisma_flow` reads `papers.status`), methods (PRISMA keys), evidence CSV/Excel/DOCX (through the mirror) | EXTRACTION-TOKEN | 9e (PRISMA), then 9e (3b) |
| `tests/test_prisma_reconciliation.py` | 8 | `db.update_status(pid, "EXTRACTED")`, `"AI_AUDIT_COMPLETE"`, `"EXTRACT_FAILED"`; raw `UPDATE papers SET pdf_exclusion_reason`, `SET status = 'BOGUS_STATUS'`, `INSERT INTO papers` | PRISMA | EXTRACTION-TOKEN | 9e (PRISMA) |
| `tests/test_adjudication_pairs.py` | 28 | own DDL `CREATE TABLE extractions (` / `CREATE TABLE evidence_spans (` + raw `INSERT INTO extractions VALUES`; private mirror `_mirror(db_path)` reading `FROM evidence_spans es JOIN extractions e` into events | `analysis.paper1.adjudication` (which itself reads `FROM evidence_spans es JOIN extractions e`) | ANALYSIS | none |
| `tests/test_concordance_pipeline.py` | 29 | `TestCheckSchemaParity._make_db`: `CREATE TABLE extractions (` + `INSERT INTO extractions (paper_id, codebook_hash)` — 2 ids (`test_matching_hashes_no_warning`, `test_mismatched_hashes_warns`); the other 27 do not touch legacy | `engine.analysis.concordance.check_schema_parity` (`"SELECT DISTINCT codebook_hash FROM extractions"`, kept, R31) | EXTRACTION-TOKEN | session 12 (A16) |
| `tests/test_db_backup.py` | 8 | raw `INSERT INTO extractions (...)` / `INSERT INTO evidence_spans (...)` as sample content, asserted after the copy (`SELECT model FROM extractions`) | the backup API (a byte copy) | NOT-DEPENDENT | none |
| `tests/analysis/paper1/test_judge_cli.py` | 14 | raw `INSERT INTO papers ... 'EXTRACTED'`, `INSERT INTO extractions`, `INSERT INTO evidence_spans`, then `_mirror_spans_into_events(rdb)` imported from `test_judge_loader` | `analysis.paper1.judge_cli` (reader since 6a) | ANALYSIS | none |
| `tests/analysis/paper1/test_judge_loader.py` | 16 | raw `'EXTRACTED'` papers rows, `INSERT INTO extractions` / `evidence_spans`; defines `_mirror_spans_into_events` (`FROM evidence_spans es JOIN extractions e`) | `analysis.paper1.judge_loader` (reader since 6a) | ANALYSIS | none |
| `tests/analysis/paper1/test_pi_audit_sampler.py` | 9 | raw `INSERT INTO extractions (id, paper_id)` / `INSERT INTO evidence_spans` | `analysis.paper1.pi_audit_sampler` (reads `evidence_spans` directly) | ANALYSIS | none |
| `tests/test_corpus_authority.py` | 22 | imports `CORPUS_STATUSES, corpus_status_sql, is_corpus_member` (frozen `engine/core/corpus.py`, R35) and `ALLOWED_TRANSITIONS, STATUSES`; `test_the_transition_graph_still_does_not_yield_the_four_by_closure` walks `ALLOWED_TRANSITIONS` from `FT_ELIGIBLE` and pins `{"EXTRACT_FAILED", "FT_FLAGGED", "FT_SCREENED_OUT", "PARSED", "REJECTED"}` | the frozen corpus authority (017's import), `analysis.eval.schema_eval2`, `CloudExtractorBase` pending/progress | MIGRATION-ONLY | 9e (3b) for the transition-closure id (§3 C); none otherwise |
| `tests/test_parse_gate.py` | 26 | `test_reparse_writes_new_version_and_leaves_status_alone`: raw `UPDATE papers SET status='EXTRACTED'` / `'AI_AUDIT_COMPLETE'`, asserts the status is `"AI_AUDIT_COMPLETE"  # untouched` after `reparse_papers`; `update_status("ABSTRACT_SCREENED_IN")`, `("PDF_ACQUIRED")` in `_staged` | the parse gate / `reparse_papers` | NOT-DEPENDENT | none (raw SQL, survives R160b) |
| `tests/test_pdf_quality_import.py` | 25 | `db.update_status(pid, "ABSTRACT_SCREENED_IN")`; raw `UPDATE papers SET status = 'PDF_EXCLUDED'` | `engine.acquisition.pdf_quality_import` | SCREENING-TOKEN | junior |
| `tests/test_stage_completion.py` | 3 | `test_t6_a_legacy_status_alone_completes_nothing`: raw `UPDATE papers SET status = 'AI_AUDIT_COMPLETE'`, asserts it completes nothing; the other two write processing-axis events | `run_pipeline._advance_extraction_workflow` (already on the processing axis) | NOT-DEPENDENT | none (the status is a negative control) |
| `tests/test_screener.py` | 19 | `test_complete_stage_no_such_table_handled` mocks `OperationalError("no such table: workflow_state")`; no extraction token | `engine.agents.screener.run_verification` | SCREENING-TOKEN | junior |
| `tests/test_migration_018_cloud_shape.py` | 12 | `CREATE TABLE evidence_spans (id INTEGER PRIMARY KEY);` (shape stub) | migration 018 | MIGRATION-ONLY | none |

N6 by label: EXTRACTION-TOKEN 3 · ANALYSIS 4 · NOT-DEPENDENT 3 · SCREENING-TOKEN 2 ·
MIGRATION-ONLY 2 · RETIRE-CANDIDATE 0.

Staleness against the census: `test_exporters.py` is 22 ids now (census "19 ids via ..."; +3 in 9d
C2/C3, of which 4 ids no longer use a legacy fixture — `test_methods_uses_db_extraction_model`,
`test_methods_uses_db_audit_model`, `test_methods_counts_papers_not_calls`,
`test_export_all_exports_the_spec_arm`). Two files match the census's Q1 pattern only through
screening constructs (`test_pdf_quality_import.py`, `test_screener.py`), which the census allowed
("some of these may be screening-token").

---

## §2 N10 — engine / script / analysis sites

The grep `FROM extractions|JOIN extractions|FROM evidence_spans|JOIN evidence_spans|evidence_spans es`
over `engine/ scripts/ analysis/` lists **20** files at HEAD. The same grep at `9456c40` lists
**27** (the census reported 26). Seven have left since, none added:
`engine/adjudication/audit_adjudicator.py`, `engine/review/extraction_audit_html.py`,
`engine/review/human_review.py`, `engine/utils/extraction_cleanup.py` (9c C2/C3/C4),
`engine/core/database.py` (9c C5, the retired no-consumer members), `engine/validators/
extraction_validator.py` (9d C1), `engine/exporters/methods_section.py` (9d C2).

No `analysis/` file in the list is imported by engine code: engine imports only
`analysis.provenance.segment` (`engine/elicitation/units.py`, `contracts.py`,
`engine/parsers/parse_quality.py`), which reads no table. `scripts/_pass2_stability.py` imports
`analysis.paper1.judge` / `judge_loader` (neither in the list).

Scripts: git records edits, not runs, so whether a script has **run** since `40ea017`
(2026-09-25) cannot be established from git. Each script's last edit is given; every one predates
`40ea017`. No log under `logs/` names any of them.

| site | hit (quoted) | drives | label | owner |
|---|---|---|---|---|
| `engine/analysis/concordance.py` | `"SELECT DISTINCT codebook_hash FROM extractions"` in `check_schema_parity` | concordance's schema-parity warning (kept, R31) | EXTRACTION-TOKEN | session 12 (A16) |
| `engine/exporters/prisma.py` | `"SELECT COUNT(*) FROM evidence_spans WHERE audit_status = 'verified'"` / `'flagged'` (+ the `papers.status` reads) | PRISMA, methods | EXTRACTION-TOKEN | 9e (PRISMA) |
| `engine/exporters/evidence_table.py` | docstring only: "What was here was `"SELECT id FROM extractions WHERE paper_id = ? ORDER BY id DESC LIMIT 1"`" | — | NOT-DEPENDENT | none |
| `engine/exporters/docx_export.py` | docstring only, the same quotation | — | NOT-DEPENDENT | none |
| `engine/migrations/006_not_null_confidence_tier.py` | `"SELECT COUNT(*) FROM evidence_spans"` | applied migration 006 | MIGRATION-ONLY | none |
| `engine/acquisition/pdf_quality_html.py` (named) | `WHERE p.status IN ('ABSTRACT_SCREENED_IN', 'AI_AUDIT_COMPLETE', 'PDF_ACQUIRED', 'PARSED', 'FT_ELIGIBLE', 'EXTRACTED', 'HUMAN_AUDIT_COMPLETE')` (twice) | the acquisition / quality-check HTML generators — extraction tokens used as "past screening" | EXTRACTION-TOKEN | junior (the same dead-read shape as D18) |
| `ReviewDatabase.get_screening_summary` (named) | `"SELECT status, COUNT(*) as cnt FROM papers GROUP BY status"` — every status, extraction tokens included | `get_pipeline_stats()["screening"]` (R171) | SCREENING-TOKEN | junior |
| `scripts/rescreen_with_specialty.py` (named) | `TARGET_STATUSES = ("ABSTRACT_SCREENED_IN", "AI_AUDIT_COMPLETE")`; `_force_status`: raw `UPDATE papers SET status = ?` "bypasses state machine" | a one-off re-screen; last edit `521b92a` 2026-09-12 | RETIRE-CANDIDATE | ruled later |
| `scripts/backfill_authors.py` (named) | `AND status IN ('ABSTRACT_SCREENED_IN', 'AI_AUDIT_COMPLETE', ... 'EXTRACTED', 'HUMAN_AUDIT_COMPLETE')` | one-off author backfill; last edit `e124b20` 2026-03-19 | RETIRE-CANDIDATE | ruled later |
| `scripts/rescreen_original_251.py` (named) | `old_status in ("SCREENED_IN", "AI_AUDIT_COMPLETE", "EXTRACTED", "EXTRACT_FAILED", ... "HUMAN_AUDIT_COMPLETE")` — `"SCREENED_IN"` is not a status | one-off re-screen to CSV; last edit `fa6c738` 2026-09-10 | RETIRE-CANDIDATE | ruled later |
| `scripts/_pass2_eyeball.py` | `"SELECT extracted_data FROM extractions WHERE paper_id=? "` | "One-off: dump 28 UNSUPPORTED Pass 2 smoke verdicts"; last edit `ab96078` 2026-04-22 | RETIRE-CANDIDATE | ruled later |
| `scripts/q8_validation.py` | `"SELECT extracted_data FROM extractions WHERE paper_id = ?"` | q8_0 KV-cache re-extraction comparison; last edit `fa6c738` 2026-09-10 | RETIRE-CANDIDATE | ruled later |
| `scripts/q8_validation_fast.py` | same query | same, fast variant; last edit `fa6c738` 2026-09-10 | RETIRE-CANDIDATE | ruled later |
| `analysis/eval/analyze_qualgap01.py` | `FROM evidence_spans s JOIN extractions e`; `SELECT reasoning_trace FROM extractions` | QUALGAP-01 | ANALYSIS | none |
| `analysis/eval/analyze_schema_eval.py` | `FROM evidence_spans s JOIN extractions e` | SCHEMA-EVAL-01 | ANALYSIS | none |
| `analysis/eval/analyze_schema_eval2.py` | same, twice | SCHEMA-EVAL-02 | ANALYSIS | none |
| `analysis/eval/elicit01/manifest.py` | `"SELECT DISTINCT paper_id FROM extractions"` | ELICIT-01 | ANALYSIS | none |
| `analysis/eval/parse01/sweep.py` | `"SELECT DISTINCT paper_id FROM extractions ORDER BY paper_id"` | PARSE-01 | ANALYSIS | none |
| `analysis/eval/prime01.py` | `SELECT reasoning_trace FROM extractions`; `FROM evidence_spans s JOIN extractions e` | PRIME-01 | ANALYSIS | none |
| `analysis/eval/schema_eval2.py` | `FROM evidence_spans s JOIN extractions e` | SCHEMA-EVAL-02 sampler | ANALYSIS | none |
| `analysis/paper1/adjudication.py` | `FROM evidence_spans es JOIN extractions e ON e.id = es.extraction_id` | Paper 1 adjudication export | ANALYSIS | none |
| `analysis/paper1/judge_provenance.py` | `FROM evidence_spans s JOIN extractions e` | Paper 1 judge provenance | ANALYSIS | none |
| `analysis/paper1/pi_audit_sampler.py` | `SELECT value, source_snippet FROM evidence_spans WHERE ... SELECT id FROM extractions WHERE paper_id = ?` | PI audit v1 sampler | ANALYSIS | none |
| `analysis/paper1/spanloss_autopsy.py` | `FROM evidence_spans s WHERE s.extraction_id = e.id`; `FROM extractions e` | Paper 1 span-loss autopsy | ANALYSIS | none |
| `analysis/provenance/census.py` | `FROM evidence_spans s JOIN extractions e` | provenance census | ANALYSIS | none |

N10 by label (25 rows: 20 grep files + 5 named sites): ANALYSIS 12 · RETIRE-CANDIDATE 6 ·
EXTRACTION-TOKEN 3 · NOT-DEPENDENT 2 · MIGRATION-ONLY 1 · SCREENING-TOKEN 1.

Four of the six RETIRE-CANDIDATE scripts are pinned in `tests/test_review_paths.py`'s
`SPEC_BEARING_ENTRY_POINTS` list (`q8_validation.py`, `q8_validation_fast.py`,
`rescreen_original_251.py`, `rescreen_with_specialty.py`); retiring any of them removes its
parametrized ids there (R47 precedent: the list's comments).

---

## §3 The 3b blocker list (R160b)

Every test id whose fixture or body calls `add_extraction`, `add_evidence_span`, `update_audit`,
`reject_paper`, or `update_status` into an extraction-stage status (`EXTRACTED`, `EXTRACT_FAILED`,
`AI_AUDIT_COMPLETE`, `HUMAN_AUDIT_COMPLETE`, `REJECTED`), read per fixture and helper. Raw-SQL
seeding (`UPDATE papers SET status`, `INSERT INTO extractions`) is not listed: it does not pass
through the writers R160b retires.

**A. Writers — the four `ReviewDatabase` methods (through a fixture or helper).**

| file | fixture / helper | ids | reader served |
|---|---|---|---|
| `tests/test_exporters.py` | `populated_db` | `test_prisma_flow_counts`, `test_prisma_csv`, `test_atomic_prisma_csv_no_partial_on_error`, `test_evidence_csv_columns`, `test_evidence_excel_sheets`, `test_atomic_csv_no_partial_on_error`, `test_docx_created`, `test_atomic_docx_no_partial_on_error`, `test_methods_section_content`, `test_methods_md_export`, `test_atomic_methods_md_no_partial_on_error`, `test_methods_uses_spec_screening_model`, `test_export_all` (13) | PRISMA; evidence CSV/Excel/DOCX via `mirror_legacy_into_events`; methods (PRISMA keys only) |
| `tests/test_exporters.py` | `db_with_empty_extractions` | `test_empty_extraction_has_marker`, `test_exclude_empty_omits_empty_papers`, `test_exclude_empty_excel` (3) | evidence CSV/Excel via the mirror |
| `tests/test_exporters.py` | inline (`add_extraction`, `add_evidence_span`, `update_audit`) | `test_methods_multi_model_ft_screening` (1) | methods (FT screening models; the extraction writes are incidental to its assertion) |
| `tests/test_low_yield.py` | `_advance_to_ai_audit` + `reject_paper` | `TestPrismaLowYield::test_prisma_includes_low_yield_rejected` (1) | PRISMA (`rejected_reason`) |
| `tests/test_low_yield.py` | inline `add_extraction` | `TestLowYieldSchema::test_low_yield_defaults_to_zero` (1) | none — pins the legacy `extractions.low_yield` column default |
| `tests/test_database.py` | inline | `test_evidence_spans_and_audit`, `test_evidence_spans_contested_status`, `test_evidence_spans_invalid_snippet_status`, `test_null_confidence_raises_integrity_error`, `test_null_tier_raises_integrity_error` (5) | none — the writers' own tests |
| `tests/test_database.py` | `_walk_to_ai_audit` + `reject_paper` | `test_reject_paper`, `test_reject_paper_invalid_status` (2) | none — retire at R160b as ruled |

**B. The extraction-stage `update_status` edges (no writer call).**

| file | ids | what it does |
|---|---|---|
| `tests/test_exporters.py` | `test_methods_placeholder_when_no_data` (1) | walks one paper to `EXTRACTED` → `AI_AUDIT_COMPLETE`; methods |
| `tests/test_prisma_reconciliation.py` | `TestReconciliation::test_reconciliation_passes_clean_db`, `TestNoDoubleCount::test_no_paper_in_multiple_terminal_boxes`, `TestNoDoubleCount::test_ai_audit_complete_not_double_counted_with_ft`, `TestExtractFailed::test_extract_failed_appears_in_flow_and_csv`, `TestExtractFailed::test_extract_failed_zero_omitted_from_csv` (5) | PRISMA status counts |
| `tests/test_database.py` | `test_full_lifecycle`, `test_ai_to_human_audit_transition`, `test_invalid_transition_raises`, `test_normal_pipeline_cannot_use_admin_transition`, `test_update_status_invalid_transition_still_raises` (5) | the state machine itself; the last three are R160b's named B5 rewrites |
| `tests/test_ft_screening.py` | `TestFTTransitions::test_ft_eligible_to_extracted`, `TestFTTransitions::test_parsed_can_skip_ft_to_extracted` (2) | pin the edges `FT_ELIGIBLE → EXTRACTED` and `PARSED → EXTRACTED` |
| `tests/test_ft_screening.py` | `TestFTScreeningSkipsAdvancedStatus::test_ft_screen_ai_audit_complete_records_decision` via `_advance_to_ai_audit` (1) | reader 10 (R163 as amended: raw-SQL seeding at R160b) |

**C. Pins on the transition graph (break when R160b removes edges).**

| file | ids | pin |
|---|---|---|
| `tests/test_corpus_authority.py` | `test_the_transition_graph_still_does_not_yield_the_four_by_closure` (1) | the closure of `ALLOWED_TRANSITIONS` from `FT_ELIGIBLE` minus the corpus set equals `{"EXTRACT_FAILED", "FT_FLAGGED", "FT_SCREENED_OUT", "PARSED", "REJECTED"}` |

Totals: **A 26 ids** (test_exporters 17, test_low_yield 2, test_database 7) · **B 14 ids** ·
**C 1 id** — **41 ids in 6 files** (`test_exporters.py`, `test_prisma_reconciliation.py`,
`test_low_yield.py`, `test_database.py`, `test_ft_screening.py`, `test_corpus_authority.py`).

Not blockers, by read: `tests/test_parse_gate.py` and `tests/test_stage_completion.py` set
extraction statuses by raw SQL only; the analysis tests (§1) seed legacy rows by raw SQL.

---

## §4 `min_status`

Call sites that pass it: `engine/exporters/__init__.py::export_all` only —
`export_evidence_csv(..., min_status=min_status, arm=arm)`, `export_evidence_excel(...,
min_status=min_status, arm=arm)`, `export_evidence_docx(..., min_status=min_status, arm=arm)`, and
`_build_evidence_rows(db, spec, min_status=min_status, ...)` inside `evidence_table.py` (twice). No
caller of `export_all` passes it (`run_pipeline._stage_export` and every test use the default). In
all three exporters it is declared `min_status: str = "AI_AUDIT_COMPLETE"` and documented as
selecting nothing ("kept for call compatibility and **no longer selects papers**").

Retirement plan for 9e (not worked): remove the parameter from `export_all`, the three exporters and
`_build_evidence_rows`; edit the four internal call lines; no test passes it, so no test call line
changes; `CLAUDE.md`'s "min_status parameter on exporters" key-pattern line is the architect's to
edit.

---

## §5 `normalize_prefix`

Definition: `engine/validators/extraction_validator.py`, `def normalize_prefix(value: str,
valid_values: list[str]) -> str:` ("If *value* is an unambiguous case-insensitive prefix of exactly
one valid value, return the canonical form").

Every grep hit (`grep -rn normalize_prefix --include=*.py`), read:
- the definition;
- `tests/test_extraction_validator.py`: the import, and the four tests `test_normalize_prefix_exact_match`, `test_normalize_prefix_unambiguous`, `test_normalize_prefix_ambiguous`, `test_normalize_prefix_no_match`;
- **`tests/test_non_value_tokens_downstream.py::test_site3_a_terminal_state_never_reaches_the_rewrite_path`**, which imports and calls it: `assert normalize_prefix("CONTRACT_UNMET", enum) == "CONTRACT_UNMET_BUT_CATEGORICAL"`. Its docstring ("`normalize_prefix` UPDATEs the row when ...") describes the in-place rewrite path that retired at 9c-C5 (R160a); the function returns a string and writes nothing;
- 10 mentions in `docs/**/*.md` (records, not callers).

**Callers by read: zero in engine/, scripts/, analysis/. Five test ids call it**, not four: the
brief's I2 ("its four tests") is false. `_closest_match` does not call it. Retiring it removes five
ids and edits `tests/test_non_value_tokens_downstream.py`, a file outside 9d-C4's PERMISSIONS, so
commit 2 was not made (see the session report).

Other zero-caller functions in the module (report only): **`verify_schema_parity(spec)`** — no
caller in engine/, scripts/ or analysis/; its only callers are `tests/test_extraction_validator.py::
test_same_spec_same_hash` and `::test_modified_codebook_different_hash`. Every other function has an
in-module or engine caller (`select_arm`, `open_read_only`, `ArmNotRegistered` are used by `main`).

---

## §6 Findings not asked for

- The census's N10 grep count was 26; the same grep at `9456c40` returns 27.
- Three copies of one fixture helper exist in tests: `mirror_legacy_into_events`
  (`tests/_event_store_fixture.py`), `_mirror` (`tests/test_adjudication_pairs.py`) and
  `_mirror_spans_into_events` (`tests/analysis/paper1/test_judge_loader.py`, also used by
  `test_judge_cli.py`) — "one predicate, two programs" in tests.
- `tests/test_ft_screening.py::TestFTTransitions::test_ft_eligible_to_extracted` and
  `::test_parsed_can_skip_ft_to_extracted` pin extraction-stage edges and are not named in R160b.
- `tests/test_database.py`'s seven writer ids (§3 A) and `test_full_lifecycle` /
  `test_ai_to_human_audit_transition` are not named in R160b either; they test the retiring
  writers and edges themselves.
- `tests/test_low_yield.py::TestLowYieldSchema::test_extractions_has_low_yield_column` and
  `::test_low_yield_defaults_to_zero` pin a legacy column whose only reader retired at 9c
  (`4c5310c`, R148 addendum).
- `tests/test_prisma_reconciliation.py::TestReconciliation::test_reconciliation_catches_mismatch`
  asserts `valid is True`; its name says the opposite (already in the Phase A read-out).
- `test_site3_a_terminal_state_never_reaches_the_rewrite_path`'s docstring describes a rewrite
  path that no longer exists.
- `scripts/rescreen_original_251.py` tests membership in `"SCREENED_IN"`, which is not a status.
- `get_screening_summary` (the `screening` sub-dict of `get_pipeline_stats`) counts every
  `papers.status`, extraction tokens included; after R160b those counts freeze at their last value.
