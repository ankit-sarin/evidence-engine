# 12i — scoping read for the internal review (which engine code assessments A and B covered)

**Status: COMPLETE.** Read-only. No change under `engine/`, `tests/`, `scripts/`, `analysis/`,
`review_specs/` or `data/`; no model call; no network call beyond git; no live write (live opened
`mode=ro` only). Ends at a STOP: **no internal-review read has been started and no lot is open.**
No inventory row was opened.

| | |
| --- | --- |
| Reference HEAD | `0e9093d37621ed92f25b909b367125d120cc0c68` (the 12h closeout) |
| Purpose | Unified plan v78 §6 item 1, as quoted in the 12i-OPEN brief: the internal review covers "the packages no outside assessment read, and papers 4, 23, 168"; "Its first step is a read-only scoping read by CC (which packages A and B covered) before any review read." |
| Scripts and outputs | `12i_scope/` beside this file. Every count below is computed by a script named with it, or is labelled as CC's reading. |
| Live fingerprint | IDENTICAL to the migration-02 record at open (overall `0d3eedead60c2ce1c6341b973fa5931d69c4b13c14e93bd5b805bc06734fcb25`, 36 tables) |

## What this read-out says, in short

- **The census is 149 `.py` units, 40,732 lines** (`engine/` 124 units, 35,010 lines; `scripts/` 25
  units, 5,722 lines). 108 units (30,486 lines) are reachable by import from
  `scripts/run_pipeline.py`; 59 of them (18,046 lines) at import time, the other 49 only through an
  import written inside a function.
- **A finding is located to 25 units (9,442 lines) by at least one assessment**: 17 by B, 10 by A,
  2 by both. The other 124 units (31,290 lines) have no located finding: 54 (15,412 lines) sit
  inside an area an assessment says it reviewed, and **70 (15,878 lines) are mentioned by neither**.
- **On the Run 7 import graph, 87 units (21,986 lines) have no located finding** — 36 units
  (7,829 lines) mentioned by neither assessment and 51 (14,157 lines) inside a stated area only.
- Of the 89 "inside a stated area" cells (43 for A, 46 for B), 67 rest on CC's reading of an area phrase, not on a
  file the assessment names. A's coverage of `engine/elicitation/` is the clearest case: A's
  section is titled "Elicitation & Prompt Pipelines" and every path it cites there is under
  `analysis/eval/`.
- **A located finding is not a read of the file.** B calls itself "a targeted assessment, not an
  exhaustive semantic review of every line"; `engine/agents/extractor.py` (1,077 lines) is graded F
  on one function name in A. §3e carries this as a question for the ruling.
- Papers 4, 23 and 168 are `papers.id` values on live; each is `ABSTRACT_SCREENED_OUT` with FT
  decision rows, as R549 says, and they are the only three such papers.

## 3a — Outside inputs

`sha256sum` and `wc -c` over `docs/session-reports/session-12/outside/` at HEAD:

| file | sha256 | bytes |
| --- | --- | ---: |
| `README.md` | `b49db24dea680e2f8c4d55c7ec877852ac9dc51d3321017f28a34b51541e41c6` | 1,447 |
| `assessment_A_architectural.md` | `2ef65a1a90c4ba68f58ed58559a370f954738b5ecfdbfe2b269425ac294a8bb4` | 14,089 |
| `assessment_B_17_findings.md` | `95ea43c2bdf7189de0694ba0eaafb8b30f5cfd5bf7832d3522795684d8f18e37` | 36,733 |

Both assessment hashes equal the ones `outside/README.md` and `12f_triage.md` record.

**I1 — confirmed.** Both files were read in full for this read-out.
- B carries the line `**Source SHA-256:** 7507f4d9f89bf68831b142a6a90e2bf802468c264d64c422c4cb9d9819764e97`,
  the scope sentence "Reviewed active search, deduplication, database construction/migrations,
  acquisition, parsing publication, screening, local model calls, provenance readers, exports, and
  related tests." and the words "this was a targeted assessment, not an exhaustive semantic review
  of every line or historical experiment". It has seventeen `### Fnn` sections, F01 to F17.
- A has four numbered sections — "Local Compute Concurrency & Workstation Execution", "State
  Machine & Database Integrity", "Elicitation & Prompt Pipelines", "Analysis, Provenance &
  Exporters" — each with three finding bullets, then four "Fresh-Pass" gaps and a four-row
  "Targeted Implementation Plan" table. `coverage.py` cuts A into those 12 + 4 + 4 finding parts.
  By `matches.json` (PATH and DOTTED rows) A names 24 distinct tracked files — 12 under
  `analysis/` (9 of them under `analysis/eval/`), 11 under `engine/`, 1 under `scripts/` — in 34
  path mentions, 18 of them under `analysis/` (12 under `analysis/eval/`). The brief's R4 says
  "Most of its file references are under analysis/eval/"; by these counts it is the largest single
  group and under half.

**I6 — unknown.** `outside/README.md` says: "The commit that export was taken from is not
identified here." Neither `12f_triage.md`, `12f_triage_rows.md`, `12f_C56-A.md` nor
`12f_repro/READER_RULES.md` contains the word "repomix"; the reader rules call it "an unknown
earlier snapshot". The commit is not inferred here. (Recorded only: B reports "approximately
35,018 Python source lines" for the engine; `engine/` at HEAD is 35,010 lines by `census.py`.)

## 3b — Census of code units at HEAD

Script: `12i_scope/census.py` (`.venv/bin/python docs/session-reports/session-12/12i_scope/census.py . docs/session-reports/session-12/12i_scope`).
Outputs: `census.json` (units, entry points, every import edge), `census.csv`.

- **Units:** every git-tracked `.py` under `engine/` and `scripts/`, one row each — **149 units,
  40,732 lines** (`engine/` 124 / 35,010; `scripts/` 25 / 5,722). Lines are newline counts.
  The per-unit rows, with line count and the reachability columns, are the table in §3c.
- **Not units, recorded:** four tracked non-`.py` files under the two trees —
  `engine/migrations/README.md` (109 lines), `scripts/nightly_tests.sh` (25),
  `scripts/run_expanded_screen_and_verify.sh` (15), `scripts/watch_run4.sh` (19). B's F17 names
  `scripts/nightly_tests.sh`, `requirements.txt` and `pyproject.toml`.
- **`analysis/` at directory level** (`.py` files only): `analysis/eval` 31 files / 6,897 lines;
  `analysis/paper1` 23 / 13,976; `analysis/provenance` 10 / 1,617; `analysis/__init__.py` 0 lines.
- **`analysis/` modules imported by `engine/` or `scripts/`** (AST):

  | module | lines | imported by | on the Run 7 graph |
  | --- | ---: | --- | --- |
  | `analysis/provenance/segment.py` | 64 | `engine/elicitation/contracts.py`, `engine/elicitation/units.py`, `engine/parsers/parse_quality.py` (all at module level) | yes, at import time |
  | `analysis/provenance/__init__.py` | 5 | the same three (as the parent package) | yes, at import time |
  | `analysis/__init__.py` | 0 | the same three, and `scripts/_pass2_stability.py` | yes, at import time |
  | `analysis/paper1/judge.py` | 348 | `scripts/_pass2_stability.py` | no |
  | `analysis/paper1/judge_loader.py` | 427 | `scripts/_pass2_stability.py` | no |
  | `analysis/paper1/__init__.py` | 0 | `scripts/_pass2_stability.py` | no |

  The three engine imports of `analysis.provenance.segment` are the existing row A-5 (R530); nothing
  new is recorded for them here. `segment.py` is outside the census and inside Run 7's import-time
  set, so it is named in lot L07 below.

**The import graph and the RUN7 column.** Every `import` / `from … import` node of every tracked
`.py` under `engine/`, `scripts/` and `analysis/`, at any nesting depth, resolved to tracked files
(parent packages included). An edge is *module-level* unless the import sits inside a function or
lambda. One edge is declared by hand because the AST cannot see it: `engine/migrations/runner.py`
→ every `engine/migrations/NNN_*.py` (`importlib.import_module(f"engine.migrations.{migration_id}")`).

- **RUN7 = `top`**: reachable from `scripts/run_pipeline.py` over module-level edges only — the
  code executed by importing the runner. **59 units, 18,046 lines.**
- **RUN7 = `lazy`**: reachable only through at least one in-function import. **49 units, 12,440
  lines** (45 at one such edge, 4 at two). Whether Run 7 executes the function that holds the
  import is **not** established by the graph. The elicited extractor is here
  (`engine/elicitation/pipeline.py`, one in-function edge from `extractor.py`), and so are the
  cloud arms, acquisition, the 21 numbered migrations and the FT screener.
- **RUN7 = `-`**: not reachable by import. **41 units, 10,246 lines.**
- **FT** = the same from `engine/agents/ft_screener.py` (`python -m engine.agents.ft_screener`);
  **MIG** = the same from `engine/migrations/__main__.py` (`python -m engine.migrations`).
- `census.json` also carries, per unit, which of sixteen further entry points CLAUDE.md's
  "Running" block names reach it (`OTHER`: `scripts/screen_expanded.py`, the five acquisition
  CLIs, `extraction_validator`, `db_fingerprint`, `inventory`, `ollama_preflight`,
  `scripts/run_cloud_extraction.py`, `distribution_monitor`, `scripts/backfill_cloud_spans.py`,
  the two `q8_validation` scripts, `advance_stage`). It is not a table column: over all edges
  almost every engine unit is reachable from 14 of the 16 (`OTHER_n` in `census.csv`), so the
  column does not separate units.
- **Limits of the graph.** It sees imports, not calls: a `lazy` or even a `top` unit may never
  run in Run 7. It does not see subprocess launches; `census.json` lists 23 string references
  from census units that name another module (`string_refs`), which are not edges —
  `engine/adjudication/workflow.py` carries most of them, as operator messages.

## 3c — Coverage map

Script: `12i_scope/coverage.py`, with CC's judgements in `12i_scope/judgement.json`.
Outputs: `coverage.json`, `coverage.csv`, `coverage_table.md` (the table below),
`coverage_summary.md`, `matches.json` (every script match and every ambiguous token).

**Grades**, per unit and per assessment: **F** a finding is located to the unit · **C** inside an
area the assessment says it reviewed, no finding located to it · **N** not mentioned.
**Marks:** **S** set by the script from the assessment text · **J** set by CC's reading, with the
reason in the row.

**Script rules (S).** Each assessment is cut into *finding parts* (B: each `### Fnn` section; A:
each bullet of §1–§4 as `A-§n.m`, each Fresh-Pass gap as `A-FPn`, each implementation-table row
as `A-Tn`) and *non-finding parts* (everything else). A `.py` path, a dotted module name, or the
name of a function, class or module-level assignment that resolves to exactly one tracked file is
a match. A match inside a finding part grades **F** and cites the part; a match only outside
finding parts, or a named directory, grades **C**; no match grades **N**. A token that resolves to
several files is left to judgement (4 tokens: `units.py` twice, `MIN_UNIT_TOKENS`, `_norm`).
B's line numbers are never used.

**Judgement (J).** Two kinds, both in `judgement.json`:
- *Area rules* (16): a quoted area phrase mapped to unit globs. They grade a unit **C** only where
  the script found nothing; three of them record a deliberate **N** where the phrase looks like a
  package name but the assessment's text does not reach the package (`engine/analysis/` for A;
  the parser support modules and the standalone screening and adjudication units for B).
- *Overrides* (10): three of A's script F cells and one of B's lowered to C because the file is
  named as background or consequence and the finding is located elsewhere (migrations 015, 016,
  017; `parsed_text.py`); one false name match rejected (`source_snippet` in
  `materialize.py`); one F kept with an incidental name match noted (`ollama_client.py`); the ambiguous `units.py` resolved to A's own full path under
  `analysis/eval/`; three openpyxl-importing units graded C for B's F15 ("other workbook builders
  accepting external strings", which B does not name).
- "scope-only" in a reason means the assessment states the area and names no file or function of
  that package.

**The 12F column (I5).** 12f did not record which files its readers read. Its rules told each
reader to "Quote the code that decides the matter, by CONTENT ANCHOR: file path + function/class
name + the quoted lines", so what is on the page is the files its rows *name*. The column records
only that, and nothing is reconstructed:
`Q<n>` — the unit's path occurs n times in `12f_triage_rows.md`; `H<n>` — it occurs only in
`12f_C56-A.md` (the broad-handler census: handlers were read, not the file); `not recorded` —
neither. **`not recorded` is not "not read"**, and `Q` does not say how much of the file was read.
Q 76 units (26,396 lines of file size), H 2 (642), not recorded 71 (13,694).

### Summary

**Assessment A** — units (lines)

| grade | RUN7 top | RUN7 lazy | not on the Run 7 import graph | all |
| --- | ---: | ---: | ---: | ---: |
| F | 7 (3,011) | 1 (83) | 2 (214) | 10 (3,308) |
| C | 11 (4,286) | 31 (7,384) | 1 (57) | 43 (11,727) |
| N | 41 (10,749) | 17 (4,973) | 38 (9,975) | 96 (25,697) |
| all | 59 (18,046) | 49 (12,440) | 41 (10,246) | 149 (40,732) |

**Assessment B** — units (lines)

| grade | RUN7 top | RUN7 lazy | not on the Run 7 import graph | all |
| --- | ---: | ---: | ---: | ---: |
| F | 11 (5,032) | 4 (1,524) | 2 (728) | 17 (7,284) |
| C | 13 (5,313) | 30 (6,567) | 3 (1,255) | 46 (13,135) |
| N | 35 (7,701) | 15 (4,349) | 36 (8,263) | 86 (20,313) |
| all | 59 (18,046) | 49 (12,440) | 41 (10,246) | 149 (40,732) |

**Either assessment (the higher of the two grades)** — units (lines)

| grade | RUN7 top | RUN7 lazy | not on the Run 7 import graph | all |
| --- | ---: | ---: | ---: | ---: |
| F | 16 (6,893) | 5 (1,607) | 4 (942) | 25 (9,442) |
| C | 15 (5,790) | 36 (8,367) | 3 (1,255) | 54 (15,412) |
| N | 28 (5,363) | 8 (2,466) | 34 (8,049) | 70 (15,878) |
| all | 59 (18,046) | 49 (12,440) | 41 (10,246) | 149 (40,732) |

**Cells by mark** — A C·J 22; A C·S 21; A F·J 1; A F·S 9; A N·J 6; A N·S 90; B C·J 45; B C·S 1; B F·S 17; B N·J 11; B N·S 75

**12F column** — Q 76 units (26,396 lines), H 2 (642), not recorded 71 (13,694)

**On the Run 7 import graph, N by both** — 36 units, 7,829 lines (top 28 / 5,363; lazy 8 / 2,466)

- `engine/__init__.py` — 0 lines, RUN7 top, A N·S, B N·S, 12F not recorded
- `engine/adjudication/__init__.py` — 43 lines, RUN7 top, A N·S, B N·S, 12F not recorded
- `engine/adjudication/abstract_adjudication_html.py` — 708 lines, RUN7 lazy, A N·S, B N·J, 12F Q1
- `engine/adjudication/categorizer.py` — 259 lines, RUN7 top, A N·S, B N·J, 12F not recorded
- `engine/adjudication/ft_adjudication_html.py` — 533 lines, RUN7 lazy, A N·S, B N·J, 12F not recorded
- `engine/agents/__init__.py` — 0 lines, RUN7 top, A N·S, B N·S, 12F not recorded
- `engine/agents/audit_events.py` — 250 lines, RUN7 top, A N·S, B N·S, 12F Q1
- `engine/agents/auditor.py` — 259 lines, RUN7 top, A N·S, B N·S, 12F Q1
- `engine/cloud/anthropic_extractor.py` — 260 lines, RUN7 lazy, A N·S, B N·S, 12F Q1
- `engine/cloud/base.py` — 517 lines, RUN7 lazy, A N·S, B N·S, 12F Q3
- `engine/cloud/openai_extractor.py` — 232 lines, RUN7 lazy, A N·S, B N·S, 12F Q1
- `engine/core/__init__.py` — 0 lines, RUN7 top, A N·S, B N·S, 12F not recorded
- `engine/core/audit_telemetry.py` — 49 lines, RUN7 top, A N·S, B N·S, 12F Q1
- `engine/core/citation_guard.py` — 210 lines, RUN7 top, A N·S, B N·S, 12F not recorded
- `engine/core/codebook.py` — 482 lines, RUN7 top, A N·S, B N·S, 12F not recorded
- `engine/core/constants.py` — 10 lines, RUN7 top, A N·S, B N·S, 12F Q1
- `engine/core/effective_config.py` — 534 lines, RUN7 top, A N·S, B N·S, 12F Q3
- `engine/core/eligibility_render.py` — 424 lines, RUN7 top, A N·S, B N·S, 12F not recorded
- `engine/core/extraction_events.py` — 529 lines, RUN7 top, A N·S, B N·S, 12F Q4
- `engine/core/extraction_telemetry.py` — 148 lines, RUN7 top, A N·S, B N·S, 12F not recorded
- `engine/core/locator.py` — 102 lines, RUN7 top, A N·S, B N·S, 12F Q1
- `engine/core/naming.py` — 54 lines, RUN7 lazy, A N·S, B N·S, 12F not recorded
- `engine/core/reuse_key.py` — 55 lines, RUN7 top, A N·S, B N·S, 12F not recorded
- `engine/core/review_paths.py` — 92 lines, RUN7 top, A N·S, B N·S, 12F not recorded
- `engine/core/run_telemetry.py` — 69 lines, RUN7 top, A N·S, B N·S, 12F Q2
- `engine/core/selection.py` — 119 lines, RUN7 top, A N·S, B N·S, 12F not recorded
- `engine/parsers/__init__.py` — 0 lines, RUN7 top, A N·S, B N·J, 12F not recorded
- `engine/parsers/font_audit.py` — 719 lines, RUN7 top, A N·S, B N·J, 12F Q2
- `engine/parsers/markers.py` — 41 lines, RUN7 top, A N·S, B N·J, 12F Q1
- `engine/parsers/models.py` — 57 lines, RUN7 top, A N·S, B N·J, 12F not recorded
- `engine/parsers/parse_quality.py` — 364 lines, RUN7 top, A N·S, B N·J, 12F Q1
- `engine/utils/__init__.py` — 0 lines, RUN7 top, A N·S, B N·S, 12F not recorded
- `engine/utils/background.py` — 66 lines, RUN7 lazy, A N·S, B N·S, 12F not recorded
- `engine/utils/progress.py` — 96 lines, RUN7 lazy, A N·S, B N·S, 12F not recorded
- `engine/validators/__init__.py` — 1 lines, RUN7 top, A N·S, B N·S, 12F not recorded
- `engine/validators/distribution_monitor.py` — 547 lines, RUN7 top, A N·S, B N·S, 12F H1

**On the Run 7 import graph, C at best (no finding located by either)** — 51 units, 14,157 lines (top 15 / 5,790; lazy 36 / 8,367)

- `engine/acquisition/__init__.py` — 29 lines, RUN7 lazy, A N·S, B C·J, 12F not recorded
- `engine/acquisition/check_oa.py` — 197 lines, RUN7 lazy, A N·S, B C·J, 12F Q1
- `engine/acquisition/pdf_quality_check.py` — 326 lines, RUN7 lazy, A N·S, B C·J, 12F Q4
- `engine/acquisition/pdf_quality_import.py` — 316 lines, RUN7 lazy, A N·S, B C·J, 12F Q3
- `engine/acquisition/verify_downloads.py` — 379 lines, RUN7 lazy, A N·S, B C·J, 12F not recorded
- `engine/adjudication/ft_screening_adjudicator.py` — 753 lines, RUN7 top, A C·J, B C·J, 12F Q2
- `engine/adjudication/schema.py` — 78 lines, RUN7 top, A N·S, B C·J, 12F Q1
- `engine/adjudication/screening_adjudicator.py` — 899 lines, RUN7 top, A C·J, B C·J, 12F Q3
- `engine/adjudication/workflow.py` — 380 lines, RUN7 top, A C·J, B N·S, 12F Q3
- `engine/cloud/schema.py` — 96 lines, RUN7 lazy, A N·S, B C·S, 12F Q4
- `engine/core/effective.py` — 599 lines, RUN7 top, A N·S, B C·J, 12F Q2
- `engine/core/events.py` — 491 lines, RUN7 top, A C·J, B C·J, 12F Q5
- `engine/core/parsed_text.py` — 211 lines, RUN7 top, A N·S, B C·J, 12F Q1
- `engine/core/review_spec.py` — 932 lines, RUN7 top, A N·S, B C·J, 12F Q1
- `engine/core/run_manifest.py` — 766 lines, RUN7 top, A N·S, B C·J, 12F Q3
- `engine/elicitation/__init__.py` — 10 lines, RUN7 top, A C·J, B N·S, 12F not recorded
- `engine/elicitation/classes.py` — 278 lines, RUN7 top, A C·J, B N·S, 12F not recorded
- `engine/elicitation/contracts.py` — 458 lines, RUN7 lazy, A C·J, B N·S, 12F Q1
- `engine/elicitation/materialize.py` — 116 lines, RUN7 lazy, A C·J, B N·S, 12F Q1
- `engine/elicitation/pipeline.py` — 612 lines, RUN7 lazy, A C·J, B N·S, 12F Q3
- `engine/elicitation/prompts.py` — 378 lines, RUN7 lazy, A C·J, B N·S, 12F not recorded
- `engine/elicitation/terminal.py` — 104 lines, RUN7 lazy, A C·J, B N·S, 12F not recorded
- `engine/elicitation/units.py` — 132 lines, RUN7 lazy, A C·J, B N·S, 12F Q7
- `engine/exporters/docx_export.py` — 194 lines, RUN7 top, A C·J, B C·J, 12F not recorded
- `engine/exporters/review_workbook.py` — 374 lines, RUN7 lazy, A C·J, B C·J, 12F Q2
- `engine/migrations/002_screening_rename.py` — 207 lines, RUN7 lazy, A C·S, B C·J, 12F not recorded
- `engine/migrations/003_backfill_expanded_screening.py` — 411 lines, RUN7 lazy, A C·S, B C·J, 12F not recorded
- `engine/migrations/004_pdf_quality_check.py` — 95 lines, RUN7 lazy, A C·S, B C·J, 12F not recorded
- `engine/migrations/005_model_digest.py` — 83 lines, RUN7 lazy, A C·S, B C·J, 12F not recorded
- `engine/migrations/006_not_null_confidence_tier.py` — 116 lines, RUN7 lazy, A C·S, B C·J, 12F not recorded
- `engine/migrations/007_add_judge_tables.py` — 187 lines, RUN7 lazy, A C·S, B C·J, 12F not recorded
- `engine/migrations/008_add_fabrication_verifications.py` — 143 lines, RUN7 lazy, A C·S, B C·J, 12F not recorded
- `engine/migrations/009_add_backfill_audit_log.py` — 180 lines, RUN7 lazy, A C·S, B C·J, 12F not recorded
- `engine/migrations/010_add_provenance_classifications.py` — 168 lines, RUN7 lazy, A C·S, B C·J, 12F not recorded
- `engine/migrations/011_add_absence_claim_class.py` — 205 lines, RUN7 lazy, A C·S, B C·J, 12F not recorded
- `engine/migrations/012_codebook_provenance.py` — 98 lines, RUN7 lazy, A C·S, B C·J, 12F not recorded
- `engine/migrations/013_drop_schema_hash_not_null.py` — 148 lines, RUN7 lazy, A C·S, B C·J, 12F not recorded
- `engine/migrations/014_cloud_tables.py` — 60 lines, RUN7 lazy, A C·S, B C·J, 12F Q1
- `engine/migrations/015_drop_prerename_adjudication_indices.py` — 78 lines, RUN7 lazy, A C·J, B C·J, 12F not recorded
- `engine/migrations/016_event_store.py` — 263 lines, RUN7 lazy, A C·J, B C·J, 12F not recorded
- `engine/migrations/017_seed_event_store.py` — 205 lines, RUN7 lazy, A C·J, B C·J, 12F not recorded
- `engine/migrations/018_cloud_shape_and_audit_adjudication.py` — 231 lines, RUN7 lazy, A C·S, B C·J, 12F Q1
- `engine/migrations/019_paper_state_axes.py` — 304 lines, RUN7 lazy, A C·S, B C·J, 12F not recorded
- `engine/migrations/020_run_manifest.py` — 496 lines, RUN7 lazy, A C·S, B C·J, 12F not recorded
- `engine/migrations/021_parsed_text_sha256.py` — 279 lines, RUN7 lazy, A C·S, B C·J, 12F not recorded
- `engine/migrations/022_run_kinds_and_audit_tables.py` — 590 lines, RUN7 lazy, A C·S, B C·J, 12F not recorded
- `engine/migrations/__init__.py` — 0 lines, RUN7 lazy, A C·S, B C·J, 12F not recorded
- `engine/search/__init__.py` — 0 lines, RUN7 top, A N·S, B C·J, 12F not recorded
- `engine/search/models.py` — 19 lines, RUN7 top, A N·S, B C·J, 12F not recorded
- `engine/search/pubmed.py` — 180 lines, RUN7 top, A N·S, B C·J, 12F not recorded
- `engine/utils/ollama_preflight.py` — 303 lines, RUN7 lazy, A C·J, B C·J, 12F Q2

### The map — one row per unit

Cells read `grade·mark`. RUN7 / FT / MIG: `top`, `lazy` or `-` as defined in §3b.

| unit | lines | RUN7 | FT | MIG | A | B | 12F | A: cite or reason | B: cite or reason |
| --- | ---: | --- | --- | --- | --- | --- | --- | --- | --- |
| `engine/__init__.py` | 0 | top | top | top | N·S | N·S | not recorded | — | — |
| `engine/acquisition/__init__.py` | 29 | lazy | lazy | lazy | N·S | C·J | not recorded | — | B-R4: B scope sentence: '… acquisition …'; F10 is located to download.py 'and the other download strategies' (not named); no finding in this file |
| `engine/acquisition/check_oa.py` | 197 | lazy | lazy | lazy | N·S | C·J | Q1 | — | B-R4: B scope sentence: '… acquisition …'; F10 is located to download.py 'and the other download strategies' (not named); no finding in this file |
| `engine/acquisition/download.py` | 477 | lazy | lazy | lazy | N·S | F·S | Q2 | — | F10 (path `engine/acquisition/download.py`) |
| `engine/acquisition/manual_list.py` | 448 | - | - | - | N·S | C·J | Q1 | — | B-R4: B scope sentence: '… acquisition …'; F10 is located to download.py 'and the other download strategies' (not named); no finding in this file |
| `engine/acquisition/pdf_quality_check.py` | 326 | lazy | lazy | lazy | N·S | C·J | Q4 | — | B-R4: B scope sentence: '… acquisition …'; F10 is located to download.py 'and the other download strategies' (not named); no finding in this file |
| `engine/acquisition/pdf_quality_html.py` | 750 | - | - | - | N·S | C·J | not recorded | — | B-R4: B scope sentence: '… acquisition …'; F10 is located to download.py 'and the other download strategies' (not named); no finding in this file |
| `engine/acquisition/pdf_quality_import.py` | 316 | lazy | lazy | lazy | N·S | C·J | Q3 | — | B-R4: B scope sentence: '… acquisition …'; F10 is located to download.py 'and the other download strategies' (not named); no finding in this file |
| `engine/acquisition/verify_downloads.py` | 379 | lazy | lazy | lazy | N·S | C·J | not recorded | — | B-R4: B scope sentence: '… acquisition …'; F10 is located to download.py 'and the other download strategies' (not named); no finding in this file |
| `engine/adjudication/__init__.py` | 43 | top | lazy | lazy | N·S | N·S | not recorded | — | — |
| `engine/adjudication/abstract_adjudication_html.py` | 708 | lazy | lazy | lazy | N·S | N·J | Q1 | — | B-N2: deliberate non-mapping: B's area 'screening' is carried by F11 (ft_screener.py), F12 (run_pipeline._stage_screen) and F13 (screener.py); B never mentions adjudication or the standalone screening scripts |
| `engine/adjudication/advance_stage.py` | 101 | - | - | - | F·S | N·S | Q3 | A-FP2 (path `engine/adjudication/advance_stage.py`) | — |
| `engine/adjudication/categorizer.py` | 259 | top | lazy | lazy | N·S | N·J | not recorded | — | B-N2: deliberate non-mapping: B's area 'screening' is carried by F11 (ft_screener.py), F12 (run_pipeline._stage_screen) and F13 (screener.py); B never mentions adjudication or the standalone screening scripts |
| `engine/adjudication/ft_adjudication_html.py` | 533 | lazy | lazy | lazy | N·S | N·J | not recorded | — | B-N2: deliberate non-mapping: B's area 'screening' is carried by F11 (ft_screener.py), F12 (run_pipeline._stage_screen) and F13 (screener.py); B never mentions adjudication or the standalone screening scripts |
| `engine/adjudication/ft_screening_adjudicator.py` | 753 | top | lazy | lazy | C·J | C·J | Q2 | A-R4: A §4 scope, 'Analysis, Provenance & Exporters' — 'Systematic review outputs feed directly into PRISMA flow generation (exporters/prisma.py) and adjudication workbooks'; scope-only (A names prisma.py and corpus.py only; the two adjudicators are the units under engine/adjudication/ that import openpyxl) | F15's unnamed 'other workbook builders accepting external strings': this unit imports openpyxl (AST); whether it is one of those builders was not read here. B does not otherwise mention adjudication |
| `engine/adjudication/import_extraction_entry.py` | 333 | - | - | - | N·S | N·S | Q2 | — | — |
| `engine/adjudication/import_screening_entry.py` | 189 | - | - | - | N·S | N·S | not recorded | — | — |
| `engine/adjudication/schema.py` | 78 | top | lazy | lazy | N·S | C·J | Q1 | — | B-R3: B scope sentence: '… database construction/migrations …'; B also reports 'A fresh ReviewDatabase smoke check stopped at migration 014'. F02 is located to database.py and runner.py; no numbered migration is named. (adjudication/schema.py: its tables are created during ReviewDatabase construction — CLAUDE.md, the audit_adjudication row; B names no function of it) |
| `engine/adjudication/screening_adjudicator.py` | 899 | top | lazy | lazy | C·J | C·J | Q3 | A-R4: A §4 scope, 'Analysis, Provenance & Exporters' — 'Systematic review outputs feed directly into PRISMA flow generation (exporters/prisma.py) and adjudication workbooks'; scope-only (A names prisma.py and corpus.py only; the two adjudicators are the units under engine/adjudication/ that import openpyxl) | F15's unnamed 'other workbook builders accepting external strings': this unit imports openpyxl (AST); whether it is one of those builders was not read here. B does not otherwise mention adjudication |
| `engine/adjudication/workflow.py` | 380 | top | lazy | lazy | C·J | N·S | Q3 | A-R2: A §2 scope, 'State Machine & Database Integrity' — 'tracks paper states across systematic review phases … using SQLite … a two-axis model … supported by an append-only event store'; scope-only (A's §2 bullets name paper_state.py, completeness.py, advance_stage.py and three migrations, and no function of this file) | — |
| `engine/agents/__init__.py` | 0 | top | top | lazy | N·S | N·S | not recorded | — | — |
| `engine/agents/audit_events.py` | 250 | top | - | - | N·S | N·S | Q1 | — | — |
| `engine/agents/auditor.py` | 259 | top | lazy | lazy | N·S | N·S | Q1 | — | — |
| `engine/agents/extractor.py` | 1077 | top | lazy | lazy | F·S | N·S | Q8 | A-§1.2 (name `restart_ollama`) | — |
| `engine/agents/ft_screener.py` | 682 | lazy | top | lazy | N·S | F·S | Q6 | — | F11 (path `engine/agents/ft_screener.py`) |
| `engine/agents/models.py` | 47 | top | lazy | lazy | F·S | N·S | Q1 | A-§3.2 (name `ExtractionOutput`) | — |
| `engine/agents/screener.py` | 340 | top | lazy | lazy | N·S | F·S | Q5 | — | F12 (name `run_screening`); F13 (path `engine/agents/screener.py`) |
| `engine/analysis/__init__.py` | 0 | - | - | - | N·J | N·S | not recorded | A-N1: deliberate non-mapping: A §4's title says 'Analysis', but every analysis path it cites is in the top-level analysis/ tree (analysis/eval/adjud01_pairs.py, analysis/provenance/classifier.py, normalize.py); nothing in A describes concordance scoring, kappa or engine/analysis/ | — |
| `engine/analysis/concordance.py` | 399 | - | - | - | N·J | N·S | Q1 | A-N1: deliberate non-mapping: A §4's title says 'Analysis', but every analysis path it cites is in the top-level analysis/ tree (analysis/eval/adjud01_pairs.py, analysis/provenance/classifier.py, normalize.py); nothing in A describes concordance scoring, kappa or engine/analysis/ | — |
| `engine/analysis/metrics.py` | 233 | - | - | - | N·J | N·S | not recorded | A-N1: deliberate non-mapping: A §4's title says 'Analysis', but every analysis path it cites is in the top-level analysis/ tree (analysis/eval/adjud01_pairs.py, analysis/provenance/classifier.py, normalize.py); nothing in A describes concordance scoring, kappa or engine/analysis/ | — |
| `engine/analysis/normalize.py` | 181 | - | - | - | N·J | N·S | not recorded | A-N1: deliberate non-mapping: A §4's title says 'Analysis', but every analysis path it cites is in the top-level analysis/ tree (analysis/eval/adjud01_pairs.py, analysis/provenance/classifier.py, normalize.py); nothing in A describes concordance scoring, kappa or engine/analysis/ | — |
| `engine/analysis/report.py` | 359 | - | - | - | N·J | N·S | Q1 | A-N1: deliberate non-mapping: A §4's title says 'Analysis', but every analysis path it cites is in the top-level analysis/ tree (analysis/eval/adjud01_pairs.py, analysis/provenance/classifier.py, normalize.py); nothing in A describes concordance scoring, kappa or engine/analysis/ | — |
| `engine/analysis/scoring.py` | 259 | - | - | - | N·J | N·S | not recorded | A-N1: deliberate non-mapping: A §4's title says 'Analysis', but every analysis path it cites is in the top-level analysis/ tree (analysis/eval/adjud01_pairs.py, analysis/provenance/classifier.py, normalize.py); nothing in A describes concordance scoring, kappa or engine/analysis/ | — |
| `engine/cloud/__init__.py` | 5 | lazy | lazy | lazy | N·S | F·S | Q3 | — | F17 (path `engine/cloud/__init__.py`) |
| `engine/cloud/anthropic_extractor.py` | 260 | lazy | lazy | lazy | N·S | N·S | Q1 | — | — |
| `engine/cloud/base.py` | 517 | lazy | lazy | lazy | N·S | N·S | Q3 | — | — |
| `engine/cloud/openai_extractor.py` | 232 | lazy | lazy | lazy | N·S | N·S | Q1 | — | — |
| `engine/cloud/schema.py` | 96 | lazy | lazy | lazy | N·S | C·S | Q4 | — | named outside a finding: B:Scope and verification (dotted `engine.cloud.schema`) |
| `engine/core/__init__.py` | 0 | top | top | top | N·S | N·S | not recorded | — | — |
| `engine/core/audit_telemetry.py` | 49 | top | - | - | N·S | N·S | Q1 | — | — |
| `engine/core/citation_guard.py` | 210 | top | lazy | lazy | N·S | N·S | not recorded | — | — |
| `engine/core/codebook.py` | 482 | top | lazy | lazy | N·S | N·S | not recorded | — | — |
| `engine/core/completeness.py` | 313 | top | lazy | lazy | F·S | N·S | Q5 | A-§2.2 (path `engine/core/completeness.py`) | — |
| `engine/core/constants.py` | 10 | top | top | lazy | N·S | N·S | Q1 | — | — |
| `engine/core/corpus.py` | 83 | lazy | lazy | top | F·S | N·S | not recorded | A-§4.3 (path `engine/core/corpus.py`) | — |
| `engine/core/database.py` | 748 | top | top | lazy | C·J | F·S | Q9 | A-R2: A §2 scope, 'State Machine & Database Integrity' — 'tracks paper states across systematic review phases … using SQLite … a two-axis model … supported by an append-only event store'; scope-only (A's §2 bullets name paper_state.py, completeness.py, advance_stage.py and three migrations, and no function of this file) | F02 (path `engine/core/database.py`); F05 (path `engine/core/database.py`); F13 (path `engine/core/database.py`) |
| `engine/core/effective.py` | 599 | top | top | top | N·S | C·J | Q2 | — | B-R8: B scope sentence: '… provenance readers …' and overall assessment 'provenance-aware readers'; no finding located here |
| `engine/core/effective_config.py` | 534 | top | top | lazy | N·S | N·S | Q3 | — | — |
| `engine/core/eligibility_render.py` | 424 | top | top | lazy | N·S | N·S | not recorded | — | — |
| `engine/core/events.py` | 491 | top | top | top | C·J | C·J | Q5 | A-R2: A §2 scope, 'State Machine & Database Integrity' — 'tracks paper states across systematic review phases … using SQLite … a two-axis model … supported by an append-only event store'; scope-only (A's §2 bullets name paper_state.py, completeness.py, advance_stage.py and three migrations, and no function of this file) | B-M1: B overall assessment lists 'append-only event history' among mechanisms to preserve; no finding located here |
| `engine/core/extraction_events.py` | 529 | top | lazy | lazy | N·S | N·S | Q4 | — | — |
| `engine/core/extraction_telemetry.py` | 148 | top | lazy | lazy | N·S | N·S | not recorded | — | — |
| `engine/core/locator.py` | 102 | top | lazy | lazy | N·S | N·S | Q1 | — | — |
| `engine/core/naming.py` | 54 | lazy | lazy | lazy | N·S | N·S | not recorded | — | — |
| `engine/core/paper_state.py` | 233 | top | top | top | F·S | N·S | Q3 | A-§2.2 (path `engine/core/paper_state.py`); A-FP2 (path `engine/core/paper_state.py`) | — |
| `engine/core/parsed_text.py` | 211 | top | top | lazy | N·S | C·J | Q1 | — | named in F01 only as the consequence ('Later reads raise `ParsedTextMissing`'); the finding is located to parse_pdf. B's overall assessment lists 'hash-verified parsed text' among mechanisms to preserve [script grade was F: F01 (name `ParsedTextMissing`)] |
| `engine/core/reuse_key.py` | 55 | top | lazy | lazy | N·S | N·S | not recorded | — | — |
| `engine/core/review_paths.py` | 92 | top | lazy | lazy | N·S | N·S | not recorded | — | — |
| `engine/core/review_spec.py` | 932 | top | top | lazy | N·S | C·J | Q1 | — | B-M3: B overall assessment lists 'strict review specifications' among mechanisms to preserve; F07 reads spec.cloud.enabled_arms but locates its finding in methods_section.py |
| `engine/core/run_manifest.py` | 766 | top | top | lazy | N·S | C·J | Q3 | — | B-M2: B overall assessment lists 'arm configuration pinning' among mechanisms to preserve; no finding located here |
| `engine/core/run_telemetry.py` | 69 | top | - | - | N·S | N·S | Q2 | — | — |
| `engine/core/selection.py` | 119 | top | lazy | lazy | N·S | N·S | not recorded | — | — |
| `engine/elicitation/__init__.py` | 10 | top | lazy | lazy | C·J | N·S | not recorded | A-R3: A §3 scope, 'Elicitation & Prompt Pipelines' — 'Pass 1 elicits free-form reasoning or segmented citations, while Pass 2 extracts structured JSON'; scope-only: every path A cites in §3 and Fresh-Pass 3 is under analysis/eval/ (elicit01/manifest.py, analyze.py, units.py, schema_eval2.py), none under engine/elicitation/ | — |
| `engine/elicitation/classes.py` | 278 | top | lazy | lazy | C·J | N·S | not recorded | A-R3: A §3 scope, 'Elicitation & Prompt Pipelines' — 'Pass 1 elicits free-form reasoning or segmented citations, while Pass 2 extracts structured JSON'; scope-only: every path A cites in §3 and Fresh-Pass 3 is under analysis/eval/ (elicit01/manifest.py, analyze.py, units.py, schema_eval2.py), none under engine/elicitation/ | — |
| `engine/elicitation/contracts.py` | 458 | lazy | lazy | lazy | C·J | N·S | Q1 | A-R3: A §3 scope, 'Elicitation & Prompt Pipelines' — 'Pass 1 elicits free-form reasoning or segmented citations, while Pass 2 extracts structured JSON'; scope-only: every path A cites in §3 and Fresh-Pass 3 is under analysis/eval/ (elicit01/manifest.py, analyze.py, units.py, schema_eval2.py), none under engine/elicitation/ | — |
| `engine/elicitation/materialize.py` | 116 | lazy | lazy | lazy | C·J | N·S | Q1 | script NAME match rejected: A-§4.2 uses `source_snippet` as the name of the span field that analysis/provenance/classifier.py classifies, not the function materialize.source_snippet. C under A-R3, scope-only [script grade was F: A-§4.2 (name `source_snippet`)] | — |
| `engine/elicitation/pipeline.py` | 612 | lazy | lazy | lazy | C·J | N·S | Q3 | A-R3: A §3 scope, 'Elicitation & Prompt Pipelines' — 'Pass 1 elicits free-form reasoning or segmented citations, while Pass 2 extracts structured JSON'; scope-only: every path A cites in §3 and Fresh-Pass 3 is under analysis/eval/ (elicit01/manifest.py, analyze.py, units.py, schema_eval2.py), none under engine/elicitation/ | — |
| `engine/elicitation/prompts.py` | 378 | lazy | lazy | lazy | C·J | N·S | not recorded | A-R3: A §3 scope, 'Elicitation & Prompt Pipelines' — 'Pass 1 elicits free-form reasoning or segmented citations, while Pass 2 extracts structured JSON'; scope-only: every path A cites in §3 and Fresh-Pass 3 is under analysis/eval/ (elicit01/manifest.py, analyze.py, units.py, schema_eval2.py), none under engine/elicitation/ | — |
| `engine/elicitation/terminal.py` | 104 | lazy | lazy | lazy | C·J | N·S | not recorded | A-R3: A §3 scope, 'Elicitation & Prompt Pipelines' — 'Pass 1 elicits free-form reasoning or segmented citations, while Pass 2 extracts structured JSON'; scope-only: every path A cites in §3 and Fresh-Pass 3 is under analysis/eval/ (elicit01/manifest.py, analyze.py, units.py, schema_eval2.py), none under engine/elicitation/ | — |
| `engine/elicitation/units.py` | 132 | lazy | lazy | lazy | C·J | N·S | Q7 | ambiguous tokens `units.py` and `MIN_UNIT_TOKENS` (A-§3.3, A-FP3) each match this file and analysis/eval/elicit01/units.py; A's own sentences give the full path analysis/eval/elicit01/units.py both times, so the findings are located there. This file is the engine's port of that module (CLAUDE.md: 'units.py (ELICIT-01 index space, ported …)'). C under A-R3, scope-only | — |
| `engine/exporters/__init__.py` | 73 | top | lazy | lazy | C·J | F·S | Q1 | A-R4: A §4 scope, 'Analysis, Provenance & Exporters' — 'Systematic review outputs feed directly into PRISMA flow generation (exporters/prisma.py) and adjudication workbooks'; scope-only (A names prisma.py and corpus.py only; the two adjudicators are the units under engine/adjudication/ that import openpyxl) | F09 (path `engine/exporters/__init__.py`) |
| `engine/exporters/docx_export.py` | 194 | top | lazy | lazy | C·J | C·J | not recorded | A-R4: A §4 scope, 'Analysis, Provenance & Exporters' — 'Systematic review outputs feed directly into PRISMA flow generation (exporters/prisma.py) and adjudication workbooks'; scope-only (A names prisma.py and corpus.py only; the two adjudicators are the units under engine/adjudication/ that import openpyxl) | B-R9: B scope sentence: '… exports …'; F06, F07, F09, F15 are in this package, no finding in this file |
| `engine/exporters/evidence_table.py` | 253 | top | lazy | lazy | C·J | F·S | Q3 | A-R4: A §4 scope, 'Analysis, Provenance & Exporters' — 'Systematic review outputs feed directly into PRISMA flow generation (exporters/prisma.py) and adjudication workbooks'; scope-only (A names prisma.py and corpus.py only; the two adjudicators are the units under engine/adjudication/ that import openpyxl) | F09 (path `engine/exporters/evidence_table.py`); F15 (path `engine/exporters/evidence_table.py`) |
| `engine/exporters/methods_section.py` | 207 | top | lazy | lazy | C·J | F·S | Q1 | A-R4: A §4 scope, 'Analysis, Provenance & Exporters' — 'Systematic review outputs feed directly into PRISMA flow generation (exporters/prisma.py) and adjudication workbooks'; scope-only (A names prisma.py and corpus.py only; the two adjudicators are the units under engine/adjudication/ that import openpyxl) | F07 (path `engine/exporters/methods_section.py`) |
| `engine/exporters/prisma.py` | 411 | top | lazy | lazy | F·S | F·S | Q4 | A-§4.3 (path `exporters/prisma.py`) | F06 (path `engine/exporters/prisma.py`) |
| `engine/exporters/review_workbook.py` | 374 | lazy | lazy | lazy | C·J | C·J | Q2 | A-R4: A §4 scope, 'Analysis, Provenance & Exporters' — 'Systematic review outputs feed directly into PRISMA flow generation (exporters/prisma.py) and adjudication workbooks'; scope-only (A names prisma.py and corpus.py only; the two adjudicators are the units under engine/adjudication/ that import openpyxl) | F15 is located to evidence_table.py 'and other workbook builders accepting external strings', which B does not name; this unit imports openpyxl (AST), and whether it is one of those builders was not read here. Also C under B-R9 |
| `engine/migrations/002_screening_rename.py` | 207 | lazy | lazy | top | C·S | C·J | not recorded | directory named: A:§2 overview (`engine/migrations/`) | B-R3: B scope sentence: '… database construction/migrations …'; B also reports 'A fresh ReviewDatabase smoke check stopped at migration 014'. F02 is located to database.py and runner.py; no numbered migration is named. (adjudication/schema.py: its tables are created during ReviewDatabase construction — CLAUDE.md, the audit_adjudication row; B names no function of it) |
| `engine/migrations/003_backfill_expanded_screening.py` | 411 | lazy | lazy | top | C·S | C·J | not recorded | directory named: A:§2 overview (`engine/migrations/`) | B-R3: B scope sentence: '… database construction/migrations …'; B also reports 'A fresh ReviewDatabase smoke check stopped at migration 014'. F02 is located to database.py and runner.py; no numbered migration is named. (adjudication/schema.py: its tables are created during ReviewDatabase construction — CLAUDE.md, the audit_adjudication row; B names no function of it) |
| `engine/migrations/004_pdf_quality_check.py` | 95 | lazy | lazy | top | C·S | C·J | not recorded | directory named: A:§2 overview (`engine/migrations/`) | B-R3: B scope sentence: '… database construction/migrations …'; B also reports 'A fresh ReviewDatabase smoke check stopped at migration 014'. F02 is located to database.py and runner.py; no numbered migration is named. (adjudication/schema.py: its tables are created during ReviewDatabase construction — CLAUDE.md, the audit_adjudication row; B names no function of it) |
| `engine/migrations/005_model_digest.py` | 83 | lazy | lazy | top | C·S | C·J | not recorded | directory named: A:§2 overview (`engine/migrations/`) | B-R3: B scope sentence: '… database construction/migrations …'; B also reports 'A fresh ReviewDatabase smoke check stopped at migration 014'. F02 is located to database.py and runner.py; no numbered migration is named. (adjudication/schema.py: its tables are created during ReviewDatabase construction — CLAUDE.md, the audit_adjudication row; B names no function of it) |
| `engine/migrations/006_not_null_confidence_tier.py` | 116 | lazy | lazy | top | C·S | C·J | not recorded | directory named: A:§2 overview (`engine/migrations/`) | B-R3: B scope sentence: '… database construction/migrations …'; B also reports 'A fresh ReviewDatabase smoke check stopped at migration 014'. F02 is located to database.py and runner.py; no numbered migration is named. (adjudication/schema.py: its tables are created during ReviewDatabase construction — CLAUDE.md, the audit_adjudication row; B names no function of it) |
| `engine/migrations/007_add_judge_tables.py` | 187 | lazy | lazy | top | C·S | C·J | not recorded | directory named: A:§2 overview (`engine/migrations/`) | B-R3: B scope sentence: '… database construction/migrations …'; B also reports 'A fresh ReviewDatabase smoke check stopped at migration 014'. F02 is located to database.py and runner.py; no numbered migration is named. (adjudication/schema.py: its tables are created during ReviewDatabase construction — CLAUDE.md, the audit_adjudication row; B names no function of it) |
| `engine/migrations/008_add_fabrication_verifications.py` | 143 | lazy | lazy | top | C·S | C·J | not recorded | directory named: A:§2 overview (`engine/migrations/`) | B-R3: B scope sentence: '… database construction/migrations …'; B also reports 'A fresh ReviewDatabase smoke check stopped at migration 014'. F02 is located to database.py and runner.py; no numbered migration is named. (adjudication/schema.py: its tables are created during ReviewDatabase construction — CLAUDE.md, the audit_adjudication row; B names no function of it) |
| `engine/migrations/009_add_backfill_audit_log.py` | 180 | lazy | lazy | top | C·S | C·J | not recorded | directory named: A:§2 overview (`engine/migrations/`) | B-R3: B scope sentence: '… database construction/migrations …'; B also reports 'A fresh ReviewDatabase smoke check stopped at migration 014'. F02 is located to database.py and runner.py; no numbered migration is named. (adjudication/schema.py: its tables are created during ReviewDatabase construction — CLAUDE.md, the audit_adjudication row; B names no function of it) |
| `engine/migrations/010_add_provenance_classifications.py` | 168 | lazy | lazy | top | C·S | C·J | not recorded | directory named: A:§2 overview (`engine/migrations/`) | B-R3: B scope sentence: '… database construction/migrations …'; B also reports 'A fresh ReviewDatabase smoke check stopped at migration 014'. F02 is located to database.py and runner.py; no numbered migration is named. (adjudication/schema.py: its tables are created during ReviewDatabase construction — CLAUDE.md, the audit_adjudication row; B names no function of it) |
| `engine/migrations/011_add_absence_claim_class.py` | 205 | lazy | lazy | top | C·S | C·J | not recorded | directory named: A:§2 overview (`engine/migrations/`) | B-R3: B scope sentence: '… database construction/migrations …'; B also reports 'A fresh ReviewDatabase smoke check stopped at migration 014'. F02 is located to database.py and runner.py; no numbered migration is named. (adjudication/schema.py: its tables are created during ReviewDatabase construction — CLAUDE.md, the audit_adjudication row; B names no function of it) |
| `engine/migrations/012_codebook_provenance.py` | 98 | lazy | lazy | top | C·S | C·J | not recorded | directory named: A:§2 overview (`engine/migrations/`) | B-R3: B scope sentence: '… database construction/migrations …'; B also reports 'A fresh ReviewDatabase smoke check stopped at migration 014'. F02 is located to database.py and runner.py; no numbered migration is named. (adjudication/schema.py: its tables are created during ReviewDatabase construction — CLAUDE.md, the audit_adjudication row; B names no function of it) |
| `engine/migrations/013_drop_schema_hash_not_null.py` | 148 | lazy | lazy | top | C·S | C·J | not recorded | directory named: A:§2 overview (`engine/migrations/`) | B-R3: B scope sentence: '… database construction/migrations …'; B also reports 'A fresh ReviewDatabase smoke check stopped at migration 014'. F02 is located to database.py and runner.py; no numbered migration is named. (adjudication/schema.py: its tables are created during ReviewDatabase construction — CLAUDE.md, the audit_adjudication row; B names no function of it) |
| `engine/migrations/014_cloud_tables.py` | 60 | lazy | lazy | top | C·S | C·J | Q1 | directory named: A:§2 overview (`engine/migrations/`) | B-R3: B scope sentence: '… database construction/migrations …'; B also reports 'A fresh ReviewDatabase smoke check stopped at migration 014'. F02 is located to database.py and runner.py; no numbered migration is named. (adjudication/schema.py: its tables are created during ReviewDatabase construction — CLAUDE.md, the audit_adjudication row; B names no function of it) |
| `engine/migrations/015_drop_prerename_adjudication_indices.py` | 78 | lazy | lazy | top | C·J | C·J | not recorded | named in A-§2.3 as background ('While migration 015… cleans up legacy indices'); the finding (PRAGMA foreign_keys on every connection) is located to no file [script grade was F: A-§2.3 (path `015_drop_prerename_adjudication_indices.py`)] | B-R3: B scope sentence: '… database construction/migrations …'; B also reports 'A fresh ReviewDatabase smoke check stopped at migration 014'. F02 is located to database.py and runner.py; no numbered migration is named. (adjudication/schema.py: its tables are created during ReviewDatabase construction — CLAUDE.md, the audit_adjudication row; B names no function of it) |
| `engine/migrations/016_event_store.py` | 263 | lazy | lazy | top | C·J | C·J | not recorded | named in A-§2.2 as background ('The migration … to an append-only event store (016…, 017…) ensures PRISMA auditability'); the finding is located to paper_state.py and completeness.py [script grade was F: A-§2.2 (path `016_event_store.py`)] | B-R3: B scope sentence: '… database construction/migrations …'; B also reports 'A fresh ReviewDatabase smoke check stopped at migration 014'. F02 is located to database.py and runner.py; no numbered migration is named. (adjudication/schema.py: its tables are created during ReviewDatabase construction — CLAUDE.md, the audit_adjudication row; B names no function of it) |
| `engine/migrations/017_seed_event_store.py` | 205 | lazy | lazy | top | C·J | C·J | not recorded | named in A-§2.2 as background, as 016; the finding is located to paper_state.py and completeness.py [script grade was F: A-§2.2 (path `017_seed_event_store.py`)] | B-R3: B scope sentence: '… database construction/migrations …'; B also reports 'A fresh ReviewDatabase smoke check stopped at migration 014'. F02 is located to database.py and runner.py; no numbered migration is named. (adjudication/schema.py: its tables are created during ReviewDatabase construction — CLAUDE.md, the audit_adjudication row; B names no function of it) |
| `engine/migrations/018_cloud_shape_and_audit_adjudication.py` | 231 | lazy | lazy | top | C·S | C·J | Q1 | directory named: A:§2 overview (`engine/migrations/`) | B-R3: B scope sentence: '… database construction/migrations …'; B also reports 'A fresh ReviewDatabase smoke check stopped at migration 014'. F02 is located to database.py and runner.py; no numbered migration is named. (adjudication/schema.py: its tables are created during ReviewDatabase construction — CLAUDE.md, the audit_adjudication row; B names no function of it) |
| `engine/migrations/019_paper_state_axes.py` | 304 | lazy | lazy | top | C·S | C·J | not recorded | named outside a finding: A:§2 overview (path `019_paper_state_axes.py`) | B-R3: B scope sentence: '… database construction/migrations …'; B also reports 'A fresh ReviewDatabase smoke check stopped at migration 014'. F02 is located to database.py and runner.py; no numbered migration is named. (adjudication/schema.py: its tables are created during ReviewDatabase construction — CLAUDE.md, the audit_adjudication row; B names no function of it) |
| `engine/migrations/020_run_manifest.py` | 496 | lazy | lazy | top | C·S | C·J | not recorded | directory named: A:§2 overview (`engine/migrations/`) | B-R3: B scope sentence: '… database construction/migrations …'; B also reports 'A fresh ReviewDatabase smoke check stopped at migration 014'. F02 is located to database.py and runner.py; no numbered migration is named. (adjudication/schema.py: its tables are created during ReviewDatabase construction — CLAUDE.md, the audit_adjudication row; B names no function of it) |
| `engine/migrations/021_parsed_text_sha256.py` | 279 | lazy | lazy | top | C·S | C·J | not recorded | directory named: A:§2 overview (`engine/migrations/`) | B-R3: B scope sentence: '… database construction/migrations …'; B also reports 'A fresh ReviewDatabase smoke check stopped at migration 014'. F02 is located to database.py and runner.py; no numbered migration is named. (adjudication/schema.py: its tables are created during ReviewDatabase construction — CLAUDE.md, the audit_adjudication row; B names no function of it) |
| `engine/migrations/022_run_kinds_and_audit_tables.py` | 590 | lazy | lazy | top | C·S | C·J | not recorded | directory named: A:§2 overview (`engine/migrations/`) | B-R3: B scope sentence: '… database construction/migrations …'; B also reports 'A fresh ReviewDatabase smoke check stopped at migration 014'. F02 is located to database.py and runner.py; no numbered migration is named. (adjudication/schema.py: its tables are created during ReviewDatabase construction — CLAUDE.md, the audit_adjudication row; B names no function of it) |
| `engine/migrations/__init__.py` | 0 | lazy | lazy | top | C·S | C·J | not recorded | directory named: A:§2 overview (`engine/migrations/`) | B-R3: B scope sentence: '… database construction/migrations …'; B also reports 'A fresh ReviewDatabase smoke check stopped at migration 014'. F02 is located to database.py and runner.py; no numbered migration is named. (adjudication/schema.py: its tables are created during ReviewDatabase construction — CLAUDE.md, the audit_adjudication row; B names no function of it) |
| `engine/migrations/__main__.py` | 57 | - | - | top | C·S | C·J | not recorded | directory named: A:§2 overview (`engine/migrations/`) | B-R3: B scope sentence: '… database construction/migrations …'; B also reports 'A fresh ReviewDatabase smoke check stopped at migration 014'. F02 is located to database.py and runner.py; no numbered migration is named. (adjudication/schema.py: its tables are created during ReviewDatabase construction — CLAUDE.md, the audit_adjudication row; B names no function of it) |
| `engine/migrations/runner.py` | 360 | lazy | lazy | top | C·S | F·S | Q5 | directory named: A:§2 overview (`engine/migrations/`) | F02 (path `engine/migrations/runner.py`) |
| `engine/parsers/__init__.py` | 0 | top | lazy | lazy | N·S | N·J | not recorded | — | B-N1: deliberate non-mapping: B's area is 'parsing publication' (the write of parsed text), which F01 locates to parse_pdf; nothing in B concerns parse quality, the font audit, markers or the parser models |
| `engine/parsers/font_audit.py` | 719 | top | lazy | lazy | N·S | N·J | Q2 | — | B-N1: deliberate non-mapping: B's area is 'parsing publication' (the write of parsed text), which F01 locates to parse_pdf; nothing in B concerns parse quality, the font audit, markers or the parser models |
| `engine/parsers/markers.py` | 41 | top | lazy | lazy | N·S | N·J | Q1 | — | B-N1: deliberate non-mapping: B's area is 'parsing publication' (the write of parsed text), which F01 locates to parse_pdf; nothing in B concerns parse quality, the font audit, markers or the parser models |
| `engine/parsers/models.py` | 57 | top | lazy | lazy | N·S | N·J | not recorded | — | B-N1: deliberate non-mapping: B's area is 'parsing publication' (the write of parsed text), which F01 locates to parse_pdf; nothing in B concerns parse quality, the font audit, markers or the parser models |
| `engine/parsers/parse_quality.py` | 364 | top | lazy | lazy | N·S | N·J | Q1 | — | B-N1: deliberate non-mapping: B's area is 'parsing publication' (the write of parsed text), which F01 locates to parse_pdf; nothing in B concerns parse quality, the font audit, markers or the parser models |
| `engine/parsers/pdf_parser.py` | 1295 | top | lazy | lazy | N·S | F·S | Q7 | — | F01 (path `engine/parsers/pdf_parser.py`) |
| `engine/search/__init__.py` | 0 | top | top | lazy | N·S | C·J | not recorded | — | B-R1: B scope sentence: 'Reviewed active search, deduplication …'; findings F04 (dedup.py) and F08 (openalex.py) are in this package, no finding in this file |
| `engine/search/dedup.py` | 200 | top | - | - | N·S | F·S | Q2 | — | F04 (path `engine/search/dedup.py`) |
| `engine/search/models.py` | 19 | top | top | lazy | N·S | C·J | not recorded | — | B-R1: B scope sentence: 'Reviewed active search, deduplication …'; findings F04 (dedup.py) and F08 (openalex.py) are in this package, no finding in this file |
| `engine/search/openalex.py` | 148 | top | - | - | N·S | F·S | Q3 | — | F08 (path `engine/search/openalex.py`) |
| `engine/search/pubmed.py` | 180 | top | - | - | N·S | C·J | not recorded | — | B-R1: B scope sentence: 'Reviewed active search, deduplication …'; findings F04 (dedup.py) and F08 (openalex.py) are in this package, no finding in this file |
| `engine/tools/__init__.py` | 1 | - | - | - | N·S | N·S | not recorded | — | — |
| `engine/tools/db_fingerprint.py` | 451 | - | - | - | N·S | F·S | Q1 | — | F16 (path `engine/tools/db_fingerprint.py`) |
| `engine/tools/inventory.py` | 757 | - | - | - | N·S | N·S | not recorded | — | — |
| `engine/utils/__init__.py` | 0 | top | top | lazy | N·S | N·S | not recorded | — | — |
| `engine/utils/background.py` | 66 | lazy | lazy | lazy | N·S | N·S | not recorded | — | — |
| `engine/utils/db_backup.py` | 277 | - | - | - | N·S | F·S | Q6 | — | F03 (path `engine/utils/db_backup.py`) |
| `engine/utils/ollama_client.py` | 739 | top | top | lazy | F·J | F·S | Q8 | F stands on A-§1.1 and A-T1 (the module-level `_client`). The A-§3.1 `n_ctx_train` name match is incidental: A attributes that figure to analysis/eval/elicit01/manifest.py [script: A-§1.1 (dotted `engine.utils.ollama_client._client`); A-§3.1 (name `n_ctx_train`); A-T1 (path `ollama_client.py`)] | F14 (path `engine/utils/ollama_client.py`) |
| `engine/utils/ollama_lock.py` | 191 | top | top | lazy | F·S | C·J | Q2 | A-§1.2 (path `engine/utils/ollama_lock.py`) | B-R7: B scope sentence: '… local model calls …'; F14 says 'Existing experiment locks and restart guards are valuable' without locating a finding here |
| `engine/utils/ollama_preflight.py` | 303 | lazy | lazy | lazy | C·J | C·J | Q2 | A-R1: A §1 scope, 'Local Compute Concurrency & Workstation Execution' — 'The engine runs local inference on large models … via Ollama'; scope-only for this file (A names ollama_lock.py and ollama_client._client, not the preflight) | B-R7: B scope sentence: '… local model calls …'; F14 says 'Existing experiment locks and restart guards are valuable' without locating a finding here |
| `engine/utils/progress.py` | 96 | lazy | lazy | lazy | N·S | N·S | not recorded | — | — |
| `engine/validators/__init__.py` | 1 | top | lazy | lazy | N·S | N·S | not recorded | — | — |
| `engine/validators/distribution_monitor.py` | 547 | top | lazy | lazy | N·S | N·S | H1 | — | — |
| `engine/validators/extraction_validator.py` | 347 | - | - | - | N·S | N·S | not recorded | — | — |
| `scripts/_pass2_delta.py` | 287 | - | - | - | N·S | N·S | not recorded | — | — |
| `scripts/_pass2_eyeball.py` | 142 | - | - | - | N·S | N·S | not recorded | — | — |
| `scripts/_pass2_stability.py` | 102 | - | - | - | N·S | N·S | Q1 | — | — |
| `scripts/advance_to_pdf_acquired.py` | 113 | - | - | - | F·S | N·S | Q5 | A-FP2 (path `scripts/advance_to_pdf_acquired.py`) | — |
| `scripts/backfill_authors.py` | 209 | - | - | - | N·S | N·S | Q1 | — | — |
| `scripts/backfill_cloud_spans.py` | 120 | - | - | - | N·S | N·S | Q1 | — | — |
| `scripts/ft_screening_smoke_test.py` | 254 | - | - | - | N·S | N·S | not recorded | — | — |
| `scripts/parse_expanded_corpus.py` | 95 | - | - | - | N·S | N·S | H1 | — | — |
| `scripts/pdf_acquisition/step1_export_citations.py` | 82 | - | - | - | N·S | N·S | Q1 | — | — |
| `scripts/pdf_acquisition/step2_unpaywall_check.py` | 225 | - | - | - | N·S | N·S | Q1 | — | — |
| `scripts/pdf_acquisition/step3_download_oa_pdfs.py` | 165 | - | - | - | N·S | N·S | Q1 | — | — |
| `scripts/pdf_acquisition/step3b_retry_failed.py` | 373 | - | - | - | N·S | N·S | not recorded | — | — |
| `scripts/pdf_acquisition/step4_manual_download_list.py` | 254 | - | - | - | N·S | N·S | not recorded | — | — |
| `scripts/prepare_concordance_pdfs.py` | 87 | - | - | - | N·S | N·S | not recorded | — | — |
| `scripts/q8_validation.py` | 251 | - | - | - | N·S | N·S | not recorded | — | — |
| `scripts/q8_validation_fast.py` | 162 | - | - | - | N·S | N·S | not recorded | — | — |
| `scripts/reparse_cloud_spans.py` | 119 | - | - | - | N·S | N·S | Q1 | — | — |
| `scripts/rescreen_original_251.py` | 181 | - | - | - | N·S | N·J | Q1 | — | B-N2: deliberate non-mapping: B's area 'screening' is carried by F11 (ft_screener.py), F12 (run_pipeline._stage_screen) and F13 (screener.py); B never mentions adjudication or the standalone screening scripts |
| `scripts/rescreen_with_specialty.py` | 407 | - | - | - | N·S | N·J | Q1 | — | B-N2: deliberate non-mapping: B's area 'screening' is carried by F11 (ft_screener.py), F12 (run_pipeline._stage_screen) and F13 (screener.py); B never mentions adjudication or the standalone screening scripts |
| `scripts/run_cloud_extraction.py` | 211 | - | - | - | N·S | N·S | Q1 | — | — |
| `scripts/run_pipeline.py` | 618 | top | - | - | N·S | F·S | Q9 | — | F05 (path `scripts/run_pipeline.py`); F06 (path `scripts/run_pipeline.py`); F12 (path `scripts/run_pipeline.py`) |
| `scripts/screen_expanded.py` | 540 | - | - | - | N·S | N·J | Q4 | — | B-N2: deliberate non-mapping: B's area 'screening' is carried by F11 (ft_screener.py), F12 (run_pipeline._stage_screen) and F13 (screener.py); B never mentions adjudication or the standalone screening scripts |
| `scripts/smoke_test_fixes.py` | 200 | - | - | - | N·S | N·S | not recorded | — | — |
| `scripts/test_e2e_search_screen.py` | 164 | - | - | - | N·S | N·S | Q1 | — | — |
| `scripts/test_extraction_validation.py` | 361 | - | - | - | N·S | N·S | not recorded | — | — |

## 3d — Papers 4, 23, 168 (live, `mode=ro`; counts only)

Script: `12i_scope/papers_4_23_168.py`; output `papers_4_23_168.json`.
(`PYTHONPATH=. .venv/bin/python docs/session-reports/session-12/12i_scope/papers_4_23_168.py data/surgical_autonomy/review.db`)

**I4 — confirmed:** 4, 23 and 168 are `papers.id` values on live.

| | paper 4 | paper 23 | paper 168 |
| --- | --- | --- | --- |
| `papers.status` | `ABSTRACT_SCREENED_OUT` | `ABSTRACT_SCREENED_OUT` | `ABSTRACT_SCREENED_OUT` |
| `papers.source` | pubmed | openalex | openalex |
| abstract decision rows, pass 1 | 2 (1 `include`, 1 `exclude`) | 2 (1 `include`, 1 `exclude`) | 2 (1 `include`, 1 `exclude`) |
| abstract decision rows, pass 2 | 2 (1 `include`, 1 `exclude`) | 2 (1 `include`, 1 `exclude`) | 2 (1 `include`, 1 `exclude`) |
| abstract decision rows by model | 4 `qwen3:8b`; first 2026-02-28, last 2026-03-13 | 4 `qwen3:8b`; first 2026-02-28, last 2026-03-13 | 4 `qwen3:8b`; first 2026-02-28, last 2026-03-13 |
| abstract verification rows | 0 | 0 | 0 |
| abstract adjudication rows | 0 | 0 | 0 |
| FT decision rows, primary (`ft_screening_decisions`) | 1 (`FT_EXCLUDE`) | 1 (`FT_EXCLUDE`) | 1 (`FT_ELIGIBLE`) |
| FT decision rows, verifier (`ft_verification_decisions`) | 0 | 0 | 1 (`FT_ELIGIBLE`) |
| FT adjudication rows | 0 | 0 | 0 |
| paper events, eligibility axis | 0 | 0 | 0 |
| paper events, processing axis | 0 | 0 | 0 |
| `parsed_text_refs` rows | 0 | 0 | 0 |
| `full_text_assets` rows | 1 | 1 | 1 |
| `parse_attempts` rows | 0 | 0 | 0 |

**EXPECTED (R549) — met for all three:** each is `ABSTRACT_SCREENED_OUT` with at least one FT
decision row. The same script lists every `ABSTRACT_SCREENED_OUT` paper that has any FT decision
row: `[4, 23, 168]` — these three and no other. No interpretation is offered here.

## 3e — PROPOSED lots (nothing started)

**Everything in this section is PROPOSED.** Script: `12i_scope/lots.py` (outputs `lots.json`,
`lots.md`). The lot definitions and the reader counts are CC's proposal; the script checks that
every unit with no located finding is in exactly one lot and that no F unit is in any, and computes
the columns.

**In scope for the lots:** the 124 units whose higher grade across A and B is N or C (31,290
lines). **Not in any lot:** the 25 units with a located finding (9,442 lines) — see the question
below.

**The size reference.** `12f_triage.md` describes the 12f read as "eight parallel read-only
readers (five for B, two for A, two halves of the C56 census)" — the parenthesis adds up to nine;
which figure is right is not established here. Those readers each verified a few named findings
or swept one half of the tree for broad handlers; none read a package end to end. A whole-file
review read is heavier per line than either. The estimate below is CC's: one reader per lot of
roughly 500 to 2,700 lines of live code, two for the two lots near or above 3,000 lines of
hand-read code (L09, L17), one for the migrations lot because its text is frozen. **19 readers in all,
against 12f's eight; 12 of them on lots that are wholly on the Run 7 import graph (L01–L07, L09,
L11–L13).**

| lot | PROPOSED content | units (N / C) | lines | Run 7 share of lines (top + lazy) | 12F not recorded | readers (est.) |
| --- | --- | ---: | ---: | --- | ---: | ---: |
| L01 | Extraction write path: claims as events, guards, selection, locator, telemetry | 9 (8 / 1) | 1,772 | 100% (1,772 top + 0 lazy) | 4 | 1 |
| L02 | Elicitation (the Run 7 extraction design) | 8 (0 / 8) | 2,088 | 100% (288 top + 1,800 lazy) | 4 | 1 |
| L03 | Audit and the distribution gate | 4 (4 / 0) | 1,057 | 100% (1,057 top + 0 lazy) | 1 | 1 |
| L04 | Resolver and run manifest | 2 (1 / 1) | 1,300 | 100% (1,300 top + 0 lazy) | 0 | 1 |
| L05 | Spec, codebook, review identity, eligibility rendering | 5 (4 / 1) | 1,940 | 100% (1,940 top + 0 lazy) | 3 | 1 |
| L06 | Readers: the one reader, the parsed-text resolver, naming | 3 (1 / 2) | 864 | 100% (810 top + 54 lazy) | 1 | 1 |
| L07 | Parser support: font audit, parse-quality verdict, markers, models | 4 (4 / 0) | 1,181 | 100% (1,181 top + 0 lazy) | 1 | 1 |
| L08 | Workflow, adjudication schema and the two entry importers | 5 (3 / 2) | 1,023 | 49% (501 top + 0 lazy) | 2 | 1 |
| L09 | Human screening adjudication: adjudicators, HTML generators, categorizer | 5 (3 / 2) | 3,152 | 100% (1,911 top + 1,241 lazy) | 2 | 2 |
| L10 | Front half remainder: PubMed client, search models, acquisition except download.py | 9 (0 / 9) | 2,644 | 55% (199 top + 1,247 lazy) | 5 | 1 |
| L11 | Local-model utilities and run support: preflight, tmux background, progress | 3 (2 / 1) | 465 | 100% (0 top + 465 lazy) | 2 | 1 |
| L12 | Cloud arms (R71: no cloud extraction yet) | 4 (3 / 1) | 1,105 | 100% (0 top + 1,105 lazy) | 0 | 1 |
| L13 | Export remainder: DOCX and the shared review workbook | 2 (0 / 2) | 568 | 100% (194 top + 374 lazy) | 1 | 1 |
| L14 | Concordance: engine/analysis | 6 (6 / 0) | 1,431 | 0% (0 top + 0 lazy) | 4 | 1 |
| L15 | Numbered migrations and the migrations CLI (applied text is frozen by receipt) | 23 (0 / 23) | 4,604 | 99% (0 top + 4,547 lazy) | 21 | 1 |
| L16 | Developer tools and the read-only validator | 3 (3 / 0) | 1,105 | 0% (0 top + 0 lazy) | 3 | 1 |
| L17 | Scripts off the Run 7 import graph | 23 (23 / 0) | 4,991 | 0% (0 top + 0 lazy) | 10 | 2 |
| L18 | Empty package markers (0 lines each; nothing to read) | 6 (5 / 1) | 0 | — | 6 | 0 |
| all | | 124 | 31,290 | | | 19 |

- **L01** — `engine/core/audit_telemetry.py` (49), `engine/core/citation_guard.py` (210), `engine/core/events.py` (491), `engine/core/extraction_events.py` (529), `engine/core/extraction_telemetry.py` (148), `engine/core/locator.py` (102), `engine/core/reuse_key.py` (55), `engine/core/run_telemetry.py` (69), `engine/core/selection.py` (119)
- **L02** — `engine/elicitation/__init__.py` (10), `engine/elicitation/classes.py` (278), `engine/elicitation/contracts.py` (458), `engine/elicitation/materialize.py` (116), `engine/elicitation/pipeline.py` (612), `engine/elicitation/prompts.py` (378), `engine/elicitation/terminal.py` (104), `engine/elicitation/units.py` (132)
- **L03** — `engine/agents/audit_events.py` (250), `engine/agents/auditor.py` (259), `engine/validators/__init__.py` (1), `engine/validators/distribution_monitor.py` (547)
- **L04** — `engine/core/effective_config.py` (534), `engine/core/run_manifest.py` (766)
- **L05** — `engine/core/codebook.py` (482), `engine/core/constants.py` (10), `engine/core/eligibility_render.py` (424), `engine/core/review_paths.py` (92), `engine/core/review_spec.py` (932)
- **L06** — `engine/core/effective.py` (599), `engine/core/naming.py` (54), `engine/core/parsed_text.py` (211)
- **L07** — `engine/parsers/font_audit.py` (719), `engine/parsers/markers.py` (41), `engine/parsers/models.py` (57), `engine/parsers/parse_quality.py` (364)
- **L08** — `engine/adjudication/__init__.py` (43), `engine/adjudication/import_extraction_entry.py` (333), `engine/adjudication/import_screening_entry.py` (189), `engine/adjudication/schema.py` (78), `engine/adjudication/workflow.py` (380)
- **L09** — `engine/adjudication/abstract_adjudication_html.py` (708), `engine/adjudication/categorizer.py` (259), `engine/adjudication/ft_adjudication_html.py` (533), `engine/adjudication/ft_screening_adjudicator.py` (753), `engine/adjudication/screening_adjudicator.py` (899)
- **L10** — `engine/acquisition/__init__.py` (29), `engine/acquisition/check_oa.py` (197), `engine/acquisition/manual_list.py` (448), `engine/acquisition/pdf_quality_check.py` (326), `engine/acquisition/pdf_quality_html.py` (750), `engine/acquisition/pdf_quality_import.py` (316), `engine/acquisition/verify_downloads.py` (379), `engine/search/models.py` (19), `engine/search/pubmed.py` (180)
- **L11** — `engine/utils/background.py` (66), `engine/utils/ollama_preflight.py` (303), `engine/utils/progress.py` (96)
- **L12** — `engine/cloud/anthropic_extractor.py` (260), `engine/cloud/base.py` (517), `engine/cloud/openai_extractor.py` (232), `engine/cloud/schema.py` (96)
- **L13** — `engine/exporters/docx_export.py` (194), `engine/exporters/review_workbook.py` (374)
- **L14** — `engine/analysis/__init__.py` (0), `engine/analysis/concordance.py` (399), `engine/analysis/metrics.py` (233), `engine/analysis/normalize.py` (181), `engine/analysis/report.py` (359), `engine/analysis/scoring.py` (259)
- **L15** — `engine/migrations/002_screening_rename.py` (207), `engine/migrations/003_backfill_expanded_screening.py` (411), `engine/migrations/004_pdf_quality_check.py` (95), `engine/migrations/005_model_digest.py` (83), `engine/migrations/006_not_null_confidence_tier.py` (116), `engine/migrations/007_add_judge_tables.py` (187), `engine/migrations/008_add_fabrication_verifications.py` (143), `engine/migrations/009_add_backfill_audit_log.py` (180), `engine/migrations/010_add_provenance_classifications.py` (168), `engine/migrations/011_add_absence_claim_class.py` (205), `engine/migrations/012_codebook_provenance.py` (98), `engine/migrations/013_drop_schema_hash_not_null.py` (148), `engine/migrations/014_cloud_tables.py` (60), `engine/migrations/015_drop_prerename_adjudication_indices.py` (78), `engine/migrations/016_event_store.py` (263), `engine/migrations/017_seed_event_store.py` (205), `engine/migrations/018_cloud_shape_and_audit_adjudication.py` (231), `engine/migrations/019_paper_state_axes.py` (304), `engine/migrations/020_run_manifest.py` (496), `engine/migrations/021_parsed_text_sha256.py` (279), `engine/migrations/022_run_kinds_and_audit_tables.py` (590), `engine/migrations/__init__.py` (0), `engine/migrations/__main__.py` (57)
- **L16** — `engine/tools/__init__.py` (1), `engine/tools/inventory.py` (757), `engine/validators/extraction_validator.py` (347)
- **L17** — `scripts/_pass2_delta.py` (287), `scripts/_pass2_eyeball.py` (142), `scripts/_pass2_stability.py` (102), `scripts/backfill_authors.py` (209), `scripts/backfill_cloud_spans.py` (120), `scripts/ft_screening_smoke_test.py` (254), `scripts/parse_expanded_corpus.py` (95), `scripts/pdf_acquisition/step1_export_citations.py` (82), `scripts/pdf_acquisition/step2_unpaywall_check.py` (225), `scripts/pdf_acquisition/step3_download_oa_pdfs.py` (165), `scripts/pdf_acquisition/step3b_retry_failed.py` (373), `scripts/pdf_acquisition/step4_manual_download_list.py` (254), `scripts/prepare_concordance_pdfs.py` (87), `scripts/q8_validation.py` (251), `scripts/q8_validation_fast.py` (162), `scripts/reparse_cloud_spans.py` (119), `scripts/rescreen_original_251.py` (181), `scripts/rescreen_with_specialty.py` (407), `scripts/run_cloud_extraction.py` (211), `scripts/screen_expanded.py` (540), `scripts/smoke_test_fixes.py` (200), `scripts/test_e2e_search_screen.py` (164), `scripts/test_extraction_validation.py` (361)
- **L18** — `engine/__init__.py` (0), `engine/agents/__init__.py` (0), `engine/core/__init__.py` (0), `engine/parsers/__init__.py` (0), `engine/search/__init__.py` (0), `engine/utils/__init__.py` (0)

**Questions the lots raise for the ruling** (CC's, not decided here):

1. **Do the 25 F units stay out?** A located finding covers the function it names. On the Run 7
   import graph the F units are 21 (8,500 lines), among them `engine/parsers/pdf_parser.py`
   (1,295 lines; B's F01 names `parse_pdf`), `engine/agents/extractor.py` (1,077; A names
   `restart_ollama` only, B does not mention the file), `engine/core/database.py` (748),
   `engine/utils/ollama_client.py` (739), `engine/agents/ft_screener.py` (682) and
   `scripts/run_pipeline.py` (618). 12f quoted each of these (12F `Q6`–`Q9`), which again is not a
   whole-file read. If they join the review they are two to four further lots.
2. **Is "C" enough to leave a unit out?** 67 of the 89 C cells are CC's area mapping. If only an
   *unmentioned* unit needs the review, the scope is the 70 N units (15,878 lines; 36 units and
   7,829 lines on the Run 7 graph); if a unit with no located finding needs it, the scope is all
   124.
3. **L15 (migrations):** 21 of the 23 units are applied migrations whose text is checksummed by
   receipt, so a finding there can only become a new migration; and the migration session (P6)
   comes later. The lot may be better read then.
4. **L17 (scripts) and L16:** none is importable from the Run 7 runner. Several scripts read, by
   their docstrings, as one-off runners (`rescreen_*`, `_pass2_*`, `scripts/pdf_acquisition/step*`); R31's
   retention test may dispose of some before any review read is spent on them.
5. **`lazy` is an upper bound.** L12 (cloud) and most of L10 (acquisition) are on the graph only
   through in-function imports; R71 keeps cloud extraction off.
6. **Papers 4, 23, 168** are on the review's list by R549 as a data question, not a code lot.
   Which code wrote their rows was not read here; the two screening agents (`screener.py`,
   `ft_screener.py`) are F units and so outside the lots as proposed.

## Adjacent items seen while reading (R509) — recorded, not chased; no row opened

| # | what | proposed class | proposed package |
| --- | --- | --- | --- |
| 1 | `engine/acquisition/manual_list.py` (448 lines) opens with "DEPRECATED — Use pdf_quality_html.py --mode acquisition instead … will be removed in a future version". None of the entry points used in §3b reaches it by import (it has its own `python -m` entry); `tests/test_verify_downloads.py` imports `classify_publisher` from it, and `engine/adjudication/workflow.py` still prints `python -m engine.acquisition.manual_list --review <name>` in an operator message. The plan has no row naming it (`grep -c manual_list` on the plan: 0). An R31 retention question. | 3 | P7 |
| 2 | `12f_triage.md` says "eight parallel read-only readers" and lists five + two + two. A committed read-out's wording; not an engine defect. | 3 (docs) | none proposed — a dated addendum to that file, if wanted |

Not new, recorded so it is not re-found: the three module-level engine imports of
`analysis.provenance.segment` are row A-5 (R530).

## The brief's INFERRED items

| | result |
| --- | --- |
| I1 | **Confirmed** (§3a). |
| I2 | **Not contradicted; the plan does not use the phrase.** `docs/plan/ENGINE_REFACTOR_PLAN.md` has no sentence describing the internal review's scope as "packages" of any kind: its statements are "the internal review (R562 as amended by R563), whose list includes papers 4, 23 and 168 (R549)" (12g and 12h closures, 12i-open block) and R540's "12g runtime discovery (rehearsals, internal review, the R516 and R536 measurements) first". Wherever the plan's 12f–12h closures and R540–R576 say "package" they mean R540's work packages P1–P7 ("then the seven packages in order"; "with a proposed class and package"). Nothing in the plan says the internal review covers P1–P7, and the brief's quoted phrase — packages an outside assessment "read" — cannot describe P1–P7, which were defined on 2026-10-08, after both assessments. The STOP condition ("If it means P1–P7") was therefore not met and this read-out proceeded on the brief's reading: engine code modules. **The architect should confirm the reading against the unified plan, which CC cannot see.** |
| I3 | **Confirmed.** `~/claude-session-archive/d6e1b3cf-b2ae-47b7-8999-826f58d68ec4.jsonl` is readable; its closeout block carries the lane-line proposal. |
| I4 | **Confirmed** (§3d). |
| I5 | **Confirmed in part.** `12f_triage_rows.md` records the code each row *quotes*, by path and function; neither it nor `READER_RULES.md` records which files the readers read. The 12F column carries what is recorded (Q / H) and reads "not recorded" otherwise; nothing was reconstructed. |
| I6 | **Not recorded — unknown** (§3a). |
