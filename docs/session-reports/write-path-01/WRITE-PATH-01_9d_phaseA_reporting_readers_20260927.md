# WRITE-PATH-01 9d Phase A — the three reporting readers (read-out)

Session 9d, sub-brief 9d-P0/PA. Read-only on every engine, script and test file. HEAD at read:
`2f2525afb35f592711964aedd74b24418d6efc7c`. Every classification below is READ — the site and the
code it calls were read at HEAD. Anchors are quoted content, never line numbers. Test-id counts
come from `pytest --collect-only -q` at HEAD (appendix A), never from grep. Live `review.db` was
opened `mode=ro` only; no `ReviewDatabase` was constructed on live; no model call.

Part 0 (startup verify) matched every expected value M1–M7; the table is in the session's final
report, not repeated here.

---

## §0 Ledger results (I1–I6)

| item | verdict | evidence |
|---|---|---|
| **I1** `normalize_categorical_values` gone; validator file collects < 24 | **holds** | `grep -rn normalize_categorical_values engine tests scripts analysis` → no hit. `tests/test_extraction_validator.py` collects **20** ids (appendix A), and the file carries the note "The four in-place categorical normalisation tests retired 2026-09-25 with their subject (9c-C5, R160a; R47)." 24 − 4 = 20; no other R160a retirement touched this file. |
| **I2** `run_stage_configs` has run id, stage, model identity; `run_calls` has run id, stage, `paper_id` | **holds, with two corrections** | DDL quoted in §2b. The model column is **`model_name`** (not `model`), with `model_digest` beside it and `stage_kind` carrying the resolver stage. `run_calls.paper_id` exists but is **nullable** (`paper_id INTEGER REFERENCES papers(id)`), and the **audit stage never fills it**: `auditor.semantic_verify` calls `ollama_chat(messages=build_audit_messages(span, field_type=field_type), **cfg.kwargs())` with no `paper_id`, so every audit `run_calls` row would carry `paper_id` NULL (§2c). |
| **I3** no arm in the validator; `iter_grid` requires `arms=` | **false in part** | The validator half holds: no arm anywhere in `extraction_validator.py`. The `iter_grid` half is false: its signature is `def iter_grid(conn, *, codebook, sentinels=None, papers=None, arms=None)`, and `grid_cells` does `arm_names = tuple(arms) if arms is not None else registered_arms(conn)`. Omitting `arms=` enumerates **every registered, non-retired arm** — on live today the three pre-manifest Run-6 arms. Where an arm would come from: §3c. |
| **I4** every non-status count is consumed only by `validate_prisma_counts` and/or a test | **false** | (a) `spans_verified` / `spans_flagged` are read by **another engine site**: `methods_section.generate_methods_section` renders `f"{flow['spans_verified']} evidence spans were verified and "` / `f"{flow['spans_flagged']} flagged for review."`. (b) `validate_prisma_counts` reads **none** of the non-status counts except the PDF-reason sub-counts: not `rejection_reasons`, not `low_yield_rejected`, not the span counts. (c) `rejection_reasons` and `low_yield_rejected` are read by `export_prisma_csv` (CSV rows) and by two `test_low_yield.py` ids; nothing else. Per-count consumers in §1c. |
| **I5** screening-table joins read live screening tokens; no screening count depends on an extraction token | **false in part** | The two screening-TABLE reads (`abstract_screening_decisions`, `ft_screening_adjudication`) are joins on screening tokens and stay as they are. But **three screening-box counts are derived from the same `GROUP BY status` that carries the extraction tokens**, by complement: `screened_in = sum(c for s, c in status_counts.items() if s not in _pre_screening)` (feeds `records_screened`), and `full_text_retrieved` / `full_text_assessed` (`s not in _pre_fulltext`). Every `EXTRACTED` / `AI_AUDIT_COMPLETE` / `EXTRACT_FAILED` paper is counted inside them. §1e measures it: 5 phantom `EXTRACTED` papers raise `records_screened` to 5. |
| **I6** the eight reconciliation ids seed through `update_status` / `ReviewDatabase`, none through raw SQL | **false** | Four of the eight use raw SQL: three set `papers.pdf_exclusion_reason` with `db._conn.execute("UPDATE papers SET pdf_exclusion_reason = ...")` (`test_reconciliation_passes_clean_db`, `test_pdf_excluded_subcounts_sum`, `test_no_paper_in_multiple_terminal_boxes`), and `test_reconciliation_catches_mismatch` writes `UPDATE papers SET status = 'BOGUS_STATUS' ...` and a raw `INSERT INTO papers (title, source, status, created_at, updated_at) VALUES ('Ghost', 'test', 'ABSTRACT_SCREENED_OUT', ...)`. Routes per id in §1d. |

No false item sent the read elsewhere: each was read where the brief pointed, and the correction is
recorded above.

---

## §1 PRISMA (`engine/exporters/prisma.py`)

### 1a Every count `generate_prisma_flow` emits, in output order

The status source for every "status" row is one query:
`"SELECT status, COUNT(*) as cnt FROM papers GROUP BY status"`.

| # | key | how computed (quoted) | current source | event-side source |
|---|---|---|---|---|
| 1 | `records_identified` | `total_identified = sum(source_counts.values())` | `papers` (row count via source) | **none needed** — `papers` is identity, not state; stays |
| 2 | `records_by_source` | `"SELECT source, COUNT(*) as cnt FROM papers GROUP BY source"` | `papers.source` | none needed (identity) |
| 3 | `duplicates_removed` | literal `0  # tracked externally by dedup module` | none | none (constant) |
| 4 | `records_screened` | `screened_in + screened_out + screen_flagged`, with `screened_in = sum(c for s, c in status_counts.items() if s not in _pre_screening)` | `papers.status` (complement — includes every extraction token) | eligibility axis *if* screening wrote events; it writes none today (screening table live, R129) |
| 5 | `records_excluded` | `status_counts.get("ABSTRACT_SCREENED_OUT", 0)` | `papers.status` | eligibility `abstract_out` — **no writer**; screening is out of slice |
| 6 | `exclusion_reasons` | `"""SELECT sd.rationale, COUNT(*) as cnt FROM abstract_screening_decisions sd JOIN papers p ON p.id = sd.paper_id WHERE p.status = 'ABSTRACT_SCREENED_OUT' AND sd.decision = 'exclude' GROUP BY sd.rationale"""` | screening table ⨝ `papers.status` | screening table (live) |
| 7 | `screen_flagged` | `status_counts.get("ABSTRACT_SCREEN_FLAGGED", 0)` | `papers.status` | **NONE** — no two-axis token |
| 8 | `pdf_excluded` | `status_counts.get("PDF_EXCLUDED", 0)` | `papers.status` | split, no writer (see mapping) |
| 9 | `pdf_exclusion_reasons` | `"SELECT pdf_exclusion_reason, COUNT(*) as cnt FROM papers " "WHERE status = 'PDF_EXCLUDED' GROUP BY pdf_exclusion_reason"` | `papers.pdf_exclusion_reason` | **NONE** as a column; `paper_events.reason_code` could carry it, no writer |
| 10 | `reports_not_retrieved` | `pdf_exclusion_reasons.get("INACCESSIBLE", 0)` | derived from 9 | processing `full_text_not_obtainable` (S3h reason #1) — no writer |
| 11 | `pdf_eligibility_exclusions` | `{reason: count ... if reason != "INACCESSIBLE"}` | derived from 9 | eligibility `full_text_out` + reason — no writer |
| 12 | `eligibility_exclusions` | `dict(pdf_eligibility_exclusions)` + `"FT screening (AI primary)"` + optional `"FT screening (PI adjudicated)"` | derived | derived |
| 13 | `eligibility_excluded_total` | `sum(eligibility_exclusions.values())` | derived | derived |
| 14 | `full_text_retrieved` | `sum(c for s, c in status_counts.items() if s not in _pre_fulltext)` | `papers.status` (complement — includes every extraction token) | processing `parsed` or later, *if* parse wrote events — it does not on live (0 processing events) |
| 15 | `full_text_assessed` | `= full_text_retrieved` | derived | derived |
| 16 | `ft_screened_out` | `status_counts.get("FT_SCREENED_OUT", 0)` | `papers.status` | eligibility `full_text_out` — no writer (screening out of slice) |
| 17 | `ft_ai_primary` | `ft_screened_out - ft_pi_adjudicated` | derived | derived |
| 18 | `ft_pi_adjudicated` | `"""SELECT COUNT(*) FROM ft_screening_adjudication fta JOIN papers p ON p.id = fta.paper_id WHERE p.status = 'FT_SCREENED_OUT' AND fta.adjudication_decision = 'FT_SCREENED_OUT'"""` | screening table ⨝ `papers.status` | screening table (live) |
| 19 | `ft_flagged` | `status_counts.get("FT_FLAGGED", 0)` | `papers.status` | **NONE** — no two-axis token |
| 20 | `studies_included` | `sum(status_counts.get(s, 0) for s in _TERMINAL_INCLUDED)` | `papers.status` | eligibility `eligible` ∧ processing `audited_ai` (i.e. `analysis_ready` restricted to `audited_ai`; `analysis_ready` also admits `extracted`) |
| 21 | `papers_rejected` | `status_counts.get("REJECTED", 0)` | `papers.status` | **NONE** — no token |
| 22 | `rejection_reasons` | `"SELECT rejected_reason, COUNT(*) as cnt FROM papers " "WHERE status = 'REJECTED' GROUP BY rejected_reason"` | `papers.rejected_reason` | **NONE** |
| 23 | `low_yield_rejected` | `sum(cnt for reason, cnt in rejection_reasons.items() if "low_yield" in reason.lower())` | derived from 22 (substring match, N5) | **NONE as a rejection.** LOW_YIELD itself is derivable on read — `audit_events.low_yield(conn, paper_id, arm, *, codebook, threshold)` (R136) — but it is per arm and is not a rejection state |
| 24 | `in_progress` | `sum(c for s, c in status_counts.items() if s not in _terminal)` | `papers.status` (complement) | derived; no single axis expresses it |
| 25 | `extract_failed` | `status_counts.get("EXTRACT_FAILED", 0)` | `papers.status` | processing `extraction_failed` (and `input_exceeds_context`, which legacy had no separate status for) — **writer exists** (extractor's event mapping, F9) |
| 26 | `spans_verified` | `"SELECT COUNT(*) FROM evidence_spans WHERE audit_status = 'verified'"` | `evidence_spans.audit_status` | **NONE in the database.** Semantic verdicts go to `record_verdict(review_dir, ...)` → `telemetry/audit_calls.jsonl` (`read_verdicts` reads it); locator results are `citation_located` field events (`payload.located`). Neither is the legacy quantity: legacy `verified` mixed grep- and semantic-verified spans |
| 27 | `spans_flagged` | `"SELECT COUNT(*) FROM evidence_spans WHERE audit_status = 'flagged'"` | `evidence_spans.audit_status` | as 26. Note live also holds 449 `contested` spans, which PRISMA counts in neither box |

**Legacy status → two-axis vocabulary** (tokens from `engine/core/paper_state.py`: eligibility
`eligible · abstract_out · full_text_out`; processing `parsed · extracted · extraction_failed ·
full_text_not_obtainable · parse_failed · input_exceeds_context · audited_ai`; reason codes from
`EXTRACTION_REASONS`):

| legacy status | set | eligibility | processing | note |
|---|---|---|---|---|
| `AI_AUDIT_COMPLETE` | `_TERMINAL_INCLUDED` | `eligible` | `audited_ai` | writer: `audit_events.audit_run` |
| `HUMAN_AUDIT_COMPLETE` | `_TERMINAL_INCLUDED` | `eligible` | **no token** | human review is field-level reviewer events; no paper token (J6/J7, sophomore) |
| `INGESTED` | `_IN_PROGRESS` | `no_recorded_state` | `no_recorded_state` | `identified` event type maps to eligibility but has no token |
| `ABSTRACT_SCREENED_IN` | `_IN_PROGRESS` | **no token** | — | not yet `eligible` (that is FT) |
| `ABSTRACT_SCREEN_FLAGGED` | `_IN_PROGRESS` | **no token** | — | |
| `PDF_ACQUIRED` | `_IN_PROGRESS` | — | **no token** | `acquired` event type maps to processing but has no token |
| `PARSED` | `_IN_PROGRESS` | — | `parsed` | no parse-stage writer on live (0 processing events) |
| `FT_ELIGIBLE` | `_IN_PROGRESS` | `eligible` | — | |
| `FT_FLAGGED` | `_IN_PROGRESS` | **no token** | — | |
| `EXTRACTED` | `_IN_PROGRESS` | `eligible` | `extracted` | writer: extractor |
| `EXTRACT_FAILED` | `_IN_PROGRESS` | `eligible` | `extraction_failed` / `input_exceeds_context` + `reason_code` | writer: extractor (F9); legacy had one status for both tokens |
| `ABSTRACT_SCREENED_OUT` | `_TERMINAL_EXCLUDED` | `abstract_out` | — | no writer (screening out of slice) |
| `PDF_EXCLUDED` | `_TERMINAL_EXCLUDED` | `full_text_out` (non-INACCESSIBLE) | `full_text_not_obtainable` (INACCESSIBLE) | split by `pdf_exclusion_reason`; no writer for either |
| `FT_SCREENED_OUT` | `_TERMINAL_EXCLUDED` | `full_text_out` | — | no writer (screening out of slice) |
| `REJECTED` | `_TERMINAL_EXCLUDED` | **no token** | **no token** | |

Tokens with **no two-axis equivalent**: `HUMAN_AUDIT_COMPLETE`, `ABSTRACT_SCREENED_IN`,
`ABSTRACT_SCREEN_FLAGGED`, `PDF_ACQUIRED`, `FT_FLAGGED`, `REJECTED` (and `INGESTED`, which maps to
the absence of a record). Two-axis tokens with no legacy status of their own: `parse_failed`,
`input_exceeds_context`.

**The live fact that bounds the re-point.** On live, `paper_events` holds 190 rows, all
`('eligible', 'state_at_migration')`; `papers.status` reads `ABSTRACT_SCREENED_OUT` 9,647 ·
`AI_AUDIT_COMPLETE` 190 · `FT_SCREENED_OUT` 171 · `PDF_EXCLUDED` 31 (read `mode=ro` this
session). So 9,849 papers read `no_recorded_state` on **both** axes. Any status count moved to the
event store today reports those papers as unrecorded rather than screened out, until screening
writes eligibility events (junior). Only the post-eligibility counts (20, 25, and the
extraction-token share of 4/14/24) have a live writer to move to.

### 1b `validate_prisma_counts`

It raises `ValueError(f"PRISMA reconciliation failed: {'; '.join(details)}")` on any failed check,
and `export_prisma_csv` calls it first (`# Reconcile before exporting`), so a failure aborts
`export_all` and with it `run_pipeline._stage_export`. Checks and the counts each needs:

1. **Totals** — `total_prisma = records_excluded + pdf_excluded + ft_screened_out +
   papers_rejected + studies_included + in_progress` against `"SELECT COUNT(*) FROM papers"`.
2. **PDF sub-counts** — `sum(flow["pdf_exclusion_reasons"].values())` vs `pdf_excluded`.
3. **2b eligibility sub-counts** — `sum(flow["eligibility_exclusions"].values())` vs
   `eligibility_excluded_total` (tautological: the total is defined as that sum).
4. **2c** — `reports_not_retrieved + sum(pdf_eligibility_exclusions.values())` vs `pdf_excluded`.
5. **Terminal boxes** — a **second status read**: `f"SELECT COUNT(*) FROM papers WHERE status IN
   ({placeholders})"` over `_TERMINAL_EXCLUDED | _TERMINAL_INCLUDED`, against the flow's terminal
   sum.

It reads no `rejection_reasons`, `low_yield_rejected`, span count, `extract_failed`,
`screen_flagged`, `ft_flagged` or `records_screened`. **What breaks if a count becomes
unavailable:** check 1 and check 5 are partition identities over one store. If
`papers_rejected` is removed they still hold whenever no paper sits at `REJECTED` (live: 0). If
some partition terms move to events and others stay on `papers.status`, check 1 becomes a
cross-store sum and check 5's direct status read no longer mirrors the flow — a paper can be
counted in both stores or in neither, and the check fails (or, worse, passes by coincidence).
The reconciler's contract is only sound while every term it sums comes from one store.

### 1c Consumers of the PRISMA dict

- `export_all` (`engine/exporters/__init__.py`) calls `export_prisma_csv(db, prisma_path)` and
  records only the path; the dict never leaves `prisma.py` on this route.
  `run_pipeline._stage_export` calls `export_all(db, spec, review_name)` and returns
  `{"files": paths, "elapsed": elapsed}` — no count.
- `export_prisma_csv` reads every key except `ft_ai_primary`, `ft_pi_adjudicated`,
  `pdf_exclusion_reasons` / `pdf_eligibility_exclusions` directly (they arrive via
  `eligibility_exclusions`) — including `rejection_reasons`, `low_yield_rejected`,
  `extract_failed`, `in_progress`, `spans_verified`, `spans_flagged`.
- **`methods_section.generate_methods_section`** calls `generate_prisma_flow(db)` itself and
  reads `records_by_source`, `studies_included`, `full_text_assessed`, `records_screened`,
  `records_excluded`, `screen_flagged`, `records_identified`, `duplicates_removed`,
  `spans_verified`, `spans_flagged`. (`screened_in = flow["studies_included"] +
  flow["full_text_assessed"]` is computed and never used.)
- Per count, from a read of every `flow[...]` site (engine + tests):
  `rejection_reasons` → CSV, `test_low_yield`; `low_yield_rejected` → CSV, `test_low_yield`;
  `papers_rejected` → validator checks 1/5, CSV, `test_low_yield`; `spans_verified` /
  `spans_flagged` → CSV, **methods**; `pdf_exclusion_reasons` → validator 2, reconciliation test;
  `extract_failed` / `in_progress` → CSV, reconciliation tests (`in_progress` also validator 1).
- **Coupling:** methods' commit depends on PRISMA's shape. Re-pointing PRISMA changes the
  numbers methods renders; removing `spans_*` from the dict breaks methods.

### 1d The eleven PRISMA test ids (R-b)

Route key: **RD** = `ReviewDatabase` + `add_papers`; **US** = `update_status`; **SD** =
`add_screening_decision`; **LX** = `add_extraction` / `add_evidence_span` / `update_audit`;
**RP** = `reject_paper`; **SQL** = raw SQL; **M** = `mirror_legacy_into_events`.

| id | route | asserts | event-side equivalent |
|---|---|---|---|
| `test_exporters.py::test_prisma_flow_counts` | `populated_db`: RD, SD, US, LX, M | `records_identified` 15; `records_by_source` pubmed 10 / openalex 5; `records_excluded` 4; `screen_flagged` 3; `studies_included` 3 | partial: identity counts stay; `records_excluded` needs `abstract_out` (no writer); `screen_flagged` has **no token**; `studies_included` → `eligible` ∧ `audited_ai` (M writes only `eligible`, no processing event) |
| `test_low_yield.py::TestPrismaLowYield::test_prisma_includes_low_yield_rejected` | `tmp_db`: RD, US, LX (`_advance_to_ai_audit`), **RP** | `papers_rejected` 1; `low_yield_rejected` 1; `"low_yield_excluded" in str(flow["rejection_reasons"])` | **none — MARKED: carried entirely by `rejected_reason` / `REJECTED`** |
| `test_low_yield.py::TestPrismaLowYield::test_prisma_no_low_yield_when_none_rejected` | `tmp_db`: RD only, empty | `low_yield_rejected` 0 | **none — MARKED: carried entirely by `rejected_reason`** (trivially true on an empty db) |
| `test_prisma_reconciliation.py::TestReconciliation::test_reconciliation_passes_clean_db` | RD, US, **SQL** (`pdf_exclusion_reason`) | `validate_prisma_counts` valid; `total_db` 10; `discrepancy` 0 | partial: the partition identity is expressible per axis, but 7 of 10 papers sit at screening/PDF tokens with no writer |
| `test_prisma_reconciliation.py::TestReconciliation::test_reconciliation_catches_mismatch` | RD, **SQL** (`status = 'BOGUS_STATUS'`; raw `INSERT INTO papers`) | `valid is True` — despite its name it asserts no mismatch is caught | none: a bogus token is refused at write by 019's `to_state` CHECK, so the event-side analogue is that CHECK, not the reconciler |
| `test_prisma_reconciliation.py::TestReconciliation::test_in_progress_papers_counted` | RD, US | `in_progress` 4; `records_excluded` 2; valid | partial: `ABSTRACT_SCREENED_IN` has no token; `PARSED` → `parsed`; `FT_ELIGIBLE` → `eligible` |
| `test_prisma_reconciliation.py::TestPDFExcludedSubcounts::test_pdf_excluded_subcounts_sum` | RD, US, **SQL** (`pdf_exclusion_reason`) | `pdf_excluded` 5; reason sub-counts NON_ENGLISH 2 / NOT_MANUSCRIPT 2 / INACCESSIBLE 1 | none today: needs `full_text_out` / `full_text_not_obtainable` + `reason_code`, no writer |
| `test_prisma_reconciliation.py::TestNoDoubleCount::test_no_paper_in_multiple_terminal_boxes` | RD, US, **SQL** (`pdf_exclusion_reason`) | 1/1/1/1 terminal, `in_progress` 0, valid, `total_db` 4 | partial: `studies_included` yes; screening/PDF boxes no writer |
| `test_prisma_reconciliation.py::TestNoDoubleCount::test_ai_audit_complete_not_double_counted_with_ft` | RD, US | `studies_included` 1; `in_progress` 1; valid | yes: `audited_ai` vs `eligible` with no processing |
| `test_prisma_reconciliation.py::TestExtractFailed::test_extract_failed_appears_in_flow_and_csv` | RD, US | `extract_failed` 2; `studies_included` 1; valid; CSV contains "Extraction failed" / "Model timeout/error" | yes: `extraction_failed` + reason code |
| `test_prisma_reconciliation.py::TestExtractFailed::test_extract_failed_zero_omitted_from_csv` | RD, US | `extract_failed` 0; CSV omits the line | yes |

**Beyond the eleven.** Five further collected ids drive `generate_prisma_flow` and are not in
R-b's list: `test_exporters.py::test_prisma_csv`, `::test_atomic_prisma_csv_no_partial_on_error`,
`::test_export_all` (via `export_prisma_csv`), and — because methods calls PRISMA — every methods
id in §2d. Their assertions are shape/atomicity, not counts, except where §2d notes a count.

### 1e B13 measurement (constructed fixture, scratch directory, never live)

N = **5**. Script kept outside the repository. Four routes, because the first two could not run
PRISMA at all — which is itself the measurement for them:

| route | construction | result |
|---|---|---|
| A | `add_values(path, "local_fixture", "study_type", ["RCT"] * 5)` alone | `papers.status` = `[('EXTRACTED', 5)]`; `eligible_paper_ids` = (1..5). `generate_prisma_flow` **raises** `OperationalError: no such table: abstract_screening_decisions` — `_event_store_fixture` builds no screening tables. |
| B | `ReviewDatabase` file, then `add_values` on it | `add_values` **raises** `IntegrityError: FOREIGN KEY constraint failed`: its `INSERT OR IGNORE INTO papers (id) VALUES (?)` is silently ignored by `ReviewDatabase`'s NOT NULL `papers` columns, so the paper event's FK has no parent. `add_values` cannot seed a `ReviewDatabase`-shaped file whose papers it did not create. |
| **D** | route A + the tables/columns PRISMA reads grafted from a fresh `ReviewDatabase` schema (`abstract_screening_decisions`, `ft_screening_adjudication`, `evidence_spans.audit_status`, `papers.pdf_exclusion_reason`, `papers.rejected_reason`); no row added | **The B13 measurement.** `papers.status` `EXTRACTED` 5; events: all 5 `('eligible', 'no_recorded_state')`. PRISMA: `records_identified` **5**, `records_screened` **5**, `full_text_retrieved` **5**, `full_text_assessed` **5**, `in_progress` **5**; `records_excluded` 0, `screen_flagged` 0, `studies_included` 0, `extract_failed` 0, `papers_rejected` 0, `spans_*` 0, `exclusion_reasons` {}, `eligibility_exclusions` {'FT screening (AI primary)': 0}. |
| C | `ReviewDatabase` + `add_papers(5)` (INGESTED), then `add_values` on those ids | `papers.status` `INGESTED` 5 while all 5 are `eligible` on events. PRISMA: `records_identified` 5, `in_progress` 5, every screening/full-text count 0. The two stores disagree the other way. |

Against the brief's expectation: "the status counts do" — **holds** (`in_progress`, and the
complement-derived `full_text_*`); "the screening counts do not" — **holds for the two
screening-table counts** (`exclusion_reasons`, `ft_pi_adjudicated`) and **fails for
`records_screened`**, which is status-derived by complement and shows all 5 phantom papers (I5).

### 1f `mirror_legacy_into_events` at HEAD (`tests/_event_store_fixture.py`)

Signature: `def mirror_legacy_into_events(db, *, arm: str = "local") -> int:`

Docstring, verbatim:

> Mirror a legacy-shaped fixture's rows into the event store. TEST ONLY.
>
> Some fixtures declare their world through `ReviewDatabase.add_extraction` /
> `add_evidence_span` / `update_status` — the engine's own write path before
> the event store existed. Those calls are still the clearest way to say what
> the fixture contains, and rewriting them by hand into `write_field_event`
> calls would change what the test READS without changing what it MEANS.
>
> So this translates instead: the latest extraction's spans become `asserted`
> (+ `citation_located`) field events, and a corpus status becomes one
> `eligible` paper event. It is the shape migration 017 seeded on live, with
> field values added — which 017 deliberately did not do (R25: seed, do not
> migrate). **That is why this lives in `tests/` and not in `engine/`:** it is
> a fixture convenience, not a supported import path, and nothing in the
> engine may grow a dependency on it.
>
> Returns the number of field events written.

It **reads** `papers.status` (the five statuses `"FT_ELIGIBLE", "EXTRACTED", "AI_AUDIT_COMPLETE",
"HUMAN_AUDIT_COMPLETE", "EXTRACT_FAILED"`) and the latest extraction's `evidence_spans`, and
**writes** the fixture run (`fixture_run(conn, arm)`: a `run_manifests` row, the arm's
registration and a `run_stage_configs` row with `model_name` `'fixture'`), one `state_at_migration`
`eligible` paper event per corpus-status paper that has none, and one `asserted` plus one
`citation_located` field event per non-NULL span value. It writes **no processing-axis event**, so
a mirrored fixture's papers read `processing = no_recorded_state` — an `AI_AUDIT_COMPLETE` legacy
paper is not `audited_ai` on events. Its only consumer is `tests/test_exporters.py` (two
fixtures: `populated_db`, `db_with_empty_extractions`).

---

## §2 methods_section (`engine/exporters/methods_section.py`)

### 2a Every data read in the module

Database:
- `_query_ft_screening_models`: `"SELECT model, COUNT(DISTINCT paper_id) as cnt FROM ft_screening_decisions GROUP BY model"` — screening table, live, out of slice.
- `_query_extraction_models`: `"SELECT model, COUNT(DISTINCT paper_id) as cnt FROM extractions WHERE model IS NOT NULL GROUP BY model"` — legacy.
- `_query_audit_models`: `"SELECT auditor_model, COUNT(DISTINCT es.extraction_id) as cnt " "FROM evidence_spans es WHERE es.auditor_model IS NOT NULL GROUP BY es.auditor_model"` — legacy. (It counts **extractions**, not papers, despite rendering beside paper counts.)
- `flow = generate_prisma_flow(db)` — every PRISMA read in §1a, keys listed in §1c.

Codebook: `n_fields = len(load_codebook_beside(db.db_path).fields)` (`engine.core.codebook`).

Spec (all direct attribute reads, **none through the resolver**):
- `spec.search_strategy.databases`, `.date_range`, `.query_terms`;
- `spec.screening_models.primary` (abstract screening, always from spec);
- `spec.ft_screening_models.primary` / `.verifier` (fallback when `ft_screening_decisions` is empty);
- `spec.auditor_model` (fallback when `_query_audit_models` is empty);
- `spec.cloud.enabled_arms`, `spec.arm(name).provider`, `spec.arm(name).model`.

**Finding (not asked):** the audit fallback reads `spec.auditor_model`, which is `Optional[str]`
with default `None` and is unset on the live spec. The resolver resolves the audit stage as
`override = getattr(spec, "auditor_model", None)` and otherwise `blk.model`
(`AuditModels.model` default `"gemma3:27b"`). So on a database with no legacy audit rows,
methods renders `"[MODEL NOT SPECIFIED]"` for the auditor while `stage_config("audit", spec).model`
would name `gemma3:27b`. The screening fallbacks likewise bypass `stage_config`.

### 2b Migration 020 DDL, verbatim (as rendered into live `sqlite_master`, read `mode=ro`)

The source is `RUN_MANIFESTS_SQL` / `RUN_STAGE_CONFIGS_SQL` / `RUN_CALLS_SQL` in
`engine/migrations/020_run_manifest.py`; the two `CHECK (... IN ({_in(...)}))` f-string slots are
shown here expanded, exactly as SQLite stored them.

```sql
CREATE TABLE run_manifests (
    run_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_uid         TEXT    NOT NULL UNIQUE,
    review_id       TEXT    NOT NULL,
    run_kind        TEXT    NOT NULL CHECK (run_kind IN ('extraction', 'screening', 'judge', 'review_session')),
    git_commit      TEXT    NOT NULL CHECK (length(git_commit) = 40),
    git_dirty       INTEGER NOT NULL CHECK (git_dirty = 0),
    git_tag         TEXT,
    engine_state    TEXT,
    spec_hash       TEXT    NOT NULL,
    codebook_hash   TEXT    NOT NULL,
    codebook_sha256 TEXT    NOT NULL,
    library_versions_json TEXT NOT NULL,
    host            TEXT    NOT NULL,
    started_at      TEXT    NOT NULL,
    ended_at        TEXT,
    end_status      TEXT    CHECK (end_status IS NULL OR end_status IN ('completed', 'failed', 'interrupted')),
    cloud_arms_json TEXT    NOT NULL DEFAULT '[]',
    payload_description TEXT,
    manifest_json   TEXT    NOT NULL,
    manifest_sha256 TEXT    NOT NULL,
    CHECK ((ended_at IS NULL) = (end_status IS NULL)),
    CHECK (cloud_arms_json = '[]' OR payload_description IS NOT NULL)
)
```

```sql
CREATE TABLE run_stage_configs (
    run_id          INTEGER NOT NULL REFERENCES run_manifests(run_id),
    stage           TEXT    NOT NULL,
    stage_kind      TEXT    NOT NULL CHECK (stage_kind IN ('abstract_screen_primary', 'abstract_screen_verifier', 'ft_screen_primary', 'ft_screen_verifier', 'audit', 'extract_pass1', 'extract_pass2', 'extract_retry_snippet', 'elicitation_pass1', 'vision_parse', 'pdf_quality', 'preflight', 'cloud')),
    arm_name        TEXT    REFERENCES arms(arm_name),
    provider        TEXT    NOT NULL CHECK (provider IN ('ollama', 'openai', 'anthropic')),
    model_name      TEXT    NOT NULL,
    model_digest    TEXT,
    options_json    TEXT    NOT NULL,
    options_hash    TEXT    NOT NULL,
    sent_keys_json  TEXT    NOT NULL,
    sources_json    TEXT    NOT NULL,
    keep_alive      TEXT    NOT NULL,
    format_schema_hash TEXT NOT NULL,
    prompt_hash     TEXT    NOT NULL,
    PRIMARY KEY (run_id, stage),
    -- R78: NULL-safe. `length(NULL) = 64` is NULL, which a CHECK passes, so the
    -- digest's presence is tested with IS NOT NULL before its length.
    CHECK (provider IS NOT 'ollama' OR (model_digest IS NOT NULL AND length(model_digest) = 64))
)
```

```sql
CREATE TABLE run_calls (
    call_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          INTEGER NOT NULL REFERENCES run_manifests(run_id),
    stage           TEXT    NOT NULL,
    paper_id        INTEGER REFERENCES papers(id),
    request_hash    TEXT    NOT NULL,
    response_digest TEXT,
    started_at      TEXT    NOT NULL,
    ended_at        TEXT    NOT NULL,
    FOREIGN KEY (run_id, stage) REFERENCES run_stage_configs(run_id, stage)
)
```

Writer sites (`engine/core/run_manifest.py`):
- `run_stage_configs.model_name` — `open_run`, inside `SAVEPOINT open_run`: `"INSERT INTO
  run_stage_configs (run_id, stage, stage_kind, arm_name, " "provider, model_name, model_digest,
  ..."` with `r.config.model` in the `model_name` slot, one row per resolved stage (`stage` is the
  resolver key; `stage_kind` is `"cloud"` for `cloud:<arm>`, else `cfg.stage`).
- `run_calls` — `record_call(conn, run_id, stage, paper_id, request, digest, started_at,
  ended_at)`: `"INSERT INTO run_calls (run_id, stage, paper_id, request_hash, response_digest, "`.
  Two callers: `record_active_ollama_call` (from `ollama_chat`'s `_done`, whenever a run is
  active and the call carries a `stage`; `preflight` is keyed `f"preflight:{request.get('model')}"`),
  and `engine/cloud/base.py` (`rm.record_call(self._conn, self.run_id, self.stage_cfg.stage,
  paper_id, ...`).
- `paper_id` on `run_calls` is whatever `ollama_chat` received: the extractor passes it
  (`ollama_chat(paper_id=paper_id, messages=pass1_messages(prompt), ...`); **the auditor does
  not** (quoted in §0 I2).
- Test fixtures also write `run_stage_configs` directly: `_event_store_fixture.fixture_run`
  inserts `model_name` `'fixture'`, `stage` `f"fixture:{arm}"`, `stage_kind` `'extract_pass2'`.

### 2c Mapping — rendered string → event-side expression

| rendered today | from | event-side expression | if absent |
|---|---|---|---|
| extraction model string (`next(iter(extraction_model_counts))`, or `"m (n=X) and ..."` when several) | `extractions.model`, `COUNT(DISTINCT paper_id)` | `run_stage_configs.model_name` for stages of kind `extract_pass1` / `extract_pass2` (+ `extract_retry_snippet`, `elicitation_pass1`), with papers = `COUNT(DISTINCT run_calls.paper_id)` joined on `(run_id, stage)`; or papers = `COUNT(DISTINCT paper_id)` of the arm's claim events joined to their `run_id` | no extraction run → `"[MODEL NOT SPECIFIED]"` (today's behaviour); a run with stage rows but no calls → a model with n=0, which the current formatter would render as `"m (n=0)"` only when there are ≥ 2 models |
| audit model string | `evidence_spans.auditor_model`, `COUNT(DISTINCT extraction_id)` | `run_stage_configs.model_name WHERE stage = 'audit'`. Papers **cannot** come from `run_calls` (audit rows carry `paper_id` NULL); the countable source is the `audited` paper events (`to_state = 'audited_ai'`, `stage_name = 'audit'`, `run_id`), which `audit_run` writes per paper with `actor_digest` = the audit stage's digest | no audit stage in any run → falls to the spec fallback, which today is `spec.auditor_model` (None on live → `"[MODEL NOT SPECIFIED]"`) rather than the resolver |
| `"{flow['spans_verified']} evidence spans were verified and {flow['spans_flagged']} flagged for review."` | PRISMA 26/27 | **none in the database** (§1a 26) | the sentence has no source |
| `included_for_extraction` (`flow["studies_included"]`) | PRISMA 20 | `eligible` ∧ `audited_ai` | 0 until an audit stage runs |
| `records_screened`, `records_excluded`, `flagged`, `records_identified`, source breakdown | PRISMA 1/2/4/5/7 | follows PRISMA's ruling | — |
| FT model counts | `ft_screening_decisions` | unchanged (screening table, live) | — |

On live today `run_manifests`, `run_stage_configs` and `run_calls` are all empty (M4), so the
placeholder path — `"[MODEL NOT SPECIFIED]"` for extraction and the spec fallback for audit — **is
the live path until Run 7** (the freshman smoke run in session 10 is the first run to write them).

### 2d The methods test ids

R-c names six; eight collected ids drive `generate_methods_section` (`export_methods_md` calls it),
plus `test_export_all`.

| id | route | asserts | needs a legacy column? |
|---|---|---|---|
| `test_methods_section_content` | `populated_db` (RD, SD, US, LX, M) | "PubMed", "OpenAlex", `spec.screening_models.primary`, **"deepseek-r1:32b"** (from `extractions.model`), **"qwen3:32b"** (from `evidence_spans.auditor_model`, set by `update_audit(..., "qwen3:32b", ...)`), "dual-pass", "two-pass", "15" | **yes** — the two model strings |
| `test_methods_md_export` | `populated_db` | file starts "# Methods"; contains "systematic search" | no |
| `test_atomic_methods_md_no_partial_on_error` | `populated_db` | no file after `open` raises | no |
| `test_methods_uses_spec_screening_model` | `populated_db` | `spec.screening_models.primary` in text | no (spec) |
| `test_methods_uses_db_extraction_model` | `populated_db` | "deepseek-r1:32b" | **yes** — only `extractions.model` supplies it (the fixture run's `model_name` is `'fixture'`) |
| `test_methods_uses_db_audit_model` | `populated_db` | "qwen3:32b" | **yes** — only `evidence_spans.auditor_model` supplies it; `qwen3:32b` is not the spec's auditor, so no fallback can produce it |
| `test_methods_multi_model_ft_screening` | inline: RD, SD, US, **SQL** (`INSERT INTO ft_screening_decisions`), LX | `"qwen3.5:27b (n=3)"`, `"qwen3:32b (n=2)"` | no — screening table (the LX writes are incidental to the assertion) |
| `test_methods_placeholder_when_no_data` | inline: RD, SD, US (no LX) | `"[MODEL NOT SPECIFIED]" in methods` | no — satisfied by the extraction placeholder; also by the audit fallback, since `spec.auditor_model` is None |

---

## §3 validate_all (`engine/validators/extraction_validator.py`)

### 3a Reads and return shapes

- `validate_extraction(spec, paper_id, db) -> list[dict]` — codebook via
  `load_codebook_beside(db.db_path)` (**`engine.core.codebook`**, confirmed); non-value tokens via
  `non_value_tokens_for(Path(db.db_path).parent / "extraction_codebook.yaml")`; spans via
  `"""SELECT es.field_name, es.value FROM evidence_spans es JOIN extractions e ON es.extraction_id
  = e.id WHERE e.paper_id = ?"""` — **every** extraction of the paper, no latest filter. Returns
  `{paper_id, field_name, value, issue}` dicts. `spec` is accepted and unused.
- `validate_all(spec, db, statuses=("EXTRACTED", "AI_AUDIT_COMPLETE", "HUMAN_AUDIT_COMPLETE"))
  -> tuple[list[dict], list[dict]]` — papers via `"SELECT id FROM papers WHERE status = ?"` per
  status; per paper, `validate_extraction` then the same span SQL again, fed to
  `detect_cross_field_bleed(load_codebook_beside(db.db_path), spans, _non_value_tokens(db))`.
  Returns `(issues, bleeds)`; bleeds are `{field_name, extracted_value, belongs_to_field,
  paper_id}`.
- `main()` — constructs `ReviewDatabase(args.review)` (the CLI opens live read-write-capable),
  counts spans with `"""SELECT COUNT(*) FROM evidence_spans es JOIN extractions e ON
  es.extraction_id = e.id JOIN papers p ON e.paper_id = p.id WHERE p.status IN ('EXTRACTED',
  'AI_AUDIT_COMPLETE', 'HUMAN_AUDIT_COMPLETE')"""`, calls `validate_all(spec, db)`, prints.
  Flags: `--review`, `--spec`. Callers of `validate_all`: `main` only.
- Also in the module, unaffected: `verify_schema_parity(spec)` (prompt hash; the brief's
  `check_schema_parity` is a different function in `engine/analysis/concordance.py`),
  `normalize_prefix` (no engine caller; H3), `_closest_match`.

### 3b `iter_grid` and what a row carries

Signature: `def iter_grid(conn, *, codebook, sentinels=None, papers=None, arms=None):` —
docstring: "`(paper_id, field_name, arm, EffectiveValue)` for every cell in the grid." `papers`
defaults to `eligible_paper_ids(conn)`, `arms` to `registered_arms(conn)`, fields always to
`codebook.field_names`. `EffectiveValue` is `value: str | None`, `state: str`, `rule_row: int`,
`provenance: dict`.

Against `validate_extraction`'s checks:
- **Unknown field name** — **cannot fire on a grid row.** The grid's fields are the codebook's by
  construction. The check has no target on the grid. `engine/core/events.py::write_field_event`
  holds no codebook reference, so the writer does not refuse an unknown field name; such a claim
  would be stored and be invisible to every grid reader. Finding it would take a direct
  `field_events` read (whether the extractor's completeness layer prevents one upstream was not
  read).
- **Categorical membership / semicolon lists / `sample_size` numeric / absence sentinel skip /
  non-value skip** — run unchanged on `(field_name, ev.value)`, **provided a `None` guard**:
  `missing` cells yield `value=None`, and today's code calls `value.split(";")` and
  `value.strip()` unguarded (legacy spans never had NULL values reach it).
- Semantics shift: legacy validated every value of every extraction; the grid validates one
  effective value per cell (latest live claim by rule v2.1). Superseded claims are no longer
  checked.
- `detect_cross_field_bleed` takes `list[{"field_name", "value"}]`, which a grid row supplies.

### 3c The arm (I3)

`validate_all` / `main` have no arm. Candidates at HEAD, each read:
- **`spec.extraction_models.arm`** (`local_deepseek_r1_32b`) — how the extractor, the auditor
  (`audit_run(..., arm=spec.extraction_models.arm, ...)`) and selection choose; not yet in the
  live registry (it pins at its first manifest), so `iter_grid(arms=(it,))` on live today yields
  190×20 `missing` cells.
- **`registered_arms(conn)`** — `iter_grid`'s own default; on live the three pre-manifest Run-6
  arms (`local`, `anthropic_sonnet_4_6`, `openai_o4_mini_high`), whose claims are not in the event
  store (R25) — also all `missing`.
- **A CLI flag** — precedent: `distribution_monitor`'s `parser.add_argument("--arm",
  required=True, ...)`; the exporters instead take `arm: str = "local"` ("Required knowledge,
  defaulted for compatibility") and raise `UnknownArm` against `registered_arms(conn,
  include_retired=True)`.

### 3d The validator test ids (R-d, after I1)

Route: **VF** = the file's `_add_paper_and_extraction` helper — `add_papers`, `update_status` ×4
to `EXTRACTED`, then **raw SQL** `INSERT INTO extractions ...` and `INSERT INTO evidence_spans
...`. All nine call `validate_extraction` directly; none calls `validate_all`.

| id | route | asserts | check on a value / a legacy row shape |
|---|---|---|---|
| `test_valid_spans_no_issues` | VF | 4 valid values → `[]` | value — rewritable to grid rows |
| `test_unknown_field_name_flagged` | VF | 1 issue, "unknown field name", suggests `study_type` | **row shape — no grid target** (field set is the codebook's) |
| `test_invalid_categorical_value_flagged` | VF | 1 issue, "invalid categorical value", "closest:", "Original Research" | value |
| `test_numeric_field_non_numeric_flagged` | VF | "non-numeric sample_size" | value |
| `test_not_found_value_accepted` | VF | `NOT_FOUND`, `NR` → `[]` | value (sentinel skip) |
| `test_semicolon_all_valid` | VF | `[]` | value |
| `test_semicolon_one_invalid` | VF | 1 issue, value "Robotic" | value |
| `test_single_value_no_semicolons` | VF | 1 issue, closest "Original Research" | value |
| `test_semicolon_all_invalid` | VF | 2 issues {"Robotic", "Autonomous"} | value |
| `test_non_value_tokens_downstream.py::test_site3_the_skip_is_wired_into_all_three_check_points` | none (source inspection) | `"non_value" in inspect.getsource(fn)` for `detect_cross_field_bleed`, `validate_extraction` | **source-text pin** — survives a re-point only if both functions keep the `non_value` name; not a value check |

The other 11 collected ids in `test_extraction_validator.py` (`_closest_match`, four
`normalize_prefix`, four bleed, two schema-hash) take no database and are untouched by a re-point.

---

## §4 Cross-cutting

### 4a `tests/test_exporters.py` — 20 collected ids, by reader and fixture

| reader | `populated_db` | `db_with_empty_extractions` | inline |
|---|---|---|---|
| PRISMA | `test_prisma_flow_counts`, `test_prisma_csv`, `test_atomic_prisma_csv_no_partial_on_error` | — | — |
| methods | `test_methods_section_content`, `test_methods_md_export`, `test_atomic_methods_md_no_partial_on_error`, `test_methods_uses_spec_screening_model`, `test_methods_uses_db_extraction_model`, `test_methods_uses_db_audit_model` | — | `test_methods_multi_model_ft_screening`, `test_methods_placeholder_when_no_data` |
| evidence-table | `test_evidence_csv_columns`, `test_evidence_excel_sheets`, `test_atomic_csv_no_partial_on_error` | `test_empty_extraction_has_marker`, `test_exclude_empty_omits_empty_papers`, `test_exclude_empty_excel` | — |
| docx | `test_docx_created`, `test_atomic_docx_no_partial_on_error` | — | — |
| other (`export_all`, all five) | `test_export_all` | — | — |

15 + 3 + 2 = 20. Both fixtures seed through RD, SD, US, LX and end in
`mirror_legacy_into_events(db)`; the two inline ids use no mirror. The evidence-table and docx
ids already read through the reader (READERS-01) — they touch legacy only through the fixture's
seeding, which is the route R160b's writer retirement breaks.

### 4b The event-fixture toolkit at HEAD

`tests/_event_store_fixture.py`:
- `def ensure_event_store(db_path: str | Path) -> None:` — `papers` (via `_PAPERS_DDL`, default status `'EXTRACTED'` — B13) + 016/019/020/021; memoised per path.
- `def fixture_run(conn, arm: str | None = None, *, arm_kind: str = "model") -> int:` — the fixture run; with `arm`, registers and pins it (`run_stage_configs` row, `model_name` `'fixture'`).
- `def seed_pre_manifest_paper_event(conn, paper_id: int, *, to_state: str = "eligible", actor_name: str = "fixture", payload: dict | None = None) -> None:` — a 017-shaped `state_at_migration` row, below the writer.
- `def add_values(db_path: str | Path, arm: str, field_name: str, values, *, arm_kind: str = "model", start_paper: int = 1, located: bool = True) -> None:` — one claim per paper (+ `citation_located`); creates bare `papers` rows.
- `def mirror_legacy_into_events(db, *, arm: str = "local") -> int:` — §1f.
- `def upgrade_event_store(db_path) -> None:` — 016 → 019 → 020 on a file that already has `papers`.
- `def seed_claim(conn, *, arm: str, paper_id: int, field_name: str, value=None, claim_id: str | None = None, extraction_uid: str | None = None, source_snippet: str | None = None, event_type: str = "asserted", actor_name: str = "seed", payload: dict | None = None) -> str:` — pre-manifest seeded claim, below the writer.
- `def run_for(conn) -> int:` — the fixture run, pinning every claimable model arm.
- `def seed_eligibility(conn, paper_id: int, *, to_state: str = "eligible") -> int:` — one eligibility event through the writer (`event_type="screened"`).
- `def open_extraction_run(db, spec, *, digest: str = FIXTURE_DIGEST) -> int:` — a real `open_run` manifest on a scratch db: the spec's extraction stages + `audit`, real `model_name`s from the spec.
- `def claim_identity(arm: str, paper_id: int, *, sha: str = FIXTURE_TEXT_SHA, uid: str | None = None) -> dict:` — the three input-identity payload keys.
- Constants: `FIXTURE_RUN_UID = "fixture-run"`, `FIXTURE_DIGEST = "a" * 64`, `FIXTURE_TEXT_SHA = "f" * 64`.

`tests/_corpus_fixture.py`: `def build_fixture(root: Path) -> Path:` (a self-contained review dir,
one paper per legacy status + a pool), `def _seed_event_store(db_path: Path) -> None:` (eligible
events for `_ELIGIBLE_FROM_STATUS`, plus an `extraction_failed` processing event per
`EXTRACT_FAILED` paper); constants `FIXTURE_STATUSES`, `STATUS_PROBE_IDS`, `FIXTURE_CARRIED`,
`FIXTURE_CARRIED_NON_CORPUS`.

**Gaps a rewrite would meet** (read, not proposed): there is no helper that writes a
**processing-axis** paper event through the writer (`seed_eligibility` is eligibility-only;
`_corpus_fixture` writes `extraction_failed` by raw INSERT); no helper that writes a
`run_stage_configs` row with a **chosen `model_name`** other than `open_extraction_run` (spec
models only) and `fixture_run` (`'fixture'`); no helper that writes a `run_calls` row; and
`add_values` cannot seed a `ReviewDatabase`-shaped file (§1e route B).

### 4c Size estimate per reader

No recommendation; counts are ids in the current tree.

| reader | ids to rewrite | ids with no rewrite target (ruling decides) | new helper needed | scope note |
|---|---|---|---|---|
| **PRISMA** | 3 fully event-expressible: `test_ai_audit_complete_not_double_counted_with_ft`, `test_extract_failed_appears_in_flow_and_csv`, `test_extract_failed_zero_omitted_from_csv`. 4 partial, rewritable only if screening/PDF tokens are seeded in the fixture: `test_prisma_flow_counts`, `test_in_progress_papers_counted`, `test_reconciliation_passes_clean_db`, `test_no_paper_in_multiple_terminal_boxes`. 2 follow the fixture: `test_prisma_csv`, `test_atomic_prisma_csv_no_partial_on_error` | 2 marked (`TestPrismaLowYield` ×2); `test_reconciliation_catches_mismatch` (asserts nothing its name says); `test_pdf_excluded_subcounts_sum` (no PDF-reason writer) | **y** — processing-axis paper event through the writer; screening/PDF tokens if seeded | **Larger than "re-point plus fixture rewrite."** Half the status counts have no live event writer (screening is junior), three are complement-derived across both axes, two are source-less (`rejection_reasons`/`low_yield_rejected`, span verdicts), and `validate_prisma_counts` is a single-store partition identity that a partial move breaks. A re-point needs a count model (which boxes come from which axis, what "in progress" means on two axes) — a design decision, flagged for R51. |
| **methods** | 5: `test_methods_section_content`, `test_methods_uses_db_extraction_model`, `test_methods_uses_db_audit_model` (model strings from a run), `test_methods_placeholder_when_no_data` (placeholder path), plus the fixture move for `test_methods_md_export` / `test_atomic_methods_md_no_partial_on_error` / `test_methods_uses_spec_screening_model` | 0 outright; the spans sentence has no source (it is PRISMA's count) | **y** — a run with a chosen `model_name` per stage, and `audited` paper events (or `run_calls` with `paper_id`) | Depends on PRISMA: methods renders eight PRISMA keys. Commit order methods-after-PRISMA, or methods' PRISMA-derived numbers change twice. |
| **validator** | 8: the value checks (`test_valid_spans_no_issues`, `test_invalid_categorical_value_flagged`, `test_numeric_field_non_numeric_flagged`, `test_not_found_value_accepted`, four semicolon) | 1: `test_unknown_field_name_flagged` (no grid target); `test_site3_the_skip_is_wired_into_all_three_check_points` stays as a source pin if the names survive | **n** — `add_values` / `seed_claim` cover it (a `ReviewDatabase` is not needed: the validator reads only the codebook beside the db and the events) | Mechanical apart from the arm and a `None` guard for `missing` cells. |

---

## §5 Found, not asked

- `export_all` passes no `arm`, so `export_evidence_csv` / `_excel` / `export_evidence_docx`
  export their default `arm="local"` — the pre-manifest Run-6 arm — not
  `spec.extraction_models.arm`. A Run-7 pipeline export would render Run 6's (empty, R25) arm.
- `reject_paper` (`engine/core/database.py`) has **no engine or script caller** at HEAD; its only
  writer path to `REJECTED` / `rejected_reason` is tests. Live holds 0 `REJECTED` papers. So
  `papers_rejected`, `rejection_reasons` and `low_yield_rejected` are produced by nothing in the
  engine today.
- PRISMA counts `evidence_spans` in `verified` and `flagged` only; live holds 449 `contested`
  spans (with 2,159 `verified`, 1,152 `flagged`) that appear in neither box.
- `validate_prisma_counts` check 2b is tautological (`eligibility_excluded_total` is defined as
  the sum it is compared with).
- `export_prisma_csv` computes the flow twice (once inside `validate_prisma_counts`), and methods
  a third time; harmless, noted for whoever touches it.
- `_query_audit_models` counts `COUNT(DISTINCT es.extraction_id)` — extractions, not papers —
  while the extraction and FT counts beside it are papers.
- The validator's `main` constructs `ReviewDatabase(args.review)` on the live review to run a
  read-only diagnostic.

---

## Appendix A — `pytest --collect-only -q` lines at HEAD for the files cited

Standard-gate selection (`-m "not network and not ollama and not integration"`): 2,790 collected,
17 deselected. Every id in §1d, §2d, §3d, §4a appears below.

```
tests/test_exporters.py::test_prisma_flow_counts
tests/test_exporters.py::test_prisma_csv
tests/test_exporters.py::test_evidence_csv_columns
tests/test_exporters.py::test_evidence_excel_sheets
tests/test_exporters.py::test_docx_created
tests/test_exporters.py::test_methods_section_content
tests/test_exporters.py::test_methods_md_export
tests/test_exporters.py::test_export_all
tests/test_exporters.py::test_atomic_csv_no_partial_on_error
tests/test_exporters.py::test_atomic_docx_no_partial_on_error
tests/test_exporters.py::test_atomic_prisma_csv_no_partial_on_error
tests/test_exporters.py::test_atomic_methods_md_no_partial_on_error
tests/test_exporters.py::test_empty_extraction_has_marker
tests/test_exporters.py::test_exclude_empty_omits_empty_papers
tests/test_exporters.py::test_exclude_empty_excel
tests/test_exporters.py::test_methods_uses_spec_screening_model
tests/test_exporters.py::test_methods_uses_db_extraction_model
tests/test_exporters.py::test_methods_uses_db_audit_model
tests/test_exporters.py::test_methods_multi_model_ft_screening
tests/test_exporters.py::test_methods_placeholder_when_no_data
tests/test_extraction_validator.py::test_valid_spans_no_issues
tests/test_extraction_validator.py::test_unknown_field_name_flagged
tests/test_extraction_validator.py::test_invalid_categorical_value_flagged
tests/test_extraction_validator.py::test_numeric_field_non_numeric_flagged
tests/test_extraction_validator.py::test_not_found_value_accepted
tests/test_extraction_validator.py::test_closest_match_similarity
tests/test_extraction_validator.py::test_normalize_prefix_exact_match
tests/test_extraction_validator.py::test_normalize_prefix_unambiguous
tests/test_extraction_validator.py::test_normalize_prefix_ambiguous
tests/test_extraction_validator.py::test_normalize_prefix_no_match
tests/test_extraction_validator.py::test_semicolon_all_valid
tests/test_extraction_validator.py::test_semicolon_one_invalid
tests/test_extraction_validator.py::test_single_value_no_semicolons
tests/test_extraction_validator.py::test_semicolon_all_invalid
tests/test_extraction_validator.py::test_bleed_no_bleed
tests/test_extraction_validator.py::test_bleed_detected
tests/test_extraction_validator.py::test_bleed_invalid_for_all_fields
tests/test_extraction_validator.py::test_bleed_semicolon_multi_value
tests/test_extraction_validator.py::test_same_spec_same_hash
tests/test_extraction_validator.py::test_modified_codebook_different_hash
tests/test_low_yield.py::TestCountPopulatedFields::test_all_populated
tests/test_low_yield.py::TestCountPopulatedFields::test_with_absence_values
tests/test_low_yield.py::TestCountPopulatedFields::test_with_null_and_empty
tests/test_low_yield.py::TestCountPopulatedFields::test_empty_list
tests/test_low_yield.py::TestCountPopulatedFields::test_the_absence_set_is_the_codebooks
tests/test_low_yield.py::TestCountPopulatedFields::test_a_declared_value_and_an_undeclared_legacy_form_both_count
tests/test_low_yield.py::TestCheckLowYield::test_threshold_from_review_spec
tests/test_low_yield.py::TestPrismaLowYield::test_prisma_includes_low_yield_rejected
tests/test_low_yield.py::TestPrismaLowYield::test_prisma_no_low_yield_when_none_rejected
tests/test_low_yield.py::TestLowYieldSchema::test_extractions_has_low_yield_column
tests/test_low_yield.py::TestLowYieldSchema::test_low_yield_defaults_to_zero
tests/test_non_value_tokens_downstream.py::test_the_tokens_come_from_the_codebook
tests/test_non_value_tokens_downstream.py::test_an_incomplete_codebook_fails_the_reader_instead_of_emptying_it
tests/test_non_value_tokens_downstream.py::test_absence_sentinels_are_required_not_defaulted
tests/test_non_value_tokens_downstream.py::test_the_write_side_still_refuses_a_codebook_without_the_token
tests/test_non_value_tokens_downstream.py::test_site1_auditor_no_longer_flags_a_terminal_state[CONTRACT_UNMET]
tests/test_non_value_tokens_downstream.py::test_site1_auditor_no_longer_flags_a_terminal_state[NO_EVIDENCE_LOCATABLE]
tests/test_non_value_tokens_downstream.py::test_site1_a_real_value_is_still_audited
tests/test_non_value_tokens_downstream.py::test_site2_terminal_states_do_not_count_as_populated
tests/test_non_value_tokens_downstream.py::test_site2_refuses_the_retired_v1_dict_shape
tests/test_non_value_tokens_downstream.py::test_site3_a_terminal_state_never_reaches_the_rewrite_path
tests/test_non_value_tokens_downstream.py::test_site3_the_skip_is_wired_into_all_three_check_points
tests/test_non_value_tokens_downstream.py::test_site4_terminal_states_are_not_categorical_observations[CONTRACT_UNMET]
tests/test_non_value_tokens_downstream.py::test_site4_terminal_states_are_not_categorical_observations[NO_EVIDENCE_LOCATABLE]
tests/test_non_value_tokens_downstream.py::test_site4_real_values_and_absences_are_unaffected
tests/test_non_value_tokens_downstream.py::test_site4_manufactured_variance_would_have_masked_a_collapse
tests/test_non_value_tokens_downstream.py::test_site5_terminal_states_are_dropped_from_the_arm
tests/test_non_value_tokens_downstream.py::test_site5_a_field_the_codebook_does_not_declare_is_ignored
tests/test_prisma_reconciliation.py::TestReconciliation::test_reconciliation_passes_clean_db
tests/test_prisma_reconciliation.py::TestReconciliation::test_reconciliation_catches_mismatch
tests/test_prisma_reconciliation.py::TestReconciliation::test_in_progress_papers_counted
tests/test_prisma_reconciliation.py::TestPDFExcludedSubcounts::test_pdf_excluded_subcounts_sum
tests/test_prisma_reconciliation.py::TestNoDoubleCount::test_no_paper_in_multiple_terminal_boxes
tests/test_prisma_reconciliation.py::TestNoDoubleCount::test_ai_audit_complete_not_double_counted_with_ft
tests/test_prisma_reconciliation.py::TestExtractFailed::test_extract_failed_appears_in_flow_and_csv
tests/test_prisma_reconciliation.py::TestExtractFailed::test_extract_failed_zero_omitted_from_csv
```
