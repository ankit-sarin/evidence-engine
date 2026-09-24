# WRITE-PATH-01 Phase 1a — read-only design read-out

**Session 9 (S5a · S5b · S5d · S3f-min — the write path). Read-only.** No engine, test, migration,
spec or codebook edit. No model call. Live `review.db` was opened raw `mode=ro` for P9/P10 only;
no `ReviewDatabase` was constructed on live.

* Tree: HEAD `8297af3`, clean, at 2026-09-24T21:56:29Z.
* Live read window: P9/P10 at 22:01:12 UTC; closing `--compare` at 22:02:18 UTC against
  `docs/session-reports/input-identity-01/review_db_fingerprint_20260924T205225Z.json`: exit 0,
  IDENTICAL, 34 tables, overall `ea05912d…ce01`, `-wal` 0 B.
* CHECK and trigger text in P5 was read from `sqlite_master` of a **fresh** database built in the
  session scratchpad (`ReviewDatabase('fresh_wp01', data_root=<scratch>)`, 17 receipts, 34 tables),
  never from live and never from memory.
* Citations are by file path and a quoted content anchor. **MEASURED** = produced by a command
  named here; **READ** = from reading source; a claim from reading a function's head is READ.

---

## Ledger — I1–I6

| # | verdict | evidence |
|---|---|---|
| **I1** | **TRUE for the agent; raw SQL writes exist beside it** | `engine/agents/extractor.py` writes only through `db.add_extraction_atomic(` and `db.update_status(`. Its one raw statement is a READ: `"SELECT id FROM extractions WHERE paper_id = ? AND codebook_hash = ?"`. The elicited path also writes through `db.add_extraction_atomic(`. **Raw SQL writes on the post-extraction path:** `engine/agents/auditor.py::check_low_yield` issues `"UPDATE extractions SET low_yield = 1 WHERE id = ?"` and `"UPDATE extractions SET low_yield = 0 WHERE id = ?"` directly on `db._conn`; `scripts/reextract_failed.py` issues `"UPDATE papers SET status = 'PARSED', updated_at = datetime('now') WHERE id = ?"`. |
| **I2** | **TRUE** (≥ 2; 14 run-path readers listed in P2) | e.g. `scripts/run_pipeline.py::_stage_extract` `db.get_papers_by_status("PARSED")`; `extractor._run_extraction_unlocked` `db.get_papers_by_status("FT_ELIGIBLE")` + `("PARSED")`; `auditor.run_audit` `db.get_papers_by_status("EXTRACTED")`. |
| **I3** | **PARTLY FALSE** | The writer is called only from runner-level sites, **but a manifest IS written on the local extraction path when it runs through `run_pipeline`**: `scripts/run_pipeline.py::_open_run_manifest` → `rm.open_run(db._conn, spec, kind=kind, stages=stages, …)` with `"extract": ("extract_pass1", "extract_pass2", "extract_retry_snippet")` and `kind = "extraction"`, then `run_token = rm.activate(db._conn, run_id)` around every stage. The other call site is `scripts/run_cloud_extraction.py` (`handle = rm.open_run(`). No call from `engine/agents/`, `engine/elicitation/` or any screening module. `scripts/run5_extract_and_audit.py` and `scripts/reextract_failed.py` extract **without** a manifest. |
| **I4** | **FALSE** | Both kinds I4 expected to be missing are in the CHECK and in the reader. Fresh `sqlite_master`: `CHECK (event_type IN ('asserted', 'declined', 'contract_unmet', 'superseded', 'human_accepted', 'human_corrected', 'human_withdrew', 'duplicate_detected', 'citation_located', 'state_at_migration'))`. `engine/core/effective.py`: `CONTRACT_UNMET = "contract unmet"         # row 15`, `UNRESOLVED_DUPLICATE = "unresolved (duplicate values)"  # row 6`. All eight S5b states are derivable with no migration (P5). There is no "located/not-located" event *kind*; location is one kind, `citation_located`, whose `payload_json.$.located` is required non-NULL by CHECK. |
| **I5** | **PARTLY TRUE** | Completeness accepts duplicated and unexpected fields: TRUE (`check_completeness`: `"extra or duplicated fields are reported but do not by themselves fail the check"`; re-measured, P6). The budget is split: the **constant** `MAX_COMPLETENESS_ATTEMPTS = 3` is defined in `engine/core/completeness.py`; the **loop** is in `engine/agents/extractor.py::extract_paper_with_completeness` (`for attempt in range(1, max_attempts + 1):`) and in `engine/cloud/base.py` (`max_attempts: int = MAX_COMPLETENESS_ATTEMPTS`). |
| **I6** | **FALSE** | The spec model has `class ExtractionModels(_UnsentOptions):` (`engine/core/review_spec.py`), mounted as `extraction_models: ExtractionModels = Field(default_factory=ExtractionModels)`, and the live spec carries the block: `review_specs/surgical_autonomy.yaml` `extraction_models:` / `extractor: "deepseek-r1:32b"` / `pass1_think: true` / `pass2_think: false` / `temperature: 0` / `elicitation: false` / `arm: local_deepseek_r1_32b`. The four extraction stages resolve from it (`effective_config.stage_config`: `blk_src, blk = _block(spec, "extraction_models", ExtractionModels)`). **Plan R6's premise (S5d: "The live spec gains an extraction_models block") is already met on disk.** |

---

## P1 — Extractor write inventory

Scope: the local extraction path, meaning `engine/agents/extractor.py`, `engine/elicitation/pipeline.py`, the
`ReviewDatabase` methods they call, `run_pipeline`'s extraction and audit stages, and the auditor's
post-extraction writes. Grouped by table.

### `extractions`
| function | statement | fires |
|---|---|---|
| `ReviewDatabase.add_extraction_atomic` ← `extractor.extract_paper` (legacy) | `INSERT INTO extractions (paper_id, extraction_schema_hash, extracted_data, reasoning_trace, model, model_digest, auditor_model_digest, extracted_at, codebook_hash, codebook_sha256)` inside `BEGIN` … `COMMIT` | once per paper per **successful** attempt, after `enforce_completeness` and `enforce_citations` pass. `schema_hash=None` (retired column). `reasoning_trace` = Pass-1 thinking trace. `model_digest`, `auditor_model_digest` from `_run_extraction_unlocked`'s own `fetch_model_digest(...)` calls. |
| same ← `pipeline.extract_paper_elicited` | same INSERT | once per paper per successful attempt, after `enforce_terminal_states`, `enforce_completeness`, `enforce_citations(mode=STRICT)`. `extracted_data` entries carry `"terminal_state"`; `reasoning_trace=priming` (`"the materialized evidence IS the trace"`). |
| `auditor.check_low_yield` (post-audit) | raw `UPDATE extractions SET low_yield = 1 / 0 WHERE id = ?` on `db._conn`, one `commit()` at the end | per `AI_AUDIT_COMPLETE` paper, on its latest extraction (`ORDER BY id DESC LIMIT 1`), every audit run. |

### `evidence_spans`
| function | statement | fires |
|---|---|---|
| `add_extraction_atomic` | `INSERT INTO evidence_spans (extraction_id, field_name, value, source_snippet, confidence, tier, audit_status) VALUES (…, 'pending')` | **one per span in the list handed to it**, same transaction as the extraction. Legacy path: every Pass-2 span, **including duplicated and unexpected fields** (completeness only warns; P6). Elicited path: one per expected field in prompt order; a `CONTRACT_UNMET` or escape field is stored as a token span with `source_snippet` empty. |
| `ReviewDatabase.update_audit` ← `auditor.run_audit` | `UPDATE evidence_spans SET audit_status = ?, auditor_model = ?, audit_rationale = ?, audited_at = ? WHERE id = ?` + `commit()` | per pending span, per `EXTRACTED` paper, one statement each. |

### `papers` (`status`, `updated_at`)
| function | statement | fires |
|---|---|---|
| `update_status` ← `extractor._run_extraction_unlocked` | `UPDATE papers SET status = ?, updated_at = ? WHERE id = ?` under `BEGIN IMMEDIATE`, after an `ALLOWED_TRANSITIONS` check | `"EXTRACTED"` after `extract_paper_with_completeness` returns; `"EXTRACT_FAILED"` in the `except Exception` branch (every failure, including exhausted retries and `InputFitError`). Once per paper. **Not** on the pre-extraction skips (`NoParsedText`, `ParsedTextError`, already-extracted): those write nothing. |
| `update_status` ← `auditor.run_audit` | same | `"AI_AUDIT_COMPLETE"` when no pending span remains on the latest extraction. |
| no other column | — | Nothing on the extraction path writes any other `papers` column. |

### `workflow_state`
| function | statement | fires |
|---|---|---|
| `workflow.complete_stage` ← `scripts/run_pipeline.py` (after `_stage_audit`) | `UPDATE workflow_state SET status = 'complete', completed_at = ?, metadata = ? WHERE stage_name = ?` | `EXTRACTION_COMPLETE` if any paper is at `('EXTRACTED', 'AI_AUDIT_COMPLETE', 'HUMAN_AUDIT_COMPLETE')`; `AI_AUDIT_COMPLETE_STAGE` if any at `('AI_AUDIT_COMPLETE', 'HUMAN_AUDIT_COMPLETE')`. Both inside `try: … except Exception: pass  # workflow table may not exist`. **The extractor itself writes no `workflow_state`.** |

### Everything else the path writes
| target | writer | fires |
|---|---|---|
| `run_manifests`, `run_stage_configs`, `arms` (pin) | `run_manifest.open_run` ← `run_pipeline._open_run_manifest` | once per `run_pipeline` run, before the first call; pins `extraction_models.arm` for kind `extraction`. `close_run` UPDATEs `ended_at`/`end_status` once. |
| `run_calls` | `ollama_client.ollama_chat` → `record_active_ollama_call` → `record_call` | once per Ollama call **only while `rm.activate` is in force** (i.e. under `run_pipeline`). The three legacy extractor calls pass no `paper_id` (`ollama_chat(messages=pass1_messages(prompt), **cfg.kwargs())`), so their `run_calls.paper_id` would be NULL; the elicited Pass-1 call passes it. |
| telemetry JSONL (file) | `extraction_telemetry.record_call` ← `extract_paper_with_completeness` | once per attempt: `contract_retry` / `incomplete_retry` / `*_exhausted` / `stored`. |
| unit-map JSON (file) | `pipeline.persist_unit_map` | elicited path, once per paper per run, under `data/<review>/elicitation/<run_id>/unit_maps/`, where this `run_id` is the string `_default_run_id()` → `"run_%Y%m%dT%H%M%SZ"`, **not** a manifest id. |

**Two-pass structure and priming.** Legacy path: Pass 1 (`extract_pass1_reasoning`) returns the
thinking trace; Pass 2 (`extract_pass2_structured`) is primed with it in-memory
(`"Here is your prior analysis of this paper:"`). Nothing is written between the passes; the trace
reaches the database only as `extractions.reasoning_trace` in the final INSERT. The snippet retry
(`_retry_snippet`, `SNIPPET_MAX_RETRIES = 2` per invalid snippet) also writes nothing. The elicited path is the same shape:
only the unit map is written before Pass 2, as a file.

---

## P2 — Run-path readers of what P1 writes

Sites outside `analysis/` and the six migrated readers that read `papers.status`, `workflow_state`,
`extractions` or `evidence_spans` **to decide what to do next**. Located by grep, then each site read.
Upstream stages (search, screen, acquire, parse) read `papers.status` for their own tokens, which
P1 does not write, and are excluded.

| # | site | reads | decision made |
|---|---|---|---|
| 1 | `scripts/run_pipeline.py::_stage_extract` | `get_papers_by_status("PARSED")` | skip the whole extract stage if none. **Narrower than the pickup below**: an `FT_ELIGIBLE`-only review is skipped here. |
| 2 | `engine/agents/extractor.py::_run_extraction_unlocked` (M1) | `get_papers_by_status("FT_ELIGIBLE")` + `("PARSED")` | which papers are extracted. |
| 3 | same, per paper | `SELECT id FROM extractions WHERE paper_id = ? AND codebook_hash = ?` | skip "already extracted with current schema". |
| 4 | same, pre-flight | `extraction_cleanup.check_stale_extractions` (reads `extractions`) | informational log only. |
| 5 | `scripts/run_pipeline.py::_stage_audit` | `get_papers_by_status("EXTRACTED")` | skip the audit stage if none. |
| 6 | `engine/agents/auditor.py::run_audit` | `get_papers_by_status("EXTRACTED")`; latest `extractions` row; `evidence_spans … audit_status = 'pending'`; pending count | which papers and spans are audited; whether to advance to `AI_AUDIT_COMPLETE`. |
| 7 | `engine/agents/auditor.py::check_low_yield` | `get_papers_by_status("AI_AUDIT_COMPLETE")`; `extractions.extracted_data` | set `low_yield`. |
| 8 | `scripts/run_pipeline.py` (after audit) | `COUNT(*) FROM papers WHERE status IN (…)` | whether to `complete_stage` the two extraction workflow stages. |
| 9 | `scripts/run_pipeline.py` gates | `workflow.is_adjudication_complete` / `is_audit_review_complete` / `get_current_blocker` (→ `workflow_state`) | block post-screening stages; block export. |
| 10 | `engine/agents/ft_screener.py` (FT hand-off) | `get_papers_by_status("PARSED") + ("AI_AUDIT_COMPLETE")`; `paper["status"]` against `_PAST_FT = {"FT_ELIGIBLE", … "EXTRACTED", "EXTRACT_FAILED", "AI_AUDIT_COMPLETE", …}`; verifier reads `get_papers_by_status("FT_ELIGIBLE")` | which papers to screen, and whether a decision may move status; writes `FT_ELIGIBLE`, which is what #2 picks up. |
| 11 | `engine/adjudication/audit_adjudicator.py` (`_collect_papers_for_review`, `import_audit_review_decisions`, `_import_legacy_format`, `check_audit_review_gate`) | `AI_AUDIT_COMPLETE` papers, latest `extractions`, `evidence_spans.audit_status` | build the human audit queue; apply decisions to spans; transition to `HUMAN_AUDIT_COMPLETE`; gate count. (CLAUDE.md: the human-audit importer onto `field_events` is session 12.) |
| 12 | `engine/review/human_review.py` (`export_review_queue`, `_apply_audit_decisions`, `bulk_accept`) | `status = 'AI_AUDIT_COMPLETE'`; spans via latest extraction | queue; resolve spans; raw `UPDATE papers SET status = 'HUMAN_AUDIT_COMPLETE'`. |
| 13 | `engine/review/extraction_audit_html.py::_query_review_spans` | `p.status = 'AI_AUDIT_COMPLETE'` ⋈ `evidence_spans.audit_status IN (…)` | which spans enter the HTML audit sheet. |
| 14 | CLI / status | `advance_stage --status` → `format_workflow_status` (`workflow_state`); `ReviewDatabase.get_pipeline_stats` (`papers.status` counts, `extractions`, `evidence_spans`, printed by `run_pipeline`'s `finally`); `scripts/monitor_extraction.py` (`status = 'EXTRACTED'`) | display only. |

Reporting readers, which make no progression decision but will read empty tables once the writers retire:
`engine/exporters/prisma.py` (status counts, `evidence_spans.audit_status`),
`engine/exporters/methods_section.py` (`extractions.model`, `evidence_spans.auditor_model`),
`engine/validators/extraction_validator.py::validate_all` (`statuses=("EXTRACTED", …)` ⋈ spans),
`engine/analysis/concordance.py::check_schema_parity` (`SELECT DISTINCT codebook_hash FROM
extractions`. Kept deliberately under R31, per its comment: `"this KEEPS its legacy read, deliberately"`).
The cloud pickup (`engine/cloud/base.py::get_pending_papers`) already takes the corpus from
`corpus_id_sql` (the eligibility axis), but excludes by `cloud_extractions`.
**No Gradio app exists in this repo** (no `import gradio` outside `venv/`).

---

## P3 — Selection today, the corpus predicate, and whether "already asserted under this text" is answerable

* **Pickup (READ):** `extractor._run_extraction_unlocked`: `ft_papers = db.get_papers_by_status("FT_ELIGIBLE")`,
  `parsed_papers = db.get_papers_by_status("PARSED")`, `papers = ft_papers + parsed_papers`;
  skip `"SELECT id FROM extractions WHERE paper_id = ? AND codebook_hash = ?"`.
  `run_pipeline._stage_extract`: `parsed = db.get_papers_by_status("PARSED")` then
  `if not parsed: … return {"extracted": 0, "elapsed": 0}`. On live all 190 are `AI_AUDIT_COMPLETE` (row D9), so both return 0.
* **`engine/core/corpus.py`** is FROZEN (R35): `corpus_status_sql(column: str = "status") -> tuple[str, tuple[str, ...]]`
  returns an `<column> IN (?, …)` fragment over `CORPUS_STATUSES = ("FT_ELIGIBLE", "EXTRACTED",
  "AI_AUDIT_COMPLETE", "HUMAN_AUDIT_COMPLETE")`. It reads the **legacy `papers.status`**, not an axis;
  `is_corpus_member(status)` is the boolean form. The live predicate is
  `effective.eligible_paper_ids(conn) -> tuple[int, ...]` and `corpus_id_sql(conn, column)`, which use
  the **eligibility axis** of `paper_events`.
* **`reuse_key(arm_id: str, paper_id: int, parsed_text_sha256: str) -> str`**, pure, `rk1:` + sha256
  of `canonical_json({"arm", "paper_id", "parsed_text_sha256"})`. No production caller (grep: only
  its own module and `tests/`).
* **Can `field_events` answer "this arm already asserted this paper under this text"? No, not as the
  table stands.** The fresh `field_events` DDL carries `claim_id`, `extraction_uid`, `paper_id`,
  `field_name`, `arm`, `value`, `source_snippet` and `payload_json`. It has no parsed-text hash, no
  `parsed_text_uid`, no reuse key, and no link to `run_calls` or `parsed_text_refs`.
  `extraction_uid` is `mint_extraction_uid()` → `str(uuid.uuid4())`: `"Nothing is derived from
  content"`. `claim_id` is `f"{arm}:{extraction_uid}:{field_name}"`. **Missing:** a record, per claim
  or per extraction, of the parsed text it was made from: the sha256, the `parsed_text_uid`, or the
  reuse key itself. `payload_json` is free-form, so the key *could* ride there without a migration.
  It would then be unindexed and unconstrained, and no writer puts it there today (fork F2).
* **Second missing input:** the extractor loads text by `load_parsed_text(db._conn, pid)`, which
  returns a `str`. The hash lives on the `ParsedTextRef` from `resolve_parsed_text`, which the
  extractor never holds.

---

## P4 — The event writer

**Signatures (READ, `engine/core/events.py`):**

```
write_field_event(conn, *, event_type, paper_id, field_name, arm,
                  claim_id=None, extraction_uid=None, value=None, source_snippet=None,
                  actor_kind, actor_role, actor_name, actor_digest=None, occurred_at=None,
                  run_id=_MISSING, run_marker=None, prior_event_id=None,
                  presented_context_sha256=None, reason=None, payload=None,
                  against_claims=(), against_decisions=(), sentinels=frozenset(),
                  commit=True) -> int
write_paper_event(conn, *, event_type, paper_id, to_state, from_state=None,
                  actor_kind, actor_role, actor_name, actor_digest=None, occurred_at=None,
                  run_id=_MISSING, run_marker=None, prior_event_id=None,
                  presented_context_sha256=None, reason=None, reason_code=None,
                  stage_name=None, payload=None, commit=True) -> int
```

Required, with no default: `event_type`, `paper_id`, `field_name`, `arm` and the three `actor_*` for field
events; `event_type`, `paper_id`, `to_state` and the three `actor_*` for paper events. `run_id` is
required in effect: `_MISSING` or `None` raises unless the caller is a migration passing
`'pre-manifest'`. The writer mints `claim_id` from `arm`/`extraction_uid`/`field_name` when none is given, and
stamps `payload.state_at_write` from `classify_field_state` (the reader's function, imported).

**Refusals (R74, the module docstring's list, each checked in code):** `ReviewerDecisionAmbiguous` (R20),
`AcceptAgainstMultipleClaims` (R20), `AgainstReferenceIncomplete` (R24), `CellNotAssigned` (R22-U2),
`ArmConfigurationFrozen` (R21), `RunLinkRefused` (R68), `ClaimOnPreManifestArm` (R59),
`ClaimOnRetiredArm` (R21), `ArmNotInRun` (R10: `SELECT 1 FROM run_stage_configs WHERE run_id = ?
AND arm_name = ?`). Also enforced by the schema: the `event_type` CHECK, the run-link CHECK, the
`citation_located` payload CHECK, the `arm` FK to `arms`, and the append-only triggers
`field_events_no_update` / `field_events_no_delete` (P5).

**Run linkage (M4):** `_run_link` checks `SELECT 1 FROM run_manifests WHERE run_id = ?`. A caller
obtains a `run_id` only from `run_manifest.open_run(...)` → `RunHandle.run_id` (int) or
`open_review_session`. **Call sites (I3):** `scripts/run_pipeline.py::_open_run_manifest` and
`scripts/run_cloud_extraction.py`. Nowhere else in `engine/`, `scripts/` or `analysis/`.

**Can a local extraction run obtain a run_id today without new code? No.** Under `run_pipeline`
the manifest id exists in-process: `run_id = _open_run_manifest(db, spec, start_idx)` pins
`extraction_models.arm`, so `ArmNotInRun` would pass, and `rm.activate` stores it in the private
contextvar `_ACTIVE`. But nothing hands it to `run_extraction` → `extract_paper`. That function's
`run_id: str | None` parameter is the unit-map directory string from `_default_run_id()`, not a
manifest id, and the module has no public accessor for `_ACTIVE`. So `write_field_event` cannot be
called from the extractor with a valid `run_id` unless new code is added to pass it through.
`run5_extract_and_audit.py` and `reextract_failed.py` open no manifest at all.

---

## P5 — S5b representability

The reader is `engine/core/effective.py::effective_value`: first matching row wins, rows 0–15 per
cell. It is `_classify_claim` for rows 10–15.

| S5b state | produced today (reader) by | status |
|---|---|---|
| asserted with evidence | an `asserted` event plus a `citation_located` event on the same `claim_id` whose payload has `located` truthy → rows **10** (value) / **12** (absence sentinel) | **derivable.** The reader and CHECK are present. **No writer and no locator exist**: nothing in `engine/` writes `citation_located` (R17's "one shared deterministic locator" is not built). |
| asserted without locatable evidence | `asserted` with no located citation → rows **11** / **13** | **derivable.** Present in the reader and CHECK; no production writer. |
| declined | `declined` → row **14** | **derivable.** Present; no writer. |
| contract unmet | `contract_unmet` → row **15**, provenance `payload.violation_codes`, `payload.attempts` | **derivable.** In the CHECK and the reader; no writer. (The elicited path stores the `CONTRACT_UNMET` token in `evidence_spans.value` today.) |
| missing | assigned cell with no field event → row **1** `"assigned, no claim"`; or every claim superseded → row **1** `"assigned, no live claim"`. **"Not assigned"** is row **0** `OUT_OF_SCOPE`, from `is_assigned`, which returns `row[0] == "model"`: every model arm is assigned the full corpus, and a `human_extractor` arm nothing until session 12. `OUT_OF_SCOPE` is marked `"NOT an S5b state (R13)"`. | **present.** |
| unresolved (duplicate or needs re-review) | duplicate → row **6**: fires when one live `claim_id` carries `asserted` events with more than one distinct value (`for cid in live: vals = [… e.claim_id == cid]; if len(set(vals)) > 1`). Re-review → rows **2** (conflicting reviewer decisions), **3** (the reviewer's against-set is no longer live), **7** (more than one live claim on a **pre-manifest** arm only). | **derivable, with a condition.** Row 6 is **per claim_id**. Two *different* live claims on a pinned model arm do not fire row 6 or row 7; they fall through to `_newest_claim_event`, **newest wins, silently**. `duplicate_detected` is permitted by the CHECK but **read by nothing** in `effective.py`. See fork F4. |
| corrected by human | `human_corrected` reviewer event whose against-set equals the live set → row **5** | **present** (reviewer path; the importer is session 12). |
| withdrawn | `human_withdrew`, same condition → row **4** | **present.** |

**Migration verdict: none of the eight states needs a new column, CHECK or table.** Every event
kind the table relies on is already admitted by the fresh-database CHECK:

```
CHECK (event_type IN ('asserted', 'declined', 'contract_unmet', 'superseded', 'human_accepted',
       'human_corrected', 'human_withdrew', 'duplicate_detected', 'citation_located',
       'state_at_migration')),
CHECK (event_type <> 'citation_located'
       OR json_extract(payload_json, '$.located') IS NOT NULL),
CHECK (
    (run_id IS NOT NULL AND run_marker IS NULL)
    OR
    (run_id IS NULL AND run_marker IS 'pre-manifest')
)
```

and `CHECK (actor_role <> 'system' OR actor_kind = 'engine')`, with `actor_kind IN ('model', 'human',
'engine')` and `actor_role IN ('reviewer', 'extractor', 'system')`. Append-only triggers:
`field_events_no_update` / `field_events_no_delete` → `RAISE(ABORT, 'field_events is append-only:
correct by appending an event')`. A **reader** change, not a migration, is needed only if the
duplicate is represented as two claims or as a `duplicate_detected` event (F4). The reuse-key
question (P3) is separate: it is a migration only if the key becomes a column.

---

## P6 — Completeness and the retry budget

* **Duplicated / unexpected (READ):** `check_completeness` computes `missing`, `unexpected` and
  `duplicated`; `complete=not missing`. `enforce_completeness` raises only on `missing` and
  otherwise logs `"extraction complete but irregular — %s"`. On the legacy path the spans list is
  then stored **as is**, so duplicated and unexpected fields reach `evidence_spans`. On the elicited
  path, Pass 1's `check_response` builds `by_name = {str(e.get(KEY_FIELD)): e for e in entries …}`: a
  duplicated entry is **overwritten, last wins, silently**, and unknown names are **reported** in
  `Pass1Result.unknown_fields` and never stored. Pass 2's `pass2_values[span.field_name] = span` is
  the same dict overwrite, and only the `order` fields are emitted. So S5a's "unexpected fields
  dropped and logged" holds only on the elicited Pass-1 side today.
* **D1-8 reproducer:** DISCOVERY-01 Part A §"D1-8 — Completeness guard" (b). It was an ad hoc
  temp-review run, **never committed as a script**. **Re-run MEASURED this session** in-process on the same
  payload (a pure function, no database): `check: True ('study_type',) ('not_in_the_codebook',)`,
  `enforce returned: True`, plus the WARNING line. It still reproduces. Part A's per-paper duplicate
  census was never run. P9 bounds it: no corpus paper has more than 20 spans.
* **Retry budget:** outer loop `extractor.extract_paper_with_completeness`, `max_attempts =
  MAX_COMPLETENESS_ATTEMPTS` = **3** (constant in `completeness.py`), with `RETRYABLE =
  (IncompleteExtractionError, UncitedValueError)`. `TerminalStateError` subclasses the former. It
  re-issues the identical request (`"Re-issuing identical request."`); exhaustion re-raises and
  `_run_extraction_unlocked` sets `EXTRACT_FAILED`. The cloud path mirrors it in
  `CloudExtractorBase.extract_with_completeness`. Inner loop (elicited only):
  `MAX_PASS1_ATTEMPTS = 2  # Ruling 4`, with typed feedback, accepted by strict inequality. Per
  snippet: `SNIPPET_MAX_RETRIES = 2`. Ollama transport: `ollama_chat` `max_retries` default.
  `extractor.MAX_RETRIES = 2` and `RETRY_DELAY = 30` are **defined and referenced nowhere** (grep
  across `engine/`, `scripts/`, `analysis/`).
* **What "contract unmet" does today:** *elicited:* a field failing its class contract takes the
  `CONTRACT_UNMET` terminal state and is stored as that token in `evidence_spans.value` with no
  snippet; the paper stores. *Legacy:* there is no per-field contract-unmet outcome. An uncited
  non-sentinel value raises `UncitedValueError`, which enters the outer budget of 3, and exhaustion
  fails the **paper** (`EXTRACT_FAILED`).
* **`tests/test_completeness_guard.py::test_unexpected_and_duplicate_fields_reported_but_not_fatal`**
  as written: `spans = _complete(expected) + [{"field_name": "Title", …}, {"field_name": "country",
  "value": "dup", …}]`; asserts `r.complete is True`, `r.unexpected == ("Title",)`,
  `r.duplicated == ("country",)`. **MEASURED:** 1 passed. It pins the current behaviour by name,
  so S5a's change turns it red by design.

---

## P7 — Elicitation / extraction configuration path, spec → `ollama_chat`

**Does an `extraction_models` block exist? Yes**, in both the spec model and the live spec (I6).

Path: `review_specs/surgical_autonomy.yaml` `extraction_models:` → `ReviewSpec.extraction_models:
ExtractionModels` → `effective_config.stage_config(stage, spec)` → `EffectiveConfig.kwargs()` →
`ollama_chat(messages=…, **cfg.kwargs())` → `_client.chat(model=…, messages=…, **kwargs)`.

**Set/override sites on the path: 12** (READ, enumerated):

| # | where | what |
|---|---|---|
| 1 | `review_spec.ExtractionModels` defaults | `extractor="deepseek-r1:32b"`, `pass1_think=True`, `pass2_think=False`, `retry_think=False`, `temperature=0`, `elicitation=False`, `arm=None`; `_UnsentOptions` `seed=None`, `num_ctx=None` ("None = not sent (R66)") |
| 2 | `review_spec.OllamaRuntime.keep_alive` default `-1` | `keep_alive` for every stage |
| 3 | live spec YAML | sets `extractor`, `pass1_think`, `pass2_think`, `temperature`, `elicitation`, `arm`; **does not set** `seed`, `num_ctx`, `retry_think`, `ollama.keep_alive` |
| 4 | `stage_config` extraction branch | `chosen = blk.extractor`; `options["temperature"] = blk.temperature`; `think` from `{"extract_pass1": "pass1_think", "elicitation_pass1": "pass1_think", "extract_pass2": "pass2_think", "extract_retry_snippet": "retry_think"}`; `sent |= {"think"}`; `format` only for `extract_pass2` |
| 5 | `stage_config._unsent` | `seed`/`num_ctx` enter `options` only if the spec declares them; otherwise recorded `UNSENT`, source `modelfile_or_server` |
| 6 | `stage_config._format` | `extract_pass2` → `ExtractionOutput.model_json_schema()` (no spec input) |
| 7 | `stage_config(model=…)` | caller model override, source `caller` (not used on the extraction path) |
| 8 | `extractor._with_think(cfg, think)` | a `think=` argument overrides, source `caller` |
| 9 | `extract_pass1_reasoning(prompt, think=None, *, cfg=None)` | fallback `stage_config("extract_pass1")` **without spec** (declared defaults) when no `cfg` is passed |
| 10 | `extract_pass2_structured(… cfg=None)` | fallback `stage_config("extract_pass2", spec)` |
| 11 | `_retry_snippet(… cfg=None)` | fallback `stage_config("extract_retry_snippet")` **without spec** |
| 12 | digest | set **twice, independently**: `resolve_run(…, digest_fn=…)` for `run_stage_configs.model_digest` at manifest open, and `_run_extraction_unlocked` `fetch_model_digest(extractor_model)` for `extractions.model_digest` |

`extract_paper` and `extract_paper_elicited` always pass spec-resolved `cfg`s (sites 9–11's
fallbacks are not reached from them). `ollama_chat` consumes `stage` and **changes nothing it
sends** (`"The guard reads messages and options and changes nothing it sends"`). The input-fit
guard only reads `options.num_ctx` to compute the ceiling.

---

## P8 — Legacy-table dependency in tests

**Method (MEASURED locate, READ classify):** grep across `tests/` for constructors and consumers of
`extractions`, `evidence_spans`, extraction-stage `papers.status` tokens, and `workflow_state`. Then
each file's matching lines were read and classified by the code under test. Excluded: analysis
fixtures (`tests/analysis/paper1/*`, `test_adjudication_pairs`, `test_concordance_pipeline`,
`test_exporters`, `test_prisma_reconciliation`, `test_db_backup`, the reader and event-store
tests), migration tests, and upstream-stage tests (screen, acquire, parse, PDF), which walk
`papers.status` only to `PARSED` or earlier.

**18 test files** construct legacy rows as fixtures for run-path behaviour:
`test_extractor.py`, `test_auditor.py`, `test_low_yield.py`, `test_codebook_staleness.py`,
`test_extraction_cleanup.py`, `test_atomic_terminal_write.py`, `test_codebook_provenance.py`,
`test_ollama_client.py` (digest columns via `add_extraction_atomic`), `test_database.py` (133
matching lines), `test_human_review.py`, `test_audit_adjudication.py`, `test_workflow.py`,
`test_ft_screening.py`, `test_retry_failed.py` (imports `run5_extract_and_audit.reset_failed_papers`),
`test_request_capture.py` (drives `extract_paper` on a scratch database), `test_run_manifest.py`,
`test_cloud_extraction.py`, `test_extraction_validator.py`.
**Plus 2** that couple to the writer by a **fake** `add_extraction_atomic` without constructing rows
(`test_citation_guard.py`, `test_elicitation_pipeline.py`), and **2 shared helpers**
(`tests/_corpus_fixture.py`, `tests/_event_store_fixture.py`). The classification is a judgment and
is labelled READ.

---

## P9 — Legacy extraction telemetry, live (raw `mode=ro`, 22:01:12 UTC)

Corpus = the 190 papers whose latest eligibility-axis `paper_events` row is `eligible` (mirrors
`effective.eligible_paper_ids`). SQL (`p9_p10.sql`, run as `sqlite3 "file:…/review.db?mode=ro" <
p9_p10.sql`; the temp views live in the connection's temp schema):

```sql
CREATE TEMP VIEW corpus AS
SELECT pe.paper_id FROM paper_events pe
WHERE pe.to_state IN ('eligible','abstract_out','full_text_out')
  AND pe.event_id = (SELECT MAX(e2.event_id) FROM paper_events e2
                     WHERE e2.paper_id = pe.paper_id
                       AND e2.to_state IN ('eligible','abstract_out','full_text_out'))
  AND pe.to_state = 'eligible';
CREATE TEMP VIEW per_paper AS
SELECT c.paper_id,
  (SELECT COUNT(*) FROM extractions e WHERE e.paper_id = c.paper_id) AS n_ext,
  (SELECT COUNT(*) FROM evidence_spans s JOIN extractions e ON e.id = s.extraction_id
     WHERE e.paper_id = c.paper_id) AS n_spans,
  (SELECT COUNT(*) FROM extractions e WHERE e.paper_id = c.paper_id
     AND e.reasoning_trace IS NOT NULL AND length(e.reasoning_trace) > 0) AS n_trace
FROM corpus c;
-- min/max/sum, medians (LIMIT 2 OFFSET (n-1)/2), GROUP BY value, trace counts, table totals
```

| metric | min | median | max | total |
|---|---|---|---|---|
| extractions per corpus paper | 1 | 1 | 1 | 190 |
| evidence_spans per corpus paper | 1 | 20 | 20 | 3,760 |

Spans-per-paper distribution: **20 → 186 papers; 19 → 2 (papers 121, 458); 1 → 2 (papers 415,
719)**, all `deepseek-r1:32b`. Papers 415 and 719 are the two collapsed local extractions INSTRUMENT-01
names. Papers with a stored `reasoning_trace`: **190 / 190** (190 rows). Reconciliation: the
whole tables hold 190 `extractions` and 3,760 `evidence_spans`, equal to the corpus sums.

## P10 — `'No comparison reported'`, live (same connection)

`SELECT COUNT(*) FROM evidence_spans WHERE value = 'No comparison reported'` → **131**.
`… GROUP BY field_name` → **`comparison_to_human`: 131** (one field only). Recorded for Phase 1b; not interpreted.

---

## What Phase 2 must decide (forks; options as CC sees them, no recommendation)

* **F1 — Selection predicate, and what holds R19 once it moves.** (a) keep legacy status pickup
  until retirement; (b) eligibility axis (`eligible_paper_ids`) + reuse-key skip; (c) eligibility
  axis + processing axis. **Today R19 holds structurally because the status pickup returns 0.**
  Under (b) with `field_events` empty, the reuse-key skip skips nothing, so the first run would select
  all 190. Enforcing R19 then needs an explicit guard: a refusal, a flag, or the spec.
* **F2 — Where the reuse key or parsed-text identity lives on a claim.** (a) `payload_json`: no
  migration, but unindexed and unconstrained; (b) a `field_events` column: a migration and a
  rebuild of an append-only table; (c) a side table keyed by `extraction_uid`: a migration, with the
  events table untouched; (d) derive it through `run_calls`, which today carries no parsed-text
  hash either. Separately, the extractor must hold a `ParsedTextRef`, not the `str` from
  `load_parsed_text`.
* **F3 — run_id plumbing and runners without a manifest.** (a) pass `RunHandle.run_id` down
  `run_extraction` → `extract_paper`; (b) add a public accessor for the active-run contextvar;
  (c) have `run_extraction` open its own manifest. The name collision needs resolving too:
  `extract_paper(run_id: str)` is the unit-map directory. Decide the fate of `run5_extract_and_audit.py`,
  `reextract_failed.py` and the ELICIT-DESIGN-01 smoke (no manifest): refuse, open one, or retire.
* **F4 — Duplicate representation (S5a "both values retained as claims").** (a) one `claim_id`
  with two `asserted` events, so row 6 fires as written; (b) two `claim_id`s, which row 6 does not see:
  on a pinned model arm newest wins silently, so this needs a reader change; (c) a
  `duplicate_detected` event, which the CHECK admits but no reader rule reads, also a reader change.
* **F5 — A duplicated field becomes retryable.** Change `enforce_completeness` to raise, or add a
  separate predicate. Decide whether it shares the outer budget of 3 (S5a says "same retry budget as a missing
  one"). The elicited path's two silent last-wins overwrites (`by_name`, `pass2_values`) must
  surface too. `test_unexpected_and_duplicate_fields_reported_but_not_fatal` turns red by design.
* **F6 — Unexpected fields.** The legacy path stores them today; S5a says drop and log. Decide
  where: at parse, at the completeness boundary, or in the event mapper.
* **F7 — Contract unmet on the legacy (non-elicited) path.** (a) only the elicited path emits
  `contract_unmet` events, and the legacy path keeps failing the paper on an exhausted
  `UncitedValueError`; (b) the legacy path maps exhaustion to per-field `contract_unmet`.
* **F8 — "Asserted with evidence" needs a locator.** No `citation_located` writer exists (R17).
  (a) build the shared deterministic locator in session 9; (b) assert without locating, so values read
  as "asserted without locatable evidence" until it lands. Also decide how the elicited path's
  anchored-by-construction snippet maps to `located`.
* **F9 — Paper events from extraction outcomes.** Map exceptions to `paper_events.to_state`:
  `extracted`; `extraction_failed` with a NOT NULL `reason_code`; `input_exceeds_context` for
  `InputFitError`. The reason-code vocabulary is undecided. Pre-extraction skips write nothing today.
* **F10 — Order of re-pointing the P2 readers against retiring the writers.** The auditor reads
  `EXTRACTED` + pending `evidence_spans` and writes `low_yield`. When the extractor stops writing
  those tables, the auditor, the `run_pipeline` gates and auto-advance, and the human-audit tools read
  nothing. Options: (a) re-point in session 9; (b) dual-write for an interval; (c) retire and
  accept that audit is unavailable until re-pointed (the human-audit importer is session 12).
* **F11 — Hard cut-over or dual-write** for `extractions` / `evidence_spans` / `papers.status`
  (R67/R83 say the writes retire; the interval is not specified).
* **F12 — Which digest an event carries.** `actor_digest` could come from the manifest's resolved
  stage digest or from the separate `fetch_model_digest` in `_run_extraction_unlocked`. Today two
  independent fetches can disagree.
* **F13 — `paper_id` on `run_calls`.** The three legacy extractor calls pass none (NULL rows);
  decide whether session 9 passes it.
* **F14 — Stage gate mismatch.** `_stage_extract` gates on `PARSED` only; the pickup also takes
  `FT_ELIGIBLE`. Align it, or let the new selection replace both.
* **F15 — Cloud arms.** R67/R83 name "the extractor". Decide whether `cloud_extractions` /
  `cloud_evidence_spans` writes and the cloud pickup's `NOT IN cloud_extractions` move in session 9 or
  later. R71 keeps cloud off until 8 and 9 land.
* **F16 — B5 rewrite volume.** 18 run-path test files, 2 fakes, 2 helpers (P8). Decide whether
  fixtures move to events wholesale or per module as each reader is re-pointed.

---

## Findings that contradict the ledger or a plan premise

1. **I3 partly false.** `run_pipeline` writes and activates a manifest (kind `extraction`) around the
   local extraction stage; only the side runners extract without one.
2. **I4 false.** `contract_unmet` and `duplicate_detected` are admitted by the CHECK; the reader
   emits `contract unmet` (row 15) and `unresolved (duplicate values)` (row 6). All eight S5b states
   are derivable without a migration.
3. **I5 partly false.** The budget constant lives in `completeness.py`; the loop lives in the extractor (and cloud base).
4. **I6 false, and with it plan R6's premise.** The `extraction_models` block exists in the spec
   model and the live spec, and reaches the call through the resolver.

## Stale lines noticed, not fixed (outside this brief's write scope)

* `CLAUDE.md` "Distribution collapse detection … runs automatically at end of all extraction
  pipelines": the local path (`run_extraction`, `run_pipeline`) never calls it. Only the cloud
  extractors (`run_distribution_check`) and `scripts/run5_extract_and_audit.py` do.
* `engine/agents/extractor.py`: `MAX_RETRIES = 2` and `RETRY_DELAY = 30` are unreferenced. The module
  docstring still reads `"Two-pass extraction agent using DeepSeek-R1:32b via Ollama."`; the model
  comes from the spec.
* `engine/core/database.py`: `reset_for_reextraction` (a DELETE path), `reset_for_reaudit`,
  `add_extraction`, `add_evidence_span`, `cleanup_orphaned_spans` and `admin_reset_status` have **no
  caller** in `engine/`, `scripts/` or `analysis/`; only `tests/` call them. `reset_for_reextraction`
  survives beside R94's retirement of the other delete branches.
* `scripts/reextract_failed.py` extracts through `extractor.extract_paper` directly: no completeness
  retry, no manifest, and a raw `UPDATE papers SET status = 'PARSED'`.
