# S2 Phase 1 — the effective-result model, design read-out

**Task** EFFECTIVE-RESULT-01 Phase 1 (session 4 of the refactor plan) · **read-only**
**HEAD at measurement** `59dc247`, tree clean, level with origin
**Database** `data/surgical_autonomy/review.db`, opened `mode=ro` throughout, never `immutable=1`
**Fingerprint** `--compare` against `docs/session-reports/migrations-01/review_db_fingerprint_20260921T002809Z.json`
exits **0** at open and at close; overall `a9926e626928d1d47f4935e129da698cd2d50c82902f25c0f4f3cd9b1b1eafae`

This read-out proposes. It builds nothing. Session 5 builds the event store, the reader and the
rule; the PI approves the rule in §4 before session 5 opens.

Every number below is followed by the query or the code anchor that produced it. Probe scripts
lived in the session scratchpad and are not committed; the queries are reproduced here in full so
the numbers can be re-derived without them.

**Reconciliation carried from the startup verify.** The fingerprint tool's `table_count` is **25**
and includes `sqlite_sequence`; there are **24 user tables**, and `structure()` hashes those 24.
The two predicates are `fingerprint()`'s
`sorted(row[1] for row in master if row[0] == "table")` and `structure()`'s
`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite\_%' ESCAPE '\'`.
Both numbers are correct for their own definition. This is reconciliation, not drift.

---

## §1 Current state, re-measured at this HEAD

### 1.1 Stores that hold a result or a decision

D2-15 enumerated 13. **All 13 are present at this HEAD and the count holds.** Row counts were
re-measured; every one matches the value D2-15 published.

```sql
SELECT COUNT(*) FROM <table>;                       -- per store
SELECT status, COUNT(*) n FROM papers GROUP BY status ORDER BY n DESC;
SELECT audit_status, COUNT(*) n FROM evidence_spans GROUP BY audit_status ORDER BY n DESC;
SELECT auditor_model, COUNT(*) n FROM evidence_spans GROUP BY auditor_model ORDER BY n DESC;
SELECT pdf_quality_check_status, COUNT(*) n FROM papers GROUP BY pdf_quality_check_status ORDER BY n DESC;
```

| # | store | kind | measured at `59dc247` | matches D2-15 |
|---|---|---|---|---|
| 1 | `abstract_screening_adjudication` | table | **0 rows** | ✅ |
| 2 | `ft_screening_adjudication` | table | **36 rows** (`FT_ELIGIBLE` 31 · `FT_SCREENED_OUT` 5) | ✅ |
| 3 | `audit_adjudication` | table | **0 rows** | ✅ |
| 4 | `papers.status` | mutated column | `ABSTRACT_SCREENED_OUT` 9,647 · `AI_AUDIT_COMPLETE` 190 · `FT_SCREENED_OUT` 171 · `PDF_EXCLUDED` 31 (Σ **10,039**) | ✅ |
| 5 | `papers.rejected_reason` | mutated column | no `REJECTED` paper exists (status set is the four above) | ✅ |
| 6 | `evidence_spans.value` | mutated column | 3,760 rows | ✅ |
| 7 | `evidence_spans.audit_status` / `.auditor_model` / `.audit_rationale` / `.audited_at` | mutated columns | verified **2,159** · flagged **1,152** · contested **449** (Σ 3,760); `auditor_model` = `gemma3:27b` on **all 3,760** | ✅ |
| 8 | `evidence_spans.source_snippet` | mutated column | 141 rows empty (see §5) | ✅ |
| 9 | `papers.pdf_quality_check_status` / `.pdf_exclusion_reason` / `.pdf_exclusion_detail` | mutated columns | `HUMAN_CONFIRMED` 364 · `AI_CHECKED` 17 · NULL 9,658; **31** papers at `PDF_EXCLUDED` | ✅ |
| 10 | `workflow_state` | table | **12 rows**, 7 complete, 5 pending | ✅ |
| 11 | `judge_run_audit` | table | **1 row** | ✅ |
| 12 | HTML → JSON → import files | filesystem | `data/surgical_autonomy/adjudication/` holds `screening_queue_20260310.xlsx` (326,570 B, 2026-03-12), `specialty_rescreen_flagged_86.xlsx`, `test_self_documenting_workbook.xlsx` | ✅ |
| 13 | PI audit workbooks | filesystem | v1 tracked, v2 untracked, under `artifacts/paper1/` | ✅ |

**Three are still write-only, re-measured.** A grep for the three adjudication table names across
`engine/`, `scripts/` and `analysis/`, excluding `engine/migrations/`, returns **22 hits, of which
exactly one is a SELECT**: `engine/exporters/prisma.py`,
`"""SELECT COUNT(*) FROM ft_screening_adjudication fta`. Every other hit is an INSERT, an UPDATE,
a DDL statement in `engine/adjudication/schema.py`, or a docstring. **Nothing in the codebase reads
`audit_adjudication` or `abstract_screening_adjudication`.** D2-15's finding holds unchanged.

**Finding F1 — `human_extractions` is documented but does not exist.** `CLAUDE.md`'s *Database
Tables* section lists `human_extractions` ("Human extractor workbook values (paper_id as
"EE-NNN", extractor_id A/B/C/D)"). `SELECT COUNT(*) FROM human_extractions` raises
`no such table: human_extractions`, and the table is absent from the 24-name list in
`sqlite_master`. The human arms named in the concordance architecture (`human_A`…`human_D`) have
no store on this database. This matters to S2 because the resolution rule must say what an arm is,
and one documented arm class has no rows and no table. **Not in the D2-15 count** — D2-15 counted
stores that exist. Recorded as a documentation defect for the PI, not repaired here.

### 1.2 Readers that resolve a result

D2-15 enumerated 15. **All 15 exist at this HEAD with the resolution rules D2-15 quoted.** Each
was re-opened and the rule re-read; anchors below are function names and quoted source, never line
numbers.

| # | reader (file · function) | resolution rule, quoted from source | human decisions override? | abstentions? |
|---|---|---|---|---|
| 1 | `engine/exporters/evidence_table.py` · `_build_evidence_rows` | `"SELECT id FROM extractions WHERE paper_id = ? ORDER BY id DESC LIMIT 1"`, then that extraction's spans | only via the span mutation; `audit_adjudication` never read | no |
| 2 | `engine/analysis/concordance.py` · `load_arm` (`local`) | `SELECT e.paper_id, es.field_name, es.value / FROM evidence_spans es / JOIN extractions e ON e.id = es.extraction_id / ORDER BY e.paper_id, es.field_name` — **no run selection** | no | **yes, and backwards**: `if str(row["value"] or "").strip().upper() in non_value: continue` |
| 3 | `engine/analysis/concordance.py` · `load_arm` (cloud) | same shape plus `WHERE ce.arm = ?` | no | same |
| 4 | `engine/analysis/metrics.py` · `field_summary` | consumes `load_arm` output via `score_pair` | no | inherits |
| 5 | `analysis/paper1/judge_loader.py` · `load_ai_triples_csv` | one `JudgeInput` per CSV row | no | inherits the CSV's |
| 6 | `engine/adjudication/audit_adjudicator.py` · `_collect_papers_for_review` | `"SELECT id, low_yield FROM extractions WHERE paper_id = ? ORDER BY id DESC LIMIT 1"` | reads `audit_status` | n/a |
| 7 | `engine/adjudication/audit_adjudicator.py` · `import_audit_review_decisions` | same `ORDER BY id DESC LIMIT 1`, then the field's span | writes them | n/a |
| 8 | `engine/review/human_review.py` · `_import_review_csv` | `SELECT es.id FROM evidence_spans es / JOIN extractions e ON es.extraction_id = e.id / WHERE e.paper_id = ? AND es.field_name = ? / ORDER BY es.id DESC LIMIT 1` — **newest SPAN across all extractions** | writes them | n/a |
| 9 | `engine/review/human_review.py` · `_import_review_json` | `"SELECT id FROM evidence_spans WHERE id = ?"` — **binds by `span_id`** | writes them | n/a |
| 10 | `engine/core/corpus.py` · `is_corpus_member` | `return status in CORPUS_STATUSES`, where `CORPUS_STATUSES = ("FT_ELIGIBLE", "EXTRACTED", "AI_AUDIT_COMPLETE", "HUMAN_AUDIT_COMPLETE")` | yes, through status | n/a |
| 11 | `scripts/run_pipeline.py` gates | `if not is_adjudication_complete(db._conn):` / `if not is_audit_review_complete(db._conn):` — both `is_stage_done` on `workflow_state` | **no** — reads the stage flag, not the work | n/a |
| 12 | `engine/exporters/prisma.py` · `_build_flow` | `"SELECT status, COUNT(*) as cnt FROM papers GROUP BY status"`, plus the one FT adjudication COUNT | yes, through status | n/a |
| 13 | `analysis/provenance/census.py` | `FROM evidence_spans s` and `FROM cloud_evidence_spans s`, unfiltered | no | classifies as `ABSENCE_*` |
| 14 | `engine/adjudication/ft_screening_adjudicator.py` export | `"SELECT parsed_text_path FROM full_text_assets WHERE paper_id = ? ORDER BY id DESC LIMIT 1"` for context; decisions from the two FT tables | n/a | n/a |
| 15 | `analysis/paper1/pi_audit_sampler_v2.py` | arm values from the disagreement-pairs CSV | no | empty cell → `NOT REPORTED` |

**Verdict on I5: HOLDS.** 13 stores and 15 readers, re-measured at `59dc247`, not inherited from
the plan summary. No store has appeared or disappeared; no reader has changed its rule.

**One wording correction to D2-15, immaterial.** D2-15 quoted reader 12 as
`"SELECT status, COUNT(*) FROM papers GROUP BY status"`. The source reads
`"SELECT status, COUNT(*) as cnt FROM papers GROUP BY status"`. Same query, D2-15 dropped the
alias when it paraphrased.

**The three disagreeing reader pairs are unchanged at this HEAD**, and pair 1 remains latent for
the reason D2-15 gave — see §2, where it is re-measured.

---

## §2 Claim identity

### 2.1 What a claim is today

A claim is one arm's assertion of one field for one paper, produced by one extraction. Today it is
a row in `evidence_spans` (local) or `cloud_evidence_spans` (cloud), reachable only through its
parent extraction.

```sql
SELECT sql FROM sqlite_master WHERE type='table' AND name IN
  ('extractions','evidence_spans','cloud_extractions','cloud_evidence_spans');
```

| arm class | extraction row | span row | arm identified by | uniqueness the schema enforces |
|---|---|---|---|---|
| local | `extractions` (`id INTEGER PRIMARY KEY`, `paper_id`, `codebook_hash`, `model`, `model_digest`, `extracted_at`) | `evidence_spans` (`id INTEGER PRIMARY KEY`, `extraction_id`, `field_name`, `value`, `source_snippet`, `confidence` NOT NULL, `tier` NOT NULL, `audit_status`, `auditor_model`, `audit_rationale`, `audited_at`) | **nothing** — "local" is implicit in the table | **none**: no UNIQUE on `(extraction_id, field_name)` |
| cloud | `cloud_extractions` (… `arm TEXT NOT NULL`, `model_string`, `prompt_text`, `UNIQUE(paper_id, arm)`) | `cloud_evidence_spans` (… `UNIQUE(cloud_extraction_id, field_name)`, `confidence` and `tier` nullable on live) | `cloud_extractions.arm` | `UNIQUE(paper_id, arm)` and `UNIQUE(cloud_extraction_id, field_name)` |

Two asymmetries follow directly and both constrain the rule in §4:

- **A cloud arm cannot hold two extractions of one paper.** `UNIQUE(paper_id, arm)` forbids it. A
  cloud re-extraction is therefore not an append — it is a delete-and-replace or a refused insert.
  "A newer extraction supersedes an older one within one arm" is, for cloud arms, *unrepresentable
  in today's schema*.
- **The local arm has no uniqueness at all**, so a duplicated field (R2) is structurally possible
  there and structurally impossible in cloud. Measured today:
  `SELECT COUNT(*) FROM (SELECT extraction_id, field_name, COUNT(*) c FROM evidence_spans GROUP BY extraction_id, field_name HAVING c>1)` → **0**;
  the same query on `cloud_evidence_spans` → **0**.

### 2.2 Are today's ids stable across re-extraction? — I7, measured

**I7 is FALSE in both halves, and the two halves fail for different reasons.**

**(a) There is no live data on which the question can be answered by observation.**

```sql
SELECT k, COUNT(*) papers FROM (SELECT paper_id, COUNT(*) k FROM extractions GROUP BY paper_id) GROUP BY k ORDER BY k;
-- → k=1, papers=190
SELECT k, COUNT(*) pairs FROM (SELECT paper_id, arm, COUNT(*) k FROM cloud_extractions GROUP BY paper_id, arm) GROUP BY k ORDER BY k;
-- → k=1, pairs=379
SELECT MIN(id), MAX(id), COUNT(*), COUNT(DISTINCT paper_id) FROM extractions;   -- 1, 190, 190, 190
```

**Every paper has exactly one extraction, in every arm.** There is no paper with more than one, so
the brief's suggested measurement ("e.g. papers with more than one extraction") has an empty
sample. Pair 1 of D2-15's three disagreeing readers is latent for exactly this reason, and it is
still latent at `59dc247`.

**(b) The ids are not a stable identity, by construction — and this is the decisive finding.**

Every one of the four tables declares `id INTEGER PRIMARY KEY` **without `AUTOINCREMENT`**, and
none of them appears in `sqlite_sequence`:

```sql
SELECT * FROM sqlite_sequence ORDER BY name;
-- fabrication_verifications 7431 | judge_pair_ratings 6831 | judge_ratings 2277
-- judge_run_audit 1 | provenance_classifications 22034
```

Only five tables in the database have `AUTOINCREMENT`, and none of them is an extraction or span
table. For a plain `INTEGER PRIMARY KEY`, SQLite assigns `max(rowid)+1`, so **deleting the highest
rows returns their ids to the pool** — demonstrated this session on a throwaway `:memory:`
database rather than asserted from the documentation (§10 A-1): the plain table reuses the freed
id for a different row, the `AUTOINCREMENT` table does not. And rows *are* deleted — five delete sites exist outside
`engine/migrations/`:

```
engine/utils/extraction_cleanup.py   DELETE FROM evidence_spans WHERE extraction_id IN (…)
engine/utils/extraction_cleanup.py   DELETE FROM extractions WHERE id IN (…)
engine/core/database.py              DELETE FROM evidence_spans  (×2)
engine/core/database.py              DELETE FROM extractions
scripts/reextract_all.py             DELETE FROM evidence_spans WHERE extraction_id = ?
scripts/reextract_all.py             DELETE FROM extractions WHERE paper_id = ?
```

`scripts/reextract_all.py` is the re-extraction path, and it *deletes then re-inserts*. So the
failure is not hypothetical: **run the documented re-extraction script on the highest-numbered
paper and the new spans can receive the ids the old spans had.** A `span_id` recorded in a queue
file, a workbook, or `audit_adjudication` can therefore come to point at a different claim without
anything detecting it. Reader 9 (`_import_review_json`) binds by `span_id` and is the reader this
breaks silently.

The ranges are contiguous today —
`SELECT MIN(id), MAX(id), COUNT(*), COUNT(DISTINCT id) FROM evidence_spans` → `1, 3760, 3760, 3760`,
and `cloud_evidence_spans` → `1, 7257, 7257` — which means nothing has been deleted yet. That is
the *absence of the trigger*, not the *absence of the defect*.

**Consequence for S2: no event may reference a claim by `evidence_spans.id`.** Any migration that
reconstructs history from today's spans must mint a new identity and record the old span id only
as a provenance note.

### 2.3 The proposed claim id

**Mint a per-extraction unique id, and derive the claim id from it.**

- Add `extraction_uid TEXT NOT NULL UNIQUE` to `extractions` and to `cloud_extractions`, a UUIDv4
  minted at insert. For the 190 + 379 existing rows the migration mints one each, once, and
  records it; nothing is derived from content, so a re-extraction that reproduces identical text
  still gets a new uid, which is the required behaviour.
- `claim_id` is then a readable composite, stored as TEXT:

  ```
  claim_id = "<arm>:<extraction_uid>:<field_name>"
  arm      = "local" | cloud_extractions.arm | "human_<X>"   (see §9 Q4)
  ```

Why not the alternatives:

| candidate | why rejected |
|---|---|
| `evidence_spans.id` | reusable after delete (§2.2b); this is the defect, not a fix for it |
| `(paper_id, field_name, arm)` | names the *cell*, not the *claim* — it cannot distinguish two extractions, which is precisely what supersession needs |
| content hash of the value | two runs that agree collide, so a re-extraction that confirms a value would be indistinguishable from no re-extraction at all |
| `(extraction_id, field_name)` | inherits `extraction_id`'s reusability |

**Arm identity.** `local` is implicit today and must become explicit: the event carries the arm as
data, not as "which table the row was in". Cloud arms come from `cloud_extractions.arm`, measured
today as `anthropic_sonnet_4_6` (190 extractions) and `openai_o4_mini_high` (189) —
`SELECT arm, COUNT(*) FROM cloud_extractions GROUP BY arm`. The one-paper difference is a real gap
in the OpenAI arm and is counted in §5.

### 2.4 What a queue file must carry (ruling R3)

So that a stale import is refused *and names its superseding claim*, the queue artifact carries,
per row:

| field | purpose |
|---|---|
| `claim_id` | the exact claim the row was built from; the import binds to this and nothing else |
| `presented_value` | what the reviewer was shown, so a mismatch is detectable even if the claim id were right |
| `presented_context_sha256` | hash of the rendered context (value, snippet, rationale, passage, paper text window) — J5 |
| `field_name`, `paper_id`, `arm` | human-readable, and a cross-check on the claim id |

and, once per file:

| field | purpose |
|---|---|
| `queue_id` | uuid of this export |
| `built_at`, `generator`, `generator_version` | provenance of the artifact |
| `review_id` | refuses a workbook imported into the wrong review |
| `engine_state` / `run_id` | which state produced the claims |

**Refusal rule.** On import, for each row: resolve the current claim for `(arm, paper_id,
field_name)` through the reader. If it is not `claim_id`, **refuse that row** and report
`claim <claim_id> is superseded by <current_claim_id>`. A refusal is per row, not per file, and the
importer reports the refused set as a first-class outcome — a partial import that silently drops
rows is the failure mode A4 already describes.

---

## §3 Event tables, designed for the whole of S2

Two append-only streams. Scope is the whole of S2 — session 5 writes the tables and the reader,
sessions 6 and 12 fill in producers — so the schema reserves what the later slices need and the
behaviour of those slices is deliberately left unspecified here.

### 3.1 Shared columns

Both tables carry the same provenance spine.

| column | type | notes |
|---|---|---|
| `event_id` | `INTEGER PRIMARY KEY AUTOINCREMENT` | **`AUTOINCREMENT` is load-bearing** — §2.2b is the reason. An event id must never be reused. |
| `event_uid` | `TEXT NOT NULL UNIQUE` | uuid; the id an external artifact may quote |
| `event_type` | `TEXT NOT NULL` | closed vocabulary per table, CHECK-constrained |
| `occurred_at` | `TEXT NOT NULL` | ISO-8601 UTC — when the thing happened |
| `recorded_at` | `TEXT NOT NULL` | when the engine wrote the row; differs from `occurred_at` for reconstructed and imported events |
| `actor_kind` | `TEXT NOT NULL CHECK (actor_kind IN ('model','human','engine'))` | |
| `actor_name` | `TEXT NOT NULL` | model name, named reviewer, or engine component |
| `actor_digest` | `TEXT` | model digest; NULL for human and engine actors |
| `run_id` | `INTEGER` | **nullable**; FK to the run manifest added in **session 7** (S3b). Migrated rows keep it NULL. |
| `run_marker` | `TEXT` | `'pre-manifest'` on every migrated row (R-d); NULL once `run_id` is set |
| `prior_event_id` | `INTEGER REFERENCES <same table>(event_id)` | the exact prior result this event was made against |
| `presented_context_sha256` | `TEXT` | required when `actor_kind='human'`; NULL otherwise |
| `reason` | `TEXT` | free text; **required** for manual advances, bypasses and withdrawals |
| `payload_json` | `TEXT NOT NULL DEFAULT '{}'` | typed per `event_type`; the extension point |

`run_id` is nullable *with a marker* rather than defaulted, because R-d requires that nothing is
backfilled by guesswork and a NULL alone cannot distinguish "before manifests existed" from "the
writer forgot".

### 3.2 `field_events`

Additional columns:

| column | type | notes |
|---|---|---|
| `claim_id` | `TEXT NOT NULL` | §2.3 |
| `paper_id` | `INTEGER NOT NULL REFERENCES papers(id)` | denormalised from `claim_id` so the reader can index on it |
| `field_name` | `TEXT NOT NULL` | denormalised, same reason |
| `arm` | `TEXT NOT NULL` | denormalised, same reason |
| `against_claim_id` | `TEXT` | for a human decision: the claim the reviewer was deciding about. Distinct from `prior_event_id`, which is an event; this is the claim. Both are needed — "needs re-review" in §4 is detected by comparing this against the current claim. |
| `value` | `TEXT` | the asserted value; NULL for withdraw and for declined |
| `source_snippet` | `TEXT` | the engine's materialized quote, not the model's (ELICIT-DESIGN-01) |
| `field_state` | `TEXT NOT NULL` | one of the eight S5b states (R-e), CHECK-constrained |

`event_type` vocabulary, closed:
`asserted` · `declined` · `contract_unmet` · `superseded` · `human_accepted` ·
`human_corrected` · `human_withdrew` · `duplicate_detected` · `state_at_migration`.

Indices: `(paper_id, field_name, arm, event_id)` is the reader's access path;
`(claim_id)` for the import refusal check.

### 3.3 `paper_events`

Additional columns:

| column | type | notes |
|---|---|---|
| `paper_id` | `INTEGER NOT NULL REFERENCES papers(id)` | |
| `to_state` | `TEXT NOT NULL` | from the S2 paper-level vocabulary, CHECK-constrained |
| `from_state` | `TEXT` | NULL for the first event of a paper |
| `reason_code` | `TEXT` | closed vocabulary for failure states (S3h) |
| `criterion_id` | `TEXT` | **reserved for S4** — the eligibility criterion an exclusion cites |
| `evidence_offset_start` / `_end` | `INTEGER` | **reserved for S4** — character offsets into the materialized title-and-abstract text |
| `verifier_actor_name` / `verifier_actor_digest` / `verifier_verdict` | `TEXT` | **reserved for S4** — the cross-family verification |
| `stage_name` | `TEXT` | for stage-derivation and manual-advance events |

`event_type` vocabulary, closed:
`identified` · `duplicate_of` · `screened` · `verified` · `adjudicated` · `acquired` ·
`not_obtainable` · `parsed` · `extracted` · `extraction_failed` · `audited` ·
`manual_advance` · `bypass` · `state_at_migration`.

`criterion_id`, the two offsets and the three verifier columns are **reserved and unused in
session 5**. They are declared now because adding a column to an append-only table later means
either a rebuild or a nullable column whose absence is ambiguous on old rows; declaring them up
front makes "NULL because S4 had not been built" the same shape as "NULL because this event has no
criterion", which §9 Q5 asks the PI to settle.

### 3.4 How append-only is enforced

**By trigger, in the database, not by convention in the writer.**

```sql
CREATE TRIGGER field_events_no_update BEFORE UPDATE ON field_events
  BEGIN SELECT RAISE(ABORT, 'field_events is append-only'); END;
CREATE TRIGGER field_events_no_delete BEFORE DELETE ON field_events
  BEGIN SELECT RAISE(ABORT, 'field_events is append-only'); END;
```

and the pair again for `paper_events`. This is chosen over a code-level discipline for one reason:
the database already has five delete sites and two importers that bypass each other, and a rule
that lives only in the writer is a rule that the next writer does not inherit. A trigger refuses
`scripts/`, `analysis/`, an ad-hoc `sqlite3` session and a future app equally.

Consequence to state plainly: **a mistake is corrected by appending a correcting event**, never by
editing. A migration that must remove an event is a migration that drops and rebuilds the table
with its triggers, which is visible in the receipt store.

### 3.5 What the later slices need, reserved here

| slice | session | what the schema must hold | where |
|---|---|---|---|
| importer (one vocabulary: ACCEPT · CORRECT · WITHDRAW) | 12 | `against_claim_id`, `presented_context_sha256`, the three `human_*` event types | `field_events` |
| stage derivation | 12 | `stage_name`, and the ability to compute a stage from paper states with no stamp | `paper_events` |
| manual advance with reviewer and reason | 12 | `actor_kind='human'` + `actor_name` + `reason` NOT NULL for `manual_advance` / `bypass` | `paper_events` |
| S4 screening evidence | 18–20 | `criterion_id`, offsets, verifier triple | `paper_events` |
| run linkage | 7 | `run_id` FK, `run_marker` retired as rows gain manifests | both |

Their **behaviour** is out of scope for this read-out, as the brief requires.

---

## §4 The resolution rule

`effective_value(paper, field, arm)` returns `(value, field_state, provenance)`.
`effective_state(paper)` returns the paper's lifecycle state with who and why.

**Reading order of the table.** Rows are evaluated top to bottom; the first matching row wins. The
ordering is itself the rule — a human decision is above supersession, and "needs re-review" is
above both, because a stale human decision must not be silently resolved in either direction.

| # | situation | inputs the reader sees | effective value | S5b state | provenance returned |
|---|---|---|---|---|---|
| 1 | **No claim for the cell** | no `field_events` row for `(paper, field, arm)` | — (none) | **missing** | `{arm, reason: "no claim"}` |
| 2 | **Duplicate values within one claim** (R2) | ≥2 `asserted` events with the same `claim_id`, different values, retry budget exhausted | — (none); **both values surfaced** | **unresolved (duplicate values)** | `{arm, claim_id, candidates: [v1, v2], awaiting: human}` |
| 3 | **Human decision against a superseded claim** | newest human event's `against_claim_id` ≠ current claim id for the cell | — (none); **both values surfaced** | **unresolved (needs re-review)** | `{human: {value, actor, at, against_claim_id, presented_context_sha256}, current: {value, claim_id}, awaiting: human}` |
| 4 | **Human withdrew** (R1) | newest human event `human_withdrew`, `against_claim_id` = current claim | — (no value) | **withdrawn** | `{withdrawn_by, at, reason, original_value, original_claim_id}` — the original is in history, never in the value |
| 5 | **Human corrected** | newest human event `human_corrected`, `against_claim_id` = current claim | the human's value | **corrected by human** | `{corrected_by, at, original_value, original_claim_id, presented_context_sha256}` |
| 6 | **Human accepted** | newest human event `human_accepted`, `against_claim_id` = current claim | the model's value | the underlying model state (rows 8–11), **endorsed** | `{model provenance} + {accepted_by, at, presented_context_sha256}` |
| 7 | **Superseded within one arm** | ≥2 claims for the cell in this arm, no human event attached to any of them | newest claim's value, then rows 8–11 | per rows 8–11 | `{…, supersedes: [older claim_ids]}` |
| 8 | **Model asserted, citation validated** | newest claim, `value` not a token, ≥1 validated citation | the value | **asserted with evidence** | `{arm, claim_id, actor, digest, run_id, snippet}` |
| 9 | **Model asserted, no locatable citation** | newest claim, `value` not a token, zero citations | the value | **asserted without locatable evidence** | `{arm, claim_id, actor, digest, run_id, snippet: null}` |
| 10 | **Absence sentinel** (one of the six) | `value ∈ absence_sentinels`, ≥1 citation | the sentinel — **it is a value** | **asserted with evidence** | as row 8; the claim is "the paper does not report this" |
| 10b | **Absence sentinel, uncited** | `value ∈ absence_sentinels`, zero citations | the sentinel | **asserted without locatable evidence** | as row 9 |
| 11 | **Model declined** | `value = escape_token` (`NO_EVIDENCE_LOCATABLE`), zero citations | — (no value) | **declined** | `{arm, claim_id, actor, digest, run_id}` — **never skipped**; the arm is present in every export and every score with this state |
| 12 | **Contract unmet** | `value = contract_unmet_token` (`CONTRACT_UNMET`) | — (no value) | **contract unmet** | `{arm, claim_id, violation_codes, attempts}` — the engine's failure, not the model's |
| 13 | **Arms disagree** | two arms, each resolving to its own value by rows 1–12 | **not resolved** — `effective_value` is per arm and returns each arm's own answer | each arm keeps its own state | `{per_arm: {...}}`; **arms never supersede each other** |
| 14 | **Migrated row with no reconstructable history** (R-f) | only a `state_at_migration` event | today's stored value | the state derived from today's row (§5), tagged | `{source: "state at migration", migrated_at, note: "history not reconstructable from the record"}` |

**How each required clause is discharged.**

| required clause | row(s) |
|---|---|
| a human decision beats a model value | 4, 5, 6 above 7–12 |
| a human decision against a superseded claim → "needs re-review", both values shown | 3 |
| a newer extraction supersedes an older one within one arm unless a human decision is attached | 7, subordinate to 3–6 |
| arms never supersede each other | 13 |
| abstention is "declined", never skipped | 11 |
| R1 withdraw — no value, original kept in history | 4 |
| R2 duplicated fields — "unresolved: duplicate values" | 2 |
| R3 stale import refused and names its superseding claim | §2.4; the reader supplies the current claim id the refusal quotes |

### 4.1 Situations the rule does **not** decide — open questions

| # | undecided situation | why it is open |
|---|---|---|
| U1 | **Two human decisions on the same claim by different reviewers** | Single-reviewer today (S7 captures one identity). The rule takes "newest human event" but has no tie-break for genuine disagreement, and "newest wins" is the wrong default for two named reviewers. → §9 Q1 |
| U2 | **A human decision, then a re-extraction that reproduces the identical value** | Row 3 sends it to "needs re-review" because the claim id changed, even though nothing a human would care about changed. Strictly correct, possibly noisy. → §9 Q2 |
| U3 | **A cloud arm's re-extraction** | `UNIQUE(paper_id, arm)` makes row 7 unrepresentable for cloud (§2.1). Either the constraint goes or cloud arms cannot supersede. → §9 Q3 |
| U4 | **Which arm `effective_value(paper, field)` means with no arm argument** | S2 writes the signature without an arm; row 13 requires one. → §9 Q4 |
| U5 | **A field not in the codebook** (`field_1`, `Title`) | S5a says unexpected fields are dropped and logged, but two are already stored (§5). Dropping them at read time and cleaning them at migration are different acts with different audit trails. → §9 Q6 |
| U6 | **`audit_status` = `flagged` or `contested` with no human decision yet** | These are the auditor's verdicts, not the human's. They are not one of the eight S5b states. The reader must return *something* for 1,601 spans today. → §9 Q7 |

---

## §5 Mapping today's data to states, with measured counts

### 5.1 Field-level — local arm

```sql
SELECT value, source_snippet, field_name FROM evidence_spans;
-- bucketed in Python: value kind (real | absence sentinel | escape | contract-unmet | empty)
--                     × cited (source_snippet non-blank) × codebook field vs artefact
```

Sentinels are the six in `data/surgical_autonomy/extraction_codebook.yaml` `absence_sentinels`:
`NR`, `N/A`, `NA`, `NOT_FOUND`, `NOT FOUND`, `NOT REPORTED`.

| row class | count | S5b field state under §4 | rule row |
|---|---:|---|---|
| real value, cited, codebook field | **3,516** | asserted with evidence | 8 |
| real value, uncited, codebook field | **59** | asserted without locatable evidence | 9 |
| absence sentinel, cited | **101** | asserted with evidence | 10 |
| absence sentinel, uncited | **82** | asserted without locatable evidence | 10b |
| real value, cited, **artefact field** (`field_1`, `Title`) | **2** | *not a codebook field* — see U5 | — |
| **stored spans, total** | **3,760** | | |
| **cells with no span at all** | **42** | **missing** | 1 |
| **codebook cells, total** | **3,800** | = 190 papers × 20 codebook fields | |

The 42 missing cells decompose exactly:
`SELECT n, COUNT(*) FROM (SELECT extraction_id, COUNT(*) n FROM evidence_spans GROUP BY extraction_id) GROUP BY n`
→ 186 extractions with 20 spans, 2 with 19, **2 with 1**. The two single-span extractions are
extraction 49 (paper **415**, field `Title`) and extraction 171 (paper **719**, field `field_1`) —
so **those two papers have no codebook field extracted at all**: 2 × 20 = 40 missing cells. The two
19-span extractions (15 and 61) account for the remaining 2, which are the single missing
`task_select` and `country`
(`SELECT field_name, COUNT(*) FROM evidence_spans GROUP BY field_name` → 18 fields at 188, `task_select` 187, `country` 187).

**`escape_token` and `contract_unmet_token` do not occur**: 0 spans carry `NO_EVIDENCE_LOCATABLE`
and 0 carry `CONTRACT_UNMET` in either span table. **No row on this database maps to "declined" or
"contract unmet" today.** Those two states arrive with the elicited path (Run 7) and the reader
must implement them against zero live rows — which §7 turns into a fixture requirement rather than
a live-agreement check.

### 5.2 Field-level — cloud arms

Same query against `cloud_evidence_spans`:

| row class | count | S5b field state | rule row |
|---|---:|---|---|
| real value, cited | **6,949** | asserted with evidence | 8 |
| real value, uncited | **142** | asserted without locatable evidence | 9 |
| absence sentinel, cited | **37** | asserted with evidence | 10 |
| absence sentinel, uncited | **129** | asserted without locatable evidence | 10b |
| **stored spans, total** | **7,257** | | |
| **cells with no span** | **323** | **missing** | 1 |
| **cloud cells, total** | **7,580** | = 379 extractions × 20 fields | |

Of the 323, **20 are a whole paper absent from the OpenAI arm** (190 anthropic vs 189 openai
extractions); the other 303 are per-field gaps.

### 5.3 The auditor's verdicts are not field states — U6

```sql
SELECT audit_status, COUNT(*) FROM evidence_spans GROUP BY audit_status;
-- verified 2159 | flagged 1152 | contested 449
```

Cross-tabbed against value kind:

| `audit_status` | real value | absence sentinel |
|---|---:|---:|
| verified | 1,977 | 182 |
| flagged | 1,151 | 1 |
| contested | 449 | 0 |

`auditor_model` is `gemma3:27b` on **all 3,760** — so **not one of these is a human decision**.
They are the AI auditor's opinion of a model claim. None of the eight S5b states names them, and
the reader must still answer for the 1,601 flagged-or-contested spans. The proposal in §4 is that
they are **provenance, not state**: the field state stays rows 8–10b and the provenance carries
`audit_status`, `audit_rationale` and `audited_at`. §9 Q7 asks the PI to confirm, because the
alternative — a ninth state "AI-audit contested" — is a change to the frozen S5b vocabulary.

### 5.4 Paper-level

```sql
SELECT status, COUNT(*) FROM papers GROUP BY status;
SELECT * FROM workflow_state ORDER BY rowid;
SELECT adjudication_decision, COUNT(*) FROM ft_screening_adjudication GROUP BY adjudication_decision;
SELECT decision, COUNT(*) FROM abstract_screening_decisions GROUP BY decision;
SELECT decision, COUNT(*) FROM abstract_verification_decisions GROUP BY decision;
SELECT p.status, COUNT(*) FROM papers p JOIN extractions e ON e.paper_id = p.id GROUP BY p.status;
```

| today's row class | count | proposed paper state | reconstructable from the record? |
|---|---:|---|---|
| `papers.status = 'ABSTRACT_SCREENED_OUT'` | **9,647** | *Abstract: out* | **partly** — 18,315 primary `exclude` and 485 verifier `exclude` decisions exist with model, rationale and timestamp, so the AI route reconstructs. **The 416 human exclusions do not** — see F2. |
| `papers.status = 'PDF_EXCLUDED'` | **31** | *Full text: not obtainable (reason)* | **yes** — `pdf_exclusion_reason` / `_detail` carry it; `pdf_quality_check_status` is `HUMAN_CONFIRMED` 364 / `AI_CHECKED` 17 |
| `papers.status = 'FT_SCREENED_OUT'` | **171** | *Full text: out* | **partly** — 366 FT primary + 182 verifier decisions with model and rationale; 36 FT adjudications carry the human half (31 `FT_ELIGIBLE`, 5 `FT_SCREENED_OUT`) |
| `papers.status = 'AI_AUDIT_COMPLETE'` | **190** | *Audited, AI* | **yes** — 190 extractions, 3,760 spans, all `auditor_model = 'gemma3:27b'` with `audited_at` |
| papers with an extraction | **190**, all at `AI_AUDIT_COMPLETE` | — | the join returns exactly one status class, so there is no extraction on a paper in any other state |
| `workflow_state` complete | **7** of 12 | stage-derivation input | **stamps only** — see F3 |
| `judge_run_audit` | **1** | not a paper state | — |
| `abstract_screening_adjudication` | **0** | — | — |
| `audit_adjudication` | **0** | — | — (and unwritable — §6) |

**No paper is at `INGESTED`, `ABSTRACT_SCREENED_IN`, `PARSED`, `FT_ELIGIBLE`, `EXTRACTED`,
`HUMAN_AUDIT_COMPLETE` or `REJECTED`.** The status column holds exactly four values. Several
lifecycle states in `CLAUDE.md`'s *Paper Lifecycle* therefore have **zero** live instances, and the
reader's mapping for them is unexercisable on this database.

**Finding F2 — the 416 human abstract exclusions exist only as a status mutation and a prose
string.** `workflow_state` row 5 reads:

> `ABSTRACT_ADJUDICATION_COMPLETE … '416 expanded-search papers excluded via human review (screening_queue_20260310.xlsx). 0 missing, 0 invalid decisions.'`

and `abstract_screening_adjudication` holds **0 rows**. The identity of the 416 papers is not in
the database. It is recoverable only from
`data/surgical_autonomy/adjudication/screening_queue_20260310.xlsx` (326,570 B, mtime 2026-03-12),
a file with no claim binding and no presented-context hash. **These 416 are the largest single
population of human decisions on this database and the least reconstructable.** §7 proposes they
migrate as `state_at_migration` unless the PI wants the workbook parsed — §9 Q8.

**Finding F3 — the 7 completed workflow stamps carry no per-paper binding.** Each is one row with
a `completed_at` and a prose `metadata` string. They reconstruct as **seven `paper_events`? No** —
they are not paper events at all; they are stage assertions about a scope that is not recorded.
Under S2 "stage completion is derived, never stamped", so the honest migration is to record them
as **provenance notes on the migration**, not as events, and let derivation recompute the stages.
§9 Q9.

### 5.5 How the session-5 reader returns a state before S5b's write path exists

S5b's write path is session 9. Session 5's reader must still answer for every row above. The
proposal:

- **The reader derives the state; it never reads a stored one.** In session 5 `field_state` in
  `field_events` is populated by the migration from the derivation in §5.1–5.2 (value kind × cited),
  and by the reader's own classification for anything written after. There is no period in which
  the reader waits for a writer.
- **Derivation is one function, `classify_field_state(value, citations, event_type)`, in the
  reader module**, and session 9's write path calls the same function rather than reimplementing
  it. One predicate, two programs, shared by import — not copied.
- **"declined" and "contract unmet" are implemented and tested against fixtures only**, because
  §5.1 measured zero live rows of either. This is stated as a limit in §10, not hidden.

---

## §6 A11 — options for `audit_adjudication`

**Re-measured at this HEAD.** The FK is on disk exactly as A11 describes:

```sql
CREATE TABLE audit_adjudication (
    id                      INTEGER PRIMARY KEY,
    span_id                 INTEGER REFERENCES "_evidence_spans_old"(id),
    paper_id                INTEGER REFERENCES papers(id),
    field_name              TEXT NOT NULL,
    original_value          TEXT,
    human_decision          TEXT CHECK (human_decision IN ('accept', 'override', 'reject_paper')),
    override_value          TEXT,
    reviewer_notes          TEXT,
    adjudication_timestamp  TEXT,
    created_at              TEXT NOT NULL
)
```

`_evidence_spans_old` is not among the 24 user tables. **Rows held: 0**
(`SELECT COUNT(*) FROM audit_adjudication`). Writers: three `INSERT INTO audit_adjudication`
statements in `engine/adjudication/audit_adjudicator.py`. Readers: **none**, re-confirmed in §1.1.

Its vocabulary is also wrong for the plan: `CHECK (human_decision IN ('accept','override','reject_paper'))`
against S2's **ACCEPT · CORRECT · WITHDRAW**. And the two importers disagree about what a REJECT
*means*, which is A3, measured in source at this HEAD:

- xlsx (`audit_adjudicator.py`, `REJECT` branch): inserts `'reject_paper'`, then
  `UPDATE evidence_spans SET audit_status = 'verified', auditor_model = 'human_review', …` —
  **the rejected value is kept and the span is marked verified.**
- JSON (`human_review.py`, `REJECT_VALUE` branch): `SET value = 'NR', audit_status = 'verified'`,
  documented in its own docstring as `REJECT  → value='NR', audit_status='verified'` —
  **the value is replaced by an absence sentinel.**

One vocabulary, two meanings, and under R1 **both are wrong**: REJECT means withdraw, which is
neither "keep and verify" nor "overwrite with NR".

### Options

| | **Option A — rebuild with a corrected FK** | **Option B — retire into the field-value event stream** |
|---|---|---|
| **What it does** | Migration rebuilds `audit_adjudication` with `span_id INTEGER REFERENCES evidence_spans(id)` and, optionally, the S2 vocabulary in the CHECK | Table is dropped; every human audit decision becomes a `field_events` row (`human_accepted` / `human_corrected` / `human_withdrew`) carrying `claim_id`, `against_claim_id` and `presented_context_sha256` |
| **Cost** | One small migration; the rebuild is the standard 12-step table rebuild, and 0 rows means no data movement. Cheap. | Larger: the three INSERT sites in `audit_adjudicator.py` are rewritten to append events, and the importer consolidation (session 12) must land at the same time or the path stays unwritable |
| **What it does to the rows it holds** | **Nothing — there are 0.** Re-measured this session. | **Nothing — there are 0.** No data is at risk under either option. |
| **Does it fix the defect?** | It makes the table *writable*. It does not make it *read* — nothing in the codebase selects from it, and A2 stays open. It also preserves a vocabulary R1 has superseded, so it would be rebuilt again in session 12. | Yes, and it closes A2 and A3 in the same act: the event stream has exactly one reader, and the vocabulary is R1's. |
| **Risk** | Low, and low value. It repairs a table into a design the plan has already replaced. | The human-audit path stays unwritable until session 12. Today that costs nothing — the path has never been used and holds 0 rows — but it is a real gap if the PI wants to audit before session 12. |

**Recommendation: Option B**, with one condition. Retiring the table is the only option that also
closes A2 (write-only) and A3 (two meanings of REJECT), and the 0-row measurement means retirement
destroys nothing. Option A spends a migration to restore a table the plan intends to delete.

The condition is the timing of the drop. Dropping in session 5 while the importer is not rewritten
until session 12 leaves seven sessions in which the xlsx audit path raises a missing-table error
instead of an FK error — a different failure, not a better one. So: **session 5 stops writing to
it and marks it deprecated in the schema module; session 12 drops it** once `field_events` has a
producer. That keeps every session's tree coherent.

### Where the PI v2 audit adjudication lands, and when

The v2 audit workbook (`pi_audit_sampler_v2`, n=200, blinded) is **not** an `audit_adjudication`
subject and never was — `pi_audit_unblind.py` "Writes branded results xlsx + JSON sidecar; **no DB
access**". It is a *judge-reliability measurement*, not a correction of extraction values, and its
gold standard is the PI's adjudication of the judge, not of the arms.

It therefore lands as follows:

| | where | session |
|---|---|---|
| v2 adjudication as a **measurement** of judge reliability | stays a file artifact + JSON sidecar, as today; no event | unchanged, no session owes it |
| any v2 row the PI wants to apply as a **correction to a field value** | `field_events` as `human_corrected`, with `against_claim_id` and the presented-context hash from the workbook | **12** (the importer session) |
| the judge run the v2 audit measures | already has a store — `judge_runs` / `judge_run_audit` | unchanged |

**This is a question, not a settled fact** — §9 Q10. The distinction matters because if the PI
intends the v2 adjudications to change exported values, they need claim binding that the v2
workbook does not currently carry, and that is a change to `pi_audit_sampler_v2`, in session 12.

---

## §7 Session-5 migration plan

**Scope: session 5 only.** Sessions 6 and 12 are §8.

### 7.1 Bracketing

1. `python -m engine.tools.db_fingerprint data/surgical_autonomy/review.db --compare docs/session-reports/migrations-01/review_db_fingerprint_20260921T002809Z.json` → must exit **0**.
2. `auto_backup(conn, reason="pre-EFFECTIVE-RESULT-02")` — the WAL-aware, self-verifying backup from S0; its `BackupResult.fingerprint` must equal the live fingerprint or it raises and the session stops.
3. Migrations run, one transaction each, through `engine/migrations/runner.py`.
4. New fingerprint written with `--out docs/session-reports/effective-result-01/review_db_fingerprint_<ts>.json`, committed, and its `overall_sha256` quoted in the closeout. The 2026-09-21 record is **superseded, never edited**.

### 7.2 Proposed migrations, in order

Next free number is **016** (`schema_migrations` holds 14 receipts, 002–015; 015 is the only one
with `mode = 'executed'`).

| # | migration | what it does | why it is separate |
|---|---|---|---|
| **016** | `016_cloud_span_not_null` | **executes C10 on live** — the `confidence` / `tier` NOT NULL tightening that `014_cloud_tables` recorded as `registered_preapplied`. Measured safe: 0 NULLs in 7,257 rows, and no reader selects either column. | It is the inherited item the brief names, it is independent of the event store, and it is the one that moves G3 |
| **017** | `017_extraction_uid` | adds `extraction_uid TEXT UNIQUE` to `extractions` and `cloud_extractions`; mints one uuid per existing row (190 + 379) | claim identity (§2.3) must exist before any event can reference a claim |
| **018** | `018_event_store` | creates `paper_events`, `field_events`, their CHECK constraints, their indices, and the four append-only triggers | the structure, with no data |
| **019** | `019_seed_field_events` | writes one `field_events` row per existing span: 3,760 local + 7,257 cloud, `event_type='state_at_migration'`, `run_marker='pre-manifest'`, `field_state` from the §5.1/§5.2 derivation, `claim_id` from 017's uid, `source_span_id` recorded in `payload_json` as provenance only | data migration, separately revertible, and the largest row count |
| **020** | `020_seed_paper_events` | writes reconstructed `paper_events` where the record supports it, and `state_at_migration` where it does not (§7.3) | same reason; different table and different reconstruction rules |

Migration 016 is deliberately **first**: it is the only one that touches an existing table's
constraints, so if it fails the event store was never created and the tree is trivially clean.

### 7.3 What is reconstructed, and from what

R-f binds: *historical events are reconstructed only where the record supports it … and are
otherwise marked "state at migration" — no invented history.*

| population | count | reconstructed? | source |
|---|---:|---|---|
| auditor verdicts | **3,760** | **yes** — one `paper_events`-adjacent audit fact per span, as `field_events` provenance | `evidence_spans.audit_status`, `.auditor_model`, `.audit_rationale`, `.audited_at`; all four columns populated, `auditor_model` uniform |
| FT adjudications | **36** | **yes** | `ft_screening_adjudication` carries decision, reason code, both rationales and a timestamp |
| FT screening decisions | 366 primary + 182 verifier | **yes** | model, decision, reason code, rationale, `decided_at` all present |
| abstract screening decisions | 21,374 primary + 1,422 verifier | **yes** | same columns present |
| PDF quality dispositions | 31 excluded, 364 `HUMAN_CONFIRMED`, 17 `AI_CHECKED` | **yes** | `pdf_quality_check_status`, `pdf_exclusion_reason`, `pdf_exclusion_detail` |
| extraction claims | 3,760 + 7,257 spans | **as state, not as history** | the span row is the current projection; there is no per-field event log behind it |
| **the 416 human abstract exclusions** | **416** | **NO** — `state_at_migration` | F2: 0 rows in `abstract_screening_adjudication`; the identities exist only in an xlsx with no claim binding |
| **workflow stamps** | **7 complete** | **NO** — recorded as migration provenance, not as events | F3: no per-paper scope; S2 derives stages instead |
| `audit_adjudication` | **0** | n/a | nothing to migrate (§6) |

**No reconstructed event invents an `occurred_at`.** Where the source row has a timestamp it is
used and `recorded_at` is the migration's own time; where it does not, `occurred_at` equals
`recorded_at` and the payload says so.

### 7.4 How old and new readers coexist

- The event store and the reader are **added**; not one of the 15 readers is changed in session 5.
  Session 6 migrates the first four.
- `evidence_spans.value` and `papers.status` keep their current meaning as **the projection**, and
  the migration asserts they equal what the reader returns. The plan's "`papers.status` is retained
  during migration as a projection" is honoured literally.
- Because nothing reads the new tables in session 5, a defect in the reader cannot reach an export.
  That is the point of the slice.

### 7.5 The session-5 gate — old-vs-new reader agreement on live data

The gate is: **on the live data, the new reader agrees with each old reader wherever the old reader
was unambiguous, and every disagreement is listed with its cause.**

Design:

1. **Scope.** Four readers, chosen because they are session 6's migration targets: `evidence_table._build_evidence_rows`, `concordance.load_arm` (local and each cloud arm), `corpus.is_corpus_member`, and the judge loader's cell set.
2. **Comparison unit.** The cell `(paper_id, field_name, arm)` for the value readers — **11,380 comparisons** (3,800 local codebook cells + 7,580 cloud cells) — and the paper for `is_corpus_member` (10,039).
3. **"Unambiguous" is defined in advance, not discovered.** An old reader is unambiguous for a cell when exactly one extraction exists for that `(paper, arm)` — **which is every cell on this database** (§2.2a) — and the cell is a codebook field. The 2 artefact-field spans are declared ambiguous up front (U5).
4. **Expected disagreements, predicted before the run**, so that a surprise is visible as a surprise:
   - **183 local + 166 cloud absence sentinels** where `concordance.load_arm` drops the cell (`if … in non_value: continue`) and the new reader returns a state. These are **known and intended** divergences — D1-4 is the defect the reader exists to fix — and they are listed, not counted as failures.
   - **2 artefact-field cells** (U5).
   - **0 expected from supersession**, because no paper has two extractions.
   - Everything else must agree exactly.
5. **Runs read-only.** The check opens `mode=ro` and writes its output to the read-out directory; it does not touch `review.db`.
6. **Failure is a listed disagreement with a named cause, not a count.** A disagreement whose cause cannot be named stops the session.

**Prediction recorded now, so the gate can fail:** the agreement check should return exactly
**349 intended divergences** (183 + 166 sentinel drops) plus **2 declared-ambiguous** cells, and
**zero** unexplained. If it returns anything else, something in §5 is wrong.

---

## §8 Slice boundary

| | **session 5** (S2 core, Phase 2) | **session 6** (readers) | **session 12** (importer + stage derivation) |
|---|---|---|---|
| builds | migrations 016–020; `paper_events` + `field_events` + triggers; `extraction_uid`; `effective_value` / `effective_state`; `classify_field_state`; the §4 rule as tested code | migrates concordance, exporter, judge loader and corpus predicate onto the reader; S1c judge universe; S3h eligibility-vs-processing | one importer with ACCEPT · CORRECT · WITHDRAW; stage completion derived; manual advances as events; the two legacy importers retired; `audit_adjudication` dropped |
| does **not** | change any of the 15 readers; drop `audit_adjudication`; write an importer; derive a stage | add new event types | — |
| reproducers it owns | **D1-4** (concordance abstention) as a **fixture**, since no live paper has two extractions | **D1-4** on the live path, once `load_arm` reads through the reader | **D1-1** (REJECT semantics), **D1-2** (stale workbook), **D1-3** (partial audit completes the stage) |
| D2-15 three-pair fixture | **built here** — two extractions, one human decision, and the three reader pairs must return identical answers. It is a fixture because the live database cannot exercise pair 1. | re-run against the migrated readers | re-run against the one importer, where pair 2 disappears by construction |

**Assignment of the four D1 reproducers, and why they are not all in session 5.** D1-1, D1-2 and
D1-3 all concern the *importer*: what REJECT means, what a stale workbook binds to, and whether a
partial audit completes a stage. Session 5 writes no importer, so a reproducer for any of them
would have nothing to run against. They belong to session 12, which is where the plan already puts
them ("D1-1, D1-2, D1-3 reproducers pass"). **D1-4 is different** — it is a *reader* defect, so its
fixture belongs to session 5 and its live form to session 6.

### Dependencies on other units, stated rather than absorbed

Per the brief's constraint, where the design touches another unit the dependency is named and the
work stops:

| dependency | unit | what session 5 does about it |
|---|---|---|
| `run_id` FK to the run manifest | **S3b**, session 7 | column declared nullable with `run_marker='pre-manifest'`; **no FK** |
| `field_state` written by the extraction write path | **S5b**, session 9 | the reader derives it; the write path is not touched |
| `criterion_id`, offsets, verifier verdict | **S4**, sessions 18–20 | columns reserved, unused, documented as reserved |
| presented-context hash produced by a UI | **S7**, sessions 14–15 | column declared and populated by the importer in session 12; NULL before that |

None of these requires a change to those units in session 5.

---

## §9 Questions for the PI

**Q1 — two human decisions on one claim (U1).** Newest wins? Or refuse to resolve and mark
"unresolved: needs re-review"? *Recommendation:* **refuse**. Single-reviewer today makes this
free, and "newest wins" is the rule that would silently discard a second reviewer the day S7 gains
one.

**Q2 — re-extraction that reproduces the identical value under a human decision (U2).** Row 3
sends it to "needs re-review" because the claim id changed. *Recommendation:* **keep row 3 as
written**, but have the reader mark the provenance `value_unchanged: true` so the app can offer a
one-click re-affirm. Suppressing it in the rule would make the rule depend on value equality, which
is exactly the comparison S1b spent a session proving is hard.

**Q3 — cloud arms cannot supersede (U3).** `UNIQUE(paper_id, arm)` on `cloud_extractions` makes a
second cloud extraction impossible. Options: (a) drop the constraint in migration 017 so cloud
behaves like local; (b) keep it and state that cloud arms are single-shot. *Recommendation:* **(a),
drop it**, in the same migration that adds `extraction_uid`. Keeping it means the resolution rule
is true for one arm class and false for the other, and the difference is invisible at the call
site.

**Q4 — the reader's signature (U4).** S2 writes `effective_value(paper, field)`; row 13 needs an
arm. Options: (a) `effective_value(paper, field, arm)` with arm required; (b) a designated result
arm in the spec, defaulting to `local`, with the three-argument form available. *Recommendation:*
**(a)**. A default arm is the kind of implicit configuration C1 exists to remove.

**Q5 — reserved columns (§3.3).** Declare S4's `criterion_id`, offsets and verifier triple now, or
add them in session 18? *Recommendation:* **declare now**. Adding a column to an append-only table
later means old rows carry a NULL that is ambiguous between "not applicable" and "predates the
column"; declaring now makes it unambiguously the former.

**Q6 — the two artefact fields (U5).** `field_1` (paper 719) and `Title` (paper 415) are stored
spans naming no codebook field, and they are the *only* spans those two extractions have.
Options: (a) the reader ignores non-codebook fields and the rows stay; (b) a one-time cleanup
deletes them in session 5; (c) they migrate as events with a `not_in_codebook` state.
*Recommendation:* **(a) now, (b) in session 12**, with the deletion recorded as an event. Deleting
in session 5 would remove the only evidence that papers 415 and 719 produced an extraction at all,
before the event store has a place to say so.

**Q7 — `flagged` and `contested` (U6).** 1,601 spans carry an AI auditor verdict that is not one of
the eight S5b states. Options: (a) provenance only, field state from value+citation; (b) a ninth
S5b state. *Recommendation:* **(a)**. S5b is a vocabulary for *the value's* standing; the auditor's
opinion is evidence about the value, not a different kind of value. But this changes what an
exporter shows for 1,601 spans, so it is the PI's call.

**Q8 — the 416 human abstract exclusions (F2).** They exist only as a status mutation and a prose
string; the workbook is on disk. Options: (a) migrate as `state_at_migration`, decision recorded as
"human, identity of the 416 not in the database"; (b) parse `screening_queue_20260310.xlsx` in
migration 020 and reconstruct 416 human events. *Recommendation:* **(a)**. R-f says no invented
history, and a workbook with no claim binding, no presented-context hash and a six-month-old mtime
is a weaker record than an honest "state at migration". If the PI wants (b), it should be its own
task with its own verification, not a clause inside a migration.

**Q9 — the 7 workflow stamps (F3).** Options: (a) record as migration provenance and let S2's
derivation recompute every stage; (b) migrate as seven `paper_events` with no paper scope.
*Recommendation:* **(a)**. (b) would create seven events that assert something about a set of
papers the event cannot name, which is the shape of the defect A6 describes.

**Q10 — where the PI v2 audit adjudication lands (§6).** Is v2 a measurement of the judge only, or
does the PI intend any of its 200 rows to change an exported field value? *Recommendation:*
**measurement only**, unchanged. If any row is to change a value, `pi_audit_sampler_v2` must emit
claim ids, which is a session-12 change to that sampler and should be scoped as one.

**Q11 — `human_extractions` (F1).** `CLAUDE.md` documents the table; it does not exist. Is the
human arm live work, dropped, or held elsewhere? *Recommendation:* the PI states which, and
`CLAUDE.md` is corrected in the same act. The resolution rule needs to know whether `human_A`…`D`
are arms it must resolve.

---

## §10 Assumptions this read-out made and did not measure

**Four of the eight were measured before this read-out was committed** and are recorded below as
resolved rather than deleted, so the reader can see what was assumed and what settled it.

| tag | assumption | why it is unmeasured | what falsifies it |
|---|---|---|---|
| **A-1** | ~~SQLite reuses a freed rowid for a plain `INTEGER PRIMARY KEY`~~ — **RESOLVED, MEASURED** | Demonstrated on a throwaway `:memory:` database (`review.db` untouched, nothing written to disk). Insert 3 rows, delete the highest, insert one more: a plain `INTEGER PRIMARY KEY` returns `[(1,'a'),(2,'b'),(3,'NEW')]` — **id 3 reused by a different row**; the same sequence on `INTEGER PRIMARY KEY AUTOINCREMENT` returns `[(1,'a'),(2,'b'),(4,'NEW')]` — **not reused**. §2.2b and the `AUTOINCREMENT` requirement in §3.1 are measured, not inferred. | — |
| **A-2** | ~~The 42 and 323 missing cells are genuine absences~~ — **RESOLVED, MEASURED** | Enumerated per cell, not derived: for every extraction, the codebook's 20 field names were differenced against the field names that extraction actually has a span for. Local **3,800 cells, 3,758 codebook-field spans, 42 missing**; cloud **7,580 cells, 7,257 spans, 323 missing**. Both match the arithmetic in §5 exactly. Also measured: **0** cloud spans name a non-codebook field, so the two artefact fields are local-only. | — |
| **A-3** | ~~The codebook's 20 fields are the right denominator~~ — **RESOLVED, MEASURED** | The codebook yields **20** field names; `analysis/provenance/field_class3.py::FIELD_CLASS3` holds **20** entries and `set(FIELD_CLASS3) == set(codebook)` is **True**. The codebook's `field_class` counts are **stated 9 · inferable 6 · judgment 5**, which is `CLAUDE.md`'s STATED (9) / INFERABLE (6) / JUDGMENT (5). (Note: the module is `analysis/provenance/field_class3.py`; `CLAUDE.md` says "mirrored in `field_class3.py`" without the path, and there is no `engine/elicitation/field_class3.py`.) | — |
| **A-4** | An append-only trigger cannot be bypassed by the writers in this repo | No writer uses `PRAGMA ignore_check_constraints` or drops triggers today; not exhaustively grepped | A writer that disables triggers |
| **A-5** | The v2 PI audit workbook carries no claim binding | From `CLAUDE.md`'s description of `pi_audit_sampler_v2` and `pi_audit_unblind` ("no DB access"); the workbook files were not opened | A `claim_id`-like column in the v2 key workbook |
| **A-6** | `screening_queue_20260310.xlsx` contains the 416 decisions | Inferred from the `workflow_state` metadata string naming that file; **the workbook was not opened** | The file not containing per-paper decisions |
| **A-7** | ~~No reader outside the three trees~~ — **RESOLVED, MEASURED, and it refines §1.1** | `tests/` and `artifacts/` were searched. **Three SELECTs exist, all in `tests/`**: `tests/test_ft_screening.py` (`SELECT * FROM ft_screening_adjudication WHERE paper_id = ?`) and `tests/test_audit_adjudication.py` (two, `SELECT * FROM audit_adjudication …`). All three are **assertions inside the importers' own tests** — they read back what the test just wrote. They are not production readers, so §1.1's "nothing in the codebase reads them" stands **for production**, and the precise statement is: *the only non-test reader of any adjudication table is `prisma.py`'s one COUNT*. Worth noting for §6: the two `audit_adjudication` test SELECTs are the only things that will notice its retirement. |
| **A-8** | Session 5's agreement gate can run read-only | It compares two readers over live data and writes only its report | A reader that opens read-write — **and `concordance.load_arm` does**: `conn = sqlite3.connect(db_path)` with no URI, which is INSTRUMENTS-01's item I5. The gate must therefore call it against a **copy**, not the live file. Recorded here because it changes how the gate is built. |
