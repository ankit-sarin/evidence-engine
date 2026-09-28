# WRITE-PATH-01 9e — staged-entry design read-out (R182(6))

Read-only census at HEAD `b0ee3bc`, 2026-09-28. Live `review.db` was opened `mode=ro`
only; no `ReviewDatabase` was constructed on live. Every claim carries a quoted content
anchor. Counts come from SQL. A classification made by eye is marked **(by eye)**.
Nothing here is a design. §5 lists options and recommends none.

---

## §1 The search stage's write set (I1)

**Modules.** `engine/search/` (`pubmed.py`, `openalex.py`, `dedup.py`, `models.py`) makes
`Citation` objects and writes nothing. The one database write is
`ReviewDatabase.add_papers`, called by `scripts/run_pipeline.py::_stage_search`
(`added = db.add_papers(unique)`) and by `scripts/test_e2e_search_screen.py`.

**Table written: `papers` only.** `add_papers` issues:

```
INSERT INTO papers
   (pmid, doi, title, abstract, authors, journal, year,
    source, status, created_at, updated_at)
   VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'INGESTED', ?, ?)
```

It skips a citation whose pmid already exists ("Skip if PMID already exists") and
swallows `sqlite3.IntegrityError` ("UNIQUE constraint on pmid — skip"). It writes no
event, no `run_calls` row, no dedup record (`"duplicates_removed": 0,  # tracked externally
by dedup module` in prisma.py), and no `ee_identifier`.

**Manifest.** `add_papers` opens none. **But the search stage does run inside one when
driven by `run_pipeline`.** `_open_run_manifest` runs before any stage, and the stage runs
inside `run_token = rm.activate(db._conn, run_id)`. The manifest kind is derived:
`kind = "extraction" if any(s.startswith(("extract", "elicitation", "audit")) for s in
stages) else "screening"`. Search contributes no stage row:
`_PIPELINE_STAGE_CONFIGS` has no `"search"` key.

**Other ways `papers` rows arrived on live** (these bear on Q1's premise):
- **Migration 003** (`003_backfill_expanded_screening.py`) wrote `INSERT INTO papers …
  source, status …` with a *derived* status (`_determine_status(screening_decision,
  ver_row)`), plus `abstract_screening_decisions` and `abstract_verification_decisions`.
  The expanded search's screening script "writes screening results to flat CSV files
  instead of the review database" (`scripts/screen_expanded.py`, its TODO(retention)
  block). The migration was the importer.
- **One `hand-search` paper** (id 605, `FT_SCREENED_OUT`, `EE-453`, created
  2026-03-12T05:49:46Z). No code path in `engine/`, `scripts/` or `analysis/` names
  `hand-search`. It was inserted by hand.

Live: `papers` by source — openalex 8,964 · pubmed 1,074 · hand-search 1.
`ee_identifier` non-null: 648.

**I1 verdict: true for the stage's own write set** (`papers` only, `status 'INGESTED'`,
no events). **False as stated about manifests:** under `run_pipeline` the search stage
runs inside a `screening` or `extraction` manifest, though it writes no row that names
the run. The live corpus also shows two non-search entry routes (migration 003 and a
manual insert).

---

## §2 The screening stages' write set today (I2)

| stage | module / entry | tables written | status tokens written | manifest |
|---|---|---|---|---|
| Abstract primary + verifier | `engine/agents/screener.py::run_screening`, via `run_pipeline._stage_screen` | `abstract_screening_decisions` (`db.add_screening_decision(pid, 1, …)`, `(pid, 2, …)`), `abstract_verification_decisions` (`db.add_verification_decision(`), `workflow_state` (`complete_stage(`) | ABSTRACT_SCREENED_IN, ABSTRACT_SCREENED_OUT, ABSTRACT_SCREEN_FLAGGED | **yes when run by `run_pipeline`**: stage rows `("abstract_screen_primary", "abstract_screen_verifier")`, calls recorded by `record_active_ollama_call` |
| Expanded abstract screen | `scripts/screen_expanded.py` | **none** ("without modifying review.db"; writes `screening_results.csv`, `abstracts.jsonl`) | none | none |
| FT primary | `engine/agents/ft_screener.py::run_ft_screening` (CLI `python -m engine.agents.ft_screener`) | `ft_screening_decisions` (`db.add_ft_screening_decision(`), `workflow_state` | FT_ELIGIBLE, FT_SCREENED_OUT, FT_FLAGGED | **none**: the CLI does `db = ReviewDatabase(args.review)` then `run_ft_screening(...)`; no `open_run` / `activate` in the module |
| FT verifier | `ft_screener.py::run_ft_verification` | `ft_verification_decisions` (`db.add_ft_verification_decision(`), `workflow_state` | FT_FLAGGED (from FT_ELIGIBLE) | none |
| Abstract adjudicator | `engine/adjudication/screening_adjudicator.py` | `abstract_screening_adjudication` (`INSERT INTO abstract_screening_adjudication`), `workflow_state` | `new_status = "ABSTRACT_SCREENED_IN" if decision == "INCLUDE" else "ABSTRACT_SCREENED_OUT"` | none |
| FT adjudicator | `engine/adjudication/ft_screening_adjudicator.py` | `ft_screening_adjudication` (`INSERT INTO ft_screening_adjudication`), `workflow_state` | `review_db.update_status(int(paper_id), decision)`, decision ∈ `("FT_ELIGIBLE", "FT_SCREENED_OUT")` | none |

No screening module writes a paper event. `write_paper_event` has three callers:
`extraction_events.py`, `audit_events.py` and migration 017 (§3, I4).

**Where FT_ELIGIBLE is decided — exactly two sites:**
1. **FT primary, before verification.** `run_ft_screening`, after
   `db.add_ft_screening_decision(`:
   ```
   elif decision.decision == "FT_ELIGIBLE":
       db.update_status(pid, "FT_ELIGIBLE")
   ```
   It is guarded by `if current_status in _PAST_FT:` ("FT decision recorded without
   status change"). **The primary's include is written before the verifier runs**; the
   verifier then reads `db.get_papers_by_status("FT_ELIGIBLE")` and may flag.
2. **FT adjudicator.** `review_db.update_status(int(paper_id), decision)` with
   `decision == "FT_ELIGIBLE"` (from FT_FLAGGED; "Update paper status (FT_FLAGGED →
   FT_ELIGIBLE or FT_SCREENED_OUT)").

The reversal FT_ELIGIBLE → FT_FLAGGED happens at the verifier's three unguarded sites
(`logger.warning("Paper %d has no parsed text — marking FT_FLAGGED", pid)`, the malformed
branch, and `else: db.update_status(pid, "FT_FLAGGED")`), per M3.

**I2 verdict: partly false.** Abstract screening driven by `run_pipeline` opens a
`screening` manifest and records its calls. FT screening, FT verification, both
adjudicators and `screen_expanded.py` open none.

---

## §3 The event writer's constraints (I3)

**`paper_events`**, live DDL verbatim (019's shape; the run-link CHECK is 020's):

```
actor_kind  TEXT NOT NULL CHECK (actor_kind IN ('model', 'human', 'engine')),
actor_role  TEXT NOT NULL CHECK (actor_role IN ('reviewer', 'extractor', 'system')),
to_state    TEXT NOT NULL CHECK (to_state IN ('eligible', 'abstract_out', 'full_text_out', 'parsed', 'extracted', 'extraction_failed', 'full_text_not_obtainable', 'parse_failed', 'input_exceeds_context', 'audited_ai')),
CHECK (actor_role <> 'system' OR actor_kind = 'engine'),
CHECK (event_type IN ('identified', 'duplicate_of', 'screened', 'verified', 'adjudicated', 'acquired', 'not_obtainable', 'parsed', 'extracted', 'extraction_failed', 'audited', 'manual_advance', 'bypass', 'state_at_migration')),
-- R39: an event's type and its to_state must name the same axis.
CHECK (
    (event_type IN ('identified', 'duplicate_of', 'screened', 'verified', 'adjudicated') AND to_state IN ('eligible', 'abstract_out', 'full_text_out'))
 OR (event_type IN ('acquired', 'not_obtainable', 'parsed', 'extracted', 'extraction_failed', 'audited') AND to_state IN ('parsed', 'extracted', 'extraction_failed', 'full_text_not_obtainable', 'parse_failed', 'input_exceeds_context', 'audited_ai'))
 OR (event_type IN ('manual_advance', 'bypass', 'state_at_migration'))
),
-- R39 / S3h: a reason for exactly the failure tokens, and for no other.
CHECK (
    (to_state IN ('extraction_failed', 'full_text_not_obtainable', 'parse_failed', 'input_exceeds_context') AND reason_code IS NOT NULL)
 OR (to_state NOT IN ('extraction_failed', 'full_text_not_obtainable', 'parse_failed', 'input_exceeds_context') AND reason_code IS NULL)
),
-- R68: a row with no run is a seeded, pre-manifest row and nothing else.
CHECK (
    (run_id IS NOT NULL AND run_marker IS NULL)
    OR
    (run_id IS NULL AND run_marker IS 'pre-manifest')
)
```

`reason TEXT` and `stage_name TEXT` carry **no CHECK**.

**Consequences for Q1's vocabulary, read from the DDL:**
- **`eligible` with `reason_code` = `external_screen` is refused today.** `eligible` is
  not a failure token, so the reason CHECK requires `reason_code IS NULL`. The free-text
  `reason` column would accept it, but that column is not CHECK-constrained, so it is
  not "019's pattern". Carrying a constrained reason on an eligibility event needs the
  CHECK rewritten.
- **There is no "external" actor.** `actor_kind` is model / human / engine and
  `actor_role` is reviewer / extractor / system. "An actor that says the decision was
  external" needs a new value in one of the two CHECKs, or must be expressed through
  `actor_name` (unconstrained).
- **`identified` and `duplicate_of` event types exist, but no eligibility token records
  identification or duplication.** Such an event could only carry `eligible`,
  `abstract_out` or `full_text_out`. **(by eye)** They are writable only with a
  to_state that does not describe them — the same condition 016 was in for
  `extraction_failed` before 019.

**Python-side writer checks on a paper event.** `write_paper_event` calls only
`_run_link`:
- `"event refused: run_id is required (R68)"`
- `"event refused: run_id {run_id} names no run manifest (R68)"`
- `run_marker` accepted only `migration=_called_from_migration(2)`

No arm, input-identity or assignment check applies to paper events.

**`run_manifests`** (020), live DDL: `run_kind TEXT NOT NULL CHECK (run_kind IN
('extraction', 'screening', 'judge', 'review_session'))`. Also `RUN_KINDS = ("extraction",
"screening", "judge", "review_session")` in `run_manifest.py`, enforced before the INSERT
by `if kind not in RUN_KINDS: raise ValueError`. **An `import` kind needs both the CHECK
and the constant.**

What an import run with no model stage must still supply (NOT NULL columns, and what
`open_run` fills them from):

| column | constraint | source in `open_run` | an import with no model stage |
|---|---|---|---|
| `git_commit` | `CHECK (length(git_commit) = 40)` | `git_state()` | available |
| `git_dirty` | `CHECK (git_dirty = 0)` | `if g.dirty: raise DirtyTree(` | the tree must be clean |
| `spec_hash` | NOT NULL | `spec_hash(spec)` | **a spec must exist and load** |
| `codebook_hash`, `codebook_sha256` | NOT NULL | `codebook.semantic_hash`, `codebook.sha256` | **a codebook must exist beside the db** |
| `library_versions_json`, `host`, `started_at` | NOT NULL | runtime | available |
| `cloud_arms_json` | NOT NULL DEFAULT '[]' | `cloud_list` | `[]` |
| `payload_description` | `CHECK (cloud_arms_json = '[]' OR payload_description IS NOT NULL)` | argument | optional when no cloud arm |
| `manifest_json`, `manifest_sha256` | NOT NULL | the manifest dict | see below |

**There is no column and no `open_run` parameter for an input file's sha256** (Q1: "pins
the input file's sha256"). The manifest body's keys are fixed in `open_run`: `"run_uid",
"review_id", "run_kind", "git", "engine_state", "spec_hash", "codebook", "libraries",
"host", "started_at", "cloud_arms", "payload_description", "stages", "pins"`. **(by eye)**
Pinning an input could ride in `manifest_json` as a code change with no migration, or in
a new column (a migration). `review_session` already shows a run with zero stage rows
(`open_run(conn, spec, kind="review_session", stages=(), …)`).

**`field_events`** (for §4c), live:
`CHECK (event_type IN ('asserted', 'declined', 'contract_unmet', 'superseded',
'human_accepted', 'human_corrected', 'human_withdrew', 'duplicate_detected',
'citation_located', 'state_at_migration'))`, with the same `actor_kind` / `actor_role`
CHECKs and the same R68 run-link CHECK. `arms.arm_kind` is `CHECK (arm_kind IN ('model',
'human_extractor'))`.

**I3 verdict: true**, with the refinements above: `reason` is unconstrained, and the
reason CHECK forbids a reason code on `eligible`.

---

## §4 The three importers

| | (a) extraction-entry | (b) screening-entry | (c) human-arm load |
|---|---|---|---|
| **input** | a corpus list + one parsed text per paper (file); identity to pin: the list file's sha256 and each text's sha256 | a search export (citation list); identity: the export's sha256 | `Extraction_Workbook_v2` `.xlsx` per extractor (§4c) |
| **tables written** | `papers` (as §1); `parsed_text_refs` (+ optionally `full_text_assets`); `paper_events` eligible; `workflow_state` (see below) | `papers` only (the §1 write set) | field events on a `human_extractor` arm; `arms` registration |
| **events** | `eligible` per paper | none (search writes none, §1) | `asserted` claims (+ `citation_located`?) |
| **preconditions** | a spec and a codebook load (§3 manifest columns); pmid uniqueness (`add_papers` skips duplicates silently) | same; pmid uniqueness | papers exist with a mapping from `EE-NNN` (`papers.ee_identifier`, 648 non-null) |
| **vocabularies needed (§3)** | `run_kind` 'import'; an external-actor value; a constrained reason on `eligible` (the reason CHECK rewritten); the input-sha pin | `run_kind` 'import' (if it runs under a manifest at all, since it writes no event) | `run_kind` 'import' or `review_session`; the input-identity rule for human claims (below) |

**(a) Parsed text (I5).** `parsed_text_refs` has exactly one writer,
`engine/core/parsed_text.py::record_parsed_text(conn, *, paper_id, path, version, data,
source_asset_id, recorded_at=None)`. Its preconditions, all readable in the function:
- `paper_id` FK to `papers`.
- `UNIQUE (paper_id, parsed_text_version)`, with `next_version(conn, paper_id)` available.
- `source_asset_id` may be `None` (the column `source_full_text_assets_id INTEGER` is
  nullable).
- The sha256 is computed from the `data` bytes passed in (`sha =
  hashlib.sha256(data).hexdigest()`).
- The path is stored through `canonical_path`: "relative to the repo root when under it,
  absolute otherwise".
- "Does NOT commit — it belongs to the caller's unit of work (D8)".

Its only caller today is `pdf_parser.py`. `full_text_assets` writers:
`pdf_parser.py` (`INSERT INTO full_text_assets (paper_id, pdf_path, pdf_hash,
parsed_text_path, …)`), `engine/acquisition/verify_downloads.py` (insert and `UPDATE
full_text_assets SET pdf_path`) and `scripts/advance_to_pdf_acquired.py`. **An
extraction-entry importer has a write path for a supplied text file with no parser and no
`full_text_assets` row.** No `parsed` processing event is written by the parser today,
so none is required by any reader.

**(a) Workflow gate.** `run_pipeline` refuses post-screening stages until the screening
workflow is complete: `_POST_SCREENING_STAGES = {"parse", "extract", "audit", "export"}`
… `if not is_adjudication_complete(db._conn): … logger.error("BLOCKED: Adjudication
workflow incomplete.")`. **(by eye)** On a review entered at extraction, the screening
stages in `workflow_state` would have to be completed by the importer, or the gate would
have to read the manifest kind.

**(c) Human-arm load.**
- **I6, the input contract, verbatim.** Sheet `"Extraction Form"`; `_HEADER_ROW = 2`,
  `_DATA_START_ROW = 3`; "Column layout: A–D identifiers, E–X extraction fields, Y–Z
  source quotes, AA notes".
  - Required headers are the 20 names in `_EXTRACTION_FIELDS`: `"study_type",
    "robot_platform", "task_performed", "sample_size", "surgical_domain",
    "autonomy_level", "validation_setting", "task_monitor", "task_generate",
    "task_select", "task_execute", "system_maturity", "study_design", "country",
    "primary_outcome_metric", "primary_outcome_value", "comparison_to_human",
    "secondary_outcomes", "key_limitation", "clinical_readiness_assessment"`.
  - Optional headers: `"SQ: key_limitation [REQ]"`, `"SQ: clinical_readiness [REQ]"`,
    `"Extractor Notes"`.
  - Column A is the paper id, matched against `^EE-\d{3}$`. The extractor id comes from
    the filename (`*_A.xlsx`).
  - Cell normalisation: `"blank and 'NR' -> None"`.
  - Stored long in `human_extractions (paper_id TEXT, extractor_id, field_name, value,
    source_quote, notes, imported_at, UNIQUE(paper_id, extractor_id, field_name))`,
    self-provisioned with `CREATE TABLE IF NOT EXISTS`. That table is **absent on live**
    (0 rows in `sqlite_master`).
- **What the event vocabulary allows today for a human claim:**
  - `actor_kind 'human'` + `actor_role 'extractor'` + `event_type 'asserted'` on an arm
    with `arm_kind 'human_extractor'` passes the CHECKs.
  - The writer then applies `if actor_role == "extractor" and event_type in
    CLAIM_EVENT_TYPES:` and refuses a claim without `PAYLOAD_REUSE_KEY,
    PAYLOAD_PARSED_TEXT_SHA256, PAYLOAD_PARSED_TEXT_UID` ("An extractor's claim names the
    input it was made from"). **A human who read the PDF has no parsed-text uid.**
  - `_refuse_claim_on_arm`'s run-pin check applies only `if kind == "model"`, so human
    arms are not pinned.
  - `is_assigned` returns `row[0] == "model"`: "A `human_extractor` arm is assigned
    nothing until the assignment table arrives with human arm loading (session 12)".
    **Every human cell reads as out of scope (row 0) today.**
  - `citation_located` exists. Only 2 of the 20 fields carry a source quote in the
    workbook (`_SQ_FIELDS`).

**I5 verdict: true** — a generic write path exists (`record_parsed_text`), and its
preconditions are listed above. **I6 verdict: reported.** The contract above is verbatim
for the architect's comparison. The ledger's phrasing "one row per field per paper" is
**false for the input**: the workbook is wide (one row per paper, 20 field columns). It is
long only in storage.

---

## §5 The eligibility bridge (Q5)

**The gap, restated from 9e-C-P1 §7:** "the screening route writes no eligibility event;
the 190 `eligible` rows were seeded by migration 017. On a review screened in freshman,
FT_ELIGIBLE papers would have no `eligible` event, so identity B1 would fail and
extraction selection would select nothing."

**Candidate sites** (from §2). Every option needs a `run_id` (R68). No FT screening path
opens a manifest today, so each option also needs **a manifest opened by that path**
(kind `screening` exists).

| option | write site | event written | what it needs that does not exist | serves FT_SCREENED_OUT as `full_text_out`? | the reversal FT_ELIGIBLE → FT_FLAGGED |
|---|---|---|---|---|---|
| **A. At the primary's include** | `run_ft_screening`, beside `db.update_status(pid, "FT_ELIGIBLE")` | `screened` → `eligible` (actor model / reviewer? or engine / system) | a `screening` manifest opened by the FT CLI; an actor choice within the CHECKs | yes: the same site's `else: db.update_status(pid, "FT_SCREENED_OUT")` can write `screened` → `full_text_out` | **not covered**: the verifier flags *after* `eligible` is written, and there is no event for "pending adjudication". A flagged paper stays `eligible` on events. Identity 1(b) then fails by design (R196), and extraction selection would take it |
| **B. After verification** | `run_ft_verification`, on the verifier's `FT_ELIGIBLE` decision (today it writes nothing on confirm: `if decision.decision == "FT_ELIGIBLE": stats["confirmed"] += 1`) | `verified` → `eligible` | a manifest opened by the verifier CLI; primary-only reviews (`--screen-only`) would never get `eligible` | no: the verifier never excludes (it flags); exclusions stay at the primary or the adjudicator | avoided: `eligible` is written only on confirm. A flagged paper has no eligibility event until adjudication |
| **C. At adjudication** | `ft_screening_adjudicator`, beside `review_db.update_status(int(paper_id), decision)` | `adjudicated` → `eligible` / `full_text_out` (actor human / reviewer) | a manifest (a `review_session` exists: `open_review_session`); covers only flagged papers, so it must be paired with A or B for unflagged ones | yes, for adjudicated papers | resolves it: the human decision is the last word |
| **D. Derive, don't write** | a reader: eligibility from `papers.status` when no eligibility event exists | none | a second source of truth on the eligibility axis, the thing R29/R39 and `effective_state` exclude ("`papers.status` is never read") | n/a | n/a |
| **E. A seed at the screening/extraction boundary** | a step (script or `run_pipeline` pre-extract) that writes `eligible` for every FT_ELIGIBLE paper lacking one | `bypass` / `manual_advance` → `eligible` (both-axis types) or `screened` | a manifest; a statement of which status tokens count (the frozen set is the obvious one, R35 forbids reading it outside migrations) | yes, if extended to FT_SCREENED_OUT / ABSTRACT_SCREENED_OUT | covered only if it re-runs; a reversal after the seed leaves a stale `eligible` |

Common to all: no `reason_code` may accompany `eligible` or `full_text_out` (§3). The
option table is descriptive; no option is recommended.

---

## §6 PRISMA and the manifest kind

**Boxes whose meaning changes per entry point (by eye, from prisma.py's keys):**

| entry | boxes that change meaning |
|---|---|
| screening-entry | `records_identified` / `records_by_source`: from an import, not a search run; `duplicates_removed` is still the literal `0`; the rest as today |
| extraction-entry | every screening-side box: `records_screened`, `records_excluded`, `screen_flagged`, `reports_sought`, `reports_not_retrieved`, `full_text_retrieved`, `full_text_assessed`, `eligibility_exclusions` — "not performed here". `n_eligible` is the import's corpus |
| search / full run | unchanged |

**What the seam must admit.** Identity 1 today (9e-R1a R186 1) compares
`remainder_ids = {pid for pid, s in status_of.items() if s not in SCREENING_TOKENS}` with
`eligible_ids = set(eligible_paper_ids(conn))` and names each id in either difference. An
extraction-entry paper would be `eligible` on events with a status the importer chooses:
- If `INGESTED` (the §1 write set), it lands in (b): "papers eligible on the eligibility
  axis but at a screening token on papers.status".
- If `FT_ELIGIBLE`, it passes the seam unchanged.

Admitting `external_screen` therefore depends on the importer's status write and on how
the external marker is stored. Per §3 it cannot be a `reason_code`, so it would be
readable only through `actor_*`, `reason`, or the run's `run_kind`.

**Live screening tables (mode=ro, SQL):**

| table | rows |
|---|---|
| abstract_screening_decisions | 21,374 |
| abstract_verification_decisions | 1,422 |
| abstract_screening_adjudication | 0 |
| ft_screening_decisions | 366 (FT_ELIGIBLE 198 · FT_EXCLUDE 168) |
| ft_verification_decisions | 182 |
| ft_screening_adjudication | 36 (FT_ELIGIBLE 31 · FT_SCREENED_OUT 5) |
| full_text_assets | 794 |
| parsed_text_refs | 194 |
| workflow_state | 12 |
| paper_events | 190 |
| run_manifests | 0 |

All 190 AI_AUDIT_COMPLETE papers have at least one `ft_screening_decisions` row (0
without).

---

## §7 Findings and open questions

**Findings**
1. **Contradicts a plan-v62 premise (Q1).** "`eligible` with reason `external_screen`" is
   refused by the live reason CHECK (`to_state NOT IN (…failure tokens…) AND reason_code
   IS NULL`). A constrained external reason on an eligibility event needs that CHECK
   rewritten in session 10's migration, or the marker must live elsewhere (§3).
2. **Qualifies Q1 / Q3 ("search writes no events").** True of the write set. But under
   `run_pipeline` the search stage runs inside a `screening` or `extraction` manifest,
   and abstract screening under `run_pipeline` records its calls (§1, §2). The premise
   holds; the "no manifest" reading does not.
3. **The live corpus already has two non-search entry routes**: migration 003 (the
   expanded search, 8,000+ openalex rows) and one hand-inserted row (id 605). Neither
   left a record of its input (§1).
4. **`identified` and `duplicate_of` exist as event types with no to_state that
   describes them** (§3). A screening-entry importer that wanted an identification event
   has none to write.
5. **The FT decision token and the FT status token differ for exclusion.** The primary's
   `FTScreeningDecision.decision` is `Literal["FT_ELIGIBLE", "FT_EXCLUDE"]` (168 live
   rows carry `FT_EXCLUDE`) and maps to status `FT_SCREENED_OUT`. The verifier's
   `FTVerificationDecision.decision` is `Literal["FT_ELIGIBLE", "FT_FLAGGED"]`. The
   adjudication table stores the status token (`FT_SCREENED_OUT`). An importer
   supplying screening decisions must pick one vocabulary per table.
6. **The FT primary writes FT_ELIGIBLE before verification** (§2). The status token alone
   does not say "verified".
7. **I6's phrasing is false for the input** (the workbook is wide). The importer drops
   `NR` to `None` (`"blank and 'NR' -> None"`), while R132 declares `NR` the canonical
   absence sentinel. The engine writes it and it requires a citation, so importing `NR`
   as None erases "not reported" as a value.
8. **Human claims are unwritable-as-readable today**: an `extractor`-role claim requires
   parsed-text identity keys, and `is_assigned` returns False for every
   `human_extractor` arm (§4c).

**Open questions for the architect**
1. Where does the external-decision marker live: a rewritten reason CHECK admitting a
   reason code on `eligible`; a new `actor_kind` / `actor_role` value; the run's
   `run_kind = 'import'` alone; or the unconstrained `reason` column?
2. Is the input file's sha256 pinned in `manifest_json` (code, no migration) or in a new
   `run_manifests` column (migration)?
3. Does an extraction-entry import write `papers.status` = `FT_ELIGIBLE` (the seam passes
   unchanged) or `INGESTED` (the seam must admit it)?
4. Does an extraction-entry import complete the screening stages in `workflow_state`, or
   does `run_pipeline`'s adjudication gate learn to read the manifest kind?
5. For human claims: is the input-identity rule scoped to `actor_kind 'model'`, or do
   human claims carry a document identity of their own (the PDF's sha256)?
6. Do `identified` / `duplicate_of` get eligibility tokens of their own, or are they
   retired from the event-type CHECK?
7. For the bridge (§5): which of A–E, and what reverses `eligible` when the verifier
   flags (R196)?

---

## Ledger verdicts (INFERRED)

| item | verdict |
|---|---|
| I1 | true for the write set; false on "no manifest" under `run_pipeline` (§1) |
| I2 | partly false: abstract screening under `run_pipeline` opens a `screening` manifest; FT, adjudicators and `screen_expanded.py` open none (§2) |
| I3 | true; `reason`/`stage_name` unconstrained; the reason CHECK forbids a reason code on `eligible` (§3) |
| I4 | true: engine paper-event writers are `extraction_events.py` and `audit_events.py` (processing axis only); `eligible` is written only by migration 017 |
| I5 | true: `record_parsed_text` accepts a supplied file; preconditions in §4a |
| I6 | reported verbatim (§4c); the "one row per field per paper" premise is false for the input |
