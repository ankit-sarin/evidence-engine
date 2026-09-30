# STAGED-ENTRY-01 BRIDGE-A — eligibility bridge, Phase A read-out (with the V1/V2 read and BRIDGE-R1)

Read-only census at HEAD `ce86e301b70f4681cf4c98abc398259a2edd8063`, 2026-09-30, CC session
`4f668f80`. Live `review.db` was opened `mode=ro` only. No file was written during the reads. This
document carries three parts verbatim: the BRIDGE-A report as returned to the architect, the V1/V2
read that followed ruling BRIDGE-R1, and BRIDGE-R1's rulings **as transcribed from the brief that
issued them — transcribed, not yet in the decision log** (they are numbered R242+ at the closeout).

---

## Part A — the BRIDGE-A report (verbatim)

Nothing was written; the tree is clean at `ce86e30` and live's fingerprint is IDENTICAL at the stop.
The contract needs rulings before Phase B: C1, C2, C4 and C5 are implementable only once decided,
and C3 and C6 need a ruling outright. The most important point is the PRISMA seam: under C2, a paper
the primary includes but the verifier hasn't yet confirmed fails identity 1, so `export_prisma_csv`
raises in that interval. Separately, the manifest must also declare the `preflight` stage, and C3
needs a change in `database.py`.

### 1. R1 — the PRISMA seam (I1 is false, but the seam still bites)
**The nine `SCREENING_TOKENS`** (`engine/core/database.py`) are `INGESTED`, `ABSTRACT_SCREENED_IN`,
`ABSTRACT_SCREEN_FLAGGED`, `ABSTRACT_SCREENED_OUT`, `PDF_ACQUIRED`, `PDF_EXCLUDED`, `PARSED`,
`FT_FLAGGED`, `FT_SCREENED_OUT`. `FT_ELIGIBLE` is not among them, so I1 is false.

**The identities as coded** (`engine/exporters/prisma.py::validate_prisma_counts`):
- **Identity 1:** `remainder_ids = {pid for pid, s in status_of.items() if s not in SCREENING_TOKENS}`
  must equal `eligible_ids = set(eligible_paper_ids(conn))` as sets.
  - A paper in the remainder that isn't eligible reports "Seam: papers past screening on
    papers.status but not eligible on the eligibility axis".
  - An eligible paper outside the remainder reports "Seam: papers eligible on the eligibility axis
    but at a screening token on papers.status".
  - The total must also match.
- **Identity 2:** `studies_included + sum(failures) + extraction_in_progress == n_eligible`, where
  `_EXTRACTION_IN_PROGRESS = (NO_RECORDED_STATE, "parsed", "extracted")`.
- `export_prisma_csv` calls `validate_prisma_counts`, which raises `ValueError` on any failure.

**What each C2 outcome does to the seam:**

| state after the bridge | status | event | identity 1 |
|---|---|---|---|
| primary exclude | `FT_SCREENED_OUT` (screening token) | `full_text_out` | passes |
| primary include, not yet verified | `FT_ELIGIBLE` (remainder) | none | **fails** ("past screening … but not eligible") |
| verifier confirm | `FT_ELIGIBLE` (remainder) | `eligible` | passes; identity 2 counts it as in progress |
| verifier flag | `FT_FLAGGED` (screening token) | none | passes |

So a confirmed paper, status `FT_ELIGIBLE` with an `eligible` event, does pass identity 1. The failure
is the gap between the primary's include and the verifier's confirm: a `--screen-only` run, or any
export taken mid-screening, raises in `export_prisma_csv`. R205 recorded a `--screen-only` run as
yielding no `eligible` event "recorded, not a defect". In fact it blocks export.

Adding `FT_ELIGIBLE` to `SCREENING_TOKENS` would break the confirmed row the other way ("eligible …
but at a screening token"). The same gap exists for papers the adjudicator moves `FT_FLAGGED →
FT_ELIGIBLE` until the adjudication-import commit lands.

**Live today:** eligible 190, remainder 190, both differences empty, so identity 1 passes.

### 2. R2–R9
**R2 and R7 — decision sites and transactions** (I2 true, I7 answered). Every write uses `db._conn`
with default isolation.
- `add_ft_screening_decision` and `add_ft_verification_decision` both end with
  `self._conn.commit()`, so the decision row always commits on its own.
- `update_status`, called outside a transaction, runs its own `BEGIN IMMEDIATE … COMMIT`; called
  inside one, it participates.
- Result: two transactions per decided paper today, the decision row then the status.
- **Primary:** decision row, then `update_status(pid, "FT_ELIGIBLE")` or `"FT_SCREENED_OUT"`. On
  missing text or malformed output it writes `update_status(pid, "FT_FLAGGED")` with no decision row.
- **Verifier:** decision row. On confirm, `if decision.decision == "FT_ELIGIBLE":
  stats["confirmed"] += 1` (no status write). Otherwise `update_status(pid, "FT_FLAGGED")`. Its
  no-text and malformed paths write `FT_FLAGGED` with no decision row.
- Checkpoints are saved every 10 papers.

**R3 — actor conventions** (I3 false as phrased).
- The `paper_events` CHECKs at 022 (live DDL, quoted in P0 §3):
  - `actor_kind IN ('model','human','engine')`
  - `actor_role IN ('reviewer','extractor','system')`
  - `actor_role <> 'system' OR actor_kind = 'engine'`
  - an event-type/axis pairing, so `screened`/`verified`/`adjudicated` go with
    `eligible`/`abstract_out`/`full_text_out`
  - `reason_code IS NULL` for non-failure tokens.
- The extraction path's paper events are not model-actor events. `_paper_event` writes
  `actor_kind="engine", actor_role="system", actor_name="extractor"`, and the audit event writes
  `actor_kind="engine", actor_role="system", actor_name="auditor", actor_digest=auditor_digest`.
- The only model-actor convention is on field events: `actor_kind="model", actor_role="extractor",
  actor_name=rec.model, actor_digest=rec.model_digest`.
- Live's 190 seed rows are `state_at_migration`/`eligible`/`engine`/`system`/`017_seed_event_store`,
  `pre-manifest`.
- A model screening decision is admissible as `model`/`reviewer`; `extractor` would be the wrong
  role. The choice between `model`/`reviewer`/`<model>` and the engine convention is not settled.

**R4 — the paper-event writer** (I4 partly false). The signature is `write_paper_event(conn, *,
event_type, paper_id, to_state, from_state=None, actor_kind, actor_role, actor_name,
actor_digest=None, occurred_at=None, run_id=_MISSING, run_marker=None, prior_event_id=None,
presented_context_sha256=None, reason=None, reason_code=None, stage_name=None, payload=None,
commit=True)`.
- With `commit=False` it participates in the caller's transaction.
- `_run_link` requires a `run_id` that names an existing manifest.
- It has no arm parameter. The arm refusals live only in `write_field_event`, so "refuses … only
  when an arm is given" doesn't describe it.

**R5 — `open_run`** (I5 partly false). The signature is `open_run(conn, spec, *, kind, stages,
codebook, cloud_arms=(), preflight_models=(), arms=(), digest_fn=None, git=None, host=None,
payload_description=None, selection_bound=None)`.
- It requires:
  - `kind in RUN_KINDS`
  - a clean tree (`DirtyTree`)
  - a loaded `Codebook` (for `.semantic_hash`, `.sha256` and `.path`)
  - a digest per model via `resolve_run(…, digest_fn=…)`, which defaults to `fetch_model_digest` on
    `/api/tags`.
- It does not require a review directory.
- **The preflight point:** it declares only the stages it's given. `resolve_run` turns
  `"preflight"` into one `preflight:<model>` entry per model in `preflight_models`, and
  `record_active_ollama_call` records preflight under `key = f"preflight:{request.get('model')}"`.
  Since `run_calls` has `FOREIGN KEY (run_id, stage) REFERENCES run_stage_configs(run_id, stage)`,
  a screening manifest must declare `preflight` with `preflight_models=[primary, verifier]`.
  Declaring only the two FT stages (C1 as written) would fail every preflight write.
- **The pattern to mirror:** `run_pipeline._open_run_manifest` uses `codebook =
  load_codebook_beside(db.db_path)`, appends `"preflight"` to its stages when it needs preflight,
  calls `rm.open_run(db._conn, spec, kind=kind, stages=stages, codebook=codebook,
  preflight_models=sorted(set(preflight)), …)`, returns `handle.run_id`, runs the stages inside
  `rm.activate(db._conn, run_id)`, and closes through `_finish_review_run`.

**R6 — request hashes and preflight rows** (I6 true, with a caveat).
- `ollama_chat(…, return_request_hash=True)` returns `(response, request_hash)`, "the same hash
  `run_calls.request_hash` stores". Today `ft_screen_paper`/`ft_verify_paper` don't use it, so
  returning the hash is a signature change to those two functions.
- `require_preflight` → `ollama_chat(messages=PREFLIGHT_MESSAGES, max_retries=0, wall_timeout=60.0,
  **cfg.kwargs())` with no `paper_id`, so the rows would have NULL `paper_id` (R240), provided
  `preflight` is declared.

**R7 and R8 — files and fixtures** (I8 true).
- Checkpoints go to `db.db_path.parent / "ft_screening_checkpoint{suffix}.json"`. `--background`
  logs to `Path("data") / review_name / "logs"`. Foreground logging goes to stderr. Nothing is
  written under `docs/`.
- `maybe_background` runs before `load_spec_for`, so a manifest opened after it opens in the tmux
  child.
- The FT test fixtures:
  - `tmp_db` is `ReviewDatabase("test_review", data_root=tmp_path)`, a fresh database migrated
    through 022, so `run_manifests` exists. It has no codebook beside it; bridge tests would copy
    one in, as `test_undeclared_override`'s `review` fixture does.
  - `_add_paper` and `_advance_to_parsed` build `PARSED` papers, `write_parsed` supplies the text,
    `patch("engine.agents.ft_screener.ft_screen_paper", …)` stubs decisions, and
    `require_preflight` is patched.
  - A manifest can be opened offline with `git=rm.GitState(commit="b"*40, dirty=False, tag=None)`
    and a fixed `digest_fn`.
  - For request-level assertions, `test_request_capture.py`'s `FakeClient` replaces
    `ollama_client._client`.

**R9 — digests.** `/api/tags` lists `qwen3:32b
030ee887880fc378860c2dd35101da424377520441ae4bfe7be6deff8ade7840` and `gemma3:27b
a418f5838eaf7fe2cfe0a3046c8384b68ba43a4435542c942f9db00a5f342203`.

### 3. The proposed contract

| item | verdict |
|---|---|
| C1 | **Needs ruling:** the stage set must also include `preflight` with `preflight_models=[primary, verifier]` (see R5). Otherwise implementable: `maybe_background` runs first, `load_codebook_beside` works, and the close can mirror `_finish_review_run`. |
| C2 | **Needs ruling:** the actor. `model`/`reviewer`/`<resolved model>` is admissible, but no existing paper event uses a model actor (see R3). Event types pair naturally as primary exclude → `screened`, verifier confirm → `verified`. |
| C3 | **Needs ruling:** the two decision writers call `self._conn.commit()` internally, so one transaction needs a `commit=False` path in `engine/core/database.py`, which is outside the FT module. `update_status` already participates in an open transaction, and the FT screener swallows no refusal today, so "raise" is the existing behaviour. |
| C4 | Implementable, but the request hash needs `ft_screen_paper`/`ft_verify_paper` to return it: a signature change to two functions this session just narrowed. |
| C5 | Implementable once C1 declares `preflight`: 2 `run_calls` rows, 0 events and a completed manifest on a zero-selection `run_ft_screening`. A `--verify-only` run makes 0 calls. |
| C6 | **Needs ruling:** a confirmed paper passes the seam, but a primary-included, unverified paper fails identity 1 and blocks `export_prisma_csv` (see section 1). |

### 4. Findings against the ledger
1. I1 is false: `FT_ELIGIBLE` is not a screening token. Its consequence is the C6 gap above, which
   contradicts R205's "recorded, not a defect" reading of `--screen-only`.
2. I3 is false as phrased: extraction and audit paper events use `engine`/`system`. The model-actor
   convention exists only on field events.
3. I4 is partly false: `write_paper_event` takes no arm, and the arm refusals are field-event-only.
4. I5 is partly false: a screening manifest must also declare `preflight` and `preflight_models`, or
   the preflight `run_calls` writes violate the foreign key. It doesn't require a review directory.
5. C3 needs changes in `engine/core/database.py` (`add_ft_screening_decision` and
   `add_ft_verification_decision` commit internally).
6. C4 needs `ft_screen_paper`/`ft_verify_paper` to return the request hash.
7. Until the adjudication-import commit lands, adjudicated `FT_FLAGGED → FT_ELIGIBLE` papers fail the
   seam too.

### 5. Estimated size of Phase B
- **Source, three files:**
  - `engine/agents/ft_screener.py`: CLI manifest open and close, a required `run_id` keyword on both
    run functions, event writes, per-paper transactions, and possibly the request-hash return.
  - `engine/core/database.py`: a `commit=False` path on the two decision writers.
  - `docs/inventory/entry_points.*`: regenerated.
- **Tests:** a new bridge test module of about 10–12 tests:
  - manifest open and close, and the preflight rows
  - primary exclude → `full_text_out`, and verifier confirm → `eligible`
  - include and flag write no event
  - atomic rollback on a refused transition
  - `DirtyTree`
  - a resumed run gets a new manifest
  - the seam passes after a confirm
  - plus follow-through at the existing `run_ft_screening`/`run_ft_verification` call sites in
    `test_ft_screening.py` (about 5) and `test_ollama_preflight.py` (1).
- **Estimate:** about 3 source files and 3 test files. That's comparable to B21 and FT-OVR together,
  plus a copy smoke. My judgement is that it fits one more brief in this session, and that the
  adjudication import and both importers belong in 11b under R51.

---

## Part B — the V1/V2 read (verbatim)

Both checks hold, so the rulings stand as written. Nothing was written, and the tree is still clean
at `ce86e30`.

**V1 holds.** No reader in `engine/`, `analysis/` or `scripts/` looks at `actor_role` or
`actor_kind` on a paper event.
- `effective_state` (`engine/core/effective.py`) reads `SELECT event_id, event_type, to_state,
  from_state, actor_name, occurred_at, payload_json, reason_code, stage_name FROM paper_events`. It
  never selects either column, and it branches only on `to_state in ELIGIBILITY_STATES` /
  `PROCESSING_STATES`.
- Every reviewer branch reads `field_events` only:
  - `_cell_events` (`SELECT … FROM field_events`) feeds `reviewer_events = [e for e in evs if
    e.event_type in REVIEWER_EVENT_TYPES and e.actor_role == "reviewer"]`.
  - In `events.py`, the checks `is_reviewer = event_type in REVIEWER_EVENT_TYPES and actor_role ==
    "reviewer"` and `if actor_role == "extractor" and event_type in CLAIM_EVENT_TYPES` sit inside
    `write_field_event`.
- `write_paper_event` checks neither column.

So a `model`/`reviewer` screening event can't be read as a human override. R-C2 stands.

**V2 holds.** The box can carry the count without a second seam change.
- `screening_in_progress` in `generate_prisma_flow` is a sum of status counts:
  `n("ABSTRACT_SCREENED_IN") + n("ABSTRACT_SCREEN_FLAGGED") + n("PDF_ACQUIRED") + n("PARSED") +
  n("FT_FLAGGED")`.
- `eligible_ids = eligible_paper_ids(conn)` is computed a few lines later in the same function, so
  adding |P \ E| there needs no new reader. It only has to use `eligible_ids` after it's computed.
- The one output consumer is the CSV row `("Screening in progress", flow["screening_in_progress"],
  "Papers still in screening")`, printed when > 0. No identity reads the box.
- The two pinning tests stay unchanged:
  - `test_prisma_reconciliation.py::test_in_progress_papers_counted` expects `== 4` (`#
    SCREENED_IN(2) + PARSED(1) + FT_FLAGGED(1)`) and `TestNoDoubleCount` expects `== 0`.
  - In both, `seed_prisma_world` puts every eligible paper at `FT_ELIGIBLE` with an `eligible` event
    ("FT_ELIGIBLE on status, `eligible` on events"). Those papers are in P ∩ E, so neither count
    moves.
- Live is also unchanged: P = 0 (no paper at `FT_ELIGIBLE`), and all 190 eligible papers sit at
  `AI_AUDIT_COMPLETE`, outside S and P. So (b) holds 190 == 190 and in-progress stays 0.
- One point for the seam brief: a paper in P \ E is counted in neither `full_text_assessed` nor
  `n_eligible`. That's consistent with "verification pending", and it doesn't disturb the total check
  (`screening_total + len(remainder_ids)`), because the remainder is still defined by status.

---

## Part C — BRIDGE-R1 rulings (transcribed, not yet in the decision log)

Transcribed verbatim from RULING [evidence-engine STAGED-ENTRY-01 BRIDGE-R1] — "rulings on BRIDGE-A;
C1–C6 settled; session split". Its V1 and V2 verification items are answered in Part B.

> **R-S1** Identity 1 refined (prisma.py, its own commit, before the bridge). Let E = papers with a
> live eligible event; S = papers at a SCREENING_TOKEN; P = papers at FT_ELIGIBLE. Then:
> (a) E ∩ S must be empty ("eligible … but at a screening token" — unchanged).
> (b) every paper not in S and not in P must be in E ("past screening … but not eligible" —
> unchanged for every token except FT_ELIGIBLE).
> (c) a paper in P \ E is screening in progress (verification pending): counted in the
> screening_in_progress box, reported by id count, never a failure.
> (d) a paper in P with a live full_text_out event is a failure ("reversed on the eligibility axis
> without a status write" — new).
> Identity 2 unchanged; n_eligible stays |E|. Live today must read identically (190 == 190,
> in-progress 0). Tests: the four cases on a fixture; live-equivalence pinned on the seed shape.
> R205's "--screen-only: recorded, not a defect" is corrected at transcription: it was a seam
> failure until R-S1; after R-S1 it is an in-progress count.
>
> **R-S2** Consequence named: until the adjudication-import commit, an adjudicated
> FT_FLAGGED→FT_ELIGIBLE paper without an event is counted in progress under (c). Recorded as a note
> on the adjudication-import item, not as a defect of R-S1.
>
> **R-C1** Manifest: one screening manifest per CLI invocation, opened in the tmux child (after
> maybe_background and load_spec_for, before require_preflight), codebook =
> load_codebook_beside(db.db_path), stages = [ft_screen_primary, ft_screen_verifier] plus
> "preflight" with preflight_models = sorted({primary, verifier}) whenever run_ft_screening runs
> (mirror run_pipeline._open_run_manifest); run_id a required keyword on both run functions,
> executed inside rm.activate; closed via the _finish_review_run pattern (completed / failed /
> aborted). A resumed run is a new manifest.
>
> **R-C2** Actor and types: primary exclude → event_type screened, to_state full_text_out; verifier
> confirm → event_type verified, to_state eligible; actor_kind model, actor_role reviewer,
> actor_name the stage's resolved model name, actor_digest its digest; stage_name the stage; reason
> NULL. Include, flag, no-text and malformed paths write no event. Conditional on V1.
>
> **R-C3** Transactions: add_ft_screening_decision and add_ft_verification_decision gain
> commit=True (default) and are called with commit=False by the bridge; per decided paper one
> transaction: decision row → status write (update_status participates) →
> write_paper_event(commit=False) → commit. On confirm: decision row → event → commit. A refused
> transition raises as today and the transaction rolls back with nothing written. This database.py
> change is part of the bridge commit (inseparable).
>
> **R-C4** Provenance: ft_screen_paper / ft_verify_paper gain return_request_hash=True (the
> extractor's pattern); the deciding call's hash goes in presented_context_sha256; payload is None.
>
> **R-C5** Copy smoke expectation (zero PARSED on the copy): one screening manifest;
> run_stage_configs rows for ft_screen_primary, ft_screen_verifier, preflight:qwen3:32b,
> preflight:gemma3:27b; exactly 2 run_calls (preflight, paper_id NULL, outcome completed); 0 paper
> events; manifest completed, no end_reason; copy fingerprint otherwise unchanged; seam 190 == 190,
> in-progress 0; --verify-only on the copy: a manifest with 0 calls. Copy moved out of data/ before
> the next gate (R241(iv)); B21 makes its presence harmless meanwhile.
>
> **R-C6** Settled by R-S1.
>
> **R-B10** B10's writer tests are their own commit after the bridge (plan detail reversed: they test
> the extraction writer, not the FT path).
>
> **R-SPLIT** Session 11 closes after two more commits: (1) the BRIDGE-A read-out committed as
> docs/session-reports/staged-entry-01/STAGED-ENTRY-01_BRIDGE-A_readout_<ts>.md (the report above,
> verbatim, plus these rulings' ids), docs-only; (2) the R-S1 seam commit. Then the closeout (rows,
> ledger, retention: the two backups and data/evidence_engine.db, the CLAUDE.md AI_AUDIT_COMPLETE
> sentence, the throwaway copies), wrap. Session 11b opens fresh for the bridge Phase B, B10, the
> adjudication import and the importers.

The BRIDGE-R1 assumptions V1 ("No reader in engine/ or analysis/ branches on actor_role ==
'reviewer' for PAPER events …") and V2 ("The screening_in_progress box in the PRISMA reader can
carry the FT_ELIGIBLE-without-event count without a second seam change") were verified in Part B.
