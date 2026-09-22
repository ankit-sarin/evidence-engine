# MANIFEST-01 Phase 2a — resolver, capture instrument, run manifest, arm pinning, cloud opt-in

**Session 7 (S3a, S3b, S3g), Phase 2a** under the R51 pattern: code, tests and migration 020 in
the tree with fresh-database receipts. **`review.db` was not written**: every open was `mode=ro`
(the G3 copy was taken by `auto_backup`, which reads through `mode=ro`). Phase 2b, the rehearsal
and live write, is a separate brief.

**Commits:** `1cd80a498b3894378847d1a7c54702b501f10285` (code, tests, spec, migration, inventory)
and the docs commit that carries this report. **Base:** `b1429ce1c97ce834799b3313780af5d76135874a`.

Tags: **MEASURED** = produced here by running code or a query. **READ** = quoted from a file.
**INFERRED** = a reading or a cause.

---

## 1. Startup verify and ledger

| check | expected | measured |
|---|---|---|
| HEAD | `b1429ce1…`, clean, level | same; porcelain empty; level with origin |
| `db_fingerprint --compare` (readers-01 record) | exit 0 | **exit 0** at open, after G3, and at close |
| gate at open | 2,564 / 17 | carried from the Phase 1 close (same HEAD, docs-only commit between) |

| item | disposition | evidence (MEASURED) |
|---|---|---|
| **I14** installed versions are the ones the engine ran under | **VERIFIED** | `pip show`: ollama 0.6.1, openai 2.24.0, anthropic 0.84.0. dist-info dates 2026-02-23 (ollama) and 2026-03-04 (openai, anthropic), all before Run 6's extractions (2026-03-15 → 03-18) and cloud runs (2026-03-16 → 03-18); no reinstall since |
| **I15** the 019 rebuild template preserves rowids, values and triggers | **VERIFIED** on a fresh database and a throwaway copy of live (§3) — never on live | 190 / 190 `paper_events` rows byte-identical, same column order, AUTOINCREMENT high-water mark 190 → 190, both append-only triggers restored |
| **I16** P9's names are not in the tree | **VERIFIED** | no `run_manifests`, `run_stage_configs`, `run_calls`, `pinned_run_id`, `EffectiveConfig` or `run_config` module; the only near-hit is `analysis/paper1/judge_storage.py`'s local variable `run_config` (a parameter, not an object) |

---

## 2. G1 — per-site capture diff (the session-7 gate instrument)

`tests/test_request_capture.py` replaces `engine.utils.ollama_client._client` with a fake that
records every `chat(**kwargs)`, answers `_client.show` with a 131,072-token context, and returns a
`prompt_eval_count` inside the guard's accepted band. **Both input-fit guards therefore run as in
production.** Per site, the test asserts the key set and every value, **compared with its type**,
against (a) Phase 1 P1's measured request plus `keep_alive=-1`, and (b) the resolver's
`EffectiveConfig`. Any real HTTP request from the module raises (R45). **Mutation check:** sending
`{"temperature": 0.0}` at the screener fails the test, so the gate could fail.

| # | site (driven through) | stage | model | options sent | think | format | keep_alive | Δ vs Phase 1 |
|---|---|---|---|---|---|---|---|---|
| 1 | `screener.screen_paper` (primary) | `abstract_screen_primary` | `qwen3:8b` | `{temperature: 0}` (int) | `False` | ScreeningDecision | **-1** | keep_alive only |
| 1 | `screener.screen_paper` (verifier) | `abstract_screen_verifier` | `gemma3:27b` | `{temperature: 0}` (int) | `False` | ScreeningDecision | **-1** | keep_alive only |
| 2 | `ft_screener.ft_screen_paper` | `ft_screen_primary` | `qwen3:32b` | `{temperature: 0.0}` (float) | `False` | FT schema + reason-code vocabulary | **-1** | keep_alive only |
| 3 | `ft_screener.ft_verify_paper` | `ft_screen_verifier` | `gemma3:27b` | `{temperature: 0.0}` (float) | `False` | FTVerificationDecision | **-1** | keep_alive only |
| 4 | `auditor.audit_span` → `semantic_verify` | `audit` | `gemma3:27b` | `{temperature: 0}` | `False` | AuditVerdict | **-1** | keep_alive only |
| 5 | `extractor.extract_paper` (Pass 1) | `extract_pass1` | `deepseek-r1:32b` | `{temperature: 0}` | `True` | — | **-1** | keep_alive only |
| 6 | `extractor.extract_paper` (Pass 2) | `extract_pass2` | `deepseek-r1:32b` | `{temperature: 0}` | `False` | ExtractionOutput | **-1** | keep_alive only |
| 7 | `extractor.extract_paper` (snippet retry) | `extract_retry_snippet` | `deepseek-r1:32b` | `{temperature: 0}` | `False` | — | **-1** | keep_alive only |
| 8 | `elicitation.pipeline.run_pass1` | `elicitation_pass1` | `deepseek-r1:32b` | `{temperature: 0}` | `True` | — | **-1** | keep_alive only |
| 9 | `pdf_parser.parse_with_vision` | `vision_parse` | `qwen2.5vl:7b` | `{temperature: 0, num_predict: 2048, num_ctx: 8192}` | — | — | **-1** | keep_alive only |
| 10 | `pdf_quality_check._classify_page` | `pdf_quality` | `qwen2.5vl:7b` | `{temperature: 0}` | — | — | **-1** | keep_alive only |
| 11 | `ollama_preflight.check_model` | `preflight` | caller's model | `{temperature: 0, num_predict: 4}` | — | — | **-1** | keep_alive only |
| C-a | `OpenAIExtractor.extract_paper` | `cloud:openai_o4_mini_2025_04_16_high` | `o4-mini-2025-04-16` | `reasoning_effort: high`, `response_format: {type: json_object}` | — | — | — | **none** |
| C-b | `AnthropicExtractor.extract_paper` | `cloud:anthropic_claude_sonnet_4_6` | `claude-sonnet-4-6` | `max_tokens: 16000`, `thinking: {type: enabled, budget_tokens: 10000}` | — | — | — | **none** |

The cloud rows also assert the system message is the Phase 1 literal, the user turn carries the
paper text, and `last_request_hash == sha256(canonical(complete kwargs))` (C16).

**Recorded, not sent (R66, R69):** at every stage except `vision_parse`, `seed` and `num_ctx` are
recorded as `"unset"` with source `modelfile_or_server`. `vision_parse` has always sent
`num_ctx = 8192`, and records it.

---

## 3. Migration 020

**File:** `engine/migrations/020_run_manifest.py`, sha256
**`b0658f0f53a46786a6bf47fe2b2223b990659815cb264fc9c0fe8e01f1ac559f`**. Declared `schema` in
`runner.KINDS`. Self-contained (R35): its source contains no `from engine` or `import engine`
(pinned by test), and its re-declared lists are asserted equal to `effective_config`,
`run_manifest`, `events`, 016 and 019.

### G2 — fresh database (MEASURED, a fresh `ReviewDatabase` in the scratchpad)

| | value |
|---|---|
| receipt | `020_run_manifest`, `executed`, sha256 as above |
| second pass | runner `executed: []`, 020 in `already`; `run_migration` → `already_applied` |
| tables | **34** |
| `schema_structure_hash` | `effd6d519e0618e498d2c44df756b276968c207c27b50b27b826a4c5ca2d08fd` |
| textual `schema_hash_sha256` | `0ba77776ffe94f42db8e945897b577cdd385ae594b33a5e76fed6e02981d3305` |
| append-only triggers | present on both event tables; record-only triggers on the three new tables; `arms_configuration_frozen` present, `arms_configuration_frozen_once_claimed` gone |

### The CHECK texts 2b must read back (R77, R78)

On **both** `field_events` and `paper_events`, verbatim:

```sql
CHECK (
            (run_id IS NOT NULL AND run_marker IS NULL)
            OR
            (run_id IS NULL AND run_marker IS 'pre-manifest')
        )
```

On `run_stage_configs`:

```sql
CHECK (provider IS NOT 'ollama' OR (model_digest IS NOT NULL AND length(model_digest) = 64))
```

On `run_manifests`: `run_kind TEXT NOT NULL CHECK (run_kind IN ('extraction', 'screening',
'judge', 'review_session'))`, `git_commit TEXT NOT NULL CHECK (length(git_commit) = 40)`,
`git_dirty INTEGER NOT NULL CHECK (git_dirty = 0)`, `end_status TEXT CHECK (end_status IS NULL
OR end_status IN ('completed', 'failed', 'interrupted'))`, `CHECK ((ended_at IS NULL) =
(end_status IS NULL))`, `CHECK (cloud_arms_json = '[]' OR payload_description IS NOT NULL)`
(with `cloud_arms_json` NOT NULL).

**R77 — the four rejected states, on both tables (MEASURED, 8 parametrized tests, all refused
with `CHECK constraint failed`):** NULL/NULL; `run_id` set with `'pre-manifest'`; `run_id` set
with another marker; `run_id` NULL with another marker. Both permitted states are admitted (2
tests).

**R78 — a NULL in every column each CHECK names (MEASURED, all refused):**
`run_manifests.run_kind`, `.git_commit`, `.git_dirty`, `.cloud_arms_json`, `.ended_at` (with
`end_status` set), `.end_status` (with `ended_at` set), `.payload_description` (with a cloud arm);
`run_stage_configs.stage_kind`, `.provider`, `.model_digest` (an Ollama stage). Plus: an
`end_status` outside the vocabulary and `git_dirty = 1` are refused.

**Two NULL holes found and closed.** The brief's R68 CHECK admitted NULL/NULL (reproducer: an
INSERT succeeded; ruled R77). The audit under R78 found a second one:
`CHECK (provider <> 'ollama' OR length(model_digest) = 64)` admitted an Ollama stage with no
digest, because `length(NULL) = 64` is NULL. A mutation check restoring that text fails
`test_r78_every_check_refuses_a_null[run_stage_configs.model_digest]`.

**Transaction.** A forced failure after both rebuilds (`_fail_after_copy`) leaves
`sqlite_master` byte-identical to before, and a clean run afterwards executes (MEASURED). One
ordering finding, fixed before commit: SQLite validates every trigger at `ALTER TABLE … RENAME`,
and the arms freeze trigger names `field_events`. So the old trigger comes off **before** the
rebuild and the widened one goes on **after** it. The first draft failed on a fresh database with
"error in trigger arms_configuration_frozen: no such table: main.field_events".

### G3 — a throwaway `auto_backup` copy of live (MEASURED; never live)

`auto_backup(data/surgical_autonomy/review.db)` → verified overall
`e564f250afe40af7eb9a6bc07596a3c795972f7ef3b653c37599e8bc18285b63` (equal to live), moved at once
to the scratchpad, and migrated through `runner.run`:

| check | result |
|---|---|
| runner | `executed: ['020_run_manifest']`, 0.029 s; receipt sha `b0658f0f…559f`, `executed` |
| `paper_events` | 190 → 190 rows, **identical tuples** (rowid, every value), identical column order, `run_marker = 'pre-manifest'` and `run_id` NULL on all 190, `sqlite_sequence` 190 → 190 |
| `field_events` | 0 rows |
| `arms` | the three seeded rows' six original columns unchanged; `pinned_run_id`, `pinned_sha256` NULL |
| content hashes | **28 of 31 tables unchanged**, `field_events` and `paper_events` included. Changed: `arms` (two appended NULL columns), `schema_migrations` (18 → 19), `sqlite_sequence` (one new row, `('field_events', 0)`, created by the rebuild's `INSERT … SELECT`; no id issued). New: `run_manifests`, `run_stage_configs`, `run_calls`, each 0 rows |
| structure | `schema_structure_hash` `effd6d51…08fd` = the fresh database's; `structure_differences(fresh, copy) = []` |
| textual | `a4e49d4a96d171cfe85b919bbc4ee08064ce1733fde7b52a71491ef46876c864` |
| CHECK read-back | R77's text on both tables and the digest CHECK, verbatim as above |
| `foreign_key_check` | empty on `paper_events` and `arms` |
| idempotent | second `runner.run` executes nothing; `run_migration` → `already_applied` |
| after | fingerprinted immediately before deletion: 34 tables, overall `3e01ded4a9644d0fcb2cdbfb87740bfd657d992ec504f65ae6f7812cae12f90f`; deleted; no `*throwaway*` file remains in `data/` |

---

## 4. G4 — refusal and pinning fixtures (all MEASURED passing)

| requirement | test |
|---|---|
| manifest written before the first call | `test_run_manifest::test_the_manifest_exists_before_the_first_model_call_and_every_call_is_recorded` (the fake client queries the database at the call: 1 manifest, 3 stage rows; the call lands in `run_calls` with `request_hash` = hash of the kwargs sent) |
| `run_id` on every event row a run writes | `test_run_manifest::test_run_id_is_on_every_event_row_a_run_writes`; the R77 CHECK makes it structural |
| dirty tree refused | `test_a_dirty_tree_is_refused` (nothing written); `test_r78_a_dirty_tree_row_is_refused_by_the_database_itself` |
| pre-manifest arm refused | `test_a_pre_manifest_arm_is_refused` (run open); `test_r59_a_claim_on_a_pre_manifest_arm_is_refused` (writer); `test_r59_a_pre_manifest_arm_can_never_be_pinned` (trigger) |
| pinned-arm mismatch refused | `test_a_pinned_arm_resolving_differently_is_refused_and_the_difference_named` (names `options_hash`); `test_a_changed_model_digest_is_a_pin_mismatch` |
| declared arm pinned at first manifest | `test_a_declared_arm_is_pinned_at_its_first_manifest`; `test_an_unpinned_registered_arm_is_pinned_too`; `test_the_same_configuration_opens_again_and_keeps_the_first_pin`; `test_r59_a_pinned_arm_is_frozen_before_it_holds_any_claim` |
| event-writer refusals (R68/R59/R21/R10) | `test_r68_an_event_without_a_run_id_is_refused`, `…_run_marker_from_a_caller_that_is_not_a_migration_is_refused`, `…_a_run_id_that_names_no_manifest_is_refused`, `test_r21_a_claim_on_a_retired_arm_is_refused`, `test_r10_a_claim_on_a_model_arm_the_run_did_not_pin_is_refused`; `test_r68_migration_017_may_still_write_its_pre_manifest_seed` (017's text is unchanged and still seeds) |
| `--arm` not in `enabled_arms` refuses | `test_the_cli_refuses_an_arm_outside_enabled_arms_before_any_request` (no extractor constructed; exit 2); `test_a_cloud_arm_the_spec_does_not_enable_is_refused` |
| `enabled_arms` empty + no `--arm` proceeds | `test_no_arm_and_nothing_enabled_proceeds_and_sends_nothing` |
| reviewer event on a pre-manifest claim allowed | `test_r68_a_reviewer_event_on_a_pre_manifest_claim_is_allowed_under_a_review_session` (row 7 → exit via `human_withdrew` under a `review_session` run) |
| v2.1 rows 6 and 7, D1-1…D1-4 reachable | `test_effective_reader.py`, 34 passed: `test_row7_*` (claims seeded below the writer, as a migration would), row 6, `test_d1_1_*` … `test_d1_4_*` |
| review session = run with zero stage rows | `test_a_review_session_is_a_run_with_zero_stage_rows` |
| digest failure refuses | `test_a_digest_failure_refuses_the_run` (nothing written) |

---

## 5. Retirements and ledger rows

All retired in `1cd80a498b3894378847d1a7c54702b501f10285`:

| retired | replacement | ledger / row |
|---|---|---|
| `analysis/eval/run_cloud_strict.py` and **three** parametrized cases in `tests/test_review_paths.py` (commit 1's message says two; collection measures three) | none | retention ledger (R72) |
| `ollama_client.get_model_digest` | `fetch_model_digest` (`/api/tags`), which raises | C15 (R57) |
| class-constant `ARM` on both cloud extractors; `_DEFAULT_MODEL`, `_DEFAULT_COST_*`, `COST_*_PER_M`; `spec.cloud_models` | spec `arms` + `cloud.prices`; `arm_name` per instance | C20 (R64), C19 |
| `extractor.MODEL`, `auditor.DEFAULT_AUDITOR_MODEL`, `screener.DEFAULT_PRIMARY_MODEL` / `DEFAULT_VERIFICATION_MODEL`, `pdf_parser._VISION_MODEL` / `_VISION_NUM_PREDICT` / `_VISION_NUM_CTX`, every literal temperature and `think` | the resolver and the spec model's declared defaults | C1, C8, C19 |
| `run_pipeline`'s `review_runs` writer | `run_manifest.open_run` / `close_run` | retention ledger (R73): retire at session 10 |

---

## 6. Gate, rewrites, commits, close

**Gate: 581 + 708 + 433 + 575 + 417 = 2,714 passed, deselects 0/0/10/6/1 = 17** (from 2,564 / 17).
Ollama `NRestarts=0`. The +150 by file, from collection at both commits: `test_effective_config`
+55, `test_run_manifest_migration` +41, `test_run_manifest` +21, `test_request_capture` +12,
`test_event_writer_refusals` +11, `test_api_parity` +9 (new engine imports in scripts, auto-
parametrized), `test_review_spec` +2, `test_cloud_extraction` +1, `test_ollama_client` +1,
`test_review_paths` −3.

**B5 rewrites (tests that pinned retired behaviour, rewritten to the working behaviour):**
`test_ollama_client::TestGetModelDigest` (2) → `TestTheDigestRoute` (3: `/api/tags` not
`/api/show`; failure raises, never None; the function is gone).
`test_cloud_extraction::TestSpecDrivenConfig` (3) → 4 (arm, model and price from spec arms; no
default price; no default arm). `test_ollama_preflight` (2): the preflight model is the
resolver's and the spec travels with it. `test_review_spec`: the two `cloud_models` nested-key
cases → `cloud`, `ollama`, `audit`, `preflight`. `test_citation_guard`, `test_elicitation_pipeline`,
`test_completeness_guard`: `MODEL` / `ARM` references. `test_extractor` (8): patches move to
`fetch_model_digest`.

**R68 caller rewrites.** Every test that writes events now writes under a run.
`tests/_event_store_fixture.py` gains `fixture_run`, `run_for`, `upgrade_event_store`,
`seed_claim` and `seed_pre_manifest_paper_event`. Seed rows, like 017's, are built below the
writer, because only a migration may write `'pre-manifest'`. Touched: `test_effective_reader`,
`test_event_writer_refusals`, `test_readers_on_the_reader`, `test_two_axis_state_and_grid`,
`test_adjudication_pairs`, `test_judge_loader`, `test_non_value_tokens_downstream`,
`test_cloud_extraction`.

**Retired tests (R47):** the three `run_cloud_strict.py` cases above.

**Frozen harnesses touched** (R31-exempt imports, not behaviour): `run_local_abc.py`,
`run_qualgap01.py`, `run_capture01.py` and `elicit01/runner.py` each declare the literal
`MODEL = "deepseek-r1:32b"` they ran with, because the engine constant they imported is gone.
`elicit_design01/smoke.py`, `scripts/test_extraction_validation.py`,
`scripts/eval_auditor_models.py` and `scripts/test_e2e_search_screen.py` take their model from
the resolver.

**Fingerprint at close:** `--compare` against
`docs/session-reports/readers-01/review_db_fingerprint_20260922T165644Z.json` exits **0**.

---

## 7. Findings against the brief, and new inventory candidates

**Against the brief (reported, resolved as stated):**

1. **R68's CHECK text admitted NULL/NULL** → ruled R77/R78/R79 in session; implemented.
2. **The run-stage digest CHECK had the same NULL hole** (§3) → closed under R78.
3. **Contract 1 names one stage `abstract_screen`**, but one call site serves two models and two
   prompts, and a stage carries one model. Implemented as `abstract_screen_primary` /
   `abstract_screen_verifier`. Not ruled; flagged here.
4. **Spec model blocks beyond `arms` / `extraction_models` / `cloud`.** Three small declared-default
   blocks were added because their defaults were module literals with no spec home (C19):
   `ollama` (`keep_alive`), `audit` (`model`, `temperature`, `think`, `seed`, `num_ctx`) and
   `preflight` (`temperature`, `num_predict`). The pre-existing `auditor_model` stays as the
   override it always was, because `methods_section` reads it. Not ruled; flagged.
5. **The brief's Phase 2b expectation "31 tables, rebuilds only" is wrong.** 020 creates three
   tables, so live goes 31 → **34** (measured on the G3 copy).
6. **R66 and the wire type of `0`.** Pydantic turns YAML `0` into `0.0` in a `float` field. The
   resolver keeps the literal the site sent (`Temperature = StrictInt | float`; the FT fields stay
   `float`, because they always sent `0.0`). Without this, seven sites would have changed `0` to
   `0.0` on the wire. The capture compares types.

**New inventory candidates:**

| class | problem | anchor |
|---|---|---|
| E | `screen_paper(role="verifier")` with no `model=` sends the **primary** model (behaviour preserved). `run_screening` always passes the verifier model, so only a direct caller is exposed | `cfg = stage_config(stage, spec, model=model or spec.screening_models.primary)` |
| C | Migration 017 imports `engine.core.events`, whose writer changed. 017 still seeds identically, because the writer recognises a migration **by its caller's file** (`_called_from_migration`). That file location is now a coupling 017's checksum cannot see (C13 family) | `Path(frame.f_code.co_filename).resolve().parent == _MIGRATIONS_DIR` |
| C | The auditor's caller override `ollama_options` remains an option channel no manifest records. Its only user is `scripts/eval_auditor_models.py` | `cfg = cfg.with_options(ollama_options)` |
| I | `run_pipeline` and `run_cloud_extraction` now need 020 on the target database. Against live before 2b they fail at run open (`no such table: run_manifests`). That is a safety property, noted for 2b | `rm.open_run(db._conn, …)` |
| H | `CLAUDE.md`'s "Cloud Extraction Architecture" still describes code-level cost rates and `cloud_models`, and "Key Patterns" does not mention the resolver or manifest. `CLAUDE.md` is the PI's file and was not edited | `CLAUDE.md` "Cost rates: OpenAI $1.10/$4.40 …" |

---

## 8. Phase 2b prerequisites

* **Re-read 020's checksum** on a fresh database built at 2b's HEAD:
  `b0658f0f53a46786a6bf47fe2b2223b990659815cb264fc9c0fe8e01f1ac559f`. The receipt must carry it.
* **Expected after the live write:** **34 tables** (31 + `run_manifests`, `run_stage_configs`,
  `run_calls`); 28 of the 31 existing tables content-identical, `paper_events` and `field_events`
  included. The expected differences are `arms` (two NULL columns appended), `schema_migrations`
  (+1 receipt) and `sqlite_sequence` (+`('field_events', 0)`). `schema_structure_hash`
  `effd6d519e0618e498d2c44df756b276968c207c27b50b27b826a4c5ca2d08fd` = fresh, with
  `structure_differences = 0`. The textual hash of live-after must equal the rehearsal-after
  (G3 copy: `a4e49d4a…c864`, which the rehearsal re-measures).
* **CHECK read-backs:** R77's text on both event tables, and the digest CHECK on
  `run_stage_configs`, verbatim as in §3.
* **Row preservation:** 190 `paper_events`, tuples identical, all `run_id` NULL /
  `'pre-manifest'`; `sqlite_sequence` for `paper_events` stays at 190.
* **Restore point:** `review.db.bak-readers-01-phase3-pre-write-20260922-165453` (retained to
  session 10) plus a fresh `auto_backup` immediately before the write.
* **Embargo:** a fresh CC session opened **after 10:00 UTC**, as R51 set for 6b. Before 2b,
  nothing on live may open a run: no `run_pipeline`, no `run_cloud_extraction`. R19 and R71 keep
  extraction frozen regardless.
