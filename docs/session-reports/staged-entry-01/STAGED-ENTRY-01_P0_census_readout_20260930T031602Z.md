# STAGED-ENTRY-01 P0 — session 11 open: startup verify and read census

Read-only census at HEAD `0e0bd8f524869e209b3f52a278da64626344e38e`, 2026-09-30, CC session
`4f668f80`. Live `review.db` and every backup were opened `mode=ro` (by `db_fingerprint` or a
`mode=ro` URI), and no `ReviewDatabase` was constructed on any of them. Every claim is cited by
file path and a quoted content anchor, never by line number. A count a command printed is
labelled **measured** and names the command. A count reached by reading is labelled **(by
reading)**. Nothing here is a design, and no option is recommended.

---

## Part 0 — startup verify (summary)

| item | expected | measured | verdict |
|---|---|---|---|
| I1 | HEAD `0e0bd8f5…`, clean, level | `git rev-parse HEAD` → `0e0bd8f524869e209b3f52a278da64626344e38e`; `git status -sb` → `## main...origin/main`; `git rev-list --left-right --count HEAD...origin/main` → `0 0` | match |
| I2 | plan carries R233–R241 and a 10b closure paragraph | `## Decision log — session 10b (2026-09-29)` with rows R233–R241; closure paragraph "Session 10b (MIGRATION-02, R233–R241) closed 2026-09-29" | match |
| I3 | claude-config `930dddc`, one file modified (`PROJECT_LEDGER.md`, +4) | `930dddc`; `git diff --stat` → `PROJECT_LEDGER.md \| 4 ++++`; `primer.md` unmodified | match |
| I4 | the verbatim primer diff exists on this box | **not** in the closure paragraph, **not** in the project primer, **found** in the archived session `~/claude-session-archive/bb3f326d-e9c9-4050-986a-081a0d2defc4.jsonl` ("## 7. Proposed global-primer diff (for your approval; not written or committed)") | match (third place) |
| I5 | gate 2,911 / 1 xfail / 17 deselected, 2,912 collected; ids == `collected_ids_c1c6210.txt` | `pytest tests/ -q -m "not network and not ollama and not integration"` (03:09:03–03:19:53 UTC) → `2911 passed, 17 deselected, 1 xfailed, 2 warnings in 648.13s`, wall 650.38 s (`/usr/bin/time`), rc 0; deselect sum 2,911 + 1 + 17 = 2,929; `--collect-only` under the same marker → 2,912 ids, `diff` against `collected_ids_c1c6210.txt` → 0 added / 0 removed; the xfail is `test_fast_matches_legacy[   ]` ("R17 / 9b-2d R8: a snippet that normalises to nothing is not located"); `tests/test_eligibility.py` (T7, the fourteen frozen renderings) green inside the gate | match |
| I6 | `inventory --check` in sync; one `review.db` under `data/`; throwaway at `~/scratch/retained/…` | `inventory in sync` (exit 0); `find data -maxdepth 2 -name review.db` → `data/surgical_autonomy/review.db` only; `~/scratch/retained/surgical_autonomy_smoke10b/` holds `review.db`, `spec.yaml`, `extraction_codebook.yaml`, `logs`, `telemetry`, `exports`, … | match |
| I7 | `--compare` IDENTICAL, 36 tables, the three hashes; 21 receipts, last 022, no pending | `IDENTICAL — schema, every table, and the overall hash all match.` exit 0; `-wal : 0 B`; 36 tables; overall `0d3eedea…cb25`, structure `b0dffa3f…57aa`, textual `f8d63887…3cee`; 21 receipts, last `022_run_kinds_and_audit_tables` `da1f31c8…a67b` `executed`; 21 migration files == 21 receipts, 0 checksum mismatches, 0 pending | match |
| I8 | 3 arms, 190 / 194 (all hashed) / 3 / 0; five tables empty | arms 3 (`local`, `anthropic_sonnet_4_6`, `openai_o4_mini_high`, all `not recorded (pre-manifest)`); paper_events 190; parsed_text_refs 194, 194 with `parsed_text_sha256`; review_identities 3; field_events 0; run_manifests / run_stage_configs / run_calls / claim_inputs / audit_verdicts 0 | match |
| I9 | five restore points with the stated hashes | all five equal (table below, §5) | match |
| I10 | two unruled backups with sidecars | both present, each with `-shm` (32,768 B) and `-wal` (0 B) | match |
| I11 | codebook `89dbfa91…2b` and hash baseline `67754a47…e7` | `sha256sum` → both equal | match |
| I12 | `elicitation` false, arm `local_deepseek_r1_32b`, `enabled_arms` empty, no audit block; Ollama 0.21.0 with both models | `False`, `local_deepseek_r1_32b`, `[]`, no top-level `audit` key; `/api/version` → `0.21.0`; `/api/tags` lists `gemma3:27b` and `deepseek-r1:32b` (20 models) | match |

Item 0 (claude-config) is reported in the session's final report, not here.

---

## §1 B21 — the review-id census

**The predicate.** `engine/tools/inventory.py::review_ids` is a **union of two sources**, not
the `data/` glob alone:

```
ids = set()
for spec in sorted((repo_root / SPEC_DIR).glob("*.yaml")):
    ids.add(spec.stem)
...
        for child in sorted(data_root.iterdir()):
            if not child.is_dir():
                continue
            if (child / "review.db").exists():
                ids.add(child.name)
            else:
                ...
                excluded[child.name] = "no review.db"
return sorted(ids), excluded
```

It returns **two** values: `ids`, and `excluded`, which maps every other `data/` subdirectory to
the reason `"no review.db"`.

**Every consumer of the result** (found with a repo-wide grep for `inventory`, `review_ids`,
`data_dirs_excluded` and `entry_points.(json|md)` over `.py`/`.sh`/`.toml`/`.yaml`, excluding
`.venv` and `data/`):

1. `build_inventory` — `ids, excluded = review_ids(repo_root)` then `id_set = set(ids)`. The id
   set is passed to **every file's analysis**, `files[rel] = analyze_file(path, rel, id_set)`,
   which records each string equal to a review id under `"literal_review_ids"`. The summary
   counts `len([h for h in f["literal_review_ids"] if h["role"] == "code"])`. Both sets are
   returned as `"review_ids": ids, "data_dirs_excluded": excluded`.
2. `drift()` (the `--check` comparison) compares **both**:
   `if committed.get("review_ids") != fresh["review_ids"]:` → `"review ids changed: …"`, and
   `if committed.get("data_dirs_excluded") != fresh["data_dirs_excluded"]:` →
   `"data/ subdirectories changed: …"`. Then it compares every file's analysis and the summary.
3. `write_inventory` / `render_markdown` — `docs/inventory/entry_points.{json,md}`; the Markdown
   prints `"Review ids on disk: …"` and `"`data/` subdirectories NOT counted as reviews (no
   `review.db`):"`.
4. `tests/test_inventory.py::test_committed_inventory_is_in_sync_with_the_tree` —
   `message = inventory.drift(REPO_ROOT)`; `assert message is None, message`. It runs in the
   standard gate **and** in the 09:00 nightly: the crontab line
   `0 9 * * * /bin/bash …/scripts/nightly_tests.sh` runs `python -m pytest tests/ -v --tb=short`
   (all tiers).
5. `tests/test_inventory.py::test_review_ids_exclude_data_dirs_without_a_review_db` — a unit
   test on a `tmp_path` tree: `assert ids == ["alpha"]` and
   `assert "backups" in excluded and "no review.db" in excluded["backups"]`. **It pins the
   current predicate's shape**, so a change to the predicate is a change to this test.
6. The other `test_inventory.py` drift tests (`test_drift_names_the_first_differing_file`,
   `test_drift_reports_a_file_missing_from_the_committed_inventory`,
   `test_full_scan_is_fast_enough_for_the_standard_gate`) call `drift` / `build_inventory` and
   so traverse `review_ids`. They do not assert on its output.

No doc generator other than `write_inventory` exists. `tests/test_codebook_loader.py` names
`"engine/tools/inventory.py"` only as a file allowed to mention YAML loading, not as a consumer.

**What it counts today (measured).** Committed `docs/inventory/entry_points.json` `data`:
`review_ids` `['surgical_autonomy']`; `data_dirs_excluded`
`{'backups': 'no review.db', 'my_review': 'no review.db', 'myreview': 'no review.db',
'review': 'no review.db', 'test_review': 'no review.db'}`. `ls data/` shows the same six
directories plus two files (`.gitkeep`, a 0-byte `evidence_engine.db`), which `if not
child.is_dir(): continue` skips. `review_specs/` contains exactly one file,
`surgical_autonomy.yaml`.

**Is a spec-presence predicate derivable without a behaviour change elsewhere?**
- Spec stems are **already** ids. The change B21 describes is dropping the `data/` branch's
  `ids.add(child.name)`, or gating it on a spec.
- **Finding 1: a spec-only predicate does not, alone, stop the drift.** A smoke copy
  `data/<id>/` that holds a `review.db` but has no `review_specs/<id>.yaml` would leave
  `ids`, but under today's `else:` it would enter `excluded` (with the wrong reason, `"no
  review.db"`). `drift()` compares `data_dirs_excluded` too, so `--check`, the gate and the
  nightly suite go red either way ("data/ subdirectories changed"). The same is true today of
  **any** new `data/<dir>`, with or without a `review.db`. R241(iv) moves the whole directory
  out, which is why it works. B21's fix therefore has to decide what `excluded` records for a
  declared throwaway, or exclude a declared pattern from **both** sets, or stop comparing
  `excluded` in `drift`. All three are behaviour changes to `drift` or to the committed file's
  shape.
- **No other code enumerates reviews by directory** (by reading: the grep for `iterdir()`,
  `glob("*/review.db")` and a `review_specs` glob across `engine/`, `scripts/`, `analysis/` and
  `tests/` hits only `inventory.py` among review enumerators; `engine/core/review_paths.py`
  exposes `spec_path_for`, `data_root_for` and `load_spec_for`, which resolve a named id and
  never list). So the predicate change is local to `inventory.py`, its committed output and
  `test_inventory.py`.
- The id set also feeds `analyze_file`. A narrower id set changes `literal_review_ids` only for
  strings equal to a dropped id. By reading, no scanned file contains the string
  `surgical_autonomy_smoke10b`, so with the throwaway gone the committed per-file analyses would
  be unchanged.

**Consumer set vs the brief's expectation.** Expected: one test and the `--check` gate. Found:
two tests that constrain the result (the drift test, and the unit test pinning the predicate's
shape), plus `--check`, `--write` and its two committed files, and the per-file
`literal_review_ids` analysis. B21's commit touches `inventory.py`, `test_inventory.py` and a
regenerated `docs/inventory/entry_points.{json,md}` (R241(ii)).

---

## §2 The FT screening path's manifest-less shape

**The 9e-SE-A read-out** is
`docs/session-reports/write-path-01/WRITE-PATH-01_9e_staged_entry_readout_20260928.md`
(commit `46cd7df`, "docs(write-path-01): 9e-SE-A — staged-entry design read-out (R182(6))",
census at `b0ee3bc`). Its §2 rows for FT, quoted:

> | FT primary | `engine/agents/ft_screener.py::run_ft_screening` (CLI `python -m engine.agents.ft_screener`) | `ft_screening_decisions` (`db.add_ft_screening_decision(`), `workflow_state` | FT_ELIGIBLE, FT_SCREENED_OUT, FT_FLAGGED | **none**: the CLI does `db = ReviewDatabase(args.review)` then `run_ft_screening(...)`; no `open_run` / `activate` in the module |
> | FT verifier | `ft_screener.py::run_ft_verification` | `ft_verification_decisions` (`db.add_ft_verification_decision(`), `workflow_state` | FT_FLAGGED (from FT_ELIGIBLE) | none |
> | FT adjudicator | `engine/adjudication/ft_screening_adjudicator.py` | `ft_screening_adjudication` (`INSERT INTO ft_screening_adjudication`), `workflow_state` | `review_db.update_status(int(paper_id), decision)`, decision ∈ `("FT_ELIGIBLE", "FT_SCREENED_OUT")` | none |

and: "**I2 verdict: partly false.** Abstract screening driven by `run_pipeline` opens a
`screening` manifest and records its calls. FT screening, FT verification, both adjudicators
and `screen_expanded.py` open none."

**The code at HEAD.**

*No manifest on this path.* `engine/agents/ft_screener.py`'s `__main__` block does
`spec = load_spec_for(args.review, args.spec)` and `db = ReviewDatabase(args.review)`, then
`run_ft_screening(db, spec, …)` and/or `run_ft_verification(db, spec, …)`. The module contains
no `open_run`, `activate` or `active_run` (grep). `scripts/run_pipeline.py` has no FT stage:
`STAGES = ("search", "screen", "parse", "extract", "audit", "export")` and
`_PIPELINE_STAGE_CONFIGS` has keys `"screen"` (`("abstract_screen_primary",
"abstract_screen_verifier")`), `"parse"`, `"extract"` and `"audit"` only. **FT screening runs
only from its own CLI, and only without a manifest.** This matches 9e-SE-A.

*Where the FT temperature is set today.*
- **Declared:** `engine/core/review_spec.py::FTScreeningModels`,
  `temperature: float = Field(default=0.0, ge=0.0, le=1.0)`, with the comment "A plain float,
  unlike the other stages: the FT sites have always sent the spec's value through this float
  field, so YAML `0` goes out as `0.0`". The live spec sets it:
  `ft_screening_models:` … `temperature: 0`.
- **Resolved:** `engine/core/effective_config.py::stage_config`, branch
  `elif stage in ("ft_screen_primary", "ft_screen_verifier"):` →
  `options["temperature"], sources["options.temperature"] = blk.temperature,
  _src(blk_src, "temperature")`.
- **Overridden:** `engine/agents/ft_screener.py::_ft_config`:
  ```
  if temperature is not None and temperature != cfg.options.get("temperature"):
      cfg = cfg.with_options({"temperature": temperature})
  ```
  This is the `with_options` call R223a names. **Production never reaches it:**
  `run_ft_screening` calls `ft_screen_paper(truncated, spec, model=primary_model)` and
  `run_ft_verification` calls `ft_verify_paper(truncated, spec, model=verification_model)`,
  with no `temperature=`. The only callers that pass a temperature are
  `tests/test_undeclared_override.py` (`_ft_config("ft_screen_primary", spec, None, None,
  0.7)`). `scripts/ft_screening_smoke_test.py` calls `ft_screen_paper(truncated, spec)` and
  `ft_verify_paper(truncated, spec)` with neither.

**Finding 2: the FT temperature is already declared and resolver-supplied.** Expected:
"`with_options` sets the FT temperature outside the resolver". Found: the resolver sets it from
`ft_screening_models.temperature`. `with_options` is only a dead caller-override branch on top.
"Declaring the FT temperature in the same commit" (R223a, and the plan's "Next" block) has
nothing left to declare. Only the removal remains: the `temperature` parameter of `_ft_config`,
`ft_screen_paper` and `ft_verify_paper`, and the branch. The first ruling question is whether
R223a means something else by "declare", such as moving the float carve-out.

**Finding 3: `_ft_config` carries a second caller override that R223a does not name.**
```
if think is not None and think != cfg.think:
    from engine.core.effective_config import _replace
    cfg = _replace(cfg, think=think, sources={**cfg.sources, "think": "caller"})
```
It goes through the private `_replace`, not `with_options`, so R223's `UndeclaredOverride`
check (which lives in `with_options`) would **not** fire on it under an active run. Production
passes no `think=` either. A third override is `stage_config(stage, spec, model=model)`, which
records `sources["model"] = "caller"` when `model` is given. Production passes the spec's own
value (`spec.ft_screening_models.primary` / `.verifier`), so the sent model is the spec's, but
by reading, its source is recorded as `caller`. Only the first of these is in R223a.

*`papers.status` writes on the FT path, and from where* (**by reading**, every `update_status`
call in `ft_screener.py` and the adjudicator):

| token | function | site (content anchor) | guard |
|---|---|---|---|
| FT_FLAGGED | `run_ft_screening` | `"Paper %d has no parsed text — marking FT_FLAGGED"` → `db.update_status(pid, "FT_FLAGGED")` | `if current_status not in _PAST_FT:` |
| FT_FLAGGED | `run_ft_screening` | `"malformed FT screening output — flagging"` → `db.update_status(pid, "FT_FLAGGED")` | `if current_status not in _PAST_FT_2:` |
| FT_ELIGIBLE | `run_ft_screening` | `elif decision.decision == "FT_ELIGIBLE":` `db.update_status(pid, "FT_ELIGIBLE")` | the `if current_status in _PAST_FT:` branch writes no status |
| FT_SCREENED_OUT | `run_ft_screening` | `else:` `db.update_status(pid, "FT_SCREENED_OUT")` | same |
| FT_FLAGGED | `run_ft_verification` | no parsed text; malformed output; `else: db.update_status(pid, "FT_FLAGGED")` | **unguarded** (R196) |
| FT_ELIGIBLE / FT_SCREENED_OUT | `ft_screening_adjudicator._apply_ft_decisions` | `review_db.update_status(int(paper_id), decision)` | `except ValueError` logs "status update failed — adjudication recorded but paper will not progress" and continues |

The verifier's confirm writes no status: `if decision.decision == "FT_ELIGIBLE":
stats["confirmed"] += 1`. `_PAST_FT` is `{"FT_ELIGIBLE", "FT_FLAGGED", "EXTRACTED",
"EXTRACT_FAILED", "AI_AUDIT_COMPLETE", "HUMAN_AUDIT_COMPLETE", "REJECTED"}`.

**Divergences from 9e-SE-A: none of substance.** Its rows, its "two sites" for FT_ELIGIBLE and
its three unguarded verifier sites all read true at HEAD. Findings 2 and 3 are about the
temperature and override mechanics, which 9e-SE-A did not cover.

---

## §3 The four bridge writes (R205)

R205, quoted: "the FT screening path opens a `screening` manifest; `eligible` is written on the
verifier's confirm and on the adjudicator's FT_ELIGIBLE; `full_text_out` on the primary's
exclude and the adjudicator's FT_SCREENED_OUT; the primary's include writes no event".

**Where each decision is made today, and on what transaction.** Every site uses the
`ReviewDatabase` connection `db._conn` / `review_db._conn`, which is
`sqlite3.connect(str(self.db_path))` with Python's default isolation, so DML opens an implicit
transaction (`engine/core/database.py`, `ReviewDatabase.__init__`; no `isolation_level` is set
in `database.py`, `events.py` or `extraction_events.py`, by grep).

| write | decision site | transaction today (by reading) |
|---|---|---|
| verifier confirm → `eligible` | `run_ft_verification`, `if decision.decision == "FT_ELIGIBLE": stats["confirmed"] += 1`, directly after `db.add_ft_verification_decision(…)` | `add_ft_verification_decision` INSERTs and then `self._conn.commit()`. The confirm branch writes nothing. A bridge event here would be its own transaction, after the decision row has already committed |
| adjudicator FT_ELIGIBLE → `eligible` | `ft_screening_adjudicator._apply_ft_decisions`, `review_db.update_status(int(paper_id), decision)` | **one implicit transaction for the whole batch.** Each `INSERT INTO ft_screening_adjudication` opens it, `update_status` sees `in_transaction` and "participates in that transaction without starting a nested one", and a single `review_db._conn.commit()` follows the loop. `complete_stage(… "FULL_TEXT_ADJUDICATION_COMPLETE" …)` runs after that commit |
| primary exclude → `full_text_out` | `run_ft_screening`, `else: db.update_status(pid, "FT_SCREENED_OUT")` | `add_ft_screening_decision` commits first. `update_status`, not in a transaction, then runs its own `BEGIN IMMEDIATE` … `COMMIT`. Decision row and status are **two transactions** today |
| adjudicator FT_SCREENED_OUT → `full_text_out` | as the adjudicator's FT_ELIGIBLE | as above |

Two properties of today's sites bear on the bridge (by reading):
- **The primary's exclude writes no status for a paper already past FT.** Under
  `if current_status in _PAST_FT:` it logs "FT decision recorded without status change".
  Whether `full_text_out` follows the status write or the decision is not stated in R205. It
  matters on live, where all 190 corpus papers are `AI_AUDIT_COMPLETE` (9e-SE-A §6) and
  `run_ft_screening` selects `get_papers_by_status("PARSED") +
  get_papers_by_status("AI_AUDIT_COMPLETE")`.
- **The adjudicator swallows a refused transition** (`except ValueError`) and records the
  adjudication row anyway. Whether a bridge event is written when the status write is refused
  is not stated in R205.

**The `paper_events` constraints at HEAD.** `paper_events` is created by `016`, reshaped by
`019` and `020`, and rebuilt last by `022` (`"* **`paper_events` rebuilt** (R213):
`identified` and `duplicate_of` leave both"`; `def paper_events_sql(name: str =
"paper_events")`). The live DDL (`sqlite_master`, `mode=ro`), quoted:

```
actor_kind  TEXT    NOT NULL CHECK (actor_kind IN ('model', 'human', 'engine')),
actor_role  TEXT    NOT NULL CHECK (actor_role IN ('reviewer', 'extractor', 'system')),
...
to_state    TEXT    NOT NULL CHECK (to_state IN ('eligible', 'abstract_out', 'full_text_out', 'parsed', 'extracted', 'extraction_failed', 'full_text_not_obtainable', 'parse_failed', 'input_exceeds_context', 'audited_ai')),
...
CHECK (actor_role <> 'system' OR actor_kind = 'engine'),
CHECK (event_type IN ('screened', 'verified', 'adjudicated', 'acquired', 'not_obtainable', 'parsed', 'extracted', 'extraction_failed', 'audited', 'manual_advance', 'bypass', 'state_at_migration')),
-- R39: an event's type and its to_state must name the same axis.
CHECK (
    (event_type IN ('screened', 'verified', 'adjudicated') AND to_state IN ('eligible', 'abstract_out', 'full_text_out'))
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

`reason TEXT` and `stage_name TEXT` carry no CHECK. **`paper_events` has no `arm` column.**
`run_manifests` (022): `run_kind TEXT NOT NULL CHECK (run_kind IN ('extraction',
'screening', 'judge', 'review_session', 'import'))`, which matches
`RUN_KINDS = ("extraction", "screening", "judge", "review_session", "import")` in
`engine/core/run_manifest.py`. Expected values met: the reason CHECK permits no reason on
`eligible` or `full_text_out`, and `run_kind` includes both `'screening'` and `'import'`.

**The writer's refusals.** `engine/core/events.py::write_paper_event` calls only
`_run_link(conn, run_id, run_marker, migration=_called_from_migration(2))`, which refuses:
`"event refused: run_marker={run_marker!r} may be written only by a migration (R68)"`;
`"event refused: run_id is required (R68)"`; and
`"event refused: run_id {run_id} names no run manifest (R68)"`. The arm refusals are in
`_refuse_claim_on_arm` — `ClaimOnPreManifestArm` ("registered pre-manifest; it never pins"),
`ClaimOnRetiredArm` ("a retired arm accepts no new claims"), `ArmNotInRun` ("run {run_id} did
not pin arm {arm!r}"). They are reached **only from `write_field_event`** (the claim path). **No
arm check applies to a paper event**, as 9e-SE-A §3 also found.

**What an `eligible` / `full_text_out` event under a screening manifest must carry** (by
reading):
- `run_id` of an existing `run_manifests` row, and `run_marker` NULL (R68 CHECK and `_run_link`).
- `event_type` from `('screened', 'verified', 'adjudicated')` (the R39 axis CHECK). The natural
  pairing is primary exclude → `screened`, verifier confirm → `verified`, adjudicator →
  `adjudicated`; R205 does not name the types.
- `reason_code` NULL (neither token is a failure token).
- `actor_kind` / `actor_role` within the CHECKs, with `actor_role 'system'` only for
  `actor_kind 'engine'`. R205 does not fix them. R202 fixes `human` / `reviewer` for **import**
  runs only. Today's processing-axis writers use `actor_kind="engine", actor_role="system"`
  (`extraction_events.py`, `audit_events.py`), while a model decision could be `model` /
  `reviewer`.
- No arm: there is no column for one.

**Does a screening-kind manifest have an arm? It needs none.** `open_run`'s signature takes
`arms: Iterable[str] = ()` and `named_arms = sorted(set(arms_by_stage.values()) | set(arms))`,
where `_stage_arm` returns an arm only for `cloud:` stages and `_LOCAL_EXTRACTION_STAGES`, and
`return None` otherwise. With `stages=("ft_screen_primary", "ft_screen_verifier")` a screening
manifest pins **zero** arms. **`open_run` does not require an arm for every kind**, so the brief's
"first ruling the bridge needs" does not arise in that form. What `open_run` **does** require of
a screening run:
- a `codebook` (`codebook_hash` / `codebook_sha256` NOT NULL). `run_pipeline._open_run_manifest`
  gets it from `load_codebook_beside(db.db_path)`; the FT CLI loads none today;
- a clean tree (`DirtyTree`);
- digests of the declared Ollama stages (the `fetch_model_digest` path).

**Finding 4: the adjudicator is not a screening run by construction.** It is a human decision
imported from a JSON/xlsx file, and `open_review_session(conn, spec, *, codebook, …)` exists
for exactly that (R68: "a reviewer's session is a run of kind `review_session`"). R205 says "the
FT screening path opens a `screening` manifest" and places two of the four writes at the
adjudicator. Whether the adjudicator's events run under a `screening` manifest or a
`review_session` is not ruled.

---

## §4 B10 — the writer's one-transaction property

**The boundary.** `engine/core/extraction_events.py::write_extraction_events`, quoted:

```
conn.execute("SAVEPOINT extraction_events")
try:
    for fe in plan.field_events:
        write_field_event(conn, **fe, commit=False)
    write_paper_event(conn, **plan.paper_event, commit=False)
    conn.execute("RELEASE extraction_events")
    if conn.in_transaction:
        conn.commit()
except BaseException:
    if conn.in_transaction:
        conn.execute("ROLLBACK TO extraction_events")
        conn.execute("RELEASE extraction_events")
    raise
```

**The writes inside it** (by reading):
1. each planned field event, via `write_field_event(conn, **fe, commit=False)` — `superseded`
   (with `field_event_against` rows), `asserted`, `declined` or `contract_unmet`;
2. inside the **first claim-bearing** `write_field_event` of an `extraction_uid`, the
   `claim_inputs` row (`engine/core/events.py`: `if insert_claim_inputs:` →
   `"INSERT INTO claim_inputs (extraction_uid, arm, paper_id, reuse_key, …"`, R217), plus any
   `field_event_against` / `field_event_against_decisions` rows;
3. the paper event, via `write_paper_event(conn, **plan.paper_event, commit=False)`.

**No `citation_located` is written here.** The module says "no `citation_located` event — the
locator is 2(d)". The locator writes them in `engine/agents/audit_events.py`
(`event_type="citation_located"`), in the audit stage's own savepoint. The R130 telemetry row is
"written after the commit, outside it". Expected values met: one savepoint per paper, and
`claim_inputs` inside it.

**The test gap, as documented.** `tests/test_atomic_terminal_write.py`'s module docstring:
"(Retired 2026-09-25 with the legacy atomic writer it tested — 9c-C5, R160a. The event writer's
one-transaction property, `engine/core/extraction_events.write_extraction_events`, has no test
yet: an open finding, not a covered case.)"

**Finding 5: the expectation "no mid-write failure test" is false as stated.**
`tests/test_extraction_events.py::test_t11_a_refusal_on_the_last_field_writes_nothing_for_the_paper`
(added at `ca93558`, "WRITE-PATH-01 9b-2c", 2026-09-25) drives exactly that:

```
rec = _record(db, spec, run_id, [_value("study_design"), _value("country", "USA"),
                                 _value("sample_size", "40")])
real, n = X.write_field_event, {"k": 0}

def flaky(conn, **kw):
    n["k"] += 1
    if n["k"] == 3:
        raise events.ArmNotInRun("refused on the last field")
    return real(conn, **kw)
with patch.object(X, "write_field_event", side_effect=flaky):
    with pytest.raises(events.ArmNotInRun):
        X.write_extraction_events(db._conn, rec, sentinels=sentinels)
assert _fev(db) == [] and _pev(db) == []
```

Two real field events are written and then rolled back, and no paper event is written. The
docstring in `test_atomic_terminal_write.py` and plan row B10 ("no test drives a mid-write
failure on the event path") therefore disagree with the tree. **What T11 does not cover** (by
reading):
- **`claim_inputs` is not asserted.** The first `asserted` event writes the `claim_inputs` row
  (R217, which landed at `85669ff`, after T11). T11 asserts only `_fev` (`field_events`) and
  `_pev` (`paper_events` excluding `event_type <> 'screened'`). It would stay green if
  `claim_inputs` survived a rollback.
- **No failure at the paper event.** Every field event succeeds and then
  `write_paper_event` fails, which is the "between the first field event and the paper event"
  boundary the brief names. T11 fails on the third field event, before the paper event is
  reached.
- **No database-level failure.** T11 raises a Python exception from a patched function. A CHECK
  or FK failure raised by SQLite inside the savepoint is not exercised.
- **No enclosing transaction.** `if conn.in_transaction: conn.commit()` commits a caller's
  outer transaction too. No test opens one first.

**What a fixture would need** (a statement of shape only, not written): the existing `db`,
`spec`, `run_id` and `sentinels` fixtures of `test_extraction_events.py` (a `tmp_path`
`ReviewDatabase`, `open_extraction_run(db, spec)`); a record of N ≥ 2 values; and
`patch.object(X, "write_paper_event", side_effect=…)` raising after the real field events, or
`patch.object(X, "write_field_event", …)` raising on call k with 1 < k ≤ N. It would then assert
that `field_events`, `paper_events` (unfiltered), `claim_inputs` and `field_event_against`
are all empty for the paper, and that `db._conn.in_transaction` is False. A SQLite-raised variant
would need a planned paper event that violates a CHECK, for example a `to_state` with an illegal
`reason_code`. That requires the plan or the patch to produce one, and it is not answered here
because it needs a fixture to try.

---

## §5 The two unruled backups (I10)

**Stat and bytes, before any read** (`stat -c '%n|%s|%y'`, `sha256sum`; measured):

| file | size B | mtime (UTC) | sha256 |
|---|---|---|---|
| `review_backup_pre_refactor.db` | 2,310,144 | 2026-03-05 19:21:47.866 | `a97c74d9…94eb` |
| `review_backup_pre_refactor.db-shm` | 32,768 | 2026-08-28 22:12:01.265 | `fd4c9fda…89eb` |
| `review_backup_pre_refactor.db-wal` | 0 | 2026-05-26 14:31:10.656 | `e3b0c442…b855` (empty) |
| `review_backup_v1_schema.db` | 2,121,728 | 2026-03-04 03:58:27.940 | `766e93f3…fdb9` |
| `review_backup_v1_schema.db-shm` | 32,768 | 2026-09-22 05:05:35.611 | `fd4c9fda…89eb` |
| `review_backup_v1_schema.db-wal` | 0 | 2026-09-22 05:05:35.598 | `e3b0c442…b855` (empty) |

Both main files are **WAL-mode** databases (header bytes 18–19 `0202`). The five restore points
are rollback-journal files (`0101`) with no sidecars.

**Fingerprints** (`python -m engine.tools.db_fingerprint <file>`, `mode=ro`, `immutable=False`,
no `--out`; measured):

| file | tables | overall_sha256 |
|---|---|---|
| `review_backup_pre_refactor.db` | 6 | `1f3e5f689bdc76dd4f2917f80abf7e19cec11d6de478076cddc6caa739911689` |
| `review_backup_v1_schema.db` | 8 | `b1db6cf544578517ba08abb985849259e6dcbc498cc6029fea2e58f03d5acb70` |
| `…bak-migration-02-phase3-pre-write-20260929-172104` | 34 | `ea05912d67f0b841bf003a7a941f6505e2c57140b5c2f62e1ff446ea16b9ce01` |
| `…bak-input-identity-01-phase3-pre-write-20260924-204138` | 34 | `bb39ba81170c4f11d59954b9e70837afc427c66da710bde30881aad570d16c40` |
| `…bak-manifest-01-phase3-pre-write-20260923-161020` | 31 | `e564f250afe40af7eb9a6bc07596a3c795972f7ef3b653c37599e8bc18285b63` |
| `…bak-readers-01-phase3-pre-write-20260922-165453` | 32 | `62f3912813b6e9efa3a651ee4c4ebcd611adf89c989f240a58e83dcf6759b79a` |
| `…bak-pre-migrations01-20260921-002807` | 24 | `f376562e095cfbcbf23ca997f31375feba340f74ed7a14ffe26e42cc63839e00` |

Every restore point equals I9 and the plan's session-11 expected values
(`…bak-pre-migrations01` `f376562e…9e00`). **Neither unruled backup's overall hash equals any
of the five retained hashes.** Both have far fewer tables than the oldest retained copy (6 and 8
against 24), so both predate migration 002 and everything after it. The expectation holds.

**What they are** (`mode=ro`, `sqlite_master` and counts; measured):
- `review_backup_pre_refactor.db`: tables `evidence_spans, extractions, full_text_assets,
  papers, review_runs, screening_decisions`; papers 251 (`AUDITED` 96, `SCREENED_OUT` 155);
  extractions 96; evidence_spans 1,439.
- `review_backup_v1_schema.db`: the same six plus `cloud_evidence_spans, cloud_extractions`;
  papers 251 (`AUDITED` 96, `SCREENED_OUT` 155); extractions 96; evidence_spans 1,429.

**Finding 6: `review_backup_v1_schema.db` has a consumer in the gate.**
`tests/test_cloud_extraction.py`: `BACKUP_DB = … / "data" / "surgical_autonomy" /
"review_backup_v1_schema.db"`, with a module `pytestmark = pytest.mark.skipif(not
BACKUP_DB.exists() …)`, and the `test_db` fixture does `shutil.copy2(BACKUP_DB, db_copy)` on
every use (row B14). Retiring or moving this file would silently turn that module's tests into
skips (B14's own words: "a lost corpus reads as skips, not reds"). So a retention ruling on it is
also a ruling on B14. The fixture copies only the main file, and the original's `-wal` is 0 B, so
the copy is complete. `review_backup_pre_refactor.db` has no reference in any `.py` file (grep).
Its only other mentions are in `docs/plan/ENGINE_REFACTOR_PLAN.md`,
`docs/session-reports/VERIFY-EXIT-01_font-audit_report.md` and
`docs/session-reports/write-path-01/WRITE-PATH-01_phase1_slice3_census_20260925.md`.

**Finding 7: a `mode=ro` fingerprint of a WAL database touches its `-shm` mtime.** After the two
fingerprints above, both main files, both `-wal` files and both `-shm` files were
**byte-identical** (`sha256sum -c` → all six `OK`), and every size and main-file mtime was
unchanged. But both `-shm` mtimes moved to 2026-09-30 03:10:06 UTC, because SQLite maps and
touches the shared-memory index on a WAL open, `mode=ro` included. Acceptance gate 6 is about the
two backups (size and mtime); the main files meet it. The sidecars' contents are unchanged and
their mtimes are not. An open that avoids the `-shm` entirely needs `immutable=1`, which the
database invariant forbids for live and which no ruling covers for a static backup.

---

## Open questions for the architect (before B21 or the bridge is specified)

1. **B21, the excluded map.** A spec-presence predicate moves a smoke copy from `ids` into
   `data_dirs_excluded`, which `drift()` also compares (Finding 1). Should B21 (a) exclude a
   declared throwaway pattern from both sets, (b) give `excluded` a second reason ("no spec")
   and accept that a smoke copy still drifts, or (c) stop comparing `data_dirs_excluded` in
   `drift`? And is `test_review_ids_exclude_data_dirs_without_a_review_db` rewritten or kept
   beside a new test?
2. **R223a, the FT temperature.** It is already declared (`FTScreeningModels.temperature`) and
   resolved (Finding 2). Is the commit only the removal of the dead `with_options` branch and
   the `temperature` parameters? And do the `think` override (via `_replace`, which bypasses
   `UndeclaredOverride`) and the `model=` pass-through that records source `caller` (Finding 3)
   go in the same commit?
3. **The bridge, the adjudicator's run.** Under a `screening` manifest, or a `review_session`
   (`open_review_session`) (Finding 4)? And what `actor_kind` / `actor_role` does each of the
   four events carry, with `event_type` `screened` / `verified` / `adjudicated` as the natural
   pairing?
4. **The bridge, the edge cases.** Is `full_text_out` written when the primary excludes a
   paper already past FT (the `_PAST_FT` branch, which writes no status: every live corpus
   paper), and when the adjudicator's `update_status` is refused and swallowed? Is the bridge
   event written in the same transaction as its decision row? Today the primary's decision row
   and status are two transactions, and the verifier's confirm has only a committed decision
   row.
5. **The bridge, the codebook.** A screening manifest needs a codebook (`open_run`'s NOT NULL
   hashes). The FT CLI loads none today. Is `load_codebook_beside(db.db_path)`, as in
   `run_pipeline`, the source?
6. **B10, its scope.** T11 already drives a mid-field rollback (Finding 5). Is B10 closed by
   extending T11, or by a new test adding the paper-event failure, the `claim_inputs` assertion,
   a SQLite-raised variant and an enclosing transaction? And is the stale docstring in
   `test_atomic_terminal_write.py` corrected in the same commit?
7. **The two backups.** `review_backup_v1_schema.db` is a gate fixture (B14, Finding 6), so its
   retention is tied to B14's fix. `review_backup_pre_refactor.db` has no code consumer. Both
   predate the lane, and neither equals a retained hash.
