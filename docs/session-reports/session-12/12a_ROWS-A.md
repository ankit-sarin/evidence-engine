<!-- PROVENANCE HEADER (12c-OPEN-R2 R-1) — everything below the END HEADER line is the body, byte-for-byte.
report: 12a-ROWS-A (session 12a, CC session 2eb166e9), read-only; never written to a file in 12a
source: ~/claude-session-archive/2eb166e9-ff84-4140-9001-00929be1254e.jsonl, line 749 (1-based; index 748 0-based), the single assistant text block, message timestamp 2026-10-01T00:48:09.551Z
chars: 11425
written at tree: 198a349 (198a3492ff5babb3bcfc4c0f7144fa20e555bef0)
extracted: 2026-10-01 (session 12c, CC session bbd7293d)
body sha256 (UTF-8, no trailing newline added): dcb3f26f274748c8a803583b78283b137d0fa3fddc3ae1de930d99f2c258f70a
END HEADER -->
The census found no row that is stale about the code: all 13 descriptions hold at HEAD `198a349`, with one stale figure in I20 and one row (D14) whose count I couldn't re-measure without the database. Part 1 matched, so the census ran. I wrote nothing and the tree is unchanged.

## Part 1 — MATCH
- Line 100 is 247 characters and ends `…date_status\` refuses any transition into or out of one with \`RetiredTransition\`)`. The parenthesis closes; the earlier cut-off was a display artifact.
- The block is the header plus 7 lines, then one blank line before `## Pipeline Stages` (9 physical lines including that blank).

## How Run 7 is launched (assumed, not recorded)
No plan text gives Run 7's command. Every extraction smoke used `scripts/run_pipeline.py --review <id> [--spec …] --skip-to extract --max-papers N`, so I traced that path:
- `main()` calls `run_pipeline(...)`, which opens the manifest and runs `_stage_extract` and then `_stage_audit`.
- It then reaches the audit-review gate, which returns BLOCKED and closes the run `"interrupted"`.
- `_stage_extract` → `select_for_extraction` → `run_extraction` → `extract_paper`. With elicitation on, `extract_paper` hands off to `extract_paper_elicited`.

The plan records this outcome for two smokes: "manifest `('interrupted', NULL)` at the audit-review gate (C40/C47)" (`ee_extract_smoke_11c`, and the 11c EE-X run). FT screening is not a `run_pipeline` stage (`STAGES = ("search", "screen", "parse", "extract", "audit", "export")`).

## Census table
| Row | (a) at 198a349 | (b) on Run 7 path? | (c) smallest fix | (d) test ids |
|---|---|---|---|---|
| C38 | STILL TRUE (T9 partly covers it, see below) | NO (migration tests) | Five new tests in `test_022_…py`, one per postcondition | +5 additions, 0 changes |
| C39 | STILL TRUE | YES, inert (audit writes `audit_verdicts`; the missing FK only matters for an unknown arm name) | Migration 023 rebuilds `audit_verdicts` with `arm REFERENCES arms(arm_name)` (live has 0 rows); this is a live write | Additions (023 tests); T9's expected table set gains nothing |
| C40 | STILL TRUE | **YES**: every `--skip-to extract` run ends at this gate | Rule what `end_reason` an interrupted close carries; have the two BLOCKED exits pass `reason=` naming the blocking stage; add one test driving a BLOCKED exit through `close_run` | +1–2 additions |
| C41 | STILL TRUE | `_with_think` **runs but always returns early**; nothing in production passes `think=`, with or without elicitation | Delete `_with_think` and the `think=` parameters on the three signatures; the eval runners pass `cfg=` instead | **Change:** `test_thinking_channel.py::test_pass1_think_is_overridable` is removed or rewritten; eval-runner tests to check |
| C47 | STILL TRUE | **YES** (`run_pipeline`); the FT half NO | Catch `KeyboardInterrupt` in `run_pipeline` and close `"interrupted"` with a reason before re-raising; same in `run_ft_invocation`. SIGHUP/SIGTERM need a signal handler that raises | +2 additions |
| C48 | STILL TRUE | NO (FT only) | Pass `paper_id=` through `ft_screen_paper` / `ft_verify_paper` to `ollama_chat` | +1 addition; check the request-capture test for kwargs pins |
| C49 | STILL TRUE | NO (FT only) | Add `spec=spec` to the FT `require_preflight` call; the live spec has no `preflight:` key, so nothing changes on the wire | +0–1 |
| C50 | STILL TRUE | YES, inert (reached through `write_extraction_events`; the rollback only fires if an INSERT fails) | Call `conn.rollback()` only when `commit=True`, otherwise re-raise and leave the transaction to the caller (`extraction_events` already holds a SAVEPOINT) | +1 addition |
| D14 | Code side STILL TRUE; the 250 count can't be re-measured without DB access | NO (selection only visits eligible papers) | Per its own row: a seed migration from files and hashes, if Run 7's scope needs one | Additions only |
| D21 | STILL TRUE | **YES** (`select_for_extraction` is Run 7's selection; pre-flight measured 0 of 190) | At selection, write a processing event carrying `exc.reason_code` for each refused paper (inside the run) | +1–2 additions; `test_selection.py` expectations may change |
| I20 | Code STILL TRUE; **wall figure CHANGED** (row ~640–655 s, today 730.61 s) | NO (test-only) | Patch `engine.utils.ollama_preflight.ollama.ps` in `test_raises_on_failure`, as its sibling does | 0 id change |
| A16 | STILL TRUE (both halves) | NO for the extraction run; CONDITIONAL: reached whenever concordance compares Run 7's arm | Route `check_schema_parity` by the arms registry instead of the name `"local"`: pre-manifest `local` reads `extractions`, a pinned arm reads its manifest's `codebook_hash`. Correct the frozen docstring's "nine" to three. Only 017's file is checksummed, not `corpus.py`'s | +1 addition; `test_corpus_authority.py` pins callers, not text |
| B12 | STILL TRUE (the difference is deliberate, A9) | NO (test fixture) | Build `_ELIGIBLE_FROM_STATUS` as `CORPUS_STATUSES + ("EXTRACT_FAILED",)` so the difference holds by construction | 0 id change |

## Quotes and traces
**C38.** `test_022_run_kinds_and_audit_tables.py` has T1–T9. Against the five postconditions:
- **Single transaction:** no test injects a failure partway through 022.
- **`run_stage_configs` CREATE text:** not compared. T9 compares only PRAGMA structure: `assert named == {"run_manifests", "run_calls", "claim_inputs", "audit_verdicts"}`. Its own comment says `structure()` "captures only columns, indices and foreign keys — never CHECK constraints".
- **`paper_events` other CHECKs and trigger text:** T2 asserts only `'identified'` in the before-SQL, plus that the triggers fire.
- **`audit_verdicts` columns against `audit_telemetry.FIELDS`:** not compared.
- **36-table count:** no `== 36` anywhere in tests/.

**C39.** In 022: `claim_inputs` has `arm TEXT NOT NULL REFERENCES arms(arm_name),`, while `audit_verdicts` has `arm TEXT NOT NULL,`.

**C40.**
- `run_pipeline.py:145` and `:184` both call `_finish_review_run(db, run_id, "interrupted")` (no reason). `close_run` writes `(_now(), status, reason, run_id)`.
- The only tests touching `'interrupted'` are 022's CHECK pairs (`("interrupted", None)` and `("interrupted", "r")`). `close_run` is called in tests only with `"completed"` / `"failed"`, and `_finish_review_run` only with `"aborted"` / `"failed"`.
- **New finding:** `close_run`'s docstring says "`reason` … no caller passes it yet; wiring `RunAborted` … is 10a-C3", but `run_pipeline.py:198` already passes `reason=str(exc)`.

**C41.**
- `_with_think` (`extractor.py:195–200`): `if think is None or think == cfg.think: return cfg` … `return _replace(cfg, think=think, sources={**cfg.sources, "think": "caller"})`.
- `_replace` builds `EffectiveConfig(**data)` directly. Only `with_options` raises `UndeclaredOverride`.

**C47.**
- `run_pipeline`: `except RunAborted` / `except Exception` / `finally: rm.deactivate(run_token)`.
- `run_ft_invocation`: `except Exception: … rm.close_run(db._conn, run_id, "failed")` / `finally`.
- No `signal.signal` anywhere in engine/ or scripts/. A `KeyboardInterrupt` derives from `BaseException`, so it skips both handlers.

**C48.** Both FT calls are `ollama_chat(messages=build_ft_messages(...), return_request_hash=True, **cfg.kwargs())`, with no `paper_id`. `ollama_chat` takes `paper_id: int | None = None`.

**C49.**
- FT: `require_preflight([spec.ft_screening_models.primary, spec.ft_screening_models.verifier], runner_name="FT screening",)`.
- Extractor: `require_preflight([extractor_model], runner_name="Extraction", spec=spec)`.
- `grep '^\s*preflight' review_specs/surgical_autonomy.yaml` returns nothing.

**C50.** In `events.py`, the `try:` opens at line 383, around the INSERTs only: `… if commit: conn.commit()` / `except Exception: conn.rollback(); raise`. `write_paper_event` has no handler. The caller `extraction_events.py:443–453` uses `SAVEPOINT extraction_events` … `except BaseException: if conn.in_transaction: ROLLBACK TO …`. Because the inner `rollback()` ends the transaction, the caller's `ROLLBACK TO` is skipped, and the net effect is the same whole rollback the row calls harmless.

**D14.** 017 seeds `parsed_text_refs` from rows filtered by `corpus_status_sql("p.status")` (lines 92 and 131). `resolve_parsed_text` raises `NoParsedText` for a paper with no ref.

**D21.** `selection.py:81–88`:
```python
for paper_id in eligible_paper_ids(conn):
    try: ref = resolve_parsed_text(conn, paper_id); read_parsed_bytes(ref)
    except ParsedTextError as exc:
        logger.warning("Paper %d: not selected — %s (%s)", …)
        skipped_refused.append((paper_id, exc.reason_code)); continue
```

**I20.**
- `test_raises_on_failure` patches only `check_ollama_env` and `ollama_chat`. `preflight_check` still reaches `ps = ollama.ps()` (line 116) after a failed model check. Its sibling patches `engine.utils.ollama_preflight.ollama.ps`.
- The waits are still there: `time.sleep(60)` / `(30)` paired with `wall_timeout=1.0` / `2.0` in `test_ollama_client.py` and `test_restart_recovery_guard.py`. I didn't re-time them.

**A16.**
- `concordance.py:186`: `if arm == "local": … FROM extractions` / `else: … FROM cloud_extractions WHERE arm = ?`.
- `corpus.py:42`: "Its forward closure from `FT_ELIGIBLE` is nine statuses, not four — it drags in `EXTRACT_FAILED`, …".

**B12.**
- `_corpus_fixture.py:148`: `("FT_ELIGIBLE", "EXTRACTED", "AI_AUDIT_COMPLETE", "HUMAN_AUDIT_COMPLETE", "EXTRACT_FAILED")`.
- `corpus.py:59`: `CORPUS_STATUSES` holds the four without `EXTRACT_FAILED`. Its only user is `test_corpus_authority.py`.
- The cloud copy (`test_cloud_extraction.py:85`) still holds the four (F15's row).

## C41 trace with elicitation on (I3 holds)
1. `extract_paper` hands off to `extract_paper_elicited(...)` with no `think`.
2. That calls `elicit(unit_map, codebook, field_names, paper_id, cfg=cfg_p1,)` (`pipeline.py:284–286`), again no `think`.
3. `elicit` calls `run_pass1(…, think=think, cfg=cfg)` with `think=None`. `run_pass1` runs `cfg = _with_think(cfg or stage_config("elicitation_pass1"), think)`, which returns early on `think is None`.
4. Pass 2 is `extract_pass2_structured(pass2_prompt, priming_msg, spec, paper_id, cfg=cfg_p2, …)`, with no `think`, so the same early return.

The only callers that pass `think=` are `analysis/eval/run_local_abc.py:130` and `smoke_regression01.py:66` (with `models.pass1_think`), and the test `test_pass1_think_is_overridable` (`think=False`). That test is the one contract a fix would change.

## Inferred items and row text
- **I1:** true (a display artifact).
- **I2:** true for the code in every row. Exceptions: I20's "~640–655 s" is stale, and D14's count is unverified (no DB access).
- **I3:** true.
- **I4:** C40 and C47 apply to `run_pipeline`, the assumed Run 7 entry point. C47's FT half does not.

## Things I noticed but didn't trace
- The extractor's per-paper loop catches `except Exception` (`extractor.py:1026`), so `outcome_for_exception`'s `isinstance(exc, KeyboardInterrupt)` branch looks unreachable from that loop. I didn't trace its other possible caller near line 714.
- With elicitation on, `_open_run_manifest` builds the preflight list from `stage_config("extract_pass1", spec).model` rather than from `elicitation_pass1`. I didn't check whether the two can differ.

Tree: `198a349`, clean, 1 ahead of origin. claude-config still holds the uncommitted ledger row for the wrap. I'm stopping here for your ruling.