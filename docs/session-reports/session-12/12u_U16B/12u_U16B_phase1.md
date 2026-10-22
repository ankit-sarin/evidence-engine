# 12u-U16B Phase 1 — U16's acceptance tests against HEAD (no engine edit)

Session 12u, 2026-10-22 UTC. Item 0 landed at `21a224d6f54e91fe80685051ff8c6ea941be9b58` (R685 as
Block 83, R686 as Block 84, four notes). The tests were written in a scratch clone of
`97e50798b854dab26d0206e8421411241810b68a` (`~/scratch/12u-u16b/checkout/`, the live codebook copied
into its `data/surgical_autonomy/`, the 12t-U15B form). Its engine and tests trees equal HEAD's:
`git diff --stat 97e5079 HEAD -- engine tests scripts analysis review_specs` is empty. Every run
used `--basetemp` under `~/scratch/12u-u16b/bt/`. The repository's `engine/` and `tests/` are
unchanged. `r687.txt` (R687, to be transcribed at the Phase 2 item 0) is beside this file.

Files beside it:

| file | what |
| --- | --- |
| `test_audit_stage_completion_phase1.py` | the test file as run (it goes to `tests/test_audit_stage_completion.py` in Phase 2) |
| `head_run.txt` | the file against HEAD's modules: 5 failed |
| `test_audit_stage_completion_phase1_v1.py`, `head_run_1_v1.txt` | the first version and its run; see §6 finding 1 |
| `head_run_0_no_basetemp_parent.txt` | a setup error: the basetemp's parent did not exist; no test ran |
| `census_regression.txt`, `regression_run.txt` | the regression set's census and its run: 157 passed |
| `journal_run_window.txt` | `journalctl -u ollama` from 03:44:00 UTC: no entries |

## §1 The tests

§5 of `12u_U16A.md`, verbatim:

> | T1 `test_ai_audit_stage_completes_only_when_every_extracted_corpus_paper_is_audited` (P1A's) | completes (`u16_states`) | pending, the shortfall names paper 2; completes once 2 is audited |
> | T2 `test_a_text_refused_at_audit_leaves_the_ai_audit_stage_pending_and_named` | completes (run B) | pending, names 17 and 18 with `parsed_text_modified` / `parsed_text_missing` |
> | T3 `test_an_extracted_paper_with_nothing_to_audit_is_reported_not_silent` | no line (`u16_declined`) | outcome per finding 4's ruling, with a line naming paper 22 either way |
> | T4 `test_a_failing_workflow_advance_is_not_swallowed` | swallowed, by code (finding 6; not driven) | the run fails with the error |
> | T5 `test_a_second_arm_with_unlocated_claims_keeps_the_stage_pending` | completes (`u16_second_arm`) | per the second-arm option ruled; latent under R71 |

**Deferred:**
- **T4** goes to C56 handler 3 (R685(e)(iv)).
- **T5** goes to A15 under F15 (R687(b)). The processing axis is per paper, so a per-arm stage
  completion cannot be expressed while A15 stands. Under R685(e)(i), T5's world (paper
  `audited_ai`, nothing `extracted`) completes the stage, so a "keeps … pending" test could not pass
  after this fix (R687(a)).

**I2:** the remaining tests do not contradict R685(e) (R687 resume, 1c).

**The mapping (1b).** Every test drives `run_pipeline._stage_extract` (the model passes stubbed as
`tests/test_text_refusal_at_selection.py` stubs them), `run_pipeline._stage_audit` (`semantic_verify`
patched) and `run_pipeline._advance_extraction_workflow`, in that order, on a scratch review under
`tmp_path`. T3 and TV1 are **two** tests, both on the same all-declined world. T3 carries T3's
assertions (a line naming the paper; the paper `audited_ai`). TV1 carries R685(e)(ii)'s (the
payload, the report, the stage).

| test | fixture | action | assertion | exercises (HEAD anchor) | HEAD expected |
| --- | --- | --- | --- | --- | --- |
| T1 | 201 extracted + audited; 203's file removed so selection refuses it (`extraction_failed` `parsed_text_missing`); then 202 extracted, holding 20 live unlocated asserted claims (R687(c)) | advance; then `_stage_audit` and advance again | stage **pending**; exactly one `pipeline` line naming 202 and "extracted"; after 202 is audited the stage completes | `if audited > 0:` → `complete_stage(conn, "AI_AUDIT_COMPLETE_STAGE",` | fail (stage completes) |
| T2 | 211, 217, 218 extracted; then 217's file altered and 218's removed | `_stage_audit`, advance | a line naming 217 with `parsed_text_modified` and one naming 218 with `parsed_text_missing`; then the stage **pending** | the same; `audit_run`'s `logger.warning("Paper %d: audit skipped — %s; parsed text uid(s) %s (%s)",` | fail on its stage half; the naming half holds on HEAD |
| TV2 | T2's world | `_stage_audit`, advance | 217 and 218 stay `extracted` and appear on the report's `skipped_refused` with their reasons; 211 `audited_ai`; the stage **pending** | the same | fail on its stage half |
| T3 | 221 extracted; 222 added with 20 `declined` claims and an `extracted` event (the engine's writers; the live spec is not elicited) | `_stage_audit` | exactly one line naming 222; 222 `audited_ai` | `claims = live_claim_events(conn, pid, arm, event_types=("asserted",))` → `if not claims: continue` | fail (no line) |
| TV1 | T3's world | `_stage_audit`, advance | exactly one `audited_ai` event for 222 with payload `located == 0`; 222 `audited_ai`; exactly one `audit_events` line naming 222; the report counts it (`papers_audited == 2`, or 222 listed); the stage completes | the same | fail (no event) |

## §2 The test file

`test_audit_stage_completion_phase1.py`, sha256
`c6bd744dfce3a6019f481f68a93223a15beae8e8073abad76bf999e2d8f8029a`. Its fixtures:
- **Extraction:** `_event_store_fixture` (`open_extraction_run`, `seed_eligibility`,
  `seed_processing`, `claim_identity`, `FIXTURE_CONTEXT_SHA`) and `_parsed_text_fixture.write_parsed`.
- **The all-declined claims:** written as `12u_U16A/repro_u16a.py` writes them (I3 holds).
- **No model call:** the suite's conftest fences apply (the scratch clone carries `tests/conftest.py`).
  `require_preflight` is patched.

## §3 Against HEAD (`head_run.txt`)

`5 failed in 6.06s`, each at the assertion the expectation names:

| test | failed at | token | vs expected |
| --- | --- | --- | --- |
| T1 | line 149 `assert not is_stage_done(db._conn, STAGE)` | `assert not True` (the stage completed with 202 unaudited) | as expected |
| T2 | line 176 `assert not is_stage_done(db._conn, STAGE)`, after both naming asserts passed | `assert not True` | as expected (stage half) |
| TV2 | line 187 `assert not is_stage_done(db._conn, STAGE)`, after the state and report asserts passed | `assert not True` | as expected (stage half) |
| T3 | line 222 `assert len(said) == 1, said` | `AssertionError: []` (no line names 222) | as expected |
| TV1 | line 232 `assert len(audited) == 1, audited` | `AssertionError: []` (no `audited_ai` event for 222) | as expected |

## §4 The regression set (`regression_run.txt`)

Census (`census_regression.txt`), 12 files:
- **Named by the brief:** `test_audit_events.py`, `test_audit_admitted_set.py`, `test_audit_paper_id.py`, `test_run_pipeline_closes.py`.
- **Naming `_advance_extraction_workflow` or `AI_AUDIT_COMPLETE_STAGE`:** `test_arm_pinned_at_extract.py`,
  `test_entry_points_take_the_lock.py`, `test_run_pipeline_closes.py`, `test_stage_completion.py`,
  `test_run_interrupts.py`.
- **Naming `audit_run`, `AuditReport` or `_stage_audit`** (R685(e)(vi)): additionally
  `test_cut_over.py`, `test_extraction_events.py`, `test_migration_backfill_audit_log.py`,
  `test_ollama_preflight.py`.

Run with the standard marker expression in the scratch clone, the new file not included:
`collected 157 items`, **`157 passed in 80.73s`**, exit 0.

## §5 The fix plan for Phase 2 (R685(e)(i)–(iii); pin effect none)

**`engine/agents/audit_events.py::audit_run` — the no-claim branches (R685(e)(ii)).** Current code:

```python
        claims = live_claim_events(conn, pid, arm, event_types=("asserted",))
        if not claims:
            continue
        done = {ev.claim_id for ev in live_claim_events(
            conn, pid, arm, event_types=("citation_located",))}
        todo = [c for c in claims if c.claim_id not in done]
        if not todo:
            continue
        # U15 (R677(c)): the processing state admits the paper, or nothing is
        # written for it. Read once; the same value stamps `from_state` below.
        state = effective_state(conn, pid)
        processing = state.processing
        if processing not in AUDIT_ADMITTED_STATES:
```

The plan:
- When there is no claim to locate (`not claims`, or `not todo`), read the state once.
- If it is `extracted`, write one `audited` → `audited_ai` paper event, with the payload's existing
  `"located": 0` and `"not_located": 0`, plus `"arm"` and `"locator_version"`. No text is read, so
  there is no `parsed_text_sha256`. Then log one line naming the paper and count it on the report.
  No model call.
- If it is `audited_ai` (F10) or any other state, `continue` as today.

U15's guard and every other branch are unchanged.

**`AuditReport`** — current fields: `papers_audited, located, not_located, verified, flagged,
skipped_refused, low_yield, skipped_by_state`. The plan counts the vacuous paper in `papers_audited`
and lists it on a new field (e.g. `audited_vacuously: tuple[int, ...]`). `skipped_refused` keeps
carrying D23 refusals with their reasons (R685(e)(iii); TV2 asserts it, unchanged).

**`scripts/run_pipeline.py::_advance_extraction_workflow` (R685(e)(i)).** Current code:

```python
    states = _processing_states(conn)
    extracted = sum(1 for s in states.values() if s in COMPLETED_PROCESSING_STATES)
    audited = sum(1 for s in states.values() if s == "audited_ai")
    if extracted > 0:
        complete_stage(conn, "EXTRACTION_COMPLETE",
                       metadata=f"{extracted} papers extracted")
    if audited > 0:
        complete_stage(conn, "AI_AUDIT_COMPLETE_STAGE",
                       metadata=f"{audited} papers audited")
    return {"extracted": extracted, "audited": audited}
```

The plan:
- `pending` = the eligible papers whose processing state is `extracted`.
- The stage completes when `audited > 0 and not pending`.
- Otherwise one `pipeline` WARNING per pending paper, naming its id, state and processing reason.
- `EXTRACTION_COMPLETE` is unchanged.
- **The return value stays exactly `{"extracted": …, "audited": …}`** (finding 1).
- The signature stays `(conn)` (finding 2).

**The caller's `except Exception: pass`** is not changed (R685(e)(iv), C56 handler 3).

**Pin effect:** none. Audit-side events and the runner's workflow advance. No request, template,
schema or stage changes. The check is `12l_U04/pin_tuples.py` byte-identical before and after.

**The Phase 2 item 0** adds the A15 dated note (R687(b); one "−" line, prefix-checked with that
commit's rows) and transcribes R687 from `12u_U16B/r687.txt` as Block 85.

## §6 Findings (R509)

1. **The read-out's "names" is met by log lines; the advance's return cannot carry the shortfall.**
   - The first version of the file (`…_v1.py`, run in `head_run_1_v1.txt`, the same 5 failures)
     also asserted that the pending papers appear in `_advance_extraction_workflow`'s return. That
     was CC's addition, not the read-out's words (R681(c)/R4).
   - It also conflicts with the regression set. `tests/test_stage_completion.py` pins the return
     exactly ("`assert rp._advance_extraction_workflow(db._conn) == {"extracted": 1, "audited": 1}`",
     and `{"extracted": 0, "audited": 0}`, and `counts == {"extracted": 1, "audited": 0}`). In that
     last case paper 1 is `extracted`, so a pending key would break it.
   - The assertion was removed before the recorded run. T1 and T2 check the naming by log line.
   - R685(e)(i)'s "and on the stage summary" is therefore not the advance's return. In this
     project's vocabulary the stage summary is `_stage_audit`'s returned stats (`12t_U15B/12t_U15B.md`:
     "the last field, so `_stage_audit`'s `asdict(report)` carries it into the stage summary"). But
     `_stage_audit` returns before the advance runs. Where the shortfall appears "on the stage
     summary" is a Phase 2 design point for the ruling. No test asserts it.
2. **The advance's signature is pinned by four test callers.** `test_run_interrupts.py:87`,
   `test_entry_points_take_the_lock.py:94`, `test_arm_pinned_at_extract.py:103` and
   `test_run_pipeline_closes.py:52` replace it with "`lambda conn: {}`". A new argument at the
   call site would raise `TypeError` there, inside the caller's `except Exception: pass`, and would
   be **swallowed**. So the reason the advance names is the processing reason it can read
   (`effective_state`). The D23 refusal reason stays on `audit_run`'s line and report (T2, TV2).
3. **R687(c)'s T1 sequence, read literally, cannot hold.** The clause says "the stage test runs the
   audit stage and the stage stays pending because paper 2 is unaudited". An admitted paper with
   live unlocated claims is audited by the audit stage it runs under. As built:
   - 202 is extracted after the audit stage that audited 201.
   - The advance is then driven, and the stage stays pending because 202 is unaudited (not
     because it has nothing to audit).
   - `_stage_audit` then audits 202, and the stage completes.

   This keeps the ruling's fixture (paper 2 carries live unlocated claims) and both halves of T1.
   Reported, not a stop.
4. **The shared pytest temp root gained `pytest-19`** (created 2026-10-22 03:24–03:28 UTC, before
   this brief's first run at 03:44; its directories, e.g. `app_db_template0` and
   `test_0002_adds_notified_at_col0`, are not this suite's). `pytest-16` rotated out. No run here
   wrote to the root; all used `--basetemp` under scratch. A reading.
