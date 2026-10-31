# 12ab-U21B — Phase B reads: I2 false; the edit not made

## §0 Scope

- **HEAD read:** `a34cc7179090c8bbb7d5332e4728fb407a6e9606` (item 0, R758–R761 as Blocks 156–159). Engine tree equal to `bc2ae19`'s.
- **Method:** read-only. No engine or test edit. No model call.
- **Result:** I3 and I4 hold. **I2 is false**, so under the brief this stops here, before any edit, with the two placements that would work (§1).

## §1 I2, I3, I4

**I2 — false.** The brief: "if the re-check cannot be called from there without that import, STOP after Phase B and report the two placements that would work."

- **The digest check can be reached.** `ollama_client.py:690`: `from engine.core.run_manifest import check_call_digest, check_declared_call`; `:695`: `call_digest = check_call_digest(stage, model)`. It is a no-op outside a run (`run_manifest.py:1131–1133`). It needs `stage`, which `ollama_chat` has and `_restart_ollama_and_retry` does not (`:798–800`: `*, model, messages, paper_label, effective_timeout, max_retries, **kwargs,`).
- **The environment re-check can't, directly.** `recheck_ollama_env` is `ollama_preflight.py:239`; that module imports from the client at `:14`: `from engine.utils.ollama_client import ollama_chat, parse_environment_line, server_ps`. A call from the client needs exactly the import R734(e) names: "the re-check would import across the `ollama_preflight` → `ollama_client` direction at call time".
- **Its one other caller** is `extractor.py:730–731`, inside `_recheck_env_after_restart`: `from engine.utils import ollama_preflight as pf`; `reading = pf.recheck_ollama_env()`. It then writes telemetry (`record_run_event`) and raises `rm.ServerEnvChanged.at(...)`.

**The two placements that would work:**

| | P1: a run-manifest check beside the digest's | P2: move the reader down into the client |
|---|---|---|
| **Shape** | `run_manifest.check_call_env(stage, …)`, beside `check_call_digest`, with a local `from engine.utils import ollama_preflight` inside it. `ollama_client` calls it as it calls `check_call_digest` | `read_ollama_env`, `recheck_ollama_env`, `EnvReading`, `_drift` and the two env tables move to `ollama_client.py`; `ollama_preflight` imports them back |
| **The direction** | Crossed indirectly, at call time: client → run_manifest → preflight. It has a precedent: `run_manifest.py:1138`, `from engine.utils import ollama_client as oc`, inside `check_call_digest` | Not crossed |
| **Files** | `run_manifest.py`, `ollama_client.py`: inside the Touched list | Adds `engine/utils/ollama_preflight.py`: **outside the Touched list** (I5) |
| **Tests** | A no-op outside a run, so restart tests that fake `subprocess.run` at the client are not reached. Under a run the fence refuses `systemctl`, so those tests fake the reader | The env read's `subprocess.run` moves to the client's module, so `mock_subprocess.assert_called_once()` at `test_ollama_client.py:228` and kin count it, if a run is active; `pf.*` patch targets move |
| **Telemetry** | `_recheck_env_after_restart`'s `record_run_event` needs a review dir; the check has only the connection | The same |

**I recommend P1:** it stays in the Touched list and follows the existing form at `run_manifest.py:1138`. It does not remove R734(e)'s objection, only makes the crossing indirect, so it needs the ruling's word.

**I3 — holds.** `future.result` returns what the worker returned, so a second wait on the same future yields the response. The post-call sequence runs on it unchanged: `:782` `return _finish(response)`; `:675` `checked_fit = fit if call_digest is None else _check_loaded_context(model, call_digest, fit)`; `:677` `checked = _check_input_was_read(response, checked_fit, paper_label)`; `:684` `_record("completed", response=checked)`. `_record` (`:659–667`) takes `outcome_detail` from `exc` only (`:664`: `detail = None if exc is None else f"{type(exc).__name__}: {exc}"`), so a late completion needs a new way to pass a detail.

**I4 — holds.** `run_manifest.py`'s module-level imports are stdlib plus `engine.core.effective` and `engine.core.effective_config` (`:62–63`); its uses of `ollama_client` are local (`:506`, `:517`, `:1138`). `ollama_client` imports `run_manifest` locally only (`:416`, `:656`, `:663`, `:690`). A tuple beside `END_STATUSES` (`:76`) and the `REASON_*` strings (`:80–82`) needs no new import.

**The classes (for the placements).** `RunRefused(Exception)` (`run_manifest.py:113`), so `ModelDigestChanged` and `ServerEnvChanged` pass `except RuntimeError` at `ollama_client.py:744`. They would be swallowed by `except Exception as post_exc` at `:868` if raised inside that `try`.

## §2 The sites, as they stand

Unchanged from `12ab_U21A.md` §2, whose lines are still true at this HEAD: the attempts loop `:710`; the executor `:712`; the send `:714–719`; the watchdog `:720`; the shutdowns `:724`, `:752`, `:766`, `:869`; the conversion `:744–748`; the `run_calls` writes `:679`, `:682`, `:684`, `:701`, `:704`, `:791`; the restart `:840–845`; the post-restart send `:857–863`. The outcome tokens: `022_run_kinds_and_audit_tables.py:138–141`.

## §3 The census pre-step (2b)

`census_u21b.py` is the read-out's script, unchanged but for its first line, re-run at this HEAD; its output's counts equal the read-out's (`census_u21b.txt`). **The classification below is by reading.**

| Class | Tests | What each pins | Under R761(c) |
|---|---|---|---|
| **(c) hang fakes** | `test_ollama_client.py`: `test_watchdog_fires_on_hang`, `test_watchdog_retries_then_raises`, `test_timeout_logs_warning`, `test_restart_attempted_after_retries_exhausted`, `test_restart_failure_raises_original_timeout`, `test_restart_logs_warning`, `test_restart_ollama_and_retry_raises_on_post_restart_failure`; `test_restart_recovery_guard.py`: `test_refusal_surfaces_as_timeout_from_ollama_chat` | `time.sleep(60)` as a request that never returns | A fake that sleeps 60 s now holds the call for 60 s per attempt. Each fake raises `httpx.ReadTimeout` after a short sleep instead, the fake read timeout; type and message assertions are kept |
| **(d) pins the defect** | `test_watchdog_retries_then_raises`: `assert call_count == 4  # 1 initial + 2 retries + 1 post-restart` | The retried watchdog | **Cannot be kept as written:** R761(c)(i) sends one, then the post-restart, so 2. It is the behaviour the unit removes (D2 below) |
| **(c) timing** | `test_watchdog_fires_on_hang`: `assert elapsed < 20.0` with its comment | Elapsed wall time | Re-timed with its fake |
| **(p) kept** | `test_transient_failure_then_success` (retry on `ConnectionError`); `test_run_calls_outcomes.py` T1–T6, T8; `test_presented_context.py::test_T1_exhaustion_hash_equals_the_recorded_row`; `test_restart_success_then_call_succeeds`; `test_restart_ollama_and_retry_raises_on_restart_failure`; `test_restart_recovery_guard.py`'s gate tests; `test_qualgap01.py:103`; `test_lock_census.py` and `_lock_census.py:43` (the name kept); `test_run_fault_predicate.py:92`, `:102` (the type stays `TimeoutError`) | Non-timeout retries, one row per call, the gates, the type | Unchanged, given `stage` as an optional keyword |
| **(p) unreached** | The 136 whole-function replacements of `ollama_chat`; the 24 alias and remaining patch sites | The signature and return, which don't move | Unchanged |

**The test-side Touched list this yields:** `tests/test_ollama_client.py`, `tests/test_restart_recovery_guard.py`, and new acceptance files. Any other test the change reaches is class (c) at the gate (R725(b)).

**Limit.** The census sees names and strings, and a test coupled through a fixture's behaviour isn't seen. Its (iv) count is broad, as `12ab_U21A.md` F13 says.

## §4 The placements (2c), read, not made

| Item | Placement |
|---|---|
| **The wait after the watchdog** | At `:722`, in `except FuturesTimeoutError:`, replacing the comment at `:723` "Abandon the hung thread — do not wait for it" and the shutdown at `:724`. A second `future.result(timeout=…)` up to the read timeout, its outcome handled as if it had arrived at `:720` |
| **The late-completion record** | At `:684`, `_record("completed", response=checked)`, with a detail argument added to `_record` at `:659`, carrying the watchdog's lateness |
| **The re-checks before the post-restart send** | At `:854`, after the line reading `# One final attempt after restart`, and **outside the `try` at `:856`**, or `except Exception as post_exc` at `:868` swallows the refusal. Needs `stage` added to the signature at `:798–800` and to the call at `:737` |
| **The detail-string set** | `run_manifest.py`, after the `REASON_*` block following `END_STATUSES` at `:76`. Writers: opt-out `:823–827`; foreign lock `:828–833`; restart command `:846–849`; post-restart call `:868–873`; recovered `:749`'s `_finish` |
| **The finally for every executor** | One `try/finally` around `:712–782` per attempt, and around `:855–873`, replacing the shutdowns at `:724`, `:752`, `:766`, `:869` and covering `:782`'s and `:867`'s returns (N24) |
| **The restart reached from a read timeout** | At `:751`, the HTTP arm: `httpx.ReadTimeout` goes to the restart path that `:735–749` holds today; the rest of that arm keeps its retry |

## §5 What the placements surfaced for the ruling

**D1. The bound's far side.** R761(c)(i): "waits on the same future up to the HTTP read timeout". A worker can outlast that: finding 41 of 12ab-U21P-R3 measured one blocked past its handler's end (after a close, which this design doesn't do). If the wait expires with the worker alive, (i) forbids a send and (iii) requires the request to have ended. So the call must end with neither, which is a **sixth ending** beside R761(c)(iv)'s closed five.

**D2. `test_watchdog_retries_then_raises`' count.** R761(d) says "rewrites with assertions kept". `call_count == 4` is the behaviour removed; keeping it isn't possible. R690(b), "a test that pins the defect a unit fixes is rewritten to the corrected behaviour", is the form that fits.

**D3. Which timeouts route to the restart.** R761(c)(iii) names "the read timeout", and `max_retries` governs "an error other than a timeout". `:751` catches `httpx.TimeoutException`, which also holds `ConnectTimeout`, `WriteTimeout` and `PoolTimeout`. None of those has a request running on the server by the time it fires, unless it's a write. Whether they retry or restart isn't ruled.

**D4. The restart's trigger moves from three watchdogs to one read timeout,** so one silent hang costs at most 900 s plus 40 s plus the final attempt's 900 s. A `wall_timeout` caller (the preflight's 60 s, `pdf_quality_check`, the vision page) now waits up to 900 s before it ends, where it ended at its limit. That is what R761(c)(i) says; I state the cost because no caller's limit bounds it any more.

**D5. What a telemetry record the re-check owes.** The proactive path writes `ollama_env_after_restart` to run telemetry. R761 says "through the same functions", and the telemetry half needs the review directory, which `ollama_chat` lacks; `run_manifest._database_file(conn)` could give it. Whether this path writes that event isn't ruled.
