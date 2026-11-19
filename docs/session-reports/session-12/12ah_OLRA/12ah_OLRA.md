# 12ah-OLRA — Ollama restart paths: Phase A read-out (read-only, to scratch)

Run 2026-11-15 23:53 UTC → 2026-11-16, DGX `spark-59e4`, under the 12ah-OLRA brief as amended by
ruling 12ah-OLRA-R1 (interim, between sessions 12ag and 12ah). evidence-engine at
`35a914b2bdd75147af104e5abfaea388eae3a057` throughout (main, clean, 0/0 with origin);
claude-config at `7c74c2bea1fef8aec5ed457e199306be441f42ed`.
Every line quoted below is `path:line` at that HEAD. "CC's inference" marks a classification made
by reading, not by a command. No model call, no server request, no lock acquisition, no pytest run
(collection was not needed: every Q6 answer comes from the source).

Files beside this one: `brief_12ah_OLRA.md`, `brief_12ah_OLRA_R1.md`, `OLR-1_REPORT.md` (the copy),
`q2_grep_raw.txt`, `q2_entry_census.json`, `q2_restart_reach.txt`, `q6_test_grep.txt`,
`q7_text_grep.txt`, `q10_commits.txt`, `q10_7c74c2b_primer_diff.txt`, `work/` (comparison inputs).

---

## 0. Step 0 and the resume's re-read

- Step 0 (2026-11-15 23:53:33 UTC): evidence-engine `main` @ `35a914b`, clean, 0/0; claude-config
  `7c74c2b`, clean, 0/0. B1 false (no 12ah-OPEN); stopped before item 0. B1 withdrawn by R1-a.
- Resume (23:57:33 UTC): both unchanged; `/proc/locks` has no entry for the lock inode 2627318;
  `fuser -v ~/.ollama_experiment.lock` prints nothing (empty = no holder); no tmux server.
- Brief files, each hashed straight after its one write (R1-b(i)):
  `brief_12ah_OLRA.md` `9ef43e270e688eb441e50d9da32411bca0a737b7cca541a6c779e1d28900354d` (11,298 B);
  `brief_12ah_OLRA_R1.md` `ed24c8daa3edde164969edb967afe3fc817d1628979ec275c281cca23cad3f7d`.
- Session-start reads this resume: the config pull; `STANDING_RULES.md` entries R661(e), R703(j),
  R725(b); `ENGINE_REFACTOR_PLAN.md`'s 12ag closure paragraph, its "Expected values at session-12ah
  open" block and the R810 row. The global and project `primer.md` were not read beyond the
  claude-config `primer.md` lines Q10 names.

## E1–E9

| | verdict | what the file shows |
|---|---|---|
| E1 | holds | `~/scratch/ollama-OLR-1/REPORT.md` exists, 386 lines, sha256 `86e4db40…d782` |
| E2 | holds, all line numbers | `extractor.py:744` def; `:789-792` subprocess; `:1267-1268` cadence; `:61` `RESTART_EVERY_N = 25`; `:1000`, `:1107` defaults; `:1276-1280` catch-and-continue. **Addition:** `:1281-1285` the U19 re-check after a performed restart, which the handoff does not name |
| E3 | holds | `pass2_full.py:257`, `--restart-every` `default=0` at `:712`; `elicit01/runner.py:193`; `run_capture01.py:227`. **Difference:** the last two take `--restart-every` with `default=RESTART_EVERY_N` (`runner.py:148`, `run_capture01.py:184`), so their default is 25 too |
| E4 | holds | `ollama_client.py:878` def; `:931-936` subprocess; `:811` call inside `except httpx.ReadTimeout` (`:801`) |
| E5 | holds, with an order | both paths check the opt-out **first**, then the foreign lock (`extractor.py:760`, `:775`; `ollama_client.py:912`, `:918`) |
| E6 | holds | `extractor.py:747-748`; no measurement is cited at the site or in the introducing commit (§Q9) |
| E7 | holds | `review_spec.py:675-676` `keep_alive: Union[StrictInt, str] = Field(` / `default=-1,`; the live spec `review_specs/surgical_autonomy.yaml` sets no `keep_alive`, so −1 is `declared_default` |
| E8 | holds | `~/projects/inference-determinism/CONTAMINATED.md:36-38`, quoted in §Q9 |
| E9 | holds (by census, §Q2) | exactly two sites in engine/, scripts/, analysis/ issue a service restart; the suite already pins this (`tests/test_lock_census.py:186-192`) |

---

## Q1. The two paths as they stand

### Q1.1 `extractor.restart_ollama` (proactive)

```
engine/agents/extractor.py:61:   RESTART_EVERY_N = 25  # proactive Ollama restart interval (0 = disabled)
engine/agents/extractor.py:744:  def restart_ollama(reason: str = "proactive", papers_done: int = 0) -> bool:
engine/agents/extractor.py:747:      Uses ``sudo systemctl restart ollama`` to fully clear CUDA context
engine/agents/extractor.py:748:      fragmentation (a simple model unload/reload is not sufficient).
engine/agents/extractor.py:760:      if restart_disabled():
engine/agents/extractor.py:775:      if foreign_lock_held():
engine/agents/extractor.py:789:          subprocess.run(
engine/agents/extractor.py:790:              ["sudo", "systemctl", "restart", "ollama"],
engine/agents/extractor.py:791:              timeout=30, check=True, capture_output=True,
engine/agents/extractor.py:792:          )
engine/agents/extractor.py:793:      except Exception as exc:
engine/agents/extractor.py:794:          raise RuntimeError(f"Failed to restart Ollama: {exc}") from exc
engine/agents/extractor.py:797:      deadline = time.monotonic() + 60
engine/agents/extractor.py:800:              resp = httpx.get(f"{ollama_host()}/api/tags", timeout=5)
engine/agents/extractor.py:806:                  return True
engine/agents/extractor.py:811:      raise RuntimeError("Ollama did not become responsive within 60s after restart")
```

- **Command:** `["sudo", "systemctl", "restart", "ollama"]`. **No `-n`.** `timeout=30` (on expiry
  `subprocess.run` kills the child and raises `TimeoutExpired`). `check=True`. `capture_output=True`
  (stdout and stderr piped). **stdin is not set**, so it is inherited from the process.
- **Stand-down order:** (1) `restart_disabled()` → returns `False` (`:760-766`); (2)
  `foreign_lock_held()` → returns `False` (`:775-782`).
- **Endings:** `False` for either skip; `RuntimeError("Failed to restart Ollama: …")` for any
  exception from `subprocess.run` (non-zero exit, timeout, missing binary); `True` once `/api/tags`
  answers 200 within 60 s; `RuntimeError("Ollama did not become responsive within 60s…")` otherwise.
- **The opt-out predicate:** `engine/utils/ollama_client.py:44` `RESTART_OPT_OUT_ENV =
  "EVIDENCE_ENGINE_NO_OLLAMA_RESTART"`; `:49` `return os.environ.get(RESTART_OPT_OUT_ENV,
  "").strip().lower() not in ("", "0", "false", "no")`. Read at call time, not at import.
- **The caller's cadence and U19 re-check** (`_extract_selected`):

```
engine/agents/extractor.py:1267:        papers_since_restart += 1
engine/agents/extractor.py:1268:        if restart_every > 0 and papers_since_restart >= restart_every:
engine/agents/extractor.py:1271:                restarted = restart_ollama(
engine/agents/extractor.py:1275:                papers_since_restart = 0
engine/agents/extractor.py:1276:            except RuntimeError:
engine/agents/extractor.py:1281:            if restarted:
engine/agents/extractor.py:1285:                _recheck_env_after_restart(db, run_id, stats["extracted"] + stats["failed"])
```
  `_recheck_env_after_restart` (`:725-741`) writes one `ollama_env_after_restart` run event and
  raises `rm.ServerEnvChanged` on a correctness-tier drift (`:739-740`).

### Q1.2 `ollama_client._restart_ollama_and_retry` (read-timeout recovery)

```
engine/utils/ollama_client.py:801:            except httpx.ReadTimeout as exc:
engine/utils/ollama_client.py:811:                    response = _restart_ollama_and_retry(
engine/utils/ollama_client.py:820:                except RuntimeError as ended:
engine/utils/ollama_client.py:821:                    error = TimeoutError(
engine/utils/ollama_client.py:825:                    error.run_call_detail = getattr(ended, "detail", None)
engine/utils/ollama_client.py:826:                    raise error from ended
engine/utils/ollama_client.py:827:                return _finish(response, late_by=late[0] if late else None, restarted=True)
engine/utils/ollama_client.py:912:    if restart_disabled():
engine/utils/ollama_client.py:918:    if foreign_lock_held():
engine/utils/ollama_client.py:931:        subprocess.run(
engine/utils/ollama_client.py:932:            ["sudo", "systemctl", "restart", "ollama"],
engine/utils/ollama_client.py:933:            timeout=30,
engine/utils/ollama_client.py:934:            check=True,
engine/utils/ollama_client.py:935:            capture_output=True,
engine/utils/ollama_client.py:936:        )
engine/utils/ollama_client.py:944:    time.sleep(10)
engine/utils/ollama_client.py:947:    check_call_digest(stage, model)
engine/utils/ollama_client.py:948:    check_call_env(stage)
```

- **Command:** identical to Q1.1: no `-n`, `timeout=30`, `check=True`, `capture_output=True`, stdin
  inherited.
- **Stand-down order:** (1) opt-out → `_RestartEnded(…, "restart_refused_opt_out")` (`:912-917`);
  (2) foreign lock → `_RestartEnded(…, "restart_refused_foreign_lock")` (`:918-924`).
- **Endings** (`_RestartEnded` subclasses `RuntimeError`, `:591`): command exception →
  `restart_command_failed` (`:937-941`); after the 10 s sleep, `check_call_digest` may raise
  `ModelDigestChanged` and `check_call_env` may raise `ServerEnvChanged`. Both are `RunRefused`
  subclasses of `Exception` (`run_manifest.py:127`, `:202`, `:231`), **not** `RuntimeError`, so
  they pass `ollama_chat`'s `except RuntimeError` (`:820`) and reach the run as themselves. A worker
  alive past the bound on the final attempt → `worker_alive_at_read_timeout` (`:967-972`); any other
  final-attempt exception → `post_restart_call_failed` (`:973-978`); success → the response, recorded
  `completed` with `restart_recovered` (`:750`).
- **When it is reached:** on the **first** `httpx.ReadTimeout` of any attempt. A read timeout is
  never retried inside `ollama_chat`: the handler either returns the post-restart response or
  raises (`:811-827`). Other `httpx.TimeoutException` members and errors go to the retry branches
  (`:829-853`). `ollama` 0.6.1 does not wrap `ReadTimeout`: `.venv/…/ollama/_client.py:132-135`
  converts only `HTTPStatusError` and `ConnectError`.
- **The closed set of endings:** `engine/core/run_manifest.py:93-96` `RUN_CALL_ENDING_DETAILS = (
  DETAIL_RESTART_REFUSED_OPT_OUT, DETAIL_RESTART_REFUSED_LOCK, DETAIL_RESTART_COMMAND_FAILED,
  DETAIL_POST_RESTART_FAILED, DETAIL_RESTART_RECOVERED, DETAIL_WORKER_ALIVE, )`.

---

## Q2. Census of restart / stop / reload / unload sites

Pattern (git grep over tracked files in engine, scripts, analysis, tests and `*.sh`):
`systemctl|"ollama", *"stop"|ollama stop|keep_alive|restart_ollama|_restart_ollama_and_retry|pkill|killall|/api/generate|service ollama`
→ 274 lines (`q2_grep_raw.txt`). Separately, for keep_alive 0:
`keep_alive["']?\s*[:=]\s*["']?0\b|"keep_alive": *0|keep_alive=0` over engine, scripts, analysis,
tests and review_specs → **no such site found.** `ollama stop`, `pkill`, `killall`,
`/api/generate` → no such site found in engine/, scripts/ or analysis/.

| site | what it does | reached from (entry_census.py method, `q2_restart_reach.txt`) | status |
|---|---|---|---|
| `engine/agents/extractor.py:790` | `sudo systemctl restart ollama` | `scripts/run_pipeline.py` (extract stage, default 25), `analysis/eval/elicit01/runner.py` (default 25), `analysis/eval/run_capture01.py` (default 25), `analysis/paper1/pass2_full.py` (default 0). All four hold the lock | **live** |
| `engine/utils/ollama_client.py:932` | `sudo systemctl restart ollama` | every `ollama_chat` caller. 17 entry points: `analysis/eval/{elicit01/runner,run_capture01,run_local_ab,run_local_abc,run_qualgap01,smoke_regression01}.py`, `analysis/paper1/{judge_cli,judge_codebook_smoke,pass2_full,pass2_retry_single,pass2_smoke}.py`, `engine/acquisition/pdf_quality_check.py`, `engine/agents/ft_screener.py`, `engine/utils/ollama_preflight.py`, `scripts/{parse_expanded_corpus,run_pipeline,screen_expanded}.py`. All hold the lock | **live** (`run_qualgap01.py:92` sets the opt-out for itself) |
| `engine/utils/ollama_preflight.py:188` | `["systemctl", "show", "ollama", "--property=Environment"]` | the preflight and the U19 re-check | live, **read-only** (no restart, stop or unload) |
| `engine/core/review_spec.py:675-676` / `engine/core/effective_config.py:311-312` | keep_alive −1 sent on every Ollama call | every Ollama stage | live; **pins residency, never unloads** |
| `docs/session-reports/tools/launch_gate.sh:8,13,14,36,42` and the dated `…/host_snapshot.sh`, `12ag_GATEUNIT_A/reads/box_reads.sh`, `12g_ra/checkpoint.sh` | `systemctl show` / `--user status` / `is-active` / `list-*` / `cat` | the session tools | read-only; none restarts, stops or reloads |
| `scripts/nightly_tests.sh`, `scripts/watch_run4.sh` (crontab: `0 9 * * * …/scripts/nightly_tests.sh`) | — | cron | no restart, stop, unload or systemctl call (the grep finds none) |
| tests (`q6_test_grep.txt`) | patched or fenced calls | the suite | **test-only** (§Q6) |

The proactive column of `entry_census.py` gives exactly the four entry points E3 and the handoff
name. **E9 holds.** The 07:00 health check (`0 7 * * * /bin/bash
/home/ankitsarin/scripts/ollama_health_check.sh`) is dgx-infra's, not this repository's. OLR-1
records that it sends keep_alive 0 (`OLR-1_REPORT.md` §E4).

---

## Q3. Where the cadence comes from; manifest, pin and hash

| entry point | value source | quoted line |
|---|---|---|
| `scripts/run_pipeline.py` (extract stage) | the **argument default**, through two layers; no CLI flag | `run_pipeline.py:404` `stats = run_extraction(db, spec, review_name, selection=selection, run_id=run_id)` → `extractor.py:1000` `restart_every: int = RESTART_EVERY_N,` → `:61` `= 25`. Its parser has `--review --spec --skip-to --limit --max-papers` only (`run_pipeline.py:595-626`) |
| `analysis/eval/elicit01/runner.py` | CLI flag, default the constant | `:148` `ap.add_argument("--restart-every", type=int, default=RESTART_EVERY_N)` |
| `analysis/eval/run_capture01.py` | CLI flag, default the constant | `:184` `p.add_argument("--restart-every", type=int, default=RESTART_EVERY_N)` |
| `analysis/paper1/pass2_full.py` | CLI flag, default 0 | `:712` `p.add_argument("--restart-every", type=int, default=0,` |
| the recovery restart | no value; it is armed unless the environment says otherwise | `ollama_client.py:49` |

No spec field and no environment variable sets the cadence. The only environment input is the
opt-out, which disarms both paths.

**Manifest, pin, hash.** Searching for `restart` in `engine/core/run_manifest.py`,
`effective_config.py`, `review_spec.py` and migrations 020–022 finds only the ending vocabulary
(`run_manifest.py:84-96`) and `ServerEnvChanged`'s text (`:232-260`). In detail:
- **Body keys** (`run_manifest.py:634-656`): `run_uid, review_id, run_kind, git, engine_state,
  spec_hash, codebook, libraries, host, ollama_host, started_at, cloud_arms, payload_description,
  stages, pins`, plus optional `lineage_check, selection_bound, inputs, ollama_environment`. **No
  restart key.**
- **Pin tuple** (`run_manifest.py:448-463`): `arm_kind, provider, model, stages{model,
  model_digest, options_hash, format_schema_hash, prompt_hash}, codebook_hash, client_library`, plus
  `segmentation` for the unit-citing stage. **No restart member.**
- **prompt_hash** (`effective_config.py:471-477`): `messages`, `format`, and the parameters for a
  cloud arm. **No restart input.** `run_calls.request_hash` is computed over `{"model", "messages",
  **kwargs}` as sent (`ollama_client.py:713-717`); restart settings are not kwargs.
- **pass2_full alone records it**, in its own `judge_runs` config: `pass2_full.py:228`
  `"restart_every": restart_every,`.

**Plainly:** changing `RESTART_EVERY_N` from 25 to 0, or making the recovery restart default-off
through code, moves **no pin, no body key set and no prompt_hash or request_hash** (computed from
the lists above). It also does not change `spec_hash`. One caveat goes with that: if the switch
were a **new spec field**, `spec_hash` (`run_manifest.py:354` `sha256_canonical(spec.model_dump(
mode="json"))`) would change value on every run, because `model_dump` includes defaults. That is a
body value, not a key set or a pin. Whether anything compares `spec_hash` across runs was not
traced (CC's inference: nothing in the pin tuple does). `docs/inventory/entry_points.{json,md}`
record elicit01's and capture01's flag default as `dynamic: RESTART_EVERY_N` (`entry_points.md:71`,
`:74`), so a changed value does not drift the inventory. A new `--restart-every` flag on
`run_pipeline.py` would add a row there.

---

## Q4. A read timeout with no restart (the opt-out set)

**In `ollama_chat`.** The first `httpx.ReadTimeout` → `_restart_ollama_and_retry` raises
`_RestartEnded(restart_refused_opt_out)` at once (`:912-917`) → `TimeoutError("Ollama call timed
out after {attempt+1} attempts + restart …")` with `run_call_detail = "restart_refused_opt_out"`
(`:820-826`) → recorded by `:871` `_record("error", exc=exc, note=…)`. Under a run, that writes one
`run_calls` row with `outcome='error'` and `outcome_detail` beginning
`restart_refused_opt_out: TimeoutError: ` (pinned at `tests/test_u21_no_overlap.py:297-300`).
**No retry and no restart.** The `max_retries` budget (default 2, `ollama_client.py:127`) is never
used for a read timeout: its branch raises (`:811-826`). Retries apply only to the other timeout
members and errors (`:829-853`).

| caller | what it does with that `TimeoutError` | quoted line | run closes |
|---|---|---|---|
| extract loop | fails the **paper** `model_call_failed` → `extraction_failed`, counts toward the abort; no retry (the completeness driver retries only `RETRYABLE = (IncompleteExtractionError, UncitedValueError, ValidationError)`, `extractor.py:876`) | `extraction_events.py:535-537` `if isinstance(exc, (TimeoutError, httpx.HTTPError)) or … return fail(PS.REASON_MODEL_CALL_FAILED)`; `extractor.py:1220-1230` | `aborted` after 3 in a row (`extraction_events.py:138` `CONSECUTIVE_FAILURE_ABORT = 3`; `extractor.py:1183-1189` `RunAborted`; `run_pipeline.py:199-205`) |
| auditor (audit stage) | not caught: only `ValidationError` is (`audit_events.py:237`) → escapes the stage | `audit_events.py:233-239` | `failed` (`run_pipeline.py:213-216` `except Exception … "failed"`) |
| abstract screener | not caught: only `(json.JSONDecodeError, ValidationError)` (`screener.py:201`) → escapes the loop | `screener.py:193-201` | `failed` (run_pipeline) |
| FT screener / FT runner | not caught per paper (`ft_screener.py:376` catches parse errors only) → escapes | `ft_screener.py:374-376`; `:619-622` `except Exception as exc: … rm.close_run(db._conn, run_id, "failed", …)` | `failed` |
| preflight | caught; returns `status="error"` (`ollama_preflight.py:86-92`); `require_preflight` then raises `RuntimeError` (`:296`) | `ollama_preflight.py:69-74` (`max_retries=0`, `wall_timeout=60.0`) | `failed` (the stage raises before any paper) |
| pdf_quality_check | retried **once** by its own loop (`MAX_RETRIES = 1`, `pdf_quality_check.py:42`; `time.sleep(5)`, `:144`), then raised and caught per paper: `stats["failed"] += 1`, continue | `pdf_quality_check.py:103-146`, `:254-256` | continues; no consecutive bound |

**Bounds.** Consecutive read timeouts are bounded only in the **extract** loop (3 papers in a row →
`aborted`). A timeout on a screening, FT, audit or preflight call ends the run on the first
occurrence. `pdf_quality_check` has no bound. Non-consecutive extraction timeouts (with successes
between them, which reset the counter, `extractor.py:1257`) are unbounded.

**Worst-case wall time, from the constants.** `ollama_client.py:53-63` gives `connect=30.0`,
`read=900.0`, `write=30.0`, `pool=30.0`, and `:69` `_REQUEST_BOUND = pool + connect + write + read
= 990 s`. The watchdog (`MODEL_TIMEOUTS`, `:116-121`, e.g. `r"32b": 900.0`) only marks a call late
(`:613-619`).
- **Opt-out set (today's opt-out, and the PI's default-off):** a timed-out call ends by ~990 s, then
  `TimeoutError`, and the paper ends there. Worst case for an extraction paper ending in a timeout ≈
  the paper's earlier completed calls + ~990 s (completed calls have no upper bound of their own).
  Worst case for 3 consecutive timed-out papers before `aborted` ≈ 3 × 990 s ≈ 50 min. For
  pdf_quality_check, ≈ 2 × 990 + 5 s per paper.
- **Today, restart armed, no foreign lock:** up to 990 s + restart (≤30 s) + 10 s sleep + env
  re-check + final attempt (≤990 s), about 2,020 s per call (CC's sum from the constants above).

---

## Q5. A restart command that fails

| caller | non-zero exit (`CalledProcessError`) or any `subprocess.run` exception | quoted line | test that pins it |
|---|---|---|---|
| extract loop | `restart_ollama` raises `RuntimeError` → caught, logged, run continues. **`papers_since_restart` is not reset** (the reset at `:1275` follows the call inside the `try`), so every later paper tries again (CC's reading of `:1267-1280`) | `extractor.py:1276-1280` | `tests/test_extractor.py:504-515` (`test_restart_failure_continues_gracefully`), `:677-693` (`TestRestartOllamaGraceful`) |
| `pass2_full.py` | caught (`except Exception`), logged, continues | `pass2_full.py:256-259` | none found (`# pragma: no cover`, `:258`) |
| `elicit01/runner.py` | **not caught** → the `RuntimeError` leaves the loop and `hold_experiment_lock()` and ends the script with a traceback | `runner.py:191-193` (no `try`) | none found |
| `run_capture01.py` | **not caught**, same | `run_capture01.py:225-227` (no `try`) | none found |
| `_restart_ollama_and_retry` | `_RestartEnded(restart_command_failed)` → `TimeoutError`, recorded `error` / `restart_command_failed: TimeoutError: …` | `ollama_client.py:937-941` | `tests/test_u21_no_overlap.py:311-314`; `tests/test_ollama_client.py:307-318` (OSError → RuntimeError); `:262` (`test_restart_failure_raises_original_timeout`) |

**If sudo asked for a password** (from Q1's form: no `-n`, stdin inherited, `timeout=30`). sudo
reads the password from the controlling terminal, not stdin (CC's inference from sudo's documented
behaviour; not exercised):
- **No controlling tty** (a `systemd-run --user` unit, cron, the Claude Code Bash tool): sudo fails
  at once ("a terminal is required…", exit 1) → the non-zero-exit rows above. OLR-1 §6 point 1
  draws the same inference.
- **A controlling tty** (a run launched in tmux): sudo prints its prompt to the tty and **waits up
  to the 30 s `timeout`**, then `TimeoutExpired` → the same rows. If an operator typed the
  password in that window, or if that tty held a live sudo timestamp (`tty_tickets` scopes it per
  tty), the restart would **succeed** (CC's inference, not measured).
No test pins the password case. Every test patches `subprocess.run` or is fenced (§Q6).

---

## Q6. Test census (first pass, R725(b) classes)

Pattern over tests/: `restart_ollama|_restart_ollama_and_retry|RESTART_OPT_OUT_ENV|NO_OLLAMA_RESTART|restart_disabled|DETAIL_RESTART|DETAIL_POST_RESTART|DETAIL_WORKER_ALIVE|RUN_CALL_ENDING_DETAILS|_recheck_env_after_restart|ollama_env_after_restart|check_call_env|restart_every|RESTART_EVERY_N|ReadTimeout|systemctl`
→ 34 files (`q6_test_grep.txt`). **No test pins the value 25.** `RESTART_EVERY_N` appears in no
test. Tests that pass `restart_every=` all pass an explicit value (0, 1 or 3).
The coupling column is **CC's inference**, given against the PI's decision (proactive default 0;
recovery off unless enabled for the run).

| file | what it pins | coupled to default-off? |
|---|---|---|
| `tests/test_ollama_client.py` | `TestOllamaRestartRecovery` `:208-343`: `:211` `test_restart_attempted_after_retries_exhausted` asserts `restart_args == ["sudo", "systemctl", "restart", "ollama"]` (`:236`) after a `ReadTimeout`, without setting the opt-out; `:240`, `:262`, `:285`, `:308`, `:321`; `TestTimeoutLogging` `:148-160` | **(c) coupled**: the read-timeout→restart tests assume the restart is armed by default |
| `tests/test_restart_recovery_guard.py` | gates: `:78` foreign lock refuses; `:90`, `:104` proceeds; `:125` opt-out refuses; `:134`/`:140` truthy/falsy spellings; `:145` `test_unset_leaves_restart_armed`; `:153` refusal → `TimeoutError` | **(c) coupled**: `:145` and the "proceeds" tests pin armed-by-default |
| `tests/test_restart_ollama_guard.py` | proactive gates: `:59` foreign-lock skip; `:75`, `:88` proceed; `:142` opt-out before the lock; `:160` falsy values leave it armed; `:171` one skip per cadence; `:108`, `:125` the run holds the lock | **(c) coupled** for `:75`, `:88`, `:160` (armed unless opted out); the skip/lock tests are not |
| `tests/test_u21_no_overlap.py` | the fixture arms the restart (`:102` `monkeypatch.delenv(oc.RESTART_OPT_OUT_ENV, raising=False)`); `:208` reaches the restart once; `:227`, `:239`, `:254` post-restart checks; `:279-280` `RUN_CALL_ENDING_DETAILS == (` the six; `:297-328` each ending recorded; `:336` other timeout members keep their retry | **(c) coupled**: the restart-reaching tests; `:280`'s exact-set pin is coupled if a new ending (e.g. "restart not enabled") is added; `:336` is not coupled |
| `tests/test_u19_env_recheck.py` | `:106` the env check after every restart; `:115` drift → `ServerEnvChanged`; `:127` run_pipeline closes `aborted`; `:174` a skipped restart does no check; `:201`, `:211` telemetry | not coupled by value (they monkeypatch `restart_ollama`, `:88`); coupled only if the opt-in mechanism changes `restart_ollama`'s signature |
| `tests/test_extractor.py` | `TestProactiveRestart` `:461-515` (`restart_every=3` fires once; `=0` disables; failure continues); `:677-693` | not coupled (explicit values) |
| `tests/test_lock_census.py` / `tests/_lock_census.py` | `_lock_census.py:40-45` `SINKS` names both restart functions as "last-resort Ollama restart" / "proactive Ollama restart"; `test_lock_census.py:186-192` `test_only_the_two_restart_sites_restart_the_service` (exactly the two files, one site each); `:162-176` the scan; `:195-200` its negative control | not coupled to a default change; **coupled** if a site moves or a third appears |
| `tests/test_cut_over.py`, `test_citation_record.py`, `test_duplicate_on_pass1_attempt_2.py`, `test_extraction_run_link.py`, `test_floor_and_stored_attempt.py`, `test_parsed_text_resolver.py`, `test_run_fault_predicate.py`, `test_selection_refusals.py`, `test_text_refusal_at_selection.py`, `test_undeclared_call.py`, `test_units_as_source_slices.py`, `test_value_divergence.py`, `test_write_boundary_guard.py` | pass `restart_every=0` | not coupled |
| `tests/test_qualgap01.py` | the harness sets the opt-out | coupled only if the variable is renamed or inverted |
| `tests/conftest.py`, `tests/test_service_call_fence.py` | the fence (below) | not coupled |

**The fence, quoted.** `tests/conftest.py:64-67` `BLOCKED_COMMANDS = frozenset({ "systemctl",
"service", "sudo", "doas", "pkexec", "shutdown", "reboot", "halt", "poweroff", "init", "telinit",
})`. It matches on the basename of every argv word, skipping `VAR=value` words (`:104-113`). Both
list and shell-string forms are flattened (`:82-101`). The autouse `block_service_calls`
(`:147-185`) wraps `subprocess.run/Popen/call/check_call/check_output` and `os.system` (`:150-175`).
`_refuse` appends the message and `raise ServiceCallBlocked(message)` (`:130-131`), a
`BaseException` (`:70`). A teardown `pytest.fail`s any test that swallowed one, unless it is marked
`fence_selftest` (`:182-185`). `tests/test_service_call_fence.py` pins it with positive probes,
each `pytest.raises(ServiceCallBlocked)`: `:26-29` `subprocess.run(["sudo", "systemctl", "restart",
"ollama"], check=False)`; `:33-35` bare systemctl; `:39-42` `/usr/bin/systemctl`; `:46-48`
`shell=True` string; `:52-54` `os.system`; `:57-61` the other four entry points. Its negative
control is `:67-70` (`echo hello` still runs).

---

## Q7. Text that states the behaviour (`q7_text_grep.txt`; nothing edited)

- `CLAUDE.md:156-158` "Two independent code paths can run `sudo systemctl restart ollama`, and a /
  `NOPASSWD` sudoers rule for exactly that command means both really fire. Both are / now gated,
  and the suite is fenced. Do not add a third ungated path."
- `CLAUDE.md:173` "- `extractor.restart_ollama()` — proactive CUDA-defrag restart (`RESTART_EVERY_N`)."
- `CLAUDE.md:174-175` "- `ollama_client._restart_ollama_and_retry()` — last-resort recovery after a
  request / ends by the read timeout (OPSFIX-01; U21, R761(c)(iii))."
- `CLAUDE.md:176-180` "**Opt-out:** `EVIDENCE_ENGINE_NO_OLLAMA_RESTART` disarms both restart paths, …"
- `CLAUDE.md:359` "After a performed proactive restart, a correctness-tier drift in the re-read …"
- `CLAUDE.md:654-655` the `EVIDENCE_ENGINE_NO_OLLAMA_RESTART=1 PYTHONPATH=. python -m <harness>` recipe.
- `docs/architecture/models.md:62` "| 3. Restart recovery | `sudo systemctl restart ollama` + poll
  `/api/tags` (60s max) | Final attempt after all retries exhausted |". This is **stale**: since U21
  a read timeout goes to the restart on its first occurrence, and there is no `/api/tags` poll on
  this path (a 10 s sleep instead, `ollama_client.py:944`).
- `docs/architecture/models.md:64` "On final exhaustion, restart Ollama and make one last attempt." (stale, same reason)
- `docs/architecture/models.md:66` "**Proactive restart:** Extractor restarts Ollama every
  `RESTART_EVERY_N` papers (default 25, configurable via `--restart-every 0` to disable)". **No
  `--restart-every` exists on `run_pipeline.py`** (§Q3). The flag belonged to the retired
  `run5_extract_and_audit.py` (commit `f979763`, §Q9).
- `docs/architecture/modules.md:146` "… `MAX_RETRIES = 2`, `RETRY_DELAY = 30`, … `RESTART_EVERY_N = 25`";
  `:151` "`run_extraction(db, spec, review_name, restart_every)` — … proactive restart";
  `:375` "`ollama_chat(model, messages, ...)` — HTTP timeout + wall-clock timeout + restart recovery …";
  `:511` "| `test_ollama_client.py` | Timeout wrapper, retry, restart recovery |".
- `docs/architecture/pipeline.md:223` "8. **Proactive restart:** `restart_ollama()` every
  `RESTART_EVERY_N` papers (default 25). Polls `/api/tags` for up to 60s"; `:231` constants;
  `:430` "Proactive `restart_ollama()` failures are logged and the run continues …"; `:435` "Ollama
  timeouts raise after retry exhaustion (with restart recovery as final attempt)" (stale since U21).
- `engine/core/review_spec.py:678-680` (a spec-model description): "-1 equals the service value,
  so behaviour is unchanged." This becomes **untrue** once the host default moves to 30m (Section 1
  item 4). The value sent is unaffected; the sentence is what goes stale.
- In-code docstrings: `extractor.py:747-748` (E6); `ollama_lock.py:3-7` "the Evidence /
  Engine's own proactive `restart_ollama()` (RESTART_EVERY_N=25) killed the Arm P / rerun";
  `run_capture01.py:20`; `elicit01/runner.py:6-7`.
- README: no restart text (`git grep -i restart -- 'README*'`: no such site found). The live spec
  and the codebook: no such site found (pattern as above over `review_specs/`).

---

## Q8. Ways to say "restarts enabled for this run"

| option | exists today? | files it would touch | recorded with the run? |
|---|---|---|---|
| A. `restart_every` argument of `run_extraction` | yes (`extractor.py:1000`); not exposed by `run_pipeline.py` | `extractor.py:61` (constant to 0); a `--restart-every` flag in `run_pipeline.py` (next to `--max-papers`, `:619-626`); elicit01/capture01 defaults follow the constant | not today. Recorded only if passed to `open_run` as a body key the way `selection_bound` is (`run_pipeline.py:549-557`, `run_manifest.py:650-651`) |
| B. The existing variable read in the opposite sense (e.g. `EVIDENCE_ENGINE_OLLAMA_RESTART=1` to opt in) | the opt-out exists (`ollama_client.py:44-49`) | `ollama_client.py` (predicate), `extractor.py` (both paths read it), `run_qualgap01.py:92`, the tests in §Q6 that set or clear it, `CLAUDE.md:176-180,654-655` | **no**: process environment, not in the body |
| C. A spec field (e.g. under `ollama:` / `OllamaRuntime`, `review_spec.py:672-682`) | no | `review_spec.py`, `effective_config.py` (if the resolver carries it), both restart paths (they would need the spec, which `ollama_chat` does not have), the request-capture test | indirectly: `spec_hash` covers it (whole-spec hash), so it is in the body by value. **Adding the field changes `spec_hash` on every run**; it moves no pin (`pin_tuple` has no `spec_hash`) |
| D. A run-manifest key, declared at `open_run` and consulted through the active run | nearly: `active_run` already carries per-run state to `ollama_chat` (`check_call_digest` / `check_call_env` read `_ACTIVE` and `_RUN_DIGEST_FNS`, `run_manifest.py:1203-1204`) | `run_manifest.py` (key + accessor), `run_pipeline.py` / `ft_screener.py` (pass it), `ollama_client.py:912` and `extractor.py:760` (consult it), §Q6 tests | **yes**, by construction |
| E. pass2_full's precedent | yes, for pass2 only (`pass2_full.py:228`, in `judge_runs`) | — | yes, in its own table |

**CC's preference (labelled as CC's own).** D for the read-timeout path, with the constant at 0 and
A's flag feeding the same body key for the proactive path. That gives one run-scoped declaration,
recorded in the body like `selection_bound`. It leaves B's existing opt-out in place as the
stronger, process-wide "never". D alone does not reach any entry point that calls `ollama_chat`
outside an active run (finding 8), so for those "off unless enabled" has to be the code default,
not a run key.

---

## Q9. The record

- **OLR-1 copy.** `~/scratch/ollama-OLR-1/REPORT.md` →
  `~/scratch/12ah-olra/OLR-1_REPORT.md` (`cp -p`). sha256 before
  `86e4db4061f09a8988f231aa0dc819135013f634fddc426e645e64ff973dd782`, after the same, and the
  source re-hashed after the copy the same (G3).
- **E8, quoted** (`/home/ankitsarin/projects/inference-determinism/CONTAMINATED.md`, last changed at
  that repo's `f1ccc057…` 2026-08-30):
  ```
  CONTAMINATED.md:36: The rerun was launched to replace attempt 1 and was itself destroyed, this time
  CONTAMINATED.md:37: by the Evidence Engine's own proactive `restart_ollama()` (`RESTART_EVERY_N=25`)
  CONTAMINATED.md:38: issuing `systemctl restart ollama` while the arm was in flight. The run record
  ```
- **E6's commit.** `git log -S"a simple model unload/reload is not sufficient"` (and `-S"unload/reload"`):
  one commit, `ed9edee76e095276ef3aeddf0c86dd14ea0f9e9e`, 2026-03-17T15:04:58+00:00, "feat:
  pre-flight env assertions, restart_ollama timestamp/paper-count logging". Its message cites no
  measurement for the sentence. The diff changes the comment from "clear memory fragmentation" to
  "clear CUDA context fragmentation". `restart_ollama` itself came in
  `f979763f9d82ba173e1e0503723e71020f93e9c6` (2026-03-16, "feat: Sonnet arm fixes, proactive Ollama
  restart, …"), whose message says "Proactive restart every N papers (default 25) to clear memory
  fragmentation" and "--restart-every CLI flag in run5_extract_and_audit.py (0 disables)". It also
  cites no measurement.
- **Fields that would show per-call slowdown over a long run** (schema read only):
  - `run_calls` (migration 022, `engine/migrations/022_run_kinds_and_audit_tables.py:325-337`):
    `stage`, `paper_id`, `started_at TEXT NOT NULL`, `ended_at TEXT NOT NULL`, `outcome`,
    `outcome_detail`. `ended_at − started_at` per call, ordered by `call_id` within a run and
    stage, is the only per-call wall time on record. `outcome_detail` also carries
    `late_completion: …s past the watchdog limit of …s` (`run_manifest.py:98`).
  - Extraction telemetry (`data/<review>/telemetry/extraction_calls.jsonl`,
    `engine/core/extraction_telemetry.py:29-31`, schema `extraction-telemetry-4`): `ts`, `paper_id`,
    `attempt`, `pass1_prompt_eval_count`, `pass2_prompt_eval_count`, `pass1_eval_count`,
    `pass2_eval_count` (`:66-69`). Token counts, no durations.
  - Run telemetry (`run_events.jsonl`, `engine/core/run_telemetry.py:24-26`): the
    `ollama_env_after_restart` event per performed restart.
  - **Not recorded anywhere in the engine:** Ollama's `total_duration`, `load_duration`,
    `prompt_eval_duration`, `eval_duration` (`git grep eval_duration|total_duration|load_duration` over
    engine, scripts, analysis/eval finds only `analysis/eval/score_screen2f.py:489`, an eval script).
    So tokens-per-second per call is not derivable from the engine's records; wall time per call is.

---

## Q10. claude-config since the 12ag wrap commit

X1 holds: `a2bbd81ffed41ccd6c6702dcdba2aa148724f0ca` exists, is an ancestor of HEAD, 2026-11-14T22:07:58Z,
"primer: the evidence-engine lane line for session 12ag (R811(f))". Commits after it, oldest first
(`q10_commits.txt`):

| full hash | date (UTC) | subject | files |
|---|---|---|---|
| `879d361a98366e93da06d78493a5cd238f1ea9d4` | 2026-11-14T22:54:39 | primer: brand rollout note, lists done (lists brand-rollout close) | primer.md (2 +-) |
| `ba6a6924cc075e1a2bc04af29943c96daa97a781` | 2026-11-15T01:11:48 | primer: brand rollout note, llm-council done (llm-council brand-rollout close) | primer.md (2 +-) |
| `ae0dfe4fa56f097f315914a13aae7cc4d16e6469` | 2026-11-15T01:11:48 | ledger: rows through 2026-11-15T00:26:37Z | PROJECT_LEDGER.md (+1) |
| `1afb2c88b8501ce2e6351f7aafbbfd5d53e4df53` | 2026-11-15T19:37:08 | primer: restore Brand rollout rules dropped in ba6a692 | primer.md (2 +-) |
| `67157c0fcbb4aef206ad9cd8b74899ee59bf63b9` | 2026-11-15T21:25:14 | ledger: rows through 2026-11-15T20:17:30Z | PROJECT_LEDGER.md (+3) |
| `16498c236828ceb2d2be8b7cb8f4f48f2f76de81` | 2026-11-15T23:01:47 | ledger: rows through 2026-11-15T22:20:57Z | PROJECT_LEDGER.md (+1) |
| `a5fd9e18945ac7fb2e65910aa93f27d3ef787675` | 2026-11-15T23:01:47 | primer: llm-council hardening done (LCH-1); 7861 binds 127.0.0.1; cross-lane Ollama item | primer.md (4 ++--) |
| `7c74c2bea1fef8aec5ed457e199306be441f42ed` | 2026-11-15T23:46:33 | primer: Ollama cross-lane policy (PI rulings 2026-11-15, OLR-1) | primer.md (+4) |

- **primer.md lines 41–45: byte-equal** at `a2bbd81` and HEAD (`cmp` silent). Both extracts
  sha256 `88b9c696529424ce464f6adeca5c349c104d48b8202e58d93053c156383bcfd9`. They are this lane's
  12ag line ("(updated 2026-11-14, session 12ag closed, session `acb83b21`): HEAD `35a914b…`").
- **Ledger rows of this lane:** `grep "evidence-engine"` over `PROJECT_LEDGER.md` → 673 rows at
  `a2bbd81`, 673 at HEAD, and 0 rows of the former missing from the latter (`grep -vxF`). All are
  present.
- **The text `7c74c2b` added to primer.md, whole** (a new §18 after §17, hunk `@@ -2484,3 +2484,7 @@`):

> 
> ## 18. Ollama cross-lane policy
> 
> Ollama cross-lane policy (PI rulings 2026-11-15, after read-out OLR-1, ~/scratch/ollama-OLR-1/REPORT.md). No restart of Ollama since 2026-08-31; the live cost is model eviction, because there is one model slot. Decisions: (1) host OLLAMA_KEEP_ALIVE changes from -1 to 30m, with RestartSec=10 added in the override, applied in one quiet-window Ollama restart as a PI walk (pending); the same change must update dgx-infra scripts/morning_digest.sh:118 ([OLLAMA_KEEP_ALIVE]=-1) and this primer's pinned Ollama Environment line. (2) No standing per-lane keep_alive rule; evidence-engine's pinned -1 is its own decision. (3) evidence-engine's proactive (RESTART_EVERY_N) and read-timeout Ollama restarts become opt-in, default off (handed to the Evidence Engine project; pending). (4) /etc/sudoers.d/ollama-restart is removed after (3) lands (pending, PI walk). (5) The 07:00 ollama_health_check becomes a daily light check of the lanes' models plus the resident model, with the full sweep of all models weekly on Sunday (pending, dgx-infra). (6) OLLAMA_MAX_LOADED_MODELS stays 1. (7) earlyoom keeps --prefer '^ollama$'.

  (The full diff with its commit header is `q10_7c74c2b_primer_diff.txt`.)

---

## Findings outside the questions

1. **Default-off makes a read timeout a single-attempt failure.** Computed from
   `ollama_client.py:801-827`. Section 1 item 2 says a timeout should fail "through the existing
   error path and the run's normal retry or skip logic". Today no layer retries a read timeout:
   `ollama_chat` sends it only to the restart branch, the extract completeness driver does not
   retry `TimeoutError`, and the screeners, FT runner, auditor and preflight end the run on it.
   Only pdf_quality_check retries (once). Whether a default-off read timeout should regain one or
   more plain retries is a design question the ruling has to answer. Today's code gives it none.
2. **A failed proactive restart is retried on every later paper.** CC's reading of
   `extractor.py:1267-1280`: `papers_since_restart = 0` sits inside the `try` after the call, so a
   `RuntimeError` leaves the counter at or above `restart_every`. Once sudoers is removed, an
   opted-in run would issue one failing sudo per paper, each up to 30 s under a tty.
3. **elicit01 and capture01 crash on a failed restart, and neither runs U19's re-check.** Computed
   from `runner.py:191-193` and `run_capture01.py:225-227`: no `try` and no
   `_recheck_env_after_restart`. Both default to 25 through `RESTART_EVERY_N`, so a change to the
   constant changes their default too.
4. **sudo without `-n` can wait 30 s, or succeed, under a tty.** CC's inference, §Q5. After the
   sudoers file is removed, an opted-in restart from a tmux pane would prompt for up to 30 s. If
   the pane held a live sudo timestamp, the restart would succeed without a prompt. `-n` would make
   both paths fail at once in every context.
5. **`ConnectError` never reaches `ollama_chat`'s `httpx.ConnectError` arm.** Computed: ollama 0.6.1
   converts it to the builtin `ConnectionError` (`_client.py:134-135`), which falls to the generic
   `except Exception` retry (`ollama_client.py:842-853`). The extract loop treats it as a run fault
   (`extraction_events.py:437-441` says so). This is harmless, and the tuple at
   `ollama_client.py:829` is partly dead.
6. **Stale or misleading text.** Computed, §Q7: `docs/architecture/models.md:62-66` and
   `pipeline.md:435` describe a pre-U21 "after all retries exhausted" recovery and a `--restart-every`
   flag that `run_pipeline.py` does not have. `tests/test_ollama_client.py:211`'s name
   (`…_after_retries_exhausted`) describes the same retired behaviour. `extractor.py:1011-1013`
   ("The acquire is blocking: waiting for another experiment to finish is safe") contradicts
   `CLAUDE.md:165` ("No acquisition waits") and the U12 lock. `review_spec.py:680` ("-1 equals the
   service value") goes stale with the host keep_alive change.
7. **`extractor.MAX_RETRIES = 2` and `RETRY_DELAY = 30` are still referenced nowhere.** Computed:
   `extractor.py:58-59`; grep finds no use. WRITE-PATH-01's Phase 1a read-out recorded the same fact
   (`docs/session-reports/write-path-01/WRITE-PATH-01_phase1a_readout_20260924.md:261`), and
   `docs/architecture/{modules,pipeline}.md` still list them.
8. **Most recovery-reaching entry points open no run in their own file.** Computed as a location
   only: `grep -cE "open_run\(|active_run\(|_open_run_manifest\("` over the 17 entry-point files
   hits only `engine/agents/ft_screener.py` (1) and `scripts/run_pipeline.py` (3). Whether the
   other 15 reach a run through imported functions was not traced (a grep hit is a location, not a
   semantics). If they do not, a run-scoped opt-in (Q8 D) leaves them on the code default, which
   is a reason the default itself must be off (CC's inference).

## Gates

- **G1** (as amended): evidence-engine HEAD `35a914b2…` = step-0 HEAD; `git status --porcelain`
  empty (no untracked file); 0/0 with origin. Read at the STOP; see the final report.
- **G2:** every Q1–Q10 answer above carries a quoted `path:line` or "no such site found" with its pattern.
- **G3:** `86e4db40…d782` source = copy (§Q9).
- **G4: PASS for this task, with a host event inside the window.** `journalctl -u ollama --utc
  --since "2026-11-15 23:53:00" --until "2026-11-16 00:07:34"` → 28 lines (`g4_ollama_journal.txt`).
  No `POST`. Under `session_open_check.py:119`'s pattern there are 4 hits, all `starting runner`
  lines at 00:06:51–52, which are the GPU-discovery runners of a **service restart that this task
  did not issue**. The sudo journal (`journalctl _COMM=sudo`, same window) attributes it to an
  operator on `TTY=pts/12`, PWD `/home/ankitsarin`, applying the PI's decision (1):
  `00:05:33 cp -p …/ollama.service.d/override.conf …override.conf.bak-20261115`;
  `00:05:47 sed -i s/^Environment="OLLAMA_KEEP_ALIVE=-1"$/Environment="OLLAMA_KEEP_ALIVE=30m"/ …`;
  `00:06:00 tee -a …/override.conf`; `00:06:28 systemctl daemon-reload`;
  `00:06:51 systemctl restart ollama`; `00:07:30 etckeeper commit 'ollama override: KEEP_ALIVE 30m,
  RestartSec 10 (PI 2026-11-15, OLR-1 decision 1)'`. After it: `MainPID=3633749`,
  `ExecMainStartTimestamp=Mon 2026-11-16 00:06:51 UTC`, `NRestarts=0`, and the unit's environment
  carries `OLLAMA_KEEP_ALIVE=30m`. The remaining lines are `GET /api/version|ps|tags` triplets from
  127.0.0.1 at 23:53:08–09, 00:04:27 and 00:07:16. None is this task's: no command this task ran
  makes an HTTP request (its commands are git, grep, awk, sed -n, cp, sha256sum, journalctl,
  systemctl show and one AST script). Consequence for Section 1 item 4: the host default **has now
  moved** to 30m, so `review_spec.py:680`'s "-1 equals the service value" is untrue as of 00:06:51
  UTC (§Q7, finding 6). The engine's own −1 per request is unaffected.
- **G5** (as amended): claude-config at `7c74c2b` or a child carrying only this session's hook row;
  read at the STOP.
