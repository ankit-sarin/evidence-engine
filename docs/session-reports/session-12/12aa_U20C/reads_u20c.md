# 12aa-U20C — the pre-edit reads (R744(e), brief 2a–2b, A2)

Read at HEAD `e3d303d71a8ace32721726552048ec14748609aa`, before any edit. Quoted with file and line as found. The stop report held these in scratch; R745(h) resumed at 2b.

**I3** true: `_finish` runs the post-call checks, `fit["ceiling"]` is the ceiling the call was checked against, `check_call_digest`'s non-None result is the active-run test (R745(e)).
**I4** true: `ollama.Client.ps()` returns `ProcessResponse`, each model with `name`, `digest`, `context_length`; committed body `12aa_U20P/load/P3_ps.json`, quoted below.
**I5** false in its close-path clause: `ServerEnvChanged` appears nowhere in `ft_screener.py`, and `run_pipeline`'s clause is in `scripts/` (R745(a)).
**I6** true: the writer is `_check_input_was_read`; the exact key-set test is `tests/test_ollama_input_fit.py:218`, outside a run.

## The fixed engine list (A1)

```text
The earlier list (2a, 12aa-U20C), quoted:
  > The fixed engine list by the brief's rule, not acted on (the brief stopped at I5):
  > engine/utils/ollama_client.py
  > engine/core/run_manifest.py   (aa76124 defined ServerEnvChanged here)
  > CLAUDE.md                     (aa76124 named ServerEnvChanged there; one line)
  > docs/inventory/               (if not in sync)
  > Outside the list, and needed by R744(c) as written:
  > scripts/run_pipeline.py       (aa76124 added the close path here; scripts/ is DO NOT TOUCH)
  > engine/agents/ft_screener.py  (aa76124 changed it for the open only; it has no ServerEnvChanged close path)
  > Not changed by aa76124 for the class: engine/core/extraction_events.py (RunRefused subclasses join the tuple)

The fixed engine list under R745 / A1 (written before any edit):
engine/utils/ollama_client.py
engine/core/run_manifest.py
engine/utils/ollama_preflight.py
engine/agents/ft_screener.py
scripts/run_pipeline.py
CLAUDE.md (the line naming ServerEnvChanged; one line naming the tuple's new member)
docs/inventory/ (if not in sync)
```

## The quotes

```text

--- engine/utils/ollama_client.py:650-664
650:     def _finish(response):
651:         """Post-call: the only place a checked response and its raw response
652:         both exist, so this is where 'completed', 'refused_input_truncated'
653:         and 'refused_input_dropped' are recorded (R216) — one site, not
654:         three, because they share this one calling point."""
655:         try:
656:             checked = _check_input_was_read(response, fit, paper_label)
657:         except InputTruncated as exc:
658:             _record("refused_input_truncated", response=response, exc=exc)
659:             raise
660:         except InputDropped as exc:
661:             _record("refused_input_dropped", response=response, exc=exc)
662:             raise
663:         _record("completed", response=checked)
664:         return (checked, req_hash) if return_request_hash else checked

--- engine/utils/ollama_client.py:666-685
666:     # C54: under an active run, a call the manifest did not declare is refused
667:     # here — before the input-fit read and the send, so nothing reaches the
668:     # server, and unrecorded, because nothing was sent. A no-op outside a run.
669:     from engine.core.run_manifest import check_call_digest, check_declared_call
670:     check_declared_call(stage, model)
671:     # U18 commit 2 (R721(b)): the model's digest now must be the one the manifest
672:     # recorded for this stage, or ModelDigestChanged — before the input-fit read
673:     # and the send, unrecorded like C54's refusal. A no-op outside a run.
674:     digest_token = _CALL_DIGEST.set(check_call_digest(stage, model))
675: 
676:     try:
677:         fit = _check_input_fits(model, messages, kwargs.get("options"), paper_label)
678:     except InputOverflow as exc:
679:         _record("refused_input_overflow", exc=exc)
680:         raise
681:     except CeilingUnavailable as exc:
682:         _record("refused_ceiling_unavailable", exc=exc)
683:         raise
684:     finally:
685:         _CALL_DIGEST.reset(digest_token)

--- engine/utils/ollama_client.py:761-770
761:     except (InputTruncated, InputDropped):
762:         raise  # already recorded inside _finish — never double-recorded
763:     except Exception as exc:
764:         # Every OTHER way this call can end after the request hash exists:
765:         # the wall-clock watchdog exhausted with a failed restart (TimeoutError),
766:         # an HTTP-level timeout/connect failure exhausted, or any other
767:         # exception from the client exhausted. One site, because none of these
768:         # carry a response object to record against — 10a-C3 Phase A A3.
769:         _record("error", exc=exc)
770:         raise

--- engine/utils/ollama_client.py:535-540
535: def server_ps():
536:     """The server's resident models (`/api/ps`), through the engine client (U18).
537: 
538:     Not `ollama.ps()`: that is the library's own module client, bound at
539:     `import ollama` and not moved by anything the engine does."""
540:     return _client.ps()

--- engine/utils/ollama_client.py:412-424
412: def _check_input_was_read(response, fit: dict, paper_label: str):
413:     """Post-call: the count must be under the ceiling and above the drop floor."""
414:     count = getattr(response, "prompt_eval_count", None)
415:     if isinstance(count, bool) or not isinstance(count, int):
416:         count = None
417:     done_reason = getattr(response, "done_reason", None)
418:     chars, ceiling, model = fit["chars"], fit["ceiling"], fit["model"]
419:     telemetry = {
420:         **fit,
421:         "count": count,
422:         "ratio": round(count / chars, 4) if count is not None and chars else None,
423:         "done_reason": done_reason if isinstance(done_reason, str) else None,
424:     }

--- engine/core/run_manifest.py:113-114
113: class RunRefused(Exception):
114:     """A run that may not open. Raised before anything is written."""

--- engine/core/run_manifest.py:217-233
217: class ServerEnvChanged(RunRefused):
218:     """U19, R734(c)(ii): after a performed proactive restart, the Ollama service's
219:     configured environment fails the correctness tier — a drop-in edited during the run
220:     and applied by the run's own restart, under which every later paper would be
221:     extracted. Raised by the extract loop before the next paper; the runner closes the
222:     run 'aborted' with this message, which starts with the class name.
223: 
224:     Constructed from a message, as every `RunRefused` is; `at` composes the message from
225:     the drifted keys and keeps them as `drift` ({key: (expected, actual or None)})."""
226: 
227:     def __init__(self, message: str, *, drift: Mapping[str, tuple[str, str | None]] | None = None,
228:                  papers_done: int | None = None):
229:         super().__init__(message)
230:         self.drift = dict(drift or {})
231:         self.papers_done = papers_done
232: 
233:     @classmethod

--- engine/core/run_manifest.py:1088-1103
1088: def check_call_digest(stage: str | None, model: str) -> str | None:
1089:     """U18 commit 2, R721(b): under an active run, read `model`'s current digest
1090:     through the run's own `digest_fn` and refuse the call if it is not the digest the
1091:     manifest recorded for the call's stage key. Called by `ollama_chat` after
1092:     `check_declared_call` and before the input-fit read and the send. Returns the
1093:     digest read, or `None` when no check applies — outside a run, for a call that
1094:     names no stage, or for a run `open_run` did not register.
1095: 
1096:     A digest that cannot be read is refused the same way, except a refused or
1097:     timed-out connection, which is re-raised as itself: it stays the run fault it
1098:     was before this check (R532)."""
1099:     active = _ACTIVE.get()
1100:     if active is None or stage is None:
1101:         return None
1102:     conn, run_id = active
1103:     entry = (_database_file(conn), run_id)

--- engine/core/extraction_events.py:464-475
464:         from engine.core.run_manifest import RunRefused, UndeclaredCall
465:         from engine.elicitation.classes import CodebookContractError
466:         from engine.elicitation.materialize import SnippetNotExact
467:         from engine.utils.ollama_client import CeilingUnavailable
468:         _RUN_FAULT_CLASSES = (
469:             sqlite3.Error, CeilingUnavailable,
470:             ConnectionError, httpx.ConnectError, httpx.ConnectTimeout,
471:             UndeclaredCall, UndeclaredOverride, RunRefused,
472:             RunAborted, EventRefused, CodebookContractError, ExhaustedWithoutRecord,
473:             SnippetNotExact,
474:         )
475:     return _RUN_FAULT_CLASSES

--- tests/test_run_fault_predicate.py:73-84
73: RUN_REFUSED_MEMBERS = {
74:     rm.DirtyTree, rm.AmbiguousEngineState, rm.PreManifestArm, rm.RetiredArm,
75:     rm.ArmPinMismatch, rm.CloudArmNotEnabled, rm.StageNotInRun,
76:     rm.CodebookReviewMismatch,
77:     rm.ModelLineageMissing, rm.SameLineageVerifier,  # U17, R703(d)/(e)
78:     rm.OllamaHostNotLoopback,  # U18, R721(c)
79:     rm.OllamaHostNotLocalService,  # U19, R734(d)(i)
80:     rm.ModelDigestChanged,  # U18, R721(b)
81:     rm.ServerEnvChanged,  # U19, R734(c)(ii)
82:     ElicitationSentinelUnsatisfiable,
83: }
84: FAULTS = [pytest.param(make, id=cls.__name__) for cls, make in RUN_FAULTS.items()] + [

--- tests/test_run_fault_predicate.py:139-144
139:     for cls in RUN_REFUSED_MEMBERS:
140:         assert issubclass(cls, rm.RunRefused)
141:     # no RunRefused member exists that this file does not exercise
142:     def subclasses(c):
143:         return {s for d in c.__subclasses__() for s in {d} | subclasses(d)}
144:     assert subclasses(rm.RunRefused) == RUN_REFUSED_MEMBERS

--- scripts/run_pipeline.py:200-222
200:         # 9b-FLIP: the consecutive-failure abort. Every aborted paper's event is
201:         # written; the manifest records end_status 'aborted' with the reason
202:         # (10a-C3, R215/C26 — the manifest carries it now, not just the log).
203:         logger.error("RUN ABORTED: %s", exc)
204:         _finish_review_run(db, run_id, "aborted", reason=str(exc))
205:         raise
206:     except rm.ModelDigestChanged as exc:
207:         # U18 commit 2 (R721(b), R727(b)): a model replaced under its tag mid-run.
208:         # Nothing was sent for the refused call; the run closes 'aborted' with the
209:         # refusal's message, which names the model, the stage and both digests.
210:         logger.error("RUN ABORTED: %s", exc)
211:         _finish_review_run(db, run_id, "aborted", reason=str(exc))
212:         raise
213:     except rm.ServerEnvChanged as exc:
214:         # U19 (R734(c)(ii)): the configured environment failed the correctness tier on
215:         # the read after a performed proactive restart. No paper was extracted under it;
216:         # the run closes 'aborted' with the refusal's message, which names each key.
217:         logger.error("RUN ABORTED: %s", exc)
218:         _finish_review_run(db, run_id, "aborted", reason=str(exc))
219:         raise
220:     except Exception as exc:
221:         logger.error("Pipeline failed: %s", exc, exc_info=True)
222:         _finish_review_run(db, run_id, "failed", reason=rm.failure_reason(exc))

--- engine/agents/ft_screener.py:602-621
602:     token = rm.activate(db._conn, run_id)
603:     try:
604:         if verify_only:
605:             run_ft_verification(db, spec, review_name=review_name, run_id=run_id)
606:         elif screen_only:
607:             run_ft_screening(db, spec, review_name=review_name, run_id=run_id)
608:         else:
609:             run_ft_screening(db, spec, review_name=review_name, run_id=run_id)
610:             run_ft_verification(db, spec, review_name=review_name, run_id=run_id)
611:         rm.close_run(db._conn, run_id, "completed")
612:     except rm.ModelDigestChanged as exc:
613:         # U18 commit 2 (R721(b), R727(b)): as run_pipeline's — the run closes
614:         # 'aborted' with the refusal's message; nothing was sent for that call.
615:         logger.error("FT screening run %d aborted: %s", run_id, exc)
616:         rm.close_run(db._conn, run_id, "aborted", reason=str(exc))
617:         raise
618:     except Exception as exc:
619:         logger.error("FT screening run %d failed", run_id, exc_info=True)
620:         rm.close_run(db._conn, run_id, "failed", reason=rm.failure_reason(exc))
621:         raise

--- engine/agents/extractor.py:725-739
725: 
726:     Every read, pass or drift, is one run-telemetry event carrying the raw string and
727:     the verdict; a correctness-tier drift then raises `ServerEnvChanged`, a run fault
728:     that S9's `except RuntimeError` does not catch.
729:     """
730:     from engine.utils import ollama_preflight as pf
731:     reading = pf.recheck_ollama_env()
732:     record_run_event(Path(db.db_path).parent, run_id=run_id, kind="ollama_env_after_restart",
733:                      payload={"environment": reading.raw, "papers_done": papers_done,
734:                               "verdict": "drift" if reading.correctness else "ok",
735:                               "correctness": reading.correctness,
736:                               "performance": reading.performance})
737:     if reading.correctness:
738:         raise rm.ServerEnvChanged.at(reading.correctness, papers_done=papers_done)
739:     logger.info("Ollama environment OK after restart (%d papers done)", papers_done)

--- engine/utils/ollama_preflight.py:58-90
58:     A generation call (R63): its options come from the resolver's `preflight`
59:     stage — the spec's `preflight` block, or the spec model's declared defaults.
60:     Any failure is returned as an `error` result, except `UndeclaredCall`
61:     (under an active run that did not declare this model's probe), which is
62:     re-raised.
63:     """
64:     from engine.core.effective_config import stage_config
65:     from engine.core.run_manifest import UndeclaredCall
66:     cfg = stage_config("preflight", spec, model=model_name)
67:     start = time.time()
68:     try:
69:         ollama_chat(
70:             messages=[dict(m) for m in PREFLIGHT_MESSAGES],
71:             max_retries=0,
72:             wall_timeout=60.0,
73:             **cfg.kwargs(),
74:         )
75:         elapsed = time.time() - start
76:         vram = _get_model_vram_gb(model_name)
77:         return ModelResult(
78:             model=model_name, status="ok",
79:             load_time_seconds=round(elapsed, 1), vram_used_gb=round(vram, 1),
80:         )
81:     except UndeclaredCall:
82:         # C55: a probe the run's manifest did not declare is a run fault, not a
83:         # model that failed to load — it reaches the run as itself.
84:         raise
85:     except Exception as exc:
86:         elapsed = time.time() - start
87:         return ModelResult(
88:             model=model_name, status="error",
89:             load_time_seconds=round(elapsed, 1),
90:             error_message=str(exc),

--- tests/test_ollama_input_fit.py:211-219
211: def test_every_call_logs_one_structured_input_fit_line(monkeypatch, caplog):
212:     _use(monkeypatch, count=630)
213:     with caplog.at_level("INFO", logger="engine.utils.ollama_client"):
214:         _chat(_user(3000), paper_id=7)
215:     lines = [r.getMessage() for r in caplog.records if r.getMessage().startswith("input_fit paper_id=7 ")]
216:     assert len(lines) == 1
217:     record = json.loads(lines[0].split(" ", 2)[2])
218:     assert record == {"model": "m", "ceiling": 131_072, "chars": 3000, "estimate_low": 570,
219:                       "count": 630, "ratio": 0.21, "done_reason": "stop"}

--- docs/session-reports/session-12/12aa_U20P/load/P3_ps.json:1-1
1: {"models":[{"name":"llama3.2:3b","model":"llama3.2:3b","size":3395661824,"digest":"a80c4f17acd55265feec403c7aef86be0c25983ab279d83f3bcd3abbcb5b8b72","details":{"parent_model":"","format":"gguf","family":"llama","families":["llama"],"parameter_size":"3.2B","quantization_level":"Q4_K_M"},"expires_at":"2319-02-09T19:15:38.168634347Z","size_vram":3395661824,"context_length":8192}]}

engine/agents/extractor.py:727:    the verdict; a correctness-tier drift then raises `ServerEnvChanged`, a run fault
engine/agents/extractor.py:738:        raise rm.ServerEnvChanged.at(reading.correctness, papers_done=papers_done)
engine/core/run_manifest.py:217:class ServerEnvChanged(RunRefused):
engine/core/run_manifest.py:235:           papers_done: int | None = None) -> "ServerEnvChanged":
engine/core/run_manifest.py:241:            f"ServerEnvChanged: run refused after a proactive Ollama restart{done}: the "
scripts/run_pipeline.py:213:    except rm.ServerEnvChanged as exc:
aa76124

 CLAUDE.md                        |  3 ++-
 engine/agents/extractor.py       | 37 ++++++++++++++++++++++++----
 engine/agents/ft_screener.py     |  7 +++++-
 engine/core/run_manifest.py      | 43 ++++++++++++++++++++++++++++++++-
 engine/utils/ollama_preflight.py | 52 +++++++++++++++++++++++++++++++++-------
 scripts/run_pipeline.py          | 13 +++++++++-
 6 files changed, 138 insertions(+), 17 deletions(-)
```
