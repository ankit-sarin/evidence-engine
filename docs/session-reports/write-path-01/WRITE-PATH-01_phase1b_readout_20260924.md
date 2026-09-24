# WRITE-PATH-01 Phase 1b — read-out: /api/chat truncate (R62), A14, D15, B6/C23, R46, sentinels

**Session 9. Read-only apart from M1.** M1 made five raw HTTP calls to the local Ollama service and
wrote nothing to any database. No engine, test, migration, spec or codebook edit; no live database
read; no pull; no restart; no Ollama configuration change.

* Tree: HEAD `80d8df1`, clean, 0/0, at 2026-09-24T22:10:01Z.
* Ollama health before M1: `systemctl is-active ollama` → `active`, `NRestarts=0`,
  `ExecMainStartTimestamp=Fri 2026-09-11 21:09:13 UTC`, `/api/version` → `{"version":"0.21.0"}`;
  resident: `qwen3:8b`. After M1: the same start timestamp and `NRestarts=0`; resident: `llama3.2:3b`
  at `context_length` 1024 (residency irrelevant per the brief). The experiment lock
  (`~/.ollama_experiment.lock`) was held for the run with `flock -n`.
* Citations are by file path and a quoted content anchor. **MEASURED** = produced by a command
  named here; **READ** = from reading source.

---

## Ledger — I1–I5

| # | verdict | evidence |
|---|---|---|
| **I1** | **TRUE** | `/api/tags` (MEASURED): smallest chat model `llama3.2:3b`, 2.02 GB, 3.2B; `qwen3:8b` 5.23 GB also present. Nothing pulled. |
| **I2** | **TRUE, and the stated expectation held** | `"truncate": false` at top level → **HTTP 400** `{"error": "the input length exceeds the context length"}`. Not a truncated 200 (M1 b). |
| **I3** | **TRUE** | `engine/utils/ollama_client.py::_check_input_was_read`: `count = getattr(response, "prompt_eval_count", None)` … `if count >= ceiling:` → `InputTruncated` (Q1). |
| **I4** | **TRUE as a description, with two corrections to what it records** | `engine/core/extraction_telemetry.py` computes no trace statistics. It appends one JSONL row per *attempt* (its docstring says "per API call"), carrying `thinking_chars`, `thinking_present`, `parse_branch` and the Pass-2 `raw_content`, to `data/<review>/telemetry/extraction_calls.jsonl`. It does not store Pass-1 text, Pass-1 `done_reason` or `prompt_eval_count` (Q6). |
| **I5** | **TRUE** | Fresh-database CHECK (Phase 1a P5, re-read in `engine/migrations/020_run_manifest.py` `FIELD_EVENT_TYPES = ("asserted", "declined", "contract_unmet", "superseded", "human_accepted", "human_corrected", "human_withdrew", "duplicate_detected", "citation_located", "state_at_migration")`). No value is trace-shaped. `PAPER_EVENT_TYPES` has none either. |

---

## M1 — `/api/chat` truncate (R62 / I13)

**Verdict: `truncate:false` is honoured at the top level (the server refuses over-length input with
HTTP 400). Inside `options` it is not honoured: it is rejected as an invalid option and the input is
silently truncated.**

**Setup (MEASURED).** Raw HTTP via Python `urllib` (script
`m1_truncate.py` in the session scratchpad; never ollama-python). Server `0.21.0` (from `/api/version`).
Model `llama3.2:3b`; `/api/show` `model_info` → `llama.context_length: 131072` (= n_ctx_train).
`options = {"num_ctx": 1024, "num_predict": 16, "temperature": 0}`, `stream: false`. Long prompt:
one user message of the sentence "The quick brown fox jumps over the lazy dog near the river bank."
repeated 600 times, which is 7,800 words and 38,999 characters. The server's journal counts it as `prompt=8425` tokens.
Control: `"Reply with the single word: ok."`. Measured at 2026-09-24T22:10:20Z.

| request | body delta | HTTP | error | prompt_eval_count | eval_count | done_reason | total_duration (ns) |
|---|---|---|---|---|---|---|---|
| (a) no key | — | **200** | — | **1024** | 16 | `length` | 2,099,998,453 (load 1.76 s) |
| (b) `"truncate": false` top-level | `truncate` key | **400** | `the input length exceeds the context length` | — | — | — | — (0.09 s wall) |
| (c) `"truncate": true` top-level | `truncate` key | **200** | — | **1024** | 16 | `length` | 445,444,566 |
| (d) `"truncate": false` inside `options` | `options.truncate` | **200** | — | **1024** | 16 | `length` | 498,394,391 |
| control: short prompt, `"truncate": false` | `truncate` key | **200** | — | 33 | 3 | `stop` | 149,893,391 |

Raw JSON per response (message content removed; all other keys verbatim):

```json
{"a_no_key": {"http_status": 200, "response": {"model": "llama3.2:3b", "done": true, "done_reason": "length", "prompt_eval_count": 1024, "eval_count": 16, "total_duration": 2099998453, "load_duration": 1763106626, "prompt_eval_duration": 142839430, "eval_duration": 171054689}},
 "b_truncate_false_toplevel": {"http_status": 400, "response": {"error": "the input length exceeds the context length"}},
 "c_truncate_true_toplevel": {"http_status": 200, "response": {"model": "llama3.2:3b", "done": true, "done_reason": "length", "prompt_eval_count": 1024, "eval_count": 16, "total_duration": 445444566, "load_duration": 83298267, "prompt_eval_duration": 145707206, "eval_duration": 170699678}},
 "d_truncate_false_in_options": {"http_status": 200, "response": {"model": "llama3.2:3b", "done": true, "done_reason": "length", "prompt_eval_count": 1024, "eval_count": 16, "total_duration": 498394391, "load_duration": 135720010, "prompt_eval_duration": 145368195, "eval_duration": 169996331}},
 "control_short_truncate_false": {"http_status": 200, "response": {"model": "llama3.2:3b", "done": true, "done_reason": "stop", "prompt_eval_count": 33, "eval_count": 3, "total_duration": 149893391, "load_duration": 90214276, "prompt_eval_duration": 32245139, "eval_duration": 23561584}}}
```

**Server journal (MEASURED, `journalctl -u ollama --since "2026-09-24 22:10:00"`):**

```
22:10:22.630Z level=WARN source=runner.go:153 msg="truncating input prompt" limit=1024 prompt=8425 keep=5 new=1024
22:10:23.038Z level=INFO source=server.go:1634 msg="llm predict error: the input length exceeds the context length"
22:10:23.160Z level=WARN source=runner.go:153 msg="truncating input prompt" limit=1024 prompt=8425 keep=5 new=1024
22:10:23.580Z level=WARN source=types.go:977 msg="invalid option provided" option=truncate
22:10:23.660Z level=WARN source=runner.go:153 msg="truncating input prompt" limit=1024 prompt=8425 keep=5 new=1024
```

**Reading the table.**
* (a), (c) and (d) show that the default behaviour and `truncate:true` are identical: silent truncation
  to `num_ctx`, reported **only** by `prompt_eval_count == 1024` and a server WARN line.
* `done_reason` is `length` in all three because `num_predict=16` capped the output, not because of the input. It carries no
  input-truncation signal, which confirms the learning.
* The control shows the `truncate` key by itself does not cause an error.
* The ceiling here is `num_ctx` (1024), not n_ctx_train (131,072). That is consistent with
  `effective_ceiling`: `"An explicit options.num_ctx is what the runtime loads, clamped only by the model's trained context: min(n_ctx_train, num_ctx)"`.

**The client boundary (R62), re-confirmed (MEASURED):** `ollama-python 0.6.1`; `'truncate' in
ChatRequest.model_fields` → `False`; `'truncate' in inspect.signature(ollama.Client.chat).parameters`
→ `False`. The server honours the key, but the pinned client cannot send it. In (d) the one place
the client *can* put it, `options`, is where the server ignores it.

---

## Q1 — The input-fit guard

* **Where `prompt_eval_count` is read:** `engine/utils/ollama_client.py::_check_input_was_read`:
  `count = getattr(response, "prompt_eval_count", None)`.
* **The ceiling:** `fit["ceiling"]` from `_check_input_fits` → `effective_ceiling(model, options)`:
  `min(trained, num_ctx)` when `options.num_ctx` is set, else `min(trained, SERVER_DEFAULT_CTX = 262_144,
  OLLAMA_CONTEXT_LENGTH if set)`. `trained` comes from `n_ctx_train(model)`.
* **On a hit:** `if count >= ceiling:` → `logger.error("input_fit TRUNCATED %s %s", …)` and
  `raise InputTruncated(model=…, count=…, ceiling=…, chars=…)`. There are two sibling refusals:
  `InputOverflow` before the call (`chars * RATIO_MIN (0.19) >= ceiling`, "Nothing was sent") and
  `InputDropped` after it (`count < chars * RATIO_DROP (0.10)`). They are raised **outside** the
  retry handlers (`"an input-fit failure is neither retried nor converted into a timeout"`).
* **What that does on the extraction path:** `InputFitError` is **not** in
  `extract_paper_with_completeness`'s `RETRYABLE`, so it propagates at once.
  `_run_extraction_unlocked`'s `except Exception` logs `"Paper %d extraction failed: %s%s"` with
  `input_fit={exc.fields}` and calls `db.update_status(pid, "EXTRACT_FAILED")`. **No event is
  written** (no field-event writer exists). And because `_done(...)` wraps the check's return,
  `return _done(_check_input_was_read(response, fit, paper_label))`, **a truncated or dropped call
  raises before `record_active_ollama_call`, so it leaves no `run_calls` row** even inside a run.
* **M1 bearing:** with top-level `truncate:false` the same condition would arrive as an HTTP 400
  **before any output**, instead of a 200 that the guard inspects afterwards. The pinned client cannot send it (M1).

## Q2 — A14: the four `ParsedTextMissing` / `ParsedTextModified` handling sites

Both are subclasses of `ParsedTextError(RuntimeError)`; `NoParsedText` is the third
(`engine/core/parsed_text.py`).

| site | quoted handling | effect on the run |
|---|---|---|
| `engine/agents/extractor.py::_run_extraction_unlocked` | `except ParsedTextError as exc: logger.error("Paper %d: parsed text refused — %s", pid, exc); stats["failed"] += 1; progress.report(pid, "FAILED", 0); continue` | **skip the paper**, run continues, counted `failed`. **Writes no status** (the `update_status` calls are inside the later `try`). |
| `engine/agents/auditor.py::run_audit` | `except ParsedTextError as exc: logger.error("Paper %d: parsed text refused — %s — skipping audit", pid, exc); continue` | skip; no status; not counted in `stats`. |
| `engine/agents/ft_screener.py::_load_parsed_text` | `try: return load_parsed_text(db._conn, paper_id)` / `except NoParsedText: return None` | Missing/Modified **propagate** out of `run_ft_screening` / `run_ft_verification` (no handler at either call site: `parsed_text = _load_parsed_text(db, pid)`) and out of `main()` (`try: … finally: db.close()`). **The run aborts**; the uncaught exception makes Python exit 1. |
| `engine/cloud/base.py::CloudExtractorBase.load_parsed_text` | `except NoParsedText as exc: raise FileNotFoundError(str(exc)) from exc` | Only `NoParsedText` is converted. The per-paper loop catches only `FileNotFoundError` (`openai_extractor.py` / `anthropic_extractor.py`: `except FileNotFoundError as exc: logger.warning("Paper %d: %s — skipping", pid, exc)`), so Missing/Modified **propagate** out of `run()`. `scripts/run_cloud_extraction.py` then runs `finally: rm.close_run(extractor._conn, handle.run_id, status)` with `status = "failed"`, and the exception escapes. **The run aborts, the manifest is closed `failed`, and the exit is 1.** |

## Q3 — D15: `parse_pdf`'s same-hash short-circuit

`engine/parsers/pdf_parser.py::parse_pdf`:

```python
existing = None if force else db._conn.execute(
    "SELECT parsed_text_version, parser_used FROM full_text_assets "
    "WHERE paper_id = ? AND pdf_hash = ? ORDER BY parsed_text_version DESC LIMIT 1",
    (paper_id, pdf_hash),
).fetchone()
if existing:
    version = existing[0]
    ...
    md_path = (Path(db.db_path).parent / "parsed_text" / f"{paper_id}_v{version}.md")
    ...
    return ParsedDocument(..., parsed_markdown=md_path.read_text() if md_path.exists() else "", ...)
```

* **What it reads, and from which row:** the newest `full_text_assets` row for `(paper_id, pdf_hash)`.
  It takes only `parsed_text_version` and `parser_used`, not `parsed_text_path`. It builds the path
  from the version, reads it **without a hash check**, and returns `""` **silently** if the file is absent.
* **Could the resolver serve it as it stands? Not as a like-for-like swap** (signature check):
  `resolve_parsed_text(conn, paper_id) -> ParsedTextRef` takes no version or pdf-hash argument and
  returns the paper's **greatest** recorded version. The short-circuit wants the version that
  matches **this PDF's hash**. The two agree only when that version is also the greatest. A version-keyed
  read, or a `pdf_hash` on `parsed_text_refs`, does not exist today.
  `read_parsed_text(ref)` would verify a ref once one existed.
* **Consumers of the unverified text (READ):** the two in-engine callers of `parse_pdf`
  (`pdf_parser.py` `result = parse_pdf(pdf_path, pid, review_name, db)` and the re-parse path's
  `result = parse_pdf(pdf_path, pid, review_dir.name, db, spec=spec, …)`) read `parser_used`,
  `attempts`, `accepted_parser` and `version`. **Neither reads `parsed_markdown`.** Today the
  unchecked read goes to no consumer inside `engine/` or `scripts/`.

## Q4 — B6: absence hand-lists against the codebook's sentinels

**The codebook's sentinels, verbatim** (`data/surgical_autonomy/extraction_codebook.yaml`, top-level
key `absence_sentinels:`):

```yaml
absence_sentinels:
  - "NR"
  - "N/A"
  - "NA"
  - "NOT_FOUND"
  - "NOT FOUND"
  - "NOT REPORTED"
```

and, separately, `escape_token: "NO_EVIDENCE_LOCATABLE"` and `contract_unmet_token: "CONTRACT_UNMET"`.

**`audit_span`'s hand-list and the tier branch** (`engine/agents/auditor.py`):

```python
    # Values that indicate the field is absent/not reported — auto-verify
    _ABSENCE_VALUES = {"NOT_FOUND", "Not discussed", "NR", "No comparison reported"}
    if value in _ABSENCE_VALUES:
        return "verified", f"Field value '{value}' indicates absence — no extraction to audit."
...
SEMANTIC_ONLY_TIERS = {4}
...
    is_semantic_only = field_tier in SEMANTIC_ONLY_TIERS
    if is_semantic_only:
        grep_pass = True  # not evaluated, treat as pass for routing
    else:
        grep_pass = grep_verify(source_snippet, paper_text)
```

The hand-list shares only `NOT_FOUND` and `NR` with the codebook. It lacks `N/A`, `NA`, `NOT FOUND` and
`NOT REPORTED`, and adds `Not discussed` and `No comparison reported`. The comparison is exact and
case-sensitive (`value in _ABSENCE_VALUES`). The non-value-token branch just above it *does* read the
codebook (`str(value).strip().upper() in non_value_tokens`).

**Every other hardcoded sentinel or absence phrase in `engine/`** (grep, then each site read;
`"N/A"` used as a display string for NaN in `analysis/report.py`, `concordance.py` and `pdf_parser.py` is excluded):

| site | literal | role |
|---|---|---|
| `engine/agents/auditor.py` (module level, LOW_YIELD) | `_ABSENCE_VALUES = {"NOT_FOUND", "Not discussed", "NR", "No comparison reported", "Not assessable"}` | a **second, different** list (5 items; adds `Not assessable`) |
| `engine/agents/extractor.py::build_extraction_prompt` | `set to "NOT_FOUND"` / `If value is "NOT_FOUND", set source_snippet to ""` | legacy prompt instruction |
| `engine/elicitation/prompts.py` | example entry `… "{KEY_VALUE}": "NR"}}` | elicitation prompt example |
| `engine/validators/distribution_monitor.py` | `_NULL_SYNONYMS = {"", "nr", "n/r", "not reported", "not_found", "none", "n/a"}` | absence for distribution analysis (lowercased) |
| `engine/validators/extraction_validator.py` (×3) | `if value in ("NOT_FOUND", "NR"):` | validator skip |
| `engine/cloud/base.py` (response parsing) | `span["value"] = "NR"` on a null value | **writes** a sentinel into cloud spans |
| `engine/review/human_review.py` | `SET value = 'NR', audit_status = 'verified'` on REJECT | **writes** a sentinel into `evidence_spans` |

`engine/elicitation/classes.py` already names four of these as fix-phase item **N2** (`"two divergent
_ABSENCE_VALUES in auditor.py, the normaliser's ("NOT_FOUND", "NR"), the monitor's _NULL_SYNONYMS"`).
`engine/analysis/normalize.py` reads the codebook (`_absence_sentinels()`: `"One source. The
codebook declares absence_sentinels"`) and is clean.

**`'No comparison reported'` in the codebook or prompts: yes, in the codebook, as an instruction,
not as a sentinel.** Field `comparison_to_human` (tier 3, `field_class: stated`, `type: free_text`),
`instruction:` ends `If no comparison was made, record "No comparison reported".` Because
`build_extraction_prompt` renders each field's `instruction`, the phrase reaches every extraction
prompt. It is **not** in `absence_sentinels`. That accounts for Phase 1a P10's 131 spans, all in
`comparison_to_human`. Elsewhere it appears in the two auditor hand-lists and under `analysis/provenance/`
(`absence.py`, `field_class3.py`); it is in no `engine/` prompt text.

## Q5 — C23: `cfg.with_options(ollama_options)`

* The auditor site (`engine/agents/auditor.py::semantic_verify`): `cfg = cfg.with_options(ollama_options)`;
  docstring: `"ollama_options is a caller override merged over the resolved options (only
  scripts/eval_auditor_models.py passes one)"`. `audit_span(…, ollama_options: dict | None = None, …)`
  forwards it.
* **Callers passing `ollama_options`: confirmed one.** `scripts/eval_auditor_models.py`:
  `status, reasoning = audit_span(s, paper_text, …, model=model_name, ollama_options=opts)` with
  `MODEL_OPTIONS = {"llama4:scout": {"num_ctx": 4096}}`. The other `audit_span` callers
  (`auditor.run_audit`, `scripts/smoke_test_fixes.py`, the tests) pass none.
* **But `EffectiveConfig.with_options` itself has three callers, not one.** Its docstring reads
  `"A caller's option override — today only scripts/eval_auditor_models.py"`. The other two are
  `engine/agents/ft_screener.py` (`cfg = cfg.with_options({"temperature": temperature})`) and
  `engine/parsers/pdf_parser.py` (the vision path: `cfg = cfg.with_options(override)` for
  `num_predict` / `num_ctx`). The docstring is stale.
* **What the script is for:** its docstring says `"Evaluate candidate auditor models on 5 papers from the current
  corpus. Compares Qwen3:32b (current), Llama4:scout, and Gemma3:27b"`. It reads legacy
  `extractions` / `evidence_spans`. `llama4:scout` is **not installed** (M1 `/api/tags`).
* **Tests:** no test passes `ollama_options`. `tests/test_review_paths.py` names the script's
  **path** in a list of entry points (`"scripts/eval_auditor_models.py",`), which is a coupling by filename only.

## Q6 — R46: trace events

* **What the extractor captures under `reasoning_trace` today:** Pass 1 only.
  `extract_pass1_reasoning` → `parse_thinking_trace(content, thinking)`: `message.thinking` (branch
  `native`), else inline `<think>…</think>` in `message.content` (`legacy-tags`), else
  `MissingThinkingChannelError`. That string is stored as `extractions.reasoning_trace`
  (`reasoning_trace=reasoning_trace` in `add_extraction_atomic`). Pass 2 runs with `think=False` and
  its `message.content` is the JSON. The elicited path instead stores `reasoning_trace=priming` (the
  materialized unit text), which is not a model trace.
* **`extraction_telemetry` (I4):** one row per attempt with `thinking_present`,
  `thinking_chars = len(trace)`, `parse_branch`, Pass 2's `finish_reason` and `raw_content` (Pass 2),
  and `extra` (elicitation). Pass 1's `finish_reason` is put into `_LAST_PASS1_TELEMETRY` but
  `record_call` is passed `_LAST_PASS2_TELEMETRY.get("finish_reason")`, so **Pass 1's `done_reason` is
  never recorded**. The file `data/surgical_autonomy/telemetry/` **does not exist**: no telemetered local run has happened on this review.
* **`run_calls`** holds `request_hash`, `response_digest` (a sha256 over content and thinking), stage,
  paper_id and times. It holds no text, no length and no token counts.
* **The retired exporter's three quantities**
  (`443e3d8968bf5bcee9679102dcb798bf4f40bdcf:engine/exporters/trace_exporter.py::_trace_quality_report`)
  and where each could come from now:

| quantity | retired definition | on `run_calls`? | in `extraction_telemetry`? | otherwise |
|---|---|---|---|---|
| truncation flag | `_is_truncated(trace)`: `not stripped.endswith((".", "?", "!", ">"))`. A heuristic on the trace's last character | no | **no**: text not stored; Pass-1 `done_reason` (the direct signal) not recorded | needs the trace text or Pass-1 `done_reason`; would need an event or a telemetry change |
| length distribution | min / max / mean / median / std of `len(reasoning_trace)`; `under_500` | no | **yes**: `thinking_chars` per attempt (local arm; the file does not exist on this review yet) | legacy `extractions.reasoning_trace` (190 rows; retiring) |
| flagged papers | `if length < 500: flagged_papers.append(...)` | no | **derivable** from `thinking_chars` | same |

---

## Forks (options as CC sees them; no recommendation)

* **A14.** (a) Keep per-site handling and make it explicit; (b) one policy everywhere: skip plus
  a `paper_events` processing event with a reason code; (c) one policy everywhere: abort the run. Also decide
  whether the extractor's skip should write any state: today it writes none, so a refused paper is
  invisible outside the log.
* **D15.** (a) Route the short-circuit through the resolver (the version may differ from the
  hash-matched one); (b) add a version-keyed verified read; (c) drop `parsed_markdown` from the
  short-circuit result, since no caller reads it; (d) leave it, recorded as unreached.
* **B6.** (a) Read the auditor's list from the codebook (`absence_sentinels` + non-value tokens);
  (b) add `No comparison reported` (and `Not discussed`, `Not assessable`) to `absence_sentinels`, a
  codebook edit that is also a scientific-record decision; (c) change the `comparison_to_human`
  instruction to a declared sentinel; (d) keep the tier-4 `grep_pass = True` or route tier 4 through
  the locator (R17). The seven `engine/` sites above either converge on one or two readers, or stay as N2.
* **C23.** (a) Retire `ollama_options` from `audit_span` / `semantic_verify` with the script (which
  reads legacy tables and names an uninstalled model); (b) keep it as a recorded caller override. Separately,
  `with_options`'s stale docstring and its two live callers (FT screener temperature, vision
  `num_predict` / `num_ctx`).
* **R46.** (a) A trace event per extraction attempt: needs a new `field_events`/`paper_events` type
  (a CHECK change, so a migration), or a new table; (b) extend `extraction_telemetry` to carry Pass-1
  `done_reason`, `prompt_eval_count` and the trace (no migration; file-based, not in the database);
  (c) record per-call lengths or token counts on `run_calls` (a migration). And whether the
  truncation measure stays the retired last-character heuristic or becomes Pass-1 `done_reason` / `prompt_eval_count`.
* **S3f-min (from M1).** (a) Send top-level `truncate:false` over raw HTTP beside the pinned client;
  (b) change or upgrade the client (R75 pins it); (c) keep the post-call `prompt_eval_count` guard
  only. Decide also whether an HTTP 400 "input length exceeds" maps to `InputOverflow` or to a new
  refusal, and that a truncated call currently leaves no `run_calls` row.

## Findings that contradict the ledger or a plan premise

1. **C23's premise needs narrowing.** It is true that `scripts/eval_auditor_models.py` is the only
   caller passing `ollama_options`. But the `with_options` mechanism has two further live callers,
   and its docstring's "today only" is stale.
2. **I4 correction.** `extraction_telemetry` records per **attempt**, not per API call, and it omits
   Pass-1 `done_reason`. No telemetry file exists on this review.
3. **Learning refinement.** "The enforced context ceiling is the model's n_ctx_train clamp" holds only
   when no `num_ctx` is sent. With `options.num_ctx`, the ceiling M1 measured is `num_ctx` (1024 of a
   131,072 n_ctx_train), which matches `effective_ceiling`'s docstring. The code is right; the shorthand is incomplete.

## Stale lines noticed, not fixed

* `EffectiveConfig.with_options` docstring: "today only `scripts/eval_auditor_models.py`" (three callers).
* `extraction_telemetry.py` module docstring: "One JSON line per API call" (it writes per attempt).
* `scripts/eval_auditor_models.py`: `MODELS = ["qwen3:32b", "llama4:scout", "gemma3:27b"]`. `llama4:scout`
  is not installed, and `qwen3:32b` is described as "current" auditor. The live spec sets no
  `auditor_model` or `audit` block, so the resolver's `audit` stage is the declared default
  `AuditModels.model = "gemma3:27b"`.

---

## Addendum 2026-09-24 (WRITE-PATH-01 Phase 1c) — client transport for a top-level key; `run_calls` outcome columns

*Appended; the read-out above is unedited.* Read-only. One wheel was downloaded into the session
scratchpad with `pip download --no-deps --only-binary=:all:` and unpacked with `python3 -m zipfile`;
nothing was installed. `.venv` `pip freeze` is byte-identical before and after (sha256 `be60485a…20ac`
both times; `ollama==0.6.1`). HEAD `99934ca` at 22:17:41Z.

### A — Can the pinned client (ollama-python 0.6.1) send a top-level `truncate`? **(iii) Yes, only via a private method.**

* **No public parameter.** `ollama/_client.py` `Client.chat(self, model='', messages=None, *,
  tools=None, stream=False, think=None, logprobs=None, top_logprobs=None, format=None, options=None,
  keep_alive=None)` has no `truncate` and no `**kwargs`. It builds the body as
  `json=ChatRequest(model=model, messages=…, tools=…, stream=stream, think=think, logprobs=…,
  top_logprobs=…, format=format, options=options, keep_alive=keep_alive).model_dump(exclude_none=True)`.
* **The request model drops unknown keys.** `ollama/_types.py` `class ChatRequest(BaseGenerateRequest):`
  sets no `model_config` of its own (MEASURED: `ChatRequest.model_config` → `{}`, i.e. pydantic's
  default `extra='ignore'`). MEASURED: `ChatRequest.model_validate({'model':'m','messages':[],
  'truncate':False}).model_dump(exclude_none=True)` → `{'model': 'm', 'messages': []}`, so the key
  is silently discarded. Fields: `model, stream, options, format, keep_alive, messages, tools, think,
  logprobs, top_logprobs`.
* **The call path to the HTTP send:** `Client.chat` → `self._request(ChatResponse, 'POST', '/api/chat',
  json=<dict>, stream=stream)` → (non-stream) `cls(**self._request_raw(*args, **kwargs).json())` →
  `Client._request_raw`: `r = self._client.request(*args, **kwargs); r.raise_for_status()`, where
  `self._client` is the `httpx.Client` built in `BaseClient.__init__` (`self._client = client(base_url=…,
  follow_redirects=…, timeout=timeout, headers=headers, **kwargs)`). The JSON body is formed at the
  `json=ChatRequest(...).model_dump(exclude_none=True)` line inside `chat`.
* **The seam that accepts a prepared body dict is private:** `Client._request(cls, *args, stream=False,
  **kwargs)`, e.g. `_client._request(ChatResponse, 'POST', '/api/chat', json=<body with truncate>)`. It
  bypasses `Client.chat`'s own body construction: `ChatRequest` validation and serialization,
  `_copy_messages`, and `_copy_tools`. **It also bypasses the R56 capture.** `tests/test_request_capture.py`
  replaces the whole module client (`monkeypatch.setattr(oc, "_client", fake)`), and the fake defines
  only `show`, `chat(self, **kwargs)` and `_client = SimpleNamespace(base_url=…)`. A `_request` call
  would not be seen by `chat`, and against the fake it would raise `AttributeError`. `ollama_client`'s
  run-call `request_hash` is taken over the kwargs it builds, not the HTTP body, so it would not
  include `truncate` unless the caller put it there. Errors keep their shape: `_request_raw` maps HTTP errors
  to `ResponseError(e.response.text, e.response.status_code)`.
* **Not counted as the client's own transport:** `BaseClient.__init__` documents `"kwargs are passed to the
  httpx client"`, so `ollama.Client(transport=<custom httpx transport>)` could rewrite the body on
  the way out. That *replaces* the transport rather than using it, and it is equally invisible to
  the capture at `_client.chat`.

### B — The latest released client

`pip index versions ollama` → `ollama (0.6.2)`, with 0.6.1 the previous release. Wheel
`ollama-0.6.2-py3-none-any.whl` inspected unpacked (imported from scratch via `sys.path`, never
installed). **0.6.2 `ChatRequest` fields: `model, stream, options, format, keep_alive, messages,
tools, think, logprobs, top_logprobs`, with `model_config` `{}`. No `truncate`.** 0.6.2
`Client.chat` parameters: `self, model, messages, tools, stream, think, logprobs, top_logprobs, format,
options, keep_alive`. **No `truncate`.** The `def chat(` … `def embed` source block is byte-identical to
0.6.1. In both versions the only `truncate` is on embeddings: 0.6.2 `class EmbedRequest(BaseRequest):`
`truncate: Optional[bool] = None` and `def embed(… truncate: Optional[bool] = None …)`; 0.6.1's
`_client.py` `embed` carries the same parameter.

### C — `run_calls` columns

Fresh-database `sqlite_master` (identical to `engine/migrations/020_run_manifest.py` `RUN_CALLS_SQL`):

```sql
CREATE TABLE run_calls (
    call_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          INTEGER NOT NULL REFERENCES run_manifests(run_id),
    stage           TEXT    NOT NULL,
    paper_id        INTEGER REFERENCES papers(id),
    request_hash    TEXT    NOT NULL,
    response_digest TEXT,
    started_at      TEXT    NOT NULL,
    ended_at        TEXT    NOT NULL,
    FOREIGN KEY (run_id, stage) REFERENCES run_stage_configs(run_id, stage)
)
```

plus `idx_run_calls_run` and the append-only triggers `run_calls_no_update` / `run_calls_no_delete`
(`'run_calls is a record: one row per call, never edited'`).

**No outcome or status column exists. A refused call cannot be recorded *distinguishably* with the existing
columns.** `response_digest` is nullable, and a refused call could be written with it NULL. But
NULL is already the value **every cloud call** records: `engine/cloud/base.py` `rm.record_call(self._conn,
self.run_id, self.stage_cfg.stage, paper_id, payload, None, started, …)`. So a NULL digest cannot mean
"refused". A pre-call refusal (`InputOverflow`, "Nothing was sent") would have a request hash but no
send. A post-call refusal (`InputTruncated` / `InputDropped`) raises before `_done(...)`, so today it
writes no row at all (Q1). Recording the outcome therefore needs a new column or table (a migration)
or an overloaded existing field.

### Ledger (Phase 1c)

* **I1 — TRUE**, with the answer (iii): `ChatRequest` ignores extra keys (`model_config` `{}`), and the
  only seam taking a prepared body is the private `Client._request`.
* **I2 — TRUE**: 0.6.2 (latest) has no `truncate` on `ChatRequest` or `Client.chat`, only on `embed`.
* **I3 — TRUE**: no outcome or status column; the nullable `response_digest` is already NULL on every cloud call.
