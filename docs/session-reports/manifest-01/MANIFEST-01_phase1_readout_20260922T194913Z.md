# MANIFEST-01 Phase 1 — configuration, manifest, run linkage, cloud opt-in: read-only read-out

**Session 7 of the refactor lane (S3a, S3b, S3g), Phase 1.** Read-only census and design proposal.
**Date:** 2026-09-22. **HEAD at read:** `f07c9a5e21bf88e152420bf6575d6f47eb47d412`, clean, level
with origin. **Task type:** mixed — read-only measurement plus this docs-only commit.

No engine code changed, no migration created, no model or cloud call, no write to `review.db`.
Every open of `review.db` was `mode=ro` (never `immutable=1`). Probes were disposable scripts in
the session scratchpad and are not committed; each is described where its result is used.

**Tags.** **MEASURED** = produced here by running code, a query, or an AST walk. **READ** = quoted
from a file at this HEAD. **INFERRED** = a reading, a pairing, or a cause, including anything
counted by eye. Anchors are quoted content, never line numbers.

**Sequence.** The session stopped twice on falsified ledger items (I3 at P2, I12 at P3) and resumed
under rulings R56–R59 and R60–R65. Both stops and their rulings are recorded in §"Ledger
corrections".

---

## P0 — Startup verify

| check | expected | measured | |
|---|---|---|---|
| HEAD | `f07c9a5e21bf88e152420bf6575d6f47eb47d412`, clean, level | same; `git status --porcelain` empty; `HEAD...origin/main` = `0 0` | match |
| claude-config | `a3eee186a1a07ab1a68f65da766747e9adb76275` | same | match |
| standard gate | 2,564 / 17, deselects 0/0/10/6/1 | 549 + 662 + 407 + 529 + 417 = **2,564** passed; deselects 0/0/10/6/1 = **17** | match |
| `db_fingerprint --compare` vs `docs/session-reports/readers-01/review_db_fingerprint_20260922T165644Z.json` | exit 0 | **exit 0** ("IDENTICAL — schema, every table, and the overall hash all match.") | match |
| overall | `e564f250afe40af7eb9a6bc07596a3c795972f7ef3b653c37599e8bc18285b63` | same | match |
| tables · `-wal` | 31 · 0 B or absent | 31 · 0 B | match |
| receipts | 18; 018 `073307f0f29ada4abcfeb75563509e208923b3543cf3f601441c9e2368a88eee`; 019 `131c7c12d8105c55009b9693553c2f0a37bf5ef0578721f7d033878e44d85bca` | 18; both identical, both `executed` | match |
| event store | 3 arms · 190 paper events · 194 parsed-text refs · 3 identities · 0 field events | 3 · 190 · 194 · 3 · 0 | match |
| restore point `data/surgical_autonomy/review.db.bak-readers-01-phase3-pre-write-20260922-165453` | present, 32 tables, `62f3912813b6e9efa3a651ee4c4ebcd611adf89c989f240a58e83dcf6759b79a` | present; 32; same | match |
| Ollama untouched by the gate | — | `NRestarts=0`, `ExecMainStartTimestamp` 2026-09-11 21:09:13 UTC | — |

`engine.tools.inventory --check`: "inventory in sync".

---

## Ledger corrections

Recorded here so the Step 4 closure paragraph can cite them by number.

### I3 — FALSIFIED at P2

**Ledger text.** "A request-capture stub from DISCOVERY-01 Part B exists in the tree and can show
the request as sent per Ollama call site."

**Measured.** No such artifact is committed. DISCOVERY-01 committed only documents: `7e09de3`
changed 2 files (Part A read-out and a fingerprint JSON), and `b8a83b1` changed 1 file (the Part B
read-out). Part B's method line describes a stub that was substituted during the measurement and
never committed: "`engine.utils.ollama_client._client` replaced by a capture stub;
`_check_input_fits` and `_check_input_was_read` stubbed to no-ops". The only committed request
capture is `tests/test_eligibility.py::_capture`, from SCREEN-AUTH-01. It patches `ollama_chat` in
the caller's module and records `box["messages"] = _kw.get("messages")` and
`box["format"] = _kw.get("format")`, for four screening sites. That is what the builder passed, and
it never sees model, options, `think` or `keep_alive`.

**Cause.** The architect read the session-summary gate wording ("The Part B capture stub shows
…") as evidence that a committed artifact existed.

### I12 — FALSIFIED at P3

**Ledger text.** "No renderer among the fourteen frozen surfaces reads a spec field outside
spec.eligibility."

**Measured.** Ten of the fourteen surfaces also read `spec.pico`, and R1 also reads `spec.title`.
See P3 for the tables.

### Rulings made at the stops (PI)

* **R56.** I3 is amended to "No gate instrument exists for 'every option originates in the spec or
  a declared default'." The instrument is a session-7 Phase 2 deliverable: one committed capture at
  the `_client.chat` boundary, driven through every P1 call site.
  `test_eligibility::_capture` stays as the screening-surface pin.
* **R57.** Inventory row **C15** (class C): `extractions.model_digest` was never filled.
  `get_model_digest` reads an endpoint that does not expose the digest; `fetch_model_digest`
  (`/api/tags`) does. The manifest and the S3d key use `/api/tags`. `get_model_digest` is retired
  in Phase 2 under B5, and its tests are rewritten rather than deleted.
* **R58.** Inventory row **C16** (class C): `cloud_extractions.prompt_text` is a partial record of
  what leaves the machine. S3g's payload description is a hash over the complete outbound template
  plus a plain-language description of the per-paper content.
* **R59.** An arm's configuration freezes at its first manifest, and pre-manifest arms are refused.
  The R21 freeze widens to "holds a claim OR pinned by a manifest". A manifest naming a pre-manifest
  arm refuses before its first call, and the event writer refuses a claim on a pre-manifest arm.
* **R60.** I12 is amended to the measured fact. Inventory row **C17** (class C): `screening_hash()`
  covers eligibility only, while pico reaches 10 of the 14 frozen surfaces and title reaches one.
  `prompt_hash` is the hash of the rendered request per stage, never of a spec sub-block. Spec
  identity for S9 is the whole-spec hash (noted, not built).
* **R61.** Capture boundary: the kwargs handed to `_client.chat` at the 11 generation sites, plus
  the two cloud request objects at their engine-owned boundary. There is no transport-level
  capture. The manifest records the ollama-python version. Every resolver option is explicit, never
  `None`.
* **R62.** `truncate` drops out of R56's capture list. D6 gains a dated addendum. Whether
  `/api/chat` honours `truncate` is **I13**, measured in session 9 before S3f-min.
* **R63.** `check_model` is a generation site and falls within the resolver's scope as the
  **preflight** stage.
* **R64.** Cloud arm naming comes from the spec's arms block, resolved by the resolver, and the
  class-constant `ARM` names are retired (Phase 2). R22-U4 is unchanged. `run_cloud_strict.py` is
  classified in P7; its retention ruling is the PI's.
* **R65.** One stop point per ruling. A P4–P8 finding that contradicts the ledger is recorded here
  and does not halt Phase 1.

### Ledger items found to contradict something during P4–P8 (reported under R65, not resolved)

* **R59 vs `engine/core/events.py`'s module docstring.** READ: "**No refusal on a pre-manifest
  arm.** An earlier draft of this session refused `asserted` on an arm marked `not recorded
  (pre-manifest)`. That was reversed: v2.1 row 7 *is* two claims on such an arm … the refusal would
  have deleted the row it was meant to protect." R59 reinstates a refusal on claims. The two can be
  made consistent: row 7's exits are **reviewer** events (R20), and on live there are **0** field
  events on any pre-manifest arm (MEASURED). So under R59, row 7 stays reachable **only on
  constructed fixtures**. Recorded for the architect; the docstring is Phase 2's to amend.
* **`write_field_event` and `write_paper_event` default to `run_marker="pre-manifest"`.** READ:
  `run_id=None, run_marker="pre-manifest"` in both signatures. A new event written without a run is
  therefore silently marked pre-manifest. This contradicts S3b ("Every new decision, extraction and
  span row carries a run_id"). ARMED once the extractor moves to the event writer (session 9).
* **R21's "accepts no new claims" is not enforced at write.** READ: `retire_arm`'s docstring says
  "R21: retiring means the arm accepts no new claims; its claims stand", but `write_field_event`'s
  refusals are `CellNotAssigned`, `ReviewerDecisionAmbiguous`, `AcceptAgainstMultipleClaims` and
  `AgainstReferenceIncomplete`. None of them reads `arms.retired_at` (MEASURED by AST reading of
  the function; the only `retired_at` write is `retire_arm`). New inventory candidate, see §5.
* **R61 "the pinned client library".** `requirements.txt` carries `ollama`, `openai` and
  `anthropic` as **bare tokens**, so none is pinned. MEASURED installed versions: ollama-python
  **0.6.1**, openai **2.24.0**, anthropic **0.84.0**, httpx **0.28.1**; Ollama server **0.21.0**.
  The manifest must record the installed version (`importlib.metadata.version`), not the
  requirement line.

---

## P1 — Model call-site census at HEAD

**Method — MEASURED.** A disposable AST walk over `engine/`, `scripts/` and `analysis/` found every
`Call` whose callee ends in `ollama_chat`, a client `.chat`, `chat.completions.create`,
`messages.create`, `get_model_digest` or `fetch_model_digest`. For each call it recorded the
enclosing function and every keyword argument as source text. Origins were then read at each site.
Nothing was executed.

**Totals.** In `engine/`: **11 Ollama generation sites** and **2 cloud sites**. In `scripts/`:
**none**. In `analysis/`: 11 Ollama sites across the eval and judge lanes, plus a second cloud
harness (P7).

### I10 — one client (VERIFIED for generation)

Every engine generation call reaches the server through `engine.utils.ollama_client`, either by
`ollama_chat(...)` or, after a restart, through `_restart_ollama_and_retry`. Both submit
`_client.chat, model=model, messages=messages, **kwargs`. Two non-generating calls bypass it:
`ollama.ps()` in `engine/utils/ollama_preflight.py::_get_model_vram_gb`, using the library's module
default client, and `httpx.get("http://127.0.0.1:11434/api/tags", timeout=5)` in
`engine/agents/extractor.py`'s restart poll. Two further non-generating calls go through `_client`:
`_client.show(model)` in `n_ctx_train` (the input-fit ceiling) and in `get_model_digest`.

### Engine generation sites — option origins

Legend. **spec** = a spec-model field; **const** = a module constant; **lit** = a literal at the
call; **arg** = a caller argument; **—** = not sent.

| # | stage | site | model | temperature | seed | num_ctx | think | format | keep_alive |
|---|---|---|---|---|---|---|---|---|---|
| 1 | abstract primary | `screener.screen_paper` (role primary) | spec `screening_models.primary` (arg override) | **lit** `{'temperature': 0}` | — | — | **lit** `False` | lit `ScreeningDecision.model_json_schema()` | — |
| 2 | abstract verifier | `screener.screen_paper` (role verifier) | spec `screening_models.verification` | **lit** 0 | — | — | **lit** `False` | same | — |
| 3 | FT primary | `ft_screener.ft_screen_paper` | spec `ft_screening_models.primary` | spec `.temperature` | — | — | spec `.think` | `render.with_reason_code_vocabulary(FTScreeningDecision.model_json_schema(), spec.eligibility)` | — |
| 4 | FT verifier | `ft_screener.ft_verify_paper` | spec `.verifier` | spec `.temperature` | — | — | spec `.think` | lit `FTVerificationDecision.model_json_schema()` | — |
| 5 | extract pass 1 | `extractor.extract_pass1_reasoning` | **const** `MODEL = "deepseek-r1:32b"` | **lit** 0 | — | — | spec `extraction_models.pass1_think` via arg | — | — |
| 6 | extract pass 2 | `extractor.extract_pass2_structured` | **const** `MODEL` | **lit** 0 | — | — | spec `.pass2_think` via arg | lit `ExtractionOutput.model_json_schema()` | — |
| 7 | snippet retry | `extractor._retry_snippet` | **const** `MODEL` | **lit** 0 | — | — | **lit** `False` | — | — |
| 8 | elicit pass 1 | `elicitation.pipeline.run_pass1` | **const**, the extractor's `MODEL` imported (`from engine.agents.extractor import (MODEL, …`) | **lit** 0 | — | — | spec `.pass1_think` via arg | — | — |
| 9 | audit | `auditor.semantic_verify` | arg > spec `auditor_model` > **const** `DEFAULT_AUDITOR_MODEL = "gemma3:27b"` | **lit** 0, merged under arg `ollama_options` | arg (none in production) | arg (none in production) | **lit** `False` | lit `AuditVerdict.model_json_schema()` | — |
| 10 | vision parse | `pdf_parser.parse_with_vision` | spec `pdf_parsing.vision_model`, else **const** `_VISION_MODEL` | **lit** 0 | — | spec `vision_num_ctx` via `getattr(..., _VISION_NUM_CTX)` | — | — | — |
| 11 | PDF quality | `pdf_quality_check._classify_page` | spec `pdf_quality_check.ai_model` via caller | **lit** 0 | — | — | — | — | — |
| 12 | **preflight** | `ollama_preflight.check_model` | arg (the caller's model list) | **lit** 0, plus **lit** `num_predict: 4` | — | — | — | — | — |

Site 10 also sends `num_predict` from spec `vision_num_predict`. Sites 7 and 8 share a model with 5
and 6. `max_retries` and `wall_timeout` are client-side controls and are not sent.

**The auditor merge point has no production caller.**
`options={**{"temperature": 0}, **(ollama_options or {})}` is live code, but the only caller that
passes `ollama_options` is `scripts/eval_auditor_models.py` (`model=model_name,
ollama_options=opts`). The audit pipeline passes none, so in production site 9 sends
`{'temperature': 0}` only.

**Digest calls (R57).** In `engine/`, only `extractor._run_extraction_unlocked` fetches a digest,
and it calls **`get_model_digest`** twice (extractor and auditor model). No other engine site
records a digest. Every analysis-lane digest (`judge.run_pass1`, `judge.run_pass2`, `judge_cli`,
`pass2_full`, `pass2_smoke`, `pass2_retry_single`, `judge_codebook_smoke`, `runner_smoke_phase2a`)
calls **`fetch_model_digest`**. MEASURED on live: `extractions.model_digest` NULL on **190 / 190**,
`auditor_model_digest` NULL on **190 / 190**. The mechanism is confirmed in the database itself:
`judge_run_audit`'s one row, `backfill_judge_model_digest`, records "/api/show verified empty of
digest field on Ollama version at backfill time".

### Deltas from the 4e2a66c census (DISCOVERY-01 D2-1)

MEASURED by `git diff 4e2a66c HEAD` over every call-site module. **No option at any engine site has
changed since 4e2a66c.** The only diffs in those files are the corpus predicate in
`engine/cloud/base.py` (`corpus_status_sql` → `corpus_id_sql(self._conn, "p.id")`, READERS-01) and
a comment in `engine/elicitation/pipeline.py`. The census itself differs from D2-1 in three places:

1. **`ollama_preflight.check_model` is a generation site D2-1 did not list.** Its request,
   `options={"temperature": 0, "num_predict": 4}`, is identical at 4e2a66c. It loads the model (R63).
2. **D2-1 named "module constant `elicitation.pipeline.MODEL`".** At both commits that name is the
   extractor's constant, imported, not a second constant. One constant governs sites 5–8.
3. **The cloud pending-paper set now comes from the eligibility axis** (`corpus_id_sql`), not from
   `papers.status`.

**C1 is re-measured and still true.** `spec.extraction_models.extractor` and `.temperature` reach
no call. READ: `extractor: str = Field(default="deepseek-r1:32b", …)` and
`temperature: float = Field(default=0.0, …)` are declared, and every extraction site uses
`model=MODEL, … options={'temperature': 0}`.

**Defaults declared twice.** `pdf_parser` keeps `_VISION_MODEL`, `_VISION_NUM_PREDICT` and
`_VISION_NUM_CTX` beside the spec fields `vision_model`, `vision_num_predict` and `vision_num_ctx`.
`screener` keeps `DEFAULT_PRIMARY_MODEL` and `DEFAULT_VERIFICATION_MODEL` beside
`ScreeningModels.primary` and `.verification`. Under S3a each default lives only in the spec model.

### Cloud sites

| # | site | model | parameters (all literals) | system message |
|---|---|---|---|---|
| C-a | `OpenAIExtractor.extract_paper` → `self.client.chat.completions.create` | spec `cloud_models.openai.model`, else const `_DEFAULT_MODEL = "o4-mini-2025-04-16"` | `reasoning_effort='high'`, `response_format={'type': 'json_object'}` | literal, sent as a `"role": "system"` message |
| C-b | `AnthropicExtractor.extract_paper` → `self.client.messages.create` | spec `cloud_models.anthropic.model`, else const `_DEFAULT_MODEL = "claude-sonnet-4-6"` | `max_tokens=16000`, `thinking={'type': 'enabled', 'budget_tokens': 10000}` | literal, sent as `system=` |

---

## P2 — Specifying the instrument (R56, R61)

### I11 — VERIFIED, with a refinement

The last engine-owned view of a request is the kwargs of the `_client.chat` submission.
`ollama_chat` passes `**kwargs` straight through: READ, "Passed through to ollama.Client.chat()
(format, options, think, etc.)". The literal JSON body is assembled inside the library. MEASURED
for ollama-python 0.6.1, the installed version, by reading `ollama._client.Client.chat`:

```python
      json=ChatRequest(
        model=model,
        messages=list(_copy_messages(messages)),
        tools=list(_copy_tools(tools)),
        stream=stream,
        think=think,
        logprobs=logprobs,
        top_logprobs=top_logprobs,
        format=format,
        options=options,
        keep_alive=keep_alive,
      ).model_dump(exclude_none=True),
```

`ChatRequest.model_fields` = `model, stream, options, format, keep_alive, messages, tools, think,
logprobs, top_logprobs`. **There is no `truncate` field**, and `Client.chat`'s signature has no
`truncate` parameter, so passing one through `ollama_chat` would raise `TypeError` (R62).
`exclude_none=True` is why R61 requires every resolver option to be explicit.

### What a capture at `_client.chat` sees and needs

| item | seen at the boundary? |
|---|---|
| model, messages (system and user), format, options, think, keep_alive | yes: every kwarg the engine supplies |
| `stream`, `tools`, `logprobs` | no: library defaults, added below the boundary; constant across the engine |
| the literal JSON body | no (R61: no transport capture) |

**Requirements the instrument must meet (Phase 2):**

1. **Patch `engine.utils.ollama_client._client`** with an object whose `.chat(**kw)` records `kw`
   and returns a well-formed response. A capture that raises on the first call, as
   `test_eligibility::_capture` does, cannot follow a site that makes two calls (sites 5→6, the
   elicitation retry).
2. **Answer `_client.show`.** `ollama_chat` calls `_check_input_fits` before `_client.chat`, and
   `effective_ceiling` → `n_ctx_train` → `_client.show(model)`. The capture must return a
   `modelinfo` with a `*.context_length`. It must also give the post-call guard
   (`_check_input_was_read`) a `prompt_eval_count` inside `[chars × RATIO_DROP, ceiling)`. That is
   better than stubbing both guards out as DISCOVERY-01 did: the guards run in production, so the
   instrument should run them too.
3. **Record, per call:** site id, the stage name the resolver assigned, and the full kwargs. It then
   asserts, for every option key, that the value equals the resolver's `StageConfig` for that stage.
   This is the gate check "originates in the spec or a declared default", evaluated as equality
   with the resolver's output, which itself reads only the spec model.
4. **Cloud sites (C-a, C-b):** patch `self.client` on the extractor instance and record the kwargs
   of `chat.completions.create` and `messages.create`. That is their engine-owned boundary, the
   equivalent of `_client.chat`.
5. **The bypass calls need nothing.** `ollama.ps()` and the `/api/tags` poll carry no model options
   and send no prompt, so neither is a configuration channel. The instrument should still refuse
   them if reached, as the suite fence does for services, because a live GET is still a live call
   (inventory row I14).

---

## P3 — Review Spec model and the frozen surfaces

**READ.** `class _SpecModel(BaseModel)` carries `model_config = ConfigDict(extra="forbid")` at every
level. `review_id: str = Field(pattern=r"^[a-z][a-z0-9_]*$", …)` is required.

**Top-level blocks the model declares:** `review_id, title, version, authors, date, prospero_id,
pico, search_strategy, screening_models, ft_screening_models, extraction_models, eligibility,
low_yield_threshold, auditor_model, unpaywall_email, institutional_proxy_pattern,
pdf_quality_check, cloud_models, pdf_parsing, distribution_monitor`.

**Blocks the live spec sets** (MEASURED, `model_fields_set`): `authors, date, eligibility,
ft_screening_models, institutional_proxy_pattern, low_yield_threshold, pdf_quality_check, pico,
prospero_id, review_id, screening_models, search_strategy, title, unpaywall_email, version`. The
following take pydantic defaults: `extraction_models` (so `elicitation: False`), `cloud_models`
(`None`), `auditor_model` (`None`), `pdf_parsing` and `distribution_monitor`.

**Hashes.** READ: `screening_hash()` = `_canonical_hash(self.eligibility.model_dump())`, where
`_canonical_hash` is `json.dumps(data, sort_keys=True, default=str)` → SHA-256. There is no
whole-spec hash. `review_runs.review_spec_hash` is written by `run_pipeline` as
`spec.screening_hash() + cb.semantic_hash`, a concatenation that its own comment calls "read by
nothing … deprecated".

### I12 — per renderer (MEASURED, two disposable probes)

**Access probe.** Each surface was rendered through a proxy that logs every top-level attribute read
from the spec. The proxy is transparent: **all 14 hashes still equal `FROZEN`**. **Perturbation
probe.** One spec block at a time was changed and every surface re-hashed.

| surface | renderer | top-level spec fields read | moved by |
|---|---|---|---|
| H1 | `screening_adjudicator._build_decision_criteria` | `eligibility` | — |
| H3 | `ft_screening_adjudicator._build_ft_decision_criteria` | `eligibility` | — |
| H6 | `screening_adjudicator._build_edge_case_guidance` | `eligibility` | — |
| H7 | `ft_screening_adjudicator._build_ft_edge_case_guidance` | `eligibility` | — |
| H4 | `screening_adjudicator._build_reference_content` | `eligibility`, **`pico`** | pico |
| H5 | `ft_screening_adjudicator._build_ft_reference_content` | `eligibility`, **`pico`** | pico |
| P1, P2 | `screener._build_prompt` (primary, verifier) | `eligibility`, **`pico`** | pico |
| P3, P4 | `ft_screener.build_ft_screening_prompt`, `build_ft_verification_prompt` | `eligibility`, **`pico`** | pico |
| R1 | `screener.screen_paper` (primary) request `{format, messages}` | `eligibility`, **`pico`**, **`title`**, (`screening_models`) | pico, **title** |
| R2 | `screener.screen_paper` (verifier) | `eligibility`, **`pico`**, (`screening_models`) | pico |
| R3, R4 | `ft_screener.ft_screen_paper`, `ft_verify_paper` | `eligibility`, **`pico`**, (`ft_screening_models`) | pico |

The access probe renders all four R requests on each R key, so its read sets for R1–R4 are the
union; the "moved by" column is per surface.

**Anchors (READ).** pico: `f"Population: {spec.pico.population}\n"` in `screener.py` and
`ft_screener.py`, and `f"  Population:   {spec.pico.population}"` in
`ft_screening_adjudicator.py`. title: `render.messages(stage, user_prompt, review_title=spec.title)`
in `screener.screen_paper`; `eligibility_render.system_message` substitutes the title only
`if _TOPIC_PLACEHOLDER in template`. Of the four request stages, only the abstract-primary template
has that slot, which is why title moves R1 alone. The `ft_screener` requests also pass
`review_title=spec.title`, but their templates carry no slot. `screening_models` and
`ft_screening_models` are read on the request path (model, temperature, think), but R1–R4 hash
`{format, messages}` only, so perturbing them moves nothing.

**The P3 question answered.** Adding top-level `arms`, `extraction_models` or `cloud` blocks
**moves no frozen surface**. No renderer reads any of them, and `screening_hash()` does not cover
them. Adding a field **inside** `pico`, or editing `title`, would move surfaces while leaving
`screening_hash()` unchanged. That is row **C17** (R60).

---

## P4 — Write-site census

**Method — MEASURED.** A disposable AST walk collected every string constant and f-string in
`engine/`, `scripts/` and `analysis/` containing `INSERT INTO | UPDATE | DELETE FROM | REPLACE INTO`
followed by one of the tables below. Callers of each write method were found by an AST-level name
search. Migrations (`engine/migrations/`) are excluded: they run once.

**Path legend.** **F** = on the path a freshman smoke run or Run 7 executes (extraction, cloud
extraction and audit on the corpus; no screening). **S** = screening path (S4, junior). **H** =
human round-trip (session 12). **L** = legacy, one-off script, or maintenance. **—** = no
production writer.

| table | writer (function) | op | path |
|---|---|---|---|
| `extractions` | `ReviewDatabase.add_extraction_atomic`, called by `extractor` and `elicitation.pipeline` | INSERT | **F** |
| | `ReviewDatabase.add_extraction` | INSERT | — (no caller outside `database.py`) |
| | `auditor.check_low_yield` | UPDATE | **F** (audit) |
| | `reset_for_reextraction` (no caller outside `database.py`); `extraction_cleanup.cleanup_stale_extractions`; `scripts/reextract_all.py` | DELETE | L |
| `evidence_spans` | `add_extraction_atomic` | INSERT | **F** |
| | `ReviewDatabase.update_audit`, called by `auditor` | UPDATE | **F** (audit) |
| | `add_evidence_span`, `cleanup_orphaned_spans`, `reset_for_reextraction` (no external caller) | INSERT/DELETE | — |
| | `reset_for_reaudit` (`scripts/reextract_all.py`), `cleanup_stale_extractions`, `extraction_validator.normalize_categorical_values` | UPDATE/DELETE | L |
| | `audit_adjudicator.import_audit_review_decisions`, `._import_legacy_format`; `human_review._apply_audit_decisions`, `.bulk_accept` | UPDATE | H (legacy importers; S2 retires them) |
| `cloud_extractions` | `CloudExtractorBase.store_result` | INSERT | **F** (cloud arms) |
| `cloud_evidence_spans` | `CloudExtractorBase.store_result` | INSERT | **F** |
| | `cloud.schema.init_cloud_tables` (rebuild branch) | INSERT/UPDATE | L (runs on each extractor construction; INFERRED, not measured: its rebuild branch should not fire on live since 018 closed C10) |
| | `scripts/backfill_cloud_spans.py`, `scripts/reparse_cloud_spans.py` | INSERT | L |
| `abstract_screening_decisions` | `ReviewDatabase.add_screening_decision` | INSERT | S |
| `abstract_verification_decisions` | `ReviewDatabase.add_verification_decision` | INSERT | S |
| `ft_screening_decisions` | `ReviewDatabase.add_ft_screening_decision` | INSERT | S |
| `ft_verification_decisions` | `ReviewDatabase.add_ft_verification_decision` | INSERT | S |
| `abstract_screening_adjudication` | `screening_adjudicator._apply_abstract_decisions` | INSERT/UPDATE | H/S |
| `ft_screening_adjudication` | `ft_screening_adjudicator._apply_ft_decisions` | INSERT | H/S |
| `workflow_state` | `workflow.complete_stage`, `bypass_stage`, `reset_stage`, `ensure_workflow_table` | UPDATE/INSERT | F and S (stage stamps) |
| `field_events` (+ `field_event_against`, `field_event_against_decisions`) | `events.write_field_event` | INSERT | **— today**; F from session 9 |
| `paper_events` | `events.write_paper_event` | INSERT | **— today** (only caller: migration 017); F from session 9 |
| `parsed_text_refs` | none outside migration 017 | — | — (a new parse writes no ref; see §5) |
| `review_runs` | `scripts/run_pipeline._start_review_run`, `._finish_review_run` | INSERT/UPDATE | L/F (only via `run_pipeline`) |
| `arms` | `events.register_arm`, `retire_arm`, `repin_arm_configuration` | INSERT/UPDATE | — today (017 and tests) |
| `admin_resets` | `ReviewDatabase.admin_reset_status` (`CREATE TABLE IF NOT EXISTS` on demand; absent on live) | INSERT | L |

**Census answer (INFERRED from the table).** Today a freshman smoke run would write the legacy
tables `extractions`, `evidence_spans`, `cloud_extractions` and `cloud_evidence_spans`, plus
`papers.status` via `update_status` and `workflow_state`. The event writer has **no production
caller**. Which tables the freshman path writes is therefore decided by session 9's move of the
extractor to the event writer, not by anything on disk now.

**I2 — VERIFIED (PRAGMA, `mode=ro`).** `field_events` and `paper_events` both carry
`run_id INTEGER` and `run_marker TEXT`. `pragma foreign_key_list` shows **no FK on `run_id`** in
either (field_events: `arms`, `papers`, self; paper_events: `papers`, self). `paper_events` has 190
rows, all `run_id IS NULL`, `run_marker = 'pre-manifest'`. `field_events` has 0 rows. There is no
CHECK on either column (016's DDL: `run_id INTEGER,` `run_marker TEXT,`).

---

## P5 — `review_runs`

**I4 — VERIFIED.** MEASURED schema: `id INTEGER PK, review_spec_hash TEXT NOT NULL,
screening_hash TEXT NOT NULL, extraction_hash TEXT, started_at TEXT NOT NULL, completed_at TEXT,
status TEXT NOT NULL DEFAULT 'running', log TEXT NOT NULL DEFAULT '[]', codebook_hash TEXT,
codebook_sha256 TEXT`. **Six rows**, 2026-02-28 → 2026-03-06, content MEASURED:

| id | screening_hash | extraction_hash | status | completed_at | log | codebook_* |
|---|---|---|---|---|---|---|
| 1, 2, 4 | `d980eae6…4ac6` | `fb5f613d…ec6` | completed | set | `[]` | NULL |
| 3 | `d980eae6…4ac6` | `fb5f613d…ec6` | **`running`** | **NULL** (a run that never finished) | `[]` | NULL |
| 5, 6 | `d980eae6…4ac6` | `41af0b0f…1ac4` | completed | set | `[]` | NULL |

No other table's SQL references `review_runs` (MEASURED over `sqlite_master`). **Writer:**
`scripts/run_pipeline.py` only, in `_start_review_run` and `_finish_review_run`. **Readers:** none
in `engine/`, `scripts/` or `analysis/`. The only other references are in tests
(`test_codebook_provenance.py`, `test_codebook_staleness.py`, `test_database.py`) and migrations
012/013.

**Decides new-table vs grow → new table** (P9a). `review_runs` has no per-stage configuration, no
digest, no link, a hash (`review_spec_hash`) its own writer calls deprecated, and one row stuck at
`running`. Growing it would give a manifest table six rows that are not manifests. Under R25/R31
it stays as read-only telemetry, and `run_pipeline` stops writing it when the manifest lands.

---

## P6 — The arm registry

**I7 — VERIFIED, qualified.** MEASURED schema: `arm_name TEXT PK, arm_kind TEXT NOT NULL,
configuration_json TEXT NOT NULL DEFAULT '{}', configuration_marker TEXT, registered_at TEXT NOT
NULL, retired_at TEXT`. Seeded rows: `local`, `anthropic_sonnet_4_6`, `openai_o4_mini_high`, each
`arm_kind='model'`, `configuration_json='{}'`, `configuration_marker='not recorded
(pre-manifest)'`, `registered_at 2026-09-21T21:17:40.193732+00:00`, `retired_at NULL`.

Triggers (MEASURED from `sqlite_master`):

```sql
CREATE TRIGGER arms_configuration_frozen_once_claimed
      BEFORE UPDATE OF arm_kind, configuration_json, configuration_marker ON arms
      WHEN EXISTS (SELECT 1 FROM field_events WHERE arm = OLD.arm_name)
      BEGIN
        SELECT RAISE(ABORT,
          'arms: configuration cannot be re-pinned once the arm holds a claim (R21) — a changed configuration is a new arm (R10)');
      END
CREATE TRIGGER arms_name_frozen
      BEFORE UPDATE OF arm_name ON arms
      BEGIN SELECT RAISE(ABORT, 'arms: arm_name is immutable — claim ids embed it, so a rename orphans every claim'); END
```

With 0 field events, **the three seeded arms are editable today**, which is what R59 closes. There
is no DELETE trigger on `arms`. The FK from `field_events.arm` protects claimed arms.

**How a name resolves today.** `engine/core/effective.py::registered_arms(conn, *, kind=None,
include_retired=False)` runs `"SELECT arm_name FROM arms WHERE 1=1"`, then `AND arm_kind = ?`, then
`AND retired_at IS NULL`, then `ORDER BY arm_name`. It has no configuration lookup: the registry
routes by name only. `_arm_is_pre_manifest` compares `configuration_marker` to
`PRE_MANIFEST = "not recorded (pre-manifest)"` and feeds rule row 7.

**Third state, found in the fixtures (READ).** `tests/_event_store_fixture.py` registers arms with
`events.register_arm(conn, arm, arm_kind)`, which gives configuration `{}` and marker **NULL**:
neither pinned nor pre-manifest. R59's design must name this "unpinned" state; see P9(c).

**Storing a pinned tuple without altering the seeded rows.** Either:
1. a new nullable column `pinned_run_id INTEGER REFERENCES run_manifests(run_id)` added by
   `ALTER TABLE ADD COLUMN` (NULL on the three seeded rows), with the tuple in the existing
   `configuration_json` and `configuration_marker = 'pinned'`; or
2. no column: the tuple and the pinning run's uid both inside `configuration_json`.

Both leave the seeded rows byte-identical. Option 1 is recommended because the link is an FK that
`foreign_key_list` and the structure hash can see (P9c).

---

## P7 — The cloud path (I8, R58, R64)

**I8 — VERIFIED, qualified.**

**`--arm` consumers.** `scripts/run_cloud_extraction.py` is the only entry point:
`choices=["openai", "anthropic", "both"]`, `arms = ["openai", "anthropic"] if args.arm == "both"
else [args.arm]`, then `run_arm(...)` or `dry_run(...)`. `run_arm` constructs `OpenAIExtractor` or
`AnthropicExtractor`. Nothing else constructs either class (MEASURED). Other `--arm`/`--arms` flags
in the tree (`distribution_monitor`, `concordance`, `adjudication`, `screen2f_worker`) select
**readers**, not transfers.

**Hardcoded values (READ).** Both extractors are selected by `getattr(self.spec, "cloud_models",
None)` and fall back to `_DEFAULT_MODEL`, `_DEFAULT_COST_INPUT_PER_M` and
`_DEFAULT_COST_OUTPUT_PER_M`: OpenAI `"o4-mini-2025-04-16"`, 1.10 / 4.40; Anthropic
`"claude-sonnet-4-6"`, 3.00 / 15.00. The live spec sets no `cloud_models`, so **every live cloud
run uses the constants**. The arm name is a class constant (`ARM = "anthropic_sonnet_4_6"`)
independent of `model_string`, so a spec change of model writes into the old arm (R64). The
endpoints are SDK defaults: no `base_url` anywhere in `engine/cloud` or the script (MEASURED).
The only other reader of `spec.cloud_models` is `engine/exporters/methods_section.py`, which writes
methods text.

**Complete outbound request, per provider (R58):**

| component | OpenAI (C-a) | Anthropic (C-b) | constant or per paper |
|---|---|---|---|
| endpoint | SDK default (`api.openai.com`) | SDK default (`api.anthropic.com`) | constant |
| model | `self.model_string` | `self.model_string` | constant per run |
| system | `"role": "system"` message, literal: "You are a systematic review data extractor. Output valid JSON matching the requested schema. Be thorough and cite source text for every extracted value." | `system=` the same literal | constant |
| user turn | `prompt = self.build_prompt(parsed_text)` → `build_extraction_prompt(parsed_text, self.spec)` | same | **per paper** |
| user-turn content | the codebook-driven field blocks (tier labels, per-field definitions, values, examples) plus the paper's **full parsed Markdown**, interpolated as `{paper_text}`, **untruncated** | same | per paper |
| parameters | `reasoning_effort="high"`, `response_format={"type": "json_object"}` | `max_tokens=16000`, `thinking={"type": "enabled", "budget_tokens": 10000}` | constant |
| credentials | `OPENAI_API_KEY` (env) | `ANTHROPIC_API_KEY` (env) | not payload |

**Where the full text comes from (READ).** `CloudExtractorBase.load_parsed_text`:
`md_files = sorted(parsed_dir.glob(f"{paper_id}_v*.md"), reverse=True)`. This is a **lexical glob
resolver**, so `_v9` beats `_v10` (D1/D3). It closes in session 8 (S3e) and is not touched here
(R64).

**What is stored (READ, `store_result`).** `cloud_extractions` holds `prompt_text` (the user turn
only), token counts, reasoning trace and cost. It does **not** hold the system message, the
parameters, or the endpoint (C16).

**Description the manifest could record (INFERRED, proposed wording):**
> "Per enabled cloud arm, per corpus paper: one HTTPS request to the provider's public API
> containing a fixed system instruction, the review's extraction codebook rendered as a prompt,
> and the paper's complete parsed full text. No database content other than the paper text, no
> identifiers beyond what the paper itself contains, and no PDF bytes leave the machine."

**Second off-box path: `analysis/eval/run_cloud_strict.py`.** It has its own
`--arms` (`default="openai,anthropic"`) and literals `OPENAI_MODEL = "o4-mini-2025-04-16"` and
`ANTHROPIC_MODEL = "claude-sonnet-4-6"`. It sends the same `build_extraction_prompt` user turn,
under strict-schema contracts (OpenAI `json_schema` strict; Anthropic forced tool, thinking OFF).
Its docstring says "Writes to the eval store only. Never touches review.db." Classification under
D2-10: **frozen experiment code** (SCHEMA-EVAL-01's cloud condition B), unchanged from DISCOVERY-01
Part B. Last-run evidence (MEASURED): `data/surgical_autonomy/eval/schema_eval/`
`cloud_strict_20260728T175143Z.jsonl` (54,779 B) and `cloud_strict_20260728T175532Z.jsonl`
(84,968 B), both 2026-07-28. **Edited after its last run:** `fa4a7de` and `fa6c738`
(2026-09-09/10, spec-resolver plumbing), so the code at HEAD is not the code that produced those
files. No committed code reads its outputs (MEASURED: `cloud_strict_2026` appears in no `.py`
under `analysis/`). **Retention ruling: PI's, under R31 (R64).**

---

## P8 — Ollama service, keep_alive, digest

**I5 — VERIFIED.** `systemctl show ollama --property=Environment`:
`OLLAMA_KV_CACHE_TYPE=f16 OLLAMA_MAX_LOADED_MODELS=1 OLLAMA_NUM_PARALLEL=1
OLLAMA_FLASH_ATTENTION=true OLLAMA_KEEP_ALIVE=-1`. No engine site sends `keep_alive` (P1), so the
service value governs every call today. Server version `ollama version is 0.21.0`.

**I9 — VERIFIED from the tree; no live call made.** READ, `ollama_client.fetch_model_digest`:
"Return the SHA-256 manifest digest for `model_name` via /api/tags. The digest is not exposed on
/api/show in current Ollama versions; the canonical structured field is models[].digest on
/api/tags. This function performs the HTTP GET, filters by exact name match, and asserts the result
is a 64-char lowercase hex string." `/api/tags` lists installed models and loads none. It raises
`ModelDigestError` on any failure, with "No silent fallback to the model-name string." That
behaviour is what a manifest needs (R57). `get_model_digest` (`/api/show`, returns `None` on
failure) is the route that left C15 NULL.

---

## P9 — Proposal for the ruling

Everything in P9 is **INFERRED design**, a proposal for the architect. Names are proposals.

### (a) The manifest table

**New table `run_manifests`, one row per run**, written and committed **before** the run's first
model call. Per-stage configuration goes in **rows** of a child table, not in JSON, because S3d's
reuse key joins on `(model_digest, options_hash, prompt_hash)` per stage and has to do it in SQL.
The full canonical manifest is also stored whole, as a blob with its hash, so the record is
self-describing.

`run_manifests`

| column | type / rule | note |
|---|---|---|
| `run_id` | INTEGER PRIMARY KEY AUTOINCREMENT | INTEGER to match the existing `run_id INTEGER` on both event tables |
| `run_uid` | TEXT NOT NULL UNIQUE | uuid4, used in logs and file names |
| `review_id` | TEXT NOT NULL | |
| `run_kind` | TEXT NOT NULL CHECK IN ('extraction', 'cloud_extraction', 'audit', 'screening', 'parse', 'smoke') | open to extension by migration only |
| `engine_state` | TEXT | NULL until the freshman tag (session 10), see (f) |
| `git_commit` | TEXT NOT NULL CHECK (length(git_commit) = 40) | |
| `git_dirty` | INTEGER NOT NULL CHECK (git_dirty = 0) | refusal made structural, see (f) |
| `spec_sha256` | TEXT NOT NULL | whole-spec canonical hash (R60) |
| `screening_hash` | TEXT NOT NULL | the eligibility hash, kept for continuity; not an identity (C17) |
| `codebook_semantic_hash`, `codebook_sha256` | TEXT NOT NULL | |
| `ollama_python_version`, `ollama_server_version` | TEXT NOT NULL | R61; server version from `GET /api/version`, which loads no model |
| `cloud_enabled` | INTEGER NOT NULL CHECK IN (0, 1) DEFAULT 0 | R6 |
| `off_box_description` | TEXT, CHECK (cloud_enabled = 0 OR off_box_description IS NOT NULL) | (e) |
| `host` | TEXT NOT NULL | |
| `started_at` | TEXT NOT NULL | |
| `ended_at`, `end_status` | TEXT; `end_status` CHECK IN ('completed', 'failed', 'interrupted') | written once at the end |
| `manifest_json`, `manifest_sha256` | TEXT NOT NULL | canonical JSON of everything above plus every stage row |

**Mutability.** A trigger refuses any UPDATE except the single transition `ended_at IS NULL →
NOT NULL` together with `end_status`. DELETE is refused. A run still open at startup is reported as
`interrupted` by writing its end, never by editing its body. That follows the global
no-long-work-under-lock pattern: a stuck transient state means a crashed run.

`run_stage_configs`, one row per (run, stage)

| column | type / rule |
|---|---|
| `run_id` | INTEGER NOT NULL REFERENCES run_manifests(run_id) |
| `stage` | TEXT NOT NULL, from the resolver's closed stage list (b) |
| `arm_name` | TEXT REFERENCES arms(arm_name), NULL for stages that make no claim (preflight, parse, pdf_quality, audit) |
| `provider` | TEXT NOT NULL CHECK IN ('ollama', 'openai', 'anthropic') |
| `model_name` | TEXT NOT NULL |
| `model_digest` | TEXT, CHECK (provider <> 'ollama' OR model_digest GLOB 64 lowercase hex) |
| `options_json`, `options_hash` | TEXT NOT NULL |
| `format_schema_hash` | TEXT NOT NULL; the literal token `'none'` when the stage sends no format (R61: never NULL-by-omission) |
| `prompt_hash` | TEXT NOT NULL (R60) |
| `client_library_version` | TEXT NOT NULL |
| PRIMARY KEY | (run_id, stage) |

**The "pre-manifest" marker on existing rows.**
* **Event tables:** already `run_id NULL` with `run_marker = 'pre-manifest'` on all 190 paper
  events. Migration 020 makes this a rule (d): `run_id IS NOT NULL OR run_marker =
  'pre-manifest'`, and new rows must carry `run_id`.
* **Legacy result tables** (`extractions`, `evidence_spans`, `cloud_*`, screening, `review_runs`):
  **no new column.** Under R25 they are read-only telemetry, and the absence of `run_id` there **is**
  the pre-manifest marker. Adding a column to telemetry would be a live rewrite with no forward use
  (R31).

### (b) The resolver

**One module, proposed `engine/core/run_config.py`,** with **one public function**:

```
resolve(spec: ReviewSpec, *, stages: Iterable[str], cloud_arms: Iterable[str] = ()) -> RunConfig
```

* **Input:** the loaded `ReviewSpec` only. Every default lives in the spec model (S3a). The
  resolver has no module constants. Phase 2 deletes `extractor.MODEL`, `DEFAULT_AUDITOR_MODEL`,
  `screener.DEFAULT_*`, the `_VISION_*` duplicates and every literal temperature and `think`.
* **Output:** a frozen `RunConfig` holding one frozen `StageConfig` per stage: `stage, provider,
  model, options (dict, every key explicit), think, format_schema, keep_alive, arm`. Call sites
  receive a `StageConfig` and pass its fields; they never construct options.
* **Closed stage list (from P1):** `abstract_primary, abstract_verifier, ft_primary, ft_verifier,
  extract_pass1, extract_pass2, extract_snippet_retry, elicit_pass1, audit_semantic, parse_vision,
  pdf_quality, preflight, cloud_openai, cloud_anthropic`. `elicit_pass1` and `extract_pass1` share
  a model by default. Each is still its own row, because its prompt differs.
* **Hashes emitted per stage, in the shape S3d consumes.** One canonical-JSON function, owned by
  the resolver and imported everywhere else ("one predicate, two programs"):
  `json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)` → SHA-256.
  * `options_hash` = H(`{"options": …, "think": …, "keep_alive": …}`): every request kwarg except
    model, messages and format.
  * `format_schema_hash` = H(format schema), or `'none'`.
  * `prompt_hash` = H(`{"format": …, "messages": [system, user]}`) of the request rendered with a
    **fixed sentinel paper text**, the same technique the frozen R-surfaces use (`"Title:
    T\n\nAbstract: A"`). This is a per-stage template identity (R60). The per-call exact request is
    recorded separately (d).
  * `model_digest` = `fetch_model_digest(model)` (R57). A failure refuses the run. Called once per
    model at manifest open.
* **keep_alive: sent per request, `-1`, and recorded.** Reasons: R61 requires every option to be
    explicit. The service value lives in a systemd drop-in that the manifest cannot see or pin, and a
    per-request value overrides the server default, so sending it makes the manifest's value the one
    in force. Sending `-1` equals the service value, so behaviour today is unchanged. Architect's
    assumption 3 ("set it … measure") is met by recording it; measuring reload cost is a smoke-run
    observation.
* **Decision the architect owes (D-1):** which options that are **absent today** become explicit.
  `seed` and `num_ctx` are absent at every non-judge site. Declaring them changes the request:
  an explicit `num_ctx` replaces the runtime's own choice, and INPUT-FIT-01's `effective_ceiling`
  reads `options.num_ctx` first. The recommendation: declare the value the runtime uses today where
  it can be measured, and leave an option out of the resolver's key set, rather than sending
  `None`, where it cannot. Either way the instrument asserts the key set per stage.
* **Out of scope proposal (D-2):** the judge (`analysis/paper1/judge.py`) has its own
  `run_config_json` and digests. Recommend leaving it on its own run record until S5c (session 9+),
  and listing it as a known exception to the session-7 gate.

### (c) Arm pinning under R10, R21 and R59

**Pinning tuple** = canonical JSON of `{arm_kind, provider, model_name, model_digest, stages:
{stage: {options_hash, format_schema_hash, prompt_hash}}, codebook_semantic_hash,
client_library_version}`, stored in `arms.configuration_json`, with `configuration_marker =
'pinned'` and `pinned_run_id` (P6 option 1). Equality is **hash equality of the canonical JSON**.

**Four arm states:**

| state | marker | configuration | reachable how |
|---|---|---|---|
| pre-manifest | `not recorded (pre-manifest)` | `{}` | the three seeded arms; never pins (R59) |
| unregistered | (no row) | — | a spec arm not yet in the registry |
| unpinned | NULL | `{}` | `register_arm` with no configuration (today's fixtures) |
| pinned | `pinned` | tuple | first manifest that names the arm |

**Manifest-open refusals, all before the first call:**
1. The spec names an arm whose row is **pre-manifest** → refuse (R59).
2. A **pinned** arm whose resolved tuple differs from the stored tuple → refuse, naming the
   differing keys (R10).
3. A **retired** arm → refuse (R21).
4. An **unregistered** or **unpinned** arm → register and pin **inside the manifest's
   transaction**, so a manifest never exists without its pins, or pins without their manifest.

**The widened trigger** (replacing `arms_configuration_frozen_once_claimed`):

```sql
BEFORE UPDATE OF arm_kind, configuration_json, configuration_marker, pinned_run_id ON arms
WHEN EXISTS (SELECT 1 FROM field_events WHERE arm = OLD.arm_name)
  OR OLD.configuration_marker IN ('pinned', 'not recorded (pre-manifest)')
```

This makes pre-manifest arms immutable too: "never pin" becomes enforced, not a convention. A
`retired_at` UPDATE stays allowed because it is not in the column list.

**Event-writer refusals (Phase 2, in `events.write_field_event`):**
* a **claim** (non-reviewer event) on a pre-manifest arm → refuse (R59);
* a claim on a retired arm → refuse (R21, not enforced today: §"Ledger corrections");
* a claim whose `run_id` is NULL, or names a manifest that did not pin this arm → refuse (R10: "a
  write whose configuration does not match its declared arm");
* **reviewer events on pre-manifest claims stay allowed.** They are row 7's exits (R20), which is
  how R59 coexists with the reachability `events.py`'s docstring defends (§"Ledger corrections").

Each refusal becomes one constructed-fixture test. The fixture helper `register_arm(conn, arm,
arm_kind)` creates **unpinned** arms, so existing fixtures keep working if the helper pins through a
test manifest. **Tests are callers:** there are 29 `write_field_event` references across 8 test
files (MEASURED by `grep -c`), and every one needs a `run_id` once (d) lands.

### (d) run_id: which tables gain a foreign key

| table | FK? | why |
|---|---|---|
| `field_events` | **yes** | the claim store from session 9 (S2) |
| `paper_events` | **yes** | the state store (S3h) |
| `run_stage_configs` | yes (new) | child of the manifest |
| **`run_calls`** (new, proposed) | yes, NOT NULL | one row per model or cloud call: `run_id, stage, paper_id, request_sha256` (the exact request kwargs, canonical), response metadata (`prompt_eval_count`, token counts, cost, finish reason). Holds R58's per-call payload hash for cloud **and** local calls. Today local calls have no per-call record at all, and the field-event store has no place for token and cost telemetry |
| `arms` | `pinned_run_id` (c) | |
| `extractions`, `evidence_spans`, `cloud_extractions`, `cloud_evidence_spans` | **no** | R25 telemetry; R31: no forward use once session 9 moves the writers |
| six screening tables, `workflow_state`, `review_runs` | **no** | screening moves to events under S4d; `review_runs` is superseded by (a) |

**On the event tables, the recommendation is a rebuild, not a trigger.** A real `REFERENCES
run_manifests(run_id)` plus `CHECK (run_id IS NOT NULL OR run_marker = 'pre-manifest')` requires
rebuilding both tables, using the 019 pattern (temporary name, pinned column list, triggers
recreated, marker last). A trigger-only FK avoids the rebuild but cannot be seen by
`foreign_key_list` or `schema_structure_hash` (R41's blindness family). `paper_events` has already
been rebuilt once this way (019, 190 rows, content hash unchanged), and `field_events` has 0 rows. A
`BEFORE INSERT … WHEN NEW.run_id IS NULL → RAISE` trigger is created **after** the copy, so the 190
seeded rows are carried and new rows are refused. The writer's default `run_marker="pre-manifest"`
is removed in the same commit (§"Ledger corrections").

**Decision the architect owes (D-3): cloud before session 9.** `store_result` writes
`cloud_extractions` and `cloud_evidence_spans`, not events. If R22-U4 lifts at the end of session 7,
a cloud run would write rows with no `run_id`, failing the session-7 gate "`run_id` on every row a
run writes". **Recommendation:** keep U4 in force until session 9 moves `store_result` onto the
event writer and `run_calls`, rather than adding `run_id` to two telemetry tables.

**Decision the architect owes (D-4): human reviewer events.** S3b says "every new decision". A
session-12 reviewer event has no model run. Recommendation: a reviewer session opens a manifest of
`run_kind 'review_session'` with no stage rows, so the rule "new rows carry `run_id`" has no
exception. Only `run_kind`'s CHECK list differs.

### (e) Cloud opt-in

**Spec shape (proposed).** A top-level `arms` list carrying every arm, local and cloud, per R12 and
R64, plus a top-level `cloud` switch. `cloud_models` is retired: the live spec does not set it, its
two extractor readers and `methods_section` move to `arms`, and `extra='forbid'` then rejects the
old key rather than ignoring it.

```yaml
cloud:
  enabled_arms: []            # default: empty — nothing leaves the machine (R6)
arms:
  - name: local_deepseek_r1_32b_v1
    kind: model
    provider: ollama
    stages: {extract_pass1: {model: deepseek-r1:32b, temperature: 0.0, think: true},
             extract_pass2: {model: deepseek-r1:32b, temperature: 0.0, think: false}}
  - name: anthropic_claude_sonnet_4_6_v1
    kind: model
    provider: anthropic
    model: claude-sonnet-4-6
    parameters: {max_tokens: 16000, thinking_budget_tokens: 10000}
    cost_per_m: {input: 3.00, output: 15.00}       # required for a cloud arm; no default
```

* **Default off.** `cloud.enabled_arms` defaults to `[]`. A cloud arm may be declared and still not
  run.
* **CLI-vs-spec rule.** `--arm` names **spec arm names** (the `openai`, `anthropic` and `both`
  choices retire). The run refuses before its first call unless the CLI set is non-empty **and** a
  subset of `cloud.enabled_arms`. If the spec enables none, any `--arm` refuses. A spec-enabled arm
  not named on the CLI does not run.
* **Prices and model names:** declared per arm in the spec, **required**, with no module default.
  A price in a module constant is the C5 defect. `_DEFAULT_*` in both extractors is deleted.
* **What the manifest records:** `cloud_enabled`, `off_box_description` (P7 wording), and per cloud
  stage a `run_stage_configs` row whose `prompt_hash` covers the system message, the user-turn
  template with the sentinel text, and the parameters (R58's template hash). The provider endpoint
  (`client.base_url`) and SDK version go in `client_library_version` and `manifest_json`.
* **Per call:** `run_calls.request_sha256` over the complete outbound kwargs (R58). **R58's
  column-or-table question → a table (`run_calls`)**, because the same record serves local calls
  and because `cloud_extractions` is telemetry (d). `cloud_extractions.prompt_text` keeps being
  written only for as long as `store_result` writes that table (until session 9).

### (f) Refusal rules at manifest open

| condition | action |
|---|---|
| working tree dirty (`git status --porcelain` non-empty) | **refuse**; `git_dirty` CHECK = 0 makes it structural |
| HEAD untagged | **allow**; `engine_state` NULL until session 10; `git_commit` always recorded |
| any digest unobtainable (`ModelDigestError`) | refuse |
| codebook missing or failing to load | refuse |
| CLI cloud set not a subset of `cloud.enabled_arms` | refuse (e) |
| arm pre-manifest / pinned-mismatch / retired | refuse (c) |
| a `run_manifests` row still open at start | record it `interrupted`, then proceed |

Architect's proposal (record commit and dirty flag, refuse dirty, allow untagged) is adopted as
written. One addition: `data/` is gitignored, so a dirty `data/` does not dirty the tree. The
manifest's input identity for data (codebook hash, parsed-text hashes) is what covers it.

### (g) Migration 020

* **Self-contained (R35).** Every DDL string, CHECK list, the stage vocabulary and the
  `'not recorded (pre-manifest)'` literal are declared in the module. The resolver imports the
  stage list from a constants module, and a test asserts the two agree (Step 4 rule 10).
* **Contents:** CREATE `run_manifests`, `run_stage_configs`, `run_calls` and their triggers; `ALTER
  TABLE arms ADD COLUMN pinned_run_id` (ALTER on both fresh and live, so column order agrees — the
  C14 lesson); DROP and CREATE the widened arms trigger; rebuild `field_events` and `paper_events`
  under temporary names with the FK and CHECK, pinned column lists, `INSERT … SELECT` of every
  column, rename, recreate the append-only triggers and the new insert trigger. There is **no
  `executescript`** inside the transaction.
* **One transaction, marker last, ROLLBACK on any exception** (the 017 template).
  **Idempotency by postcondition (R44):** `foreign_key_list(paper_events)` names `run_manifests`,
  the three tables exist, and the arms trigger's SQL names `'pinned'`.
* **Rehearsal then live**, on a verified copy, then through `runner.run(db_path)` after an
  `auto_backup`, bracketed by fingerprints, with a new record committed.
* **Split: recommended. Session 7 should split into 7a (resolver, instrument, refusals, manifest
  writer and migration 020 in the tree, green on fresh databases and fixtures) and 7b (the
  rehearsal and live write of 020 in a fresh session after 10:00 UTC), as R51 did for session 6.**

### (h) G3 impact

* **Structure (R53 i).** `schema_structure_hash` **changes on live**: three new tables, a new
  `arms` column, new FKs on both event tables. Fresh and live are expected to stay equal, with
  `structure_differences(fresh, live) = 0`, because both reach 020 through the same module. A new
  record of reference is committed at 7b.
* **Textual (R53 ii).** Live-after must equal rehearsal-after. Fresh-vs-live textual equality stays
  **not** a gate: 020's rebuild quotes names, as 018/019's did (R53 F1).
* **Postcondition read-backs (R53 iii pattern)**, needed because the structure hash sees no CHECK
  and no trigger (R41): (1) both event tables' `run_id`/`run_marker` CHECK text from
  `sqlite_master`; (2) the insert trigger refusing `run_id IS NULL`; (3) the widened arms trigger
  naming `'pinned'`; (4) `run_manifests`' `git_dirty = 0` and `off_box_description` CHECKs.
* **Content.** `paper_events` must keep its per-table content hash: same 190 rows, same values,
  same column order (the rebuild pins the list), as 019 did. `field_events` stays empty. `arms`
  gains a NULL column on three rows, so its content hash **changes** (the row tuple grows) while
  the values of the existing columns do not. That is the expected table-level difference, to be
  read column-wise, as `schema_migrations` was in 6b.
* **C14.** Physical column order on `arms` after `ADD COLUMN` is the same on fresh and live. The
  rebuilt event tables take 020's declared order on both.

---

## 5. New inventory candidates (for tabulation; C15–C17 already ruled)

| class | problem, one line | evidence anchor |
|---|---|---|
| A | `write_field_event` does not refuse a claim on a **retired** arm, so R21's "accepts no new claims" is enforced nowhere | `retire_arm` docstring "R21: retiring means the arm accepts no new claims" vs the refusal list in `write_field_event` |
| C | `write_field_event` and `write_paper_event` default `run_marker="pre-manifest"`, so a run-less write is silently marked pre-manifest | `run_id=None, run_marker="pre-manifest"` in both signatures |
| C | Client libraries unpinned: `ollama`, `openai`, `anthropic` are bare tokens in `requirements.txt`; installed 0.6.1 / 2.24.0 / 0.84.0 | `requirements.txt` |
| C | Cloud arm name is a class constant independent of the model sent (R64 records the ruling; this is the row) | `ARM = "anthropic_sonnet_4_6"` beside `self.model_string = anth_cfg.model` |
| C | Defaults declared twice, as a spec-model default and a module constant (`_VISION_*`, `screener.DEFAULT_*`, `DEFAULT_AUDITOR_MODEL` beside `auditor_model: Optional[str] = … "Defaults to gemma3:27b if not set."`) | P1 "Defaults declared twice" |
| D | A new parse writes no `parsed_text_refs` row: only migration 017 writes the table, so the S3e resolver's reference store does not grow | P4: `parsed_text_refs` has no writer outside 017 |
| F | `review_runs` row 3 is `status='running'`, `completed_at` NULL since 2026-03-01: a crashed run no process reconciles | P5 |
| I | Four `judge_runs` rows carry the tag `'gemma3:27b'` in `judge_model_digest`; three carry the SHA-256. The column mixes identity kinds | P1 digest paragraph |

---

## Session facts at close

* Gate re-run at close: 549 + 662 + 407 + 529 + 417 = **2,564** passed, deselects 0/0/10/6/1 = **17**. Unchanged. Ollama `NRestarts=0`.
* `db_fingerprint --compare`: exit 0 at open, at both stops, and at close.
* No file under `engine/`, `scripts/`, `review_specs/` or `data/` changed.
