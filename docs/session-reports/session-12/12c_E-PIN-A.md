# 12c E-PIN — Phase A (delta)

Session 12c, CC session `bbd7293d`, 2026-10-01. Brief 12c-OPEN Step 3, as amended by rulings
12c-OPEN-R1 and 12c-OPEN-R2.

**Read-only.** No database write, no manifest, no model call and no digest fetch. The only files
written are this report and the two extracted 12a reports below, all uncommitted. Tree at
`93e3a55`, which is `77fe1a2` plus the item-0c plan addendum; no code differs from `77fe1a2`.

**Sources cited (R-4):**
- `docs/session-reports/session-12/12a_ELICIT-A.md`: 12a-ELICIT-A, extracted verbatim from
  `~/claude-session-archive/2eb166e9-ff84-4140-9001-00929be1254e.jsonl`, 0-based line index 957
  (1-based line 958); 11,198 chars; body sha256
  `da97b78c05a9794c07c0fd1c2085ac8cc0bed1cc3c35f7deac200a3d9c964a80`; written at `198a349`.
- `docs/session-reports/session-12/12a_ROWS-A.md`: 12a-ROWS-A, same archive, 0-based index 748
  (1-based line 749); 11,425 chars; body sha256
  `dcb3f26f274748c8a803583b78283b137d0fa3fddc3ae1de930d99f2c258f70a`; written at `198a349`.

Under I9 it is confirmed transcript-only. Under `docs/`, "ROWS-A" appears only in
`ENGINE_REFACTOR_PLAN.md`'s log text.

The 12c-OPEN stop report gave "line 957". That is the 0-based index; the extracted file headers
give both numbers.

---

## A. The Phase A read: 12a-ELICIT-A §2.3 and closing-list item 2 (in place of "§2.4", per R-2)

The report's §2.4 is "`run_calls` stages". Its pin material is §2.3 and closing-list item 2,
quoted verbatim from `12a_ELICIT-A.md`.

> **2.3 Pin and `extraction_digest`.**
> - `pin_tuple` keys `stages` by stage name, using `{"model", "model_digest", "options_hash", "format_schema_hash", "prompt_hash"}` for each stage `if r.arm_name == arm_name`. `_LOCAL_EXTRACTION_STAGES` includes `"elicitation_pass1"`.
> - **The pin tuple contains no spec hash.** So turning elicitation on changes the pin **through the set of stages hashed**: the key `elicitation_pass1` replaces `extract_pass1` and carries a different `prompt_hash`. It does not change it through the spec bytes. The new value can only be known by computing it, which I didn't do.
> - Live has no pinned `local_deepseek_r1_32b` (only the 3 pre-manifest arms), so Run 7 pins fresh and nothing refuses. That matches v70's "a different pin is then expected — record both".
> - `extraction_stages(spec)` returns `("elicitation_pass1", "extract_pass2", "extract_retry_snippet")` when elicitation is on. `extraction_digest` raises `StageNotInRun` if `stages[0]` isn't declared, then checks each declared stage's **model digest only** against the pin.
> - Order in the pipeline: `select_for_extraction` (read-only) runs in `_stage_extract` *before* `verify_extraction_run`. The check comes before any model call, not before selection.

> 2. **The pin doesn't cover two prompt templates.** Neither the Pass-2 priming wrapper (`build_pass2_priming_message`) nor the attempt-2 feedback template feeds any stage's `prompt_hash`, because `extract_pass2` hashes `pass2_messages(prompt, "R")`. An edit to either would leave the pin and every stage hash unchanged; only `git_commit` would show it.

**I8 result: holds for every quote.** `git diff --stat 198a349 77fe1a2` per file:

| file | diff | the quoted definitions, compared by AST source segment at both trees |
|---|---|---|
| `engine/core/run_manifest.py` | 162+ / 7− (E-STATE, C47, E-EXEC) | `pin_tuple`, `_LOCAL_EXTRACTION_STAGES`, `extraction_digest`, `StageNotInRun`: **IDENTICAL** |
| `scripts/run_pipeline.py` | 44+ / 9− (C40, C47, E-EXEC) | `_stage_extract`, `_open_run_manifest`: **IDENTICAL** |
| `engine/agents/extractor.py` | no change | — |
| `engine/core/effective_config.py` | no change | — |
| `engine/elicitation/prompts.py` | no change | — |
| `engine/core/selection.py` | no change | — |

Every quote stands at HEAD. Q1 is answered from them and confirmed against source below.

---

## Q1. The pin: what is hashed

`pinned_sha256 = sha256_canonical(pin_tuple(...))`, in `open_run` (`engine/core/run_manifest.py`):

```python
    for name in named_arms:
        tup = pin_tuple(spec, name, resolved, codebook.semantic_hash, libs)
        sha = sha256_canonical(tup)
```

`pin_tuple` (`run_manifest.py:204`):

```python
    arm = spec.arm(arm_name)
    stages = {
        key: {"model": r.config.model, "model_digest": r.model_digest,
              "options_hash": r.options_hash,
              "format_schema_hash": r.format_schema_hash,
              "prompt_hash": r.prompt_hash}
        for key, r in sorted(resolved.items()) if r.arm_name == arm_name
    }
    lib = {"ollama": "ollama"}.get(arm.provider, arm.provider)
    return {
        "arm_kind": _REGISTRY_KIND[arm.kind],
        "provider": arm.provider,
        "model": arm.model,
        "stages": stages,
        "codebook_hash": codebook_hash,
        "client_library": {lib: libraries.get(lib)} if lib else {},
    }
```

**Which stages carry the arm.** `_stage_arm` gives `spec.extraction_models.arm` only to
`_LOCAL_EXTRACTION_STAGES = ("extract_pass1", "extract_pass2", "extract_retry_snippet",
"elicitation_pass1")`, and gives cloud stages their own arm. `audit` and `preflight:*` carry
`arm_name None`, so they are **not in the local arm's pin**.

**Serialization** (`effective_config.py:94–100`):

```python
def canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

def sha256_canonical(obj: Any) -> str:
    return hashlib.sha256(canonical_json(obj).encode("utf-8")).hexdigest()
```

Key order is therefore sorted, not declaration order.

**`options_hash`** (`effective_config.py:181`) covers options, think, keep_alive and the recorded
values:

```python
        return sha256_canonical({
            "options": dict(self.options),
            "think": self.think if "think" in self.sent_keys else UNSENT,
            "keep_alive": self.keep_alive if "keep_alive" in self.sent_keys else UNSENT,
            "recorded": dict(self.recorded),
        })
```

`format_schema_hash` is `sha256_canonical(_thaw(self.format))` when a format is sent, else
`"none"`.

**Not in the tuple:** the spec hash, the git commit, the engine state, and any `run_calls` data.
This matches §2.3.

## Q2. `prompt_hash` per declared stage, both configurations

`prompt_hash` (`effective_config.py:455`):

```python
    messages = render_messages(stage, spec, codebook_path=codebook_path)
    body = {"messages": messages,
            "format": _thaw(cfg.format) if "format" in cfg.sent_keys else "none"}
    if cfg.provider != "ollama":
        body["parameters"] = dict(cfg.options)
    return sha256_canonical(body)
```

`render_messages` (`effective_config.py:410`) uses the sentinels
`SENTINEL_TEXT = "Title: T\n\nAbstract: A"` and
`SENTINEL_PAPER = {"title": "T", "abstract": "A", "id": 1}`:

```python
    if stage in ("extract_pass1", "extract_pass2", "extract_retry_snippet"):
        from engine.agents import extractor as ex
        prompt = ex.build_extraction_prompt(SENTINEL_TEXT, spec, codebook_path)
        if stage == "extract_pass1":
            return ex.pass1_messages(prompt)
        if stage == "extract_pass2":
            return ex.pass2_messages(prompt, "R")
        return ex.retry_snippet_messages("f", "v", SENTINEL_TEXT)
    if stage == "elicitation_pass1":
        from engine.elicitation import pipeline as el
        return el.pass1_messages(el.sentinel_pass1_prompt(spec, codebook_path))
```

`audit` renders `build_audit_messages(span, field_type="text") +
build_audit_messages(span, field_type="categorical")`. `preflight` renders
`PREFLIGHT_MESSAGES`.

The declared stages for `--skip-to extract` are `extract_pass1` (or `elicitation_pass1`),
`extract_pass2`, `extract_retry_snippet`, `audit` and `preflight`. The table gives the hashes
measured at HEAD by Q9's pure computation.

| stage | elicitation **false** | elicitation **true** | what enters the render |
|---|---|---|---|
| `extract_pass1` | `c0083f30…4f34` | not declared | `pass1_messages`'s system literal (`extractor.py:203`) + `build_extraction_prompt(SENTINEL_TEXT)` (`extractor.py:119`: codebook field blocks via `_build_field_block`, the Instructions literal, the sentinel as paper text) |
| `elicitation_pass1` | not declared | `36179e6c…d56f` | `SYSTEM_PASS1` (`prompts.py:39`) + `build_pass1_prompt(build_unit_map(0, SENTINEL_TEXT), cb.raw, field_names)` (`prompts.py:181`). **`feedback` is the default `""`**, so the attempt-2 block never enters |
| `extract_pass2` | `a4f3cb35…8e78` | `a4f3cb35…8e78` (**same**) | `pass2_messages`'s system literal + `build_extraction_prompt(SENTINEL_TEXT)` + the outer "Here is your prior analysis…" wrapper (`extractor.py:291`), with **`"R"`** as `reasoning_trace`; the format schema (`2c9ec255…`) |
| `extract_retry_snippet` | `05a98181…55aa` | `05a98181…55aa` | `retry_snippet_messages("f", "v", SENTINEL_TEXT)` (`extractor.py:380`) |
| `audit` | `d972c46d…838d` | same | `build_audit_messages` in both field-type variants (`auditor.py:80`) |
| `preflight:<model>` | `de95332f…322a` | same | `PREFLIGHT_MESSAGES` |

**R-2's added expectation is confirmed:** `extract_pass2` is hashed over
`pass2_messages(prompt, "R")`.

**What "R" stands in for:** the second argument, `reasoning_trace`, of
`pass2_messages(prompt: str, reasoning_trace: str)`. At run time that is:
- **elicitation false:** the Pass-1 thinking trace returned by `extract_pass1_reasoning`
  (`extract_paper`: `reasoning_trace, pass1_hash = extract_pass1_reasoning(...)`, then
  `extract_pass2_structured(prompt, reasoning_trace, ...)`).
- **elicitation true:** the whole priming message (`pipeline.py:350–353`):
  ```python
          priming_msg = build_pass2_priming_message(priming)
          # R224a(1): the structured pass's hash is presented_context_sha256.
          result, pass2_hash = extract_pass2_structured(
              pass2_prompt, priming_msg, spec, paper_id, cfg=cfg_p2,
  ```
  So the priming wrapper's literal text and the per-field evidence formatting it carries both sit
  inside the slot that `"R"` replaces.

The identical `extract_pass2` hash in both configurations is the measured consequence.

## Q3. Every text the extraction path can send

| text | defined where | sent by which call | literal or spec/codebook-sourced | covered by `prompt_hash`? |
|---|---|---|---|---|
| Pass-1 system ("You are a systematic review data extractor. Read the paper carefully and reason…") | `engine/agents/extractor.py:203` `pass1_messages` | `extract_pass1_reasoning` → `ollama_chat(messages=pass1_messages(prompt))` (`:237`) | literal | **yes**: `extract_pass1` renders `ex.pass1_messages(prompt)` |
| Extraction prompt (schema blocks, "## Instructions", "## Paper Text") | `extractor.py:119` `build_extraction_prompt`, `:69` `_build_field_block` | Pass 1 and Pass 2, both configurations; cloud arms via `outbound_messages` | literal template + codebook (field blocks, `canonical_absence_sentinel`) + paper text | **yes, template**: rendered over `SENTINEL_TEXT` for `extract_pass1`/`extract_pass2`/`cloud:*`; paper text is input, by design |
| Pass-2 system ("…Use your prior reasoning to produce accurate structured output…") | `extractor.py:291` `pass2_messages` | `extract_pass2_structured` (`:344`), **both configurations** | literal | **yes**: `extract_pass2` renders `ex.pass2_messages(prompt, "R")` |
| Pass-2 outer wrapper ("Here is your prior analysis of this paper:\n\n{reasoning_trace}\n\nNow output the structured extraction as JSON…") | `extractor.py:291` `pass2_messages` | same call, both configurations | literal | **yes**: the wrapper renders around `"R"` |
| **Pass-2 priming wrapper** ("Here is the evidence you cited for this paper in your first pass…" / "Now output the structured extraction as JSON… use the cited evidence above…") | `engine/elicitation/prompts.py:247` `build_pass2_priming_message` | elicited Pass 2, passed as `reasoning_trace` (`pipeline.py:350–353`) | literal | **NO**: it occupies the `"R"` slot (`effective_config.py:434` `return ex.pass2_messages(prompt, "R")`) |
| **Per-field evidence formatting** (`"### {field}  [{class}]"`, `'  [S{n}] "{text}"'`, `"  Declared inference: "`, `"  Step {i} ({basis}): "`, `"criteria application, no textual basis claimed"`, `"  Pass-1 value: "`, `"  (declared: {value} -- no evidence was locatable)"`, the `"\n\n"` join) | `engine/elicitation/materialize.py:76` `evidence_block`, `:101` `priming_block` | elicited Pass 2, inside the priming message | literal template + run-time evidence | **NO**: inside the `"R"` slot. **Not named in E-PIN** |
| `SYSTEM_PASS1` ("…first cite the numbered sentence units…") | `prompts.py:39` | `run_pass1` → `ollama_chat(messages=pass1_messages(prompt))` (`pipeline.py:137`) | literal | **yes**: `elicitation_pass1` renders `el.pass1_messages(el.sentinel_pass1_prompt(...))` |
| Elicitation Pass-1 prompt (`_CLASS_TITLE`, `_CLASS_CONTRACT`, `_escape_line`, `_worked_example`, `_sentinel_rule`, the numbered text) | `prompts.py:51–245` `build_pass1_prompt` | `run_pass1`, both attempts | literal + codebook (classes, tokens, field blocks) + unit map | **yes, template**: rendered over `build_unit_map(0, SENTINEL_TEXT)` |
| **Attempt-2 feedback block** (header "## Your previous response did not meet the contract on {n} field(s)", the "Each one below shows…" paragraph, per-field lines "you returned value:", "with …:", "indices that did not resolve:", "✗ {code} — …") | `prompts.py:342` `build_feedback_block` | `elicit` attempt 2 → `run_pass1(..., feedback=feedback)`; appended to the prompt string (`pipeline.py:131`) | literal + echoes of the model's attempt-1 output | **NO**: `sentinel_pass1_prompt` calls `build_pass1_prompt` with no feedback, and `run_pass1`'s default is `feedback: str = ""` |
| **Feedback requirement texts** (one per violation code, plus the class `accompaniment` strings) | `prompts.py:285` `_requirement` | inside the feedback block | literal + codebook escape token | **NO**: same reason. **Not named separately in E-PIN** (part of "the attempt-2 feedback template") |
| `SYSTEM_PASS2` ("…Use the cited evidence to produce accurate structured output…") | `prompts.py:45` | **never sent**: no reference anywhere in `engine/`, `scripts/`, `tests/` or `analysis/` (grep). Elicited Pass 2 uses `extractor.pass2_messages`'s system | literal | n/a (dead constant) |
| Snippet retry ("You previously extracted the value below…") + system "Respond ONLY with JSON." | `extractor.py:380` `retry_snippet_messages` | `_retry_snippet` (`:420`); non-elicited only (ELICIT-A §2.4: declared but uncalled on the elicited path) | literal + field/value/paper text | **yes, template**: rendered with `("f", "v", SENTINEL_TEXT)` |
| Missing-field / duplicate-field / unparseable / uncited retry | `extractor.py:735` `extract_paper_with_completeness` | re-calls `extract_paper` | **no separate text exists**: "re-running the identical two-pass request until complete… The prompt is rebuilt identically on each attempt" | n/a. The resend is the same texts as above. A duplicate field in elicited Pass 2 raises `DuplicateFieldError` (`pipeline.py`) into this same budget |
| Audit messages | `engine/agents/auditor.py:80` `build_audit_messages` | auditor → `ollama_chat(messages=build_audit_messages(...), **cfg.kwargs())` (`:72`) | literal + span | **yes**: `audit` stage, both field-type variants. **The auditor has no pin**: `audit` gets `arm_name None` from `_stage_arm`, so its hash reaches `run_stage_configs` and the manifest only |
| Preflight probe | `engine/utils/ollama_preflight.py` `PREFLIGHT_MESSAGES` | preflight check | literal | **yes**, `preflight:<model>`; no arm |
| Cloud outbound (`SYSTEM_MESSAGE` + extraction prompt + parameters) | `engine/cloud/base.py:32`, `:50` `outbound_messages` | cloud arms (R71: none enabled on live) | literal + codebook + paper | **yes**, `cloud:<arm>`, with parameters; carries the cloud arm's own pin |

**Observation, not a fix.** On the elicited path the Pass-2 user turn is the outer wrapper around
the priming wrapper:

```
Here is your prior analysis of this paper:

Here is the evidence you cited for this paper in your first pass. …
…
Now output the structured extraction as JSON matching the schema. Include all fields from the extraction schema. For each field, use the cited evidence above …

Now output the structured extraction as JSON matching the schema. Include all fields from the extraction schema.
```

This is two framings and the "Now output…" instruction twice. It follows from `pass2_messages`
wrapping whatever arrives as `reasoning_trace`.

## Q4. If the priming wrapper (`build_pass2_priming_message`) changed

| value | changes? | reason (quoted) |
|---|---|---|
| git commit | **yes** | It is a code literal in a tracked file. `open_run` refuses an uncommitted edit: `raise DirtyTree("run refused: the working tree has uncommitted changes, so the commit …")`. So any run under the edit carries a new `git_commit` |
| live spec sha256 | **no** | The literal is in `engine/elicitation/prompts.py`, not `review_specs/surgical_autonomy.yaml`. The manifest's `spec_hash` is `sha256_canonical(spec.model_dump(mode="json"))` (ELICIT-A §3.2), which a code literal cannot reach |
| `run_stage_configs.prompt_hash` | **no** | `extract_pass2` renders `ex.pass2_messages(prompt, "R")` (`effective_config.py:434`); the wrapper never enters any render. Measured: `extract_pass2` = `a4f3cb35…8e78` with elicitation off and on (Q9) |
| arm `pinned_sha256` | **no** | `pin_tuple`'s per-stage entry is `{"model", "model_digest", "options_hash", "format_schema_hash", "prompt_hash"}`, and none of the five moves |
| `run_calls.request_hash` | **yes, on elicited runs, for `extract_pass2` calls**; no on non-elicited runs, which never send it | `ollama_client.py:525`: `request = {"model": model, "messages": messages, **kwargs}` → `req_hash = _compute_request_hash(request)`, and `run_manifest.request_hash` is `sha256_canonical(dict(request))` over the messages as sent, which contain `priming_msg` |
| `presented_context_sha256` | **yes, on elicited papers where Pass 2 runs**; no where Pass 2 is skipped | `pipeline.py:351`: "R224a(1): the structured pass's hash is presented_context_sha256", i.e. `pass2_hash`; `elicited_presented = pass2_hash if pass2_hash is not None else p1_tel["pass1_request_hash"]` |
| `context_chain` | **yes, same condition** | `elicited_chain = pass1_hashes + ((pass2_hash,) if pass2_hash is not None else ())` |

**The attempt-2 feedback template, by the same reading.** It changes git and the
`elicitation_pass1` `request_hash` of **attempt-2 calls only**, and therefore `context_chain`
wherever attempt 2 ran (`pass1_hashes = tuple(tel["pass1_request_hash"] for tel in pass1_tels)`).
`presented_context_sha256` changes only on papers where Pass 2 is skipped and attempt 2 was
accepted. It never changes `prompt_hash` or the pin.

## Q5. The reuse key

`engine/core/reuse_key.py`:

```python
    digest = sha256_canonical({"arm": arm_id, "paper_id": paper_id,
                               "parsed_text_sha256": parsed_text_sha256})
    return f"{SCHEME}:{digest}"
```

Its docstring:

> `(arm, paper_id, parsed_text_sha256)`. The other components the plan's S3d six-tuple named —
> codebook hash, prompt hash, model digest, options hash — are NOT hashed in again: the arm
> carries them. An arm is pinned at its first manifest to a tuple holding exactly those
> (`run_manifest.pin_tuple`), and a later run resolving differently refuses (`ArmPinMismatch`…).

`claim_inputs` stores `(arm, paper_id, reuse_key, parsed_text_sha256, parsed_text_uid, run_id)`
per `extraction_uid` (`events.py`, the `INSERT INTO claim_inputs` statement). **It contains
neither `pinned_sha256` nor any `prompt_hash`.**

Selection (`selection.py:78`):

```python
        key = reuse_key(arm, paper_id, ref.sha256)
        if key in _live_reuse_keys(conn, paper_id, arm):
            skipped_asserted.append(paper_id)
```

**Would a wrapper-only edit cause a skip? Yes.** The pin is unchanged (Q4), so `open_run`
accepts the run on the existing arm: `if existing[name][2] != sha: … raise ArmPinMismatch`
does not fire, and it hits `continue`. Selection then skips every paper that already has a live
claim from that arm on the same parsed text.

A run after a wrapper edit would extract only the remaining papers, under the new wrapper, in the
**same arm**. The arm's claims would then mix two priming texts, with nothing in the pin, the
stage rows or the reuse key to separate them. Only `run_manifests.git_commit` and the per-call
`request_hash` / `presented_context_sha256` would differ.

The reuse-key docstring's premise, "the arm carries them" (prompt hash among them), is
**incomplete for these texts**: the arm carries `prompt_hash`, and `prompt_hash` does not carry
them.

## Q6. Every place the pin is compared

1. **`open_run` registration** (`run_manifest.py:316–326`). This is the only comparison of the
   full `pinned_sha256`:
   ```python
           if name in existing and existing[name][1] == ARM_PINNED:
               if existing[name][2] != sha:
                   diff = _diff_keys(json.loads(existing[name][0]), tup)
                   raise ArmPinMismatch(
                       f"run refused: arm {name!r} is pinned to a different configuration; "
                       f"this run differs at {diff}. A changed configuration is a new arm "
                       "(R10); declare one in the spec.")
   ```
   Before it, `PreManifestArm` ("run refused: arm {name!r} was registered pre-manifest. It never
   pins and accepts no new claims (R59)…") and `RetiredArm` refuse without comparing.
2. **`extraction_digest` / `verify_extraction_run`** (`run_manifest.py:444`;
   `extractor.py:900`). This compares **`model_digest` only**, per declared stage, against the
   pin's `configuration_json["stages"]`:
   ```python
           raise ArmPinMismatch(
               f"run refused: arm {arm!r} is not pinned, so run {run_id}'s digest {first} "
               "has no pin to agree with (R10, R117).")
   …
               raise ArmPinMismatch(
                   f"run refused: stage {stage!r} of run {run_id} resolved digest {digest}, "
                   f"but arm {arm!r} is pinned to {pin}. A changed model is a new arm "
                   "(R10); declare one in the spec.")
   ```
   There is also `StageNotInRun` if the pass-1 stage is undeclared.
3. **The arms freeze trigger.** It does not compare; it forbids re-pinning. Live
   `sqlite_master` (mode=ro):
   ```
   CREATE TRIGGER arms_configuration_frozen BEFORE UPDATE OF arm_kind, configuration_json, configuration_marker, pinned_run_id, pinned_sha256 ON arms WHEN EXISTS (SELECT 1 FROM field_events WHERE arm = OLD.arm_name) OR OLD.configuration_marker IN ('pinned', 'not recorded (pre-manifest)') BEGIN SELECT RAISE(ABORT, 'arms: configuration is frozen once the arm holds a claim or is pinned by a manifest, and a pre-manifest arm never pins (R21, R59) — a changed configuration is a new arm (R10)'); END
   ```
   Plus `arms_name_frozen`: "arms: arm_name is immutable — claim ids embed it, so a rename
   orphans every claim". 016's `arms_configuration_frozen_once_claimed` is not on live.
4. **The event writer** (`events.py` `_refuse_claim_on_arm`). This checks that the run declared
   the arm, not the pin value: `ArmNotInRun("claim refused: run {run_id} did not pin arm {arm!r},
   so this write's configuration is not its declared arm's (R10).")`. It also has
   `ClaimOnPreManifestArm` and `ClaimOnRetiredArm`.
5. **The R357 resume check is not code.** `grep -rl R357 engine scripts tests` returns nothing.
   R357 assigns it to procedure: "Any resume launches only with HEAD exactly at the tag; the 12b
   pre-flight checks this, and checks the pin against the first manifest's." On a resume, the pin
   comparison that actually executes is (1), which a wrapper-only edit passes (Q5). The tag-HEAD
   condition is the only guard that would see such an edit, and it lives in the 12b pre-flight.

## Q7. Live arms (Step 3's own read, mode=ro)

```
SELECT arm_name, arm_kind, configuration_marker, pinned_sha256, pinned_run_id, retired_at FROM arms ORDER BY rowid
('local', 'model', 'not recorded (pre-manifest)', None, None, None)
('anthropic_sonnet_4_6', 'model', 'not recorded (pre-manifest)', None, None, None)
('openai_o4_mini_high', 'model', 'not recorded (pre-manifest)', None, None, None)
count 3   pinned (pinned_sha256 or pinned_run_id NOT NULL) 0
```

No live arm is pinned, and `local_deepseek_r1_32b` is not registered. Any change to what the pin
hashes therefore has no stored pin on live to break. The stored pins are on the retained
throwaway reviews (e.g. the 10b smoke's `defd293b…`, `SMOKE-10b_log_20260929T191255Z.md:120`).

## Q8. Tests

**Tests asserting a pin value or the pin's composition.** None asserts the literal `defd293b…`,
which appears only in session reports.

- `tests/test_run_manifest.py::test_a_declared_arm_is_pinned_at_its_first_manifest`:
  `set(tup["stages"]) == set(EXTRACTION)` with
  `EXTRACTION = ("extract_pass1", "extract_pass2", "extract_retry_snippet")`, and
  `row["pinned_sha256"] == rm.sha256_canonical(tup)`
- `tests/test_run_manifest.py::test_an_unpinned_registered_arm_is_pinned_too`
- `tests/test_run_manifest.py::test_the_same_configuration_opens_again_and_keeps_the_first_pin`
- `tests/test_run_manifest.py::test_a_pinned_arm_resolving_differently_is_refused_and_the_difference_named`
  (`match="options_hash"`)
- `tests/test_run_manifest.py::test_a_changed_model_digest_is_a_pin_mismatch`
- `tests/test_run_manifest.py::test_a_changed_prompt_hash_is_a_pin_mismatch` (R97: patches
  `ec.render_messages` to reword `extract_pass1`'s last message and expects
  `ArmPinMismatch` naming `prompt_hash`)
- `tests/test_run_manifest.py::test_a_changed_codebook_hash_is_a_pin_mismatch`
- `tests/test_extraction_run_link.py::test_t3_a_digest_disagreeing_with_the_pin_refuses_before_selection`
- `tests/test_event_writer_refusals.py::test_r59_a_pinned_arm_is_frozen_before_it_holds_any_claim`,
  `::test_r59_a_pre_manifest_arm_can_never_be_pinned`
- `tests/test_run_manifest_migration.py::test_a_fresh_database_carries_020_with_an_executed_receipt`
  (column order `["pinned_run_id", "pinned_sha256"]`)

**Tests that would fail if `prompt_hash` coverage widened.** Read, not measured: nothing was
edited or run.
- **If the widening changes the rendering of existing stages** (e.g. `extract_pass2` rendering a
  sentinel priming message, or `elicitation_pass1` rendering a sentinel feedback block), I found
  no test that fails by construction. No test asserts a literal `prompt_hash` value.
  `test_request_capture.py`'s `_assert_site` excludes `messages` from its comparison
  (`after = {k: v for k, v in sent.items() if k != "messages"}`). The equality tests
  (`test_effective_config.py:79`, `test_undeclared_override.py:138`) compare two renders of the
  same stage.
  - `test_effective_config.py::test_a_pico_edit_moves_the_screening_prompt_hashes_and_not_the_others`
    and `::test_a_title_edit_moves_only_the_abstract_primary_prompt_hash` would fail only if the
    widened render made an extraction stage depend on PICO or title. Code literals do not.
- **If the widening adds stage keys** (a separate stage for the priming wrapper or the feedback
  block), these fail:
  - `test_run_manifest.py::test_a_declared_arm_is_pinned_at_its_first_manifest`: its stage set is
    a literal.
  - `tests/test_request_capture.py::test_every_stage_has_a_capture_test`:
    `assert covered == set(OLLAMA_STAGES)`.
  - Every `test_effective_config.py` test looping over `ec.OLLAMA_STAGES` must classify the new
    key.
- `tests/test_elicitation_codebook.py::test_prompt_builders_name_no_codebook_field[build_pass2_priming_message]`
  inspects source for field names. It is unaffected unless a fix names a field.

## Q9. Pin recomputed at HEAD, live spec (non-elicited)

**`defd293bfe62e427b906e072dcf56d0ca6e321e7d1ca4ad1fde3758fb84f3e6e`, which equals the
prediction and the 10b/11c value.**

**How it was computed.** No model call, no digest fetch, no manifest, no database connection:
- `load_spec_for("surgical_autonomy")` and
  `load_codebook_beside("data/surgical_autonomy/review.db")`. The latter reads the YAML beside
  the database; it does not open the database.
- Stages as `_open_run_manifest` builds them for `--skip-to extract`: `extract_pass1`,
  `extract_pass2`, `extract_retry_snippet`, `audit`, `preflight`. `arms_by_stage` comes from
  `rm._stage_arm`.
- `rm.resolve_run(..., digest_fn=dfn, preflight_models=["deepseek-r1:32b", "gemma3:27b"])`.
  `dfn` returns the two digests read from `/api/tags` in Step 1
  (`deepseek-r1:32b edba8017…960c`, `gemma3:27b a418f583…2203`) and raises on any other model.
  `engine.utils.ollama_client.fetch_model_digest` was patched to raise, and it did not fire.
- `rm.pin_tuple(spec, "local_deepseek_r1_32b", resolved, cb.semantic_hash,
  rm.library_versions())`, then `sha256_canonical`.
- The mtimes of `data/surgical_autonomy/review.db*` were identical before and after.

**The tuple:**

```json
{"arm_kind": "model", "provider": "ollama", "model": "deepseek-r1:32b",
 "stages": {
  "extract_pass1":         {"model": "deepseek-r1:32b", "model_digest": "edba8017…960c", "options_hash": "a611f2d4…13c0", "format_schema_hash": "none",          "prompt_hash": "c0083f30…4f34"},
  "extract_pass2":         {"model": "deepseek-r1:32b", "model_digest": "edba8017…960c", "options_hash": "f4705792…e6e9", "format_schema_hash": "2c9ec255…10b3", "prompt_hash": "a4f3cb35…8e78"},
  "extract_retry_snippet": {"model": "deepseek-r1:32b", "model_digest": "edba8017…960c", "options_hash": "f4705792…e6e9", "format_schema_hash": "none",          "prompt_hash": "05a98181…55aa"}},
 "codebook_hash": "1e67684b81c82e6002c45951f5f0ea4860f3a2a1774177f7324df0f54aa99172",
 "client_library": {"ollama": "0.6.1"}}
```

**Elicitation on (in-memory `model_copy`, informational).** The same computation with the spec
copy's `extraction_models.elicitation = True`, stage `elicitation_pass1` in place of
`extract_pass1`, gives **`65c8d942523eaa5a0d2c6a4a0f0f2232b900257f70e7ea4dd5a43a76db0e3c56`**.

- `elicitation_pass1` prompt_hash: `36179e6c…d56f`.
- `extract_pass2`'s prompt_hash: unchanged (`a4f3cb35…8e78`).

This is not the pin of any committed spec. Under ELICIT-A §2.3 it is the value a committed flip
would produce at this HEAD, provided nothing else changes.

---

## Predictions

| prediction | result |
|---|---|
| Q3 finds the two named texts and possibly more uncovered texts | **held, and more were found.** Beyond `build_pass2_priming_message` and the feedback block: (a) `materialize.evidence_block` / `priming_block`, the per-field evidence formatting inside the priming message; (b) the `_requirement` texts inside the feedback block; (c) `SYSTEM_PASS2`, which is never sent (dead constant) |
| I6 holds (literals) | **held.** Both named texts are literals in `engine/elicitation/prompts.py`; the additional ones are literals in `prompts.py` and `materialize.py`. Their runtime contents (evidence, echoes) are model or paper data |
| Q4: `run_calls.request_hash` changes but `pinned_sha256` does not | **held, with a qualification:** `request_hash` changes only on elicited runs, only for `extract_pass2` calls (and `presented_context_sha256`/`context_chain` with it). A non-elicited run never sends the wrapper |
| Q5: unknown | Key = `(arm, paper_id, parsed_text_sha256)`; no pin and no prompt_hash. **A wrapper-only edit passes `open_run` and is skipped past by selection**, so an arm can mix wrapper versions |
| Q7: no live arm pinned | **held** |
| Q9: equals `defd293b…3e6e` | **held** |

**No prediction broke.** Two findings beyond the predictions bear on the PI fork:

1. **Q5/Q6.** The pin is the only guard the reuse key relies on for prompt identity, and R357's
   resume check is procedural, not code.
2. **The elicited Pass 2 double-wraps its priming** (the Q3 observation).

## Items recorded for the 12c close (not acted on)

- **R-3 (12c-OPEN-R2):** plan line 409's E-PIN source "12a-ELICIT-A §2.4" should read "§2.3 and
  closing-list item 2". Not fixed.
- **R-3 (12c-OPEN-R1), residency amendment proposed to the PI:** "Startup residency is recorded,
  not matched: one model resident with keep_alive −1 is expected; which model depends on the last
  load, including the 09:00 nightly. A specific model is checked only in a run's or smoke's own
  pre-flight." Observed at 12c open: `qwen3:8b`, keep_alive Forever.
- These three files are uncommitted and are committed at the 12c close:
  - `12a_ELICIT-A.md`
  - `12a_ROWS-A.md`
  - this report
