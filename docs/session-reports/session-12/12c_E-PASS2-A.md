# 12c E-PASS2 — Phase A: the elicited Pass-2 composition, history read

Session 12c, CC session `bbd7293d`, 2026-10-01. Brief 12c-E-PASS2-A. **Read-only**: git
log/show/grep, source and docs reads, and transcript reads. No checkout, no code run from an old
commit, no database connection, no model call. Tree at `93e3a55`. Old code was compared by
`git show <commit>:<path>` and the stdlib `ast` module, parse only.

**The prediction broke on both counts.** `SYSTEM_PASS2` was **never** sent by any commit. The
current composition is **not** from the write-path move (9b–10b): it is the one the priming
commit `87f3434` introduced on 2026-09-03, before every ELICIT-DESIGN smoke, and both smokes
that made Pass-2 calls sent it.

---

## Q1. `SYSTEM_PASS2`

`git log --all -S SYSTEM_PASS2` and `git log --all -G SYSTEM_PASS2` each return exactly one
commit:

```
54f4490 2026-09-03 feat(elicit): per-class Pass-1 evidence contracts, enforced at parse time
```

That is the commit that introduced it. **No commit ever added or removed a reference to it.**
`git grep` at `54f4490`, `87f3434` and `a87db2b` finds it only at its definition
(`engine/elicitation/prompts.py:41`, later `:45`). At `77fe1a2` it is still defined
(`prompts.py:45`) and still unreferenced in `engine/`, `scripts/`, `tests/` or `analysis/`
(M3, 12c_E-PIN-A Q3).

```python
SYSTEM_PASS2 = (
    "You are a systematic review data extractor. "
    "Use the cited evidence to produce accurate structured output. "
    "Respond ONLY with the requested JSON."
)
```

**Last code that sent it: none exists.** It was defined in the Pass-1 contracts commit, one
commit before the Pass-2 priming commit, and the priming commit did not use it.

## Q2. The elicited Pass-2 call site: introduced when, replacing what

**Introduced at `87f3434`** (2026-09-03 15:38:28 UTC, "feat(elicit): materialize cited units and
prime Pass 2 with them"). This commit created `engine/elicitation/pipeline.py` (266 lines
added). `git log -S build_pass2_priming_message` shows `54f4490` (definition) and `87f3434`
(first call) as the only code commits; every later hit is a docs commit.

`87f3434:engine/elicitation/pipeline.py:203–209`:

```python
    pass2_prompt = build_extraction_prompt(paper_text, spec, cb_path)
    priming_msg = build_pass2_priming_message(priming)
    S.enforce_fit(pass2_prompt + priming_msg, label=PASS2_LABEL, paper_id=paper_id)

    result = extract_pass2_structured(
        pass2_prompt, priming_msg, spec, paper_id, think=pass2_think,
    )
```

At that commit, `extract_pass2_structured` built its messages inline
(`87f3434:engine/agents/extractor.py`, inside `extract_pass2_structured`):

```python
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a systematic review data extractor. "
                    "Use your prior reasoning to produce accurate structured output. "
                    "Respond ONLY with the requested JSON."
                ...
            {"role": "user", "content": prompt},
            {
                "role": "user",
                "content": (
                    f"Here is your prior analysis of this paper:\n\n"
                    ...
                    f"Now output the structured extraction as JSON matching the schema. "
                    f"Include all fields from the extraction schema."
```

**What it replaced: nothing on the elicited path.** `pipeline.py` did not exist before
`87f3434`, so there is no prior elicited composition. The composition was double-wrapped from its
first commit:
- system: "…Use your prior reasoning…";
- user 1: the extraction prompt with paper text;
- user 2: "Here is your prior analysis of this paper:\n\n" + `build_pass2_priming_message(…)`
  + "\n\nNow output the structured extraction as JSON…".

Its message (Pass 2 only):

> Pass 2 retains the full paper text (C6-Q4 ruling: this task changes elicitation, not context
> composition) and receives the materialized evidence in place of the free-form trace.

**Changes since, and why none of them altered the text.**
- `git log -L '/^def pass2_messages/,+22:engine/agents/extractor.py'` names one commit:
  `1cd80a4` (MANIFEST-01 Phase 2a, 2026-09-22), which moved the inline list into
  `pass2_messages`.
- An AST comparison of the role-bearing message dicts in `extract_pass2_structured` at each of
  `87f3434`, `336bd76`, `d3de9c7` and `9581a4c` against `pass2_messages` at `77fe1a2` returns
  **IDENTICAL** for all four. The only removed dict is the inline `{'temperature': 0}` options,
  which now comes from the resolver.
- `build_pass2_priming_message`, and `materialize.evidence_block` / `priming_block` /
  `citations`, are AST-**IDENTICAL** between each smoke commit and `77fe1a2`.
- The call site at `77fe1a2` (`pipeline.py:350–353`) differs only in its keyword arguments
  (`cfg=cfg_p2, codebook_hash=…, return_request_hash=True`, from MANIFEST-01 and R224a), and
  `enforce_fit` is gone (INPUT-FIT-01 moved the guard into `ollama_chat`).

## Q3. The ELICIT-DESIGN smokes and the commit each ran at

All four ran `analysis/eval/elicit_design01/smoke.py`, which drives the **production** entry
point with elicitation forced on (`9581a4c:analysis/eval/elicit_design01/smoke.py:173,188,218`;
the same three lines at `336bd76`):

```python
    from engine.agents.extractor import MODEL, extract_paper_with_completeness
    spec.extraction_models.elicitation = True
                result = extract_paper_with_completeness(pid, text, spec, db,
```

| run id | commit | identified by | Pass-2 calls |
|---|---|---|---|
| `aborted_smoke_20260903T153852Z` | `8cf0332` (inferred) | run id 15:38:52, after `8cf0332` 15:38:38 and before `1ba2d58` 15:56:22; killed after p121 (ELICIT-DESIGN-01 report §(b)) | none recorded |
| `smoke_20260903T155654Z` | **`336bd76`** | ELICIT-DESIGN-01 report: "**HEAD:** `336bd76` + this report. **Smoke:** `smoke_20260903T155654Z`, **0/3 papers stored.**" | **0**: "9 Pass-1 calls, **zero Pass-2 calls** — every paper failed before Pass 2" |
| `smoke_20260905T011330Z` | **`d3de9c7`**, tree clean | transcript `~/claude-session-archive/4d489a72*.jsonl`: the pre-flight output at 2026-09-05T01:13:26Z reads "=== tree === \| (clean) \| d3de9c7", and the launch follows at 01:13:29Z | **3**: ELICIT-DESIGN-02 report §3.1 "Pass-2 calls \| 0 \| 3"; telemetry `outcome=stored` for p121, p604, p498 with `raw_content_chars` 6378 / 5236 / 4910 |
| `smoke_20260905T024348Z` (p498 probe) | **`9581a4c`** | same transcript: `9581a4c` committed at 02:43:35Z, probe launched at 02:43:48Z ("--papers 498"); the probe report is titled "after the JUDGMENT-instruction rewrite", which is `9581a4c`'s subject | **1**: telemetry `outcome=stored`, `raw_content_chars` 4669 |

I1 holds for `155654Z` (stated in a report) and for `011330Z` (stated in a transcript).
`024348Z`'s commit comes from the transcript's command order, not a stated HEAD. The
aborted run's commit is inferred from timestamps only.

**The Pass-2 composition at the two smokes that sent Pass 2** (`d3de9c7`, `9581a4c`): exactly the
`87f3434` composition quoted in Q2, by AST. Call site at `9581a4c` and at `a87db2b`
(`pipeline.py:283–288`):

```python
        pass2_prompt = build_extraction_prompt(paper_text, spec, cb_path)
        priming_msg = build_pass2_priming_message(priming)
        S.enforce_fit(pass2_prompt + priming_msg, label=PASS2_LABEL, paper_id=paper_id)
        result = extract_pass2_structured(
            pass2_prompt, priming_msg, spec, paper_id, think=pass2_think,
        )
```

**Request skeleton:** the smokes did not log request messages. The JSONL keys are `attempts`,
`error`, `error_type`, `fields`, `finished_utc`, `latency_s`, `model`, `model_digest`,
`n_units`, `ok`, `paper_id`, `parsed_chars`, `server_version`, `started_utc`, `telemetry`. The
telemetry keys include `raw_content`, the Pass-2 **response**, but no request. A grep of
`data/surgical_autonomy/eval/elicit_design01/` for `"messages"`, "Here is your prior analysis"
and "Here is the evidence you cited" returns nothing. The skeleton below is therefore
reconstructed from the code at `d3de9c7` / `9581a4c`, not taken from a log:

```
system: "You are a systematic review data extractor. Use your prior reasoning to produce accurate structured output. Respond ONLY with the requested JSON."
user:   <build_extraction_prompt(paper_text): schema blocks + Instructions + "## Paper Text" + full paper>
user:   "Here is your prior analysis of this paper:\n\n"
        "Here is the evidence you cited for this paper in your first pass. Each quote is the verbatim text of a numbered unit you named, resolved by the engine — not text you retyped.\n\n"
        "### <field>  [<class>]\n  [S<n>] \"…\"\n  …  Pass-1 value: …"   (one block per evidenced field; evidence elided)
        "\n\nNow output the structured extraction as JSON matching the schema. Include all fields from the extraction schema. For each field, use the cited evidence above as your source_snippet wherever it supports the value, and keep the value consistent with the evidence you cited."
        "\n\nNow output the structured extraction as JSON matching the schema. Include all fields from the extraction schema."
format: ExtractionOutput JSON schema
```

## Q4. Design documents, reports or rulings that specify the elicited Pass-2 message

Searched `docs/` and `CLAUDE.md` for `SYSTEM_PASS2`, `build_pass2_priming_message`,
"priming message", "Pass-2 (user) message", "C6-Q4", "prime(s) Pass 2", "Here is the evidence
you cited" and "prior analysis", excluding the 12a/12c extracts.

**No document specifies the elicited Pass-2 message text, its system text, or whether the outer
wrapper should remain.** What exists:

- `docs/session-reports/ELICIT-DESIGN-01_report.md` §2 documents the **pre-elicitation** Pass 2
  as found at STEP 0:
  > ```
  >   result = extract_pass2_structured(prompt, reasoning_trace, ...)     # :296
  >         system: "...Use your prior reasoning... Respond ONLY with the requested JSON."
  >         user 1: prompt                        # THE SAME PROMPT — full paper text AGAIN
  >         user 2: "Here is your prior analysis of this paper:\n\n{reasoning_trace}\n\n
  >                  Now output the structured extraction as JSON matching the schema..."
  > ```
- The same report, C6-Q4. This decides context (whether the paper stays), not the message
  wrapper:
  > **Q4 — does Pass 2 still see the full paper text?** … Spec item 4 says the primed context "is
  > built from the materialized evidence" but does not say the paper leaves. … **Recommendation:
  > keep the paper text for the smoke** (change one thing at a time — the elicitation, not the
  > priming *and* the context) …
- `build_pass2_priming_message`'s docstring (unchanged since `54f4490`):
  > The Pass-2 user message that replaces the free-form reasoning trace.
  >
  > Pass 2 still receives the full paper text as its first user message (ELICIT-DESIGN-01 C6-Q4:
  > this task changes elicitation, not context composition), so the evidence below is priming,
  > not the model's only view of the paper.
- `CLAUDE.md` (Per-Class Evidence Elicitation): "The engine materializes verbatim text from the
  persisted unit map and primes Pass 2 with it." It says nothing about the message shape.
- `docs/session-reports/INPUT-FIT-01_phase-1_readout.md:178,225` and
  `write-path-01/WRITE-PATH-01_phase1a_readout_20260924.md:73` describe the extractor's Pass 2
  ("user = `"Here is your prior analysis of this paper:\n\n{reasoning_trace}\n\n…"`"). They do
  not separate out the elicited path.

The "spec item 4" that C6-Q4 quotes is the architect's ELICIT-DESIGN-01 brief. It is not under
`docs/`, and I did not search the transcripts for it.

## Q5. Tests asserting elicited Pass-2 message content or system text

**None asserts the Pass-2 system text, the priming wrapper's literal text, or the outer
wrapper on the elicited path.** Searched `tests/` for `SYSTEM_PASS2`, "Use the cited evidence",
"Here is the evidence you cited", "prior analysis", "Use your prior reasoning",
`build_pass2_priming_message`, `priming_msg` and `pass2_messages`.

Adjacent hits:
- `tests/test_elicitation_pipeline.py::test_reasoning_trace_is_the_materialized_evidence`
  asserts content of the **priming body** (from `materialize.evidence_block`), as carried on the
  result rather than read off a sent request:
  ```python
      trace = result.reasoning_trace          # 9b-FLIP: the trace is the result's, not a row
      assert "[S1]" in trace and "[S2]" in trace
      assert "Declared inference:" in trace
      assert "The system used a da Vinci Research Kit." in trace
  ```
- `tests/test_elicitation_codebook.py::test_prompt_builders_name_no_codebook_field[build_pass2_priming_message]`
  inspects the builder's source for field names only.
- `tests/test_ollama_input_fit.py:236,257` use `"Here is your prior analysis. " * N` as filler
  for sizing. They do not assert content.
- `tests/test_request_capture.py` has no elicited Pass-2 case (`test_elicitation_pass1` drives
  `run_pass1` only), and `_assert_site` excludes `messages`.

## Q6. Classification — **INFERRED** (my judgement)

**(a) The current composition is the tested composition.**

Basis:
1. Both smokes that made Pass-2 calls ran at commits whose Pass-2 composition is AST-identical
   to HEAD:
   - `smoke_20260905T011330Z` at `d3de9c7`, 3 Pass-2 calls, tree clean per the transcript;
   - `smoke_20260905T024348Z` at `9581a4c`, 1 Pass-2 call.

   Identical means the message dicts, `build_pass2_priming_message`, and `evidence_block` /
   `priming_block` (Q2, Q3).
2. The composition has been unchanged since its introduction at `87f3434` (Q2). The only commit
   touching its builder, `1cd80a4`, moved it into `pass2_messages` and left the text unchanged.
3. `SYSTEM_PASS2` was never sent at any commit (Q1), so there is no earlier "sent" state to
   regress from.

**This classifies regression only, not intent.** Nothing on disk specifies the message (Q4) and
nothing tests it (Q5), so whether the double wrap was *intended* is not answerable from disk.
Two texts point different ways:
- **The builder's own docstring**, "The Pass-2 **user message** that replaces the free-form
  reasoning trace", together with the unused `SYSTEM_PASS2` ("Use the cited evidence…"), reads as
  a design for a whole message under its own system text.
- **The commit message**, "receives the materialized evidence **in place of the free-form
  trace**", is satisfied by what was built: the evidence passed as the `reasoning_trace`
  argument.

The ELICIT-DESIGN-02 result ("smoke 3/3 stored") and the p498 probe were measured on the
double-wrapped composition.

## The prediction

**It broke, on both clauses:**
- "SYSTEM_PASS2 was once sent by an elicitation-specific Pass-2 builder": **false.** No commit
  references it beyond its definition (`-S` and `-G` both give only `54f4490`).
- "The current composition dates from the write-path move (sessions 9b–10b), after the design
  smokes": **false.** It dates from `87f3434` (2026-09-03), before every smoke, and is the
  composition the Pass-2-making smokes sent.

## Bearing on E-PIN's shared-builder design (12c-E-PIN-R1)

- **Changing what Pass 2 sends changes the tested composition.** Under (a), the ELICIT-DESIGN-02
  evidence is about the double-wrapped message. An E-PASS2 change is therefore a change to what
  Run 7 sends relative to what was smoke-tested, which is the PI fork that 12c-E-PASS2-R1
  anticipates.
- **E-PIN can be built without touching the composition.** E-PIN-R1 asks that a stage's
  `prompt_hash` render "every template its runtime call can send, through the same builder the
  runtime calls". On the elicited path, `extract_pass2`'s runtime call is
  `pass2_messages(prompt, build_pass2_priming_message(priming_block(...)))`. One builder chain
  serves both paths, and on the elicited path the priming builder fills `reasoning_trace`. A
  shared render must therefore nest the two builders exactly as the runtime does, with model
  output and paper text as the only placeholders. That makes the double wrap visible in the hash
  without changing it.
- **The render will depend on configuration.** The non-elicited `extract_pass2` call sends
  `pass2_messages(prompt, <Pass-1 trace>)`. If the elicited render nests the priming builder,
  then `extract_pass2`'s `prompt_hash` will differ between elicitation off and on, where today
  both are `a4f3cb35…8e78` (12c_E-PIN-A Q9). The pin changes accordingly, as R1 intends.
- **`SYSTEM_PASS2` has no runtime caller.** Under the R1 invariant ("every template its runtime
  call can send") it has nothing to render. Whether it is retired or wired in is E-PASS2's
  question, not E-PIN's.
