# 12w-V1A resume — Q3 and Q4, stopped at Q4 (R706)

HEAD at the reads: `7457b28b7d93387a332ecc5c238f6e42c3c641ab` (item 0 of the resume: R706 as Block
104 with the INT-v12-1 note), parent `a0f2323`. Live spec `aeb90ba8…8b33`, unchanged. No engine, test,
spec or codebook change on main; no model call; no Ollama request. The worktree, the pin, capture and
telemetry scripts replace `ollama_chat` and `fetch_model_digest` with functions that raise.

The Q2 record is `12w_V1A_q2_stopped.md` (a0f2323), not edited. Everything for this resume is under
`r2/`.

**Why this record stops at Q4.** R706(d) gives an expected value for one column of Q4's table: "a
future ruled structure change would move the pin (A and C should; B alone does not, since it adds no
pin coverage)". Measured:

- **B:** a structure change made on both sides, as B's test requires to pass (B + V3), moves the
  elicited pin to `8f66789a…ec0f`, and B's test passes. A runtime-only change (B + V1) leaves the pin
  unchanged and B's test fails. So under B a ruled change that lands with the suite green does move
  the pin. The ruling's reason holds (B adds no pin coverage; B + V1's pin is unchanged), but its
  expected answer does not.
- **A:** the pin moves only if the change is made inside A's one builder (A + V3′). Made at
  `run_pass1`'s call site (A + V1′), it does not.

Per CONTRADICTION HANDLING ("Any finding that contradicts an assumption or EXPECTED value: STOP at
that step, report both values … do not adapt"), Q5 and Q7 are not started. Q3 (a reading) and Q4's
measurements are complete and recorded below.

## 0d — R706(f)(i)

`git ls-files data/surgical_autonomy/extraction_codebook.yaml` prints
`data/surgical_autonomy/extraction_codebook.yaml`, exit 0: the codebook is **tracked**. So `git worktree
add` created it in the first worktree at 00:11:59Z, and my `cp` overwrote it with identical bytes at
00:13:31Z. That explains the times. It is also consistent with the Q2 reruns, which showed only that
neither script creates `data/`. The copy was unnecessary. In `wt2` the tracked file was used as
checked out (sha256 `89dbfa91…ad82`) and nothing was copied.

## Q3 — census of every hashed stage (READ, at HEAD)

**Method.** Every `ollama_chat(` call site in `engine/` (`grep -rn "ollama_chat(" engine/`), each read
beside its `render_messages` branch in `engine/core/effective_config.py`, plus the two cloud transports'
`request_payload`.

| stage | runtime call site → list sent | hash path → list rendered | one function or two (for the list) | a structure change invisible to the hash? | Run 7's path |
|---|---|---|---|---|---|
| `abstract_screen_primary` / `_verifier` | `screener.py` `screen_paper`: `ollama_chat(messages=build_messages(paper, spec, role=role), …)` | `build_messages(SENTINEL_PAPER, spec, role=role)` | one | only at the call site | no |
| `ft_screen_primary` / `_verifier` | `ft_screener.py` `ft_screen_paper` / `ft_verify_paper`: `messages=build_ft_messages(paper_text, spec, "primary"/"verifier")` | `build_ft_messages(SENTINEL_TEXT, spec, which)` | one | only at the call site | no |
| `extract_pass1` (non-elicited arm) | `extractor.py` `extract_pass1_reasoning`: `messages=pass1_messages(prompt)`; `prompt = build_extraction_prompt(paper_text, spec, cb_path)` | `ex.pass1_messages(ex.build_extraction_prompt(SENTINEL_TEXT, …))` | one | only at the call site | no (live spec `elicitation: false` today; Run 7 runs elicited, R347) |
| `extract_pass2` (both arms) | `extract_pass2_structured`: `messages=pass2_messages(prompt, reasoning_trace)`. Elicited: `reasoning_trace` = `pass2_priming(…)[1]` | `ex.pass2_messages(prompt, el.sentinel_priming_message(…))` (elicited) or `ex.pass2_messages(prompt, "R")` | one | only at the call site | yes (elicited) |
| `extract_retry_snippet` | `_retry_snippet`: `messages=retry_snippet_messages(field_name, value, paper_text)` | `ex.retry_snippet_messages("f", "v", SENTINEL_TEXT)` | one | only at the call site | yes |
| `elicitation_pass1` | `run_pass1`: `messages=pass1_messages(prompt)`, `prompt = pass1_prompt(…, feedback)`; `elicit` runs attempt 2 with `build_feedback_block(first, …)` | `pass1_messages(sentinel_pass1_prompt(…))` + the same with the sentinel feedback, concatenated in the render | one per attempt; the two-attempt composition is decided twice (`elicit` / `render_messages`) | **yes, measured** (Q2, V1) | yes |
| `audit` | `auditor.py` `semantic_verify`: `messages=build_audit_messages(span, field_type=field_type)` | `build_audit_messages(span, "text") + build_audit_messages(span, "categorical")`, concatenated in the render | one per call; the two-variant composition is decided twice (the caller's `field_type` / the render) | only at the call site | yes |
| `vision_parse` | `pdf_parser.py`: `messages=vision_messages(img_b64)` | `vision_messages(SENTINEL_IMAGE)` | one | only at the call site | no (parse precedes the run) |
| `pdf_quality` | `pdf_quality_check.py`: `messages=classification_messages(img_b64)` | `classification_messages(SENTINEL_IMAGE)` | one | only at the call site | no |
| `preflight` (in the manifest, not in a pin) | `ollama_preflight.py`: `messages=[dict(m) for m in PREFLIGHT_MESSAGES]` | `[dict(m) for m in PREFLIGHT_MESSAGES]` | **two copies of one expression** over a shared constant | yes, at either copy | yes (the probe, not extraction) |
| `cloud:<anthropic arm>` | `anthropic_extractor.py` `request_payload`: `system, user = outbound_messages(prompt)`, then `"system": system["content"], "messages": [user]` | `outbound_messages(build_extraction_prompt(SENTINEL_TEXT, …))`, a two-message list | **two**: the runtime reshapes the list into Anthropic's `system=` + one-message form after the shared builder | yes (the reshaping is runtime-only) | no (R71; `enabled_arms` empty) |
| `cloud:<openai arm>` | `openai_extractor.py`: `"messages": outbound_messages(prompt)` | `outbound_messages(…)` | one | only at the call site | no |

**Reading.** "Only at the call site" means every stage shares V1's site class: the hash renders the
builder, never the call site. So a message appended, dropped or reordered at the call site, after the
builder returns, is invisible to its prompt_hash at every stage. Three stages have a second place:
- **`elicitation_pass1` and `audit`:** the render concatenates variants the runtime sends as separate
  calls, so how many variants exist is decided in two places.
- **The Anthropic arm:** it reshapes the list at runtime.
- **`preflight`:** its expression is duplicated.

Only `elicitation_pass1`'s exposure has been exercised (Q2). The others are READ, not exercised (Q3
said "Exercise only by reading").

## Q4 — options A, B and C, measured in a fresh worktree (MEASURED)

**Method.**
- **Worktree:** `~/scratch/12w-v1a/wt2`, a detached worktree of `7457b28`.
- **Each run:** from a clean worktree, apply the listed diffs in order, then run the pin script,
  `capture_attempts.py` (the fixed case of Q2), the four named test files plus the candidate (in the
  worktree's `tests/`), and the 31-file supplement (32 with B's file). Then
  `git checkout -- . && git clean -f tests/`, confirmed clean.
- **Driver:** `r2/scripts/run_all.sh` → `measure2.sh` → `run_tests2.sh`, one background job, exit 0,
  log `r2/run_all.log`. Every pytest run passed `--basetemp` under `~/scratch/12w-v1a/r2/basetemp/`.
- **Reused unchanged:** the V1–V3 diffs and the candidate are applied from `12w_V1A/diffs/` and
  `12w_V1A/repro/` (R706(d)).

**The options as built** (`r2/diffs/`):
- **A** (`A_one_builder.diff`, `pipeline.py` + `effective_config.py`): `pass1_attempt_messages(unit_map,
  codebook, field_names, feedback)` returns one attempt's complete list. `run_pass1` sends it.
  `sentinel_pass1_messages(spec, codebook_path, feedback)` calls it over the sentinel text, and
  `render_messages` uses that for both attempts.
- **B** (`B_structural_test.diff`, tests only): the candidate as
  `tests/test_elicitation_pass1_runtime_structure.py`, copied, not retyped.
- **C** (`C_hash_over_runtime.diff`, `pipeline.py` + `effective_config.py`):
  `sentinel_runtime_messages(spec, codebook_path)` runs `run_pass1` itself for attempt 1 and attempt 2
  over the sentinel inputs. It replaces the module's `ollama_chat` with a capture that raises once the
  request is handed over, then restores it. `render_messages` returns the captured lists.

**V1, V2 and V3 on each option.** As written, V1 and V2 do not apply on A, and V2 and V3 do not apply
on C (`git apply` → "patch does not apply", recorded in `run_all.log`). The option has replaced the
lines they edit. For those cases an **adapted** change of the same kind was measured, marked ′ and
kept as a combined diff:
- **A + V1′** (`A_plus_V1prime.diff`): the feedback as a third message at `run_pass1`'s call site,
  around A's builder.
- **A + V2′** (`A_plus_V2prime.diff`): the same in A's render only.
- **A + V3′** (`A_plus_V3prime.diff`): the same inside A's one builder.
- **C + V2′** (`C_plus_V2prime.diff`): attempt 2 reshaped after capture, in C's hash-only function.

Under C, V1 itself applies and is the both-sides case.

| run | elicited pin | `elicitation_pass1` hash | attempt-2 request (msgs; kwargs sha256) | failed (both runs identical) |
|---|---|---|---|---|
| HEAD | `d70bf688…59f0` | `5939ce6f…b713` | 2; `307077c9…71a3` | none (62 / 480 passed) |
| A | `d70bf688…59f0` | `5939ce6f…b713` | 2; `307077c9…71a3` | none |
| A + V1′ | `d70bf688…59f0` | `5939ce6f…b713` | 3; `123f6630…4db2` | `…requests_are_byte_identical`, candidate |
| A + V2′ | `8f66789a…ec0f` | `5754f6cf…cca0` | 2; `307077c9…71a3` | `…hashes_both_attempts`, candidate |
| A + V3′ | `8f66789a…ec0f` | `5754f6cf…cca0` | 3; `123f6630…4db2` | `…requests_are_byte_identical`, `…hashes_both_attempts` |
| B | `d70bf688…59f0` | `5939ce6f…b713` | 2; `307077c9…71a3` | none (64 / 482 passed) |
| B + V1 | `d70bf688…59f0` | `5939ce6f…b713` | 3; `123f6630…4db2` | B's test, `…requests_are_byte_identical`, candidate |
| B + V2 | `8f66789a…ec0f` | `5754f6cf…cca0` | 2; `307077c9…71a3` | B's test, `…hashes_both_attempts`, candidate |
| B + V3 | `8f66789a…ec0f` | `5754f6cf…cca0` | 3; `123f6630…4db2` | `…requests_are_byte_identical`, `…hashes_both_attempts` (B's test passes) |
| C | `d70bf688…59f0` | `5939ce6f…b713` | 2; `307077c9…71a3` | none |
| C + V1 | `8f66789a…ec0f` | `5754f6cf…cca0` | 3; `123f6630…4db2` | `…requests_are_byte_identical`, `…hashes_both_attempts` (candidate passes) |
| C + V2′ | `8f66789a…ec0f` | `5754f6cf…cca0` | 2; `307077c9…71a3` | `…hashes_both_attempts`, candidate |

Two constants across every run:
- The non-elicited pin `d3672ab6…ff5c` and attempt 1's request `32c5900d…092a` are unchanged in every
  run.
- Each option alone gives HEAD's elicited pin, prompt hash and request bytes exactly.

**`pass1_prompt_chars` (R706(f)(ii); `r2/scripts/telemetry_probe.py` → `r2/telemetry_probe.txt`).**
Attempt 2 reads 35,965 at HEAD and under A, B and C. Under V1 it reads 31,914: it no longer counts the
4,051 feedback chars. Under A + V3′ it still reads 35,965 while the user turn sent is 31,914, plus the
feedback in a third message, because A's `run_pass1` keeps computing `pass1_prompt(…, feedback)` for
the telemetry.

**The option table** (R706(c), (d), (f)(ii)). "Catches" means the change cannot land silently: the pin
moves where it should, or the option's own test fails. Existing-test failures are listed above and are
not credited to an option.

| | catches V1 (runtime-only) | catches V2 (hash-only) | a future ruled structure change moves the pin | request bytes changed by the option | `pass1_prompt_chars` changed by the option | files / tests touched |
|---|---|---|---|---|---|---|
| A, one builder | **no**: at the call site (A + V1′) the pin is unchanged. A change inside the builder is seen by both sides | **no**: A + V2′ moves the pin with no request byte changed | **yes if made inside A's builder** (A + V3′ → `8f66789a…`); **no** if made at the call site (A + V1′) | no | no | `engine/elicitation/pipeline.py`, `engine/core/effective_config.py`; no test edited; with any engine fix, `docs/inventory/` (R705(c)) |
| B, structural equality test | **yes, in the suite** (B + V1: B's test fails); the pin does not move | **yes, in the suite** (B + V2: B's test fails) | **yes, when it lands with B green** (B + V3 → `8f66789a…`, B passes). A runtime-only change fails B. *The ruling expected "no"* | no | no | `tests/` (one new file); no engine file |
| C, hash over the runtime-built list | **yes, in the pin** (C + V1 → `8f66789a…`; the candidate passes) | **no**: C + V2′ moves the pin with no request byte changed; the candidate catches it | **yes**, for a change in `run_pass1` or below (C + V1). **Not** for a change in `elicit`'s loop: C calls `run_pass1` directly (READ) | no | no | `engine/elicitation/pipeline.py`, `engine/core/effective_config.py`; with any engine fix, `docs/inventory/` |

**Further options the measurements suggest** (each READ from the table above, not built separately):
- **C + B.** The candidate is present in every run, so the C rows are C + candidate. It catches V1 in
  the pin and V2′ in the suite.
- **A + B.** It catches V1′ and V2′ in the suite, and moves the pin for a builder-located change.

Neither moves the pin for V2-type changes, because no request byte changes there by construction.

## Findings

1. **R706(d)'s expected value for B does not hold as measured.** B + V3: pin `8f66789a…ec0f`, B's
   test passes. B + V1: pin `d70bf688…59f0`, B's test fails. Under B, a structure change can only land
   with the suite green if the render changes too, and the render changing is what moves the pin.
   This is the stop.
2. **A's "should" holds only for changes inside A's builder.** A + V1′ (the call site) leaves the pin
   at `d70bf688…59f0`.
3. **None of A, B or C changes a request byte or `pass1_prompt_chars` on its own.** Each option row
   equals HEAD's pin, prompt hash and request hashes. Under R703(k) this is the input to Q7's
   rehearsal question. Q7 itself is not started.
4. **V2-type changes move the pin under every option, with no request byte changed.** These are
   A + V2′, B + V2 and C + V2′, all `8f66789a…`. No option tested can stop the pin moving for them,
   because the hash path is what the pin is. B and the candidate turn them into suite failures.
5. **C does not see `elicit`.** `sentinel_runtime_messages` calls `run_pass1` per attempt; a change
   in `elicit`'s loop (the attempt count is `MAX_PASS1_ATTEMPTS`, read by both) is outside its
   capture. READ, not exercised.
6. **The call-site exposure is not peculiar to `elicitation_pass1`** (Q3). Every stage's hash renders
   its builder, not its call site. Two stages have a second runtime-only place: the Anthropic arm
   reshapes after the builder, and `preflight` duplicates its expression. READ, not exercised.
7. **Under A, telemetry stops describing what is sent once a structure change is made in the
   builder** (A + V3′: `pass1_prompt_chars` 35,965 against a sent user turn of 31,914).
8. **The codebook is tracked** (0d), so the first worktree's copy was redundant and its timestamps are
   explained.

## Open questions for the ruling

1. Is B's cell in the "moves the pin" column read as measured (yes, when B is kept green), or as the
   ruling stated it (no pin coverage of its own)? Does the answer change what Q5 and Q7 should state?
2. Should Q3's other exposures (the call site at every stage, the Anthropic reshaping, the preflight
   duplication) be weighed in INT-v12-1's options, or kept out of this row?
3. Q5 and Q7 resume after the ruling. Q7's rehearsal input is already measured (finding 3).

## Worktree

`git worktree remove ~/scratch/12w-v1a/wt2` exited 0 on a clean worktree. `git worktree list` shows
only `/home/ankitsarin/projects/evidence-engine  7457b28 [main]`.
