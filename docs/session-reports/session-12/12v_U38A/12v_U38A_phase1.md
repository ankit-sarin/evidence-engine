# 12v-U38A — U38 Phase 1: the acceptance tests for INT-s12-1 (S-1 + S-2), run against the unmodified tree

Session 12v, brief 12v-U38A, under R694(f) and R698(h), 2026-10-23 UTC. Tests only: no engine edit, no
model call, no live write. The tree under test is `553609a` (item 0 of this brief; its engine tree
equals `62d5e7e`'s). The test file is held at
`phase1/tests/test_u38_snippet_precheck.py` and enters `tests/` with the fix (R695(b)).

## 1. What was read (2a)

- **The precheck** (`engine/agents/extractor.py`):

  ```python
  def _has_invalid_snippet(snippet: str | None) -> bool:
      """Return True if snippet is non-null and contains ellipsis bridging."""
      return bool(snippet and INVALID_SNIPPET_RE.search(snippet))
  ```

  `INVALID_SNIPPET_RE = re.compile(r"\[\.{3}\]|\[…\]|…|\.{3,}")` (`engine/core/constants.py`).
  A pure function of the snippet string (I1 holds).
- **Its callers.**
  - `_validate_and_retry_snippets(fields, paper_text, paper_id, *, cfg, …)`: "if not
    _has_invalid_snippet(span.source_snippet): validated.append(span); continue".
  - `_retry_snippet`: "if new_snippet and _has_invalid_snippet(new_snippet): new_snippet = None".
  - **`paper_text` is a parameter of `_validate_and_retry_snippets`** and is in scope at the first call
    (12v_RSA F16). `_retry_snippet` receives it too, as its third argument. Its one production caller
    is `extract_paper`'s legacy body after Pass 2.
- **The replacement site:** in `_validate_and_retry_snippets`, after `for attempt in range(1,
  SNIPPET_MAX_RETRIES + 1)` (`SNIPPET_MAX_RETRIES = 2`):
  "`validated.append(EvidenceSpan(… source_snippet=new_snippet or "", …))`". It can be driven with the
  extractor's `ollama_chat` mocked, so `_retry_snippet`'s own re-check runs for real (I2 holds).
- **The write boundary on this path:** `enforce_citations(span_dicts, …, mode=LEGACY, …)`. Its docstring
  says "Every other value still requires a non-empty snippet", and `check_citations` computes
  `has_evidence = bool(snippet)`. **It checks that a snippet is present, not that it can be placed.**
- **12v_RSA.md:**
  - §Q6: "It checks the snippet alone, although the paper text is in scope. Each flag costs up to 2 retry
    calls … If both fail, a valid snippet is replaced with "", which the legacy write boundary refuses
    (UncitedValueError, retryable)".
  - §Q7: "29 of 190 corpus texts contain an ellipsis".
  - §11's S-1 entry: "`_validate_and_retry_snippets` on a text containing "…" with a snippet copied from
    it makes no `_retry_snippet` call. With a bridging snippet on a text without the ellipsis it makes
    the call. Both pin tuples unchanged." S-2: "a B5 rewrite of
    `test_retry_exhausted_nulls_snippet_preserves_value`; no `UncitedValueError` at the boundary".

## 2. The tests, predicted and observed (2b, 2c)

Predicted before the run, from the reads above: T1 FAIL, T2 PASS, T3 PASS, T4 FAIL, **T5 PASS on HEAD**
(the emptied snippet is refused by the legacy boundary), T6 PASS.

| id | from | asserts | predicted | observed |
|---|---|---|---|---|
| T1 `test_u38_a_verbatim_snippet_containing_an_ellipsis_is_not_flagged` | §11 S-1, R694(f) | a snippet with "…" and one with "..." that are exact substrings of the paper text send no retry and pass through | FAIL | **FAILED**: "a verbatim quote was sent to the retry" (call_count 1) |
| T2 `test_u38_a_snippet_bridging_passages_with_an_ellipsis_is_still_flagged` | §11 S-1 | U14's bridging snippet, absent from the text, is retried once | PASS | PASSED |
| T3 `test_u38_a_snippet_without_an_ellipsis_is_unchanged` | R694(f) | no ellipsis → no retry, snippet unchanged; a snippet absent from the text without an ellipsis is not flagged (the precheck is not a locator; today's behaviour, pinned) | PASS | PASSED |
| T4 `test_u38_exhausted_retries_keep_the_original_snippet` | §11 S-2, R694(f) | the mocked model returns the bridging snippet on both retries; after exhaustion the field keeps the original | FAIL | **FAILED**: `'' == 'Two were lost to follow-up ... Complications were minor.'` |
| T5 `test_u38_the_write_boundary_still_refuses_an_uncited_value` | the brief's T5 | after exhaustion the span reaching `enforce_citations(mode=LEGACY)` raises `UncitedValueError` | PASS | PASSED |
| T6 `test_u38_both_pins_are_unchanged` | R694(f), F14 | both arms' per-stage prompt hashes equal `12v_U37B/pin_tuples_u37.json`'s | PASS | PASSED |

- **Run** (`phase1_run_head.txt`): "2 failed, 4 passed in 0.34s".
- **How it was run.** pytest on the held path with `-p conftest` and `PYTHONPATH=tests`, so the suite's
  fences (`tests/conftest.py`: service calls, Ollama, the experiment lock) applied exactly as under
  `tests/`; `--basetemp` under `~/scratch/12v-u38a/`.

## 3. The census of existing tests (2d)

`census_u38.py` → `census_u38.json`: an AST pass (test functions naming the precheck, its regex, the
retry functions or `SNIPPET_MAX_RETRIES`) and a grep pass. Every AST hit was read; the grep-only files
reference `extract_retry_snippet` as a stage name, or patch `_validate_and_retry_snippets` out.

| test | what it pins | after S-1 + S-2 | Phase 2 action (R690(b)) |
|---|---|---|---|
| `test_extractor.py::test_retry_exhausted_nulls_snippet_preserves_value` | after two failed retries `source_snippet == ""`, value kept, `call_count == 2` | **fails** (S-2 keeps the original) | rewrite under its id: the original snippet is kept, the value kept, two calls |
| `test_extractor.py::test_ellipsis_snippet_triggers_retry`, `::test_bracket_ellipsis_triggers_retry` | `_has_invalid_snippet(snippet)` True with one argument; the retry is called on paper text "paper text" (the snippet is not in it) | pass if `_has_invalid_snippet` still accepts a one-argument call; the behaviour is unchanged (not in the text → flagged) | none, if the signature keeps the snippet-only call valid; otherwise rewrite under its id |
| `test_extractor.py::test_valid_snippet_no_retry` | `not _has_invalid_snippet(snippet)` with one argument; no retry | as above | as above |
| `test_extractor.py::test_invalid_snippet_re_imported_from_constants` | the regex is `engine.core.constants`' | passes (the regex is not changed) | none |
| `test_auditor.py::test_invalid_snippet_ellipsis_bracket`, `…_unicode_ellipsis`, `…_triple_dots`, `test_valid_snippet_no_ellipsis` | the regex itself | pass | none |
| `test_run_fault_predicate.py::test_an_answered_snippet_retry_that_does_not_parse_keeps_its_request_hash` | an unparseable answer keeps its hash; snippet "a ... b" on text "text" sends two retries, `chain == ["b"*64]*2`, `contexts == {}` | passes (not in the text → still flagged; the snippet value is not asserted) | none |
| `test_run_fault_predicate.py::test_run_faults_leave_the_snippet_retry_as_themselves`, `::test_the_complement_is_still_a_failed_snippet_retry` | `_retry_snippet`'s fault handling | pass | none |
| `test_extraction_run_link.py::test_t4_the_snippet_retry_records_its_paper` | `_retry_snippet` records its paper in `run_calls` | passes | none |
| `test_extraction_signatures.py::test_cfg_is_required` | `cfg` required on `_retry_snippet` and `_validate_and_retry_snippets` | passes if `cfg` stays required | none (a constraint) |
| stage-set and sentinel pins (`test_engine_state.py`, `test_presented_context.py`, `test_pin_records.py`, `test_prompt_hash_coverage.py`, `test_request_capture.py`, `test_run_manifest.py`, `test_elicitation_sentinel.py`, `test_u37_feedback_block.py`) | `extract_retry_snippet` in the non-elicited stage set and its template | pass (no stage or template changes) | none |
| tests that patch `_validate_and_retry_snippets` out (`test_audit_stage_completion.py`, `test_floor_and_stored_attempt.py`, `test_text_refusal_at_selection.py`, `test_write_boundary_guard.py`, `test_value_divergence.py`) | not the precheck | pass | none |

**Phase 2's rewrite list:**
- `test_retry_exhausted_nulls_snippet_preserves_value`;
- conditionally the three one-argument precheck calls, if the signature does not keep them valid;
- **T5 itself, whose form the ruling sets** (§5, F1).

## 4. Pins (2b T6)

Both arms' per-stage prompt hashes equal `12v_U37B/pin_tuples_u37.json`: elicited
`elicitation_pass1 5939ce6f…b713`, `extract_pass2 b2a44f28…10fe`; non-elicited `extract_retry_snippet
05a98181…55aa`; and the rest as tabled in the test. By 12v_RSA F14 (measured there by in-memory
variants) neither the precheck nor its regex is rendered into any prompt hash, so the fix moves no pin.

## 5. Phase 2 scope, as this record states it

- **S-1:** `_has_invalid_snippet` consults the paper text that `_validate_and_retry_snippets` already
  holds. A snippet whose ellipsis is verbatim in the text (an exact substring, or the locator's
  `bridged` reading, as U14 made the auditor's) is not flagged.
  - Keep the one-argument call valid (text optional) or rewrite the three census tests under their ids.
  - `_retry_snippet`'s re-check of the retry's own answer may take the text the same way.
- **S-2:** at the replacement site, `source_snippet=new_snippet or span.source_snippet`; the original
  stays when retries run out.
- **T5 (finding F1):** under S-2 the kept snippet is non-empty, and the legacy boundary checks presence,
  not placement, so it will not refuse. "The write boundary decides" therefore means **accepts**. A
  bridging snippet is then stored, and judged later by the audit stage's locator (`bridged`, not
  located). T5 as worded holds on HEAD only because the snippet was emptied. The ruling sets T5's form
  before Phase 2, from three choices:
  - (a) T5 asserts the boundary **accepts** the kept snippet, and the audit-stage locator reports it
    bridged;
  - (b) the legacy boundary gains a placement check (a scope widening, beyond R694(f));
  - (c) S-2 keeps the original only for snippets the paper text contains, and a bridging snippet still
    empties and is refused as today.
- **Expected:** collection 3,370 + 6 = 3,376; gate 3,358 passed / 1 xfailed / 17 deselected on 3,359
  selected (if no test is added or removed beyond these six); no pin moves; no rehearsal.

## 6. Findings

- **F1** — The legacy write boundary checks that a snippet is present, not that it is placeable (`has_evidence = bool(snippet)`), so with S-2 keeping the original non-empty snippet it accepts a bridging snippet; T5 as worded passes on HEAD only because HEAD empties the snippet, and cannot pass after the fix as R694(f) specifies.
- **F2** — `paper_text` is a parameter of `_validate_and_retry_snippets` and of `_retry_snippet`; S-1 needs no new data in scope (I1 holds).
- **F3** — The retry-exhaustion path can be driven with only the extractor's `ollama_chat` mocked, so `_retry_snippet`'s own re-check of the model's answer runs for real (I2 holds).
- **F4** — One existing test pins the "" replacement as a positive property (`test_retry_exhausted_nulls_snippet_preserves_value`); three call `_has_invalid_snippet` with the snippet alone and constrain the S-1 signature.
- **F5** — On HEAD the six tests read 2 failed (T1, T4) and 4 passed (T2, T3, T5, T6), as predicted before the run.
