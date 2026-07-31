# INSTRUMENT-01 — Write-boundary completeness enforcement + response telemetry

Project: evidence-engine · Machine: DGX (spark-59e4) · Date: 2026-07-28
Commits: **`a90bbfe`** (telemetry) → **`ee6a82b`** (guard + retry). Clean tree.

**Zero Ollama calls, zero cloud API calls, no extraction runs, no schema changes.**
Full offline suite: **1437 passed, 15 deselected** (was 1417; +20 new guard tests).

---

## 1. Guard locus per arm

One predicate — `engine/core/completeness.py:enforce_completeness()` — called at every write
boundary. Nothing hardcodes a field count.

| arm | guard locus | fires before |
|---|---|---|
| openai_o4_mini_high | `engine/cloud/base.py:254` (`store_result`) | the `INSERT INTO cloud_extractions` at `base.py:270` |
| anthropic_sonnet_4_6 | `engine/cloud/base.py:254` — same method, inherited | same |
| local (deepseek-r1:32b) | `engine/agents/extractor.py:425` (`extract_paper`) | `db.add_extraction_atomic` at `:441` |

The local guard sits in the extractor rather than in `ReviewDatabase.add_extraction_atomic`
because the database layer is generic — it serves migrations and tests and has no `ReviewSpec`
from which to derive an expected field set. That reasoning is recorded in a comment at the
call site rather than left implicit.

**Expected field set — single source, codebook-derived.** `expected_field_names(spec,
codebook_path)` (`completeness.py:97`) traverses `spec.extraction_schema.fields_by_tier(1..4)`
— the *same traversal* `build_extraction_prompt` uses to decide which fields to ask for — and
cross-checks the result against the codebook YAML, logging any divergence. The spec drives
because it is what the prompt asks for; the codebook is the cross-check. A test asserts the two
agree today (`test_expected_set_matches_the_codebook`) and another asserts the set is in prompt
order. A codebook that grows to 25 fields moves the guard, the prompt and the test fixtures
together.

The old check is retained beneath the new one: `if not spans: raise ValueError` still fires
first for a genuinely empty result, so its existing test coverage and error message are intact.

---

## 2. Retry policy as implemented

Identical on every arm, budget `MAX_COMPLETENESS_ATTEMPTS = 3` (`completeness.py:37`).

| | cloud | local |
|---|---|---|
| driver | `CloudExtractorBase.extract_with_completeness` (`base.py:305`) | `extract_paper_with_completeness` (`extractor.py:517`) |
| call sites | `openai_extractor.py:169`, `anthropic_extractor.py:198` | `extractor.py:670` |

Behaviour:

1. Issue the **identical** request — same prompt, same `response_format`, same parameters. The
   failure being guarded against is stochastic response *shape*, not a prompt defect, so
   re-asking the same question is the remedy; changing the question would change the extraction
   contract mid-run.
2. Check completeness. Write a telemetry row **before** accepting or rejecting, so an exhausted
   paper leaves a full record of what each attempt returned.
3. Complete → return (and log at INFO if it took more than one attempt).
4. Incomplete and attempts remain → WARN naming the missing-field count and the salvage that
   fired, then retry.
5. Exhausted → ERROR, raise `IncompleteExtractionError`. **Nothing is written.** The caller
   marks the paper FAILED and continues:
   - `openai_extractor.py:172-178` and `anthropic_extractor.py:200-206` catch
     `IncompleteExtractionError` **before** their generic `except Exception`, so an exhausted
     paper is not re-multiplied through the existing 3-attempt API-error loop (which would cost
     up to 9 calls).
   - The local loop's existing `except Exception` marks `EXTRACT_FAILED` and reports FAILED.

There is no "store what we got" path. That path is what produced the 21 collapsed extractions.

---

## 3. Salvage demotion

The `wrap-single-span-in-list` branch (`base.py:157-165`) and the `flat_field_dict` branch
(`base.py:166-172`) remain as parse aids, with three changes:

- **Distinct logging.** Each emits `SALVAGE <name>: …` at WARNING. The single-span message says
  in terms: *"This is NOT a complete extraction and must pass the completeness guard."*
- **Recorded.** `parse_response_to_spans` sets `self._last_salvage` (reset at entry), which the
  retry driver reads into the telemetry row and `store_result` passes into the guard, so an
  `IncompleteExtractionError` names the repair that was attempted.
- **Subordinated.** Salvaged output flows through `enforce_completeness` exactly like any other
  result. A salvaged single span can never again be stored as a complete extraction — it may
  repair a *shape*, it may never certify a *result*.

---

## 4. Telemetry schema

`engine/core/extraction_telemetry.py`, schema id `extraction-telemetry-1`. One JSON object per
line, appended to `data/<review>/telemetry/extraction_calls.jsonl` — inside the already
gitignored `data/` tree, per existing convention.

| field | notes |
|---|---|
| `schema`, `ts` | version tag, UTC ISO timestamp |
| `arm`, `paper_id`, `attempt`, `outcome` | `outcome` ∈ `stored` / `incomplete_retry` / `incomplete_exhausted` / `error` |
| `model` | model string as sent |
| **`finish_reason`** | the provider's own field **verbatim**: `finish_reason` (OpenAI), `stop_reason` (Anthropic), `done_reason` (Ollama). Deliberately not normalized into a common vocabulary — the values stay traceable to their source. |
| **`raw_content`** | the pre-parse response string, capped at 200,000 chars |
| `raw_content_chars`, `raw_content_truncated` | true length and an explicit truncation flag — never silent |
| `spans_parsed`, `fields_expected`, `missing_fields` | the guard's own diagnosis |
| `salvage` | which repair branch fired, if any |
| `input_tokens`, `output_tokens`, `reasoning_tokens` | as reported by the provider |
| `error` | populated on the error path |

Capture points (response handling only): `openai_extractor.py:84-86`,
`anthropic_extractor.py:85`, and `extractor.py:281-286` for the local arm, where Pass 2 stashes
the raw response and `done_reason` in a module-level dict read by the driver — mirroring the
`_last_salvage` pattern so no function signature changes.

`record_call` never raises; a telemetry failure logs a warning and the run continues.

---

## 5. Test inventory added

`tests/test_completeness_guard.py` — **20 tests**:

*Expected field set (3)* — derived not hardcoded; matches the codebook; in prompt order.
*check_completeness (6)* — complete passes; missing named; **single span is incomplete**; empty
is incomplete; unexpected/duplicate reported but not fatal (covers the local arm's `Title` /
`field_1` junk names); accepts objects as well as dicts.
*enforce_completeness (2)* — passes complete; **raises on the paper-277 shape**, asserting
`n_stored == 1`, `salvage == "single_span_dict"`, `study_type` absent from `missing` and
`robot_platform` present.
*Telemetry (5)* — records a call including `finish_reason` and round-tripped `raw_content`;
one line per attempt; truncation flagged with true length; never raises on a bad path;
malformed lines skipped on read.
*Retry driver (4)* — stops as soon as a complete result arrives (2 calls, outcomes
`incomplete_retry`→`stored`); **paper-277 regression: exhausts all 3 attempts, raises, writes 3
rows ending `incomplete_exhausted`, stores nothing**; `finish_reason`/`raw_content`/tokens
recorded per attempt; a complete first attempt makes exactly one call.

**Gate 2 — the paper-277 regression.** `PAPER_277_RESPONSE` reproduces the collapsed response
shape from SPANLOSS-01 §2 verbatim (bare, unwrapped, syntactically complete single span with
the real snippet). `test_paper_277_regression_exhausts_and_never_stores` asserts it is retried
to exhaustion and never stored; `test_enforce_raises_on_the_paper_277_shape` asserts the guard
rejects it directly.

**Existing fixtures updated, not weakened.** Seven pre-existing tests failed on first run
because their fixtures stored 1–7 of 20 fields — the guard working as intended. Store-path and
run-path fixtures now use a **spec-derived complete field set** (`COMPLETE_FIELDS` /
`_complete_pass2_fields`), so they track the codebook rather than pinning a count; parse-only
tests keep their small payloads, since the guard is not in play there. Two assertions were
scoped to the field under test rather than to `spans[0]`.

---

## 6. Diff-scope assertion — request paths untouched (Gate 3)

Mechanically checked against `HEAD~2`; every grep returned empty:

| checked | result |
|---|---|
| `build_extraction_prompt` body, `## Instructions` block, `source_snippet` clause, allowed-values rendering, `schema_text` / `field_blocks` assembly | **no changed lines** |
| OpenAI `chat.completions.create`, `reasoning_effort`, `response_format`, `messages=`, roles, `model=` | **no changed lines** |
| Anthropic `messages.create`, `max_tokens`, `thinking=`, `system=`, `budget_tokens`, `model=` | **no changed lines** |
| Local `ollama_chat(`, `format=`, `options=`, `think=`, temperature, message roles | **no changed lines** |

The extraction contract is byte-identical. Every change is in **acceptance** (guard, retry) and
**observation** (logging, telemetry). Diffstat: `engine/agents/extractor.py` +111,
`engine/cloud/base.py` +121, `engine/cloud/openai_extractor.py` +21,
`engine/cloud/anthropic_extractor.py` +16, plus two new engine modules and test updates.

---

## Acceptance gates

| gate | status |
|---|---|
| 1. Guard live on all three arm write paths, codebook-derived field set | ✅ §1 — `base.py:254` (both cloud), `extractor.py:425` (local); set derived from spec + codebook cross-check, no hardcoded count |
| 2. Paper-277 regression passing | ✅ §5 — retried to exhaustion, raises, stores nothing |
| 3. Diff-scope: no prompt or request-payload changes | ✅ §6 — four mechanical greps, all empty |
| 4. Telemetry fields persisted and covered by a test | ✅ §4, §5 — 5 telemetry tests incl. `finish_reason` and raw-content round-trip |
| 5. Full suite green | ✅ 1437 passed, 15 deselected |

## One thing worth flagging before the repair run

The guard makes an incomplete extraction a **failed paper**, which is correct but changes
run economics: an arm that collapses stochastically at ~9% (SPANLOSS-01's measured openai rate)
will now spend up to 3× the calls on those papers and still fail some of them. If the repair
run leaves papers unfixable after three attempts, that is a real signal about the arm, not a
guard defect — and the telemetry sidecar will, for the first time, say whether the response was
truncated (`finish_reason`) or simply short.

**Out of scope and not done:** re-extraction, strict JSON schema / structured outputs,
pairs-CSV or census regeneration, downstream dependency map, `NOT_FOUND` schema.
