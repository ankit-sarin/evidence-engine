# ELICIT-DESIGN-02 — the four smoke rulings, implemented and re-smoked

**Machine:** DGX (`~/projects/evidence-engine`) · **HEAD at STEP 0:** `ea9f806`, tree clean,
local == remote (verified by VERIFY-EXIT-01 the same session).
**Status:** STEP 1 implemented and committed, STEP 2 smoke run. **STOPPED for gate
adjudication.** Nothing pushed. Directional measures are reported, not self-adjudicated.

---

## 1. STEP 0 findings, as acknowledged

- **I1 CONFIRMED** — Pass 1 elicits all 20 fields in one request (`pipeline.py` one
  `ollama_chat` per attempt; the prompt says "Emit exactly one entry per field (20 total)").
  So the feedback block is a single appended section, not something threaded per call.
- **I2 CONFIRMED** — `"NR"` is the first of six `absence_sentinels`, distinct from
  `escape_token`.
- **I3 CONFIRMED, and richer than assumed** — `FieldRecord` already carried field, class,
  value, valid indices, **verbatim** unresolved indices, inference, steps and a closed
  violation vocabulary. Caveat recorded: codes are per-field, not per-offence-instance, so the
  builder attributes content to codes rather than assuming one code ⇒ one artefact.
- **I4 RESOLVED as Option A′ (D1)** — `evidence_spans.value` is the only unconstrained slot
  (`audit_status` carries a CHECK constraint, so a new state there is a migration, refused by
  D3). Five consumers read that column at face value; all five now read one codebook-derived
  authority.
- **0.3 CONFIRMED** — `enforce_fit` counts anything concatenated into the prompt string, so the
  feedback block is sized for free. Worst-case attempt 2 projected ≥35k tokens of headroom on
  the largest paper. **The smoke came in far under the projection** (§4.6).

---

## 2. Implementation, per ruling

| ruling | commit | what landed |
|---|---|---|
| 1 (token + 3a/3b codebook) | `21731d8` | `contract_unmet_token`, `non_value_tokens()`, evidence-modality lint, `key_limitation` rewritten, both `source_quote_required` flags removed |
| 1 (terminal states) | `a5abcbb` | `elicitation/terminal.py`, `enforce_terminal_states`, `TerminalStateError`, citation guard learns the second non-value token |
| 3(c) | `7a15e3d` | `DIRECTLY_STATED` branch in the INFERABLE contract |
| 2 + 4 (prompt side) | `dfa1453` | 20 per-field escape lines, worked example, sentinel rule, typed feedback builder |
| 1 + 4 (engine side) | `a87db2b` | two-attempt Pass-1 loop, conservative acceptance, Pass-2 skip, `Pass1ContractError` deleted |
| D1/D2 | `3424c90` | five downstream consumers taught both tokens |
| addendum | `d3de9c7` | dated correction of ELICIT-DESIGN-01's stale status header |

**Offline gate: 1,712 passed, 15 deselected (205 s)** — 1,636 prior + 76 net new. All 1,636
pre-existing tests pass. One deliberate replacement:
`test_contract_violation_stops_the_paper_before_pass_2` asserted the paper-level refusal
Ruling 1 retired, and three successors assert the new policy in its place.

**Two scope notes.**

- **The Ruling 3(b) lint caught a second field.** `clinical_readiness_assessment` carried the
  identical `source_quote_required: true` on the identical JUDGMENT class. Gate 2 requires the
  lint to PASS post-fix, so its flag was removed. Ruling 3(a) authorised an instruction
  rewrite for `key_limitation` only, so `clinical_readiness_assessment`'s instruction still
  reads *"Quote the key evidence behind your assessment"* — the same F5 defect one layer down,
  in prose no flag-level lint reaches. **Not changed. Architect's call.**
- **`Pass1ContractError` was deleted, not left unraised.** It expressed exactly the
  paper-level refusal Ruling 1 replaced. `TerminalStateError` subclasses
  `IncompleteExtractionError`, so the one bounded retry budget still covers the boundary.
  **Consequence: CLAUDE.md's Write-Boundary section still lists it in that budget and is now
  stale.** That is the user's file and was not edited.

---

## 3. The smoke — same instrument, new run id

`analysis/eval/elicit_design01/smoke.py`, unmodified, run id `smoke_20260905T011330Z`. Prior
scratch (`smoke_20260903T155654Z`, `aborted_smoke_20260903T153852Z`) untouched — the harness
refuses to reuse a run id. Ollama 0.21.0 throughout, `NRestarts=0`,
`ExecMainStartTimestamp` unchanged, no restart, no config change. Production `review.db` mtime
**2026-07-27 19:47:48**, byte-identical before and after.

### 3.1 Side by side

| measure | ELICIT-DESIGN-01 (2026-09-03) | ELICIT-DESIGN-02 (2026-09-05) |
|---|---|---|
| **papers stored** | **0 / 3** | **3 / 3** |
| Pass-1 calls | 9 (3 × 3) | 6 (3 × 2) |
| Pass-2 calls | 0 | 3 |
| failures/attempt, p121 | 6 → 7 → 7 | **6 → 0** |
| failures/attempt, p604 | 2 → 5 → 5 | **5 → 1** |
| failures/attempt, p498 | 13 → 13 → 13 | **8 → 2** |
| accepted attempt | — (none stored) | **2, 2, 2** |
| violations, accepted attempt | — (final attempt: 7 / 5 / 13) | **0 / 1 / 2** |
| violation totals, accepted | — | `VALUE_WITHOUT_CITATION` 2, `INFERENCE_MISSING` 1 = **3** |
| violation totals, all attempts | 54 / 8 / 6 / 3 = **71** over 180 entries | 13 / 6 / 2 / 2 = **23** over 120 entries |
| **escape-token uses** | **0 / 180** | **9 / 60 accepted** (15 / 120 all attempts) |
| `NR` uses | 23 | **1** |
| **uncited sentinels** | **19** | **1** |
| `DIRECTLY_STATED` uses | n/a | 1 |
| malformed / out-of-range / duplicate indices | 0 / 0 / 0 | **0 / 0 / 0** |
| parse path | `direct` 9/9 | **`direct` 6/6** |
| truncation tripwire | 0 | **0** |
| latency (121 / 604 / 498) | 581 / 665 / 837 s | 600 / 643 / 963 s |

The prior-run column was recomputed from that run's own telemetry by the same script that
produced the new column, and reproduces F1 and §10.2 exactly (23 `NR`, 19 uncited, 54/8/6/3).
The method is therefore not doing the work.

### 3.2 Terminal states, per paper

| paper | EVIDENCED_VALUE | NO_EVIDENCE_LOCATABLE | CONTRACT_UNMET | fields refused |
|---|---:|---:|---:|---|
| 121 | 15 | 5 | **0** | — |
| 604 | 17 | 2 | **1** | `country` |
| 498 | 16 | 2 | **2** | `comparison_to_human`, `secondary_outcomes` |
| **total** | **48** | **9** | **3** | of 60 |

60 spans across 3 extractions in the scratch DB — 20 per paper, every field, as Ruling 1
requires.

### 3.3 What the feedback block actually did

| paper | attempt 1 failures | feedback chars | attempt 2 failures |
|---|---|---:|---|
| 121 | `autonomy_level`, `system_maturity`, `study_design`, `country`, `key_limitation`, `clinical_readiness_assessment` | 2,268 | **none** |
| 604 | `task_monitor`, `task_select`, `task_execute`, `comparison_to_human`, `secondary_outcomes` | 2,701 | `country` |
| 498 | `surgical_domain`, `autonomy_level`, `system_maturity`, `study_design`, `country`, `primary_outcome_value`, `key_limitation`, `clinical_readiness_assessment` | 3,574 | `comparison_to_human`, `secondary_outcomes` |

Attempt 2 improved on attempt 1 on **all three** papers, so the strict-inequality rule accepted
it every time and the tie-break branch was not exercised in the smoke. It is exercised by test.

**Attempt 1 is not the prior run's attempt 1** and must not be read as one: the prompt changed
(Ruling 2's twenty escape lines, the worked example, the sentinel rule, the `DIRECTLY_STATED`
branch), so the two attempt-1 columns are different instruments. p604 got worse at attempt 1
(2 → 5) and p498 better (13 → 8); with n=3 neither is a signal. The comparison that carries
information is the **stored** row, and the **accepted-attempt violation totals**.

### 3.4 F1, answered

`key_limitation` and `clinical_readiness_assessment` — the two fields Ruling 3 addressed — both
failed attempt 1 on p121 and p498 and both cleared on attempt 2. `comparison_to_human`, the
field ELICIT-DESIGN-01 called "the maximum possible, every attempt of every paper", is now the
escape token on p121 and p604 and CONTRACT_UNMET on p498 — never a bare uncited assertion.

The escape token went **0 → 9 uses** and `NR` went **23 → 1**. F1's diagnosis was that the
model had a sentinel habit and the token had no purchase against it from a preamble. Put on
every field's response-format line, the token is reached for.

### 3.5 A finding: "verbatim by construction" is true at word level, not byte level

The harness's `snippet_verbatim` check reported **14 / 16** on p498 (121 and 604 were clean).
Root-caused, not patched, per the no-mid-smoke-patching constraint:

- `robot_platform` cites **one** unit, [S839], so no join is involved. The unit itself is not a
  substring of the source.
- `country` cites [S31, S32], a contiguous run joined with a space.

Both diverge from the source in **whitespace only**. `units.py` collapses the paper's `\n\n`
into a single space when building units — the source reads `"## Notes\n\n1. Examples of…"` and
`"…Hong Kong, 1/f,\n\nAB1, CUHK…"`. Under `re.sub(r"\s+", " ")` both stored snippets are exact
substrings of the source. **No fabrication, no stitching: the words and their order are the
paper's.**

Why it matters anyway. The design note says a materialized quote is "ANCHORED by construction"
and warns that anchored rates from this path would be uninformatively high. **The bias runs the
other way for any span crossing a line break**: the frozen v1.1 ladder tests exact substring
containment, so such a span scores NOT anchored. This is a property of the ELICIT-01 segmenter
that this task inherited, it is pre-existing, and it is **not** what the smoke was gating. It
belongs with the queued parse-quality-gate work.

---

## 4. Acceptance gates

| # | gate | result |
|---|---|---|
| 1 | full suite green; pre-existing tests pass or deliberately updated | **1,712 passed**, one explained replacement |
| 2 | codebook lint fails pre-fix, passes post-fix (both demonstrated) | **PASS** — `test_the_pre_fix_codebook_is_caught` reconstructs the pre-fix codebook and catches both fields; `test_the_shipped_codebook_is_clean` |
| 3 | no uncited non-escape value storable; partial writes impossible | **PASS** — `test_no_uncited_non_escape_value_can_be_stored_as_a_value` (sentinels included), `test_all_twenty_states_land_in_one_transaction`, `test_a_failing_span_rolls_the_whole_paper_back` against real SQLite |
| 4 | a regressing attempt 2 is discarded | **PASS** — `test_a_regressing_attempt_2_is_discarded`, `test_a_tie_keeps_attempt_1` (the strict-inequality boundary) |
| 5 | smoke mechanics zero-defect; production `review.db` untouched | **PASS** — 0 malformed, 0 out-of-range, 0 duplicates, tripwire 0, parse `direct` 6/6, mtime unchanged |

### 4.6 Sizing, measured

| paper | attempt-2 prompt | estimate | actual `prompt_eval_count` | over-prediction |
|---|---:|---:|---:|---:|
| 121 | 55,462 ch | 27,135 | 13,054 | 2.08× |
| 604 | 74,118 ch | 36,263 | 18,491 | 1.96× |
| 498 | 188,675 ch | 92,311 | **48,763** | 1.89× |

p498's attempt 2 used **48,763 of 131,072 tokens — 37% of the ceiling**, against the STEP 0
projection of 95,523 on the conservative estimator. Ruling 2 cost +3,843 prompt chars per paper
(estimated +4,005) and the feedback blocks ran 2,268–3,574 chars against a 9,977-char worst
case. Headroom is not close.

---

## 5. Directional measures — for the architect, not self-adjudicated

| # | measure | observed |
|---|---|---|
| 6 | 3/3 papers reach full terminal-state coverage and are stored | **3/3 stored**, 60/60 fields carry a terminal state |
| 7a | escape-token uses > 0 | **9** on accepted attempts (was 0) |
| 7b | uncited-sentinel firings < 19 | **1** |
| 7c | accepted-attempt violations < prior final-attempt totals (7 / 5 / 13) | **0 / 1 / 2** |
| 7d | feedback-retry delta per paper | 121: 6 → 0 · 604: 5 → 1 · 498: 8 → 2 |

**Verdict: left blank for the architect.**

---

## 6. Recorded, not authorized

- **N1** — `extraction_validator.normalize_prefix`'s UPDATE path silently rewrites unrecognised
  values whenever they are an unambiguous prefix of one enum member. Pre-existing
  silent-fallback defect; the non-value tokens are now guarded in front of it, nothing else is.
- **N2** — divergent hand-lists: two `_ABSENCE_VALUES` in `auditor.py` (4 members and 5, the
  second adding `"Not assessable"`), the validator's `("NOT_FOUND", "NR")`, the monitor's
  `_NULL_SYNONYMS`. All differ from the codebook's own `absence_sentinels`.
- **N3 (new, this task)** — `clinical_readiness_assessment`'s instruction still demands a quote
  on a JUDGMENT field (§2).
- **N4 (new, this task)** — CLAUDE.md's Write-Boundary section lists the deleted
  `Pass1ContractError` (§2).
- **N5 (new, this task)** — unit text collapses source newlines to spaces, so a materialized
  span crossing a line break is not a byte-exact substring of the paper (§3.5).
