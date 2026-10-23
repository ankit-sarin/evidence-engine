# 12v-RSA — Phase A, read-only: INT-r12-1 (attempt-2 feedback re-emission) and INT-s12-1 (the extractor's `_has_invalid_snippet` precheck)

Session 12v, brief 12v-RSA, under R693(g), 2026-10-23 UTC. No model call, no Ollama request (every
script that imports the engine refuses socket connects), no engine, test, script, spec or codebook
change, no live write. The live database was opened `mode=ro` once, by `freq_invalid_snippet.py`
(16:10:24Z–16:10:38Z), with the content fingerprint IDENTICAL before and after. No retained
throwaway's `review.db` was opened: only telemetry, spec, codebook and unit-map files beside them.
Tags: **MEASURED** (a committed script's output, quoted), **READ** (code or plan text, quoted),
**INFERRED** (CC's reasoning; not a measurement).

Scripts and outputs in this directory: `census_attempt2.py` → `census_attempt2.json` (Q4);
`render_feedback.py` → `render_feedback.json`, `feedback_12r_p8.rendered.txt`,
`feedback_12r_p12.rendered.txt` (Q2, **rendered**, not stored); `pin_sensitivity.py` →
`pin_sensitivity.json` (Q3); `freq_invalid_snippet.py` → `freq_invalid_snippet_local.json`,
`freq_invalid_snippet_cloud.json` (Q7).

## Q1 — Which path Run 7 runs, and whether each row is on it

- **READ** the live spec, `review_specs/surgical_autonomy.yaml:62`: `  elicitation: false`.
- **READ** R347 (2026-10-01): "ORDER AT THE TAG. Rows → migrations → elicitation flip committed to
  review_specs/surgical_autonomy.yaml → full standard gate → elicited smoke on a throwaway review, on
  that exact tree → tag freshman. Run 7 (12b) runs on the tagged spec at HEAD == the tag; no spec edit
  after the tag."
- **READ** R350: "RUN 7 LAUNCH. From the repo root: scripts/run_pipeline.py --review surgical_autonomy
  --skip-to extract (no --spec, no --max-papers)." (the live spec, so whatever the tag commits).
- **READ** S5d: "Run 7 turns elicitation on deliberately and the manifest records it." Row C1: "Run
  7's deliberate elicitation flip". The P7 line of the package plan: "then the freeze (elicitation
  flip committed → gate → elicited smoke recording the binding pin; …) → tag freshman → Run 7."
  R426 (X3): "the binding value is the one the freeze session's elicited smoke records."
- **No later ruling reverses the flip** (READ: every plan line after line 2000 naming "elicitation
  flip", "the flip" or "elicited path" was read; none withdraws R347).

**Answer.** Run 7's path is settled by ruling: **elicited** (R347, R350, R426). The live spec reads
`false` today because the flip is a freeze-session step, not yet taken.

- **INT-r12-1 is on Run 7's path.** READ `engine/agents/extractor.py` `extraction_stages`: "if
  spec.extraction_models.elicitation: return ("elicitation_pass1", "extract_pass2")"; the attempt-2
  loop is `engine/elicitation/pipeline.py::elicit`.
- **INT-s12-1 is not on Run 7's path.** READ `extract_paper`: "if getattr(getattr(spec,
  "extraction_models", None), "elicitation", False): … return extract_paper_elicited(…)" precedes the
  only call of `_validate_and_retry_snippets` (extractor.py, in the legacy body after Pass 2). READ:
  no module under `engine/elicitation/` references `_has_invalid_snippet`, `INVALID_SNIPPET_RE` or
  `retry_snippet` (grep, every hit read). R349 already recorded "extract_retry_snippet declared but
  uncalled on the elicited path", and R612(d) took that stage out of the elicited stage set.
- **The two rows lie on disjoint paths.** INT-r12-1 is elicited-only; INT-s12-1 is
  non-elicited-only. The only engine code they share is the codebook and `ollama_chat`.

## Q2 — The attempt-2 request (INT-r12-1)

**How it is built** (READ, `engine/elicitation/pipeline.py`):
- `elicit`: "if first.failed_fields and MAX_PASS1_ATTEMPTS > 1: feedback = build_feedback_block(first,
  codebook)"; then `run_pass1(…, feedback=feedback)`. `MAX_PASS1_ATTEMPTS = 2`.
- `run_pass1`: "`feedback` is appended to the prompt STRING, not sent as a separate message";
  `prompt = pass1_prompt(unit_map, codebook, field_names, feedback)`, which is
  `build_pass1_prompt(…) + feedback`.
- `pass1_messages(prompt)`: `[{"role": "system", "content": SYSTEM_PASS1}, {"role": "user",
  "content": prompt}]`.
- So attempt 2 is **two messages, system + one user turn**. **Attempt 1's response is not sent back
  as an assistant turn.** The feedback block is the **last thing in the user turn, after the whole
  numbered paper text**. MEASURED (`render_feedback.json`): `"messages_roles": ["system", "user"]`,
  `"feedback_is_suffix_of_user_turn": true` for 12r papers 8 and 12.
- READ `build_pass1_prompt`: the "## Output" instruction sits **before** "## Numbered Paper Text":
  "Emit exactly one entry per field (20 total), in the order the fields are listed above, with the
  evidence keys BEFORE the value key, as JSON:" (MEASURED, `pass1_output_section_p8`). In attempt 2
  that instruction and the feedback are separated by the entire paper (12r paper 8: attempt-2
  `prompt_eval_count` 20,587, 12r_U13_PA §2).

**`build_feedback_block`** (READ, `engine/elicitation/prompts.py`): for each record with `not rec.ok`,
a block "- **{name}** [{CLASS}]", "you returned value: …", "with unit_indices: […]", the class lines,
and one "✗ {code} — {requirement}" per FATAL code. Header, verbatim: "\n\n## Your previous response
did not meet the contract on {n} field(s)\n\nEach one below shows what you returned and what its
contract requires. Fix these, and re-emit EVERY field in the schema — this is a full replacement
response, not a patch.\n\n". The block names **only the failed fields**. It does not state the field
count (20), the classes not named, or the output shape.

**For a STATED-only attempt-1 failure** (the feedback that preceded every STATED-only attempt 2), the
render from 12r paper 8's stored attempt-1 response, **RENDERED** by HEAD's builders
(`feedback_12r_p8.rendered.txt`, 610 chars, sha256 `5a904b45…c499`):

```text
## Your previous response did not meet the contract on 1 field(s)

Each one below shows what you returned and what its contract requires. Fix these, and re-emit EVERY field in the schema — this is a full replacement response, not a patch.

- **comparison_to_human** [STATED]
    you returned value: 'NR'
    with unit_indices: []
    ✗ VALUE_WITHOUT_CITATION — a value was asserted with nothing cited behind it. The contract is: cite at least one unit that asserts the value, before the value. If nothing in this paper can be cited for it, the value must be NO_EVIDENCE_LOCATABLE with an EMPTY citation list.
```

MEASURED (`render_feedback.json`): rendered length 610 equals the recorded `feedback_chars` 610.
`request_hash(attempt-1 prompt + rendered block)` equals the **stored** attempt-2 request hash
`e14ca615…7658`, and attempt 1's hash also reproduces. Paper 12 (two STATED fields, 978 chars,
`feedback_12r_p12.rendered.txt`) reproduces `91016de9…acec` the same way. HEAD therefore sends today,
byte for byte, what 12r sent for these two papers.

**Template history** (READ, `git log -L`/`-S`): `build_feedback_block`, including the "re-emit EVERY
field" sentence, is unchanged since `dfa1453` (2026-09-05 01:12:06Z), before every census source.
`_requirement` changed once since, at `36de472` (2026-10-11, R571(b)): JUDGMENT's accompaniment
gained "At least one step must cite a unit." That commit is not in 12g's tree and is in 12r's. The
STATED requirement text a STATED-only block carries is the same in every source.

**Prompt text the feedback could conflict with** (READ):
- the STATED class contract: "For each field below: cite at least one unit that asserts the value,
  then state the value." plus R571(a)'s "A field instruction to record NR does not set aside the
  sentinel rule: cite the sentence that states the absence, or output NO_EVIDENCE_LOCATABLE if no
  such sentence exists.";
- "## Output — Emit exactly one entry per field (20 total)";
- `SYSTEM_PASS1`: "For each field, first cite the numbered sentence units that carry the evidence,
  then state the value."

No sentence of the feedback contradicts these. **INFERRED, not tested:** the block's only list is of
STATED fields, it sits after the paper, and it names neither the count nor the other classes. A
response that answers the list's class and stops is consistent with "Fix these" read without "re-emit
EVERY field". This is a hypothesis for a probe, not a finding.

## Q3 — Pin coverage

READ `engine/core/effective_config.py::render_messages`, stage `elicitation_pass1`: "both attempts, as
the audit stage renders both variants — attempt 2 is attempt 1's prompt plus the typed feedback
block". It renders `el.pass1_messages(el.sentinel_pass1_prompt(spec))` and, when
`MAX_PASS1_ATTEMPTS > 1`, adds `el.pass1_messages(el.sentinel_pass1_prompt(spec,
feedback=el.sentinel_feedback_block(spec)))`. The sentinel result reaches all ten FATAL codes
(`_FEEDBACK_SENTINEL`, `sentinel_pass1_result`: "all ten FATAL codes, each class's accompaniment …,
the unresolved-index line, the inference and step lines, and the echo's truncation marker"). It goes
through the runtime's own `build_feedback_block`.

MEASURED (`pin_sensitivity.py`: an in-memory patch per variant, then `12l_U04/pin_tuples.py`
unchanged). Baseline pins: elicited `8f0aeecd…3488`, non-elicited `d3672ab6…ff5c`, equal to the
record.

| variant | elicited pin | stages moved | non-elicited pin |
|---|---|---|---|
| `feedback_header` (the "re-emit EVERY field" sentence reworded) | **moves** | `elicitation_pass1` | unchanged |
| `feedback_requirement` (the STATED requirement line reworded) | **moves** | `elicitation_pass1` | unchanged |
| `precheck_never` (`_has_invalid_snippet` always False) | unchanged | — | **unchanged** |
| `invalid_snippet_re` (the regex matches nothing) | unchanged | — | **unchanged** |

- **A change to the feedback template** moves the elicited pin through `elicitation_pass1` only. The
  non-elicited pin is unchanged. **Confirmed.**
- **A change to the message structure** (for example, attempt 1's response sent back as an assistant
  turn). READ: the render uses `pass1_messages` and `pass1_prompt`, the same two builders `run_pass1`
  sends through. A structure change made in those builders moves the hash. A structure change made
  only inside `run_pass1` or `elicit` (a third message assembled there) would **not** be seen by the
  render unless `render_messages` is changed with it (INFERRED from the code; not exercised). Any
  such option must change the shared builder, and its acceptance test must show the hash move.
- **The attempt-1 echo** (`_echo`, `FEEDBACK_ECHO_CAP = 200`) is inside the rendered sentinel
  (`_SENTINEL_LONG_VALUE` reaches the truncation marker). A change to the echo moves the elicited pin
  the same way (INFERRED from the sentinel table; same render path as the measured variants).
- **INT-s12-1's precheck is invisible to every pin.** The pin tuple hashes the
  `extract_retry_snippet` *template* (`retry_snippet_messages("f", "v", SENTINEL_TEXT)`), not the
  predicate that decides when it is sent.

## Q4 — The denominator (INT-r12-1)

**Sources** (MEASURED, `census_attempt2.json` `sources`, by sha256):

| label | file | what it is |
|---|---|---|
| `12g_ra_12g` | `~/scratch/retained/ra_12g/telemetry/extraction_calls.jsonl` (`e49ead1d…856e`, byte-equal to 12r-U13-PA's `ra_12g_copy`) | 12g Rehearsal A (RA-A design, RA-B the run read-out), 12 papers |
| `12r_ra_12q` | `~/scratch/12r-u13/run/ra_12q/telemetry/extraction_calls.jsonl` | 12r-U13, ra_12q (manifest run 2 — its only extraction run on disk) |
| `sep05_0113` | `data/surgical_autonomy/eval/elicit_design01/scratch/smoke_20260905T011330Z/…` | ELICIT-DESIGN smoke, feedback retry, 3 papers |
| `sep05_0243` | `…/smoke_20260905T024348Z/…` | ELICIT-DESIGN smoke, feedback retry, 1 paper |

**Searched and excluded** (READ; every `extraction_calls*.jsonl` under `~/scratch` and `data/` was
listed):
- the 12r probe (`~/scratch/12r-u13-probe/out/`) sent attempt-1 reproductions only;
- `sep03_1538_aborted` and `sep03_1556` used `identical_reissue` (no feedback block;
  `12h_rd/rd1_attempts_cells.csv` policy column);
- U06's `u06_floor_20261012` attempts came from a fake client (12l_U06: "a fake client at
  `ollama_client._client`");
- rb_12g (spec `elicitation: false`) has no telemetry;
- every other file has no elicited attempts.

**Every case** (MEASURED; shape from each attempt's recorded codes, the rule of
`12r_U13_PA/shape_pa.py`; present fields as STATED/INFERABLE/JUDGMENT of 9/6/5; feedback classes =
the classes of attempt 1's failed fields, which is exactly what `build_feedback_block` lists):

| source | paper | a1 shape (S/I/J) | a1 failed | feedback classes (chars) | a2 shape (S/I/J) | a2 failed | accepted | outcome | a2 text |
|---|---|---|---|---|---|---|---|---|---|
| 12g | 1 | full | 2 | J,S (1057) | full | 3 | 1 | stored | derived |
| 12g | 3 | full | 9 | I,J,S (4413) | partial 9/4/5 | 4 | 2 | stored | stored |
| 12g | 4 | partial 8/6/5 | 9 | I,J,S (4176) | full | 5 | 2 | stored | stored |
| 12g | 5 | full | 8 | I,J,S (3901) | full | 1 | 2 | stored | stored |
| 12g | 6 | full | 1 | J (743) | full | 7 | 1 | stored | derived |
| 12g | 7 | full | 6 | J,S (2524) | full | 6 | 1 | stored | derived |
| **12g** | **8** | full | 4 | **S only** (1710) | **stated_only 9/0/0** | 12 | 1 | stored | **derived** |
| **12g** | **9** | full | 1 | **S only** (610) | **stated_only 9/0/0** | 12 | 1 | stored | **derived** |
| **12g** | **10** | full | 3 | **S only** (1346) | **stated_only 9/0/0** | 11 | 1 | stored | **derived** |
| 12g | 12 | partial 9/5/5 | 3 | I,S (1208) | full | 1 | 2 | stored | stored |
| 12r | 1 | full | 11 | I,J,S (6443) | full | 1 | 2 | stored | stored |
| 12r | 3 | stated_only 8/0/0 | 13 | I,J,S (3413) | full | 1 | 2 | stored | stored |
| 12r | 4 | partial 8/5/5 | 4 | I,J,S (1753) | full | 5 | 1 | stored | stored |
| 12r | 5 | partial 9/5/5 | 13 | I,J,S (6687) | full | 1 | 2 | stored | stored |
| 12r | 6 | full | 1 | I (577) | full | 1 | 1 | stored | stored |
| 12r | 7 | full | 3 | I,S (1324) | full | 0 | 2 | stored | stored |
| **12r** | **8** | full | 1 | **S only** (610) | **stated_only 9/0/0** | 12 | 1 | stored | **stored** |
| 12r | 9 | full | 4 | J,S (1855) | full | 6 | 1 | stored | stored |
| 12r | 10 | stated_only 9/0/0 | 13 | I,J,S (3567) | full | 0 | 2 | stored | stored |
| 12r | 11 | full | 7 | I,S (4350) | full | 0 | 2 | stored | stored |
| **12r** | **12** | full | 2 | **S only** (978) | full | 0 | 2 | stored | stored |
| sep05_0113 | 121 | partial 9/6/1 | 6 | I,J (2268) | full | 0 | 2 | stored | stored |
| sep05_0113 | 604 | full | 5 | I,S (2701) | full | 1 | 2 | stored | stored |
| sep05_0113 | 498 | partial 8/5/5 | 8 | I,J,S (3574) | full | 2 | 2 | stored | stored |
| **sep05_0243** | **498** | partial 8/6/5 | 3 | **S only** (1205) | full | 1 | 2 | stored | stored |

"a2 text": `stored` means the response text exists (`attempts[].raw_content` on 12r, or
`pass1_raw_content` for an accepted attempt). `derived` means the shape is read from the recorded codes
alone. Codes per attempt are in `census_attempt2.json` `cases`. Per-attempt request hashes are stored
on 12r only (`a2_request_hash`).

**Totals** (MEASURED, `totals`):
- **All elicited attempt 2s on disk: 25.**
- **After STATED-only feedback: 6.** Attempt 2 was STATED-only in **4** (12g 8, 9, 10; 12r 8) and
  full in 2 (12r 12; sep05_0243 498). Of the 4 STATED-only, **1 rests on stored text** (12r 8) and
  **3 are derived from codes** (12g 8, 9, 10). Attempt 2 was accepted in 2 of the 6.
- **After any other feedback: 19.** Attempt 2 was STATED-only in **0**: 18 full, 1 partial (12g 3,
  9/4/5). Attempt 2 failed fewer fields than attempt 1 in 13, and was accepted in 13.
- **"4 of 5" reproduces** on its own population (12g and 12r: 12g 3 of 3, 12r 1 of 2), and 3 of the 4
  rest on inference from codes (12r_U13_PA F7: "with no raw text stored, A rather than B is CC's
  inference for them"). Adding the one September case with STATED-only feedback, the census reads
  **4 of 6**.
- **The shape is specific to STATED-only feedback** in this data: 4 of 6 against 0 of 19.
- **Attempt 1 STATED-only (12r 3, 10) is a different case.** Its feedback names all three classes,
  and attempt 2 came back full in both (`a1_stated_only_then_a2`).
- **INFERRED, not tested:** 12r 8 and 12r 12 carry near-identical feedback (one and two STATED
  fields, the same requirement line) on the same model at temperature 0, and diverge (stated_only vs
  full). So the feedback text is not by itself sufficient for the shape.

## Q5 — The cost of no change (INT-r12-1)

READ `terminal.accept_attempt`: "Attempt 2 wins only on STRICTLY fewer CONTRACT_UNMET fields. A tie
keeps attempt 1." A STATED-only attempt 2 fails every INFERABLE and JUDGMENT field (11–12 failed in
the four cases), so it always loses to an attempt 1 whose failures were STATED-only (at most 9).

MEASURED (telemetry rows of the four cases):

| case | accepted | outcome | contract_unmet (= attempt 1's failed fields, kept) | evidenced |
|---|---|---|---|---|
| 12g 8 | 1 | stored | 4 | 16 |
| 12g 9 | 1 | stored | 1 | 19 |
| 12g 10 | 1 | stored | 3 | 17 |
| 12r 8 | 1 | stored | 1 | 18 (+1 NO_EVIDENCE_LOCATABLE) |

**Per occurrence:**
- **Calls:** one extra Pass-1 call carrying the whole paper (12r 8: `prompt_eval_count` 20,587), then
  Pass 2 on attempt 1's record as usual. No call beyond the two Pass-1 attempts: the outer completeness
  budget is not entered (the record is not refused).
- **Events:** the paper is `extracted` (stored), with one `contract_unmet` field event per
  attempt-1 failure. The yield the retry existed to recover (here 9 fields over 4 papers) is lost.
- **The floor cannot be reached from this case.** READ `extraction_events.below_floor`: "return
  held_count(rec) * 2 < expected_count(rec)". A STATED-only feedback means attempt 1 failed at most
  the 9 STATED fields, so at least 11 of 20 are held and the floor (10) is met (INFERRED by arithmetic
  from those two lines and the 9/6/5 codebook split). R668(b)'s "loud under U06's floor — the paper
  fails held_fields_below_floor" does not hold for any measured case or for this case by
  construction. It is **quiet**: a stored paper with fewer fields.
- **Selected again? No.** READ `engine/core/selection.py`: "Skip a paper only when this arm already
  holds a LIVE claim-bearing event (asserted, declined or contract_unmet …)". The kept
  `contract_unmet` claims hold the paper, so later runs do not re-extract it. R619(e)'s "selected
  again on a later run" applies to a record refused at the write boundary, which this is not.
- **Run 7's summary and PRISMA.** MEASURED in 12r_U13 §6 on the same path: field events
  "`contract_unmet 13`", while `run_extraction_summary` reads "`stored_incomplete 0, below_floor 0,
  all_declined 0`" and the run summary counts the paper `audited_ai` / `analysis_ready`. The lost
  fields appear only as field-level `contract_unmet` counts, not in any paper-level summary line.
  PRISMA counts the paper as included (corpus membership is the eligibility axis; READ CLAUDE.md
  "PRISMA … the corpus, from the eligibility axis").

## Q6 — The precheck (INT-s12-1)

READ `engine/agents/extractor.py`:
```python
def _has_invalid_snippet(snippet: str | None) -> bool:
    """Return True if snippet is non-null and contains ellipsis bridging."""
    return bool(snippet and INVALID_SNIPPET_RE.search(snippet))
```
`INVALID_SNIPPET_RE = re.compile(r"\[\.{3}\]|\[…\]|…|\.{3,}")` (`engine/core/constants.py`).

- **Callers.** `_validate_and_retry_snippets` ("if not _has_invalid_snippet(span.source_snippet):
  validated.append(span); continue") and `_retry_snippet` (a retry's own answer is re-checked:
  "if new_snippet and _has_invalid_snippet(new_snippet): new_snippet = None").
  `_validate_and_retry_snippets` has one caller, `extract_paper`'s legacy body after Pass 2.
- **What a True costs.** "for attempt in range(1, SNIPPET_MAX_RETRIES + 1)" with
  `SNIPPET_MAX_RETRIES = 2`: up to **2** `extract_retry_snippet` calls per flagged field. Each sends
  `retry_snippet_messages(field_name, value, paper_text)`, which carries "## Paper Text\n{paper_text}",
  the **whole paper**. The loop stops at the first non-null clean answer.
- **If every retry fails** (null, an answer with an ellipsis, unparseable, or a non-fault exception):
  "source_snippet=new_snippet or """. The field's original snippet is **replaced by an empty
  string**, value kept (pinned by `tests/test_extractor.py::test_retry_exhausted_nulls_snippet_preserves_value`).
  At the write boundary the legacy guard (`mode=LEGACY`) exempts sentinels only: "Every other value
  still needs a quote" (CLAUDE.md, citation_guard). An emptied non-sentinel field raises
  `UncitedValueError`, which is in `RETRYABLE = (IncompleteExtractionError, UncitedValueError,
  ValidationError)`. So a false flag can send the paper back through the outer completeness budget
  (`MAX_COMPLETENESS_ATTEMPTS = 3`), which re-runs Pass 1 and Pass 2. INFERRED from those lines; not
  exercised.
- **Is the paper text in scope at the precheck? Yes, and it is not consulted.** `paper_text` is a
  parameter of `_validate_and_retry_snippets` and is handed to the retry, but `_has_invalid_snippet`
  receives only the snippet.
- **The other two callers the row names, at HEAD.** `locate` still uses the regex but sets "bridged =
  ellipsis and not (located and INVALID_SNIPPET_RE.search(best_window))" (U14, `7074305`).
  `auditor.audit_span` no longer calls the regex: it reads `located.bridged` ("An ellipsis bridges
  passages only when the located text lacks it (INT-i12-8, R675(d))"). Of the row's three callers,
  **only the extractor's precheck still judges the snippet alone** (READ; `git log -S
  INVALID_SNIPPET_RE -- engine/agents/auditor.py` → `7074305`).
- **Tests that pin today's behaviour (B5).** `tests/test_extractor.py`:
  `test_invalid_snippet_re_imported_from_constants`, `test_ellipsis_snippet_triggers_retry`,
  `test_bracket_ellipsis_triggers_retry`, `test_retry_exhausted_nulls_snippet_preserves_value` (and
  the count assertion "mock_retry.call_count == 2  # SNIPPET_MAX_RETRIES").
  `tests/test_auditor.py`: `test_invalid_snippet_ellipsis_bracket`,
  `test_invalid_snippet_unicode_ellipsis`, `test_invalid_snippet_triple_dots`,
  `test_valid_snippet_no_ellipsis` (the regex itself, imported from `engine.core.constants`; the auditor module no longer imports it), plus U14's
  `test_u14_audit_span_does_not_invalidate_a_verbatim_quote_containing_an_ellipsis`.
  `tests/test_locator.py`: U14's `test_u14_*` four.
  Tests that patch `_validate_and_retry_snippets` out (`test_audit_stage_completion.py`,
  `test_floor_and_stored_attempt.py`, `test_text_refusal_at_selection.py`,
  `test_write_boundary_guard.py`) and the stage-set and signature pins (`test_engine_state.py`,
  `test_presented_context.py`, `test_extraction_signatures.py`, `test_run_fault_predicate.py`'s
  handler-1 cases, `test_extraction_run_link.py`) couple to its name and stage, not its predicate
  (READ, grep then each site read).

## Q7 — Frequency (INT-s12-1)

**12s_U14A holds no count** (READ: its §6.2 is a four-case synthetic reproduction; `census_u14a.txt` is
the AST census). 12h RD-2's `rd2_spans.csv` carries a per-span `bridged` column (the regex on the
snippet): MEASURED **0 of 3,760**.

MEASURED by `freq_invalid_snippet.py` (`mode=ro`). **Resolver:**
`engine.core.parsed_text.resolve_parsed_text` + `read_parsed_text`, the paper's current version,
sha256-verified, the text the non-elicited extractor is handed. The locator is run on raw-substring
cases only. Fingerprint IDENTICAL at 16:10:24Z and 16:10:38Z.

| population | spans | with snippet | flagged | of which raw substrings of the resolved text (false flags) | not exact |
|---|---|---|---|---|---|
| **local** (Run 6's `evidence_spans`) | 3,760 | 3,619 | **0** | 0 | 0 |
| **cloud** (proxy: Run 6's `cloud_evidence_spans`) | 7,257 | 6,986 | **1,133** (1,113 `...`, 20 `…`; 167 papers) | **0** | 1,133 (Anthropic 1,102, OpenAI 31) |

Opportunity (same script): **29 of 190** corpus papers' resolved texts contain a regex match (118
matches in all), so a verbatim quote that would trip the precheck is possible on those papers.

**What the counts mean.**
- **The local zero is not a rate.** Run 6's local snippets were stored *after* Run 6's own precheck and
  retry: a flagged snippet was rewritten or emptied before storage, so the stored population is the
  survivors. That is why it holds no ellipsis at all (INFERRED from the code path; the zero is
  MEASURED). Brief assumption I5 holds for the resolver but not for the purpose: these spans cannot
  measure how often the precheck fires.
- **The cloud population was never prechecked.** `engine/cloud/` does not reference the precheck. It
  is a proxy from different models on the same corpus: **0 of 1,133 flagged snippets are verbatim**.
  Every flagged cloud snippet is a genuine bridge or a paraphrase.
- On available data the false-flag rate is **0 measured**; the mechanism (12s_U14A F8) is real, and
  the opportunity exists on 29 papers.
- How often Run 6's local arm fired the precheck at all is **not recoverable**: the snippet retry
  logged at DEBUG, and Run 6 predates `run_calls` (INFERRED; not searched further).

## Q8 — Options, not chosen

### INT-r12-1 (elicited; Run 7's path)

Every option below changes **attempt 2 only**. None touches attempt-1 text, so none is barred by
R670(b) ("No further prompt change before the freshman tag is made to move a first-attempt failure
rate"). Each changes what the elicited run sends, so under R539 it lands before the freeze smoke.

| | option | changes | does not change | pin effect | acceptance test that pins it | what the comparison would have to show |
|---|---|---|---|---|---|---|
| R-0 | **No change** | — | — | none | — | — |
| R-1 | **Name the whole schema in the feedback header**: the field count and each class with its count, "including the N fields not listed below" | the header string | which fields are listed; the acceptance rule; message structure | elicited `elicitation_pass1` → elicited pin moves (MEASURED class: `feedback_header`); non-elicited unchanged | a render test: for a STATED-only attempt-1 result the block names 20 fields and all three classes; the elicited prompt hash moves and the non-elicited one does not | on cases with STATED-only feedback, attempt 2 not STATED-only, with no regression on the 19 other-feedback cases |
| R-2 | **Repeat the output instruction after the feedback** (the "## Output" sentence and shape, at the end of the user turn) | the attempt-2 suffix | attempt 1 (its prompt is a prefix of attempt 2's, unchanged) | as R-1 | render test: the attempt-2 user turn ends with the output instruction; attempt 1's prompt unchanged byte for byte (its hash equal before and after) | as R-1 |
| R-3 | **Multi-turn attempt 2**: attempt 1's response as an assistant turn, the feedback as a new user turn | message structure | attempt 1 | elicited pin moves **only if** the change is made in `pass1_messages`/`pass1_prompt`, which `render_messages` shares (Q3); a change inside `elicit` alone is invisible to the pin | a render test asserting the three-role structure **and** that `elicitation_pass1`'s prompt hash moves; the input-fit guard measures the longer request | as R-1, plus attempt-2 request size against the input-fit ceiling on the largest papers |
| R-4 | **Ask attempt 2 for the failed fields only and merge** | the feedback and the acceptance rule | attempt 1 | elicited pin moves | — | **conflicts with Ruling 4** (CLAUDE.md: "No per-field mixing across attempts: a composite is an extraction no single response produced"); listed for completeness, it needs that ruling reopened |

**Cost of R-0**, measured (Q4, Q5): 4 of 25 attempt 2s on 26 elicited papers (4 of 6 after STATED-only feedback), each one
extra Pass-1 call, 9 fields left `contract_unmet` across 4 papers, every paper stored, none failed,
none re-selected, the loss visible only in field-level counts.

**Evidence limit for any of R-1…R-3.** MEASURED (the census sources): 26 elicited papers carry Pass-1
attempt telemetry, 25 of them an attempt 2; STATED-only feedback occurred on 6 (about 1 paper in 4),
the STATED-only re-emission on 4 (about 1 in 6). A rehearsal of N papers on the natural incidence
would see about N/4 cases with STATED-only feedback; with 12 papers, about three. Showing an effect
against 4 of 6 needs more cases than that, which is why constructed cases are raised in Q9.

### INT-s12-1 (non-elicited; off Run 7's path)

| | option | changes | does not change | pin effect | acceptance test | comparison |
|---|---|---|---|---|---|---|
| S-0 | **No change** | — | — | none | — | — |
| S-1 | **The precheck consults the paper**: flag only when the regex fires on the snippet and the snippet is not a raw substring of `paper_text` (or: use `locate(paper_text, snippet).bridged`, the auditor's rule since U14) | `_has_invalid_snippet`'s signature and its two call sites | the retry template, the stage set, `SNIPPET_MAX_RETRIES` | **none** (MEASURED: `precheck_never` and `invalid_snippet_re` move no pin) | a verbatim quote containing `…` / `...` from a text containing it sends no retry call; a bridging snippet still does; `test_ellipsis_snippet_triggers_retry` and `test_bracket_ellipsis_triggers_retry` keep passing on texts that lack the ellipsis | none possible on Run 7 (path not taken); a non-elicited smoke would show retry calls only for non-verbatim snippets |
| S-2 | **Keep the snippet on exhaustion** instead of `""`, and let the audit-stage locator judge it | the exhaustion branch | the precheck; the retry | none | `test_retry_exhausted_nulls_snippet_preserves_value` is rewritten (B5) to assert the original snippet is kept and no `UncitedValueError` follows | as S-1 |
| S-3 | **Retire the precheck and the snippet retry on the legacy path** (R31: does it serve the engine going forward, if every future run is elicited?) | the non-elicited stage set (`extract_retry_snippet` leaves it) | the elicited path | **non-elicited pin moves** (stage set); elicited unchanged | the stage-set and signature pins rewritten; `extraction_stages` for a non-elicited spec returns two stages | none on Run 7 |

**Cost of S-0**: on Run 7 none (the path is not taken, Q1). On any non-elicited run, per false flag:
up to 2 retry calls carrying the whole paper and, if both fail, a valid snippet emptied and a
write-boundary refusal into the outer budget (Q6). Measured incidence: 0 (Q7). Opportunity: 29 of 190
papers.

## Q9 — Units, numbering and the rehearsal

**§3 as on disk** (READ, `12l_P1A/12l_P1A.md` §3, verbatim headings and the order sentence): "One
handoff per unit (R607(c)). U01–U05 are the pin-affecting units; U06–U11 change what the extract path
stores; U12 protects every model-calling run; U13 is the rehearsal; U14–U17 are audit-side; U18–U25 are
the run's environment and loaders; U26–U36 are P1's tail (R542) and the cloud rows".
- "### U17 — Cross-family rule enforced at run open" — Rows: INT-i12-45; Pin effect: not affecting.
- "### U18 — Digest host equals chat host; digest re-checked per call" — Rows: A-4b; not affecting.
- "### U19 — The server environment is re-read after a restart and recorded" — Rows: INT-j12-17; not
  affecting.
- "### U20 — The input-fit ceiling reads a Modelfile `num_ctx`" — Rows: INT-i12-11; not affecting.
- "### U13 — COMPARISON REHEARSAL (placed step, not a fix unit)". It ran in 12r (`12r_U13/`).

**Proposal (for the ruling):**
- **Two units, not one.** Q1 finds the rows on disjoint paths, and Q3 finds their pin effects
  disjoint: INT-r12-1 moves the elicited pin, INT-s12-1 moves none.
- **Numbers.** §3's numbers are not reassigned (U17 stays INT-i12-45, R692(b)), so the next free
  numbers: **U37 — INT-r12-1** (attempt-2 feedback), **U38 — INT-s12-1** (the precheck).
- **Place.**
  - **U37 next in the order, before U17.** It is the only open pin-affecting P1 unit. §3 puts the
    pin-affecting units first, R540 says "P1 Run-7 path (pin-affecting first)", and R539 requires it
    before the freeze smoke.
  - **U38 with the audit-side group, after U17**, or at P1's tail beside U26–U27 (the other
    off-Run-7-path handlers). It is not pin-affecting and not on Run 7's path.
- **P1's fix-unit count: 35 → 37** (two units), or 36 if the ruling folds both into one batch as
  R675(e)'s "placed with INT-r12-1's batch" reads.
- **The comparison rehearsal — questions for the ruling:**
  - (a) R675(e) places INT-s12-1 "with INT-r12-1's batch and its one comparison rehearsal (R539)". An
    elicited rehearsal cannot exercise the precheck: `extract_retry_snippet` is not in the elicited
    stage set (Q1). Is INT-s12-1 verified by a synthetic acceptance test alone, or by a separate
    non-elicited smoke?
  - (b) U13 is spent. For INT-r12-1, is "one comparison rehearsal" a new elicited rehearsal, or an
    offline replay probe? Natural incidence is about one STATED-only feedback per 4 papers (Q8), so a 12-paper rehearsal would see about three. The
    probe would send stored attempt-1 prompts plus the new attempt-2 suffix for the six cases of Q4,
    like 12r-U13-PROBE, whose requests HEAD reproduces by hash (Q2). It would be a model call, under
    the lock, outside any run, proposed here and not run.
  - (c) Does R670(b) leave an attempt-2-only change open before the tag? Its text bars a change "made
    to move a first-attempt failure rate"; R-1…R-3 move none (INFERRED from the text; the ruling's to
    confirm).

## §10 Findings

- **F1** — Run 7's extraction path is fixed by ruling as elicited (R347's "elicitation flip committed to review_specs/surgical_autonomy.yaml" before the tag, R350's launch on the tagged spec), although the live spec reads `elicitation: false` today.
- **F2** — INT-r12-1 is on Run 7's path and INT-s12-1 is not: `extract_paper` returns `extract_paper_elicited` before the only call of `_validate_and_retry_snippets`, and no module under `engine/elicitation/` references the precheck.
- **F3** — Attempt 2 is one system and one user turn, the user turn being attempt 1's prompt with the feedback block appended after the whole numbered paper text; attempt 1's response is not sent back.
- **F4** — The feedback block lists only the failed fields and says "re-emit EVERY field in the schema" without the field count or the unlisted classes, while the prompt's "Emit exactly one entry per field (20 total)" sits before the paper text.
- **F5** — HEAD's builders reproduce 12r's stored attempt-2 request hashes for papers 8 and 12 (`e14ca615…7658`, `91016de9…acec`), and `build_feedback_block` is unchanged since `dfa1453` (2026-09-05), so every census source ran the same STATED feedback text HEAD sends.
- **F6** — Across all 25 elicited attempt 2s on disk, attempt 2 came back STATED-only in 4 of the 6 that followed STATED-only feedback and in 0 of the 19 that followed any other feedback; three of the four rest on codes alone (12g, no stored text).
- **F7** — "4 of 5" reproduces on 12g and 12r; the one September feedback-retry case with STATED-only feedback recovered (full, accepted), making the census 4 of 6.
- **F8** — Only one 12r elicited run exists on disk (ra_12q, manifest run 2); the 12r probe sent attempt-1 reproductions only.
- **F9** — A STATED-only attempt 2 always loses under `accept_attempt`'s strict-inequality rule; in all four cases attempt 1 was kept and the paper stored, with attempt 1's failed fields (4, 1, 3, 1) left `contract_unmet`.
- **F10** — With STATED-only feedback attempt 1 failed at most 9 of 20 fields, so the floor (`held_count * 2 < expected_count`) cannot be reached and R668(b)'s "loud under U06's floor — the paper fails held_fields_below_floor" does not describe this case: it is a quiet yield loss.
- **F11** — A stored paper holding `contract_unmet` claims is not re-selected (selection skips a live claim-bearing event "asserted, declined or contract_unmet"), so the lost fields stay lost for the run, visible only as field-level `contract_unmet` counts.
- **F12** — A change to the feedback template moves the elicited pin through `elicitation_pass1` and leaves the non-elicited pin unchanged (measured by in-memory variants of `pin_tuples.py`).
- **F13** — A message-structure change made inside `elicit` or `run_pass1` without changing `pass1_messages`/`pass1_prompt` would be invisible to the pin, because `render_messages` renders attempt 2 through those two builders.
- **F14** — INT-s12-1's precheck is invisible to every pin: disabling it or emptying its regex moves neither the elicited nor the non-elicited pin, so "pin-affecting by R612(c)'s reading" describes a change in calls sent, not a pin value.
- **F15** — A flagged field costs up to two `extract_retry_snippet` calls each carrying the whole paper, and on exhaustion its snippet is replaced by `""`, which the legacy write-boundary guard refuses as `UncitedValueError` into the outer budget.
- **F16** — The precheck has the paper text in scope (`paper_text` is a parameter of `_validate_and_retry_snippets`) and does not consult it.
- **F17** — Since U14 (`7074305`) `auditor.audit_span` reads `locate(...).bridged` and no longer applies the regex itself; of INT-s12-1's three named callers, only the extractor's precheck still judges the snippet alone.
- **F18** — Run 6's 3,619 stored local snippets contain no ellipsis at all because they are the precheck's survivors, so they cannot measure the precheck's false-flag rate (brief assumption I5 holds for the resolver, not for the purpose).
- **F19** — On the unprechecked proxy, Run 6's cloud snippets, 1,133 of 6,986 are flagged and 0 of the 1,133 are verbatim substrings of the resolved text; 29 of 190 corpus texts contain an ellipsis.

## §11 Proposed acceptance tests, per option (not chosen)

INT-r12-1 (each a render or unit test, no model call, failing on the unmodified tree):
- **R-1** `test_feedback_after_stated_only_failure_names_the_whole_schema`: build a Pass1Result whose
  only failures are STATED; the block contains the count 20 and the three class names. The elicited
  `elicitation_pass1` prompt hash differs from today's. The non-elicited pin tuple is byte-identical.
- **R-2** `test_attempt2_user_turn_ends_with_the_output_instruction`: attempt 2's user turn ends with
  the output sentence. Attempt 1's prompt is a byte-identical prefix of attempt 2's. Attempt 1's
  request hash is unchanged (12r paper 8's stored `attempt1` hash still reproduces).
- **R-3** `test_attempt2_is_three_messages_and_the_pin_sees_it`: roles system, user, assistant, user.
  `render_messages("elicitation_pass1")` carries the same structure, and its prompt hash moves. A
  request at the input-fit ceiling is refused, not truncated.
- **R-0**: no test; a dated note on INT-r12-1 recording F9–F11.

INT-s12-1:
- **S-1** `test_a_verbatim_quote_containing_an_ellipsis_is_not_retried`: `_validate_and_retry_snippets`
  on a text containing "…" with a snippet copied from it makes no `_retry_snippet` call. With a
  bridging snippet on a text without the ellipsis it makes the call. Both pin tuples unchanged.
- **S-2** `test_retry_exhaustion_keeps_the_original_snippet`: a B5 rewrite of
  `test_retry_exhausted_nulls_snippet_preserves_value`; no `UncitedValueError` at the boundary.
- **S-3**: `extraction_stages` on a non-elicited spec returns two stages; the stage-set pins
  (`test_engine_state.py`, `test_presented_context.py`) rewritten; the non-elicited pin moves.
- **S-0**: no test; a dated note on INT-s12-1 recording F2, F14, F17–F19.
