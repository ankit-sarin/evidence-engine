# 12v-U37A — U37 Phase 1: the acceptance tests for options R-1 + R-2, run against the unmodified tree

Session 12v, brief 12v-U37A, under R694(e), (i), 2026-10-23 UTC. Tests only: no engine edit, no model
call, no Ollama request, no live write. The tree under test is `1e15b50` (item 0 of this brief; its
engine tree equals `cc2d83a`'s).

## 1. The block as it stands (2a)

`feedback_block_head.txt` is 12v_RSA's Q2 render of 12r paper 8's attempt-2 feedback, copied by hash
(`5a904b454f7bbe7c51ed130045d2de723cf7d6ae32760678bfbaae80dcbfc499`, equal to
`12v_RSA/feedback_12r_p8.rendered.txt`). It lists one STATED field. It asks to "re-emit EVERY field in
the schema" and gives neither the count nor the unlisted fields.

## 2. The tests (`tests/test_u37_feedback_block.py`, sha256 `ba86e629…0af`)

| id | from | asserts | HEAD |
|---|---|---|---|
| T1 `test_u37_the_feedback_header_names_every_schema_field` | §11 R-1 ("the count 20 and the three class names"), widened by R694(e) ("every field, with the count") | for a STATED-only failure the block names all 20 live-codebook fields, states 20, and names STATED, INFERABLE and JUDGMENT | **FAILED**: "the block names 1 of 20 fields" |
| T2 `test_u37_the_output_instruction_is_repeated_after_the_feedback` | §11 R-2 | attempt 2's user turn carries "Emit exactly one entry per field" after the feedback heading, and still before "## Numbered Paper Text" | **FAILED**: "the output instruction is repeated after the feedback block" |
| T3 `test_u37_attempt_1_prompt_text_is_unchanged` | §11 R-2, R670(b) | the attempt-1 system + user messages hash to the HEAD fixture, and attempt 2 begins with them byte for byte | PASSED |
| T4 `test_u37_the_non_elicited_pin_is_unchanged` | §11 R-1, R694(e) | the non-elicited pin, recomputed as `12l_U04/pin_tuples.py` does, equals `d3672ab6…ff5c` | PASSED |
| T5 `test_u37_a_non_stated_feedback_block_still_lists_only_its_failed_fields_codes` | R694(e) ("only the header and the trailing instruction differ") | for a mixed failure the '- **name** [CLASS]' entries carry exactly the three failed fields, byte-identical to HEAD | PASSED |

Run: `phase1_run_head.txt`, "2 failed, 3 passed in 0.28s" (`--basetemp` under `~/scratch/12v-u37a/`).
Each failure is at the assertion its docstring predicts.

**Fixtures** (`fixtures_head.json`, built by the test module's own helpers):
- `ATTEMPT1_MESSAGES_SHA256` `d3c047de668aa59bcf6e486a747843a8cb7c3d724fe67ccb69ebfa10e4101e31`, over
  `fixture_attempt1_messages.json`: a four-unit fixed paper and the live codebook.
- `MIXED_ENTRIES_SHA256` `5f8c11217eec7daebe25d5654c2cd50b6a8145c8822860bd35559df346d590b7`, over
  `fixture_mixed_entries.txt`; failed fields `autonomy_level`, `robot_platform`, `study_type`.
- The STATED-only block on that paper: `fixture_stated_only_block_head.txt`, 601 chars,
  `6cfa960d…6e20`.

**Constraints the tests put on Phase 2:**
- T5 requires that passing fields are not rendered in the failed-entry form ("- **name** [").
- T4 carries the installed client-library versions and the codebook's semantic hash, so a library
  upgrade moves it too.

## 3. Pin preview (2d)

`pin_preview_head.json` is `pin_tuples.py` at this tree, byte-identical to
`12u_close/pin_tuples_close.json` (`08f1374c…55e6`):
- elicited `8f0aeecdeda659a13b3c182fabdc2568523f38891cd692f0a8cf9ce629de3488`;
- non-elicited `d3672ab6fb1d9b627b9fa8dd19112e148e88cf174b1d854bb0b7fff004f7ff5c`;
- `elicitation_pass1` prompt hash `18ac4255473a556b83e3aefffdc48de0ca2ae1d6826322deccb39175927f2f1c`.

By 12v_RSA Q3 (F12), the template edit moves the elicited value through `elicitation_pass1` and must
leave the non-elicited one (T4). The post-fix elicited value is measured in Phase 2, not predicted here.

## 4. Census of existing tests (2e)

`census_phase2.py` → `census_phase2.json`: an AST pass (test functions naming a feedback, attempt-2
or pin builder) and a grep pass (the block's distinctive strings, pin literals, `elicitation_pass1`).
Every hit was read. The 14 `tests/analysis/paper1/test_judge*.py` hits are the judge's own `run_pass1`
and `prompt_hash`, unrelated. No test carries an elicited pin literal (`8f0aeecd`, `77ae99d8`: no
hits).

| test | what it pins | after R-1 + R-2 | Phase 2 action (R690(b)) |
|---|---|---|---|
| `test_feedback_block.py::test_clean_fields_are_not_mentioned` | `"robot_platform" not in block` for a passing field | **fails**: R-1 names every field | rewrite: a passing field is not listed as a failed entry (T5's form) |
| `test_elicitation_pipeline.py::test_the_elicited_requests_are_byte_identical` | `sha256_canonical(sent) == ELICITED_REQUESTS_SHA256` (`a56ac21b…be0`) over both Pass-1 requests and Pass 2; its fake provokes attempt 2 | **fails**: attempt 2's request changes | rewrite the constant to the post-fix value, stating why; `ELICITED_TRACE_SHA256` (Pass 2's priming) should not move |
| `test_feedback_block.py::test_it_demands_a_full_replacement_not_a_patch` | `"re-emit EVERY field" in block` | passes if the phrase is kept | none, or rewrite if the header is reworded |
| `test_feedback_block.py::test_it_names_no_field_of_its_own` | no literal field name in `_requirement`'s or `build_feedback_block`'s source | passes if the names come from the codebook or result | none (a constraint) |
| `test_prompt_hash_coverage.py::test_the_feedback_sentinel_carries_every_line_template` | a fixed list of the block's line templates is in the sentinel render | passes, but the new header and trailing instruction are not in its list | extend the list (coverage, R-1 and R-2's pieces) |
| `test_prompt_hash_coverage.py::test_a_feedback_block_edit_moves_the_elicitation_pass1_hash`, `test_each_violation_requirement_text_is_hashed`, `test_each_class_accompaniment_is_hashed`, `test_the_echo_truncation_marker_is_hashed`, `test_the_feedback_sentinel_carries_every_fatal_code`, `…every_class_accompaniment` | mutation and coverage over the sentinel render | pass | none |
| `test_prompt_hash_coverage.py::test_elicitation_pass1_hashes_both_attempts` | four roles; attempt 2's user turn starts with attempt 1's | passes (R-2 appends) | none |
| `test_duplicate_on_pass1_attempt_2.py` (line 109), `test_elicitation_pipeline.py` (line 509) | detect attempt 2 by `"did not meet the contract"` in the last message | pass if the heading is kept | none (a constraint) |
| `test_pass1_contract_text.py::test_the_stated_and_judgment_contract_sentences_are_in_the_pass1_prompt` | counts in the attempt-1 sentinel prompt; `T_B in sentinel_feedback_block`; hash `!=` a pre-U04 value | passes | none |
| `test_pass1_contract_text.py::test_a_judgment_field_needs_a_step_that_cites_a_unit` | the JUDGMENT requirement in a built block | passes | none |
| `test_request_capture.py::test_elicitation_pass1` | attempt 1's key set and typed values (fake returns "{}" — one call) | passes | none |
| `test_run_manifest.py::test_a_changed_prompt_hash_is_a_pin_mismatch`, `test_pin_records.py`, `test_elicitation_sentinel.py`, `test_units_as_source_slices.py`, `test_extraction_signatures.py`, `test_thinking_channel.py` | stage sets, signatures, mutation refusals | pass | none |

**Phase 2's rewrite list:** `test_clean_fields_are_not_mentioned` and
`test_the_elicited_requests_are_byte_identical`; plus the coverage extension of
`test_the_feedback_sentinel_carries_every_line_template`. The list is from reading, not from running
a provisional fix (Phase 1 makes no template edit).
