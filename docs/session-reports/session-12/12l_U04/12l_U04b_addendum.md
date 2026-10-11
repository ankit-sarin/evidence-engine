# 12l-U04b — addendum to 12l_U04.md: commit 2 (what the pin records) and commit 4 (the JUDGMENT step-citation check)

U04 resumed under R616 after its I2 stop. Written from the tree at `f3c3535` (item 0, R616). No model call, no live write. `12l_U04.md` is not edited; this file completes it. With these two commits U04 is whole: commits 1 (`36de472`), 2, 3 (`a0b818e`) and 4.

## Commit 2 — what the pin records

- **INT-i12-4.** `run_manifest.pin_tuple` adds a `segmentation` key — `{pysbd, min_unit_tokens, units_version}` — to the pin of an arm whose stages include `elicitation_pass1`. `pysbd` joins `run_manifest.LIBRARIES`, so every manifest's `library_versions` records it. The constants are read from `engine/elicitation/units.py` at pin time; that file is not edited. A non-elicited arm's pin has no `segmentation` key.
- **C34.** `canonical_absence_sentinel` joins `codebook.SEMANTIC_KEYS`.
- **INT-l12-1.** `extractor.extraction_stages` gives an elicited run `("elicitation_pass1", "extract_pass2")`. `scripts/run_pipeline.py::_open_run_manifest` takes its extract stages from that function, in place of its own rename, so there is one declaration. `effective_config` already read `extraction_stages(spec)[0]`.

Tests, `tests/test_pin_records.py`, on the code before the commit (`tests_before_commit_2.txt`): **3 failed**. After: 3 passed.

| test | before |
| --- | --- |
| `test_a_segmentation_change_moves_the_elicited_arm_pin` | failed: `KeyError: 'segmentation'` |
| `test_semantic_hash_moves_on_a_canonical_sentinel_edit` | failed: both hashes `1e67684b…9172` |
| `test_an_elicited_run_declares_and_pins_two_stages_and_refuses_the_snippet_retry` | failed: the stage set held `extract_retry_snippet` |

The first opens and pins an elicited run, then changes each of the three values in turn: `pin_tuple` differs and a second `open_run` raises `ArmPinMismatch` naming `segmentation.pysbd`, `segmentation.min_unit_tokens` or `segmentation.units_version`, and no `prompt_hash`. The third opens the manifest through `run_pipeline._open_run_manifest`, reads the declared stages and the pinned tuple back from the database, and makes a call on `extract_retry_snippet` under the run: `UndeclaredCall`, nothing sent, no `run_calls` row.

## I1, I2

- **I1 holds: `VALUE_WITHOUT_CITATION`.** R616(c) states the rule as the citation requirement, and this is the code for an unmet citation requirement. Its JUDGMENT attempt-2 feedback, rendered (`prompts._requirement`), already carries T-b:

  > a value was asserted with nothing cited behind it. The contract is: reason in steps where EVERY step either cites at least one unit or sets criteria_application: true, then the value. At least one step must cite a unit. If nothing in this paper can be cited for it, the value must be NO_EVIDENCE_LOCATABLE with an EMPTY citation list.

  No feedback text is added; `prompts.py` is not touched.
- **I2 holds.** The Pass-1 prompt shows a JUDGMENT field in two places. The JUDGMENT contract's example has a step that cites: `{"field_name": "...", "reasoning_steps": [{"step": "...", "unit_indices": [7]}, {"step": "...", "criteria_application": true}], "value": "..."}`. The escape line on every JUDGMENT field block is the escape token with an empty list, which the rule does not reach. The worked example is on `funding_source`, "a field that is not in this schema", with no class and no steps (F1).

## Commit 4 — the check follows R571(b)

`contracts.check_entry`: for a non-escape value, `VALUE_WITHOUT_CITATION` is raised when nothing is cited, **or** when the field is JUDGMENT and no reasoning step cites a unit. The record's `indices` (field-level then steps') is unchanged. A JUDGMENT field with a field-level citation and no steps is now both `STEPS_MISSING` and `VALUE_WITHOUT_CITATION`.

Test, `tests/test_pass1_contract_text.py::test_a_judgment_field_needs_a_step_that_cites_a_unit`, on the code before the commit (`tests_before_commit_4.txt`): **failed** — a field-level citation with one criteria-application step read `ok`. After: passed. It also asserts that the same field with one cited step passes, that the escape and the other classes are untouched, and that the failing field's feedback block carries the code and T-b.

## Gate

Run once, after commit 4: **3,274 passed / 1 xfailed / 17 deselected, 3,275 collected, 822.82 s** (23:37:23 → 23:51:08 UTC; band 763–843 s). Against the 12l-U04 id list: 5 added, 0 removed — this addendum's four tests, and `test_api_parity.py::test_engine_import_resolves[run_pipeline.py::engine.agents.extractor.extraction_stages]`, a case the parity test generates for `run_pipeline.py`'s new import. `inventory --check` in sync (regenerated in commit 2: line numbers moved); Ollama `NRestarts=0`.

One existing test changed its pinned expectation: `tests/test_run_manifest.py::test_a_run_writes_its_manifest_and_one_row_per_stage` asserted the manifest's library set as `{ollama, openai, anthropic}`; it is now those three and `pysbd` (INT-i12-4). No recorded request digest moved, and no existing stage-list or semantic-hash expectation needed a change: the existing tests name the non-elicited three-stage set, pass their stages explicitly, or compare against the loader's own hash.

## Hashes and pins

`pin_tuples.py` recomputes both pins as a run pins them (stages from `extraction_stages`, the two digests recorded at 12f). `pin_tuples_before.json` is the tree at `f3c3535`; its two pins equal MEASURED. `pin_tuples_after_commit_2.json` and `pin_tuples_after_commit_4.json` are byte-identical.

| key | before (MEASURED) | after commit 2 | after commit 4 |
| --- | --- | --- | --- |
| codebook semantic hash | `1e67684b81c82e6002c45951f5f0ea4860f3a2a1774177f7324df0f54aa99172` | `f261af76feadd650f3c58b36498e2d140ef2dc4ffec6a80a16c06741eb2f5646` | same |
| **elicited arm pin** | `255462870d11ba4037d18f3d08a84ac7594b3ca7064c2df29c1793c3c0e73541` | `8f0aeecdeda659a13b3c182fabdc2568523f38891cd692f0a8cf9ce629de3488` | same |
| elicited `codebook_hash` | `1e67684b…9172` | `f261af76…5646` | same |
| elicited `segmentation.pysbd` | absent | `0.3.4` | same |
| elicited `segmentation.min_unit_tokens` | absent | `3` | same |
| elicited `segmentation.units_version` | absent | `units-2` | same |
| elicited `stages.extract_retry_snippet` (model, digest, options, format, prompt) | present | absent | same |
| elicited `stages.elicitation_pass1`, `stages.extract_pass2` (every key) | as recorded | same | same |
| **non-elicited arm pin** | `defd293bfe62e427b906e072dcf56d0ca6e321e7d1ca4ad1fde3758fb84f3e6e` | `d3672ab6fb1d9b627b9fa8dd19112e148e88cf174b1d854bb0b7fff004f7ff5c` | same |
| non-elicited `codebook_hash` | `1e67684b…9172` | `f261af76…5646` | same |
| non-elicited `stages.*` (three stages, every key) | as recorded | same | same |
| `arm_kind`, `provider`, `model`, `client_library` (both arms) | as recorded | same | same |
| prompt hashes: `elicitation_pass1` `18ac4255…2f1c`, elicited `extract_pass2` `b2a44f28…10fe`, non-elicited `extract_pass1` `c0083f30…4f34`, `extract_pass2` `a4f3cb35…8e78`, `extract_retry_snippet` `05a98181…55aa`, `audit` `d972c46d…838d`, preflight `de95332f…322a` | as recorded | same | same |
| manifest `library_versions` | ollama 0.6.1, openai 2.24.0, anthropic 0.84.0 | plus pysbd 0.3.4 | same |

As expected in the brief: the elicited pin moved through `codebook_hash`, the segmenter keys and the stage set; the non-elicited pin through `codebook_hash`; the semantic hash moved; no prompt hash moved. Commit 4 moves nothing a pin records.

## R509 findings (none Class 1)

- **F1 — the worked example shows only the field-level citation shape.** Its cases 2 and 3 cite at field level with no steps, on a field with no class. A JUDGMENT field answered in that shape failed before this unit (`STEPS_MISSING`) and fails now. The prompt shows no JUDGMENT field reporting an absence, where the citing step would be the one that cites the absence sentence. Any change is prompt text and the PI's wording. Proposed Class 3, P7, or a note on INT-h12-2 for the rehearsal to read against.
- **F2 — the feedback's opening clause is inexact for the new case.** "a value was asserted with nothing cited behind it" is sent to a field that cited at field level; the sentence that names the defect is T-b, two sentences on. No new text was written (I1). Proposed: with INT-l12-17, the same kind of gap; no new row.
- **F3 — the session-open pin script no longer computes the elicited pin a run would write.** `~/scratch/12i-scope/pins.py` hard-codes three extraction stages for both arms, so its elicited figure now includes a stage no elicited run declares. `12l_U04/pin_tuples.py` takes the stages from `extraction_stages` and prints the tuples. Proposed: the next expected-values record uses it; no row.
- **F4 — `pysbd` is now in every manifest's `library_versions`, screening and audit runs included.** It is recorded, never compared, outside an elicited arm's pin. Proposed: no row.
