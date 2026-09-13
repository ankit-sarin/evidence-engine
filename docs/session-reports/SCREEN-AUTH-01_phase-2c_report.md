# SCREEN-AUTH-01 Phase 2c — reason-code authority and the fold

**Task:** replace every transitional carrier introduced in 2b with canonical content derived
from the eligibility model, establish one reason-code vocabulary, and render every screening
surface for PI review before committing.
**Commits:** `a01ab47` (the fold), `4d78a2c` (the renders), and this report. All on `main`,
pushed. **Parent:** `83defc5`.
**Session:** `410e9ecc-67a0-44c6-9523-ffad8f528ca3`, 2026-09-12 → 2026-09-13.
**Gate at close:** 2,221 passed / 17 deselected, five chunks, zero failures.
**Approval:** the PI approved the before/after renders (`screen-auth-2c-render/`) in session,
under rulings R27–R38.

---

## 1. What the fold removed, and what replaced it

| 2b carrier | replaced by |
|---|---|
| `Criterion.transitional_text` | canonical `text` only, rendered identically at every stage |
| `VerifierTest.transitional_text` | canonical text with an `{evidence}` placeholder the renderer fills with "abstract" or "full text" |
| `reason_code_prompt_text` / `reason_code_sheet_text` | one vocabulary (§2) |
| `StagePolicy.instruction_text`, `absent_abstract_text` | prose derived from `when_uncertain` / `when_evidence_absent`; the no-abstract fallback states the evidence-sufficiency criterion itself |
| `StagePolicy.rubric_text` | the adjudication rubric derived from the criteria (kind, text, examples) |
| `StagePolicy.system_text` | the abstract primary system message names the review by `spec.title` |

`extra='forbid'` is the proof: a spec carrying any of the ten retired keys fails to load and
names the key (`test_a_spec_carrying_a_retired_transitional_field_fails_and_names_it`, ten
cases). An AST scan of `engine/`, `scripts/` and `analysis/` for a field, definition,
attribute, keyword or string key with a carrier's name returns **zero**.

## 2. One reason-code vocabulary

- **Four structural codes, owned by the engine**, one description each, in
  `review_spec.STRUCTURAL_REASON_CODES`: `eligible`, `protocol_only`, `duplicate_cohort`,
  `insufficient_data`.
- **One code per exclusion criterion, required and unique**: `review_article`,
  `editorial_commentary`, `abstract_only`, `wrong_intervention`, `teleoperation_only`,
  `analysis_only`, `no_autonomy_content`, and `insufficient_data` on EXC-8 as a reference to
  the structural code, whose engine description is authoritative.
- **One on the specialty scope**: `wrong_specialty`.
- **Twelve in all**, exposed as `eligibility.reason_codes()` in prompt order.

Enforced where decisions are written: `add_ft_screening_decision` requires `reason_codes` and
refuses a code outside it before any insert. `constants.FT_REASON_CODES`, a seven-code copy with
no production consumer left, is deleted (R29).

**Full-text surfaces show each code beside the rule that declares it** — `[teleoperation_only]
Purely teleoperated…`, `SPECIALTY SCOPE [wrong_specialty]:` — and the reason-code menu lists only
the structural codes. Abstract surfaces show no codes, because abstract decisions record none.

## 3. Content the PI approved

- The abstract primary prompt loses its free-standing *"surgical robotics at all"* bar.
  Recall-first and absence-means-exclude are separate derived sentences, and no longer
  contradict the evidence-sufficiency criterion rendered in the same prompt.
- The primary pass sees canonical EXC-4 (`warehouse`, `assistive devices` restored) and canonical
  EXC-8. It still sees four of the eight exclusions; that subset is 2f's smoke arm.
- EXC-6 and EXC-7 carry examples:
  - EXC-6 — *"planning-only papers — surgical motion, path, or task planning computed from data
    without controlling a robot to execute it"*
  - EXC-7 — *"hardware, sensor, or instrument papers that describe a robot or device without
    autonomous control"*
- The abstract verifier's system message is a verification agent's, parallel to the FT verifier.
- Scope notes appear once on the abstract instructions sheet; the rubric's `EDGE CASE:` line is
  gone. They still render on the reference sheets through `format_for_prompt()`.
- The abstract adjudication guidance derives from `abstract_primary`'s policy and gains the
  absence sentence. FT adjudication inherits `ft_primary`'s policy, which is empty, so its
  guidance is byte-identical to before.

## 4. The fourteen frozen surfaces

Every surface is pinned in `tests/test_eligibility.py::test_the_screening_surfaces_are_frozen` at
the approved value, with a failure message reading *"frozen after SCREEN-AUTH-01 2c — any change
requires a measured smoke (2f) and an architect ruling."* The test comment records each surface's
2b pin and, for R1–R4, its value at `83defc5` under the new request definition.

**The request definition widened (R31).** R1–R4 now hash `{format, messages}`, not `messages`
alone. The structured-output schema lists reason codes to the model, and before this change no
hash covered it — a second gate hole of the same shape as the system message the 2b addendum
closed.

| # | surface | bytes | frozen |
|---|---|---:|---|
| R1 | abstract primary request | 4,046 | `e02ce2c9…84649` |
| R2 | abstract verifier request | 5,039 | `bc36e291…71ff3` |
| R3 | FT primary request | 5,844 | `c5cfdac0…11cf93` |
| R4 | FT verifier request | 4,757 | `bd7adb8f…92b4` |
| P1 | abstract primary prompt | 3,205 | `c2c60b16…342dd` |
| P2 | abstract verifier prompt | 4,281 | `a5f8b253…ac388` |
| P3 | FT primary prompt | 4,754 | `ed7dd674…c4c46` |
| P4 | FT verifier prompt | 3,989 | `e05cb95f…35dc2` |
| H1 | abstract rubric | 2,268 | `59b7bd5b…a2df0` |
| H3 | FT rubric | 2,510 | `fc06eb3a…99677` |
| H4 | abstract reference | 3,501 | `ec9f0a44…0e9f` |
| H5 | FT reference | 4,134 | `b45a69ad…16505` |
| H6 | abstract edge-case guidance | 733 | `4e9a62d1…ba380` |
| H7 | FT edge-case guidance | 634 | `404cbc8f…27a413` (unchanged) |

## 5. `screening_hash`

```
d804ced7bd4c45e872524ebfd4d55f3d093ff9d18db28cfee8e2f67f4c571ae9   after the 2b addendum
d563134ed6a92a8d0ee3e877574fcf844f7094889ec411f88e3285bb58a06a29   after 2c — frozen
```

## 6. Collisions the fold surfaced, and their rulings

| # | finding | ruling |
|---|---|---|
| 1 | The missing-parsed-text path wrote reason code `no_parsed_text`, outside the vocabulary — and wrote an `FT_EXCLUDE` decision row while setting status `FT_FLAGGED` | **R27** — no decision row; status only. Nothing was screened. |
| 2 | `scripts/ft_screening_smoke_test.py` called the writer without the vocabulary | **R28** — repointed |
| 3 | `constants.FT_REASON_CODES` had no production consumer and two test pins | **R29** — deleted |
| 4 | I3 false: `test_ft_screening.py` pinned the tuple and prompt content | **R30** — updated to the folded content |

**A0, recorded (R34), not changed:** `FT_EXCLUDE` (model decision value) → `FT_SCREENED_OUT`
(paper status) is a clean mapping. `FT_FLAGGED` is overloaded — verifier verdict, queue status,
and parking for papers no verifier judged. `FT_SCREENED_OUT` doubles as terminal status and human
verdict. Queued as FT-STATUS-01, after 2f.

## 7. Tests changed

**`test_eligibility.py`**, rewritten. 45 test functions, 69 cases.

Superseded and removed — each exercised a carrier or the pre-fold identity gate:
`test_transitional_text_for_an_unrendered_stage_is_rejected`,
`test_a_reason_code_description_without_a_reason_code_is_rejected`,
`test_transitional_text_is_honoured_at_the_stage_that_declares_it`,
`test_a_verifier_test_renders_its_stage_wording`,
`test_a_test_without_transitional_text_is_shared_verbatim`,
`test_reason_code_descriptions_differ_between_prompt_and_sheet`,
`test_topic_reason_codes_are_described_by_the_spec`,
`test_the_abstract_stages_carry_their_topic_sentence_in_the_spec`,
`test_the_ft_stages_declare_no_topic_sentence`,
`test_a_topic_slot_with_no_system_text_is_refused`,
`test_system_text_for_a_stage_with_no_topic_slot_is_refused`,
`test_rendering_is_byte_identical_to_the_pre_relocation_literals` and
`test_the_full_request_is_byte_identical_to_the_pre_relocation_literals` — the last two replaced by
`test_the_screening_surfaces_are_frozen`.

Added: T1 reason codes (required, unique, `eligible` refused, structural reference, twelve-code
vocabulary, structural-only menu); T2 examples; T3 policy per stage family, adjudication
derivation, notes-once, the fallback; T4 evidence substitution and unknown placeholders; T5
write-time refusal and acceptance; T6 the ten retired carriers; the system-message, format-schema
and fallback contracts; T7 the fourteen frozen surfaces.

**`test_ft_screening.py`.** `TestConstants::test_reason_codes_tuple` becomes
`TestReasonCodeVocabulary::test_reason_codes_come_from_the_eligibility_vocabulary`, plus
`test_the_engine_tuple_is_gone` (R29). The module import drops `FT_REASON_CODES`. The
`_advance_to_ft_flagged` fixture and `test_add_ft_screening_decision` pass the vocabulary.
`test_ft_screening_prompt_contains_reason_codes` asserts every code and the bracketed form (R30).
`TestMissingParsedText::test_no_parsed_text_marks_ft_flagged` asserts no decision row (R27).

**`test_review_spec.py`.** The `screening_hash` baseline is frozen at the 2c value.

## 8. Scans

**Topic literals** in the ten touched files, docstrings excluded. The remaining hits are
structural, or belong to 2e:
- "systematic review" in the engine's own role framing (prompt scaffolding and system templates);
- `review_editorial` and `rehabilitation_prosthetics` — categorizer category names (2e);
- the FT workflow note ("flagged because"), engine text by ruling;
- one smoke-test console label in `scripts/ft_screening_smoke_test.py`, never sent to a model.

**Carriers by AST:** zero. The renderer's no-abstract function was renamed from
`absent_abstract_text` to `absent_abstract_fallback` so that it no longer shares a retired field's
name, which would have left the carrier scan permanently ambiguous.

## 9. Worth knowing

- **The rewrite of `test_eligibility.py` first dropped seven tests that still applied** — stage,
  `derives_from` and duplicate-id validators, and the primary-subset and stage-absence checks.
  They were caught by diffing test names against HEAD before commit, and restored.
- **`review.db` was never opened.** It is unchanged at 101,978,112 B,
  `2026-09-11 02:00:52.636956943 UTC`. No model call was made.
- Stored decisions are not re-validated. All seven codes the 366 existing
  `ft_screening_decisions` rows use are inside the new vocabulary.
