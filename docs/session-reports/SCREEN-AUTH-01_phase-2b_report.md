# SCREEN-AUTH-01 Phase 2b — eligibility model and byte-identical relocation

**Task:** move screening's topic content out of seven literal sites into a spec eligibility
model with one renderer, and prove by hash that no rendered prompt or rubric changed.
**Commit:** `521b92a`, on `main`, pushed. **Parent:** `fefd112`.
**Session:** `410e9ecc-67a0-44c6-9523-ffad8f528ca3`, 2026-09-12.
**Gate at close:** 2,180 passed / 17 deselected, five chunks, zero failures.
**Content changed:** none. Every divergence is carried as a declared transitional field.

---

## 1. What moved

`spec.eligibility` replaces the former `screening_criteria` and top-level `specialty_scope`
sections. It holds:

| part | count | notes |
|---|---:|---|
| `criteria` | 14 | 6 inclusion, 8 exclusion; each with a stable id, `kind`, canonical `text`, the `stages` it renders at, and (exclusions only) a `reason_code` |
| `specialty_scope` | 1 | gains `id: specialty-scope` and `reason_code: wrong_specialty` so a verifier test can cite it |
| `verifier_tests` | 6 | each must declare `derives_from` — criterion ids or the scope id; empty is rejected |
| `stage_policies` | 5 | per-stage uncertainty policy and transitional prose |

Six stages: `abstract_primary`, `abstract_verifier`, `ft_primary`, `ft_verifier`,
`abstract_adjudication`, `ft_adjudication`.

`engine/core/eligibility_render.py` is the single renderer. All four prompt builders and both
adjudication rubric builders call it; no topic literal remains in them.

## 2. The identity gate

Ten rendered strings, each byte-identical to what the literals produced at `fefd112`.
P1–P4 and H1/H3 were measured in Phase 2a; H4–H7 in Part A step A0, before any edit.

| # | surface | bytes | verdict |
|---|---|---:|---|
| P1 | abstract primary prompt | 2,963 | MATCH |
| P2 | abstract verifier prompt | 4,041 | MATCH |
| P3 | FT primary prompt | 4,323 | MATCH |
| P4 | FT verifier prompt | 3,587 | MATCH |
| H1 | abstract adjudication rubric | 1,224 | MATCH |
| H3 | FT adjudication rubric | 574 | MATCH |
| H4 | abstract reference content | 3,262 | MATCH |
| H5 | FT reference content | 3,794 | MATCH |
| H6 | abstract edge-case guidance | 605 | MATCH |
| H7 | FT edge-case guidance | 634 | MATCH |

Pinned by `tests/test_eligibility.py`. **These are expected to go red at the fold step (2c/2f)**,
when the transitional fields are deleted and the paraphrases collapse to canonical text.
Updating them is how that content change gets approved, not a chore on the way past.

## 3. What the transitional fields carry

`transitional_text` on a criterion or a verifier test holds the wording a *stage* renders today
where it differs from canonical. Five carriers exist:

- **`exc-non-surgical-robotics`** at `abstract_primary` — the hardcoded primary list drops
  `warehouse` and `assistive devices` from EXC-4.
- **`exc-insufficient-evidence`** at `abstract_primary` — drops EXC-8's
  `or insufficient information to determine eligibility` *and* its
  `do not default to inclusion when evidence is absent` clause.
- **`vt-executes-action`** and **`vt-physical-task`** at `ft_verifier` — the FT prompt says
  "the full text" where the abstract prompt says "the abstract", and wraps its lines.
  `vt-autonomous-component` needs no override: it is the one test byte-identical at both stages.
- **`reason_code_prompt_text` / `reason_code_sheet_text`** on the three topic codes, and
  `STRUCTURAL_REASON_CODES` in the renderer for the other four — because all seven codes are
  described one way to the model and another to the human.
- **`rubric_text`** on the two adjudication stages — a third paraphrase of the criteria.

## 4. What was deleted

- **The four `hasattr(spec, …)` guards** in `_build_reference_content`,
  `_build_ft_reference_content`, `_build_edge_case_guidance`, `_build_ft_edge_case_guidance`.
  They could not fire on a validly-loaded spec (`extra='forbid'`, required field). Their only
  live effect was to convert this field move into **silent content loss** — measured before
  removal: H4 fell from **3,262 B to 722 B**, rendering `SCREENING ELIGIBILITY CRITERIA`
  followed immediately by `PICO FRAMEWORK:` with every criterion gone, and no error anywhere.
- **`_REASON_CODE_DESCRIPTIONS`** in `ft_screening_adjudicator.py` — a duplicate of the table
  the renderer now owns.
- **The spec-less export fallback** in `_write_xlsx` — a fifth divergent copy of the criteria,
  unreachable whenever a spec was supplied. `export_adjudication_queue` now raises without one.
- **`ScreeningCriteria`** — replaced, not orphaned.

## 5. `screening_hash`

It hashes its subject, so it moved with it: `eligibility.model_dump()` rather than the retired
`screening_criteria` section. Transitional fields are inside the hash deliberately — until they
are folded away they are part of what a stage actually renders, and a provenance record that
ignored them would call two different renderings the same protocol.

```
0d97b9d61161eeca6c81dd82f895bfb8c6f933b8e8ea23f79056a69f0cf98b90   before
4a30960fe685b251f6fe1067bcdb3c0e70c6f35de767adf3b1a733a1fdd4653f   after
```

The column name in `review_runs` is unchanged in 2b; it is renamed in 2d.

## 6. Findings this task surfaced

**An eighth topic-bearing literal, outside the inventory and outside the gate.**
`screener.py`'s **system message** reads *"You are a systematic review screening agent. Evaluate
whether the paper involves autonomous or semi-autonomous surgical robotics."* That is review
topic content, and the ten hashes cannot see it: they cover `_build_prompt`'s output, which is
the *user* message. The two FT system messages are structural by comparison. **Not moved** —
relocating it cannot be proved identical by any existing gate. It needs its own hash and a
ruling.

**`_build_decision_criteria` had a fifth entry that Phase 2a missed.** The abstract rubric ends
with `EDGE CASE: {specialty_scope.notes}`; the FT rubric does not, though both render the notes
again in their edge-case guidance. The abstract sheet therefore states the notes twice. Recorded
in the renderer as `_EDGE_CASE_RUBRIC_STAGES` so the divergence is visible rather than inherited.
It was caught only because H1 came back 491 bytes short — the identity gate doing its job.

**The export path had no spec in twelve tests.** Making the rubric spec-mandatory broke
`test_adjudication.py`, `test_ft_screening.py` and `test_workflow.py`, which exported without
one. They now supply the live spec. That those tests passed for so long is the same finding as
the fallback itself: the spec-less path was exercised only by tests.

**Pre-migration spec** preserved at
`data/backups/surgical_autonomy_pre_SCREEN-AUTH-01-2b_20260912T212019Z.yaml`.

## 7. Out of scope, deliberately

Content is unchanged: the fold to canonical text, reason-code unification, and the tie-break
reconciliation between the primary pass's recall-first instruction and EXC-8's explicit
anti-default clause are 2c/2f. Provenance columns are 2d. The categorizer is 2e. The
`FT_FLAGGED` / `FT_EXCLUDE` / `FT_SCREENED_OUT` naming is read out in 2c. The eleven remaining
`hasattr`/`getattr` guards on spec-typed objects are queued as SPEC-GUARD-01; an attribute-read
category for the inventory tool is queued as INVENTORY-02.

---

# Addendum — 2026-09-12: full-request identity gate, the system message, two guards

**Commit:** `e9c4aa0`, on `main`, pushed. **Parent:** `95ddc3f`.
**Gate at close:** 2,190 passed / 17 deselected, five chunks.
**Append-only:** nothing above this line was rewritten.

## Why

Section 6 of the report above recorded an eighth topic-bearing literal that the ten hashes
could not see. This addendum closes that hole. The finding generalises, and it is the reason
the addendum exists rather than a follow-up ticket: **the ten surface hashes proved something
narrower than they appeared to prove.** They covered what `_build_prompt` and the rubric
builders return — the *user* message. The screening call sites also send a *system* message,
assembled inline at the call site, and the abstract screener's carried a sentence of review
topic content. A gate that hashes half a request certifies half a relocation.

## What the four stages actually send

| stage | system message | topic content? |
|---|---|---|
| `abstract_primary` | `You are a systematic review screening agent. Evaluate whether the paper involves autonomous or semi-autonomous surgical robotics. Follow the criteria and instructions in the user message. Respond ONLY with the requested JSON.` | **yes** |
| `abstract_verifier` | identical to `abstract_primary` — one literal served both roles | **yes** |
| `ft_primary` | `You are a systematic review full-text screening agent. Evaluate eligibility based on the full paper text. Respond ONLY with the requested JSON.` | no |
| `ft_verifier` | `You are a systematic review full-text verification agent. Your job is to catch false positives. Be strict. Respond ONLY with the requested JSON.` | no |

Every stage sends exactly `[system, user]`.

## The split, and where each half lives

`StagePolicy.system_text` carries **only the topic sentence**, and only for the two abstract
stages. The rest of each system message is structural — it describes the agent's role and its
output contract, which are properties of the pipeline, not of any review — and lives in
`eligibility_render.SYSTEM_TEMPLATES`, one per stage, with a `{topic}` slot where review
content belongs.

Both halves could have rendered byte-identically from either home. The split was chosen so
that what is in the spec is exactly what is about *this review*, and what is in code is what
would be true of any review. A stage whose template has a slot and no `system_text` **raises**;
so does a stage that declares `system_text` for a template with no slot. Rendering an empty
slot would drop the review's subject from the request silently, which is the failure mode this
lane exists to prevent.

## The gate is now fourteen

Four full-request hashes join the ten surfaces. All were measured at `95ddc3f` before any edit
and matched byte for byte afterwards.

| # | request | bytes | verdict |
|---|---|---:|---|
| R1 | abstract primary | 3,315 | MATCH |
| R2 | abstract verifier | 4,404 | MATCH |
| R3 | FT primary | 4,612 | MATCH |
| R4 | FT verifier | 3,868 | MATCH |

Hashed as key-sorted JSON of the message list, UTF-8. Captured by replacing `ollama_chat` with
a recorder that raises once it has the arguments, so **the test never sends a request**. The
ten surface hashes (P1–P4, H1, H3–H7) are unchanged and still pinned.

## Two more guards gone

`hasattr(spec, "pico")` removed from `_build_reference_content` and
`_build_ft_reference_content` — the last spec guards in those two files, and the same defect as
the four removed in `521b92a`. H4 and H5 still match. Nine guards remain elsewhere under
SPEC-GUARD-01; none were touched.

## `screening_hash`

Moves again, because the topic sentence is now part of the object it hashes:

```
4a30960fe685b251f6fe1067bcdb3c0e70c6f35de767adf3b1a733a1fdd4653f   after 521b92a
d804ced7bd4c45e872524ebfd4d55f3d093ff9d18db28cfee8e2f67f4c571ae9   after e9c4aa0
```

## Ops finding — qwen3:8b resident forever

Not a defect, and nothing loaded it from this lane. **The Ollama service's own default is
infinite keep-alive:** `OLLAMA_KEEP_ALIVE:2562047h47m16.854775807s` in its systemd environment
— `MaxInt64` nanoseconds. With `OLLAMA_MAX_LOADED_MODELS:1`, every load evicts the previous one
and **the last model loaded stays resident indefinitely**.

What loaded it: the **07:00 UTC `ollama_health_check.sh` cron**, which runs
`ollama run "$model" "Reply OK"` over every model `ollama list` returns. The journal shows that
sequence on 2026-09-12 from `07:00:02`; whichever model it exercised last is the one left in
VRAM. No process asked for `keep_alive: forever` — the service grants it by default.

Ollama state was not changed: `ollama ps` is identical before and after this task.

**Incidental, and checked because it looked worse than it was:** `/api/show` calls appear in the
journal during this session's gate runs. They are **not** from the suite — a control run of one
chunk produced a delta of **zero**. A VS Code server with an Ollama-aware extension is running
on this host and is the likely source. The test fence holds.
