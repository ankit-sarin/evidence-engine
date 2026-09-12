# SCREEN-AUTH-01 phase-1 — recovered read-out

**Provenance header.** This file is a verbatim recovery of one Claude Code
assistant message. Nothing below the header has been edited, summarised or
reformatted.

- **Source transcript:** `/home/ankitsarin/claude-session-archive/4500483d-106b-4e9c-989a-a3314c775cb2.jsonl`
- **Session id:** `4500483d-106b-4e9c-989a-a3314c775cb2`
  - Note: the unified plan v46 attributes this session's work to 2885f251; the work was done in 4500483d, its continuation.
- **Source message line:** L2966 (NDJSON record, `type: assistant`)
- **Brief at transcript line:** L2852, 2026-09-11T03:15:36.813Z
- **Message timestamp:** 2026-09-11T03:18:41.223Z
- **HEAD stated in read-out:** **HEAD `8d43b99`**, tree clean.
- **Architect acknowledgement:** L2970, 2026-09-11T03:25:16.661Z — "RULINGS on SCREEN-AUTH-01 Phase 1 read-out — acknowledged at HEAD 8d43b99."
  *(user-authored; architect turn, not CC output)*
- **Trust:** Transcript-derived. Values in this report that trace to a tool call in the source transcript at the stated HEAD may be cited as MEASURED. Classifications, counts arrived at by reading, and judgements are INFERRED until re-measured.
- **Recovered by:** READOUT-ARCHIVE-01, 2026-09-11, recovering session `410e9ecc-67a0-44c6-9523-ffad8f528ca3`.
  Compiled from transcript `/home/ankitsarin/claude-session-archive/4500483d-106b-4e9c-989a-a3314c775cb2.jsonl` by session 410e9ecc… (READOUT-ARCHIVE-01) on 2026-09-11.
  *(files written 2026-09-12 00:49 UTC; the session opened 2026-09-11 23:29 UTC and crossed midnight)*

---

Read-only phase complete. No writes, no commit, no model call.

---

# SCREEN-AUTH-01 PHASE 1 — READ-OUT

**HEAD `8d43b99`**, tree clean. `review.db` `101,978,112 B @ 2026-09-11 02:00:52.636956943 UTC` — baseline R1, unchanged. `ollama ps` identical before and after (`deepseek-r1:32b`, 64 GB, resident from a prior session — nothing loaded by me). **G2 met.**

## Assumption verdicts

| | verdict |
|---|---|
| **I1** no stored screening decision is compared against `screening_hash` | **TRUE.** `screening_hash` has exactly one producer (`review_spec.py:395`), one writer (`run_pipeline.py:354` into `review_runs`), and **zero readers** outside tests. No screening table carries a hash or version column, and no staleness reader exists for screening at all |
| **I2** the categorizer config is under `data/` and carries topic vocabulary | **TRUE** — `data/surgical_autonomy/adjudication_categories.yaml`, 3,927 B. But see S4: it uses `regex_patterns`, while the in-code template emits `title_keywords`/`abstract_keywords` — **two different schemas** |
| **I3** a labeled screening sample exists | **TRUE, and better than expected** — `specialty_rescreen_flagged_86.xlsx` carries a **`PI_decision`** column, 86 rows, 76 exclude / 10 include. See S6 for the caveats on the other two candidates |
| **I4** prompts mix spec fields and code literals with no single template | **TRUE** — four f-string templates, no shared builder |

## S1 — What the spec already carries

| model | fields | live values |
|---|---|---|
| `PICO` (`review_spec.py`) | `population: str`, `intervention: str`, `comparator: str`, `outcomes: list[str]` — all required | 4 populated; 5 outcomes |
| `ScreeningCriteria` | `inclusion: list[str]`, `exclusion: list[str]` — both required | **6 inclusion, 8 exclusion** |
| `SpecialtyScope` | `included: list[str]` (min 1), `excluded: list[str]` (min 1), `notes: Optional[str]`; carries `format_for_prompt()` | 7 included, 6 excluded, a 3-sentence `notes` |

Verbatim values are long; the load-bearing ones for S7 are that **exclusion already holds the full 8-item list** that the primary screener replaces with 4 hardcoded items, and that `SpecialtyScope` is the **only** screening model with a formatter on it.

## S2 — Prompt assembly, four templates, no shared builder

### Abstract primary — `screener.py:_build_prompt(role="primary")`

| # | part | source |
|---|---|---|
| 1 | `/no_think` + framing | literal `screener.py:112-113` |
| 2 | PICO block | **spec** `pico.*` (`:49-55`) |
| 3 | INCLUSION CRITERIA | **spec** `screening_criteria.inclusion` (`:57`) |
| 4 | EXCLUSION CRITERIA | 🔴 **four hardcoded literals** `screener.py:64-69` — **topic-bearing** |
| 5 | SPECIALTY SCOPE | **spec** via `format_for_prompt()` (`:71-74`) |
| 6 | PAPER / no-abstract fallback | literal `:80-85`, restating an exclusion criterion in prose |
| 7 | decision instruction | 🔴 literal `:104-110` — **topic-bearing** ("autonomous surgical robotics", "surgical robotics at all") |

### Abstract verifier — same function, `role="verifier"`

Parts 2, 3, 5 identical. Part 4 becomes **spec** `screening_criteria.exclusion` (`:61`) — the verifier-only branch. Part 7 becomes 🔴 **the four numbered tests**, `screener.py:88-101`, all topic-bearing (autonomous execution, teleoperated/master-slave, physical surgical task, original research).

### FT primary — `ft_screener.py:build_ft_screening_prompt`

Parts 2/3/4/5 from **spec** (exclusion full, not simplified). Adds 🔴 **the inline REASON CODES block with descriptions**, `ft_screener.py:137-143` — topic-bearing (`wrong_specialty`, `no_autonomy_content`, "industrial, rehabilitation").

### FT verifier — `ft_screener.py:build_ft_verification_prompt`

PICO + exclusion + specialty from **spec**; then 🔴 **five numbered tests**, `ft_screener.py:180-188` — the abstract verifier's four plus a specialty test that **re-states the scope in prose** ("not dental, ophthalmic, etc.") alongside the spec-derived `specialty_block` two lines above it.

### Adjudication HTML — three more literal sets

🔴 `screening_adjudicator.py:246-253` (INCLUDE/EXCLUDE criteria prose), `screening_adjudicator.py:488-492` (a second, shorter copy naming the categorizer's own vocabulary: "perception-only, planning-only, teleoperation-only, reviews/editorials, hardware/sensors, rehabilitation, non-medical"), and `ft_screening_adjudicator.py:154-159`.

**Tally: 7 topic-bearing literal sites across 4 prompts and 3 HTML builders.**

## S3 — Reason codes: **four copies, not three**

| # | location | form |
|---|---|---|
| 1 | `constants.py:10-18` `FT_REASON_CODES` | names only, the tuple |
| 2 | `ft_screener.py:34` `Field(description=...)` | names in one prose string |
| 3 | 🔴 **`ft_screener.py:137-143`** | **names + descriptions, inline in the prompt** — the brief listed three copies; this is a fourth |
| 4 | `ft_screening_adjudicator.py:22-30` `_REASON_CODE_DESCRIPTIONS` | names + descriptions, for the reference sheet |

Copies 3 and 4 give **different descriptions for the same code** — e.g. `insufficient_data` is *"Commentary, letter, or editorial with no extractable data"* in the prompt and *"Insufficient methodological detail to assess eligibility"* in the adjudication sheet. The model is told one thing and the human is told another.

**Structural vs topic:** `eligible`, `protocol_only`, `duplicate_cohort`, `insufficient_data` are structural — they apply to any review. `wrong_specialty`, `no_autonomy_content`, `wrong_intervention` are topic-bearing; only the first is derivable from `SpecialtyScope` today.

**Emitted** into `ft_screening_decisions.reason_code` (TEXT, unconstrained — no CHECK). **Consumed** by `ft_screening_adjudicator` for the reference sheet and by `ft_screening_adjudication.reason_code`. Exports depend on the literal strings only through those two.

## S4 — The categorizer, and a schema divergence

`categorizer.py:45` reads `data/<review>/adjudication_categories.yaml` (`config_path_for_review`, `:77-80`). Live file: 3,927 B, **7 categories**, header comment *"derived from diagnostic sampling of 416 flagged papers"*.

```
cv_perception              10 regex_patterns
review_editorial            9
hardware_sensing            7
planning_only               4
teleoperation_only          2
rehabilitation_prosthetics  4
industrial_nonmedical       4
```

Topic-bearing examples: `'\b(surgical\s+phase\s+recognition|tool\s+detection|tool\s+segmentation)\b'`, `'\b(teleoperation|da\s*vinci)\b'`.

🔴 **The in-code template at `categorizer.py:178-226` emits a different schema** — `title_keywords` / `abstract_keywords` / `exclude_if_also` — while the live file uses `regex_patterns`. So the vocabulary a **new review** would be scaffolded with is a keyword list hardcoded in engine code, and it is *not the shape the one real review uses*. (My first pass counted zero keywords in the live file and was wrong about why — I was reading the template's keys. Correcting that here.)

Consumers: `screening_adjudicator.py:312,333` and `abstract_adjudication_html.py:158-160`. Categorisation verified working: a CV title → `cv_perception`, a review → `review_editorial`, an autonomy paper → `ambiguous`.

## S5 — Screening provenance: none

| table | rows | hash/version column |
|---|---:|---|
| `abstract_screening_decisions` | **21,374** | none |
| `abstract_verification_decisions` | **1,422** | none |
| `abstract_screening_adjudication` | **0** | none |
| `ft_screening_decisions` | **366** | none |
| `ft_verification_decisions` | **182** | none |
| `ft_screening_adjudication` | **36** | none |

Columns are `paper_id, pass_number, decision, rationale, model, decided_at` (+`reason_code`, `confidence` for FT). **No screening table records which spec produced the decision**, and no reader compares stored screening state to the current spec — confirming I1. A spec restructure therefore **cannot invalidate stored screening provenance, because there is none to invalidate.** That is freedom for the restructure and a gap in its own right: 21,374 abstract decisions exist with no record of the criteria that produced them.

Read-only connection used throughout; `review.db` mtime unchanged.

## S6 — Smoke assets

**`rescreen_original_251.py` no longer screens 251 papers.** It does `SELECT id, title, abstract, status FROM papers ORDER BY id` (`:59-61`) — **all 10,039 today.** The name is stale by a factor of forty. It runs dual-pass `screen_paper` (primary twice, `pass_number=1` and `2` — *not* primary+verifier), derives `old_decision` from `papers.status`, and writes a CSV plus a resume checkpoint. It is read-only against the DB. **It imports cleanly** after SPEC-AUTH-01/CODEBOOK-AUTH-01 (import-level check only; no model call).

Three candidate label sets:

| asset | rows | labels | usable? |
|---|---:|---|---|
| `expanded_search/rescreen_original_251.csv` | 251 | `old_decision` 155/96, `new_decision` 173/77/1 | **AI vs AI** — a regression baseline, not ground truth |
| `adjudication/screening_queue_20260310.xlsx` | 416 | `DECISION (INCLUDE/EXCLUDE)` all 416 filled — **all "exclude"** | degenerate: no positives, so it can only measure false positives |
| **`adjudication/specialty_rescreen_flagged_86.xlsx`** | **86** | **`PI_decision`: 76 exclude / 10 include** | ✅ **genuine human labels, both classes** — the best before/after smoke set |

Plus 36 human FT rows in `ft_screening_adjudication`. `abstract_screening_adjudication` is **empty**, so no human abstract labels live in the database.

## S7 — What an eligibility object would have to carry

The spec already holds everything the prompts take from it — PICO, the full inclusion and exclusion lists, and the specialty scope — so the gap is not missing *data* but missing *slots* for the seven topic-bearing literal sites. An eligibility object would need: a **primary-pass exclusion subset** (today's four hardcoded items are a hand-picked simplification of the spec's eight, and the relationship between the two lists is recorded nowhere); a **verifier test list** (the four abstract tests and five FT tests, which are the same tests plus a specialty one, expressed as ordered assertions rather than prose); a **reason-code vocabulary with one description per code**, replacing four divergent copies and giving `ft_screening_decisions.reason_code` something to be checked against; an **absent-evidence policy**, currently a prose fallback at `screener.py:80-85` restating an exclusion criterion; and the **adjudication criteria text**, which is three more copies of the same include/exclude statement. Against that, several S2 items are structural engine text and should stay in code: the `/no_think` directive, the JSON response contract, the "you are the VERIFICATION pass, catch false positives" role framing, the tier/section headings, and the recall-vs-precision instruction shape — these describe *how the engine talks to a model*, not *what this review is about*. The sharpest design question the read-out raises is the categorizer: its live vocabulary is regex-based in a per-review file while its scaffolding template is keyword-based in engine code, so "make the categorizer spec-driven" first has to decide which of those two schemas is the real one.

---

Stopping here. Design not drafted. Awaiting the ruling.
