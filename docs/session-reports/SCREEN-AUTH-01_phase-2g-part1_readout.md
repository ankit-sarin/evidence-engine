# SCREEN-AUTH-01 Phase 2g Part 1 — what moved 42 papers from primary IN to primary OUT (A→B)

**Date:** 2026-09-13. **Task type:** read-only analysis. No model call, no run, no surface
changed.
**Harness session:** `e6c94396-9455-4c17-be3d-1edf2c546b1b` — **a continuation.** The
transcript of `91f0009c-5166-4bd2-a397-d74ee5fa8faa` (which ran 2f) ends with a
`continued-in` record at 2026-09-13T04:36:51.717Z naming `e6c94396…`. Web session
`session_016ed9ad1TCpLqtDVpfu8yxt`. HEAD at start `34a348e`, clean, level with origin.

**Tags.** Counts computed from files or the database are **MEASURED**. What a rationale cites,
which prompt change a rationale echoes, and every cause are **INFERRED** — a reading, not a
measurement.

**Sources.** 2f outputs committed in `8a7dc4f` (`docs/session-reports/screen-auth-2f-smoke/`:
`full/arm_*/primary.jsonl`, `arm_*_papers.csv`); 2c renders committed in `4d78a2c`
(`docs/session-reports/screen-auth-2c-render/R1_abstract_primary_request.{before,after,diff}`);
`git show 83defc5:review_specs/surgical_autonomy.yaml`; HEAD `engine/core/eligibility_render.py`;
one read-only query of the review database (P5). Scratch scripts were not committed.

## Assumptions

| id | verdict | evidence |
|---|---|---|
| I1 | **FALSE, with the brief's own fallback** | Primary outputs carry no reason code. `ScreeningDecision` is `decision` / `rationale` / `confidence` at both 83defc5 and HEAD; a stored raw output is `{"decision": …, "rationale": …, "confidence": …}`. Abstract surfaces render no codes (2c). Criteria below are read from rationale text and tagged INFERRED. |
| I2 | **TRUE** | `R1_abstract_primary_request.before.txt` / `.after.txt` carry SYSTEM, USER and FORMAT. The before file contains exactly the system and user text of arm A's committed placeholder capture (`full/arm_A/identity.json`, hash `33bdeea9…`); the after file contains arm B's (`e02ce2c9…` = frozen R1). `engine/` and `review_specs/` are byte-identical from the fold `a01ab47` to HEAD. |
| I3 | **TRUE** | Read first by `PRAGMA table_info`: `papers` has `status`, `rejected_reason`, `updated_at`, `ee_identifier`; decision history is in `abstract_screening_decisions`, `abstract_verification_decisions`, `ft_screening_decisions` (with `reason_code`), `ft_verification_decisions`; `full_text_assets` and `extractions` carry per-paper rows. |
| I4 | **TRUE** | The 2b report names "the tie-break reconciliation between the primary pass's recall-first instruction and EXC-8's explicit anti-default clause". The recall-first text is in the 83defc5 request and absent from HEAD's (P3 C4, C5). |

## P1 — what B cites when it excludes

No reason code exists (I1), so this is a reading of the rationale text (INFERRED). A rationale
may cite several criteria; each cited criterion is counted.

**Criterion cited, per pass, split by PI decision (INFERRED reading; MEASURED tally)**

| criterion cited | 42 flips (A IN → B OUT): p1 PI-inc / p1 PI-exc / p2 PI-inc / p2 PI-exc / papers | all 47 B primary OUT: same | all 49 C primary OUT: same |
|---|---|---|---|
| autonomy absent or not stated | 6 / 27 / 6 / 29 / **35** | 6 / 32 / 6 / 34 / 40 | 9 / 37 / 9 / 37 / 46 |
| no physical surgical task / no robot control of a surgical action | 4 / 16 / 5 / 17 / **23** | 4 / 19 / 5 / 20 / 26 | 3 / 13 / 3 / 14 / 18 |
| excluded specialty | 1 / 18 / 1 / 18 / **19** | 1 / 19 / 1 / 19 / 20 | 1 / 9 / 1 / 9 / 11 |
| analysis / planning / simulation / data method only | 1 / 11 / 1 / 11 / **12** | 1 / 12 / 1 / 12 / 13 | 1 / 11 / 1 / 12 / 14 |
| insufficient information | 0 / 3 / 0 / 6 / **6** | 0 / 3 / 0 / 6 / 6 | 1 / 4 / 1 / 4 / 6 |
| not original research | — | — | 1 / 0 / 1 / 0 / 1 |

The 42 flips are 35 PI excludes and 7 PI includes (MEASURED). By workbook category: 22
scope/autonomy-level, 8 neurosurgery, 6 dental/oral-maxillofacial/ENT, 2 ophthalmology/retinal,
2 orthopedic/spine, 1 scope [primary-excluded], 1 vascular [primary-excluded].

**Specific wording, papers matching on either pass (MEASURED counts of text patterns)**

| wording in the rationale | A, 42 flips | B, 42 flips | B, of the 7 lost PI includes |
|---|---:|---:|---|
| "does not involve / describe / demonstrate … autonom…" (absent) | 0 | 21 | 4 — 583, 786, 170, 702 |
| "does not (clearly) mention / specify / indicate / state / clarify" (not stated) | 2 | 18 | 3 — 222, 786, 693 |
| "does not meet … inclusion criteri…" | 0 | 19 | 4 — 583, 569, 786, 170 |
| names an excluded specialty as the reason | 1 | 15 | 1 — 569 |
| specialty "… unless … generalizable" | 1 | 10 | 0 |
| teleoperat / shared control / robotic assist | 5 | 10 | 4 — 583, 569, 786, 170 |
| "sufficient detail/information", "determine eligibility", "too vague", "cannot be determined" | **0** | **6** | **0** |

**Whole-arm vocabulary (MEASURED; rationales containing the phrase, of 172 per arm)**

| phrase | A | B | C |
|---|---:|---:|---:|
| "autonomous or semi-autonomous surgical robotic(s)" (A's old system message) | 104 | 20 | 14 |
| "meets / meeting / aligns with … the (inclusion) criteria" | 125 | 39 | 38 |
| "does not meet … inclusion criteri…" | 0 | 41 | 51 |
| "explicitly excluded" | 3 | 27 | 16 |
| "not … physical surgical task" | 18 | 47 | 32 |
| insufficient-information phrasing | 0 | 8 | 11 |
| "determine eligibility" | 0 | 13 | 5 |
| "default to inclusion" | 0 | 0 | 0 |
| "evidence is absent" / "absence of evidence" | 0 | 0 | 0 |
| "partial evidence" | 0 | 0 | 1 |
| "clearly meets an exclusion" | 0 | 2 | 0 |

Method note: a first keyword pass that counted any mention of "specialty" labelled 71 of 84
flip rationales "specialty scope", because rationales list "the included specialties" in
passing. It was discarded; the tables above use a reason-shaped pattern and were checked by
reading all 42 flips' B rationales.

## P2 — the seven lost PI includes

The brief lists 702 and 693 separately; both are among the seven (A final IN, B primary OUT).
Where B's two passes are word-identical they are quoted once.

**583 EE-431** — *A supervision system for the intuitive usage of a telemanipulated surgical robotic setup*
- **B p1 = p2 (exclude):** "The paper describes a telemanipulated surgical robotic system with haptic feedback and supervision features, but it does not involve autonomous or semi-autonomous robotic components that control or direct a physical robot to execute a surgical action. The system is primarily teleoperated with supervision, not autonomous. It does not meet the inclusion criteria for involving autonomous or semi-autonomous robotic components."
- **A p1 (include):** "The paper describes a system that includes autonomous tasks such as cutting and needle-insertion performed by robotic actuators. It also mentions the use of a supervision system that integrates optical tracking and simulation for autonomous task execution, meeting the criteria for autonomous or semi-autonomous surgical robotics."

**222 EE-092** — *Adaptive Artificial Intelligence Based Model Base Controller: Applied to Surgical Endoscopy Telemanipulator*
- **B p1 = p2 (exclude):** "The paper describes a surgical robot assistant with an artificial intelligence-based controller for endoscopic tool localization, but it does not mention any level of autonomy (Levels 1–5) or any autonomous/semi-autonomous robotic component that controls or directs a physical robot to execute a surgical action. The focus appears to be on control algorithms rather than autonomy in surgical robotics. Additionally, the paper does not involve a physical surgical task on tissue, phantoms, or in-vivo models, which is a required inclusion criterion."
- **A p1 (include):** "The paper describes a surgical robot assistant with an articulated PUMA robot and an artificial intelligence-based controller for endoscopic tool localization, indicating the use of autonomous or semi-autonomous robotic components. The system involves physical robotic control for surgical tasks, meeting the inclusion criteria for autonomous or semi-autonomous surgical robotics."

**569 EE-417** — *Automation of retinal surgery: A shared control robotic system for laser ablation*
- **B p1 (exclude):** "The paper describes a shared control robotic system for retinal surgery, which involves a level of autonomy (shared control). However, the specialty of retinal surgery (ophthalmic surgery) is explicitly excluded according to the inclusion criteria. The paper does not meet the inclusion criteria for specialty scope as it falls under ophthalmic surgery, which is excluded."
- **B p2 (exclude):** "The paper describes a shared control robotic system for retinal surgery, which involves a level of autonomy. However, retinal surgery falls under ophthalmic surgery, which is explicitly excluded per the specialty scope criteria. The paper does not meet the inclusion criteria for any of the included specialties and is therefore excluded."
- **A p1 (include):** "The paper describes a shared control robotic system for retinal surgery, which involves a level of autonomy (shared control) as defined in the Yang et al. taxonomy. The system controls or directs a physical robot to execute a surgical action (laser ablation), and it is applied in a surgical context (retinal surgery). The system includes autonomous or semi-autonomous components that guide the surgeon's actions through haptic feedback, meeting the inclusion criteria for autonomous or semi-autonomous surgical robotics."

**786 EE-634** — *Laparoscope Manipulator Control for Minimally Invasive Surgery*
- **B p1 (exclude):** "The paper describes a laparoscope manipulator robot designed to automatically control the position of the laparoscope using visual information, but it does not clearly involve autonomous or semi-autonomous surgical robotic systems performing physical surgical tasks on tissue, phantoms, or in-vivo models. The focus is on maintaining a clinical view without additional operator input, which does not meet the inclusion criteria for surgical autonomy in the context of executing physical surgical tasks. Additionally, the paper does not mention any level of autonomy (Yang et al. taxonomy) or compare with human-controlled systems, manual surgery, or no comparator. The described system appears to be a robotic assistant rather than an autonomous or semi-autonomous system performing surgical tasks."
- **B p2 (exclude):** identical to p1 except the last sentence: "The system appears to be a robotic assistant rather than an autonomous or semi-autonomous surgical system performing a physical surgical task."
- **A p1 (include):** "The paper describes a laparoscope manipulator robot that automatically controls the position of the laparoscope using visual information and a control system, without requiring continuous human input. This system autonomously maintains the clinical viewing plane, which is a physical surgical task. The robot controls the movement of the laparoscope, fulfilling the criteria for autonomous or semi-autonomous surgical robotics. The task is performed in a surgical context (laparoscopic surgery), and the system involves a physical robotic component that directs the laparoscope."

**170 EE-070** — *Share control of surgery robot master manipulator guiding tool along the standard path*
- **B p1 (exclude):** "The paper describes a share control system for a surgical robot, where the surgeon maintains full control over the tool's position while the system provides support. This system does not involve autonomous or semi-autonomous robotic behavior that controls or directs a physical robot to execute a surgical action. Instead, it is a teleoperated system with shared control, which does not meet the inclusion criteria of involving an autonomous or semi-autonomous robotic component. Additionally, the paper does not demonstrate any level of surgical autonomy (Levels 1–5 per the Yang et al. taxonomy)."
- **B p2 (exclude):** "The paper describes a share control system for a surgical robot, where the surgeon maintains full control over the tool's position while the system provides support. This system does not involve autonomous or semi-autonomous robotic behavior that controls or directs a physical robot to execute a surgical action. Instead, it is a teleoperated system with shared control, which does not meet the inclusion criteria requiring an autonomous or semi-autonomous robotic component. Additionally, the paper does not involve a physical surgical task on tissue, phantoms, or in-vivo models, and it does not evaluate task accuracy, error rates, or safety events. The system described is purely teleoperated with shared control, which falls under the exclusion criteria for non-autonomous systems."
- **A p1 (include):** "The paper describes a share control system for a surgical robot, where the surgeon's hand controls the master manipulator while the system assists in guiding the tool. This involves a semi-autonomous robotic component that directs the physical robot to execute a surgical action, meeting the inclusion criteria for autonomous or semi-autonomous surgical robotics. The system supports the surgeon in guiding the tool, which aligns with the intervention criteria of semi-autonomous systems."

**702 EE-550** — *SurgiPose: Estimating Surgical Tool Kinematics from Monocular Video for Surgical Robot Learning* (A final IN)
- **B p1 = p2 (exclude):** "The paper describes a method for estimating surgical tool kinematics from monocular video to enable surgical robot learning, but it does not involve an autonomous or semi-autonomous robotic component that controls or directs a physical robot to execute a surgical action. Instead, it focuses on data acquisition and learning algorithms without demonstrating autonomous task execution in a physical surgical context."
- **A p1 (include):** "The paper describes a method for estimating surgical tool kinematics from monocular video to enable autonomous surgical robot learning. It involves autonomous robotic systems (dVRK Si) performing physical surgical tasks (tissue lifting and needle pickup) and focuses on enabling autonomous surgical policies through kinematic estimation. The work is relevant to the inclusion criteria as it involves autonomous or semi-autonomous robotic components controlling physical robots to execute surgical actions."

**693 EE-541** — *Assistive Cartesian Healthcare Robot for Forearm Vein Puncturing* (A final IN; the one corpus paper, P5)
- **B p1 = p2 (exclude):** "The paper describes a medical robot for forearm vein puncturing, but it does not mention any level of surgical autonomy (Levels 1–5 per the Yang et al. taxonomy) or any autonomous/semi-autonomous robotic component that controls or directs a physical robot to execute a surgical action. The focus is on assistive robotics with intelligent healthcare features, not on autonomous or semi-autonomous surgical robotic systems performing physical surgical tasks. Additionally, the paper does not involve a physical surgical task in the context of the included specialties (e.g., abdominal, gynecologic, urologic, etc.)."
- **A p1 (include):** "The paper describes an autonomous robotic system for venipuncture, which is a physical surgical task. The robot uses Cartesian coordinate mechanisms to locate and puncture veins, indicating control or direction of a physical robot to execute a surgical action. It involves autonomous behavior with sensor and image technology for needle placement, meeting the inclusion criteria for autonomous or semi-autonomous surgical robotics."

None of the seven B rationales uses insufficient-information wording (MEASURED, P1).

## P3 — every content change in the abstract-primary request, 83defc5 → HEAD

From `R1_abstract_primary_request.diff` (MEASURED: these are all the differing lines). The
format schema, PICO block, inclusion criteria, EXC-1, EXC-2, the specialty scope and its notes
are byte-identical. Grouping into items is by eye (INFERRED).

| item | before (83defc5) | after (HEAD) | before came from |
|---|---|---|---|
| **C1** system message | "You are a systematic review screening agent. **Evaluate whether the paper involves autonomous or semi-autonomous surgical robotics.** Follow the criteria and instructions in the user message. Respond ONLY with the requested JSON." | "You are a systematic review screening agent. **Screen the paper for the review titled "Autonomy in Surgical Robotics: A Systematic Review".** Follow the criteria and instructions in the user message. Respond ONLY with the requested JSON." | `stage_policies.abstract_primary.system_text` |
| **C2** EXC-4 | "Non-surgical robotics (industrial, rehabilitation, exoskeletons, prosthetics)" | "Non-surgical robotics (industrial, warehouse, rehabilitation, exoskeletons, prosthetics, assistive devices)" | `transitional_text.abstract_primary` |
| **C3** EXC-8 | "Papers with no abstract available" | "Papers with no abstract or insufficient information to determine eligibility — do not default to inclusion when evidence is absent" | `transitional_text.abstract_primary` |
| **C4** the uncertainty rule | "Decide 'include' or 'exclude'. When uncertain and the paper MIGHT involve autonomous surgical robotics, prefer 'include' — a later verification pass will catch false positives." | "Decide 'include' or 'exclude'." / "If the abstract gives partial evidence that the paper might meet the criteria, include it — a later verification pass will catch false positives." | `stage_policies.abstract_primary.instruction_text` |
| **C5** the bar on excluding | "EXCLUDE only if the paper CLEARLY does not involve surgical robotics at all, or CLEARLY has no abstract available." | "Exclude when the paper clearly meets an exclusion criterion listed above." | same `instruction_text` |
| **C6** evidence absent | — (no such sentence for a paper with an abstract) | "If there is no abstract, or it gives too little information to determine eligibility, exclude it — do not default to inclusion when evidence is absent." | — |

**Echoes, per P1 cluster (INFERRED)**

- **Excluded specialty (19 flips).** Rationales quote the specialty notes, which did not change:
  598 B "According to the exclusion criteria, orthopedic surgery is excluded unless the autonomous
  task is generalizable"; 372 B "neurosurgery is explicitly excluded unless the autonomous task is
  generalizable", against the prompt's "Orthopedic and neurosurgery papers are excluded only when
  the autonomous task is specialty-specific". The same notes were in A's request, and A included
  all 18 of these specialty-category flips. No changed text is echoed. The change that removes
  the obstacle is **C5**: before it, exclusion was allowed only for a paper that "CLEARLY does not
  involve surgical robotics at all"; a dental or neurosurgical robot paper plainly involves
  surgical robotics.
- **Autonomy absent / inclusion criterion unmet (21 "does not involve … autonom…", 19 "does not
  meet … inclusion criteri…").** Rationales quote the unchanged inclusion criterion: 583 B "does
  not involve autonomous or semi-autonomous robotic components that control or direct a physical
  robot to execute a surgical action … It does not meet the inclusion criteria", against "Must
  involve an autonomous or semi-autonomous robotic component that CONTROLS or DIRECTS a physical
  robot to execute a surgical action". A never wrote "does not meet … inclusion criteri…" (0 of
  172). Change items: **C5** (inclusion failures could not ground an exclusion before) and **C1**
  (the old system message reduced the task to one question). A's rationales echo C1's before-text
  directly: 583 A "meeting the criteria for autonomous or semi-autonomous surgical robotics"; the
  phrase appears in 104 of A's 172 rationales and 20 of B's.
- **Autonomy not stated (18).** 222 B "it does not mention any level of autonomy (Levels 1–5)";
  693 B "does not mention any level of surgical autonomy (Levels 1–5 per the Yang et al. taxonomy)".
  The Levels text is the unchanged PICO Intervention. Treating "not mentioned" as a ground to
  exclude is what the hypothesis describes; the candidate items are **C4** (the include trigger
  narrowed from "MIGHT involve autonomous surgical robotics" to "partial evidence that the paper
  might meet the criteria") and **C6**. No rationale in this cluster quotes C4's or C6's words.
- **Insufficient information (6).** The only cluster that echoes changed text word for word:
  290 B "The lack of sufficient detail to determine eligibility leads to exclusion"; 112 B "The
  abstract does not provide sufficient information to determine if the paper meets the inclusion
  criteria"; 737 B p2 "The abstract lacks sufficient detail to determine eligibility" — against
  **C6** "gives too little information to determine eligibility, exclude it" and **C3**
  "insufficient information to determine eligibility". All six are PI excludes.
- **Analysis / planning only (12).** 702 B "focuses on data acquisition and learning algorithms
  without demonstrating autonomous task execution". EXC-6 is not in either primary request; this
  is the unchanged inclusion criterion again (**C5**, **C1**).
- **C2** is echoed nowhere that matters: non-surgical-robotics wording appears in 2 of 84 B flip
  rationales.

## P4 — how the policy renders

**HEAD** (`engine/core/eligibility_render.py`, `decision_instruction` for `abstract_primary`,
from `when_uncertain: include` and `when_evidence_absent: exclude`):

> Decide 'include' or 'exclude'.
> If the abstract gives partial evidence that the paper might meet the criteria, include it — a later verification pass will catch false positives.
> If there is no abstract, or it gives too little information to determine eligibility, exclude it — do not default to inclusion when evidence is absent.
> Exclude when the paper clearly meets an exclusion criterion listed above.

The second line is `_uncertain_sentence("include")`, the third `_absent_sentence("exclude")`,
the fourth a fixed engine line. The same policy is also stated in EXC-8's canonical text (C3).

**83defc5.** The spec already carried the same policy values — `stage_policies.abstract_primary`:
`when_uncertain: include`, `when_evidence_absent: exclude` — but the prompt did not render
from them. It rendered `instruction_text` verbatim:

> Decide 'include' or 'exclude'. When uncertain and the paper MIGHT involve autonomous surgical robotics, prefer 'include' — a later verification pass will catch false positives.
> EXCLUDE only if the paper CLEARLY does not involve surgical robotics at all, or CLEARLY has no abstract available.

The first sentence is the equivalent of `when_uncertain: include`, with a different trigger.
**For a paper with an abstract there was no equivalent of `when_evidence_absent: exclude`.** The
insufficient-information rule existed only in `absent_abstract_text`, rendered in place of a
missing abstract: *"Abstract: [Not available. Per the exclusion criteria, papers with no abstract
or insufficient information to determine eligibility should be EXCLUDED.]"* All 86 papers have
an abstract, so no 2f request at 83defc5 contained it.

## P5 — where the 86 papers stand (read-only query)

The review database was opened once, `mode=ro`. File before and after: 101,978,112 B, mtime
2026-09-11 02:00:52.636956943 — unchanged (MEASURED, by the query script and by shell `stat`).
Exit stage is read from `status` (INFERRED mapping: `ABSTRACT_SCREENED_OUT` → abstract screening,
`PDF_EXCLUDED` → PDF quality check, `FT_SCREENED_OUT` → full-text screening).

| status | PI exclude | PI include |
|---|---:|---:|
| ABSTRACT_SCREENED_OUT | 76 | 2 |
| PDF_EXCLUDED | 0 | 2 |
| FT_SCREENED_OUT | 0 | 5 |
| AI_AUDIT_COMPLETE | 0 | 1 |

**The ten PI includes (MEASURED)**

| paper | status | exit | FT decision (reason_code, model) | 2f B primary |
|---|---|---|---|---|
| 693 EE-541 | **AI_AUDIT_COMPLETE** | in corpus; 1 extraction | FT_ELIGIBLE (qwen3:32b), FT verifier FT_ELIGIBLE | **OUT** |
| 579 EE-427 | FT_SCREENED_OUT | full text | FT_EXCLUDE `no_autonomy_content` (qwen3.5:27b) | IN |
| 583 EE-431 | FT_SCREENED_OUT | full text | FT_EXCLUDE `protocol_only` (qwen3.5:27b) | OUT |
| 569 EE-417 | FT_SCREENED_OUT | full text | FT_EXCLUDE `wrong_specialty` (qwen3.5:27b) | OUT |
| 469 EE-317 | FT_SCREENED_OUT | full text | FT_EXCLUDE `insufficient_data` (qwen3.5:27b) | IN |
| 702 EE-550 | FT_SCREENED_OUT | full text | FT_EXCLUDE `no_autonomy_content` (qwen3:32b) | OUT |
| 190 EE-080 | PDF_EXCLUDED | PDF quality | — | IN |
| 786 EE-634 | PDF_EXCLUDED | PDF quality | — | OUT |
| 222 EE-092 | ABSTRACT_SCREENED_OUT | abstract, `rejected_reason` "verifier-excluded, consistent with 416/416 adjudication concordance" | — | OUT |
| 170 EE-070 | ABSTRACT_SCREENED_OUT | abstract, same `rejected_reason` | — | OUT |

All 76 PI excludes are `ABSTRACT_SCREENED_OUT` with `rejected_reason` NULL, `updated_at`
2026-03-13T04:19 for every one. All 86 carry `[specialty_rescreen]` abstract decisions; 80 carry
`[specialty_rescreen]` verifier decisions. Verifier history per paper: include then exclude 67,
exclude only 13, include only 6.

## P6 — verdict

**Hypothesis:** the shift is driven by resolving the recall-first/EXC-8 contradiction into
evidence-absent → exclude, which the model reads as "autonomy not stated → exclude".

**Verdict (INFERRED): the evidence does not support it as the main driver. It points to the
removal of the old primary pass's single gate — C5's "EXCLUDE only if the paper CLEARLY does not
involve surgical robotics at all", together with C1's "Evaluate whether the paper involves
autonomous or semi-autonomous surgical robotics" — under which the inclusion criteria and the
specialty scope, present and unchanged in both requests, were not being used as grounds to
exclude. The evidence-absent rule is a minority contributor.**

For the hypothesis:
- "Not stated" reasoning is real and new: 18 of 42 B flip rationales say the abstract does not
  mention or specify autonomy, against 2 in A; 3 of the 7 lost includes (222, 786, 693) are in
  that group.
- Evidence-absent wording appears only in B and C (insufficient-information 0 / 8 / 11 and
  "determine eligibility" 0 / 13 / 5 across 172 rationales per arm), and 6 flips quote C3/C6
  nearly word for word.
- Before 2c the model never saw an evidence-absent rule for a paper with an abstract (P4).

Against:
- Only 6 of 42 flips cite insufficient information, **none of the 7 lost PI includes** does, and
  "default to inclusion" and "evidence is absent" are never echoed.
- The largest groups cite text that did not change: the inclusion criterion's "CONTROLS or
  DIRECTS a physical robot" (21 "does not involve … autonom…", 19 "does not meet … inclusion
  criteri…"; 0 of each in A) and the specialty notes (15 name an excluded specialty, 10 quote
  "unless … generalizable"; 1 each in A). These are positive claims that a criterion fails, not
  claims that evidence is missing.
- 18 of the 42 flips are papers in excluded-specialty categories. Specialty exclusion has nothing
  to do with evidence absence, and A included every one of them under an identical specialty
  scope — consistent with an exclusion bar that permitted nothing but non-robotics exclusions.
- A's rationales argue a single question: 104 of 172 contain C1's old phrase and 125 of 172
  argue the paper meets the criteria; none argue an inclusion criterion fails.

What this readout cannot do: separate C1, C4, C5 and C6 causally. They changed together, and
only an ablation would apportion the 42 among them. Out of scope here.

## Found, not asked

1. **Two PI includes are recorded as abstract-screened-out.** 222 EE-092 and 170 EE-070 have
   status `ABSTRACT_SCREENED_OUT` and `rejected_reason` "verifier-excluded, consistent with 416/416
   adjudication concordance", though the workbook's PI decision is include. The workbook's mtime
   is 2026-03-13 04:16:19; the 76 excludes were set at 04:19. Whether PI decisions were ever
   applied to these two is not established.
2. **Only one of the ten PI includes is in the corpus — 693 EE-541 — and arm B drops it at
   primary.**
3. **B's specialty exclusions include misassignments (INFERRED readings):** 261 and 325, cochlear
   implantation "falls under ophthalmic surgery"; 774, lateral skull base as ophthalmic; 429,
   robot-guided neuromapping in rectal cancer surgery (taTME) as neurosurgery. 344 calls a
   retrospective study not "peer-reviewed original research".
4. **A misreads the scope in the other direction (INFERRED readings):** 737 A "an application in
   neurosurgery, which falls under the included specialty"; 238 A "ophthalmic surgery (eye
   surgery), which falls under the included specialty of head and neck surgery".
5. **At 83defc5 the stage-policy values already existed and did not reach the prompt.** The fold
   made `when_uncertain` and `when_evidence_absent` render for the first time; before it, a
   verbatim `instruction_text` override and `system_text` stood in their place (P4).
6. **The startup `git pull` in claude-config was skipped**, because this brief lists
   claude-config under DO NOT TOUCH.

## Standard gate before this commit

2,259 passed / 17 deselected — five chunks via `find` (475 / 565 / 361 / 442 / 416; deselects
0 / 0 / 10 / 6 / 1). Review database unchanged across the run.
