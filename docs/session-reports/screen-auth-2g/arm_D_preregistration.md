# SCREEN-AUTH-01 Phase 2g — arm-D pre-registration and PI verdicts on the 2f reading sheet

Committed before the Phase 2g Part 2 fold and before the arm-D re-smoke, so the rule and the
reference correction exist in git ahead of any result they will judge.

**Source:** the PI's verdict workbook on the 2f reading sheet, Part 1 (2026-09-19), transcribed
verbatim into the SCREEN-AUTH-01 Phase 2g Part 2 brief and copied from that brief into this
directory. Nothing here was derived or re-read from the workbook by the implementing session.

## Pre-registered arm-D rule (PI, 2026-09-19, verbatim)

> GO if at least ten of the twelve SILENT papers return to IN or FLAGGED at primary, none of the
> five EVIDENCED papers move to IN, and the primary flag rate stays materially below A's.

## Verdicts

The 18 rows are in [`pi_verdicts_2f_part1.csv`](pi_verdicts_2f_part1.csv), verbatim. Columns:
`verdict` is the PI's reading of arm B's OUT (SILENT / EVIDENCED / MISREAD); `april_label` is the
April workbook label; `label_after_full` is the PI's label after reading the whole abstract.

Counts: 12 SILENT, 5 EVIDENCED, 1 MISREAD.

**Reference correction.** Three April include labels are reversed to exclude after the full
abstract was read: paper 569 (EE-417), 170 (EE-070), 702 (EE-550). The 2f PI reference file is
not edited; a later step carries these three as a separate corrections file read alongside it.

Groups: G1 = the one PI include that reached the corpus and B dropped; G2 = the four specialty
misreads flagged by eye in 2g Part 1; G3 = the other six April includes B dropped; G4 = seven
April excludes where B's ground looked like absence.

PI note on 261: "if picked correctly ENT speciality would still have been excluded".

## Architect rulings recorded with the verdicts (from the brief)

- **R1.** abstract_primary exclusion_basis = evidenced_exclusion_only; inclusion
  non-satisfaction and silence are not grounds.
- **R2.** "The abstract" at that stage means the rendered title plus abstract.
- **R3.** Specialty scope: no content change in the fold commit (1 of 4 flags confirmed,
  outcome-neutral).
- **R4.** Other five stages declare absence_is_evidence; nothing new is rendered for them in the
  fold commit. FT wording and the abstract verifier's basis are separate items after D is ruled.

## Operational reading of the rule (architect, 2026-09-19, verbatim)

Appended before any arm-D model call, in the Phase 2g P3 pre-flight commit.

> O1. A paper's outcome in an arm = its final abstract-stage decision after the verifier, exactly
>     as 2f scored it: IN, FLAGGED, or OUT.
> O2. Clause 1: count of the 12 SILENT papers (pi_verdicts_2f_part1.csv, verdict=SILENT) whose
>     arm-D outcome ∈ {IN, FLAGGED}; GO requires ≥ 10.
> O3. Clause 2: count of the 5 EVIDENCED papers whose arm-D outcome = IN; GO requires 0.
>     FLAGGED among the five is reported separately and does not fail the clause.
> O4. Clause 3: flag rate = FLAGGED / 86 per arm, from this run's A and D. "Materially below" =
>     D's flag rate ≤ A's flag rate − 15 percentage points.
> O5. All three clauses are reported with raw counts whether or not they pass. Nothing in O1–O4
>     changes after the run starts.

## Arms for the 2g re-smoke

| arm | tree | what it is |
|---|---|---|
| A | worktree at `83defc5` | pre-fold engine and spec, as in 2f |
| B | worktree at `61326fa` | the exact 2f arm-B tree, so the stability control is a pure rerun |
| D | repo tree at `56c5c57` | the exclusion-basis fold |

Arm C (eight exclusions at abstract primary) is retired by the 2f four-vs-eight ruling and is
not part of this set.
