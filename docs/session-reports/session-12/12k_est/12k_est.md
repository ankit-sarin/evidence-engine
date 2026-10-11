# 12k-EST — read-only inputs for the ruling on the path to the tag (R604)

**Status: COMPLETE. Read-only.** No change to the plan, to any row, or to `engine/`, `tests/`, `scripts/`, `analysis/` or `review_specs/`; no gate run; no model call; live `review.db` read `mode=ro` by the fingerprint compare only. Nothing here reclassifies, reorders or re-scopes. Reference HEAD `6b6354e0c12f5619c215b87835295bf4009c6288`.

Everything counted is counted by `12k_est/est_12k.py` → `est_12k.json`, from the plan, the two internal-review read-outs, `12k_0c/recount_12k.py` and git history. The tables T1–T17 at the end are the script's output, unedited. **MEASURED** means read from the plan, a committed read-out or git by the script. **INFERRED** means a figure this read-out derives; each names its basis.

R605 is pending transcription and is not written here. Its confirmation that the first-named package "is a counting rule, not an ownership ruling" applies to every package figure below.

## 0. Pre-check and E1

- HEAD `6b6354e`, clean, level with origin; the decision log ends at R604; `db_fingerprint --compare` exit 0, IDENTICAL.
- **E1 holds, with one precision.** `recount_12k.json` carries a per-row class and package only for the 70 rows that entered at 12k. The per-row values for all 238 exist in `recount_12k.py`'s run, which this script executes: it reproduces `recount_12k.json` byte for byte, 238 rows, 98 / 44 / 96, and the package table of the 12k-OPEN report (T1's row columns).
- **E2 does not hold for most rows.** The plan states a pin effect on 11 rows and the R599 block covers the 70 Wave 2 rows; 87 Wave 1 rows and 70 older rows carry no statement (§3.1). A finding, not a stop.

## 1. Rows by package and class (MEASURED)

T1 gives the counts; T2 the IDs; T3 the 35 conditional Class 1 rows with their condition; T4 the 27 rows that name more than one package; T5 the 47 rows whose class a Phase A may move.

Rules applied, all mechanical:
- **Class and first-named package** are `recount_12k.py`'s.
- **Conditional Class 1**: the row's Class cell opens "1 (conditional" or "1 (latent", or — for a row on the list by notes only — every Class 1 note on it is marked conditional.
- **Every package named**: each `P1`–`P7` in the row's package cell, its coverage-map line, its 12g note, and its classed Wave 1 / Wave 2 notes.
- **Class may move at Phase A**: the conditional rows (R587, R599), the rows carrying R600's "may weigh" sentence, and rows carrying a conditional note.

P7 holds one row that is not Class 3: INT-j12-30 (Class 2, as the read-out proposed and R599 accepted).

## 2. Fix units (MEASURED grouping; no grouping by similarity)

**238 rows are 215 fix units: Class 1 / 2 / 3 = 85 / 38 / 92.** 16 units hold more than one row (T6); every other row is one unit. A unit is counted at the package and class of its highest-class row.

Rows are merged only on these stated bases, and only when both rows have the same first-named package:
1. **One finding on several rows** (R597(c)), where every row carrying that finding is on the list by notes only, so the finding is the whole of what is pre-tag on each. This gives Wave 2 row 5 (C42, A2), rows 15 and adjacent item 5 (C47, C37, C12), and row 25 (C28, B9).
2. **"with <row>"** in the row's package cell, coverage-map line or 12g note — the plan's own ride-together statements. An alternative the read-out offered and the ruling did not take ("(or P6 with D22's 023 build)") is not counted.
3. **R589**: the rows that land "in the INT-h12-2 batch".
4. **R543** (E-TEST with INT-g6-1), **R531/R533/R542** (A-7 with C56's predicate), **R538** (INT-g3-1 with B-F15).

Not merged, and listed instead:
- T7: 5 findings that sit on two rows where one row has content of its own.
- T7b: 4 rides across packages, where a part of one row lands with a row of another package.
- **R601's rows stay one unit each.** No ruling makes them one piece of work; R601 sets the size of each fix (the loud-failure minimum), not a grouping. They are T10.

## 3. Ordering constraints, and the path they imply

### 3.1 The pin (R539) — MEASURED from the rows' own words

| what the plan states | rows |
| --- | ---: |
| pin-affecting, stated | 5 |
| pin-affecting in part, stated | 1 |
| not pin-affecting, stated ("not pin-affecting", "outside the pin", "audit-side") | 5 |
| not pin-affecting by the readers' reading — Wave 2 rows (the R599 block's MEASURED line) | 70 |
| no statement on the row; a Wave 1 row not among those `12i_review_w1.md` and R589 name | 87 |
| **pin-effect unknown — the row states nothing** | **70** |

T8 lists them by package. The stated pin-affecting rows are INT-g6-1 ("pin-affecting, first"), INT-h12-2 (items (a) and (b); item (e) is stated not pin-affecting), INT-i12-4, INT-i12-38, C34 (Wave 1 row 14) and the prompt-text item of INT-i12-41, which is a P7 row.

**E2's finding.** 70 rows from the 12f, 12g and 12h lists and the inventory state nothing about the pin. 16 of them are in P1: B-F14, B-F17, A-1, A-1N, A-4b, A-7, A-10, A-11, INT-g12-4, INT-g12-6, INT-g12-7, C49, C56, E-UNITMAP, E-ROOT, E-TEST. For the Wave 1 rows the plan carries no per-row statement either; the read-out names the pin-affecting ones and is silent on the rest, which is not the same as stating they are not.

### 3.2 Migrations (P6) — MEASURED

T9 lists the 13 rows that name P6 or a migration. As the plan states them:
- **023**: C39, E-CALL, D22 with D23's refusal writer, D25, D26(b); A-2's assertion ("P2; its assertion in P6 (023)"); B-F06's search ledger (R540: "023 with B-F06's ledger").
- **024**: D14, "after measuring the count".
- **Before either**: P6's Phase A reads the schema and the applied migrations (R579) and states the conditions under which the drift refusal holds (R592).
- Rows whose first part must land in an earlier package: A-2 (P2) and B-F06 (P3, "the minimum fix").

### 3.3 The R601 / R546 gate — MEASURED

- R546: the B-F11 fix (P4) comes first; P4's Phase A measures whether the re-screen covers 129 or all 171 papers. R549 and R591(b) leave open, for the same Phase A, the 165 eligible papers screened on cut text and the 15 empty-abstract exclusions.
- R601: "The R546 re-screen's FT adjudication runs only after the FT page and FT importer rows in the set have their loud-failure fixes."
- **The rows in R601's set that name an FT page, the FT importer or the shared workbook** (by the files their Evidence cell names; T10): INT-i12-77, INT-j12-6, INT-j12-13, INT-j12-14, INT-j12-23, INT-j12-24, INT-j12-25. INT-i12-77 is there for Wave 2 row 19's page/workbook half.
- Set rows that name only the abstract page or the PDF-quality page: INT-j12-3, INT-j12-4, INT-j12-5, INT-j12-10, INT-j12-26.

### 3.4 B19 (R537) and other stated dependencies

- B19 is off the tag gate and "scheduled before the four human workbooks load" (R537). It is not on the list.
- INT-g12-1 lands "after INT-g6-1" (R568). E-TEST lands "with or immediately after INT-g6-1" (R543).
- C56: the predicate first with the extract-path handlers; handlers 4–7 and 8–11 "at P1's tail"; handler 3 in P7 (R542).
- INT-i12-7 and INT-i12-8 are a LOCATOR_VERSION bump, audit-side, outside the pin (R590).
- A-1N is "a hard prerequisite" of Run 7, which spans 09:00 UTC (R561).
- The INT-h12-2 batch ends in "a comparison rehearsal on Rehearsal A's 12 papers" (R571, R589): model calls on a throwaway review.

### 3.5 The critical path these imply, as rows (no reorder proposed)

1. **P1, pin-affecting first**: INT-g6-1 → (A-10, E-TEST with it; INT-g12-1 after it) · C56's predicate with A-7 · the INT-h12-2 batch (INT-h12-2, INT-i12-4, INT-i12-38, C34's Wave 1 row 14, INT-i12-41's prompt item) → the comparison rehearsal.
2. **P2, P3** in R540's order, carrying the first parts of A-2 and B-F06.
3. **P4**: B-F11 → P4's Phase A (R546, R549, R591(b)) → the re-screen → the FT rows of R601's set at their loud-failure minimum → the FT adjudication, which is a person's work.
4. **P5**, which holds INT-i12-77 (see observation O1).
5. **P6**: its Phase A (R579, R592) → 023 → 024, each rehearsed and then written to live.
6. **P7**, the Class 3 batch.
7. **The freeze smoke**, which records the binding pin (R539) → tag `freshman` → Run 7, with A-1N closed.

## 4. The Class 3 batch (MEASURED; file grouping by a stated rule)

- **96 Class 3 rows: 89 in P7 and 7 riding in another package** (T12; R543 and the rows' own package cells).
- In P7 the 89 Class 3 rows are 88 fix units.
- **By first-named file or module (T11): 40 distinct files or modules; 33 rows name none; 14 rows name more than one.** The rule: the first `.py` / `.md` / `.yaml` file in the row's text cell or in the "where" part of its Evidence cell, or — for a Wave 1 row, whose "where" is written `module.function` — the first dotted module name. Read-out and reader files a row cites are excluded. A row counted under its first-named file may touch others.
- The 33 rows naming no file are housekeeping lines, measurement records, batch rows ("Class 3 batch") and rows whose text names a function without its file.
- **P7 rows that name a pin-affecting file: 10** (T13). "Pin-affecting file" is INFERRED: the modules behind R539's four categories and the 12c closure's account of what the pin hashes — `units`, `prompts`, `pipeline`, `materialize`, `contracts`, `extractor`, `effective_config`, `review_spec`, `codebook`, `run_manifest`. Naming such a file does not make a row pin-affecting; only INT-i12-41 is stated so.

## 5. Phase A load (MEASURED)

T14 lists every sentence of rulings R540–R604 that names a Phase A; T15 the rows whose package cell names one. By package, the items in force:

| package | Phase A items the plan assigns | conditional rows whose trigger a Phase A establishes (R587, R599) |
| --- | --- | ---: |
| P1 | INT-g12-1's form (R568); INT-g12-4, Phase A first (R569); Wave 1 row 64's family attribute, Phase A first (R588, A4) | 16 |
| P2 | none named beyond its conditional rows | 3 |
| P3 | recover the March query text read-only before any OpenAlex call (R547) | 4 |
| P4 | which re-screen scope applies (R546); the 165 papers screened on cut text (R549); paper 168 and the other 14 (R591(b)); execute the page script or state it cannot, rows 7, 20 and 38 (R600) | 6 |
| P5 | INT-h12-1, Phase A first, with the B5 question (R567); INT-i12-77's list of every path that reads or writes a paper key (R595, R596), with Wave 2 rows 14 and 19 and adjacent item 12 as inputs (R600, R602); whether I5 and A12 are fixed at HEAD (R603) | 5 |
| P6 | read the schema and the applied migrations before 023 / 024 (R579); the conditions under which the drift refusal holds (R592) | 1 |
| P7 | none | 0 |

## 6. Effort basis

### 6.1 Measured throughput (git and the plan's session records)

T16. Four fix-bearing sessions, 12a, 12c, 12d and 12e, closed **20 rows in 4 sessions: 4–7 rows a session**, at 1.16–3.74 wall hours a row from the plan's stated open time to the closeout commit. Those hours include rulings round trips: 35–58 rulings were issued per session. The record does not state gate runs as a count; the plan's rule is a gate before every commit, and 4–8 commits per session touched code or tests.

What those sessions' rows were: mostly single defects with a Phase A read first and a ruling per stop. They are not a sample of this list.

### 6.2 Per-package range in sessions

T17. **Each range is INFERRED**: the package's Class 1 and Class 2 fix units divided by the measured 7 and 4 rows a session.

| package | Class 1 + 2 units | sessions (INFERRED) |
| --- | ---: | --- |
| P1 | 36 | 6–9 |
| P2 | 18 | 3–5 |
| P3 | 9 | 2–3 |
| P4 | 33 | 5–9 |
| P5 | 17 | 3–5 |
| P6 | 9 | no measured basis for this list's two migrations (see below) |
| P7 | 1 (+ 88 Class 3 units) | no measured basis |

What the ranges leave out, each with no measured basis and so no number:
- **Class 3 work anywhere.** No earlier session ran a batched Class 3 pass (R508); the 96 Class 3 rows are outside every range above.
- **P4's re-screen and FT adjudication.** Model calls on 129 or 171 papers, and a person's adjudication, have no session record to scale from.
- **P1's comparison rehearsal** on 12 papers.
- **Phase A reads** (§5) and whatever they rule. Many of this list's units are conditional rows whose first step is a Phase A.
- **P6.** The one measured migration session is 10b: opened 2026-09-29 16:04 UTC, migration 022 written to live at 17:21:15 UTC after two rehearsals, with a 5-paper smoke of 2,791.2 s the same day; 10a before it (13 commits) built the migration. That is one migration, not this list's 023 and 024, so no range is given.

### 6.3 Fixed costs, stated separately (MEASURED unless marked)

- **Gate wall time: 792.72 s per run** (12k); band 763–843 s.
- **Migration rehearsal.** The plan's rule: "a session that migrates the database records the fingerprint before and after and verifies its backup first" (Step 4). The plan's "Appendix B" is the startup verify; this file has no appendix that sets a rehearsal procedure, and the working record is 10b's (rehearsal, rehearsal 2, live write). Live writes are barred 07:00–10:35 UTC (`CLAUDE.md`, R86).
- **Freeze smoke.** Session-sequence row 10: "a 5-paper smoke run on the tagged state". 10b's 5-paper smoke took 2,791.2 s on the non-elicited path; no elicited 5-paper smoke time is on record under that name.
- **Phase A may add rows under R509; that growth is not estimated.**

## 7. Run 7

- R561, as the plan carries it: "R561 — **12g-RA-B.** RB-8 / RA-5: the Run 7 estimate is 35–38 h (sample mean 38.0 h, length model 34.6 h; both lean long), replacing v76's 20–35 h. Run 7 therefore spans 09:00 UTC; A-1N stays a hard prerequisite.".
- **Rows opened or entered at 12j and 12k: 165. Stated pin-affecting among them: INT-i12-4, INT-i12-38, INT-i12-41, C34** — all Wave 1 (12j). **No row opened at 12k is pin-affecting**, by the readers' reading that the R599 block records.
- Those four change what the elicited run sends (prompt text and the semantic hash) and land in the INT-h12-2 batch, whose comparison rehearsal re-measures on Rehearsal A's 12 papers (R571, R589).
- Run 7's extraction inputs are the eligible corpus and its parsed texts. Three ruled items can change the corpus before Run 7, none from 12k: the R546 re-screen, R549's open item, and R591(b)'s 15 papers. Wave 2 rows 22 and 42 change what a PubMed search retrieves, which is not a Run 7 input for this review.
- **So the carried 35–38 h rests on a basis no 12k row disturbs.** It was measured before the INT-h12-2 batch's prompt changes and on the corpus as it stood, and both of those are already ruled to be re-measured or decided before Run 7. It is not re-estimated here.

## 8. The C12 and C37 notes

Both notes of 2026-10-11 state R602 item 5's condition, by quoting the ruling's bullet. They put "conditional" after the package rather than after the class.

- **C12**, quoted: "*Note 2026-10-11 (12k item 0c, R602):* Wave 2 adjacent item 5 (seen by L09a) is noted on this row — Class 1, P5; conditional, with Wave 2 row 15 (which is 1 / P5) — R602: "5 → notes on C12 and C37 (commits a caller's open transaction), Class 1 conditional, with row 15.""
- **C37**, quoted: "*Note 2026-10-11 (12k item 0c, R602):* Wave 2 adjacent item 5 (seen by L09a) is noted on this row — Class 1, P5; conditional, with Wave 2 row 15 (which is 1 / P5) — R602: "5 → notes on C12 and C37 (commits a caller's open transaction), Class 1 conditional, with row 15.""

Neither was edited.

## 9. Observations, not acted on

- **O1.** INT-i12-77 is a P5 row, and Wave 2 row 19's page/workbook half sits on it. R601 makes the FT adjudication wait on that half, and the FT adjudication follows P4's re-screen. A P4 step therefore waits on part of a P5 row.
- **O2.** 70 rows carry no pin statement, 16 of them in P1 (§3.1).
- **O3.** C56 names four packages through its notes (P1, P2, P4, P7); it is counted in P1.
- **O4.** INT-j12-30 is Class 2 in P7.
- **O5.** Other rows that name FT files and are outside R601's set, so carry full fixes pre-tag: INT-j12-37, INT-j12-40, INT-j12-42 and INT-j12-43 (all Class 3, P7).
- **O6.** The brief's "migration rehearsal per Appendix B" finds no such appendix in the plan (§6.3).
- **O7.** T14 shows R598's and R596's Phase A sentences beside the rulings they amend; they are the same items, not additional ones.

## Generated tables (the script's output, unedited)

### T1 — rows and fix units by package and class

| package | rows C1 | rows C2 | rows C3 | rows | units C1 | units C2 | units C3 | units | C1 conditional | rows naming >1 package | class may move at Phase A |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| P1 | 29 | 14 | 4 | 47 | 26 | 10 | 1 | 37 | 16 | 11 | 19 |
| P2 | 12 | 7 | 1 | 20 | 12 | 6 | 1 | 19 | 3 | 3 | 5 |
| P3 | 10 | 2 | 2 | 14 | 7 | 2 | 2 | 11 | 4 | 1 | 4 |
| P4 | 24 | 10 | 0 | 34 | 23 | 10 | 0 | 33 | 6 | 5 | 11 |
| P5 | 15 | 8 | 0 | 23 | 10 | 7 | 0 | 17 | 5 | 1 | 7 |
| P6 | 8 | 2 | 0 | 10 | 7 | 2 | 0 | 9 | 1 | 0 | 1 |
| P7 | 0 | 1 | 89 | 90 | 0 | 1 | 88 | 89 | 0 | 6 | 0 |
| all | 98 | 44 | 96 | 238 | 85 | 38 | 92 | 215 | 35 | 27 | 47 |

### T2 — rows by package and class (IDs)

| package | Class 1 | Class 2 | Class 3 |
| --- | --- | --- | --- |
| P1 | A-4b, A-10, A-11, INT-g6-1, INT-g12-1, INT-i12-1, INT-i12-2, INT-i12-3, INT-i12-4, INT-i12-5, INT-i12-6, INT-i12-7, INT-i12-8, INT-i12-9, INT-i12-10, INT-i12-11, INT-i12-12, INT-i12-69, INT-i12-71, INT-i12-73, C56, E-UNITMAP, C34, B9, C24, C28, INT-j12-17, INT-j12-18, INT-j12-19 | B-F14, B-F17, A-1, A-1N, A-7, INT-g12-4, INT-h12-2, INT-i12-31, INT-i12-45, C49, E-ROOT, INT-j12-31, INT-j12-32, INT-j12-33 | INT-g12-6, INT-g12-7, INT-i12-38, E-TEST |
| P2 | B-F01, B-F02, B-F03, A-2, INT-g6-2, INT-i12-13, INT-i12-14, INT-i12-16, INT-i12-19, INT-i12-23, INT-i12-24, INT-j12-11 | B-F16, INT-i12-50, INT-i12-68, INT-i12-74, A16, C50, INT-j12-21 | INT-i12-51 |
| P3 | B-F04, B-F05, B-F06, B-F07, B-F08, INT-g2-1, INT-i12-21, INT-i12-72, E-METHODS, INT-j12-16 | B-F12, INT-j12-28 | INT-g12-9, INT-g12-10 |
| P4 | B-F10, B-F11, B-F13, INT-i12-17, INT-i12-18, INT-i12-20, INT-i12-22, D20, J8, A2, C42, INT-j12-3, INT-j12-4, INT-j12-5, INT-j12-6, INT-j12-7, INT-j12-8, INT-j12-9, INT-j12-10, INT-j12-12, INT-j12-13, INT-j12-14, INT-j12-15, INT-j12-22 | INT-i12-30, INT-i12-32, INT-i12-33, INT-i12-35, INT-i12-36, INT-j12-23, INT-j12-24, INT-j12-25, INT-j12-27, INT-j12-29 |  |
| P5 | B-F15, INT-h12-1, INT-i12-26, INT-i12-27, INT-i12-77, B18, B2, C12, C37, C47, G3, INT-j12-1, INT-j12-2, INT-j12-20, INT-j12-58 | B-F09, INT-g3-1, INT-i12-28, INT-i12-29, INT-i12-70, C45, INT-j12-26, INT-j12-34 |  |
| P6 | INT-i12-25, C39, D22, D23, D25, D26(b), E-CALL, C10 | INT-i12-34, D14 |  |
| P7 |  | INT-j12-30 | A-3, A-4a, A-5, A-6, A-8, A-9, HK-RETIRE, HK-ROOTDB, HK-12C, HK-ARCH, HK-WITHMODEL, R501, INT-g12-2, INT-g12-3, INT-g12-5, INT-g12-8, INT-i12-15, INT-i12-37, INT-i12-39, INT-i12-40, INT-i12-41, INT-i12-42, INT-i12-43, INT-i12-44, INT-i12-46, INT-i12-47, INT-i12-48, INT-i12-49, INT-i12-52, INT-i12-53, INT-i12-54, INT-i12-55, INT-i12-56, INT-i12-57, INT-i12-58, INT-i12-59, INT-i12-60, INT-i12-61, INT-i12-62, INT-i12-63, INT-i12-64, INT-i12-65, INT-i12-66, INT-i12-67, INT-i12-75, INT-i12-76, INT-i12-78, INT-i12-79, INT-i12-80, INT-i12-81, INT-i12-82, INT-i12-83, INT-i12-84, INT-i12-85, INT-i12-86, INT-i12-87, INT-i12-88, INT-i12-89, B12, C38, C48, I20, I21, A14, C13, INT-j12-35, INT-j12-36, INT-j12-37, INT-j12-38, INT-j12-39, INT-j12-40, INT-j12-41, INT-j12-42, INT-j12-43, INT-j12-44, INT-j12-45, INT-j12-46, INT-j12-47, INT-j12-48, INT-j12-49, INT-j12-50, INT-j12-51, INT-j12-52, INT-j12-53, INT-j12-54, INT-j12-55, INT-j12-56, INT-j12-57, INT-j12-59 |

### T3 — conditional Class 1 rows, with the condition as the plan carries it

| package | row | condition |
| --- | --- | --- |
| P1 | A-4b | 1 (latent) |
| P1 | INT-i12-2 | reproduced |
| P1 | INT-i12-5 | code order |
| P1 | INT-i12-6 | conditional (a legacy-prompt response returns the escape token); mechanics reproduced |
| P1 | INT-i12-10 | conditional (run_cloud_extraction.py --db outside data/<review>/; cloud runs at all, R71) |
| P1 | INT-i12-11 | conditional (a Modelfile sets num_ctx below the trained context and the request sends none) |
| P1 | INT-i12-69 | 1 (conditional — the read-out lists it "1 conditional / P1"; entered at Class 1, and the owning package's Phase A establishes the trigger and may move the class, R587. Condition, as the read-out states it: the paper's "text file later goes missing") |
| P1 | INT-i12-71 | 1 (latent — the read-out lists it "1 latent / P1 (R71)"; entered at Class 1, and the owning package's Phase A establishes the trigger and may move the class, R587. Condition, as the read-out gives it: R71, under which no cloud extraction runs) |
| P1 | INT-i12-73 | 1 (conditional — the read-out lists it "1 conditional / P1"; entered at Class 1, and the owning package's Phase A establishes the trigger and may move the class, R587. The read-out states no trigger beyond its sentence) |
| P1 | C34 | conditional by its note(s): W1 row 14 |
| P1 | B9 | conditional by its note(s): W2 row 25 |
| P1 | C24 | conditional by its note(s): W2 row 28 |
| P1 | C28 | conditional by its note(s): W2 row 25, W2 row 27 |
| P1 | INT-j12-17 | conditional (override edited without restart, or applied by the run's own restart; or `OLLAMA_HOST` elsewhere) — cell: 1 (conditional / latent / weak) / P1 (the reader: the lead may lower it to 2) |
| P1 | INT-j12-18 | reproduced — cell: 1 (conditional / latent / weak) / P1 (cloud; not reachable on live while R71 keeps `enabled_arms` empty) |
| P1 | INT-j12-19 | reproduced — cell: 1 (conditional / latent / weak) / P1 (cloud; not reachable on live while R71 keeps `enabled_arms` empty) (weak: pinned as intended by a test; could be read as Class 3) |
| P2 | A-2 | 1 (latent) |
| P2 | INT-i12-19 | code order (no production caller: grep finds the definition only) |
| P2 | INT-i12-24 | reproduced |
| P3 | B-F08 | 1 (latent) |
| P3 | INT-i12-21 | code order (the difference); conditional (its effect — OpenAlex's default operator) |
| P3 | INT-i12-72 | 1 (conditional — the read-out lists it "1 conditional / P3, with B-F06"; entered at Class 1, and the owning package's Phase A establishes the trigger and may move the class, R587. The read-out states no trigger beyond its sentence; the code is "inside the excluded `_stage_search`") |
| P3 | INT-j12-16 | code order (the drops); conditional (the trigger) |
| P4 | INT-i12-18 | conditional (an indirect /DescendantFonts array, or any name-join miss); no such PDF was opened". **Consequence unestablished** (R587): the lead confirmed the regex and nothing else — "the consequence depends on how PyMuPDF names fonts in a text trace, which no one established |
| P4 | INT-i12-22 | reproduced |
| P4 | D20 | conditional by its note(s): W1 row 31 |
| P4 | INT-j12-13 | reproduced; conditional (a workbook with no `PI_decision` header) |
| P4 | INT-j12-14 | conditional (same paper id on a later page for the same review name, same browser profile); code order |
| P4 | INT-j12-15 | code order; conditional (the quality-check CLI is run) |
| P5 | INT-i12-27 | reproduced on a writer-built history; conditional for live |
| P5 | INT-i12-77 | 1 (conditional — R596(c): "The row stays Class 1, conditional, P5 with B18. Trigger: any engine path, in any review, that joins a key from an external or human-produced file to a paper through an identifier other than the one the file was keyed with.") |
| P5 | C12 | conditional by its note(s): W2 adjacent item 5 |
| P5 | G3 | conditional by its note(s): W2 row 32 |
| P5 | INT-j12-20 | reproduced, on a narrow input — cell: 1 (conditional / latent / weak) / P5 (the reader: the lead may weigh it as 3) |
| P6 | C10 | conditional by its note(s): W2 row 29 |

### T4 — rows that name more than one package

| first-named package | row | every package named |
| --- | --- | --- |
| P1 | B-F14 | P1, P7 |
| P1 | A-1 | P1, P2 |
| P1 | A-7 | P1, P7 |
| P1 | A-11 | P1, P7 |
| P1 | INT-g12-1 | P1, P7 |
| P1 | INT-i12-3 | P1, P7 |
| P1 | INT-i12-12 | P1, P6 |
| P1 | INT-i12-38 | P1, P7 |
| P1 | INT-i12-45 | P1, P7 |
| P1 | C56 | P1, P2, P4, P7 |
| P1 | E-TEST | P1, P7 |
| P2 | B-F01 | P2, P4 |
| P2 | A-2 | P2, P6 |
| P2 | INT-i12-50 | P2, P7 |
| P3 | B-F06 | P3, P6 |
| P4 | B-F13 | P4, P7 |
| P4 | INT-j12-3 | P4, P7 |
| P4 | INT-j12-12 | P4, P7 |
| P4 | INT-j12-23 | P4, P5 |
| P4 | INT-j12-24 | P4, P7 |
| P5 | INT-j12-34 | P5, P7 |
| P7 | A-9 | P1, P7 |
| P7 | INT-g12-3 | P1, P7 |
| P7 | INT-g12-5 | P3, P7 |
| P7 | INT-i12-15 | P5, P7 |
| P7 | INT-i12-41 | P1, P7 |
| P7 | INT-j12-42 | P2, P7 |

### T5 — rows whose class a Phase A may move (R587, R599, R600)

| package | rows |
| --- | --- |
| P1 | A-4b, A-10, A-11, INT-i12-2, INT-i12-5, INT-i12-6, INT-i12-10, INT-i12-11, INT-i12-69, INT-i12-71, INT-i12-73, C56, C34, B9, C24, C28, INT-j12-17, INT-j12-18, INT-j12-19 |
| P2 | B-F02, A-2, INT-i12-19, INT-i12-24, INT-j12-11 |
| P3 | B-F08, INT-i12-21, INT-i12-72, INT-j12-16 |
| P4 | B-F10, INT-i12-18, INT-i12-22, D20, INT-j12-4, INT-j12-6, INT-j12-10, INT-j12-12, INT-j12-13, INT-j12-14, INT-j12-15 |
| P5 | INT-i12-27, INT-i12-77, C12, C37, G3, INT-j12-20, INT-j12-34 |
| P6 | C10 |
| P7 |  |

### T6 — fix units of more than one row, with the basis

| lead row (package, class) | rows | packages of the members | basis |
| --- | --- | --- | --- |
| C56 (P1, 1) | A-7, C56 | P1 | "with A-7" in the package cell / coverage line / 12g note of C56 // "with C56" in the package cell / coverage line / 12g note of A-7 |
| A-10 (P1, 1) | A-10, INT-g6-1, E-TEST | P1 | "with INT-g6-1" in the package cell / coverage line / 12g note of A-10 // R543: E-TEST lands with or immediately after INT-g6-1 |
| INT-i12-4 (P1, 1) | INT-h12-2, INT-i12-4, INT-i12-38, C34 | P1 | R589: lands in the INT-h12-2 batch; one comparison rehearsal covers them together |
| B9 (P1, 1) | B9, C28 | P1 | one finding on several rows (W2 row 25; R597(c)); every carrier is on the list by notes only |
| B-F17 (P1, 2) | B-F17, A-1, A-1N, INT-g12-7 | P1 | "with A-1" in the package cell / coverage line / 12g note of A-1N // "with A-1" in the package cell / coverage line / 12g note of B-F17 // "with A-1" in the package cell / coverage line / 12g note of INT-g12-7 // "with A-1N" in the package cell / coverage line / 12g note of A-1 // "with A-1N" in the package cell / coverage line / 12g note of B-F17 // "with B-F17" in the package cell / coverage line / 12g note of A-1 // "with B-F17" in the package cell / coverage line / 12g note of A-1N |
| B-F02 (P2, 1) | B-F02, INT-i12-74 | P2 | "with B-F02" in the package cell / coverage line / 12g note of INT-i12-74 |
| B-F06 (P3, 1) | B-F06, INT-i12-72 | P3 | "with B-F06" in the package cell / coverage line / 12g note of INT-i12-72 |
| B-F07 (P3, 1) | B-F07, E-METHODS | P3 | "with E-METHODS" in the package cell / coverage line / 12g note of B-F07 |
| B-F08 (P3, 1) | B-F08, INT-g2-1 | P3 | "with B-F08" in the package cell / coverage line / 12g note of INT-g2-1 // "with INT-g2-1" in the package cell / coverage line / 12g note of B-F08 |
| A2 (P4, 1) | A2, C42 | P4 | one finding on several rows (W2 row 5; R597(c)); every carrier is on the list by notes only |
| B-F15 (P5, 1) | B-F15, INT-g3-1 | P5 | "with B-F15" in the package cell / coverage line / 12g note of INT-g3-1 // "with INT-g3-1" in the package cell / coverage line / 12g note of B-F15 |
| INT-i12-77 (P5, 1) | INT-i12-77, B18 | P5 | "with B18" in the package cell / coverage line / 12g note of INT-i12-77 |
| C12 (P5, 1) | C12, C37, C47 | P5 | one finding on several rows (W2 adjacent item 5; R597(c)); every carrier is on the list by notes only // one finding on several rows (W2 row 15; R597(c)); every carrier is on the list by notes only |
| INT-j12-1 (P5, 1) | INT-j12-1, INT-j12-2, INT-j12-58 | P5 | "with INT-j12-1" in the package cell / coverage line / 12g note of INT-j12-58 // "with INT-j12-2" in the package cell / coverage line / 12g note of INT-j12-58 |
| D22 (P6, 1) | D22, D23 | P6 | "with D22" in the package cell / coverage line / 12g note of D23 |
| INT-g12-8 (P7, 3) | INT-g12-8, C48 | P7 | "with C48" in the package cell / coverage line / 12g note of INT-g12-8 |

### T7 — one finding on several rows, not merged (a carrier has content of its own)

| finding | rows |
| --- | --- |
| W1 row 25 | B-F01, C56 |
| W1 row 34 | B-F06, B-F07 |
| W2 row 53 | B-F06, B-F07 |
| W1 row 31 | B-F10, D20 |
| W1 row 86 | A-6, A14 |

### T7b — rides across packages, not merged (a part of one row lands with a row of another package)

| row | its package | rides with | that row's package | basis |
| --- | --- | --- | --- | --- |
| A-1 | P1 | INT-i12-68 | P2 | "(INT-i12-68) goes with it" in a dated note on A-1 |
| INT-g12-5 | P7 | B-F07 | P3 | "with B-F07" in the package cell / coverage line / 12g note of INT-g12-5 |
| INT-i12-41 | P7 | INT-h12-2 | P1 | R589: lands in the INT-h12-2 batch; one comparison rehearsal covers them together |
| B12 | P7 | A16 | P2 | "with A16" in the package cell / coverage line / 12g note of B12 |

### T8 — pin effect as the plan states it

| statement | rows | by package |
| --- | --- | --- |
| pin-effect unknown (the row states nothing) | 70 | P1: 16 rows; P2: B-F01, B-F02, B-F03, B-F16, A-2, INT-g6-2, A16, C50; P3: B-F04, B-F05, B-F06, B-F07, B-F08, B-F12, INT-g2-1, INT-g12-9, INT-g12-10, E-METHODS; P4: B-F10, B-F11, B-F13; P5: B-F09, B-F15, INT-g3-1, INT-h12-1, B18; P6: C39, D14, D22, D23, D25, D26(b), E-CALL; P7: 21 rows |
| pin-affecting (stated) | 5 | P1: INT-g6-1, INT-i12-4, INT-i12-38, C34; P7: INT-i12-41 |
| not pin-affecting (stated) | 5 | P1: INT-g12-1, INT-i12-1, INT-i12-3, INT-i12-7, INT-i12-8 |
| pin-affecting in part (stated) | 1 | P1: INT-h12-2 |
| not among the rows Wave 1 names pin-affecting (12i_review_w1.md; R589) — no statement on the row | 87 | P1: INT-i12-2, INT-i12-5, INT-i12-6, INT-i12-9, INT-i12-10, INT-i12-11, INT-i12-12, INT-i12-31, INT-i12-45, INT-i12-69, INT-i12-71, INT-i12-73; P2: INT-i12-13, INT-i12-14, INT-i12-16, INT-i12-19, INT-i12-23, INT-i12-24, INT-i12-50, INT-i12-51, INT-i12-68, INT-i12-74; P3: INT-i12-21, INT-i12-72; P4: INT-i12-17, INT-i12-18, INT-i12-20, INT-i12-22, INT-i12-30, INT-i12-32, INT-i12-33, INT-i12-35, INT-i12-36, D20, J8; P5: INT-i12-26, INT-i12-27, INT-i12-28, INT-i12-29, INT-i12-70, INT-i12-77, C45; P6: INT-i12-25, INT-i12-34; P7: 43 rows |
| not pin-affecting by the readers' reading (R599 block; 12j_review_w2.md) | 70 | P1: B9, C24, C28, INT-j12-17, INT-j12-18, INT-j12-19, INT-j12-31, INT-j12-32, INT-j12-33; P2: INT-j12-11, INT-j12-21; P3: INT-j12-16, INT-j12-28; P4: 20 rows; P5: B2, C12, C37, C47, G3, INT-j12-1, INT-j12-2, INT-j12-20, INT-j12-26, INT-j12-34, INT-j12-58; P6: C10; P7: 25 rows |

### T9 — rows naming P6 or a migration

| row | first-named package | packages named | the plan's words |
| --- | --- | --- | --- |
| B-F06 | P3 | P3, P6 | P3 (the minimum fix); P6 (its search ledger) // P3 (the minimum fix); P6 (its search ledger) — R515 (R512) |
| A-2 | P2 | P2, P6 | P2; its assertion in P6 (023) // P2; its assertion in P6 (023) — R541 |
| INT-i12-12 | P1 | P1, P6 | P1 (the read-out's cell: "1 / P1 (or P6 with D22's 023 build)") |
| INT-i12-25 | P6 | P6 | P6 // P6 — R587 |
| INT-i12-34 | P6 | P6 | P6 // P6 — R587 |
| C39 | P6 |  | the migration session, 023 (R345, R363) |
| D14 | P6 |  | the migration session, 024 after measuring the count (R345, R363) |
| D22 | P6 |  | 12d (ruled C3, `audit_not_possible`, R431), then the migration session, 023 (R423, R427, R442) |
| D23 | P6 |  | 12d (code closed `485e7b9edbdcd3400bc51c2417a856a2629bbb2a` + `c44ff732150db2b595b9a9025d28df6757a8a641`, R432, R433, R436, R440); the refusal writer with D22 in 023 (R435) |
| D25 | P6 |  | the migration session, 023 (R434, R442) |
| D26(b) | P6 |  | (a) 12d (closed `0d19bb422ff3b34d9953d8a07cb038a371a77cd1`, R441, R444); (b) the migration session, 023 (R442) |
| E-CALL | P6 |  | the migration session, with 023 (R362, R363) |
| C10 | P6 | P6 | *Note 2026-10-11 (12k item 0c, R599):* Wave 2 row 29 (L12-C5) extends this row — Class 1, P6; conditional |

### T10 — R601's rows (loud-failure minimum pre-tag)

| row | FT page or FT importer (by the files its Evidence cell names) | where |
| --- | --- | --- |
| INT-i12-77 | yes | `12i_ee_locate.md` (script and output `12i_ee/ee_locate.py`, `12i_ee/ee_locate.json`) *Note 2026-10-11 (12k item 0c, R599; R601):* Wave 2 row 19 (L09a-C3 + L09b-C2 + L13-C1) extends this row — Class 1, P5; conditional — the read-out marks it "conditional / lat |
| INT-j12-3 | no | `12j_review_w2.md` row 6 (L09b-C1 + L09a-C8; `abstract_adjudication_html.py`, `_query_abstract_flagged` // `screening_adjudicator.py::_collect_db_flagged` |
| INT-j12-4 | no | `12j_review_w2.md` row 7 (L09b-C4; `abstract_adjudication_html.py`, page script `setDecision`, `applyFilter`, the `keydown` listener |
| INT-j12-5 | no | `12j_review_w2.md` row 8 (L09b-C5; `abstract_adjudication_html.py`, `generate_abstract_adjudication_html` (category assignment, batch buttons) and page script `batchExclude`, `exportJSON` |
| INT-j12-6 | yes | `12j_review_w2.md` row 9 (L09b-C6; `abstract_adjudication_html.py` `generate_abstract_adjudication_html`; `ft_adjudication_html.py` `_query_ft_flagged` |
| INT-j12-10 | no | `12j_review_w2.md` row 13 (L10-C4; `engine/acquisition/pdf_quality_check.py::run_quality_check`; `engine/acquisition/pdf_quality_html.py::_query_section1/2/3`, `generate_quality_html` |
| INT-j12-13 | yes | `12j_review_w2.md` row 18 (L09a-C1; `screening_adjudicator.py::_import_abstract_xlsx` and `ft_screening_adjudicator.py::_import_ft_xlsx` — the nested `_find_col`, identical in both. |
| INT-j12-14 | yes | `12j_review_w2.md` row 20 (L09b-C3; both files, page script `STORAGE_KEY`, `restoreState` |
| INT-j12-23 | yes | `12j_review_w2.md` row 36 (L09a-C5 + L13-C4; `_import_ft_xlsx`, `_import_abstract_xlsx` // `engine/exporters/review_workbook.py` · `InstructionsConfig` (defaults `notes_on_rejection`, `pre_import_validation`), written by `_build_instructions_sheet` |
| INT-j12-24 | yes | `12j_review_w2.md` row 37 (L09b-C8 + L09a-C7; both files, `generate_*_adjudication_html` // `screening_adjudicator.py::export_adjudication_queue`, `ft_screening_adjudicator.py::export_ft_adjudication_queue` |
| INT-j12-25 | yes | `12j_review_w2.md` row 38 (L09b-C9; both files, page script `restoreState`, `setDecision`, `saveState` |
| INT-j12-26 | no | `12j_review_w2.md` row 39 (L10-C7; `engine/acquisition/pdf_quality_html.py::generate_acquisition_html`; `pdf_quality_import.VALID_DISPOSITIONS` |

### T11 — P7 by first-named file or module

| file or module | rows | IDs |
| --- | --- | --- |
| (no file or module named in the row) | 33 | A-4a, A-8, A-9, HK-ROOTDB, HK-12C, HK-ARCH, HK-WITHMODEL, R501, INT-g12-2, INT-g12-3, INT-g12-5, INT-g12-8, INT-i12-46, INT-i12-62, INT-i12-67, INT-i12-75, INT-i12-76, INT-i12-85, INT-i12-86, INT-i12-87, INT-i12-88, INT-i12-89, C38, C48, I21, INT-j12-38, INT-j12-44, INT-j12-45, INT-j12-51, INT-j12-52, INT-j12-53, INT-j12-55, INT-j12-59 |
| ft_screening_adjudicator | 3 | INT-j12-37, INT-j12-40, INT-j12-42 |
| ollama_preflight | 3 | INT-j12-46, INT-j12-48, INT-j12-49 |
| pdf_parser | 3 | INT-i12-56, INT-i12-58, INT-i12-59 |
| CLAUDE.md | 2 | A-3, INT-i12-15 |
| effective | 2 | INT-i12-54, INT-i12-55 |
| events | 2 | INT-i12-81, INT-i12-84 |
| extraction_events | 2 | INT-i12-37, INT-i12-78 |
| extractor | 2 | INT-i12-60, A14 |
| import_extraction_entry | 2 | INT-j12-35, INT-j12-36 |
| ollama_client | 2 | INT-i12-65, INT-i12-66 |
| paper_state | 2 | A-6, INT-i12-80 |
| run_manifest | 2 | INT-i12-47, INT-i12-49 |
| run_pipeline | 2 | INT-i12-63, INT-i12-64 |
| 017_seed_event_store | 1 | C13 |
| advance_to_pdf_acquired | 1 | HK-RETIRE |
| audit_events | 1 | INT-i12-43 |
| background | 1 | INT-j12-47 |
| categorizer | 1 | INT-j12-41 |
| completeness | 1 | INT-i12-83 |
| contracts | 1 | INT-i12-39 |
| distribution_monitor | 1 | INT-i12-44 |
| effective_config | 1 | INT-i12-48 |
| extraction_telemetry | 1 | INT-i12-82 |
| font_audit | 1 | INT-i12-57 |
| ft_adjudication_html | 1 | INT-j12-43 |
| metrics | 1 | INT-j12-56 |
| models | 1 | INT-i12-42 |
| pipeline | 1 | INT-i12-41 |
| progress | 1 | INT-j12-50 |
| prompts | 1 | INT-i12-40 |
| report | 1 | INT-j12-57 |
| review_paths | 1 | INT-i12-53 |
| review_spec | 1 | INT-i12-52 |
| review_workbook | 1 | INT-j12-54 |
| runner | 1 | INT-i12-61 |
| screening_adjudicator | 1 | INT-j12-39 |
| selection | 1 | INT-i12-79 |
| test_cloud_extraction | 1 | B12 |
| test_ollama_client | 1 | I20 |
| units | 1 | A-5 |

### T12 — Class 3 rows riding outside P7 (R543)

| package | rows |
| --- | --- |
| P1 | INT-g12-6, INT-g12-7, INT-i12-38, E-TEST |
| P2 | INT-i12-51 |
| P3 | INT-g12-9, INT-g12-10 |
| P4 |  |
| P5 |  |
| P6 |  |

### T13 — P7 rows that name a pin-affecting file

| row | file(s) | pin statement on the row |
| --- | --- | --- |
| A-5 | contracts, units | pin-effect unknown (the row states nothing) |
| INT-i12-39 | contracts | not among the rows Wave 1 names pin-affecting (12i_review_w1.md; R589) — no statement on the row |
| INT-i12-40 | prompts | not among the rows Wave 1 names pin-affecting (12i_review_w1.md; R589) — no statement on the row |
| INT-i12-41 | pipeline, prompts | pin-affecting (stated) |
| INT-i12-47 | run_manifest | not among the rows Wave 1 names pin-affecting (12i_review_w1.md; R589) — no statement on the row |
| INT-i12-48 | effective_config | not among the rows Wave 1 names pin-affecting (12i_review_w1.md; R589) — no statement on the row |
| INT-i12-49 | run_manifest | not among the rows Wave 1 names pin-affecting (12i_review_w1.md; R589) — no statement on the row |
| INT-i12-52 | review_spec | not among the rows Wave 1 names pin-affecting (12i_review_w1.md; R589) — no statement on the row |
| INT-i12-60 | extractor | not among the rows Wave 1 names pin-affecting (12i_review_w1.md; R589) — no statement on the row |
| A14 | extractor | not among the rows Wave 1 names pin-affecting (12i_review_w1.md; R589) — no statement on the row |

### T14 — Phase A items by ruling

| ruling | packages named | text |
| --- | --- | --- |
| R546 | P4 | P4's Phase A measures which applies. |
| R547 | P3 | P3's Phase A first recovers the March query text read-only (review spec, PROSPERO record, git history, logs); the call is decided after, knowing that today's count bounds March's in one direction only (index growth). |
| R549 | P4 | **12g-MEAS.** Open item for P4's Phase A (not ruled now): whether the 165 eligible papers screened on cut text are also re-screened before Run 7. |
| R562 |  | **12g-RA-B.** Plan: the internal review moves to session 12h. 12g continues with the front-half rehearsal (Phase A now, run after ruling), then closes. |
| R567 | P5 | **12h-RD.** ADJ-RD3-1 enters section L as a new row (ID per the INT convention for this session), Class 1, P5, Phase A first. |
| R567 |  | The Phase A also states why the exporter and judge-loader tests passed with the snippet empty (B5). *Transcribed:* row INT-h12-1. |
| R568 | P1 | The form is set by P1's Phase A. |
| R569 |  | Phase A first. |
| R579 | P6 | L15 (moves to P6's Phase A, which reads the schema and the applied migrations before 023/024), L16, L17, L18. |
| R587 |  | Rows marked conditional / latent / weak enter at Class 1 with their condition stated; the owning package's Phase A establishes the trigger and may move the class. |
| R588 | P1 | Class 2, P1, Phase A first (A4) — an audit model of the same family as the arm's extractor is refused or recorded at run open (the settled cross-family rule). |
| R591 | P4 | (b) Paper 168 and the other 14 empty-abstract [specialty_rescreen] exclusions join R549's open item for P4's Phase A: whether each goes to full-text screening or to human FT adjudication before Run 7. |
| R592 | P6 | P6's Phase A states the conditions under which the migration drift refusal holds. |
| R595 |  | - Its Phase A states which numbering each artifact given to a human extractor carries (PDFs, workbooks, manifest), and checks filled workbook rows against both candidate papers. |
| R596 |  | (b) R595's Phase A drops the check of filled workbook rows against two candidate papers. |
| R596 |  | Its Phase A lists every engine path that reads a paper key from such a file or writes one into an artifact for people, and which identifier each uses. |
| R598 | P4 | (c) B-F11 gets a dated note recording R591(b): paper 168 and the other 14 empty-abstract [specialty_rescreen] exclusions join R549's open item for P4's Phase A; none is moved to the corpus by the engine. |
| R599 |  | Conditional / latent / weak rows enter at Class 1 with their condition stated; the owning package's Phase A may move the class. |
| R600 | P2 | - row 14: a note on INT-g6-2, package P2; its key-alone join is on INT-i12-77's Phase A list. |
| R600 | P4 | P4's Phase A executes the page script, or states it cannot, before any fix. |
| R600 |  | - every other "may weigh" alternative (rows 7, 9, 13, 16, 17(a), 23, 30, 31, 49) stands as the lead entered it; the owning package's Phase A may move the class. |
| R602 |  | - 12 → note on INT-i12-77 (no writer of papers.ee_identifier), an input to its Phase A. |
| R603 | P5 | P5's Phase A confirms whether I5 and A12 are fixed at HEAD, as L14 read them. |

### T15 — rows whose package cell names a Phase A

| package | row | text |
| --- | --- | --- |
| P1 | INT-g12-4 | Phase A first. |
| P1 | INT-i12-45 | P1, Phase A first (A4) — "an audit model of the same family as the arm's extractor is refused or recorded at run open (the settled cross-family rule)" (R588; // ruling assumption A4: the Phase A establishes whether a model-family attribute exists to compare, and if none exists the fix includes declaring one; |
| P1 | C56 | || 12f, Phase A first (R479, R483, R500, R503) |
| P1 | C34 | entered at Class 1, the owning package's Phase A establishes the trigger and may move the class), P1 |
| P1 | INT-j12-17 | the owning package's Phase A may move the class." |
| P1 | INT-j12-19 | the owning package's Phase A may move the class." |
| P2 | INT-j12-11 | the owning package's Phase A may move the class." |
| P4 | D20 | entered at Class 1, the owning package's Phase A establishes the trigger and may move the class), P4 |
| P4 | INT-j12-4 | the owning package's Phase A may move the class." R600: "rows 7, 20 and 38: P4's Phase A executes the page script, or states it cannot, before any fix." Pre-tag: closes on its minimum loud-failure fix (R601); |
| P4 | INT-j12-6 | the owning package's Phase A may move the class." Pre-tag: closes on its minimum loud-failure fix (R601); |
| P4 | INT-j12-10 | the owning package's Phase A may move the class." Pre-tag: closes on its minimum loud-failure fix (R601); |
| P4 | INT-j12-12 | the owning package's Phase A may move the class." |
| P4 | INT-j12-14 | P4 (the read-out's cell: "1 (conditional / latent / weak) / P4") R600: "rows 7, 20 and 38: P4's Phase A executes the page script, or states it cannot, before any fix." Pre-tag: closes on its minimum loud-failure fix (R60 |
| P4 | INT-j12-25 | P4 R600: "rows 7, 20 and 38: P4's Phase A executes the page script, or states it cannot, before any fix." Pre-tag: closes on its minimum loud-failure fix (R601); |
| P5 | INT-h12-1 | P5, Phase A first; // the Phase A also states why the exporter and judge-loader tests passed with the snippet empty (B5) // P5, Phase A first — R567 |
| P5 | INT-i12-77 | Phase A, as R596(c) states it: "Its Phase A lists every engine path that reads a paper key from such a file or writes one into an artifact for people, and which identifier each uses. // It may lower the class if no path can mis-join." R595's Phase A and fix stand as amended by R596(b) and R596(d); |
| P5 | INT-j12-20 | the owning package's Phase A may move the class." |
| P5 | INT-j12-34 | the owning package's Phase A may move the class." |

### T16 — fix-bearing sessions, measured

| session | commits | touching code or tests | rows closed | rulings issued | opened (plan) | closeout commit (git) | wall h | h per row |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 12a | 9 | 7 | 5 (C40, C47, E-DEADKI, E-EXEC, E-STATE) | 39 (R332–R370) | 2026-10-01T00:08 | 2026-10-01T05:55 | 5.79 | 1.16 |
| 12c | 10 | 8 | 7 (E-PIN, C41, D21, E-PASS2, E-SENTINEL, C52, C53) | 58 (R371–R428) | 2026-10-01T12:18 | 2026-10-01T21:16 | 8.98 | 1.28 |
| 12d | 7 | 6 | 4 (D23 (code), D24, D26 (a), B23) | 35 (R429–R463) | 2026-10-06T03:46 | 2026-10-06T18:43 | 14.95 | 3.74 |
| 12e | 6 | 4 | 4 (C54, C55, C57, C58) | 43 (R464–R506) | 2026-10-06T19:28 | 2026-10-07T00:11 | 4.72 | 1.18 |

### T17 — per-package range in sessions

| package | Class 1 and 2 fix units | sessions | label and basis |
| --- | --- | --- | --- |
| P1 | 36 | 6–9 | INFERRED — Class 1 and 2 fix units ÷ rows closed per fix-bearing session, 12a–12e (min 4, max 7) |
| P2 | 18 | 3–5 | INFERRED — Class 1 and 2 fix units ÷ rows closed per fix-bearing session, 12a–12e (min 4, max 7) |
| P3 | 9 | 2–3 | INFERRED — Class 1 and 2 fix units ÷ rows closed per fix-bearing session, 12a–12e (min 4, max 7) |
| P4 | 33 | 5–9 | INFERRED — Class 1 and 2 fix units ÷ rows closed per fix-bearing session, 12a–12e (min 4, max 7) |
| P5 | 17 | 3–5 | INFERRED — Class 1 and 2 fix units ÷ rows closed per fix-bearing session, 12a–12e (min 4, max 7) |
| P6 | 9 | no measured basis | see the text |
| P7 | 1 | no measured basis | see the text |

