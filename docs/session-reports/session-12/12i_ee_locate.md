# 12i — the two "EE-NNN" numberings (12i-EE-LOCATE)

**Status: COMPLETE.** Read-only; no model call; live opened `mode=ro` only. Facts only. No
inventory row was opened here; R595 rules a new row, to be opened at 12j item 0c.

| | |
| --- | --- |
| Reference HEAD | `ba633018b7afdc2dc1c10b24e6ac0393622c5116` |
| Question | whether the observation in `12i_review_w1.md` ("the human-workbook key 'EE-014' names a different paper from live paper 23's `ee_identifier`") is owned by an existing row or ruling, and how wide it is |
| Script and output | `12i_ee/ee_locate.py`, `12i_ee/ee_locate.json` (`PYTHONPATH=. .venv/bin/python docs/session-reports/session-12/12i_ee/ee_locate.py data/surgical_autonomy`) |

## Result in short

"EE-NNN" means two different things on this box, and the two numberings never coincide. One is
`EE-{papers.id}`; the other is the stored column `papers.ee_identifier`. Nothing is mis-joined
today — `human_extractions` does not exist on live and the importer has never run. The hazard is
for whoever builds the reconciliation, or reads a PDF named `EE-014_…` beside a workbook row
`EE-014`: the same string points at two papers.

## 1. The owning ruling or row

**R14** owns EE-key reconciliation by name. It is a ruling, not an inventory row:

> "R14 — Human extractor workbooks are loaded as arms in session 12 alongside the importer.
> `human_extractions` is created by a numbered migration with a receipt; the self-provisioning
> `CREATE TABLE IF NOT EXISTS` is retired; the TEXT "EE-NNN" key is reconciled to `papers.id` in
> that session's design" (PI, 2026-09-21)

Status: not built. R537 narrowed the placement (B18 stays pre-tag in P5; B19's design "before the
four human workbooks load"). No inventory row names the two-numbering collision: B18 is the
importer erasing `NR`; B19 is human claims lacking input identity; the plan does not contain the
string `ee_identifier`. R14's text assumes one "EE-NNN" key to reconcile and does not say a
second exists.

## 2. Sources compared

- **Human side:** `data/surgical_autonomy/Extraction_Workbook_v2_A.xlsx`, sheet "Extraction
  Form", column 1 "Paper ID" with column 4 "Title" (70 rows). The only human workbook on disk in
  this repository; B, C and D are not here.
- **Live:** `papers.ee_identifier`, populated on 648 of 10,039 papers.
- **What the importer validates against:** `analysis/paper1/human_import.py` builds its accepted
  set with `SELECT printf('EE-%03d', id) FROM papers`.
- **Also checked:** `data/surgical_autonomy/concordance_pdfs/paper_manifest.csv` (`ee_id`,
  `db_id`) and the EE number in `papers.pdf_local_path` file names.

By reading the code (grep, not a script): `EE-{papers.id}` is built in
`analysis/paper1/human_import.py`, `analysis/paper1/adjudication.py` and
`engine/utils/progress.py` (the extraction progress line). The string `ee_identifier` occurs in
14 files under `engine/`, `scripts/` and `analysis/` (`grep -rl`): `engine/core/database.py`,
`engine/acquisition/` (`manual_list.py`, `pdf_quality_check.py`, `pdf_quality_html.py`,
`verify_downloads.py`), `engine/adjudication/ft_screening_adjudicator.py`,
`engine/migrations/007_add_judge_tables.py`, `analysis/paper1/pi_audit_sampler.py` and
`pi_audit_sampler_v2.py`, `analysis/eval/score_screen2f.py` and `screen2f_export.py`, and three
scripts (`backfill_authors.py`, `parse_expanded_corpus.py`, `rescreen_with_specialty.py`). What
each does with it was not read.

## 3. Counts (`ee_locate.py`)

| comparison | result |
| --- | --- |
| Workbook rows resolved to one live paper by title | 70 of 70 (all `AI_AUDIT_COMPLETE`) |
| Workbook key equals `EE-{papers.id}` of that paper | 70 of 70 |
| Workbook key equals that paper's `ee_identifier` | 0 of 70 |
| Workbook key is also the `ee_identifier` of a different paper | 58 of 70 |
| Workbook key is no paper's `ee_identifier` | 12 of 70 (EE-659, EE-670, EE-683, EE-690, EE-700, EE-708, EE-750, EE-764, EE-769, EE-783, EE-796, EE-801) |
| Live papers where `ee_identifier` equals `EE-{id}` | 0 of 648 |
| `ee_identifier` values that also name another paper as `EE-{id}` | 648 of 648 |
| Manifest `ee_id` equals live `ee_identifier` of its `db_id` | 95 of 96 (the exception: EE-095, `db_id` 229, live `ee_identifier` NULL) |
| PDF file-name EE number equals `ee_identifier` | 381 of 381 |

**Correction to `12i_review_w1.md`'s sentence**, which was imprecise: live paper 23's
`ee_identifier` is EE-014; the workbook's EE-014 is live paper 14, whose `ee_identifier` is EE-007.

**The 58 keys that name two papers** (the full rows, with titles, are in `ee_locate.json`):

| key | human-side paper (`papers.id`) | live paper holding that `ee_identifier` |
| --- | ---: | ---: |
| EE-011 | 11 | 20 |
| EE-012 | 12 | 21 |
| EE-014 | 14 | 23 |
| EE-017 | 17 | 30 |
| EE-024 | 24 | 47 |
| EE-067 | 67 | 167 |
| EE-081 | 81 | 195 |
| EE-102 | 102 | 254 |
| EE-121 | 121 | 273 |
| EE-132 | 132 | 284 |
| EE-262 | 262 | 414 |
| EE-268 | 268 | 420 |
| EE-281 | 281 | 433 |
| EE-286 | 286 | 438 |
| EE-292 | 292 | 444 |
| EE-347 | 347 | 499 |
| EE-370 | 370 | 522 |
| EE-376 | 376 | 528 |
| EE-388 | 388 | 540 |
| EE-395 | 395 | 547 |
| EE-402 | 402 | 554 |
| EE-406 | 406 | 558 |
| EE-409 | 409 | 561 |
| EE-411 | 411 | 563 |
| EE-432 | 432 | 584 |
| EE-439 | 439 | 591 |
| EE-445 | 445 | 597 |
| EE-449 | 449 | 601 |
| EE-455 | 455 | 607 |
| EE-463 | 463 | 615 |
| EE-464 | 464 | 616 |
| EE-467 | 467 | 619 |
| EE-472 | 472 | 624 |
| EE-476 | 476 | 628 |
| EE-478 | 478 | 630 |
| EE-485 | 485 | 637 |
| EE-487 | 487 | 639 |
| EE-488 | 488 | 640 |
| EE-492 | 492 | 644 |
| EE-497 | 497 | 649 |
| EE-504 | 504 | 656 |
| EE-507 | 507 | 659 |
| EE-515 | 515 | 667 |
| EE-516 | 516 | 668 |
| EE-517 | 517 | 669 |
| EE-528 | 528 | 680 |
| EE-534 | 534 | 686 |
| EE-536 | 536 | 688 |
| EE-543 | 543 | 695 |
| EE-550 | 550 | 702 |
| EE-570 | 570 | 722 |
| EE-590 | 590 | 742 |
| EE-608 | 608 | 760 |
| EE-614 | 614 | 766 |
| EE-623 | 623 | 775 |
| EE-628 | 628 | 780 |
| EE-644 | 644 | 796 |
| EE-645 | 645 | 797 |

## 4. Proposed class and package

R14 owns the reconciliation, so none was proposed beyond a dated note on R14; the alternative
given was a row, Class 1 latent, P5 with B18, since a join on the wrong key would attach a
human's values to another paper silently. **R595 rules the row** (Class 1, P5 with B18; ID
assigned at 12j item 0c) and the dated note on R14.

## 5. The brief's INFERRED items

- **I1 held.** R14 is a real ruling whose text covers EE-key reconciliation to `papers.id`. It
  does not mention the second numbering.
- **I2 held in part.** One workbook (extractor A) is on disk and the importer's rule is in code.
  There is no separate id map or allocation file for the human side in the repository; the
  `id_map.json` files under `12g_ra/` and `12g_rb/` belong to the rehearsals and were not used.

## The architect's check on the PI's copies (2026-10-09) — not a CC measurement

Quoted from ruling R595's assumptions, as issued in the 12i-CLOSE brief. CC has not seen these
four files; nothing above depends on them.

> READ (architect, 2026-10-09, on the PI's copies of the four distributed blank workbooks
> A_Tiffany, B_Sarah, C_Chris, D_Lilit, checked against extraction_corpus_metadata.csv):
> each has 70 rows in "Extraction Form"; every Paper ID is EE-{papers.id}; every row's Title
> matches that paper id's title; no field is filled in any of them. A_Tiffany is the same
> allocation as data/surgical_autonomy/Extraction_Workbook_v2_A.xlsx.
> INFERRED: which numbering the PDFs given to the extractors carried. Unknown; the PI is
> establishing it outside this session. Nothing in this session acts on it.

One fact from §3 bears on that open point and is recorded without interpretation: every
EE-named PDF that live points at (381 files) carries the `ee_identifier` numbering, and so does
the concordance manifest; whether those are the files the extractors were given is not
established here.
