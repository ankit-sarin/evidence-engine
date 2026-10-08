# 12g — R516 and R536 read-only measurements

**Status: COMPLETE.** Read-only. Live `review.db` opened `mode=ro` only; parsed-text and stored
search files read, never written. No engine, test or script change; no model call; no network
call. Scratch output under `~/scratch/12g-meas/`. Ends at a STOP.

| | |
| --- | --- |
| HEAD at the reads | `f51a2d5110569ff4d4c200ffa5ace08a97c68b94` (12g item 0c) |
| Live fingerprint | IDENTICAL to the migration-02 record before the reads and after the push (overall `0d3eedea…cb25`, 36 tables) |
| M1 script | `12g_meas/m1_ft_budget.py` → `12g_meas/m1_summary.json`, `12g_ft_budget_versions.csv`, `12g_ft_budget_decisions.csv` |
| M2 script | `12g_meas/m2_openalex.py` → `12g_meas/m2_summary.json` |

Every count below is a value of one of the two summary files, written here by a generator that
read them; none was tallied by eye.

**Two results depart from what the brief expected.**
- **M1:** the brief expected "some tens" of the 171 full-text exclusions to be over budget. The
  measured figure is **129 of 171**.
- **M2:** none of the three verdict labels fits as defined. The stored OpenAlex count is
  9,419, not exactly 10,000, and nothing on disk shows whether pagination
  ended. The verdict given is **UNDETERMINED**, for a reason the label's definition does not name.

## The inferred items

| | Result | Evidence |
| --- | --- | --- |
| I1 | confirmed, with one difference in the quantity | `engine/core/constants.py`: `FT_MAX_TEXT_CHARS = 32_000  # ~8,000 tokens`. `engine/agents/ft_screener.py::truncate_paper_text(full_text, title="", abstract="", max_chars=FT_MAX_TEXT_CHARS)`: `remaining_budget = max_chars - len(header)` … `if len(full_text) <= remaining_budget: return header + full_text`. Two call sites, `run_ft_screening` and `run_ft_verification`, each `truncate_paper_text(parsed_text, title=paper.get("title", ""), abstract=paper.get("abstract", ""))`. **The budget covers the title and abstract header as well as the text**, so the test is `len(text) > 32,000 − len(header)`, not `len(text) > 32,000`. Both are measured. The function body is identical at `c21ad34`, `de7e6a5`, `8ba20bb` (the three commits spanning the March decisions) and at HEAD |
| I2 | confirmed | all 171 papers have a `full_text_assets.parsed_text_path` whose file exists; none has a `parsed_text_refs` row. Each has exactly **one** parsed-text file on disk (files on disk per paper: {"1": 171}) |
| I3 | confirmed | `ft_screening_decisions` holds 366 rows over 364 papers: D5's 366 is the count of **all** primary full-text decision rows, not of decisions on over-budget text |
| I4 | confirmed for the entry; one qualification | Migration 003 reads `data/surgical_autonomy/expanded_search/` (`abstracts.jsonl`, `screening_results.csv`, `verification_results.csv`, `rescreen_original_251.csv`). Qualification: the script that **wrote** the search files on 2026-03-08 07:54 UTC is not in the repository (no file writes `expanded_search_results.csv` or `stats.json`; `stats.json`'s keys `duplicates_removed`, `overlap_with_existing`, `net_new` are not `dedup.py`'s). Whether it called `engine/search/openalex.py` is not established |
| I5 | answered: **no** | no stored OpenAlex response exists. No file in the directory carries an advertised total, a page count or a cursor (fields searched: [] in `stats.json`; [] among all columns) |

## M1 — R516: full-text decisions made on input longer than the budget

**The mechanism.** The screener loads the paper's parsed text, prepends `Title: …` and
`Abstract: …`, and cuts the text when it does not fit in 32,000 characters less that header.
When it cuts, it cuts at the first line beginning `references`, `bibliography` or
`acknowledgement` if that line falls inside the budget, and otherwise takes the first
`remaining_budget` characters; it then trims to the last sentence end if that is in the final
fifth. The model is not told anything was omitted (12f, B-F11).

**The quantity measured:** `len(text)` of the parsed file decoded as UTF-8 with universal
newlines, against `32,000 − len(header)`, the header built from the paper's current
`papers.title` and `papers.abstract`. R516's wording, `len(text) > 32,000`, is reported beside it.

**The version rule.** In March 2026 the loader read `sorted(glob("{id}_v*.md"), reverse=True)[0]`
(`de7e6a5:engine/agents/ft_screener.py`). The version a decision read is therefore the
reverse-sorted first of the files whose mtime is not later than the decision's `decided_at`. A
paper is undecidable if no file on disk predates the decision or the chosen file's
`full_text_assets.parsed_at` is later than it. The rule was decidable for every paper: each of
the 171 has one file, written 2026-03-01 to 2026-03-14 03:34 UTC, before its decision.

### The 171 FT_SCREENED_OUT papers

| Bucket | Papers |
| --- | --- |
| **Over budget** under the version the decision read | **129** |
| Under budget | 42 |
| Undecidable | 0 |
| File missing | 0 |
| File unreadable | 0 |
| No primary decision row | 0 |
| Total | 171 |

- Over budget under **any** version on disk: 129; under **none**:
  42; no readable version: 0. The same split,
  because each paper has one version.
- By R516's wording (`len(text) > 32,000`): **122**. The
  other 7 are over only once the header is counted: papers
  99, 285, 397, 469, 483, 529, 789.
- How the 129 were cut: 103 by prefix, 26 at a
  `references…` line. 12f found that line test matches any line beginning with the word, so a
  cut there is not proof the body was complete.
- Text omitted among the 129: minimum 1,350 characters, median 17,050,
  maximum 2,085,356; median share of the text omitted 36.2%,
  maximum 98.6%; **33** papers lost more
  than half their text.
- Route to exclusion: 166 by the primary's `FT_EXCLUDE`
  (124 over budget); 5
  by primary `FT_ELIGIBLE`, verifier `FT_FLAGGED`, then PI adjudication
  (5 over budget — both models read cut
  text; the PI's own decision is a human one).

**Length of the parsed text, the 171 papers** (characters): minimum 369,
25th percentile 30,131, median 41,056,
75th percentile 57,809, maximum 2,115,796, mean
58,829. The median paper is longer than the whole budget.

Over-budget paper ids (129): 5, 47, 99, 106, 253, 255, 260, 271, 274, 275, 285, 288, 289, 291, 293, 294, 304, 311, 312, 321, 327, 329, 332, 334, 338, 339, 340, 345, 352, 358, 361, 367, 369, 371, 373, 375, 381, 389, 390, 396, 397, 398, 399, 401, 416, 417, 421, 423, 436, 437, 438, 440, 444, 447, 448, 469, 471, 479, 483, 494, 496, 500, 501, 506, 510, 520, 521, 525, 527, 529, 535, 544, 545, 547, 551, 552, 555, 558, 559, 560, 561, 563, 564, 571, 573, 578, 579, 584, 585, 591, 593, 594, 597, 599, 601, 605, 611, 612, 616, 627, 631, 648, 651, 656, 658, 664, 666, 671, 672, 675, 681, 686, 695, 702, 706, 711, 713, 718, 739, 756, 759, 760, 762, 766, 770, 776, 777, 789, 803.

### Informational — the decision rows (supersedes D5's 366)

| Table | Rows | Papers | Over budget (code's test) | Under | `len(text) > 32,000` |
| --- | --- | --- | --- | --- | --- |
| `ft_screening_decisions` (primary) | 366 | 364 | **299** | 67 | 289 |
| `ft_verification_decisions` (verifier) | 182 | 182 | **160** | 22 | 157 |

- Primary, by decision: `FT_EXCLUDE` 126 of
  168 over budget; `FT_ELIGIBLE`
  173 of 198.
- Verifier, by decision: `FT_ELIGIBLE` 129 of
  146; `FT_FLAGGED`
  31 of 36.
- Primary rows by the paper's present status: {"ABSTRACT_SCREENED_OUT": [3, 3], "AI_AUDIT_COMPLETE": [167, 192], "FT_SCREENED_OUT": [129, 171]}
  (over budget, rows). Three papers now `ABSTRACT_SCREENED_OUT` carry full-text decision rows
  (papers 4, 23, 168); not chased.
- No decision row was left without a version (0 primary,
  0 verifier).

D5's "366 FT decisions on partial text" is superseded by: **299 of
366 primary rows and 160 of 182 verifier rows were made on
cut text.**

### Informational — the 190 eligible papers (outside R516's scope)

165 of 190 were screened on cut text (25 under budget; undecidable
0, missing 0); 122 by prefix and
43 at a `references…` line; 37 lost more than half
their text. These were decisions to include. Four of these papers have two files on disk
(455, 586, 699, 719 — v2 and v3); the rule picks v2, the only one that existed in March.

## M2 — R536: did the OpenAlex retrieval hit the 10,000 cap?

### The stored files (`data/surgical_autonomy/expanded_search/`)

| File | sha256 | Bytes | Records | By source | What it is |
| --- | --- | --- | --- | --- | --- |
| `abstracts.jsonl` | `ab21456aa9412cb051caa92fc92ce4a03d360c0f83d1c1104bf7b2942b8fbe56` | 14,243,306 | 9,787 | pubmed 1,068 · openalex 8,719 | derived — abstracts for the net-new records (both sources) |
| `expanded_search_results.csv` | `939aca46d5fdccfc4c230cb3cab8a3f824c79564a03456f02f9b0ad483d1b6fe` | 1,708,936 | 10,038 | pubmed 1,088 · openalex 8,950 | the deduplicated search result, PubMed + OpenAlex |
| `net_new_papers.csv` | `8c7eb75641a0677a4f5950b27157243658e12a8a2fba7b6ca0b52a4f11fb27e1` | 1,668,514 | 9,787 | pubmed 1,068 · openalex 8,719 | the result minus the 251 papers already in the review |
| `rescreen_original_251.csv` | `651564313a46c3b3a8c8d51802392c362fa1a5238af4517c5800b3c106547f65` | 264,470 | 251 | — | screening output (the original 251), no search data |
| `screening_progress.json` | `2b0bcf7df84faac59fb735c2ab9b07ee59f9035e8d217a0293225fd517b8b1e9` | 405,952 | — | — | screening checkpoint |
| `screening_results.csv` | `c7c5f2128d683b38d9141636dec2fd4e4e58009f54577106110e053de75c258c` | 9,054,854 | 9,787 | pubmed 1,068 · openalex 8,719 | screening output over the net-new records |
| `stats.json` | `409464f5f78317ddef55fcf98cdadd65548c4e55892850391b80546668c70658` | 157 | — | — | the search's own counts |
| `verification_progress.json` | `1004415f6cbda7cfb2e7696192057bfc3c1ed63296ebb7c0f90542ba69a2b38a` | 38,497 | — | — | verification checkpoint |
| `verification_results.csv` | `39c4543b198ed661ff6403815e4349d7c9a8b955fb815a515891c61bc4cb669e` | 603,807 | 958 | pubmed 316 · openalex 642 | verification output |

The search result and its counts were written 2026-03-08 07:54 UTC. No raw PubMed or OpenAlex
response is stored, and no file from the review's first search (the 251 papers created on live
2026-02-28) exists.

### The one OpenAlex query

One query per source is implied by `stats.json`; the files do not record the query text. The
committed search module issues one OpenAlex request stream:
`Works().search(" ".join(query_terms)).filter(publication_year="2010-2025", type="article|review")`,
paged by cursor at 200 per page with pyalex's default `n_max` of 10,000 (pyalex 0.20, installed
2026-02-23, before the search).

| | Value | Where it sits |
| --- | --- | --- |
| OpenAlex records retrieved | 9,419 | `stats.json` `openalex_total` |
| PubMed records retrieved | 1,088 | `stats.json` `pubmed_total` |
| Duplicates removed | 469 | `stats.json` `duplicates_removed` |
| Unique | 10,038 | `stats.json` `unique_total`; `expanded_search_results.csv` has 10,038 rows |
| OpenAlex-labelled rows after deduplication | 8,950 | `expanded_search_results.csv` (9,419 − 469 = 8,950; every duplicate was resolved to the PubMed record) |
| Advertised total | none stored | — |
| Pages, cursor end | none stored | — |
| 9,419 mod 200 | 19 | computed |
| 10,000 − 9,419 | 581 | computed |

The file arithmetic closes: 1,088 + 9,419 −
469 = 10,038 = `unique_total`; less
251 already in the review = 9,787 = `net_new_papers.csv`'s rows.

### Verdict: UNDETERMINED

- It is **not** the CAPPED case as defined: the stored count is 9,419, not 10,000.
- It is **not** the NOT CAPPED case as defined: no advertised total is stored, and there is no
  evidence that pagination ended (no log, no cursor, no page count).
- Why a count below 10,000 does not settle it: if `openalex_total` counts parsed citations, as
  the committed module's does, a work with no title is dropped before the count. A capped
  retrieval of exactly 10,000 works with 581 title-less ones would store
  the same 9,419. That needs 5.8% of the
  works to have no title, which would be unusual for articles and reviews matched by a text
  search; that is circumstantial, not evidence. A count that is not a multiple of 200 fits either
  reading.
- What would decide it: the advertised total (`meta.count`) for that query and filter. That is
  the one network call R536 reserves for the PI's permission. **Not made.** A total at or below
  about 9,419 plus growth since March would read NOT CAPPED; a total well above 10,000 would
  read CAPPED.

### Did the records reach live as stored?

- Every one of the 10,038 rows of `expanded_search_results.csv` matches a live paper
  by DOI, PMID or title (the keys migration 003 uses): 10,038 found,
  0 not found, 10,038 distinct live ids. The
  9,787 rows of `net_new_papers.csv`: 9,787 found, sources equal
  on every row.
- Live: 10,039 papers — openalex 8,964, pubmed
  1,074, hand-search 1. The one live paper
  matching no file row is the hand-search paper (id 605).
- Source labels: 14 records are
  `pubmed` in the result file and `openalex` on live; all are among the first 251 live papers,
  which kept the labels of the earlier search (live ids ≤ 251: {"openalex": 245, "pubmed": 6}).
  Hence live's 8,964 OpenAlex papers against the file's
  8,950.
- Before deduplication: 9,419 OpenAlex records, known only from `stats.json`;
  the 469 merged records themselves are not recoverable.

## Findings, each with class and package (R509)

| # | Finding | Class | Package |
| --- | --- | --- | --- |
| M1-1 | 129 of the 171 full-text exclusions were decided on cut text (124 by the primary alone; 5 through the verifier and the PI). This is the R516 re-screen's measured scope. No new row: it sizes B-F11 | 1 (B-F11's) | P4 |
| M1-2 | R516's wording (`parsed text exceeds 32,000 characters`) selects 122; the code's test selects 129. The re-screen scope should use the code's test, or it misses 7 papers | 1 (B-F11's) | P4 |
| M1-3 | Row D5's "366" is the number of primary decision rows, not of decisions on partial text; the measured figures are 299 of 366 primary and 160 of 182 verifier rows. A row-text correction, by dated note | 3 | P7 |
| M1-4 | 165 of the 190 eligible papers were also screened on cut text. Outside R516 (inclusions); recorded so the number exists | — (informational; no row proposed) | — |
| M2-1 | The live corpus's OpenAlex retrieval cannot be shown complete from disk: no advertised total, page count or cursor was stored. Sizes INT-g2-1; the missing record is what B-F06's search ledger would hold | 1 (INT-g2-1's) | P3 (the ledger in P6) |
| M2-2 | The script that wrote the stored search files is not in the repository, and the query text it sent is recorded nowhere beside them | 3 | P3, with B-F06 |
| M2-3 | 14 records carry different source labels in the search file and on live (the first 251 kept an earlier search's labels), so a by-source count differs by where it is read | 3 | P3, with B-F06 |

None of these was acted on. No row was opened and the refactor plan was not edited.
