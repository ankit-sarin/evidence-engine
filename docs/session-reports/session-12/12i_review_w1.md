# 12i — internal review, wave 1 (the Run 7 core no outside assessment read; papers 4, 23, 168)

**Status: COMPLETE.** Read-only. No change under `engine/`, `tests/`, `scripts/`, `analysis/`,
`review_specs/` or `data/`; no model call; no network call beyond git; no live write (live opened
`mode=ro` only, by the lead, for §W1-d). Ends at a STOP for rulings (R580). **No fix was made, no
inventory row was opened, and Wave 2 has not been started.** Every finding below is a CANDIDATE
with a proposed class and package (R509, R581).

| | |
| --- | --- |
| Reference HEAD | `b711683af520466bff473ad3391c894b3c90b4ed` (the 12i scoping read) |
| Rulings this read works under | R577–R586 (PI, 2026-10-09; not yet transcribed — they are transcribed at the 12i close) |
| Scope | R578, R580: lots L01–L07 of `12i_scope.md`, plus every unit with a located outside finding that is on the Run 7 import graph, minus the function or block that finding located |
| Readers | 12 read-only readers in parallel, one per lot, each under `12i_review_w1/READER_RULES_W1.md` |
| Result | 98 reader candidates → 94 rows after merging: **23 proposed Class 1, 17 Class 1 conditional / latent / weak, 13 Class 2, 41 Class 3** |
| Live fingerprint | IDENTICAL to the migration-02 record (overall `0d3eedead60c2ce1c6341b973fa5931d69c4b13c14e93bd5b805bc06734fcb25`, 36 tables) |

## What this read-out says, in short

- **The elicited extraction path has a silent total-failure mode.** If Pass 1 returns nothing
  usable for a paper — empty, cut off, or not JSON — the paper is stored as `extracted` with twenty
  `contract_unmet` claims, the run's abort counter is reset, and later runs skip the paper. A
  run-wide Pass-1 failure would read as a completed run on the pinned arm (row 1; reproduced).
- **Several evidence checks on the Run 7 path are weaker than their text says** (rows 2–4, 11, 12):
  JUDGMENT steps are not all required to have a basis; a blank value is stored as a value; the
  locator scores long near-verbatim snippets far too low; a verbatim quote containing an ellipsis
  is marked invalid; value divergence between the passes is recorded nowhere.
- **The two quality gates after extraction can stay silent** (rows 18–22): the audit writes
  `audited_ai` over a recorded failure state; the distribution monitor reports OK for a field that
  is absent on every paper and cannot fire on a field with one off-spelling; LOW_YIELD reaches no
  reader; the spec's monitor thresholds are never read.
- **Parsing can mark a paper `PARSED` without a usable text, or exclude it on an environment
  fault** (rows 23–28).
- **The migration runner's "data migrations are never executed on a fresh database" is false**
  (row 37; reproduced on fake migrations), and its receipt read turns any read fault into "no
  receipts", which also disables the drift check (row 38). The second bears on assumption A1 of
  the R577–R583 ruling.
- **Reviewer decisions can be lost by the one reader** (rows 39–41). Nothing writes reviewer events
  yet; this becomes reachable with the human importer.
- **Papers 4, 23 and 168 were not excluded by a person.** A March script
  (`scripts/rescreen_with_specialty.py`) set their status by direct SQL after two model passes,
  about 80 minutes after a five-paper full-text smoke test had screened them. For 168, both
  full-text models had said eligible (§W1-d).
- Nothing found here is established as having corrupted a stored value on live. Live holds no
  field events, and the Run 6 tables were not read.

## W1-a — Lot assembly (R585)

Script: `12i_review_w1/lots_w1.py` (outputs `lots_w1.json`, `lots_w1.md`). 56 census units plus
`analysis/provenance/segment.py`, 18,433 in-scope lines, 12 readers.

| lot / reader | content | units | in-scope lines | F units added (excluded block) |
| --- | --- | ---: | ---: | --- |
| L01 | Extraction write path: claims as events, guards, selection, locator, telemetry | 11 | 2,318 | `engine/core/completeness.py` (313 of 313; A:A-§2.2; whole file in scope — no function named); `engine/core/paper_state.py` (233 of 233; A:A-§2.2, A:A-FP2; whole file in scope — no function named) |
| L02 | Elicitation (the Run 7 extraction design) | 9 | 2,131 | `engine/agents/models.py` (43 of 47; A:A-§3.2; excluding `ExtractionOutput`) |
| L03 | Audit and the distribution gate | 4 | 1,057 | — |
| L04 | Resolver and run manifest | 2 | 1,300 | — |
| L05 | Spec, codebook, review identity, eligibility rendering | 5 | 1,940 | — |
| L06 | Readers: the one reader, the parsed-text resolver, naming | 4 | 947 | `engine/core/corpus.py` (83 of 83; A:A-§4.3; whole file in scope — no function named) |
| L07 | Parser support: font audit, parse-quality verdict, markers, models | 6 | 2,540 | `engine/parsers/pdf_parser.py` (1295 of 1295; B:F01; excluding only the F01 block inside `parse_pdf`); also `analysis/provenance/segment.py` (64; outside the census; imported at module level by parse_quality.py, contracts.py, units.py (row A-5)) |
| W1-F1 | The two-pass extractor | 1 | 1,023 | `engine/agents/extractor.py` (1023 of 1077; A:A-§1.2; excluding `restart_ollama`) |
| W1-F2 | Database construction and the migration runner | 3 | 995 | `engine/cloud/__init__.py` (5 of 5; B:F17; whole file in scope — no function named); `engine/core/database.py` (630 of 748; B:F02, B:F05, B:F13; excluding `ReviewDatabase.__init__`, `ReviewDatabase._run_migrations`, `ReviewDatabase.add_papers`); `engine/migrations/runner.py` (360 of 360; B:F02; whole file in scope — no function named) |
| W1-F3 | Screening agents, dedup, OpenAlex client, download | 5 | 1,799 | `engine/acquisition/download.py` (477 of 477; B:F10; whole file in scope — no function named); `engine/agents/ft_screener.py` (682 of 682; B:F11; whole file in scope — no function named); `engine/agents/screener.py` (340 of 340; B:F12, B:F13; whole file in scope — no function named); `engine/search/dedup.py` (178 of 200; B:F04; excluding `_exact_match`); `engine/search/openalex.py` (122 of 148; B:F08; excluding `_paginate_with_retry`) |
| W1-F4 | The pipeline runner and the exporters | 5 | 1,453 | `engine/exporters/__init__.py` (15 of 73; B:F09; excluding `export_all`); `engine/exporters/evidence_table.py` (253 of 253; B:F09, B:F15; whole file in scope — no function named); `engine/exporters/methods_section.py` (207 of 207; B:F07; whole file in scope — no function named); `engine/exporters/prisma.py` (411 of 411; A:A-§4.3, B:F06; whole file in scope — no function named); `scripts/run_pipeline.py` (567 of 618; B:F05, B:F06, B:F12; excluding `_stage_search`, `_stage_screen`) |
| W1-F5 | The Ollama client and the experiment lock | 2 | 930 | `engine/utils/ollama_client.py` (739 of 739; A:A-§1.1, A:A-T1, A:A-§3.1, B:F14; whole file in scope — no function named); `engine/utils/ollama_lock.py` (191 of 191; A:A-§1.2; whole file in scope — no function named) |
| all | 12 readers | 56 + 1 | 18,433 | |

**The exclusion list is the lead's reading** of the assessment text, not a computed result. A bare
name match was tried first and over-excluded (it caught `main`, `receipts`, the whole
`ReviewDatabase` class and the exception `PendingMigrations`). A function is excluded only where a
finding names it as the place of the defect:

| unit | excluded | the assessment sentence behind it |
| --- | --- | --- |
| `engine/agents/extractor.py` | `restart_ollama` | A §1, second bullet: "However, restart_ollama() executes a system-level service restart." |
| `engine/agents/models.py` | `ExtractionOutput` | A §3, second bullet: "Under Condition B (format=ExtractionOutput.model_json_schema()), the schema wraps spans in {\"fields\": [...]}." |
| `engine/core/database.py` | `ReviewDatabase.__init__`, `._run_migrations`, `.add_papers` | B F02: "`ReviewDatabase.__init__` creates directories, enables WAL, executes `_SCHEMA`, and runs inline schema maintenance before the numbered runner checks pending migrations." and "the current `_run_migrations` finally block reopens a connection even after runner failure". B F05: "`papers` has a unique PMID; `add_papers` checks only PMID." (B F13 cites lines of this file and names no function.) |
| `engine/search/dedup.py` | `_exact_match` | B F04: "After failing DOI/PMID matching, `_exact_match` falls back to normalized title without checking whether both records have different known identifiers." |
| `engine/search/openalex.py` | `_paginate_with_retry` | B F08: "**Location:** `engine/search/openalex.py:60–88` (`_paginate_with_retry`)." |
| `engine/exporters/__init__.py` | `export_all` | B F09: "`export_all` writes PRISMA, CSV, workbook, DOCX, and methods sequentially without a shared read transaction." |
| `scripts/run_pipeline.py` | `_stage_search`, `_stage_screen` | B F06: "`_stage_search` returns raw and duplicate totals but does not persist a search ledger consumed by PRISMA." B F12: "`_stage_screen` detects more INGESTED papers than the requested limit, logs that screening is limited, then executes `pass` and calls `run_screening` on all INGESTED papers." |
| `engine/parsers/pdf_parser.py` | only the temp-file → INSERTs → commit → rename block inside `parse_pdf` | B F01: "**Location:** `engine/parsers/pdf_parser.py:941–984` (`parse_pdf`)." and "The parser writes a temporary Markdown file, inserts the asset/reference/attempt rows, commits the database, and then renames the temporary file to its final name." The rest of the 449-line function was in scope. |

The other 13 F units name no function as the place of a finding and were read whole (ruling
assumption A2's fallback, accepted by R585).

## W1-b — The readers

Each reader was given: the rules (`READER_RULES_W1.md`, which carries the R508 classes, the R540
packages, the five patterns of the brief, and the "Known false or silent at HEAD" list verbatim as
R584 supplied it); its lot file (`12i_review_w1/inputs/<LOT>.md`: files, scope, any excluded
block, the Step 2 inventory rows naming its files, the `12f_triage_rows.md` sections naming
them — built by `prep_inputs.py`); and pointers to CLAUDE.md, the plan and the 12f triage.
Reports: `12i_review_w1/readers/<LOT>.md`. Reproducers and their recorded outputs:
`12i_review_w1/repro/<LOT>/` (the reports cite them under `~/scratch/12i-w1/repro/`).

| reader | files | all read whole | candidates | proposed Class 1 (incl. conditional) / 2 / 3 | reproducers |
| --- | ---: | --- | ---: | --- | ---: |
| L01 | 11 | yes | 6 | 4 / 0 / 2 | 4 |
| L02 | 9 | yes | 13 | 7 / 0 / 6 | 1 |
| L03 | 4 | yes | 9 | 5 / 0 / 4 | 2 |
| L04 | 2 | yes | 9 | 3 / 2 / 4 | 2 |
| L05 | 5 | yes | 10 | 4 / 2 / 4 | 7 |
| L06 | 4 | yes | 6 | 2 / 1 / 3 | 1 |
| L07 | 6 | yes | 14 | 6 / 4 / 4 | 1 |
| W1-F1 | 1 | yes | 7 | 3 / 0 / 4 | 4 |
| W1-F2 | 3 | yes | 5 | 2 / 1 / 2 | 4 |
| W1-F3 | 5 | yes | 10 | 4 / 2 / 4 | 1 |
| W1-F4 | 5 | yes | 4 | 2 / 0 / 2 | 2 |
| W1-F5 | 2 | yes | 5 | 1 / 1 / 3 | 0 |
| all | 57 | | 98 | 43 / 13 / 42 | 29 |

(The counts in this table are read from the readers' final messages and reports — the lead's
reading, not a script. Every reader reported the repository tree clean when it finished, and
`git status --short` was empty when the lead checked after the last one.)

Each report also lists "Known, seen again" (existing rows the reader met and did not re-report),
"Adjacent" and "Could not establish". Those sections were not merged into the table below; the
adjacent items are collected in the last section.

## W1-c — Lead synthesis

**How each Class 1 and Class 2 candidate was re-derived.** For every one of the 56 reader
candidates proposed as Class 1 or 2 (53 rows after merging) the lead read the deciding lines in
the file at HEAD — `grep` / `sed` on the named function — and, where the reader wrote a
reproducer, reran it. All 29 reproducers were rerun by the lead from scratch copies
(`12i_review_w1/lead_rerun/*.out`): 22 outputs are byte-identical to the readers' recorded ones;
the other 7 differ only in timestamps, temp-directory names and hashes, and in that five of the
readers' files had stderr merged in. The per-candidate note is `12i_review_w1/lead_notes.md`.
`verify_quotes.py` (a mechanical check that quoted code occurs at HEAD) was run as an aid; it is
not the re-derivation and about half of what it flags as missing is prose or reproducer output.

**What "re-derived" does and does not mean.** `Y code+rerun`: the lead read the lines and reran
the reproducer. `Y code`: the lead read the lines; the consequence is by code order. In neither
case did the lead re-argue the reader's refutation search (whether a ruling accepted the behaviour
as built): readers grepped the plan by function name, and a ruling filed under other words could
have been missed. **One row is weaker than its mark:** row 27 (L07-C6), where the lead confirmed
the regex and nothing else — the consequence depends on how PyMuPDF names fonts in a text trace,
which no one established. Class 3 rows rest on the reader's quote and were not re-derived.

**Classes are the readers' proposals.** "1 (conditional / latent / weak)" marks a Class 1 the
reader itself qualified: the trigger is unestablished, or no path reaches it today, or the reader
gave a lower class as the alternative. Where a reader offered an alternative class the package
cell carries it.

### Counts by proposed class

Script: `12i_review_w1/synthesis.py` (the rows are the lead's data; the script renders and counts).

| proposed class | rows after merging | of which new (proposed INT-i12-n) | of which extend or duplicate an existing row |
| --- | ---: | ---: | ---: |
| 1 | 23 | 16 | 7 |
| 1, conditional / latent / weak | 17 | 11 | 6 |
| 2 | 13 | 9 | 4 |
| 3 | 41 | 30 | 11 |
| all | 94 | 66 | 28 |

Reader candidates: 98, in 94 rows after 4 merges. Class 1 / 2 rows: 53, every one marked re-derived by the lead (29 by code and rerun, 24 by code).

By package (first-named), Class 1 and 1-conditional / Class 2 / Class 3: P1 18 / 1 / 1; P2 8 / 1 / 1; P3 2 / 0 / 1; P4 8 / 7 / 0; P5 3 / 3 / 0; P6 1 / 1 / 0; P7 0 / 0 / 38

### The candidates

Order: Class 1, then Class 1 conditional, interleaved by subject (Run 7 path first), then Class
2, then Class 3. "new" rows carry a proposed ID `INT-i12-n`; **none is opened.**

| # | reader ID(s) | lot | file / function | pattern | evidence level | class / package | existing row, or new (proposed ID) | re-derived by lead | statement |
| ---: | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | L01-C1 + L02-C1 | L01, L02 | extraction_events.counts_toward_abort / plan_extraction_events; elicitation contracts.check_response → terminal_states → pipeline 'Pass 2 skipped'; selection.select_for_extraction | ii | reproduced | 1 / P1 | extends A-11 (R535's floor counts fields held, not values) | Y code+rerun | A record whose 20 fields are all contract_unmet or declined — including one produced by an empty, truncated or non-JSON Pass-1 response — is stored as `extracted`, resets the abort counter, and the paper is skipped by reuse key afterwards. A run-wide Pass-1 failure reads as a completed run. |
| 2 | L02-C4 | L02 | elicitation contracts._parse_steps / Step.has_basis | i | reproduced | 1 / P1 (checker-side, not pin-affecting) | new — **INT-i12-1** | Y code+rerun | JUDGMENT 'every step has a basis' is not enforced: a bare-string step is dropped silently, criteria_application: "false" counts as a basis, a text-less step passes. The field reads contract-met. |
| 3 | L02-C3 + W1-F1-C3 | L02, W1-F1 | elicitation pipeline.extract_paper_elicited span loop; extractor.extract_paper legacy write boundary; citation_guard.check_citations | other | reproduced | 1 (conditional / latent / weak) / P1 | new — **INT-i12-2** | Y code+rerun | A blank or whitespace Pass-2 value with evidence passes both guards and is stored as an `asserted`, non-sentinel value (reader row 11) on both the elicited and the legacy path. Pass 1 has VALUE_MISSING; Pass 2, which supplies the stored value, has no equivalent. |
| 4 | L02-C2 | L02 | elicitation pipeline.extract_paper_elicited tail; module docstring | i / iv | code order | 1 / P1 (3 / P7 if ruled wording-only) | new — **INT-i12-3** | Y code | 'Value divergence is counted in telemetry' is false: value_divergence, n_value_divergence, elicitation_run_id and accepted_pass1_attempt are set on a dict nothing reads. A Pass-2 value stored over Pass-1 evidence for a different value leaves no trace. |
| 5 | L02-C5 | L02 | elicitation units.py constants; pipeline.persist_unit_map, sentinel_pass1_prompt; run_manifest.LIBRARIES | iv | code order | 1 / P1 ((b) pin-affecting) | (a) extends INT-g12-1 / R568; (b) new — **INT-i12-4** | Y code | (a) The unit-map directory is named by process-start timestamp, not the manifest run_id, and the name is recorded only in the unread dict above. (b) pysbd is not in the manifest's library versions and the prompt hash renders a two-line sentinel text, so a segmentation change is invisible to the arm pin. |
| 6 | L02-C7 | L02 | elicitation pipeline.run_pass1 / extract_paper_elicited `except DuplicateFieldError` | iv | code order | 1 (conditional / latent / weak) / P1 | new — **INT-i12-5** | Y code | A duplicate field on Pass-1 attempt 2 writes a context_chain of one hash though two calls ran; attempt 1's usable answer and the duplicate-bearing response reach no telemetry. |
| 7 | L02-C6 | L02 | elicitation units.COMMENT_RE / strip_comments | i | reproduced (behaviour); conditional (a parsed text has an unterminated `<!--`) | 1 (conditional / latent / weak) / P1, with A-10 | extends A-10 | Y code+rerun | The comment says unterminated comment tails are matched; they are not. An unterminated `<!--` followed later by `-->` deletes the body text between them from the numbered units. |
| 8 | W1-F1-C1 | W1-F1 | extractor._extract_selected per-paper try | iii / ii | reproduced (fault injected) | 1 / P1 | extends C56 (handler #6) | Y code+rerun | The try also covers post-commit bookkeeping. A failure there (the reproducer injects a failed stderr write in progress.report) records extraction_failed / unclassified_error on a paper whose claims are live; selection then skips it. Whether that fault occurs in the real run environment is not established. |
| 9 | W1-F1-C2 | W1-F1 | extractor.extract_paper_with_completeness (`last_error`) | other / iv | reproduced (stubbed replies that differ per attempt) | 1 (conditional / latent / weak) / P1 | extends A-11 | Y code+rerun | At budget exhaustion only the last attempt is stored: a field uncited only on attempt 3 becomes contract_unmet with attempts: 3; two 19-field attempts followed by an unparseable third store nothing. No ruling says which attempt wins. |
| 10 | L01-C4 | L01 | citation_guard.check_citations (escape branch); extraction_events.legacy_record | i | conditional (a legacy-prompt response returns the escape token); mechanics reproduced | 1 (conditional / latent / weak) / P1 | new — **INT-i12-6** | Y code+rerun | On the legacy path a value equal to the escape token is exempt from the guard and stored as an `asserted` value with no snippet; legacy_record has no `declined` branch. |
| 11 | L01-C3 | L01 | locator.locate | iv / i | reproduced | 1 / P1 (audit-side, outside the pin; needs a new LOCATOR_VERSION — timing against R572) | new — **INT-i12-7** | Y code+rerun | SequenceMatcher runs with autojunk on (analysis/provenance/classifier.py and font_audit.py pass autojunk=False and say why). A 379-character snippet with 3 of 70 words changed scores 0.732 against 0.974 and is recorded located = false. Fails toward review, not toward a wrong value. Every fuzzy test fixture is under 200 characters. |
| 12 | L05-C3 | L05 | constants.INVALID_SNIPPET_RE (auditor, extractor._has_invalid_snippet, locator.locate) | ii | reproduced (regex, locate); code order (auditor, extractor) | 1 / P1 | new — **INT-i12-8** | Y code+rerun | The ellipsis regex is applied to the snippet alone. A quote verbatim from a passage containing `…` or `...` is EXACT-located yet bridged=True, and the auditor returns invalid_snippet before locating. |
| 13 | L05-C2 + W1-F1-C6 | L05, W1-F1 | codebook.load_codebook / load_codebook_beside; extractor.build_extraction_prompt docstring | i / iv | code order | 1 / P1 | new — **INT-i12-9** | Y code | The codebook's `review` key is compared only inside load_codebook_for. run_pipeline, open_run, extract_paper, the elicitation pipeline and cloud all load by path, so a mis-copied codebook beside a database (R235's hand edit) is used unrefused. |
| 14 | L05-C4 | L05 | codebook.SEMANTIC_KEYS / compute_semantic_hash | i / iv | reproduced | 1 (conditional / latent / weak) / P1, pin-affecting if fixed | extends C34 | Y code+rerun | canonical_absence_sentinel reaches the extraction prompts and is the value cloud writes for a null, but is outside the semantic hash. The byte hash and the prompt hash still move. Fixing it moves every pin, so it is a before-Run-7 decision. |
| 15 | L04-C1 | L04 | effective_config.render_messages (cloud) / prompt_hash; run_manifest.open_run / pin_tuple; cloud/base.build_prompt | iv | conditional (run_cloud_extraction.py --db outside data/<review>/; cloud runs at all, R71) | 1 (conditional / latent / weak) / P1 | new (kin to A4-b and to L05-C2) — **INT-i12-10** | Y code | Manifest codebook hashes, the cloud prompt hash and the arm pin are taken over the codebook beside the database; cloud prompts are built by build_extraction_prompt(parsed_text, self.spec) with no path, i.e. from data/<review_id>/. No refusal when they differ. |
| 16 | L04-C3 | L04 | effective_config.resolve_run; run_manifest.check_declared_call, extraction_digest | iv | conditional (a tag re-pulled or re-created mid-run) | 1 (conditional / latent / weak) / P1 | extends A4-b | Y code | The model digest is read once at run open; the per-call check compares the model name only. A model replaced under the same tag mid-run is attributed to the opening digest and pin. |
| 17 | W1-F5-C1 | W1-F5 | ollama_client.effective_ceiling, n_ctx_train, _check_input_was_read | i (+iv) | conditional (a Modelfile sets num_ctx below the trained context and the request sends none) | 1 (conditional / latent / weak) / P1 | new — **INT-i12-11** | Y code | The input-fit ceiling has three terms (trained context, options.num_ctx, service env) and never reads a Modelfile PARAMETER num_ctx. If one exists the runtime cuts at N while the guard's ceiling stays higher, so a truncated call passes. The only protection is a dated measurement. |
| 18 | L03-C1 | L03 | audit_events.audit_run | iii / other | reproduced (mechanism; production trigger not driven) | 1 / P1 (or P6 with D22's 023 build) | new (the inverse of D22) — **INT-i12-12** | Y code+rerun | audited_ai is written whatever the processing state is: a paper at parse_failed with live unlocated claims became audited_ai, reason cleared, analysis_ready True. No test seeds a processing state before an audit. |
| 19 | L03-C2 | L03 | distribution_monitor.check_distribution | i | reproduced | 1 / P2 (3 if ruled intended) | new — **INT-i12-13** | Y code+rerun | A field whose every value is an absence sentinel reads status OK: 40×NR → OK; 31×NR + 9 identical → OK. Pinned as built by test_all_nr_excluded; no ruling found. |
| 20 | L03-C3 | L03 | distribution_monitor._query_all_fields → check_distribution | i | reproduced | 1 / P2 | new — **INT-i12-14** | Y code+rerun | Values are counted raw, so one case or punctuation variant is a second level and COLLAPSED cannot fire: 39 + 1 off-case → LOW_VARIANCE only; 15 + 1 → OK. |
| 21 | L03-C4 | L03 | audit_events.audit_run / low_yield; run_pipeline._stage_audit | i / v | code order | 1 / P5 (3 if ruled telemetry-only) | new — **INT-i12-15** | Y code | LOW_YIELD has no reader: computed only for papers audited in that call and surfaced only in the stage's log line. CLAUDE.md says 'PRISMA-reported'; the spec says 'flagged … for PI review'. |
| 22 | L03-C5 | L03 | distribution_monitor.run_post_extraction_check, main; callers | i / iv | code order | 1 / P2 (3 if the dead field is deleted) | new as a row (SPEC-AUTH-01 phase 2 and GENERALIZE-READOUT-01 noted it, never rowed) — **INT-i12-16** | Y code | The spec's distribution_monitor thresholds are validated and hashed but no caller passes them; the population floor is a separate literal 10. The live spec does not set the block. |
| 23 | L07-C1 | L07 | pdf_parser.parse_with_vision, parse_with_pymupdf, parse_pdf judge loop; parse_quality.assess | i (+v) | reproduced (gate half); code order (cascade half); trigger conditional on blank per-page output | 1 / P4 | new — **INT-i12-17** | Y code+rerun | Vision and PyMuPDF always emit `<!-- Page N -->` per page, so blank output is never 'empty'. A 1-page marker-only text PASSES the gate and the paper goes PARSED; at 2+ pages it is stored and excluded as SHATTERED instead of raising cascade-empty. The tests fake "  ", which the real functions cannot return. |
| 24 | L07-C2 | L07 | pdf_parser.parse_pdf same-hash short-circuit; parse_all_pdfs | iii | code order | 1 / P4 | extends B-F01 (12f reproduced the missing-file variant; this is the gate-failed variant, which B-F01's minimum fix does not close) | Y code | The short-circuit returns no verdict and no attempts, and the driver reads 'no accepted attempt' as a pass: a stored gate-FAILED parse on a PDF_ACQUIRED paper becomes PARSED. |
| 25 | L07-C3 | L07 | pdf_parser.parse_pdf outer `except BaseException` + _commit_attempts | iii | code order | 1 / P2 | extends B-F01 and the C56 half-B note in 12f_triage.md ('commits ledger rows on an interrupt') | Y code | On KeyboardInterrupt or RunInterrupted inside the asset write block the inner `except Exception` rollback is skipped and the outer handler commits the pending asset, ref and hash rows with the attempt rows. The file is never renamed. |
| 26 | L07-C4 | L07 | pdf_parser.parse_pdf re-route tail; parse_all_pdfs | ii | code order | 1 / P4 | extends C56 | Y code | A tier that raises on a re-route (OCR crash, Ollama down, UndeclaredOverride) breaks the cascade without trying the next tier; the paper ends PDF_EXCLUDED / PARSE_QUALITY (terminal) on an environment fault. |
| 27 | L07-C6 | L07 | font_audit._font_objects (_DESCENDANT), audit pass 1 | iv | conditional (an indirect /DescendantFonts array, or any name-join miss); no such PDF was opened | 1 (conditional / latent / weak) / P4 | new — **INT-i12-18** | Y code (the regex only; the consequence rests on the reader) | A signature font whose characters do not join by name contributes zero to exposure, with no unjoined residual, so FONT_EXPOSURE can pass a damaged text. |
| 28 | L07-C7 | L07 | pdf_parser.reparse_papers | i | code order (no production caller: grep finds the definition only) | 1 (conditional / latent / weak) / P2 | new — **INT-i12-19** | Y code | 'Evidence only, the ruling is a human step' holds for papers.status only: the new version is at once the resolver's current text, even when it failed the gate. |
| 29 | W1-F3-C1 | W1-F3 | ft_screener.FTScreeningDecision, run_ft_screening; database.add_ft_screening_decision | iv / i | reproduced | 1 / P4 | new — **INT-i12-20** | Y code+rerun | An FT decision and its reason code are never checked against each other: FT_EXCLUDE + "eligible" is stored as FT_SCREENED_OUT with a full_text_out event, and FT_ELIGIBLE + an exclusion code passes on. The format schema names the vocabulary in the description only, not as an enum. |
| 30 | W1-F3-C2 | W1-F3 | ft_screener.run_ft_verification, _complete_ft_stage | i | reproduced | 1 / P4 (arguably 2) | extends J8 | Y code+rerun | FULL_TEXT_SCREENING_COMPLETE is set when the run wrote at least one verification decision, not when nothing is left: a verify-only run completed it with two PARSED papers never screened. |
| 31 | W1-F3-C3 | W1-F3 | download.download_papers, _download_one | iv | code order | 1 (conditional / latent / weak) / P4 | extends B-F10 / D20 | Y code | Which of five strategies or URLs supplied a PDF, its hash and the cause of a failure are printed only; acquisition_date is the batch-start time for every paper. |
| 32 | W1-F3-C4 | W1-F3 | openalex.search_openalex | i / iv | code order (the difference); conditional (its effect — OpenAlex's default operator) | 1 (conditional / latent / weak) / P3 | new — **INT-i12-21** | Y code | OpenAlex gets the query terms space-joined with a hard-coded type=article\|review filter; PubMed gets them joined with " AND ". The methods text reports one query for both and no filter. |
| 33 | L05-C1 | L05 | review_spec.Eligibility._check, StagePolicy; eligibility_render.decision_instruction, edge_case_guidance | i (v) | reproduced | 1 (conditional / latent / weak) / P4 | new — **INT-i12-22** | Y code+rerun | Every model stage must declare a policy, but only abstract_primary renders one; the FT adjudication sheet renders ft_primary's policy while the FT prompt carries none. Latent on live, where ft_primary declares only absence_is_evidence. |
| 34 | W1-F4-C1 | W1-F4 | methods_section.generate_methods_section; prisma.generate_prisma_flow | i / iv | reproduced | 1 / P3 | extends B-F07 and B-F06 | Y code+rerun | A review entered through import_extraction_entry (screened elsewhere, zero screening rows, no model call) exports a methods text stating a PubMed/OpenAlex search and model screening, and PRISMA reports the imported papers as screened, retrieved and assessed; validate_prisma_counts returns valid. Not reachable on live. |
| 35 | W1-F4-C2 | W1-F4 | run_pipeline.run_pipeline (`except Exception` / `except RunAborted`) with run_manifest.close_run | iii | reproduced | 1 / P2 (a reader could call it 2) | new (the failed-path twin of C47's interrupt rollback); kin to L04-C4 — **INT-i12-23** | Y code+rerun | The `failed` close commits on the run's one connection without a rollback, so a failing stage's open transaction is committed with it: with add_papers raising mid-batch, 2 of 4 papers were committed under a failed manifest; the same half-write under KeyboardInterrupt leaves 0. The one known writer is add_papers; its realistic trigger was not observed. |
| 36 | L04-C2 | L04 | run_manifest.git_state → open_run DirtyTree | ii / i | reproduced | 1 (conditional / latent / weak) / P2 | new — **INT-i12-24** | Y code+rerun | No return code is read for `git rev-parse` or `git status --porcelain`. A failing `git status` (corrupt index) gives dirty=False with a valid commit, so an edited tree opens a run recorded as clean. |
| 37 | W1-F2-C1 | W1-F2 | migrations/runner.run | i | reproduced | 1 / P6 | new (related S9) — **INT-i12-25** | Y code+rerun | include_data is one boolean with no freshness test and no migration id: run(<fresh>, include_data=True) executes data migrations on a fresh database, and --apply-pending --include-data executes every unreceipted data migration (002, 003, 017), not one named. 003's source is an absolute path to the surgical_autonomy corpus. 'Never executed on a fresh database' is false in the runner docstring, the CLI help, the README and CLAUDE.md. Live is not exposed (21 receipts). |
| 38 | W1-F2-C2 | W1-F2 | migrations/runner.receipts (check_drift, run) | ii | reproduced (receipts / check_drift); conditional inside run | 1 (conditional / latent / weak) / P2 | extends B-F02(b) | Y code+rerun | `except sqlite3.OperationalError: return {}` turns any read fault into 'no receipts'. Under a lock check_drift returned [] despite real drift; inside run that means fresh=True. Bears on this ruling's assumption A1 (an applied migration's text cannot change without the runner refusing). |
| 39 | L01-C2 + L06-C1 | L01, L06 | events.write_field_event (R20 branch); effective._governing_reviewer / effective_value rows 2, 4, 5 | i (v) | reproduced | 1 / P5 | new — **INT-i12-26** | Y code+rerun | The writer accepts a reviewer decision naming competing decisions but no claim; the reader then clears the row-2 conflict and returns the machine's value (a CORRECT to "7" reads back as "5") with no reviewer key in provenance. No production writer of reviewer events exists yet; reachable with the human importer. |
| 40 | L06-C2 | L06 | effective.effective_value rows 6 / 8; events.write_field_event | i | reproduced on a writer-built history; conditional for live | 1 (conditional / latent / weak) / P5 | new — **INT-i12-27** | Y code+rerun | Row 6's 'ACCEPT is refused at write' is false for one claim id holding two differing asserted values (the writer counts claim ids): the reader skips row 6 and endorses the newest value. |
| 41 | L06-C3 | L06 | effective.effective_value row 3 exit; _governing_reviewer | i | reproduced | 2 / P5 | new — **INT-i12-28** | Y code+rerun | Row 3's printed exit ('a new decision against the current claim') lands in row 2 unless the new decision repeats the stale one's type and value or lists it in against_decisions. Loud, but the exit does not exit. |
| 42 | L04-C4 | L04 | run_manifest.open_run savepoint tail (also close_run, record_call) | iii | reproduced (mechanism); code order for open_run | 2 / P5 | new (family of C50); kin to W1-F4-C2 — **INT-i12-29** | Y code+rerun | RELEASE open_run then `if conn.in_transaction: conn.commit()` commits a caller's open transaction, against the 'nests inside a caller's open transaction' comment. No current production caller holds a transaction there. |
| 43 | L04-C5 | L04 | run_manifest.pin_tuple, open_run, open_review_session | other | conditional (a caller passes arms=; none does today) | 2 / P5 | extends C45 | Y code | An arm named with zero stages is pinned to "stages": {} plus the codebook hash: a review session naming a model arm either refuses or pins it so extraction refuses forever. |
| 44 | L05-C5 | L05 | review_spec.Eligibility._check, VerifierTest; eligibility_render.absent_abstract_fallback, verifier_tests_block | other / i | reproduced | 2 / P4 | new — **INT-i12-30** | Y code+rerun | A verifier stage with zero tests loads and renders 'Apply these tests strictly:' with nothing (silent). A spec with no insufficient_data criterion loads and raises ValueError at the first abstract-less paper. |
| 45 | L05-C7 | L05 | codebook._validate_valid_values, _validate_top_level, _parse | other | reproduced | 2 / P1 | new — **INT-i12-31** | Y code+rerun | Unquoted Yes/No in valid_values load as booleans and raise TypeError later in prompt building; an unquoted sentinel NO or null silently becomes "False" or "None". The live codebook quotes everything. |
| 46 | L07-C5 | L07 | pdf_parser._REROUTE, _next_parser; parse_quality.CRITERIA | i | code order | 2 / P4 | new — **INT-i12-32** | Y code | FONT_EXPOSURE has no re-route entry, so an exposure-only failure is excluded without OCR being tried. |
| 47 | L07-C8 | L07 | pdf_parser.strip_links_to_temp and its call in parse_pdf | ii | code order | 2 / P4 | extends C56 | Y code | If the link-strip raises, the cascade aborts before PyMuPDF and the temp PDF leaks, against 'never outlive the call'. |
| 48 | L07-C9 | L07 | pdf_parser.parse_pdf initial scanned and digital-sparse routes | i | code order | 2 / P4 | new — **INT-i12-33** | Y code | vision_max_pages is enforced only on re-routes: a scanned PDF over the OCR cap skips OCR and is sent whole to vision. |
| 49 | L07-C10 | L07 | pdf_parser.parse_all_pdfs no-PDF branch | ii | code order | 2 / P4 | extends D20 | Y code | A missing PDF is a log line plus stats['failed'], with no event even under a run, and is retried silently every run. |
| 50 | W1-F2-C3 | W1-F2 | migrations/runner.run | iii | code order | 2 / P6 | new (adjacent to C11) — **INT-i12-34** | Y code | The migration commits on its own connection and the receipt is a second transaction outside the MigrationError wrapper: a failure between them leaves the migration applied with no receipt. |
| 51 | W1-F3-C5 | W1-F3 | ft_screener.run_ft_screening | ii (inverse) / other | reproduced | 2 / P4 | new — **INT-i12-35** | Y code+rerun | An out-of-vocabulary reason code raises ValueError inside the paper transaction, outside the malformed-output handler, so the whole run closes failed; the paper stays first in line and an identical rerun failed the same way. |
| 52 | W1-F3-C6 | W1-F3 | download.download_papers | i | code order | 2 / P4 (3 if docstring only) | new (adjacent to D20) — **INT-i12-36** | Y code | 'All included papers with PDF URLs or DOIs' actually selects every non-terminal status, including INGESTED and ABSTRACT_SCREEN_FLAGGED, with no identifier test. |
| 53 | W1-F5-C2 | W1-F5 | ollama_lock.check_experiment_lock, foreign_lock_held; ollama_client._restart_ollama_and_retry | i | code order | 2 / P2 | extends A1 (same gate, different mechanism) | Y code | The restart gate is a probe released immediately, not a hold across the restart: a process that takes the lock after the probe is restarted beneath, with no refusal on either side. |
| 54 | L01-C5 | L01 | extraction_events.write_extraction_events tail, outcome_for_exception fit mapping | iv | code order | 3 / P7 | new — **INT-i12-37** | N | The R130 telemetry row always says attempt=1 and files the count as pass1_prompt_eval_count whichever call was truncated. |
| 55 | L01-C6 | L01 | batch across eight L01 modules | i | code order | 3 / P7 | item 1 extends A-11; item 8 touches RB-7; rest new | N | Eight wording or dead-code items; none changes a stored value. |
| 56 | L02-C8 | L02 | elicitation prompts._sentinel_rule | i | code order | 3 / P7 (prompt text: pin-affecting if changed) | new — **INT-i12-38** | N | The rule hard-codes "NR" and ignores its sentinels argument. |
| 57 | L02-C9 | L02 | materialize.contiguous_runs / source_snippet | i | reproduced | 3 / P7, or fold into INT-g12-1 | extends INT-g12-1 | N (lead reran the reproducer) | 'First contiguous run' is the lowest-numbered run after a sort, not the first cited: cited (47, 48, 12) stores unit 12. |
| 58 | L02-C10 | L02 | contracts._resolve_indices, parse_container, _entries | i | reproduced (scalar) + code order | 3 / P7 | new — **INT-i12-39** | N (lead reran the reproducer) | Shape repairs go unrecorded despite 'no silent repair anywhere'. |
| 59 | L02-C11 | L02 | prompts.build_pass2_priming_message docstring | i | code order | 3 / P7 | new — **INT-i12-40** | N | The priming message is nested inside the legacy wrapper rather than replacing the trace message. |
| 60 | L02-C12 | L02 | pipeline.elicit, prompts.build_feedback_block, build_pass1_prompt | i | code order | 3 / P7 | new (the lost losing attempt is R571(e)) — **INT-i12-41** | N | Batch: losing attempt's raw content not persisted; unparseable feedback says FIELD_MISSING ×N; a stale prompt sentence; dead tier defaults. |
| 61 | L02-C13 | L02 | agents/models.EvidenceSpan.clamp_confidence | ii / i | code order | 3 / P7 | new — **INT-i12-42** | N | Out-of-range confidence is silently clamped and stored; the schema description contradicts the strict contract. |
| 62 | L03-C6 | L03 | audit_events.audit_run; auditor.semantic_verify | ii | code order | 3 / P7 | new (neighbours B23-EXP) — **INT-i12-43** | N | An unparseable or empty auditor response is stored as verdict `flagged`. |
| 63 | L03-C7 | L03 | distribution_monitor.check_distribution, main | iv / i | code order | 3 / P7 | new — **INT-i12-44** | N | Two codebooks are read; the CLI's --codebook is half-honoured. |
| 64 | L03-C8 | L03 | audit_events docstring / audit_run | i | conditional | 3 / P7 (2 if a refusal is wanted) | new — **INT-i12-45** | N | 'Cross-family' verification is stated but nothing compares the audit model with the arm's model. |
| 65 | L03-C9 | L03 | distribution_monitor, auditor | i | code order | 3 / P7 | new — **INT-i12-46** | N | Hygiene batch (truncating log count, stale docstrings, unused names). |
| 66 | L04-C6 | L04 | run_manifest docstring / record_active_ollama_call | i / iv | code order | 3 / P7 | extends B-F14 | N | run_calls is one row per ollama_chat call, not per request sent. |
| 67 | L04-C7 | L04 | run_manifest.git_state (state_tags) | i | code order | 3 / P7 | new (E-STATE residue) — **INT-i12-47** | N | A lightweight tag sets engine_state though the docstring says annotated. |
| 68 | L04-C8 | L04 | effective_config.stage_config, _format | i | code order | 3 / P7 | new — **INT-i12-48** | N | stage_config("ft_screen_primary", None) raises AttributeError despite the docstring. |
| 69 | L04-C9 | L04 | run_manifest._RUN_FIELD_NAMES | i | conditional (two databases in one process) | 3 / P7 | new — **INT-i12-49** | N | Registry keyed by run_id alone, which is per database. |
| 70 | L05-C6 | L05 | review_spec.load_review_spec; codebook._parse | i | reproduced | 3 / P7 (1 if a dead declaration counts) | new — **INT-i12-50** | N (lead reran the reproducer) | A YAML key declared twice loads in both the spec and the codebook, last wins. |
| 71 | L05-C8 | L05 | codebook._parse vs compute_codebook_sha256 | i / iv | reproduced | 3 / P2 | new — **INT-i12-51** | N (lead reran the reproducer) | Codebook.sha256 hashes decoded text, not the file bytes; differs on a CRLF file. |
| 72 | L05-C9 | L05 | review_spec.ScreeningModels defaults | i | code order | 3 / P7 | new — **INT-i12-52** | N | Declared defaults pair two qwen models against the cross-family rule; live declares both explicitly. |
| 73 | L05-C10 | L05 | review_paths._validate_requested_id | i | reproduced | 3 / P7 | new — **INT-i12-53** | N (lead reran the reproducer) | re.match with `$` accepts "id\n". |
| 74 | L06-C4 | L06 | effective._classify_claim, iter_grid | i | reproduced (row number) | 3 / P7 | new (also seen by L01, L03, L05, W1-F1 as an adjacent item) — **INT-i12-54** | N (lead reran the reproducer) | The sentinel test is exact membership, not Codebook.is_absence_sentinel: 'nr' or ' NR' reads row 11 instead of 13, and that rule_row is exported. Value and state are unaffected. |
| 75 | L06-C5 | L06 | corpus.py docstring | i | code order | 3 / P7 | extends C13 | N | 'Exactly one permitted consumer' is two; R42's retirement of analysis/eval/schema_eval2.py has not happened. |
| 76 | L06-C6 | L06 | effective.py docstrings | i | code order | 3 / P7 | new (INT-g12-3 family) — **INT-i12-55** | N | Four drifted sentences; load_absence_sentinels has no caller. |
| 77 | L07-C11 | L07 | pdf_parser.parse_pdf digital sparse fall-through | iv | code order | 3 / P7 | new — **INT-i12-56** | N | A sparse Docling result leaves no ledger row; OCR can be re-run and use up an attempt. |
| 78 | L07-C12 | L07 | font_audit.audit, _font_rows | iv | conditional | 3 / P7 | new — **INT-i12-57** | N | Program facts can come from the wrong same-named font; telemetry only. |
| 79 | L07-C13 | L07 | pdf_parser.verify_hashes | i | code order | 3 / P7 | new — **INT-i12-58** | N | Takes an arbitrary asset row and resolves the path against cwd. |
| 80 | L07-C14 | L07 | pdf_parser.parse_all_pdfs, parse_pdf | i | code order | 3 / P7 | new — **INT-i12-59** | N | skipped_existing never incremented; `or "docling"` under a 'never the literal docling' comment; arbitrary glob pick. |
| 81 | W1-F1-C4 | W1-F1 | extractor._retry_snippet / _validate_and_retry_snippets | iv / i | reproduced | 3 / P1 | extends C56 (handler #4) | N (lead reran the reproducer) | An answered snippet-retry call that is not a JSON object is `completed` in run_calls yet missing from every claim's context_chain. |
| 82 | W1-F1-C5 | W1-F1 | extractor._extract_selected / record_selection_refusals | iv | code order | 3 / P7 | new (also seen by L01) — **INT-i12-60** | N | Failure events hard-code stage_name ('extract_pass2' for every exception; 'extract_pass1' for text refusals). |
| 83 | W1-F1-C7 | W1-F1 | extractor.py, several | i | code order | 3 / P7 (item a with A-11 in P1) | (a), (d) extend A-11; rest new | N | Seven stale statements or counts. Also: row C41's text is stale — `_with_think` is no longer in extractor.py. |
| 84 | W1-F2-C4 | W1-F2 | database.ReviewDatabase.update_status | ii (masked) | reproduced | 3 / P7 | extends the 12f A-7 section | N (lead reran the reproducer) | When BEGIN IMMEDIATE fails on a lock the handler issues ROLLBACK with no transaction open; the caller sees 'cannot rollback', not 'database is locked'. |
| 85 | W1-F2-C5 | W1-F2 | migrations/runner.discover, check_drift | i | reproduced | 3 / P7 | new — **INT-i12-61** | N (lead reran the reproducer) | Files not matching ^\d{3}_[a-z0-9_]+\.py$ are silently not migrations; a receipt whose file was deleted is not drift. |
| 86 | W1-F3-C7 | W1-F3 | ft_screener and screener flag branches | ii | code order | 3 / P7 | extends A14 / 12f A-6(e) | N | The cause of a flag lives only in a log line. |
| 87 | W1-F3-C8 | W1-F3 | ft_screener checkpoint helpers | iii | code order | 3 / P7 | extends B-F13(c) | N | Same id-only, non-atomic checkpoint as the abstract screener; survives a failed run into the next manifest. |
| 88 | W1-F3-C9 | W1-F3 | download, openalex, screener, ft_screener, dedup | i / other | code order | 3 / P7 | new — **INT-i12-62** | N | Batch: dead iteration; two hard-coded personal emails; discarded confidence; unused arguments. |
| 89 | W1-F3-C10 | W1-F3 | openalex._parse_work | other | conditional | 3 / P3 (1 if the condition holds) | extends B-F04 | N | PMID and DOI are normalised by one exact literal prefix each. |
| 90 | W1-F4-C3 | W1-F4 | run_pipeline.run_pipeline finally, main | i / other | code order | 3 / P7 | new — **INT-i12-63** | N | Every exit logs 'PIPELINE COMPLETE'; a BLOCKED gate stop exits 0. |
| 91 | W1-F4-C4 | W1-F4 | run_pipeline.main with background.maybe_background | i | code order | 3 / P7 | new — **INT-i12-64** | N | With --background the logs directory is created from an unvalidated argv pre-scan before the spec check. |
| 92 | W1-F5-C3 | W1-F5 | ollama_client._check_input_was_read, ollama_chat._finish | i / iv | conditional (the runtime omits prompt_eval_count) | 3 / P7 (1 only if the condition is shown) | new — **INT-i12-65** | N | A response with no count skips both post-call checks and is recorded completed. Pinned as intended by a test. |
| 93 | W1-F5-C4 | W1-F5 | ollama_client.ollama_chat, _restart_ollama_and_retry | ii / iv | code order | 3 / P7 | extends B-F14 | N | Four different endings are all recorded as 'timed out after N attempts + restart'. |
| 94 | W1-F5-C5 | W1-F5 | ollama_client.ollama_chat except arm | other | code order | 3 / P7 | new — **INT-i12-66** | N | The httpx.ConnectError arm is dead under ollama 0.6.1; only the log label is wrong. |

### Things in the table that bear on rulings already made

- **Ruling assumption A1** ("an applied migration's text cannot change without the runner
  refusing") — row 38: under a read fault `receipts()` returns `{}` and `check_drift` returns `[]`
  (reproduced under a lock). The refusal exists; it is not unconditional.
- **R535's floor** — row 1: the floor counts fields held; a record holding twenty non-values
  holds twenty fields.
- **R572** (no locator-2 before the tag) — row 11: the fix to the fuzzy score is a new
  `LOCATOR_VERSION`. Row 12 (the ellipsis regex) is in the same instrument.
- **R568** (each claim records its citation evidence without the unit-map directory) — row 5(a)
  is the same gap from the directory's side.
- **R571(e)** (keep the rejected attempt's raw response) — rows 6 and 60 (L02-C7, L02-C12) are
  further places where a Pass-1 attempt's content is lost.
- **Pin-affecting if fixed** (so before the freeze, R539): row 5(b) (pysbd version and the
  segmentation constants), row 14 (the semantic hash), and the prompt-text items in rows 56, 59
  and 60 (L02-C8, C11, C12).
- **D22 / R431** (`audit_not_possible`) — row 18 is the inverse case: the audit running *over* a
  failure state.

## W1-d — Papers 4, 23 and 168 (live, `mode=ro`; facts only)

Script: `12i_review_w1/papers_timeline.py`; output `papers_timeline.json`.

**I2.** `decided_at` is populated on every row of `abstract_screening_decisions` (21,374 rows),
`ft_screening_decisions` (366) and `ft_verification_decisions` (182), so decisions are ordered by
timestamp, not by id. `abstract_screening_adjudication` has **0 rows on live for any paper** (row
A2, per R586), so no abstract-adjudication row or reviewer can be given. `paper_events` has no row
for these three. The only status-transition record is `papers.updated_at`, which records the time
of the last UPDATE of the row and not what it changed.

**Paper 4** — "Robots and Tools for Remodeling Bone." (source pubmed; `papers.status` `ABSTRACT_SCREENED_OUT`; `abstract` present)

| time (UTC) | record | decision | model | rationale (first words) |
| --- | --- | --- | --- | --- |
| 2026-02-28 23:33:01.951 | papers.created_at |  |  |  |
| 2026-02-28 23:33:19.758 | abstract pass 1 | include | qwen3:8b | The paper discusses robotic surgery and its application in bone remodeling, which involves a physical surgical… |
| 2026-02-28 23:33:22.752 | abstract pass 2 | include | qwen3:8b | The paper discusses robotic surgery and its application in bone remodeling, which involves a physical surgical… |
| 2026-03-01 20:41:07.987 | full_text_assets row (parsed_at) | parsed text v1 |  |  |
| 2026-03-12 06:28:12.989 | papers.acquisition_date |  |  |  |
| 2026-03-13 00:11:43.682 | FT primary | FT_EXCLUDE (reason code `insufficient_data`) | qwen3.5:27b | The paper is a methodological review providing an overview of current robots and tools for bone remodeling, pr… |
| 2026-03-13 01:30:10.257 | abstract pass 1 | exclude | qwen3:8b | [specialty_rescreen] The paper discusses robotic tools for bone remodeling but does not specify whether the ro… |
| 2026-03-13 01:30:13.629 | abstract pass 2 | exclude | qwen3:8b | [specialty_rescreen] The paper discusses robotic tools for bone remodeling but does not specify whether the ro… |
| 2026-03-13 01:30:13.640 | `papers.updated_at` — the last UPDATE of the row |  |  |  |

**Paper 23** — "Robot-Assisted Minimally Invasive Surgery—Surgical Robotics in the Data Age" (source openalex; `papers.status` `ABSTRACT_SCREENED_OUT`; `abstract` present)

| time (UTC) | record | decision | model | rationale (first words) |
| --- | --- | --- | --- | --- |
| 2026-02-28 23:33:01.951 | papers.created_at |  |  |  |
| 2026-02-28 23:35:05.331 | abstract pass 1 | include | qwen3:8b | The paper discusses robot-assisted minimally invasive surgery (RAMIS), which involves a physical surgical task… |
| 2026-02-28 23:35:09.314 | abstract pass 2 | include | qwen3:8b | The paper discusses robot-assisted minimally invasive surgery (RAMIS), which involves a physical surgical task… |
| 2026-03-01 20:45:00.747 | full_text_assets row (parsed_at) | parsed text v1 |  |  |
| 2026-03-12 06:28:12.989 | papers.acquisition_date |  |  |  |
| 2026-03-13 00:11:18.363 | FT primary | FT_EXCLUDE (reason code `insufficient_data`) | qwen3.5:27b | The paper is a review article (specifically described as a 'scoping literature review' and an overview of the … |
| 2026-03-13 01:31:25.175 | abstract pass 1 | exclude | qwen3:8b | [specialty_rescreen] The paper provides a general overview of telesurgical robotics and its evolution, focusin… |
| 2026-03-13 01:31:27.648 | abstract pass 2 | exclude | qwen3:8b | [specialty_rescreen] The paper provides an overview of telesurgical robotics and discusses advancements in the… |
| 2026-03-13 01:31:27.658 | `papers.updated_at` — the last UPDATE of the row |  |  |  |

**Paper 168** — "Autonomous pick-and-place using the dVRK" (source openalex; `papers.status` `ABSTRACT_SCREENED_OUT`; `abstract` empty)

| time (UTC) | record | decision | model | rationale (first words) |
| --- | --- | --- | --- | --- |
| 2026-02-28 23:33:01.951 | papers.created_at |  |  |  |
| 2026-02-28 23:46:58.393 | abstract pass 1 | include | qwen3:8b | The title suggests the study involves autonomous robotic surgery, which meets the inclusion criteria for a phy… |
| 2026-02-28 23:47:01.280 | abstract pass 2 | include | qwen3:8b | The title suggests the study involves autonomous robotic surgery, which meets the inclusion criteria for a phy… |
| 2026-03-01 21:13:10.514 | full_text_assets row (parsed_at) | parsed text v1 |  |  |
| 2026-03-12 06:28:12.990 | papers.acquisition_date |  |  |  |
| 2026-03-13 00:10:53.827 | FT primary | FT_ELIGIBLE (reason code `eligible`) | qwen3.5:27b | The paper describes a semi-autonomous robotic system using the da Vinci Research Kit (dVRK) to perform a physi… |
| 2026-03-13 00:12:42.695 | FT verifier | FT_ELIGIBLE | gemma3:27b | The paper clearly describes a robotic system (dVRK) performing a surgical task (US probe positioning) with an … |
| 2026-03-13 01:35:57.097 | abstract pass 1 | exclude | qwen3:8b | [specialty_rescreen] The paper has no abstract available, which is a clear exclusion criterion as per the prov… |
| 2026-03-13 01:35:58.304 | abstract pass 2 | exclude | qwen3:8b | [specialty_rescreen] The paper has no abstract available, which is a clear exclusion criterion as per the prov… |
| 2026-03-13 01:35:58.315 | `papers.updated_at` — the last UPDATE of the row |  |  |  |

**`papers.rejected_reason`** is NULL for all three.

**The workflow stamps for the human abstract review** (`workflow_state`): `ABSTRACT_QUEUE_EXPORTED`
complete 2026-03-10T05:48:19 ("416 papers exported to
data/surgical_autonomy/adjudication/screening_queue_20260310.xlsx"); `ABSTRACT_ADJUDICATION_COMPLETE`
complete 2026-03-12T05:37:12 ("416 expanded-search papers excluded via human review
(screening_queue_20260310.xlsx). 0 missing, 0 invalid decisions."). Both precede every event that
excluded these three papers.

**The March queue artifact.** `data/surgical_autonomy/adjudication/screening_queue_20260310.xlsx`
exists (file mtime 2026-03-12 05:12 UTC; sheet "Review Queue", 416 data rows, columns Row #, Auto
Category, Title, Abstract, DOI, PMID, Year, Journal, Source, Flagged By, Primary/Verifier Decision
and Rationale). **None of the three papers is in it** (matched on title, PMID and DOI). A second
workbook beside it, `specialty_rescreen_flagged_86.xlsx` (mtime 2026-03-13 04:16 UTC; 86 data
rows; it has a `paper_id` column and `PI_decision` / `PI_notes` columns), **also contains none of
the three.** `ft_adjudication_queue.json` and `surgical_autonomy_ft_adjudication_decisions.json`
(36 items each) contain none of them. A3 held: `effective-result-01/S2_phase1_readout_addendum3_20260921.md`
§F identifies the human abstract exclusions by `rejected_reason` (416 + 7); these three carry none.

**Which row last set `ABSTRACT_SCREENED_OUT`.** No row records it; it is inferred from two facts.
(1) For each paper `papers.updated_at` is 10–11 ms after its second 2026-03-13 abstract decision
row, whose rationale begins `[specialty_rescreen]`. (2) That tag and that write are
`scripts/rescreen_with_specialty.py`: `RESCREEN_TAG = "specialty_rescreen"`, and

```python
if d1.decision == "exclude" and d2.decision == "exclude":
    # Both passes say exclude → ABSTRACT_SCREENED_OUT
    _force_status(db, pid, "ABSTRACT_SCREENED_OUT")
```

where `_force_status` is "Direct SQL status update — bypasses state machine"
(`UPDATE papers SET status = ?, updated_at = ? WHERE id = ?`, then commit). The same code is in
the March commit `d5fa5bc` (2026-03-13 01:02 UTC). The script's docstring gives its targets as
"ABSTRACT_SCREENED_IN (554) + AI_AUDIT_COMPLETE (95) = 649 papers"; live holds 649 distinct papers
with a 2026-03-13 abstract decision row.

**Where the FT rows came from.** `scripts/ft_screening_smoke_test.py` has
`PAPER_IDS = [9, 12, 168, 23, 4]`. Live holds exactly five `ft_screening_decisions` rows dated
2026-03-13 (00:10:03 to 00:11:43), for papers 9, 12, 168, 23 and 4, and three
`ft_verification_decisions` rows that day; the main FT run is dated 2026-03-14 (361 and 179 rows).
Papers 9 and 12 are in the corpus today (`AI_AUDIT_COMPLETE`).

**Population context** (same script): the 2026-03-13 re-screen wrote two `exclude` rows for 182
papers and two `include` rows for 467. 179 papers have two `[specialty_rescreen]` exclude rows
and are `ABSTRACT_SCREENED_OUT` today. 15 papers were excluded in that re-screen with a rationale
containing "no abstract"; all 15 have an empty `abstract`. Of the `ABSTRACT_SCREENED_OUT` papers
last updated on 2026-03-13, 255 have a NULL `rejected_reason` and 2 carry "verifier-excluded,
consistent with 416/416 adjudication concordance".

### The three answers for paper 168

- **(a) Does the human exclusion demonstrably postdate both FT_ELIGIBLE rows? — There is no human
  exclusion to date.** Paper 168 has no adjudication row, a NULL `rejected_reason`, and is in
  neither queue workbook. What postdates both FT_ELIGIBLE rows (primary 00:10:53, verifier
  00:12:42 UTC on 2026-03-13) is a model exclusion: two `qwen3:8b` re-screen passes at 01:35:57
  and 01:35:58, each with the rationale "The paper has no abstract available, which is a clear
  exclusion criterion as per the provided guidelines." Paper 168's `abstract` is empty (length 0);
  its title is "Autonomous pick-and-place using the dVRK"; its parsed full text exists on disk.
- **(b) Did the reviewer's artifact show the FT result? — No artifact shown to a reviewer contains
  this paper**, so the question does not arise for 168. Neither workbook has an FT column.
- **(c) Did any engine path set the status without a human decision? — Yes.**
  `scripts/rescreen_with_specialty.py` set `ABSTRACT_SCREENED_OUT` on two agreeing model passes,
  by direct SQL that bypasses `update_status`, with no reason written to `rejected_reason` and no
  event. (Two agreeing exclude passes excluding a paper without a human is also what the abstract
  screener itself does by design; what is particular here is the forced write on papers that
  already had a later-stage result, and that nothing recorded it.)

**Proposed class, because an engine path is implicated:** the script is one of row **I19**'s six
retire-candidates, and its `_force_status` is the same shape as **INT-g6-2** (a status set by
direct UPDATE, bypassing `update_status`). Proposed: extends I19 and INT-g6-2 — Class 1 while the
script remains runnable (it would move an in-corpus paper to a terminal status with no event),
P7 if it is retired, which closes it. Whether paper 168, or the other 178 re-screen exclusions,
should be looked at again is a review decision and is not proposed here. For the record, the live
spec does carry a no-abstract exclusion criterion ("Papers with no abstract or insufficient
information to determine eligibility — do not default to inclusion when evidence is absent").

## Not reviewed (R579)

| lot | reason |
| --- | --- |
| L15 — numbered migrations and the migrations CLI | moves to P6's Phase A, which reads the schema and the applied migrations before 023 / 024 |
| L16 — developer tools and the read-only validator | out of the internal review by R579; not on the Run 7 import graph |
| L17 — scripts off the Run 7 import graph | out of the internal review by R579 (two of them, `rescreen_with_specialty.py` and `ft_screening_smoke_test.py`, were read in part for §W1-d) |
| L18 — empty package markers | nothing to read |
| L08–L14 | Wave 2, under its own brief (R580) |
| F units off the Run 7 graph (`db_backup.py`, `db_fingerprint.py`, `advance_stage.py`, `scripts/advance_to_pdf_acquired.py`) | out by R578 |

## Adjacent items (recorded, not chased; no row opened)

From the readers' "Adjacent" sections, outside their own lots. Items that another reader's
candidate already covers are left out.

| # | where | what | proposed class / package |
| --- | --- | --- | --- |
| 1 | `engine/core/extraction_events.py`, exception classifier | the builtin `ConnectionError` raised for a down Ollama server matches none of the listed types and is classified `unclassified_error`, not `model_call_failed` (W1-F5) | 3 / P7 |
| 2 | `engine/agents/extractor.py::restart_ollama` | the same probe-then-restart window as row 53 (W1-F5); inside the block excluded from W1-F1 | 2 / P2, with row 53 |
| 3 | `engine/agents/extractor.py::record_selection_refusals` | an already extracted and audited paper whose text file later goes missing gets a new `extraction_failed` event each run while its claims stay live (L01) | 1 conditional / P1 |
| 4 | `engine/agents/audit_events.py::audit_run` | a paper whose live claims are all `contract_unmet` or `declined` never receives `audited_ai` (L01) — the audit-side face of row 1 | with row 1 |
| 5 | `engine/core/events.py::write_field_event` | `against_claims` is not checked to be claims of this cell; a mistyped id is stored and the cell reads row 3 (L06) | 2 / P5 |
| 6 | `scripts/run_cloud_extraction.py::run_arm` | the cloud run never calls `rm.activate`, so R225's field-name check does not apply there; and its `finally: close_run(status)` closes `failed` on an interrupt with no rollback first (L04) | 1 latent / P1 (R71) |
| 7 | `scripts/screen_expanded.py`; `engine/acquisition/pdf_quality_check.py` CLI | no `open_run`, so their model calls run outside any manifest (L04) — Wave 2 / L17 files | for Wave 2 |
| 8 | `scripts/run_pipeline.py::_stage_search` | always searches both PubMed and OpenAlex; `spec.search_strategy.databases` feeds only the methods text (W1-F3); inside the excluded `_stage_search` | 1 conditional / P3, with B-F06 |
| 9 | `scripts/run_pipeline.py::_stage_audit`, `_advance_extraction_workflow` | only `spec.extraction_models.arm` is audited while `audited_ai` is paper-level; the audit workflow stage completes once any paper is `audited_ai` (L03) | 1 conditional / P1 |
| 10 | `engine/adjudication/ft_screening_adjudicator.py::_collect_ft_flagged` | reads the excerpt straight from `full_text_assets.parsed_text_path`, bypassing the hash-checked resolver (W1-F3) — a Wave 2 file | for Wave 2 |
| 11 | `engine/acquisition/check_oa.py`, `manual_list.py` | the same "every non-terminal status" selection as row 52 (W1-F3) — Wave 2 files | for Wave 2 |
| 12 | `engine/acquisition/pdf_quality_check.py`, `engine/adjudication/categorizer.py` | each declares a private `DATA_ROOT = Path("data")` (W1-F2) — Wave 2 files | for Wave 2 |
| 13 | `engine/core/database.py::DATA_ROOT` | cwd-relative, so `ReviewDatabase("<mistyped>")` silently creates a new empty review tree (W1-F2); inside the excluded `__init__` | 2 / P2, with B-F02 |
| 14 | `engine/utils/background.py::maybe_background` | the relaunch is `cmd 2>&1 \| tee log` with no `pipefail`, so the 128+signum exit is lost (W1-F4) — a Wave 2 file | for Wave 2 |
| 15 | `tests/migrations/test_pending_guard.py::_inject_fake_migration` | filters the real data migrations out of discovery, which is what hides row 37 from the suite (W1-F2) | with row 37 |
| 16 | `scripts/smoke_test_fixes.py` | calls `audit_span(...)` then `.status` on the returned tuple (L03) — an L17 file | 3 / P7 |
| 17 | `engine/analysis/report.py::_get_tier_map` | `except Exception: _TIER_CACHE = {}` around a hard-coded `load_codebook_for("surgical_autonomy")` (L05) — a Wave 2 file | for Wave 2 |
| 18 | `scripts/advance_to_pdf_acquired.py::main` | inserts a `full_text_assets` row with a hash and no parse, which would satisfy the same-hash short-circuit of row 24 (L07). Dead today | 3 / P7 |
| 19 | row C41 of the plan | its text says `_with_think` remains in `extractor.py`; it is not in the file at HEAD (W1-F1) | row-text correction |

Also from §W1-d (the lead's, not chased): the human-workbook key "EE-014" names a different paper
from live paper 23's `ee_identifier` — the R14 reconciliation session 12 already owns.

## The INFERRED items

| | result |
| --- | --- |
| I1 (ruling assumption A2) | **Held in part; the ruling's own fallback applied.** Of the 21 F units on the Run 7 graph, 8 have a function (or, for `pdf_parser.py`, a block) named as the place of the finding; 13 name none and were read whole. Reported before dispatch and accepted by R585. |
| I2 | **Held for the decision tables; the abstract adjudication table is empty** (0 rows on live for any paper). Stated in §W1-d; nothing is ordered by id. |
| Ruling assumption A3 | **Held.** Addendum 3 §F is at `docs/session-reports/effective-result-01/S2_phase1_readout_addendum3_20260921.md`. |
| Ruling assumption A1 | Not acted on in this session, as ruled. Row 38 bears on it. |
| R584's list | Passed to every reader verbatim, inside `READER_RULES_W1.md`. The brief's premise that the list is in the refactor plan did not hold (it is in the unified plan only); recorded by R584 as an architect error. |
