"""12i-REVIEW-W1, W1-c — the lead's synthesis table. The DATA below is the lead's judgement
(merges, mapping to existing rows, proposed IDs, the re-derivation note per row); the script only
renders it and counts.

usage:  synthesis.py <out_dir>      writes synthesis.json, candidates_table.md, counts.md

Row tuple:
  (reader IDs, lot, file / function, pattern, evidence level, class, package, mapping,
   re-derived, statement)
  class     the reader's proposed class; "1c" = Class 1 conditional / latent / weak as the reader
            qualified it (see the reader's section for the stated condition)
  mapping   "new" (gets a proposed INT-i12-n) or "extends <row>" / "duplicates <row>"
  re-derived  for Class 1 / 2 rows: how the LEAD re-derived the deciding lines at HEAD —
            "Y code" (lead read the lines at HEAD), "Y code+rerun" (and reran the reader's
            reproducer, same output). Class 3 rows: "N" — they rest on the reader's quote.
"""
import json, os, sys
from collections import Counter

ROWS = [
 # ---- Class 1 ---------------------------------------------------------------------------
 ("L01-C1 + L02-C1", "L01, L02", "extraction_events.counts_toward_abort / plan_extraction_events; elicitation contracts.check_response → terminal_states → pipeline 'Pass 2 skipped'; selection.select_for_extraction", "ii", "reproduced", "1", "P1", "extends A-11 (R535's floor counts fields held, not values)", "Y code+rerun",
  "A record whose 20 fields are all contract_unmet or declined — including one produced by an empty, truncated or non-JSON Pass-1 response — is stored as `extracted`, resets the abort counter, and the paper is skipped by reuse key afterwards. A run-wide Pass-1 failure reads as a completed run."),
 ("L02-C4", "L02", "elicitation contracts._parse_steps / Step.has_basis", "i", "reproduced", "1", "P1 (checker-side, not pin-affecting)", "new", "Y code+rerun",
  "JUDGMENT 'every step has a basis' is not enforced: a bare-string step is dropped silently, criteria_application: \"false\" counts as a basis, a text-less step passes. The field reads contract-met."),
 ("L02-C3 + W1-F1-C3", "L02, W1-F1", "elicitation pipeline.extract_paper_elicited span loop; extractor.extract_paper legacy write boundary; citation_guard.check_citations", "other", "reproduced", "1c", "P1", "new", "Y code+rerun",
  "A blank or whitespace Pass-2 value with evidence passes both guards and is stored as an `asserted`, non-sentinel value (reader row 11) on both the elicited and the legacy path. Pass 1 has VALUE_MISSING; Pass 2, which supplies the stored value, has no equivalent."),
 ("L02-C2", "L02", "elicitation pipeline.extract_paper_elicited tail; module docstring", "i / iv", "code order", "1", "P1 (3 / P7 if ruled wording-only)", "new", "Y code",
  "'Value divergence is counted in telemetry' is false: value_divergence, n_value_divergence, elicitation_run_id and accepted_pass1_attempt are set on a dict nothing reads. A Pass-2 value stored over Pass-1 evidence for a different value leaves no trace."),
 ("L02-C5", "L02", "elicitation units.py constants; pipeline.persist_unit_map, sentinel_pass1_prompt; run_manifest.LIBRARIES", "iv", "code order", "1", "P1 ((b) pin-affecting)", "(a) extends INT-g12-1 / R568; (b) new", "Y code",
  "(a) The unit-map directory is named by process-start timestamp, not the manifest run_id, and the name is recorded only in the unread dict above. (b) pysbd is not in the manifest's library versions and the prompt hash renders a two-line sentinel text, so a segmentation change is invisible to the arm pin."),
 ("L02-C7", "L02", "elicitation pipeline.run_pass1 / extract_paper_elicited `except DuplicateFieldError`", "iv", "code order", "1c", "P1", "new", "Y code",
  "A duplicate field on Pass-1 attempt 2 writes a context_chain of one hash though two calls ran; attempt 1's usable answer and the duplicate-bearing response reach no telemetry."),
 ("L02-C6", "L02", "elicitation units.COMMENT_RE / strip_comments", "i", "reproduced (behaviour); conditional (a parsed text has an unterminated `<!--`)", "1c", "P1, with A-10", "extends A-10", "Y code+rerun",
  "The comment says unterminated comment tails are matched; they are not. An unterminated `<!--` followed later by `-->` deletes the body text between them from the numbered units."),
 ("W1-F1-C1", "W1-F1", "extractor._extract_selected per-paper try", "iii / ii", "reproduced (fault injected)", "1", "P1", "extends C56 (handler #6)", "Y code+rerun",
  "The try also covers post-commit bookkeeping. A failure there (the reproducer injects a failed stderr write in progress.report) records extraction_failed / unclassified_error on a paper whose claims are live; selection then skips it. Whether that fault occurs in the real run environment is not established."),
 ("W1-F1-C2", "W1-F1", "extractor.extract_paper_with_completeness (`last_error`)", "other / iv", "reproduced (stubbed replies that differ per attempt)", "1c", "P1", "extends A-11", "Y code+rerun",
  "At budget exhaustion only the last attempt is stored: a field uncited only on attempt 3 becomes contract_unmet with attempts: 3; two 19-field attempts followed by an unparseable third store nothing. No ruling says which attempt wins."),
 ("L01-C4", "L01", "citation_guard.check_citations (escape branch); extraction_events.legacy_record", "i", "conditional (a legacy-prompt response returns the escape token); mechanics reproduced", "1c", "P1", "new", "Y code+rerun",
  "On the legacy path a value equal to the escape token is exempt from the guard and stored as an `asserted` value with no snippet; legacy_record has no `declined` branch."),
 ("L01-C3", "L01", "locator.locate", "iv / i", "reproduced", "1", "P1 (audit-side, outside the pin; needs a new LOCATOR_VERSION — timing against R572)", "new", "Y code+rerun",
  "SequenceMatcher runs with autojunk on (analysis/provenance/classifier.py and font_audit.py pass autojunk=False and say why). A 379-character snippet with 3 of 70 words changed scores 0.732 against 0.974 and is recorded located = false. Fails toward review, not toward a wrong value. Every fuzzy test fixture is under 200 characters."),
 ("L05-C3", "L05", "constants.INVALID_SNIPPET_RE (auditor, extractor._has_invalid_snippet, locator.locate)", "ii", "reproduced (regex, locate); code order (auditor, extractor)", "1", "P1", "new", "Y code+rerun",
  "The ellipsis regex is applied to the snippet alone. A quote verbatim from a passage containing `…` or `...` is EXACT-located yet bridged=True, and the auditor returns invalid_snippet before locating."),
 ("L05-C2 + W1-F1-C6", "L05, W1-F1", "codebook.load_codebook / load_codebook_beside; extractor.build_extraction_prompt docstring", "i / iv", "code order", "1", "P1", "new", "Y code",
  "The codebook's `review` key is compared only inside load_codebook_for. run_pipeline, open_run, extract_paper, the elicitation pipeline and cloud all load by path, so a mis-copied codebook beside a database (R235's hand edit) is used unrefused."),
 ("L05-C4", "L05", "codebook.SEMANTIC_KEYS / compute_semantic_hash", "i / iv", "reproduced", "1c", "P1, pin-affecting if fixed", "extends C34", "Y code+rerun",
  "canonical_absence_sentinel reaches the extraction prompts and is the value cloud writes for a null, but is outside the semantic hash. The byte hash and the prompt hash still move. Fixing it moves every pin, so it is a before-Run-7 decision."),
 ("L04-C1", "L04", "effective_config.render_messages (cloud) / prompt_hash; run_manifest.open_run / pin_tuple; cloud/base.build_prompt", "iv", "conditional (run_cloud_extraction.py --db outside data/<review>/; cloud runs at all, R71)", "1c", "P1", "new (kin to A4-b and to L05-C2)", "Y code",
  "Manifest codebook hashes, the cloud prompt hash and the arm pin are taken over the codebook beside the database; cloud prompts are built by build_extraction_prompt(parsed_text, self.spec) with no path, i.e. from data/<review_id>/. No refusal when they differ."),
 ("L04-C3", "L04", "effective_config.resolve_run; run_manifest.check_declared_call, extraction_digest", "iv", "conditional (a tag re-pulled or re-created mid-run)", "1c", "P1", "extends A4-b", "Y code",
  "The model digest is read once at run open; the per-call check compares the model name only. A model replaced under the same tag mid-run is attributed to the opening digest and pin."),
 ("W1-F5-C1", "W1-F5", "ollama_client.effective_ceiling, n_ctx_train, _check_input_was_read", "i (+iv)", "conditional (a Modelfile sets num_ctx below the trained context and the request sends none)", "1c", "P1", "new", "Y code",
  "The input-fit ceiling has three terms (trained context, options.num_ctx, service env) and never reads a Modelfile PARAMETER num_ctx. If one exists the runtime cuts at N while the guard's ceiling stays higher, so a truncated call passes. The only protection is a dated measurement."),
 ("L03-C1", "L03", "audit_events.audit_run", "iii / other", "reproduced (mechanism; production trigger not driven)", "1", "P1 (or P6 with D22's 023 build)", "new (the inverse of D22)", "Y code+rerun",
  "audited_ai is written whatever the processing state is: a paper at parse_failed with live unlocated claims became audited_ai, reason cleared, analysis_ready True. No test seeds a processing state before an audit."),
 ("L03-C2", "L03", "distribution_monitor.check_distribution", "i", "reproduced", "1", "P2 (3 if ruled intended)", "new", "Y code+rerun",
  "A field whose every value is an absence sentinel reads status OK: 40×NR → OK; 31×NR + 9 identical → OK. Pinned as built by test_all_nr_excluded; no ruling found."),
 ("L03-C3", "L03", "distribution_monitor._query_all_fields → check_distribution", "i", "reproduced", "1", "P2", "new", "Y code+rerun",
  "Values are counted raw, so one case or punctuation variant is a second level and COLLAPSED cannot fire: 39 + 1 off-case → LOW_VARIANCE only; 15 + 1 → OK."),
 ("L03-C4", "L03", "audit_events.audit_run / low_yield; run_pipeline._stage_audit", "i / v", "code order", "1", "P5 (3 if ruled telemetry-only)", "new", "Y code",
  "LOW_YIELD has no reader: computed only for papers audited in that call and surfaced only in the stage's log line. CLAUDE.md says 'PRISMA-reported'; the spec says 'flagged … for PI review'."),
 ("L03-C5", "L03", "distribution_monitor.run_post_extraction_check, main; callers", "i / iv", "code order", "1", "P2 (3 if the dead field is deleted)", "new as a row (SPEC-AUTH-01 phase 2 and GENERALIZE-READOUT-01 noted it, never rowed)", "Y code",
  "The spec's distribution_monitor thresholds are validated and hashed but no caller passes them; the population floor is a separate literal 10. The live spec does not set the block."),
 ("L07-C1", "L07", "pdf_parser.parse_with_vision, parse_with_pymupdf, parse_pdf judge loop; parse_quality.assess", "i (+v)", "reproduced (gate half); code order (cascade half); trigger conditional on blank per-page output", "1", "P4", "new", "Y code+rerun",
  "Vision and PyMuPDF always emit `<!-- Page N -->` per page, so blank output is never 'empty'. A 1-page marker-only text PASSES the gate and the paper goes PARSED; at 2+ pages it is stored and excluded as SHATTERED instead of raising cascade-empty. The tests fake \"  \", which the real functions cannot return."),
 ("L07-C2", "L07", "pdf_parser.parse_pdf same-hash short-circuit; parse_all_pdfs", "iii", "code order", "1", "P4", "extends B-F01 (12f reproduced the missing-file variant; this is the gate-failed variant, which B-F01's minimum fix does not close)", "Y code",
  "The short-circuit returns no verdict and no attempts, and the driver reads 'no accepted attempt' as a pass: a stored gate-FAILED parse on a PDF_ACQUIRED paper becomes PARSED."),
 ("L07-C3", "L07", "pdf_parser.parse_pdf outer `except BaseException` + _commit_attempts", "iii", "code order", "1", "P2", "extends B-F01 and the C56 half-B note in 12f_triage.md ('commits ledger rows on an interrupt')", "Y code",
  "On KeyboardInterrupt or RunInterrupted inside the asset write block the inner `except Exception` rollback is skipped and the outer handler commits the pending asset, ref and hash rows with the attempt rows. The file is never renamed."),
 ("L07-C4", "L07", "pdf_parser.parse_pdf re-route tail; parse_all_pdfs", "ii", "code order", "1", "P4", "extends C56", "Y code",
  "A tier that raises on a re-route (OCR crash, Ollama down, UndeclaredOverride) breaks the cascade without trying the next tier; the paper ends PDF_EXCLUDED / PARSE_QUALITY (terminal) on an environment fault."),
 ("L07-C6", "L07", "font_audit._font_objects (_DESCENDANT), audit pass 1", "iv", "conditional (an indirect /DescendantFonts array, or any name-join miss); no such PDF was opened", "1c", "P4", "new", "Y code (the regex only; the consequence rests on the reader)",
  "A signature font whose characters do not join by name contributes zero to exposure, with no unjoined residual, so FONT_EXPOSURE can pass a damaged text."),
 ("L07-C7", "L07", "pdf_parser.reparse_papers", "i", "code order (no production caller: grep finds the definition only)", "1c", "P2", "new", "Y code",
  "'Evidence only, the ruling is a human step' holds for papers.status only: the new version is at once the resolver's current text, even when it failed the gate."),
 ("W1-F3-C1", "W1-F3", "ft_screener.FTScreeningDecision, run_ft_screening; database.add_ft_screening_decision", "iv / i", "reproduced", "1", "P4", "new", "Y code+rerun",
  "An FT decision and its reason code are never checked against each other: FT_EXCLUDE + \"eligible\" is stored as FT_SCREENED_OUT with a full_text_out event, and FT_ELIGIBLE + an exclusion code passes on. The format schema names the vocabulary in the description only, not as an enum."),
 ("W1-F3-C2", "W1-F3", "ft_screener.run_ft_verification, _complete_ft_stage", "i", "reproduced", "1", "P4 (arguably 2)", "extends J8", "Y code+rerun",
  "FULL_TEXT_SCREENING_COMPLETE is set when the run wrote at least one verification decision, not when nothing is left: a verify-only run completed it with two PARSED papers never screened."),
 ("W1-F3-C3", "W1-F3", "download.download_papers, _download_one", "iv", "code order", "1c", "P4", "extends B-F10 / D20", "Y code",
  "Which of five strategies or URLs supplied a PDF, its hash and the cause of a failure are printed only; acquisition_date is the batch-start time for every paper."),
 ("W1-F3-C4", "W1-F3", "openalex.search_openalex", "i / iv", "code order (the difference); conditional (its effect — OpenAlex's default operator)", "1c", "P3", "new", "Y code",
  "OpenAlex gets the query terms space-joined with a hard-coded type=article|review filter; PubMed gets them joined with \" AND \". The methods text reports one query for both and no filter."),
 ("L05-C1", "L05", "review_spec.Eligibility._check, StagePolicy; eligibility_render.decision_instruction, edge_case_guidance", "i (v)", "reproduced", "1c", "P4", "new", "Y code+rerun",
  "Every model stage must declare a policy, but only abstract_primary renders one; the FT adjudication sheet renders ft_primary's policy while the FT prompt carries none. Latent on live, where ft_primary declares only absence_is_evidence."),
 ("W1-F4-C1", "W1-F4", "methods_section.generate_methods_section; prisma.generate_prisma_flow", "i / iv", "reproduced", "1", "P3", "extends B-F07 and B-F06", "Y code+rerun",
  "A review entered through import_extraction_entry (screened elsewhere, zero screening rows, no model call) exports a methods text stating a PubMed/OpenAlex search and model screening, and PRISMA reports the imported papers as screened, retrieved and assessed; validate_prisma_counts returns valid. Not reachable on live."),
 ("W1-F4-C2", "W1-F4", "run_pipeline.run_pipeline (`except Exception` / `except RunAborted`) with run_manifest.close_run", "iii", "reproduced", "1", "P2 (a reader could call it 2)", "new (the failed-path twin of C47's interrupt rollback); kin to L04-C4", "Y code+rerun",
  "The `failed` close commits on the run's one connection without a rollback, so a failing stage's open transaction is committed with it: with add_papers raising mid-batch, 2 of 4 papers were committed under a failed manifest; the same half-write under KeyboardInterrupt leaves 0. The one known writer is add_papers; its realistic trigger was not observed."),
 ("L04-C2", "L04", "run_manifest.git_state → open_run DirtyTree", "ii / i", "reproduced", "1c", "P2", "new", "Y code+rerun",
  "No return code is read for `git rev-parse` or `git status --porcelain`. A failing `git status` (corrupt index) gives dirty=False with a valid commit, so an edited tree opens a run recorded as clean."),
 ("W1-F2-C1", "W1-F2", "migrations/runner.run", "i", "reproduced", "1", "P6", "new (related S9)", "Y code+rerun",
  "include_data is one boolean with no freshness test and no migration id: run(<fresh>, include_data=True) executes data migrations on a fresh database, and --apply-pending --include-data executes every unreceipted data migration (002, 003, 017), not one named. 003's source is an absolute path to the surgical_autonomy corpus. 'Never executed on a fresh database' is false in the runner docstring, the CLI help, the README and CLAUDE.md. Live is not exposed (21 receipts)."),
 ("W1-F2-C2", "W1-F2", "migrations/runner.receipts (check_drift, run)", "ii", "reproduced (receipts / check_drift); conditional inside run", "1c", "P2", "extends B-F02(b)", "Y code+rerun",
  "`except sqlite3.OperationalError: return {}` turns any read fault into 'no receipts'. Under a lock check_drift returned [] despite real drift; inside run that means fresh=True. Bears on this ruling's assumption A1 (an applied migration's text cannot change without the runner refusing)."),
 ("L01-C2 + L06-C1", "L01, L06", "events.write_field_event (R20 branch); effective._governing_reviewer / effective_value rows 2, 4, 5", "i (v)", "reproduced", "1", "P5", "new", "Y code+rerun",
  "The writer accepts a reviewer decision naming competing decisions but no claim; the reader then clears the row-2 conflict and returns the machine's value (a CORRECT to \"7\" reads back as \"5\") with no reviewer key in provenance. No production writer of reviewer events exists yet; reachable with the human importer."),
 ("L06-C2", "L06", "effective.effective_value rows 6 / 8; events.write_field_event", "i", "reproduced on a writer-built history; conditional for live", "1c", "P5", "new", "Y code+rerun",
  "Row 6's 'ACCEPT is refused at write' is false for one claim id holding two differing asserted values (the writer counts claim ids): the reader skips row 6 and endorses the newest value."),
 # ---- Class 2 ---------------------------------------------------------------------------
 ("L06-C3", "L06", "effective.effective_value row 3 exit; _governing_reviewer", "i", "reproduced", "2", "P5", "new", "Y code+rerun",
  "Row 3's printed exit ('a new decision against the current claim') lands in row 2 unless the new decision repeats the stale one's type and value or lists it in against_decisions. Loud, but the exit does not exit."),
 ("L04-C4", "L04", "run_manifest.open_run savepoint tail (also close_run, record_call)", "iii", "reproduced (mechanism); code order for open_run", "2", "P5", "new (family of C50); kin to W1-F4-C2", "Y code+rerun",
  "RELEASE open_run then `if conn.in_transaction: conn.commit()` commits a caller's open transaction, against the 'nests inside a caller's open transaction' comment. No current production caller holds a transaction there."),
 ("L04-C5", "L04", "run_manifest.pin_tuple, open_run, open_review_session", "other", "conditional (a caller passes arms=; none does today)", "2", "P5", "extends C45", "Y code",
  "An arm named with zero stages is pinned to \"stages\": {} plus the codebook hash: a review session naming a model arm either refuses or pins it so extraction refuses forever."),
 ("L05-C5", "L05", "review_spec.Eligibility._check, VerifierTest; eligibility_render.absent_abstract_fallback, verifier_tests_block", "other / i", "reproduced", "2", "P4", "new", "Y code+rerun",
  "A verifier stage with zero tests loads and renders 'Apply these tests strictly:' with nothing (silent). A spec with no insufficient_data criterion loads and raises ValueError at the first abstract-less paper."),
 ("L05-C7", "L05", "codebook._validate_valid_values, _validate_top_level, _parse", "other", "reproduced", "2", "P1", "new", "Y code+rerun",
  "Unquoted Yes/No in valid_values load as booleans and raise TypeError later in prompt building; an unquoted sentinel NO or null silently becomes \"False\" or \"None\". The live codebook quotes everything."),
 ("L07-C5", "L07", "pdf_parser._REROUTE, _next_parser; parse_quality.CRITERIA", "i", "code order", "2", "P4", "new", "Y code",
  "FONT_EXPOSURE has no re-route entry, so an exposure-only failure is excluded without OCR being tried."),
 ("L07-C8", "L07", "pdf_parser.strip_links_to_temp and its call in parse_pdf", "ii", "code order", "2", "P4", "extends C56", "Y code",
  "If the link-strip raises, the cascade aborts before PyMuPDF and the temp PDF leaks, against 'never outlive the call'."),
 ("L07-C9", "L07", "pdf_parser.parse_pdf initial scanned and digital-sparse routes", "i", "code order", "2", "P4", "new", "Y code",
  "vision_max_pages is enforced only on re-routes: a scanned PDF over the OCR cap skips OCR and is sent whole to vision."),
 ("L07-C10", "L07", "pdf_parser.parse_all_pdfs no-PDF branch", "ii", "code order", "2", "P4", "extends D20", "Y code",
  "A missing PDF is a log line plus stats['failed'], with no event even under a run, and is retried silently every run."),
 ("W1-F2-C3", "W1-F2", "migrations/runner.run", "iii", "code order", "2", "P6", "new (adjacent to C11)", "Y code",
  "The migration commits on its own connection and the receipt is a second transaction outside the MigrationError wrapper: a failure between them leaves the migration applied with no receipt."),
 ("W1-F3-C5", "W1-F3", "ft_screener.run_ft_screening", "ii (inverse) / other", "reproduced", "2", "P4", "new", "Y code+rerun",
  "An out-of-vocabulary reason code raises ValueError inside the paper transaction, outside the malformed-output handler, so the whole run closes failed; the paper stays first in line and an identical rerun failed the same way."),
 ("W1-F3-C6", "W1-F3", "download.download_papers", "i", "code order", "2", "P4 (3 if docstring only)", "new (adjacent to D20)", "Y code",
  "'All included papers with PDF URLs or DOIs' actually selects every non-terminal status, including INGESTED and ABSTRACT_SCREEN_FLAGGED, with no identifier test."),
 ("W1-F5-C2", "W1-F5", "ollama_lock.check_experiment_lock, foreign_lock_held; ollama_client._restart_ollama_and_retry", "i", "code order", "2", "P2", "extends A1 (same gate, different mechanism)", "Y code",
  "The restart gate is a probe released immediately, not a hold across the restart: a process that takes the lock after the probe is restarted beneath, with no refusal on either side."),
 # ---- Class 3 (rest on the reader's quote; not re-derived by the lead) ---------------------
 ("L01-C5", "L01", "extraction_events.write_extraction_events tail, outcome_for_exception fit mapping", "iv", "code order", "3", "P7", "new", "N", "The R130 telemetry row always says attempt=1 and files the count as pass1_prompt_eval_count whichever call was truncated."),
 ("L01-C6", "L01", "batch across eight L01 modules", "i", "code order", "3", "P7", "item 1 extends A-11; item 8 touches RB-7; rest new", "N", "Eight wording or dead-code items; none changes a stored value."),
 ("L02-C8", "L02", "elicitation prompts._sentinel_rule", "i", "code order", "3", "P7 (prompt text: pin-affecting if changed)", "new", "N", "The rule hard-codes \"NR\" and ignores its sentinels argument."),
 ("L02-C9", "L02", "materialize.contiguous_runs / source_snippet", "i", "reproduced", "3", "P7, or fold into INT-g12-1", "extends INT-g12-1", "N (lead reran the reproducer)", "'First contiguous run' is the lowest-numbered run after a sort, not the first cited: cited (47, 48, 12) stores unit 12."),
 ("L02-C10", "L02", "contracts._resolve_indices, parse_container, _entries", "i", "reproduced (scalar) + code order", "3", "P7", "new", "N (lead reran the reproducer)", "Shape repairs go unrecorded despite 'no silent repair anywhere'."),
 ("L02-C11", "L02", "prompts.build_pass2_priming_message docstring", "i", "code order", "3", "P7", "new", "N", "The priming message is nested inside the legacy wrapper rather than replacing the trace message."),
 ("L02-C12", "L02", "pipeline.elicit, prompts.build_feedback_block, build_pass1_prompt", "i", "code order", "3", "P7", "new (the lost losing attempt is R571(e))", "N", "Batch: losing attempt's raw content not persisted; unparseable feedback says FIELD_MISSING ×N; a stale prompt sentence; dead tier defaults."),
 ("L02-C13", "L02", "agents/models.EvidenceSpan.clamp_confidence", "ii / i", "code order", "3", "P7", "new", "N", "Out-of-range confidence is silently clamped and stored; the schema description contradicts the strict contract."),
 ("L03-C6", "L03", "audit_events.audit_run; auditor.semantic_verify", "ii", "code order", "3", "P7", "new (neighbours B23-EXP)", "N", "An unparseable or empty auditor response is stored as verdict `flagged`."),
 ("L03-C7", "L03", "distribution_monitor.check_distribution, main", "iv / i", "code order", "3", "P7", "new", "N", "Two codebooks are read; the CLI's --codebook is half-honoured."),
 ("L03-C8", "L03", "audit_events docstring / audit_run", "i", "conditional", "3", "P7 (2 if a refusal is wanted)", "new", "N", "'Cross-family' verification is stated but nothing compares the audit model with the arm's model."),
 ("L03-C9", "L03", "distribution_monitor, auditor", "i", "code order", "3", "P7", "new", "N", "Hygiene batch (truncating log count, stale docstrings, unused names)."),
 ("L04-C6", "L04", "run_manifest docstring / record_active_ollama_call", "i / iv", "code order", "3", "P7", "extends B-F14", "N", "run_calls is one row per ollama_chat call, not per request sent."),
 ("L04-C7", "L04", "run_manifest.git_state (state_tags)", "i", "code order", "3", "P7", "new (E-STATE residue)", "N", "A lightweight tag sets engine_state though the docstring says annotated."),
 ("L04-C8", "L04", "effective_config.stage_config, _format", "i", "code order", "3", "P7", "new", "N", "stage_config(\"ft_screen_primary\", None) raises AttributeError despite the docstring."),
 ("L04-C9", "L04", "run_manifest._RUN_FIELD_NAMES", "i", "conditional (two databases in one process)", "3", "P7", "new", "N", "Registry keyed by run_id alone, which is per database."),
 ("L05-C6", "L05", "review_spec.load_review_spec; codebook._parse", "i", "reproduced", "3", "P7 (1 if a dead declaration counts)", "new", "N (lead reran the reproducer)", "A YAML key declared twice loads in both the spec and the codebook, last wins."),
 ("L05-C8", "L05", "codebook._parse vs compute_codebook_sha256", "i / iv", "reproduced", "3", "P2", "new", "N (lead reran the reproducer)", "Codebook.sha256 hashes decoded text, not the file bytes; differs on a CRLF file."),
 ("L05-C9", "L05", "review_spec.ScreeningModels defaults", "i", "code order", "3", "P7", "new", "N", "Declared defaults pair two qwen models against the cross-family rule; live declares both explicitly."),
 ("L05-C10", "L05", "review_paths._validate_requested_id", "i", "reproduced", "3", "P7", "new", "N (lead reran the reproducer)", "re.match with `$` accepts \"id\\n\"."),
 ("L06-C4", "L06", "effective._classify_claim, iter_grid", "i", "reproduced (row number)", "3", "P7", "new (also seen by L01, L03, L05, W1-F1 as an adjacent item)", "N (lead reran the reproducer)", "The sentinel test is exact membership, not Codebook.is_absence_sentinel: 'nr' or ' NR' reads row 11 instead of 13, and that rule_row is exported. Value and state are unaffected."),
 ("L06-C5", "L06", "corpus.py docstring", "i", "code order", "3", "P7", "extends C13", "N", "'Exactly one permitted consumer' is two; R42's retirement of analysis/eval/schema_eval2.py has not happened."),
 ("L06-C6", "L06", "effective.py docstrings", "i", "code order", "3", "P7", "new (INT-g12-3 family)", "N", "Four drifted sentences; load_absence_sentinels has no caller."),
 ("L07-C11", "L07", "pdf_parser.parse_pdf digital sparse fall-through", "iv", "code order", "3", "P7", "new", "N", "A sparse Docling result leaves no ledger row; OCR can be re-run and use up an attempt."),
 ("L07-C12", "L07", "font_audit.audit, _font_rows", "iv", "conditional", "3", "P7", "new", "N", "Program facts can come from the wrong same-named font; telemetry only."),
 ("L07-C13", "L07", "pdf_parser.verify_hashes", "i", "code order", "3", "P7", "new", "N", "Takes an arbitrary asset row and resolves the path against cwd."),
 ("L07-C14", "L07", "pdf_parser.parse_all_pdfs, parse_pdf", "i", "code order", "3", "P7", "new", "N", "skipped_existing never incremented; `or \"docling\"` under a 'never the literal docling' comment; arbitrary glob pick."),
 ("W1-F1-C4", "W1-F1", "extractor._retry_snippet / _validate_and_retry_snippets", "iv / i", "reproduced", "3", "P1", "extends C56 (handler #4)", "N (lead reran the reproducer)", "An answered snippet-retry call that is not a JSON object is `completed` in run_calls yet missing from every claim's context_chain."),
 ("W1-F1-C5", "W1-F1", "extractor._extract_selected / record_selection_refusals", "iv", "code order", "3", "P7", "new (also seen by L01)", "N", "Failure events hard-code stage_name ('extract_pass2' for every exception; 'extract_pass1' for text refusals)."),
 ("W1-F1-C7", "W1-F1", "extractor.py, several", "i", "code order", "3", "P7 (item a with A-11 in P1)", "(a), (d) extend A-11; rest new", "N", "Seven stale statements or counts. Also: row C41's text is stale — `_with_think` is no longer in extractor.py."),
 ("W1-F2-C4", "W1-F2", "database.ReviewDatabase.update_status", "ii (masked)", "reproduced", "3", "P7", "extends the 12f A-7 section", "N (lead reran the reproducer)", "When BEGIN IMMEDIATE fails on a lock the handler issues ROLLBACK with no transaction open; the caller sees 'cannot rollback', not 'database is locked'."),
 ("W1-F2-C5", "W1-F2", "migrations/runner.discover, check_drift", "i", "reproduced", "3", "P7", "new", "N (lead reran the reproducer)", "Files not matching ^\\d{3}_[a-z0-9_]+\\.py$ are silently not migrations; a receipt whose file was deleted is not drift."),
 ("W1-F3-C7", "W1-F3", "ft_screener and screener flag branches", "ii", "code order", "3", "P7", "extends A14 / 12f A-6(e)", "N", "The cause of a flag lives only in a log line."),
 ("W1-F3-C8", "W1-F3", "ft_screener checkpoint helpers", "iii", "code order", "3", "P7", "extends B-F13(c)", "N", "Same id-only, non-atomic checkpoint as the abstract screener; survives a failed run into the next manifest."),
 ("W1-F3-C9", "W1-F3", "download, openalex, screener, ft_screener, dedup", "i / other", "code order", "3", "P7", "new", "N", "Batch: dead iteration; two hard-coded personal emails; discarded confidence; unused arguments."),
 ("W1-F3-C10", "W1-F3", "openalex._parse_work", "other", "conditional", "3", "P3 (1 if the condition holds)", "extends B-F04", "N", "PMID and DOI are normalised by one exact literal prefix each."),
 ("W1-F4-C3", "W1-F4", "run_pipeline.run_pipeline finally, main", "i / other", "code order", "3", "P7", "new", "N", "Every exit logs 'PIPELINE COMPLETE'; a BLOCKED gate stop exits 0."),
 ("W1-F4-C4", "W1-F4", "run_pipeline.main with background.maybe_background", "i", "code order", "3", "P7", "new", "N", "With --background the logs directory is created from an unvalidated argv pre-scan before the spec check."),
 ("W1-F5-C3", "W1-F5", "ollama_client._check_input_was_read, ollama_chat._finish", "i / iv", "conditional (the runtime omits prompt_eval_count)", "3", "P7 (1 only if the condition is shown)", "new", "N", "A response with no count skips both post-call checks and is recorded completed. Pinned as intended by a test."),
 ("W1-F5-C4", "W1-F5", "ollama_client.ollama_chat, _restart_ollama_and_retry", "ii / iv", "code order", "3", "P7", "extends B-F14", "N", "Four different endings are all recorded as 'timed out after N attempts + restart'."),
 ("W1-F5-C5", "W1-F5", "ollama_client.ollama_chat except arm", "other", "code order", "3", "P7", "new", "N", "The httpx.ConnectError arm is dead under ollama 0.6.1; only the log label is wrong."),
]

out = sys.argv[1]
n_new = 0
table = []
for ids, lot, where, pat, ev, cls, pkg, mapping, rd, stmt in ROWS:
    pid = ""
    if mapping.startswith("new") or mapping.startswith("(a) extends") :
        n_new += 1
        pid = "INT-i12-%d" % n_new
    table.append({"ids": ids, "lot": lot, "where": where, "pattern": pat, "evidence": ev, "class": cls,
                  "package": pkg, "mapping": mapping, "proposed_id": pid, "rederived": rd, "statement": stmt})
reader_ids = [x.strip() for r in table for x in r["ids"].split("+")]
assert len(reader_ids) == len(set(reader_ids)), "a reader ID appears twice"
json.dump({"rows": table, "reader_candidates": len(reader_ids)}, open(os.path.join(out, "synthesis.json"), "w"), indent=1)
esc = lambda s: s.replace("|", "\\|")
with open(os.path.join(out, "candidates_table.md"), "w", encoding="utf-8") as f:
    f.write("| # | reader ID(s) | lot | file / function | pattern | evidence level | class / package | existing row, or new (proposed ID) | re-derived by lead | statement |\n")
    f.write("| ---: | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n")
    for i, r in enumerate(table, 1):
        cls = {"1": "1", "1c": "1 (conditional / latent / weak)", "2": "2", "3": "3"}[r["class"]]
        m = r["mapping"] + (" — **%s**" % r["proposed_id"] if r["proposed_id"] else "")
        f.write("| %d | %s | %s | %s | %s | %s | %s / %s | %s | %s | %s |\n" % (
            i, r["ids"], r["lot"], esc(r["where"]), r["pattern"], esc(r["evidence"]), cls, esc(r["package"]), esc(m),
            r["rederived"], esc(r["statement"])))
c = Counter(r["class"] for r in table)
newc = Counter(r["class"] for r in table if r["proposed_id"])
pk = Counter((r["class"], r["package"].split(" ")[0].rstrip(",")) for r in table)
with open(os.path.join(out, "counts.md"), "w", encoding="utf-8") as f:
    f.write("| proposed class | rows after merging | of which new (proposed INT-i12-n) | of which extend or duplicate an existing row |\n| --- | ---: | ---: | ---: |\n")
    for k, label in (("1", "1"), ("1c", "1, conditional / latent / weak"), ("2", "2"), ("3", "3")):
        f.write("| %s | %d | %d | %d |\n" % (label, c[k], newc[k], c[k] - newc[k]))
    f.write("| all | %d | %d | %d |\n\n" % (len(table), sum(newc.values()), len(table) - sum(newc.values())))
    f.write("Reader candidates: %d, in %d rows after %d merges. Class 1 / 2 rows: %d, every one marked re-derived by the lead (%d by code and rerun, %d by code).\n\n" % (
        len(reader_ids), len(table), len(reader_ids) - len(table),
        c["1"] + c["1c"] + c["2"],
        sum(1 for r in table if r["class"] != "3" and "rerun" in r["rederived"]),
        sum(1 for r in table if r["class"] != "3" and "rerun" not in r["rederived"])))
    f.write("By package (first-named), Class 1 and 1-conditional / Class 2 / Class 3: " + "; ".join(
        "%s %d / %d / %d" % (p, pk[("1", p)] + pk[("1c", p)], pk[("2", p)], pk[("3", p)])
        for p in ("P1", "P2", "P3", "P4", "P5", "P6", "P7")) + "\n")
assert all(r["rederived"].startswith("Y") for r in table if r["class"] != "3")
print(open(os.path.join(out, "counts.md")).read())
