# Module Inventory

Complete inventory of every Python file under `engine/`, `scripts/`, `analysis/`, and `tests/` with purpose, key functions/classes, and dependencies.

---

## engine/core/ — Core Infrastructure

### `constants.py`
**Purpose:** Shared constants used across agents and validators.
- `INVALID_SNIPPET_RE` — Regex for detecting invalid snippets (ellipsis bridging, Unicode U+2026)
- `FT_REASON_CODES` — Tuple of 7 FT screening exclusion codes: eligible, wrong_specialty, no_autonomy_content, wrong_intervention, protocol_only, duplicate_cohort, insufficient_data
- `FT_MAX_TEXT_CHARS = 32_000` — Character budget for FT screening prompts

### `database.py`
**Purpose:** SQLite state machine — one database per review with full provenance.
- `ReviewDatabase` — Main class. Creates `data/{review}/` with subdirs: pdfs/, parsed_text/, vector_store/. WAL mode, 5s busy timeout, foreign keys
- `STATUSES` — 15 paper lifecycle states (see state-machine.md)
- `ALLOWED_TRANSITIONS` — State machine transition rules
- **Key methods:** `add_papers()`, `update_status()`, `reject_paper()`, `admin_reset_status()`, `add_screening_decision()`, `add_verification_decision()`, `add_ft_screening_decision()`, `add_ft_verification_decision()`, `add_extraction()`, `add_extraction_atomic()`, `add_evidence_span()`, `update_audit()`, `get_stale_extractions()`, `get_pipeline_stats()`, `cleanup_orphaned_spans()`, `reset_for_reaudit()`, `reset_for_reextraction()`, `min_status_gate()`
- **Tables:** papers (24 columns), abstract_screening_decisions, abstract_verification_decisions, ft_screening_decisions, ft_verification_decisions, full_text_assets, extractions (with model_digest, auditor_model_digest, low_yield), evidence_spans, review_runs

### `naming.py`
**Purpose:** Canonical artifact filename generation for human review round-trips.
- `REVIEW_STAGES` — Dict mapping stage names to display labels (5 stages)
- `review_artifact_filename(review_name, stage, direction, ext)` — Returns `{review}_{stage}_{direction}.{ext}`
- `review_artifact_path(data_dir, review_name, stage, direction, ext)` — Full path

### `review_spec.py`
**Purpose:** Pydantic v2 models for Review Spec YAML + protocol hashing.
- **Models (13):** `PICO`, `SearchStrategy`, `ScreeningModels`, `FTScreeningModels`, `ScreeningCriteria`, `SpecialtyScope`, `PDFParsing`, `CloudModelConfig`, `CloudModels`, `DistributionMonitorConfig`, `PDFQualityCheck`, `ExtractionField`, `ExtractionSchema`, `ReviewSpec`
- `ExtractionField` — name, description, type, tier (1–4), enum_values
- `ExtractionSchema` — fields list, validator (≥1 tier-1 field), `fields_by_tier(tier)`
- `ReviewSpec` — Top-level: title, version, authors, date, pico, search_strategy, screening_models, ft_screening_models, screening_criteria, extraction_schema, specialty_scope, low_yield_threshold (default 4), auditor_model, unpaywall_email, pdf_quality_check, cloud_models, pdf_parsing, distribution_monitor
- `screening_hash()` / `extraction_hash()` — SHA-256 of canonical JSON for staleness detection
- `load_review_spec(path)` — YAML loader with `ReviewSpecError` on failure

---

## engine/search/ — Literature Search

### `models.py`
**Purpose:** Search data models.
- `Citation` — pmid, doi, title, abstract, authors, journal, year, source (Literal["pubmed", "openalex"]), raw_data

### `pubmed.py`
**Purpose:** PubMed search via Biopython Entrez.
- `search_pubmed(spec)` — ESearch + EFetch in batches of 500. Rate limit 0.34s (3 req/s). 3 retries with exponential backoff
- `_build_query(spec)` — AND-joined query terms + date_range `[dp]` filter
- `_parse_record(rec)` — MEDLINE record → Citation

### `openalex.py`
**Purpose:** OpenAlex search via pyalex.
- `search_openalex(spec)` — Cursor pagination (200/page), type="article|review" filter, retry on HTTP errors
- `reconstruct_abstract(inverted_index)` — Reassemble from OpenAlex inverted index
- `_parse_work(work)` — OpenAlex Work → Citation

### `dedup.py`
**Purpose:** Two-phase deduplication.
- `deduplicate(pubmed, openalex)` — Phase 1: exact (DOI, PMID, normalized title). Phase 2: fuzzy (SequenceMatcher > 0.9). Returns `DedupResult` with unique_citations, duplicate_pairs, stats
- `normalize_title(title)` — Lowercase, strip punctuation, collapse whitespace
- `title_similarity(t1, t2)` — SequenceMatcher ratio

---

## engine/agents/ — AI Agents

### `models.py`
**Purpose:** Shared Pydantic models for agents.
- `EvidenceSpan` — field_name, value, source_snippet, confidence (0–1, clamped), tier (1–4)
- `ExtractionResult` — paper_id, fields (list[EvidenceSpan]), reasoning_trace, model, extraction_schema_hash, extracted_at
- `ExtractionOutput` — Ollama structured JSON schema for Pass 2

### `screener.py`
**Purpose:** Dual-model abstract screening.
- `DEFAULT_PRIMARY_MODEL = "qwen3:8b"`, `DEFAULT_VERIFICATION_MODEL = "qwen3:32b"` (engine default: gemma3:27b via review spec)
- `ScreeningDecision` — decision (include/exclude), rationale, confidence
- `screen_paper(paper, spec, pass_number, model, role)` — Single screening call. Role-aware: primary sees simplified exclusions, verifier sees full criteria
- `run_screening(db, spec)` — Dual-pass primary. Both include → IN; both exclude → OUT; disagree → FLAGGED
- `run_verification(db, spec)` — Single verification pass on SCREENED_IN. Auto-advances ABSTRACT_SCREENING_COMPLETE

### `ft_screener.py`
**Purpose:** Full-text screening with specialty scope.
- `FTScreeningDecision` — decision (FT_ELIGIBLE/FT_EXCLUDE), reason_code, rationale, confidence
- `FTVerificationDecision` — decision (FT_ELIGIBLE/FT_FLAGGED), rationale, confidence
- `truncate_paper_text(full_text, title, abstract, max_chars)` — Section-aware truncation to 32K chars
- `build_ft_screening_prompt(paper_text, spec)` — PICO + criteria + 7 reason codes + /no_think
- `build_ft_verification_prompt(paper_text, spec)` — 5-test FP catcher + /no_think
- `ft_screen_paper()` / `ft_verify_paper()` — Single-paper calls
- `run_ft_screening(db, spec, review_name)` — Primary FT screen. Papers without parsed text → FT_FLAGGED with reason `no_parsed_text`. Status-aware: papers past FT gate record decision without status change
- `run_ft_verification(db, spec, review_name)` — Verification pass. Auto-advances FULL_TEXT_SCREENING_COMPLETE

### `extractor.py`
**Purpose:** Two-pass codebook-driven extraction.
- `MODEL = "deepseek-r1:32b"`, `MAX_RETRIES = 2`, `RETRY_DELAY = 30`, `SNIPPET_MAX_RETRIES = 2`, `RESTART_EVERY_N = 25`
- `build_extraction_prompt(paper_text, spec, codebook_path)` — Codebook-driven prompt with tier labels, field blocks, source_quote_required flag
- `extract_pass1_reasoning(prompt)` — Free reasoning with `<think>` tags
- `extract_pass2_structured(prompt, reasoning_trace, spec, paper_id)` — Grammar-constrained JSON
- `extract_paper(paper_id, paper_text, spec, db, model_digest, auditor_model_digest)` — Full two-pass + snippet validation + atomic storage
- `run_extraction(db, spec, review_name, restart_every)` — Batch pipeline with pre-flight, staleness check, proactive restart

### `auditor.py`
**Purpose:** Cross-model grep + semantic audit.
- `DEFAULT_AUDITOR_MODEL = "gemma3:27b"`, `SEMANTIC_ONLY_TIERS = {4}`
- `_ABSENCE_VALUES = {"NOT_FOUND", "Not discussed", "NR", "No comparison reported", "Not assessable"}`
- `grep_verify(source_snippet, paper_text)` — Normalized exact + fuzzy (>0.85) substring match
- `semantic_verify(span, paper_text, field_type, model, ollama_options)` — LLM verification. Categorical: "Does source support this classification?" Text: "Does value match snippet?"
- `audit_span(span_data, paper_text, field_type, field_tier, model, ollama_options)` — 4-state audit: absence → auto-verify; invalid snippet → invalid_snippet; tier 4 → semantic-only; others → grep then semantic
- `check_low_yield(db, threshold)` — Flags papers with < threshold populated fields
- `run_audit(db, review_name, spec, model)` — Batch audit. Builds field type/tier lookup. Post-audit LOW_YIELD detection

---

## engine/acquisition/ — PDF Acquisition

All acquisition modules use `ReviewDatabase` as a context manager. Terminal statuses (ABSTRACT_SCREENED_OUT, REJECTED, PDF_EXCLUDED, FT_SCREENED_OUT) are filtered from all queries.

### `check_oa.py`
**Purpose:** Unpaywall OA status check (1 req/sec rate limit).
- `check_oa_status(review_name, spec_path)` — Updates papers: oa_status, pdf_url, acquisition_date

### `download.py`
**Purpose:** 5-strategy cascade PDF downloader.
- Strategies: Unpaywall direct → PMC OA tar.gz → IEEE stamp.jsp → MDPI → DOI redirect
- `is_valid_pdf(path)` — %PDF magic byte check
- `download_papers(review_name, retry_failed)` — Main orchestrator. Invalid PDFs quarantined

### `verify_downloads.py`
**Purpose:** Match, validate, rename to canonical format.
- `canonical_filename(ee_identifier, authors_json, year)` — `EE-{nnn}_{Author}_{Year}.pdf`
- `verify_downloads(review_name, pdf_dir, dry_run)` — 3 matching patterns (bare int, EE-prefix, rich name). Validates %PDF + ≥10 KB + no HTML

### `manual_list.py`
**Purpose:** Legacy manual download list generator (superseded by `pdf_quality_html.py --mode acquisition`).

### `pdf_quality_check.py`
**Purpose:** AI first-page classification.
- `run_quality_check(review_name, dry_run, limit, config)` — Renders page 0 → base64 PNG → qwen2.5vl:7b → JSON classification (language, content_type, confidence)

### `pdf_quality_html.py`
**Purpose:** Interactive HTML review for acquisition + quality check (two modes).
- `generate_acquisition_html(review_name, output_path)` — Papers without PDFs, disposition dropdowns
- `generate_quality_html(review_name, mode, output_path)` — Three-section collapsible HTML. localStorage draft saves, keyboard navigation, JSON export

### `pdf_quality_import.py`
**Purpose:** Import HTML/JSON dispositions.
- `validate_disposition_json(data, conn)` — Returns error list
- `import_dispositions(review_name, input_path, dry_run)` — PROCEED → HUMAN_CONFIRMED; EXCLUDE_* → PDF_EXCLUDED (terminal)

---

## engine/adjudication/ — Workflow & Human Gates

### `workflow.py`
**Purpose:** 12-stage sequential workflow state machine.
- 12 stages from ABSTRACT_SCREENING_COMPLETE to AUDIT_REVIEW_COMPLETE
- `ensure_workflow_table(conn)` — Creates + seeds workflow_state table
- `complete_stage()`, `bypass_stage()`, `reset_stage()`, `advance_stage()`, `can_advance_to()`, `get_current_blocker()`, `format_workflow_status()`

### `advance_stage.py`
**Purpose:** CLI for manual workflow advancement.
- `main()` — `--review`, `--stage`, `--note`, `--force`, `--status`

### `schema.py`
**Purpose:** DDL for 3 adjudication tables + workflow_state.
- `abstract_screening_adjudication`, `ft_screening_adjudication`, `audit_adjudication`

### `categorizer.py`
**Purpose:** Rule-based FP pattern categorization.
- `CategoryConfig` — Loaded from `adjudication_categories.yaml`. Regex patterns + keyword matching + exclude_if_also logic
- `categorize_paper(title, abstract, config)` — Returns category name or 'ambiguous'

### `screening_adjudicator.py`
**Purpose:** Abstract screening adjudication export/import.
- Malformed JSONL lines logged at WARNING with line number and content (not silently skipped)
- `export_adjudication_queue(db, output_path, ...)` — HTML or xlsx with auto-categorization
- `import_adjudication_decisions(db, input_path)` — .json or .xlsx. INCLUDE/EXCLUDE → status update + adjudication table. Auto-advances workflow stages
- `check_adjudication_gate(db)` — Count of unresolved ABSTRACT_SCREEN_FLAGGED

### `abstract_adjudication_html.py`
**Purpose:** Interactive HTML for abstract adjudication.
- Card-based layout, category badges, batch actions, keyboard shortcuts (I/E/↑↓), localStorage, JSON export

### `ft_screening_adjudicator.py`
**Purpose:** FT screening adjudication export/import.
- `export_ft_adjudication_queue(db, output_path, ...)` — HTML or xlsx with reason codes
- `import_ft_adjudication_decisions(db, input_path)` — FT_ELIGIBLE/FT_SCREENED_OUT. Auto-advances FULL_TEXT_ADJUDICATION_COMPLETE
- `check_ft_adjudication_gate(db)` — Count of unresolved FT_FLAGGED

### `ft_adjudication_html.py`
**Purpose:** Interactive HTML for FT adjudication.
- Table-based, reason code grouping, localStorage, JSON export

### `audit_adjudicator.py`
**Purpose:** Extraction audit review per-span export/import.
- `_collect_papers_for_review(db, spot_check_pct, spot_check_failure_threshold)` — Contested/flagged/invalid_snippet spans + LOW_YIELD papers + 10% spot-check sample
- `export_audit_review_queue(db, output_path, ...)` — Per-span rows. Auto-advances AUDIT_QUEUE_EXPORTED
- `import_audit_review_decisions(db, input_path)` — ACCEPT/REJECT/CORRECT per span. Two-pass validation. Auto-advances AUDIT_REVIEW_COMPLETE
- `check_audit_review_gate(db)` — Count of unresolved AI_AUDIT_COMPLETE papers

---

## engine/parsers/ — PDF Processing

### `models.py`
**Purpose:** Parser data models.
- `ParsedDocument` — paper_id, source_pdf_path, pdf_hash, parsed_markdown, parser_used (Literal["docling", "pymupdf", "qwen2.5vl"]), parsed_at, version

### `pdf_parser.py`
**Purpose:** Three-tier PDF → Markdown parser.
- `compute_pdf_hash(pdf_path)` — SHA-256 of file
- `is_scanned_pdf(pdf_path, threshold)` — Heuristic: chars/page < threshold
- `parse_with_docling(pdf_path)` — DocumentConverter → Markdown
- `parse_with_pymupdf(pdf_path)` — PyMuPDF page.get_text("text") + `<!-- Page N -->` separators
- `parse_with_vision(pdf_path, vision_model)` — 200 DPI page rendering → base64 → vision model OCR
- `parse_pdf(pdf_path, paper_id, review_name, db, spec)` — Three-tier routing with hash caching + atomic temp-file-then-rename write. Raises `ValueError` if all parsers return empty text
- `parse_all_pdfs(db, review_name)` — Batch processing. DB-driven path resolution with glob fallback
- `verify_hashes(db)` — Check stored hashes vs current files

---

## engine/cloud/ — Cloud Concordance Arms

### `base.py`
**Purpose:** Shared base class for cloud extractors.
- `CloudExtractorBase` — Prompt building (delegates to local extractor), response parsing (8+ alternate keys), atomic storage (rejects 0-span), progress tracking, distribution check integration

### `openai_extractor.py`
**Purpose:** OpenAI o4-mini extraction arm.
- `OpenAIExtractor(CloudExtractorBase)` — ARM="openai_o4_mini_high", reasoning_effort=high, json_object format. 3-attempt retry with exponential backoff

### `anthropic_extractor.py`
**Purpose:** Anthropic Sonnet 4.6 extraction arm.
- `AnthropicExtractor(CloudExtractorBase)` — ARM="anthropic_sonnet_4_6", extended thinking (budget=10K), max_tokens=16K. Markdown fence stripping, empty string → null

### `schema.py`
**Purpose:** Cloud extraction table DDL.
- `init_cloud_tables(db_path)` — Creates cloud_extractions + cloud_evidence_spans with migrations for notes column and NOT NULL constraints

---

## engine/analysis/ — Concordance Analysis

### `normalize.py`
**Purpose:** Canonical normalization for concordance scoring.
- Null synonyms → None; numeric → strip non-digits; categorical → prefix matching; multi-value → frozenset; free-text → lowercase + collapse whitespace

### `scoring.py`
**Purpose:** Field-pair scoring.
- `score_pair(field_name, value_a, value_b, spec)` — MATCH/MISMATCH/AMBIGUOUS. Categorical: exact only. Free-text: substring + Jaccard > 0.7. Sets: identical/disjoint/partial

### `metrics.py`
**Purpose:** Agreement statistics.
- `cohens_kappa(scores)` — Binary (MATCH/MISMATCH), AMBIGUOUS excluded from denominator. Fleiss 1981 SE with 95% CI
- `percent_agreement(scores)` — p(MATCH) among decisive pairs
- `field_summary(field_name, scores)` — Combined metrics

### `concordance.py`
**Purpose:** Multi-arm concordance pipeline.
- `load_arm(db_path, arm)` — Load from evidence_spans (local) or cloud_evidence_spans (cloud) or human_extractions (human_*). Raises `sqlite3.OperationalError` on DB errors (never returns empty dict on failure)
- `align_arms(arm_a, arm_b)` — Align by paper_id and field_name
- `run_concordance(db_path, arm_a, arm_b, spec_path)` — Full pipeline for one pair
- `run_all_pairs(db_path, arms, spec_path)` — All unique pairs

### `report.py`
**Purpose:** Concordance report generators.
- `print_summary(reports)` — Terminal table with tier sorting, mean categorical kappa
- `write_report(reports, output_dir)` — concordance_summary.csv, disagreements.csv, concordance_report.html (branded, kappa color-coding, expandable disagreements)

---

## engine/validators/ — Quality Gates

### `extraction_validator.py`
**Purpose:** Post-extraction field-level validation (read-only diagnostic).
- `validate_extraction(spec, paper_id, db)` — Checks unknown field names, invalid categorical values, non-numeric sample_size. Prefix shorthand supported
- `validate_all(spec, db, statuses)` — All eligible papers
- `detect_cross_field_bleed(spec, extraction_data)` — Flags values valid for wrong field
- `normalize_categorical_values(spec, paper_id, db)` — In-place canonical normalization

### `distribution_monitor.py`
**Purpose:** Post-extraction categorical field collapse detection.
- `check_distribution(db_path, review_name, arm, codebook_path, ...)` — Per-field: COLLAPSED / LOW_VARIANCE / OK
- `assert_no_collapse(results, strict)` — Raises `DistributionCollapseError` on COLLAPSED; strict also on LOW_VARIANCE
- `shannon_entropy(values)` — Shannon entropy H = -Σ(p × log₂(p))
- `run_post_extraction_check(...)` — Integrated check with skip conditions

---

## engine/exporters/ — Output Generation

### `review_workbook.py`
**Purpose:** Shared self-documenting Excel workbook builder.
- `create_review_workbook(output_path, rows, columns, decision_columns, instructions, ...)` — 3-sheet workbook: Instructions, Review Queue (with DataValidation dropdowns, conditional formatting), Reference. Atomic write

### `prisma.py`
**Purpose:** PRISMA 2020 flow diagram counts.
- `generate_prisma_flow(db)` — Comprehensive counts with reconciliation validation (terminal + in-progress = total)

### `evidence_table.py`
**Purpose:** Evidence table exports.
- `NO_EXTRACTION_MARKER = "[NO EXTRACTION DATA]"` — marks papers with missing extractions; `exclude_empty` flag omits them
- `export_evidence_csv(db, spec, output_path, min_status, exclude_empty)` — One row per paper with per-field value/snippet/confidence/audit_status. Atomic temp-file-then-rename
- `export_evidence_excel(db, spec, output_path, min_status, exclude_empty)` — 3-sheet: Evidence Table, Extraction Summary, Field Stats. Atomic temp-file-then-rename

### `docx_export.py`
**Purpose:** Publication-ready DOCX.
- `export_evidence_docx(db, spec, output_path, min_status)` — Landscape, 0.5" margins, Study + Year + Journal + extraction fields

### `methods_section.py`
**Purpose:** Auto-generated PRISMA methods paragraph.
- `generate_methods_section(db, spec)` — Queries actual DB models (not spec defaults). Covers search, screening, exclusion, extraction, audit

### `trace_exporter.py`
**Purpose:** Extraction reasoning trace analysis.
- `export_trace_quality_report(db_path, output_path)` — Trace length stats (min/max/mean/median/stdev), under-500, truncated detection

---

## engine/review/ — Human Review Interface

### `extraction_audit_html.py`
**Purpose:** Self-contained HTML for extraction audit.
- `generate_extraction_audit_html(review_name, output_path)` — Per-span interface with state badges (flagged/contested/invalid_snippet/low_yield/verified), localStorage draft, JSON export

### `human_review.py`
**Purpose:** CSV/JSON-based review queue.
- `export_review_queue(db, output_path, paper_ids, ...)` — Exports contested/flagged spans with parsed text context
- `import_review_decisions(db, csv_path, dry_run)` — Supports .csv and .json. ACCEPT/REJECT_VALUE/ACCEPT_CORRECTED/REJECT_PAPER

---

## engine/utils/ — Utilities

### `background.py`
**Purpose:** Detached tmux session launcher.
- `maybe_background(stage, review_name)` — Re-launches in tmux if `--background` flag present. Session name: `ee_{stage}_{timestamp}`. Tees output to log file

### `db_backup.py`
**Purpose:** Auto-backup before destructive operations.
- `auto_backup(db_path, reason)` — Timestamped backup: `{db_name}.bak-{reason}-{YYYYMMDD-HHMMSS}`

### `extraction_cleanup.py`
**Purpose:** Schema-hash-based stale extraction removal (dry-run default).
- `cleanup_stale_extractions(db, schema_hash, dry_run)` — Deletes mismatched extractions, cascade spans, resets EXTRACTED/AI_AUDIT_COMPLETE → PARSED. HUMAN_AUDIT_COMPLETE protected

### `ollama_client.py`
**Purpose:** Three-layer Ollama timeout wrapper.
- `ollama_chat(model, messages, ...)` — HTTP timeout + wall-clock timeout + restart recovery. Model-aware timeouts (8b:300s, 27b:600s, 32b:900s, 70b:1200s). Default 2 retries + 30s delay
- `get_model_digest(model_name)` — Exact model hash via `ollama.show()`

### `ollama_preflight.py`
**Purpose:** Pre-flight health check.
- `preflight_check(models, timeout)` — Load test + VRAM budget (100 GB limit)
- `check_ollama_env()` — Validates required systemd env vars (OLLAMA_FLASH_ATTENTION, OLLAMA_MAX_LOADED_MODELS)
- `require_preflight(models, runner_name)` — Orchestrator: env check → model check → abort on failure

### `progress.py`
**Purpose:** Batch progress reporting.
- `ProgressReporter` — Per-paper elapsed tracking, ETA calculation, summary stats (mean/median/max per-paper times)

---

## engine/migrations/ — Database Migrations

All migrations use selective error filtering: only "column/table already exists" `OperationalError` is swallowed; all other database errors re-raise.

### `002_screening_rename.py`
Renames SCREENED_* → ABSTRACT_SCREENED_*, screening_decisions → abstract_screening_decisions, adds FT workflow stages. Idempotent.

### `003_backfill_expanded_screening.py`
Imports expanded-corpus papers + screening/verification traces from CSV/JSONL files. DOI → PMID → title matching. Idempotent.

### `004_pdf_quality_check.py`
Adds PDF quality columns (pdf_exclusion_reason, pdf_exclusion_detail, pdf_quality_check_status, pdf_ai_language, pdf_ai_content_type, pdf_ai_confidence). Idempotent.

### `005_model_digest.py`
Adds model_digest + auditor_model_digest columns to extractions table. Idempotent.

---

## analysis/paper1/ — Paper 1 Analysis Layer

### `human_import.py`
**Purpose:** Parse human extractor workbooks (.xlsx v2).
- `parse_workbook(filepath)` — Reads "Extraction Form" sheet, maps 20 extraction fields + source quotes + notes
- `validate_workbook(rows, codebook_path, db_path)` — Paper ID format (EE-NNN), DB existence, categorical validity

### `consensus.py`
**Purpose:** Majority-vote consensus derivation.
- `identify_shared_papers(db_path, min_extractors)` — Papers with ≥ N distinct extractors
- `_majority_vote(values, field_type)` — Statuses: INSUFFICIENT, UNANIMOUS, MAJORITY, NO_CONSENSUS. Categorical/numeric: exact match. Free-text: whitespace-normalized cluster

### `adjudication.py`
**Purpose:** Export AMBIGUOUS concordance pairs for human review.
- `export_ambiguous_pairs(db_path, review_name, arms, codebook_path)` — Loads all arms, aligns, scores, collects AMBIGUOUS with source snippets

---

## scripts/ — Pipeline Runners & Utilities

### Pipeline Orchestration

| Script | Purpose | Key CLI Args |
|--------|---------|--------------|
| `run_pipeline.py` | Full pipeline orchestrator (search → export) | `--spec`, `--name`, `--skip-to` |
| `run5_extract_and_audit.py` | Extraction + audit for tmux | `--retry-failed`, `--restart-every`, `--paper-ids` |
| `screen_expanded.py` | Three-phase expanded screening | `--fetch-only`, `--screen-only`, `--verify-only` |
| `run_cloud_extraction.py` | Cloud concordance arms | `--arm`, `--max-cost`, `--progress`, `--dry-run` |
| `monitor_extraction.py` | Extraction watchdog (20 min poll) | `--review` |

### Re-screening / Remediation

| Script | Purpose | Key CLI Args |
|--------|---------|--------------|
| `reextract_all.py` | Full re-extraction (wipe + redo) | `--review`, `--spec` |
| `reextract_failed.py` | Retry EXTRACT_FAILED papers | `--review`, `--spec` |
| `rescreen_original_251.py` | Re-screen with updated criteria | `--review`, `--spec` |
| `rescreen_with_specialty.py` | Re-screen with specialty_scope | `--background`, `--force` |

### PDF / Parsing

| Script | Purpose | Key CLI Args |
|--------|---------|--------------|
| `parse_expanded_corpus.py` | Advance SCREENED_IN → PARSED | `--review` |
| `advance_to_pdf_acquired.py` | Bulk status advancement for PDFs on disk | `--review` |
| `backfill_authors.py` | Backfill missing author metadata | `--review`, `--dry-run` |
| `retry_parse_6.py` | Retry stuck paper parsing | `--review`, `--spec` |
| `reparse_cloud_spans.py` | Re-parse 0-span cloud extractions from stored JSON | `--review` |
| `backfill_cloud_spans.py` | Backfill cloud extractions missing span rows | `--review`, `--dry-run` |
| `prepare_concordance_pdfs.py` | Copy + rename PDFs for concordance | `--review` |

### Testing / Validation

| Script | Purpose |
|--------|---------|
| `smoke_test_fixes.py` | Extract + audit 5 papers with temp DB |
| `test_e2e_search_screen.py` | Live end-to-end search + screening on 20 papers |
| `test_extraction_validation.py` | Parse 3 PDFs + validate extraction quality |
| `eval_auditor_models.py` | Compare auditor model performance |
| `ft_screening_smoke_test.py` | FT screening on 5 known papers |
| `q8_validation.py` | q8_0 KV cache validation (full) |
| `q8_validation_fast.py` | q8_0 KV cache validation (fast, no snippet retries) |

### Legacy (scripts/pdf_acquisition/)

5 scripts (step1–step4) for single-strategy PDF acquisition. Superseded by `engine/acquisition/` cascade.

### Shell Scripts

| Script | Purpose |
|--------|---------|
| `run_expanded_screen_and_verify.sh` | tmux runner for expanded screening phases 2+3 |
| `watch_run4.sh` | Poll DB every 5 min for extraction progress |

---

## tests/ — Test Suite

38 test files, 934 total tests (920 offline, 14 network/ollama/integration).

| Test File | Covers |
|-----------|--------|
| `test_adjudication.py` | Abstract screening adjudication export/import |
| `test_adjudication_pairs.py` | Concordance AMBIGUOUS pair export |
| `test_api_parity.py` | Cloud extractor API compatibility |
| `test_audit_adjudication.py` | Audit review export/import |
| `test_auditor.py` | Grep verify, semantic verify, audit_span, LOW_YIELD |
| `test_background.py` | tmux background launcher |
| `test_cloud_extraction.py` | Cloud extractor base, store_result, parse_response |
| `test_codebook_prompt.py` | Extraction prompt building from codebook |
| `test_concordance.py` | Concordance scoring, normalization |
| `test_concordance_pipeline.py` | Multi-arm concordance pipeline |
| `test_concordance_report.py` | Report generation (terminal, CSV, HTML) |
| `test_consensus.py` | Majority-vote consensus derivation |
| `test_database.py` | ReviewDatabase: state machine, transitions, queries |
| `test_db_backup.py` | Auto-backup functionality |
| `test_dedup.py` | Two-phase deduplication |
| `test_distribution_monitor.py` | Collapse detection, Shannon entropy |
| `test_exporters.py` | PRISMA, evidence table, DOCX, methods section |
| `test_extraction_cleanup.py` | Stale extraction cleanup |
| `test_extraction_validator.py` | Field validation, cross-field bleed |
| `test_extractor.py` | Two-pass extraction, snippet validation |
| `test_ft_screening.py` | FT screening, truncation, reason codes |
| `test_human_import.py` | Human workbook parsing + validation |
| `test_human_review.py` | Review queue export/import |
| `test_low_yield.py` | LOW_YIELD detection logic |
| `test_ollama_client.py` | Timeout wrapper, retry, restart recovery |
| `test_ollama_preflight.py` | Pre-flight checks, VRAM budget |
| `test_openalex.py` | OpenAlex search + abstract reconstruction |
| `test_pdf_parser.py` | Three-tier parsing routing |
| `test_pdf_quality_import.py` | Disposition import validation |
| `test_prisma_reconciliation.py` | PRISMA count reconciliation |
| `test_progress.py` | Progress reporter formatting |
| `test_pubmed.py` | PubMed search + query building |
| `test_retry_failed.py` | Download retry strategies |
| `test_review_spec.py` | ReviewSpec loading, validation, hashing |
| `test_screener.py` | Abstract screening dual-pass + verification |
| `test_trace_exporter.py` | Trace quality report |
| `test_verify_downloads.py` | PDF verify/rename/match |
| `test_workflow.py` | 12-stage workflow enforcement |

**Markers:**
- `@pytest.mark.network` — Requires network (PubMed, OpenAlex, etc.)
- `@pytest.mark.ollama` — Requires running Ollama with specific models
- `@pytest.mark.integration` — Requires heavy dependencies (Docling, PDF generation)

*Generated 2026-03-19 from commit e124b20*
