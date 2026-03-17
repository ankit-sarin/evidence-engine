# Module Inventory

Complete inventory of every Python file under `engine/`, `scripts/`, `analysis/`, and `tests/` with purpose, key functions/classes, and dependencies.

---

## engine/core/

| File | Purpose | Key Exports | Dependencies |
|------|---------|-------------|--------------|
| `__init__.py` | Package init | — | — |
| `constants.py` | Shared regex + FT screening constants | `INVALID_SNIPPET_RE`, `FT_REASON_CODES`, `FT_MAX_TEXT_CHARS` (32,000) | `re` |
| `database.py` | SQLite state machine — one DB per review, full provenance retention | `ReviewDatabase`, `STATUSES`, `ALLOWED_TRANSITIONS`, `DATA_ROOT` | `engine.search.models`, `engine.utils.db_backup` |
| `naming.py` | Human review artifact naming conventions | `review_artifact_filename()`, `review_artifact_path()`, `REVIEW_STAGES` | — |
| `review_spec.py` | YAML Review Spec parser, Pydantic models, protocol hashing | `ReviewSpec`, `load_review_spec()`, `ExtractionField`, `ExtractionSchema`, `PICO`, `ScreeningCriteria`, `ScreeningModels`, `FTScreeningModels`, `SpecialtyScope`, `PDFQualityCheck` | `pydantic`, `yaml` |

**`ReviewDatabase` key methods:** `add_papers()`, `update_status()`, `reject_paper()`, `add_extraction_atomic()`, `update_audit()`, `add_ft_screening_decision()`, `add_ft_verification_decision()`, `min_status_gate()`, `reset_for_reaudit()`, `reset_for_reextraction()`, `get_pipeline_stats()`, `cleanup_orphaned_spans()`

**Database tables:** `papers`, `abstract_screening_decisions`, `abstract_verification_decisions`, `full_text_assets`, `extractions` (with `model_digest`, `auditor_model_digest`, `low_yield`), `evidence_spans`, `ft_screening_decisions`, `ft_verification_decisions`, `review_runs`

---

## engine/search/

| File | Purpose | Key Exports | Dependencies |
|------|---------|-------------|--------------|
| `__init__.py` | Package init | — | — |
| `models.py` | Shared citation data model | `Citation` (Pydantic: pmid, doi, title, abstract, authors, journal, year, source) | `pydantic` |
| `pubmed.py` | PubMed search via Biopython Entrez (esearch + efetch, batch 500, 3 req/s) | `search_pubmed()` | `Bio.Entrez`, `Bio.Medline` |
| `openalex.py` | OpenAlex search via pyalex with pagination retry and abstract reconstruction | `search_openalex()`, `reconstruct_abstract()` | `pyalex` |
| `dedup.py` | Two-phase dedup: exact (DOI/PMID/title) then fuzzy (SequenceMatcher > 0.9) | `deduplicate()`, `DedupResult`, `normalize_title()`, `title_similarity()` | `difflib` |

---

## engine/agents/

| File | Purpose | Key Exports | Dependencies |
|------|---------|-------------|--------------|
| `__init__.py` | Package init | — | — |
| `models.py` | Pydantic data models for extraction | `EvidenceSpan` (with confidence clamping validator), `ExtractionResult`, `ExtractionOutput` | `pydantic` |
| `screener.py` | Role-aware dual-model abstract screening with specialty scope | `screen_paper()`, `run_screening()`, `run_verification()`, `ScreeningDecision` | `engine.utils.ollama_client` |
| `ft_screener.py` | Full-text dual-model screening: reason codes, 32K truncation, checkpoint/resume, status-aware | `ft_screen_paper()`, `ft_verify_paper()`, `run_ft_screening()`, `run_ft_verification()`, `truncate_paper_text()`, `FTScreeningDecision`, `FTVerificationDecision` | `engine.utils.ollama_client`, `engine.utils.ollama_preflight`, `engine.core.constants` |
| `extractor.py` | Two-pass codebook-driven extraction with proactive Ollama restart | `extract_paper()`, `run_extraction()`, `build_extraction_prompt()`, `parse_thinking_trace()` | `engine.utils.ollama_client`, `engine.utils.ollama_preflight`, `engine.utils.extraction_cleanup` |
| `auditor.py` | Cross-model grep + semantic audit, LOW_YIELD detection | `audit_span()`, `run_audit()`, `grep_verify()`, `semantic_verify()`, `AuditVerdict`, `count_populated_fields()`, `check_low_yield()` | `engine.utils.ollama_client`, `engine.utils.ollama_preflight` |

**Constants:**
- `screener.py`: `DEFAULT_PRIMARY_MODEL = "qwen3:8b"`, `DEFAULT_VERIFICATION_MODEL = "qwen3:32b"`
- `extractor.py`: `MODEL = "deepseek-r1:32b"`, `MAX_RETRIES = 2`, `RESTART_EVERY_N = 25`
- `auditor.py`: `DEFAULT_AUDITOR_MODEL = "gemma3:27b"`, `SEMANTIC_ONLY_TIERS = {4}`

---

## engine/acquisition/

| File | Purpose | Key Exports | Dependencies |
|------|---------|-------------|--------------|
| `__init__.py` | Re-exports public API | `check_oa_status()`, `download_papers()`, `verify_downloads()`, `import_dispositions()` | — |
| `check_oa.py` | Unpaywall API OA status lookup (1 req/sec rate limit) | `check_oa_status()`, `_query_unpaywall()` | `requests` |
| `download.py` | 5-strategy cascade PDF downloader with %PDF validation | `download_papers()`, `is_valid_pdf()` | `requests`, `tarfile` |
| `verify_downloads.py` | Match/validate/rename PDFs to canonical format, update DB | `verify_downloads()`, `canonical_filename()`, `_validate_pdf()` | `engine.core.database` |
| `pdf_quality_check.py` | AI first-page classification via vision model (language + content type) | `run_quality_check()` | `fitz`, `engine.utils.ollama_client` |
| `pdf_quality_html.py` | HTML review pages: acquisition mode and quality_check mode | `generate_acquisition_html()`, `generate_quality_html()` | `engine.core.naming` |
| `pdf_quality_import.py` | Import disposition JSON → DB (PROCEED/EXCLUDE), atomic with two-pass validation | `import_dispositions()`, `validate_disposition_json()` | `engine.core.database` |
| `manual_list.py` | DEPRECATED — superseded by `pdf_quality_html.py --mode acquisition` | `generate_manual_list()` | — |

---

## engine/adjudication/

| File | Purpose | Key Exports | Dependencies |
|------|---------|-------------|--------------|
| `__init__.py` | Re-exports all public adjudication functions | — | — |
| `workflow.py` | 12-stage sequential workflow state machine | `WORKFLOW_STAGES`, `SCREENING_STAGES`, `FULL_TEXT_STAGES`, `EXTRACTION_STAGES`, `advance_stage()`, `complete_stage()`, `bypass_stage()`, `format_workflow_status()`, `get_current_blocker()`, `can_advance_to()` | `sqlite3` |
| `schema.py` | DDL for adjudication tables (abstract + FT + audit) | `ensure_adjudication_table()` | `sqlite3` |
| `categorizer.py` | Rule-based FP categorization from YAML config (keyword/regex matching) | `CategoryConfig`, `categorize_paper()`, `generate_starter_config()`, `load_config()` | `yaml` |
| `screening_adjudicator.py` | Export/import abstract screening adjudication queue (self-documenting workbook) | `export_adjudication_queue()`, `import_adjudication_decisions()`, `check_adjudication_gate()` | `openpyxl`, `engine.exporters.review_workbook` |
| `abstract_adjudication_html.py` | HTML export for ABSTRACT_SCREEN_FLAGGED papers with category badges and batch actions | `generate_adjudication_html()` | `engine.adjudication.categorizer` |
| `ft_adjudication_html.py` | HTML export for FT_FLAGGED papers with reason codes, localStorage persistence | `generate_ft_adjudication_html()` | `engine.core.naming` |
| `ft_screening_adjudicator.py` | Export/import FT screening adjudication queue (xlsx + JSON support) | `export_ft_adjudication_queue()`, `import_ft_adjudication_decisions()`, `check_ft_adjudication_gate()` | `openpyxl`, `engine.exporters.review_workbook` |
| `audit_adjudicator.py` | Per-span audit export (ACCEPT/REJECT/CORRECT), spot-check sampling, LOW_YIELD integration, legacy format support | `export_audit_review_queue()`, `import_audit_review_decisions()`, `check_audit_review_gate()` | `openpyxl`, `engine.exporters.review_workbook` |
| `advance_stage.py` | CLI for manual workflow advancement and status display | `main()` | `engine.adjudication.workflow` |

**CLI:** `python -m engine.adjudication.advance_stage --review NAME --stage STAGE --note NOTE [--force] [--status]`

---

## engine/parsers/

| File | Purpose | Key Exports | Dependencies |
|------|---------|-------------|--------------|
| `__init__.py` | Package init | — | — |
| `models.py` | Parsed document data model | `ParsedDocument` (parser_used: `docling` \| `pymupdf` \| `qwen2.5vl`) | `pydantic` |
| `pdf_parser.py` | Three-tier PDF → Markdown parser with DB-driven path resolution and hash caching | `parse_pdf()`, `parse_all_pdfs()`, `parse_with_docling()`, `parse_with_pymupdf()`, `parse_with_vision()`, `is_scanned_pdf()`, `compute_pdf_hash()`, `verify_hashes()` | `fitz`, `docling`, `engine.utils.ollama_client` |

---

## engine/cloud/

| File | Purpose | Key Exports | Dependencies |
|------|---------|-------------|--------------|
| `__init__.py` | Re-exports | `OpenAIExtractor`, `AnthropicExtractor`, `init_cloud_tables` | — |
| `schema.py` | DDL for cloud extraction tables | `init_cloud_tables()` | `sqlite3` |
| `base.py` | Shared cloud extractor logic: prompt building, response parsing, span storage | `CloudExtractorBase` | `engine.agents.extractor`, `engine.agents.models` |
| `openai_extractor.py` | OpenAI o4-mini arm (reasoning_effort=high, json_object format) | `OpenAIExtractor` | `openai` |
| `anthropic_extractor.py` | Anthropic Sonnet 4.6 arm (extended thinking, markdown fence stripping, rate-limit backoff) | `AnthropicExtractor` | `anthropic` |

---

## engine/analysis/ (Concordance)

| File | Purpose | Key Exports | Dependencies |
|------|---------|-------------|--------------|
| `__init__.py` | Package init | — | — |
| `normalize.py` | Canonical normalization for concordance comparison (null synonyms, prefix mapping, case folding) | `normalize_for_concordance()` | `engine.core.review_spec` |
| `scoring.py` | Field-pair comparison logic (MATCH/MISMATCH/AMBIGUOUS) | `score_pair()`, `FieldScore` | `engine.analysis.normalize` |
| `metrics.py` | Agreement statistics: Cohen's κ with 95% CI, percent agreement | `cohens_kappa()`, `percent_agreement()`, `field_summary()`, `KappaResult`, `FieldSummary` | — |
| `concordance.py` | Multi-arm concordance orchestrator: load arms, align, score, report | `load_arm()`, `align_arms()`, `run_concordance()`, `run_all_pairs()`, `check_schema_parity()`, `ConcordanceReport`, `Disagreement` | `engine.analysis.scoring`, `engine.analysis.metrics` |
| `report.py` | Human-readable concordance reports (terminal table, CSV, branded HTML) | `print_summary()`, `write_report()` | `engine.analysis.metrics`, `engine.core.review_spec` |

**CLI:** `python -m engine.analysis.concordance --review NAME --arms arm1,arm2,...`

---

## engine/validators/

| File | Purpose | Key Exports | Dependencies |
|------|---------|-------------|--------------|
| `__init__.py` | Package init | — | — |
| `extraction_validator.py` | Post-extraction field validation: unknown names, invalid categoricals, non-numeric sample_size (read-only diagnostic) | `validate_extraction()`, `validate_all()`, `verify_schema_parity()`, `normalize_prefix()` | `engine.core.review_spec`, `engine.agents.extractor` |
| `distribution_monitor.py` | Post-extraction distribution collapse detector: flags categorical fields with zero variance (COLLAPSED) or dominant single value (LOW_VARIANCE), Shannon entropy | `check_distribution()`, `print_distribution_report()`, `assert_no_collapse()`, `shannon_entropy()`, `DistributionCollapseError` | `yaml` |

**CLI:**
- `python -m engine.validators.extraction_validator --review NAME`
- `python -m engine.validators.distribution_monitor --review NAME --arm ARM [--strict]`

---

## engine/exporters/

| File | Purpose | Key Exports | Dependencies |
|------|---------|-------------|--------------|
| `__init__.py` | Master export orchestrator | `export_all()` | — |
| `review_workbook.py` | Shared self-documenting Excel workbook builder (used by all 3 adjudication exporters) | `create_review_workbook()`, `ColumnDef`, `DecisionColumnDef`, `FreeTextColumnDef`, `InstructionsConfig` | `openpyxl` |
| `prisma.py` | PRISMA flow data + CSV with automatic reconciliation (PDF/FT exclusions, LOW_YIELD, in-progress) | `generate_prisma_flow()`, `export_prisma_csv()`, `validate_prisma_counts()` | `csv` |
| `evidence_table.py` | Evidence CSV + 3-sheet Excel with min_status filtering | `export_evidence_csv()`, `export_evidence_excel()` | `openpyxl` |
| `docx_export.py` | Formatted DOCX evidence table (landscape) | `export_evidence_docx()` | `python-docx` |
| `methods_section.py` | Auto-generated PRISMA methods paragraph in Markdown | `generate_methods_section()`, `export_methods_md()` | `engine.exporters.prisma` |
| `trace_exporter.py` | Per-paper reasoning traces, quality report, disagreement pairs | `export_traces_markdown()`, `export_trace_quality_report()`, `export_disagreement_pairs()` | `statistics` |

---

## engine/review/

| File | Purpose | Key Exports | Dependencies |
|------|---------|-------------|--------------|
| `__init__.py` | Package init | — | — |
| `extraction_audit_html.py` | HTML per-span audit review interface (ACCEPT/REJECT/CORRECT), grouped by paper, localStorage persistence | `generate_audit_html()` | `engine.core.naming` |
| `human_review.py` | CSV-based human review queue (alternative to Excel): export/import with snippet validation | `export_review_queue()`, `import_review_decisions()`, `bulk_accept()` | `csv`, `difflib` |

---

## engine/utils/

| File | Purpose | Key Exports | Dependencies |
|------|---------|-------------|--------------|
| `__init__.py` | Package init | — | — |
| `background.py` | tmux auto-detach for long-running scripts | `maybe_background()` | `subprocess` (tmux) |
| `db_backup.py` | Auto-backup SQLite before destructive operations (timestamped copies) | `auto_backup()` | — |
| `extraction_cleanup.py` | Schema-version extraction cleanup: remove stale extractions, cascade delete spans, reset papers (dry-run default) | `cleanup_stale_extractions()`, `check_stale_extractions()`, `get_current_schema_hash()`, `find_review_spec()` | `engine.core.database` |
| `ollama_client.py` | Shared Ollama client with three-layer watchdog (httpx timeout → wall-clock watchdog → service restart) | `ollama_chat()`, `get_model_digest()`, `MODEL_TIMEOUTS` | `ollama`, `httpx` |
| `ollama_preflight.py` | Pre-flight health check: model load test, VRAM budget, environment assertions | `check_model()`, `preflight_check()`, `require_preflight()`, `check_ollama_env()`, `PreflightResult`, `ModelResult` | `ollama` |
| `progress.py` | Periodic progress reporting for extraction runs (ETA calculation) | `ProgressReporter` | — |

**Constants in `ollama_client.py`:**
- `MODEL_TIMEOUTS`: `{8b: 300s, 27b: 600s, 32b: 900s, 70b: 1200s}`, default 600s
- `DEFAULT_MAX_RETRIES = 2` (3 total attempts)

**Constants in `ollama_preflight.py`:**
- `_VRAM_BUDGET_GB = 100.0`
- `_REQUIRED_ENV = {"OLLAMA_FLASH_ATTENTION": "true", "OLLAMA_MAX_LOADED_MODELS": "1"}`

---

## engine/migrations/

| File | Purpose | Key Exports | Dependencies |
|------|---------|-------------|--------------|
| `__init__.py` | Package init | — | — |
| `002_screening_rename.py` | Rename screening tables/columns for abstract/FT separation | — | `sqlite3` |
| `003_backfill_expanded_screening.py` | Backfill expanded corpus screening data into renamed tables | — | `sqlite3` |
| `004_pdf_quality_check.py` | Add PDF quality check columns + PDF_EXCLUDED status | `run_migration()` | `sqlite3` |
| `005_model_digest.py` | Add `model_digest` and `auditor_model_digest` columns to extractions table | `run_migration()` | `sqlite3` |

---

## analysis/paper1/ (Paper 1 Analysis Layer)

| File | Purpose | Key Exports | Dependencies |
|------|---------|-------------|--------------|
| `__init__.py` | Package init | — | — |
| `human_import.py` | Import human extractor workbooks (.xlsx) into `human_extractions` table (long format, 20 fields × N papers) | `parse_workbook()`, `validate_workbook()`, `store_human_extractions()` | `openpyxl`, `yaml` |
| `adjudication.py` | Concordance adjudication: export AMBIGUOUS pairs as HTML, import decisions | `export_ambiguous_pairs()`, `generate_adjudication_html()`, `import_adjudication_decisions()` | `engine.analysis.concordance`, `engine.analysis.scoring` |
| `consensus.py` | Majority-vote consensus derivation for shared human extraction papers (gold standard reference) | `identify_shared_papers()`, `derive_consensus()`, `store_consensus()`, `export_no_consensus_for_adjudication()`, `print_summary()` | `engine.analysis.normalize` |

**CLI:**
- `python -m analysis.paper1.human_import --workbook PATH --review NAME [--dry-run]`
- `python -m analysis.paper1.adjudication export --review NAME --arms arm1,arm2 --output PATH`
- `python -m analysis.paper1.adjudication import --decisions PATH --review NAME`
- `python -m analysis.paper1.consensus derive --review NAME [--dry-run]`
- `python -m analysis.paper1.consensus summary --review NAME`

---

## scripts/

### Pipeline Runners

| File | Purpose |
|------|---------|
| `run_pipeline.py` | Full pipeline orchestrator (search → screen → parse → extract → audit → export) with adjudication gates |
| `run5_extract_and_audit.py` | Extraction + audit for eligible papers with crash recovery and proactive Ollama restart |
| `run_cloud_extraction.py` | Cloud concordance extraction CLI (`--arm openai\|anthropic\|both`, `--max-cost`, `--progress`) |
| `screen_expanded.py` | Three-phase expanded search screening (fetch → dual-pass → verification) |
| `monitor_extraction.py` | Watchdog: polls extract log for stalls every 20 min, reports progress |

### Re-screening / Remediation

| File | Purpose |
|------|---------|
| `reextract_all.py` | Full re-extraction + re-audit (admin override: wipes extraction state, resets to PARSED) |
| `reextract_failed.py` | Re-extract specific failed papers with extended timeouts |
| `rescreen_original_251.py` | Re-screen original corpus with updated criteria (read-only, writes staging CSV) |
| `rescreen_with_specialty.py` | Re-screen included papers with specialty_scope filtering |

### Parsing / Acquisition

| File | Purpose |
|------|---------|
| `parse_expanded_corpus.py` | Advance ABSTRACT_SCREENED_IN → PDF_ACQUIRED → PARSED for expanded corpus |
| `advance_to_pdf_acquired.py` | Bulk status advancement for papers with PDFs on disk (SHA-256, full_text_assets insert) |
| `backfill_authors.py` | Backfill missing author metadata from PubMed/OpenAlex |
| `retry_parse_6.py` | Retry parsing specific stuck papers (Docling → PyMuPDF fallback) |
| `reparse_cloud_spans.py` | Re-parse cloud extractions with 0 spans from stored JSON (no API calls) |
| `prepare_concordance_pdfs.py` | Copy included papers to concordance_pdfs/ with EE-### naming + manifest |

### Testing / Validation

| File | Purpose |
|------|---------|
| `smoke_test_fixes.py` | Extract + audit 5 papers with temporary DB (integration check) |
| `test_e2e_search_screen.py` | Live end-to-end: search + dual-pass screening with markdown log |
| `test_extraction_validation.py` | Parse 3 PDFs, extract, validate schema + snippets |
| `eval_auditor_models.py` | Compare auditor model performance on 5-paper sample |
| `ft_screening_smoke_test.py` | Full-text screening on 5 known papers (primary + verifier) |

### PDF Acquisition Pipeline (legacy scripts)

| File | Purpose |
|------|---------|
| `pdf_acquisition/step1_export_citations.py` | Export citations from DB to CSV |
| `pdf_acquisition/step2_unpaywall_check.py` | Unpaywall OA check with priority sorting |
| `pdf_acquisition/step3_download_oa_pdfs.py` | Download OA PDFs (direct URL) |
| `pdf_acquisition/step3b_retry_failed.py` | Cascade retry: PMC/IEEE/DOI strategies |
| `pdf_acquisition/step4_manual_download_list.py` | Generate manual download checklist (HTML + CSV) |

### Shell Scripts

| File | Purpose |
|------|---------|
| `run_expanded_screen_and_verify.sh` | Fresh primary screening + verification (tmux) |
| `watch_run4.sh` | Monitor screening progress every 5 min |

---

## tests/

| File | Focus |
|------|-------|
| `test_database.py` | SQLite review database: tables, lifecycle, transitions, staleness, reject, min_status, FT states |
| `test_review_spec.py` | YAML loading, protocol hashing, validation |
| `test_screener.py` | Dual-model abstract screening: structured output, verification, specialty scope |
| `test_ft_screening.py` | FT screener: decisions, truncation, prompts, adjudicator, status-aware, self-documenting workbook |
| `test_extractor.py` | Two-pass extraction: prompt building, thinking trace, codebook integration, ellipsis retry |
| `test_codebook_prompt.py` | Codebook-driven extraction prompt generation |
| `test_auditor.py` | Cross-model audit: grep verify, semantic verify, full audit mocked |
| `test_low_yield.py` | Populated field counting, threshold flagging, audit queue, PRISMA integration |
| `test_cloud_extraction.py` | Cloud tables, span parsing (all API calls mocked), store, CLI |
| `test_dedup.py` | DOI/PMID/fuzzy match, merge, stats |
| `test_pdf_parser.py` | Hash, routing, Docling integration, versioning, PyMuPDF fallback |
| `test_adjudication.py` | Categorizer, screening export/import, gate checks, self-documenting workbook |
| `test_adjudication_pairs.py` | Paper 1 concordance adjudication interface |
| `test_audit_adjudication.py` | Per-span audit export/import: ACCEPT/REJECT/CORRECT, spot-check, legacy format |
| `test_workflow.py` | 12-stage workflow enforcement, blockers, format display |
| `test_exporters.py` | PRISMA, CSV, Excel, DOCX, methods, export_all |
| `test_prisma_reconciliation.py` | Reconciliation pass/fail, in-progress, PDF_EXCLUDED sub-counts |
| `test_verify_downloads.py` | Author cleaning, canonical names, PDF validation, publisher classification |
| `test_pdf_quality_import.py` | PDF quality disposition import: validation, atomic, PROCEED/EXCLUDE |
| `test_extraction_validator.py` | Valid spans, unknown fields, invalid categorical, closest-match |
| `test_extraction_cleanup.py` | Dry-run, schema cleanup, cascade delete, status reset, HUMAN_AUDIT_COMPLETE protection |
| `test_ollama_client.py` | Ollama client wrapper: watchdog timeouts, retry, service restart |
| `test_ollama_preflight.py` | Pre-flight health check: model check, VRAM, env assertions |
| `test_concordance.py` | Concordance normalization and scoring |
| `test_concordance_pipeline.py` | Metrics and concordance pipeline (mock data) |
| `test_concordance_report.py` | Concordance report generation |
| `test_human_review.py` | Human review queue export/import |
| `test_human_import.py` | Paper 1 human extractor workbook importer |
| `test_consensus.py` | Paper 1 majority-vote consensus derivation |
| `test_distribution_monitor.py` | Distribution collapse detector: COLLAPSED, LOW_VARIANCE, arm routing, entropy |
| `test_trace_exporter.py` | Per-paper traces, quality report, disagreements |
| `test_background.py` | tmux background mode |
| `test_db_backup.py` | DB auto-backup before destructive operations |
| `test_progress.py` | ProgressReporter utility |
| `test_retry_failed.py` | Extraction --retry-failed flag |
| `test_api_parity.py` | Verify all scripts/ imports resolve correctly |
| `test_pubmed.py` | Live PubMed queries (network marker) |
| `test_openalex.py` | Live OpenAlex + abstract reconstruction (network marker) |

**Total: 706 offline tests passing** (10 network/ollama tests deselected by default)

```bash
python -m pytest tests/ -v -m "not network and not ollama"  # 706 passed
```

---

*Generated 2026-03-17 from commit d0bf07c*
