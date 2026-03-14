# Module Inventory

Complete inventory of every Python file under `engine/`, `scripts/`, and `tests/` with purpose, key functions/classes, and external dependencies.

---

## engine/core/

| File | Lines | Purpose | Key Exports | External Deps |
|------|-------|---------|-------------|---------------|
| `__init__.py` | 1 | Empty module | — | — |
| `constants.py` | 21 | Shared regex + FT screening constants | `INVALID_SNIPPET_RE`, `FT_REASON_CODES`, `FT_MAX_TEXT_CHARS` | `re` |
| `database.py` | 823 | SQLite state machine — one DB per review, data retention | `ReviewDatabase`, `STATUSES`, `ALLOWED_TRANSITIONS`, `DATA_ROOT` | `sqlite3` |
| `review_spec.py` | 232 | YAML parser, Pydantic models, protocol hashing | `ReviewSpec`, `load_review_spec()`, `ExtractionSchema`, `PICO`, `ScreeningCriteria`, `ScreeningModels`, `FTScreeningModels`, `SpecialtyScope`, `PDFQualityCheck` | `pydantic`, `yaml` |

**`ReviewDatabase` key methods:** `add_papers()`, `update_status()`, `reject_paper()`, `add_extraction_atomic()`, `update_audit()`, `add_ft_screening_decision()`, `add_ft_verification_decision()`, `min_status_gate()`, `reset_for_reaudit()`, `reset_for_reextraction()`, `get_pipeline_stats()`, `cleanup_orphaned_spans()`

**`ReviewSpec` notable fields:** `screening_models` (abstract), `ft_screening_models` (full-text), `specialty_scope` (SpecialtyScope with included/excluded specialties), `low_yield_threshold` (default 4), `auditor_model`, `unpaywall_email`, `institutional_proxy_pattern`, `pdf_quality_check` (PDFQualityCheck with ai_model, dpi, timeout, exclude_reasons)

**Database tables:** `papers`, `abstract_screening_decisions` (renamed from `screening_decisions`), `abstract_verification_decisions` (renamed from `verification_decisions`), `full_text_assets`, `extractions` (with `low_yield` column), `evidence_spans`, `ft_screening_decisions`, `ft_verification_decisions`, `review_runs`

---

## engine/search/

| File | Lines | Purpose | Key Exports | External Deps |
|------|-------|---------|-------------|---------------|
| `__init__.py` | 1 | Empty module | — | — |
| `models.py` | 20 | Shared citation data model | `Citation` (Pydantic) | `pydantic` |
| `pubmed.py` | 181 | PubMed search via Biopython Entrez | `search_pubmed()` | `Bio.Entrez`, `Bio.Medline` |
| `openalex.py` | 149 | OpenAlex search via pyalex | `search_openalex()`, `reconstruct_abstract()` | `pyalex` |
| `dedup.py` | 201 | DOI/PMID/fuzzy-title deduplication | `deduplicate()`, `DedupResult`, `normalize_title()`, `title_similarity()` | `difflib` |

---

## engine/agents/

| File | Lines | Purpose | Key Exports | External Deps |
|------|-------|---------|-------------|---------------|
| `__init__.py` | 1 | Empty module | — | — |
| `models.py` | 44 | Pydantic data models for extraction | `EvidenceSpan`, `ExtractionResult`, `ExtractionOutput` | `pydantic` |
| `screener.py` | 345 | Role-aware dual-model abstract screening with specialty scope | `screen_paper()`, `run_screening()`, `run_verification()`, `ScreeningDecision` | `ollama` |
| `ft_screener.py` | 538 | Full-text dual-model screening with reason codes | `ft_screen_paper()`, `ft_verify_paper()`, `run_ft_screening()`, `run_ft_verification()`, `truncate_paper_text()`, `FTScreeningDecision`, `FTVerificationDecision` | `ollama` |
| `extractor.py` | 382 | Two-pass DeepSeek-R1 extraction | `extract_paper()`, `run_extraction()`, `build_extraction_prompt()` | `ollama` |
| `auditor.py` | 442 | Cross-model grep + semantic audit + LOW_YIELD detection | `audit_span()`, `run_audit()`, `grep_verify()`, `semantic_verify()`, `AuditVerdict`, `count_populated_fields()`, `check_low_yield()` | `ollama`, `difflib` |

**Constants:**
- `screener.py`: `DEFAULT_PRIMARY_MODEL = "qwen3:8b"`, `DEFAULT_VERIFICATION_MODEL = "qwen3:32b"`
- `ft_screener.py`: Uses `FT_MAX_TEXT_CHARS` (32,000) and `FT_REASON_CODES` from `engine/core/constants.py`
- `extractor.py`: `MODEL = "deepseek-r1:32b"`, `OLLAMA_TIMEOUT = 900.0`, `MAX_RETRIES = 2`
- `auditor.py`: `DEFAULT_AUDITOR_MODEL = "gemma3:27b"`, `SEMANTIC_ONLY_TIERS = {4}`, `_ABSENCE_VALUES` (NR, NOT_FOUND, etc.)

---

## engine/acquisition/

| File | Lines | Purpose | Key Exports | External Deps |
|------|-------|---------|-------------|---------------|
| `__init__.py` | 29 | Re-exports public API | `check_oa_status()`, `download_papers()`, `verify_downloads()`, `import_dispositions()` | — |
| `check_oa.py` | 198 | Unpaywall API OA status lookup | `check_oa_status()` | `requests` |
| `download.py` | 458 | 5-strategy cascade PDF downloader | `download_papers()`, `is_valid_pdf()` | `requests`, `tarfile` |
| `manual_list.py` | 450 | DEPRECATED — superseded by `pdf_quality_html.py --mode acquisition` | `generate_manual_list()`, `classify_publisher()` | — |
| `pdf_quality_check.py` | 317 | AI first-page classification via vision model (language + content type) | `run_quality_check()`, `_classify_page()`, `_render_first_page()` | `fitz`, `ollama` |
| `pdf_quality_html.py` | 743 | HTML review pages: `--mode acquisition` (download list) or `--mode quality_check` (post-download) | `generate_acquisition_html()`, `generate_quality_html()` | — |
| `pdf_quality_import.py` | 308 | Import disposition JSON → DB (PROCEED/EXCLUDE/WILL_ATTEMPT) | `import_dispositions()`, `validate_disposition_json()` | — |
| `verify_downloads.py` | 379 | Scan/match/validate/rename PDFs, update DB | `verify_downloads()`, `canonical_filename()`, `_validate_pdf()` | — |

**Download strategies:** Direct Unpaywall URL → PMC OA package (Europe PMC + NCBI tar.gz) → IEEE stamp page scrape → MDPI URL construction → DOI redirect with content negotiation

**Publisher classification:** 17 DOI prefix rules (`_DOI_PUBLISHER_RULES`): IEEE, Elsevier, Springer/Nature, Wiley, MDPI, Taylor & Francis, SAGE, Science/AAAS, PLOS, Frontiers, Wolters Kluwer, RSNA, AME, De Gruyter, SPIE, Zenodo

**Verify/rename pipeline:** 3 filename match patterns (bare integer → EE-prefix → rich name), PDF validation (%PDF header, ≥10KB, HTML error page detection), canonical rename (`EE-{nnn}_{Author}_{Year}.pdf`), updates both `papers.pdf_local_path` and `full_text_assets.pdf_path`

**PDF quality check pipeline:** AI classification (qwen2.5vl:7b) → HTML quality review → JSON export → `import_dispositions()` atomic DB update. Disposition values: PROCEED → `HUMAN_CONFIRMED`, EXCLUDE_* → `PDF_EXCLUDED` (terminal), PDF_WILL_ATTEMPT → no change, UNSET → skipped. Two-pass validation (reject all on any error).

**CLI entry points:**
- `python -m engine.acquisition.check_oa --review NAME [--spec YAML] [--background]`
- `python -m engine.acquisition.download --review NAME [--retry] [--background]`
- `python -m engine.acquisition.pdf_quality_html --review NAME --mode acquisition [--output PATH]`
- `python -m engine.acquisition.verify_downloads --review NAME [--pdf-dir PATH] [--dry-run]`
- `python -m engine.acquisition.pdf_quality_check --review NAME [--spec YAML] [--dry-run] [--limit N]`
- `python -m engine.acquisition.pdf_quality_html --review NAME --mode quality_check [--output PATH]`
- `python -m engine.acquisition.pdf_quality_import --review NAME --input JSON [--dry-run]`

---

## engine/parsers/

| File | Lines | Purpose | Key Exports | External Deps |
|------|-------|---------|-------------|---------------|
| `__init__.py` | 0 | Empty module | — | — |
| `models.py` | 19 | Parsed document data model | `ParsedDocument` (Pydantic) | `pydantic` |
| `pdf_parser.py` | 259 | PDF → Markdown with Docling/Qwen2.5-VL routing, DB-driven PDF path resolution | `parse_pdf()`, `parse_all_pdfs()`, `is_scanned_pdf()`, `compute_pdf_hash()` | `fitz` (PyMuPDF), `docling`, `ollama` |

**Routing:** Digital PDFs (> 100 chars/page) → Docling; Scanned PDFs (< 100 chars/page) → Qwen2.5-VL vision model via Ollama

**PDF path resolution in `parse_all_pdfs()`:** DB-driven with glob fallback — checks `full_text_assets.pdf_path` → `papers.pdf_local_path` → filesystem glob. Handles both absolute and relative paths from DB.

---

## engine/cloud/

| File | Lines | Purpose | Key Exports | External Deps |
|------|-------|---------|-------------|---------------|
| `__init__.py` | 6 | Re-exports | `OpenAIExtractor`, `AnthropicExtractor`, `init_cloud_tables` | — |
| `schema.py` | 42 | DDL for cloud extraction tables | `init_cloud_tables()` | `sqlite3` |
| `base.py` | 214 | Shared cloud extractor logic | `CloudExtractorBase` | `sqlite3` |
| `openai_extractor.py` | 196 | OpenAI o4-mini extraction arm | `OpenAIExtractor` | `openai` |
| `anthropic_extractor.py` | 206 | Anthropic Sonnet 4.6 extraction arm | `AnthropicExtractor` | `anthropic` |

**Cloud tables:** `cloud_extractions` (with cost tracking: input/output/reasoning tokens, cost_usd), `cloud_evidence_spans`

**Response parsing:** Handles 8+ alternate JSON key formats (`fields`, `extractions`, `data`, `extracted_fields`, etc.), markdown fence stripping, single-span-dict wrapping

---

## engine/adjudication/

| File | Lines | Purpose | Key Exports | External Deps |
|------|-------|---------|-------------|---------------|
| `__init__.py` | 49 | Re-exports public API | All major functions from submodules (including FT adjudication) | — |
| `workflow.py` | 364 | 12-stage workflow state machine | `WORKFLOW_STAGES`, `SCREENING_STAGES`, `FULL_TEXT_STAGES`, `EXTRACTION_STAGES`, `advance_stage()`, `complete_stage()`, `format_workflow_status()`, `get_current_blocker()`, `is_adjudication_complete()` | `sqlite3` |
| `schema.py` | 78 | DDL for adjudication tables | `ensure_adjudication_table()` | `sqlite3` |
| `categorizer.py` | 260 | FP category config + keyword/regex matching | `CategoryConfig`, `categorize_paper()`, `generate_starter_config()`, `load_config()` | `yaml` |
| `screening_adjudicator.py` | 761 | Export/import abstract screening adjudication queue (self-documenting workbook) | `export_adjudication_queue()`, `import_adjudication_decisions()`, `check_adjudication_gate()` | `openpyxl` |
| `ft_screening_adjudicator.py` | 572 | Export/import FT screening adjudication queue (self-documenting workbook, two-pass validation) | `export_ft_adjudication_queue()`, `import_ft_adjudication_decisions()`, `check_ft_adjudication_gate()` | `openpyxl` |
| `audit_adjudicator.py` | 926 | Per-span audit export (ACCEPT/REJECT/CORRECT), LOW_YIELD integration, legacy format support | `export_audit_review_queue()`, `import_audit_review_decisions()`, `check_audit_review_gate()`, `_flatten_to_span_rows()` | `openpyxl` |
| `advance_stage.py` | 100 | CLI for manual workflow advancement | `main()` | — |

**CLI:** `python -m engine.adjudication.advance_stage --review NAME --stage STAGE --note NOTE [--force] [--status]`

---

## engine/migrations/

| File | Lines | Purpose | Key Exports | External Deps |
|------|-------|---------|-------------|---------------|
| `__init__.py` | 0 | Empty module | — | — |
| `002_screening_rename.py` | 207 | "The Great Rename" — screening_decisions → abstract_screening_decisions, SCREENED_IN → ABSTRACT_SCREENED_IN, etc. | — | `sqlite3` |
| `003_backfill_expanded_screening.py` | 411 | Backfill expanded corpus screening data into renamed tables | — | `sqlite3` |
| `004_pdf_quality_check.py` | 95 | Add PDF quality check columns + PDF_EXCLUDED status to papers table | `run_migration()` | `sqlite3` |

---

## engine/review/

| File | Lines | Purpose | Key Exports | External Deps |
|------|-------|---------|-------------|---------------|
| `__init__.py` | 0 | Empty module | — | — |
| `human_review.py` | 349 | CSV-based human review queue (alternative to Excel) | `export_review_queue()`, `import_review_decisions()`, `bulk_accept()` | `csv`, `difflib` |

**Decisions:** `ACCEPT`, `ACCEPT_CORRECTED` (with snippet validation), `REJECT_VALUE` (set value to "NR"), `REJECT_PAPER`

---

## engine/exporters/

| File | Lines | Purpose | Key Exports | External Deps |
|------|-------|---------|-------------|---------------|
| `__init__.py` | 71 | Master export orchestrator | `export_all()` | — |
| `review_workbook.py` | 360 | Shared self-documenting Excel workbook builder | `create_review_workbook()`, `ColumnDef`, `DecisionColumnDef`, `FreeTextColumnDef`, `InstructionsConfig` | `openpyxl` |
| `prisma.py` | 172 | PRISMA flow data + CSV (includes PDF exclusions, FT exclusions, LOW_YIELD rejections) | `generate_prisma_flow()`, `export_prisma_csv()` | `csv` |
| `evidence_table.py` | 176 | Evidence CSV + 3-sheet Excel | `export_evidence_csv()`, `export_evidence_excel()` | `openpyxl` |
| `docx_export.py` | 111 | Formatted DOCX evidence table | `export_evidence_docx()` | `python-docx` |
| `methods_section.py` | 81 | PRISMA methods paragraph | `generate_methods_section()`, `export_methods_md()` | — |
| `trace_exporter.py` | 549 | Per-paper traces, quality report, disagreement pairs | `export_traces_markdown()`, `export_trace_quality_report()`, `export_disagreement_pairs()` | `statistics` |

**PRISMA additions:** `pdf_excluded`, `pdf_exclusion_reasons`, `ft_screened_out`, `ft_flagged`, `low_yield_rejected` fields in flow dict

---

## engine/utils/

| File | Lines | Purpose | Key Exports | External Deps |
|------|-------|---------|-------------|---------------|
| `__init__.py` | 0 | Empty module | — | — |
| `background.py` | 67 | tmux auto-detach for long-running scripts | `maybe_background()` | `subprocess` (tmux) |

---

## scripts/

| File | Lines | Purpose |
|------|-------|---------|
| `run_pipeline.py` | 382 | Full pipeline orchestrator (search → screen → parse → extract → audit → export) with adjudication gates and `--background` support |
| `run_cloud_extraction.py` | 156 | Cloud concordance extraction CLI (`--arm openai/anthropic/both`, `--max-cost`, `--progress`) |
| `screen_expanded.py` | 485 | Three-phase expanded search screening (fetch abstracts → primary dual-pass → verification) |
| `rescreen_original_251.py` | 173 | Re-screen original corpus with updated criteria (read-only, writes staging CSV) |
| `rescreen_with_specialty.py` | 403 | Re-screen included papers with specialty_scope (dual-pass + verification, checkpoint/resume) |
| `ft_screening_smoke_test.py` | 243 | Full-text screening smoke test — 5 known papers, primary + verification, timing |
| `reextract_all.py` | 162 | Full re-extraction + re-audit of all papers (admin override) |
| `reextract_failed.py` | 161 | Re-extract specific failed papers with extended timeout (up to 25 min) |
| `reparse_cloud_spans.py` | 105 | Re-parse cloud extractions with 0 spans using stored JSON (no API calls) |
| `eval_auditor_models.py` | 279 | Multi-model auditor evaluation (5-paper sample, 3 candidate models) |
| `backfill_authors.py` | 185 | Backfill missing first_author from title heuristics |
| `parse_expanded_corpus.py` | 84 | Parse expanded corpus PDFs |
| `advance_to_pdf_acquired.py` | 102 | Bulk ABSTRACT_SCREENED_IN → PDF_ACQUIRED transition for papers with PDFs on disk |
| `prepare_concordance_pdfs.py` | 66 | Rename PDFs to EE-XXX format + paper_manifest.csv |
| `monitor_extraction.py` | 68 | Watchdog: checks extract_log.txt for stalls every 20 min |
| `test_e2e_search_screen.py` | 162 | Live E2E test: search + screen 20 papers, writes markdown log |
| `smoke_test_fixes.py` | 187 | Smoke test: extract + audit 5 papers, reports flag rates |
| `test_extraction_validation.py` | 360 | Parse + extract + verify 3 PDFs, writes validation log |
| `run_expanded_screen_and_verify.sh` | 16 | Bash: fresh primary screening + verification (tmux) |
| `watch_run4.sh` | 20 | Bash: monitor screening progress every 5 min |

### scripts/pdf_acquisition/ (legacy, superseded by engine/acquisition/)

| File | Lines | Purpose |
|------|-------|---------|
| `step1_export_citations.py` | 83 | Export citations from DB to CSV |
| `step2_unpaywall_check.py` | 226 | Unpaywall OA check with priority sorting |
| `step3_download_oa_pdfs.py` | 166 | Download OA PDFs (direct URL only) |
| `step3b_retry_failed.py` | 374 | Retry with PMC/IEEE/DOI redirect strategies |
| `step4_manual_download_list.py` | 255 | Generate manual download HTML + CSV |

---

## tests/

| File | Lines | Tests | Focus |
|------|-------|-------|-------|
| `test_review_spec.py` | 103 | 11 | YAML loading, hashing, validation |
| `test_pubmed.py` | 62 | 5 | Live PubMed queries (network) |
| `test_openalex.py` | 102 | 7 | Live OpenAlex + abstract reconstruction (network) |
| `test_dedup.py` | 206 | 15 | DOI/PMID/fuzzy match, merge, stats |
| `test_database.py` | 525 | 28 | Tables, lifecycle, transitions, staleness, reject, min_status, FT states |
| `test_screener.py` | 254 | 14 | Structured output, dual-pass, verification logic, specialty scope |
| `test_pdf_parser.py` | 175 | 9 | Hash, routing, Docling integration, versioning |
| `test_extractor.py` | 353 | 17 | Prompt, thinking trace, two-pass, ellipsis retry |
| `test_auditor.py` | 354 | 25 | Grep verify, semantic verify, full audit mocked |
| `test_exporters.py` | 236 | 8 | PRISMA, CSV, Excel, DOCX, methods, export_all |
| `test_cloud_extraction.py` | 361 | 18 | Cloud tables, span parsing, store, CLI |
| `test_adjudication.py` | 544 | 37 | Categorizer, screening export/import, gate checks, self-documenting workbook |
| `test_audit_adjudication.py` | 574 | 17 | Per-span audit export/import, ACCEPT/REJECT/CORRECT, flatten, spot-check |
| `test_workflow.py` | 318 | 30 | 12-stage workflow enforcement, blockers, format |
| `test_ft_screening.py` | 673 | 61 | FT screener decisions, truncation, prompts, FT adjudicator, self-documenting workbook |
| `test_low_yield.py` | 337 | 15 | Populated field counting, threshold flagging, audit queue, PRISMA |
| `test_human_review.py` | 121 | 6 | Human review queue export/import |
| `test_background.py` | 112 | 7 | tmux background mode |
| `test_trace_exporter.py` | 186 | 11 | Per-paper traces, quality report, disagreements |
| `test_pdf_quality_import.py` | 368 | — | PDF quality disposition import: validation, atomic import, PROCEED/EXCLUDE/WILL_ATTEMPT |
| `test_verify_downloads.py` | 491 | 40 | Author cleaning, canonical names, PDF validation, publisher classification, verify/rename integration |

**Total: 377 offline tests passing** (10 network/ollama tests deselected by default)

```bash
python -m pytest tests/ -v -m "not network and not ollama"  # 377 passed
python -m pytest tests/ -v                                   # all 387
```

---

*Generated 2026-03-14 from commit `66563cb`*
