# Module Inventory

Complete inventory of every Python file under `engine/`, `scripts/`, and `tests/` with purpose, key functions/classes, and external dependencies.

---

## engine/core/

| File | Lines | Purpose | Key Exports | External Deps |
|------|-------|---------|-------------|---------------|
| `__init__.py` | 1 | Empty module | — | — |
| `constants.py` | 8 | Shared regex for invalid snippet detection | `INVALID_SNIPPET_RE` | `re` |
| `database.py` | 738 | SQLite state machine — one DB per review | `ReviewDatabase`, `STATUSES`, `ALLOWED_TRANSITIONS`, `DATA_ROOT` | `sqlite3` |
| `review_spec.py` | 152 | YAML parser, Pydantic models, protocol hashing | `ReviewSpec`, `load_review_spec()`, `ExtractionSchema`, `PICO`, `ScreeningCriteria`, `ScreeningModels` | `pydantic`, `yaml` |

**`ReviewDatabase` key methods:** `add_papers()`, `update_status()`, `reject_paper()`, `add_extraction_atomic()`, `update_audit()`, `min_status_gate()`, `reset_for_reaudit()`, `reset_for_reextraction()`, `get_pipeline_stats()`, `cleanup_orphaned_spans()`

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
| `screener.py` | 342 | Role-aware dual-model screening | `screen_paper()`, `run_screening()`, `run_verification()`, `ScreeningDecision` | `ollama` |
| `extractor.py` | 382 | Two-pass DeepSeek-R1 extraction | `extract_paper()`, `run_extraction()`, `build_extraction_prompt()` | `ollama` |
| `auditor.py` | 363 | Cross-model grep + semantic audit | `audit_span()`, `run_audit()`, `grep_verify()`, `semantic_verify()`, `AuditVerdict` | `ollama`, `difflib` |

**Constants:**
- `screener.py`: `DEFAULT_PRIMARY_MODEL = "qwen3:8b"`, `DEFAULT_VERIFICATION_MODEL = "qwen3:32b"`
- `extractor.py`: `MODEL = "deepseek-r1:32b"`, `OLLAMA_TIMEOUT = 900.0`, `MAX_RETRIES = 2`
- `auditor.py`: `DEFAULT_AUDITOR_MODEL = "gemma3:27b"`, `SEMANTIC_ONLY_TIERS = {4}`

---

## engine/acquisition/

| File | Lines | Purpose | Key Exports | External Deps |
|------|-------|---------|-------------|---------------|
| `__init__.py` | 20 | Re-exports public API | `check_oa_status()`, `download_papers()`, `generate_manual_list()` | — |
| `check_oa.py` | 198 | Unpaywall API OA status lookup | `check_oa_status()` | `requests` |
| `download.py` | 458 | 5-strategy cascade PDF downloader | `download_papers()`, `is_valid_pdf()` | `requests`, `tarfile` |
| `manual_list.py` | 258 | HTML + CSV manual download list | `generate_manual_list()` | — |

**Download strategies:** Direct Unpaywall URL → PMC OA package (Europe PMC + NCBI tar.gz) → IEEE stamp page scrape → MDPI URL construction → DOI redirect with content negotiation

**CLI entry points:**
- `python -m engine.acquisition.check_oa --review NAME [--spec YAML] [--background]`
- `python -m engine.acquisition.download --review NAME [--retry] [--background]`
- `python -m engine.acquisition.manual_list --review NAME [--spec YAML] [--background]`

---

## engine/parsers/

| File | Lines | Purpose | Key Exports | External Deps |
|------|-------|---------|-------------|---------------|
| `__init__.py` | 0 | Empty module | — | — |
| `models.py` | 19 | Parsed document data model | `ParsedDocument` (Pydantic) | `pydantic` |
| `pdf_parser.py` | 226 | PDF → Markdown with Docling/Qwen2.5-VL routing | `parse_pdf()`, `parse_all_pdfs()`, `is_scanned_pdf()`, `compute_pdf_hash()` | `fitz` (PyMuPDF), `docling`, `ollama` |

**Routing:** Digital PDFs (> 100 chars/page) → Docling; Scanned PDFs (< 100 chars/page) → Qwen2.5-VL vision model via Ollama

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
| `__init__.py` | 44 | Re-exports public API | All major functions from submodules | — |
| `workflow.py` | 343 | 10-stage workflow state machine | `WORKFLOW_STAGES`, `advance_stage()`, `complete_stage()`, `format_workflow_status()`, `get_current_blocker()`, `is_adjudication_complete()` | `sqlite3` |
| `schema.py` | 59 | DDL for adjudication tables | `ensure_adjudication_table()` | `sqlite3` |
| `categorizer.py` | 260 | FP category config + keyword/regex matching | `CategoryConfig`, `categorize_paper()`, `generate_starter_config()`, `load_config()` | `yaml` |
| `screening_adjudicator.py` | 579 | Export/import screening adjudication queue | `export_adjudication_queue()`, `import_adjudication_decisions()`, `check_adjudication_gate()` | `openpyxl` |
| `audit_adjudicator.py` | 594 | Export/import audit review queue | `export_audit_review_queue()`, `import_audit_review_decisions()`, `check_audit_review_gate()` | `openpyxl` |
| `advance_stage.py` | 100 | CLI for manual workflow advancement | `main()` | — |

**CLI:** `python -m engine.adjudication.advance_stage --review NAME --stage STAGE --note NOTE [--force] [--status]`

---

## engine/review/

| File | Lines | Purpose | Key Exports | External Deps |
|------|-------|---------|-------------|---------------|
| `__init__.py` | 0 | Empty module | — | — |
| `human_review.py` | 350 | CSV-based human review queue (alternative to Excel) | `export_review_queue()`, `import_review_decisions()`, `bulk_accept()` | `csv`, `difflib` |

**Decisions:** `ACCEPT`, `ACCEPT_CORRECTED` (with snippet validation), `REJECT_VALUE` (set value to "NR"), `REJECT_PAPER`

---

## engine/exporters/

| File | Lines | Purpose | Key Exports | External Deps |
|------|-------|---------|-------------|---------------|
| `__init__.py` | 71 | Master export orchestrator | `export_all()` | — |
| `prisma.py` | 134 | PRISMA flow data + CSV | `generate_prisma_flow()`, `export_prisma_csv()` | `csv` |
| `evidence_table.py` | 176 | Evidence CSV + 3-sheet Excel | `export_evidence_csv()`, `export_evidence_excel()` | `openpyxl` |
| `docx_export.py` | 111 | Formatted DOCX evidence table | `export_evidence_docx()` | `python-docx` |
| `methods_section.py` | 81 | PRISMA methods paragraph | `generate_methods_section()`, `export_methods_md()` | — |
| `trace_exporter.py` | 549 | Per-paper traces, quality report, disagreement pairs | `export_traces_markdown()`, `export_trace_quality_report()`, `export_disagreement_pairs()` | `statistics` |

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
| `rescreen_original_251.py` | 170 | Re-screen original corpus with updated criteria (read-only, writes staging CSV) |
| `reextract_all.py` | 162 | Full re-extraction + re-audit of all papers (admin override) |
| `reextract_failed.py` | 161 | Re-extract specific failed papers with extended timeout (up to 25 min) |
| `reparse_cloud_spans.py` | 105 | Re-parse cloud extractions with 0 spans using stored JSON (no API calls) |
| `eval_auditor_models.py` | 279 | Multi-model auditor evaluation (5-paper sample, 3 candidate models) |
| `advance_to_pdf_acquired.py` | 102 | Bulk SCREENED_IN → PDF_ACQUIRED transition for papers with PDFs on disk |
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
| `test_database.py` | 434 | 27 | Tables, lifecycle, transitions, staleness, reject, min_status |
| `test_screener.py` | 254 | 11 | Structured output, dual-pass, verification logic |
| `test_pdf_parser.py` | 175 | 9 | Hash, routing, Docling integration, versioning |
| `test_extractor.py` | 353 | 17 | Prompt, thinking trace, two-pass, ellipsis retry |
| `test_auditor.py` | 490 | 26 | Grep verify, semantic verify, full audit mocked |
| `test_exporters.py` | 268 | 8 | PRISMA, CSV, Excel, DOCX, methods, export_all |
| `test_cloud_extraction.py` | 361 | 18 | Cloud tables, span parsing, store, CLI |
| `test_adjudication.py` | 563 | 37 | Categorizer, screening export/import, gate checks |
| `test_audit_adjudication.py` | 349 | 15 | Audit export/import, spot-check, reject, min_status |
| `test_workflow.py` | 316 | 30 | 10-stage workflow enforcement, blockers, format |
| `test_human_review.py` | 121 | 6 | Human review queue export/import |
| `test_background.py` | 112 | 7 | tmux background mode |
| `test_trace_exporter.py` | 186 | 11 | Per-paper traces, quality report, disagreements |

**Total: 256 offline tests passing** (10 network/ollama tests deselected by default)

```bash
python -m pytest tests/ -v -m "not network and not ollama"  # 256 passed
python -m pytest tests/ -v                                   # all 266
```

---

*Generated 2026-03-12 from commit `d65d614`*
