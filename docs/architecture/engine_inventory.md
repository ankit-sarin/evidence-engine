# Engine Module Inventory

> Auto-generated reference for all Python modules under `engine/`.

---

## core/

### `core/__init__.py`
- **Purpose**: Package marker.

### `core/constants.py`
- **Purpose**: Shared regex patterns for validation across extraction and audit agents.
- **Key exports**:
  - `INVALID_SNIPPET_RE` — Detects ellipsis patterns (`...`, `[...]`) in source snippets that indicate truncation.
- **Dependencies**: None

### `core/review_spec.py`
- **Purpose**: YAML-based review specification parser with Pydantic validation and protocol hashing.
- **Key exports**:
  - `ReviewSpec` — Top-level spec model (title, PICO, screening criteria, extraction schema, model config).
  - `ExtractionSchema` / `ExtractionField` — Structured field definitions with tiers 1–4.
  - `ScreeningModels` — Primary + verifier model names (defaults: qwen3:8b + gemma3:27b).
  - `PICO` — Population / Intervention / Comparator / Outcomes.
  - `SearchStrategy` — Databases, query terms, date range.
  - `load_review_spec(path)` — Load and validate YAML from disk.
  - `screening_hash()` / `extraction_hash()` — SHA-256 protocol hashes for staleness detection.
- **Dependencies**: None

### `core/database.py`
- **Purpose**: SQLite state machine managing the paper lifecycle, extractions, evidence spans, and audit records.
- **Key exports**:
  - `ReviewDatabase` — Main DB manager (one per review). Methods:
    - `add_papers()`, `update_status()`, `get_papers_by_status()`, `reject_paper()`
    - `add_screening_decision()`, `add_verification_decision()`, `get_screening_summary()`
    - `add_extraction()`, `add_extraction_atomic()`, `get_stale_extractions()`
    - `add_evidence_span()`, `update_audit()`
    - `reset_for_reaudit()`, `reset_for_reextraction()`, `cleanup_orphaned_spans()`
    - `min_status_gate()`, `get_pipeline_stats()`
  - `STATUSES` — 11 lifecycle states (INGESTED through REJECTED).
  - `ALLOWED_TRANSITIONS` — Directed graph of valid state changes.
  - `_STATUS_ORDER` — Ordered levels for min_status filtering (PARSED=0 … HUMAN_AUDIT_COMPLETE=4).
- **Dependencies**: `engine.search.models`, `engine.adjudication.schema`

---

## search/

### `search/__init__.py`
- **Purpose**: Package marker.

### `search/models.py`
- **Purpose**: Shared citation data model across all search sources.
- **Key exports**:
  - `Citation` — Paper metadata: pmid, doi, title, abstract, authors, journal, year, source, raw_data.
- **Dependencies**: None

### `search/pubmed.py`
- **Purpose**: PubMed literature search via Biopython Entrez.
- **Key exports**:
  - `search_pubmed(spec)` — Two-phase search (esearch → efetch) with pagination and rate limiting (0.34s, <3 req/s).
  - `_build_query()` — Combine query terms with AND + date range.
- **Dependencies**: `engine.core.review_spec`, `engine.search.models`

### `search/openalex.py`
- **Purpose**: OpenAlex literature search via pyalex library.
- **Key exports**:
  - `search_openalex(spec)` — Cursor-paginated search (200/page) with retry on HTTP errors.
  - `reconstruct_abstract(inverted_index)` — Reassemble OpenAlex inverted-index abstracts.
- **Dependencies**: `engine.core.review_spec`, `engine.search.models`

### `search/dedup.py`
- **Purpose**: Deduplicate citations from PubMed and OpenAlex.
- **Key exports**:
  - `deduplicate(pubmed, openalex)` — Phase 1: exact DOI/PMID/title match (O(n)); Phase 2: fuzzy title (SequenceMatcher >0.9).
  - `DedupResult` — unique_citations, duplicate_pairs, stats.
  - `normalize_title()`, `title_similarity()` — Title normalization and comparison.
- **Dependencies**: `engine.search.models`

---

## agents/

### `agents/__init__.py`
- **Purpose**: Package marker.

### `agents/models.py`
- **Purpose**: Pydantic models for extraction output.
- **Key exports**:
  - `EvidenceSpan` — Single extracted field (field_name, value, source_snippet, confidence 0–1, tier). Includes `clamp_confidence()` validator for DeepSeek's -1 outputs.
  - `ExtractionResult` — Full extraction: paper_id, fields list, reasoning_trace, model, schema_hash.
  - `ExtractionOutput` — Ollama grammar-constrained Pass 2 schema.
- **Dependencies**: None

### `agents/screener.py`
- **Purpose**: Dual-model screening with role-aware prompts.
- **Key exports**:
  - `screen_paper(paper, spec, role)` — Screen one paper via Ollama.
  - `ScreeningDecision` — Structured output: decision, rationale, confidence.
  - `run_screening(db, spec)` — Dual-pass primary screening on INGESTED papers (checkpoint/resume).
  - `run_verification(db, spec)` — Re-screen SCREENED_IN with verifier model + 4 FP-catching tests.
  - `_build_prompt(role)` — Primary sees simplified exclusions (high recall); verifier sees full criteria (high precision).
- **Dependencies**: `engine.core.database`, `engine.core.review_spec`

### `agents/extractor.py`
- **Purpose**: Two-pass extraction agent using DeepSeek-R1:32b via Ollama.
- **Key exports**:
  - `build_extraction_prompt(spec, paper_text)` — Generate prompt with schema fields by tier.
  - `extract_pass1_reasoning(paper_text, spec)` — Pass 1: free reasoning trace.
  - `extract_pass2_structured(reasoning, spec)` — Pass 2: grammar-constrained JSON.
  - `run_extraction(db, spec, review_name)` — Full extraction pipeline with ellipsis retry.
- **Settings**: MODEL=deepseek-r1:32b, TIMEOUT=900s, MAX_RETRIES=2.
- **Dependencies**: `engine.agents.models`, `engine.core.constants`, `engine.core.database`, `engine.core.review_spec`

### `agents/auditor.py`
- **Purpose**: Cross-model audit agent — grep + semantic verification of extractions.
- **Key exports**:
  - `grep_verify(snippet, paper_text)` — Exact match → fuzzy match (>0.85) on normalized text.
  - `semantic_verify(span, paper_text, ollama_options)` — LLM-based semantic check.
  - `audit_span(span_data, paper_text, ollama_options)` — Combined grep + semantic audit.
  - `run_audit(db, review_name, spec)` — Audit all EXTRACTED papers.
  - `AuditVerdict` — status (verified/flagged), grep_found, reasoning.
- **Settings**: DEFAULT_AUDITOR_MODEL=gemma3:27b, SEMANTIC_ONLY_TIERS={4}.
- **Dependencies**: `engine.agents.models`, `engine.core.constants`, `engine.core.database`, `engine.core.review_spec`

---

## parsers/

### `parsers/__init__.py`
- **Purpose**: Package marker.

### `parsers/models.py`
- **Purpose**: Data model for PDF parsing results.
- **Key exports**:
  - `ParsedDocument` — paper_id, source_pdf_path, pdf_hash, parsed_markdown, parser_used, parsed_at, version.
- **Dependencies**: None

### `parsers/pdf_parser.py`
- **Purpose**: Route PDFs between Docling (digital) and Qwen2.5-VL (scanned).
- **Key exports**:
  - `parse_pdf(db, paper_id, review_name)` — Hash → cache check → route to parser → save → record.
  - `compute_pdf_hash(path)` — SHA-256 of PDF content.
  - `is_scanned_pdf(path)` — Heuristic: <100 chars/page → scanned.
  - `parse_with_docling(path)`, `parse_with_vision(path)` — Parser implementations.
  - `parse_all_pdfs(db, review_name)` — Batch parse all PDF_ACQUIRED papers.
- **Dependencies**: `engine.core.database`, `engine.parsers.models`

---

## cloud/

### `cloud/__init__.py`
- **Purpose**: Cloud extraction arm exports.
- **Key exports**: `OpenAIExtractor`, `AnthropicExtractor`, `init_cloud_tables`.
- **Dependencies**: `engine.cloud.schema`, `engine.cloud.openai_extractor`, `engine.cloud.anthropic_extractor`

### `cloud/schema.py`
- **Purpose**: Database schema for cloud concordance extraction tables.
- **Key exports**:
  - `init_cloud_tables(conn)` — Create `cloud_extractions` + `cloud_evidence_spans` tables.
- **Dependencies**: None

### `cloud/base.py`
- **Purpose**: Shared base class for cloud API extraction arms.
- **Key exports**:
  - `CloudExtractorBase` — Base with `get_pending_papers()`, `load_parsed_text()`, `build_prompt()`, `parse_response_to_spans()` (handles 8+ JSON key variants, markdown fences, flat dicts).
  - `store_result()`, `get_progress()` — DB operations.
- **Dependencies**: `engine.agents.extractor`, `engine.cloud.schema`, `engine.core.review_spec`

### `cloud/openai_extractor.py`
- **Purpose**: OpenAI o4-mini concordance arm.
- **Key exports**:
  - `OpenAIExtractor` — `extract_paper()` with reasoning_effort=high. Pricing: $1.10/$4.40 per M tokens.
- **Dependencies**: `engine.cloud.base`

### `cloud/anthropic_extractor.py`
- **Purpose**: Anthropic Sonnet 4.6 concordance arm.
- **Key exports**:
  - `AnthropicExtractor` — `extract_paper()` with extended thinking (10K budget). Pricing: $3.00/$15.00 per M tokens.
- **Dependencies**: `engine.cloud.base`

---

## adjudication/

### `adjudication/__init__.py`
- **Purpose**: Re-exports for screening + audit adjudication.
- **Dependencies**: All adjudication submodules.

### `adjudication/schema.py`
- **Purpose**: DDL for screening_adjudication + audit_adjudication tables.
- **Key exports**:
  - `ensure_adjudication_table(conn)` — Create both tables + workflow_state.
- **Dependencies**: `engine.adjudication.workflow`

### `adjudication/workflow.py`
- **Purpose**: 9-stage sequential workflow state machine (5 screening + 4 extraction).
- **Key exports**:
  - `WORKFLOW_STAGES` — Tuple of 9 ordered stage names.
  - `SCREENING_STAGES` / `EXTRACTION_STAGES` — Subsets for display grouping.
  - `ensure_workflow_table()`, `complete_stage()`, `bypass_stage()`, `reset_stage()`
  - `is_stage_done()`, `can_advance_to()`, `advance_stage()`
  - `get_current_blocker()`, `is_adjudication_complete()`, `is_audit_review_complete()`
  - `format_workflow_status()` — Human-readable display with section headers.
- **Dependencies**: None

### `adjudication/categorizer.py`
- **Purpose**: Rule-based categorization of flagged papers into FP pattern groups.
- **Key exports**:
  - `CategoryConfig` — Load from YAML, `categorize_paper()` against regex/keyword rules.
  - `load_config(review_name)`, `generate_starter_config()`.
- **Dependencies**: None

### `adjudication/screening_adjudicator.py`
- **Purpose**: Export/import screening adjudication queue (flagged papers).
- **Key exports**:
  - `export_adjudication_queue(db, output_path)` — Collect DB + expanded flagged papers → Excel.
  - `import_adjudication_decisions(db, input_path)` — Read decisions, update DB, auto-advance workflow.
- **Dependencies**: `engine.adjudication.categorizer`, `engine.adjudication.schema`, `engine.adjudication.workflow`, `engine.core.database`

### `adjudication/audit_adjudicator.py`
- **Purpose**: Export/import audit review queue (contested/flagged extraction spans).
- **Key exports**:
  - `export_audit_review_queue(db, output_path)` — Per-paper rows with per-field columns → Excel.
  - `import_audit_review_decisions(db, input_path)` — Accept/override/reject, record originals, auto-advance.
  - `check_audit_review_gate(db)` — Count papers with unresolved spans.
  - Spot-check: random 10% of all-verified papers; >20% failure rate promotes all.
- **Dependencies**: `engine.adjudication.schema`, `engine.adjudication.workflow`, `engine.core.database`

### `adjudication/advance_stage.py`
- **Purpose**: CLI to manually advance workflow stages.
- **Key exports**:
  - `main()` — argparse CLI: `--review`, `--stage`, `--note`, `--force`, `--status`.
- **Dependencies**: `engine.adjudication.workflow`, `engine.core.database`

---

## exporters/

### `exporters/__init__.py`
- **Purpose**: Convenience `export_all()` that runs all exporters with min_status threading.
- **Dependencies**: All exporter submodules, `engine.core.database`, `engine.core.review_spec`

### `exporters/prisma.py`
- **Purpose**: PRISMA flow diagram data and CSV export.
- **Key exports**:
  - `generate_prisma_flow(db)` — Compute counts: identified, duplicates, screened, excluded, assessed, included.
- **Dependencies**: `engine.core.database`

### `exporters/evidence_table.py`
- **Purpose**: Evidence table as CSV and Excel (3-sheet workbook).
- **Key exports**:
  - `_build_evidence_rows(db, spec, min_status)` — Build rows filtered by `_STATUS_ORDER`.
  - `export_evidence_csv()`, `export_evidence_excel()` — Output files.
- **Dependencies**: `engine.core.database`, `engine.core.review_spec`

### `exporters/docx_export.py`
- **Purpose**: Landscape DOCX evidence table for journal submission.
- **Key exports**:
  - `export_evidence_docx(db, spec, output_path, min_status)` — Professional table with per-field columns.
- **Dependencies**: `engine.core.database`, `engine.core.review_spec`

### `exporters/methods_section.py`
- **Purpose**: Auto-generated PRISMA methods paragraph.
- **Key exports**:
  - `generate_methods_section(db, spec)` — Narrative paragraph.
  - `export_methods_md(db, spec, output_path)` — Write to Markdown.
- **Dependencies**: `engine.core.database`, `engine.core.review_spec`, `engine.exporters.prisma`

### `exporters/trace_exporter.py`
- **Purpose**: Extraction quality metrics, per-paper traces, disagreement pairs.
- **Key exports**:
  - `export_trace_quality_report(db_path)` — JSON + Markdown quality report.
  - `export_traces_markdown(db_path, output_dir)` — Per-paper Markdown trace files.
  - `export_disagreement_pairs(db_path, output_path)` — Concordance disagreement CSV.
- **Dependencies**: None (reads SQLite directly)

---

## review/

### `review/__init__.py`
- **Purpose**: Package marker.

### `review/human_review.py`
- **Purpose**: Human review infrastructure for flagged/contested spans.
- **Key exports**:
  - `export_review_queue(db, output_path, review_name)` — CSV with context windows for each span.
  - `import_review_decisions(db, input_path)` — Process ACCEPT/ACCEPT_CORRECTED/REJECT_VALUE/REJECT_PAPER.
- **Dependencies**: `engine.agents.auditor`, `engine.core.database`

---

## utils/

### `utils/__init__.py`
- **Purpose**: Package marker.

### `utils/background.py`
- **Purpose**: Launch long-running scripts in detached tmux sessions.
- **Key exports**:
  - `maybe_background(stage, review_name)` — Check `--background` flag; if set, re-launch in tmux with log tee.
- **Dependencies**: None

---

## Summary

| Layer | Modules | Purpose |
|-------|---------|---------|
| core | 3 | Review spec, database state machine, shared constants |
| search | 4 | PubMed + OpenAlex search, deduplication |
| agents | 4 | Screening, extraction, audit (local Ollama models) |
| parsers | 2 | PDF → Markdown routing (Docling / Qwen2.5-VL) |
| cloud | 5 | Concordance extraction (OpenAI + Anthropic APIs) |
| adjudication | 7 | 9-stage workflow, screening + audit human review |
| exporters | 6 | PRISMA, evidence tables, DOCX, methods, traces |
| review | 1 | Per-span human review queue |
| utils | 1 | Tmux background launcher |
| **Total** | **33** | |
