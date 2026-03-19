# Pipeline Data Flow

Each stage of the pipeline is described below with its trigger, handler module, database transitions, artifacts, and CLI commands.

---

## 1. SEARCH

**Modules:** `engine/search/pubmed.py`, `engine/search/openalex.py`, `engine/search/dedup.py`

**Flow:**
1. Build PubMed query from ReviewSpec search_strategy (query_terms AND'd, date_range as `[dp]` filter)
2. ESearch → EFetch in batches of 500 (rate limit: 3 req/s via 0.34s delay, 3 retries with exponential backoff)
3. Build OpenAlex query (publication_year range, type="article|review", pagination 200/page with retry)
4. Two-phase deduplication:
   - Phase 1 — exact match: DOI (case-insensitive), PMID (exact), normalized title
   - Phase 2 — fuzzy match: SequenceMatcher > 0.9 on unresolved records only
5. Merge: PubMed records seeded first, OpenAlex fills missing fields (doi, pmid, abstract, journal, year, authors)

**DB transitions:** → INGESTED

**Artifacts:** Papers inserted into `papers` table with source ("pubmed" or "openalex")

**CLI:**
```bash
python scripts/run_pipeline.py --spec review_specs/surgical_autonomy_v1.yaml --name surgical_autonomy
```

---

## 2. ABSTRACT SCREENING

**Module:** `engine/agents/screener.py`

**Flow:**
1. **Primary dual-pass** (`run_screening()`):
   - Pass 1: `screen_paper(pass_number=1, role="primary")` with spec.screening_models.primary (default qwen3:8b), simplified exclusion criteria (high recall)
   - Pass 2: `screen_paper(pass_number=2, role="primary")` with same primary model, full exclusion criteria
   - Resolution: both include → ABSTRACT_SCREENED_IN; both exclude → ABSTRACT_SCREENED_OUT; disagreement → ABSTRACT_SCREEN_FLAGGED
2. **Verification** (`run_verification()`):
   - Single pass on ABSTRACT_SCREENED_IN papers with spec.screening_models.verification (gemma3:27b)
   - Verifier include → stays ABSTRACT_SCREENED_IN; verifier exclude → ABSTRACT_SCREEN_FLAGGED
   - Auto-advances workflow: ABSTRACT_SCREENING_COMPLETE

**Prompt design:**
- Primary role sees simplified exclusions (4 hardcoded rules: SRs/MAs, editorials, non-surgical robotics, no abstract)
- Verifier role sees full `spec.screening_criteria.exclusion` list
- Specialty scope injected via `spec.specialty_scope.format_for_prompt()` if present
- All calls: temperature=0, think=False (`/no_think`), structured JSON output via `ScreeningDecision.model_json_schema()`

**DB transitions:** INGESTED → {ABSTRACT_SCREENED_IN, ABSTRACT_SCREENED_OUT, ABSTRACT_SCREEN_FLAGGED}

**Checkpoint/resume:** Screened paper IDs saved to `screening_checkpoint.json` / `screening_checkpoint_verify.json`

**Data retention:** All paper data retained permanently. SCREENED_OUT is a label, not a deletion.

**Expanded search variant:** `scripts/screen_expanded.py` — three-phase (fetch abstracts → dual-pass screen → verify), writes to CSV files

**CLI:**
```bash
python scripts/screen_expanded.py                # all phases
python scripts/screen_expanded.py --screen-only  # primary dual-pass only
python scripts/screen_expanded.py --verify-only  # verification pass only
```

---

## 3. ABSTRACT SCREENING ADJUDICATION

**Modules:** `engine/adjudication/categorizer.py`, `engine/adjudication/screening_adjudicator.py`, `engine/adjudication/abstract_adjudication_html.py`

**Flow:**
1. **Diagnostic** (manual): Human reviews 50-paper FP sample → workflow stage ABSTRACT_DIAGNOSTIC_COMPLETE
2. **Categorize:** Rule-based FP categorization from `adjudication_categories.yaml` (regex + keyword matching) → ABSTRACT_CATEGORIES_CONFIGURED
3. **Export queue:** `export_adjudication_queue()` produces interactive HTML (card-based, auto-categorized, batch actions, keyboard shortcuts, localStorage draft) → ABSTRACT_QUEUE_EXPORTED
4. **Human review:** INCLUDE or EXCLUDE each ABSTRACT_SCREEN_FLAGGED paper
5. **Import decisions:** `import_adjudication_decisions()` validates and applies (supports .json from HTML tool, .xlsx from workbook)
   - INCLUDE → ABSTRACT_SCREENED_IN
   - EXCLUDE → ABSTRACT_SCREENED_OUT
   - Records in `abstract_screening_adjudication` table
   - Auto-advances: ABSTRACT_ADJUDICATION_COMPLETE when all flagged resolved

**HTML features:** Category color-coded badges, category filtering, batch "Exclude all in [category]", keyboard shortcuts (I=Include, E=Exclude, ↑↓=navigate), JSON export

**Naming convention:** `{review}_abstract_adjudication_queue.html` / `{review}_abstract_adjudication_decisions.json`

---

## 4. PDF ACQUISITION + QUALITY CHECK

**Modules:** `engine/acquisition/check_oa.py`, `engine/acquisition/download.py`, `engine/acquisition/verify_downloads.py`, `engine/acquisition/pdf_quality_check.py`, `engine/acquisition/pdf_quality_html.py`, `engine/acquisition/pdf_quality_import.py`

**Flow (6-step iterative loop):**

1. **OA check:** Query Unpaywall API (1 req/sec rate limit, 15s timeout) for OA status + PDF URL. Updates papers: `oa_status`, `pdf_url`. Skips terminal statuses (ABSTRACT_SCREENED_OUT, REJECTED, PDF_EXCLUDED, FT_SCREENED_OUT). All acquisition modules use `ReviewDatabase` as a context manager
2. **Download:** 5-strategy cascade with %PDF magic byte validation:
   - Strategy 1: Direct Unpaywall URL
   - Strategy 2: PMC OA package (tar.gz extraction, 100 MB ceiling)
   - Strategy 3: IEEE stamp.jsp + iframe PDF (10.1109 DOIs only)
   - Strategy 4: MDPI URL construction (mdpi-like DOIs only)
   - Strategy 5: DOI redirect with content negotiation + `/pdf` suffix
   - Invalid PDFs quarantined to `{pdf_dir}/quarantine/` (not deleted)
   - 2s delay between downloads
3. **Verify/rename:** Match downloaded files to papers via 3 patterns (bare integer, EE-prefixed, rich name). Validate: %PDF header, ≥10 KB, no HTML error pages. Rename to canonical `EE-{nnn}_{Author}_{Year}.pdf`. Update `papers.pdf_local_path` + `full_text_assets.pdf_path`
4. **AI quality check:** First-page rendering (PyMuPDF, configurable DPI) → base64 PNG → qwen2.5vl:7b classification → JSON `{language, content_type, confidence}`. Content types: full_manuscript, abstract_only, trial_registration, editorial_erratum, conference_poster, other
5. **HTML review:** Interactive page with sticky summary bar, disposition dropdowns, localStorage draft saves. Two modes: acquisition (papers without PDFs) and quality_check (three sections: needs disposition, flagged by AI, passed)
6. **Import dispositions:** Atomic two-pass validation → DB writes:
   - PROCEED → `pdf_quality_check_status = 'HUMAN_CONFIRMED'`
   - EXCLUDE_* → `status = 'PDF_EXCLUDED'` (terminal) with reason code
   - PDF_WILL_ATTEMPT → no change (pending)

**DB transitions:** ABSTRACT_SCREENED_IN → PDF_ACQUIRED → {PDF_EXCLUDED (terminal), PARSED}

**Workflow stage:** PDF_ACQUISITION (manual — human confirms all PDFs acquired)

**CLI:**
```bash
python -m engine.acquisition.check_oa --review surgical_autonomy --spec ...
python -m engine.acquisition.download --review surgical_autonomy [--retry] [--background]
python -m engine.acquisition.verify_downloads --review surgical_autonomy [--dry-run]
python -m engine.acquisition.pdf_quality_check --review surgical_autonomy --spec ... [--dry-run] [--limit N]
python -m engine.acquisition.pdf_quality_html --review surgical_autonomy [--mode acquisition|quality_check]
python -m engine.acquisition.pdf_quality_import --review surgical_autonomy --input dispositions.json [--dry-run]
```

---

## 5. PARSE

**Module:** `engine/parsers/pdf_parser.py`

**Flow — three-tier routing:**
1. Compute PDF SHA-256 hash; check for existing parse with same hash (skip if cached)
2. Heuristic: chars/page < threshold (default 100, configurable via `spec.pdf_parsing.scanned_text_threshold`) → scanned
3. If scanned → qwen2.5vl:7b vision (render each page to PNG at 200 DPI, send base64 to Ollama)
4. If digital → Docling (`DocumentConverter().convert().export_to_markdown()`)
5. If Docling fails or result is sparse (<100 chars) → PyMuPDF fallback (`page.get_text("text")` with `<!-- Page N -->` separators)
6. If PyMuPDF result is sparse → qwen2.5vl:7b vision
7. If all three tiers return empty text → raises `ValueError` (no file written, no DB row created)
8. Atomic write: temp file → DB commit → rename to `{paper_id}_v{version}.md`

**DB transitions:** PDF_ACQUIRED → PARSED

**Artifacts:** Markdown files at `data/{review}/parsed_text/{paper_id}_v{version}.md`. Parser used recorded in `full_text_assets.parser_used` (one of: docling, pymupdf, qwen2.5vl)

**DB-driven path resolution** (`parse_all_pdfs()`):
1. `full_text_assets.pdf_path` (post-rename canonical path)
2. `papers.pdf_local_path` (post-download path)
3. Glob fallback: `{paper_id}_*.pdf` or `{paper_id}.pdf`

**Hash verification:** `verify_hashes()` checks stored `pdf_content_hash` against current files, returns list of mismatches

---

## 6. FULL-TEXT SCREENING

**Module:** `engine/agents/ft_screener.py`

**Flow:**
1. **Pre-flight:** `require_preflight()` verifies both screening models loaded and responsive
2. **Text preparation:** `truncate_paper_text()` — keeps title + abstract header, fills with body text up to 32K chars (`FT_MAX_TEXT_CHARS`). Section-aware: tries to cut at References or last sentence boundary
3. **Primary screening** (`run_ft_screening()`):
   - Processes PARSED + AI_AUDIT_COMPLETE papers with parsed text
   - `ft_screen_paper()` via spec.ft_screening_models.primary (qwen3:32b)
   - Decision: FT_ELIGIBLE or FT_EXCLUDE with reason_code
   - 7 reason codes: eligible, wrong_specialty, no_autonomy_content, wrong_intervention, protocol_only, duplicate_cohort, insufficient_data
   - **Missing parsed text:** Papers without parsed text are marked FT_FLAGGED with reason_code `no_parsed_text` (not silently skipped)
   - **Status-aware:** papers already past FT gate (EXTRACTED, AI_AUDIT_COMPLETE, etc.) record decision without status change
4. **Verification** (`run_ft_verification()`):
   - Processes FT_ELIGIBLE papers with spec.ft_screening_models.verifier (default gemma3:27b)
   - 5-test FP catcher criteria
   - FT_ELIGIBLE → stays FT_ELIGIBLE; FT_FLAGGED → FT_FLAGGED
   - Auto-advances workflow: FULL_TEXT_SCREENING_COMPLETE

**Prompt design:** PICO block, inclusion/exclusion criteria, 7 reason codes, specialty scope. `/no_think` appended. Structured JSON output via `FTScreeningDecision`/`FTVerificationDecision` schemas

**DB transitions:** PARSED → {FT_ELIGIBLE, FT_SCREENED_OUT, FT_FLAGGED}

**Checkpoint/resume:** `ft_screening_checkpoint.json` / `ft_screening_checkpoint_verify.json`

**CLI:**
```bash
python -m engine.agents.ft_screener --review surgical_autonomy --spec ...
python -m engine.agents.ft_screener ... --screen-only
python -m engine.agents.ft_screener ... --verify-only
```

---

## 7. FULL-TEXT SCREENING ADJUDICATION

**Modules:** `engine/adjudication/ft_screening_adjudicator.py`, `engine/adjudication/ft_adjudication_html.py`

**Flow:**
1. **Export queue:** `export_ft_adjudication_queue()` collects FT_FLAGGED papers with reason codes + verifier rationale + parsed text excerpt (~500 chars). Produces HTML (table-based, localStorage draft, keyboard navigation) or xlsx
2. **Human review:** FT_ELIGIBLE or FT_SCREENED_OUT for each flagged paper
3. **Import decisions:** `import_ft_adjudication_decisions()` validates and applies
   - FT_ELIGIBLE → FT_ELIGIBLE status
   - FT_SCREENED_OUT → FT_SCREENED_OUT status (terminal)
   - Status update failures tracked separately (not counted as success)
   - Records in `ft_screening_adjudication` table
   - Auto-advances: FULL_TEXT_ADJUDICATION_COMPLETE when all resolved

**Naming convention:** `{review}_ft_adjudication_queue.html` / `{review}_ft_adjudication_decisions.json`

---

## 8. EXTRACT (Local)

**Module:** `engine/agents/extractor.py`

**Flow:**
1. **Pre-flight:** `require_preflight()` + stale extraction check (`get_stale_extractions()`)
2. **Candidate selection:** Papers at FT_ELIGIBLE or PARSED (skip path). Skip if already extracted with current `extraction_schema_hash`
3. **Prompt building:** `build_extraction_prompt()` — codebook-driven from `extraction_codebook.yaml`. Fields organized by tier (1=explicit, 2=interpretive, 3=numeric, 4=judgment). Includes per-field: name, type, definition, instruction, valid_values, decision_criteria, examples, source_quote_required flag
4. **Pass 1 — reasoning:** `extract_pass1_reasoning()` with deepseek-r1:32b, temperature=0. Extracts `<think>...</think>` reasoning trace
5. **Pass 2 — structured:** `extract_pass2_structured()` with deepseek-r1:32b, temperature=0, think=False. Grammar-constrained JSON via `ExtractionOutput.model_json_schema()`. Reasoning trace from Pass 1 injected as context
6. **Snippet validation:** `_validate_and_retry_snippets()` — checks each `source_snippet` against `INVALID_SNIPPET_RE` (ellipsis bridging). Invalid snippets retried up to 2 times via targeted re-prompt. Unrepairable snippets set to empty string
7. **Atomic storage:** `db.add_extraction_atomic()` — single transaction inserts extraction + all evidence spans. Raises `ValueError` if 0 spans (prevents silent data loss)
8. **Proactive restart:** `restart_ollama()` every `RESTART_EVERY_N` papers (default 25). Polls `/api/tags` for up to 60s

**DB transitions:** FT_ELIGIBLE → {EXTRACTED, EXTRACT_FAILED}; PARSED → {EXTRACTED, EXTRACT_FAILED} (skip path)

**Model digest tracking:** Captures extractor + auditor model digests via `get_model_digest()` at run start. Stored per extraction for reproducibility

**Post-extraction:** Distribution monitor runs automatically at the end of the local extraction path (`run5_extract_and_audit.py` calls `run_post_extraction_check()` with arm="local")

**Constants:** `MODEL = "deepseek-r1:32b"`, `MAX_RETRIES = 2`, `RETRY_DELAY = 30`, `SNIPPET_MAX_RETRIES = 2`, `RESTART_EVERY_N = 25`

**CLI:**
```bash
python scripts/run5_extract_and_audit.py --review surgical_autonomy --spec ...
python scripts/run5_extract_and_audit.py ... --retry-failed
python scripts/run5_extract_and_audit.py ... --restart-every 0  # disable proactive restart
python scripts/run5_extract_and_audit.py ... --paper-ids 82 4 24
```

---

## 9. EXTRACT (Cloud Concordance Arms)

**Modules:** `engine/cloud/base.py`, `engine/cloud/openai_extractor.py`, `engine/cloud/anthropic_extractor.py`

**Flow:**
1. Both arms share the same codebook prompt (via `CloudExtractorBase.build_prompt()`)
2. Each arm processes papers with status IN (FT_ELIGIBLE, EXTRACTED, AI_AUDIT_COMPLETE, HUMAN_AUDIT_COMPLETE) missing a cloud extraction for that arm
3. **OpenAI arm:** `chat.completions.create()` with reasoning_effort=high, json_object format. Reasoning trace from `message.reasoning_content`
4. **Anthropic arm:** `messages.create()` with extended thinking (budget_tokens=10000), max_tokens=16000. Thinking blocks separated from text blocks. Markdown fence stripping
5. Response JSON parsed via `parse_response_to_spans()` (8+ alternate key names, flat dict restructuring, null → "NR")
6. `store_result()` — atomic insert to cloud_extractions + cloud_evidence_spans. Rejects 0-span results
7. Post-extraction: `run_distribution_check()` for the arm

**No paper status change:** Cloud extractions are parallel data stored in separate tables

**Cost tracking:** Per-extraction `cost_usd` = (input_tokens × rate / 1M) + (output_tokens × rate / 1M). Batch runs accept `--max-cost` to cap total spend

**CLI:**
```bash
PYTHONPATH=. python scripts/run_cloud_extraction.py --arm both --max-cost 25.00
PYTHONPATH=. python scripts/run_cloud_extraction.py --arm openai --max-papers 10
PYTHONPATH=. python scripts/run_cloud_extraction.py --progress
PYTHONPATH=. python scripts/run_cloud_extraction.py --dry-run
```

---

## 10. AUDIT

**Module:** `engine/agents/auditor.py`

**Flow:**
1. **Model resolution:** explicit `--model` param > `spec.auditor_model` > `DEFAULT_AUDITOR_MODEL` ("gemma3:27b")
2. **Candidate selection:** Papers at EXTRACTED with pending evidence spans
3. **Per-span audit** (`audit_span()`):
   - Absence values (`NOT_FOUND`, `NR`, etc.) → auto-verified
   - Invalid snippet (INVALID_SNIPPET_RE match) → `invalid_snippet`
   - Empty snippet on non-absence value → `flagged`
   - Tier 4 fields → semantic-only (skip grep)
   - `grep_verify()`: normalized substring match or sliding-window fuzzy match (SequenceMatcher > 0.85)
   - `semantic_verify()`: LLM check via gemma3:27b — categorical fields ask "Does source support this classification?", text fields ask "Does value match snippet?"
   - Outcomes: `verified` (grep+semantic pass), `contested` (grep fail + semantic pass), `flagged` (semantic fail), `invalid_snippet`
4. **Status transition:** When no pending spans remain for a paper → AI_AUDIT_COMPLETE
5. **LOW_YIELD detection** (`check_low_yield()`): Post-audit, papers with fewer than `spec.low_yield_threshold` (default 4) populated fields flagged (`extractions.low_yield = 1`)

**DB transitions:** EXTRACTED → AI_AUDIT_COMPLETE

**Updates:** `evidence_spans`: audit_status, auditor_model, audit_rationale, audited_at

**Text normalization:** Whitespace collapse, glued punctuation fixing (Table.I → Table I), smart quote straightening (NFKC)

---

## 11. DISTRIBUTION COLLAPSE MONITORING

**Module:** `engine/validators/distribution_monitor.py`

**Flow:**
1. Load categorical field names from codebook
2. For each field, query all non-null values (routes to local/cloud/human table by arm name)
3. Compute: total non-null count, distinct value count, most common value percentage, Shannon entropy
4. Apply detection rules:
   - **COLLAPSED:** ≤1 distinct values AND ≥ collapsed_min_papers (default 10) → hard failure
   - **LOW_VARIANCE:** top value ≥ low_variance_threshold (default 0.85) AND ≥ low_variance_min_papers (default 20) → soft warning
   - **OK:** all other fields
5. `assert_no_collapse()` raises `DistributionCollapseError` on COLLAPSED fields; `--strict` also fails on LOW_VARIANCE

**Skip conditions:** `run_post_extraction_check()` skips if extracted_count < 10, failed_count > 0, or codebook missing

**Integrated into:** Cloud extractors (`run()` method calls `run_distribution_check()` post-extraction) and local pipeline

**CLI:**
```bash
python -m engine.validators.distribution_monitor --review surgical_autonomy --arm local
python -m engine.validators.distribution_monitor --review surgical_autonomy --arm anthropic_sonnet_4_6 [--strict]
```

---

## 12. HUMAN AUDIT REVIEW

**Modules:** `engine/adjudication/audit_adjudicator.py`, `engine/review/extraction_audit_html.py`, `engine/review/human_review.py`

**Flow:**
1. **Collect papers** (`_collect_papers_for_review()`):
   - AI_AUDIT_COMPLETE papers with contested/flagged/invalid_snippet spans
   - LOW_YIELD papers (all spans exported for full picture)
   - Spot-check sample (default 10% of papers with all-verified spans)
2. **Flatten to span rows:** One row per problematic span (or all spans for low_yield/spot_check papers)
3. **Export:** HTML (self-contained, per-span ACCEPT/REJECT/CORRECT interface with localStorage, JSON export) or xlsx (per-span rows with DataValidation dropdowns)
4. **Human review:** Per-span decisions (stats increment only after confirmed DB update):
   - ACCEPT → span verified
   - REJECT → `audit_adjudication` record with `human_decision='reject_paper'`
   - CORRECT → `audit_adjudication` record with `human_decision='override'`, span value updated
5. **Import decisions:** Two-pass validation (reject entire import on any error)
   - Auto-discovers: `{review}_extraction_audit_decisions.json`
   - Supports .json (from HTML tool) and .xlsx (from workbook)
   - All verified spans → `audit_status = 'verified'`, `auditor_model = 'human_review'`
   - Paper transitions: AI_AUDIT_COMPLETE → HUMAN_AUDIT_COMPLETE when all spans resolved
   - Auto-advances: AUDIT_REVIEW_COMPLETE

**Naming convention:** `{review}_extraction_audit_queue.html` / `{review}_extraction_audit_decisions.json`

---

## 13. CONCORDANCE ANALYSIS

**Modules:** `engine/analysis/concordance.py`, `engine/analysis/scoring.py`, `engine/analysis/normalize.py`, `engine/analysis/metrics.py`, `engine/analysis/report.py`

**Flow:**
1. **Load arms:** `load_arm()` — queries evidence_spans (local) or cloud_evidence_spans (cloud arms) or human_extractions (human_A/B/C/D). Returns `{paper_id: {field_name: value}}`
2. **Schema parity check:** `check_schema_parity()` warns if extraction schema hashes differ across arms
3. **Align:** `align_arms()` — align by paper_id and field_name. Track a-only and b-only papers
4. **Normalize:** `normalize_for_concordance()` per field:
   - Null synonyms ("", "nr", "n/r", "not reported", "not_found", "none", "n/a") → None
   - Numeric fields: strip non-digits, return integer string
   - Categorical fields: canonical prefix matching (e.g., "2" → "2 (Task autonomy)")
   - Multi-value fields (semicolon-separated): → frozenset for set comparison
   - Free-text: lowercase, strip, collapse whitespace
5. **Score:** `score_pair()` per field:
   - Both None → MATCH; one None → MISMATCH
   - Sets: identical → MATCH; disjoint → MISMATCH; partial overlap → AMBIGUOUS (with Jaccard)
   - Categorical: exact → MATCH; else MISMATCH (no fuzzy)
   - Free-text: exact → MATCH; substring containment → MATCH; Jaccard > 0.7 → AMBIGUOUS; else MISMATCH
6. **Metrics:** `cohens_kappa()` — binary (MATCH/MISMATCH), AMBIGUOUS excluded from denominator but counted. Analytical SE (Fleiss, 1981) with 95% CI
7. **Reports:** Terminal summary table, `concordance_summary.csv`, `disagreements.csv`, `concordance_report.html` (branded, color-coded kappa, expandable disagreement sections)

**CLI:**
```bash
python -m engine.analysis.concordance --review surgical_autonomy --arms local,openai_o4_mini_high,anthropic_sonnet_4_6
```

---

## 14. EXPORT

**Modules:** `engine/exporters/prisma.py`, `engine/exporters/evidence_table.py`, `engine/exporters/docx_export.py`, `engine/exporters/methods_section.py`, `engine/exporters/trace_exporter.py`

### PRISMA Flow (`prisma.py`)

`generate_prisma_flow(db)` returns comprehensive counts: records_identified (by source), duplicates_removed, records_screened, records_excluded, screen_flagged, pdf_excluded, full_text_assessed, studies_included, rejected, rejection_reasons, low_yield counts, extraction failures. Includes PRISMA 2020 split: Reports Not Retrieved (INACCESSIBLE) vs Eligibility Exclusions (other reasons).

**Reconciliation:** Validates terminal + in-progress = total (no double-counting). Logs WARNING on mismatch.

### Evidence Table (`evidence_table.py`)

- `export_evidence_csv()` — one row per paper, columns: paper metadata + per-field (value, source_snippet, confidence, audit_status)
- `export_evidence_excel()` — three-sheet workbook: Evidence Table, Extraction Summary, Field Stats
- Papers with no extraction data marked `[NO EXTRACTION DATA]`; `--exclude-empty` flag omits them entirely

Both accept `min_status` parameter: `AI_AUDIT_COMPLETE` (raw AI) or `HUMAN_AUDIT_COMPLETE` (human-verified)

### DOCX (`docx_export.py`)

`export_evidence_docx()` — landscape orientation, 0.5" margins, Study (first author et al.) + Year + Journal + extraction fields. python-docx

### Methods Section (`methods_section.py`)

`generate_methods_section()` — auto-generated PRISMA methods paragraph. Reads model names dynamically from the review spec and DB metadata (actual models used per extraction/audit, not hardcoded). Covers: search strategy, screening process, exclusion criteria, extraction, audit

### Trace Export (`trace_exporter.py`)

`export_trace_quality_report()` — reasoning trace analysis: min/max/mean/median/stdev chars, under-500 count, truncated count. Outputs JSON + Markdown

### Self-Documenting Workbooks (`review_workbook.py`)

Shared builder used by all three adjudication exporters. Creates 3-sheet workbooks:
1. **Instructions** — review context, decision rules, import commands
2. **Review Queue** — data rows with DataValidation dropdowns, conditional formatting for blank cells, frozen header, auto-filter
3. **Reference** — verbatim screening criteria or custom content

Atomic write via temp file.

**CLI:**
```bash
python scripts/run_pipeline.py --spec ... --name ... --skip-to export
```

---

## Engine Design Principles

These cross-cutting patterns are enforced throughout the codebase.

### No Silent Data Loss

- **Zero-span rejection:** Both local (`engine/agents/extractor.py`) and cloud (`engine/cloud/base.py`) extractors raise `ValueError` if an extraction produces 0 evidence spans. Empty extractions are never stored
- **Empty parsed text guard:** `parse_pdf()` raises `ValueError` if all three parser tiers return empty text — no file written, no DB row created
- **Missing parsed text → FT_FLAGGED:** Papers without parsed text at FT screening time are marked FT_FLAGGED with reason_code `no_parsed_text`, not silently skipped
- **`load_arm()` raises on DB error:** `engine/analysis/concordance.py` propagates `sqlite3.OperationalError` on missing or corrupted databases instead of returning an empty dict
- **FT adjudication read failures logged:** If parsed text cannot be loaded for FT adjudication export, the paper is included with `[text unavailable]` marker rather than silently skipped

### Atomic Writes with Full Rollback

All multi-step database writes use explicit `BEGIN`/`COMMIT`/`ROLLBACK` transactions:
- `update_status()` — uses `BEGIN IMMEDIATE` (write lock acquired before validation read, preventing TOCTOU races)
- `add_extraction_atomic()` — extraction + all spans in one transaction
- `store_result()` (cloud) — cloud_extraction + all cloud_evidence_spans in one transaction
- `admin_reset_status()` — table creation + audit log + status update in one transaction
- `reset_for_reaudit()` / `reset_for_reextraction()` — multi-phase resets are fully atomic
- `import_dispositions()` (PDF quality) — full rollback on any failure
- Human review span updates — span audit_status + paper status transition in one transaction
- Extraction cleanup — evidence_spans delete + extractions delete + status reset in one transaction
- Import functions (adjudication, audit review) — validate 100% of input before any write; reject entire batch on any error
- All 6 exporters (CSV, Excel, DOCX, JSON, Markdown, PRISMA) — atomic temp-file-then-rename write

### Context Manager Protocol

`ReviewDatabase` implements `__enter__`/`__exit__` for use as a context manager, ensuring connections are closed on scope exit (including exceptions).

### Per-Paper Fault Isolation

Batch runs isolate failures at the paper level so one bad paper does not abort the pipeline:
- Abstract screener, FT screener, and auditor wrap each paper in try/except — malformed model output logs a warning and continues
- Cloud extractors wrap each `extract_paper()` call — failures increment the failed counter and the run continues
- Proactive `restart_ollama()` failures are logged and the run continues (graceful degradation, not crash)

### Fail-Fast Error Propagation

Structural errors that indicate a broken pipeline are raised, not logged and swallowed:
- Ollama timeouts raise after retry exhaustion (with restart recovery as final attempt)
- Pre-flight failures raise `RuntimeError` with actionable messages — all four agents (screener, FT screener, extractor, auditor) run pre-flight checks
- Schema validation failures raise `ReviewSpecError` — `load_review_spec()` catches `FileNotFoundError` and `YAMLError` with clear messages
- Distribution collapse raises `DistributionCollapseError`
- Authentication errors (cloud APIs) re-raise immediately without retry (OpenAI/Anthropic `AuthenticationError`)
- Migration errors: only "column already exists" is swallowed; all other `OperationalError` exceptions re-raise

### Distribution Monitor Integration

The distribution monitor (`engine/validators/distribution_monitor.py`) is wired into the automatic extraction completion path:
- **Local extraction:** `run5_extract_and_audit.py` calls `run_post_extraction_check()` after extraction completes
- **Cloud extraction:** Both `OpenAIExtractor.run()` and `AnthropicExtractor.run()` call `run_distribution_check()` post-extraction
- COLLAPSED fields are hard failures; LOW_VARIANCE fields are warnings (hard failures with `--strict`)

### Defensive Logging

- Malformed JSONL lines logged at WARNING with line number and content (`screening_adjudicator.py`) — not silently skipped
- `logger.exception` used in all except blocks that continue execution (tracebacks preserved)
- `EVIDENCE_ENGINE_CONTACT_EMAIL` env var used for API contact email in scripts (`backfill_authors.py`, `screen_expanded.py`); core engine modules (`pubmed.py`, `openalex.py`, `check_oa.py`, `download.py`) still use hardcoded addresses (migration in progress)

### Idempotent Operations

- All database migrations check for existing state before modifying
- Screening and FT screening use checkpoint/resume (JSON files of processed paper IDs)
- Extraction skips papers already extracted with the current `extraction_schema_hash`
- Import functions deduplicate by paper_id and skip already-processed records

*Generated 2026-03-19 from commit e124b20*
