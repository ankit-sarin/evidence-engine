# Pipeline Data Flow

Each stage of the pipeline is described below with its trigger, handler module, database transitions, artifacts, and CLI commands.

---

## 1. SEARCH

**Trigger:** First stage of `run_pipeline.py`, or manual invocation.

**Modules:**
- `engine/search/pubmed.py` — Biopython Entrez (esearch + efetch in batches of 500, rate-limited 3 req/s)
- `engine/search/openalex.py` — pyalex cursor pagination (200/page), abstract reconstruction from inverted index
- `engine/search/dedup.py` — Two-phase dedup: exact match (DOI → PMID → normalized title), then fuzzy title (SequenceMatcher > 0.9)

**Database transitions:**
- New papers inserted with status `INGESTED`
- Duplicates skipped by PMID UNIQUE constraint

**Artifacts:**
- SQLite `papers` table populated with title, abstract, DOI, PMID, authors, journal, year, source

**CLI:**
```bash
python scripts/run_pipeline.py --spec review_specs/surgical_autonomy_v1.yaml \
    --name surgical_autonomy
```

---

## 2. ABSTRACT SCREENING (Dual-Pass Primary + Verification)

**Trigger:** After SEARCH, or `--skip-to screen` in pipeline. Expanded search uses `screen_expanded.py`.

**Module:** `engine/agents/screener.py`

### Phase 1 — Fetch Abstracts (expanded search only)
- `scripts/screen_expanded.py --fetch-only`
- Queries OpenAlex (by DOI) and PubMed (by PMID) for abstracts
- Writes to `abstracts.jsonl` (append-only, crash-safe)

### Phase 2 — Primary Dual-Pass Screening
- Model: `qwen3:8b` (configurable via `spec.screening_models.primary`)
- Pass 1 (pass_number=1): Simplified exclusion criteria (high recall)
- Pass 2 (pass_number=2): Full exclusion criteria (high precision)
- Specialty scope injected from Review Spec when configured
- Decision logic:
  - Both include → `ABSTRACT_SCREENED_IN`
  - Both exclude → `ABSTRACT_SCREENED_OUT`
  - Disagree → `ABSTRACT_SCREEN_FLAGGED`
- Checkpoint/resume via JSON file

### Phase 3 — Verification
- Model: `gemma3:27b` (configurable via `spec.screening_models.verification`)
- Re-screens all `ABSTRACT_SCREENED_IN` papers with `role="verifier"` (full strict criteria)
- Verifier exclude → `ABSTRACT_SCREEN_FLAGGED`
- Auto-advances workflow stage `ABSTRACT_SCREENING_COMPLETE`

**Database transitions:**
- `INGESTED` → `ABSTRACT_SCREENED_IN` | `ABSTRACT_SCREENED_OUT` | `ABSTRACT_SCREEN_FLAGGED`
- `ABSTRACT_SCREENED_IN` → `ABSTRACT_SCREEN_FLAGGED` (if verifier excludes)
- Records in `abstract_screening_decisions` (pass 1 + 2) and `abstract_verification_decisions`

**Data Retention:** All paper data retained permanently regardless of outcome. `ABSTRACT_SCREENED_OUT` is a label, not a deletion.

**CLI:**
```bash
python scripts/screen_expanded.py                # all phases
python scripts/screen_expanded.py --screen-only  # phase 2 only
python scripts/screen_expanded.py --verify-only  # phase 3 only
```

---

## 3. ABSTRACT SCREENING ADJUDICATION

**Trigger:** Manual — human reviews flagged papers after abstract screening.

**Modules:** `engine/adjudication/screening_adjudicator.py`, `engine/adjudication/abstract_adjudication_html.py`, `engine/adjudication/categorizer.py`

**Steps:**
1. Human reviews 50-paper diagnostic sample → advance `ABSTRACT_DIAGNOSTIC_COMPLETE`
2. Create `adjudication_categories.yaml` (FP pattern groups) → auto-set `ABSTRACT_CATEGORIES_CONFIGURED`
3. Export flagged papers to HTML or Excel → auto-set `ABSTRACT_QUEUE_EXPORTED`
4. Human fills INCLUDE/EXCLUDE decisions
5. Import decisions → auto-set `ABSTRACT_ADJUDICATION_COMPLETE` (if zero unresolved)

**Database transitions:**
- `ABSTRACT_SCREEN_FLAGGED` → `ABSTRACT_SCREENED_IN` (INCLUDE) or `ABSTRACT_SCREENED_OUT` (EXCLUDE)
- Records in `screening_adjudication` table

**Artifacts:**
- HTML adjudication interface with category badges and batch actions (`abstract_adjudication_html.py`)
- Self-documenting Excel workbook (via `review_workbook.py`): Instructions sheet, Review Queue with DataValidation dropdowns, Screening Criteria reference

**Design notes:** The `categorizer.py` module classifies flagged papers into FP pattern groups using keyword/regex matching from a YAML config. This pre-categorization helps the human reviewer process flagged papers in batches by failure mode rather than one-at-a-time.

---

## 4. PDF ACQUISITION + QUALITY CHECK

**Trigger:** After `ABSTRACT_ADJUDICATION_COMPLETE` workflow stage.

**Modules:** `engine/acquisition/check_oa.py`, `engine/acquisition/download.py`, `engine/acquisition/verify_downloads.py`, `engine/acquisition/pdf_quality_check.py`, `engine/acquisition/pdf_quality_html.py`, `engine/acquisition/pdf_quality_import.py`

This is a **6-step iterative human-AI loop**:

### Step 1 — Download (5-strategy cascade)
- Strategy 1: Direct Unpaywall PDF URL
- Strategy 2: PMC OA package (Europe PMC → NCBI tar.gz)
- Strategy 3: IEEE stamp page scrape (for `10.1109` DOIs)
- Strategy 4: MDPI URL construction (for MDPI DOIs)
- Strategy 5: DOI redirect with `Accept: application/pdf` + `/pdf` suffix
- All downloads validated with `%PDF` magic bytes. 2-second delay between downloads. Idempotent.

### Step 2 — Verify / Rename (flexible matching, canonical rename)
- Scans PDF directory, matches files to papers using 3 patterns: bare integer, EE-prefix, rich name
- Validates PDF integrity: `%PDF` header, minimum 10KB, HTML error page detection
- Renames to canonical `EE-{nnn}_{Author}_{Year}.pdf`
- Updates both `papers.pdf_local_path` and `full_text_assets.pdf_path`
- Supports `--dry-run`

### Step 3 — AI Quality Check (Qwen2.5-VL first-page classification)
- Renders page 0 to PNG via PyMuPDF at configurable DPI (default 150)
- Sends to qwen2.5vl:7b via Ollama for classification: language + content type
- Content types: `full_manuscript`, `abstract_only`, `trial_registration`, `editorial_erratum`, `conference_poster`, `other`
- Results stored in `papers.pdf_ai_language`, `pdf_ai_content_type`, `pdf_ai_confidence`

### Step 4 — HTML Review (round-trip HTML with sticky summary)
- `pdf_quality_html.py` generates a self-contained HTML page showing AI-flagged papers
- Two modes: `--mode acquisition` (download tracking) and `--mode quality_check` (post-download quality flags)
- Sticky summary bar shows progress counts
- LocalStorage draft support for resuming

### Step 5 — Human Disposition (PROCEED/EXCLUDE)
- Human reviews each flagged paper in the HTML interface
- Marks as PROCEED (AI was wrong, paper is fine) or EXCLUDE with reason code
- Exports decisions as JSON

### Step 6 — Import (JSON → DB, atomic)
- `pdf_quality_import.py` validates all dispositions, then applies atomically
- PROCEED → `pdf_quality_check_status = HUMAN_CONFIRMED`
- EXCLUDE → status = `PDF_EXCLUDED` (terminal), with reason and detail
- Two-pass validation: reject entire import on any error

**Database transitions:**
- `papers.oa_status` set (gold/hybrid/bronze/green/closed/not_found/no_doi)
- `papers.download_status` set (success/failed/pending/manual)
- `ABSTRACT_SCREENED_IN` → `PDF_ACQUIRED` (via `advance_to_pdf_acquired.py`)
- `PDF_ACQUIRED` → `PDF_EXCLUDED` (via `import_dispositions()`, terminal)

**CLI:**
```bash
python -m engine.acquisition.check_oa --review surgical_autonomy \
    --spec review_specs/surgical_autonomy_v1.yaml
python -m engine.acquisition.download --review surgical_autonomy [--retry] [--background]
python -m engine.acquisition.verify_downloads --review surgical_autonomy [--dry-run]
python -m engine.acquisition.pdf_quality_check --review surgical_autonomy \
    --spec review_specs/surgical_autonomy_v1.yaml
python -m engine.acquisition.pdf_quality_html --review surgical_autonomy --mode quality_check
python -m engine.acquisition.pdf_quality_import --review surgical_autonomy \
    --input dispositions.json
```

---

## 5. PARSE

**Trigger:** After PDFs acquired; `PDF_ACQUIRED` papers in DB.

**Module:** `engine/parsers/pdf_parser.py`

**PDF path resolution** (DB-driven with glob fallback):
1. Check `full_text_assets.pdf_path`
2. Check `papers.pdf_local_path`
3. Fall back to filesystem glob

**Three-tier routing:**
- Digital PDFs (> 100 chars/page): Docling `DocumentConverter` (primary)
- Docling failure: PyMuPDF raw text extraction (structural fallback)
- Scanned PDFs (< 100 chars/page) or sparse output: Qwen2.5-VL via Ollama (vision, renders pages to PNG)
- Hash-based caching: if PDF hash unchanged, reuses existing parse

**Database transitions:**
- `PDF_ACQUIRED` → `PARSED`
- Records in `full_text_assets` (pdf_hash, parser_used, version)

**Artifacts:**
- Markdown files: `data/{review}/parsed_text/{paper_id}_v{version}.md`

---

## 6. FULL-TEXT SCREENING

**Trigger:** After PARSE; papers at `PARSED` or `AI_AUDIT_COMPLETE` status.

**Module:** `engine/agents/ft_screener.py`

**Pre-flight:** `require_preflight()` verifies both screening models before starting.

### Primary Screen
- Model: `qwen3:32b` (configurable via `spec.ft_screening_models.primary`)
- Input: parsed Markdown, truncated to 32,000 chars (`FT_MAX_TEXT_CHARS`) via section-aware truncation (preserves title/abstract/methods; drops references first)
- Output: structured `FTScreeningDecision` with decision, reason code, rationale, confidence
- Reason codes: `eligible`, `wrong_specialty`, `no_autonomy_content`, `wrong_intervention`, `protocol_only`, `duplicate_cohort`, `insufficient_data`
- Specialty scope injected from Review Spec
- Checkpoint/resume support

### Verification
- Model: `gemma3:27b` (configurable via `spec.ft_screening_models.verifier`)
- Re-screens `FT_ELIGIBLE` papers with strict 5-test FP-catching criteria
- Output: `FT_ELIGIBLE` (confirmed) or `FT_FLAGGED` (for human review)
- Auto-advances workflow stage `FULL_TEXT_SCREENING_COMPLETE`

**Status-aware screening:** Papers at `AI_AUDIT_COMPLETE` (already extracted/audited) have FT decisions recorded for concordance, but their workflow status is not changed — prevents backward transitions that would lose extraction/audit data.

**Database transitions:**
- `PARSED` → `FT_ELIGIBLE` | `FT_SCREENED_OUT` | `FT_FLAGGED`
- Records in `ft_screening_decisions` and `ft_verification_decisions`

**CLI:**
```bash
python -m engine.agents.ft_screener --review surgical_autonomy \
    --spec review_specs/surgical_autonomy_v1.yaml
python -m engine.agents.ft_screener ... --screen-only
python -m engine.agents.ft_screener ... --verify-only
```

---

## 7. FULL-TEXT SCREENING ADJUDICATION

**Trigger:** Manual — human reviews FT_FLAGGED papers.

**Modules:** `engine/adjudication/ft_screening_adjudicator.py`, `engine/adjudication/ft_adjudication_html.py`

**Steps:**
1. Export `FT_FLAGGED` papers to HTML (with reason codes, primary/verifier decisions) or Excel
2. Human fills `FT_ELIGIBLE` or `FT_SCREENED_OUT` decisions
3. Import decisions → auto-set `FULL_TEXT_ADJUDICATION_COMPLETE`

**Database transitions:**
- `FT_FLAGGED` → `FT_ELIGIBLE` or `FT_SCREENED_OUT`
- Records in `ft_screening_adjudication` table

---

## 8. EXTRACT (Local)

**Trigger:** After FT screening; `FT_ELIGIBLE` (or `PARSED` via skip path) papers in DB.

**Module:** `engine/agents/extractor.py`

### Codebook-Driven Prompt Architecture

`build_extraction_prompt()` reads from the extraction codebook YAML (`data/{review}/extraction_codebook.yaml`), not hardcoded field guides. The codebook defines all 20 fields with:
- Name, type (categorical/free_text/numeric), tier (1-4)
- Definition, instruction, decision criteria
- Per-value definitions for all categorical fields (each valid value has its own definition string)
- Examples (scenario → value) for interpretive fields
- `source_quote_required` flag for judgment fields (tier 4)

`_build_field_block()` renders each field into the prompt. Fields are grouped by tier in the prompt output.

### Two-Pass Extraction (DeepSeek-R1:32b)
- **Pass 1 — Free reasoning:** Model thinks through each field in `<think>` tags. Parsed via `parse_thinking_trace()`.
- **Pass 2 — Structured JSON:** Reasoning trace provided as context. Grammar-constrained output via Ollama `format` parameter set to `ExtractionOutput.model_json_schema()`.
- **Post-processing:** Snippet validation detects ellipsis bridging via `INVALID_SNIPPET_RE`. Invalid snippets retried up to `SNIPPET_MAX_RETRIES` (2) times via `_retry_snippet()`.

### Pre-Flight Checks
- `require_preflight()` verifies extraction model is loaded and responsive
- `check_stale_extractions()` warns if papers have extractions from a different schema hash (non-blocking)
- Model digest captured via `get_model_digest()` for both extractor and auditor models

### Proactive Ollama Restart
- `RESTART_EVERY_N = 25` — after every 25 papers, runs `sudo systemctl restart ollama`
- Polls `/api/tags` for up to 60s to confirm service recovery
- Configurable via `--restart-every N` (0 disables)

**Database transitions:**
- `FT_ELIGIBLE` → `EXTRACTED` (success) | `EXTRACT_FAILED` (exception)
- Atomic insert: extraction record + all evidence spans in single transaction
- Schema hash stored in `extractions.extraction_schema_hash` for staleness detection

**Artifacts:**
- `extractions` table: extracted_data (JSON), reasoning_trace, model, model_digest, schema_hash
- `evidence_spans` table: field_name, value, source_snippet, confidence (0.0-1.0)

**CLI:**
```bash
# Unattended batch (tmux recommended)
python scripts/run5_extract_and_audit.py [--retry-failed] [--restart-every 25]
```

---

## 9. EXTRACT (Cloud Concordance Arms)

**Trigger:** After local extraction; runs concurrently on pending papers.

**Modules:** `engine/cloud/openai_extractor.py`, `engine/cloud/anthropic_extractor.py`, `engine/cloud/base.py`

**Arms:**
- **OpenAI o4-mini:** `reasoning_effort=high`, `response_format=json_object`
- **Anthropic Sonnet 4.6:** Extended thinking (`budget_tokens=10000`), markdown fence stripping, rate-limit backoff (30s/60s/120s)

Both arms use the same prompt from `build_extraction_prompt()`. Papers eligible if status IN (`FT_ELIGIBLE`, `EXTRACTED`, `AI_AUDIT_COMPLETE`, `HUMAN_AUDIT_COMPLETE`) and no existing cloud extraction for that arm.

**Database transitions:**
- No paper status change — cloud extractions are parallel data
- Records in `cloud_extractions` and `cloud_evidence_spans` tables (separate from local)
- Cost tracked per extraction (input_tokens, output_tokens, reasoning_tokens, cost_usd)

**CLI:**
```bash
PYTHONPATH=. python scripts/run_cloud_extraction.py --arm both --max-cost 25.00
PYTHONPATH=. python scripts/run_cloud_extraction.py --progress
```

---

## 10. AUDIT

**Trigger:** After EXTRACT; `EXTRACTED` papers in DB.

**Module:** `engine/agents/auditor.py`

**Pre-flight:** `require_preflight()` verifies auditor model.

### Two-Step Audit Per Evidence Span

1. **Grep verify** — Normalized substring match or sliding-window fuzzy match (SequenceMatcher > 0.85). Normalization: lowercase, collapse whitespace, fix glued punctuation, straighten quotes.
2. **Semantic verify** — gemma3:27b checks if extracted value is supported by source snippet. Categorical fields use specialized prompt.

### Audit Outcomes (4 states)
- `verified` — grep pass + semantic pass
- `contested` — grep fail + semantic pass (paraphrased snippet, value likely correct)
- `flagged` — semantic fail (value may be wrong)
- `invalid_snippet` — ellipsis bridging detected by regex, no LLM call

### Special Handling
- Absence values auto-verified: `NOT_FOUND`, `NR`, `Not discussed`, `No comparison reported`, `Not assessable`
- Tier 4 fields (judgment): skip grep, semantic-only
- `ollama_options` pass-through for per-model settings

### Post-Audit: LOW_YIELD Detection
- `check_low_yield()` counts non-null, non-absence fields per extraction
- Papers below `low_yield_threshold` (default 4) flagged `low_yield=1`
- LOW_YIELD papers prioritized in audit review queue

**Database transitions:**
- `EXTRACTED` → `AI_AUDIT_COMPLETE`
- Each span's `audit_status`, `auditor_model`, `audit_rationale` updated

---

## 11. DISTRIBUTION COLLAPSE MONITORING

**Trigger:** Manual — run after extraction completes as a quality gate.

**Module:** `engine/validators/distribution_monitor.py`

For each categorical field defined in the codebook:
- Queries all extracted values for that field across the specified arm (local, cloud, or human)
- Excludes null/NR values
- Computes: total non-null count, distinct value count, most common value percentage, Shannon entropy

**Detection thresholds:**
- **COLLAPSED:** distinct values ≤ 1 AND non-null count ≥ 10 (hard failure — zero variance)
- **LOW_VARIANCE:** most common value ≥ 85% AND non-null count ≥ 20 (soft warning)
- **OK:** neither condition met

**`assert_no_collapse()`** raises `DistributionCollapseError` on COLLAPSED fields. `--strict` flag also fails on LOW_VARIANCE.

**Design notes:** This module was created after Run 5 where DeepSeek-R1:32b assigned `"Proof of concept only"` to 100% of papers for the `clinical_readiness_assessment` field — a prompt content deficiency that wasn't caught until manual inspection. The monitor would have flagged this automatically.

**CLI:**
```bash
python -m engine.validators.distribution_monitor --review surgical_autonomy --arm local [--strict]
```

---

## 12. HUMAN AUDIT REVIEW

**Trigger:** Manual — after AI audit. Workflow stage `AI_AUDIT_COMPLETE_STAGE`.

**Modules:** `engine/adjudication/audit_adjudicator.py`, `engine/review/extraction_audit_html.py`, `engine/review/human_review.py`

**Steps:**
1. Export per-span rows to self-documenting Excel workbook → auto-set `AUDIT_QUEUE_EXPORTED`
   - Problem spans only for papers with contested/flagged/invalid_snippet spans
   - All spans for LOW_YIELD papers (full picture needed)
   - All spans for spot-check sample (10% of all-verified papers)
2. Human reviews each span: ACCEPT / REJECT / CORRECT
3. Two-pass validation on import: reject entirely on any error — zero DB changes on failure
4. Import decisions → auto-set `AUDIT_REVIEW_COMPLETE`
5. Alternative: HTML interface via `extraction_audit_html.py` with per-span ACCEPT/REJECT/CORRECT, localStorage persistence

**Database transitions:**
- ACCEPT: span → `verified`, `auditor_model = human_review`
- CORRECT: span value overwritten, original in `audit_adjudication` table
- REJECT: recorded in `audit_adjudication` table
- Paper: `AI_AUDIT_COMPLETE` → `HUMAN_AUDIT_COMPLETE` when all spans resolved

---

## 13. CONCORDANCE ANALYSIS

**Trigger:** Manual — after extraction across multiple arms.

**Module:** `engine/analysis/concordance.py`, `engine/analysis/scoring.py`, `engine/analysis/metrics.py`, `engine/analysis/normalize.py`, `engine/analysis/report.py`

**Architecture:** Permanent engine infrastructure for multi-arm extraction comparison.

### Data Flow
1. `load_arm()` loads extracted values from any arm: `local` (evidence_spans), cloud arms (cloud_evidence_spans), or human arms (`human_A`, `human_B`, etc. from human_extractions)
2. `align_arms()` aligns two arms by paper_id and field_name
3. `score_pair()` normalizes values via `normalize_for_concordance()` and scores each pair:
   - Both null → `MATCH`
   - One null → `MISMATCH`
   - Categorical: exact match after prefix mapping + case folding → `MATCH` or `MISMATCH`
   - Free-text: Jaccard token overlap > 0.7 → `AMBIGUOUS`; else `MISMATCH`
   - Multi-value categorical (sets): set equality
4. `cohens_kappa()` computes Cohen's κ with analytical 95% CI (Fleiss 1981). AMBIGUOUS excluded from denominator.
5. `check_schema_parity()` verifies extraction_schema_hash consistency across arms

### Normalization
`normalize_for_concordance()` in `engine/analysis/normalize.py`:
- Null synonyms: `""`, `nr`, `n/r`, `not reported`, `not_found`, `none`, `n/a` → None
- Categorical: exact match → case-insensitive → prefix mapping (e.g., `"2"` → `"2 (Task autonomy)"`)
- Multi-value categorical: semicolon-split → normalize each → return as set
- Free-text: lowercase, strip, collapse whitespace
- Numeric (sample_size): strip non-digits, return integer string

### Output
- Terminal summary: per-field κ, %Agreement, AMBIGUOUS counts
- CSV: `concordance_summary.csv`, `disagreements.csv`
- HTML: branded report with per-pair field tables and expandable disagreements

**CLI:**
```bash
python -m engine.analysis.concordance --review surgical_autonomy \
    --arms local,openai_o4_mini_high,anthropic_sonnet_4_6
```

---

## 14. EXPORT

**Trigger:** After all review stages complete (or with `min_status` filtering).

**Module:** `engine/exporters/`

| Exporter | File | Description |
|----------|------|-------------|
| `prisma.py` | `prisma_flow.csv` | PRISMA flow counts with automatic reconciliation |
| `evidence_table.py` | `evidence_table.csv`, `.xlsx` | Flat evidence table (3-sheet Excel) |
| `docx_export.py` | `evidence_table.docx` | Formatted DOCX (landscape) |
| `methods_section.py` | `methods_section.md` | Auto-generated PRISMA methods paragraph |
| `trace_exporter.py` | `traces/*.md` | Per-paper reasoning traces + quality report |

**`min_status` filtering:** `AI_AUDIT_COMPLETE` (raw AI output) vs `HUMAN_AUDIT_COMPLETE` (human-verified only)

**PRISMA reconciliation:** `validate_prisma_counts()` checks terminal + in-progress = total, PDF exclusion sub-counts sum correctly, no double-counting. Runs automatically before CSV export.

**Self-documenting workbooks:** `review_workbook.py` provides shared builder used by all 3 adjudication exporters (abstract, FT, audit). Features: Instructions sheet (opens first), DataValidation dropdowns, conditional formatting, frozen headers, `ColumnDef` configuration.

**CLI:**
```bash
python scripts/run_pipeline.py --spec ... --name ... --skip-to export
```

---

*Generated 2026-03-17 from commit d0bf07c*
