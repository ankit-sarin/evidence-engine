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
# Search is the first stage; use --skip-to to jump past it
```

---

## 2. ABSTRACT SCREENING (Dual-Pass Primary + Verification)

**Trigger:** After SEARCH, or `--skip-to screen` in pipeline. Expanded search uses `screen_expanded.py` (3-phase).

**Module:** `engine/agents/screener.py`

### Phase 1 — Fetch Abstracts (expanded search only)
- `scripts/screen_expanded.py --fetch-only`
- Queries OpenAlex (by DOI) and PubMed (by PMID) for abstracts
- Writes to `data/surgical_autonomy/expanded_search/abstracts.jsonl` (append-only, crash-safe)

### Phase 2 — Primary Dual-Pass Screening
- Model: `qwen3:8b` (configurable via `spec.screening_models.primary`)
- Pass 1 (pass_number=1): Simplified exclusion criteria (high recall)
- Pass 2 (pass_number=2): Full exclusion criteria (high precision)
- Specialty scope (if configured): included/excluded surgical specialties injected into prompt
- Decision logic:
  - Both include → `ABSTRACT_SCREENED_IN`
  - Both exclude → `ABSTRACT_SCREENED_OUT`
  - Disagree → `ABSTRACT_SCREEN_FLAGGED`
- Checkpoint/resume via JSON file (every paper saved)

### Phase 3 — Verification
- Model: `gemma3:27b` (configurable via `spec.screening_models.verification`)
- Re-screens all `ABSTRACT_SCREENED_IN` papers with `role="verifier"` (full strict criteria)
- Verifier exclude → `ABSTRACT_SCREEN_FLAGGED`
- Auto-advances workflow stage `ABSTRACT_SCREENING_COMPLETE` on success

**Database transitions:**
- `INGESTED` → `ABSTRACT_SCREENED_IN` | `ABSTRACT_SCREENED_OUT` | `ABSTRACT_SCREEN_FLAGGED`
- `ABSTRACT_SCREENED_IN` → `ABSTRACT_SCREEN_FLAGGED` (if verifier excludes)
- Records stored in `abstract_screening_decisions` (pass 1 + 2) and `abstract_verification_decisions` tables

**Data Retention:** All paper data (metadata, abstract, screening traces) is retained permanently regardless of outcome. `ABSTRACT_SCREENED_OUT` is a label, not a deletion. The database is the single source of truth for all papers ever evaluated.

**Artifacts:**
- Screening decisions with rationale in DB
- Expanded search: `screening_results.csv`, `verification_results.csv`

**CLI:**
```bash
# Full pipeline screening
python scripts/run_pipeline.py --spec ... --name ... --skip-to screen

# Expanded search (three-phase)
python scripts/screen_expanded.py                # all phases
python scripts/screen_expanded.py --fetch-only   # phase 1
python scripts/screen_expanded.py --screen-only  # phase 2
python scripts/screen_expanded.py --verify-only  # phase 3
```

---

## 3. ABSTRACT SCREENING ADJUDICATION

**Trigger:** Manual — human reviews flagged papers after abstract screening.

**Module:** `engine/adjudication/screening_adjudicator.py`, `engine/adjudication/categorizer.py`

**Steps:**
1. Human reviews 50-paper diagnostic sample → advance `ABSTRACT_DIAGNOSTIC_COMPLETE`
2. Create `adjudication_categories.yaml` → auto-set `ABSTRACT_CATEGORIES_CONFIGURED`
3. Export flagged papers to Excel → auto-set `ABSTRACT_QUEUE_EXPORTED`
4. Human fills INCLUDE/EXCLUDE decisions in Excel
5. Import decisions → auto-set `ABSTRACT_ADJUDICATION_COMPLETE` (if zero unresolved)

**Database transitions:**
- `ABSTRACT_SCREEN_FLAGGED` → `ABSTRACT_SCREENED_IN` (INCLUDE decision)
- `ABSTRACT_SCREEN_FLAGGED` → `ABSTRACT_SCREENED_OUT` (EXCLUDE decision)
- Records in `screening_adjudication` table

**Artifacts:**
- Self-documenting Excel workbook (via `review_workbook.py`): Instructions sheet (opens first), Review Queue sheet (DataValidation dropdowns, conditional formatting, frozen headers), Screening Criteria reference sheet

**CLI:**
```bash
# Export queue
python -c "
from engine.core.database import ReviewDatabase
from engine.adjudication import export_adjudication_queue
db = ReviewDatabase('surgical_autonomy')
export_adjudication_queue(db, 'queue.xlsx', review_name='surgical_autonomy')
"

# Import decisions
python -c "
from engine.core.database import ReviewDatabase
from engine.adjudication import import_adjudication_decisions
db = ReviewDatabase('surgical_autonomy')
import_adjudication_decisions(db, 'queue_completed.xlsx')
"
```

---

## 4. PDF ACQUISITION

**Trigger:** After `ABSTRACT_ADJUDICATION_COMPLETE` workflow stage.

**Module:** `engine/acquisition/check_oa.py`, `engine/acquisition/download.py`, `engine/acquisition/pdf_quality_html.py`, `engine/acquisition/pdf_quality_check.py`, `engine/acquisition/pdf_quality_import.py`, `engine/acquisition/verify_downloads.py`

**Steps:**
1. **OA Check** — Query Unpaywall API for every DOI (1 req/sec rate limit). Stores `oa_status` and `pdf_url` in DB.
2. **Download** — 5-strategy cascade per paper:
   - Strategy 1: Direct Unpaywall PDF URL
   - Strategy 2: PMC OA package (Europe PMC → NCBI tar.gz)
   - Strategy 3: IEEE stamp page scrape (for `10.1109` DOIs)
   - Strategy 4: MDPI URL construction (for MDPI DOIs)
   - Strategy 5: DOI redirect with `Accept: application/pdf` + `/pdf` suffix
3. **Acquisition List** — Generate HTML review page for remaining papers (replaces deprecated `manual_list.py`). Disposition workflow: mark each paper as Acquired / Will Reattempt / Exclude. Exports JSON for import.
4. **Verify Downloads** — Scan PDF directory, match files to papers (3 patterns: bare integer, EE-prefix, rich name), validate PDF integrity (%PDF header + minimum 10KB + HTML error page detection), rename to canonical `EE-{nnn}_{Author}_{Year}.pdf`, update both `papers` and `full_text_assets` tables. Supports `--dry-run`.
5. **PDF Quality Check** — AI-based first-page classification (qwen2.5vl:7b via Ollama). Renders page 0 to PNG, classifies language + content type (full_manuscript, abstract_only, trial_registration, editorial_erratum, conference_poster, other). Results stored in `papers.pdf_ai_language`, `pdf_ai_content_type`, `pdf_ai_confidence`. Config from Review Spec `pdf_quality_check` section.
6. **Quality Review + Import** — Generate HTML review page showing AI-flagged papers (non-English or non-manuscript). Human marks each as PROCEED (AI was wrong) or EXCLUDE with reason. JSON export → `import_dispositions()` applies atomic DB updates: PROCEED → `HUMAN_CONFIRMED`, EXCLUDE → `PDF_EXCLUDED` (terminal status).

All downloads validated with `%PDF` magic bytes. Idempotent (skips papers with valid PDFs on disk). 2-second delay between downloads.

**Database transitions:**
- `papers.oa_status` updated (gold/hybrid/bronze/green/closed/not_found/no_doi)
- `papers.download_status` updated (success/failed/pending/manual)
- `papers.pdf_local_path` set on success
- `full_text_assets.pdf_path` set on verify (canonical path)
- Paper status: `ABSTRACT_SCREENED_IN` → `PDF_ACQUIRED` (via `advance_to_pdf_acquired.py`)
- Paper status: `PDF_ACQUIRED` → `PDF_EXCLUDED` (via `import_dispositions()`, terminal)
- `papers.pdf_quality_check_status`: `AI_CHECKED` → `HUMAN_CONFIRMED`
- `papers.pdf_exclusion_reason`, `papers.pdf_exclusion_detail` set on exclusion

**Artifacts:**
- PDFs in `data/{review}/pdfs/` (bare integer initially, renamed to `EE-{nnn}_{Author}_{Year}.pdf` by verify)
- `acquisition_list.html` — interactive disposition-tracking HTML (replaces deprecated `manual_download_list.html`)
- `pdf_quality_review.html` — AI quality flag review page (post-download)

**CLI:**
```bash
python -m engine.acquisition.check_oa --review surgical_autonomy \
    --spec review_specs/surgical_autonomy_v1.yaml
python -m engine.acquisition.download --review surgical_autonomy [--retry] [--background]
python -m engine.acquisition.pdf_quality_html --review surgical_autonomy --mode acquisition
python -m engine.acquisition.verify_downloads --review surgical_autonomy [--dry-run]
python -m engine.acquisition.pdf_quality_check --review surgical_autonomy \
    --spec review_specs/surgical_autonomy_v1.yaml [--dry-run] [--limit N]
python -m engine.acquisition.pdf_quality_html --review surgical_autonomy --mode quality_check
python -m engine.acquisition.pdf_quality_import --review surgical_autonomy \
    --input dispositions.json [--dry-run]
```

---

## 5. PARSE

**Trigger:** After PDFs acquired; `PDF_ACQUIRED` papers in DB.

**Module:** `engine/parsers/pdf_parser.py`

**PDF path resolution** (DB-driven with glob fallback):
1. Check `full_text_assets.pdf_path` (set by parser or verify_downloads)
2. Check `papers.pdf_local_path` (set by downloader or verify_downloads)
3. Fall back to filesystem glob (`{paper_id}_*.pdf`, `{paper_id}.pdf`)

**Routing logic:**
- Digital PDFs (> 100 chars/page): Docling `DocumentConverter`
- Scanned PDFs (< 100 chars/page): Qwen2.5-VL via Ollama (renders pages to PNG, sends as base64)

**Database transitions:**
- `PDF_ACQUIRED` → `PARSED`
- Records in `full_text_assets` table (pdf_hash, parser_used, version number)
- Hash-based skip: if PDF hash unchanged, reuses existing parse

**Artifacts:**
- Markdown files: `data/{review}/parsed_text/{paper_id}_v{version}.md`

**CLI:**
```bash
python scripts/run_pipeline.py --spec ... --name ... --skip-to parse
```

---

## 6. FULL-TEXT SCREENING

**Trigger:** After PARSE; `PARSED` papers in DB.

**Module:** `engine/agents/ft_screener.py`

### Primary Screen
- Model: `qwen3.5:27b` (configurable via `spec.ft_screening_models.primary`)
- Input: parsed Markdown text, truncated to 32,000 chars (`FT_MAX_TEXT_CHARS`) via section-aware truncation (preserves title, abstract, intro, methods, results, discussion; drops references/appendices first)
- Output: structured `FTScreeningDecision` with decision (`FT_ELIGIBLE` or `FT_EXCLUDE`), reason code, rationale, confidence
- Reason codes: `eligible`, `wrong_specialty`, `no_autonomy_content`, `wrong_intervention`, `protocol_only`, `duplicate_cohort`, `insufficient_data`
- Specialty scope injected from Review Spec

### Verification
- Model: `gemma3:27b` (configurable via `spec.ft_screening_models.verifier`)
- Re-screens `FT_ELIGIBLE` papers with strict criteria
- Output: `FTVerificationDecision` — `FT_ELIGIBLE` (confirmed) or `FT_FLAGGED` (for human review)
- Auto-advances workflow stage `FULL_TEXT_SCREENING_COMPLETE`

**Database transitions:**
- `PARSED` → `FT_ELIGIBLE` | `FT_SCREENED_OUT` | `FT_FLAGGED`
- `FT_ELIGIBLE` → `FT_FLAGGED` (if verifier flags)
- Records stored in `ft_screening_decisions` and `ft_verification_decisions` tables
- Skip path: `PARSED` → `EXTRACTED` (for papers already at extraction stage)

**Artifacts:**
- FT screening decisions with reason codes and rationale in DB

**CLI:**
```bash
# FT screening smoke test (5 known papers)
python scripts/ft_screening_smoke_test.py
```

---

## 7. FULL-TEXT SCREENING ADJUDICATION

**Trigger:** Manual — human reviews FT_FLAGGED papers after full-text screening.

**Module:** `engine/adjudication/ft_screening_adjudicator.py`

**Steps:**
1. Export `FT_FLAGGED` papers to Excel with primary/verifier decisions and rationale
2. Human fills `FT_ELIGIBLE` or `FT_SCREENED_OUT` decisions
3. Import decisions → auto-set `FULL_TEXT_ADJUDICATION_COMPLETE` (if zero unresolved)

**Database transitions:**
- `FT_FLAGGED` → `FT_ELIGIBLE` (INCLUDE decision)
- `FT_FLAGGED` → `FT_SCREENED_OUT` (EXCLUDE decision)
- Records in `ft_screening_adjudication` table

**CLI:**
```bash
python -c "
from engine.adjudication.ft_screening_adjudicator import (
    export_ft_adjudication_queue, import_ft_adjudication_decisions
)
from engine.core.database import ReviewDatabase
db = ReviewDatabase('surgical_autonomy')
export_ft_adjudication_queue(db, 'ft_queue.xlsx')
"
```

---

## 8. EXTRACT

**Trigger:** After FT screening; `FT_ELIGIBLE` (or `PARSED` via skip path) papers in DB.

**Module:** `engine/agents/extractor.py`

**Two-pass extraction (DeepSeek-R1:32b):**
- Pass 1: Free reasoning — model thinks through each field in `<think>` tags
- Pass 2: Structured JSON output — reasoning trace provided as context, grammar-constrained output via Ollama `format` parameter
- Post-processing: snippet validation detects ellipsis bridging (`...`) via regex; invalid snippets retried up to 2 times

**Database transitions:**
- `FT_ELIGIBLE` → `EXTRACTED` (success) | `EXTRACT_FAILED` (exception)
- `PARSED` → `EXTRACTED` (success, skip path) | `EXTRACT_FAILED` (exception)
- Atomic insert: extraction record + all evidence spans in single transaction
- Schema hash stored for staleness detection

**Artifacts:**
- `extractions` table: extracted_data (JSON), reasoning_trace, model, schema_hash, low_yield
- `evidence_spans` table: field_name, value, source_snippet, confidence

**CLI:**
```bash
python scripts/run_pipeline.py --spec ... --name ... --skip-to extract

# Cloud concordance arms (optional)
PYTHONPATH=. python scripts/run_cloud_extraction.py --arm both --max-cost 25.00
PYTHONPATH=. python scripts/run_cloud_extraction.py --progress
```

---

## 9. AUDIT

**Trigger:** After EXTRACT; `EXTRACTED` papers in DB.

**Module:** `engine/agents/auditor.py`

**Two-step audit per evidence span:**
1. **Grep verify** — Normalized substring match or sliding-window fuzzy match (SequenceMatcher > 0.85) of `source_snippet` against paper text
2. **Semantic verify** — gemma3:27b LLM checks if extracted value is supported by the source snippet

**Audit outcomes (4 states):**
- `verified` — grep pass + semantic pass
- `contested` — grep fail + semantic pass (paraphrased snippet, value likely correct)
- `flagged` — semantic fail (value may be wrong)
- `invalid_snippet` — snippet contains ellipsis bridging (detected by regex, no LLM call)

**Special handling:**
- Tier 4 fields (judgment): skip grep, go straight to semantic verification
- Categorical fields: auditor checks if source text supports classification (label need not appear verbatim)

**Post-audit: LOW_YIELD detection:**
- `check_low_yield()` runs automatically after audit completes
- Counts non-null, non-absence fields per extraction (absence sentinels: `NOT_FOUND`, `NR`, `Not discussed`, `No comparison reported`, `Not assessable`)
- Papers with fewer than `spec.low_yield_threshold` (default 4) populated fields are flagged `low_yield=1` on the extraction record
- LOW_YIELD papers are prioritized in the audit review queue for PI review

**Database transitions:**
- `EXTRACTED` → `AI_AUDIT_COMPLETE` (when all spans audited, no pending remain)
- Each span's `audit_status`, `auditor_model`, `audit_rationale` updated
- `extractions.low_yield` set to 1 for papers below threshold

**CLI:**
```bash
python scripts/run_pipeline.py --spec ... --name ... --skip-to audit
```

---

## 10. HUMAN AUDIT REVIEW

**Trigger:** Manual — after AI audit. Workflow stage `AI_AUDIT_COMPLETE_STAGE`.

**Module:** `engine/adjudication/audit_adjudicator.py`, `engine/review/human_review.py`

**Steps:**
1. Export per-span rows to self-documenting Excel workbook → auto-set `AUDIT_QUEUE_EXPORTED`
   - Problem spans only for papers with contested/flagged/invalid_snippet spans
   - All spans for LOW_YIELD papers (reviewer needs full picture)
   - All spans for spot-check sample (10% of all-verified papers)
2. LOW_YIELD papers always included with `review_reason="low_yield"`, sorted first
3. Human reviews each span with ACCEPT/REJECT/CORRECT decisions
4. Two-pass validation on import: scan all rows first, reject entirely on any error (blank decisions, invalid values, CORRECT without corrected_value) — zero DB changes on failure
5. Import decisions → auto-set `AUDIT_REVIEW_COMPLETE` (if zero unresolved)
6. Legacy per-paper format (accept_as_is/reject_paper columns) auto-detected and supported

**Database transitions:**
- ACCEPT: span `audit_status` → `verified`, `auditor_model` = `human_review`
- REJECT: span marked verified, recorded in `audit_adjudication` table
- CORRECT: span value overwritten, original preserved in `audit_adjudication` table
- Paper-level: `AI_AUDIT_COMPLETE` → `HUMAN_AUDIT_COMPLETE` (when all spans resolved)
- Records in `audit_adjudication` table

**CLI:**
```bash
# Export audit queue
python -c "
from engine.core.database import ReviewDatabase
from engine.adjudication import export_audit_review_queue
db = ReviewDatabase('surgical_autonomy')
export_audit_review_queue(db, 'audit_queue.xlsx')
"

# Import decisions
python -c "
from engine.core.database import ReviewDatabase
from engine.adjudication import import_audit_review_decisions
db = ReviewDatabase('surgical_autonomy')
import_audit_review_decisions(db, 'audit_queue_completed.xlsx')
"
```

---

## 11. EXPORT

**Trigger:** After all review stages complete (or with `min_status` filtering).

**Module:** `engine/exporters/` (includes `review_workbook.py` — shared self-documenting workbook builder used by all adjudication exporters)

**Outputs:**
| Exporter | File | Description |
|----------|------|-------------|
| `prisma.py` | `prisma_flow.csv` | PRISMA flow counts by stage (includes PDF exclusions, FT screening exclusions, LOW_YIELD rejections) |
| `evidence_table.py` | `evidence_table.csv` | Flat evidence table |
| `evidence_table.py` | `evidence_table.xlsx` | 3-sheet Excel (evidence, screening log, audit log) |
| `docx_export.py` | `evidence_table.docx` | Formatted DOCX (landscape, "First Author et al.") |
| `methods_section.py` | `methods_section.md` | Auto-generated PRISMA methods paragraph |
| `trace_exporter.py` | `traces/*.md` | Per-paper reasoning traces |
| `trace_exporter.py` | `trace_quality_report.json` | Trace length distribution and quality metrics |

**min_status filtering:** `AI_AUDIT_COMPLETE` (raw AI) vs `HUMAN_AUDIT_COMPLETE` (human-verified)

**CLI:**
```bash
python scripts/run_pipeline.py --spec ... --name ... --skip-to export
```

---

*Generated 2026-03-14 from commit `66563cb`*
