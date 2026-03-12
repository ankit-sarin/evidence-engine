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

## 2. SCREEN (Dual-Pass Primary + Verification)

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
- Decision logic:
  - Both include → `SCREENED_IN`
  - Both exclude → `SCREENED_OUT`
  - Disagree → `SCREEN_FLAGGED`
- Checkpoint/resume via JSON file (every paper saved)

### Phase 3 — Verification
- Model: `gemma3:27b` (configurable via `spec.screening_models.verification`)
- Re-screens all `SCREENED_IN` papers with `role="verifier"` (full strict criteria)
- Verifier exclude → `SCREEN_FLAGGED`
- Auto-advances workflow stage `SCREENING_COMPLETE` on success

**Database transitions:**
- `INGESTED` → `SCREENED_IN` | `SCREENED_OUT` | `SCREEN_FLAGGED`
- `SCREENED_IN` → `SCREEN_FLAGGED` (if verifier excludes)
- Records stored in `screening_decisions` (pass 1 + 2) and `verification_decisions` tables

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

## 3. SCREENING ADJUDICATION

**Trigger:** Manual — human reviews flagged papers after screening.

**Module:** `engine/adjudication/screening_adjudicator.py`, `engine/adjudication/categorizer.py`

**Steps:**
1. Human reviews 50-paper diagnostic sample → advance `DIAGNOSTIC_SAMPLE_COMPLETE`
2. Create `adjudication_categories.yaml` → auto-set `CATEGORIES_CONFIGURED`
3. Export flagged papers to Excel → auto-set `QUEUE_EXPORTED`
4. Human fills INCLUDE/EXCLUDE decisions in Excel
5. Import decisions → auto-set `ADJUDICATION_COMPLETE` (if zero unresolved)

**Database transitions:**
- `SCREEN_FLAGGED` → `SCREENED_IN` (INCLUDE decision)
- `SCREEN_FLAGGED` → `SCREENED_OUT` (EXCLUDE decision)
- Records in `screening_adjudication` table

**Artifacts:**
- Excel workbook: Review Queue sheet, Category Summary sheet, Instructions sheet

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

**Trigger:** After `ADJUDICATION_COMPLETE` workflow stage.

**Module:** `engine/acquisition/check_oa.py`, `engine/acquisition/download.py`, `engine/acquisition/manual_list.py`

**Steps:**
1. **OA Check** — Query Unpaywall API for every DOI (1 req/sec rate limit). Stores `oa_status` and `pdf_url` in DB.
2. **Download** — 5-strategy cascade per paper:
   - Strategy 1: Direct Unpaywall PDF URL
   - Strategy 2: PMC OA package (Europe PMC → NCBI tar.gz)
   - Strategy 3: IEEE stamp page scrape (for `10.1109` DOIs)
   - Strategy 4: MDPI URL construction (for MDPI DOIs)
   - Strategy 5: DOI redirect with `Accept: application/pdf` + `/pdf` suffix
3. **Manual List** — Generate HTML + CSV for remaining papers

All downloads validated with `%PDF` magic bytes. Idempotent (skips papers with valid PDFs on disk). 2-second delay between downloads.

**Database transitions:**
- `papers.oa_status` updated (gold/hybrid/bronze/green/closed/not_found/no_doi)
- `papers.download_status` updated (success/failed/pending/manual)
- `papers.pdf_local_path` set on success
- Paper status: `SCREENED_IN` → `PDF_ACQUIRED` (via `advance_to_pdf_acquired.py`)

**Artifacts:**
- PDFs in `data/{review}/pdfs/{paper_id}.pdf`
- `manual_download_list.html` — interactive checklist with localStorage progress
- `manual_downloads_needed.csv`

**CLI:**
```bash
python -m engine.acquisition.check_oa --review surgical_autonomy \
    --spec review_specs/surgical_autonomy_v1.yaml
python -m engine.acquisition.download --review surgical_autonomy [--retry] [--background]
python -m engine.acquisition.manual_list --review surgical_autonomy \
    --spec review_specs/surgical_autonomy_v1.yaml
```

---

## 5. PARSE

**Trigger:** After PDFs acquired; `PDF_ACQUIRED` papers in DB.

**Module:** `engine/parsers/pdf_parser.py`

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

## 6. EXTRACT

**Trigger:** After PARSE; `PARSED` papers in DB.

**Module:** `engine/agents/extractor.py`

**Two-pass extraction (DeepSeek-R1:32b):**
- Pass 1: Free reasoning — model thinks through each field in `<think>` tags
- Pass 2: Structured JSON output — reasoning trace provided as context, grammar-constrained output via Ollama `format` parameter
- Post-processing: snippet validation detects ellipsis bridging (`...`) via regex; invalid snippets retried up to 2 times

**Database transitions:**
- `PARSED` → `EXTRACTED` (success)
- `PARSED` → `EXTRACT_FAILED` (exception)
- Atomic insert: extraction record + all evidence spans in single transaction
- Schema hash stored for staleness detection

**Artifacts:**
- `extractions` table: extracted_data (JSON), reasoning_trace, model, schema_hash
- `evidence_spans` table: field_name, value, source_snippet, confidence

**CLI:**
```bash
python scripts/run_pipeline.py --spec ... --name ... --skip-to extract

# Cloud concordance arms (optional)
PYTHONPATH=. python scripts/run_cloud_extraction.py --arm both --max-cost 25.00
PYTHONPATH=. python scripts/run_cloud_extraction.py --progress
```

---

## 7. AUDIT

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

**Database transitions:**
- `EXTRACTED` → `AI_AUDIT_COMPLETE` (when all spans audited, no pending remain)
- Each span's `audit_status`, `auditor_model`, `audit_rationale` updated

**CLI:**
```bash
python scripts/run_pipeline.py --spec ... --name ... --skip-to audit
```

---

## 8. HUMAN AUDIT REVIEW

**Trigger:** Manual — after AI audit. Workflow stage `AI_AUDIT_COMPLETE_STAGE`.

**Module:** `engine/adjudication/audit_adjudicator.py`, `engine/review/human_review.py`

**Steps:**
1. Export contested/flagged/invalid_snippet spans to Excel → auto-set `AUDIT_QUEUE_EXPORTED`
2. Random spot-check sample (10% of all-verified papers) included for QA
3. Human reviews: accept_as_is, per-field corrections, or reject_paper
4. Import decisions → auto-set `AUDIT_REVIEW_COMPLETE` (if zero unresolved)

**Database transitions:**
- Span corrections: `audit_status` → `verified`, value overwritten if corrected
- Paper-level: `AI_AUDIT_COMPLETE` → `HUMAN_AUDIT_COMPLETE` (all spans resolved)
- Paper-level: `AI_AUDIT_COMPLETE` → `REJECTED` (reject_paper decision)
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

## 9. EXPORT

**Trigger:** After all review stages complete (or with `min_status` filtering).

**Module:** `engine/exporters/`

**Outputs:**
| Exporter | File | Description |
|----------|------|-------------|
| `prisma.py` | `prisma_flow.csv` | PRISMA flow counts by stage |
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

*Generated 2026-03-12 from commit `d65d614`*
