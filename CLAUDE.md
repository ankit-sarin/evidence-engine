# Surgical Evidence Engine (Project 4)

## Location
~/projects/evidence-engine

## Deployment
- Port: 7864
- URL: evidence.digitalsurgeon.dev
- Service: evidence-engine (systemd, when ready)

## Purpose
Local systematic review engine on DGX Spark. Accepts Review Specs (YAML), runs search/screening/extraction/audit pipeline, exports publication-ready evidence tables. No data leaves the machine.

## Project Structure
```
evidence-engine/
├── CLAUDE.md
├── pyproject.toml              # pytest config (markers: network, ollama, integration)
├── requirements.txt
├── review_specs/
│   └── surgical_autonomy_v1.yaml   # First review spec (autonomy in surgical robotics)
├── engine/
│   ├── __init__.py
│   ├── core/
│   │   ├── __init__.py
│   │   ├── review_spec.py      # Pydantic models, YAML loader, protocol hashing
│   │   └── database.py         # SQLite state machine, lifecycle, provenance
│   ├── search/
│   │   ├── __init__.py
│   │   ├── models.py           # Citation model (shared across search modules)
│   │   ├── pubmed.py           # PubMed via Biopython Entrez
│   │   ├── openalex.py         # OpenAlex via pyalex
│   │   └── dedup.py            # DOI/PMID/fuzzy-title deduplication
│   ├── agents/
│   │   ├── __init__.py
│   │   ├── models.py           # EvidenceSpan, ExtractionResult models
│   │   ├── screener.py         # Dual-pass screening (qwen3:8b)
│   │   ├── extractor.py        # Two-pass extraction (deepseek-r1:32b)
│   │   └── auditor.py          # Cross-model audit (qwen3:32b)
│   ├── cloud/
│   │   ├── __init__.py         # Cloud extraction arm exports
│   │   ├── base.py             # Shared logic: prompt build, span parsing, DB storage
│   │   ├── schema.py           # cloud_extractions + cloud_evidence_spans tables
│   │   ├── openai_extractor.py # OpenAI o4-mini arm (reasoning_effort=high)
│   │   └── anthropic_extractor.py # Anthropic Sonnet 4.6 arm (extended thinking)
│   ├── parsers/
│   │   ├── __init__.py
│   │   ├── models.py           # ParsedDocument model
│   │   └── pdf_parser.py       # Docling + Qwen2.5-VL routing
│   └── exporters/
│       ├── __init__.py         # export_all() convenience function
│       ├── prisma.py           # PRISMA flow data + CSV
│       ├── evidence_table.py   # Evidence CSV + Excel (3-sheet)
│       ├── docx_export.py      # DOCX formatted evidence table
│       ├── methods_section.py  # Auto-generated PRISMA methods paragraph
│       └── trace_exporter.py   # Per-paper MD traces, quality report, disagreement pairs
├── scripts/
│   ├── run_pipeline.py         # Full pipeline CLI (--spec, --name, --skip-to, --limit)
│   ├── run_cloud_extraction.py # Cloud extraction CLI (--arm, --max-papers, --max-cost, --progress)
│   ├── reextract_failed.py     # Re-extract specific failed papers with extended timeout
│   ├── advance_to_pdf_acquired.py  # Bulk status transition helper
│   ├── monitor_extraction.py   # Live extraction progress monitor
│   ├── prepare_concordance_pdfs.py # EE-XXX renamed PDFs + paper_manifest.csv
│   ├── pdf_acquisition/        # Multi-step PDF download pipeline (export, unpaywall, OA, manual)
│   └── test_e2e_search_screen.py  # Live E2E test: search + screen 20 papers
├── tests/                      # 121 tests, all passing
│   ├── test_review_spec.py     # 11 tests — YAML loading, hashing, validation
│   ├── test_pubmed.py          #  5 tests — live PubMed queries
│   ├── test_openalex.py        #  7 tests — live OpenAlex + abstract reconstruction
│   ├── test_dedup.py           # 15 tests — DOI/PMID/fuzzy match, merge, stats
│   ├── test_database.py        # 15 tests — tables, lifecycle, transitions, staleness
│   ├── test_screener.py        #  8 tests — structured output, dual-pass logic (2 live Ollama)
│   ├── test_pdf_parser.py      #  9 tests — hash, routing, Docling integration, versioning
│   ├── test_extractor.py       # 12 tests — prompt, thinking trace, two-pass mocked flow
│   ├── test_auditor.py         # 13 tests — grep verify, semantic verify, full audit mocked
│   ├── test_exporters.py       #  8 tests — PRISMA, CSV, Excel, DOCX, methods, export_all
│   ├── test_cloud_extraction.py # 18 tests — cloud tables, span parsing, store, CLI
│   ├── e2e_test_log.md         # Test coverage notes
│   └── e2e_search_screen_log.md  # Latest live E2E results
└── data/                       # gitignored — per-review databases, PDFs, exports
```

## Agent Architecture
| Agent | Model | Role |
|-------|-------|------|
| Screener (S) | qwen3:8b | Dual-pass title/abstract screening |
| PDF Parser (A) | Docling + Qwen2.5-VL | Digital + scanned PDF to Markdown |
| Extractor (B) | deepseek-r1:32b | Two-pass structured extraction with reasoning trace |
| Auditor (C) | qwen3:32b | Cross-model verification of extractions |
| Cloud Extractor (OpenAI) | o4-mini-2025-04-16 | Concordance arm — reasoning_effort=high |
| Cloud Extractor (Anthropic) | claude-sonnet-4-6 | Concordance arm — extended thinking |

## Data Architecture
- SQLite: One database per review (state machine, provenance)
- ChromaDB: Vector embeddings per review (disposable, rebuildable)
- File system: Immutable PDF + parsed Markdown store

## Paper Lifecycle
INGESTED → SCREENED_IN / SCREENED_OUT / SCREEN_FLAGGED → PDF_ACQUIRED → PARSED → EXTRACTED / EXTRACT_FAILED → AI_AUDIT_COMPLETE → HUMAN_AUDIT_COMPLETE

## Pipeline Stages
1. **SEARCH** — PubMed + OpenAlex → deduplicate → add to DB
2. **SCREEN** — Dual-pass qwen3:8b with structured output
3. **PARSE** — Docling (digital) or Qwen2.5-VL (scanned) → Markdown
4. **EXTRACT** — Pass 1: DeepSeek-R1 reasoning → Pass 2: structured JSON
5. **AUDIT** — Grep verify + semantic verify via qwen3:32b
6. **EXPORT** — PRISMA CSV, evidence CSV/Excel/DOCX, methods section

## Inference
- Local models via Ollama at localhost:11434. Temperature 0 for all agents.
- Cloud models via OpenAI and Anthropic APIs (env vars OPENAI_API_KEY, ANTHROPIC_API_KEY).

## Key Patterns
- Review Spec (YAML) defines the entire review contract
- Protocol hashing: SHA-256 of screening/extraction sections for staleness detection
- Dual-pass screening: two independent runs, flag disagreements
- Two-pass extraction: free reasoning trace → grammar-constrained structured output
- Evidence spans: source_snippet fields for traceability
- Grep + semantic audit: check snippet exists in paper, then verify value matches
- Per-review isolation: each review gets its own SQLite DB and directory tree

## Running
```bash
# Full pipeline
python scripts/run_pipeline.py --spec review_specs/surgical_autonomy_v1.yaml --name surgical_autonomy

# Cloud extraction (concordance study)
PYTHONPATH=. python scripts/run_cloud_extraction.py --arm both --max-cost 25.00
PYTHONPATH=. python scripts/run_cloud_extraction.py --progress  # check status

# Search + screen test (20 papers, live)
python scripts/test_e2e_search_screen.py

# Test suite
python -m pytest tests/ -v
```

## Current Review Status (surgical_autonomy)
- **251 total papers** (PubMed + OpenAlex, deduplicated)
- **155 SCREENED_OUT**, **96 AI_AUDIT_COMPLETE**
- 96 local extractions, 1,429 evidence spans (872 verified / 557 flagged)
- Cloud concordance extraction: 96 papers × 2 arms (OpenAI o4-mini + Anthropic Sonnet 4.6) — in progress
- 3 papers excluded post-audit: db_id 37, 149 (HSMR extended abstracts), 225 (SPR pediatric radiology)
- Concordance PDFs: `data/surgical_autonomy/concordance_pdfs/` with EE-001 to EE-099 (gaps at EE-019, EE-062, EE-094)
- Exports: `data/surgical_autonomy/exports/` (evidence CSV/Excel/DOCX, PRISMA CSV, methods section, trace archives)

## Known Issues & Fixes
- DeepSeek-R1 outputs `confidence: -1` for NOT_FOUND fields → clamped via `@field_validator` in `EvidenceSpan`
- ~50% of grep audit failures caused by ellipsis in source_snippets (model abbreviates quotes with `...`)
- ~49% of grep failures caused by paraphrased (non-verbatim) snippets — values typically correct
- 4 papers have <15 spans (11-14): db_id 94, 102, 145, 221 — model omitted some fields
- Cloud parser: OpenAI uses `{"extractions":[...]}` or `{"data":[...]}` instead of `{"fields":[...]}` — handled
- Cloud parser: Anthropic wraps JSON in ` ```json ``` ` markdown fences — stripped before parsing
- Cloud parser: o4-mini occasionally returns single flat span dict instead of list — wrapped automatically

## Build Plan
See Project4_Surgical_Evidence_Engine_Unified_Plan_v5.md
