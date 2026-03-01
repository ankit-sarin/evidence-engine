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
│   ├── parsers/
│   │   ├── __init__.py
│   │   ├── models.py           # ParsedDocument model
│   │   └── pdf_parser.py       # Docling + Qwen2.5-VL routing
│   └── exporters/
│       ├── __init__.py         # export_all() convenience function
│       ├── prisma.py           # PRISMA flow data + CSV
│       ├── evidence_table.py   # Evidence CSV + Excel (3-sheet)
│       ├── docx_export.py      # DOCX formatted evidence table
│       └── methods_section.py  # Auto-generated PRISMA methods paragraph
├── scripts/
│   ├── run_pipeline.py         # Full pipeline CLI (--spec, --name, --skip-to, --limit)
│   └── test_e2e_search_screen.py  # Live E2E test: search + screen 20 papers
├── tests/                      # 103 tests, all passing
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

## Data Architecture
- SQLite: One database per review (state machine, provenance)
- ChromaDB: Vector embeddings per review (disposable, rebuildable)
- File system: Immutable PDF + parsed Markdown store

## Paper Lifecycle
INGESTED → SCREENED_IN / SCREENED_OUT / SCREEN_FLAGGED → PDF_ACQUIRED → PARSED → EXTRACTED → AUDITED

## Pipeline Stages
1. **SEARCH** — PubMed + OpenAlex → deduplicate → add to DB
2. **SCREEN** — Dual-pass qwen3:8b with structured output
3. **PARSE** — Docling (digital) or Qwen2.5-VL (scanned) → Markdown
4. **EXTRACT** — Pass 1: DeepSeek-R1 reasoning → Pass 2: structured JSON
5. **AUDIT** — Grep verify + semantic verify via qwen3:32b
6. **EXPORT** — PRISMA CSV, evidence CSV/Excel/DOCX, methods section

## Inference
All models via Ollama at localhost:11434. Temperature 0 for all agents.

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

# Search + screen test (20 papers, live)
python scripts/test_e2e_search_screen.py

# Test suite
python -m pytest tests/ -v
```

## Build Plan
See Project4_Surgical_Evidence_Engine_Unified_Plan_v5.md
