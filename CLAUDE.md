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
│   │   ├── screener.py         # Abstract screening: primary (qwen3:8b) + verifier (gemma3:27b), specialty_scope
│   │   ├── ft_screener.py      # Full-text screening: primary (qwen3.5:27b) + verifier (gemma3:27b)
│   │   ├── extractor.py        # Two-pass extraction (deepseek-r1:32b)
│   │   └── auditor.py          # Cross-model audit (gemma3:27b) + LOW_YIELD detection
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
│   ├── acquisition/
│   │   ├── __init__.py         # Re-exports: check_oa_status, download_papers, generate_manual_list
│   │   ├── check_oa.py         # Unpaywall API OA status + PDF URL lookup, rate-limited, idempotent
│   │   ├── download.py         # 5-strategy cascade downloader (Unpaywall → PMC → IEEE → MDPI → DOI redirect)
│   │   └── manual_list.py      # HTML + CSV manual download list, multi-link (Scholar/DOI/PubMed/Proxy), localStorage progress
│   ├── migrations/
│   │   ├── __init__.py
│   │   ├── 002_screening_rename.py     # Rename SCREENED_IN/OUT → ABSTRACT_SCREENED_IN/OUT
│   │   └── 003_backfill_expanded_screening.py  # Backfill 9,235 expanded-corpus screening traces
│   ├── adjudication/
│   │   ├── __init__.py             # Re-exports for screening + audit adjudication
│   │   ├── advance_stage.py        # CLI to advance workflow stages (12 stages)
│   │   ├── audit_adjudicator.py    # Export/import audit review queue, spot-check, LOW_YIELD, reject cascade
│   │   ├── categorizer.py          # FP category config + keyword matching
│   │   ├── ft_screening_adjudicator.py # Export/import FT screening adjudication queue
│   │   ├── schema.py               # screening + ft_screening + audit adjudication DDL
│   │   ├── screening_adjudicator.py # Export/import abstract screening adjudication queue
│   │   └── workflow.py             # 12-stage workflow (5 abstract + 1 acquisition + 2 FT + 4 extraction)
│   └── exporters/
│       ├── __init__.py         # export_all() with min_status threading
│       ├── prisma.py           # PRISMA flow data + CSV
│       ├── evidence_table.py   # Evidence CSV + Excel (3-sheet), min_status filtering
│       ├── docx_export.py      # DOCX formatted evidence table, min_status filtering
│       ├── methods_section.py  # Auto-generated PRISMA methods paragraph
│       └── trace_exporter.py   # Per-paper MD traces, quality report, disagreement pairs
├── scripts/
│   ├── run_pipeline.py         # Full pipeline CLI with adjudication + audit review gates
│   ├── eval_auditor_models.py  # Multi-model auditor evaluation (5-paper sample)
│   ├── run_cloud_extraction.py # Cloud extraction CLI (--arm, --max-papers, --max-cost, --progress)
│   ├── reextract_failed.py     # Re-extract specific failed papers with extended timeout
│   ├── screen_expanded.py      # Three-phase expanded search screening (fetch/screen/verify)
│   ├── rescreen_original_251.py # Re-screen original 251 papers with updated criteria
│   ├── rescreen_with_specialty.py  # Re-screen included papers with specialty_scope (dual-pass + verification)
│   ├── ft_screening_smoke_test.py  # 5-paper FT screening integration test
│   ├── advance_to_pdf_acquired.py  # Bulk status transition helper
│   ├── monitor_extraction.py   # Live extraction progress monitor
│   ├── prepare_concordance_pdfs.py # EE-XXX renamed PDFs + paper_manifest.csv
│   ├── pdf_acquisition/        # Multi-step PDF download pipeline (export, unpaywall, OA, manual)
│   ├── run_expanded_screen_and_verify.sh  # Tmux launcher for expanded screening
│   ├── watch_run4.sh           # Monitor screening progress
│   └── test_e2e_search_screen.py  # Live E2E test: search + screen 20 papers
├── tests/                      # 332 offline + 10 network/ollama tests
│   ├── test_review_spec.py     # 11 tests — YAML loading, hashing, validation
│   ├── test_pubmed.py          #  5 tests — live PubMed queries
│   ├── test_openalex.py        #  7 tests — live OpenAlex + abstract reconstruction
│   ├── test_dedup.py           # 15 tests — DOI/PMID/fuzzy match, merge, stats
│   ├── test_database.py        # 28 tests — tables, lifecycle, transitions, staleness, reject, min_status, FT states
│   ├── test_screener.py        # 14 tests — structured output, dual-pass, verification, specialty scope
│   ├── test_pdf_parser.py      #  9 tests — hash, routing, Docling integration, versioning
│   ├── test_extractor.py       # 17 tests — prompt, thinking trace, two-pass, ellipsis retry
│   ├── test_auditor.py         # 25 tests — grep verify, semantic verify, full audit mocked
│   ├── test_exporters.py       #  8 tests — PRISMA, CSV, Excel, DOCX, methods, export_all
│   ├── test_cloud_extraction.py # 18 tests — cloud tables, span parsing, store, CLI
│   ├── test_adjudication.py    # 37 tests — categorizer, screening export/import, gate checks
│   ├── test_audit_adjudication.py # 15 tests — audit export/import, spot-check, reject, min_status
│   ├── test_workflow.py        # 30 tests — 12-stage workflow enforcement, blockers, format
│   ├── test_ft_screening.py    # 61 tests — FT screening pipeline, adjudication, prompts, truncation
│   ├── test_low_yield.py       # 15 tests — LOW_YIELD detection, audit queue, PRISMA
│   ├── test_human_review.py    #  6 tests — human review queue export/import
│   ├── test_background.py      #  7 tests — tmux background mode
│   ├── test_trace_exporter.py  # 11 tests — per-paper traces, quality report, disagreements
│   ├── e2e_test_log.md         # Test coverage notes
│   └── e2e_search_screen_log.md  # Latest live E2E results
└── data/                       # gitignored — per-review databases, PDFs, exports
```

## Agent Architecture
| Agent | Model | Role |
|-------|-------|------|
| Abstract Screener — Primary | qwen3:8b | High-recall abstract screen (simplified exclusion criteria) |
| Abstract Screener — Verifier | gemma3:27b | Strict verification of primary includes (full exclusion criteria) |
| FT Screener — Primary | qwen3.5:27b | Full-text screen with specialty scope (/no_think, ~27s/paper) |
| FT Screener — Verifier | gemma3:27b | Strict FT verification, 5-test FP catcher (~20s/paper) |
| PDF Parser (A) | Docling + Qwen2.5-VL | Digital + scanned PDF to Markdown |
| Extractor (B) | deepseek-r1:32b | Two-pass structured extraction with reasoning trace |
| Auditor (C) | gemma3:27b | Cross-model verification + LOW_YIELD detection |
| Cloud Extractor (OpenAI) | o4-mini-2025-04-16 | Concordance arm — reasoning_effort=high |
| Cloud Extractor (Anthropic) | claude-sonnet-4-6 | Concordance arm — extended thinking |

## Data Architecture
- SQLite: One database per review (state machine, provenance)
- ChromaDB: Vector embeddings per review (disposable, rebuildable)
- File system: Immutable PDF + parsed Markdown store

## Paper Lifecycle
INGESTED → ABSTRACT_SCREENED_IN / ABSTRACT_SCREENED_OUT / ABSTRACT_SCREEN_FLAGGED → PDF_ACQUIRED → PARSED → FT_ELIGIBLE / FT_SCREENED_OUT / FT_FLAGGED → EXTRACTED / EXTRACT_FAILED → AI_AUDIT_COMPLETE → HUMAN_AUDIT_COMPLETE → REJECTED
(PARSED can also skip FT screening and go directly to EXTRACTED for reviews without FT screening)

## Pipeline Stages
1. **SEARCH** — PubMed + OpenAlex → deduplicate → add to DB
2. **ABSTRACT SCREEN** — Dual-model: primary (qwen3:8b, high-recall) → verifier (gemma3:27b, strict + 4 FP tests)
3. **ACQUIRE** — Unpaywall OA check → 5-strategy cascade download → manual list for remainder
4. **PARSE** — Docling (digital) or Qwen2.5-VL (scanned) → Markdown
5. **FT SCREEN** — Dual-model full-text: primary (qwen3.5:27b) → verifier (gemma3:27b, 5-test FP catcher). Specialty scope filtering. Text truncation to 32K chars.
6. **EXTRACT** — Pass 1: DeepSeek-R1 reasoning → Pass 2: structured JSON
7. **AUDIT** — Grep verify + semantic verify via gemma3:27b + LOW_YIELD detection (configurable threshold)
8. **ADJUDICATION GATE** — 12-stage workflow: 5 abstract + 1 acquisition + 2 FT + 4 extraction audit (human review required)
9. **EXPORT** — PRISMA CSV, evidence CSV/Excel/DOCX, methods section (min_status filtering)

## Inference
- Local models via Ollama at localhost:11434. Temperature 0 for all agents.
- Cloud models via OpenAI and Anthropic APIs (env vars OPENAI_API_KEY, ANTHROPIC_API_KEY).

## Key Patterns
- Review Spec (YAML) defines the entire review contract
- Protocol hashing: SHA-256 of screening/extraction sections for staleness detection
- Role-aware screening: primary model sees simplified exclusions (high recall), verifier sees full strict criteria (high precision). Cross-family model diversity (Qwen vs Gemma) catches different error types.
- Two-pass extraction: free reasoning trace → grammar-constrained structured output
- Evidence spans: source_snippet fields for traceability
- Grep + semantic audit: check snippet exists in paper, then verify value matches
- Per-review isolation: each review gets its own SQLite DB and directory tree
- 12-stage workflow enforcement: ABSTRACT_SCREENING_COMPLETE → ABSTRACT_DIAGNOSTIC_COMPLETE → ABSTRACT_CATEGORIES_CONFIGURED → ABSTRACT_QUEUE_EXPORTED → ABSTRACT_ADJUDICATION_COMPLETE → PDF_ACQUISITION → FULL_TEXT_SCREENING_COMPLETE → FULL_TEXT_ADJUDICATION_COMPLETE → EXTRACTION_COMPLETE → AI_AUDIT_COMPLETE_STAGE → AUDIT_QUEUE_EXPORTED → AUDIT_REVIEW_COMPLETE
- Full-text screening: dual-model cross-family (Qwen3.5:27b + Gemma3:27b), specialty scope filtering, /no_think mode, text truncation (32K chars), checkpoint/resume, 7 reason codes
- LOW_YIELD detection: post-audit quality gate — flags papers with fewer than N populated fields (default 4, configurable via low_yield_threshold). Included in audit queue export for PI review. PRISMA reports LOW_YIELD rejections as distinct category.
- Abstract retention policy: all paper data retained permanently regardless of screening outcome — ABSTRACT_SCREENED_OUT is a label, not a deletion
- PDF acquisition: 5-strategy cascade (Unpaywall direct → PMC OA package → IEEE stamp scrape → MDPI URL construction → DOI redirect with content negotiation), %PDF magic byte validation, strategy logging, --background tmux support. Manual download list shows multi-link options per paper (Google Scholar, Direct DOI, PubMed, Institutional Proxy) — proxy requires browser-level VPN/SSO
- Audit adjudication: export contested/flagged spans to Excel, import human decisions (accept/override/reject), spot-check sampling with configurable threshold
- min_status parameter on exporters: AI_AUDIT_COMPLETE (raw AI) vs HUMAN_AUDIT_COMPLETE (human-verified)
- ollama_options pass-through: per-model Ollama settings (e.g., num_ctx for memory-constrained models)

## Running
```bash
# Full pipeline
python scripts/run_pipeline.py --spec review_specs/surgical_autonomy_v1.yaml --name surgical_autonomy

# Expanded search screening (three-phase: fetch abstracts, primary screen, verification)
python scripts/screen_expanded.py                # all phases
python scripts/screen_expanded.py --screen-only  # primary dual-pass only
python scripts/screen_expanded.py --verify-only  # verification pass only

# PDF acquisition (OA check → download → manual list)
python -m engine.acquisition.check_oa --review surgical_autonomy --spec review_specs/surgical_autonomy_v1.yaml
python -m engine.acquisition.download --review surgical_autonomy [--retry] [--background]
python -m engine.acquisition.manual_list --review surgical_autonomy --spec review_specs/surgical_autonomy_v1.yaml

# Cloud extraction (concordance study)
PYTHONPATH=. python scripts/run_cloud_extraction.py --arm both --max-cost 25.00
PYTHONPATH=. python scripts/run_cloud_extraction.py --progress  # check status

# Search + screen test (20 papers, live)
python scripts/test_e2e_search_screen.py

# Workflow status
python -m engine.adjudication.advance_stage --review surgical_autonomy --status

# Full-text screening (primary + verification)
python -m engine.agents.ft_screener --review surgical_autonomy --spec review_specs/surgical_autonomy_v1.yaml
python -m engine.agents.ft_screener --review surgical_autonomy --spec review_specs/surgical_autonomy_v1.yaml --screen-only
python -m engine.agents.ft_screener --review surgical_autonomy --spec review_specs/surgical_autonomy_v1.yaml --verify-only

# Advance a workflow stage
python -m engine.adjudication.advance_stage --review surgical_autonomy \
    --stage ABSTRACT_DIAGNOSTIC_COMPLETE --note "50-paper sample reviewed"

# Test suite
python -m pytest tests/ -v                           # all tests (342)
python -m pytest tests/ -v -m "not network and not ollama"  # offline only (332)
```

## Current Review Status (surgical_autonomy)
- **Total DB: 10,039 papers** (251 original + 9,788 expanded/backfilled)
  - 9,390 ABSTRACT_SCREENED_OUT, 554 ABSTRACT_SCREENED_IN, **95 AI_AUDIT_COMPLETE** (original corpus)
  - 20,076 screening decisions, 958 verification decisions backfilled
  - 5 excluded in manual review: db_id 37, 149 (HSMR extended abstracts), 225 (SPR pediatric radiology), 229 (assistive not autonomous), 105 (VR sim, no autonomous robot)
- **Original corpus (EE-001 to EE-099, id ≤ 251):** 95 included, all PDFs downloaded, 95 local extractions, 1,429 evidence spans (872 verified / 557 flagged)
- Cloud concordance extraction: 95 papers × 2 arms (OpenAI o4-mini + Anthropic Sonnet 4.6) — complete
- Concordance PDFs: `data/surgical_autonomy/concordance_pdfs/` with EE-001 to EE-099 (gaps at EE-019, EE-062, EE-094)
- **Expanded corpus (EE-100+, id > 251):** 553 ABSTRACT_SCREENED_IN, 142 PDFs downloaded (25.7%), 411 failed — mostly closed-access 2025 publications
  - Failed by publisher: IEEE (193), Other (79), Elsevier (56), Springer/Nature (31), Wiley (20), MDPI (11), T&F/SAGE (10), Science (7), WK (4)
  - Manual download list: `data/surgical_autonomy/pdf_acquisition/manual_download_list.html`
- FT screening smoke test: 5 papers tested (3 FT_ELIGIBLE, 2 FT_EXCLUDE), ~27s primary + ~20s verifier per paper
- Exports: `data/surgical_autonomy/exports/` (evidence CSV/Excel/DOCX, PRISMA CSV, methods section, trace archives)

## Known Issues & Fixes
- DeepSeek-R1 outputs `confidence: -1` for NOT_FOUND fields → clamped via `@field_validator` in `EvidenceSpan`
- ~50% of grep audit failures caused by ellipsis in source_snippets (model abbreviates quotes with `...`)
- ~49% of grep failures caused by paraphrased (non-verbatim) snippets — values typically correct
- 4 papers have <15 spans (11-14): db_id 94, 102, 145, 221 — model omitted some fields
- Cloud parser: handles 8+ alternate JSON keys (`extractions`, `data`, `extracted_fields`, `extraction`, `results`, `entries`, `extraction_results`, `extracted_data`) plus flat field dict format — all normalized to `{"fields":[...]}`
- Cloud parser: Anthropic wraps JSON in ` ```json ``` ` markdown fences — stripped before parsing
- Cloud parser: o4-mini occasionally returns single flat span dict instead of list — wrapped automatically
- Screening FP rate: original criteria yielded 38% FP rate. Fixed via role-aware prompts (primary=permissive, verifier=strict) and model swap (gemma3:27b verifier)

## Architecture Docs
See `docs/architecture/` — 6-file code-audited reference (README, pipeline, models, state-machine, workflow, modules).

## Build Plan
See Project4_Surgical_Evidence_Engine_Unified_Plan_v5.md
