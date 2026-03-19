Read primer.md for current project state before starting work.

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
├── CLAUDE.md                   # Static architecture (this file)
├── primer.md                   # Working state (maintained by Claude Code, gitignored)
├── pyproject.toml              # pytest config (markers: network, ollama, integration)
├── requirements.txt
├── review_specs/               # Review spec YAML files
├── engine/
│   ├── core/                   # Pydantic models, YAML loader, SQLite state machine
│   ├── search/                 # PubMed, OpenAlex, DOI/PMID/fuzzy dedup
│   ├── agents/                 # Screener, FT screener, extractor, auditor
│   ├── cloud/                  # Cloud extraction arms (OpenAI, Anthropic) + schema
│   ├── analysis/               # Concordance analysis (scoring, metrics, normalization, reports)
│   ├── parsers/                # Three-tier PDF parser (Docling → PyMuPDF → Qwen2.5-VL)
│   ├── acquisition/            # Unpaywall, download cascade, PDF quality check, verify
│   ├── migrations/             # DB schema migrations
│   ├── adjudication/           # Workflow stages, screening/FT/audit adjudication
│   ├── utils/                  # tmux background, extraction cleanup, ollama preflight
│   ├── validators/             # Extraction validator + distribution collapse monitor
│   └── exporters/              # PRISMA, evidence tables, DOCX, methods, traces
├── analysis/
│   └── paper1/                 # Human workbook import, consensus derivation, adjudication
├── scripts/                    # Pipeline runners, batch scripts, monitors
├── tests/                      # 440+ offline + 10 network/ollama tests
└── data/                       # gitignored — per-review databases, PDFs, exports
```

## Agent Architecture
| Agent | Model | Role |
|-------|-------|------|
| Abstract Screener — Primary | qwen3:8b | High-recall abstract screen (simplified exclusion criteria) |
| Abstract Screener — Verifier | gemma3:27b | Strict verification of primary includes (full exclusion criteria) |
| FT Screener — Primary | qwen3.5:27b | Full-text screen with specialty scope (/no_think, ~27s/paper) |
| FT Screener — Verifier | gemma3:27b | Strict FT verification, 5-test FP catcher (~20s/paper) |
| PDF Parser | Docling → PyMuPDF → Qwen2.5-VL:7b | Three-tier: digital → structural fallback → scanned vision |
| Extractor | deepseek-r1:32b | Two-pass structured extraction with reasoning trace |
| Auditor | gemma3:27b | Cross-model verification + LOW_YIELD detection |
| Cloud Extractor (OpenAI) | o4-mini-2025-04-16 | Concordance arm — reasoning_effort=high |
| Cloud Extractor (Anthropic) | claude-sonnet-4-6 | Concordance arm — extended thinking |

## Data Architecture
- SQLite: One database per review (state machine, provenance)
- ChromaDB: Vector embeddings per review (disposable, rebuildable)
- File system: Immutable PDF + parsed Markdown store

### Database Tables (beyond core papers/extractions/evidence_spans)
| Table | Purpose |
|-------|---------|
| cloud_extractions | Parallel to `extractions` — tracks arm, model, cost, reasoning traces |
| cloud_evidence_spans | Parallel to `evidence_spans` — cloud-arm field values |
| human_extractions | Human extractor workbook values (paper_id as "EE-NNN", extractor_id A/B/C/D) |

## Paper Lifecycle
INGESTED → ABSTRACT_SCREENED_IN / ABSTRACT_SCREENED_OUT / ABSTRACT_SCREEN_FLAGGED → PDF_ACQUIRED → PDF_EXCLUDED (terminal) or PARSED → FT_ELIGIBLE / FT_SCREENED_OUT / FT_FLAGGED → EXTRACTED / EXTRACT_FAILED → AI_AUDIT_COMPLETE → HUMAN_AUDIT_COMPLETE → REJECTED
(PARSED can skip FT screening directly to EXTRACTED for reviews without FT screening)
(PDF_EXCLUDED is terminal — papers excluded at quality check do not advance)
(Papers at AI_AUDIT_COMPLETE entering FT screening: decisions recorded but status not changed)

## Pipeline Stages
1. **SEARCH** — PubMed + OpenAlex → deduplicate → add to DB
2. **ABSTRACT SCREEN** — Dual-model: primary (qwen3:8b, high-recall) → verifier (gemma3:27b, strict + 4 FP tests)
3. **ACQUIRE** — Unpaywall OA check → 5-strategy cascade download → manual list for remainder
4. **PARSE** — Docling (digital) → PyMuPDF fallback (Docling errors) → Qwen2.5-VL (scanned) → Markdown
5. **FT SCREEN** — Dual-model full-text: primary (qwen3.5:27b) → verifier (gemma3:27b, 5-test FP catcher). Specialty scope filtering. Text truncation to 32K chars.
6. **EXTRACT** — Pass 1: DeepSeek-R1 reasoning → Pass 2: structured JSON
7. **CLOUD EXTRACT** — Parallel concordance arms: OpenAI o4-mini + Anthropic Sonnet 4.6. Same codebook prompt, independent parsing
8. **DISTRIBUTION CHECK** — Post-extraction quality gate: detect categorical field collapse across any arm
9. **AUDIT** — Grep verify + semantic verify via gemma3:27b + LOW_YIELD detection (configurable threshold)
10. **CONCORDANCE** — Multi-arm agreement analysis: scoring, normalization, kappa + percent agreement with 95% CI
11. **ADJUDICATION GATE** — 12-stage workflow: 5 abstract + 1 acquisition + 2 FT + 4 extraction audit (human review required)
12. **EXPORT** — PRISMA CSV, evidence CSV/Excel/DOCX, methods section (min_status filtering)

## Inference
- Local models via Ollama at localhost:11434. Temperature 0 for all agents.
- Cloud models via OpenAI and Anthropic APIs (env vars OPENAI_API_KEY, ANTHROPIC_API_KEY).

## Key Patterns
- Review Spec (YAML) defines the entire review contract
- Protocol hashing: SHA-256 of screening/extraction sections for staleness detection
- Role-aware screening: primary sees simplified exclusions (high recall), verifier sees full strict criteria (high precision). Cross-family diversity (Qwen vs Gemma)
- Two-pass extraction: free reasoning trace → grammar-constrained structured output
- Evidence spans: source_snippet fields for traceability
- Grep + semantic audit: check snippet exists in paper, then verify value matches
- Per-review isolation: each review gets its own SQLite DB and directory tree
- 12-stage workflow enforcement with human gates between phases
- Abstract retention policy: all paper data retained permanently — SCREENED_OUT is a label, not a deletion
- LOW_YIELD detection: post-audit quality gate, configurable threshold, PRISMA-reported
- PDF acquisition: 5-strategy cascade, %PDF validation, publisher grouping, --background tmux support
- PDF verify/import: filename matching, canonical rename to `EE-{nnn}_{Author}_{Year}.pdf`, DB update
- DB-driven PDF path resolution: `full_text_assets.pdf_path` → `papers.pdf_local_path` → glob fallback
- Audit adjudication: per-span ACCEPT/REJECT/CORRECT, spot-check sampling, two-pass import validation
- min_status parameter on exporters: AI_AUDIT_COMPLETE (raw AI) vs HUMAN_AUDIT_COMPLETE (human-verified)
- ollama_options pass-through: per-model Ollama settings (e.g., num_ctx)
- PRISMA reconciliation: validates terminal + in-progress = total, no double-counting
- Three-tier PDF parsing: Docling → PyMuPDF fallback (hyperlink/structure errors) → Qwen2.5-VL:7b (scanned). Sparse threshold <100 chars after both text parsers
- Self-documenting review workbooks: shared builder with DataValidation dropdowns, conditional formatting, Instructions sheet. Used by all 3 adjudication exporters
- PDF quality check: AI classification (vision model) + HTML disposition + JSON import. PDF_EXCLUDED is terminal
- Extraction validator: schema-driven field name + categorical value check. Read-only diagnostic
- Extraction cleanup: schema-hash-based stale data removal. Dry-run default. Pre-flight warning in extractor
- Ollama pre-flight: model health check + VRAM budget validation. Wired into FT screener, extractor, auditor
- FT screening: dual-model cross-family, specialty scope, /no_think, 32K truncation, checkpoint/resume, 7 reason codes. Status-aware for papers at any lifecycle stage

## Cloud Extraction Architecture
- `CloudExtractorBase` (engine/cloud/base.py): shared logic — pending paper query, codebook-driven prompt building, response JSON parsing (8+ alternate keys + raw content recovery), progress tracking, cost calculation, distribution monitor integration
- `OpenAIExtractor`: o4-mini-2025-04-16, reasoning_effort=high. Per-paper cost tracking (input/output/reasoning tokens)
- `AnthropicExtractor`: claude-sonnet-4-6, extended thinking (10K token budget). Streaming response with thinking block capture
- `store_extraction()` rejects 0-span results with ValueError — prevents silent data loss
- Cloud schema (engine/cloud/schema.py): creates cloud_extractions + cloud_evidence_spans tables
- Cost rates: OpenAI $1.10/$4.40 per 1M tokens (in/out); Anthropic $3.00/$15.00 per 1M tokens (in/out)

## Concordance Analysis Architecture
- Multi-arm alignment: load extractions from local, openai_o4_mini_high, anthropic_sonnet_4_6, human_A/B/C/D arms → align by paper_id
- Field-pair scoring (engine/analysis/scoring.py): MATCH/MISMATCH/AMBIGUOUS with fuzzy text matching for free-text fields
- Normalization (engine/analysis/normalize.py): canonical categorical prefix matching, multi-value fields, numeric handling
- Metrics (engine/analysis/metrics.py): Cohen's kappa, percent agreement, field summary statistics with 95% CI
- Reports (engine/analysis/report.py): terminal, CSV, and HTML concordance report generators
- Distribution collapse detection (engine/validators/distribution_monitor.py): post-extraction quality gate, flags COLLAPSED/LOW_VARIANCE categorical fields, minimum 10 papers, runs automatically at end of all extraction pipelines

## Paper 1 Analysis (analysis/paper1/)
- Human workbook import (human_import.py): parse v2 extraction workbooks (.xlsx), validate against codebook, import to human_extractions table
- Consensus derivation (consensus.py): identify ~30 shared papers across human extractors, derive majority-vote gold standard
- Adjudication (adjudication.py): export AMBIGUOUS concordance pairs for human review (HTML/JSON), import decisions

## Human-in-the-Loop Review Standard

All human review uses HTML → JSON → import round-trip.

File naming: `{review}_{stage}_{queue|decisions}.{html|json}`
Stages: abstract_adjudication, ft_adjudication, pdf_acquisition, pdf_quality, extraction_audit

Generators:
- engine/adjudication/abstract_adjudication_html.py
- engine/adjudication/ft_adjudication_html.py
- engine/acquisition/pdf_quality_html.py (mode=acquisition | quality_check)
- engine/review/extraction_audit_html.py

Importers auto-detect .json vs .xlsx. Default --file auto-discovers
from naming convention. xlsx retained with --format xlsx for archival.

## Running
```bash
# Full pipeline
python scripts/run_pipeline.py --spec review_specs/surgical_autonomy_v1.yaml --name surgical_autonomy

# Expanded search screening
python scripts/screen_expanded.py                # all phases
python scripts/screen_expanded.py --screen-only  # primary dual-pass only
python scripts/screen_expanded.py --verify-only  # verification pass only

# PDF acquisition
python -m engine.acquisition.check_oa --review surgical_autonomy --spec review_specs/surgical_autonomy_v1.yaml
python -m engine.acquisition.download --review surgical_autonomy [--retry] [--background]
python -m engine.acquisition.verify_downloads --review surgical_autonomy [--dry-run]
python -m engine.acquisition.pdf_quality_check --review surgical_autonomy --spec review_specs/surgical_autonomy_v1.yaml
python -m engine.acquisition.pdf_quality_import --review surgical_autonomy --input dispositions.json

# Full-text screening
python -m engine.agents.ft_screener --review surgical_autonomy --spec review_specs/surgical_autonomy_v1.yaml
python -m engine.agents.ft_screener ... --screen-only
python -m engine.agents.ft_screener ... --verify-only

# Extraction cleanup (schema transition)
python -m engine.utils.extraction_cleanup --review surgical_autonomy          # dry-run
python -m engine.utils.extraction_cleanup --review surgical_autonomy --confirm # execute

# Post-extraction validation
python -m engine.validators.extraction_validator --review surgical_autonomy

# Ollama pre-flight
python -m engine.utils.ollama_preflight --models qwen3.5:27b gemma3:27b deepseek-r1:32b

# Cloud extraction
PYTHONPATH=. python scripts/run_cloud_extraction.py --arm both --max-cost 25.00
PYTHONPATH=. python scripts/run_cloud_extraction.py --progress

# Distribution monitor
python -m engine.validators.distribution_monitor --review surgical_autonomy --arm local
python -m engine.validators.distribution_monitor --review surgical_autonomy --arm anthropic_sonnet_4_6

# Cloud span backfill (for extractions missing span rows)
PYTHONPATH=. python scripts/backfill_cloud_spans.py --review surgical_autonomy [--dry-run]

# q8 KV cache validation
PYTHONPATH=. python scripts/q8_validation.py
PYTHONPATH=. python scripts/q8_validation_fast.py

# Workflow status
python -m engine.adjudication.advance_stage --review surgical_autonomy --status

# Test suite
python -m pytest tests/ -v                                        # all tests
python -m pytest tests/ -v -m "not network and not ollama"        # offline only
```

## Architecture Docs
See `docs/architecture/` — 6-file code-audited reference (README, pipeline, models, state-machine, workflow, modules).
