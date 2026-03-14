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
├── primer.md                   # Working state (maintained by Claude Code)
├── pyproject.toml              # pytest config (markers: network, ollama, integration)
├── requirements.txt
├── review_specs/               # Review spec YAML files
├── engine/
│   ├── core/                   # Pydantic models, YAML loader, SQLite state machine
│   ├── search/                 # PubMed, OpenAlex, DOI/PMID/fuzzy dedup
│   ├── agents/                 # Screener, FT screener, extractor, auditor
│   ├── cloud/                  # Cloud extraction arms (OpenAI, Anthropic)
│   ├── parsers/                # Three-tier PDF parser (Docling → PyMuPDF → Qwen2.5-VL)
│   ├── acquisition/            # Unpaywall, download cascade, PDF quality check, verify
│   ├── migrations/             # DB schema migrations
│   ├── adjudication/           # Workflow stages, screening/FT/audit adjudication
│   ├── utils/                  # tmux background, extraction cleanup, ollama preflight
│   ├── validators/             # Post-extraction field validation
│   └── exporters/              # PRISMA, evidence tables, DOCX, methods, traces
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
7. **AUDIT** — Grep verify + semantic verify via gemma3:27b + LOW_YIELD detection (configurable threshold)
8. **ADJUDICATION GATE** — 12-stage workflow: 5 abstract + 1 acquisition + 2 FT + 4 extraction audit (human review required)
9. **EXPORT** — PRISMA CSV, evidence CSV/Excel/DOCX, methods section (min_status filtering)

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

# Workflow status
python -m engine.adjudication.advance_stage --review surgical_autonomy --status

# Test suite
python -m pytest tests/ -v                                        # all tests
python -m pytest tests/ -v -m "not network and not ollama"        # offline only
```

## Architecture Docs
See `docs/architecture/` — 6-file code-audited reference (README, pipeline, models, state-machine, workflow, modules).
