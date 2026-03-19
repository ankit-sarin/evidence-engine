# Surgical Evidence Engine — Architecture Overview

The Surgical Evidence Engine is a local-first systematic review pipeline that accepts a Review Spec (YAML), runs automated search across PubMed and OpenAlex, dual-model abstract screening with cross-family verification, PDF acquisition via a 6-step iterative human-AI loop, three-tier PDF parsing (Docling → PyMuPDF → Qwen2.5-VL), full-text screening with specialty scope filtering, codebook-driven two-pass extraction (DeepSeek-R1:32b), cross-model audit with LOW_YIELD detection, optional cloud concordance arms (OpenAI o4-mini, Anthropic Sonnet 4.6), distribution collapse monitoring, and human adjudication gates at every stage — then exports publication-ready evidence tables. All inference runs on-device via Ollama with temperature 0; no data leaves the machine. Each review is isolated in its own SQLite database with full provenance retention — every paper ever evaluated is permanently recorded regardless of screening outcome.

## Pipeline Flow

```
Review Spec (YAML)
       │
       ▼
 ┌──────────┐    ┌───────────────┐    ┌──────────────┐    ┌────────────────┐
 │ SEARCH   │───▶│ ABSTRACT      │───▶│ ABSTRACT     │───▶│ PDF ACQUIRE    │
 │ PubMed + │    │ SCREEN        │    │ ADJUDICATE   │    │ + QUALITY CHECK│
 │ OpenAlex │    │ (dual-model)  │    │ (human gate) │    │ (6-step loop)  │
 │ + Dedup  │    │ qwen3:8b +    │    │              │    │                │
 └──────────┘    │ gemma3:27b    │    └──────────────┘    └───────┬────────┘
                 └───────────────┘                                │
                                                                  ▼
 ┌──────────┐    ┌───────────────┐    ┌──────────────┐    ┌──────────────┐
 │ EXPORT   │◀───│ HUMAN REVIEW  │◀───│ AI AUDIT     │◀───│ EXTRACT      │
 │ PRISMA   │    │ ACCEPT/REJECT │    │ grep +       │    │ DeepSeek-R1  │
 │ CSV/DOCX │    │ /CORRECT      │    │ semantic     │    │ two-pass     │
 │ Traces   │    │               │    │ gemma3:27b   │    │              │
 └──────────┘    └───────────────┘    └──────────────┘    └──────────────┘
                                                                  ▲
 ┌──────────────────┐    ┌───────────────┐                        │
 │ CONCORDANCE      │◀───│ CLOUD EXTRACT │                 ┌──────┴───────┐
 │ ANALYSIS         │    │ o4-mini +     │                 │ FT SCREEN    │
 │ (multi-arm)      │    │ Sonnet 4.6    │                 │ + ADJUDICATE │
 └──────────────────┘    └───────────────┘                 │ qwen3:32b +  │
                                                           │ gemma3:27b   │
 ┌──────────────────┐                                      └──────────────┘
 │ DISTRIBUTION     │                                             ▲
 │ MONITOR          │                                      ┌──────┴───────┐
 │ (quality gate)   │                                      │ PARSE        │
 └──────────────────┘                                      │ Docling →    │
                                                           │ PyMuPDF →    │
                                                           │ Qwen2.5-VL   │
                                                           └──────────────┘
```

## Companion Documents

| Document | Description |
|----------|-------------|
| [pipeline.md](pipeline.md) | Stage-by-stage data flow, triggers, DB transitions, CLI commands, artifacts |
| [models.md](models.md) | Model roster, agent stack, Ollama stability architecture, cloud arm settings |
| [state-machine.md](state-machine.md) | Paper lifecycle statuses, transitions, terminal states, admin overrides |
| [workflow.md](workflow.md) | 12-stage workflow enforcement with human gates |
| [modules.md](modules.md) | Complete module inventory — every Python file with purpose, exports, dependencies |
| [_generated.json](_generated.json) | Machine-readable codebase metadata |

## Technology Stack

| Layer | Components |
|-------|------------|
| Language | Python 3.12.3, per-project virtualenv |
| Database | SQLite (one DB per review), WAL mode, full provenance |
| Local inference | Ollama (localhost:11434), temperature 0 for all models |
| Local models | qwen3:8b, qwen3:32b, gemma3:27b, deepseek-r1:32b, qwen2.5vl:7b |
| Cloud APIs | OpenAI (o4-mini-2025-04-16), Anthropic (claude-sonnet-4-6) |
| PDF processing | Docling, PyMuPDF (fitz), Qwen2.5-VL via Ollama |
| Search APIs | Biopython Entrez (PubMed), pyalex (OpenAlex), Unpaywall |
| Data models | Pydantic v2 (ReviewSpec, ExtractionResult, EvidenceSpan) |
| Export | openpyxl (Excel), python-docx (DOCX), csv, HTML templates |
| Background jobs | tmux (detached sessions with logging) |
| Testing | pytest (934 total, 920 offline / 14 network+ollama+integration) |
| Version control | Git + GitHub via SSH |

## Codebase Statistics

| Metric | Value |
|--------|-------|
| Source files | 145 |
| Lines of code | 40,825 |
| Total tests | 934 (920 offline, 14 deselected by default) |
| Engine modules | 74 `.py` files under `engine/` |
| Test files | 38 under `tests/` |
| Scripts | 28 `.py` + 2 `.sh` under `scripts/` |
| Analysis modules | 11 under `analysis/` |

*Generated 2026-03-19 from commit e124b20*
