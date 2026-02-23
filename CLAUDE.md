# Surgical Evidence Engine (Project 4)

## Location
~/projects/evidence-engine

## Deployment
- Port: 7864
- URL: evidence.digitalsurgeon.dev
- Service: evidence-engine (systemd, when ready)

## Purpose
Local systematic review engine on DGX Spark. Accepts Review Specs (YAML), runs search/screening/extraction/audit pipeline, exports publication-ready evidence tables. No data leaves the machine.

## Agent Architecture
| Agent | Model | Role |
|-------|-------|------|
| Screener (S) | qwen3:8b | Dual-pass title/abstract screening |
| PDF Parser (A) | Docling + MiniCPM-V | Digital + scanned PDF to Markdown |
| Extractor (B) | deepseek-r1:32b | Two-pass structured extraction with reasoning trace |
| Auditor (C) | qwen3:32b | Cross-model verification of extractions |

## Data Architecture
- SQLite: One database per review (state machine, provenance)
- ChromaDB: Vector embeddings per review (disposable, rebuildable)
- File system: Immutable PDF + parsed Markdown store

## Inference
All models via Ollama at localhost:11434. Temperature 0 for all agents.

## Key Patterns
- Review Spec (YAML) defines the entire review contract
- Dual-pass screening: two independent runs, flag disagreements
- Two-pass extraction: free reasoning trace → grammar-constrained structured output
- Evidence spans: source_snippet fields for traceability
- Per-review isolation: autonomy and SAGES reviews run concurrently

## Build Plan
See Project4_Surgical_Evidence_Engine_Unified_Plan_v5.md
