# Model Roster

## Active Local Models (Ollama)

| Model | Role | Module | Default? | VRAM (approx) | Notes |
|-------|------|--------|----------|---------------|-------|
| **qwen3:8b** | Screener — Primary | `agents/screener.py` | Yes (spec.screening_models.primary) | ~5 GB | High-recall primary screen. Simplified exclusion criteria for maximum sensitivity. |
| **gemma3:27b** | Screener — Verifier | `agents/screener.py` | Yes (spec.screening_models.verification) | ~17 GB | Strict verification. Full exclusion criteria + 4 FP-catching tests. |
| **deepseek-r1:32b** | Extractor | `agents/extractor.py` | Yes (hardcoded) | ~20 GB | Two-pass extraction: reasoning trace → grammar-constrained JSON. 900s timeout. |
| **gemma3:27b** | Auditor | `agents/auditor.py` | Yes (DEFAULT_AUDITOR_MODEL) | ~17 GB | Cross-model semantic verification. Supports `ollama_options` pass-through. |
| **qwen2.5-vl:7b** | PDF Parser (scanned) | `parsers/pdf_parser.py` | Yes (hardcoded) | ~5 GB | Vision model for scanned PDF → Markdown conversion. |

> **Shared models**: gemma3:27b serves both screening verification and auditing, loaded once in VRAM.

## Cloud Models (API)

| Model | Role | Module | Pricing (per M tokens) | Notes |
|-------|------|--------|----------------------|-------|
| **o4-mini-2025-04-16** | Cloud Extractor (OpenAI) | `cloud/openai_extractor.py` | $1.10 in / $4.40 out | Concordance arm. reasoning_effort=high. |
| **claude-sonnet-4-6** | Cloud Extractor (Anthropic) | `cloud/anthropic_extractor.py` | $3.00 in / $15.00 out | Concordance arm. Extended thinking (10K budget tokens). |

## Cross-Family Diversity Rationale

The engine deliberately uses models from **different model families** at each pipeline stage to catch different error patterns:

| Stage | Model Family A | Model Family B | Why |
|-------|---------------|---------------|-----|
| **Screening** | Qwen (qwen3:8b) — primary | Google (gemma3:27b) — verifier | Different training data and biases. Qwen is permissive (high recall); Gemma is strict (high precision). Disagreements → SCREEN_FLAGGED for human review. |
| **Extraction → Audit** | DeepSeek (deepseek-r1:32b) — extractor | Google (gemma3:27b) — auditor | Auditor from a different family is more likely to catch systematic extraction errors. Same-family auditing tends to replicate the same mistakes. |
| **Cloud Concordance** | OpenAI (o4-mini) | Anthropic (sonnet-4.6) | Three independent extraction arms (local + 2 cloud) from 3 different providers enables concordance analysis and identifies provider-specific blind spots. |

## VRAM Budget

| Component | VRAM |
|-----------|------|
| qwen3:8b | ~5 GB |
| deepseek-r1:32b | ~20 GB |
| gemma3:27b (shared: verifier + auditor) | ~17 GB |
| qwen2.5-vl:7b | ~5 GB |
| **Peak (all loaded)** | **~47 GB** |
| DGX Spark available | 128 GB |
| Headroom | ~81 GB |

> Models are loaded on-demand by Ollama and evicted under memory pressure. Typical pipeline runs only load 2–3 models concurrently (screening uses qwen3:8b + gemma3:27b; extraction uses deepseek-r1:32b; audit uses gemma3:27b which may already be resident).

## Model Configuration

Models are configured at multiple levels:

1. **Review Spec YAML** (`review_specs/*.yaml`): `screening_models.primary`, `screening_models.verification`, `auditor_model`
2. **Module defaults**: Hardcoded in each agent module (used when spec doesn't override)
3. **Runtime**: `ollama_options` dict pass-through for per-call Ollama settings (e.g., `num_ctx: 4096` for memory-constrained models)

### Auditor Model Evaluation (2025-03-10)

Three models evaluated on 75 spans across 5 papers:

| Model | Verified | Flagged | Contested | Invalid | Errors | Agreement |
|-------|----------|---------|-----------|---------|--------|-----------|
| qwen3:32b | 35 | 23 | 14 | 3 | 0 | baseline |
| gemma3:27b | 38 | 21 | 13 | 3 | 0 | 89.3% w/ qwen3 |
| llama4:scout | 26 | 27 | 20 | 1 | 1 | 89.3% w/ qwen3 |

gemma3:27b was selected as default: similar accuracy to qwen3:32b, slightly more lenient (fewer false flags), 10 GB less VRAM, and provides cross-family diversity with the DeepSeek extractor.
