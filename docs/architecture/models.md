# Models & Agent Architecture

## Agent Roster

| Agent | Model | Role | Source |
|-------|-------|------|--------|
| Abstract Screener — Primary | qwen3:8b | High-recall dual-pass abstract screening (simplified exclusion criteria) | `engine/agents/screener.py` |
| Abstract Screener — Verifier | gemma3:27b | Strict verification of primary includes (full exclusion criteria) | code default qwen3:32b |
| FT Screener — Primary | qwen3:32b | Full-text primary screen with specialty scope, /no_think, 32K truncation | code default qwen3.5:27b |
| FT Screener — Verifier | gemma3:27b | Strict FT verification, 5-test FP catcher | `engine/core/review_spec.py` (FTScreeningModels) |
| PDF Parser (scanned) | qwen2.5vl:7b | Vision OCR for scanned PDFs (200 DPI page rendering) | `engine/parsers/pdf_parser.py` |
| PDF Quality Checker | qwen2.5vl:7b | First-page AI classification (language + content type) | `engine/acquisition/pdf_quality_check.py` |
| Extractor | deepseek-r1:32b | Two-pass: Pass 1 reasoning (`<think>` tags) → Pass 2 structured JSON | `engine/agents/extractor.py` |
| Auditor | gemma3:27b | Cross-model grep + semantic verification, LOW_YIELD detection | `engine/agents/auditor.py` |
| Cloud — OpenAI | o4-mini-2025-04-16 | Concordance arm — reasoning_effort=high, json_object format | `engine/cloud/openai_extractor.py` |
| Cloud — Anthropic | claude-sonnet-4-6 | Concordance arm — extended thinking (budget_tokens=10000) | `engine/cloud/anthropic_extractor.py` |

## Locked vs Configurable

| Category | Models | Override Mechanism |
|----------|--------|--------------------|
| **Locked in code** | deepseek-r1:32b (extractor) | Requires code change in `engine/agents/extractor.py` |
| **Configurable via ReviewSpec** | qwen3:8b (abstract primary), gemma3:27b (abstract verifier) | `screening_models.primary`, `screening_models.verification` |
| **Configurable via ReviewSpec** | qwen3:32b (FT primary), gemma3:27b (FT verifier) | `ft_screening_models.primary`, `ft_screening_models.verifier` |
| **Configurable via ReviewSpec** | gemma3:27b (auditor) | `auditor_model` (also overridable via CLI `--model`) |
| **Configurable via ReviewSpec** | qwen2.5vl:7b (PDF quality, vision parser) | `pdf_quality_check.ai_model`, `pdf_parsing.vision_model` |
| **Configurable via ReviewSpec** | o4-mini-2025-04-16, claude-sonnet-4-6 | `cloud_models.openai.model`, `cloud_models.anthropic.model` |

## Cross-Family Verification Principle

Every verification step uses a different model family from the generation step. Same-family verification produces negligible quality signal.

| Stage | Generator | Verifier | Families |
|-------|-----------|----------|----------|
| Abstract screening | qwen3:8b (Alibaba) | gemma3:27b (Google) | Cross-family |
| FT screening | qwen3:32b (Alibaba) | gemma3:27b (Google) | Cross-family |
| Extraction → Audit | deepseek-r1:32b (DeepSeek) | gemma3:27b (Google) | Cross-family |
| Cloud concordance | o4-mini (OpenAI) | claude-sonnet-4-6 (Anthropic) | Cross-provider |

## Inference Parameters

All local models run via Ollama at localhost:11434.

| Parameter | Value | Scope |
|-----------|-------|-------|
| temperature | 0 | All local agents (screener, FT screener, extractor, auditor, parser) |
| think | False (`/no_think` in prompt) | Abstract screener, FT screener (configurable), extractor Pass 2, auditor |
| think | True | Extractor Pass 1 only (reasoning trace via `<think>` tags) |
| format | Pydantic JSON schema | All structured outputs (screening decisions, extractions, audit verdicts) |
| FT text budget | 32,000 chars (`FT_MAX_TEXT_CHARS`) | FT screener (section-aware truncation) |
| Snippet validation | `INVALID_SNIPPET_RE` regex | Extractor post-processing (detects ellipsis bridging) |
| Snippet retries | 2 (`SNIPPET_MAX_RETRIES`) | Extractor (per-field retry for invalid snippets) |

## Ollama Stability Architecture

Three-layer timeout and recovery system (`engine/utils/ollama_client.py`):

| Layer | Mechanism | Values |
|-------|-----------|--------|
| 1. HTTP | httpx connect/read/write/pool timeouts | connect=30s, read=900s, write=30s, pool=30s |
| 2. Wall-clock | `ThreadPoolExecutor.result(timeout)` per attempt | 8b→300s, 27b→600s, 32b→900s, 70b→1200s (default 600s) |
| 3. Restart recovery | `sudo systemctl restart ollama` + poll `/api/tags` (60s max) | Final attempt after all retries exhausted |

**Retry logic:** Up to `1 + max_retries` attempts (default 3 total) with 30s delay between retries. On final exhaustion, restart Ollama and make one last attempt.

**Proactive restart:** Extractor restarts Ollama every `RESTART_EVERY_N` papers (default 25, configurable via `--restart-every 0` to disable). Restart failure is graceful — logged via `logger.exception` and the run continues (does not crash the pipeline).

### Required Ollama Environment

Validated by `engine/utils/ollama_preflight.py`:

| Variable | Required Value | Purpose |
|----------|----------------|---------|
| `OLLAMA_FLASH_ATTENTION` | `true` | Enable flash attention |
| `OLLAMA_MAX_LOADED_MODELS` | `1` | Prevent VRAM contention |

**VRAM budget:** 100 GB (`_VRAM_BUDGET_GB`). Pre-flight validates model load + VRAM sum before batch runs. Raises `RuntimeError` if budget exceeded. All four agents (abstract screener, FT screener, extractor, auditor) run pre-flight checks before batch operations.

**Model digest tracking:** `ollama.show()` captures exact model hash at extraction time. Stored in `extractions.model_digest` and `extractions.auditor_model_digest` for reproducibility.

## Cloud Arm Settings

### OpenAI (`engine/cloud/openai_extractor.py`)

| Setting | Value |
|---------|-------|
| Model | o4-mini-2025-04-16 |
| ARM identifier | `openai_o4_mini_high` |
| reasoning_effort | high |
| response_format | `{"type": "json_object"}` |
| Retry attempts | 3 per paper (exponential backoff) |
| Rate limit backoff | retry-after header or 30s × 2^attempt (30s/60s/120s) |
| Auth error | Immediate re-raise — aborts entire run |
| Per-paper isolation | Failures logged and counted; run continues to next paper |

### Anthropic (`engine/cloud/anthropic_extractor.py`)

| Setting | Value |
|---------|-------|
| Model | claude-sonnet-4-6 |
| ARM identifier | `anthropic_sonnet_4_6` |
| max_tokens | 16,000 |
| thinking | enabled, budget_tokens=10,000 |
| Post-processing | Markdown ` ```json ` fence stripping, empty string → null conversion |
| Retry attempts | 3 per paper (exponential backoff) |
| Rate limit backoff | retry-after header or 30s × 2^attempt (30s/60s/120s) |
| Auth error | Immediate re-raise — aborts entire run |
| Per-paper isolation | Failures logged and counted; run continues to next paper |

### Shared Cloud Logic (`engine/cloud/base.py`)

Both arms inherit from `CloudExtractorBase` which provides:

- **Prompt building:** Delegates to local extractor's `build_extraction_prompt()` (codebook-driven, same prompt for all arms)
- **Response parsing:** Handles 8+ alternate JSON top-level keys (`fields`, `extractions`, `extracted_fields`, `extracted_data`, `data`, `extraction`, `results`, `entries`), markdown fence stripping, flat dict restructuring, null → "NR" conversion
- **Atomic storage:** `store_result()` rejects 0-span results with `ValueError`, full rollback on any error
- **Progress tracking:** `get_progress()` returns total/completed/remaining/cost
- **Distribution check:** `run_distribution_check()` integrates with post-extraction quality gate

### Cloud Tables (`engine/cloud/schema.py`)

| Table | Key Columns | Constraints |
|-------|-------------|-------------|
| `cloud_extractions` | paper_id, arm, model_string, extracted_data, reasoning_trace, prompt_text, input_tokens, output_tokens, reasoning_tokens, cost_usd, extraction_schema_hash, extracted_at | UNIQUE(paper_id, arm) |
| `cloud_evidence_spans` | cloud_extraction_id, field_name, value, source_snippet, confidence, tier, notes | UNIQUE(cloud_extraction_id, field_name) |

*Generated 2026-03-19 from commit e124b20*
