# Model Roster and Agent Stack

## Agent Architecture

| Agent | Model | Family | Ollama Tag | Role | VRAM (approx) | Context / Settings |
|-------|-------|--------|------------|------|---------------|-------------------|
| S-abs — Primary | Qwen3 8B | Qwen (Alibaba) | `qwen3:8b` | High-recall abstract screen | ~5 GB | default, /no_think |
| S-abs — Verifier | Gemma3 27B | Gemma (Google) | `gemma3:27b` | Strict abstract verification | ~17 GB | default, /no_think |
| S-ft — Primary | Qwen3 32B | Qwen (Alibaba) | `qwen3:32b` | Full-text primary screen | ~20 GB | think=false, temp=0 |
| S-ft — Verifier | Gemma3 27B | Gemma (Google) | `gemma3:27b` | Full-text verification (5-test FP catcher) | ~17 GB | default |
| PDF Parser (scanned) | Qwen2.5-VL 7B | Qwen (Alibaba) | `qwen2.5vl:7b` | Vision OCR for scanned PDFs | ~5 GB | default |
| PDF Quality Checker | Qwen2.5-VL 7B | Qwen (Alibaba) | `qwen2.5vl:7b` | First-page AI classification | ~5 GB | configurable DPI (default 150) |
| Extractor | DeepSeek-R1 32B | DeepSeek | `deepseek-r1:32b` | Two-pass structured extraction | ~20 GB | default |
| Auditor | Gemma3 27B | Gemma (Google) | `gemma3:27b` | Cross-model verification + LOW_YIELD | ~17 GB | ollama_options pass-through |
| Cloud — OpenAI | o4-mini-2025-04-16 | GPT (OpenAI) | N/A (API) | Concordance extraction arm | N/A | reasoning_effort=high |
| Cloud — Anthropic | claude-sonnet-4-6 | Claude (Anthropic) | N/A (API) | Concordance extraction arm | N/A | extended thinking (budget=10K) |

## Locked vs Configurable Models

**Locked** — these models are hardcoded in engine code and cannot be changed via Review Spec:

| Model | Role | Why locked |
|-------|------|-----------|
| `gemma3:27b` | Verifier role (abstract + FT) | Cross-family principle; empirically validated |
| `deepseek-r1:32b` | Extractor | Two-pass architecture depends on `<think>` tag format |
| `qwen2.5vl:7b` | PDF parser (scanned) + quality checker | Only working vision model on Blackwell GB10 |

**Configurable** — set via Review Spec YAML (`screening_models`, `ft_screening_models`, `auditor_model`):

| Model | Current Config | Spec Field |
|-------|---------------|------------|
| `qwen3:8b` | Abstract primary | `screening_models.primary` |
| `qwen3:32b` | FT primary | `ft_screening_models.primary` |
| `gemma3:27b` | Auditor | `auditor_model` (or `spec.auditor_model`) |
| `o4-mini-2025-04-16` | Cloud arm | `OpenAIExtractor.MODEL_STRING` |
| `claude-sonnet-4-6` | Cloud arm | `AnthropicExtractor.MODEL_STRING` |

Note: The Python defaults for some configurable fields differ from the Review Spec values (e.g., `DEFAULT_VERIFICATION_MODEL = "qwen3:32b"` in `screener.py`, but the spec overrides this to `gemma3:27b`). The spec always takes precedence at runtime.

## Cross-Family Verification Principle

The engine deliberately uses models from different families at each verification boundary. This is based on empirical observation:

- **Same-family verification produced zero signal** — in a test with 4,100 screening decisions, a same-family verifier agreed with every primary decision, catching no false positives.
- **Cross-family verification caught FPs effectively** — different model architectures make different errors, so a second model from a different family acts as an effective check.
- **416/416 human-verifier concordance** — in the original corpus, the Gemma verifier's decisions matched human reviewer judgment perfectly.

The four cross-family verification boundaries:

| Boundary | Generator | Verifier | Families |
|----------|-----------|----------|----------|
| Abstract Screening | qwen3:8b (Qwen) | gemma3:27b (Gemma) | Qwen → Gemma |
| Full-Text Screening | qwen3:32b (Qwen) | gemma3:27b (Gemma) | Qwen → Gemma |
| Extraction → Audit | deepseek-r1:32b (DeepSeek) | gemma3:27b (Gemma) | DeepSeek → Gemma |
| Cloud Concordance | o4-mini (OpenAI) + Sonnet 4.6 (Anthropic) | Independent arms | GPT ↔ Claude |

## Per-Agent Model Selection

**S-abs — Primary (qwen3:8b):**
Chosen for speed and high recall. Simplified exclusion criteria to maximize sensitivity. At 8B parameters, fast enough for 10K+ paper screening runs. Specialty scope injected into prompt when configured.

**S-abs — Verifier (gemma3:27b):**
Chosen for precision and cross-family diversity. Sees full strict exclusion criteria plus FP-catching tests.

**S-ft — Primary (qwen3:32b):**
Replaced `qwen3.5:27b` after that model caused systematic Ollama hangs during FT screening batches. Qwen3:32b provides stable, reliable FT screening with structured reason codes. Text truncated to 32K chars via section-aware truncation. Think mode disabled for speed.

**S-ft — Verifier (gemma3:27b):**
Cross-family verification of FT primary includes. Returns `FT_ELIGIBLE` or `FT_FLAGGED` for human review.

**Extractor (deepseek-r1:32b):**
Selected for reasoning capability. Two-pass design: Pass 1 produces free reasoning in `<think>` tags; Pass 2 uses the reasoning trace as context for grammar-constrained structured JSON output via Ollama `format` parameter. Known issue: outputs `confidence: -1` for NOT_FOUND fields; clamped to 0.0 via Pydantic field validator.

**Auditor (gemma3:27b):**
Cross-family from the extractor (DeepSeek → Gemma). Performs grep verification (normalized text matching) then semantic LLM verification. Supports `ollama_options` pass-through for per-model settings. Post-audit: runs `check_low_yield()` to flag papers below threshold.

**PDF Parser — Three-tier routing:**
1. Docling (digital PDFs, primary)
2. PyMuPDF raw text extraction (Docling failure fallback)
3. Qwen2.5-VL:7b (scanned PDFs or sparse output < 100 chars/page)

**PDF Quality Checker (qwen2.5vl:7b):**
Same vision model used for post-download quality classification. Renders page 0 to PNG, classifies language and content type. Results drive human disposition workflow.

## Cloud Arm Settings

### OpenAI o4-mini (o4-mini-2025-04-16)

- `reasoning_effort="high"` — thorough extraction reasoning
- `response_format={"type": "json_object"}` — structured output
- 3-attempt retry with exponential backoff (2s, 4s)
- Cost tracking per extraction (input + output tokens)

### Anthropic Sonnet 4.6 (claude-sonnet-4-6)

- Extended thinking enabled: `budget_tokens=10000`, `max_tokens=16000`
- Markdown fence stripping — response parser strips `` ```json ``` `` wrappers
- Rate-limit-aware backoff: `RateLimitError` uses `retry-after` header or exponential (30s, 60s, 120s)
- Null-to-NR normalization: empty strings converted to null with `notes="empty_string_to_null"` annotation
- 3-attempt retry with exponential backoff for non-rate-limit errors (2s, 4s)

Both arms use the same extraction prompt (built by `build_extraction_prompt()`). Results stored in `cloud_extractions` and `cloud_evidence_spans` tables for concordance analysis.

## Ollama Stability Architecture

### Three-Layer Watchdog

**Layer 1 — httpx HTTP timeouts:**
- Connect timeout: 30s
- Read timeout: 900s (permissive; wall-clock watchdog is primary)
- Configured via `httpx.Timeout` in `engine/utils/ollama_client.py`

**Layer 2 — Wall-clock watchdog with model-tier thresholds:**
```
8b models:  300s  (5 min)
27b models: 600s  (10 min)
32b models: 900s  (15 min)
70b models: 1200s (20 min)
Default:    600s  (10 min)
```
Pattern-matched against model name (e.g., `deepseek-r1:32b` matches `32b` → 900s). Uses `ThreadPoolExecutor` with timeout — if the Ollama call exceeds the wall-clock limit, the call is cancelled and retried.

**Layer 3 — Service restart + final retry:**
If all retries (default 2, so 3 total attempts) are exhausted, `_restart_ollama_and_retry()` runs `sudo systemctl restart ollama`, waits 10s for the service to come back, then makes one final attempt. If that fails, raises `TimeoutError`.

### Environment Configuration

- `OLLAMA_FLASH_ATTENTION=true` — required, verified by `check_ollama_env()`
- `OLLAMA_MAX_LOADED_MODELS=1` — required, prevents multi-model VRAM competition
- `OLLAMA_NUM_PARALLEL=1` — single concurrent request
- `check_ollama_env()` reads from `systemctl show ollama --property=Environment` and asserts values match

### Proactive Periodic Restart

`RESTART_EVERY_N = 25` in `engine/agents/extractor.py` — after every 25 papers extracted, the extraction loop calls `restart_ollama()` to run `sudo systemctl restart ollama`, then polls `/api/tags` for up to 60s to confirm the service is back. This prevents gradual VRAM fragmentation during long batch runs.

Configurable via `--restart-every N` CLI flag (0 disables).

### Pre-Flight Health Checks

`engine/utils/ollama_preflight.py` — wired into FT screener, extractor, and auditor. Before any batch starts:

1. `check_ollama_env()` verifies `OLLAMA_FLASH_ATTENTION` and `OLLAMA_MAX_LOADED_MODELS` are set correctly
2. `check_model()` sends a minimal completion to each required model, measures load time
3. `_get_model_vram_gb()` queries `ollama.ps()` for per-model VRAM usage
4. Total VRAM checked against 100 GB budget (`_VRAM_BUDGET_GB`)
5. On any failure: batch aborts with clear error message — no silent failures into broken runs

**CLI:** `python -m engine.utils.ollama_preflight --models deepseek-r1:32b gemma3:27b`

### Temperature

All local models run at **temperature 0** (deterministic output). Set explicitly in every `ollama.chat()` call via `options={"temperature": 0}`.

### Model Digest Tracking

`get_model_digest(model_name)` in `ollama_client.py` calls Ollama's `/api/show` endpoint to retrieve the model binary digest. Stored per extraction in `extractions.model_digest` and `extractions.auditor_model_digest` for reproducibility tracking. Returns `None` (logged) if the endpoint is unavailable.

## Inference Infrastructure

- **Host:** DGX Spark (Blackwell GB10, sm_121)
- **VRAM:** 128 GB unified, 100 GB usable budget (20% safety margin)
- **Server:** Ollama at `localhost:11434`
- **Cloud APIs:** OpenAI and Anthropic via env vars `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`

---

*Generated 2026-03-17 from commit d0bf07c*
