"""Shared Ollama client with HTTP-level timeouts and wall-clock watchdog.

All batch runners (FT screener, extractor, auditor, screener, PDF parser)
should call `ollama_chat(...)` instead of `ollama.chat(...)` directly.
This provides:
  1. HTTP-level connect/read timeouts via httpx
  2. Wall-clock watchdog via concurrent.futures to catch mid-generation hangs
  3. Retry logic with configurable attempts and delay
  4. Structured WARNING logging on every timeout event
"""

import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

import httpx
import ollama

logger = logging.getLogger(__name__)

# ── HTTP-level timeouts (Layer 1) ────────────────────────────────────

_HTTP_CONNECT_TIMEOUT = 30.0   # seconds to establish TCP connection
_HTTP_READ_TIMEOUT = 900.0     # permissive — prompt eval on 32K+ chars can take
                               # several minutes with no bytes; the wall-clock
                               # watchdog (Layer 2) is the primary timeout guard

_httpx_timeout = httpx.Timeout(
    connect=_HTTP_CONNECT_TIMEOUT,
    read=_HTTP_READ_TIMEOUT,
    write=30.0,
    pool=30.0,
)

_client = ollama.Client(timeout=_httpx_timeout)

# ── Wall-clock watchdog limits (Layer 2) ─────────────────────────────
# Keyed by regex pattern matched against model name.
# Order matters: first match wins.

MODEL_TIMEOUTS: dict[str, float] = {
    r"8b":  300.0,   # 5 min for 8B models
    r"27b": 600.0,   # 10 min for 27B models
    r"32b": 900.0,   # 15 min for 32B models
    r"70b": 1200.0,  # 20 min for 70B models
}

_DEFAULT_WALL_TIMEOUT = 600.0  # fallback if no pattern matches

# ── Retry defaults ───────────────────────────────────────────────────

DEFAULT_MAX_RETRIES = 2
DEFAULT_RETRY_DELAY = 30  # seconds


def _wall_timeout_for_model(model: str) -> float:
    """Return wall-clock timeout in seconds for a given model name."""
    for pattern, timeout in MODEL_TIMEOUTS.items():
        if re.search(pattern, model, re.IGNORECASE):
            return timeout
    return _DEFAULT_WALL_TIMEOUT


def ollama_chat(
    *,
    model: str,
    messages: list[dict],
    paper_id: int | None = None,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_delay: float = DEFAULT_RETRY_DELAY,
    wall_timeout: float | None = None,
    **kwargs,
):
    """Call Ollama chat with HTTP timeouts, wall-clock watchdog, and retries.

    Parameters
    ----------
    model : str
        Ollama model name.
    messages : list[dict]
        Chat messages in Ollama format.
    paper_id : int | None
        Paper ID for logging context (optional).
    max_retries : int
        Number of retries after the initial attempt (default 2 → 3 total).
    retry_delay : float
        Seconds to wait between retries.
    wall_timeout : float | None
        Override wall-clock timeout. If None, auto-detected from model name.
    **kwargs
        Passed through to ollama.Client.chat() (format, options, think, etc.).

    Returns
    -------
    ollama response object

    Raises
    ------
    TimeoutError
        If all attempts exceed the wall-clock timeout.
    Exception
        If all retries exhausted on a non-timeout error.
    """
    effective_timeout = wall_timeout or _wall_timeout_for_model(model)
    paper_label = f"paper_id={paper_id}" if paper_id is not None else "paper_id=unknown"

    for attempt in range(1 + max_retries):
        t0 = time.monotonic()
        executor = ThreadPoolExecutor(max_workers=1)
        try:
            future = executor.submit(
                _client.chat,
                model=model,
                messages=messages,
                **kwargs,
            )
            return future.result(timeout=effective_timeout)

        except FuturesTimeoutError:
            # Abandon the hung thread — do not wait for it
            executor.shutdown(wait=False, cancel_futures=True)
            elapsed = time.monotonic() - t0
            logger.warning(
                "Ollama wall-clock timeout: model=%s, %s, elapsed=%.0fs, "
                "limit=%.0fs, attempt=%d/%d",
                model, paper_label, elapsed, effective_timeout,
                attempt + 1, 1 + max_retries,
            )
            if attempt < max_retries:
                time.sleep(retry_delay)
            else:
                raise TimeoutError(
                    f"Ollama call timed out after {1 + max_retries} attempts "
                    f"(model={model}, {paper_label}, limit={effective_timeout}s)"
                )

        except (httpx.TimeoutException, httpx.ConnectError) as exc:
            executor.shutdown(wait=False, cancel_futures=True)
            elapsed = time.monotonic() - t0
            logger.warning(
                "Ollama HTTP timeout: model=%s, %s, elapsed=%.0fs, "
                "error=%s, attempt=%d/%d",
                model, paper_label, elapsed, exc,
                attempt + 1, 1 + max_retries,
            )
            if attempt < max_retries:
                time.sleep(retry_delay)
            else:
                raise

        except Exception as exc:
            executor.shutdown(wait=False, cancel_futures=True)
            elapsed = time.monotonic() - t0
            logger.warning(
                "Ollama call failed: model=%s, %s, elapsed=%.0fs, "
                "error=%s, attempt=%d/%d",
                model, paper_label, elapsed, exc,
                attempt + 1, 1 + max_retries,
            )
            if attempt < max_retries:
                time.sleep(retry_delay)
            else:
                raise
