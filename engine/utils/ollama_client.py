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
import subprocess
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


def get_model_digest(model_name: str) -> str | None:
    """Return the Ollama model digest (hash) for the given model name.

    Calls POST /api/show to get model metadata.  Returns the digest string
    on success, or None on failure (logged at WARNING).
    """
    try:
        info = _client.show(model_name)
        # ollama-python returns a dict-like with 'digest' at the top level
        # or under modelinfo.  Try the common paths.
        digest = None
        if hasattr(info, "digest"):
            digest = info.digest
        elif isinstance(info, dict):
            digest = info.get("digest")
        # Fallback: modelinfo dict may contain general.file_type etc. but
        # the top-level 'digest' field is what we want (set by ollama show).
        if not digest and hasattr(info, "modelinfo"):
            mi = info.modelinfo if not isinstance(info.modelinfo, dict) else info.modelinfo
            if isinstance(mi, dict):
                digest = mi.get("digest")
        return digest or None
    except Exception as exc:
        logger.warning("Failed to get digest for model %s: %s", model_name, exc)
        return None


# ── Strict digest fetch for judge runs ───────────────────────────────

OLLAMA_BASE_URL = "http://localhost:11434"
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")


class ModelDigestError(RuntimeError):
    """Raised by fetch_model_digest when the digest cannot be verified.

    No silent fallback to the model-name string — callers should treat this
    as a hard failure. Used by the Paper 1 judge orchestrator so every
    judge_runs row stores a verifiable content digest, not a tag.
    """


def fetch_model_digest(
    model_name: str,
    *,
    base_url: str = OLLAMA_BASE_URL,
    timeout: float = 5.0,
) -> str:
    """Return the SHA-256 manifest digest for `model_name` via /api/tags.

    The digest is not exposed on /api/show in current Ollama versions; the
    canonical structured field is models[].digest on /api/tags. This
    function performs the HTTP GET, filters by exact name match, and
    asserts the result is a 64-char lowercase hex string.

    Raises
    ------
    ModelDigestError
        On any of: non-200 response, unparseable JSON, missing models key,
        zero or multiple entries matching model_name, or a digest that
        does not match ^[0-9a-f]{64}$.
    """
    url = f"{base_url.rstrip('/')}/api/tags"
    try:
        resp = httpx.get(url, timeout=timeout)
    except httpx.HTTPError as exc:
        raise ModelDigestError(
            f"fetch_model_digest: HTTP error calling {url}: {exc}"
        ) from exc

    if resp.status_code != 200:
        raise ModelDigestError(
            f"fetch_model_digest: non-200 from {url}: "
            f"status={resp.status_code} body={resp.text[:200]!r}"
        )

    try:
        payload = resp.json()
    except ValueError as exc:
        raise ModelDigestError(
            f"fetch_model_digest: response not JSON: {exc}"
        ) from exc

    models = payload.get("models") if isinstance(payload, dict) else None
    if not isinstance(models, list):
        raise ModelDigestError(
            f"fetch_model_digest: 'models' key missing or not a list in "
            f"response: keys={sorted(payload.keys()) if isinstance(payload, dict) else type(payload).__name__}"
        )

    matches = [m for m in models if isinstance(m, dict) and m.get("name") == model_name]
    if len(matches) == 0:
        available = [m.get("name") for m in models if isinstance(m, dict)]
        raise ModelDigestError(
            f"fetch_model_digest: no entry for model_name={model_name!r} "
            f"in /api/tags; available={available}"
        )
    if len(matches) > 1:
        raise ModelDigestError(
            f"fetch_model_digest: ambiguous — {len(matches)} entries "
            f"match model_name={model_name!r} in /api/tags"
        )

    digest = matches[0].get("digest")
    if not isinstance(digest, str) or not _DIGEST_RE.fullmatch(digest):
        raise ModelDigestError(
            f"fetch_model_digest: malformed digest for {model_name!r}: "
            f"{digest!r} (expected 64 lowercase hex chars)"
        )

    return digest


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
                # All retries exhausted — attempt Ollama restart as last resort
                try:
                    return _restart_ollama_and_retry(
                        model=model, messages=messages,
                        paper_label=paper_label,
                        effective_timeout=effective_timeout,
                        max_retries=max_retries,
                        **kwargs,
                    )
                except RuntimeError:
                    raise TimeoutError(
                        f"Ollama call timed out after {1 + max_retries} attempts + restart "
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


# ── Ollama restart recovery (Layer 3) ────────────────────────────────


def _restart_ollama_and_retry(
    *, model, messages, paper_label, effective_timeout, max_retries, **kwargs,
):
    """Restart the Ollama service and attempt one final call.

    Returns the response on success, or None if the restart or final call fails.
    """
    logger.warning(
        "All %d retries exhausted — restarting Ollama service (model=%s, %s)",
        1 + max_retries, model, paper_label,
    )
    try:
        subprocess.run(
            ["sudo", "systemctl", "restart", "ollama"],
            timeout=30,
            check=True,
            capture_output=True,
        )
    except Exception as restart_exc:
        raise RuntimeError(
            f"Ollama restart failed: {restart_exc} — cannot recover"
        ) from restart_exc

    logger.info("Ollama restarted — waiting 10s for stabilization")
    time.sleep(10)

    # One final attempt after restart
    executor = ThreadPoolExecutor(max_workers=1)
    try:
        future = executor.submit(
            _client.chat,
            model=model,
            messages=messages,
            **kwargs,
        )
        result = future.result(timeout=effective_timeout)
        logger.info(
            "Post-restart call succeeded (model=%s, %s)", model, paper_label,
        )
        return result
    except Exception as post_exc:
        executor.shutdown(wait=False, cancel_futures=True)
        raise RuntimeError(
            f"Post-restart Ollama call failed: {post_exc} — cannot recover "
            f"(model={model}, {paper_label})"
        ) from post_exc
