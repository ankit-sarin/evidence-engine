"""Shared Ollama client with HTTP-level timeouts and wall-clock watchdog.

All batch runners (FT screener, extractor, auditor, screener, PDF parser)
should call `ollama_chat(...)` instead of `ollama.chat(...)` directly.
This provides:
  1. HTTP-level connect/read timeouts via httpx
  2. Wall-clock watchdog via concurrent.futures to catch mid-generation hangs
  3. Retry logic with configurable attempts and delay
  4. Structured WARNING logging on every timeout event
  5. An input-fit guard on every call (INPUT-FIT-01): an input that cannot fit
     the model's context is refused before it is sent, and an input the
     runtime truncated or silently dropped part of fails loudly after the call
"""

import json
import logging
import os
import re
import shlex
import subprocess
import time
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from pathlib import Path

import httpx
import ollama

from engine.utils.ollama_lock import foreign_lock_held

logger = logging.getLogger(__name__)

# Opt-out for harnesses that must never restart the service — a runtime-version
# A/B talking to a second Ollama on another port, say, where restarting the
# systemd unit would neither help nor be in scope. QUALGAP-01 achieved this by
# monkeypatching `_restart_ollama_and_retry` from the outside; this is the
# supported switch that replaces that.
RESTART_OPT_OUT_ENV = "EVIDENCE_ENGINE_NO_OLLAMA_RESTART"


def restart_disabled() -> bool:
    """True if the last-resort service restart has been switched off explicitly."""
    return os.environ.get(RESTART_OPT_OUT_ENV, "").strip().lower() not in ("", "0", "false", "no")

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

# ── Input fit (INPUT-FIT-01) ─────────────────────────────────────────
#
# PROVISIONAL under CONST-PROV-01. Both ratios are tokens per character of
# message text, from one measurement on this host: the 418 screening calls of
# SCREEN-AUTH-01 Phase 2f (committed outputs, 8a7dc4f), their request text
# rebuilt offline and divided into Ollama's own prompt_eval_count. Observed
# range 0.1917 (gemma3:27b) to 0.2586 (qwen3:8b), median ~0.21 (INPUT-FIT-01
# Phase 2 stop report). Ollama reported identical counts for 172 of 172
# identical request pairs, so the prompt cache does not lower the count.
#
# RATIO_MIN answers "can this input possibly fit?". It is the smallest ratio
# observed, rounded down, so chars x RATIO_MIN is a LOW estimate: an input
# refused at it cannot fit however favourably it tokenizes. It is deliberately
# not a worst-case ratio — at ~0.43 the pre-call check refused 97 of 415 judge
# Pass-2 prompts that do fit. The post-call check is the guarantee that nothing
# was cut.
RATIO_MIN = 0.19

# RATIO_DROP answers "did the model see all of it?". Half the observed minimum:
# a count below chars x RATIO_DROP means roughly half the text or more never
# reached the model — the silent middle-message drop that logs no truncation
# WARN (INPUT-FIT-01 Phase 1, extractor Pass 2). No observed call comes near it.
RATIO_DROP = 0.10

# Ollama 0.21.0's default context when neither OLLAMA_CONTEXT_LENGTH nor
# options.num_ctx is set, before it is clamped to the model's trained context.
# A runtime property, not a model property: observed in the journal's load lines
# as `msg="requested context size too large for model" num_ctx=262144` and as
# `KvSize:262144` for models trained on 262,144 tokens. Re-verify on any Ollama
# version change.
SERVER_DEFAULT_CTX = 262_144

# Where the local service's OLLAMA_CONTEXT_LENGTH would be set. Read as files —
# the drop-in is where this host keeps Ollama's environment — never through
# systemctl, which the test suite's service-call fence refuses by design.
SERVICE_UNIT_FILE = Path("/etc/systemd/system/ollama.service")
SERVICE_DROPIN_DIR = Path("/etc/systemd/system/ollama.service.d")
_LOCAL_SERVICE_HOSTS = frozenset({"http://127.0.0.1:11434", "http://localhost:11434"})

_CEILING_CACHE: dict[tuple[str, str], int] = {}


class InputFitError(Exception):
    """Base class of the input-fit guard's refusals (INPUT-FIT-01).

    Deliberately an `Exception`, not a `RuntimeError`: `ollama_chat` turns a
    RuntimeError from its restart path into a TimeoutError, and an input that
    does not fit is not a timeout. It is raised outside the retry handlers, so
    it is never retried — the same input gets the same answer.

    Every field is available both as an attribute and in `fields`.
    """

    def __init__(self, message: str, **fields):
        super().__init__(message)
        self.fields = fields
        for name, value in fields.items():
            setattr(self, name, value)


class CeilingUnavailable(InputFitError):
    """The model's trained context could not be read, so no call is made."""

    def __init__(self, *, model: str, reason: str):
        super().__init__(
            f"cannot resolve the context ceiling for {model}: {reason}. The call "
            "was not sent; a guard that cannot read its ceiling must not guess one.",
            model=model, reason=reason,
        )


class InputOverflow(InputFitError):
    """Refused before the call: even the low estimate reaches the ceiling."""

    def __init__(self, *, model: str, chars: int, estimate_low: float, ceiling: int):
        super().__init__(
            f"input cannot fit {model}: at least {estimate_low:,.0f} tokens "
            f"({chars:,} characters x {RATIO_MIN}) against a ceiling of {ceiling:,}. "
            "Nothing was sent.",
            model=model, chars=chars, estimate_low=estimate_low, ceiling=ceiling,
        )


class InputTruncated(InputFitError):
    """After the call: the runtime evaluated a full window, so it cut the input."""

    def __init__(self, *, model: str, count: int, ceiling: int, chars: int):
        super().__init__(
            f"input truncated by the runtime for {model}: prompt_eval_count "
            f"{count:,} reached the ceiling of {ceiling:,} ({chars:,} characters "
            "sent). done_reason cannot report this.",
            model=model, count=count, ceiling=ceiling, chars=chars,
        )


class InputDropped(InputFitError):
    """After the call: far fewer tokens were evaluated than the text could hold."""

    def __init__(self, *, model: str, count: int, chars: int, floor: float, ceiling: int):
        super().__init__(
            f"input partly dropped for {model}: prompt_eval_count {count:,} is below "
            f"the floor of {floor:,.0f} ({chars:,} characters x {RATIO_DROP}); "
            "part of the request never reached the model.",
            model=model, count=count, chars=chars, floor=floor, ceiling=ceiling,
        )


def clear_ceiling_cache() -> None:
    """Forget every cached trained-context read (tests; a model re-pulled mid-process)."""
    _CEILING_CACHE.clear()


def _client_host() -> str:
    base = getattr(getattr(_client, "_client", None), "base_url", None)
    return str(base).rstrip("/") if base is not None else ""


def n_ctx_train(model: str) -> int:
    """The model's trained context length, read once per model per server.

    Read through the module's `_client`, so a caller that rebinds the client to
    another server (run_qualgap01) reads that server's value.
    """
    key = (_client_host(), model)
    if key in _CEILING_CACHE:
        return _CEILING_CACHE[key]
    try:
        info = _client.show(model)
    except Exception as exc:
        raise CeilingUnavailable(model=model, reason=f"show failed: {exc}") from exc
    modelinfo = getattr(info, "modelinfo", None)
    if not isinstance(modelinfo, Mapping):
        raise CeilingUnavailable(model=model, reason="show returned no model_info")
    values = [
        v for k, v in modelinfo.items()
        if isinstance(k, str) and k.endswith(".context_length")
    ]
    if len(values) != 1 or isinstance(values[0], bool) or not isinstance(values[0], int) or values[0] <= 0:
        raise CeilingUnavailable(
            model=model, reason=f"expected one positive *.context_length, found {values!r}"
        )
    _CEILING_CACHE[key] = values[0]
    return values[0]


def server_context_length() -> int | None:
    """OLLAMA_CONTEXT_LENGTH as the local systemd service sets it, or None.

    Only for the local service: a client rebound to another server cannot learn
    that server's environment this way, so the term is omitted for it. The unit
    file is read first and drop-ins in lexical order after it, later values
    winning, which is systemd's own order. Only `*.conf` drop-ins count.
    """
    if _client_host() not in _LOCAL_SERVICE_HOSTS:
        return None
    files = [SERVICE_UNIT_FILE]
    try:
        files += sorted(SERVICE_DROPIN_DIR.glob("*.conf"))
    except OSError:
        pass
    value = None
    for path in files:
        try:
            text = path.read_text()
        except OSError:
            continue
        for line in text.splitlines():
            line = line.strip()
            if not line.startswith("Environment="):
                continue
            try:
                tokens = shlex.split(line[len("Environment="):])
            except ValueError:
                continue
            for token in tokens:
                name, sep, raw = token.partition("=")
                if sep and name == "OLLAMA_CONTEXT_LENGTH":
                    value = raw
    try:
        parsed = int(value) if value is not None else 0
    except ValueError:
        return None
    return parsed if parsed > 0 else None


def _option(options, name: str) -> int | None:
    if options is None:
        return None
    value = options.get(name) if isinstance(options, Mapping) else getattr(options, name, None)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return None
    return value


def effective_ceiling(model: str, options=None) -> int:
    """The token count at which the runtime starts cutting this call's input.

    An explicit options.num_ctx is what the runtime loads, clamped only by the
    model's trained context: min(n_ctx_train, num_ctx). Without one, the
    runtime's default applies: min(n_ctx_train, OLLAMA_CONTEXT_LENGTH if the
    local service sets it, SERVER_DEFAULT_CTX). INPUT-FIT-01 rulings R-D and R-3;
    reproduced against every load line in the journal on this host.
    """
    trained = n_ctx_train(model)
    num_ctx = _option(options, "num_ctx")
    if num_ctx is not None:
        return min(trained, num_ctx)
    terms = [trained, SERVER_DEFAULT_CTX]
    env = server_context_length()
    if env is not None:
        terms.append(env)
    return min(terms)


def message_chars(messages) -> int:
    """Characters of text content across the messages. Images are not counted."""
    total = 0
    for message in messages or ():
        content = message.get("content") if isinstance(message, Mapping) else getattr(message, "content", None)
        if isinstance(content, str):
            total += len(content)
    return total


def _check_input_fits(model: str, messages, options, paper_label: str) -> dict:
    """Pre-call: refuse only an input that certainly cannot fit."""
    ceiling = effective_ceiling(model, options)
    chars = message_chars(messages)
    estimate_low = chars * RATIO_MIN
    fit = {"model": model, "ceiling": ceiling, "chars": chars, "estimate_low": round(estimate_low)}
    if estimate_low >= ceiling:
        logger.error("input_fit REFUSED %s %s", paper_label, json.dumps(fit, sort_keys=True))
        raise InputOverflow(model=model, chars=chars, estimate_low=round(estimate_low, 1), ceiling=ceiling)
    return fit


def _check_input_was_read(response, fit: dict, paper_label: str):
    """Post-call: the count must be under the ceiling and above the drop floor."""
    count = getattr(response, "prompt_eval_count", None)
    if isinstance(count, bool) or not isinstance(count, int):
        count = None
    done_reason = getattr(response, "done_reason", None)
    chars, ceiling, model = fit["chars"], fit["ceiling"], fit["model"]
    telemetry = {
        **fit,
        "count": count,
        "ratio": round(count / chars, 4) if count is not None and chars else None,
        "done_reason": done_reason if isinstance(done_reason, str) else None,
    }
    logger.info("input_fit %s %s", paper_label, json.dumps(telemetry, sort_keys=True))
    if count is None:
        logger.warning(
            "input_fit UNVERIFIED %s: the response carries no prompt_eval_count, "
            "so truncation and dropping cannot be checked (model=%s)", paper_label, model,
        )
        return response
    if count >= ceiling:
        logger.error("input_fit TRUNCATED %s %s", paper_label, json.dumps(telemetry, sort_keys=True))
        raise InputTruncated(model=model, count=count, ceiling=ceiling, chars=chars)
    floor = chars * RATIO_DROP
    if count < floor:
        logger.error("input_fit DROPPED %s %s", paper_label, json.dumps(telemetry, sort_keys=True))
        raise InputDropped(model=model, count=count, chars=chars, floor=round(floor, 1), ceiling=ceiling)
    return response


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

    Input fit (INPUT-FIT-01)
    ------------------------
    The guard reads `messages` and `options` and changes nothing it sends.
    Before the call it resolves the effective ceiling (`effective_ceiling`) and
    refuses the input if even `chars x RATIO_MIN` reaches it. After the call it
    compares Ollama's `prompt_eval_count`: at or above the ceiling the runtime
    truncated the input; below `chars x RATIO_DROP` part of the request never
    reached the model. One structured `input_fit` line is logged per call.

    Only text content is counted. Image-bearing callers (the vision parser, the
    PDF quality check) are therefore under-estimated before the call and rely
    on the post-call check, which sees the image tokens in the count.

    Returns
    -------
    ollama response object

    Raises
    ------
    InputOverflow
        Before the call, if the input cannot fit. Nothing is sent.
    InputTruncated, InputDropped
        After the call, if the runtime cut or dropped part of the input.
    CeilingUnavailable
        Before the call, if the model's trained context cannot be read.
    TimeoutError
        If all attempts exceed the wall-clock timeout.
    Exception
        If all retries exhausted on a non-timeout error.
    """
    effective_timeout = wall_timeout or _wall_timeout_for_model(model)
    paper_label = f"paper_id={paper_id}" if paper_id is not None else "paper_id=unknown"
    fit = _check_input_fits(model, messages, kwargs.get("options"), paper_label)

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
            response = future.result(timeout=effective_timeout)

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
                    response = _restart_ollama_and_retry(
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
                return _check_input_was_read(response, fit, paper_label)

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

        else:
            # Outside the handlers above on purpose: an input-fit failure is
            # neither retried nor converted into a timeout.
            return _check_input_was_read(response, fit, paper_label)


# ── Ollama restart recovery (Layer 3) ────────────────────────────────


def _restart_ollama_and_retry(
    *, model, messages, paper_label, effective_timeout, max_retries, **kwargs,
):
    """Restart the Ollama service and attempt one final call.

    Returns the response on success; raises RuntimeError if recovery is refused
    or fails, which `ollama_chat` converts to TimeoutError.

    Two gates run before the restart, both of which make this branch refuse
    rather than proceed (OPSFIX-01):

    * **The experiment flock (OPS-GUARD-01).** This is the same predicate
      `extractor.restart_ollama` uses, and deliberately the narrower
      `foreign_lock_held()` rather than `check_experiment_lock()`: a process
      holding the lock for its own long run must still be able to recover, but
      restarting the service out from under *someone else's* multi-hour
      experiment destroys it. That is not hypothetical — an unguarded restart on
      this box killed the inference-determinism Arm P rerun.
    * **An explicit opt-out env var**, for harnesses pointed at a different
      server entirely.

    Refusing is always safe. The restart is a last-resort mitigation for a hung
    server, not a correctness requirement; the caller has already exhausted its
    retries and will surface a TimeoutError either way.
    """
    if restart_disabled():
        raise RuntimeError(
            f"Ollama restart refused: {RESTART_OPT_OUT_ENV} is set "
            f"(model={model}, {paper_label}). Recovery is disabled for this process."
        )
    if foreign_lock_held():
        raise RuntimeError(
            f"Ollama restart refused: another process holds the experiment lock "
            f"(model={model}, {paper_label}). Restarting would destroy its run; "
            f"see OPS-GUARD-01."
        )

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
