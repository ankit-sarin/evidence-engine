# 12aa-U20B — the pre-edit reads (R743(f))

Read at `df92bf1d13e16192966a17f4e876af3209e39b5b` (item 0), before any edit. Line numbers as found. No request to Ollama beyond version, tags and ps.

## 2a. The code

- **The constant:** line 152, `SERVER_DEFAULT_CTX = 262_144`.
- **How the terms combine:** `effective_ceiling` below. A request `num_ctx` returns `min(trained, num_ctx)` at once and reads neither other term. Otherwise the ceiling is `min` of the trained context, `SERVER_DEFAULT_CTX` and, when present, `server_context_length()`.
- **The cache:** line 160, `_CEILING_CACHE: dict[tuple[str, str], int] = {}`. Its key, line 253: `key = (digest, model) if digest else (_client_host(), model)`. It holds the trained context, one int per key.
- **Pre-call refusals:** `InputOverflow` at `_check_input_fits`, before the send, against `effective_ceiling(model, options)`. `CeilingUnavailable` from `n_ctx_train`, reached through the same call, before the send (`ollama_chat`, line 643: `fit = _check_input_fits(model, messages, kwargs.get("options"), paper_label)`).
- **Post-call refusals:** `InputTruncated` and `InputDropped` at `_check_input_was_read`, after the send, against the `ceiling` carried in `fit` from the pre-call check. `InputDropped` is a post-call refusal, not a pre-call one, and uses `chars * RATIO_DROP` as its floor; the ceiling only appears in its fields.
- **Raise sites, `grep`:** `CeilingUnavailable` at 259, 262, 268 (all in `n_ctx_train`); `InputOverflow` at 374; `InputTruncated` at 400; `InputDropped` at 404. No other module raises them.
- **`CeilingUnavailable` is a run fault:** `engine/core/extraction_events.py`:

```python

        import httpx

        from engine.core.effective_config import UndeclaredOverride
        from engine.core.run_manifest import RunRefused, UndeclaredCall
        from engine.elicitation.classes import CodebookContractError
        from engine.elicitation.materialize import SnippetNotExact
        from engine.utils.ollama_client import CeilingUnavailable
        _RUN_FAULT_CLASSES = (
            sqlite3.Error, CeilingUnavailable,
            ConnectionError, httpx.ConnectError, httpx.ConnectTimeout,
            UndeclaredCall, UndeclaredOverride, RunRefused,
            RunAborted, EventRefused, CodebookContractError, ExhaustedWithoutRecord,
            SnippetNotExact,
        )
    return _RUN_FAULT_CLASSES
```

- **CLAUDE.md:** no line states the ceiling's terms. `grep -n 'ceiling\|num_ctx\|n_ctx' CLAUDE.md` returns only line 333, "`seed` and `num_ctx` are recorded as `"unset"` / `modelfile_or_server`…". So commit 1 changes no CLAUDE.md line.

### `effective_ceiling`, `engine/utils/ollama_client.py`, lines 336–353

```python
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
```

### `n_ctx_train`, `engine/utils/ollama_client.py`, lines 243–272

```python
def n_ctx_train(model: str) -> int:
    """The model's trained context length, read once per model digest.

    Read through the module's `_client`, so a caller that rebinds the client to
    another server (run_qualgap01) reads that server's value. Under a run the
    cache is keyed by the digest the per-call check just read (U18 commit 2), so a
    tag replaced mid-process cannot keep its old ceiling; outside a run there is no
    digest and it is keyed by the client's host, as before.
    """
    digest = _CALL_DIGEST.get()
    key = (digest, model) if digest else (_client_host(), model)
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
```

### `_option`, `engine/utils/ollama_client.py`, lines 327–333

```python
def _option(options, name: str) -> int | None:
    if options is None:
        return None
    value = options.get(name) if isinstance(options, Mapping) else getattr(options, name, None)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return None
    return value
```

### `server_context_length`, `engine/utils/ollama_client.py`, lines 289–324

```python
def server_context_length() -> int | None:
    """OLLAMA_CONTEXT_LENGTH as the local systemd service sets it, or None.

    Only for the local service: a client rebound to another server cannot learn
    that server's environment this way, so the term is omitted for it. The unit
    file is read first and drop-ins in lexical order after it, later values
    winning, which is systemd's own order. Only `*.conf` drop-ins count.
    """
    if not is_local_service_host(_client_host()):
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
                env = parse_environment_line(line[len("Environment="):])
            except ValueError:
                continue
            if "OLLAMA_CONTEXT_LENGTH" in env:
                value = env["OLLAMA_CONTEXT_LENGTH"]
    try:
        parsed = int(value) if value is not None else 0
    except ValueError:
        return None
    return parsed if parsed > 0 else None
```

### `CeilingUnavailable`, `engine/utils/ollama_client.py`, lines 186–194

```python
class CeilingUnavailable(InputFitError):
    """The model's trained context could not be read, so no call is made."""

    def __init__(self, *, model: str, reason: str):
        super().__init__(
            f"cannot resolve the context ceiling for {model}: {reason}. The call "
            "was not sent; a guard that cannot read its ceiling must not guess one.",
            model=model, reason=reason,
        )
```

### `InputOverflow`, `engine/utils/ollama_client.py`, lines 197–206

```python
class InputOverflow(InputFitError):
    """Refused before the call: even the low estimate reaches the ceiling."""

    def __init__(self, *, model: str, chars: int, estimate_low: float, ceiling: int):
        super().__init__(
            f"input cannot fit {model}: at least {estimate_low:,.0f} tokens "
            f"({chars:,} characters x {RATIO_MIN}) against a ceiling of {ceiling:,}. "
            "Nothing was sent.",
            model=model, chars=chars, estimate_low=estimate_low, ceiling=ceiling,
        )
```

### `InputTruncated`, `engine/utils/ollama_client.py`, lines 209–218

```python
class InputTruncated(InputFitError):
    """After the call: the runtime evaluated a full window, so it cut the input."""

    def __init__(self, *, model: str, count: int, ceiling: int, chars: int):
        super().__init__(
            f"input truncated by the runtime for {model}: prompt_eval_count "
            f"{count:,} reached the ceiling of {ceiling:,} ({chars:,} characters "
            "sent). done_reason cannot report this.",
            model=model, count=count, ceiling=ceiling, chars=chars,
        )
```

### `InputDropped`, `engine/utils/ollama_client.py`, lines 221–230

```python
class InputDropped(InputFitError):
    """After the call: far fewer tokens were evaluated than the text could hold."""

    def __init__(self, *, model: str, count: int, chars: int, floor: float, ceiling: int):
        super().__init__(
            f"input partly dropped for {model}: prompt_eval_count {count:,} is below "
            f"the floor of {floor:,.0f} ({chars:,} characters x {RATIO_DROP}); "
            "part of the request never reached the model.",
            model=model, count=count, chars=chars, floor=floor, ceiling=ceiling,
        )
```

### `_check_input_fits`, `engine/utils/ollama_client.py`, lines 366–375

```python
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
```

### `_check_input_was_read`, `engine/utils/ollama_client.py`, lines 378–405

```python
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
```

## 2b. The ceiling at HEAD, from the committed show bodies

Instrument: `ceiling_at_head.py` (beside this file), a fake client returning each raw body from `12aa_U20P/show/` through the library's own `ollama.ShowResponse`. No network. `server_context_length()` reads this box's real unit files.

| Model | Trained | `SERVER_DEFAULT_CTX` | `server_context_length()` | Ceiling at HEAD, no request `num_ctx` | Differs from trained |
|---|---|---|---|---|---|
| `qwen3:8b` | 40,960 | 262,144 | None | 40,960 | no |
| `qwen3:32b` | 40,960 | 262,144 | None | 40,960 | no |
| `gemma3:27b` | 131,072 | 262,144 | None | 131,072 | no |
| `deepseek-r1:32b` | 131,072 | 262,144 | None | 131,072 | no |
| `qwen2.5vl:7b` | 128,000 | 262,144 | None | 128,000 | no |
| `llama3.2:3b` | 131,072 | 262,144 | None | 131,072 | no |

**No ceiling at HEAD differs from the trained context.** The finding R743(f) reserves for R743(d) does not arise on these models. Each is `min(trained, 262,144)`, and the service term is absent.

## 2d. R743(c) against the quoted code

No clause is contradicted. The `parameters` field is on the same `show` response `n_ctx_train` reads; the early return at the request `num_ctx` is what "a request `num_ctx` still wins" rests on; `CeilingUnavailable` is the existing refusal for an unreadable ceiling.
