# 12y-U18B 2c — planned rewrites of the 17 patches (R722(b)); nothing edited

Engine side assumed (3a/3b plan): `fetch_model_digest` reads `_client._client.get("/api/tags")` (the
keep-alive httpx client inside the engine's `ollama.Client`) at call time; the preflight calls
`oc._client.ps()` at call time through `from engine.utils import ollama_client as oc`.

| # | file:line | patched name (before) | replacement (after) | assertion kept |
|---|---|---|---|---|
| 1 | test_ollama_client.py:348 | `patch("engine.utils.ollama_client.httpx.get", return_value=_tags_response(models))` inside `@patch("engine.utils.ollama_client._client")` | the existing `mock_client`: `mock_client._client.get.return_value = _tags_response(models)` | digest == "c"*64; the requested path ends "/api/tags" (now `mock_client._client.get.call_args.args[0]`); `mock_client.show.assert_not_called()` |
| 2 | :358 | `httpx.get` side_effect `httpx.ConnectError("connection refused")` | `patch("engine.utils.ollama_client._client")`, `._client.get.side_effect = httpx.ConnectError(...)` (the real transport's error vocabulary, kept) | raises `ModelDigestError` |
| 3 | :393 | `httpx.get` return two models | engine-client fake as in 1 | digest == VALID_DIGEST; 64 lowercase hex; called path ends "/api/tags" |
| 4 | :408 | `httpx.get` malformed (32 chars) | as 1 | `ModelDigestError` match "malformed digest" |
| 5 | :418 | `httpx.get` uppercase | as 1 | `ModelDigestError` match "malformed digest" |
| 6 | :426 | `httpx.get` missing model | as 1 | `ModelDigestError` match "no entry for model_name" |
| 7 | :438 | `httpx.get` (as found; read before edit) | as 1 | as found (read and quoted before edit) |
| 8 | :445 | `httpx.get` return_value=resp | as 1 | as found |
| 9 | test_ollama_preflight.py:54 | `patch("engine.utils.ollama_preflight.ollama.ps", ...)` | `patch.object(oc._client, "ps", ...)` (or `patch("engine.utils.ollama_client._client.ps", ...)`), same return value | each VRAM / status assertion as found |
| 10 | :73 | `ollama.ps` return_value=ps_data | as 9 | as found |
| 11 | :93 | as 10 | as 9 | as found |
| 12 | :114 | `ollama.ps` | as 9 | as found |
| 13 | :226 | `ollama.ps` | as 9 | as found |
| 14 | :236 | `ollama.ps` | as 9 | as found |
| 15 | :347 | `ollama.ps` | as 9 | as found |
| 16 | test_run_pipeline_starts.py:166 | `patch("engine.utils.ollama_preflight.ollama.ps", return_value={"models": []})` | `ps` on the test's fake client (the fixture binds `_client` to a fake; give it `ps` returning `{"models": []}`), or `patch("engine.utils.ollama_client._client.ps", ...)` after the fixture binds | the run starts; the docstring's "(`systemctl show`, `ollama.ps`)" sentence updated to the engine client |
| 17 | test_undeclared_call.py:177 | as 16 | as 16 | as found |

Rows 7, 8 and 9–17's assertions are to be quoted verbatim from the file at 2c proper; none is dropped.
A `ProcessResponse` from `Client.ps()` is the same type `ollama.ps()` returns (both are `Client.ps`), so
fakes returning a dict keep working where the code calls `.get("models", [])`.
