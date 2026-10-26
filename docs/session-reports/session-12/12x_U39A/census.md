# 12x-U39A Q1 — census of hashed stages (read from code at a5892c9; engine tree equal at 88e28bb)

Method: an AST walk of `engine/` for every `ollama_chat(` call and its `messages=` argument
(output in `12x_U39A.md` §Q1), each read beside its branch of `render_messages`
(`engine/core/effective_config.py`, `def render_messages(stage, spec, *, codebook_path=None)`),
and every declaration of a stage to a manifest (`prompt_hash` has exactly two callers, both in
`resolve_run`, so a stage's hash is rendered only if some `open_run` declares it):
`scripts/run_pipeline.py` `_open_run_manifest` (`_PIPELINE_STAGE_CONFIGS`, with
`extraction_stages(spec)` substituted for the extract stages, plus `preflight`),
`engine/agents/ft_screener.py` `open_screening_manifest` (both FT stages, plus `preflight`),
and `engine/core/run_manifest.py` `open_run` (`cloud_arms` → `cloud:<arm>`, from
`scripts/run_cloud_extraction.py`).

| stage | scope | render (effective_config.py, anchor) | runtime call site | shared builder | variants and how the render combines them | declared by |
|---|---|---|---|---|---|---|
| abstract_screen_primary | LOCAL | `return build_messages(SENTINEL_PAPER, spec, role=role)` | screener.py:125 `screen_paper` | `build_messages` | one; role from the stage name | run_pipeline (screen) |
| abstract_screen_verifier | LOCAL | same branch, role "verifier" | screener.py:125 (same line) | `build_messages` | one | run_pipeline (screen) |
| ft_screen_primary | LOCAL | `return build_ft_messages(SENTINEL_TEXT, spec, which)` | ft_screener.py:216 `ft_screen_paper` | `build_ft_messages` | one | ft_screener |
| ft_screen_verifier | LOCAL | same branch, "verifier" | ft_screener.py:226 `ft_verify_paper` | `build_ft_messages` | one | ft_screener |
| extract_pass1 | LOCAL (non-elicited spec; the live spec) | `return ex.pass1_messages(prompt)` | extractor.py:236 `extract_pass1_reasoning` | `pass1_messages` + `build_extraction_prompt` | one | run_pipeline via `extraction_stages` |
| extract_pass2 | LOCAL, two render branches | elicited: `ex.pass2_messages(prompt, el.sentinel_priming_message(spec, codebook_path))`; else `ex.pass2_messages(prompt, "R")` | extractor.py:343 `extract_pass2_structured` | `pass2_messages`; elicited priming via `pass2_priming` | one per spec; the branch is chosen by `extraction_stages(spec)[0]` | run_pipeline |
| extract_retry_snippet | LOCAL (non-elicited only) | `return ex.retry_snippet_messages("f", "v", SENTINEL_TEXT)` | extractor.py:430 `_retry_snippet` | `retry_snippet_messages` | one | run_pipeline (non-elicited) |
| elicitation_pass1 | LOCAL (elicited spec; Run 7) | attempt 1 `el.pass1_messages(el.sentinel_pass1_prompt(...))` `+=` attempt 2 with `sentinel_feedback_block`, if `MAX_PASS1_ATTEMPTS > 1` | pipeline.py:295 `run_pass1`, called by `elicit` once or twice | `pass1_messages` + `pass1_prompt` | **2, concatenated in the render**; the runtime sends them as separate calls, attempt 2 only when attempt 1 left a field failing | run_pipeline (elicited) |
| audit | LOCAL | `build_audit_messages(span, field_type="text") + build_audit_messages(span, field_type="categorical")` | auditor.py:73 `semantic_verify` (caller audit_events.py:234, `field_type=types.get(c.field_name, "text")`) | `build_audit_messages` | **2, concatenated in the render**; the runtime sends one per claim, its `field_type` the codebook's `type` (see finding 3) | run_pipeline (audit) |
| vision_parse | LOCAL | `return vision_messages(SENTINEL_IMAGE)` | pdf_parser.py:279 `parse_with_vision` (per page) | `vision_messages` | one per page | run_pipeline (parse) |
| preflight | LOCAL (manifest, not in a pin) | `return [dict(m) for m in PREFLIGHT_MESSAGES]` | ollama_preflight.py:71 `check_model` `messages=[dict(m) for m in PREFLIGHT_MESSAGES]` | the constant only; **the expression is duplicated** | one; hashed once per probed model, the render ignores the model | run_pipeline, ft_screener |
| pdf_quality | NOT HASHED AT RUNTIME | `return classification_messages(SENTINEL_IMAGE)` | pdf_quality_check.py:105 `_classify_page` | `classification_messages` | one | **no manifest declares it** (finding 4) |
| cloud:openai_o4_mini_2025_04_16_high | CLOUD (F15, record only) | `outbound_messages(build_extraction_prompt(SENTINEL_TEXT, spec, codebook_path))` | openai_extractor.py:42 `"messages": outbound_messages(prompt)` | `outbound_messages` | one | run_cloud_extraction (`enabled_arms: []`, R71) |
| cloud:anthropic_claude_sonnet_4_6 | CLOUD (F15, record only) | same as openai | anthropic_extractor.py:42–46 `system, user = outbound_messages(prompt)` → `"system": system["content"], "messages": [user]` | `outbound_messages`, then **reshaped at runtime** into Anthropic's `system=` + one-message form | one | run_cloud_extraction |

Inputs the runtime needs to reproduce the hashed list exactly: the render's sentinels
(`SENTINEL_PAPER` = `{"title": "T", "abstract": "A", "id": 1}`, `SENTINEL_TEXT` =
`"Title: T\n\nAbstract: A"`, `SENTINEL_IMAGE` = `"<image>"`, the audit span f/v/s), the live
spec (the elicited stages through `model_copy(elicitation=True)`, as the seed does), the live
codebook path, and the hash-side sentinel helpers where the runtime's input is itself
engine-built (`sentinel_feedback_block` for attempt 2, `sentinel_priming_message` for the elicited
Pass 2). For vision, the page image's base64 is replaced by `SENTINEL_IMAGE` at the name
`parse_with_vision` looks `base64` up by, over a one-page PDF made in `tmp_path`.
