# INPUT-FIT-01 Phase 1 — silent input truncation: what, which papers, where

**Date:** 2026-09-13. **Task type:** read-only read-out, plus two probe calls (P6). No fix, no
guard design, no change to any prompt, spec or client.
**Harness session:** `e6c94396-9455-4c17-be3d-1edf2c546b1b`, a continuation of
`91f0009c-5166-4bd2-a397-d74ee5fa8faa` (`continued-in` record 2026-09-13T04:36:51.717Z).
HEAD at start `a6543c8`, clean, level with origin.

**Tags.** **MEASURED** = computed here from a file, the journal or an API response. **READ** = a
line quoted from a committed document or the journal. **INFERRED** = a pairing, elimination or
cause. Pairings by timestamp or size are always INFERRED.

**Scope, as ruled.** Reads were limited to committed outputs, parsed-text files, engine source,
the dgx-infra probe document (`~/projects/dgx-infra/docs/ollama-truncation-probe.md`, committed
`995294d`) and the system journal. **The gitignored eval stores and run logs were not read.**
That limit is why P1 and P3 below rest on committed read-outs and elimination rather than on
per-call rows, and it is recorded as a finding under I4.

The review database was never opened. File before and after this task: 101,978,112 B, mtime
2026-09-11 02:00:52.636956943 (MEASURED, stat only).

---

## Ledger verdicts

| id | verdict | evidence |
|---|---|---|
| **I1** | **TRUE** (READ) | Probe doc §7 quotes `runner/llamarunner/runner.go` 125–155, identical at v0.14.1 / v0.15.5 / v0.17.7 / v0.21.0: `if !params.truncate { return nil, errorInputTooLong }` … `newInputs := inputs[:params.numKeep]` / `newInputs = append(newInputs, inputs[params.numKeep+discard:]...)` / `slog.Warn("truncating input prompt", …)`; ChatHandler `truncate := req.Truncate == nil \|\| *req.Truncate`; "Every call that truncated completed `200 /api/chat`". P6 reproduces the WARN and the 200 on this host. |
| **I2** | **TRUE** for the count (MEASURED); model split READ | `journalctl -u ollama -o short-iso --no-pager \| grep 'truncating input prompt'` → **26** lines before this task's probe (journal starts 2026-01-14T23:54:09; 654,295 lines); `exceeds the context length` → **0**. By limit: 131,072 ×19, 24,576 ×4 (`runner.go:187`), 4,096 ×3. The journal carries blob digests, not tags; "20 on deepseek-r1:32b" rests on the probe's digest→manifest resolution, not re-derived. |
| **I3** | **FALSE as stated** | Three Run 6-era events, not two: #9 2026-03-16 19:30:23 `prompt=441524` (bounded by `run6_20260316_185138.log`), and #10 2026-03-18 03:49:30 `prompt=132206` and #11 04:02:37 `prompt=132577`, which the probe grades only "consistent with evidence-engine Run 6. No artifact bounds the events." |
| **I4** | **FALSE as stated** | Per-call `prompt_eval_count` for SCHEMA-EVAL-02 and CAPTURE-01 exists only in gitignored stores (`data/surgical_autonomy/eval/schema_eval2/local_abc.jsonl`, `…/eval/capture01/capture01.jsonl`). The only committed per-call records are the 2f screening outputs. Not read, per scope. |
| **I5** | **Consistent; no contradiction** | `prompt=441524` is identical on 2026-03-16, 2026-07-29/30 (×3) and 2026-08-30 (MEASURED). The one commit touching `build_extraction_prompt` between those dates, `9190e41` (2026-07-29 15:59), changed only the lines at its boundary with `extract_pass1_reasoning`, not the prompt body (READ from `git log -L`). The codebook that feeds the prompt is gitignored; its history cannot be read. No template difference between the runs is established. |
| **I6** | **TRUE, both halves** | Client (MEASURED): ollama-python 0.6.1 `Client.chat(self, model, messages, *, tools, stream, think, logprobs, top_logprobs, format, options, keep_alive)` — no `truncate`, no `**kwargs`; `truncate` exists only on `embed()` / `EmbedRequest` (`_types.py:408`). Server (MEASURED, P6): `truncate:false` → HTTP 400. |
| **J1** | **TRUE except extractor Pass 2** | See R1. Pass 2 sends three messages and the document is not in the last one. |
| **J2** | **Supported by size and count elimination (INFERRED); not paired by timestamp** | See P3. |
| **J3** | **Partly TRUE** | p415 and p719 at the ceiling rest on measured `prompt_eval_count`; "only those two in the corpus" rests on measurement for 37 papers and an estimate for 153. See R3. |

---

## P1 — calls at or near the ceiling

**Effective ceilings.** Native context from `ollama show` (`model_info.*.context_length`,
MEASURED); no Modelfile sets `num_ctx`. The runtime clamps its default to the native value
(probe §4: `msg="requested context size too large for model" num_ctx=262144 n_ctx_train=131072`).

| model | native (ollama show) | effective where no caller sets num_ctx |
|---|---:|---:|
| deepseek-r1:32b | 131,072 | 131,072 |
| gemma3:27b | 131,072 | 131,072 (judge sets 8,192 / 24,576) |
| qwen3:8b | 40,960 | 40,960 |
| qwen3:32b | 40,960 | 40,960 |
| qwen2.5vl:7b | 128,000 | vision parser sets 8,192 |
| qwen3.5:27b | 262,144 | 262,144 |

**Committed per-call outputs (MEASURED).** The 2f screening outputs are the only committed files
with per-call `prompt_eval_count`: qwen3:8b 534 calls, max **3,906** of 40,960 (arm C primary,
paper 469); gemma3:27b 160 calls, max **3,880** of 131,072 (arm B verifier, paper 469). No call is
at or above 0.98 of its ceiling.

**Committed read-outs that state per-call ceiling hits (READ).**

| run / task | model | pass | paper | count | ceiling | source |
|---|---|---|---|---:|---:|---|
| CAPTURE-01 (2026-08-30) | deepseek-r1:32b | Pass 1 | 415 | 131,072 | 131,072 | `PARSE-01_report.md` §4(a); `CAPTURE-01_report.md` addendum |
| CAPTURE-01 (2026-08-30) | deepseek-r1:32b | Pass 1 | 719 | 131,072 | 131,072 | same |
| SCHEMA-EVAL-02 (2026-07-29/30) | deepseek-r1:32b | not stated | 415, 719 | 131,072 | 131,072 | `SCHEMA-EVAL-02_report.md` addendum: "in every local arm — this study's included" |
| ELICIT-01 | deepseek-r1:32b | Pass 1 | none | "0 rows" at 131,072 | 131,072 | `ELICIT-01_report.md` |
| ELICIT-DESIGN-01 smoke | deepseek-r1:32b | Pass 1 | none | max 46,855 | 131,072 | `ELICIT-DESIGN-01_report.md` |
| ELICIT-DESIGN-02 | deepseek-r1:32b | Pass 1 | 498 (attempt 2) | 48,763 | 131,072 | `ELICIT-DESIGN-02_report.md` |

Per-call rows behind the SCHEMA-EVAL-02 and CAPTURE-01 lines were not read (scope; I4).

## P2 — the largest parsed texts

All 446 files under `data/surgical_autonomy/parsed_text/` (101 v1, 341 v2, 4 v3), MEASURED.
Estimated tokens = characters × 0.2313 (PARSE-01's central ratio) and × 0.4288 (the elicitation
guard's worst-observed ratio).

| rank | paper | version | path | bytes | chars | est. @0.2313 | est. @0.4288 | FAIL set |
|---|---|---|---|---:|---:|---:|---:|---|
| 1 | 416 | v2 | `data/surgical_autonomy/parsed_text/416_v2.md` | 2,120,262 | 2,115,796 | 489,384 | 907,253 | no |
| 2 | 415 | v2 | `…/415_v2.md` | 1,776,058 | 1,771,635 | 409,779 | 759,677 | no |
| 3 | 225 | v1 | `…/225_v1.md` | 1,222,143 | 1,221,130 | 282,447 | 523,621 | no |
| 4 | 149 | v1 | `…/149_v1.md` | 758,736 | 754,469 | 174,509 | 323,516 | no |
| 5 | 37 | v1 | `…/37_v1.md` | 758,736 | 754,469 | 174,509 | 323,516 | no |

These five are the only files over 131,072 estimated tokens at either ratio. The FAIL-set files
are all far smaller (largest: `719_v2`, 279,426 B, 119,802 at 0.4288).

**Corpus membership.** PARSE-01's committed corpus distribution (n = 190) gives a maximum of
**1,771,635** characters, which is `415_v2`. Papers 416, 225, 149 and 37 are larger or equal-sized
duplicates and so are not among the 190 (INFERRED from that maximum). 415 and 719 are rows in
`PARSE-GATE-10_readout.md`'s corpus table.

**The largest file, `416_v2`, counts only (MEASURED):** 15,638 lines (9,187 non-empty, 7,916
distinct); 117 distinct non-empty lines repeat, accounting for 1,271 repeated occurrences beyond
the first; 925 Markdown headings, of which **86** are reference-list headings holding 248,222
characters (**11.7%** of the file); **0** supplement or appendix headings; 411 `<!-- image -->`
markers; 0 `GLYPH<` tokens.

## P3 — journal cross-walk

Timestamps come from the journal (MEASURED) and from the probe's artifact spans (READ, §6).
Per-call timestamps are in gitignored logs and were not read, so **no event is paired to a paper
by timestamp**. Where a paper is named, it is by elimination on size and on the committed
measurements (INFERRED).

| events | UTC | prompt → limit | run (probe tier) | paper | basis |
|---|---|---|---|---|---|
| #9 | 2026-03-16 19:30:23 | 441,524 → 131,072 | Run 6 (attributed) | **415**, by elimination | the only corpus text over 1M characters is 415 (PARSE-01 max 1,771,635); 441,524 ÷ 1,771,635 = 0.249 tokens/char |
| #10, #11 | 2026-03-18 03:49:30, 04:02:37 | 132,206 / 132,577 → 131,072 | consistent with Run 6 | **719**, by elimination (weaker) | the other corpus paper measured at the ceiling; the 153 estimated papers rest on the estimator below |
| #16–18 | 2026-07-29 23:53:12, 07-30 00:07:30, 00:22:47 | 441,524 ×3 | consistent with SCHEMA-EVAL-02 | **415**, by elimination | SCHEMA-EVAL-02's committed 40 ids include 415 and 719 and none of 416, 225, 149, 37 |
| #19–24 | 2026-07-30 11:00:05 → 11:57:46 | 132,206 ×3, 132,622 ×3, alternating | consistent with SCHEMA-EVAL-02 | **719**, by elimination | same 40 ids; 719 is the only other paper measured at the ceiling |
| #25 | 2026-08-30 05:42:44 | 441,524 | CAPTURE-01 (attributed) | **415**, by count + size | CAPTURE-01's committed addendum names exactly two ceiling papers, 415 and 719; the journal shows exactly two events inside `full.log`'s 05:29:18–07:59:21 span |
| #26 | 2026-08-30 07:33:15 | 132,206 | CAPTURE-01 (attributed) | **719**, by count + size | same |
| #4–8 | 2026-03-02 23:05:47 → 03-03 06:25:57 | 189,424 / 189,793 ×2 each, 278,247 | extraction run (attributed) | **unpaired** | predates Run 6; no committed per-paper data for that run |
| #12–15 | 2026-06-04 18:12:39 → 18:18:03 | 24,587–24,824 → 24,576 | Pass 2 judge (attributed) | **unpaired** | windowed judge prompts; no committed per-triple data read |
| #1–3 | 2026-01-19, 2026-01-30 | → 4,096 | llm-council | not evidence-engine | probe §6 |

**Tokenizer counts for the same paper, stated as facts only** (Sonnet from `PARSE-01_report.md`,
READ; local from the journal, pairing INFERRED as above):

| paper | Sonnet `input_tokens` | local `prompt` in the journal |
|---|---:|---:|
| 415 | 481,357 | 441,524 |
| 719 | 176,990 | 132,206 |

The ruling's example set 176,990 beside 441,524; those two figures belong to different papers
(719 and 415). Both correct pairs are shown above.

## P4 — the client path

**Wrapper** (`engine/utils/ollama_client.py`, READ). The library is ollama-python; the call is
`_client.chat` on `ollama.Client(timeout=_httpx_timeout)`, run in a one-thread executor for a
wall-clock watchdog:

```python
future = executor.submit(
    _client.chat,
    model=model,
    messages=messages,
    **kwargs,
)
return future.result(timeout=effective_timeout)
```

It forwards `model`, `messages` and whatever keyword arguments the caller passes (`format`,
`options`, `think` in practice) and returns the response object unchanged. **It captures
nothing and drops nothing**; `prompt_eval_count` and `done_reason` are dropped or kept by callers.

**Callers (READ):**

| caller | `done_reason` | `prompt_eval_count` |
|---|---|---|
| `engine/agents/extractor.py` Pass 1 / Pass 2 | kept as `finish_reason=getattr(response, "done_reason", None)` | dropped |
| `engine/elicitation/pipeline.py` Pass 1 | kept (`pass1_done_reason`) | kept (`pass1_prompt_eval_count`) and checked (R2) |
| `engine/parsers/pdf_parser.py` vision | logged; `"length"` raises | logged |
| FT screener, judge, auditor, abstract screener, PDF quality check | dropped | dropped |

**`num_ctx` set by a caller (READ):** `analysis/paper1/judge.py` Pass 1 `DEFAULT_NUM_CTX = 8192`,
Pass 2 parameter default `num_ctx: int = 24576`; `analysis/paper1/pass2_retry_single.py`
`DEFAULT_NUM_CTX = 24576`; `engine/parsers/pdf_parser.py` `_VISION_NUM_CTX = 8192` (overridable by
`spec.pdf_parsing.vision_num_ctx`); `scripts/eval_auditor_models.py` `"llama4:scout": {"num_ctx":
4096}`. The extractor, FT screener, auditor and elicitation pipeline set none.

**ollama-python** 0.6.1 (MEASURED): `chat()` carries no `truncate` and no `**kwargs`, so the
wrapper cannot forward one; a caller passing `truncate=` would reach a signature that does not
accept it (INFERRED from the signature; not executed).

## P5 — exposure map

Every path found that sends document-length text to a local model (READ from source; entry points
from `git grep` of the importing modules):

| path | model | num_ctx | messages | document in last message | existing guard | entry points (examples) |
|---|---|---|---|---|---|---|
| extractor Pass 1 `extract_pass1_reasoning` | deepseek-r1:32b | none → 131,072 | 2 (system, user) | yes | none | `scripts/run_pipeline.py`, `reextract_all.py`, `reextract_failed.py`, `run5_extract_and_audit.py`, `q8_validation*.py` |
| extractor Pass 2 `extract_pass2_structured` | deepseek-r1:32b | none → 131,072 | **3** (system, user = prompt + paper, user = prior analysis) | **no** | none | same |
| extractor snippet retry (`## Paper Text\n{paper_text}`) | deepseek-r1:32b | none → 131,072 | 2 | yes | none | same |
| eval runners `run_local_ab`, `run_local_abc`, `run_capture01`, `run_qualgap01` | deepseek-r1:32b | none | via the extractor's prompt | — | none | the runners themselves |
| `analysis/eval/elicit01/runner.py` | deepseek-r1:32b | none | — | — | pre-flight fit check | itself |
| elicitation pipeline Pass 1 / Pass 2 | deepseek-r1:32b | none | 2 (Pass 1) | yes (Pass 1) | `enforce_fit` both passes; tripwire Pass 1 only | extractor dispatch when `extraction_models.elicitation` is set; `analysis/eval/elicit_design01/smoke.py` |
| FT screener primary / verifier | qwen3:32b / gemma3:27b (spec) | none → 40,960 / 131,072 | 2 (system, user) | yes | `FT_MAX_TEXT_CHARS = 32_000  # ~8,000 tokens` cut before the call | `engine/agents/ft_screener.py` CLI, `scripts/ft_screening_smoke_test.py` |
| judge Pass 2 (windowed source) | gemma3:27b | 24,576 | 1 (user) | yes | windowing to ~20K tokens before the call | `analysis/paper1/pass2_full.py`, `pass2_smoke.py`, `judge_codebook_smoke.py`, `scripts/_pass2_stability.py` |
| `pass2_retry_single` | gemma3:27b | 24,576 | 1 | yes | windowing | itself |
| vision parser, per page image | qwen2.5vl:7b | 8,192 | 1 (user + image) | yes | `num_predict` cap; `done_reason == "length"` raises | `reparse_papers`, the parse cascade |

Adjacent, not document-length: judge Pass 1 (arm values; 8,192), the auditor (snippet only;
gemma3:27b), the abstract screener (title + abstract), the PDF quality check (first-page image;
qwen2.5vl:7b, no `num_ctx`). The cloud extractors send the full text but do not use Ollama.

## P6 — server probe

Two `/api/chat` calls to qwen3:8b, `options = {num_ctx: 4096, num_predict: 8, temperature: 0}`,
`think: false`, one user message with a 92,721-character synthetic prompt (no document content).
Started 2026-09-13T19:11:42Z. MEASURED responses; journal lines READ.

| call | HTTP | body / fields | journal |
|---|---:|---|---|
| A — `truncate: false` | **400** | `{"error": "the input length exceeds the context length"}` | `level=INFO source=server.go:1634 msg="llm predict error: the input length exceeds the context length"`; `400 \| 1.860067432s \| 127.0.0.1 \| POST "/api/chat"` |
| B — no `truncate` field | **200** | `prompt_eval_count` **4096**, `done_reason` `length`, `eval_count` 8 | `level=WARN source=runner.go:187 msg="truncating input prompt" limit=4096 prompt=20713 keep=4 new=4096`; `200 \| 1.705809984s \| … POST "/api/chat"` |

`ollama ps` before: `qwen3:8b 11 GB 100% GPU 40960 Forever`. After: **`qwen3:8b 6.0 GB 100% GPU
4096 Forever`** — the probe left qwen3:8b resident at a 4,096 context. The journal now carries 27
`truncating input prompt` lines and 1 `exceeds the context length` line; the additional one of
each is this probe's.

## P7 — full-text screening exposure

- **Dates.** No committed read-out records the dates of `ft_screening_decisions`. As a proxy only
  (not a decision date): the FT screener code first lands in `c21ad34` (2026-03-13) and
  `de7e6a5` (2026-03-14), both after the journal's retention start of 2026-01-14. Any FT
  screening run using that code therefore falls inside retention (INFERRED).
- **Events.** qwen3:32b (the spec's FT primary) appears in **no** event; nor does qwen3.5:27b
  (READ, probe §2–3 resolves all 26 to deepseek-r1:32b, gemma3:27b, qwen2.5:32b, mistral:7b).
  gemma3:27b's only events are at the judge's 24,576 limit.
- **Ceiling vs input** (INFERRED): the FT screener cuts text to 32,000 characters before the call;
  at the worst observed 0.4288 tokens/char that is about 13,700 tokens, inside qwen3:32b's 40,960.

## R1 — request structure

| path | messages (roles) | source |
|---|---|---|
| extractor Pass 1 | `system` ("You are a systematic review data extractor…"), `user` = `prompt` | `engine/agents/extractor.py` `extract_pass1_reasoning` |
| extractor Pass 2 | `system`, `user` = `prompt`, `user` = `"Here is your prior analysis of this paper:\n\n{reasoning_trace}\n\n…"` | `extract_pass2_structured` |
| extractor snippet retry | `system` ("Respond ONLY with JSON."), `user` = instructions + `## Paper Text\n{paper_text}` | `_retry_snippet` |
| FT screener primary / verifier | `render.messages(stage, prompt, …)` → `system`, `user` | `engine/agents/ft_screener.py`; `engine/core/eligibility_render.py` `messages()` |
| judge Pass 1 / Pass 2 | `user` only | `analysis/paper1/judge.py` |
| auditor | `system`, `user` (snippet, no paper text) | `engine/agents/auditor.py` |

**Does this make "≤2 papers" a bound?** For every path whose document sits in the final message —
all rows except extractor Pass 2 — the probe's reading of the message layer applies: "it cannot
drop the last message and always keeps system messages … So a prompt whose size is in the final
message reaches the runner whole and is token-cut there" (probe §7, READ). An overflow on those
paths therefore reaches the token layer and its WARN, and the retained WARNs are a bound for them
over the retention window, subject to the probe's own limits (retention, Debug-only message
truncation).

**It does not bind extractor Pass 2.** Its document is in the second of three messages, so the
probe's argument does not cover it, and a message-layer drop would log only at Debug. What the
journal does show, stated without explanation: no event larger than 441,524 exists in any cluster
(MEASURED), and the ~132k clusters contain companions 371 (132,577) and 416 (132,622) tokens above
132,206 (MEASURED) that are not paired to Pass 2 by any read evidence.

## R2 — the existing tripwire

`engine/elicitation/sizing.py` (READ, quoted in full where it decides):

```python
CEILING_TOKENS = 131_072          # n_ctx_train, the enforced clamp (PARSE-01 / M3)
WORST_RATIO = 0.4288              # worst observed tokens/char (CAPTURE-01 p719)
INDEX_MARKER_INFLATION = 1.141    # measured INDEX/COPY prompt_eval_count (ELICIT-01 5.6)

def estimate_tokens(prompt: str) -> int:
    """Conservative token projection for a marker-bearing prompt."""
    return int(len(prompt) * WORST_RATIO * INDEX_MARKER_INFLATION)

def enforce_fit(prompt: str, *, label: str, paper_id: int | None = None,
                ceiling: int = CEILING_TOKENS) -> int:
    """Hard-fail on projected overflow. Returns the estimate when it fits."""
    est = estimate_tokens(prompt)
    if est >= ceiling:
        raise PromptTooLargeError(
            label=label, paper_id=paper_id, chars=len(prompt),
            estimated_tokens=est, ceiling=ceiling,
        )
    return est

def truncation_tripwire(prompt_eval_count: int | None,
                        ceiling: int = CEILING_TOKENS) -> bool:
    """True when a completed call's input sat exactly at the ceiling.

    `>=` rather than `==` because a runtime that clamped differently would still
    be reporting a truncation, and the tripwire's job is to notice, not to
    adjudicate.
    """
    return prompt_eval_count is not None and prompt_eval_count >= ceiling
```

`engine/elicitation/pipeline.py` `run_pass1` (READ):

```python
    est = S.enforce_fit(prompt, label=PASS1_LABEL, paper_id=paper_id)
    response = ollama_chat(...)
    pec = getattr(response, "prompt_eval_count", None)
    ...
        "pass1_prompt_eval_count": pec,
        "pass1_truncation_tripwire": S.truncation_tripwire(pec),
    ...
    if telemetry["pass1_truncation_tripwire"]:
        logger.error(
            "TRIPWIRE paper %d: Pass-1 prompt_eval_count %s is at the enforced "
            "ceiling %d — the input was truncated and done_reason cannot say so.",
            paper_id, pec, S.CEILING_TOKENS,
        )
    return result, telemetry
```

- **Where the ceiling comes from:** a module constant, `131_072`, documented as deepseek-r1:32b's
  `n_ctx_train`. It is not read from `ollama show` or from the response, and it is specific to
  that model.
- **What it compares:** before the call, `len(prompt) × 0.4288 × 1.141` against the ceiling
  (hard fail); after the call, the response's `prompt_eval_count` against the ceiling (`>=`).
- **What it does on trip:** the pre-call check raises `PromptTooLargeError`; the post-call
  tripwire sets `pass1_truncation_tripwire` in telemetry and logs an ERROR, and **does not raise**.
- **Coverage:** Pass 2 calls `enforce_fit` (`pipeline.py:290`); there is no tripwire on Pass 2.

## R3 — PARSE-01's ceiling evidence

READ, `docs/session-reports/PARSE-01_report.md`:

> | local `deepseek-r1:32b`, 37 of the 190 | **MEASURED** | CAPTURE-01 `prompt_eval_count` |
> | local, the other 153 | **ESTIMATED** | 0.2313 tokens/char, the median over CAPTURE-01's 38 non-truncated rows, plus a measured 26,239-char prompt scaffold |

> ### (a) How many of the 190 saturated the local ceiling in Run 6? — **Exactly 2: p415 and p719.**
> … Both papers hit `prompt_eval_count` = 131,072 **exactly** in CAPTURE-01's measured telemetry.
> For the 153 papers with no measurement, both bounds agree on zero

The quantity is **`prompt_eval_count` from CAPTURE-01** for the 37 measured corpus papers (which
include 415 and 719), and a **character-count estimate** at 0.2313 and 0.4288 tokens/char for the
other 153. "Only p415 and p719" is therefore measured for 37 papers and estimated for 153.

---

## Found, not asked

1. **The worst-case ratio is derived from a capped count.** PARSE-01 gives p719's "true ratio
   **0.4289** tokens/char"; that equals 131,072 ÷ (279,389 text + 26,239 scaffold characters) =
   0.42886 (MEASURED arithmetic), i.e. the truncated count, not the full prompt. If `prompt=132206`
   is p719 (P3), the full ratio is about 0.4326. At 0.4288 alone p719 projects to about 131,053
   tokens — under the ceiling — while the journal shows it overflowed by 1,134 (INFERRED, on the
   pairing). The elicitation guard's extra ×1.141 does catch it; a guard using 0.4288 alone would not.
2. **The committed "a quarter never reached the model" statements for p719 do not match the
   journal**, if the pairing holds. QUALGAP-01, PRIME-01 and CAPTURE-01's addenda say roughly a
   quarter of p719 was lost locally and PARSE-01 says "~74% seen"; `132206 → 131072` cuts 1,134
   tokens (0.9%). p415's figure (131,072 of ~441,524 ≈ 30% seen) agrees with PARSE-01's ~31%.
3. **Papers 149 and 37 share one parsed text.** `149_v1.md` and `37_v1.md` are byte-identical
   (sha256 `a2db000f90759522192ceece75ee8a4d06d3028d4e05ae133dfd13bbead36e25`, MEASURED).
4. **The largest parsed text on disk, `416_v2` (2.1 MB), is not a corpus paper and never appears
   as a truncation event**; the 441k events match 415.
5. **The probe changed the resident model's context.** qwen3:8b is now loaded at 4,096 (6.0 GB);
   the next default-context qwen3:8b call will reload it.
6. **Two WARN / error lines in the journal are this task's**, from P6; a later count of
   `truncating input prompt` (27) or `exceeds the context length` (1) includes them.
7. **The project `CLAUDE.md` agent table names `qwen3.5:27b` as the FT primary; the spec at HEAD
   sets `ft_screening_models.primary: "qwen3:32b"`.** Not reconciled here.
8. **dgx-infra is one commit ahead of its upstream**; not touched (out of scope).

## Standard gate before this commit

2,259 passed / 17 deselected — five chunks via `find` (475 / 565 / 361 / 442 / 416; deselects
0 / 0 / 10 / 6 / 1). Review database unchanged across the run.
