# PARSE-01 — Corpus-wide parse-quality and input-truncation audit

**Task:** PARSE-01. **Date:** 2026-08-30. **Scope:** all **190** EXTRACTED papers of the
`surgical_autonomy` corpus. Diagnosis and documentation only — **no model calls, no re-parsing,
no writes to `review.db`, no fixes.**

ELICIT-01's pre-flight found two parse-defective papers by accident while checking whether a
prompt would fit the local context window. This task looked for the rest deliberately.

---

## 1. Method and its limits

**Phase 1 sweep** (`analysis/eval/parse01/sweep.py`) reads the newest `parsed_text/{pid}_v*.md`
per paper and records size, line geometry, `## References` section count, `GLYPH<...>` artifact
count, image/formula comment counts, replacement characters, non-ASCII density, pysbd unit count
and unit length, and an estimated local prompt size. Sentence segmentation reuses
`analysis/provenance/segment.py` unmodified.

**Token accounting differs by arm, and the difference matters.**

| arm | method | basis |
|---|---|---|
| `anthropic_sonnet_4_6`, `openai_o4_mini_high` | **MEASURED** | `cloud_extractions.input_tokens`, provider-reported, all 379 rows |
| local `deepseek-r1:32b`, 37 of the 190 | **MEASURED** | CAPTURE-01 `prompt_eval_count` |
| local, the other 153 | **ESTIMATED** | 0.2313 tokens/char, the median over CAPTURE-01's 38 non-truncated rows, plus a measured 26,239-char prompt scaffold |

**Why the local arm has no telemetry.** Run 6 ran 2026-03-15/18; per-call telemetry
(`engine/core/extraction_telemetry.py`) arrives with INSTRUMENT-01 four months later.
`data/surgical_autonomy/telemetry/` does not exist, and `run6_extract.log` contains zero
occurrences of `prompt_eval` or `token`. The estimate is the only available instrument and is
labelled as such in every row.

**The estimator's known weakness, stated up front.** It underestimates exactly the papers most
likely to truncate. p719's true ratio is **0.4289** tokens/char against the 0.2313 median,
because glyph-corrupted text is token-dense; its central estimate (70,680) sits well under a
ceiling it actually hit. Question (a) is therefore answered with **two bounds**, central and
worst-observed, rather than one.

**Phase 2 thresholds** (`analysis/eval/parse01/flag.py`) are Tukey **far**-outlier fences,
Q3 + 3×IQR — stricter than the usual 1.5× because the question is "is this document broken", not
"is it unusual". The rule was fixed before looking at which papers it caught. One correction was
needed and is itself distribution-derived: several artifact counts are zero for most of the
corpus, so Q3 = IQR = 0 and the fence degenerates to "any non-zero value", flagging a paper with
one stray glyph. For any metric whose Q3 is 0 the fence is recomputed on the **non-zero
subpopulation**. That took the flag set from 33 to 18. Every flagged paper was then opened
against its source PDF; no classification rests on the heuristics alone.

## 2. Corpus distribution (n = 190)

| metric | min | p25 | median | p75 | p95 | max |
|---|---:|---:|---:|---:|---:|---:|
| chars | 7,987 | 34,468 | 43,154 | 57,533 | 84,708 | **1,771,635** |
| max line chars | 44 | 1,373 | 1,667 | 1,991 | 3,303 | 9,323 |
| `## References` sections | 0 | 1 | 1 | 1 | 1 | **75** |
| GLYPH artifacts | 0 | 0 | 0 | 0 | 41 | **5,472** |
| pysbd units | 67 | 356 | 451 | 634 | 1,103 | 20,893 |
| chars per unit | 7.1 | 84.3 | 94.1 | 107.2 | 119.4 | 1,447.6 |
| short-unit share % | 4.1 | 12.2 | 16.7 | 22.5 | 31.6 | 98.8 |
| local est. prompt tokens | 7,915 | 14,039 | 16,048 | 19,373 | 25,658 | 415,782 |

The corpus is tight and the defects are extreme outliers, not a tail.

## 3. Flagged papers — 18 of 190, all PDF-grounded

| severity | n | papers |
|---|---:|---|
| **SEVERE** | 4 | 415, 455, 586, 719 |
| MODERATE | 5 | 491, 562, 607, 644, 699 |
| MINOR | 2 | 262, 532 |
| NONE (legitimate) | 7 | 11, 14, 15, 459, 498, 514, 690 |

| paper | PDF | pages | parsed / pdftext | class | why |
|---|---|---:|---:|---|---|
| **415** | `EE-263_Kam_2019.pdf` | **728** | 1.04 | **MERGED_DOCUMENT** | *Medical Image Computing and Computer Assisted Intervention – MICCAI 2019, Proceedings Part V.* A whole proceedings volume acquired in place of one article. **The parse is faithful — the acquired document is wrong.** |
| **719** | `EE-567_Bauzano_2010.pdf` | 6 | **8.84** | **EXTRACTION_FAILURE** | Font-glyph encoded: 5,472 `GLYPH<c=3,font=/JGFKKL+TimesNewRoman>` tokens. Prose unrecoverable. |
| **455** | `EE-303_Bauzano_2013.pdf` | 12 | **1.00** | **EXTRACTION_FAILURE** | Character-shattered — `c\n o\n m\n p u\n t e\n r`. 99% of units under 3 tokens, 7.1 chars/unit. |
| **586** | `EE-434_Varghese_2017.pdf` | 8 | 1.58 | **EXTRACTION_FAILURE** | 542 glyph artifacts; headings glyph-encoded, body partly intact. |
| 699 | `EE-547_Han_2024.pdf` | 6 | 1.62 | EXTRACTION_FAILURE | 419 glyph artifacts, body prose reads normally — localised. |
| 644 | `EE-492_Wang_2024.pdf` | 7 | 1.20 | EXTRACTION_FAILURE | 16 glyph artifacts, 35% short lines. |
| 491 | `EE-339_Zhu_2022.pdf` | 2 | 1.05 | EXTRACTION_FAILURE | 22 glyph artifacts in a 2-page paper. |
| 562 | `EE-410_Nagy_2018.pdf` | 9 | 0.99 | LAYOUT_SHATTER | 76% short lines; prose readable, line structure destroyed. |
| 607 | `EE-455_Urrea_2025.pdf` | 32 | 1.45 | LAYOUT_SHATTER | 23% short lines; prose readable. |
| 262, 532 | — | 10, 9 | 1.04, 1.06 | OTHER (MINOR) | 6 and 9 `U+FFFD` replacement characters in otherwise clean prose. |
| 11, 14, 15, 459, 498, 514, 690 | — | 6–34 | 0.90–1.71 | LEGITIMATE_LONG | Long, image- or formula-heavy, clean prose. Flagged on size alone. |

**One methodological finding worth carrying forward.** **p455's character ratio is 1.00** — it
contains exactly the right number of characters. Every size-, ratio- or truncation-based check is
blind to it. Only the segmentation metric (chars-per-unit 7.1, short-unit share 99%) exposed it.
A future parse-quality gate that checks only length or token counts would pass this document.

## 4. The three questions

### (a) How many of the 190 saturated the local ceiling in Run 6? — **Exactly 2: p415 and p719.**

The ceiling is **131,072** tokens, and it is a property of the model rather than the runtime:
QUALGAP-01 §1.2 independently records that both Ollama 0.17.7 and 0.21.0 derive
`default_num_ctx=262144`, "clamped identically against `n_ctx_train=131072`". Both papers hit
`prompt_eval_count` = 131,072 **exactly** in CAPTURE-01's measured telemetry.

For the 153 papers with no measurement, both bounds agree on zero:

| ratio | over 131,072 |
|---|---|
| 0.2313 tokens/char (central) | **0** |
| 0.4288 tokens/char (worst observed, token-dense) | **0** |

The largest untested paper (p11, 156,196 chars) reaches 78,228 tokens even at worst-case density
— 40% of the ceiling. The answer is robust to the estimator's weakness.

### (b) What the Sonnet arm received and did — p415 first

**p415 is the only paper in the corpus that exceeds any cloud window.** The Sonnet row records:

| field | value |
|---|---|
| `model_string` | **`claude-sonnet-4-6`** |
| `input_tokens` | **481,357** |
| `output_tokens` | 4,785 |
| `reasoning_tokens` | 0 |
| `cost_usd` | **1.515846** |
| `extracted_at` | 2026-03-16T19:59:50Z |

Sonnet **did not truncate**: it accepted the full 1,797,874-character prompt and returned 20
fields. The billing is consistent with the recorded token count at standard Sonnet rates —
481,357 × $3/1M + 4,785 × $15/1M = $1.5158 against the recorded $1.515846.

**What the row does not say.** 481,357 tokens exceeds a 200,000-token window, so some
long-context configuration must have served this request, but `cloud_extractions` stores no
context-window field, no API version and no request headers — only `model_string`, which is
`claude-sonnet-4-6` for **all 190 rows** with no variant distinguishing this one. **The stored
record therefore cannot identify which long-context variant served it.** Establishing that would
require provider-side logs, which are outside this task.

**The consequence for concordance.** On p415 the three arms saw three different documents:
**local ~31%** (131,072 of ~416,000 tokens), **o4-mini nothing** (the single missing extraction
in that arm, excluded for context length), **Sonnet 100%**. On p719: local truncated at 131,072
(~74% seen), while Sonnet (176,990 tokens) and o4-mini (119,898) both received it whole. No
other paper in the corpus differs across arms this way — the next largest Sonnet input is
176,990 and the o4-mini maximum is 119,898, both inside their windows.

### (c) Is any paper in ELICIT-01's intended n=38 flagged? — **One, and it is clean.**

**p498**, flagged on `chars` alone (148,805 chars, 33 pages, ratio 1.06, prose clean) →
`LEGITIMATE_LONG`. **ELICIT-01's n=38 set carries no parse defect.**

## 5. Sample composition — the `select_sample()` filter gap

Three of the 40 papers in the canonical `SEED=20260729` sample — **547, 629, 799** — are
`FT_SCREENED_OUT` with **zero** extractions. They are not members of the 190-paper corpus.

The cause is a filter gap in `analysis/eval/schema_eval2.select_sample()`. The function builds
its 40 from two paths, and applies the eligibility filter to only one:

```python
picked = [make(p, "carried") for p in CARRIED if p in sizes]          # no eligibility filter
pool   = sorted(p for p in sizes if p not in set(CARRIED) and p in eligible)   # filtered
```

`eligible` is `status IN ('FT_ELIGIBLE','EXTRACTED','AI_AUDIT_COMPLETE','HUMAN_AUDIT_COMPLETE')`.
The 30 pool draws honour it; the 10 `CARRIED` papers are filtered only on having parsed text.
`CARRIED = (39, 386, 466, 498, 547, 629, 691, 694, 708, 799)` — and 547, 629 and 799 are
precisely the three `FT_SCREENED_OUT` members. Every affected paper entered through the carried
path; **not one entered through the filtered pool.**

By contrast **415 and 719 are `AI_AUDIT_COMPLETE`** and were drawn legitimately from the pool.
Their problem is the document and its parse, not their eligibility.

Recorded, not fixed — no change was made to `select_sample()`.

## 6. Blast radius per study

| study | analysis set | exposure |
|---|---|---|
| CAPTURE-01 | 40 | 415 + 719 truncated; corrected pooled 27.4% (n=38), paired −14.1 pp, rho +0.020; conclusions unchanged |
| QUALGAP-01 | 36 matched | 719 in set, truncated **identically in both cells** — verdict unaffected; 415/547/629/799 already excluded by §2 |
| PRIME-01 | 36 matched | 719 in set, truncated in Run 6 too; 415 already excluded |
| SCHEMA-EVAL-02 | 40 | contains 415/719 (deliberately, as collapse cases) and 547/629/799 (non-members); no figure re-scored |
| Census reports (×5) | 11,017 spans | 4 severe papers contribute **364 / 22,034** rows = **1.65%**; 415 and 719 carry **1** local span each |

## 7. Findings for the fix-phase queue

Recorded here so the fix phase does not have to rediscover them. **Nothing below is
implemented by this task**, and none of it is a corpus decision.

### F1 — `select_sample()` bypasses the eligibility filter on the carried path

The sample builder assembles its 40 from two paths and filters only one:

```python
picked = [make(p, "carried") for p in CARRIED if p in sizes]                  # NOT filtered
pool   = sorted(p for p in sizes if p not in set(CARRIED) and p in eligible)  # filtered
```

`eligible` is `status IN ('FT_ELIGIBLE','EXTRACTED','AI_AUDIT_COMPLETE','HUMAN_AUDIT_COMPLETE')`.
The 30 pool draws honour it; the 10 `CARRIED` papers are filtered only on **having parsed text**.
`CARRIED = (39, 386, 466, 498, 547, 629, 691, 694, 708, 799)`, and 547, 629 and 799 are exactly
the three `FT_SCREENED_OUT` non-members in the sample. Every affected paper entered through the
carried path; **not one entered through the filtered pool.**

The consequence is silent: a carried paper that later leaves the corpus stays in the sample
forever, and nothing in the draw reports it. Any fix should also decide what happens to a
carried paper whose status changes *after* it was carried — the current code cannot express that
question.

### F2 — the effective context ceiling is `clamp(num_ctx, n_ctx_train)`, not `num_ctx`

Design constraint for the input-fit guard. QUALGAP-01 §1.2 records both Ollama 0.17.7 and 0.21.0
deriving `default_num_ctx=262144` and clamping it "identically against `n_ctx_train=131072`".
The number that governs truncation is therefore **the minimum of the configured context and the
model's trained context** — a property of the model, not of the runtime or the environment.

Three implications for a guard:

1. **Reading the configured value alone is wrong by a factor of two here.** A guard that trusted
   `default_num_ctx=262144`, or `OLLAMA_CONTEXT_LENGTH` (which is `0` on this host), would have
   passed both p415 and p719 as fitting.
2. **`done_reason` cannot detect input truncation.** Both truncated papers returned
   `done_reason=stop`. The pre-flight signal is *estimated prompt tokens vs the clamped ceiling*;
   the post-hoc signature is `prompt_eval_count` landing **exactly** on the ceiling.
3. **A length-only gate is insufficient for parse quality**, which is a different question the
   same guard will be tempted to answer. p455 (§3) has a character ratio of 1.00 and is
   catastrophically malformed; it fits the context window comfortably.

### F3 — `cloud_extractions` records no request configuration

The table stores `model_string`, token counts, cost and the prompt text, but **no context-window
setting, API version, request headers or beta flags**, and `model_string` is the identical
`claude-sonnet-4-6` across all 190 Sonnet rows. The concrete cost: the p415 row records 481,357
input tokens accepted and billed — a figure that requires some long-context configuration — and
**the stored record cannot say which** (§4b). Cost auditing and reproducibility both depend on
information that is currently not captured anywhere.

## 8. What this audit does not establish

- **It does not diagnose the MODERATE and LAYOUT_SHATTER papers' impact on any figure.** They
  were classified from the parsed text and PDF; no per-span or per-field consequence was traced.
- **It does not verify the other 172 papers are sound** — only that they are not distributional
  outliers on the measured metrics. p455 is the proof that a defect can hide inside a normal
  size profile; a different defect could hide inside a normal segmentation profile.
- **The local token figures for 153 papers are estimates, not measurements.** Question (a)'s
  answer is robust under both bounds, but no individual estimate should be quoted as a count.
- **It does not identify which Sonnet long-context configuration served p415** — §4(b). The
  stored record cannot answer it.
- **It makes no corpus decision.** Whether p415 leaves the corpus, and whether the failures are
  re-parsed, is a PI ruling; nothing here implements or pre-states one.
- **No re-parsing was attempted**, so the recoverability of 719, 455 and 586 under a vision
  fallback is untested and unknown.

## 9. Acceptance gates

| gate | status |
|---|---|
| 1. I1–I3 verified before conclusions rest on them | ✅ §1 — I1 **false** (no Run 6 telemetry; authorized estimation fallback used and labelled); I2 true; I3 true |
| 2. Phase 1 table complete 190/190, method per arm recorded | ✅ §1–2, `parse01/sweep.jsonl` |
| 3. Every flagged paper classified against its source PDF | ✅ §3 — all 18 opened; `parse01/classified.json` |
| 4. Stop-and-report between Phase 2 and Phase 3 honored | ✅ addendum scope signed off before any addendum was written |
| 5. Addenda strictly append-only | ✅ 9 files, prefix byte-identical to pre-edit snapshots, **zero** deletions |
| 6. Report committed and pushed; `review.db` and `parsed_text` untouched | ✅ §10 |

## 10. Invariants

`review.db` mtime **2026-07-27T19:47** and the newest `parsed_text` file **2026-03-14T03:34**,
both predating this session — neither was written. No model calls, no Ollama interaction, no
service touched. New artifacts are confined to `data/surgical_autonomy/eval/parse01/`
(gitignored) and `analysis/eval/parse01/` (committed for provenance).

**Out of scope and not done:** re-parsing, corpus membership decisions, the `select_sample()`
fix, the input-fit guard, ELICIT-01, any model call.
