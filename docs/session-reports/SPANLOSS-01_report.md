# SPANLOSS-01 — Root cause of the single-span extraction collapse

Project: evidence-engine · Machine: DGX (spark-59e4) · Date: 2026-07-28
Commit **`db93ce6`** · repo clean at start (`97179b4`).

**Read-only confirmed. Zero Ollama calls, zero cloud API calls, zero repairs, no schema
changes, no re-extraction.** Full offline suite: **1417 passed, 15 deselected**.

---

## 1. Affected set

Threshold: fewer than 20 stored spans (the prompt demands exactly 20 —
`engine/agents/extractor.py:177`).

| arm | papers | spans stored | span deficit |
|---|---:|---|---:|
| **openai_o4_mini_high** | **17** | 1 each | **323** |
| local_deepseek_r1_32b | 2 | 1 each | 38 |
| local_deepseek_r1_32b | 2 | 19 each | 2 |
| **anthropic_sonnet_4_6** | **0** | 20/20 on all 190 papers | **0** |

openai: 277, 386, 457, 460, 466, 477, 489, 498, 514, 519, 526, 553, 610, 614, 683, 694, 699.
local single-span: 415, 719. local 19-span: 121 (missing `country`), 458 (missing `task_select`).

**The surviving field is diagnostic, and it differs by arm:**

| arm | surviving field | pattern |
|---|---|---|
| openai (17/17) | **`study_type`** — always | the **first** field in the schema, with a real per-paper snippet, `value` = "Original Research" (17 chars) in all 17 |
| local (415) | `Title` | not a codebook field |
| local (719) | `field_1` | not a codebook field; value is a chat preamble ("Here's a structured summary of the key points:") |

A separate gap worth recording: **paper 415 has no openai extraction row at all** (openai
covers 189 of 190 papers), so openai's true deficit against the full corpus is 343 spans, of
which 323 are the collapse and 20 are the absent paper.

---

## 2. The fork: extraction defect vs. storage defect

**Verdict: EXTRACTION defect on all 21. Zero storage loss.**

The fork is decidable from the database because both extractors persist **the parsed model
response verbatim**:

- `engine/cloud/openai_extractor.py:100` — `extracted_data = json.loads(content)`
- `engine/cloud/base.py:226` — `json.dumps(extracted_data)` on write

So `extracted_data` *is* the model's response. If it holds one span, the model sent one span.

### openai — raw evidence, quoted

`cloud_extractions.extracted_data` for paper 277, in full:

```json
{"field_name": "study_type", "value": "Original Research", "source_snippet": "We introduced an open-source surgical embodied intelligence simulator for an interactive environment to develop reinforcement learning methods for minimally invasive surgical robots.", "confidence": 0.9, "tier": 1}
```

Paper 477: `{"field_name": "study_type", "value": "Original Research", "source_snippet": "We describe the approach and physical experiments with repeatability of 96% for 50 trials of the 3d-DVTP subtask and 70% for 20 trials of the 2d-PCOTP subtask.", "confidence": 1.0, "tier": 1}`

Against a healthy response (paper 9): `{"fields": [{"field_name": "study_type", …}, {"field_name": "robot_platform", …}, … 20 objects …]}` — and paper 11 uses the `{"extractions": [...]}` variant.

The affected responses are **bare, unwrapped, syntactically complete single-span objects**.
They are not truncated: a truncation would not survive `json.loads`, and would have landed in
the `except` branch as `{"fields": [], "raw": content}` (`openai_extractor.py:101-103`), which
appears nowhere in the affected set.

### local — different shape, same symptom

Paper 415 stored `[{"field_name": "Title", "value": "Enhancing Left Ventricle Segmentation in
Echocardiograms…", …}]` and paper 719 stored `[{"field_name": "field_1", "value": "The paper
presents a dynamic potential field method… Here's a structured summary of the key points:", …}]`
— **one-element arrays with non-codebook field names**. Papers 121 and 458 stored full 19-element
arrays. All faithfully persisted.

**These are two different mechanisms wearing the same symptom** (§4).

### One anomaly I could not close, stated rather than smoothed

OpenAI's token accounting is inconsistent with the stored content on 7 of the 17 papers. Visible
tokens (`completion_tokens − reasoning_tokens`) vs. stored JSON length:

| consistent (stored JSON ≈ visible output) | inconsistent (visible ≫ stored) |
|---|---|
| 460 (74 tok / 288 ch), 519 (60/227), 683 (69/216), 699 (64/221), 614 (144/371), 466 (116/318), 277 (243/292), 477 (198/269) | **386 (1994 tok / 167 ch)**, 498 (1411/238), 526 (1768/405), 610 (1341/277), 489 (798/372), 457 (751/250), 553 (635/224) |

For the second group the API reports 600–2,000 visible tokens while ~45–100 tokens' worth of
JSON was stored. Since `json.loads` is strict — trailing data raises, and any failure would have
produced a `raw` fallback that is absent — the stored object cannot be a prefix of a larger
document. The most consistent explanation is that o4-mini's `completion_tokens` accounting
includes output from internal attempts that were never returned, but **I cannot prove that from
what was persisted**. What would settle it: capturing `response.choices[0].finish_reason` and
the raw `content` string (pre-`json.loads`) alongside every extraction. Neither is recorded
anywhere today.

---

## 3. Mechanism and code locus

End-to-end trace for paper 277:

1. **Request** — `openai_extractor.py:64-79`: `o4-mini-2025-04-16`,
   `reasoning_effort="high"`, `response_format={"type": "json_object"}`. Note there is **no
   JSON *schema*** — `json_object` mode constrains syntax only, not shape, so a bare single-span
   object is a fully conforming response.
2. **Receive** — `:83` `content = response.choices[0].message.content`. HTTP 200; the Run 6 log
   (`data/surgical_autonomy/logs/cloud_o4mini_20260316_031717.log`) shows a clean
   `POST … "HTTP/1.1 200 OK"` at 03:46:13 with **no warning of any kind**. `finish_reason` is
   never read.
3. **Parse** — `:100` `json.loads(content)` succeeds → a dict with `field_name` at top level.
4. **Salvage** — `engine/cloud/base.py:130-132`:
   ```python
   # Single span dict (has field_name key) — wrap in list
   if "field_name" in response_json:
       response_json = {"fields": [response_json]}
   ```
   **This is the locus.** The branch exists to rescue a model that forgot the wrapper, and it
   does exactly that — converting a shape error into one valid span. It is not itself a bug; the
   bug is that nothing downstream notices that one span is not twenty.
5. **Validate** — `base.py:171` `ExtractionOutput.model_validate` passes (one well-formed span).
   No warning logged, consistent with the log.
6. **Write** — `base.py:206-212`:
   ```python
   if not spans:
       raise ValueError(f"Paper {paper_id} ({arm}): extraction produced 0 spans — refusing to store…")
   ```
   **`store_result` rejects 0 spans and accepts 1.** There is no cardinality check against the
   20 fields the prompt demands, no comparison against `spec.extraction_schema`, and no warning
   at any count between 1 and 19. The extraction is committed and the run continues.

**Locus, stated precisely: `engine/cloud/base.py:206-212` — the guard is a
non-empty check where it needed to be a completeness check.** The salvage branch at
`base.py:130-132` is what makes the failure *silent* rather than loud, and
`openai_extractor.py:83` discarding `finish_reason` is what makes it *undiagnosable* after
the fact.

### Candidates checked and ruled out

| candidate | verdict |
|---|---|
| JSON parse salvage misbehaving | **Ruled out** — salvage worked correctly on what it received; the defect is upstream of it. It does, however, convert a loud failure into a silent one. |
| `INVALID_SNIPPET_RE` retry (`extractor.py:326-360`) | **Ruled out for openai** — local-only code path, and the openai arm never executes it. Also ruled out for local 415/719: their snippets contain no ellipsis, so the retry never fired. |
| Response truncation / stop tokens | **Ruled out as the storage story** — every affected response is syntactically complete; a truncated one would have failed `json.loads`. Cannot be fully ruled out as the *model-side* story for the 7 token-anomaly papers (see §2). |
| Atomic-write rollback | **Ruled out** — `store_result` writes the extraction row and its spans in one transaction (`base.py:216-240`); a rollback would leave no `cloud_extractions` row at all, and all 17 rows are present. |
| Pydantic validation dropping fields | **Ruled out** — the log's 39 `Failed to validate response against ExtractionOutput` warnings (all `fields.3.value`, `sample_size` returned as an int) occur on **healthy** papers, and that path `return []`s, which `store_result` would then reject. No affected paper has such a warning. |

---

## 4. Clustering

**No clustering. This is a stochastic per-call output-format failure at ~9% (17/189).**

| observable | affected (n=17) | healthy (n=172) |
|---|---:|---:|
| median paper chars | 46,178 | 42,669 |
| median input tokens | 15,920 | 15,230 |
| median reasoning tokens | 9,472 | **10,272** |
| median visible tokens | 243 | 1,582 |
| parser tier | docling 17/17 | docling 167, pymupdf 5 |
| median cost | $0.0671 | $0.0690 |

- **Not a bad batch**: run positions 20, 39, 60, 63, 68, 76, 83, 88, 95, 99, 101, 115, 136, 137,
  161, 167, 168 of 189 — spread from 11% to 89% through the run, with gaps of 1–24.
- **No time-of-day trend**: hourly incidence 03:00 1/30, 04:00 4/45, 05:00 7/44, 06:00 3/43,
  07:00 2/27.
- **Not a length effect**: affected papers are marginally *longer* (46.2k vs 42.7k chars) but
  well within the healthy distribution.
- **Not a parse-tier effect**: all 17 are docling; the 5 pymupdf papers are all healthy.
- **Not a reasoning-exhaustion effect**: affected papers used *fewer* reasoning tokens than
  healthy ones on median, which argues against "ran out of budget before emitting fields".

**The 2 local papers are a coincidence of symptom, not the same mechanism.** Different arm,
different model, different run (415 at 2026-03-16T19:43, 719 at 2026-03-18T04:11 — the local
run, not the 03-16 cloud run), different response shape (one-element array vs. bare dict),
and different surviving content (junk field names and a chat preamble vs. a correct first-field
extraction). The local failures look like the model answering a different question — summarizing
the paper — while the openai failures look like the model answering the right question and
emitting only the first field.

---

## 5. Blast radius

### Paper 1 concordance — contaminated, and quantifiably

`disagreement_pairs_3arm.csv` has rows for the affected papers, but **`o4mini_value` is empty
in 320 of the 322**, and the concordance scorer recorded those as **`MISMATCH`, not as missing
data**:

| score column | MISMATCH total | from empty o4mini cells | share |
|---|---:|---:|---:|
| `local_vs_o4mini_score` | 1,220 | **304** | **24.9%** |
| `o4mini_vs_sonnet_score` | 941 | **320** | **34.0%** |
| `local_vs_sonnet_score` | 1,007 | 82 | 8.1% |

**A quarter of all local-vs-openai disagreements and a third of all openai-vs-sonnet
disagreements are this defect, not arm disagreement.** The affected papers are also
over-represented in the disagreement corpus as a direct consequence — 322 of 2,267 rows
(14.2%) come from 17 of 190 papers (8.9%) — because a missing value forces MISMATCH on two of
the three pairs, pulling nearly every field of those papers into the disagreement set.

The `local_vs_sonnet` column is the control: 82 mismatches on the same rows, i.e. those papers
are not intrinsically high-disagreement; the inflation is specific to the pairs involving the
arm with missing values.

### Pass 2 judge — already quantified in JUDGE-RESTATE-01

Pass 2 read the empty CSV cells as `NOT REPORTED` absence claims and judged them: **164 arm-rows
with no stored span, 160 scored SUPPORTED**. That is the downstream consequence of the same 320
empty cells.

### Census coverage

The 323 missing openai spans would have entered the census as **17 papers × 19 fields**, spread
across all 20 field names except `study_type` and across all three field classes
(STATED/INFERABLE/JUDGMENT). Because they are missing rather than mis-valued, every openai rate
in TAXONOMY-CENSUS-01/02 and FIELDCLASS-01 is computed on a denominator 8.5% smaller than it
should be (3,457 instead of 3,780), and the missing cells are **not missing at random** — they
are concentrated in 17 whole papers. anthropic (0 deficit) and local (40) are essentially
unaffected by comparison.

---

## 6. Recommendation (not implemented)

**Re-extraction of the 17 openai papers is required before Paper 1's concordance numbers can be
reported, and it is the only repair that works** — the values were never produced, so there is
nothing to recover from logs, the raw response, or any cache; the arm genuinely has no opinion
on those 323 field-paper cells. Two things should land before that re-run rather than after,
because they are what let the failure pass silently the first time: a completeness check at the
write boundary (`cloud/base.py:206-212` currently rejects 0 spans and accepts 1 — it should
compare against the schema's field count and refuse or loudly flag anything short), and
capture of `finish_reason` plus the raw pre-parse `content` string, without which the seven
token-anomaly papers remain undiagnosable. Switching `response_format` from `json_object` to a
JSON *schema* would prevent the shape error at source and is worth considering in the same
pass, though it changes the extraction contract and so belongs to the fix phase's judgement,
not mine. Whether the 2 local papers warrant re-extraction is a smaller call: they are 2 of 190
and their failure mode is different, but they are also the two spans carrying non-codebook field
names that FIELDCLASS-01 had to exclude from every aggregate, so re-running them would close
that hole too. Until re-extraction happens, any concordance figure involving the openai arm
needs the §5 caveat attached, and the honest interim presentation is to exclude the 17 papers
from openai-involving comparisons rather than to let 320 artefactual mismatches stand.

---

## Acceptance gates

| gate | status |
|---|---|
| 1. Affected set enumerated with per-paper span accounting | ✅ §1 — 21 extractions, per-arm deficits, surviving field per paper |
| 2. Extraction-vs-storage fork answered per paper with raw evidence | ✅ §2 — EXTRACTION on all 21, raw `extracted_data` quoted for 3 papers; token anomaly stated, not smoothed |
| 3. Code locus with file:line, or ruled undeterminable with required instrumentation | ✅ §3 — `cloud/base.py:206-212` (non-empty where completeness was needed), `base.py:130-132` (silences it), `openai_extractor.py:83` (`finish_reason` discarded); 5 candidates explicitly ruled out |
| 4. Clustering analysis with timestamps | ✅ §4 — no clustering on position, hour, length, tier, or reasoning tokens; local papers are a separate mechanism |
| 5. Blast-radius statement | ✅ §5 — 24.9% / 34.0% of pair-wise MISMATCHes are artefactual; 323 census cells; 164 judge rows |
| 6. Read-only; zero model calls; suite green | ✅ 1417 passed, 15 deselected; commit touches one analysis module + one test file |

**Out of scope and not done:** any fix or re-extraction, Pass 2 input-contract enforcement,
census coverage-note drafting, `NOT_FOUND` schema design.

---

## Addendum (2026-08-30): parse-defective source papers, per PARSE-01

Task PARSE-01 swept all 190 EXTRACTED corpus papers for parse defects and input-limit
saturation and classified **four** as severely defective at source: **415** (a 728-page
conference proceedings volume acquired in place of one article), **719** and **586**
(font-glyph-encoded PDF text) and **455** (character-shattered extraction, one character per
line). Spans derived from these papers are present in the provenance census this report draws
on.

**Exposure is small and the headline distributions are unaffected.** The four contribute
**364 of 22,034** census classification rows — **1.65%**. Papers 415 and 719 carry only **one**
local evidence span each, so their weight in any local-arm rate is negligible. No figure in this
report is re-scored, and no claim it made is withdrawn; this is a pointer so that a reader
auditing individual spans knows which source documents are unsound.

**See:** `docs/session-reports/PARSE-01_report.md`. Appended by task PARSE-01; all text above
this heading is unchanged.
