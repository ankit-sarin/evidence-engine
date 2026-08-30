# CAPTURE-01 — The 0.21.0 Pass-1 draft channel, captured and measured

**Task:** CAPTURE-01. **Runtime:** Ollama **0.21.0** (pinned; unchanged throughout).
**Model:** `deepseek-r1:32b`, digest `edba8017331d15236e…`. **Run:** 2026-08-30
05:17:37Z → 07:59:21Z UTC. **Calls:** 40 Pass-1 only, 0 Pass-2. **Store:**
`data/surgical_autonomy/eval/capture01/`.

PRIME-01 left one blocking unknown: no 0.21.0 Pass-1 **draft** text existed on disk, so the
channel that Run 6 accidentally primed from had never been characterised on the runtime the
pipeline actually runs. This task captured it. **No design recommendation is made here** — that
decision is architect-side.

---

## 1. Method

**The measure is PRIME-01's, imported, not restated.** `analyze_capture01.py` imports
`verbatim_window_rate`, `measure`, `SNIPPET_RE`, `WINDOW_WORDS`, `load_papers` and
`load_schema_eval1_draft_lengths` from `analysis/eval/prime01.py` **unmodified**, so these
numbers sit in the same table as PRIME-01's 0.17.7 figures and QUALGAP-01's published
0.3% / 37.7% rather than merely resembling them. `tests/test_prime01.py` pins that walk. No
change to the metric was needed, and none was made.

> normalize the text, split to words, walk non-overlapping 8-word windows; a window scores if
> it appears verbatim in the normalized paper. On a hit advance a full window, on a miss one
> word. Rate = hits / (word_count // 8).

Paper text is `data/surgical_autonomy/parsed_text/{pid}_v*.md` (newest version), normalized —
the identical source PRIME-01 measured against. The 0.17.7 comparison column is read out of
PRIME-01's stored `analysis_summary.json` at analysis time, not hardcoded, so the side-by-side
cannot drift from the study it cites.

**Capture discipline.** `parse_thinking_trace` is never imported by the runner. Both channels
are persisted exactly as the server returned them; every derived quantity is computed later
from the JSONL. No Pass-2 call was made anywhere. Nothing was written to `review.db`.

**Call shape.** `build_extraction_prompt(paper_text, spec)` — the production Pass-1 path — with
the byte-identical Pass-1 system message, `think=True` declared explicitly (never inherited),
`options={"temperature": 0}`. Every row carries the runtime version, model tag and digest, the
options dict and its SHA-256, the prompt SHA-256, and the system-message SHA-256.

## 2. Paper set provenance

**Manifest:** `analysis/eval/schema_eval2.select_sample()` — `SEED=20260729`, `N_TOTAL=40`, the
sample SCHEMA-EVAL-02 drew and QUALGAP-01 reused. Verified before running: the materialized
paper sets in `eval/schema_eval2/local_abc.jsonl` and `eval/qualgap01/runtime_v12.jsonl` are
**identical**, 40 IDs. Strata as drawn: long 13, medium 12, short 15.

PRIME-01's `matched_papers` (36) is a **strict subset** of this 40 — it is an analysis-time
intersection with papers having a Run 6 `reasoning_trace`, not a sampled manifest. CAPTURE-01
needs no Run 6 counterpart, so the full 40 is the right set; §4 also reports the like-for-like
36 so the comparison cannot be a set-composition artifact.

## 3. Smoke gate — 3/3 PASS

Papers 39, 67, 121 (one per stratum), captured and analysed before the full run:

| paper | ok | content chars | thinking chars | done | prompt_eval | eval | latency | channels identical |
|---|---|---:|---:|---|---:|---:|---:|---|
| 39 (long, Original Research) | ✅ | 4,187 | 2,897 | stop | 27,823 | 1,618 | 282 s | no |
| 67 (medium, Original Research) | ✅ | 3,461 | 2,734 | stop | 18,132 | 1,336 | 186 s | no |
| 121 (short, Review) | ✅ | 3,757 | 2,469 | stop | 10,580 | 1,422 | 167 s | no |

All 21 required persist fields present, no nulls. **No interface surprise:** `think=True` on
0.21.0 returned a populated `message.thinking` distinct from `message.content` on every call,
matching the documented native-thinking account. The analysis method ran on the three.

The two channels are the same two kinds of text PRIME-01 found on 0.17.7. Paper 39 content
opens `### Extraction Fields:\n\n1. **study_type**: Original Research\n - **source_snippet**:
"This article introduces a novel solution for robot-assisted TAVI…"`; its thinking opens
`Okay, I need to extract structured data from this paper… Let me go through each field step by
step.`

## 4. Results

**Capture completeness: 40/40 (100%).** Zero failures, zero retries (`attempts=1` on every
row), `done_reason=stop` on all 40. One runtime value (`0.21.0`), one digest, one options hash,
`think=True` on every row. No empty channel, no identical pair. Median Pass-1 latency 212 s
(mean 241, range 127–818); 2.68 h of model time.

### 4.1 Channel richness on 0.21.0 (n=40)

| channel | pooled rate | median/doc | min–max/doc | median chars | fenced JSON | docs enumerating snippets | median snippet labels |
|---|---:|---:|---:|---:|---:|---:|---:|
| **draft** (`message.content`) | **26.6%** | 23.6% | 0.0–55.6% | 3,993 | 7.5% | **28/40** | 15.0 |
| **thinking** (`message.thinking`) | **0.2%** | 0.0% | — | 2,658 | 0.0% | **0/40** | 0.0 |

### 4.2 Side-by-side against the recorded 0.17.7 values

| arm | runtime | pooled | median/doc | median chars | fenced | snippet-docs | median labels |
|---|---|---:|---:|---:|---:|---:|---:|
| **CAPTURE-01 draft** | **0.21.0** | **26.6%** | 23.6% | 3,993 | 7.5% | 28/40 | 15.0 |
| PRIME-01 Run 6 draft | 0.17.7 | 42.9% | 42.9% | 5,906 | 27.8% | 35/36 | 20.0 |
| PRIME-01 V1 draft | 0.17.7 | 37.9% | 37.4% | 5,141 | 8.3% | 34/36 | 20.0 |
| PRIME-01 V2 draft | 0.17.7 | 37.9% | 37.4% | 5,240 | 8.3% | 34/36 | 20.0 |
| **CAPTURE-01 thinking** | **0.21.0** | **0.2%** | 0.0% | 2,658 | 0.0% | 0/40 | 0.0 |
| PRIME-01 V1 thinking | 0.17.7 | 0.4% | 0.0% | 2,463 | 0.0% | 0/36 | 0.0 |
| PRIME-01 V2 thinking | 0.17.7 | 0.4% | 0.0% | 2,531 | 0.0% | 0/36 | 0.0 |

Two things are in this table and they point in different directions.

**The channel asymmetry replicates on 0.21.0, at full strength.** Drafts quote the paper;
thinking traces do not — 26.6% against 0.2%, snippet enumeration in 28/40 documents against
**0/40**, median 15 snippet labels against 0. The thinking channel's numbers are
indistinguishable from its 0.17.7 numbers (0.2% vs 0.4%, 0/40 vs 0/36, median chars 2,658 vs
2,463/2,531). Whatever else changed, the thinking channel is the same non-quoting text it was.

**The 0.21.0 draft is materially less quote-rich than the 0.17.7 draft.** 26.6% against 37.9%
for the same-shaped V1/V2 calls — **−11.3 pp pooled** — and against 42.9% for Run 6's draft,
−16.3 pp. It is also shorter (median 3,993 vs 5,141–5,906 chars) and enumerates snippets in a
smaller share of documents (70% vs 94%). This was not predicted by anything on disk, and the
JSONL is the record.

**Not a set-composition artifact.** Restricted to PRIME-01's like-for-like 36 papers, the
0.21.0 draft is **27.5%** pooled (median/doc 23.6%, snippet-docs 25/36, median labels 15.5) —
still ~10 pp below the 0.17.7 draft on the same papers. The four papers unique to the 40 (415,
547, 629, 799) do not drive the gap.

**The distribution is wide and partly bimodal.** Per-document draft rate: min 0.0, Q1 3.7,
median 23.6, Q3 37.0, max 55.6. **Nine of 40 documents score exactly 0.0%**, and 12 of 40
enumerate no snippet labels at all — so the pooled 26.6% is a mixture of drafts that quote
heavily and drafts that do not quote at all, not a uniform shift.

### 4.3 Draft length vs the surviving pre-fix measurement

The one prior 0.21.0 draft measurement that survived is a **length**: SCHEMA-EVAL-01's
`think_chars`, valid as a draft length only because that run predates the REGRESSION-01 fix.
On the 10 papers shared with this capture:

| source | median draft chars |
|---|---:|
| SCHEMA-EVAL-01 `think_chars` (pre-fix, 0.21.0) | 3,742 |
| CAPTURE-01 captured draft (0.21.0) | 3,929 |
| CAPTURE-01 captured draft, all 40 | 3,993 |

The two agree within ~5%. That is a **sanity check on the capture** — it is evidence that what
was captured here is the same channel that measurement was of — and it is not a richness
result, because length is not richness.

### 4.4 Paired against 0.17.7, same paper and same prompt

§4.2's side-by-side puts 40 papers next to PRIME-01's 36, and §5 notes that the 0.17.7
comparison is across studies. This block narrows that: for each paper, its stored 0.17.7 draft
(QUALGAP-01's V1/V2 `pass1_content`) against its 0.21.0 draft captured here — same paper, same
prompt path, same measure, paired.

| prior cell | n | 0.17.7 pooled | 0.21.0 pooled | median paired delta | 0.21.0 higher on | rho (0.177 vs 0.210) | median chars | snippet docs |
|---|---:|---:|---:|---:|---:|---:|---|---|
| V2 (`think=True`) | 39 | 37.6% | 27.1% | **−14.0 pp** | **12/39** | +0.081 | 5,189 → 4,014 | 37/39 → 28/39 |
| V1 (`think` omitted) | 39 | 37.7% | 27.1% | **−14.0 pp** | **12/39** | +0.085 | 5,094 → 4,014 | 37/39 → 28/39 |

The paired reading agrees with the pooled one and is the stronger of the two: the 0.21.0 draft
is less quote-rich on **27 of 39** papers, median drop 14 points. Together with the 36-paper
subset in §4.2, neither set composition nor denominator explains the gap.

One further measurement, recorded without explanation: the rank correlation between a paper's
0.17.7 draft richness and its **own** 0.21.0 draft richness is **rho ≈ +0.08** — essentially
zero. On 0.17.7 the two cells agree with each other almost exactly (V1 and V2 differ by 0.1 pp
pooled and rank together), so this is not measurement noise in the metric. Which papers yield
quote-rich drafts does not carry across the two capture regimes. PRIME-01's counterfactual arm
found the opposite pattern within a single regime — draft richness there tracked anchoring at
+0.347 even when the draft was discarded, which it read as paper quotability. These two facts
are in tension and this task does not resolve them.

## 5. What these artifacts do not establish

- **Not a cause.** This is a measurement of two channels on one runtime. Nothing here explains
  *why* the 0.21.0 draft quotes less than the 0.17.7 draft. Runtime, model behaviour, prompt
  interaction and sampling are not separated, and no A/B was run to separate them.
- **No anchoring outcome.** No Pass-2 call was made, so there is no anchored rate here and no
  richness→anchoring correlation for 0.21.0. PRIME-01's rho figures (+0.576 causal, +0.347
  counterfactual) are 0.17.7/Run 6 measurements and are **not** reproduced or extended by this
  task.
- **No intervention was tested.** Nothing here primes Pass 2 from a draft, so nothing here
  says what such a design would recover. PRIME-01's counterfactual arm remains the only
  evidence bearing on that, and it bounds the priming share below the raw correlation.
- **The 0.17.7 comparison is across studies, and run-to-run variance is still unbounded.**
  §4.4 pairs the two at the paper level, which removes set composition and denominator as
  explanations, but it does not make this a paired *re-run*: the 0.17.7 side is stored output
  from QUALGAP-01's second-server A/B, the 0.21.0 side a fresh capture months later. Only one
  draw per paper per regime was taken, so nothing here separates a stable regime difference
  from run-to-run variance. QUALGAP-01's same-condition replication band is the nearest
  available reference and was not re-measured.
- **The near-zero cross-regime rank correlation (§4.4) is unexplained.** It sits awkwardly
  beside PRIME-01's +0.347 counterfactual, and no analysis here reconciles them.
- **n = 40 papers**, one review, one model, one seed. No confidence intervals are computed and
  none should be read in.
- **The 9 zero-rate documents are unexplained.** Whether they are short papers, unusual study
  types, or drafts that answered without quoting is not analysed here.

## 6. Acceptance gates

| gate | status |
|---|---|
| 1. Pre-flight recorded (version, config, flock, manifest) | ✅ §1–2 — 0.21.0 exact; four `OLLAMA_*` vars present; flock free then held; `select_sample()` verified identical across two stores |
| 2. Smoke 3/3 before full run | ✅ §3 — channels non-empty and distinct on all 3, rows complete, analysis ran |
| 3. ≥90% captured with complete rows | ✅ §4 — **40/40 = 100%**, no nulls, every row carries version, options and both raw channels |
| 4. Analysis with unmodified PRIME-01 definitions + side-by-side | ✅ §1, §4.2 — metric imported unchanged; 0.17.7 column read from PRIME-01's summary |
| 5. Report committed and pushed, flock released, tree clean | ✅ — lock released at 07:59:21Z; runtime still 0.21.0 |

**Ops note.** The proactive restart fired once, at call 25 (07:08:06Z); `ExecMainStartTimestamp`
moved to 07:08:07Z, confirming it. It was permitted because *this* run held the experiment
lock, so `foreign_lock_held()` was False — the self-vs-foreign distinction working as designed.
The runtime was 0.21.0 before and after.

**Out of scope and not done:** any Pass-2 call, any priming design or recommendation, any
change to production extraction code, any re-analysis of Run 6, determinism re-runs.

**See also:** `PRIME-01_report.md` (the 0.17.7 measurement this extends),
`QUALGAP-01_report.md` (the runtime acquittal that pinned 0.21.0), and `CLAUDE.md`
"Extraction Quality Investigation".

---

## Addendum (2026-08-30): input truncation, per PARSE-01

Two claims in this report are corrected below. The measurements were made by task PARSE-01,
which swept all 190 EXTRACTED corpus papers for parse defects and input-limit saturation.

**1. The completeness claim is wrong.** §3 states *"`done_reason` = `stop` on all 40 — no
truncation"*. `done_reason` describes how **generation** terminated; it carries no information
about whether the **input** was truncated, so it was never evidence for that conclusion. Two of
the 40 papers — **415 and 719** — saturated the local context ceiling exactly
(`prompt_eval_count` = **131,072**, the model's `n_ctx_train`) and had their prompts truncated
on input. p415's full prompt is ~416,000 tokens, so roughly 31% of the document reached the
model; p719's roughly 74%. Both still returned `done_reason=stop`, which is precisely why that
field could not detect this.

**2. The "9 zero-rate documents are unexplained" bullet is partly wrong.** §5 lists nine
documents scoring 0.0% verbatim as unexplained. **Two of the nine are explained**: 415 and 719
are the truncated pair. Their drafts were measured against the *full* parsed text, including
the portion the model never received, so a 0.0% rate was partly an artifact of the measurement
frame rather than a property of the draft. The remaining seven are still unexplained.

**Corrected figures**, recomputed on the 38 untruncated papers with the metric unchanged:

| metric | as published | corrected (n=38) |
|---|---|---|
| 0.21.0 draft pooled richness | 26.6% (n=40) | **27.4%** |
| paired median delta vs 0.17.7 (V2) | −14.0 pp (n=39) | **−14.1 pp** |
| paired rho (0.177 vs 0.210) | +0.081 | **+0.020** |

**The headline conclusions are unchanged, and here is why.** The draft channel is still far
richer than thinking (0.2%) and still materially thinner than the 0.17.7 draft (37.9–42.9%).
The paired comparison in §4.4 is unaffected because **0.17.7 hit the same ceiling on the same
paper**: QUALGAP-01 §1.2 records that both runtimes derive `default_num_ctx=262144` "clamped
identically against `n_ctx_train=131072`", so p719 was truncated identically in both arms and
the pairing compares like with like. p415 was never in the paired set at all.

**Paper-set composition, for completeness.** §2 states the full 40 "is the right set". PARSE-01
found the 40 also contains **547, 629 and 799**, which are `FT_SCREENED_OUT` with zero
extractions and are therefore not members of the 190-paper corpus. This does not affect any
figure in this report — CAPTURE-01 captures Pass-1 drafts and needs no corpus membership — but a
reader should not infer corpus membership from inclusion in this sample. Root cause is recorded
in `PARSE-01_report.md`.

**See:** `docs/session-reports/PARSE-01_report.md`. Appended by task PARSE-01; all text above
this heading is unchanged.
