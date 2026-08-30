# PRIME-01 — Pass-1 channel quote-richness and its link to anchoring

**Task:** PRIME-01. **Code commit:** `9674c8c` (2026-08-23 06:31:16 +0000),
*feat(eval): measure Pass-1 channel quote-richness and its link to anchoring*.
**Report written:** 2026-08-29 by task EXIT-REMED-01, which found the 2026-08-23
exit session had committed the code and its outputs but no session report.

**Written from artifacts on disk only.** This report post-dates the analysis by six days
and reconstructs it from the stored summary, the module docstrings, and the pinned findings
carried forward in `CLAUDE.md`. It records no reasoning that is not present in those sources.
Where the artifacts are silent, the section says so rather than filling the gap.

---

## 1. The question

QUALGAP-01 acquitted the runtime: the Ollama 0.17.7 → 0.21.0 change is not why local
extraction anchoring fell after Run 6 (`HYPOTHESIS_DEAD`, +4.3pp pooled, 0.0pp median paired).
What it left standing was a different explanation — that Run 6's Pass 2 was **primed** from the
content channel, a first-draft answer dense with verbatim quotes, because the pre-fix parser's
whole-content fallback was active; and that post-fix runs prime from the thinking channel,
which quotes the paper almost never.

PRIME-01 asks the measurable version of that: **how quote-rich is each Pass-1 channel, and
does draft richness actually predict the final anchored rate?**

**Zero model calls.** Every input was already on disk.

## 2. Evidence artifacts

Inputs read by `analysis/eval/analyze_prime01.py`:

| path | role |
|---|---|
| `data/surgical_autonomy/eval/qualgap01/runtime_v12.jsonl` | 0.17.7 V1/V2 cells — the only rows carrying **both** `pass1_content` and `pass1_trace` |
| `data/surgical_autonomy/review.db` (`extractions.reasoning_trace`) | Run 6 stored Pass-1 drafts |
| `data/surgical_autonomy/eval/schema_eval2/local_abc.jsonl` | SCHEMA-EVAL-02 condition B (0.21.0) — checked for Pass-1 text; **has none** |
| `data/surgical_autonomy/eval/schema_eval/local_ab_20260728T200410Z.jsonl` | SCHEMA-EVAL-01 `think_chars` — pre-fix, so a genuine 0.21.0 draft **length** |

Output: `data/surgical_autonomy/eval/prime01/analysis_summary.json` (50,034 bytes,
2026-08-23 06:28). Code: `analysis/eval/prime01.py`, `analysis/eval/analyze_prime01.py`,
`tests/test_prime01.py`. All under gitignored `data/` except the code.

**Source availability, as the summary records it:**

| source | status | detail |
|---|---|---|
| 0.21.0 Pass-1 drafts | **ABSENT** | 40 condition-B rows, **0 with Pass-1 text** |
| 0.17.7 both channels | PRESENT | 80 rows, 78 ok, 78 with content **and** trace, 39 papers |
| Run 6 traces | PRESENT | 36 of 39 papers queried, median 5,906 chars |
| 0.21.0 draft lengths | PRESENT (length only) | 10 papers, 20 calls |

**Matched analysis set: 36 papers** — those with a Run 6 trace and 0.17.7 V1/V2 rows.

## 3. The measure

Deliberately identical to the ad-hoc check QUALGAP-01 used, so the numbers tabulate against
that report's published 0.3% / 37.7% rather than merely resembling them:

> normalize the text, split to words, walk non-overlapping 8-word windows; a window scores if
> it appears verbatim in the normalized paper. On a hit, advance a full window; on a miss,
> advance one word. Rate = hits / (word_count // 8).

`tests/test_prime01.py` pins this walk, and pins `SNIPPET_RE` to be pattern-identical to
`analyze_qualgap01`'s, so "does this draft enumerate fields with snippets" is answered the same
way in both reports. It is a coarse instrument on purpose: it asks *does this text repeat the
paper*, not *is this a well-formed quotation* — which is exactly the question priming raises.
Per `CLAUDE.md`, this measure is pinned by test against QUALGAP-01's figures and must not be
"tidied".

## 4. Measures 1–2 — channel richness and character

All on the same 36 matched papers, same model, same calls:

| channel | docs | hits/windows | pooled rate | median doc rate | median chars | fenced JSON | docs enumerating snippets | median snippet labels |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| **Run 6 draft** (0.17.7, content) | 36 | 1399/3261 | **42.9%** | 42.9% | 5,906 | 27.8% | **35/36** | 20.0 |
| **V1 draft** (0.17.7, content) | 36 | 1157/3051 | **37.9%** | 37.4% | 5,141 | 8.3% | **34/36** | 20.0 |
| **V2 draft** (0.17.7, content) | 36 | 1175/3100 | **37.9%** | 37.4% | 5,240 | 8.3% | **34/36** | 20.0 |
| **V1 thinking** (0.17.7) | 36 | 6/1634 | **0.4%** | 0.0% | 2,463 | 0.0% | **0/36** | 0.0 |
| **V2 thinking** (0.17.7) | 36 | 6/1645 | **0.4%** | 0.0% | 2,531 | 0.0% | **0/36** | 0.0 |
| 0.21.0 drafts | 0 | — | **NOT_MEASURABLE** | — | — | — | — | — |

**On identical 0.17.7 calls the two channels are categorically different texts.** Drafts repeat
the paper in **37.9–42.9%** of their 8-word windows and enumerate all 20 fields with
`source_snippet` labels in 34–35 of 36 documents. Thinking traces repeat the paper in **0.4%**
of windows — six hits across 36 documents — and enumerate snippets in **0 of 36**, with a
median doc rate of exactly 0.0%. This is not a difference of degree.

The gap is not an artifact of one channel being longer: thinking traces are roughly half the
length of drafts, but the rate is length-normalized, and the median doc rate of 0.0% means the
typical trace contains no verbatim window at all.

## 5. Measure 3 — does richness predict anchoring?

Spearman rho over the 36 matched papers, with quartiles by draft rate:

| relationship | primer? | rho |
|---|---|---:|
| Run 6 draft richness → Run 6 anchored | **yes — the draft was the actual primer** | **+0.576** |
| V1 draft richness → V1 anchored | **no — counterfactual; the draft was generated then discarded** | **+0.347** |
| V1 thinking richness → V1 anchored | yes — thinking was the actual primer | **−0.121** |
| V2 thinking richness → V2 anchored | yes | **−0.121** |

**Run 6 quartiles (draft was the primer) — monotone across all four:**

| quartile | n | draft rate | anchored |
|---|---:|---:|---:|
| Q1 | 9 | 17.3% | **37.2%** |
| Q2 | 9 | 38.6% | 56.2% |
| Q3 | 9 | 46.4% | 58.9% |
| Q4 | 9 | 55.8% | **74.4%** |

Where the draft primed Pass 2, richness tracks anchoring monotonically: **74.4% top quartile
vs 37.2% bottom**, rho **+0.576**. Where the thinking channel primed Pass 2, there is nothing
to track — three of four quartiles have a 0.0% richness rate, the fourth 1.3%, and the
correlation is a slightly negative **−0.121** on a variable with almost no variance.

**The counterfactual is the load-bearing result.** In the V1 arm the draft was generated and
then *discarded* — Pass 2 never saw it — yet draft richness still correlates **+0.347** with
that arm's anchored rate. A draft cannot influence an output it was not fed to. So a large
share of the Run 6 association is **paper quotability**: papers whose text the model tends to
repeat are papers that anchor well regardless of which channel primes Pass 2. The priming
effect is the excess of +0.576 over +0.347, not the whole of +0.576. **Naive draft-priming
should therefore be expected to recover materially less than the outstanding gap.**

**Run 6's own ceiling was leaky.** Its draft repeated the paper in 42.9% of windows and its
final anchored rate was 58.3% — the transfer was lossy in one direction and lucky in the other.
On the 36 matched papers the per-paper mean anchored rates are Run 6 **56.7%**, V1 **42.6%**,
V2 **42.2%** (QUALGAP-01's span-level figures on its own denominators are 58.3% / 43.5% /
43.1%). Run 6 is **a target to beat, not a level to restore** — restoring it would mean
restoring the defect.

## 6. Measure 4 — the residual: NOT_COMPUTABLE

The task asked what share of the outstanding gap draft-priming could close. The summary records
this as **`NOT_COMPUTABLE`**, and the reason is a data gap, not a modelling choice:

> the 0.21.0 Pass-1 draft text was never persisted; the pre-registered bands are defined on
> verbatim-window RATE, which cannot be derived from a length

The mechanism, as `prime01.py` states it: the eval runners store `raw_content` = the **Pass-2**
response and `think_chars` = an integer **length** of the Pass-1 trace; the trace text itself is
discarded. `record_call` telemetry would have held it, but is never reached — the runners
deliberately bypass `extract_paper()`, which is what calls it. **The 0.21.0 draft is absent, not
truncated** (`truncated_rows: 0`, "not applicable — never captured, so nothing to truncate").

One weak proxy survives, flagged in the summary as **"LENGTH ONLY — not richness"**:
SCHEMA-EVAL-01 ran 2026-07-28, before the REGRESSION-01 fix, so its pre-fix
`extract_pass1_reasoning` returned whole content and its `think_chars` is a genuine 0.21.0
*draft* length. On the 7 papers shared with the matched set: median 0.21.0 draft **4,511**
chars, vs 0.17.7 V1 draft **5,094** and Run 6 draft **5,799**. Suggestive of a shorter draft;
it says nothing about richness, and the report does not treat it as evidence.

**This is the blocking unknown, and it motivates CAPTURE-01** — approximately 40 Pass-1-only
calls on 0.21.0 to capture the draft text that no artifact on disk contains. Until that exists,
the residual question cannot be answered at all.

## 7. What this report does not establish

- **No 0.21.0 draft character.** §6. Every richness figure here is 0.17.7 or Run 6.
- **No causal estimate of the priming effect.** The counterfactual bounds it below +0.576 and
  above whatever +0.347 represents, but the artifacts contain no decomposition.
- **No intervention was run.** PRIME-01 made zero model calls; nothing here tests a
  quote-bearing-draft primer, it only measures the association.
- **n = 36 papers**, single review, single model. The summary reports no confidence intervals
  and this report does not manufacture any.
- The artifacts are silent on why 3 of 39 papers lack a Run 6 trace.

## 8. Acceptance gates

| gate | status |
|---|---|
| 1. Both channels measured on identical calls with a pinned method | ✅ §3–4 — 36 papers; method pinned by `test_prime01.py` against QUALGAP-01's 0.3%/37.7% |
| 2. Richness→anchoring relationship reported with a counterfactual arm | ✅ §5 — causal +0.576, counterfactual +0.347, thinking −0.121 |
| 3. Residual estimate against pre-registered bands | ⚠️ §6 — **`NOT_COMPUTABLE`**; 0.21.0 draft text was never persisted. Disclosed, not worked around |
| 4. Zero model calls; no write to `review.db` | ✅ analysis is read-only over on-disk artifacts |
| 5. Code committed with tests | ✅ `9674c8c` — `prime01.py`, `analyze_prime01.py`, `test_prime01.py` (664 insertions) |

**Out of scope and not done:** CAPTURE-01 (the ~40 Pass-1-only 0.21.0 calls that would unblock
§6), any priming intervention, Run 7, and any change to the extraction path.

**See also:** `docs/session-reports/QUALGAP-01_report.md` (the runtime acquittal that set up
this question), `CLAUDE.md` "Extraction Quality Investigation → The standing finding", and the
2026-08-29 addenda appended to `REGRESSION-01_report.md`, `SCHEMA-EVAL-01_report.md` and
`SCHEMA-EVAL-02_report.md`.

---

## Addendum (2026-08-30): p719 input truncation, per PARSE-01

Task PARSE-01 swept all 190 EXTRACTED corpus papers for parse defects and input-limit
saturation, and found two papers whose prompts exceed the local context ceiling
(`prompt_eval_count` = **131,072**, `n_ctx_train` for `deepseek-r1:32b`). One of them, **paper
719, is inside this report's 36-paper matched set**; the other, 415, was already excluded from
it for lack of a Run 6 `reasoning_trace`.

**What this means for the figures here.** p719 was truncated on input in Run 6 as well —
QUALGAP-01 §1.2 records that the clamp is against the model's `n_ctx_train`, not the runtime, so
it applies to every study in this chain regardless of Ollama version. Roughly a quarter of that
document never reached the model in any arm. PARSE-01 further classifies 719's parsed text as an
**extraction failure**: font-glyph encoded, 5,472 `GLYPH<c=..,font=..>` tokens, 8.84× inflation
against `pdftext`, so the portion the model did receive is largely glyph noise. Its channel
richness and anchored rate are therefore measured against text that was partly unavailable and
partly unreadable, and every per-paper figure involving 719 — its row in `anchored_by_paper`,
its contribution to the pooled rates in §4, and its rank in the §5 quartiles — carries that
caveat.

**The conclusions are unchanged.** The categorical channel split (drafts 37.9–42.9% verbatim
versus thinking 0.4%, snippet enumeration in ~35/36 documents versus 0/36) is a difference of
two orders of magnitude across 36 papers; one truncated paper cannot account for it. The
richness→anchoring relationships (+0.576 causal, +0.347 counterfactual, −0.121 thinking-primed)
are rank correlations over 36 pairs, and 719 is a single pair affected identically on both sides
of each comparison.

**See:** `docs/session-reports/PARSE-01_report.md`. Appended by task PARSE-01; all text above
this heading is unchanged.
