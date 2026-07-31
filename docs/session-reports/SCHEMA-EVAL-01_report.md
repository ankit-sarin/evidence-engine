# SCHEMA-EVAL-01 — Schema-constrained vs unconstrained extraction

Project: evidence-engine · Machine: DGX (spark-59e4) · Date: 2026-07-28
Commit **`c8fcb03`** (harness + analysis; raw outputs in gitignored
`data/surgical_autonomy/eval/schema_eval/`). Clean tree. Suite **1437 passed, 15 deselected**.

**No production code path or review.db extraction table was modified.** The local runner
deliberately avoids `extract_paper()` because that stores; all eval output goes to a separate
store.

**Model-call accounting.** Budget was local 20 / cloud 10. Actual: local **20** (as budgeted);
cloud **15 requests, 10 model-executed** — the 5 first-pass Anthropic requests were rejected at
the API boundary (HTTP 400, zero tokens, zero inference) and were replaced by 5 executed calls
under the only configuration the API permits (§1.3). Plus **3 pre-flight acceptance probes**
(one per provider) and **3 think-channel probes** on a trivial prompt.

---

## 1. Pre-flight findings

### 1.1 Ollama version and the think channel

**Ollama 0.21.0** (`ollama --version`, `/api/version`). Three probes on `deepseek-r1:32b`:

| probe | think field | `<think>` in content | schema-valid | latency |
|---|---|---|---|---|
| production: `format=<schema>`, `think=False` | absent | no | **yes** | 21.6s |
| `format=<schema>`, `think=True` | **present, 1,744 chars** | no | **yes** | 49.0s |
| unconstrained, `think=True` | present, **1,744 chars** | no | no — returned ```` ```json {"study_type": …} ```` | 38.6s |

**The think channel is not degraded by the constraint.** Thinking is present and the final
channel is schema-valid simultaneously, and the thinking length is *identical* (1,744 chars) to
the unconstrained call — the grammar constrains the answer channel only. The third clause of the
decision rule ("think-channel behavior degraded/absent") therefore does not fire.

### 1.2 Two findings that reframe the experiment

**The local arm is already schema-constrained in production.** `extract_pass2_structured`
passes `format=ExtractionOutput.model_json_schema()` to Ollama. The brief's condition A
("current production path … no grammar constraint") is accurate for the cloud arms but not for
local. For local the labels are therefore **A = unconstrained (novel), B = constrained
(production)**, and the question the A/B answers is *"should the existing constraint be
kept?"*, not *"should one be added"*. Carried through the harness and this report.

**The production schema cannot constrain cardinality.**
`ExtractionOutput.model_json_schema()` puts no `minItems`/`maxItems` on the `fields` array — a
one-element array is schema-valid. **Constrained decoding as currently configured could not
have prevented SPANLOSS-01.** Only a 20-property all-required object can, which is what
condition B uses for the cloud arms.

### 1.3 Schema acceptance — all three providers, with one hard incompatibility

The 20-property, all-required, `additionalProperties: false` schema (current field order,
current span shape, **no enums**) is **accepted by all three providers**, each returning all 20
fields on a trivial prompt: Ollama (`format=`), OpenAI (`json_schema` strict), Anthropic (forced
tool `input_schema` — there is no `response_format` equivalent).

**But Anthropic rejects extended thinking together with a forced tool.** All 5 first-pass calls
returned:

```
400 invalid_request_error: Thinking may not be enabled when tool_choice forces tool use.
```

On that arm, **structured output and extended thinking are mutually exclusive**. This is a
provider constraint, not a tuning choice. The Anthropic condition was therefore re-run with
thinking OFF — a second changed variable on that arm, declared here and in the module docstring
rather than hidden.

---

## 2. Measures

Sample (fixed seed 20260728, stratified): **4 collapse-class** (386, 466, 498, 694),
**2 long** (39 @86k, 708 @86k chars), **4 ordinary** (547, 629, 691, 799). Cloud subset:
39, 629, 691, 694, 799.

### Local arm — deepseek-r1:32b (A = unconstrained, B = constrained/production)

| measure | A unconstrained | B constrained | delta |
|---|---|---|---|
| **1. Shape** — calls / errors | 10 / 0 | 10 / 0 | — |
| guard pass rate | **100%** | **100%** | 0 |
| parse path | `flat_field_dict` ×4, `bare_list` ×6 — **never canonical** | `schema_valid` ×10 | — |
| would-retry count | 0 | 0 | 0 |
| **2. Value agreement A↔B** | 189/200 fields agree — **disagreement 5.5%** | | |
| vs Run 6 stored | 51.4% disagree | 50.0% disagree | −1.4pp |
| **3. Provenance** ANCHORED | **16.5%** (33/200) | **10.5%** (21/200) | **−6.0pp** |
| STITCHED / DRIFTED | 2.0% / 15.0% | 2.0% / 12.5% | −2.5pp drift |
| UNTRACEABLE_NO_BASIS | 52.0% (104) | 60.5% (121) | +8.5pp |
| **4. Absence** value / assertion | 12.0% / 0.5% | 13.0% / 0.5% | +1.0pp |
| empty snippets | 27 | 27 | 0 |
| **5. Cost** median latency | 419.0s | 427.5s | +8.5s (+2%) |
| think present | 10/10 | 10/10 | 0 |

### Cloud arms — condition B only; control is the Run 6 stored extraction

| measure | openai (strict `json_schema`) | anthropic (forced tool, **thinking off**) |
|---|---|---|
| calls executed / errors | 5 / 0 | 5 / 0 (after 5 API-rejected) |
| complete | **5/5 (100%)** | **4/5 (80%)** |
| parse path | `strict_json_schema` | `forced_tool_schema` |
| ANCHORED | **64.0%** | 52.5% |
| STITCHED / DRIFTED | 1.0% / 8.0% | 14.1% / 31.3% |
| UNTRACEABLE_NO_BASIS | **0.0%** | 1.0% |
| absence values | 22.0% | 16.2% |
| median latency | 45.1s | 43.3s |
| median in/out tokens | 14,290 / 7,273 | 17,329 / 2,461 |
| vs Run 6 disagreement | 41.5% | 41.7% |

**Anthropic's forced tool does not strictly enforce `required`.** Paper 629 returned 19 of 20
properties — `study_type` simply absent — with `stop_reason=tool_use` and no error. The schema
listed it as required. On OpenAI's `strict: true`, all 5 calls returned all 20.

---

## 3. Decision-rule outcome

Pinned before the run: adopt unless **anchored drops >3pp A→B**, or **A-vs-B disagreement
>10%**, or **think channel degraded/absent**.

| arm | anchored A→B | disagreement | think | **verdict** |
|---|---|---|---|---|
| **local (deepseek-r1:32b)** | 16.5% → 10.5% = **−6.0pp** ✗ | 5.5% ✓ | intact ✓ | **REJECT** — one clause tripped |
| **openai** | not measurable (no A condition) | not measurable | n/a | **AMBIGUOUS by the rule; see §5** |
| **anthropic** | not measurable | not measurable | **forced off** ✗ | **REJECT** on the think clause |

The rule fires mechanically. §4 explains why I do not think the local REJECT should be acted on
as written, and §5 gives the recommendation.

---

## 4. Anomalies — and one that invalidates the local absolute levels

### 4.1 A live production regression, found incidentally, larger than the question asked

Run 6 on **these same 10 papers** scored local ANCHORED **54.3%** / NO_BASIS **20.0%**. This
eval's condition B — which is byte-identical to the production path — scored ANCHORED **10.5%**
/ NO_BASIS **60.5%**. Same model file (digest `edba8017331d`, unmodified since 2026-01-19), same
prompt, same flow.

Side-by-side snippets show what changed:

| paper / field | Run 6 | this eval (condition B = production path) |
|---|---|---|
| 39 `study_type` | "TYPE Original Research PUBLISHED 21 October 2025 DOI 10.3389/frobt.2025.1650228" | "The paper describes new experimental data and results on robot-assisted TAVI." |
| 466 `robot_platform` | "We implemented our approach on the Berkeley Surgical Robots (see Fig. 1)…" | "Mentioned in the abstract and implementation details." |
| 466 `primary_outcome_value` | "Results suggest that the approach enables (i) rapid learning of trajectories…" | "Achieved speeds in experiments." |

Run 6 quoted; today's identical code authors prose.

**Mechanism, established from the pre-flight probes without extra calls.**
`extract_pass1_reasoning` gets the reasoning trace via `parse_thinking_trace`, which extracts
text between `<think>` tags and **falls back to returning the whole content when no tags are
found** (`extractor.py`). On Ollama 0.21.0 `deepseek-r1` no longer emits `<think>` in content —
the probes show thinking in a **separate `message.thinking` field** with `'<think>' in content`
False in all three. So the fallback fires on every call and Pass 1 now returns the model's
*answer* as its "reasoning trace"; Pass 2 is then primed with a first-draft answer instead of
reasoning, and paraphrases it rather than quoting the paper.

This is a **live production defect affecting every local extraction since the Ollama upgrade**,
independent of the schema question. It is out of this task's scope to fix, and it is the most
consequential thing the task found.

**Consequence for this eval:** the local *absolute* provenance levels are measured under the
broken Pass-1 regime and must not be compared to Run 6. The A-vs-B *delta* remains internally
valid — both conditions share the defect identically — but it is a delta measured on a
degraded baseline.

### 4.2 The 6.0pp anchored drop is small-sample and fragile

33 vs 21 anchored spans out of 200. Twelve spans move the verdict across a 3pp threshold. On
n=10 papers this is not a stable estimate, and I would not treat the REJECT as settled evidence
that constrained decoding harms anchoring — particularly since it was measured while Pass 1 was
feeding both conditions an answer instead of reasoning.

### 4.3 Snippet-drift direction: no tokenization-drift signature

The literature's concern is that constrained decoding forces non-canonical token boundaries and
damages verbatim quoting. The signature would be **anchored → drifted** movement. Observed
movement is anchored → **no-basis** (−6.0pp anchored, +8.5pp no-basis) with drifted *also
falling* (15.0% → 12.5%). Spans are not being lightly corrupted; they are being replaced by
authored prose — which is §4.1's mechanism, not a tokenization effect. **No tokenization-drift
signature was detected on either arm.**

### 4.4 The unconstrained condition never produced the canonical shape

Condition A returned `flat_field_dict` ×4 and `bare_list` ×6 — 0/10 canonical
`{"fields": [...]}`. It reached 100% completeness *only* because the salvage ladder rescued it.
That is the shape tax the constraint removes, and it is the strongest argument for the
constraint independent of the anchored delta.

### 4.5 Methodological contamination I introduced

My Ollama schema-acceptance probe ran concurrently with the local A/B job. With
`OLLAMA_NUM_PARALLEL=1` the requests queued against each other, so that probe's 347s is not a
usable latency figure and it delayed early A/B calls. **Local measure 5 (latency) is
contaminated and should not be used**; the A↔B latency difference (+2%) is within that noise.
Cloud latencies are unaffected.

### 4.6 Provenance gap

Run 6's `extractions.model_digest` is empty, and `get_model_digest('deepseek-r1:32b')` currently
returns unavailable, so I could not verify by digest that the Run 6 model equals today's. The
`/api/tags` digest (`edba8017331d`, modified 2026-01-19) predates Run 6, which is consistent
with "same weights, changed runtime" but does not prove it.

---

## 5. Recommendation

**Do not act on the local REJECT, and do not treat this eval as having answered the local
question — fix the Pass-1 regression first, then re-measure.** The engine-first justification is
that §4.1 is a defect in how the local arm obtains its reasoning trace, it degrades anchored
from 54.3% to 10.5% on identical papers, and it dominates the ±6pp effect the decision rule was
built to detect; measuring a format-constraint tax on top of a broken reasoning channel answers
a question nobody asked. The constraint itself is meanwhile doing visible good on the axis the
rule did *not* test: unconstrained returned a non-canonical shape on 10 of 10 papers and
survived only because the salvage ladder caught it, while constrained was schema-valid 10 of 10,
value agreement between the two is 5.5% (well inside tolerance), the think channel is untouched,
and latency cost is ~2%. My reading is that the local constraint should be **kept** and the
adopt/reject decision re-run after the Pass-1 fix, with more than 10 papers.

**For the cloud arms, adopt strict schema on OpenAI and do not adopt the forced tool on
Anthropic.** OpenAI's `json_schema` strict is unambiguously better than the current
`json_object`: 5/5 complete, 64.0% anchored, **0.0% no-basis**, all 20 required properties
honoured every time, at 45s and no observed cost penalty — and it is the only mechanism tested
that would have prevented SPANLOSS-01's 17 collapses, since `json_object` constrains syntax but
not shape. Anthropic is the opposite: its forced tool **silently dropped a required property**
on 1 of 5 papers, and adopting it costs extended thinking outright by API rule — a large
behavioural change to buy an enforcement guarantee that the same run showed is not actually
enforced. The INSTRUMENT-01 completeness guard already covers Anthropic's failure mode at the
write boundary without giving up thinking, which is the cheaper and more honest control.

---

## Acceptance gates

| gate | status |
|---|---|
| 1. Pre-flight probe results incl. exact Ollama version | ✅ §1 — Ollama 0.21.0; think channel intact under constraint; two reframing findings; schema accepted by all three providers |
| 2. Both conditions complete within budget, guard telemetry attached | ✅ §2 — local 20/20, cloud 10 executed; guard pass rates and parse paths reported per condition |
| 3. All five measures per arm with A/B deltas | ✅ §2 — with measure 5 (local latency) flagged contaminated in §4.5 |
| 4. Decision-rule outcome per arm against pinned thresholds | ✅ §3 — local REJECT (−6.0pp), anthropic REJECT (think clause), openai AMBIGUOUS by the rule |
| 5. No production paths or review.db extraction tables modified; suite green | ✅ commit touches `analysis/eval/` only; 1437 passed, 15 deselected |

**Out of scope and not done:** enum constraints, field reordering / evidence-first structure,
`NOT_FOUND` escape values, production cutover, Run 7, and any fix to the Pass-1 regression in
§4.1 — which I recommend be raised as its own task immediately.
