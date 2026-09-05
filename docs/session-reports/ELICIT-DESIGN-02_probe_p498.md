# ELICIT-DESIGN-02 — D2 probe: one paper (p498), after the JUDGMENT-instruction rewrite

**Machine:** DGX · run id `smoke_20260905T024348Z` · Ollama 0.21.0, `NRestarts=0`, no service
action · prior scratch (`smoke_20260903T155654Z`, `smoke_20260905T011330Z`) preserved ·
production `review.db` mtime **2026-07-27 19:47:48**, byte-identical.

**Claim under test: breakage, not compliance.** The probe asks whether rewriting
`clinical_readiness_assessment`'s instruction broke anything. It does not ask, and cannot
answer, whether the rewrite made extraction better. **n = 1.**

---

## 1. D2 expectations, checked

| expectation | result |
|---|---|
| mechanics zero-defect | **PASS** — 0 malformed, 0 out-of-range, 0 duplicates, parse `direct` 2/2, tripwire 0 |
| 1/1 stored with 20/20 terminal states | **PASS** — 1 extraction, 20 evidence_spans, every field one state |
| `clinical_readiness_assessment` reaches a terminal state | **PASS** — `EVIDENCED_VALUE`, 2 citations, 1 reasoning step, **zero violations** |

No stop condition triggered. The rewritten field did not go CONTRACT_UNMET.

## 2. Side by side, p498 only

| measure | smoke_20260905T011330Z | probe smoke_20260905T024348Z |
|---|---|---|
| stored | 1/1 | 1/1 |
| accepted attempt | 2 | 2 |
| failures per attempt | 8 → 2 | **3 → 1** |
| attempt-1 failures | `surgical_domain`, `autonomy_level`, `system_maturity`, `study_design`, `country`, `primary_outcome_value`, `key_limitation`, `clinical_readiness_assessment` | `sample_size`, `primary_outcome_value`, `comparison_to_human` |
| attempt-2 failures | `comparison_to_human`, `secondary_outcomes` | `country` |
| violations on accepted | `VALUE_WITHOUT_CITATION` 2 | `INFERENCE_MISSING` 1 |
| terminal states | 16 ev / 2 esc / 2 unmet | **11 ev / 8 esc / 1 unmet** |
| escape-token uses | 2 | **8** |
| malformed / out-of-range / duplicate | 0 / 0 / 0 | 0 / 0 / 0 |
| parse path · tripwire | `direct` 2/2 · 0 | `direct` 2/2 · 0 |
| prompt chars · `prompt_eval_count` | 188,675 · 48,763 | 186,519 · 48,246 |
| latency | 963 s | 869 s |

`clinical_readiness_assessment` and `key_limitation` — the two JUDGMENT fields Ruling 3 and D1
touched — were `EVIDENCED_VALUE` with zero violations in **both** runs.

## 3. The observation the architect needs, stated without a verdict

**Escape-token uses on this paper went 2 → 8, and evidenced fields 16 → 11.** Six fields changed
terminal state, and **none of them is the field that was rewritten**:

| field | prior | probe |
|---|---|---|
| `task_monitor` | EVIDENCED_VALUE (`R`) | **NO_EVIDENCE_LOCATABLE** |
| `task_generate` | EVIDENCED_VALUE (`R`) | **NO_EVIDENCE_LOCATABLE** |
| `task_select` | EVIDENCED_VALUE (`R`) | **NO_EVIDENCE_LOCATABLE** |
| `task_execute` | EVIDENCED_VALUE (`R`) | **NO_EVIDENCE_LOCATABLE** |
| `primary_outcome_metric` | EVIDENCED_VALUE | **NO_EVIDENCE_LOCATABLE** |
| `comparison_to_human` | CONTRACT_UNMET | NO_EVIDENCE_LOCATABLE |
| `secondary_outcomes` | CONTRACT_UNMET | **EVIDENCED_VALUE** (3 outcomes, 1 citation) |
| `country` | EVIDENCED_VALUE | **CONTRACT_UNMET** (`INFERENCE_MISSING`) |

**Why a one-field prose edit can move fields it did not touch, and why n=1 cannot separate the
two explanations.** The edit shortened the prompt by 2,156 characters, at a position ahead of
most of the schema. Every token after it shifts, so the whole completion differs — this is not
a semantic effect of the rewrite propagating to `task_monitor`, it is a different sample from
the same model. Temperature is 0, which fixes the sampling rule, not the prompt.

Two readings are therefore both live and this probe distinguishes neither:

1. **Noise.** One draw at n=1 from a 1,064-unit paper, where ELICIT-DESIGN-01's F6 already
   measured that "long papers degrade by ceasing to cite, not by citing badly". The four
   `task_*` fields ceasing to cite is that failure mode exactly.
2. **Signal.** The escape token, now taught on every field line, is being reached for where the
   model previously asserted a value it could not evidence. On that reading `task_*` going
   escape is the guard working, and the prior run's four `R` values are the thing to check.

**The `task_*` values are the ones to eyeball.** All four were the bare string `R` with 1
citation each in the prior run. Whether `R` was a defensible Yang-framework reading of this
paper, or an under-evidenced habit the escape token has now displaced, is a question about the
paper and the codebook that this probe cannot answer and that I have not tried to.

## 4. What is NOT claimed

No compliance verdict. No regression verdict. The probe's only claim is the one D2 authorised:
the rewrite did not break the pipeline — mechanics stayed zero-defect, the paper stored whole,
and the rewritten field reached a clean terminal state with reasoning steps and citations.
