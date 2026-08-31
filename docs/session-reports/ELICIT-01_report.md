# ELICIT-01 — Copy-based vs index-based quote elicitation, STATED fields

**Task:** ELICIT-01 (reissue). **Date:** 2026-08-30/31. **Runtime:** Ollama **0.21.0**,
`deepseek-r1:32b`, digest `edba8017331d15236e…`, temperature 0, `think=True` declared.
**76 Pass-1 calls** (38 papers × 2 conditions). **No Pass-2 call. No write to `review.db`.**

Two elicitation modes were held identical in every respect except how evidence is asked for:

* **COPY** — value plus one or more **verbatim quotes** the model copies from the paper.
* **INDEX** — value plus a list of **numbered unit indices**; the engine materializes the quotes
  from a persisted unit map after the run. Invalid indices are recorded as-is, never repaired,
  clamped, or nearest-matched.

**This report contains no design recommendation.** The mechanism decision is architect-side.

---

## 1. Pre-flight

| check | expected | found |
|---|---|---|
| Ollama version | exactly 0.21.0 | **0.21.0** |
| Reliability config | 4 vars set | `FLASH_ATTENTION=true`, `MAX_LOADED_MODELS=1`, `NUM_PARALLEL=1`, `KEEP_ALIVE=-1` (+`KV_CACHE_TYPE=f16`) |
| Model digest | `edba8017331d…` | **match** |
| Experiment flock | free | free → held for both runs → released |
| **I1** classifier `value` participation | "carried, not scored" | **TRUE for non-empty snippets; FALSE for empty ones** — see below |
| **I2** `select_sample()` stability | identical to CAPTURE-01's 40 | **identical in set *and* order** |

**I1, precisely.** `value` is read in exactly one branch of `classify_span`: when the snippet is
**empty**, it selects `ABSENCE_DECLARED` (value ∈ `{"", "nr", "n/a", "na", "not_found",
"not found", "not reported", "none"}`) versus `MISSING_SNIPPET`. For any non-empty snippet the
ladder never reads it again — anchoring, absence-claim detection, sentence decomposition and
ratios are purely lexical on the snippet. Verified empirically: one snippet across six different
values returned `ANCHORED` every time; an empty snippet returned `ABSENCE_DECLARED` for
`NR`/`NOT_FOUND`/`""`/`None` and `MISSING_SNIPPET` for `"Original Research"`. Values are
therefore passed through faithfully in scoring rather than blanked, so absent fields land in the
correct non-taxonomy class.

**Manifest — n = 38.** `select_sample()` (SEED=20260729, N_TOTAL=40) minus **415** (PARSE-01
`MERGED_DOCUMENT`, truncates at the ceiling) and **719** (PARSE-01 `EXTRACTION_FAILURE`,
truncates at the ceiling). Of the 38: **35 in corpus, 3 not** — 547, 629, 799 are
`FT_SCREENED_OUT` non-members retained deliberately for CAPTURE-01 comparability and reported
separately in §7. Strata and per-paper detail in `eval/elicit01/manifest.json`.

## 2. Numbered-unit rendering (INDEX)

Starts from `analysis/provenance/segment.py` (pysbd 0.3.4, `clean=False`) **unmodified**, then one
deterministic post-pass, frozen at smoke sign-off:

1. `<!-- … -->` comment blocks are stripped **before** segmentation, so no index can point at a
   Docling artifact. Raw pysbd shreds `<!-- image -->` into `<!` and `-- image -->`.
2. Units below **3 whitespace tokens** are **merged** into a neighbour — forward into the
   successor, backward for a trailing unit — never discarded. The threshold reuses
   `segment.py`'s existing pinned `MIN_SENTENCE_TOKENS` rather than inventing a number.
3. **Bijection:** concatenating the units reproduces the comment-stripped source token for token.
   Enforced by test, because a merge that silently drops text would make an index space that does
   not describe the paper.

| paper | raw units | raw short-unit % | after | after short-unit % |
|---|---:|---:|---:|---:|
| 39 (long) | 790 | 15.4% | 678 | **0.0%** |
| 67 (medium) | 609 | 28.6% | 450 | **0.0%** |
| 121 (short) | 180 | 12.2% | 152 | **0.0%** |

Unit maps for all 38 papers are persisted (`eval/elicit01/unit_maps.json`).

## 3. Fit check and truncation tripwire

All **76** prompts were rendered and sized **before any model call**, against the enforced
ceiling of **131,072** (`n_ctx_train`). PARSE-01 established that the configured and derived
values — 262,144, and `OLLAMA_CONTEXT_LENGTH=0` — are the wrong numbers to trust, because the
runtime clamps against the trained context. Sizing used the **worst** observed chars→tokens
ratio (0.4288), not the median, because a central estimate under-predicts exactly the token-dense
text most likely to truncate.

| | value |
|---|---|
| prompts checked | **76** |
| largest estimate | **69,267** tokens (p498, PARSE-01 `LEGITIMATE_LONG`) |
| headroom at that paper | 61,805 tokens |
| failures | **none** |

**Post-hoc tripwire: 0 rows.** No row reached `prompt_eval_count == 131,072`. The tripwire exists
because, per PARSE-01, `done_reason` cannot detect input truncation — it reports `stop` either
way — and a test asserts a synthetic row at the ceiling is flagged `TRUNCATED` despite
`done_reason="stop"`.

## 4. Completeness

| | COPY | INDEX |
|---|---:|---:|
| calls | 38 | 38 |
| ok | **38** | **38** |
| failed | 0 | 0 |
| truncated | 0 | 0 |
| container parsed | **36** (`direct`) | **38** (`direct`) |
| **unparseable** | **2** (p445, p522) | **0** |
| field entries returned | 324 / 342 | **342 / 342** |

Every call succeeded at the transport level; no retry was exhausted. Two COPY responses were
**unparseable as a container** — the run recorded them, the analysis excluded them, and neither
was repaired. Wall time 2.37 h + 0.21 h smoke; two proactive restarts fired at the 25-call
cadence.

## 5. Results

### 5.1 COPY — provenance ladder over every quote returned (n = 341 spans)

Classified with the frozen v1.1 ladder, `classify_span` imported unmodified.

| class | n | % |
|---|---:|---:|
| **ANCHORED** | 123 | **36.1%** |
| DRIFTED | 53 | **15.5%** |
| ABSENCE_DECLARED | 48 | 14.1% |
| UNTRACEABLE_NO_BASIS | 44 | **12.9%** |
| STITCHED | 43 | 12.6% |
| MISSING_SNIPPET | 22 | 6.5% |
| UNTRACEABLE_PARTIAL | 7 | 2.1% |
| ABSENCE_CLAIM | 1 | 0.3% |

Per field, ANCHORED share ranges from **64.9%** (`task_performed`) to **15.4%**
(`comparison_to_human`):

| field | ANCH | STITCH | DRIFT | UNTR_PART | UNTR_NB | ABS_DECL | MISS_SNIP | total | ANCH % |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| task_performed | 24 | 7 | 3 | 1 | 2 | 0 | 0 | 37 | **64.9%** |
| validation_setting | 19 | 4 | 10 | 0 | 4 | 2 | 0 | 39 | 48.7% |
| study_type | 17 | 9 | 8 | 0 | 2 | 0 | 0 | 36 | 47.2% |
| robot_platform | 17 | 7 | 9 | 1 | 4 | 1 | 0 | 39 | 43.6% |
| primary_outcome_metric | 14 | 4 | 11 | 1 | 3 | 3 | 0 | 36 | 38.9% |
| primary_outcome_value | 10 | 3 | 3 | 1 | 10 | 11 | 0 | 38 | 26.3% |
| sample_size | 8 | 3 | 3 | 2 | 1 | 19 | 0 | 36 | 22.2% |
| secondary_outcomes | 8 | 5 | 6 | 0 | 12 | 10 | 0 | 41 | 19.5% |
| comparison_to_human | 6 | 1 | 0 | 1 | 6 | 2 | 22 | 39 | **15.4%** |

### 5.2 INDEX — selection validity (n = 505 indices)

| | value |
|---|---|
| indices returned | 505 |
| **valid** | **505** |
| out of range | **0** |
| malformed (non-integer) | **0** |
| **validity rate** | **100.0%** |
| materialized quote length, median | 135 chars |
| round-trip integrity | 505 / 505 |

**The headline is the validity rate, not an anchoring rate.** A materialized quote is ANCHORED by
construction, so reporting "100% ANCHORED" would measure the materializer, not the model. The
round-trip figure is reported only as an integrity check that index → quote → verbatim-in-source
holds; it carries no information about model behaviour.

### 5.3 Abstention: NOT_FOUND per field per condition

| field | COPY | INDEX |
|---|---:|---:|
| study_type | 0 | 0 |
| robot_platform | 1 | 1 |
| task_performed | 0 | 0 |
| sample_size | 4 | 2 |
| validation_setting | 0 | 1 |
| primary_outcome_metric | 2 | 4 |
| primary_outcome_value | 5 | 5 |
| comparison_to_human | 1 | 3 |
| secondary_outcomes | 2 | 3 |
| **total** | **15** | **19** |

**Paired asymmetry, both directions** (same paper, same field, one condition answers and the
other abstains):

| direction | n | fields |
|---|---:|---|
| INDEX abstains, COPY answers | **12** | primary_outcome_value 4, primary_outcome_metric 3, comparison_to_human 2, secondary_outcomes 2, validation_setting 1 |
| COPY abstains, INDEX answers | **8** | primary_outcome_value 4, sample_size 2, secondary_outcomes 1, primary_outcome_metric 1 |

**The smoke-stage signal did not survive the full run.** At n=3 the asymmetry was 4–0 against
INDEX, which looked like index-citation suppressing extraction. At n=38 it is **12 vs 8** — still
tilted, but present in both directions and concentrated in the same numeric-outcome fields on
both sides. This report records the counts and does not interpret them as a suppression effect.

### 5.4 VALUE_WITHOUT_CITATION

A value returned with no evidence cited. Counted separately and **excluded from the INDEX
validity denominator**, which counts indices rather than fields.

| field | INDEX (no unit cited) | COPY analogue (empty quote list) |
|---|---:|---:|
| comparison_to_human | 20 | 23 |
| sample_size | 14 | 15 |
| secondary_outcomes | 6 | 8 |
| primary_outcome_value | 2 | 6 |
| validation_setting | 1 | 2 |
| primary_outcome_metric | 1 | 1 |
| **total** | **44** | **55** |

**COPY shows the analogous failure, and slightly more of it.** The COPY analogue is partly
concealed inside the §5.1 ladder: an empty quote list reaches the classifier's empty-snippet
branch and is recorded as `MISSING_SNIPPET` (22 spans), or as `ABSENCE_DECLARED` when the value
happens to be a codebook absence sentinel. Reading the ladder alone would attribute those to
absence rather than to uncited assertion.

### 5.5 Presence agreement across conditions (presence only, no correctness judgment)

| field | pairs | agree | agree % | COPY-only | INDEX-only |
|---|---:|---:|---:|---:|---:|
| study_type | 36 | 36 | 100.0% | 0 | 0 |
| robot_platform | 36 | 36 | 100.0% | 0 | 0 |
| task_performed | 36 | 36 | 100.0% | 0 | 0 |
| validation_setting | 36 | 35 | 97.2% | 1 | 0 |
| sample_size | 36 | 34 | 94.4% | 0 | 2 |
| comparison_to_human | 36 | 34 | 94.4% | 2 | 0 |
| secondary_outcomes | 36 | 33 | 91.7% | 2 | 1 |
| primary_outcome_metric | 36 | 32 | 88.9% | 3 | 1 |
| **primary_outcome_value** | 36 | 28 | **77.8%** | 4 | 4 |

### 5.6 Latency and prompt inflation

| | COPY | INDEX |
|---|---:|---:|
| median latency | 127.2 s | **113.5 s** |
| total | 1.36 h | 1.22 h |
| median `prompt_eval_count` | 11,041 | 12,601 |

**INDEX is the faster condition despite the larger prompt**, because its outputs are roughly half
the length — it emits integers where COPY emits sentences.

**Calibration note for the future input-fit guard.** Measured token inflation of INDEX over COPY
on the same paper is **1.141×** (1.148× on smoke). The manifest fit check predicted **1.03–1.05×**
from a character-length ratio. `[Sn]` markers tokenize worse than they read, so a guard
estimating from characters will **under-predict INDEX-style prompts by roughly 10 percentage
points**. The margin here was large enough that it did not matter; on a paper near the ceiling it
would.

## 6. Eyeball material

Verbatim side-by-sides, one clean and one disagreeing per stratum. COPY carries the classifier
verdict; INDEX shows unit ids and unit boundaries.

**LONG — clean.** p383, `primary_outcome_metric`. Both values `"Suture spacing consistency"`.
COPY quote **[ANCHORED]**: *"When measuring suture spacing to quantify anastomosis quality, the
STAR's average spacing of 2.63 mm was less than that for LAP (4.22 mm) and RAS (5.05 mm). The
average suture spacing was significantly more consistent for STAR as …"* — INDEX cited `[297]`:
*"[S297] When measuring suture spacing to quantify anastomosis quality, the STAR's average
spacing of 2.63 mm was less than that for LAP (4.22 mm) and RAS (5.05 mm)."* The COPY quote runs
one sentence longer than the unit boundary; the evidence is the same passage.

**LONG — disagreement.** p708, `secondary_outcomes`. COPY value *"Targeting time: average 16.9 s;
Path following deviation: 0.62 mm average; …"*; INDEX value *"Root-mean-square error; Standard
deviation; Maximum tip error (2.27 mm)"*. COPY quote **[DRIFTED]**: *"The autonomous targeting of
five points within the robot workspace showed effective convergence rates (average 16.9 s).
Performance of the proposed learning-based PCC and analytic PCC models was compared in a path
following task, …"* — INDEX cited `[351, 352]`: *"[S351] The spatial positional errors of shape
tracking for the bending section are shown in Fig. 9"* / *"[S352] (b). The average positional
error of 21 FBG sensing segments is 0.63 mm, and 1.53 mm at the tip."* The two conditions
selected **different outcomes from the same paper**, and the COPY quote is DRIFTED rather than
verbatim.

**MEDIUM — clean.** p542, `study_type`. Both `"Original Research"`. COPY quote **[ANCHORED]**;
INDEX cited `[7, 164]`, of which `[S7]` is a direct methods statement and `[S164]` is a design
rationale sentence — a valid but weaker second citation.

**MEDIUM — disagreement.** p604, `primary_outcome_value`. COPY value `"80%; 5/7 trials"`; INDEX
value `"80% in simulation; 5/7 successful trials in real experiment"`. COPY quote **[STITCHED]**:
*"Our method achieves close to 80 percent task success rate**...** We conducted seven trials of
the experiment, each featuring a different attachment point location. …"* — the ellipsis bridges
two distant passages, which is what STITCHED records. INDEX cited `[198, 224]`, the two source
passages **separately and in full**, each verbatim.

**SHORT — clean.** p763, `sample_size`. Both `"10"`. COPY quote **[ANCHORED]** mentions *"Five
trials were performed …"*; INDEX cited `[69, 75]` — *"[S69] … five trials were performed …"* and
*"[S75] Five cyst biopsy trials were performed …"*. **INDEX exposes the arithmetic** (5 + 5 = 10)
across two units; the single COPY quote supports only half of its own value.

**SHORT — disagreement.** p764, `study_type`. Both values `"Original Research"`, but the evidence
differs sharply. COPY quote **[DRIFTED]**: *"This work aims at developing a robot-assisted 2D
US-based fetoscope tracking approach with automatic initialization as an FLP intervention
auxiliary. …"* — INDEX cited `[1]`: *"[S1] ## Development of Robot-assisted Ultrasound System for
Fetoscopic Tracking in Twin to Twin Transfusion Syndrome Surgery"*, the **title header**. Both
conditions reached the same value; one produced a non-verbatim quote, the other a technically
valid citation of a heading.

## 7. The three non-corpus papers, separately

547, 629 and 799 are `FT_SCREENED_OUT` with zero extractions — not members of the 190-paper
corpus, retained for CAPTURE-01 comparability (PARSE-01 traced their inclusion to the
`select_sample()` carried-path filter gap). All three behaved unremarkably: **9/9 fields returned
in both conditions, `direct` parse on all six calls**, no truncation, no invalid index. They
contribute 9 of the 244 collected eyeball rows. Nothing in §5 depends on them, and no figure here
changes materially if they are dropped.

## 8. What these artifacts do not establish

- **No correctness judgment anywhere.** Presence agreement is presence only. The ladder measures
  whether a quote is traceable to the paper, **not whether the value is right** and not whether
  the cited passage actually supports it. A `[S1]` title citation (§6, p764) is valid and
  well-anchored and may still be poor evidence. Judge-based supportedness is the follow-on task.
- **INDEX's 100% validity is not 100% quality.** It says every index named a real unit. It says
  nothing about whether the right unit was chosen. §5.4's 44 `VALUE_WITHOUT_CITATION` cases sit
  entirely outside that denominator.
- **The two conditions are not blind to prompt differences.** They share the field set,
  definitions, ordering and container, but the instruction text necessarily differs, and INDEX
  additionally sees a renumbered rendering of the paper. Any of that could contribute.
- **One draw per paper per condition, temperature 0, n = 38, one model, one review.** No
  repetition, so no within-paper variance estimate; no confidence intervals are computed and none
  should be read in.
- **The unit post-pass is a study artifact, not a production segmenter.** Its 3-token merge and
  comment stripping were frozen from three papers' distributions.
- **Two COPY responses were unparseable and are simply absent** from §5.1 and §5.5. They are not
  scored as failures of quoting, and no attempt was made to recover them.
- **The DRIFTED and UNTRACEABLE rates are not directly comparable to the Run 6 census figure**
  (18.0% local-arm drift): different field subset (9 STATED vs 20), different prompt, different
  elicitation instruction, different paper set.

## 9. Acceptance gates

| gate | status |
|---|---|
| 1. Pre-flight recorded incl. I1–I2 and manifest | ✅ §1 |
| 2. Fit check passed for all 76 prompts before any call | ✅ §3 — max 69,267 vs 131,072 |
| 3. Smoke 3/3 both conditions, reported, signed off before full run | ✅ 6/6 calls, tripwire 0 |
| 4. Full run ≥90% complete rows; tripwire count reported | ✅ §4 — **100%** (76/76 ok), tripwire **0** |
| 5. Analysis from unmodified taxonomy; unit maps persisted | ✅ `classify_span`/`PaperIndex` imported unchanged; `unit_maps.json` |
| 6. Report + limitations, no design recommendation; committed, flock released, gate green | ✅ §8, §10 |

## 10. Invariants

`review.db` **not written** (read-only `immutable=1` access only; mtime unchanged);
`parsed_text` read-only. Ollama version unchanged at 0.21.0 and its config untouched; two
proactive restarts fired from the run's own 25-call cadence under a self-held flock, which is the
designed behaviour. Experiment flock **released**. Store is append-only under
`data/surgical_autonomy/eval/elicit01/` (gitignored); code and tests committed.

**Out of scope and not done:** any Pass-2 call, judge-based supportedness scoring, INFERABLE and
JUDGMENT field designs, the production input-fit guard and parse-quality gate, contract C
cutover, and any change to production extraction, `select_sample()` or corpus membership.
