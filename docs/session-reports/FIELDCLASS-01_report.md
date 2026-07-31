# FIELDCLASS-01 — Three-way field reclassification and census recount

Project: evidence-engine · Machine: DGX (spark-59e4) · Date: 2026-07-28
Commit **`9b0da41`** · repo state at start: clean at **`f182f0c`** (OPS-GUARD-01 handed back).

**Ollama call count: 0.** Read-only against `provcensus_surgical_autonomy_20260727T194748Z`
and Run 6 source tables; pure-Python string search. No prompt, codebook, or judge edits.
**The provenance taxonomy is untouched and remains frozen at v1.1** — no span changed class;
only the cross-tab axis changed.

**Ordering discipline (Gate 1).** `analysis/provenance/FIELD_CLASSES.md` was written with §1
criteria **only**, then hashed, before any field was examined:

```
3bcffc576a29bd35a940f8c90c4ced212301d7ea1b8f2b4bd07319bc45b088a9  (criteria-only)
criteria pinned at 2026-07-27T23:58:27Z
```

The criteria-only revision is preserved at `scratchpad/FIELD_CLASSES.criteria-only.md`.
Assignments (§2) and corpus evidence (§3) were appended afterwards; final file hash
`dcd2e7e3…`. A unit test asserts the module never drifts from the document.

Full offline suite: **1387 passed, 15 deselected** (was 1369).

---

## 1. Classification

Criteria, in one line each: **STATED** — asserted in body text, a quote is sufficient.
**INFERABLE** — fixed by paratext/context, never asserted; quote + one mechanical step.
**JUDGMENT** — a synthesis no passage states; quote + rationale, and competent extractors
may defensibly disagree. The INFERABLE/JUDGMENT boundary is **whether the inference step is
contestable**.

| # | field | tier | class | basis | justification |
|---|---|---:|---|---|---|
| 1 | study_type | 1 | STATED | reasoned | Codebook leads with "Look for explicit statements like 'prospective study,' 'case series'"; the inference fallback is the minority case. |
| 2 | robot_platform | 1 | STATED | reasoned | Hardware is named in prose; a paper that used a robot says which one. |
| 3 | task_performed | 1 | STATED | reasoned | The methods describe the task in the authors' own words, which is what the field asks for. |
| 4 | sample_size | 1 | STATED ⚠ | measured (weak) | Counts are asserted ("n = 5 pigs"); summing groups is arithmetic over stated numbers. Paper-variable — §4. |
| 5 | surgical_domain | 1 | INFERABLE ⚠ | reasoned | Clinical papers name the specialty; this corpus is dominated by bench/phantom work where the value is assigned from the setup. Paper-variable — §4. |
| 6 | autonomy_level | 2 | JUDGMENT | reasoned | The codebook supplies a five-step decision tree precisely because papers routinely don't reference the Yang levels. |
| 7 | validation_setting | 2 | STATED | reasoned | Methods assert in vivo / ex vivo / phantom directly; "select most advanced" ranks stated facts. |
| 8 | task_monitor | 2 | INFERABLE | reasoned | No paper writes "R" or "Shared"; the architecture description fixes who observes but never states it. |
| 9 | task_generate | 2 | INFERABLE | reasoned | Who authors the plan is read off the system description; not a claim the paper makes. |
| 10 | task_select | 2 | INFERABLE | reasoned | Same decomposition; NR rate is the highest of the four at 5.3%, still low enough that the answer is normally derivable. |
| 11 | task_execute | 2 | INFERABLE | reasoned | Same decomposition; "Shared" summarizes cooperative control described across several sentences. |
| 12 | system_maturity | 2 | JUDGMENT | reasoned | Readiness categories are the codebook's frame for the work, not the authors' claim. |
| 13 | study_design | 2 | JUDGMENT | reasoned | The codebook's design vocabulary is finer than authors' self-description. |
| 14 | **country** | 2 | **INFERABLE** | **measured** | **19/20 sampled papers fix the country only through paratext** — §2. |
| 15 | primary_outcome_metric | 3 | STATED | reasoned | Defined positionally; the codebook calls it "positional order, no judgment". |
| 16 | primary_outcome_value | 3 | STATED | **measured** | 14/15 sampled values locatable in body prose, not table-only — §2.2. |
| 17 | comparison_to_human | 3 | STATED | reasoned | Where the comparison exists the paper reports it; where it doesn't, "no comparison reported" is a checkable claim about the text. |
| 18 | secondary_outcomes | 3 | STATED | reasoned | Quoted metric/value pairs from results. |
| 19 | key_limitation | 4 | JUDGMENT | reasoned | Instruction forbids copying the authors' limitations and asks for the extractor's judgment. |
| 20 | clinical_readiness_assessment | 4 | JUDGMENT | reasoned | Codebook states outright that "there is no right answer in the text". |

**9 STATED · 6 INFERABLE · 5 JUDGMENT.** Three assignments are measured against a declared
sample; seventeen are reasoned from codebook semantics. That split is stated per field rather
than blurred.

**The axes are not nested.** Every JUDGMENT field was `interpretive` and every STATED field
was `extractive`, but the INFERABLE six come from **both**: `country` was extractive;
`surgical_domain` and the four `task_*` were interpretive. So the recount is not a relabelling.

---

## 2. Corpus evidence

### 2.1 country — 20 papers, seed 20260728 (required by Gate 1)

| verdict | n | share |
|---|---:|---:|
| **INFERABLE-only** — country fixed solely by paratext (affiliations, correspondence, funding, copyright, references, author bios) | **19** | **95%** |
| STATED — asserted in body text | **1** | 5% |

The single STATED case is paper 659: *"surgeons from shanghai changzheng hospital (shanghai,
china) were invited to participate in the experiment."*

**The finding that matters beyond the classification:** country strings appear in body text
constantly and almost never state the answer. Observed body-text occurrences that are *not*
the study's country include background epidemiology (paper 383: "over 479,814 hysterectomies
were performed in the united states"; paper 347: "the leading cause of cancer-related deaths
in the united states"), standards bodies ("united states pharmacopeia"), equipment vendor
addresses (paper 801: "blue phantom, sarasota, fl, usa"; a 3-D printer from "prague, czech
republic"), and commercial system lists (paper 386: "virtual navigator (esaote, genoa,
italy)"). A naive "is the value present in the text" check passes on every one of these while
the paper never says where the work was done.

### 2.2 primary_outcome_value — 15 papers, seed 20260728 (correction on the record)

TAXONOMY-CENSUS-03 found 8/15 inspected spans were table-cell verbalizations, suggesting the
field's answer might live in tables. **That sample was drawn from the no-basis subset** — i.e.
precisely the cases where prose quotation had already failed. Re-measured without that bias:

| location | n | share |
|---|---:|---:|
| body prose | **14** | 93% |
| table-only | 0 | 0% |
| not locatable | 1 | 7% |

`primary_outcome_value` is STATED. The earlier impression was a sampling artifact and is
corrected here rather than quietly dropped.

### 2.3 Fields deliberately not sampled

Seventeen assignments are reasoned. Where the codebook itself declares the answer's status —
`key_limitation` ("using YOUR judgment"), `clinical_readiness_assessment` ("there is no right
answer in the text"), `primary_outcome_metric` ("positional order, no judgment"),
`autonomy_level` (a decision tree for when the paper doesn't reference Yang) — that
declaration is treated as authoritative.

---

## 3. Recount: three-way vs binary

Same census run, same 11,017 spans, same taxonomy classes. Coverage check passes: both axes
leave exactly the same two spans unclassified (`Title`, `field_1` — the known collapsed Pass 2
outputs), so the comparison is like-for-like.

Denominator throughout = spans with a snippet, **excluding `ABSENCE_CLAIM`** (the population
carved out in taxonomy v1.1).

### 3.1 No-basis by class

**THREE-WAY (new)**

| class | POOLED | local | openai | anthropic |
|---|---:|---:|---:|---:|
| stated | 305/4479 (**6.81%**) | 219/1445 (15.16%) | 25/1346 (1.86%) | 61/1688 (3.61%) |
| **inferable** | 365/3234 (**11.29%**) | 304/1087 (**27.97%**) | 38/1008 (**3.77%**) | 23/1139 (2.02%) |
| judgment | 216/2740 (**7.88%**) | 189/935 (20.21%) | 15/855 (1.75%) | 12/950 (1.26%) |

**BINARY (CENSUS-02 axis, reproduced for the disclosure)**

| class | POOLED | local | openai | anthropic |
|---|---:|---:|---:|---:|
| extractive | 359/5017 (7.16%) | 269/1623 (16.57%) | 27/1517 (1.78%) | 63/1877 (3.36%) |
| interpretive | 527/5436 (9.69%) | 443/1844 (24.02%) | 51/1692 (3.01%) | 33/1900 (1.74%) |

### 3.2 Full taxonomy distribution by three-way class (pooled)

| class | n | ANCHORED | STITCHED | DRIFTED | UNTR_PARTIAL | UNTR_NO_BASIS | ABSENCE_CLAIM |
|---|---:|---:|---:|---:|---:|---:|---:|
| stated | 4967 | 57.8% | 5.2% | 18.3% | 2.8% | 6.1% | **2.7%** |
| inferable | 3298 | 54.8% | 6.0% | 23.3% | 2.9% | **11.1%** | 0.5% |
| judgment | 2750 | 57.8% | **10.0%** | 21.2% | 2.7% | 7.9% | 0.0% |

`ABSENCE_CLAIM` falls almost entirely on STATED fields (2.7% vs 0.5% vs 0.0%) — which is what
the class means: only a field whose answer would be *asserted* can be meaningfully *absent*.

### 3.3 Per-arm, per-class (the coping-style view)

| arm | class | ANCHORED | STITCHED | DRIFTED | NO_BASIS |
|---|---|---:|---:|---:|---:|
| local | stated | 54.7% | 0.3% | 16.2% | 12.9% |
| local | inferable | 48.3% | 0.1% | 20.6% | **27.0%** |
| local | judgment | 59.9% | 0.5% | 18.1% | 20.1% |
| openai | stated | 67.2% | 3.7% | 12.5% | 1.6% |
| openai | inferable | 76.0% | 1.9% | 14.9% | 3.7% |
| openai | judgment | 81.9% | 2.6% | 12.1% | 1.7% |
| anthropic | stated | 52.1% | **11.3%** | 25.7% | 3.6% |
| anthropic | inferable | 42.0% | **15.4%** | 33.7% | 2.0% |
| anthropic | judgment | 33.9% | **26.2%** | 32.6% | 1.3% |

### 3.4 Per-field (all 20, sorted by pooled no-basis)

| field | class3 | binary | no-basis | denom | rate | local | openai | anthro | absence |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|
| task_execute | **inferable** | interpretive | 80 | 546 | 14.7% | 35.1% | 5.4% | 2.6% | 0 |
| primary_outcome_value | stated | extractive | 73 | 516 | 14.2% | 25.7% | 3.9% | 12.2% | 7 |
| task_select | **inferable** | interpretive | 71 | 522 | 13.6% | 31.5% | 6.6% | 4.2% | 14 |
| task_generate | **inferable** | interpretive | 71 | 545 | 13.0% | 31.9% | 4.8% | 1.6% | 0 |
| task_monitor | **inferable** | interpretive | 64 | 539 | 11.9% | 28.1% | 4.9% | 2.1% | 0 |
| secondary_outcomes | stated | extractive | 51 | 485 | 10.5% | 25.2% | 4.0% | 2.8% | 12 |
| primary_outcome_metric | stated | extractive | 56 | 541 | 10.3% | 20.5% | 3.6% | 6.3% | 1 |
| **country** | **inferable** | *extractive* | 54 | 538 | 10.0% | 28.1% | 1.2% | 1.1% | 3 |
| system_maturity | judgment | interpretive | 53 | 548 | 9.7% | 25.1% | 2.3% | 1.1% | 0 |
| study_design | judgment | interpretive | 50 | 548 | 9.1% | 20.9% | 2.9% | 3.2% | 0 |
| comparison_to_human | stated | extractive | 27 | 304 | 8.9% | 19.2% | 6.0% | 5.5% | 75 |
| clinical_readiness_assessment | judgment | interpretive | 46 | 547 | 8.4% | 23.7% | 0.6% | 0.5% | 1 |
| autonomy_level | judgment | interpretive | 39 | 548 | 7.1% | 18.2% | 1.8% | 1.1% | 0 |
| validation_setting | stated | extractive | 35 | 545 | 6.4% | 17.4% | 0.0% | 1.6% | 0 |
| key_limitation | judgment | interpretive | 28 | 549 | 5.1% | 13.3% | 1.2% | 0.5% | 0 |
| surgical_domain ⚠ | **inferable** | interpretive | 25 | 544 | 4.6% | 13.1% | 0.0% | 0.5% | 0 |
| sample_size ⚠ | stated | extractive | 19 | 429 | 4.4% | 14.4% | 0.8% | 1.1% | 37 |
| task_performed | stated | extractive | 19 | 549 | 3.5% | 8.5% | 0.6% | 1.1% | 0 |
| study_type | stated | extractive | 14 | 566 | 2.5% | 5.8% | 1.1% | 0.5% | 0 |
| robot_platform | stated | extractive | 11 | 544 | 2.0% | 4.3% | 0.0% | 1.6% | 0 |

Median field-level rate: **inferable 12.4% > judgment 8.4% > stated 6.4%** (local medians
29.8% / 20.9% / 17.4%) — the same ordering as the pooled figures, so it is not an artifact of
unequal field sizes.

---

## 4. Headline answers

### (a) Does no-basis concentrate on JUDGMENT once INFERABLE is separated? — **NO**

Pooled ordering is **INFERABLE 11.29% > JUDGMENT 7.88% > STATED 6.81%**. Local: **27.97% >
20.21% > 15.16%**. OpenAI: **3.77% > 1.75% ≈ 1.86%**. INFERABLE is highest on every arm that
produces no-basis spans at all, at both pooled and field-median level, and the four `task_*`
fields take four of the top five per-field slots.

The prediction behind the original binary split — that fabrication tracks *judgment* — is not
what the corpus shows. What it shows is that fabrication tracks **unquotability**: fields whose
answer exists in the document but is never asserted are the worst, worse than fields where the
answer is openly a synthesis. A plausible reading, offered as description not design: the two
JUDGMENT fields carrying `source_quote_required` are told to quote the passage that informed
the judgment, so the model has a sanctioned thing to quote; INFERABLE fields have an answer
but nothing to quote and no instruction acknowledging that, so the model writes its own
sentence. That gap is the fix phase's problem, not this task's.

### (b) Do INFERABLE fields account for the restatement-dominant pattern? — **YES**

`country` moved from extractive to INFERABLE and carries a 28.1% local no-basis rate — the
second-highest of any field on that arm. The four `task_*` fields, all INFERABLE, occupy the
top of the table. Under the binary axis these five were split across both classes, which is
exactly why CENSUS-02's contrast was muddy: `country` was inflating "extractive" while
`task_*` inflated "interpretive", and the two effects partially cancelled. Separating them
raises the gap between the most- and least-affected class from **2.5 pp (binary) to 4.5 pp
(three-way)** pooled, and from **7.5 pp to 12.8 pp** on the local arm.

### (c) Does the local-vs-anthropic coping-style story survive? — **YES, and it sharpens**

Both arms respond to the same gradient — distance from quotability — in opposite ways, and the
three-way axis makes the gradient monotone where the binary axis could not:

- **anthropic stitches, monotonically**: STITCHED 11.3% (stated) → 15.4% (inferable) → 26.2%
  (judgment), while ANCHORED falls 52.1% → 42.0% → 33.9% and no-basis *falls* 3.6% → 2.0% →
  1.3%. The less quotable the field, the more it bridges real passages, and it essentially
  never invents.
- **local invents**: no-basis 12.9% → 27.0% → 20.1%, peaking at INFERABLE, with STITCHED flat
  at ~0.3% throughout. It does not bridge at all; when it cannot quote, it writes.
- **openai mostly complies**, with its own quiet signal: ANCHORED *rises* with unquotability
  (67.2% → 76.0% → 81.9%), which deserves a look in the fix phase — plausibly it quotes
  something adjacent and defensible rather than reaching.

---

## 5. Paper-variable fields — the hard cases for setup-time declaration

Two fields are flagged ⚠ under the §1.5 tiebreak. These are where a "declare the class once,
at setup" design will hurt:

| field | declared | the variance |
|---|---|---|
| **sample_size** | STATED | 30.4% of stored values are `NR` — in nearly a third of papers no sample size is reported at all — and the codebook's "if multiple groups, sum them" rule means a real minority require a derivation (4 pigs + 5 phantoms → 9) that no passage states. The prose-vs-table probe returned 13/15 prose but is **not trustworthy for this field**: the values are small integers that match somewhere in almost any paper by chance. Assignment is therefore reasoned, and reported as such. |
| **surgical_domain** | INFERABLE | A clinical paper naming its specialty is STATED; a bench or phantom paper assigned "Non-clinical Bench / Phantom" is INFERABLE. **The split was not measured** — the flag records genuine uncertainty, not a measured near-tie. Its low no-basis rate (4.6% pooled, 13.1% local) is the lowest of the INFERABLE six, consistent with a class that is partly mis-assigned. |

Two further fields are worth watching even though they are not flagged: `study_type` (codebook
documents an inference fallback) and `comparison_to_human` (75 absence claims — by far the
most of any field, so its *modal* behaviour is arguably "assert absence" rather than "quote").

---

## 6. Arguments against the three-way design

**6.1 INFERABLE is not homogeneous, and its headline depends on which members you keep.**
Within the class, `task_execute` is 14.7% and `surgical_domain` is 4.6% — a threefold spread.
Drop `surgical_domain` and the class looks stronger; drop the four `task_*` and it collapses to
`country` alone. The class's elevated rate is carried by five of its six members, and one of
those six is flagged as possibly mis-assigned. A reviewer entitled to ask "is INFERABLE a real
kind, or a bag containing the four `task_*` fields?" would not be answered by these numbers.

**6.2 Seventeen of twenty assignments are reasoned, not measured.** Only `country`,
`primary_outcome_value` and (weakly) `sample_size` rest on a declared sample. The rest are
arguments from codebook wording against criteria I wrote. That is defensible — and the
codebook's own declarations are strong evidence for four of them — but the classification
should not be described in the manuscript as an empirical result. It is a considered
taxonomy with three empirical anchors.

**6.3 The criteria were pinned by me, and one of them decided a contested case in advance.**
§1.1 says a table row "read as a claim" counts as STATED. That was written before I looked, and
it is why `primary_outcome_value` is STATED rather than something table-specific — but it is
also a substantive choice made by the person who then benefited from it having been made early.
Pre-registration constrains, it does not neutralize.

**6.4 The three-way split does not explain the local arm's ordering.** If unquotability drove
everything, JUDGMENT should exceed INFERABLE for local; instead INFERABLE (27.0%) beats
JUDGMENT (20.1%). My `source_quote_required` explanation in §4(a) is post-hoc and untested —
only two of five JUDGMENT fields carry that flag, and one of them
(`clinical_readiness_assessment`, 23.7% local) is nearly as bad as the INFERABLE fields. The
axis describes the data better than the binary one; it does not yet explain it.

**6.5 The gain over the binary axis is real but modest.** 4.5 pp vs 2.5 pp pooled separation.
The strongest argument for the change is not the effect size — it is that the binary axis
asserted something about `country` that 19 of 20 sampled papers contradict. Correctness, not
power, is the case here, and the report should say so.

**6.6 Two spans remain unclassifiable on both axes.** `Title` and `field_1` (paper 415 and 719,
local arm, collapsed Pass 2 outputs) sit outside every class. They cannot affect any
conclusion at n=2, but they are live corruption in the Run 6 source table and remain in the
fix-phase backlog.

---

## Acceptance gates

| gate | status |
|---|---|
| 1. Criteria pinned before assignment; every field justified; country split reported | ✅ criteria-only SHA `3bcffc57…` at 2026-07-27T23:58:27Z, preserved in scratchpad; 20/20 justified; country 19/20 INFERABLE (§2.1) |
| 2. Recount complete, presented alongside the binary version | ✅ §3.1, same run, same spans, coverage check confirms like-for-like |
| 3. Headline questions answered yes/no with numbers | ✅ §4 — (a) NO, (b) YES, (c) YES-and-sharper |
| 4. Zero Ollama; no prompt/codebook/judge edits; full suite green | ✅ `git show --stat 9b0da41` touches only `analysis/provenance/` + one test file; **1387 passed, 15 deselected** |

**Out of scope and not done:** evidence-policy implementation, codebook metadata schema for
setup-time class declaration, reasoning-trace prompt changes, taxonomy amendments (v1.1 stays
frozen), judge restatement, Arm P.
