# Field Classification — Where Each Field's Answer Lives

**Status:** pre-registration artifact for Paper 1/1b. Manuscript-grade wording.
**Version:** `prov-fieldclass-1`
**Supersedes:** the binary extractive/interpretive split in `field_class.py`
(TAXONOMY-CENSUS-01 §3), which is retained for the revision disclosure.

> **Ordering discipline.** §1 (criteria) was written and its content hashed
> *before* any field was examined or assigned. §2 (assignments) and §3 (corpus
> evidence) were added afterwards. The criteria-only revision is preserved with
> its SHA-256 in the FIELDCLASS-01 report so the ordering is auditable rather
> than asserted.

This document defines the cross-tab axis only. **The evidence-provenance
taxonomy is frozen at v1.1 and is not reopened here** — no span changes class,
no census is re-run under different taxonomy rules. The only thing that changes
is how fields are grouped when the same spans are tabulated.

---

## 1. Criteria (pinned before classification)

The question is **where a correct answer's evidence lives in the source
document**, not whether the field is hard, not whether models do well on it, and
not what the codebook asks the extractor to do. A field's class is a property of
the papers, not of the arms.

### 1.1 STATED

**The answer, when present, exists as an assertion in the paper's body text; a
verbatim quote can evidence it.**

There is a sentence (or a caption, or a table row read as a claim) in which the
authors assert the thing. An extractor who copies that sentence has produced
complete, sufficient evidence: no reader needs to supply a step. Absence is
meaningful and quotable-about — if the paper never asserts it, "not reported" is
itself a checkable claim about the body text.

*Test:* could a careful reader, handed only the extracted quote, confirm the
value without knowing anything else about the paper? If yes → STATED.

### 1.2 INFERABLE

**The answer is derivable from paratext or context — affiliations, headers,
funding notes, population descriptions, apparatus lists — but is typically not
asserted as a claim. Evidence = source material + an inference step.**

The material that grounds the answer is genuinely present in the document, so
this is not judgment or invention; but the document never *says* the answer.
Paratext is the usual home: author affiliation blocks, correspondence addresses,
ethics statements, equipment manufacturer strings. The extractor must supply one
short, mechanical, near-universally-agreed step (Vancouver BC → Canada;
`@imperial.ac.uk` → United Kingdom) that the paper leaves implicit.

*Test:* is there text that fixes the answer, while no text states it? If yes →
INFERABLE. The distinguishing mark against JUDGMENT is that two competent
extractors given the same paratext would reach the same answer essentially
always.

### 1.3 JUDGMENT

**The answer is a synthesis no single passage states. Evidence = supporting
passages + rationale.**

The value belongs to the codebook's vocabulary rather than the paper's. Reaching
it requires weighing several parts of the document against a rubric, and
competent extractors can legitimately disagree. Quoting is still useful — it
shows what informed the judgment — but no quote can be *sufficient*, because the
inferential step is contestable rather than mechanical.

*Test:* could two careful extractors, both reading the whole paper honestly,
defensibly disagree? If yes → JUDGMENT.

### 1.4 The boundary that matters

INFERABLE and JUDGMENT both require a step beyond quotation. They are separated
by **whether the step is contestable**:

| | inference step | competent extractors | evidence sufficient on its own |
|---|---|---|---|
| STATED | none | agree | yes |
| INFERABLE | mechanical, near-universal | agree | no — quote + one step |
| JUDGMENT | evaluative, rubric-mediated | may differ | no — quote + rationale |

### 1.5 Tiebreak rule for paper-variable fields

Some fields are STATED in one paper and INFERABLE in the next — a result given
in prose in one study and only as a table cell in another; a country named in
the methods of a multi-site trial but only derivable from affiliations in a
single-lab technical report.

**Rule: classify by the MODAL case across the Run 6 corpus**, estimated from a
declared random sample, and **report the variance explicitly** — sample size,
split, and the sampling seed. A field whose modal case commands less than ~70%
of its sample is flagged **paper-variable** in §2 and must be treated as a hard
case by any downstream design that wants to declare a field's class once, at
setup time, rather than per paper.

Ties and near-ties resolve **toward the more demanding class** (STATED →
INFERABLE → JUDGMENT). Rationale: the cost of under-declaring is that an
extractor is held to an evidence standard the paper cannot support, which
manufactures apparent fabrication; the cost of over-declaring is a weaker
evidence requirement on some papers. The first error corrupts the measurement,
the second only loosens it.

### 1.6 What this classification is not

- **Not a difficulty ranking.** A STATED field can have terrible agreement (the
  assertion may be buried, ambiguous, or contradicted elsewhere).
- **Not a statement about arm behaviour.** Classes are assigned from the papers
  and the codebook, never from which arm failed where. Where corpus evidence is
  used in §3 it measures *the papers* (does the country appear in body text?),
  never *the extractions*.
- **Not a licence to relabel spans.** The taxonomy is frozen; this axis only
  regroups.

---

## 2. Assignments

Written **after** §1 was hashed. Evidence basis is stated per field: *measured*
(a declared corpus sample, §3), or *reasoned* (codebook semantics and the §1
tests, no sample drawn). Reasoned assignments are not weaker claims about the
criteria, only weaker claims about the corpus.

| # | field | tier | class | basis | justification |
|---|---|---:|---|---|---|
| 1 | study_type | 1 | **STATED** | reasoned | The codebook instruction leads with "Look for explicit statements like 'prospective study,' 'case series'"; papers name their own type in the methods, and the documented inference fallback is the minority case. |
| 2 | robot_platform | 1 | **STATED** | reasoned | Hardware is named in prose ("a da Vinci Research Kit (dVRK)"); a paper that used a robot says which one. |
| 3 | task_performed | 1 | **STATED** | reasoned | The methods describe the task in the authors' own words; the field asks for that description, not a category. |
| 4 | sample_size | 1 | **STATED** ⚠ | measured (weak) | Counts are asserted ("n = 5 pigs"); summing across groups is arithmetic over stated numbers. **Paper-variable** — see §3.3. |
| 5 | surgical_domain | 1 | **INFERABLE** ⚠ | reasoned | Clinical papers name the specialty, but this corpus is dominated by bench, phantom and simulation work where no specialty is asserted and values such as "Non-clinical Bench / Phantom" are assigned from the described setup. **Paper-variable** — see §3.4. |
| 6 | autonomy_level | 2 | **JUDGMENT** | reasoned | The codebook supplies a five-step decision tree precisely because papers routinely do not reference the Yang levels; applying it requires deciding whether the robot "chooses its own strategy", which competent extractors can weigh differently. |
| 7 | validation_setting | 2 | **STATED** | reasoned | The methods assert in vivo / ex vivo / phantom / simulation directly; the "select most advanced" rule ranks stated facts rather than deriving new ones. |
| 8 | task_monitor | 2 | **INFERABLE** | reasoned | No paper writes "R" or "Shared"; the architecture description (cameras, force sensors, who watches) fixes the answer mechanically, but never states it. |
| 9 | task_generate | 2 | **INFERABLE** | reasoned | Who authors the plan is read off the system description — trajectory computation vs. surgeon-set parameters — and is not itself a claim the paper makes. |
| 10 | task_select | 2 | **INFERABLE** | reasoned | Same decomposition; the material is present in the control-flow description. NR rate is the highest of the four (5.3%) but still low, so the answer is normally derivable rather than absent. |
| 11 | task_execute | 2 | **INFERABLE** | reasoned | Same decomposition; "Shared" summarizes a cooperative-control arrangement described across several sentences rather than named. |
| 12 | system_maturity | 2 | **JUDGMENT** | reasoned | Technology-readiness categories are the codebook's frame for the work, not a claim the authors make about themselves; "Research prototype" vs "Algorithm on existing platform" is a defensible disagreement. |
| 13 | study_design | 2 | **JUDGMENT** | reasoned | The codebook's design vocabulary is finer than authors' self-description, so "single best-fit" selection is an evaluative call in most papers. |
| 14 | country | 2 | **INFERABLE** | **measured** | 19 of 20 sampled papers fix the country only through affiliations, correspondence addresses or funding notes — paratext, not assertion. See §3.1. |
| 15 | primary_outcome_metric | 3 | **STATED** | reasoned | Defined positionally as the first quantitative outcome in results; the codebook calls it "positional order, no judgment", and the metric is named in the results prose. |
| 16 | primary_outcome_value | 3 | **STATED** | **measured** | 14 of 15 sampled values are locatable in body prose rather than table-only. See §3.2. |
| 17 | comparison_to_human | 3 | **STATED** | reasoned | Where a robot-vs-human comparison exists the paper reports it in text; where it does not, "no comparison reported" is a checkable claim about the body text. |
| 18 | secondary_outcomes | 3 | **STATED** | reasoned | Additional reported outcomes are quoted metric/value pairs drawn from the results section. |
| 19 | key_limitation | 4 | **JUDGMENT** | reasoned | The instruction explicitly forbids copying the authors' own limitations section and asks for the extractor's judgment; `source_quote_required` is set precisely because the answer is not itself quotable. |
| 20 | clinical_readiness_assessment | 4 | **JUDGMENT** | reasoned | The codebook states outright that "there is no right answer in the text" and asks the extractor to synthesize results, limitations and validation setting. |

**Totals: 9 STATED · 6 INFERABLE · 5 JUDGMENT.**
⚠ = flagged paper-variable (§1.5); see §3.3–3.4.

Mapping to the superseded binary axis: every JUDGMENT field was `interpretive`;
every STATED field was `extractive`; the INFERABLE six are drawn from **both**
(`country` was extractive; `surgical_domain` and the four `task_*` were
interpretive). The three-way axis is therefore not a relabelling of the binary
one and the two cross-tabs are not nested.

---

## 3. Corpus evidence

### 3.1 country — 20-paper sample, seed 20260728

Sampling: 20 papers drawn without replacement from those with a local-arm
`country` value. For each, every occurrence of the country and its common
surface variants was located in the parsed text and judged **paratext**
(affiliation block, correspondence address, funding note, copyright line,
reference list, author biography) versus **body-text assertion locating the
study**.

| verdict | n | share |
|---|---:|---:|
| INFERABLE-only — country fixed solely by paratext | **19** | **95%** |
| STATED — asserted in body text | **1** | 5% |

The single STATED case is paper 659: *"surgeons from shanghai changzheng
hospital (shanghai, china) were invited to participate in the experiment"* — a
methods sentence that locates the study.

The finding that matters for anyone building a checker: **country strings appear
in body text constantly, and almost never state the answer.** Observed
body-text occurrences that are *not* the study's country include background
epidemiology (paper 383: "over 479,814 hysterectomies were performed in the
united states"; paper 347: "the leading cause of cancer-related deaths in the
united states"), standards bodies ("united states pharmacopeia"), equipment
vendor addresses (paper 801: "blue phantom, sarasota, fl, usa"; a 3-D printer
from "prague, czech republic"), and commercial system lists (paper 386:
"virtual navigator (esaote, genoa, italy)"). A naive "is the value present in
the text" test passes on all of these while the paper never says where the work
was done.

### 3.2 primary_outcome_value — 15-paper sample, seed 20260728

Motivation: TAXONOMY-CENSUS-03 found that 8 of 15 inspected `primary_outcome_value`
spans were table-cell verbalizations, raising the possibility that the field's
answer typically lives in a table rather than in prose. That sample was drawn
from the **no-basis subset** and is therefore biased toward exactly the cases
where prose quotation had failed.

Re-measured without that bias (values located spacing-tolerantly; a hit is
"table" if its neighbourhood contains three or more pipe characters):

| location of the value | n | share |
|---|---:|---:|
| body prose | **14** | 93% |
| table-only | 0 | 0% |
| not locatable | 1 | 7% |

`primary_outcome_value` is STATED. The CENSUS-03 impression was a sampling
artifact, and this correction is recorded rather than quietly dropped.

### 3.3 sample_size — measured, but the measurement is weak (⚠ paper-variable)

The same probe reports 13/15 prose, 1/15 table, 1/15 not-found — but the
measurement is **not trustworthy for this field** and is reported as such: the
values are small integers ("9", "20", "30") that match somewhere in essentially
any paper by chance, so a positive hit is not evidence that the number was
asserted *as the sample size*. The assignment is therefore reasoned, not
measured.

Two sources of variance argue for the ⚠ flag:
- **30.4% of stored values are NR** across the census — in nearly a third of
  papers no sample size is reported at all.
- The codebook's "If multiple groups, sum them" rule means a real minority of
  papers require a derivation (4 pigs + 5 phantoms → 9) that no passage states.

Modal case is still an asserted count, so STATED stands under §1.5, with the
tiebreak's "toward the more demanding class" note recorded for the fix phase.

### 3.4 surgical_domain — reasoned, unmeasured (⚠ paper-variable)

No sample was drawn. The class is assigned on the codebook's own vocabulary:
"Non-clinical Bench / Phantom" and "Computational / Simulation Only" are
categories the corpus's bench and simulation papers never claim for themselves,
while a clinical paper naming its specialty is STATED. The split between those
two populations was **not measured**, so the flag records genuine uncertainty
rather than a measured near-tie. Measuring it is a candidate for the fix phase
if the class matters to the evidence policy.

### 3.5 Fields deliberately not sampled

The remaining 15 assignments are reasoned from codebook semantics against the §1
tests. Where the codebook itself declares the answer's status — `key_limitation`
("using YOUR judgment"), `clinical_readiness_assessment` ("there is no right
answer in the text"), `primary_outcome_metric` ("positional order, no judgment"),
`autonomy_level` (a decision tree for when the paper does not reference Yang) —
that declaration is treated as authoritative and no sample would add anything.
