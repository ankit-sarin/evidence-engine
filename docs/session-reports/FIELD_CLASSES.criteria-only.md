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
