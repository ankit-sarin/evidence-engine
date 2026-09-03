# ELICIT-DESIGN-01 — per-class evidence elicitation design (Pass 1) + primed Pass 2

**Machine:** DGX (`~/projects/evidence-engine`) · **HEAD at STEP 0:** `8f1a3e4` · tree clean,
local == remote (M5 re-verified).
**Status:** STEP 0 delivered. **STOPPED for architect sign-off.** No implementation performed.

---

## 0. Pre-flight

| item | verified |
|---|---|
| repo clean at `8f1a3e4`, local == remote | ✅ `git status --porcelain` empty, `git log` matches M5 |
| `review.db` access | read-only `immutable=1` only, this step |
| model calls | **zero** — STEP 0 is read-only |
| eval store `data/surgical_autonomy/eval/elicit01/` | read only (manifest + unit_maps); not written |

---

## 1. Field census and class table (I1)

**20 fields.** Class column is **not proposed by this task** — it already exists, ratified, and is
reproduced verbatim from `analysis/provenance/field_class3.FIELD_CLASS3` (mirror of
`analysis/provenance/FIELD_CLASSES.md` §2, version `prov-fieldclass-1`). Counts: **9 STATED,
6 INFERABLE, 5 JUDGMENT**, pinned by `tests/analysis/provenance/test_field_class3_recount.py:50-52`.

| # | field | codebook tier | codebook type | class | basis | paper-variable |
|---:|---|---:|---|---|---|---|
| 1 | study_type | 1 | categorical (8) | **STATED** | reasoned | |
| 2 | robot_platform | 1 | free_text | **STATED** | reasoned | |
| 3 | task_performed | 1 | free_text | **STATED** | reasoned | |
| 4 | sample_size | 1 | numeric | **STATED** | measured (weak) | ⚠ **yes** |
| 5 | surgical_domain | 1 | categorical (10) | **INFERABLE** | reasoned | ⚠ **yes** |
| 6 | autonomy_level | 2 | categorical (8, ordinal) | **JUDGMENT** | reasoned | |
| 7 | validation_setting | 2 | categorical (8, ordinal) | **STATED** | reasoned | |
| 8 | task_monitor | 2 | categorical (4) | **INFERABLE** | reasoned | |
| 9 | task_generate | 2 | categorical (4) | **INFERABLE** | reasoned | |
| 10 | task_select | 2 | categorical (4) | **INFERABLE** | reasoned | |
| 11 | task_execute | 2 | categorical (4) | **INFERABLE** | reasoned | |
| 12 | system_maturity | 2 | categorical (6, ordinal) | **JUDGMENT** | reasoned | |
| 13 | study_design | 2 | categorical (10) | **JUDGMENT** | reasoned | |
| 14 | country | 2 | free_text | **INFERABLE** | measured | |
| 15 | primary_outcome_metric | 3 | free_text | **STATED** | reasoned | |
| 16 | primary_outcome_value | 3 | free_text | **STATED** | measured | |
| 17 | comparison_to_human | 3 | free_text | **STATED** | reasoned | |
| 18 | secondary_outcomes | 3 | free_text | **STATED** | reasoned | |
| 19 | key_limitation | 4 | free_text | **JUDGMENT** | reasoned | `source_quote_required: true` |
| 20 | clinical_readiness_assessment | 4 | categorical (5, ordinal) | **JUDGMENT** | reasoned | `source_quote_required: true` |

The 9 STATED fields are exactly the ELICIT-01 field set (M1), derived in
`analysis/eval/elicit01/prompts.stated_fields()` and asserted by `tests/test_elicit01.py:99-116`.

---

## 2. Current Pass-1 → Pass-2 data flow, as it actually is (I3)

`engine/agents/extractor.py`, single local write path.

```
extract_paper(paper_id, paper_text, spec, db)                        # :441
  prompt = build_extraction_prompt(paper_text, spec)                 # :114  ONE prompt, both passes
  pass1_think  = spec.extraction_models.pass1_think  (default True)
  pass2_think  = spec.extraction_models.pass2_think  (default False)

  reasoning_trace = extract_pass1_reasoning(prompt, think=True)      # :206
        system: "You are a systematic review data extractor. Read the paper carefully
                 and reason through each extraction field step by step..."
        user:   prompt                       # schema block + instructions + FULL paper text
        -> parse_thinking_trace(content, thinking)                    # :259
             native branch  = response.message.thinking   (0.21.0 path, always)
             legacy branch  = <think>...</think> in content
             neither        -> MissingThinkingChannelError            # REGRESSION-01, no fallback
        RETURNS: the thinking channel ONLY. response.message.content is DISCARDED.

  result = extract_pass2_structured(prompt, reasoning_trace, ...)     # :296
        system: "...Use your prior reasoning... Respond ONLY with the requested JSON."
        user 1: prompt                        # THE SAME PROMPT — full paper text AGAIN
        user 2: "Here is your prior analysis of this paper:\n\n{reasoning_trace}\n\n
                 Now output the structured extraction as JSON matching the schema..."
        format = ExtractionOutput.model_json_schema()                 # grammar-constrained
        -> ExtractionOutput.model_validate_json(raw)

  validated = _validate_and_retry_snippets(result.fields, paper_text) # :404
        per span with an ellipsis (INVALID_SNIPPET_RE): up to 2 EXTRA model calls,
        each re-sending the full paper text; on exhaustion source_snippet := ""

  enforce_completeness(span_dicts, expected_field_names(spec, codebook))   # :503
  db.add_extraction_atomic(...)                                            # :508
```

Retry driver `extract_paper_with_completeness()` (:581) wraps the above, `MAX_COMPLETENESS_ATTEMPTS = 3`,
writes one `record_call()` telemetry row per attempt.

**Four facts that bear on the design, not stated in the spec:**

1. **Pass 1's elicitation output today lands nowhere except the thinking channel.**
   `content` — the first-draft answer — is read and thrown away at `extractor.py:227`. This is
   exactly the channel PRIME-01 measured at 37.9–42.9% quote-richness while thinking sat at 0.4%.
   The new design's Pass-1 response *is* a `content` artifact, so it will be read from the channel
   production currently discards, and `parse_thinking_trace`'s no-fallback guard does not stand
   in the way (it governs the thinking channel only).
2. **Pass 2 already receives the full paper text a second time**, as `user 1`. The spec is silent
   on whether it stays once materialized evidence is present. **Ruling needed** (§7 Q4) — it is
   the single largest term in Pass-2 prompt size.
3. **The field set comes from the ReviewSpec, not the codebook.** `build_extraction_prompt`
   iterates `spec.extraction_schema.fields_by_tier(1..4)`; the codebook supplies the *content* of
   each field block via `_build_field_block`. `expected_field_names()` follows the spec for the
   same reason and only *warns* on divergence. Gate 2 wording ("codebook is the sole source of
   field classes") is satisfiable — classes can come from the codebook while the field set stays
   spec-driven — but the two sources exist and both are load-bearing today.
4. **`_validate_and_retry_snippets` becomes structurally dead for cited fields.** A materialized
   unit-map quote is verbatim by construction and cannot contain ellipsis bridging. **Ruling
   needed** (§7 Q5).

---

## 3. Port plan for the ELICIT-01 unit machinery (I2)

`analysis/eval/elicit01/` — 939 lines across 5 modules.

| artifact | verdict | note |
|---|---|---|
| `units.UnitMap` (frozen dataclass, `.n`, `.render()`, `.resolve()`, `.to_json()`) | **ports as-is** | `resolve()` already returns `None` for out-of-range and never clamps or nearest-matches — precisely spec item 4's "no silent repair" |
| `units.strip_comments` + `COMMENT_RE` | **ports as-is** | Docling `<!-- image -->` artifacts excluded from the index space |
| `units.merge_short` + `MIN_UNIT_TOKENS = 3` | **ports, with a disclosure** | ELICIT-01 §8 states outright: *"The unit post-pass is a study artifact, not a production segmenter. Its 3-token merge and comment stripping were frozen from three papers' distributions."* Porting it promotes a study artifact to production. It has a bijection property enforced by test, which is the part that matters; I propose porting unchanged and recording the provenance rather than re-deriving the threshold (out of scope). **Flag, not a blocker.** |
| `units.build_unit_map` | **ports as-is** | depends on `analysis.provenance.segment.sentences` (pysbd 0.3.4, `clean=False`) — already a shared production-adjacent module |
| `prompts.build_index_prompt` instruction text | **ports as the STATED contract**, extended per class | the ratified wording; INFERABLE/JUDGMENT additions are new |
| `prompts._schema_block` / `stated_fields` | **eval-only** | 9-field STATED-only slice; production needs all 20, grouped by class |
| `prompts.SYSTEM_PASS1` (duplicated literal) | **eval-only** | deliberately a frozen copy so a production edit cannot move the study |
| `prompts.parse_fields` container recovery | **ports as the container parser**, must be **tightened** | doc-comment: *"Deliberately shallow: it recovers the JSON container and nothing more. Per-field validity … is measured by the analysis, not repaired here."* Gate 3 requires per-class contract violations be **detected**, so validation moves from the analysis into the parser |
| `manifest.CEILING_TOKENS = 131_072`, `WORST_RATIO = 0.4288` | **ports into a new engine sizing module** | nothing in `engine/` today knows either number (grep: zero hits) |
| `manifest.build` / `select_sample` coupling | **eval-only** | `select_sample()` is the 40-paper study sampler with the known carried-path filter gap (out of scope) |
| `runner.py` per-call row + tripwire (`prompt_eval_count >= CEILING`) | **ports as telemetry**, target `record_call(extra=...)` | `record_call` already has an `extra: dict` escape hatch; no schema change needed |
| `analyze.py` | **eval-only** | post-hoc measurement, not a runtime path |

**Index-validity semantics to preserve verbatim from `analyze.py`:** malformed (`unit_indices`
absent, non-list scalar, non-int, `bool`), empty-list-with-a-value (`VALUE_WITHOUT_CITATION`),
out-of-range. All four were counted, none repaired. Item 4 says a field failing its evidence
contract is **recorded and fails**, which is a behaviour change from "counted" — intended.

---

## 4. Absence-sentinel findings (I4) — **spec premise does not survive contact with disk**

**I4 says "an 8-member sentinel set". No 8-member set exists.** Two distinct artifacts, neither
of that size, and they are different *kinds* of thing:

| artifact | shape | members | governs |
|---|---|---|---|
| `data/surgical_autonomy/extraction_codebook.yaml: absence_sentinels` | literal list | **6** — `NR`, `N/A`, `NA`, `NOT_FOUND`, `NOT FOUND`, `NOT REPORTED` | a **value**: "the arm claims this field is NOT reported" |
| `analysis/paper1/judge_prompts.ABSENCE_SENTINELS` | `frozenset`, 6 | identical 6 | the authoritative Python mirror, named as such in the codebook's own comment |
| `analysis/provenance/absence.py` (`prov-absence-1`) | **6 anchored regexes** (P1,P2,P3,P5,P6,P4) | P4's bare-sentinel alternation lists 10 literals incl. `none`, `not applicable`, `no comparison reported` | a **snippet** that asserts non-reporting instead of quoting |

`absence.py` is a snippet classifier for the frozen v1.1 taxonomy, not a value-sentinel set;
ELICIT-01's classifier reaches it through `classify_span`. Neither is 8 members. Matching is
case-insensitive and whitespace-trimmed (`extractor`-side via `judge_prompts`, `.strip().upper()`).

### 4.1 The collision that blocks spec item 3

Spec item 3 requires NOT_FOUND to be *"a single explicit escape token, **distinct from codebook
absence sentinels**"*, with:

- **NOT_FOUND** — "no evidence locatable for this field in this paper", **zero citations by definition**;
- **absence sentinel** — "the paper explicitly declares the thing absent" — is a **VALUE and requires citations**.

**`NOT_FOUND` is already a member of `absence_sentinels`.** It is also the token the current
production prompt instructs the model to emit (`extractor.py:171`: *"If the field is not found in
the paper, set to `NOT_FOUND`"*; `:175`: *"If value is NOT_FOUND, set source_snippet to `""`"*) —
i.e. today NOT_FOUND means *both* things at once, and the frozen classifier consumes that
conflation: an empty snippet plus a sentinel value resolves to `ABSENCE_DECLARED`, an empty
snippet plus a non-sentinel value to `MISSING_SNIPPET` (`analyze.py` doc-comment, verified in
ELICIT-01's pre-flight). The requested split is therefore **not expressible with the current
token set**, and any resolution touches something already pinned:

| option | change | cost |
|---|---|---|
| **A** — coin a new escape token, e.g. `NO_EVIDENCE_LOCATABLE` | codebook gains the token + the documented distinction; `absence_sentinels` untouched | new token is unknown to `judge_prompts.ABSENCE_SENTINELS`, `absence.py` P4, and every prior report; **it will be read as a non-sentinel value by the frozen classifier** → `MISSING_SNIPPET`, not `ABSENCE_DECLARED`. Recommend A **plus** an explicit written statement that pre-Run-7 artifacts do not carry it |
| **B** — remove `NOT_FOUND` from `absence_sentinels` | one-line codebook edit | changes the meaning of **existing** stored values; breaks the codebook↔`judge_prompts` mirror the codebook comment asserts; edits an input to a frozen taxonomy. **Not recommended** |
| **C** — keep NOT_FOUND doing both jobs | zero change | abandons spec item 3's distinction, and with it the "absence sentinel is a value and needs citations" half of TEST 2. **Not recommended** |

**Recommendation: A.** It is the only option that leaves the frozen taxonomy, the judge mirror and
prior reports untouched, and it is additive. **Architect ruling required — I have not chosen.**

---

## 5. Open conflicts with the spec (each needs a ruling before STEP 1)

### C1 — I1 is partly false: the class assignment exists, and moving its authority has a cost

The codebook has **no** `field_class` key (verified: `yaml.safe_load` top keys are exactly
`version, review, date, absence_sentinels, fields`; no field carries one). But no class needs
inventing: `analysis/provenance/FIELD_CLASSES.md` is a **pre-registration artifact for Paper 1/1b**
whose §1 criteria were *written and hashed before any field was examined*, with the criteria-only
revision's SHA-256 preserved in the FIELDCLASS-01 report "so the ordering is auditable rather than
asserted". `field_class3.py` is its machine-readable mirror, and the suite asserts the two agree.

Spec item 1 ("the codebook remains the single authoritative source") + gate 2 ("codebook is the
sole source of field classes") would relocate authority **away from that artifact**. Three ways:

| option | shape | consequence |
|---|---|---|
| **A** — codebook carries `field_class`; `field_class3.FIELD_CLASS3` becomes a **derived** read of the codebook | one authority, gate 2 literally met | the `FIELD_CLASSES.md` ↔ module drift test must be rewritten to test codebook ↔ doc; the pre-registration doc stops being the machine-checked source it is today |
| **B** — codebook carries `field_class`; **a test asserts codebook == `FIELD_CLASS3` == `FIELD_CLASSES.md` §2** | gate 2 met for prompt construction; three-way pin, drift impossible | one duplication, held by test — the same discipline already used for `absence_sentinels` ↔ `judge_prompts` |
| **C** — prompts read `field_class3.py` directly; codebook unchanged | zero duplication | **fails gate 2 as written** |

**Recommendation: B.** It satisfies gate 2 where the gate is aimed (prompt construction derives
classes from the codebook, no hand-curated lists) without demoting a pre-registration artifact,
and the drift risk is closed by test rather than by hope — the pattern the codebook already uses.
**Ruling required.**

### C2 — the two paper-variable fields are exactly what this design cannot express

`field_class3.PAPER_VARIABLE = {sample_size, surgical_domain}`, and `FIELD_CLASSES.md` §1.5 says
in terms: a paper-variable field *"must be treated as a hard case by any downstream design that
wants to declare a field's class once, at setup time, rather than per paper."* **This design
declares once, at setup time.** Two specifics:

- **`sample_size`** is STATED on basis *"measured (weak)"* — and §1.5's tiebreak resolves ties
  *toward the more demanding class*, whereas STATED is the **less** demanding one here. It is also
  the **#2 source of `VALUE_WITHOUT_CITATION`** in ELICIT-01 (14 of 44, §5.4). Under the STATED
  contract those 14 become **hard write failures**, since the value is neither cited nor NOT_FOUND.
- **`surgical_domain`** is INFERABLE, so on a clinical paper that names its specialty the model
  will be required to emit a declared inference for something the paper states outright.

Neither is a spec defect; both are predictable smoke findings. **I am not adapting.** Options:
(i) run the ratified classes unchanged and let the smoke measure the cost — **recommended**, it is
the honest measurement and the classes are ratified; (ii) reclassify per §1.5's tiebreak
(`sample_size` → INFERABLE), which is a change to a pre-registration artifact and out of scope.

### C3 — gate 2 ("zero hand-curated field lists, grep-provable") fails **today**, before any change

`engine/agents/extractor.py:172-176` hard-codes five field names in the prompt's Instructions block:

```
  - For **sample_size**: report as a single integer ...
  - For **validation_setting** and **surgical_domain**: if multiple categories apply ...
  - For **system_maturity** and **study_design**: select the single best-fit category ...
```

I checked each of the five against that field's own codebook `instruction`, which
`_build_field_block` already renders into the same prompt. **Four are redundant. One is a
contradiction, and it is a live defect in the current production prompt.**

| field | hand-curated clause | codebook `instruction` | verdict |
|---|---|---|---|
| sample_size | "report as a single integer … if multiple groups, sum them. Example: 4 pigs + 5 phantoms = 9" | same, verbatim | redundant |
| validation_setting | "if multiple categories apply, list all separated by semicolons" | "If multiple settings were used, list all separated by semicolons." | redundant |
| system_maturity | "pick the most advanced stage demonstrated" | "Classify based on the most advanced capability demonstrated" | redundant |
| study_design | "pick the primary design" | "Select the single best-fit category." | redundant |
| **surgical_domain** | **"if multiple categories apply, list all separated by semicolons"** | **"If explicitly testing across multiple specialties, use `Multiple`."** | ⚠ **CONTRADICTION** |

`surgical_domain` has `Multiple` as one of its 10 `valid_values`. The hand-curated clause tells the
model to semicolon-join specialties instead, which produces a value that is **not** an allowed
value — and the very next line of the same Instructions block says *"For all categorical fields:
use ONLY the exact allowed values listed. Do not paraphrase, abbreviate, or combine them."* So the
prompt contradicts the codebook **and itself**, on the one field that is also flagged
**paper-variable** and **INFERABLE** (C2). The clause got there by being written for
`validation_setting`, where semicolon-joining *is* the codebook rule, and picking up
`surgical_domain` alongside it.

Deleting the five clauses therefore makes gate 2 grep-provable **and fixes a real defect**. But it
**changes the production prompt** for reasons partly unrelated to elicitation, on a pipeline whose
prompt history is the subject of five prior reports. **Ruling required:** delete all five (gate 2
clean, defect fixed, prompt changes); delete only the `surgical_domain` mention (minimal defect
fix, gate 2 still fails); or keep and scope gate 2 to *class* derivation only. **Recommendation:
delete all five** — the redundancy makes four of them free, and leaving a self-contradicting
instruction in place while building a design that requires the model to obey per-field contracts
would be the wrong trade. Report it as a finding either way.

### C4 — spec item 7's `1.141×` cannot be applied to a character estimate without double-counting

M2 is correct but is **a ratio between two measured `prompt_eval_count`s** (INDEX/COPY = 1.141×,
report §5.6), not a char→token correction. The same paragraph says the manifest's **character**
estimate predicted 1.03–1.05× for the same inflation, i.e. *a character count of the rendered
numbered text already carries ~4% of it*. So:

- multiplying a char-based estimate **of the rendered numbered prompt** by 1.141× **double-counts**
  the ~1.04× the characters already contain (residual correction is 1.141/1.04 ≈ **1.10×**);
- multiplying a char-based estimate **of the un-numbered prompt** by 1.141× is exactly right.

Recommendation: size the **rendered** prompt with ELICIT-01's own instrument — `WORST_RATIO =
0.4288` tokens/char, *worst observed, deliberately not the median* — and then apply the full
**1.141×** anyway. That over-predicts by ~4%, which is the safe direction for a hard-fail guard,
and it keeps spec item 7's number literal rather than substituting a derived 1.10×. **Stated as an
assumption; flag if the architect wants the 1.10× residual instead.**

### C5 — no home for cited indices in `review.db`, and none is authorized

`evidence_spans` columns are `field_name, value, source_snippet, confidence, tier, audit_status,
auditor_model, audit_rationale, audited_at`. There is **no column for cited unit indices**, and the
task's blast radius makes `review.db` read-only. Spec item 6 persists **unit maps** per paper per
run (files — ELICIT-01's `unit_maps.json` shape), and item 4 puts **invalid** indices in per-call
telemetry. The gap is the **valid** indices of a stored span: without them "every cited index is
auditable after the fact" holds only via the raw Pass-1 response.
**Recommendation:** persist the full parsed per-field citation record (indices, class, declared
inference / steps, NOT_FOUND flag) into `record_call(extra=...)` alongside a per-run unit-map file,
and add **no** DB migration in this task. **Ruling required** — the alternative is a migration,
which is scope creep.

### C6 — two silent decisions the spec leaves open

- **Q4 — does Pass 2 still see the full paper text?** Today it does (§2 fact 2). Spec item 4 says
  the primed context "is built from the materialized evidence" but does not say the paper leaves.
  Keeping both is the smallest change and the safest for fields where the model must still choose
  a categorical value; dropping the paper is the stronger version of the priming hypothesis and
  roughly halves Pass-2 prompt size. **Recommendation: keep the paper text for the smoke** (change
  one thing at a time — the elicitation, not the priming *and* the context), and treat
  "evidence-only Pass 2" as a separate measured arm later. **Ruling required.**
- **Q5 — retire `_validate_and_retry_snippets` for cited fields?** A materialized quote cannot
  contain ellipsis bridging, so the path is dead for them but still live for anything that reaches
  a write without materialization. **Recommendation: leave it in place, untouched** (it costs
  nothing when no span is invalid) and let the fail-fast predicate be the new guard.

---

## 6. What did NOT contradict the spec

- **M1** — 9 STATED fields, `stated_fields()` derivation, 505/505 index validity: consistent with
  the code and the committed report.
- **M3** — `131_072` is the only ceiling anywhere in the tree; `engine/` contains no competing
  constant (grep: zero hits for `131072`, `CEILING`, `prompt_eval_count` under `engine/`).
- **M4/M5** — pre-flight and exit state as recorded.
- **R1** — `docs/session-reports/ELICIT-01_report.md` §5.4 confirmed verbatim, incl. the 44 vs 55
  split and the note that empty-quote cases are *concealed* inside the §5.1 ladder as
  `MISSING_SNIPPET`/`ABSENCE_DECLARED`. This is precisely why the fail-fast must be a **write-boundary
  predicate on the parsed citation record**, not a ladder verdict.
- **R2** — heading units stay citable; no unit-type filtering. `units.py` already excludes only
  Docling comment artifacts, nothing else.
- **I2** — the unit machinery is portable, with the two caveats in §3.
- **I3** — accurate as far as it goes; the four additions are in §2.

## 7. Proposed smoke sample (STEP 2), for approval

3 papers, all `in_corpus` (have Run 6 extractions), one per length stratum, drawn from the
ELICIT-01 manifest so the unit counts are already measured and the fit is already known:

| stratum | paper | parsed chars | units | why |
|---|---:|---:|---:|---|
| short | 121 | 21,348 | 152 | ELICIT-01 smoke paper — directly comparable |
| medium | 604 | 38,794 | 304 | mid-stratum, unremarkable in ELICIT-01 |
| long | 498 | 148,805 | 1,064 | **largest in the sample** — the real sizing test; exercises C4 |

Paper 498 is the one that will show whether the sizing guard is calibrated; papers 415 and 719 stay
excluded (PARSE-01 `MERGED_DOCUMENT` / `EXTRACTION_FAILURE`, both truncate at the ceiling).
**Approval requested** — substitute freely.

---

## 8. STOP

No file under `engine/`, `data/` or the codebook has been modified. Nothing committed. Awaiting
rulings on **§4.1 (NOT_FOUND token)**, **C1 (class authority)**, **C2 (paper-variable fields)**,
**C3 (gate-2 clause deletion)**, **C4 (sizing multiplier)**, **C5 (index persistence)** and
**C6 Q4/Q5**, plus the §7 smoke sample.
