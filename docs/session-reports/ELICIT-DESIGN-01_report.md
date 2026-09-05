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

---
---

# END OF TASK — STEP 1 implementation + STEP 2 smoke

**Appended, not edited.** Section 8 above ("STOP") recorded the state at STEP 0 hand-off and
stands as written; everything below is what happened after sign-off.

**HEAD:** `336bd76` + this report. **Smoke:** `smoke_20260903T155654Z`, **0/3 papers stored.**
**Ollama:** 0.21.0, digest `edba8017331d`, `NRestarts=0`, `ExecMainStartTimestamp` unchanged
(2026-08-31 00:41:59 UTC) across both smoke runs. Flock acquired and released by the run itself.

---

## 9. Acceptance gates

| # | gate | status |
|---|---|---|
| 1 | STEP 0 report delivered; sign-off before STEP 1 | ✅ committed `b05380e` before any implementation (D1) |
| 2 | Codebook is the sole source of field classes; zero hand-curated field lists, grep-provable | ✅ `tests/test_elicitation_codebook.py` asserts no prompt builder names a codebook field, and that adding a field to the codebook reaches its class's contract with no code change. `tests/test_prompt_no_hardcoded_fields.py` pins the same for the legacy builder |
| 3 | Contract shapes enforced by parsing — a violating response is detected, not absorbed | ✅ 33 tests in `tests/test_elicitation_contracts.py`. Demonstrated live: **9/9 smoke attempts** surfaced their violations by code and refused the write |
| 4 | Fail-fast demonstrably fires: an uncited-value write is impossible | ✅ `tests/test_citation_guard.py` (12) + pipeline tests. Demonstrated live: 0/3 papers stored, `review.db` untouched |
| 5 | Smoke 3/3 papers complete; report delivered; STOP | ❌ **0/3 stored.** Every paper exhausted its 3-attempt budget on Pass-1 contract violations. Report delivered; stopping |

Gate 5 fails. Gates 1–4 pass, and gate 5 fails **because** gates 3 and 4 work: nothing was
stored, and the reason each paper was refused is recorded per field.

---

## 10. Smoke — per paper

| paper | chars | units | attempts | wall | outcome | final violation set |
|---|---:|---:|---:|---:|---|---|
| 121 | 21,348 | 152 | 3/3 | 581 s | `contract_exhausted` | 6 × `VALUE_WITHOUT_CITATION`, 1 × `STEPS_MISSING` |
| 604 | 38,794 | 304 | 3/3 | 665 s | `contract_exhausted` | 1 × `VALUE_WITHOUT_CITATION`, 3 × `STEP_WITHOUT_BASIS`, 1 × `STEPS_MISSING` |
| 498 | 148,805 | 1,064 | 3/3 | 837 s | `contract_exhausted` | 11 × `VALUE_WITHOUT_CITATION`, 1 × `INFERENCE_MISSING`, 1 × `STEPS_MISSING` |

Total 2,083 s (34.7 min), 9 Pass-1 calls, **zero Pass-2 calls** — every paper failed before Pass 2,
which is the design working: a doomed extraction does not spend a second 32B call.

### 10.1 Per field, final attempt

Legend: cites = valid citations · bad = invalid indices · esc = escape token used ·
inf = declared inference present · stp = reasoning steps.

**p121** — 13/20 fields met their contract.

| field | class | cites | bad | esc | inf | stp | result |
|---|---|---:|---:|:-:|:-:|---:|---|
| study_type | stated | 1 | 0 | · | · | 0 | ok |
| robot_platform | stated | 3 | 0 | · | · | 0 | ok |
| task_performed | stated | 3 | 0 | · | · | 0 | ok |
| **sample_size** | stated | **0** | 0 | · | · | 0 | `VALUE_WITHOUT_CITATION` |
| validation_setting | stated | 2 | 0 | · | · | 0 | ok |
| **primary_outcome_metric** | stated | **0** | 0 | · | · | 0 | `VALUE_WITHOUT_CITATION` |
| **primary_outcome_value** | stated | **0** | 0 | · | · | 0 | `VALUE_WITHOUT_CITATION` |
| **comparison_to_human** | stated | **0** | 0 | · | · | 0 | `VALUE_WITHOUT_CITATION` |
| **secondary_outcomes** | stated | **0** | 0 | · | · | 0 | `VALUE_WITHOUT_CITATION` |
| surgical_domain | inferable | 2 | 0 | · | Y | 0 | ok |
| task_monitor | inferable | 1 | 0 | · | Y | 0 | ok |
| task_generate | inferable | 1 | 0 | · | Y | 0 | ok |
| task_select | inferable | 1 | 0 | · | Y | 0 | ok |
| task_execute | inferable | 1 | 0 | · | Y | 0 | ok |
| **country** | inferable | **0** | 0 | · | Y | 0 | `VALUE_WITHOUT_CITATION` |
| autonomy_level | judgment | 1 | 0 | · | · | 2 | ok |
| system_maturity | judgment | 1 | 0 | · | · | 2 | ok |
| study_design | judgment | 1 | 0 | · | · | 2 | ok |
| **key_limitation** | judgment | 1 | 0 | · | · | **0** | `STEPS_MISSING` |
| clinical_readiness_assessment | judgment | 2 | 0 | · | · | 2 | ok |

**p604** — 15/20 fields met their contract.

| field | class | cites | bad | esc | inf | stp | result |
|---|---|---:|---:|:-:|:-:|---:|---|
| study_type | stated | 1 | 0 | · | · | 0 | ok |
| robot_platform | stated | 2 | 0 | · | · | 0 | ok |
| task_performed | stated | 2 | 0 | · | · | 0 | ok |
| sample_size | stated | 3 | 0 | · | · | 0 | ok |
| validation_setting | stated | 2 | 0 | · | · | 0 | ok |
| primary_outcome_metric | stated | 2 | 0 | · | · | 0 | ok |
| primary_outcome_value | stated | 3 | 0 | · | · | 0 | ok |
| **comparison_to_human** | stated | **0** | 0 | · | · | 0 | `VALUE_WITHOUT_CITATION` |
| secondary_outcomes | stated | 2 | 0 | · | · | 0 | ok |
| surgical_domain | inferable | 2 | 0 | · | Y | 0 | ok |
| task_monitor | inferable | 2 | 0 | · | Y | 0 | ok |
| task_generate | inferable | 2 | 0 | · | Y | 0 | ok |
| task_select | inferable | 2 | 0 | · | Y | 0 | ok |
| task_execute | inferable | 2 | 0 | · | Y | 0 | ok |
| country | inferable | 1 | 0 | · | Y | 0 | ok |
| autonomy_level | judgment | 1 | 0 | · | · | 2 | ok |
| **system_maturity** | judgment | 2 | 0 | · | · | 2 | `STEP_WITHOUT_BASIS` |
| **study_design** | judgment | 2 | 0 | · | · | 2 | `STEP_WITHOUT_BASIS` |
| **key_limitation** | judgment | 1 | 0 | · | · | **0** | `STEPS_MISSING` |
| **clinical_readiness_assessment** | judgment | 1 | 0 | · | · | 2 | `STEP_WITHOUT_BASIS` |

**p498** — 7/20 fields met their contract. The long paper degrades hard, and specifically:
**every citation it does produce is valid, and it simply stops citing** on 11 of 20 fields.

| field | class | cites | bad | esc | inf | stp | result |
|---|---|---:|---:|:-:|:-:|---:|---|
| study_type | stated | 1 | 0 | · | · | 0 | ok |
| robot_platform | stated | 2 | 0 | · | · | 0 | ok |
| task_performed | stated | 2 | 0 | · | · | 0 | ok |
| **sample_size** | stated | **0** | 0 | · | · | 0 | `VALUE_WITHOUT_CITATION` |
| validation_setting | stated | 2 | 0 | · | · | 0 | ok |
| primary_outcome_metric | stated | 1 | 0 | · | · | 0 | ok |
| primary_outcome_value | stated | 2 | 0 | · | · | 0 | ok |
| **comparison_to_human** | stated | **0** | 0 | · | · | 0 | `VALUE_WITHOUT_CITATION` |
| secondary_outcomes | stated | 2 | 0 | · | · | 0 | ok |
| **surgical_domain** | inferable | **0** | 0 | · | Y | 0 | `VALUE_WITHOUT_CITATION` |
| **task_monitor** | inferable | **0** | 0 | · | Y | 0 | `VALUE_WITHOUT_CITATION` |
| **task_generate** | inferable | **0** | 0 | · | Y | 0 | `VALUE_WITHOUT_CITATION` |
| **task_select** | inferable | **0** | 0 | · | Y | 0 | `VALUE_WITHOUT_CITATION` |
| **task_execute** | inferable | **0** | 0 | · | Y | 0 | `VALUE_WITHOUT_CITATION` |
| **country** | inferable | 1 | 0 | · | **·** | 0 | `INFERENCE_MISSING` |
| **autonomy_level** | judgment | **0** | 0 | · | · | 1 | `VALUE_WITHOUT_CITATION` |
| **system_maturity** | judgment | **0** | 0 | · | · | 1 | `VALUE_WITHOUT_CITATION` |
| **study_design** | judgment | **0** | 0 | · | · | 1 | `VALUE_WITHOUT_CITATION` |
| **key_limitation** | judgment | 1 | 0 | · | · | **0** | `STEPS_MISSING` |
| **clinical_readiness_assessment** | judgment | **0** | 0 | · | · | 1 | `VALUE_WITHOUT_CITATION` |

### 10.2 Violation totals, all 9 attempts (180 field entries)

| code | severity | n | by class |
|---|---|---:|---|
| `VALUE_WITHOUT_CITATION` | fatal | **54** | stated 24 · inferable 21 · judgment 26 (fatals overall) |
| `STEPS_MISSING` | fatal | 8 | judgment |
| `STEP_WITHOUT_BASIS` | fatal | 6 | judgment |
| `INFERENCE_MISSING` | fatal | 3 | inferable |
| `INDEX_MALFORMED` | fatal | **0** | — |
| `INDEX_OUT_OF_RANGE` | fatal | **0** | — |
| `ESCAPE_WITH_CITATION` | fatal | **0** | — |
| `FIELD_MISSING` | fatal | **0** | — |
| `DUPLICATE_INDICES` | advisory | **0** | — |
| `VALUE_BEFORE_EVIDENCE` | advisory | **0** | — |

**Index validity was perfect.** Zero malformed, zero out-of-range, zero duplicates across every
index in nine responses — consistent with ELICIT-01's 505/505 (M1). Parse path was `direct` on
**9/9**; the marker-token backstop never fired. Ordering was honoured on every entry.

**The whole failure is the accompaniment, not the citation mechanism.** The mechanism the
architect ratified works. What the model will not reliably do is cite for *every* field, supply an
inference for every INFERABLE, and decompose every JUDGMENT into based steps.

---

## 11. Nine verbatim materialized evidence samples (one per class per paper)

These are engine-materialized unit text, resolved from the persisted unit map — not model output.

### p121

**`robot_platform` [STATED]** — value `"da Vinci Surgical System; sentire surgical system; SurRoL simulator"` → **ok**
```
[S37] "The system autonomously completed all 17 required tasks, achieving a 100 % success
       rate across eight different ex vivo gallbladders without any human intervention."
[S69] "The power of modern AI, particularly foundation models, is derived from training on
       massive, diverse datasets."
[S87] "The power of this approach was demonstrated through extensive validation across seven
       skill training tasks on the da Vinci Research Kit (dVRK) and five surgical assistive
       tasks for the commercial sentire surgical system in ex vivo settings."
```
⚠ Contract-valid, evidentially mixed: S87 supports the value, **S69 is about foundation-model
training data and supports nothing**. This is R2's ruling biting exactly where it was expected —
a valid citation is not necessarily good evidence, and judge-scored supportedness is the follow-on
task, not this one.

**`task_monitor` [INFERABLE]** — value `"R"` → **ok**
```
[S58] "The HL policy can recognize when the LL policy has entered a suboptimal or failure
       state and generate corrective language instructions, such as 'move left' or 'grab top'."
INFERENCE: The robot uses language commands and visual inputs to self-correct, indicating
           independent sensing.
```
This is the INFERABLE contract doing precisely what it was designed for: no paper writes "R", and
the inference names the step from the architecture description to the code.

**`clinical_readiness_assessment` [JUDGMENT]** — value `"Proof of concept only"` → **ok**
```
STEP 1 (cites S37, S87): Demonstrations are on ex vivo and animal models.
STEP 2 (criteria application, no textual basis claimed): No clinical deployment or regulatory
                                                          clearance mentioned.
```
The mixed-basis JUDGMENT shape working as specified — one cited step, one honest criteria step.

### p604

**`primary_outcome_value` [STATED]** — value `"Sphere Task: ~80%; Cutting Task: ~76-80%; Physical trials: 5/7"` → **ok**
```
[S198] "For Sphere Task, Fig. 6 shows that our method achieves close to 80 percent task success
        rate, upper-bounded by the 85 percent task success rate achieved by the policy trained
        on the ground-truth (GT) reward."
[S199] "For Cutting Task, Fig. 6 shows that our method achieves 80 percent task success rate,
        upperbounded by the almost 90 percent task success rate achieved by the policy trained
        on GT reward."
[S225] "The robot successfully accomplished both reaching the attachment points and executing
        the cutting task in 5 of the 7 trials."
```
Three citations, each supporting a distinct clause of the value. The best evidence in the run.

**`country` [INFERABLE]** — value `"USA"` → **ok**
```
[S22] "Robotics Center and the Kahlert School of Computing at the University of Utah, Salt Lake
       City, UT 84112, USA; (email: {...}@utah.edu)."
INFERENCE: Authors' affiliation is University of Utah, Salt Lake City, UT, USA.
```
Exactly the paratext case FIELD_CLASSES.md §1.2 describes and measured `country` on (19 of 20).

**`autonomy_level` [JUDGMENT]** — value `"2 (Task autonomy)"` → **ok**
```
STEP 1 (cites S175): Robot executes tasks without continuous human control.
STEP 2 (criteria application): Uses learned reward function for task execution.
[S175] "Our action space is the space of end-effector position so that RL can be more sample
        efficient and"
```
⚠ Contract-valid, weak: S175 is a **truncated sentence about action-space design** and does not
establish "without continuous human control". Same R2 caveat as p121's S69.

### p498

**`validation_setting` [STATED]** — value `"Phantom/Simulation; Ex vivo"` → **ok**
```
[S739] "Figure 26 illustrates the frames of task execution process on both the artificial and
        porcine tissue viewing from the sensing (left) camera."
[S750] "Suturing the porcine tissue owns lower success rate, as the tissue surface might
        encounter irreversible deformation after many stitches."
```

**`country` [INFERABLE]** — value `"Hong Kong, China"` → **`INFERENCE_MISSING`**
```
[S32] "AB1, CUHK, Shatin, N.T., HKSAR, Hong Kong, China."
```
The citation is perfect and the value is right. The contract still fails, because the INFERABLE
class requires the inference to be *declared* and the model declared none. Reasonable people will
read this two ways, and it is the sharpest single illustration of §13's question.

**`key_limitation` [JUDGMENT]** — value `"Reliance on pre-calibrated transformations without online updates."` → **`STEPS_MISSING`**
```
[S826] "Once it could be updated to minimize the residual positioning error, the motion accuracy
        apart from the planning and control framework could be further improved, which is
        currently our ongoing work."
```
A well-chosen citation and a defensible limitation, refused for having no reasoning steps.
`key_limitation` failed this way on **5 of 6** attempts where it was reached — the single most
consistent failure in the run.

---

## 12. The findings

### F1 — The escape token was never used. Not once.

**`NO_EVIDENCE_LOCATABLE`: 0 uses in 180 field entries.** `NR` was used 23 times. The model has a
sentinel habit and the new token has no purchase against it.

This matters more than it looks. Under the §4.1 ruling a sentinel is a *value* that owes a
citation, so a model that reaches for `NR` instead of the escape token converts "I found nothing"
into a claim it must then evidence — and fails. **19 of the 54 `VALUE_WITHOUT_CITATION` firings
are uncited `NR`.** The escape hatch the design built for exactly this case was never taken.

Reported as a measurement, not fixed (ruling §3 / FINAL REPORT).

### F2 — 35 of the 54 uncited values are REAL values, not sentinels.

This is R1's failure mode, alive and being caught. Examples, verbatim:

| paper | field | class | value asserted with zero citations |
|---|---|---|---|
| p121, p604 (×5) | comparison_to_human | stated | `"No comparison reported"` |
| p498 | surgical_domain | inferable | `"General Surgery"` |
| p498 | autonomy_level | judgment | `"Task autonomy"` |
| p498 | task_monitor | inferable | `"R"` |

`"No comparison reported"` is the striking one: it is an absence claim written as free text, so it
is neither a sentinel nor an evidenced value — exactly the class `analysis/provenance/absence.py`
enumerates as `P2_bare_no_np`. The guard refuses it. **Before this design it would have been
stored**, and the ELICIT-01 report says such cases were *concealed* in the ladder as
`MISSING_SNIPPET`. The engine now refuses to store what it previously mislabelled.

### F3 — The failure concentrates on the fields ELICIT-01 already named.

ELICIT-01 §5.4's uncited-assertion table listed `comparison_to_human` (20), `sample_size` (14),
`secondary_outcomes` (6), `primary_outcome_value` (2). The smoke's top offenders are
`comparison_to_human` (9 firings, the maximum possible — every attempt of every paper),
`sample_size` (6), then a long flat tail. **This is not a new failure. It is the same failure,
now fatal instead of silent**, and its rank order reproduces.

### F4 — C2's paper-variable prediction landed, and split by paper as predicted.

`sample_size` (STATED, basis "measured (weak)") was uncited on **p121 and p498** and cleanly cited
with three units on **p604**. `surgical_domain` (INFERABLE) was fine on p121/p604 and uncited on
p498. That is precisely what "paper-variable" means, observed rather than argued: the same field,
the same contract, different papers, different outcomes. FIELD_CLASSES.md §1.5's warning that a
declare-once design must treat these as hard cases is now a measurement.

### F5 — `key_limitation` cannot satisfy the JUDGMENT contract.

`STEPS_MISSING` on 5 of 6 attempts, on all three papers. The model returns a limitation with a
citation and no steps. Note `key_limitation` is the one field whose codebook instruction says
*"state the key limitation using YOUR judgment. Do not simply copy what the authors say"* and which
carries `source_quote_required: true` — the codebook asks for a quote, and the class contract asks
for steps, and the model does the first.

### F6 — Long papers degrade by ceasing to cite, not by citing badly.

p498 (1,064 units) produced **11 uncited fields and zero invalid indices**. It did not hallucinate
unit numbers, did not drift out of range, did not stitch. It stopped citing. Whatever the
mechanism, the failure is abstention under length, not fabrication under length — which is the
better of the two failure modes and is worth knowing before Run 7 sizing.

### F7 — Retrying an identical request does not help, and sometimes hurts.

| paper | failures per attempt |
|---|---|
| 121 | 6 → 7 → 7 |
| 604 | **2 → 5 → 5** |
| 498 | 13 → 13 → 13 |

p604's first attempt was two fields short of a complete extraction and its second was five. The
retry policy — inherited unchanged from the completeness guard, where "re-issue the identical
request" is right because the failure is response *shape* — is a poor fit for a contract failure,
where the failure is response *content* at temperature 0. Nine calls bought nothing over three.
**Not changed in this task** (out of scope: retry harmonization is queued separately), but it is a
real finding and Run 7 should not inherit it unexamined.

### F8 — Index validity and parse reliability are not the problem.

0 malformed, 0 out-of-range, 0 duplicates, 0 ordering violations, `direct` parse 9/9, tripwire 0.
The unit-index mechanism the architect ratified is doing its job. Every failure in this smoke is
about what accompanies a citation, or about a field that produced none.

---

## 13. What this leaves for the architect

The design works and the corpus does not meet it. **0/3 is not evidence the mechanism is broken —
gates 3 and 4 are exactly why nothing was stored.** The open question is a policy one and it is
not mine to settle:

- **Is a per-field contract the right granularity for a write-boundary refusal?** Today one
  uncited field out of twenty fails the whole paper. p604 lost a 15/20-clean extraction over
  `comparison_to_human` and `key_limitation`. The alternative — store the contract-meeting fields
  and refuse only the offenders — collides with the completeness guard, which requires all twenty.
  These two guards now disagree about what a paper is, and something has to give.
- **Should the escape token be taught, not just offered?** F1 says the model does not reach for
  it. Prompt-salience work was explicitly out of scope for this task.
- **`key_limitation` (F5) and `country`-without-inference (p498) look like contract/codebook
  mismatches**, not model failures. Both are arguably the contract asking for the wrong thing.

## 14. Corrections carried forward (append-only, per D-directive 6)

**(a) The estimator claim in commit `bbfd7f7` is wrong as written.** It says the C4 estimator
"OVER-predicts by roughly 4%". The ~4% figure describes the marker-inflation term alone;
`WORST_RATIO = 0.4288` dominates it, and the real over-prediction is about **2×**. Measured across
all nine attempts, estimate vs. the runtime's own `prompt_eval_count`:

| paper | prompt chars | estimated | actual | over-prediction |
|---|---:|---:|---:|---:|
| 121 | 56,308 | 24,145 | 11,440 | **2.11×** |
| 604 | 77,102 | 33,061 | 16,798 | **1.97×** |
| 498 | 206,819 | 88,682 | 46,855 | **1.89×** |

Direction is safe (a hard-fail guard must over-predict) and the guard never wrongly refused a
paper. But **the magnitude was misstated by roughly fiftyfold**, and the ratio drifts downward
with length, so headroom on the largest papers is understated most. The commit message stands in
history; this is the correction of record, and the future cross-arm input-fit guard should inherit
**~2×, not ~4%**. Tripwire fired **0/9**; the largest real prompt used 46,855 of 131,072 tokens
(36%), so p498 is nowhere near the ceiling — the earlier worry about it was unfounded.

**(b) The aborted first smoke** (`smoke_20260903T153852Z`, artifacts preserved unmodified at
`eval/elicit_design01/aborted_smoke_20260903T153852Z/`). Killed by architect ruling after p121 and
one p604 attempt; **not run to exhaustion**. p121 failed 3/3 with `parse=unparseable` — the model
emitted bare `[S1]` markers in index lists, which is invalid JSON, so all 20 fields surfaced as
`FIELD_MISSING`. Cause: the three-class prompt rewrite dropped ELICIT-01's load-bearing line *"Use
the integer only, not the '[S12]' marker."* Fixed in `1ba2d58`, with a backstop that turns a
regression into `INDEX_MALFORMED` rather than a lost response. The defect was **not universal** —
p604's one attempt parsed `direct` — which is why the backstop earns its place. The re-run shows
the fix held: `direct` on 9/9, marker backstop never fired.

**(c) `_validate_and_retry_snippets` is structurally dead on the elicited path** (C6-Q5 note). A
materialized unit quote cannot contain ellipsis bridging, so the function's retry branch is
unreachable for cited fields. Left untouched per ruling; retirement is a later cleanup task.

**(d) One deviation from "port unchanged"** in `units.py`: `resolve()`'s guard was
`not isinstance(index, int)`, which accepts `True` (bool subclasses int) and would resolve it to
unit 1. ELICIT-01 never hit it because its analyzer rejected bools before calling resolve; here
resolve is the only gate, so it reaches the same verdict one layer earlier.

**(e) `MIN_UNIT_TOKENS = 3` is an ELICIT-01 study artifact adopted provisionally**, per the PORT
ruling — recorded in the module docstring and pinned by test. Re-derivation belongs to the queued
parse-quality-gate task.

**(f) Anchored rates from this path will not be comparable to Run 6's.** The stored snippet is
engine-materialized and therefore ANCHORED by construction — ELICIT-01's own caution about
reporting "100% ANCHORED", now structural. Citation validity, contract-violation counts and
judge-scored supportedness are the measures that carry information.

**(g) One unplanned harness fix.** `build_scratch` cleared a single shared `scratch/` at startup,
so the re-run would have destroyed the aborted run's telemetry — the evidence for its own finding —
before anyone read it twice. Scratch is now keyed by run id and refuses to reuse one (`336bd76`).

---

## 15. Tests and invariants

| | |
|---|---|
| baseline (M5) | 1,556 offline passed, 15 deselected |
| new this task | **80** |
| final offline gate | **1,636 passed, 15 deselected** (206 s) — 1,556 + 80, exact |
| `review.db` | **not written** — read-only `immutable=1` for three papers' metadata; smoke wrote only its own gitignored scratch DB |
| `parsed_text` | read-only |
| Ollama | 0.21.0 throughout, `NRestarts=0`, `ExecMainStartTimestamp` unchanged; config, unit and version untouched |
| experiment flock | acquired and released by each run; released after the abort (verified with `fuser`) |
| eval store `eval/elicit01/` | read only; never written |

New tests by file: `test_codebook_field_class.py` 6 · `test_units_and_sizing.py` 8 ·
`test_elicitation_contracts.py` 33 · `test_elicitation_codebook.py` 8 ·
`test_citation_guard.py` 12 · `test_elicitation_pipeline.py` 11 ·
`test_prompt_no_hardcoded_fields.py` 2.

---

## Addendum (2026-09-05): the status header is stale, per ELICIT-DESIGN-02

**Superseded:** the header at line 5 — *"**Status:** STEP 0 delivered. **STOPPED for
architect sign-off.** No implementation performed."*

That line was true when it was written and stopped being true 80 minutes later, inside the
same session. It was never revised, and line 5 is not edited now: this report is append-only,
and rewriting a header to match the body would erase the fact that the session ran past its
own stopping point. The correction belongs here, where it is dated.

**What the report actually contains.** §§0–8 are STEP 0 as the header describes. From line 359
— *"# END OF TASK — STEP 1 implementation + STEP 2 smoke"* — the report continues into the
delivered implementation and the three-paper smoke: acceptance gates (§9), per-paper and
per-field smoke results (§§10–11), findings F1–F8 (§12), the architect's open questions (§13),
carried-forward corrections (§14) and the test ledger (§15). The task shipped 80 new tests,
moved the offline gate 1,556 → 1,636, and was committed and pushed. A reader who stops at
line 5 concludes the opposite of all of it.

**Verified independently.** VERIFY-EXIT-01 (2026-09-04) re-derived the smoke numbers from
telemetry rather than from this report: 3 papers × 3 attempts = 9, 0 stored, 180 field
entries, violation totals 54 / 8 / 6 / 3 exactly as §10.2 states, index mechanics zero-defect,
escape-token uses 0 as F1 states. The body's measurements survive contact with disk. Only the
header does not.

**Two framing notes for anyone citing this report.**

- §13 frames **three** open questions, not four. The retry-policy question is raised in **F7**
  and explicitly deferred there — *"Not changed in this task (out of scope: retry harmonization
  is queued separately)"* — rather than put to the architect in §13.
- The findings run **F1–F8**. F8 (index validity and parse reliability are not the problem) is
  easy to miss in a list described elsewhere as F1–F7, and it is the finding that says the
  ratified citation mechanism works.

**Disposition of the findings.** ELICIT-DESIGN-02 implements architect rulings against F1
(escape-token teaching), F5 (the `key_limitation` codebook/contract collision), F7 (the retry
policy) and §13's first question (refusal granularity). F2, F3, F4 and F6 are measurements
carried forward unchanged; F8 remains the reason the mechanism was kept.
