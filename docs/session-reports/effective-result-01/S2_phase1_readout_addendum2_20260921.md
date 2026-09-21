# Addendum 2 — resolution rule v2 (supersedes §4 of S2_phase1_readout.md, 7887a6e)

**Dated** 2026-09-21 (UTC) · **docs-only** · **HEAD at measurement** `9235e6f`, tree clean
`review.db` opened `mode=ro` only; no write. Addendum 1
(`S2_phase1_readout_addendum_20260921.md`) and the read-out itself are **not edited**.

Rule v2 applies the PI's rulings R10–R19 of 2026-09-21, including **R17 in its revised form**.
§4 of the read-out is superseded in whole by §A below; every other section of the read-out
stands, except where a statement here names it.

---

## A. Resolution rule v2

`effective_value(paper, field, arm)` → `(value, field_state, provenance)`.
`effective_state(paper)` → the paper's lifecycle state with who and why.

**`arm` is a required argument** (R18/Q4). There is no default arm and no two-argument form.

**Arms are data, not code** (R12). One arm registry, read by one reader, with **no per-arm
branches**. An arm has a name, a kind (`model` | `human_extractor`) and one pinned configuration
(R10). The number and kind of arms vary by review — zero, one or several human extractor arms
(R12).

**Evaluation is top to bottom; the first matching row wins.** The ordering is itself the rule.
Row 0 is evaluated before everything, including "missing". Reviewer rows sit above supersession,
and the two "unresolved" rows sit above them, so a stale or contested reviewer decision is never
silently resolved in either direction.

**Role gate (R11).** Every event records an actor role: `reviewer` or `extractor` (the latter
model or human). **Rows 3–6 fire only on `reviewer` events.** A human *extractor* arm is an
ordinary arm and its claims are resolved by rows 7–12 exactly as a model arm's are; it can never
override another arm.

**The citation test (R17, revised).** "Asserted with evidence" requires a **citation-located
event against the current claim**, written by one shared deterministic locator, recording the
parsed-text identity checked, the threshold, and the locator version. **Absent a located event,
the state is "asserted without locatable evidence"**, and *snippet supplied: yes/no* is retained
as provenance, not as state. The test is identical for values and for sentinels, for every arm,
and for human extractor source quotes.

| # | situation | inputs the reader sees | effective value | S5b state | provenance returned |
|---|---|---|---|---|---|
| **0** | **Not assigned** (R13) | the cell is outside this arm's assignment | — | **not an S5b state** — returned as **out of scope** | `{arm, assigned: false}`. **Excluded from every denominator.** Model arms default to the full corpus; the assignment table arrives with human arm loading (session 12) |
| 1 | **Missing** | assigned, and no `field_events` row for `(paper, field, arm)` | — | **missing** | `{arm, reason: "assigned, no claim"}` |
| 2 | **Duplicate values within one claim** (R2) | ≥2 `asserted` events, same `claim_id`, different values, retry budget exhausted | — ; **both surfaced** | **unresolved (duplicate values)** | `{arm, claim_id, candidates: […], awaiting: reviewer}` |
| 3 | **Two reviewer decisions on one claim** (R18/Q1) | ≥2 reviewer events against the same `claim_id`, differing in effect | — ; **both surfaced** | **unresolved (needs re-review)** | `{decisions: [{reviewer, at, effect, presented_context_sha256}, …], awaiting: reviewer}`. **Newest does not win.** |
| 4 | **Reviewer decision against a superseded claim** | newest reviewer event's `against_claim_id` ≠ current claim id | — ; **both surfaced** | **unresolved (needs re-review)** | `{reviewer: {...}, current: {value, claim_id}, value_unchanged: <bool>, awaiting: reviewer}` — **R18/Q2**: `value_unchanged` is provenance, never a suppression; the app may offer a one-click re-affirm |
| 5 | **Two claims sharing a pre-manifest configuration** (R10) | ≥2 claims on the cell in this arm, both registered `configuration: "not recorded (pre-manifest)"` | — ; **both surfaced** | **unresolved (needs re-review)** | `{claims: […], reason: "configurations indistinguishable — pre-manifest"}`. **They do not supersede.** |
| 6 | **Reviewer withdrew** (R1) | newest reviewer event `human_withdrew`, against the current claim | — (no value) | **withdrawn** | `{withdrawn_by, at, reason, original_value, original_claim_id}` — the original lives in history, never in the value |
| 7 | **Reviewer corrected** | newest reviewer event `human_corrected`, against the current claim | the reviewer's value | **corrected by human** | `{corrected_by, at, original_value, original_claim_id, presented_context_sha256}` |
| 8 | **Reviewer accepted** | newest reviewer event `human_accepted`, against the current claim | the extractor's value | the underlying state from rows 9–13, **endorsed** | `{extractor provenance} + {accepted_by, at, presented_context_sha256}` |
| 9 | **Superseded within one arm** (R10) | ≥2 claims on the cell in this arm, with **distinguishable configurations**, and the newer arose from a re-extraction whose **input identity changed** (S3d); no reviewer event attached | newest claim's value, then rows 10–13 | per rows 10–13 | `{…, supersedes: [older claim_ids], superseded_because: "input identity changed"}` |
| 10 | **Value, citation located** (R17) | newest claim; value is not a token, not a sentinel; **a `citation_located` event exists against this claim with `located = true`** | the value | **asserted with evidence** | `{arm, claim_id, actor, role, run_id, snippet, located: {parsed_text_id, threshold, locator_version, at}}` |
| 11 | **Value, no located event** (R17) | newest claim; value is not a token, not a sentinel; **no `citation_located` event, or one with `located = false`** | the value | **asserted without locatable evidence** | `{arm, claim_id, actor, role, run_id, snippet_supplied: <bool>, located: null \| {…, located: false}}` |
| 12 | **Absence sentinel, citation located** (R17) | `value ∈ codebook absence_sentinels`; `citation_located` with `located = true` | the sentinel — **it is a value** | **asserted with evidence** | as row 10. **Identical test to row 10**; a sentinel is a claim about the paper's text |
| 12b | **Absence sentinel, no located event** (R17) | `value ∈ codebook absence_sentinels`; no located event, or `located = false` | the sentinel | **asserted without locatable evidence** | as row 11 |
| 13 | **Declined** | `value = escape_token` (`NO_EVIDENCE_LOCATABLE`), zero citations | — (no value) | **declined** | `{arm, claim_id, actor, role, run_id}` — **never skipped**; present in every export and every score carrying this state |
| 14 | **Contract unmet** | `value = contract_unmet_token` (`CONTRACT_UNMET`) | — (no value) | **contract unmet** | `{arm, claim_id, violation_codes, attempts}` — the engine's failure, not the extractor's |
| 15 | **Arms compared, never merged** (R10/R12) | two or more arms each resolving by rows 0–14 | **not resolved across arms** — the reader is per arm and answers per arm | each arm keeps its own state | `{per_arm: {...}}`. **Arms never supersede each other**, including human extractor arms (R11, R14) |
| 16 | **State at migration** (R-f) | only a `state_at_migration` event | today's stored value | the state derived from today's row, tagged | `{source: "state at migration", migrated_at, note: "history not reconstructable from the record"}` |

### Two standing conditions, outside the row order

**Non-codebook fields (R18/Q6).** The reader **ignores** any field not in the codebook. `field_1`
(paper 719) and `Title` (paper 415) therefore never reach this table, in any row, and are absent
from every denominator. Their deletion is recorded as an event in session 12, not performed now.

**Auditor verdicts are provenance (R18/Q7).** `audit_status`, `audit_rationale`, `auditor_model`
and `audited_at` ride in provenance on rows 8–14 and are **never** a field state. Separately, a
span at `flagged` or `contested` **counts as work owed** in derived stage completion, so a stage
is not complete while any assigned cell is in that condition.

---

## B. Clause coverage

Every S2 clause, R1–R3 and R10–R18 against the row that discharges it.

| clause | source | row(s) |
|---|---|---|
| a human decision beats a model value | S2 | 6, 7, 8 — above 9–14, and reviewer-role-gated by R11 |
| human decision against a superseded claim → needs re-review, both shown | S2 | 4 |
| newer extraction supersedes older within one arm unless a human decision is attached | S2 | 9, subordinate to 3–8 |
| arms never supersede each other | S2 | 15 |
| abstention is "declined", never skipped | S2 | 13 |
| withdraw = no value, original kept in history | **R1** | 6 |
| duplicated field → "unresolved: duplicate values" | **R2** | 2 |
| stale import refused, naming the superseding claim | **R3** | read-out §2.4; the reader supplies the current claim id the refusal quotes |
| arms declared per review; changed config = new arm, not a newer claim | **R10** | 9 (the supersession condition), 15 |
| pre-manifest configurations do not supersede → unresolved | **R10** | 5 |
| existing arms registered with "not recorded (pre-manifest)"; nothing backfilled | **R10** | 5, 16 |
| actor role on every event; override rows fire only on reviewer events | **R11** | role gate above the table; rows 3–8 |
| human extractor arms treated exactly like model arms | **R11**, **R14** | 15, and rows 10–12b applying unchanged to them |
| arms are data — one registry, no per-arm branches | **R12** | preamble; row 15 is per-arm by construction |
| partial coverage → "not assigned", out of scope, excluded from denominators | **R13** | **0** |
| "missing" means assigned and absent | **R13** | 1 |
| human workbooks loaded as arms, session 12; migration with a receipt | **R14** | §D below (claim-id implications); no row needed at read time |
| no S4 columns reserved on the event tables | **R15** | §C below |
| cloud `UNIQUE(paper_id, arm)` dropped in session 6 with `load_arm`'s migration | **R16** | enables row 9 for cloud arms; see U4 |
| "asserted with evidence" requires a citation-located event; identical for values, sentinels, human source quotes | **R17** | 10, 11, 12, 12b |
| snippet supplied kept as provenance, not state | **R17** | 11, 12b |
| relevance of a located citation is the judge's question | **R17**, S5c | not a reader row — stated as a non-responsibility |
| two reviewer decisions on one claim → refuse | **R18**/Q1 | 3 |
| `value_unchanged` in provenance, row kept | **R18**/Q2 | 4 |
| arm is a required argument | **R18**/Q4 | preamble; 15 |
| reader ignores non-codebook fields now | **R18**/Q6 | standing condition |
| auditor verdicts are provenance; flagged/contested are work owed | **R18**/Q7 | standing condition |
| Q8, Q9 recorded as state at migration | **R18** | 16 |
| Q10 PI v2 audit is measurement only | **R18** | no row — it writes no field event |
| A11 Option B: session 5 stops writing and deprecates, session 12 drops | **R18** | no row — schema lifecycle, read-out §6 |

---

## C. Event-schema statements changed by these rulings

Read-out §3 stands except as follows. **Schema only; nothing is built, placed or scheduled here.**

**C.1 New event type: `citation_located`** (R17). A `field_events` row with
`event_type = 'citation_located'`, carrying:

| field | notes |
|---|---|
| `claim_id` | the claim whose snippet was tested — the event is **against a claim**, never against a cell |
| `located` | boolean outcome |
| `parsed_text_id` | the identity of the parsed text checked. **Depends on S3e (session 8)**, which is why legacy rows cannot get one yet |
| `threshold` | the numeric threshold in force (see §E on I14 — it is a literal today, not a parameter) |
| `locator_version` | version of the shared locator that produced the verdict |

**Written by the extraction write path, session 9.** The elicited path writes it at extraction
time. **Legacy rows start with no located event**, and upgrading them is a separate dated
locatability measurement after parsed-text identity is settled (S3e, session 8), recorded as new
events and never as reconstructed history.

**C.2 Actor role becomes a first-class column** (R11). `actor_role TEXT NOT NULL CHECK
(actor_role IN ('reviewer','extractor'))`, beside the existing `actor_kind` / `actor_name` /
`actor_digest`. The two are not the same axis: a *human* can be either an extractor (a human arm)
or a reviewer, and rows 3–8 gate on **role**, not on kind.

**C.3 The S4 reservations are withdrawn** (R15). Read-out §3.3 proposed reserving `criterion_id`,
`evidence_offset_start`/`_end` and the three verifier columns on `paper_events`. **They are not
reserved.** S4 screening evidence becomes a **separate table keyed to the event**, added with S4.
Read-out §3.3's paragraph arguing for up-front declaration, and read-out §9's Q5 recommendation,
are both superseded by R15.

**C.4 Arm registry.** Arms cease to be an implicit property of which table a row sits in. The
registry holds `arm_name`, `arm_kind` (`model` | `human_extractor`), and one pinned configuration
per arm. Existing arms — `local`, `anthropic_sonnet_4_6`, `openai_o4_mini_high` — are registered
at migration with `configuration = "not recorded (pre-manifest)"`, which is what makes row 5
reachable. From session 7 a write whose configuration does not match its declared arm is refused
(R10).

**C.5 Assignment table** (R13), a separate table, built with human arm loading in session 12.
Until it exists, model arms default to the full corpus and row 0 never fires.

---

## D. Claim-id implications of R10 and R14 — design statements only

Read-out §2.3 proposed `claim_id = "<arm>:<extraction_uid>:<field_name>"`. R10 and R14 constrain
it in three ways. These are statements of what must hold, not a design to be built here.

**D.1 The arm component becomes a registry key, not a free string.** Under R12 the arm is data,
so the first component of a claim id must be an **arm registry key**, and a claim id is
well-formed only if that key resolves in the review's registry. A claim id naming an unregistered
arm is not a claim id. This also removes the read-out's implicit assumption that `local` is a
literal: it is a registered arm name like any other, and a second review may not have one.

**D.2 A changed configuration produces a new arm, therefore a new claim-id namespace** (R10). The
read-out treated re-extraction under a changed model or prompt as a *newer claim in the same
arm*, which row 9 would then supersede. R10 forbids that: it is a **different arm**. So the
claim-id's stability requirement is narrower than the read-out stated — `extraction_uid` must be
unique within an arm and distinguish re-extractions **within one pinned configuration**, and
nothing about the id needs to encode configuration, because the arm already does.

**D.3 The human key space must be reconciled, not bridged in the id** (R14).
`human_extractions.paper_id` is `TEXT` holding `"EE-NNN"`; `extractions.paper_id` and
`cloud_extractions.paper_id` are `INTEGER … REFERENCES papers(id)`. R14 places the reconciliation
in **session 12's design**, so the claim id does **not** carry a per-arm key space: by the time a
human arm produces claim ids, its `paper_id` is `papers.id` like every other arm's. The read-out's
alternative — declaring that human arms use a different key space — is therefore **closed**, and
addendum 1's note to that effect is superseded by R14.

---

## E. I13 and I14 findings

### I13 — no stored grep result, no locator version. **Confirmed, with one qualification.**

```sql
PRAGMA table_info(evidence_spans);
→ id, extraction_id, field_name, value, source_snippet, confidence, tier,
  audit_status, auditor_model, audit_rationale, audited_at
PRAGMA table_info(cloud_evidence_spans);
→ id, cloud_extraction_id, field_name, value, source_snippet, confidence, tier, notes
```

No column on either span table records a grep result separately from `audit_status`, and none
records an auditor code version. `extractions.auditor_model_digest` exists and is **NULL on all
190 rows** (`SELECT auditor_model_digest, COUNT(*) FROM extractions GROUP BY auditor_model_digest`
→ `(None, 190)`). `cloud_evidence_spans` has **no audit columns at all**, so the cloud arms carry
no audit record of any kind.

**The qualification: `audit_rationale` is free text, and it partially encodes the grep outcome.**

```
SELECT audit_status, COALESCE(audit_rationale,'') FROM evidence_spans;   -- bucketed by prefix
```

| `audit_status` | rationale pattern | n | grep outcome recoverable? |
|---|---|---:|---|
| `contested` | `Grep failed but semantic verified. …` | **449** | **yes — grep FAILED** |
| `verified` | `Field value '<v>' indicates absence — no extraction to audit.` | **313** | **yes — grep NEVER RAN** |
| `verified` | semantic reasoning, no grep marker | **1,846** | **no marker** — a grep pass is only *inferable* |
| `flagged` | semantic reasoning, no grep marker | **1,152** | **no** — `flagged` means semantic fail, grep pass or not |

### I14 — `grep_verify` is pure. **Confirmed. The threshold is not a parameter.**

```python
def grep_verify(source_snippet: str, paper_text: str) -> bool:
    """Check if source_snippet exists in paper_text (exact or fuzzy).

    1. Exact substring match on normalized text.
    2. Sliding window fuzzy match (SequenceMatcher > 0.85).
    """
```

It takes no auditor state, opens no database, makes no model call, and reads no module-level
mutable state. It depends only on `_normalize` and `difflib.SequenceMatcher`. **It can be
extracted as a pure function.**

**But I14's signature `(snippet, text, threshold → result)` is two-thirds right.** The threshold
is a literal in the body — `if ratio > 0.85:` — not an argument and not a named constant.
**R17 requires the located event to record the threshold**, so extraction must promote `0.85` to
a parameter or a versioned constant; a locator that cannot state its own threshold cannot satisfy
R17. Equally, `_normalize` is module-private to the auditor and depends on three module constants
(`_SMART_QUOTES`, `_PUNCT_GLUED_RE`, `_WS_RE`); it must move **with** the locator rather than be
re-implemented beside it, or the two programs diverge on what "the same text" means. **Session 5
designs the extraction; this session only records the constraint.**

### Is any located event reconstructable at migration?

**No, and the default in R17 stands unchanged.** The record proves a grep **failure** for 449
spans and proves that grep **never ran** for 313, but it nowhere proves a grep **pass**: the 1,846
plain-`verified` rows carry only the semantic verdict's reasoning, with no grep marker, so "grep
passed" is an inference from the four-state contract rather than a fact recorded span by span. An
inference is not a located event, and R17 requires each event to record the parsed-text identity
checked, the threshold and the locator version — **none of which exists anywhere on disk**, at any
status. The parsed text those 1,846 were checked against is not identified either, which is
precisely what S3e settles in session 8. **Migration therefore reconstructs no located events at
all**, from any status; every legacy row begins at row 11 or 12b, and the dated locatability
measurement after session 8 writes the first ones as new events.

*(The 449 `contested` rows are the one population where a `located = false` event would be
defensible on the record. They are still not reconstructed, for the same reason: the event's three
required fields are unrecorded, so what would be written is a verdict with no provenance.)*

---

## F. Situations rule v2 still does not decide

| # | undecided | why it is open |
|---|---|---|
| **U1** | **A reviewer decision against a claim in an arm that has since been retired or re-pinned** (R10 makes a configuration change a new arm). The old claim is not superseded — it belongs to an arm that no longer receives writes. Row 4 does not fire, because nothing superseded it; row 8 endorses a claim from a dormant arm. | R10 creates arm lifecycle (registered, active, retired) and the rule has no lifecycle vocabulary |
| **U2** | **Whether a reviewer may decide on a cell that is "not assigned" in that arm** (row 0). Row 0 returns out-of-scope before any reviewer row is reached, so a decision recorded against such a cell is unreachable rather than refused. | R13 defines assignment for extractors; it does not say whether reviewer scope follows it |
| **U3** | **Cross-arm reviewer decisions.** Row 15 says arms are compared, never merged, and rows 3–8 are per arm. A reviewer who adjudicates "which arm is right" for a cell has no event shape. | S2 never claimed to resolve across arms; S5c's judge and the PI audit occupy this space, and whether their output is ever a *value* is unsettled (cf. R18/Q10, which says the v2 audit is measurement only) |
| **U4** | **What the reader returns for a cloud cell with two claims between session 5 and session 6.** R16 drops `UNIQUE(paper_id, arm)` in session 6; until then cloud arms cannot hold two claims, so row 9 and row 5 are unreachable for them. After the drop, both become reachable, and which applies depends on whether the pre-manifest registration (C.4) makes the two configurations indistinguishable — **it does**, so a second cloud claim lands on **row 5**, not row 9. | Recorded rather than ruled: this means a cloud re-extraction immediately after session 6 yields *unresolved (needs re-review)* rather than supersession, until session 7 pins configurations. That may be the intent, but it is a consequence of R10 + R16 together that neither ruling states |
| **U5** | **Ordering between row 2 (duplicate within a claim) and row 3 (two reviewer decisions)** when both hold. v2 puts duplicate first, so the cell reports "duplicate values" and the reviewer conflict is invisible until the duplicate is resolved. | Arbitrary; both are "unresolved", and the provenance could carry both |
| **U6** | **Whether a `citation_located` event with `located = false` should differ, in state, from no event at all.** v2 collapses them into rows 11 / 12b. | R17 says "without a located event, the state is asserted without locatable evidence" and treats a false verdict identically. Distinguishing "tested and not found" from "never tested" is a provenance distinction today; making it a state distinction would be a ninth S5b value |

---

## G. Three draft inventory rows

**Drafts only. The inventory tables in `docs/plan/ENGINE_REFACTOR_PLAN.md` are not edited by this
session;** these are appended to the plan at this session's closeout, per the ruling.

### A12 — `concordance.load_arm` returns an empty dict for a human arm, silently

| | |
|---|---|
| **Problem** | `load_arm` has two branches, `if arm == "local": … else: <cloud>`. A `human_*` arm falls into the cloud branch, queries `cloud_extractions WHERE arm = 'human_A'`, matches nothing, and returns `{}` with no error. `engine/validators/distribution_monitor.py::_query_values` — *"Route value query to the right table based on arm name."* — has **three** branches and routes `arm.startswith("human_")` to `human_extractions`. Two components, one predicate, divergent. |
| **Evidence** | addendum 1 M-d. `load_arm`'s own docstring blesses the result: *"empty dict when no data exists for the arm (valid result)"*. `docs/architecture/pipeline.md` and `docs/architecture/modules.md` both document the `human_extractions` branch that does not exist; the latter also promises *"never returns empty dict on failure"*, true of a DB error and false of a human arm. |
| **On disk** | **LATENT** — no human arm has been loaded, and `human_extractions` does not exist. **ARMED** the moment R14's session-12 load runs, if `load_arm` is still the reader. |
| **Closes** | **session 6**, when `load_arm` moves behind the reader and the arm registry (R12) becomes the single routing predicate. The two `docs/architecture/` corrections are recorded as session-6 hygiene; this session was forbidden to touch them. |

### B6 — the auditor's absence branch auto-verifies without any text check, from a hand-list divergent from the codebook

| | |
|---|---|
| **Problem** | `engine/agents/auditor.py::audit_span` returns `"verified"` **before `grep_verify` is ever called** for any value in a four-item hand-list: `_ABSENCE_VALUES = {"NOT_FOUND", "Not discussed", "NR", "No comparison reported"}`. That list is **not** the codebook's six `absence_sentinels` (`NR`, `N/A`, `NA`, `NOT_FOUND`, `NOT FOUND`, `NOT REPORTED`): it omits four of them and adds two values the codebook does not recognise as absences at all. A second, further-diverged copy exists in the same file with `"Not assessable"` added. |
| **Evidence, measured** | **313 of the 2,159 `verified` spans (14.5%) carry the rationale `Field value '<v>' indicates absence — no extraction to audit.`** and were therefore never text-checked. Breakdown: `NR` **176**, `NOT_FOUND` **6** (both codebook sentinels, 182 together — matching the sentinel × status cross-tab of verified 182 / flagged 1, the one `flagged` being the single `N/A`, which is **not** in the auditor's list), and **`'No comparison reported'` 131**, which is **not a codebook sentinel** and is therefore an ordinary **value** under rule v2 — auto-verified on a hand-list membership. Separately, `SEMANTIC_ONLY_TIERS = {4}` sets `grep_pass = True` unconditionally; **measured, every span in `evidence_spans` is tier 1**, so this path has **zero live rows** and is a latent hazard rather than a present contaminant. |
| **Consequence** | **`verified` does not imply *located*.** Any reading of the 2,159 figure as "2,159 spans whose evidence was found in the paper" is wrong by at least 313. Both PI-audit sampling frames stratify on verdicts derived from this status, and the 131 `'No comparison reported'` spans are values, not absences, so they were exempted from a check they were owed. |
| **On disk** | **WRONG** — 313 rows carry a verdict that was not earned by the test its four-state contract describes. |
| **Closes** | **provisionally in session 9** (the write path, where R17's locator and the shared absence predicate land) and **must precede Run 7's audit pass**, or Run 7 reproduces it. |
| **Cross-reference** | The file's own comment already records the two-hand-list divergence as **fix-phase item N2** — *"the two `_ABSENCE_VALUES` hand-lists in this file already have (that divergence is recorded fix-phase item N2 and is deliberately untouched)"*. B6 is the wider defect: N2 is the two lists disagreeing with **each other**, B6 is either list disagreeing with **the codebook**, and the auto-verify that follows from it. |

### D7 — the extraction reuse key cannot match on any existing row

| | |
|---|---|
| **Problem** | `engine/agents/extractor.py` skips a paper on `"SELECT id FROM extractions WHERE paper_id = ? AND codebook_hash = ?"`, logging *"Paper %d: already extracted with current schema — skipping"*. `codebook_hash` is **NULL on all 190 rows**, and `NULL = <anything>` is never true in SQL. **The skip can never fire for any paper in this database.** The key is correctly implemented against a column nothing populated. |
| **Evidence** | addendum 1 M-c: `SELECT codebook_hash, COUNT(*) FROM extractions GROUP BY codebook_hash` → `(None, 190)`; same for `codebook_sha256`, and for both columns on all 379 `cloud_extractions` rows. |
| **On disk** | **ARMED** — the data is correct; the next local extraction run is the event. A re-run would **re-extract all 190 papers** rather than skip them, at full model cost, and under R10 the resulting claims would carry a configuration indistinguishable from the originals', so rule v2 **row 5** would put every re-extracted cell into *unresolved (needs re-review)* rather than superseding. |
| **Interim control** | **R19** — no local extraction runs on corpus papers until S3d lands. This is an operational invariant, not a code change, and it is the only thing standing between the current state and 190 unresolved papers. |
| **Closes** | **S3d, session 8** — the reuse key becomes `(paper_id, parsed_text_hash, codebook_hash, prompt_hash, model_digest, options_hash)` and the columns behind it are populated. |
