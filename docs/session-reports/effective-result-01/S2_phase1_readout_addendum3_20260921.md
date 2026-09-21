# Addendum 3 — resolution rule v2.1 (supersedes addendum 2's table)

**Dated** 2026-09-21 (UTC) · **docs-only** · **HEAD at measurement** `1281e02`, tree clean
`review.db` opened `mode=ro` only; no write. The read-out and addenda 1–2 are **not edited**.

v2.1 applies R20–R22 to the v2 table of addendum 2 (`1281e02`), which it supersedes in whole.
Everything else in addenda 1–2 and the read-out stands.

**One finding in this addendum falsifies a premise the PI has already ruled on** — see §F. It
does not touch v2.1, which is a field-level rule; it touches R18/Q8 and read-out §7.3, both
paper-level. Recorded, not adapted.

---

## A. Resolution rule v2.1

`effective_value(paper, field, arm)` → `(value, field_state, provenance)`.
`effective_state(paper)` → the paper's lifecycle state with who and why.

**`arm` is required** (R18/Q4). **Arms are data** (R12), **immutable once they hold a claim**
(R21). First matching row wins. **Rows 2–5 and 8 fire only on `reviewer` events** (R11).

### The against-reference, made precise (R20)

R20 makes a reviewer event's against-reference **a set**. Rows 2–5 and 8 turn on how that set
compares with the cell's live claims, so the comparison is defined once here rather than
re-stated per row.

- `live(cell, arm)` — the claims on this cell in this arm that row 9 has not superseded.
- A reviewer event carries `against: set[claim_id]`, and for the conflict case
  `against_decisions: set[event_id]`.

| relation | meaning | row |
|---|---|---|
| `against` names any claim **not** in `live` | the decision was made against something since superseded | **3** |
| `against` **==** `live` (set equality, any size) | the decision speaks for the whole cell — **this is R20's exit** | **4 / 5 / 8** |
| `against` **⊊** `live` (proper subset, all live) | a partial decision: it cannot say which value it endorses or replaces, so it resolves nothing | falls through to **6 / 7** |

Set equality is what lets one CORRECT or WITHDRAW discharge a duplicate (row 6) or a
pre-manifest pair (row 7): naming *every* competing claim is exactly the condition. A partial
reference is inert by construction — it does not need a separate refusal.

**ACCEPT is refused at write when `|live| > 1`** (R20), so row 8 can only ever see a
single-claim cell. That is why row 8 sits below rows 6 and 7 without shadowing them.

### The table

| # | situation | inputs | effective value | S5b state | provenance — and, for unresolved rows, **the exit** |
|---|---|---|---|---|---|
| **0** | **Not assigned** (R13) | cell outside this arm's assignment | — | **not an S5b state** — **out of scope** | `{arm, assigned: false}`; **excluded from every denominator**. Model arms default to the full corpus. A reviewer decision here is **refused at write** (R22/U2) |
| **1** | **Missing** | assigned; no `field_events` row | — | **missing** | `{arm, reason: "assigned, no claim"}` |
| **2** | **Conflicting reviewer decisions** (R18/Q1) | ≥2 reviewer events on this cell differing in effect, and no later reviewer event whose `against_decisions` covers all of them | — ; all surfaced | **unresolved (needs re-review)** | `{decisions: […]}` — **newest does not win**. **EXIT (R20):** a reviewer event referencing **all** competing decisions; that event then evaluates as withdraw / correct / accept |
| **3** | **Reviewer decision against a superseded claim** | governing reviewer event's `against` names a claim not in `live` | — ; both surfaced | **unresolved (needs re-review)** | `{reviewer, current, value_unchanged: <bool>}` (R18/Q2). **EXIT:** a new decision against the current claim |
| **4** | **Reviewer withdrew** (R1) | governing reviewer event `human_withdrew`, `against == live` (any size) | — (no value) | **withdrawn** | `{withdrawn_by, at, reason, original_values, original_claim_ids}` — originals live in history, never in the value |
| **5** | **Reviewer corrected** | governing reviewer event `human_corrected`, `against == live` (any size) | the reviewer's value | **corrected by human** | `{corrected_by, at, original_values, original_claim_ids, presented_context_sha256}` |
| **6** | **Duplicate values within one claim** (R2) | ≥2 `asserted` events, same `claim_id`, differing values, retry budget spent; no reviewer event with `against == live` | — ; both surfaced | **unresolved (duplicate values)** | `{candidates: […]}`. **EXIT (R20):** CORRECT or WITHDRAW naming **every** competing claim. **ACCEPT is refused at write** |
| **7** | **Indistinguishable pre-manifest claims** (R10) | ≥2 claims, all registered `configuration: "not recorded (pre-manifest)"`; no reviewer event with `against == live` | — ; both surfaced | **unresolved (needs re-review)** | `{claims: […], reason: "configurations indistinguishable — pre-manifest"}`. **EXIT (R20):** CORRECT or WITHDRAW naming **every** competing claim. **ACCEPT is refused at write** |
| **8** | **Reviewer accepted** | governing reviewer event `human_accepted`, `against == live`, `\|live\| == 1` | the extractor's value | underlying state from 10–15, **endorsed** | extractor provenance + `{accepted_by, at, presented_context_sha256}` |
| **9** | **Superseded within one arm** (R10, R21) | ≥2 claims with **distinguishable configurations**, the newer from a re-extraction whose **input identity changed** (S3d); no reviewer event | newest claim's value, then 10–15 | per 10–15 | `{supersedes: […], superseded_because: "input identity changed"}`. An arm's configuration is never re-pinned (R21), so "distinguishable" means *different arms are not involved* — same arm, same pin, changed input |
| **10** | **Value, citation located** (R17) | newest claim; not a token, not a sentinel; `citation_located` with `located = true` | the value | **asserted with evidence** | `{…, located: {parsed_text_id, threshold, locator_version, at}}` |
| **11** | **Value, not located** (R17, R22/U6) | newest claim; not a token, not a sentinel; **no** located event, **or** one with `located = false` | the value | **asserted without locatable evidence** | `{…, snippet_supplied: <bool>, located: null \| {…, located: false}}` — **one state, two provenances** (R22/U6) |
| **12** | **Sentinel, citation located** (R17) | `value ∈ codebook absence_sentinels`; `located = true` | the sentinel — **it is a value** | **asserted with evidence** | as row 10. **Identical test to row 10** |
| **13** | **Sentinel, not located** (R17, R22/U6) | sentinel; no located event or `located = false` | the sentinel | **asserted without locatable evidence** | as row 11 |
| **14** | **Declined** | `NO_EVIDENCE_LOCATABLE`, zero citations | — (no value) | **declined** | `{arm, claim_id, actor, role, run_id}` — **never skipped** |
| **15** | **Contract unmet** | `CONTRACT_UNMET` | — (no value) | **contract unmet** | `{violation_codes, attempts}` — the engine's failure, not the extractor's |
| **16** | **Arms compared, never merged** (R10, R12, R22/U3) | ≥2 arms each resolving by 0–15 | **not resolved across arms** | each arm keeps its own | `{per_arm: {…}}`. **A cross-arm judgment is not an event shape** (R22/U3); a reference standard, when Paper 1 needs one, is **its own arm** whose claims reference their source claims |
| **17** | **State at migration** (R-f) | only a `state_at_migration` event, with no derivable value | today's recorded state, tagged | the state as recorded | `{source: "state at migration", migrated_at, note: "history not reconstructable from the record"}` |

### Standing conditions, outside the row order

Unchanged from v2: the reader **ignores non-codebook fields** (R18/Q6); **auditor verdicts are
provenance, never a field state** (R18/Q7), and `flagged` / `contested` **count as work owed** in
derived stage completion.

New in v2.1: **`'No comparison reported'` is an ordinary value** (R22), resolved by rows 10/11 and
not by 12/13. Whether the phrase becomes a codebook sentinel is a **codebook decision for session
9**, not a reader decision. Measured population: **131 spans** (addendum 2, B6).

---

## B. v2 → v2.1 row mapping

| v2 | situation | v2.1 | moved? |
|---:|---|---:|---|
| 0 | Not assigned | **0** | — |
| 1 | Missing | **1** | — |
| 2 | Duplicate values | **6** | **down 4** — below the reviewer exits, per R20 |
| 3 | Conflicting reviewer decisions | **2** | up 1 |
| 4 | Reviewer vs superseded claim | **3** | up 1 |
| 5 | Indistinguishable pre-manifest claims | **7** | **down 2** — below the reviewer exits, per R20 |
| 6 | Reviewer withdrew | **4** | **up 2** |
| 7 | Reviewer corrected | **5** | **up 2** |
| 8 | Reviewer accepted | **8** | — (position unchanged; still below 6 and 7, safe because R20 refuses ACCEPT there at write) |
| 9 | Superseded within arm | **9** | — |
| 10 | Value, located | **10** | — |
| 11 | Value, not located | **11** | — |
| 12 | Sentinel, located | **12** | — |
| 12b | Sentinel, not located | **13** | renumbered (no `b` suffix) |
| 13 | Declined | **14** | — |
| 14 | Contract unmet | **15** | — |
| 15 | Arms compared | **16** | — |
| 16 | State at migration | **17** | — |

**The whole substantive change is that withdraw and correct now outrank the two
"unresolved-because-more-than-one-claim" rows.** In v2 a CORRECT naming both duplicated values
would have been shadowed by row 2 and never fired — the cell would have stayed unresolved with no
way out. That is the defect R20 fixes, and the reordering is what makes the exit reachable. Row 8
did not need to move because R20 removes the case that would have shadowed it.

---

## C. Clause coverage

| clause | source | row(s) |
|---|---|---|
| withdraw = no value, original in history | **R1** | 4 |
| duplicated field → unresolved (duplicate values) | **R2** | 6 |
| stale import refused, naming the superseding claim | **R3** | read-out §2.4; row 3 supplies the current claim the refusal quotes |
| arms declared per review; changed config = new arm | **R10** | 9 (supersession condition), 16 |
| pre-manifest configurations do not supersede | **R10** | 7 |
| existing arms registered "not recorded (pre-manifest)"; nothing backfilled | **R10** | 7, 17 |
| actor role on every event; override rows reviewer-only | **R11** | preamble; rows 2–5, 8 |
| human extractor arms are ordinary arms | **R11**, **R14** | 16, and rows 10–13 applying to them unchanged |
| arms are data — one registry, no per-arm branches | **R12** | preamble; 16 |
| partial coverage → not assigned, out of scope, no denominator | **R13** | **0** |
| "missing" = assigned and absent | **R13** | 1 |
| human workbooks loaded as arms, session 12, with a receipt | **R14** | addendum 2 §D; no reader row |
| no S4 columns reserved on the event tables | **R15** | addendum 2 §C.3 |
| cloud `UNIQUE(paper_id, arm)` dropped in session 6 | **R16** | enables 9 and 7 for cloud arms |
| "asserted with evidence" needs a citation-located event; identical for values, sentinels, human source quotes | **R17** | 10, 11, 12, 13 |
| snippet-supplied is provenance, not state | **R17** | 11, 13 |
| relevance of a located citation is the judge's question | **R17**, S5c | not a reader row — a stated non-responsibility |
| two reviewer decisions on one claim → refuse | **R18**/Q1 | 2 |
| `value_unchanged` in provenance, row kept | **R18**/Q2 | 3 |
| arm is a required argument | **R18**/Q4 | preamble; 16 |
| reader ignores non-codebook fields | **R18**/Q6 | standing condition |
| auditor verdicts are provenance; flagged/contested are work owed | **R18**/Q7 | standing condition |
| Q8, Q9 recorded as state at migration | **R18** | 17 — **but see §F: Q8's premise is falsified** |
| Q10 PI v2 audit is measurement only | **R18** | no row — writes no field event |
| A11 Option B | **R18** | no row — schema lifecycle |
| no local extraction until S3d; **no cloud extraction until session 7** | **R19**, extended by **R22**/U4 | operational, not a row; protects 7 and 9 |
| every unresolved outcome names its exit; exits are reviewer events | **R20** | 2, 6, 7 (exits in the provenance column); 3's exit; the against-set semantics above |
| a reviewer event's against-reference is a set | **R20** | the relation table above; rows 2–5, 8 |
| ACCEPT refused at write on a multi-claim cell | **R20** | 6, 7 (stated); 8 (why it may sit below them) |
| arms immutable once they hold a claim; retirement ≠ re-pinning | **R21** | 9; resolves U1 |
| reviewer decision outside assignment refused at write | **R22**/U2 | 0 |
| cross-arm judgment is not an event shape; a reference standard is its own arm | **R22**/U3 | 16 |
| U4 accepted as correct behaviour | **R22** | 7 (a post-session-6 cloud re-extraction lands here, by design) |
| `located = false` and no located event are one state | **R22**/U6 | 11, 13 |
| `'No comparison reported'` is an ordinary value | **R22** | standing condition; 10/11 |

**Every S2 clause and every one of R1–R3, R10–R22 maps to a row, a standing condition, or an
explicitly stated non-responsibility. No clause is unmapped.**

---

## D. Reachability check (I16)

One minimal event history per row. `Cn` = claim id, `arm A` unless stated.

| # | minimal history that reaches it | reachable |
|---:|---|---|
| 0 | arm A assigned to papers {1, 2}; ask `effective_value(3, f, A)` | ✅ |
| 1 | paper 1 assigned to A; no `field_events` row for `(1, f, A)` | ✅ |
| 2 | `C1` asserted; reviewer R1 `human_corrected`→"X" against `{C1}`; reviewer R2 `human_withdrew` against `{C1}`; no event covering both | ✅ |
| 3 | `C1` asserted; `C2` asserted (distinguishable config, input changed) → `C1` superseded; reviewer `human_corrected` against `{C1}` | ✅ |
| 4 | `C1` asserted; reviewer `human_withdrew` against `{C1}`; `live = {C1}` | ✅ |
| 5 | `C1` asserted; reviewer `human_corrected`→"X" against `{C1}` | ✅ |
| 6 | `C1` with two `asserted` events, values "A" and "B", retry budget spent; no reviewer event | ✅ |
| 7 | `C1`, `C2` both `configuration: "not recorded (pre-manifest)"`; no reviewer event | ✅ |
| 8 | `C1` asserted; reviewer `human_accepted` against `{C1}`; `\|live\| = 1` | ✅ |
| 9 | `C1`, `C2`, distinguishable configurations, `C2` from a re-extraction with changed input identity; no reviewer event | ✅ |
| 10 | `C1` value "45"; `citation_located(C1, located=true)` | ✅ |
| 11 | `C1` value "45"; no located event **(or one with `located=false`)** | ✅ |
| 12 | `C1` value "NR"; `citation_located(C1, located=true)` | ✅ |
| 13 | `C1` value "NR"; no located event | ✅ |
| 14 | `C1` value `NO_EVIDENCE_LOCATABLE`, zero citations | ✅ |
| 15 | `C1` value `CONTRACT_UNMET` | ✅ |
| 16 | arms A and B each hold a claim on `(p, f)`; caller asks each | ✅ |
| 17 | a `paper_events` `state_at_migration` row for paper 1 with no reconstructable history; `effective_state(1)` | ✅ |

**No row is unreachable. I16 holds.** Two qualifications, recorded because they are properties of
the check rather than failures of it:

**D.1 Row 16 is a scope statement, not a branch.** It is not reached by falling past row 15; it
is what the reader *does* — answer per arm. Its "minimal history" is two arms holding claims,
which is true of this database today (`local`, `anthropic_sonnet_4_6`, `openai_o4_mini_high`).

**D.2 Row 17's field-level population is empty at migration, though the row is reachable.**
`evidence_spans.value` is `TEXT NOT NULL`, so every migrated field cell carries a value and
derives a state through rows 10–13; none falls to 17. Row 17 is reached through
`effective_state(paper)` — the paper-lifecycle stream, where the workflow stamps (read-out F3)
land. This is not a defect: R17 already requires that legacy field rows begin at 11 or 13, and
that is exactly where they land.

**D.3 The partial against-reference is inert, not unreachable.** `against ⊊ live` matches no
reviewer row and falls to 6 or 7. That is intended under R20 — a decision that names some but not
all competing claims cannot say which value it endorses — and it needs no refusal at write,
because the reader simply does not act on it. Worth stating because it is the one case where a
reviewer event exists and no reviewer row fires.

---

## E. I15 result

**I15 HOLDS. No human field-level decision exists on disk.**

```sql
SELECT 'audit_adjudication rows', COUNT(*) FROM audit_adjudication
UNION ALL SELECT 'evidence_spans auditor_model = human_review',
                 COUNT(*) FROM evidence_spans WHERE auditor_model = 'human_review'
UNION ALL SELECT 'evidence_spans auditor_model NOT gemma3:27b',
                 COUNT(*) FROM evidence_spans WHERE auditor_model IS NULL OR auditor_model <> 'gemma3:27b'
UNION ALL SELECT 'evidence_spans audit_rationale mentions human',
                 COUNT(*) FROM evidence_spans WHERE LOWER(COALESCE(audit_rationale,'')) LIKE '%human%'
UNION ALL SELECT 'papers at HUMAN_AUDIT_COMPLETE', COUNT(*) FROM papers WHERE status = 'HUMAN_AUDIT_COMPLETE';
```

| probe | n |
|---|---:|
| `audit_adjudication` rows | **0** |
| `evidence_spans.auditor_model = 'human_review'` | **0** |
| `evidence_spans.auditor_model` other than `gemma3:27b` (incl. NULL) | **0** |
| `evidence_spans.audit_status = 'invalid_snippet'` | **0** |
| papers at `HUMAN_AUDIT_COMPLETE` | **0** |
| papers at `REJECTED` | **0** |
| `evidence_spans.audit_rationale` containing "human" | **197** |

**The 197 are the semantic verifier's prose, not human decisions.** They concentrate in
`autonomy_level` (71), `clinical_readiness_assessment` (26), `task_execute` (21) and
`comparison_to_human` (19) — fields about human supervision — and every sample begins
`Grep failed but semantic verified. …`. Decisive check: the audit importer writes three literal
prefixes, and **none occurs**:

| literal prefix written by the importers | matches |
|---|---:|
| `Accepted by human reviewer` | **0** |
| `Rejected by human reviewer` | **0** |
| `Human override` | **0** |

So rows 2–5 and 8 have an **empty field-level population at migration**, as I15 predicted, and row
17's field-level population is empty for the separate reason in D.2.

---

## F. 🔴 A finding that falsifies read-out F2, and with it the premise of R18/Q8

**Not adapted. Reported for the PI to re-rule.** It does not affect v2.1 — the 416 are
paper-level abstract exclusions and appear in no row of a field-level table.

Read-out §5.4 (F2) and addendum 1 stated:

> The identity of the 416 papers is **not in the database**. It is recoverable only from
> `data/surgical_autonomy/adjudication/screening_queue_20260310.xlsx` … a file with no claim
> binding and no presented-context hash.

**That is false.** Measured this session:

```sql
SELECT rejected_reason, COUNT(*) FROM papers
WHERE TRIM(COALESCE(rejected_reason,'')) <> '' GROUP BY rejected_reason ORDER BY COUNT(*) DESC;
```

| n | `rejected_reason` |
|---:|---|
| **416** | `PI adjudication 2026-03-11: excluded (100% of flagged)` |
| 2 | `verifier-excluded, consistent with 416/416 adjudication concordance` |
| 1 | `Manual review (Mar 8 re-screen): simulation-only extended abstract …` |
| 1 | `Manual review (Mar 8 re-screen): mechanical design paper (SCARA + da Vinci …)` |
| 1 | `Manual review (Mar 8 re-screen): confirmed false positive — pediatric radiology conference abstracts (SPR 2020) …` |
| 1 | `Manual review (Mar 8 re-screen): Industrial human-robot interaction paper. KUKA iiwa cobot …` |
| 1 | `Manual review (Mar 8 re-screen): AI/task planning methodology paper (ILP/ASP) …` |

**The 416 are individually identifiable by a `WHERE` clause**, and the record is internally
coherent: all 423 are at `ABSTRACT_SCREENED_OUT`, and `updated_at` splits **2026-03-12 ×416**
(matching `workflow_state` row 5's `completed_at` of `2026-03-12T05:37:12`), 2026-03-08 ×3 and
2026-03-09 ×2 (matching the "Mar 8 re-screen" text), 2026-03-13 ×2. Five of the seven reasons are
*per-paper* and give a substantive exclusion ground, not a batch label.

**Two further findings come with it.**

**F.1 — nothing in the codebase wrote these rows.** The only writer of `rejected_reason` is
`engine/core/database.py`:

```
SET status = 'REJECTED', rejected_reason = ?, updated_at = ?
```

It sets `status = 'REJECTED'` in the same statement, and **all 423 of these papers are at
`ABSTRACT_SCREENED_OUT`, none at `REJECTED`**. A repository-wide grep for the literal
`PI adjudication` across `*.py` returns only unrelated hits in `analysis/paper1/consensus.py` and
`pi_audit_unblind.py`. **These 423 rows were written by hand, outside every code path and outside
the migration régime** — which is why no session found them by reading code.

**F.2 — `prisma.py` cannot see any of them.** Its rejection-reason breakdown reads

```
"SELECT rejected_reason, COUNT(*) as cnt FROM papers WHERE status = 'REJECTED' GROUP BY rejected_reason"
```

and **0 papers are at `REJECTED`**, so the PRISMA "reasons for exclusion" section is **empty while
423 reasons exist**. This is a reader/writer mismatch of the same family as H1, and it is not
covered by H1's text.

**What this changes for R18/Q8.** Q8 offered (a) migrate the 416 as `state_at_migration`, on the
stated ground that their identity is not in the database, or (b) parse the workbook. The PI chose
(a). **Option (a)'s justification no longer holds**: the identities are in the database, dated,
attributed to the PI, and consistent with the workflow stamp. A third option now exists that
neither ruling considered — **reconstruct 416 human exclusion events directly from
`papers.rejected_reason` and `papers.updated_at`, with no workbook parsing and no guesswork** —
which is materially stronger than (a) and cheaper than (b). Whether R-f's "no invented history"
permits it turns on whether a hand-written column with a coherent timestamp counts as "the record
supporting it". **That is the PI's call, and this addendum does not make it.** R18/Q8 stands as
ruled until the PI says otherwise; read-out §7.3's row for the 416 is affected in the same way.

---

## G. Rows whose exit is not expressible in the schema as S2 describes it

**One, and it is the schema change R20 already anticipates.**

S2 as written in the plan describes a reviewer decision as being made against *"the exact prior
result it was made against (a claim or event id)"* — **singular**. R20 requires a **set**: rows 6
and 7 can only be exited by a decision naming *every* competing claim, and row 2 by one naming
*every* competing decision. A single-valued `against_claim_id`, as read-out §3.2 proposed, cannot
express either exit.

R20 states this consequence and assigns the representation to session 5. Recorded here as the one
place where v2.1 outruns the read-out's schema: **read-out §3.2's `against_claim_id TEXT` is
superseded** by a set-valued reference, and session 5 must not implement the singular column.

No other row's exit is inexpressible. Rows 3, 4, 5 and 8 are satisfied by the same set-valued
reference; rows 0 and 1 have no exit because they are not unresolved states; rows 10–15 and 17 are
terminal classifications, not conditions awaiting a person.
