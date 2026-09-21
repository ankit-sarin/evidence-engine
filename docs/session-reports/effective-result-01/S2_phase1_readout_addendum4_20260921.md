# Addendum 4 — legacy scope reset (seed, not migrate)

**Dated** 2026-09-21 (UTC) · **docs-only** · **HEAD at measurement** `b0d05f8`, tree clean
`review.db` opened `mode=ro` only; no write.

**This addendum supersedes read-out §5 (mapping today's data to states) and read-out §7 (the
session-5 migration plan) in whole.** Both were written to migrate legacy results into the event
store. R25 does not migrate them. The read-out and addenda 1–3 are not edited.

**Unchanged and not re-drafted here:** resolution rule v2.1 (addendum 3 §A, approved by R23), the
event schema (read-out §3 as amended by addendum 2 §C), and claim identity (read-out §2.3 as
amended by addendum 2 §D).

---

## A. What is seeded, and what stays behind (R25)

**Seeded into the event store — this is the whole of it:**

| seed | source, measured |
|---|---|
| Papers | `papers`, **10,039 rows** |
| Current corpus membership, as **one `state_at_migration` paper event per corpus paper** | the corpus predicate in `engine/core/corpus.py`, **190 papers** (I20). **No reconstruction of how the state arose** — not the screening decisions, not the verifier, not the 416 adjudications |
| Parsed-text references | `full_text_assets.parsed_text_path`, **194 rows across the 190 corpus papers**, every file present on disk (I21) |
| Spec identity | `screening_hash` `e8fa9719…f043596`; the Review Spec hash |
| Codebook identity | `data/surgical_autonomy/extraction_codebook.yaml` — **20 fields, frozen for Paper 1 (R26)** |

**Left in place, read-only, as regression fixture and telemetry — not imported:**

| stays behind | rows |
|---|---:|
| `extractions` | 190 |
| `evidence_spans` | 3,760 |
| `cloud_extractions` | 379 |
| `cloud_evidence_spans` | 7,257 |
| auditor verdicts (`audit_status` / `auditor_model` / `audit_rationale` / `audited_at`) | 3,760 spans |
| `abstract_screening_decisions` · `abstract_verification_decisions` | 21,374 · 1,422 |
| `ft_screening_decisions` · `ft_verification_decisions` | 366 · 182 |
| `abstract_screening_adjudication` · `ft_screening_adjudication` · `audit_adjudication` | 0 · 36 · 0 |
| `workflow_state` stamps | 12 rows, 7 complete |
| `papers.rejected_reason` (incl. the 416 PI adjudications) | 423 |
| `review_runs` | 6 |

Nothing is deleted and nothing is rewritten. The legacy tables keep their current contents and
become the fixture the new reader is regression-tested *against*, never a source it reads *from*.

---

## B. The revised session-5 gate

Session 5 passes when the event store and the reader exist and **v2.1 is tested on fixtures** —
including the D1-1 … D1-4 reproducers, each built as a constructed event history rather than
found in live data, which is what the empty-sample measurements of addendum 1 already required —
and when **the seed of §A has been applied after a verified backup**, with the content fingerprint
recorded **before and after** and the new record committed and quoted in the closeout. There is
**no old-vs-new reader agreement check**: R25 removes the legacy field-level history the check was
defined over, so agreement with a legacy reader is no longer evidence of anything. The reader is
validated on fixtures here, and then in production by the freshman smoke run and Run 7.

---

## C. Moot under R25

| item | where it was | why moot |
|---|---|---|
| R18's **Q8** clause (the 416 abstract exclusions → state at migration) | Decision log, `1281e02` | no screening history is reconstructed at all; the 416 stay in `papers.rejected_reason` as telemetry. **Also relevant: addendum 3 §F falsified Q8's stated premise** — moot either way |
| R18's **Q9** clause (the 7 workflow stamps → state at migration) | Decision log, `1281e02` | stamps are not imported; stage completion is derived |
| R17's **legacy-backfill clause** (a dated locatability measurement upgrading legacy rows after S3e) | Decision log, `1281e02` | there are no legacy field rows in the store to upgrade. R17's *forward* half — a located event written at extraction time, recording parsed-text identity, threshold and locator version — **stands unchanged** |
| **v2.1 row 17 at field level** | addendum 3 §A | its field-level population was already empty (addendum 3 §D.2); now it has no field-level source at all. **The row stands for `effective_state`**, which is where the corpus-membership seed lands |
| Session-5 gate clause *"the new reader agrees with each old reader on the live data"* | read-out §7.5 | see §B |
| Session-6 judge target of **3,802 legacy cells** (S1c) | Step 4, session 6 | the judge's universe becomes the cell grid of a run made on the settled state, not Run 6's |

Cloud comparator arms, if Paper 1 needs them, are **re-run on the tagged state under declared
arms** (R25); the decision is deferred to Run 7.

---

## D. Four engine-defect rows — drafts

Re-framed per this ruling as **engine fixes, not data corrections**. The inventory tables in the
plan are not edited by this session; these are appended at closeout. A12, B6 and D7 are restated
from addendum 2 §G with their framing corrected; **H2 is new.**

### A12 — `load_arm` returns an empty dict for a human arm, silently
`engine/analysis/concordance.py::load_arm` has two branches (`local` / else-cloud);
`engine/validators/distribution_monitor.py::_query_values` has three and routes
`arm.startswith("human_")`. A `human_*` arm therefore queries `cloud_extractions` and returns `{}`
with no error. `docs/architecture/pipeline.md` and `modules.md` document the branch that does not
exist. **On disk: LATENT** (no human arm loaded). **Closes session 6**, when the arm registry
becomes the single routing predicate. Doc corrections are session-6 hygiene.

### B6 — the auditor's absence branch auto-verifies without a text check
`engine/agents/auditor.py::audit_span` returns `"verified"` before `grep_verify` runs, for any
value in a four-item hand-list divergent from the codebook's six sentinels. **Measured: 313 of the
2,159 `verified` spans (14.5%) were never text-checked** — `NR` 176, `NOT_FOUND` 6, and **131
`'No comparison reported'`, an ordinary value under v2.1 (R22)**. `SEMANTIC_ONLY_TIERS = {4}`
forces `grep_pass = True`, with **zero live rows** (every span is tier 1). **On disk: WRONG.**
Under R25 this corrupts no result the engine will read — the 3,760 spans are telemetry — so it is
purely an **engine fix that must land before Run 7's audit pass**, or Run 7 reproduces it.
**Closes session 9.** Cross-reference: the file's own comment records the two-hand-list divergence
as fix-phase item **N2**; B6 is the wider defect of either list disagreeing with the codebook.

### D7 — the extractor's skip key cannot match
`engine/agents/extractor.py` skips on `WHERE paper_id = ? AND codebook_hash = ?`; `codebook_hash`
is **NULL on all 190 rows**, and `NULL = <anything>` is never true. **On disk: ARMED** — the next
local run re-extracts all 190. Under R25 that is no longer a data hazard (nothing is migrated from
those rows), but it remains a correctness defect and a cost one. **R19 is the interim control.**
**Closes S3d, session 8.**

### H2 — `prisma.py`'s rejection-reason breakdown is blind to every reason on disk *(new)*
`engine/exporters/prisma.py` builds its exclusion-reason table from
`"SELECT rejected_reason, COUNT(*) as cnt FROM papers WHERE status = 'REJECTED' GROUP BY rejected_reason"`.
**Measured: 0 papers are at `REJECTED`, and 423 carry a `rejected_reason`** — all at
`ABSTRACT_SCREENED_OUT`, including the 416 `PI adjudication 2026-03-11: excluded (100% of flagged)`
(addendum 3 §F). The section renders **empty while 423 reasons exist**. Same family as H1, and not
covered by H1's text. **On disk: WRONG** (every PRISMA export to date). **Closes with PRISMA from
events, S8.** Note for whoever writes it: under R25 the reasons are not seeded, so S8's PRISMA is
built from events of the new run, and these 423 remain telemetry.
