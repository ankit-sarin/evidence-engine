# ADDENDUM to DISCOVERY-01 Part A — 2026-09-21

**This is an addendum, not an edit.** `DISCOVERY-01_part-A_readout.md` is
unchanged. Issued by MIGRATIONS-01 Phase 1, which found a live schema defect
that changes the status of two findings.

## D1-1 and D1-2 — "path unwritable" supersedes "exposure entirely prospective"

Part A classified both REPRODUCES and wrote, of their consequence:

> **The exposure is entirely prospective**, and it is aimed squarely at the
> first real PI audit import.

**Measured 2026-09-21 on a scratch copy of the live database** (taken with
`auto_backup`, read-only on its source, fingerprint `f376562e…39e00`, probe
deleted):

```
PRAGMA foreign_keys = ON          -- exactly what ReviewDatabase.__init__ sets
INSERT INTO audit_adjudication (...) VALUES (...)
  -> OperationalError: no such table: main._evidence_spans_old
```

`audit_adjudication.span_id` declares `REFERENCES "_evidence_spans_old"(id)` — a
table that **does not exist**, left behind by the `evidence_spans` CHECK rebuild
in `_run_migrations`, which renamed the original out of the way and never
re-pointed this foreign key. `PRAGMA foreign_key_check` reports nothing only
because the table has zero rows.

**So the correct status is stronger than Part A's.** The human-audit import path
is **not defective — it is unwritable**. `import_audit_review_decisions`'s REJECT
and CORRECT branches both begin with an `INSERT INTO audit_adjudication`, so
neither could reach the behaviour D1-1 and D1-2 describe. It also explains,
without any other hypothesis, why `audit_adjudication` has **0 rows**.

**What does not change.** Both findings' *diagnosis* stands — the REJECT branch
does leave `evidence_spans.value` intact and set `audit_status='verified'`, and
the importer does bind to the newest extraction regardless of which one a
workbook was generated from. Both were reproduced on temp databases built by
`ReviewDatabase`, where the foreign key points at `evidence_spans` correctly, so
the reproducers are sound. What changes is the consequence sentence: on the
**live** database the path cannot run at all.

**Ownership.** Recorded as inventory row **A11**, deferred to session 4 (S2
Phase 1), which decides what `audit_adjudication` becomes under the event model
— if it survives, the rebuild is S2's migration; if field-value events replace
it, it is dropped. Nothing writes to it before session 12. It was deliberately
**not** fixed by MIGRATIONS-01: repairing it means rebuilding a table on the
live database, which that task's single authorised write did not cover.

Superseded by: `docs/session-reports/MIGRATIONS-01_report.md`.
