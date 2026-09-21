# ADDENDUM to DISCOVERY-01 Part B — 2026-09-21

**This is an addendum, not an edit.** `DISCOVERY-01_part-B_readout.md` is
unchanged and stays the record of what was found on 2026-09-20. Issued by
MIGRATIONS-01 Phase 1, which measured the same code more closely.

## D2-6 — one clause is wrong

Part B wrote, of `ReviewDatabase._run_migrations`:

> It iterates `_SIMPLE_MIGRATIONS` and two inline rebuilds; **it never imports or
> runs the numbered migration modules.**

**That is false.** Measured at the same HEAD (`b8a83b1`), `_run_migrations`
imports and runs **six** numbered modules by `importlib`, on **every**
construction:

| migration | condition |
|---|---|
| `006_not_null_confidence_tier` | only when `evidence_spans.tier` is missing |
| `007_add_judge_tables` | unconditional |
| `008_add_fabrication_verifications` | unconditional |
| `009_add_backfill_audit_log` | unconditional |
| `012_codebook_provenance` | unconditional |
| `013_drop_schema_hash_not_null` | unconditional |

**The accurate statement** is that it never runs **002, 003, 010 or 011** — and
that 004's and 005's effects are duplicated as inline `ALTER TABLE`s in
`_SIMPLE_MIGRATIONS` rather than being run from their files.

**Cause:** the function is 96 lines and the six `importlib` calls are in its
second half. Part B read the first thirty lines — enough to see the inline list
and the two rebuilds — and generalised from them. The quoted anchor in that
read-out is real; the sentence drawn from it went further than the anchor
supported.

**What does not change.** D2-6's substantive finding stands and was verified
again: **010 and 011 have no receipt anywhere**, `PRAGMA user_version` is 0,
there is no migrations table, and both target states are present in the live
schema because they were applied by hand. I3 is unaffected.

## I1's count, for the next reader

The brief for MIGRATIONS-01 inherited "001 through 011, one file each" from this
read-out's framing. Measured: **twelve files, `002` … `013`.** There is no
`001`, and `012` and `013` exist. Since this addendum, `014` and `015` have been
added, so the series is **002 … 015, fourteen files**.

Superseded by: `docs/session-reports/MIGRATIONS-01_report.md`.
