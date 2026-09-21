# EFFECTIVE-RESULT-02 Core, Phase 2 — the event store, the reader, the fixtures

**Dated** 2026-09-21 (UTC) · **HEAD at build** `d028c7ab77eb3ff2663aba95d111c31371dffacb`, tree clean
**The live database was not written.** It was opened `mode=ro` by the fingerprint tool and once by
`auto_backup`, which reads through SQLite's online backup API. The rehearsal ran on a **copy**.

## What was built

| path | what |
|---|---|
| `engine/migrations/016_event_store.py` | schema migration — seven tables, their append-only triggers, the two R21 triggers, four indices. No existing table touched |
| `engine/migrations/017_seed_event_store.py` | **data** migration — the addendum 4 §A seed, atomic, idempotent by marker |
| `engine/core/effective.py` | the reader: `effective_value` / `effective_state` / `classify_field_state` / `live_claims` / `is_assigned` |
| `engine/core/events.py` | the writer and every write-time refusal |
| `engine/migrations/runner.py` | `KINDS` gains `016: schema`, `017: data`, each with its reason |
| `engine/adjudication/audit_adjudicator.py` | R18 A11 Option B — one refusal at the single entry point |

## The seven tables

`arms` · `field_events` · `paper_events` · `field_event_against` ·
`field_event_against_decisions` · `parsed_text_refs` · `review_identities`.
25 → **32** tables.

**Append-only by trigger**, on all six event-store tables — not by convention in the writer,
because the database already has five delete sites and two importers that bypass each other. A
trigger refuses `scripts/`, `analysis/`, an ad-hoc `sqlite3` session and a future app equally.

**The against-reference is set-valued** (R20), as two junction tables written in the same
transaction as their event. Addendum 3 §G: read-out §3.2's singular `against_claim_id TEXT` "is
superseded … and session 5 must not implement the singular column."

**`arms` is deliberately not append-only.** R21 freezes an arm's configuration once it holds a
claim and leaves `retired_at` settable, because retiring means "accepts no new claims" while its
claims stand. `arm_name` is frozen unconditionally: claim ids embed it, so a rename orphans every
claim that ever named it.

## Two rulings reversed by measurement, before any code was written

**C1 — `UNIQUE` on `extraction_uid` / `claim_id`.** v2.1 row 6 is *"≥2 `asserted` events, same
`claim_id`, differing values"*, and `claim_id` embeds `extraction_uid`, so a UNIQUE on either makes
row 6 unreachable and its fixture unconstructible. Neither constraint exists.
`event_uid TEXT NOT NULL UNIQUE` is the per-event identity, which is what read-out §2.2b needs.

**C2 — refusing `asserted` on a pre-manifest arm.** v2.1 row 7 *is* two claims on such an arm, and
addendum 2 §C.4 registers the three existing arms pre-manifest precisely because that "is what
makes row 5 reachable" (v2 row 5 = v2.1 row 7). The refusal would have deleted the row it was meant
to protect. It does not exist; R19 and R22-U4 remain the operational control until session 7.

## Tests — 93 new, every one green

`tests/test_effective_reader.py` (34) · `tests/test_event_writer_refusals.py` (23) ·
`tests/test_migration_016_017.py` (16) · `tests/test_audit_adjudication.py` (15, of which nine
rewritten). Every fixture is a history **built through the writer**, never found in live data —
addendum 4 §B requires exactly that, and it is what makes rows with zero live population testable.

All 18 v2.1 rows, both exits for rows 6 and 7, R22/U6's two provenances, and D1-1 … D1-4 as
constructed histories. Five refusals: R20 ambiguous, R20 ACCEPT with |live| > 1, R24 proper subset
(naming the omission), R22/U2 unassigned cell, R21 re-pin on a claimed arm.

**Sharing proved by construction, not by equivalent behaviour**: one test changes
`classify_field_state` on the owner and watches the writer's `state_at_write` follow; another spies
`live_claims` to prove R24's refusal sees what the rule sees; a third asserts the pre-manifest
marker is one object across all four modules, since v2.1 row 7 turns on string equality.

## Scratch rehearsal — on a verified backup copy, then deleted

Backup `auto_backup(LIVE, "effective-result-02-rehearsal")`, fingerprint
`a9926e626928d1d47f4935e129da698cd2d50c82902f25c0f4f3cd9b1b1eafae`, 25 tables — **identical to the
live record**, which is what proves the copy. 016 then 017 through the **runner**, never by hand.

| table | rows inserted |
|---|---:|
| `arms` | **3** — `local`, `anthropic_sonnet_4_6`, `openai_o4_mini_high`, all `model`, all `not recorded (pre-manifest)`, none retired |
| `paper_events` | **190** — one `state_at_migration` per corpus paper, `to_state='eligible'`, `from_state` NULL |
| `parsed_text_refs` | **194** — every version, no choice made |
| `review_identities` | **3** — spec, codebook, seed marker |
| `field_events` and both junctions | **0** — R25: the store begins empty of field-level history |
| `papers` | **0 inserted**, **10,039 addressable** by foreign key |

Receipts, both `executed`:
`016_event_store` `deb0ae70ebc2d471902e6d4a093449a986fadf6e9cab314991206ecfe5e0bb2d` ·
`017_seed_event_store` `efcc3a063186eacdf6adf876cde1f34f63c8c7f9568262c94b7e9f55da3b8aff`.
16 receipts total.

**Every legacy table's content hash is unchanged** against the 2026-09-21 record. Of the 25 tables
compared, the only two that moved are `schema_migrations` (the two new receipts) and
`sqlite_sequence` (the AUTOINCREMENT counters the new tables create). The other **23 are
byte-identical**. That is the Phase 3 checkpoint criterion, met in rehearsal.

Read-back on the seeded copy: three corpus papers → `eligible`, rule row 17; one non-corpus paper →
`no_recorded_state`; a cell with no claim → `missing`, rule row 1 — **not** a value recovered from
the 3,760 legacy spans sitting in the same file, which is R25 working.

017 run a second time: `already_seeded: True`, **0 rows inserted in every table**, counts unchanged.

Scratch fingerprint: **32 tables**, overall
`a65c1fa1f46271fa00a3ed640d75c2934c943fc7aabaa07bf5937c5d1e26d716`. Recorded for comparison, not as
a prediction: it embeds timestamps and minted uuids, so the live seed will differ. The **counts**
are what Phase 3 must match. Scratch copy and rehearsal backup deleted; no `-wal`/`-shm` sidecar was
left beside the live database.

## Two standing guards that the new code met by moving, not by relaxing

**The raw-YAML guard (C8).** `effective.load_absence_sentinels` and 017's codebook read both began
as bare `yaml.safe_load` and turned `test_only_the_two_loaders_parse_yaml_directly` red. Both now go
through `engine.core.codebook`. The inventory's `raw yaml load sites` count is unchanged at **3**.

**The entry-point drift guard.** `docs/inventory/entry_points.{md,json}` were regenerated with
`python -m engine.tools.inventory --write`, mechanically — files scanned 207 → 211. This crosses the
brief's `docs/inventory/` DO NOT TOUCH, and is reported rather than quietly done: the guard exists
to force regeneration when the tree gains modules, and leaving it red would fail G6 and block the
commit. Nothing in those files was hand-edited.

## Findings

**I12 is false as stated.** The runner does **not** execute a migration inside a transaction: it
closes its own connection (`conn.close()  # migrations open their own connection`) and calls
`module.run_migration(db_path)`, and each migration opens and commits its own. The docstring's "one
transaction per migration" is about the **receipt** write. So 017 manages its own: one `BEGIN`, the
seed marker written **last**, `ROLLBACK` on any exception — which matters here more than usual,
because the append-only triggers make a partial seed uncleanable.

**No paper-level `to_state` vocabulary existed.** Read-out §3.3 declared the column "CHECK-
constrained … from the S2 paper-level vocabulary" and enumerated it nowhere; §5.4 gives four prose
labels, not tokens. The five tokens are introduced in 016 and recorded here so the next reader knows
they were minted in session 5, not inherited.
