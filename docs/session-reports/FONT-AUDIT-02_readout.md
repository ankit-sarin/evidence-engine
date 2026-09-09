# FONT-AUDIT-02 — FONT_EXPOSURE wired into the verdict and the attempt ledger

**Date:** 2026-09-09 · **Machine:** DGX Spark · **Type:** feature + one schema migration + docs
**HEAD at start:** `6b6260a`, tree clean.

FONT-AUDIT-01 shipped the exposure measurement as a pure module. This makes it part
of the gate: a fifth absolute criterion, a per-attempt ledger column, and the
correction PARSE-GATE-10 needed. **No re-parse, no recovery, and no ruling on 586
or 699.**

---

## 1. The criterion

```
FONT_EXPOSURE = (marked_in_text + silent_in_text_lo) × 1000 / exported_chars
limit         = 5.0 per kchar, absolute
```

`marked_in_text` is the `RE_GLYPH` count in the exported text — both marker forms.
`silent_in_text_lo` is the alignment estimator's **floor**: silent characters
placed inside a matched block. `_hi` is deliberately *not* used, because a gate
criterion that can fail a document on an upper bound fails it on characters nobody
located. `_hi`, `silent_on_dropped_pages`, `space_recoverable` and the three font
counts are telemetry and stay telemetry.

`None` means **not evaluated** and is never coerced to `0.0`. A document nobody
measured and a document measured clean are different facts; collapsing them would
let an un-audited parse report a clean bill. When it is `None` the criterion
appears in neither `failures` nor `metrics`, and the verdict is the other four.

### Why the limit is 5.0 — consistency, not calibration

The limit is **equal to `glyph_density_per_kchar_max`**, and that equality is the
entire argument. Marked and silent corruption are two halves of one defect
measured in the same unit: Docling writes `GLYPH<c=N,font=/…>` when a CID is below
32 and the raw `chr(CID)` when it is at or above 32, and which half a character
lands in is a property of where the subsetter happened to put the glyph, not of
how damaged the document is. A document that fails at 5 markers per kchar should
fail at 5 wrong characters per kchar, and a reader should not have to hold two
threshold semantics for two halves of one thing.

**This is not a calibrated value and is not claimed as one.** Like the other four
it is provisional; §5 shows it changes no verdict on this corpus, which means the
corpus cannot calibrate it either. Its value is the next corpus and the ledger
record.

### 🔴 Two consequences worth reading before relying on it

**(a) The comparison is strictly greater, so exactly 5.0 does NOT fail.** The
brief's G4 asked for "fires at ≥ 5.0". An inclusive fifth criterion standing
beside four exclusive ones would make one limit mean two things, so the
implementation matches the other three and the boundary is **pinned by test** at
4.99 / 5.00 / 5.01 rather than left to a reader's assumption. This is the one
place the implementation deviates from the brief as written.

**(b) `FONT_EXPOSURE` strictly dominates `GLYPH_DENSITY`.** It counts the same
`RE_GLYPH` markers over the same denominator and adds a non-negative term, so
`FONT_EXPOSURE ≥ GLYPH_DENSITY` always. The practical consequence: **a Review
Spec that relaxes `glyph_density_per_kchar_max` alone no longer relaxes the
gate.** That is the brief's formula, not an accident, but it changed the meaning
of an existing spec knob and broke `test_spec_override_reaches_assess`, whose
whole purpose was to show that knob working. The test now relaxes both limits and
says why; `test_font_exposure_dominates_glyph_density` pins the relationship.
`ParseQuality` gains `font_exposure_per_kchar_max` with the coupling named in its
description.

---

## 2. Placement

`assess(text, thresholds=None, *, font_exposure_per_kchar: float | None = None)`.

The cascade measures against the PDF and passes one number. All five criteria are
therefore produced by one function with one threshold vocabulary, and
`Verdict.failures` stays the single source that `format_exclusion_detail`, the
ledger and `select_attempt`'s least-bad ranking all read. `parse_quality` still
opens no file and imports no PDF library.

The module docstring's purity sentence was amended: `compute_metrics` and `assess`
are pure functions **of their arguments**, not "of their text argument alone".

---

## 3. `font_structure()` — the half that needs no text

`font_structure(pdf_path) -> tuple[FontRow, ...]` returns the font inventory with
every character count at 0. It performs no character scan, which its test enforces
by monkeypatching `fitz.Page.get_texttrace` to raise rather than by reading the
source. Cost, measured:

| paper | `font_structure` | full `audit` |
|---|---:|---:|
| 415 (728 pages, 1,647 font objects) | **0.18 s** | 212 s |
| 670 | 0.06 s | 7.6 s |
| 586 | 0.01 s | 12.5 s |

`UNRESOLVING` can never be returned from structure alone — a font is UNRESOLVING
only once one of its characters has actually failed despite a `/ToUnicode` being
present, which is a fact about rendering. Rows that would qualify carry
`klass == ""`, and the docstring says so rather than leaving the absence to be
discovered.

---

## 4. The ledger column

`parse_attempts.font_audit TEXT` — JSON, added by **one line appended to
`_SIMPLE_MIGRATIONS`**, matching all 21 existing entries and auto-run on open with
the duplicate-column swallow that already covers them:

```python
# FONT-AUDIT-02: per-attempt font-exposure audit summary, JSON. NULL means
# the attempt was not audited (it produced no text, or predates the column);
# it never means "audited and clean" -- see engine/parsers/font_audit.py.
"ALTER TABLE parse_attempts ADD COLUMN font_audit TEXT",
```

> **No numbered migration module, and the reason is a finding.** The brief
> specified "migration 010". **010 and 011 already exist** —
> `010_add_provenance_classifications.py` and `011_add_absence_claim_class.py` —
> so the number is taken and the next free one is 012. Reading further,
> **neither 010 nor 011 is wired into `_run_migrations()`**: 006–009 are invoked
> on open, and those two are hand-run (`python engine/migrations/0NN_….py <db>`),
> exercised only by `tests/test_migration_absence_claim_class.py`. So "numbered,
> idempotent, run on open" describes 007–009 and not the two most recent.
> `_SIMPLE_MIGRATIONS` is the exact precedent for adding a column and needs no
> wiring. **MIGRATION-WIRE-01** — wire 010/011 into the self-migrating open, or
> record that they are deliberately manual — is queued and out of scope.

Payload: the 15 `PaperAudit` fields plus `block_min` and `reason`, 18 keys, with
`fonts` filtered to rows whose `klass` is non-empty. Unfiltered it would store
**1,647 font objects for paper 415** — almost all irrelevant Type1 subsets — to
describe seven damaged characters; `total_fonts` carries the full count.

**NULL, and what it means.** NULL on the 8 pre-existing rows (never backfilled),
on skip and error attempts. `{}` was rejected: NULL means nobody looked, `{}`
would read as "looked, found nothing".

Three `reason` values exist: `null` (fully measured), `alignment_timeout: N of M
pages …`, and `no_text_produced`. A structure-only payload carries the same 18
keys with `font_exposure_per_kchar`, `exposure_lo` and `exposure_hi` all `null` —
**absent, never 0.0** — so one reader can read both shapes.

### The cost guard

`audit(..., deadline_s=300.0)`. The deadline is checked **before each page, never
during one**, because `difflib.SequenceMatcher.get_matching_blocks` is a single
uninterruptible call. The real bound is therefore the deadline plus one page's
alignment, and the reason string names how many of how many pages were reached so
the overshoot is legible rather than implied. On expiry the remaining pages are
**neither placed nor dropped**: `hi + dropped == silent total` does not hold when
`reason` is set, and the dataclass says so.

⚠️ **The guard is marginal for paper 415.** Its alignment took **294.0 s** in §5's
run against a 300 s deadline — 6 seconds of headroom, where FONT-AUDIT-01 measured
212 s for the same file. The difference is machine load (§5 ran alongside the test
suite). 415 did not time out, but it is the document that will, first.

---

## 5. The twelve papers, re-assessed

`assess()` re-run on every paper FONT-AUDIT-01 audited, with and without the
criterion. Read-only: `review.db` `mode=ro` for paths, parsed text through
`analysis.provenance.census.parsed_text_path`, PDFs through PyMuPDF. The four
papers PARSE-GATE-07 re-parsed appear twice — once on the version the resolver
returns (v3, what Run 7 would read today) and once on v2, the text Run 6 read and
the version PARSE-GATE-10 and gate 3 are stated on.

| paper | EE | text | `glyph_density` | **`FONT_EXPOSURE`** | verdict BEFORE | verdict AFTER | changed? |
|---:|---|---|---:|---:|---|---|:--:|
| 719 | EE-567 | v3 | 0.0 | 0.000 | `PASS` | `PASS` | no |
| 719 | EE-567 | v2 | 19.586 | 37.281 | `FAIL:GLYPH_DENSITY` | `FAIL:GLYPH_DENSITY,FONT_EXPOSURE` | **+FONT_EXPOSURE** |
| 670 | EE-518 | v2 | 0.082 | 0.082 | `PASS` | `PASS` | no |
| 699 | EE-547 | v3 | 0.0 | 0.000 | `PASS` | `PASS` | no |
| 699 | EE-547 | v2 | 8.631 | 8.631 | `FAIL:GLYPH_DENSITY` | `FAIL:GLYPH_DENSITY,FONT_EXPOSURE` | **+FONT_EXPOSURE** |
| 586 | EE-434 | v3 | 0.0 | 0.000 | `PASS` | `PASS` | no |
| 586 | EE-434 | v2 | 7.831 | 9.781 | `FAIL:GLYPH_DENSITY` | `FAIL:GLYPH_DENSITY,FONT_EXPOSURE` | **+FONT_EXPOSURE** |
| 473 | EE-321 | v2 | 0.08 | 0.320 | `PASS` | `PASS` | no |
| 618 | EE-466 | v2 | 0.034 | 0.034 | `PASS` | `PASS` | no |
| 516 | EE-364 | v2 | 0.0 | 0.000 | `PASS` | `PASS` | no |
| 415 | EE-263 | v2 | 0.0 | 0.000 | `PASS` | `PASS` | no |
| 14 | EE-007 | v1 | 0.0 | 0.000 | `PASS` | `PASS` | no |
| 449 | EE-297 | v2 | 0.0 | 0.000 | `PASS` | `PASS` | no |
| 474 | EE-322 | v2 | 0.0 | 0.000 | `PASS` | `PASS` | no |
| 455 | EE-303 | v3 | 0.0 | 0.000 | `PASS` | `PASS` | no |
| 455 | EE-303 | v2 | 0.0 | 0.000 | `FAIL:SHATTERED` | `FAIL:SHATTERED` | no |

**No verdict changed. Zero of sixteen rows moved between PASS and FAIL.** Three
rows gained a second failing criterion, and they are the three papers that already
failed `GLYPH_DENSITY`:

| paper | predicted (ruling 7) | measured | equals `exposure_lo × 10`? |
|---|---:|---:|:--:|
| 719 v2 | 37.281 | **37.281** | ✓ (3.7281%) |
| 586 v2 | 9.781 | **9.781** | ✓ (0.9781%) |
| 699 v2 | 8.631 | **8.631** | ✓ (0.8631%) |
| every other row | ≤ 0.320 | **≤ 0.320** (473 v2) | ✓ |

Sixteen audits, **357 s**, of which paper 415 is 294 s.

Two rows worth reading rather than scanning:

- **670 v2 scores 0.082**, against a PDF-level coverage of 3.47%. That is the
  whole reason this task exists: the coverage is real and the exposure is not,
  because 670's SimSun subsets set figure labels Docling drops. A criterion built
  on coverage would have failed it.
- **455 v2 stays `FAIL:SHATTERED` and gains nothing.** It carries zero signature
  fonts; its defect is a different class, and the fifth criterion correctly says
  nothing about it.

---

## 6. M4 — the live database

One read-write open, via `ReviewDatabase("surgical_autonomy")`, after a verified
backup. Everything else this task did against the live DB was `mode=ro`.

| | before | after |
|---|---|---|
| size | 99,770,368 B | **99,770,368 B** |
| mtime | 2026-09-08 03:48:48.732577380 +0000 | 2026-09-09 19:48:46.784974707 +0000 |
| `parse_attempts` rows | **8** | **8** |
| `parse_attempts` columns | 13 | **14** (`font_audit` added) |
| non-NULL `font_audit` | — | **0** (not backfilled) |
| tables | 24 | 24 |
| row counts, all 24 tables | 84,400 total | **identical, table by table — zero differences** |

Backup: `data/backups/review_pre_FONT-AUDIT-02_20260909T194840Z.db`,
sha256 `aff12bb91d33f0644c8eab809a1ffd79de807f395a47bef8753d61c3fba133f1` — equal
to the live file's sha256 at the moment it was taken, verified by comparing both.

`ALTER TABLE ADD COLUMN` rewrites only the schema record, which is why the file
size is unchanged to the byte. The WAL and SHM were removed on the clean close and
recreated by the subsequent read-only opens, at 0 and 32,768 bytes.

---

## 7. Acceptance gates

| # | gate | result |
|---|---|---|
| 1 | Phase 1 acknowledged before edits | **PASS** — Phase 1 reported with a clean tree; edits began after the ruling |
| 2 | standard gate passes; 1,869 + tests added | **PASS** — **1,894 passed, 17 deselected** = 1,869 + 25 (5 in `test_parse_quality`, 3 in `test_font_audit`, 17 in the new `test_font_audit_ledger`) |
| 3 | twelve-paper re-assessment: no verdict changes; 719/586/699 v2 equal `lo × 10` | **PASS** — 0 of 16 rows changed PASS/FAIL; 37.281 / 9.781 / 8.631 exactly |
| 4 | M4 report: backup sha256, 8 rows before and after, all counts unchanged, column present, size/mtime | **PASS** — §6 |
| 5 | live DB opened read-write exactly once; tests never touch it | **PASS** — one `ReviewDatabase("surgical_autonomy")` open, in M4. `tests/test_live_db_guard.py` green in every run, and the new ledger tests build their databases with `ReviewDatabase(name, data_root=tmp_path)` |
| 6 | three commits scoped as listed; `ls-remote` == local HEAD; tree clean | **PASS** — §8 |

---

## 8. Commits

| commit | scope |
|---|---|
| `ebdbd68` | `feat(parse)` — the criterion, the threshold, `font_structure`, the CRITERIA pin at five, tests |
| `d6972da` | `feat(db)` — the migration line, the ledger column and payload, the cascade wiring, the cost guard, tests |
| *(this file)* | `docs(session-reports)` — the PARSE-GATE-10 addendum and this read-out |

---

## 9. Observed, not asked about

1. **`_run_migrations()` does not run migrations 010 and 011.** Found while
   resolving I4. Both are numbered modules with `run_migration(db_path)`, both are
   tested, and neither is called from `ReviewDatabase`'s self-migrating open —
   they were applied to the live database by hand. A reader who assumes "numbered
   migration ⇒ runs on open", which 006–009 teach, will be wrong about the two
   most recent. Queued as MIGRATION-WIRE-01.
2. **A blank parse now takes the structure path, not the alignment path.** An
   audit of empty text is arithmetically valid and reports an exposure of
   **0.0** — the one number this design refuses to write for a document nobody
   could measure. `EMPTY_TEXT` already decides that verdict; the ledger row should
   say the text was absent, not that it was clean. Found because a total-failure
   cascade test produced a row with `reason: null` and exposure 0.0.
3. **Two timeout tests were silently skipping.** Written against the `fpdf`
   fixture, which embeds no font, they hit their `pytest.skip` guard and asserted
   nothing; one of them "checked" a reason string by comparing it with a string
   typed into the test. Both now run against a six-page synthetic signature PDF
   and read the page counts **out of** the produced reason. A one-page fixture
   can never show a per-page deadline biting.
4. **`ParseAttempt` was already shaped for this.** It is a pydantic model with
   defaulted fields, so `font_audit: dict | None = None` required no change at any
   of its three construction sites, and the ledger's single write path
   (`_insert_attempts`, two callers) meant one INSERT to edit.
