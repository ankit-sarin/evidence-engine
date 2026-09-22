# READERS-01 — Phase 1 read-out (read-only)

**Session 6, S2 readers · S1c · S3h.** Task: READERS-01 Part 0 + Phase 1.
**Date:** 2026-09-22 (UTC). **HEAD at read:** `c650347a7d10e75421da8a79368434f8f40a1f17`
(Part 0 commit made in this session, docs only; no code file touched).
**Database:** opened `mode=ro` throughout; never `immutable=1`; not written.

This read-out is the evidence for Phase 2 (reader migration, cut-over, two-axis state) and
Phase 3 (018 rehearsal and live write). **It writes no engine code and no database row.**
Every code claim below cites a quoted content anchor, never a line number.

---

## §0 — INFERRED items I1–I8: verdicts

| id | verdict | quote / evidence |
|---|---|---|
| **I1** | **TRUE** | `engine/migrations/runner.py::run` — `for migration_id, path in discover(): / if migration_id in have: / result["already"].append(migration_id) / continue`, and before the loop `drifted = check_drift(conn) / if drifted: raise MigrationDrift("refusing to run: these migration files changed after they were applied — …")`. There is **no force flag and no re-run path**. 014's receipt is present (see §4). R27 stands unchanged. |
| **I2** | **NOT VERIFIABLE as written** — no on-disk source | No wrap proposal for these two lines exists in this repository or in `~/claude-config/PROJECT_LEDGER.md` (searched for `ruling about a table`, `does not wrap a migration`, `017 pattern`, `marker last`: zero hits in `docs/`, `CLAUDE.md`, `primer.md`, `~/claude-config/`). Line **(b)** is, however, corroborated **verbatim in substance** by the plan's session-5 closure paragraph: *"the migration runner does **not** wrap a migration in a transaction — it closes its own connection (`conn.close()  # migrations open their own connection`) and calls `module.run_migration(db_path)`, so each module owns its transaction, and the docstring's 'one transaction per migration' describes the **receipt** write."* Line **(a)** has **no on-disk source at all**; the closest support is the same paragraph's *"Two architect rulings were reversed by measurement before any code was written, and the cause is the same in both: each was made from decision-log text without the v2.1 table beside it."* **Action taken:** the brief's own text for both lines was used. See §9 Q4. |
| **I3** | **PARTLY FALSE** — more than one candidate for "exporter"; judge loader is unambiguous | Judge loader = **`analysis/paper1/judge_loader.py`** (sole candidate; `"""Loader: disagreement CSV + codebook YAML → list[JudgeInput]."""`). "Exporter" has **three** candidates carrying the A1 *latest-extraction* rule, and **none is under `engine/` and named "concordance exporter"**: `engine/exporters/evidence_table.py`, `engine/exporters/docx_export.py`, `engine/exporters/trace_exporter.py`. A fourth, `analysis/paper1/export_disagreement_pairs.py`, is the Paper-1 field-value exporter but lives under `analysis/`, not `engine/`. All four are inventoried in §1. **See §9 Q1 — the PI names which.** |
| **I4** | **PARTLY FALSE** | Signatures take a **connection only, never a path**, and `effective_value` requires a keyword-only `sentinels`: `def effective_value(conn, paper_id, field_name, arm, *, sentinels) -> EffectiveValue:` and `def effective_state(conn, paper_id) -> EffectiveState:`. `effective_state` returning **one** token is **TRUE**: `@dataclass(frozen=True) class EffectiveState: state: str; provenance: dict`. |
| **I5** | **PARTLY TRUE** | `engine/core/corpus.py` computes membership from a status **string handed to it**; it opens no database and never reads `papers.status` itself. The exclusion of `EXTRACT_FAILED` is **by omission from a positive allowlist**, not by an exclusion clause: `CORPUS_STATUSES: tuple[str, ...] = ("FT_ELIGIBLE", "EXTRACTED", "AI_AUDIT_COMPLETE", "HUMAN_AUDIT_COMPLETE")` and `return status in CORPUS_STATUSES`. A9's substance holds; its mechanism is an allowlist, which matters for §1's cut-over. |
| **I6** | **TRUE, with two nuances that bear on §3** | `to_state TEXT NOT NULL CHECK (to_state IN ({_in(TO_STATES)}))` over `TO_STATES = ("eligible", "abstract_out", "full_text_out", "full_text_not_obtainable", "audited_ai")`. Nuance 1: `full_text_not_obtainable` **is already one of S3h's four processing reasons**, sitting on the eligibility column. Nuance 2: `PAPER_EVENT_TYPES` **already contains `"extraction_failed"`** with **no `to_state` token it can carry**, and `to_state` is `NOT NULL`. |
| **I7** | **TRUE** | `docs/architecture/pipeline.md`: *"**Load arms:** `load_arm()` — queries evidence_spans (local) or cloud_evidence_spans (cloud arms) **or human_extractions (human_A/B/C/D)**."* `docs/architecture/modules.md`: *"`load_arm(db_path, arm)` — Load from evidence_spans (local) or cloud_evidence_spans (cloud) **or human_extractions (human_\*)**."* Neither branch exists in `concordance.py` (see §1). |
| **I8** | **TRUE**, with the site named precisely | The claim is in **`run()`'s** docstring, not the module docstring: *"One transaction per migration, committed before the next begins, so a failure leaves the earlier receipts intact and the failing one absent — the database then says exactly how far it got."* The body then does `conn.close()  # migrations open their own connection` before `module.run_migration(str(db_path))`, and re-opens only to `_write_receipt(...)`/`conn.commit()`. The sentence is true of the **receipt**, not of the migration. |

**No item is false in a way that blocks Part 0**; I1, on which R27 rests, is TRUE. The four
partial items (I2–I6) are recorded above and carried into §2–§4 as findings, which is what a
read-out is for. They are restated in the final report's contradiction list.

---

## §1 — Reader inventory

### 1.1 `engine/analysis/concordance.py` — `load_arm`, and both `connect` sites

**Current data source, quoted.** Two branches, no registry:

```
        if arm == "local":
            rows = conn.execute(
                """SELECT e.paper_id, es.field_name, es.value
                   FROM evidence_spans es
                   JOIN extractions e ON e.id = es.extraction_id
                   ORDER BY e.paper_id, es.field_name"""
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT ce.paper_id, cs.field_name, cs.value
                   FROM cloud_evidence_spans cs
                   JOIN cloud_extractions ce ON ce.id = cs.cloud_extraction_id
                   WHERE ce.arm = ?
                   ORDER BY ce.paper_id, cs.field_name""",
                (arm,),
            ).fetchall()
```

Three facts follow from that text and nothing else:

* **A7 is visible in the SQL.** The local branch has **no run selection at all** — no
  `MAX(e.id)`, no `WHERE e.id = …`. It folds **every** extraction a paper ever had, and the
  dict write `result[pid][row["field_name"]] = row["value"]` means the **last row in
  `ORDER BY e.paper_id, es.field_name` wins**, which is neither newest nor oldest by design.
* **A12 is visible in the `else`.** A `human_*` arm falls into the cloud branch, matches no
  `ce.arm`, and returns `{}` **with no error**.
* The **non-value guard** drops terminal tokens before the value is stored:
  `if str(row["value"] or "").strip().upper() in non_value: continue`, where
  `non_value = non_value_tokens_for(Path(db_path).parent / "extraction_codebook.yaml")`.

**Both `connect` sites (I5), quoted.** `load_arm`: `conn = sqlite3.connect(db_path)` followed by
`conn.row_factory = sqlite3.Row`. `check_schema_parity`: the identical pair. **Neither carries
`mode=ro`.** `check_schema_parity`'s own branching is a third copy of the two-way routing:
`if arm == "local": … "SELECT DISTINCT codebook_hash FROM extractions" … else: … "SELECT DISTINCT
codebook_hash FROM cloud_extractions WHERE arm = ?"`.

**Call sites (grep + read, quoted anchors).**

| caller | anchor |
|---|---|
| `engine/analysis/concordance.py::run_concordance` | `data_a = load_arm(db_path, arm_a)` / `data_b = load_arm(db_path, arm_b)` |
| `analysis/paper1/export_disagreement_pairs.py` | `arm_data = {arm: load_arm(db_path, arm) for arm in ARMS}` |
| `analysis/paper1/adjudication.py` | `from engine.analysis.concordance import load_arm, align_arms` … `data = load_arm(db_str, arm)` |
| `tests/test_concordance.py` | `result = load_arm(str(db_path), "local")` |
| `tests/test_non_value_tokens_downstream.py` | `got = load_arm(str(db), "local")` |
| `tests/test_effective_reader.py` | `"""A7: load_arm folded every extraction and the older claim stood."""` (fixture reproducer; does not call it) |

`analysis/eval/score_screen2f.py::load_arm` and `scripts/_pass2_eyeball.py::load_arm_value*` are
**different functions of the same name** over screening run directories and the pairs CSV — not
call sites. Recorded so the name is not over-applied.

**What the reader-backed replacement must return for the call site to be unchanged.**
`load_arm` returns `dict[int, dict[str, str]]` and `align_arms` consumes `arm_a[pid]` /
`arm_b[pid]` and `set(fields_a.keys()) | set(fields_b.keys())`. So the replacement must return
**the same nested-dict shape**, populated only where the reader's state is value-bearing
(`asserted with evidence` / `asserted without locatable evidence` / `corrected by human`), and
must **omit** the key where the state is `missing`, `declined`, `contract unmet`, `withdrawn` or
`out of scope` — because `align_arms` already models an absent key as "this arm recorded no
value here", which is exactly the representation the existing non-value guard chose.

**Where the call site itself must change.** `arm` is already positional and required here, so
R18/Q4 costs nothing at `load_arm`. What must change is **`ARMS`**: the three call sites named
above take their arm list from a **module literal** —
`ARMS = ["local", "openai_o4_mini_high", "anthropic_sonnet_4_6"]` in
`export_disagreement_pairs.py` — which under R12 must come from the `arms` registry. `load_arm`
must also stop opening its own connection or open it `mode=ro` (see §5).

**REMOVED at cut-over (R30).** The whole `if arm == "local": … else: …` block in `load_arm`;
the same two-way branch in `check_schema_parity`; both bare `sqlite3.connect(db_path)` calls;
the module-literal `ARMS` in `export_disagreement_pairs.py`. Nothing is retained beside the
reader.

### 1.2 The exporter (I3 — four candidates, PI to name)

| candidate | data source, quoted | what it is |
|---|---|---|
| **`engine/exporters/evidence_table.py`** | `"SELECT id FROM extractions WHERE paper_id = ? ORDER BY id DESC LIMIT 1"` then `"SELECT * FROM evidence_spans WHERE extraction_id = ?"`; paper set from `f"SELECT * FROM papers WHERE status IN ({placeholders}) ORDER BY id"` over `qualifying_statuses = [s for s, level in _STATUS_ORDER.items() if level >= min_level]` | **The A1 row-3 "exporter (latest extraction)".** CSV + Excel evidence table. Also carries a second reader in its Audit Log sheet: `FROM evidence_spans es JOIN extractions e ON e.id = es.extraction_id JOIN papers p ON p.id = e.paper_id` with **no** latest-extraction filter — so **one file disagrees with itself**. |
| **`engine/exporters/docx_export.py`** | identical `"SELECT id FROM extractions WHERE paper_id = ? ORDER BY id DESC LIMIT 1"` + `"SELECT field_name, value FROM evidence_spans WHERE extraction_id = ?"` | second copy of the same rule |
| **`engine/exporters/trace_exporter.py`** | `"WHERE e.id = (SELECT MAX(e2.id) FROM extractions e2 WHERE e2.paper_id = p.id)"` and `"SELECT MAX(e2.id) FROM extractions e2 GROUP BY e2.paper_id"` | third copy, expressed differently |
| **`analysis/paper1/export_disagreement_pairs.py`** | `arm_data = {arm: load_arm(db_path, arm) for arm in ARMS}` — i.e. it inherits concordance's rule, **not** the latest-extraction rule | the Paper-1 field-value exporter; under `analysis/`, not `engine/` |

**A1's "three reader pairs disagree structurally" is understated by one:** `evidence_table.py`'s
two queries disagree with each other, and the two `engine/` latest-extraction copies disagree
with `analysis/paper1`'s fold-everything copy.

**What the replacement must return.** Per paper, per codebook field, one `EffectiveValue` for
the named arm. `evidence_table.py` emits four columns per field —
`[fname, f"{fname}_snippet", f"{fname}_confidence", f"{fname}_audit"]` — so the reader must
supply value, snippet, and a state that can stand where `audit_status` stands. `confidence` has
**no** counterpart in the event store; it is a per-span column the reader does not model.
**See §9 Q2.**

**Where the call site must change.** `min_status` selects papers by
`_STATUS_ORDER` over `papers.status`; under R29/S3h that becomes eligibility axis + processing
axis, and `min_status=HUMAN_AUDIT_COMPLETE` becomes "eligible **and** a human decision exists on
the field", which is a **per-field**, not a per-paper, predicate. `arm` becomes a **required
argument** on every exporter entry point (R18/Q4) — today none of the three `engine/` exporters
takes one, and all three silently mean `local`.

**REMOVED at cut-over (R30).** Every `ORDER BY id DESC LIMIT 1` / `MAX(e2.id)` extraction
selection; every direct `evidence_spans` read; the `_STATUS_ORDER`-derived paper query.

### 1.3 The judge loader — `analysis/paper1/judge_loader.py`

**Current data source, quoted.** The cell set is **the CSV**, not a query:

```
    with csv_path.open(newline="") as f:
        rows = list(csv.DictReader(f))

    rows.sort(key=lambda r: (int(r["paper_id"]), r["field_name"]))
```

and the arm list is a **module literal**:

```
CSV_VALUE_COLS = (
    ("local_value", "local"),
    ("o4mini_value", "openai_o4_mini_high"),
    ("sonnet_value", "anthropic_sonnet_4_6"),
)
```

Spans come from a **fourth** copy of the latest-extraction rule, via a private attribute on an
open read-write handle (`db._conn`):

```
           WHERE e.paper_id = ?
           AND e.id = (
               SELECT MAX(e2.id) FROM extractions e2
               WHERE e2.paper_id = e.paper_id
           )
```

and, for cloud, `AND ce.id = (SELECT MAX(ce2.id) FROM cloud_extractions ce2 WHERE
ce2.paper_id = ce.paper_id AND ce2.arm = ce.arm)` — a per-arm latest, which **the
`UNIQUE(paper_id, arm)` R16 drops makes load-bearing**: today that subquery can only ever return
the one row the UNIQUE permits; after the drop it becomes a real selection.

**B3 is a property of the CSV producer, not of this file.** The loader applies no verdict
filter; it reads whatever rows it is given, and `--pairs-csv
data/surgical_autonomy/exports/disagreement_pairs_3arm.csv` contains only
*"all paper × field combinations where at least one arm-pair disagrees (MISMATCH or AMBIGUOUS)"*
(`export_disagreement_pairs.py` docstring). **Measured:** that file holds **2,267 rows**, one
per distinct `(paper_id, field_name)`, over **189 papers** and **21 field names** — the 20
codebook fields plus the A10 parse artefact `field_1`.

**Call sites.** `analysis/paper1/judge_cli.py` (`--input AI_TRIPLES --pairs-csv …`),
`analysis/paper1/pass2_full.py`, `pass2_smoke.py`, `pass2_retry_single.py`,
`pass2_branchB_report.py` — every one of them takes `--pairs-csv` on its command line.

**What the replacement must return for the call site to be unchanged.** `load_ai_triples_csv`
returns `list[JudgeInput]`, each with `paper_id`, `field_name`, `field_type`,
`field_definition`, `field_valid_values`, and `arms: list[ArmOutput]` where
`ArmOutput(arm_name=…, value=…, span=…, precheck_flags=…)`. A reader-backed enumerator must
produce **the same list**, over the **full grid** (R28), with the scorer verdict carried as an
**additional feature on the record** rather than as the thing that decided whether the record
exists.

**Where the call site must change.** `--pairs-csv` stops being the universe and becomes an
optional **feature source** (or is dropped entirely and the verdict recomputed through
`engine.analysis.scoring`). The arm list moves from `CSV_VALUE_COLS` to the registry. The grid's
size must be a **derivation**, never a literal: **190 corpus papers × 20 codebook fields = 3,800
(paper, field) cells**, 11,400 (paper, field, arm) cells, and — since C(3,2)=3 with three
registered arms — 11,400 (paper, field, arm-pair) cells. **The plan's `3,802` is a measurement
of a day and must not be re-pinned** (Working Practices: *a count is a measurement of a day, not
an invariant*); R28 already declares it moot.

**REMOVED at cut-over (R30).** `load_ai_triples_csv`'s CSV-as-universe path; `CSV_VALUE_COLS`;
`_fetch_spans_for_paper`'s two `MAX(…)` queries; the `db._conn` private-attribute reads.

### 1.4 `engine/core/corpus.py` — the corpus predicate

**Quoted in full** (the predicate is three lines):

```
CORPUS_STATUSES: tuple[str, ...] = (
    "FT_ELIGIBLE",
    "EXTRACTED",
    "AI_AUDIT_COMPLETE",
    "HUMAN_AUDIT_COMPLETE",
)

def is_corpus_member(status: str | None) -> bool:
    return status in CORPUS_STATUSES

def corpus_status_sql(column: str = "status") -> tuple[str, tuple[str, ...]]:
    placeholders = ", ".join("?" * len(CORPUS_STATUSES))
    return f"{column} IN ({placeholders})", CORPUS_STATUSES
```

The module's own docstring states the rule and its limit: *"A paper is a corpus member once
full-text screening has admitted it and it has not since left by a failure or exclusion branch"*
and *"`engine.core.database.ALLOWED_TRANSITIONS` is a transition graph with no marking of which
states are successes."* **A9 is an allowlist omission**, not an exclusion clause (see I5).

**Call sites (production).** `engine/cloud/base.py` ×2 — `corpus_sql, corpus_params =
corpus_status_sql("p.status")` and `corpus_status_sql()`; `analysis/eval/schema_eval2.py` ×2 —
`corpus_status_sql()` and `is_corpus_member(statuses.get(pid))`;
`engine/migrations/017_seed_event_store.py` ×2 — `frag, params = corpus_status_sql()` and
`corpus_status_sql("p.status")`.

**What the replacement must return.** A boolean per paper from the **eligibility axis** of
`effective_state` (R29), and — for the SQL consumers — either a paper-id set to splice as
`IN (…)`, or a rewritten query. `corpus_status_sql` returns **SQL text**, and the event store
has no `status` column to point it at, so the SQL form **cannot survive as SQL**; it must
become an id-set producer. That is a real signature change at four production call sites.

**🔴 Two standing tests pin the current shape and will go red at cut-over.**
`tests/test_corpus_authority.py` asserts the literal appears **only** in `engine/core/corpus.py`
(`assert hits <= allowed`), and, harder:

```
def test_the_three_adopting_sites_carry_no_status_literal():
    for rel in ("engine/cloud/base.py", "analysis/eval/schema_eval2.py"):
        text = (REPO / rel).read_text()
        assert "AI_AUDIT_COMPLETE" not in text, f"{rel} still names a status inline"
        assert "corpus_status_sql" in text, f"{rel} does not use the authority"
```

Removing `corpus_status_sql` from those two files under R30 **fails the second assertion**.
Under Step 4 rule 3 the test is **rewritten to the corrected behaviour, never deleted** — it
becomes "these sites carry no status literal **and** reach the corpus question only through
`effective_state`". Recorded so Phase 2 does not discover it as a surprise.

**REMOVED at cut-over (R30).** `CORPUS_STATUSES`, `is_corpus_member`, `corpus_status_sql` —
**except** that `017_seed_event_store.py` is an **applied migration whose text is checksummed**
(`efcc3a06…d3b8aff`): editing it would trip `MigrationDrift` and make the runner refuse to start.
**017 must not be touched.** Its import of `corpus_status_sql` therefore keeps the function
alive, which means the cut-over is a **rename-and-freeze**, not a deletion: the four production
call sites move to the reader, and the predicate survives only as the thing 017 imported.
**See §9 Q3.**

### 1.5 `engine/validators/distribution_monitor.py::_query_values`

**Quoted in full:**

```
def _query_values(conn: sqlite3.Connection, field_name: str, arm: str,
                  non_value: frozenset[str] = frozenset()) -> list[str]:
    """Route value query to the right table based on arm name."""
    if arm == "local":
        return _query_local_values(conn, field_name, non_value)
    if arm.startswith("human_"):
        extractor_id = arm.split("_", 1)[1]
        return _query_human_values(conn, field_name, extractor_id, non_value)
    return _query_cloud_values(conn, field_name, arm, non_value)
```

**A12 exactly as stated:** three branches here against `load_arm`'s two, and this is the file the
architecture docs describe when they describe `load_arm`. It takes a `conn` (good) and routes on
a **string prefix** (bad under R12). The `human_` branch reads `human_extractions`, a table that
**does not exist on the live database** (CLAUDE.md, Database Tables) — so the branch is not
merely divergent, it is unreachable here.

**What the replacement must return.** `list[str]` of values for one (field, arm) over the
corpus, for entropy and collapse detection. From the reader: the value of every value-bearing
`EffectiveValue` in the grid column for that arm.

**Where the call site must change.** `arm` becomes a registry key, and `arm.startswith("human_")`
goes — the registry's `arm_kind` (`model` | `human_extractor`) is the routing fact, and it is
already a column: `arms(arm_name, arm_kind, configuration_json, configuration_marker,
registered_at, retired_at)`.

**REMOVED at cut-over (R30).** All three branches of `_query_values` and the three
`_query_*_values` helpers behind it.

---

## §2 — Reader surface

### 2.1 What exists today, quoted

```
def effective_value(conn, paper_id, field_name, arm, *, sentinels) -> EffectiveValue:
    """Resolution rule v2.1, first matching row wins."""

def effective_state(conn, paper_id) -> EffectiveState:
    """The paper's lifecycle state, from `paper_events` and nothing else."""

@dataclass(frozen=True)
class EffectiveValue:
    value: str | None
    state: str
    rule_row: int
    provenance: dict = _dc_field(default_factory=dict)

@dataclass(frozen=True)
class EffectiveState:
    state: str
    provenance: dict = _dc_field(default_factory=dict)
```

`effective_state`'s body reads the **last** `paper_events` row and returns its `to_state`:

```
    eid, etype, to_state, from_state, actor, at, payload_json = rows[-1]
    …
    return EffectiveState(to_state, prov)
```

`NO_RECORDED_STATE = "no_recorded_state"` when there are no rows.

**Measured on live (read-only, this session):** `effective_state` over all 190 seeded papers
returns `{'eligible': 190}`. `effective_value` over 20 papers × 20 fields × 3 arms returns
`{('missing', 1): 1200}` — rule row 1, *"assigned, no claim"*. **This is R25 working as ruled**,
not a defect: the field-level history is empty until the freshman smoke run.

### 2.2 Proposed two-axis return shape (R29), at contract level

`effective_state` returns **one object carrying two independent values**. Returning a tuple
would let a caller index the wrong element silently; a frozen dataclass with two named fields
cannot be mis-indexed.

```
@dataclass(frozen=True)
class EffectiveState:
    eligibility: str                 # the axis the corpus predicate reads
    processing: str                  # the axis exports read
    processing_reason: str | None    # non-None iff processing is a failure token
    provenance: dict
```

**Eligibility axis — a closed vocabulary of four**, derived from the *latest paper event whose
`to_state` is an eligibility token*:

| token | meaning | derived from |
|---|---|---|
| `eligible` | in the corpus | the seed's `state_at_migration`, and any later admitting event |
| `abstract_out` | excluded at abstract screening | `screened` / `adjudicated` events |
| `full_text_out` | excluded at full-text screening | `screened` / `adjudicated` events |
| `not_yet_screened` | no eligibility event yet | **absence** of any eligibility-token event |

**Processing axis — a closed vocabulary of six**, derived from the *latest paper event whose
`to_state` is a processing token*:

| token | reason (S3h) |
|---|---|
| `not_started` | — |
| `parsed` | — |
| `extracted` | — |
| `audited_ai` | — |
| `failed` | one of the four S3h reasons, in `processing_reason` |
| `blocked` | one of the four S3h reasons, in `processing_reason` |

**How each axis is derived from the event stream.** Not "the last row wins" — that is what
makes one axis impossible. Each axis scans `paper_events` in `event_id` order and takes the
**last event whose `to_state` belongs to that axis's vocabulary**, independently. A paper with
`eligible` at event 1 and `extraction_failed` at event 9 therefore reads
`eligibility='eligible', processing='failed', processing_reason='extraction failed after
retries'` — which is precisely the row-6 gate's *"`EXTRACT_FAILED` papers stay eligible with a
reason"*, expressed structurally rather than by convention.

**Compatibility.** Today's single `state` field disappears under R30 (clean cut-over). Its only
production consumer is the reader's own test suite; the four production corpus call sites in
§1.4 move to `.eligibility`.

### 2.3 What else is MISSING for §1's consumers

1. **A grid enumerator.** Nothing in `engine/core/` enumerates *paper set × codebook fields ×
   registered arms*. Concordance, the exporter, the judge loader and the distribution monitor
   each build their own universe from a different source (a table scan, `_STATUS_ORDER`, a CSV,
   a per-field query). R28 requires one. Contract: it takes a connection, a codebook path and
   optionally an arm filter, and yields `(paper_id, field_name, arm, EffectiveValue)` for
   **every** combination, `missing` included. Its paper set comes from the **eligibility axis**,
   its field set from the codebook (which is already the sole source of field names —
   `load_absence_sentinels` goes through `engine.core.codebook`, *"never a bare
   `yaml.safe_load`"*), and its arm set from the registry.
2. **An arm-list read from the registry.** `engine/core/events.py` has `register_arm`,
   `retire_arm`, `repin_arm_configuration` — **no lister**. `effective.py` reads `arms` twice
   (`"SELECT arm_kind FROM arms WHERE arm_name = ?"`, `"SELECT configuration_marker FROM arms
   WHERE arm_name = ?"`) but exposes no enumeration. Every consumer in §1 needs one, and every
   one of them currently has a module literal instead. Contract: `registered_arms(conn, *,
   kind=None, include_retired=False) -> tuple[str, ...]`, ordered by `arm_name` so a report is
   reproducible. **This single function closes A12** — it is the "single routing predicate" the
   plan's A12 row names.
3. **A batch form — NOT needed. Measured, not estimated.** The full grid is
   **190 × 20 × 3 = 11,400** `effective_value` calls. Timed on the live database, `mode=ro`,
   this session: **1,200 calls in 0.006 s (≈5 µs/call)**, extrapolating to **≈0.1 s for the full
   grid**; `effective_state` over all 190 papers took **0.001 s**. A batch form would be
   optimisation without a measured problem. **Recommendation: do not build one.** ⚠ That
   measurement is taken on an **empty** field-event store (every cell returns at rule row 1,
   which is the cheapest path). It is therefore an **upper bound on speed, not on cost**, and
   must be **re-measured after the freshman smoke run**, when cells carry real event chains.
   Recorded so the number is not later quoted as an invariant.

---

## §3 — Vocabulary migration

### 3.1 The CHECK today, quoted

```
TO_STATES = (
    "eligible",
    "abstract_out",
    "full_text_out",
    "full_text_not_obtainable",
    "audited_ai",
)
…
        to_state    TEXT    NOT NULL CHECK (to_state IN ({_in(TO_STATES)})),
        from_state  TEXT,
        reason_code TEXT,
        stage_name  TEXT,
```

016's own docstring says why the list is what it is: *"The five below are those four tokenised
plus the unqualified `eligible`, which is what the seed writes: R4 makes corpus membership
eligibility, and R25 seeds eligibility only with processing empty, so `audited_ai` would be the
processing fact the seed is forbidden to carry."*

**Three findings from that text.**

* **`reason_code` and `stage_name` already exist and are unconstrained.** R29's processing
  reason needs **no new column** — only a CHECK.
* **The vocabulary is already two axes collapsed onto one column.** `eligible`, `abstract_out`
  and `full_text_out` are eligibility; `audited_ai` is processing; `full_text_not_obtainable`
  is **S3h reason #1 wearing an eligibility column's clothes**. That is the confusion R29
  removes.
* **🔴 `PAPER_EVENT_TYPES` already contains `"extraction_failed"` and `"parsed"` and
  `"extracted"`, and `to_state` is `NOT NULL` with no token any of them can carry.** Quoted:
  `PAPER_EVENT_TYPES = ("identified", "duplicate_of", "screened", "verified", "adjudicated",
  "acquired", "not_obtainable", "parsed", "extracted", "extraction_failed", "audited",
  "manual_advance", "bypass", "state_at_migration")`. **Writing an `extraction_failed` event
  today is impossible without lying about `to_state`.** A9 is therefore not merely latent in the
  predicate; it is **unwritable in the store**. This is the strongest single argument that the
  vocabulary change is session-6 work and not deferrable.

### 3.2 S3h's closed reason vocabulary — confirmed against Step 3

Quoted from the plan, S3h: *"Failure states carry a reason from a closed vocabulary (**full text
not obtainable; parse failed; extraction failed after retries; input exceeds context**)."*
**Four reasons, exactly as the brief states.** Proposed tokens, one per reason, kebab-free for
CHECK legibility: `full_text_not_obtainable` · `parse_failed` · `extraction_failed_after_retries`
· `input_exceeds_context`.

### 3.3 The CHECK change R29 requires

1. **`to_state`** gains the processing tokens and loses `full_text_not_obtainable` as an
   *eligibility* value (it moves to `reason_code`). Proposed:
   `TO_STATES = ("eligible", "abstract_out", "full_text_out", "not_yet_screened", "not_started",
   "parsed", "extracted", "audited_ai", "failed", "blocked")`.
2. **`reason_code`** gains `CHECK (reason_code IS NULL OR reason_code IN (<the four S3h
   reasons>))`.
3. **A cross-column CHECK**: `CHECK (to_state NOT IN ('failed','blocked') OR reason_code IS NOT
   NULL)` — a failure without a reason is the defect S3h exists to prevent, and a CHECK is where
   that belongs, not in the writer.

**The 190 live rows survive unchanged.** All carry `to_state='eligible'`, which is in both the
old and the new vocabulary, and `reason_code` NULL, which the new CHECK permits.

### 3.4 Recommendation: **019, not 018** — with reasons

**Recommend a separate migration 019.** Four reasons, in order of weight:

1. **The two changes rebuild different tables under different hazards.** 018 rebuilds
   `cloud_evidence_spans` and `cloud_extractions` — **legacy result tables, zero triggers, no
   append-only guarantee**. The vocabulary change rebuilds `paper_events` — **an append-only
   table carrying six triggers and the S2 store's integrity claim**. A single module that does
   both makes one `ROLLBACK` responsible for two unrelated invariants.
2. **Rebuilding `paper_events` means dropping and recreating its triggers.** Measured on the
   fresh database: `paper_events_no_update` and `paper_events_no_delete` exist, among fourteen
   triggers. `DROP TABLE paper_events` drops them with it. A migration that **removes the
   append-only guard, copies 190 rows, and restores the guard** is a substantive piece of work
   whose failure mode is *the guard silently not coming back*, and it deserves its own module,
   its own test and its own name in the receipt table.
3. **R27 names 018's contract precisely** — *"rebuild `cloud_evidence_spans` to the fresh NOT
   NULL shape and drop `UNIQUE(paper_id, arm)` on `cloud_extractions`"*. Adding a third,
   unrelated table to it makes the receipt `018_…` describe something its name does not say, and
   **a receipt is the only record of what ran**.
4. **R29 explicitly permits it**: *"lands this session (in 018 if Phase 1 shows that is clean,
   otherwise as 019)"*. Phase 1 shows it is **not** clean: it crosses the append-only boundary.

**Cost of the recommendation:** one extra migration module, one extra receipt, one extra
rehearsal. **Cost of the alternative:** one transaction responsible for both the legacy cloud
tables and the event store's append-only guarantee. **See §9 Q5.**

---

## §4 — Migration 018 plan (R27)

### 4.1 The runner's skip and checksum logic, and 014's receipt (I1)

**Skip, quoted:**

```
        for migration_id, path in discover():
            if migration_id in have:
                result["already"].append(migration_id)
                continue
            if kind_of(migration_id) == "data" and not include_data:
                result["skipped"].append(migration_id)
                continue
```

**Checksum refusal, quoted:**

```
        drifted = check_drift(conn)
        if drifted:
            raise MigrationDrift(
                "refusing to run: these migration files changed after they were "
                "applied — " + ", ".join(drifted) + ". A migration whose text "
                "changed is a different migration, and this database cannot know "
                "which one it got."
            )
```

over `check_drift`'s `if rec and rec["file_sha256"] != file_sha256(path): drifted.append(...)`.

**014's receipt row on live, measured verbatim:**

```
('014_cloud_tables',
 '0aed58dd126533111ac7be501b28e6abb70c18cda12dcf45f5e5711a261e37bb',
 '2026-09-21T00:28:08.713018+00:00',
 'registered_preapplied', 1,
 'tables present from engine/cloud/schema.py; the NOT NULL tightening on
  cloud_evidence_spans.confidence/tier is NOT present on this database - see the
  MIGRATIONS-01 report')
```

**014 cannot be re-run** (its id is in `have`) and **cannot be edited** (the checksum would
drift and the runner would refuse to start at all — for every migration, not just 014). R27 is
therefore not merely preferable; it is the **only** available route. **I1 verified TRUE.**

### 4.2 Live vs fresh DDL — measured, not asserted

A temporary `ReviewDatabase` was built in the scratchpad (32 tables, migrations 004–016 executed,
data migrations skipped) and fingerprinted. **This is the only place this task created anything,
and it is outside the repository.**

**`cloud_extractions` — live and fresh are STRUCTURALLY IDENTICAL today.** The DDL *text*
differs only in what `ALTER TABLE` leaves behind (live: `extracted_at TEXT NOT NULL,
codebook_hash TEXT, codebook_sha256 TEXT,` on one line; fresh: three aligned lines) — and
`db_fingerprint`'s structure definition excludes exactly that: *"Column ORDER, quoting of the
table name, comments and whitespace are excluded: they are what ALTER TABLE and a rename leave
behind."* Both carry `UNIQUE(paper_id, arm)` and its `sqlite_autoindex_cloud_extractions_1`.

**🔴 Consequence R27 does not state.** Dropping `UNIQUE(paper_id, arm)` on **live only** would
*create* a structural difference where none exists. The drop must land in
**`engine/cloud/schema.py`'s `_CLOUD_SCHEMA`** as well, so a fresh database reaches the same
shape. That edit is **safe**: `014_cloud_tables.py`'s own text is unchanged (it calls
`init_cloud_tables` rather than re-declaring the DDL — *"This migration does not re-declare the
DDL … so the tables have exactly one definition and this file cannot drift from it"*), so its
receipt checksum still matches and the runner does not refuse.

**`cloud_evidence_spans` — the C10 difference, measured:**

| column | live | fresh |
|---|---|---|
| `confidence` | `REAL`, notnull **0** | `REAL`, notnull **1** |
| `tier` | `INTEGER`, notnull **0** | `INTEGER`, notnull **1** |

**Live NULL counts, measured: `confidence IS NULL` = 0, `tier IS NULL` = 0**, over 7,257 rows.
So `init_cloud_tables`'s backfill statements — `UPDATE cloud_evidence_spans SET confidence = 0.0
WHERE confidence IS NULL` and `SET tier = 1 WHERE tier IS NULL` — **are no-ops on this data**.
018 may therefore assert "no row content changed" truthfully. **Had either count been non-zero,
R27's contract and `init_cloud_tables`'s behaviour would have contradicted each other**; they do
not, and the measurement is why we know.

### 4.3 Contract for 018

One migration, `018_cloud_tables_freshman_shape.py`, `KINDS["018"] = "schema"`, module-owned
transaction on the 017 pattern (`BEGIN` … `ROLLBACK` on any exception), **no row content
changed**:

1. `PRAGMA foreign_keys = OFF` (the runner already sets this: `conn.execute("PRAGMA foreign_keys
   = OFF")  # table rebuilds re-point FKs`) — **and this is load-bearing**: with foreign keys
   **on**, `ALTER TABLE … RENAME TO` rewrites `REFERENCES` clauses in other tables, and
   `cloud_evidence_spans` references `cloud_extractions(id)`. **That exact mechanism produced
   A11's phantom `_evidence_spans_old`.**
2. `BEGIN`.
3. Rebuild `cloud_evidence_spans` to the fresh NOT NULL shape: create under a temp name with
   **the fresh column order** (`id, cloud_extraction_id, field_name, value, source_snippet,
   confidence, tier, notes`, which **is already the live order** — verified by
   `PRAGMA table_info`), `INSERT … SELECT` **naming every column explicitly** and preserving
   `id`, drop the old, rename. 7,257 rows. Recreate `UNIQUE(cloud_extraction_id, field_name)`.
4. Rebuild `cloud_extractions` **without** `UNIQUE(paper_id, arm)`, same pattern, preserving
   `id` and column order. 379 rows.
5. `PRAGMA foreign_key_check` before `COMMIT`; any row returned is a `ROLLBACK`.
6. `COMMIT`. Idempotent: on a database already in the target shape (a fresh one after the
   `schema.py` edit) the migration is a no-op that still writes its receipt.

**Companion, not part of the migration:** delete `UNIQUE(paper_id, arm)` from
`engine/cloud/schema.py::_CLOUD_SCHEMA`, and **keep the NOT NULL rebuild branch there**
(after 018 it becomes unreachable on this database, but it is still the only path for any other
database that predates it).

### 4.4 Per-table identity expectation for the live write

`db_fingerprint`'s per-table record is `{"row_count", "columns", "order_basis", "sha256"}`, where
`sha256` hashes `SELECT <colnames in PRAGMA order> FROM <table> ORDER BY rowid ASC`. So:

| table(s) | expectation after 018 |
|---|---|
| `cloud_evidence_spans` | **content `sha256` BYTE-IDENTICAL** (same 7,257 rows, same column order, same rowid order, no value changed); `columns` identical; **structure changes** (`confidence`/`tier` notnull 0 → 1) |
| `cloud_extractions` | **content `sha256` BYTE-IDENTICAL** (same 379 rows); **structure changes** (`sqlite_autoindex_cloud_extractions_1` disappears from `PRAGMA index_list`) |
| `schema_migrations` | **content changes** — exactly one new row, `018_…`, mode `executed` |
| `sqlite_sequence` | unchanged (neither cloud table uses `AUTOINCREMENT`) |
| **the other 29 tables** | **BYTE-IDENTICAL**, per-table `sha256` unchanged |
| `schema_hash_sha256` (textual) | **changes** — the DDL text of both tables is rewritten |
| `schema_structure_hash` | **changes** — two notnull flags and one index |
| `overall_sha256` | **changes** — it folds `schema_migrations`' new row |

**How the rehearsal proves row content unchanged.** On a verified copy (`auto_backup`, whose
`BackupResult` carries the fingerprint that proves the copy): fingerprint → run 018 → fingerprint,
then compare the two records with `structure_differences` and a per-table `sha256` comparison.
**Pass condition, written as a derivation and not as a literal:** every table's `row_count` and
`sha256` equal the pre-run record **except `schema_migrations`**, whose `row_count` is exactly
one greater; and `structure_differences` returns **exactly three** entries — the two
`cloud_evidence_spans.columns` notnull flips and the one `cloud_extractions` index removal.
Anything else is a `ROLLBACK`-and-report.

### 4.5 🔴 G3 would **NOT** become MET — and the reason is a second, unnamed difference

`structure_differences(fresh, live)` was run this session. **Six entries, in two groups:**

```
  - audit_adjudication.foreign_keys: only in live:  ['span_id', '_evidence_spans_old', 'id', 'NO ACTION', 'NO ACTION']
  - audit_adjudication.foreign_keys: only in fresh: ['span_id', 'evidence_spans',       'id', 'NO ACTION', 'NO ACTION']
  - cloud_evidence_spans.columns: only in live:  ['confidence', 'REAL',    0, None, 0]
  - cloud_evidence_spans.columns: only in live:  ['tier',       'INTEGER', 0, None, 0]
  - cloud_evidence_spans.columns: only in fresh: ['confidence', 'REAL',    1, None, 0]
  - cloud_evidence_spans.columns: only in fresh: ['tier',       'INTEGER', 1, None, 0]
```

018 closes the **second** group (C10). It does not touch the **first**, which is **A11** — the
live `audit_adjudication.span_id` foreign key points at the phantom `_evidence_spans_old`,
against `evidence_spans` on a fresh database. A11's disposition is **R18 / Option B: session 5
stopped writing and marked it deprecated; session 12 drops the table.**

**Therefore: after 018 lands, G3 (fresh structure hash == live) remains NOT MET, blocked by A11
until session 12.** The plan's C10 row says *"G3 stays NOT MET until `014` is executed there"*,
which reads as though C10 were the only blocker. **It is not.** This is reported, not adapted
around; it changes no ruling — R27 is still the right change and still worth making — but it
means **the session-3 gate wording "a fresh database's schema hash equals the live one" cannot
be met in session 6 by any migration inside R27's scope.** See §10 and the final report's
contradiction list.

---

## §5 — I5 under R31: the three `connect` sites

| # | site | quoted | verdict |
|---|---|---|---|
| 1 | `engine/analysis/concordance.py::load_arm` | `conn = sqlite3.connect(db_path)` / `conn.row_factory = sqlite3.Row` | **MOOT at cut-over.** Under R30 the two branches are removed and the reader owns the connection; `load_arm` takes a `conn` (as `_query_values` already does) or a path it opens `mode=ro`. No separate `mode=ro` fix is needed if Phase 2 lands the cut-over in the same change. **If the cut-over slips, the interim fix is `sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)` — never `immutable=1`.** |
| 2 | `engine/analysis/concordance.py::check_schema_parity` | `conn = sqlite3.connect(db_path)` / `conn.row_factory = sqlite3.Row` | **NOT moot.** This function reads `extractions.codebook_hash` / `cloud_extractions.codebook_hash` — **legacy provenance the event store does not carry** — so it survives the cut-over as a legacy reader and needs the `mode=ro` form on its own account. |
| 3 | `analysis/paper1/export_disagreement_pairs.py::_load_paper_info` | `conn = sqlite3.connect(db_path)` / `conn.row_factory = sqlite3.Row` / `rows = conn.execute("SELECT id, title, authors, year FROM papers").fetchall()` | **Routed to §7 for a keep/retire verdict, as instructed.** Not assumed to be a `mode=ro` fix. |

**A fourth read-write path I5 does not name.** `analysis/paper1/judge_loader.py` reaches the live
database through `db._conn` — a **private attribute of an open `ReviewDatabase`**, which is
read-write by construction. It is not a `sqlite3.connect` call, so the I5 grep could not see it,
but it is the same exposure. Recorded as an **inventory candidate** (see §10); no row is opened
here, because opening one is an architect decision.

---

## §6 — A12 hygiene: exact corrections for Phase 2

| file | current text (quoted) | correction |
|---|---|---|
| `docs/architecture/pipeline.md` | *"**Load arms:** `load_arm()` — queries evidence_spans (local) or cloud_evidence_spans (cloud arms) **or human_extractions (human_A/B/C/D)**. Returns `{paper_id: {field_name: value}}`"* | Delete the `human_extractions` clause. After Phase 2: *"`load_arm()` — reads `effective_value` for every (paper, field) in the grid for one registered arm; routing is the `arms` registry's `arm_kind`, not a name test."* |
| `docs/architecture/modules.md` | *"`load_arm(db_path, arm)` — Load from evidence_spans (local) or cloud_evidence_spans (cloud) **or human_extractions (human_\*)**. Raises `sqlite3.OperationalError` on DB errors (never returns empty dict on failure)"* | Same deletion. The `sqlite3.OperationalError` clause stays true only while `load_arm` owns a connection; revise with the signature. |
| `engine/migrations/runner.py::run` docstring | *"One transaction per migration, committed before the next begins, so a failure leaves the earlier receipts intact and the failing one absent — the database then says exactly how far it got."* | *"One transaction per **receipt**, committed before the next migration begins. The migration itself runs in its own connection and owns its own transaction (`conn.close()  # migrations open their own connection`); a module that needs atomicity writes its own `BEGIN`/`ROLLBACK`, as 017 does. A failure leaves the earlier receipts intact and the failing one absent."* **Class C row C11.** |

`docs/architecture/pipeline.md`'s other `load_arm` mention — *"`load_arm()` raises on DB error:
`engine/analysis/concordance.py` propagates `sqlite3.OperationalError` on missing or corrupted
databases instead of returning an empty dict"* — is **correct today** and must be re-checked
against the new signature, not deleted blind.

**These are Phase 2 edits. Nothing in §6 was applied in this task** (docs/architecture edits and
docstring fixes are both OUT OF SCOPE).

---

## §7 — Legacy-retention ledger (R31)

Scope: what **this session touches or reads**. Not a tree inventory. "Serves the engine going
forward" is the R31 test; serving a publication is **not** a reason to retain.

### 7.1 Code — `analysis/paper1/`

| artifact | what it does | serves the engine forward? | verdict | owner |
|---|---|---|---|---|
| `judge_loader.py` | builds `list[JudgeInput]` from the disagreement CSV + codebook | **YES** — it is the S1c judge loader row 6 migrates | **keep**, rewritten Phase 2 (grid from the reader, R28) | session 6 |
| `judge.py` | Pass 1 + Pass 2 orchestrator, no DB writes, no CLI | **YES** — Run 7's judge pass runs through it | **keep** | session 13 (S5c) |
| `judge_prompts.py`, `judge_schema.py`, `judge_storage.py`, `precheck.py` | prompt builders, Pydantic schemas, the `judge_*` table writers, deterministic pre-judge flags | **YES** — all four are on the S5c path | **keep** | session 13 |
| `judge_cli.py` | CLI: loader → `run_pass1` → storage | **YES** — the only entry point to a judge run | **keep**, `--pairs-csv` demoted from universe to feature | session 6 |
| `export_disagreement_pairs.py` | builds the 3-arm disagreement CSV/XLSX/HTML; **the file that made the scorer's verdict a filter (B3)** | **PARTLY** — its `build_disagreement_rows` is the scorer-verdict *feature* R28 still needs; its **CSV-as-universe role dies with R28** | **retire the universe role Phase 2, keep the row builder**; the third I5 `connect` site (§5) is fixed or removed with it | session 6 |
| `adjudication.py` | exports AMBIGUOUS concordance pairs for human review, imports decisions | **NO for the engine** — it is one of A2's write-only human-decision paths, replaced by `field_events` | **retire in session 12** (with the importer unification) | session 12 |
| `human_import.py` | parses v2 human workbooks into `human_extractions` | **YES, later** — R14 makes human workbooks arms in session 12 | **keep, untouched**; R14 moves its `CREATE TABLE IF NOT EXISTS` to a numbered migration | session 12 |
| `consensus.py` | majority-vote gold standard over ~30 shared human papers | **NO for the engine** — a Paper-1 analysis, and R22/U3 rules a reference standard *its own arm*, not a cross-arm fold | **retire in session 12** or re-express as an arm | session 12 |
| `pass1_inspection.py`, `pass2_smoke.py`, `pass2_full.py`, `pass2_retry_single.py`, `pass2_branchB_report.py`, `runner_smoke_phase2a.py`, `judge_codebook_smoke.py` | Pass-1/Pass-2 orchestrators, smokes and descriptive reports over the **April/June 2026** judge runs | **NO** — frozen harnesses over legacy runs; telemetry under R30 | **keep frozen** (they have committed reports; deleting orphans a reader — F4) | — |
| `pi_audit_sampler.py`, `pi_audit_sampler_v2.py`, `pi_audit_unblind.py` | blinded PI-audit workbook generation and unblinded judge-reliability scoring | **NO for the engine** — measurement only (R18/Q10) | **keep frozen** | — |
| `judge_provenance.py` | restates Pass-2 verdicts against the frozen v1.1 provenance taxonomy | **NO for the engine** | **keep frozen** | — |
| `spanloss_autopsy.py` | reconstructs the 19 Run-6 single-span extractions (SPANLOSS-01) | **NO** | **keep frozen** | — |

### 7.2 Direct-table paths in §1 (removed at cut-over under R30)

| artifact | what it does | serves the engine forward? | verdict | owner |
|---|---|---|---|---|
| `concordance.load_arm`'s two-branch SQL | folds every extraction (A7); routes by name (A12) | **NO** — it is the defect | **retire Phase 2** | session 6 |
| `concordance.check_schema_parity`'s two-branch SQL | compares `codebook_hash` across arms | **YES** — legacy provenance the event store does not carry | **keep**, + `mode=ro` (§5) | session 6 |
| `evidence_table.py` / `docx_export.py` / `trace_exporter.py` latest-extraction queries | three copies of one resolution rule | **NO** — A1 is exactly this | **retire Phase 2** | session 6 |
| `evidence_table.py` Audit Log query (no latest filter) | the file's self-disagreement | **NO** | **retire Phase 2** | session 6 |
| `judge_loader._fetch_spans_for_paper`'s two `MAX(…)` queries | a fourth copy | **NO** | **retire Phase 2** | session 6 |
| `distribution_monitor._query_values` + its three helpers | prefix routing; a dead `human_` branch | **NO** | **retire Phase 2** | session 6 |
| `corpus.CORPUS_STATUSES` / `is_corpus_member` / `corpus_status_sql` | the status allowlist | **NO for readers — YES as 017's frozen import** | **retire from the four production call sites Phase 2; the module survives because `017_seed_event_store.py` is checksummed and must not be edited** (§1.4, §9 Q3) | session 6 |

### 7.3 The two legacy importers (owned by session 12 — recorded only)

| artifact | what it does | serves the engine forward? | verdict | owner |
|---|---|---|---|---|
| the **xlsx** audit-review importer | binds to the newest extraction; REJECT keeps the value and marks it `verified` (A3, A4) | **NO** — two importers, one vocabulary, two meanings | **retire in session 12** | session 12 |
| the **JSON/per-span** audit-review importer | binds by `span_id`; REJECT sets the value to `NR` | **NO as a second importer** — session 12 unifies to one | **retire in session 12** | session 12 |
| `audit_adjudication` (table) | 0 rows; FK to the phantom `_evidence_spans_old`; **deprecated** — `import_audit_review_decisions` refuses at its single entry point | **NO** | **retire in session 12** (R18 / A11 Option B); **note it is the other half of the G3 gap, §4.5** | session 12 |

### 7.4 Committed / on-disk outputs

| artifact | what it does | serves the engine forward? | verdict | owner |
|---|---|---|---|---|
| **The March kappa artifact** — `data/surgical_autonomy/exports/disagreement_pairs_3arm.{csv,xlsx,html}` (2026-03-20; **sha256 `1772407d…b64b7c` / `a181a895…3cf1d994` / `34a4e86d…4d9f7e12f6`**) | the published figures, built on two wrong instruments (B1, B2) | **YES, narrowly** — it is the **input to every judge run on disk** and the **regression fixture** for S1b's corrected scorer | **keep, byte-frozen.** Its own sidecar already binds this: *"The March files are **unchanged and must stay that way**: they are the record of what was published, and a record that gets edited is not one."* **Not retained for the publication** — retained because the engine's S1b/S1c regression reads it | session 6 |
| **Its `SUPERSEDED` sidecar** — `disagreement_pairs_3arm.SUPERSEDED.md` | names the three superseded hashes, the replacement, the old-vs-new CSV, and why | **YES** — without it the frozen files read as current | **keep** | — |
| The **regenerated** `disagreement_pairs_3arm_20260920T230645Z.{csv,xlsx,html}` + `…_kappa_old_vs_new_….csv` | INSTRUMENTS-01's corrected replacement, 2,266 rows, 63 old-vs-new kappa rows | **YES** — the current legacy comparator | **keep** | — |
| **The Run 6 judge subset** — `judge_runs` / `judge_ratings` / `judge_pair_ratings` / `fabrication_verifications` (measured: 2 Pass-1 runs at **10** and **2,266** ratings; 5 Pass-2 runs; **7,422** verifications) | the April/June judge output over the scorer's disagreement set (B3) | **YES** — the **regression fixture and telemetry R25 keeps them for**, and the session-8 reuse-key / input-identity tests read them | **keep under R25**, read-only, never rewritten | — |
| Legacy DB tables `extractions` (190) / `evidence_spans` (3,760) / `cloud_extractions` (379) / `cloud_evidence_spans` (7,257) | the pre-event-store result stores | **YES** — R25's stated engine reason: *"they remain in place, read-only, as a regression fixture and telemetry"*, and session 8's reuse-key and input-identity tests need real rows | **keep under R25**; 018 changes only their **shape**, never a row | session 6 |
| `docs/architecture/pipeline.md`, `docs/architecture/modules.md` | the 6-file code-audited architecture reference | **YES** — but **two passages are false today** (I7) | **keep, correct in Phase 2** (§6) | session 6 |

**Nothing in this ledger was executed.** R31 verdicts are recorded; retirement happens in the
session that owns the artifact. **No legacy file was deleted, moved or renamed in this task.**

---

## §8 — Fixture plan for Phase 2

All fixtures are **constructed event histories** on a scratch database. **No live data.** Each is
built by calling `engine/core/events.py`'s writers, so a fixture that the writer would refuse
cannot exist — which is itself part of what is being tested.

| fixture | shape | proves |
|---|---|---|
| **F1 — two arms, one paper, one field** | register `arm_x` (model) and `arm_y` (model); `asserted` on each with different values; one `citation_located` with `located: true` on `arm_x` only | the grid enumerator yields **both** arms; `effective_value` returns rule row 10 for `arm_x` and row 11 for `arm_y`; no per-arm branch anywhere |
| **F2 — eligible with an extraction failure** | paper event `to_state='eligible'`; later paper event `event_type='extraction_failed'`, `to_state='failed'`, `reason_code='extraction_failed_after_retries'` | **the row-6 gate.** `effective_state().eligibility == 'eligible'` (the paper **stays in the corpus**), `.processing == 'failed'`, `.processing_reason == 'extraction_failed_after_retries'`; the corpus predicate counts it; the export reports it by reason. **Not constructible before the §3.3 CHECK change** — this fixture is the gate's own reproducer |
| **F3 — a declined field** | `declined` event, zero citations | rule row 14, state `declined`, `value is None`; the enumerator yields the cell; `load_arm`'s replacement **omits** the key so `align_arms` sees an absent value |
| **F4 — a withdrawn value (D1-4)** | `asserted` claim, then `human_withdrew` by an `actor_role='reviewer'` event whose `field_event_against` names that claim | rule row 4, state `withdrawn`; **and the A7 reproducer**: a second, older `asserted` on the same cell does **not** resurface — which is what `load_arm`'s fold did |
| **F5 — full-grid enumeration, verdict not a filter (S1c/R28)** | 3 papers × 4 codebook fields × 3 arms = 36 cells; populate **only 5** with claims; make 2 of the populated ones an all-MATCH triple under the scorer | the enumerator yields **36** records; the judge loader builds **36** `JudgeInput`s; the 2 all-MATCH cells **are attempted**, with the scorer verdict present as a field on the record. The count is asserted as `len(papers) * len(fields) * len(arms)`, **a derivation, never the literal 36** |
| **F6 — an unregistered arm** | `effective_value(..., arm="arm_z")` with `arm_z` absent from `arms` | `UnknownArm` — *"an arm that does not resolve in the review's registry"* — so a typo in a migrated caller fails loudly rather than returning `{}` the way `load_arm`'s `else` branch does today. **This is A12's regression test** |
| **F7 — a retired arm** | register, claim, then `retire_arm` | the registry lister excludes it by default (`include_retired=False`) while its claims still resolve through `effective_value` (R21: *"its claims and decisions stand"*) |

**F2 is the load-bearing one** and it **cannot be built against the CHECK as it stands today**
(§3.1). That is the dependency between §3 and §8: the vocabulary migration is a prerequisite for
the row-6 gate's own reproducer, not a tidy-up that can follow it.

---

## §9 — Questions for the PI

R27–R31 are settled and none is questioned. Five questions remain where a read shows a genuine
open choice.

**Q1. Which file is "the exporter" in the row-6 gate?** I3 named one; disk holds four (§1.2).
*Recommended answer:* **all three under `engine/exporters/` — `evidence_table.py`,
`docx_export.py`, `trace_exporter.py` — migrate in Phase 2; `analysis/paper1/export_disagreement_pairs.py`
is handled under §7 as a legacy universe-producer, not as "the exporter".**
*Trade-off:* migrating three files rather than one is roughly three times the Phase 2 exporter
work and widens the cut-over blast radius. Migrating only one leaves **two live copies of the
rule A1 exists to remove**, and R30 forbids a reader keeping its direct-table path — so a
one-file answer would leave the session's own ruling violated in two files. The cheaper variant,
if the PI wants Phase 2 smaller: migrate `evidence_table.py` now and **name the other two in the
plan as session-6-Phase-2b**, so they are scheduled rather than forgotten.

**Q2. `confidence` has no counterpart in the event store — what does the migrated
`evidence_table.py` emit in the `{field}_confidence` column?** Today it is the per-span
`evidence_spans.confidence`. The event store models located/not-located, not a scalar.
*Recommended answer:* **drop the column and emit `{field}_state` and `{field}_rule_row`
instead** — the reader's state and the numbered v2.1 row that produced it, which is strictly
more information and is auditable. *Trade-off:* it changes a published CSV's column set, so any
downstream reader of `evidence_table.csv` breaks. Measured: the only on-disk consumer is the
March `evidence_table.{csv,docx,xlsx}` (2026-03-08), which is frozen output, not an input. The
alternative — carrying `confidence` forward as NULL — keeps the header stable and publishes a
column that means nothing, which is the shape of defect C7.

**Q3. `engine/core/corpus.py` cannot be deleted, because applied migration `017_seed_event_store.py`
imports `corpus_status_sql` and its text is checksummed (`efcc3a06…d3b8aff`). Editing 017 would
make the runner refuse to start for every migration.** So R30's "removed, not retained" cannot be
literal here. *Recommended answer:* **keep the module, delete nothing, and mark it frozen** — add
a module-docstring line saying it exists **only** as the migration-017 import and that no reader
may call it, plus a test that greps for callers outside `engine/migrations/` and `tests/`.
*Trade-off:* a frozen module is a thing a future reader can still call, which is exactly the
coexistence R30 forbids — the grep test is what converts "please don't" into "the suite goes
red". The alternative, vendoring the four statuses into 017 as a literal, would require editing
017 and is therefore not available at all.

**Q4. The two CLAUDE.md lines have no recoverable wrap proposal (I2).** Nothing in the repo or in
`~/claude-config/PROJECT_LEDGER.md` records what the wrap proposed, so "the wrap's proposal wins"
could not be evaluated. The brief's text for both lines was used, placed in **`## Ops Invariants
— the database`** — because the project `CLAUDE.md` has **no assumption-discipline section** at
all (its sections are Location, Deployment, Purpose, Project Structure, Agent Architecture, Data
Architecture, Paper Lifecycle, Pipeline Stages, Inference, Key Patterns, two Ops Invariants
sections, and the architecture blocks), and the global `~/.claude/CLAUDE.md` is a symlink into
`~/claude-config/`, which this brief forbids editing. *Recommended answer:* **accept the
placement**, and note that line (b) as added **explicitly corrects** the adjacent pre-existing
clause *"The runner applies `engine/migrations/NNN_*.py` in order, **one transaction each**"* —
which carries the same wrong claim as the runner docstring. *Trade-off:* correcting the old
clause in place would be cleaner prose, but `CLAUDE.md` is the PI's file and this brief
authorised adding lines, not rewriting existing ones. **If the PI prefers, the old clause can be
struck in Phase 2 alongside the C11 docstring fix.**

**Q5. 018 or 019 for the `to_state` vocabulary change?** §3.4 recommends **019**, because the
vocabulary change rebuilds `paper_events` — an **append-only table whose two triggers must be
dropped and restored** — while 018 rebuilds two trigger-free legacy tables, and R27 names 018's
contract precisely. *Trade-off:* 019 costs one more module, one more receipt and one more
rehearsal in Phase 3; 018-only saves that but makes a single `ROLLBACK` answerable for both the
legacy cloud tables and the event store's integrity claim, and gives the `018_…` receipt a name
that does not describe what it did.

---

## §10 — Risks, and what would stop Phase 2 / Phase 3

| # | risk | what it stops | detection |
|---|---|---|---|
| **R1** | **F2 is unbuildable until the `to_state` CHECK changes** (§3.1, §8). The row-6 gate's own reproducer depends on a migration that has not been written. | **Phase 2 stops** if the vocabulary migration is deferred: the gate would be declared met by a test that could not have failed — *a check that passes before the change it verifies is not a check*. | Try to write F2 first. It raises `CHECK constraint failed` today. |
| **R2** | **G3 cannot be met in session 6** — A11's `_evidence_spans_old` foreign key is a second structural difference outside R27's scope (§4.5). | Nothing, if the plan's C10 row and the session-3 gate wording are corrected. **Phase 3 stops** if its acceptance gate is written as "G3 becomes MET", because that gate cannot pass. | Already measured: `structure_differences` returns six entries in two groups. |
| **R3** | **Dropping `UNIQUE(paper_id, arm)` on live only would *create* a G3 difference** where none exists today (§4.2). | **Phase 3 stops** if `engine/cloud/schema.py` is not edited in the same change. | Post-018 `structure_differences` would gain a `cloud_extractions.indices` entry. |
| **R4** | `judge_loader._fetch_spans_for_paper`'s `MAX(ce2.id)` per-arm subquery is **inert today because the UNIQUE guarantees one row**, and becomes a live selection the moment 018 drops it. | Nothing by itself — but any legacy read of `cloud_extractions` written before 018 must be re-read afterwards. | Grep for `cloud_extractions` readers before Phase 3; four are in §1. |
| **R5** | `tests/test_corpus_authority.py::test_the_three_adopting_sites_carry_no_status_literal` asserts `"corpus_status_sql" in text` and **goes red at cut-over** (§1.4). | Nothing — it is expected, and Step 4 rule 3 says rewrite, never delete. Recorded so it is not mistaken for a regression. | The standard gate. |
| **R6** | The 5 µs/call reader measurement was taken on an **empty field-event store**; every cell returned at rule row 1, the cheapest path (§2.3). | Nothing now. It would stop a later "no batch form needed" claim from being true. **Re-measure after the freshman smoke run.** | Re-run the timing when `field_events` is non-empty. |
| **R7** | `judge_loader` reads the live database through `db._conn`, a **private attribute of a read-write `ReviewDatabase`** — a fourth read-write path I5 does not name (§5). | Nothing in Phase 2. It is an inventory candidate the architect may wish to open as an I-class row. | Grep `_conn` outside `engine/core/database.py`. |
| **R8** | **Under R25 the migrated readers return no field values on live** — measured: 11,400 cells, all `missing` at rule row 1. Any Phase 2 acceptance gate written against live data would pass vacuously. | **Phase 2 stops** if a gate is written against live rather than fixtures. | Already measured (§2.1). Every Phase 2 gate must name a constructed fixture. |

---

*End of read-out. Nothing here was applied. The database was opened `mode=ro` and is unchanged;
the checkpoint fingerprint comparison is in the task's final report.*
