# Addendum to S2_phase1_readout.md (7887a6e)

**Dated** 2026-09-21 (UTC) · **read-only** · **HEAD at measurement** `7887a6e`, tree clean, level
with origin · `review.db` opened `mode=ro` throughout, never `immutable=1`

Four measurements requested by the PI before ruling on row 7, Q1–Q11 and A11. This file is an
addendum; the read-out it supplements is **not edited**.

Every number is followed by the query that produced it; every code claim by a quoted anchor.
Probe scripts stayed in the session scratchpad and are not committed.

**Verdicts up front.**

| item | verdict |
|---|---|
| **I8** — the cloud writer may depend on `UNIQUE(paper_id, arm)` | **FALSE.** No writer depends on it. The duplicate guard is a **pre-insert `NOT IN` filter** in `get_pending_papers`, not the constraint. |
| **I9** — existing rows carry enough configuration to group claims by configuration | **FALSE.** `model_digest`, `codebook_hash` and `codebook_sha256` are **NULL on all 190 local rows**; `codebook_hash`/`codebook_sha256` are **NULL on all 379 cloud rows**; `cloud_extractions` has **no `model_digest` column at all**. Neither table records a prompt hash, options, temperature, seed or parsed-text hash. |
| **I10** — `human_A…D` appear as intended arms with no table behind them | **TRUE, and understated.** A complete import path exists (`analysis/paper1/human_import.py`) which **creates the table itself** on first run, and three components already query it. A fourth — `concordance.load_arm`, the one two architecture docs say reads it — **does not**, and returns an empty dict silently. |

No finding contradicts this session's MEASURED values.

---

## M-a (I8) — every writer of `cloud_extractions` and `cloud_evidence_spans`

### The writers

```
grep -rn "INTO cloud_extractions|INTO cloud_evidence_spans|UPDATE cloud_extractions|UPDATE cloud_evidence_spans" --include=*.py .
```

**Four production writers; everything else is under `tests/`.**

| # | writer | writes | statement |
|---|---|---|---|
| 1 | `engine/cloud/base.py` · `store_result` | both tables | plain `INSERT`, below |
| 2 | `scripts/backfill_cloud_spans.py` | spans only | plain `INSERT` |
| 3 | `scripts/reparse_cloud_spans.py` | spans only | plain `INSERT` |
| 4 | `engine/cloud/schema.py` | spans only | the rebuild branch (`UPDATE … SET confidence = 0.0 WHERE confidence IS NULL`, then `INSERT INTO cloud_evidence_spans` into the rebuilt table) — **this is the branch C10 records as never having run on live** |

Writer 1 is the only one that inserts a `cloud_extractions` row. Writers 2 and 3 insert spans
against an **existing** `cloud_extraction_id`, so neither can touch `UNIQUE(paper_id, arm)`.

### The insert statement, quoted

`engine/cloud/base.py` · `store_result` — *"Store extraction result and spans atomically."*

```python
        try:
            cur = self._conn.execute(
                """INSERT INTO cloud_extractions
                   (paper_id, arm, model_string, extracted_data, reasoning_trace,
                    prompt_text, input_tokens, output_tokens, reasoning_tokens,
                    cost_usd, extracted_at,
                    codebook_hash, codebook_sha256)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
```

and, per span:

```python
                self._conn.execute(
                    """INSERT INTO cloud_evidence_spans
                       (cloud_extraction_id, field_name, value, source_snippet,
                        confidence, tier, notes)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
```

closing with:

```python
            self._conn.commit()
            return ext_id

        except Exception:
            self._conn.rollback()
            raise
```

### Conflict handling — there is none

```
grep -rn "OR REPLACE|ON CONFLICT|IntegrityError|OR IGNORE" engine/cloud/ scripts/run_cloud_extraction.py scripts/backfill_cloud_spans.py scripts/reparse_cloud_spans.py
→ (no matches)
```

**No `INSERT OR REPLACE`, no `ON CONFLICT`, no `OR IGNORE`, and no `IntegrityError` is caught
anywhere on the cloud path.** The `except Exception:` in `store_result` rolls back and **re-raises**;
it does not swallow. So the constraint is not used as an upsert mechanism and its violation is not
used as a signal.

### The existence check — this is the real guard

`engine/cloud/base.py` · `get_pending_papers` — *"Get extraction-eligible papers with no cloud
extraction for this arm."*

```python
            f"""SELECT p.id AS paper_id, p.title, p.authors, p.year
               FROM papers p
               WHERE {corpus_sql}
               AND p.id NOT IN (
                   SELECT ce.paper_id FROM cloud_extractions ce WHERE ce.arm = ?
               )
               ORDER BY p.id""",
```

Its three non-test callers are the whole of the cloud entry surface:

```
engine/cloud/anthropic_extractor.py:   pending = self.get_pending_papers(self.ARM)
engine/cloud/openai_extractor.py:      pending = self.get_pending_papers(self.ARM)
scripts/run_cloud_extraction.py:       pending = base.get_pending_papers(arm_key)
```

**Every path to `store_result` passes through this filter first.** In normal operation the UNIQUE
constraint is therefore never reached: the paper is excluded from the work list before a second
insert can be attempted.

### What a second extraction for the same pair does

| | **today (constraint present)** | **after the constraint is dropped** |
|---|---|---|
| via the normal pipeline | **cannot happen** — `get_pending_papers` excludes the paper. Re-running an arm is a no-op for already-extracted papers. **Unchanged by the drop.** | **identical** — the `NOT IN` filter still excludes it. The drop changes nothing here. |
| via a direct `store_result` call, or two concurrent runs of one arm racing past the filter | `sqlite3.IntegrityError: UNIQUE constraint failed: cloud_extractions.paper_id, cloud_extractions.arm`; `except Exception: rollback; raise`. **No partial write, loud failure.** | the insert **succeeds**. Two extractions now exist for the pair. |

**So: dropping the constraint changes no writer's behaviour on any reachable path, and removes a
backstop on one unreachable one.** I8 is false as stated — nothing depends on the constraint — but
the honest statement is narrower than "dropping it is free":

**Consequence the read-out must carry into Q3.** Once two cloud extractions for a pair can exist,
`concordance.load_arm`'s cloud branch folds **both** into one dict:

```python
                   FROM cloud_evidence_spans cs
                   JOIN cloud_extractions ce ON ce.id = cs.cloud_extraction_id
                   WHERE ce.arm = ?
                   ORDER BY ce.paper_id, cs.field_name
```

`ORDER BY` does not disambiguate two rows with the same `(paper_id, field_name)`, so which
extraction's value survives the `result[pid][row["field_name"]] = row["value"]` assignment is
decided by physical row order. **That is A7/D1-4 extended to the cloud arms** — today structurally
impossible there, tomorrow possible. The constraint is currently the *only* thing making the cloud
arms immune to a defect the local arm already has.

**Recommendation, revised from the read-out's Q3.** Drop the constraint **in the same session that
puts `load_arm` behind the reader (session 6), not in session 5.** The read-out recommended
dropping it in migration 017; that would open a seven-session window in which the cloud arms carry
A7 and nothing reads through a reader that resolves it. The ordering the measurement argues for is:
session 5 mints `extraction_uid` and builds the reader; **session 6 migrates `load_arm` and drops
the constraint in the same commit**, so supersession becomes representable and resolvable at the
same moment. This is a **change to the read-out's recommendation, offered for the PI's ruling** —
the read-out is not edited.

### One documentation drift found in passing

`CLAUDE.md` states: *"`store_extraction()` rejects 0-span results with ValueError"*. **There is no
`store_extraction` on the cloud path**; the method is **`store_result`**. The behaviour described
is real and lives in `store_result`, which raises before the insert on a 0-span result and then
calls `enforce_completeness(...)`. Name only; recorded so a future grep for `store_extraction`
does not come back empty and get misread as "the check is gone".

---

## M-b (rule row 10) — absence sentinels, by arm and citation

Sentinel set, read from the codebook rather than restated:

```python
yaml.safe_load(open("data/surgical_autonomy/extraction_codebook.yaml"))["absence_sentinels"]
→ ['NR', 'N/A', 'NA', 'NOT_FOUND', 'NOT FOUND', 'NOT REPORTED']
```

### Counts

```sql
-- LOCAL
SELECT CASE WHEN TRIM(COALESCE(es.source_snippet,''))='' THEN 'absent' ELSE 'present' END cit,
       COUNT(*) n
FROM evidence_spans es
WHERE UPPER(TRIM(es.value)) IN (?,?,?,?,?,?) GROUP BY cit;

-- CLOUD, per arm
SELECT ce.arm,
       CASE WHEN TRIM(COALESCE(cs.source_snippet,''))='' THEN 'absent' ELSE 'present' END cit,
       COUNT(*) n
FROM cloud_evidence_spans cs
JOIN cloud_extractions ce ON ce.id = cs.cloud_extraction_id
WHERE UPPER(TRIM(COALESCE(cs.value,''))) IN (?,?,?,?,?,?)
GROUP BY ce.arm, cit ORDER BY ce.arm, cit;
```

| arm | citation **present** | citation **absent** | total | % cited |
|---|---:|---:|---:|---:|
| `local` | **101** | **82** | 183 | 55.2% |
| `anthropic_sonnet_4_6` | **36** | **2** | 38 | **94.7%** |
| `openai_o4_mini_high` | **1** | **127** | 128 | **0.8%** |
| **all** | **138** | **211** | **349** | 39.5% |

The local split (101/82) reproduces the read-out's §5.1 exactly, and the cloud total (37/129)
reproduces §5.2 — **the per-arm split is new, and it is the finding.**

**The two cloud arms behave in opposite ways.** Anthropic attaches a snippet to 36 of its 38
sentinels; OpenAI attaches one to **1 of 128**. That is not a small difference in degree, and it
means rule row 10 ("absence sentinel, cited → asserted with evidence") and row 10b ("uncited →
asserted without locatable evidence") will classify the two cloud arms into almost disjoint
states. Any cross-arm comparison of "asserted with evidence" rates is therefore measuring a
formatting habit as much as an extraction property.

### Which tokens actually occur

```sql
SELECT UPPER(TRIM(COALESCE(value,''))) v, COUNT(*) n FROM <table>
WHERE UPPER(TRIM(COALESCE(value,''))) IN (?,?,?,?,?,?) GROUP BY v ORDER BY n DESC;
```

| table | tokens |
|---|---|
| `evidence_spans` | `NR` 176 · `NOT_FOUND` 6 · `N/A` 1 |
| `cloud_evidence_spans` | `NOT_FOUND` 87 · `NR` 79 |

Three of the six declared sentinels (`NA`, `NOT FOUND`, `NOT REPORTED`) occur **zero** times.

### Three cited snippets, verbatim — and what they show

```sql
SELECT es.id span_id, e.paper_id, es.field_name, es.value, es.source_snippet
FROM evidence_spans es JOIN extractions e ON e.id = es.extraction_id
WHERE UPPER(TRIM(es.value)) IN (?,?,?,?,?,?) AND TRIM(COALESCE(es.source_snippet,''))<>''
ORDER BY es.id LIMIT 3;
```

**1. paper 14 · `task_select` · value `NR` · span_id 50**

> `No explicit selection process described.`

**2. paper 15 · `sample_size` · value `NR` · span_id 64**

> `The system proposed in this study was subjected to testing through simulations on a Linux Ubuntu 20.04 workstation.`

**3. paper 11 · `sample_size` · value `NR` · span_id 104**

> `No specific sample size is provided.`

**🔴 These three are not the same kind of thing, and rule row 10 treats them as one.**

- **1 and 3 are the extractor's own prose**, not a quotation. "No explicit selection process
  described" is a statement *about* the paper. It is a defensible justification for an absence
  claim, but it is **not evidence** in the sense row 8 means — there is nothing to anchor against
  the source text, and the provenance ladder would class it as untraceable, not anchored.
- **2 is paper text that has nothing to do with the claim.** A sentence about a Linux workstation
  is offered as the evidence that sample size is not reported. It is a real quote and it is
  **non-probative**.

The cloud arms show the same split. The single cited OpenAI sentinel:

> **paper 511 · `task_monitor` · `NR`** — `It is worth noticing that the control is carried out completely in open-loop, without any sensory feedback about the manipulator's end-effector position.`

is genuinely probative (open-loop control *is* the evidence that nothing monitors). Two of three
sampled Anthropic sentinels are not:

> **paper 22 · `sample_size` · `NR`** — `Each condition underwent testing over a constrained number of trials (e.g., 4-5 for robotic methods, as reported), limited by the finite complexity of the phantom's predefined vascular paths. …`
>
> **paper 121 · `sample_size` · `NR`** — `This perspective outlines the critical research frontiers that will define the next generation of surgical robotics, moving beyond a review of current capabilities to a forward-looking blueprint for achieving true human-AI symbiosis in surgery.`

The paper-22 snippet arguably *contradicts* its own `NR` — it names 4–5 trials and 10 simulation
trials. The paper-121 snippet is unrelated boilerplate.

**What this does to rule row 10.** Row 10 as written says a cited sentinel is *asserted with
evidence*. The measurement says **the citation's presence does not establish that the citation
supports the absence claim**, in either direction: 82 local and 129 cloud sentinels have no
citation at all, and an unmeasured share of the 138 that do are non-probative or
extractor-authored. Row 10 is not wrong — it is **weaker than it reads**, and the read-out's §4
does not say so.

Three ways to tighten it, for the PI:

| | option | cost |
|---|---|---|
| (a) | **Keep row 10 as written**, and record in the rule's own text that "with evidence" means *a citation is attached*, not *the citation supports the claim* | free; but the phrase then means something different for sentinels than for row 8 values, which is the kind of one-word-two-meanings defect A3 is |
| (b) | **Require the citation to be verbatim-locatable in the source text** for row 10, as the elicited path already does for every value; a non-locatable snippet demotes to row 10b | needs the parsed text at read time; the elicited path (Run 7) gets this for free, the 349 legacy rows do not |
| (c) | **Split the sentinel state**: *absence asserted with locatable quote* vs *absence asserted with justification* | a ninth S5b state, which S5b freezes at eight |

**No recommendation is offered** — this is a change to the vocabulary the PI is being asked to
approve, and the read-out's §4 was written before this was measured.

---

## M-c (I9) — configuration recorded on existing extraction rows

```sql
PRAGMA table_info(extractions);
PRAGMA table_info(cloud_extractions);
SELECT <col> v, COUNT(*) n FROM extractions GROUP BY <col> ORDER BY n DESC;
SELECT arm, <col> v, COUNT(*) n FROM cloud_extractions GROUP BY arm, <col> ORDER BY arm;
```

### What the columns are

**`extractions`** — `id`, `paper_id`, `extraction_schema_hash`, `extracted_data`,
`reasoning_trace`, `model`, `extracted_at`, `low_yield`, `model_digest`, `auditor_model_digest`,
`codebook_hash`, `codebook_sha256`

**`cloud_extractions`** — `id`, `paper_id`, `arm`, `model_string`, `extracted_data`,
`reasoning_trace`, `prompt_text`, `input_tokens`, `output_tokens`, `reasoning_tokens`, `cost_usd`,
`extraction_schema_hash`, `extracted_at`, `codebook_hash`, `codebook_sha256`

### What they contain

| kind | `extractions` (local, n=190) | `cloud_extractions` (n=379) |
|---|---|---|
| **model identity** | `model` = `'deepseek-r1:32b'` ×190 — **1 distinct, populated** | `model_string` = `'claude-sonnet-4-6'` ×190, `'o4-mini-2025-04-16'` ×189 — **populated** |
| **model digest** | `model_digest` = **NULL ×190** | **column does not exist** |
| **auditor digest** | `auditor_model_digest` = **NULL ×190** | n/a |
| **prompt** | **no column** | `prompt_text` populated, 0 empty, **190/189 distinct** — one per paper, because it embeds the paper text |
| **prompt hash** | **no column** | **no column** |
| **codebook hash** | `codebook_hash` = **NULL ×190**; `codebook_sha256` = **NULL ×190** | `codebook_hash` = **NULL ×379**; `codebook_sha256` = **NULL ×379** |
| **schema hash** | `extraction_schema_hash` = `d311eb20d1f8c9ea47ef8038a18924198348efce4903…` ×190 — **1 distinct, populated** | same value `d311eb20…` ×190 and ×189 — **identical across all three arms** |
| **options / temperature / seed / num_ctx / think** | **no column** | **no column** |
| **parsed-text hash or path** | **no column** | **no column** |
| **run linkage** | **no `run_id`** | **no `run_id`** |

### Is "arm = configuration" representable for these rows?

**No.** What is on disk is:

- a **model name** per arm (three values across three arms), and
- **one `extraction_schema_hash`, `d311eb20…`, identical on all 569 rows in all three arms.**

That single shared hash is the one genuine cross-arm configuration fact the database records: all
three arms ran the same extraction schema. Everything else that distinguishes a configuration —
digest, prompt template, temperature, seed, context length, think policy, the parsed text actually
fed in — is **absent, not merely null**: there is no column for it on either table, with the sole
exception of `cloud_extractions.prompt_text`, which stores the *rendered* prompt per paper and so
cannot serve as a configuration key without hashing out the paper-specific part.

**What is missing, to make it representable** — this is S3a/S3b's scope, listed here so session 7
inherits a measured gap rather than an assumed one:

| needed | local | cloud |
|---|---|---|
| model digest | column exists, **never populated** | **column absent** |
| prompt template hash (paper-independent) | absent | derivable only by re-templating `prompt_text` |
| options hash (temperature, seed, `num_ctx`, `think`, format) | absent | absent |
| codebook hash | column exists, **never populated** | column exists, **never populated** |
| parsed-text hash | absent | absent |
| run id | absent | absent |

**🔴 A consequence worth surfacing separately: the extraction reuse key is inert on every existing
row.** `engine/agents/extractor.py` skips a paper on

```python
            "SELECT id FROM extractions WHERE paper_id = ? AND codebook_hash = ?",
```

logging *"Paper %d: already extracted with current schema — skipping"*. `codebook_hash` is **NULL
on all 190 rows**, and in SQL `NULL = <anything>` is never true. **The skip can therefore never
fire for any paper currently in the database** — a re-run of local extraction would re-extract all
190, not skip them. This is D2 ("extraction reuse key is `(paper_id, codebook_hash)`") meeting the
data: the key is correctly implemented and has nothing to match on. It does not change the S2
design, but it changes what a session-5 or session-9 operator should expect from a re-run, and it
is the kind of thing that is discovered at the worst moment.

---

## M-d (I10) — the human arm

```
grep -rn "human_extractions" --include=*.py --include=*.md --include=*.yaml --include=*.yml .
grep -rnE "human_[ABCD]\b" ...
grep -rn "extractor_id" ...
```

### The table does not exist, and a code path would create it

`SELECT COUNT(*) FROM human_extractions` → `no such table: human_extractions`, and the name is
absent from the 24 user tables in `sqlite_master`. **But the import path is complete and
self-provisioning.**

`analysis/paper1/human_import.py` — module docstring: *"Import human extractor workbooks into the
human_extractions table."* It carries its own DDL:

```python
_CREATE_TABLE = """\
CREATE TABLE IF NOT EXISTS human_extractions (
    id INTEGER PRIMARY KEY,
    paper_id TEXT NOT NULL,
    extractor_id TEXT NOT NULL,
    field_name TEXT NOT NULL,
    value TEXT,
    source_quote TEXT,
    notes TEXT,
    imported_at TEXT NOT NULL,
    UNIQUE(paper_id, extractor_id, field_name)
)"""
```

a writer `store_human_extractions(rows, extractor_id, db_path)` — *"Store parsed rows into
human_extractions table (long format)"* — and a CLI whose argparse description reads *"Import
human extractor workbook into human_extractions table"*, resolving the extractor letter from the
filename (`_extract_extractor_id`, tested against `Extraction_Workbook_v2_A.xlsx` → `"A"`).

**Answer to the question as posed: yes, a code path would load human extractor workbooks, and into
a `human_extractions` table that the path itself creates.** It has never been run against this
database. Note that this table is **outside the numbered-migration régime** — it would appear in
`review.db` with no receipt in `schema_migrations`, which is exactly the shape MIGRATIONS-01 exists
to prevent.

### Who already reads it

| consumer | anchor | routes a human arm? |
|---|---|---|
| `analysis/paper1/consensus.py` | four sites, incl. `FROM human_extractions` and `HAVING COUNT(DISTINCT extractor_id) >= ?` | yes — this is the shared-paper consensus derivation |
| `analysis/paper1/adjudication.py` | *"Get source_quote from human_extractions."*, with the comment `# human_A, human_B, etc.` | yes |
| `engine/validators/distribution_monitor.py` | `_query_human_values`: *"Get all values for a field from human_extractions for a specific extractor."* | **yes, by name** — see below |
| **`engine/analysis/concordance.py` · `load_arm`** | — | **NO** |

### 🔴 The two arm routers disagree

`engine/validators/distribution_monitor.py` · `_query_values` — *"Route value query to the right
table based on arm name."*

```python
    if arm == "local":
        return _query_local_values(conn, field_name, non_value)
    if arm.startswith("human_"):
        extractor_id = arm.split("_", 1)[1]
        return _query_human_values(conn, field_name, extractor_id, non_value)
    return _query_cloud_values(conn, field_name, arm, non_value)
```

**Three branches: local, human, cloud.**

`engine/analysis/concordance.py` · `load_arm` has **two**:

```python
        if arm == "local":
            …  FROM evidence_spans es JOIN extractions e ON e.id = es.extraction_id
        else:
            …  FROM cloud_evidence_spans cs JOIN cloud_extractions ce ON ce.id = cs.cloud_extraction_id
               WHERE ce.arm = ?
```

**So `load_arm(db_path, "human_A")` falls into the cloud branch, queries
`cloud_extractions WHERE arm = 'human_A'`, matches nothing, and returns `{}` — silently, with no
error**, and `load_arm`'s own docstring blesses the result: *"empty dict when no data exists for
the arm (valid result)"*.

This is the *one predicate, two programs* shape: two components must agree on what an arm is, and
one of them has a `human_` branch the other lacks. It matters to S2 directly, because
`effective_value(paper, field, arm)` is the third program that will need the same predicate, and
the right move is for it to **own** the routing and for both existing callers to import it.

### Two architecture documents describe the branch that does not exist

`docs/architecture/pipeline.md`:

> `1. **Load arms:** load_arm() — queries evidence_spans (local) or cloud_evidence_spans (cloud arms) or human_extractions (human_A/B/C/D). Returns {paper_id: {field_name: value}}`

`docs/architecture/modules.md`:

> `load_arm(db_path, arm) — Load from evidence_spans (local) or cloud_evidence_spans (cloud) or human_extractions (human_*). Raises sqlite3.OperationalError on DB errors (never returns empty dict on failure)`

**Both are false at `7887a6e`.** `load_arm` has no `human_extractions` query. The second is doubly
misleading: it promises that an empty dict never signals failure, which is true of a *DB error* and
false of a *human arm*, where an empty dict is exactly what a caller gets.

`CLAUDE.md` makes the same claim in the Concordance section (*"load extractions from local,
openai_o4_mini_high, anthropic_sonnet_4_6, human_A/B/C/D arms"*) and lists `human_extractions`
among the database tables.

### Where the concept appears, in full

| file | nature |
|---|---|
| `analysis/paper1/human_import.py` | the writer, the DDL, the CLI — **the only producer** |
| `analysis/paper1/consensus.py` | reader ×4 |
| `analysis/paper1/adjudication.py` | reader ×1 |
| `engine/validators/distribution_monitor.py` | reader ×1, with `human_`-prefix routing |
| `engine/migrations/007_add_judge_tables.py` | a docstring reference: *"analysis/paper1/human_extractions (paper_id = "EE-NNN" strings)"* |
| `CLAUDE.md` | table list + concordance arm list + the `human_import.py` bullet |
| `docs/architecture/state-machine.md` | table row |
| `docs/architecture/pipeline.md`, `docs/architecture/modules.md` | the false `load_arm` claim |
| `docs/session-reports/DIAG-OPTSET-01_report.md` | a prior report already noting a consumer reads `human_extractions.value` against `evidence_spans.value` without normalization |
| `tests/` | `test_human_import.py`, `test_consensus.py`, `test_distribution_monitor.py`, `test_adjudication_pairs.py` all create the table themselves |

**Note the `paper_id` type mismatch, since it bears on claim identity.** `human_extractions.paper_id`
is `TEXT NOT NULL` holding `"EE-NNN"` strings; `extractions.paper_id` and
`cloud_extractions.paper_id` are `INTEGER … REFERENCES papers(id)`. A claim id spanning all arms
(read-out §2.3) must therefore either normalise the human arm's paper key or declare that human
arms use a different key space. **This is a new constraint on §2.3 that the read-out did not
have.**

### What this does to Q11

Q11 asked whether the human arms are live work, dropped, or held elsewhere. The measurement
narrows it: **the arm is fully built and has never been run.** The question for the PI is no longer
"does this exist" but:

- is the human arm **in scope** for `effective_value`, given that four components already assume it and one silently returns nothing; and
- if yes, does `human_extractions` get a **numbered migration** so it enters the database with a receipt, rather than being conjured by `CREATE TABLE IF NOT EXISTS` on first import?

---

## Standard gate

Run before the commit that carries this addendum.

**2,412 passed · 17 deselected · zero failures.** Per-chunk (informational): 508 / 608 / 405 / 475
/ 416; deselects 0 / 0 / 10 / 6 / 1.

## Close fingerprint

`python -m engine.tools.db_fingerprint data/surgical_autonomy/review.db --compare docs/session-reports/migrations-01/review_db_fingerprint_20260921T002809Z.json`
→ **exit 0**, `IDENTICAL — schema, every table, and the overall hash all match`. Overall
`a9926e626928d1d47f4935e129da698cd2d50c82902f25c0f4f3cd9b1b1eafae`. The database was not written.
