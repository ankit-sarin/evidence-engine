# INVENTORY-02 Phase 1 — the spec-bearing row, and an attribute-read category for spec objects

**Read-out. Operational. No tool change, no regeneration.**
**Harness session:** `e6c94396-9455-4c17-be3d-1edf2c546b1b` — a **continuation** of
`91f0009c-5166-4bd2-a397-d74ee5fa8faa` (continued-in record 2026-09-13T04:36:51.717Z).
**Parent:** `b3a061e`. **Standard gate:** 2,288 passed / 17 deselected, five chunks. Chunk list from `find`; passes
504 / 572 / 358 / 438 / 416; deselects 0 / 0 / 10 / 6 / 1; zero failures.

`engine/tools/inventory.py`, `docs/inventory/entry_points.{md,json}`, `tests/test_inventory.py` and
every engine file are untouched. `data/surgical_autonomy/review.db` was stat-ed and never opened:
101,978,112 B, mtime 2026-09-11 02:00:52.636956943 UTC, before and after.

The census in P3 was run by a scratch AST script outside the repository. It imports and runs
nothing it scans. Historical trees were read through `git archive` into the scratchpad, not
through a worktree or a checkout.

---

## Findings in one paragraph

The row labelled **"of those, spec-bearing"** is rendered under "argparse entry points naming a
review" (74). It is fed `entry_points_with_spec_flag`, which is computed over **every** entry
point. "Of those" is true only while no entry point carries `--spec` without also carrying
`--review`/`--name`.

That held from the inventory's creation until **`e2adff3`**. There `analysis/eval/screen2f_worker.py`
became the first spec-only entry point, and the label stopped describing the population.

The row has moved twice:
- **`dfde733`** — a real change in both populations: `engine/utils/extraction_cleanup.py`
  renamed `--spec` to `--codebook`. It moved the value 29 → 28 and was correctly shown as
  differing, though the note said "unexplained" when it wasn't. The same commit moved
  "name-only" 45 → 46 and "name-only, constructing ReviewDatabase" 20 → 21. Both rows still
  read "differs — unexplained, investigate".
- **`e2adff3`** — a change outside the labelled population. It moved the value back to 29,
  and the row turned "matches" by coincidence. **The "of those" figure the label describes is
  28 today, not 29.**

The row now contradicts its neighbours: 29 + 46 = 75, not 74.

For P3, the census finds **152 attribute reads on spec-bound objects in 33 files**:
- 145 direct reads
- 5 `getattr` with a default
- 2 `hasattr`
- 0 bare `getattr`
- 0 dynamic

It finds another 7 guards on a spec sub-object, 1 nested guard and 6 guards on locals copied
from a spec field. The brief's "nine" is **not** any single census population. It is reproduced
exactly by one counting rule, given in P3.

---

## P1 — The tool as it is

### Populations

All populations come from `build_inventory` (`engine/tools/inventory.py`, "entry_points = {r: f
for r, f in files.items() if f["entry_point"]}"). An entry point is a scanned file with any
argparse flag or a `__main__` guard: `result["entry_point"] = bool(result["flags"]) or
result["has_main_guard"]`.

```python
spec_bearing = {
    r: f for r, f in entry_points.items()
    if any("--spec" in fl["names"] for fl in f["flags"])
}
name_bearing = {
    r: f for r, f in entry_points.items()
    if any(n in ("--review", "--name") for fl in f["flags"] for n in fl["names"])
}
constructs_db = {r: f for r, f in entry_points.items() if f["review_database"]}
```

`spec_bearing` and `name_bearing` are both drawn from `entry_points`. Neither is drawn from the
other.

### Summary categories

The Summary table renders each key with `k.replace('_', ' ')`, so its labels are the key names.

| key (Summary label) | population it is computed over | HEAD | label ↔ population |
|---|---|---:|---|
| files_scanned | every `*.py` under `engine`, `scripts`, `analysis` except `__pycache__` | 203 | matches |
| entry_points | files with argparse flags or a `__main__` guard | 103 | matches |
| entry_points_with_spec_flag | **all** entry points with a `--spec` flag | 29 | matches (the label says nothing about `--review`) |
| entry_points_with_review_name_flag | entry points with `--review` or `--name` | 74 | matches |
| entry_points_name_only | `name_bearing` minus `spec_bearing` | 46 | matches |
| entry_points_constructing_reviewdatabase | entry points with a `ReviewDatabase(...)` call | 37 | matches |
| name_only_constructing_reviewdatabase | `constructs_db` ∩ `name_bearing`, minus `spec_bearing` | 21 | matches |
| files_calling_resolver | all scanned files calling `load_spec_for` / `spec_path_for` / `data_root_for` | 32 | matches |
| files_calling_load_review_spec_directly | all scanned files calling `load_review_spec` | 9 | matches |
| raw_yaml_load_sites | `<x>yaml.safe_load/load` calls, all files | 3 | matches |
| files_with_raw_yaml_loads | files with at least one such call | 3 | matches |
| review_id_constants | module-level `NAME = "<review id>"` assignments | 9 | matches |
| literal_review_id_sites_in_code | review-id string constants, role `code`, all files | 35 | matches |
| path_construction_sites_in_code | strings carrying `review_specs` / `data/` / `.yaml`, role `code` | 67 | matches |
| db_before_spec_scopes | scopes where the first `ReviewDatabase` line precedes the first spec call | 0 | matches |
| fstring_spec_path_sites | code-role f-strings containing `review_specs` | 0 | matches |
| default_review_named_constants | `review_id_constants` named `DEFAULT_REVIEW` | 7 | matches |
| unparsed_sites | UNPARSED entries | 0 | matches |

**Every Summary label matches its population.** The mismatch is in the reconciliation table
only.

### Reconciliation rows

The rows are the `BASELINE` literal, rendered by `_render_reconciliation`. Its verdict rule is:

```python
if now == baseline:
    verdict = "matches"
else:
    verdict = note or "differs — unexplained, investigate"
```

It compares one integer to one integer and has no notion of which population the integer
counts.

| row label (as rendered) | key | population the label claims | population computed | HEAD row | |
|---|---|---|---|---|---|
| argparse entry points naming a review | entry_points_with_review_name_flag | entry points with `--review`/`--name` | same | 74 / 74 matches | ok |
| **of those, spec-bearing** | **entry_points_with_spec_flag** | the 74, with `--spec` | **all 103 entry points with `--spec`** | 29 / 29 matches | **MISMATCH**: the labelled population is 28 |
| of those, name-only | entry_points_name_only | the 74, without `--spec` | same | 45 / 46 differs — unexplained | label ok; **verdict text wrong** (explained by `dfde733`, P2) |
| entry points constructing ReviewDatabase | entry_points_constructing_reviewdatabase | entry points constructing a DB | same | 35 / 37 (note) | ok |
| name-only, constructing ReviewDatabase | name_only_constructing_reviewdatabase | name-only ∩ constructs DB | same | 20 / 21 differs — unexplained | label ok; **verdict text wrong** (explained by `dfde733`, P2) |
| raw yaml load sites | raw_yaml_load_sites | yaml loads outside the spec loader | same | 13 / 3 differs — unexplained | label ok; not investigated here (out of scope) |
| f-string spec-path builders | fstring_spec_path_sites | hand-built spec paths | same | 19 / 0 (note) | ok |
| DEFAULT_REVIEW constants | default_review_named_constants | `DEFAULT_REVIEW` constants | same | 7 / 7 matches | ok |

**The table is internally inconsistent at HEAD.** "Of those, spec-bearing" (29) plus "of those,
name-only" (46) is 75. The parent row says 74.

The two "of those" rows use different set expressions:
- name-only is `name_bearing − spec_bearing`;
- spec-bearing is `spec_bearing` alone, where it would have to be `spec_bearing ∩ name_bearing`.

The two are complementary only when `spec_bearing ⊆ name_bearing`. At HEAD
`spec_bearing − name_bearing = {analysis/eval/screen2f_worker.py}`.

**Mismatches marked:**
1. The "of those, spec-bearing" row counts the wrong population.
2. The "of those, name-only" and "name-only, constructing ReviewDatabase" rows say "unexplained"
   for a move that is explained.

---

## P2 — History of the row

`BASELINE` has not changed since the tool was introduced: `git log -S'"of those, spec-bearing"'`
and `-S'"entry_points_with_spec_flag", 29'` both return only `5f4de81`. So **no move below was
in the label.** Every move was in the counted value.

Populations below were recomputed from each commit's committed `entry_points.json`, one row per
commit that changed any of them.

| commit | spec, all EPs (value shown) | spec ∩ name (what the label claims) | spec − name | name-only | name-only ∩ DB | rendered row |
|---|---:|---:|---:|---:|---:|---|
| `3ab8cff` (first generated) … `9de8249` | 29 | 29 | 0 | 45 | 20 | `\| 29 \| 29 \| matches \|` |
| **`dfde733`** | **28** | **28** | 0 | **46** | **21** | `\| 29 \| 28 \| differs — unexplained, investigate \|` |
| `e99204f` … `a01ab47` | 28 | 28 | 0 | 46 | 21 | unchanged |
| **`e2adff3`** | **29** | 28 | **1** | 46 | 21 | `\| 29 \| 29 \| matches \|` |
| `8a7dc4f`, `de2abe7` | 29 | 28 | 1 | 46 | 21 | unchanged |

- **`dfde733`** (feat(provenance): staleness, cleanup and parity read codebook_hash) —
  `engine/utils/extraction_cleanup.py` flags went from `--review, --keep-schema, --spec,
  --confirm` to `--review, --keep-schema, --codebook, --confirm`.
  - One file left `spec_bearing` and, still carrying `--review`, entered name-only. It also
    constructs a `ReviewDatabase`, so it entered name-only ∩ DB.
  - **This move was in the counted population, in both the labelled and the computed sense.**
  - The row was right to read "differs". The reason was knowable from the commit and never
    recorded as a note.
- **`e2adff3`** (feat(eval): SCREEN-AUTH-01 Phase 2f three-arm abstract-screening harness) —
  `analysis/eval/screen2f_worker.py` added a required `--spec` and no `--review`/`--name`.
  - **This move was in the computed population only.** The value shown rose 28 → 29, while the
    population the label describes stayed at 28.
  - The row turned "matches" because two unrelated changes offset each other against a
    hand-count baseline. That baseline was an `∩ name` figure: `BASELINE[0]`'s note says "the
    hand scan keyed on --review/--name".
  - This confirms M1.
- `8a7dc4f` and `de2abe7` regenerated the inventory (M2) with no effect on this row.
  `de2abe7`'s only summary change was `files_scanned` 204 → 203 from deleting `sizing.py` (M3).

---

## P3 — Attribute-read census on review-spec-bound objects

### Method

This is scratch AST, over the tool's own `SCAN_ROOTS` (`engine`, `scripts`, `analysis`).

**Binding rules.** They are the three mechanisms of I2, applied per function scope and inherited
by nested scopes:
- **LOAD** — a name or `self.<attr>` assigned from a call to `load_review_spec` or
  `load_spec_for`.
- **ANNOT** — a parameter, or annotated assignment, whose annotation text contains
  `ReviewSpec`. That covers `ReviewSpec`, `"ReviewSpec"`, `Optional[ReviewSpec]` and
  `ReviewSpec | None`.
- **NAME** — a `Name` `spec` / `review_spec`, or any attribute `<x>.spec` / `<x>.review_spec`.

**Access forms:**
- `direct` — `spec.attr` in Load context.
- `getattr` — `getattr(spec, "attr")`.
- `getattr_default` — `getattr(spec, "attr", d)`.
- `hasattr` — `hasattr(spec, "attr")`.
- `dynamic` — `getattr` or `hasattr` with a non-literal name.

Each attribute is also checked against `ReviewSpec`'s own members, read by AST from
`engine/core/review_spec.py` (21 members).

### Totals per file (HEAD `b3a061e`)

| file | direct | getattr | getattr w/ default | hasattr | dynamic | total |
|---|---:|---:|---:|---:|---:|---:|
| analysis/eval/elicit_design01/smoke.py | 4 | 0 | 0 | 0 | 0 | 4 |
| analysis/eval/run_local_abc.py | 2 | 0 | 0 | 0 | 0 | 2 |
| analysis/eval/run_qualgap01.py | 2 | 0 | 0 | 0 | 0 | 2 |
| analysis/eval/screen2f_worker.py | 7 | 0 | 0 | 0 | 0 | 7 |
| analysis/eval/smoke_regression01.py | 1 | 0 | 0 | 0 | 0 | 1 |
| analysis/paper1/export_disagreement_pairs.py | 3 | 0 | 0 | 0 | 0 | 3 |
| engine/acquisition/check_oa.py | 1 | 0 | 0 | 0 | 0 | 1 |
| engine/acquisition/manual_list.py | 1 | 0 | 0 | 0 | 0 | 1 |
| engine/adjudication/audit_adjudicator.py | 3 | 0 | 0 | 0 | 0 | 3 |
| engine/adjudication/categorizer.py | 5 | 0 | 0 | 0 | 0 | 5 |
| engine/adjudication/ft_screening_adjudicator.py | 13 | 0 | 0 | 0 | 0 | 13 |
| engine/adjudication/screening_adjudicator.py | 12 | 0 | 0 | 0 | 0 | 12 |
| engine/agents/auditor.py | 3 | 0 | 0 | 1 | 0 | 4 |
| engine/agents/extractor.py | 2 | 0 | 2 | 0 | 0 | 4 |
| engine/agents/ft_screener.py | 23 | 0 | 0 | 0 | 0 | 23 |
| engine/agents/screener.py | 9 | 0 | 0 | 0 | 0 | 9 |
| engine/cloud/anthropic_extractor.py | 0 | 0 | 1 | 0 | 0 | 1 |
| engine/cloud/openai_extractor.py | 0 | 0 | 1 | 0 | 0 | 1 |
| engine/core/completeness.py | 1 | 0 | 0 | 0 | 0 | 1 |
| engine/core/review_paths.py | 3 | 0 | 0 | 0 | 0 | 3 |
| engine/elicitation/pipeline.py | 0 | 0 | 1 | 0 | 0 | 1 |
| engine/exporters/docx_export.py | 3 | 0 | 0 | 0 | 0 | 3 |
| engine/exporters/methods_section.py | 12 | 0 | 0 | 0 | 0 | 12 |
| engine/parsers/pdf_parser.py | 9 | 0 | 0 | 1 | 0 | 10 |
| engine/search/openalex.py | 2 | 0 | 0 | 0 | 0 | 2 |
| engine/search/pubmed.py | 2 | 0 | 0 | 0 | 0 | 2 |
| engine/tools/inventory.py | 1 | 0 | 0 | 0 | 0 | 1 |
| scripts/ft_screening_smoke_test.py | 7 | 0 | 0 | 0 | 0 | 7 |
| scripts/rescreen_with_specialty.py | 3 | 0 | 0 | 0 | 0 | 3 |
| scripts/run_pipeline.py | 6 | 0 | 0 | 0 | 0 | 6 |
| scripts/screen_expanded.py | 1 | 0 | 0 | 0 | 0 | 1 |
| scripts/test_e2e_search_screen.py | 2 | 0 | 0 | 0 | 0 | 2 |
| scripts/test_extraction_validation.py | 2 | 0 | 0 | 0 | 0 | 2 |
| **TOTAL (33 files)** | **145** | **0** | **5** | **2** | **0** | **152** |

**Per binding** — the rules each site satisfies:

| rules | sites |
|---|---:|
| ANNOT + NAME | 52 |
| NAME only | 49 |
| LOAD + NAME | 28 |
| ANNOT + LOAD + NAME | 23 |

No site is bound by LOAD or ANNOT without NAME.

Of the 49 NAME-only sites, **8 are not spec objects**. None of their attributes is a
`ReviewSpec` member:
- `args.spec.resolve` ×2 in `screen2f_worker.py` — a `Path` from argparse.
- `spec.get` ×5 in `categorizer.py` `CategoryConfig.load` — a dict.
- `spec.stem` ×1 in `inventory.py` `review_ids` — a `Path` in a glob loop.

With those removed, the census holds **144 reads on true spec objects**.

**Attribute frequency, true sites:**

| attribute | reads |
|---|---:|
| pico | 24 |
| ft_screening_models | 17 |
| eligibility | 16 |
| extraction_models | 12 |
| review_id | 12 |
| screening_models | 10 |
| title | 10 |
| pdf_parsing | 10 |
| version | 7 |
| cloud_models | 7 |
| search_strategy | 7 |
| screening_hash | 4 |
| auditor_model | 4 |
| unpaywall_email | 1 |
| institutional_proxy_pattern | 1 |
| low_yield_threshold | 1 |
| date | 1 |

### The guards

**Guards on a spec-bound object, 7:**

| file | function | line | form | expression |
|---|---|---:|---|---|
| engine/agents/auditor.py | run_audit | 378 | hasattr | `hasattr(spec, "auditor_model")` |
| engine/agents/extractor.py | extract_paper | 468 | getattr w/ default | `getattr(spec, "extraction_models", None)` (inner) |
| engine/agents/extractor.py | extract_paper | 491 | getattr w/ default | `models = getattr(spec, "extraction_models", None)` |
| engine/elicitation/pipeline.py | extract_paper_elicited | 215 | getattr w/ default | `models = getattr(spec, "extraction_models", None)` |
| engine/parsers/pdf_parser.py | parse_pdf | 554 | hasattr | `hasattr(spec, "pdf_parsing")` |
| engine/cloud/anthropic_extractor.py | AnthropicExtractor.__init__ | 48 | getattr w/ default | `getattr(self.spec, "cloud_models", None)` |
| engine/cloud/openai_extractor.py | OpenAIExtractor.__init__ | 47 | getattr w/ default | `getattr(self.spec, "cloud_models", None)` |

**Guards the per-object census does not count, because the object is not itself spec-bound:**

| population | sites |
|---|---|
| `getattr(spec.pdf_parsing, "<field>", default)` — sub-object | 7: pdf_parser.py 557, 558, 559, 560, 562, 563, 568 |
| `getattr(getattr(spec, …), "elicitation", False)` — nested, object is a call | 1: extractor.py 468 (outer) |
| `getattr(models, "pass1_think"/"pass2_think", d)` on `models = getattr(spec, …)` | 4: extractor.py 492, 493; pipeline.py 216, 217 |
| `cloud_cfg.<x>` guards on `cloud_cfg = getattr(self.spec, …)` | 2: anthropic_extractor.py, openai_extractor.py |

All guard calls on spec objects and anything derived from them come to **7 + 7 + 1 + 4 + 2 = 21**.

### Do the nine guards appear?

**Not as any one census population.** Here is how the figure lines up with the census, at the
point it was written and at HEAD. The census was re-run on archived trees.

| tree | guards on spec-bound objects | guards on a spec sub-object | report text |
|---|---:|---:|---|
| `521b92a^` (`fefd112`) | 17 | 7 | — |
| `521b92a` | 9 | 7 | "The eleven remaining `hasattr`/`getattr` guards" (2b §7) |
| `e9c4aa0` | 7 | 7 | "Nine guards remain elsewhere under SPEC-GUARD-01" (2b addendum) |
| `a01ab47` | 7 | 7 | — |
| HEAD `b3a061e` | 7 | 7 | brief I1: nine |

The guard population has not changed since `e9c4aa0`. **The gap is a counting rule, not code
movement.**

One rule reproduces both published numbers exactly (**INFERRED**): count getattr/hasattr calls
whose object is the spec itself, or a local assigned from `getattr(spec, …)`, **in the four
files the brief names**:

| file | calls |
|---|---|
| auditor.py | 1 |
| extractor.py | 468 inner, 491, 492, 493 = 4 |
| pipeline.py | 215, 216, 217 = 3 |
| pdf_parser.py | 554 = 1 |
| **total** | **9** |

At `521b92a` the two `hasattr(spec, "pico")` guards later removed in `e9c4aa0` add 2, giving
the "eleven".

That rule excludes:
- the two cloud extractors, whose object is `self.spec`, not `spec`;
- the seven `spec.pdf_parsing` sub-object guards;
- the nested outer guard at extractor.py 468.

The brief's file list is therefore **incomplete for a spec-object guard population**: two
`getattr(self.spec, "cloud_models", None)` guards live in `engine/cloud/`.

---

## P4 — What a category would need, and what it would miss (INFERRED)

This section is design inference from the census. None of it is implemented or tested.

### Binding rules, and what each buys today

1. **NAME is sufficient for reach and insufficient for precision.** It reaches all 152 sites,
   including every LOAD- and ANNOT-bound site, but 8 of the 152 (5.3%) are not specs. The
   rule has no type information. `spec` is also the conventional name for an argparse path
   (`args.spec`), a dict, and a loop `Path`.
2. **Member filter.** Keep a NAME-bound read only if its attribute is a `ReviewSpec` member,
   read by AST from `engine/core/review_spec.py` so the category cannot drift from the model.
   - Today it removes all 8 false positives and no true site.
   - It fails for a non-spec object whose attribute happens to share a member name, e.g. a
     dict-like with `.title`.
   - The drift test would carry the member list inside the category's own output.
3. **LOAD and ANNOT** are precise but, today, strictly redundant with NAME: 51 and 75 sites,
   all already NAME-bound. They are what would survive a rename (`rs = load_spec_for(...)`).
   The grep and AST pass found **zero** such renames today.
   - LOAD as written above misses the conditional form
     `spec = load_review_spec(p) if p else None` (concordance.py 220, adjudication.py 131),
     because the assigned value is an `IfExp`, not a call. The rule would need to look through
     `IfExp` branches. Both sites are caught by NAME today.
   - LOAD splits 25 `load_spec_for` / 10 `load_review_spec` assignments. The resolver is the
     dominant load path, so binding on `load_review_spec` alone, as I2 phrases it, would miss
     most loads.
4. **Scope.** The tool's `_scoped_nodes` yields a dotted scope per node but keeps no binding
   table. A category needs a per-scope table inherited by nested scopes, because LOAD and ANNOT
   are scope-local facts.

### Misses — present today

| miss class | what it loses | count today |
|---|---|---|
| **Spec field copied into a local** (`elig = spec.eligibility`, `models = spec.extraction_models`, `model = spec.auditor_model`) | every later read on the local | 6 direct copies; 7 direct reads through them |
| **Local assigned from a guard** (`models = getattr(spec, …)`, `cloud_cfg = getattr(self.spec, …)`) | the guards on the local | 4 locals; 6 `getattr` w/ default through them |
| **Sub-object guard** (`getattr(spec.pdf_parsing, "x", d)`) | counted only if the category also follows `spec.<member>` one level | 7 |
| **Nested guard** (`getattr(getattr(spec, …), "elicitation", False)`) | outer call's object is a call, not a binding | 1 |
| **Chained read on a loader result** (`load_spec_for(...).pdf_quality_check`) | no binding exists | 1 (pdf_quality_check.py 266), whose result then flows into a param annotated `PDFQualityCheck` |
| **Spec sub-model passed as its own type** (`elig: Eligibility`, `config: PDFQualityCheck`) | the whole downstream reader | 16 params (15 in `eligibility_render.py`, 1 in `pdf_quality_check.py`); reads not counted |
| **Cross-class attribute flow** (`self.spec` bound by LOAD in `CloudExtractorBase.__init__`, read in a subclass) | LOAD does not see it; only NAME does | 2 subclass sites |
| **Loader called for its side effect** (identity gate, result discarded) | nothing to read; not a miss, but a LOAD rule must not count it as a binding | 2 (concordance.py 371, export_disagreement_pairs.py 636) |

### Misses — classes with no instance today

These are still a category's structural limits:
- **Dynamic access** — `getattr(spec, name_var)`, `vars(spec)`, `spec.__dict__`,
  `operator.attrgetter`, `spec.model_dump()`: 0 hits.
- **Aliasing** (`s = spec`): 0 hits.
- **Spec passed into a callee under a different parameter name without an annotation**: every
  unannotated spec parameter found is still named `spec`/`review_spec`, so NAME covers it.
- **`tests/`**: outside `SCAN_ROOTS`, so any guard or read there is invisible to the tool by
  construction.

**Where the guards live.** A category counting only guards on spec-bound objects finds 7. To
find the full 21 it must follow one level of member access and one level of local assignment.
Those are the two rules above that cost the most precision.

---

## Verdicts on the brief's INFERRED items

- **I1 — nine guards on spec objects, in auditor.py, extractor.py, elicitation/pipeline.py and
  pdf_parser.py.** **NOT REPRODUCED AS STATED.** Per the brief, the difference is the finding,
  not a stop.
  - The census finds **7** guards on spec-bound objects, in **6** files (5 in the four named
    files, plus 2 in `engine/cloud/`).
  - It finds **21** guard calls on spec objects and anything derived from them.
  - "Nine" is reproduced exactly by the rule in P3: spec-or-guard-local, the four named files
    only. That rule also reproduces 2b's "eleven" at `521b92a`.
  - The guard population is unchanged since `e9c4aa0`.
- **I2 — spec objects reach code as the return of `load_review_spec`, parameters annotated
  `ReviewSpec`, and names/attributes called `spec` or `review_spec`.** **TRUE, all three
  used.**

  | mechanism | sites |
  |---|---:|
  | LOAD | 51 |
  | ANNOT | 75 |
  | NAME | all 152 |

  Two qualifications:
  - Most LOAD bindings come from **`load_spec_for`** (25 of 35 assignments), not from
    `load_review_spec`.
  - Spec **sub-models** also reach code under their own types (16 params). That is a fourth
    route the brief does not name.
- **I3 — the drift test compares the committed inventory to a fresh generation at the standard
  gate.** **TRUE, with one qualification.** The test, in `tests/test_inventory.py`:

  ```python
  def test_committed_inventory_is_in_sync_with_the_tree():
      """The whole point. If this is red, regenerate — do not edit the file."""
      message = inventory.drift(REPO_ROOT)
      assert message is None, message
  ```

  - It carries no marker, and `tests/test_inventory.py` has no `pytestmark`, so the gate's
    `-m "not network and not ollama and not integration"` selects it.
  - The qualification: `drift()` compares only the JSON `data` block (`review_ids`,
    `data_dirs_excluded`, `files`, `summary`). Its docstring says: "Only `data` is compared.
    `meta` carries the generating commit hash".
  - **`entry_points.md` is never compared.** A change to a label or verdict rule in
    `render_markdown` or `BASELINE` leaves the drift test green until the next `--write`
    rewrites the `.md`. The mislabelled row is exactly that kind of defect.

---

## Not asked

- **The verdict text on two rows is stale in the same way.** "Of those, name-only" (45/46) and
  "name-only, constructing ReviewDatabase" (20/21) have read "differs — unexplained,
  investigate" since `dfde733`. Their cause (`extraction_cleanup.py --spec → --codebook`) is
  established in P2.
- **The `raw yaml load sites` row (13 / 3)** also reads "unexplained". Not investigated.
- **`extraction_cleanup.py` is now a name-only entry point that constructs a
  `ReviewDatabase`.** It is the kind of site `name_only_constructing_reviewdatabase` exists to
  surface. Recorded, not evaluated.
- **Three guards are defensive against a field that cannot be absent.** The two cloud extractors'
  `getattr(self.spec, "cloud_models", None)` and `pdf_parser.py`'s `hasattr(spec, "pdf_parsing")`
  guard `ReviewSpec` members. Those members always exist on a loaded spec (`extra='forbid'`;
  `cloud_models: Optional[...] = None`; `pdf_parsing` has a `default_factory`). `hasattr` on
  them is always true for a real `ReviewSpec`. This is the same shape as the 2b guards that
  converted a field move into silent content loss. **SPEC-GUARD-01 material; not touched.**
