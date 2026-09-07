# VERIFY-EXIT-01 — verification of the ELICIT-DESIGN-02 close state

**Date:** 2026-09-07 · **Machine:** DGX · **Mode:** read-only
**Result: 8/10 literal PASS.** Both MISMATCHes are literal-wording failures whose
underlying close claim verifies; neither indicates stale state. Architect ruling requested.

## Ledger

| id | expected | actual | verdict | note |
|---|---|---|---|---|
| C1 | HEAD == `dfe8cce` | `dfe8ccec9c08634662fafdf4564b40e9ba6c7c91` | **PASS** | exact |
| C2 | tree clean AND HEAD == fetched origin/main | clean; `git rev-list --left-right --count HEAD...origin/main` = `0 0` | **PASS** | fetch only, no pull |
| C3 | 1,714 passed, 0 failed, 0 errors | **1,718 passed, 11 deselected, 0 failed, 0 errors** (214.81s) | **MISMATCH** (reconciled) | see §C3 |
| C4 | `contract_unmet_token` present | line 72: `contract_unmet_token: "CONTRACT_UNMET"` | **PASS** | |
| C5 | zero `source_quote_required` on JUDGMENT | **zero in the entire file** — stated 0 / inferable 0 / judgment 0 | **PASS** | stronger than required |
| C6 | both JUDGMENT instructions rewritten stepwise | both are judgment-as-steps prose | **PASS** | text below |
| C7 | zero `Pass1ContractError` hits | **6 hits**, all prose/comment; **zero** code constructs | **MISMATCH** (literal) | see §C7 |
| C8 | review.db mtime `2026-07-27 19:47:48` | `2026-07-27 19:47:48.448294416 +0000`, 99,753,984 bytes | **PASS** | unchanged at end of task too |
| C9 | 3 scratch dirs present | 3/3 present | **PASS** | see §C9 |
| C10 | Ollama 0.21.0, NRestarts=0 | `ollama version is 0.21.0`; `NRestarts=0`, active/running since 2026-08-31 00:41:59 UTC | **PASS** | |

## §C3 — the count delta is a marker-expression difference, not a test delta

Total collected is **identical** in both measurements: 1,729.

| run form | selected | deselected | sum |
|---|---|---|---|
| `-m "not network and not ollama"` (CLAUDE.md "offline only") | **1,718** | 11 | 1,729 |
| `-m "not network and not ollama and not integration"` (CLAUDE.md "standard gate") | **1,714** | 15 | 1,729 |

`primer.md:12` records "1,714/1,714 offline tests green, **15 deselected**". The 15
identifies the figure as the **standard gate**, not the two-marker offline form. The
4-test delta is `integration`-marked tests (5 exist; one is also network/ollama-marked
so it is deselected either way):

- `tests/test_pdf_parser.py::test_parse_with_docling`
- `tests/test_pdf_parser.py::test_digital_routes_to_docling`
- `tests/test_pdf_parser.py::test_version_increments_on_reparse`
- `tests/test_pdf_parser.py::test_skip_if_same_hash`
- (`tests/analysis/paper1/test_judge_pass2.py::test_paper_366_grammar_prevents_four_element_emission` — deselected in both)

**Finding, not an obstacle:** C3 pinned the value 1,714 to the phrase "the way CLAUDE.md
specifies for offline runs", but CLAUDE.md defines two such forms and 1,714 belongs to the
narrower one. The close claim reproduces **exactly** under the gate it was measured at.
Zero failures and zero errors under both. Nothing on disk is stale.

## §C7 — six documentary references, zero code constructs

`git grep -nE "^\s*(class|from|import|raise|except|def).*Pass1ContractError"` returns
**nothing** (exit 1). The symbol is undefined, unimported, unraised and uncaught. All six
hits are prose explaining *that it was deleted* — which is the deletion being documented,
not surviving:

```
CLAUDE.md:284
docs/session-reports/ELICIT-DESIGN-02_report.md:39
docs/session-reports/ELICIT-DESIGN-02_report.md:56
docs/session-reports/ELICIT-DESIGN-02_report.md:213
engine/agents/extractor.py:667      (comment: "`Pass1ContractError` was the fourth member and is gone")
tests/test_elicitation_pipeline.py:327  (docstring: "Ruling 1 removed `Pass1ContractError` from that budget")
```

Recorded as MISMATCH because C7 said "any hit". Architect to rule whether the intended
check was "no code construct" (PASS) or literal grep-zero (MISMATCH, and the six comments
would then need rewording to avoid naming the symbol).

## §C9 — scratch trees

Parent: `data/surgical_autonomy/eval/elicit_design01/scratch/` (gitignored).

| dir | top-level entries | files (recursive) | bytes | mtime |
|---|---|---|---|---|
| `smoke_20260903T155654Z` | 1 (`elicit_design01`) | 6 | 551,020 | 2026-09-03 15:56:54 |
| `smoke_20260905T011330Z` | 1 (`elicit_design01`) | 6 | 596,483 | 2026-09-05 01:13:30 |
| `smoke_20260905T024348Z` | 1 (`elicit_design01`) | 4 | 438,109 | 2026-09-05 02:43:48 |

## §C6 — the two rewritten JUDGMENT instructions (verbatim)

Codebook: `data/surgical_autonomy/extraction_codebook.yaml` (gitignored; 36,450 bytes,
mtime 2026-09-05 02:38).

**`key_limitation`** (line 586, `field_class: judgment`), first two lines of `instruction`:

```
Reason to the key limitation in steps, and state it in 1–2 sentences.
Each step must either cite the unit(s) that support it or be marked as
```

**`clinical_readiness_assessment`** (line 600, `field_class: judgment`), first two lines:

```
Reason to this judgment in steps, then select the category. Each step must
either cite the unit(s) that support it or be marked as applying these
```

Field-class census confirms CLAUDE.md's 9/6/5: stated 9, inferable 6, judgment 5.

## Incidental observations

- **INCIDENTAL** — `data/data/surgical_autonomy/review.db/` is a *directory* (containing
  `review.db`, `-wal`, `-shm`, `pdfs/`, `parsed_text/`, `vector_store/`), i.e. a
  doubled-`data` path from a past bad `--review`/path argument. Gitignored, inert.
- **INCIDENTAL** — `data/test_verify/` holds a stale 131,072-byte `review.db` plus
  `-wal`/`-shm` sidecars, mtime 2026-08-25. Gitignored test residue.
- **INCIDENTAL** — `data/surgical_autonomy/eval/elicit_design01/aborted_smoke_20260903T153852Z/`
  exists alongside the three preserved trees; not named in the close claim, not checked.
- **INCIDENTAL** — the suite's collected total (1,729) is stable across both marker
  expressions, so no data-dependent collection is in play despite four test files
  parametrizing near codebook material.

## Gate compliance

- **G1** — all ten checks executed, each with an actual value. ✅
- **G2** — `git status --porcelain` byte-identical at start and end (both empty); HEAD
  unmoved at `dfe8cce`; `review.db` mtime unchanged after the suite run. Only this report
  file was written. ✅
- **G3** — this file is **uncommitted and untracked**. Not staged, not committed. ✅
