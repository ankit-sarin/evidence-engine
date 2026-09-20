# DISCOVERY-01 Part A — result-integrity reconciliation (D1-1 … D1-9)

**Date:** 2026-09-20. **Task type:** read-only diagnostic. No engine code changed, no fix, no
model call, no write to `review.db`.
**Harness session:** `ba16c4af-2c55-4156-81b6-aad32200662d`.
**HEAD at write:** `4e2a66c` (tree clean, level with origin, behind=0 ahead=0 measured at origin).
**Startup verify:** 04:48–04:53 UTC. **Part A fixtures:** 04:53–04:57 UTC.

**Trust statement.** Every classification below rests on a reproducer executed at this HEAD in
this session against a disposable temp `ReviewDatabase` (per R8), or on source read at this HEAD.
Nothing is inherited from the external assessment: R1–R5 are treated as *claims to be tested*,
and where a claim and the measurement disagree the measurement is what is recorded. Scratch
fixtures were written outside the repository and are not committed.

**Tags.** **MEASURED** = produced here by running code or a query. **READ** = quoted from a file
at this HEAD. **INFERRED** = a reading, a pairing or a cause. Every field below is tagged.

**Anchors are quoted content**, never line numbers. Where a quoted line occurs more than once in
its file, the enclosing function is named and the count given.

**The review database was opened read-only only** (`file:…/review.db?mode=ro`, never
`immutable=1`, one read transaction, no checkpoint). File before and after: 101,978,112 B,
mtime 2026-09-11 02:00:52.636956943 UTC.

---

## Classification table

| # | finding | classification | architect expected | agrees? |
|---|---|---|---|---|
| D1-1 | Rejection reaching export | **REPRODUCES** | REPRODUCES | yes |
| D1-2 | Stale workbook binding | **REPRODUCES** | REPRODUCES | yes |
| D1-3 | Audit completion with unresolved work | **REPRODUCES** | REPRODUCES | yes |
| D1-4 | Concordance selected-run / abstention | **REPRODUCES** | REPRODUCES | yes |
| D1-5 | Scoring semantics | **REPRODUCES** | REPRODUCES | yes |
| D1-6 | Categorical kappa (`metrics.py`) | **REPRODUCES** | REPRODUCES | yes |
| D1-6b | The 2g P3 screening kappas | **not defective** — own correct implementation, verified against sklearn | no expectation | — |
| D1-7 | WAL-safe backup and restore | **REPRODUCES** | REPRODUCES | yes |
| D1-8 | Completeness guard | **REPRODUCES** (declared deliberate in the code) | REPRODUCES | yes |
| D1-9 | Normalization codebook identity | **REPRODUCES** | FIXED-WITH-EVIDENCE | **NO — see §D1-9** |

Nine findings, nine classifications. Eight of nine match the architect's expectation. **D1-9 does
not**: the normaliser still reads one review's codebook from a module literal. The brief
anticipated this branch explicitly (*"a literal or default is REPRODUCES"*), so it is recorded
rather than escalated as a contradiction — but it contradicts the stated expectation and is
flagged here.

### The one chain that matters most

D1-1, D1-3 and the exporter are not three defects; they are one path, and it ends at the
**supported runner**:

```
import_audit_review_decisions(db, <completed audit queue>.xlsx)
  REJECT  ->  evidence_spans.value UNCHANGED, audit_status := 'verified'
          ->  complete_stage("AUDIT_REVIEW_COMPLETE")   [unconditional]
  is_audit_review_complete(conn)  ->  True
  scripts/run_pipeline.py::run_pipeline  ->  AUDIT REVIEW GATE does not block
  export_all()  ->  evidence_table.{csv,xlsx,docx} carries the rejected value,
                    in the audit column, as 'verified'
```

`workflow.py`'s own docstring says stage 12 is *"auto: import with zero unresolved spans"*. The
code does not check that condition.

---

## D1-1 — Rejection reaching export

**(a) ANCHOR** — READ. `engine/adjudication/audit_adjudicator.py`, function
`import_audit_review_decisions`, the `REJECT` branch of Pass 2:

```python
            review_db._conn.execute(
                """UPDATE evidence_spans
                   SET audit_status = 'verified',
                       auditor_model = 'human_review',
                       audit_rationale = ?,
                       audited_at = ?
                   WHERE id = ?""",
                (f"Rejected by human reviewer. {notes}", now, span["id"]),
            )
```

The last line is unique in the file. **`value` is not in the SET list.** The rejected value
survives; only its audit label changes, and it changes to `'verified'`.

The export side, `engine/exporters/evidence_table.py`, `_build_evidence_rows` (unique in file):

```python
                    row.extend([
                        span["value"],
                        span["source_snippet"] or "",
                        span["confidence"],
                        span["audit_status"],
                    ])
```

Found via the committed inventory (`docs/inventory/entry_points.json`, `--check` reports
*inventory in sync*); both files present, **zero anchor misses across all thirteen paths in I1**.

**(b) REPRODUCER** — MEASURED. Temp `ReviewDatabase` under a tempdir `data_root`, codebook
written beside it as `tests/conftest.py` does. One paper at `AI_AUDIT_COMPLETE`, one extraction,
`study_type = 'RCT'` `audit_status='flagged'`. One xlsx with
`PI_decision=REJECT`, `PI_notes="WRONG_VALUE: the paper is a cohort"`. Observed:

```
import stats: {'rejected': 1, 'papers_transitioned': 1, ...}
span after REJECT -> value='RCT' audit_status='verified'
audit_adjudication: [{'human_decision': 'reject_paper', 'original_value': 'RCT'}]
paper status: HUMAN_AUDIT_COMPLETE
CSV  (min_status=HUMAN_AUDIT_COMPLETE): study_type='RCT'  study_type_audit='verified'
XLSX (min_status=HUMAN_AUDIT_COMPLETE): study_type='RCT'  study_type_audit='verified'
```

Expected-if-defective was *"the rejected value appears as accepted/verified in at least one
export"*. It appears as `verified` in **two** — and in the strictest export mode there is
(`HUMAN_AUDIT_COMPLETE`, the human-verified production mode). The DOCX route was reached but
raised on a `None` spec before emitting (`AttributeError`), so it is **not** evidence either way;
it shares `_build_evidence_rows` by construction.

**(c) CALLERS** — MEASURED (AST call index over `engine/`, `scripts/`, `analysis/`, `tests/`).
`import_audit_review_decisions` is defined once and called from **nine tests in
`tests/test_audit_adjudication.py` and from no entry point in the tree**. It is nonetheless on
the supported path by the workflow module's own remediation text, `engine/adjudication/workflow.py`:

```
    "AUDIT_REVIEW_COMPLETE": (
        "Complete human review of the audit queue, then run:\n"
        "  from engine.adjudication.audit_adjudicator import import_audit_review_decisions\n"
        "  import_audit_review_decisions(review_db, 'path/to/completed_audit_queue.xlsx')"
    ),
```

— i.e. the workflow instructs the operator into the **xlsx** branch by name. `export_evidence_csv`
and `export_evidence_excel` are called by `engine/exporters/__init__.py::export_all`, which is
called by `scripts/run_pipeline.py::_stage_export` — **supported path, confirmed**.

**INFERRED, and material:** the sibling **JSON** route does *not* have this defect.
`engine/review/human_review.py::_apply_audit_decisions` handles `REJECT_VALUE` as

```python
                    """UPDATE evidence_spans
                       SET value = 'NR', audit_status = 'verified',
```

Two importers, one decision vocabulary, two different meanings of REJECT. This is the
*one-predicate-two-programs* shape: whichever is right, they cannot both be.

**(d) CONSEQUENCE** — MEASURED, read-only, from the live DB:
`audit_adjudication` = **0 rows**; `evidence_spans` with `auditor_model='human_review'` = **0**;
every one of the 3,760 spans carries `auditor_model='gemma3:27b'`; `AUDIT_REVIEW_COMPLETE` is
`pending`. **No human audit adjudication has ever been imported into the live review**, so no
existing artifact carries this defect — not Run 6's outputs, not `evidence_table.*` (written
2026-03-08), not the PI audit workbooks, not the 2g P3 kappas. **The exposure is entirely
prospective**, and it is aimed squarely at the first real PI audit import.

**(e) CLASSIFICATION — REPRODUCES.**
*Failing user-visible outcome:* a value the PI rejected as wrong is published in the
publication-ready evidence table with the audit column reading `verified`.
*Candidate passing condition:* after importing a REJECT, no export at any `min_status` presents
that span's original value, and its audit column never reads `verified` — the two importers
agreeing on one meaning for REJECT before either is wired to a CLI.

---

## D1-2 — Stale workbook binding

**(a) ANCHOR** — READ. `engine/adjudication/audit_adjudicator.py`,
`import_audit_review_decisions`, Pass 2 (the comment line is unique in the file; the SQL occurs
3× in the file, once per function — this is the occurrence inside the Pass 2 decision loop):

```python
        # Find the span
        extraction = review_db._conn.execute(
            "SELECT id FROM extractions WHERE paper_id = ? ORDER BY id DESC LIMIT 1",
            (pid,),
        ).fetchone()
```

The workbook carries `paper_id` and `field_name` and **nothing that identifies the extraction it
was generated from**. The importer binds to whatever is newest at import time.

**(b) REPRODUCER** — MEASURED. Temp DB. Extraction v1 `sample_size='50'` (flagged). A workbook
generated from v1 correcting it to `'500'`. Extraction v2 then lands, `sample_size='51'`,
already `verified`. The v1 workbook is then imported:

```
import of the STALE v1 workbook: {'corrected_fields': 1, 'papers_transitioned': 1, ...}  warnings: []
  v1 extraction 1 sample_size -> value='50'  audit='flagged'   rationale=''
  v2 extraction 2 sample_size -> value='500' audit='verified'  rationale="Human override: '51' -> '500'. …"
  audit_adjudication: [{'span_id': 5, 'field_name': 'sample_size', 'original_value': '51', 'override_value': '500'}]
```

Expected-if-defective was *"import applies v1's decisions to v2 or overwrites v2"*. It does
**both**, silently, with `warnings: []`. Three separate losses in one row: v2's newer value 51 is
destroyed; the audit trail records `original_value='51'`, a value **no human ever saw**; and the
span the decision was actually about is left `flagged` — so the paper still transitions to
`HUMAN_AUDIT_COMPLETE` on the strength of a decision applied to the wrong row.

**(c) CALLERS** — MEASURED. Same as D1-1: one definition, nine test callers, no entry point,
named by `workflow.py`'s remediation text. The **CSV** variant of the same mistake exists
independently at `engine/review/human_review.py::_import_review_csv`
(`ORDER BY es.id DESC LIMIT 1` over a paper/field join — also newest-wins, also no extraction
identity). The **JSON** route is the only one that binds by `span_id` and is therefore immune.

**(d) CONSEQUENCE** — MEASURED. Live DB: `audit_adjudication` = 0 rows, and **exactly one
extraction per paper** (190 papers × 1) and one `cloud_extraction` per (paper, arm). With one
version per paper there is no v1/v2 to confuse, so nothing on disk carries this today.
**INFERRED:** it becomes live the moment a second extraction round lands — which is precisely
what the Run 7 elicited path would produce — with any workbook generated before it.

**(e) CLASSIFICATION — REPRODUCES.**
*Failing user-visible outcome:* a PI decision taken against one extraction silently overwrites a
newer extraction's value and is recorded in the audit trail against a value the PI never saw.
*Candidate passing condition:* a queue workbook carries the `extraction_id` (or its hash) it was
built from, and an import whose extraction is no longer current refuses the file and names the
superseding extraction, rather than rebinding.

---

## D1-3 — Audit completion with unresolved work

**(a) ANCHOR** — READ. `engine/adjudication/audit_adjudicator.py`, end of
`import_audit_review_decisions`, at the same indent as the function body and outside every
conditional:

```python
    # Auto-advance workflow: AUDIT_REVIEW_COMPLETE
    complete_stage(
        review_db._conn, "AUDIT_REVIEW_COMPLETE",
        metadata=(
            f"{stats['accepted']} accepted, {stats['corrected_fields']} corrected, "
            f"{stats['rejected']} rejected (of {stats['total']} total)"
        ),
    )
```

Against `engine/adjudication/workflow.py`'s own stage list:

```
 12. AUDIT_REVIEW_COMPLETE     — auto: import with zero unresolved spans
```

The stated precondition — *zero unresolved spans* — is never evaluated.

**(b) REPRODUCER** — MEASURED. One paper, **two** flagged spans. A workbook resolving **one**
(`ACCEPT` on `study_type`). Observed:

```
import stats: {'accepted': 1, 'papers_transitioned': 0, ...}
unresolved spans still present: [{'field_name': 'sample_size', 'audit_status': 'flagged'}]
paper status:               AI_AUDIT_COMPLETE      <- correct, paper-level guard held
check_audit_review_gate():  1                      <- correct, one paper still unresolved
workflow_state AUDIT_REVIEW_COMPLETE: status='complete', completed_at='2026-09-20T04:53:02…'
```

Expected-if-defective was *"reported complete"*. **It is — at the stage level, while both the
paper-level transition and `check_audit_review_gate()` correctly report the work outstanding.**
That is the precise shape of the defect: two guards that work, and a third that overrides them
because it is the one the runner reads.

**(c) CALLERS** — MEASURED. `complete_stage` is called from eleven sites; the unconditional one
is `audit_adjudicator.py::import_audit_review_decisions` (and again in `_import_legacy_format`).
The consumer is **`engine/adjudication/workflow.py::is_audit_review_complete`**, whose only
non-test caller is **`scripts/run_pipeline.py::run_pipeline`** — the supported entry point —
at the `AUDIT REVIEW GATE`:

```python
        if start_idx <= STAGES.index("export"):
            if not is_audit_review_complete(db._conn):
```

`check_audit_review_gate`, the function that *does* count unresolved papers, is called by **no
production code at all** — only two tests. The correct predicate exists and is unwired; the
incorrect one gates the export.

**(d) CONSEQUENCE** — MEASURED. Live `workflow_state`: `AUDIT_REVIEW_COMPLETE` is `pending`
(`EXTRACTION_COMPLETE`, `AI_AUDIT_COMPLETE_STAGE` and `AUDIT_QUEUE_EXPORTED` likewise). The
five screening/FT stages are `complete`. So the gate has never been passed and no export has
ever been released through it. **Prospective only**; it would fire on the first audit import.
Note the live corpus has **1,152 flagged and 449 contested spans** outstanding across 190 papers
— exactly the population this gate exists to protect.

**(e) CLASSIFICATION — REPRODUCES.**
*Failing user-visible outcome:* the pipeline's export gate opens after a partial audit, so an
evidence table is published while 1,601 spans are still flagged or contested.
*Candidate passing condition:* `complete_stage("AUDIT_REVIEW_COMPLETE")` fires only when
`check_audit_review_gate(review_db) == 0`, and the runner's gate reads that same predicate —
one predicate, not two.

---

## D1-4 — Concordance selected-run / abstention

**(a) ANCHOR** — READ. `engine/analysis/concordance.py`, `load_arm`, local branch:

```python
            rows = conn.execute(
                """SELECT e.paper_id, es.field_name, es.value
                   FROM evidence_spans es
                   JOIN extractions e ON e.id = es.extraction_id
                   ORDER BY e.paper_id, es.field_name"""
            ).fetchall()
            for row in rows:
                pid = row["paper_id"]
                if pid not in result:
                    result[pid] = {}
                if str(row["value"] or "").strip().upper() in non_value:
                    continue
                result[pid][row["field_name"]] = row["value"]
```

(the `non_value` test occurs 2× — once per arm branch.) **There is no `WHERE` on run,
extraction id or recency, and no ORDER BY on extraction.** Every extraction a paper has ever had
is folded into one `{field: value}` dict. The cloud branch adds only `WHERE ce.arm = ?`.

The `continue` is the ELICIT-DESIGN-02 D1 site-5 guard, and it is correct in isolation. Its
interaction with the missing run selection is the defect: **the guard drops the newer
abstention, and the older assertion is what is left standing.**

**(b) REPRODUCER** — MEASURED. Temp DB, one paper. Run R1 (`extraction 1`)
`study_type='RCT'`. Run R2 (`extraction 2`, newer/selected) `study_type='NO_EVIDENCE_LOCATABLE'`
— the codebook's escape token. Observed:

```
load_arm('local') -> {1: {'study_type': 'RCT'}}
the value scored for paper 1 is: 'RCT'
after adding an absence-sentinel span (NOT_FOUND): {'sample_size': 'NOT_FOUND', 'study_type': 'RCT'}
```

Expected-if-defective was *"concordance for R2 still shows X"*. It shows `'RCT'`. The second
line also shows the guard behaving as designed on a sentinel (`NOT_FOUND` is a *value*, so it is
kept) — the two token classes are handled correctly; it is the run identity that is missing.

**(c) CALLERS** — MEASURED. `engine.analysis.concordance.load_arm` is called by
`concordance.py::run_concordance` (→ `run_all_pairs` → `main`, the module's own argparse entry
point, which the inventory lists), by `analysis/paper1/adjudication.py::export_ambiguous_pairs`,
and by `analysis/paper1/export_disagreement_pairs.py::build_disagreement_rows` — the last being
the producer of `disagreement_pairs_3arm.csv`, which `CLAUDE.md` names as the input to
`judge_cli` and `pass2_full`. **Supported and analysis paths both.** (A same-named `load_arm`
exists in `analysis/eval/score_screen2f.py`; it is unrelated — screening arms, not extraction
arms.)

**(d) CONSEQUENCE** — MEASURED. Live DB: `extractions` = exactly **1 per paper** for all 190
papers; `cloud_extractions` = exactly 1 per (paper, arm); **zero** spans in either span table
hold `NO_EVIDENCE_LOCATABLE` or `CONTRACT_UNMET`. So no committed concordance output, no
`disagreement_pairs_3arm.*`, and neither PI audit workbook can carry this today.
**INFERRED:** the defect is armed for the first re-extraction, and the elicited path is the one
that *produces* escape tokens — so the run that first emits an abstention is also the first run
whose concordance would silently read the previous run's claim.

**(e) CLASSIFICATION — REPRODUCES.**
*Failing user-visible outcome:* after a re-extraction, a field the current run declined to answer
is scored, reported and judged as though the current run had asserted the previous run's value.
*Candidate passing condition:* `load_arm` selects one extraction per (paper, arm) by an explicit
run identity, and a paper with more than one candidate extraction and no selection rule is a
refusal, not a silent fold.

---

## D1-5 — Scoring semantics

**(a) ANCHOR** — READ. `engine/analysis/scoring.py`, `_score_free_text` (both lines unique):

```python
    # Substring containment (either direction)
    if a in b:
        return FieldScore("MATCH", f"substring: '{a}' contained in '{b}'")
    if b in a:
        return FieldScore("MATCH", f"substring: '{b}' contained in '{a}'")
```

Containment in **either direction**, on the raw normalised string, with no token or word-boundary
condition and no polarity check.

**(b) REPRODUCER** — MEASURED. `score_pair` at HEAD, five pairs, no DB and no model:

| pair | field | result | detail |
|---|---|---|---|
| `'5'` vs `'50'` | `sample_size` (numeric) | **MATCH** | `substring: '5' contained in '50'` |
| `'5'` vs `'50'` | `key_finding` (free text) | **MATCH** | `substring: '5' contained in '50'` |
| `'benefit'` vs `'no benefit'` | `key_finding` | **MATCH** | `substring: 'benefit' contained in 'no benefit'` |
| `'reduced complications'` vs `'significantly reduced complications overall'` | `key_finding` | **MATCH** | substring |
| `'the intervention improved survival'` vs `'the intervention did not improve survival'` | `key_finding` | AMBIGUOUS | `Jaccard=0.43` |

Expected-if-defective was *"any scores as a match"*. Three do, including both R1 cases: **numeric
5 versus 50**, and a **negated benefit statement**. The fifth row is the honest qualifier — the
longer negation lands on AMBIGUOUS, not MATCH, so the negation failure is a property of *short*
values where the negation is a prefix, not of negation generally.

**Note on `sample_size`.** `normalize.py` sends `sample_size` through `_normalize_numeric`, which
strips non-digits and returns a **string**; `score_pair` then compares strings and falls through
to `_score_free_text`. Numeric fields are never compared numerically.

**(c) CALLERS** — MEASURED. `score_pair` is called by `engine/analysis/concordance.py::run_concordance`,
`analysis/paper1/adjudication.py::export_ambiguous_pairs`, and
`analysis/paper1/export_disagreement_pairs.py::build_disagreement_rows` — plus 19 tests. All
three callers are live analysis code; `run_concordance` sits under the module's own argparse
entry point.

**The behaviour is PINNED GREEN by a test.** `tests/test_concordance.py::TestScorePairFreeText::
test_substring_containment` asserts `'da Vinci Xi'` vs `'da Vinci Xi (Intuitive Surgical)'` →
MATCH. **INFERRED:** that case is a reasonable one, and the rule was written for it; the rule's
generality is what fails. Any fix must keep that case passing.

**(d) CONSEQUENCE** — MEASURED, and this one **does** reach committed artifacts.
`data/surgical_autonomy/exports/disagreement_pairs_3arm.{csv,html,xlsx}` (2026-03-20; 2,268 rows;
columns `local_vs_o4mini_score`, `local_vs_sonnet_score`, `o4mini_vs_sonnet_score`) are `score_pair`
output in full. Because that CSV is the declared input to `judge_cli` and `pass2_full`, every
downstream row inherits the selection: **`judge_ratings` 2,276 · `judge_pair_ratings` 6,828 ·
`fabrication_verifications` 7,422**, and both PI audit workbooks (v1 sampled from Pass 2; v2
sources arm values from that same CSV). `evidence_table.*` (2026-03-08) is *not* affected —
exporters do not score.
**INFERRED:** the direction of the error is one-way. A false MATCH removes a genuine
disagreement from the set, so the judge never saw it; it does not fabricate disagreements.

**(e) CLASSIFICATION — REPRODUCES.**
*Failing user-visible outcome:* a concordance measurement counts `5` and `50`, and `benefit` and
`no benefit`, as agreement — so the reported inter-arm agreement is biased upward and the
disagreements the judge was given are a filtered subset.
*Candidate passing condition:* numeric fields compare as numbers; free-text containment requires
token-boundary containment plus a polarity check, and `'da Vinci Xi'` vs
`'da Vinci Xi (Intuitive Surgical)'` still scores MATCH.

---

## D1-6 — Categorical kappa

**(a) ANCHOR** — READ. `engine/analysis/metrics.py`, `cohens_kappa` (unique in file):

```python
    p_match = n_agree / n
    p_mismatch = n_disagree / n
    p_e = p_match ** 2 + p_mismatch ** 2
```

Its inputs are `FieldScore` objects carrying only `MATCH`/`MISMATCH`/`AMBIGUOUS`. The original
label distributions are gone before the function is entered, so `p_e` is computed from the
**agreement rate**, not from the raters' marginals.

**Algebraically** — MEASURED, and this is the whole finding in one line. With `p_o` the observed
agreement, `p_e = p_o² + (1−p_o)²`, so

```
kappa_engine = (p_o − p_e)/(1 − p_e) = 1 − 1/(2·p_o)
```

**The reported kappa is a monotone function of the observed agreement alone.** It carries no
information the percent-agreement column does not already carry. It is not Cohen's kappa; it is
not chance-corrected by any rater's base rate.

**(b) REPRODUCER** — MEASURED. Two fixtures, engine vs an independent reference
(`sklearn.metrics.cohen_kappa_score`, **sklearn 1.8.0 importable — I6 confirmed**) and against
hand arithmetic from the contingency table:

*Fixture A* — 2×2, n=100, a=45 b=5 c=5 d=45 (both raters 50/50):

```
p_o = 0.9000   p_e = Σ pA(c)·pB(c) = 0.5000
TRUE kappa  = (0.9000−0.5000)/(1−0.5000) = 0.8000
sklearn                                  = 0.8000
engine metrics.cohens_kappa              = 0.4444      <- R1's number, exactly
engine field_summary().kappa             = 0.4444
engine's p_e = p_match²+p_mismatch²      = 0.8200
1 − 1/(2·p_o)                            = 0.4444
```

*Fixture B* — n=100, rater B constant `Yes`, rater A 95 `Yes` / 5 `No`:

```
p_o = 0.9500   p_e = 0.9500
TRUE kappa  = (0.9500−0.9500)/(1−0.9500) = 0.0000
sklearn                                  = 0.0000
engine metrics.cohens_kappa              = 0.4737      <- R2's number, exactly
1 − 1/(2·p_o)                            = 0.4737
```

Both external numbers reproduce to four decimals. The invariance was demonstrated directly: a
third label set with **true kappa 0.0** and the same `p_o = 0.90` returns the **same 0.4444** as
Fixture A's true 0.8000.

**Why a green suite never caught it** — READ. `tests/test_concordance_pipeline.py::TestCohensKappa`
has seven tests. Every one asserts a *range* or a degenerate endpoint: `== 1.0`, `== 0.0`,
`0 < kappa < 1.0`, `isnan`, or a count. **Not one asserts a kappa value against a known-correct
reference.** The defect is invisible to the suite by construction.

**(c) CALLERS** — MEASURED. `cohens_kappa` ← `metrics.py::field_summary` (+7 tests).
`field_summary` ← `engine/analysis/concordance.py::run_concordance` and
`analysis/paper1/export_disagreement_pairs.py::build_disagreement_rows`. Both live.
`percent_agreement` is correct and unaffected.
**A third, independent kappa exists**: `analysis/paper1/pi_audit_unblind.py::weighted_kappa` /
`weighted_kappa_with_ci`, pure-Python, ordinal, linear weights, bootstrap CI — it imports nothing
from `metrics.py`, so **the PI-audit judge-reliability kappas are not affected by this defect.**
Three kappa implementations in one repo, only one of them wrong.

**(d) CONSEQUENCE** — MEASURED. `disagreement_pairs_3arm.xlsx`, sheet `Summary`, **63 rows**, each
with a `kappa`, `ci_lower`, `ci_upper` triple, all produced by this function. Spot-verified:
`robot_platform / local_vs_o4mini`, `pct_agreement 0.6961`, published `kappa 0.2817`; and
`1 − 1/(2 × 0.6961) = 0.28172`. The published value is the defective formula to four decimals.
The same 63 values are rendered in the HTML's "Kappa by Arm Pair" table. The CIs inherit the
error (the SE uses the same `p_e`).
**Not affected:** the 2g P3 screening kappas (§D1-6b), the PI audit v1/v2 reliability statistics,
`evidence_table.*`, and `prisma_flow.csv`.

**(e) CLASSIFICATION — REPRODUCES.**
*Failing user-visible outcome:* every inter-arm kappa the project has published is a restatement
of percent agreement, so an agreement that is entirely explained by a skewed base rate is
reported as moderate reliability.
*Candidate passing condition:* `cohens_kappa` receives the two arms' **normalised label
sequences**, not the collapsed `FieldScore` verdicts, and Fixtures A and B return 0.8000 and
0.0000 against sklearn.

---

## D1-6b — The 2g P3 screening kappas (0.126 A-vs-D, 0.698 B-vs-D)

**Which function produced them** — MEASURED. **`analysis/eval/score_screen2f.py::cohen_kappa`**,
reached via `score_screen2f.agreement()` from `score()`. **I2 CONFIRMED**: that scorer is its own
implementation and is not in the external review's archive scope. It does **not** call
`engine/analysis/metrics.py` — `score_screen2f.py` imports `math`, `statistics`, `Counter` and
`analysis.eval.screen2f`, and nothing from `engine.analysis`.

**It is a correct Cohen's kappa** — READ:

```python
    cats = sorted(set(xs) | set(ys))
    po = sum(a == b for a, b in zip(xs, ys)) / n
    cx, cy = Counter(xs), Counter(ys)
    pe = sum(cx[c] * cy[c] for c in cats) / (n * n)
```

Marginals from the actual label distributions, over the observed categories. This is the textbook
estimator.

**Recomputation** — MEASURED. A-vs-D `final`, rebuilt from the committed arm outputs under
`docs/session-reports/screen-auth-2g/full/` (`arm_A/{primary,verifier}.jsonl`,
`arm_D/{primary,verifier}.jsonl`), final outcomes derived with `screen2f.final_outcome`, **no
workbook read, no DB**:

```
n papers (intersection of the committed arm outputs): 86
contingency: {'FLAGGED->FLAGGED': 38, 'FLAGGED->OUT': 39, 'IN->OUT': 4, 'OUT->OUT': 5}
pct agreement:                                   0.5
engine-of-record  score_screen2f.cohen_kappa:    0.1258
independent ref   sklearn cohen_kappa_score:     0.1258
```

Identical to `scoring.json`'s committed `A-D.final.kappa = 0.1258`, and identical to the
independent reference. **The 2g P3 kappas stand.**

The contrast is worth recording: fed the *same* 86 pairs, `engine/analysis/metrics.cohens_kappa`
returns **0.0** — because `p_o = 0.5` and `1 − 1/(2·0.5) = 0`.

---

## D1-7 — WAL-safe backup and restore

**(a) ANCHOR** — READ. `engine/utils/db_backup.py`, `auto_backup` — the whole mechanism is one
line, unique in the file:

```python
    shutil.copy2(str(db_path), str(backup_path))
```

A byte copy of the **main database file only**. `-wal` and `-shm` are not copied and no
checkpoint is taken. `sqlite3.Connection.backup()` appears nowhere in the repository; the only
hit for `.backup(` is a `sqlite3.connect` in the test file.

**(b) REPRODUCER** — MEASURED, twice.

*Minimal, per the brief:* temp DB, `PRAGMA journal_mode=WAL`, `CREATE TABLE`, a committed
`INSERT`, connection left open, no checkpoint.

```
-wal size after the committed INSERT: 12392 B
rows visible on the live connection:  1
auto_backup -> t.db.bak-disc01-… (4096 B)
backup sidecars copied?  -wal: False   -shm: False
rows readable in the BACKUP: ERROR: no such table: t
```

*In the production shape* — a real `ReviewDatabase` (which sets WAL in `__init__` and holds
`self._conn` open for its lifetime), the shape every real caller uses:

```
live DB journal_mode: wal        -wal size: 688072 B
live connection sees: papers=1 extractions=1 spans=1
backup: review.db.bak-disc01-prod-shape-… 4096 B   (live db file: 4096 B)
  BACKUP papers:          ERROR: no such table: papers
  BACKUP extractions:     ERROR: no such table: extractions
  BACKUP evidence_spans:  ERROR: no such table: evidence_spans
```

Expected-if-defective was *"row absent"*. The **entire schema** is absent: the backup is a
4,096-byte empty SQLite file. What survives a `copy2` is whatever SQLite happened to have
checkpointed into the main file; everything since the last checkpoint — i.e. the most recent
work, which is exactly what a pre-destructive-operation backup exists to protect — is in the
`-wal` and is not copied.

**Why the suite is green** — READ. `tests/test_db_backup.py`'s `temp_db` fixture does
`conn.close()` before returning and **never enables WAL**. The fixture is more permissive than
production: it proves a path `ReviewDatabase` cannot be in. Its three assertions (readable copy,
filename format, size equality) all pass against a closed rollback-journal database and would all
pass against this defect forever.

**(c) CALLERS** — MEASURED. `auto_backup` is called by **three production sites**:
`engine/core/database.py::ReviewDatabase.cleanup_orphaned_spans`,
`engine/core/database.py::ReviewDatabase.reset_for_reextraction`, and
`engine/utils/extraction_cleanup.py::cleanup_stale_extractions` — the last reachable from
`extraction_cleanup.py::main`, an argparse entry point the inventory lists and `CLAUDE.md`
documents (`python -m engine.utils.extraction_cleanup --review … --confirm`). All three hold an
open WAL connection at the moment they call it. Plus three tests.

**Restore** — MEASURED. **No restore procedure exists anywhere in the repository.** Grepping
`engine/`, `scripts/`, `analysis/` and `tests/` for `.bak` returns exactly three hits: the
filename construction in `db_backup.py`, a filename assertion in its test, and an unrelated
systemd `override.conf.bak` in `tests/test_ollama_input_fit.py`. **Nothing reads a `.bak-*` file
back, and no test exercises a restore.** The backup half is implemented; the restore half does
not exist in any form.

**(d) CONSEQUENCE** — MEASURED, read-only. Three backups sit beside the live DB and all three
open and read:

| file | papers | extractions | evidence_spans | cloud_extractions |
|---|---|---|---|---|
| `review.db.bak-pre-run6-cleanup-20260315194955` | 10,039 | 33 | 660 | 114 |
| `review.db.bak-pre-sonnet-cleanup-20260316-165020` | 10,039 | 39 | 779 | 298 |
| `review.db.pre_rename_backup` | 804 | 96 | 1,439 | 192 |

They are **readable but unverifiable**: SQLite auto-checkpoints at ~1,000 pages, so most rows do
reach the main file and these are not empty — but there is no way to establish that any of them
is complete as of its timestamp, and the loss is by construction the newest writes. `review.db`
itself is sound (its `-wal` was **0 bytes** at fingerprint time). These files are also the
population behind global open item **#9**.

**(e) CLASSIFICATION — REPRODUCES.**
*Failing user-visible outcome:* the safety copy taken immediately before a destructive cleanup
omits the most recent committed work, and in the worst case is an empty database — and there is
no restore path to discover that with.
*Candidate passing condition:* `auto_backup` uses `sqlite3.Connection.backup()` (or a
`VACUUM INTO`) on the live connection; a test that commits on an open WAL connection with no
checkpoint finds the row in the backup; and a restore procedure exists and is exercised.

---

## D1-8 — Completeness guard

**(a) ANCHOR** — READ. `engine/core/completeness.py`, `check_completeness` — the return
(`complete=not missing,` is unique in the file):

```python
    return CompletenessResult(
        complete=not missing,
        n_produced=len(seen),
        n_expected=len(expected),
        missing=missing,
        unexpected=unexpected,
        duplicated=tuple(sorted(set(dupes))),
        produced=tuple(produced),
    )
```

`complete` depends on `missing` alone. `unexpected` and `duplicated` are computed, carried, and
have no effect on the verdict. `enforce_completeness` raises only `if not result.complete`, then
logs the irregularity at WARNING.

**This is declared, not accidental** — READ, from the function's own docstring:

> *"Completeness means every expected field is present at least once; extra or duplicated fields
> are reported but do not by themselves fail the check — they are shape problems, not loss, and
> failing on them would turn a recoverable oddity into a dropped paper."*

**(b) REPRODUCER** — MEASURED. Temp review, three-field codebook. Payload: `study_type` twice
(once `RCT`, once `Cohort`), `sample_size`, `key_finding`, and `not_in_the_codebook`.

```
expected fields: ('study_type', 'sample_size', 'key_finding')
check_completeness -> complete=True duplicated=('study_type',) unexpected=('not_in_the_codebook',)
enforce_completeness RETURNED (no raise): complete=True
log: WARNING Paper 1 (local): extraction complete but irregular — 4/3 fields,
     unexpected=['not_in_the_codebook'], duplicated=['study_type']
```

Expected-if-defective was *"accepted as complete"*. It is — with a WARNING and no refusal.

**(c) CALLERS** — MEASURED. `check_completeness` ← `engine/agents/extractor.py::extract_paper_with_completeness`,
`engine/cloud/base.py::CloudExtractorBase.extract_with_completeness`, `enforce_completeness`, and
five `analysis/eval/` runners. `enforce_completeness` ← `engine/agents/extractor.py::extract_paper`,
`engine/cloud/base.py::CloudExtractorBase.store_result`, `engine/elicitation/pipeline.py::extract_paper_elicited`.
**All three of those are pre-write, on the supported path**, including the Run 7 elicited path.
`tests/test_completeness_guard.py::test_unexpected_and_duplicate_fields_reported_but_not_fatal`
**pins the current behaviour by name.**

**(d) CONSEQUENCE** — INFERRED, and bounded. A duplicated field means two spans compete for one
`span_map[field_name]` slot in `_build_evidence_rows` and one `result[pid][field_name]` slot in
`load_arm`; which one wins is row order. An unexpected field is stored and then ignored by every
consumer that iterates the codebook. **MEASURED:** the live DB's `evidence_spans` (3,760) is
consistent with 190 papers × the codebook set, and no consumer has reported a collision; this
read-out did not attempt a per-paper duplicate census, which would need a fuller query than the
brief's scope allows.

**(e) CLASSIFICATION — REPRODUCES** — with the qualifier that the behaviour is **declared
deliberate in the code**, so the finding is a *policy* question, not an oversight. It is recorded
as REPRODUCES because the brief's own test (*"accepted as complete"*) is satisfied.
*Failing user-visible outcome:* an extraction that produced two contradictory values for one
field is written to the database as complete, and which of the two reaches the evidence table is
decided by row order.
*Candidate passing condition:* a **ruling** on whether duplication is loss. If it is, a duplicated
field is a refusal with the same retry budget as a missing one; if it is not, the WARNING names
both values and the write path records which one was kept and why. Either way the choice stops
being implicit.

---

## D1-9 — Normalization codebook identity

**(a) ANCHOR** — READ. `engine/analysis/normalize.py` (both lines unique in the file):

```python
_FALLBACK_REVIEW_ID = "surgical_autonomy"


@lru_cache(maxsize=1)
def _default_codebook():
    return load_codebook_for(_FALLBACK_REVIEW_ID)
```

and, in `_get_field_def`:

```python
    cb = _default_codebook()
```

whose docstring says, verbatim: *"`spec` is accepted and ignored: callers pass one, and the schema
it used to carry is gone (SCHEMA-DERIVE-01)."*

**The call path, read end to end** — READ. `score_pair` → `normalize_for_concordance(field_name,
value, spec)` → `_get_field_def(field_name, spec)` → `_default_codebook()` →
`load_codebook_for("surgical_autonomy")`. **The `spec` argument reaches no loader.** It is not
obtained from the resolver; it is a module-level literal.

**On CODEBOOK-AUTH-01's `review`-key check.** The check is intact and is *not* what fails.
`engine/core/codebook.py::load_codebook_for` does

```python
    if cb.review != review_id:
        raise CodebookIdentityError(…)
```

and it passes here — because the literal `"surgical_autonomy"` is compared against
`surgical_autonomy`'s own codebook, which of course declares `surgical_autonomy`. **The guard
verifies that the codebook matches the review id it was handed; it cannot verify that the review
id was the caller's.** CODEBOOK-AUTH-01 fixed the loader. The review id is a different problem,
and the module says so in its own comment:

```python
#: GENERALIZE B2, still open. Concordance normalisation falls back to ONE
#: review's enum set when no spec is passed, which resolves another review's
#: categorical values against the wrong vocabulary — silently, with no
#: exception, just mismatches. SPEC-AUTH-01 only stops this module building
#: the path itself; the hardcoded review is PATH-AUTH-01's to remove.
```

**(b) REPRODUCER** — MEASURED. A scratch review `scratch_review_other` ≠ `surgical_autonomy`,
whose codebook declares `study_type` with enum `['RCT', 'Cohort']`. A spec object carrying
`review_id = 'scratch_review_other'` is passed in:

```
_FALLBACK_REVIEW_ID = 'surgical_autonomy'
_get_field_def('study_type', spec(review_id='scratch_review_other'))
  -> enum_values = ['Original Research', 'Case Report/Series', 'Review', 'Systematic Review',
                    'Meta-Analysis', 'Conference Abstract', 'Technical Report', 'Other']
normalize_for_concordance('study_type', '2', scratch spec) -> '2'
```

It returned **`surgical_autonomy`'s** enum set for a scratch review's field, with no exception and
no log line. Expected per the brief: *"a literal or default is REPRODUCES"*.

**(c) CALLERS** — MEASURED. `_get_field_def` ← `normalize.py::normalize_for_concordance`,
`engine/analysis/report.py::_is_categorical`, `engine/analysis/scoring.py::score_pair` — three
live sites, zero tests calling it directly. `normalize_for_concordance` ←
`analysis/paper1/consensus.py::derive_consensus`, `scoring.py::score_pair`, + 24 tests. Every
concordance and consensus measurement in the project passes through here.

**(d) CONSEQUENCE** — MEASURED. `docs/inventory/entry_points.json` lists **one** review id on
disk: `surgical_autonomy`. With a single review, the literal is coincidentally correct, so **no
committed artifact carries a wrong-vocabulary normalisation**: not `disagreement_pairs_3arm.*`,
not the PI audit workbooks, not the 2g P3 kappas (which do not use this module at all).
**INFERRED:** the defect is latent and is armed by the *second* review — which is the whole point
of the generalisation lane. Its failure mode is silence: a categorical value that should
normalise to a canonical enum entry is returned unchanged, so it simply scores MISMATCH.

**(e) CLASSIFICATION — REPRODUCES.**
*Failing user-visible outcome:* on any review other than `surgical_autonomy`, concordance scoring
resolves categorical values against `surgical_autonomy`'s vocabulary and reports the resulting
non-matches as genuine inter-arm disagreement.
*Candidate passing condition:* `_get_field_def` obtains its codebook from the review the caller
named — via the resolver, through `load_codebook_for(spec.review_id)` or
`load_codebook_beside(db_path)` — and a call with no review in scope refuses instead of
defaulting; `_FALLBACK_REVIEW_ID` ceases to exist.

---

## Assumption-ledger verdicts (I1–I7)

| item | verdict | evidence |
|---|---|---|
| **I1** — the 13 file anchors exist at those paths | **CONFIRMED** | All 13 present in `docs/inventory/entry_points.json` (203 files) and on disk. `python -m engine.tools.inventory --check` → *inventory in sync*, exit 0. **Zero misses.** |
| **I2** — the 2g P3 kappas came from `analysis/eval/score_screen2f.py`, outside the review's archive scope | **CONFIRMED** | `score_screen2f.cohen_kappa` via `agreement()` via `score()`. It imports nothing from `engine.analysis`; recomputation from the committed jsonl reproduces `0.1258` and matches sklearn exactly. |
| **I3** — migrations 010 and 011 have no applied-receipt | **DEFERRED to Part B (D2-6)** — not evaluated in Part A; no Part A conclusion depends on it. |
| **I4** — `screener.py` and `scripts/screen_expanded.py` still carry two copies of the routing rule | **DEFERRED to Part B (D2-7)** — not evaluated in Part A. |
| **I5** — a consistent read snapshot via `mode=ro` in one read transaction, without touching WAL state | **CONFIRMED** | Fingerprint pass: one `BEGIN … COMMIT`, 0.365 s, 24 tables. `review.db` size and nanosecond mtime identical before and after; `-wal` 0 B throughout; only `-shm` mtime moved, which the lane's convention excludes. |
| **I6** — sklearn importable in `.venv` | **CONFIRMED** | `sklearn 1.8.0` (also `scipy 1.17.1`). Used as the independent reference for D1-6 and D1-6b; hand arithmetic from the contingency table is shown alongside regardless. |
| **I7** — the 09:00 and 07:00 UTC cron jobs do not write `review.db` | **CONFIRMED for this session, by postcondition** | `review.db` size and mtime unchanged across the whole session; `-wal` 0 B. This is an observation of one window, not a proof about the jobs, and none of Part A depends on it. |

**R8** — *"conftest.py writes a codebook beside every temp ReviewDatabase"* — **CONFIRMED** and
used: `tests/conftest.py::_ensure_test_codebook`, called from the autouse `block_live_database`
fixture's `guarded_init`. The Part A fixtures replicate it explicitly rather than relying on
pytest, so every reproducer runs standalone outside the repository.

---

## Startup-verify results

| # | check | EXPECTED | measured | verdict |
|---|---|---|---|---|
| E1 | HEAD / tree / origin | `4e2a66c`, clean, level | `4e2a66c` (`4e2a66ce903c…`), porcelain empty, `origin/main…HEAD` = `0 0` | **match** |
| E2 | standard gate, five chunks | 2,329 passed / 17 deselected; 504/594/377/438/416; deselects 0/0/10/6/1 | 504 + 594 + 377 + 438 + 416 = **2,329**; deselects 0/0/10/6/1 = **17**; zero failures. Chunk list from `find tests -name '*.py' -not -path '*/analysis/*'` split into four, plus `tests/analysis`. Foreground. | **match** |
| E3 | `review.db` file | 101,978,112 B @ 2026-09-11 02:00:52.636956943 UTC | identical, and identical again after the fingerprint read | **match** |
| E4 | `tests/test_eligibility.py` | fourteen pinned SHA-256 surfaces green | 91 passed, 0 failed; **14** unique 64-hex constants in the file | **match** |
| E5 | ten row counts + 24 tables | see brief | all ten exact; **24 tables** including `sqlite_sequence` | **match** |

**No mismatch. No STOP condition was reached in the startup verify or in Part A.**

**Fingerprint** — `docs/session-reports/discovery-01/review_db_fingerprint_20260920T044845Z.json`.
Schema hash `1d6af8b9fa6780a7508aee41c13353f8127f51e6d8c250047822f3a69473a18d`; overall hash over
the per-table hashes `f376562e095cfbcbf23ca997f31375feba340f74ed7a14ffe26e42cc63839e00`; wall
0.365 s; URI `file:/home/ankitsarin/projects/evidence-engine/data/surgical_autonomy/review.db?mode=ro`
(**never `immutable=1`**, per R6); one read transaction held for the whole pass; canonical
serialization stated in the file itself. **The `-wal` file was 0 bytes — empty — at the time of
the read**, so the snapshot required no WAL frames and the main file alone is the whole state.

Per-table row counts against E5:

| table | E5 | measured | | table | measured |
|---|---|---|---|---|---|
| `papers` | 10,039 | **10,039** | | `cloud_evidence_spans` | 7,257 |
| `abstract_screening_decisions` | 21,374 | **21,374** | | `evidence_spans` | 3,760 |
| `abstract_verification_decisions` | 1,422 | **1,422** | | `fabrication_verifications` | 7,422 |
| `abstract_screening_adjudication` | 0 | **0** | | `full_text_assets` | 794 |
| `ft_screening_decisions` | 366 | **366** | | `judge_pair_ratings` | 6,828 |
| `ft_verification_decisions` | 182 | **182** | | `judge_ratings` | 2,276 |
| `ft_screening_adjudication` | 36 | **36** | | `judge_run_audit` | 1 |
| `extractions` | 190 | **190** | | `judge_runs` | 7 |
| `cloud_extractions` | 379 | **379** | | `parse_attempts` | 8 |
| `review_runs` | 6 | **6** | | `provenance_census_runs` | 2 |
| `audit_adjudication` | — | 0 | | `provenance_classifications` | 22,034 |
| `sqlite_sequence` | — | 5 | | `workflow_state` | 12 |

**Zero differences against E5.**

---

## What this read-out does not claim

* No fix is proposed or implemented. Every "candidate passing condition" is a *condition*, not a
  design, and D1-8's is explicitly a ruling the architect owes rather than an edit.
* Part A did not evaluate I3 or I4; both belong to Part B and are marked deferred, not confirmed.
* The duplicate-field census on the live corpus (D1-8 (d)) was **not** run — it needs a per-paper
  query wider than this brief's read budget, and the finding does not depend on it.
* The DOCX export route raised before emitting under a `None` spec, so it is recorded as
  unobserved rather than as passing or failing.
* `evidence_table.*`, `prisma_flow.csv` and the PI-audit reliability statistics were checked and
  found **not** to carry D1-1, D1-2, D1-3, D1-4, D1-6 or D1-9. They do inherit D1-5's selection
  effect only indirectly, through the disagreement set, and `evidence_table.*` not at all.
