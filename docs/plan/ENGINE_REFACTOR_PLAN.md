# Engine Refactor Plan

**File of record:** `docs/plan/ENGINE_REFACTOR_PLAN.md` (evidence-engine repository) · **Origin:** architect session 2026-09-19 → 2026-09-21 (plan v49 §8 item 1) · **Companion:** Unified Plan v50 · **Prior name:** Engine Big Picture Plan — Sept 19

## Status

This is the standing engineering record for the multi-session refactor of the Surgical Evidence Engine. It was produced in the architect session of 2026-09-19 to 2026-09-21 (plan v49 §8 item 1) and is now the file of record at `docs/plan/ENGINE_REFACTOR_PLAN.md` in the evidence-engine repository, read by Claude Code at every session start. Unified Plan v50 is the short operational plan (status, next steps, procedures) and points here for the inventory, the solutions, the state ladder and the decision log. Maintenance: each session's closure paragraph is appended under Step 4 by Claude Code in its closeout under a brief from the architect; read-outs are never edited, corrections are dated addenda; a copy of the committed file is uploaded to project knowledge at every wrap.

| Step | Content | State |
| --- | --- | --- |
| 0 | Examine the external review, verdict on each item | Closed 2026-09-19 |
| 1 | Discovery from the current checkout via Claude Code | Closed: DISCOVERY-01 Parts A and B (7e09de3, b8a83b1); nine of nine findings reproduce; four v49 counts contradicted |
| 2 | Problem inventory | Closed: 53 rows in 10 classes, plus rows added in sessions 2–3 (I5, A10, C9, A11, C10) |
| 3 | Solutions | Closed: S0–S11 under nine PI rulings; six architect's assumptions kept as written |
| 4 | Order of sessions and steps, with gates | Active: four named states; sessions 1–3 closed; next is session 4 (S2 Phase 1, read-only) |

Evidence tags used throughout: MEASURED (a tool result at a named HEAD), READ (a file read this session — plan v49 and the review), INFERRED (another lane's report, including every code finding in the external review). The review's code findings come from a source archive dated Sep 19 whose relation to HEAD `4e2a66c` is unstated; they are INFERRED here until Step 1 re-measures them.

## Examination of the external review

The review is right on its central point and the architect's earlier proposal was wrong on ordering: it put a provenance stamp (migration 014) ahead of result-integrity defects that the plan never recorded. The architect reasoned from plan v49 alone, and v49 carries none of the twelve code findings the review lists. That is the failure mode plan §2.2 names (a plan is a summary), applied to the plan's own coverage: v49 calls itself the sole planning document while a code assessment with sixteen diagnostics sat outside it.

Verdicts on each item follow. Accept = adopt as written. Amend = adopt with a change stated. Reverse = the architect's earlier position was wrong and is withdrawn. Hold = cannot be decided until Step 1 measures it.

### Result integrity (review §7, §15)

| Finding (INFERRED from the Sep 19 archive) | Why it matters | Likely status vs HEAD `4e2a66c` (from v49 task ids) | Verdict |
| --- | --- | --- | --- |
| Excel REJECT kept WRONG\_VALUE and exported it as verified | Corrupts the evidence table and Paper 1 tables directly | No task id covers `audit_adjudicator.py` or `evidence_table.py`; likely open | Accept; Step 1 item |
| Stale workbook overwrote a newer extraction | Human corrections attach to the wrong run | Likely open | Accept; Step 1 item |
| Audit completed with a flagged span unresolved | Completion is not computed over required items | Likely open | Accept; Step 1 item |
| Categorical kappa computed from MATCH/MISMATCH frequencies (0.8000 reported as 0.4444; a constant-rater fixture gave 0.4737 for a true 0) | The measurement instrument for every concordance claim is wrong | No task id covers `metrics.py`; likely open. The 2g P3 screening kappas (0.126, 0.698) came from a different scorer — unknown whether it shares the helper | Accept; Step 1 must trace which function produced the screening kappas |
| Numeric 5 vs 50 and negated statements scored as matches | Concordance scoring does not obey field semantics | Likely open | Accept; Step 1 item |
| A newer abstention left an older claim in concordance | The selected run does not own its result | Likely open | Accept; Step 1 item |
| Live SQLite backup omitted a committed row (WAL) | Restore may lose data; the plan's `review.db` mtime baseline cannot detect a write that sits in the WAL | Likely open. The v49 convention "baseline is review.db alone" is an operational indicator, not an integrity check | Accept; Appendix B needs a logical-content check (per-table counts plus a content hash on a consistent snapshot) |
| Normalization loaded the autonomy codebook for a different review | Cross-review contamination | Probably closed by CODEBOOK-AUTH-01 (review key checked at load) and SPEC-AUTH-01 — unverified | Hold; Step 1 confirms with the current call path |
| Lexical sort chose parsed v9 over v10 | Wrong input text silently | PARSED-PATH-01 still queued ("seven resolvers"); likely open | Accept; folds into PARSED-PATH-01, which rises in rank |
| Duplicate and unexpected fields passed the completeness guard | Extraction output accepted with the wrong shape | No task id; likely open | Accept; Step 1 item |
| Configuration values not reaching model, search or parser calls; deepseek-r1:32b ran at temperature 0 despite different requested settings | Every model comparison is uninterpretable without the effective settings | Partly addressed (`extra='forbid'`); EFFECTIVE-SPEC-01 queued and v49 itself lists five subsystems on defaults not in the file | Accept; EFFECTIVE-SPEC-01 rises to a prerequisite for any benchmark |
| `corpus.py` predicate excluded EXTRACT\_FAILED | Eligibility conflated with processing success: an eligible study drops out because software failed | v49 calls `corpus.py` "reusable unchanged" on the grounds it has no topic literals — the review's point is that generic code can still encode a wrong rule | Accept; separate scientific eligibility from processing state |
| Runner did not connect all stages; extraction precheck ignored FT\_ELIGIBLE | No supported end-to-end path | FT\_ELIGIBLE has zero rows today so the defect is latent; it fires on the first new review | Accept; Step 1 item |
| The archive's spec disabled elicitation | The two-pass extraction with its passed smoke gate may not be the deployed path | v49 states the gate passed; it does not state the live flag | Hold; Step 1 confirms the live flag and the executed path |

### Screening design (review §3, §4, §5)

| Review recommendation | Verdict | Note |
| --- | --- | --- |
| Evidence-backed exclusion with cross-family verification is the leading candidate, adopted as an experiment with a declared evaluation target | Accept | It is a mechanism hypothesis (plan rule 18) and gets a pre-registered measurement, not a ruling |
| "Recall-bounded" overstates; say recall-prioritized with an evaluated target | Accept | No statistical bound has been established |
| The primary's silence-exclusions and the verifier's overturn rate share a possible cause, not a demonstrated one | Accept | The architect's "same flaw from two sides" is a hypothesis |
| Cross-family does not make errors independent; the verifier's ability to detect unsupported exclusions must itself be measured | Accept | Amends the interpretation of the settled cross-family rule: necessary, not sufficient |
| A fragment can supply a quote; keep SCREEN-INPUT-01 as its own adequacy contract, not folded into quote matching | Reverse | The architect claimed a fragment cannot supply a quote. Paper 737's abstract is two words and could be quoted. Input adequacy is a separate check; no blanket minimum length |
| Literal presence is not support: negation, scope, study attribution, criterion applicability are verifier work | Accept | The deterministic check closes fabricated quotation, not interpretation |
| Spans by offset into engine-materialized text, with the mapping preserved under any normalization | Accept | Stronger than quote-string matching |
| No stored decision is irreversible; superseding events plus one effective-result reader | Accept | This is an architecture item, not a screening item; it also fixes the stale-workbook and abstention findings |
| Measure the routing of uncertainty: human abstract review vs direct full-text advancement | Accept | A FLAGGED queue that saves no work is a cost, not a safeguard |
| Keep the evidenced-decision primitive small: shared representation, distinct acceptance policies; one abstract-exclusion route and one extraction field type first | Accept | The architect's "one primitive across three stages" was scope inflation |
| Hold the C1-only ablation rather than cut it | Accept | If the evidence-backed route fails its target, C1 becomes relevant again; a held item costs nothing |
| Hold the regex categorizer (2e) until consumers are known | Accept | Unchanged from the architect's position |
| Stop Part 2 of the 86-set workbook; keep the 86 as a regression and failure-analysis set | Accept |  |

### Evaluation program (review §6, §8)

| Review recommendation | Verdict | Note |
| --- | --- | --- |
| SYNERGY: adopt; audit label provenance per review; labels are published-review final inclusions, not an abstract-stage reference for negatives | Accept | The architect wrote "multi-adjudicator reference" without checking; that is withdrawn |
| Separate development reviews from held-out reviews before tuning; declare when a held-out set becomes development data | Accept | Missing from the architect's proposal |
| Training-data contamination is possible; record it, keep reference labels out of prompts, add a prospective review later | Accept |  |
| Three evaluation layers: contract fixtures (no inference), external screening reviews, a bounded clinical full-text set with independently adjudicated field answers | Accept | The title/abstract benchmark cannot validate extraction |
| Report per review and in aggregate; lost final inclusions, advanced, unresolved, human volume, failures, model time, human minutes | Accept |  |
| Comparators: existing route, all-advance reference, proposed route, a non-LLM baseline for any practice claim | Accept | The all-advance reference is the cheap control the architect omitted |
| 39–43% anchored is not extraction accuracy; the judge-health 12.1% needs its denominator | Accept | Both figures are instrument readings under a taxonomy |
| Six distinct field outcomes kept distinct in storage, scoring and export | Accept | Extends ELICIT-DESIGN-02's terminal states |
| Clinical fixture of roughly 15–25 reports and 8–12 fields, independently extracted, some held untouched | Accept as a development workload, not a validation sample |  |

### Architecture, order and product claims (review §9–§13)

| Review recommendation | Verdict | Note |
| --- | --- | --- |
| Migration 014's eligibility hash alone is insufficient; connect new decisions to a run/request record | Accept | 2d stays but its scope changes: stamp plus execution linkage, no invented history |
| Five contracts: review context, input snapshot, execution record, effective result, stage readiness | Accept as the target shape | Built in narrow slices, not as one refactor |
| Name one supported end-to-end entrypoint; declare stage omissions; resume obeys fresh-run prerequisites | Accept |  |
| Reuse depends on source, parse, contract, prompt, model, settings; a corrected parse invalidates dependent results | Accept | Extends the `codebook_hash` idempotence key |
| Data model must distinguish search record, report, study, arm, outcome observation; `sample_size` alone is insufficient | Accept as a reuse requirement | Scope it to what the second clinical review needs |
| Search/dedup evidence trail from events, not reconstructed from the corpus | Accept | The exporter's zero duplicate count is a Step 1 item |
| "Run 7 once the engine is settled" is an open-ended dependency; use a named contract freeze and a closed blocker set | Accept | Plan §5 item 20/21 wording changes |
| Order: 0 reconcile → 1 integrity → 2 execution foundation → 3 external baseline → 4 exclusion experiment → 5 extraction/judge → 6 release candidate | Accept as the dependency order | Step 4 of this document will place the §5 items inside it |
| Classify harnesses (supported / frozen experiment / obsolete duplicate); do not delete on inactivity | Accept |  |
| Product description: locally operated, human-supervised, explicit specs, source-linked extraction, auditable decisions, optional cloud benchmarking; end-to-end reliability under evaluation | Accept | Cloud arms become opt-in if local-only is a product promise; the run manifest records external transfers |
| Separate artifact reproducibility from computational repeatability | Accept |  |
| Do not publish landscape novelty claims without a reproducible landscape review | Accept | Plan §10 "no published system…" is downgraded to a dated search note |
| Throughput: about 47.7 serial hours upper bound for 10,000 records at measured means | Accept as arithmetic | Verified-OUT volume after routing is the number to measure |
| Plan v50: concise active plan; history to the repository; no hard byte limit | Accept |  |
| Next brief: read-only reconciliation of the source-audit findings, not migration 014 | Accept | Becomes Step 1 below, with the screening-seam facts the redesign needs added as a second read-out |

### Where the review is itself limited

Its code findings were reproduced on an archive, not at HEAD `4e2a66c`, with three function bodies isolated and stubbed; it says so. It did not have the current files. Its LUMEN and RAISE checks failed to confirm the architect's two citations; both are provisional until run through citation-mcp. It makes no claim about the screening scorer. None of these limits change any verdict above; they define what Step 1 has to measure.

## Step 1 — Discovery scope (approved 2026-09-19; executed as DISCOVERY-01)

One CC session, read-only throughout: startup verify, then two read-outs in sequence. No inference, no migration, no write to `review.db`, no rescreen. Disposable fixtures only. The session's product is two committed read-outs under `docs/session-reports/`; nothing else changes. The handoff block is written after the PI approves this scope.

### Why two read-outs and not one

D1 answers "is the evidence table and the measurement instrument trustworthy at HEAD." D2 answers "what does the screening redesign and a second review need from the code as it is." Step 2's problem list needs both. They are separate briefs because they touch different files and have different STOP conditions; they run back to back in one session.

### Pre-step: startup verify (Appendix B), plus one addition

Expected values as v49 Appendix B states them: HEAD `4e2a66c` clean and level; gate 2,329 / 17 in five chunks; `review.db` 101,978,112 B at 2026-09-11 02:00:52 UTC; fourteen frozen hashes green. Addition: compute and record a logical-content fingerprint of `review.db` from a consistent read snapshot (per-table row counts and a content hash over the dump). This costs one read-only pass and gives Appendix B the check the WAL finding says it lacks. Report the cost so it can become a standing close/start step.

### D1 — Result-integrity reconciliation

For each finding below, report five things: the current code anchor as quoted content (never a line number); whether a disposable-fixture reproducer shows the defect at HEAD; every caller from the AST inventory (not grep); the consequence for existing artifacts (Run 6 outputs, the PI audit workbooks, any concordance table); and a recommended repair order. Classify each as FIXED-WITH-EVIDENCE (name the commit and the test), REPRODUCES, ISOLATED-TO-UNUSED-ROUTE (name the route and prove nothing supported calls it), or UNRESOLVED-MATERIAL-UNAVAILABLE.

| # | Finding to reconcile | Anchor from the review (INFERRED — paths may have moved) |
| --- | --- | --- |
| D1-1 | A REJECT adjudication of a WRONG\_VALUE claim still reaches an export as verified | `engine/adjudication/audit_adjudicator.py`, `engine/exporters/evidence_table.py` |
| D1-2 | A stale workbook can overwrite a newer extraction; import does not bind to the exact reviewed result | `audit_adjudicator.py` |
| D1-3 | Audit completion is reported while a flagged span is unresolved | `audit_adjudicator.py` |
| D1-4 | Concordance mixes historical results; a newer abstention leaves an older claim in place | `engine/analysis/concordance.py` |
| D1-5 | Numeric 5 vs 50 and negated statements score as matches; substring matching | `engine/analysis/scoring.py`, `normalize.py` |
| D1-6 | Categorical kappa computed from MATCH/MISMATCH frequencies; plus: which function computed the 2g P3 screening kappas (0.126, 0.698) and whether it shares that helper | `engine/analysis/metrics.py`; `analysis/eval/score_screen2f.py` |
| D1-7 | Live backup under WAL omits committed rows; restore never proven | `engine/utils/db_backup.py` |
| D1-8 | Duplicate and unexpected fields pass the completeness guard | `engine/core/completeness.py` |
| D1-9 | Normalization loaded the autonomy codebook for a different review — confirm closed by CODEBOOK-AUTH-01 on the actual call path, or not | `normalize.py` |

STOP conditions for D1: a reproducer would need a model call; a reproducer would need the live database; a finding's anchor cannot be found by the inventory and a grep is the only remaining tool (report the miss, do not classify from grep).

### D2 — Execution foundation and the screening seam

| # | Fact to establish | Why Step 2 needs it |
| --- | --- | --- |
| D2-1 | For each Ollama call site: which requested settings (temperature, seed, num\_ctx, think, format) actually reach the request, taken from the request as sent, not the builder | Effective configuration is a prerequisite for any benchmark (review §9) |
| D2-2 | The live value of the elicitation flag and the extraction path that would execute on the next run | The archive's spec disabled elicitation; v49 reports a passed smoke gate for the two-pass path |
| D2-3 | `scripts/run_pipeline.py`: which stages it connects, what resume checks, whether the extraction precheck ignores FT\_ELIGIBLE | One supported entrypoint has to be named |
| D2-4 | `engine/core/corpus.py`: the exact status set the predicate includes and excludes; whether EXTRACT\_FAILED papers leave the corpus | Eligibility vs processing success |
| D2-5 | The seven parsed-text resolvers: which one the production path uses and how it orders versions | The v9/v10 lexical-sort finding |
| D2-6 | Whether migrations 010 and 011 are applied to the live schema (read-only schema inspection) | Any later write depends on it; v49 §9 known unknown |
| D2-7 | The screening routing rule: where it lives, whether `screener.py` and `scripts/screen_expanded.py` are still two copies, which is the production path, and how many sites change to verify OUT instead of IN | The redesign's first code change |
| D2-8 | The abstract-stage output schema as sent (the `format` block in frozen request P1) and what the six screening tables can store; whether any column could hold a criterion id, a span offset, or a verifier judgment on an OUT | The evidence-backed exclusion needs a schema and a place to persist |
| D2-9 | Whether `review_runs` links to any screening row today | 2d's scope changed to stamp plus execution linkage |
| D2-10 | The seven `analysis/eval/run_*` harnesses: classify each as supported product code, frozen experiment code, or obsolete duplicate, with the last-run evidence | Review §12: classify, do not delete |
| D2-11 | The exporter's duplicate count and where identification accounting comes from | Search/dedup evidence trail |

STOP conditions for D2: any item that would require running a model; any item whose answer would require opening `review.db` for write; a fact that contradicts a v49 §3.1 row (report the contradiction with both values, do not resolve it).

### Assumption ledger for the handoff (to be carried into Register B)

| Assumption | Tag | If false |
| --- | --- | --- |
| HEAD is `4e2a66c`, clean, level | MEASURED at last closeout | STOP; report the delta before either read-out |
| The review's file anchors exist at those paths | INFERRED (another lane; paths have been wrong before — Appendix A row 2) | Find by inventory; report misses |
| The Sep 19 archive equals some commit between `56c5c57` and `4e2a66c` | INFERRED | Irrelevant to the read-outs; they measure HEAD |
| The 2g P3 screening scorer is outside the earlier review's scope | READ (review §7) | D1-6 answers it either way |
| A consistent-snapshot fingerprint is possible without a write | INFERRED (SQLite backup API or `immutable=1` on a copy) | Report the obstacle; do not touch the WAL |

### Decisions the PI makes at this step

1. Approve D1 and D2 as the discovery scope, or add and remove items.
2. Approve the `review.db` fingerprint as a startup-verify addition.
3. Confirm both read-outs run in one CC session, D1 first.

## Step 2 — Problem inventory (consolidated after DISCOVERY-01)

Fifty-three problems in ten classes, from DISCOVERY-01 Parts A and B (commits `7e09de3`, `b8a83b1`; HEAD at read `4e2a66c`), plan v49's open items, and the PI's statements this session. The single deepest finding is A1: there is no reader of "the current value," and A2–A7, B3 and J3 are instances of that absence. Nothing here reverses a settled decision; two settled decisions are not yet implemented (Review Spec as configuration source — C1, C5; cross-family verification on the irreversible screening path — E1).

"On disk" says whether existing artifacts already carry the defect (WRONG), whether it fires on the next write or run (ARMED), or whether it only matters for a second review or an unbuilt stage (LATENT).

Provenance of human decisions: every human decision on disk (36 full-text adjudication rows, 31 `PDF_EXCLUDED` papers, 7 completed workflow stages, the 86-set labels, both PI-audit workbooks) was made by the PI without Claude (PI statement, 2026-09-20). The PI notes that those decisions are only as good as the information and clarity the queue artifact presented — see J5.

### A. Human decisions and the effective result

| ID | Problem | Evidence | On disk |
| --- | --- | --- | --- |
| A1 | No single reader of the current value: 13 stores, 15 readers, each with its own resolution rule. Three reader pairs disagree structurally at HEAD: exporter (latest extraction) vs concordance (folds every extraction); xlsx vs CSV importer on the same workbook; every reader vs the adjudication log, which nothing consults | D2-15 | ARMED (pair 3 is the current design) |
| A2 | `audit_adjudication` and `abstract_screening_adjudication` are write-only; `ft_screening_adjudication` is read once for a PRISMA count. A human decision survives only as the side effect applied to a span or a status | D2-15 | ARMED |
| A3 | Workbook (xlsx) REJECT keeps the rejected value and marks it `verified`; every export route emits it as verified, including `min_status=HUMAN_AUDIT_COMPLETE`. The JSON importer gives REJECT the opposite effect (value := `NR`). Two importers, one vocabulary, two meanings | D1-1 | ARMED (`audit_adjudication` 0 rows) |
| A4 | Workbook import binds to the newest extraction (xlsx) or the newest span across all extractions (CSV), never to the extraction the workbook was built from; only the JSON route binds by `span_id`. A stale workbook overwrites a newer run with `warnings: []` and records an `original_value` no human saw | D1-2 | ARMED on first re-extraction |
| A5 | `AUDIT_REVIEW_COMPLETE` completes unconditionally; the runner's export gate reads the stage flag; 1,152 flagged + 449 contested spans would export as complete. The legacy importer gated on `missing == 0`; the per-span importer that replaced it dropped the check | D1-3, D2-12 | ARMED |
| A6 | Stage-completion census over 12 stages: 3 unconditional (`AUDIT_QUEUE_EXPORTED`, `FULL_TEXT_ADJUDICATION_COMPLETE`, `AUDIT_REVIEW_COMPLETE`), 2 complete on "any > 0" rather than "all" (`EXTRACTION_COMPLETE`, `AI_AUDIT_COMPLETE_STAGE`), 2 proxies (loop ended), 2 manual. Three correct `check_*_gate` predicates have zero production callers | D2-12 | ARMED |
| A7 | `concordance.load_arm` has no run selection; it folds every extraction a paper ever had; a newer abstention is dropped by the non-value guard and the older claim stands | D1-4 | ARMED on first re-extraction |
| A8 | Policy: the completeness guard accepts duplicated and unexpected fields by declared design ("shape problems, not loss"); which duplicate reaches the table is row order. Made by the PI with Claude.ai; to be re-ruled at Step 3 | D1-8 | ruling pending |
| A9 | Corpus predicate excludes `EXTRACT_FAILED`: scientific eligibility conflated with processing success | D2-4 | LATENT (0 today) |

### B. Measurement instruments

| ID | Problem | Evidence | On disk |
| --- | --- | --- | --- |
| B1 | `metrics.cohens_kappa` = 1 − 1/(2·p\_o): a monotone function of observed agreement, not chance-corrected. 63 committed kappa/CI triples in `disagreement_pairs_3arm.{xlsx,html}` are the defective formula to four decimals | D1-6 | WRONG |
| B2 | `score_pair` free text: substring containment in either direction, no token boundary, no polarity; numeric fields are normalised to strings and never compared as numbers (`5` vs `50` MATCH; `benefit` vs `no benefit` MATCH). `da Vinci Xi` vs `da Vinci Xi (Intuitive Surgical)` MATCH is the case the rule was written for and must survive | D1-5 | WRONG |
| B3 | The judge's universe is the scorer's disagreement set: 2,266 of 3,802 cells; 1,535 (40.4%) never judged, one-directionally (false matches removed, none fabricated). Contaminated: the 63 kappas, every judge-derived rate, 7,422 fabrication verifications, both PI-audit sampling frames. Clean: FIELDCLASS-01 census rates (12.1%, 33.7%), `evidence_table.*`, `prisma_flow.csv`, the 2g P3 kappas, the PI-audit weighted kappa | D2-13 | WRONG |
| B4 | Three kappa implementations in one repo: `metrics.py` (wrong), `score_screen2f` (correct), `pi_audit_unblind.weighted_kappa` (correct, independent). Recorded so B1 is not over-applied | D1-6b | — |
| B5 | The suite pins three defects green by name (substring MATCH, permissive completeness, a backup fixture that closes the connection and never enables WAL) and asserts only ranges for kappa; 2,329 passing tests could not see any of the nine | D1-5, D1-6, D1-7, D1-8 | structural |

### C. Configuration and execution provenance

| ID | Problem | Evidence | On disk |
| --- | --- | --- | --- |
| C1 | Extraction model and temperature are module constants (`extractor.MODEL`, literal `0` at three sites); `spec.extraction_models.extractor` and `.temperature` reach no call — only `pass1_think`/`pass2_think` do. The live spec has no `extraction_models` block, so Run 7's deliberate elicitation flip has nowhere to go yet | D2-1, D2-2 | WRONG (Run 6 ran on constants) |
| C2 | No resolved configuration is persisted per run; temperature, seed, `num_ctx`, `think`, `format` are recorded nowhere for any non-judge run; `judge_runs.run_config_json` is the only run-level blob; `keep_alive` is never sent | D2-1 | WRONG |
| C3 | `review_runs` links to nothing: no `run_id` or FK in any of ten result tables; `log = '[]'` in all six rows; codebook hashes NULL | D2-9 | WRONG |
| C4 | Screening rows carry a model name only; no spec, prompt, renderer or routing identity | v49 §3.1, D2-9 | WRONG (21,374 rows) |
| C5 | Cloud arms are governed by the `--arm` CLI flag, not the spec; models and prices hardcoded; off-box transfer is neither spec-governed nor recorded in a run manifest. Mitigation: `cloud_extractions.prompt_text` stores the literal prompt sent | D2-2 | ARMED |
| C6 | Numbered migrations are never executed by `_run_migrations`; no receipt (`user_version 0`); 010/011 were hand-applied; a fresh `ReviewDatabase` lacks them, so the provenance census cannot run on a second review | D2-6 | LATENT, blocks reuse |
| C7 | Abstract `confidence` is required of the model (22,796 calls) and discarded; the abstract tables have no such column | D2-8 | minor |
| C8 | The auditor's `options={**{"temperature": 0}, **ollama_options}` is the engine's only options merge point; every other site is a literal | D2-1 | design |

### D. Input identity

| ID | Problem | Evidence | On disk |
| --- | --- | --- | --- |
| D1 | 42 glob resolvers in 37 modules plus 3 DB-driven; all 6 engine sites lexical (`sorted(..., reverse=True)`), so `_v9` beats `_v10`; the only numeric ordering is in `analysis/provenance/census.py` | D2-5 | LATENT (max version 3; all 190 agree today) |
| D2 | Extraction reuse key is `(paper_id, codebook_hash)`, checked before the text loads; a one-character edit or a new `_v2` parse is skipped | D2-14 | ARMED |
| D3 | D1 and D2 compose: a re-parse may be mis-resolved and is never re-extracted regardless | D2-5, D2-14 | ARMED |
| D4 | 350 NULL `parsed_text_path` rows (350 papers); 0 NULL resolutions for corpus papers under any of four selection rules; v49's "16" matches no rule | D2-5 | WRONG (plan) |
| D5 | Full-text input cut at 32,000 characters; no input-fit guard on the FT calls | v49 FT-INPUT-01 | WRONG (366 FT decisions on partial text) |
| D6 | Ollama client does not send `truncate: false` | v49 OLLAMA-CLIENT-01 | ARMED |

### E. Screening logic

| ID | Problem | Evidence | On disk |
| --- | --- | --- | --- |
| E1 | OUT, the only irreversible screening output, is decided by the same model agreeing with itself; the cross-family verifier sees primary IN only | v49 §3.1, D2-7 | WRONG (21,374 rows under this rule) |
| E2 | The primary excludes on silence; the declared-basis wording did not move it (D ≈ B) | 2g Part 2 | WRONG |
| E3 | The output schema is decision/rationale/confidence; no column in the six screening tables can hold a criterion id or a character offset; a verified OUT needs a `ScreeningDecision` change (moves frozen R1 and R2) plus a migration | D2-8 | LATENT |
| E4 | The routing rule exists as two copies; 11 code sites, R2, and the `abstract_verifier` stage policy change to verify OUT, because the verifier prompt is built to confirm an include | D2-7 | — |
| E5 | No input-adequacy check at abstract; a two-word abstract is screened as complete | v49 SCREEN-INPUT-01 | WRONG |
| E6 | The abstract adjudication path has no consumer (A2): FLAGGED → human → nothing | D2-15 | LATENT |
| E7 | The only screening reference is the 86-set: single review, boundary residue, mixed adjudication history, prefix-based labels (J5) | v49, review §2 | — |

### F. Runner and stage model

| ID | Problem | Evidence | On disk |
| --- | --- | --- | --- |
| F1 | `run_pipeline` connects 6 of 12 stages (search, screen, parse, extract, audit, export). Not connected: full-text screening, four acquisition steps ("PDF acquisition is manual for v1"), three adjudication round-trips, cloud extraction, the distribution monitor, concordance | D2-3 | — |
| F2 | The extract precheck selects `PARSED` only; `FT_ELIGIBLE`, the first corpus status, is invisible to the runner; 366 FT decisions exist that `run_pipeline` could not have written | D2-3 | LATENT (0 today) |
| F3 | No supported end-to-end entry point; the operational unit is the stage CLI | D2-3 | — |
| F4 | Harnesses: six under `run_*.py` (`elicit01/runner.py` a possible seventh); one live, four frozen with committed reports, one superseded whose output file is read by `analyze_capture01.py`; none deletable without orphaning a reader | D2-10 | — |

### G. Reuse across reviews

| ID | Problem | Evidence | On disk |
| --- | --- | --- | --- |
| G1 | 9 review-id constants, 35 literal sites, 24 files; three engine literals no resolver can reach: `normalize.py::_FALLBACK_REVIEW_ID`, `report.py::_get_tier_map`, `migrations/003` DATA\_DIR. The seven `DEFAULT_REVIEW` constants are argparse defaults and benign | D2-16 | LATENT |
| G2 | `normalize.py` hands its literal to the codebook loader; the review-key guard checks the codebook against the id it was handed, not the caller's; every concordance and consensus measurement passes through here | D1-9 | LATENT |
| G3 | `_get_tier_map`'s bare `except` turns any failure into an empty map: every field reports tier 0 silently | D2-16 | ARMED for any generated report |
| G4 | No second review has ever loaded; effective-spec resolution, the new-review scaffold, and the data model (search record / report / study / arm / outcome observation) are unbuilt | v49 §5, review §10 | — |

### H. Reporting

| ID | Problem | Evidence | On disk |
| --- | --- | --- | --- |
| H1 | PRISMA: `records_identified` is counted from surviving papers; `duplicates_removed` is a literal 0; the real dedup count reaches a log line and dies; `add_papers` dedups a second time uncounted. Committed CSV reads 251 / 0 / 251; run today it would read 10,039 / 0 | D2-11 | WRONG |

### I. Operations

| ID | Problem | Evidence | On disk |
| --- | --- | --- | --- |
| I1 | `auto_backup` is `shutil.copy2` of the main file under WAL: in the production shape the backup is a 4,096-byte empty database. Called at three destructive sites. No restore procedure exists in any form | D1-7 | ARMED (every backup to date unverifiable) |
| I2 | The size/mtime baseline cannot see a WAL-resident write; the fingerprint (0.365 s) exists but is not a standing close/start step | Part A | — |
| I3 | The nightly cron loads models without the experiment lock | v49 NIGHTLY-LOCK-01 | ARMED |
| I4 | The session-archive tool leaves `tool-results/` undetected | v49 ARCHIVE-FIX-01 | — |

### J. Operator and human-in-the-loop

| ID | Problem | Evidence | On disk |
| --- | --- | --- | --- |
| J1 | No operator interface for any human-in-the-loop step (abstract adjudication, full-text adjudication, audit review, PDF acquisition, manual stage advances); the entry instruction is a Python snippet in workflow remediation text; the queue is a file round-trip | D1-1(c), D2-12, D2-15 #12 | — |
| J2 | A review cannot be run without a Claude Code session: no supported runner, stage CLIs by hand, manual advances via `advance_stage`. The engine depends on Claude for operation, which violates the independence requirement (PI, 2026-09-20) | D2-3 | — |
| J3 | Decision semantics differ by surface (A3, A4); any interface built now inherits three binding rules and two meanings of REJECT | D1-1, D1-2, D2-15 | — |
| J4 | Manual advances and `bypass_stage` record no who or why; a human span decision carries `auditor_model = 'human_review'` and nothing else (INFERRED: no reviewer column was reported) | D2-12, D2-15 | — |
| J5 | What the reviewer saw at decision time is not persisted with the decision. The 86-set reference labels were made on 500-character prefixes and 3 of 18 flipped on full text; decision quality is bounded by the queue artifact's content and that bound is unrecorded | PI statement 2026-09-20; v49 2g Part 2 | WRONG (reference labels) |

### Not engine defects — plan v50 wording

The product description ("autonomously executes … synthesis," "publication-ready," "no data leaves the machine" — the last is CLI-flag-dependent per C5); the four contradicted counts (D4, D1, F4, G1); the §10 novelty sentence; and "Run 7 once the engine is settled" as an open-ended dependency.

## Step 3 — Solutions

Twelve solution units, S0–S11, each closing named rows of the Step 2 inventory under the nine rulings of 2026-09-20. Every unit is written at contract level: what must be true when it is done, what it costs, and what is left to Claude Code inside the contract. Sequencing and gates are Step 4. A coverage map at the end shows every one of the 53 rows against its unit.

Three conventions. "Acceptance" is a user-visible outcome plus a passing condition, never a test count. "Architect's assumption" marks a design choice made here that the PI has not ruled on and can reverse. Rows are cited by their Step 2 IDs.

### S0 — Safe ground: backup, restore, and the standing fingerprint

Closes I1, I2. Prerequisite for every unit that writes to `review.db`.

Design. `auto_backup` takes its copy through SQLite's online backup API (or `VACUUM INTO`) from the live connection, so WAL-resident commits are included. Every backup is fingerprinted the same way the live database is (per-table counts and content hashes) and the two fingerprints are compared before the backup is trusted. A restore procedure exists, is documented in Appendix B, and is exercised once on a copy of the live database before any migration in S2 or S3 runs. The fingerprint becomes a standing open/close step in Appendix B, replacing the size/mtime convention as the integrity check (size and mtime stay as a cheap first look).

Trade-offs. None material; the backup API is standard library. The restore drill costs one session-hour once.

Acceptance. The Part A reproducer (commit a row on an open WAL connection, no checkpoint, back up, open the backup) finds the row. A restore from a backup of a copy of the live database yields the live fingerprint. The `test_db_backup` fixture is rewritten to use WAL and an open connection (B5).

### S1 — Measurement instruments

Closes B1, B2, B3 (forward), B4, B5.

S1a Kappa. `cohens_kappa` receives the two arms' normalised label sequences, never the collapsed MATCH/MISMATCH verdicts; chance agreement is computed from each rater's marginals. One implementation is shared; `metrics.py` delegates to it. Whether `score_screen2f` (already correct) imports it or keeps its own is Claude Code's choice; the frozen harness is not edited for style. The 63 committed kappa/CI triples are regenerated and the March artifact is relabelled as superseded, not deleted.

S1b Scoring. Numeric fields compare as numbers (normalisation keeps the type; `sample_size` no longer falls through to free text). Free-text containment requires token-boundary containment and a polarity check (a negation cue in one value and not the other blocks MATCH). `da Vinci Xi` vs `da Vinci Xi (Intuitive Surgical)` stays MATCH; `5` vs `50` and `benefit` vs `no benefit` do not. Architect's assumption: a longer negation that lands on AMBIGUOUS today may stay AMBIGUOUS; the scorer is a triage instrument, not the judge.

S1c Judge universe. The judge's input is the full cell grid (paper × field × arm pair), with the scorer's verdict passed as a feature, not a filter. Consequence: 3,802 cells rather than 2,266 for the Run 6 corpus, about 1.7× the judge calls. Whether Run 6 is re-judged under this contract or the next run is the first to use it is a Step 4 decision; either way the Run 6 judge figures are relabelled "scorer-filtered subset" wherever they are cited.

S1d Test contract. For each instrument, a test asserts against an external reference (sklearn for kappa; hand-computed contingency tables) or a user-visible outcome, and the tests that pin the old behaviour by name are rewritten rather than deleted (B5).

Acceptance. Fixtures A and B return 0.8000 and 0.0000. The five scoring pairs from Part A return MATCH only for the da Vinci case. On the Run 6 corpus, a judge run under S1c attempts 3,802 cells.

### S2 — The effective-result model (ruling 7)

Closes A1, A2, A3, A4, A5, A6, A7, J3, J4, J5 (with S7), E6 (with S7). The spine; S7 and S4 stand on it.

Design, two levels, one discipline.

History is immutable. Two append-only event streams: paper-lifecycle events and field-value events. Every event carries an actor (a model name and digest, or a named reviewer), an occurred-at time, the run it belongs to, the exact prior result it was made against (a claim or event id), and, for a human decision, a hash of the context the reviewer was shown (J5). Model outputs written into `evidence_spans` are never mutated again; `value`, `audit_status` and their siblings become the last projection of the history, not the history.

One reader. `effective_value(paper, field)` returns the current value with its provenance and one of the field states in S5b; `effective_state(paper)` returns the paper's lifecycle state. Every exporter, scorer, judge loader, gate, PRISMA builder and corpus predicate reads through these two functions and nothing else. The resolution rule is written down and tested: a human decision beats a model value; a human decision made against a claim that has since been superseded puts the field into "needs re-review" with both values shown, rather than silently winning or losing; within one arm a newer extraction supersedes an older one for the same field unless a human decision is attached; a model's abstention is a state ("declined"), never skipped; a withdrawn value is no value.

Paper-level state vocabulary, from the PI's list with ruling 4's reasons. Identified · Duplicate (of) · Abstract: in / out, AI-verified (criterion, span) / out, human / insufficient input / needs review · Full text: obtained / not obtainable (reason) · Full text: in / out, AI-verified / out, human / needs review · Extracted · Extraction failed (reason) · Audited, AI · Audited, human · Analysis-ready. Each state is computed from events and carries who and why. `papers.status` is retained during migration as a projection; when the reader is the only consumer it is dropped.

Stage completion is derived, never stamped. A stage is complete when no paper in scope is in a state that stage owes work on. The three existing `check_*_gate` predicates become the definitions; the eleven `complete_stage` call sites are removed or made derived; manual advances and bypasses become events that require a reviewer identity and a reason (J4).

One importer, one vocabulary. ACCEPT · CORRECT(value) · WITHDRAW (ruling 1), applied identically from xlsx, CSV, JSON and, later, the app. A queue file carries the claim id it was built from and the presented-context hash; an import whose claim is superseded is refused and names the superseding claim (ruling 3). The two legacy importers are retired, not patched.

Migration. Existing rows are the current projection; historical events are reconstructed only where the record supports it (the 3,760 auditor verdicts, the 36 FT adjudications, workflow stamps) and are otherwise marked "state at migration" — no invented history. Run in slices under the hybrid ruling: event store and reader first; then readers migrated one at a time, in Step 4's order, each with a fixture showing the old and new reader agree on the live data before the old one is removed.

Trade-offs. Largest single change in the plan; a live migration (S0 first); a period during which two readers coexist; Paper 1 exports must be regenerated through the reader. The rule set is small and will be argued once, in the brief, not rediscovered per reader.

Acceptance. The Part A reproducers for D1-1, D1-2, D1-3 and D1-4 pass: a rejected value appears in no export at any status; a stale workbook is refused by name; a partial audit does not complete the stage; a declined field in the selected run is reported as declined. The three reader pairs from D2-15 return identical answers on a fixture with two extractions and one human decision. A manual advance without a reviewer and reason is refused.

### S3 — Execution provenance, configuration, and input identity

Closes C1, C2, C3, C4, C5, C6, C8, D1, D2, D3, D4, D5, D6, A9.

S3a Effective configuration and the run manifest. One resolver produces the effective settings for every call site from the spec, with every default declared in the spec model and none in module constants; `extractor.MODEL` and the literal temperatures go. Each run writes a manifest before its first call: engine state name and git tag, spec hash, codebook hash, per-stage model name, digest and options (temperature, seed, `num_ctx`, `think`, format-schema hash, `keep_alive`), renderer and prompt hashes, which cloud arms are enabled and what leaves the machine, host, start and end. `keep_alive` is set deliberately (architect's assumption: set it, to stop reloads between calls; measure). This is the scope migration 014 grows into.

S3b Run linkage. Every new decision, extraction and span row carries a `run_id` foreign key to the manifest. Existing rows keep `run_id` NULL with a "pre-manifest" marker; nothing is backfilled by guesswork.

S3c Migration runner. Numbered migrations are executed by `_run_migrations` with a receipt (`schema_version` or a migrations table); the two hand-applied migrations are registered as applied on the live database. A fresh `ReviewDatabase` reaches the live schema.

S3d Extraction reuse key. `(paper_id, parsed_text_hash, codebook_hash, prompt_hash, model_digest, options_hash)`. A changed input re-extracts; the previous extraction is superseded by an event (S2), never deleted.

S3e Input identity. One parsed-text resolver, database-driven, numeric version order, returning the asset id and hash; the six engine glob sites use it. The `analysis/` resolvers are frozen with their studies and left alone. The 350 NULL `parsed_text_path` rows are explained in an accounting note and the plan's "16" is corrected.

S3f Full-text input. `truncate: false` on every call; FT calls go through the input-fit guard; a full text that exceeds the context is not cut — the paper enters "full text exceeds context" and waits. Architect's assumption: refuse-and-mark first; section-aware chunking is a later unit if the volume warrants it.

S3g Cloud opt-in (ruling 6). The spec declares cloud arms enabled or not, per run; the CLI flag must agree with the spec or the run refuses; the manifest records the arms and the payload description; `cloud_extractions.prompt_text` continues to store what was sent.

S3h Eligibility versus processing (ruling 4). Corpus membership is computed from the effective state: eligible is eligible. `analysis_ready` is eligible and processing complete. Failure states carry a reason from a closed vocabulary (full text not obtainable; parse failed; extraction failed after retries; input exceeds context). Exports take `analysis_ready` and report the failed count by reason.

Acceptance. The Part B capture stub shows every call site's options originating in the spec or a declared default; a run manifest exists before the first call and the same run id is on every row the run wrote; a fresh database's schema hash equals the live one; a one-character change to a parsed text re-extracts; `_v10` beats `_v9`; a CLI cloud flag disagreeing with the spec refuses; a paper at `extraction failed` appears in the PRISMA flow with its reason.

### S4 — Evidence-backed exclusion, verified across families (ruling 5)

Closes E1, E2, E3, E4, E5, E7, C7. An experiment with a pre-registered bar, adopted only if it meets it.

S4a Contract. Primary output schema v2: decision ∈ {include, exclude, insufficient}; an exclude carries a `criterion_id` from the spec's eligibility criteria and a verbatim `quote`; the engine computes character offsets into the materialized title-and-abstract text and stores them. `confidence` is dropped from the schema (architect's assumption; it has never been used). Before the call, an input-adequacy check (E5) marks a record "insufficient input" and routes it forward without a model decision.

S4b Verification. Deterministic first: the quote must occur verbatim in the materialized text; a failure means the record advances, never OUT. Then cross-family: gemma3:27b is asked whether this quoted span, under this criterion, excludes this record, with instructions on negation, scope, study attribution and criterion applicability. Only its agreement makes the record "out, AI-verified." Everything else advances to full-text screening by default (PI ruling); the volume of unevidenced and insufficient records is a primary measurement, and the default is revisited with that number.

S4c Routing as declared data. Which decisions are verified, by which family, and which are final live in the spec's stage policy; one implementation reads it; `screen_expanded.py` reads the same table. The eleven sites collapse.

S4d Storage. Screening decisions are events in S2 with criterion, offsets, verifier verdict and run id.

S4e Verifier prompt. A new `abstract_verifier` stage policy for verifying an exclusion; R1 and R2 are re-pinned after the change through the eligibility model's identity gate; the old surfaces are archived with their hashes.

S4f Evaluation, pre-registered before any run. Reference: SYNERGY, label provenance audited per review, development and held-out reviews declared before tuning, enough pooled held-out final inclusions to bound the loss (about 300 for a 1% upper limit at zero losses). Comparators: the current route, an all-advance reference, the proposed route, and a non-LLM baseline (an ASReview-style TF-IDF/SVM prioritiser). Metrics per review and pooled: final inclusions lost (point estimate and 95% upper limit), work saved, insufficient-input volume, unevidenced-exclusion volume, model time, human minutes. Adoption bar: at most 1% of final inclusions lost, per review (Cochrane RCT Classifier standard, PMID 33171275); dual-human 97.5% (PMID 31972274) reported as the practice comparator. Training-data contamination is recorded as a limit; reference labels never enter a prompt. The 86-set becomes a regression and failure-analysis set only.

Trade-offs. More model calls (an upper bound near 48 serial hours per 10,000 records before the verbatim filter removes most candidates); two frozen surfaces move; the unevidenced pile may be large. Straight-to-full-text costs PDFs, and acquisition is manual today (S6 makes that an explicit waiting state, S7 makes it visible).

Acceptance. No record leaves the abstract stage as OUT without a stored criterion, offsets, and a verifier verdict from a different model family. On the held-out SYNERGY reviews the proposed route's loss and its upper limit are reported per review against the bar, with the three comparators beside it.

### S5 — Extraction outcomes, duplicates, and the judge (ruling 2)

Closes A8 (as implemented policy) and the forward half of B3.

S5a Duplicates and unexpected fields. A duplicated field is a contract failure with the same retry budget as a missing one; if it persists, both values are retained as claims and the field enters "unresolved: duplicate values" for a person. Unexpected fields are dropped and logged, never stored. `test_unexpected_and_duplicate_fields_reported_but_not_fatal` is rewritten to the new policy.

S5b Field state vocabulary, stored, scored and exported distinctly: asserted with evidence · asserted without locatable evidence · declined · contract unmet · missing · unresolved (duplicate or needs re-review) · corrected by human · withdrawn.

S5c Judge redesign. The judge receives the field definition, the surrounding context, both values and their spans, and instructions to distinguish negation, arm, denominator and timepoint; it scores all cells (S1c). Development workload: 15–25 reports × 8–12 fields independently extracted, some held untouched; a workload, not a validation sample.

S5d Elicitation. The live spec gains an `extraction_models` block; with S3a the values reach the call; Run 7 turns elicitation on deliberately and the manifest records it.

Acceptance. The Part A completeness reproducer refuses and retries; after the budget, both values are visible in the queue. Every field in an export carries exactly one of the S5b states.

### S6 — One supported entry point and an explicit stage model

Closes F1, F2, F3, F4, J2 (with S7).

Design. One runner connects every stage in order — search → dedup → abstract screening → abstract adjudication → acquisition → parse → full-text screening → full-text adjudication → extraction → audit → audit review → export — with stage readiness computed from effective states (S2). Stages that need a person do not stop the runner; they leave papers in an "awaiting …" state the runner reports and S7 shows. Acquisition stays manual for now but becomes an explicit state with an import step, not a log line. Resume obeys the same prerequisites as a fresh run. The extraction stage selects on effective state, so `FT_ELIGIBLE` is not a special case. Harnesses are classified in a committed table (S-supported, F-frozen with its study, X-superseded) with the `run_local_ab` output reference fixed; nothing is deleted.

Acceptance. A second, empty review can be carried from search to an export by the runner and the app alone, with every human step surfaced as a waiting state and no terminal session.

### S7 — The operator app (ruling 8)

Closes J1, J2, E6, J5 (with S2).

Design. A Gradio app on the DGX, served locally, single named reviewer (identity captured at login; J4). It reads and writes only through the S2 API — no direct SQL. Screens in build order: (1) review dashboard: paper-state counts, stage readiness, what is waiting on a person; (2) audit review: one span at a time with the value, the evidence quote in its passage, the auditor's rationale and the full paper text on screen; actions ACCEPT / CORRECT / WITHDRAW; the presented-context hash stored with the decision; (3) abstract and full-text adjudication screens on the same pattern; (4) a run launcher that starts a stage with a manifest. The DGX `gradio` and `brand` skills apply.

Trade-offs. A new codebase to maintain; it must not be built before S2 or it freezes today's inconsistencies into screens; single-reviewer authentication is deliberately minimal.

Acceptance. The PI completes an audit decision end to end without a terminal, and the decision appears in the next export through the reader with reviewer, time and presented context recorded.

### S8 — Search and dedup accounting

Closes H1.

Design. Search and dedup write events: records returned per source, duplicates removed by the deduplicator, duplicates skipped by `add_papers`, each counted. PRISMA is built from events, not from the surviving corpus; failure reasons (ruling 4) appear in the flow. The committed `prisma_flow.csv` is regenerated and the March version labelled superseded.

Acceptance. A PRISMA export's identified count equals the sum of source events, its duplicates count equals the sum of both dedup events, and identified minus duplicates equals screened.

### S9 — Reuse across reviews

Closes G1, G2, G3, G4, C6 (with S3c).

Design. The three unreachable literals go: `normalize.py` takes its codebook from the caller's spec or the resolver and refuses without one; `report.py::_get_tier_map` takes the review from the resolver and raises on failure; migration 003 takes `--review`. The new-review scaffold and effective-spec resolution are built by making the SYNERGY reviews the first real users (S4f): each needs an eligibility spec transcribed from its published criteria, which is the actual test of a review-agnostic eligibility model. The clinical data model (search record / report / study / arm / outcome observation) is deferred until the second clinical review; the SYNERGY runs need screening only.

Acceptance. A scratch review with a different vocabulary normalises against its own codebook; a report on it shows real tiers; two SYNERGY reviews load, screen and export with no literal naming the autonomy review on any path.

### S10 — Operations

Closes I3, I4. The nightly job takes the experiment lock; the archive tool detects `tool-results/`. Small, independent, any session.

### S11 — Named engine states and the product description (ruling 9; not defects)

An engine state is a git tag plus the manifest fields in S3a. The first named state is *freshman*, defined in Step 4 as the set of units closed before Run 7. The product description in plan v50: a locally operated, human-supervised review engine with explicit eligibility and extraction specifications, source-linked extraction, auditable decisions at every stage, and optional cloud benchmarking that is off by default and recorded when on; end-to-end reliability under evaluation. The §10 novelty claim becomes a dated search note pending a reproducible landscape review.

### Coverage map — every Step 2 row to its unit

| Rows | Unit |
| --- | --- |
| A1 A2 A3 A4 A5 A6 A7 J3 J4 | S2 |
| A8 | S5a |
| A9 | S3h |
| B1 B4 | S1a |
| B2 | S1b |
| B3 | S1c (instrument) · S5c (judge) |
| B5 | S1d · S0 |
| C1 C2 C8 | S3a |
| C3 C4 | S3b · S4d |
| C5 | S3g |
| C6 | S3c · S9 |
| C7 | S4a |
| D1 D3 D4 | S3e |
| D2 D3 | S3d |
| D5 D6 | S3f |
| E1 E2 | S4a · S4b |
| E3 | S4d |
| E4 | S4c |
| E5 | S4a |
| E6 | S2 · S7 |
| E7 | S4f |
| F1 F2 F3 F4 | S6 |
| G1 G2 G3 G4 | S9 |
| H1 | S8 |
| I1 I2 | S0 |
| I3 I4 | S10 |
| J1 J2 | S7 · S6 |
| J5 | S2 · S7 |

### Architect's assumptions in this step, for the PI to keep or reverse

1. `confidence` is dropped from the screening schema (S4a).
2. A full text that exceeds the context waits rather than being chunked (S3f).
3. `keep_alive` is set and measured (S3a).
4. The scorer stays a triage instrument; the judge, not the scorer, is the instrument of record (S1b, S1c).
5. Single-reviewer authentication in the app (S7).
6. Run 6's judge figures are relabelled rather than re-run until Step 4 decides (S1c).

## Step 4 — Order of sessions and steps, with gates

The order follows dependency, not preference: nothing writes to the database before the backup works; nothing is measured before the instruments are right; nothing reads a value before there is one reader; nothing runs on the corpus before its configuration is recorded. Units are grouped into four named engine states. A state is a git tag plus the manifest fields of S3a; Run 7 runs on the first.

### The four named states

| State | Meaning in one sentence | Units closed | What stays open, with its stated limit |
| --- | --- | --- | --- |
| **freshman** | Run 7 produces valid, reproducible, correctly measured extractions | S0 · S1a–d · S2 core (event store, reader, resolution rule; readers migrated: concordance, exporter, judge loader, corpus predicate) · S3a b c d e g h · S3f (`truncate: false` only) · S5a b d · S11 (tag and manifest naming) | No human audit path yet (importer and stage derivation not migrated); judge prompt unchanged (S5c); screening route unchanged (loss unmeasured); no runner, no app; FT input-fit guard absent (FT screening is not re-run in freshman); PRISMA counts still reconstructed |
| **sophomore** | A person can audit Run 7 without Claude | S2 remaining slices (one importer, one vocabulary; stage completion derived; manual advances as events) · S5c · S7 dashboard and audit screen · S10 | Screening route unchanged; runner absent; PRISMA reconstructed; second review not yet loaded |
| **junior** | The abstract stage is measured against an external reference and adopted or not on the bar | S9 · S4a–f | Runner absent; remaining app screens; PRISMA reconstructed |
| **senior** | One complete workflow a second researcher can run end to end | S6 · S8 · S7 remaining screens · S3f chunking if warranted · product description in v50 | Clinical data model (report / study / arm / outcome) deferred to the second clinical review; synthesis deferred |

### Session sequence to freshman

One task per session, one verification checkpoint before the next. Every session opens with the Appendix B startup verify plus the fingerprint and closes with the fingerprint; a session that migrates the database records the fingerprint before and after and verifies its backup first. Standard gate before every commit. Sessions are units of work, not dates.

| # | Session | Unit | Gate to pass before the next session |
| --- | --- | --- | --- |
| 1 | Backup, restore, fingerprint | S0 | The WAL reproducer finds the row in the backup; a restore drill on a copy reproduces the live fingerprint; the fingerprint tool is in the repo and Appendix B |
| 2 | Instruments | S1a S1b S1d | Fixtures A and B return 0.8000 and 0.0000 against sklearn; the five scoring pairs; the 63 kappas regenerated and the March artifact labelled superseded |
| 3 | Migration runner and receipts | S3c | Numbered migrations execute with receipts; 010 and 011 registered as applied on the live database; a fresh database's schema hash equals the live one |
| 4 | Effective-result model, Phase 1 (read-only) | S2 design | A read-out proposing the event tables, claim identity, the resolution rule as a table, and the migration plan; the PI approves the rule before Phase 2 |
| 5 | Effective-result model, Phase 2 | S2 core | Migration applied after a verified backup; `effective_value` and `effective_state` exist with the rule tested on fixtures; on the live data the new reader agrees with each old reader wherever the old reader was unambiguous, and every disagreement is listed with its cause |
| 6 | Readers onto the reader | S2 readers · S1c · S3h | Concordance, exporter, judge loader and corpus predicate read through the reader; the D1-4 reproducer passes; a judge run under S1c attempts 3,802 cells; `EXTRACT_FAILED` papers stay eligible with a reason |
| 7 | Configuration, manifest, run linkage, cloud opt-in | S3a S3b S3g | The capture stub shows every option originating in the spec or a declared default; a manifest exists before the first call; `run_id` on every row a run writes; a cloud flag disagreeing with the spec refuses |
| 8 | Reuse key and input identity | S3d S3e | A one-character change to a parsed text re-extracts and supersedes; `_v10` beats `_v9`; the 350 NULL rows explained |
| 9 | Write path: duplicates, field states, elicitation, truncate | S5a S5b S5d S3f-min | The completeness reproducer refuses and retries; every written field carries one S5b state; the live spec has an `extraction_models` block whose values reach the call; `truncate: false` on every call |
| 10 | Freeze and smoke | S11 · freshman tag | Full gate green; fingerprint recorded; tag `freshman` with manifest fields; a 5-paper smoke run on the tagged state writes a manifest, run-linked rows, S5b states, and a concordance that reads through the reader |
| 11 | Run 7 | — | Launched in tmux `--background` under the experiment lock on the tagged state; no engine code changes while it runs |

Sessions 4 and 5 are the same task in two phases because the resolution rule is a PI decision and the migration is irreversible in spirit even with a backup. Session 6 may split in two if the judge loader's migration turns out to be large. The count is eleven sessions to Run 7; at this project's measured cadence that is weeks, and the plan should say so rather than imply otherwise.

### While Run 7 runs

The engine's hot path is frozen. Sessions in this window touch only documents, specs, analysis code that does not run in Run 7, or operations: S10; the S4f pre-registration document; the SYNERGY review selection and label-provenance audit; transcription of two or three SYNERGY eligibility specs (the first real test of S9's path, done as spec files, not engine edits); the S2 Phase 1 read-out for the importer and stage-derivation slices.

### After Run 7: sophomore, junior, senior

| # | Session | Unit | Gate |
| --- | --- | --- | --- |
| 12 | One importer, stage derivation, manual advances as events | S2 remaining | D1-1, D1-2, D1-3 reproducers pass; a manual advance without reviewer and reason refuses; the two legacy importers retired |
| 13 | Judge redesign | S5c | Judge scores the clinical fixture with negation, arm, denominator and timepoint distinguished; Run 7's judge pass runs under S1c and S5c |
| 14–15 | App: dashboard and audit screen | S7 | The PI completes an audit decision on Run 7 without a terminal; the decision appears in the next export through the reader with reviewer, time and presented context. Tag `sophomore` |
| 16 | PI audit of Run 7 | — | Human audit proceeds through the app; Paper 1's extraction arm is fixed at this point |
| 17 | Reuse: the three literals, new-review path, effective spec | S9 | Two SYNERGY reviews load and screen with no autonomy literal on any path |
| 18–20 | Evidence-backed exclusion: schema, verification, routing as data, storage, verifier policy | S4a–e | No record leaves the abstract stage as OUT without criterion, offsets and a cross-family verdict; R1/R2 re-pinned |
| 21 | Screening benchmark | S4f | Per-review loss with its upper limit against the 1% bar, with the three comparators; adoption decision recorded. Tag `junior` |
| 22–24 | Runner, PRISMA from events, remaining app screens | S6 S8 S7 | A second, empty review carried from search to export by the runner and the app alone. Tag `senior` |

### Rules that hold across every session

1. One task per handoff; a two-phase task stops for a ruling between phases.
2. Root cause before fix; a reproducer before a repair; the reproducer becomes the test.
3. A test that pinned a defect is rewritten to the corrected behaviour, never deleted (B5).
4. Fingerprint at open and close; a migration session verifies its backup before it writes.
5. No engine code changes while a run is in progress; runs execute on a tagged state.
6. Every read-out and report is committed under `docs/session-reports/`; no result lives in a chat transcript.
7. Claims about the plan's own counts are re-measured before they are relied on; four were wrong in v49.

### The first brief

Session 1 (S0, `SAFE-GROUND-01`) closed 2026-09-20: commits 11dd9dd, fa32025, ed0d92a; gates G1–G6 met; live fingerprint unchanged. Lesson: `Connection.backup()` copies the WAL journal mode into the backup, so a backup litters sidecars on first read — the documented cause of open item #9; the destination is now switched to DELETE mode before close.

Session 2 (S1a/b/d, `INSTRUMENTS-01`) closed 2026-09-20: commits 4fb0a20, 62d3a0b; gates G1–G6 met; gate now 2,397 / 17; live fingerprint unchanged. Findings: four of the 63 published "kappas" were below −1 (−11.5, −3.4, −1.8, −1.5), which no Cohen's kappa can be — proof on the face of the artifact; the corrected free-text kappas are correct and uninformative (≈300 labels over 189 papers puts p\_e near 0, so κ ≈ p\_o), which means kappa is not the instrument for free-text fields and S5c's judge is; the absence-sentinel divergence moved zero rows (NA, NOT FOUND, none have no spans). Inventory rows added: I5 — analysis readers open the live database read-write (`concordance.py` ×2, `export_disagreement_pairs.py` ×1), closes in session 6; A10 — parse-artefact field names `field_1` (paper 719) and `Title` (paper 415) stored as spans, `field_1` carried into every published figure since March, removed under S5a plus a one-time cleanup in S2; C9 — two hardcoded dispatch lists in `normalize.py` (`_PASSTHROUGH_NUMERIC_FIELDS` on a free\_text field; `_MULTI_VALUE_FIELDS` with a dead `secondary_outcomes` branch), for S5b.

Session 3 (S3c, `MIGRATIONS-01`) closed 2026-09-21: commits a849660, 806cc5d, addendum f19e0bf; the first live write since 2026-09-11, bracketed by a verified backup and fingerprints before and after (overall f376562e…39e00 → a9926e62…1eafae; 25 tables; textual schema c286cde5…a790c5; structure 8625b7c5…625d37; record `docs/session-reports/migrations-01/review_db_fingerprint_20260921T002809Z.json`). Twelve migration files 002–013 (no 001) plus new 014 (cloud tables) and 015 (drop three duplicate adjudication indices, executed on live); `schema_migrations` receipt table with 14 rows; runner refuses on checksum drift and is idempotent; `user_version` deliberately left at 0. Gate 2,412 / 17 (516/607/398/475/416; wall 414.7 s, +21.9%; temp-DB construction 397.5 ms, +81%, because a fresh database now builds its whole schema for the first time). G3 (fresh structure hash == live) NOT MET for two pre-existing causes, both deferred with rows: A11 — `audit_adjudication.span_id` references the phantom `_evidence_spans_old`, so the human-audit import path is unwritable (session 4 input); C10 — `cloud_evidence_spans.confidence`/`.tier` NOT NULL on a fresh database but nullable on live because `init_cloud_tables`' rebuild branch never ran on live; 014's receipt note records the half-applied state; the addendum measured 0 NULLs in 7,257 rows and no reader of either column, so C10 is latent (session 5 migration batch). Two dated addenda committed: Part B D2-6 corrected (`_run_migrations` imports six numbered modules; the clause came from reading only the head of the function); Part A D1-1/D1-2 status upgraded from "prospective" to "path unwritable". Migration 003's literal recorded for S9: source directory and default target must derive from a passed review id. Migration 006's docstring asserts a constraint that is false on this database — a comment standing in for a check.

Post-wrap 2026-09-21: CLAUDE.md gained an "Ops Invariants — the database" block and a corrected Concordance section (docs-only commit; gate re-run 2,412 / 17; audit clean); the project primer was applied on disk (untracked by convention). CC session `ba16c4af` exited and archived.

Next: session 4 (S2 Phase 1, read-only), opened by a docs-only commit of this file at `docs/plan/ENGINE_REFACTOR_PLAN.md`, a CLAUDE.md pointer to it, and `docs/session-reports/architect/` — inputs A11 and C10; the PI approves the resolution rule before Phase 2. Expected values at open: HEAD `59dc247` clean and level (claude-config `41d9ba8`); gate 2,412 / 17 (516/607/398/475/416; deselects 0/0/10/6/1); fingerprint --compare exit 0 against the 2026-09-21 record (overall a9926e62…1eafae); 25 tables. Closure paragraphs for later sessions are appended here by Claude Code in each closeout.

## Decision log

Rulings made by the PI in the architect session of 2026-09-19 to 2026-09-21, in order. The architect's four provisional rulings of 2026-09-19 (recall-bounded filter with code-enforced basis; SYNERGY as reference; a tiered lane with 2d first; a 50 KB plan) were withdrawn after the external review and replaced by R1–R9 below.

| Date | Ruling | Made by |
| --- | --- | --- |
| 2026-09-19 | Step 1 scope approved as proposed: one read-only CC session, startup verify + fingerprint, D1 then D2 (brief DISCOVERY-01 issued) | PI |
| 2026-09-20 | Push 7e09de3 immediately; Part B proceeds with five added items (D2-12…D2-16) | Architect, within the approved scope |
| 2026-09-20 | All human decisions on disk were made by the PI without Claude; their quality is bounded by what the queue artifact presented (J5) | PI |
| 2026-09-20 | The engine must be operable without Claude; human-in-the-loop steps need an interface (class J added) | PI |
| 2026-09-20 | R1 — REJECT means withdraw: no value in the current result, original kept in history, correction is a separate act | PI |
| 2026-09-20 | R2 — A duplicated field is a failure with the missing-field retry budget, then goes to a person unresolved; unexpected fields are dropped and logged | PI |
| 2026-09-20 | R3 — A stale workbook is refused and the superseding extraction named; re-presentation belongs to the app | PI |
| 2026-09-20 | R4 — Eligibility and processing outcome are separate facts; corpus = eligible; exports use an analysis-ready subset and report failures with a reason | PI |
| 2026-09-20 | R5 — Evidence-backed exclusion with cross-family verification adopted as a pre-registered experiment; neither-in-nor-out records go to full text by default; adoption bar ≤1% of final inclusions lost per review with the 95% upper limit reported (Cochrane RCT Classifier standard, PMID 33171275; dual-human 97.5% as practice comparator, PMID 31972274); benchmark pools \~300 held-out final inclusions | PI, number from literature |
| 2026-09-20 | R6 — Cloud arms opt-in per run, declared in the spec, recorded in the manifest, off by default | PI |
| 2026-09-20 | R7 — Immutable history at paper and field level, a written resolution rule, one reader; built as event store + reader first, readers migrated one at a time | PI |
| 2026-09-20 | R8 — Operator app: local Gradio on the DGX, one screen per decision, who and when recorded; audit review first | PI |
| 2026-09-20 | R9 — Run 7 runs on a named, tagged engine state (first: *freshman*), defined as a closed set of units with everything else open under stated limits; set to be argued in Step 4 | PI |
| 2026-09-21 | R10 — Arms are declared per review in the Review Spec. Each arm has a name, a kind (model \| human extractor) and one pinned configuration; a changed model, prompt, codebook or options is a new arm, never a newer claim in the old one. Supersession within an arm happens only when that arm re-extracts a paper whose input identity changed (S3d). From session 7, a write whose configuration does not match its declared arm is refused. Existing arms (local, anthropic_sonnet_4_6, openai_o4_mini_high) are registered at migration with configuration "not recorded (pre-manifest)"; nothing is backfilled. Two claims on one cell that share a pre-manifest configuration do not supersede — the cell is unresolved (needs re-review) | PI |
| 2026-09-21 | R11 — Actor role is recorded on every event: reviewer \| extractor (model or human). Rule rows that let a human decision override a claim fire only on reviewer events. Human extractor arms are independent arms, treated exactly like model arms | PI |
| 2026-09-21 | R12 — Arms are data: an arm registry read by one reader with no per-arm code branches. The number and kind of arms vary by review (zero, one or several human extractor arms). Asking the PI or review architect which arms a review uses is an S7 review-setup requirement (recorded, not built); until then the spec declares them (block lands with S3a, session 7) | PI |
| 2026-09-21 | R13 — Partial coverage: an arm may be assigned a subset of papers. A cell outside an arm's assignment is "not assigned" — returned by the reader as out of scope, not an S5b state, excluded from every denominator. "Missing" means assigned and absent. Model arms default to the full corpus. The assignment table is built with human arm loading (session 12), as a separate table | PI |
| 2026-09-21 | R14 — Human extractor workbooks are loaded as arms in session 12 alongside the importer. `human_extractions` is created by a numbered migration with a receipt; the self-provisioning `CREATE TABLE IF NOT EXISTS` is retired; the TEXT "EE-NNN" key is reconciled to `papers.id` in that session's design | PI |
| 2026-09-21 | R15 — Q5: S4 screening evidence is a separate table keyed to the event, added with S4; no S4 columns are reserved on the event tables | PI |
| 2026-09-21 | R16 — Q3: `UNIQUE(paper_id, arm)` on `cloud_extractions` is dropped in session 6, in the same change that puts `load_arm` behind the reader | PI, on architect recommendation |
| 2026-09-21 | R17 — "Asserted with evidence" requires a citation-located event against the current claim. That event is written by one shared, deterministic locator (a single implementation of the auditor's normalize + exact/fuzzy match). Each located / not-located event records the parsed-text identity checked, the threshold, and the locator version. Without a located event, the state is "asserted without locatable evidence", and "snippet supplied: yes/no" is kept as provenance. The test is identical for values and sentinels, for every arm, and for human extractor source quotes. The elicited path writes the located event at extraction time. Legacy rows start with no located event. Upgrading them is a separate, dated locatability measurement after parsed-text identity is settled (S3e, session 8), recorded as new events, never as reconstructed history. Default at migration: no located events reconstructed from `audit_status` | PI, on architect recommendation |
| 2026-09-21 | R18 — Q1 refuse (two reviewer decisions on one claim → unresolved); Q2 keep row 3 with `value_unchanged` in provenance; Q4 arm is a required argument; Q6 reader ignores non-codebook fields now, deletion recorded as an event in session 12; Q7 auditor verdicts are provenance, not a field state, and flagged/contested spans count as work owed in derived stage completion; Q8 and Q9 recorded as state at migration; Q10 PI v2 audit is measurement only; A11 Option B (session 5 stops writing and marks deprecated, session 12 drops) | PI, on architect recommendation |
| 2026-09-21 | R19 — Operational invariant until S3d lands (session 8): no local extraction runs on corpus papers (the skip key cannot match; a run would re-extract all 190) | PI |
