# TAXONOMY-CENSUS-03 — Numeric-restatement inspection (final pre-freeze amendment decision)

Project: evidence-engine · Machine: DGX (spark-59e4) · Date: 2026-07-27
**Read-only inspection. No reclassification, no migration, no commit.**

**Ollama call count: 0.** All work was read-only SQLite against
`provcensus_surgical_autonomy_20260727T194748Z` and Run 6 source tables, plus pure-Python
string search over the parsed text.

**Sample:** seed `20260727`, 15 spans per field, local arm, drawn from residual
`UNTRACEABLE_NO_BASIS` (pools: `primary_outcome_value` 44, `secondary_outcomes` 40,
`country` 50). Span IDs are `provenance_classifications.id`.

**One incidental write to disclose:** while checking the validator I instantiated
`ReviewDatabase("data/surgical_autonomy/review.db")` — the constructor takes a *review name*,
not a path, and `engine/core/database.py:278-282` unconditionally `mkdir(parents=True)`s
whatever it is given. That re-created child directories under the pre-existing stray tree
`data/data/surgical_autonomy/review.db/` (originally created 2026-03-06 by the same mistake).
Nothing was written to the real review DB, and the path is inside gitignored `data/`. It is
also a finding in its own right — see §4.3.

---

## 1. Classification table

Classification key, applied consistently: **(a)** the value is locatable in the paper
(spacing-tolerant; codebook-sanctioned normalization such as `USA`→`United States` allowed)
**and** the snippet is model-authored prose conveying that value; **(b)** the value is not
locatable, or is locatable only in a clearly different role (a design requirement rather than
a result); **(c)** neither cleanly.

### 1a. `primary_outcome_value` (15)

| # | span_id | paper | value | snippet (abbrev.) | where the value is in the paper | class |
|---|---:|---:|---|---|---|---|
| 1 | 17201 | 549 | `mean 0.58 m vs 1.16 m` | "The path length of the remote controller is reduced from 1.16 m to 0.58 m." | "path length of the remote controller" present; **0.58 / 1.16 absent even spacing-tolerant** | **(b)** |
| 2 | 13934 | 434 | `2.74 ± 0.99 mm` | "The average targeting accuracy was 2.74 ± 0.99 mm." | title-block/abstract: "the average positional accuracy of the system was 2.74 ± 0.99 mm" | **(a)** |
| 3 | 11812 | 9 | `0.17 ± 0.44` | "STAR's hesitancy events." | §Phantom end-to-end anastomosis: "average suture hesitancy per stitch of 0.17 ± 0.44" | (c) — value real, snippet carries no value |
| 4 | 18644 | 572 | `99.8% ± 0.04%` | "The success rate is 99.8 ± 0.04%." | §A. Results of the Artificial Team: "average success rate of 99 . 8 ± 0 . 04%" (parser digit-spacing) | **(a)** |
| 5 | 21388 | 764 | `78.87%` | "The average visibility rate is 78.87%." | abstract: "the system achieved a 78 . 87% visibility rate" | **(a)** |
| 6 | 14837 | 470 | `106.57 ± 50.84 s vs 111.30 ± 45.00 s` | "Task completion time (T (s)) \| Manual: 106.57 ± 50.84 \| Semi-Autonomous: 111.30 ± 45.00" | §E. Results Analysis, **TABLE** row `t (s)` | **(a)** table verbalized |
| 7 | 20368 | 700 | `0.49 mm` | "YOLO11 model achieved the lowest RMSE of 0.49 mm." | §C. Procedure Performance, **TABLE** row `yolo11l-pose` | **(a)** table verbalized |
| 8 | 12232 | 11 | `Mean error <1mm` | "Tracking error was below 1mm." | "1 mm" occurs only as a **design requirement** ("position error smaller than 1 mm"); no such result | **(b)** |
| 9 | 17261 | 553 | `10 pixels` | "The tracking error is reduced to 10 pixels after 11 converging phases." | "the acceptable error value is 10 pixels" — an **acceptance threshold**, not a reported result | (c) |
| 10 | 11213 | 14 | `Mean 16.2 seconds` | "Mean total time using proposed system: 16.2 seconds." | §TIME RESULT OF SINGLE THROW SUTURING TASK, **TABLE** `mean` row = 16.2 | **(a)** table verbalized |
| 11 | 20788 | 607 | `1.80 × 10⁻³ m, 1.08 × 10⁻⁴ m, 1.90 × 10⁻³ m` | "Results from experiments in simulated environments." | abstract: "maximum tracking errors of 1.80 × 10 -3 m, 1.08 × 10 -4 m, and 1.90 × 10 -3 m" | (c) — value real, snippet carries no value |
| 12 | 14777 | 472 | `81.25%` | "Mean success rate: 81.25%" | §B. Autonomous Needle Manipulation Task, **TABLE** `mean \| 81.25%` | **(a)** table verbalized |
| 13 | 12112 | 102 | `Median 3.3 mm` | "Median MSE is 3.3 mm at 2 N preloading." | abstract: "median lesion localization error is 3.3 mm … preloading of 2 n" | **(a)** (metric renamed) |
| 14 | 14395 | 455 | `Reduced forces through the PWE strategy` | "The PWE strategy reduces the forces applied to the abdomen." | body: "minimizing the forces exerted over the abdominal wall" | **(a)** — non-numeric |
| 15 | 11872 | 295 | `1.46 ± 1.09 mm` | "1.46 ± 1.09 mm" | §B. Results, **TABLE** `cuts \| average \| 1.46 ± 1 . 09` (parser digit-spacing) | **(a)** table verbalized |

**(a) 10 · (b) 2 · (c) 3**

### 1b. `secondary_outcomes` (15)

| # | span_id | paper | value | snippet (abbrev.) | where the value is in the paper | class |
|---|---:|---:|---|---|---|---|
| 16 | 12815 | 296 | force dispersion 0.78 ± 0.57 N vs 1.15 ± 0.97 N | "The test group had force dispersion of 0.78 ± 0.57 N compared to the control group's 1.15 ± 0.97 N." | abstract: "force dispersion (0.78 ± 0.57 n vs. 1.15 ± 0.97 n)" | **(a)** |
| 17 | 14157 | 378 | exec time 34 ± 13 / 33 ± 12 min; SUS 74% | "The average total execution time was 34 ± 13 min for manual and 33 ± 12 min for autonomous. The SUS score was 74%." | body: "average t_tot_m 34 ± 13min vs. average t_tot_a 33 ± 12min"; "an average of 74% in the system usability" | **(a)** |
| 18 | 13936 | 434 | `Cutting depth accuracy: 2.44 ± 0.34 mm` | "The average cutting depth was 2.44 ± 0.34 mm." | "average depth of 2.44 mm and standard deviation of 0.34 mm" — recomposed into ± form | **(a)** |
| 19 | 12114 | 102 | probe displacement 20 mm; lesion radius 3.6 mm | "Additional metrics include probe displacement and lesion radius." | §C. Results: "median lesion radius (3.6 mm)" | (c) — pointer snippet |
| 20 | 18646 | 572 | time 5.3s ± 0.2s; path 268 ± 13 mm; collisions 0.01 ± 0.18 | "Additional metrics include time, path length, and collisions." | §A: "5 . 3 ± 0 . 2 s"; "path length reaches an average of 268 ± 13 mm" | (c) — pointer snippet |
| 21 | 14839 | 470 | clutching 15.9 ± 6.8 vs 8.0 ± 6.4; trajectory 4.72 ± 1.57 vs 2.37 ± 1.50 m | "M (m) \| Manual: 4.72 ± 1.57 \| Semi-Autonomous: 2.37 ± 1.50 \| p-value: 0.0003" | §E. Results Analysis **TABLE** rows `m (m)` and `c` | **(a)** table verbalized |
| 22 | 13816 | 432 | insertion depth 125.23 mmrms; localization error 2.65 mmrms | "Additional metrics include insertion depth and localization error." | §Accuracy for Stationary Targets: "average target depth was 125.23 mmrms"; §Accuracy of Feature Localization: "2.65 … mmrms" | (c) — pointer snippet |
| 23 | 12875 | 380 | suction accuracy 2.75 ± 1.45 mm; max force 2.66 ± 0.61 N | "The suction system motion accuracy was 2.75 ± 1.45 mm on sample surface plane guided by NIR markers…" | abstract: "2 . 75 ± 1 . 45 mm on sample surface plane guided by nir markers" (parser digit-spacing) | **(a)** |
| 24 | 14457 | 460 | fragment FN 1.9%; grasp multiple 5.2% / 7.1% | "Fragment false negative (%): 1.9 (single arm); 0.0 (two arm)" | §C. Multilateral Coordination **TABLE** rows | **(a)** table verbalized |
| 25 | 13216 | 405 | `NR` | "The paper focuses on the primary outcome metric." | n/a | (c) — absence-adjacent rationale (a known §A5 detector false negative) |
| 26 | 20370 | 700 | distance 56.1 mm; duration 59 s | "Performance metrics include distance traveled and duration." | §C. Procedure Performance **TABLE** (`distance (mm)`, `duration (s)`) | (c) — pointer snippet |
| 27 | 15300 | 485 | SDs of master manipulator positions; task completion times | "The standard deviations of the master manipulator positions are given in Table III." | §VI. EXPERIMENTS: "the compared standard deviations of the master manipulator positions … are given in …" | **(a)** — non-numeric |
| 28 | 18365 | 570 | translation 0.711 ± 0.315 mm; rotation 1.383 ± 0.711° | "Table III and Figure 4 show additional outcomes." | §VISUAL SERVOING ERROR **TABLE** `total \| 0.711 ± 0.315 \| 1.383 ± 0.711` | (c) — pointer snippet |
| 29 | 11694 | 39 | accuracy 0.12 mm; precision 0.15 mm | "Accuracy absolute positioning error [mm]: Median 0.12, IQR 0.15, Range 0.47." | §FIGURE 5 **TABLE** row `autonomous \| 0.12 \| 0.15 \| 0.47` | **(a)** table verbalized |
| 30 | 14498 | 402 | RMSE 0.280 N (trigger); 0.268 N (pinch) | "…the RMSE of the desired and actual pinch forces being 0.158 N and 0.164 N for γ = 0…" | §3 RESULTS / abstract contain 0.280 and 0.268 | (c) — snippet quotes *different* numbers than the value |

**(a) 8 · (b) 0 · (c) 7**

### 1c. `country` (15)

Every span in this field has the same shape: the value is a country, and the snippet is a
model-authored summary of the affiliation block. The codebook explicitly sanctions deriving
country from institution (`extraction_codebook.yaml`, `country`: "Use first author's
institution if not stated"), so a value backed by an in-paper affiliation counts as located.

| # | span_id | paper | value | snippet (abbrev.) | where the value is in the paper | class |
|---|---:|---:|---|---|---|---|
| 31 | 21506 | 801 | United States | "…Massachusetts General Hospital and MIT Lincoln Laboratory, Boston, MA, USA." | funding/affiliation block: "massachusetts general hospital…"; "…fl, usa" | **(a)** |
| 32 | 15176 | 478 | Canada | "…University of British Columbia, Vancouver, BC, Canada." | §I. INTRODUCTION funding note: "research council of canada" | **(a)** |
| 33 | 15116 | 486 | Hungary | "…Óbuda University, Bécsi út 96/b, 1034, Budapest, Hungary" | author block: "obuda university, becsi ut 96/b … budapest, hungary" | **(a)** |
| 34 | 17300 | 550 | United States; Canada; Switzerland; Germany | "Authors are from University of Toronto, University of Bern, UC Berkeley, NVIDIA, and Georgia Institute of Technology." | author block lists all five institutions verbatim; **country names themselves absent** — codebook-sanctioned inference | **(a)** |
| 35 | 14554 | 433 | USA; Hong Kong | "…Johns Hopkins University and the Chinese University of Hong Kong." | affiliation: "chinese university of hong kong, shatin, n.t., hong kong; and 3 johns hopkins … md, usa" | **(a)** |
| 36 | 19943 | 660 | United Kingdom | "…institutions in London, United Kingdom." | affiliation: "queen mary university of london, london, united kingdom" | **(a)** |
| 37 | 14093 | 442 | Canada | "…University of Alberta, Canada." | affiliation: "university of alberta, edmonton, ab t6g 1h9, canada" | **(a)** |
| 38 | 17079 | 541 | United States | "…University of Illinois Chicago, USA." | affiliation: "university of illinois chicago, chicago, il 60607, usa" | **(a)** |
| 39 | 14153 | 378 | Italy | "The study was conducted at the Leonardo Robotics Laboratory of Politecnico di Milano." | affiliation: "politecnico di milano, milan, italy" | **(a)** |
| 40 | 18361 | 570 | United Kingdom | "…Hamlyn Centre for Robotic Surgery, Imperial College London, UK." | "the hamlyn centre for robotic surgery, imperial college london"; "@imperial.ac.uk" | **(a)** |
| 41 | 12350 | 323 | United Kingdom | "…institutions in London, UK." | affiliation: "weiss, ucl, london, uk" | **(a)** |
| 42 | 12530 | 277 | Hong Kong | "…Chinese University of Hong Kong." | §INTRODUCTION: "chinese university of hong kong, hksar, china" | **(a)** |
| 43 | 12110 | 102 | Italy | "Authors are from the University of Verona, Italy." | §I. INTRODUCTION: "university of verona, 37134 verona, italy" | **(a)** |
| 44 | 12811 | 296 | U.K.; China | "…King's College London, U.K., and Beijing Institute of Technology, China." | affiliation: "…u.k., also with the school of mechatronical engineering, beijing institute of technology, china" | **(a)** |
| 45 | 12691 | 392 | United States | "The authors' affiliations are from the United States." | §INTRODUCTION: "the association of military surgeons of the united states 2021" | **(a)** |

**(a) 15 · (b) 0 · (c) 0**

### 1d. Tally

| field | (a) | (b) | (c) | n | (a) % |
|---|---:|---:|---:|---:|---:|
| primary_outcome_value | 10 | 2 | 3 | 15 | 66.7% |
| secondary_outcomes | 8 | 0 | 7 | 15 | 53.3% |
| country | 15 | 0 | 0 | 15 | 100.0% |
| **overall** | **33** | **2** | **10** | **45** | **73.3%** |

---

## 2. Recommendation under the pinned rule

> **Pinned rule:** ≥60% (a) → recommend NUMERIC_RESTATEMENT as amendment v1.2.
> 40–60% → report composition and hold. <40% → freeze at v1.1.

**33/45 = 73.3% (a) ≥ 60% → the rule fires: recommend an amendment.**

I apply the rule verbatim as pre-registered. Two qualifications the architect must weigh
before acting on it, both of which follow from the sample rather than from re-litigating the
rule:

1. **The category the data supports is not "numeric".** Of the 33 (a)s, only **18** are on the
   two numeric fields, and 2 of those 18 are themselves non-numeric restatements (#14 "Reduced
   forces through the PWE strategy", #27 "standard deviations … given in Table III"). The
   other **15 are `country`** — affiliation restatement, with no number involved anywhere.
   Naming the class `NUMERIC_RESTATEMENT` would misdescribe 45% of its own members.
   **Recommend `VALUE_RESTATEMENT` if the amendment proceeds.**
2. **The rule's own arithmetic lands in the hold band under a strictly numeric reading.**
   Excluding `country`: 18/30 = **60.0%**, exactly on the boundary. Excluding `country` *and*
   the two non-numeric restatements: 16/30 = **53.3%**, which is squarely in the
   **40–60% hold** band. So: *if the architect intends specifically a numeric category, the
   pre-registered rule says HOLD, not proceed.* The ≥60% verdict is entirely carried by
   `country`.

**Net recommendation:** proceed with a v1.2 amendment named `VALUE_RESTATEMENT`, or hold —
but do **not** proceed with a category named `NUMERIC_RESTATEMENT`, because the sample that
authorizes the amendment is not numeric.

### 2a. A rival explanation that was tested and rejected

Before recommending a new class I tested whether the residual is a **normalization gap**
rather than a behaviour. Several sampled values were locatable only with spacing tolerance —
the parser renders `99.8 ± 0.04%` as `99 . 8 ± 0 . 04%` — which raised the possibility that a
tenth normalization transform (collapse whitespace around a decimal point between digits)
would dissolve the whole population without any new class.

Measured directly on **all 134** local residual no-basis spans across the three fields, with
that candidate transform applied to both paper and snippet:

| outcome | n | % |
|---|---:|---:|
| still `UNTRACEABLE_NO_BASIS` | 129 | 96.3% |
| → `ANCHORED` | 2 | 1.5% |
| → `DRIFTED` | 3 | 2.2% |

**Only 3.7% is a normalization artifact.** The rival explanation is rejected: the values are
frequently mis-spaced, but the *snippets* are genuinely model-authored prose, which is why
fixing the spacing does not rescue them. (The transform is still worth ~5 spans corpus-wide
and could be folded into a future `prov-norm-2` on its own merits — it is not a reason to
skip the amendment.)

---

## 3. Secondary observation — openai arm (no action)

The openai arm has only 14 residual no-basis spans across these three fields (vs 134 local).
I inspected 10. **The pattern is visibly different and does not support extending the category
on this arm's evidence.**

- **Its snippets are quotation attempts, not restatements.** #6 (paper 462): "From Fig.
  10(a)-(d), the medians of d_si, e_si, a_si and d_s were 2.44 mm, 0.26 mm, 90.65° and 9.71
  mm, respectively." — sentence-shaped, all four numbers locatable, and it reads as a failed
  near-verbatim quote rather than authored prose. #9 (paper 562, `country`) is essentially the
  affiliation line verbatim: "* Antal Bejczy Center for Intelligent Robotics, Óbuda
  University, Bécsi út 96/B, Budapest 1034, Hungary".
- **Where it fails, it more often fails on locatability.** #2/#7 (paper 82): the values
  92.93 ± 1.53 / 96.86 ± 3.63 / 92.21 ± 3.02 are not findable in the paper at all — a (b), not
  an (a). #5 (paper 515): the p-values in the snippet do not match the paper.
- **No pointer-snippet family.** The "Additional metrics include X and Y" shape that accounts
  for 7 of the 15 local `secondary_outcomes` spans does not appear in the openai sample.

Read conservatively: restatement as a *characteristic behaviour* is local's. Any amendment
justified on this evidence describes one arm, and the manuscript should say so.

---

## 4. Validator gap diagnosis

### 4.1 What the two spans are

| paper | arm | span row | field_name | value (abbrev.) |
|---:|---|---:|---|---|
| 415 | local_deepseek_r1_32b | `evidence_spans.id=960` | `Title` | "Enhancing Left Ventricle Segmentation in Echocardiograms Using Anatomically Constrained CycleGAN and U-Net" |
| 719 | local_deepseek_r1_32b | `evidence_spans.id=3380` | `field_1` | "The paper presents a dynamic potential field method for robot path planning… Here's a structured summary of the key points:" |

Both are local-arm. Neither is a stray field inside an otherwise good extraction: **both
papers have exactly one span in total.** Span-count distribution across the 190 extracted
papers is 20 spans ×186, 19 ×2, **1 ×2**. These are *collapsed* Pass 2 outputs — the model
returned a single summary record instead of the 20 required — and the `field_1` value is
visibly a chat-style preamble ("Here's a structured summary of the key points:").

### 4.2 Where it should have been caught — three boundaries, none of them closed

1. **The check exists and works, but is never invoked by the pipeline.**
   `engine/validators/extraction_validator.py:259-267` implements exactly this test inside
   `validate_extraction()` (`:232`). Run manually it flags both papers correctly:
   `{'paper_id': 415, 'field_name': 'Title', 'issue': 'unknown field name'}`. But the module
   docstring (`:1`) declares it a "read-only diagnostic tool", and `grep` finds no caller
   outside its own `validate_all` (`:331`) and the test file — nothing in
   `engine/agents/extractor.py`, the pipeline runner, or the adjudication flow calls it.
   **This is the primary gap: detection without enforcement.**
2. **The write boundary performs no schema check.** `engine/core/database.py:805-815`
   (`store_extraction`) inserts `s["field_name"]` straight into `evidence_spans`; the only
   constraint is `field_name TEXT NOT NULL` (`engine/core/database.py:143`). Anything the
   model emits is persisted. This is where a gate would be cheapest and hardest to bypass.
3. **The type permits it.** `engine/agents/models.py:11` declares `field_name: str`, not a
   `Literal`/enum over the codebook fields. Because Pass 2 is grammar-constrained via
   `ExtractionOutput.model_json_schema()` (`engine/agents/extractor.py:258`), constraining
   this one annotation would make the malformed name **unrepresentable at generation time** —
   the same technique already used for the Pass 2 judge schema (`arm_slot: Literal[1,2,3]`).

A fourth, related gap: nothing enforces the cardinality the prompt demands
("You MUST emit exactly one entry per field listed above (20 fields total)",
`engine/agents/extractor.py:177`). The two 1-span papers would have been caught by a count
check even without a field-name check.

### 4.3 Incidental finding — `ReviewDatabase` silently creates trees from bad arguments

`engine/core/database.py:277-285` takes a `review_name` and calls
`root.mkdir(parents=True, exist_ok=True)` before connecting. Passing a *path* by mistake
therefore creates a plausible-looking but empty review tree (`data/data/surgical_autonomy/
review.db/{pdfs,parsed_text,vector_store}` plus an empty `review.db`) and returns a working
handle against the wrong database — which reports zero rows rather than failing. The stray
tree on this box dates from 2026-03-06, so this has already happened at least once before
today. Worth a guard; out of scope here.

---

## 5. Arguments against `NUMERIC_RESTATEMENT` as a category

**5.1 The name is wrong for its own evidence.** 15 of 33 qualifying spans are `country`
affiliation restatements with no number in them, and 2 more are non-numeric prose. A category
whose modal member is "Authors are affiliated with the University of Alberta, Canada." should
not be called numeric. Under a strictly numeric reading the pinned rule returns **hold**
(53.3%), so the amendment as literally titled is not authorized by its own decision rule.

**5.2 The class is heterogeneous in the way that matters.** Within the 33 (a)s there are at
least four mechanisms doing different work: table-cell verbalization (#6, 7, 10, 12, 15, 21,
24, 29 — 8 spans), prose recomposition into a different notation (#18: two separately reported
numbers merged into `2.44 ± 0.34`), metric renaming (#13: "lesion localization error" →
"MSE"), and affiliation summary (all 15 `country`). Collapsing these into one class buys a
cleaner headline at the cost of the same conflation the v1.1 amendment was created to undo.
If the argument for splitting absence claims out was that different defects deserve different
labels, that argument applies here too — and it argues for *either* several narrow classes
*or* none, not one broad one.

**5.3 It would relabel a real evidence failure as a benign one.** A restatement is still not a
quotation, and in three sampled cases the restatement is *wrong about the paper* while the
value is right: #13 renames the metric, #24's snippet assigns "1.9 (single arm); 0.0 (two arm)"
where the table reads `0.0 | 1.9 | 0.0` across three columns, and #30's snippet quotes
0.158/0.164 N when the value is 0.280/0.268 N. Moving these out of no-basis makes the audit
trail *look* better while the underlying evidence remains untraceable and, in these cases,
misdescribed.

**5.4 (c) is not a residual — it is a third of the sample and has its own shape.** 10 of 45
spans are (c), and 7 of those are one recognizable family: **pointer snippets** — "Additional
metrics include probe displacement and lesion radius.", "Performance metrics include distance
traveled and duration.", "Table III and Figure 4 show additional outcomes." The value is real
and locatable, but the snippet names the *topic* instead of restating the *value*. These are
not restatements and would not enter the proposed class, so the amendment would leave a
comparably sized, equally coherent population sitting in no-basis. Any freeze decision should
be made knowing the taxonomy would still be conflating that family with genuine fabrication.

**5.5 One arm, one corpus, and the pre-registered rule was applied to a hand-picked stratum.**
The three fields were selected *because* CENSUS-02 flagged them as anomalous, so the 73.3%
figure describes the worst-behaving stratum of one arm, not the corpus. It cannot be read as a
prevalence estimate. Local's overall no-basis population is 714 spans; this sample speaks for
the 134 on these three fields.

**5.6 The freeze itself has value.** This is the last candidate amendment before the taxonomy
locks, and v1.1 already carries one carve-out justified by a mechanism. A second carve-out
justified by a 45-span sample from one arm, whose name does not match its contents and whose
authorization depends on including a field nobody would call numeric, is a weaker case than
the first. **Freezing at v1.1 and documenting restatement as a known limitation of
`UNTRACEABLE_NO_BASIS` — with the numbers in §1 as the citation — is a defensible outcome
that the pinned rule permits the architect to choose on the grounds in 5.1.**

---

## Acceptance gates

| gate | status |
|---|---|
| 1. 45-span table with paper-location evidence for every (a) | ✅ §1a–1c; all 33 (a)s carry a section/table reference from the parsed text |
| 2. Pinned rule applied verbatim; recommendation stated | ✅ §2 — 73.3% ≥ 60% → amendment recommended, with the numeric-reading sensitivity (60.0% / 53.3%) stated rather than substituted |
| 3. Validator gap located with file/line | ✅ §4.2 — `extraction_validator.py:232,259-267` (detects, never invoked), `database.py:805-815` + `:143` (write boundary, no check), `models.py:11` (unconstrained type), `extractor.py:177` (uncheck­ed cardinality) |
| 4. Zero Ollama; zero writes beyond the scratchpad | ✅ zero Ollama; one incidental `mkdir` under gitignored `data/data/…` disclosed in the header and §4.3; no writes to `review.db` or the repo |

**Out of scope and not done:** implementing any category, guard implementation, other
amendment candidates, judge restatement, Arm P.

---

## Addendum (2026-08-30): parse-defective source papers, per PARSE-01

Task PARSE-01 swept all 190 EXTRACTED corpus papers for parse defects and input-limit
saturation and classified **four** as severely defective at source: **415** (a 728-page
conference proceedings volume acquired in place of one article), **719** and **586**
(font-glyph-encoded PDF text) and **455** (character-shattered extraction, one character per
line). Spans derived from these papers are present in the provenance census this report draws
on.

**Exposure is small and the headline distributions are unaffected.** The four contribute
**364 of 22,034** census classification rows — **1.65%**. Papers 415 and 719 carry only **one**
local evidence span each, so their weight in any local-arm rate is negligible. No figure in this
report is re-scored, and no claim it made is withdrawn; this is a pointer so that a reader
auditing individual spans knows which source documents are unsound.

**See:** `docs/session-reports/PARSE-01_report.md`. Appended by task PARSE-01; all text above
this heading is unchanged.
