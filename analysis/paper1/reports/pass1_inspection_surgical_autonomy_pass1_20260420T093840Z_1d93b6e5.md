# Pass 1 inspection — `surgical_autonomy_pass1_20260420T093840Z_1d93b6e5`

## Q7 · `field_1` root cause (headline)

1 row in `disagreement_pairs_3arm.csv` had `field_name = field_1` (paper_id=719, 'Bauzano 2010', tier=0, type=free_text). Classification: **local-arm extractor artifact — deepseek-r1:32b emitted a field with a synthetic name `field_1` containing a paper summary; cloud arms correctly returned empty.** The codebook has no `field_1` entry, so the loader correctly skipped this row with a WARNING. Not a real field; no re-export needed.

local_value preview: `"The paper presents a dynamic potential field method for robot path planning, which allows robots to navigate complex environments efficiently. Here's "`; o4mini_value: `''`; sonnet_value: `''`.

## Run metadata

- run_id: `surgical_autonomy_pass1_20260420T093840Z_1d93b6e5`
- triples analyzed: **2266**
- pair-ratings analyzed: **6798**
- source CSV: `data/surgical_autonomy/exports/disagreement_pairs_3arm.csv`
- codebook: `data/surgical_autonomy/extraction_codebook.yaml`
- codebook_sha256: `82e901509f8b3d73057983f39e85bcd7120f389cbd344b52cabf457f5031e9cb`

## Q1 · Per-field Level 1 cross-tab

| field_name                    |   EQUIVALENT |   PARTIAL |   DIVERGENT |   total |   pct_eq | flag    |
|:------------------------------|-------------:|----------:|------------:|--------:|---------:|:--------|
| key_limitation                |           28 |       415 |         124 |     567 |      4.9 | LOW_EQ  |
| secondary_outcomes            |           33 |       350 |         175 |     558 |      5.9 |         |
| primary_outcome_value         |           56 |       256 |         198 |     510 |     11.0 |         |
| task_performed                |           33 |       428 |          43 |     504 |      6.5 |         |
| primary_outcome_metric        |           37 |       235 |         196 |     468 |      7.9 |         |
| robot_platform                |           55 |       348 |          56 |     459 |     12.0 |         |
| system_maturity               |           85 |       142 |         193 |     420 |     20.2 |         |
| surgical_domain               |           89 |        92 |         221 |     402 |     22.1 |         |
| study_design                  |           72 |       110 |         175 |     357 |     20.2 |         |
| sample_size                   |           73 |        73 |         211 |     357 |     20.4 |         |
| autonomy_level                |           64 |       100 |         151 |     315 |     20.3 |         |
| comparison_to_human           |           59 |       138 |         118 |     315 |     18.7 |         |
| task_generate                 |           55 |        73 |         181 |     309 |     17.8 |         |
| validation_setting            |           68 |       166 |          75 |     309 |     22.0 |         |
| task_monitor                  |           59 |        43 |         147 |     249 |     23.7 |         |
| task_select                   |           40 |        38 |         117 |     195 |     20.5 |         |
| country                       |           85 |        74 |          33 |     192 |     44.3 | HIGH_EQ |
| clinical_readiness_assessment |           30 |        66 |          51 |     147 |     20.4 |         |
| task_execute                  |           41 |        20 |          86 |     147 |     27.9 |         |
| study_type                    |            6 |         2 |          10 |      18 |     33.3 | HIGH_EQ |

**Flags:** 1 field(s) with <5% EQ (strong disagreement); 2 field(s) with >30% EQ (trivially concordant — verify they're actually in scope for Paper 1 methods).

## Q2 · Per-field Level 2 cross-tab

| field_name                    |   GRANULARITY |   SELECTION |   OMISSION |   CONTRADICTION |   FABRICATION |   noneq_total |   pct_granularity | flag                  |
|:------------------------------|--------------:|------------:|-----------:|----------------:|--------------:|--------------:|------------------:|:----------------------|
| key_limitation                |           408 |          93 |         38 |               0 |             0 |           539 |              75.7 |                       |
| secondary_outcomes            |           316 |          78 |        122 |               4 |             5 |           525 |              60.2 |                       |
| task_performed                |           418 |          11 |         38 |               0 |             4 |           471 |              88.7 | GRANULARITY_SATURATED |
| primary_outcome_value         |           233 |          97 |         91 |              27 |             6 |           454 |              51.3 |                       |
| primary_outcome_metric        |           229 |         147 |         54 |               1 |             0 |           431 |              53.1 |                       |
| robot_platform                |           328 |          13 |         59 |               4 |             0 |           404 |              81.2 | GRANULARITY_SATURATED |
| system_maturity               |           140 |         150 |         37 |               6 |             2 |           335 |              41.8 |                       |
| surgical_domain               |            73 |         186 |         37 |              14 |             3 |           313 |              23.3 |                       |
| study_design                  |           105 |         143 |         36 |               0 |             1 |           285 |              36.8 |                       |
| sample_size                   |            53 |          14 |        147 |              54 |            16 |           284 |              18.7 |                       |
| comparison_to_human           |           111 |           8 |        112 |              21 |             4 |           256 |              43.4 |                       |
| task_generate                 |            63 |          73 |         40 |              77 |             1 |           254 |              24.8 |                       |
| autonomy_level                |           102 |          86 |         33 |              25 |             5 |           251 |              40.6 |                       |
| validation_setting            |           145 |          29 |         50 |              10 |             7 |           241 |              60.2 |                       |
| task_monitor                  |            33 |          79 |         49 |              28 |             1 |           190 |              17.4 |                       |
| task_select                   |            15 |          46 |         47 |              45 |             2 |           155 |               9.7 |                       |
| clinical_readiness_assessment |            59 |          15 |         36 |               7 |             0 |           117 |              50.4 |                       |
| country                       |            59 |           0 |         44 |               4 |             0 |           107 |              55.1 |                       |
| task_execute                  |            12 |          28 |         37 |              29 |             0 |           106 |              11.3 |                       |
| study_type                    |             2 |           8 |          2 |               0 |             0 |            12 |              16.7 |                       |

**GRANULARITY-saturated fields (>80% of non-EQ pairs):** `task_performed`, `robot_platform`. Degenerate-classification signal per §9.1 of the plan — these fields need Level 2 collapsed to binary (GRANULARITY vs not) for Paper 1 methods, since the judge is effectively only emitting one label for them.

## Q3 · field_type × Level 2

| field_type   |   GRANULARITY |   SELECTION |   OMISSION |   CONTRADICTION |   FABRICATION |   noneq_total |   pct_granularity |
|:-------------|--------------:|------------:|-----------:|----------------:|--------------:|--------------:|------------------:|
| free_text    |          2102 |         447 |        558 |              61 |            19 |          3187 |              66.0 |
| categorical  |           749 |         843 |        404 |             241 |            22 |          2259 |              33.2 |
| numeric      |            53 |          14 |        147 |              54 |            16 |           284 |              18.7 |

GRANULARITY peaks on `free_text` and is lowest on `numeric`. If free-text carries the bulk of GRANULARITY labels, the collapsed binary recommendation from §9.1 applies specifically to the free-text subset, not uniformly.

## Q4 · Per-arm-pair × Level 2

| arm_pair                                   |   GRANULARITY |   SELECTION |   OMISSION |   CONTRADICTION |   FABRICATION |   noneq_total |
|:-------------------------------------------|--------------:|------------:|-----------:|----------------:|--------------:|--------------:|
| anthropic_sonnet_4_6 × local               |          1139 |         515 |        187 |             142 |            26 |          2009 |
| local × openai_o4_mini_high                |           774 |         494 |        460 |             133 |            14 |          1875 |
| anthropic_sonnet_4_6 × openai_o4_mini_high |           991 |         295 |        462 |              81 |            17 |          1846 |

**FABRICATION skew:** the `anthropic_sonnet_4_6 × local` pair carries the largest share of FABRICATION labels, consistent with the §10 observation. This is a pair-level disagreement count, not per-arm attribution (see Q5).

## Q5 · Fabricator-arm attribution audit

- Pass1Output contains a per-arm attribution field: **False**

Pass 1 emits a triple-level fabrication_risk label (low/medium/high) and a proposed_consensus string. Neither carries per-arm attribution; FABRICATION counts in Q4 are pair-level (arm_a vs arm_b disagreement type) and do not identify which of the two arms is fabricating. Per-arm fabricator identity is a Pass 2 responsibility: Pass 2 re-grounds each arm's (value, span) against the source paper and labels each arm as grounded / ungrounded / absent. This is the intended design.

## Q6 · B-filter simulation (Pass 2 sizing)

- Total triples: **2266**
- Medium or high fabrication_risk: **1212**
- **Kept after B-filter: 1212**
  - high (auto-keep): 14
  - medium (surviving the flag test): 1198

Kept by field_type:
- categorical: 582
- free_text: 557
- numeric: 73

Kept by arm-pair (every kept triple runs all three pairs):
- anthropic_sonnet_4_6 × local: 1212
- anthropic_sonnet_4_6 × openai_o4_mini_high: 1212
- local × openai_o4_mini_high: 1212

Pass 2 wall-clock projection (single-stream):
- at 45 s/triple: **15.15 h** (54540 s)
- at 60 s/triple: **20.2 h** (72720 s)

### B-filter discriminative power

| fab_risk | n | tripped | % tripped |
|---|---:|---:|---:|
| low | 1054 | 1024 | 97.2% |
| medium | 1198 | 1198 | 100.0% |
| high | 14 | 14 | 100.0% |

**Filter is non-discriminative.** Low-risk triples trip the precheck flag at 97.2% vs 100.0% for medium. Because the test is per-triple (any arm failing across all three), even a single noisy arm saturates the OR. The B-filter as currently defined does not meaningfully cull the medium bucket — Pass 2 scope reduces to **"all medium + high triples" = 1212**. Consider moving the precheck cull to a per-arm step inside Pass 2 (where we already re-ground each arm against the source) instead of as an up-front triple filter.

