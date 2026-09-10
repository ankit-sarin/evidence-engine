# SCHEMA-DERIVE-01 Phase 2a — `spec.description` vs the codebook, field by field

**Generated, read-only. No recommendation is made here and no wording is proposed.** This document exists so the architect and PI can decide, field by field, whether any content in the Review Spec's `extraction_schema.fields[].description` is genuinely absent from the codebook — and therefore whether anything must be folded into the codebook before the spec section is removed (SCHEMA-DERIVE-01 R2).

Source: `review_specs/surgical_autonomy.yaml` and `data/surgical_autonomy/extraction_codebook.yaml` as of codebook_sha256 `3807335b31852c03e2141acf217553cfa96ce44ed68e440132d98d921f4e6b5f`, spec extraction_hash `fc40fe1340fdc49256efac9bebf23b21aa98692495cd9d599c8dd799e40b8307`. Both files are read, never written.

## 🔴 The heuristic is not a finding

Every "unmatched" block below is the output of a **string-similarity check**: each spec sentence is compared against every piece of that field's codebook text (definition, instruction, decision_criteria, each valid_value and its definition, each example), and reported when nothing reaches 0.85 similarity or contains it literally.

It cannot distinguish **paraphrase** from **omission**. A spec sentence rewritten in the codebook with different words is reported as unmatched; a sentence whose meaning is genuinely missing looks identical to it. Sentence splitting is punctuation-based, so the `H = … R = … Shared = …` legends and the Yang level enumeration are split at full stops that do not correspond to units of meaning. **Read the verbatim text on both sides; the heuristic only says where to look first.**

## Summary

| field | spec chars | codebook chars | unmatched spec sentences | unmatched codebook units |
|---|---:|---:|---:|---:|
| `study_type` | 129 | 1754 | 1 | 27 |
| `robot_platform` | 387 | 387 | 0 | 0 |
| `task_performed` | 288 | 288 | 0 | 0 |
| `sample_size` | 235 | 362 | 0 | 3 |
| `surgical_domain` | 342 | 1659 | 0 | 23 |
| `autonomy_level` | 799 | 2782 | 8 | 39 |
| `validation_setting` | 213 | 1524 | 2 | 26 |
| `task_monitor` | 325 | 737 | 3 | 9 |
| `task_generate` | 332 | 766 | 4 | 9 |
| `task_select` | 327 | 751 | 4 | 10 |
| `task_execute` | 291 | 624 | 3 | 7 |
| `system_maturity` | 71 | 1882 | 0 | 27 |
| `study_design` | 262 | 2637 | 1 | 34 |
| `country` | 213 | 213 | 0 | 0 |
| `primary_outcome_metric` | 332 | 378 | 1 | 1 |
| `primary_outcome_value` | 231 | 231 | 0 | 0 |
| `comparison_to_human` | 250 | 323 | 0 | 1 |
| `secondary_outcomes` | 165 | 241 | 0 | 1 |
| `key_limitation` | 231 | 409 | 4 | 4 |
| `clinical_readiness_assessment` | 289 | 2500 | 2 | 37 |
| **total** | **5712** | **20448** | **33** | **258** |

The six fields whose spec text is longer than the codebook's `definition + instruction` — measured in Phase 1 — are `autonomy_level`, `task_generate`, `task_select`, `task_monitor`, `task_execute` and `validation_setting`. Note that the codebook-chars column here counts **all** codebook text for the field including valid_value definitions, which is where the per-value rubric lives; that is why several of those six no longer look shorter.

---

## study_type

`type: categorical` · `tier: 1` · `field_class: stated` · `judge_rubric_family: nominal_categorical`

### Spec — `extraction_schema.fields[].description`

**description**

```text
The type of study as described in the methods section. Look for explicit statements like 'prospective study,' 'case series,' etc.
```

### Codebook

**definition**

```text
The type of study as described in the methods section.
```

**instruction**

```text
Look for explicit statements like "prospective study," "case series," "systematic review," etc. If the paper does not explicitly state the study type, infer from the methods section structure.
```

**decision_criteria**

```text
1. Does the paper report original experimental data or results? → "Original Research"
2. Does it follow PRISMA or systematic search methods? → "Systematic Review"
3. Does it pool quantitative results across studies? → "Meta-Analysis"
4. Does it summarize literature without systematic methodology? → "Review"
5. Is it a brief conference proceeding? → "Conference Abstract"
6. Does it describe a system in detail without evaluation? → "Technical Report"
7. Does it report 1–3 clinical cases narratively? → "Case Report/Series"
8. None of the above → "Other"
```

**valid_values**

| value | definition |
|---|---|
| `Original Research` | Primary research reporting new data — experiments, trials, technical evaluations, algorithm development with results. |
| `Case Report/Series` | Report of one or a small number of clinical cases, typically without a formal experimental design. |
| `Review` | Narrative or scoping review summarizing existing literature without systematic search methodology. |
| `Systematic Review` | Review following a pre-specified protocol with systematic search, screening, and synthesis (PRISMA or equivalent). |
| `Meta-Analysis` | Quantitative synthesis of results from multiple studies using statistical pooling methods. |
| `Conference Abstract` | Short-form publication from a conference proceeding, typically under 500 words without full methods. |
| `Technical Report` | Detailed technical description of a system, algorithm, or method, often without clinical or experimental evaluation. |
| `Other` | Editorials, commentaries, letters, guidelines, or papers that do not fit other categories. |

### HEURISTIC — for human review

_Not a finding. A normalised similarity check (≥0.85, or literal containment) of each spec sentence against every piece of this field's codebook text. It cannot tell paraphrase from omission, and it splits on punctuation rather than meaning._

Spec sentences with no near-match in the codebook (1 of 2):

> Look for explicit statements like 'prospective study,' 'case series,' etc.

Codebook units with no near-match in the spec description: **27** of 28 (count only, per the brief).

---

## robot_platform

`type: free_text` · `tier: 1` · `field_class: stated` · `judge_rubric_family: numeric_verbatim`

### Spec — `extraction_schema.fields[].description`

**description**

```text
The name/model of the robotic system used, including manufacturer if stated. Record exactly as the paper states it. If multiple robots are compared, list all separated by semicolons. If the paper describes an algorithm without a physical platform, enter 'No physical platform — [brief descriptor]'. If a generic robot arm not designed for surgery (e.g., KUKA LBR iiwa), record as stated.
```

### Codebook

**definition**

```text
The name/model of the robotic system used, including manufacturer if stated.
```

**instruction**

```text
Record exactly as the paper states it. If multiple robots are compared, list all separated by semicolons. If the paper describes an algorithm without a physical platform, enter "No physical platform — [brief descriptor]". If a generic robot arm not designed for surgery (e.g., KUKA LBR iiwa), record as stated.
```

### HEURISTIC — for human review

_Not a finding. A normalised similarity check (≥0.85, or literal containment) of each spec sentence against every piece of this field's codebook text. It cannot tell paraphrase from omission, and it splits on punctuation rather than meaning._

Spec sentences with no near-match in the codebook: **none** (0 of 5).

Codebook units with no near-match in the spec description: **0** of 5 (count only, per the brief).

---

## task_performed

`type: free_text` · `tier: 1` · `field_class: stated` · `judge_rubric_family: numeric_verbatim`

### Spec — `extraction_schema.fields[].description`

**description**

```text
The specific surgical task the robot performed autonomously or semi-autonomously. Record the specific autonomous task, not the overall procedure — e.g. 'suturing' not 'colectomy'. Be specific: 'needle driving' not 'surgery'. If multiple autonomous tasks, list all separated by semicolons.
```

### Codebook

**definition**

```text
The specific surgical task the robot performed autonomously or semi-autonomously.
```

**instruction**

```text
Record the specific autonomous task, not the overall procedure — e.g. "suturing" not "colectomy". Be specific: "needle driving" not "surgery". If multiple autonomous tasks, list all separated by semicolons.
```

### HEURISTIC — for human review

_Not a finding. A normalised similarity check (≥0.85, or literal containment) of each spec sentence against every piece of this field's codebook text. It cannot tell paraphrase from omission, and it splits on punctuation rather than meaning._

Spec sentences with no near-match in the codebook: **none** (0 of 4).

Codebook units with no near-match in the spec description: **0** of 4 (count only, per the brief).

---

## sample_size

`type: numeric` · `tier: 1` · `field_class: stated` · `judge_rubric_family: numeric_verbatim`

### Spec — `extraction_schema.fields[].description`

**description**

```text
Total number of cases, experiments, procedures, or subjects. For animal studies, report number of animals. For simulations, report number of trials. For phantom studies, report number of experimental runs. Enter the number only, or NR.
```

### Codebook

**definition**

```text
Total number of cases, experiments, procedures, or subjects.
```

**instruction**

```text
Report as a single integer representing the total sample. If multiple groups, sum them. Example: if 4 pigs + 5 phantoms = "9". For animal studies, report number of animals. For simulations, report number of trials. For phantom studies, report number of experimental runs. Enter the number only, or NR.
```

### HEURISTIC — for human review

_Not a finding. A normalised similarity check (≥0.85, or literal containment) of each spec sentence against every piece of this field's codebook text. It cannot tell paraphrase from omission, and it splits on punctuation rather than meaning._

Spec sentences with no near-match in the codebook: **none** (0 of 5).

Codebook units with no near-match in the spec description: **3** of 8 (count only, per the brief).

---

## surgical_domain

`type: categorical` · `tier: 1` · `field_class: inferable` · `judge_rubric_family: nominal_categorical`

### Spec — `extraction_schema.fields[].description`

**description**

```text
The surgical specialty or application area the autonomous task applies to. If testing on a bench model without specific clinical specialty context, use 'Non-clinical Bench / Phantom'. If purely computational with no physical experiment, use 'Computational / Simulation Only'. If explicitly testing across multiple specialties, use 'Multiple'.
```

### Codebook

**definition**

```text
The surgical specialty or application area the autonomous task applies to.
```

**instruction**

```text
If testing on a bench model without specific clinical specialty context, use "Non-clinical Bench / Phantom". If purely computational with no physical experiment, use "Computational / Simulation Only". If explicitly testing across multiple specialties, use "Multiple".
```

**decision_criteria**

```text
1. Does the paper describe a task within a specific clinical specialty? → Use that specialty
2. Is the task performed on a bench/phantom without specialty context? → "Non-clinical Bench / Phantom"
3. Is there no physical experiment at all? → "Computational / Simulation Only"
4. Does the paper explicitly test across multiple specialties? → "Multiple"
```

**valid_values**

| value | definition |
|---|---|
| `General Surgery` | Abdominal, colorectal, foregut, HPB, or unspecified general surgical procedures. |
| `Urology` | Urologic procedures including prostatectomy, nephrectomy, lithotripsy. |
| `Gynecology` | Gynecologic procedures including hysterectomy, oophorectomy, endometriosis. |
| `Cardiac/Thoracic` | Cardiac surgery, thoracic surgery, TAVR/TAVI, lung resection. |
| `Head & Neck / ENT` | Transoral robotic surgery (TORS), thyroidectomy, otologic procedures. |
| `Vascular` | Vascular surgical procedures, endovascular interventions. |
| `Non-clinical Bench / Phantom` | Autonomous task demonstrated on a bench model, phantom, or tissue surrogate without specific clinical specialty context. Use this for generalizable tasks like suturing or needle driving on phantoms. |
| `Computational / Simulation Only` | Purely computational or simulated work with no physical experiment or hardware in the loop. |
| `Multiple` | Paper explicitly tests across multiple surgical specialties. |
| `Other` | Specialty not listed above, or unclear. |

### HEURISTIC — for human review

_Not a finding. A normalised similarity check (≥0.85, or literal containment) of each spec sentence against every piece of this field's codebook text. It cannot tell paraphrase from omission, and it splits on punctuation rather than meaning._

Spec sentences with no near-match in the codebook: **none** (0 of 4).

Codebook units with no near-match in the spec description: **23** of 30 (count only, per the brief).

---

## autonomy_level

`type: categorical` · `tier: 2` · `field_class: judgment` · `judge_rubric_family: ordinal`

### Spec — `extraction_schema.fields[].description`

**description**

```text
The highest level of autonomy demonstrated, mapped to the Yang et al. (2017) classification. Level 0 = no autonomy (teleoperation). Level 1 = robot assistance (tremor filtration, haptic feedback, virtual fixtures). Level 2 = task autonomy (robot executes specific preprogrammed tasks, surgeon initiates and monitors). Level 3 = conditional autonomy (robot generates strategies for surgeon to select). Level 4 = high autonomy (robot makes decisions and executes independently, surgeon supervises). Level 5 = full autonomy (no human intervention). If the paper does not use this framework, map their description using the decision tree. For algorithms/simulations, classify based on what the system demonstrates. Use 'Mixed/Multiple' only when paper explicitly tests multiple distinct autonomy levels.
```

### Codebook

**definition**

```text
The highest level of autonomy demonstrated, mapped to the Yang et al. (2017) classification.
```

**instruction**

```text
If the paper does not explicitly reference Yang levels, map their description using the decision tree below. For algorithms/simulations, classify based on what the system demonstrates, not the hardware. Use "Mixed/Multiple" ONLY when the paper explicitly tests multiple distinct autonomy levels — not as an escape hatch for uncertainty.
```

**decision_criteria**

```text
1. Does the robot execute any action without continuous real-time human control? → If NO → Level 1
2. If YES — does the surgeon define the exact plan and initiate execution? → If YES → Level 2
3. Does the robot generate candidate strategies for the surgeon to select from? → If YES → Level 3
4. Does the robot independently plan and execute based on patient-specific data, with surgeon monitoring? → If YES → Level 4
5. Does the robot operate without any human in the loop? → If YES → Level 5
NOTE on algorithms/simulations: Classify based on what the system demonstrates, not the hardware. A simulated algorithm that autonomously plans and executes is still Level 2+.
```

**valid_values**

| value | definition |
|---|---|
| `0 (No autonomy)` | Pure teleoperation. Human directly controls all robot movements in real-time. No autonomous component. |
| `1 (Robot assistance)` | Robot provides assistive features during human-controlled operation: tremor filtration, haptic feedback, virtual fixtures, motion scaling. Human maintains continuous real-time control. |
| `2 (Task autonomy)` | Robot executes specific preprogrammed or learned tasks autonomously. Surgeon initiates the task, defines the plan, and monitors execution but does not control moment-to-moment movements. |
| `3 (Conditional autonomy)` | Robot generates candidate strategies, plans, or action options for the surgeon to select from. Surgeon retains decision authority but robot contributes to planning. |
| `4 (High autonomy)` | Robot independently plans and executes based on patient-specific or task-specific data, with surgeon supervising but not directing. Surgeon can intervene but robot acts independently by default. |
| `5 (Full autonomy)` | Robot operates without any human in the loop. No surgeon monitoring, no override capability exercised during the task. |
| `Mixed/Multiple` | Paper explicitly tests and reports on multiple distinct autonomy levels as separate experimental conditions. |
| `NR` | Autonomy level cannot be determined from the paper content. |

**examples**

- Surgeon teleoperates da Vinci with tremor filtering active → **1 (Robot assistance)**
- Surgeon marks suture entry/exit points, robot drives needle autonomously → **2 (Task autonomy)**
- Robot generates three trajectory options, surgeon selects one, robot executes → **3 (Conditional autonomy)**
- Robot uses computer vision to detect tissue, plans grasping strategy, and executes without surgeon input while surgeon watches monitor → **4 (High autonomy)**

**ordered_values**: `['0 (No autonomy)', '1 (Robot assistance)', '2 (Task autonomy)', '3 (Conditional autonomy)', '4 (High autonomy)', '5 (Full autonomy)', 'Mixed/Multiple', 'NR']`

**dimension**: `autonomy`

### HEURISTIC — for human review

_Not a finding. A normalised similarity check (≥0.85, or literal containment) of each spec sentence against every piece of this field's codebook text. It cannot tell paraphrase from omission, and it splits on punctuation rather than meaning._

Spec sentences with no near-match in the codebook (8 of 10):

> Level 0 = no autonomy (teleoperation).

> Level 1 = robot assistance (tremor filtration, haptic feedback, virtual fixtures).

> Level 2 = task autonomy (robot executes specific preprogrammed tasks, surgeon initiates and monitors).

> Level 3 = conditional autonomy (robot generates strategies for surgeon to select).

> Level 4 = high autonomy (robot makes decisions and executes independently, surgeon supervises).

> Level 5 = full autonomy (no human intervention).

> If the paper does not use this framework, map their description using the decision tree.

> Use 'Mixed/Multiple' only when paper explicitly tests multiple distinct autonomy levels.

Codebook units with no near-match in the spec description: **39** of 42 (count only, per the brief).

---

## validation_setting

`type: categorical` · `tier: 2` · `field_class: stated` · `judge_rubric_family: ordinal`

### Spec — `extraction_schema.fields[].description`

**description**

```text
The experimental setting in which the autonomous capability was tested. Select the most advanced setting if multiple were used. Hierarchy: human > animal > cadaver > ex vivo > phantom > simulation > computational.
```

### Codebook

**definition**

```text
The experimental setting in which the autonomous capability was tested.
```

**instruction**

```text
If multiple settings were used, list all separated by semicolons. Each value must exactly match one allowed value.
```

**decision_criteria**

```text
1. Was the system tested on living human patients? → "In vivo (human)"
2. On living animals? → "In vivo (animal)"
3. On human cadavers? → "Cadaver"
4. On excised tissue outside a living body? → "Ex vivo"
5. On synthetic phantoms or bench models, or simulation with hardware? → "Phantom/Simulation"
6. Purely computational, no physical hardware? → "Computational/Virtual"
7. Multiple settings and cannot identify individual ones? → "Mixed"
If multiple settings are clearly identifiable, list with semicolons (e.g., "In vivo (animal); Phantom/Simulation").
```

**valid_values**

| value | definition |
|---|---|
| `In vivo (human)` | Tested on living human patients in a clinical or clinical-trial setting. |
| `In vivo (animal)` | Tested on living animals (e.g., porcine, rodent). Includes survival and non-survival animal studies. |
| `Ex vivo` | Tested on excised biological tissue outside a living organism (e.g., excised porcine intestine, bovine liver). |
| `Phantom/Simulation` | Tested on synthetic phantoms, bench models, or software simulations with hardware in the loop. |
| `Cadaver` | Tested on human cadaveric specimens. |
| `Computational/Virtual` | Purely computational or virtual simulation with no physical hardware or tissue involved. |
| `Mixed` | Multiple validation settings used, none dominant. Prefer listing individual settings with semicolons when identifiable. |
| `NR` | Validation setting not described or cannot be determined. |

**ordered_values**: `['In vivo (human)', 'In vivo (animal)', 'Ex vivo', 'Phantom/Simulation', 'Cadaver', 'Computational/Virtual', 'Mixed', 'NR']`

**dimension**: `validation fidelity`

### HEURISTIC — for human review

_Not a finding. A normalised similarity check (≥0.85, or literal containment) of each spec sentence against every piece of this field's codebook text. It cannot tell paraphrase from omission, and it splits on punctuation rather than meaning._

Spec sentences with no near-match in the codebook (2 of 3):

> Select the most advanced setting if multiple were used.

> Hierarchy: human > animal > cadaver > ex vivo > phantom > simulation > computational.

Codebook units with no near-match in the spec description: **26** of 29 (count only, per the brief).

---

## task_monitor

`type: categorical` · `tier: 2` · `field_class: inferable` · `judge_rubric_family: nominal_categorical`

### Spec — `extraction_schema.fields[].description`

**description**

```text
Who observes the surgical environment and gathers information during the task? H = surgeon visually observes/interprets (robot has no independent perception). R = robot autonomously senses via cameras/force sensors/imaging without human input. Shared = both independently gather and interpret information. NR = not described.
```

### Codebook

**definition**

```text
Who observes the surgical environment and gathers information during the task?
```

**instruction**

```text
Determine who has perceptual awareness of the task state.
```

**valid_values**

| value | definition |
|---|---|
| `H` | Surgeon visually observes and interprets the environment. Robot has no independent perception capability. |
| `R` | Robot autonomously senses via cameras, force sensors, or imaging without requiring human input for perception. |
| `Shared` | Both independently gather and interpret information. Robot uses sensors AND surgeon independently monitors. |
| `NR` | Monitoring arrangement not described in the paper. |

**examples**

- Surgeon watches endoscopic video while teleoperating → **H**
- Vision system tracks tissue deformation in real time to guide autonomous cutting → **R**
- Robot uses CV to track a needle while surgeon monitors on display → **Shared**

### HEURISTIC — for human review

_Not a finding. A normalised similarity check (≥0.85, or literal containment) of each spec sentence against every piece of this field's codebook text. It cannot tell paraphrase from omission, and it splits on punctuation rather than meaning._

Spec sentences with no near-match in the codebook (3 of 5):

> H = surgeon visually observes/interprets (robot has no independent perception).

> R = robot autonomously senses via cameras/force sensors/imaging without human input.

> NR = not described.

Codebook units with no near-match in the spec description: **9** of 18 (count only, per the brief).

---

## task_generate

`type: categorical` · `tier: 2` · `field_class: inferable` · `judge_rubric_family: nominal_categorical`

### Spec — `extraction_schema.fields[].description`

**description**

```text
Who creates the plan or strategy for how the task will be performed (trajectory, action sequence, parameters, surgical strategy)? H = surgeon defines all task parameters. R = system computes plan autonomously from data. Shared = both contribute (e.g., surgeon defines target anatomy, robot generates trajectory). NR = not described.
```

### Codebook

**definition**

```text
Who creates the plan or strategy for how the task will be performed? Covers trajectory, action sequence, parameters (speed, force, path), or surgical strategy.
```

**instruction**

```text
Determine who is responsible for planning the task execution.
```

**valid_values**

| value | definition |
|---|---|
| `H` | Surgeon defines all task parameters, trajectories, and action sequences. |
| `R` | System computes the plan autonomously from data (e.g., path planning from tissue geometry). |
| `Shared` | Both contribute — e.g., surgeon defines target anatomy, robot generates trajectory to reach it. |
| `NR` | Planning arrangement not described. |

**examples**

- Surgeon places suture entry/exit points on the image, robot follows → **H**
- Path-planning algorithm generates optimal trajectory from tissue geometry → **R**
- Surgeon defines target anatomy on preop scan, robot generates intraop trajectory → **Shared**

### HEURISTIC — for human review

_Not a finding. A normalised similarity check (≥0.85, or literal containment) of each spec sentence against every piece of this field's codebook text. It cannot tell paraphrase from omission, and it splits on punctuation rather than meaning._

Spec sentences with no near-match in the codebook (4 of 5):

> Who creates the plan or strategy for how the task will be performed (trajectory, action sequence, parameters, surgical strategy)?

> H = surgeon defines all task parameters.

> R = system computes plan autonomously from data.

> NR = not described.

Codebook units with no near-match in the spec description: **9** of 17 (count only, per the brief).

---

## task_select

`type: categorical` · `tier: 2` · `field_class: inferable` · `judge_rubric_family: nominal_categorical`

### Spec — `extraction_schema.fields[].description`

**description**

```text
Who chooses which plan to execute when alternatives exist? H = surgeon reviews options and authorizes a specific plan. R = system autonomously selects from its own generated options (also use R when system generates a single plan and executes it — no selection step). Shared = both have selection authority. NR = not described.
```

### Codebook

**definition**

```text
Who chooses which plan to execute when alternatives exist?
```

**instruction**

```text
Determine who has selection authority. When the system generates a single plan and executes it (no selection step), classify as R.
```

**valid_values**

| value | definition |
|---|---|
| `H` | Surgeon reviews options and authorizes a specific plan before execution. |
| `R` | System autonomously selects from its own generated options. Also use R when the system generates a single plan and executes — no selection step exists. |
| `Shared` | Both have selection authority — e.g., robot narrows to shortlist, surgeon approves. |
| `NR` | Selection arrangement not described. |

**examples**

- Robot generates three paths, surgeon selects one on screen → **H**
- RL controller evaluates strategies and commits to highest-scored → **R**
- Robot narrows to shortlist of grasping poses, surgeon approves one → **Shared**

### HEURISTIC — for human review

_Not a finding. A normalised similarity check (≥0.85, or literal containment) of each spec sentence against every piece of this field's codebook text. It cannot tell paraphrase from omission, and it splits on punctuation rather than meaning._

Spec sentences with no near-match in the codebook (4 of 5):

> H = surgeon reviews options and authorizes a specific plan.

> R = system autonomously selects from its own generated options (also use R when system generates a single plan and executes it — no selection step).

> Shared = both have selection authority.

> NR = not described.

Codebook units with no near-match in the spec description: **10** of 18 (count only, per the brief).

---

## task_execute

`type: categorical` · `tier: 2` · `field_class: inferable` · `judge_rubric_family: nominal_categorical`

### Spec — `extraction_schema.fields[].description`

**description**

```text
Who physically carries out the surgical action? H = surgeon directly controls all instrument movements (standard teleoperation). R = system performs the physical action autonomously. Shared = both contribute simultaneously (e.g., cooperative control, active constraints). NR = not described.
```

### Codebook

**definition**

```text
Who physically carries out the surgical action?
```

**instruction**

```text
Determine who controls the physical instrument movements.
```

**valid_values**

| value | definition |
|---|---|
| `H` | Surgeon directly controls all instrument movements (standard teleoperation). |
| `R` | System performs the physical action autonomously without human hand-on-instrument control. |
| `Shared` | Both contribute simultaneously — e.g., cooperative control where surgeon holds instrument while robot applies active constraints. |
| `NR` | Execution arrangement not described. |

**examples**

- Standard teleoperation with da Vinci → **H**
- Robot drives needle through tissue autonomously → **R**
- Surgeon holds instrument, robot applies force limits and virtual fixtures → **Shared**

### HEURISTIC — for human review

_Not a finding. A normalised similarity check (≥0.85, or literal containment) of each spec sentence against every piece of this field's codebook text. It cannot tell paraphrase from omission, and it splits on punctuation rather than meaning._

Spec sentences with no near-match in the codebook (3 of 5):

> R = system performs the physical action autonomously.

> Shared = both contribute simultaneously (e.g., cooperative control, active constraints).

> NR = not described.

Codebook units with no near-match in the spec description: **7** of 16 (count only, per the brief).

---

## system_maturity

`type: categorical` · `tier: 2` · `field_class: judgment` · `judge_rubric_family: ordinal`

### Spec — `extraction_schema.fields[].description`

**description**

```text
The technology readiness level of what the paper actually demonstrates.
```

### Codebook

**definition**

```text
The technology readiness level of what the paper actually demonstrates.
```

**instruction**

```text
Classify based on the most advanced capability demonstrated in the paper, not what is described as future work. Focus on what was actually built and tested.
```

**decision_criteria**

```text
1. Is the paper proposing a design without building/testing it? → "Conceptual / framework"
2. Is the work purely in simulation with no physical robot? → "Simulation / computational only"
3. Is the contribution primarily a new algorithm tested on an existing robot? → "Algorithm on existing platform"
4. Was a custom physical robot built for this work? → "Research prototype (hardware)"
5. Was a commercial robot modified for research autonomy? → "Commercial system + research autonomy"
6. Was a commercial robot used in its approved capacity? → "Commercial clinical system"
Pick the MOST ADVANCED stage demonstrated.
```

**valid_values**

| value | definition |
|---|---|
| `Commercial clinical system` | FDA-cleared or CE-marked robot used in its approved clinical capacity. Examples: da Vinci in standard teleoperation, Mako for joint replacement. |
| `Commercial system + research autonomy` | Commercial robot modified or programmed for autonomous tasks not in the cleared indication. Examples: dVRK used for autonomous suturing, da Vinci Research Kit experiments. |
| `Research prototype (hardware)` | Purpose-built physical robot not commercially available. Examples: STAR surgical robot, custom needle-steering robots, novel end-effector designs. |
| `Algorithm on existing platform` | New software or algorithm deployed on an existing commercial or research robot. The contribution is the algorithm, not the hardware. |
| `Simulation / computational only` | No physical robot involved. Purely in-silico experiments — simulated environments, computational models, virtual robot control. |
| `Conceptual / framework` | No experimental demonstration. Proposes a design, taxonomy, architecture, or theoretical framework without building or testing it. |

**ordered_values**: `['Commercial clinical system', 'Commercial system + research autonomy', 'Research prototype (hardware)', 'Algorithm on existing platform', 'Simulation / computational only', 'Conceptual / framework']`

**dimension**: `technology readiness`

### HEURISTIC — for human review

_Not a finding. A normalised similarity check (≥0.85, or literal containment) of each spec sentence against every piece of this field's codebook text. It cannot tell paraphrase from omission, and it splits on punctuation rather than meaning._

Spec sentences with no near-match in the codebook: **none** (0 of 1).

Codebook units with no near-match in the spec description: **27** of 28 (count only, per the brief).

---

## study_design

`type: categorical` · `tier: 2` · `field_class: judgment` · `judge_rubric_family: nominal_categorical`

### Spec — `extraction_schema.fields[].description`

**description**

```text
The specific study design, more granular than study_type. This is about HOW the study was conducted, not WHAT it studied. If a paper demonstrates a new algorithm on a phantom, classify based on emphasis (initial technical demonstration vs algorithm development).
```

### Codebook

**definition**

```text
The specific study design, more granular than study_type. This is about HOW the study was conducted, not WHAT it studied.
```

**instruction**

```text
Select the single best-fit category. If a paper demonstrates a new algorithm on a phantom, classify as "Initial technical demonstration" if the emphasis is on showing the system works, or "Algorithm development and evaluation" if the emphasis is on the algorithm's performance metrics.
```

**decision_criteria**

```text
1. Is this a literature review with systematic methods? → "Systematic review / meta-analysis"
2. Is the primary contribution a new algorithm with quantitative evaluation? → "Algorithm development and evaluation"
3. Is it a first demonstration that a system can perform a task? → "Initial technical demonstration"
4. Does it systematically evaluate feasibility with success/failure criteria? → "Feasibility study"
5. Does it directly compare autonomous vs. human/alternative? → "Comparative study (vs. human or alternative)"
6. Is it validated on animal tissue or cadavers for translational purposes? → "Preclinical validation (animal/cadaver)"
7. Is it first use on human patients? → "Clinical pilot / first-in-human"
8. Is it a prospective randomized trial? → "Randomized controlled trial"
9. Is it a retrospective analysis of clinical cases? → "Retrospective clinical review"
```

**valid_values**

| value | definition |
|---|---|
| `Initial technical demonstration` | First-time demonstration that a system or method works. Focus is on 'can it do this?' rather than systematic evaluation. |
| `Feasibility study` | Structured evaluation of whether a technique is practical and safe enough to warrant further development. Often includes success/failure criteria. |
| `Comparative study (vs. human or alternative)` | Direct comparison between the autonomous system and a human operator, manual technique, or alternative automated approach. |
| `Preclinical validation (animal/cadaver)` | Validation on biological tissue (animal or cadaver) as a step toward clinical translation. |
| `Clinical pilot / first-in-human` | First use of the autonomous capability on human patients, typically with small sample size and safety focus. |
| `Randomized controlled trial` | Prospective trial with randomization to autonomous vs. control arm. |
| `Retrospective clinical review` | Analysis of previously collected clinical data from cases using the autonomous system. |
| `Algorithm development and evaluation` | Primary contribution is a new algorithm, evaluated with quantitative metrics. May be tested on phantom, simulation, or real tissue, but the paper's focus is the algorithm. |
| `Systematic review / meta-analysis` | Structured review of existing literature with systematic search methodology. |
| `Other` | Study design does not fit any of the above categories. |

### HEURISTIC — for human review

_Not a finding. A normalised similarity check (≥0.85, or literal containment) of each spec sentence against every piece of this field's codebook text. It cannot tell paraphrase from omission, and it splits on punctuation rather than meaning._

Spec sentences with no near-match in the codebook (1 of 3):

> If a paper demonstrates a new algorithm on a phantom, classify based on emphasis (initial technical demonstration vs algorithm development).

Codebook units with no near-match in the spec description: **34** of 37 (count only, per the brief).

---

## country

`type: free_text` · `tier: 2` · `field_class: inferable` · `judge_rubric_family: free_text`

### Spec — `extraction_schema.fields[].description`

**description**

```text
Country where the study was conducted. Use first author's institution country if not explicitly stated. Metadata-inferred is acceptable. If multi-country, list all separated by semicolons. If unclear, record 'NR'.
```

### Codebook

**definition**

```text
Country where the study was conducted.
```

**instruction**

```text
Use first author's institution country if not explicitly stated. Metadata-inferred is acceptable. If multi-country, list all separated by semicolons. If unclear, record "NR".
```

### HEURISTIC — for human review

_Not a finding. A normalised similarity check (≥0.85, or literal containment) of each spec sentence against every piece of this field's codebook text. It cannot tell paraphrase from omission, and it splits on punctuation rather than meaning._

Spec sentences with no near-match in the codebook: **none** (0 of 5).

Codebook units with no near-match in the spec description: **0** of 5 (count only, per the brief).

---

## primary_outcome_metric

`type: free_text` · `tier: 3` · `field_class: stated` · `judge_rubric_family: free_text`

### Spec — `extraction_schema.fields[].description`

**description**

```text
The primary outcome measure reported. Record the first quantitative outcome in the results section related to the autonomous task's performance. Use the paper's exact terminology (e.g., 'task completion time,' 'suture accuracy score,' 'Dice coefficient'). Do not select based on judgment of 'most emphasized' — use positional order.
```

### Codebook

**definition**

```text
The primary outcome measure reported, related to the autonomous task's performance.
```

**instruction**

```text
Record the first quantitative outcome in the results section related to the autonomous task's performance. Use the paper's exact terminology (e.g., "task completion time," "suture accuracy score," "Dice coefficient"). Do not select based on judgment of "most emphasized" — use positional order.
```

### HEURISTIC — for human review

_Not a finding. A normalised similarity check (≥0.85, or literal containment) of each spec sentence against every piece of this field's codebook text. It cannot tell paraphrase from omission, and it splits on punctuation rather than meaning._

Spec sentences with no near-match in the codebook (1 of 4):

> The primary outcome measure reported.

Codebook units with no near-match in the spec description: **1** of 4 (count only, per the brief).

---

## primary_outcome_value

`type: free_text` · `tier: 3` · `field_class: stated` · `judge_rubric_family: free_text`

### Spec — `extraction_schema.fields[].description`

**description**

```text
The numeric result for the primary outcome metric. Include units, confidence intervals, or p-values if reported. Copy the exact numeric reporting (e.g., 'mean 4.2 ± 0.8 mm', '92.3% (95% CI: 88.1–96.5)', 'median 12 min (IQR 8–15)').
```

### Codebook

**definition**

```text
The numeric result for the primary outcome metric.
```

**instruction**

```text
Include units, confidence intervals, or p-values if reported. Copy the exact numeric reporting (e.g., "mean 4.2 ± 0.8 mm", "92.3% (95% CI: 88.1–96.5)", "median 12 min (IQR 8–15)").
```

### HEURISTIC — for human review

_Not a finding. A normalised similarity check (≥0.85, or literal containment) of each spec sentence against every piece of this field's codebook text. It cannot tell paraphrase from omission, and it splits on punctuation rather than meaning._

Spec sentences with no near-match in the codebook: **none** (0 of 3).

Codebook units with no near-match in the spec description: **0** of 3 (count only, per the brief).

---

## comparison_to_human

`type: free_text` · `tier: 3` · `field_class: stated` · `judge_rubric_family: free_text`

### Spec — `extraction_schema.fields[].description`

**description**

```text
If the paper compares autonomous performance to human/manual performance, report the comparison result. Include both values if available (e.g., 'Autonomous: 142s vs Manual: 198s (p=0.003)'). If no comparison was made, record 'No comparison reported'.
```

### Codebook

**definition**

```text
Comparison of autonomous system performance to human/manual performance.
```

**instruction**

```text
If the paper compares autonomous performance to human/manual performance, report the comparison result. Include both values if available (e.g., "Autonomous: 142s vs Manual: 198s (p=0.003)"). If no comparison was made, record "No comparison reported".
```

### HEURISTIC — for human review

_Not a finding. A normalised similarity check (≥0.85, or literal containment) of each spec sentence against every piece of this field's codebook text. It cannot tell paraphrase from omission, and it splits on punctuation rather than meaning._

Spec sentences with no near-match in the codebook: **none** (0 of 3).

Codebook units with no near-match in the spec description: **1** of 4 (count only, per the brief).

---

## secondary_outcomes

`type: free_text` · `tier: 3` · `field_class: stated` · `judge_rubric_family: free_text`

### Spec — `extraction_schema.fields[].description`

**description**

```text
Additional quantitative outcomes reported beyond the primary. Record as semicolon-separated entries in format 'metric: value'. Enter NR if only one outcome reported.
```

### Codebook

**definition**

```text
Additional quantitative outcomes reported beyond the primary.
```

**instruction**

```text
Record as semicolon-separated entries in format "metric: value". Example: "Accuracy: 94.2% ± 3.1%; Force: 2.3 ± 0.8 N; Success rate: 18/20". Enter NR if only one outcome reported.
```

### HEURISTIC — for human review

_Not a finding. A normalised similarity check (≥0.85, or literal containment) of each spec sentence against every piece of this field's codebook text. It cannot tell paraphrase from omission, and it splits on punctuation rather than meaning._

Spec sentences with no near-match in the codebook: **none** (0 of 3).

Codebook units with no near-match in the spec description: **1** of 4 (count only, per the brief).

---

## key_limitation

`type: free_text` · `tier: 4` · `field_class: judgment` · `judge_rubric_family: free_text`

### Spec — `extraction_schema.fields[].description`

**description**

```text
The most important limitation of the study. In 1-2 sentences, state the key limitation using YOUR judgment. Do not simply copy what the authors wrote in their limitations section. Quote the passage(s) that informed your assessment.
```

### Codebook

**definition**

```text
The most important limitation of the study, based on your judgment.
```

**instruction**

```text
Reason to the key limitation in steps, and state it in 1–2 sentences. Each step must either cite the unit(s) that support it or be marked as applying these criteria with no textual basis claimed. Do not simply restate what the authors wrote in their limitations section: the steps are how you got to YOUR judgment, not a quotation of theirs.
```

### HEURISTIC — for human review

_Not a finding. A normalised similarity check (≥0.85, or literal containment) of each spec sentence against every piece of this field's codebook text. It cannot tell paraphrase from omission, and it splits on punctuation rather than meaning._

Spec sentences with no near-match in the codebook (4 of 4):

> The most important limitation of the study.

> In 1-2 sentences, state the key limitation using YOUR judgment.

> Do not simply copy what the authors wrote in their limitations section.

> Quote the passage(s) that informed your assessment.

Codebook units with no near-match in the spec description: **4** of 4 (count only, per the brief).

---

## clinical_readiness_assessment

`type: categorical` · `tier: 4` · `field_class: judgment` · `judge_rubric_family: ordinal`

### Spec — `extraction_schema.fields[].description`

**description**

```text
YOUR JUDGMENT: Based on the totality of evidence in this paper, how close is the described technology to clinical use in patients? This field has no right answer in the text — it requires you to synthesize the results, limitations, and setting. Quote the key evidence behind your judgment.
```

### Codebook

**definition**

```text
Your judgment of how close the described technology is to clinical use in patients. There is no right answer in the text — this requires you to synthesize the results, limitations, and validation setting.
```

**instruction**

```text
Reason to this judgment in steps, then select the category. Each step must either cite the unit(s) that support it or be marked as applying these criteria with no textual basis claimed. Synthesize the results, the limitations and the validation setting: the steps ARE that synthesis, not a quotation of what the authors claim about readiness.
```

**decision_criteria**

```text
1. Does the paper demonstrate a working system performing an autonomous task? If NO → "Early-stage research"
2. If YES — was it demonstrated on patients (in vivo human)? If YES with regulatory clearance → "Ready for clinical use". If YES without clearance → "Approaching clinical readiness"
3. If demonstrated on animals with clinical-grade setup or human cadavers with translational intent → "Approaching clinical readiness"
4. If demonstrated on phantom, bench, ex vivo, or non-clinical animal → "Proof of concept only"
5. If the paper is a review or cannot be assessed → "Not assessable"
```

**valid_values**

| value | definition |
|---|---|
| `Ready for clinical use` | System is FDA-cleared/CE-marked for the autonomous capability described, with clinical evidence supporting safety and efficacy in patients. |
| `Approaching clinical readiness` | System has been demonstrated in vivo (human or animal with clinical-grade setup), with a clear regulatory or translational pathway. Not yet cleared for autonomous clinical use. |
| `Early-stage research` | Foundational algorithmic or computational work. No integrated robotic system demonstrated performing the task. Theoretical frameworks, pure simulations without hardware-in-the-loop, or isolated software components. |
| `Proof of concept only` | A working system demonstrated performing the task on a phantom, bench model, cadaver, ex vivo tissue, or in a non-clinical animal experiment. Shows technical feasibility but no clinical pathway established. |
| `Not assessable` | Review articles, editorials, or papers where clinical readiness cannot be determined from the content. |

**examples**

- Algorithm autonomously sutures on a tissue phantom using dVRK → **Proof of concept only**
- STAR robot performs in vivo bowel anastomosis in pigs → **Approaching clinical readiness**
- Path-planning algorithm tested in simulation only, no robot involved → **Early-stage research**
- FDA-cleared autonomous laser lithotripsy on patients → **Ready for clinical use**
- Scoping review of autonomous surgical robotics literature → **Not assessable**

**ordered_values**: `['Ready for clinical use', 'Approaching clinical readiness', 'Early-stage research', 'Proof of concept only', 'Not assessable']`

**dimension**: `clinical readiness`

### HEURISTIC — for human review

_Not a finding. A normalised similarity check (≥0.85, or literal containment) of each spec sentence against every piece of this field's codebook text. It cannot tell paraphrase from omission, and it splits on punctuation rather than meaning._

Spec sentences with no near-match in the codebook (2 of 3):

> YOUR JUDGMENT: Based on the totality of evidence in this paper, how close is the described technology to clinical use in patients?

> Quote the key evidence behind your judgment.

Codebook units with no near-match in the spec description: **37** of 38 (count only, per the brief).

---
