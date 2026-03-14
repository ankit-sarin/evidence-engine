# Extraction Codebook v2 — Autonomy in Surgical Robotics

**Review:** Autonomy in Surgical Robotics
**Schema version:** 2.0 (revised Mar 13, 2026)
**Total fields:** 20 (5 Tier 1, 9 Tier 2, 4 Tier 3, 2 Tier 4)
**Applicable to:** All three concordance arms (local AI, cloud AI, human extractors)

---

## TIER 1 — EXPLICIT (5 fields)

Expected AI-Human κ > 0.90. Source quotes not required.

---

### 1. `study_type` — Categorical

**Definition:** The type of study as described in the methods section.

**Valid values:** Original Research, Case Report/Series, Review, Systematic Review, Meta-Analysis, Conference Abstract, Technical Report, Other

**Instruction:** Look for explicit statements like "prospective study," "case series," etc. in the methods section.

**Example:** Original Research

---

### 2. `robot_platform` — Free text

**Definition:** The name/model of the robotic system used, including manufacturer if stated.

**Valid values:** Any robot name as stated in paper.

**Instruction:** Record the platform exactly as the paper states it. If multiple robots are compared, list all separated by semicolons. If the paper describes an algorithm, control method, or simulation without a physical robotic platform, enter "No physical platform — [brief descriptor]" (e.g., "No physical platform — path planning algorithm"). If the paper uses a generic robot arm not designed for surgery (e.g., KUKA LBR iiwa), record it as stated.

**Example:** da Vinci Xi (Intuitive Surgical); da Vinci Research Kit (dVRK)

---

### 3. `task_performed` — Free text

**Definition:** The specific surgical task or procedure the robot performed autonomously or semi-autonomously.

**Valid values:** Any surgical task.

**Instruction:** Record the specific task the robot performed autonomously, not the overall procedure. If a robot autonomously sutured during a human-performed colectomy, record "suturing," not "colectomy." Be specific — "needle driving" not "surgery." If multiple autonomous tasks were tested, list all separated by semicolons.

**Example:** Bowel anastomosis; Tissue retraction

---

### 4. `sample_size` — Numeric

**Definition:** Total number of cases, experiments, procedures, or subjects.

**Valid values:** Integer or NR.

**Instruction:** For animal studies, report number of animals. For simulations, report number of trials. For phantom studies, report number of experimental runs. Enter the number only.

**Example:** 45

---

### 5. `surgical_domain` — Categorical *(NEW — replaces `country` in Tier 1)*

**Definition:** The surgical specialty or application area the autonomous task applies to.

**Valid values:** General Surgery, Urology, Gynecology, Cardiac/Thoracic, Head & Neck / ENT, Vascular, Non-clinical Bench / Phantom, Computational / Simulation Only, Multiple, Other

**Instruction:** Record the surgical specialty the autonomous task is intended for or demonstrated in. If the paper tests on a bench model (tissue phantom, ex vivo tissue) without a specific clinical specialty context, use "Non-clinical Bench / Phantom." If purely computational with no physical experiment, use "Computational / Simulation Only." If the paper explicitly tests across multiple specialties, use "Multiple."

**Example:** Urology

---

## TIER 2 — INTERPRETIVE (9 fields)

Expected AI-Human κ 0.70–0.85. Source quotes optional but encouraged for fields requiring judgment.

---

### 6. `autonomy_level` — Categorical (Yang/Haidegger classification)

**Definition:** The level of surgical autonomy demonstrated, using the Yang classification (Levels 0–5).

**Valid values:** 0 (No autonomy), 1 (Robot assistance), 2 (Task autonomy), 3 (Conditional autonomy), 4 (High autonomy), 5 (Full autonomy), Mixed/Multiple, NR

**Level definitions:**

- **Level 0 — No Autonomy:** The surgeon performs all tasks manually with no robotic assistance. The surgeon generates, selects, and executes all actions.
- **Level 1 — Robot Assistance:** The robot provides passive support (e.g., tremor filtration, tool tracking) or active guidance (e.g., haptic feedback, virtual fixtures/active constraints), but the human maintains continuous, direct control over all movements.
- **Level 2 — Task Autonomy:** The robot can execute specific, preprogrammed tasks (e.g., suturing, camera positioning) independently. Tasks are initiated by the surgeon, who then monitors the action and intervenes as needed.
- **Level 3 — Conditional Autonomy:** The robot generates different task strategies for the surgeon to select from or approve. The robot then autonomously performs the chosen plan.
- **Level 4 — High Autonomy:** The robot can make surgical decisions and execute a full, patient-specific plan independently, operating under the supervision of a qualified doctor who can intervene.
- **Level 5 — Full Autonomy:** The robot operates entirely independently, from preoperative planning through the full procedure, without any human intervention or supervision.

**Decision tree (when the paper does not explicitly reference Yang levels):**

1. Does the robot execute any action without continuous real-time human control of that specific motion? → If **no** → **Level 1**
2. If yes — does the robot choose its own task strategy, or does the surgeon define the exact plan? → If surgeon defines plan and initiates execution → **Level 2**
3. Does the robot generate candidate strategies for the surgeon to select from? → If **yes** → **Level 3**
4. Does the robot independently plan and execute based on patient-specific data, with surgeon monitoring but not selecting the plan? → If **yes** → **Level 4**
5. Does the robot operate without any human in the loop from planning through execution? → If **yes** → **Level 5**

**On algorithms/simulations:** Classify the autonomy level based on what the system demonstrates, not where or on what hardware it runs. A simulated algorithm that autonomously plans and executes a task is still task-autonomous (Level 2+). The validation setting and system maturity fields capture the platform context.

**On "Mixed/Multiple":** Use only when the paper explicitly compares or tests multiple distinct autonomy levels — e.g., "we tested the system in both teleoperated (Level 1) and autonomous suturing (Level 2) modes." If a paper is ambiguous between adjacent levels, pick the best fit and note the ambiguity in the source quote. "Mixed/Multiple" is not an escape hatch for uncertainty.

---

### 7. `validation_setting` — Categorical

**Definition:** The experimental setting in which the autonomous capability was tested.

**Valid values:** In vivo (human), In vivo (animal), Ex vivo, Phantom/Simulation, Cadaver, Computational/Virtual, Mixed, NR

**Instruction:** Select the most advanced setting if multiple were used. Hierarchy: human > animal > cadaver > ex vivo > phantom > simulation > computational.

**Example:** In vivo (animal)

---

### 8–11. Task Specification Sub-Fields — Categorical (H / R / Shared / NR each)

These four fields decompose the human-robot interaction during the autonomous task. For each, record who performs that function: **Human (H)**, **Robot (R)**, **Shared**, or **NR** (not reported).

---

### 8. `task_monitor` — Who observes the surgical environment and gathers information during the task?

- **Human:** The surgeon visually observes the field, interprets imaging, or reads sensor data. The robot has no independent perception. *Example: surgeon watches a screen while teleoperating.*
- **Robot:** The system autonomously senses the environment using cameras, force sensors, or imaging and interprets that data without human input. *Example: a vision system tracks tissue deformation in real time to adjust a cutting path.*
- **Shared:** Both independently gather and interpret information. *Example: robot uses computer vision to track a suture needle while the surgeon monitors progress on a display.*
- **NR:** Not described.

---

### 9. `task_generate` — Who creates the plan or strategy for how the task will be performed?

This covers determining the trajectory, action sequence, parameters (speed, force, path), or surgical strategy.

- **Human:** The surgeon defines all task parameters. *Example: surgeon manually places suture entry and exit points; surgeon specifies a resection margin on preoperative imaging.*
- **Robot:** The system computes the plan autonomously from data. *Example: a path-planning algorithm generates an optimal needle trajectory from tissue geometry; system analyzes preoperative CT and produces a milling plan.*
- **Shared:** Both contribute. *Example: surgeon defines target anatomy on a scan, robot generates a specific trajectory to reach it.*
- **NR:** Not described.

---

### 10. `task_select` — Who chooses which plan to execute when alternatives exist?

- **Human:** The surgeon reviews options and authorizes a specific plan. *Example: robot generates three possible needle insertion paths, surgeon selects one.*
- **Robot:** The system autonomously selects from its own generated options. Also use **Robot** when the system generates a single plan and executes it — there is no selection step because no alternatives are presented (implicitly robot-selected). *Example: a reinforcement-learning controller evaluates multiple grasping strategies and commits to the highest-scored one.*
- **Shared:** Both have selection authority. *Example: robot narrows to a shortlist, surgeon approves or modifies the final choice.*
- **NR:** Not described.

---

### 11. `task_execute` — Who physically carries out the surgical action?

- **Human:** The surgeon directly controls all instrument movements. *Example: standard teleoperation.*
- **Robot:** The system performs the physical action autonomously. *Example: robot drives a needle through tissue along the planned trajectory without human hand-on-controller input.*
- **Shared:** Both contribute simultaneously. *Example: cooperative control where surgeon holds the instrument and robot applies active constraints; surgeon controls gross positioning, robot handles fine tremor-compensated insertion.*
- **NR:** Not described.

---

### 12. `system_maturity` — Categorical *(NEW — replaces `fda_status`)*

**Definition:** The technology readiness level of what the paper actually demonstrates.

**Valid values:**

- **Commercial clinical system** — FDA-cleared or CE-marked robot used in its approved clinical capacity (e.g., da Vinci teleop, Mako, TSolution One)
- **Commercial system + research autonomy** — Commercial robot modified or programmed to perform autonomous tasks not part of its cleared indication (e.g., da Vinci Research Kit running autonomous suturing algorithms)
- **Research prototype (hardware)** — Purpose-built physical robot not commercially available (e.g., STAR, custom needle-steering robot)
- **Algorithm on existing platform** — New software/control algorithm demonstrated on an existing robot (commercial or research), where the focus is the algorithm not the hardware
- **Simulation / computational only** — No physical robot involved, purely in-silico demonstration
- **Conceptual / framework** — No experimental demonstration, proposes a design or taxonomy

**Example:** Commercial system + research autonomy

---

### 13. `study_design` — Categorical *(converted from free text)*

**Definition:** The specific study design, more granular than study_type.

**Valid values:** Initial technical demonstration, Feasibility study, Comparative study (vs. human or alternative), Preclinical validation (animal/cadaver), Clinical pilot / first-in-human, Randomized controlled trial, Retrospective clinical review, Algorithm development and evaluation, Systematic review / meta-analysis, Other

**Instruction:** Select the best fit. If a paper demonstrates a new algorithm on a phantom, that's "Initial technical demonstration" or "Algorithm development and evaluation" depending on emphasis.

**Example:** Feasibility study

---

### 14. `country` — Free text *(demoted from Tier 1)*

**Definition:** Country where the study was conducted.

**Valid values:** Country name or NR.

**Instruction:** Use first author's institution country if not explicitly stated. Metadata-inferred is acceptable — no source quote required.

**Example:** United States

---

## TIER 3 — NUMERIC (4 fields)

Expected AI-Human κ variable. Source quotes optional but encouraged for complex tables.

---

### 15. `primary_outcome_metric` — Free text

**Definition:** The primary outcome measure reported.

**Instruction:** Record the first quantitative outcome reported in the results section. Do not select based on judgment of "most emphasized" — use positional order. If the first result is a demographic or descriptive statistic, skip to the first outcome related to the autonomous task's performance.

**Example:** Task completion time (seconds)

---

### 16. `primary_outcome_value` — Free text

**Definition:** The numeric result for the primary outcome metric.

**Instruction:** Include units, confidence intervals, or p-values if reported. Copy the exact numeric reporting.

**Example:** 142.3 ± 28.1 seconds (p < 0.001)

---

### 17. `comparison_to_human` — Free text

**Definition:** If the paper compares autonomous performance to human/manual performance, report the comparison result.

**Instruction:** Include both values if available. Enter NR if no comparison was made.

**Example:** Autonomous: 142s vs Manual: 198s (p=0.003)

---

### 18. `secondary_outcomes` — Free text *(NEW)*

**Definition:** Additional quantitative outcomes reported beyond the primary.

**Instruction:** Record as semicolon-separated entries in format "metric: value." Enter NR if only one outcome reported. These may be excluded from analysis if not meaningful.

**Example:** Accuracy: 94.2% ± 3.1%; Force applied: 2.3 ± 0.8 N; Success rate: 18/20

---

## TIER 4 — JUDGMENT (2 fields)

Expected AI-Human κ 0.50–0.70 (by design). Source quotes **MANDATORY**.

---

### 19. `key_limitation` — Free text

**Definition:** The most important limitation of the study.

**Instruction:** In 1–2 sentences, state the key limitation using YOUR judgment. Do not simply copy what the authors wrote in their limitations section. Quote the passage(s) that informed your assessment.

**Example:** Small sample size (n=8) limits generalizability, and the phantom model does not replicate tissue variability seen in live surgery.

---

### 20. `clinical_readiness_assessment` — Categorical

**Definition:** Your judgment of how close the described technology is to clinical use in patients.

**Valid values:** Ready for clinical use, Approaching clinical readiness, Early-stage research, Proof of concept only, Not assessable

**Instruction:** Synthesize the results, limitations, and validation setting to make this judgment. There is no right answer in the text — this requires your assessment. Quote the key evidence behind your judgment.

**Example:** Early-stage research

---

## CONCORDANCE SCORING NOTES (Paper 1)

**Categorical fields:** Exact match scoring. Cohen's κ computed per field.

**Free-text fields** (`robot_platform`, `task_performed`, `primary_outcome_metric`, `primary_outcome_value`, `comparison_to_human`, `secondary_outcomes`, `key_limitation`): Semantic equivalence classes adjudicated manually or via embedding similarity. "Prospective single-arm feasibility study" and "single-arm prospective feasibility trial" are scored as agreement.

**Task specification sub-fields:** Each scored independently as a 3-category (H/R/Shared) forced choice, excluding NR pairs.

---

## SYNTHESIS METHOD (Paper 2)

Narrative synthesis with structured tabulation, following the SWiM (Synthesis Without Meta-analysis) reporting guideline. Meta-analysis is inappropriate due to extreme heterogeneity in interventions, outcomes, study designs, and validation settings across the surgical autonomy literature. Tables will be organized by autonomy level and surgical domain, with narrative synthesis of findings within each stratum.

---

## REVISION HISTORY

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | Feb 2026 | Original 15-field schema |
| 2.0 | Mar 13, 2026 | 20-field schema. `country` demoted to Tier 2, replaced by `surgical_domain` in Tier 1. `fda_status` replaced by `system_maturity`. `human_oversight_model` replaced by 4 task specification sub-fields (monitor/generate/select/execute). `study_design` converted from free text to categorical. `secondary_outcomes` added. Concordance scoring and synthesis method specified. Autonomy level decision tree added. |
