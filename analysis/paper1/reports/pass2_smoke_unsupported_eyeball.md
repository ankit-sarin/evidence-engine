# Pass 2 smoke — UNSUPPORTED verdicts (28)

**Run:** `surgical_autonomy_pass2_smoke_20260421T122916Z`

One block per UNSUPPORTED verdict. `value` is what the arm extracted; `span` is what Gemma quoted from the source (null = nothing quotable found); `reasoning` is why Gemma ruled UNSUPPORTED; `hypothesis` is Gemma's guess at the fabrication mode.

## 1. paper 9 / `robot_platform` / **local**

- **value:** Smart Tissue Autonomous Robot (STAR); KUKA LBR Med
- **span:** _(null)_
- **reasoning:** The source does not mention "Smart Tissue Autonomous Robot (STAR); KUKA LBR Med" as a single platform. It mentions both STAR and KUKA LBR Med robots separately.
- **hypothesis:** The arm likely conflated the system name (STAR) with the robot manufacturer (KUKA).

## 2. paper 9 / `sample_size` / **anthropic_sonnet_4_6**

- **value:** 18
- **span:** _(null)_
- **reasoning:** The text mentions study groups of n=5, n=4, and n=4, but does not provide a total sample size of 18. The provided span only lists the sizes of individual groups, not their sum.
- **hypothesis:** The arm likely summed the sizes of the three groups, assuming they were additive, which is not stated in the text.

## 3. paper 9 / `sample_size` / **local**

- **value:** N/A
- **span:** _(null)_
- **reasoning:** The source text does not mention any sample size, and the arm provides no supporting span. The claim of 'N/A' is not grounded in the provided text.
- **hypothesis:** The arm defaulted to 'N/A' because it could not find a single, overall sample size in the text.

## 4. paper 9 / `sample_size` / **openai_o4_mini_high**

- **value:** 9
- **span:** _(null)_
- **reasoning:** The text mentions study groups of n=5, n=4, and n=4, but does not provide a total sample size of 9. The provided span only lists the sizes of individual groups, not their sum.
- **hypothesis:** The arm likely summed the sizes of the first two groups, assuming they were additive, which is not stated in the text.

## 5. paper 9 / `study_design` / **local**

- **value:** Feasibility study
- **span:** _(null)_
- **reasoning:** The source does not mention a 'feasibility study'. It describes a demonstration of a system, but doesn't categorize it as such. The provided span only states the system's effectiveness and safety, not the study's design.
- **hypothesis:** Plausible-sounding default study design based on the early stage of research.

## 6. paper 9 / `system_maturity` / **local**

- **value:** Research prototype (hardware)
- **span:** _(null)_
- **reasoning:** The source does not mention a custom-built research robot named 'STAR'. It describes STAR as a system *using* a KUKA robot, not *being* a robot itself.
- **hypothesis:** The system conflated the name of the overall system (STAR) with the hardware it uses (KUKA robot).

## 7. paper 12 / `robot_platform` / **local**

- **value:** da Vinci Surgical System; da Vinci Research Kit (dVRK); Asynchronous Multi-Body Framework (AMBF)
- **span:** _(null)_
- **reasoning:** The source does not mention 'Asynchronous Multi-Body Framework (AMBF)' in relation to the robotic platform. It only mentions AMBF as the framework used for the simulation itself, not as a component of the robot.
- **hypothesis:** conflation with adjacent field

## 8. paper 17 / `task_performed` / **local**

- **value:** exchanging a red ring between robotic tools
- **span:** _(null)_
- **reasoning:** The source describes exchanging a ring, but not specifically 'exchanging a red ring between robotic tools'. The color is mentioned in the context of the ring itself, but not as part of the task description.
- **hypothesis:** The arm added detail (color, specific tools) to create a more descriptive task, even though it wasn't explicitly stated.

## 9. paper 39 / `autonomy_level` / **local**

- **value:** 3
- **span:** _(null)_
- **reasoning:** The source does not mention any specific autonomy level. The supporting span only discusses the need for innovation in valve deployment.
- **hypothesis:** Plausible-sounding default value assigned without source support.

## 10. paper 67 / `sample_size` / **anthropic_sonnet_4_6**

- **value:** 1800
- **span:** _(null)_
- **reasoning:** The source states the training set contained 1360 images and the testing set contained 440 images. The extracted value of 1800 is not mentioned in the text.
- **hypothesis:** The value appears to be a sum of the training and testing set sizes, but is presented as a single sample size.

## 11. paper 67 / `sample_size` / **local**

- **value:** NR
- **span:** _(null)_
- **reasoning:** The source mentions the CPU and GPU used for training and testing, but does not provide a sample size. The value 'NR' is not found in the text.
- **hypothesis:** The arm likely defaulted to 'NR' because it could not find a numerical value for sample size in the provided span.

## 12. paper 295 / `sample_size` / **openai_o4_mini_high**

- **value:** NOT_FOUND
- **span:** _(null)_
- **reasoning:** The source does not mention a sample size. The arm reports 'NOT_FOUND' but provides no supporting span, indicating a lack of evidence in the text.
- **hypothesis:** Plausible-sounding default response for missing data.

## 13. paper 383 / `sample_size` / **anthropic_sonnet_4_6**

- **value:** 19
- **span:** _(null)_
- **reasoning:** The source mentions a single Yorkshire piglet (N=1) but does not state a total sample size of 19. The claim appears to conflate the piglet with the other sample sizes mentioned earlier in the text.
- **hypothesis:** conflation with adjacent field

## 14. paper 383 / `sample_size` / **local**

- **value:** 19
- **span:** _(null)_
- **reasoning:** The source does not mention a sample size of 19. The text discusses experiments but does not provide a total number of cases or subjects equal to 19.
- **hypothesis:** plausible-sounding default

## 15. paper 407 / `comparison_to_human` / **local**

- **value:** Autonomous: 34.8s vs Manual: 28.3s; Autonomous: 45.2s vs Manual: 31.6s
- **span:** _(null)_
- **reasoning:** The source only provides operating times for incision #1. The claim of '45.2s vs Manual: 31.6s' is not found in the text, and appears to be a fabrication or conflation of data from different trials or conditions.
- **hypothesis:** hallucinated phrasing

## 16. paper 411 / `sample_size` / **anthropic_sonnet_4_6**

- **value:** 30
- **span:** _(null)_
- **reasoning:** The source states 10 tests were conducted, not 30. This appears to be a simple multiplication error.
- **hypothesis:** multiplication of the correct value by a factor of three

## 17. paper 411 / `sample_size` / **openai_o4_mini_high**

- **value:** 30
- **span:** _(null)_
- **reasoning:** The source states 10 tests were conducted, not 30. This appears to be a simple multiplication error.
- **hypothesis:** multiplication of the correct value by a factor of three

## 18. paper 458 / `validation_setting` / **local**

- **value:** Ex vivo
- **span:** _(null)_
- **reasoning:** The source text does not mention 'Ex vivo' as a validation setting. The claim appears to be a default or unrelated value.
- **hypothesis:** plausible-sounding default

## 19. paper 478 / `sample_size` / **anthropic_sonnet_4_6**

- **value:** 140
- **span:** _(null)_
- **reasoning:** The source only mentions 80 trials of the surgical debridement subtask and 20 trials of the MTS subtask. The claim of 140 trials is a combination of these two values, but the text does not support combining them to arrive at this number.
- **hypothesis:** summation of unrelated values

## 20. paper 557 / `primary_outcome_value` / **local**

- **value:** NR
- **span:** _(null)_
- **reasoning:** The source states that the exact numeric results are not provided, directly contradicting the 'NR' value. The arm correctly identifies the lack of numeric results but reports it as a value.
- **hypothesis:** Plausible-sounding default; 'NR' is often used to indicate 'not reported'.

## 21. paper 570 / `secondary_outcomes` / **local**

- **value:** Tissue tracking error: Translation Error: 0.711 ± 0.315 mm; Rotation Error: 1.383 ± 0.711 degrees; Ultrasound stability: Normalised Cross-Correlation (NCC) score comparison with and without motion compensation.
- **span:** _(null)_
- **reasoning:** The source does not mention 'Tissue tracking error: Translation Error: 0.711 ± 0.315 mm; Rotation Error: 1.383 ± 0.711 degrees; Ultrasound stability: Normalised Cross-Correlation (NCC) score comparison with and without motion compensation.'
- **hypothesis:** The arm likely conflated values from different tables or sections of the paper.

## 22. paper 693 / `sample_size` / **anthropic_sonnet_4_6**

- **value:** 31
- **span:** _(null)_
- **reasoning:** The source mentions 16 test instances in one sentence, but does not state a total sample size of 31. This value appears to be fabricated or conflated with another value.
- **hypothesis:** plausible-sounding default

## 23. paper 719 / `autonomy_level` / **local**

- **value:** _(empty / not emitted)_
- **span:** _(null)_
- **reasoning:** The source does not mention any absence of autonomy or a 'NR' value. The arm provides no supporting span.
- **hypothesis:** Plausible-sounding default; the arm defaulted to 'absent / NR' when no autonomy level was present.

## 24. paper 719 / `autonomy_level` / **openai_o4_mini_high**

- **value:** NOT_FOUND
- **span:** _(null)_
- **reasoning:** The source does not mention any level of autonomy. The arm provides no supporting span.
- **hypothesis:** Plausible-sounding default; the arm defaulted to 'NOT_FOUND' when no autonomy level was present.

## 25. paper 719 / `key_limitation` / **local**

- **value:** _(empty / not emitted)_
- **span:** _(null)_
- **reasoning:** The source does not mention any limitation. The arm provides no supporting span.
- **hypothesis:** Plausible-sounding default response indicating absence of information.

## 26. paper 719 / `key_limitation` / **openai_o4_mini_high**

- **value:** NOT_FOUND
- **span:** _(null)_
- **reasoning:** The source does not mention any limitation. The arm provides no supporting span.
- **hypothesis:** Plausible-sounding default response indicating absence of information.

## 27. paper 738 / `validation_setting` / **anthropic_sonnet_4_6**

- **value:** Computational/Virtual
- **span:** _(null)_
- **reasoning:** The provided span is truncated and does not mention 'Computational/Virtual'. The text describes an ex-vivo phantom experiment, not a computational or virtual one. The span is also not found in the source.
- **hypothesis:** The arm likely conflated the computational aspects of the cognitive engine with the experimental setting.

## 28. paper 755 / `sample_size` / **anthropic_sonnet_4_6**

- **value:** 200
- **span:** _(null)_
- **reasoning:** The text mentions 50 experiments per method for a total of 150, and 5 participants, but does not state a total sample size of 200. The claimed span is a conflation of experiment counts and participant numbers.
- **hypothesis:** The arm likely added the experiment counts and participant counts to arrive at 200.
