# 12s-U14B — expected gate values, written 2026-10-19T17:10:53Z before the gate launches

Tree: `7074305160eb781f9863eb141d0b46af0f3567e1` (the U14 fix). Derived from the collected-id list
(`pytest --collect-only -m "not network and not ollama and not integration"`), compared as a set with
`12q_close/collected_ids_12q_close.txt` (R655(b)): only-now **24** ids (`expected_new_ids.txt`, listed
below), only-ref **0** (the pin test `test_the_payload_carries_what_r17_names` is rewritten under its id).

| value | expected |
| --- | --- |
| passed | 3306 + 24 − 0 = **3330** |
| xfailed | **1** |
| deselected | **17** (no new test carries a deselected marker) |
| collected (selected) | 3,307 + 24 = **3,331** (`3331/3348 tests collected (17 deselected)`) |
| no-marker collection | 3,324 + 24 = **3,348** |
| warnings | **2**, sklearn's, from `tests/test_kappa.py::test_both_raters_constant_and_identical_is_undefined_not_one` |
| tests/test_api_parity.py ids | 69 now, 69 in the reference: **delta 0** (I5) |
| inventory --check | **in sync** (run at the fix, before the commit: `inventory in sync`); the inventory records no line number in the two engine files that moved |
| only-gate | exactly the 24 ids below |
| only-ref | **empty** |
| wall time | a reading beside 868.02, 871.59, 906.50, 858.50 s (R674(c)); concurrent jobs named at launch |

New ids (24):

```text
tests/test_audit_events.py::test_u14_every_writer_carries_locator_1_1
tests/test_auditor.py::test_u14_audit_span_does_not_invalidate_a_verbatim_quote_containing_an_ellipsis
tests/test_locator.py::test_u14_a_fuzzy_fixture_of_300_or_more_characters_is_located
tests/test_locator.py::test_u14_a_fuzzy_match_keeps_bridged_only_when_the_window_lacks_the_ellipsis
tests/test_locator.py::test_u14_a_long_snippet_with_no_counterpart_stays_not_located
tests/test_locator.py::test_u14_a_long_unchanged_quote_is_still_exact
tests/test_locator.py::test_u14_a_quote_without_an_ellipsis_is_unchanged
tests/test_locator.py::test_u14_a_seeded_30_edit_subset_of_the_sweep_is_all_located
tests/test_locator.py::test_u14_a_snippet_bridging_passages_with_an_ellipsis_is_still_bridged
tests/test_locator.py::test_u14_a_verbatim_quote_containing_an_ellipsis_is_not_bridged[...-The protocol was revised twice... Recruitment closed in March.]
tests/test_locator.py::test_u14_a_verbatim_quote_containing_an_ellipsis_is_not_bridged[\u2026-Patients were followed for twelve months\u2026 Two were lost to follow-up.]
tests/test_locator.py::test_u14_below_200_characters_the_score_is_unchanged[We used the robotic platform to suture the small bowel in eleven adult pigs. The arm held a curved needle with plaited nylon thread. Each knot was-0.9658]
tests/test_locator.py::test_u14_below_200_characters_the_score_is_unchanged[We used the robotic platform to suture the small bowel in twelve adult pigs. The zqrm held a curved needle with braided nylon thread. Each knot was-None]
tests/test_locator.py::test_u14_every_pre_existing_fuzzy_fixture_is_unchanged[The trial enrolled forty patients at two centres. The robot performed autonomous suturing on porcine tissue in ten trials. Outcomes were measured at six months.-Conducted in Norway by the authors of this study.-False-0.3551]
tests/test_locator.py::test_u14_every_pre_existing_fuzzy_fixture_is_unchanged[The trial enrolled forty patients at two centres. The robot performed autonomous suturing on porcine tissue in ten trials. Outcomes were measured at six months.-Conducted in Norway.-False-0.4242]
tests/test_locator.py::test_u14_every_pre_existing_fuzzy_fixture_is_unchanged[The trial enrolled forty patients at two centres. The robot performed autonomous suturing on porcine tissue in ten trials. Outcomes were measured at six months.-Table.I shows-False-0.3846]
tests/test_locator.py::test_u14_every_pre_existing_fuzzy_fixture_is_unchanged[The trial enrolled forty patients at two centres. The robot performed autonomous suturing on porcine tissue in ten trials. Outcomes were measured at six months.-The robot performd autonomous suturing on porcine tisue in ten trials.-True-0.9859]
tests/test_locator.py::test_u14_every_pre_existing_fuzzy_fixture_is_unchanged[The trial enrolled forty patients at two centres. The robot performed autonomous suturing on porcine tissue in ten trials. Outcomes were measured at six months.-outcomes were measured at six month-True-1.0]
tests/test_locator.py::test_u14_every_pre_existing_fuzzy_fixture_is_unchanged[This study evaluates the Smart Tissue Autonomous Robot (STAR) system for autonomous suturing. A randomized controlled trial was conducted with 20 trials on porcine tissue. The system achieved Level 3 autonomy per the Yang et al. taxonomy. Suture placement accuracy was 95.2% compared to 91.0% for expert surgeons. No tissue damage events were reported during autonomous operation.-20 trials on porcme tissue-True-0.9259]
tests/test_locator.py::test_u14_every_pre_existing_fuzzy_fixture_is_unchanged[This study evaluates the Smart Tissue Autonomous Robot (STAR) system for autonomous suturing. A randomized controlled trial was conducted with 20 trials on porcine tissue. The system achieved Level 3 autonomy per the Yang et al. taxonomy. Suture placement accuracy was 95.2% compared to 91.0% for expert surgeons. No tissue damage events were reported during autonomous operation.-500 experiments on synthetic phantoms with laser guidance-False-0.3717]
tests/test_locator.py::test_u14_every_pre_existing_fuzzy_fixture_is_unchanged[This study evaluates the Smart Tissue Autonomous Robot (STAR) system for autonomous suturing. A randomized controlled trial was conducted with 20 trials on porcine tissue. The system achieved Level 3 autonomy per the Yang et al. taxonomy. Suture placement accuracy was 95.2% compared to 91.0% for expert surgeons. No tissue damage events were reported during autonomous operation.-Cooking temperature affects protein denaturation-False-0.3678]
tests/test_locator.py::test_u14_every_pre_existing_fuzzy_fixture_is_unchanged[This study evaluates the Smart Tissue Autonomous Robot (STAR) system for autonomous suturing. A randomized controlled trial was conducted with 20 trials on porcine tissue. The system achieved Level 3 autonomy per the Yang et al. taxonomy. Suture placement accuracy was 95.2% compared to 91.0% for expert surgeons. No tissue damage events were reported during autonomous operation.-The experiment was performed on cadaveric specimens-False-0.4211]
tests/test_locator.py::test_u14_every_pre_existing_fuzzy_fixture_is_unchanged[This study evaluates the Smart Tissue Autonomous Robot (STAR) system for autonomous suturing. A randomized controlled trial was conducted with 20 trials on porcine tissue. The system achieved Level 3 autonomy per the Yang et al. taxonomy. Suture placement accuracy was 95.2% compared to 91.0% for expert surgeons. No tissue damage events were reported during autonomous operation.-This snippet does not exist in the paper text at all.-False-0.4]
tests/test_locator.py::test_u14_the_worst_draw_of_the_sweep_is_located
```
