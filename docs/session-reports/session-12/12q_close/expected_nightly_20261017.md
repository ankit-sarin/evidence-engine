# Expected values for the 2026-10-17 09:00 UTC nightly

The tree at the 12q closeout: the code is `b3f0072`'s. Session 12q's one code commit, `b3f0072` (the INT-p12-2 fix: `engine/agents/extractor.py`, one line, and one test added to `tests/test_duplicate_on_pass1_attempt_2.py`), carried its own gate; the commits after it (`ddfd1cc` and this closeout) touch `docs/` only, and `git diff --stat b3f0072 HEAD -- engine tests scripts analysis review_specs` is empty (measured at 12q-CLOSE step 0, I1). The values are therefore that gate's (12q-P12-2B: 3,306 passed / 1 xfailed / 17 deselected, collected 3,307, exit 0), with the no-marker collection one higher than 12p's (3,324). This is the first nightly on the b3f0072 tree and the first on the upgraded host (R657(g)). Predictions until the run is read; checking it is part of the 12r startup verify (R661(c)).

| Value | Expected | Where to read it |
| --- | --- | --- |
| Collection line | `collected 3324 items / 17 deselected / 3307 selected` | `logs/nightly_test_20261017.log` |
| Summary line | `3306 passed, 17 deselected, 1 xfailed, 2 warnings` | the same log |
| Exit-code line | `pytest exit code: 0`, the log's last line | the same log |
| Durations file | present, one line per test phase | `logs/nightly_durations_20261017.txt` |
| Ollama journal, 09:00–09:25 UTC | no entries | `journalctl -u ollama --utc` |
| The real lock file's mtime | unchanged (2026-07-27 23:22:08 UTC) | `stat` only |

The counts hold only if no further commit changes the collected set before 09:00 UTC on 2026-10-17. The collected set is `12q_close/collected_ids_12q_close.txt` (3,307 ids; 12p's set plus `tests/test_duplicate_on_pass1_attempt_2.py::test_a_duplicate_on_attempt_1_is_named_in_every_retry_row`). The two warnings are sklearn's, from `tests/test_kappa.py::test_both_raters_constant_and_identical_is_undefined_not_one` (R641(c)). The nightly's basetemp lands in `/tmp/pytest-of-ankitsarin` beside those already there (at this closeout: `pytest-2`, `pytest-3` and `pytest-4`, all this lane's, `12q_close/temp_root_close.txt`); its entry count and size are recorded at the 12r open, not matched (R647(c)).

The 07:00 UTC health job's model loads on 2026-10-17 are expected and are not a nightly finding; they are the first full model load on the upgraded host, and 12r-OPEN reads and reports them (the loads and their outcome in the Ollama journal) before U13's launch (R657(g), R661(c)).
