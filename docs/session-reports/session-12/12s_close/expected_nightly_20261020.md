# Expected values for the 2026-10-20 09:00 UTC nightly

The tree at the 12s closeout: the code is `7074305`'s (the U14 fix). Session 12s's only engine commit is `7074305`; every later 12s commit touches `docs/` only (`git diff --stat 8b2c388 HEAD -- engine tests scripts analysis review_specs` is empty at this closeout). The values are therefore the 12s-U14B gate's on the same code (3,330 passed / 1 xfailed / 17 deselected, collected 3,331 of 3,348, exit 0; `12s_close/durations_12s_u14b.txt`). Predictions until the run is read; checking it is part of the 12t startup verify.

| Value | Expected | Where to read it |
| --- | --- | --- |
| Collection line | `collected 3348 items / 17 deselected / 3331 selected` | `logs/nightly_test_20261020.log` |
| Summary line | `3330 passed, 17 deselected, 1 xfailed, 2 warnings` | the same log |
| Exit-code line | `pytest exit code: 0`, the log's last line | the same log |
| Durations file | present, one line per test phase | `logs/nightly_durations_20261020.txt` |
| Ollama journal, 09:00–09:25 UTC | no entries | `journalctl -u ollama --utc` |
| The real lock file's mtime | unchanged (2026-07-27 23:22:08 UTC) | `stat` only |

The counts hold only if no further commit changes the collected set before 09:00 UTC on 2026-10-20. The collected set is `12s_close/collected_ids_12s_close.txt` (3,331 ids: `12q_close/collected_ids_12q_close.txt` plus the 24 ids of `12s_U14B/expected_new_ids.txt`), unchanged since `7074305`. The two warnings are sklearn's, from `tests/test_kappa.py::test_both_raters_constant_and_identical_is_undefined_not_one` (R641(c)). The nightly's basetemp lands in `/tmp/pytest-of-ankitsarin` beside those already there (at this closeout `pytest-11` and `pytest-12`, this lane's ad-hoc 12s-U14B runs, and `pytest-13`, the 12s-U14B gate's; `12s_close/temp_root_close.txt`); its entry count and size are recorded at the 12t open, not matched (R647(c)).

The 07:00 UTC health job on 2026-10-20 runs on the HTTP design (every installed model loaded at `keep_alive` 0). It is not a nightly finding; 12t-OPEN reads its log and journal window for its loads, their outcome and any NVRM line, as readings (R663(d), R670(f)). At this closeout nothing is resident.
