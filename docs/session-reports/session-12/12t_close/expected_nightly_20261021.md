# Expected values for the 2026-10-21 09:00 UTC nightly

The tree at the 12t closeout: the code is `9cfd726`'s (the U15 fix). Session 12t's only engine commit is `9cfd726`; every later 12t commit touches `docs/` only (`git diff --stat 9cfd726 HEAD -- engine tests scripts analysis review_specs` is empty at this closeout). The values are therefore the 12t-U15B gate's on the same code (3,342 passed / 1 xfailed / 17 deselected, collected 3,343 of 3,360, exit 0; `12t_close/durations_12t_u15b.txt`). Predictions until the run is read; checking it is part of the 12u startup verify.

| Value | Expected | Where to read it |
| --- | --- | --- |
| Collection line | `collected 3360 items / 17 deselected / 3343 selected` | `logs/nightly_test_20261021.log` |
| Summary line | `3342 passed, 17 deselected, 1 xfailed, 2 warnings` | the same log |
| Exit-code line | `pytest exit code: 0`, the log's last line | the same log |
| Durations file | present, one line per test phase | `logs/nightly_durations_20261021.txt` |
| Ollama journal, 09:00–09:25 UTC | no entries | `journalctl -u ollama --utc` |
| The real lock file's mtime | unchanged (2026-07-27 23:22:08 UTC) | `stat` only |

The counts hold only if no further commit changes the collected set before 09:00 UTC on 2026-10-21. The collected set is `12t_close/collected_ids_12t_close.txt` (3,343 ids: `12s_close/collected_ids_12s_close.txt` plus the 12 ids of `12t_U15B/expected_new_ids.txt`), unchanged since `9cfd726`. The two warnings are sklearn's, from `tests/test_kappa.py::test_both_raters_constant_and_identical_is_undefined_not_one` (R641(c)). The nightly's basetemp lands in `/tmp/pytest-of-ankitsarin` beside those already there (at this closeout `pytest-14`, the 2026-10-20 nightly's, `pytest-15`, the 12t-OPEN-R1 gate's, and `pytest-16`, the 12t-U15B gate's; `12t_close/temp_root_close.txt`); its entry count and size are recorded at the 12u open, not matched (R647(c)).

The 07:00 UTC health job on 2026-10-21 runs on the HTTP design (every installed model loaded at `keep_alive` 0). It is not a nightly finding; 12u-OPEN reads its log and journal window for its loads, their outcome and any NVRM line, as readings (R663(d), R670(f)). At this closeout nothing is resident, and the kernel journal holds six `NV_ERR_NO_MEMORY` lines this boot.
