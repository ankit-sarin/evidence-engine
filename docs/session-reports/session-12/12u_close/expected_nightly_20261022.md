# Expected values for the 2026-10-22 09:00 UTC nightly

The tree at the 12u-U16C read-out: HEAD `2579f7c`, whose engine tree equals `2c3822b`'s (the U16 fix). Session 12u's only engine commit is `2c3822b`; `2579f7c` touches `docs/` only (`git diff --stat 2c3822b 2579f7c -- engine tests scripts analysis review_specs` is empty). The values are therefore the 12u-U16C gate's on the same code (3,347 passed / 1 xfailed / 17 deselected, collected 3,365, selected 3,348, exit 0; `12u_U16B/gate_12u_u16c/durations_12u_u16c.txt`), as R691(g) states them. Written at 12u-CLOSE step R2, before the nightly log was read; checking the log against this table is that step.

| Value | Expected | Where to read it |
| --- | --- | --- |
| Collection line | `collected 3365 items / 17 deselected / 3348 selected` | `logs/nightly_test_20261022.log` |
| Summary line | `3347 passed, 17 deselected, 1 xfailed, 2 warnings` | the same log |
| Exit-code line | `pytest exit code: 0`, the log's last line | the same log |
| Durations file | present, one line per test phase | `logs/nightly_durations_20261022.txt` |
| Ollama journal, 09:00–09:25 UTC | no entries | `journalctl -u ollama --utc` |
| The real lock file's mtime | unchanged (2026-07-27 23:22:08 UTC) | `stat` only |

The collected set is `12u_U16B/collected_ids_12u_u16c.txt` (3,348 ids: `12t_close/collected_ids_12t_close.txt` plus the five ids of `12u_U16B/expected_new_ids.txt`). The two warnings are sklearn's, from `tests/test_kappa.py::test_both_raters_constant_and_identical_is_undefined_not_one` (R641(c)).
