# Expected values for the 2026-10-27 09:00 UTC nightly

The tree at this closeout: HEAD the 12x closeout commit (child of the item-0 commit `2b11a57`), whose engine tree equals `f3f552e`'s (the U39 audit extension). Session 12x's two test commits are `1ebb3ae` (U39) and `f3f552e` (U39's audit extension); every later commit touches `docs/` only (`git diff --stat f3f552e HEAD -- engine tests scripts analysis review_specs` is empty). The values are therefore the 12x-U39C gate's on the same code (3,388 passed / 1 xfailed / 17 deselected, collected 3,406, selected 3,389, exit 0; `12x_U39C/gate/durations_12x_u39c.txt`, copied here as `durations_12x_u39c.txt`), and the 2026-10-26 nightly on `f3f552e` reproduced them (`3388 passed, 17 deselected, 1 xfailed, 2 warnings in 894.72s`, ids equal to the gate's). Written at 12x-CLOSE step 3j, before the nightly ran; 12y-OPEN checks the log against this table.

| Value | Expected | Where to read it |
| --- | --- | --- |
| Collection line | `collected 3406 items / 17 deselected / 3389 selected` | `logs/nightly_test_20261027.log` |
| Summary line | `3388 passed, 17 deselected, 1 xfailed, 2 warnings` | the same log |
| Exit-code line | `pytest exit code: 0`, the log's last line | the same log |
| Durations file | present, one line per test phase | `logs/nightly_durations_20261027.txt` |
| Ollama journal, 09:00–09:25 UTC | no entries (a read-only request is a reading, R696(b); a model load is not) | `journalctl -u ollama --utc` |
| The real lock file's mtime | unchanged (2026-07-27 23:22:08 UTC) | `stat` only |

The collected set is `12x_close/collected_ids_12x_close.txt` (3,389 ids: `12x_U39C/collected_ids_12x_u39c.txt`, the f3f552e gate's list, which is `12w_close/collected_ids_12w_close.txt` plus the 13 ids of `tests/test_u39_runtime_equals_hash.py`). The two warnings are sklearn's, from `tests/test_kappa.py::test_both_raters_constant_and_identical_is_undefined_not_one` (R641(c)).
