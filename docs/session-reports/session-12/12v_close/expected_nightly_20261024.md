# Expected values for the 2026-10-24 09:00 UTC nightly

The tree at this closeout: HEAD the 12v closeout commit (child of the item-0 commit `4738ebf`), whose engine tree equals `731ebf2`'s (the U38 fix). Session 12v's engine commits are `62d5e7e` (U37) and `731ebf2` (U38); every later commit touches `docs/` only (`git diff --stat 731ebf2 HEAD -- engine tests scripts analysis review_specs` is empty). The values are therefore the 12v-U38B gate's on the same code (3,358 passed / 1 xfailed / 17 deselected, collected 3,376, selected 3,359, exit 0; `12v_U38B/gate_12v_u38b/durations_12v_u38b.txt`, copied here as `durations_12v_u38b.txt`), as R699(d) and R700(a) state them. Written at 12v-CLOSE step 2h, before the nightly ran; 12w-OPEN checks the log against this table (R700(g)).

| Value | Expected | Where to read it |
| --- | --- | --- |
| Collection line | `collected 3376 items / 17 deselected / 3359 selected` | `logs/nightly_test_20261024.log` |
| Summary line | `3358 passed, 17 deselected, 1 xfailed, 2 warnings` | the same log |
| Exit-code line | `pytest exit code: 0`, the log's last line | the same log |
| Durations file | present, one line per test phase | `logs/nightly_durations_20261024.txt` |
| Ollama journal, 09:00–09:25 UTC | no entries (a read-only request is a reading, R696(b); a model load is not) | `journalctl -u ollama --utc` |
| The real lock file's mtime | unchanged (2026-07-27 23:22:08 UTC) | `stat` only |

The collected set is `12v_close/collected_ids_12v_close.txt` (3,359 ids: `12u_U16B/collected_ids_12u_u16c.txt` plus the five U37 ids and the six U38 ids). The two warnings are sklearn's, from `tests/test_kappa.py::test_both_raters_constant_and_identical_is_undefined_not_one` (R641(c)).
