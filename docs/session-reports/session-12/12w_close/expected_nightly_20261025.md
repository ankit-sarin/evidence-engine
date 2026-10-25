# Expected values for the 2026-10-25 09:00 UTC nightly

The tree at this closeout: HEAD the 12w closeout commit (child of the item-0 commit `f7a16f3`), whose engine tree equals `e81d699`'s (the U17 fix). Session 12w's one engine commit is `e81d699` (U17); every later commit touches `docs/` only (`git diff --stat e81d699 HEAD -- engine tests scripts analysis review_specs` is empty). The values are therefore the 12w-U17B gate's (run 2) on the same code (3,375 passed / 1 xfailed / 17 deselected, collected 3,393, selected 3,376, exit 0; `12w_U17B/gate_12w_u17b/durations_12w_u17b.txt`, copied here as `durations_12w_u17b.txt`), as R705(a) accepts them. Written at 12w-CLOSE step 2h, before the nightly ran; 12x-OPEN checks the log against this table (R707(h)).

| Value | Expected | Where to read it |
| --- | --- | --- |
| Collection line | `collected 3393 items / 17 deselected / 3376 selected` | `logs/nightly_test_20261025.log` |
| Summary line | `3375 passed, 17 deselected, 1 xfailed, 2 warnings` | the same log |
| Exit-code line | `pytest exit code: 0`, the log's last line | the same log |
| Durations file | present, one line per test phase | `logs/nightly_durations_20261025.txt` |
| Ollama journal, 09:00–09:25 UTC | no entries (a read-only request is a reading, R696(b); a model load is not) | `journalctl -u ollama --utc` |
| The real lock file's mtime | unchanged (2026-07-27 23:22:08 UTC) | `stat` only |

The collected set is `12w_close/collected_ids_12w_close.txt` (3,376 ids: `12v_close/collected_ids_12v_close.txt` plus the 17 U17 ids: the 13 of `tests/test_u17_cross_lineage.py` and the 4 run-fault parametrisations, R705(b)). The two warnings are sklearn's, from `tests/test_kappa.py::test_both_raters_constant_and_identical_is_undefined_not_one` (R641(c)).
