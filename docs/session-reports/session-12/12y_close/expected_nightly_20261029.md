# Expected values for the 2026-10-29 09:00 UTC nightly

The tree at this closeout: HEAD the 12y closeout commit (child of the item-0 commit `0f83f46`), whose engine tree equals `32fc09e`'s (U18 commit 2). Session 12y's two engine commits are `e7425b3` (U18 commit 1, one Ollama host) and `32fc09e` (U18 commit 2, the digest re-checked per call); every later commit touches `docs/` only (`git diff --stat 32fc09e HEAD -- engine tests scripts analysis review_specs` is empty). The values are therefore the 12y-U18C gate's on the same code (3,411 passed / 1 xfailed / 17 deselected, collected 3,429, selected 3,412, exit 0; `12y_U18C/gate_round1/durations_12y_u18c.txt`, copied here as `durations_12y_u18c.txt`). Written at 12y-CLOSE step 2a, before the nightly ran; 12z-OPEN checks the log against this table.

| Value | Expected | Where to read it |
| --- | --- | --- |
| Collection line | `collected 3429 items / 17 deselected / 3412 selected` | `logs/nightly_test_20261029.log` |
| Summary line | `3411 passed, 17 deselected, 1 xfailed, 2 warnings` | the same log |
| Exit-code line | `pytest exit code: 0`, the log's last line | the same log |
| Durations file | present, one line per test phase | `logs/nightly_durations_20261029.txt` |
| Ollama journal, 09:00–09:25 UTC | no entries (a read-only request is a reading, R696(b); a model load is not) | `journalctl -u ollama --utc` |
| The real lock file's mtime | unchanged (2026-07-27 23:22:08 UTC) | `stat` only |

The collected set is `12y_close/collected_ids_12y_close.txt` (3,412 ids: `12y_U18C/collected_ids_12y_u18c.txt`, the 32fc09e gate's list, which is `12y_U18B/collected_ids_12y_u18b.txt` plus the 10 ids of `tests/test_u18_digest_per_call.py` and the two `[ModelDigestChanged]` ids of `tests/test_run_fault_predicate.py`). The two warnings are sklearn's, from `tests/test_kappa.py::test_both_raters_constant_and_identical_is_undefined_not_one` (R641(c)).
