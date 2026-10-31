# Expected values for the 2026-10-31 09:00 UTC nightly

The tree at this writing: HEAD the 12aa-SETUP tools commit or its item-0 child, whose engine tree equals `bc2ae19`'s (U20 commit 2). Session 12aa's two engine commits are `ca29906` (U20 commit 1, the Modelfile `num_ctx` term) and `bc2ae19` (U20 commit 2, the loaded context confirmed after each completed call); every later commit touches `docs/` only (`git diff --stat bc2ae19 HEAD -- engine tests scripts analysis review_specs` is empty). The values are therefore the 12aa-U20C commit-2 gate's on the same code (3,474 passed / 1 xfailed / 17 deselected, collected 3,492, selected 3,475, exit 0; `12aa_U20C/gate_c2/durations_12aa_u20c_c2.txt`). Written at 12aa-SETUP step 3, before the nightly ran; the next open checks the log against this table.

| Value | Expected | Where to read it |
| --- | --- | --- |
| Collection line | `collected 3492 items / 17 deselected / 3475 selected` | `logs/nightly_test_20261031.log` |
| Summary line | `3474 passed, 17 deselected, 1 xfailed, 2 warnings` | the same log |
| Exit-code line | `pytest exit code: 0`, the log's last line | the same log |
| Durations file | present, one line per test phase | `logs/nightly_durations_20261031.txt` |
| Ollama journal, 09:00–09:25 UTC | no entries (a read-only request is a reading, R696(b); a model load is not) | `journalctl -u ollama --utc` |
| The real lock file's mtime | unchanged (2026-07-27 23:22:08 UTC) | `stat` only |

The collected set is `12aa_U20C/gate_c2/collected_ids.txt` (3,475 ids: `12aa_U20B/gate_c1/collected_ids.txt`, the ca29906 gate's list, plus commit 2's 23 new ids — the 21 of `tests/test_u20_loaded_context.py` and the two `[LoadedContextMismatch]` ids of `tests/test_run_fault_predicate.py`). The two warnings are sklearn's, from `tests/test_kappa.py::test_both_raters_constant_and_identical_is_undefined_not_one` (R641(c)).
