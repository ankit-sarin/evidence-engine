# Expected values for the 2026-11-01 09:00 UTC nightly

This file supersedes `12ab_close/expected_nightly_20261031.md` for 2026-11-01 under R774(b); that file is not edited. It governs the 2026-11-02 nightly too while the engine tree stays unchanged, with that date in the log and durations file names (R749(b), R774(b)).

The tree at this writing: HEAD the 12ac closeout commit or its item-0 parent, whose engine tree equals `bfe3bd3`'s (U22 with its class-(c) fix). Session 12ac's engine commits are `51f8317` (U22: the `input_fit` line and the extraction telemetry row record `eval_count`, schema `extraction-telemetry-4`) and `bfe3bd3` (the one test-only (c) fix, `tests/test_u20_loaded_context.py`'s key-set pin); every later commit touches `docs/` only (`git diff --stat bfe3bd3 HEAD -- engine tests scripts analysis review_specs` is empty). The values are therefore U22's re-run gate's on the same code (3,492 passed / 1 xfailed / 17 deselected, collected 3,510, selected 3,493, exit 0; `12ac_U22B/gate_u22b_rerun/durations_gate_12ac_u22b_rerun.txt`). Written at 12ac-CLOSE step 2, before the nightly ran; the next open or brief after it runs checks the log against this table.

| Value | Expected | Where to read it |
| --- | --- | --- |
| Collection line | `collected 3510 items / 17 deselected / 3493 selected` | `logs/nightly_test_20261101.log` |
| Summary line | `3492 passed, 17 deselected, 1 xfailed, 2 warnings` | the same log |
| Exit-code line | `pytest exit code: 0`, the log's last line | the same log |
| Durations file | present, one line per test phase | `logs/nightly_durations_20261101.txt` |
| Ollama journal, 09:00–09:25 UTC | no entries (a read-only request is a reading, R696(b); a model load is not) | `journalctl -u ollama --utc` |
| The real lock file's mtime | unchanged (2026-07-27 23:22:08 UTC) | `stat` only |

The collected set is `12ac_close/collected_ids_12ac_close.txt` (3,493 ids: `12ab_close/collected_ids_12ab_close.txt`, plus U22's 2 new ids, `tests/test_ollama_input_fit.py::test_the_input_fit_line_records_eval_count` and `tests/test_thinking_channel.py::test_telemetry_records_each_passs_eval_count_under_schema_v4`; a copy of `12ac_U22B/gate_u22b_rerun/collected_ids.txt`). The two warnings are sklearn's, from `tests/test_kappa.py::test_both_raters_constant_and_identical_is_undefined_not_one` (R641(c)).
