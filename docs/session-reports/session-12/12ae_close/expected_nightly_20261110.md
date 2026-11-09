# Expected values for the 2026-11-10 09:00 UTC nightly

This file is written under R749(b) and R795(e): the engine tree changed during session 12ae, last at `24ce26b` (U28), so no earlier file governs the 2026-11-10 nightly; `12ad_close/expected_nightly_20261102.md` governed only while the tree stayed `9ae1fde`'s, and is not edited. This file governs the 2026-11-11 nightly too while the engine tree stays unchanged, with that date in the log and durations file names (R749(b), R795(e)).

The tree at this writing: HEAD the 12ae closeout commit or its item-0 parent `df3e721`, whose engine tree equals `24ce26b`'s (U28). Session 12ae's engine commits are `94ab7a8` and `1f6bc16` (U24), `168199a` (U25), `69e9db8` (U26), `077cb0f` (U27) and `24ce26b` (U28: the cloud gate's two flags, the no-values summary and CLI line, the cloud write boundary's citation guard; the acceptance test, the rewritten test and the docstring word); every later commit touches `docs/` only (`git diff --stat 24ce26b HEAD -- engine tests scripts analysis review_specs` is empty, `12ae_close/diff_since_24ce26b.txt`). The values are therefore U28's gate's on the same code (3,544 passed / 1 xfailed / 17 deselected, collected 3,562, selected 3,545, exit 0; `12ae_U28B/gate_u28b/durations_12ae_u28b.txt`). Written at 12ae-CLOSE step 2, before the nightly ran; the next open or brief after it runs checks the log against this table.

| Value | Expected | Where to read it |
| --- | --- | --- |
| Collection line | `collected 3562 items / 17 deselected / 3545 selected` | `logs/nightly_test_20261110.log` |
| Summary line | `3544 passed, 17 deselected, 1 xfailed, 2 warnings` | the same log |
| Exit-code line | `pytest exit code: 0`, the log's last line | the same log |
| Durations file | present, one line per test phase | `logs/nightly_durations_20261110.txt` |
| Ollama journal, 09:00–09:25 UTC | no entries (a read-only request is a reading, R696(b); a model load is not) | `journalctl -u ollama --utc` |
| The real lock file's mtime | unchanged (2026-07-27 23:22:08 UTC) | `stat` only |

The collected set is `12ae_close/collected_ids_12ae_close.txt` (3,545 ids, a copy of `12ae_U28B/gate_u28b/collected_ids.txt`, cmp equal). The two warnings are sklearn's, from `tests/test_kappa.py::test_both_raters_constant_and_identical_is_undefined_not_one` (R641(c)).
