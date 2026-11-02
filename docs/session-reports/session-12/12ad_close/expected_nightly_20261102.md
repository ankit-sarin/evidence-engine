# Expected values for the 2026-11-02 09:00 UTC nightly

This file supersedes `12ac_close/expected_nightly_20261101.md` for 2026-11-02 under R782(f); that file is not edited. It governs the 2026-11-03 nightly too while the engine tree stays unchanged, with that date in the log and durations file names (R749(b), R782(f)).

The tree at this writing: HEAD the 12ad closeout commit or its item-0 parent, whose engine tree equals `9ae1fde`'s (U23). Session 12ad's engine commit is `9ae1fde` (U23: one `REPO_ROOT` in `engine/core/repo_root.py`, `DATA_ROOT` and `SPEC_ROOT` anchored to it; the acceptance test and the nine rewrites); every later commit touches `docs/` only (`git diff --stat 9ae1fde HEAD -- engine tests scripts analysis review_specs` is empty). The values are therefore U23's gate's on the same code (3,493 passed / 1 xfailed / 17 deselected, collected 3,511, selected 3,494, exit 0; `12ad_U23B/gate_u23b/durations_12ad_u23b.txt`). Written at 12ad-CLOSE step 2, before the nightly ran; the next open or brief after it runs checks the log against this table.

| Value | Expected | Where to read it |
| --- | --- | --- |
| Collection line | `collected 3511 items / 17 deselected / 3494 selected` | `logs/nightly_test_20261102.log` |
| Summary line | `3493 passed, 17 deselected, 1 xfailed, 2 warnings` | the same log |
| Exit-code line | `pytest exit code: 0`, the log's last line | the same log |
| Durations file | present, one line per test phase | `logs/nightly_durations_20261102.txt` |
| Ollama journal, 09:00–09:25 UTC | no entries (a read-only request is a reading, R696(b); a model load is not) | `journalctl -u ollama --utc` |
| The real lock file's mtime | unchanged (2026-07-27 23:22:08 UTC) | `stat` only |

The collected set is `12ad_close/collected_ids_12ad_close.txt` (3,494 ids: `12ac_close/collected_ids_12ac_close.txt`, plus U23's 1 new id, `tests/test_review_paths.py::test_spec_and_data_roots_do_not_follow_the_working_directory`; a copy of `12ad_U23B/gate_u23b/collected_ids.txt`). The two warnings are sklearn's, from `tests/test_kappa.py::test_both_raters_constant_and_identical_is_undefined_not_one` (R641(c)).
