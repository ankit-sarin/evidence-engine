# Expected values for the 2026-10-31 09:00 UTC nightly

This file supersedes `12aa_U20C/expected_nightly_20261031.md` for 2026-10-31 under R764(c); that file is not edited. It governs the 2026-11-01 nightly too while the engine tree stays unchanged, with that date in the log and durations file names (R749(b), R764(c)).

The tree at this writing: HEAD the 12ab closeout commit or its item-0 parent, whose engine tree equals `a6c31b1`'s (U21). Session 12ab's one engine commit is `a6c31b1` (no request sent while the previous one is alive; the recovery restart only after a request has ended); every later commit touches `docs/` and one sentence of `CLAUDE.md` only (`git diff --stat a6c31b1 HEAD -- engine tests scripts analysis review_specs` is empty). The values are therefore the 12ab-U21B gate's on the same code (3,490 passed / 1 xfailed / 17 deselected, collected 3,508, selected 3,491, exit 0; `12ab_U21B/gate_u21b/durations_12ab_u21b.txt`). Written at 12ab-CLOSE step 2, before the nightly ran; the next open or brief after it runs checks the log against this table.

| Value | Expected | Where to read it |
| --- | --- | --- |
| Collection line | `collected 3508 items / 17 deselected / 3491 selected` | `logs/nightly_test_20261031.log` |
| Summary line | `3490 passed, 17 deselected, 1 xfailed, 2 warnings` | the same log |
| Exit-code line | `pytest exit code: 0`, the log's last line | the same log |
| Durations file | present, one line per test phase | `logs/nightly_durations_20261031.txt` |
| Ollama journal, 09:00–09:25 UTC | no entries (a read-only request is a reading, R696(b); a model load is not) | `journalctl -u ollama --utc` |
| The real lock file's mtime | unchanged (2026-07-27 23:22:08 UTC) | `stat` only |

The collected set is `12ab_close/collected_ids_12ab_close.txt` (3,491 ids: `12aa_close/collected_ids_12aa_close.txt`, plus U21's 16 new ids, all of `tests/test_u21_no_overlap.py`; a copy of `12ab_U21B/gate_u21b/collected_ids.txt`). The two warnings are sklearn's, from `tests/test_kappa.py::test_both_raters_constant_and_identical_is_undefined_not_one` (R641(c)).
