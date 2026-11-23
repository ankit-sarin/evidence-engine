# Expected values for the 09:00 UTC nightlies from 2026-11-23

This file is written under R749(b) and R832(d): the engine tree's five paths changed once during session 12aj, at `30b7f16` (the two cloud-span repair scripts retired, with their one list entry in `tests/test_review_paths.py`; nothing under `engine/`), so no earlier file governs a nightly on that tree; `12ai_close/expected_nightly_20261120.md` governed only while the tree stayed `7fad72a`'s, and is not edited. The 2026-11-21 and 2026-11-22 nightlies ran on `dccc546`, whose engine tree is `7fad72a`'s, and matched that file (`12ai_close/nightly_20261121.txt`, `nightly_20261122.txt`). This file governs each nightly on a tree equal to `30b7f16`'s, the first being 2026-11-23's, with that date in the log and durations file names (R749(b)). Its name carries the date of the read that wrote it, 2026-11-23.

The tree at this writing: HEAD the 12aj closeout commit or its item-0 parent `bc42a1f`, whose engine tree equals `30b7f16`'s. Session 12aj made no engine commit; `30b7f16` removed `scripts/reparse_cloud_spans.py` and `scripts/backfill_cloud_spans.py` and one entry of `SPEC_BEARING_ENTRY_POINTS`, and every later commit touches `docs/` only (`git diff --stat 30b7f16 HEAD -- engine tests scripts analysis review_specs` is empty). The values are therefore the U31R gate's on the same code (3,600 passed / 1 xfailed / 17 deselected, collected 3,618, selected 3,601, exit 0; `12aj_U31R/gate_u31r/durations_12aj_u31r.txt`). Written at 12aj-CLOSE step 2, before the 2026-11-23 nightly ran; the next open or brief after it runs checks the log against this table.

| Value | Expected | Where to read it |
| --- | --- | --- |
| Collection line | `collected 3618 items / 17 deselected / 3601 selected` | `logs/nightly_test_<date>.log` |
| Summary line | `3600 passed, 17 deselected, 1 xfailed, 2 warnings` | the same log |
| Exit-code line | `pytest exit code: 0`, the log's last line | the same log |
| Durations file | present, one line per test phase | `logs/nightly_durations_<date>.txt` |
| Ollama journal, 09:00–09:25 UTC | no entries (a read-only request is a reading, R696(b); a model load is not) | `journalctl -u ollama --utc` |
| The real lock file's mtime | unchanged (2026-07-27 23:22:08 UTC) | `stat` only |

The collected set is `12aj_close/collected_ids_12aj_close.txt` (3,601 ids, a copy of `12aj_U31R/gate_u31r/collected_ids.txt`, cmp equal). The two warnings are sklearn's, from `tests/test_kappa.py::test_both_raters_constant_and_identical_is_undefined_not_one` (R641(c)). The HEAD each nightly ran on is found by the reflog method and read with the lock line (R779(c)); a nightly on a tree that is not `30b7f16`'s is read against that tree's own gate.
