# Expected values for the 09:00 UTC nightlies from 2026-11-21

This file is written under R749(b) and R824(d): the engine tree changed during session 12ai, last at `7fad72a` (U30 with N46), so no earlier file governs a nightly on that tree; `12af_close/expected_nightly_20261111.md` governed only while the tree stayed `977c499`'s, and is not edited. The 2026-11-20 nightly ran on `03abfaf`, whose engine tree is `339370d`'s (U41), and was read against the U41B gate (`12ai_close/nightly_20261120.txt`). This file governs each nightly on a tree equal to `7fad72a`'s, the first being 2026-11-21's, with that date in the log and durations file names (R749(b)). Its name carries the date of the read that wrote it, 2026-11-20.

The tree at this writing: HEAD the 12ai closeout commit or its item-0 parent `45980ce`, whose engine tree equals `7fad72a`'s. Session 12ai's engine commits are `339370d` (U41: both Ollama restart paths opt-in and off by default; `restart_not_enabled`, the seventh call ending; `sudo -n`; `pdf_quality_check` bounded) and `7fad72a` (U30 with N46: an interrupted cloud run rolls back and closes `interrupted`; the cloud store drops a field outside the codebook); every later commit touches `docs/` only (`git diff --stat 7fad72a HEAD -- engine tests scripts analysis review_specs` is empty). The values are therefore U30B's gate's on the same code (3,607 passed / 1 xfailed / 17 deselected, collected 3,625, selected 3,608, exit 0; `12ai_U30B/gate_u30b/durations_12ai_u30b.txt`). Written at 12ai-CLOSE step 2, before the nightly ran; the next open or brief after it runs checks the log against this table.

| Value | Expected | Where to read it |
| --- | --- | --- |
| Collection line | `collected 3625 items / 17 deselected / 3608 selected` | `logs/nightly_test_<date>.log` |
| Summary line | `3607 passed, 17 deselected, 1 xfailed, 2 warnings` | the same log |
| Exit-code line | `pytest exit code: 0`, the log's last line | the same log |
| Durations file | present, one line per test phase | `logs/nightly_durations_<date>.txt` |
| Ollama journal, 09:00–09:25 UTC | no entries (a read-only request is a reading, R696(b); a model load is not) | `journalctl -u ollama --utc` |
| The real lock file's mtime | unchanged (2026-07-27 23:22:08 UTC) | `stat` only |

The collected set is `12ai_close/collected_ids_12ai_close.txt` (3,608 ids, a copy of `12ai_U30B/gate_u30b/collected_ids.txt`, cmp equal). The two warnings are sklearn's, from `tests/test_kappa.py::test_both_raters_constant_and_identical_is_undefined_not_one` (R641(c)). The HEAD each nightly ran on is found by the reflog method and read with the lock line (R779(c)); a nightly on a tree that is not `7fad72a`'s is read against that tree's own gate.
