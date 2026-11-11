# Expected values for the 2026-11-11 09:00 UTC nightly

This file is written under R749(b) and R802(d): the engine tree changed during session 12af, last at `977c499` (U29), so no earlier file governs the 2026-11-11 nightly; `12ae_close/expected_nightly_20261110.md` governed only while the tree stayed `24ce26b`'s, and is not edited. This file governs each later nightly too while the engine tree stays unchanged, with that date in the log and durations file names (R749(b)).

The tree at this writing: HEAD the 12af closeout commit or its item-0 parent `4884bc3`, whose engine tree equals `977c499`'s (U29). Session 12af's engine commits are `53eabfd` (U40: the cloud store writes the arm's claim events and no paper event; the `paper_event` keyword on the writer) and `977c499` (U29: `send` records every call inside a run, `read_response` on each transport, `max_retries=0` at both client constructions); every later commit touches `docs/` only (`git diff --stat 977c499 HEAD -- engine tests scripts analysis review_specs` is empty, `12af_close/diff_since_977c499.txt`). The values are therefore U29's gate's on the same code (3,573 passed / 1 xfailed / 17 deselected, collected 3,591, selected 3,574, exit 0; `12af_U29B/gate_u29b/durations_12af_u29b.txt`). Written at 12af-CLOSE step 2, before the nightly ran; the next open or brief after it runs checks the log against this table.

| Value | Expected | Where to read it |
| --- | --- | --- |
| Collection line | `collected 3591 items / 17 deselected / 3574 selected` | `logs/nightly_test_20261111.log` |
| Summary line | `3573 passed, 17 deselected, 1 xfailed, 2 warnings` | the same log |
| Exit-code line | `pytest exit code: 0`, the log's last line | the same log |
| Durations file | present, one line per test phase | `logs/nightly_durations_20261111.txt` |
| Ollama journal, 09:00–09:25 UTC | no entries (a read-only request is a reading, R696(b); a model load is not) | `journalctl -u ollama --utc` |
| The real lock file's mtime | unchanged (2026-07-27 23:22:08 UTC) | `stat` only |

The collected set is `12af_close/collected_ids_12af_close.txt` (3,574 ids, a copy of `12af_U29B/gate_u29b/collected_ids.txt`, cmp equal). The two warnings are sklearn's, from `tests/test_kappa.py::test_both_raters_constant_and_identical_is_undefined_not_one` (R641(c)).
