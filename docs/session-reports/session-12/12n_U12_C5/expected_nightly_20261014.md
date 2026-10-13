# Expected values for the 2026-10-14 09:00 UTC nightly (G5)

The first scheduled run of the edited `scripts/nightly_tests.sh` (U12 commit 5). These are predictions until the run is read.

| Value | Expected | Where to read it |
| --- | --- | --- |
| Collection line | `collected 3312 items / 17 deselected / 3295 selected` | `logs/nightly_test_20261014.log` |
| Summary line | `3294 passed, 17 deselected, 1 xfailed` — "17 deselected", where the 2026-10-13 log read "17 skipped" | the same log |
| Exit-code line | `pytest exit code: 0`, the log's last line | the same log |
| Durations file | present, one line per test phase | `logs/nightly_durations_20261014.txt` |
| Ollama journal, 09:00–09:25 UTC | no entries | `journalctl -u ollama --utc` |
| The real lock file's mtime | unchanged (2026-07-27 23:22:08 UTC) | `stat` only |

The counts hold only if no commit changes the collected set before 09:00 UTC on 2026-10-14; the script runs whatever tree is checked out then. The 07:00 UTC health job's model loads are expected and are not a finding.
