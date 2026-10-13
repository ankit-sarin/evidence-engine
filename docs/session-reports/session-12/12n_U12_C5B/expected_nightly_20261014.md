# Expected values for the 2026-10-14 09:00 UTC nightly — supersedes `12n_U12_C5/expected_nightly_20261014.md`

The first scheduled run of the edited `scripts/nightly_tests.sh`. The C5 file's counts are raised by the four tests of `tests/test_nightly_script.py` (U12 commit 5B); nothing else in it changes. Predictions until the run is read; checking it is item 1 of the next session open (R635(d)).

| Value | Expected | Where to read it |
| --- | --- | --- |
| Collection line | `collected 3316 items / 17 deselected / 3299 selected` | `logs/nightly_test_20261014.log` |
| Summary line | `3298 passed, 17 deselected, 1 xfailed` | the same log |
| Exit-code line | `pytest exit code: 0`, the log's last line | the same log |
| Durations file | present, one line per test phase | `logs/nightly_durations_20261014.txt` |
| Ollama journal, 09:00–09:25 UTC | no entries | `journalctl -u ollama --utc` |
| The real lock file's mtime | unchanged (2026-07-27 23:22:08 UTC) | `stat` only |

The counts hold only if no further commit changes the collected set before 09:00 UTC on 2026-10-14. The 07:00 UTC health job's model loads are expected and are not a finding.
