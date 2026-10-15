# Expected values for the 2026-10-16 09:00 UTC nightly

The tree at the 12p closeout: no code changed since the 2026-10-15 nightly. Session 12p's three commits before this closeout (`9e32a46`, `57c1604`, `f4b296d`) and the closeout commit touch `docs/` only; `git diff --stat 5dbd3c4..f4b296d -- engine tests scripts analysis review_specs` is empty (measured at 12p-CLOSE step 0, I2). The values are therefore those of `12o_close/expected_nightly_20261015.md`, which the 2026-10-15 nightly matched line for line (870.95 s). Predictions until the run is read; checking it is part of the 12q startup verify (R650(h)).

| Value | Expected | Where to read it |
| --- | --- | --- |
| Collection line | `collected 3323 items / 17 deselected / 3306 selected` | `logs/nightly_test_20261016.log` |
| Summary line | `3305 passed, 17 deselected, 1 xfailed, 2 warnings` | the same log |
| Exit-code line | `pytest exit code: 0`, the log's last line | the same log |
| Durations file | present, one line per test phase | `logs/nightly_durations_20261016.txt` |
| Ollama journal, 09:00–09:25 UTC | no entries | `journalctl -u ollama --utc` |
| The real lock file's mtime | unchanged (2026-07-27 23:22:08 UTC) | `stat` only |

The counts hold only if no further commit changes the collected set before 09:00 UTC on 2026-10-16. The 07:00 UTC health job's model loads are expected and are not a finding. The two warnings are sklearn's, from `tests/test_kappa.py::test_both_raters_constant_and_identical_is_undefined_not_one` (R641(c)). The nightly's basetemp lands in `/tmp/pytest-of-ankitsarin` beside those already there (at this closeout: `pytest-0`, `pytest-1`, and `pytest-2`, which is another repository's suite); its entry count and size are recorded at the 12q open, not matched (R647(c)).
