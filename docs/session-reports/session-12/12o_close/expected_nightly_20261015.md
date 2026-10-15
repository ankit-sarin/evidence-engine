# Expected values for the 2026-10-15 09:00 UTC nightly

The tree at the 12o closeout: U12 commits 6–8 landed since the 2026-10-14 nightly (`ce9d4cc`, `f957ccb`, `5dbd3c4`), adding seven collected cases (three in `tests/test_cloud_package_init.py`, one in `tests/test_requirements_census.py`, two in `tests/test_text_refusal_at_selection.py`, one `tests/test_api_parity.py` case). Predictions until the run is read; checking it is part of the 12p startup verify (R645(e)).

| Value | Expected | Where to read it |
| --- | --- | --- |
| Collection line | `collected 3323 items / 17 deselected / 3306 selected` | `logs/nightly_test_20261015.log` |
| Summary line | `3305 passed, 17 deselected, 1 xfailed, 2 warnings` | the same log |
| Exit-code line | `pytest exit code: 0`, the log's last line | the same log |
| Durations file | present, one line per test phase | `logs/nightly_durations_20261015.txt` |
| Ollama journal, 09:00–09:25 UTC | no entries | `journalctl -u ollama --utc` |
| The real lock file's mtime | unchanged (2026-07-27 23:22:08 UTC) | `stat` only |

The counts hold only if no further commit changes the collected set before 09:00 UTC on 2026-10-15. The 07:00 UTC health job's model loads are expected and are not a finding. The two warnings are sklearn's, from `tests/test_kappa.py::test_both_raters_constant_and_identical_is_undefined_not_one` (R641(c)).
