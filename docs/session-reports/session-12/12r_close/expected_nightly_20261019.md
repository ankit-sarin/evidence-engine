# Expected values for the 2026-10-19 09:00 UTC nightly

The tree at the 12r closeout: the code is `b3f0072`'s. Session 12r changed nothing under `engine`, `tests`, `scripts`, `analysis` or `review_specs`: `git diff --stat e5ec600 HEAD -- engine tests scripts analysis review_specs` is empty at this closeout (12r-CLOSE step 0, I3), and every 12r commit touches `docs/` only. The values are therefore the 12r-OPEN gate's on the same code (3,306 passed / 1 xfailed / 17 deselected, collected 3,307, exit 0; `12r_close/durations_12r_open.txt`) and the 2026-10-17 and 2026-10-18 nightlies', which both read so. Predictions until the run is read; checking it is part of the 12s startup verify.

| Value | Expected | Where to read it |
| --- | --- | --- |
| Collection line | `collected 3324 items / 17 deselected / 3307 selected` | `logs/nightly_test_20261019.log` |
| Summary line | `3306 passed, 17 deselected, 1 xfailed, 2 warnings` | the same log |
| Exit-code line | `pytest exit code: 0`, the log's last line | the same log |
| Durations file | present, one line per test phase | `logs/nightly_durations_20261019.txt` |
| Ollama journal, 09:00–09:25 UTC | no entries | `journalctl -u ollama --utc` |
| The real lock file's mtime | unchanged (2026-07-27 23:22:08 UTC) | `stat` only |

The counts hold only if no further commit changes the collected set before 09:00 UTC on 2026-10-19. The collected set is `12q_close/collected_ids_12q_close.txt` (3,307 ids), unchanged since `b3f0072`. The two warnings are sklearn's, from `tests/test_kappa.py::test_both_raters_constant_and_identical_is_undefined_not_one` (R641(c)). The nightly's basetemp lands in `/tmp/pytest-of-ankitsarin` beside those already there (at this closeout `pytest-4`, `pytest-5`, surgical-cv's, and `pytest-6`, the 12r-OPEN gate's; `12r_close/temp_root_close.txt`); its entry count and size are recorded at the 12s open, not matched (R647(c)).

The 07:00 UTC health job on 2026-10-19 runs on the new design (dgx-infra `7723cf1`: every installed model loaded over HTTP at `num_ctx` 4096 with `keep_alive` 0, nothing resident after). It is not a nightly finding; 12s-OPEN reads its journal window for its loads, their outcome and any NVRM line, as readings (R663(d), R670(f)). At this closeout `deepseek-r1:32b` is resident (keep-alive forever, from 12r-U13-PROBE); the health job's first call may evict it.
