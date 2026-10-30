# Expected values for the 2026-10-30 09:00 UTC nightly

The tree at this closeout: HEAD the 12z closeout commit (child of the item-0 commit `237c321`), whose engine tree equals `973ecef`'s (U19 commit 2). Session 12z's two engine commits are `aa76124` (U19 commit 1, the Ollama environment recorded at open and re-checked after every performed restart) and `973ecef` (U19 commit 2, the local-service guard at open and one `Environment=` parser); every later commit touches `docs/` only (`git diff --stat 973ecef HEAD -- engine tests scripts analysis review_specs` is empty). The values are therefore the 12z-U19B commit-2 gate's on the same code (3,433 passed / 1 xfailed / 17 deselected, collected 3,451, selected 3,434, exit 0; `12z_U19B/gate_c2/durations_12z_u19b_c2.txt`, copied here as `durations_12z_u19b_c2.txt`). Written at 12z-CLOSE step 2a, before the nightly ran; 12aa-OPEN checks the log against this table.

| Value | Expected | Where to read it |
| --- | --- | --- |
| Collection line | `collected 3451 items / 17 deselected / 3434 selected` | `logs/nightly_test_20261030.log` |
| Summary line | `3433 passed, 17 deselected, 1 xfailed, 2 warnings` | the same log |
| Exit-code line | `pytest exit code: 0`, the log's last line | the same log |
| Durations file | present, one line per test phase | `logs/nightly_durations_20261030.txt` |
| Ollama journal, 09:00–09:25 UTC | no entries (a read-only request is a reading, R696(b); a model load is not) | `journalctl -u ollama --utc` |
| The real lock file's mtime | unchanged (2026-07-27 23:22:08 UTC) | `stat` only |

The collected set is `12z_close/collected_ids_12z_close.txt` (3,434 ids: `12z_U19B/gate_c2/collected_ids.txt`, the 973ecef gate's list, which is `12y_close/collected_ids_12y_close.txt` plus commit 1's 14 new ids — the 11 of `tests/test_u19_env_recheck.py`, the two `[ServerEnvChanged]` ids of `tests/test_run_fault_predicate.py` and `tests/test_api_parity.py::test_engine_import_resolves[run_pipeline.py::engine.utils.ollama_preflight]` — and commit 2's 8: the 6 of `tests/test_u19_local_service.py` and the two `[OllamaHostNotLocalService]` ids). The two warnings are sklearn's, from `tests/test_kappa.py::test_both_raters_constant_and_identical_is_undefined_not_one` (R641(c)).
