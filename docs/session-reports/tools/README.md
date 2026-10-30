# Session tools (R746(e))

Built and validated at 12aa-SETUP. Each takes every per-use value from its arguments or the files it reads (R742(b)). Evidence sits in `docs/session-reports/session-12/12aa_SETUP/`. Nothing here changes `engine/`, `scripts/` or `tests/`. The briefs stay in their present form until a later PI ruling (R746(e)).

| Tool | Inputs | Validation | Evidence |
| --- | --- | --- | --- |
| `extract_rules.py` | `--plan`; `--glosses` and `--out` to write; or `--list`; or `--spot <n>` | Writes `docs/plan/STANDING_RULES.md`. Ten clauses at fixed stride, each line grepped in the plan, verbatim. Refuses a selected clause with no gloss, or a gloss with no clause | `2a_extract.txt`, `2a_list.txt`, `2a_spot.txt`, `2a_forms.txt`, `rules_glosses.json` |
| `transcribe_ruling.py` | `<rNNN.txt> <block number> <meta.json>`; `--check-plan <plan>` or `--append <plan>` | Blocks 135, 138, 139, 140, 141 and 142 regenerated from their files and metadata, byte-identical to the plan | `2b_validation.txt`, `transcribe_meta/` |
| `session_open_check.py` | `<expected.json>` | With today's references at `ca12c30`, the 2026-10-30 nightly included: every value MATCH, exit 0. With three wrong values: exit 1. A section the file omits prints SKIPPED | `2c_validation.txt`, `2c_negative.txt`, `open_expected_validation.json` |
| `launch_gate.sh` | `<evidence_dir> <session_name> <ref_ids\|-> <ref_durations\|-> [selection...]`, run from the repository root | Dry run on `tests/test_u20_modelfile_num_ctx.py`: the nineteen files, and the launch line's inner command equal to `gate_launch.txt`'s line 2 | `dry_run/`, `2d_check.txt` |
| `run_gate.sh` | as `launch_gate.sh`, which calls it inside tmux | The same dry run. It writes `gate_rc.txt` last, so a wait watches that file. No selection is the standard gate in the shared temp root; a selection passes `--basetemp` under the evidence directory's sibling (R703(j)) | the same |

`transcribe_ruling.py`'s metadata keys are in its docstring. `launch_gate.sh` writes `host_snapshot.sh` and `ollama_reads.sh` into the evidence directory itself, so the set stays self-describing. One addition to the set's contents: `gate_launch.txt` has a second line, the command as run, which is what the launch-line check reads.

`session_open_check.py` takes the expected repository `status` as a list, default empty. The validation file names the three untracked paths of that moment; a real open leaves it out.
