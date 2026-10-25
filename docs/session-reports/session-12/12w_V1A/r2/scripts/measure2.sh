#!/bin/bash
# 12w-V1A resume Q4: one combination from a clean wt2: apply each diff in order, pin, capture, tests, reset.
# usage: measure2.sh <label> [diff...]
set -u
L=$1; shift; WT=$HOME/scratch/12w-v1a/wt2; S=$HOME/scratch/12w-v1a/r2; P=/home/ankitsarin/projects/evidence-engine/.venv/bin/python
cd "$WT" || exit 9
[ -z "$(git status --porcelain)" ] || { echo "$L: worktree not clean before"; exit 8; }
for d in "$@"; do
  if ! git apply "$d" 2> "$S/diffs/${L}_apply_err.txt"; then
    echo "$L: DOES NOT APPLY: $(basename "$d")"; cat "$S/diffs/${L}_apply_err.txt"
    git checkout -q -- . ; git clean -qf tests/ ; [ -z "$(git status --porcelain)" ] && echo "$L: worktree clean after"; exit 0
  fi
done
rm -f "$S/diffs/${L}_apply_err.txt"
git status --porcelain > "$S/diffs/${L}_applied_status.txt"; git diff --stat >> "$S/diffs/${L}_applied_status.txt"
$P docs/session-reports/session-12/12l_U04/pin_tuples.py review_specs/surgical_autonomy.yaml data/surgical_autonomy/extraction_codebook.yaml > "$S/pins/pin_${L}.json"; echo "$L: pin exit $?"
$P $HOME/scratch/12w-v1a/captures/capture_attempts.py review_specs/surgical_autonomy.yaml data/surgical_autonomy/extraction_codebook.yaml "$S/captures/capture_${L}.json" | sed "s/^/$L: /"
"$S/run_tests2.sh" "$L" | sed "s/^/$L: /"
git checkout -q -- . ; git clean -qf tests/ ; [ -z "$(git status --porcelain)" ] && echo "$L: worktree clean after"
