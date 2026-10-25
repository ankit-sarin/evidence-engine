#!/bin/bash
# 12w-V1A: one variant from a clean worktree: apply, pin, capture, tests, reset. usage: measure.sh <label> [diff...]
set -u
L=$1; shift; WT=$HOME/scratch/12w-v1a/wt; S=$HOME/scratch/12w-v1a; P=/home/ankitsarin/projects/evidence-engine/.venv/bin/python
CB=$WT/data/surgical_autonomy/extraction_codebook.yaml
cd "$WT" || exit 9
[ -z "$(git status --porcelain)" ] || { echo "worktree not clean before $L"; exit 8; }
[ $# -gt 0 ] && { git apply "$@" || exit 7; }
git diff --stat > "$S/diffs/${L}_applied_stat.txt"
$P docs/session-reports/session-12/12l_U04/pin_tuples.py review_specs/surgical_autonomy.yaml data/surgical_autonomy/extraction_codebook.yaml > "$S/pins/pin_${L}.json"; echo "pin exit $?"
$P "$S/captures/capture_attempts.py" review_specs/surgical_autonomy.yaml data/surgical_autonomy/extraction_codebook.yaml "$S/captures/capture_${L}.json"; echo "capture exit $?"
"$S/tests/run_tests.sh" "$L"
git checkout -q -- . ; [ -z "$(git status --porcelain)" ] && echo "worktree clean after $L"
