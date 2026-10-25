#!/bin/bash
# 12w-V1A Q2/Q6: run, inside the worktree, the four named files + the repro candidate, then the
# 31 files that import engine.elicitation (supplementary census). usage: run_tests.sh <label>
set -u
L=$1; WT=$HOME/scratch/12w-v1a/wt2; OUT=$HOME/scratch/12w-v1a/r2/tests; P=/home/ankitsarin/projects/evidence-engine/.venv/bin/python
REPRO=/home/ankitsarin/projects/evidence-engine/docs/session-reports/session-12/12w_V1A/repro/test_v1a_attempt2_structure.py
cd "$WT" || exit 9
cp "$REPRO" tests/test_v1a_repro.py
sha256sum "$REPRO" tests/test_v1a_repro.py > "$OUT/${L}_repro_sha.txt"
M="not network and not ollama and not integration"
timeout 900 $P -m pytest tests/test_prompt_hash_coverage.py tests/test_elicitation_pipeline.py tests/test_feedback_block.py tests/test_u37_feedback_block.py $(ls tests/test_elicitation_pass1_runtime_structure.py 2>/dev/null) tests/test_v1a_repro.py \
  -m "$M" -rA -q -p no:cacheprovider --basetemp="$HOME/scratch/12w-v1a/r2/basetemp/${L}_named" > "$OUT/${L}_named.txt" 2>&1
echo "named exit $?" >> "$OUT/${L}_named.txt"
SUPP=$(grep -rlE "elicitation\.pipeline|elicitation import pipeline|from engine.elicitation" tests/ --include='test_*.py' | grep -v test_v1a_repro.py | sort)
echo "$SUPP" > "$OUT/${L}_supp_files.txt"
timeout 900 $P -m pytest $SUPP tests/test_v1a_repro.py -m "$M" -rfE -q -p no:cacheprovider --basetemp="$HOME/scratch/12w-v1a/r2/basetemp/${L}_supp" > "$OUT/${L}_supp.txt" 2>&1
echo "supp exit $?" >> "$OUT/${L}_supp.txt"
rm tests/test_v1a_repro.py
tail -n 3 "$OUT/${L}_named.txt"; tail -n 3 "$OUT/${L}_supp.txt"; wc -l < "$OUT/${L}_supp_files.txt"
