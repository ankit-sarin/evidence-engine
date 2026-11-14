#!/bin/bash
# 12ag-GATEUNIT-B step 5 — the tools commit's check: the staged set is exactly the three tools, nothing unstaged or untracked,
# the numstat read, and 4e's assertions (check_diff_gub.py) re-run on the staged tree. Exit 0 only if all hold.
T=docs/session-reports/tools; S=$1; f=0
st=$(git diff --cached --name-status | sort); want=$(printf 'M\t%s\nM\t%s\nM\t%s\n' $T/README.md $T/launch_gate.sh $T/run_gate.sh | sort)
[ "$st" = "$want" ] && echo "PASS staged set is exactly the three tools" || { echo "FAIL staged set: $st"; f=1; }
[ -z "$(git diff --name-only)" ] && [ -z "$(git ls-files --others --exclude-standard)" ] && echo "PASS nothing unstaged, nothing untracked" || { echo "FAIL unstaged or untracked"; f=1; }
echo "numstat:"; git diff --cached --numstat
python3 -I "$S/check_diff_gub.py" || f=1
echo "RESULT: $([ $f = 0 ] && echo PASS || echo FAIL)"; exit $f
