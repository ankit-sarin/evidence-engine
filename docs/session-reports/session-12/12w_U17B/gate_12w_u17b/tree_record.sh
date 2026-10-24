#!/bin/bash
# The tree as one record: HEAD, the full diff's sha256 (tracked changes + untracked files' content), and per-file sha256 of every changed or new file.
cd $HOME/projects/evidence-engine
echo "HEAD $(git rev-parse HEAD)"
{ git diff HEAD --binary; for f in $(git ls-files --others --exclude-standard | sort); do echo "== untracked $f"; cat "$f"; done; } | sha256sum | sed 's/^/tree_diff_sha256 /'
for f in $( (git diff HEAD --name-only; git ls-files --others --exclude-standard) | sort -u); do sha256sum "$f"; done
