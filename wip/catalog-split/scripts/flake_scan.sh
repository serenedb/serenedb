#!/usr/bin/env bash
S=/tmp/claude-1005/-home-mironov-projects-serenedb-serenedb/beac1a2c-e90b-481b-b24c-c7fe3e3ebed6/scratchpad
export TMPDIR=$S
cd $S/flake_scan
: > failed_tests2.txt
awk '$4=="failure"{print $1, $2, $3}' runs.txt | while read run branch sha; do
  gh run view $run --repo serenedb/serenedb --json jobs --jq '.jobs[] | select(.conclusion=="failure") | "\(.databaseId) \(.name)"' 2>/dev/null | while read job name; do
    case "$name" in *dev*|*asan*|*tsan*|*perf*) ;; *) continue;; esac
    gh api repos/serenedb/serenedb/actions/jobs/$job/logs 2>/dev/null | grep -oE '[a-zA-Z0-9_/.-]+\.test(_slow)? +\.\. \[FAILED\]|[0-9]+ \| [0-9]+ passed \| [0-9]+ failed|\[  FAILED  \] [A-Za-z0-9_./]+' | sort -u | sed "s|^|$run $branch $sha ${name%% *} |" >> failed_tests2.txt
  done
done
echo done >> failed_tests2.txt
