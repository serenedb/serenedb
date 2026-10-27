#!/bin/bash

set -uo pipefail

WORKSPACE="${WORKSPACE:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
cd "$WORKSPACE"
BASE="${1:-origin/main}"

head=$(git rev-parse HEAD)
git fetch -q --no-tags --depth=1000 origin main "$head"
merge_base=$(git merge-base "$BASE" HEAD)
mkdir -p out
rm -f out/bugfix-build.env
echo "building the merge-base $merge_base, or reusing its bundle"
git checkout -q --force "$merge_base" && git submodule update -q --init --force --recursive || exit 1
GITHUB_OUTPUT="$WORKSPACE/out/bugfix-build.env" ./scripts/ci/build_bundle.sh dev
built=$?
git checkout -q --force "$head" && git submodule update -q --init --force --recursive || exit 1
digest=$(sed -n 's/^digest=//p' out/bugfix-build.env 2>/dev/null)
((built == 0)) && [[ -n "$digest" ]] || exit 1
echo '{"units": {}}' >out/timing-bugfix.json
SDB_CHECK=bugfix ./scripts/ci/run_shard.sh dev "$digest" 1/1 out/timing-bugfix.json \
	--changed "$BASE" --only-changed --expect-fail --relabel 0
