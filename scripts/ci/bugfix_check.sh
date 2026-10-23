#!/bin/bash

set -uo pipefail

WORKSPACE="${WORKSPACE:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
cd "$WORKSPACE"
# shellcheck source=scripts/ci/config.sh
source scripts/ci/config.sh
sdb_config dev
BASE="${1:-origin/main}"
: "${BUILD_IMAGE:=serenedb/serenedb-build-ubuntu:latest}"

head=$(git rev-parse HEAD)
git fetch -q --no-tags --depth=1000 origin main
merge_base=$(git merge-base "$BASE" HEAD)
digest=$(python3 scripts/ci/ci.py digest --config dev --flags "$BUILD_FLAGS" --image "$BUILD_IMAGE" --rev "$merge_base")
if ! python3 scripts/ci/ci.py exists "bundles/dev-${ARCH}-${digest}.tar.zst"; then
	echo "building the merge-base $merge_base"
	git checkout -q --force "$merge_base" && git submodule update -q --init --force --recursive || exit 1
	./scripts/ci/build_bundle.sh dev || exit 1
	git checkout -q --force "$head" && git submodule update -q --init --force --recursive || exit 1
fi
mkdir -p out
echo '{"units": {}}' >out/timing-bugfix.json
./scripts/ci/run_shard.sh dev "$digest" 1/1 out/timing-bugfix.json \
	--changed "$BASE" --only-changed --expect-fail --relabel 0
