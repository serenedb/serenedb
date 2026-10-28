#!/bin/bash

set -euo pipefail

WORKSPACE="${WORKSPACE:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
cd "$WORKSPACE"
# shellcheck source=scripts/ci/config.sh
source scripts/ci/config.sh
sdb_config "$@"

: "${CCACHE_HOST_DIR:=/mnt/data/.ccache}"
: "${CARGO_HOME_CACHE:=/mnt/data/.cargo-home-sqllogic}"
: "${CARGO_TARGET_CACHE:=/mnt/data/.cargo-target-sqllogic}"

output() {
	echo "$1=$2"
	if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
		echo "$1=$2" >>"$GITHUB_OUTPUT"
	fi
}

for i in 1 2 3; do
	docker pull -q "$BUILD_IMAGE" && break
	sleep 10
done

digest=$(python3 scripts/ci/ci.py digest --config "$CONFIG" --flags "$BUILD_FLAGS" --image "$BUILD_IMAGE")
output digest "$digest"

where=$(python3 scripts/ci/ci.py locate --config "$CONFIG" --arch "$ARCH" --digest "$digest") || exit 1
if [[ "$where" != none ]]; then
	echo "the bundle of $CONFIG $digest is in the $where: nothing to build"
	output reused true
	exit 0
fi
output reused false

remote=()
if [[ "$ARCH" == x86_64 && -n "${SDB_STORE_HOST:-}" ]]; then
	remote=(-e "CCACHE_REMOTE_STORAGE=$SDB_STORE_HOST/ccache|layout=bazel")
fi
mkdir -p "$WORKSPACE/out/logs" "$WORKSPACE/out/home" "$CARGO_HOME_CACHE" "$CARGO_TARGET_CACHE"

docker run --rm \
	${SDB_DOCKER_OWNER:+--label "serenedb.owner=$SDB_DOCKER_OWNER"} \
	--user "$(id -u):$(id -g)" \
	-e HOME=/serenedb/out/home \
	-e CCACHE_DIR=/.ccache \
	-e CCACHE_BASEDIR=/serenedb \
	-e CCACHE_NOHASHDIR=1 \
	-e CCACHE_MAXSIZE=100G \
	"${remote[@]}" \
	-e BUILD_DIR -e BUILDMODE -e SDB_DEBUG_INFO -e SDB_DEV -e SDB_GTEST -e SDB_FAULT_INJECTION \
	-e SDB_LTO -e SDB_COVERAGE -e SDB_VTUNE -e SDB_ALLOC -e SANITIZERS -e EXTRA_CMAKE_FLAGS \
	-v "$WORKSPACE:/serenedb" \
	-v "$CCACHE_HOST_DIR:/.ccache" \
	"$BUILD_IMAGE" /serenedb/scripts/ci/build.bash "${TARGETS[@]}"

docker run --rm \
	${SDB_DOCKER_OWNER:+--label "serenedb.owner=$SDB_DOCKER_OWNER"} \
	--user "$(id -u):$(id -g)" \
	-e HOME=/serenedb/out/home \
	-e CARGO_HOME=/cargo-home \
	-v "$WORKSPACE:/serenedb" \
	-v "$CARGO_HOME_CACHE:/cargo-home" \
	-v "$CARGO_TARGET_CACHE:/cargo-target" \
	-w /serenedb \
	"$BUILD_IMAGE" cargo build --release --quiet \
	--manifest-path third_party/sqllogictest-rs/sqllogictest-bin/Cargo.toml --target-dir /cargo-target

python3 scripts/ci/ci.py stage --config "$CONFIG" --arch "$ARCH" --digest "$digest" \
	--build-dir "$WORKSPACE/$BUILD_DIR" --runner "$CARGO_TARGET_CACHE/release/sqllogictest"
