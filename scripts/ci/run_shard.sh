#!/bin/bash

set -uo pipefail

WORKSPACE="${WORKSPACE:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
cd "$WORKSPACE"
# shellcheck source=scripts/ci/config.sh
source scripts/ci/config.sh
sdb_config "$1"
DIGEST="$2"
SHARD="$3"
timing=()
timing_mount=()
if [[ "$4" != - ]]; then
	TIMING="$(realpath "$4")"
	timing=(--timing "$TIMING")
	timing_mount=(-v "$TIMING:$TIMING:ro")
fi
shift 4

: "${SDB_RUN_ID:=local-$$}"
: "${SDB_STORE_RESULTS:=${GITHUB_ACTIONS:-false}}"
: "${SDB_DOCKER_OWNER:=test-$(id -un)-$$}"
: "${SDB_CHECK:=}"
export SDB_DOCKER_OWNER SDB_CHECK
run_name="${CONFIG}${SDB_CHECK:+-$SDB_CHECK}-${SHARD/\//of}"

build=()
if [[ "$DIGEST" == - ]]; then
	: "${CARGO_HOME_CACHE:=/mnt/data/.cargo-home-sqllogic}"
	: "${CARGO_TARGET_CACHE:=/mnt/data/.cargo-target-sqllogic}"
	mkdir -p "$CARGO_HOME_CACHE" "$CARGO_TARGET_CACHE"
	build=(-v "$CARGO_HOME_CACHE:/cargo-home" -v "$CARGO_TARGET_CACHE:/cargo-target"
		-e CARGO_HOME=/cargo-home -e CARGO_TARGET_DIR=/cargo-target)
	test_args=()
else
	if [[ -d "$DIGEST" ]]; then
		bundle=$(realpath "$DIGEST")
	else
		bundle=$(python3 scripts/ci/ci.py fetch --config "$CONFIG" --arch "$ARCH" --digest "$DIGEST") || exit 1
		exec {bundle_lock}<"$bundle/.complete" && flock -s "$bundle_lock" || exit 1
	fi
	build=(-v "$bundle:$bundle")
	test_args=(--build-dir "$bundle")
fi
out="out/test-$run_name"
rm -rf "$out"
mkdir -p "$out" out/home out/sockets out/tmp
docker run --rm "$BUILD_IMAGE" cat /etc/passwd >out/passwd
echo "ci:x:$(id -u):$(id -g)::$WORKSPACE/out/home:/bin/bash" >>out/passwd
docker run --rm "$BUILD_IMAGE" cat /etc/group >out/group
grep -q ":$(id -g):" out/group || echo "ci:x:$(id -g):" >>out/group

name="sdbt-${SDB_DOCKER_OWNER//[^A-Za-z0-9_.-]/-}-$run_name"
gateway=$(docker network inspect bridge -f '{{(index .IPAM.Config 0).Gateway}}')
hosts=()
for h in duckdb-minio.com test-bucket.duckdb-minio.com test-bucket-2.duckdb-minio.com test-bucket-public.duckdb-minio.com; do
	hosts+=(--add-host "$h:127.0.0.2")
done
pass=()
for var in SEARCHBENCH_S3_KEY_ID SEARCHBENCH_S3_SECRET BIGLAKE_PROJECT BIGLAKE_CATALOG \
	BIGLAKE_CLIENT_EMAIL BIGLAKE_PRIVATE_KEY BIGLAKE_PRIVATE_KEY_ID SDB_STORE_HOST; do
	[[ -n "${!var:-}" ]] && pass+=(-e "$var")
done
mkdir -p /mnt/data/searchbench 2>/dev/null || true

docker rm -fv "$name" >/dev/null 2>&1 || true
docker run --rm --name "$name" \
	--label "serenedb.owner=$SDB_DOCKER_OWNER" \
	--user "$(id -u):$(id -g)" \
	--group-add "$(stat -c '%g' /var/run/docker.sock)" \
	--cap-add=SYS_PTRACE --security-opt seccomp=unconfined \
	--ulimit core=0 --ulimit nofile=65536:65536 --shm-size=8g \
	"${hosts[@]}" \
	-e HOME="$WORKSPACE/out/home" -e CI=true \
	-e SDB_SERVICE_NETNS="container:$name" -e SDB_SERVICE_BIND="$gateway" -e SDB_SERVICE_HOST="$gateway" \
	-e SDB_DOCKER_OWNER -e SDB_CHECK -e SDB_SHA="${SDB_SHA:-}" -e SDB_PR="${SDB_PR:-}" -e SDB_BRANCH="${SDB_BRANCH:-}" \
	-e SDB_RUN_ID="$SDB_RUN_ID" \
	-e SDB_SERVICE_SOCKET_ROOT="$WORKSPACE/out/sockets" -e SDB_TEST_TMP_ROOT="$WORKSPACE/out/tmp" \
	-e SDB_ICEBERG_DATA=/tmp/sdb-duckdb-iceberg \
	"${pass[@]}" \
	-v "$WORKSPACE:$WORKSPACE" -w "$WORKSPACE" \
	-v "$WORKSPACE/out/passwd:/etc/passwd:ro" -v "$WORKSPACE/out/group:/etc/group:ro" \
	"${build[@]}" \
	"${timing_mount[@]}" \
	-v /var/run/docker.sock:/var/run/docker.sock \
	-v /mnt/data/searchbench:/mnt/data/searchbench \
	"$BUILD_IMAGE" python3 scripts/ci/test.py "${test_args[@]}" --config "$CONFIG" \
	--shard "$SHARD" "${timing[@]}" --out "$out" "$@"
rc=$?
if ((rc == 130)); then
	echo "the run was interrupted: its results and profiles stay out of the store" >&2
	SDB_STORE_RESULTS=false
fi

if [[ -f "$out/results.jsonl" ]] && [[ "$SDB_STORE_RESULTS" == true ]]; then
	python3 scripts/ci/ci.py elsewhere --results "$out/results.jsonl" ||
		echo "warning: cannot tell whether the failures fail elsewhere too" >&2
	python3 scripts/ci/ci.py results --results "$out/results.jsonl" --run "$SDB_RUN_ID" ||
		echo "warning: results did not reach the store" >&2
fi

if compgen -G "$out/profiles/*.prof*" >/dev/null; then
	docker run --rm --user "$(id -u):$(id -g)" -v "$WORKSPACE:$WORKSPACE" -w "$WORKSPACE" "$BUILD_IMAGE" \
		bash -c 'llvm-profdata-23 merge -sparse -failure-mode=any -o "$0/shard.profdata" "$0"/profiles/*.prof*' "$out" &&
		rm -rf "$out/profiles"
	if [[ "$SDB_STORE_RESULTS" == true && -z "$SDB_CHECK" ]]; then
		python3 scripts/ci/ci.py upload "$out/shard.profdata" \
			"profdata/${SDB_RUN_ID}/${CONFIG}-${ARCH}-${SHARD/\//of}.profdata" ||
			echo "warning: the coverage profile did not reach the store" >&2
	fi
fi

if [[ $rc -ne 0 ]]; then
	docker run --rm --user "$(id -u):$(id -g)" -v "$WORKSPACE:$WORKSPACE" -w "$WORKSPACE/out" "$BUILD_IMAGE" \
		tar -I 'zstd -T0 -3' -cf "failure-$run_name.tar.zst" \
		--exclude=profiles --exclude=shard.profdata --exclude=duckdb-iceberg-data \
		--exclude='*/data.*' "${out#out/}" || true
fi
exit $rc
