#!/bin/bash

set -uo pipefail

WORKSPACE="${WORKSPACE:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
cd "$WORKSPACE"
# shellcheck source=scripts/ci/config.sh
source scripts/ci/config.sh
sdb_config "$1"
DIGEST="$2"
SHARD="$3"
TIMING="$(realpath "$4")"
shift 4

: "${BUILD_IMAGE:=serenedb/serenedb-build-ubuntu:latest}"
: "${SDB_RUN_ID:=local-$$}"
: "${SDB_DOCKER_OWNER:=test-$(id -un)-$$}"
export SDB_DOCKER_OWNER

if [[ -d "$DIGEST" ]]; then
	bundle=$(realpath "$DIGEST")
else
	bundle=$(python3 scripts/ci/ci.py fetch --config "$CONFIG" --arch "$ARCH" --digest "$DIGEST") || exit 1
fi
out="out/test-${CONFIG}-${SHARD/\//of}"
rm -rf "$out"
mkdir -p "$out" out/home out/sockets

name="sdbt-${SDB_DOCKER_OWNER//[^A-Za-z0-9_.-]/-}-${CONFIG}-${SHARD/\//of}"
gateway=$(docker network inspect bridge -f '{{(index .IPAM.Config 0).Gateway}}')
hosts=()
for h in duckdb-minio.com test-bucket.duckdb-minio.com test-bucket-2.duckdb-minio.com test-bucket-public.duckdb-minio.com; do
	hosts+=(--add-host "$h:127.0.0.2")
done
pass=()
for var in SEARCHBENCH_S3_KEY_ID SEARCHBENCH_S3_SECRET BIGLAKE_PROJECT BIGLAKE_CATALOG \
	BIGLAKE_CLIENT_EMAIL BIGLAKE_PRIVATE_KEY BIGLAKE_PRIVATE_KEY_ID SDB_STORE; do
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
	-e SDB_DOCKER_OWNER -e SDB_SHA="${SDB_SHA:-}" -e SDB_PR="${SDB_PR:-}" \
	-e SDB_SERVICE_SOCKET_ROOT="$WORKSPACE/out/sockets" \
	"${pass[@]}" \
	-v "$WORKSPACE:$WORKSPACE" -w "$WORKSPACE" \
	-v "$bundle:$bundle" \
	-v "$TIMING:$TIMING:ro" \
	-v /var/run/docker.sock:/var/run/docker.sock \
	-v /mnt/data/searchbench:/mnt/data/searchbench \
	"$BUILD_IMAGE" python3 scripts/ci/test.py --build-dir "$bundle" --config "$CONFIG" \
	--shard "$SHARD" --timing "$TIMING" --out "$out" "$@"
rc=$?

if [[ -f "$out/results.jsonl" ]] && [[ "${SDB_STORE_RESULTS:-true}" == true ]]; then
	python3 scripts/ci/ci.py results --results "$out/results.jsonl" --run "$SDB_RUN_ID" ||
		echo "warning: results did not reach the store" >&2
fi

if compgen -G "$out/profiles/*.profraw" >/dev/null; then
	docker run --rm --user "$(id -u):$(id -g)" -v "$WORKSPACE:$WORKSPACE" -w "$WORKSPACE" "$BUILD_IMAGE" \
		bash -c 'llvm-profdata-23 merge -sparse -failure-mode=any -o "$0/shard.profdata" "$0"/profiles/*.profraw' "$out" &&
		python3 scripts/ci/ci.py upload "$out/shard.profdata" \
			"profdata/${SDB_RUN_ID}/${CONFIG}-${ARCH}-${SHARD/\//of}.profdata" ||
		echo "warning: the coverage profile did not reach the store" >&2
	rm -rf "$out/profiles"
fi

if [[ $rc -ne 0 ]]; then
	tar -C out -I 'zstd -T0 -3' -cf "out/failure-${CONFIG}-${SHARD/\//of}.tar.zst" \
		--exclude=profiles --exclude='*/data.*' "${out#out/}" || true
fi
exit $rc
