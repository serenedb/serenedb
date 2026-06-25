#!/bin/bash

# Docker Compose runner for sqllogic tests
# Usage:
#   ./run_in_docker.sh                    # Uses default 'sqllogic' kind
#   ./run_in_docker.sh recovery           # Uses 'recovery' kind
#   TEST_KIND=recovery ./run_in_docker.sh # Uses env variable

TEST_KIND=${1:-${TEST_KIND:-sqllogic}}

: "${BUILD_DIR:=build}"

if test -z "$SQLLOGIC_DIR"; then
	export SQLLOGIC_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
fi

if ! test -f "$SQLLOGIC_DIR/run_in_docker.sh"; then
	echo "SQLLOGIC_DIR is undefined or invalid"
	exit 255
fi

# In case of running locally
if test -z "$WORKSPACE"; then
	export WORKSPACE=$(realpath "$SQLLOGIC_DIR/../../")
fi

# Resources directory as seen inside the serenedb container
# ($WORKSPACE is mounted at /serenedb, so resources lives at /serenedb/resources)
export RESOURCES="/serenedb/resources"

if ! test -f "$WORKSPACE/docker.env"; then
	touch "$WORKSPACE/docker.env"
fi

# Local sanitizer runs (run_in_docker.sh outside CI) skip the CI's
# 01-ci-set-sanitizer-options step, so docker.env carries no *_OPTIONS and the
# suppressions files are never loaded. If the built serened is a sanitizer binary
# and the matching *_OPTIONS isn't already provided, append the same options CI
# uses so a local run matches CI behaviour. Paths are the in-container ones
# (/serenedb is the mounted WORKSPACE). LSan runs under ASan, so an ASan binary
# also gets LSAN_OPTIONS.
if test -x "$WORKSPACE/$BUILD_DIR/bin/serened"; then
	sdb_syms=$(nm -D "$WORKSPACE/$BUILD_DIR/bin/serened" 2>/dev/null)

	if ! grep -q '^TSAN_OPTIONS=' "$WORKSPACE/docker.env" 2>/dev/null &&
		grep -q '__tsan_init' <<<"$sdb_syms"; then
		sdb_opts="detect_deadlocks=true:second_deadlock_stack=1:history_size=0"
		if test -f "$WORKSPACE/resources/suppressions/tsan.txt"; then
			sdb_opts="${sdb_opts}:suppressions=/serenedb/resources/suppressions/tsan.txt"
		fi
		echo "TSAN_OPTIONS=${sdb_opts}" >>"$WORKSPACE/docker.env"
	fi

	if grep -q '__asan_init' <<<"$sdb_syms"; then
		if ! grep -q '^ASAN_OPTIONS=' "$WORKSPACE/docker.env" 2>/dev/null; then
			sdb_opts="handle_ioctl=true:check_initialization_order=true:detect_odr_violation=1:strict_init_order=true"
			if test -f "$WORKSPACE/resources/suppressions/asan.txt"; then
				sdb_opts="${sdb_opts}:suppressions=/serenedb/resources/suppressions/asan.txt"
			fi
			echo "ASAN_OPTIONS=${sdb_opts}" >>"$WORKSPACE/docker.env"
		fi
		if ! grep -q '^LSAN_OPTIONS=' "$WORKSPACE/docker.env" 2>/dev/null &&
			test -f "$WORKSPACE/resources/suppressions/lsan.txt"; then
			echo "LSAN_OPTIONS=suppressions=/serenedb/resources/suppressions/lsan.txt" >>"$WORKSPACE/docker.env"
		fi
	fi

	if ! grep -q '^UBSAN_OPTIONS=' "$WORKSPACE/docker.env" 2>/dev/null &&
		grep -q '__ubsan_handle' <<<"$sdb_syms"; then
		sdb_opts="print_stacktrace=1"
		if test -f "$WORKSPACE/resources/suppressions/ubsan.txt"; then
			sdb_opts="${sdb_opts}:suppressions=/serenedb/resources/suppressions/ubsan.txt"
		fi
		echo "UBSAN_OPTIONS=${sdb_opts}" >>"$WORKSPACE/docker.env"
	fi

	if ! grep -q '^MSAN_OPTIONS=' "$WORKSPACE/docker.env" 2>/dev/null &&
		grep -q '__msan_init' <<<"$sdb_syms"; then
		sdb_opts="poison_in_dtor=1"
		if test -f "$WORKSPACE/resources/suppressions/msan.txt"; then
			sdb_opts="${sdb_opts}:suppressions=/serenedb/resources/suppressions/msan.txt"
		fi
		echo "MSAN_OPTIONS=${sdb_opts}" >>"$WORKSPACE/docker.env"
	fi

	unset sdb_syms sdb_opts
fi

mkdir -p "$WORKSPACE"/out/sanitizers/{leak,undefined,address,memory,thread}
mkdir -p "$WORKSPACE/out/coverage/profiles"
mkdir -p "$WORKSPACE/out/logs"
mkdir -p "${CARGO_TARGET_CACHE:-${HOME}/.cache/serenedb-cargo-target}"
mkdir -p "${CARGO_HOME_CACHE:-${HOME}/.cache/serenedb-cargo-home}"
mkdir -p "${SDB_TIMING_CACHE:-${HOME}/.cache/serenedb-timing-cache}"

if test -z "$BUILD_IMAGE"; then
	export BUILD_IMAGE=serenedb/serenedb-build-ubuntu:latest
fi

# Pass host user to compose/docker for correct file ownership
export DOCKER_UID="$(id -u)"
export DOCKER_GID="$(id -g)"
export DOCKER_SOCK_GID="$(stat -c '%g' /var/run/docker.sock 2>/dev/null || echo 999)"

cd $SQLLOGIC_DIR

export BUILD_DIR

PREFIX="$(LC_ALL=C tr -dc 'a-z0-9' </dev/urandom 2>/dev/null | head -c 4)"

COMPOSE_FILE="docker-compose.$TEST_KIND.yml"
# Validate that compose file exists
if ! test -f "$SQLLOGIC_DIR/$COMPOSE_FILE"; then
	echo "Error: Unknown test kind '$TEST_KIND' - file '$COMPOSE_FILE' not found" >&2
	exit 255
fi

# Reap orphans from earlier interrupted runs before bringing the stack up.
# See tests/scripts/reap_stale_docker_orphans.sh for the full rationale.
source "$SQLLOGIC_DIR/../scripts/reap_stale_docker_orphans.sh"
reap_stale_docker_orphans

cleanup() {
	docker compose -p "${PREFIX}" -f "$COMPOSE_FILE" down --volumes --remove-orphans
}
trap cleanup EXIT INT TERM

docker compose \
	-p "${PREFIX}" \
	-f "$COMPOSE_FILE" \
	up \
	--attach tests \
	--exit-code-from tests \
	--remove-orphans

test_exit_code=$?

if ! test "${test_exit_code}" -eq "0"; then
	echo "$TEST_KIND tests failed!"
	# Print all non-test container logs
	for svc in $(docker compose -p "${PREFIX}" -f "$COMPOSE_FILE" ps -a --format '{{.Service}}' 2>/dev/null); do
		[[ "$svc" == "tests" ]] && continue
		echo "$svc container log begin:"
		docker compose -p "${PREFIX}" -f "$COMPOSE_FILE" logs "$svc" 2>&1
		echo "$svc container log end!"
	done
fi

exit "$test_exit_code"
