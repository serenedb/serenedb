#!/bin/bash

# Runs recovery tests in parallel, each test file against its own serened instance.
#
# By default, spawns one serened per test file (capped by nproc). Each instance
# auto-restarts on crash via run_serened_loop.sh.
#
# Usage (local dev):
#   ./run_recovery_tests.sh                              # auto-parallel
#   ./run_recovery_tests.sh --jobs 4                     # 4 workers
#
# Usage (Docker, new compose):
#   ./run_recovery_tests.sh --runner /sqllogictest-rs    # auto-parallel in container
#
# Usage (legacy external serened -- sequential fallback):
#   SERVICE_HOST=serenedb-recovery SERVICE_PORT=7777 ./run_recovery_tests.sh

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

: "${SERVICE_HOST:=localhost}"
: "${SERVICE_PORT:=7777}"
: "${BASE_PORT:=${SERVICE_PORT}}"
: "${JOBS:=0}"

cd "$SCRIPT_DIR"

export RETRY_ATTEMPTS=10
export BACKOFF_DURATION=500ms

# --- Parse arguments ---

RUNNER_ARGS=()

while [[ $# -gt 0 ]]; do
	case "$1" in
	--jobs)
		JOBS="$2"
		shift 2
		;;
	--runner)
		RUNNER_ARGS=(--runner "$2")
		shift 2
		;;
	*)
		echo "Unknown option: $1" >&2
		exit 1
		;;
	esac
done

# --- Discover test files ---

declare -a test_files=()
while IFS= read -r -d '' file; do
	test_files+=("${file#./}")
done < <(find recovery/ -name "*.test" -type f -print0 | sort -z)

if [[ ${#test_files[@]} -eq 0 ]]; then
	echo "No test files found in recovery/ directory"
	exit 1
fi

echo "Found ${#test_files[@]} recovery test(s)"

# --- Determine mode and parallelism ---

if [[ "$SERVICE_HOST" != "localhost" && "$SERVICE_HOST" != "127.0.0.1" ]]; then
	EXTERNAL_MODE=true
	JOBS=1
	echo "External mode: connecting to $SERVICE_HOST:$SERVICE_PORT (sequential)"
else
	EXTERNAL_MODE=false
	if [[ "$JOBS" -eq 0 ]]; then
		JOBS=${#test_files[@]}
		max_jobs=$(nproc 2>/dev/null || echo 4)
		((JOBS > max_jobs)) && JOBS=$max_jobs
	fi
fi

((JOBS > ${#test_files[@]})) && JOBS=${#test_files[@]}
echo "Parallelism: $JOBS worker(s) for ${#test_files[@]} test(s)"

# --- Serened instance management (local mode only) ---

LOOP_PIDS=()
DATADIRS=()

cleanup() {
	echo "Cleaning up serened instances..."
	for pid in "${LOOP_PIDS[@]}"; do
		pkill -P "$pid" 2>/dev/null
		kill "$pid" 2>/dev/null
		wait "$pid" 2>/dev/null
	done
	for dir in "${DATADIRS[@]}"; do
		rm -rf "$dir"
	done
}

wait_for_port() {
	local host=$1 port=$2 timeout=${3:-60}
	for ((i = 0; i < timeout * 2; i++)); do
		if bash -c "echo > /dev/tcp/$host/$port" 2>/dev/null; then
			return 0
		fi
		sleep 0.5
	done
	return 1
}

if [[ "$EXTERNAL_MODE" == "false" ]]; then
	: "${BUILD_DIR:=build}"
	: "${WORKSPACE:=$(realpath "$SCRIPT_DIR/../../")}"

	if [[ ! -x "$WORKSPACE/$BUILD_DIR/bin/serened" ]]; then
		echo "ERROR: serened not found at $WORKSPACE/$BUILD_DIR/bin/serened"
		exit 1
	fi

	trap cleanup EXIT INT TERM

	echo "Spawning $JOBS serened instance(s)..."
	for ((i = 0; i < JOBS; i++)); do
		port=$((BASE_PORT + i))
		datadir=$(mktemp -d "${TMPDIR:-/tmp}/recovery-worker-${i}-XXXXXX")
		DATADIRS+=("$datadir")

		PORT=$port "$SCRIPT_DIR/run_serened_loop.sh" "$datadir" \
			>"${LOG_DIR:-/tmp}/serened-worker-${i}.log" 2>&1 &
		LOOP_PIDS+=($!)

		echo "  Worker $i: port=$port pid=${LOOP_PIDS[-1]}"
	done

	echo "Waiting for instances to become ready..."
	for ((i = 0; i < JOBS; i++)); do
		port=$((BASE_PORT + i))
		if ! wait_for_port localhost "$port"; then
			echo "ERROR: serened on port $port failed to start within 60s"
			exit 1
		fi
	done
	echo "All $JOBS instance(s) ready"
fi

# --- Distribute tests across workers (round-robin) ---

declare -a worker_tests
for ((i = 0; i < JOBS; i++)); do
	worker_tests[$i]=""
done

for ((t = 0; t < ${#test_files[@]}; t++)); do
	w=$((t % JOBS))
	if [[ -n "${worker_tests[$w]}" ]]; then
		worker_tests[$w]+=$'\n'
	fi
	worker_tests[$w]+="${test_files[$t]}"
done

# --- Run test workers in parallel ---

run_worker() {
	local worker_id=$1
	local host=$2
	local port=$3
	local tests="$4"
	local worker_exit=0

	while IFS= read -r test_file; do
		[[ -z "$test_file" ]] && continue
		echo "[worker $worker_id] Running: $test_file"

		./run.sh \
			--host "$host" \
			--single-port "$port" \
			--test "$test_file" \
			--junit "tests-serenedb-recovery-w${worker_id}" \
			--engines pg-wire-simple \
			"${RUNNER_ARGS[@]}"

		local exit_code=$?
		if [[ $exit_code != 0 ]]; then
			echo "[worker $worker_id] FAILED: $test_file (exit $exit_code)"
			worker_exit=$exit_code
		fi
	done <<<"$tests"

	return $worker_exit
}

declare -a worker_pids=()

for ((i = 0; i < JOBS; i++)); do
	if [[ "$EXTERNAL_MODE" == "true" ]]; then
		host=$SERVICE_HOST
		port=$SERVICE_PORT
	else
		host=localhost
		port=$((BASE_PORT + i))
	fi

	run_worker "$i" "$host" "$port" "${worker_tests[$i]}" &
	worker_pids+=($!)
done

# --- Collect results ---

final_exit_code=0

for ((i = 0; i < ${#worker_pids[@]}; i++)); do
	wait "${worker_pids[$i]}"
	exit_code=$?
	if [[ $exit_code != 0 ]]; then
		echo "Worker $i failed (exit $exit_code)"
		final_exit_code=$exit_code
	fi
done

if [[ $final_exit_code != 0 ]]; then
	echo "Recovery tests FAILED"
	exit $final_exit_code
fi

echo "All recovery tests passed"
exit 0
