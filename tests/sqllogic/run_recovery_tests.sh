#!/bin/bash

# Runs recovery tests in parallel, each test file against its own serened instance.
#
# By default, spawns one serened per test file (capped by nproc). Each instance
# auto-restarts on crash via run_serened_loop.sh.
#
# Usage (local dev):
#   ./run_recovery_tests.sh                              # auto-parallel, all tests
#   ./run_recovery_tests.sh --jobs 4                     # 4 workers
#   ./run_recovery_tests.sh --fast                       # skip .test_slow files
#   SKIP_SLOW_TESTS=true ./run_recovery_tests.sh         # same as --fast
#
# Usage (Docker, new compose):
#   ./run_recovery_tests.sh --runner /sqllogictest-rs    # auto-parallel in container
#
# Usage (legacy external serened -- sequential fallback):
#   SERVICE_HOST=serenedb-recovery SERVICE_PORT=7777 ./run_recovery_tests.sh

set -o pipefail

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
FAST=${SKIP_SLOW_TESTS:-false}

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
	--fast)
		FAST=true
		shift
		;;
	*)
		echo "Unknown option: $1" >&2
		exit 1
		;;
	esac
done

# --- Discover test files ---

declare -a test_files=()
declare -a find_args=(recovery/ -type f \( -name "*.test" -o -name "*.test_slow" \))
if [[ "$FAST" == "true" ]]; then
	find_args=(recovery/ -type f -name "*.test")
fi
while IFS= read -r -d '' file; do
	test_files+=("${file#./}")
done < <(find "${find_args[@]}" -print0 | sort -z)

if [[ ${#test_files[@]} -eq 0 ]]; then
	echo "No test files found in recovery/ directory"
	exit 1
fi

echo "Found ${#test_files[@]} recovery test(s) (fast=$FAST)"

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

# Kill any leftover recovery serenedds from previous runs. The "recovery-worker-"
# datadir pattern is unique to this script, so we can safely pkill on it.
# Without this, orphans from prior aborted runs hold test ports and cascade-fail.
kill_recovery_orphans() {
	local pids
	pids=$(ps -eo pid,args --no-headers | grep -E "recovery-worker-|run_serened_loop\.sh" | grep -v grep | awk '{print $1}')
	if [[ -n "$pids" ]]; then
		echo "Killing leftover recovery processes: $(echo $pids | tr '\n' ' ')"
		echo "$pids" | xargs -r kill -9 2>/dev/null
		sleep 1
	fi
}

cleanup() {
	echo "Cleaning up serened instances..."
	# Workers run in subshells, so their pids/datadirs aren't visible here.
	# kill_recovery_orphans pattern-matches the loop + serened processes, and the
	# recovery-worker-* glob sweeps any datadir a worker didn't rm inline.
	kill_recovery_orphans
	rm -rf "${TMPDIR:-/tmp}"/recovery-worker-* 2>/dev/null || true
}

kill_recovery_orphans

# Kept after the run for post-mortem; its serened-logs-* name is outside the
# recovery-worker-* sweep in cleanup().
SERENED_LOG_DIR=$(mktemp -d "${TMPDIR:-/tmp}/serened-logs-XXXXXX")

if [[ "$EXTERNAL_MODE" == "false" ]]; then
	: "${BUILD_DIR:=build}"
	: "${WORKSPACE:=$(realpath "$SCRIPT_DIR/../../")}"

	SERENED="$WORKSPACE/$BUILD_DIR/bin/serened"
	if [[ ! -x "$SERENED" ]]; then
		echo "ERROR: serened not found at $SERENED"
		exit 1
	fi

	trap cleanup EXIT INT TERM
fi

# Pre-build sqllogictest once. Without this, each forked worker would invoke
# `cargo build` against the same target dir; cargo races on its lock files
# under that load and a worker can exit non-zero even when the cached binary
# is still functional (which then surfaces as a passing test reported as a
# failure). Workers honour SDB_SKIP_SQLLOGIC_BUILD and skip cargo entirely.
SQLLOGIC_RUNNER="${RUNNER_ARGS[1]:-$(realpath "$SCRIPT_DIR/../../third_party/sqllogictest-rs")}"
SQLLOGIC_TARGET="${CARGO_TARGET_DIR:-$(realpath "$SCRIPT_DIR/../../.cache/cargo-target")}"
mkdir -p "$SQLLOGIC_TARGET"
echo "Pre-building sqllogictest..."
# cargo's crates.io fetch flakes intermittently on the CI runners (HTTP/2
# framing errors, "unable to update registry"). It's transient, so retry a
# few times before giving up instead of failing the whole suite.
build_exit_code=0
build_attempts="${SDB_SQLLOGIC_BUILD_ATTEMPTS:-3}"
for attempt in $(seq 1 "$build_attempts"); do
	cargo build --manifest-path "$SQLLOGIC_RUNNER/sqllogictest-bin/Cargo.toml" \
		--target-dir "$SQLLOGIC_TARGET" --release --quiet
	build_exit_code=$?
	[[ $build_exit_code == 0 ]] && break
	if [[ $attempt -lt $build_attempts ]]; then
		echo "sqllogictest build failed (exit $build_exit_code); attempt $attempt/$build_attempts, retrying in 10s..." >&2
		sleep 10
	fi
done
if [[ $build_exit_code != 0 ]]; then
	echo "ERROR: failed to pre-build sqllogictest after $build_attempts attempt(s)" >&2
	exit $build_exit_code
fi
export SDB_SKIP_SQLLOGIC_BUILD=1

# Start a fresh serened instance (restart loop + empty datadir) and wait for
# the port to accept connections. Each test gets its own, so one test's crash
# cannot cascade into the next. Writes to: $log_file, tracks pid in $pid_var,
# tracks datadir in $dir_var.
start_fresh_serened() {
	local worker_id=$1 port=$2 log_file=$3 pid_var=$4 dir_var=$5

	local datadir
	datadir=$(mktemp -d "${TMPDIR:-/tmp}/recovery-worker-${worker_id}-XXXXXX")
	printf -v "$dir_var" '%s' "$datadir"

	PORT=$port "$SCRIPT_DIR/run_serened_loop.sh" "$datadir" >"$log_file" 2>&1 &
	local pid=$!
	printf -v "$pid_var" '%s' "$pid"

	local start_ts
	start_ts=$(date +%s.%N)
	local attempt
	for ((attempt = 0; attempt < 60; attempt++)); do
		if ! kill -0 "$pid" 2>/dev/null; then
			echo "  ERROR: worker $worker_id loop (pid $pid) died before port was up"
			return 1
		fi
		if python3 -c "import socket,sys; s=socket.socket(); s.settimeout(1); s.connect(('localhost',$port)); s.close()" 2>/dev/null; then
			echo "  worker $worker_id serened ready in $(awk "BEGIN{printf \"%.1f\", $(date +%s.%N) - $start_ts}")s"
			return 0
		fi
		sleep 1
	done
	echo "  ERROR: worker $worker_id not ready after 60s"
	return 1
}

stop_serened() {
	local pid=$1
	[[ -z "$pid" ]] && return
	local start_ts
	start_ts=$(date +%s.%N)
	# Kill children FIRST (serened) -- once the parent bash dies, its children
	# are reparented to init and pkill -P can no longer find them, leaving
	# orphaned serened processes holding the port. Since each test uses a
	# unique port + datadir, we don't need to wait for graceful shutdown.
	pkill -KILL -P "$pid" 2>/dev/null
	kill -KILL "$pid" 2>/dev/null
	wait "$pid" 2>/dev/null
	echo "  serened stopped in $(awk "BEGIN{printf \"%.1f\", $(date +%s.%N) - $start_ts}")s"
}

declare -a heavy=() light=()
for tf in "${test_files[@]}"; do
	case "$tf" in
	*stress* | *huge* | *_slow* | *loop*) heavy+=("$tf") ;;
	*) light+=("$tf") ;;
	esac
done
test_files=("${heavy[@]}" "${light[@]}")

# Slowest-first off the persisted timing cache when warm; the heavy-first split
# above is the cold-cache fallback (unknown tests keep that order).
if [[ -n "${SDB_TIMING_CACHE_DIR:-}" ]]; then
	recovery_cache="$SDB_TIMING_CACHE_DIR/pg-wire-simple${SDB_TIMING_KEY:+.$SDB_TIMING_KEY}.tsv"
	if ordered=$(printf '%s\n' "${test_files[@]}" | python3 "$SCRIPT_DIR/timing_cache.py" order "$recovery_cache" 2>/dev/null) && [[ -n "$ordered" ]]; then
		mapfile -t test_files <<<"$ordered"
	fi
fi

QUEUE_FILE="$SERENED_LOG_DIR/queue.txt"
QUEUE_IDX="$SERENED_LOG_DIR/queue.idx"
QUEUE_LOCK="$SERENED_LOG_DIR/queue.lock"
QUEUE_TOTAL=${#test_files[@]}
printf '%s\n' "${test_files[@]}" >"$QUEUE_FILE"
echo 0 >"$QUEUE_IDX"
: >"$QUEUE_LOCK"

pop_test() {
	(
		flock 200
		local i
		i=$(<"$QUEUE_IDX")
		((i >= QUEUE_TOTAL)) && exit 1
		echo $((i + 1)) >"$QUEUE_IDX"
		sed -n "$((i + 1))p" "$QUEUE_FILE"
	) 200>"$QUEUE_LOCK"
}

# --- Run test workers in parallel ---

run_worker() {
	local worker_id=$1
	local host=$2
	local port=$3
	local worker_exit=0
	local failures_file="$SERENED_LOG_DIR/failures-w${worker_id}.txt"
	: >"$failures_file"

	local test_idx=0
	local test_file
	while test_file=$(pop_test); do
		[[ -z "$test_file" ]] && continue

		# One serened per test: fresh datadir, fresh port, fresh process.
		# Ask the kernel for a free port each time. Fixed port schemes collide
		# with other developers' docker containers, IDE servers, etc.
		local test_port
		test_port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()')
		local serened_log="$SERENED_LOG_DIR/worker-${worker_id}-test-${test_idx}.log"
		echo "$test_file" >"$serened_log.test"
		local pid="" datadir=""

		echo
		echo "========================================================================"
		echo "[worker $worker_id] RUN  $test_file  (port $test_port, log $serened_log)"
		echo "========================================================================"

		local run_port=$test_port
		if [[ "$EXTERNAL_MODE" == "true" ]]; then
			run_port=$port
		else
			if ! start_fresh_serened "$worker_id" "$test_port" "$serened_log" pid datadir; then
				echo "[worker $worker_id] FAIL $test_file (serened did not start)"
				echo "--- serened log ---"
				sed "s/^/[srvd] /" "$serened_log"
				echo "--- end serened log ---"
				echo "$test_file" >>"$failures_file"
				# A partial start still created a loop process + datadir; reap both.
				stop_serened "$pid"
				[[ -n "$datadir" ]] && rm -rf "$datadir"
				worker_exit=1
				test_idx=$((test_idx + 1))
				continue
			fi
		fi

		SDB_SQLLOGIC_QUIET=1 ./run.sh \
			--host "$host" \
			--single-port "$run_port" \
			--test "$test_file" \
			--junit "tests-serenedb-recovery-w${worker_id}" \
			--engines pg-wire-simple \
			"${RUNNER_ARGS[@]}" 2>&1 | sed "s/^/[test] /"

		local exit_code=${PIPESTATUS[0]}

		if [[ "$EXTERNAL_MODE" == "false" ]]; then
			stop_serened "$pid"
			[[ -n "$datadir" ]] && rm -rf "$datadir"
		fi

		if [[ $exit_code != 0 ]]; then
			echo "------------------------------------------------------------------------"
			echo "[worker $worker_id] FAIL $test_file (exit $exit_code)"
			if [[ -f "$serened_log" ]]; then
				echo "--- serened log ---"
				sed "s/^/[srvd] /" "$serened_log"
				echo "--- end serened log ---"
			fi
			echo "$test_file" >>"$failures_file"
			worker_exit=$exit_code
		else
			echo "[worker $worker_id] PASS $test_file"
			# Keep logs only on failure to avoid clutter
			rm -f "$serened_log" "$serened_log.test"
		fi

		test_idx=$((test_idx + 1))
	done

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

	run_worker "$i" "$host" "$port" &
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

# --- Summary ---

declare -a all_failures=()
if [[ -n "${SERENED_LOG_DIR:-}" ]]; then
	for f in "$SERENED_LOG_DIR"/failures-w*.txt; do
		[[ -f "$f" ]] || continue
		while IFS= read -r line; do
			[[ -n "$line" ]] && all_failures+=("$line")
		done <"$f"
	done
fi

total=${#test_files[@]}
failed=${#all_failures[@]}
passed=$((total - failed))

echo
echo "========================================================================"
echo "Summary: $passed/$total passed, $failed failed"
echo "========================================================================"
if [[ $failed -gt 0 ]]; then
	echo "Failed tests:"
	for t in "${all_failures[@]}"; do
		echo "  - $t"
	done
	echo
	echo "Serened logs: $SERENED_LOG_DIR (per-worker worker-N.log)"
fi

# Copy retained per-test logs into LOG_DIR so CI can collect them as artifacts.
if [[ -n "${LOG_DIR:-}" && -d "$LOG_DIR" && -n "${SERENED_LOG_DIR:-}" ]]; then
	cp "$SERENED_LOG_DIR"/worker-*.log "$LOG_DIR/" 2>/dev/null || true
	cp "$SERENED_LOG_DIR"/failures-w*.txt "$LOG_DIR/" 2>/dev/null || true
fi

if [[ $final_exit_code != 0 ]]; then
	echo "Recovery tests FAILED"
	exit $final_exit_code
fi

echo "All recovery tests passed"
exit 0
