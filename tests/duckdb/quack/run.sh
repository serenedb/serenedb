#!/bin/bash

set -uo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
WORKSPACE=$(cd "$SCRIPT_DIR/../../.." && pwd)

: "${BUILD_DIR:=build}"
: "${DUCKDB_JOBS:=$(nproc 2>/dev/null || echo 4)}"

QUACK_DIR="$WORKSPACE/third_party/duckdb_quack"
SERENED="$WORKSPACE/$BUILD_DIR/bin/serened"
UNITTEST="$WORKSPACE/$BUILD_DIR/third_party/duckdb/test/unittest"
CONFIG="$SCRIPT_DIR/../config/quack.json"
for bin in "$SERENED" "$UNITTEST"; do
	if [[ ! -x "$bin" ]]; then
		echo "ERROR: $bin is not built" >&2
		exit 1
	fi
done

WORK=$(mktemp -d "${TMPDIR:-/tmp}/duckdb-quack.XXXXXX")
trap 'rm -rf "$WORK"' EXIT

free_port() {
	python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

# One test file against a fresh serened, which serves quack with the token the templates attach with. The
# server reads files from the client's working directory, as the in-process server of the upstream suite does.
run_one() {
	local test=$1 dir pid port rc attempt ready_by
	dir=$(mktemp -d "$WORK/test.XXXXXX")
	echo "$test" >"$dir/name"
	for attempt in 1 2 3; do
		port=$(free_port)
		(cd "$QUACK_DIR" && exec "$SERENED" "$dir/data.$attempt" \
			"--listen=postgres://127.0.0.1:$(free_port),http://127.0.0.1:$port?api=quack" \
			--auth_bearer_token=asdf) >"$dir/serened.log" 2>&1 &
		pid=$!
		ready_by=$((SECONDS + 60))
		while kill -0 "$pid" 2>/dev/null && ! curl -s -o /dev/null "http://127.0.0.1:$port/quack"; do
			if ((SECONDS > ready_by)); then
				kill "$pid"
				break
			fi
			sleep 0.1
		done
		kill -0 "$pid" 2>/dev/null && break
		wait "$pid"
	done
	QUACK_SERVER="quack:127.0.0.1:$port" timeout 300 "$UNITTEST" --test-dir "$WORKSPACE/third_party/duckdb" \
		--test-config "$CONFIG" --env-passthrough QUACK_SERVER "$test" >"$dir/unittest.log" 2>&1
	rc=$?
	kill "$pid" 2>/dev/null
	wait "$pid"
	if [[ $rc -ne 0 ]] || ! grep -qE '^All tests passed|^All tests were skipped' "$dir/unittest.log"; then
		echo FAILED >"$dir/result"
	elif grep -q '^All tests were skipped' "$dir/unittest.log"; then
		echo skipped >"$dir/result"
	else
		echo ok >"$dir/result"
	fi
}
export -f run_one free_port
export WORK WORKSPACE QUACK_DIR SERENED UNITTEST CONFIG

mapfile -t skips < <(python3 -c 'import json, sys
for group in json.load(open(sys.argv[1]))["skip_tests"]:
    print("\n".join(group["paths"]))' "$CONFIG")
tests=()
while IFS= read -r test; do
	[[ " ${skips[*]} " == *" ${test#"$QUACK_DIR"/} "* ]] && continue
	tests+=("$test")
done < <(find "$QUACK_DIR/test" -name '*.test' -o -name '*.test_slow' | sort)

echo "===== [duckdb quack] serened: $SERENED"
echo "===== [duckdb quack] ${#tests[@]} files, ${#skips[@]} skipped by $CONFIG, $DUCKDB_JOBS at a time"
printf '%s\0' "${tests[@]}" | xargs -0 -P "$DUCKDB_JOBS" -I{} bash -c 'run_one "$1"' _ {}

passed=0
failed=()
for dir in "$WORK"/test.*; do
	name=$(<"$dir/name")
	result=$(cat "$dir/result" 2>/dev/null || echo FAILED)
	printf '  %-8s%s\n' "$result" "${name#"$QUACK_DIR"/}"
	if [[ "$result" == FAILED ]]; then
		failed+=("$dir")
	else
		passed=$((passed + 1))
	fi
done
for dir in "${failed[@]}"; do
	echo "----- $(<"$dir/name")"
	grep -vE '^\[[0-9]+/[0-9]+\]' "$dir/unittest.log" | tail -n 40 | sed 's/^/    /'
	echo "    serened:"
	tail -n 10 "$dir/serened.log" | sed 's/^/      /'
done

echo "===== [duckdb quack] $passed/${#tests[@]} passed"
[[ ${#failed[@]} -eq 0 ]]
