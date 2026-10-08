#!/usr/bin/env bash

set -uo pipefail

: "${WORKSPACE:=$(pwd)}"
OUT="${WORKSPACE}/out"
LOGS="${OUT}/logs"
SUMMARY="${GITHUB_STEP_SUMMARY:-/dev/null}"

failures=()
lines=()

add() {
	lines+=("| $1 | $2 |")
}

if [[ -s "${LOGS}/suites.tsv" ]]; then
	while IFS=$'\t' read -r name status; do
		add "$name" "exit $status"
		[[ "$status" =~ ^[0-9]+$ && "$status" -ne 0 ]] && failures+=("suite: $name")
	done <"${LOGS}/suites.tsv"
fi

if [[ -f "${LOGS}/sqllogic-tests.log" ]]; then
	ok=$(grep -c '\[OK\]' "${LOGS}/sqllogic-tests.log")
	failed=$(grep -c '\[FAILED\]' "${LOGS}/sqllogic-tests.log")
	add "sqllogic" "${ok} passed, ${failed} failed"
fi

if [[ -f "${LOGS}/recovery-tests.log" ]]; then
	add "recovery" "$(grep -oE 'Summary: [0-9]+/[0-9]+ passed, [0-9]+ failed' "${LOGS}/recovery-tests.log" | tail -1 | sed 's/^Summary: //')"
fi

if [[ -f "${LOGS}/duckdb-tests.log" ]]; then
	while read -r result; do
		add "duckdb" "$result"
	done < <(grep -oE '(All tests passed \(.*\)|test cases: .*)' "${LOGS}/duckdb-tests.log" | sort -u)
fi

for suite in serenedb-tests iresearch-tests; do
	if [[ -f "${LOGS}/${suite}.log" ]]; then
		total=$(grep -oE '^\[[0-9]+/[0-9]+\]' "${LOGS}/${suite}.log" | tail -1 | grep -oE '[0-9]+\]' | tr -d ']')
		add "$suite" "${total:-0} run"
	fi
done

if [[ -f "${LOGS}/drivers-tests.log" ]]; then
	while read -r lang tests fails errs _ status; do
		add "driver ${lang}" "${tests} tests, ${fails} failed, ${errs} errors, ${status}"
	done < <(sed -n 's/^.*|   \([a-z]\+ \+[-0-9]\+ \+[-0-9]\+ \+[-0-9]\+ \+[0-9]\+s \+[A-Z]\+\)$/\1/p' "${LOGS}/drivers-tests.log")
	while read -r drops; do
		add "sqlsmith dropped sessions" "$drops"
		[[ "$drops" -ne 0 ]] && failures+=("sqlsmith: ${drops} dropped sessions")
	done < <(grep -oE 'conn_drops=[0-9]+' "${LOGS}/drivers-tests.log" | cut -d= -f2)
fi

reports=0
for log in "${OUT}"/sanitizers/*/log*; do
	[[ -f "$log" ]] || continue
	n=$(grep -cE '^(WARNING: (ThreadSanitizer|MemorySanitizer)|==[0-9]+==ERROR: (AddressSanitizer|LeakSanitizer)|.*: runtime error: )' "$log")
	if [[ "$n" -ne 0 ]]; then
		reports=$((reports + n))
		echo "::error file=${log#"${WORKSPACE}"/}::${n} sanitizer report(s)"
		grep -m1 -E '^SUMMARY: ' "$log"
	fi
done
add "unsuppressed sanitizer reports" "$reports"
[[ "$reports" -ne 0 ]] && failures+=("sanitizers: ${reports} report(s)")

{
	echo "| check | result |"
	echo "| --- | --- |"
	printf '%s\n' "${lines[@]}"
	if [[ ${#failures[@]} -ne 0 ]]; then
		echo
		echo "**Failed:**"
		printf -- '- %s\n' "${failures[@]}"
	fi
} | tee -a "$SUMMARY"

for failure in "${failures[@]}"; do
	case "$failure" in
	sanitizers:* | sqlsmith:*) exit 1 ;;
	esac
done
exit 0
