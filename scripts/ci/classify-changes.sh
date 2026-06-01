#!/usr/bin/env bash

set -euo pipefail

BASE_REF="${1:-origin/main}"

git fetch --no-tags origin "${BASE_REF#origin/}" 2>/dev/null || true
changed="$(git diff --name-only "${BASE_REF}...HEAD")"

duckdb=false iresearch=false other_tp=false
declare -a pg_files=()

while IFS= read -r f; do
	[[ -z "$f" ]] && continue
	case "$f" in
	third_party/duckdb*) duckdb=true ;;
	libs/iresearch/* | third_party/iresearch.build/* | tests/libs/iresearch/* | resources/tests/iresearch/*) iresearch=true ;;
	third_party/*) other_tp=true ;;
	tests/sqllogic/pg/* | tests/sqllogic/any/pg/*) pg_files+=("${f#tests/sqllogic/}") ;;
	esac
done <<<"$changed"

extension=false
if [[ "$duckdb" == true || "$other_tp" == true ]]; then
	iresearch=true
	extension=true
fi

pg=false
pg_list=""
if ((${#pg_files[@]} > 0)); then
	pg=true
	pg_list=$(printf '%s\n' "${pg_files[@]}")
fi

emit() {
	echo "$1=$2"
	if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
		echo "$1=$2" >>"$GITHUB_OUTPUT"
	fi
}
emit_multiline() {
	local key="$1" val="$2"
	echo "$key (multiline):"
	printf '%s\n' "$val" | sed 's/^/  /'
	if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
		{
			echo "${key}<<__SDB_EOF__"
			printf '%s\n' "$val"
			echo "__SDB_EOF__"
		} >>"$GITHUB_OUTPUT"
	fi
}
emit iresearch "$iresearch"
emit extension "$extension"
emit duckdb "$duckdb"
emit pg "$pg"
emit_multiline pg_tests "$pg_list"
