#!/usr/bin/env bash

set -euo pipefail

BASE_REF="${1:-origin/main}"

git fetch --no-tags origin "${BASE_REF#origin/}" 2>/dev/null || true
changed="$(git diff --name-only "${BASE_REF}...HEAD")"

# Which DuckDB-level test suites a third_party directory can affect. Everything in
# ALL_SUITES_DIRS is either linked by DuckDB core itself (duckdb's link_libraries(),
# parquet's compression libs, our simdutf edge) or linked into every target globally
# (the SDB_ALLOC allocator, libc++/libc++abi/libunwind), so a change there can move
# any suite. The rest is attributed to the extension whose code path uses it; the
# mapping is derived from the CMake target graph, not from directory names.
#
# Directories that affect no DuckDB suite -- iresearch and the rest of the serenedb
# stack -- deliberately fall through to none here; they have their own gates.
ALL_SUITES="core avro azure httpfs iceberg inet markdown postgres_scanner"
ALL_SUITES_DIRS="abseil-cpp ada brotli fast_float fmt jemalloc jemalloc-cmake llvm-project lz4 re2 simdutf snappy tcmalloc zlib-ng zstd"

# dir -> space-separated suite list
declare -A SUITE_OF_DIR=(
	[duckdb_httpfs]="httpfs"
	[duckdb_azure]="azure"
	[azure-sdk-for-cpp]="azure"
	[azure-cmake]="azure"
	[libxml2]="azure"
	[libxml2-cmake]="azure"
	[duckdb_avro]="avro iceberg"
	[avro]="avro iceberg"
	[jansson]="avro iceberg"
	[duckdb_iceberg]="iceberg"
	[croaring]="iceberg"
	[aws-cmake]="iceberg"
	[aws-checksums]="iceberg"
	[aws-crt-cpp]="iceberg"
	[aws-sdk-cpp]="iceberg"
	[aws-c-auth]="iceberg"
	[aws-c-cal]="iceberg"
	[aws-c-common]="iceberg"
	[aws-c-compression]="iceberg"
	[aws-c-event-stream]="iceberg"
	[aws-c-http]="iceberg"
	[aws-c-io]="iceberg"
	[aws-c-mqtt]="iceberg"
	[aws-c-s3]="iceberg"
	[aws-c-sdkutils]="iceberg"
	[duckdb_postgres]="postgres_scanner"
	[postgres]="postgres_scanner"
	[database-connector]="postgres_scanner"
	[duckdb_inet]="inet"
	[duckdb_markdown]="markdown"
	[cmark-gfm]="markdown"
	[curl]="httpfs iceberg postgres_scanner"
	[openssl]="httpfs azure iceberg postgres_scanner"
	[openssl-cmake]="httpfs azure iceberg postgres_scanner"
)

iresearch=false other_tp=false
declare -A suite_hit=()
declare -a pg_files=()

while IFS= read -r f; do
	[[ -z "$f" ]] && continue
	case "$f" in
	libs/iresearch/* | third_party/iresearch.build/* | tests/libs/iresearch/* | resources/tests/iresearch/*)
		iresearch=true
		continue
		;;
	tests/sqllogic/pg/* | tests/sqllogic/any/pg/*)
		pg_files+=("${f#tests/sqllogic/}")
		continue
		;;
	esac

	case "$f" in
	third_party/duckdb/* | third_party/CMakeLists.txt)
		# The fork itself (including its own vendored third_party: mbedtls, ICU,
		# yyjson...) and the wiring that decides which vendored copy every dep
		# resolves to. Either can move any suite.
		other_tp=true
		for s in $ALL_SUITES; do suite_hit[$s]=1; done
		;;
	third_party/*)
		dir="${f#third_party/}"
		dir="${dir%%/*}"
		other_tp=true
		case " $ALL_SUITES_DIRS " in
		*" $dir "*)
			for s in $ALL_SUITES; do suite_hit[$s]=1; done
			;;
		*)
			for s in ${SUITE_OF_DIR[$dir]:-}; do suite_hit[$s]=1; done
			;;
		esac
		;;
	esac
done <<<"$changed"

if [[ "$other_tp" == true ]]; then
	iresearch=true
fi

# Ordered so the list reads the same way run.sh --list prints it.
duckdb_suites=""
for s in $ALL_SUITES; do
	[[ -n "${suite_hit[$s]:-}" ]] && duckdb_suites="${duckdb_suites:+$duckdb_suites,}$s"
done

# No separate "any pg tests changed" flag: the list itself is the signal, so the
# gate downstream is `pg_tests != ''`. It is only emitted when non-empty, since a
# heredoc output of one blank line would otherwise read as set.
pg_list=""
if ((${#pg_files[@]} > 0)); then
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
emit duckdb_suites "$duckdb_suites"
[[ -n "$pg_list" ]] && emit_multiline pg_tests "$pg_list"
exit 0
