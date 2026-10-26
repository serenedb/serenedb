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
ALL_SUITES="core cpp avro azure httpfs iceberg inet markdown postgres_scanner spatial interop"
ALL_SUITES_DIRS="abseil-cpp ada brotli fast_float fmt jemalloc jemalloc-cmake llvm-project lz4 re2 simdutf snappy tcmalloc zlib-ng zstd"

# dir -> space-separated suite list
declare -A SUITE_OF_DIR=(
	[duckdb_httpfs]="httpfs"
	[duckdb_spatial]="spatial"
	[geos]="spatial"
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

iresearch=false other_tp=false re2=false pg=false everything=false
declare -A suite_hit=()

while IFS= read -r f; do
	[[ -z "$f" ]] && continue
	case "$f" in
	CMakeLists.txt | CMakePresets.json | cmake/* | scripts/ci/build-ubuntu.Dockerfile | scripts/ci/build.bash | \
		scripts/ci/config.sh | scripts/ci/test.py | scripts/ci/run_shard.sh | scripts/ci/testlib/* | \
		tests/drivers/harness/*)
		everything=true
		;;
	esac
	case "$f" in
	third_party/re2 | third_party/re2/* | third_party/abseil-cpp | third_party/abseil-cpp/* | \
		third_party/googletest | third_party/googletest/* | third_party/simdutf | third_party/simdutf/* | \
		third_party/duckdb | third_party/duckdb/* | third_party/llvm-project | third_party/llvm-project/* | \
		third_party/jemalloc | third_party/jemalloc/* | third_party/jemalloc-cmake | third_party/jemalloc-cmake/* | \
		third_party/tcmalloc | third_party/tcmalloc/* | third_party/CMakeLists.txt)
		re2=true
		;;
	esac
	case "$f" in
	iresearch/* | tests/iresearch/* | resources/tests/iresearch/*)
		iresearch=true
		continue
		;;
	tests/sqllogic/pg/* | tests/sqllogic/any/pg/*)
		pg=true
		continue
		;;
	tests/duckdb/interop/*)
		suite_hit[interop]=1
		continue
		;;
	tests/duckdb/config/*.json)
		suite="${f#tests/duckdb/config/}"
		suite="${suite%.json}"
		if [[ " $ALL_SUITES " == *" $suite "* ]]; then
			suite_hit[$suite]=1
		else
			for s in $ALL_SUITES; do suite_hit[$s]=1; done
		fi
		continue
		;;
	tests/duckdb/*.md)
		continue
		;;
	tests/duckdb/*)
		for s in $ALL_SUITES; do suite_hit[$s]=1; done
		continue
		;;
	esac

	case "$f" in
	third_party/duckdb | third_party/CMakeLists.txt)
		# A bump of the fork submodule (which carries its own vendored third_party:
		# mbedtls, ICU, yyjson...) and the wiring that decides which vendored copy
		# every dep resolves to. Either can move any suite.
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
if [[ "$everything" == true ]]; then
	iresearch=true re2=true pg=true
	for s in $ALL_SUITES; do suite_hit[$s]=1; done
fi

duckdb_suites=""
for s in $ALL_SUITES; do
	[[ -n "${suite_hit[$s]:-}" ]] && duckdb_suites="${duckdb_suites:+$duckdb_suites,}$s"
done

emit() {
	echo "$1=$2"
	if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
		echo "$1=$2" >>"$GITHUB_OUTPUT"
	fi
}
emit iresearch "$iresearch"
emit duckdb_suites "$duckdb_suites"
emit re2 "$re2"
emit pg "$pg"
emit everything "$everything"
