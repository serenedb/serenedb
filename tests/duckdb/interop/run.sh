#!/bin/bash

set -uo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
WORKSPACE=$(cd "$SCRIPT_DIR/../../.." && pwd)

: "${BUILD_DIR:=build}"
: "${VANILLA_DUCKDB_TAG:=1.5.5}"
: "${INTEROP_STORAGE_VERSIONS:=v1.0.0 v1.5.0}"
: "${INTEROP_COMPRESSIONS:=uncompressed rle dictionary bitpacking fsst alp alprd zstd roaring dict_fsst}"

SERENED="$WORKSPACE/$BUILD_DIR/bin/serened"
if [[ ! -x "$SERENED" ]]; then
	echo "ERROR: $SERENED is not built" >&2
	exit 1
fi

if [[ -z "${VANILLA_DUCKDB:-}" ]]; then
	VANILLA_DUCKDB="$WORKSPACE/$BUILD_DIR/duckdb-$VANILLA_DUCKDB_TAG/duckdb"
	if [[ ! -x "$VANILLA_DUCKDB" ]]; then
		mkdir -p "$(dirname "$VANILLA_DUCKDB")"
		container=$(docker create "duckdb/duckdb:$VANILLA_DUCKDB_TAG") || exit 1
		docker cp "$container:/duckdb" "$VANILLA_DUCKDB" >/dev/null
		copied=$?
		docker rm "$container" >/dev/null
		[[ $copied -eq 0 ]] || exit 1
	fi
fi

WORK=$(mktemp -d "${TMPDIR:-/tmp}/duckdb-interop.XXXXXX")
trap 'rm -rf "$WORK"' EXIT

cases=0
failed=()

engine() {
	case "$1" in
	fork) "$SERENED" shell -bail -csv :memory: ;;
	vanilla) "$VANILLA_DUCKDB" -bail -csv :memory: ;;
	esac
}

other() {
	[[ "$1" == fork ]] && echo vanilla || echo fork
}

show() {
	echo "  $1"
	tail -n 20 "$2" | sed 's/^/    /'
}

apply() {
	local who=$1 db=$2 options=$3
	shift 3
	if ! {
		echo "ATTACH '$db' AS o${options:+ ($options)};"
		echo "USE o;"
		echo "SET checkpoint_threshold = '10GB';"
		cat "$@"
	} | engine "$who" >"$WORK/apply.log" 2>&1; then
		show "$who failed to write $(basename "$db"):" "$WORK/apply.log"
		return 1
	fi
}

compression_sql() {
	local method
	for method in $INTEROP_COMPRESSIONS; do
		cat <<EOF
SET force_compression = '$method';
CREATE TABLE o.main.c_$method AS
SELECT i AS id, i // 1000 AS runs, 42 AS konst, i / 7.0 AS dbl, CAST(i / 3.0 AS FLOAT) AS flt, 'v' || (i % 50) AS lowcard,
	'some_prefix_' || i AS highcard, i % 3 = 0 AS flag, CASE WHEN i % 7 = 0 THEN NULL ELSE i END AS nullable,
	DATE '2020-01-01' + (i % 3650)::INTEGER AS dt, TIMESTAMP '2020-01-01' + to_seconds(i) AS ts,
	(i / 1000.0)::DECIMAL(18, 3) AS dec, i::HUGEINT AS hi, [i, i + 1] AS lst, {'a': i % 10, 'b': 'x' || (i % 3)} AS st
FROM range(150000) r(i);
CHECKPOINT o;
EOF
	done
	echo "RESET force_compression;"
}

relations_sql() {
	cat <<'EOF'
.headers off
.mode list
SELECT 'SELECT ''' || schema_name || '.' || name || ''' AS relation, count(*) AS n, sum(hash(r)::HUGEINT) AS digest FROM (SELECT r FROM o."' || schema_name || '"."' || name || '" AS r);'
FROM (
	SELECT schema_name, table_name AS name FROM duckdb_tables() WHERE database_name = 'o'
	UNION ALL
	SELECT schema_name, view_name FROM duckdb_views() WHERE database_name = 'o' AND NOT internal
)
ORDER BY ALL;
SELECT 'SELECT ''' || table_name || ''' AS relation, column_name, segment_type, compression, count(*) AS segments FROM pragma_storage_info(''o.main.' || table_name || ''') GROUP BY ALL ORDER BY ALL;'
FROM duckdb_tables() WHERE database_name = 'o' AND table_name LIKE 'c\_%' ESCAPE '\'
ORDER BY ALL;
EOF
}

copy() {
	cp "$1" "$2"
	rm -f "$2.wal"
	if [[ -f "$1.wal" ]]; then
		cp "$1.wal" "$2.wal"
	fi
}

open_copy() {
	copy "$1" "$2"
	echo "ATTACH '$2' AS o;"
	echo "USE o;"
	echo "PRAGMA disable_checkpoint_on_shutdown;"
}

check() {
	local name=$1 db=$2 who
	shift 2
	if ! {
		open_copy "$db" "$WORK/$name.relations.db"
		echo ".output $WORK/$name.relations.sql"
		relations_sql
	} | engine fork >"$WORK/$name.relations.err" 2>&1; then
		show "fork failed to list the relations of $name:" "$WORK/$name.relations.err"
		return 1
	fi
	for who in fork vanilla; do
		if ! {
			open_copy "$db" "$WORK/$name.$who.db"
			echo ".output $WORK/$name.$who.csv"
			cat "$SCRIPT_DIR/check.sql" "$WORK/$name.relations.sql" "$@"
		} | engine "$who" >"$WORK/$name.$who.err" 2>&1; then
			show "$who failed to read $name:" "$WORK/$name.$who.err"
			return 1
		fi
	done
	if ! diff -u "$WORK/$name.fork.csv" "$WORK/$name.vanilla.csv" >"$WORK/$name.diff"; then
		echo "  fork and vanilla read $name differently:"
		head -n 60 "$WORK/$name.diff" | sed 's/^/    /'
		return 1
	fi
}

record() {
	cases=$((cases + 1))
	if [[ $2 -eq 0 ]]; then
		echo "  ok      $1"
	else
		echo "  FAILED  $1"
		failed+=("$1")
	fi
}

scenario() {
	local writer=$1 version=$2
	local reader name db
	reader=$(other "$writer")

	name="$writer-$version-checkpoint"
	db="$WORK/$name.db"
	apply "$writer" "$db" "STORAGE_VERSION '$version'" "$SCRIPT_DIR/base.sql" <(echo "CHECKPOINT o;") &&
		check "$name" "$db" "$SCRIPT_DIR/check_base.sql" "$SCRIPT_DIR/use_base.sql"
	record "$name" $?

	name="$writer-$version-wal"
	db="$WORK/$name.db"
	apply "$writer" "$db" "STORAGE_VERSION '$version'" <(echo "PRAGMA disable_checkpoint_on_shutdown;") \
		"$SCRIPT_DIR/base.sql" "$SCRIPT_DIR/changes.sql" &&
		check "$name" "$db" "$SCRIPT_DIR/check_changes.sql" "$SCRIPT_DIR/use_changes.sql"
	record "$name" $?

	name="$writer-$version-mixed"
	db="$WORK/$name.db"
	apply "$writer" "$db" "STORAGE_VERSION '$version'" "$SCRIPT_DIR/base.sql" <(echo "CHECKPOINT o;") &&
		apply "$writer" "$db" "" <(echo "PRAGMA disable_checkpoint_on_shutdown;") "$SCRIPT_DIR/changes.sql" &&
		check "$name" "$db" "$SCRIPT_DIR/check_changes.sql" "$SCRIPT_DIR/use_changes.sql"
	record "$name" $?

	name="$writer-$version-mixed-checkpointed-by-$reader"
	apply "$reader" "$db" "" <(echo "CHECKPOINT o;") &&
		check "$name" "$db" "$SCRIPT_DIR/check_changes.sql" "$SCRIPT_DIR/use_changes.sql"
	record "$name" $?

	name="$writer-$version-compression"
	db="$WORK/$name.db"
	apply "$writer" "$db" "STORAGE_VERSION '$version'" <(compression_sql) &&
		check "$name" "$db"
	record "$name" $?
}

echo "===== [duckdb interop] fork: $SERENED"
echo "===== [duckdb interop] vanilla: $("$VANILLA_DUCKDB" --version) ($VANILLA_DUCKDB)"
for version in $INTEROP_STORAGE_VERSIONS; do
	for writer in fork vanilla; do
		scenario "$writer" "$version"
	done
done

echo "===== [duckdb interop] $((cases - ${#failed[@]}))/$cases passed"
if [[ ${#failed[@]} -gt 0 ]]; then
	printf '  failed: %s\n' "${failed[@]}"
	exit 1
fi
