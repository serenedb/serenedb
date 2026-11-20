#!/bin/bash

set -uo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
WORKSPACE=$(cd "$SCRIPT_DIR/../../.." && pwd)

: "${BUILD_DIR:=build}"
: "${VANILLA_DUCKDB_TAG:=2.0.0}"
: "${INTEROP_STORAGE_VERSIONS:=v1.0.0 v2.0.0}"
: "${INTEROP_COMPRESSIONS:=uncompressed rle dictionary bitpacking fsst alp alprd zstd roaring dict_fsst}"

SERENEDB="$WORKSPACE/$BUILD_DIR/bin/serenedb"
if [[ ! -x "$SERENEDB" ]]; then
	echo "ERROR: $SERENEDB is not built" >&2
	exit 1
fi

fetch_vanilla() {
	local tag=$1 binary=$2 container copied
	[[ -x "$binary" ]] && return 0
	mkdir -p "$(dirname "$binary")"
	container=$(docker create ${SDB_DOCKER_OWNER:+--label "serenedb.owner=$SDB_DOCKER_OWNER"} "duckdb/duckdb:$tag") || return 1
	docker cp "$container:/duckdb" "$binary" >/dev/null
	copied=$?
	docker rm "$container" >/dev/null
	return $copied
}

if [[ -z "${VANILLA_DUCKDB:-}" ]]; then
	VANILLA_DUCKDB="$WORKSPACE/$BUILD_DIR/duckdb-$VANILLA_DUCKDB_TAG/duckdb"
	fetch_vanilla "$VANILLA_DUCKDB_TAG" "$VANILLA_DUCKDB" || exit 1
fi

VANILLA_HOME=$(dirname "$VANILLA_DUCKDB")
HOME="$VANILLA_HOME" "$VANILLA_DUCKDB" -bail :memory: -c "INSTALL quack" >/dev/null || exit 1

WORK=$(mktemp -d "${TMPDIR:-/tmp}/duckdb-interop.XXXXXX")
quack_pids=()
trap 'kill "${quack_pids[@]}" 2>/dev/null; rm -rf "$WORK"' EXIT

cases=0
failed=()

engine() {
	case "$1" in
	fork)
		sed -E 's/\bduckdb_(schemas|tables|columns|views|indexes|constraints|sequences|types|functions)\(/sdb_\1(/g' |
			"$SERENEDB" shell -bail -csv :memory:
		;;
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

expect() {
	local what=$1 got=$2 want=$3
	if ! diff -u "$want" "$got" >"$got.diff"; then
		echo "  $what:"
		head -n 60 "$got.diff" | sed 's/^/    /'
		return 1
	fi
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
	expect "fork and vanilla read $name differently" "$WORK/$name.vanilla.csv" "$WORK/$name.fork.csv"
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

free_port() {
	python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'
}

wait_port() {
	local deadline=$((SECONDS + 60))
	until (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; do
		((SECONDS < deadline)) || return 1
		sleep 0.2
	done
}

quack_vanilla_client() {
	local pg quack pid remote
	pg=$(free_port)
	quack=$(free_port)
	"$SERENEDB" server "$WORK/quack-server" --listen="postgres://127.0.0.1:$pg,http://127.0.0.1:$quack?api=quack" \
		>"$WORK/quack-server.log" 2>&1 &
	pid=$!
	quack_pids+=("$pid")
	if ! wait_port "$quack"; then
		show "serened did not start:" "$WORK/quack-server.log"
		return 1
	fi
	remote="quack:127.0.0.1:$quack"
	HOME="$VANILLA_HOME" "$VANILLA_DUCKDB" -bail -csv :memory: >"$WORK/quack-vanilla-client.csv" 2>&1 <<EOF
LOAD quack;
SELECT * FROM quack_query('$remote', 'CREATE TABLE v3 AS SELECT i AS id, ''n'' || i AS name FROM range(1000) r(i)', token := 'postgres:');
SELECT * FROM quack_query('$remote', 'SELECT count(*) AS n, sum(id) AS s, count(DISTINCT name) AS d FROM v3', token := 'postgres:');
SELECT * FROM quack_query('$remote', 'INSERT INTO v3 VALUES (1000, NULL)', token := 'postgres:');
SELECT * FROM quack_query('$remote', 'SELECT id, name FROM v3 WHERE id >= 998 ORDER BY id', token := 'postgres:');
ATTACH '$remote' AS s (TOKEN 'postgres:');
SELECT count(*) AS n, max(name) AS m FROM s.public.v3;
SELECT schema_name FROM duckdb_schemas() WHERE database_name = 's' AND schema_name IN ('public', 'pg_catalog', 'information_schema') ORDER BY schema_name;
EOF
	kill "$pid"
	wait "$pid"
	expect "a vanilla quack client reads serened differently" "$WORK/quack-vanilla-client.csv" <(
		cat <<'EOF'
Count
1000
n,s,d
1000,499500,1000
Count
1
id,name
998,n998
999,n999
1000,NULL
n,m
1001,n999
schema_name
public
EOF
	)
}

quack_vanilla_server() {
	local port pid feed
	port=$(free_port)
	mkfifo "$WORK/quack-vanilla.in"
	HOME="$VANILLA_HOME" "$VANILLA_DUCKDB" -bail -csv :memory: <"$WORK/quack-vanilla.in" \
		>"$WORK/quack-vanilla.log" 2>&1 &
	pid=$!
	quack_pids+=("$pid")
	exec {feed}>"$WORK/quack-vanilla.in"
	echo "LOAD quack;
CREATE TABLE served AS SELECT i AS id, 'n' || i AS name FROM range(5000) r(i);
CALL quack_serve('quack:127.0.0.1:$port', token := 'interop');" >&$feed
	if ! wait_port "$port"; then
		exec {feed}>&-
		show "vanilla did not serve:" "$WORK/quack-vanilla.log"
		return 1
	fi
	"$SERENEDB" shell -bail -csv :memory: >"$WORK/quack-fork-client.csv" 2>&1 <<EOF
ATTACH 'quack:127.0.0.1:$port' AS v (TOKEN 'interop');
SELECT count(*) AS n, sum(id) AS s FROM v.served;
SELECT * FROM v.query('SELECT 42 AS answer');
CREATE TABLE v.made_by_fork AS SELECT 7 AS x;
INSERT INTO v.served VALUES (5000, 'last');
SELECT count(*) AS n, max(name) AS name FROM v.served WHERE id >= 4999;
SELECT x FROM v.made_by_fork;
EOF
	echo ".quit" >&$feed
	exec {feed}>&-
	wait "$pid"
	expect "serened reads a vanilla quack server differently" "$WORK/quack-fork-client.csv" <(
		cat <<'EOF'
n,s
5000,12497500
answer
42
Count
1
Count
1
n,name
2,n4999
x
7
EOF
	)
}

# Both peers over TLS with a vanilla self-signed certificate, which a client trusts by its pinned fingerprint
quack_tls() {
	local keys fingerprint wrong port pid feed pg quack server
	keys="$WORK/quack-keys"
	mkdir -p "$keys"
	fingerprint=$(HOME="$VANILLA_HOME" "$VANILLA_DUCKDB" -bail -noheader -list :memory: \
		-c "LOAD quack; SELECT fingerprint FROM quack_generate_keys(directory := '$keys')") || return 1
	wrong=$([[ ${fingerprint:0:1} == 0 ]] && echo 1 || echo 0)${fingerprint:1}
	port=$(free_port)
	mkfifo "$WORK/quack-tls.in"
	HOME="$VANILLA_HOME" "$VANILLA_DUCKDB" -bail -csv :memory: <"$WORK/quack-tls.in" >"$WORK/quack-tls.log" 2>&1 &
	pid=$!
	quack_pids+=("$pid")
	exec {feed}>"$WORK/quack-tls.in"
	echo "LOAD quack;
CREATE TABLE served AS SELECT 7 AS x;
CALL quack_serve('quack:127.0.0.1:$port', token := 'interop', disable_ssl := false,
                 ssl_cert_file := '$keys/server.pem', ssl_key_file := '$keys/private_key.pem');" >&$feed
	if ! wait_port "$port"; then
		exec {feed}>&-
		show "vanilla did not serve over TLS:" "$WORK/quack-tls.log"
		return 1
	fi
	"$SERENEDB" shell -bail -csv :memory: >"$WORK/quack-tls-fork-client.csv" 2>&1 <<EOF
ATTACH 'quack:127.0.0.1:$port' AS v (TOKEN 'interop', SSL_FINGERPRINT '$fingerprint');
SELECT x FROM v.served;
SELECT * FROM quack_query('quack:127.0.0.1:$port', 'SELECT 41 AS a', token := 'interop', ssl_fingerprint := '$fingerprint');
CREATE SECRET pinned (TYPE quack, SCOPE 'quack:127.0.0.1:$port', TOKEN 'interop', SSL_FINGERPRINT 'sha256:${fingerprint,,}');
ATTACH 'quack:127.0.0.1:$port' AS by_scope;
SELECT x FROM by_scope.served;
ATTACH 'quack:127.0.0.1:$port' AS by_name (SECRET 'pinned');
SELECT x FROM by_name.served;
EOF
	"$SERENEDB" shell -csv :memory: >"$WORK/quack-tls-refused.log" 2>&1 <<EOF
ATTACH 'quack:127.0.0.1:$port' AS unpinned (TOKEN 'interop', DISABLE_SSL false);
ATTACH 'quack:127.0.0.1:$port' AS wrong (TOKEN 'interop', SSL_FINGERPRINT '$wrong');
EOF
	echo ".quit" >&$feed
	exec {feed}>&-
	wait "$pid"
	pg=$(free_port)
	quack=$(free_port)
	"$SERENEDB" server "$WORK/quack-tls-server" --listen="postgres://127.0.0.1:$pg,https://127.0.0.1:$quack?api=quack" \
		--tls_cert="$keys/server.pem" --tls_key="$keys/private_key.pem" >"$WORK/quack-tls-server.log" 2>&1 &
	server=$!
	quack_pids+=("$server")
	if ! wait_port "$quack"; then
		show "serened did not serve over TLS:" "$WORK/quack-tls-server.log"
		return 1
	fi
	HOME="$VANILLA_HOME" "$VANILLA_DUCKDB" -bail -csv :memory: >"$WORK/quack-tls-vanilla-client.csv" 2>&1 <<EOF
LOAD quack;
SELECT * FROM quack_query('quack:127.0.0.1:$quack', 'SELECT 43 AS a', token := 'postgres:', ssl_fingerprint := '$fingerprint');
EOF
	kill "$server"
	wait "$server"
	expect "serened reads a vanilla quack server over TLS differently" "$WORK/quack-tls-fork-client.csv" <(
		cat <<'EOF'
x
7
a
41
Success
true
x
7
x
7
EOF
	) || return 1
	if [[ $(grep -c 'SSL peer certificate or SSH remote key was not OK' "$WORK/quack-tls-refused.log") -ne 2 ]]; then
		show "serened trusted a vanilla certificate it was not pinned to:" "$WORK/quack-tls-refused.log"
		return 1
	fi
	expect "a vanilla quack client reads serened over TLS differently" "$WORK/quack-tls-vanilla-client.csv" <(
		cat <<'EOF'
a
43
EOF
	)
}

echo "===== [duckdb interop] fork: $SERENEDB"
echo "===== [duckdb interop] vanilla: $("$VANILLA_DUCKDB" --version) ($VANILLA_DUCKDB)"
for version in $INTEROP_STORAGE_VERSIONS; do
	for writer in fork vanilla; do
		scenario "$writer" "$version"
	done
done

quack_vanilla_client
record "quack-vanilla-client" $?
quack_vanilla_server
record "quack-vanilla-server" $?
quack_tls
record "quack-tls" $?

echo "===== [duckdb interop] $((cases - ${#failed[@]}))/$cases passed"
if [[ ${#failed[@]} -gt 0 ]]; then
	printf '  failed: %s\n' "${failed[@]}"
	exit 1
fi
