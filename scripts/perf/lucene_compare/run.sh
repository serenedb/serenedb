#!/usr/bin/env bash
# Lucene vs SereneDB inverted index vs PostgreSQL native indexes on INET, INT
# and VARCHAR columns: ingestion until searchable, index size and count(*)
# latency on the same prepared data. See README.md.
#
#   run.sh prepare                        generate the datasets once
#   run.sh bench [inet|int|string|all]    load, index, query, write the report
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(cd "$here/../../.." && pwd)
data=${SERENEDB_LUCENE_BENCH_DATA:-$HOME/.cache/serenedb-lucene-bench}
serened=${SERENED:-$repo/build_bench_no_lto/bin/serened}
pg_image=${PG_IMAGE:-postgres:18.6}
java_image=${JAVA_IMAGE:-eclipse-temurin:21.0.12.1_1-jdk}
lucene_version=10.5.2
pgjdbc_version=42.7.14
maven=https://repo1.maven.org/maven2
pg_settings=(-c shared_buffers=2GB -c maintenance_work_mem=1GB -c max_wal_size=8GB)
java_opts=(-Xms4g -Xmx8g --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector)

fetch_jar() {
	local jar=$data/jars/$2
	[[ -s $jar ]] && return
	mkdir -p "$data/jars"
	curl -fsSL --max-time 300 -o "$jar.part" "$maven/$1/$2"
	local want
	want=$(curl -fsSL --max-time 60 "$maven/$1/$2.sha1")
	if [[ $(sha1sum "$jar.part" | cut -d' ' -f1) != "$want" ]]; then
		rm -f "$jar.part"
		echo "checksum mismatch for $2" >&2
		exit 1
	fi
	mv "$jar.part" "$jar"
}

java_name=

in_java() {
	java_name=serenedb-lucene-bench-java-$$
	docker run --rm --name "$java_name" --network host --user "$(id -u):$(id -g)" \
		-v "$data:$data" -v "$here:$here:ro" -w "$data" "$@"
	java_name=
}

compile() {
	local image
	for image in "$java_image" "$pg_image"; do
		docker image inspect "$image" >/dev/null 2>&1 || docker pull -q "$image" >/dev/null
	done
	fetch_jar "org/apache/lucene/lucene-core/$lucene_version" "lucene-core-$lucene_version.jar"
	fetch_jar "org/postgresql/postgresql/$pgjdbc_version" "postgresql-$pgjdbc_version.jar"
	local jars="$data/jars/lucene-core-$lucene_version.jar:$data/jars/postgresql-$pgjdbc_version.jar"
	if [[ ! -f $data/classes/Bench.class || $here/Bench.java -nt $data/classes/Bench.class ]]; then
		mkdir -p "$data/classes"
		in_java "$java_image" javac -d "$data/classes" -cp "$jars" "$here/Bench.java"
	fi
	classpath="$data/classes:$jars"
}

free_port() {
	local port
	for _ in $(seq 200); do
		port=$((20000 + RANDOM % 30000))
		if [[ -z $(ss -ltnH "sport = :$port") && $port != "${1:-}" ]]; then
			echo "$port"
			return
		fi
	done
	echo "no free port found" >&2
	exit 1
}

say() {
	echo "$@" | tee -a "$report"
}

load() {
	local top_out idle busy
	top_out=$(top -b -n 2 -d 1 -o %CPU -w 200)
	idle=$(awk '/^%Cpu/ {v = $0} END {sub(/.* ni, */, "", v); sub(/ id.*/, "", v); print v}' <<<"$top_out")
	busy=$(awk '/^top -/ {n++} n == 2 && $1 ~ /^[0-9]+$/ && k < 3 {printf "%s (%s) %s%%; ", $12, $2, $9; k++}' <<<"$top_out")
	say "- $1: load average $(cut -d' ' -f1-3 /proc/loadavg), CPU idle ${idle}%, busiest: ${busy%; }"
}

sdb_pid=
sdb_dir=
pg_name=
pg_dir=

stop_servers() {
	if [[ -n $java_name ]]; then
		docker stop -t 10 "$java_name" >/dev/null 2>&1 || true
	fi
	java_name=
	if [[ -n $sdb_pid ]] && kill -0 "$sdb_pid" 2>/dev/null; then
		kill "$sdb_pid"
		wait "$sdb_pid" 2>/dev/null || true
	fi
	sdb_pid=
	if [[ -n $pg_name ]]; then
		docker stop -t 10 "$pg_name" >/dev/null 2>&1 || true
	fi
	pg_name=
	if [[ -n $sdb_dir ]]; then
		rm -rf "$sdb_dir"
	fi
	sdb_dir=
	if [[ -n $pg_dir ]]; then
		rm -rf "$pg_dir"
	fi
	pg_dir=
}

start_servers() {
	local type=$1
	sdb_port=$(free_port)
	sdb_dir=$(mktemp -d "$data/serened.XXXXXX")
	"$serened" "$sdb_dir" --listen="postgres://127.0.0.1:$sdb_port" >"$logs/serened-$type.log" 2>&1 &
	sdb_pid=$!
	pg_port=$(free_port "$sdb_port")
	pg_name=serenedb-lucene-bench-$$-$type
	pg_dir=$(mktemp -d "$data/pgdata.XXXXXX")
	docker run -d --rm --name "$pg_name" --network host --user "$(id -u):$(id -g)" \
		-v /etc/passwd:/etc/passwd:ro -e POSTGRES_HOST_AUTH_METHOD=trust -e PGDATA=/pgdata \
		-v "$pg_dir:/pgdata" -v "$data:/data:ro" --shm-size=2g "$pg_image" \
		-c listen_addresses=127.0.0.1 -c port="$pg_port" "${pg_settings[@]}" >/dev/null
	for _ in $(seq 240); do
		if ! kill -0 "$sdb_pid" 2>/dev/null; then
			echo "serened exited, see $logs/serened-$type.log" >&2
			exit 1
		fi
		if [[ -n $(ss -ltnH "sport = :$sdb_port") ]] &&
			docker exec "$pg_name" pg_isready -q -h 127.0.0.1 -p "$pg_port"; then
			return
		fi
		sleep 0.5
	done
	echo "servers did not come up in 120 s" >&2
	exit 1
}

run_type() {
	local type=$1 start
	say "## $type"
	say
	start_servers "$type"
	load "before"
	start=$(date +%s)
	say
	in_java -e SDB_PORT="$sdb_port" -e SDB_DATA="$sdb_dir" -e PG_PORT="$pg_port" -e PG_DATA_MOUNT=/data \
		-e REPORT="$report" -e PLANS="$plans" "$java_image" timeout 900 java "${java_opts[@]}" -cp "$classpath" \
		Bench bench "$data" "$type"
	say "- total wall time for \`$type\` incl. server start: $(($(date +%s) - start)) s"
	load "after"
	say
	stop_servers
}

header() {
	local dirty
	dirty=$(git -C "$repo" status --porcelain --untracked-files=no | wc -l)
	say "# Lucene vs SereneDB vs PostgreSQL index benchmark"
	say
	say "- $(date -Iseconds), $(nproc) cores ($(awk -F': ' '/model name/ {print $2; exit}' /proc/cpuinfo)), $(awk '/MemTotal/ {printf "%.0f GB", $2 / 1e6}' /proc/meminfo) RAM, shared with CI runners"
	say "- SereneDB: \`$serened\` built $(date -r "$serened" -Iseconds), git $(git -C "$repo" rev-parse --short HEAD) with $dirty modified tracked files"
	say "- Lucene $lucene_version (lucene-core, in-process in the benchmark JVM): docker \`$java_image\` ($(docker run --rm "$java_image" java -version 2>&1 | head -1)), \`--network host\`, index on the host disk in \`$data\`, MMapDirectory, \`IndexSearcher.count\` with the query cache off"
	say "- PostgreSQL: docker \`$pg_image\` ($(docker image inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$pg_image" | grep '^PG_VERSION=' | cut -d= -f2)), \`--network host\`, PGDATA on the host disk in \`$data\`, \`${pg_settings[*]}\`, other settings default"
	say "- SereneDB: native process on the host, data on the host disk in \`$data\`"
	say "- SereneDB and PostgreSQL over pg-wire: pgjdbc $pgjdbc_version, one connection, one server-side prepared \`SELECT count(*)\`, the same run count per query for all engines"
	local sizes=
	for f in hosts networks ints strings; do
		sizes+="$f $(wc -l <"$data/$f.csv") rows, "
	done
	say "- datasets in \`$data\` (seeded, from \`run.sh prepare\`): ${sizes%, }"
	say
}

trap stop_servers EXIT

case ${1:-} in
prepare)
	compile
	in_java "$java_image" java "${java_opts[@]}" -cp "$classpath" Bench prepare "$data"
	;;
bench)
	case ${2:-all} in
	all) types=(inet int string) ;;
	inet | int | string) types=("$2") ;;
	*)
		echo "usage: $0 bench [inet|int|string|all]" >&2
		exit 2
		;;
	esac
	[[ -x $serened ]] || {
		echo "no serened at $serened: ninja -C build_bench_no_lto serened" >&2
		exit 1
	}
	for f in hosts networks ints strings; do
		[[ -f $data/$f.csv ]] || {
			echo "$data/$f.csv is missing: $0 prepare" >&2
			exit 1
		}
	done
	compile
	stamp=$(date +%Y%m%d-%H%M%S)
	mkdir -p "$data/reports"
	logs=$data/reports/$stamp-logs
	mkdir -p "$logs"
	report=$data/reports/$stamp-${2:-all}.md
	plans=$data/reports/$stamp-${2:-all}.plans.txt
	: >"$report"
	header
	for type in "${types[@]}"; do
		run_type "$type"
	done
	ln -sfn "$(basename "$report")" "$data/reports/latest.md"
	echo "report: $report"
	echo "plans:  $plans"
	;;
*)
	echo "usage: $0 prepare | bench [inet|int|string|all]" >&2
	exit 2
	;;
esac
