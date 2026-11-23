#!/usr/bin/env bash
# One VACUUM (COMPACT_TABLE) over a table of many segments whose inverted index
# has norm fields: time, peak anonymous RSS above idle, and the merged segment's
# files.
#
# usage: bench_merge.sh <label>=<serened> [<label>=<serened> ...]
#
# Each binary builds its input once (a snapshot, kept in BENCH_DIR and reused
# by later invocations; remove BENCH_DIR after changing a binary); every run
# copies the snapshot, syncs, starts a fresh server on it, merges, and stops
# it. Runs alternate between the binaries so slow drift hits all of them.
# merge_ms is wall time and includes the fsync of the merged segment; cpu_ms is
# the server's CPU time over the merge.
#
# env: BENCH_ROWS (60000000), BENCH_SEGMENTS (8), BENCH_RUNS (3),
#      BENCH_PORT (55620), BENCH_DIR (/tmp/$USER-bench-merge),
#      BENCH_QUIET_LOAD (unset: each merge waits until the 1-minute load
#      average is below it)
set -euo pipefail

rows=${BENCH_ROWS:-60000000}
segments=${BENCH_SEGMENTS:-8}
runs=${BENCH_RUNS:-3}
port=${BENCH_PORT:-55620}
dir=${BENCH_DIR:-/tmp/$USER-bench-merge}
quiet=${BENCH_QUIET_LOAD:-}
results="$dir/results.tsv"

wait_quiet() {
	[[ -n $quiet ]] || return 0
	until awk -v quiet="$quiet" '{ exit !($1 < quiet) }' /proc/loadavg; do
		sleep 10
	done
}

psql_q() {
	psql -h 127.0.0.1 -p "$port" -U postgres -d postgres -v ON_ERROR_STOP=1 -qAt "$@"
}

server_pid=
start() {
	[[ -z "$(ss -ltnH "sport = :$port")" ]] || {
		echo "port $port is busy" >&2
		exit 1
	}
	"$1" "$2" --listen="postgres://127.0.0.1:$port" >"$2.log" 2>&1 &
	server_pid=$!
	for _ in $(seq 1 120); do
		psql_q -c 'SELECT 1' >/dev/null 2>&1 && return
		kill -0 "$server_pid" 2>/dev/null || {
			tail -5 "$2.log" >&2
			exit 1
		}
		sleep 0.5
	done
	echo "server did not start" >&2
	exit 1
}

stop() {
	kill "$server_pid"
	wait "$server_pid" 2>/dev/null || true
	server_pid=
}
trap '[[ -n $server_pid ]] && stop' EXIT

cpu_ms() {
	local stat
	stat=$(</proc/"$server_pid"/stat)
	read -r -a fields <<<"${stat##*) }"
	echo $(((fields[11] + fields[12]) * 1000 / $(getconf CLK_TCK)))
}

anon_kb() {
	local key value _
	while read -r key value _; do
		[[ $key == RssAnon: ]] && {
			echo "$value"
			return
		}
	done </proc/"$server_pid"/status
}

build_input() {
	local bin=$1 data=$2
	[[ -d $data ]] && return
	mkdir -p "$data"
	start "$bin" "$data"
	psql_q <<'SQL'
CREATE TEXT SEARCH DICTIONARY bm_en AS split_text(case := 'lower') WITH (frequency, norm);
CREATE TABLE m (id BIGINT, a TEXT, b TEXT, c TEXT)
  WITH (compaction_interval = 0, optimize_top_k = 'bm25(1.2, 0.75)');
CREATE INDEX m_idx ON m USING inverted (a bm_en, b bm_en, c bm_en);
SQL
	local step=$((rows / segments))
	for ((k = 0; k < segments; ++k)); do
		psql_q -c "INSERT INTO m
                 SELECT i, repeat('a' || (i % 1009) || ' ', 1 + i % 31),
                        'b' || (i % 50021) || repeat(' q', i % 17),
                        repeat('c' || (i % 7) || ' ', 1 + i * 7 % 23)
                 FROM range($((k * step)), $(((k + 1) * step))) t(i)" \
			-c "VACUUM (FLUSH_TABLE) m"
	done
	stop
}

metric() {
	psql_q -c "SELECT value FROM sdb_metrics
             WHERE metric = '$1'
               AND relation_id = (SELECT oid FROM pg_class WHERE relname = 'm')"
}

kind_bytes() {
	find "$1" -name "*.$2" -printf '%s\n' | awk '{ s += $1 } END { print s + 0 }'
}

measure() {
	local label=$1 bin=$2 run=$3 data="$dir/run"
	rm -rf "${data:?}"
	cp -a "$dir/input-$label" "$data"
	sync
	start "$bin" "$data"
	psql_q -c 'SELECT count(*) FROM m' >/dev/null
	wait_quiet
	local before_segments idle peak sample cpu_before load
	before_segments=$(metric num_segments)
	load=$(cut -d' ' -f1 /proc/loadavg)
	idle=$(anon_kb)
	peak=$idle
	cpu_before=$(cpu_ms)
	psql_q -c '\timing on' -c 'VACUUM (COMPACT_TABLE) m' >"$dir/vacuum.out" &
	local vacuum=$!
	while kill -0 "$vacuum" 2>/dev/null; do
		sample=$(anon_kb)
		((sample > peak)) && peak=$sample
		sleep 0.005
	done
	wait "$vacuum"
	local cpu=$(($(cpu_ms) - cpu_before))
	local ms after_segments files store
	ms=$(grep -oP 'Time: \K[0-9.]+' "$dir/vacuum.out" | tail -1)
	after_segments=$(metric num_segments)
	files=$(metric num_files)
	store=$(find "$data" -type d -name store | head -1)
	for _ in $(seq 1 100); do
		(($(find "$store" -type f | wc -l) == files)) && break
		sleep 0.1
	done
	local topk
	topk=$(psql_q -c "SELECT round(sum(s), 2) FROM (
                      SELECT BM25(m.tableoid) AS s FROM m WHERE a @@ 'a7'
                      ORDER BY s DESC LIMIT 1000) t")
	printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%d\t%d\t%s\t%s\t%s\t%s\t%s\n' \
		"$label" "$run" "$load" "$before_segments" "$after_segments" "$ms" "$cpu" \
		$((idle / 1024)) $((peak / 1024)) $(((peak - idle) / 1024)) \
		"$(metric index_size)" "$(kind_bytes "$store" idx)" \
		"$(kind_bytes "$store" col)" "$(kind_bytes "$store" doc)" "$topk" |
		tee -a "$results"
	stop
	rm -rf "${data:?}"
}

mkdir -p "$dir"
labels=()
declare -A bins
for arg in "$@"; do
	labels+=("${arg%%=*}")
	bins[${arg%%=*}]=${arg#*=}
done
((${#labels[@]} > 0)) || {
	echo "usage: $0 <label>=<serened> ..." >&2
	exit 2
}

for label in "${labels[@]}"; do
	build_input "${bins[$label]}" "$dir/input-$label"
done

printf 'label\trun\tload\tsegments\tafter\tmerge_ms\tcpu_ms\tidle_anon_mb\tpeak_anon_mb\tmerge_anon_mb\tindex_size\tidx\tcol\tdoc\ttopk_sum\n' |
	tee "$results"
for ((run = 1; run <= runs; ++run)); do
	for label in "${labels[@]}"; do
		measure "$label" "${bins[$label]}" "$run"
	done
done

awk -F'\t' 'NR > 1 { ms[$1] = ms[$1] " " $6; cpu[$1] = cpu[$1] " " $7; mb[$1] = mb[$1] " " $10 }
  END { for (l in ms) printf "%s merge_ms:%s  cpu_ms:%s  merge_anon_mb:%s\n", l, ms[l], cpu[l], mb[l] }' "$results"
