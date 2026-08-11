#!/usr/bin/env bash
# bench_v7_1_paths.sh -- end-to-end old-vs-new for the two v7_1 paths that
# reading the code could not settle.
#
#   delim     The multi-delimited tokenizer. It runs in the WRITE path, so what
#             is timed is INSERT + CREATE INDEX + REFRESH over text split on a
#             delimiter set. Swept over set size, because the tokenizer's DFA
#             table is dense (`states x 256 x 4` bytes) and the question is
#             where it stops fitting in cache.
#
#   postings  The `de_for_bitset` / `de_delta_all_equal_to_1` fast paths. They
#             only fire when a posting block is dense, so the corpus plants a
#             term in EVERY row (delta-1 blocks) and a second at 1/16 (mixed
#             blocks). What is timed is the read side: a count over the dense
#             term (fill path), a scored top-k over it (fill + score), and a
#             conjunction of dense AND rare (seek path into dense blocks).
#
# Engines, following full_matrix.sh:
#   old = binary built from $V71_OLD_REF into a temporary git worktree, cached
#         at results/serened_v71_old
#   new = working-tree build, results/serened_v71_new (copied from build_perf)
#
# Each phase is run cold (engine restart + `vmtouch -e` on the datadir, no sudo)
# and hot (mean of $V71_HOT further runs). Cold is the honest number for a fill
# path; hot is the honest number for a decode path. Both are reported because
# the two changes land on different sides of that.
#
# Usage:
#   ninja -C build_perf bin/serened
#   V71_OLD_REF=85e2d4b13 scripts/perf/bench_v7_1_paths.sh
#
# Env:
#   V71_OLD_REF   git ref for the baseline (required unless the binary is cached)
#   V71_BENCHES   subset of "delim postings"        (default: both)
#   V71_ROWS      rows per corpus                    (default: 2000000)
#   V71_DELIMS    delimiter-set sizes for `delim`    (default: "1 4 32 256")
#   V71_HOT       hot repetitions                    (default: 3)
#   V71_PORT      base port                          (default: 6311)
#   V71_REBUILD   1 = rebuild datadirs even if present

set -uo pipefail

ROOT="$(cd "$(dirname "$0")"/../.. && pwd)"
RES="${ROOT}/scripts/perf/results"
WORK="${V71_DIR:-${RES}/v71}"
mkdir -p "$WORK"

OLD_BIN="${V71_OLD_BIN:-${RES}/serened_v71_old}"
NEW_BIN="${V71_NEW_BIN:-${RES}/serened_v71_new}"
PORT="${V71_PORT:-6311}"
# Big enough that decode work dominates; at 2M every read was single-digit ms.
ROWS="${V71_ROWS:-20000000}"
# `delim` builds one datadir per set size per engine, so it uses a smaller
# corpus -- its signal is index-build time, which does not need 20M rows.
DELIM_ROWS="${V71_DELIM_ROWS:-5000000}"
DELIM_SET=(${V71_DELIMS:-1 4 32 256})
HOT="${V71_HOT:-3}"
BENCHES="${V71_BENCHES:-delim postings}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ 2>/dev/null || echo run)"
OUT="${WORK}/v71-${STAMP}.tsv"

command -v vmtouch >/dev/null || {
	echo "vmtouch is required for cold runs: apt install vmtouch" >&2
	exit 1
}

# ---------------------------------------------------------------- binaries

if [[ ! -x "$OLD_BIN" && -n "${V71_OLD_REF:-}" ]]; then
	OLD_WT="${WORK}/old_worktree"
	echo "building baseline from ${V71_OLD_REF} into ${OLD_WT}"
	git -C "$ROOT" worktree add -f "$OLD_WT" "$V71_OLD_REF" &&
		cmake --preset "${V71_PRESET:-perf}" -S "$OLD_WT" -B "$OLD_WT/build_perf" &&
		ninja -C "$OLD_WT/build_perf" bin/serened &&
		cp "$OLD_WT/build_perf/bin/serened" "$OLD_BIN"
fi
[[ -x "$OLD_BIN" ]] || {
	echo "missing baseline binary $OLD_BIN (set V71_OLD_REF)" >&2
	exit 1
}
if [[ ! -x "$NEW_BIN" || "$ROOT/build_perf/bin/serened" -nt "$NEW_BIN" ]]; then
	cp "$ROOT/build_perf/bin/serened" "$NEW_BIN" || {
		echo "build build_perf/bin/serened first" >&2
		exit 1
	}
fi

printf 'bench\tengine\tvariant\tphase\tsec\n' >"$OUT"
echo "results -> $OUT"

# ---------------------------------------------------------------- serened

SRV_PID=""
SRV_DATA=""

start_engine() { # bin datadir
	local bin="$1" data="$2"
	"$bin" "$data" --listen="postgres://0.0.0.0:${PORT}" \
		>"${WORK}/serened.log" 2>&1 &
	SRV_PID=$!
	SRV_DATA="$data"
	local tries=0
	until psql -h 127.0.0.1 -p "$PORT" -U postgres -d postgres -c 'SELECT 1' \
		>/dev/null 2>&1; do
		tries=$((tries + 1))
		[[ $tries -lt 600 ]] || {
			echo "engine did not come up; see ${WORK}/serened.log" >&2
			exit 1
		}
		sleep 0.2
	done
}

stop_engine() {
	[[ -n "$SRV_PID" ]] || return 0
	kill -9 "$SRV_PID" 2>/dev/null
	wait "$SRV_PID" 2>/dev/null
	SRV_PID=""
}

trap 'stop_engine' EXIT INT TERM

sql() { psql -h 127.0.0.1 -p "$PORT" -U postgres -d postgres -v ON_ERROR_STOP=1 -q -c "$1"; }

# Wait until the engine stops burning CPU, i.e. background compaction has run to
# completion. Without this the two engines end up with different segment counts
# -- a query over 5 segments does 5 dictionary lookups, over 4 it does 4 -- and
# every number below measures compaction layout instead of code. Same approach as
# full_matrix.sh.
quiesce() {
	local hz u1 s1 u2 s2 pct idle=0
	[[ -n "$SRV_PID" ]] || return 0
	hz=$(getconf CLK_TCK)
	for _ in $(seq 1 120); do
		read -r u1 s1 < <(awk '{print $14,$15}' "/proc/$SRV_PID/stat" 2>/dev/null)
		[[ -n "${u1:-}" ]] || return 0
		sleep 2
		read -r u2 s2 < <(awk '{print $14,$15}' "/proc/$SRV_PID/stat" 2>/dev/null)
		[[ -n "${u2:-}" ]] || return 0
		pct=$(awk "BEGIN{printf \"%.0f\", (($u2+$s2)-($u1+$s1))/$hz/2*100}")
		if [[ "$pct" -lt 12 ]]; then idle=$((idle + 1)); else idle=0; fi
		[[ "$idle" -ge 3 ]] && return 0
	done
}

# Server-side execution time, in seconds, via psql's own `\timing`. Wall-clock
# around psql includes ~14 ms of connect + parse + plan, which at these corpus
# sizes is larger than the work being measured and hides it entirely.
timed() { # sql
	local ms
	ms=$(psql -h 127.0.0.1 -p "$PORT" -U postgres -d postgres \
		-v ON_ERROR_STOP=1 -q -c '\timing on' -c "$1" 2>/dev/null |
		awk '/^Time:/ {print $2; exit}')
	[[ -n "$ms" ]] || return 1
	awk -v m="$ms" 'BEGIN{printf "%.3f", m/1000}'
}

record() { # bench engine variant phase sec
	printf '%s\t%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$4" "$5" >>"$OUT"
	printf '  %-9s %-4s %-10s %-5s %8ss\n' "$1" "$2" "$3" "$4" "$5"
}

# Restart the engine and drop the page cache for its datadir, so the next read
# is genuinely cold without needing sudo.
go_cold() { # bin datadir
	stop_engine
	vmtouch -e "$2" >/dev/null 2>&1
	start_engine "$1" "$2"
	quiesce
}

# Mean of $HOT runs of one query.
hot_mean() { # sql
	local total=0 s i
	for ((i = 0; i < HOT; ++i)); do
		s=$(timed "$1") || return 1
		total=$(awk -v a="$total" -v b="$s" 'BEGIN{printf "%.6f", a+b}')
	done
	awk -v t="$total" -v n="$HOT" 'BEGIN{printf "%.3f", t/n}'
}

# ---------------------------------------------------------------- delim

# n comma-free 4-byte delimiters plus the one actually planted in the text, as
# the quoted list the `multi_delimiter` template expects.
delim_list() { # n
	local n="$1" out='"|"' i a b
	for ((i = 0; i < n - 1; ++i)); do
		a=$(printf "\\$(printf '%03o' $((65 + i / 26)))")
		b=$(printf "\\$(printf '%03o' $((65 + i % 26)))")
		out="${out},\"Q${a}${b}Z\""
	done
	printf '%s' "$out"
}

run_delim() { # bin engine
	local bin="$1" engine="$2" n data
	for n in "${DELIM_SET[@]}"; do
		data="${WORK}/${engine}_delim_${n}"
		if [[ -n "${V71_REBUILD:-}" || ! -d "$data" ]]; then
			rm -rf "$data"
			start_engine "$bin" "$data"
			sql "CREATE TEXT SEARCH DICTIONARY d${n}(
			       template = 'multi_delimiter',
			       delimiters = '$(delim_list "$n")')"
			sql "CREATE TABLE t(id BIGINT PRIMARY KEY, body VARCHAR)"
			# Eight '|'-separated tokens per row: the tokenizer sees every byte
			# and emits a fixed number of terms whatever the set size is.
			record delim "$engine" "n=${n}" load \
				"$(timed "INSERT INTO t SELECT i,
				     'alpha'||i||'|beta'||i||'|gamma'||i||'|delta'||i||
				     '|epsilon'||i||'|zeta'||i||'|eta'||i||'|theta'||i
				   FROM range(${DELIM_ROWS}) s(i)")"
			# The tokenizer runs here.
			record delim "$engine" "n=${n}" index \
				"$(timed "CREATE INDEX idx ON t USING inverted(id, body d${n})")"
			record delim "$engine" "n=${n}" refresh \
				"$(timed "VACUUM (REFRESH_TABLE) t")"
			# Let compaction finish before freezing the datadir, so both engines
			# read from the same segment layout.
			quiesce
			stop_engine
		fi
		go_cold "$bin" "$data"
		record delim "$engine" "n=${n}" cold \
			"$(timed "SELECT count(*) FROM idx WHERE body @@ 'alpha7'")"
		record delim "$engine" "n=${n}" hot \
			"$(hot_mean "SELECT count(*) FROM idx WHERE body @@ 'alpha7'")"
		stop_engine
	done
}

# ---------------------------------------------------------------- postings

run_postings() { # bin engine
	local bin="$1" engine="$2"
	local data="${WORK}/${engine}_postings"
	if [[ -n "${V71_REBUILD:-}" || ! -d "$data" ]]; then
		rm -rf "$data"
		start_engine "$bin" "$data"
		sql "CREATE TEXT SEARCH DICTIONARY ws(
		       template = 'delimiter', delimiter = ' ',
		       frequency = true, position = true)"
		sql "CREATE TABLE p(id BIGINT PRIMARY KEY, body VARCHAR)"
		# Two terms of each shape, because a single-term `count(*)` is answered
		# from the term's `docs_count` and decodes nothing at all -- only an
		# intersection forces both posting lists to be decoded end to end.
		#
		# ubiq/ubiq2 are in every row      -> delta-1 blocks, the
		#                                     `de_delta_all_equal_to_1` /
		#                                     `e_all_equal_to_1` shape
		# dense/dense2 are in 15 of 16     -> dense but not runs, the
		#                                     `de_for_bitset` shape, and the two
		#                                     drop different rows so the bitsets
		#                                     differ rather than being equal
		# rare is 1 in 4096                -> drives the seek path
		record postings "$engine" corpus load \
			"$(timed "INSERT INTO p SELECT i,
			     'ubiq ubiq2 w'||i||
			     CASE WHEN i%16<>0 THEN ' dense' ELSE '' END||
			     CASE WHEN i%16<>7 THEN ' dense2' ELSE '' END||
			     CASE WHEN i%4096=0 THEN ' rare' ELSE '' END
			   FROM range(${ROWS}) s(i)")"
		record postings "$engine" corpus index \
			"$(timed "CREATE INDEX pidx ON p USING inverted(id, body ws)
			            WITH (optimize_top_k = 'bm25(1.2, 0.75)')")"
		record postings "$engine" corpus refresh \
			"$(timed "VACUUM (REFRESH_TABLE) p")"
		quiesce
		stop_engine
	fi

	# full decode of two delta-1 lists, intersected
	local q_run="SELECT count(*) FROM pidx WHERE body @@ ts_all(['ubiq','ubiq2'])"
	# full decode of two dense-but-not-run lists, intersected
	local q_bitset="SELECT count(*) FROM pidx WHERE body @@ ts_all(['dense','dense2'])"
	# one delta-1 list against one dense list, so fill runs on both shapes at once
	local q_mixed="SELECT count(*) FROM pidx WHERE body @@ ts_all(['ubiq','dense'])"
	# fill + score over delta-1 blocks
	local q_score="SELECT id FROM pidx WHERE body @@ 'ubiq'
	               ORDER BY BM25(pidx.tableoid) DESC LIMIT 10"
	# seek into delta-1 blocks, driven by the rare side
	local q_seek="SELECT count(*) FROM pidx WHERE body @@ ts_all(['rare','ubiq'])"
	# seek into dense blocks
	local q_seek_bitset="SELECT count(*) FROM pidx WHERE body @@ ts_all(['rare','dense'])"
	# materializes every matching doc id and fetches the row, so this is the one
	# case where the decoded ids are actually consumed downstream
	local q_fetch="SELECT sum(id) FROM pidx WHERE body @@ 'dense'"

	local name q
	for pair in "run:$q_run" "bitset:$q_bitset" "mixed:$q_mixed" \
		"score:$q_score" "seek-run:$q_seek" "seek-bitset:$q_seek_bitset" \
		"fetch:$q_fetch"; do
		name="${pair%%:*}"
		q="${pair#*:}"
		go_cold "$bin" "$data"
		record postings "$engine" "$name" cold "$(timed "$q")"
		record postings "$engine" "$name" hot "$(hot_mean "$q")"
		stop_engine
	done
}

# ---------------------------------------------------------------- drive

for bench in $BENCHES; do
	for engine in old new; do
		bin="$OLD_BIN"
		[[ "$engine" == new ]] && bin="$NEW_BIN"
		echo "== ${bench} / ${engine}"
		case "$bench" in
		delim) run_delim "$bin" "$engine" ;;
		postings) run_postings "$bin" "$engine" ;;
		*) echo "unknown bench $bench" >&2 ;;
		esac
	done
done

stop_engine

echo
echo "== old vs new (hot, seconds; ratio <1 means new is faster)"
awk -F'\t' 'NR>1 {
	k=$1"\t"$3"\t"$4; if ($2=="old") o[k]=$5; else n[k]=$5
}
END {
	printf "%-9s %-12s %-5s %9s %9s %7s\n","bench","variant","phase","old","new","ratio"
	for (k in o) if (k in n) {
		split(k,f,"\t")
		r = (o[k]>0) ? n[k]/o[k] : 0
		printf "%-9s %-12s %-5s %9s %9s %7.3f\n",f[1],f[2],f[3],o[k],n[k],r
	}
}' "$OUT" | sort
