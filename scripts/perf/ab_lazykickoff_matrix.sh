#!/usr/bin/env bash
# Full A/B matrix for the #801 lazy-kickoff patch on EXCLUSIVE cores.
#   dimensions:  connections {1,8,32,1024} x query {select1,aggp,aggsort,window,
#                join,rcte,readmany} x protocol {simple,prepared}
#
# Measurement design (after an adversarial review of v1):
#  * Reserves PHYS whole physical cores as a cgroup-v2 root partition; the kernel
#    evicts every co-tenant, so the only things on these cores are serened and
#    the pgbench/psql driving it. (Client + server share the reserved cores on
#    purpose: a starved server's idle DuckDB workers SPIN on their semaphore and
#    that spin lands in perf's cycle count -- keeping the server fed is what
#    makes cyc/q meaningful. perf attaches to the server PID, so the client's own
#    cycles are never counted regardless.)
#  * Per cell: pgbench runs with -P 1; cycles (perf stat -p server) and txns are
#    BOTH counted over the SAME inner steady-state window (skip RAMP seconds of
#    connection setup / ramp, measure WIN seconds strictly inside the run), so
#    cyc/q's numerator and denominator share one interval.
#  * REPS runs per cell; we report the MEDIAN tps / lat / cyc-per-query plus the
#    tps min..max spread (not best-of, which is optimistic and noisy).
#  * BEFORE then AFTER, each its own server + fresh datadir, never co-resident:
#    an idle DuckDB server's worker pool spins and would steal cycles from the
#    one under test, so the two must not overlap. Residual time-drift shows up in
#    the per-cell spread; re-run a cell whose ratio sits inside its spread.
#
# Each query targets a distinct scheduler regime:
#   select1  - transport-bound, single trivial task (lazy-kickoff hot path)
#   aggp     - parallel HASH_GROUP_BY over a real table (multi-task; parity expected)
#   aggsort  - ORDER_BY stacked on HASH_GROUP_BY: two barriers / sequential events
#   window   - global-ORDER window (single-partition sort) + partitioned window:
#              the reshuffle/sync case (repartition to one partition + merge)
#   join     - HASH_JOIN build barrier + parallel probe
#   rcte     - RECURSIVE_CTE: long chain of sequential single-task events
#   readmany - STREAMING_LIMIT, ROWS_READ rows streamed: result-drain path
#
# Usage:
#   sudo BEFORE_BIN=/tmp/serenedb-mironov-<hash>/bin/serened \
#        AFTER_BIN="$PWD/build_perf/bin/serened" \
#        bash scripts/perf/ab_lazykickoff_matrix.sh
#
# Knobs (env): BENCH_PHYS_CORES=8  DUR=14  RAMP=3  REPS=3  ROWS=10000000
#   ROWS_READ=10000  CONNS="1 8 32 1024"  PROTOCOLS="simple prepared"
#   QUERIES="select1 aggp aggsort window join rcte readmany"   (subset to trim)
#   EXPLAIN=1  -> dump each query's physical plan once, then run.
# Full matrix is large; trim CONNS/QUERIES for quick passes.
set -euo pipefail

BEFORE_BIN="${BEFORE_BIN:?set BEFORE_BIN to the unpatched serened}"
AFTER_BIN="${AFTER_BIN:?set AFTER_BIN to the patched serened}"
PHYS="${BENCH_PHYS_CORES:-8}"
DUR="${DUR:-14}"
RAMP="${RAMP:-3}"
REPS="${REPS:-3}"
CONN_MULT="${CONN_MULT:-3}" # multiply DUR for c>=512 so 1024-conn setup is amortized
ROWS="${ROWS:-10000000}"
ROWS_READ="${ROWS_READ:-10000}"
CONNS="${CONNS:-1 8 32 1024}"
PROTOCOLS="${PROTOCOLS:-simple prepared}"
QUERIES="${QUERIES:-select1 aggp aggsort window join rcte readmany}"
EXPLAIN="${EXPLAIN:-0}"
CG=/sys/fs/cgroup/lazykickoff_mx
RUN_USER="${SUDO_USER:-$(id -un)}"
RES_BEFORE=/tmp/lkm_before.tsv
RES_AFTER=/tmp/lkm_after.tsv

# ---------------------------------------------------------------- inner mode --
if [[ "${1:-}" == "--inner" ]]; then
	CPU="${BENCH_CPU:?}"
	IO="${IO_THREADS:-8}"
	for t in pgbench perf psql; do command -v "$t" >/dev/null || {
		echo "missing $t" >&2
		exit 1
	}; done

	declare -A QSQL
	QSQL[select1]="SELECT 1;"
	QSQL[aggp]="SELECT k, count(*) c, sum(v) s FROM fact GROUP BY k;"
	QSQL[aggsort]="SELECT k, count(*) c FROM fact GROUP BY k ORDER BY c DESC, k;"
	QSQL[window]="SELECT count(*) FROM (SELECT row_number() OVER (ORDER BY id) rn, sum(v) OVER (PARTITION BY k ORDER BY id) sw FROM fact) t;"
	QSQL[join]="SELECT count(*) FROM fact f JOIN dim d USING(k);"
	QSQL[rcte]="WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n<50000) SELECT count(*) FROM c;"
	QSQL[readmany]="SELECT id, s FROM fact LIMIT ${ROWS_READ};"

	jobs_for() {
		local c="$1"
		if ((c <= 1)); then echo 1; elif ((c <= 8)); then echo "$c"; elif ((c <= 64)); then echo 16; else echo 32; fi
	}
	ramp_for() {
		local c="$1"
		if ((c >= 512)); then echo $((RAMP + 2)); else echo "$RAMP"; fi
	}

	wait_up() {
		local port="$1" i
		for i in $(seq 1 80); do
			psql -h 127.0.0.1 -p "$port" -U postgres -d postgres -tAc "SELECT 1" >/dev/null 2>&1 && return 0
			sleep 0.5
		done
		echo "server :$port never came up" >&2
		return 1
	}

	build_tables() {
		psql -h 127.0.0.1 -p "$1" -U postgres -d postgres -q \
			-c "CREATE TABLE fact AS SELECT i AS id,(i%997)::int AS k,(i%50000)::int AS g,((i*2654435761)%1000000)::bigint AS v,('row'||i) AS s FROM range(${ROWS}) t(i);" \
			-c "CREATE TABLE dim AS SELECT i AS k,('d'||i) AS name FROM range(997) t(i);" >/dev/null
	}

	median() { sort -n | awk '{a[NR]=$1} END{if(NR==0){print 0;exit} m=(NR%2)?a[(NR+1)/2]:(a[NR/2]+a[int(NR/2)+1])/2; printf "%.0f\n",m}'; }
	medf() { sort -n | awk '{a[NR]=$1} END{if(NR==0){print 0;exit} m=(NR%2)?a[(NR+1)/2]:(a[NR/2]+a[int(NR/2)+1])/2; printf "%.2f\n",m}'; }
	minmaxf() { sort -n | awk '{a[NR]=$1} END{if(NR==0){print "0 0";exit} printf "%.1f %.1f\n",a[1],a[NR]}'; }

	# one rep -> "tps lat cyq". pgbench's own steady tps/txns (robust for us-fast
	# AND multi-second queries and any conn count); perf co-started over the same
	# full window so cyc/q's numerator+denominator share it. Empty on failure.
	one_rep() {
		local port="$1" srv="$2" q="$3" proto="$4" cl="$5"
		local f="/tmp/lkm_${q}.sql"
		printf '%s\n' "${QSQL[$q]}" >"$f"
		local j dur
		j="$(jobs_for "$cl")"
		dur="$DUR"
		((cl >= 512)) && dur=$((DUR * CONN_MULT))
		local pgl="/tmp/lkm_pgb_${port}.log" stat="/tmp/lkm_stat_${port}.txt"
		pgbench -h 127.0.0.1 -p "$port" -U postgres postgres -n -f "$f" \
			-M "$proto" -c "$cl" -j "$j" -T "$dur" >"$pgl" 2>&1 &
		local pgb=$!
		perf stat -p "$srv" -e cycles -o "$stat" -- sleep "$dur" >/dev/null 2>&1 || true
		wait "$pgb" 2>/dev/null || true
		local tps txns lat cyc
		tps=$(awk -F'= ' '/tps = .*without/{print $2+0}' "$pgl")
		txns=$(awk '/transactions actually processed/{print $NF+0}' "$pgl")
		lat=$(awk -F'= ' '/latency average/{print $2+0}' "$pgl")
		cyc=$(awk '/cycles/{gsub(/[^0-9]/,"",$1); print $1; exit}' "$stat" 2>/dev/null || true)
		if [[ -z "${tps:-}" || -z "${cyc:-}" || "${txns:-0}" -le 0 || "$cyc" -le 0 ]]; then
			echo ""
			return
		fi
		awk -v tp="$tps" -v l="${lat:-0}" -v c="$cyc" -v t="$txns" 'BEGIN{printf "%.2f %.3f %d\n", tp, l, c/t}'
	}

	# one cell: REPS reps -> median tps/lat/cyq + tps spread (printed); echoes "tps lat cyq"
	cell() {
		local port="$1" srv="$2" q="$3" proto="$4" cl="$5"
		local f="/tmp/lkm_${q}.sql"
		printf '%s\n' "${QSQL[$q]}" >"$f"
		pgbench -h 127.0.0.1 -p "$port" -U postgres postgres -n -f "$f" \
			-M "$proto" -c "$cl" -j "$(jobs_for "$cl")" -T "$(ramp_for "$cl")" >/dev/null 2>&1 || true
		local tps=() lat=() cyq=() out attempts=0
		while ((${#tps[@]} < REPS && attempts < REPS + 2)); do
			attempts=$((attempts + 1))
			out="$(one_rep "$port" "$srv" "$q" "$proto" "$cl")"
			[[ -z "$out" ]] && {
				echo "    WARN ${proto}/${q}/c=${cl}: bad rep (zero txns/cycles), retrying" >&2
				continue
			}
			local a b c
			read -r a b c <<<"$out"
			tps+=("$a")
			lat+=("$b")
			cyq+=("$c")
		done
		if ((${#tps[@]} == 0)); then
			printf '    %-9s %-9s c=%-5s -> FAILED (no valid reps)\n' "$proto" "$q" "$cl" >&2
			echo "0 0 0"
			return
		fi
		local mt ml mc lo hi
		mt=$(printf '%s\n' "${tps[@]}" | medf)
		ml=$(printf '%s\n' "${lat[@]}" | medf)
		mc=$(printf '%s\n' "${cyq[@]}" | median)
		read -r lo hi < <(printf '%s\n' "${tps[@]}" | minmaxf)
		printf '    %-9s %-9s c=%-5s -> tps=%-9s (%s..%s) lat=%-8s cyc/q=%s\n' "$proto" "$q" "$cl" "$mt" "$lo" "$hi" "$ml" "$mc" >&2
		echo "$mt $ml $mc"
	}

	run_matrix() {
		local bin="$1" port="$2" out="$3"
		local data="/tmp/lkm_data_${port}"
		rm -rf "$data"
		"$bin" "$data" --io_threads "$IO" --cpu_threads "$CPU" \
			--listen="postgres://0.0.0.0:${port}" >"/tmp/lkm_srv_${port}.log" 2>&1 &
		local srv=$!
		wait_up "$port"
		echo "  building fact(${ROWS}) + dim(997) ..."
		build_tables "$port"
		if [[ "$EXPLAIN" == 1 ]]; then
			local qx
			for qx in $QUERIES; do
				echo "  -- plan[$qx] --"
				psql -h 127.0.0.1 -p "$port" -U postgres -d postgres -tAc "EXPLAIN ${QSQL[$qx]}" 2>&1 | grep -iE "GROUP_BY|ORDER|WINDOW|JOIN|CTE|LIMIT|UNGROUPED|TOP_N|SCAN" | sed 's/[[:space:]]\+/ /g' | head -8 || true
			done
		fi
		: >"$out"
		local q proto cl res a b c
		for proto in $PROTOCOLS; do
			for q in $QUERIES; do
				for cl in $CONNS; do
					res="$(cell "$port" "$srv" "$q" "$proto" "$cl")"
					read -r a b c <<<"$res"
					printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$proto" "$q" "$cl" "$a" "$b" "$c" >>"$out"
				done
			done
		done
		kill -9 "$srv" 2>/dev/null || true
		wait "$srv" 2>/dev/null || true
		rm -rf "$data"
	}

	echo ">> BEFORE (unpatched) ${BEFORE_BIN}  [cpu_threads ${CPU}]"
	run_matrix "$BEFORE_BIN" "${PORT_BEFORE:-6493}" "$RES_BEFORE"
	echo ">> AFTER  (lazy-kickoff) ${AFTER_BIN}"
	run_matrix "$AFTER_BIN" "${PORT_AFTER:-6494}" "$RES_AFTER"

	echo
	printf '%-8s %-8s %-5s | %-9s %-9s %-7s | %-9s %-9s %-7s | %-7s\n' \
		proto query conns B_tps A_tps tps_x B_lat A_lat lat_x cyq_x
	printf -- '-%.0s' {1..96}
	echo
	join -t$'\t' -1 1 -2 1 <(awk -F'\t' '{print $1"|"$2"|"$3"\t"$4"\t"$5"\t"$6}' "$RES_BEFORE" | sort) \
		<(awk -F'\t' '{print $1"|"$2"|"$3"\t"$4"\t"$5"\t"$6}' "$RES_AFTER" | sort) |
		awk -F'\t' '{ split($1,k,"|"); bt=$2; bl=$3; bc=$4; at=$5; al=$6; ac=$7;
		tx=(bt>0)? sprintf("%.2fx",at/bt):"-";
		lx=(al>0)? sprintf("%.2fx",bl/al):"-";   # lat ratio before/after: >1 = AFTER lower latency
		cx=(ac>0)? sprintf("%.2fx",bc/ac):"-";
		printf "%-8s %-8s %-5s | %-9s %-9s %-7s | %-9s %-9s %-7s | %-7s\n",k[1],k[2],k[3],bt,at,tx,bl,al,lx,cx }'
	echo
	echo "tps_x>1 => AFTER more throughput; lat_x>1 => AFTER lower latency; cyq_x>1 =>"
	echo "AFTER fewer server cycles/query. <1 = regression. For us-fast queries read"
	echo "tps_x/cyq_x; for multi-second queries (aggp/window/join over ${ROWS} rows) read"
	echo "lat_x (throughput is ~queries/sec and tiny). cyc/q is meaningful when the"
	echo "server is the bottleneck (c<=32); c=1024 is a throughput-under-oversubscription"
	echo "probe (client-bound). Re-run a cell whose ratio sits inside its tps min..max spread."
	exit 0
fi

# ---------------------------------------------------------------- outer mode --
[[ $EUID -eq 0 ]] || {
	echo "must run as root: sudo BEFORE_BIN=.. AFTER_BIN=.. bash $0" >&2
	exit 1
}

mapfile -t _coresets < <(cat /sys/devices/system/cpu/cpu[0-9]*/topology/thread_siblings_list | sort -t, -k1,1n -u)
((PHYS <= ${#_coresets[@]})) || {
	echo "asked $PHYS cores, only ${#_coresets[@]} exist" >&2
	exit 1
}
_sel=("${_coresets[@]: -$PHYS}")
CORES="$(
	IFS=,
	echo "${_sel[*]}"
)"
NCPU=0
for p in "${_sel[@]}"; do
	IFS=',' read -ra _parts <<<"$p"
	for q in "${_parts[@]}"; do if [[ "$q" == *-* ]]; then NCPU=$((NCPU + ${q#*-} - ${q%-*} + 1)); else NCPU=$((NCPU + 1)); fi; done
done
echo "reserving $PHYS physical cores -> cpuset ${CORES} (${NCPU} logical); serened cpu_threads=${NCPU}"

cleanup() {
	echo $$ >/sys/fs/cgroup/cgroup.procs 2>/dev/null || true
	# Order matters: demote the partition AND release the exclusive cpu claim
	# before rmdir, else rmdir fails and the dir lingers holding the cores
	# exclusive -- which blocks the next isolated run from reserving them.
	[[ -d "$CG" ]] && {
		echo member >"$CG/cpuset.cpus.partition" 2>/dev/null || true
		echo >"$CG/cpuset.cpus.exclusive" 2>/dev/null || true
		rmdir "$CG" 2>/dev/null || true
	}
	echo "released reserved cores"
}
trap cleanup EXIT

mkdir -p "$CG"
echo "$CORES" >"$CG/cpuset.cpus"
cat /sys/devices/system/node/online >"$CG/cpuset.mems"
echo "$CORES" >"$CG/cpuset.cpus.exclusive" 2>/dev/null || true
echo root >"$CG/cpuset.cpus.partition"
state="$(cat "$CG/cpuset.cpus.partition")"
[[ "$state" == "root" ]] || {
	echo "ERROR: partition is '${state}', not exclusive -- aborting" >&2
	exit 1
}
echo $$ >"$CG/cgroup.procs"
echo "partition active; matrix as ${RUN_USER} (DUR=${DUR}s RAMP=${RAMP}s REPS=${REPS} ROWS=${ROWS})"
echo "  CONNS=[${CONNS}] PROTOCOLS=[${PROTOCOLS}] QUERIES=[${QUERIES}]"

sudo -H -u "$RUN_USER" --preserve-env=PATH \
	BEFORE_BIN="$BEFORE_BIN" AFTER_BIN="$AFTER_BIN" BENCH_CPU="$NCPU" IO_THREADS=8 \
	DUR="$DUR" RAMP="$RAMP" REPS="$REPS" CONN_MULT="$CONN_MULT" ROWS="$ROWS" ROWS_READ="$ROWS_READ" \
	CONNS="$CONNS" PROTOCOLS="$PROTOCOLS" QUERIES="$QUERIES" EXPLAIN="$EXPLAIN" \
	PORT_BEFORE="${PORT_BEFORE:-6493}" PORT_AFTER="${PORT_AFTER:-6494}" \
	bash "$(readlink -f "$0")" --inner
# trap cleanup releases the partition
