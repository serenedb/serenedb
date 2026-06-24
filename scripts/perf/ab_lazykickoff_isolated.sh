#!/usr/bin/env bash
# A/B the #801 lazy-kickoff patch on EXCLUSIVE cores, so co-tenant load on a
# shared box cannot pollute the numbers. Reserves a cgroup-v2 cpuset "root
# partition" (the kernel migrates every other task off these cores), runs BEFORE
# (unpatched snapshot) and AFTER (patched build) of serened across the headline
# workloads AND the low-concurrency regression candidates, prints per-workload
# ratios, then relinquishes the cores. Bare processes only (no docker), since a
# container cannot schedule inside the partition.
#
# Usage:
#   sudo BEFORE_BIN=/tmp/serenedb-mironov-<hash>/bin/serened \
#        AFTER_BIN="$PWD/build_perf/bin/serened" \
#        bash scripts/perf/ab_lazykickoff_isolated.sh
#
# Knobs (env): BENCH_PHYS_CORES=8  DUR=12  REPS=3  PORT_BEFORE=6491 PORT_AFTER=6492
set -euo pipefail

BEFORE_BIN="${BEFORE_BIN:?set BEFORE_BIN to the unpatched serened}"
AFTER_BIN="${AFTER_BIN:?set AFTER_BIN to the patched serened}"
PHYS="${BENCH_PHYS_CORES:-8}"
DUR="${DUR:-12}"
REPS="${REPS:-3}"
PORT_BEFORE="${PORT_BEFORE:-6491}"
PORT_AFTER="${PORT_AFTER:-6492}"
CG=/sys/fs/cgroup/lazykickoff_ab
RUN_USER="${SUDO_USER:-$(id -un)}"

# ---- inner mode: the actual A/B, already unprivileged and inside the cgroup ----
if [[ "${1:-}" == "--inner" ]]; then
	CPU="${BENCH_CPU:?}"
	IO=8
	for t in pgbench perf psql; do command -v "$t" >/dev/null || {
		echo "missing $t" >&2
		exit 1
	}; done

	# one measured run: <bin> <port> <name> <sql> <mode> <clients> -> echoes tps/lat/cyc-per-query
	measure() {
		local bin="$1" port="$2" name="$3" sql="$4" mode="$5" cl="$6"
		local data="/tmp/lk_${name}_${port}"
		rm -rf "$data"
		"$bin" "$data" --io_threads "$IO" --cpu_threads "$CPU" \
			--listen="postgres://0.0.0.0:${port}" >"/tmp/lk_srv_${port}.log" 2>&1 &
		local srv=$!
		local i
		for i in $(seq 1 60); do
			PGPASSWORD=x psql -h 127.0.0.1 -p "$port" -U postgres -d postgres -tAc "SELECT 1" >/dev/null 2>&1 && break
			sleep 0.5
		done
		local f="/tmp/lk_${name}.sql"
		printf '%s\n' "$sql" >"$f"
		# warm
		PGPASSWORD=x pgbench -h 127.0.0.1 -p "$port" -U postgres postgres -n -f "$f" -M "$mode" \
			-c "$cl" -j "$cl" -T 2 >/dev/null 2>&1 || true
		local stat="/tmp/lk_stat_${port}_${name}.txt" pgl="/tmp/lk_pgb_${port}.log"
		PGPASSWORD=x pgbench -h 127.0.0.1 -p "$port" -U postgres postgres -n -f "$f" -M "$mode" \
			-c "$cl" -j "$cl" -T "$DUR" >"$pgl" 2>&1 &
		local pgb=$!
		perf stat -p "$srv" -e cycles -o "$stat" -- sleep "$((DUR - 1))" >/dev/null 2>&1 || true
		wait "$pgb" 2>/dev/null || true
		local tps lat txns cyc cpq=0
		tps=$(awk -F'= ' '/tps = .*without/{print int($2)}' "$pgl")
		lat=$(awk -F'= ' '/latency average/{print $2+0}' "$pgl")
		txns=$(awk '/transactions actually processed/{print $NF+0}' "$pgl")
		cyc=$(awk '/cycles/{gsub(/[^0-9]/,"",$1); print $1; exit}' "$stat" 2>/dev/null || true)
		[[ -n "${cyc:-}" && "${txns:-0}" -gt 0 ]] && cpq=$((cyc / txns))
		kill -9 "$srv" 2>/dev/null || true
		wait "$srv" 2>/dev/null || true
		rm -rf "$data"
		echo "${tps:-0} ${lat:-0} ${cpq}"
	}

	# best-of-REPS by tps (least co-tenant interference); cyc/q from the same run
	best() {
		local bin="$1" port="$2" name="$3" sql="$4" mode="$5" cl="$6"
		local r btps=0 blat=0 bcpq=0 out
		local t rest l c
		for r in $(seq 1 "$REPS"); do
			out=$(measure "$bin" "$port" "$name" "$sql" "$mode" "$cl")
			t=${out%% *}
			rest=${out#* }
			l=${rest%% *}
			c=${rest##* }
			if [[ "${t:-0}" -gt "$btps" ]]; then
				btps=$t
				blat=$l
				bcpq=$c
			fi
		done
		echo "$btps $blat $bcpq"
	}

	declare -a NAMES SQLS MODES CLS
	add() {
		NAMES+=("$1")
		SQLS+=("$2")
		MODES+=("$3")
		CLS+=("$4")
	}
	# headline (trivial single-task) + parse-cost + small rows + parallel
	add select1_c16 "SELECT 1;" prepared 16
	add wide_c16 "SELECT 1 WHERE 1=1 AND 2=2 AND 3=3 AND 4=4 AND 5=5 AND 6=6 AND 7=7 AND 8=8 AND 9=9 AND 10=10;" prepared 16
	add rows1k_c16 "SELECT i,i::text s,i::float8 f FROM generate_series(1,1000) g(i);" prepared 16
	add analytical_c16 "SELECT k,count(*) c,sum(i) s FROM (SELECT i,i%997 AS k FROM generate_series(1,3000000) g(i)) t GROUP BY k ORDER BY k;" prepared 16
	add select1_c64 "SELECT 1;" prepared 64
	# REGRESSION CANDIDATES: multi-chunk single-task with spare cores (low concurrency).
	# Stock offloads the pipeline to a worker (PROCESS_ALL) overlapping execute|send
	# across cores; lazy-kickoff runs it inline on the driver -> suspected slower here.
	add rows10k_c1 "SELECT i,i::text s,i::float8 f FROM generate_series(1,10000) g(i);" prepared 1
	add rows1m_c1 "SELECT i,i::text s,i::float8 f FROM generate_series(1,1000000) g(i);" prepared 1
	add rows10k_c4 "SELECT i,i::text s,i::float8 f FROM generate_series(1,10000) g(i);" prepared 4
	# folding-proof real scan (filtered count: optimizer can't fold; single-thread source)
	add fcount20m_c1 "SELECT count(*) FROM generate_series(1,20000000) g(i) WHERE (i*1103515245+12345)%7=0;" prepared 1
	add fcount20m_c8 "SELECT count(*) FROM generate_series(1,20000000) g(i) WHERE (i*1103515245+12345)%7=0;" prepared 8

	printf '\n%-16s | %16s | %16s | %s\n' "workload" "BEFORE(tps/lat/cyq)" "AFTER(tps/lat/cyq)" "tps_ratio  cyq_ratio"
	printf -- '-%.0s' {1..86}
	echo
	for k in "${!NAMES[@]}"; do
		read -r bt bl bc < <(best "$BEFORE_BIN" "$PORT_BEFORE" "${NAMES[$k]}" "${SQLS[$k]}" "${MODES[$k]}" "${CLS[$k]}")
		read -r at al ac < <(best "$AFTER_BIN" "$PORT_AFTER" "${NAMES[$k]}" "${SQLS[$k]}" "${MODES[$k]}" "${CLS[$k]}")
		tr="-"
		cr="-"
		[[ "$bt" -gt 0 ]] && tr=$(awk -v a="$at" -v b="$bt" 'BEGIN{printf "%.2fx", a/b}')
		[[ "$ac" -gt 0 ]] && cr=$(awk -v a="$ac" -v b="$bc" 'BEGIN{printf "%.2fx", b/a}')
		printf '%-16s | %6s/%5s/%-6s | %6s/%5s/%-6s | tps %-7s cyc/q %s\n' \
			"${NAMES[$k]}" "$bt" "$bl" "$bc" "$at" "$al" "$ac" "$tr" "$cr"
	done
	echo
	echo "tps_ratio>1 and cyq_ratio>1 => AFTER (lazy-kickoff) faster; <1 => regression."
	exit 0
fi

# ---- outer mode: reserve exclusive cores as root, then re-exec --inner as user ----
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
	IFS=','
	for q in $p; do if [[ "$q" == *-* ]]; then NCPU=$((NCPU + ${q#*-} - ${q%-*} + 1)); else NCPU=$((NCPU + 1)); fi; done
done
echo "reserving $PHYS physical cores -> cpuset ${CORES} (${NCPU} logical); serened cpu_threads=${NCPU}"

cleanup() {
	echo $$ >/sys/fs/cgroup/cgroup.procs 2>/dev/null || true
	# Release the exclusive cpu claim before rmdir, else rmdir fails and the dir
	# lingers holding the cores exclusive -- blocking the next isolated run.
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
echo "partition active; running A/B as ${RUN_USER} on the reserved cores (DUR=${DUR}s, REPS=${REPS})"

sudo -H -u "$RUN_USER" --preserve-env=PATH \
	BEFORE_BIN="$BEFORE_BIN" AFTER_BIN="$AFTER_BIN" BENCH_CPU="$NCPU" \
	DUR="$DUR" REPS="$REPS" PORT_BEFORE="$PORT_BEFORE" PORT_AFTER="$PORT_AFTER" \
	bash "$(readlink -f "$0")" --inner
# trap cleanup releases the partition
