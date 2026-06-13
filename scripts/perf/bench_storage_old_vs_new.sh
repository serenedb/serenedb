#!/usr/bin/env bash
# bench_storage_old_vs_new.sh -- storage-engine benchmark: the OLD rocksdb
# table plane (the pinned main baseline binary) vs the NEW native duckdb
# store tables (the current branch's perf binary), over the same pg wire.
#
# Both servers speak the same protocol and run the same workload set, so the
# delta is the storage layer: row format, WAL/fsync policy, index machinery,
# search feeding. Workload matrix follows docs/remove_rocksdb_plan.md #10.
#
# Each server is its own process with its own datadir, and is started, fully
# benchmarked, and KILLED before the other is ever launched. pgbench drives
# sustained QPS; perf stat -p <pid> captures cycles/instructions for the same
# window (cycles-per-query is thread-count independent). Single-statement
# workloads (bulk load, index build, recovery) are wall-clocked over
# PERF_STORAGE_REPS repetitions and report the median as 1/seconds "tps".
#
# The OLD side only changes when the baseline binary changes: measured once,
# cached in results/baselines-storage/old.tsv, reused across branch
# iterations (PERF_REMEASURE_OLD=1 refreshes).
#
# Isolation: run under scripts/perf/run_bench_isolated.sh-style core
# reservation when you have sudo; without sudo there is no cpuset partition
# (user slices on this box have no cpuset delegation), so this script falls
# back to plain taskset affinity: server pinned to PERF_SERVER_CORES, pgbench
# to PERF_CLIENT_CORES (disjoint sets, whole physical cores incl. SMT
# siblings), plus a loadavg gate (PERF_QUIET_LOAD) before every measured
# window. Affinity does not keep co-tenants off the cores -- prefer the sudo
# wrapper for publishable numbers.
#
# Pre-reqs: pgbench, perf, the baseline binary, the branch perf binary.
# No other serened on the ports.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")"/../.. && pwd)"
BUILD_DIR="${PERF_BUILD_DIR:-${ROOT}/build_perf}"
NEW_BIN="${PERF_STORAGE_NEW_BIN:-${BUILD_DIR}/bin/serened}"
OLD_BIN="${PERF_STORAGE_OLD_BIN:-${HOME}/baselines/serened-main-aa1595ddb}"
RESULTS_DIR="${ROOT}/scripts/perf/results"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="${RESULTS_DIR}/storage-old-vs-new-${STAMP}"
BASELINE_DIR="${PERF_BASELINE_DIR:-${RESULTS_DIR}/baselines-storage}"
OLD_CACHE="${BASELINE_DIR}/old.tsv"
REMEASURE_OLD="${PERF_REMEASURE_OLD:-0}"

PORT="${PERF_STORAGE_PORT:-6490}"
CLIENTS="${PERF_STORAGE_CLIENTS:-8}"
JOBS="${PERF_STORAGE_JOBS:-4}"
DURATION="${PERF_STORAGE_DURATION:-15}"
REPS="${PERF_STORAGE_REPS:-3}"
IO_THREADS="${PERF_STORAGE_IO_THREADS:-4}"
QUIET_LOAD="${PERF_QUIET_LOAD:-8}"
QUIET_WAIT_SECS="${PERF_QUIET_WAIT_SECS:-600}"

# --- core selection ---------------------------------------------------------
# Inside run_bench_isolated.sh, PERF_BENCH_CORES is the reserved set: split it
# server/client. Without it, derive whole physical cores (both SMT siblings)
# from topology: server gets the top PERF_SERVER_PHYS physical cores, clients
# the PERF_CLIENT_PHYS below them, leaving cpu0's housekeeping alone.
SERVER_PHYS="${PERF_SERVER_PHYS:-6}"
CLIENT_PHYS="${PERF_CLIENT_PHYS:-3}"
phys_cores() { # emit thread_siblings_list lines sorted by first cpu
    cat /sys/devices/system/cpu/cpu[0-9]*/topology/thread_siblings_list |
        sort -t, -k1,1n -u
}
if [[ -n "${PERF_SERVER_CORES:-}" && -n "${PERF_CLIENT_CORES:-}" ]]; then
    SERVER_CORES="${PERF_SERVER_CORES}"
    CLIENT_CORES="${PERF_CLIENT_CORES}"
elif [[ -n "${PERF_BENCH_CORES:-}" ]]; then
    # Reserved set from the isolation wrapper: server takes ~2/3, clients 1/3.
    IFS=',' read -r -a _parts <<<"${PERF_BENCH_CORES}"
    _cpus=()
    for _p in "${_parts[@]}"; do
        if [[ "${_p}" == *-* ]]; then
            for ((c = ${_p%-*}; c <= ${_p#*-}; c++)); do _cpus+=("$c"); done
        else _cpus+=("${_p}"); fi
    done
    _ns=$((${#_cpus[@]} * 2 / 3))
    SERVER_CORES="$(
        IFS=,
        echo "${_cpus[*]:0:_ns}"
    )"
    CLIENT_CORES="$(
        IFS=,
        echo "${_cpus[*]:_ns}"
    )"
else
    mapfile -t _coresets < <(phys_cores)
    _total=${#_coresets[@]}
    ((SERVER_PHYS + CLIENT_PHYS <= _total)) || {
        echo "need $((SERVER_PHYS + CLIENT_PHYS)) physical cores, have ${_total}" >&2
        exit 1
    }
    _srv=("${_coresets[@]: -$SERVER_PHYS}")
    _cli=("${_coresets[@]: -$((SERVER_PHYS + CLIENT_PHYS)):$CLIENT_PHYS}")
    SERVER_CORES="$(
        IFS=,
        echo "${_srv[*]}"
    )"
    CLIENT_CORES="$(
        IFS=,
        echo "${_cli[*]}"
    )"
fi
cpuset_count() {
    local n=0 part
    local IFS=','
    for part in $1; do
        if [[ "${part}" == *-* ]]; then
            n=$((n + ${part#*-} - ${part%-*} + 1))
        else
            n=$((n + 1))
        fi
    done
    echo "${n}"
}
CPU_THREADS="${PERF_STORAGE_CPU_THREADS:-$(cpuset_count "${SERVER_CORES}")}"

for bin in "${OLD_BIN}" "${NEW_BIN}"; do
    [[ -x "${bin}" ]] || {
        echo "missing binary: ${bin}" >&2
        exit 1
    }
done
for tool in pgbench perf psql; do
    command -v "${tool}" >/dev/null 2>&1 || {
        echo "${tool} not found" >&2
        exit 1
    }
done

mkdir -p "${OUT_DIR}" "${BASELINE_DIR}"
CUR_PID=""
CUR_DATA=""
cleanup() {
    [[ -n "${CUR_PID}" ]] && kill -9 "${CUR_PID}" 2>/dev/null || true
    [[ -n "${CUR_DATA}" ]] && rm -rf "${CUR_DATA}" || true
}
trap cleanup EXIT

# Wait for a quiet machine before each measured window; proceed with a warning
# (and record the loadavg) if it never quiets down.
wait_quiet() {
    local waited=0
    while :; do
        local load
        load="$(awk '{print int($1)}' /proc/loadavg)"
        ((load <= QUIET_LOAD)) && return 0
        ((waited >= QUIET_WAIT_SECS)) && {
            echo "  WARNING: loadavg ${load} > ${QUIET_LOAD} after ${waited}s -- measuring anyway" >&2
            return 0
        }
        sleep 10
        waited=$((waited + 10))
    done
}

PSQL=(psql -h 127.0.0.1 -p "${PORT}" -U postgres -d postgres -AtX -q)

wait_up() {
    local log="$1"
    for _ in $(seq 1 120); do
        if "${PSQL[@]}" -c 'SELECT 1' >/dev/null 2>&1; then return 0; fi
        sleep 0.5
    done
    echo "server on :${PORT} did not come up:" >&2
    tail -40 "${log}" >&2
    return 1
}

start_serened() {
    local srv="$1" datadir="$2"
    taskset -c "${SERVER_CORES}" "${BENCH_BIN}" "${datadir}" \
        --server_endpoints "pgsql+tcp://0.0.0.0:${PORT}" \
        --server_io_threads "${IO_THREADS}" \
        --server_cpu_threads "${CPU_THREADS}" \
        >"${OUT_DIR}/serened_${srv}.log" 2>&1 &
    CUR_PID=$!
}

stop_serened() {
    kill -9 "${CUR_PID}" 2>/dev/null || true
    wait "${CUR_PID}" 2>/dev/null || true
    CUR_PID=""
}

# --- pgbench workload scripts ------------------------------------------------
mkdir -p "${OUT_DIR}/wl"
cat >"${OUT_DIR}/wl/insert.sql" <<'EOF'
\set id random(1, 999999999999)
INSERT INTO bench_ins (id, v, pad) VALUES (:id, :id % 1000, 'padpadpadpadpad');
EOF
cat >"${OUT_DIR}/wl/update.sql" <<'EOF'
\set id random(1, 100000)
UPDATE bench_upd SET v = v + 1 WHERE id = :id;
EOF
cat >"${OUT_DIR}/wl/lookup.sql" <<'EOF'
\set id random(1, 500000)
SELECT id, grp, val FROM bench_big WHERE id = :id;
EOF
cat >"${OUT_DIR}/wl/range.sql" <<'EOF'
\set lo random(1, 490000)
SELECT sum(val) FROM bench_big WHERE id BETWEEN :lo AND :lo + 10000;
EOF
cat >"${OUT_DIR}/wl/agg.sql" <<'EOF'
SELECT grp, sum(val), avg(val), count(*) FROM bench_big GROUP BY grp ORDER BY grp;
EOF
cat >"${OUT_DIR}/wl/sk_lookup.sql" <<'EOF'
\set g random(0, 99)
SELECT count(*) FROM bench_big WHERE grp = :g;
EOF
cat >"${OUT_DIR}/wl/search.sql" <<'EOF'
\set tok random(0, 49)
SELECT id FROM bench_txt_idx WHERE body @@ ts_phrase('token' || :tok) LIMIT 10;
EOF
cat >"${OUT_DIR}/wl/nextval.sql" <<'EOF'
INSERT INTO bench_seq (v) VALUES (1);
EOF

# run_pgb <label> <sqlfile> <mode> [clients] [extra pgbench args...]
run_pgb() {
    local label="$1" sqlfile="$2" mode="$3" clients="${4:-${CLIENTS}}"
    shift 4 || true
    local extra=("$@")
    local pgb_log="${OUT_DIR}/pgbench_${label}.log"
    local stat_log="${OUT_DIR}/stat_${label}.txt"
    wait_quiet
    # warmup
    taskset -c "${CLIENT_CORES}" pgbench -h 127.0.0.1 -p "${PORT}" -U postgres \
        -n -f "${sqlfile}" -M "${mode}" -c "${clients}" -j "${JOBS}" -T 2 \
        "${extra[@]}" postgres >/dev/null 2>&1 || true
    taskset -c "${CLIENT_CORES}" pgbench -h 127.0.0.1 -p "${PORT}" -U postgres \
        -n -f "${sqlfile}" -M "${mode}" -c "${clients}" -j "${JOBS}" \
        -T "${DURATION}" "${extra[@]}" postgres >"${pgb_log}" 2>&1 &
    local pgb_pid=$!
    perf stat -p "${CUR_PID}" -e task-clock,cycles,instructions \
        -o "${stat_log}" -- sleep "$((DURATION - 2))" >/dev/null 2>&1 || true
    wait "${pgb_pid}" 2>/dev/null || true

    local tps lat txns cycles insns load
    tps=$(awk -F'= ' '/tps = .*without initial/{print $2+0}' "${pgb_log}")
    [[ -z "${tps}" ]] && tps=$(awk -F'= ' '/^tps =/{print $2+0; exit}' "${pgb_log}")
    lat=$(awk -F'= ' '/latency average/{print $2+0}' "${pgb_log}")
    txns=$(awk '/number of transactions actually processed/{print $NF+0}' "${pgb_log}")
    cycles=$(awk '/cycles/{gsub(/[^0-9]/,"",$1); print $1; exit}' "${stat_log}" 2>/dev/null)
    insns=$(awk '/instructions/{gsub(/[^0-9]/,"",$1); print $1; exit}' "${stat_log}" 2>/dev/null)
    load="$(awk '{print $1}' /proc/loadavg)"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "${label}" "${tps:-0}" "${lat:-0}" \
        "${txns:-0}" "${cycles:-0}" "${insns:-0}" "${load}" >>"${OUT_DIR}/results.tsv"
    printf '  %-26s tps=%-12s lat=%-9s load=%s\n' "${label}" "${tps:-0}" "${lat:-0}" "${load}"
}

# run_wall <label> <reps> <setup-sql> <timed-sql> <teardown-sql>
# Wall-clocks ONE statement, median over reps; rows are tps=1/median so the
# summary ratio machinery applies. Setup/teardown run untimed around each rep.
run_wall() {
    local label="$1" reps="$2" setup="$3" timed="$4" teardown="$5"
    local times=()
    local r s e
    for ((r = 0; r < reps; r++)); do
        wait_quiet
        [[ -n "${setup}" ]] && "${PSQL[@]}" -c "${setup}" >/dev/null
        s=$(date +%s.%N)
        "${PSQL[@]}" -c "${timed}" >/dev/null
        e=$(date +%s.%N)
        [[ -n "${teardown}" ]] && "${PSQL[@]}" -c "${teardown}" >/dev/null
        times+=("$(echo "${e} - ${s}" | bc)")
    done
    local med
    med=$(printf '%s\n' "${times[@]}" | sort -n | awk -v n="${reps}" 'NR==int((n+1)/2){print}')
    local load tps
    load="$(awk '{print $1}' /proc/loadavg)"
    tps=$(echo "1 / ${med}" | bc -l)
    printf '%s\t%.4f\t%s\t1\t0\t0\t%s\n' "${label}" "${tps}" "${med}" "${load}" >>"${OUT_DIR}/results.tsv"
    printf '  %-26s median=%-8ss (1/t=%.3f) load=%s\n' "${label}" "${med}" "${tps}" "${load}"
}

# run_recovery <srv> <datadir>: kill -9 the running server, wall-clock
# restart-to-ready (includes search index rebuild on the new side), reps times.
run_recovery() {
    local srv="$1" datadir="$2"
    local times=()
    local r s e
    for ((r = 0; r < REPS; r++)); do
        wait_quiet
        kill -9 "${CUR_PID}" 2>/dev/null || true
        wait "${CUR_PID}" 2>/dev/null || true
        s=$(date +%s.%N)
        start_serened "${srv}_recovery" "${datadir}"
        wait_up "${OUT_DIR}/serened_${srv}_recovery.log"
        e=$(date +%s.%N)
        times+=("$(echo "${e} - ${s}" | bc)")
    done
    local med load tps
    med=$(printf '%s\n' "${times[@]}" | sort -n | awk -v n="${REPS}" 'NR==int((n+1)/2){print}')
    load="$(awk '{print $1}' /proc/loadavg)"
    tps=$(echo "1 / ${med}" | bc -l)
    printf '%s\t%.4f\t%s\t1\t0\t0\t%s\n' "${srv}_recovery" "${tps}" "${med}" "${load}" >>"${OUT_DIR}/results.tsv"
    printf '  %-26s median=%-8ss load=%s\n' "${srv}_recovery" "${med}" "${load}"
}

bench_server() {
    local srv="$1"
    local datadir
    datadir="$(mktemp -d "/tmp/sdb_storage_${srv}.XXXXXX")"
    CUR_DATA="${datadir}"
    echo "-- ${srv} server (:${PORT}) [server cores ${SERVER_CORES}, client cores ${CLIENT_CORES}, cpu_threads=${CPU_THREADS}]"
    start_serened "${srv}" "${datadir}"
    wait_up "${OUT_DIR}/serened_${srv}.log"

    # Seed the shared fixtures.
    "${PSQL[@]}" \
        -c "CREATE TABLE bench_ins (id BIGINT PRIMARY KEY, v BIGINT, pad TEXT);" \
        -c "CREATE TABLE bench_upd (id INTEGER PRIMARY KEY, v BIGINT);" \
        -c "INSERT INTO bench_upd SELECT x, 0 FROM generate_series(1, 100000) t(x);" \
        -c "CREATE TABLE bench_seq (id BIGSERIAL PRIMARY KEY, v INTEGER);" \
        >/dev/null

    # 1. single-row INSERT, autocommit, 1 client (fsync-per-commit probe).
    run_pgb "${srv}_insert_1c" "${OUT_DIR}/wl/insert.sql" prepared 1
    # 2. concurrent small writers, same table.
    run_pgb "${srv}_insert_${CLIENTS}c" "${OUT_DIR}/wl/insert.sql" prepared "${CLIENTS}"
    # 3. point UPDATE churn.
    run_pgb "${srv}_update_${CLIENTS}c" "${OUT_DIR}/wl/update.sql" prepared "${CLIENTS}"
    # 4. nextval-heavy insert.
    run_pgb "${srv}_nextval_${CLIENTS}c" "${OUT_DIR}/wl/nextval.sql" prepared "${CLIENTS}"

    # 5. bulk INSERT .. SELECT 1M rows (wall clock).
    run_wall "${srv}_bulk_insert_1m" "${REPS}" \
        "CREATE TABLE bench_bulk (id INTEGER PRIMARY KEY, grp INTEGER, val DOUBLE PRECISION, pad TEXT);" \
        "INSERT INTO bench_bulk SELECT x, x % 100, x * 0.5, 'padpadpadpadpad' || x FROM generate_series(1, 1000000) t(x);" \
        "DROP TABLE bench_bulk;"

    # Scan fixture: 500k rows + plain index on grp.
    "${PSQL[@]}" \
        -c "CREATE TABLE bench_big (id INTEGER PRIMARY KEY, grp INTEGER, val DOUBLE PRECISION, pad TEXT);" \
        -c "INSERT INTO bench_big SELECT x, x % 100, x * 0.5, 'padpadpadpadpad' || x FROM generate_series(1, 500000) t(x);" \
        >/dev/null

    # 6-8. PK point lookups / PK range sums / full-scan aggregate.
    run_pgb "${srv}_pk_lookup" "${OUT_DIR}/wl/lookup.sql" prepared "${CLIENTS}"
    run_pgb "${srv}_pk_range" "${OUT_DIR}/wl/range.sql" prepared "${CLIENTS}"
    run_pgb "${srv}_fullscan_agg" "${OUT_DIR}/wl/agg.sql" prepared 2

    # 9. secondary index: build (wall) + filtered count through it.
    run_wall "${srv}_sk_build" "${REPS}" "" \
        "CREATE INDEX bench_big_grp ON bench_big(grp);" \
        "DROP INDEX bench_big_grp;"
    "${PSQL[@]}" -c "CREATE INDEX bench_big_grp ON bench_big(grp);" >/dev/null
    run_pgb "${srv}_sk_lookup" "${OUT_DIR}/wl/sk_lookup.sql" prepared "${CLIENTS}"

    # 10. inverted index: build over 200k texts (wall) + scored fetch.
    "${PSQL[@]}" \
        -c "CREATE TABLE bench_txt (id INTEGER PRIMARY KEY, body TEXT);" \
        -c "INSERT INTO bench_txt SELECT x, 'lorem ipsum dolor sit amet word' || (x % 1000) || ' token' || (x % 50) FROM generate_series(1, 200000) t(x);" \
        >/dev/null
    run_wall "${srv}_inverted_build" "${REPS}" "" \
        "CREATE INDEX bench_txt_idx ON bench_txt USING inverted(body);" \
        "DROP INDEX bench_txt_idx;"
    "${PSQL[@]}" -c "CREATE INDEX bench_txt_idx ON bench_txt USING inverted(body);" >/dev/null
    "${PSQL[@]}" -c "VACUUM (REFRESH_TABLE) bench_txt;" >/dev/null
    run_pgb "${srv}_search_fetch" "${OUT_DIR}/wl/search.sql" prepared "${CLIENTS}"

    # 11. recovery: kill -9, time restart-to-ready with the data above.
    run_recovery "${srv}" "${datadir}"

    stop_serened
    rm -rf "${datadir}"
    CUR_DATA=""
}

: >"${OUT_DIR}/results.tsv"
echo "old: ${OLD_BIN}"
echo "new: ${NEW_BIN}"
echo "out: ${OUT_DIR}"
echo "clients=${CLIENTS} jobs=${JOBS} dur=${DURATION}s reps=${REPS} quiet_load<=${QUIET_LOAD}"
echo

if [[ -s "${OLD_CACHE}" && "${REMEASURE_OLD}" != 1 ]]; then
    echo "-- old: reusing cached baseline (${OLD_CACHE}); PERF_REMEASURE_OLD=1 to refresh"
    cat "${OLD_CACHE}" >>"${OUT_DIR}/results.tsv"
else
    BENCH_BIN="${OLD_BIN}"
    bench_server old
    grep '^old_' "${OUT_DIR}/results.tsv" >"${OLD_CACHE}" &&
        echo "  cached old baseline -> ${OLD_CACHE}"
fi
BENCH_BIN="${NEW_BIN}"
bench_server new

echo
echo "================ STORAGE BENCH SUMMARY ================"
echo "clients=${CLIENTS} jobs=${JOBS} dur=${DURATION}s reps=${REPS}"
echo "(wall-clocked rows report 1/median-seconds in the tps columns)"
echo
awk -F'\t' '
{
  row=$1; p=index(row,"_"); side=substr(row,1,p-1); key=substr(row,p+1);
  tps[key,side]=$2; lat[key,side]=$3; txn[key,side]=$4; cyc[key,side]=$5; load[key,side]=$7;
  if (!(key in seen)) { order[++n]=key; seen[key]=1 }
}
END {
  printf "%-22s %12s %12s %10s %14s %14s %12s\n", \
    "workload","tps(old)","tps(new)","new/old","cyc/q(old)","cyc/q(new)","load(o/n)";
  printf "%-22s %12s %12s %10s %14s %14s %12s\n", \
    "----------------------","------------","------------","----------","--------------","--------------","------------";
  for (i=1;i<=n;i++) {
    k=order[i];
    to=tps[k,"old"]+0; tn=tps[k,"new"]+0;
    r=(to>0)?sprintf("%.2fx",tn/to):"n/a";
    cqo=(txn[k,"old"]+0>0 && cyc[k,"old"]+0>0)?sprintf("%.0f",cyc[k,"old"]/txn[k,"old"]):"-";
    cqn=(txn[k,"new"]+0>0 && cyc[k,"new"]+0>0)?sprintf("%.0f",cyc[k,"new"]/txn[k,"new"]):"-";
    printf "%-22s %12.1f %12.1f %10s %14s %14s %6s/%-6s\n", \
      k,to,tn,r,cqo,cqn,load[k,"old"],load[k,"new"];
  }
}' "${OUT_DIR}/results.tsv" | tee "${OUT_DIR}/summary.txt"
echo "======================================================="
echo "results: ${OUT_DIR}"
