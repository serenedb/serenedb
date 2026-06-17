#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "$0")"/../.. && pwd)"
BUILD_DIR="${PERF_BUILD_DIR:-${ROOT}/build_perf}"
NEW_BIN="${PERF_STORAGE_NEW_BIN:-${BUILD_DIR}/bin/serened}"

OLD_BIN="${1:-${PERF_STORAGE_OLD_BIN:-}}"
if [[ -z "${OLD_BIN}" ]]; then
	echo "usage: $0 <baseline-serened-binary> [quick|full]   (new binary defaults to build_perf/bin/serened)" >&2
	exit 2
fi
PROFILE="${2:-${PERF_PROFILE:-full}}"
full_only() { [[ "${PROFILE}" == full ]]; }

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

phys_cores() {
	cat /sys/devices/system/cpu/cpu[0-9]*/topology/thread_siblings_list |
		sort -t, -k1,1n -u
}

ISOLATE="${PERF_ISOLATE:-0}"
ISOLATE_PHYS="${PERF_ISOLATE_PHYS:-9}"
CG_ROOT="/sys/fs/cgroup"
CG_SHIELD="${CG_ROOT}/sdb_bench_shield"
CG_REST="${CG_ROOT}/sdb_bench_rest"
ISO_ENABLED_CPUSET=0
ISO_ACTIVE=0

_su() { sudo "$@"; }

_su_write() { printf '%s' "$2" | sudo tee "$1" >/dev/null; }

teardown_cpuset_shield() {
	((ISO_ACTIVE == 1)) || return 0
	local cg pid
	for cg in "${CG_SHIELD}" "${CG_REST}"; do
		[[ -e "${cg}/cgroup.procs" ]] || continue
		sudo tee "${cg}/cpuset.cpus.partition" >/dev/null 2>&1 <<<"member" || true
		while read -r pid; do
			[[ -n "${pid}" ]] || continue
			sudo tee "${CG_ROOT}/cgroup.procs" >/dev/null 2>&1 <<<"${pid}" || true
		done < <(sudo cat "${cg}/cgroup.procs" 2>/dev/null || true)
		sudo rmdir "${cg}" 2>/dev/null || true
	done
	if ((ISO_ENABLED_CPUSET == 1)); then
		sudo tee "${CG_ROOT}/cgroup.subtree_control" >/dev/null 2>&1 <<<"-cpuset" || true
	fi
	ISO_ACTIVE=0
	echo "-- cpuset shield torn down"
}

setup_cpuset_shield() {
	command -v sudo >/dev/null 2>&1 || {
		echo "PERF_ISOLATE=1 requires sudo, which is not installed. Aborting." >&2
		exit 1
	}
	if ! sudo -v; then
		echo "PERF_ISOLATE=1 requires working sudo (cgroup writes need root). Aborting." >&2
		exit 1
	fi
	[[ "$(stat -fc %T "${CG_ROOT}" 2>/dev/null)" == cgroup2fs ]] || {
		echo "PERF_ISOLATE=1 needs a cgroup-v2 (unified) hierarchy at ${CG_ROOT}. Aborting." >&2
		exit 1
	}
	grep -qw cpuset "${CG_ROOT}/cgroup.controllers" || {
		echo "cpuset controller not available in ${CG_ROOT}/cgroup.controllers. Aborting." >&2
		exit 1
	}

	mapfile -t _iso_cs < <(phys_cores)
	local _tot=${#_iso_cs[@]}
	((ISOLATE_PHYS < _tot)) || {
		echo "PERF_ISOLATE_PHYS=${ISOLATE_PHYS} must be < ${_tot} physical cores (need a non-empty complement). Aborting." >&2
		exit 1
	}
	local _shield_arr=("${_iso_cs[@]: -$ISOLATE_PHYS}")
	local _rest_arr=("${_iso_cs[@]:0:$((_tot - ISOLATE_PHYS))}")
	local SHIELD_CPUS REST_CPUS
	SHIELD_CPUS="$(
		IFS=,
		echo "${_shield_arr[*]}"
	)"
	REST_CPUS="$(
		IFS=,
		echo "${_rest_arr[*]}"
	)"

	echo "-- PERF_ISOLATE: cpuset shield"
	echo "   shield (reserved) cpus = ${SHIELD_CPUS}"
	echo "   rest   (complement) cpus = ${REST_CPUS}"

	if ! grep -qw cpuset "${CG_ROOT}/cgroup.subtree_control"; then
		_su_write "${CG_ROOT}/cgroup.subtree_control" "+cpuset"
		ISO_ENABLED_CPUSET=1
	fi

	ISO_ACTIVE=1

	_su mkdir -p "${CG_SHIELD}" "${CG_REST}"

	_su_write "${CG_REST}/cpuset.cpus" "${REST_CPUS}"
	_su_write "${CG_REST}/cpuset.mems" "0"
	local _pid
	while read -r _pid; do
		[[ -n "${_pid}" ]] || continue
		printf '%s' "${_pid}" | sudo tee "${CG_REST}/cgroup.procs" >/dev/null 2>&1 || true
	done < <(sudo cat "${CG_ROOT}/cgroup.procs" 2>/dev/null || true)

	_su_write "${CG_SHIELD}/cpuset.cpus" "${SHIELD_CPUS}"
	_su_write "${CG_SHIELD}/cpuset.mems" "0"
	if [[ -e "${CG_SHIELD}/cpuset.cpus.exclusive" ]]; then
		_su_write "${CG_SHIELD}/cpuset.cpus.exclusive" "${SHIELD_CPUS}" || true
	fi
	_su_write "${CG_SHIELD}/cpuset.cpus.partition" "root" || true
	local _part
	_part="$(sudo cat "${CG_SHIELD}/cpuset.cpus.partition" 2>/dev/null || echo '?')"
	if [[ "${_part}" != root ]]; then
		echo "   note: exclusive partition unavailable (partition='${_part}') -- using soft" >&2
		echo "   isolation: other tasks are confined to the rest cgroup off these cpus" >&2
	fi

	printf '%s' "$$" | sudo tee "${CG_SHIELD}/cgroup.procs" >/dev/null || {
		echo "could not move bench shell ($$) into the shield. Aborting." >&2
		exit 1
	}

	export PERF_BENCH_CORES="${SHIELD_CPUS}"
	echo "   PERF_BENCH_CORES=${PERF_BENCH_CORES} (partition=root)"
}

CUR_PID=""
CUR_DATA=""
cleanup() {
	[[ -n "${CUR_PID}" ]] && kill -9 "${CUR_PID}" 2>/dev/null || true
	[[ -n "${CUR_DATA}" ]] && rm -rf "${CUR_DATA}" || true
	teardown_cpuset_shield
}
trap cleanup EXIT

if [[ "${ISOLATE}" == 1 ]]; then
	setup_cpuset_shield
fi

SERVER_PHYS="${PERF_SERVER_PHYS:-6}"
CLIENT_PHYS="${PERF_CLIENT_PHYS:-3}"
if [[ -n "${PERF_SERVER_CORES:-}" && -n "${PERF_CLIENT_CORES:-}" ]]; then
	SERVER_CORES="${PERF_SERVER_CORES}"
	CLIENT_CORES="${PERF_CLIENT_CORES}"
elif [[ -n "${PERF_BENCH_CORES:-}" ]]; then
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

guard() {
	local label="$1" reason="$2"
	shift 2
	set +e
	("$@")
	local rc=$?
	set -e
	if ((rc != 0)); then
		echo "  skip: ${label} (${reason})" >&2
	fi
	return 0
}

mkdir -p "${OUT_DIR}/wl"
cat >"${OUT_DIR}/wl/insert.sql" <<'EOF'
\set id random(1, 999999999999)
\set v random(0, 999)
INSERT INTO bench_ins (id, v, pad) VALUES ((:id)::BIGINT, (:v)::BIGINT, 'padpadpadpadpad');
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

cat >"${OUT_DIR}/wl/insert_inv.sql" <<'EOF'
\set id random(100000000, 999999999)
\set r random(0, 49)
INSERT INTO bench_inv (id, body) VALUES ((:id)::BIGINT, 'token' || :r);
EOF
cat >"${OUT_DIR}/wl/update_inv.sql" <<'EOF'
\set id random(1, 300000)
\set r random(0, 49)
UPDATE bench_inv SET body = 'upd token' || :r WHERE id = :id;
EOF
cat >"${OUT_DIR}/wl/delete_inv.sql" <<'EOF'
\set id random(1, 300000)
DELETE FROM bench_inv WHERE id = :id;
EOF
cat >"${OUT_DIR}/wl/upsert.sql" <<'EOF'
\set id random(1, 100000)
INSERT INTO bench_upd (id, v) VALUES (:id, 0) ON CONFLICT DO NOTHING;
EOF
cat >"${OUT_DIR}/wl/insert_sk.sql" <<'EOF'
\set id random(100000000, 999999999)
\set a random(0, 1000000)
\set b random(0, 1000000)
INSERT INTO bench_sk (id, a, b) VALUES ((:id)::BIGINT, :a, :b);
EOF
cat >"${OUT_DIR}/wl/insert_uniq.sql" <<'EOF'
\set id random(100000000, 999999999)
\set u random(1, 9000000000000000000)
INSERT INTO bench_uniq (id, u) VALUES ((:id)::BIGINT, (:u)::BIGINT);
EOF
cat >"${OUT_DIR}/wl/delete_point.sql" <<'EOF'
\set id random(1, 2000000)
DELETE FROM bench_del WHERE id = :id;
EOF
cat >"${OUT_DIR}/wl/txn_small.sql" <<'EOF'
BEGIN;
INSERT INTO bench_txn(body) SELECT 'tok' || g FROM generate_series(1, 10) g;
COMMIT;
EOF
cat >"${OUT_DIR}/wl/rollback.sql" <<'EOF'
BEGIN;
INSERT INTO bench_txn(body) SELECT 'tok' || g FROM generate_series(1, 10) g;
ROLLBACK;
EOF
cat >"${OUT_DIR}/wl/search_materialize.sql" <<'EOF'
\set tok random(0, 49)
SELECT id, pad FROM bench_txt_idx WHERE body @@ ts_phrase('token' || :tok) LIMIT 10;
EOF
cat >"${OUT_DIR}/wl/insert_sweep.sql" <<'EOF'
\set id random(1, 999999999999)
\set v random(0, 999)
INSERT INTO bench_sweep (id, v, pad) VALUES ((:id)::BIGINT, (:v)::BIGINT, 'padpadpadpadpad');
EOF

run_pgb() {
	local label="$1" sqlfile="$2" mode="$3" clients="${4:-${CLIENTS}}"
	shift 4 || true
	local extra=("$@")
	local pgb_log="${OUT_DIR}/pgbench_${label}.log"
	local stat_log="${OUT_DIR}/stat_${label}.txt"
	local log_prefix="${OUT_DIR}/pgblog_${label}"
	wait_quiet
	if [[ "${PGB_NO_WARMUP:-0}" != 1 ]]; then
		taskset -c "${CLIENT_CORES}" pgbench -h 127.0.0.1 -p "${PORT}" -U postgres \
			-n -f "${sqlfile}" -M "${mode}" -c "${clients}" -j "${JOBS}" -T 2 \
			"${extra[@]}" postgres >/dev/null 2>&1 || true
	fi
	taskset -c "${CLIENT_CORES}" pgbench -h 127.0.0.1 -p "${PORT}" -U postgres \
		-n -f "${sqlfile}" -M "${mode}" -c "${clients}" -j "${JOBS}" \
		-l --sampling-rate=0.1 --log-prefix "${log_prefix}" \
		-T "${DURATION}" "${extra[@]}" postgres >"${pgb_log}" 2>&1 &
	local pgb_pid=$!
	perf stat -p "${CUR_PID}" -e task-clock,cycles,instructions \
		-o "${stat_log}" -- sleep "$((DURATION - 2))" >/dev/null 2>&1 || true
	wait "${pgb_pid}" 2>/dev/null || true

	local tps lat txns cycles insns load p50 p99
	tps=$(awk -F'= ' '/tps = .*without initial/{print $2+0}' "${pgb_log}")
	[[ -z "${tps}" ]] && tps=$(awk -F'= ' '/^tps =/{print $2+0; exit}' "${pgb_log}")
	lat=$(awk -F'= ' '/latency average/{print $2+0}' "${pgb_log}")
	txns=$(awk '/number of transactions actually processed/{print $NF+0}' "${pgb_log}")
	cycles=$(awk '/cycles/{gsub(/[^0-9]/,"",$1); print $1; exit}' "${stat_log}" 2>/dev/null)
	insns=$(awk '/instructions/{gsub(/[^0-9]/,"",$1); print $1; exit}' "${stat_log}" 2>/dev/null)
	load="$(awk '{print $1}' /proc/loadavg)"
	p50=0
	p99=0
	read -r p50 p99 < <(
		set +e
		cat "${log_prefix}".* 2>/dev/null |
			awk '{print $3+0}' | sort -n |
			awk '{a[NR]=$0}
			     END{
			       n=NR;
			       if(n==0){print "0 0";exit}
			       i50=int(0.5*n); if(0.5*n>i50)i50++; if(i50<1)i50=1; if(i50>n)i50=n;
			       i99=int(0.99*n); if(0.99*n>i99)i99++; if(i99<1)i99=1; if(i99>n)i99=n;
			       printf "%s %s\n", a[i50]+0, a[i99]+0;
			     }' 2>/dev/null
		set -e
	) || true
	[[ -z "${p50}" ]] && p50=0
	[[ -z "${p99}" ]] && p99=0
	rm -f "${log_prefix}".* 2>/dev/null || true
	printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "${label}" "${tps:-0}" "${lat:-0}" \
		"${txns:-0}" "${cycles:-0}" "${insns:-0}" "${load}" "${p50}" "${p99}" >>"${OUT_DIR}/results.tsv"
	printf '  %-26s tps=%-12s lat=%-9s p50=%-8s p99=%-8s load=%s\n' \
		"${label}" "${tps:-0}" "${lat:-0}" "${p50}" "${p99}" "${load}"
}

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
	printf '%s\t%.4f\t%s\t1\t0\t0\t%s\t0\t0\n' "${label}" "${tps}" "${med}" "${load}" >>"${OUT_DIR}/results.tsv"
	printf '  %-26s median=%-8ss (1/t=%.3f) load=%s\n' "${label}" "${med}" "${tps}" "${load}"
}

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
	printf '%s\t%.4f\t%s\t1\t0\t0\t%s\t0\t0\n' "${srv}_recovery" "${tps}" "${med}" "${load}" >>"${OUT_DIR}/results.tsv"
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

	"${PSQL[@]}" \
		-c "CREATE TEXT SEARCH DICTIONARY bench_dict(template = 'text', locale = 'en_US.UTF-8', case = 'none', stemming = false, accent = false, frequency = true, position = true);" \
		-c "CREATE TABLE bench_ins (id BIGINT PRIMARY KEY, v BIGINT, pad TEXT);" \
		-c "CREATE TABLE bench_upd (id INTEGER PRIMARY KEY, v BIGINT);" \
		-c "INSERT INTO bench_upd SELECT x, 0 FROM generate_series(1, 100000) t(x);" \
		-c "CREATE TABLE bench_seq (id BIGSERIAL PRIMARY KEY, v INTEGER);" \
		>/dev/null

	run_pgb "${srv}_insert_1c" "${OUT_DIR}/wl/insert.sql" prepared 1
	run_pgb "${srv}_insert_${CLIENTS}c" "${OUT_DIR}/wl/insert.sql" prepared "${CLIENTS}"
	run_pgb "${srv}_update_${CLIENTS}c" "${OUT_DIR}/wl/update.sql" prepared "${CLIENTS}"
	run_pgb "${srv}_nextval_${CLIENTS}c" "${OUT_DIR}/wl/nextval.sql" prepared "${CLIENTS}"

	grp_inv_writes() {
		"${PSQL[@]}" \
			-c "CREATE TABLE bench_inv (id BIGINT PRIMARY KEY, body TEXT);" \
			-c "INSERT INTO bench_inv SELECT x, 'lorem token' || (x % 50) FROM generate_series(1, 300000) t(x);" \
			-c "CREATE INDEX bench_inv_idx ON bench_inv USING inverted(body bench_dict);" >/dev/null
		"${PSQL[@]}" -c "VACUUM (REFRESH_TABLE) bench_inv;" >/dev/null
		run_pgb "${srv}_insert_inv_${CLIENTS}c" "${OUT_DIR}/wl/insert_inv.sql" prepared "${CLIENTS}"
		run_pgb "${srv}_update_inv_${CLIENTS}c" "${OUT_DIR}/wl/update_inv.sql" prepared "${CLIENTS}"
		run_pgb "${srv}_delete_inv_${CLIENTS}c" "${OUT_DIR}/wl/delete_inv.sql" prepared "${CLIENTS}"
	}
	guard "${srv}_inv_writes" "inverted-table writes unsupported" grp_inv_writes

	guard "${srv}_upsert_${CLIENTS}c" "ON CONFLICT unsupported" \
		run_pgb "${srv}_upsert_${CLIENTS}c" "${OUT_DIR}/wl/upsert.sql" prepared "${CLIENTS}"

	grp_sk_writes() {
		"${PSQL[@]}" \
			-c "CREATE TABLE bench_sk (id BIGINT PRIMARY KEY, a INTEGER, b INTEGER);" \
			-c "CREATE INDEX bench_sk_a ON bench_sk(a);" \
			-c "CREATE INDEX bench_sk_b ON bench_sk(b);" >/dev/null
		run_pgb "${srv}_insert_sk_${CLIENTS}c" "${OUT_DIR}/wl/insert_sk.sql" prepared "${CLIENTS}"
	}
	guard "${srv}_insert_sk_${CLIENTS}c" "secondary-index writes unsupported" grp_sk_writes

	grp_uniq_writes() {
		"${PSQL[@]}" -c "CREATE TABLE bench_uniq (id BIGINT PRIMARY KEY, u BIGINT UNIQUE);" >/dev/null
		run_pgb "${srv}_insert_uniq_${CLIENTS}c" "${OUT_DIR}/wl/insert_uniq.sql" prepared "${CLIENTS}"
	}
	guard "${srv}_insert_uniq_${CLIENTS}c" "UNIQUE constraint unsupported" grp_uniq_writes

	grp_delete_point() {
		"${PSQL[@]}" \
			-c "CREATE TABLE bench_del (id BIGINT PRIMARY KEY, v BIGINT);" \
			-c "INSERT INTO bench_del SELECT x, x FROM generate_series(1, 2000000) t(x);" >/dev/null
		run_pgb "${srv}_delete_point_${CLIENTS}c" "${OUT_DIR}/wl/delete_point.sql" prepared "${CLIENTS}"
	}
	guard "${srv}_delete_point_${CLIENTS}c" "point delete unsupported" grp_delete_point

	run_wall "${srv}_bulk_insert_1m" "${REPS}" \
		"CREATE TABLE bench_bulk (id INTEGER PRIMARY KEY, grp INTEGER, val DOUBLE PRECISION, pad TEXT);" \
		"INSERT INTO bench_bulk SELECT x, x % 100, x * 0.5, 'padpadpadpadpad' || x FROM generate_series(1, 1000000) t(x);" \
		"DROP TABLE bench_bulk;"

	grp_copy_out() {
		run_wall "${srv}_copy_out_1m" "${REPS}" "" \
			"COPY (SELECT x, x % 100, x * 0.5, 'pad' || x FROM generate_series(1, 1000000) t(x)) TO '${OUT_DIR}/copy_data.csv' (FORMAT csv);" \
			""
	}
	guard "${srv}_copy_out_1m" "COPY TO file unsupported" grp_copy_out
	grp_copy_in() {
		run_wall "${srv}_copy_in_1m" "${REPS}" \
			"CREATE TABLE IF NOT EXISTS bench_copy(id INTEGER, grp INTEGER, val DOUBLE PRECISION, pad TEXT); TRUNCATE bench_copy;" \
			"COPY bench_copy FROM '${OUT_DIR}/copy_data.csv' (FORMAT csv);" \
			""
		"${PSQL[@]}" -c "DROP TABLE IF EXISTS bench_copy;" >/dev/null
	}
	guard "${srv}_copy_in_1m" "COPY FROM file unsupported" grp_copy_in

	grp_txn() {
		"${PSQL[@]}" \
			-c "CREATE TABLE bench_txn (id BIGSERIAL PRIMARY KEY, body TEXT);" \
			-c "CREATE INDEX bench_txn_idx ON bench_txn USING inverted(body bench_dict);" >/dev/null
		run_pgb "${srv}_txn_small_${CLIENTS}c" "${OUT_DIR}/wl/txn_small.sql" simple "${CLIENTS}"
		run_wall "${srv}_txn_large" "${REPS}" "" \
			"BEGIN; INSERT INTO bench_txn(body) SELECT 'tok' || g FROM generate_series(1, 100000) g; COMMIT;" \
			"TRUNCATE bench_txn;"
		run_pgb "${srv}_rollback_${CLIENTS}c" "${OUT_DIR}/wl/rollback.sql" simple "${CLIENTS}"
	}
	guard "${srv}_txn" "transactional inverted writes unsupported" grp_txn

	"${PSQL[@]}" \
		-c "CREATE TABLE bench_big (id INTEGER PRIMARY KEY, grp INTEGER, val DOUBLE PRECISION, pad TEXT);" \
		-c "INSERT INTO bench_big SELECT x, x % 100, x * 0.5, 'padpadpadpadpad' || x FROM generate_series(1, 500000) t(x);" \
		>/dev/null

	run_pgb "${srv}_pk_lookup" "${OUT_DIR}/wl/lookup.sql" prepared "${CLIENTS}"
	run_pgb "${srv}_pk_range" "${OUT_DIR}/wl/range.sql" prepared "${CLIENTS}"
	run_pgb "${srv}_fullscan_agg" "${OUT_DIR}/wl/agg.sql" prepared 2

	run_wall "${srv}_sk_build" "${REPS}" "" \
		"CREATE INDEX bench_big_grp ON bench_big(grp);" \
		"DROP INDEX bench_big_grp;"
	"${PSQL[@]}" -c "CREATE INDEX bench_big_grp ON bench_big(grp);" >/dev/null
	run_pgb "${srv}_sk_lookup" "${OUT_DIR}/wl/sk_lookup.sql" prepared "${CLIENTS}"

	"${PSQL[@]}" \
		-c "CREATE TABLE bench_txt (id INTEGER PRIMARY KEY, body TEXT, pad TEXT);" \
		-c "INSERT INTO bench_txt SELECT x, 'lorem ipsum dolor sit amet word' || (x % 1000) || ' token' || (x % 50), 'padpadpadpadpad' || x FROM generate_series(1, 200000) t(x);" \
		>/dev/null
	run_wall "${srv}_inverted_build" "${REPS}" "" \
		"CREATE INDEX bench_txt_idx ON bench_txt USING inverted(body bench_dict);" \
		"DROP INDEX bench_txt_idx;"
	"${PSQL[@]}" -c "CREATE INDEX bench_txt_idx ON bench_txt USING inverted(body bench_dict);" >/dev/null
	"${PSQL[@]}" -c "VACUUM (REFRESH_TABLE) bench_txt;" >/dev/null
	run_pgb "${srv}_search_fetch" "${OUT_DIR}/wl/search.sql" prepared "${CLIENTS}"
	guard "${srv}_search_materialize" "search materialize unsupported" \
		run_pgb "${srv}_search_materialize" "${OUT_DIR}/wl/search_materialize.sql" prepared "${CLIENTS}"

	guard "${srv}_truncate_plain" "TRUNCATE plain unsupported" \
		run_wall "${srv}_truncate_plain" "${REPS}" \
		"DROP TABLE IF EXISTS bench_trunc; CREATE TABLE bench_trunc (id INTEGER PRIMARY KEY, v BIGINT); INSERT INTO bench_trunc SELECT x, x FROM generate_series(1, 200000) t(x);" \
		"TRUNCATE bench_trunc;" \
		"DROP TABLE IF EXISTS bench_trunc;"
	guard "${srv}_truncate_inv" "TRUNCATE inverted unsupported" \
		run_wall "${srv}_truncate_inv" "${REPS}" \
		"DROP TABLE IF EXISTS bench_trunci; CREATE TABLE bench_trunci (id INTEGER PRIMARY KEY, body TEXT); INSERT INTO bench_trunci SELECT x, 'lorem token' || (x % 50) FROM generate_series(1, 200000) t(x); CREATE INDEX bench_trunci_idx ON bench_trunci USING inverted(body bench_dict);" \
		"TRUNCATE bench_trunci;" \
		"DROP TABLE IF EXISTS bench_trunci;"

	grp_bulk_insert_inv() {
		"${PSQL[@]}" \
			-c "CREATE TABLE bench_inv2 (id BIGINT PRIMARY KEY, body TEXT);" \
			-c "CREATE INDEX bench_inv2_idx ON bench_inv2 USING inverted(body bench_dict);" >/dev/null
		run_wall "${srv}_bulk_insert_inv" "${REPS}" \
			"TRUNCATE bench_inv2;" \
			"INSERT INTO bench_inv2 SELECT x, 'lorem token' || (x % 50) FROM generate_series(1, 500000) t(x);" \
			""
		"${PSQL[@]}" -c "DROP TABLE bench_inv2;" >/dev/null
	}
	guard "${srv}_bulk_insert_inv" "bulk insert into inverted unsupported" grp_bulk_insert_inv
	run_wall "${srv}_bulk_update" "${REPS}" \
		"DROP TABLE IF EXISTS bench_bupd; CREATE TABLE bench_bupd (id INTEGER PRIMARY KEY, v BIGINT); INSERT INTO bench_bupd SELECT x, x FROM generate_series(1, 500000) t(x);" \
		"UPDATE bench_bupd SET v = v + 1;" \
		"DROP TABLE IF EXISTS bench_bupd;"
	run_wall "${srv}_bulk_delete" "${REPS}" \
		"DROP TABLE IF EXISTS bench_bdel; CREATE TABLE bench_bdel (id INTEGER PRIMARY KEY, v BIGINT); INSERT INTO bench_bdel SELECT x, x FROM generate_series(1, 500000) t(x);" \
		"DELETE FROM bench_bdel WHERE id <= 250000;" \
		"DROP TABLE IF EXISTS bench_bdel;"

	guard "${srv}_vacuum_refresh" "VACUUM REFRESH_TABLE unsupported" \
		run_wall "${srv}_vacuum_refresh" "${REPS}" "" \
		"VACUUM (REFRESH_TABLE) bench_inv;" \
		""

	if full_only; then
		run_wall "${srv}_bulk_insert_100k" "${REPS}" \
			"DROP TABLE IF EXISTS bench_scale; CREATE TABLE bench_scale (id INTEGER PRIMARY KEY, grp INTEGER, val DOUBLE PRECISION, pad TEXT);" \
			"INSERT INTO bench_scale SELECT x, x % 100, x * 0.5, 'padpadpadpadpad' || x FROM generate_series(1, 100000) t(x);" \
			"DROP TABLE IF EXISTS bench_scale;"
		run_wall "${srv}_bulk_insert_10m" "${REPS}" \
			"DROP TABLE IF EXISTS bench_scale; CREATE TABLE bench_scale (id INTEGER PRIMARY KEY, grp INTEGER, val DOUBLE PRECISION, pad TEXT);" \
			"INSERT INTO bench_scale SELECT x, x % 100, x * 0.5, 'padpadpadpadpad' || x FROM generate_series(1, 10000000) t(x);" \
			"DROP TABLE IF EXISTS bench_scale;"
		grp_insert_sweep() {
			"${PSQL[@]}" \
				-c "DROP TABLE IF EXISTS bench_sweep;" \
				-c "CREATE TABLE bench_sweep (id BIGINT PRIMARY KEY, v BIGINT, pad TEXT);" >/dev/null
			run_pgb "${srv}_insert_sweep_1c" "${OUT_DIR}/wl/insert_sweep.sql" prepared 1
			run_pgb "${srv}_insert_sweep_4c" "${OUT_DIR}/wl/insert_sweep.sql" prepared 4
			run_pgb "${srv}_insert_sweep_8c" "${OUT_DIR}/wl/insert_sweep.sql" prepared 8
			run_pgb "${srv}_insert_sweep_16c" "${OUT_DIR}/wl/insert_sweep.sql" prepared 16
			"${PSQL[@]}" -c "DROP TABLE IF EXISTS bench_sweep;" >/dev/null
		}
		guard "${srv}_insert_sweep" "insert sweep unsupported" grp_insert_sweep
	fi

	local dirbytes peakrss load
	dirbytes="$(du -sb "${datadir}" 2>/dev/null | awk '{print $1+0}')"
	peakrss="$(awk '/^VmHWM:/{print $2+0}' "/proc/${CUR_PID}/status" 2>/dev/null)"
	load="$(awk '{print $1}' /proc/loadavg)"
	printf '%s\t%s\t0\t1\t0\t0\t%s\t0\t0\n' "${srv}_datadir_bytes" "${dirbytes:-0}" "${load}" \
		>>"${OUT_DIR}/results.tsv"
	printf '%s\t%s\t0\t1\t0\t0\t%s\t0\t0\n' "${srv}_peak_rss_kb" "${peakrss:-0}" "${load}" \
		>>"${OUT_DIR}/results.tsv"
	printf '  %-26s datadir_bytes=%s\n' "${srv}_datadir_bytes" "${dirbytes:-0}"
	printf '  %-26s peak_rss_kb=%s\n' "${srv}_peak_rss_kb" "${peakrss:-0}"

	run_recovery "${srv}" "${datadir}"

	PGB_NO_WARMUP=1 run_pgb "${srv}_pk_lookup_cold" "${OUT_DIR}/wl/lookup.sql" prepared "${CLIENTS}"

	stop_serened
	rm -rf "${datadir}"
	CUR_DATA=""
}

: >"${OUT_DIR}/results.tsv"
echo "old: ${OLD_BIN}"
echo "new: ${NEW_BIN}"
echo "out: ${OUT_DIR}"
echo "profile=${PROFILE} clients=${CLIENTS} jobs=${JOBS} dur=${DURATION}s reps=${REPS} quiet_load<=${QUIET_LOAD}"
echo

if [[ -s "${OLD_CACHE}" && "${REMEASURE_OLD}" != 1 ]]; then
	echo "note: reusing cached old baseline; pass PERF_REMEASURE_OLD=1 to remeasure (required after adding workloads)" >&2
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
  p99[key,side]=$9;
  if (!(key in seen)) { order[++n]=key; seen[key]=1 }
}
END {
  printf "%-22s %12s %12s %10s %14s %14s %10s %10s %12s %s\n", \
    "workload","tps(old)","tps(new)","new/old","cyc/q(old)","cyc/q(new)","p99(old)","p99(new)","load(o/n)","flag";
  printf "%-22s %12s %12s %10s %14s %14s %10s %10s %12s %s\n", \
    "----------------------","------------","------------","----------","--------------","--------------","----------","----------","------------","----";
  for (i=1;i<=n;i++) {
    k=order[i];
    # footprint/memory keys are reported in their own table below.
    if (k ~ /_(bytes|rss_kb)$/) continue;
    to=tps[k,"old"]+0; tn=tps[k,"new"]+0;
    r=(to>0)?sprintf("%.2fx",tn/to):"n/a";
    cqo=(txn[k,"old"]+0>0 && cyc[k,"old"]+0>0)?sprintf("%.0f",cyc[k,"old"]/txn[k,"old"]):"-";
    cqn=(txn[k,"new"]+0>0 && cyc[k,"new"]+0>0)?sprintf("%.0f",cyc[k,"new"]/txn[k,"new"]):"-";
    # READ workloads tolerate less regression headroom than WRITEs.
    thr=(k ~ /lookup|range|scan|search/)?0.90:0.85;
    flag="ok";
    if (to>0 && tn/to<thr) flag="REGRESSION";
    printf "%-22s %12.1f %12.1f %10s %14s %14s %10s %10s %6s/%-6s %s\n", \
      k,to,tn,r,cqo,cqn,p99[k,"old"]+0,p99[k,"new"]+0,load[k,"old"],load[k,"new"],flag;
  }
  # Footprint + memory: lower is better, so a larger new value is the concern.
  print "";
  printf "%-22s %16s %16s %10s %s\n", "footprint/memory","old","new","new/old","flag";
  printf "%-22s %16s %16s %10s %s\n", "----------------------","----------------","----------------","----------","----";
  split("datadir_bytes peak_rss_kb", fk, " ");
  for (j=1;j<=2;j++) {
    k=fk[j];
    if (!(k in seen)) continue;
    fo=tps[k,"old"]+0; fn=tps[k,"new"]+0;
    rr=(fo>0)?sprintf("%.2fx",fn/fo):"n/a";
    fflag="ok";
    if (k=="datadir_bytes" && fo>0 && fn>1.20*fo) fflag="REGRESSION";
    printf "%-22s %16.0f %16.0f %10s %s\n", k,fo,fn,rr,fflag;
  }
}' "${OUT_DIR}/results.tsv" | tee "${OUT_DIR}/summary.txt"
echo "======================================================="
echo "results: ${OUT_DIR}"
