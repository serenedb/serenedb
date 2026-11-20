#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")"/../.. && pwd)"
OLD_BIN="${PERF_RBAC_OLD_BIN:?set PERF_RBAC_OLD_BIN to the baseline serened}"
NEW_BIN="${PERF_RBAC_NEW_BIN:-${ROOT}/build_perf/bin/serened}"
RESULTS_DIR="${ROOT}/scripts/perf/results"
OUT="${RESULTS_DIR}/rbac-$(date -u +%Y%m%dT%H%M%SZ).log"

PORT="${PERF_RBAC_PORT:-6498}"
REPS="${PERF_RBAC_REPS:-3}"
DURATION="${PERF_RBAC_SECONDS:-10}"
ROWS="${PERF_RBAC_ROWS:-100000}"
N_OBJECTS="${PERF_RBAC_OBJECTS:-100000}"
N_FILES="${PERF_RBAC_FILES:-1000}"
N_GRANT="${PERF_RBAC_GRANT_TABLES:-2000}"
N_GRANTEES="${PERF_RBAC_GRANTEES:-200}"
COMMENT_CALLS="${PERF_RBAC_COMMENT_CALLS:-200}"

for bin in "${OLD_BIN}" "${NEW_BIN}"; do
	[[ -x "${bin}" ]] || {
		echo "missing binary: ${bin}" >&2
		exit 1
	}
done
mkdir -p "${RESULTS_DIR}"

WORK="$(mktemp -d "/tmp/${USER}-rbac-bench-XXXXXX")"
DATA_DIR=""
SERENED_PID=""
export PGPASSWORD=bench

stop_server() {
	if [[ -n "${SERENED_PID}" ]]; then
		kill "${SERENED_PID}" 2>/dev/null || true
		wait "${SERENED_PID}" 2>/dev/null || true
	fi
	SERENED_PID=""
	[[ -n "${DATA_DIR}" ]] && rm -rf "${DATA_DIR}"
	DATA_DIR=""
}
cleanup() {
	stop_server
	rm -rf "${WORK}"
}
trap cleanup EXIT

start_server() {
	if [[ -n "$(ss -ltnH "sport = :${PORT}")" ]]; then
		echo "port ${PORT} is already in use" >&2
		exit 1
	fi
	DATA_DIR="$(mktemp -d "${WORK}/data-XXXXXX")"
	"$1" "${DATA_DIR}" --listen "postgres://127.0.0.1:${PORT}" >"${WORK}/serened.log" 2>&1 &
	SERENED_PID=$!
	for _ in $(seq 1 120); do
		psql -h 127.0.0.1 -p "${PORT}" -U postgres -d postgres -qtAXc 'SELECT 1' >/dev/null 2>&1 && return 0
		kill -0 "${SERENED_PID}" 2>/dev/null || break
		sleep 0.5
	done
	echo "server failed to start:" >&2
	tail -5 "${WORK}/serened.log" >&2
	exit 1
}

sql() { psql -h 127.0.0.1 -p "${PORT}" -U "${SQL_USER:-postgres}" -d postgres -qtAX -v ON_ERROR_STOP=1 "$@"; }

now_ms() { date +%s%3N; }

timed_ms() {
	local t0 t1
	t0=$(now_ms)
	"$@" >/dev/null
	t1=$(now_ms)
	echo $((t1 - t0))
}

tps() { # script clients protocol [user]
	pgbench -n -h 127.0.0.1 -p "${PORT}" -U "${4:-bench_reader}" -d postgres -f "$1" -c "$2" -j "$2" \
		-T "${DURATION}" -M "$3" 2>&1 | awk '/^tps = /{print int($3)}' || true
}

latency_ms() { # script transactions user
	pgbench -n -h 127.0.0.1 -p "${PORT}" -U "$3" -d postgres -f "$1" -c 1 -t "$2" 2>&1 |
		awk '/^latency average = /{print $4}' || true
}

write_scripts() {
	printf '\\set id random(0, %d)\nSELECT v FROM bench_t WHERE id = :id;\n' $((ROWS - 1)) >"${WORK}/point.sql"
	printf '\\set id random(0, %d)\nSELECT v FROM bench_v WHERE id = :id;\n' $((ROWS - 1)) >"${WORK}/view.sql"
	printf '\\set id random(0, %d)\nSET ROLE bench_alt;\nSELECT v FROM bench_t WHERE id = :id;\nRESET ROLE;\nSELECT v FROM bench_t WHERE id = :id;\n' \
		$((ROWS - 1)) >"${WORK}/set_role.sql"
	printf '\\set n random(1, 1000000000)\nCREATE ROLE bench_churn_:n;\nDROP ROLE bench_churn_:n;\n' >"${WORK}/churn.sql"
	echo 'SELECT count(*) FROM pg_class;' >"${WORK}/pg_class.sql"
	echo 'SELECT count(*) FROM information_schema.tables;' >"${WORK}/information_schema.sql"
	{
		echo "CREATE SCHEMA bench_many;"
		for ((i = 0; i < N_OBJECTS; i += 1000)); do
			echo "BEGIN;"
			for ((j = i; j < i + 1000 && j < N_OBJECTS; j++)); do
				echo "CREATE TABLE bench_many.t${j} (a INTEGER);"
			done
			echo "COMMIT;"
		done
	} >"${WORK}/objects.sql"
	{
		echo "BEGIN;"
		for ((i = 0; i < N_GRANT; i++)); do
			echo "CREATE TABLE bench_g.t${i} (a INTEGER);"
		done
		echo "COMMIT;"
	} >"${WORK}/grant_tables.sql"
	{
		for ((i = 0; i < N_GRANTEES; i++)); do
			echo "CREATE ROLE bench_w${i};"
			echo "GRANT SELECT ON bench_wide TO bench_w${i};"
		done
	} >"${WORK}/grantees.sql"
	for ((i = 0; i < COMMENT_CALLS; i++)); do
		echo "COMMENT ON TABLE bench_wide IS 'c${i}';"
	done >"${WORK}/comment.sql"
	mkdir -p "${WORK}/parquet"
	for ((i = 0; i < N_FILES; i++)); do
		echo "COPY (SELECT i FROM range(100) t(i)) TO '${WORK}/parquet/p${i}.parquet' (FORMAT PARQUET);"
	done >"${WORK}/parquet_files.sql"
	echo "SELECT count(*) FROM read_parquet('${WORK}/parquet/*.parquet');" >"${WORK}/parquet.sql"
}

setup() {
	sql <<SQL
CREATE TABLE bench_t (id INTEGER PRIMARY KEY, v TEXT);
INSERT INTO bench_t SELECT i, 'v' || i FROM range(${ROWS}) t(i);
CREATE ROLE bench_owner;
CREATE ROLE bench_alt;
CREATE ROLE bench_reader LOGIN PASSWORD 'bench';
CREATE ROLE bench_ddl LOGIN PASSWORD 'bench';
CREATE ROLE bench_files LOGIN PASSWORD 'bench';
GRANT bench_alt TO bench_reader;
GRANT SELECT ON bench_t TO bench_owner;
GRANT SELECT ON bench_t TO bench_alt;
GRANT SELECT ON bench_t TO bench_reader;
CREATE VIEW bench_v AS SELECT id, v FROM bench_t;
ALTER VIEW bench_v OWNER TO bench_owner;
GRANT SELECT ON bench_v TO bench_reader;
CREATE SCHEMA bench_g AUTHORIZATION bench_ddl;
CREATE TABLE bench_wide (a INTEGER);
ALTER TABLE bench_wide OWNER TO bench_ddl;
SQL
	sql -c "GRANT pg_read_server_files TO bench_files" >/dev/null 2>&1 || true
	SQL_USER=bench_ddl sql -f "${WORK}/grant_tables.sql" >/dev/null
	sql -f "${WORK}/grantees.sql" >/dev/null
	sql -f "${WORK}/parquet_files.sql" >/dev/null
}

declare -A RESULT

record() { # name binary value
	RESULT["$1|$2"]+="$3 "
}

measure() { # binary label
	local bin=$2
	record point_simple_1 "${bin}" "$(tps "${WORK}/point.sql" 1 simple)"
	record point_simple_16 "${bin}" "$(tps "${WORK}/point.sql" 16 simple)"
	record point_prepared_1 "${bin}" "$(tps "${WORK}/point.sql" 1 prepared)"
	record point_prepared_16 "${bin}" "$(tps "${WORK}/point.sql" 16 prepared)"
	record view_simple_1 "${bin}" "$(tps "${WORK}/view.sql" 1 simple)"
	record view_simple_16 "${bin}" "$(tps "${WORK}/view.sql" 16 simple)"
	record view_prepared_1 "${bin}" "$(tps "${WORK}/view.sql" 1 prepared)"
	record view_prepared_16 "${bin}" "$(tps "${WORK}/view.sql" 16 prepared)"
	record set_role_prepared_1 "${bin}" "$(tps "${WORK}/set_role.sql" 1 prepared)"
	pgbench -n -h 127.0.0.1 -p "${PORT}" -U postgres -d postgres -f "${WORK}/churn.sql" -c 1 -T "${DURATION}" \
		>"${WORK}/churn.log" 2>&1 &
	local churn=$!
	record point_prepared_4_with_create_role "${bin}" "$(tps "${WORK}/point.sql" 4 prepared)"
	wait "${churn}" || true
	record grant_all_ms "${bin}" "$(SQL_USER=bench_ddl timed_ms sql -c "GRANT SELECT ON ALL TABLES IN SCHEMA bench_g TO bench_reader")"
	record revoke_all_ms "${bin}" "$(SQL_USER=bench_ddl timed_ms sql -c "REVOKE SELECT ON ALL TABLES IN SCHEMA bench_g FROM bench_reader")"
	record comment_ms "${bin}" "$(SQL_USER=bench_ddl timed_ms sql -f "${WORK}/comment.sql")"
	record read_parquet_superuser_ms "${bin}" "$(latency_ms "${WORK}/parquet.sql" 10 postgres)"
	local files_ms
	files_ms="$(latency_ms "${WORK}/parquet.sql" 10 bench_files)"
	record read_parquet_role_ms "${bin}" "${files_ms:-refused}"
}

measure_objects() { # binary label
	local bin=$2
	sql -f "${WORK}/objects.sql" >/dev/null
	record pg_class_ms "${bin}" "$(latency_ms "${WORK}/pg_class.sql" 10 bench_reader)"
	record information_schema_ms "${bin}" "$(latency_ms "${WORK}/information_schema.sql" 10 bench_reader)"
}

median() {
	tr ' ' '\n' | { grep -v '^$' || true; } | sort -g | awk '{v[NR]=$1} END {if (NR == 0) print "-"; else if (v[1] !~ /^[0-9.]+$/) print v[1]; else print (NR % 2 ? v[(NR + 1) / 2] : (v[NR / 2] + v[NR / 2 + 1]) / 2)}'
}

write_scripts
{
	echo "old ${OLD_BIN}"
	echo "new ${NEW_BIN}"
	echo "reps ${REPS}, ${DURATION} s per pgbench run, ${ROWS} rows, ${N_OBJECTS} objects, ${N_FILES} files, ${N_GRANT} tables, ${N_GRANTEES} grantees"
	echo "load before: $(cut -d' ' -f1-3 /proc/loadavg)"
} | tee "${OUT}"

for ((rep = 1; rep <= REPS; rep++)); do
	for side in old new; do
		bin_var="${side^^}_BIN"
		start_server "${!bin_var}"
		setup
		measure "${!bin_var}" "${side}"
		stop_server
		echo "rep ${rep} ${side} done, load $(cut -d' ' -f1-3 /proc/loadavg)" | tee -a "${OUT}"
	done
done
for side in old new; do
	bin_var="${side^^}_BIN"
	start_server "${!bin_var}"
	setup
	measure_objects "${!bin_var}" "${side}"
	stop_server
	echo "objects ${side} done, load $(cut -d' ' -f1-3 /proc/loadavg)" | tee -a "${OUT}"
done

{
	printf '%-36s %14s %14s %8s   %s\n' workload old new new/old "runs (old | new)"
	for name in point_simple_1 point_simple_16 point_prepared_1 point_prepared_16 view_simple_1 view_simple_16 \
		view_prepared_1 view_prepared_16 set_role_prepared_1 point_prepared_4_with_create_role grant_all_ms \
		revoke_all_ms comment_ms read_parquet_superuser_ms read_parquet_role_ms pg_class_ms information_schema_ms; do
		old_runs="${RESULT["${name}|old"]:-}"
		new_runs="${RESULT["${name}|new"]:-}"
		old=$(echo "${old_runs}" | median)
		new=$(echo "${new_runs}" | median)
		ratio=$(awk -v a="${old}" -v b="${new}" 'BEGIN {if (a ~ /^[0-9.]+$/ && b ~ /^[0-9.]+$/ && a > 0) printf "%.3f", b / a; else print "-"}')
		printf '%-36s %14s %14s %8s   %s| %s\n' "${name}" "${old}" "${new}" "${ratio}" "${old_runs}" "${new_runs}"
	done
	echo "load after: $(cut -d' ' -f1-3 /proc/loadavg)"
} | tee -a "${OUT}"
