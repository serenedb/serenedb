#!/bin/bash
#
# Driver for the vendored duckdb_postgres extension's upstream test suite
# (third_party/duckdb_postgres/test/sql/**/*.test).
#
# Brings up a postgres fixture in docker, then runs DuckDB's `unittest`
# binary against the upstream `.test` files.
#
# Requires `unittest` to be built. It is by default; the driver will run
# `ninja unittest` if the binary isn't present and the cmake flag isn't
# explicitly OFF.
#
# Usage:
#   tests/postgres_scanner/run.sh                  # docker fixture (default)
#   tests/postgres_scanner/run.sh --use-existing   # CI: postgres already up,
#                                                  #     PGHOST/PGPORT supplied
#
# Env:
#   BUILD_DIR    (default: build)
#   REPORTS_DIR  (default: tests/sqllogic)       -- where JUnit XML lands
#   PGHOST/PGPORT/PGUSER/PGDATABASE -- required when --use-existing

set -eo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
WORKSPACE=$(cd "$SCRIPT_DIR/../.." && pwd)

: "${BUILD_DIR:=build}"
: "${REPORTS_DIR:=$WORKSPACE/tests/sqllogic}"

FIXTURE_MODE=docker
for arg in "$@"; do
	case "$arg" in
	--docker) FIXTURE_MODE=docker ;;
	--use-existing) FIXTURE_MODE=existing ;;
	-h | --help)
		sed -n '2,/^$/p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
		exit 0
		;;
	*)
		echo "Unknown option: $arg" >&2
		exit 1
		;;
	esac
done

# ---------------------------------------------------------------------------
# Locate `unittest`. CMake lays it down at build/third_party/duckdb/test/unittest.
# Build it if it's not there and we have a configured build dir.
UNITTEST="$WORKSPACE/$BUILD_DIR/third_party/duckdb/test/unittest"
if [[ ! -x "$UNITTEST" ]]; then
	if [[ ! -f "$WORKSPACE/$BUILD_DIR/CMakeCache.txt" ]]; then
		echo "ERROR: $WORKSPACE/$BUILD_DIR is not a configured build directory." >&2
		echo "Run: cmake --preset lldb \\" >&2
		echo "       -DCMAKE_C_COMPILER=clang-21 -DCMAKE_CXX_COMPILER=clang++-21" >&2
		exit 1
	fi
	if grep -q 'SDB_BUILD_DUCKDB_UNITTESTS:.*=OFF' "$WORKSPACE/$BUILD_DIR/CMakeCache.txt"; then
		echo "ERROR: build was configured with -DSDB_BUILD_DUCKDB_UNITTESTS=OFF." >&2
		echo "Drop that flag (or pass =ON) and reconfigure to enable unittest." >&2
		exit 1
	fi
	echo "Building unittest..."
	ninja -C "$WORKSPACE/$BUILD_DIR" unittest
fi
[[ -x "$UNITTEST" ]] || {
	echo "ERROR: still no unittest at $UNITTEST" >&2
	exit 1
}

# ---------------------------------------------------------------------------
# Fixture: postgres in docker, reachable via $PGHOST:$PGPORT, no password
# (trust auth). --use-existing skips this and trusts caller-supplied env.

PG_FIXTURE_KIND=""
PG_DOCKER_PROJECT=""

cleanup_postgres() {
	if [[ "$PG_FIXTURE_KIND" == "docker" && -n "$PG_DOCKER_PROJECT" ]]; then
		docker compose -p "$PG_DOCKER_PROJECT" -f "$SCRIPT_DIR/docker-compose.yml" \
			down --volumes --remove-orphans >/dev/null 2>&1 || true
	fi
}
trap cleanup_postgres EXIT INT TERM

free_port() {
	python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()'
}

start_postgres_docker() {
	if ! command -v docker >/dev/null 2>&1; then
		echo "ERROR: docker not found on PATH." >&2
		exit 1
	fi
	if ! docker ps >/dev/null 2>&1; then
		echo "ERROR: docker daemon not reachable (permission denied or not running)." >&2
		exit 1
	fi
	PG_FIXTURE_KIND=docker
	PG_DOCKER_PROJECT="sdb-pgscan-$$"
	export POSTGRES_HOST_PORT
	POSTGRES_HOST_PORT=$(free_port)
	# Bind-mount the workspace into the container at the same path so tests
	# that run COPY FROM '<host-path>' via postgres_execute() can resolve the
	# files inside the postgres backend.
	export SDB_WORKSPACE_DIR="$WORKSPACE"
	echo "Starting postgres in docker (host port $POSTGRES_HOST_PORT)..."
	docker compose -p "$PG_DOCKER_PROJECT" -f "$SCRIPT_DIR/docker-compose.yml" up -d
	for i in $(seq 1 30); do
		if docker compose -p "$PG_DOCKER_PROJECT" -f "$SCRIPT_DIR/docker-compose.yml" \
			exec -T postgres pg_isready -U postgres >/dev/null 2>&1; then
			break
		fi
		[[ $i -eq 30 ]] && {
			echo "ERROR: postgres docker container failed to become ready" >&2
			exit 1
		}
		sleep 1
	done
	export PGHOST=127.0.0.1 PGPORT="$POSTGRES_HOST_PORT" PGUSER=postgres PGDATABASE=postgres
}

case "$FIXTURE_MODE" in
docker) start_postgres_docker ;;
existing)
	: "${PGHOST:?--use-existing requires PGHOST set}"
	: "${PGPORT:?--use-existing requires PGPORT set}"
	: "${PGUSER:=postgres}"
	: "${PGDATABASE:=postgres}"
	export PGHOST PGPORT PGUSER PGDATABASE
	PG_FIXTURE_KIND=existing
	echo "Using existing postgres at $PGHOST:$PGPORT (user=$PGUSER)."
	;;
esac

# Upstream tests gate on this env var (require-env POSTGRES_TEST_DATABASE_AVAILABLE).
export POSTGRES_TEST_DATABASE_AVAILABLE=1

# The upstream test-config (attach_postgres.json) uses a master database named
# `postgresscanner` to CREATE/DROP per-test databases. Pre-create it; load
# upstream's static fixtures (all_pg_types/decimals/other) which define the
# tables that several `attach_existing_*` and `decimals` tests query. Skip
# everything if it's already there so repeated runs are idempotent.
psql_postgresscanner_exists() {
	PGPASSWORD="" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres \
		-tAc "SELECT 1 FROM pg_database WHERE datname='postgresscanner'" | grep -q 1
}

if ! psql_postgresscanner_exists; then
	echo "Provisioning master database 'postgresscanner' + upstream fixtures..."
	PGPASSWORD="" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres \
		-c "CREATE DATABASE postgresscanner" >/dev/null
	for fixture in all_pg_types.sql decimals.sql other.sql; do
		PGPASSWORD="" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgresscanner \
			-v ON_ERROR_STOP=1 -q -f "$WORKSPACE/third_party/duckdb_postgres/test/$fixture"
	done

	# Synthetic tpch.lineitem (upstream seeds tpch via the duckdb CLI's dbgen,
	# which we don't ship). 10k rows is enough that the 3-way cross-join in
	# attach_timeout_error.test trips its 1s statement_timeout.
	echo "Provisioning synthetic tpch.lineitem fixture..."
	PGPASSWORD="" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgresscanner \
		-v ON_ERROR_STOP=1 -q <<-'SQL'
			CREATE SCHEMA IF NOT EXISTS tpch;
			CREATE TABLE tpch.lineitem AS
			  SELECT i AS l_orderkey FROM generate_series(1, 10000) AS i;
		SQL
else
	echo "Master database 'postgresscanner' already provisioned, skipping."
fi

# ---------------------------------------------------------------------------
# Run unittest. --test-dir points it at the extension's source root so its
# `test/` scan finds the .test files. We pass our own test-config.json, NOT
# upstream's attach_postgres.json -- see the README for why.
mkdir -p "$REPORTS_DIR"
JUNIT_XML="$REPORTS_DIR/postgres_scanner.xml"

echo "Running postgres_scanner tests..."
echo "  unittest:    $UNITTEST"
echo "  fixture:     $PG_FIXTURE_KIND ($PGHOST:$PGPORT)"
echo "  junit:       $JUNIT_XML"
echo

"$UNITTEST" \
	--test-dir "$WORKSPACE/third_party/duckdb_postgres" \
	--test-config "$SCRIPT_DIR/test-config.json" \
	-r junit -o "$JUNIT_XML"
