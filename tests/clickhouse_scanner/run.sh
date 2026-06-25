#!/bin/bash
#
# Driver for the vendored duckdb_clickhouse extension's upstream test suite
# (third_party/duckdb_clickhouse/test/sql/**/*.test).
#
# Brings up a clickhouse-server fixture in docker, then runs DuckDB's `unittest`
# binary against the upstream `.test` files.
#
# Requires `unittest` to be built. It is by default; the driver will run
# `ninja unittest` if the binary isn't present and the cmake flag isn't
# explicitly OFF.
#
# Usage:
#   tests/clickhouse_scanner/run.sh                  # docker fixture (default)
#   tests/clickhouse_scanner/run.sh --use-existing   # CI: clickhouse already up,
#                                                    #     CHHOST/CHPORT supplied
#
# Env:
#   BUILD_DIR    (default: build)
#   REPORTS_DIR  (default: tests/sqllogic)       -- where JUnit XML lands
#   CHHOST/CHPORT/CHUSER -- required when --use-existing

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
# Fixture: clickhouse-server in docker, reachable via $CHHOST:$CHPORT. The image
# runs /docker-entrypoint-initdb.d/*.sql at startup, so fixtures/01_init.sql is
# pre-seeded. --use-existing skips this and trusts caller-supplied env.

CH_FIXTURE_KIND=""
CH_DOCKER_PROJECT=""

cleanup_clickhouse() {
	if [[ "$CH_FIXTURE_KIND" == "docker" && -n "$CH_DOCKER_PROJECT" ]]; then
		docker compose -p "$CH_DOCKER_PROJECT" -f "$SCRIPT_DIR/docker-compose.yml" \
			down --volumes --remove-orphans >/dev/null 2>&1 || true
	fi
}
trap cleanup_clickhouse EXIT INT TERM

free_port() {
	python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()'
}

start_clickhouse_docker() {
	if ! command -v docker >/dev/null 2>&1; then
		echo "ERROR: docker not found on PATH." >&2
		exit 1
	fi
	if ! docker ps >/dev/null 2>&1; then
		echo "ERROR: docker daemon not reachable (permission denied or not running)." >&2
		exit 1
	fi
	CH_FIXTURE_KIND=docker
	CH_DOCKER_PROJECT="sdb-chscan-$$"
	export CLICKHOUSE_HOST_PORT
	CLICKHOUSE_HOST_PORT=$(free_port)
	echo "Starting clickhouse in docker (host port $CLICKHOUSE_HOST_PORT)..."
	docker compose -p "$CH_DOCKER_PROJECT" -f "$SCRIPT_DIR/docker-compose.yml" up -d
	for i in $(seq 1 60); do
		if docker compose -p "$CH_DOCKER_PROJECT" -f "$SCRIPT_DIR/docker-compose.yml" \
			exec -T clickhouse clickhouse-client --query "SELECT 1" >/dev/null 2>&1; then
			break
		fi
		[[ $i -eq 60 ]] && {
			echo "ERROR: clickhouse docker container failed to become ready" >&2
			exit 1
		}
		sleep 1
	done
	export CHHOST=127.0.0.1 CHPORT="$CLICKHOUSE_HOST_PORT" CHUSER=default
}

case "$FIXTURE_MODE" in
docker) start_clickhouse_docker ;;
existing)
	: "${CHHOST:?--use-existing requires CHHOST set}"
	: "${CHPORT:?--use-existing requires CHPORT set}"
	: "${CHUSER:=default}"
	export CHHOST CHPORT CHUSER
	CH_FIXTURE_KIND=existing
	echo "Using existing clickhouse at $CHHOST:$CHPORT (user=$CHUSER)."
	;;
esac

# The clickhouse fixtures are pre-seeded by the container init script, so there
# is nothing to provision here. For the docker fixture, wait for the chtest
# tables to materialize before handing off to unittest.
if [[ "$CH_FIXTURE_KIND" == "docker" ]]; then
	echo "Waiting for clickhouse fixtures to load..."
	for i in $(seq 1 60); do
		table_count=$(docker compose -p "$CH_DOCKER_PROJECT" -f "$SCRIPT_DIR/docker-compose.yml" \
			exec -T clickhouse clickhouse-client \
			--query "SELECT count() FROM system.tables WHERE database='chtest'" 2>/dev/null || echo 0)
		if [[ "$table_count" =~ ^[0-9]+$ ]] && [[ "$table_count" -gt 0 ]]; then
			echo "clickhouse fixtures loaded ($table_count tables)."
			break
		fi
		[[ $i -eq 60 ]] && {
			echo "ERROR: clickhouse fixtures failed to load within 60 seconds" >&2
			exit 1
		}
		sleep 1
	done
fi

# ---------------------------------------------------------------------------
# Run unittest. --test-dir points it at the extension's source root so its
# `test/` scan finds the .test files. The .test files reference the fixture
# through ${CHHOST}/${CHPORT} (gated by require-env), so export them.
mkdir -p "$REPORTS_DIR"
JUNIT_XML="$REPORTS_DIR/clickhouse_scanner.xml"

echo "Running clickhouse_scanner tests..."
echo "  unittest:    $UNITTEST"
echo "  fixture:     $CH_FIXTURE_KIND ($CHHOST:$CHPORT)"
echo "  junit:       $JUNIT_XML"
echo

"$UNITTEST" \
	--test-dir "$WORKSPACE/third_party/duckdb_clickhouse" \
	--test-config "$SCRIPT_DIR/test-config.json" \
	-r junit -o "$JUNIT_XML"
