#!/bin/bash
#
# Single entry point for every DuckDB-level test suite: DuckDB core's own test
# tree and each vendored extension's, run through DuckDB's `unittest` binary
# against the same build that statically links those extensions.
#
# Every suite runs in ONE unittest process: the binary registers DuckDB core's
# test tree plus each statically linked extension's (via LOAD_TESTS/TEST_DIR in
# .github/config/extensions/<ext>.cmake), so selecting suites is purely a matter
# of which name filters we pass. Filters are also what makes `.test_slow` run at
# all -- those files are registered with Catch2's hidden `[.]` tag, which the
# default (unfiltered) test set excludes.
#
# Each suite has a checked-in test-config (config/<suite>.json) listing the tests
# we skip and why. Those are SereneDB divergences from upstream DuckDB, not
# flakes: every other test is a regression gate on the fork. All in-scope configs
# are passed at once -- their skip lists merge into one set, and extension paths
# still match because ShouldSkipTest() strips absolute names back to test/sql...
#
# postgres_scanner is the one suite needing a live postgres; when it is in scope
# the fixture comes up first -- an existing server when PGHOST is set (CI),
# otherwise a docker postgres on a free port.
#
# Usage:
#   tests/duckdb/run.sh                     # every suite
#   tests/duckdb/run.sh --suite core,inet   # a subset
#   tests/duckdb/run.sh --list              # show suite names
#
# Env:
#   BUILD_DIR    (default: build)
#   REPORTS_DIR  (default: <workspace>/out/test-results)  -- where JUnit XML lands
#   PGHOST/PGPORT/PGUSER/PGDATABASE -- postgres_scanner uses an existing server
#                                      when PGHOST is set

set -uo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
WORKSPACE=$(cd "$SCRIPT_DIR/../.." && pwd)

: "${BUILD_DIR:=build}"
: "${REPORTS_DIR:=$WORKSPACE/out/test-results}"

# suite name -> vendored source root whose test/ tree we run.
declare -A SUITE_DIR=(
	[core]="$WORKSPACE/third_party/duckdb"
	[avro]="$WORKSPACE/third_party/duckdb_avro"
	[azure]="$WORKSPACE/third_party/duckdb_azure"
	[httpfs]="$WORKSPACE/third_party/duckdb_httpfs"
	[iceberg]="$WORKSPACE/third_party/duckdb_iceberg"
	[inet]="$WORKSPACE/third_party/duckdb_inet"
	[postgres_scanner]="$WORKSPACE/third_party/duckdb_postgres"
)
SUITE_ORDER=(core avro azure httpfs iceberg inet postgres_scanner)

# suite name -> Catch2 name filter. Core's tests register relative to --test-dir
# (so "test/..."), while extension tests come from LoadedExtensionTestPaths() and
# register under their absolute path.
suite_filter() {
	if [[ "$1" == "core" ]]; then
		echo 'test/*'
	else
		echo "${SUITE_DIR[$1]}/test/*"
	fi
}

SUITES="${SUITE_ORDER[*]}"
# An empty --suite is rejected rather than treated as "every suite": CI passes the
# diff-derived list through, and a variable that lost its value should fail loudly
# instead of quietly running all 800-odd slow tests.
require_suites() {
	if [[ -z "${1// /}" ]]; then
		echo "--suite was given an empty list" >&2
		exit 2
	fi
}
while [ $# -gt 0 ]; do
	case "$1" in
	--suite)
		SUITES="${2//,/ }"
		require_suites "$SUITES"
		shift 2
		;;
	--suite=*)
		SUITES="${1#*=}"
		SUITES="${SUITES//,/ }"
		require_suites "$SUITES"
		shift
		;;
	--list)
		printf '%s\n' "${SUITE_ORDER[@]}"
		exit 0
		;;
	-h | --help)
		sed -n '2,/^$/p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
		exit 0
		;;
	*)
		echo "Unknown option: $1" >&2
		exit 2
		;;
	esac
done

UNITTEST="$WORKSPACE/$BUILD_DIR/third_party/duckdb/test/unittest"
if [[ ! -x "$UNITTEST" ]]; then
	if [[ ! -f "$WORKSPACE/$BUILD_DIR/CMakeCache.txt" ]]; then
		echo "ERROR: $WORKSPACE/$BUILD_DIR is not a configured build directory." >&2
		exit 1
	fi
	if grep -q 'SDB_BUILD_DUCKDB_UNITTESTS:.*=OFF' "$WORKSPACE/$BUILD_DIR/CMakeCache.txt"; then
		echo "ERROR: build was configured with -DSDB_BUILD_DUCKDB_UNITTESTS=OFF." >&2
		exit 1
	fi
	echo "Building unittest..."
	ninja -C "$WORKSPACE/$BUILD_DIR" unittest || exit 1
fi

mkdir -p "$REPORTS_DIR"

# --- postgres fixture, for the postgres_scanner suite only -------------------
PG_DOCKER_PROJECT=""

cleanup_postgres() {
	if [[ -n "$PG_DOCKER_PROJECT" ]]; then
		docker compose -p "$PG_DOCKER_PROJECT" -f "$SCRIPT_DIR/docker-compose.postgres.yml" \
			down --volumes --remove-orphans >/dev/null 2>&1 || true
	fi
}
trap cleanup_postgres EXIT INT TERM

start_postgres_docker() {
	if ! docker ps >/dev/null 2>&1; then
		echo "ERROR: docker daemon not reachable, and PGHOST is not set." >&2
		return 1
	fi
	PG_DOCKER_PROJECT="sdb-pgscan-$$"
	export POSTGRES_HOST_PORT
	POSTGRES_HOST_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()')
	# Same path inside the container as outside, so tests that COPY FROM a
	# host path via postgres_execute() resolve it in the postgres backend.
	export SDB_WORKSPACE_DIR="$WORKSPACE"
	echo "Starting postgres in docker (host port $POSTGRES_HOST_PORT)..."
	docker compose -p "$PG_DOCKER_PROJECT" -f "$SCRIPT_DIR/docker-compose.postgres.yml" up -d || return 1
	for i in $(seq 1 30); do
		if docker compose -p "$PG_DOCKER_PROJECT" -f "$SCRIPT_DIR/docker-compose.postgres.yml" \
			exec -T postgres pg_isready -U postgres >/dev/null 2>&1; then
			break
		fi
		[[ $i -eq 30 ]] && {
			echo "ERROR: postgres container never became ready" >&2
			return 1
		}
		sleep 1
	done
	export PGHOST=127.0.0.1 PGPORT="$POSTGRES_HOST_PORT" PGUSER=postgres PGDATABASE=postgres
}

# The upstream test-config creates and drops per-test databases from a master
# database named `postgresscanner`, and several attach_existing_* / decimals
# tests query fixtures that must already be in it. Idempotent: repeated runs
# against the same server skip straight past.
provision_postgres() {
	if PGPASSWORD="" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres \
		-tAc "SELECT 1 FROM pg_database WHERE datname='postgresscanner'" | grep -q 1; then
		echo "Master database 'postgresscanner' already provisioned, skipping."
		return 0
	fi
	echo "Provisioning master database 'postgresscanner' + upstream fixtures..."
	PGPASSWORD="" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgres \
		-c "CREATE DATABASE postgresscanner" >/dev/null || return 1
	for fixture in all_pg_types.sql decimals.sql other.sql; do
		PGPASSWORD="" psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d postgresscanner \
			-v ON_ERROR_STOP=1 -q -f "$WORKSPACE/third_party/duckdb_postgres/test/$fixture" || return 1
	done
	# Upstream seeds tpch through the duckdb CLI's dbgen, which we don't ship. The
	# unittest binary links the same tpch extension, so generate the dataset there
	# and push it into postgres over the scanner itself.
	echo "Provisioning tpch fixture (sf=0.01) via dbgen..."
	"$UNITTEST" --stdin <"$SCRIPT_DIR/provision_tpch.test" || return 1
}

ensure_postgres_fixture() {
	if [[ -n "${PGHOST:-}" ]]; then
		: "${PGPORT:=5432}" "${PGUSER:=postgres}" "${PGDATABASE:=postgres}"
		export PGHOST PGPORT PGUSER PGDATABASE
		echo "Using existing postgres at $PGHOST:$PGPORT (user=$PGUSER)."
	else
		start_postgres_docker || return 1
	fi
	provision_postgres || return 1
	# Upstream tests gate on this: require-env POSTGRES_TEST_DATABASE_AVAILABLE.
	export POSTGRES_TEST_DATABASE_AVAILABLE=1
}
# -----------------------------------------------------------------------------

for suite in $SUITES; do
	if [[ -z "${SUITE_DIR[$suite]:-}" ]]; then
		echo "Unknown suite '$suite' (see --list)" >&2
		exit 2
	fi
done

log="$REPORTS_DIR/duckdb.log"
args=(--test-dir "${SUITE_DIR[core]}")
filters=()
for suite in $SUITES; do
	config="$SCRIPT_DIR/config/$suite.json"
	[[ -f "$config" ]] && args+=(--test-config "$config")
	filters+=("$(suite_filter "$suite")")
done

# One spec, comma-separated: this binary hands the leftover argv to Catch2 as a
# single test spec, so separate arguments would be concatenated into one bogus
# pattern that matches nothing.
spec="$(
	IFS=,
	echo "${filters[*]}"
)"

echo
echo "===== [duckdb] BEGIN ====="
echo "  suites:   ${SUITES// /, }"
echo "  test-dir: ${SUITE_DIR[core]}"
echo "  filter:   $spec"
echo "  log:      $log"

if [[ " $SUITES " == *" postgres_scanner "* ]] && ! ensure_postgres_fixture; then
	echo "===== [duckdb] END (rc=1, postgres fixture failed) ====="
	exit 1
fi

# No --test-temp-dir here, on purpose: that flag also flips DeleteTestPath
# off, which turns the per-test ClearTestDirectory() into a no-op. Persistent
# `load {TEST_DIR}/x.db` tests then inherit the previous test's database and
# fail with "Table with name ... already exists". The default scratch dir is
# duckdb_unittest_tempdir/<pid>/ under the test-dir, which every vendored
# repo gitignores.
# Console reporter, not `-r junit`: Catch2 v2 allows exactly one reporter, and
# the junit one both suppresses the per-failure detail that makes this log
# worth reading and counts every skipped test as a failure. Nothing in CI
# parses the XML, so the log is the artifact.
"$UNITTEST" "${args[@]}" "$spec" >"$log" 2>&1
rc=$?

# A spec that matches nothing exits 0, which would turn a typo'd filter (or an
# extension whose tests stopped being registered) into a silent pass.
if grep -qE '^No tests ran|No test cases matched' "$log"; then
	echo "ERROR: the filter matched no tests -- suites=${SUITES// /,}" >&2
	rc=1
fi

cat "$log"
echo "===== [duckdb] END (rc=$rc) ====="

echo
echo "===== [duckdb] SUMMARY ====="
# Catch prints one of three shapes: "test cases: N | ..." when anything failed
# or was skipped, "All tests passed (...)" when fully clean, and "All tests
# were skipped (...)" when every test was gated behind a require-env.
printf '  %-10s %s\n' "$([[ $rc -eq 0 ]] && echo PASS || echo FAIL)" \
	"$(grep -m1 -E '^test cases:|^All tests (passed|were skipped)' "$log" ||
		echo 'no summary -- run did not reach the end')"
# Failures carry the test path, so attribute them back to the suite that owns it.
if [[ $rc -ne 0 ]]; then
	for suite in $SUITES; do
		pat="${SUITE_DIR[$suite]}/test/"
		[[ "$suite" == "core" ]] && pat="^test/"
		n=$(grep -cE "^[0-9]+\. .*${pat//\//\\/}" "$log" 2>/dev/null || true)
		[[ "${n:-0}" -gt 0 ]] && printf '  %-10s %s failing test(s)\n' "$suite" "$n"
	done
fi

final_exit=$rc

exit $final_exit
