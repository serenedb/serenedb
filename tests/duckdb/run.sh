#!/bin/bash
#
# Single entry point for every DuckDB-level test suite: DuckDB core's own test
# tree and each vendored extension's, run through DuckDB's `unittest` binary
# against the same build that statically links those extensions.
#
# The binary registers DuckDB core's test tree plus each statically linked
# extension's (via LOAD_TESTS/TEST_DIR in .github/config/extensions/<ext>.cmake),
# so selecting suites is purely a matter of which name filters we pass. Filters
# are also what makes `.test_slow` run at all -- those files are registered with
# Catch2's hidden `[.]` tag, which the default (unfiltered) test set excludes.
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
#   SDB_DUCKDB_MAX_THREADS  (default: min(nproc, 16)) -- caps DuckDB's default
#     thread count, via the SLURM_CPUS_ON_NODE lever GetSystemMaxThreads()
#     honours on Linux. The memory-limit tests set a fixed budget (100MB-1GB)
#     but the minimum footprint scales per thread, so on a many-core box they
#     OOM before they can spill. Set to empty to use the real core count.

set -uo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
WORKSPACE=$(cd "$SCRIPT_DIR/../.." && pwd)

: "${BUILD_DIR:=build}"
: "${REPORTS_DIR:=$WORKSPACE/out/test-results}"
: "${DUCKDB_JOBS:=$(nproc 2>/dev/null || echo 4)}"
: "${SDB_DUCKDB_MAX_THREADS:=$(($(nproc) < 16 ? $(nproc) : 16))}"

if [[ -n "$SDB_DUCKDB_MAX_THREADS" ]] && [[ -z "${SLURM_CPUS_ON_NODE:-}" ]]; then
	export SLURM_CPUS_ON_NODE="$SDB_DUCKDB_MAX_THREADS"
fi

# suite name -> vendored source root whose test/ tree we run.
declare -A SUITE_DIR=(
	[core]="$WORKSPACE/third_party/duckdb"
	[cpp]="$WORKSPACE/third_party/duckdb"
	[avro]="$WORKSPACE/third_party/duckdb_avro"
	[azure]="$WORKSPACE/third_party/duckdb_azure"
	[httpfs]="$WORKSPACE/third_party/duckdb_httpfs"
	[iceberg]="$WORKSPACE/third_party/duckdb_iceberg"
	[inet]="$WORKSPACE/third_party/duckdb_inet"
	[markdown]="$WORKSPACE/third_party/duckdb_markdown"
	[postgres_scanner]="$WORKSPACE/third_party/duckdb_postgres"
	[spatial]="$WORKSPACE/third_party/duckdb_spatial"
	[interop]="$SCRIPT_DIR/interop"
)
SUITE_ORDER=(core cpp avro azure httpfs iceberg inet markdown postgres_scanner spatial interop)

# suite name -> Catch2 name filter. Core's tests register relative to --test-dir
# (so "test/..."), while extension tests come from LoadedExtensionTestPaths() and
# register under their absolute path. The C++ tests register under their own
# names, so cpp is everything that is not a sqllogic file.
suite_filter() {
	if [[ "$1" == "core" ]]; then
		echo 'test/*'
	elif [[ "$1" == "cpp" ]]; then
		echo '~"*.test" ~"*.test_slow" ~"*.test_coverage",[.] ~"*.test" ~"*.test_slow" ~"*.test_coverage"'
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
	--jobs)
		DUCKDB_JOBS="$2"
		shift 2
		;;
	--jobs=*)
		DUCKDB_JOBS="${1#*=}"
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

run_interop=false
unittest_suites=""
for suite in $SUITES; do
	if [[ "$suite" == interop ]]; then
		run_interop=true
	else
		unittest_suites="$unittest_suites $suite"
	fi
done

UNITTEST="$WORKSPACE/$BUILD_DIR/third_party/duckdb/test/unittest"
if [[ -n "${unittest_suites// /}" && ! -x "$UNITTEST" ]]; then
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

ICEBERG_DIR="${SUITE_DIR[iceberg]}"
ICEBERG_COMPOSE="$ICEBERG_DIR/scripts/docker-compose.yml"
ICEBERG_REST_URI=http://127.0.0.1:8181
ICEBERG_DOCKER_STARTED=false
S3_HOST=duckdb-minio.com
S3_DOCKER_PROJECT=""
HTTP_SERVER_PID=""
AZURITE_CONTAINER=""
AZURITE_CONNECTION_STRING="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
PROXY_PIDS=()

cleanup_fixtures() {
	if [[ -n "$AZURITE_CONTAINER" ]]; then
		docker rm -f "$AZURITE_CONTAINER" >/dev/null 2>&1 || true
	fi
	if [[ ${#PROXY_PIDS[@]} -gt 0 ]]; then
		kill "${PROXY_PIDS[@]}" 2>/dev/null || true
	fi
	rm -f "$ICEBERG_DIR/mitmproxy.log"
	if [[ -n "$S3_DOCKER_PROJECT" ]]; then
		docker compose -p "$S3_DOCKER_PROJECT" -f "$SCRIPT_DIR/docker-compose.s3.yml" \
			down --volumes --remove-orphans >/dev/null 2>&1 || true
	fi
	if [[ -n "$HTTP_SERVER_PID" ]]; then
		kill "$HTTP_SERVER_PID" 2>/dev/null || true
	fi
	if [[ -n "$PG_DOCKER_PROJECT" ]]; then
		docker compose -p "$PG_DOCKER_PROJECT" -f "$SCRIPT_DIR/docker-compose.postgres.yml" \
			down --volumes --remove-orphans >/dev/null 2>&1 || true
	fi
	if [[ "$ICEBERG_DOCKER_STARTED" == true ]]; then
		docker compose -f "$ICEBERG_COMPOSE" down --volumes --remove-orphans >/dev/null 2>&1 || true
	fi
}
trap cleanup_fixtures EXIT INT TERM

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
	if grep -qE '^SDB_SANITIZE:STRING=.+' "$WORKSPACE/$BUILD_DIR/CMakeCache.txt"; then
		export SANITIZER_BUILD=1
	fi
}

start_iceberg_docker() {
	if ! docker ps >/dev/null 2>&1; then
		echo "ERROR: docker daemon not reachable, and no Iceberg REST fixture answers at $ICEBERG_REST_URI." >&2
		return 1
	fi
	echo "Starting the Iceberg REST fixture in docker..."
	docker compose -f "$ICEBERG_COMPOSE" up -d || return 1
	ICEBERG_DOCKER_STARTED=true
	wait_iceberg_fixture
}

wait_iceberg_fixture() {
	for i in $(seq 1 60); do
		curl -fsS -o /dev/null "$ICEBERG_REST_URI/v1/config" 2>/dev/null && return 0
		sleep 1
	done
	echo "ERROR: the Iceberg REST fixture at $ICEBERG_REST_URI never became ready" >&2
	return 1
}

generate_iceberg_data() {
	local generator=("$SCRIPT_DIR/generate_iceberg_data.sh" fixture local)
	echo "Generating Iceberg test data, log: $REPORTS_DIR/iceberg-data.log"
	if ! python3 -c 'import pyspark' 2>/dev/null; then
		generator=(docker run --rm --network host -u "$(id -u):$(id -g)" -e HOME=/tmp
		-v "$WORKSPACE:$WORKSPACE" "${BUILD_IMAGE:-serenedb/serenedb-build-ubuntu:latest}" "${generator[@]}")
	fi
	"${generator[@]}" >"$REPORTS_DIR/iceberg-data.log" 2>&1 || {
		echo "ERROR: Iceberg data generation failed, see $REPORTS_DIR/iceberg-data.log" >&2
		return 1
	}
}

ensure_iceberg_fixture() {
	if [[ -n "${ICEBERG_FIXTURE_RUNNING:-}" ]] || curl -fsS -o /dev/null "$ICEBERG_REST_URI/v1/config" 2>/dev/null; then
		echo "Using the Iceberg REST fixture at $ICEBERG_REST_URI."
		wait_iceberg_fixture || return 1
	else
		start_iceberg_docker || return 1
	fi
	generate_iceberg_data || return 1
	export FIXTURE_SERVER_AVAILABLE=1 DUCKDB_ICEBERG_HAVE_GENERATED_DATA=1
	start_mitmproxies
}

start_mitmproxy() {
	local port=$1 log=$2
	mitmdump --mode "regular@$port" --flow-detail 2 --set confdir="$REPORTS_DIR/mitmproxy" "${@:3}" >"$log" 2>&1 &
	PROXY_PIDS+=($!)
	for i in $(seq 1 60); do
		(exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null && return 0
		sleep 0.5
	done
	echo "ERROR: mitmdump on port $port never became ready, see $log" >&2
	return 1
}

start_mitmproxies() {
	if ! command -v mitmdump >/dev/null; then
		echo "Skipping the Iceberg proxy tests: mitmproxy is not installed (the build image has it)."
		return 0
	fi
	start_mitmproxy 8878 "$ICEBERG_DIR/mitmproxy.log" || return 1
	start_mitmproxy 19133 "$REPORTS_DIR/vended-credentials-refresh-proxy.log" \
		-s "$ICEBERG_DIR/scripts/vended_credentials_refresh_proxy.py" || return 1
	ICEBERG_HTTP_PROXY=localhost:8878
	export VENDED_CREDENTIAL_REFRESH_PROXY=http://127.0.0.1:19133
}

generate_s3_data() {
	local dir=$1 script="$1/generate.test"
	{
		echo "require tpch"
		echo
		echo "require parquet"
		echo
		echo "statement ok"
		echo "CALL dbgen(sf=1);"
		echo
		echo "statement ok"
		echo "COPY lineitem TO '$dir/presigned-url-lineitem.parquet' (FORMAT parquet);"
		echo
		echo "statement ok"
		echo "ATTACH '$dir/lineitem_sf1.db' AS sf1;"
		echo
		echo "statement ok"
		echo "COPY FROM DATABASE memory TO sf1;"
		echo
		echo "statement ok"
		echo "DETACH sf1;"
		echo
		echo "statement ok"
		echo "ATTACH '$dir/attach.db' AS db (STORAGE_VERSION 'latest');"
		echo
		echo "statement ok"
		echo "USE db;"
		echo
		echo "statement ok"
		grep -v '^[[:space:]]*$' "${SUITE_DIR[core]}/test/sql/storage_version/generate_storage_version.sql"
		echo
	} >"$script"
	"$UNITTEST" --stdin <"$script"
}

provision_s3() {
	local provision=(python3 "$SCRIPT_DIR/provision_s3.py" "http://$S3_HOST:9000"
		"${SUITE_DIR[core]}/data" "$1")
	if ! python3 -c 'import boto3' 2>/dev/null; then
		provision=(docker run --rm --network host -u "$(id -u):$(id -g)" -e HOME=/tmp
		--add-host "$S3_HOST:$(getent hosts "$S3_HOST" | awk '{print $1}')"
		-v "$WORKSPACE:$WORKSPACE" -v "$1:$1" "${BUILD_IMAGE:-serenedb/serenedb-build-ubuntu:latest}"
		"${provision[@]}")
	fi
	"${provision[@]}"
}

ensure_s3_fixture() {
	if [[ -z "${S3_FIXTURE_RUNNING:-}" ]]; then
		if ! getent hosts "test-bucket.$S3_HOST" >/dev/null; then
			echo "Skipping the httpfs S3 tests: $S3_HOST and its bucket hostnames do not resolve (see tests/duckdb/README.md)."
			return 0
		fi
		S3_DOCKER_PROJECT="sdb-s3-$$"
		export S3_HOST_IP
		S3_HOST_IP=$(getent hosts "$S3_HOST" | awk '{print $1}')
		echo "Starting the S3 test server in docker ($S3_HOST_IP:9000)..."
		docker compose -p "$S3_DOCKER_PROJECT" -f "$SCRIPT_DIR/docker-compose.s3.yml" up -d || return 1
	fi
	for i in $(seq 1 60); do
		curl -s -o /dev/null "http://$S3_HOST:9000" && break
		[[ $i -eq 60 ]] && {
			echo "ERROR: the S3 test server at $S3_HOST:9000 never became ready" >&2
			return 1
		}
		sleep 1
	done
	local data="$REPORTS_DIR/s3-data" urls
	rm -rf "$data"
	mkdir -p "$data"
	echo "Generating and uploading the S3 test data, log: $REPORTS_DIR/s3-data.log"
	generate_s3_data "$data" >"$REPORTS_DIR/s3-data.log" 2>&1 &&
		urls=$(provision_s3 "$data" 2>>"$REPORTS_DIR/s3-data.log") || {
		echo "ERROR: S3 test data provisioning failed, see $REPORTS_DIR/s3-data.log" >&2
		return 1
	}
	while IFS='=' read -r name url; do
		export "$name=$url"
	done <<<"$urls"
	export S3_TEST_SERVER_AVAILABLE=1 AWS_DEFAULT_REGION=eu-west-1 \
		AWS_ACCESS_KEY_ID=minio_duckdb_user AWS_SECRET_ACCESS_KEY=minio_duckdb_user_password \
		DUCKDB_S3_ENDPOINT="$S3_HOST:9000" DUCKDB_S3_USE_SSL=false \
		S3_ATTACH_DB=s3://test-bucket/presigned/attach.db
}

ensure_azurite() {
	if [[ -z "${AZURITE_RUNNING:-}" ]] && ! curl -s -o /dev/null http://127.0.0.1:10000; then
		AZURITE_CONTAINER="sdb-azurite-$$"
		echo "Starting Azurite in docker (127.0.0.1:10000)..."
		docker run -d --name "$AZURITE_CONTAINER" -p 127.0.0.1:10000:10000 \
			mcr.microsoft.com/azure-storage/azurite:3.37.0 \
			azurite-blob --blobHost 0.0.0.0 --skipApiVersionCheck >/dev/null || return 1
	fi
	for i in $(seq 1 60); do
		curl -s -o /dev/null http://127.0.0.1:10000 && break
		[[ $i -eq 60 ]] && {
			echo "ERROR: Azurite never became ready" >&2
			return 1
		}
		sleep 1
	done
	local provision=(python3 "$SCRIPT_DIR/provision_azurite.py" "$AZURITE_CONNECTION_STRING" "${SUITE_DIR[azure]}/data")
	if ! python3 -c 'import azure.storage.blob' 2>/dev/null; then
		provision=(docker run --rm --network host -u "$(id -u):$(id -g)" -e HOME=/tmp
		-v "$WORKSPACE:$WORKSPACE" "${BUILD_IMAGE:-serenedb/serenedb-build-ubuntu:latest}" "${provision[@]}")
	fi
	"${provision[@]}" >"$REPORTS_DIR/azurite-data.log" 2>&1 || {
		echo "ERROR: uploading the Azurite test data failed, see $REPORTS_DIR/azurite-data.log" >&2
		return 1
	}
	export AZURE_STORAGE_CONNECTION_STRING="$AZURITE_CONNECTION_STRING" AZURE_STORAGE_ACCOUNT=devstoreaccount1 \
		AZ_STORAGE_ACCOUNT=devstoreaccount1 AZ_DATA_DIR=testing-private AZ_TEMP_DIR="writes/run-$$"
}

start_squid() {
	local dir="$REPORTS_DIR/squid-$1"
	rm -rf "$dir"
	mkdir -p "$dir"
	(cd "$dir" && exec "${SUITE_DIR[azure]}/scripts/run_squid.sh" --port "$1" --log_dir logs "${@:2}") \
		>"$dir/run.log" 2>&1 &
	PROXY_PIDS+=($!)
	for i in $(seq 1 30); do
		(exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null && return 0
		sleep 0.5
	done
	echo "ERROR: squid on port $1 never became ready, see $dir/run.log" >&2
	return 1
}

start_proxies() {
	if ! command -v squid >/dev/null; then
		echo "Skipping the HTTP proxy tests: squid is not installed (the build image has it)."
		return 0
	fi
	start_squid 3128 || return 1
	start_squid 3129 --auth || return 1
	export HTTP_PROXY_RUNNING=1 HTTP_PROXY_PUBLIC=localhost:3128 \
		HTTP_PROXY_PRIVATE=localhost:3129 HTTP_PROXY_PRIVATE_USERNAME=john HTTP_PROXY_PRIVATE_PASSWORD=doe
}

start_http_server() {
	export PYTHON_HTTP_SERVER_DIR="$REPORTS_DIR/http-server"
	rm -rf "$PYTHON_HTTP_SERVER_DIR"
	mkdir -p "$PYTHON_HTTP_SERVER_DIR"
	local port
	port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()')
	python3 -m http.server --bind 127.0.0.1 --directory "$PYTHON_HTTP_SERVER_DIR" "$port" >/dev/null 2>&1 &
	HTTP_SERVER_PID=$!
	export PYTHON_HTTP_SERVER_URL="http://127.0.0.1:$port"
	for i in $(seq 1 30); do
		curl -s -o /dev/null "$PYTHON_HTTP_SERVER_URL" && return 0
		sleep 0.2
	done
	echo "ERROR: the python HTTP server never became ready" >&2
	return 1
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
serial_filters=()
for suite in $unittest_suites; do
	config="$SCRIPT_DIR/config/$suite.json"
	[[ -f "$config" ]] && args+=(--test-config "$config")
	if [[ "$suite" == "postgres_scanner" ]]; then
		serial_filters+=("$(suite_filter "$suite")")
	elif [[ "$suite" == "iceberg" ]]; then
		filters+=("\"$(suite_filter "$suite")\" ~\"$ICEBERG_DIR/test/sql/local/catalog_*\"")
	elif [[ "$suite" == "httpfs" ]]; then
		mapfile -t s3_tests < <(grep -rl --include='*.test' --include='*.test_slow' \
			'^require-env S3_TEST_SERVER_AVAILABLE' "${SUITE_DIR[httpfs]}/test" | sort)
		mapfile -t httpfs_skips < <(python3 -c 'import json, sys
for group in json.load(open(sys.argv[1]))["skip_tests"]:
    print("\n".join(group["paths"]))' "$config")
		httpfs_filter="\"$(suite_filter "$suite")\""
		for test in "${s3_tests[@]}"; do
			[[ " ${httpfs_skips[*]} " == *" ${test#"${SUITE_DIR[httpfs]}"/} "* ]] && continue
			httpfs_filter+=" ~\"$test\""
			serial_filters+=("\"$test\"")
		done
		filters+=("$httpfs_filter")
	else
		filters+=("$(suite_filter "$suite")")
	fi
done

# One spec, comma-separated: this binary hands the leftover argv to Catch2 as a
# single test spec, so separate arguments would be concatenated into one bogus
# pattern that matches nothing.
spec="$(
	IFS=,
	echo "${filters[*]}"
)"
serial_spec="$(
	IFS=,
	echo "${serial_filters[*]}"
)"

echo
echo "===== [duckdb] BEGIN ====="
echo "  suites:   ${SUITES// /, }"
echo "  test-dir: ${SUITE_DIR[core]}"
echo "  filter:   $spec"
echo "  serial:   $serial_spec"
echo "  jobs:     $DUCKDB_JOBS"
echo "  log:      $log"

# The scratch dir is duckdb_unittest_tempdir/<pid>/ under each vendored repo,
# and ClearTestDirectory() only clears the current pid's subdir. A self-hosted
# runner keeps the workspace between runs and a fresh container hands the binary
# the same low pid, so last run's files get adopted -- an ATTACH'd db survives
# and its test fails with "Table with name ... already exists". Start clean.
for suite in "${SUITE_ORDER[@]}"; do
	rm -rf "${SUITE_DIR[$suite]:?}/duckdb_unittest_tempdir"
done

if [[ " $SUITES " == *" postgres_scanner "* ]] && ! ensure_postgres_fixture; then
	echo "===== [duckdb] END (rc=1, postgres fixture failed) ====="
	exit 1
fi

if [[ " $SUITES " == *" httpfs "* ]]; then
	if ! ensure_s3_fixture || ! start_http_server; then
		echo "===== [duckdb] END (rc=1, httpfs fixture failed) ====="
		exit 1
	fi
fi
if [[ " $SUITES " == *" azure "* ]] && ! ensure_azurite; then
	echo "===== [duckdb] END (rc=1, azurite fixture failed) ====="
	exit 1
fi
if [[ " $SUITES " =~ " "(azure|httpfs|iceberg)" " ]] && ! start_proxies; then
	echo "===== [duckdb] END (rc=1, proxy fixture failed) ====="
	exit 1
fi
if [[ " $SUITES " == *" httpfs "* || " $SUITES " == *" core "* ]]; then
	chmod -R go-rwx "${SUITE_DIR[httpfs]}/data/secrets" "${SUITE_DIR[core]}/data/secrets" 2>/dev/null
	export TEST_PERSISTENT_SECRETS_AVAILABLE=true
fi

if [[ " $SUITES " == *" iceberg "* ]] && ! ensure_iceberg_fixture; then
	echo "===== [duckdb] END (rc=1, iceberg fixture failed) ====="
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
# parses the XML, so the log is the artifact. Streamed through tee so the
# per-test progress shows up while the suites run, not 6000 lines at the end.
rc=0
summaries=()
run_unittest() {
	local start
	start=$(wc -l <"$log")
	"$UNITTEST" "${args[@]}" "$@" 2>&1 | tee -a "$log"
	local unittest_rc=${PIPESTATUS[0]}
	summaries+=("$(tail -n +$((start + 1)) "$log" | grep -E '^test cases:|^All tests (passed|were skipped)' | tail -1)")
	[[ $rc -eq 0 ]] && rc=$unittest_rc
}
: >"$log"
[[ -n "$spec" ]] && run_unittest --jobs "$DUCKDB_JOBS" "$spec"
[[ -n "$serial_spec" ]] && run_unittest "$serial_spec"
if [[ " $SUITES " == *" iceberg "* ]]; then
	run_unittest --order lex --test-config "$ICEBERG_DIR/test/configs/fixture.json" \
		"$ICEBERG_DIR/test/sql/local/catalog_test_config_setup/*"
	http_proxy_public="${HTTP_PROXY_PUBLIC-}"
	unset HTTP_PROXY_PUBLIC
	[[ -n "${ICEBERG_HTTP_PROXY:-}" ]] && export HTTP_PROXY_PUBLIC="$ICEBERG_HTTP_PROXY"
	CATALOG_TEST_CONFIG_SETUP=fixture run_unittest --order lex \
		"$ICEBERG_DIR/test/sql/local/catalog_custom_setup/*,$ICEBERG_DIR/test/sql/local/partitioning/foldable_expression_filter.test,$ICEBERG_DIR/test/sql/local/partitioning/in_filter.test"
	[[ -n "$http_proxy_public" ]] && export HTTP_PROXY_PUBLIC="$http_proxy_public"
fi
if [[ "$run_interop" == true ]]; then
	start=$(wc -l <"$log")
	BUILD_DIR="$BUILD_DIR" "$SCRIPT_DIR/interop/run.sh" 2>&1 | tee -a "$log"
	interop_rc=${PIPESTATUS[0]}
	summaries+=("$(tail -n +$((start + 1)) "$log" | grep -E '^===== \[duckdb interop\] [0-9]+/[0-9]+ passed' | tail -1)")
	[[ $rc -eq 0 ]] && rc=$interop_rc
fi

# A spec that matches nothing exits 0, which would turn a typo'd filter (or an
# extension whose tests stopped being registered) into a silent pass.
if grep -qE '^No tests ran|No test cases matched' "$log"; then
	echo "ERROR: the filter matched no tests -- suites=${SUITES// /,}" >&2
	rc=1
fi

echo "===== [duckdb] END (rc=$rc) ====="

echo
echo "===== [duckdb] SUMMARY ====="
# Catch prints one of three shapes: "test cases: N | ..." when anything failed
# or was skipped, "All tests passed (...)" when fully clean, and "All tests
# were skipped (...)" when every test was gated behind a require-env.
for summary in "${summaries[@]}"; do
	printf '  %-10s %s\n' "$([[ $rc -eq 0 ]] && echo PASS || echo FAIL)" \
		"${summary:-no summary -- run did not reach the end}"
done
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
