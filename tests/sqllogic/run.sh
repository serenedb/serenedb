#!/bin/bash

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

: "${RESOURCES:=$(realpath "$SCRIPT_DIR/../../resources")}"
export RESOURCES

# Boolean flags configuration
declare -a BOOLEAN_FLAGS=(debug override force-override format show-all-errors fast cancellation)

is_boolean_flag() {
	local key="$1"
	for flag in "${BOOLEAN_FLAGS[@]}"; do
		[[ "$key" == "$flag" ]] && return 0
	done
	return 1
}

# Default values
declare -A defaults=(
	[single_port]=''
	[single_port_ssl]=''
	[cluster_port]=''
	[engines]='pg-wire-simple,pg-wire-extended'
	[test]='./tests/sqllogic/sdb/**/*.test*'
	[junit]='./out/sqllogic-tests'
	[runner]='./third_party/sqllogictest-rs'
	[jobs]=$(nproc)
	[debug]=false
	[override]=false
	[force_override]=false
	[format]=false
	[show_all_errors]=false
	[fast]=false
	[skip_failed]=''
	[skip]=''
	[database]='serenedb'
	[host]='localhost'
	[iterations]=1
	[cancellation]=false
)

# Type validators
validate_number() {
	[[ "$1" =~ ^[0-9]+$ ]] || {
		echo "Error: $2 must be a number" >&2
		return 1
	}
}
validate_boolean() {
	[[ "$1" =~ ^(true|false)$ ]] || {
		echo "Error: $2 must be 'true' or 'false'" >&2
		return 1
	}
}

# Trap is armed before launch_external so a MinIO startup failure still cleans
# up the container. Helpers clear their state var before doing work, so repeat
# invocations (e.g. INT then EXIT) are no-ops on the second pass.
MINIO_CONTAINER_NAME=""
MINIO_LOG_FILE=""
ICEBERG_REST_CONTAINER_NAME=""
ICEBERG_REST_LOG_FILE=""
OLLAMA_CONTAINER_NAME=""
OLLAMA_LOG_FILE=""
POSTGRES_CONTAINER_NAME=""
POSTGRES_LOG_FILE=""
TEST_NETWORK=""
cancel_pid=""

cleanup_test_network() {
	if [[ -n "$TEST_NETWORK" ]]; then
		local net="$TEST_NETWORK"
		TEST_NETWORK=""
		# Removed only after the containers that joined it are gone, otherwise
		# docker errors with "network has active endpoints". cleanup_all calls
		# this last for that reason.
		docker network rm "$net" >/dev/null 2>&1 || true
	fi
}

cleanup_minio() {
	if [[ -n "$MINIO_CONTAINER_NAME" ]]; then
		local name="$MINIO_CONTAINER_NAME"
		MINIO_CONTAINER_NAME=""
		if [[ -n "$MINIO_LOG_FILE" ]]; then
			echo "Saving MinIO logs to ${MINIO_LOG_FILE}..."
			docker logs "$name" >"${MINIO_LOG_FILE}" 2>&1 || true
		fi
		echo "Stopping MinIO container..."
		docker rm -fv "$name" >/dev/null 2>&1 || true
	fi
}

cleanup_iceberg_rest() {
	if [[ -n "$ICEBERG_REST_CONTAINER_NAME" ]]; then
		local name="$ICEBERG_REST_CONTAINER_NAME"
		ICEBERG_REST_CONTAINER_NAME=""
		if [[ -n "$ICEBERG_REST_LOG_FILE" ]]; then
			echo "Saving iceberg-rest logs to ${ICEBERG_REST_LOG_FILE}..."
			docker logs "$name" >"${ICEBERG_REST_LOG_FILE}" 2>&1 || true
		fi
		echo "Stopping iceberg-rest container..."
		docker rm -fv "$name" >/dev/null 2>&1 || true
	fi
}

cleanup_cancel_pid() {
	if [[ -n "$cancel_pid" ]]; then
		local pid="$cancel_pid"
		cancel_pid=""
		kill "$pid" 2>/dev/null || true
		wait "$pid" 2>/dev/null || true
	fi
}

cleanup_ollama() {
	if [[ -n "$OLLAMA_CONTAINER_NAME" ]]; then
		local name="$OLLAMA_CONTAINER_NAME"
		OLLAMA_CONTAINER_NAME=""
		if [[ -n "$OLLAMA_LOG_FILE" ]]; then
			echo "Saving Ollama logs to ${OLLAMA_LOG_FILE}..."
			docker logs "$name" >"${OLLAMA_LOG_FILE}" 2>&1 || true
		fi
		echo "Stopping Ollama container..."
		docker rm -fv "$name" >/dev/null 2>&1 || true
	fi
}

cleanup_postgres() {
	if [[ -n "$POSTGRES_CONTAINER_NAME" ]]; then
		local name="$POSTGRES_CONTAINER_NAME"
		POSTGRES_CONTAINER_NAME=""
		if [[ -n "$POSTGRES_LOG_FILE" ]]; then
			echo "Saving postgres logs to ${POSTGRES_LOG_FILE}..."
			docker logs "$name" >"${POSTGRES_LOG_FILE}" 2>&1 || true
		fi
		echo "Stopping postgres container..."
		docker rm -fv "$name" >/dev/null 2>&1 || true
	fi
}

cleanup_all() {
	cleanup_cancel_pid
	cleanup_iceberg_rest
	cleanup_ollama
	cleanup_postgres
	cleanup_minio
	cleanup_test_network
}

trap cleanup_all EXIT
# Signal handlers just exit; EXIT trap then runs cleanup_all. Without these,
# a default `trap ... INT` would run cleanup but NOT exit -- bash would resume
# the test loop after Ctrl-C, which is not what we want.
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP

launch_s3() {
	PREFIX="$(LC_ALL=C tr -dc 'a-z0-9' </dev/urandom 2>/dev/null | head -c 4)"
	MINIO_CONTAINER_NAME="${PREFIX}-serenedb-test-minio-$$"
	MINIO_LOG_FILE="${LOG_DIR:-/tmp}/${MINIO_CONTAINER_NAME}.log"
	export MINIO_ACCESS_KEY="minioadmin"
	export MINIO_SECRET_KEY="minioadmin"
	export MINIO_BUCKET="testbucket"
	export MINIO_PORT
	MINIO_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()')

	local network_args=()
	if [[ -n "${COMPOSE_NETWORK:-}" ]]; then
		network_args=(--network "$COMPOSE_NETWORK")
		export MINIO_HOST="$MINIO_CONTAINER_NAME"
		export MINIO_PORT=9000
	else
		TEST_NETWORK="${PREFIX}-serenedb-test-net-$$"
		docker network create "$TEST_NETWORK" >/dev/null
		network_args=(--network "$TEST_NETWORK" -p "$MINIO_PORT:9000")
		export MINIO_HOST="localhost"
	fi

	echo "Starting MinIO (host=$MINIO_HOST, port=$MINIO_PORT)..."
	docker run -d \
		--name "$MINIO_CONTAINER_NAME" \
		"${network_args[@]}" \
		-e "MINIO_ROOT_USER=$MINIO_ACCESS_KEY" \
		-e "MINIO_ROOT_PASSWORD=$MINIO_SECRET_KEY" \
		minio/minio:latest server /data

	echo "Waiting for MinIO to be ready..."
	for i in $(seq 1 30); do
		if docker exec "$MINIO_CONTAINER_NAME" \
			mc alias set local http://127.0.0.1:9000 "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" \
			>/dev/null 2>&1; then
			echo "MinIO is ready."
			break
		fi
		if [[ $i -eq 30 ]]; then
			echo "ERROR: MinIO failed to start within 30 seconds"
			exit 1
		fi
		sleep 1
	done

	echo "Creating bucket '$MINIO_BUCKET'..."
	docker exec "$MINIO_CONTAINER_NAME" \
		mc mb "local/$MINIO_BUCKET"

	echo "MinIO running (host=$MINIO_HOST, port=$MINIO_PORT), bucket '$MINIO_BUCKET' created."
	echo
}

# Launches an Apache Iceberg REST catalog (iceberg-rest-fixture) backed by
# MinIO as the warehouse. iceberg-rest itself doesn't serve table data --
# parquet/avro files have to live on storage both the catalog (writes) and
# serened (reads) can reach -- so MinIO is required.
launch_iceberg_rest() {
	local prefix
	prefix="$(LC_ALL=C tr -dc 'a-z0-9' </dev/urandom 2>/dev/null | head -c 4)"
	ICEBERG_REST_CONTAINER_NAME="${prefix}-serenedb-test-iceberg-rest-$$"
	ICEBERG_REST_LOG_FILE="${LOG_DIR:-/tmp}/${ICEBERG_REST_CONTAINER_NAME}.log"
	export ICEBERG_WAREHOUSE="demo"

	local docker_args=(
		-d
		--name "$ICEBERG_REST_CONTAINER_NAME"
	)
	# Join the same docker network as MinIO so the catalog reaches it by
	# container name. In compose mode that's COMPOSE_NETWORK (the consumer is
	# the tests container on that network, so use the iceberg-rest container's
	# name + internal port and skip -p); in local mode it's TEST_NETWORK and
	# the consumer is serened on host, so publish a host port.
	if [[ -n "${COMPOSE_NETWORK:-}" ]]; then
		docker_args+=(--network "$COMPOSE_NETWORK")
		export ICEBERG_REST_HOST="$ICEBERG_REST_CONTAINER_NAME"
		export ICEBERG_REST_PORT=8181
	else
		ICEBERG_REST_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()')
		export ICEBERG_REST_HOST="localhost"
		export ICEBERG_REST_PORT
		docker_args+=(--network "$TEST_NETWORK" -p "${ICEBERG_REST_PORT}:8181")
	fi
	export ICEBERG_REST_URL="http://${ICEBERG_REST_HOST}:${ICEBERG_REST_PORT}"

	# S3 warehouse on MinIO. Both containers share the network above, so the
	# catalog reaches MinIO via container name on its internal port.
	local catalog_env=(
		-e "AWS_ACCESS_KEY_ID=${MINIO_ACCESS_KEY}"
		-e "AWS_SECRET_ACCESS_KEY=${MINIO_SECRET_KEY}"
		-e "AWS_REGION=us-east-1"
		-e "CATALOG_WAREHOUSE=s3://${MINIO_BUCKET}/warehouse/"
		-e "CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO"
		-e "CATALOG_S3_ENDPOINT=http://${MINIO_CONTAINER_NAME}:9000"
		-e "CATALOG_S3_PATH__STYLE__ACCESS=true"
	)

	echo "Starting iceberg-rest (port=$ICEBERG_REST_PORT)..."
	docker run "${docker_args[@]}" "${catalog_env[@]}" \
		apache/iceberg-rest-fixture:1.10.1

	echo "Waiting for iceberg-rest to be ready..."
	for i in $(seq 1 60); do
		if bash -c "echo > /dev/tcp/${ICEBERG_REST_HOST}/${ICEBERG_REST_PORT}" 2>/dev/null; then
			echo "iceberg-rest is ready."
			break
		fi
		if [[ $i -eq 60 ]]; then
			echo "ERROR: iceberg-rest failed to start within 60 seconds"
			exit 1
		fi
		sleep 1
	done

	echo "iceberg-rest running (url=$ICEBERG_REST_URL)."
	echo
}

# Launches an Ollama server and pulls a small embedding model. Ollama exposes
# an OpenAI-compatible API at /v1/embeddings on port 11434, which ai_embed()
# targets via a SECRET of TYPE openai with a custom base_url. The pulled
# model determines the embedding dimension; pick the smallest one that's
# still useful so the first run stays under ~30s in CI.
launch_ollama() {
	local prefix
	prefix="$(LC_ALL=C tr -dc 'a-z0-9' </dev/urandom 2>/dev/null | head -c 4)"
	OLLAMA_CONTAINER_NAME="${prefix}-serenedb-test-ollama-$$"
	OLLAMA_LOG_FILE="${LOG_DIR:-/tmp}/${OLLAMA_CONTAINER_NAME}.log"
	export OLLAMA_MODEL="${OLLAMA_MODEL:-all-minilm}"

	local network_args=()
	if [[ -n "${COMPOSE_NETWORK:-}" ]]; then
		network_args=(--network "$COMPOSE_NETWORK")
		export OLLAMA_HOST="$OLLAMA_CONTAINER_NAME"
		export OLLAMA_PORT=11434
	else
		if [[ -z "$TEST_NETWORK" ]]; then
			TEST_NETWORK="${prefix}-serenedb-test-net-$$"
			docker network create "$TEST_NETWORK" >/dev/null
		fi
		OLLAMA_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()')
		network_args=(--network "$TEST_NETWORK" -p "$OLLAMA_PORT:11434")
		export OLLAMA_HOST="localhost"
		export OLLAMA_PORT
	fi

	echo "Starting Ollama (host=$OLLAMA_HOST, port=$OLLAMA_PORT)..."
	docker run -d \
		--name "$OLLAMA_CONTAINER_NAME" \
		"${network_args[@]}" \
		ollama/ollama:latest

	echo "Waiting for Ollama to be ready..."
	for i in $(seq 1 60); do
		if docker exec "$OLLAMA_CONTAINER_NAME" \
			ollama list >/dev/null 2>&1; then
			echo "Ollama is ready."
			break
		fi
		if [[ $i -eq 60 ]]; then
			echo "ERROR: Ollama failed to start within 60 seconds"
			exit 1
		fi
		sleep 1
	done

	echo "Pulling model '$OLLAMA_MODEL'..."
	docker exec "$OLLAMA_CONTAINER_NAME" ollama pull "$OLLAMA_MODEL"

	echo "Ollama running (host=$OLLAMA_HOST, port=$OLLAMA_PORT, model=$OLLAMA_MODEL)."
	echo
}

# Launches a postgres 16 container, used by tests that ATTACH a real postgres
# via the postgres_scanner DuckDB extension (filename suffix `_pgscan.`). Trust
# auth + a single postgres role; per-test database scoping is the test's
# responsibility (`CREATE DATABASE` / `ATTACH ... TYPE postgres`).
launch_postgres() {
	local prefix
	prefix="$(LC_ALL=C tr -dc 'a-z0-9' </dev/urandom 2>/dev/null | head -c 4)"
	POSTGRES_CONTAINER_NAME="${prefix}-serenedb-test-postgres-$$"
	POSTGRES_LOG_FILE="${LOG_DIR:-/tmp}/${POSTGRES_CONTAINER_NAME}.log"

	local network_args=()
	if [[ -n "${COMPOSE_NETWORK:-}" ]]; then
		network_args=(--network "$COMPOSE_NETWORK")
		export PGHOST="$POSTGRES_CONTAINER_NAME"
		export PGPORT=5432
	else
		if [[ -z "$TEST_NETWORK" ]]; then
			TEST_NETWORK="${prefix}-serenedb-test-net-$$"
			docker network create "$TEST_NETWORK" >/dev/null
		fi
		PGPORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()')
		network_args=(--network "$TEST_NETWORK" -p "$PGPORT:5432")
		export PGHOST="localhost"
		export PGPORT
	fi
	export PGUSER=postgres
	export PGDATABASE=postgres

	echo "Starting postgres (host=$PGHOST, port=$PGPORT)..."
	docker run -d \
		--name "$POSTGRES_CONTAINER_NAME" \
		"${network_args[@]}" \
		-e POSTGRES_HOST_AUTH_METHOD=trust \
		-e POSTGRES_DB=postgres \
		postgres:18.3

	echo "Waiting for postgres to be ready..."
	for i in $(seq 1 60); do
		if docker exec "$POSTGRES_CONTAINER_NAME" \
			pg_isready -U postgres >/dev/null 2>&1; then
			echo "postgres is ready."
			break
		fi
		if [[ $i -eq 60 ]]; then
			echo "ERROR: postgres failed to start within 60 seconds"
			exit 1
		fi
		sleep 1
	done

	echo "postgres running (host=$PGHOST, port=$PGPORT)."
	echo
}

launch_external() {
	shopt -s globstar
	local pattern test_files needs_s3=false needs_iceberg=false needs_ollama=false needs_postgres=false
	for pattern in "${tests[@]}"; do
		test_files=$(compgen -G "$pattern" 2>/dev/null || true)
		if echo "$test_files" | grep -q '_s3\.'; then
			needs_s3=true
		fi
		if echo "$test_files" | grep -q 'iceberg'; then
			needs_iceberg=true
		fi
		if echo "$test_files" | grep -q '_ollama\.'; then
			needs_ollama=true
		fi
		if echo "$test_files" | grep -q '_pgscan\.'; then
			needs_postgres=true
		fi
	done
	shopt -u globstar

	# iceberg-rest's warehouse runs on MinIO (see launch_iceberg_rest), so any
	# iceberg test transitively requires MinIO too.
	if [[ "$needs_iceberg" == "true" ]]; then
		needs_s3=true
		# Path-based iceberg_scan() tests (e.g. equality_deletes regression)
		# read static fixtures shipped with the duckdb_iceberg submodule.
		# Inside the sqllogic compose the workspace is mounted at /serenedb;
		# in local-runner mode the same path is the repo root.
		local repo_root
		repo_root="$(cd "${SCRIPT_DIR}/../.." && pwd)"
		if [[ -n "${COMPOSE_NETWORK:-}" ]]; then
			export ICEBERG_FIXTURES="/serenedb/third_party/duckdb_iceberg/data/persistent"
		else
			export ICEBERG_FIXTURES="${repo_root}/third_party/duckdb_iceberg/data/persistent"
		fi
	fi
	if [[ "$needs_s3" == "true" ]]; then
		launch_s3
	fi
	if [[ "$needs_iceberg" == "true" ]]; then
		launch_iceberg_rest
	fi
	if [[ "$needs_ollama" == "true" ]]; then
		launch_ollama
	fi
	if [[ "$needs_postgres" == "true" ]]; then
		launch_postgres
	fi
}

# Main parsing function
parse_options() {
	while [ $# -gt 0 ]; do
		local opt="$1"
		local key="${opt#--}"
		local value=""
		local is_equal_format=false

		# Check if option uses --key=value format
		if [[ "$opt" == *=* ]]; then
			key="${opt%%=*}"
			key="${key#--}"
			value="${opt#*=}"
			is_equal_format=true
		fi

		case "$key" in
		single-port | single-port-ssl | cluster-port | jobs | engines | test | junit | runner | debug | override | format | force-override | show-all-errors | fast | skip-failed | skip | database | host | iterations | cancellation)
			local var_name="${key//-/_}" # Convert dashes to underscores

			# For non-equal format (--option value), get the next argument
			if ! $is_equal_format; then
				if [ $# -ge 2 ] && [[ $2 != --* ]]; then
					value="$2"
					shift
				else
					# Boolean flags get special treatment
					if is_boolean_flag "$key"; then
						value=true
					else
						value=""
					fi
				fi
			fi

			# Apply default if value is empty (except for boolean flags)
			if [[ -z "$value" ]] && ! is_boolean_flag "$key"; then
				value="${defaults[$var_name]}"
			fi

			# Type validation
			case "$key" in
			single-port | single-port-ssl | cluster-port | jobs | iterations)
				validate_number "$value" "--$key" || return 1
				;;
			debug)
				validate_boolean "$value" "--$key" || return 1
				;;
			esac

			# --test is repeatable: accumulate into the `tests` array. Every
			# other option is scalar.
			if [[ "$key" == "test" ]]; then
				tests+=("$value")
			else
				declare -g "$var_name"="$value"
			fi
			;;
		*)
			echo "Unknown option: --$key" >&2
			return 1
			;;
		esac
		shift
	done
}

# --test is repeatable; collect into array and fall back to the single default
# glob when none are provided.
tests=()

# Example usage:
parse_options "$@" || exit 1

# Apply defaults for any options not provided
for var_name in "${!defaults[@]}"; do
	# `test` is handled as an array (`tests`); skip the scalar default here.
	[[ "$var_name" == "test" ]] && continue
	if [[ -z "${!var_name}" ]]; then
		declare -g "$var_name"="${defaults[$var_name]}"
	fi
done

if [[ ${#tests[@]} -eq 0 ]]; then
	tests=("${defaults[test]}")
fi

# Display the values (for demonstration)
IFS=',' read -ra engines_list <<<"$engines"
for engine in "${engines_list[@]}"; do
	if [[ "$engine" != "pg-wire-simple" && "$engine" != "pg-wire-extended" ]]; then
		echo "Invalid engine '$engine'. Must be 'pg-wire-simple' or 'pg-wire-extended'" >&2
		exit 1
	fi
done

echo "Database: $database"
echo "Host: $host"
echo "Single Port: $single_port"
echo "Single Port SSL: $single_port_ssl"
echo "Cluster Port: $cluster_port"
echo "Engines: $engines"
echo "Test Paths: ${tests[*]}"
echo "JUnit Path: $junit"
echo "Runner: $runner"
echo "Jobs: $jobs"
echo "Debug: $debug"
echo "Override: $override"
echo "Force override: $force_override"
echo "Format: $format"
echo "Show all errors: $show_all_errors"
echo "Fast: $fast"
echo "Skip failed: $skip_failed"
echo "Skip: $skip"
echo "Iterations: $iterations"
echo "Cancellation: $cancellation"

if [[ "$fast" == "true" ]]; then
	# Strip trailing * to exclude .test_slow files (*.test* -> *.test)
	for i in "${!tests[@]}"; do
		tests[i]="${tests[i]%\*}"
	done
fi

launch_external

# Run tests based on parameters
run_tests() {
	local port=$1
	local engine=$2

	echo
	echo "Running tests for $database database on port $port with $engine engine"

	local ssl_port_opt=""
	if [[ -n "$3" ]]; then
		ssl_port_opt="--ssl-port $3"
		echo "Using SSL port: $3"
	fi

	echo

	# Build options dynamically
	local options=""

	if [[ "$debug" != "true" ]]; then
		options+="--shutdown-timeout 60 "
	fi

	# Boolean flags - map shell variable names to CLI flags
	declare -A flag_map=(
		[override]="--override"
		[format]="--format"
		[force_override]="--force-override"
		[show_all_errors]="--show-all-errors"
	)

	for var_name in "${!flag_map[@]}"; do
		if [[ "${!var_name}" == "true" ]]; then
			options+="${flag_map[$var_name]} "
		fi
	done

	local skip_failed_opt=""
	if [[ -n "$skip_failed" ]]; then
		skip_failed_opt="--skip-failed"
	fi

	local skip_opt=""
	if [[ -n "$skip" ]]; then
		skip_opt="--skip $skip"
	fi

	# TODO(Misha) move this to sqllogictest-rs
	mkdir -p "$(dirname -- "$junit-$engine")"

	# Slowest-first off the persisted per-engine timing cache (opt-in): the runner
	# reads SDB_TIMING_CACHE to order discovered files and SDB_TIMING_OUT to record.
	local cache="" timing_out=""
	if [[ -n "${SDB_TIMING_CACHE_DIR:-}" ]]; then
		cache="$SDB_TIMING_CACHE_DIR/${engine}${SDB_TIMING_KEY:+.$SDB_TIMING_KEY}.tsv"
		timing_out=$(mktemp)
		export SDB_TIMING_CACHE="$cache" SDB_TIMING_OUT="$timing_out"
	fi

	sqllogictest "${tests[@]}" \
		--host "$host" --port "$port" --engine "$engine" \
		--jobs "$jobs" \
		--label "$database" \
		--junit "$junit-$engine" \
		$options \
		$skip_failed_opt ${skip_failed:+"$skip_failed"} \
		$skip_opt \
		$ssl_port_opt
	local rc=$?

	if [[ -n "$timing_out" ]]; then
		unset SDB_TIMING_CACHE SDB_TIMING_OUT
		python3 "$SCRIPT_DIR/timing_cache.py" merge "$cache" "$timing_out" 2>/dev/null || true
		rm -f "$timing_out"
	fi
	return $rc
}

# Default port when none specified
if [[ -z "$single_port" ]]; then
	single_port=5432
fi

# Variable to track the highest exit code encountered
final_exit_code=0

build_type="release"
[[ "$debug" == "true" ]] && build_type="debug"

# Use workspace-local target dir so builds are incremental across runs
SQLLOGIC_TARGET="${CARGO_TARGET_DIR:-${SCRIPT_DIR}/../../.cache/cargo-target}"
mkdir -p "$SQLLOGIC_TARGET"

# SDB_SKIP_SQLLOGIC_BUILD: a parent harness has already built sqllogictest
# into SQLLOGIC_TARGET. Multiple parallel run.sh invocations against a shared
# target dir (recovery harness fans out one per test) race on cargo's internal
# locks and occasionally exit non-zero even when the cached artifact is fine;
# that surfaces as a passing test reported as a failure.
if [[ "${SDB_SKIP_SQLLOGIC_BUILD:-0}" != "1" ]]; then
	build_start=$(date +%s)
	if [[ "$debug" == "true" ]]; then
		cargo build --manifest-path "$runner/sqllogictest-bin/Cargo.toml" --target-dir "$SQLLOGIC_TARGET" --quiet
	else
		cargo build --manifest-path "$runner/sqllogictest-bin/Cargo.toml" --target-dir "$SQLLOGIC_TARGET" --release --quiet
	fi
	test_exit_code=$?
	echo "sqllogictest build: $(($(date +%s) - build_start))s"
	[[ $test_exit_code != 0 ]] && final_exit_code=$test_exit_code
fi
export PATH="${SQLLOGIC_TARGET}/${build_type}:${PATH}"

if [[ "$cancellation" == "true" ]]; then
	# TODO: move this cancellation driver into the sqllogictest-rs runner.
	# Doing it there is more native -- we can cancel queries at the protocol
	# level instead of spraying SIGINTs across our own process group and
	# relying on `trap '' INT` to shield the parent shell. Known issues here:
	#   * `trap '' INT` is never restored, so INT stays ignored for the rest
	#     of the script (including the health check below).
	#   * `kill -INT 0` hits every process in our pgid; children inherit
	#     SIG_IGN from the parent's `trap '' INT`, so the runner only sees
	#     SIGINT because tokio reinstalls its own handler on startup.
	trap '' INT
	(
		while true; do
			sleep "$(awk "BEGIN{srand(); printf \"%.3f\", 0.05 + rand() * 2.0}")"
			kill -INT 0 2>/dev/null || true
		done
	) &
	cancel_pid=$!
fi

for iter in $(seq 1 "$iterations"); do
	for engine in "${engines_list[@]}"; do
		run_tests "$single_port" "$engine" "$single_port_ssl"
		test_exit_code=$?
		[[ $test_exit_code != 0 ]] && final_exit_code=$test_exit_code
	done
done

if [[ "$cancellation" == "true" ]]; then
	# Stop the SIGINT sender before the health check so pg_isready isn't killed
	cleanup_cancel_pid

	local_port="${single_port:-$cluster_port}"
	# TODO: pg_isready -h "$host" -p "$local_port" returns "no attempt" (exit 3)
	# inside the docker test container, even though the server is up. Works fine
	# outside docker. Needs investigation into what pg_isready expects from the
	# container environment (HOME, user mapping, pg_service.conf, etc.).
	if bash -c "echo > /dev/tcp/$host/$local_port" 2>/dev/null; then
		echo "[cancellation] Health check OK"
	else
		echo "[cancellation] ERROR: DB is not responsive!"
		final_exit_code=1
	fi
fi

exit $final_exit_code
