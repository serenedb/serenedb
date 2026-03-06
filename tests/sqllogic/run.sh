#!/bin/bash

# Boolean flags configuration
declare -a BOOLEAN_FLAGS=(debug override force-override format show-all-errors fast)

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
	[cluster_port]=''
	[protocol]='simple' # simple|extended|both TODO(mbkkt) make both
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
	[database]='serenedb'
	[host]='localhost'
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

launch_s3() {
	MINIO_CONTAINER_NAME="serenedb-test-minio-$$"
	export MINIO_ACCESS_KEY="minioadmin"
	export MINIO_SECRET_KEY="minioadmin"
	export MINIO_BUCKET="testbucket"
	export MINIO_PORT
	MINIO_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()')

	cleanup_minio() {
		if [[ -n "${MINIO_CONTAINER_NAME:-}" ]]; then
			echo "Stopping MinIO container..."
			docker rm -f "$MINIO_CONTAINER_NAME" 2>/dev/null || true
		fi
	}
	trap cleanup_minio EXIT

	local network_args=()
	if [[ -n "${COMPOSE_NETWORK:-}" ]]; then
		network_args=(--network "$COMPOSE_NETWORK")
		export MINIO_HOST="$MINIO_CONTAINER_NAME"
		export MINIO_PORT=9000
	else
		network_args=(-p "$MINIO_PORT:9000")
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
		if curl -sf "http://${MINIO_HOST}:${MINIO_PORT}/minio/health/ready" >/dev/null 2>&1; then
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
		mc alias set local http://localhost:9000 "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" >/dev/null 2>&1
	docker exec "$MINIO_CONTAINER_NAME" \
		mc mb "local/$MINIO_BUCKET" >/dev/null 2>&1 || true

	echo "MinIO running (host=$MINIO_HOST, port=$MINIO_PORT), bucket '$MINIO_BUCKET' created."
	echo
}

launch_external() {
	shopt -s globstar
	local test_files
	test_files=$(compgen -G "$test" 2>/dev/null || true)
	shopt -u globstar
	if echo "$test_files" | grep -q '_s3\.'; then
		launch_s3
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
		single-port | cluster-port | jobs | protocol | test | junit | runner | debug | override | format | force-override | show-all-errors | fast | skip-failed | database | host)
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
			single-port | cluster-port | jobs)
				validate_number "$value" "--$key" || return 1
				;;
			debug)
				validate_boolean "$value" "--$key" || return 1
				;;
			esac

			declare -g "$var_name"="$value"
			;;
		*)
			echo "Unknown option: --$key" >&2
			return 1
			;;
		esac
		shift
	done
}

# Example usage:
parse_options "$@" || exit 1

# Apply defaults for any options not provided
for var_name in "${!defaults[@]}"; do
	if [[ -z "${!var_name}" ]]; then
		declare -g "$var_name"="${defaults[$var_name]}"
	fi
done

# Display the values (for demonstration)
echo "Database: $database"
echo "Host: $host"
echo "Single Port: $single_port"
echo "Cluster Port: $cluster_port"
echo "Protocol: $protocol"
echo "Test Path: $test"
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

if [[ "$fast" == "true" ]]; then
	test='./tests/sqllogic/sdb/**/*.test'
fi

launch_external

# Run tests based on parameters
run_tests() {
	local mode=$1
	local port=$2
	local engine=$3

	echo
	echo "Running tests for $database database in $mode mode on port $port with $engine engine"
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

	# Execute the command and capture the exit code
	sqllogictest "$test" \
		--host "$host" --port "$port" --engine "$engine" \
		--jobs "$jobs" \
		--label "$database" --label "$mode" --label "$engine-protocol" \
		--junit "$junit-$mode-$engine" \
		$options \
		$skip_failed_opt ${skip_failed:+"$skip_failed"}
	return $?
}

# Determine mode based on port settings
if [[ -n "$single_port" && -n "$cluster_port" ]]; then
	mode="both"
	if [[ "$single_port" == "$cluster_port" ]]; then
		echo "ERROR: single-port and cluster-port must be different when both are specified"
		exit 1
	fi
elif [[ -n "$single_port" ]]; then
	mode="single"
elif [[ -n "$cluster_port" ]]; then
	mode="cluster"
else
	# Default case when neither is specified
	mode="single"
	single_port=5432
fi

# Validate protocol
if [[ "$protocol" != "simple" && "$protocol" != "extended" && "$protocol" != "both" ]]; then
	echo "Invalid protocol. Must be 'simple', 'extended', or 'both'"
	exit 1
fi

# Variable to track the highest exit code encountered
final_exit_code=0

if [[ "$debug" == "true" ]]; then
	cargo install --debug --path $runner/sqllogictest-bin --quiet --force
	test_exit_code=$?
else
	cargo install --path $runner/sqllogictest-bin --quiet --force
	test_exit_code=$?
fi
[[ $test_exit_code != 0 ]] && final_exit_code=$test_exit_code

# Execute tests based on mode and protocol
if [[ "$protocol" == "simple" || "$protocol" == "both" ]]; then
	if [[ "$mode" == "single" || "$mode" == "both" ]]; then
		run_tests "single" "$single_port" "postgres"
		test_exit_code=$?
		[[ $test_exit_code != 0 ]] && final_exit_code=$test_exit_code
	fi
	if [[ "$mode" == "cluster" || "$mode" == "both" ]]; then
		run_tests "cluster" "$cluster_port" "postgres"
		test_exit_code=$?
		[[ $test_exit_code != 0 ]] && final_exit_code=$test_exit_code
	fi
fi

if [[ "$protocol" == "extended" || "$protocol" == "both" ]]; then
	if [[ "$mode" == "single" || "$mode" == "both" ]]; then
		run_tests "single" "$single_port" "postgres-extended"
		test_exit_code=$?
		[[ $test_exit_code != 0 ]] && final_exit_code=$test_exit_code
	fi
	if [[ "$mode" == "cluster" || "$mode" == "both" ]]; then
		run_tests "cluster" "$cluster_port" "postgres-extended"
		test_exit_code=$?
		[[ $test_exit_code != 0 ]] && final_exit_code=$test_exit_code
	fi
fi

exit $final_exit_code
