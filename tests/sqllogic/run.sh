#!/bin/bash

if ((BASH_VERSINFO[0] < 4)); then
	for _sdb_bash in /opt/homebrew/bin/bash /usr/local/bin/bash; do
		if [[ -x $_sdb_bash ]]; then
			exec "$_sdb_bash" "$0" "$@"
		fi
	done
	echo "run.sh requires bash >= 4 (macOS ships 3.2); install via 'brew install bash'" >&2
	exit 1
fi

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

: "${RESOURCES:=$(realpath "$SCRIPT_DIR/../../resources")}"
export RESOURCES

# Cross-uid container setups (the RTA composes): the server accesses
# __TEST_DIR__ as a different uid, so everything tests create there --
# including subdirs made by `system` commands -- must be world-writable.
# The runner already relaxes __TEST_DIR__ itself under the same flag.
if [[ -n "${SLT_TEST_DIR_SHARED:-}" ]]; then
	umask 0
fi

# Local sanitizer runs: if the built serened is a sanitizer binary and the
# matching *_OPTIONS var isn't already set, export it (with the local
# suppressions file) so a serened started from this shell -- or a sanitizer-built
# sqllogictest client -- loads the same suppressions CI uses. Guarded on the
# binary being instrumented for that sanitizer and the file existing, so
# uninstrumented local runs are not perturbed. Existing *_OPTIONS are left
# untouched. LSan runs under ASan, so an ASan binary also gets LSAN_OPTIONS.
_sdb_repo_root=$(realpath "$SCRIPT_DIR/../..")
for _sdb_bin in "$_sdb_repo_root"/build_tsan/bin/serened \
	"$_sdb_repo_root"/build_asan/bin/serened \
	"$_sdb_repo_root"/build*/bin/serened; do
	[[ -x "$_sdb_bin" ]] || continue
	_sdb_syms=$(nm -D "$_sdb_bin" 2>/dev/null)

	if [[ -z "${TSAN_OPTIONS:-}" ]] && grep -q '__tsan_init' <<<"$_sdb_syms"; then
		_sdb_opts="detect_deadlocks=true:second_deadlock_stack=1:history_size=0"
		if [[ -f "$_sdb_repo_root/resources/suppressions/tsan.txt" ]]; then
			_sdb_opts="${_sdb_opts}:suppressions=${_sdb_repo_root}/resources/suppressions/tsan.txt"
		fi
		export TSAN_OPTIONS="$_sdb_opts"
	fi

	if grep -q '__asan_init' <<<"$_sdb_syms"; then
		if [[ -z "${ASAN_OPTIONS:-}" ]]; then
			_sdb_opts="handle_ioctl=true:check_initialization_order=true:detect_odr_violation=1:strict_init_order=true"
			if [[ -f "$_sdb_repo_root/resources/suppressions/asan.txt" ]]; then
				_sdb_opts="${_sdb_opts}:suppressions=${_sdb_repo_root}/resources/suppressions/asan.txt"
			fi
			export ASAN_OPTIONS="$_sdb_opts"
		fi
		if [[ -z "${LSAN_OPTIONS:-}" && -f "$_sdb_repo_root/resources/suppressions/lsan.txt" ]]; then
			export LSAN_OPTIONS="suppressions=${_sdb_repo_root}/resources/suppressions/lsan.txt"
		fi
	fi

	if [[ -z "${UBSAN_OPTIONS:-}" ]] && grep -q '__ubsan_handle' <<<"$_sdb_syms"; then
		_sdb_opts="print_stacktrace=1"
		if [[ -f "$_sdb_repo_root/resources/suppressions/ubsan.txt" ]]; then
			_sdb_opts="${_sdb_opts}:suppressions=${_sdb_repo_root}/resources/suppressions/ubsan.txt"
		fi
		export UBSAN_OPTIONS="$_sdb_opts"
	fi

	if [[ -z "${MSAN_OPTIONS:-}" ]] && grep -q '__msan_init' <<<"$_sdb_syms"; then
		_sdb_opts="poison_in_dtor=1"
		if [[ -f "$_sdb_repo_root/resources/suppressions/msan.txt" ]]; then
			_sdb_opts="${_sdb_opts}:suppressions=${_sdb_repo_root}/resources/suppressions/msan.txt"
		fi
		export MSAN_OPTIONS="$_sdb_opts"
	fi

	break
done
unset _sdb_repo_root _sdb_bin _sdb_syms _sdb_opts

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
	[runner]="$SCRIPT_DIR/../../third_party/sqllogictest-rs"
	[jobs]=$(nproc 2>/dev/null || sysctl -n hw.logicalcpu 2>/dev/null || echo 4)
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

SERVICES="$SCRIPT_DIR/../drivers/harness/services.py"
SERVICES_STATE=""
cancel_pid=""
: "${ICEBERG_BACKEND:=local}"

cleanup_cancel_pid() {
	if [[ -n "$cancel_pid" ]]; then
		local pid="$cancel_pid"
		cancel_pid=""
		kill "$pid" 2>/dev/null || true
		wait "$pid" 2>/dev/null || true
	fi
}

cleanup_services() {
	if [[ -n "$SERVICES_STATE" ]]; then
		local state="$SERVICES_STATE"
		SERVICES_STATE=""
		python3 "$SERVICES" down --state "$state"
		rm -f "$state"
	fi
}

cleanup_all() {
	cleanup_cancel_pid
	cleanup_services
}

trap cleanup_all EXIT
# Signal handlers just exit; EXIT trap then runs cleanup_all. Without these,
# a default `trap ... INT` would run cleanup but NOT exit -- bash would resume
# the test loop after Ctrl-C, which is not what we want.
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP

generated_tests() {
	local generator target
	shopt -s globstar nullglob
	for generator in "$SCRIPT_DIR"/generators/**/*.py; do
		target="$SCRIPT_DIR/${generator#"$SCRIPT_DIR/generators/"}"
		target="${target%.py}.gen.test"
		if ! python3 "$SCRIPT_DIR/generated.py" "$1" "$generator" "$target"; then
			echo "ERROR: $generator failed to $1 $target" >&2
			exit 1
		fi
	done
	shopt -u globstar nullglob
}

launch_external() {
	shopt -s globstar
	local pattern f
	local -a files=() kept=()
	for pattern in "${tests[@]}"; do
		while IFS= read -r f; do
			[[ -n "$f" ]] || continue
			[[ -n "$skip" && "$f" =~ $skip ]] && continue
			files+=("$f")
		done < <(compgen -G "$pattern" 2>/dev/null || true)
	done
	shopt -u globstar
	local backend=()
	if [[ "$ICEBERG_BACKEND" == "biglake" ]]; then
		backend=(--biglake)
		for f in "${files[@]}"; do
			[[ "$f" == *_fixture_iceberg.test_slow ]] || kept+=("$f")
		done
		if [[ ${#kept[@]} -eq 0 ]]; then
			echo "ICEBERG_BACKEND=biglake: nothing left to run."
			exit 0
		fi
		files=("${kept[@]}")
		tests=("${kept[@]}")
	fi
	[[ ${#files[@]} -gt 0 ]] || return 0
	local names exports
	names=$(python3 "$SERVICES" needs "${backend[@]}" "${files[@]}") || exit 1
	[[ -n "$names" ]] || return 0
	SERVICES_STATE=$(mktemp)
	read -ra names <<<"$names"
	exports=$(python3 "$SERVICES" up --state "$SERVICES_STATE" --log-dir "${LOG_DIR:-/tmp}" "${names[@]}") || exit 1
	eval "$exports"
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

# The recovery harness invokes run.sh once per test; the identical config
# banner on every call drowns the logs. SDB_SQLLOGIC_QUIET=1 suppresses it.
if [[ "${SDB_SQLLOGIC_QUIET:-0}" != "1" ]]; then
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
fi

if [[ "$fast" == "true" ]]; then
	# Strip trailing * to exclude .test_slow files (*.test* -> *.test)
	for i in "${!tests[@]}"; do
		tests[i]="${tests[i]%\*}"
	done
fi

generated_tests write
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

	sqllogictest "${tests[@]}" \
		--host "$host" --port "$port" --engine "$engine" \
		--jobs "$jobs" \
		--label "$database" \
		${PGSOCKETDIR:+--label pgsocket} \
		--junit "$junit-$engine" \
		$options \
		$skip_failed_opt ${skip_failed:+"$skip_failed"} \
		$skip_opt \
		$ssl_port_opt
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
	build_cmd=(cargo build --manifest-path "$runner/sqllogictest-bin/Cargo.toml" --target-dir "$SQLLOGIC_TARGET" --quiet)
	[[ "$debug" != "true" ]] && build_cmd+=(--release)

	# cargo's crates.io fetch flakes intermittently on the CI runners (HTTP/2
	# framing errors, "unable to update registry"). It's transient, so retry a
	# few times before giving up instead of failing the whole suite.
	build_exit_code=0
	build_attempts="${SDB_SQLLOGIC_BUILD_ATTEMPTS:-3}"
	for attempt in $(seq 1 "$build_attempts"); do
		"${build_cmd[@]}"
		build_exit_code=$?
		[[ $build_exit_code == 0 ]] && break
		if [[ $attempt -lt $build_attempts ]]; then
			echo "sqllogictest build failed (exit $build_exit_code); attempt $attempt/$build_attempts, retrying in 10s..." >&2
			sleep 10
		fi
	done
	echo "sqllogictest build: $(($(date +%s) - build_start))s"

	# Abort here on a failed build: falling through into the test loop runs a
	# stale or missing binary and reports the build failure as a test failure.
	if [[ $build_exit_code != 0 ]]; then
		echo "ERROR: sqllogictest build failed after $build_attempts attempt(s); aborting before tests run." >&2
		exit $build_exit_code
	fi
fi
if [[ ! -x "${SQLLOGIC_TARGET}/${build_type}/sqllogictest" ]]; then
	echo "ERROR: no sqllogictest in ${SQLLOGIC_TARGET}/${build_type}" >&2
	exit 1
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

if [[ "$override" == "true" || "$force_override" == "true" ]]; then
	generated_tests record
fi

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
