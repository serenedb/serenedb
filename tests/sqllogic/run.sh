#!/bin/bash

# Default values
declare -A defaults=(
    [single_port]=''
    [cluster_port]=''
    [protocol]='simple' # simple|extended|both TODO(mbkkt) make both
    [test]='./tests/sqllogic/sdb/*/*/*.test*'
    [junit]='./out/sqllogic-tests'
    [runner]='./third_party/sqllogictest-rs'
    [jobs]=$(nproc)
    [debug]=false
    [override]=false
    [show_all_errors]=false
    [database]='serenedb'
    [host]='localhost'
)

# Type validators
validate_number() {
    [[ "$1" =~ ^[0-9]+$ ]] || { echo "Error: $2 must be a number" >&2; return 1; }
}
validate_boolean() {
    [[ "$1" =~ ^(true|false)$ ]] || { echo "Error: $2 must be 'true' or 'false'" >&2; return 1; }
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
            single-port|cluster-port|jobs|protocol|test|junit|runner|debug|override|show-all-errors|database|host)
                local var_name="${key//-/_}"  # Convert dashes to underscores

                # For non-equal format (--option value), get the next argument
                if ! $is_equal_format; then
                    if [ $# -ge 2 ] && [[ $2 != --* ]]; then
                        value="$2"
                        shift
                    else
                        # Boolean flags get special treatment
                        if [[ "$key" == "debug" || "$key" == "override" || "$key" == "show-all-errors" ]]; then
                            value=true
                        else
                            value=""
                        fi
                    fi
                fi

                # Apply default if value is empty (except for boolean flags)
                if [[ -z "$value" && "$key" != "debug" && "$key" != "override" && "$key" != "show-all-errors" ]]; then
                    value="${defaults[$var_name]}"
                fi

                # Type validation
                case "$key" in
                    single-port|cluster-port|jobs)
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
echo "Show all errors: $show_all_errors"

# Run tests based on parameters
run_tests() {
  local mode=$1
  local port=$2
  local engine=$3

  echo
  echo "Running tests in $mode mode on port $port with $engine"
  echo

  if [[ "$debug" != "true" ]]; then
    timeout="--shutdown-timeout 60"
  fi
  if [[ "$override" == "true" ]]; then
    override_options="--override"
  fi
  if [[ "$show_all_errors" == "true" ]]; then
    show_all_errors_options="--show-all-errors"
  fi

  # Execute the command and capture the exit code
  sqllogictest "$test" \
    --host "$host" --port "$port" --engine "$engine" \
    --jobs "$jobs" \
    --label "$database" --label "$mode" --label "$engine-protocol" \
    --junit "$junit-$mode-$engine" \
    $override_options \
    $show_all_errors_options \
    $timeout
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
