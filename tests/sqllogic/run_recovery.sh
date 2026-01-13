#!/bin/bash

# Schema of work:
# 1. Run serened server
# 2. Run first test with sqllogictest-rs
# 3. Reboot serened server if it's failed
# 4. Run second test with sqllogictest-rs

# Details
# - This script creates database itself using psql
# - sqllogictest-rs run without -j option, and pass created database with --db option

# Script usage is following
# ./tests/sqllogic/run_recovery.sh \
#   --path ./build \
#   --host 127.0.0.1 \
#   --port 6162 \
#   --first ./tests/sqllogic/recovery/first.test \
#   --second ./tests/sqllogic/recovery/second.test \
#   --datadir /tmp/datadir/

set -e
set -o pipefail

# Default values
declare -A defaults=(
    [path]='./build'
    [host]='127.0.0.1'
    [port]='6162'
    [first]=''
    [second]=''
    [database]='recovery_test_db'
    [runner]='./third_party/sqllogictest-rs'
    [timeout]='60'
    [datadir]=''
)

# Type validators
validate_number() {
    [[ "$1" =~ ^[0-9]+$ ]] || { echo "Error: $2 must be a number" >&2; return 1; }
}

validate_path() {
    [[ -n "$1" ]] || { echo "Error: $2 must not be empty" >&2; return 1; }
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
            path|host|port|first|second|database|runner|timeout|datadir)
                local var_name="${key//-/_}"  # Convert dashes to underscores

                # For non-equal format (--option value), get the next argument
                if ! $is_equal_format; then
                    if [ $# -ge 2 ] && [[ $2 != --* ]]; then
                        value="$2"
                        shift
                    else
                        value=""
                    fi
                fi

                # Apply default if value is empty
                if [[ -z "$value" ]]; then
                    value="${defaults[$var_name]}"
                fi

                # Type validation
                case "$key" in
                    port|timeout)
                        validate_number "$value" "--$key" || return 1
                        ;;
                    path|first|second)
                        validate_path "$value" "--$key" || return 1
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

# Parse command line arguments
parse_options "$@" || exit 1

# Apply defaults for any options not provided
for var_name in "${!defaults[@]}"; do
    if [[ -z "${!var_name}" ]]; then
        declare -g "$var_name"="${defaults[$var_name]}"
    fi
done

# Validate required arguments
if [[ -z "$first" || -z "$second" ]]; then
    echo "Error: both --first and --second test paths are required" >&2
    exit 1
fi

if [[ ! -f "$first" ]]; then
    echo "Error: first test file not found: $first" >&2
    exit 1
fi

if [[ ! -f "$second" ]]; then
    echo "Error: second test file not found: $second" >&2
    exit 1
fi

if [[ ! -d "$path" ]]; then
    echo "Error: build directory not found: $path" >&2
    exit 1
fi

# Create datadir if it doesn't exist
mkdir -p "$datadir"

if [[ ! -d "$datadir" ]]; then
    echo "Error: data directory not found: $datadir" >&2
    exit 1
fi



# Display configuration
echo "=== Recovery Test Configuration ==="
echo "Build path: $path"
echo "Data directory: $datadir"
echo "Host: $host"
echo "Port: $port"
echo "Database: $database"
echo "First test: $first"
echo "Second test: $second"
echo "Runner: $runner"
echo "Timeout: $timeout"
echo "===================================="
echo

# Install sqllogictest-rs if needed
echo "Installing sqllogictest-rs..."
cargo install --path "$runner/sqllogictest-bin" --quiet --force || {
    echo "Error: Failed to install sqllogictest-rs" >&2
    exit 1
}

# Server process PID
server_pid=""

# Cleanup function
cleanup() {
    echo "Cleaning up..."

    # Drop test database if it exists
    psql -h "$host" -p "$port" -U postgres -d postgres -c "DROP DATABASE IF EXISTS $database;" 2>/dev/null || true

    if [[ -n "$server_pid" ]]; then
        echo "Stopping server (PID: $server_pid)..."
        kill -TERM "$server_pid" 2>/dev/null || true
        sleep 1
        kill -KILL "$server_pid" 2>/dev/null || true
    fi
}

trap cleanup EXIT INT TERM

# Start server function
start_server() {
    echo "Starting serened server..."
    "$path/bin/serened" "$datadir" --server.endpoint pgsql+tcp://$host:$port &
    server_pid=$!
    echo "Server started with PID: $server_pid"
    
    # Wait for server to be ready
    local retries=30
    while [ $retries -gt 0 ]; do
        if psql -h "$host" -p "$port" -U postgres -d postgres -c "SELECT 1;" &>/dev/null; then
            echo "Server is ready"
            return 0
        fi
        echo "Waiting for server to be ready... ($retries retries left)"
        sleep 1
        retries=$((retries - 1))
    done
    
    echo "Error: Server failed to start within timeout" >&2
    return 1
}

# Stop server function
stop_server() {
    if [[ -n "$server_pid" ]]; then
        echo "Stopping server (PID: $server_pid)..."
        kill -TERM "$server_pid" || true
        
        # Wait for graceful shutdown
        local retries=10
        while [ $retries -gt 0 ] && kill -0 "$server_pid" 2>/dev/null; do
            sleep 1
            retries=$((retries - 1))
        done
        
        # Force kill if still running
        if kill -0 "$server_pid" 2>/dev/null; then
            echo "Force killing server..."
            kill -KILL "$server_pid" || true
        fi
        
        wait "$server_pid" 2>/dev/null || true
        server_pid=""
    fi
}

# Run test function
run_test() {
    local test_file="$1"
    local test_name="$2"
    
    echo
    echo "=== Running $test_name: $test_file ==="
    echo
    
    sqllogictest "$test_file" \
        --host "$host" \
        --port "$port" \
        --engine postgres \
        --label "$database" \
        --label "$test_name" \
        --db "$database" \
        --shutdown-timeout "$timeout"
    
    local exit_code=$?
    
    if [[ $exit_code -eq 0 ]]; then
        echo "✅ $test_name completed successfully"
    else
        echo "❌ $test_name failed with exit code: $exit_code"
    fi
    
    return $exit_code
}

# Main execution
final_exit_code=0

# Step 1: Start server
start_server || {
    echo "Error: Failed to start server" >&2
    exit 1
}

# Create test database
echo "Creating test database: $database..."
psql -h "$host" -p "$port" -U postgres -d postgres -c "DROP DATABASE $database;" || true
psql -h "$host" -p "$port" -U postgres -d postgres -c "CREATE DATABASE $database;" || {
    echo "Error: Failed to create database" >&2
    exit 1
}

# Step 2: Run first test
run_test "$first" "first-test"
first_test_exit_code=$?

sleep 1

# Step 3: Reboot server if it's not running
if ! kill -0 "$server_pid" 2>/dev/null; then
    echo
    echo "Server is not running, rebooting..."
    stop_server
    sleep 2
    start_server || {
        echo "Error: Failed to restart server" >&2
        exit 1
    }
else
    echo "Server is still running"
fi

if [[ $first_test_exit_code -ne 0 ]]; then
    final_exit_code=$first_test_exit_code
fi

# Step 4: Run second test
run_test "$second" "second-test"
second_test_exit_code=$?

if [[ $second_test_exit_code -ne 0 ]]; then
    final_exit_code=$second_test_exit_code
fi

# Final summary
echo
echo "=== Recovery Test Summary ==="
echo "First test exit code: $first_test_exit_code"
echo "Second test exit code: $second_test_exit_code"
echo "Final exit code: $final_exit_code"
echo "=============================="

exit $final_exit_code
