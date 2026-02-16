#!/usr/bin/env bash
set -euo pipefail

FAILED=0
FAILED_LOGS=()

run_native_test() {
  local name="$1"
  local cmd="$2"
  local log_file
  log_file="$(mktemp)"

  echo "🧪 Running $name..."
  if bash -lc "$cmd" >"$log_file" 2>&1; then
    echo "✅ $name passed"
    rm -f "$log_file"
    return 0
  fi

  echo "❌ $name failed"
  FAILED=1
  FAILED_LOGS+=("$name:$log_file")
  return 0
}

run_native_test "backend tests" "npm run --prefix $(pwd)/apps/backend test"
run_native_test "storybook tests" "npm run --prefix $(pwd)/apps/web test-storybook"

if [[ "$FAILED" -ne 0 ]]; then
  for entry in "${FAILED_LOGS[@]}"; do
    name="${entry%%:*}"
    log_file="${entry#*:}"
    echo "----- $name logs -----"
    cat "$log_file"
    rm -f "$log_file"
  done
  exit 1
fi

echo "✅ All tests passed"
