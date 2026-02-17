#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="docker-compose.test-docker.yaml"
MAX_ATTEMPTS=30

cleanup() {
  echo "🧹 Tearing down..."
  docker compose -f "$COMPOSE_FILE" down --volumes --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT

wait_for_service() {
  echo "⏳ Waiting for serene-ui..."
  for i in $(seq 1 "$MAX_ATTEMPTS"); do
    container_id="$(docker compose -f "$COMPOSE_FILE" ps -q serene-ui || true)"
    if [[ -n "$container_id" ]] && [[ "$(docker inspect -f '{{.State.Running}}' "$container_id" 2>/dev/null || true)" == "true" ]]; then
      return 0
    fi
    echo "  attempt $i/$MAX_ATTEMPTS..."
    sleep 2
  done
  echo "❌ serene-ui failed to start"
  exit 1
}

echo "🚀 Starting services..."
docker compose -f "$COMPOSE_FILE" up --build -d

wait_for_service

FAILED=0
FAILED_LOGS=()

run_docker_test() {
  local name="$1"
  local cmd="$2"
  local log_file
  log_file="$(mktemp)"

  echo "🧪 Running $name..."
  if docker compose -f "$COMPOSE_FILE" exec -T serene-ui sh -c "$cmd" >"$log_file" 2>&1; then
    echo "✅ $name passed"
    rm -f "$log_file"
    return 0
  fi

  echo "❌ $name failed"
  FAILED=1
  FAILED_LOGS+=("$name:$log_file")
  return 0
}

run_docker_test "backend tests" "npm run --prefix /test-app/apps/backend test"
run_docker_test "storybook tests" "npm run --prefix /test-app/apps/web test-storybook"

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
