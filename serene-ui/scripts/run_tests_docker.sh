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
    docker compose -f "$COMPOSE_FILE" exec -T serene-ui \
      wget -qO /dev/null http://localhost:3000 2>/dev/null && return 0
    echo "  attempt $i/$MAX_ATTEMPTS..."
    sleep 2
  done
  echo "❌ serene-ui failed to start"
  exit 1
}

echo "🚀 Starting services..."
docker compose -f "$COMPOSE_FILE" up --build -d

wait_for_service

echo "🧪 Running tests..."
docker compose -f "$COMPOSE_FILE" exec -T serene-ui sh -c '
  npm run --prefix /test-app/apps/backend test &&
  npm run --prefix /test-app/apps/web test-storybook
'

echo "✅ All tests passed"
