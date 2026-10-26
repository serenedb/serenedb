#!/bin/bash
set -e
set -o pipefail

COMPOSE_FILE="docker-compose.test.yaml"

cleanup() {
	echo "Stopping serene-ui Docker container..."
	docker compose -f "$COMPOSE_FILE" down --volumes --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "Starting serene-ui Docker container..."
docker compose -f "$COMPOSE_FILE" up --build -d serene-ui
sleep 10

echo "Running backend tests..."
docker exec serene-ui npm run --prefix /test-app/apps/backend test
echo "Backend tests completed"

echo "Running storybook tests..."
docker exec serene-ui npm run --prefix /test-app/apps/web test-storybook
echo "Storybook tests completed"

echo "All tests completed successfully"
