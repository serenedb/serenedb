#!/bin/bash
set -e  # Stop on error
set -o pipefail

echo "ℹ️ Starting serene-ui Docker container..."
docker compose -f docker-compose.test-native.yaml up --build -d
sleep 10  # Wait for it to start

echo "ℹ️ Running backend tests..."
npm run --prefix apps/backend test
echo "✅ Backend tests completed"

echo "ℹ️ Running storybook tests..."
npm run --prefix apps/web test-storybook
echo "✅ Storybook tests completed"

echo "ℹ️ Stopping serene-ui Docker container..."
docker compose -f docker-compose.test-native.yaml down
echo "✅ Serene-ui Docker container stopped"

echo "✅ All tests completed successfully"