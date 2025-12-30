#!/bin/bash
set -e  # Stop on error
set -o pipefail

echo "ℹ️ Starting serene-ui Docker container..."
docker compose -f docker-compose.test.yaml up --build -d serene-ui
sleep 10  # Wait for it to start

echo "ℹ️ Generating screenshots..."
docker exec serene-ui npm run --prefix /test-app/apps/web test-storybook:renew
echo "✅ Screenshots generated successfully"

echo "ℹ️ Stopping serene-ui Docker container..."
docker compose -f docker-compose.test.yaml down
echo "✅ Serene-ui Docker container stopped"