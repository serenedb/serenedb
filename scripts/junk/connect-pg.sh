#!/bin/bash

# Script to launch PostgreSQL in Docker and connect via psql
set -e

CONTAINER_NAME="postgres-latest"
POSTGRES_VERSION="18.1"
POSTGRES_USER="postgres"
POSTGRES_PASSWORD="postgres"
POSTGRES_DB="postgres"
PORT="5432"

# Function to check if container is running
is_container_running() {
    docker ps --filter "name=$CONTAINER_NAME" --filter "status=running" --format "{{.Names}}" | grep -q "$CONTAINER_NAME"
}

# Function to check if container exists (including stopped ones)
container_exists() {
    docker ps -a --filter "name=$CONTAINER_NAME" --format "{{.Names}}" | grep -q "$CONTAINER_NAME"
}

# Function to start container
start_container() {
    echo "Starting PostgreSQL container..."
    docker run -d \
        --name "$CONTAINER_NAME" \
        -e POSTGRES_USER="$POSTGRES_USER" \
        -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
        -e POSTGRES_DB="$POSTGRES_DB" \
        -p "$PORT":5432 \
        postgres:"$POSTGRES_VERSION"
}

# Function to start existing container
start_existing_container() {
    echo "Starting existing container..."
    docker start "$CONTAINER_NAME"
}

# Function to wait for PostgreSQL to be ready
wait_for_postgres() {
    echo "Waiting for PostgreSQL to be ready..."
    local max_attempts=30
    local attempt=1

    while [ $attempt -le $max_attempts ]; do
        if docker exec "$CONTAINER_NAME" pg_isready -U "$POSTGRES_USER" > /dev/null 2>&1; then
            echo "PostgreSQL is ready!"
            return 0
        fi
        echo "Attempt $attempt/$max_attempts: PostgreSQL not ready yet..."
        sleep 2
        ((attempt++))
    done

    echo "Failed to connect to PostgreSQL after $max_attempts attempts"
    return 1
}

# Main script logic
echo "Checking PostgreSQL container status..."

if is_container_running; then
    echo "PostgreSQL container is already running."
else
    if container_exists; then
        echo "Container exists but is not running."
        start_existing_container
    else
        echo "Container does not exist. Creating new container..."
        start_container
    fi

    # Wait for PostgreSQL to be ready
    wait_for_postgres
fi

# Connect to PostgreSQL
echo "Connecting to PostgreSQL..."
docker exec -it "$CONTAINER_NAME" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"
