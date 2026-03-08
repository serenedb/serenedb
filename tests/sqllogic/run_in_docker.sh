#!/bin/bash

# Docker Compose runner for sqllogic tests
# Usage:
#   ./run_in_docker.sh                    # Uses default 'sqllogic' kind
#   ./run_in_docker.sh recovery           # Uses 'recovery' kind
#   TEST_KIND=recovery ./run_in_docker.sh # Uses env variable

TEST_KIND=${1:-${TEST_KIND:-sqllogic}}

: "${BUILD_DIR:=build}"

if test -z "$SQLLOGIC_DIR"; then
	export SQLLOGIC_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
fi

if ! test -f "$SQLLOGIC_DIR/run_in_docker.sh"; then
	echo "SQLLOGIC_DIR is undefined or invalid"
	exit 255
fi

# In case of running locally
if test -z "$WORKSPACE"; then
	export WORKSPACE=$(realpath "$SQLLOGIC_DIR/../../")
fi

if ! test -f "$WORKSPACE/docker.env"; then
	touch "$WORKSPACE/docker.env"
fi

# Docker container assumes that sanitizers and coverage dirs are created.
# Executable 'serened' switches to 'serenedb' user. If required dirs are not created,
# /serenedb dir owner is 'root' and it's impossible for 'serenedb' user to create directory there.
mkdir -p "$WORKSPACE/sanitizers"
mkdir -p "$WORKSPACE/$BUILD_DIR/coverage"
mkdir -p "$WORKSPACE/logs"

if test -z "$BUILD_IMAGE"; then
	export BUILD_IMAGE=registry.serenedb.com:5000/serenedb-build-ubuntu:latest
fi

cd $SQLLOGIC_DIR

export BUILD_DIR

PREFIX="$(LC_ALL=C tr -dc 'a-z0-9' </dev/urandom 2>/dev/null | head -c 4)"

if [[ "$TEST_KIND" == "recovery" ]]; then
	SERVICE_NAME="${PREFIX}-serenedb-recovery-serened"
	NETWORK_NAME="${PREFIX}-serenedb-recovery-net"
	VOLUME_NAME="${PREFIX}-serenedb-recovery-datadir"

	docker swarm init 2>/dev/null || true

	cleanup() {
		docker service scale --detach=false "$SERVICE_NAME"=0
		docker service rm "$SERVICE_NAME"
		docker ps -a --filter "label=com.docker.swarm.service.name=$SERVICE_NAME" -q | xargs -r docker rm
		docker network rm "$NETWORK_NAME"
		docker volume rm "$VOLUME_NAME"
	}
	trap cleanup EXIT

	docker network create --driver overlay --attachable "$NETWORK_NAME"
	docker volume create "$VOLUME_NAME"

	docker service create \
		--name "$SERVICE_NAME" \
		--restart-condition on-failure \
		--replicas 1 \
		--restart-delay 1ns \
		--restart-max-attempts 1 \
		--restart-window 1ns \
		--network "$NETWORK_NAME" \
		--mount type=bind,src="$WORKSPACE",dst=/serenedb \
		--mount type=bind,src="$WORKSPACE/logs",dst=/var/log/serenedb \
		--mount type=volume,src="$VOLUME_NAME",dst=/serenedb_datadir \
		"${env_args[@]}" \
		"$BUILD_IMAGE" \
		sh -c '
      if ! id serenedb >/dev/null 2>&1; then
        useradd serenedb &&
        chown -R serenedb:serenedb /serenedb/sanitizers /serenedb/${BUILD_DIR}/coverage
      fi &&
      chown serenedb:serenedb /serenedb_datadir &&
      exec /serenedb/${BUILD_DIR}/bin/serened /serenedb_datadir \
        --server.endpoint pgsql+tcp://0.0.0.0:7777 \
        --server.authentication 0
    '

	# Run tests
	docker run --rm \
		--network "$NETWORK_NAME" \
		-e "SERVICE_HOST=$SERVICE_NAME" \
		-v "$SQLLOGIC_DIR:/sqllogic" \
		-v "$WORKSPACE/third_party/sqllogictest-rs:/sqllogictest-rs" \
		${BUILD_IMAGE} \
		bash -c 'exec sqllogic/run_recovery.sh'
	test_exit_code=$?

	if ! test "${test_exit_code}" -eq "0"; then
		echo "Recovery tests failed!"
		echo "serened service log begin:"
		docker service logs "$SERVICE_NAME"
		echo "serened service log end!"
	fi
else
	COMPOSE_FILE="docker-compose.$TEST_KIND.yml"
	# Validate that compose file exists
	if ! test -f "$SQLLOGIC_DIR/$COMPOSE_FILE"; then
		echo "Error: Unknown test kind '$TEST_KIND' - file '$COMPOSE_FILE' not found" >&2
		exit 255
	fi

	cleanup() {
		docker compose -p "${PREFIX}" -f "$COMPOSE_FILE" down --volumes --remove-orphans --timeout 30
	}
	trap cleanup EXIT INT TERM

	docker compose \
		-p "${PREFIX}" \
		-f "$COMPOSE_FILE" \
		up \
		--attach tests \
		--exit-code-from tests \
		--remove-orphans

	test_exit_code=$?

	if ! test "${test_exit_code}" -eq "0"; then
		echo "$TEST_KIND tests failed!"
		echo "serenedb-single container log begin:"
		docker compose -p "${PREFIX}" -f "$COMPOSE_FILE" logs serenedb-single
		echo "serenedb-single container log end!"
	fi
fi

exit "$test_exit_code"
