#!/bin/bash
set -euo pipefail

: "${RUNNER_ID:?RUNNER_ID is required (UID:GID)}"
: "${WORKSPACE:?WORKSPACE is required}"
: "${BUILD_IMAGE:?BUILD_IMAGE is required}"
# Update project's file ownership inside Docker container

CONTAINER_SCRIPT='
set -o pipefail

# Fix ownership for CI runner
chown "${RUNNER_ID}" -R /serene-ui
'

if
  docker run --rm \
    --ulimit core=-1 \
    --ulimit nofile=16384:16384 \
    --cap-add=SYS_PTRACE \
    --privileged \
    --security-opt seccomp=unconfined \
    -e RUNNER_ID="$RUNNER_ID" \
    -v "${WORKSPACE}:/serene-ui" \
    -v /etc/passwd:/etc/passwd:ro \
    -v /etc/group:/etc/group:ro \
    "$BUILD_IMAGE" \
    bash -c "${CONTAINER_SCRIPT}"
then
  echo "UPDATE_OWNERSHIP=PASSED"
else
  echo "UPDATE_OWNERSHIP=FAILED"
  exit 123
fi
