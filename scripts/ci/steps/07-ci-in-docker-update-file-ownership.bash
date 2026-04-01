#!/bin/bash
set -euo pipefail

# Update project's file ownership inside Docker container

CONTAINER_SCRIPT='
set -o pipefail

# Fix ownership for CI runner
chown "${RUNNER_ID}" -R /serenedb
'

if docker run --rm \
	--ulimit core=-1 \
	--ulimit nofile=16384:16384 \
	--cap-add=SYS_PTRACE \
	--privileged \
	--security-opt seccomp=unconfined \
	--env-file ./docker.env \
	-v "${WORKSPACE}:/serenedb" \
	-v /etc/passwd:/etc/passwd:ro \
	-v /etc/group:/etc/group:ro \
	"${BUILD_IMAGE}" \
	bash -c "${CONTAINER_SCRIPT}"; then
	echo "UPDATE_OWNERSHIP=PASSED"
else
	echo "UPDATE_OWNERSHIP=FAILED"
	exit 123
fi
