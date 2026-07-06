#!/bin/bash

# Run network tests (HBA end-to-end mask/CIDR matching over real sockets). The
# test starts its own serened and connects from varied 127.x source addresses,
# so it runs in-container on the same host as the server (loopback is always
# available inside the container).
if ! docker run --rm \
	--user "$(id -u):$(id -g)" \
	-e HOME=/serenedb \
	--ulimit core=-1 \
	--ulimit nofile=16384:16384 \
	--cap-add=SYS_PTRACE \
	--security-opt seccomp=unconfined \
	--env-file ./docker.env \
	-e BUILD_DIR="${BUILD_DIR}" \
	-v "${WORKSPACE}:/serenedb" \
	"${BUILD_IMAGE}" \
	bash -c '
    set -o pipefail
    cd /serenedb
    WORKSPACE=/serenedb BUILD_DIR="${BUILD_DIR}" ./tests/network/run.sh 2>&1 | tee -a /serenedb/out/logs/network-tests.log
  '; then
	echo "NETWORK_TESTS=FAILED"
	exit 123
fi
echo "NETWORK_TESTS=PASSED"
