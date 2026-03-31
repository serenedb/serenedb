#!/bin/bash

if docker run --rm \
	--cap-add=SYS_PTRACE \
	--privileged \
	--security-opt seccomp=unconfined \
	--env-file ./docker.env \
	-v "${WORKSPACE}:/serenedb" \
	-v /mnt/data/.ccache:/.ccache \
	-v /etc/passwd:/etc/passwd:ro \
	-v /etc/group:/etc/group:ro \
	"${BUILD_IMAGE}" /serenedb/scripts/ci/build_install.bash; then
	echo "SDB_BUILD=PASSED"
else
	echo "SDB_BUILD=FAILED"
	exit 1
fi

mkdir -p cores
