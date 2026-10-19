#!/bin/bash

if docker run --rm \
	${SDB_DOCKER_OWNER:+--label "serenedb.owner=$SDB_DOCKER_OWNER"} \
	--user "$(id -u):$(id -g)" \
	-e HOME=/serenedb \
	--cap-add=SYS_PTRACE \
	--security-opt seccomp=unconfined \
	--env-file ./docker.env \
	-v "${WORKSPACE}:/serenedb" \
	-v /mnt/data/.ccache:/.ccache \
	"${BUILD_IMAGE}" /serenedb/scripts/ci/build_install.bash; then
	echo "SDB_BUILD=PASSED"
else
	echo "SDB_BUILD=FAILED"
	exit 1
fi

mkdir -p cores
