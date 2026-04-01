#!/bin/bash

set -o pipefail

# Find the .deb package produced by the packaging step
DEB_PACKAGE="${DEB_PACKAGE:-$(ls "${WORKSPACE}"/serenedb_*.deb 2>/dev/null | head -1)}"

if [[ -z "$DEB_PACKAGE" ]]; then
	echo "ERROR: No .deb package found in ${WORKSPACE}"
	exit 1
fi

echo "RTA: using package $(basename "$DEB_PACKAGE")"

# Build the RTA image: minimal Ubuntu 24.04 with the .deb installed.
# invoke-rc.d is mocked during install to prevent postinst from trying
# to start the serenedb service (which has no init system in a container).
export RTA_IMAGE="serenedb-rta:$(LC_ALL=C tr -dc 'a-z0-9' </dev/urandom 2>/dev/null | head -c 8)"

cp "$DEB_PACKAGE" "${WORKSPACE}/serenedb-rta.deb"

docker build -t "$RTA_IMAGE" -f - "${WORKSPACE}" <<'DOCKERFILE'
FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive
COPY serenedb-rta.deb /tmp/serenedb.deb
RUN ln -sf /bin/true /usr/sbin/invoke-rc.d \
 && apt-get update \
 && apt-get install -y /tmp/serenedb.deb \
 && rm /usr/sbin/invoke-rc.d \
 && rm -rf /tmp/serenedb.deb /var/lib/apt/lists/*
DOCKERFILE

cleanup() {
	docker rmi "$RTA_IMAGE" 2>/dev/null || true
	rm -f "${WORKSPACE}/serenedb-rta.deb"
}
trap cleanup EXIT

if cd "${WORKSPACE}" && RTA_IMAGE="$RTA_IMAGE" ./tests/sqllogic/run_in_docker.sh rta 2>&1 | tee -a ./rta-tests.log; then
	test_result="PASSED"
	exit_code=0
else
	test_result="FAILED"
	exit_code=123
fi

echo "RTA_TESTS=${test_result}"
exit ${exit_code}
