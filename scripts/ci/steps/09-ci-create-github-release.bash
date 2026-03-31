#!/bin/bash

set -euo pipefail

source "${WORKSPACE}/packages/find_version.bash"
TAG="v${DOCKER_TAG}"

if [[ "$SERENEDB_VERSION_RELEASE_TYPE" == "lts" ]]; then
	TITLE="SereneDB ${TAG} LTS"
else
	TITLE="SereneDB ${TAG}"
fi

BODY="Docker image: https://hub.docker.com/r/serenedb/serenedb/tags?name=${DOCKER_TAG}"

gh release create "${TAG}" \
	--draft \
	--title "${TITLE}" \
	--notes "${BODY}" \
	serenedb_*.deb \
	serenedb-*.tar.gz
