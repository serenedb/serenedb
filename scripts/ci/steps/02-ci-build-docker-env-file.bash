#!/bin/bash
# Creates a docker.env file from environment variables and pulls the build image.

set -e

# Use a heredoc to create the env file efficiently
cat >./docker.env <<EOF
CCACHE_MAXSIZE=${CCACHE_MAXSIZE:-100G}
BUILDMODE=${BUILDMODE}
USE_DEBUG_INFO=${USE_DEBUG_INFO}
SDB_DEV=${SDB_DEV}
USE_IPO=${USE_IPO}
SDB_ALLOC=${SDB_ALLOC}
ENSURE_VTUNE_SYMBOLS=${ENSURE_VTUNE_SYMBOLS}
SANITIZERS=${SANITIZERS}
USE_COVERAGE=${USE_COVERAGE}
DEB_TAR_PACKAGES=${DEB_TAR_PACKAGES}
STRIP_TARBALL=${STRIP_TARBALL}
DEBUG_SYMBOLS=${DEBUG_SYMBOLS}
SERENEDB_VERSION_PATCH=${SERENEDB_VERSION_PATCH:-0}
SDB_FUZZ_QUERIES=${SDB_FUZZ_QUERIES:-}
SDB_FUZZ_PARALLEL=${SDB_FUZZ_PARALLEL:-}
EOF

if test -f ./san_options.env; then
	cat ./san_options.env >>./docker.env
	rm ./san_options.env
fi

echo "--- Generated docker.env ---"
cat ./docker.env
echo "--------------------------"

# Pull the image with retries to handle transient VPN/registry issues
for i in 1 2 3; do
	docker pull "${BUILD_IMAGE}" && break
	echo "Pull attempt $i failed, retrying in 10s..."
	sleep 10
done
