#!/bin/bash

WORKDIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)/..
cd "$WORKDIR"

BUILD_H="${WORKDIR}/build/libs/basics/build.h"

if [[ ! -f "$BUILD_H" ]]; then
	echo "Error: $BUILD_H not found. Run cmake build first." >&2
	exit 1
fi

_get() { grep -m1 "^#define $1 " "$BUILD_H" | sed 's/.*"\(.*\)".*/\1/'; }

export SERENEDB_VERSION_MAJOR=$(_get SERENEDB_VERSION_MAJOR)
export SERENEDB_VERSION_MINOR=$(_get SERENEDB_VERSION_MINOR)
export SERENEDB_VERSION_PATCH=$(_get SERENEDB_VERSION_PATCH)
export SERENEDB_VERSION_RELEASE_TYPE=$(_get SERENEDB_VERSION_RELEASE_TYPE)
export SERENEDB_VERSION=$(_get SERENEDB_VERSION)

export SERENEDB_SNIPPETS="$SERENEDB_VERSION_MAJOR.$SERENEDB_VERSION_MINOR"
export SERENEDB_PACKAGES="$SERENEDB_VERSION_MAJOR.$SERENEDB_VERSION_MINOR"
export SERENEDB_DEBIAN_UPSTREAM="$SERENEDB_VERSION"
export SERENEDB_TGZ_UPSTREAM="$SERENEDB_VERSION"
export DOCKER_TAG="$SERENEDB_VERSION"
export SERENEDB_DEBIAN_REVISION="1"

if [[ -n "${DOCKER_DISTRO:-}" ]] && [[ "${DOCKER_DISTRO:-}" != "alpine" ]]; then
	export DOCKER_TAG="${DOCKER_TAG}-${DOCKER_DISTRO}"
fi

echo '------------------------------------------------------------------------------'
echo "SERENEDB_VERSION:                  $SERENEDB_VERSION"
echo
echo "SERENEDB_VERSION_MAJOR:            $SERENEDB_VERSION_MAJOR"
echo "SERENEDB_VERSION_MINOR:            $SERENEDB_VERSION_MINOR"
echo "SERENEDB_VERSION_PATCH:            $SERENEDB_VERSION_PATCH"
echo "SERENEDB_VERSION_RELEASE_TYPE:     $SERENEDB_VERSION_RELEASE_TYPE"
echo
echo "SERENEDB_DEBIAN_UPSTREAM/REVISION: $SERENEDB_DEBIAN_UPSTREAM / $SERENEDB_DEBIAN_REVISION"
echo "SERENEDB_PACKAGES:                 $SERENEDB_PACKAGES"
echo "SERENEDB_SNIPPETS:                 $SERENEDB_SNIPPETS"
echo "SERENEDB_TGZ_UPSTREAM:             $SERENEDB_TGZ_UPSTREAM"
echo "DOCKER_TAG:                        $DOCKER_TAG"
echo '------------------------------------------------------------------------------'
echo
