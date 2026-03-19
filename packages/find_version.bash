#!/bin/bash

WORKDIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)/..
cd $WORKDIR

CMAKELIST="CMakeLists.txt"
AV="set(SERENEDB_VERSION"

SEDFIX='s/.*"\([0-9a-zA-Z]*\)".*$/\1/'

export SERENEDB_VERSION_MAJOR=$(grep "$AV""_MAJOR" $CMAKELIST | sed -e $SEDFIX)
export SERENEDB_VERSION_MINOR=$(grep "$AV""_MINOR" $CMAKELIST | sed -e $SEDFIX)
export SERENEDB_VERSION_PATCH=$(grep "$AV""_PATCH" $CMAKELIST | grep -v unset | sed -e $SEDFIX)
export SERENEDB_VERSION_RELEASE_TYPE=$(grep "$AV""_RELEASE_TYPE" $CMAKELIST | grep -v unset | sed -e $SEDFIX)

export SERENEDB_SNIPPETS="$SERENEDB_VERSION_MAJOR.$SERENEDB_VERSION_MINOR"
export SERENEDB_PACKAGES="$SERENEDB_VERSION_MAJOR.$SERENEDB_VERSION_MINOR"

BASE="$SERENEDB_VERSION_MAJOR.$SERENEDB_VERSION_MINOR.$SERENEDB_VERSION_PATCH"

if [[ "$SERENEDB_VERSION_RELEASE_TYPE" == "lts" ]]; then
	export SERENEDB_VERSION="$BASE-lts"
	export SERENEDB_DEBIAN_UPSTREAM="$BASE~lts"
	export SERENEDB_TGZ_UPSTREAM="$BASE-lts"
	export DOCKER_TAG="$BASE-lts"
else
	export SERENEDB_VERSION="$BASE"
	export SERENEDB_DEBIAN_UPSTREAM="$BASE"
	export SERENEDB_TGZ_UPSTREAM="$BASE"
	export DOCKER_TAG="$BASE"
fi

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
