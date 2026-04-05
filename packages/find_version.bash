#!/bin/bash

WORKDIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)/..
cd "$WORKDIR"

BUILD_H="${WORKDIR}/build/libs/basics/build.h"

if [[ ! -f "$BUILD_H" ]]; then
	echo "Error: $BUILD_H not found. Run cmake build first." >&2
	exit 1
fi

_get() { grep -m1 "^#define $1 " "$BUILD_H" | sed 's/.*"\(.*\)".*/\1/'; }

export SERENEDB_VERSION=$(_get SERENEDB_VERSION)

echo "SereneDB version: $SERENEDB_VERSION"
