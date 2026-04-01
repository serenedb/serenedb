#!/bin/bash
set -eo pipefail

if [[ -z "$DEB_TAR_PACKAGES" ]]; then
	DEB_TAR_PACKAGES="false"
fi

# Trim the environment variable for comparing
DEB_TAR_PACKAGES=$(echo "$DEB_TAR_PACKAGES" | tr -d '[:space:]')

BUILDIR="/serenedb/build"
INSTDIR="/serenedb/install"
PACKDIR="/serenedb/install_all"

# Check that build was run
if [[ ! -d "$BUILDIR" ]]; then
	echo "Error: Build directory $BUILDIR does not exist."
	echo "Run build.bash first."
	exit 1
fi

if [[ ! -f "$BUILDIR/build.ninja" ]]; then
	echo "Error: $BUILDIR/build.ninja not found."
	echo "Run build.bash first."
	exit 1
fi

if [[ ! -f "$BUILDIR/bin/serened" ]]; then
	echo "Error: $BUILDIR/bin/serened not found."
	echo "Run build.bash with 'serened' target first."
	exit 1
fi

cd /serenedb
git config --global --add safe.directory '*'

cd "$BUILDIR"

rm -rf "$INSTDIR" "$PACKDIR"
mkdir -p "$INSTDIR" "$PACKDIR"

echo "DESTDIR=$INSTDIR ninja install" | tee /serenedb/install.log
DESTDIR="$INSTDIR" ninja install 2>&1 | tee -a /serenedb/install.log || exit 1

cp -r "$INSTDIR"/* "$PACKDIR"

if [ "$DEB_TAR_PACKAGES" = "true" ]; then
	/serenedb/packages/build_deb.bash || exit 1
	/serenedb/packages/build_targz.bash || exit 1
fi
