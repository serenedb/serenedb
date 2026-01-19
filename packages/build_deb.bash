#!/bin/bash
#
# build-deb.sh - Build SereneDB Debian Package
#
set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/serenedb}"
cd "$PROJECT_ROOT"

source packages/find_version.bash

# Configuration
VERSION="${SERENEDB_DEBIAN_UPSTREAM}-${SERENEDB_DEBIAN_REVISION}"
EDITION="serenedb"
ARCH="$(dpkg --print-architecture)"
BASE="package_all"
SOURCE="packages/debian/source"
TARGET="${BASE}/debian"

echo "==> Building SereneDB DEB package (v${VERSION}, ${ARCH})..."

# Clean and prepare
rm -rf "$BASE"
mkdir -p "$TARGET"

# Copy files
cp -a "${SOURCE}/main/." "$TARGET/"
cp -a "install_all" "${BASE}/install"

# Process templates
for f in serenedb.init serenedb.service compat config templates preinst prerm postinst postrm rules; do
  [[ -f "${SOURCE}/common/$f" ]] && {
    cp "${SOURCE}/common/$f" "${TARGET}/$f"
    sed -i "s/@EDITION@/${EDITION}/g" "${TARGET}/$f"
  }
done

# Copy source format
cp -a "${SOURCE}/common/source" "$TARGET/"

# Generate changelog
{
  echo "${EDITION} (${VERSION}) UNRELEASED; urgency=medium"
  echo ""
  echo "  * New version."
  echo ""
  echo -n " -- SereneDB  "
  date -R
} >"${TARGET}/changelog"

# Set architecture
sed -i "s/@ARCHITECTURE@/${ARCH}/g" "${TARGET}/control"

# Build (clean locale environment)
unset LC_ALL LC_CTYPE LANG LANGUAGE
export LC_ALL=C

cd "$BASE"
debian/rules binary

echo "==> Build complete!"
