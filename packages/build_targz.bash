#!/bin/bash
set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/serenedb}"
source "${PROJECT_ROOT}/packages/find_version.bash"

VERSION="${SERENEDB_TGZ_UPSTREAM}"
ARCH=$(uname -m)

case "$ARCH" in
x86_64)
  arch="_$ARCH"
  ;;

*)
  if [[ "$ARCH" =~ ^arm64$|^aarch64$ ]]; then
    arch="_arm64"
  else
    echo "fatal, unknown architecture $ARCH for TGZ"
    exit 1
  fi
  ;;
esac

NAME="serenedb-${VERSION}-linux-${ARCH}"

cd "$PROJECT_ROOT"

# Strip
if [[ "${STRIP_TARBALL:-true}" == "true" ]]; then
  find install/usr -type f -executable -exec strip --strip-all {} \; 2>/dev/null || true
fi

# Create bin directory with symlinks to usr/sbin
mkdir -p install/bin
cd install/bin
ln -sf ../usr/sbin/serened serened
ln -sf ../usr/sbin/serene-init-database serene-init-database
ln -sf ../usr/sbin/serene-secure-installation serene-secure-installation
cd "$PROJECT_ROOT"

# Packaging - Transform usr/etc and usr/var to top level
tar -czvf "${NAME}.tar.gz" \
  --transform="s|^install/usr/etc|${NAME}/etc|" \
  --transform="s|^install/usr/var|${NAME}/var|" \
  --transform="s|^install/usr|${NAME}/usr|" \
  --transform="s|^install/bin|${NAME}/bin|" \
  install/usr/ \
  install/bin/

# Cleanup
rm -rf install/bin/

echo "Created: ${NAME}.tar.gz ($(du -h "${NAME}.tar.gz" | cut -f1))"

# Create symlink for follow-up docker image production step
mkdir -p "${PROJECT_ROOT}/packages/tarball"
cd "${PROJECT_ROOT}/packages/tarball"
ln -sf "../../${NAME}.tar.gz" "install.tar.gz"
