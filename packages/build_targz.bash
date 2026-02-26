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

# Extract debug symbols and strip
# -print0/-d '': null-delimited I/O, IFS=/-r: preserve paths verbatim
if [[ "${STRIP_TARBALL:-true}" == "true" ]]; then
  if [[ "${DEBUG_SYMBOLS:-false}" == "true" ]]; then
    mkdir -p install/usr/lib/debug
  fi
  find install/usr -type f -executable -print0 | while IFS= read -r -d '' bin; do
    if [[ "${DEBUG_SYMBOLS:-false}" == "true" ]]; then
      dbg="install/usr/lib/debug/$(basename "$bin").dbg"
      objcopy --only-keep-debug "$bin" "$dbg" 2>/dev/null || continue
    fi
    strip --strip-all "$bin" 2>/dev/null || true
    if [[ "${DEBUG_SYMBOLS:-false}" == "true" ]]; then
      objcopy --add-gnu-debuglink="$dbg" "$bin" 2>/dev/null || true
    fi
  done
fi

# Create bin directory with symlinks to usr/sbin
mkdir -p install/bin
cd install/bin
ln -sf ../usr/sbin/serened serened
cd "$PROJECT_ROOT"

# Packaging - Transform usr/etc and usr/var to top level
tar -czvf "${NAME}.tar.gz" \
  --exclude="install/usr/lib/debug" \
  --transform="s|^install/usr/etc|${NAME}/etc|" \
  --transform="s|^install/usr/var|${NAME}/var|" \
  --transform="s|^install/usr|${NAME}/usr|" \
  --transform="s|^install/bin|${NAME}/bin|" \
  install/usr/ \
  install/bin/

# Package debug symbols
if [[ "${DEBUG_SYMBOLS:-false}" == "true" ]]; then
  tar -czvf "${NAME}-dbgsym.tar.gz" \
    --transform="s|^install/usr/lib/debug|${NAME}-dbgsym|" \
    install/usr/lib/debug/
fi

# Cleanup
rm -rf install/bin/ install/usr/lib/debug

echo "Created: ${NAME}.tar.gz ($(du -h "${NAME}.tar.gz" | cut -f1))"
if [[ "${DEBUG_SYMBOLS:-false}" == "true" ]]; then
  echo "Created: ${NAME}-dbgsym.tar.gz ($(du -h "${NAME}-dbgsym.tar.gz" | cut -f1))"
fi

# Create symlink for follow-up docker image production step
mkdir -p "${PROJECT_ROOT}/packages/tarball"
cd "${PROJECT_ROOT}/packages/tarball"
ln -sf "../../${NAME}.tar.gz" "install.tar.gz"
