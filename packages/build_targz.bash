#!/bin/bash
set -euo pipefail

PROJECT_ROOT="${PROJECT_ROOT:-/serenedb}"
source "${PROJECT_ROOT}/packages/find_version.bash"

VERSION="${SERENEDB_VERSION}"

case "$(uname -m)" in
x86_64) ARCH="amd64" ;;
aarch64 | arm64) ARCH="arm64" ;;
*)
	echo "fatal, unknown architecture $(uname -m) for TGZ"
	exit 1
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

# Collect dirs to package
TAR_DIRS=(install/usr/)
for d in install/etc install/var; do
	[ -d "$d" ] && TAR_DIRS+=("$d/")
done

# Packaging
tar -czf "${NAME}.tar.gz" \
	--exclude="install/usr/lib/debug" \
	--transform="s|^install/usr/etc|${NAME}/etc|" \
	--transform="s|^install/usr/var|${NAME}/var|" \
	--transform="s|^install/|${NAME}/|" \
	"${TAR_DIRS[@]}"

# Package debug symbols
if [[ "${DEBUG_SYMBOLS:-false}" == "true" ]]; then
	tar -czf "${NAME}-dbgsym.tar.gz" \
		--transform="s|^install/usr/lib/debug|${NAME}-dbgsym|" \
		install/usr/lib/debug/
fi

# Cleanup
rm -rf install/usr/lib/debug

echo "Created: ${NAME}.tar.gz ($(du -h "${NAME}.tar.gz" | cut -f1))"
if [[ "${DEBUG_SYMBOLS:-false}" == "true" ]]; then
	echo "Created: ${NAME}-dbgsym.tar.gz ($(du -h "${NAME}-dbgsym.tar.gz" | cut -f1))"
fi

# Create symlink for multi-arch build_docker.bash
mkdir -p "${PROJECT_ROOT}/packages/tarball"
ln -sf "../../${NAME}.tar.gz" "${PROJECT_ROOT}/packages/tarball/install-${ARCH}.tar.gz"
