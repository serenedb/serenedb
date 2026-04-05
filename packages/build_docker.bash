#!/bin/bash

# Builds Docker images for available architectures and loads them into the local daemon.
# Does NOT push -- use push_docker.bash for that.
#
# Environment Configuration:
#   DOCKER_TAG               Version tag (if unset, derived from find_version.bash)
#   DOCKER_REGISTRY          Registry prefix (default: serenedb)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DOCKER_DIR="${SCRIPT_DIR}/docker"
TARBALL_DIR="${SCRIPT_DIR}/tarball"
IMAGE_NAME="serenedb"
BUILDER_NAME="serenedb-image-builder"

: "${DOCKER_REGISTRY:=serenedb}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }
error() {
	echo "[ERROR] $*" >&2
	exit 1
}

cleanup() {
	rm -rf "${BUILD_DIR:-}"
	rm -f /tmp/serenedb-amd64.tar /tmp/serenedb-arm64.tar
	docker buildx rm "${BUILDER_NAME}" 2>/dev/null || true
}

# --- Detect host architecture ---
RAW_ARCH=$(uname -m)
case $RAW_ARCH in
x86_64) HOST_ARCH="amd64" ;;
aarch64) HOST_ARCH="arm64" ;;
*) error "Unsupported architecture: $RAW_ARCH" ;;
esac

# --- Determine version ---
if [ -z "${DOCKER_TAG:-}" ]; then
	source "${SCRIPT_DIR}/find_version.bash"
	DOCKER_TAG="${SERENEDB_VERSION}"
fi
[ -z "${DOCKER_TAG:-}" ] && error "Failed to determine version"
VERSION="$DOCKER_TAG"
FULL_IMAGE_NAME="${DOCKER_REGISTRY}/${IMAGE_NAME}"

# --- Detect available arch tarballs ---
AVAILABLE_ARCHES=()
for arch in amd64 arm64; do
	tarball="${TARBALL_DIR}/install-${arch}.tar.gz"
	if [ -f "$tarball" ] || [ -L "$tarball" ]; then
		AVAILABLE_ARCHES+=("$arch")
	fi
done

if [ ${#AVAILABLE_ARCHES[@]} -eq 0 ]; then
	error "No install-{arch}.tar.gz found in ${TARBALL_DIR}. Run build_targz.bash first."
fi

# --- Prepare build context ---
BUILD_DIR=$(mktemp -d)
trap cleanup EXIT INT TERM

log "=== SereneDB Docker Build ==="
log "Version:  ${VERSION}"
log "Image:    ${FULL_IMAGE_NAME}"
log "Arches:   ${AVAILABLE_ARCHES[*]}"

cp "${DOCKER_DIR}/Dockerfile" "${BUILD_DIR}/"
cp "${DOCKER_DIR}/entrypoint.sh" "${BUILD_DIR}/"

for arch in "${AVAILABLE_ARCHES[@]}"; do
	cp "${TARBALL_DIR}/install-${arch}.tar.gz" "${BUILD_DIR}/install-${arch}.tar.gz"
	log "  Tarball (${arch}): $(du -h "${BUILD_DIR}/install-${arch}.tar.gz" | cut -f1)"
done

log "  Context size: $(du -sh "${BUILD_DIR}" | cut -f1)"

# --- Setup buildx ---
log "Configuring Docker Buildx..."
if docker buildx inspect "$BUILDER_NAME" >/dev/null 2>&1; then
	docker buildx rm "$BUILDER_NAME" >/dev/null
fi
docker buildx create --name "$BUILDER_NAME" --use --driver-opt network=host >/dev/null

LABEL_ARGS=(
	--label "org.opencontainers.image.version=${VERSION}"
	--label "org.opencontainers.image.created=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
	--label "org.opencontainers.image.revision=$(git rev-parse HEAD 2>/dev/null || echo 'unknown')"
)

# --- Build each arch and load into daemon ---
for arch in "${AVAILABLE_ARCHES[@]}"; do
	log "Building linux/${arch}..."
	docker buildx build \
		--platform "linux/${arch}" \
		-t "${FULL_IMAGE_NAME}:${VERSION}-${arch}" \
		"${LABEL_ARGS[@]}" \
		--output "type=docker,dest=/tmp/serenedb-${arch}.tar" \
		--no-cache \
		--file "${BUILD_DIR}/Dockerfile" \
		"${BUILD_DIR}"
done

log "Loading images..."
for arch in "${AVAILABLE_ARCHES[@]}"; do
	docker load </tmp/serenedb-${arch}.tar
	rm -f "/tmp/serenedb-${arch}.tar"
done

# --- Smoke test (native arch only) ---
log "=== Smoke test ==="
SMOKE_ARCH="${AVAILABLE_ARCHES[0]}"
if docker run --rm "${FULL_IMAGE_NAME}:${VERSION}-${SMOKE_ARCH}" --version 2>/dev/null; then
	log "  Version check passed (${SMOKE_ARCH})"
else
	if [ "$SMOKE_ARCH" != "$HOST_ARCH" ]; then
		log "  Skipping smoke test: built ${SMOKE_ARCH} but host is ${HOST_ARCH}"
	else
		error "  Version check failed"
	fi
fi

# --- Summary ---
log ""
log "=== Build complete ==="
log "Images in local daemon:"
for arch in "${AVAILABLE_ARCHES[@]}"; do
	log "  ${FULL_IMAGE_NAME}:${VERSION}-${arch}"
done
log ""
log "To test:  docker run -d -p 7890:7890 ${FULL_IMAGE_NAME}:${VERSION}-${HOST_ARCH}"
log "To push:  DOCKER_TAG=${VERSION} ./packages/push_docker.bash"
