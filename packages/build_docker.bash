#!/bin/bash

# Environment Configuration:
#   DOCKER_TAG               Set version (if unset, derived from find_version.bash)
#   DOCKER_EXTRA_TAGS        Comma or space-separated list of additional tags
#   PUSH_IMAGES_2_REGISTRY   'true' to push multi-arch to registry
#   DOCKER_REGISTRY          Registry URL (default: serenedb)
#   DOCKER_USERNAME          Registry username (for push)
#   DOCKER_PASSWORD          Registry password (for push)

set -e

# Configuration from Environment Variables
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DOCKER_DIR="${SCRIPT_DIR}/docker"
TARBALL_DIR="${SCRIPT_DIR}/tarball"
IMAGE_NAME="serenedb"
BUILDER_NAME="serenedb-image-builder"

: "${DOCKER_REGISTRY:=serenedb}"
: "${PUSH_IMAGES_2_REGISTRY:=false}"

IFS=', ' read -r -a EXTRA_TAGS_ARRAY <<<"${DOCKER_EXTRA_TAGS:-}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }
error() {
	echo "[ERROR] $*" >&2
	exit 1
}

NEEDS_LOGOUT=""
cleanup() {
	rm -rf "${BUILD_DIR:-}"
	rm -f /tmp/serenedb-amd64.tar /tmp/serenedb-arm64.tar
	docker buildx rm "${BUILDER_NAME}" 2>/dev/null || true
	if [ -n "$NEEDS_LOGOUT" ]; then
		if [ "$NEEDS_LOGOUT" = "hub" ]; then
			docker logout 2>/dev/null || true
		else
			docker logout "$NEEDS_LOGOUT" 2>/dev/null || true
		fi
	fi
}

# --- Detect host architecture ---
RAW_ARCH=$(uname -m)
case $RAW_ARCH in
x86_64) HOST_ARCH="amd64" ;;
aarch64) HOST_ARCH="arm64" ;;
*) error "Unsupported architecture: $RAW_ARCH" ;;
esac

# --- Determine version ---
[ -z "${DOCKER_TAG:-}" ] && source "${SCRIPT_DIR}/find_version.bash"
[ -z "${DOCKER_TAG:-}" ] && error "Failed to determine DOCKER_TAG"
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
trap cleanup EXIT

log "=== SereneDB Docker Build ==="
log "Version:  ${VERSION}"
log "Image:    ${FULL_IMAGE_NAME}"
log "Arches:   ${AVAILABLE_ARCHES[*]}"

cp "${DOCKER_DIR}/Dockerfile" "${BUILD_DIR}/"
cp "${DOCKER_DIR}/setup.sh" "${BUILD_DIR}/"
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

# --- Build & Push ---
if [ "${PUSH_IMAGES_2_REGISTRY}" = "true" ]; then
	if [ -z "${DOCKER_USERNAME:-}" ] || [ -z "${DOCKER_PASSWORD:-}" ]; then
		error "DOCKER_USERNAME and DOCKER_PASSWORD are required to push."
	fi

	# Build each arch separately and export (anonymous pulls)
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

	# Load into docker daemon
	log "Loading images..."
	for arch in "${AVAILABLE_ARCHES[@]}"; do
		docker load </tmp/serenedb-${arch}.tar
		rm -f "/tmp/serenedb-${arch}.tar"
	done

	# Smoke test (use first available arch; only works natively if it matches HOST_ARCH)
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

	# Login and push
	docker logout >/dev/null 2>&1 || true
	log "Logging in to Docker Hub as ${DOCKER_USERNAME}..."
	if [[ "${DOCKER_REGISTRY}" =~ [.:] ]]; then
		echo "$DOCKER_PASSWORD" | docker login "$DOCKER_REGISTRY" -u "$DOCKER_USERNAME" --password-stdin
		NEEDS_LOGOUT="$DOCKER_REGISTRY"
	else
		echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
		NEEDS_LOGOUT="hub"
	fi

	for arch in "${AVAILABLE_ARCHES[@]}"; do
		log "Pushing ${FULL_IMAGE_NAME}:${VERSION}-${arch}..."
		docker push "${FULL_IMAGE_NAME}:${VERSION}-${arch}"
	done

	# Build manifest sources list
	MANIFEST_SOURCES=()
	for arch in "${AVAILABLE_ARCHES[@]}"; do
		MANIFEST_SOURCES+=("${FULL_IMAGE_NAME}:${VERSION}-${arch}")
	done

	# Create and push multi-arch manifests
	log "Creating multi-arch manifests..."
	ALL_TAGS=("${VERSION}")
	for tag in "${EXTRA_TAGS_ARRAY[@]}"; do
		[ -n "$tag" ] && ALL_TAGS+=("$tag")
	done

	for tag in "${ALL_TAGS[@]}"; do
		docker manifest create --amend "${FULL_IMAGE_NAME}:${tag}" "${MANIFEST_SOURCES[@]}"
		docker manifest push "${FULL_IMAGE_NAME}:${tag}"
		log "  Pushed ${FULL_IMAGE_NAME}:${tag}"
	done

	# Cleanup arch-specific images
	for arch in "${AVAILABLE_ARCHES[@]}"; do
		docker rmi "${FULL_IMAGE_NAME}:${VERSION}-${arch}" >/dev/null 2>&1 || true
	done

	log "Push complete!"
else
	# Local build: single arch, --load
	log "Building for local use (linux/${HOST_ARCH})..."
	docker buildx build \
		--load \
		--platform "linux/${HOST_ARCH}" \
		--tag "${FULL_IMAGE_NAME}:${VERSION}" \
		"${LABEL_ARGS[@]}" \
		--no-cache \
		--file "${BUILD_DIR}/Dockerfile" \
		"${BUILD_DIR}"

	log "Build complete!"

	# Smoke test
	log "=== Testing Image ==="
	if docker run --rm "${FULL_IMAGE_NAME}:${VERSION}" --version 2>/dev/null; then
		log "  Version check passed"
	else
		error "  Version check failed"
	fi
fi

# Summary
log ""
log "=== Done ==="
log "Image: ${FULL_IMAGE_NAME}:${VERSION}"
log ""
log "Run with:"
log "  docker run -d -p 8529:8529 ${FULL_IMAGE_NAME}:${VERSION}"
