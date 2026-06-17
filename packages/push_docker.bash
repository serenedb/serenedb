#!/bin/bash

# Pushes pre-built Docker images to a registry and creates multi-arch manifests.
# Expects images already loaded in the local daemon (via build_docker.bash).
#
# Environment Configuration:
#   DOCKER_TAG               Version tag (required)
#   DOCKER_EXTRA_TAGS        Comma or space-separated list of additional tags
#   DOCKER_REGISTRY          Registry URL (default: serenedb)
#   DOCKER_USERNAME          Registry username (required)
#   DOCKER_PASSWORD          Registry password (required)

set -e

IMAGE_NAME="serenedb"

: "${DOCKER_REGISTRY:=serenedb}"

log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"; }
error() {
	echo "[ERROR] $*" >&2
	exit 1
}

[ -z "${DOCKER_TAG:-}" ] && error "DOCKER_TAG is required"
[ -z "${DOCKER_USERNAME:-}" ] && error "DOCKER_USERNAME is required"
[ -z "${DOCKER_PASSWORD:-}" ] && error "DOCKER_PASSWORD is required"

VERSION="$DOCKER_TAG"
FULL_IMAGE_NAME="${DOCKER_REGISTRY}/${IMAGE_NAME}"

IFS=', ' read -r -a EXTRA_TAGS_ARRAY <<<"${DOCKER_EXTRA_TAGS:-}"

# --- Detect which arch images are available in daemon ---
AVAILABLE_ARCHES=()
for arch in amd64 arm64; do
	if docker image inspect "${FULL_IMAGE_NAME}:${VERSION}-${arch}" >/dev/null 2>&1; then
		AVAILABLE_ARCHES+=("$arch")
	fi
done

if [ ${#AVAILABLE_ARCHES[@]} -eq 0 ]; then
	error "No images found in daemon. Run build_docker.bash first."
fi

log "=== SereneDB Docker Push ==="
log "Version:  ${VERSION}"
log "Image:    ${FULL_IMAGE_NAME}"
log "Arches:   ${AVAILABLE_ARCHES[*]}"

NEEDS_LOGOUT=""
cleanup() {
	if [ -n "$NEEDS_LOGOUT" ]; then
		if [ "$NEEDS_LOGOUT" = "hub" ]; then
			docker logout 2>/dev/null || true
		else
			docker logout "$NEEDS_LOGOUT" 2>/dev/null || true
		fi
	fi
}
trap cleanup EXIT INT TERM

# --- Login ---
docker logout >/dev/null 2>&1 || true
log "Logging in to registry as ${DOCKER_USERNAME}..."
if [[ "${DOCKER_REGISTRY}" =~ [.:] ]]; then
	echo "$DOCKER_PASSWORD" | docker login "$DOCKER_REGISTRY" -u "$DOCKER_USERNAME" --password-stdin
	NEEDS_LOGOUT="$DOCKER_REGISTRY"
else
	echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
	NEEDS_LOGOUT="hub"
fi

# --- Push per-arch images ---
for arch in "${AVAILABLE_ARCHES[@]}"; do
	log "Pushing ${FULL_IMAGE_NAME}:${VERSION}-${arch}..."
	docker push "${FULL_IMAGE_NAME}:${VERSION}-${arch}"
done

# --- Create and push multi-arch manifests ---
MANIFEST_SOURCES=()
for arch in "${AVAILABLE_ARCHES[@]}"; do
	MANIFEST_SOURCES+=("${FULL_IMAGE_NAME}:${VERSION}-${arch}")
done

ALL_TAGS=("${VERSION}")
for tag in "${EXTRA_TAGS_ARRAY[@]}"; do
	[ -n "$tag" ] && ALL_TAGS+=("$tag")
done

log "Creating multi-arch manifests for: ${ALL_TAGS[*]}"
TAG_ARGS=()
for tag in "${ALL_TAGS[@]}"; do
	TAG_ARGS+=(-t "${FULL_IMAGE_NAME}:${tag}")
done
docker buildx imagetools create "${TAG_ARGS[@]}" "${MANIFEST_SOURCES[@]}"

# --- Cleanup arch-specific images ---
for arch in "${AVAILABLE_ARCHES[@]}"; do
	docker rmi "${FULL_IMAGE_NAME}:${VERSION}-${arch}" >/dev/null 2>&1 || true
done

log "Push complete!"
