#!/bin/bash

# Publishes the release image: joins the architectures' images, which the package jobs pushed under
# <version>-<arch>, by their digests into one multi-platform image under the version tag and every
# extra tag.
#
# Environment Configuration:
#   DOCKER_TAG               Version tag (required)
#   DOCKER_EXTRA_TAGS        Comma or space-separated list of additional tags
#   IMAGE_DIGESTS            Space-separated digests of the architectures' images (required)
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

read -r -a DIGESTS <<<"${IMAGE_DIGESTS:-}"
[ ${#DIGESTS[@]} -gt 0 ] || error "IMAGE_DIGESTS is required"
IFS=', ' read -r -a EXTRA_TAGS_ARRAY <<<"${DOCKER_EXTRA_TAGS:-}"

log "=== SereneDB Docker Push ==="
log "Version:  ${VERSION}"
log "Image:    ${FULL_IMAGE_NAME}"
log "Digests:  ${DIGESTS[*]}"

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

# --- Join the architectures under every tag ---
SOURCES=()
for digest in "${DIGESTS[@]}"; do
	SOURCES+=("${FULL_IMAGE_NAME}@${digest}")
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
docker buildx imagetools create "${TAG_ARGS[@]}" "${SOURCES[@]}"

log "Push complete!"
