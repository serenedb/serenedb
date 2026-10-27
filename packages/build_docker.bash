#!/bin/bash

# Builds the Docker image of this machine's architecture, natively, from its install tarball
# (packages/tarball/install-<arch>.tar.gz, made by build_targz.bash) and loads it into the local
# daemon as <registry>/serenedb:<version>-<arch> (written to GITHUB_OUTPUT as image=).
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
	docker buildx rm "${BUILDER_NAME}" 2>/dev/null || true
}

case $(uname -m) in
x86_64) ARCH="amd64" ;;
aarch64) ARCH="arm64" ;;
*) error "Unsupported architecture: $(uname -m)" ;;
esac

if [ -z "${DOCKER_TAG:-}" ]; then
	source "${SCRIPT_DIR}/find_version.bash"
	DOCKER_TAG="${SERENEDB_VERSION}"
fi
[ -z "${DOCKER_TAG:-}" ] && error "Failed to determine version"

FULL_IMAGE_NAME="${DOCKER_REGISTRY}/${IMAGE_NAME}"
LOCAL_IMAGE="${FULL_IMAGE_NAME}:${DOCKER_TAG}-${ARCH}"
TARBALL="${TARBALL_DIR}/install-${ARCH}.tar.gz"
[ -e "$TARBALL" ] || error "No ${TARBALL}. Run build_targz.bash first."

BUILD_DIR=$(mktemp -d)
trap cleanup EXIT INT TERM
cp "${DOCKER_DIR}/Dockerfile" "${DOCKER_DIR}/entrypoint.sh" "${BUILD_DIR}/"
cp "$TARBALL" "${BUILD_DIR}/install-${ARCH}.tar.gz"

docker buildx rm "$BUILDER_NAME" >/dev/null 2>&1 || true
docker buildx create --name "$BUILDER_NAME" --driver-opt network=host >/dev/null

log "Building ${LOCAL_IMAGE} for linux/${ARCH}..."
docker buildx build \
	--builder "$BUILDER_NAME" \
	--platform "linux/${ARCH}" \
	-t "$LOCAL_IMAGE" \
	--label "org.opencontainers.image.version=${DOCKER_TAG}" \
	--label "org.opencontainers.image.created=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
	--label "org.opencontainers.image.revision=$(git -C "$PROJECT_ROOT" rev-parse HEAD 2>/dev/null || echo 'unknown')" \
	--provenance=false \
	--sbom=false \
	--no-cache \
	--load \
	--file "${BUILD_DIR}/Dockerfile" \
	"${BUILD_DIR}"

log "=== Smoke test ==="
docker run --rm ${SDB_DOCKER_OWNER:+--label "serenedb.owner=$SDB_DOCKER_OWNER"} "$LOCAL_IMAGE" --version ||
	error "Version check failed"

if [ -n "${GITHUB_OUTPUT:-}" ]; then
	echo "image=${LOCAL_IMAGE}" >>"$GITHUB_OUTPUT"
fi
log "Built ${LOCAL_IMAGE}"
