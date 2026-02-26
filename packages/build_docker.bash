#!/bin/bash
# =============================================================================
# SereneDB Docker Image Build Script
#
# Environment Configuration:
#   DOCKER_TAG_OVERRIDE      Set version (default: from find_version.bash)
#   DOCKER_EXTRA_TAGS        Comma or space-separated list of additional tags
#   PUSH_IMAGES_2_REGISTRY   'true' to push to registry after build
#   DOCKER_REGISTRY          Registry URL (default: registry.serenedb.com:5000)
#   DOCKER_NO_CACHE          'true' to build without cache
#   DOCKER_PLATFORM          Target platform (default: linux/amd64)
#   DOCKER_USERNAME          Registry username (for push)
#   DOCKER_PASSWORD          Registry password (for push)
# =============================================================================
set -e

# -----------------------------------------------------------------------------
# Configuration from Environment Variables
# -----------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DOCKER_DIR="${SCRIPT_DIR}/docker"
TARBALL_DIR="${SCRIPT_DIR}/tarball"
IMAGE_NAME="serenedb"

: "${DOCKER_REGISTRY:=registry.serenedb.com:5000}"
: "${DOCKER_PLATFORM:=linux/amd64}"

# Parse DOCKER_EXTRA_TAGS into a bash array (handles commas and spaces)
IFS=', ' read -r -a EXTRA_TAGS_ARRAY <<<"${DOCKER_EXTRA_TAGS:-}"

# -----------------------------------------------------------------------------
# Functions
# -----------------------------------------------------------------------------
log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

error() {
  echo "[ERROR] $*" >&2
  exit 1
}

get_version_and_tag() {
  # Source find_version.bash to populate all version variables
  if [ -f "${SCRIPT_DIR}/find_version.bash" ]; then
    # Source it in a subshell and capture the variables we need
    local version_output
    version_output=$(bash -c "source '${SCRIPT_DIR}/find_version.bash' >/dev/null 2>&1 && echo \"\$DOCKER_TAG\"")

    if [ -z "$version_output" ]; then
      error "Failed to determine version from find_version.bash"
    fi

    echo "$version_output"
  else
    error "find_version.bash not found at ${SCRIPT_DIR}/find_version.bash"
  fi
}

# -----------------------------------------------------------------------------
# Determine version and Docker tag
# -----------------------------------------------------------------------------
if [ -n "${DOCKER_TAG_OVERRIDE:-}" ]; then
  VERSION="$DOCKER_TAG_OVERRIDE"
  log "Using version override: ${VERSION}"
elif [ -n "${DOCKER_TAG:-}" ]; then
  # DOCKER_TAG already set in environment
  VERSION="$DOCKER_TAG"
  log "Using DOCKER_TAG from environment: ${VERSION}"
else
  # Get from find_version.bash
  VERSION=$(get_version_and_tag)
  log "Using version from find_version.bash: ${VERSION}"
fi

FULL_IMAGE_NAME="${DOCKER_REGISTRY}/${IMAGE_NAME}"
BUILD_DIR=$(mktemp -d)

trap "rm -rf ${BUILD_DIR}" EXIT

log "=== SereneDB Docker Build ==="
log "Version:  ${VERSION}"
log "Image:    ${FULL_IMAGE_NAME}"
log "Platform: ${DOCKER_PLATFORM}"
log "Build:    ${BUILD_DIR}"

# Verify prerequisites
log "Checking prerequisites..."

# Check for tarball
TARBALL="${TARBALL_DIR}/install.tar.gz"
if [ ! -f "$TARBALL" ] && [ ! -L "$TARBALL" ]; then
  # Try alternative location
  TARBALL="${PROJECT_ROOT}/package_all/install.tar.gz"
fi

if [ ! -f "$TARBALL" ] && [ ! -L "$TARBALL" ]; then
  error "install.tar.gz not found. Run build_targz.bash first."
fi

log "  Tarball: ${TARBALL} ($(du -h "$TARBALL" | cut -f1))"

# Prepare build context
log "Preparing build context..."

cp "${DOCKER_DIR}/Dockerfile" "${BUILD_DIR}/"
cp "${DOCKER_DIR}/setup.sh" "${BUILD_DIR}/"
cp "${DOCKER_DIR}/entrypoint.sh" "${BUILD_DIR}/"
cp "${TARBALL}" "${BUILD_DIR}/install.tar.gz"

log "  Context size: $(du -sh "${BUILD_DIR}" | cut -f1)"

# Build image
log "Building Docker image..."

BUILD_ARGS=(
  --platform "${DOCKER_PLATFORM}"
  --tag "${FULL_IMAGE_NAME}:${VERSION}"
  --label "org.opencontainers.image.version=${VERSION}"
  --label "org.opencontainers.image.created=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  --label "org.opencontainers.image.revision=$(git rev-parse HEAD 2>/dev/null || echo 'unknown')"
  --file "${BUILD_DIR}/Dockerfile"
)

if [ "${DOCKER_NO_CACHE:-false}" = "true" ]; then
  BUILD_ARGS+=(--no-cache)
fi

# Add extra tags
for tag in "${EXTRA_TAGS_ARRAY[@]}"; do
  if [ -n "$tag" ]; then
    BUILD_ARGS+=(--tag "${FULL_IMAGE_NAME}:${tag}")
  fi
done

docker build "${BUILD_ARGS[@]}" "${BUILD_DIR}"

log "Build complete!"

# Show image info
log ""
log "=== Image Info ==="
docker images "${FULL_IMAGE_NAME}" --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}\t{{.CreatedAt}}"

# Test image
log ""
log "=== Testing Image ==="

# Quick smoke test
if docker run --rm "${FULL_IMAGE_NAME}:${VERSION}" --version 2>/dev/null; then
  log "  ✓ Version check passed"
else
  log "  ✗ Version check failed (continuing anyway)"
fi

# Push to registry
if [ "${PUSH_IMAGES_2_REGISTRY:=false}" = true ]; then
  log ""
  log "=== Pushing to Registry ==="

  # Login if credentials provided
  if [ -n "${DOCKER_USERNAME:-}" ] && [ -n "${DOCKER_PASSWORD:-}" ]; then
    log "Logging in to ${DOCKER_REGISTRY}..."
    echo "$DOCKER_PASSWORD" | docker login "$DOCKER_REGISTRY" -u "$DOCKER_USERNAME" --password-stdin
  fi

  # Push version tag
  log "Pushing ${FULL_IMAGE_NAME}:${VERSION}..."
  docker push "${FULL_IMAGE_NAME}:${VERSION}"

  # Push extra tags
  for tag in "${DOCKER_EXTRA_TAGS[@]}"; do
    if [ -n "$tag" ]; then
      log "Pushing ${FULL_IMAGE_NAME}:${tag}..."
      docker push "${FULL_IMAGE_NAME}:${tag}"
    fi
  done

  log "Push complete!"
fi

# Summary
log ""
log "=== Done ==="
log "Image: ${FULL_IMAGE_NAME}:${VERSION}"
log ""
log "Run with:"
log "  docker run -d -p 8529:8529 ${FULL_IMAGE_NAME}:${VERSION}"
log ""
log "Or with docker-compose:"
log "  See ${DOCKER_DIR}/docker-compose.yml"
