#!/bin/bash
# =============================================================================
# SereneDB Docker Image Build Script
#
# Usage:
#   ./build_docker.bash [OPTIONS]
#
# Options:
#   --version VERSION    Set version (default: from find_version.bash)
#   --tag TAG            Additional tag (can be repeated)
#   --push               Push to registry after build
#   --registry URL       Registry URL (default: docker.io)
#   --no-cache           Build without cache
#   --platform PLATFORM  Target platform (default: linux/amd64)
#   --help               Show this help
#
# Environment:
#   DOCKER_REGISTRY      Registry URL
#   DOCKER_USERNAME      Registry username (for push)
#   DOCKER_PASSWORD      Registry password (for push)
#   SERENEDB_VERSION     Version override
#
# Examples:
#   ./build_docker.bash
#   ./build_docker.bash --version 1.0.0 --push
#   ./build_docker.bash --tag latest --tag stable --push
# =============================================================================
set -e

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DOCKER_DIR="${SCRIPT_DIR}/docker"
TARBALL_DIR="${SCRIPT_DIR}/tarball"

IMAGE_NAME="serenedb"
REGISTRY="${DOCKER_REGISTRY:-registry.serenedb.com:5000}"
PLATFORM="linux/amd64"
PUSH_IMAGES_2_REGISTRY=false
NO_CACHE=""
DOCKER_EXTRA_TAGS=()

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

show_help() {
  sed -n '/^# Usage:/,/^# ====/p' "$0" | grep -v '====' | sed 's/^# //'
  exit 0
}

get_version() {
  if [ -n "$SERENEDB_VERSION" ]; then
    echo "$SERENEDB_VERSION"
  elif [ -f "${SCRIPT_DIR}/find_version.bash" ]; then
    bash "${SCRIPT_DIR}/find_version.bash"
  else
    error "Cannot determine version. Set SERENEDB_VERSION or create find_version.bash"
  fi
}

# -----------------------------------------------------------------------------
# Parse arguments
# -----------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
  case $1 in
  --version)
    SERENEDB_VERSION="$2"
    shift 2
    ;;
  --tag)
    DOCKER_EXTRA_TAGS+=("$2")
    shift 2
    ;;
  --push)
    PUSH_IMAGES_2_REGISTRY=true
    shift
    ;;
  --registry)
    REGISTRY="$2"
    shift 2
    ;;
  --no-cache)
    NO_CACHE="--no-cache"
    shift
    ;;
  --platform)
    PLATFORM="$2"
    shift 2
    ;;
  --help | -h)
    show_help
    ;;
  *)
    error "Unknown option: $1"
    ;;
  esac
done

# Setup
VERSION=$(get_version)
FULL_IMAGE_NAME="${REGISTRY}/${IMAGE_NAME}"
BUILD_DIR=$(mktemp -d)

trap "rm -rf ${BUILD_DIR}" EXIT

log "=== SereneDB Docker Build ==="
log "Version:  ${VERSION}"
log "Image:    ${FULL_IMAGE_NAME}"
log "Platform: ${PLATFORM}"
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
  --platform "${PLATFORM}"
  --tag "${FULL_IMAGE_NAME}:${VERSION}"
  --label "org.opencontainers.image.version=${VERSION}"
  --label "org.opencontainers.image.created=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  --label "org.opencontainers.image.revision=$(git rev-parse HEAD 2>/dev/null || echo 'unknown')"
  --file "${BUILD_DIR}/Dockerfile"
  ${NO_CACHE}
)

# Add extra tags
for tag in "${DOCKER_EXTRA_TAGS[@]}"; do
  BUILD_ARGS+=(--tag "${FULL_IMAGE_NAME}:${tag}")
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
if docker run --rm "${FULL_IMAGE_NAME}:${VERSION}" --version; then
  log "  ✓ Version check passed"
else
  log "  ✗ Version check failed (continuing anyway)"
fi

# Push to registry
if [ "$PUSH_IMAGES_2_REGISTRY" = true ]; then
  log ""
  log "=== Pushing to Registry ==="

  # Login if credentials provided
  if [ -n "$DOCKER_USERNAME" ] && [ -n "$DOCKER_PASSWORD" ]; then
    echo "$DOCKER_PASSWORD" | docker login "$REGISTRY" -u "$DOCKER_USERNAME" --password-stdin
  fi

  # Push version tag
  log "Pushing ${FULL_IMAGE_NAME}:${VERSION}..."
  docker push "${FULL_IMAGE_NAME}:${VERSION}"

  # Push extra tags
  for tag in "${DOCKER_EXTRA_TAGS[@]}"; do
    log "Pushing ${FULL_IMAGE_NAME}:${tag}..."
    docker push "${FULL_IMAGE_NAME}:${tag}"
  done

  log "Push complete!"
fi

# Summary
log ""
log "=== Done ==="
log "Image: ${FULL_IMAGE_NAME}:${VERSION}"
log ""
log "Run with:"
log "  docker run -d -p 8529:8529 -e SERENE_ROOT_PASSWORD=secret ${FULL_IMAGE_NAME}:${VERSION}"
log ""
log "Or with docker-compose:"
log "  See ${DOCKER_DIR}/docker-compose.yml"
