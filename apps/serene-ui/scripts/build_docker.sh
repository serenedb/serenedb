#!/bin/bash
set -eo pipefail

[ -z "$DOCKER_REGISTRY" ] && exit 1        # DOCKER_REGISTRY="serenedb"
[ -z "$IMAGE" ] && exit 1                  # IMAGE="serene-ui"
[ -z "$DOCKER_TAG" ] && exit 1             # DOCKER_TAG="1.2.3"
[ -z "$PUSH_IMAGE_TO_REGISTRY" ] && exit 1 # PUSH_IMAGE_TO_REGISTRY="false"

FULL_IMAGE_NAME="${DOCKER_REGISTRY}/${IMAGE}"
BUILDER_NAME="sereneui-builder"
[ -d logs ] || mkdir logs

# Detect host architecture
RAW_ARCH=$(uname -m)
case $RAW_ARCH in
x86_64) HOST_ARCH="amd64" ;;
aarch64) HOST_ARCH="arm64" ;;
*)
	echo "[-] Error: Unsupported architecture: $RAW_ARCH"
	exit 1
	;;
esac

NEEDS_LOGOUT=""
cleanup() {
	rm -f /tmp/serene-ui-amd64.tar /tmp/serene-ui-arm64.tar
	docker buildx rm "${BUILDER_NAME}" 2>/dev/null || true
	if [ -n "$NEEDS_LOGOUT" ]; then
		if [ "$NEEDS_LOGOUT" = "hub" ]; then
			docker logout 2>/dev/null || true
		else
			docker logout "$NEEDS_LOGOUT" 2>/dev/null || true
		fi
	fi
}

# Ensure no stale login interferes with anonymous pulls
docker logout >/dev/null 2>&1 || true

# Setup buildx
echo ">>> Configuring Docker Buildx..."
if docker buildx inspect "$BUILDER_NAME" >/dev/null 2>&1; then
	docker buildx rm "$BUILDER_NAME" >/dev/null
fi
docker buildx create --name "$BUILDER_NAME" --use --driver-opt network=host >/dev/null
trap cleanup EXIT INT TERM

LABEL_ARGS=(
	--label "org.opencontainers.image.version=${DOCKER_TAG}"
	--label "org.opencontainers.image.created=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
	--label "org.opencontainers.image.revision=$(git rev-parse HEAD 2>/dev/null || echo 'unknown')"
	--label "org.opencontainers.image.title=${IMAGE}"
)

if [ "$PUSH_IMAGE_TO_REGISTRY" = true ]; then
	if [ -z "${DOCKER_USERNAME:-}" ] || [ -z "${DOCKER_PASSWORD:-}" ]; then
		echo "[-] Error: DOCKER_USERNAME and DOCKER_PASSWORD are required to push."
		exit 1
	fi

	# Build each arch separately and export (anonymous pulls, no login needed)
	for arch in amd64 arm64; do
		echo ">>> Building linux/${arch}..."
		docker buildx build \
			--platform "linux/${arch}" \
			-t "${FULL_IMAGE_NAME}:${DOCKER_TAG}-${arch}" \
			"${LABEL_ARGS[@]}" \
			--output "type=docker,dest=/tmp/serene-ui-${arch}.tar" \
			--pull --no-cache \
			. 2>&1 | tee -a logs/build.log
	done

	# Load into docker daemon
	echo ">>> Loading images..."
	for arch in amd64 arm64; do
		docker load </tmp/serene-ui-${arch}.tar
		rm -f "/tmp/serene-ui-${arch}.tar"
	done

	# Login and push
	if [[ "$DOCKER_REGISTRY" =~ [.:] ]]; then
		echo ">>> Logging in to $DOCKER_REGISTRY"
		echo "$DOCKER_PASSWORD" | docker login "$DOCKER_REGISTRY" -u "$DOCKER_USERNAME" --password-stdin
		NEEDS_LOGOUT="$DOCKER_REGISTRY"
	else
		echo ">>> Logging in to Docker Hub"
		echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
		NEEDS_LOGOUT="hub"
	fi

	for arch in amd64 arm64; do
		echo ">>> Pushing ${FULL_IMAGE_NAME}:${DOCKER_TAG}-${arch}..."
		docker push "${FULL_IMAGE_NAME}:${DOCKER_TAG}-${arch}" 2>&1 | tee -a logs/build.log
	done

	# Build list of all tags to create manifests for
	ALL_TAGS=("${DOCKER_TAG}")
	if [ "$PUSH_RELEASE" = true ]; then
		ALL_TAGS+=("latest")
	fi
	IFS=', ' read -ra EXTRA_TAGS <<<"${EXTRA_DOCKER_TAGS:-}"
	for tag in "${EXTRA_TAGS[@]}"; do
		[ -n "$tag" ] && ALL_TAGS+=("$tag")
	done

	# Create and push multi-arch manifests
	echo ">>> Creating multi-arch manifests for: ${ALL_TAGS[*]}"
	TAG_ARGS=()
	for tag in "${ALL_TAGS[@]}"; do
		TAG_ARGS+=(-t "${FULL_IMAGE_NAME}:${tag}")
	done
	docker buildx imagetools create "${TAG_ARGS[@]}" \
		"${FULL_IMAGE_NAME}:${DOCKER_TAG}-amd64" \
		"${FULL_IMAGE_NAME}:${DOCKER_TAG}-arm64"

	# Cleanup arch-specific images
	for arch in amd64 arm64; do
		docker rmi "${FULL_IMAGE_NAME}:${DOCKER_TAG}-${arch}" >/dev/null 2>&1 || true
	done
else
	# Local build: single arch, --load
	echo ">>> Building ${FULL_IMAGE_NAME}:${DOCKER_TAG} (linux/${HOST_ARCH})"
	docker buildx build \
		--load \
		--platform "linux/${HOST_ARCH}" \
		--tag "${FULL_IMAGE_NAME}:${DOCKER_TAG}" \
		"${LABEL_ARGS[@]}" \
		--pull --no-cache \
		. 2>&1 | tee -a logs/build.log
fi
