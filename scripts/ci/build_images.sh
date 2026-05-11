#!/bin/bash
set -e

# --- Configuration ---
REGISTRY="${DOCKER_REGISTRY:-serenedb}"
PUSH_ENABLED=${PUSH_IMAGES_2_REGISTRY:-false}
BUILDER_NAME="serenedb-builder"

# --- Validate push credentials early ---
if [ "$PUSH_ENABLED" = "true" ]; then
	if [ -z "${DOCKER_USERNAME:-}" ] || [ -z "${DOCKER_PASSWORD:-}" ]; then
		echo "[-] Error: DOCKER_USERNAME and DOCKER_PASSWORD are required to push."
		exit 1
	fi
fi

# --- Detect Host Architecture ---
RAW_ARCH=$(uname -m)
case $RAW_ARCH in
x86_64) HOST_PLATFORM="linux/amd64" ;;
aarch64) HOST_PLATFORM="linux/arm64" ;;
*)
	echo "[-] Error: Unsupported architecture: $RAW_ARCH"
	exit 1
	;;
esac

# --- Ensure no stale login interferes with anonymous pulls ---
docker logout >/dev/null 2>&1 || true

# --- Setup Build Environment ---
echo "[*] Configuring Docker Buildx..."

if docker buildx inspect "$BUILDER_NAME" >/dev/null 2>&1; then
	docker buildx rm "$BUILDER_NAME" >/dev/null
fi
docker buildx create --name "$BUILDER_NAME" --use --driver-opt network=host >/dev/null

# --- Build Loop ---
for os in ubuntu; do
	REPO="${REGISTRY}/serenedb-build-${os}"
	DOCKERFILE="build-${os}.Dockerfile"

	echo "----------------------------------------------------"
	echo "[*] Processing $os..."

	# 1. Build Probe (host arch, loaded locally for version extraction)
	echo "    > Building local probe ($HOST_PLATFORM)..."
	docker buildx build --load --platform "$HOST_PLATFORM" -t "${REPO}:probe" --file "${DOCKERFILE}" . >/dev/null

	# 2. Extract Version Info
	echo "    > Inspecting versions..."
	CLANG_VER=$(docker run --rm "${REPO}:probe" clang++ --version | grep -Po " \K\d+\.\d+[\.\d+]*" | head -1)

	case ${os} in
	ubuntu)
		OS_VER=$(docker run --rm "${REPO}:probe" cat /etc/os-release | grep -Po "VERSION=\"\K\d+\.\d+[\.\d]*")
		;;
	alpine)
		OS_VER=$(docker run --rm "${REPO}:probe" cat /etc/alpine-release | grep -Po "\d+\.\d+[\.\d+]*")
		;;
	esac

	IMAGE_TAG="${OS_VER}_clang-${CLANG_VER}_commit-$(git rev-parse --short HEAD)"
	echo "    > Resolved Tag: ${IMAGE_TAG}"

	# 3. Multi-Arch Build & Push
	if [ "$PUSH_ENABLED" = "true" ]; then
		for arch in amd64 arm64; do
			echo "    > Building linux/${arch}..."
			docker buildx build \
				--platform "linux/${arch}" \
				-t "${REPO}:${IMAGE_TAG}-${arch}" \
				--output "type=docker,dest=/tmp/${os}-${arch}.tar" \
				--file "${DOCKERFILE}" .
		done

		echo "    > Loading images..."
		for arch in amd64 arm64; do
			docker load </tmp/${os}-${arch}.tar
			rm -f "/tmp/${os}-${arch}.tar"
		done

		echo "[*] Logging in to Docker Hub as $DOCKER_USERNAME..."
		echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
		trap 'docker logout' EXIT INT TERM

		for arch in amd64 arm64; do
			echo "    > Pushing ${REPO}:${IMAGE_TAG}-${arch}..."
			docker push "${REPO}:${IMAGE_TAG}-${arch}"
		done

		echo "    > Creating manifests ${REPO}:{${IMAGE_TAG},latest}..."
		docker buildx imagetools create \
			--tag "${REPO}:${IMAGE_TAG}" \
			--tag "${REPO}:latest" \
			"${REPO}:${IMAGE_TAG}-amd64" \
			"${REPO}:${IMAGE_TAG}-arm64"

		echo "[+] SUCCESS: Pushed ${REPO}:${IMAGE_TAG}"

		for arch in amd64 arm64; do
			docker rmi "${REPO}:${IMAGE_TAG}-${arch}" >/dev/null 2>&1 || true
		done
	else
		echo "[!] SKIP: Pushing disabled."
	fi

	# Cleanup local probe tag
	docker rmi "${REPO}:probe" >/dev/null 2>&1 || true

done
