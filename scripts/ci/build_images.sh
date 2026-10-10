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

CONTEXTS=(--build-context drivers=../../tests/drivers)

# --- Build Loop ---
for os in ubuntu; do
	REPO="${REGISTRY}/serenedb-build-${os}"
	DOCKERFILE="build-${os}.Dockerfile"

	echo "----------------------------------------------------"
	echo "[*] Processing $os..."

	# 1. Build Probe (host arch, loaded locally for version extraction)
	echo "    > Building local probe ($HOST_PLATFORM)..."
	docker buildx build --load --platform "$HOST_PLATFORM" -t "${REPO}:probe" "${CONTEXTS[@]}" --file "${DOCKERFILE}" . >/dev/null

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
				"${CONTEXTS[@]}" \
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

		TAGS=(--tag "${REPO}:${IMAGE_TAG}")
		if [ "${TAG_LATEST:-false}" = "true" ]; then
			TAGS+=(--tag "${REPO}:latest")
		fi
		if [ -n "${EXTRA_TAG:-}" ]; then
			TAGS+=(--tag "${REPO}:${EXTRA_TAG//\//-}")
		fi
		echo "    > Creating manifests ${TAGS[*]}..."
		docker buildx imagetools create \
			"${TAGS[@]}" \
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

# --- Test fixture images ---
FIXTURES=../../tests/sqllogic/fixtures
FIXTURE_IMAGES=()
docker logout >/dev/null 2>&1 || true
for fixture in ollama postgres; do
	FIXTURE_IMAGE="${REGISTRY}/serenedb-test-${fixture}:$("${FIXTURES}/image_tag.sh" "${FIXTURES}/${fixture}")"
	echo "[*] Building ${FIXTURE_IMAGE}..."
	if [ "$PUSH_ENABLED" = "true" ]; then
		for arch in amd64 arm64; do
			docker buildx build --platform "linux/${arch}" -t "${FIXTURE_IMAGE}-${arch}" \
				--output "type=docker,dest=/tmp/${fixture}-${arch}.tar" "${FIXTURES}/${fixture}"
		done
		FIXTURE_IMAGES+=("${FIXTURE_IMAGE}")
	else
		docker buildx build --platform "$HOST_PLATFORM" -t "${FIXTURE_IMAGE}" --load "${FIXTURES}/${fixture}"
	fi
done

if [ ${#FIXTURE_IMAGES[@]} -ne 0 ]; then
	echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
	trap 'docker logout' EXIT INT TERM
	for fixture_image in "${FIXTURE_IMAGES[@]}"; do
		fixture="${fixture_image#"${REGISTRY}"/serenedb-test-}"
		fixture="${fixture%%:*}"
		for arch in amd64 arm64; do
			docker load <"/tmp/${fixture}-${arch}.tar"
			rm -f "/tmp/${fixture}-${arch}.tar"
			docker push "${fixture_image}-${arch}"
		done
		docker buildx imagetools create --tag "${fixture_image}" "${fixture_image}-amd64" "${fixture_image}-arm64"
		echo "[+] SUCCESS: Pushed ${fixture_image}"
	done
fi
