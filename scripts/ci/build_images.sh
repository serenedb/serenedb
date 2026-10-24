#!/bin/bash
set -euo pipefail

PUSH_ENABLED=${PUSH_IMAGES_2_REGISTRY:-false}
OWNER_LABEL=()
if [ -n "${SDB_DOCKER_OWNER:-}" ]; then
	OWNER_LABEL=(--label "serenedb.owner=$SDB_DOCKER_OWNER")
fi

if [ "$PUSH_ENABLED" = "true" ]; then
	if [ -z "${DOCKER_USERNAME:-}" ] || [ -z "${DOCKER_PASSWORD:-}" ]; then
		echo "[-] Error: DOCKER_USERNAME and DOCKER_PASSWORD are required to push."
		exit 1
	fi
fi

RAW_ARCH=$(uname -m)
case $RAW_ARCH in
x86_64) HOST_PLATFORM="linux/amd64" ;;
aarch64) HOST_PLATFORM="linux/arm64" ;;
*)
	echo "[-] Error: Unsupported architecture: $RAW_ARCH"
	exit 1
	;;
esac

docker logout >/dev/null 2>&1 || true
trap 'docker logout >/dev/null 2>&1 || true' EXIT

BUILDER_READY=false
builder() {
	if [ "$BUILDER_READY" = "true" ]; then
		return
	fi
	BUILDER_READY=true
	if [ "$PUSH_ENABLED" = "true" ]; then
		docker run --rm "${OWNER_LABEL[@]}" --privileged tonistiigi/binfmt --install all >/dev/null
	fi
	docker buildx rm serenedb-builder >/dev/null 2>&1 || true
	docker buildx create --name serenedb-builder --use --driver-opt network=host >/dev/null
}

publish() {
	local image=$1
	shift
	if docker buildx imagetools inspect "$image" >/dev/null 2>&1; then
		echo "[*] ${image} is in the registry"
		return
	fi
	builder
	if [ "$PUSH_ENABLED" != "true" ]; then
		echo "[*] Building ${image} for ${HOST_PLATFORM}..."
		docker buildx build --load --platform "$HOST_PLATFORM" -t "$image" "$@"
		return
	fi
	local tars
	tars=$(mktemp -d)
	for arch in amd64 arm64; do
		echo "[*] Building ${image}-${arch}..."
		docker buildx build --platform "linux/${arch}" -t "${image}-${arch}" \
			--output "type=docker,dest=${tars}/${arch}.tar" "$@"
	done
	for arch in amd64 arm64; do
		docker load <"${tars}/${arch}.tar"
	done
	rm -rf "$tars"
	echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
	for arch in amd64 arm64; do
		docker push "${image}-${arch}"
	done
	docker buildx imagetools create --tag "$image" "${image}-amd64" "${image}-arm64"
	docker logout
	for arch in amd64 arm64; do
		docker rmi "${image}-${arch}" >/dev/null 2>&1 || true
	done
	echo "[+] Pushed ${image}"
}

publish "$(python3 ci.py image)" --build-context drivers=../../tests/drivers --file build-ubuntu.Dockerfile .

FIXTURES=../../tests/sqllogic/fixtures
for fixture in ollama postgres; do
	publish "serenedb/serenedb-test-${fixture}:$("${FIXTURES}/image_tag.sh" "${FIXTURES}/${fixture}")" \
		"${FIXTURES}/${fixture}"
done
