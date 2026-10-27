#!/bin/bash
set -euo pipefail

MODE=${1:-local}
case "$MODE" in
local | missing | arch | merge) ;;
*)
	echo "usage: $0 [local|missing|arch|merge]" >&2
	exit 2
	;;
esac

case $(uname -m) in
x86_64) ARCH=amd64 ;;
aarch64) ARCH=arm64 ;;
*)
	echo "[-] Error: Unsupported architecture: $(uname -m)" >&2
	exit 1
	;;
esac

if [[ "$MODE" == arch || "$MODE" == merge ]]; then
	if [ -z "${DOCKER_USERNAME:-}" ] || [ -z "${DOCKER_PASSWORD:-}" ]; then
		echo "[-] Error: DOCKER_USERNAME and DOCKER_PASSWORD are required to push." >&2
		exit 1
	fi
	trap 'docker logout >/dev/null 2>&1 || true' EXIT
	docker logout >/dev/null 2>&1 || true
fi

login() {
	docker login -u "$DOCKER_USERNAME" --password-stdin <<<"$DOCKER_PASSWORD" >/dev/null
}

BUILT=()

BUILDER_READY=false
builder() {
	if [ "$BUILDER_READY" = true ]; then
		return
	fi
	BUILDER_READY=true
	docker buildx rm serenedb-builder >/dev/null 2>&1 || true
	docker buildx create --name serenedb-builder --use --driver-opt network=host >/dev/null
}

image() {
	local image=$1
	shift
	if docker buildx imagetools inspect "$image" >/dev/null 2>&1; then
		if [ "$MODE" != missing ]; then
			echo "[*] ${image} is in the registry"
		fi
		return
	fi
	case "$MODE" in
	missing)
		echo "$image"
		;;
	local)
		builder
		echo "[*] Building ${image} for linux/${ARCH}..."
		docker buildx build --load --platform "linux/${ARCH}" -t "$image" "$@"
		;;
	arch)
		builder
		echo "[*] Building ${image}-${ARCH}..."
		docker buildx build --load --platform "linux/${ARCH}" -t "${image}-${ARCH}" "$@"
		BUILT+=("${image}-${ARCH}")
		;;
	merge)
		BUILT+=("$image")
		;;
	esac
}

image "$(python3 ci.py image)" --build-context drivers=../../tests/drivers --file build-ubuntu.Dockerfile .

FIXTURES=../../tests/sqllogic/fixtures
for fixture in ollama postgres; do
	image "serenedb/serenedb-test-${fixture}:$("${FIXTURES}/image_tag.sh" "${FIXTURES}/${fixture}")" \
		"${FIXTURES}/${fixture}"
done

if [ ${#BUILT[@]} -ne 0 ]; then
	login
	for image in "${BUILT[@]}"; do
		if [ "$MODE" = arch ]; then
			docker push "$image"
		else
			docker buildx imagetools create --tag "$image" "${image}-amd64" "${image}-arm64"
		fi
		echo "[+] Pushed ${image}"
	done
fi
