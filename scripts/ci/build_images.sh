#!/bin/bash
set -euo pipefail

MODE=${1:-local}
case "$MODE" in
local | arch) ;;
tag)
	TAG=${2:-}
	if [ -z "$TAG" ]; then
		echo "usage: $0 tag <name>" >&2
		exit 2
	fi
	;;
*)
	echo "usage: $0 [local|arch|tag <name>]" >&2
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

if [ "$MODE" != local ] && { [ -z "${DOCKER_USERNAME:-}" ] || [ -z "${DOCKER_PASSWORD:-}" ]; }; then
	echo "[-] Error: DOCKER_USERNAME and DOCKER_PASSWORD are required to push." >&2
	exit 1
fi

LOGGED_IN=false
login() {
	if [ "$LOGGED_IN" = true ]; then
		return
	fi
	LOGGED_IN=true
	trap 'docker logout >/dev/null 2>&1 || true' EXIT
	docker logout >/dev/null 2>&1 || true
	docker login -u "$DOCKER_USERNAME" --password-stdin <<<"$DOCKER_PASSWORD" >/dev/null
}

present() {
	docker buildx imagetools inspect "$1" >/dev/null 2>&1
}

architectures() {
	docker buildx imagetools inspect "$1" \
		--format '{{range .Manifest.Manifests}}{{println .Platform.Architecture}}{{end}}' 2>/dev/null || true
}

join() {
	local image=$1 arch listed complete sources
	for _ in 1 2 3 4 5; do
		sources=()
		for arch in amd64 arm64; do
			if present "${image}-${arch}"; then
				sources+=("${image}-${arch}")
			fi
		done
		listed=$(architectures "$image")
		complete=true
		for arch in "${sources[@]##*-}"; do
			grep -qx "$arch" <<<"$listed" || complete=false
		done
		if [ "$complete" = true ]; then
			return
		fi
		docker buildx imagetools create --tag "$image" "${sources[@]}"
		echo "[+] Joined ${sources[*]} as ${image}"
	done
	echo "[-] Error: ${image} still lacks an architecture the registry has" >&2
	return 1
}

BUILT=()
JOINED=()

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
	if [ "$MODE" = tag ]; then
		JOINED+=("$image")
		return
	fi
	if architectures "$image" | grep -qx "$ARCH"; then
		echo "[*] ${image} has linux/${ARCH}"
		return
	fi
	if [ "$MODE" = local ]; then
		builder
		echo "[*] Building ${image} for linux/${ARCH}..."
		docker buildx build --load --platform "linux/${ARCH}" -t "$image" "$@"
		return
	fi
	if ! present "${image}-${ARCH}"; then
		builder
		echo "[*] Building ${image}-${ARCH}..."
		docker buildx build --load --platform "linux/${ARCH}" -t "${image}-${ARCH}" -t "$image" "$@"
		BUILT+=("${image}-${ARCH}")
	fi
	JOINED+=("$image")
}

image "$(python3 ci.py image)" --build-context drivers=../../tests/drivers --file build-ubuntu.Dockerfile .

FIXTURES=../../tests/sqllogic/fixtures
for fixture in ollama postgres; do
	image "serenedb/serenedb-test-${fixture}:$("${FIXTURES}/image_tag.sh" "${FIXTURES}/${fixture}")" \
		"${FIXTURES}/${fixture}"
done

for image in "${BUILT[@]}"; do
	login
	if present "$image"; then
		echo "[*] ${image} was pushed by another runner"
	else
		docker push "$image"
		echo "[+] Pushed ${image}"
	fi
done

for image in "${JOINED[@]}"; do
	login
	if [ "$MODE" = tag ]; then
		docker buildx imagetools create --tag "${image%:*}:${TAG}" "$image"
		echo "[+] Tagged ${image} as ${TAG}"
	else
		join "$image"
	fi
done
