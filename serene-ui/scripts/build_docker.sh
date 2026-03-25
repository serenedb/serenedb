#!/bin/bash
set -eo pipefail

[ -z "$DOCKER_REGISTRY" ] && exit 1        # DOCKER_REGISTRY="serenedb"
[ -z "$IMAGE" ] && exit 1                  # IMAGE="serene-ui"
[ -z "$DOCKER_TAG" ] && exit 1             # DOCKER_TAG="1.2.3"
[ -z "$PUSH_IMAGE_TO_REGISTRY" ] && exit 1 # PUSH_IMAGE_TO_REGISTRY="false"

FULL_IMAGE_NAME="${DOCKER_REGISTRY}/${IMAGE}:${DOCKER_TAG}"
[ -d logs ] || mkdir logs
echo ">>> Building $FULL_IMAGE_NAME"
docker build --pull --no-cache \
	--tag "$FULL_IMAGE_NAME" \
	--label "org.opencontainers.image.version=${DOCKER_TAG}" \
	--label "org.opencontainers.image.created=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
	--label "org.opencontainers.image.revision=$(git rev-parse HEAD 2>/dev/null || echo 'unknown')" \
	--label "org.opencontainers.image.title=${IMAGE}" \
	. 2>&1 | tee -a logs/build.log
echo ">>> PUSH_IMAGE_TO_REGISTRY='${PUSH_IMAGE_TO_REGISTRY}' (hex: $(echo -n "$PUSH_IMAGE_TO_REGISTRY" | xxd -p))"
if [ "$PUSH_IMAGE_TO_REGISTRY" = true ]; then
	echo ">>> DOCKER_USERNAME set: $([ -n "$DOCKER_USERNAME" ] && echo yes || echo no), DOCKER_PASSWORD set: $([ -n "$DOCKER_PASSWORD" ] && echo yes || echo no)"
	if [ -n "$DOCKER_USERNAME" ] && [ -n "$DOCKER_PASSWORD" ]; then
		echo ">>> DOCKER_REGISTRY='$DOCKER_REGISTRY' (contains dot/colon: $(echo "$DOCKER_REGISTRY" | grep -q '[.:]' && echo yes || echo no))"
		if [[ "$DOCKER_REGISTRY" =~ [.:] ]]; then
			echo ">>> Logging in to $DOCKER_REGISTRY"
			echo "$DOCKER_PASSWORD" | docker login "$DOCKER_REGISTRY" -u "$DOCKER_USERNAME" --password-stdin
			trap "docker logout $DOCKER_REGISTRY" EXIT
		else
			echo ">>> Logging in to $DOCKER_REGISTRY"
			echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
			trap "docker logout" EXIT
		fi
	else
		echo ">>> Skipping login (credentials not set)"
	fi
	echo ">>> Pushing $FULL_IMAGE_NAME"
	docker push "$FULL_IMAGE_NAME" 2>&1 | tee -a logs/build.log
	extra_tags="$EXTRA_DOCKER_TAGS"
	if [ "$PUSH_RELEASE" = true ]; then
		extra_tags="latest $extra_tags"
	fi
	IFS=', ' read -ra EXTRA_TAGS <<<"$extra_tags"
	for tag in "${EXTRA_TAGS[@]}"; do
		[ -z "$tag" ] && continue
		TARGET="${DOCKER_REGISTRY}/${IMAGE}:${tag}"
		echo ">>> Tagging and pushing: $TARGET"
		docker tag "$FULL_IMAGE_NAME" "$TARGET"
		docker push "$TARGET" 2>&1 | tee -a logs/build.log
	done
fi
