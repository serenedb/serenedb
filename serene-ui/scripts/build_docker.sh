#!/bin/bash
set -eo pipefail

[ -z "$IMAGE" ] && exit 1              # IMAGE="serenedb/serene-ui:1.2.3"
[ -z "$DOCKER_REGISTRY" ] && exit 1   # DOCKER_REGISTRY="serenedb"
[ -z "$PUSH_IMAGE_TO_REGISTRY" ] && exit 1 # PUSH_IMAGE_TO_REGISTRY="false"

FULL_IMAGE_NAME="${DOCKER_REGISTRY}/serene-ui"
[ -d logs ] || mkdir logs
docker build --pull --no-cache --tag "$IMAGE" . 2>&1 | tee -a logs/build.log
if [ "$PUSH_IMAGE_TO_REGISTRY" = "true" ]; then
  if [ -n "$DOCKER_USERNAME" ] && [ -n "$DOCKER_PASSWORD" ]; then
    if [[ "$DOCKER_REGISTRY" =~ [.:] ]]; then
      echo "$DOCKER_PASSWORD" | docker login "$DOCKER_REGISTRY" -u "$DOCKER_USERNAME" --password-stdin
      trap "docker logout $DOCKER_REGISTRY" EXIT
    else
      echo "$DOCKER_PASSWORD" | docker login -u "$DOCKER_USERNAME" --password-stdin
      trap "docker logout" EXIT
    fi
  fi
  docker push "$IMAGE" 2>&1 | tee -a logs/build.log
  extra_tags="$EXTRA_DOCKER_TAGS"
  if [ "$PUSH_RELEASE" = "true" ]; then
    extra_tags="latest $extra_tags"
  fi
  IFS=', ' read -ra EXTRA_TAGS <<< "$extra_tags"
  for tag in "${EXTRA_TAGS[@]}"; do
    [ -z "$tag" ] && continue
    docker tag "$IMAGE" "$FULL_IMAGE_NAME:$tag"
    docker push "$FULL_IMAGE_NAME:$tag" 2>&1 | tee -a logs/build.log
  done
fi
