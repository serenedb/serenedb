#!/bin/bash

[ -z "$PUSH_IMAGE_TO_REGISTRY" ] && exit 1 # PUSH_IMAGE_2_REGISTRY="false"
[ -z "$DOCKER_TAG" ] && exit 1             # DOCKER_TAG="latest"

imagename=registry.serenedb.com:5000/serene-ui:${DOCKER_TAG}

if [ "$PUSH_IMAGE_TO_REGISTRY" = true ]; then
  docker build --pull --no-cache --tag $imagename . && docker push $imagename
else
  docker build --pull --no-cache --tag $imagename .
fi
