#!/bin/bash

[ -z "$SDB_DEV" ] && exit 1                # SDB_DEV="Off"
[ -z "$USE_IPO" ] && exit 1                # USE_IPO="On"
[ -z "$ENSURE_VTUNE_SYMBOLS" ] && exit 1   # ENSURE_VTUNE_SYMBOLS="Off"
[ -z "$PUSH_IMAGES_2_REGISTRY" ] && exit 1 # PUSH_IMAGES_2_REGISTRY="false"
[ -z "$DOCKER_TAG" ] && source $WORKSPACE/packages/find_version.bash

#TODO (malandin) imagename should not be hardcoded

imagename=registry.serenedb.com:5000/serenedb:${DOCKER_TAG}
# containerpath=$WORKSPACE/packages/docker

rm -rf $WORKSPACE/package_all/docker
mkdir -p $WORKSPACE/package_all/docker
cd $WORKSPACE/package_all/docker
cp -a $WORKSPACE/install_serenedb/* .
cp -a $WORKSPACE/install_serenesh/* .
#rm -rf usr/bin
#mv usr/etc . && find etc -not -name 'serene-*.conf' -a -not -name 'serened.conf' -type f -exec rm -f {} +
mv usr/etc .
mv usr/var .
# TODO(mbkkt) strip binaries
tar czf install.tar.gz *
cp -a $WORKSPACE/packages/docker/* .

if [ "$PUSH_IMAGES_2_REGISTRY" = true ]; then
  docker build --pull --no-cache --tag $imagename . && docker push $imagename
#  docker rmi -f $(docker images $imagename -q)
else
  docker build --pull --no-cache --tag $imagename .
fi
