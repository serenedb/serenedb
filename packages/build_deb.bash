#!/bin/bash

cd /serenedb

source /serenedb/packages/find_version.bash

v="$SERENEDB_DEBIAN_UPSTREAM-$SERENEDB_DEBIAN_REVISION"
ch=package_all/debian/changelog
BASE=package_all
SOURCE=packages/debian/source
TARGET=$BASE/debian/
EDITION=serenedb
EDITIONFOLDER=$SOURCE/main
ARCH=$(dpkg --print-architecture)

echo Building main debian package...

rm -rf $BASE
mkdir -p $TARGET
cp -a $EDITIONFOLDER/* $TARGET
cp -a install_all $BASE/install
for f in serenedb.init serenedb.service compat config templates preinst prerm postinst postrm rules; do
  cp $SOURCE/common/$f $TARGET/$f
  sed -e "s/@EDITION@/$EDITION/g" -i $TARGET/$f
done
echo -n "$EDITION " >$ch
cp -a $SOURCE/common/source $TARGET
echo "($v) UNRELEASED; urgency=medium" >>$ch
echo >>$ch
echo "  * New version." >>$ch
echo >>$ch
echo -n " -- SereneDB   " >>$ch
date -R >>$ch
sed -i "s/@ARCHITECTURE@/$ARCH/g" $TARGET/control

unset LC_ALL
unset LC_CTYPE
unset LANG
unset LANGUAGE

cd package_all
pwd
debian/rules binary
