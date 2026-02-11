#!/usr/bin/env bash

IN="$1"
OUT="$2"
VERSION="$3"

mkdir -p ${OUT%/*}

section=`echo $IN | sed -e 's:.*\([0-9]\):\1:'`
command=`echo $IN | sed -e 's:.*/\([^\.]*\).[0-9]:\1:'`

(
  echo '<COMMAND> <SECTION> "<VERSION>" "SereneDB" "SereneDB"'
  cat $IN
) | sed \
  -f `dirname $0`/../resources/scripts/man.sed \
  -e "s/<SECTION>/$section/g" \
  -e "s/<COMMAND>/$command/g" \
  -e "s/<VERSION>/$VERSION/g" \
  > $OUT
