#!/usr/bin/env bash

ferr() { echo "$*"; exit 1; }

if [[ $1 == "-j" ]]; then
  threads=$2
  shift 2
else
  threads=$(nproc)
fi

if [[ -n $* ]]; then
  files=( "$@" )
else
  files=( server/ clients/ libs/ )
fi

echo "cppcheck version: $(cppcheck --version)"
cppcheck "$@" \
  -j $threads \
  --xml --xml-version=2 \
  -I server \
  -I clients \
  -I build/serened \
  -I build/clients \
  -I build/libs \
  -I libs \
  -D DEFINE_FACTORY_DEFAULT \
  --std=c++20 \
  --enable=warning,performance,portability,missingInclude \
  --force \
  --quiet \
  --platform=unix64 \
  --inline-suppr \
  --suppress="*:grammar.cpp" \
  --suppress="*:tokens.cpp" \
  --suppress="*:tokens.ll" \
  --suppress="*:libs/basics/endian.h" \
  --suppress="*:libs/basics/fpconv.cpp" \
  --suppress="constStatement" \
  --suppress="cppcheckError" \
  --suppress="duplicateCondition" \
  --suppress="duplicateConditionalAssign" \
  --suppress="internalAstError" \
  --suppress="mismatchingContainerExpression" \
  --suppress="missingInclude" \
  --suppress="passedByValue" \
  --suppress="redundantAssignInSwitch" \
  --suppress="redundantAssignment" \
  --suppress="shadowFunction" \
  --suppress="shadowVar" \
  --suppress="shadowVariable" \
  --suppress="stlFindInsert" \
  --suppress="syntaxError" \
  --suppress="uninitMemberVar" \
  --suppress="unreadVariable" \
  --suppress="useStlAlgorithm" \
  --suppress="variableScope" \
  "${files[@]}" \
  2> cppcheck.out.xml.$$ \
  || ferr "failed to run cppcheck"

grep -E "<error |<location|</error>" cppcheck.out.xml.$$ \
  | sed -e 's#^.*id="\([^"]*\)".*msg="\([^"]*\)".*#\1: \2#' \
        -e 's#^.*file="\([^"]*\)".*line="\([^"]*\)".*#    \1:\2#' \
        -e 's:&apos;:":g' \
        -e 's:&gt;:>:g' \
        -e 's:&lt;:<:g' \
        -e 's:</error>::' \
        -e 's#^\s*$##' \
  > cppcheck.log

sed -e "s:file=\":file=\"$(pwd)/:g" \
  < cppcheck.out.xml.$$ > cppcheck.xml

cat cppcheck.log
rm cppcheck.out.xml.$$
