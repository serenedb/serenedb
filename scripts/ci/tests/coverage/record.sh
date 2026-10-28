#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "$0")"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
clang++-23 -std=c++26 -O0 -fcoverage-compilation-dir=. -fprofile-instr-generate -fcoverage-mapping \
	-fcoverage-mcdc -Xclang -fmcdc-max-conditions=8 -mllvm -enable-single-byte-coverage=true \
	-mllvm -conditional-counter-update asserts.cpp -o "$tmp/asserts"
LLVM_PROFILE_FILE="$tmp/asserts.profraw" "$tmp/asserts"
llvm-profdata-23 merge -sparse "$tmp/asserts.profraw" -o "$tmp/asserts.profdata"
{
	llvm-cov-23 export -format=text -instr-profile="$tmp/asserts.profdata" "$tmp/asserts"
	echo
} >asserts.json
llvm-cov-23 export -format=lcov -instr-profile="$tmp/asserts.profdata" "$tmp/asserts" >asserts.lcov
