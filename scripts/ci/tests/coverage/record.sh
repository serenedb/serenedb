#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "$0")"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
for source in *.cpp; do
	name=${source%.cpp}
	clang++-23 -std=c++26 -O0 -fcoverage-compilation-dir=. -fprofile-instr-generate -fcoverage-mapping \
		-fcoverage-mcdc -Xclang -fmcdc-max-conditions=8 -mllvm -enable-single-byte-coverage=true \
		-mllvm -conditional-counter-update "$source" -o "$tmp/$name"
	LLVM_PROFILE_FILE="$tmp/$name.profraw" "$tmp/$name"
	llvm-profdata-23 merge -sparse "$tmp/$name.profraw" -o "$tmp/$name.profdata"
	{
		llvm-cov-23 export -format=text -instr-profile="$tmp/$name.profdata" "$tmp/$name"
		echo
	} >"$name.json"
	llvm-cov-23 export -format=lcov -instr-profile="$tmp/$name.profdata" "$tmp/$name" >"$name.lcov"
done
