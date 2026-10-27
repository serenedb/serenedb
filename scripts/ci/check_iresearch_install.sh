#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
skip_build=0
if [[ ${1:-} == --skip-build ]]; then
	skip_build=1
	shift
fi
[[ $# -le 1 ]] || {
	echo "usage: $0 [--skip-build] [build_dir]" >&2
	exit 1
}
build_dir=${1:-"$root/build_library"}
build_dir=$(cd "$build_dir" && pwd)
compiler=$(awk '/^CMAKE_CXX_COMPILER:/ {sub(/^[^=]*=/, ""); print; exit}' "$build_dir/CMakeCache.txt")

if [[ $skip_build -eq 0 ]]; then
	cmake --build "$build_dir" --target iresearch-library --parallel "${CMAKE_BUILD_PARALLEL_LEVEL:-2}"
fi

cmake --build "$build_dir" --target iresearch-package-tests --parallel 2

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cmake --install "$build_dir" --prefix "$work/original" --component IResearch
prefix="$work/relocated prefix"
mv "$work/original" "$prefix"
cp -R "$root/examples/iresearch-library" "$work/consumer"
cmake -S "$work/consumer" -B "$work/consumer-build" -G Ninja \
	-DCMAKE_CXX_COMPILER="$compiler" \
	-DCMAKE_PREFIX_PATH="$prefix" \
	"-DCMAKE_CXX_FLAGS=-Wall -Wextra -Werror" \
	-DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
	-DCMAKE_FIND_USE_PACKAGE_REGISTRY=OFF \
	-DCMAKE_FIND_USE_SYSTEM_PACKAGE_REGISTRY=OFF
cmake --build "$work/consumer-build" --parallel 2
ninja -C "$work/consumer-build" -t commands consumer consumer-shared >"$work/commands.txt"
ctest --test-dir "$work/consumer-build" --output-on-failure --timeout 120
if grep -F -- "$root/" "$work/commands.txt"; then
	echo "installed consumer uses the source tree" >&2
	exit 1
fi
if grep -F -- "$build_dir/" "$work/commands.txt"; then
	echo "installed consumer uses the library build tree" >&2
	exit 1
fi
case $(uname -s) in
Linux)
	for binary in "$work/consumer-build/consumer" "$work/consumer-build/libconsumer-shared.so"; do
		ldd "$binary" >"$work/runtime-libraries.txt"
		if grep -E 'libstdc\+\+|libc\+\+' "$work/runtime-libraries.txt"; then
			echo "installed consumer links another C++ standard library" >&2
			exit 1
		fi
	done
	;;
Darwin) otool -L "$work/consumer-build/consumer" "$work/consumer-build/libconsumer-shared.dylib" ;;
esac
echo "relocated IResearch package and external C++ consumer passed"
