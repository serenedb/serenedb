#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
[[ $# -le 1 ]] || {
	echo "usage: $0 [build_dir]" >&2
	exit 1
}
build_dir=${1:-"$root/build_ffi"}
build_dir=$(cd "$build_dir" && pwd)
cc=${CC:-cc}
case $(uname -s) in
Linux | Darwin) ;;
*)
	echo "unsupported platform: $(uname -s)" >&2
	exit 1
	;;
esac

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cmake --install "$build_dir" --prefix "$work/original" --component IResearchFFI
prefix="$work/relocated prefix"
mv "$work/original" "$prefix"
mkdir "$work/consumer"
consumers=(consumer consumer_memory consumer_rows consumer_search_types regression)
for name in "${consumers[@]}"; do
	cp "$root/examples/iresearch-ffi/$name.c" "$work/consumer/"
done
cat >"$work/consumer/header_check.c" <<'EOF'
#include <iresearch_ffi.h>
int main(void) { return 0; }
EOF
cat >"$work/consumer/CMakeLists.txt" <<'EOF'
cmake_minimum_required(VERSION 3.26)
project(installed_iresearch_consumer LANGUAGES C)
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
find_package(IResearchFFI CONFIG REQUIRED)
get_filename_component(prefix "${CMAKE_PREFIX_PATH}" REALPATH)
get_target_property(configurations iresearch::ffi IMPORTED_CONFIGURATIONS)
foreach(configuration IN LISTS configurations)
    get_target_property(location iresearch::ffi IMPORTED_LOCATION_${configuration})
    get_filename_component(location "${location}" REALPATH)
    string(FIND "${location}" "${prefix}/" inside_prefix)
    if(NOT inside_prefix EQUAL 0)
        message(FATAL_ERROR "Library resolved outside relocated prefix: ${location}")
    endif()
    message(STATUS "Installed library: ${location}")
endforeach()
message(STATUS "Installed package: ${IResearchFFI_DIR}")
foreach(name header_check consumer consumer_memory consumer_rows consumer_search_types regression)
    add_executable(${name} ${name}.c)
    target_compile_options(${name} PRIVATE -Wall -Wextra -Werror)
    target_link_libraries(${name} PRIVATE iresearch::ffi m)
endforeach()
file(GENERATE OUTPUT "${CMAKE_BINARY_DIR}/library-location.txt"
    CONTENT "$<TARGET_FILE:iresearch::ffi>\n")
EOF
cmake -S "$work/consumer" -B "$work/consumer-build" -G Ninja \
	-DCMAKE_C_COMPILER="$cc" -DCMAKE_PREFIX_PATH="$prefix" \
	-DCMAKE_FIND_USE_PACKAGE_REGISTRY=OFF \
	-DCMAKE_FIND_USE_SYSTEM_PACKAGE_REGISTRY=OFF
cmake --build "$work/consumer-build" -j 2
IFS= read -r installed_lib <"$work/consumer-build/library-location.txt"
case $(uname -s) in
Linux) ldd "$installed_lib" ;;
Darwin) otool -L "$installed_lib" ;;
esac
"$work/consumer-build/header_check"
for name in "${consumers[@]}"; do
	mkdir "$work/$name.run"
	(
		cd "$work/$name.run"
		echo "installed consumer: $name"
		"$work/consumer-build/$name"
		if [[ $name == regression ]]; then
			"$work/consumer-build/$name" --reader-only
		fi
	)
done
echo "relocated installed package and plain C consumers passed"
