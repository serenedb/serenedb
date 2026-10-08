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
build_dir=${1:-"$root/build"}
if [[ $build_dir != /* ]]; then
	build_dir="$PWD/$build_dir"
fi
build_dir=$(cd "$build_dir" && pwd)
cc=${CC:-cc}
cxx=${CXX:-c++}

if [[ $skip_build -eq 0 ]]; then
	cmake --build "$build_dir" --target iresearch-ffi
fi

case $(uname -s) in
Darwin)
	lib="$build_dir/iresearch/libiresearch-ffi.dylib"
	rpath='@executable_path'
	symbols=$(nm -gU "$lib")
	;;
Linux)
	lib="$build_dir/iresearch/libiresearch-ffi.so"
	rpath='$ORIGIN'
	symbols=$(nm -D --defined-only "$lib")
	;;
*)
	echo "unsupported platform: $(uname -s)" >&2
	exit 1
	;;
esac
[[ -f $lib ]] || {
	echo "shared library missing: $lib" >&2
	exit 1
}
exported=$(awk 'NF >= 3 { print $NF }' <<<"$symbols")
[[ -n $exported ]] || {
	echo "no exported API symbols in $lib" >&2
	exit 1
}
foreign=$(awk '$0 !~ /^_?irs_ffi_/ { print }' <<<"$exported")
if [[ -n $foreign ]]; then
	echo "symbols outside the C API:" >&2
	echo "$foreign" >&2
	exit 1
fi
for symbol in irs_ffi_index_create_memory irs_ffi_index_add_row irs_ffi_index_write irs_ffi_reader_open_bytes irs_ffi_search_typed irs_ffi_search_filtered; do
	if ! awk -v symbol="$symbol" '$0 == symbol || $0 == "_" symbol { found = 1 } END { exit !found }' <<<"$exported"; then
		echo "missing API symbol: $symbol" >&2
		exit 1
	fi
done

echo "exported symbols: $(awk 'END { print NR }' <<<"$exported"), all irs_ffi_*"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
cp "$root/iresearch/ffi/iresearch_ffi.h" "$work/"
cp "$lib" "$work/"

shopt -s nullglob
sources=("$root"/examples/iresearch-ffi/*.c "$root"/examples/iresearch-ffi/*.cpp)
[[ ${#sources[@]} -gt 0 ]] || {
	echo "no external consumers found" >&2
	exit 1
}
cp "${sources[@]}" "$work/"
roaring_lib=
for src in "${sources[@]}"; do
	filename=${src##*/}
	name=${filename%.*}
	extra=()
	if [[ $filename == *.cpp ]]; then
		compiler=$cxx
		standard=c++20
	else
		compiler=$cc
		standard=c11
	fi
	if LC_ALL=C grep -Eq '#include [<"]roaring/' "$src"; then
		if [[ -z $roaring_lib ]]; then
			candidates=()
			while IFS= read -r candidate; do
				candidates+=("$candidate")
			done < <(find "$build_dir" -type f -name 'libroaring*.a' -print)
			[[ ${#candidates[@]} -eq 1 ]] || {
				echo "expected one CRoaring archive, found ${#candidates[@]} in $build_dir" >&2
				exit 1
			}
			roaring_lib=${candidates[0]}
		fi
		extra=(-I"$root/third_party/croaring/include" -I"$root/third_party/croaring/cpp" "$roaring_lib")
	fi
	(
		cd "$work"
		"$compiler" -std="$standard" -Wall -Wextra -Werror -o "$name" "$filename" \
			-L. -liresearch-ffi "${extra[@]}" -lm -Wl,-rpath,"$rpath"
		mkdir "$name.run"
		cd "$name.run"
		echo "--- $name"
		../"$name"
		if [[ $name == regression ]]; then
			../"$name" --reader-only
		fi
	)
done

echo "every consumer built and ran outside the tree"
