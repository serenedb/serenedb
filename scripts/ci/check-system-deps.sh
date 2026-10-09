#!/usr/bin/env bash

set -uo pipefail

BUILD_DIR="$1"
SANITIZERS="${2:-None}"
STATIC="${3:-Off}"

ALLOWED_PACKAGES='^(libc6|libc6-dev|linux-libc-dev|libclang-common-[0-9]+-dev|libclang-rt-[0-9]+-dev)$'
ALLOWED_GCC_FILES='^/usr/lib/gcc/[a-z0-9_-]+/[0-9]+/crt(begin|end)[ST]?\.o$'
ALLOWED_LINK_INPUTS='^(-lc|-lm|-ldl|-lrt|-lpthread|/usr/lib/llvm-[0-9]+/lib/clang/[0-9]+/lib/linux/libclang_rt\.builtins-[a-z0-9_]+\.a)$'
if [[ "$SANITIZERS" == "None" || -z "$SANITIZERS" ]]; then
	if [[ "$STATIC" == "On" ]]; then
		ALLOWED_LIBRARIES='^$'
	else
		ALLOWED_LIBRARIES='^(libc|libm)\.so\.[0-9]+$'
	fi
else
	ALLOWED_LIBRARIES='^(libc|libm|libdl|libpthread|librt|libresolv|libutil|libgcc_s|ld-linux-[a-z0-9_-]+)\.so(\.[0-9]+)?$'
fi

failed=0

mapfile -t files < <(ninja -C "$BUILD_DIR" -t deps 2>/dev/null |
	sed -n 's#^[[:space:]]\+\(/\(usr\|lib\|lib64\)/.*\)#\1#p' | xargs -r realpath -e | sort -u)
if [[ ${#files[@]} -ne 0 ]]; then
	while IFS= read -r line; do
		package="${line%%: *}"
		package="${package%%:*}"
		file="${line#*: }"
		if [[ "$line" == dpkg-query:* ]]; then
			echo "::error::build used system file not owned by any package: ${line##* }"
			failed=1
		elif [[ "$package" =~ ^libgcc-[0-9]+-dev$ && "$file" =~ $ALLOWED_GCC_FILES ]]; then
			continue
		elif ! [[ "$package" =~ $ALLOWED_PACKAGES ]]; then
			echo "::error::build used system file ${file} from ${package}"
			failed=1
		fi
	done < <(dpkg -S "${files[@]}" 2>&1 | grep -v '^diversion by')
fi

while IFS= read -r input; do
	if ! [[ "$input" =~ $ALLOWED_LINK_INPUTS ]]; then
		echo "::error::build links system input ${input}"
		failed=1
	fi
done < <(sed -n 's/^ *LINK_\(LIBRARIES\|FLAGS\) = //p' "$BUILD_DIR/build.ninja" |
	tr ' ' '\n' | grep -E '^(/usr/|-l)' | sort -u)

while IFS= read -r binary; do
	while IFS= read -r library; do
		if ! [[ "$library" =~ $ALLOWED_LIBRARIES ]]; then
			echo "::error::${binary#"$BUILD_DIR"/} needs shared library ${library}"
			failed=1
		fi
	done < <(readelf -d "$binary" 2>/dev/null | sed -n 's/.*(NEEDED).*\[\(.*\)\]/\1/p')
done < <(find "$BUILD_DIR/bin" "$BUILD_DIR/third_party/duckdb/test" -maxdepth 1 -type f -perm -u+x 2>/dev/null)

exit "$failed"
