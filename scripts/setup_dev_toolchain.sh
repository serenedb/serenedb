#!/bin/bash
set -euo pipefail

version=23
dry_run=false
for arg in "$@"; do
	case "$arg" in
	--dry-run) dry_run=true ;;
	*)
		echo "usage: sudo $0 [--dry-run]" >&2
		exit 2
		;;
	esac
done

if [[ "$dry_run" != true && $EUID -ne 0 ]]; then
	exec sudo "$0" "$@"
fi

run() {
	echo "+ $*"
	if [[ "$dry_run" != true ]]; then
		"$@"
	fi
}

source /etc/os-release
repo="deb http://apt.llvm.org/$VERSION_CODENAME/ llvm-toolchain-$VERSION_CODENAME-$version main"
key=/etc/apt/trusted.gpg.d/apt.llvm.org.asc
if ! grep -rqsxF "$repo" /etc/apt/sources.list /etc/apt/sources.list.d; then
	if [[ ! -s "$key" ]]; then
		run curl -fsSL https://apt.llvm.org/llvm-snapshot.gpg.key -o "$key"
	fi
	run sh -c "echo '$repo' > /etc/apt/sources.list.d/llvm-$version.list"
fi
run apt-get update
run apt-get install -y --no-install-recommends \
	"clang-$version" "clang-tools-$version" "clangd-$version" "clang-tidy-$version" \
	"lld-$version" "lldb-$version" "llvm-$version" "libclang-rt-$version-dev"

retarget() {
	local target=$1
	if [[ "$target" =~ ^(.*/lib/llvm-)[0-9]+(/.*)$ ]]; then
		echo "${BASH_REMATCH[1]}$version${BASH_REMATCH[2]}"
	elif [[ "$target" =~ ^(.*-)[0-9]+$ ]]; then
		echo "${BASH_REMATCH[1]}$version"
	fi
}

is_llvm() {
	[[ "$(readlink -f "$1")" =~ ^/usr/lib/llvm-[0-9]+/ ]] &&
		[[ ! "$(readlink -f "$1")" =~ ^/usr/lib/llvm-$version/ ]]
}

for dir in /usr/bin /usr/local/bin; do
	for link in "$dir"/*; do
		name=${link##*/}
		if [[ ! -L "$link" || "$name" =~ -[0-9]+(\.[a-z]+)?$ || "$name" == *clang-format* ]] || ! is_llvm "$link"; then
			continue
		fi
		target=$(readlink "$link")
		new=$(retarget "$target")
		if [[ -z "$new" ]]; then
			continue
		fi
		if [[ "$dry_run" == true || -e "$(cd "$dir" && realpath -m "$new")" ]]; then
			run ln -sfn "$new" "$link"
		else
			echo "skip $link: $new is not installed"
		fi
	done
done

update-alternatives --get-selections | while read -r group _ value; do
	if [[ "$group" == *clang-format* ]] || ! is_llvm "$value"; then
		continue
	fi
	new=$(retarget "$value")
	link=$(update-alternatives --query "$group" | sed -n 's/^Link: //p')
	if [[ -n "$new" && -n "$link" && ("$dry_run" == true || -e "$new") ]]; then
		run update-alternatives --install "$link" "$group" "$new" "${version}0"
		run update-alternatives --set "$group" "$new"
	fi
done

if [[ "$dry_run" != true ]]; then
	for tool in clang clang++ clangd lldb llvm-symbolizer; do
		if command -v "$tool" >/dev/null; then
			printf '%-16s %s\n' "$tool" "$("$tool" --version | head -1)"
		fi
	done
	echo "Reconfigure existing build directories: cmake --preset <preset> --fresh"
fi
