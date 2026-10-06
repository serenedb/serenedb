#!/usr/bin/env bash
set -euo pipefail

usage() {
	echo "usage: $0 [--no-submodules] <path> <branch> [<base, default origin/main>]" >&2
	exit 2
}

submodules=true
if [[ ${1:-} == --no-submodules ]]; then
	submodules=false
	shift
fi
[[ $# -ge 2 && $# -le 3 ]] || usage

main=$(dirname "$(git rev-parse --path-format=absolute --git-common-dir)")
path=$(realpath -m "$1")
branch=$2
base=${3:-origin/main}

if [[ -e $path ]]; then
	echo "$path already exists" >&2
	exit 1
fi

if git -C "$main" show-ref --verify --quiet "refs/heads/$branch"; then
	git -C "$main" worktree add "$path" "$branch"
elif git -C "$main" show-ref --verify --quiet "refs/remotes/origin/$branch"; then
	git -C "$main" worktree add -b "$branch" "$path" "origin/$branch"
else
	git -C "$main" worktree add --no-track -b "$branch" "$path" "$base"
fi

if $submodules; then
	common=$(git -C "$main" rev-parse --path-format=absolute --git-common-dir)
	gitdir=$(git -C "$path" rev-parse --path-format=absolute --git-dir)
	jobs=$(($(nproc) / 4))
	if ((jobs < 1)); then
		jobs=1
	fi
	git -C "$path" ls-files -s | awk '$1 == "160000" { print $2, $4 }' |
		xargs -r -n 2 -P "$jobs" bash -c '
			set -euo pipefail
			common=$1 gitdir=$2 path=$3 main=$4 sha=$5 sub=$6
			src=$common/modules/$sub
			dst=$gitdir/modules/$sub
			if [[ ! -d $src ]]; then
				echo "$sub: not initialized in $main (git -C $main submodule update --init -- $sub)" >&2
				exit 1
			fi
			mkdir -p "$(dirname "$dst")" "$path/$sub"
			cp -al "$src" "$dst"
			git config -f "$dst/config" core.worktree "$path/$sub"
			if [[ -f $dst/config.worktree ]]; then
				git config -f "$dst/config.worktree" core.worktree "$path/$sub"
			fi
			printf "gitdir: %s\n" "$dst" >"$path/$sub/.git"
			if ! git --git-dir="$dst" --work-tree="$path/$sub" -c advice.detachedHead=false \
				checkout -q -f --detach "$sha"; then
				echo "$sub: $sha is missing locally; fetch it first: git -C $main/$sub fetch origin $sha" >&2
				exit 1
			fi
		' _ "$common" "$gitdir" "$path" "$main"
fi

echo "worktree: $path ($branch)"
echo "remove:   git -C $main worktree remove --force $path"
