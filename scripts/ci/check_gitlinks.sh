#!/bin/bash
# Fails when a gitlink points at a commit its repository can't serve, before
# any build job checks it out.

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
git init -q "$work"

missing=0
while read -r _ type sha path; do
	[[ "$type" == commit ]] || continue
	url=$(git config -f .gitmodules --get "submodule.${path}.url" || true)
	if [[ -z "$url" ]]; then
		echo "::error::$path is a gitlink without a .gitmodules entry"
		missing=1
		continue
	fi
	if ! git -C "$work" fetch -q --depth 1 --filter=tree:0 "$url" "$sha" 2>/dev/null; then
		echo "::error::$path points at $sha, which $url does not serve"
		missing=1
	fi
done < <(git ls-tree -r HEAD | awk '{print $1, $2, $3, $4}')
exit $missing
