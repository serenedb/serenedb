#!/usr/bin/env bash
set -u
root=/home/mironov/projects/serenedb/serenedb
fork=$root/third_party/duckdb
status=0
for commit in "$@"; do
	git -C "$fork" checkout -q --detach "$commit"
	files=()
	while IFS= read -r f; do
		case "$f" in
		*.cpp | *.hpp | *.h | *.c | *.cc | *.hh | *.ipp) files+=("duckdb/$f") ;;
		esac
	done < <(git -C "$fork" diff --name-only "$commit^" "$commit" -- src extension)
	subject=$(git -C "$fork" log -1 --format=%s "$commit")
	if [[ ${#files[@]} -eq 0 ]]; then
		echo "${commit:0:11} no C++ files ($subject)"
		continue
	fi
	out=$(cd "$root" && scripts/format_duckdb.sh --check --files "${files[@]}" 2>&1)
	if grep -q "needs formatting" <<<"$out"; then
		status=1
		echo "${commit:0:11} NEEDS FORMAT ($subject): $(grep 'needs formatting' <<<"$out" | tr '\n' ' ')"
	else
		echo "${commit:0:11} formatted ($subject)"
	fi
done
exit $status
