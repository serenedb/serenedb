#!/usr/bin/env bash
# Formatting and code generation for the DuckDB family, done the way DuckDB does them: through its own
# scripts/format.py, generators and Makefile targets with their pinned tools. No rule about what is
# formatted or generated is copied here.
#
# Directories: duckdb, duckdb_avro, duckdb_azure, duckdb_httpfs, duckdb_iceberg, duckdb_inet,
# duckdb_markdown, duckdb_postgres, duckdb_spatial and database-connector (submodules under third_party/)
# and duckdb_clickhouse (in-tree). Without <dir> arguments, every checked-out one.
#
# scripts/duckdb_family.sh format [--check] [--staged | --all] [<dir>...]
#   Formats the files changed against HEAD, staged or not; with --staged only the files staged for
#   commit (the pre-commit hook); with --all every file, as DuckDB's `make format-fix`. format.py
#   decides what is formatted and how: C/C++ with clang-format 11.0.1, CMakeLists.txt with
#   cmake-format, Python with black, sqllogictest files, the typos spell check, and it skips the files
#   it ignores or that are generated. duckdb and database-connector run their own scripts/format.py,
#   the other extensions and duckdb_clickhouse run duckdb's over src/ and test/, as extension-ci-tools
#   does. --check reports what would change and exits 1 instead of writing.
#
# scripts/duckdb_family.sh format [--check] --files <path>... <dir>
#   Only these files of one directory (paths relative to it): the way to format what an earlier
#   commit left unformatted, then commit the result as `fixup! <that commit's subject>` and fold it
#   in with an autosquash rebase.
#
# scripts/duckdb_family.sh format --check --range <a>..<b> <dir>
#   Every first-parent commit in the range must be formatted on its own. Each one is checked out in a
#   scratch clone, format.py formats the files it changed against its first parent, and every line
#   it wrote that formatting rewrites is reported: for a commit the lines it added or changed, for a
#   merge the lines that match none of its parents (its conflict resolution). Upstream's lines,
#   including everything a merge brings in, and `duckdb ext patch:` commits (DuckDB's own patches,
#   kept verbatim so `git patch-id` finds them upstream) are never reported. For a DuckDB update,
#   check from upstream main's tip to HEAD: the merges, the patchset and the regen: commit; for a
#   pull request into a version branch, from the branch's head.
#
# scripts/duckdb_family.sh regen [--check]
#   duckdb only. Builds the regen: commit of the patch commits under it (HEAD, or HEAD~1 when HEAD is
#   a regen: commit) in a scratch clone: a DuckDB update's one regen: commit, or a pull request's own
#   on top of a version branch. DuckDB's generators run in DuckDB's order: `make generate-files` (C
#   API v1, functions, metrics, settings, serialization, util, storage info, enum util, HTML template,
#   the PEG grammar and transformer, then format-main), then scripts/capi_v2_regen.sh (C API v2) and
#   scripts/generate_enums.py (the json enums), the generators DuckDB runs outside generate-files. In
#   the clone `main` is that commit, so format-main formats exactly the files the generators wrote.
#   All run a second time, which must change nothing. The new regen: commit replaces the old one or
#   goes on top, and the generated files in the work tree are updated to it; the work tree must be
#   clean. When the generators change nothing there is no regen: commit, and an old one is dropped.
#   --check compares with the existing regen: commit, or, when HEAD is not one, checks that the
#   generators change nothing.
#
# Tools live under ~/.cache/serenedb-duckdb: DuckDB's Makefile creates its format venv (clang_format
# 11.0.1, black 24, cmake-format), its capigen venv and the generator dependencies there, the format
# venv gets the typos version of the Makefile's spell_tools target, and uv, which capi_v2_regen.sh
# needs, is installed there when it is not on PATH.

set -euo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel)"
THIRD_PARTY="$REPO_ROOT/third_party"
DUCKDB="$THIRD_PARTY/duckdb"
SUBMODULES=(duckdb duckdb_avro duckdb_azure duckdb_httpfs duckdb_iceberg duckdb_inet duckdb_markdown duckdb_postgres
	duckdb_spatial database-connector)
INTREE=(duckdb_clickhouse)
CACHE="${XDG_CACHE_HOME:-$HOME/.cache}/serenedb-duckdb"
FORMAT_VENV="$CACHE/format-venv"
FORMAT_PYTHON="$FORMAT_VENV/bin/python"
CAPIGEN_VENV="$CACHE/capigen-venv"
GENERATE_VENV="$CACHE/generate-venv"
UV_VENV="$CACHE/uv-venv"
REGEN_MESSAGE='regen: regenerate the grammar, settings, functions, serialization and storage info'

usage() {
	sed -n '2,/^$/{s/^# \{0,1\}//;p}' "$0"
}

die() {
	echo "error: $*" >&2
	exit 1
}

is_intree() {
	local dir
	for dir in "${INTREE[@]}"; do
		[[ "$dir" == "$1" ]] && return 0
	done
	return 1
}

is_known() {
	local dir
	for dir in "${SUBMODULES[@]}" "${INTREE[@]}"; do
		[[ "$dir" == "$1" ]] && return 0
	done
	return 1
}

is_present() {
	if is_intree "$1"; then
		[[ -d "$THIRD_PARTY/$1" ]]
	else
		[[ -e "$THIRD_PARTY/$1/.git" ]]
	fi
}

format_tools_ready=""

setup_format_tools() {
	[[ -z "$format_tools_ready" ]] || return 0
	[[ -e "$DUCKDB/.git" ]] || die "third_party/duckdb is not checked out; its format.py and Makefile are needed"
	make -s -C "$DUCKDB" FORMAT_VENV="$FORMAT_VENV" format_venv >/dev/null
	local version
	version=$(sed -n '/^spell_tools:/,/^$/s/.*VERSION=\([0-9.]*\);.*/\1/p' "$DUCKDB/Makefile")
	[[ -n "$version" ]] || die "no typos version in the spell_tools target of third_party/duckdb/Makefile"
	if [[ "$("$FORMAT_VENV/bin/typos" --version 2>/dev/null)" != "typos-cli $version" ]]; then
		"$FORMAT_PYTHON" -m pip install --quiet "typos==$version"
	fi
	format_tools_ready=1
}

setup_regen_tools() {
	if [[ ! -x "$GENERATE_VENV/bin/python" ]]; then
		python3 -m venv "$GENERATE_VENV"
		make -s -C "$DUCKDB" PYTHON="$GENERATE_VENV/bin/python" generate-files-deps >/dev/null
	fi
	make -s -C "$DUCKDB" CAPIGEN_VENV="$CAPIGEN_VENV" capigen_venv >/dev/null
	if ! command -v uv >/dev/null 2>&1; then
		if [[ ! -x "$UV_VENV/bin/uv" ]]; then
			python3 -m venv "$UV_VENV"
			"$UV_VENV/bin/pip" install --quiet uv
		fi
		PATH="$UV_VENV/bin:$PATH"
	fi
}

# run_format_py <dir> <work dir> <format.py arguments...>
run_format_py() {
	local dir=$1 work=$2
	shift 2
	setup_format_tools
	case "$dir" in
	duckdb)
		(cd "$work" && "$FORMAT_PYTHON" scripts/format.py "$@")
		;;
	database-connector)
		local check=()
		[[ " $* " == *" --check "* ]] && check=(--check)
		(cd "$work" && PATH="$FORMAT_VENV/bin:$PATH" python3 scripts/format.py "${check[@]}")
		;;
	*)
		local dirs=() sub
		for sub in src test; do
			[[ -d "$work/$sub" ]] && dirs+=("$sub")
		done
		"$FORMAT_PYTHON" "$DUCKDB/scripts/format.py" -C "$work" "$@" -d "${dirs[@]}"
		;;
	esac
}

format_dir() {
	local dir=$1 scope=$2 check=$3
	local path="$THIRD_PARTY/$dir"
	local action=(--fix --noconfirm --silent)
	[[ -n "$check" ]] && action=(--check)
	if [[ "$scope" == all ]]; then
		run_format_py "$dir" "$path" --all "${action[@]}"
	elif [[ "$scope" == files ]]; then
		local status=0 file
		for file in "${files[@]}"; do
			[[ -f "$path/$file" ]] || die "$dir/$file does not exist"
			run_format_py "$dir" "$path" "$file" "${action[@]}" || status=1
		done
		return $status
	elif is_intree "$dir"; then
		local files status=0 file
		if [[ "$scope" == staged ]]; then
			files=$(git -C "$REPO_ROOT" diff --cached --name-only --diff-filter=d --relative="third_party/$dir" HEAD)
		else
			files=$({
				git -C "$REPO_ROOT" diff --name-only --diff-filter=d --relative="third_party/$dir" HEAD
				git -C "$path" ls-files --others --exclude-standard
			} | sort -u)
		fi
		for file in $files; do
			run_format_py "$dir" "$path" "$file" "${action[@]}" || status=1
		done
		return $status
	elif [[ "$scope" == staged ]]; then
		if [[ -z "$(git -C "$path" diff --cached --name-only)" ]]; then
			return 0
		fi
		run_format_py "$dir" "$path" --staged "${action[@]}"
	else
		if [[ -z "$(git -C "$path" diff --name-only HEAD)" ]]; then
			return 0
		fi
		run_format_py "$dir" "$path" HEAD "${action[@]}"
	fi
}

# changed_lines <diff -U0 on stdin> <old|new>: "<file>:<line>" for every line on that side of the hunks
changed_lines() {
	awk -v side="$1" '
		/^\+\+\+ / { file = substr($2, 3); next }
		/^@@ / {
			split(side == "old" ? substr($2, 2) : substr($3, 2), range, ",")
			start = range[1] + 0
			count = (2 in range) ? range[2] + 0 : 1
			if (count == 0 && side == "old") { count = 1; if (start == 0) start = 1 }
			for (i = 0; i < count; i++) print file ":" (start + i)
		}'
}

check_range() {
	local dir=$1 range=$2
	local repo="$THIRD_PARTY/$dir"
	is_intree "$dir" && die "--range needs a directory with its own history, not $dir"
	local scratch
	scratch=$(mktemp -d)
	trap "rm -rf '$scratch'" EXIT
	local work="$scratch/$dir"
	git clone --quiet --shared --no-checkout "$repo" "$work"
	[[ "$dir" == duckdb ]] || ln -s "$DUCKDB" "$scratch/duckdb"
	local commits commit parent count=0 failed=0
	commits=$(git -C "$repo" rev-list --reverse --first-parent "$range")
	[[ -n "$commits" ]] || die "no commits in $range"
	for commit in $commits; do
		[[ "$(git -C "$repo" log -1 --format=%s "$commit")" == "duckdb ext patch:"* ]] && continue
		count=$((count + 1))
		parent=$(git -C "$repo" rev-parse "$commit^1")
		git -C "$work" checkout --quiet --force "$commit"
		if [[ -L "$work/.clang-format" && ! -e "$work/.clang-format" ]]; then
			mkdir -p "$(dirname "$work/$(readlink "$work/.clang-format")")"
			ln -sf "$DUCKDB/.clang-format" "$work/$(readlink "$work/.clang-format")"
		fi
		[[ -e "$work/.clang-format" ]] || die "$commit: .clang-format does not resolve in the scratch clone"
		run_format_py "$dir" "$work" "$parent" --fix --noconfirm --silent >"$scratch/format.log" 2>&1 || true
		git -C "$work" diff -U0 --no-color --no-ext-diff | changed_lines old | sort -u >"$scratch/rewritten"
		git -C "$repo" diff -U0 --no-color --no-ext-diff "$parent" "$commit" | changed_lines new | sort -u >"$scratch/touched"
		local other
		for other in $(git -C "$repo" log -1 --format=%P "$commit" | cut -d' ' -f2-); do
			[[ "$other" == "$parent" ]] && continue
			git -C "$repo" diff -U0 --no-color --no-ext-diff "$other" "$commit" | changed_lines new | sort -u |
				comm -12 "$scratch/touched" - >"$scratch/touched.merge"
			mv "$scratch/touched.merge" "$scratch/touched"
		done
		comm -12 "$scratch/rewritten" "$scratch/touched" >"$scratch/bad"
		if [[ -s "$scratch/bad" ]]; then
			failed=$((failed + 1))
			echo "$(git -C "$repo" log -1 --format='%h %s' "$commit")"
			sort -t: -k1,1 -k2,2n "$scratch/bad" | awk -F: '
				function flush() {
					if (file != "") print "  " file ": " ranges " (" lines " lines)"
				}
				function close_range() {
					ranges = ranges (ranges == "" ? "" : ", ") (first == last ? first : first "-" last)
				}
				$1 != file { if (file != "") { close_range(); flush() } file = $1; ranges = ""; lines = 0; first = last = $2 + 0 }
				$1 == file && $2 + 0 > last + 1 { close_range(); first = $2 + 0 }
				{ last = $2 + 0; lines++ }
				END { if (file != "") { close_range(); flush() } }'
		fi
	done
	echo "$dir: $count commits checked, $failed not formatted"
	[[ "$failed" -eq 0 ]]
}

run_generators() {
	local work=$1 log=$2
	(cd "$work" && DUCKDB_FORMAT_SKIP_FETCH=1 make generate-files PYTHON="$GENERATE_VENV/bin/python" \
		FORMAT_VENV="$FORMAT_VENV" CAPIGEN_VENV="$CAPIGEN_VENV") >>"$log" 2>&1 || die "make generate-files failed, see $log"
	(cd "$work" && scripts/capi_v2_regen.sh) >>"$log" 2>&1 || die "scripts/capi_v2_regen.sh failed, see $log"
	(cd "$work" && "$GENERATE_VENV/bin/python" scripts/generate_enums.py) >>"$log" 2>&1 ||
		die "scripts/generate_enums.py failed, see $log"
	git -C "$work" add --update
	git -C "$work" write-tree
}

regen() {
	local check=$1
	[[ -e "$DUCKDB/.git" ]] || die "third_party/duckdb is not checked out"
	local head base old_regen="" message=$REGEN_MESSAGE
	head=$(git -C "$DUCKDB" rev-parse HEAD)
	if [[ "$(git -C "$DUCKDB" log -1 --format=%s HEAD)" == regen:* ]]; then
		old_regen=$head
		base=$(git -C "$DUCKDB" rev-parse HEAD~1)
		message=$(git -C "$DUCKDB" log -1 --format=%B HEAD)
	else
		base=$head
	fi
	if [[ -z "$check" && -n "$(git -C "$DUCKDB" status --porcelain --untracked-files=no)" ]]; then
		die "third_party/duckdb has uncommitted changes"
	fi
	setup_format_tools
	setup_regen_tools
	local scratch
	scratch=$(mktemp -d)
	trap "rm -rf '$scratch'" EXIT
	git clone --quiet --shared --no-checkout "$DUCKDB" "$scratch/duckdb"
	git -C "$scratch/duckdb" checkout --quiet --force -B main "$base"
	local tree second
	tree=$(run_generators "$scratch/duckdb" "$scratch/regen.log")
	second=$(run_generators "$scratch/duckdb" "$scratch/regen.log")
	if [[ "$tree" != "$second" ]]; then
		git -C "$scratch/duckdb" diff --stat "$tree" "$second" >&2
		die "a second generator run changed the files above"
	fi
	local untracked
	untracked=$(git -C "$scratch/duckdb" ls-files --others --exclude-standard | grep -v '^api_spec/uv.lock$' || true)
	[[ -z "$untracked" ]] || die "the generators created untracked files: $untracked"
	local base_tree
	base_tree=$(git -C "$DUCKDB" rev-parse "$base^{tree}")
	if [[ -n "$check" ]]; then
		local expected=$head
		[[ -n "$old_regen" ]] && expected=$old_regen
		if [[ "$tree" != "$(git -C "$DUCKDB" rev-parse "$expected^{tree}")" ]]; then
			mkdir -p "$CACHE"
			git -C "$scratch/duckdb" diff "$expected" "$tree" >"$CACHE/regen-check.diff"
			git -C "$scratch/duckdb" diff --stat "$expected" "$tree"
			[[ -n "$old_regen" ]] ||
				die "HEAD needs a regen: commit: the generators change the files above; the diff is in $CACHE/regen-check.diff"
			die "the regen: commit differs from what the generators produce; the diff is in $CACHE/regen-check.diff"
		fi
		if [[ -z "$old_regen" ]]; then
			echo "nothing to regenerate"
		elif [[ "$tree" == "$base_tree" ]]; then
			die "the regen: commit is empty; scripts/duckdb_family.sh regen drops it"
		else
			echo "regen: commit matches the generators"
		fi
		return 0
	fi
	local commit=$base
	if [[ "$tree" != "$base_tree" ]]; then
		commit=$(git -C "$scratch/duckdb" commit-tree "$tree" -p "$base" -m "$message")
		git -C "$scratch/duckdb" update-ref refs/heads/regen "$commit"
		git -C "$DUCKDB" fetch --quiet --no-tags "$scratch/duckdb" refs/heads/regen
	elif [[ -z "$old_regen" ]]; then
		echo "nothing to regenerate"
		return 0
	fi
	git -C "$DUCKDB" update-ref -m "duckdb_family.sh regen" HEAD "$commit" "$head"
	local path
	git -C "$DUCKDB" diff --name-only "$head" "$commit" | while IFS= read -r path; do
		if git -C "$DUCKDB" cat-file -e "$commit:$path" 2>/dev/null; then
			mkdir -p "$(dirname "$DUCKDB/$path")"
			git -C "$DUCKDB" show "$commit:$path" >"$DUCKDB/$path"
		else
			rm -f "$DUCKDB/$path"
		fi
	done
	git -C "$DUCKDB" diff --name-only "$head" "$commit" | git -C "$DUCKDB" update-index --add --remove --stdin
	if [[ "$commit" == "$base" ]]; then
		echo "the generators change nothing: the regen: commit is dropped"
		return 0
	fi
	git -C "$DUCKDB" log -1 --stat --format='%h %s' "$commit" | tail -n 3
}

[[ $# -ge 1 ]] || {
	usage
	exit 2
}
mode=$1
shift
check="" scope=changed range=""
dirs=()
files=()
while [[ $# -gt 0 ]]; do
	case "$1" in
	--check) check=1 ;;
	--staged) scope=staged ;;
	--all) scope=all ;;
	--files)
		scope=files
		while [[ $# -gt 1 && "$2" != -* ]] && ! is_known "$2"; do
			files+=("$2")
			shift
		done
		[[ ${#files[@]} -gt 0 ]] || die "--files needs paths"
		;;
	--range)
		[[ $# -ge 2 ]] || die "--range needs <a>..<b>"
		range=$2
		shift
		;;
	-h | --help)
		usage
		exit 0
		;;
	-*) die "unknown option $1" ;;
	*)
		is_known "$1" || die "unknown directory $1"
		dirs+=("$1")
		;;
	esac
	shift
done

case "$mode" in
format)
	if [[ -n "$range" ]]; then
		[[ -n "$check" ]] || die "--range only checks: add --check"
		[[ ${#dirs[@]} -eq 1 ]] || die "--range needs exactly one directory"
		check_range "${dirs[0]}" "$range"
		exit
	fi
	[[ "$scope" != files || ${#dirs[@]} -eq 1 ]] || die "--files needs exactly one directory"
	if [[ ${#dirs[@]} -eq 0 ]]; then
		for dir in "${SUBMODULES[@]}" "${INTREE[@]}"; do
			is_present "$dir" && dirs+=("$dir")
		done
	fi
	failed=()
	for dir in "${dirs[@]}"; do
		is_present "$dir" || die "$dir is not checked out"
		format_dir "$dir" "$scope" "$check" || failed+=("$dir")
	done
	[[ ${#failed[@]} -eq 0 ]] || die "not formatted: ${failed[*]}"
	;;
regen)
	[[ ${#dirs[@]} -eq 0 && "$scope" == changed && -z "$range" ]] || die "regen takes only --check"
	regen "$check"
	;;
-h | --help) usage ;;
*)
	usage
	exit 2
	;;
esac
