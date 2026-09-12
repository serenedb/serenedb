#!/bin/bash
# Regenerate third_party/libstemmer_c from upstream Snowball.
#
# The submodule holds build output, not source: the unmodified result of
# `make dist_libstemmer_c` run against a snowball commit. This script produces
# that output, drops it into the submodule working tree and commits it with the
# upstream SHA in the message. Pushing and bumping the submodule pointer is left
# to you.
#
#     scripts/update_libstemmer.sh            # upstream main
#     scripts/update_libstemmer.sh <ref>      # a specific tag or commit
#
# Needs a C compiler, make and perl -- all only on the machine running this,
# never at build time.
set -euo pipefail

REPO=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
SUBMODULE="$REPO/third_party/libstemmer_c"
UPSTREAM="https://github.com/snowballstem/snowball.git"
REF="${1:-main}"

for tool in cc make perl git; do
	command -v "$tool" >/dev/null || {
		echo "missing $tool" >&2
		exit 1
	}
done

[[ -d "$SUBMODULE/.git" || -f "$SUBMODULE/.git" ]] || {
	echo "third_party/libstemmer_c is not initialized: git submodule update --init third_party/libstemmer_c" >&2
	exit 1
}

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

echo "cloning snowball @ $REF"
git clone --quiet "$UPSTREAM" "$WORK/snowball"
git -C "$WORK/snowball" checkout --quiet "$REF"
SHA=$(git -C "$WORK/snowball" rev-parse HEAD)
SUBJECT=$(git -C "$WORK/snowball" log -1 --format=%s)

echo "building dist_libstemmer_c"
make -C "$WORK/snowball" -j"$(nproc)" dist_libstemmer_c >"$WORK/build.log" 2>&1 || {
	tail -20 "$WORK/build.log" >&2
	exit 1
}

TARBALL=$(ls "$WORK/snowball/dist"/libstemmer_c-*.tar.gz)
VERSION=$(basename "$TARBALL" .tar.gz)
tar xzf "$TARBALL" -C "$WORK"

# Replace the tracked content wholesale: the distribution is regenerated, not
# patched, so stale files from a previous version must not survive.
find "$SUBMODULE" -mindepth 1 -maxdepth 1 ! -name '.git' ! -name 'README.md' -exec rm -rf {} +
cp -r "$WORK/$VERSION"/. "$SUBMODULE"/

# The wrapper lists its sources explicitly, so a language added upstream would
# otherwise be silently left out of the build.
python3 - "$REPO" <<'EOF'
import pathlib, re, sys

repo = pathlib.Path(sys.argv[1])
wrapper = repo / "third_party/libstemmer_c-cmake/CMakeLists.txt"
stems = sorted(p.name for p in (repo / "third_party/libstemmer_c/src_c").glob("stem_UTF_8_*.c"))
listing = "\n".join(f"    ${{LIBRARY_DIR}}/src_c/{n}" for n in stems)
text = wrapper.read_text()
updated, count = re.subn(
    r"(    # BEGIN stemmers[^\n]*\n).*?(    # END stemmers\n)",
    lambda m: m.group(1) + listing + "\n" + m.group(2),
    text,
    flags=re.S,
)
if count != 1:
    sys.exit("could not find the BEGIN/END stemmers markers in the wrapper")
wrapper.write_text(updated)
print(f"wrapper lists {len(stems)} UTF-8 stemmers")
EOF

cd "$SUBMODULE"
if git diff --quiet && git diff --cached --quiet; then
	echo "already up to date with snowball $SHA"
	exit 0
fi

git add -A
git commit -q -m "make dist_libstemmer_c @ snowball $SHA

Unmodified output of 'make dist_libstemmer_c' ($VERSION) built from
snowballstem/snowball $REF @ ${SHA:0:8} '$SUBJECT'."

echo
echo "updated third_party/libstemmer_c to $VERSION (snowball ${SHA:0:8})"
git --no-pager show --stat --oneline HEAD | head -5
echo
echo "next:"
echo "  git -C third_party/libstemmer_c push"
echo "  git add third_party/libstemmer_c third_party/libstemmer_c-cmake && git commit -m 'build: update libstemmer_c'"
