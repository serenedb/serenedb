#!/usr/bin/env python3
"""Emit a translation unit that embeds the prebuilt iresearch directories.

The documentation index cannot be produced from the sources the way
generate_docs.py produces the documentation text: building it needs the
indexer, which lives in the server being built. So `serened-docs-bootstrap
<datadir> --build_docs_index=<dir>` writes the page index to <dir>/docs and
the object catalog to <dir>/objects, and this script only references the
files it left behind.

The bytes come in through #embed, so nothing is transliterated: the generated
file is a few hundred bytes of directives rather than a multiple of the index
size, and there is no escaping to get wrong. irs::byte_type is uint8_t, so
#embed's integer list initialises the arrays with no narrowing and no cast.
"""

from __future__ import annotations

import argparse
import hashlib
import pathlib
import sys

def human_size(size: int) -> str:
    if size < 1024:
        return f"{size} B"
    if size < 1024 * 1024:
        return f"{size / 1024:.1f} KiB"
    return f"{size / (1024 * 1024):.1f} MiB"


INDEXES = (
    ("docs", "kDocs", "GetDocsIndex"),
    ("objects", "kObjects", "GetObjectsIndex"),
)

Index = tuple[str, str, list[tuple[pathlib.Path, bytes]]]


def emit(indexes: list[Index]) -> str:
    lines = [
        '#include "docs/docs_index_data.h"',
        "",
        "#include <cstdint>",
        "",
        "namespace sdb::docs {",
        "",
        "namespace {",
        "",
        "#pragma clang diagnostic push",
        '#pragma clang diagnostic ignored "-Wc23-extensions"',
        "",
    ]
    number = 0
    tables = []
    for array, _, files in indexes:
        entries = []
        for path, data in files:
            digest = hashlib.sha256(data).hexdigest()
            lines.append(f"// {path.parent.name}/{path.name} "
                         f"({len(data)} bytes, sha256 {digest})")
            if not data:
                lines.append("")
                entries.append(f'  {{"{path.name}", {{}}}},')
                continue
            lines += [
                f"constexpr std::uint8_t kFile{number}[] = {{",
                f'#embed "{path}"',
                "};",
                "",
            ]
            entries.append(f'  {{"{path.name}", kFile{number}}},')
            number += 1
        tables.append((array, entries))

    lines += ["#pragma clang diagnostic pop", ""]
    for array, entries in tables:
        lines += [f"constexpr IndexFile {array}[] = {{", *entries, "};", ""]
    lines += ["}  // namespace", ""]
    for array, accessor, _ in indexes:
        lines += [
            f"std::span<const IndexFile> {accessor}() {{ return {array}; }}",
            "",
        ]
    lines += ["}  // namespace sdb::docs", ""]
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Producing the directory:\n"
            "  serened-docs-bootstrap /tmp/docsgen --build_docs_index=/tmp/docsindex\n"
            "\n"
            "The build does this for you unless configured with\n"
            "-DSDB_EMBEDDED_DOCS=OFF.\n"
        ),
    )
    parser.add_argument("directory", type=pathlib.Path,
                        help="directory holding the docs/ and objects/ "
                             "iresearch directories to embed")
    parser.add_argument("output", type=pathlib.Path)
    args = parser.parse_args()

    indexes: list[Index] = []
    for name, array, accessor in INDEXES:
        directory = args.directory / name
        files = (
            [
                (path.resolve(), path.read_bytes())
                for path in sorted(directory.iterdir())
                if path.is_file()
            ]
            if directory.is_dir()
            else []
        )
        if not files:
            print(f"no index files under {str(directory)!r}", file=sys.stderr)
            return 1
        indexes.append((array, accessor, files))

    listed = [
        (f"{path.parent.name}/{path.name}", data)
        for _, _, files in indexes
        for path, data in files
    ]
    total = sum(len(data) for _, data in listed)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(emit(indexes))
    print(f"embedded docs index: {len(listed)} files, {human_size(total)} "
          f"({total} bytes) -> {args.output}")
    width = max(len(name) for name, _ in listed)
    for name, data in sorted(listed, key=lambda item: len(item[1]),
                             reverse=True):
        print(f"  {name:<{width}}  {human_size(len(data)):>10}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
