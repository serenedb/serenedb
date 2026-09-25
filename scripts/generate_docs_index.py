#!/usr/bin/env python3
"""Emit a translation unit that embeds a prebuilt iresearch directory.

The documentation index cannot be produced from the sources the way
generate_docs.py produces the documentation text: building it needs the
indexer, which lives in the server being built. So the directory is produced
by `serened-docs-bootstrap <datadir> --build_docs_index=<dir>`, and this script
only references the files it left behind.

The bytes come in through #embed, so nothing is transliterated: the generated
file is a few hundred bytes of directives rather than a multiple of the index
size, and there is no escaping to get wrong. irs::byte_type is uint8_t, so
#embed's integer list initialises the arrays with no narrowing and no cast,
and irs::SpanDirectory lends them out without copying.
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


def emit(files: list[tuple[pathlib.Path, bytes]]) -> str:
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
    for i, (path, data) in enumerate(files):
        digest = hashlib.sha256(data).hexdigest()
        lines.append(f"// {path.name} ({len(data)} bytes, sha256 {digest})")
        if not data:
            lines.append("")
            continue
        lines += [
            f"constexpr std::uint8_t kFile{i}[] = {{",
            f'#embed "{path}"',
            "};",
            "",
        ]

    lines += [
        "#pragma clang diagnostic pop",
        "",
        "constexpr IndexFile kIndex[] = {",
    ]
    for i, (path, data) in enumerate(files):
        value = f"kFile{i}" if data else "{}"
        lines.append(f'  {{"{path.name}", {value}}},')
    lines += ["};", "", "}  // namespace", ""]
    lines += [
        "std::span<const IndexFile> GetDocsIndex() { return kIndex; }",
        "",
        "}  // namespace sdb::docs",
        "",
    ]
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
                        help="iresearch directory to embed")
    parser.add_argument("output", type=pathlib.Path)
    args = parser.parse_args()

    files = (
        [
            (path.resolve(), path.read_bytes())
            for path in sorted(args.directory.iterdir())
            if path.is_file()
        ]
        if args.directory.is_dir()
        else []
    )
    if not files:
        print(f"no index files under {str(args.directory)!r}", file=sys.stderr)
        return 1

    total = sum(len(d) for _, d in files)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(emit(files))
    print(f"embedded docs index: {len(files)} files, {human_size(total)} "
          f"({total} bytes) -> {args.output}")
    width = max(len(path.name) for path, _ in files)
    for path, data in sorted(files, key=lambda file: len(file[1]), reverse=True):
        print(f"  {path.name:<{width}}  {human_size(len(data)):>10}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
