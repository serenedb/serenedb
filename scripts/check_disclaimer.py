#!/usr/bin/env python3
import sys

DISCLAIMER = "/// DISCLAIMER"
BORDER = "/" * 80
HEADER_EXTS = {".h", ".hpp", ".hh", ".ipp", ".tpp"}

EXCEPTIONS = {
    "libs/iresearch/include/iresearch/parser/lucene_parser.hpp",
    "libs/iresearch/include/iresearch/parser/lucene_lexer.cpp",
    "libs/iresearch/include/iresearch/parser/lucene_parser.cpp",
    "libs/iresearch/include/iresearch/utils/fstext/fst_draw.hpp",
    "libs/vpack/include/vpack/wyhash.h",
    "libs/vpack/include/vpack/events_from_slice.h",
    "libs/vpack/include/vpack/validation_types.h",
    "libs/vpack/include/vpack/validation.h",
    "libs/vpack/src/asm-utf8check.cpp",
    "libs/vpack/src/asm-utf8check.h",
    "libs/vpack/src/validation.cpp",
    "libs/basics/logger/syslog_names.h",
    "server/pg/protocol.h",
    "server/pg/functions/interval.cpp",
    "tests/bench/micro/call_once.cpp",
    "tests/bench/micro/random.cpp",
    "tests/bench/micro/function.cpp",
    "tests/libs/fuerte/main.cpp",
}

PRAGMA_ONCE_EXCEPTIONS = {
    "libs/basics/logger/logger.h",       # secondary BSD license block between disclaimer and pragma once
    "libs/basics/lru_cache.h",           # secondary BSD license block between disclaimer and pragma once
    "libs/build_id/include/build_id/build_id.h",  # secondary comment block between disclaimer and pragma once
}


def check(path: str) -> list[str]:
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            text = f.read()
    except OSError as e:
        return [f"error reading: {e}"]

    errors = []

    if DISCLAIMER not in text:
        errors.append("missing disclaimer")

    ext = "." + path.rsplit(".", 1)[-1] if "." in path else ""
    if ext in HEADER_EXTS and path not in PRAGMA_ONCE_EXCEPTIONS:
        lines = text.splitlines()
        # Find DISCLAIMER line, then scan forward for the closing border
        disclaimer_idx = next((i for i, l in enumerate(lines) if DISCLAIMER in l), None)
        if disclaimer_idx is None:
            pass  # already reported above
        else:
            closing_idx = next(
                (i for i in range(disclaimer_idx, min(disclaimer_idx + 30, len(lines))) if lines[i] == BORDER),
                None,
            )
            if closing_idx is None:
                errors.append("missing closing disclaimer border")
            else:
                # Expect: border, blank line, #pragma once
                after = lines[closing_idx + 1 : closing_idx + 4]
                if after[:2] != ["", "#pragma once"]:
                    errors.append("#pragma once must appear on the line after the disclaimer (with one blank line)")
                elif len(after) < 3 or after[2] != "":
                    errors.append("#pragma once must be followed by an empty line")

    return errors


failed = []
for path in sys.argv[1:]:
    if path in EXCEPTIONS:
        continue
    errors = check(path)
    for e in errors:
        print(f"{path}: {e}", file=sys.stderr)
    if errors:
        failed.append(path)

sys.exit(1 if failed else 0)
