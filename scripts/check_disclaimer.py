#!/usr/bin/env python3
import sys

DISCLAIMER = "/// DISCLAIMER"

EXCEPTIONS = {
    "libs/iresearch/include/iresearch/parser/lucene_parser.hpp",
    "libs/iresearch/include/iresearch/parser/lucene_parser.cpp",
    "libs/iresearch/include/iresearch/parser/lucene_lexer.cpp",
    "libs/iresearch/include/iresearch/utils/fstext/fst_draw.hpp",
    "libs/vpack/include/vpack/wyhash.h",
    "libs/vpack/include/vpack/events_from_slice.h",
    "libs/basics/logger/syslog_names.h",
    "server/pg/protocol.h",
    "server/pg/functions/interval.cpp",
    "tests/bench/micro/call_once.cpp",
    "tests/bench/micro/random.cpp",
    "tests/bench/micro/function.cpp",
    "libs/vpack/src/asm-utf8check.cpp",
    "libs/vpack/src/asm-utf8check.h",
    "libs/vpack/include/vpack/validation_types.h",
    "libs/vpack/include/vpack/validation.h",
    "libs/vpack/src/validation.cpp",
    "tests/libs/fuerte/main.cpp",
}

failed = []
for path in sys.argv[1:]:
    if path in EXCEPTIONS:
        continue
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            if DISCLAIMER not in f.read():
                failed.append(path)
    except OSError as e:
        print(f"error reading {path}: {e}", file=sys.stderr)
        failed.append(path)

for path in failed:
    print(f"{path}: missing disclaimer", file=sys.stderr)

sys.exit(1 if failed else 0)
