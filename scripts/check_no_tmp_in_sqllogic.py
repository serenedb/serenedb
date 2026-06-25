#!/usr/bin/env python3
"""Pre-commit hook: forbid hardcoded /tmp paths in sqllogic tests.

Tests that write to a literal /tmp pollute a shared, non-isolated directory and
race across concurrent runs. Use ${__TEST_DIR__} instead -- the harness gives
each test file its own auto-cleaned temp dir (see sqllogictest-rs README).

A /tmp reference is allowed only if it appears in EXCEPTIONS below: doc-comment
examples and raw-doc display lines that illustrate behavior but never run.
"""
import os
import re
import sys

ROOT = "tests/sqllogic"
# /tmp as a path token: matches /tmp, /tmp/, /tmp', "/tmp" ... but not /tmpfoo.
TMP_RE = re.compile(r"/tmp(?=$|[/'\"\s,);])")

# relpath -> substrings that may legitimately contain /tmp on a single line.
EXCEPTIONS = {
    # Doc comments illustrating read_*/glob PK-shape behavior; never executed.
    "tests/sqllogic/sdb/pg/index/inverted_index_read_text.test": (
        "read_text('/tmp/x.md')",
        "read_text('/tmp/*.md')",
        "don't share /tmp",
    ),
    "tests/sqllogic/sdb/pg/index/inverted_index_view_glob.test": (
        "read_parquet('/tmp/*.parquet')",
        "read_csv('/tmp/*.csv')",
        "read_json('/tmp/*.jsonl')",
    ),
    # `#|` raw-doc display line: shows the published doc verbatim, not run.
    "tests/sqllogic/sdb/pg/site_docs/configuration/pragmas.test": (
        "SET log_query_path = '/tmp/serened_log/';",
    ),
}


def is_target(path: str) -> bool:
    norm = path.replace(os.sep, "/")
    return norm.startswith(ROOT + "/") and (".test" in os.path.basename(norm))


def iter_targets(args: list[str]):
    if args:
        yield from (p for p in args if is_target(p))
        return
    for dirpath, _, names in os.walk(ROOT):
        for name in names:
            if ".test" in name:
                yield os.path.join(dirpath, name).replace(os.sep, "/")


def check(path: str) -> list[str]:
    allowed = EXCEPTIONS.get(path.replace(os.sep, "/"), ())
    violations = []
    with open(path, encoding="utf-8", errors="replace") as f:
        for lineno, line in enumerate(f, start=1):
            if "/tmp" not in line or not TMP_RE.search(line):
                continue
            if any(needle in line for needle in allowed):
                continue
            violations.append(f"{path}:{lineno}: {line.rstrip()}")
    return violations


def main() -> int:
    failed = False
    for path in iter_targets(sys.argv[1:]):
        for v in check(path):
            print(v, file=sys.stderr)
            failed = True
    if failed:
        print(
            "\nHardcoded /tmp path in sqllogic test(s). Use ${__TEST_DIR__}/... "
            "for an isolated, auto-cleaned per-test directory instead.",
            file=sys.stderr,
        )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
