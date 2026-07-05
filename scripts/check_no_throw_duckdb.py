#!/usr/bin/env python3
"""Pre-commit hook: forbid `throw duckdb::...` in server/ -- use THROW_SQL_ERROR.

A SqlException carries the PG sqlstate (plus detail/hint) typed through
ErrorData::Throw() all the way to the wire, while a raw duckdb exception is
flattened through DuckExceptionToErrcode's lossy per-type mapping (e.g. every
InvalidInputException becomes 22023). Server code must throw THROW_SQL_ERROR
with the PG-correct ERRCODE_* instead.

The one sanctioned exception is duckdb::TransactionException: the commit /
rollback seam speaks to DuckDB's own transaction machinery and the wire layer
special-cases ExceptionType::TRANSACTION, so those throws must stay duckdb-typed.
"""
import os
import re
import sys

ROOT = "server"
EXTENSIONS = (".cpp", ".cc", ".c", ".hpp", ".hh", ".h", ".ipp", ".tpp")
THROW_RE = re.compile(r"throw duckdb::(?!TransactionException)")


def is_target(path: str) -> bool:
    norm = path.replace(os.sep, "/")
    return norm.startswith(ROOT + "/") and norm.endswith(EXTENSIONS)


def iter_targets(args: list[str]):
    if args:
        yield from (p for p in args if is_target(p))
        return
    for dirpath, _, names in os.walk(ROOT):
        for name in names:
            if name.endswith(EXTENSIONS):
                yield os.path.join(dirpath, name).replace(os.sep, "/")


def check(path: str) -> list[str]:
    violations = []
    with open(path, encoding="utf-8", errors="replace") as f:
        for lineno, line in enumerate(f, start=1):
            if THROW_RE.search(line):
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
            "\nRaw duckdb exception thrown in server code. Use "
            "THROW_SQL_ERROR(ERR_CODE(ERRCODE_...), ERR_MSG(...)) so the "
            "PG sqlstate survives to the client (pg/sql_exception_macro.h); "
            "only duckdb::TransactionException is allowed.",
            file=sys.stderr,
        )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
