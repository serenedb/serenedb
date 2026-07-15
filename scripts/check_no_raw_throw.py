#!/usr/bin/env python3
"""Pre-commit hook: forbid raw `throw` in server/ -- use the throw macros.

A raw exception loses its PG sqlstate on the way to the client: only a typed
sdb::SqlException (built by THROW_SQL_ERROR / SDB_THROW) carries the
errcode/detail/hint through ErrorData::Throw() to the wire, while anything
else is flattened through a lossy per-type mapping. Server code must not
`throw` directly.

Sanctioned exceptions:
  - `throw;`                            -- a bare rethrow keeps the original
  - `throw sdb::SqlException{...}`      -- the typed wire exception itself
  - `throw duckdb::TransactionException`-- the commit/rollback seam speaks to
    DuckDB's own transaction machinery and the wire layer special-cases
    ExceptionType::TRANSACTION
  - allowlisted paths below, each a subsystem whose exceptions never reach
    the pg wire.
"""
import os
import re
import sys

ROOT = "server"
EXTENSIONS = (".cpp", ".cc", ".c", ".hpp", ".hh", ".h", ".ipp", ".tpp")

# Paths (prefix match) whose raw throws are sanctioned.
ALLOWED_PATHS = (
    # asio coroutine resume seam: rethrows the operation's error_code as
    # asio's own system_error inside the awaitable machinery.
    "server/network/asio_awaitable.h",
    # The ES HTTP emulation's typed DslError is thrown and caught within the
    # subsystem and rendered as an ES-style HTTP error body, never a pg error.
    "server/network/http/es/",
)

STRING_RE = re.compile(r'"(?:[^"\\]|\\.)*"|\'(?:[^\'\\]|\\.)*\'')
COMMENT_RE = re.compile(r"//.*$|/\*.*?\*/")
THROW_RE = re.compile(r"\bthrow\b\s*(?P<rest>[^\s;]*)")
ALLOWED_EXPRS = ("sdb::SqlException", "duckdb::TransactionException")


def is_target(path: str) -> bool:
    norm = path.replace(os.sep, "/")
    if not (norm.startswith(ROOT + "/") and norm.endswith(EXTENSIONS)):
        return False
    return not norm.startswith(ALLOWED_PATHS)


def iter_targets(args: list[str]):
    if args:
        yield from (p for p in args if is_target(p))
        return
    for dirpath, _, names in os.walk(ROOT):
        for name in names:
            path = os.path.join(dirpath, name).replace(os.sep, "/")
            if is_target(path):
                yield path


def check(path: str) -> list[str]:
    violations = []
    with open(path, encoding="utf-8", errors="replace") as f:
        for lineno, line in enumerate(f, start=1):
            code = COMMENT_RE.sub("", STRING_RE.sub('""', line))
            for m in THROW_RE.finditer(code):
                rest = m.group("rest")
                # `throw;` (bare rethrow): the match consumed no expression.
                if not rest:
                    continue
                if any(rest.startswith(expr) for expr in ALLOWED_EXPRS):
                    continue
                violations.append(f"{path}:{lineno}: {line.rstrip()}")
                break
    return violations


def main() -> int:
    failed = False
    for path in iter_targets(sys.argv[1:]):
        for v in check(path):
            print(v, file=sys.stderr)
            failed = True
    if failed:
        print(
            "\nRaw `throw` in server code. Use "
            "THROW_SQL_ERROR(ERR_CODE(ERRCODE_...), ERR_MSG(...)) (or "
            "SDB_THROW) so the PG sqlstate survives to the client "
            "(pg/sql_exception_macro.h). Sanctioned: bare `throw;`, "
            "`throw sdb::SqlException{...}`, "
            "`throw duckdb::TransactionException`, and the allowlisted "
            "subsystems in scripts/check_no_raw_throw.py.",
            file=sys.stderr,
        )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
