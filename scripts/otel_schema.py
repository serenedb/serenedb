#!/usr/bin/env python3
"""Generate and check the sqllogic include that mirrors the canonical OTel DDL.

`resources/otel/otel_schema.sql` is the single source of truth for the
OpenTelemetry schema. Every ingestion route embeds it, so the sqllogic tests
must exercise exactly that text rather than a hand-copied variant.

    scripts/otel_schema.py generate      # rewrite the include from the .sql
    scripts/otel_schema.py check         # fail if the include has drifted
    scripts/otel_schema.py embed <out>   # emit the DDL as a C++ header
"""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
SCHEMA = ROOT / "resources" / "otel" / "otel_schema.sql"
INCLUDE = ROOT / "tests" / "sqllogic" / "sdb" / "pg" / "otel" / "schema.inc"


def statements(text):
    return [s.strip() for s in text.split(";\n") if s.strip()]


def render():
    lines = []
    for stmt in statements(SCHEMA.read_text()):
        lines.append("statement ok")
        lines.append(stmt.rstrip(";") + ";")
        lines.append("")
    return "\n".join(lines)


def embed(out_path):
    body = SCHEMA.read_text()
    statements = statements_of(body)
    lines = [
        "#pragma once",
        "",
        "#include <array>",
        "#include <string_view>",
        "",
        "namespace sdb::otel {",
        "",
        f"inline constexpr std::array<std::string_view, {len(statements)}>",
        "  kSchemaStatements{{",
    ]
    for stmt in statements:
        lines.append(f"  R\"OTELDDL({stmt})OTELDDL\",")
    lines += ["}};", "", "}  // namespace sdb::otel", ""]
    pathlib.Path(out_path).write_text("\n".join(lines))


def statements_of(text):
    return statements(text)


def main(argv):
    if len(argv) >= 2 and argv[1] == "embed":
        if len(argv) != 3:
            print(__doc__, file=sys.stderr)
            return 2
        embed(argv[2])
        return 0
    if len(argv) != 2 or argv[1] not in ("generate", "check"):
        print(__doc__, file=sys.stderr)
        return 2
    want = render()
    if argv[1] == "generate":
        INCLUDE.parent.mkdir(parents=True, exist_ok=True)
        INCLUDE.write_text(want)
        print(f"wrote {INCLUDE.relative_to(ROOT)}")
        return 0
    have = INCLUDE.read_text() if INCLUDE.exists() else ""
    if have != want:
        print(
            f"{INCLUDE.relative_to(ROOT)} is out of date with "
            f"{SCHEMA.relative_to(ROOT)}; run scripts/otel_schema.py generate",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
