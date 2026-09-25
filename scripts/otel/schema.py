#!/usr/bin/env python3
"""Render the canonical OpenTelemetry DDL into the forms other tools need.

`resources/otel/otel_schema.sql` is the single source of truth for the schema.
Nothing keeps its own copy: the server embeds it as a C++ header at build time,
and the sqllogic contract test includes a generated `.inc`. Both are derived
here so they cannot drift from the `.sql`.

    scripts/otel/schema.py generate      # rewrite the sqllogic include
    scripts/otel/schema.py check         # fail if that include has drifted
    scripts/otel/schema.py embed <out>   # write the C++ header: DDL + columns
"""

import argparse
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
SCHEMA = ROOT / "resources" / "otel" / "otel_schema.sql"
INCLUDE = ROOT / "tests" / "sqllogic" / "sdb" / "pg" / "otel" / "schema.inc"

# The DDL is one statement per `;` at end of line; no statement body contains
# one, so this needs no SQL parser.
STATEMENT_END = ";\n"


def statements():
    text = SCHEMA.read_text()
    return [s.strip().rstrip(";") for s in text.split(STATEMENT_END) if s.strip()]


def render_include():
    blocks = (f"statement ok\n{s};" for s in statements())
    return "\n\n".join(blocks) + "\n"


def render_header():
    body = "".join(f'  R"OTELDDL({s})OTELDDL",\n' for s in statements())
    return (
        "#pragma once\n"
        "\n"
        "#include <array>\n"
        "#include <cstddef>\n"
        "#include <cstdint>\n"
        "#include <string_view>\n"
        "\n"
        "namespace sdb::otel {\n"
        "\n"
        f"inline constexpr std::array<std::string_view, {len(statements())}>\n"
        "  kSchemaStatements{{\n"
        f"{body}"
        "}};\n"
        "\n"
        "}  // namespace sdb::otel\n"
        "\n"
        f"{render_columns()}"
    )


TABLE = re.compile(r"CREATE TABLE IF NOT EXISTS (\w+) \((.*)\) WITH", re.S)
COLUMN = re.compile(r'^\s*"?(\w+)"?\s+(.+?)(?:\s+NOT NULL)?,?\s*$')
TYPES = {
    "TIMESTAMP_NS": "TimestampNs",
    "VARCHAR": "Varchar",
    "JSON": "Varchar",
    "SMALLINT": "Smallint",
    "INTEGER": "Integer",
    "BIGINT": "Bigint",
    "DOUBLE PRECISION": "Double",
    "BOOLEAN": "Boolean",
}


def camel(name):
    return "".join(part.capitalize() for part in name.split("_"))


def column_type(sql):
    element = sql.removesuffix("[]")
    if element not in TYPES:
        sys.exit(f"{SCHEMA.relative_to(ROOT)}: no C++ mapping for type {sql}")
    if element != sql:
        return f"Type::List, Type::{TYPES[element]}"
    return f"Type::{TYPES[element]}"


def tables():
    for statement in statements():
        match = TABLE.match(statement)
        if match is None:
            continue
        columns = []
        for line in match.group(2).strip().splitlines():
            column = COLUMN.match(line)
            if column is None:
                sys.exit(f"{SCHEMA.relative_to(ROOT)}: cannot parse column {line!r}")
            columns.append((column.group(1), column_type(column.group(2))))
        yield match.group(1), columns


def render_columns():
    out = [
        "namespace sdb::otel::schema {",
        "",
        "enum class Type : uint8_t {",
        "  None,",
        *(f"  {t}," for t in dict.fromkeys(TYPES.values())),
        "  List,",
        "};",
        "",
        "struct Column {",
        "  std::string_view name;",
        "  Type type;",
        "  Type element = Type::None;",
        "};",
        "",
        "template<size_t N>",
        "struct Table {",
        "  std::string_view name;",
        "  std::array<Column, N> columns;",
        "};",
    ]
    for table, columns in tables():
        name = camel(table.removeprefix("otel_"))
        out += [
            "",
            f"enum class {name}Column : uint8_t {{",
            *(f"  {camel(column)}," for column, _ in columns),
            "};",
            "",
            f"inline constexpr Table<{len(columns)}> k{name}{{",
            f'  "{table}",',
            "  {{",
            *(f'    {{"{column}", {kind}}},' for column, kind in columns),
            "  }},",
            "};",
            "",
            f"constexpr const auto& TableOf({name}Column) {{ return k{name}; }}",
        ]
    out += ["", "}  // namespace sdb::otel::schema", ""]
    return "\n".join(out)


def generate():
    INCLUDE.parent.mkdir(parents=True, exist_ok=True)
    INCLUDE.write_text(render_include())
    print(f"wrote {INCLUDE.relative_to(ROOT)}")
    return 0


def check():
    current = INCLUDE.read_text() if INCLUDE.exists() else ""
    if current == render_include():
        return 0
    print(
        f"{INCLUDE.relative_to(ROOT)} is out of date with "
        f"{SCHEMA.relative_to(ROOT)}; run scripts/otel/schema.py generate",
        file=sys.stderr,
    )
    return 1


def embed(out):
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render_header())
    return 0


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("generate", help="rewrite the sqllogic include")
    commands.add_parser("check", help="fail if the include has drifted")
    embed_command = commands.add_parser("embed", help="write the C++ header")
    embed_command.add_argument("out", type=pathlib.Path)

    args = parser.parse_args()
    if args.command == "embed":
        return embed(args.out)
    return generate() if args.command == "generate" else check()


if __name__ == "__main__":
    sys.exit(main())
