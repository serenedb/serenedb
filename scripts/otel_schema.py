#!/usr/bin/env python3
"""Render the canonical OpenTelemetry DDL into the forms other tools need.

`resources/otel/otel_schema.sql` is the single source of truth for the schema.
Nothing keeps its own copy: the server embeds it as a C++ header at build time,
and the sqllogic contract test includes a generated `.inc`. Both are derived
here so they cannot drift from the `.sql`.

    scripts/otel_schema.py generate      # rewrite the sqllogic include
    scripts/otel_schema.py check         # fail if that include has drifted
    scripts/otel_schema.py embed <out>   # write the C++ header
"""

import argparse
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
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
    )


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
        f"{SCHEMA.relative_to(ROOT)}; run scripts/otel_schema.py generate",
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
