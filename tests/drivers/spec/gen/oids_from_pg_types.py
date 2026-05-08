#!/usr/bin/env python3
"""Parse server/pg/pg_types.h `enum PgTypeOID` and reconcile against types.yaml.

Modes:
  --print          dump the parsed (name -> oid) map to stdout
  --check          fail (exit 1) if pg_types.h has OIDs not present in
                   types.yaml. Used as a CI/pre-commit gate so the matrix
                   does not silently rot when new OIDs are added.
  --merge-stub     append YAML stubs (skip:[all], reason: TODO) for any
                   missing OIDs to types.yaml. Manual triage still required.

The single source of truth for what a driver test must round-trip is
tests/drivers/spec/types.yaml. This script only enforces that every OID
exposed by the server is *acknowledged* there (either implemented or
explicitly skipped).
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
PG_TYPES_H = REPO_ROOT / "server" / "pg" / "pg_types.h"
TYPES_YAML = Path(__file__).resolve().parents[1] / "types.yaml"

# k<Name> = <oid>,
ENUM_LINE = re.compile(r"^\s*k([A-Za-z0-9]+)\s*=\s*(\d+)\s*,")


def parse_pg_types_h(path: Path) -> dict[str, int]:
    """Return {camel_name: oid} for every `kFoo = N,` line inside the enum."""
    out: dict[str, int] = {}
    in_enum = False
    for line in path.read_text().splitlines():
        if "enum PgTypeOID" in line:
            in_enum = True
            continue
        if not in_enum:
            continue
        if line.lstrip().startswith("}"):
            break
        m = ENUM_LINE.match(line)
        if m:
            out[m.group(1)] = int(m.group(2))
    if not out:
        raise RuntimeError(f"no OIDs parsed from {path}; enum format changed?")
    return out


def camel_to_snake(name: str) -> str:
    out = []
    for i, ch in enumerate(name):
        if ch.isupper() and i and not name[i - 1].isupper():
            out.append("_")
        out.append(ch.lower())
    return "".join(out)


def load_yaml_oids(path: Path) -> set[int]:
    """Pull oid: NN from types.yaml without depending on PyYAML at gate time."""
    if not path.exists():
        return set()
    out: set[int] = set()
    for line in path.read_text().splitlines():
        m = re.match(r"^\s*-?\s*oid:\s*(\d+)\s*$", line)
        if m:
            out.add(int(m.group(1)))
    return out


def stub_yaml(name: str, oid: int) -> str:
    snake = camel_to_snake(name)
    return (
        f"\n- oid: {oid}\n"
        f"  name: {snake}\n"
        f"  pg_typname: {snake}\n"
        f"  binary: false\n"
        f"  text: false\n"
        f"  array: false\n"
        f"  samples: []\n"
        f"  skip:\n"
        f"    all: [\"TODO: triage {name}\"]\n"
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--print", action="store_true")
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--merge-stub", action="store_true")
    args = ap.parse_args()

    enum_map = parse_pg_types_h(PG_TYPES_H)

    if args.print:
        for name, oid in sorted(enum_map.items(), key=lambda kv: kv[1]):
            print(f"{oid:>5}  {name}")
        return 0

    yaml_oids = load_yaml_oids(TYPES_YAML)
    enum_oids = set(enum_map.values())
    missing = enum_oids - yaml_oids
    extra = yaml_oids - enum_oids

    if args.merge_stub and missing:
        with TYPES_YAML.open("a") as f:
            f.write("\n# --- auto-stubbed by oids_from_pg_types.py ---\n")
            for name, oid in sorted(enum_map.items(), key=lambda kv: kv[1]):
                if oid in missing:
                    f.write(stub_yaml(name, oid))
        print(f"appended {len(missing)} stubs to {TYPES_YAML}", file=sys.stderr)
        return 0

    if args.check:
        if missing:
            print("OIDs in pg_types.h not present in types.yaml:", file=sys.stderr)
            for name, oid in sorted(enum_map.items(), key=lambda kv: kv[1]):
                if oid in missing:
                    print(f"  {oid:>5}  k{name}", file=sys.stderr)
            print(
                "Add an entry (or `skip: {all: [...]}`) in tests/drivers/spec/types.yaml,",
                file=sys.stderr,
            )
            print(
                "or run `python tests/drivers/spec/gen/oids_from_pg_types.py --merge-stub`.",
                file=sys.stderr,
            )
            return 1
        if extra:
            print(
                f"types.yaml has OIDs not in pg_types.h (stale?): {sorted(extra)}",
                file=sys.stderr,
            )
            return 1
        print(f"OK: {len(enum_oids)} OIDs reconciled.")
        return 0

    ap.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
