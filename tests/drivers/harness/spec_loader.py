"""Shared spec loader for the Python harness.

Loads tests/drivers/spec/types.yaml and produces parametrize-ready test
cases for pytest. Language-specific harnesses in other languages reimplement
the same shape against their idiomatic test runner; this module is *only*
for Python (psycopg3, psycopg2, asyncpg).

Test-case identity is `<oid>:<name>:<protocol>:<sample_idx>` so JUnit
output stays grep-friendly.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

import yaml

SPEC_DIR = Path(os.environ.get("SDB_DRV_SPEC", Path(__file__).resolve().parent.parent / "spec"))
TYPES_YAML = SPEC_DIR / "types.yaml"

PROTOCOL_MODES = ("simple", "extended-noparam", "extended-text", "extended-binary")


@dataclass(frozen=True)
class TypeSpec:
    oid: int
    name: str
    pg_typname: str
    cast_sql: str
    text: bool
    binary: bool
    array: bool | str
    samples: list[Any]
    skip: dict[str, list[str]]

    def is_skipped(self, driver_key: str, protocol: str) -> str | None:
        modes = self.skip.get(driver_key, [])
        if "all" in modes or protocol in modes:
            return f"{driver_key} skips {protocol}"
        return None

    def supports(self, protocol: str) -> bool:
        if protocol == "extended-binary":
            return bool(self.binary)
        return bool(self.text)


def _coerce(entry: dict[str, Any]) -> TypeSpec:
    name = entry.get("name") or entry["pg_typname"]
    return TypeSpec(
        oid=int(entry["oid"]),
        name=name,
        pg_typname=entry.get("pg_typname", name),
        cast_sql=entry.get("cast_sql", f"$1::{entry.get('pg_typname', name)}"),
        text=bool(entry.get("text", False)),
        binary=bool(entry.get("binary", False)),
        array=entry.get("array", False),
        samples=list(entry.get("samples", []) or []),
        skip={k: list(v) for k, v in (entry.get("skip", {}) or {}).items()},
    )


def load_types() -> list[TypeSpec]:
    with TYPES_YAML.open() as f:
        raw = yaml.safe_load(f)
    if not isinstance(raw, list):
        raise RuntimeError(f"types.yaml: expected list at top level, got {type(raw)}")
    return [_coerce(e) for e in raw]


def _filter_active(types: list[TypeSpec], type_regex: str) -> list[TypeSpec]:
    pat = re.compile(type_regex)
    return [t for t in types if pat.search(t.name) and (t.text or t.binary)]


def cases_for_driver(driver_key: str) -> Iterator[tuple[TypeSpec, str, int, Any]]:
    """Yield (type_spec, protocol, sample_idx, sample) tuples that should run.

    Filters:
      * SDB_DRV_TYPES regex matches type name
      * SDB_DRV_PROTOCOLS comma list selects protocols
      * SDB_DRV_DRIVER, if set, narrows to one or more <lang>_<driver>
        keys; the caller passes its own driver_key, so this only filters
        when the user explicitly listed *other* drivers.
      * The type's `skip[driver_key]` list excludes a protocol.
      * The type's `text`/`binary` flag excludes formats it doesn't support.
    """
    types = _filter_active(load_types(), os.environ.get("SDB_DRV_TYPES", ".*"))
    requested_protocols = (
        os.environ.get("SDB_DRV_PROTOCOLS", ",".join(PROTOCOL_MODES)).split(",")
    )
    driver_filter = os.environ.get("SDB_DRV_DRIVER", "").strip()
    if driver_filter:
        accepted = set(driver_filter.split(","))
        if driver_key not in accepted:
            return

    for ts in types:
        for proto in requested_protocols:
            if proto not in PROTOCOL_MODES:
                continue
            if not ts.supports(proto):
                continue
            if ts.is_skipped(driver_key, proto):
                continue
            for idx, sample in enumerate(ts.samples):
                yield ts, proto, idx, sample


def conn_kwargs() -> dict[str, Any]:
    return {
        "host": os.environ.get("SDB_DRV_HOST", "localhost"),
        "port": int(os.environ.get("SDB_DRV_PORT", "5432")),
        # Default DB/user match the SereneDB defaults (postgres/postgres,
        # see libs/basics/static_strings.h kDefaultDatabase/kDefaultUser).
        "dbname": os.environ.get("SDB_DRV_DATABASE", "postgres"),
        "user": os.environ.get("SDB_DRV_USER", "postgres"),
    }


def schema_name(driver_key: str) -> str:
    run_id = os.environ.get("SDB_DRV_RUN_ID", "0")
    return f"drv_{driver_key}_{run_id}"
