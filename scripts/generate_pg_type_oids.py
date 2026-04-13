#!/usr/bin/env python3
"""Generate PostgreSQL type definitions from pg_type.dat.

Parses the PostgreSQL pg_type.dat Perl-data catalog (kept at the project root)
and emits C++ code for server/pg/pg_types.{h,cpp}:

  --enum        PgTypeOID enum body (paste into pg_types.h)
  --type2oid    Type2Oid(LogicalType, bool) function body
  --oid2type    Oid2Type(int32_t) function body
  --regtypeout  RegtypeOut(uint64_t) function body
  --regtypein   RegtypeIn(string_view) function body
  --all         all five sections (default)

All generated entries preserve pg_type.dat's native order, with each array
variant emitted immediately after its element type.

Not every pg_type has a DuckDB LogicalType mapping (and vice-versa). The
scripts encode known mappings at the top of this file; unmapped cases fall
through to the -1 / null default and can be handled manually.

Usage:
  python3 scripts/generate_pg_type_oids.py [--enum|--type2oid|--oid2type|--all] [pg_type.dat]
"""

import argparse
import os
import re
import sys


ENTRY_RE = re.compile(r"\{(.*?)\}", re.DOTALL)
KV_RE = re.compile(r"(\w+)\s*=>\s*'([^']*)'")


# ---------------------------------------------------------------------------
# Hardcoded DuckDB <-> PostgreSQL type mappings.
#
# pg_type.dat does not know about DuckDB, so these tables encode the known
# correspondences. Edit here when adding or changing a mapping.
# ---------------------------------------------------------------------------

# Type2Oid: maps pg typname -> list of duckdb::LogicalTypeId enumerators that
# should return this pg type's OID. Preserves emission order in the switch.
# Special buckets (VARCHAR, UBIGINT) dispatch by predicate -- see
# PG_TYPE_PREDICATE_BUCKETS below. LIST and ARRAY recurse.
PG_TYPE_TO_LTYPE_IDS = {
    "bool":        ["BOOLEAN"],
    "int2":        ["TINYINT", "UTINYINT", "SMALLINT"],
    "int4":        ["USMALLINT", "INTEGER"],
    "int8":        ["UINTEGER", "BIGINT"],
    "numeric":     ["HUGEINT", "UHUGEINT", "BIGNUM", "DECIMAL"],
    "date":        ["DATE"],
    "time":        ["TIME", "TIME_NS"],
    "timestamp":   ["TIMESTAMP_SEC", "TIMESTAMP_MS", "TIMESTAMP", "TIMESTAMP_NS"],
    "float4":      ["FLOAT"],
    "float8":      ["DOUBLE"],
    "text":        ["CHAR"],  # VARCHAR handled in PG_TYPE_PREDICATE_BUCKETS
    "bytea":       ["BLOB"],
    "interval":    ["INTERVAL"],
    "timestamptz": ["TIMESTAMP_TZ"],
    "timetz":      ["TIME_TZ"],
    "varbit":      ["BIT"],
    "uuid":        ["UUID"],
    "record":      ["STRUCT", "MAP"],
}


# Predicate-dispatched buckets for Type2Oid. These LogicalTypeIds cover
# multiple pg types distinguished by a C++ predicate on the LogicalType alias.
# Predicates are emitted in order; the `default` pg type is the fallback.
# A bucketed LogicalTypeId must NOT appear in PG_TYPE_TO_LTYPE_IDS.
PG_TYPE_PREDICATE_BUCKETS = {
    "VARCHAR": {
        "predicates": [
            ("json", "type.IsJSONType()"),
            ("name", "IsName(type)"),
        ],
        "default": "text",
    },
    # Postgres OID-family types (oid, xid, cid, tid, xid8 and the reg*
    # aliases) are all backed by UBIGINT in DuckDB and distinguished by
    # type-alias predicates.
    "UBIGINT": {
        "predicates": [
            ("oid",           "IsOid(type)"),
            ("xid",           "IsXid(type)"),
            ("cid",           "IsCid(type)"),
            ("tid",           "IsTid(type)"),
            ("xid8",          "IsXid8(type)"),
            ("regproc",       "IsRegproc(type)"),
            ("regprocedure",  "IsRegprocedure(type)"),
            ("regoper",       "IsRegoper(type)"),
            ("regoperator",   "IsRegoperator(type)"),
            ("regclass",      "IsRegclass(type)"),
            ("regtype",       "IsRegtype(type)"),
            ("regrole",       "IsRegrole(type)"),
            ("regnamespace",  "IsRegnamespace(type)"),
            ("regconfig",     "IsRegconfig(type)"),
            ("regdictionary", "IsRegdictionary(type)"),
            ("regcollation",  "IsRegcollation(type)"),
        ],
        "default": "numeric",
    },
}

# Oid2Type: maps pg typname -> C++ expression yielding the DuckDB LogicalType.
# Types absent from this table have no generated case and fall through to the
# default branch (SDB_ASSERT + {}).
PG_TYPE_TO_DUCK_EXPR = {
    "bool":          "LogicalType::BOOLEAN",
    "char":          "LogicalType::TINYINT",
    "int2":          "LogicalType::SMALLINT",
    "int4":          "LogicalType::INTEGER",
    "int8":          "LogicalType::BIGINT",
    "float4":        "LogicalType::FLOAT",
    "float8":        "LogicalType::DOUBLE",
    "text":          "LogicalType::VARCHAR",
    "name":          "NAME()",
    "bytea":         "LogicalType::BLOB",
    "date":          "LogicalType::DATE",
    "time":          "LogicalType::TIME",
    "timestamp":     "LogicalType::TIMESTAMP",
    "timestamptz":   "LogicalType::TIMESTAMP_TZ",
    "timetz":        "LogicalType::TIME_TZ",
    "interval":      "LogicalType::INTERVAL",
    "json":          "LogicalType::JSON()",
    "uuid":          "LogicalType::UUID",
    "bit":           "LogicalType::BIT",
    "varbit":        "LogicalType::BIT",
    "oid":           "OID()",
    "xid":           "XID()",
    "cid":           "CID()",
    "tid":           "TID()",
    "xid8":          "XID8()",
    "regproc":       "REGPROC()",
    "regtype":       "REGTYPE()",
    "regclass":      "REGCLASS()",
    "regnamespace":  "REGNAMESPACE()",
    "regoper":       "REGOPER()",
    "regoperator":   "REGOPERATOR()",
    "regprocedure":  "REGPROCEDURE()",
    "regrole":       "REGROLE()",
    "regconfig":     "REGCONFIG()",
    "regdictionary": "REGDICTIONARY()",
    "regcollation":  "REGCOLLATION()",
}


# ---------------------------------------------------------------------------
# pg_type.dat parser
# ---------------------------------------------------------------------------


def parse_entries(text):
    """Yield dicts of key->value for each { ... } entry, in file order."""
    # Strip comments (lines starting with '#', possibly after whitespace).
    lines = [ln for ln in text.splitlines() if not ln.lstrip().startswith("#")]
    cleaned = "\n".join(lines)

    for match in ENTRY_RE.finditer(cleaned):
        entry = dict(KV_RE.findall(match.group(1)))
        if "oid" in entry and "typname" in entry:
            yield entry


def to_camel(typname):
    """Convert a PostgreSQL type name to PascalCase for enum suffixes.

    Examples:
        bool        -> Bool
        int2        -> Int2
        int2vector  -> Int2Vector
        timestamptz -> TimestampTz
        timetz      -> TimeTz
        pg_type     -> PgType
    """
    name = typname.title().replace("_", "")
    # PostgreSQL uses 'tz' as shorthand for 'time zone'; capitalize as 'Tz'.
    if name.endswith("tz"):
        name = name[:-2] + "Tz"
    return name


def entry_for_array(entry):
    """Return (element_typname, array_oid) if this entry describes an array.

    Handles both autogenerated arrays (element type's array_type_oid) and
    manually-defined arrays (the `_foo` typname entries).
    """
    typname = entry["typname"]
    if typname.startswith("_"):
        return entry.get("typelem", typname[1:]), entry["oid"]
    if "array_type_oid" in entry:
        return typname, entry["array_type_oid"]
    return None


# ---------------------------------------------------------------------------
# Formatters
# ---------------------------------------------------------------------------


def format_enum(entries):
    out = []
    out.append("enum PgTypeOID : int32_t {")
    for e in entries:
        typname = e["typname"]
        if typname.startswith("_"):
            elem = e.get("typelem", typname[1:])
            out.append(f"  k{to_camel(elem)}Array = {e['oid']},")
            continue
        name = to_camel(typname)
        out.append(f"  k{name} = {e['oid']},")
        if "array_type_oid" in e:
            out.append(f"  k{name}Array = {e['array_type_oid']},")
    out.append("};")
    return "\n".join(out)


def _return_oid(pg_typname):
    name = to_camel(pg_typname)
    return f"in_array ? k{name}Array : k{name}"


def format_type2oid(entries):
    """Generate Type2Oid switch body. Validates the hardcoded mapping against
    the parsed pg_type entries so typos surface immediately."""
    known = {e["typname"] for e in entries if not e["typname"].startswith("_")}
    for pg_typname in PG_TYPE_TO_LTYPE_IDS:
        if pg_typname not in known:
            raise ValueError(
                f"PG_TYPE_TO_LTYPE_IDS references unknown pg_type '{pg_typname}'"
            )
    for lid, bucket in PG_TYPE_PREDICATE_BUCKETS.items():
        for pg_typname, _ in bucket["predicates"]:
            if pg_typname not in known:
                raise ValueError(
                    f"PG_TYPE_PREDICATE_BUCKETS[{lid}] references unknown "
                    f"pg_type '{pg_typname}'"
                )
        if bucket["default"] not in known:
            raise ValueError(
                f"PG_TYPE_PREDICATE_BUCKETS[{lid}].default is unknown pg_type "
                f"'{bucket['default']}'"
            )
        for ltype_ids in PG_TYPE_TO_LTYPE_IDS.values():
            if lid in ltype_ids:
                raise ValueError(
                    f"LogicalTypeId {lid} is both in PG_TYPE_TO_LTYPE_IDS and "
                    f"PG_TYPE_PREDICATE_BUCKETS"
                )

    out = []
    out.append("int32_t Type2Oid(const duckdb::LogicalType& type, bool in_array) {")
    out.append("  switch (type.id()) {")
    out.append("    using enum duckdb::LogicalTypeId;")
    out.append("    using enum PgTypeOID;")
    for pg_typname, ltype_ids in PG_TYPE_TO_LTYPE_IDS.items():
        for lid in ltype_ids[:-1]:
            out.append(f"    case {lid}:")
        out.append(f"    case {ltype_ids[-1]}:")
        out.append(f"      return {_return_oid(pg_typname)};")
    # Predicate-dispatched buckets (VARCHAR, UBIGINT, ...).
    for lid, bucket in PG_TYPE_PREDICATE_BUCKETS.items():
        out.append(f"    case {lid}: {{")
        for pg_typname, predicate in bucket["predicates"]:
            out.append(f"      if ({predicate}) {{")
            out.append(f"        return {_return_oid(pg_typname)};")
            out.append("      }")
        out.append(f"      return {_return_oid(bucket['default'])};")
        out.append("    }")
    # LIST / ARRAY: recurse into element type and force in_array.
    out.append("    case LIST:")
    out.append("      return Type2Oid(duckdb::ListType::GetChildType(type), true);")
    out.append("    case ARRAY:")
    out.append("      return Type2Oid(duckdb::ArrayType::GetChildType(type), true);")
    out.append("    default:")
    out.append("      return -1;")
    out.append("  }")
    out.append("}")
    return "\n".join(out)


def format_oid2type(entries):
    """Generate Oid2Type switch body.

    Emits one SDB_OID2TYPE(kFoo, expr) macro call per mapped type, which
    expands to both the scalar case and the LIST(expr) array case. Assumes
    the macro is defined in the .cpp file as:

      #define SDB_OID2TYPE(oid, type_expr)                       \\
        case koid: return (type_expr);                           \\
        case koid##Array: return duckdb::LogicalType::LIST(type_expr);
    """
    by_name = {e["typname"]: e for e in entries}
    for pg_typname in PG_TYPE_TO_DUCK_EXPR:
        if pg_typname not in by_name:
            raise ValueError(
                f"PG_TYPE_TO_DUCK_EXPR references unknown pg_type '{pg_typname}'"
            )

    has_array = _types_with_arrays(entries)

    out = []
    out.append("duckdb::LogicalType Oid2Type(int32_t oid) {")
    out.append("  switch (oid) {")
    out.append("    using enum PgTypeOID;")
    out.append("    using duckdb::LogicalType;")
    for e in entries:
        typname = e["typname"]
        if typname.startswith("_"):
            continue
        if typname not in PG_TYPE_TO_DUCK_EXPR:
            continue
        name = to_camel(typname)
        expr = PG_TYPE_TO_DUCK_EXPR[typname]
        if typname in has_array:
            out.append(f"    SDB_OID2TYPE(k{name}, {expr})")
        else:
            out.append(f"    case k{name}: return {expr};")
    out.append("    default: SDB_ASSERT(false); return {};")
    out.append("  }")
    out.append("}")
    return "\n".join(out)


def _types_with_arrays(entries):
    """Return set of typnames that have an array variant in pg_type.dat."""
    has_array = set()
    for e in entries:
        typname = e["typname"]
        if typname.startswith("_"):
            has_array.add(e.get("typelem", typname[1:]))
        elif "array_type_oid" in e:
            has_array.add(typname)
    return has_array


def format_regtype_out(entries):
    """Generate RegtypeOut body using pg_type.dat typnames verbatim.

    Uses SDB_REGTYPE_WITH_ARRAY_OUT for types that have an array OID, and
    SDB_REGTYPE_OUT for types that do not.
    """
    has_array = _types_with_arrays(entries)
    out = []
    out.append("std::string RegtypeOut(uint64_t oid) {")
    out.append("  switch (static_cast<PgTypeOID>(oid)) {")
    for e in entries:
        typname = e["typname"]
        if typname.startswith("_"):
            continue
        name = to_camel(typname)
        if typname in has_array:
            out.append(f'    SDB_REGTYPE_WITH_ARRAY_OUT(k{name}, "{typname}")')
        else:
            out.append(f'    SDB_REGTYPE_OUT(k{name}, "{typname}")')
    out.append("  }")
    out.append("  return absl::StrCat(oid);")
    out.append("}")
    return "\n".join(out)


def format_regtype_in(entries):
    """Generate RegtypeIn body using pg_type.dat typnames verbatim.

    Uses SDB_REGTYPE_WITH_ARRAY_IN for types that have an array OID, and
    SDB_REGTYPE_IN for types that do not. The map is a constexpr TrivialBiMap
    from string_view to PgTypeOID using .Case() chaining.
    """
    has_array = _types_with_arrays(entries)
    out = []
    out.append("uint64_t RegtypeIn(std::string_view name) {")
    out.append("  static constexpr containers::TrivialBiMap kTypeNameToOid =")
    out.append("    [](auto selector) {")
    out.append("      using enum PgTypeOID;")
    out.append("      return selector()")
    for e in entries:
        typname = e["typname"]
        if typname.startswith("_"):
            continue
        name = to_camel(typname)
        if typname in has_array:
            out.append(f'        .SDB_REGTYPE_WITH_ARRAY_IN("{typname}", k{name})')
        else:
            out.append(f'        .SDB_REGTYPE_IN("{typname}", k{name})')
    out.append("        ;")
    out.append("    };")
    out.append("  if (auto it = kTypeNameToOid.TryFind(name)) {")
    out.append("    return static_cast<uint64_t>(*it);")
    out.append("  }")
    out.append("  return kInvalidOid;")
    out.append("}")
    return "\n".join(out)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--enum", action="store_true", help="emit PgTypeOID enum body")
    group.add_argument("--type2oid", action="store_true", help="emit Type2Oid body")
    group.add_argument("--oid2type", action="store_true", help="emit Oid2Type body")
    group.add_argument("--regtypeout", action="store_true", help="emit RegtypeOut body")
    group.add_argument("--regtypein", action="store_true", help="emit RegtypeIn body")
    group.add_argument("--all", action="store_true", help="emit all five (default)")
    group.add_argument("--functions", action="store_true",
                       help="emit all except enum (type2oid, oid2type, regtypeout, regtypein)")
    parser.add_argument(
        "dat_path",
        nargs="?",
        default=None,
        help="path to pg_type.dat (defaults to <project_root>/pg_type.dat)",
    )
    args = parser.parse_args()

    if args.dat_path:
        path = args.dat_path
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(script_dir, "..", "pg_type.dat")

    with open(path) as f:
        entries = list(parse_entries(f.read()))

    any_flag = (args.enum or args.type2oid or args.oid2type
                or args.regtypeout or args.regtypein or args.functions)
    emit_all = args.all or not any_flag
    emit_functions = args.functions or emit_all

    sections = []
    if args.enum or emit_all:
        sections.append(("// PgTypeOID enum body", format_enum(entries)))
    if args.type2oid or emit_functions:
        sections.append(("// Type2Oid", format_type2oid(entries)))
    if args.oid2type or emit_functions:
        sections.append(("// Oid2Type", format_oid2type(entries)))
    if args.regtypeout or emit_functions:
        sections.append(("// RegtypeOut", format_regtype_out(entries)))
    if args.regtypein or emit_functions:
        sections.append(("// RegtypeIn", format_regtype_in(entries)))

    for i, (_, body) in enumerate(sections):
        if i:
            print()
        print(body)

    return 0


if __name__ == "__main__":
    sys.exit(main())
