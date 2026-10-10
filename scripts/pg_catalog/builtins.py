import os
import re

from . import config
from .common import REPO_ROOT, camel, cpp_bool, cpp_char, cpp_str

TYPES_SQL = """
SELECT t.oid::int4, typname, n.nspname, typlen, typbyval, typtype,
       typcategory, typispreferred, typdelim, typsubscript::oid::int4,
       typelem::int4, typarray::int4, typinput::oid::int4, typoutput::oid::int4,
       typreceive::oid::int4, typsend::oid::int4, typmodin::oid::int4,
       typmodout::oid::int4, typanalyze::oid::int4, typalign, typstorage,
       typcollation::int4, typbasetype::int4, typrelid::int4, typtypmod,
       typnotnull, coalesce(typdefault, '')
FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
WHERE n.nspname IN ('pg_catalog', 'information_schema')
  AND typname <> ALL(%s)
ORDER BY t.oid
"""

PROCS_SQL = """
SELECT p.oid::int4, proname, pronargs, prorettype::int4,
       proargtypes::int4[], prokind, proretset, provolatile, proisstrict,
       prolang::int4, prosrc
FROM pg_proc p
WHERE p.oid = ANY(%s)
   OR p.oid IN (SELECT amhandler FROM pg_am WHERE amname = ANY(%s))
   OR p.oid IN (SELECT unnest(ARRAY[lanplcallfoid, laninline, lanvalidator])
                FROM pg_language WHERE lanname = ANY(%s))
   OR (p.pronamespace = 'pg_catalog'::regnamespace AND p.proname = ANY(%s))
ORDER BY p.oid
"""

COLLATIONS_SQL = """
SELECT oid::int4, collname, collprovider, coalesce(collcollate, '')
FROM pg_collation
WHERE collnamespace = 'pg_catalog'::regnamespace AND collname = ANY(%s)
ORDER BY oid
"""

PROC_COLUMNS = (9, 12, 13, 14, 15, 16, 17, 18)
REFERENCE_COLUMNS = (10, 11, 22)

TYPE_MAPPINGS = os.path.join(REPO_ROOT, 'server', 'pg', 'types.cpp')
MAPPINGS_TABLE = re.compile(r'kMappings\[\] = \{\n(.*?)\n\};', re.S)
MAPPED_TYPE = re.compile(r'^\s*\{k(\w+),', re.M)


def mapped_types():
    with open(TYPE_MAPPINGS) as source:
        table = MAPPINGS_TABLE.search(source.read()).group(1)
    return set(MAPPED_TYPE.findall(table))


def array_element(row, by_oid):
    element = by_oid.get(row[10])
    if element and element[11] == row[0] and row[1].startswith('_'):
        return element
    return None


def procedures(gen, types):
    oids = sorted({row[i] for row in types for i in PROC_COLUMNS if row[i]})
    return gen.query(PROCS_SQL, (oids, list(config.ACCESS_METHODS),
                                 list(config.LANGUAGES),
                                 list(config.ENUM_PROCS)))


def supported_types(gen):
    types = gen.query(TYPES_SQL, (list(config.SDB_OWNED_TYPES),))
    by_oid = {row[0]: row for row in types}
    mapped = mapped_types()
    keep = {row[0] for row in types
            if row[23] or row[1] in config.PSEUDO_TYPES or
            (camel(row[1]) in mapped and not array_element(row, by_oid))}
    while True:
        procs = procedures(gen, [by_oid[oid] for oid in keep])
        signatures = {oid for proc in procs for oid in (proc[3], *proc[4])}
        if missing := signatures - by_oid.keys():
            raise SystemExit(f'procedures refer to types SereneDB owns: '
                             f'{sorted(missing)}')
        wanted = (keep | signatures |
                  {by_oid[oid][11] for oid in keep if by_oid[oid][11]})
        if wanted == keep:
            break
        keep = wanted
    kept = [row for row in types if row[0] in keep]
    for row in kept:
        for column in REFERENCE_COLUMNS:
            if row[column] and row[column] not in keep:
                raise SystemExit(f'type {row[1]} refers to type '
                                 f'{by_oid[row[column]][1]}, which SereneDB '
                                 f'does not list')
    return kept, by_oid, procs


def type_constants(types, by_oid):
    constants = {}
    for row in types:
        element = array_element(row, by_oid)
        row_type = element or row
        if row_type[23] and row_type[0] >= config.FIRST_INITDB_OID:
            continue
        base = camel(row_type[1])
        suffix = 'Rowtype' if row_type[23] else ''
        constants[row[0]] = f'k{base}{suffix}{"Array" if element else ""}'
    return constants


def builtin_type_constants(gen):
    types, by_oid, _ = supported_types(gen)
    return type_constants(types, by_oid)


def generate(gen):
    types, by_oid, procs = supported_types(gen)
    constants = type_constants(types, by_oid)
    collations = gen.query(COLLATIONS_SQL, (list(config.COLLATIONS),))
    gen.write('builtin_type_oids.gen.inc',
              [f'{name} = {oid},' for oid, name in constants.items()])
    gen.write('builtin_types.gen.inc', [
        f'{{{oid}, {cpp_str(name)}, {config.SCHEMAS[namespace]}, {length}, '
        f'{cpp_bool(byval)}, {cpp_char(typtype)}, {cpp_char(category)}, '
        f'{cpp_bool(preferred)}, {cpp_char(delim)}, {subscript}, {elem}, '
        f'{array}, {inp}, {outp}, {recv}, {send}, {modin}, {modout}, '
        f'{analyze}, {cpp_char(align)}, {cpp_char(storage)}, {collation}, '
        f'{basetype}, {relid}, {typmod}, {cpp_bool(notnull)}, '
        f'{cpp_str(default)}}},'
        for (oid, name, namespace, length, byval, typtype, category,
             preferred, delim, subscript, elem, array, inp, outp, recv, send,
             modin, modout, analyze, align, storage, collation, basetype,
             relid, typmod, notnull, default) in types])
    gen.write('builtin_procs.gen.inc', [
        f'{{{oid}, {cpp_str(name)}, {nargs}, {rettype}, '
        f'{{{", ".join(map(str, argtypes))}}}, {cpp_char(kind)}, '
        f'{cpp_bool(retset)}, {cpp_char(volatility)}, {cpp_bool(strict)}, '
        f'{lang}, {cpp_str(src)}}},'
        for oid, name, nargs, rettype, argtypes, kind, retset, volatility,
        strict, lang, src in procs])
    gen.write('builtin_collations.gen.inc', [
        f'{{{oid}, {cpp_str(name)}, {cpp_char(provider)}, {cpp_str(locale)}}},'
        for oid, name, provider, locale in collations])
