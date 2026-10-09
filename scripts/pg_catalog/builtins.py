from . import config
from .common import camel, cpp_bool, cpp_char, cpp_str

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
   OR p.oid IN (SELECT amhandler FROM pg_am)
   OR p.oid IN (SELECT unnest(ARRAY[lanplcallfoid, laninline, lanvalidator])
                FROM pg_language)
   OR p.oid IN (SELECT unnest(ARRAY[typinput, typoutput, typreceive,
                                    typsend]::oid[])
                FROM pg_type WHERE typname = ANY(%s))
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


def type_rows(gen):
    types = gen.query(TYPES_SQL, (list(config.SDB_OWNED_TYPES),))
    by_oid = {row[0]: row for row in types}
    constants, rows = {}, []
    for (oid, name, namespace, length, byval, typtype, category, preferred,
         delim, subscript, elem, array, inp, outp, recv, send, modin, modout,
         analyze, align, storage, collation, basetype, relid, typmod, notnull,
         default) in types:
        rows.append(
            f'{{{oid}, {cpp_str(name)}, {config.SCHEMAS[namespace]}, {length}, '
            f'{cpp_bool(byval)}, {cpp_char(typtype)}, {cpp_char(category)}, '
            f'{cpp_bool(preferred)}, {cpp_char(delim)}, {subscript}, {elem}, '
            f'{array}, {inp}, {outp}, {recv}, {send}, {modin}, {modout}, '
            f'{analyze}, {cpp_char(align)}, {cpp_char(storage)}, {collation}, '
            f'{basetype}, {relid}, {typmod}, {cpp_bool(notnull)}, '
            f'{cpp_str(default)}}},')
        element = by_oid.get(elem)
        is_array = element and element[11] == oid and name.startswith('_')
        row_type = element if is_array else by_oid[oid]
        if row_type[23] and row_type[0] >= config.FIRST_INITDB_OID:
            continue
        base = camel(element[1] if is_array else name)
        suffix = 'Rowtype' if row_type[23] else ''
        constants[oid] = f'k{base}{suffix}{"Array" if is_array else ""}'
    return types, constants, rows


def type_constants(gen):
    return type_rows(gen)[1]


def generate(gen):
    types, constants, rows = type_rows(gen)
    proc_oids = sorted({row[i] for row in types for i in PROC_COLUMNS if row[i]})
    procs = gen.query(PROCS_SQL, (proc_oids, list(config.SDB_OWNED_TYPES),
                                  list(config.ENUM_PROCS)))
    collations = gen.query(COLLATIONS_SQL, (list(config.COLLATIONS),))
    gen.write('builtin/type_oids.gen.inc',
              [f'{name} = {oid},' for oid, name in constants.items()])
    gen.write('builtin/types.gen.inc', rows)
    gen.write('builtin/procs.gen.inc', [
        f'{{{oid}, {cpp_str(name)}, {nargs}, {rettype}, '
        f'{{{", ".join(map(str, argtypes))}}}, {cpp_char(kind)}, '
        f'{cpp_bool(retset)}, {cpp_char(volatility)}, {cpp_bool(strict)}, '
        f'{lang}, {cpp_str(src)}}},'
        for oid, name, nargs, rettype, argtypes, kind, retset, volatility,
        strict, lang, src in procs])
    gen.write('builtin/collations.gen.inc', [
        f'{{{oid}, {cpp_str(name)}, {cpp_char(provider)}, {cpp_str(locale)}}},'
        for oid, name, provider, locale in collations])
