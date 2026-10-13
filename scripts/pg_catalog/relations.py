import os

from . import config
from .common import (CATALOG_DIR, GENERATED_DIR, SUPERUSER_ONLY_SQL, camel,
                     cpp_bool, cpp_char, cpp_str, generated)

PG_FOLDERS = ('pg_catalog', 'information_schema')

SDB_FOLDER = 'sdb'

RELATIONS_SQL = f"""
SELECT c.oid::int8, n.nspname, c.relname, c.relkind, c.relisshared,
       {SUPERUSER_ONLY_SQL}, c.reltype::int4, coalesce(t.typarray, 0)::int4,
       c.relreplident
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_type t ON t.oid = c.reltype
WHERE n.nspname IN ('pg_catalog', 'information_schema')
  AND (c.relkind = 'r'
       OR (c.relkind = 'v' AND (n.nspname, c.relname) IN
           (SELECT * FROM unnest(%s::text[], %s::text[]))))
ORDER BY n.nspname, c.relname
"""

ELEMENT_JOIN = """
LEFT JOIN pg_type e ON e.oid = t.typelem
                   AND t.typsubscript = 'array_subscript_handler'::regproc
"""

COLUMNS_SQL = f"""
SELECT a.attname, a.atttypid::int8, coalesce(e.oid, 0)::int8, e.typrelid <> 0,
       a.attnotnull, a.attcollation::int8, a.attndims
FROM pg_attribute a
JOIN pg_type t ON t.oid = a.atttypid
{ELEMENT_JOIN}
WHERE a.attrelid = %s AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum
"""

KEYS_SQL = """
SELECT a.attname, bool_or(i.indisunique AND i.indnkeyatts = 1)
FROM pg_index i
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
WHERE i.indrelid = %s
GROUP BY a.attname
"""

FUNCTIONS_SQL = """
SELECT p.oid::int8, n.nspname, p.proname,
       NOT EXISTS (SELECT FROM aclexplode(coalesce(p.proacl,
                                                   acldefault('f', p.proowner)))
                   WHERE grantee = 0 AND privilege_type = 'EXECUTE')
FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE p.proretset
  AND (n.nspname, p.proname) IN (SELECT * FROM unnest(%s::text[], %s::text[]))
ORDER BY n.nspname, p.proname
"""

FUNCTION_COLUMNS_SQL = f"""
SELECT a.name, a.type::int8, coalesce(e.oid, 0)::int8, e.typrelid <> 0,
       t.typcollation::int8
FROM pg_proc p
CROSS JOIN LATERAL unnest(p.proargnames, p.proallargtypes, p.proargmodes)
  WITH ORDINALITY AS a(name, type, mode, ordinal)
JOIN pg_type t ON t.oid = a.type
{ELEMENT_JOIN}
WHERE p.oid = %s AND a.mode IN ('o', 't')
ORDER BY a.ordinal
"""

MAX_COLUMNS = 64


def relation_constant(schema, name):
    return ('kInfo' if schema == 'information_schema' else 'k') + camel(name)


def column_line(column, type_oid, element, composite, notnull, collation,
                ndims, key, types):
    return (f'  {{{cpp_str(column)}, {types.get(type_oid, type_oid)}, '
            f'{types.get(element, element)}, {cpp_bool(bool(composite))}, '
            f'{cpp_bool(notnull)}, {collation}, {ndims}, SystemKey::{key}}},')


def table_lines(gen, oid, schema, name, relkind, shared, superuser, row_type,
                row_array_type, replident, types):
    columns = gen.query(COLUMNS_SQL, (oid,))
    if len(columns) > MAX_COLUMNS:
        raise SystemExit(f'{schema}.{name} has more than {MAX_COLUMNS} columns')
    keys = {column: 'Unique' if unique else 'Indexed'
            for column, unique in gen.query(KEYS_SQL, (oid,))}
    const = relation_constant(schema, name)
    return ([f'inline constexpr SystemSqlColumn {const}Columns[] = {{'] +
            [column_line(column, type_oid, element, composite, notnull,
                         collation, ndims, keys.get(column, 'None'), types)
             for column, type_oid, element, composite, notnull, collation,
             ndims in columns] +
            ['};',
             f'inline constexpr SystemSql {const}Sql{{{const}Table, '
             f'{cpp_str(schema)}, {cpp_str(name)}, {row_type}, '
             f'{row_array_type}, {cpp_char(relkind)}, '
             f'{cpp_char(replident)}, {cpp_bool(superuser)}, '
             f'{cpp_bool(shared)}, {const}Columns}};'])


def function_lines(gen, oid, schema, name, superuser, types):
    const = relation_constant(schema, name)
    return ([f'inline constexpr SystemSqlColumn {const}Columns[] = {{'] +
            [column_line(column, type_oid, element, composite, False,
                         collation, 0, 'None', types)
             for column, type_oid, element, composite, collation
             in gen.query(FUNCTION_COLUMNS_SQL, (oid,))] +
            ['};',
             f'inline constexpr SystemSql {const}Sql{{{const}Function, '
             f"{cpp_str(schema)}, {cpp_str(name)}, 0, 0, '\\0', '\\0', "
             f'{cpp_bool(superuser)}, false, {const}Columns}};'])


REGISTRY = os.path.join(GENERATED_DIR, 'registry.gen.inc')


def files(kind, folder):
    path = os.path.join(CATALOG_DIR, kind, folder)
    if not os.path.isdir(path):
        return []
    return [file.removesuffix('.cpp') for file in sorted(os.listdir(path))]


def table_files():
    return [(schema, name) for schema in PG_FOLDERS
            for name in files('tables', schema)]


def function_files():
    return [(schema, name) for schema in PG_FOLDERS
            for name in files('table_functions', schema)]


def view_files():
    return [(schema, name) for schema in PG_FOLDERS
            for name in files('views', schema)]


def registry():
    coded = sorted([relation_constant(schema, name)[1:]
                    for schema, name in table_files()] +
                   [relation_constant('pg_catalog', name)[1:]
                    for name in files('tables', SDB_FOLDER)])
    views = sorted((relation_constant(schema, name)[1:], schema, name)
                   for schema, name in view_files())
    sdb_views = sorted(relation_constant('pg_catalog', name)[1:]
                       for name in files('views', SDB_FOLDER))
    functions = sorted(relation_constant(schema, name)[1:]
                       for schema, name in function_files())
    return generated(
      'the files in server/pg/catalog/tables, table_functions and views', [
        f'extern SystemTable g{name};' for name in coded + functions] +
        ['SystemTable* const kSystemTables[] = {'] +
        [f'  &g{name},' for name in coded] + ['};'] +
        ['SystemTable* const kSystemTableFunctions[] = {'] +
        [f'  &g{name},' for name in functions] + ['};'] +
        [f'extern std::string_view g{const};' for const, _, _ in views] +
        ['const SystemViewSql kSystemViewSql[] = {'] +
        [f'  {{{cpp_str(schema)}, {cpp_str(name)}, g{const}}},'
         for const, schema, name in views] + ['};'] +
        [f'extern SystemView g{const};' for const in sdb_views] +
        ['SystemView* const kSdbViews[] = {'] +
        [f'  &g{const},' for const in sdb_views] + ['};'])


def named_oids(gen, sql, names):
    return [f'inline constexpr duckdb::idx_t {const} = '
            f'{gen.query(sql, (name,))[0][0]};'
            for name, const in names.items()]


def generate(gen, types):
    reserved = {row[0] for row in gen.query(
        "SELECT word FROM pg_get_keywords() WHERE catcode <> 'U'")}
    oids = (named_oids(gen, 'SELECT %s::regnamespace::oid::int8',
                       config.SCHEMAS) +
            named_oids(gen, 'SELECT oid::int8 FROM pg_database '
                            'WHERE datname = %s', config.DATABASES) +
            named_oids(gen, 'SELECT oid::int8 FROM pg_tablespace '
                            'WHERE spcname = %s', config.TABLESPACES) +
            named_oids(gen, 'SELECT oid::int8 FROM pg_am WHERE amname = %s',
                       config.ACCESS_METHODS) +
            named_oids(gen, 'SELECT amhandler::int8 FROM pg_am '
                            'WHERE amname = %s',
                       {name: const + 'Handler'
                        for name, const in config.ACCESS_METHODS.items()}) +
            named_oids(gen, 'SELECT oid::int8 FROM pg_language '
                            'WHERE lanname = %s', config.LANGUAGES) +
            named_oids(gen, 'SELECT lanvalidator::int8 FROM pg_language '
                            'WHERE lanname = %s',
                       {name: const + 'Validator'
                        for name, const in config.LANGUAGES.items()}) +
            named_oids(gen, 'SELECT %s::regproc::oid::int8',
                       config.PROCEDURES) +
            named_oids(gen, 'SELECT oid::int8 FROM pg_collation WHERE '
                            "collname = %s AND collnamespace = "
                            "'pg_catalog'::regnamespace",
                       config.COLLATIONS))
    coded = table_files()
    tables, all_tables, known = [], [], set()
    for (oid, schema, name, relkind, shared, superuser, row_type,
         row_array_type, replident) in gen.query(RELATIONS_SQL,
                                                 ([s for s, _ in coded],
                                                  [n for _, n in coded])):
        known.add((schema, name))
        const = relation_constant(schema, name)
        oids.append(f'inline constexpr duckdb::idx_t {const}Table = {oid};')
        tables += table_lines(gen, oid, schema, name, relkind, shared,
                              superuser, row_type, row_array_type, replident,
                              types)
        all_tables.append(f'&{const}Sql,')
    functions = function_files()
    found = []
    for oid, schema, name, superuser in gen.query(
            FUNCTIONS_SQL, ([s for s, _ in functions], [n for _, n in functions])):
        found.append((schema, name))
        const = relation_constant(schema, name)
        oids.append(f'inline constexpr duckdb::idx_t {const}Function = {oid};')
        tables += function_lines(gen, oid, schema, name, superuser, types)
    for schema, name in functions:
        if found.count((schema, name)) != 1:
            raise SystemExit(f'table_functions/{schema}/{name}.cpp names no '
                             f'single set-returning function')
    tables.append('inline constexpr const SystemSql* kGeneratedTables[] = {')
    tables += ['  ' + line for line in all_tables]
    tables.append('};')
    gen.write('catalog_oids.gen.inc', oids)
    gen.write('tables.gen.inc', tables)
    for schema, name in coded:
        if (schema, name) not in known:
            raise SystemExit(f'tables/{schema}/{name}.cpp names no catalog '
                             f'table')
    gen.write('keywords.gen.inc',
              [cpp_str(word) + ',' for word in sorted(reserved)])
