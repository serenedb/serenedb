import os
import re

from . import builtins, config
from .common import (CATALOG_DIR, GENERATED_DIR, SUPERUSER_ONLY_SQL, camel,
                     cpp_bool, cpp_char, cpp_str, generated)

TABLE_FOLDERS = {
    'pg_catalog': 'pg_catalog',
    'sdb': 'pg_catalog',
    'information_schema': 'information_schema',
}

RELATIONS_SQL = f"""
SELECT c.oid::int8, n.nspname, c.relname, c.relkind, c.relisshared,
       {SUPERUSER_ONLY_SQL}
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE (n.nspname IN ('pg_catalog', 'information_schema') AND c.relkind = 'r')
   OR (n.nspname, c.relname) IN (SELECT * FROM unnest(%s::text[], %s::text[]))
ORDER BY n.nspname, c.relname
"""

ELEMENT_JOIN = """
LEFT JOIN pg_type e ON e.oid = t.typelem
                   AND t.typsubscript = 'array_subscript_handler'::regproc
"""

COLUMNS_SQL = f"""
SELECT a.attname, a.atttypid::int8, t.typname, e.typname, e.typrelid <> 0,
       a.attnotnull
FROM pg_attribute a
JOIN pg_type t ON t.oid = a.atttypid
{ELEMENT_JOIN}
WHERE a.attrelid = %s AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum
"""

SDB_COLUMNS_SQL = f"""
SELECT c.name, t.oid::int8, t.typname, e.typname, e.typrelid <> 0, false
FROM unnest(%s::text[], %s::text[]::regtype[]) WITH ORDINALITY c(name, type, i)
JOIN pg_type t ON t.oid = c.type
{ELEMENT_JOIN}
ORDER BY c.i
"""

KEYS_SQL = """
SELECT a.attname, bool_or(i.indisunique AND i.indnkeyatts = 1)
FROM pg_index i
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
WHERE i.indrelid = %s
GROUP BY a.attname
"""

MAX_COLUMNS = 64
CATALOG_STRUCT = re.compile(r'^CATALOG\((\w+),.*?\n\{(.*?)\n\}', re.M | re.S)
CATALOG_COLUMN = re.compile(r'^\s*\w+\s+(\w+)(?:\[\d*\])?\s*(.*);', re.M)
BKI_DEFAULT = re.compile(r'BKI_DEFAULT\(((?:[^()]|\([^)]*\))*)\)')
NUMBER = re.compile(r'-?\d+(\.\d+)?')
ZERO = {'bool': 'false', 'char': '', 'name': '', 'text': '', 'bytea': '',
        'pg_node_tree': '', 'int2': '0', 'int4': '0', 'int8': '0',
        'float4': '0', 'float8': '0', 'oid': '0', 'xid': '0', 'regproc': '0',
        'regtype': '0'}


def bki_defaults(gen, name):
    path = gen.source(f'src/include/catalog/{name}.h')
    if not os.path.exists(path):
        return {}
    with open(path) as header:
        match = CATALOG_STRUCT.search(header.read())
    defaults = {}
    for column, annotations in CATALOG_COLUMN.findall(match.group(2)):
        if found := BKI_DEFAULT.search(annotations):
            defaults[column] = found.group(1)
    return defaults


def bki_literal(raw, typname):
    if typname == 'bool':
        return {'t': 'true', 'f': 'false'}.get(raw)
    if typname == 'char':
        value = raw.strip("'")
        return '' if value == '\\0' else value
    if raw == '-':
        return '0'
    return raw if NUMBER.fullmatch(raw) else None


def column_default(column, typname, element, notnull, bki, overrides):
    if column in overrides:
        return overrides[column]
    if element or typname not in ZERO:
        return None
    if column in bki:
        if (literal := bki_literal(bki[column], typname)) is not None:
            return literal
    return ZERO[typname] if notnull else None


def relation_constant(schema, name):
    return ('kInfo' if schema == 'information_schema' else 'k') + camel(name)


def physical(typname, composite):
    if composite:
        return 'STRUCT'
    if typname not in config.PHYSICAL_TYPES:
        raise SystemExit(f'no physical type for {typname}')
    return config.PHYSICAL_TYPES[typname]


def relations(gen):
    native = list(config.NATIVE_VIEWS)
    rels = gen.query(RELATIONS_SQL, ([s for s, _ in native],
                                     [n for _, n in native]))
    return rels + [(None, 'pg_catalog', name, 'r', False, False)
                   for name in config.SDB_TABLES]


def relation_columns(gen, oid, name):
    if oid is None:
        columns = config.SDB_TABLES[name][1]
        return gen.query(SDB_COLUMNS_SQL, ([c for c, _ in columns],
                                           [t for _, t in columns]))
    return gen.query(COLUMNS_SQL, (oid,))


def relation_keys(gen, oid, schema, name):
    keys = config.NATIVE_VIEWS.get((schema, name))
    if keys is None:
        keys = gen.query(KEYS_SQL, (oid,)) if oid is not None else []
    return {column: 'Unique' if unique else 'Indexed'
            for column, unique in keys}


def column_line(column, type_oid, typname, element, composite, notnull,
                default, key, types):
    if element:
        kind, child = 'LIST', physical(element, composite)
    else:
        kind, child = physical(typname, False), 'INVALID'
    value = 'std::nullopt' if default is None else cpp_str(default)
    return (f'  {{{cpp_str(column)}, {types.get(type_oid, type_oid)}, '
            f'duckdb::PhysicalType::{kind}, duckdb::PhysicalType::{child}, '
            f'{cpp_bool(notnull)}, SystemKey::{key}, {value}}},')


def table_lines(gen, oid, schema, name, relkind, shared, superuser, types):
    columns = relation_columns(gen, oid, name)
    if len(columns) > MAX_COLUMNS:
        raise SystemExit(f'{schema}.{name} has more than {MAX_COLUMNS} columns')
    keys = relation_keys(gen, oid, schema, name)
    bki = bki_defaults(gen, name) if oid and schema == 'pg_catalog' else {}
    overrides = config.COLUMN_DEFAULTS.get((schema, name), {})
    const = relation_constant(schema, name)
    rows = config.ESTIMATED_ROWS.get((schema, name),
                                     config.DEFAULT_ESTIMATED_ROWS)
    return ([f'inline constexpr SystemSqlColumn {const}Columns[] = {{'] +
            [column_line(column, type_oid, typname, element, composite,
                         notnull,
                         column_default(column, typname, element, notnull,
                                        bki, overrides),
                         keys.get(column, 'None'), types)
             for column, type_oid, typname, element, composite, notnull
             in columns] +
            ['};',
             f'inline constexpr SystemSql {const}Sql{{{const}Table, '
             f'{cpp_str(schema)}, {cpp_str(name)}, {cpp_char(relkind)}, '
             f'{cpp_bool(superuser)}, {cpp_bool(shared)}, {rows}, '
             f'{const}Columns}};'])


def data_rows(gen, schema, name, order):
    cursor = gen.conn.execute(f'SELECT * FROM {schema}.{name} ORDER BY {order}')
    columns = [column.name for column in cursor.description]
    overrides = config.DATA_CELLS.get((schema, name), {})
    const = relation_constant(schema, name)

    def cell(row, i):
        override = overrides.get((str(row[0]), columns[i]))
        if override is not None:
            return override
        return 'std::nullopt' if row[i] is None else cpp_str(str(row[i]))

    cells = [', '.join(cell(row, i) for i in range(len(row))) + ','
             for row in cursor.fetchall()]
    return ([f'constexpr SystemCell {const}Rows[] = {{']
            + ['  ' + line for line in cells] + ['};',
            f'SystemTable g{const[1:]}{{{const}Sql, {const}Rows}};'])


REGISTRY = os.path.join(GENERATED_DIR, 'registry.gen.inc')


def table_files():
    for folder, schema in TABLE_FOLDERS.items():
        for file in sorted(os.listdir(os.path.join(CATALOG_DIR, 'tables',
                                                   folder))):
            yield f'tables/{folder}/{file}', schema, file.removesuffix('.cpp')


def registry():
    coded = [relation_constant(schema, name)[1:]
             for _, schema, name in table_files()]
    data = [relation_constant(*table)[1:] for table in config.DATA_TABLES]
    return generated('the files in server/pg/catalog/tables', [
        f'extern SystemTable g{name};' for name in sorted(coded)] +
        ['SystemTable* const kSystemTables[] = {'] +
        [f'  &g{name},' for name in sorted(coded + data)] + ['};'])


def named_oids(gen, sql, names):
    return [f'inline constexpr duckdb::idx_t {const} = '
            f'{gen.query(sql, (name,))[0][0]};'
            for name, const in names.items()]


def generate(gen):
    reserved = {row[0] for row in gen.query(
        "SELECT word FROM pg_get_keywords() WHERE catcode <> 'U'")}
    types = builtins.type_constants(gen)
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
                       config.PROCEDURES))
    tables, all_tables, known = [], [], set()
    for oid, schema, name, relkind, shared, superuser in relations(gen):
        known.add((schema, name))
        const = relation_constant(schema, name)
        table_oid = (oid if oid is not None
                     else f'kMinSystem + {config.SDB_TABLES[name][0]}')
        oids.append(f'inline constexpr duckdb::idx_t {const}Table = '
                    f'{table_oid};')
        superuser = superuser or (schema, name) in config.SUPERUSER_ONLY
        tables += table_lines(gen, oid, schema, name, relkind, shared,
                              superuser, types)
        all_tables.append(f'&{const}Sql,')
    tables.append('inline constexpr const SystemSql* kGeneratedTables[] = {')
    tables += ['  ' + line for line in all_tables]
    tables.append('};')
    gen.write('catalog_oids.gen.inc', oids)
    gen.write('tables.gen.inc', tables)
    data = []
    for (schema, name), order in config.DATA_TABLES.items():
        data += data_rows(gen, schema, name, order)
    gen.write('information_schema_tables.gen.inc', data)
    for path, schema, name in table_files():
        if (schema, name) not in known:
            raise SystemExit(f'{path} names no catalog table')
    gen.write('keywords.gen.inc',
              [cpp_str(word) + ',' for word in sorted(reserved)])
    generate_settings(gen)


SETTINGS_SQL = """
SELECT name, setting, coalesce(unit, ''), category, short_desc,
       coalesce(extra_desc, ''), context,
       vartype, coalesce(min_val, ''), coalesce(max_val, ''), enumvals
FROM pg_settings WHERE name = ANY(%s)
"""


def generate_settings(gen):
    rows = {row[0]: row for row in gen.query(SETTINGS_SQL,
                                             (list(config.SETTINGS),))}
    missing = [name for name in config.SETTINGS if name not in rows]
    if missing:
        raise SystemExit(f'settings unknown to PostgreSQL: {missing}')
    out, gucs = [], []
    for i, name in enumerate(config.SETTINGS):
        (_, boot, unit, category, desc, extra, context, vartype, min_val,
         max_val, enumvals) = rows[name]
        enum = '{}'
        if enumvals:
            enum = f'kGucEnum{i}'
            out.append(f'inline constexpr std::string_view {enum}[] = {{' +
                       ', '.join(map(cpp_str, enumvals)) + '};')
        setting = config.SETTING_VALUES.get(name, boot or '')
        desc = config.SETTING_DESCRIPTIONS.get(name, desc)
        gucs.append('  {' + ', '.join(map(cpp_str, (
            name, setting, unit, category, desc, extra, context, vartype, min_val,
            max_val))) + f', {enum}}},')
    gen.write('settings.gen.inc',
              out + ['inline constexpr Guc kGucs[] = {'] + gucs + ['};'])
