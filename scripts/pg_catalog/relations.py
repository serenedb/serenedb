import os

from . import config, source
from .common import (CATALOG_DIR, GENERATED_DIR, array, camel, cpp_bool,
                     cpp_char, cpp_str, generated, oid)

PG_FOLDERS = ('pg_catalog', 'information_schema')

SDB_FOLDER = 'sdb'

PG_CATALOG = '11'

MAX_COLUMNS = 64


def relation_constant(schema, name):
    return ('kInfo' if schema == 'information_schema' else 'k') + camel(name)


def column_line(column, type_oid, element, composite, notnull, collation,
                ndims, key, types):
    return (f'  {{{cpp_str(column)}, {types.get(type_oid, type_oid)}, '
            f'{types.get(element, element)}, {cpp_bool(bool(composite))}, '
            f'{cpp_bool(notnull)}, {collation}, {ndims}, SystemKey::{key}}},')


class Types:
    def __init__(self, gen, rows):
        self.by_oid = {row[0]: row for row in rows}
        self.array_handler = int(gen.one('pg_proc',
                                         proname='array_subscript_handler')
                                 ['oid'])
        self.c_collation = int(gen.one('pg_collation', collname='C')['oid'])
        for name, (row_type, array_type) in gen.row_types.items():
            self.by_oid.setdefault(row_type, (
                row_type, name, 'pg_catalog', -1, False, 'c', 'C', False,
                ',', 0, 0, array_type) + (0,) * 7 + ('d', 'x', 0, 0,
                                                     gen.relations[name].oid,
                                                     -1, False, ''))
            self.by_oid.setdefault(array_type, (
                array_type, '_' + name, 'pg_catalog', -1, False, 'b', 'A',
                False, ',', self.array_handler, row_type, 0) + (0,) * 7 +
                ('d', 'x', 0, 0, 0, -1, False, ''))
        self.by_name = {row[1]: row for row in self.by_oid.values()}

    def element(self, type_oid):
        row = self.by_oid[type_oid]
        if row[9] != self.array_handler:
            return 0, False
        return row[10], self.by_oid[row[10]][23] != 0


def catalog_columns(gen, relation, types):
    columns, fixed_not_null = [], True
    for name, type_name, force in relation.columns:
        row = types.by_name[type_name]
        fixed_not_null = fixed_not_null and row[3] > 0
        notnull = (force == 'FORCE NOT NULL' or
                   (not force and fixed_not_null))
        fixed_not_null = fixed_not_null and notnull
        element, composite = types.element(row[0])
        columns.append((name, row[0], element, composite, notnull,
                        types.c_collation if row[21] else 0,
                        1 if element else 0))
    return columns


def domain_columns(columns, types):
    out = []
    for name, type_name in columns:
        row = types.by_name[type_name]
        out.append((name, row[0], 0, False, False, row[21], 0))
    return out


def keys(gen, table):
    out = {}
    for index in gen.indexes:
        if index.table != table:
            continue
        for column in index.columns:
            unique = index.unique and len(index.columns) == 1
            out[column] = ('Unique' if unique or out.get(column) == 'Unique'
                           else 'Indexed')
    return out


def table_lines(schema, name, relkind, shared, superuser, row_type,
                row_array_type, replident, columns, keys, public, types):
    if len(columns) > MAX_COLUMNS:
        raise SystemExit(f'{schema}.{name} has more than {MAX_COLUMNS} columns')
    const = relation_constant(schema, name)
    public_columns = sum(1 << i for i, column in enumerate(columns)
                         if column[0] in public)
    return ([f'inline constexpr SystemSqlColumn {const}Columns[] = {{'] +
            [column_line(*column, keys.get(column[0], 'None'), types)
             for column in columns] +
            ['};',
             f'inline constexpr SystemSql {const}Sql{{{const}Table, '
             f'{cpp_str(schema)}, {cpp_str(name)}, {row_type}, '
             f'{row_array_type}, {cpp_char(relkind)}, '
             f'{cpp_char(replident)}, {cpp_bool(superuser)}, '
             f'{public_columns:#x}, {cpp_bool(shared)}, {const}Columns}};'])


def function_lines(proc, schema, name, superuser, types, constants):
    names = array(proc['proargnames'])
    modes = array(proc['proargmodes'])
    columns = []
    for argname, argtype, mode in zip(names, array(proc['proallargtypes']),
                                      modes):
        if mode not in ('o', 't'):
            continue
        element, composite = types.element(int(argtype))
        columns.append((argname, int(argtype), element, composite, False,
                        types.by_oid[int(argtype)][21], 0))
    const = relation_constant(schema, name)
    return ([f'inline constexpr SystemSqlColumn {const}Columns[] = {{'] +
            [column_line(*column, 'None', constants) for column in columns] +
            ['};',
             f'inline constexpr SystemSql {const}Sql{{{const}Function, '
             f"{cpp_str(schema)}, {cpp_str(name)}, 0, 0, '\\0', '\\0', "
             f'{cpp_bool(superuser)}, 0x0, false, {const}Columns}};'])


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


def named_oids(names, lookup):
    return [f'inline constexpr duckdb::idx_t {const} = {oid(lookup(name))};'
            for name, const in names.items()]


def schema_oid(gen, name):
    if name == 'information_schema':
        return gen.initdb.namespace
    return gen.one('pg_namespace', nspname=name)['oid']


def relations(gen, coded, types):
    out = []
    for name, relation in gen.relations.items():
        out.append((relation.oid, 'pg_catalog', name, 'r', relation.shared,
                    *gen.row_types[name], 'n',
                    catalog_columns(gen, relation, types), keys(gen, name)))
    for table in gen.initdb.tables.values():
        out.append((table.oid, 'information_schema', table.name, 'r', False,
                    table.row_type, table.row_array_type, 'd',
                    domain_columns(table.columns, types), {}))
    for view in gen.initdb.views:
        if (view.schema, view.name) in coded:
            out.append((view.oid, view.schema, view.name, 'v', False,
                        view.row_type, view.row_array_type, 'n',
                        domain_columns(gen.initdb.cast_columns(view), types),
                        {}))
    return sorted(out, key=lambda relation: relation[1:3])


def generate(gen, constants, all_types):
    types = Types(gen, all_types.values())
    reserved = [word for word, category in source.keywords()
                if category != 'UNRESERVED_KEYWORD']
    oids = (named_oids(config.SCHEMAS, lambda name: schema_oid(gen, name)) +
            named_oids(config.DATABASES,
                       lambda name: source.oid_macro('pg_database.h',
                                                     camel(name) + 'DbOid')) +
            named_oids(config.TABLESPACES,
                       lambda name: gen.one('pg_tablespace',
                                            spcname=name)['oid']) +
            named_oids(config.ACCESS_METHODS,
                       lambda name: gen.one('pg_am', amname=name)['oid']) +
            named_oids({name: const + 'Handler'
                        for name, const in config.ACCESS_METHODS.items()},
                       lambda name: gen.one('pg_am',
                                            amname=name)['amhandler']) +
            named_oids(config.LANGUAGES,
                       lambda name: gen.one('pg_language',
                                            lanname=name)['oid']) +
            named_oids({name: const + 'Validator'
                        for name, const in config.LANGUAGES.items()},
                       lambda name: gen.one('pg_language',
                                            lanname=name)['lanvalidator']) +
            named_oids(config.PROCEDURES,
                       lambda name: gen.one('pg_proc', proname=name,
                                            pronamespace=PG_CATALOG)['oid']) +
            named_oids(config.COLLATIONS,
                       lambda name: gen.one('pg_collation', collname=name,
                                            collnamespace=PG_CATALOG)['oid']))
    coded = table_files()
    tables, all_tables, known = [], [], set()
    for (relation_oid, schema, name, relkind, shared, row_type,
         row_array_type, replident, columns, indexed) in relations(
            gen, set(coded), types):
        known.add((schema, name))
        const = relation_constant(schema, name)
        oids.append(f'inline constexpr duckdb::idx_t {const}Table = '
                    f'{relation_oid};')
        tables += table_lines(schema, name, relkind, shared,
                              gen.initdb.superuser_only(schema, name),
                              row_type, row_array_type, replident, columns,
                              indexed, gen.initdb.public_columns.get(name, []),
                              constants)
        all_tables.append(f'&{const}Sql,')
    for schema, name in sorted(function_files()):
        procs = (gen.find('pg_proc', proname=name, pronamespace=PG_CATALOG,
                          proretset='t') if schema == 'pg_catalog' else [])
        if len(procs) != 1:
            raise SystemExit(f'table_functions/{schema}/{name}.cpp names no '
                             f'single set-returning function')
        const = relation_constant(schema, name)
        oids.append(f'inline constexpr duckdb::idx_t {const}Function = '
                    f'{procs[0]["oid"]};')
        tables += function_lines(procs[0], schema, name,
                                 name in gen.initdb.revoked_functions, types,
                                 constants)
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
