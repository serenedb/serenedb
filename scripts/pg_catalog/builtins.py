import os
import re

from . import config
from .common import REPO_ROOT, camel, cpp_bool, cpp_char, cpp_str, oid

PG_CATALOG = '11'

PROC_COLUMNS = (9, 12, 13, 14, 15, 16, 17, 18)
REFERENCE_COLUMNS = (10, 11, 22)

TYPE_MAPPINGS = os.path.join(REPO_ROOT, 'server', 'pg', 'types.cpp')
MAPPINGS_TABLE = re.compile(r'kMappings\[\] = \{\n(.*?)\n\};', re.S)
MAPPED_TYPE = re.compile(r'^\s*\{k(\w+),', re.M)

SDB_TYPES = os.path.join(REPO_ROOT, 'server', 'pg', 'catalog', 'builtin',
                         'builtin.h')
SDB_TYPE = re.compile(r'Sdb(?:Type|Array)\(\s*k\w+,\s*"(\w+)"')


def mapped_types():
    with open(TYPE_MAPPINGS) as source:
        table = MAPPINGS_TABLE.search(source.read()).group(1)
    return set(MAPPED_TYPE.findall(table))


def sdb_types():
    with open(SDB_TYPES) as source:
        return SDB_TYPE.findall(source.read())


def array_element(row, by_oid):
    element = by_oid.get(row[10])
    if element and element[11] == row[0] and row[1].startswith('_'):
        return element
    return None


def bki_type(row):
    return (int(row['oid']), row['typname'], 'pg_catalog', int(row['typlen']),
            row['typbyval'] == 't', row['typtype'], row['typcategory'],
            row['typispreferred'] == 't', row['typdelim'],
            oid(row['typsubscript']), oid(row['typelem']),
            oid(row['typarray']), oid(row['typinput']),
            oid(row['typoutput']), oid(row['typreceive']),
            oid(row['typsend']), oid(row['typmodin']), oid(row['typmodout']),
            oid(row['typanalyze']), row['typalign'], row['typstorage'],
            oid(row['typcollation']), oid(row['typbasetype']),
            oid(row['typrelid']), int(row['typtypmod']),
            row['typnotnull'] == 't', row['typdefault'] or '')


def domain_types(gen, by_name):
    domain_in = int(gen.one('pg_proc', proname='domain_in')['oid'])
    domain_recv = int(gen.one('pg_proc', proname='domain_recv')['oid'])
    template = by_name['_int4']
    out = []
    for domain in gen.initdb.domains:
        base = by_name[domain.base]
        collation = (int(gen.one('pg_collation', collname=domain.collation)
                         ['oid']) if domain.collation else base[21])
        out.append((domain.array, '_' + domain.name, 'information_schema') +
                   template[3:10] + (domain.oid, 0) + template[12:19] +
                   ('d' if base[19] == 'd' else 'i', template[20],
                    collation) + template[22:])
        out.append((domain.oid, domain.name, 'information_schema', base[3],
                    base[4], 'd', base[6], False, ',', 0, 0, domain.array,
                    domain_in, base[13], domain_recv, base[15], 0, 0, 0,
                    base[19], base[20], collation, base[0], 0, domain.typmod,
                    False, domain.default))
    return out


def procedure(row):
    return (int(row['oid']), row['proname'], int(row['pronargs']),
            oid(row['prorettype']),
            [int(arg) for arg in row['proargtypes'].split()],
            row['prokind'], row['proretset'] == 't', row['provolatile'],
            row['proisstrict'] == 't', oid(row['prolang']), row['prosrc'])


def procedures(gen, types):
    oids = {row[i] for row in types for i in PROC_COLUMNS if row[i]}
    oids |= {oid(am['amhandler']) for am in gen.rows['pg_am']
             if am['amname'] in config.ACCESS_METHODS}
    oids |= {oid(language[column]) for language in gen.rows['pg_language']
             if language['lanname'] in config.LANGUAGES
             for column in ('lanplcallfoid', 'laninline', 'lanvalidator')}
    return sorted(procedure(row) for row in gen.rows['pg_proc']
                  if int(row['oid']) in oids or
                  (row['pronamespace'] == PG_CATALOG and
                   row['proname'] in config.PROCEDURES))


def supported_types(gen):
    excluded = set(sdb_types())
    types = [bki_type(row) for row in gen.rows['pg_type']
             if row['typname'] not in excluded]
    types = sorted(types + domain_types(gen, {row[1]: row for row in types}))
    by_oid = {row[0]: row for row in types}
    mapped = mapped_types()
    keep = {row[0] for row in types
            if row[1] in config.PSEUDO_TYPES or
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
        base = camel((element or row)[1])
        constants[row[0]] = f'k{base}{"Array" if element else ""}'
    return constants


def generate(gen):
    types, by_oid, procs = supported_types(gen)
    constants = type_constants(types, by_oid)
    collations = sorted(
        (int(row['oid']), row['collname'], row['collprovider'],
         row['collcollate'] or '') for row in gen.rows['pg_collation']
        if row['collnamespace'] == PG_CATALOG and
        row['collname'] in config.COLLATIONS)
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
    return constants, by_oid
