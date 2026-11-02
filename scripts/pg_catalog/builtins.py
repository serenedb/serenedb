import collections
import os
import re

from . import config
from .common import (PG_CATALOG, REPO_ROOT, camel, cpp_bool, cpp_char,
                     cpp_str, oid)

TypeRow = collections.namedtuple('TypeRow', (
    'oid', 'typname', 'namespace', 'typlen', 'typbyval', 'typtype',
    'typcategory', 'typispreferred', 'typdelim', 'typsubscript', 'typelem',
    'typarray', 'typinput', 'typoutput', 'typreceive', 'typsend', 'typmodin',
    'typmodout', 'typanalyze', 'typalign', 'typstorage', 'typcollation',
    'typbasetype', 'typrelid', 'typtypmod', 'typnotnull', 'typdefault'))
Procedure = collections.namedtuple('Procedure', (
    'oid', 'proname', 'pronargs', 'prorettype', 'proargtypes', 'prokind',
    'proretset', 'provolatile', 'proisstrict', 'prolang', 'prosrc'))

PROC_COLUMNS = ('typsubscript', 'typinput', 'typoutput', 'typreceive',
                'typsend', 'typmodin', 'typmodout', 'typanalyze')
REFERENCE_COLUMNS = ('typelem', 'typarray', 'typbasetype')
RANGE_COLUMNS = ('rngtypid', 'rngsubtype', 'rngmultitypid', 'rngcollation',
                 'rngsubopc', 'rngcanonical', 'rngsubdiff')
RANGE_PROCS = ('rngcanonical', 'rngsubdiff')

TYPE_MAPPINGS = os.path.join(REPO_ROOT, 'server', 'pg', 'types', 'mapping.cpp')
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
    element = by_oid.get(row.typelem)
    if (element and element.typarray == row.oid and
            row.typname.startswith('_')):
        return element
    return None


def bki_type(row):
    return TypeRow(
        oid=int(row['oid']), typname=row['typname'], namespace='pg_catalog',
        typlen=int(row['typlen']), typbyval=row['typbyval'] == 't',
        typtype=row['typtype'], typcategory=row['typcategory'],
        typispreferred=row['typispreferred'] == 't',
        typdelim=row['typdelim'], typsubscript=oid(row['typsubscript']),
        typelem=oid(row['typelem']), typarray=oid(row['typarray']),
        typinput=oid(row['typinput']), typoutput=oid(row['typoutput']),
        typreceive=oid(row['typreceive']), typsend=oid(row['typsend']),
        typmodin=oid(row['typmodin']), typmodout=oid(row['typmodout']),
        typanalyze=oid(row['typanalyze']), typalign=row['typalign'],
        typstorage=row['typstorage'], typcollation=oid(row['typcollation']),
        typbasetype=oid(row['typbasetype']), typrelid=oid(row['typrelid']),
        typtypmod=int(row['typtypmod']), typnotnull=row['typnotnull'] == 't',
        typdefault=row['typdefault'] or '')


def domain_types(gen, by_name):
    domain_in = int(gen.one('pg_proc', proname='domain_in')['oid'])
    domain_recv = int(gen.one('pg_proc', proname='domain_recv')['oid'])
    template = by_name['_int4']
    out = []
    for domain in gen.initdb.domains:
        base = by_name[domain.base]
        collation = (int(gen.one('pg_collation', collname=domain.collation)
                         ['oid']) if domain.collation else base.typcollation)
        out.append(template._replace(
            oid=domain.array, typname='_' + domain.name,
            namespace='information_schema', typelem=domain.oid, typarray=0,
            typalign='d' if base.typalign == 'd' else 'i',
            typcollation=collation))
        out.append(TypeRow(
            oid=domain.oid, typname=domain.name,
            namespace='information_schema', typlen=base.typlen,
            typbyval=base.typbyval, typtype='d',
            typcategory=base.typcategory, typispreferred=False, typdelim=',',
            typsubscript=0, typelem=0, typarray=domain.array,
            typinput=domain_in, typoutput=base.typoutput,
            typreceive=domain_recv, typsend=base.typsend, typmodin=0,
            typmodout=0, typanalyze=0, typalign=base.typalign,
            typstorage=base.typstorage, typcollation=collation,
            typbasetype=base.oid, typrelid=0, typtypmod=domain.typmod,
            typnotnull=False, typdefault=domain.default))
    return out


def procedure(row):
    return Procedure(
        oid=int(row['oid']), proname=row['proname'],
        pronargs=int(row['pronargs']), prorettype=oid(row['prorettype']),
        proargtypes=[int(arg) for arg in row['proargtypes'].split()],
        prokind=row['prokind'], proretset=row['proretset'] == 't',
        provolatile=row['provolatile'],
        proisstrict=row['proisstrict'] == 't', prolang=oid(row['prolang']),
        prosrc=row['prosrc'])


def procedures(gen, types):
    oids = {getattr(row, column) for row in types for column in PROC_COLUMNS
            if getattr(row, column)}
    oids |= {oid(am['amhandler']) for am in gen.rows['pg_am']
             if am['amname'] in config.ACCESS_METHODS}
    oids |= {oid(language[column]) for language in gen.rows['pg_language']
             if language['lanname'] in config.LANGUAGES
             for column in ('lanplcallfoid', 'laninline', 'lanvalidator')}
    oids |= {oid(row[column]) for row in gen.rows['pg_range']
             for column in RANGE_PROCS}
    return sorted(procedure(row) for row in gen.rows['pg_proc']
                  if int(row['oid']) in oids or
                  (row['pronamespace'] == PG_CATALOG and
                   row['proname'] in config.PROCEDURES))


def supported_types(gen):
    excluded = set(sdb_types())
    types = [bki_type(row) for row in gen.rows['pg_type']
             if row['typname'] not in excluded]
    types = sorted(types + domain_types(
        gen, {row.typname: row for row in types}))
    by_oid = {row.oid: row for row in types}
    kept = [row for row in types
            if (array_element(row, by_oid) or row).typtype != 'c']
    keep = {row.oid for row in kept}
    procs = procedures(gen, kept)
    if missing := {oid for proc in procs
                   for oid in (proc.prorettype, *proc.proargtypes)} - keep:
        raise SystemExit(f'procedures refer to types SereneDB owns: '
                         f'{sorted(missing)}')
    for row in kept:
        for column in REFERENCE_COLUMNS:
            reference = getattr(row, column)
            if reference and reference not in keep:
                raise SystemExit(f'type {row.typname} refers to type '
                                 f'{by_oid[reference].typname}, which '
                                 f'SereneDB does not list')
    return kept, by_oid, procs


def type_constants(types, by_oid):
    constants = {}
    for row in types:
        element = array_element(row, by_oid)
        base = camel((element or row).typname)
        constants[row.oid] = f'k{base}{"Array" if element else ""}'
    return constants


def generate(gen):
    types, by_oid, procs = supported_types(gen)
    constants = type_constants(sorted(by_oid.values()), by_oid)
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
    gen.write('builtin_ranges.gen.inc', [
        f'{{{", ".join(str(oid(row[column])) for column in RANGE_COLUMNS)}}},'
        for row in sorted(gen.rows['pg_range'],
                          key=lambda row: int(row['rngtypid']))])
    gen.write('builtin_collations.gen.inc', [
        f'{{{oid}, {cpp_str(name)}, {cpp_char(provider)}, {cpp_str(locale)}}},'
        for oid, name, provider, locale in collations])
    listed = {proc.oid for proc in procs}
    gen.write('pg_proc_identities.gen.inc', [
        f'{{{nsp}, {cpp_str(name)}, {cpp_char(kind)}, {cpp_str(argtypes)}, '
        f'{oid}}},'
        for nsp, name, kind, argtypes, oid in sorted(
            (int(row['pronamespace']), row['proname'], row['prokind'],
             row['proargtypes'], int(row['oid']))
            for row in gen.rows['pg_proc'] if int(row['oid']) not in listed)])
    return constants, by_oid
