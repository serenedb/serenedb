import re

from . import builtins
from .common import PG_CATALOG, array, camel, oid

STABILITIES = {
    'i': 'CONSISTENT',
    's': 'CONSISTENT_WITHIN_QUERY',
    'v': 'VOLATILE',
}

CALL = re.compile(r'(?<![\w.$"])(?:pg_catalog\.)?([A-Za-z_]\w*)\s*\(')


def stub(row):
    types = (array(row['proallargtypes']) if row['proallargtypes'] is not None
             else row['proargtypes'].split())
    return (row['proname'], array(row['proargnames']) or None,
            array(row['proargmodes']) or None, [int(t) for t in types],
            oid(row['prorettype']), row['proretset'] == 't',
            row['provolatile'])


def generate(gen, constants, all_types, bodies):
    names = {}
    for type_oid, row in all_types.items():
        element = all_types.get(row.typelem)
        names[type_oid] = (element.typname + '[]'
                           if element and element.typarray == type_oid
                           else row.typname)
    mapped = builtins.mapped_types()
    usable = {type_oid for type_oid, row in all_types.items()
              if row.typtype != 'p' and
              camel((builtins.array_element(row, all_types) or row).typname)
              in mapped}
    called = {name.lower() for body in bodies for name in CALL.findall(body)}
    rows = [stub(row) for row in sorted(
        (row for row in gen.rows['pg_proc']
         if row['pronamespace'] == PG_CATALOG and row['prokind'] == 'f' and
         row['proname'] in called),
        key=lambda row: (row['proname'], int(row['oid'])))]

    out, scalars = [], []
    for name, argnames, modes, types, rettype, retset, volatility in rows:
        modes = modes or ['i'] * len(types)
        argnames = argnames or [f'arg{i + 1}' for i in range(len(types))]
        inputs = [i for i, mode in enumerate(modes) if mode in 'ibv']
        columns = [i for i, mode in enumerate(modes) if mode in 'otb']
        signature = ([types[i] for i in inputs + columns] +
                     ([] if columns else [rettype]))
        if (retset and not columns) or not usable.issuperset(signature):
            continue
        if not columns:
            arguments = ', '.join(constants[types[i]] for i in inputs)
            scalars.append(f'{{"{name}", {{{arguments}}}, '
                           f'{constants[rettype]}, duckdb::FunctionStability::'
                           f'{STABILITIES[volatility]}}},')
            continue
        if 'v' in modes:
            select = ', '.join(f'NULL::{names[types[i]]} AS "{argnames[i]}"'
                               for i in columns)
            variadic = ', '.join(argnames[i] for i in inputs)
            out.append(f'{{"pg_catalog", "{name}",\n R"(({variadic}) '
                       f'AS TABLE SELECT {select} WHERE false)"}},')
            continue
        args = ', '.join(f'"{argnames[i]}" {names[types[i]]}' for i in inputs)
        returns = ',\n                '.join(
            f'"{argnames[i]}" {names[types[i]]}' for i in columns)
        nulls = ', '.join(f'NULL::{names[types[i]]}' for i in columns)
        out.append(f'{{"pg_catalog", "{name}",\n R"(({args})\n'
                   f'  RETURNS TABLE({returns})\n'
                   f'  LANGUAGE SQL\n'
                   f'  BEGIN ATOMIC\n'
                   f'    SELECT {nulls} WHERE false;\n'
                   f'  END;)"}},')
    gen.write('function_stubs.gen.inc', out)
    gen.write('scalar_stubs.gen.inc', scalars)
