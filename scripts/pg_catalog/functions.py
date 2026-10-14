from . import config
from .common import array, cpp_bool, oid

PG_CATALOG = '11'

STABILITIES = {
    'i': 'CONSISTENT',
    's': 'CONSISTENT_WITHIN_QUERY',
    'v': 'VOLATILE',
}


def stub(row):
    types = (array(row['proallargtypes']) if row['proallargtypes'] is not None
             else row['proargtypes'].split())
    return (row['proname'], array(row['proargnames']) or None,
            array(row['proargmodes']) or None, [int(t) for t in types],
            oid(row['prorettype']), row['provolatile'])


def generate(gen, constants, all_types):
    names = {}
    for type_oid, row in all_types.items():
        element = all_types.get(row[10])
        names[type_oid] = (element[1] + '[]'
                           if element and element[11] == type_oid
                           else row[1])
    stubs = (config.STUB_FUNCTIONS + config.ZERO_STUB_FUNCTIONS +
             config.NULL_STUB_FUNCTIONS)
    rows = {row['proname']: stub(row) for row in sorted(
        (row for row in gen.rows['pg_proc']
         if row['pronamespace'] == PG_CATALOG and row['proname'] in stubs),
        key=lambda row: row['proname'])}
    missing = [name for name in stubs if name not in rows]
    if missing:
        raise SystemExit(f'functions unknown to PostgreSQL: {missing}')

    def typed(oid):
        typname = names[oid]
        return config.STUB_TYPES.get(typname, typname)

    out, scalars = [], []
    for name in stubs:
        _, argnames, modes, types, rettype, volatility = rows[name]
        modes = modes or ['i'] * len(types)
        argnames = argnames or [f'arg{i + 1}' for i in range(len(types))]
        inputs = [i for i, mode in enumerate(modes) if mode in 'ib']
        if name not in config.STUB_FUNCTIONS:
            zero = name in config.ZERO_STUB_FUNCTIONS
            if len(inputs) > 1 or (zero and names[rettype] not in
                                   ('int8', 'float8')):
                raise SystemExit(f'{name}: a scalar stub takes at most one '
                                 f'argument and returns 0 as int8 or float8')
            argument = constants[types[inputs[0]]] if inputs else '0'
            scalars.append(f'{{"{name}", {argument}, {constants[rettype]}, '
                           f'{cpp_bool(zero)}, duckdb::FunctionStability::'
                           f'{STABILITIES[volatility]}}},')
            continue
        args = [f'"{argnames[i]}" {typed(types[i])}' for i in inputs]
        columns = [i for i, mode in enumerate(modes) if mode in 'otb']
        if 'v' in modes:
            variadic = [argnames[i] for i, mode in enumerate(modes)
                        if mode in 'ibv']
            select = ', '.join(f'NULL::{typed(types[i])} AS "{argnames[i]}"'
                               for i in columns)
            out.append(f'{{"pg_catalog", "{name}",\n R"(({", ".join(variadic)}) '
                       f'AS TABLE SELECT {select} WHERE false)"}},')
            continue
        returns = ',\n                '.join(
            f'"{argnames[i]}" {typed(types[i])}' for i in columns)
        nulls = ', '.join(f'NULL::{typed(types[i])}' for i in columns)
        out.append(f'{{"pg_catalog", "{name}",\n R"(({", ".join(args)})\n'
                   f'  RETURNS TABLE({returns})\n'
                   f'  LANGUAGE SQL\n'
                   f'  BEGIN ATOMIC\n'
                   f'    SELECT {nulls} WHERE false;\n'
                   f'  END;)"}},')
    gen.write('function_stubs.gen.inc', out)
    gen.write('scalar_stubs.gen.inc', scalars)
