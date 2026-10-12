from . import config
from .common import cpp_bool

STUBS_SQL = """
SELECT p.proname, p.proargnames, p.proargmodes,
       coalesce(p.proallargtypes, p.proargtypes::oid[])::int8[],
       p.prorettype::int8, p.provolatile
FROM pg_proc p
WHERE p.pronamespace = 'pg_catalog'::regnamespace AND p.proname = ANY(%s)
ORDER BY p.proname
"""

TYPE_SQL = """
SELECT t.oid::int8, coalesce(e.typname || '[]', t.typname)
FROM pg_type t LEFT JOIN pg_type e ON e.oid = t.typelem AND e.typarray = t.oid
"""

STABILITIES = {
    'i': 'CONSISTENT',
    's': 'CONSISTENT_WITHIN_QUERY',
    'v': 'VOLATILE',
}


def generate(gen, constants):
    names = dict(gen.query(TYPE_SQL))
    stubs = (config.STUB_FUNCTIONS + config.ZERO_STUB_FUNCTIONS +
             config.NULL_STUB_FUNCTIONS)
    rows = {row[0]: row for row in gen.query(STUBS_SQL, (list(stubs),))}
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
