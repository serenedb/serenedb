from . import config

STUBS_SQL = """
SELECT p.proname, p.proargnames, p.proargmodes,
       coalesce(p.proallargtypes, p.proargtypes::oid[])::int8[],
       p.prorettype::int8
FROM pg_proc p
WHERE p.pronamespace = 'pg_catalog'::regnamespace AND p.proname = ANY(%s)
ORDER BY p.proname
"""

TYPE_SQL = """
SELECT t.oid::int8, coalesce(e.typname || '[]', t.typname)
FROM pg_type t LEFT JOIN pg_type e ON e.oid = t.typelem AND e.typarray = t.oid
"""


def generate(gen):
    names = dict(gen.query(TYPE_SQL))
    rows = {row[0]: row for row in gen.query(STUBS_SQL,
                                             (list(config.STUB_FUNCTIONS),))}
    missing = [name for name in config.STUB_FUNCTIONS if name not in rows]
    if missing:
        raise SystemExit(f'functions unknown to PostgreSQL: {missing}')
    out = []
    for name in config.STUB_FUNCTIONS:
        _, argnames, modes, types, rettype = rows[name]
        modes = modes or ['i'] * len(types)
        argnames = argnames or [f'arg{i + 1}' for i in range(len(types))]

        def typed(i):
            typname = names[types[i]]
            return config.STUB_TYPES.get(typname, typname)

        columns = [i for i, mode in enumerate(modes) if mode in 'otb']
        if 'v' in modes:
            args = [argnames[i] for i, mode in enumerate(modes) if mode in 'ibv']
            select = ', '.join(f'NULL::{typed(i)} AS "{argnames[i]}"'
                               for i in columns)
            out.append(f'{{"pg_catalog", "{name}",\n R"(({", ".join(args)}) '
                       f'AS TABLE SELECT {select} WHERE false)"}},')
            continue
        args = [f'"{argnames[i]}" {typed(i)}'
                for i, mode in enumerate(modes) if mode in 'ib']
        returns = ',\n                '.join(
            f'"{argnames[i]}" {typed(i)}' for i in columns)
        nulls = ', '.join(f'NULL::{typed(i)}' for i in columns)
        out.append(f'{{"pg_catalog", "{name}",\n R"(({", ".join(args)})\n'
                   f'  RETURNS TABLE({returns})\n'
                   f'  LANGUAGE SQL\n'
                   f'  BEGIN ATOMIC\n'
                   f'    SELECT {nulls} WHERE false;\n'
                   f'  END;)"}},')
    gen.write('functions/stubs.gen.inc', out)
