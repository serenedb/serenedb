import re

from . import config, relations
from .common import SUPERUSER_ONLY_SQL, cpp_bool

VIEW_OIDS_SQL = f"""
SELECT n.nspname, c.relname, c.oid::int8, c.reltype::int4,
       coalesce(t.typarray, 0)::int4, {SUPERUSER_ONLY_SQL}
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_type t ON t.oid = c.reltype
WHERE c.relkind = 'v' AND n.nspname IN ('pg_catalog', 'information_schema')
"""

CREATE_VIEW = re.compile(
    r'^\s*CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+(\w+)'
    r'(?:\s+WITH\s*\([^)]*\))?\s+AS\s*$', re.M)


def statements(text):
    out, start, quote, i = [], 0, False, 0
    while i < len(text):
        ch = text[i]
        if quote:
            if ch == "'":
                quote = False
        elif ch == "'":
            quote = True
        elif text.startswith('--', i):
            i = text.find('\n', i)
            if i < 0:
                break
        elif text.startswith('/*', i):
            i = text.find('*/', i) + 1
        elif ch == ';':
            out.append(text[start:i])
            start = i + 1
        i += 1
    return out


def views(path):
    with open(path) as source:
        text = source.read()
    for statement in statements(text):
        match = CREATE_VIEW.search(statement)
        if match:
            yield match.group(1), statement[match.end():].strip('\n')


def dedent(body):
    lines = body.split('\n')
    indent = min((len(l) - len(l.lstrip()) for l in lines if l.strip()),
                 default=0)
    return [l[indent:].rstrip() for l in lines]


def qualify(sql, names):
    sql = re.sub(r'(?<!information_schema\.)(?<!\w)(_pg_\w+)',
                 r'information_schema.\1', sql)
    for name in sorted(names, key=len, reverse=True):
        sql = re.sub(r'(?<![\w.])' + re.escape(name) + r'(?!\w)',
                     'information_schema.' + name, sql)
    return sql


def entry(schema, name, oid, row_type, row_array_type, superuser, body):
    head = (f'{{"{schema}", "{name}", {oid}, {row_type}, {row_array_type}, '
            f'{cpp_bool(superuser)},')
    if body is None:
        return f'{head} {{}}}},'
    lines = dedent(body)
    sql = '\n'.join([lines[0]] + ['      ' + l if l else '' for l in lines[1:]])
    return f'{head}\n R"({sql})"}},'


def generate(gen):
    oids = {(s, n): (oid, row_type, row_array_type, superuser)
            for s, n, oid, row_type, row_array_type, superuser
            in gen.query(VIEW_OIDS_SQL)}
    pg = list(views(gen.source('src/backend/catalog/system_views.sql')))
    info = list(views(gen.source('src/backend/catalog/information_schema.sql')))
    info_names = {name for name, _ in info if not name.startswith('_pg_')}
    files = set(relations.view_files())
    out, defined = [], set(config.SDB_VIEWS)
    for schema, items in (('pg_catalog', pg), ('information_schema', info)):
        for name, body in items:
            if (schema, name) in config.NATIVE_VIEWS:
                continue
            defined.add((schema, name))
            if (schema, name) in files:
                body = None
            elif schema == 'information_schema':
                body = qualify(body, info_names)
            out.append(entry(schema, name, *oids[(schema, name)], body))
    out += [entry(schema, name, f'kMinSystem + {oid}', 0, 0, False, None)
            for (schema, name), oid in config.SDB_VIEWS.items()]
    if stale := sorted(files - defined):
        raise SystemExit(f'view files for views PostgreSQL does not have: {stale}')
    if missing := sorted(set(config.SDB_VIEWS) - files):
        raise SystemExit(f'SereneDB views without a file: {missing}')
    gen.write('system_views.gen.inc', out)
