import os
import re

from . import config
from .common import CATALOG_DIR, SUPERUSER_ONLY_SQL, cpp_bool

OVERRIDES_DIR = os.path.join(CATALOG_DIR, 'views', 'overrides')

VIEW_OIDS_SQL = f"""
SELECT n.nspname, c.relname, c.oid::int8, {SUPERUSER_ONLY_SQL}
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
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


def override(schema, name):
    path = os.path.join(OVERRIDES_DIR, f'{schema}.{name}.sql')
    if not os.path.exists(path):
        return None
    with open(path) as source:
        return source.read().rstrip('\n')


def entry(schema, name, oid, superuser, body):
    lines = dedent(body)
    sql = '\n'.join([lines[0]] + ['      ' + l if l else '' for l in lines[1:]])
    return (f'{{"{schema}", "{name}", {oid}, {cpp_bool(superuser)},\n'
            f' R"({sql})"}},')


def generate(gen):
    oids = {(s, n): (oid, superuser)
            for s, n, oid, superuser in gen.query(VIEW_OIDS_SQL)}
    pg = list(views(gen.source('src/backend/catalog/system_views.sql')))
    info = list(views(gen.source('src/backend/catalog/information_schema.sql')))
    info_names = {name for name, _ in info if not name.startswith('_pg_')}
    out, used = [], set()
    for schema, items in (('pg_catalog', pg), ('information_schema', info)):
        for name, body in items:
            if (schema, name) in config.NATIVE_VIEWS:
                continue
            custom = override(schema, name)
            if custom is not None:
                used.add(f'{schema}.{name}.sql')
                body = custom
            elif schema == 'information_schema':
                body = qualify(body, info_names)
            out.append(entry(schema, name, *oids[(schema, name)], body))
    stale = sorted(set(os.listdir(OVERRIDES_DIR)) - used)
    if stale:
        raise SystemExit(f'overrides for views PostgreSQL does not have: {stale}')
    gen.write('views/system_views.gen.inc', out)
