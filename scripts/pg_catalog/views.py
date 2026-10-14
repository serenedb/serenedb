import re

from . import relations
from .common import cpp_bool


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


def entry(schema, name, oid, row_type, row_array_type, superuser, options,
          body):
    head = (f'{{"{schema}", "{name}", {oid}, {row_type}, {row_array_type}, '
            f'{cpp_bool(superuser)}, "{options}",')
    if body is None:
        return f'{head} {{}}}},'
    lines = dedent(body)
    sql = '\n'.join([lines[0]] + ['      ' + l if l else '' for l in lines[1:]])
    return f'{head}\n R"({sql})"}},'


def generate(gen):
    info_names = {view.name for view in gen.initdb.views
                  if view.schema == 'information_schema' and
                  not view.name.startswith('_pg_')}
    native = set(relations.table_files())
    files = set(relations.view_files())
    out, defined = [], set()
    for view in gen.initdb.views:
        if (view.schema, view.name) in native:
            continue
        defined.add((view.schema, view.name))
        body = view.body
        if (view.schema, view.name) in files:
            body = None
        elif view.schema == 'information_schema':
            body = qualify(body, info_names)
        out.append(entry(view.schema, view.name, view.oid, view.row_type,
                         view.row_array_type,
                         gen.initdb.superuser_only(view.schema, view.name),
                         view.options, body))
    if stale := sorted(files - defined):
        raise SystemExit(f'view files for views PostgreSQL does not have: {stale}')
    gen.write('system_views.gen.inc', out)
