import re

from . import source

CREATE_VIEW = re.compile(
    r'^\s*CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+(\w+)'
    r'(?:\s+WITH\s*\(([^)]*)\))?\s+AS\s*$', re.M)
CREATE_TABLE = re.compile(r'^\s*CREATE\s+TABLE\s+(\w+)\s*\((.*)\)\s*\Z',
                          re.M | re.S)
CREATE_DOMAIN = re.compile(
    r'^\s*CREATE\s+DOMAIN\s+(\w+)\s+AS\s+(.*?)'
    r'(?:\s+COLLATE\s+"(\w+)")?(?:\s+DEFAULT\s+(\w+)\((\d+)\))?'
    r'(?:\s+CONSTRAINT\s.*)?\s*\Z', re.M | re.S)
CREATE_SCHEMA = re.compile(r'^\s*CREATE\s+SCHEMA\s+(\w+)\s*\Z', re.M)
DOMAIN_TYPE = re.compile(r'^([a-z ]+?)(?:\((\d+)\))?( with time zone)?$')
REVOKE_ALL = re.compile(r'REVOKE ALL ON (?:TABLE )?(\w+) FROM public', re.I)
GRANT_SELECT = re.compile(r'GRANT SELECT ON (?:TABLE )?(\w+) TO PUBLIC', re.I)
GRANT_COLUMNS = re.compile(r'GRANT SELECT \(([^)]*)\)\s+ON (\w+) TO public',
                           re.I)
REVOKE_EXECUTE = re.compile(r'REVOKE EXECUTE ON FUNCTION (\w+)\(', re.I)
CAST_COLUMN = re.compile(r'^CAST\s*\(.*\s+AS\s+(\w+)\)\s+AS\s+(\w+)$', re.S)

SQL_TYPES = {
    'integer': 'int4',
    'character varying': 'varchar',
    'name': 'name',
    'timestamp with time zone': 'timestamptz',
}

VARHDRSZ = 4


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
            i = text.find('*/', i)
            if i < 0:
                break
            i += 1
        elif ch == ';':
            out.append(text[start:i])
            start = i + 1
        i += 1
    return out


def split_list(text):
    items, depth, current = [], 0, ''
    for ch in text:
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
        if ch == ',' and depth == 0:
            items.append(current.strip())
            current = ''
        else:
            current += ch
    items.append(current.strip())
    return items


class View:
    def __init__(self, schema, name, options, body, oid):
        self.schema = schema
        self.name = name
        self.options = ','.join(
            re.sub(r'\s*=\s*', '=', option.strip())
            if '=' in option else option.strip() + '=true'
            for option in (options or '').split(',') if option.strip())
        self.body = body
        self.oid = oid
        self.row_type = oid + 2
        self.row_array_type = oid + 1


class Table:
    def __init__(self, name, columns, oid):
        self.name = name
        self.columns = columns
        self.oid = oid
        self.row_type = oid + 2
        self.row_array_type = oid + 1


class Domain:
    def __init__(self, name, spec, collation, default, oid):
        type_name, modifier, zone = DOMAIN_TYPE.match(spec.strip()).groups()
        base = SQL_TYPES.get(type_name + (zone or ''))
        if base is None:
            raise SystemExit(f'domain {name}: unknown type {spec!r}')
        self.name = name
        self.base = base
        self.typmod = -1
        if modifier is not None:
            self.typmod = int(modifier) + (VARHDRSZ if base == 'varchar'
                                           else 0)
        self.collation = collation
        self.default = default
        self.array = oid
        self.oid = oid + 1


class Initdb:
    def __init__(self):
        system_views = source.read('src', 'backend', 'catalog',
                                   'system_views.sql')
        information_schema = source.read('src', 'backend', 'catalog',
                                         'information_schema.sql')
        system_functions = source.read('src', 'backend', 'catalog',
                                       'system_functions.sql')
        initdb = source.read('src', 'bin', 'initdb', 'initdb.c')
        self.next_oid = source.define('access/transam.h',
                                      'FirstUnpinnedObjectId')
        self.views = []
        self.tables = {}
        self.domains = []
        self.namespace = None
        for statement in statements(system_views):
            self.add(statement, 'pg_catalog')
        for statement in statements(information_schema):
            self.add(statement, 'information_schema')
        self.revoked = set(REVOKE_ALL.findall(system_views + initdb))
        self.granted = set(GRANT_SELECT.findall(information_schema))
        self.public_columns = {
            table: [column.strip() for column in columns.split(',')]
            for columns, table in GRANT_COLUMNS.findall(system_views)}
        self.revoked_functions = set(
            REVOKE_EXECUTE.findall(system_views + system_functions))

    def allocate(self, count):
        oid = self.next_oid
        self.next_oid += count
        return oid

    def add(self, statement, schema):
        if match := CREATE_VIEW.search(statement):
            self.views.append(View(schema, match.group(1), match.group(2),
                                   statement[match.end():].strip('\n'),
                                   self.allocate(3)))
        elif match := CREATE_TABLE.search(statement):
            self.tables[match.group(1)] = Table(
                match.group(1),
                [tuple(column.split()) for column
                 in split_list(match.group(2))],
                self.allocate(3))
        elif match := CREATE_DOMAIN.search(statement):
            name, spec, collation, function, argument = match.groups()
            default = (f'{function.upper()}({argument})' if function
                       else '')
            self.domains.append(Domain(name, spec, collation, default,
                                       self.allocate(2)))
        elif CREATE_SCHEMA.search(statement):
            self.namespace = self.allocate(1)

    def view(self, schema, name):
        return next(view for view in self.views
                    if (view.schema, view.name) == (schema, name))

    def superuser_only(self, schema, name):
        if schema == 'information_schema':
            return name not in self.granted
        return name in self.revoked

    def cast_columns(self, view):
        select = re.search(r'^\s*SELECT\s(.*?)\n\s*FROM\s', view.body,
                           re.S).group(1)
        columns = []
        for item in split_list(select):
            match = CAST_COLUMN.match(item)
            if not match:
                raise SystemExit(f'{view.schema}.{view.name}: column '
                                 f'{item!r} is not a CAST(... AS type)')
            columns.append((match.group(2), match.group(1)))
        return columns
