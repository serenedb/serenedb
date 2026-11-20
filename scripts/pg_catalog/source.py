import os
import re
import subprocess
import tempfile

from .common import REPO_ROOT

ROOT = os.path.join(REPO_ROOT, 'third_party', 'postgres')

CREATE = re.compile(r'^create (\w+) (\d+)(.*)$')
COLUMN = re.compile(r'^\s*(\w+) = (\w+)( FORCE NOT NULL| FORCE NULL)?\s*,?$')
INDEX = re.compile(
    r'^declare (unique )?index \w+ \d+ on (\w+) using \w+\((.*)\)$')
KEYWORD = re.compile(r'^PG_KEYWORD\("(\w+)", \w+, (\w+), \w+\)', re.M)
CATALOG_MACRO = re.compile(r'^CATALOG\((\w+),(\d+),(\w+)\)', re.M)
CLASS_CASE = re.compile(r'^\t\tcase (\w+RelationId):', re.M)
PROPERTY_CLASS = re.compile(r'\{\s*"[^"]*",\s*(\w+RelationId),')
ACL_CLASS_ALIAS = re.compile(r'\(classId == (\w+RelationId)\) \?')
PROPERTY_TYPE = re.compile(
    r'\{\s*"[^"]*",\s*(\w+RelationId),[^{}]*?,\s*(OBJECT_\w+|-1),'
    r'\s*(?:true|false)\s*,?\s*\}')
ACL_BIT = re.compile(r'^#define\s+ACL_(\w+)\s+\(1\s*<<\s*(\d+)\)', re.M)
ACL_LETTER = re.compile(r"^#define\s+ACL_(\w+)_CHR\s+'(.)'", re.M)
ACL_RIGHTS = re.compile(r'^#define\s+ACL_ALL_RIGHTS_(\w+)\s+\(([^)]*)\)', re.M)


def path(*parts):
    return os.path.join(ROOT, *parts)


def read(*parts):
    with open(path(*parts)) as source:
        return source.read()


def available():
    return os.path.exists(path('src', 'backend', 'catalog', 'genbki.pl'))


def version():
    return re.search(r"^\s*version: '([\d.]+)',", read('meson.build'),
                     re.M).group(1)


def define(header, name):
    return int(re.search(rf'^#define\s+{name}\s+(\d+)',
                         read('src', 'include', header), re.M).group(1))


def oid_macro(header, name):
    return int(re.search(rf'DECLARE_OID_DEFINING_MACRO\({name}, (\d+)\)',
                         read('src', 'include', 'catalog', header)).group(1))


def keywords():
    return [(word, category) for word, category
            in KEYWORD.findall(read('src', 'include', 'parser', 'kwlist.h'))]


class Relation:
    def __init__(self, name, oid, flags, columns):
        self.name = name
        self.oid = oid
        self.shared = 'shared_relation' in flags
        self.rowtype = (int(flags[flags.index('rowtype_oid') + 1])
                        if 'rowtype_oid' in flags else None)
        self.columns = columns


class Index:
    def __init__(self, unique, table, columns):
        self.unique = unique
        self.table = table
        self.columns = columns


def values(text):
    out, i = [], 0
    while i < len(text):
        if text[i].isspace():
            i += 1
        elif text[i] == "'":
            j, value = i + 1, []
            while text[j] != "'" or text.startswith("''", j):
                value.append(text[j])
                j += 2 if text.startswith("''", j) else 1
            out.append(''.join(value))
            i = j + 1
        else:
            j = i
            while j < len(text) and not text[j].isspace():
                j += 1
            out.append(None if text[i:j] == '_null_' else text[i:j])
            i = j
    return out


def parse(text):
    relations, rows, indexes = {}, {}, []
    lines = iter(text.splitlines())
    current = None
    for line in lines:
        if match := CREATE.match(line):
            columns = []
            next(lines)
            for column in lines:
                if column.strip() == ')':
                    break
                name, type_name, force = COLUMN.match(column).groups()
                columns.append((name, type_name, (force or '').strip()))
            current = match.group(1)
            relations[current] = Relation(current, int(match.group(2)),
                                          match.group(3).split(), columns)
            rows[current] = []
        elif line.startswith('open '):
            current = line.split()[1]
        elif line.startswith('insert ('):
            names = [column[0] for column in relations[current].columns]
            rows[current].append(dict(zip(
                names, values(line[len('insert ('):line.rindex(')')]))))
        elif match := INDEX.match(line):
            indexes.append(Index(
                bool(match.group(1)), match.group(2),
                [key.split()[0] for key in match.group(3).split(',')]))
    return relations, rows, indexes


def tokens():
    return {
        'NAMEDATALEN': str(define('pg_config_manual.h', 'NAMEDATALEN')),
        'SIZEOF_POINTER': '8',
        'ALIGNOF_POINTER': 'd',
        'FLOAT8PASSBYVAL': 't',
    }


def substitute(text):
    replacements = tokens()
    return re.sub(r'\b(' + '|'.join(replacements) + r')\b',
                  lambda match: replacements[match.group(1)], text)


def catalog_headers():
    return re.findall(r"'(pg_\w+\.h)'", re.search(
        r'catalog_headers = \[(.*?)\]',
        read('src', 'include', 'catalog', 'meson.build'), re.S).group(1))


def function_body(text, signature):
    start = text.index('\n' + signature + '\n')
    return text[start:text.index('\n}\n', start)]


def catalogs():
    return [(name, int(oid), macro) for header in catalog_headers()
            for name, oid, macro in CATALOG_MACRO.findall(
                read('src', 'include', 'catalog', header))]


def object_classes():
    relations = {macro: name for name, _, macro in catalogs()}
    text = read('src', 'backend', 'catalog', 'objectaddress.c')
    described = CLASS_CASE.findall(function_body(
        text, 'getObjectDescription(const ObjectAddress *object, bool missing_ok)'))
    properties = text[text.index('ObjectProperty[] =\n'):]
    with_acl = (PROPERTY_CLASS.findall(properties[:properties.index('\n};\n')]) +
                ACL_CLASS_ALIAS.findall(function_body(text, 'pg_get_acl(PG_FUNCTION_ARGS)')))
    return ([relations[macro] for macro in described],
            [relations[macro] for macro in with_acl])


def object_type_classes():
    oids = {macro: oid for _, oid, macro in catalogs()}
    text = read('src', 'backend', 'catalog', 'objectaddress.c')
    properties = text[text.index('ObjectProperty[] =\n'):]
    classes = {objtype: oids[macro] for macro, objtype in PROPERTY_TYPE.findall(
        properties[:properties.index('\n};\n')]) if objtype != '-1'}
    for objtype in re.findall(r'return (OBJECT_\w+);', function_body(
            text, 'get_relkind_objtype(char relkind)')):
        classes.setdefault(objtype, oids['RelationRelationId'])
    return classes


def privilege_bits():
    return [(name, int(bit)) for name, bit in ACL_BIT.findall(
        read('src', 'include', 'nodes', 'parsenodes.h'))]


def privilege_letters():
    return dict(ACL_LETTER.findall(read('src', 'include', 'utils', 'acl.h')))


def privilege_names():
    return dict(re.findall(r'case ACL_(\w+):\s*return "([^"]+)";', function_body(
        read('src', 'backend', 'utils', 'adt', 'acl.c'),
        'convert_aclright_to_string(int aclright)')))


def all_rights():
    return {name: re.findall(r'ACL_(\w+)', value) for name, value
            in ACL_RIGHTS.findall(read('src', 'include', 'utils', 'acl.h'))}


def default_privileges():
    rights = all_rights()

    def expand(expression):
        bits = []
        for name in re.findall(r'ACL_(\w+)', expression):
            if name.startswith('ALL_RIGHTS_'):
                bits += rights[name.removeprefix('ALL_RIGHTS_')]
            elif name != 'NO_RIGHTS':
                bits.append(name)
        return bits

    body = function_body(read('src', 'backend', 'utils', 'adt', 'acl.c'),
                         'acldefault(ObjectType objtype, Oid ownerId)')
    defaults = {}
    objtypes = []
    granted = {}
    for line in body.splitlines():
        if case := re.match(r'\s*case (OBJECT_\w+):', line):
            objtypes.append(case.group(1))
        elif default := re.match(r'\s*(world|owner)_default = (.+);', line):
            granted[default.group(1)] = expand(default.group(2))
        elif line.strip() == 'break;':
            for objtype in objtypes:
                defaults[objtype] = (granted['world'], granted['owner'])
            objtypes = []
    return defaults


def object_nouns():
    body = function_body(read('src', 'backend', 'catalog', 'aclchk.c'),
                         'aclcheck_error(AclResult aclerr, ObjectType objtype,')
    return dict(re.findall(
        r'case (OBJECT_\w+):\s*msg = gettext_noop\('
        r'"(?:permission denied for|must be owner of) ([^"%]+) %s"\);', body))


def default_privilege_letters():
    return re.findall(r"case '(.)':\s*objtype = (OBJECT_\w+);", function_body(
        read('src', 'backend', 'utils', 'adt', 'acl.c'),
        'acldefault_sql(PG_FUNCTION_ARGS)'))


def bki():
    headers = catalog_headers()
    with tempfile.TemporaryDirectory() as out:
        subprocess.run(
            ['perl', '-I', path('src', 'backend', 'catalog'),
             path('src', 'backend', 'catalog', 'genbki.pl'),
             '--include-path=' + path('src', 'include'),
             '--set-version=' + version().split('.')[0],
             '--output=' + out + os.sep] +
            [path('src', 'include', 'catalog', header) for header in headers],
            check=True)
        with open(os.path.join(out, 'postgres.bki')) as generated:
            return parse(substitute(generated.read()))


def row_types(relations, types):
    arrays = {int(row['typelem']): int(row['oid']) for row in types
              if row['typelem'] != '0'}
    next_oid = define('access/transam.h', 'FirstGenbkiObjectId')
    out = {}
    for relation in relations.values():
        if relation.rowtype is None:
            out[relation.name] = (next_oid + 1, next_oid)
            next_oid += 2
        elif relation.rowtype in arrays:
            out[relation.name] = (relation.rowtype, arrays[relation.rowtype])
        else:
            out[relation.name] = (relation.rowtype, next_oid)
            next_oid += 1
    return out
