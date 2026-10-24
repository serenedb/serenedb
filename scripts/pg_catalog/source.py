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


def bki():
    headers = re.findall(r"'(pg_\w+\.h)'", re.search(
        r'catalog_headers = \[(.*?)\]',
        read('src', 'include', 'catalog', 'meson.build'), re.S).group(1))
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
