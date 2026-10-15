import os
import re

from .common import CATALOG_DIR, GENERATED_DIR, REPO_ROOT, cpp_str, generated

PATH = os.path.join(GENERATED_DIR, 'system_oids.lock')
OIDS = os.path.join(CATALOG_DIR, 'oids.h')

KINDS = ('namespace', 'relation', 'domain', 'collation', 'function')
INITDB_KINDS = ('namespace', 'relation', 'domain')
RETIRED = 'retired'

HEADER = ('# Kept by scripts/generate_pg_catalog.py, see CONTRIBUTING.md. '
          'Do not edit.\n')


def system_oids():
    with open(OIDS) as source:
        text = source.read()
    base = int(re.search(r'kMinSystem = (\d+);', text).group(1))
    return {name: base + int(offset.replace("'", '')) for name, offset in
            re.findall(r'(k\w+) = kMinSystem \+ ([\d\']+);', text)}


class Lock:
    def __init__(self):
        self.entries = {}
        self.blocks = system_oids()
        if not os.path.exists(PATH):
            return
        with open(PATH) as lock:
            for line in lock:
                if line.startswith('#'):
                    continue
                kind, oid, identity, *state = line.rstrip('\n').split('\t')
                self.entries[kind, identity] = [int(oid), state == [RETIRED]]

    def oids(self, kinds):
        return [entry[0] for (kind, _), entry in self.entries.items()
                if kind in kinds]

    def assign(self, kind, identity, allocate):
        if (kind, identity) not in self.entries:
            self.entries[kind, identity] = [allocate(), False]
        return self.entries[kind, identity][0]

    def retire(self, kind, live):
        for (entry_kind, identity), entry in self.entries.items():
            if entry_kind == kind:
                entry[1] = identity not in live

    def next_initdb(self, first):
        ends = [oid + 2 if kind == 'relation' else oid
                for (kind, _), (oid, _) in self.entries.items()
                if kind in INITDB_KINDS]
        return max(ends, default=first - 1) + 1

    def next_in_block(self, kind, first, limit):
        oids = [oid for oid in self.oids((kind,)) if oid >= first]
        oid = max(oids, default=first - 1) + 1
        if oid >= limit:
            raise SystemExit(f'the {kind} OIDs from {first} are used up')
        return oid

    def merge_builtins(self, path, pg_procs, taken):
        functions, collations = [], []
        with open(path) as dump:
            for line in dump:
                kind, *fields = line.rstrip('\n').split('\t')
                (functions if kind == 'function' else collations).append(
                    fields)
        keys = [fields[0] for fields in functions]
        if len(keys) != len(set(keys)):
            raise SystemExit(f'{path} names a builtin function twice')
        pg = {(row['pronamespace'], row['proname'], row['prokind'],
               row['proargtypes']): int(row['oid']) for row in pg_procs}
        used = set(self.oids(('function',))) | set(taken)
        first = self.blocks['kFirstBuiltinFunction']
        limit = self.blocks['kFirstUnfrozenFunction']

        def function_oid(nsp, name, prokind, argtypes, exact):
            match = pg.get((nsp, name, prokind, argtypes))
            if exact == 't' and match and match not in used:
                used.add(match)
                return match
            return self.next_in_block('function', first, limit)

        for key, *signature in functions:
            self.assign('function', key, lambda: function_oid(*signature))
        self.retire('function', set(keys))
        first = self.blocks['kFirstCollation']
        limit = self.blocks['kFirstUnfrozenCollation']
        names = [fields[0] for fields in collations]
        for name in names:
            self.assign('collation', name, lambda: self.next_in_block(
                'collation', first, limit))
        self.retire('collation', set(names))

    def check_taken(self, taken):
        clash = set(self.oids(('function',))) & set(taken)
        if clash:
            raise SystemExit(f'{os.path.relpath(PATH, REPO_ROOT)} gives '
                             f'builtin functions the OIDs of PostgreSQL '
                             f'procedures SereneDB lists: {sorted(clash)}')

    def text(self):
        rows = sorted((KINDS.index(kind), oid, identity, retired)
                      for (kind, identity), (oid, retired)
                      in self.entries.items())
        return HEADER + ''.join(
            f'{KINDS[kind]}\t{oid}\t{identity}' +
            (f'\t{RETIRED}' if retired else '') + '\n'
            for kind, oid, identity, retired in rows)

    def table(self, kind):
        source = os.path.relpath(PATH, REPO_ROOT)
        return generated(source, [
            f'{{{cpp_str(identity)}, {oid}}},' for identity, oid in sorted(
                (identity, oid) for (entry_kind, identity), (oid, retired)
                in self.entries.items()
                if entry_kind == kind and not retired)])

    def outputs(self):
        return {
            PATH: self.text(),
            os.path.join(GENERATED_DIR, 'builtin_function_oids.gen.inc'):
                self.table('function'),
            os.path.join(GENERATED_DIR, 'builtin_collation_oids.gen.inc'):
                self.table('collation'),
        }
