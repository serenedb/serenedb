#!/usr/bin/env python3
"""Generate SereneDB's system-table code and tests.

Everything SereneDB copies from PostgreSQL is generated here, never edited by
hand, from the PostgreSQL source in third_party/postgres:

  src/include/catalog/*.h, *.dat   the bootstrap catalogs, which genbki.pl
                                   turns into postgres.bki: catalog rows,
                                   table shapes, index keys
  src/backend/catalog/*.sql        what initdb creates after bootstrap:
                                   views, information_schema, privileges
  src/bin/initdb/initdb.c          the privileges initdb sets itself
  src/include/parser/kwlist.h      keywords

PostgreSQL numbers the objects initdb creates while it runs, so the source
holds no OIDs for them; scripts/pg_catalog/initdb.py numbers them in the
same order.

SereneDB's own choices live in scripts/pg_catalog/config.py (which PostgreSQL
objects SereneDB names), in server/pg/types.cpp (which PostgreSQL types
SereneDB supports), in server/pg/catalog/tables/<schema>/<table>.cpp (which
views are tables) and in server/pg/catalog/views/<schema>/<view>.cpp (view
bodies SereneDB writes).
Those outputs are server/pg/catalog/generated/*.gen.inc.

Two outputs come from this repository alone: generated/registry.gen.inc,
which registers each server/pg/catalog/tables/<folder>/<table>.cpp and
server/pg/catalog/views/<folder>/<view>.cpp, and the sqllogic test that
checks the claims of docs/compatibility/system-table-compatibility.md.
--check verifies every output is current without writing anything; without
the third_party/postgres submodule it checks those two only.
"""

import argparse
import os
import subprocess
import sys
import tempfile

from pg_catalog import builtins, claims, functions, relations, source, views
from pg_catalog.common import REPO_ROOT, Generator, write
from pg_catalog.lock import Lock


def dump_builtins(serened, directory):
    path = os.path.join(directory, 'builtins.tsv')
    run = subprocess.run([serened, os.path.join(directory, 'data'),
                          f'--dump_builtin_oids={path}'],
                         capture_output=True, text=True)
    if run.returncode != 0:
        raise SystemExit(f'{serened} --dump_builtin_oids failed:\n'
                         f'{run.stdout}{run.stderr}')
    return path


def outputs(serened):
    lock = Lock()
    out = {relations.REGISTRY: relations.registry(),
           str(claims.TEST): claims.output()}
    if source.available():
        gen = Generator(lock)
        constants, types, procs = builtins.generate(gen)
        if serened:
            with tempfile.TemporaryDirectory() as directory:
                lock.merge_builtins(dump_builtins(serened, directory),
                                    gen.rows['pg_proc'], procs)
        lock.check_taken(procs)
        relations.generate(gen, constants, types)
        functions.generate(gen, constants, types, views.generate(gen))
        out.update(gen.outputs)
    out.update(lock.outputs())
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--check', action='store_true',
                        help='fail if an output is stale')
    parser.add_argument('--serened', metavar='BINARY',
                        help='freeze the OIDs of the builtin functions and '
                             'collations this serened has')
    args = parser.parse_args()
    if not args.check and not source.available():
        parser.error('third_party/postgres has no catalog sources; run '
                     'git submodule update --init third_party/postgres')
    if args.check and args.serened:
        parser.error('--check reads no serened')
    texts = outputs(args.serened)
    if args.check:
        stale = [path for path, text in texts.items()
                 if not os.path.exists(path) or open(path).read() != text]
        for path in stale:
            print(f'{os.path.relpath(path, REPO_ROOT)} is stale; rerun '
                  f'scripts/generate_pg_catalog.py', file=sys.stderr)
        return 1 if stale else 0
    for path, text in texts.items():
        write(path, text)
    return 0


if __name__ == '__main__':
    sys.exit(main())
