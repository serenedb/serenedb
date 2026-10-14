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
objects SereneDB names, which functions are stubs), in server/pg/types.cpp
(which PostgreSQL types SereneDB supports), in
server/pg/catalog/tables/<schema>/<table>.cpp (which views are tables) and in
server/pg/catalog/views/<schema>/<view>.cpp (view bodies SereneDB writes).
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
import sys

from pg_catalog import builtins, claims, functions, relations, source, views
from pg_catalog.common import REPO_ROOT, Generator, write


def outputs():
    out = {relations.REGISTRY: relations.registry(),
           str(claims.TEST): claims.output()}
    if source.available():
        gen = Generator()
        constants, types = builtins.generate(gen)
        relations.generate(gen, constants, types)
        views.generate(gen)
        functions.generate(gen, constants, types)
        out.update(gen.outputs)
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--check', action='store_true',
                        help='fail if an output is stale')
    args = parser.parse_args()
    if not args.check and not source.available():
        parser.error('third_party/postgres has no catalog sources; run '
                     'git submodule update --init third_party/postgres')
    texts = outputs()
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
