#!/usr/bin/env python3
"""Generate SereneDB's system-table code and tests.

Everything SereneDB copies from PostgreSQL is generated here, never edited by
hand. Two inputs, both of the same PostgreSQL version:

  --pg      a running PostgreSQL (trust auth): catalog rows, oids that initdb
            assigns, table shapes, index keys, settings, keywords
  --pg-src  its source tree: view definitions, which a running server only
            returns re-deparsed

  docker run -d --name pg18 -e POSTGRES_HOST_AUTH_METHOD=trust \\
      -p 55433:5432 postgres:18.3
  git clone --depth 1 -b REL_18_3 https://github.com/postgres/postgres pg-src
  python3 scripts/generate_pg_catalog.py --pg 127.0.0.1:55433 --pg-src pg-src

SereneDB's own choices live in scripts/pg_catalog/config.py (which views are
native tables, which settings are exposed) and in
server/pg/catalog/views/overrides/<schema>.<view>.sql (view bodies SereneDB
rewrites). Those outputs are server/pg/catalog/generated/*.gen.inc.

Two outputs come from this repository alone and are written on every run,
also without --pg: generated/registry.gen.inc, which registers each
server/pg/catalog/tables/<folder>/<table>.cpp, and the sqllogic test that
checks the claims of docs/compatibility/system-table-compatibility.md.
--check verifies they are current without writing anything.
"""

import argparse
import os
import sys

from pg_catalog import builtins, claims, functions, relations, views
from pg_catalog.common import REPO_ROOT, Generator, write


def repository_outputs():
    outputs = {relations.REGISTRY: relations.registry()}
    text = claims.output()
    if text is not None:
        outputs[str(claims.TEST)] = text
    return outputs


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--pg', help='host:port')
    parser.add_argument('--pg-src',
                        help='PostgreSQL source tree of the same version')
    parser.add_argument('--check', action='store_true',
                        help='fail if an output that comes from this '
                             'repository alone is stale')
    args = parser.parse_args()
    outputs = repository_outputs()
    if args.check:
        stale = [path for path, text in outputs.items()
                 if not os.path.exists(path) or open(path).read() != text]
        for path in stale:
            print(f'{os.path.relpath(path, REPO_ROOT)} is stale; rerun '
                  f'scripts/generate_pg_catalog.py', file=sys.stderr)
        return 1 if stale else 0
    if bool(args.pg) != bool(args.pg_src):
        parser.error('--pg and --pg-src go together')
    if args.pg:
        gen = Generator(args.pg, args.pg_src)
        builtins.generate(gen)
        relations.generate(gen)
        views.generate(gen)
        functions.generate(gen)
    for path, text in outputs.items():
        write(path, text)
    return 0


if __name__ == '__main__':
    sys.exit(main())
