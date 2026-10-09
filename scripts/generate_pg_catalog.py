#!/usr/bin/env python3
"""Generate SereneDB's PostgreSQL catalog code from PostgreSQL 18.

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
rewrites). Every output is server/pg/catalog/**/*.gen.inc.
"""

import argparse

from pg_catalog import builtins, functions, relations, views
from pg_catalog.common import Generator


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--pg', required=True, help='host:port')
    parser.add_argument('--pg-src', required=True,
                        help='PostgreSQL source tree of the same version')
    args = parser.parse_args()
    gen = Generator(args.pg, args.pg_src)
    builtins.generate(gen)
    relations.generate(gen)
    views.generate(gen)
    functions.generate(gen)


if __name__ == '__main__':
    main()
