import sys
from pathlib import Path

from sqllogic_gen import HERE, Test

sys.path.insert(0, str(Path(HERE).parents[1] / "scripts"))
from pg_catalog import claims  # noqa: E402

t = Test("Claims made by docs/compatibility/data-type-compatibility.md: every listed type "
         "is in pg_type; a supported one can be named in a cast, and an unsupported one "
         "cannot.")
for name, mark in claims.type_rows():
    t.query(f"SELECT count(*) AS listed FROM pg_catalog.pg_type WHERE typname = '{name}' "
            "AND typnamespace = 'pg_catalog'::regnamespace", ["listed"], [[1]])
    cast = f'SELECT NULL::"{name}" IS NULL'
    if mark == "🔴":
        t.error(cast, f"db error: ERROR: Type with name {name} does not exist")
    else:
        t.statement(cast)
t.write()
