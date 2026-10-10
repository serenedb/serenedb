# When you change ...

Each of these changes has a follow-up step that nothing runs for you.

- **The DuckDB fork's grammar** (`third_party/duckdb/src/parser/peg/grammar/`) or a source of its generated code (settings, serialization, enum_util, functions, metrics, storage info): `scripts/duckdb_family.sh regen` runs DuckDB's generators in DuckDB's order and builds the `regen:` commit; `regen --check` proves it is current. Generated files are never edited by hand.
- **A fork under `third_party/`:** the change is a PR in that fork against its current `vYYYY.MM.DD` branch. The hand-written change goes in its own commits, each formatted on its own (`scripts/duckdb_family.sh format` for the DuckDB family); in duckdb the pull request ends with its own `regen:` commit. After the fork PR merges, serenedb moves the gitlink in a commit of its own: pre-commit `check-submodule-pointers` rejects a gitlink that no version branch contains. Submodules are cloned shallow (see [Working with Submodules](third-party.md#working-with-submodules)), and a cmake reconfigure checks the gitlink out over a clean submodule: configure with `-DAUTO_UPDATE_MODULES=OFF` while one is on a work branch.
- **Anything written to disk:** [Storage compatibility](storage.md); for DuckDB files also "Changing the DuckDB on-disk format" in [tests/duckdb/README.md](../../tests/duckdb/README.md).
- **A new C++ file:** add it to `target_sources` in its directory's `CMakeLists.txt`. `scripts/find_unused_sources.py` lists the files nothing compiles.
- **A setting:** it is defined in `server/query/config_variables.cpp` and documented in the table in `docs/configuration/overview.md`.
- **A serened command-line flag:** `python3 tests/drivers/python/cli_help.py override --bin <build dir>/bin/serened` rewrites the reference that `docs/configuration/cli.mdx` renders; the python driver suite fails until it matches.
- **The OpenTelemetry schema** (`resources/otel/otel_schema.sql`): `scripts/otel/schema.py generate`. Its conformance fixtures (`resources/otel/conformance/*.json`): `scripts/otel/fixtures.py generate`.
- **What pg_catalog and information_schema support:** update `docs/compatibility/system-table-compatibility.md`, then `python3 scripts/generate_system_table_claims.py` regenerates the test that pins it.
- **A python driver test file:** add it to the list in `tests/drivers/python/run.sh`, or no suite runs it.
- **Other generated sources:** the word-break and case tables in `iresearch/` come from `scripts/generate_unicode_tables.py`, `third_party/libstemmer_c` from `scripts/update_libstemmer.sh`, and the PostgreSQL views and functions in `server/pg/system_views.h` and `server/pg/system_functions.h` from `scripts/update_system_catalog.py`.
