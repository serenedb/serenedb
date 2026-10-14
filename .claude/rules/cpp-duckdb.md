---
paths:
  - "third_party/duckdb/**"
  - "third_party/duckdb_*/**"
  - "third_party/database-connector/**"
  - "third_party/avro/**"
---

# The DuckDB family

`third_party/duckdb`, the `duckdb_*` extensions, `database-connector`, and `avro` (Apache's avro-c with DuckDB's changes, so it has both upstreams), patched heavily. Write it like our code ([cpp-serenedb.md](cpp-serenedb.md)), using DuckDB's own types where its APIs take them, and know what our fork changed:

- `unordered_map`/`unordered_set` are `absl::node_hash_map`/`node_hash_set`, and so is everything built on them (`case_insensitive_map_t`, ...): look up with the `string_view` you have, never a temporary string.
- `duckdb::mutex` is `absl::Mutex`: wait with `Await`/`LockWhen`, never a condition variable or a yield loop.
- A race only TSan can see uses `StoreRacy`/`LoadRacy`/`FillRacy` (`duckdb/common/atomic.hpp`).
- Formatting and generated files go only through `scripts/duckdb_family.sh` in the directories it covers (`--help` lists them and its modes): never `format.py`, a generator or clang-format by hand.

## Code

- Reuse DuckDB's code paths. Where we write our own, it mirrors DuckDB's design unless a divergence was agreed, and is never slower, more allocating or more copying than the native path.
- A gap in something DuckDB implements is fixed by patching DuckDB's code, and our parallel version is deleted.
- Vectors: one `Vector` per batch holds data and validity; a vector carries its size (no separate `count`; a writer sets child cardinality); a vector reused across calls is reset first; a table function emits at most `STANDARD_VECTOR_SIZE` rows per call, straight into its output; never `Flatten` or per-cell `GetValue` on hot paths, read through `UnifiedVectorFormat`; JSON is `VARCHAR` with the `JSON` alias, so dispatch on the full type.
- Internal bulk reads consume vectors inside the pipeline (a table function), never materialize and `Fetch`.
- DuckDB-level targets (`unittest`, extensions) build on upstream's own dependencies: cut a serenedb-only edge instead of making serenedb work their prerequisite.

## Changing it

The fork rules are in [pull-requests.md](pull-requests.md); on top of them:

- In duckdb, a PR whose changes the generators see ends with its own `regen:` commit (`scripts/duckdb_family.sh regen`; `regen --check` proves it current). `format --check --range <base>..HEAD <dir>` checks every commit on its own.
- A bug in upstream code: reproduce it on upstream `main` and search upstream first; cherry-pick upstream's fix with `-x` when there is one, otherwise tell the user.
- Updating DuckDB: [tests/duckdb/README.md](../../tests/duckdb/README.md).
