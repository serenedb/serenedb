# Port fixes

Every query under `ports/` differs from its corpus copy only by the fixes listed here.
`run_corpus.py` runs the port when one exists and the corpus copy otherwise.

| Project | Query | Fix | Why |
|---|---|---|---|
| DBatUTuebingen__flummi | quickhull__output.duckdb.sql | `max` gathers with `arg_max` instead of `arg_min` | Upstream `examples/quickhull/input.fl` computes both hull ends with `arg_min`. The first edge is a single point, every distance is 0/0 = NaN, NaN sorts above every number, and the recursion never ends on any engine (DuckDB 1.5.3 included). |
| ClickHouse__RayTracer | bench_duckdb.duckdb.sql | `hashf` reduces modulo 2^31 before multiplying | The DuckDB port evaluates terrN for every row, so hashf overflows BIGINT for missed rays; ClickHouse wraps silently, serenedb throws. |
