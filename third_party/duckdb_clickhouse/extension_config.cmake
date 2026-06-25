# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(clickhouse_scanner
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    DONT_LINK
    LOAD_TESTS
)
