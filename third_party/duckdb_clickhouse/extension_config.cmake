# Included by DuckDB's build system via the DUCKDB_EXTENSION_CONFIGS variable
# (set in third_party/CMakeLists.txt) -- the config lives with the extension
# instead of as a shim inside the duckdb fork. Statically linked, like the
# postgres connector; clickhouse-cpp does not build on MinGW/WASM.
if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(clickhouse_scanner
        SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
        INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/src/include
    )
endif()
