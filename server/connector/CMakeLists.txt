add_library(
    serene_connector
    STATIC
    key_utils.cpp
    search_sink_writer.cpp
    search_remove_filter.cpp
    duckdb_storage_extension.cpp
    duckdb_client_state.cpp
    duckdb_catalog.cpp
    duckdb_entry_cache.cpp
    duckdb_schema_entry.cpp
    duckdb_system_table_entry.cpp
    duckdb_table_entry.cpp
    duckdb_index_entry.cpp
    duckdb_table_function.cpp
    duckdb_rocksdb_reader.cpp
    duckdb_rocksdb_writer.cpp
    duckdb_copy_filesystem.cpp
    duckdb_physical_insert.cpp
    duckdb_physical_sst_insert.cpp
    duckdb_physical_ctas.cpp
    duckdb_tokenizer_function.cpp
    duckdb_physical_create_index.cpp
    duckdb_index_utils.cpp
    duckdb_constraint_verify.cpp
    duckdb_search_sink_writer.cpp
    duckdb_vacuum_function.cpp
    duckdb_physical_delete.cpp
    duckdb_physical_update.cpp
    duckdb_physical_search.cpp
    duckdb_transaction.cpp
)

target_link_libraries(
    serene_connector
    serene_base
    iresearch-static
    rocksdb
    pg_query
    duckdb_static
    Folly::folly
)

# TODO(Dronplane): why not make public includes for sereneserver so we avoid this ugly line below?
# we need rocksdb utils and logical collection but sereneserver is a single monolithic target without proper include dirs setup
target_include_directories(serene_connector PUBLIC "${CMAKE_SOURCE_DIR}/server")

add_subdirectory(functions)
add_subdirectory(optimizer)
