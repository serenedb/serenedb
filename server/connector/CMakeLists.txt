add_library(
    serene_connector
    STATIC
    common.cpp
    data_sink.cpp
    primary_key.cpp
    data_source.cpp
    serenedb_connector.cpp
    key_utils.cpp
    rocksdb_sink_writer.cpp
    search_sink_writer.cpp
    search_remove_filter.cpp
    file_table.cpp
    rocksdb_column_decoder.cpp
    rocksdb_materializer.cpp
    parquet_materializer.cpp
    text_materializer.cpp
    search_count_data_source.cpp
    search_scan_data_source.cpp
    search_filter_builder.cpp
    search_filter_printer.cpp
    sst_sink_writer.cpp
    rocksdb_filter.cpp
    duckdb_storage_extension.cpp
    duckdb_client_state.cpp
    duckdb_catalog.cpp
    duckdb_entry_cache.cpp
    duckdb_schema_entry.cpp
    duckdb_table_entry.cpp
    duckdb_table_function.cpp
    duckdb_rocksdb_reader.cpp
    duckdb_rocksdb_writer.cpp
    duckdb_copy_filesystem.cpp
    duckdb_physical_insert.cpp
    duckdb_physical_sst_insert.cpp
    duckdb_physical_create_index.cpp
    duckdb_index_utils.cpp
    duckdb_search_sink_writer.cpp
    duckdb_search_functions.cpp
    duckdb_physical_delete.cpp
    duckdb_physical_update.cpp
    duckdb_transaction.cpp
)

target_link_libraries(
    serene_connector
    serene_base
    iresearch-static
    rocksdb
    velox
    pg_query
    duckdb_static
)

# TODO(Dronplane): why not make public includes for sereneserver so we avoid this ugly line below?
# we need rocksdb utils and logical collection but sereneserver is a single monolithic target without proper include dirs setup
target_include_directories(serene_connector PUBLIC "${CMAKE_SOURCE_DIR}/server")
