add_library(
    serene_connector
    STATIC
    key_encoding.cpp
    key_utils.cpp
    offsets_collector.cpp
    search_sink_writer.cpp
    search_remove_filter.cpp
    duckdb_storage_extension.cpp
    rocksdb_filter.cpp
    index_expression.cpp
    search_filter_builder.cpp
    geo_filter_builder.cpp
    search_filter_printer.cpp
    duckdb_ann_filter.cpp
    duckdb_client_state.cpp
    duckdb_catalog.cpp
    duckdb_entry_cache.cpp
    duckdb_schema_entry.cpp
    duckdb_system_table_entry.cpp
    duckdb_table_entry.cpp
    duckdb_index_entry.cpp
    duckdb_index_scan_entry.cpp
    duckdb_table_function.cpp
    duckdb_scan_base.cpp
    duckdb_pk_full_scan.cpp
    duckdb_pk_range_scan.cpp
    duckdb_pk_point_lookup.cpp
    duckdb_sk_full_scan.cpp
    duckdb_sk_point_lookup.cpp
    duckdb_sk_range_scan.cpp
    duckdb_search_full_scan.cpp
    duckdb_search_ann_scan.cpp
    duckdb_rocksdb_reader.cpp
    duckdb_rocksdb_writer.cpp
    columnstore_materializer.cpp
    index_source_factory.cpp
    index_source_rocksdb.cpp
    index_source_view.cpp
    inverted_store_index.cpp
    index_source_view_file.cpp
    index_source_view_rocksdb.cpp
    index_source_view_table.cpp
    view_fast_path.cpp
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
    duckdb_truncate_function.cpp
    duckdb_physical_truncate.cpp
    duckdb_physical_delete.cpp
    duckdb_physical_update.cpp
    duckdb_transaction.cpp
    highlight/highlight_options.cpp
    highlight/memory_index.cpp
)

target_link_libraries(
    serene_connector
    serene_base
    iresearch-static
    rocksdb
    duckdb_static
    simdjson
    Folly::folly
)

# TODO(Dronplane): why not make public includes for sereneserver so we avoid this ugly line below?
# we need rocksdb utils and logical collection but sereneserver is a single monolithic target without proper include dirs setup
target_include_directories(serene_connector PUBLIC "${CMAKE_SOURCE_DIR}/server")

# view_fast_path.cpp reads iceberg-bound snapshot ids; iceberg's REST
# headers include yyjson unprefixed.
target_include_directories(
    serene_connector
    PRIVATE
        "${CMAKE_SOURCE_DIR}/third_party/duckdb_iceberg/src/include"
        "${CMAKE_SOURCE_DIR}/third_party/duckdb/third_party/yyjson/include"
)

add_subdirectory(functions)
add_subdirectory(optimizer)
