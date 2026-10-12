add_executable(
    serenedb-tests
    # Custom main (NOT gtest_main): brings up DuckDBEngine before any
    # SDB_* macro fires -- see tests_main.cpp comment.
    tests_main.cpp
    assertions_tests.cpp
    background_scheduler_test.cpp
    database_directory_test.cpp
    docs_search_test.cpp
    duckdb_detach_test.cpp
    duckdb_roundtrip_test.cpp
    duckdb_threads_test.cpp
    json_serde_test.cpp
    json_serde_types_test.cpp
    meta_transaction_snapshot_test.cpp
    metrics_test.cpp
    network_duck_executor_test.cpp
    network_duck_pump_test.cpp
    network_h1_codec_test.cpp
    network_header_test.cpp
    network_http_compression_test.cpp
    network_http_test.cpp
    network_listen_spec_test.cpp
    network_pg_bind_decoder_test.cpp
    network_pg_codec_test.cpp
    network_pg_frame_reader_test.cpp
    network_pg_frames_test.cpp
    network_pg_hba_test.cpp
    network_pg_implicit_txn_test.cpp
    network_pg_protocol_state_test.cpp
    network_pg_scram_test.cpp
    network_pg_startup_test.cpp
    network_proxy_protocol_test.cpp
    network_router_test.cpp
    number_of_cores_test.cpp
    pointer_union_test.cpp
    random_test.cpp
    scorer_bounds_test.cpp
    serializer_binary_test.cpp
    serializer_test.cpp
    storage_version_test.cpp
    catalog/entry/tokenizer_test.cpp
    connector/ai_functions_test.cpp
    connector/copy_in_bridge_test.cpp
    connector/duckdb_search_filter_builder_test.cpp
    connector/duckdb_search_sink_writer_test.cpp
    connector/index_lifecycle_test.cpp
    connector/pg_progress_registry_test.cpp
    connector/pg_serialize_array_test.cpp
    connector/search_db_wal_test.cpp
    connector/ts_query_codec_test.cpp
    connector/writer_generations_test.cpp
)

target_link_libraries(
    serenedb-tests
    PUBLIC sereneserver gtest serene_docs_corpus
)
sdb_link_duckdb_extensions(serenedb-tests PUBLIC)

if(SDB_EMBEDDED_DOCS)
    target_link_libraries(serenedb-tests PUBLIC serene_docs_index)
    add_dependencies(serenedb-tests serened)
    sdb_embed_docs_index(serenedb-tests)
endif()

target_compile_definitions(
    serenedb-tests
    PRIVATE "SDB_RESOURCE_DIR=\"${CMAKE_SOURCE_DIR}/resources\""
)
