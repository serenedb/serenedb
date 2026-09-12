add_executable(
    serenedb-tests_connector
    # Custom main (NOT gtest_main): brings up DuckDBEngine before any
    # SDB_* macro fires -- see tests_main.cpp comment.
    ${CMAKE_CURRENT_SOURCE_DIR}/tests_main.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/index_lifecycle_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/duckdb_search_filter_builder_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/ts_query_codec_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/duckdb_search_sink_writer_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/search_db_wal_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_progress_registry_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_serialize_array_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/copy_in_bridge_test.cpp
)

target_link_libraries(
    serenedb-tests_connector
    PUBLIC sereneserver serene_connector gtest
)
