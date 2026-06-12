add_library(serene_tests_connector_deps INTERFACE)

target_link_libraries(
    serene_tests_connector_deps
    INTERFACE sereneserver serene_connector rocksdb
)

add_executable(
    serenedb-tests_connector
    # Custom main (NOT gtest_main): brings up DuckDBEngine before any
    # SDB_* macro fires -- see tests_main.cpp comment.
    ${CMAKE_CURRENT_SOURCE_DIR}/tests_main.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/duckdb_vector_builder.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/duckdb_column_serializer_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/index_lifecycle_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/duckdb_search_filter_builder_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/duckdb_search_sink_writer_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_progress_tracker_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/pg_serialize_array_test.cpp
    # ${CMAKE_CURRENT_SOURCE_DIR}/data_sink_test.cpp
    # ${CMAKE_CURRENT_SOURCE_DIR}/data_source_test.cpp
    # ${CMAKE_CURRENT_SOURCE_DIR}/primary_key_test.cpp
    # ${CMAKE_CURRENT_SOURCE_DIR}/key_utils_test.cpp
    # ${CMAKE_CURRENT_SOURCE_DIR}/search_sink_writer_test.cpp
    # ${CMAKE_CURRENT_SOURCE_DIR}/sink_with_search_test.cpp
    # ${CMAKE_CURRENT_SOURCE_DIR}/search_source_test.cpp
    # ${CMAKE_CURRENT_SOURCE_DIR}/search_filter_builder_test.cpp
    # ${CMAKE_CURRENT_SOURCE_DIR}/search_test_utils.cpp
)

target_link_libraries(
    serenedb-tests_connector
    PUBLIC serene_tests_connector_deps gtest
)
