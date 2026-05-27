add_library(serene_tests_connector_deps INTERFACE)

target_link_libraries(
    serene_tests_connector_deps
    INTERFACE sereneserver serene_connector rocksdb
)

add_executable(
    serenedb-tests_connector
    ${CMAKE_CURRENT_SOURCE_DIR}/duckdb_vector_builder.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/duckdb_column_serializer_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/duckdb_key_builder_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/duckdb_search_filter_builder_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/duckdb_search_sink_writer_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/search_table_marker_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/search_table_sink_writer_test.cpp
)

target_link_libraries(
    serenedb-tests_connector
    PUBLIC serene_tests_connector_deps gtest gtest_main
)
