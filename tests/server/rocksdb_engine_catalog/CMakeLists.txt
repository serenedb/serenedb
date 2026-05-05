add_executable(
    serenedb-tests_rocksdb_engine_catalog
    ${CMAKE_CURRENT_SOURCE_DIR}/rocksdb_sequence_manager_test.cpp
)

target_link_libraries(
    serenedb-tests_rocksdb_engine_catalog
    PUBLIC sereneserver rocksdb gtest gtest_main
)

add_test(
    NAME serenedb-tests_rocksdb_engine_catalog
    COMMAND serenedb-tests_rocksdb_engine_catalog
)
