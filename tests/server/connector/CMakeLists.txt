add_library(serene_tests_connector_deps INTERFACE)

target_link_libraries(
    serene_tests_connector_deps
    INTERFACE sereneserver serene_connector velox_vector_test_lib rocksdb
)

add_executable(
    serenedb-tests_connector
    ${CMAKE_CURRENT_SOURCE_DIR}/data_sink_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/data_source_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/primary_key_test.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/key_utils_test.cpp
)

target_link_libraries(
    serenedb-tests_connector
    PUBLIC serene_tests_connector_deps gtest gtest_main
)
