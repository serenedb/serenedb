set(VELOX_DWIO_COMMON_TEST_UTILS_DIR
    "${CMAKE_SOURCE_DIR}/third_party/velox/velox/dwio/common/tests/utils")

add_executable(
    serenedb-tests_velox_text_reader
    text_reader_test.cpp
    ${VELOX_DWIO_COMMON_TEST_UTILS_DIR}/DataFiles.cpp
)

add_test(
    NAME serenedb-tests_velox_text_reader
    COMMAND serenedb-tests_velox_text_reader
    WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}/tests/libs/velox/text/reader"
)

target_include_directories(
    serenedb-tests_velox_text_reader
    PRIVATE ${VELOX_DWIO_COMMON_TEST_UTILS_DIR}
)

target_link_libraries(
    serenedb-tests_velox_text_reader
    velox_dwio_text_reader_register
    velox_vector_test_lib
    GTest::gtest
    GTest::gtest_main
    GTest::gmock
    gflags::gflags
    glog::glog
)