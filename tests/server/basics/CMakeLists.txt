add_library(serene_tests_basics_deps INTERFACE)

target_link_libraries(serene_tests_basics_deps INTERFACE sereneserver)

add_executable(
    serenedb-tests_basics
    conversions_test.cpp
    escaper_test.cpp
    key_test.cpp
    endian_test.cpp
    connection_cache_test.cpp
    connection_pool_test.cpp
    inifile_parser_test.cpp
    parameters_test.cpp
    http_response_checker_test.cpp
    assertions_tests.cpp
    http_request_test.cpp
    csv_test.cpp
    error_test.cpp
    build_id_test.cpp
    datetime.cpp
    files_test.cpp
    fp_conv_test.cpp
    future_shared_lock_test.cpp
    hashes_test.cpp
    hybrid_logical_clock_test.cpp
    size_limited_string_test.cpp
    string_utf8_normalize_test.cpp
    string_utf8_test.cpp
    attribute_name_parser_test.cpp
    cpu_usage_snapshot_test.cpp
    encoding_utils_test.cpp
    endpoint_test.cpp
    logger_test.cpp
    memory_usage_test.cpp
    number_utils_test.cpp
    random_test.cpp
    read_write_lock_test.cpp
    recursive_locker_test.cpp
    resource_usage_allocator_test.cpp
    spin_lock_test.cpp
    string_utils_test.cpp
    serializer_test.cpp
    unshackled_mutex_test.cpp
    vpack_helper_test.cpp
    inspection_test.cpp
    vpack_load_test.cpp
    vpack_save_test.cpp
)

target_link_libraries(
    serenedb-tests_basics
    PUBLIC serene_tests_connector_deps gtest gtest_main
)
