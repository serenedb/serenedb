add_library(serene_tests_basics_deps INTERFACE)

target_link_libraries(serene_tests_basics_deps INTERFACE sereneserver)

add_executable(
    serenedb-tests_basics
    # Custom main (NOT gtest_main): brings up DuckDBEngine before any
    # SDB_* macro fires -- see tests_main.cpp comment.
    tests_main.cpp
    assertions_tests.cpp
    http_request_test.cpp
    build_id_test.cpp
    string_utf8_normalize_test.cpp
    encoding_utils_test.cpp
    random_test.cpp
    string_utils_test.cpp
    serializer_test.cpp
    serializer_binary_test.cpp
    json_serde_test.cpp
    json_serde_types_test.cpp
    vpack_helper_test.cpp
    duckdb_roundtrip_test.cpp
)

target_link_libraries(
    serenedb-tests_basics
    PUBLIC serene_tests_connector_deps gtest
)
