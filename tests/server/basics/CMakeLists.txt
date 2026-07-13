add_library(serene_tests_basics_deps INTERFACE)

target_link_libraries(serene_tests_basics_deps INTERFACE sereneserver)

add_executable(
    serenedb-tests_basics
    # Custom main (NOT gtest_main): brings up DuckDBEngine before any
    # SDB_* macro fires -- see tests_main.cpp comment.
    tests_main.cpp
    assertions_tests.cpp
    build_id_test.cpp
    metrics_test.cpp
    number_of_cores_test.cpp
    random_test.cpp
    string_utils_test.cpp
    serializer_test.cpp
    serializer_binary_test.cpp
    json_serde_test.cpp
    json_serde_types_test.cpp
    duckdb_roundtrip_test.cpp
    catalog_persistence_test.cpp
    network_http_test.cpp
    network_router_test.cpp
    network_listen_spec_test.cpp
    network_proxy_protocol_test.cpp
    network_h1_codec_test.cpp
    network_header_test.cpp
    network_pg_codec_test.cpp
    network_pg_frame_reader_test.cpp
    network_pg_startup_test.cpp
    network_pg_bind_decoder_test.cpp
    network_pg_scram_test.cpp
    network_pg_implicit_txn_test.cpp
    network_pg_protocol_state_test.cpp
    network_pg_hba_test.cpp
    network_duck_executor_test.cpp
    network_duck_pump_test.cpp
    network_pg_frames_test.cpp
)

target_link_libraries(
    serenedb-tests_basics
    PUBLIC serene_tests_connector_deps gtest
)

target_compile_definitions(
    serenedb-tests_basics
    PRIVATE "SDB_RESOURCE_DIR=\"${CMAKE_SOURCE_DIR}/resources\""
)
