add_library(
    serene_query_lite
    STATIC
    duckdb_engine.cpp
    log_types.cpp
)

target_include_directories(
    serene_query_lite
    PUBLIC ${CMAKE_SOURCE_DIR}/libs
)

target_link_libraries(
    serene_query_lite
    PUBLIC
        serene_base
        duckdb_static
)
