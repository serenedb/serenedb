add_library(
    sereneserver_catalog1
    STATIC
    catalog.cpp
    lookup.cpp
    cluster.cpp
    entry/tokenizer.cpp
    entry/role.cpp
    entry/foreign_server.cpp
    entry/database.cpp
)

target_link_libraries(
    sereneserver_catalog1
    PUBLIC boost_boost iresearch-static serene duckdb_static
)

target_include_directories(
    sereneserver_catalog1
    PUBLIC ${CMAKE_SOURCE_DIR}/server
)
