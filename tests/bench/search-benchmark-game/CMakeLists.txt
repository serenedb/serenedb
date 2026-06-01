add_compile_options(-Wno-error)

add_library(search-benchmark-common OBJECT executor.cpp index_builder.cpp)
# serene_query_lite (libs/query) gives bench code access to the
# DuckDBEngine singleton -- both build/query binaries Initialize / Shutdown
# it in their respective main(). Replaces the leaking bench::CsDb() static.
target_link_libraries(
    search-benchmark-common
    iresearch-static
    simdjson
    serene_query_lite
)
target_include_directories(
    search-benchmark-common
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
)

add_executable(search-benchmark-game-build build_index.cpp)
target_link_libraries(search-benchmark-game-build search-benchmark-common)

add_executable(search-benchmark-game-query do_query.cpp)
target_link_libraries(search-benchmark-game-query search-benchmark-common)

add_custom_target(search-benchmark-game)
add_dependencies(
    search-benchmark-game
    search-benchmark-game-build
    search-benchmark-game-query
)
