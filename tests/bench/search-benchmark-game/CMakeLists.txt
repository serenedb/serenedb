add_executable(search-benchmark-game-build build_index.cpp)
target_link_libraries(search-benchmark-game-build iresearch-static simdjson)

add_executable(search-benchmark-game-query do_query.cpp)

target_link_libraries(search-benchmark-game-query iresearch-static)

add_custom_target(search-benchmark-game)
add_dependencies(
    search-benchmark-game
    search-benchmark-game-build
    search-benchmark-game-query
)
