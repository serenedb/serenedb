add_compile_options(-Wno-error)

add_library(search-benchmark-common OBJECT executor.cpp index_builder.cpp)
target_link_libraries(search-benchmark-common iresearch-static simdjson)
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
