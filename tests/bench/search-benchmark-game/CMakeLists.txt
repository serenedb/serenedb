add_compile_options(-Wno-error)

add_library(search-benchmark-common OBJECT executor.cpp index_builder.cpp)
target_link_libraries(search-benchmark-common iresearch-static simdjson)
target_include_directories(
    search-benchmark-common
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
    PUBLIC ${CMAKE_SOURCE_DIR}/tests/iresearch
)

add_bench(build_index search-benchmark-game-build)
add_bench(do_query search-benchmark-game-query)

add_custom_target(search-benchmark-game)
add_dependencies(search-benchmark-game serenedb-bench-micro)
