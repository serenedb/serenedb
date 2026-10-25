add_library(
    search-benchmark-common
    OBJECT
    executor.cpp
    index_builder.cpp
    line_source.cpp
)
target_link_libraries(search-benchmark-common PUBLIC iresearch simdjson)
target_include_directories(
    search-benchmark-common
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_SOURCE_DIR}/tests/iresearch
)

add_executable(search-benchmark-game-build build_index.cpp)
target_link_libraries(
    search-benchmark-game-build
    PRIVATE search-benchmark-common
)

add_executable(search-benchmark-game-query do_query.cpp)
target_link_libraries(
    search-benchmark-game-query
    PRIVATE search-benchmark-common
)

add_custom_target(search-benchmark-game)
add_dependencies(
    search-benchmark-game
    search-benchmark-game-build
    search-benchmark-game-query
)
