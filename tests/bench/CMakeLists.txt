add_subdirectory(micro)
add_subdirectory(search-benchmark-game)

add_executable(serenedb-bench-vpack vpack.cpp)
target_link_libraries(serenedb-bench-vpack PRIVATE vpack simdjson rapidjson)
