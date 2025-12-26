add_executable(search-benchmark-game-build build_index.cpp)
target_link_libraries(search-benchmark-game-build
  sereneserver
)

add_executable(search-benchmark-game-query do_query.cpp)
target_link_libraries(search-benchmark-game-query
  sereneserver 
)
