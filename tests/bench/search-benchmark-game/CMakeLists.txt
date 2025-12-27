add_executable(search-benchmark-game-build build_index.cpp)
target_link_libraries(search-benchmark-game-build
  sereneserver
)

find_package(FLEX)
find_package(BISON)

set_directory_properties(PROPERTIES CLEAN_NO_CUSTOM "On")

add_custom_command(
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/tokens.cpp
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMAND ${FLEX_EXECUTABLE} -L -otokens.cpp tokens.ll
    MAIN_DEPENDENCY ${CMAKE_CURRENT_SOURCE_DIR}/tokens.ll
    VERBATIM
)

add_custom_command(
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/grammar.cpp
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMAND
        ${CMAKE_SOURCE_DIR}/scripts/bison-c.sh ${BISON_EXECUTABLE}
        grammar.cpp grammar.y
    MAIN_DEPENDENCY ${CMAKE_CURRENT_SOURCE_DIR}/grammar.y
    VERBATIM
)

add_executable(search-benchmark-game-query 
  do_query.cpp 
  grammar.cpp 
  tokens.cpp)

target_link_libraries(search-benchmark-game-query
  sereneserver 
)

add_custom_target(search-benchmark-game)
add_dependencies(search-benchmark-game
  search-benchmark-game-build
  search-benchmark-game-query)
