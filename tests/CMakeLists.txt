add_library(
    iresearch-test-writer
    OBJECT
    EXCLUDE_FROM_ALL
    iresearch/index/test_index_writer.cpp
)
target_include_directories(
    iresearch-test-writer
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/iresearch
)
target_link_libraries(iresearch-test-writer PUBLIC iresearch)

add_subdirectory(bench EXCLUDE_FROM_ALL)

if(SDB_GTEST)
    add_subdirectory(iresearch)
    add_subdirectory(server)
endif()
