add_library(
    iresearch-merge-support
    STATIC
    EXCLUDE_FROM_ALL
    iresearch/index/merge/support/parallel_merge.cpp
)
target_include_directories(
    iresearch-merge-support
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/iresearch
)
target_link_libraries(iresearch-merge-support PUBLIC iresearch)

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
