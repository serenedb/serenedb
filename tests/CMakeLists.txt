add_library(
    iresearch-policy-compaction
    STATIC
    EXCLUDE_FROM_ALL
    iresearch/index/compaction/policy_compaction.cpp
)
target_include_directories(
    iresearch-policy-compaction
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/iresearch
)
target_link_libraries(iresearch-policy-compaction PUBLIC iresearch)

add_subdirectory(bench EXCLUDE_FROM_ALL)

if(SDB_GTEST)
    add_subdirectory(iresearch)
    add_subdirectory(server)
endif()
