if(IRESEARCH_LIBRARY)
    add_subdirectory(bench/search-benchmark-game EXCLUDE_FROM_ALL)
    add_subdirectory(iresearch)
    return()
endif()

add_subdirectory(bench EXCLUDE_FROM_ALL)

if(SDB_GTEST)
    add_subdirectory(iresearch)
    add_subdirectory(server)
endif()
