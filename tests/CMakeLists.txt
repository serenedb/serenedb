add_compile_warnings_flags()

add_subdirectory(bench EXCLUDE_FROM_ALL)

if(SDB_GTEST)
    add_subdirectory(iresearch)
    add_subdirectory(server)
endif()
