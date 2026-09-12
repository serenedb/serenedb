add_compile_warnings_flags()

add_subdirectory(bench)

if(SDB_GTEST)
    add_subdirectory(iresearch)
    add_subdirectory(server)
endif()
