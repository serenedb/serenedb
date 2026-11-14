configure_file(build.h.in build.h NEWLINE_STYLE UNIX)

function(sdb_build_flags out)
    string(JOIN " " flags ${ARGN})
    string(GENEX_STRIP "${flags}" flags)
    string(REPLACE "SHELL:" "" flags "${flags}")
    string(REGEX REPLACE " +" " " flags "${flags}")
    string(STRIP "${flags}" flags)
    set(${out} "${flags}" PARENT_SCOPE)
endfunction()

string(TOUPPER "${CMAKE_BUILD_TYPE}" _sdb_build_type)
get_directory_property(
    _sdb_compile_options
    DIRECTORY ${serenedb_SOURCE_DIR}
    COMPILE_OPTIONS
)
get_directory_property(
    _sdb_link_options
    DIRECTORY ${serenedb_SOURCE_DIR}
    LINK_OPTIONS
)
sdb_build_flags(SDB_BUILD_CFLAGS "${CMAKE_C_FLAGS}"
    "${CMAKE_C_FLAGS_${_sdb_build_type}}" ${_sdb_compile_options}
)
sdb_build_flags(SDB_BUILD_CXXFLAGS "${CMAKE_CXX_FLAGS}"
    "${CMAKE_CXX_FLAGS_${_sdb_build_type}}" ${_sdb_compile_options}
)
sdb_build_flags(SDB_BUILD_LDFLAGS "${CMAKE_EXE_LINKER_FLAGS}"
    "${CMAKE_EXE_LINKER_FLAGS_${_sdb_build_type}}" ${_sdb_link_options}
)
set(SDB_BUILD_CC "${CMAKE_C_COMPILER_ID} ${CMAKE_C_COMPILER_VERSION}")
set(SDB_BUILD_CXX "${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
set(SDB_BUILD_SYSCONFDIR "${CMAKE_INSTALL_FULL_SYSCONFDIR}/serenedb")
set(SDB_BUILD_CONFIGURE "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}")
get_cmake_property(_sdb_cache_variables CACHE_VARIABLES)
list(FILTER _sdb_cache_variables INCLUDE REGEX "^SDB_")
list(SORT _sdb_cache_variables)
foreach(_sdb_variable IN LISTS _sdb_cache_variables)
    get_property(_sdb_type CACHE ${_sdb_variable} PROPERTY TYPE)
    if(
        _sdb_type MATCHES "^(BOOL|STRING|PATH|FILEPATH)$"
        AND NOT "${${_sdb_variable}}" STREQUAL ""
    )
        string(
            APPEND SDB_BUILD_CONFIGURE
            " -D${_sdb_variable}=${${_sdb_variable}}"
        )
    endif()
endforeach()
configure_file(build_options.h.in build_options.h NEWLINE_STYLE UNIX @ONLY)

find_package(Git QUIET)
add_custom_target(
    serene_build_commit
    COMMAND
        ${CMAKE_COMMAND} -DGIT_EXECUTABLE=${GIT_EXECUTABLE}
        -DSOURCE_DIR=${serenedb_SOURCE_DIR}
        -DOUTPUT=${CMAKE_CURRENT_BINARY_DIR}/build_commit.h -P
        ${serenedb_SOURCE_DIR}/cmake/BuildCommit.cmake
    BYPRODUCTS ${CMAKE_CURRENT_BINARY_DIR}/build_commit.h
    VERBATIM
)

add_library(
    serene
    STATIC
    app_server.cpp
    build_info.cpp
    drain_gate.cpp
    file_utils.cpp
    files.cpp
    init.cpp
    lifecycle.cpp
    lockfile.cpp
    message_buffer.cpp
    message_sequence_view.cpp
    metrics.cpp
    number_of_cores.cpp
    signals.cpp
)
add_dependencies(serene serene_build_commit)

target_link_libraries(serene PUBLIC iresearch boost_boost simdjson)
