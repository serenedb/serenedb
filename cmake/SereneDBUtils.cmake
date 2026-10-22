function(sdb_dir_compile_options dir)
    get_property(targets DIRECTORY ${dir} PROPERTY BUILDSYSTEM_TARGETS)
    foreach(target IN LISTS targets)
        get_target_property(type ${target} TYPE)
        if(NOT type MATCHES "^(INTERFACE_LIBRARY|UTILITY)$")
            target_compile_options(${target} PRIVATE ${ARGN})
        endif()
    endforeach()
    get_property(subdirs DIRECTORY ${dir} PROPERTY SUBDIRECTORIES)
    foreach(subdir IN LISTS subdirs)
        sdb_dir_compile_options(${subdir} ${ARGN})
    endforeach()
endfunction()

function(sdb_update_module)
    if(NOT SDB_UPDATE_SUBMODULES)
        return()
    endif()
    set(cwd ${CMAKE_SOURCE_DIR}/third_party)
    foreach(module IN LISTS ARGN)
        execute_process(
            COMMAND ${GIT_EXECUTABLE} rev-parse @:./${module}
            WORKING_DIRECTORY ${cwd}
            OUTPUT_VARIABLE expected
        )
        execute_process(
            COMMAND ${GIT_EXECUTABLE} rev-parse HEAD
            WORKING_DIRECTORY ${cwd}/${module}
            OUTPUT_VARIABLE actual
        )
        if(expected STREQUAL actual)
            continue()
        endif()
        set(changes "")
        if(EXISTS ${cwd}/${module}/.git)
            execute_process(
                COMMAND ${GIT_EXECUTABLE} status --porcelain
                WORKING_DIRECTORY ${cwd}/${module}
                OUTPUT_VARIABLE changes
            )
        endif()
        if(changes)
            message(WARNING "NOT UPDATED: ${module} has uncommitted changes")
            continue()
        endif()
        execute_process(
            COMMAND
                ${GIT_EXECUTABLE} submodule update --init --force -- ${module}
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${cwd}
        )
        if(NOT result EQUAL 0)
            message(
                WARNING
                "FAILED: ${GIT_EXECUTABLE} submodule update --init -- ${module}"
            )
        endif()
    endforeach()
endfunction()

macro(sdb_add_module module)
    cmake_parse_arguments(
        _sdb_add_module
        "EXCLUDE_FROM_ALL;NO_WARNINGS;COVERAGE"
        "DIR"
        ""
        ${ARGN}
    )
    if(DEFINED _sdb_add_module_UNPARSED_ARGUMENTS)
        message(
            FATAL_ERROR
            "sdb_add_module(${module}): unknown arguments ${_sdb_add_module_UNPARSED_ARGUMENTS}"
        )
    endif()
    if(NOT DEFINED _sdb_add_module_DIR)
        set(_sdb_add_module_DIR ${module})
    endif()
    set(_sdb_add_module_DIR
        ${CMAKE_SOURCE_DIR}/third_party/${_sdb_add_module_DIR}
    )
    sdb_update_module(${module})
    if(_sdb_add_module_EXCLUDE_FROM_ALL)
        add_subdirectory(${_sdb_add_module_DIR} EXCLUDE_FROM_ALL SYSTEM)
    else()
        add_subdirectory(${_sdb_add_module_DIR} SYSTEM)
    endif()
    if(_sdb_add_module_NO_WARNINGS)
        sdb_dir_compile_options(${_sdb_add_module_DIR} -w)
    endif()
    if(_sdb_add_module_COVERAGE AND SDB_COVERAGE)
        sdb_dir_compile_options(${_sdb_add_module_DIR} ${SDB_COVERAGE_FLAGS})
    endif()
endmacro()

function(sdb_link_duckdb_extensions target)
    set(DUCKDB_MODULE_BASE_DIR ${CMAKE_SOURCE_DIR}/third_party/duckdb)
    duckdb_link_extensions(${target} PRIVATE ${STATICALLY_LINK_EXTENSIONS})
endfunction()
