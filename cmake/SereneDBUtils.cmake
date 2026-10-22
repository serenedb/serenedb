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
    if(NOT AUTO_UPDATE_MODULES)
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
        "EXCLUDE_FROM_ALL;SYSTEM;NO_WARNINGS;COVERAGE"
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
    set(_sdb_add_module_ARGS "")
    foreach(_sdb_add_module_ARG EXCLUDE_FROM_ALL SYSTEM)
        if(_sdb_add_module_${_sdb_add_module_ARG})
            list(APPEND _sdb_add_module_ARGS ${_sdb_add_module_ARG})
        endif()
    endforeach()
    sdb_update_module(${module})
    add_subdirectory(${_sdb_add_module_DIR} ${_sdb_add_module_ARGS})
    if(_sdb_add_module_NO_WARNINGS)
        sdb_dir_compile_options(${_sdb_add_module_DIR} -w)
    endif()
    if(_sdb_add_module_COVERAGE AND SDB_COVERAGE)
        sdb_dir_compile_options(${_sdb_add_module_DIR} ${SDB_COVERAGE_FLAGS})
    endif()
endmacro()
