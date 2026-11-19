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

function(sdb_instantiation_axis axis)
    if(NOT ARGN)
        message(FATAL_ERROR "sdb_instantiation_axis(${axis}): no values")
    endif()
    set_property(GLOBAL PROPERTY SDB_INSTANTIATION_AXIS_${axis} "${ARGN}")
endfunction()

function(_sdb_instantiate_step index name call includes)
    list(LENGTH arg_AXES count)
    if(index EQUAL count)
        if(call MATCHES "@[A-Za-z0-9_.]+@")
            message(
                FATAL_ERROR
                "sdb_instantiate(${arg_DIR}/${name}): unresolved ${CMAKE_MATCH_0} in ${call}"
            )
        endif()
        set(content "")
        foreach(include IN LISTS includes)
            if(NOT include STREQUAL "" AND NOT include MATCHES "@")
                string(APPEND content "#include \"${include}\"\n")
            endif()
        endforeach()
        string(
            APPEND
            content
            "\nnamespace ${arg_NAMESPACE} {\n\n${call}\n\n}  // namespace ${arg_NAMESPACE}\n"
        )
        set(file ${CMAKE_CURRENT_BINARY_DIR}/instantiate/${arg_DIR}/${name}.cpp)
        file(CONFIGURE OUTPUT ${file} CONTENT "${content}" @ONLY)
        target_sources(${target} PRIVATE ${file})
        return()
    endif()
    list(GET arg_AXES ${index} binding)
    if(NOT binding MATCHES "^([A-Za-z0-9_]+)=([A-Za-z0-9_]+)$")
        message(
            FATAL_ERROR
            "sdb_instantiate(${arg_DIR}): AXES takes PLACEHOLDER=AXIS, got ${binding}"
        )
    endif()
    set(placeholder ${CMAKE_MATCH_1})
    get_property(
        values
        GLOBAL
        PROPERTY SDB_INSTANTIATION_AXIS_${CMAKE_MATCH_2}
    )
    if(NOT values)
        message(
            FATAL_ERROR
            "sdb_instantiate(${arg_DIR}): unknown axis ${CMAKE_MATCH_2}"
        )
    endif()
    math(EXPR next "${index} + 1")
    foreach(value IN LISTS values)
        string(REGEX MATCHALL "[^ ]+" tokens "${value}")
        list(POP_FRONT tokens value_name)
        if(value_name IN_LIST arg_SKIP)
            continue()
        endif()
        string(REPLACE "@${placeholder}@" "${value_name}" value_call "${call}")
        set(value_includes "")
        foreach(include IN LISTS includes)
            string(
                REPLACE
                "@${placeholder}@"
                "${value_name}"
                include
                "${include}"
            )
            list(APPEND value_includes "${include}")
        endforeach()
        foreach(token IN LISTS tokens)
            if(NOT token MATCHES "^([A-Za-z0-9_]+)=(.*)$")
                message(
                    FATAL_ERROR
                    "sdb_instantiation_axis: ${value} takes NAME KEY=VALUE..."
                )
            endif()
            set(key "@${placeholder}.${CMAKE_MATCH_1}@")
            string(
                REPLACE
                "${key}"
                "${CMAKE_MATCH_2}"
                value_call
                "${value_call}"
            )
            set(resolved "")
            foreach(include IN LISTS value_includes)
                string(REPLACE "${key}" "${CMAKE_MATCH_2}" include "${include}")
                list(APPEND resolved "${include}")
            endforeach()
            set(value_includes "${resolved}")
        endforeach()
        if(name STREQUAL "")
            set(value_file ${value_name})
        else()
            set(value_file ${name}_${value_name})
        endif()
        _sdb_instantiate_step(${next} ${value_file} "${value_call}" "${value_includes}")
    endforeach()
endfunction()

function(sdb_instantiate target)
    cmake_parse_arguments(
        PARSE_ARGV
        1
        arg
        ""
        "DIR;PREFIX;NAMESPACE;CALL"
        "INCLUDE;AXES;SKIP"
    )
    if(arg_UNPARSED_ARGUMENTS OR NOT arg_DIR OR NOT arg_NAMESPACE OR NOT arg_CALL)
        message(
            FATAL_ERROR
            "sdb_instantiate(${target}): DIR, NAMESPACE and CALL are required, got ${ARGN}"
        )
    endif()
    _sdb_instantiate_step(0 "${arg_PREFIX}" "${arg_CALL}" "${arg_INCLUDE}")
endfunction()
