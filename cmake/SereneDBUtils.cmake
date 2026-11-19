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

function(_sdb_instantiate_axis binding out_placeholder out_values)
    if(NOT binding MATCHES "^([A-Za-z0-9_]+)=([A-Za-z0-9_]+)$")
        message(
            FATAL_ERROR
            "sdb_instantiate(${arg_DIR}): an axis binding is PLACEHOLDER=AXIS, got ${binding}"
        )
    endif()
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
    set(${out_placeholder} ${CMAKE_MATCH_1} PARENT_SCOPE)
    set(${out_values} "${values}" PARENT_SCOPE)
endfunction()

function(_sdb_instantiate_bind placeholder value text out_name out_text)
    string(REGEX MATCHALL "[^ ]+" tokens "${value}")
    list(POP_FRONT tokens name)
    string(REPLACE "@${placeholder}@" "${name}" text "${text}")
    foreach(token IN LISTS tokens)
        if(NOT token MATCHES "^([A-Za-z0-9_]+)=(.*)$")
            message(
                FATAL_ERROR
                "sdb_instantiation_axis: ${value} takes NAME KEY=VALUE..."
            )
        endif()
        string(
            REPLACE
            "@${placeholder}.${CMAKE_MATCH_1}@"
            "${CMAKE_MATCH_2}"
            text
            "${text}"
        )
    endforeach()
    set(${out_name} ${name} PARENT_SCOPE)
    set(${out_text} "${text}" PARENT_SCOPE)
endfunction()

function(_sdb_instantiate_emit name call includes)
    foreach(binding IN LISTS arg_EXPAND)
        _sdb_instantiate_axis(${binding} placeholder values)
        set(calls "")
        set(expanded_includes "")
        foreach(value IN LISTS values)
            _sdb_instantiate_bind(${placeholder} "${value}" "${call}" value_name value_call)
            if(value_name IN_LIST arg_SKIP)
                continue()
            endif()
            _sdb_instantiate_bind(${placeholder} "${value}" "${includes}" value_name value_includes)
            string(APPEND calls "${value_call}\n")
            string(APPEND expanded_includes "${value_includes}\n")
        endforeach()
        set(call "${calls}")
        set(includes "${expanded_includes}")
    endforeach()
    if(call MATCHES "@[A-Za-z0-9_.]+@")
        message(
            FATAL_ERROR
            "sdb_instantiate(${arg_DIR}/${name}): unresolved ${CMAKE_MATCH_0} in ${call}"
        )
    endif()
    string(REPLACE "\n" ";" include_list "${includes}")
    list(REMOVE_DUPLICATES include_list)
    set(content "")
    foreach(include IN LISTS include_list)
        if(NOT include STREQUAL "" AND NOT include MATCHES "@")
            string(APPEND content "#include \"${include}\"\n")
        endif()
    endforeach()
    string(
        APPEND
        content
        "\nnamespace ${arg_NAMESPACE} {\n\n${call}\n}  // namespace ${arg_NAMESPACE}\n"
    )
    set(file ${CMAKE_CURRENT_BINARY_DIR}/instantiate/${arg_DIR}/${name}.cpp)
    file(CONFIGURE OUTPUT ${file} CONTENT "${content}" @ONLY)
    target_sources(${target} PRIVATE ${file})
endfunction()

function(_sdb_instantiate_step index name call includes)
    list(LENGTH arg_AXES count)
    if(index EQUAL count)
        _sdb_instantiate_emit(${name} "${call}" "${includes}")
        return()
    endif()
    list(GET arg_AXES ${index} binding)
    _sdb_instantiate_axis(${binding} placeholder values)
    math(EXPR next "${index} + 1")
    foreach(value IN LISTS values)
        _sdb_instantiate_bind(${placeholder} "${value}" "${call}" value_name value_call)
        if(value_name IN_LIST arg_SKIP)
            continue()
        endif()
        _sdb_instantiate_bind(${placeholder} "${value}" "${includes}" value_name value_includes)
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
        "INCLUDE;AXES;EXPAND;SKIP"
    )
    if(arg_UNPARSED_ARGUMENTS OR NOT arg_DIR OR NOT arg_NAMESPACE OR NOT arg_CALL)
        message(
            FATAL_ERROR
            "sdb_instantiate(${target}): DIR, NAMESPACE and CALL are required, got ${ARGN}"
        )
    endif()
    string(REPLACE ";" "\n" includes "${arg_INCLUDE}")
    _sdb_instantiate_step(0 "${arg_PREFIX}" "${arg_CALL}" "${includes}")
endfunction()
