cmake_minimum_required(VERSION 2.8.12...3.29)
set(TARGET_NAME clickhouse_scanner)
project(${TARGET_NAME})

if(NOT MSVC)
    add_compile_options(
        -Wno-pedantic
        -Wno-sign-compare
        -Wno-unused-variable)
endif()

include_directories(include)

add_subdirectory(src)

set(PARAMETERS "-no-warnings")
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

build_static_extension(${TARGET_NAME} ${ALL_OBJECT_FILES})
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${ALL_OBJECT_FILES})

target_link_libraries(${EXTENSION_NAME} clickhouse_cpp)
target_link_libraries(${LOADABLE_EXTENSION_NAME} clickhouse_cpp)

install(
    TARGETS ${EXTENSION_NAME}
    EXPORT "${DUCKDB_EXPORT_SET}"
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
