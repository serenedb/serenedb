if(NOT SDB_EMBEDDED_DOCS)
    add_library(serene_docs_corpus OBJECT docs_data_stub.cpp)
    target_include_directories(
        serene_docs_corpus
        PRIVATE ${CMAKE_SOURCE_DIR}/server
    )
    return()
endif()

set(SDB_DOCS_DIR ${CMAKE_SOURCE_DIR}/docs)
set(SDB_DOCS_TESTS_DIR ${CMAKE_SOURCE_DIR}/tests/sqllogic)
set(SDB_DOCS_GENERATED ${CMAKE_CURRENT_BINARY_DIR}/docs_data.cpp)

file(
    GLOB_RECURSE SDB_DOCS_SOURCES
    CONFIGURE_DEPENDS
    ${SDB_DOCS_DIR}/*.md
    ${SDB_DOCS_DIR}/*.mdx
)

file(
    GLOB_RECURSE SDB_DOCS_SNIPPET_SOURCES
    CONFIGURE_DEPENDS
    ${SDB_DOCS_TESTS_DIR}/sdb/pg/site_docs/*.test
    ${SDB_DOCS_TESTS_DIR}/sdb/pg/site_docs/*.test_slow
    ${SDB_DOCS_TESTS_DIR}/recovery/site_docs/*.test
    ${SDB_DOCS_TESTS_DIR}/recovery/site_docs/*.test_slow
)

add_custom_command(
    OUTPUT ${SDB_DOCS_GENERATED}
    COMMAND
        ${PYTHON_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/generate_docs.py
        ${SDB_DOCS_DIR} ${SDB_DOCS_GENERATED}.tmp --tests-dir
        ${SDB_DOCS_TESTS_DIR}
    COMMAND
        ${CMAKE_COMMAND} -E copy_if_different ${SDB_DOCS_GENERATED}.tmp
        ${SDB_DOCS_GENERATED}
    COMMAND ${CMAKE_COMMAND} -E remove ${SDB_DOCS_GENERATED}.tmp
    DEPENDS
        ${SDB_DOCS_SOURCES}
        ${SDB_DOCS_SNIPPET_SOURCES}
        ${CMAKE_SOURCE_DIR}/scripts/generate_docs.py
        ${CMAKE_SOURCE_DIR}/scripts/sqllogic_snippets.py
    COMMENT "Generating embedded docs ${SDB_DOCS_GENERATED}"
    VERBATIM
)

add_library(serene_docs_corpus OBJECT ${SDB_DOCS_GENERATED})
target_include_directories(
    serene_docs_corpus
    PRIVATE ${CMAKE_SOURCE_DIR}/server
)

add_library(serene_docs_builder OBJECT docs_loader.cpp)
target_link_libraries(serene_docs_builder PRIVATE sereneserver)

add_library(serene_docs_bootstrap OBJECT docs_bootstrap.cpp)
target_link_libraries(serene_docs_bootstrap PRIVATE sereneserver)

set(SDB_DOCS_INDEX_GENERATED ${CMAKE_CURRENT_BINARY_DIR}/docs_index_data.cpp)
set(SDB_DOCS_INDEX_WORK ${CMAKE_CURRENT_BINARY_DIR}/docs_index)

add_executable(
    serened-docs-bootstrap
    ${CMAKE_SOURCE_DIR}/server/rest_server/serened.cpp
)

target_link_libraries(
    serened-docs-bootstrap
    serene_docs_shell
    serene_docs_corpus
    serene_docs_builder
    serene_docs_bootstrap
)
sdb_link_duckdb_extensions(serened-docs-bootstrap)

add_custom_command(
    OUTPUT ${SDB_DOCS_INDEX_GENERATED}
    COMMAND ${CMAKE_COMMAND} -E rm -rf ${SDB_DOCS_INDEX_WORK}
    COMMAND
        $<TARGET_FILE:serened-docs-bootstrap> ${SDB_DOCS_INDEX_WORK}/datadir
        --build_docs_index=${SDB_DOCS_INDEX_WORK}/index
    COMMAND
        ${PYTHON_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/generate_docs_index.py
        ${SDB_DOCS_INDEX_WORK}/index ${SDB_DOCS_INDEX_GENERATED}.tmp
    COMMAND
        ${CMAKE_COMMAND} -E copy_if_different ${SDB_DOCS_INDEX_GENERATED}.tmp
        ${SDB_DOCS_INDEX_GENERATED}
    COMMAND ${CMAKE_COMMAND} -E remove ${SDB_DOCS_INDEX_GENERATED}.tmp
    COMMAND ${CMAKE_COMMAND} -E rm -rf ${SDB_DOCS_INDEX_WORK}/datadir
    BYPRODUCTS ${SDB_DOCS_INDEX_WORK}/index
    DEPENDS
        serened-docs-bootstrap
        ${CMAKE_SOURCE_DIR}/scripts/generate_docs_index.py
    COMMENT "Building and embedding the documentation index"
    VERBATIM
)

add_library(serene_docs_index OBJECT ${SDB_DOCS_INDEX_GENERATED})
target_include_directories(serene_docs_index PRIVATE ${CMAKE_SOURCE_DIR}/server)

target_link_libraries(serened serene_docs_index)
