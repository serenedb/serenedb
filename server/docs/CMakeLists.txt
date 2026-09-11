if(SDB_EMBEDDED_DOCS)
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

    add_library(serene_docs STATIC ${SDB_DOCS_GENERATED})
else()
    add_library(serene_docs STATIC docs_data_stub.cpp)
endif()

target_include_directories(serene_docs PUBLIC ${CMAKE_SOURCE_DIR}/server)

target_sources(sereneserver PRIVATE docs_loader.cpp docs_lookup.cpp)

add_library(serene_docs_shell STATIC docs_shell_backend.cpp)

target_link_libraries(serene_docs_shell PUBLIC sereneserver duckdb_shell_static)

target_link_libraries(sereneserver PUBLIC serene_docs)
