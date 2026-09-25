add_compile_warnings_flags()

################################################################################
## serened executable
################################################################################

add_executable(serened rest_server/serened.cpp)

target_link_libraries(serened serene_docs_shell)

################################################################################
## installation
################################################################################

install(TARGETS serened RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} OPTIONAL)
install_config(serened "${CMAKE_SOURCE_DIR}/etc/serenedb")

################################################################################
## libraries
################################################################################

add_library(sereneserver STATIC)

target_link_libraries(
    sereneserver
    PUBLIC
        boost_boost
        iresearch-static
        s2 # needed for some includes in geo analyzers
        serene
        llhttp
        duckdb_static
)

target_include_directories(
    sereneserver
    PUBLIC
        ${CMAKE_SOURCE_DIR}/server
        # ICU extension helpers for session-zone TIMESTAMPTZ text I/O.
        ${CMAKE_SOURCE_DIR}/third_party/duckdb/extension/icu/include
)

add_subdirectory(auth)
add_subdirectory(catalog)
add_subdirectory(connector)
add_subdirectory(docs)
add_subdirectory(network)
add_subdirectory(otel)
add_subdirectory(pg)
add_subdirectory(query)
add_subdirectory(rest_server)
add_subdirectory(scheduler)
add_subdirectory(storage_engine)
add_subdirectory(search)

################################################################################
## embedded documentation index
################################################################################

if(SDB_EMBEDDED_DOCS)
    set(SDB_DOCS_INDEX_GENERATED
        ${CMAKE_CURRENT_BINARY_DIR}/docs_index_data.cpp
    )
    set(SDB_DOCS_INDEX_WORK ${CMAKE_CURRENT_BINARY_DIR}/docs_index)

    add_executable(serened-docs-bootstrap rest_server/serened.cpp)

    target_link_libraries(
        serened-docs-bootstrap
        serene_docs_shell
        serene_docs_corpus
        serene_docs_builder
        serene_docs_bootstrap
    )

    add_custom_command(
        OUTPUT ${SDB_DOCS_INDEX_GENERATED}
        COMMAND ${CMAKE_COMMAND} -E rm -rf ${SDB_DOCS_INDEX_WORK}
        COMMAND
            $<TARGET_FILE:serened-docs-bootstrap> ${SDB_DOCS_INDEX_WORK}/datadir
            --build_docs_index=${SDB_DOCS_INDEX_WORK}/index
        COMMAND
            ${PYTHON_EXECUTABLE}
            ${CMAKE_SOURCE_DIR}/scripts/generate_docs_index.py
            ${SDB_DOCS_INDEX_WORK}/index ${SDB_DOCS_INDEX_GENERATED}.tmp
        COMMAND
            ${CMAKE_COMMAND} -E copy_if_different
            ${SDB_DOCS_INDEX_GENERATED}.tmp ${SDB_DOCS_INDEX_GENERATED}
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
    target_include_directories(
        serene_docs_index
        PRIVATE ${CMAKE_SOURCE_DIR}/server
    )

    target_link_libraries(serened serene_docs_index)
endif()
