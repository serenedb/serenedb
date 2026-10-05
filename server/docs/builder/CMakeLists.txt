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
set(SDB_DOCS_CORPUS
    ${CMAKE_CURRENT_BINARY_DIR}/docs_corpus.bin
    CACHE INTERNAL
    ""
)
set(SDB_DOCS_INDEX_WORK
    ${CMAKE_CURRENT_BINARY_DIR}/docs_index
    CACHE INTERNAL
    ""
)

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
    OUTPUT ${SDB_DOCS_GENERATED} ${SDB_DOCS_CORPUS}
    COMMAND
        ${PYTHON_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/generate_docs.py
        ${SDB_DOCS_DIR} ${SDB_DOCS_GENERATED}.tmp --tests-dir
        ${SDB_DOCS_TESTS_DIR} --corpus ${SDB_DOCS_CORPUS}.tmp
    COMMAND
        ${CMAKE_COMMAND} -E copy_if_different ${SDB_DOCS_GENERATED}.tmp
        ${SDB_DOCS_GENERATED}
    COMMAND
        ${CMAKE_COMMAND} -E copy_if_different ${SDB_DOCS_CORPUS}.tmp
        ${SDB_DOCS_CORPUS}
    COMMAND
        ${CMAKE_COMMAND} -E remove ${SDB_DOCS_GENERATED}.tmp
        ${SDB_DOCS_CORPUS}.tmp
    DEPENDS
        ${SDB_DOCS_SOURCES}
        ${SDB_DOCS_SNIPPET_SOURCES}
        ${CMAKE_SOURCE_DIR}/scripts/generate_docs.py
        ${CMAKE_SOURCE_DIR}/scripts/sqllogic_snippets.py
    COMMENT "Generating embedded docs ${SDB_DOCS_GENERATED}"
    VERBATIM
)
add_custom_target(serene_docs_corpus_file DEPENDS ${SDB_DOCS_CORPUS})

add_library(serene_docs_corpus OBJECT ${SDB_DOCS_GENERATED})
target_include_directories(
    serene_docs_corpus
    PRIVATE ${CMAKE_SOURCE_DIR}/server
)

add_library(serene_docs_builder OBJECT docs_loader.cpp)
target_link_libraries(serene_docs_builder PRIVATE sereneserver)

add_library(serene_docs_bootstrap OBJECT docs_bootstrap.cpp)
target_link_libraries(serene_docs_bootstrap PRIVATE sereneserver)

add_library(
    serene_docs_index
    OBJECT
    ${CMAKE_SOURCE_DIR}/server/docs/docs_index_image.cpp
)
target_include_directories(serene_docs_index PRIVATE ${CMAKE_SOURCE_DIR}/server)

function(sdb_embed_docs_index TARGET)
    if(NOT APPLE)
        set(_script ${CMAKE_SOURCE_DIR}/server/docs/docs_index.ld)
        target_link_options(${TARGET} PRIVATE -Wl,-T,${_script})
        set_property(TARGET ${TARGET} APPEND PROPERTY LINK_DEPENDS ${_script})
    endif()
    add_custom_command(
        TARGET ${TARGET}
        POST_BUILD
        COMMAND
            ${PYTHON_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/embed_docs_index.py
            ${SDB_DOCS_INDEX_WORK}/index $<TARGET_FILE:${TARGET}>
        VERBATIM
    )
    if(APPLE)
        add_custom_command(
            TARGET ${TARGET}
            POST_BUILD
            COMMAND codesign --force --sign - $<TARGET_FILE:${TARGET}>
            VERBATIM
        )
    endif()
endfunction()

function(sdb_build_docs_index TARGET)
    add_dependencies(${TARGET} serene_docs_corpus_file)
    add_custom_command(
        TARGET ${TARGET}
        POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E rm -rf ${SDB_DOCS_INDEX_WORK}
        COMMAND
            $<TARGET_FILE:${TARGET}> ${SDB_DOCS_INDEX_WORK}/datadir
            --build_docs_index=${SDB_DOCS_INDEX_WORK}/index
            --docs_corpus=${SDB_DOCS_CORPUS}
        COMMAND ${CMAKE_COMMAND} -E rm -rf ${SDB_DOCS_INDEX_WORK}/datadir
        VERBATIM
    )
    sdb_embed_docs_index(${TARGET})
endfunction()
