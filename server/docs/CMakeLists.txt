set(SDB_DOCS_DIR ${CMAKE_SOURCE_DIR}/docs)
set(SDB_DOCS_GENERATED ${CMAKE_CURRENT_BINARY_DIR}/docs_data.cpp)

file(GLOB_RECURSE SDB_DOCS_SOURCES CONFIGURE_DEPENDS
    ${SDB_DOCS_DIR}/*.md
    ${SDB_DOCS_DIR}/*.mdx
)

add_custom_command(
    OUTPUT ${SDB_DOCS_GENERATED}
    COMMAND
        ${PYTHON_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/generate_docs.py
        ${SDB_DOCS_DIR} ${SDB_DOCS_GENERATED}.tmp
    COMMAND
        ${CMAKE_COMMAND} -E copy_if_different ${SDB_DOCS_GENERATED}.tmp
        ${SDB_DOCS_GENERATED}
    COMMAND ${CMAKE_COMMAND} -E remove ${SDB_DOCS_GENERATED}.tmp
    DEPENDS
        ${SDB_DOCS_SOURCES}
        ${CMAKE_SOURCE_DIR}/scripts/generate_docs.py
    COMMENT "Generating embedded docs ${SDB_DOCS_GENERATED}"
    VERBATIM
)

add_library(serene_docs STATIC ${SDB_DOCS_GENERATED})

target_include_directories(serene_docs PUBLIC ${CMAKE_SOURCE_DIR}/server)

target_sources(sereneserver PRIVATE docs_loader.cpp)

target_link_libraries(sereneserver PUBLIC serene_docs)
