add_subdirectory(utils)

add_library(sereneserver STATIC)

target_link_libraries(
    sereneserver
    PUBLIC
        boost_boost
        iresearch
        s2 # needed for some includes in geo analyzers
        serene
        llhttp
        duckdb_static
        ZLIB::ZLIB
        zstd::zstd
        lz4_static
        zxc::zxc_lib
        brotlienc
        brotlidec
        snappy
)

target_include_directories(sereneserver PUBLIC ${CMAKE_SOURCE_DIR}/server)

add_subdirectory(auth)
add_subdirectory(catalog)
add_subdirectory(connector)
add_subdirectory(docs)
add_subdirectory(network)
add_subdirectory(otel)
add_subdirectory(pg)
add_subdirectory(query)
add_subdirectory(replication)
add_subdirectory(rest_server)
add_subdirectory(scheduler)
add_subdirectory(storage_engine)
add_subdirectory(search)

add_executable(serened rest_server/serened.cpp)
target_compile_definitions(
    serened
    PRIVATE
        SDB_REMAP_EXECUTABLE=$<IF:$<BOOL:${SDB_REMAP_EXECUTABLE}>,true,false>
)
target_link_libraries(serened PRIVATE serene_docs_shell)
sdb_link_duckdb_extensions(serened)
if(SDB_EMBEDDED_DOCS)
    target_link_libraries(
        serened
        PRIVATE serene_docs_builder serene_docs_bootstrap serene_docs_index
    )
    sdb_build_docs_index(serened)
endif()

install(TARGETS serened RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} OPTIONAL)
configure_file(
    ${CMAKE_SOURCE_DIR}/etc/serenedb/serened.conf.in
    ${CMAKE_BINARY_DIR}/etc/serenedb/serened.conf
    NEWLINE_STYLE UNIX
    @ONLY
)
install(
    FILES ${CMAKE_BINARY_DIR}/etc/serenedb/serened.conf
    DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/serenedb
)
