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

add_subdirectory(app)
add_subdirectory(auth)
add_subdirectory(backup)
add_subdirectory(catalog)
add_subdirectory(connector)
add_subdirectory(docs)
add_subdirectory(jobs)
add_subdirectory(network)
add_subdirectory(otel)
add_subdirectory(pg)
add_subdirectory(query)
add_subdirectory(replication)
add_subdirectory(runtime)
add_subdirectory(storage)
add_subdirectory(search)

add_executable(
    serenedb
    app/main.cpp
    app/cli.cpp
    app/command.cpp
    app/docs_builder.cpp
    app/server.cpp
)
target_compile_definitions(
    serenedb
    PRIVATE
        SDB_REMAP_EXECUTABLE=$<IF:$<BOOL:${SDB_REMAP_EXECUTABLE}>,true,false>
)
target_link_libraries(serenedb PRIVATE serene_docs_shell serene_docs_builder)
sdb_link_duckdb_extensions(serenedb)
if(SDB_EMBEDDED_DOCS)
    target_link_libraries(serenedb PRIVATE serene_docs_index)
    sdb_build_docs_index(serenedb)
endif()
add_custom_command(
    TARGET serenedb
    POST_BUILD
    COMMAND
        ${CMAKE_COMMAND} -E create_symlink serenedb
        $<TARGET_FILE_DIR:serenedb>/serened
    VERBATIM
)

install(TARGETS serenedb RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} OPTIONAL)
install(
    PROGRAMS $<TARGET_FILE_DIR:serenedb>/serened
    DESTINATION ${CMAKE_INSTALL_BINDIR}
    OPTIONAL
)
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
