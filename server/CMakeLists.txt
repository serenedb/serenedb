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

target_include_directories(sereneserver PUBLIC ${serenedb_SOURCE_DIR}/server)

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

add_executable(serened app/serened.cpp)
sdb_server_executable(serened)

install(TARGETS serened RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} OPTIONAL)
configure_file(
    ${serenedb_SOURCE_DIR}/etc/serenedb/serened.conf.in
    ${serenedb_BINARY_DIR}/etc/serenedb/serened.conf
    NEWLINE_STYLE UNIX
    @ONLY
)
install(
    FILES ${serenedb_BINARY_DIR}/etc/serenedb/serened.conf
    DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/serenedb
)
