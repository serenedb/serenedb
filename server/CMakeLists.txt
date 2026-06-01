add_compile_warnings_flags()

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin/")

################################################################################
## serened executable
################################################################################

add_executable(serened rest_server/serened.cpp ${ProductVersionFiles})

target_link_libraries(serened sereneserver duckdb_shell_static)

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
        immer
        iresearch-static
        s2 # needed for some includes in geo analyzers
        serene
        serene_geo
        serene_connector
        # TODO
        date_interface
        rocksdb_interface
        llhttp
        nghttp2::nghttp2
        vpack
        rocksdb
        duckdb_static
        Folly::folly
)

target_include_directories(sereneserver PUBLIC ${CMAKE_SOURCE_DIR}/server)

add_dependencies(sereneserver tzdata)

add_subdirectory(auth)
add_subdirectory(catalog)
add_subdirectory(connector)
add_subdirectory(database)
add_subdirectory(general_server)
add_subdirectory(pg)
add_subdirectory(query)
add_subdirectory(rest_server)
add_subdirectory(storage_engine)
add_subdirectory(rocksdb_engine_catalog)
add_subdirectory(utils)
add_subdirectory(search)
