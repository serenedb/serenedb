add_library(serene_docs STATIC docs_bootstrap_stub.cpp docs_index_stub.cpp)

target_include_directories(serene_docs PUBLIC ${CMAKE_SOURCE_DIR}/server)

target_sources(sereneserver PRIVATE docs_search.cpp docs_functions.cpp)

add_library(serene_docs_shell STATIC docs_shell_backend.cpp)

target_link_libraries(serene_docs_shell PUBLIC sereneserver duckdb_shell)

target_link_libraries(sereneserver PUBLIC serene_docs)

add_subdirectory(builder)
