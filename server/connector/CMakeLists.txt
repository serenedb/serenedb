add_library(
    serene_connector
    STATIC
    data_sink.cpp
    primary_key.cpp
    data_source.cpp
    serenedb_connector.cpp
    key_utils.cpp
    rocksdb_sink_writer.cpp
    search_sink_writer.cpp
    search_remove_filter.cpp
    file_table.cpp
    sst_sink_writer.cpp
)

target_link_libraries(
    serene_connector
    serene_base
    iresearch-static
    rocksdb
    velox
    pg_query
)

# TODO(Dronplane): why not make public includes for sereneserver so we avoid this ugly line below?
# we need rocksdb utils and logical collection but sereneserver is a single monolithic target without proper include dirs setup
target_include_directories(serene_connector PUBLIC "${CMAKE_SOURCE_DIR}/server")
