add_library(
    serene
    STATIC
    app_server.cpp
    dtoa.cpp
    file_utils.cpp
    files.cpp
    init.cpp
    lifecycle.cpp
    lockfile.cpp
    message_buffer.cpp
    message_sequence_view.cpp
    metrics.cpp
    number_of_cores.cpp
    random_generator.cpp
    signals.cpp
)

target_link_libraries(serene PUBLIC iresearch-static boost_boost simdjson)

target_include_directories(serene PUBLIC ${CMAKE_SOURCE_DIR})
target_include_directories(serene PUBLIC ${CMAKE_BINARY_DIR})
target_include_directories(serene SYSTEM PUBLIC ${OPENSSL_INCLUDE_DIR})
