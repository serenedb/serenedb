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
    signals.cpp
)

target_link_libraries(serene PUBLIC iresearch-static boost_boost simdjson)
