configure_file(build.h.in build.h NEWLINE_STYLE UNIX)
set_source_files_properties(
    ${CMAKE_CURRENT_BINARY_DIR}/credits.cpp
    PROPERTIES GENERATED TRUE
)

add_library(
    serene
    STATIC
    app_server.cpp
    ${CMAKE_CURRENT_BINARY_DIR}/credits.cpp
    drain_gate.cpp
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

target_link_libraries(serene PUBLIC iresearch boost_boost simdjson)
