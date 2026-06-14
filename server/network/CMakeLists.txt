target_sources(
    sereneserver
    PRIVATE
        io_context.cpp
        ada_re2_provider.cpp
        tls_context.cpp
        http/auth.cpp
        http/router.cpp
        http/h1_codec.cpp
        http/response_writer.cpp
        http/es/common.cpp
        http/es/dsl.cpp
        http/es/handlers.cpp
        http/test/handlers.cpp
        pg/pg_frame_codec.cpp
        pg/wire_frames.cpp
        pg/wire_collector.cpp
        credentials.cpp
        server.cpp
)

target_link_libraries(sereneserver PUBLIC ada::ada simdutf::simdutf re2::re2)
