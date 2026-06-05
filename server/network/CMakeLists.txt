target_sources(
    sereneserver
    PRIVATE
        io_context.cpp
        ada_re2_provider.cpp
        tls_context.cpp
        http/router.cpp
        http/h1_codec.cpp
        http/tier0_handlers.cpp
        pg/pg_frame_codec.cpp
        pg/wire_frames.cpp
        pg/auth.cpp
        network_server_feature.cpp
)

target_link_libraries(sereneserver PUBLIC ada::ada simdutf::simdutf re2::re2)
