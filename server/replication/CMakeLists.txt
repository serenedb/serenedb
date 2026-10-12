target_sources(
    sereneserver
    PRIVATE
        conninfo.cpp
        functions.cpp
        pgoutput.cpp
        pg_replication_client.cpp
        publisher_session.cpp
        repl_source.cpp
        settings.cpp
        spill_buffer.cpp
        subscription_engine.cpp
        sync_session.cpp
)

target_link_libraries(sereneserver PRIVATE libpq)
