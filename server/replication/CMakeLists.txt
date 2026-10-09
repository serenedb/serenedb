target_sources(
    sereneserver
    PRIVATE
        conninfo.cpp
        pgoutput.cpp
        pg_replication_client.cpp
        publisher_session.cpp
        repl_source.cpp
        spill_buffer.cpp
        subscription_engine.cpp
)

target_link_libraries(sereneserver PRIVATE libpq)
