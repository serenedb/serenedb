target_sources(
    sereneserver
    PRIVATE
        pgoutput.cpp
        pg_replication_client.cpp
        repl_source.cpp
        subscription_engine.cpp
)
