add_subdirectory(sdb_catalog)
add_subdirectory(commands)

# serialize.cpp reuses the inet extension's canonical IP-to-string formatter
# (ipaddress_to_string) so native PG `inet` output matches host()/::varchar.
# The extension exposes its headers under src/duckdb/inet/, included as
# <duckdb/inet/...> alongside DuckDB's own <duckdb/...> headers.
target_include_directories(
    sereneserver
    PRIVATE ${CMAKE_SOURCE_DIR}/third_party/duckdb-inet/src
)

target_sources(
    sereneserver
    PRIVATE
        pg_catalog/pg_am.cpp
        pg_catalog/pg_attribute.cpp
        pg_catalog/pg_authid.cpp
        pg_catalog/pg_class.cpp
        pg_catalog/pg_constraint.cpp
        pg_catalog/pg_database.cpp
        pg_catalog/pg_description.cpp
        pg_catalog/pg_enum.cpp
        pg_catalog/pg_extension.cpp
        pg_catalog/pg_index.cpp
        pg_catalog/pg_language.cpp
        pg_catalog/pg_namespace.cpp
        pg_catalog/pg_opclass.cpp
        pg_catalog/pg_proc.cpp
        pg_catalog/pg_tablespace.cpp
        pg_catalog/pg_ts_dict.cpp
        pg_catalog/pg_stat_progress.cpp
        pg_catalog/pg_type.cpp
        information_schema/sql_implementation_info.cpp
        information_schema/sql_parts.cpp
        information_schema/sql_sizing.cpp
        pg_comm_task.cpp
        pg_feature.cpp
        connection_context.cpp
        pg_types.cpp
        serialize.cpp
        system_catalog.cpp
        progress_tracker.cpp
        sql_utils.cpp
        option_help.cpp
        tokenizer_options.cpp
        geo_tokenizer_options.cpp
)
