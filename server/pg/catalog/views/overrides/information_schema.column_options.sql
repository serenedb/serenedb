SELECT CAST(current_database() AS sql_identifier) AS table_catalog,
       CAST(c.nspname AS sql_identifier) AS table_schema,
       CAST(c.relname AS sql_identifier) AS table_name,
       CAST(c.attname AS sql_identifier) AS column_name,
       CAST(opts.option_name AS sql_identifier) AS option_name,
       CAST(opts.option_value AS character_data) AS option_value
FROM information_schema._pg_foreign_table_columns c,
     pg_options_to_table(c.attfdwoptions) opts
