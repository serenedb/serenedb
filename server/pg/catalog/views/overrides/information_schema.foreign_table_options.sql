SELECT foreign_table_catalog,
       foreign_table_schema,
       foreign_table_name,
       CAST(opts.option_name AS sql_identifier) AS option_name,
       CAST(opts.option_value AS character_data) AS option_value
FROM information_schema._pg_foreign_tables t,
     pg_options_to_table(t.ftoptions) opts
