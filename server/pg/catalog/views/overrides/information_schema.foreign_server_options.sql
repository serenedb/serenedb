SELECT foreign_server_catalog,
       foreign_server_name,
       CAST(opts.option_name AS sql_identifier) AS option_name,
       CAST(opts.option_value AS character_data) AS option_value
FROM information_schema._pg_foreign_servers s,
     pg_options_to_table(s.srvoptions) opts
