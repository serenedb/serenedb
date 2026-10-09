SELECT foreign_data_wrapper_catalog,
       foreign_data_wrapper_name,
       CAST(opts.option_name AS sql_identifier) AS option_name,
       CAST(opts.option_value AS character_data) AS option_value
FROM information_schema._pg_foreign_data_wrappers w,
     pg_options_to_table(w.fdwoptions) opts
