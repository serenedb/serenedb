SELECT CAST(current_database() AS sql_identifier) AS constraint_catalog,
       CAST(nc_nspname AS sql_identifier) AS constraint_schema,
       CAST(conname AS sql_identifier) AS constraint_name,
       CAST(current_database() AS sql_identifier) AS table_catalog,
       CAST(nr_nspname AS sql_identifier) AS table_schema,
       CAST(relname AS sql_identifier) AS table_name,
       CAST(a.attname AS sql_identifier) AS column_name,
       CAST(ss.ea_n AS cardinal_number) AS ordinal_position,
       CAST(CASE WHEN contype = 'f' THEN
                   information_schema._pg_index_position(ss.conindid, ss.confkey[ss.ea_n])
                 ELSE NULL
            END AS cardinal_number)
         AS position_in_unique_constraint
FROM pg_attribute a,
     (SELECT r.oid AS roid, r.relname, r.relowner,
             nc.nspname AS nc_nspname, nr.nspname AS nr_nspname,
             c.oid AS coid, c.conname, c.contype, c.conindid,
             c.confkey, c.confrelid,
             ea.x AS ea_x, ea.n AS ea_n
      FROM pg_namespace nr
           JOIN pg_class r ON nr.oid = r.relnamespace
           JOIN pg_constraint c ON r.oid = c.conrelid
           JOIN pg_namespace nc ON nc.oid = c.connamespace,
           information_schema._pg_expandarray(c.conkey) AS ea
      WHERE c.contype IN ('p', 'u', 'f')
            AND r.relkind IN ('r', 'p')
            AND (NOT pg_is_other_temp_schema(nr.oid)) ) AS ss
WHERE ss.roid = a.attrelid
      AND a.attnum = ss.ea_x
      AND NOT a.attisdropped
      AND (pg_has_role(relowner, 'USAGE')
           OR has_column_privilege(roid, a.attnum,
                                   'SELECT, INSERT, UPDATE, REFERENCES'))
