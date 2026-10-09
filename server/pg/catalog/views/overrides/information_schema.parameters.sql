SELECT CAST(current_database() AS sql_identifier) AS specific_catalog,
       CAST(n_nspname AS sql_identifier) AS specific_schema,
       CAST(nameconcatoid(proname, p_oid) AS sql_identifier) AS specific_name,
       CAST(ss.ea_n AS cardinal_number) AS ordinal_position,
       CAST(
         CASE WHEN proargmodes IS NULL THEN 'IN'
            WHEN proargmodes[ss.ea_n] = 'i' THEN 'IN'
            WHEN proargmodes[ss.ea_n] = 'o' THEN 'OUT'
            WHEN proargmodes[ss.ea_n] = 'b' THEN 'INOUT'
            WHEN proargmodes[ss.ea_n] = 'v' THEN 'IN'
            WHEN proargmodes[ss.ea_n] = 't' THEN 'OUT'
         END AS character_data) AS parameter_mode,
       CAST('NO' AS yes_or_no) AS is_result,
       CAST('NO' AS yes_or_no) AS as_locator,
       CAST(NULLIF(proargnames[ss.ea_n], '') AS sql_identifier) AS parameter_name,
       CAST(
         CASE WHEN t.typelem <> 0 AND t.typlen = -1 THEN 'ARRAY'
              WHEN nt.nspname = 'pg_catalog' THEN format_type(t.oid, null)
              ELSE 'USER-DEFINED' END AS character_data)
         AS data_type,
       CAST(null AS cardinal_number) AS character_maximum_length,
       CAST(null AS cardinal_number) AS character_octet_length,
       CAST(null AS sql_identifier) AS character_set_catalog,
       CAST(null AS sql_identifier) AS character_set_schema,
       CAST(null AS sql_identifier) AS character_set_name,
       CAST(null AS sql_identifier) AS collation_catalog,
       CAST(null AS sql_identifier) AS collation_schema,
       CAST(null AS sql_identifier) AS collation_name,
       CAST(null AS cardinal_number) AS numeric_precision,
       CAST(null AS cardinal_number) AS numeric_precision_radix,
       CAST(null AS cardinal_number) AS numeric_scale,
       CAST(null AS cardinal_number) AS datetime_precision,
       CAST(null AS character_data) AS interval_type,
       CAST(null AS cardinal_number) AS interval_precision,
       CAST(current_database() AS sql_identifier) AS udt_catalog,
       CAST(nt.nspname AS sql_identifier) AS udt_schema,
       CAST(t.typname AS sql_identifier) AS udt_name,
       CAST(null AS sql_identifier) AS scope_catalog,
       CAST(null AS sql_identifier) AS scope_schema,
       CAST(null AS sql_identifier) AS scope_name,
       CAST(null AS cardinal_number) AS maximum_cardinality,
       CAST(ss.ea_n AS sql_identifier) AS dtd_identifier,
       CAST(
         CASE WHEN pg_has_role(proowner, 'USAGE')
              THEN pg_get_function_arg_default(p_oid, ss.ea_n)
              ELSE NULL END
         AS character_data) AS parameter_default

FROM pg_type t, pg_namespace nt,
     (SELECT n.nspname AS n_nspname, p.proname, p.oid AS p_oid, p.proowner,
             p.proargnames, p.proargmodes,
             ea.x AS ea_x, ea.n AS ea_n
      FROM pg_namespace n, pg_proc p,
           information_schema._pg_expandarray(coalesce(p.proallargtypes, p.proargtypes::oid[])) AS ea
      WHERE n.oid = p.pronamespace
            AND (pg_has_role(p.proowner, 'USAGE') OR
                 has_function_privilege(p.oid, 'EXECUTE'))) AS ss
WHERE t.oid = ss.ea_x AND t.typnamespace = nt.oid
