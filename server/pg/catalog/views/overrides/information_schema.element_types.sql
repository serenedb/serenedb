SELECT CAST(current_database() AS sql_identifier) AS object_catalog,
       CAST(n.nspname AS sql_identifier) AS object_schema,
       CAST(x.objname AS sql_identifier) AS object_name,
       CAST(x.objtype AS character_data) AS object_type,
       CAST(x.objdtdid AS sql_identifier) AS collection_type_identifier,
       CAST(
         CASE WHEN nbt.nspname = 'pg_catalog' THEN format_type(bt.oid, null)
              ELSE 'USER-DEFINED' END AS character_data) AS data_type,

       CAST(null AS cardinal_number) AS character_maximum_length,
       CAST(null AS cardinal_number) AS character_octet_length,
       CAST(null AS sql_identifier) AS character_set_catalog,
       CAST(null AS sql_identifier) AS character_set_schema,
       CAST(null AS sql_identifier) AS character_set_name,
       CAST(CASE WHEN nco.nspname IS NOT NULL THEN current_database() END AS sql_identifier) AS collation_catalog,
       CAST(nco.nspname AS sql_identifier) AS collation_schema,
       CAST(co.collname AS sql_identifier) AS collation_name,
       CAST(null AS cardinal_number) AS numeric_precision,
       CAST(null AS cardinal_number) AS numeric_precision_radix,
       CAST(null AS cardinal_number) AS numeric_scale,
       CAST(null AS cardinal_number) AS datetime_precision,
       CAST(null AS character_data) AS interval_type,
       CAST(null AS cardinal_number) AS interval_precision,

       CAST(current_database() AS sql_identifier) AS udt_catalog,
       CAST(nbt.nspname AS sql_identifier) AS udt_schema,
       CAST(bt.typname AS sql_identifier) AS udt_name,

       CAST(null AS sql_identifier) AS scope_catalog,
       CAST(null AS sql_identifier) AS scope_schema,
       CAST(null AS sql_identifier) AS scope_name,

       CAST(null AS cardinal_number) AS maximum_cardinality,
       CAST('a' || CAST(x.objdtdid AS text) AS sql_identifier) AS dtd_identifier

FROM pg_namespace n, pg_type at, pg_namespace nbt, pg_type bt,
     (
       /* information_schema.columns, information_schema.attributes */
       SELECT c.relnamespace, CAST(c.relname AS sql_identifier),
              CASE WHEN c.relkind = 'c' THEN 'USER-DEFINED TYPE'::text ELSE 'TABLE'::text END,
              a.attnum, a.atttypid, a.attcollation
       FROM pg_class c, pg_attribute a
       WHERE c.oid = a.attrelid
             AND c.relkind IN ('r', 'v', 'f', 'c', 'p')
             AND attnum > 0 AND NOT attisdropped

       UNION ALL

       /* information_schema.domains */
       SELECT t.typnamespace, CAST(t.typname AS sql_identifier),
              'DOMAIN'::text, 1, t.typbasetype, t.typcollation
       FROM pg_type t
       WHERE t.typtype = 'd'

       UNION ALL

       /* information_schema.parameters */
       -- Rewritten: (ss.x).n / (ss.x).x -> ea_n / ea_x
       SELECT pronamespace,
              CAST(nameconcatoid(proname, oid) AS sql_identifier),
              'ROUTINE'::text, ss.ea_n, ss.ea_x, 0
       FROM (SELECT p.pronamespace, p.proname, p.oid,
                    ea.x AS ea_x, ea.n AS ea_n
             FROM pg_proc p,
                  information_schema._pg_expandarray(coalesce(p.proallargtypes, p.proargtypes::oid[])) AS ea
             ) AS ss

       UNION ALL

       /* result types */
       SELECT p.pronamespace,
              CAST(nameconcatoid(p.proname, p.oid) AS sql_identifier),
              'ROUTINE'::text, 0, p.prorettype, 0
       FROM pg_proc p

     ) AS x (objschema, objname, objtype, objdtdid, objtypeid, objcollation)
     LEFT JOIN (pg_collation co JOIN pg_namespace nco ON (co.collnamespace = nco.oid))
       ON x.objcollation = co.oid AND (nco.nspname, co.collname) <> ('pg_catalog', 'default')

WHERE n.oid = x.objschema
      AND at.oid = x.objtypeid
      AND (at.typelem <> 0 AND at.typlen = -1)
      AND at.typelem = bt.oid
      AND nbt.oid = bt.typnamespace

      AND (n.nspname, x.objname, x.objtype, CAST(x.objdtdid AS sql_identifier)) IN
          ( SELECT object_schema, object_name, object_type, dtd_identifier
                FROM information_schema.data_type_privileges )
