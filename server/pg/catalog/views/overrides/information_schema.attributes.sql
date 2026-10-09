SELECT CAST(current_database() AS sql_identifier) AS udt_catalog,
       CAST(nc.nspname AS sql_identifier) AS udt_schema,
       CAST(c.relname AS sql_identifier) AS udt_name,
       CAST(a.attname AS sql_identifier) AS attribute_name,
       CAST(a.attnum AS cardinal_number) AS ordinal_position,
       CAST(pg_get_expr(ad.adbin, ad.adrelid) AS character_data) AS attribute_default,
       CAST(CASE WHEN a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) THEN 'NO' ELSE 'YES' END
         AS yes_or_no)
         AS is_nullable, -- This column was apparently removed between SQL:2003 and SQL:2008.

       CAST(
         CASE WHEN t.typelem <> 0 AND t.typlen = -1 THEN 'ARRAY'
              WHEN nt.nspname = 'pg_catalog' THEN format_type(a.atttypid, null)
              ELSE 'USER-DEFINED' END
         AS character_data)
         AS data_type,

       CAST(
         information_schema._pg_char_max_length(information_schema._pg_truetypid(a.atttypid, t.typtype, t.typbasetype), information_schema._pg_truetypmod(a.atttypmod, t.typtype, t.typtypmod))
         AS cardinal_number)
         AS character_maximum_length,

       CAST(
         information_schema._pg_char_octet_length(information_schema._pg_truetypid(a.atttypid, t.typtype, t.typbasetype), information_schema._pg_truetypmod(a.atttypmod, t.typtype, t.typtypmod))
         AS cardinal_number)
         AS character_octet_length,

       CAST(null AS sql_identifier) AS character_set_catalog,
       CAST(null AS sql_identifier) AS character_set_schema,
       CAST(null AS sql_identifier) AS character_set_name,

       CAST(CASE WHEN nco.nspname IS NOT NULL THEN current_database() END AS sql_identifier) AS collation_catalog,
       CAST(nco.nspname AS sql_identifier) AS collation_schema,
       CAST(co.collname AS sql_identifier) AS collation_name,

       CAST(
         information_schema._pg_numeric_precision(information_schema._pg_truetypid(a.atttypid, t.typtype, t.typbasetype), information_schema._pg_truetypmod(a.atttypmod, t.typtype, t.typtypmod))
         AS cardinal_number)
         AS numeric_precision,

       CAST(
         information_schema._pg_numeric_precision_radix(information_schema._pg_truetypid(a.atttypid, t.typtype, t.typbasetype), information_schema._pg_truetypmod(a.atttypmod, t.typtype, t.typtypmod))
         AS cardinal_number)
         AS numeric_precision_radix,

       CAST(
         information_schema._pg_numeric_scale(information_schema._pg_truetypid(a.atttypid, t.typtype, t.typbasetype), information_schema._pg_truetypmod(a.atttypmod, t.typtype, t.typtypmod))
         AS cardinal_number)
         AS numeric_scale,

       CAST(
         information_schema._pg_datetime_precision(information_schema._pg_truetypid(a.atttypid, t.typtype, t.typbasetype), information_schema._pg_truetypmod(a.atttypmod, t.typtype, t.typtypmod))
         AS cardinal_number)
         AS datetime_precision,

       CAST(
         information_schema._pg_interval_type(information_schema._pg_truetypid(a.atttypid, t.typtype, t.typbasetype), information_schema._pg_truetypmod(a.atttypmod, t.typtype, t.typtypmod))
         AS character_data)
         AS interval_type,
       CAST(null AS cardinal_number) AS interval_precision,

       CAST(current_database() AS sql_identifier) AS attribute_udt_catalog,
       CAST(nt.nspname AS sql_identifier) AS attribute_udt_schema,
       CAST(t.typname AS sql_identifier) AS attribute_udt_name,

       CAST(null AS sql_identifier) AS scope_catalog,
       CAST(null AS sql_identifier) AS scope_schema,
       CAST(null AS sql_identifier) AS scope_name,

       CAST(null AS cardinal_number) AS maximum_cardinality,
       CAST(a.attnum AS sql_identifier) AS dtd_identifier,
       CAST('NO' AS yes_or_no) AS is_derived_reference_attribute

FROM (pg_attribute a LEFT JOIN pg_attrdef ad ON attrelid = adrelid AND attnum = adnum)
     JOIN (pg_class c JOIN pg_namespace nc ON (c.relnamespace = nc.oid)) ON a.attrelid = c.oid
     JOIN (pg_type t JOIN pg_namespace nt ON (t.typnamespace = nt.oid)) ON a.atttypid = t.oid
     LEFT JOIN (pg_collation co JOIN pg_namespace nco ON (co.collnamespace = nco.oid))
       ON a.attcollation = co.oid AND (nco.nspname, co.collname) <> ('pg_catalog', 'default')

WHERE a.attnum > 0 AND NOT a.attisdropped
      AND c.relkind IN ('c')
      AND (pg_has_role(c.relowner, 'USAGE')
           OR has_type_privilege(c.reltype, 'USAGE'))
