SELECT CAST(current_database() AS sql_identifier) AS constraint_catalog,
       CAST(rs.nspname AS sql_identifier) AS constraint_schema,
       CAST(con.conname AS sql_identifier) AS constraint_name,
       CAST(pg_get_expr(con.conbin, coalesce(c.oid, 0)) AS character_data) AS check_clause
FROM pg_constraint con
       LEFT OUTER JOIN pg_namespace rs ON (rs.oid = con.connamespace)
       LEFT OUTER JOIN pg_class c ON (c.oid = con.conrelid)
       LEFT OUTER JOIN pg_type t ON (t.oid = con.contypid)
WHERE pg_has_role(coalesce(c.relowner, t.typowner), 'USAGE')
  AND con.contype = 'c'

UNION ALL
-- not-null constraints
-- sql_identifier and character_data is in system.main, not information_schema
SELECT current_database()::sql_identifier AS constraint_catalog,
       rs.nspname::sql_identifier AS constraint_schema,
       con.conname::sql_identifier AS constraint_name,
       -- format is in system.main, not pg_catalog
       format('%s IS NOT NULL', coalesce(att.attname, 'VALUE'))::character_data AS check_clause
 FROM pg_constraint con
        LEFT JOIN pg_namespace rs ON rs.oid = con.connamespace
        LEFT JOIN pg_class c ON c.oid = con.conrelid
        LEFT JOIN pg_type t ON t.oid = con.contypid
        LEFT JOIN pg_attribute att ON (con.conrelid = att.attrelid AND con.conkey[1] = att.attnum)
 WHERE pg_has_role(coalesce(c.relowner, t.typowner), 'USAGE'::text)
   AND con.contype = 'n'
