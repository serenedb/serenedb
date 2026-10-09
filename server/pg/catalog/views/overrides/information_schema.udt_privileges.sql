SELECT CAST(u_grantor.rolname AS sql_identifier) AS grantor,
       CAST(grantee.rolname AS sql_identifier) AS grantee,
       CAST(current_database() AS sql_identifier) AS udt_catalog,
       CAST(n.nspname AS sql_identifier) AS udt_schema,
       CAST(t.typname AS sql_identifier) AS udt_name,
       CAST('TYPE USAGE' AS character_data) AS privilege_type, -- sic
       CAST(
         CASE WHEN
              -- object owner always has grant options
              pg_has_role(grantee.oid, t.typowner, 'USAGE')
              OR t.grantable
              THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_grantable

FROM (
        -- TODO(mbkkt): rewrite once DuckDB parser supports (expr).* composite expansion
        SELECT oid, typname, typnamespace, typtype, typowner, acl.* FROM pg_type, aclexplode(coalesce(typacl, acldefault('T', typowner))) AS acl
     ) AS t (oid, typname, typnamespace, typtype, typowner, grantor, grantee, prtype, grantable),
     pg_namespace n,
     pg_authid u_grantor,
     (
       SELECT oid, rolname FROM pg_authid
       UNION ALL
       SELECT 0::oid, 'PUBLIC'
     ) AS grantee (oid, rolname)

WHERE t.typnamespace = n.oid
      AND t.typtype = 'c'
      AND t.grantee = grantee.oid
      AND t.grantor = u_grantor.oid
      AND t.prtype IN ('USAGE')
      AND (pg_has_role(u_grantor.oid, 'USAGE')
           OR pg_has_role(grantee.oid, 'USAGE')
           OR grantee.rolname = 'PUBLIC')
