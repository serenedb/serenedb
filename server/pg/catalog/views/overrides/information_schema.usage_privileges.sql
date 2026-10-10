/* information_schema.collations */
-- Collations have no real privileges, so we represent all information_schema.collations with implicit usage privilege here.
SELECT CAST(u.rolname AS sql_identifier) AS grantor,
       CAST('PUBLIC' AS sql_identifier) AS grantee,
       CAST(current_database() AS sql_identifier) AS object_catalog,
       CAST(n.nspname AS sql_identifier) AS object_schema,
       CAST(c.collname AS sql_identifier) AS object_name,
       CAST('COLLATION' AS character_data) AS object_type,
       CAST('USAGE' AS character_data) AS privilege_type,
       CAST('NO' AS yes_or_no) AS is_grantable

FROM pg_authid u,
     pg_namespace n,
     pg_collation c

WHERE u.oid = c.collowner
      AND c.collnamespace = n.oid
      AND collencoding IN (-1, (SELECT encoding FROM pg_database WHERE datname = current_database()))

UNION ALL

/* information_schema.domains */
SELECT CAST(u_grantor.rolname AS sql_identifier) AS grantor,
       CAST(grantee.rolname AS sql_identifier) AS grantee,
       CAST(current_database() AS sql_identifier) AS object_catalog,
       CAST(n.nspname AS sql_identifier) AS object_schema,
       CAST(t.typname AS sql_identifier) AS object_name,
       CAST('DOMAIN' AS character_data) AS object_type,
       CAST('USAGE' AS character_data) AS privilege_type,
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
      AND t.typtype = 'd'
      AND t.grantee = grantee.oid
      AND t.grantor = u_grantor.oid
      AND t.prtype IN ('USAGE')
      AND (pg_has_role(u_grantor.oid, 'USAGE')
           OR pg_has_role(grantee.oid, 'USAGE')
           OR grantee.rolname = 'PUBLIC')

UNION ALL

/* foreign-data wrappers */
SELECT CAST(u_grantor.rolname AS sql_identifier) AS grantor,
       CAST(grantee.rolname AS sql_identifier) AS grantee,
       CAST(current_database() AS sql_identifier) AS object_catalog,
       CAST('' AS sql_identifier) AS object_schema,
       CAST(fdw.fdwname AS sql_identifier) AS object_name,
       CAST('FOREIGN DATA WRAPPER' AS character_data) AS object_type,
       CAST('USAGE' AS character_data) AS privilege_type,
       CAST(
         CASE WHEN
              -- object owner always has grant options
              pg_has_role(grantee.oid, fdw.fdwowner, 'USAGE')
              OR fdw.grantable
              THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_grantable

FROM (
        -- TODO(mbkkt): rewrite once DuckDB parser supports (expr).* composite expansion
        SELECT fdwname, fdwowner, acl.* FROM pg_foreign_data_wrapper, aclexplode(coalesce(fdwacl, acldefault('F', fdwowner))) AS acl
     ) AS fdw (fdwname, fdwowner, grantor, grantee, prtype, grantable),
     pg_authid u_grantor,
     (
       SELECT oid, rolname FROM pg_authid
       UNION ALL
       SELECT 0::oid, 'PUBLIC'
     ) AS grantee (oid, rolname)

WHERE u_grantor.oid = fdw.grantor
      AND grantee.oid = fdw.grantee
      AND fdw.prtype IN ('USAGE')
      AND (pg_has_role(u_grantor.oid, 'USAGE')
           OR pg_has_role(grantee.oid, 'USAGE')
           OR grantee.rolname = 'PUBLIC')

UNION ALL

/* foreign servers */
SELECT CAST(u_grantor.rolname AS sql_identifier) AS grantor,
       CAST(grantee.rolname AS sql_identifier) AS grantee,
       CAST(current_database() AS sql_identifier) AS object_catalog,
       CAST('' AS sql_identifier) AS object_schema,
       CAST(srv.srvname AS sql_identifier) AS object_name,
       CAST('FOREIGN SERVER' AS character_data) AS object_type,
       CAST('USAGE' AS character_data) AS privilege_type,
       CAST(
         CASE WHEN
              -- object owner always has grant options
              pg_has_role(grantee.oid, srv.srvowner, 'USAGE')
              OR srv.grantable
              THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_grantable

FROM (
        -- TODO(mbkkt): rewrite once DuckDB parser supports (expr).* composite expansion
        SELECT srvname, srvowner, acl.* FROM pg_foreign_server, aclexplode(coalesce(srvacl, acldefault('S', srvowner))) AS acl
     ) AS srv (srvname, srvowner, grantor, grantee, prtype, grantable),
     pg_authid u_grantor,
     (
       SELECT oid, rolname FROM pg_authid
       UNION ALL
       SELECT 0::oid, 'PUBLIC'
     ) AS grantee (oid, rolname)

WHERE u_grantor.oid = srv.grantor
      AND grantee.oid = srv.grantee
      AND srv.prtype IN ('USAGE')
      AND (pg_has_role(u_grantor.oid, 'USAGE')
           OR pg_has_role(grantee.oid, 'USAGE')
           OR grantee.rolname = 'PUBLIC')

UNION ALL

/* information_schema.sequences */
SELECT CAST(u_grantor.rolname AS sql_identifier) AS grantor,
       CAST(grantee.rolname AS sql_identifier) AS grantee,
       CAST(current_database() AS sql_identifier) AS object_catalog,
       CAST(n.nspname AS sql_identifier) AS object_schema,
       CAST(c.relname AS sql_identifier) AS object_name,
       CAST('SEQUENCE' AS character_data) AS object_type,
       CAST('USAGE' AS character_data) AS privilege_type,
       CAST(
         CASE WHEN
              -- object owner always has grant options
              pg_has_role(grantee.oid, c.relowner, 'USAGE')
              OR c.grantable
              THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_grantable

FROM (
        -- TODO(mbkkt): rewrite once DuckDB parser supports (expr).* composite expansion
        SELECT oid, relname, relnamespace, relkind, relowner, acl.* FROM pg_class, aclexplode(coalesce(relacl, acldefault('r', relowner))) AS acl
     ) AS c (oid, relname, relnamespace, relkind, relowner, grantor, grantee, prtype, grantable),
     pg_namespace n,
     pg_authid u_grantor,
     (
       SELECT oid, rolname FROM pg_authid
       UNION ALL
       SELECT 0::oid, 'PUBLIC'
     ) AS grantee (oid, rolname)

WHERE c.relnamespace = n.oid
      AND c.relkind = 'S'
      AND c.grantee = grantee.oid
      AND c.grantor = u_grantor.oid
      AND c.prtype IN ('USAGE')
      AND (pg_has_role(u_grantor.oid, 'USAGE')
           OR pg_has_role(grantee.oid, 'USAGE')
           OR grantee.rolname = 'PUBLIC')
