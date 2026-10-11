////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include <string_view>

namespace sdb::pg {

std::string_view gInfoColumnPrivileges = R"sql(SELECT CAST(u_grantor.rolname AS sql_identifier) AS grantor,
       CAST(grantee.rolname AS sql_identifier) AS grantee,
       CAST(current_database() AS sql_identifier) AS table_catalog,
       CAST(nc.nspname AS sql_identifier) AS table_schema,
       CAST(x.relname AS sql_identifier) AS table_name,
       CAST(x.attname AS sql_identifier) AS column_name,
       CAST(x.prtype AS character_data) AS privilege_type,
       CAST(
         CASE WHEN
              -- object owner always has grant options
              pg_has_role(x.grantee, x.relowner, 'USAGE')
              OR x.grantable
              THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_grantable

FROM (
       SELECT pr_c.grantor,
              pr_c.grantee,
              attname,
              relname,
              relnamespace,
              pr_c.prtype,
              pr_c.grantable,
              pr_c.relowner
       -- TODO(mbkkt): rewrite once DuckDB parser supports (expr).* composite expansion
       FROM (SELECT oid, relname, relnamespace, relowner, acl.*
             FROM pg_class, aclexplode(coalesce(relacl, acldefault('r', relowner))) AS acl
             WHERE relkind IN ('r', 'v', 'f', 'p')
            ) pr_c (oid, relname, relnamespace, relowner, grantor, grantee, prtype, grantable),
            pg_attribute a
       WHERE a.attrelid = pr_c.oid
             AND a.attnum > 0
             AND NOT a.attisdropped
       UNION
       SELECT pr_a.grantor,
              pr_a.grantee,
              attname,
              relname,
              relnamespace,
              pr_a.prtype,
              pr_a.grantable,
              c.relowner
       -- TODO(mbkkt): rewrite once DuckDB parser supports (expr).* composite expansion
       FROM (SELECT attrelid, attname, acl.*
             FROM pg_attribute a JOIN pg_class cc ON (a.attrelid = cc.oid), aclexplode(coalesce(attacl, acldefault('c', relowner))) AS acl
             WHERE attnum > 0
                   AND NOT attisdropped
            ) pr_a (attrelid, attname, grantor, grantee, prtype, grantable),
            pg_class c
       WHERE pr_a.attrelid = c.oid
             AND relkind IN ('r', 'v', 'f', 'p')
     ) x,
     pg_namespace nc,
     pg_authid u_grantor,
     (
       SELECT oid, rolname FROM pg_authid
       UNION ALL
       SELECT 0::oid, 'PUBLIC'
     ) AS grantee (oid, rolname)

WHERE x.relnamespace = nc.oid
      AND x.grantee = grantee.oid
      AND x.grantor = u_grantor.oid
      AND x.prtype IN ('INSERT', 'SELECT', 'UPDATE', 'REFERENCES')
      AND (pg_has_role(u_grantor.oid, 'USAGE')
           OR pg_has_role(grantee.oid, 'USAGE')
           OR grantee.rolname = 'PUBLIC'))sql";

}  // namespace sdb::pg
