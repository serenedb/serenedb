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

std::string_view gInfoTablePrivileges = R"sql(SELECT CAST(u_grantor.rolname AS sql_identifier) AS grantor,
       CAST(grantee.rolname AS sql_identifier) AS grantee,
       CAST(current_database() AS sql_identifier) AS table_catalog,
       CAST(nc.nspname AS sql_identifier) AS table_schema,
       CAST(c.relname AS sql_identifier) AS table_name,
       CAST(c.prtype AS character_data) AS privilege_type,
       CAST(
         CASE WHEN
              -- object owner always has grant options
              pg_has_role(grantee.oid, c.relowner, 'USAGE')
              OR c.grantable
              THEN 'YES' ELSE 'NO' END AS yes_or_no) AS is_grantable,
       CAST(CASE WHEN c.prtype = 'SELECT' THEN 'YES' ELSE 'NO' END AS yes_or_no) AS with_hierarchy

FROM (
        -- TODO(mbkkt): rewrite once DuckDB parser supports (expr).* composite expansion
        SELECT oid, relname, relnamespace, relkind, relowner, acl.* FROM pg_class, aclexplode(coalesce(relacl, acldefault('r', relowner))) AS acl
     ) AS c (oid, relname, relnamespace, relkind, relowner, grantor, grantee, prtype, grantable),
     pg_namespace nc,
     pg_authid u_grantor,
     (
       SELECT oid, rolname FROM pg_authid
       UNION ALL
       SELECT 0::oid, 'PUBLIC'
     ) AS grantee (oid, rolname)

WHERE c.relnamespace = nc.oid
      AND c.relkind IN ('r', 'v', 'f', 'p')
      AND c.grantee = grantee.oid
      AND c.grantor = u_grantor.oid
      AND c.prtype IN ('INSERT', 'SELECT', 'UPDATE', 'DELETE', 'TRUNCATE', 'REFERENCES', 'TRIGGER')
      AND (pg_has_role(u_grantor.oid, 'USAGE')
           OR pg_has_role(grantee.oid, 'USAGE')
           OR grantee.rolname = 'PUBLIC'))sql";

}  // namespace sdb::pg
