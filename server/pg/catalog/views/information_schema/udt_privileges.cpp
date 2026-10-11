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

std::string_view gInfoUdtPrivileges = R"sql(SELECT CAST(u_grantor.rolname AS sql_identifier) AS grantor,
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
           OR grantee.rolname = 'PUBLIC'))sql";

}  // namespace sdb::pg
